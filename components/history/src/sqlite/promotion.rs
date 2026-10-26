use std::collections::BTreeMap;

use sqlx::{Acquire, Sqlite, Transaction, query, query_as};

use super::{SqliteHistory, db, role_str};
use crate::snapshot::SnapshotJournal;
use crate::{
    HistoryError, PromotionReceipt, Result, SessionAccess, SessionJournal, SessionSnapshot,
    SessionState,
};

impl SqliteHistory {
    /// Persist one owned temporary snapshot. The caller holds the session writer guard
    /// and commits its lifecycle markers/selectors in the SAME outer transaction.
    /// This method neither commits the caller's transaction nor removes RAM state.
    pub async fn promote_session_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        snapshot: &SessionSnapshot,
        promotion_key: &str,
    ) -> Result<PromotionReceipt> {
        let mut savepoint = tx.begin().await.map_err(db)?;
        let result = self
            .promote_inner(&mut savepoint, snapshot, promotion_key)
            .await;
        match result {
            Ok(receipt) => {
                savepoint.commit().await.map_err(db)?;
                Ok(receipt)
            }
            Err(error) => {
                savepoint.rollback().await.map_err(db)?;
                Err(error)
            }
        }
    }

    async fn promote_inner(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        snapshot: &SessionSnapshot,
        promotion_key: &str,
    ) -> Result<PromotionReceipt> {
        self.require_retained()?;
        snapshot.validate()?;
        if promotion_key.is_empty() || promotion_key.len() > 256 {
            return Err(HistoryError::InvalidRequest("invalid promotion key".into()));
        }
        let session = snapshot.session();
        let existing:Option<(String,String,i64,i64)>=query_as(
            "SELECT promotion_key,fingerprint,source_revision,target_revision FROM history_session_promotions WHERE session_id=?")
            .bind(session.id.as_str()).fetch_optional(&mut **tx).await.map_err(db)?;
        if let Some((key, fingerprint, source_revision, revision)) = existing {
            if key != promotion_key
                || fingerprint != snapshot.fingerprint()
                || source_revision != session.revision
            {
                return Err(HistoryError::SessionConflict);
            }
            return Ok(PromotionReceipt {
                session_id: session.id.clone(),
                fingerprint,
                source_revision,
                revision,
                duplicate: true,
            });
        }
        let used: Option<(String,)> =
            query_as("SELECT session_id FROM history_session_promotions WHERE promotion_key=?")
                .bind(promotion_key)
                .fetch_optional(&mut **tx)
                .await
                .map_err(db)?;
        if used.is_some() || self.get_session_in(tx, &session.id).await?.is_some() {
            return Err(HistoryError::SessionConflict);
        }
        let revision = session
            .revision
            .checked_add(1)
            .ok_or_else(|| HistoryError::InvalidRequest("session revision overflow".into()))?;
        for journal in &snapshot.payload.journals {
            let typed = if journal.run_id.is_empty() {
                SessionJournal::Root(session.id.clone())
            } else {
                SessionJournal::Child {
                    session_id: session.id.clone(),
                    run_id: journal.run_id.clone(),
                }
            };
            let key = typed.storage_key()?;
            self.check_unmapped(tx, &key).await?;
            let inventory = self.journal_inventory_in(tx, &key).await?;
            if inventory.turns + inventory.compacts + inventory.receipts != 0 {
                return Err(HistoryError::SessionConflict);
            }
        }
        let SessionAccess::Private { owner } = &session.access else {
            return Err(HistoryError::SessionConflict);
        };
        let state = match session.state {
            SessionState::Active => "active",
            SessionState::Archived => "archived",
        };
        query("INSERT INTO history_sessions(session_id,access_kind,owner_key,host,title,kind,state,revision,creation_request,created_at)
               VALUES (?,'private',?,?,?,'permanent',?,?,?,?)")
            .bind(session.id.as_str()).bind(owner).bind(&session.host).bind(&session.title).bind(state)
            .bind(revision).bind(&snapshot.payload.creation).bind(&session.created_at)
            .execute(&mut **tx).await.map_err(db)?;
        for journal in &snapshot.payload.journals {
            let typed = if journal.run_id.is_empty() {
                SessionJournal::Root(session.id.clone())
            } else {
                SessionJournal::Child {
                    session_id: session.id.clone(),
                    run_id: journal.run_id.clone(),
                }
            };
            let key = typed.storage_key()?;
            query("INSERT INTO history_session_journals(session_id,run_id,history_key) VALUES (?,?,?)")
                .bind(session.id.as_str()).bind(&journal.run_id).bind(&key).execute(&mut **tx).await.map_err(db)?;
            self.insert_snapshot_journal(tx, &key, journal).await?;
        }
        query("INSERT INTO history_session_promotions(session_id,promotion_key,fingerprint,source_revision,target_revision) VALUES (?,?,?,?,?)")
            .bind(session.id.as_str()).bind(promotion_key).bind(snapshot.fingerprint()).bind(session.revision).bind(revision)
            .execute(&mut **tx).await.map_err(db)?;
        Ok(PromotionReceipt {
            session_id: session.id.clone(),
            fingerprint: snapshot.fingerprint().into(),
            source_revision: session.revision,
            revision,
            duplicate: false,
        })
    }

    async fn insert_snapshot_journal(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        key: &str,
        journal: &SnapshotJournal,
    ) -> Result<()> {
        let mut turns = BTreeMap::new();
        for record in &journal.records {
            let physical =
                query("INSERT INTO turns(channel,role,content,created_at) VALUES (?,?,?,?)")
                    .bind(key)
                    .bind(role_str(record.turn.role))
                    .bind(&record.turn.content)
                    .bind(&record.created_at)
                    .execute(&mut **tx)
                    .await
                    .map_err(db)?
                    .last_insert_rowid();
            query("INSERT INTO history_public_turns(history_key,public_id,turn_id) VALUES (?,?,?)")
                .bind(key)
                .bind(record.id)
                .bind(physical)
                .execute(&mut **tx)
                .await
                .map_err(db)?;
            turns.insert(record.id, physical);
        }
        let mut compacts = BTreeMap::new();
        for compact in &journal.compacts {
            let through = turns
                .get(&compact.through_id)
                .ok_or(HistoryError::SessionConflict)?;
            let previous = compact
                .previous_id
                .map(|id| {
                    compacts
                        .get(&id)
                        .copied()
                        .ok_or(HistoryError::SessionConflict)
                })
                .transpose()?;
            let physical=query("INSERT INTO compacts(channel,through_id,previous_id,content,created_at) VALUES (?,?,?,?,?)")
                .bind(key).bind(through).bind(previous).bind(&compact.content).bind(&compact.created_at)
                .execute(&mut **tx).await.map_err(db)?.last_insert_rowid();
            query("INSERT INTO history_public_compacts(history_key,public_id,compact_id) VALUES (?,?,?)")
                .bind(key).bind(compact.id).bind(physical).execute(&mut **tx).await.map_err(db)?;
            compacts.insert(compact.id, physical);
        }
        for receipt in &journal.receipts {
            let append=query("INSERT INTO journal_appends(channel,namespace,idempotency_key,request_digest,batch_digest) VALUES (?,?,?,?,?)")
                .bind(key).bind(&receipt.namespace).bind(&receipt.idempotency_key).bind(&receipt.digest).bind(&receipt.batch_digest)
                .execute(&mut **tx).await.map_err(db)?.last_insert_rowid();
            for (ordinal, id) in receipt.record_ids.iter().enumerate() {
                let physical = turns.get(id).ok_or(HistoryError::SessionConflict)?;
                query(
                    "INSERT INTO journal_append_records(append_id,ordinal,turn_id) VALUES (?,?,?)",
                )
                .bind(append)
                .bind(ordinal as i64)
                .bind(physical)
                .execute(&mut **tx)
                .await
                .map_err(db)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        AppendRequest, CreateSession, RetainedMemoryHistory, SessionJournal, SessionKind,
        SessionStore, SqliteHistory, Turn,
    };
    use sqlx::{query, query_as, sqlite::SqlitePoolOptions};

    #[tokio::test]
    async fn full_compact_metadata_survives_and_tampered_snapshot_leaves_no_sql() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let sql = SqliteHistory::from_pool(pool).await.unwrap();
        let ram = RetainedMemoryHistory::default();
        let mut create = CreateSession::private("owner");
        create.kind = SessionKind::Temporary;
        let session = ram.create_session(create).await.unwrap();
        let root = SessionJournal::Root(session.id.clone());
        let receipt = ram
            .append_session(
                &root,
                &AppendRequest {
                    namespace: "tg".into(),
                    idempotency_key: "1".into(),
                    digest: "hash".into(),
                    turns: vec![Turn::user("data")],
                },
            )
            .await
            .unwrap();
        ram.save_session_compact(&root, None, receipt.through_id, "first")
            .await
            .unwrap();
        let previous = ram
            .session_context(&root)
            .await
            .unwrap()
            .compact
            .unwrap()
            .id;
        ram.save_session_compact(&root, Some(previous), receipt.through_id, "second")
            .await
            .unwrap();
        let snapshot = ram.export_session(&session.id).unwrap();
        let mut tampered = snapshot.clone();
        tampered.payload.journals[0].receipts[0].record_ids[0] = 999;
        let mut tx = sql.begin_journal().await.unwrap();
        assert!(
            sql.promote_session_in(&mut tx, &tampered, "bad")
                .await
                .is_err()
        );
        tx.commit().await.unwrap();
        assert!(sql.get_session(&session.id).await.unwrap().is_none());
        let mut tx = sql.begin_journal().await.unwrap();
        sql.promote_session_in(&mut tx, &snapshot, "good")
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let rows: Vec<(i64, Option<i64>, i64, String, String)> = query_as(
            "SELECT m.public_id,pm.public_id,t.public_id,c.content,c.created_at FROM compacts c
             JOIN history_public_compacts m ON m.compact_id=c.id
             JOIN history_public_turns t ON t.turn_id=c.through_id
             LEFT JOIN history_public_compacts pm ON pm.compact_id=c.previous_id
             WHERE m.history_key=? ORDER BY c.id",
        )
        .bind(root.storage_key().unwrap())
        .fetch_all(sql.pool())
        .await
        .unwrap();
        let expected: Vec<_> = snapshot.payload.journals[0]
            .compacts
            .iter()
            .map(|c| {
                (
                    c.id,
                    c.previous_id,
                    c.through_id,
                    c.content.clone(),
                    c.created_at.clone(),
                )
            })
            .collect();
        assert_eq!(rows, expected);
        assert!(
            query("PRAGMA foreign_key_check")
                .fetch_all(sql.pool())
                .await
                .unwrap()
                .is_empty()
        );
    }
}
