use sqlx::{Acquire, Sqlite, Transaction, query, query_as};

use super::{SqliteHistory, db};
use crate::{
    AppendReceipt, AppendRequest, HistoryError, JournalKeyInventory, KeyRemap, LegacySessionImport,
    LegacySessionReceipt, RemapReceipt, Result, SessionId, SessionJournal,
};

impl SqliteHistory {
    pub(super) async fn session_journal_key_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        journal: &SessionJournal,
        create: bool,
    ) -> Result<String> {
        let session_id = journal.session_id();
        self.get_session_in(tx, session_id)
            .await?
            .ok_or(HistoryError::SessionNotFound)?;
        let expected = journal.storage_key()?;
        let run_id = match journal {
            SessionJournal::Root(_) => "",
            SessionJournal::Child { run_id, .. } => run_id,
        };
        let stored: Option<(String,)> = query_as(
            "SELECT history_key FROM history_session_journals WHERE session_id=? AND run_id=?",
        )
        .bind(session_id.as_str())
        .bind(run_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)?;
        if let Some((key,)) = stored {
            return if key == expected {
                Ok(key)
            } else {
                Err(HistoryError::SessionConflict)
            };
        }
        if !create {
            return Err(HistoryError::SessionNotFound);
        }
        let occupied = self.journal_inventory_in(tx, &expected).await?;
        if occupied.turns + occupied.compacts + occupied.receipts != 0 {
            return Err(HistoryError::SessionConflict);
        }
        query(
            "INSERT INTO history_session_journals (session_id,run_id,history_key) VALUES (?,?,?)",
        )
        .bind(session_id.as_str())
        .bind(run_id)
        .bind(&expected)
        .execute(&mut **tx)
        .await
        .map_err(db)?;
        Ok(expected)
    }

    /// Typed append and assembly-owned request metadata share the caller's transaction.
    /// Failed appends roll back newly registered child journals as well as journal rows.
    pub async fn append_session_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        journal: &SessionJournal,
        request: &AppendRequest,
    ) -> Result<AppendReceipt> {
        let mut savepoint = tx.begin().await.map_err(db)?;
        let result = async {
            let key = self
                .session_journal_key_in(&mut savepoint, journal, true)
                .await?;
            self.backfill_public_ids_in(&mut savepoint, &key).await?;
            let mut receipt = self
                .append_journal_in(&mut savepoint, &key, request)
                .await?;
            self.map_public_ids_in(&mut savepoint, &key, true, false)
                .await?;
            for id in &mut receipt.record_ids {
                *id = self.public_turn_in(&mut savepoint, &key, *id).await?;
            }
            receipt.through_id = *receipt.record_ids.last().expect("nonempty receipt");
            Ok(receipt)
        }
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

    /// Atomically establish authoritative sessions and move verified legacy root journals.
    /// The assembly supplies provenance and can commit its transport defaults in `_in`.
    pub async fn import_legacy_sessions(
        &self,
        imports: &[LegacySessionImport],
    ) -> Result<Vec<LegacySessionReceipt>> {
        let mut tx = self.begin_journal().await?;
        let result = self.import_legacy_sessions_in(&mut tx, imports).await?;
        tx.commit().await.map_err(db)?;
        Ok(result)
    }

    pub async fn import_legacy_sessions_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        imports: &[LegacySessionImport],
    ) -> Result<Vec<LegacySessionReceipt>> {
        let mut savepoint = tx.begin().await.map_err(db)?;
        let result = async {
            let mut sessions = Vec::with_capacity(imports.len());
            let mut mappings = Vec::with_capacity(imports.len());
            for import in imports {
                let id = import.session.id.as_ref().ok_or_else(|| {
                    HistoryError::InvalidRequest("legacy import requires stable session ID".into())
                })?;
                let session = self
                    .create_session_in(&mut savepoint, import.session.clone())
                    .await?;
                self.bind_default_in(&mut savepoint, &import.origin, &session)
                    .await?;
                mappings.push(KeyRemap {
                    source: import.source.clone(),
                    target: SessionJournal::Root(id.clone()).storage_key()?,
                });
                sessions.push(session);
            }
            let migrated = self
                .remap_journal_keys_in(&mut savepoint, &mappings)
                .await?;
            for mapping in &mappings {
                self.backfill_public_ids_in(&mut savepoint, &mapping.target)
                    .await?;
            }
            Ok(sessions
                .into_iter()
                .zip(migrated)
                .map(|(session, receipt)| LegacySessionReceipt {
                    session,
                    turns: receipt.turns,
                    compacts: receipt.compacts,
                    receipts: receipt.receipts,
                    duplicate: receipt.duplicate,
                })
                .collect())
        }
        .await;
        match result {
            Ok(receipts) => {
                savepoint.commit().await.map_err(db)?;
                Ok(receipts)
            }
            Err(error) => {
                savepoint.rollback().await.map_err(db)?;
                Err(error)
            }
        }
    }

    /// Assign a legacy child journal only after the assembly verifies its recorded
    /// parent/origin. Its run ID and journal remain distinct from the session's root.
    pub async fn import_child_journal_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        session: &SessionId,
        run_id: &str,
        source: &JournalKeyInventory,
    ) -> Result<RemapReceipt> {
        let mut savepoint = tx.begin().await.map_err(db)?;
        let result = async {
            let journal = SessionJournal::Child {
                session_id: session.clone(),
                run_id: run_id.into(),
            };
            let target = self
                .session_journal_key_in(&mut savepoint, &journal, true)
                .await?;
            let mappings = [KeyRemap {
                source: source.clone(),
                target,
            }];
            let receipt = self
                .remap_journal_keys_in(&mut savepoint, &mappings)
                .await?
                .pop()
                .ok_or_else(|| HistoryError::Db("missing child remap receipt".into()))?;
            self.backfill_public_ids_in(&mut savepoint, &mappings[0].target)
                .await?;
            Ok(receipt)
        }
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

    pub async fn import_child_journal(
        &self,
        session: &SessionId,
        run_id: &str,
        source: &JournalKeyInventory,
    ) -> Result<RemapReceipt> {
        let mut tx = self.begin_journal().await?;
        let receipt = self
            .import_child_journal_in(&mut tx, session, run_id, source)
            .await?;
        tx.commit().await.map_err(db)?;
        Ok(receipt)
    }
}
