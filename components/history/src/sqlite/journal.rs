use serde_json::to_vec;
use sha2::{Digest, Sha256};
use sqlx::{Acquire, Sqlite, Transaction, query, query_as};

use super::{SqliteHistory, db, role_str, strict_role_from};
use crate::{
    AppendReceipt, AppendRequest, HistoryError, JournalPage, JournalRecord, PageRequest, Result,
    Turn,
};

pub(super) fn validate_key(key: &str) -> Result<()> {
    if key.is_empty() || key.len() > 4096 {
        return Err(HistoryError::InvalidRequest(
            "empty or oversized journal key".into(),
        ));
    }
    Ok(())
}

impl SqliteHistory {
    pub(super) async fn check_unmapped(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        key: &str,
    ) -> Result<()> {
        let target: Option<(String,)> =
            query_as("SELECT target_key FROM journal_key_remaps WHERE source_key = ?")
                .bind(key)
                .fetch_optional(&mut **tx)
                .await
                .map_err(db)?;
        if let Some((target_key,)) = target {
            return Err(HistoryError::KeyRemapped {
                source_key: key.into(),
                target_key,
            });
        }
        Ok(())
    }

    /// Append inside a caller-owned transaction on this store's database. The caller
    /// commits the receipt together with its application metadata, or rolls both back.
    /// Begin with `begin_journal` before reading metadata to reserve the SQLite writer.
    pub async fn append_journal_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        key: &str,
        request: &AppendRequest,
    ) -> Result<AppendReceipt> {
        let mut savepoint = tx.begin().await.map_err(db)?;
        match self.append_batch(&mut savepoint, key, request).await {
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

    async fn append_batch(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        key: &str,
        request: &AppendRequest,
    ) -> Result<AppendReceipt> {
        self.require_retained()?;
        validate_key(key)?;
        request.validate()?;
        self.check_unmapped(tx, key).await?;
        let batch_digest = format!("{:x}", Sha256::digest(to_vec(&request.turns)?));
        let inserted = query(
            "INSERT INTO journal_appends
             (channel, namespace, idempotency_key, request_digest, batch_digest)
             VALUES (?, ?, ?, ?, ?) ON CONFLICT(channel, namespace, idempotency_key) DO NOTHING",
        )
        .bind(key)
        .bind(&request.namespace)
        .bind(&request.idempotency_key)
        .bind(&request.digest)
        .bind(&batch_digest)
        .execute(&mut **tx)
        .await
        .map_err(db)?;
        let (append_id, digest, stored_batch): (i64, String, String) = query_as(
            "SELECT id, request_digest, batch_digest FROM journal_appends
             WHERE channel = ? AND namespace = ? AND idempotency_key = ?",
        )
        .bind(key)
        .bind(&request.namespace)
        .bind(&request.idempotency_key)
        .fetch_one(&mut **tx)
        .await
        .map_err(db)?;
        if digest != request.digest || stored_batch != batch_digest {
            return Err(HistoryError::IdempotencyConflict);
        }
        let duplicate = inserted.rows_affected() == 0;
        let mut record_ids = Vec::new();
        if duplicate {
            let rows: Vec<(i64,)> = query_as(
                "SELECT turn_id FROM journal_append_records WHERE append_id = ? ORDER BY ordinal",
            )
            .bind(append_id)
            .fetch_all(&mut **tx)
            .await
            .map_err(db)?;
            record_ids.extend(rows.into_iter().map(|(id,)| id));
            if record_ids.len() != request.turns.len() {
                return Err(HistoryError::Db("incomplete journal receipt".into()));
            }
        } else {
            for (ordinal, turn) in request.turns.iter().enumerate() {
                let id = query("INSERT INTO turns (channel, role, content) VALUES (?, ?, ?)")
                    .bind(key)
                    .bind(role_str(turn.role))
                    .bind(&turn.content)
                    .execute(&mut **tx)
                    .await
                    .map_err(db)?
                    .last_insert_rowid();
                query("INSERT INTO journal_append_records (append_id, ordinal, turn_id) VALUES (?, ?, ?)")
                    .bind(append_id).bind(ordinal as i64).bind(id)
                    .execute(&mut **tx).await.map_err(db)?;
                record_ids.push(id);
            }
        }
        let through_id = *record_ids
            .last()
            .ok_or_else(|| HistoryError::Db("journal receipt has no committed records".into()))?;
        Ok(AppendReceipt {
            record_ids,
            through_id,
            duplicate,
        })
    }

    pub(super) async fn load_journal_page(
        &self,
        key: &str,
        request: PageRequest,
    ) -> Result<JournalPage> {
        let mut tx = self.pool.begin().await.map_err(db)?;
        let page = self.journal_page_in(&mut tx, key, request).await?;
        tx.commit().await.map_err(db)?;
        Ok(page)
    }

    /// Strict raw journal read on the caller's existing snapshot/write transaction.
    pub async fn journal_page_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        key: &str,
        request: PageRequest,
    ) -> Result<JournalPage> {
        self.require_retained()?;
        validate_key(key)?;
        request.validate()?;
        self.check_unmapped(tx, key).await?;
        let (head,): (i64,) = query_as("SELECT COALESCE(MAX(id), 0) FROM turns WHERE channel = ?")
            .bind(key)
            .fetch_one(&mut **tx)
            .await
            .map_err(db)?;
        let through_id = request.through_id.unwrap_or(head);
        if through_id > head {
            return Err(HistoryError::InvalidRequest(
                "watermark exceeds journal head".into(),
            ));
        }
        let mut rows: Vec<(i64, String, String, String)> = query_as(
            "SELECT id, created_at, role, content FROM turns
             WHERE channel = ? AND id <= ? AND (? IS NULL OR id < ?)
             ORDER BY id DESC LIMIT ?",
        )
        .bind(key)
        .bind(through_id)
        .bind(request.before_id)
        .bind(request.before_id)
        .bind((request.limit + 1) as i64)
        .fetch_all(&mut **tx)
        .await
        .map_err(db)?;
        let has_more = rows.len() > request.limit;
        rows.truncate(request.limit);
        let next_before = has_more.then(|| rows.last().expect("nonempty page").0);
        let records = rows
            .into_iter()
            .rev()
            .map(|(id, created_at, role, content)| {
                Ok(JournalRecord {
                    id,
                    created_at,
                    turn: Turn {
                        role: strict_role_from(&role)?,
                        content,
                    },
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(JournalPage {
            records,
            next_before,
            through_id,
            has_more,
        })
    }
}
