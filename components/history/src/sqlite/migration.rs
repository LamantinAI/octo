use std::collections::HashSet;

use serde::Serialize;
use serde_json::{from_str, to_string, to_vec};
use sha2::{Digest, Sha256};
use sqlx::{Acquire, Sqlite, Transaction, query, query_as};

use super::{SqliteHistory, db, journal::validate_key};
use crate::{HistoryError, JournalKeyInventory, KeyRemap, RemapReceipt, Result};

fn hash_row<T: Serialize>(digest: &mut Sha256, row: &T) -> Result<()> {
    let bytes = to_vec(row)?;
    digest.update((bytes.len() as u64).to_le_bytes());
    digest.update(bytes);
    Ok(())
}

impl SqliteHistory {
    /// Inventory stored keys without guessing their connector, user or ownership.
    /// All fingerprints are read from one database snapshot. Content is not returned.
    pub async fn journal_inventory(&self) -> Result<Vec<JournalKeyInventory>> {
        self.require_retained()?;
        let mut tx = self.pool.begin().await.map_err(db)?;
        let inventory = self.journal_inventory_all_in(&mut tx).await?;
        tx.commit().await.map_err(db)?;
        Ok(inventory)
    }

    /// Inventory all journals on the caller's transaction, without another pool lease.
    pub async fn journal_inventory_all_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
    ) -> Result<Vec<JournalKeyInventory>> {
        self.inventory_prefix_in(tx, None).await
    }

    /// Literal prefix filter before fingerprinting, so host migration checks do
    /// not read unrelated retained transcript content on ordinary admission.
    pub async fn journal_inventory_prefix_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        prefix: &str,
    ) -> Result<Vec<JournalKeyInventory>> {
        validate_key(prefix)?;
        self.inventory_prefix_in(tx, Some(prefix)).await
    }

    async fn inventory_prefix_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        prefix: Option<&str>,
    ) -> Result<Vec<JournalKeyInventory>> {
        self.require_retained()?;
        let keys: Vec<(String,)> = query_as(
            "SELECT channel FROM turns WHERE (?1 IS NULL OR substr(channel,1,length(?1))=?1)
             UNION SELECT channel FROM compacts WHERE (?1 IS NULL OR substr(channel,1,length(?1))=?1)
             UNION SELECT channel FROM journal_appends WHERE (?1 IS NULL OR substr(channel,1,length(?1))=?1) ORDER BY channel",
        )
        .bind(prefix)
        .fetch_all(&mut **tx)
        .await
        .map_err(db)?;
        let mut inventory = Vec::with_capacity(keys.len());
        for (key,) in keys {
            inventory.push(self.journal_inventory_in(tx, &key).await?);
        }
        Ok(inventory)
    }

    /// Read one key's exact inventory on the caller's transaction. The fingerprint
    /// includes content/timestamps/references, processed in bounded batches of 128 rows.
    pub async fn journal_inventory_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        key: &str,
    ) -> Result<JournalKeyInventory> {
        self.require_retained()?;
        let mut digest = Sha256::new();
        let mut inventory = JournalKeyInventory {
            key: key.into(),
            turns: 0,
            compacts: 0,
            receipts: 0,
            first_id: None,
            last_id: None,
            fingerprint: String::new(),
        };
        let mut after = 0;
        loop {
            let rows: Vec<(i64, String, String, String)> = query_as(
                "SELECT id, role, content, created_at FROM turns
                 WHERE channel = ? AND id > ? ORDER BY id LIMIT 128",
            )
            .bind(key)
            .bind(after)
            .fetch_all(&mut **tx)
            .await
            .map_err(db)?;
            if rows.is_empty() {
                break;
            }
            for row in rows {
                hash_row(&mut digest, &("turn", &row))?;
                inventory.turns += 1;
                inventory.first_id.get_or_insert(row.0);
                inventory.last_id = Some(row.0);
                after = row.0;
            }
        }
        after = 0;
        loop {
            let rows: Vec<(i64, i64, Option<i64>, String, String)> = query_as(
                "SELECT id, through_id, previous_id, content, created_at FROM compacts
                 WHERE channel = ? AND id > ? ORDER BY id LIMIT 128",
            )
            .bind(key)
            .bind(after)
            .fetch_all(&mut **tx)
            .await
            .map_err(db)?;
            if rows.is_empty() {
                break;
            }
            for row in rows {
                hash_row(&mut digest, &("compact", &row))?;
                inventory.compacts += 1;
                after = row.0;
            }
        }
        after = 0;
        loop {
            let rows: Vec<(i64, String, String, String, String, String)> = query_as(
                "SELECT id, namespace, idempotency_key, request_digest, batch_digest, created_at
                 FROM journal_appends WHERE channel = ? AND id > ? ORDER BY id LIMIT 128",
            )
            .bind(key)
            .bind(after)
            .fetch_all(&mut **tx)
            .await
            .map_err(db)?;
            if rows.is_empty() {
                break;
            }
            for row in rows {
                hash_row(&mut digest, &("receipt", &row))?;
                let mut ordinal = -1;
                loop {
                    let links: Vec<(i64, i64)> = query_as(
                        "SELECT ordinal, turn_id FROM journal_append_records
                         WHERE append_id = ? AND ordinal > ? ORDER BY ordinal LIMIT 128",
                    )
                    .bind(row.0)
                    .bind(ordinal)
                    .fetch_all(&mut **tx)
                    .await
                    .map_err(db)?;
                    if links.is_empty() {
                        break;
                    }
                    for link in links {
                        hash_row(&mut digest, &("receipt_record", row.0, link))?;
                        ordinal = link.0;
                    }
                }
                inventory.receipts += 1;
                after = row.0;
            }
        }
        inventory.fingerprint = format!("{:x}", digest.finalize());
        Ok(inventory)
    }

    /// Apply a verified one-to-one inventory mapping atomically, preserving row IDs
    /// and every compact/receipt reference. Retrying the same mapping is a no-op.
    pub async fn remap_journal_keys(&self, mappings: &[KeyRemap]) -> Result<Vec<RemapReceipt>> {
        let mut tx = self.begin_journal().await?;
        let receipts = self.remap_journal_keys_in(&mut tx, mappings).await?;
        tx.commit().await.map_err(db)?;
        Ok(receipts)
    }

    /// Transaction seam for committing verified application metadata with key remaps.
    /// A savepoint rolls back this mapping batch on error, even if the caller later
    /// commits unrelated application metadata in its outer transaction.
    pub async fn remap_journal_keys_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        mappings: &[KeyRemap],
    ) -> Result<Vec<RemapReceipt>> {
        let mut savepoint = tx.begin().await.map_err(db)?;
        match self.remap_batch(&mut savepoint, mappings).await {
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

    async fn remap_batch(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        mappings: &[KeyRemap],
    ) -> Result<Vec<RemapReceipt>> {
        self.require_retained()?;
        let mut sources = HashSet::new();
        let mut targets = HashSet::new();
        for mapping in mappings {
            validate_key(&mapping.target)?;
            if mapping.source.key == mapping.target
                || !sources.insert(mapping.source.key.as_str())
                || !targets.insert(mapping.target.as_str())
            {
                return Err(HistoryError::MigrationConflict(
                    "mapping is not one-to-one".into(),
                ));
            }
        }
        if sources.iter().any(|source| targets.contains(source)) {
            return Err(HistoryError::MigrationConflict(
                "chained or cyclic key mapping".into(),
            ));
        }
        // Validate the entire batch before mutating any key.
        let mut receipts = Vec::with_capacity(mappings.len());
        for mapping in mappings {
            let existing: Option<(String, String)> = query_as(
                "SELECT target_key, inventory FROM journal_key_remaps WHERE source_key = ?",
            )
            .bind(&mapping.source.key)
            .fetch_optional(&mut **tx)
            .await
            .map_err(db)?;
            let duplicate = if let Some((target, observed)) = existing {
                let observed: JournalKeyInventory = from_str(&observed)?;
                if target != mapping.target || observed != mapping.source {
                    return Err(HistoryError::MigrationConflict(
                        "source was mapped differently".into(),
                    ));
                }
                true
            } else {
                let bound: Option<(String,)> = query_as(
                    "SELECT session_id FROM history_session_journals WHERE history_key = ?",
                )
                .bind(&mapping.source.key)
                .fetch_optional(&mut **tx)
                .await
                .map_err(db)?;
                if bound.is_some() {
                    return Err(HistoryError::MigrationConflict(
                        "source is an authoritative session journal".into(),
                    ));
                }
                let previous_target: Option<(String,)> =
                    query_as("SELECT source_key FROM journal_key_remaps WHERE target_key = ?")
                        .bind(&mapping.source.key)
                        .fetch_optional(&mut **tx)
                        .await
                        .map_err(db)?;
                if previous_target.is_some() {
                    return Err(HistoryError::MigrationConflict(
                        "source is a completed remap target".into(),
                    ));
                }
                self.check_unmapped(tx, &mapping.target).await?;
                let reserved: Option<(String,)> =
                    query_as("SELECT source_key FROM journal_key_remaps WHERE target_key = ?")
                        .bind(&mapping.target)
                        .fetch_optional(&mut **tx)
                        .await
                        .map_err(db)?;
                let target = self.journal_inventory_in(tx, &mapping.target).await?;
                if reserved.is_some() || target.turns + target.compacts + target.receipts != 0 {
                    return Err(HistoryError::MigrationConflict(
                        "target already exists".into(),
                    ));
                }
                let actual = self.journal_inventory_in(tx, &mapping.source.key).await?;
                if actual != mapping.source || actual.turns + actual.compacts + actual.receipts == 0
                {
                    return Err(HistoryError::MigrationConflict(
                        "source inventory changed or is empty".into(),
                    ));
                }
                false
            };
            receipts.push(RemapReceipt {
                source: mapping.source.key.clone(),
                target: mapping.target.clone(),
                turns: mapping.source.turns,
                compacts: mapping.source.compacts,
                receipts: mapping.source.receipts,
                duplicate,
            });
        }
        for (mapping, receipt) in mappings.iter().zip(&receipts) {
            if receipt.duplicate {
                continue;
            }
            for statement in [
                "UPDATE turns SET channel = ? WHERE channel = ?",
                "UPDATE compacts SET channel = ? WHERE channel = ?",
                "UPDATE journal_appends SET channel = ? WHERE channel = ?",
            ] {
                query(statement)
                    .bind(&mapping.target)
                    .bind(&mapping.source.key)
                    .execute(&mut **tx)
                    .await
                    .map_err(db)?;
            }
            query("INSERT INTO journal_key_remaps (source_key, target_key, inventory) VALUES (?, ?, ?)")
                .bind(&mapping.source.key).bind(&mapping.target).bind(to_string(&mapping.source)?)
                .execute(&mut **tx).await.map_err(db)?;
        }
        Ok(receipts)
    }
}
