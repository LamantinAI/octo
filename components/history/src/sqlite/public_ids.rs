use sqlx::{Sqlite, Transaction, query, query_as};

use super::{SqliteHistory, db, strict_role_from};
use crate::{
    Compact, ContextWindow, HistoryError, JournalPage, JournalRecord, PageRequest, Result,
    StoredTurn, Turn,
};

impl SqliteHistory {
    pub(super) async fn backfill_public_ids_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        key: &str,
    ) -> Result<()> {
        self.map_public_ids_in(tx, key, true, true).await?;
        self.map_public_ids_in(tx, key, false, true).await
    }

    /// Preserve IDs exposed by existing SQL sessions; new typed writes allocate max+1.
    pub(super) async fn map_public_ids_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        key: &str,
        turns: bool,
        identity_if_empty: bool,
    ) -> Result<()> {
        let (mapping, table, physical) = if turns {
            ("history_public_turns", "turns", "turn_id")
        } else {
            ("history_public_compacts", "compacts", "compact_id")
        };
        let statement =
            format!("SELECT COALESCE(MAX(public_id),0) FROM {mapping} WHERE history_key=?");
        let (mut maximum,): (i64,) = query_as(&statement)
            .bind(key)
            .fetch_one(&mut **tx)
            .await
            .map_err(db)?;
        let identity = identity_if_empty && maximum == 0;
        loop {
            let statement = format!(
                "SELECT r.id FROM {table} r LEFT JOIN {mapping} m ON m.{physical}=r.id WHERE r.channel=? AND m.{physical} IS NULL ORDER BY r.id LIMIT 128"
            );
            let rows: Vec<(i64,)> = query_as(&statement)
                .bind(key)
                .fetch_all(&mut **tx)
                .await
                .map_err(db)?;
            if rows.is_empty() {
                break;
            }
            for (id,) in rows {
                let public_id = if identity {
                    id
                } else {
                    maximum
                        .checked_add(1)
                        .ok_or_else(|| HistoryError::Db("journal ID overflow".into()))?
                };
                let statement = format!(
                    "INSERT INTO {mapping}(history_key,public_id,{physical}) VALUES (?,?,?)"
                );
                query(&statement)
                    .bind(key)
                    .bind(public_id)
                    .bind(id)
                    .execute(&mut **tx)
                    .await
                    .map_err(db)?;
                maximum = public_id;
            }
        }
        Ok(())
    }

    pub(super) async fn public_turn_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        key: &str,
        physical: i64,
    ) -> Result<i64> {
        let (id,): (i64,) = query_as(
            "SELECT public_id FROM history_public_turns WHERE history_key=? AND turn_id=?",
        )
        .bind(key)
        .bind(physical)
        .fetch_one(&mut **tx)
        .await
        .map_err(db)?;
        Ok(id)
    }

    pub(super) async fn public_page_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        key: &str,
        request: PageRequest,
    ) -> Result<JournalPage> {
        request.validate()?;
        let (head,): (i64,) = query_as(
            "SELECT COALESCE(MAX(public_id),0) FROM history_public_turns WHERE history_key=?",
        )
        .bind(key)
        .fetch_one(&mut **tx)
        .await
        .map_err(db)?;
        let through_id = request.through_id.unwrap_or(head);
        if through_id > head {
            return Err(HistoryError::InvalidRequest(
                "watermark exceeds scoped journal head".into(),
            ));
        }
        let mut rows: Vec<(i64,String,String,String)> = query_as(
            "SELECT m.public_id,t.created_at,t.role,t.content FROM history_public_turns m JOIN turns t ON t.id=m.turn_id
             WHERE m.history_key=? AND m.public_id<=? AND (? IS NULL OR m.public_id<?) ORDER BY m.public_id DESC LIMIT ?")
            .bind(key).bind(through_id).bind(request.before_id).bind(request.before_id).bind((request.limit+1) as i64)
            .fetch_all(&mut **tx).await.map_err(db)?;
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

    pub(super) async fn public_context_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        key: &str,
    ) -> Result<ContextWindow> {
        let row: Option<(i64, i64, String)> = query_as(
            "SELECT mc.public_id,mt.public_id,c.content FROM compacts c
             JOIN history_public_compacts mc ON mc.compact_id=c.id
             JOIN history_public_turns mt ON mt.turn_id=c.through_id
             WHERE mc.history_key=? AND mt.history_key=? ORDER BY c.id DESC LIMIT 1",
        )
        .bind(key)
        .bind(key)
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)?;
        let compact = row.map(|(id, through_id, content)| Compact {
            id,
            through_id,
            content,
        });
        let boundary = compact.as_ref().map(|c| c.through_id).unwrap_or(0);
        let rows:Vec<(i64,String,String)>=query_as(
            "SELECT m.public_id,t.role,t.content FROM history_public_turns m JOIN turns t ON t.id=m.turn_id
             WHERE m.history_key=? AND m.public_id>? ORDER BY m.public_id")
            .bind(key).bind(boundary).fetch_all(&mut **tx).await.map_err(db)?;
        let messages = rows
            .into_iter()
            .map(|(id, role, content)| {
                Ok(StoredTurn {
                    id,
                    turn: Turn {
                        role: strict_role_from(&role)?,
                        content,
                    },
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(ContextWindow { compact, messages })
    }

    pub(super) async fn save_public_compact_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        key: &str,
        previous: Option<i64>,
        through: i64,
        content: &str,
    ) -> Result<bool> {
        let through_row: Option<(i64,)> = query_as(
            "SELECT turn_id FROM history_public_turns WHERE history_key=? AND public_id=?",
        )
        .bind(key)
        .bind(through)
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)?;
        let Some((physical_through,)) = through_row else {
            return Ok(false);
        };
        let physical_previous = if let Some(previous) = previous {
            let row:Option<(i64,)>=query_as("SELECT compact_id FROM history_public_compacts WHERE history_key=? AND public_id=?")
                .bind(key).bind(previous).fetch_optional(&mut **tx).await.map_err(db)?;
            let Some((id,)) = row else { return Ok(false) };
            Some(id)
        } else {
            None
        };
        let saved = self
            .commit_compact_in(tx, key, physical_previous, physical_through, content)
            .await?;
        if saved {
            self.map_public_ids_in(tx, key, false, false).await?;
        }
        Ok(saved)
    }
}
