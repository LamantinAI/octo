use sqlx::query;
use sqlx::query_as;

use super::{SqliteHistory, db, role_from};
use crate::{Compact, ContextWindow, HistoryError, Result, StoredTurn, Turn};

impl SqliteHistory {
    pub(super) async fn load_context(&self, channel: &str) -> Result<ContextWindow> {
        if self.max.is_some() {
            return Err(HistoryError::ContextUnsupported);
        }
        let mut tx = self.pool.begin().await.map_err(db)?;
        let row: Option<(i64, i64, String)> = query_as(
            "SELECT id, through_id, content FROM compacts WHERE channel = ? ORDER BY id DESC LIMIT 1")
            .bind(channel).fetch_optional(&mut *tx).await.map_err(db)?;
        let compact = row.map(|(id, through_id, content)| Compact {
            id,
            through_id,
            content,
        });
        let boundary = compact.as_ref().map(|c| c.through_id).unwrap_or(0);
        let rows: Vec<(i64, String, String)> = query_as(
            "SELECT id, role, content FROM turns WHERE channel = ? AND id > ? ORDER BY id",
        )
        .bind(channel)
        .bind(boundary)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(ContextWindow {
            compact,
            messages: rows
                .into_iter()
                .map(|(id, role, content)| StoredTurn {
                    id,
                    turn: Turn {
                        role: role_from(&role),
                        content,
                    },
                })
                .collect(),
        })
    }

    pub(super) async fn commit_compact(
        &self,
        channel: &str,
        previous: Option<i64>,
        through: i64,
        content: &str,
    ) -> Result<bool> {
        if self.max.is_some() {
            return Err(HistoryError::ContextUnsupported);
        }
        if content.trim().is_empty() {
            return Err(HistoryError::Db("Empty compact refused".into()));
        }
        // A single conditional INSERT is atomic even across independent processes.
        // The boundary must belong to this channel, not precede the prior checkpoint,
        // and the previously observed checkpoint must still be the latest.
        let result = query("INSERT INTO compacts (channel, through_id, previous_id, content)
            SELECT ?1, ?2, ?3, ?4
            WHERE EXISTS (SELECT 1 FROM turns WHERE channel = ?1 AND id = ?2)
            AND (SELECT id FROM compacts WHERE channel = ?1 ORDER BY id DESC LIMIT 1) IS ?3
            AND ?2 >= COALESCE((SELECT through_id FROM compacts WHERE channel = ?1 ORDER BY id DESC LIMIT 1), 0)")
            .bind(channel).bind(through).bind(previous).bind(content)
            .execute(&self.pool).await.map_err(db)?;
        Ok(result.rows_affected() == 1)
    }
}
