//! SQLite-backed [`HistoryStore`] — a durable, migrated per-channel transcript
//! (opt-in behind the `sqlite` feature).
//!
//! Chosen over an ORM deliberately: the surface is one table and three trivial
//! statements (insert / trim / load-last-N), so hand-written SQL is clearer than a
//! schema DSL, and the async trait fits `sqlx` directly. Uses a **bundled**
//! libsqlite3 (no system dependency) and **runtime** queries (no `DATABASE_URL` at
//! build). Migrations under `migrations/` are embedded at compile time and run when
//! the store opens, so a schema change is a new `.sql` file, not a manual step.

mod context;

use std::path::Path;

use async_trait::async_trait;
use sqlx::{
    Pool, Sqlite,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
};

use crate::{ContextWindow, HistoryError, HistoryStore, Result, Role, Turn};

fn db(e: impl std::fmt::Display) -> HistoryError {
    HistoryError::Db(e.to_string())
}

/// A per-channel transcript persisted in a SQLite file, capped at `max` turns per
/// channel. Migrations run automatically at [`open`](Self::open).
pub struct SqliteHistory {
    pool: Pool<Sqlite>,
    max: Option<i64>,
}

impl SqliteHistory {
    /// Open (creating the file + running migrations if needed) a SQLite history.
    pub async fn open(path: impl AsRef<Path>, max: usize) -> Result<Self> {
        Self::open_mode(path, Some(max.max(1) as i64)).await
    }

    /// Retain every message, with independently committed context checkpoints.
    /// Existing capped users keep their previous behavior through `open`.
    pub async fn open_retained(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_mode(path, None).await
    }

    async fn open_mode(path: impl AsRef<Path>, max: Option<i64>) -> Result<Self> {
        let opts = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            // WAL: readers don't block the single writer — a good fit for the
            // cogitator's load-then-append rhythm.
            .journal_mode(SqliteJournalMode::Wal);
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(opts)
            .await
            .map_err(db)?;
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .map_err(db)?;
        Ok(Self { pool, max })
    }
}

#[async_trait]
impl HistoryStore for SqliteHistory {
    fn retains_context(&self) -> bool {
        self.max.is_none()
    }
    async fn context(&self, channel: &str) -> Result<ContextWindow> {
        self.load_context(channel).await
    }
    async fn save_compact(
        &self,
        channel: &str,
        previous_id: Option<i64>,
        through_id: i64,
        content: &str,
    ) -> Result<bool> {
        self.commit_compact(channel, previous_id, through_id, content)
            .await
    }
    async fn load(&self, channel: &str) -> Vec<Turn> {
        // Newest `max` for the channel, returned oldest -> newest.
        let rows: Vec<(String, String)> = match sqlx::query_as(
            "SELECT role, content FROM turns WHERE channel = ?1 ORDER BY id DESC LIMIT ?2",
        )
        .bind(channel)
        .bind(self.max.unwrap_or(-1))
        .fetch_all(&self.pool)
        .await
        {
            Ok(r) => r,
            // Mirror the file backend: a read failure yields an empty window, never a
            // panic — a missing turn must not wedge a conversation.
            Err(_) => return Vec::new(),
        };
        rows.into_iter()
            .rev()
            .map(|(role, content)| Turn {
                role: role_from(&role),
                content,
            })
            .collect()
    }

    async fn append(&self, channel: &str, turns: &[Turn]) -> Result<()> {
        let mut tx = self.pool.begin().await.map_err(db)?;
        for t in turns {
            sqlx::query("INSERT INTO turns (channel, role, content) VALUES (?1, ?2, ?3)")
                .bind(channel)
                .bind(role_str(t.role))
                .bind(&t.content)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
        }
        // Trim to the newest `max` turns for this channel.
        if self.max.is_some() {
            sqlx::query(
                "DELETE FROM turns WHERE channel = ?1 AND id NOT IN \
             (SELECT id FROM turns WHERE channel = ?1 ORDER BY id DESC LIMIT ?2)",
            )
            .bind(channel)
            .bind(self.max.unwrap_or(-1))
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        }
        tx.commit().await.map_err(db)?;
        Ok(())
    }
}

fn role_str(r: Role) -> &'static str {
    match r {
        Role::User => "user",
        Role::Assistant => "assistant",
    }
}

fn role_from(s: &str) -> Role {
    match s {
        "assistant" => Role::Assistant,
        _ => Role::User,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn persists_trims_and_isolates_channels() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history.db");

        // Open, write past the cap, close.
        {
            let h = SqliteHistory::open(&path, 2).await.unwrap();
            h.append(
                "a",
                &[Turn::user("1"), Turn::assistant("2"), Turn::user("3")],
            )
            .await
            .unwrap();
            h.append("b", &[Turn::user("x")]).await.unwrap();
        }

        // Reopen: the data survived, trimmed to the cap, oldest dropped first.
        let h = SqliteHistory::open(&path, 2).await.unwrap();
        let a = h.load("a").await;
        assert_eq!(a.len(), 2, "trimmed to cap");
        assert_eq!(a[0].content, "2", "oldest dropped first");
        assert_eq!(a[1].content, "3");
        assert_eq!(h.load("b").await.len(), 1, "channels are isolated");
        assert!(h.load("missing").await.is_empty());
    }
    #[tokio::test]
    async fn retained_context_preserves_messages_and_rejects_stale_compacts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history.db");
        let h = SqliteHistory::open_retained(&path).await.unwrap();
        h.append("a", &[Turn::user("one"), Turn::assistant("two")])
            .await
            .unwrap();
        h.append("b", &[Turn::user("other")]).await.unwrap();
        let first = h.context("a").await.unwrap();
        let boundary = first.through_id();
        h.append("a", &[Turn::user("arrived during compaction")])
            .await
            .unwrap();
        assert!(
            h.save_compact("a", None, boundary, "summary")
                .await
                .unwrap()
        );
        assert!(!h.save_compact("a", None, boundary, "stale").await.unwrap());
        assert!(
            !h.save_compact("b", None, boundary, "wrong channel")
                .await
                .unwrap()
        );
        let h = SqliteHistory::open_retained(path).await.unwrap();
        let current = h.context("a").await.unwrap();
        assert_eq!(current.compact.as_ref().unwrap().content, "summary");
        assert_eq!(current.messages.len(), 1);
        assert_eq!(
            current.messages[0].turn.content,
            "arrived during compaction"
        );
        assert_eq!(h.load("a").await.len(), 3, "original records are retained");
        assert_eq!(h.context("b").await.unwrap().messages.len(), 1);
        assert!(
            h.save_compact(
                "a",
                current.compact.as_ref().map(|c| c.id),
                current.through_id(),
                "next summary"
            )
            .await
            .unwrap()
        );
        assert!(h.context("a").await.unwrap().messages.is_empty());
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM compacts WHERE channel='a'")
            .fetch_one(&h.pool)
            .await
            .unwrap();
        assert_eq!(count.0, 2);
        let last = h.context("a").await.unwrap().compact.unwrap();
        assert!(
            h.save_compact("a", Some(last.id), last.through_id, "smaller summary")
                .await
                .unwrap()
        );
        assert_eq!(
            h.context("a").await.unwrap().compact.unwrap().content,
            "smaller summary"
        );
        assert_eq!(h.load("a").await.len(), 3);
    }
}
