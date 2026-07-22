//! SQLite-backed [`HistoryStore`] — a durable, migrated per-channel transcript
//! (opt-in behind the `sqlite` feature).
//!
//! Chosen over an ORM deliberately: the surface is one table and three trivial
//! statements (insert / trim / load-last-N), so hand-written SQL is clearer than a
//! schema DSL, and the async trait fits `sqlx` directly. Uses a **bundled**
//! libsqlite3 (no system dependency) and **runtime** queries (no `DATABASE_URL` at
//! build). Migrations under `migrations/` are embedded at compile time and run when
//! the store opens, so a schema change is a new `.sql` file, not a manual step.

use std::path::Path;

use async_trait::async_trait;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
    Pool, Sqlite,
};

use crate::{HistoryError, HistoryStore, Result, Role, Turn};

fn db(e: impl std::fmt::Display) -> HistoryError {
    HistoryError::Db(e.to_string())
}

/// A per-channel transcript persisted in a SQLite file, capped at `max` turns per
/// channel. Migrations run automatically at [`open`](Self::open).
pub struct SqliteHistory {
    pool: Pool<Sqlite>,
    max: i64,
}

impl SqliteHistory {
    /// Open (creating the file + running migrations if needed) a SQLite history.
    pub async fn open(path: impl AsRef<Path>, max: usize) -> Result<Self> {
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
        sqlx::migrate!("./migrations").run(&pool).await.map_err(db)?;
        Ok(Self { pool, max: max.max(1) as i64 })
    }
}

#[async_trait]
impl HistoryStore for SqliteHistory {
    async fn load(&self, channel: &str) -> Vec<Turn> {
        // Newest `max` for the channel, returned oldest -> newest.
        let rows: Vec<(String, String)> = match sqlx::query_as(
            "SELECT role, content FROM turns WHERE channel = ?1 ORDER BY id DESC LIMIT ?2",
        )
        .bind(channel)
        .bind(self.max)
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
            .map(|(role, content)| Turn { role: role_from(&role), content })
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
        sqlx::query(
            "DELETE FROM turns WHERE channel = ?1 AND id NOT IN \
             (SELECT id FROM turns WHERE channel = ?1 ORDER BY id DESC LIMIT ?2)",
        )
        .bind(channel)
        .bind(self.max)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
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
            h.append("a", &[Turn::user("1"), Turn::assistant("2"), Turn::user("3")])
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
}
