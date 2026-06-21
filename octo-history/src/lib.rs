//! Per-channel conversation history — a pluggable backend, the way teloxide's
//! `Storage` is pluggable. **Not** agentic memory (that's a different thing);
//! this is just the rolling transcript of a channel that gets fed back to the
//! model each turn.
//!
//! A cogitator depends on the [`HistoryStore`] trait; the backend is chosen by
//! the app — in-memory (default), file, or — added the same way — redis. The
//! crate is **LLM-agnostic**: it stores neutral [`Turn`]s and has no dependency
//! on any model library. Converting a `Turn` into a particular chat-message type
//! (e.g. `rig::completion::Message`) belongs in the binding layer, not here.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Errors a history backend can raise (mirrors the `octo_core` style: explicit
/// variants with `#[from]`, no `anyhow`).
#[derive(Debug, Error)]
pub enum HistoryError {
    #[error("history io: {0}")]
    Io(#[from] std::io::Error),

    #[error("history json: {0}")]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, HistoryError>;

/// One conversation turn, backend-neutral and serialisable (so file/redis
/// backends can persist it).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Turn {
    pub role: Role,
    pub content: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    User,
    Assistant,
}

impl Turn {
    pub fn user(content: impl Into<String>) -> Self {
        Self { role: Role::User, content: content.into() }
    }
    pub fn assistant(content: impl Into<String>) -> Self {
        Self { role: Role::Assistant, content: content.into() }
    }
}

/// Pluggable per-channel history backend (in-memory / file / redis / …).
#[async_trait]
pub trait HistoryStore: Send + Sync {
    /// All stored turns for a channel, oldest → newest.
    async fn load(&self, channel: &str) -> Vec<Turn>;
    /// Append turns to a channel, trimming to the backend's cap.
    async fn append(&self, channel: &str, turns: &[Turn]) -> Result<()>;
}

/// In-memory backend (default). Lost on restart.
pub struct InMemoryHistory {
    inner: Mutex<HashMap<String, Vec<Turn>>>,
    max: usize,
}

impl InMemoryHistory {
    pub fn new(max: usize) -> Self {
        Self { inner: Mutex::new(HashMap::new()), max }
    }
}

#[async_trait]
impl HistoryStore for InMemoryHistory {
    async fn load(&self, channel: &str) -> Vec<Turn> {
        self.inner.lock().unwrap().get(channel).cloned().unwrap_or_default()
    }

    async fn append(&self, channel: &str, turns: &[Turn]) -> Result<()> {
        let mut g = self.inner.lock().unwrap();
        let entry = g.entry(channel.to_string()).or_default();
        entry.extend_from_slice(turns);
        trim(entry, self.max);
        Ok(())
    }
}

/// File backend: one JSON file per channel under `dir`. Survives restarts.
pub struct FileHistory {
    dir: PathBuf,
    max: usize,
}

impl FileHistory {
    pub fn new(dir: impl Into<PathBuf>, max: usize) -> Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        Ok(Self { dir, max })
    }

    fn path(&self, channel: &str) -> PathBuf {
        let safe: String = channel
            .chars()
            .map(|c| if c.is_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
            .collect();
        self.dir.join(format!("{safe}.json"))
    }
}

#[async_trait]
impl HistoryStore for FileHistory {
    async fn load(&self, channel: &str) -> Vec<Turn> {
        match tokio::fs::read(self.path(channel)).await {
            Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_default(),
            Err(_) => Vec::new(),
        }
    }

    async fn append(&self, channel: &str, turns: &[Turn]) -> Result<()> {
        let mut all = self.load(channel).await;
        all.extend_from_slice(turns);
        trim(&mut all, self.max);
        let bytes = serde_json::to_vec(&all)?;
        tokio::fs::write(self.path(channel), bytes).await?;
        Ok(())
    }
}

fn trim(turns: &mut Vec<Turn>, max: usize) {
    let overflow = turns.len().saturating_sub(max);
    if overflow > 0 {
        turns.drain(0..overflow);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn in_memory_appends_and_trims_per_channel() {
        let h = InMemoryHistory::new(2);
        h.append("a", &[Turn::user("1"), Turn::assistant("2"), Turn::user("3")])
            .await
            .unwrap();
        h.append("b", &[Turn::user("x")]).await.unwrap();
        let a = h.load("a").await;
        assert_eq!(a.len(), 2, "trimmed to cap");
        assert_eq!(a[0].content, "2", "oldest dropped first");
        assert_eq!(h.load("b").await.len(), 1, "channels are isolated");
        assert!(h.load("missing").await.is_empty());
    }
}
