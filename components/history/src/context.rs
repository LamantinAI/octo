//! Neutral retained-history checkpoints. Summarization policy belongs to the caller.
use serde::{Deserialize, Serialize};

use crate::Turn;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredTurn {
    pub id: i64,
    pub turn: Turn,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Compact {
    pub id: i64,
    pub through_id: i64,
    pub content: String,
}

#[derive(Clone, Debug, Default)]
pub struct ContextWindow {
    pub compact: Option<Compact>,
    pub messages: Vec<StoredTurn>,
}
impl ContextWindow {
    pub fn through_id(&self) -> i64 {
        self.messages
            .last()
            .map(|m| m.id)
            .or_else(|| self.compact.as_ref().map(|c| c.through_id))
            .unwrap_or(0)
    }
}
