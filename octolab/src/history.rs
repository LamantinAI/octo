//! Bridge to the `octo-history` crate: re-exports the neutral history types and
//! adds the only LLM-specific bit octolab needs — converting stored [`Turn`]s
//! into the `rig` chat messages the model expects. The store itself (trait +
//! in-memory/file backends) lives in `octo-history`, binding-agnostic.

pub use octo_history::{FileHistory, HistoryStore, InMemoryHistory, Role, Turn};

use rig::completion::Message;

/// Convert stored turns into the `rig` history the model expects.
pub fn to_messages(turns: &[Turn]) -> Vec<Message> {
    turns
        .iter()
        .map(|t| match t.role {
            Role::User => Message::user(t.content.clone()),
            Role::Assistant => Message::assistant(t.content.clone()),
        })
        .collect()
}
