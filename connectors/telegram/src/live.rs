//! Live-turn feedback — what the chat shows *while* the cogitator is thinking.
//!
//! Two envelope kinds drive it (both addressed to this connector, the chat id
//! on the `channel`, like `chat.reply`):
//!
//! - `chat.typing` (payload ignored) → the Telegram "typing…" indicator, kept
//!   alive by re-sending `sendChatAction` until the turn's reply arrives (the
//!   Bot API shows it ~5 s per call, so a loop refreshes it), bounded so an
//!   abandoned turn can't type forever.
//! - `chat.status` (payload `String`) → one *status message* per turn that
//!   accumulates progress lines ("→ tool …") by editing itself in place — the
//!   openclaw-style live trace of tool use, without flooding the chat.
//!
//! A `chat.reply` ends the turn: the typing loop stops and the status message
//! (if any) is deleted — the trace is transient scaffolding, not conversation.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use teloxide::{
    prelude::*,
    types::{ChatAction, ChatId, MessageId, ParseMode},
};
use tokio::task::JoinHandle;

use crate::format;

/// Refresh cadence for the typing indicator (the Bot API shows ~5 s per call).
const TYPING_REFRESH: Duration = Duration::from_millis(4500);
/// Upper bound on one turn's typing loop — an abandoned turn stops typing here.
const TYPING_MAX_REFRESHES: u32 = 130; // ≈ 10 minutes
/// Grow the status message up to here, then start a new one (Telegram caps a
/// message at 4096 UTF-16 units; stay well under with the `<i>` wrapper).
const STATUS_MAX_LEN: usize = 3500;
/// One status line is clipped to this many chars — it's a progress trace, not a log.
const STATUS_LINE_MAX: usize = 300;

/// The turn's status trace: every Telegram message it has occupied — rollover past
/// the size cap starts a new one but the earlier ids are kept so the end of the
/// turn can delete the WHOLE trace — plus the accumulated (escaped) lines of the
/// tail message (the one still being edited in place).
#[derive(Clone)]
struct StatusMsg {
    ids: Vec<MessageId>,
    lines: String,
}

/// Per-chat live-turn state, shared by the connector's outbound loop.
#[derive(Default)]
pub(crate) struct Live {
    typing: Mutex<HashMap<i64, JoinHandle<()>>>,
    status: Mutex<HashMap<i64, StatusMsg>>,
}

impl Live {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Start (or restart) the typing indicator for a chat.
    pub(crate) fn start_typing(&self, bot: Bot, chat: ChatId) {
        let mut map = self.typing.lock().unwrap();
        if let Some(old) = map.remove(&chat.0) {
            old.abort();
        }
        let handle = tokio::spawn(async move {
            for _ in 0..TYPING_MAX_REFRESHES {
                if bot.send_chat_action(chat, ChatAction::Typing).await.is_err() {
                    break;
                }
                tokio::time::sleep(TYPING_REFRESH).await;
            }
        });
        map.insert(chat.0, handle);
    }

    /// Append a progress line to the turn's status message (creating or rolling
    /// it over as needed). Lines render italic; the message is edited in place.
    pub(crate) async fn status(&self, bot: &Bot, chat: ChatId, line: &str) {
        let line = format::esc(&clip(line, STATUS_LINE_MAX));
        let existing = self.status.lock().unwrap().get(&chat.0).cloned();

        // Try to grow the tail message in place.
        if let Some(cur) = existing {
            if let Some(&tail) = cur.ids.last() {
                if cur.lines.len() + line.len() < STATUS_MAX_LEN {
                    let lines = format!("{}\n{line}", cur.lines);
                    let edited = bot
                        .edit_message_text(chat, tail, format!("<i>{lines}</i>"))
                        .parse_mode(ParseMode::Html)
                        .await;
                    if edited.is_ok() {
                        let mut map = self.status.lock().unwrap();
                        if let Some(entry) = map.get_mut(&chat.0) {
                            entry.lines = lines;
                        }
                        return;
                    }
                    // Edit failed (message deleted / too old) → fall through to a new one.
                }
            }
        }

        // Fresh status message (first of the turn, or a rollover past the size cap).
        // Earlier ids are KEPT so end_turn deletes the whole trace, not just the tail.
        match bot
            .send_message(chat, format!("<i>{line}</i>"))
            .parse_mode(ParseMode::Html)
            .await
        {
            Ok(sent) => {
                let mut map = self.status.lock().unwrap();
                let entry = map
                    .entry(chat.0)
                    .or_insert_with(|| StatusMsg { ids: Vec::new(), lines: String::new() });
                entry.ids.push(sent.id);
                entry.lines = line;
            }
            Err(e) => tracing::warn!(chat = chat.0, error = %e, "telegram status send failed"),
        }
    }

    /// The turn's reply is out: stop typing and delete the status message — the
    /// trace is scaffolding for the wait, not part of the conversation.
    pub(crate) fn end_turn(&self, bot: &Bot, chat: ChatId) {
        if let Some(h) = self.typing.lock().unwrap().remove(&chat.0) {
            h.abort();
        }
        if let Some(status) = self.status.lock().unwrap().remove(&chat.0) {
            let bot = bot.clone();
            tokio::spawn(async move {
                // Delete every message the trace occupied — rollover may have split it
                // across several; the behaviour must be uniform for short and long turns.
                for id in status.ids {
                    if let Err(e) = bot.delete_message(chat, id).await {
                        tracing::warn!(chat = chat.0, error = %e, "telegram status delete failed");
                    }
                }
            });
        }
    }
}

/// Clip to at most `max` chars on a char boundary, marking the cut with an ellipsis.
fn clip(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let cut: String = s.chars().take(max).collect();
    format!("{cut}…")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clip_respects_char_boundaries() {
        assert_eq!(clip("привет", 10), "привет");
        assert_eq!(clip("привет мир", 6), "привет…");
    }
}
