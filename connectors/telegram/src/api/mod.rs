//! Bot API calls teloxide doesn't have yet.
//!
//! Rich Messages landed in Bot API 10.1 (June 2026); teloxide 0.17 — the latest
//! release, and the newest thing on its master branch — stops at 9.2, so
//! `sendRichMessage` has no typed builder to call. The request is a plain JSON
//! POST, so it is issued directly, reusing the bot's own token, API URL and
//! `reqwest` client rather than standing up a second connection pool.
//!
//! Everything else still goes through teloxide. When a typed builder appears,
//! this module is what gets deleted.

use crate::policy::tracking::MessageRef;
use serde_json::{Value, json};
use std::fmt::{Display, Formatter, Result as FmtResult};
use teloxide::{Bot, types::ChatId};

pub(crate) enum RichError {
    Rejected(String),
    Unknown,
}
impl RichError {
    pub(crate) fn unknown(&self) -> bool {
        matches!(self, Self::Unknown)
    }
    pub(crate) fn description(&self) -> &str {
        match self {
            Self::Rejected(message) => message,
            Self::Unknown => "Telegram delivery outcome is unknown",
        }
    }
}
impl Display for RichError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        self.description().fmt(f)
    }
}

/// Send Markdown as a rich message. `Err` carries a description for the log and
/// for deciding whether to drop back to the HTML renderer — never the URL, which
/// holds the bot token.
pub(crate) async fn send_rich_markdown(
    bot: &Bot,
    chat: ChatId,
    markdown: &str,
) -> Result<Option<MessageRef>, RichError> {
    let url = method_url(bot, "sendRichMessage");
    let body = json!({
        "chat_id": chat.0,
        "rich_message": { "markdown": markdown },
    });
    // `without_url` before stringifying: a reqwest error prints the URL it was
    // for, and this one carries the bot token — a transport hiccup must not put
    // it in the log.
    let response = bot
        .client()
        .post(url)
        .json(&body)
        .send()
        .await
        .map_err(|_| RichError::Unknown)?;
    let status = response.status();
    let payload: Value = response.json().await.map_err(|_| RichError::Unknown)?;
    if payload.get("ok").and_then(Value::as_bool) == Some(true) {
        let message = payload.get("result");
        return Ok(message.and_then(|m| {
            Some(MessageRef {
                chat_id: m.get("chat")?.get("id")?.as_i64()?,
                message_id: i32::try_from(m.get("message_id")?.as_i64()?).ok()?,
                date: m.get("date")?.as_i64()?,
            })
        }));
    }
    let description = payload
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or("no description");
    if payload.get("ok").and_then(Value::as_bool) != Some(false) {
        return Err(RichError::Unknown);
    }
    Err(RichError::Rejected(format!("{status}: {description}")))
}

/// The endpoint for a method on the bot's own API server — a `Url` always
/// carries at least a `/` path, so the base joins straight onto `bot<token>`.
fn method_url(bot: &Bot, method: &str) -> String {
    format!("{}bot{}/{method}", bot.api_url(), bot.token())
}

/// Whether an error means this server has no rich messages at all (an older
/// self-hosted Bot API), as opposed to this one message being bad. The first is
/// permanent — worth latching so every reply doesn't pay for a doomed call.
pub(crate) fn is_unsupported(error: &str) -> bool {
    let lower = error.to_lowercase();
    lower.contains("404") || lower.contains("method not found") || lower.contains("unknown method")
}

#[cfg(test)]
mod tests {
    use teloxide::Bot;

    use super::{is_unsupported, method_url};

    #[test]
    fn distinguishes_a_missing_method_from_a_bad_message() {
        assert!(is_unsupported("404 Not Found: method not found"));
        assert!(!is_unsupported("400 Bad Request: RICH_MESSAGE_TOO_LONG"));
    }

    #[test]
    fn method_url_is_built_without_a_doubled_or_missing_slash() {
        let url = method_url(&Bot::new("123:FAKE"), "sendRichMessage");
        assert_eq!(url, "https://api.telegram.org/bot123:FAKE/sendRichMessage");
    }
}
