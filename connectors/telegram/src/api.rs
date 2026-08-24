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

use serde_json::{json, Value};
use teloxide::{types::ChatId, Bot};

/// Send Markdown as a rich message. `Err` carries a description for the log and
/// for deciding whether to drop back to the HTML renderer — never the URL, which
/// holds the bot token.
pub(crate) async fn send_rich_markdown(bot: &Bot, chat: ChatId, markdown: &str) -> Result<(), String> {
    let url = method_url(bot, "sendRichMessage");
    let body = json!({
        "chat_id": chat.0,
        "rich_message": { "markdown": markdown },
    });
    let response = bot
        .client()
        .post(url)
        .json(&body)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let status = response.status();
    let payload: Value = response.json().await.map_err(|e| e.to_string())?;
    if payload.get("ok").and_then(Value::as_bool) == Some(true) {
        return Ok(());
    }
    let description = payload.get("description").and_then(Value::as_str).unwrap_or("no description");
    Err(format!("{status}: {description}"))
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
