use teloxide::payloads::SendMessageSetters;

use octo_core::Blob;
use teloxide::{
    Bot,
    requests::Requester,
    types::{ChatId, ParseMode},
};

use crate::{
    api::{is_unsupported, send_rich_markdown},
    format::{
        has_media, media_as_links, sanitize_rich, split_for_telegram, split_rich, strip_tags,
        to_telegram_html,
    },
};

/// How an outgoing file is presented in Telegram.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Outbound {
    /// `sendPhoto`: an inline preview.
    Photo,
    /// `sendVoice`: the play-in-place voice bubble. Telegram only renders OGG/Opus
    /// this way — an MP3 or M4A sent as a voice note comes back as an error.
    Voice,
    /// `sendDocument`: everything else.
    Document,
}

/// Classify an outgoing file by its extension. The name is ours (a workspace path
/// or a caller-chosen `filename`), not a remote-controlled string.
pub(super) fn outbound_kind(filename: &str) -> Outbound {
    let lower = filename.to_ascii_lowercase();
    let has = |exts: &[&str]| exts.iter().any(|ext| lower.ends_with(ext));
    if has(&[".png", ".jpg", ".jpeg", ".webp", ".gif"]) {
        Outbound::Photo
    } else if has(&[".ogg", ".oga", ".opus"]) {
        Outbound::Voice
    } else {
        Outbound::Document
    }
}

/// An audio `Blob` that Telegram will accept as a voice note: OGG/Opus by MIME, or
/// by the filename it was labelled with.
pub(super) fn is_voice_blob(blob: &Blob) -> bool {
    blob.content_type().starts_with("audio/ogg")
        || blob
            .filename()
            .is_some_and(|f| outbound_kind(f) == Outbound::Voice)
}

/// Send a model reply to a chat: a rich message where the server has them
/// (Markdown as-is, laid out by Telegram), dropping to the pre-10.1 HTML
/// renderer and then to plain text if it doesn't — so a reply always lands, and
/// only its formatting degrades.
pub(super) async fn send_reply(bot: &Bot, chat: ChatId, text: &str, rich_messages: &mut bool) {
    if !*rich_messages {
        send_html(bot, chat, text).await;
        return;
    }
    for chunk in split_rich(text) {
        let markdown = sanitize_rich(&chunk);
        match send_rich_markdown(bot, chat, &markdown).await {
            Ok(()) => tracing::info!(%chat, "sent reply (rich)"),
            Err(e) => {
                if is_unsupported(&e) {
                    tracing::warn!(error = %e, "telegram: server has no rich messages; using the HTML renderer from here on");
                    *rich_messages = false;
                    send_html(bot, chat, &chunk).await;
                    continue;
                }
                // Telegram fetches every media URL itself, and one it can't fetch
                // fails the whole message. Try once more with the media as links,
                // so the text still arrives rich and the pictures stay one tap away.
                if has_media(&chunk) {
                    tracing::warn!(error = %e, "telegram rich send with media failed; retrying with the media as links");
                    let linked = media_as_links(&chunk);
                    match send_rich_markdown(bot, chat, &sanitize_rich(&linked)).await {
                        Ok(()) => {
                            tracing::info!(%chat, "sent reply (rich, media as links)");
                            continue;
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "telegram rich send failed; falling back to HTML")
                        }
                    }
                    send_html(bot, chat, &linked).await;
                    continue;
                }
                tracing::warn!(error = %e, "telegram rich send failed; falling back to HTML");
                send_html(bot, chat, &chunk).await;
            }
        }
    }
}

/// The pre-10.1 path, and the fallback under a rejected rich send: the model's
/// Markdown rendered into Telegram's HTML subset, split to the message-length
/// limit, and stripped to plain text if the Bot API rejects a chunk's HTML.
pub(super) async fn send_html(bot: &Bot, chat: ChatId, text: &str) {
    let html = to_telegram_html(text);
    for chunk in split_for_telegram(&html) {
        let sent = bot
            .send_message(chat, chunk.clone())
            .parse_mode(ParseMode::Html)
            .await;
        match sent {
            Ok(_) => tracing::info!(%chat, "sent reply"),
            Err(e) => {
                tracing::warn!(error = %e, "telegram HTML send failed; retrying as plain text");
                let plain = strip_tags(&chunk);
                if let Err(e2) = bot.send_message(chat, plain).await {
                    tracing::warn!(error = %e2, "telegram plain send failed");
                }
            }
        }
    }
}
