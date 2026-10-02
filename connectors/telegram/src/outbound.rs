use std::path::PathBuf;
use teloxide::payloads::{
    SendDocumentSetters, SendMessageSetters, SendPhotoSetters, SendVoiceSetters,
};

use octo_core::{Blob, Envelope};
use serde_json::Value;
use teloxide::{
    Bot,
    requests::Requester,
    types::{ChatId, InputFile, ParseMode},
};

use crate::{
    api::{is_unsupported, send_rich_markdown},
    format::{sanitize_rich, split_for_telegram, split_rich, strip_tags, to_telegram_html},
    fs::{load_outgoing, workspace_root},
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
                } else {
                    tracing::warn!(error = %e, "telegram rich send failed; falling back to HTML");
                }
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

/// Handle `chat.send_file`: load a file from the shared workspace by its path and
/// send it — a photo for images, a voice note for OGG/Opus audio, a document
/// otherwise, with an optional `caption`.
/// Chat id comes from the payload `chat`, else
/// the envelope's channel. Bytes never pass through the model — the payload only
/// names a path.
pub(super) async fn send_workspace_file(bot: &Bot, workspace: &Option<PathBuf>, env: &Envelope) {
    let params = env.payload_as::<Value>().cloned().unwrap_or(Value::Null);
    let Some(path) = params.get("path").and_then(Value::as_str) else {
        tracing::warn!("chat.send_file without a `path`; dropped");
        return;
    };
    let chat = params.get("chat").and_then(Value::as_i64).or_else(|| {
        env.channel
            .as_ref()
            .and_then(|c| c.as_str().parse::<i64>().ok())
    });
    let Some(chat) = chat else {
        tracing::warn!("chat.send_file without a chat id; dropped");
        return;
    };
    let root = match workspace_root(workspace) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "chat.send_file: workspace unavailable");
            return;
        }
    };
    let (bytes, name) = match load_outgoing(&root, path) {
        Ok(x) => x,
        Err(e) => {
            tracing::warn!(error = %e, %path, "chat.send_file: cannot read workspace file");
            return;
        }
    };
    let filename = params
        .get("filename")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or(name);
    let caption = params
        .get("caption")
        .and_then(Value::as_str)
        .map(str::to_string);
    // An image goes as a photo (inline preview) and OGG/Opus as a voice note (the
    // play-in-place bubble) rather than a document. Judge by extension — the workspace
    // filename is ours, not a remote-controlled string.
    let kind = outbound_kind(&filename);
    let file = InputFile::memory(bytes).file_name(filename);
    let sent = match kind {
        Outbound::Photo => {
            let mut req = bot.send_photo(ChatId(chat), file);
            if let Some(c) = caption {
                req = req.caption(c);
            }
            req.await.map(|_| ())
        }
        Outbound::Voice => {
            let mut req = bot.send_voice(ChatId(chat), file);
            if let Some(c) = caption {
                req = req.caption(c);
            }
            req.await.map(|_| ())
        }
        Outbound::Document => {
            let mut req = bot.send_document(ChatId(chat), file);
            if let Some(c) = caption {
                req = req.caption(c);
            }
            req.await.map(|_| ())
        }
    };
    match sent {
        Ok(_) => tracing::info!(chat, %path, kind = ?kind, "sent file"),
        Err(e) => tracing::warn!(error = %e, "telegram send_file failed"),
    }
}
