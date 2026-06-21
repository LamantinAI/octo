//! `octo-connector-telegram` — bidirectional connector over the Telegram Bot
//! API (teloxide long polling).
//!
//! Speaks the runtime's generic chat shapes, so any cogitator works unchanged —
//! only the `channel` carries transport detail: here it's the `chat_id`. Inbound
//! text messages become `chat.message` (with `reply_to = chat_id`); `chat.reply`
//! envelopes targeted at us are sent back to their `channel`'s chat (a `Blob`
//! payload → photo/document, a `String` → text).

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use octo_core::{
    Blob, ChannelId, Connector, ConnectorCapabilities, ConnectorContext, ConnectorId, Envelope,
    EventKind, Filter, OctoResult, ReplyChannel, SubscribeOptions,
};
use teloxide::net::Download;
use teloxide::prelude::*;
use teloxide::types::{ChatId, InputFile, UpdateKind};
use teloxide::update_listeners::{polling_default, AsUpdateStream};

pub struct TelegramConnector {
    id: ConnectorId,
    capabilities: ConnectorCapabilities,
    token: String,
}

impl TelegramConnector {
    pub fn new(id: impl Into<String>, token: impl Into<String>) -> Arc<Self> {
        let capabilities = ConnectorCapabilities::bidirectional()
            .with_emit_kinds([EventKind::from_static("chat.message")])
            .with_accept_kinds([EventKind::from_static("chat.reply")]);
        Arc::new(Self {
            id: ConnectorId::new(id),
            capabilities,
            token: token.into(),
        })
    }
}

#[async_trait]
impl Connector for TelegramConnector {
    fn id(&self) -> &ConnectorId {
        &self.id
    }

    fn capabilities(&self) -> &ConnectorCapabilities {
        &self.capabilities
    }

    async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
        let bot = Bot::new(self.token.clone());

        // ── Outbound: chat.reply → Telegram message ──────────────────────────
        let mut replies = ctx
            .subscribe(Filter::by_target(self.id.clone()), SubscribeOptions::default())
            .await?;
        let out_bot = bot.clone();
        let out_shutdown = ctx.shutdown.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    reply = replies.next() => match reply {
                        Some(env) => {
                            // The chat id rides on the envelope's channel.
                            let Some(chat) = env.channel.as_ref().and_then(|c| c.as_str().parse::<i64>().ok()) else {
                                tracing::warn!("chat.reply without a numeric channel; dropped");
                                continue;
                            };
                            let chat_id = ChatId(chat);

                            // A media payload → photo/document; a String → text.
                            if let Some(blob) = env.payload_as::<Blob>() {
                                let file = InputFile::memory(blob.bytes().clone())
                                    .file_name(blob.filename().unwrap_or("file").to_string());
                                let sent = if blob.is_image() {
                                    out_bot.send_photo(chat_id, file).await.map(|_| ())
                                } else {
                                    out_bot.send_document(chat_id, file).await.map(|_| ())
                                };
                                match sent {
                                    Ok(_) => tracing::info!(chat, ct = blob.content_type(), "sent media"),
                                    Err(e) => tracing::warn!(error = %e, "telegram media send failed"),
                                }
                            } else if let Some(text) = env.payload_as::<String>() {
                                match out_bot.send_message(chat_id, text.clone()).await {
                                    Ok(_) => tracing::info!(chat, "sent reply"),
                                    Err(e) => tracing::warn!(error = %e, "telegram send failed"),
                                }
                            }
                        }
                        None => break,
                    },
                    _ = out_shutdown.cancelled() => break,
                }
            }
        });

        // ── Inbound: long-poll updates → chat.message ────────────────────────
        let mut listener = polling_default(bot.clone()).await;
        let stream = listener.as_stream();
        // PollingStream is !Unpin; pin it on the stack to poll in select!.
        tokio::pin!(stream);
        tracing::info!(connector = %self.id, "telegram polling started");
        loop {
            tokio::select! {
                update = stream.next() => match update {
                    Some(Ok(update)) => {
                        if let UpdateKind::Message(msg) = update.kind {
                            let chat = msg.chat.id.0.to_string();
                            if let Some(text) = msg.text() {
                                tracing::info!(%chat, "recv: {text}");
                                let env = chat_envelope(
                                    &self.id,
                                    &chat,
                                    text.to_string(),
                                    msg.caption(),
                                );
                                if let Err(e) = ctx.publish(env).await {
                                    tracing::warn!(error = %e, "failed to publish chat.message");
                                }
                            } else if let Some(photo) = msg.photo().and_then(<[_]>::last) {
                                // Largest size is last. Download bytes → Blob so a
                                // (vision) cogitator can perceive the image.
                                match download_bytes(&bot, &photo.file.id).await {
                                    Ok(bytes) => {
                                        tracing::info!(%chat, bytes = bytes.len(), "recv: photo");
                                        let blob = Blob::new(bytes, "image/jpeg")
                                            .with_filename("photo.jpg");
                                        let env = chat_envelope(
                                            &self.id,
                                            &chat,
                                            blob,
                                            msg.caption(),
                                        );
                                        if let Err(e) = ctx.publish(env).await {
                                            tracing::warn!(error = %e, "failed to publish photo chat.message");
                                        }
                                    }
                                    Err(e) => tracing::warn!(error = %e, "telegram photo download failed"),
                                }
                            }
                        }
                    }
                    Some(Err(e)) => tracing::warn!(error = %e, "telegram update error"),
                    None => return Ok(()),
                },
                _ = ctx.shutdown.cancelled() => return Ok(()),
            }
        }
    }
}

/// Build an inbound `chat.message` envelope: the payload is the text (a `String`)
/// or the image (a `Blob`); the `chat_id` rides on both the channel and the
/// reply recommendation, and a non-empty caption is attached as a `caption` tag.
fn chat_envelope<P: std::any::Any + Send + Sync>(
    id: &ConnectorId,
    chat: &str,
    payload: P,
    caption: Option<&str>,
) -> Envelope {
    let mut env = Envelope::new(id.clone(), EventKind::from_static("chat.message"), payload)
        .with_channel(ChannelId::new(chat.to_string()))
        .with_reply_to(ReplyChannel::new(ChannelId::new(chat.to_string())));
    if let Some(cap) = caption.filter(|c| !c.is_empty()) {
        env = env.with_tag("caption", cap);
    }
    env
}

/// Resolve a Telegram `file_id` and download its bytes.
async fn download_bytes(
    bot: &Bot,
    file_id: &teloxide::types::FileId,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let file = bot.get_file(file_id.clone()).await?;
    let mut bytes: Vec<u8> = Vec::new();
    bot.download_file(&file.path, &mut bytes).await?;
    Ok(bytes)
}
