use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use futures::StreamExt;
use octo_core::{
    Blob, Connector, ConnectorCapabilities, ConnectorContext, ConnectorId, Envelope, EventKind,
    Filter, OctoError, OctoResult, SubscribeOptions,
};
use serde_json::Value;
use teloxide::{
    Bot,
    requests::Requester,
    types::{AllowedUpdate, ChatId, InputFile, UpdateKind},
    update_listeners::{AsUpdateStream, Polling},
};

use super::{
    ALLOW_CHAT, FLUSH_TICK, GROUP_MODE, LIST_CHATS, REMOVE_CHAT, SEND_FILE, STATUS, TYPING,
    TelegramConnector,
    control::{handle_control, migrate, outgoing_allowed, owner_chats, register_add},
    delivery::handle_file,
    inbound::{
        caption_with_saved, coalesce_key, download_bytes, hms, image_document, inbox_name,
        publish_flush, reply_context, video_caption, video_media, voice_media, with_reply,
    },
    outbound::{is_voice_blob, send_reply},
};
use crate::{
    batch::{Batcher, Emit, Flush},
    commands::{SET_COMMANDS, set_commands},
    fs::{save_incoming, workspace_root},
    live::Live,
    source::perceive,
};

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
        let me = bot
            .get_me()
            .await
            .map_err(|error| OctoError::Connector(error.to_string()))?;
        let mut groups = self.groups.clone();
        groups.address_names.push(me.user.first_name.clone());
        let username = me.username().to_string();
        let bot_id = me.user.id;

        // ── Outbound: chat.reply → Telegram message ──────────────────────────
        let mut replies = ctx
            .subscribe(
                Filter::by_target(self.id.clone()),
                SubscribeOptions::default(),
            )
            .await?;
        let out_bot = bot.clone();
        let out_shutdown = ctx.shutdown.clone();
        let out_ctx = ctx.clone();
        let out_acl = self.acl.clone();
        let out_id = self.id.clone();
        let out_workspace = self.workspace.clone();
        let out_groups = groups.clone();
        let commands = self.commands.clone();
        let live = Live::new();
        tokio::spawn(async move {
            // Latched off the first time the server says it has no rich messages
            // (an older self-hosted Bot API), so replies stop paying for a call
            // that can't succeed. A per-message rejection doesn't clear it.
            let mut rich_messages = true;
            loop {
                tokio::select! {
                    reply = replies.next() => match reply {
                        Some(env) => {
                            // Control commands mutate the ACL; everything else is
                            // an outbound message to send.
                            if matches!(env.kind.as_str(), ALLOW_CHAT | REMOVE_CHAT | LIST_CHATS | GROUP_MODE) {
                                handle_control(&out_acl, &out_groups, &out_id, &env, &out_ctx).await;
                                continue;
                            }
                            // The bot's command menu (set by the assembly at startup).
                            if env.kind.as_str() == SET_COMMANDS {
                                if env.tags.get("control_plane").map(String::as_str) != Some("true") { continue; }
                                let owners = owner_chats(&out_acl);
                                let payload = env.payload_as::<Value>().cloned().unwrap_or(Value::Null);
                                commands.write().unwrap().update(&payload);
                                let result = set_commands(&out_bot, &owners, &payload).await;
                                let resp = Envelope::new(out_id.clone(), EventKind::new(format!("{SET_COMMANDS}.result")), result)
                                    .with_correlation(env.id);
                                if let Err(e) = out_ctx.publish(resp).await {
                                    tracing::warn!(error = %e, "telegram: failed to publish set_commands result");
                                }
                                continue;
                            }
                            if env.kind.as_str() == SEND_FILE {
                                handle_file(&out_bot, &out_workspace, &out_acl, &out_groups, &out_id, &env, &out_ctx).await;
                                continue;
                            }
                            if !outgoing_allowed(&out_acl, &out_groups, &env) {
                                tracing::warn!("telegram: outbound destination is not allowed");
                                continue;
                            }
                            // The chat id rides on the envelope's channel.
                            let Some(chat) = env.channel.as_ref().and_then(|c| c.as_str().parse::<i64>().ok()) else {
                                tracing::warn!(kind = %env.kind, "outbound without a numeric channel; dropped");
                                continue;
                            };
                            let chat_id = ChatId(chat);

                            // Live-turn feedback: typing indicator / status trace.
                            match env.kind.as_str() {
                                TYPING => {
                                    live.start_typing(out_bot.clone(), chat_id);
                                    continue;
                                }
                                STATUS => {
                                    if let Some(line) = env.payload_as::<String>() {
                                        live.status(&out_bot, chat_id, line).await;
                                    }
                                    continue;
                                }
                                _ => {}
                            }

                            // The turn's reply is going out — stop typing, delete
                            // the status trace.
                            live.end_turn(&out_bot, chat_id);

                            // A media payload → photo/voice/document; a String → text.
                            if let Some(blob) = env.payload_as::<Blob>() {
                                let file = InputFile::memory(blob.bytes().clone())
                                    .file_name(blob.filename().unwrap_or("file").to_string());
                                let sent = if blob.is_image() {
                                    out_bot.send_photo(chat_id, file).await.map(|_| ())
                                } else if is_voice_blob(blob) {
                                    out_bot.send_voice(chat_id, file).await.map(|_| ())
                                } else {
                                    out_bot.send_document(chat_id, file).await.map(|_| ())
                                };
                                match sent {
                                    Ok(_) => tracing::info!(chat, ct = blob.content_type(), "sent media"),
                                    Err(e) => tracing::warn!(error = %e, "telegram media send failed"),
                                }
                            } else if let Some(text) = env.payload_as::<String>() {
                                send_reply(&out_bot, chat_id, text, &mut rich_messages).await;
                            }
                        }
                        None => break,
                    },
                    _ = out_shutdown.cancelled() => break,
                }
            }
        });

        // ── Inbound: long-poll updates → chat.message ────────────────────────
        // A per-chat Batcher coalesces rapid/forwarded bursts into one input; a
        // periodic tick flushes buffers whose quiet window has elapsed.
        let mut batcher = Batcher::new(self.debounce, self.max_wait);
        let mut flush_tick = tokio::time::interval(FLUSH_TICK);
        flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut listener = Polling::builder(bot.clone())
            .timeout(Duration::from_secs(10))
            .allowed_updates(vec![AllowedUpdate::Message, AllowedUpdate::MyChatMember])
            .delete_webhook()
            .await
            .build();
        let stream = listener.as_stream();
        // PollingStream is !Unpin; pin it on the stack to poll in select!.
        tokio::pin!(stream);
        tracing::info!(connector = %self.id, "telegram polling started");
        loop {
            tokio::select! {
                _ = flush_tick.tick() => {
                    for flush in batcher.drain_due(Instant::now()) {
                        publish_flush(&self.id, &ctx, flush).await;
                    }
                }
                update = stream.next() => match update {
                    Some(Ok(update)) => {
                        if let UpdateKind::MyChatMember(change) = &update.kind {
                            register_add(&self.acl, &groups, change);
                        }
                        if let UpdateKind::Message(msg) = update.kind {
                            if migrate(&self.acl, &msg) { continue; }
                            let chat = msg.chat.id.0.to_string();
                            let perception = {
                                let acl = self.acl.as_ref().map(|state| state.acl.read().unwrap());
                                perceive(&msg, acl.as_deref(), &groups, bot_id, &username, &self.commands.read().unwrap())
                            };
                            let Some((trust, source)) = perception else {
                                tracing::warn!(%chat, "telegram: chat or sender denied");
                                continue;
                            };
                            let now = Instant::now();
                            // Coalesce only what Telegram groups: an album shares a
                            // media_group_id; a forward burst carries forward_origin
                            // (no batch id, so group per chat). Everything else is
                            // emitted immediately — zero latency for normal chat.
                            let coalesce_key = coalesce_key(&msg, &chat);
                            let buffer = batcher.enabled() && coalesce_key.is_some();
                            // A reply carries the message it answers — fold a short context
                            // of that quoted message into the perceived text/caption so the
                            // cogitator sees what is being replied to (#17).
                            let reply = reply_context(&msg);
                            if let Some(text) = msg.text() {
                                tracing::info!(%chat, "recv: {text}");
                                let body =
                                    with_reply(&reply, Some(text.to_string())).unwrap_or_default();
                                if buffer {
                                    batcher.push_text(
                                        coalesce_key.clone().unwrap(), &chat, body, trust, now,
                                    );
                                    batcher.annotate(coalesce_key.as_ref().unwrap(), source.clone());
                                } else {
                                    publish_flush(&self.id, &ctx, Flush {
                                        source: Some(source.clone()),
                                        chat: chat.clone(),
                                        trust,
                                        emit: Emit::Text { text: body, caption: None },
                                    }).await;
                                }
                            } else if let Some(photo) = msg.photo().and_then(<[_]>::last) {
                                // Largest size is last. Download bytes → Blob so a
                                // (vision) cogitator can perceive the image.
                                match download_bytes(&bot, &photo.file.id).await {
                                    Ok(bytes) => {
                                        tracing::info!(%chat, bytes = bytes.len(), "recv: photo");
                                        // Also persist a copy to the workspace inbox: the
                                        // Blob is ephemeral (it lives only for the turn), so
                                        // without this the image can't be saved, forwarded,
                                        // or referenced later. Perception is unaffected if
                                        // the save fails — the Blob still goes out.
                                        let saved = self
                                            .save_incoming(&inbox_name("photo.jpg"), &bytes)
                                            .map_err(|e| tracing::warn!(error = %e, "failed to save incoming photo"))
                                            .ok();
                                        let blob = Blob::new(bytes, "image/jpeg")
                                            .with_filename("photo.jpg");
                                        let caption = caption_with_saved(
                                            with_reply(&reply, msg.caption().map(str::to_string)),
                                            saved.as_deref(),
                                        );
                                        if buffer {
                                            batcher.push_image(
                                                coalesce_key.clone().unwrap(), &chat, blob, caption, trust, now,
                                            );
                                            batcher.annotate(coalesce_key.as_ref().unwrap(), source.clone());
                                        } else {
                                            publish_flush(&self.id, &ctx, Flush {
                                        source: Some(source.clone()),
                                                chat: chat.clone(),
                                                trust,
                                                emit: Emit::Image { blob, caption },
                                            }).await;
                                        }
                                    }
                                    Err(e) => tracing::warn!(error = %e, "telegram photo download failed"),
                                }
                            } else if let Some((doc, mime)) = image_document(&msg) {
                                // An image sent "as a file" (uncompressed): perceived as
                                // a photo (the vision Blob, keeping its real MIME) AND
                                // saved to the workspace inbox so it persists — same as a
                                // compressed photo above. Emitted immediately.
                                match download_bytes(&bot, &doc.file.id).await {
                                    Ok(bytes) => {
                                        tracing::info!(%chat, bytes = bytes.len(), %mime, "recv: image document");
                                        let fname =
                                            doc.file_name.clone().unwrap_or_else(|| "image".into());
                                        let saved = self
                                            .save_incoming(&inbox_name(&fname), &bytes)
                                            .map_err(|e| tracing::warn!(error = %e, "failed to save incoming image document"))
                                            .ok();
                                        let blob = Blob::new(bytes, mime).with_filename(fname);
                                        let caption = caption_with_saved(
                                            with_reply(&reply, msg.caption().map(str::to_string)),
                                            saved.as_deref(),
                                        );
                                        publish_flush(&self.id, &ctx, Flush {
                                        source: Some(source.clone()),
                                            chat: chat.clone(),
                                            trust,
                                            emit: Emit::Image { blob, caption },
                                        }).await;
                                    }
                                    Err(e) => tracing::warn!(error = %e, "telegram document download failed"),
                                }
                            } else if let Some(audio) = voice_media(&msg) {
                                // A voice note or an audio file → downloaded into a
                                // Blob so a hearing cogitator can transcribe it,
                                // the same way a photo reaches a vision one. Audio
                                // sent as a plain *document* deliberately keeps the
                                // workspace route below: long recordings belong to
                                // a tool, not to the turn itself. Emitted immediately
                                // (Telegram never groups voice into an album).
                                match download_bytes(&bot, audio.file_id).await {
                                    Ok(bytes) => {
                                        tracing::info!(
                                            %chat, bytes = bytes.len(), secs = audio.duration_secs,
                                            "recv: voice"
                                        );
                                        // Persist the raw audio to the workspace inbox too:
                                        // transcription is perception, but the file itself
                                        // should survive so it can be kept, forwarded, or
                                        // moved to storage. Save failure doesn't stop the
                                        // transcription turn.
                                        let path = self
                                            .save_incoming(&inbox_name(&audio.filename), &bytes)
                                            .map_err(|e| tracing::warn!(error = %e, "failed to save incoming voice"))
                                            .ok();
                                        let blob = Blob::new(bytes, audio.mime)
                                            .with_filename(audio.filename);
                                        publish_flush(&self.id, &ctx, Flush {
                                        source: Some(source.clone()),
                                            chat: chat.clone(),
                                            trust,
                                            emit: Emit::Audio {
                                                blob,
                                                caption: with_reply(&reply, msg.caption().map(str::to_string)),
                                                duration_secs: Some(audio.duration_secs),
                                                path,
                                            },
                                        }).await;
                                    }
                                    Err(e) => tracing::warn!(error = %e, "telegram voice download failed"),
                                }
                            } else if let Some(video) = video_media(&msg) {
                                // A video clip or a round video note. The bytes never
                                // travel to the model — no model perceives video, and
                                // clips are large — so the file lands in the workspace
                                // for a tool (ffprobe/ffmpeg, transcription) while
                                // Telegram's OWN thumbnail rides along as an `Image`
                                // blob, so a vision cogitator sees what arrived instead
                                // of a bare path. Emitted immediately (Telegram groups
                                // video into an album only with photos, handled above).
                                match download_bytes(&bot, video.file_id).await {
                                    Ok(bytes) => {
                                        let saved = self
                                            .save_incoming(&inbox_name(&video.filename), &bytes)
                                            .map_err(|e| tracing::warn!(error = %e, "failed to save incoming video"))
                                            .ok();
                                        tracing::info!(
                                            %chat, bytes = bytes.len(), secs = video.duration_secs,
                                            rel = saved.as_deref().unwrap_or("-"), "recv: video"
                                        );
                                        let caption = video_caption(
                                            &video,
                                            with_reply(&reply, msg.caption().map(str::to_string)),
                                            saved.as_deref(),
                                        );
                                        // The poster frame, when Telegram sent one — a
                                        // failed thumbnail download must not cost us the
                                        // whole turn, so it degrades to plain text.
                                        let thumb = match video.thumbnail {
                                            Some(t) => download_bytes(&bot, &t.file.id).await.ok(),
                                            None => None,
                                        };
                                        let emit = match thumb {
                                            Some(bytes) => Emit::Image {
                                                blob: Blob::new(bytes, "image/jpeg")
                                                    .with_filename("video-thumb.jpg"),
                                                caption: Some(caption),
                                            },
                                            None => Emit::Text { text: caption, caption: None },
                                        };
                                        publish_flush(&self.id, &ctx, Flush {
                                        source: Some(source.clone()),
                                            chat: chat.clone(),
                                            trust,
                                            emit,
                                        }).await;
                                    }
                                    Err(e) => {
                                        // The Bot API caps downloads at 20 MB, so a long
                                        // clip fails right here. Say so: staying silent
                                        // reads as the bot ignoring the message.
                                        tracing::warn!(error = %e, "telegram video download failed");
                                        publish_flush(&self.id, &ctx, Flush {
                                        source: Some(source.clone()),
                                            chat: chat.clone(),
                                            trust,
                                            emit: Emit::Text {
                                                text: format!(
                                                    "[received a video ({}) but could not download it \
                                                     — Telegram caps bot downloads at 20 MB]",
                                                    hms(video.duration_secs)
                                                ),
                                                caption: None,
                                            },
                                        }).await;
                                    }
                                }
                            } else if let Some(doc) = msg.document() {
                                // Any other file → saved into the shared workspace;
                                // the cogitator is handed its path (bytes by
                                // reference, never through the model). Emitted
                                // immediately.
                                let name = doc.file_name.clone().unwrap_or_else(|| "file".into());
                                match download_bytes(&bot, &doc.file.id).await {
                                    Ok(bytes) => match self.save_incoming(&name, &bytes) {
                                        Ok(rel) => {
                                            tracing::info!(%chat, %rel, bytes = bytes.len(), "recv: file");
                                            let text = format!(
                                                "[received file `{name}` — saved to workspace path `{rel}`]"
                                            );
                                            publish_flush(&self.id, &ctx, Flush {
                                        source: Some(source.clone()),
                                                chat: chat.clone(),
                                                trust,
                                                emit: Emit::Text { text, caption: None },
                                            }).await;
                                        }
                                        Err(e) => tracing::warn!(error = %e, "failed to save incoming file"),
                                    },
                                    Err(e) => tracing::warn!(error = %e, "telegram file download failed"),
                                }
                            }
                        }
                    }
                    Some(Err(e)) => tracing::warn!(error = %e, "telegram update error"),
                    None => {
                        for flush in batcher.drain_all() {
                            publish_flush(&self.id, &ctx, flush).await;
                        }
                        return Ok(());
                    }
                },
                _ = ctx.shutdown.cancelled() => {
                    for flush in batcher.drain_all() {
                        publish_flush(&self.id, &ctx, flush).await;
                    }
                    return Ok(());
                }
            }
        }
    }
}

impl TelegramConnector {
    /// Save an incoming file into the shared workspace's inbox, returning its
    /// workspace-relative path.
    fn save_incoming(
        &self,
        filename: &str,
        bytes: &[u8],
    ) -> Result<String, octo_workspace::WorkspaceError> {
        let root = workspace_root(&self.workspace)?;
        save_incoming(&root, filename, bytes)
    }
}
