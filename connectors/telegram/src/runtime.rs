use std::{
    sync::Arc,
    time::{Duration, Instant},
};
mod media;
mod pending;
use self::{
    media::receive_media,
    pending::{MediaInput, MediaQueue},
};

use async_trait::async_trait;
use futures::StreamExt;
use octo_core::{
    Blob, Connector, ConnectorCapabilities, ConnectorContext, ConnectorId, Envelope, EventKind,
    Filter, OctoError, OctoResult, SubscribeOptions,
};
use octo_workspace::WorkspaceError;
use serde_json::{Value, to_string};
use teloxide::{
    Bot,
    requests::Requester,
    types::{AllowedUpdate, ChatId, InputFile, UpdateKind},
    update_listeners::{AsUpdateStream, Polling},
};
use tokio::{
    pin, select, spawn,
    time::{MissedTickBehavior, interval},
};
use tracing::{info, warn};

use super::{
    ALLOW_CHAT, FLUSH_TICK, GROUP_MODE, LIST_CHATS, REMOVE_CHAT, SEND_FILE, STATUS, TYPING,
    TelegramConnector,
    control::{handle_control, migrate, outgoing_allowed, owner_chats, register_add},
    delivery::handle_file,
    inbound::{chat_envelope, coalesce_key, publish_flush, reply_context, with_reply},
    outbound::{is_voice_blob, send_reply},
};
use crate::{
    AllowReplyContext, DeliveryStatus, NativeFrame,
    approval::{Cards, handle_decision, message_proof},
    batch::{Batcher, Emit, Flush},
    commands::{SET_COMMANDS, set_commands},
    fs::{save_incoming, workspace_root},
    live::Live,
    policy::delivery::{Delivery, notice},
    policy::tracking::MessageRef,
    policy::{ingress::admit, tracking::Tracker},
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

        let cards = Cards::new(self.approval_cards.clone());
        let tracker = Tracker::new(
            self.policy
                .as_ref()
                .map(|p| p.tracking_limits())
                .unwrap_or_default(),
        );
        let out_cards = cards.clone();
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
        let out_live = live.clone();
        let out_tracker = tracker.clone();
        let out_policy = self.policy.clone();
        spawn(async move {
            // Latched off the first time the server says it has no rich messages
            // (an older self-hosted Bot API), so replies stop paying for a call
            // that can't succeed. A per-message rejection doesn't clear it.
            let mut rich_messages = true;
            loop {
                select! {
                    reply = replies.next() => match reply {
                        Some(env) => {
                            if out_live.handle_end(&out_bot, &env) {continue}
                            let delivery=Delivery{bot:&out_bot,tracker:&out_tracker,policy:out_policy.as_ref(),envelope:&env,trusted_notice:false};
                            if !delivery.allowed() && env.kind.as_str() != SEND_FILE {continue}
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
                                    warn!(error = %e, "telegram: failed to publish set_commands result");
                                }
                                continue;
                            }
                            if env.kind.as_str() == SEND_FILE {
                                handle_file(&out_bot, &out_workspace, &out_acl, &out_groups, &out_id, &env, &out_ctx,Some(&delivery)).await;
                                continue;
                            }
                            if !outgoing_allowed(&out_acl, &out_groups, &env) {
                                warn!("telegram: outbound destination is not allowed");
                                continue;
                            }
                            // The chat id rides on the envelope's channel.
                            let Some(chat) = env.channel.as_ref().and_then(|c| c.as_str().parse::<i64>().ok()) else {
                                warn!(kind = %env.kind, "outbound without a numeric channel; dropped");
                                continue;
                            };
                            let chat_id = ChatId(chat);

                            // Live-turn feedback: typing indicator / status trace.
                            match env.kind.as_str() {
                                TYPING => {
                                    out_live.start_typing(out_bot.clone(), chat_id,&env,out_policy.clone(),out_tracker.clone());
                                    continue;
                                }
                                STATUS => {
                                    if let Some(line) = env.payload_as::<String>() {
                                        out_live.status(&out_bot, chat_id, line,&delivery).await;
                                    }
                                    continue;
                                }
                                _ => {}
                            }

                            if out_cards.present_scoped(&out_bot,&out_id,&out_acl,&env,Some(&delivery)).await {continue;}

                            // The turn's reply is going out — stop typing, delete
                            // the status trace.
                            out_live.end_turn(&out_bot, chat_id,&env);

                            // A media payload → photo/voice/document; a String → text.
                            if let Some(blob) = env.payload_as::<Blob>() {
                                let file = InputFile::memory(blob.bytes().clone())
                                    .file_name(blob.filename().unwrap_or("file").to_string());
                                let sent = if blob.is_image() {
                                    out_bot.send_photo(chat_id, file).await
                                } else if is_voice_blob(blob) {
                                    out_bot.send_voice(chat_id, file).await
                                } else {
                                    out_bot.send_document(chat_id, file).await
                                };
                                match sent {
                                    Ok(sent) => {delivery.report(chat,&[MessageRef{chat_id:sent.chat.id.0,message_id:sent.id.0,date:sent.date.timestamp()}],DeliveryStatus::Sent).await;info!(chat, ct = blob.content_type(), "sent media");}
                                    Err(e) => {delivery.report(chat,&[],DeliveryStatus::Unknown).await;warn!(error = %e, "telegram media send failed");}
                                }
                            } else if let Some(text) = env.payload_as::<String>() {
                                send_reply(&out_bot, chat_id, text, &mut rich_messages,Some(&delivery)).await;
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
        let mut media = MediaQueue::default();
        let mut flush_tick = interval(FLUSH_TICK);
        flush_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut listener = Polling::builder(bot.clone())
            .timeout(Duration::from_secs(10))
            .allowed_updates(vec![
                AllowedUpdate::Message,
                AllowedUpdate::MyChatMember,
                AllowedUpdate::CallbackQuery,
            ])
            .delete_webhook()
            .await
            .build();
        let stream = listener.as_stream();
        // PollingStream is !Unpin; pin it on the stack to poll in select!.
        pin!(stream);
        info!(connector = %self.id, "telegram polling started");
        loop {
            select! {
                Some(ready) = media.next(), if !media.is_empty() => {
                    let input = ready.input;
                    if !input.source.allowed(self.policy.as_ref(), &self.id, &input.chat) { continue; }
                    if let Some((emit, coalescible)) = ready.result {
                        if input.buffer && coalescible {
                            if let Emit::Image { blob, caption } = emit {
                                let key = input.key.expect("buffered media key");
                                batcher.push_image(key.clone(), &input.chat, blob, caption, input.trust, Instant::now());
                                batcher.annotate(&key, input.source);
                            }
                        } else {
                            publish_flush(&self.id, &ctx, Flush { source: Some(input.source), chat: input.chat, trust: input.trust, emit }, self.policy.as_ref()).await;
                        }
                    }
                }
                _ = flush_tick.tick() => {
                    for flush in batcher.drain_due(Instant::now()) {
                        publish_flush(&self.id, &ctx, flush,self.policy.as_ref()).await;
                    }
                }
                update = stream.next() => match update {
                    Some(Ok(update)) => {
                        let update_id=update.id.0;
                        if let UpdateKind::MyChatMember(change) = &update.kind {
                            register_add(&self.acl, &groups, change);
                        }
                        if let UpdateKind::CallbackQuery(query)=update.kind {
                            cards.callback(&bot,&self.id,&self.acl,&self.authority,query,&ctx,self.approval_timeout).await;
                            continue;
                        }
                        if let UpdateKind::Message(msg) = update.kind {
                            if migrate(&self.acl, &msg) { continue; }
                            let chat = msg.chat.id.0.to_string();
                            let perception = {
                                let acl = self.acl.as_ref().map(|state| state.acl.read().unwrap());
                                perceive(&msg, acl.as_deref(), &groups, bot_id, &username, &self.commands.read().unwrap())
                            };
                            let Some((trust, mut source)) = perception else {
                                warn!(%chat, "telegram: chat or sender denied");
                                continue;
                            };
                            source.proof = self.authority.as_ref().and_then(|authority|
                                message_proof(authority, &self.id, &msg, trust.map(|t| t.0)));
                            let base=coalesce_key(&msg,&chat);
                            if let Some(base)=&base {
                                if self.policy.is_some()&&(batcher.addressed_base(base)||media.addressed(base)){source.tags.insert("addressed".into(),"true".into());}
                                source.tags.insert("native_coalesce_key".into(),base.clone());
                            }
                            source.tags.insert("update_id".into(),update_id.to_string());
                            source.tags.insert("message_date".into(),msg.date.timestamp().to_string());
                            let frame=match NativeFrame::from_message(&self.id,bot_id.0,update_id,&msg,&source){Ok(frame)=>frame,Err(_)=>continue};
                            let provisional=source.clone().apply(chat_envelope(&self.id,&chat,msg.text().or_else(||msg.caption()).unwrap_or_default().to_string(),None,trust));
                            if !admit(&self,&bot,&ctx,&tracker,&live,frame,&mut source,provisional).await{continue}
                            if !source.allowed(self.policy.as_ref(),&self.id,&chat){continue}
                            if source.scope.as_ref().is_some_and(|s|s.ephemeral)&&msg.text().is_none(){
                                notice(&bot,&tracker,self.policy.as_ref(),&self.id,msg.chat.id.0,"Media and files are not supported in temporary chats yet.".into(),source.scope.clone()).await;
                                continue;
                            }
                            if handle_decision(&self.authority, source.proof.as_ref(), &bot,
                                &self.id, &msg, &username, &ctx, self.approval_timeout).await { continue; }
                            let now = Instant::now();
                            // Coalesce only what Telegram groups: an album shares a
                            // media_group_id; a forward burst carries forward_origin
                            // (no batch id, so group per chat). Everything else is
                            // emitted immediately — zero latency for normal chat.
                            let coalesce_key = base.clone().map(|base|match &source.scope {
                                Some(scope)=>to_string(&(&scope.key,&scope.epoch,source.tags.get("sender_id"),source.tags.get("role"),base)).expect("string tuple"),
                                None=>base,
                            });
                            let buffer = batcher.enabled() && coalesce_key.is_some()&&!source.scope.as_ref().is_some_and(|s|s.ephemeral);
                            // A reply carries the message it answers — fold a short context
                            // of that quoted message into the perceived text/caption so the
                            // cogitator sees what is being replied to (#17).
                            let reply = if self.policy.is_none()||source.pinned.as_ref().is_some_and(|e|e.context.get::<AllowReplyContext>().is_some()){reply_context(&msg)}else{None};
                            if let Some(text) = msg.text() {
                                if !source.scope.as_ref().is_some_and(|s|s.ephemeral){info!(%chat, "recv: {text}");}
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
                                    },self.policy.as_ref()).await;
                                }
                            } else if self.policy.is_some() {
                                if media.full() {
                                    notice(&bot,&tracker,self.policy.as_ref(),&self.id,msg.chat.id.0,"Too many media inputs are pending. Please try again shortly.".into(),source.scope.clone()).await;
                                } else {
                                    media.push(self.clone(), bot.clone(), msg, MediaInput { source, chat, native_base: base, key: coalesce_key, buffer, trust }, reply);
                                }
                            } else if let Some((emit, coalescible)) = receive_media(&self, &bot, &msg, &source, &reply).await {
                                if buffer && coalescible {
                                    if let Emit::Image { blob, caption } = emit {
                                        batcher.push_image(coalesce_key.clone().unwrap(), &chat, blob, caption, trust, now);
                                        batcher.annotate(coalesce_key.as_ref().unwrap(), source.clone());
                                    }
                                } else {
                                    publish_flush(&self.id, &ctx, Flush { source: Some(source.clone()), chat: chat.clone(), trust, emit }, self.policy.as_ref()).await;
                                }
                            }
                        }
                    }
                    Some(Err(e)) => warn!(error = %e, "telegram update error"),
                    None => {
                        for flush in batcher.drain_all() {
                            publish_flush(&self.id, &ctx, flush,self.policy.as_ref()).await;
                        }
                        return Ok(());
                    }
                },
                _ = ctx.shutdown.cancelled() => {
                    for flush in batcher.drain_all() {
                        publish_flush(&self.id, &ctx, flush,self.policy.as_ref()).await;
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
    fn save_incoming(&self, filename: &str, bytes: &[u8]) -> Result<String, WorkspaceError> {
        let root = workspace_root(&self.workspace)?;
        save_incoming(&root, filename, bytes)
    }
}
