//! `octo-connector-telegram` — bidirectional connector over the Telegram Bot
//! API (teloxide long polling).
//!
//! Speaks the runtime's generic chat shapes, so any cogitator works unchanged —
//! only the `channel` carries transport detail: here it's the `chat_id`. Inbound
//! text messages become `chat.message` (with `reply_to = chat_id`); `chat.reply`
//! envelopes targeted at us are sent back to their `channel`'s chat (a `Blob`
//! payload → photo/document, a `String` → text).
//!
//! **Authorization** is an optional per-chat allow-list ([`Acl`]): a message from
//! a chat not on the list is dropped at the edge — before the bus — so untrusted
//! input never reaches cognition. Listed chats get their trust gradient + role
//! stamped onto the envelope ([`ChannelMetadata`]). Constructed in code
//! ([`TelegramConnector::new`] / [`with_acl`](TelegramConnector::with_acl)) or
//! from a `type = "telegram"` manifest via [`factory`] — the secret token stays
//! in the environment, the ACL in a JSON state file named by the manifest.

mod acl;
mod batch;
mod format;
mod fs;

use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::StreamExt;
use octo_core::{
    Blob, ChannelId, ChannelMetadata, Connector, ConnectorCapabilities, ConnectorContext,
    ConnectorFactory, ConnectorId, Envelope, EventKind, FactoryContext, Filter, OctoResult,
    ReplyChannel, SubscribeOptions, TrustLevel,
};
use serde::Deserialize;
use serde_json::{json, Value};
use teloxide::net::Download;
use teloxide::prelude::*;
use teloxide::types::{ChatId, InputFile, ParseMode, UpdateKind};
use teloxide::update_listeners::{polling_default, AsUpdateStream};

use crate::batch::{Batcher, Emit, Flush};

pub use acl::{Acl, AclEntry, Role};

/// Default quiet window before a coalescing burst (album / forward) is flushed.
/// Short, because these bursts arrive machine-fast; single typed messages are
/// never buffered, so this adds no latency to normal chat.
const DEFAULT_DEBOUNCE: Duration = Duration::from_millis(500);
/// Default cap on how long a buffer may stay open (so a steady stream still
/// flushes) — see [`batch::Batcher`].
const DEFAULT_MAX_WAIT: Duration = Duration::from_secs(3);
/// How often the run loop checks for buffers due to flush.
const FLUSH_TICK: Duration = Duration::from_millis(100);

/// Control commands that mutate the ACL at runtime (an owner-instructed
/// cogitator dispatches these — the connector is a manageable actor, like the
/// scheduler). Each gets a correlated `<kind>.result` reply.
const ALLOW_CHAT: &str = "octo.telegram.allow_chat";
const REMOVE_CHAT: &str = "octo.telegram.remove_chat";
const LIST_CHATS: &str = "octo.telegram.list_chats";

/// Outbound command: send a file from the shared workspace as a document.
/// Payload `{ path, chat?, filename? }` — chat falls back to the envelope channel.
const SEND_FILE: &str = "chat.send_file";

/// Shared, mutable ACL state: the list behind a lock + where it persists.
struct AclState {
    acl: RwLock<Acl>,
    /// JSON file to persist to on mutation (`None` → in-memory only).
    path: Option<PathBuf>,
}

pub struct TelegramConnector {
    id: ConnectorId,
    capabilities: ConnectorCapabilities,
    token: String,
    /// Channel allow-list. `None` → no authorization (allow all — the
    /// console-fallback / playground path). `Some` → messages from unlisted
    /// chats are dropped at the edge, listed chats get `trust`/`role` stamped,
    /// and `octo.telegram.*` control commands mutate it at runtime.
    acl: Option<Arc<AclState>>,
    /// Coalescing window: a chat's rapid burst of messages is buffered and
    /// flushed as one input after `debounce` of quiet (capped at `max_wait`), so
    /// forwarding several messages yields one turn, not one per message. A zero
    /// `debounce` disables coalescing (publish each message immediately).
    debounce: Duration,
    max_wait: Duration,
    /// Shared workspace root for file transfer (inbound documents saved here,
    /// `chat.send_file` reads from here). `None` → resolved from the environment
    /// at use, matching octo-code. See [`fs`].
    workspace: Option<PathBuf>,
}

impl TelegramConnector {
    /// Open with no access control — every chat is allowed. Fine for a local
    /// playground; a real deployment uses [`with_acl`](Self::with_acl).
    pub fn new(id: impl Into<String>, token: impl Into<String>) -> Arc<Self> {
        Self::build(id, token, None, DEFAULT_DEBOUNCE, DEFAULT_MAX_WAIT, None)
    }

    /// Open gated by an access-control list: a message from a chat not on the
    /// list is dropped before it reaches the bus (so untrusted input never
    /// reaches cognition). `acl_path`, when set, is where runtime mutations
    /// (`allow_chat` / `remove_chat`) are persisted.
    pub fn with_acl(
        id: impl Into<String>,
        token: impl Into<String>,
        acl: Acl,
        acl_path: Option<PathBuf>,
    ) -> Arc<Self> {
        let state = Arc::new(AclState { acl: RwLock::new(acl), path: acl_path });
        Self::build(id, token, Some(state), DEFAULT_DEBOUNCE, DEFAULT_MAX_WAIT, None)
    }

    fn build(
        id: impl Into<String>,
        token: impl Into<String>,
        acl: Option<Arc<AclState>>,
        debounce: Duration,
        max_wait: Duration,
        workspace: Option<PathBuf>,
    ) -> Arc<Self> {
        let capabilities = ConnectorCapabilities::bidirectional()
            .with_emit_kinds([EventKind::from_static("chat.message")])
            .with_accept_kinds([
                EventKind::from_static("chat.reply"),
                EventKind::from_static(SEND_FILE),
                EventKind::from_static(ALLOW_CHAT),
                EventKind::from_static(REMOVE_CHAT),
                EventKind::from_static(LIST_CHATS),
            ]);
        Arc::new(Self {
            id: ConnectorId::new(id),
            capabilities,
            token: token.into(),
            acl,
            debounce,
            max_wait,
            workspace,
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
        let out_ctx = ctx.clone();
        let out_acl = self.acl.clone();
        let out_id = self.id.clone();
        let out_workspace = self.workspace.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    reply = replies.next() => match reply {
                        Some(env) => {
                            // Control commands mutate the ACL; everything else is
                            // an outbound message to send.
                            if matches!(env.kind.as_str(), ALLOW_CHAT | REMOVE_CHAT | LIST_CHATS) {
                                handle_control(&out_acl, &out_id, &env, &out_ctx).await;
                                continue;
                            }
                            // Send a file from the shared workspace (by reference).
                            if env.kind.as_str() == SEND_FILE {
                                send_workspace_file(&out_bot, &out_workspace, &env).await;
                                continue;
                            }
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
                                // Render the model's Markdown to Telegram HTML so it
                                // shows formatted (not raw `**`/`#`/tables), split to
                                // the message-length limit, and fall back to plain
                                // text if the Bot API rejects a chunk's HTML.
                                let html = format::to_telegram_html(text);
                                for chunk in format::split_for_telegram(&html) {
                                    let sent = out_bot
                                        .send_message(chat_id, chunk.clone())
                                        .parse_mode(ParseMode::Html)
                                        .await;
                                    match sent {
                                        Ok(_) => tracing::info!(chat, "sent reply"),
                                        Err(e) => {
                                            tracing::warn!(error = %e, "telegram HTML send failed; retrying as plain text");
                                            let plain = format::strip_tags(&chunk);
                                            if let Err(e2) = out_bot.send_message(chat_id, plain).await {
                                                tracing::warn!(error = %e2, "telegram plain send failed");
                                            }
                                        }
                                    }
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
        // A per-chat Batcher coalesces rapid/forwarded bursts into one input; a
        // periodic tick flushes buffers whose quiet window has elapsed.
        let mut batcher = Batcher::new(self.debounce, self.max_wait);
        let mut flush_tick = tokio::time::interval(FLUSH_TICK);
        flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut listener = polling_default(bot.clone()).await;
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
                        if let UpdateKind::Message(msg) = update.kind {
                            let chat = msg.chat.id.0.to_string();
                            // Authorization at the edge: a chat not on the ACL is
                            // dropped here, before the bus — untrusted input never
                            // reaches cognition. `None` ACL = allow all; otherwise
                            // the chat's role/trust is stamped onto the envelope.
                            let trust = match &self.acl {
                                Some(state) => match state.acl.read().unwrap().role(msg.chat.id.0) {
                                    Some(role) => Some((role, role.trust())),
                                    None => {
                                        tracing::warn!(%chat, "telegram: message from unlisted chat — dropped");
                                        continue;
                                    }
                                },
                                None => None,
                            };
                            let now = Instant::now();
                            // Coalesce only what Telegram groups: an album shares a
                            // media_group_id; a forward burst carries forward_origin
                            // (no batch id, so group per chat). Everything else is
                            // emitted immediately — zero latency for normal chat.
                            let coalesce_key = coalesce_key(&msg, &chat);
                            let buffer = batcher.enabled() && coalesce_key.is_some();
                            if let Some(text) = msg.text() {
                                tracing::info!(%chat, "recv: {text}");
                                if buffer {
                                    batcher.push_text(
                                        coalesce_key.unwrap(), &chat, text.to_string(), trust, now,
                                    );
                                } else {
                                    publish_flush(&self.id, &ctx, Flush {
                                        chat: chat.clone(),
                                        trust,
                                        emit: Emit::Text { text: text.to_string(), caption: None },
                                    }).await;
                                }
                            } else if let Some(photo) = msg.photo().and_then(<[_]>::last) {
                                // Largest size is last. Download bytes → Blob so a
                                // (vision) cogitator can perceive the image.
                                match download_bytes(&bot, &photo.file.id).await {
                                    Ok(bytes) => {
                                        tracing::info!(%chat, bytes = bytes.len(), "recv: photo");
                                        let blob = Blob::new(bytes, "image/jpeg")
                                            .with_filename("photo.jpg");
                                        let caption = msg.caption().map(str::to_string);
                                        if buffer {
                                            batcher.push_image(
                                                coalesce_key.unwrap(), &chat, blob, caption, trust, now,
                                            );
                                        } else {
                                            publish_flush(&self.id, &ctx, Flush {
                                                chat: chat.clone(),
                                                trust,
                                                emit: Emit::Image { blob, caption },
                                            }).await;
                                        }
                                    }
                                    Err(e) => tracing::warn!(error = %e, "telegram photo download failed"),
                                }
                            } else if let Some(doc) = msg.document() {
                                // A file → saved into the shared workspace; the
                                // cogitator is handed its path (bytes by reference,
                                // never through the model). Emitted immediately.
                                let name = doc.file_name.clone().unwrap_or_else(|| "file".into());
                                match download_bytes(&bot, &doc.file.id).await {
                                    Ok(bytes) => match self.save_incoming(&name, &bytes) {
                                        Ok(rel) => {
                                            tracing::info!(%chat, %rel, bytes = bytes.len(), "recv: file");
                                            let text = format!(
                                                "[received file `{name}` — saved to workspace path `{rel}`]"
                                            );
                                            publish_flush(&self.id, &ctx, Flush {
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
        let root = fs::workspace_root(&self.workspace)?;
        fs::save_incoming(&root, filename, bytes)
    }
}

/// Handle `chat.send_file`: load a file from the shared workspace by its path and
/// send it as a Telegram document. Chat id comes from the payload `chat`, else
/// the envelope's channel. Bytes never pass through the model — the payload only
/// names a path.
async fn send_workspace_file(bot: &Bot, workspace: &Option<PathBuf>, env: &Envelope) {
    let params = env.payload_as::<Value>().cloned().unwrap_or(Value::Null);
    let Some(path) = params.get("path").and_then(Value::as_str) else {
        tracing::warn!("chat.send_file without a `path`; dropped");
        return;
    };
    let chat = params
        .get("chat")
        .and_then(Value::as_i64)
        .or_else(|| env.channel.as_ref().and_then(|c| c.as_str().parse::<i64>().ok()));
    let Some(chat) = chat else {
        tracing::warn!("chat.send_file without a chat id; dropped");
        return;
    };
    let root = match fs::workspace_root(workspace) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "chat.send_file: workspace unavailable");
            return;
        }
    };
    let (bytes, name) = match fs::load_outgoing(&root, path) {
        Ok(x) => x,
        Err(e) => {
            tracing::warn!(error = %e, %path, "chat.send_file: cannot read workspace file");
            return;
        }
    };
    let filename = params.get("filename").and_then(Value::as_str).map(str::to_string).unwrap_or(name);
    let file = InputFile::memory(bytes).file_name(filename);
    match bot.send_document(ChatId(chat), file).await {
        Ok(_) => tracing::info!(chat, %path, "sent file"),
        Err(e) => tracing::warn!(error = %e, "telegram send_document failed"),
    }
}

/// The coalescing key for a message, or `None` to emit it immediately.
///
/// Telegram groups an album under one `media_group_id`; a forwarded burst has no
/// batch id, so forwarded messages are grouped per chat. A plain typed message
/// (the common case) returns `None` and is published with zero added latency.
fn coalesce_key(msg: &teloxide::types::Message, chat: &str) -> Option<String> {
    if let Some(group) = msg.media_group_id() {
        Some(format!("mg:{}", group.0))
    } else if msg.forward_origin().is_some() {
        Some(format!("fwd:{chat}"))
    } else {
        None
    }
}

/// Publish a flushed batch as one `chat.message` envelope.
async fn publish_flush(id: &ConnectorId, ctx: &ConnectorContext, flush: Flush) {
    let Flush { chat, trust, emit } = flush;
    let env = match emit {
        Emit::Text { text, caption } => {
            chat_envelope(id, &chat, text, caption.as_deref(), trust)
        }
        Emit::Image { blob, caption } => {
            chat_envelope(id, &chat, blob, caption.as_deref(), trust)
        }
        Emit::Multipart(msg) => chat_envelope(id, &chat, msg, None, trust),
    };
    if let Err(e) = ctx.publish(env).await {
        tracing::warn!(error = %e, "failed to publish chat.message");
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
    trust: Option<(Role, TrustLevel)>,
) -> Envelope {
    let mut env = Envelope::new(id.clone(), EventKind::from_static("chat.message"), payload)
        .with_channel(ChannelId::new(chat.to_string()))
        .with_reply_to(ReplyChannel::new(ChannelId::new(chat.to_string())));
    if let Some(cap) = caption.filter(|c| !c.is_empty()) {
        env = env.with_tag("caption", cap);
    }
    // Front-load authorization: the trust gradient (generic reflex gating) plus
    // the precise role (capability checks live downstream).
    if let Some((role, level)) = trust {
        env = env.with_channel_metadata(
            ChannelMetadata::new().with_trust(level).with_tag("role", role.as_str()),
        );
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

/// Handle an `octo.telegram.*` control command: mutate the ACL, persist it, and
/// publish a correlated `<kind>.result` reply. Authorization (only the owner may
/// run these) is enforced *upstream* by the dispatching cogitator — the command
/// reaching here is already vouched for; the connector just applies it.
async fn handle_control(
    acl: &Option<Arc<AclState>>,
    id: &ConnectorId,
    env: &Envelope,
    ctx: &ConnectorContext,
) {
    let Some(state) = acl else {
        tracing::warn!(kind = %env.kind, "telegram: control command but no ACL configured; ignored");
        return;
    };
    let payload = env.payload_as::<Value>().cloned().unwrap_or(Value::Null);
    let chat_id = payload.get("chat_id").and_then(Value::as_i64);

    let result = match env.kind.as_str() {
        ALLOW_CHAT => match chat_id {
            Some(chat_id) => {
                let role = match payload.get("role").and_then(Value::as_str) {
                    Some("owner") => Role::Owner,
                    _ => Role::Trusted,
                };
                let added = {
                    let mut acl = state.acl.write().unwrap();
                    let added = acl.insert(chat_id, role);
                    persist(&acl, &state.path);
                    added
                };
                tracing::info!(chat_id, role = role.as_str(), added, "telegram: allow_chat");
                json!({ "ok": true, "chat_id": chat_id, "role": role.as_str(), "added": added })
            }
            None => json!({ "ok": false, "error": "missing or non-integer chat_id" }),
        },
        REMOVE_CHAT => match chat_id {
            Some(chat_id) => {
                let removed = {
                    let mut acl = state.acl.write().unwrap();
                    let removed = acl.remove(chat_id);
                    persist(&acl, &state.path);
                    removed
                };
                tracing::info!(chat_id, removed, "telegram: remove_chat");
                json!({ "ok": true, "chat_id": chat_id, "removed": removed })
            }
            None => json!({ "ok": false, "error": "missing or non-integer chat_id" }),
        },
        LIST_CHATS => {
            let chats = state.acl.read().unwrap().entries();
            json!({ "ok": true, "chats": chats })
        }
        _ => json!({ "ok": false, "error": "unknown command" }),
    };

    let resp = Envelope::new(
        id.clone(),
        EventKind::new(format!("{}.result", env.kind.as_str())),
        result,
    )
    .with_correlation(env.id);
    if let Err(e) = ctx.publish(resp).await {
        tracing::warn!(error = %e, "telegram: failed to publish control result");
    }
}

/// Persist the ACL to its file if one is configured, logging (not failing) on error.
fn persist(acl: &Acl, path: &Option<PathBuf>) {
    if let Some(p) = path {
        if let Err(e) = acl.save(p) {
            tracing::warn!(error = %e, path = %p.display(), "telegram: failed to persist ACL");
        }
    }
}

// ── Config-driven construction (`type = "telegram"` manifest) ────────────────

/// One connector manifest file (`[connector]` table at its root).
#[derive(Debug, Deserialize)]
struct ConnectorFile {
    connector: TelegramConfig,
}

/// Static config from a `type = "telegram"` manifest. The token is a secret, so
/// the manifest names the env var that holds it rather than the value.
#[derive(Debug, Deserialize)]
struct TelegramConfig {
    #[serde(default = "default_token_env")]
    token_env: String,
    /// Path (relative to the manifest) to the JSON ACL file. Absent **and** no
    /// `owner_chat` → no access control (allow all).
    acl_path: Option<String>,
    /// Seed owner chat id — inserted into the ACL at `owner`, so the bot is
    /// reachable on first run even with an empty/absent ACL file.
    owner_chat: Option<i64>,
    /// Coalescing quiet window in ms (default 500; `0` disables coalescing).
    batch_debounce_ms: Option<u64>,
    /// Cap in ms on how long a chat's buffer stays open (default 3000).
    batch_max_wait_ms: Option<u64>,
    /// Shared workspace root (relative to the manifest) for file transfer. Must
    /// match octo-code's. Absent → `$OCTO_CODE_WORKSPACE`, then the default.
    workspace: Option<String>,
}

fn default_token_env() -> String {
    "OCTO_TELEGRAM_TOKEN".to_string()
}

/// [`ConnectorFactory`] for `type = "telegram"`. Register once with
/// `Octo::builder().register_connector_type("telegram", octo_connector_telegram::factory())`,
/// and every manifest with `type = "telegram"` becomes an instance.
pub struct TelegramConnectorFactory;

impl ConnectorFactory for TelegramConnectorFactory {
    fn type_name(&self) -> &str {
        "telegram"
    }

    fn create(
        &self,
        id: ConnectorId,
        config: &toml::Value,
        ctx: FactoryContext<'_>,
    ) -> Result<Arc<dyn Connector>, Box<dyn std::error::Error + Send + Sync>> {
        let file: ConnectorFile = config.clone().try_into()?;
        let cfg = file.connector;
        let token = std::env::var(&cfg.token_env)
            .map_err(|_| format!("telegram: env var {} is not set", cfg.token_env))?;

        let debounce = cfg.batch_debounce_ms.map(Duration::from_millis).unwrap_or(DEFAULT_DEBOUNCE);
        let max_wait = cfg.batch_max_wait_ms.map(Duration::from_millis).unwrap_or(DEFAULT_MAX_WAIT);
        let workspace: Option<PathBuf> = cfg.workspace.as_ref().map(|w| ctx.base_dir.join(w));

        // No ACL configured → allow-all connector (the playground shape).
        let acl_state = if cfg.acl_path.is_none() && cfg.owner_chat.is_none() {
            None
        } else {
            // Resolve the ACL path (relative to the manifest) once — used to load
            // it now and to persist runtime mutations later.
            let acl_path: Option<PathBuf> = cfg.acl_path.as_ref().map(|p| ctx.base_dir.join(p));
            let mut acl = match &acl_path {
                Some(p) => Acl::load(p)?,
                None => Acl::new(),
            };
            if let Some(owner) = cfg.owner_chat {
                acl.ensure(owner, Role::Owner);
            }
            tracing::info!(connector = %id, allowed = acl.len(), "telegram: access-control list loaded");
            Some(Arc::new(AclState { acl: RwLock::new(acl), path: acl_path }))
        };
        Ok(TelegramConnector::build(id.as_str(), token, acl_state, debounce, max_wait, workspace))
    }
}

/// Convenience factory handle for registration.
pub fn factory() -> Arc<dyn ConnectorFactory> {
    Arc::new(TelegramConnectorFactory)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn factory_builds_connector_from_manifest() {
        // Unique env var so the test can't collide with a real OCTO_TELEGRAM_TOKEN.
        unsafe { std::env::set_var("TG_FACTORY_TEST_TOKEN", "123:abc") };
        let manifest = r#"
            [connector]
            id = "telegram"
            type = "telegram"
            token_env = "TG_FACTORY_TEST_TOKEN"
            owner_chat = 42
        "#;
        let value: toml::Value = toml::from_str(manifest).unwrap();
        let factory = TelegramConnectorFactory;
        assert_eq!(factory.type_name(), "telegram");
        let conn = factory
            .create(
                ConnectorId::new("telegram"),
                &value,
                FactoryContext { base_dir: std::path::Path::new(".") },
            )
            .expect("factory builds the connector");
        assert_eq!(conn.id().as_str(), "telegram");
    }

    #[test]
    fn factory_errors_when_token_env_unset() {
        let manifest = r#"
            [connector]
            id = "telegram"
            type = "telegram"
            token_env = "TG_DEFINITELY_UNSET_VAR_XYZ"
        "#;
        let value: toml::Value = toml::from_str(manifest).unwrap();
        let result = TelegramConnectorFactory.create(
            ConnectorId::new("telegram"),
            &value,
            FactoryContext { base_dir: std::path::Path::new(".") },
        );
        // `dyn Connector` isn't `Debug`, so match instead of `unwrap_err`.
        let err = match result {
            Ok(_) => panic!("expected an error when the token env var is unset"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("not set"), "got: {err}");
    }
}
