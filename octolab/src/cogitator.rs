//! `ReactCogitator` — LLM cogitator using **rig's native tool-calling**.
//!
//! The agent gets one tool, `dispatch_to_connector` (from `octo-rig`), so the
//! model reaches the Octo connector system through rig's own tool loop — no
//! hand-rolled JSON decide-loop. Keeps per-channel history (fed as context) and
//! a reflex fast-path for commands. Replies honor the incoming envelope's reply
//! recommendation (back to source, same channel).
//!
//! Perception is configurable (`OCTO_PERCEPTION`): the subscription filter sets
//! how much of the bus the agent *sees*, while the action trigger stays narrow —
//! it only calls the LLM on a `chat.message` addressed to it, never on its own
//! emissions or other traffic. Anything seen but not acted on is *observed*
//! (logged + kept as ambient context), so wide perception never means wide (or
//! looping) cognition.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use octo_core::{
    Blob, Cogitator, CogitatorContext, ConnectorId, Envelope, EventKind, Filter, OctoResult,
    Subscription,
};
use octo_rig::OctoDispatchTool;

use crate::config::Settings;
use crate::error::Result;
use crate::history::{to_messages, HistoryStore, Turn};
use crate::llm;

/// Max tool-calling rounds rig may take per message.
const MAX_TOOL_TURNS: usize = 5;

/// How many recently-observed (ambient) bus events to keep for context.
const AMBIENT_MAX: usize = 8;

const BASE_PREAMBLE: &str = "You are Octo, a concise, helpful assistant living inside the Octo \
event-driven runtime. Call the `dispatch_to_connector` tool to reach an available connector when \
the user's request needs its data or action; otherwise just answer. Reply in the user's language, \
keep it short. If a connector returns an error, tell the user honestly that it failed — never \
invent a result.";

pub struct ReactCogitator {
    id: String,
    self_source: ConnectorId,
    settings: Settings,
    history: Arc<dyn HistoryStore>,
    /// Recent bus events the agent perceived but didn't act on (ambient
    /// awareness). Bounded to `AMBIENT_MAX`; fed into the preamble on reply.
    ambient: Mutex<VecDeque<String>>,
}

impl ReactCogitator {
    pub fn new(
        id: impl Into<String>,
        settings: Settings,
        history: Arc<dyn HistoryStore>,
    ) -> Arc<Self> {
        let id = id.into();
        Arc::new(Self {
            self_source: ConnectorId::new(format!("cogitator/{id}")),
            id,
            settings,
            history,
            ambient: Mutex::new(VecDeque::new()),
        })
    }
}

#[async_trait]
impl Cogitator for ReactCogitator {
    fn id(&self) -> &str {
        &self.id
    }

    fn filter(&self) -> Filter {
        perception_filter(self.settings.perception.as_deref())
    }

    async fn run(
        self: Arc<Self>,
        ctx: CogitatorContext,
        mut subscription: Subscription,
    ) -> OctoResult<()> {
        loop {
            tokio::select! {
                next = subscription.next() => match next {
                    Some(envelope) => self.clone().handle(envelope, &ctx).await,
                    None => return Ok(()),
                },
                _ = ctx.shutdown.cancelled() => return Ok(()),
            }
        }
    }
}

impl ReactCogitator {
    /// Perception → action gate. Wide perception reaches here; only a
    /// `chat.message` addressed to us (and never our own emission) triggers
    /// cognition. Everything else in scope is observed, not acted on — so a wide
    /// `OCTO_PERCEPTION` can't cause feedback loops or blast the LLM.
    async fn handle(self: Arc<Self>, incoming: Arc<Envelope>, ctx: &CogitatorContext) {
        // Never react to (nor observe) our own emissions — that's the loop.
        if incoming.source == self.self_source {
            return;
        }
        // Action trigger: a user chat message. Text or an image (a photo arrives
        // as a `Blob` payload, caption on the `caption` tag). Anything else →
        // observe only.
        if incoming.kind.as_str() == "chat.message" {
            if let Some(text) = incoming.payload_as::<String>().cloned() {
                self.respond(incoming, text, None, ctx).await;
                return;
            }
            if let Some(image) = incoming.payload_as::<Blob>().cloned() {
                let caption = incoming.tags.get("caption").cloned().unwrap_or_default();
                self.respond(incoming, caption, Some(image), ctx).await;
                return;
            }
        }
        self.observe(&incoming);
    }

    async fn respond(
        self: Arc<Self>,
        incoming: Arc<Envelope>,
        text: String,
        image: Option<Blob>,
        ctx: &CogitatorContext,
    ) {
        let channel_key = incoming
            .channel
            .as_ref()
            .map(|c| c.as_str().to_string())
            .unwrap_or_default();

        // Reflexes are for plain-text commands only; an image always goes to the
        // model (which may also be a vision model).
        if image.is_none() {
            // Reflex: /pic sends an image (media path), no LLM.
            if text.trim() == "/pic" {
                tracing::info!(source = %incoming.source, "reflex: /pic");
                self.send_image(&incoming, ctx).await;
                self.record(&channel_key, text, "(sent an image)".into()).await;
                return;
            }
            // Reflex: known commands, no LLM.
            if let Some(canned) = command_reply(&text) {
                tracing::info!(source = %incoming.source, cmd = %text, "reflex (no LLM)");
                self.emit_reply(&incoming, canned, ctx).await;
                let marker = format!("(reflex reply to {})", text.trim());
                self.record(&channel_key, text, marker).await;
                return;
            }
        }

        tracing::info!(source = %incoming.source, channel = %channel_key, has_image = image.is_some(), "← {text}");

        let history = to_messages(&self.history.load(&channel_key).await);

        // env-as-tools: one rig tool that dispatches to any registered connector
        // (catalogue comes from the runtime's introspection).
        let tool = OctoDispatchTool::new(ctx.bus(), self.self_source.clone(), catalog(ctx));

        // Front-load the *incoming* provenance and any ambient bus activity, so
        // the agent knows where the message came from and what else it perceived.
        let mut preamble =
            format!("{BASE_PREAMBLE}\n\n{}", incoming_context(&incoming, &channel_key));
        if let Some(ambient) = self.ambient_context() {
            preamble += &format!("\n\n{ambient}");
        }

        let answer = match llm::chat_with_tool(
            &self.settings.api_key,
            &self.settings.model,
            &preamble,
            &text,
            image.as_ref(),
            history,
            tool,
            MAX_TOOL_TURNS,
        )
        .await
        {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!(error = %e, "llm tool-call failed");
                format!("(llm error: {e})")
            }
        };
        tracing::info!("→ {answer}");

        self.emit_reply(&incoming, answer.clone(), ctx).await;
        // Faithful transcript: note an image arrived (the model won't re-see the
        // pixels next turn; the textual note keeps recall honest).
        let user_turn = match (image.is_some(), text.is_empty()) {
            (true, true) => "(sent an image)".to_string(),
            (true, false) => format!("(sent an image) {text}"),
            (false, _) => text,
        };
        self.record(&channel_key, user_turn, answer).await;
    }

    /// Persist one user→assistant exchange to channel history. Used by *every*
    /// path — LLM and reflex alike — so the transcript faithfully reflects every
    /// message that arrived (the "recall" level of perception). Reflexes store a
    /// compact assistant marker instead of the full canned reply.
    async fn record(&self, channel: &str, user: String, assistant: String) {
        if let Err(e) = self
            .history
            .append(channel, &[Turn::user(user), Turn::assistant(assistant)])
            .await
        {
            tracing::warn!(error = %e, "failed to persist history");
        }
    }

    /// Observe an envelope that's in perceptual scope but not addressed to us:
    /// log it and keep it as ambient context (bounded ring). No LLM, no reply.
    fn observe(&self, env: &Envelope) {
        let line = ambient_line(env);
        tracing::info!(kind = %env.kind, source = %env.source, "ambient: {line}");
        let mut buf = self.ambient.lock().unwrap();
        buf.push_back(line);
        while buf.len() > AMBIENT_MAX {
            buf.pop_front();
        }
    }

    /// Render the ambient ring into a preamble block, or `None` if empty.
    fn ambient_context(&self) -> Option<String> {
        let buf = self.ambient.lock().unwrap();
        if buf.is_empty() {
            return None;
        }
        let lines = buf
            .iter()
            .map(|l| format!("- {l}"))
            .collect::<Vec<_>>()
            .join("\n");
        Some(format!(
            "Ambient — recent bus activity you also perceived (not addressed to you):\n{lines}\n\
             Mention it only if the user asks what's going on around you; otherwise ignore it."
        ))
    }

    /// Emit a `chat.reply` of any payload type (text / media [`Blob`]) back to
    /// the source connector on the same channel.
    async fn emit<P: std::any::Any + Send + Sync>(
        &self,
        incoming: &Envelope,
        payload: P,
        ctx: &CogitatorContext,
    ) {
        let mut reply = Envelope::new(
            self.self_source.clone(),
            EventKind::from_static("chat.reply"),
            payload,
        )
        .with_target(incoming.source.clone())
        .with_correlation(incoming.id);
        if let Some(channel) = &incoming.channel {
            reply = reply.with_channel(channel.clone());
        }
        if let Some(reply_to) = &incoming.reply_to {
            reply = reply.with_reply_to(reply_to.clone());
        }
        if let Err(e) = ctx.publish(reply).await {
            tracing::warn!(error = %e, "failed to publish chat.reply");
        }
    }

    async fn emit_reply(&self, incoming: &Envelope, text: String, ctx: &CogitatorContext) {
        self.emit(incoming, text, ctx).await;
    }

    async fn send_image(&self, incoming: &Envelope, ctx: &CogitatorContext) {
        match fetch_image("https://picsum.photos/400").await {
            Ok(blob) => {
                tracing::info!(bytes = blob.len(), ct = blob.content_type(), "→ image");
                self.emit(incoming, blob, ctx).await;
            }
            Err(e) => {
                tracing::warn!(error = %e, "image fetch failed");
                self.emit(incoming, format!("Не удалось загрузить картинку: {e}"), ctx)
                    .await;
            }
        }
    }
}

/// Front-load the incoming envelope's provenance for the model — the
/// perception side, symmetric to the action-space catalogue. The agent learns
/// *where* the message came from (connector + channel + any channel metadata),
/// not just what it can do.
fn incoming_context(env: &Envelope, channel: &str) -> String {
    let mut s = format!(
        "Context — this message arrived via connector \"{}\", channel \"{}\".",
        env.source, channel
    );
    if let Some(m) = &env.channel_metadata {
        s += &format!(" Channel trust: {:?}, priority: {:?}.", m.trust, m.priority);
        if !m.tags.is_empty() {
            s += &format!(" Channel tags: {:?}.", m.tags);
        }
    }
    s += " Reply through this same channel; take the source into account.";
    s
}

/// Perception scope → subscription filter (`OCTO_PERCEPTION`). Controls how much
/// of the bus the agent *sees*: `addressed` (only chat messages to us — the
/// default, today's behavior), `chat` (all chat traffic, including passing-by),
/// `all` (the whole bus), or a custom event-kind glob (e.g. `vision.**`).
fn perception_filter(spec: Option<&str>) -> Filter {
    match spec.unwrap_or("addressed").trim() {
        "addressed" => Filter::by_kind("chat.message"),
        "chat" => Filter::by_kind("chat.**"),
        "all" => Filter::all(),
        glob => Filter::by_kind(glob.to_string()),
    }
}

/// Compact one-line description of an observed envelope for ambient context.
fn ambient_line(env: &Envelope) -> String {
    let channel = env.channel.as_ref().map(|c| c.as_str()).unwrap_or("-");
    let preview = env
        .payload_as::<String>()
        .map(|s| {
            let t = s.trim();
            let short: String = t.chars().take(80).collect();
            let ellipsis = if t.chars().count() > 80 { "…" } else { "" };
            format!(": {short}{ellipsis}")
        })
        .unwrap_or_default();
    format!("[{}] from {} (channel {}){}", env.kind, env.source, channel, preview)
}

/// Catalogue (connectors advertising a description) for the dispatch tool.
fn catalog(ctx: &CogitatorContext) -> String {
    ctx.connectors()
        .iter()
        .filter_map(|c| {
            c.capabilities
                .description
                .as_ref()
                .map(|d| format!("- target \"{}\":\n{}", c.id, d))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

async fn fetch_image(url: &str) -> Result<Blob> {
    let resp = reqwest::get(url).await?.error_for_status()?;
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("image/jpeg")
        .to_string();
    let bytes = resp.bytes().await?;
    Ok(Blob::new(bytes, content_type).with_filename("image.jpg"))
}

fn command_reply(text: &str) -> Option<String> {
    match text.trim() {
        "/start" => Some(
            "👋 Привет! Я Octo — агент на event-driven рантайме. Напиши сообщение, и я отвечу \
             (с памятью в рамках чата). Доступные коннекторы (напр. petstore) дёргаю инструментом сам. \
             /help — что умею."
                .to_string(),
        ),
        "/help" => Some(
            "Я ReAct-агент поверх Octo-рантайма (rig native tool-calling).\n\
             • любой текст → ответ; при нужде сам схожу в коннектор инструментом\n\
             • /pic → картинка\n\
             • /start, /help → мгновенно, без LLM"
                .to_string(),
        ),
        _ => None,
    }
}
