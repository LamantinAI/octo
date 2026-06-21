//! `ReactCogitator` — LLM cogitator using **rig's native tool-calling**.
//!
//! The agent gets one tool, `dispatch_to_connector` (from `octo-rig`), so the
//! model reaches the Octo connector system through rig's own tool loop — no
//! hand-rolled JSON decide-loop. Keeps per-channel history (fed as context) and
//! a reflex fast-path for commands. Replies honor the incoming envelope's reply
//! recommendation (back to source, same channel).

use std::sync::Arc;

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
        })
    }
}

#[async_trait]
impl Cogitator for ReactCogitator {
    fn id(&self) -> &str {
        &self.id
    }

    fn filter(&self) -> Filter {
        Filter::by_kind("chat.message")
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
    async fn handle(self: Arc<Self>, incoming: Arc<Envelope>, ctx: &CogitatorContext) {
        let Some(text) = incoming.payload_as::<String>().cloned() else {
            return;
        };
        let channel_key = incoming
            .channel
            .as_ref()
            .map(|c| c.as_str().to_string())
            .unwrap_or_default();

        // Reflex: /pic sends an image (media path), no LLM.
        if text.trim() == "/pic" {
            tracing::info!(source = %incoming.source, "reflex: /pic");
            self.send_image(&incoming, ctx).await;
            return;
        }
        // Reflex: known commands, no LLM.
        if let Some(canned) = command_reply(&text) {
            tracing::info!(source = %incoming.source, cmd = %text, "reflex (no LLM)");
            self.emit_reply(&incoming, canned, ctx).await;
            return;
        }

        tracing::info!(source = %incoming.source, channel = %channel_key, "← {text}");

        let history = to_messages(&self.history.load(&channel_key).await);

        // env-as-tools: one rig tool that dispatches to any registered connector
        // (catalogue comes from the runtime's introspection).
        let tool = OctoDispatchTool::new(ctx.bus(), self.self_source.clone(), catalog(ctx));

        // Front-load the *incoming* provenance so the agent knows where the
        // message came from (symmetric to seeing the outgoing connector catalog).
        let preamble = format!("{BASE_PREAMBLE}\n\n{}", incoming_context(&incoming, &channel_key));

        let answer = match llm::chat_with_tool(
            &self.settings.api_key,
            &self.settings.model,
            &preamble,
            &text,
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
        if let Err(e) = self
            .history
            .append(&channel_key, &[Turn::user(text), Turn::assistant(answer)])
            .await
        {
            tracing::warn!(error = %e, "failed to persist history");
        }
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
