//! `alice` — a Yandex Alice skill (Yandex Dialogs webhook) as an Octo chat
//! channel.
//!
//! Inbound: Yandex POSTs each spoken phrase to `POST /alice/<secret>`; the
//! connector checks the speaker against its allow-list and publishes a
//! `chat.message` (String) on channel `alice:<user_id>`, tagged
//! `chat_type = "voice"` so the agent knows to answer briefly, for the ear.
//!
//! Outbound: the agent's `chat.reply` (addressed to this connector) is turned
//! into speech (Markdown stripped, split into ≤ `max_chars` pieces).
//!
//! Alice waits only ~3 s for a webhook answer, an agent turn takes longer. The
//! request that starts a turn waits `reply_wait_ms` and, if the reply is not
//! there yet, answers with a placeholder; the reply is parked per speaker and
//! handed out when they say «дальше». «дальше» itself never reaches the bus —
//! a new message would interrupt the turn still running.
//!
//! Deliberately not handled: `chat.typing` / `chat.status` (nothing to show on
//! a speaker) and `chat.send_file` (no way to deliver a file by voice).

mod config;
mod dialog;
mod protocol;
mod server;
mod speech;

use std::sync::Arc;

use async_trait::async_trait;
use octo_core::{
    Blob, Connector, ConnectorCapabilities, ConnectorContext, ConnectorId, Envelope, EventKind,
    Filter, OctoResult, SubscribeOptions,
};

pub use config::{AliceConnectorFactory, Phrases, Settings, factory};
use dialog::Dialogs;

const CHAT_MESSAGE: &str = "chat.message";
const CHAT_REPLY: &str = "chat.reply";

pub struct AliceConnector {
    id: ConnectorId,
    capabilities: ConnectorCapabilities,
    settings: Arc<Settings>,
    dialogs: Arc<Dialogs>,
}

impl AliceConnector {
    pub fn new(id: ConnectorId, settings: Settings) -> Arc<Self> {
        let capabilities = ConnectorCapabilities::bidirectional()
            .with_emit_kinds([EventKind::from_static(CHAT_MESSAGE)])
            .with_accept_kinds([EventKind::from_static(CHAT_REPLY)]);
        let dialogs = Arc::new(Dialogs::new(settings.turn_ttl));
        Arc::new(Self {
            id,
            capabilities,
            settings: Arc::new(settings),
            dialogs,
        })
    }

    /// Park an outbound reply for the speaker it is addressed to.
    fn on_outbound(&self, env: &Envelope) {
        if env.kind.as_str() != CHAT_REPLY {
            return;
        }
        let Some(channel) = env.channel.as_ref() else {
            tracing::warn!("alice: chat.reply without a channel; dropped");
            return;
        };
        let spoken = if let Some(text) = env.payload_as::<String>() {
            speech::to_speech(text)
        } else if env.payload_as::<Blob>().is_some() {
            self.settings.phrases.media.clone()
        } else {
            return;
        };
        let room = self.settings.max_chars;
        let pieces = speech::chunk(&spoken, room);
        tracing::info!(
            channel = channel.as_str(),
            pieces = pieces.len(),
            correlated = env.correlation_id.is_some(),
            "alice: reply parked"
        );
        self.dialogs
            .deliver(channel.as_str(), env.correlation_id, pieces);
    }
}

#[async_trait]
impl Connector for AliceConnector {
    fn id(&self) -> &ConnectorId {
        &self.id
    }

    fn capabilities(&self) -> &ConnectorCapabilities {
        &self.capabilities
    }

    async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
        // Subscribe before serving, so no reply can slip past.
        let mut replies = ctx
            .subscribe(
                Filter::by_target(self.id.clone()),
                SubscribeOptions::default(),
            )
            .await?;

        let listener = tokio::net::TcpListener::bind(self.settings.listen).await?;
        tracing::info!(addr = %self.settings.listen, "alice: webhook listening");
        let app = server::router(server::App {
            id: self.id.clone(),
            settings: self.settings.clone(),
            dialogs: self.dialogs.clone(),
            ctx: ctx.clone(),
        });
        let shutdown = ctx.shutdown.clone();
        let serve = axum::serve(listener, app)
            .with_graceful_shutdown(async move { shutdown.cancelled().await });
        let mut serve = std::pin::pin!(serve.into_future());

        loop {
            tokio::select! {
                served = &mut serve => {
                    return match served {
                        Ok(()) => Ok(()),
                        Err(e) => Err(e.into()),
                    };
                }
                env = replies.next() => match env {
                    Some(env) => self.on_outbound(&env),
                    None => return Ok(()),
                },
            }
        }
    }
}
