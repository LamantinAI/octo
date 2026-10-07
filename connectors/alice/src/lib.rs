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
//! request that starts a turn waits `reply_wait_ms`; if the reply is not there
//! yet it answers with a filler. With `[push]` the speaker then says the reply
//! by itself through the Yandex cloud voice ([`quasar`]); without it the reply
//! is parked per speaker until they say «дальше» (which never reaches the bus —
//! a new message would interrupt the turn still running).
//!
//! Deliberately not handled: `chat.typing` / `chat.status` (nothing to show on
//! a speaker) and `chat.send_file` (no way to deliver a file by voice).

mod config;
mod dialog;
mod protocol;
mod quasar;
mod server;
mod speech;

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use octo_core::{
    Blob, Connector, ConnectorCapabilities, ConnectorContext, ConnectorId, Envelope, EventKind,
    Filter, OctoResult, SubscribeOptions,
};

pub use config::{AliceConnectorFactory, Phrases, PushSettings, Settings, factory};
use dialog::{Delivery, Dialogs};
use quasar::Quasar;

/// Something that can make the speaker talk on its own (the cloud voice; a
/// fake in tests).
#[async_trait]
pub(crate) trait Voice: Send + Sync + 'static {
    /// Say the pieces in order.
    async fn say(&self, pieces: &[String]) -> Result<(), String>;
    /// Have the speaker execute a command as if it had been said to Alice.
    async fn command(&self, command: &str) -> Result<(), String>;
    /// A one-line readiness report for the startup log.
    async fn check(&self) -> Result<String, String>;
}

#[async_trait]
impl Voice for Quasar {
    async fn say(&self, pieces: &[String]) -> Result<(), String> {
        Quasar::say(self, pieces).await.map_err(|e| e.to_string())
    }

    async fn command(&self, command: &str) -> Result<(), String> {
        Quasar::command(self, command)
            .await
            .map_err(|e| e.to_string())
    }

    async fn check(&self) -> Result<String, String> {
        self.speakers()
            .await
            .map(|list| format!("speakers on the account (name, id): {list:?}"))
            .map_err(|e| e.to_string())
    }
}

const CHAT_MESSAGE: &str = "chat.message";
const CHAT_REPLY: &str = "chat.reply";

pub struct AliceConnector {
    id: ConnectorId,
    capabilities: ConnectorCapabilities,
    settings: Arc<Settings>,
    dialogs: Arc<Dialogs>,
    /// The speaker's cloud voice, when `[push]` is configured.
    voice: Option<Arc<dyn Voice>>,
}

impl AliceConnector {
    pub fn new(id: ConnectorId, settings: Settings) -> Arc<Self> {
        let voice = settings.push.as_ref().and_then(|p| {
            Quasar::new(p.x_token.clone(), p.device.clone())
                .map_err(|e| tracing::warn!(error = %e, "alice: cloud voice unavailable"))
                .ok()
                .map(|q| Arc::new(q) as Arc<dyn Voice>)
        });
        Self::with_voice(id, settings, voice)
    }

    pub(crate) fn with_voice(
        id: ConnectorId,
        settings: Settings,
        voice: Option<Arc<dyn Voice>>,
    ) -> Arc<Self> {
        let capabilities = ConnectorCapabilities::bidirectional()
            .with_emit_kinds([EventKind::from_static(CHAT_MESSAGE)])
            .with_accept_kinds([EventKind::from_static(CHAT_REPLY)]);
        let dialogs = Arc::new(Dialogs::new(settings.turn_ttl, settings.max_chars));
        Arc::new(Self {
            id,
            capabilities,
            settings: Arc::new(settings),
            dialogs,
            voice,
        })
    }

    /// Route an outbound reply: pushed to the speaker's voice when its turn was
    /// handed over (or it is an unsolicited reminder), otherwise parked.
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
        if spoken.is_empty() {
            return;
        }
        let delivery = self.dialogs.deliver(
            channel.as_str(),
            env.correlation_id,
            spoken,
            self.voice.is_some(),
        );
        tracing::info!(
            channel = channel.as_str(),
            correlated = env.correlation_id.is_some(),
            pushed = matches!(delivery, Delivery::Push(_)),
            "alice: reply arrived"
        );
        if let Delivery::Push(text) = delivery {
            self.push(channel.as_str(), text, Duration::ZERO);
        }
    }

    /// Deliver a finished reply through the speaker. With `relaunch` the reply
    /// is parked and the speaker reopens the skill, whose launch request then
    /// gets the whole reply as an ordinary answer (and the conversation stays
    /// open); otherwise the cloud voice reads it out in pieces. `delay` lets
    /// what the speaker is saying now finish first. On failure the reply stays
    /// parked for the next launch / «дальше».
    fn push(&self, channel: &str, text: String, delay: Duration) {
        let Some(voice) = self.voice.clone() else {
            self.dialogs.park(channel, &text);
            return;
        };
        let relaunch = self.settings.push.as_ref().and_then(|p| p.relaunch.clone());
        let dialogs = self.dialogs.clone();
        let channel = channel.to_string();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            if let Some(command) = relaunch {
                dialogs.park(&channel, &text);
                if let Err(e) = voice.command(&command).await {
                    tracing::warn!(error = %e, "alice: could not reopen the skill; reply parked for the next launch");
                }
                return;
            }
            let pieces = speech::chunk(&text, quasar::SAY_LIMIT);
            if let Err(e) = voice.say(&pieces).await {
                tracing::warn!(error = %e, "alice: cloud voice failed; reply parked for «дальше»");
                dialogs.park(&channel, &text);
            }
        });
    }

    /// Keep the speaker joking while the turn handed over to it is unanswered:
    /// a fresh filler every `filler_gap` after the previous one is said, up to
    /// `max_fillers`. `first` is the filler the webhook already answered with.
    fn start_fillers(&self, channel: &str, first: String) {
        let (Some(voice), Some(push)) = (self.voice.clone(), self.settings.push.clone()) else {
            return;
        };
        let settings = self.settings.clone();
        let dialogs = self.dialogs.clone();
        let channel = channel.to_string();
        tokio::spawn(async move {
            let mut last = first;
            for _ in 0..push.max_fillers {
                tokio::time::sleep(quasar::speaking_time(&last) + push.filler_gap).await;
                if !dialogs.awaiting_push(&channel) {
                    return;
                }
                let next = settings.filler_except(&last).to_string();
                if let Err(e) = voice.say(std::slice::from_ref(&next)).await {
                    tracing::warn!(error = %e, "alice: filler failed; stopping fillers");
                    return;
                }
                last = next;
            }
        });
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

        if let Some(voice) = self.voice.clone() {
            tokio::spawn(async move {
                match voice.check().await {
                    Ok(report) => tracing::info!("alice: cloud voice ready; {report}"),
                    Err(e) => tracing::warn!(error = %e, "alice: cloud voice check failed"),
                }
            });
        }

        let listener = tokio::net::TcpListener::bind(self.settings.listen).await?;
        tracing::info!(addr = %self.settings.listen, "alice: webhook listening");
        let app = server::router(server::App {
            id: self.id.clone(),
            settings: self.settings.clone(),
            dialogs: self.dialogs.clone(),
            ctx: ctx.clone(),
            connector: self.clone(),
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
