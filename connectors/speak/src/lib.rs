//! `octo-connector-speak` — turn text into a spoken Ogg/Opus voice note via a native
//! WebRTC call to ChatGPT Voice on the subscription (the same `auth.json` the LLM uses),
//! so speech costs no API key and no per-character billing.
//!
//! An env-as-tools organ: dispatch `speak.run { text, voice? }` and get a correlated
//! `speak.run.result { path }` (or `{ error }`). The `.ogg` is written into the shared
//! workspace (jailed against `..`/absolute escapes); `chat.send_file { path }` on the
//! telegram connector then sends it as a voice note (an `.ogg` goes out via `sendVoice`).
//! The subscription token comes from the shared [`SubscriptionAuth`] handed in at
//! construction — the SAME refresh-owner the cogitator's LLM path uses.
//!
//! Declared by a manifest (`type = "speak"`, see [`factory`]) whose `[connector]` table may
//! set `voice`, the default voice a call falls back to.
//!
//! HOW IT WORKS. The desktop ChatGPT Voice call is a client-owned WebRTC "call" to
//! GPT-Live whose session instructions are "read this text verbatim"; the model speaks on
//! its own as soon as the session starts, so those instructions turn the call into a TTS
//! engine. We drive the call with [`str0m`] (sans-IO WebRTC): we own the UDP socket and the
//! poll loop. str0m's depacketizer puts reordered packets back in order; a packet that never
//! arrives becomes a zero-length Opus frame, which the player conceals (PLC), so the timeline
//! keeps its length instead of the voice skipping ahead. The Opus frames are remuxed
//! straight into Ogg — no decode/encode.
//!
//! FIRST CUT: one call per `speak.run`, up to ~3000 chars (≈ under two minutes of speech).
//! Longer text should be split by the caller. Non-trickle ICE with a single host candidate
//! off the default route — enough on a public-IP host (the deploy); a srflx candidate via
//! STUN for hosts behind NAT is the follow-up.

mod call;
mod manifest;
mod net;
mod ogg;
mod signal;

use std::{path::PathBuf, sync::Arc};

use async_trait::async_trait;
use chrono::Utc;
use octo_core::{
    Connector, ConnectorCapabilities, ConnectorContext, ConnectorId, Envelope, EventKind, Filter,
    OctoResult, SubscribeOptions,
};
use octo_openai_auth::SubscriptionAuth;
use octo_workspace::{workspace_root, write_in_root};
use serde_json::{json, Value};
use tracing::{info, warn};

use crate::{call::synthesize, signal::CallError};

pub use crate::manifest::factory;

/// Command kind this connector accepts.
pub(crate) const RUN: &str = "speak.run";

/// The voices the endpoint accepts.
pub(crate) const VOICES: [&str; 9] =
    ["cove", "juniper", "maple", "spruce", "ember", "vale", "breeze", "arbor", "sol"];

/// Default voice when the caller does not name one.
pub(crate) const DEFAULT_VOICE: &str = "cove";

/// 833 chars ≈ 30 s of speech; keep one call under ~2 minutes.
pub(crate) const MAX_CHARS: usize = 3000;

const CATALOG: &str = "Speak text aloud as a voice note on the ChatGPT subscription. Dispatch to this connector's id:
- speak.run { text, voice? } -> { path }
  `text` is what to say (up to ~3000 chars per call, about one voice message); `voice` is optional,
  one of cove/juniper/maple/spruce/ember/vale/breeze/arbor/sol (default cove). Returns `path`: a
  workspace-relative Ogg/Opus file; send it with chat.send_file { path } and it arrives as a voice note.";

pub struct SpeakConnector {
    id: ConnectorId,
    capabilities: ConnectorCapabilities,
    /// Shared subscription auth — one refresh owner with the rest of the runtime.
    auth: Arc<SubscriptionAuth>,
    /// Explicit workspace root; `None` -> resolved from the environment at use.
    workspace: Option<PathBuf>,
    /// The voice a call falls back to (from the manifest, else [`DEFAULT_VOICE`]).
    voice: String,
}

impl SpeakConnector {
    /// Construct the connector with the shared subscription-auth handle and an optional
    /// workspace root (the shared file jail the `.ogg` is written into).
    pub fn new(
        id: impl Into<String>,
        auth: Arc<SubscriptionAuth>,
        workspace: Option<PathBuf>,
    ) -> Arc<Self> {
        Self::with_voice(id, auth, workspace, DEFAULT_VOICE)
    }

    /// Like [`new`](Self::new), with the voice a call falls back to.
    pub fn with_voice(
        id: impl Into<String>,
        auth: Arc<SubscriptionAuth>,
        workspace: Option<PathBuf>,
        voice: &str,
    ) -> Arc<Self> {
        let capabilities = ConnectorCapabilities::bidirectional()
            .with_accept_kinds([EventKind::from_static(RUN)])
            .with_description(CATALOG);
        Arc::new(Self { id: ConnectorId::new(id), capabilities, auth, workspace, voice: voice.to_string() })
    }

    async fn handle(&self, env: &Envelope, ctx: &ConnectorContext) {
        if env.kind.as_str() != RUN {
            return;
        }
        let params = env.payload_as::<Value>().cloned().unwrap_or(Value::Null);
        let payload = self.run(&params).await.unwrap_or_else(|e| json!({ "error": e }));
        let resp = Envelope::new(self.id.clone(), EventKind::new(format!("{RUN}.result")), payload)
            .with_correlation(env.id);
        if let Err(e) = ctx.publish(resp).await {
            warn!(error = %e, "speak: failed to publish result");
        }
    }

    async fn run(&self, params: &Value) -> Result<Value, String> {
        let text = params
            .get("text")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|t| !t.is_empty())
            .ok_or("provide `text` (what to say)")?;
        if text.chars().count() > MAX_CHARS {
            return Err(format!(
                "text is {} chars; max {MAX_CHARS} per call — split it into parts",
                text.chars().count()
            ));
        }
        let voice = params.get("voice").and_then(Value::as_str).unwrap_or(&self.voice);
        if !VOICES.contains(&voice) {
            return Err(format!("unknown voice {voice}; use one of {VOICES:?}"));
        }

        let root = workspace_root(self.workspace.as_deref()).map_err(|e| e.to_string())?;
        let sub = self.auth.fresh().await.map_err(|e| e.to_string())?;

        info!(chars = text.chars().count(), %voice, "speak: run");
        let (ogg, transcript) = match synthesize(text, voice, &sub).await {
            // The server can revoke a token ahead of its `exp`: refresh once and retry.
            Err(CallError::Unauthorized(_)) => {
                warn!("speak: token refused; forcing a refresh and retrying once");
                let sub = self.auth.force_refresh().await.map_err(|e| e.to_string())?;
                synthesize(text, voice, &sub).await
            }
            other => other,
        }
        .map_err(CallError::into_message)?;

        let rel = format!("speech-{}.ogg", Utc::now().timestamp_nanos_opt().unwrap_or_default());
        write_in_root(&root, &rel, &ogg).map_err(|e| e.to_string())?;
        info!(path = %rel, bytes = ogg.len(), "speak: wrote voice note");

        let mut out = json!({ "path": rel, "voice": voice, "bytes": ogg.len() });
        if let Some(t) = transcript {
            out["transcript"] = json!(t);
        }
        Ok(out)
    }
}

#[async_trait]
impl Connector for SpeakConnector {
    fn id(&self) -> &ConnectorId {
        &self.id
    }

    fn capabilities(&self) -> &ConnectorCapabilities {
        &self.capabilities
    }

    async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
        let mut cmds = ctx
            .subscribe(Filter::by_target(self.id.clone()), SubscribeOptions::default())
            .await?;
        info!(connector = %self.id, "speak ready");
        loop {
            tokio::select! {
                next = cmds.next() => match next {
                    Some(env) => self.handle(&env, &ctx).await,
                    None => return Ok(()),
                },
                _ = ctx.shutdown.cancelled() => return Ok(()),
            }
        }
    }
}
