//! Optional assembly-owned policy. The connector owns Telegram IO; the host owns
//! session meaning. Native frames/stamps are captured before download or batching.
use std::time::Duration;

use async_trait::async_trait;
use octo_core::{ConnectorId, Envelope};

use crate::{CallbackDecision, CallbackFrame, CallbackLimits, CommandMenuScope, TelegramCommand};

pub(super) mod delivery;
pub(super) mod ingress;
pub(crate) mod outbound;
pub(crate) mod preflight;
pub use preflight::{
    ActorRoleDecision, PreflightDecision, PreflightFrame, VerifiedActor, VerifiedBot,
};
pub(super) mod tracking;

#[derive(Clone, Debug)]
pub struct TrackingLimits {
    pub max_scopes: usize,
    pub max_ids_per_scope: usize,
    pub cleanup_batches: usize,
    pub cleanup_timeout: Duration,
}
impl Default for TrackingLimits {
    fn default() -> Self {
        Self {
            max_scopes: 256,
            max_ids_per_scope: 10_000,
            cleanup_batches: 4,
            cleanup_timeout: Duration::from_secs(3),
        }
    }
}

/// Host may opt into verified same-scope quote enrichment. Policy mode otherwise
/// omits automatic quoted content instead of importing another session's snippet.
#[derive(Clone)]
pub struct AllowReplyContext;

#[derive(Clone)]
pub struct NativeFrame {
    pub connector: ConnectorId,
    pub bot_id: u64,
    pub update_id: u32,
    pub message_id: i32,
    pub date: i64,
    pub chat_id: i64,
    pub actor_id: Option<i64>,
    pub chat_type: String,
    /// Present only for a real, non-forwarded text command to this bot.
    pub command_text: Option<String>,
    pub digest: String,
    pub media: bool,
}

#[derive(Clone)]
pub struct NativeBatch {
    pub frames: Vec<NativeFrame>,
}

/// Opaque host scope. Neither these labels nor possession of them grants authority.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct IngressScope {
    pub key: String,
    pub epoch: String,
    pub ephemeral: bool,
    /// Cleanup identity can outlive volatile retention (for example after Save).
    pub cleanup_scope: Option<String>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct TelegramOutputScope {
    pub scope: IngressScope,
    pub attempt_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct CleanupRequest {
    pub scope_key: String,
    pub epoch: String,
}

// Admission moves an envelope once; keeping it inline avoids a mandatory host allocation.
#[allow(clippy::large_enum_variant)]
pub enum IngressDecision {
    Accept {
        envelope: Envelope,
        scope: IngressScope,
    },
    Control {
        notice: Option<String>,
        scope: Option<IngressScope>,
        post_events: Vec<Envelope>,
        cleanup: Option<CleanupRequest>,
    },
    Ignore,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeliveryStatus {
    Sent,
    Failed,
    Unknown,
}
#[derive(Clone, Debug)]
pub struct DeliveryReport {
    pub scope: Option<TelegramOutputScope>,
    pub chat_id: i64,
    pub message_ids: Vec<i32>,
    pub status: DeliveryStatus,
}
#[derive(Clone, Debug, Default)]
pub struct CleanupReport {
    pub requested: usize,
    pub deleted: usize,
    pub failed: usize,
    pub unknown: usize,
}

#[async_trait]
pub trait TelegramHostPolicy: Send + Sync {
    /// Optional native menu for a verified scope. None preserves legacy behavior.
    async fn command_menu(
        &self,
        _scope: CommandMenuScope,
    ) -> Result<Option<Vec<TelegramCommand>>, String> {
        Ok(None)
    }
    /// Called after getMe and before polling starts. Errors are intentionally redacted.
    async fn bot_ready(&self, _bot: VerifiedBot) -> Result<(), String> {
        Ok(())
    }
    /// Runs before legacy ACL perception and before any message-body logging/hash.
    async fn preflight(&self, _frame: PreflightFrame) -> Result<PreflightDecision, String> {
        Ok(PreflightDecision::ContinueLegacy)
    }
    /// Fresh callback/approval authorization; None denies even an existing ACL owner.
    async fn actor_role(&self, _actor: VerifiedActor) -> Result<ActorRoleDecision, String> {
        Ok(ActorRoleDecision::ContinueLegacy)
    }

    /// Input carries verified Telegram metadata/native authority. Return its pinned
    /// full EnvelopeContext, preserving native authority and adding host context.
    async fn admit(
        &self,
        frame: NativeFrame,
        provisional: Envelope,
    ) -> Result<IngressDecision, String>;
    /// Generic human callback transport only; no native message/tool authority.
    async fn callback(&self, _frame: CallbackFrame) -> Result<CallbackDecision, String> {
        Ok(CallbackDecision::unavailable())
    }
    fn callback_limits(&self) -> CallbackLimits {
        CallbackLimits::default()
    }
    /// Rechecks only the pinned stamp; must never resolve the current selector again.
    fn allow_publish(&self, _envelope: &Envelope) -> bool {
        true
    }
    /// Authorize the exact resolved native destination. None preserves legacy ACL
    /// and native ingress-marker behavior; failures deny without exposing errors.
    async fn authorize_outbound(
        &self,
        _bot: VerifiedBot,
        _chat_id: i64,
        _envelope: &Envelope,
    ) -> Result<Option<bool>, String> {
        Ok(None)
    }
    fn allow_delivery(&self, _envelope: &Envelope) -> bool {
        true
    }
    /// Effective output retention may change after promotion/navigation without
    /// changing the immutable input stamp. `None` intentionally disables tracking.
    fn output_scope(&self, envelope: &Envelope) -> Option<TelegramOutputScope> {
        envelope
            .context
            .get::<TelegramOutputScope>()
            .cloned()
            .or_else(|| {
                envelope
                    .context
                    .get::<IngressScope>()
                    .cloned()
                    .map(|scope| TelegramOutputScope {
                        scope,
                        attempt_id: None,
                    })
            })
    }
    fn delivered(&self, _envelope: &Envelope, _report: &DeliveryReport) {}
    fn cleanup_finished(&self, _report: &CleanupReport) {}
    fn tracking_limits(&self) -> TrackingLimits {
        TrackingLimits::default()
    }
}
