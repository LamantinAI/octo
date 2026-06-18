//! Behavior trail — what each layer did with an envelope before passing it on.
//!
//! The trail is the structural mechanism behind front-loading reasoning:
//! by the time an envelope reaches the cognition tier, it carries a record of
//! every classification, tag, pre-action and decision performed upstream.
//!
//! See `runtime_architecture` and `novelty` § Sub-claim 1b/1c vault drafts.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{ConnectorId, EventKind, RuleId};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrailEntry {
    pub actor: TrailActor,
    pub action: TrailAction,
    pub ts: DateTime<Utc>,
    /// Free-form tags attached at this trail step (e.g. classifier outputs).
    pub tags: Vec<String>,
}

impl TrailEntry {
    pub fn new(actor: TrailActor, action: TrailAction) -> Self {
        Self {
            actor,
            action,
            ts: Utc::now(),
            tags: Vec::new(),
        }
    }

    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    pub fn with_tags<I, S>(mut self, tags: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.tags.extend(tags.into_iter().map(Into::into));
        self
    }
}

/// Who acted on the envelope at this step.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TrailActor {
    Connector(ConnectorId),
    Reflex(RuleId),
    Cognition {
        backend: String,
    },
    External(String),
}

/// What was done.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TrailAction {
    /// Emitted a new envelope (typically with this `kind`).
    Emit { kind: EventKind },
    /// Added classification tags without acting.
    Tag { added: Vec<String> },
    /// Issued an immediate acknowledgement (e.g. "обрабатываю..." in act+escalate mode).
    Ack { ack_msg_id: Option<String> },
    /// Made a final decision.
    Decision { summary: String },
    /// Observed only — no mutation, no emit (e.g. memory writer pass-through).
    Observe,
    /// Skipped this envelope (e.g. filter mismatch).
    Skipped { reason: String },
}
