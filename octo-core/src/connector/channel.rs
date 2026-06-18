//! Channel — sub-actor inside a connector ("присоска" on a "щупальце").
//!
//! A channel is an instance representing one specific source (a chat_id, an MQTT
//! topic, an RTSP stream). It owns the per-source mailbox, lifecycle FSM and
//! backpressure policy. Channels appear and die dynamically as external sources
//! come and go; they are the unit of fine-grained supervision and ordering.
//!
//! See `connector_channel_split` vault draft.

use serde::{Deserialize, Serialize};

use crate::{
    BackpressureStrategy, ChannelId, ChannelMetadata, EventKind, RestartPolicy,
};

/// Static description of a channel — a configuration record + capability list.
///
/// At runtime this is paired with a per-channel mailbox and lifecycle FSM
/// (see `bus` and `lifecycle` modules).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelDescriptor {
    pub id: ChannelId,
    /// Trust gradient + priority + free-form tags. Propagated into envelopes.
    pub metadata: ChannelMetadata,
    /// Subset of connector's `event_kinds_emit` this channel actually emits.
    /// Empty = inherits all from connector.
    pub event_kinds: Vec<EventKind>,
    /// Backpressure policy applied to this channel's outgoing mailbox.
    /// Per-channel overrides per-connector default.
    pub backpressure: BackpressureStrategy,
    /// Restart policy at channel level (lighter than connector restart).
    pub restart: RestartPolicy,
    /// Idle policy — when to graceful-shutdown an inactive dynamic channel.
    pub idle: IdlePolicy,
}

impl ChannelDescriptor {
    pub fn new(id: ChannelId) -> Self {
        Self {
            id,
            metadata: ChannelMetadata::default(),
            event_kinds: Vec::new(),
            backpressure: BackpressureStrategy::default(),
            restart: RestartPolicy::default(),
            idle: IdlePolicy::default(),
        }
    }

    pub fn with_metadata(mut self, metadata: ChannelMetadata) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn with_event_kinds(mut self, kinds: impl IntoIterator<Item = EventKind>) -> Self {
        self.event_kinds = kinds.into_iter().collect();
        self
    }

    pub fn with_backpressure(mut self, backpressure: BackpressureStrategy) -> Self {
        self.backpressure = backpressure;
        self
    }

    pub fn with_restart(mut self, restart: RestartPolicy) -> Self {
        self.restart = restart;
        self
    }

    pub fn with_idle(mut self, idle: IdlePolicy) -> Self {
        self.idle = idle;
        self
    }
}

/// Idle policy for dynamic channels — when to tear down inactive instances.
///
/// Surface'ed by Telegram-bot reference scenario (per-user DM channels created
/// on first message, killed after N minutes of silence).
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct IdlePolicy {
    /// Tear down channel after this many seconds of no traffic. None = keep
    /// indefinitely (suitable for static channels).
    pub idle_timeout_secs: Option<u64>,
}

impl IdlePolicy {
    pub fn never_idle() -> Self {
        Self {
            idle_timeout_secs: None,
        }
    }

    pub fn timeout_secs(secs: u64) -> Self {
        Self {
            idle_timeout_secs: Some(secs),
        }
    }
}
