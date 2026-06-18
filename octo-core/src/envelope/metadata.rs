//! Channel-level metadata, priority, trust, reply addressing.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::ChannelId;

/// Per-channel metadata propagated into envelopes for conditional reflex predicates.
///
/// Surface'ed by the Telegram-bot reference scenario (multi-tenant trust gradient
/// owner / family / support / public).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChannelMetadata {
    pub trust: TrustLevel,
    pub priority: Priority,
    pub tags: HashMap<String, String>,
}

impl ChannelMetadata {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_trust(mut self, trust: TrustLevel) -> Self {
        self.trust = trust;
        self
    }

    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }

    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }
}

/// Trust gradient for channel authorisation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum TrustLevel {
    High,
    Medium,
    #[default]
    Low,
    Untrusted,
}

/// Priority hint for dispatcher ordering and rate-limit class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Priority {
    High,
    #[default]
    Normal,
    Low,
}

/// Reply address — used by bidirectional connectors to thread responses back.
///
/// `channel` identifies where to reply; `message_ref` (when set) lets the
/// connector reply to a specific message (e.g. Telegram's `reply_to_message_id`,
/// or edit a previous message in act+escalate mode).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplyChannel {
    pub channel: ChannelId,
    pub message_ref: Option<String>,
}

impl ReplyChannel {
    pub fn new(channel: ChannelId) -> Self {
        Self {
            channel,
            message_ref: None,
        }
    }

    pub fn with_message_ref(mut self, msg_ref: impl Into<String>) -> Self {
        self.message_ref = Some(msg_ref.into());
        self
    }
}
