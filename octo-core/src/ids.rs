//! Identifiers — connector, channel, event, rule.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Unique event identifier — UUID v7 for temporal ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EventId(pub Uuid);

impl EventId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

impl Default for EventId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for EventId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Connector identifier — string-based, stable across restarts.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConnectorId(String);

impl ConnectorId {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ConnectorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for ConnectorId {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for ConnectorId {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

/// Channel identifier — string-based, scoped per connector.
///
/// Convention: `<topic>` or `<chat_id>` or `<source_path>`. Channel is the
/// "присоска" (sucker) on a connector ("щупальце") — see `connector_channel_split`
/// vault draft.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChannelId(String);

impl ChannelId {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ChannelId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for ChannelId {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for ChannelId {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

/// Reflex rule identifier — string-based, stable per config.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RuleId(String);

impl RuleId {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for RuleId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for RuleId {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}
