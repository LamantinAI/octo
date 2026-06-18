//! Lifecycle FSM and restart policies.
//!
//! Applies on **two levels**:
//! - **Connector level** — protocol/transport ownership (single long-lived task).
//! - **Channel level** — per-source escalation path (dynamic, lighter-weight).
//!
//! See `connector_channel_split` vault draft for the rationale.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Lifecycle {
    /// Just constructed, not yet registered with supervisor.
    Created,
    /// Registering — performing handshake, warmup, or initial subscription.
    Registering,
    /// Operating normally; heartbeat present.
    Healthy,
    /// Recoverable trouble (heartbeat lag, transient errors).
    Degraded,
    /// Unrecoverable; supervisor decides restart vs stop.
    Unhealthy,
    /// Graceful shutdown in progress.
    Stopping,
    /// Terminated.
    Stopped,
}

impl Lifecycle {
    /// Whether transitioning from `self` to `next` is allowed.
    pub fn can_transition_to(self, next: Lifecycle) -> bool {
        use Lifecycle::*;
        match (self, next) {
            (Created, Registering) => true,
            (Registering, Healthy) => true,
            (Registering, Unhealthy) => true,
            (Registering, Stopping) => true,
            (Healthy, Degraded) => true,
            (Healthy, Unhealthy) => true,
            (Healthy, Stopping) => true,
            (Degraded, Healthy) => true,
            (Degraded, Unhealthy) => true,
            (Degraded, Stopping) => true,
            (Unhealthy, Stopping) => true,
            (Unhealthy, Stopped) => true,
            (Stopping, Stopped) => true,
            _ => false,
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Lifecycle::Stopped)
    }

    pub fn is_running(self) -> bool {
        matches!(self, Lifecycle::Healthy | Lifecycle::Degraded)
    }
}

impl std::fmt::Display for Lifecycle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Lifecycle::Created => "created",
            Lifecycle::Registering => "registering",
            Lifecycle::Healthy => "healthy",
            Lifecycle::Degraded => "degraded",
            Lifecycle::Unhealthy => "unhealthy",
            Lifecycle::Stopping => "stopping",
            Lifecycle::Stopped => "stopped",
        };
        f.write_str(s)
    }
}

/// Per-actor restart policy. Applied independently at connector and channel levels.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum RestartPolicy {
    /// Always restart on any failure (use sparingly).
    Always,
    /// Never restart; failure is terminal.
    Never,
    /// Restart up to N attempts, then give up.
    MaxAttempts(u32),
    /// Exponential backoff with optional cap on attempts.
    ExponentialBackoff {
        initial_ms: u64,
        max_ms: u64,
        max_attempts: Option<u32>,
    },
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self::ExponentialBackoff {
            initial_ms: 1000,
            max_ms: 30_000,
            max_attempts: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allowed_transitions() {
        assert!(Lifecycle::Created.can_transition_to(Lifecycle::Registering));
        assert!(Lifecycle::Registering.can_transition_to(Lifecycle::Healthy));
        assert!(Lifecycle::Healthy.can_transition_to(Lifecycle::Degraded));
        assert!(Lifecycle::Degraded.can_transition_to(Lifecycle::Healthy));
        assert!(Lifecycle::Stopping.can_transition_to(Lifecycle::Stopped));
    }

    #[test]
    fn forbidden_transitions() {
        assert!(!Lifecycle::Created.can_transition_to(Lifecycle::Healthy));
        assert!(!Lifecycle::Stopped.can_transition_to(Lifecycle::Healthy));
        assert!(!Lifecycle::Healthy.can_transition_to(Lifecycle::Created));
    }

    #[test]
    fn predicates() {
        assert!(Lifecycle::Healthy.is_running());
        assert!(Lifecycle::Degraded.is_running());
        assert!(!Lifecycle::Unhealthy.is_running());
        assert!(Lifecycle::Stopped.is_terminal());
    }
}
