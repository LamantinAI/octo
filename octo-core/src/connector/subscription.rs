//! Bus subscription options + backpressure strategies.
//!
//! Backpressure operates on **two axes** in Octo:
//! - **Per-channel** (sucker-level, on connector side) — applied to the
//!   channel's outgoing mailbox.
//! - **Per-subscriber** (bus-level, on consumer side) — applied to a
//!   subscriber's intake from the bus, configured via [`SubscribeOptions`].
//!
//! See `runtime_architecture` § Backpressure.

use serde::{Deserialize, Serialize};

/// Per-subscriber options on the bus.
#[derive(Debug, Clone)]
pub struct SubscribeOptions {
    /// What to do when the subscriber falls behind.
    pub backpressure: BackpressureStrategy,
    /// Buffer size for this subscriber's intake.
    pub buffer: usize,
    /// Concurrency limit for this subscriber's handler.
    pub concurrency: usize,
    /// What to do if the handler panics.
    pub panic_policy: PanicPolicy,
}

impl Default for SubscribeOptions {
    fn default() -> Self {
        Self {
            backpressure: BackpressureStrategy::Block,
            buffer: 256,
            concurrency: 1,
            panic_policy: PanicPolicy::Restart,
        }
    }
}

impl SubscribeOptions {
    pub fn with_backpressure(mut self, b: BackpressureStrategy) -> Self {
        self.backpressure = b;
        self
    }

    pub fn with_buffer(mut self, n: usize) -> Self {
        self.buffer = n;
        self
    }

    pub fn with_concurrency(mut self, n: usize) -> Self {
        self.concurrency = n;
        self
    }

    pub fn with_panic_policy(mut self, p: PanicPolicy) -> Self {
        self.panic_policy = p;
        self
    }
}

/// Backpressure strategies. Reused at channel level (per-source mailbox) and
/// subscriber level (per-consumer intake).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub enum BackpressureStrategy {
    /// Drop the oldest queued item; lossy. Default for high-rate streaming.
    #[default]
    DropOldest,
    /// Drop the newest arrival; rare.
    DropNewest,
    /// Block the producer until space is available; suitable for delivery-critical paths.
    Block,
    /// Throttle to `rate_per_sec` events; mix of block + drop.
    Throttle { rate_per_sec: u32 },
    /// Merge incoming into the in-flight item — useful for bidirectional channels
    /// where a follow-up message should supersede an earlier one mid-flight.
    /// Borrowed from OpenClaw's "steer" mode.
    Steer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PanicPolicy {
    /// Restart the handler task; log the panic.
    Restart,
    /// Propagate the panic up — typically fatal for the runtime.
    Propagate,
    /// Kill this subscription but keep the runtime alive.
    Kill,
}
