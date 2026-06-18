//! Router — singleton actor performing data-driven routing of envelopes.
//!
//! The router holds a table of [`Route`]s (data, not code). For each envelope
//! on the bus, it evaluates the routes in priority order; on match, emits an
//! action envelope with `target` set. Routes can be `Terminate` (first-match-
//! wins), `Enrich` (continue evaluation), or `Observe` (record only, no emit).
//!
//! See `router` and `manageable_actors` vault drafts for the design.
//!
//! ## Position in the runtime
//!
//! - One router per `Octo` instance (singleton slot).
//! - Pre-subscribed by the runtime before connectors are spawned, like the
//!   cogitator. Guarantees no early publishes are missed.
//! - Lives alongside the cogitator: both subscribe to the bus independently.
//!   Coordination is via the trail field in envelopes — the cogitator sees
//!   both originals and the router's emissions.

pub mod route;
pub mod rule_based;

pub use route::{Route, RouteAction, RouteId, RoutePredicate, RouteStrategy};
pub use rule_based::{RouterState, RuleBasedRouter, RuleBasedRouterBuilder};

use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::{
    bus::{EventBus, Filter, InProcessBus, Subscription},
    Envelope, OctoResult,
};

#[async_trait]
pub trait Router: Send + Sync + 'static {
    fn id(&self) -> &str;

    /// Which envelopes the router wants to see. Default-implementations
    /// usually return `Filter::all()`.
    fn filter(&self) -> Filter {
        Filter::all()
    }

    /// Main loop. Receives a pre-built subscription (synchronously registered
    /// by the runtime, same pattern as the cogitator) so no early envelopes
    /// are missed.
    async fn run(
        self: Arc<Self>,
        ctx: RouterContext,
        subscription: Subscription,
    ) -> OctoResult<()>;
}

/// Runtime-provided context for the router. Carries shutdown signal and a
/// handle to publish action envelopes onto the bus.
pub struct RouterContext {
    pub shutdown: CancellationToken,
    bus: Arc<InProcessBus>,
}

impl RouterContext {
    pub fn new(shutdown: CancellationToken, bus: Arc<InProcessBus>) -> Self {
        Self { shutdown, bus }
    }

    /// Publish an envelope onto the runtime's bus.
    pub async fn publish(&self, envelope: Envelope) -> OctoResult<()> {
        self.bus.publish(envelope).await
    }
}

