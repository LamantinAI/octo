//! Cognition tier — the core of the runtime.
//!
//! Architectural note: the cognition tier is **always present in the pipeline**.
//! Its *work* varies — from a no-op observer ([`EmptyCogitator`]) to a full
//! deliberation backend (LLM / planner / FSM / HITL, in sibling crates).
//! Its *presence* doesn't.
//!
//! Core ships only:
//! - The [`Cogitator`] trait + [`CogitatorContext`].
//! - [`EmptyCogitator`] — no-op observer, the default in
//!   [`OctoBuilder`](crate::OctoBuilder).
//!
//! Declarative routing logic (the historical "reflex tier") now lives in the
//! [`crate::router`] module — data-driven, with `Route` predicates instead
//! of closure-based rules. Real deliberation (LLM, planner, FSM, HITL)
//! belongs in sibling crates that ship their own [`Cogitator`]
//! implementations.

use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::{
    bus::{EventBus, Filter, InProcessBus, Subscription},
    ConnectorCapabilities, ConnectorId, Envelope, OctoResult, SubscribeOptions,
};

/// A runtime snapshot of one registered connector, handed to the cogitator so
/// it can build its env-as-tools catalogue from the live runtime (not from
/// hand-wiring). Connectors that set [`ConnectorCapabilities::description`]
/// advertise themselves as agent-callable tools.
#[derive(Debug, Clone)]
pub struct ConnectorInfo {
    pub id: ConnectorId,
    pub capabilities: ConnectorCapabilities,
}

/// A cogitator — the actor inhabiting the cognition tier.
///
/// One per [`Octo`](crate::Octo) instance. The runtime pre-subscribes the
/// cogitator before any connector starts publishing, so it observes every
/// envelope from t=0 — no race.
#[async_trait]
pub trait Cogitator: Send + Sync + 'static {
    /// Stable identifier for this cogitator instance.
    fn id(&self) -> &str;

    /// Filter for the cogitator's bus subscription. Default: see everything.
    fn filter(&self) -> Filter {
        Filter::all()
    }

    /// Run the cogitator's main loop. Receives:
    /// - `ctx` — shutdown signal + bus handle (publish + late subscribe).
    /// - `subscription` — pre-made subscription on the bus (per `filter()`),
    ///    guaranteed to be registered before any connector publishes.
    async fn run(
        self: Arc<Self>,
        ctx: CogitatorContext,
        subscription: Subscription,
    ) -> OctoResult<()>;
}

/// Runtime context for a cogitator. Carries shutdown + bus handle for
/// publishing follow-up actions or registering additional subscriptions.
pub struct CogitatorContext {
    pub shutdown: CancellationToken,
    bus: Arc<InProcessBus>,
    connectors: Vec<ConnectorInfo>,
}

impl CogitatorContext {
    pub fn new(
        shutdown: CancellationToken,
        bus: Arc<InProcessBus>,
        connectors: Vec<ConnectorInfo>,
    ) -> Self {
        Self {
            shutdown,
            bus,
            connectors,
        }
    }

    /// The connectors registered in the runtime — the cogitator's environment.
    /// Build the env-as-tools catalogue from those with a `description`.
    pub fn connectors(&self) -> &[ConnectorInfo] {
        &self.connectors
    }

    /// A clone of the runtime's bus handle. Lets a cogitator hand dispatch
    /// capability to a sub-component (e.g. an LLM tool that emits commands to
    /// connectors and awaits the response).
    pub fn bus(&self) -> Arc<InProcessBus> {
        Arc::clone(&self.bus)
    }

    /// Publish an envelope onto the bus (typically the cogitator's
    /// follow-up action / decision).
    pub async fn publish(&self, envelope: Envelope) -> OctoResult<()> {
        self.bus.publish(envelope).await
    }

    /// Late-subscribe (in addition to the pre-made subscription handed to
    /// `run`) — useful when a cogitator wants additional filtered streams.
    pub async fn subscribe(
        &self,
        filter: Filter,
        opts: SubscribeOptions,
    ) -> OctoResult<Subscription> {
        self.bus.subscribe(filter, opts).await
    }

    /// Publish a command and await a single correlated reply — broker-style
    /// request/response. Mirrors [`EventBus::publish_and_await_response`].
    pub async fn publish_and_await_response(
        &self,
        request: Envelope,
        timeout: std::time::Duration,
    ) -> OctoResult<std::sync::Arc<Envelope>> {
        self.bus.publish_and_await_response(request, timeout).await
    }
}

/// Built-in no-op cogitator — observes every envelope, takes no action.
///
/// Sufficient default for the runtime to be "complete" — the core is always
/// present, even if it's not yet deciding anything. Replace with a real
/// cogitator (LLM-backed, planner, FSM, ...) via [`OctoBuilder::cogitator`](
/// crate::OctoBuilder::cogitator).
///
/// Observations are emitted to **stderr** for visibility during development.
/// When an observability story is settled, this will switch to `tracing`.
pub struct EmptyCogitator {
    id: String,
}

impl EmptyCogitator {
    pub fn new(id: impl Into<String>) -> Arc<Self> {
        Arc::new(Self { id: id.into() })
    }
}

#[async_trait]
impl Cogitator for EmptyCogitator {
    fn id(&self) -> &str {
        &self.id
    }

    async fn run(
        self: Arc<Self>,
        ctx: CogitatorContext,
        mut subscription: Subscription,
    ) -> OctoResult<()> {
        let mut observed: u64 = 0;
        loop {
            tokio::select! {
                next = subscription.next() => match next {
                    Some(envelope) => {
                        observed += 1;
                        eprintln!(
                            "[cogitator {}] observe #{observed}: kind={} src={} target={}",
                            self.id,
                            envelope.kind,
                            envelope.source,
                            envelope.target.as_ref()
                                .map(|t| t.as_str())
                                .unwrap_or("(none)"),
                        );
                    }
                    None => {
                        eprintln!("[cogitator {}] bus closed; observed {observed} total", self.id);
                        return Ok(());
                    }
                },
                _ = ctx.shutdown.cancelled() => {
                    eprintln!(
                        "[cogitator {}] shutdown; observed {observed} total",
                        self.id
                    );
                    return Ok(());
                }
            }
        }
    }
}
