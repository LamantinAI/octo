//! Internal Event Bus — abstract trait + in-process implementation.
//!
//! All envelopes flow through the bus. Subscribers (reflex, memory writer,
//! observability, cognition) read independently. The bus does not interpret
//! envelopes — it only routes them by header fields.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::{
    ChannelId, ConnectorId, Envelope, EventId, EventKind, OctoError, OctoResult, PayloadRegistry,
    SubscribeOptions,
};

/// Abstraction over the in-process bus — the medium between connector actors
/// and domain-core actors. Trait stays minimal so alternative in-process impls
/// (e.g. mpsc-shim with per-subscriber backpressure) can slot in later without
/// rewriting subscribers.
#[async_trait]
pub trait EventBus: Send + Sync {
    async fn publish(&self, envelope: Envelope) -> OctoResult<()>;

    async fn subscribe(
        &self,
        filter: Filter,
        opts: SubscribeOptions,
    ) -> OctoResult<Subscription>;

    /// Publish a command envelope and await a single response envelope
    /// correlated by id — broker-style request/response.
    ///
    /// Subscribes by `Filter::by_correlation(request.id)` **before** publishing,
    /// so a fast responder cannot beat the subscription. Returns the first
    /// envelope matching that correlation_id.
    ///
    /// Errors:
    /// - [`OctoError::Timeout`] if no matching envelope arrived in `timeout`.
    /// - [`OctoError::BusClosed`] if the bus closed before any response.
    ///
    /// For streaming or fan-out cases, do **not** use this helper — set up the
    /// subscription manually and loop. See `request_response_pattern` draft
    /// in the research vault.
    async fn publish_and_await_response(
        &self,
        request: Envelope,
        timeout: Duration,
    ) -> OctoResult<Arc<Envelope>> {
        let correlation_id = request.id;
        let mut sub = self
            .subscribe(
                Filter::by_correlation(correlation_id),
                SubscribeOptions::default(),
            )
            .await?;
        self.publish(request).await?;
        match tokio::time::timeout(timeout, sub.next()).await {
            Ok(Some(envelope)) => Ok(envelope),
            Ok(None) => Err(OctoError::BusClosed),
            Err(_) => Err(OctoError::Timeout { correlation_id }),
        }
    }
}

/// Subscription filter — declarative; combined predicates are AND-joined.
/// `None` on a field means "any".
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Filter {
    pub kinds: Option<Vec<KindPattern>>,
    pub sources: Option<Vec<ConnectorId>>,
    pub targets: Option<Vec<ConnectorId>>,
    pub channels: Option<Vec<ChannelId>>,
    /// Match envelopes whose `correlation_id` is one of these. Used by the
    /// broker-style request/response pattern: subscriber filters by the id of
    /// the command it published, responder sets `correlation_id` on the reply.
    pub correlation_ids: Option<Vec<EventId>>,
}

impl Filter {
    pub fn all() -> Self {
        Self::default()
    }

    pub fn by_kind(pattern: impl Into<KindPattern>) -> Self {
        Self {
            kinds: Some(vec![pattern.into()]),
            ..Default::default()
        }
    }

    pub fn by_source(source: ConnectorId) -> Self {
        Self {
            sources: Some(vec![source]),
            ..Default::default()
        }
    }

    pub fn by_target(target: ConnectorId) -> Self {
        Self {
            targets: Some(vec![target]),
            ..Default::default()
        }
    }

    /// Match envelopes carrying this `correlation_id`. The cornerstone of the
    /// broker-style request/response pattern — see `EventBus::publish_and_await_response`.
    pub fn by_correlation(correlation_id: EventId) -> Self {
        Self {
            correlation_ids: Some(vec![correlation_id]),
            ..Default::default()
        }
    }

    pub fn with_kind(mut self, pattern: impl Into<KindPattern>) -> Self {
        self.kinds.get_or_insert_with(Vec::new).push(pattern.into());
        self
    }

    pub fn with_source(mut self, source: ConnectorId) -> Self {
        self.sources.get_or_insert_with(Vec::new).push(source);
        self
    }

    pub fn with_target(mut self, target: ConnectorId) -> Self {
        self.targets.get_or_insert_with(Vec::new).push(target);
        self
    }

    pub fn with_channel(mut self, channel: ChannelId) -> Self {
        self.channels.get_or_insert_with(Vec::new).push(channel);
        self
    }

    pub fn with_correlation(mut self, correlation_id: EventId) -> Self {
        self.correlation_ids
            .get_or_insert_with(Vec::new)
            .push(correlation_id);
        self
    }

    pub fn matches(&self, env: &Envelope) -> bool {
        if let Some(kinds) = &self.kinds {
            if !kinds.iter().any(|p| p.matches(&env.kind)) {
                return false;
            }
        }
        if let Some(sources) = &self.sources {
            if !sources.contains(&env.source) {
                return false;
            }
        }
        if let Some(targets) = &self.targets {
            match &env.target {
                Some(t) if targets.contains(t) => {}
                _ => return false,
            }
        }
        if let Some(channels) = &self.channels {
            match &env.channel {
                Some(c) if channels.contains(c) => {}
                _ => return false,
            }
        }
        if let Some(correlation_ids) = &self.correlation_ids {
            match &env.correlation_id {
                Some(cid) if correlation_ids.contains(cid) => {}
                _ => return false,
            }
        }
        true
    }
}

/// Glob pattern for matching event kinds (see [`EventKind::matches`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KindPattern(String);

impl KindPattern {
    pub fn new(p: impl Into<String>) -> Self {
        Self(p.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn matches(&self, kind: &EventKind) -> bool {
        kind.matches(&self.0)
    }
}

impl From<&str> for KindPattern {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for KindPattern {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

impl From<EventKind> for KindPattern {
    fn from(k: EventKind) -> Self {
        Self::new(k.as_str().to_owned())
    }
}

/// Subscriber-side stream over filtered envelopes.
///
/// Returned by [`EventBus::subscribe`]. Drop to unsubscribe.
pub struct Subscription {
    rx: broadcast::Receiver<Arc<Envelope>>,
    filter: Filter,
}

impl Subscription {
    /// Receive the next matching envelope. Returns `None` when the bus closes.
    ///
    /// On lag (subscriber fell behind broadcast capacity) the lagged events
    /// are silently skipped; this matches `DropOldest` semantics in
    /// `tokio::sync::broadcast`. More elaborate per-subscriber backpressure
    /// modes are not yet wired in the in-process impl — see TODO in `InProcessBus`.
    pub async fn next(&mut self) -> Option<Arc<Envelope>> {
        loop {
            match self.rx.recv().await {
                Ok(env) => {
                    if self.filter.matches(&env) {
                        return Some(env);
                    }
                }
                Err(broadcast::error::RecvError::Closed) => return None,
                Err(broadcast::error::RecvError::Lagged(_skipped)) => {
                    // TODO: surface lag count via tracing / metrics
                    continue;
                }
            }
        }
    }

    pub fn filter(&self) -> &Filter {
        &self.filter
    }
}

/// In-process event bus — `tokio::sync::broadcast` based.
///
/// The bus is the medium between actors inside the runtime ("бульон с веществами").
/// Not a transport abstraction — distributed messaging is explicitly out of scope.
///
/// TODO: per-subscriber backpressure modes from [`SubscribeOptions`] are not
/// yet honored — broadcast inherently has drop-oldest semantics on lag; `Block`
/// / `Throttle` / `Steer` need a shim layer with per-subscriber mpsc fan-out.
pub struct InProcessBus {
    sender: broadcast::Sender<Arc<Envelope>>,
    capacity: usize,
    registry: Option<Arc<PayloadRegistry>>,
}

impl InProcessBus {
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self {
            sender,
            capacity,
            registry: None,
        }
    }

    /// Attach a payload registry. With it, every publish call validates the
    /// envelope's payload type against the registry's entry for its kind.
    /// Without it, no validation (current behaviour).
    pub fn with_registry(mut self, registry: Arc<PayloadRegistry>) -> Self {
        self.registry = Some(registry);
        self
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn subscriber_count(&self) -> usize {
        self.sender.receiver_count()
    }

    pub fn registry(&self) -> Option<&Arc<PayloadRegistry>> {
        self.registry.as_ref()
    }

    /// Synchronously create a subscription. Useful when a subscriber needs
    /// to register **before** any publisher starts emitting (e.g. the runtime
    /// pre-subscribes the cogitator before spawning connector tasks).
    pub fn subscribe_sync(&self, filter: Filter) -> Subscription {
        Subscription {
            rx: self.sender.subscribe(),
            filter,
        }
    }
}

#[async_trait]
impl EventBus for InProcessBus {
    async fn publish(&self, envelope: Envelope) -> OctoResult<()> {
        if let Some(reg) = &self.registry {
            // Validation errors are hard — caller should fix the mismatch.
            reg.validate(&envelope)?;
        }
        // `send` returns Err only if there are zero receivers — that's not an
        // error for a fanout bus (events with no subscribers are silently dropped).
        let _ = self.sender.send(Arc::new(envelope));
        Ok(())
    }

    async fn subscribe(
        &self,
        filter: Filter,
        _opts: SubscribeOptions,
    ) -> OctoResult<Subscription> {
        // TODO: opts ignored — see struct-level TODO.
        Ok(Subscription {
            rx: self.sender.subscribe(),
            filter,
        })
    }
}
