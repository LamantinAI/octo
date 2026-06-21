//! Internal Event Bus — abstract trait + in-process implementation.
//!
//! All envelopes flow through the bus. Subscribers (reflex, memory writer,
//! observability, cognition) read independently. The bus does not interpret
//! envelopes — it only routes them by header fields.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::bus_queue::SubscriberInner;
use crate::{
    BackpressureStrategy, ChannelId, ConnectorId, Envelope, EventId, EventKind, OctoError,
    OctoResult, PayloadRegistry, SubscribeOptions,
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
/// Returned by [`EventBus::subscribe`]. Each subscription has its own bounded
/// intake whose overflow behaviour is set by [`SubscribeOptions::backpressure`].
/// Drop to unsubscribe — the bus prunes the slot on its next publish and any
/// publisher blocked on this subscriber (under `Block`) is released.
pub struct Subscription {
    inner: Arc<SubscriberInner>,
}

impl Subscription {
    /// Receive the next matching envelope. Returns `None` once the subscription
    /// is closed (dropped or bus shutdown) and drained.
    ///
    /// Envelopes are pre-filtered at publish time, so every value returned here
    /// already matches [`filter`](Self::filter). If this subscriber fell behind
    /// under a lossy strategy, the lag is counted in [`dropped_count`](Self::dropped_count)
    /// and warned about — never silently swallowed.
    pub async fn next(&mut self) -> Option<Arc<Envelope>> {
        self.inner.pop().await
    }

    pub fn filter(&self) -> &Filter {
        &self.inner.filter
    }

    /// Number of envelopes dropped for this subscriber because its buffer was
    /// full (always `0` under [`BackpressureStrategy::Block`]).
    pub fn dropped_count(&self) -> u64 {
        self.inner.dropped_count()
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.inner.close();
    }
}

/// In-process event bus — a registry of per-subscriber bounded queues.
///
/// The bus is the medium between actors inside the runtime ("бульон с веществами").
/// Not a transport abstraction — distributed messaging is explicitly out of scope.
///
/// `publish` is the fan-out pump: it snapshots the live subscribers whose
/// [`Filter`] matches and pushes a shared `Arc<Envelope>` into each one's intake,
/// applying that subscriber's [`BackpressureStrategy`] on overflow. A `Block`
/// subscriber backpressures the *publishing task*; lossy subscribers drop and
/// count it (see [`Subscription::dropped_count`]).
///
/// Supported strategies: `DropOldest`, `DropNewest`, `Block`. `Throttle` and
/// `Steer` are accepted but downgraded to `DropOldest` (with a warning) until
/// implemented.
pub struct InProcessBus {
    subscribers: Mutex<Vec<Arc<SubscriberInner>>>,
    /// Default intake buffer for subscribers that don't set one explicitly
    /// (e.g. the runtime's pre-subscribed cogitator/router/control).
    capacity: usize,
    next_id: AtomicU64,
    registry: Option<Arc<PayloadRegistry>>,
}

impl InProcessBus {
    pub fn new(capacity: usize) -> Self {
        Self {
            subscribers: Mutex::new(Vec::new()),
            capacity: capacity.max(1),
            next_id: AtomicU64::new(0),
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

    /// Default intake buffer size for subscribers created without explicit opts.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Number of live (not-yet-dropped) subscriptions.
    pub fn subscriber_count(&self) -> usize {
        let mut subs = self.subscribers.lock();
        subs.retain(|s| !s.is_closed());
        subs.len()
    }

    pub fn registry(&self) -> Option<&Arc<PayloadRegistry>> {
        self.registry.as_ref()
    }

    /// Register a new subscriber slot and return its [`Subscription`]. Shared by
    /// the sync and async subscribe paths.
    fn register(&self, filter: Filter, opts: SubscribeOptions) -> Subscription {
        let strategy = effective_strategy(opts.backpressure);
        let buffer = opts.buffer.max(1);
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let inner = SubscriberInner::new(id, filter, strategy, buffer);
        self.subscribers.lock().push(Arc::clone(&inner));
        Subscription { inner }
    }

    /// Synchronously create a subscription. Useful when a subscriber needs
    /// to register **before** any publisher starts emitting (e.g. the runtime
    /// pre-subscribes the cogitator before spawning connector tasks). Uses the
    /// default backpressure strategy with the bus's default buffer size.
    pub fn subscribe_sync(&self, filter: Filter) -> Subscription {
        self.subscribe_sync_with(
            filter,
            SubscribeOptions::default().with_buffer(self.capacity),
        )
    }

    /// As [`subscribe_sync`](Self::subscribe_sync), but with explicit options
    /// (buffer size / backpressure strategy).
    pub fn subscribe_sync_with(&self, filter: Filter, opts: SubscribeOptions) -> Subscription {
        self.register(filter, opts)
    }
}

#[async_trait]
impl EventBus for InProcessBus {
    async fn publish(&self, envelope: Envelope) -> OctoResult<()> {
        if let Some(reg) = &self.registry {
            // Validation errors are hard — caller should fix the mismatch.
            reg.validate(&envelope)?;
        }
        let env = Arc::new(envelope);

        // Snapshot the matching, live subscribers and prune dead slots — all
        // under the lock — then release it before any push so a `Block`
        // subscriber can never stall the registry or other publishers'
        // enumeration. A push to a zero-subscriber bus is a no-op, not an error.
        let targets: Vec<Arc<SubscriberInner>> = {
            let mut subs = self.subscribers.lock();
            subs.retain(|s| !s.is_closed());
            subs.iter()
                .filter(|s| s.filter.matches(&env))
                .cloned()
                .collect()
        };

        for target in targets {
            // Outcome is advisory: drops are counted inside the subscriber, and a
            // `Closed` slot is reaped on the next publish's retain.
            let _ = target.push(Arc::clone(&env)).await;
        }
        Ok(())
    }

    async fn subscribe(&self, filter: Filter, opts: SubscribeOptions) -> OctoResult<Subscription> {
        Ok(self.register(filter, opts))
    }
}

impl Drop for InProcessBus {
    fn drop(&mut self) {
        // Closing each subscriber makes a parked `Subscription::next` return
        // `None` (preserving the old "bus closed → stream ends" semantics) and
        // releases any publisher blocked on a `Block` subscriber.
        for s in self.subscribers.lock().iter() {
            s.close();
        }
    }
}

/// Map a requested strategy to one the in-process bus implements, warning once
/// when a not-yet-supported mode is downgraded.
fn effective_strategy(requested: BackpressureStrategy) -> BackpressureStrategy {
    match requested {
        BackpressureStrategy::Throttle { .. } | BackpressureStrategy::Steer => {
            tracing::warn!(
                requested = ?requested,
                "backpressure strategy not yet implemented by InProcessBus; using DropOldest"
            );
            BackpressureStrategy::DropOldest
        }
        other => other,
    }
}
