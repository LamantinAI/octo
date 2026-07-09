//! Internal Event Bus — abstract trait + in-process implementation.
//!
//! All envelopes flow through the bus. Subscribers (reflex, memory writer,
//! observability, cognition) read independently. The bus does not interpret
//! envelopes — it only routes them by header fields.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, Notify};

use crate::{
    BackpressureStrategy, ChannelId, ConnectorId, Envelope, EventId, EventKind, OctoError,
    OctoResult, PayloadRegistry, SubscribeOptions, TrustLevel,
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
    /// Minimum channel trust. An envelope carrying `channel_metadata` **below**
    /// this level is filtered out — the general, runtime-level trust gate a
    /// subscriber (e.g. a cogitator) sets so untrusted input never reaches it.
    /// Envelopes with **no** `channel_metadata` (internal/system traffic) pass.
    pub min_trust: Option<TrustLevel>,
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

    /// Require a minimum channel trust. Channel-tagged envelopes below `level`
    /// are filtered out; envelopes with no `channel_metadata` (internal traffic)
    /// still pass. A general trust gate, complementary to a connector dropping
    /// untrusted at its own edge.
    pub fn with_min_trust(mut self, level: TrustLevel) -> Self {
        self.min_trust = Some(level);
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
        if let Some(min) = self.min_trust {
            // Only channel-tagged envelopes are gated; internal/system traffic
            // (no channel_metadata) passes through.
            if let Some(meta) = &env.channel_metadata {
                if meta.trust.rank() < min.rank() {
                    return false;
                }
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
/// Returned by [`EventBus::subscribe`] / [`InProcessBus::subscribe_sync`]. Drop
/// to unsubscribe. Two intake modes, chosen by [`SubscribeOptions`]:
///
/// - **Raw broadcast** (the default `DropOldest`) — rides the broadcast receiver
///   directly. Lag drops the oldest, but the dropped count is now surfaced
///   (warned + counted via [`Subscription::lagged_total`]), never silent.
/// - **Per-subscriber shim** — a forwarder task drains the broadcast into this
///   subscriber's own bounded queue, applying its strategy (deep buffer,
///   drop-newest, throttle, steer, best-effort block) without stalling others.
pub struct Subscription {
    inner: SubInner,
    filter: Filter,
    /// Envelopes dropped for *this* subscriber (broadcast lag + shim policy).
    lagged: Arc<AtomicU64>,
}

enum SubInner {
    /// Native fan-out path: drop-oldest at the broadcast's own capacity.
    Broadcast(broadcast::Receiver<Arc<Envelope>>),
    /// Per-subscriber queue fed by a forwarder task (filter applied there).
    Shim(Arc<ShimChan>),
}

/// Per-subscriber shim queue + signalling, shared between the forwarder task
/// (producer) and the [`Subscription`] (consumer).
struct ShimChan {
    queue: Mutex<VecDeque<Arc<Envelope>>>,
    /// Consumer waits here for an item.
    data: Notify,
    /// Forwarder waits here for room (only `Block` parks on it).
    space: Notify,
    /// Set once the forwarder ends (broadcast closed).
    closed: AtomicBool,
}

impl Subscription {
    /// Receive the next matching envelope. Returns `None` when the bus closes
    /// (and, for the shim, after draining anything still queued).
    pub async fn next(&mut self) -> Option<Arc<Envelope>> {
        match &mut self.inner {
            SubInner::Broadcast(rx) => loop {
                match rx.recv().await {
                    Ok(env) => {
                        if self.filter.matches(&env) {
                            return Some(env);
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => return None,
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        let total = self.lagged.fetch_add(skipped, Ordering::Relaxed) + skipped;
                        tracing::warn!(
                            lagged = skipped,
                            total,
                            "bus subscription fell behind — dropped {skipped} envelopes (drop-oldest)"
                        );
                    }
                }
            },
            SubInner::Shim(chan) => loop {
                if let Some(env) = chan.queue.lock().unwrap().pop_front() {
                    chan.space.notify_one();
                    return Some(env);
                }
                if chan.closed.load(Ordering::Acquire) {
                    // Forwarder gone; hand back any straggler, else end.
                    return chan.queue.lock().unwrap().pop_front();
                }
                chan.data.notified().await;
            },
        }
    }

    pub fn filter(&self) -> &Filter {
        &self.filter
    }

    /// Total envelopes dropped for this subscriber (broadcast lag + shim policy).
    /// Zero on a healthy, never-overflowing subscription.
    pub fn lagged_total(&self) -> u64 {
        self.lagged.load(Ordering::Relaxed)
    }
}

/// Key a `Steer` subscriber supersedes on: a follow-up on the same channel (or
/// correlation) replaces the still-queued earlier one.
#[derive(PartialEq, Eq)]
enum SteerKey {
    Channel(ChannelId),
    Correlation(EventId),
}

fn steer_key(env: &Envelope) -> Option<SteerKey> {
    if let Some(c) = &env.channel {
        return Some(SteerKey::Channel(c.clone()));
    }
    env.correlation_id.map(SteerKey::Correlation)
}

/// Spawn the forwarder draining a broadcast receiver into a subscriber's shim
/// queue under its backpressure strategy. Exits when the broadcast closes.
fn spawn_forwarder(
    mut rx: broadcast::Receiver<Arc<Envelope>>,
    filter: Filter,
    opts: SubscribeOptions,
    chan: Arc<ShimChan>,
    lagged: Arc<AtomicU64>,
) {
    let buffer = opts.buffer.max(1);
    tokio::spawn(async move {
        let mut last_emit: Option<tokio::time::Instant> = None; // Throttle only
        loop {
            let env = match rx.recv().await {
                Ok(env) => env,
                Err(broadcast::error::RecvError::Closed) => break,
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    let total = lagged.fetch_add(skipped, Ordering::Relaxed) + skipped;
                    tracing::warn!(
                        lagged = skipped,
                        total,
                        "bus shim fell behind upstream — dropped {skipped} envelopes"
                    );
                    continue;
                }
            };
            if !filter.matches(&env) {
                continue;
            }
            match &opts.backpressure {
                BackpressureStrategy::DropOldest => {
                    let mut q = chan.queue.lock().unwrap();
                    q.push_back(env);
                    if q.len() > buffer {
                        q.pop_front();
                        lagged.fetch_add(1, Ordering::Relaxed);
                    }
                }
                BackpressureStrategy::DropNewest => {
                    let mut q = chan.queue.lock().unwrap();
                    if q.len() >= buffer {
                        lagged.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    q.push_back(env);
                }
                BackpressureStrategy::Steer => {
                    let key = steer_key(&env);
                    let mut q = chan.queue.lock().unwrap();
                    match key.and_then(|k| q.iter().position(|e| steer_key(e).as_ref() == Some(&k)))
                    {
                        // Supersede in place — the latest wins, position kept.
                        // Not a drop: superseding is the intended Steer behavior.
                        Some(i) => q[i] = env,
                        None => {
                            q.push_back(env);
                            if q.len() > buffer {
                                q.pop_front();
                                lagged.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
                BackpressureStrategy::Throttle { rate_per_sec } => {
                    let min_gap = Duration::from_secs_f64(1.0 / (*rate_per_sec).max(1) as f64);
                    let now = tokio::time::Instant::now();
                    if last_emit.is_some_and(|t| now.duration_since(t) < min_gap) {
                        lagged.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    last_emit = Some(now);
                    chan.queue.lock().unwrap().push_back(env);
                }
                BackpressureStrategy::Block => {
                    // Best-effort: park until the consumer makes room. While
                    // parked we stop draining the broadcast, so *it* (not us)
                    // may lag — true end-to-end Block is impossible on a fan-out.
                    while chan.queue.lock().unwrap().len() >= buffer {
                        chan.space.notified().await;
                    }
                    chan.queue.lock().unwrap().push_back(env);
                }
            }
            chan.data.notify_one();
        }
        chan.closed.store(true, Ordering::Release);
        chan.data.notify_one();
    });
}

/// In-process event bus — `tokio::sync::broadcast` based.
///
/// The bus is the medium between actors inside the runtime (the "broth").
/// Not a transport abstraction — distributed messaging is explicitly out of scope.
///
/// Backpressure: the default subscription rides the broadcast directly
/// (drop-oldest, now with visible lag counts). A subscriber wanting other
/// semantics (deep buffer, drop-newest, throttle, steer, best-effort block) gets
/// a per-subscriber shim — a forwarder task + bounded queue — via
/// [`SubscribeOptions`]; see [`Subscription`]. (Distributed / criticality-lane
/// guarantees remain out of scope.)
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
    /// pre-subscribes the cogitator before spawning connector tasks). The
    /// broadcast receiver is registered synchronously here; any shim forwarder
    /// then drains it, so the pre-subscribe guarantee holds in both modes.
    pub fn subscribe_sync(&self, filter: Filter, opts: SubscribeOptions) -> Subscription {
        self.make_subscription(filter, opts)
    }

    /// Build a subscription, picking the raw broadcast path or the per-subscriber
    /// shim per [`SubscribeOptions::needs_shim`].
    fn make_subscription(&self, filter: Filter, opts: SubscribeOptions) -> Subscription {
        // Register the broadcast receiver now (before returning) so no publish
        // is missed between subscribe and the forwarder being scheduled.
        let rx = self.sender.subscribe();
        let lagged = Arc::new(AtomicU64::new(0));
        if !opts.needs_shim(self.capacity) {
            return Subscription { inner: SubInner::Broadcast(rx), filter, lagged };
        }
        let chan = Arc::new(ShimChan {
            queue: Mutex::new(VecDeque::new()),
            data: Notify::new(),
            space: Notify::new(),
            closed: AtomicBool::new(false),
        });
        spawn_forwarder(rx, filter.clone(), opts, Arc::clone(&chan), Arc::clone(&lagged));
        Subscription { inner: SubInner::Shim(chan), filter, lagged }
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
        opts: SubscribeOptions,
    ) -> OctoResult<Subscription> {
        Ok(self.make_subscription(filter, opts))
    }
}
