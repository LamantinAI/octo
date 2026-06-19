//! Connector trait and capability declaration.
//!
//! A **connector** is the protocol/transport layer — describes *what source*,
//! its *direction*, and the *transport implementation* (Bot API long-polling,
//! MQTT subscribe, RTSP handler). One connector instance per "way to reach
//! into the world". Channels (присоски) live inside connectors.
//!
//! This module groups everything that defines a connector and its companion
//! abstractions:
//! - [`Connector`] trait + [`ConnectorContext`] + [`ConnectorCapabilities`] (this file).
//! - [`channel`] — sub-actor inside a connector (the "sucker").
//! - [`subscription`] — bus subscription options + backpressure strategies.
//! - [`lifecycle`] — FSM and restart policies (applied on both connector and channel level).
//!
//! See `connector_channel_split` and `runtime_architecture` vault drafts.

pub mod channel;
pub mod lifecycle;
pub mod subscription;

pub use channel::{ChannelDescriptor, IdlePolicy};
pub use lifecycle::{Lifecycle, RestartPolicy};
pub use subscription::{BackpressureStrategy, PanicPolicy, SubscribeOptions};

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::{
    bus::{EventBus, Filter, InProcessBus, Subscription},
    ConnectorId, Envelope, EventKind, OctoResult,
};

/// A long-lived I/O actor wrapping a single protocol/transport.
///
/// Implementations:
/// - own connection state and reconnection logic;
/// - publish envelopes to the bus on their own cadence via `ctx.publish(...)`;
/// - accept actions from the bus when `direction` allows (via `ctx.subscribe(...)`);
/// - manage a dynamic set of [`Channel`](crate::channel::ChannelDescriptor)
///   instances per source (chat_id, topic, stream_id, ...).
#[async_trait]
pub trait Connector: Send + Sync + 'static {
    /// Stable identifier for this connector instance.
    fn id(&self) -> &ConnectorId;

    /// What this connector can do — used by dispatcher and capability negotiation.
    fn capabilities(&self) -> &ConnectorCapabilities;

    /// Run the connector's main loop. Should respect `ctx.shutdown` to stop
    /// gracefully on cancellation.
    async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()>;

    /// Restart policy the runtime's supervisor applies when this connector's
    /// `run` returns an error or panics. Default: exponential backoff with no
    /// attempt limit — keep an always-on connector trying. A clean exit
    /// (`Ok(())`, e.g. on shutdown) is never restarted.
    fn restart_policy(&self) -> RestartPolicy {
        RestartPolicy::default()
    }

    /// Register this connector's payload kinds (and any schemas) in the shared
    /// [`PayloadRegistry`](crate::PayloadRegistry). Builder-style: consumes and
    /// returns the registry. Default is a no-op so existing connectors need no
    /// change; connectors that own typed kinds override it. The runtime calls
    /// this for connectors loaded from config (see
    /// [`OctoBuilder::from_config_file`](crate::OctoBuilder::from_config_file)).
    fn register_payloads(&self, registry: crate::PayloadRegistry) -> crate::PayloadRegistry {
        registry
    }
}

/// Runtime-provided context handed to a connector on startup.
///
/// Carries the shutdown signal and a handle to the runtime's bus (publish +
/// subscribe). Connectors should not hold references to a bus directly —
/// they get it through the context, which the runtime constructs and passes
/// in. This is the "шина сама появляется" property: a connector knows nothing
/// about a specific bus instance until it's plugged into a runtime.
pub struct ConnectorContext {
    pub shutdown: CancellationToken,
    bus: Arc<InProcessBus>,
}

impl ConnectorContext {
    /// Construct a context. Normally the runtime does this; tests and
    /// standalone uses can build one explicitly.
    pub fn new(shutdown: CancellationToken, bus: Arc<InProcessBus>) -> Self {
        Self { shutdown, bus }
    }

    /// Publish an envelope onto the runtime's bus.
    pub async fn publish(&self, envelope: Envelope) -> OctoResult<()> {
        self.bus.publish(envelope).await
    }

    /// Subscribe to the bus from inside the connector — used by bidirectional
    /// connectors to listen for outgoing actions from reflex/cognition.
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
        use crate::bus::EventBus;
        self.bus.publish_and_await_response(request, timeout).await
    }
}

/// What a connector declares about itself at registration time.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConnectorCapabilities {
    pub direction: Direction,
    pub delivery: DeliveryMode,
    /// Event kinds this connector may emit (input/bidir).
    pub event_kinds_emit: Vec<EventKind>,
    /// Event kinds this connector accepts as actions (bidir/output).
    pub event_kinds_accept: Vec<EventKind>,
    /// Continuous stream vs discrete events.
    pub streaming: bool,
    /// Whether the connector accepts batches.
    pub batchable: bool,
    /// Whether the connector supports explicit poll/query.
    pub query: bool,
    /// Replay support.
    pub replay: ReplayMode,
    /// Human/LLM-facing description of what this connector offers as an
    /// agent-callable tool (e.g. its command kinds and payload fields). When
    /// `Some`, the connector opts into the cogitator's env-as-tools catalog;
    /// `None` means "not an agent tool" (e.g. a user chat channel).
    pub description: Option<String>,
}

impl ConnectorCapabilities {
    pub fn input_only() -> Self {
        Self {
            direction: Direction::InputOnly,
            ..Default::default()
        }
    }

    pub fn bidirectional() -> Self {
        Self {
            direction: Direction::Bidirectional,
            ..Default::default()
        }
    }

    pub fn output_only() -> Self {
        Self {
            direction: Direction::OutputOnly,
            ..Default::default()
        }
    }

    pub fn with_delivery(mut self, delivery: DeliveryMode) -> Self {
        self.delivery = delivery;
        self
    }

    pub fn with_emit_kinds(mut self, kinds: impl IntoIterator<Item = EventKind>) -> Self {
        self.event_kinds_emit = kinds.into_iter().collect();
        self
    }

    pub fn with_accept_kinds(mut self, kinds: impl IntoIterator<Item = EventKind>) -> Self {
        self.event_kinds_accept = kinds.into_iter().collect();
        self
    }

    pub fn with_streaming(mut self, streaming: bool) -> Self {
        self.streaming = streaming;
        self
    }

    pub fn with_batchable(mut self, batchable: bool) -> Self {
        self.batchable = batchable;
        self
    }

    pub fn with_query(mut self, query: bool) -> Self {
        self.query = query;
        self
    }

    pub fn with_replay(mut self, replay: ReplayMode) -> Self {
        self.replay = replay;
        self
    }

    /// Advertise this connector to the agent's env-as-tools catalogue with a
    /// description of how to call it (command kinds, payload fields).
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }
}

/// Direction model for connectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum Direction {
    /// Only emits envelopes (camera, MQTT subscriber, RSS poller).
    #[default]
    InputOnly,
    /// Both emits and accepts (Telegram bot, HTTP RPC listener, chat).
    Bidirectional,
    /// Only accepts actions (notifier, push, MQTT publisher without subscription).
    OutputOnly,
}

/// Delivery semantics for envelopes produced by this connector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum DeliveryMode {
    AtMostOnce,
    #[default]
    AtLeastOnce,
    ExactlyOnce,
}

/// Replay support — how much history this connector can re-deliver.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum ReplayMode {
    #[default]
    None,
    LastN(usize),
    Persistent,
}
