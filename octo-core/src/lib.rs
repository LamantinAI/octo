//! # octo-core
//!
//! Core primitives for the **Octo** runtime — an event-driven multisensor
//! actor runtime for embodied always-on agents.
//!
//! This crate provides the protocol/transport layer: connectors, channels,
//! envelopes, the bus, lifecycle FSM, and a runtime builder. Behavioral
//! actors that consume these (reflex, cognition) live in sibling crates.
//!
//! ## Envelope shape (HTTP/NATS-style)
//!
//! [`Envelope`] is a fixed-shape header (id, source, target, kind, timestamp,
//! trail, ...) carrying an opaque [`Payload`]. The bus routes by header
//! fields; handlers downcast the payload to a known type. See
//! `research/drafts/envelope_decision.md` in the parent vault for the rationale.

// Top-level groups (logical clusters)
pub mod bus;
pub mod cogitator;
pub mod config;
pub mod connector;
pub mod control;
pub mod envelope;
pub mod error;
pub mod ids;
pub mod router;
pub mod runtime;

// Re-exports — keep the public surface flat for ergonomic `use octo_core::*`.
pub use bus::{EventBus, Filter, InProcessBus, KindPattern, Subscription};
pub use cogitator::{Cogitator, CogitatorContext, ConnectorInfo, EmptyCogitator};
pub use config::{ConfigError, ConnectorFactory, FactoryContext};
pub use connector::{
    BackpressureStrategy, ChannelDescriptor, Connector, ConnectorCapabilities, ConnectorContext,
    DeliveryMode, Direction, IdlePolicy, Lifecycle, PanicPolicy, ReplayMode, RestartPolicy,
    SubscribeOptions,
};
pub use envelope::{
    Blob, ChannelMetadata, Envelope, EventKind, InboundMessage, Payload, PayloadRegistry, Priority,
    RegistryEntry, RegistryError, ReplyChannel, StreamFrame, TrailAction, TrailActor, TrailEntry,
    TrustLevel,
};
pub use error::{OctoError, OctoResult};
pub use ids::{ChannelId, ConnectorId, EventId, RuleId};
pub use router::{
    NumOp, PayloadPredicate, Route, RouteAction, RouteId, RoutePredicate, RouteStrategy, Router,
    RouterContext, RuleBasedRouter,
};
pub use runtime::{Octo, OctoBuilder};
