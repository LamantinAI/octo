//! Envelope — the unit of data flowing through the runtime.
//!
//! Protocol-style: the header is fixed and strongly typed; the [`Payload`] is
//! opaque to the bus and is accessed by downcast at the handler.
//!
//! This module groups everything that defines an envelope:
//! - [`Envelope`] (this file) — the header struct.
//! - [`kind::EventKind`] — typed routing label.
//! - [`payload::Payload`] — opaque body wrapper.
//! - [`metadata`] — channel-level metadata, priority, trust, reply-to.
//! - [`trail`] — behaviour trail records.
//!
//! See `runtime_architecture` and `envelope_decision` vault drafts.

pub mod blob;
pub mod kind;
pub mod metadata;
pub mod payload;
pub mod registry;
pub mod stream;
pub mod trail;

pub use blob::Blob;
pub use kind::EventKind;
pub use metadata::{ChannelMetadata, Priority, ReplyChannel, TrustLevel};
pub use payload::Payload;
pub use registry::{PayloadRegistry, RegistryEntry, RegistryError};
pub use stream::StreamFrame;
pub use trail::{TrailAction, TrailActor, TrailEntry};

use std::any::Any;
use std::collections::HashMap;

use chrono::{DateTime, Utc};

use crate::{ChannelId, ConnectorId, EventId};

/// The unit of data flowing through the Octo runtime.
///
/// Construct via [`Envelope::new`] and decorate with `with_*` builders.
#[derive(Debug, Clone)]
pub struct Envelope {
    /// Unique event identifier (UUID v7 for temporal ordering).
    pub id: EventId,

    /// Connector that produced this envelope.
    pub source: ConnectorId,

    /// Connector this envelope is addressed to (output/bidir actions).
    /// `None` for input/observation envelopes.
    pub target: Option<ConnectorId>,

    /// Channel within the source connector ("присоска"), if applicable.
    pub channel: Option<ChannelId>,

    /// Typed event identifier (`vision.incident.fight`, `telegram.command`, ...).
    /// This is the routing label — analogous to a NATS subject or HTTP method.
    pub kind: EventKind,

    /// When the envelope was created.
    pub timestamp: DateTime<Utc>,

    /// Opaque body. Downcast to a known type at the handler.
    pub payload: Payload,

    /// For request/response correlation; reply envelopes carry the original id.
    pub correlation_id: Option<EventId>,

    /// For bidirectional connectors: where to deliver a reply (separate from
    /// `target` because reply addressing can be more specific than just a connector).
    pub reply_to: Option<ReplyChannel>,

    /// Dispatch hint.
    pub priority: Priority,

    /// Behavior trail — what each layer did before passing on.
    pub trail: Vec<TrailEntry>,

    /// Open extension: arbitrary string tags (classifier outputs, debugging hints).
    pub tags: HashMap<String, String>,

    /// Channel-level metadata propagated for conditional reflex predicates.
    pub channel_metadata: Option<ChannelMetadata>,

    /// Streaming marker, if this envelope is part of a multi-chunk message.
    /// Chunks of one stream share [`Envelope::correlation_id`]; this field
    /// tells the subscriber where in the sequence the chunk sits.
    /// See [`stream`] module docs for the full protocol.
    pub stream: Option<StreamFrame>,
}

impl Envelope {
    /// Construct a fresh envelope. `id` is generated; `timestamp` is now.
    /// The `value` is wrapped in a [`Payload`] internally.
    pub fn new<T>(source: ConnectorId, kind: EventKind, value: T) -> Self
    where
        T: Any + Send + Sync + 'static,
    {
        Self {
            id: EventId::new(),
            source,
            target: None,
            channel: None,
            kind,
            timestamp: Utc::now(),
            payload: Payload::new(value),
            correlation_id: None,
            reply_to: None,
            priority: Priority::default(),
            trail: Vec::new(),
            tags: HashMap::new(),
            channel_metadata: None,
            stream: None,
        }
    }

    /// Construct from an already-built [`Payload`] (e.g., re-emit / forwarding).
    pub fn from_parts(source: ConnectorId, kind: EventKind, payload: Payload) -> Self {
        Self {
            id: EventId::new(),
            source,
            target: None,
            channel: None,
            kind,
            timestamp: Utc::now(),
            payload,
            correlation_id: None,
            reply_to: None,
            priority: Priority::default(),
            trail: Vec::new(),
            tags: HashMap::new(),
            channel_metadata: None,
            stream: None,
        }
    }

    pub fn with_target(mut self, target: ConnectorId) -> Self {
        self.target = Some(target);
        self
    }

    pub fn with_channel(mut self, channel: ChannelId) -> Self {
        self.channel = Some(channel);
        self
    }

    pub fn with_correlation(mut self, id: EventId) -> Self {
        self.correlation_id = Some(id);
        self
    }

    pub fn with_reply_to(mut self, reply: ReplyChannel) -> Self {
        self.reply_to = Some(reply);
        self
    }

    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }

    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }

    pub fn with_channel_metadata(mut self, meta: ChannelMetadata) -> Self {
        self.channel_metadata = Some(meta);
        self
    }

    /// Mark this envelope as a chunk in a stream. Chunks of one stream
    /// share `correlation_id`; the [`StreamFrame`] tells subscribers where
    /// in the sequence this chunk sits. See [`stream`] module docs.
    pub fn with_stream_frame(mut self, frame: StreamFrame) -> Self {
        self.stream = Some(frame);
        self
    }

    /// `true` if this envelope is part of a stream (any frame).
    pub fn is_stream(&self) -> bool {
        self.stream.is_some()
    }

    /// Append a trail entry. Mutates in place.
    pub fn push_trail(&mut self, entry: TrailEntry) {
        self.trail.push(entry);
    }

    /// Append a trail entry, returning self (builder-style).
    pub fn with_trail(mut self, entry: TrailEntry) -> Self {
        self.trail.push(entry);
        self
    }

    /// Convenience: try to read the payload as `&T`.
    pub fn payload_as<T: Any + 'static>(&self) -> Option<&T> {
        self.payload.downcast_ref::<T>()
    }
}
