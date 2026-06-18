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
pub mod envelope;
pub mod error;
pub mod ids;
pub mod router;
pub mod runtime;

// Re-exports — keep the public surface flat for ergonomic `use octo_core::*`.
pub use bus::{EventBus, Filter, InProcessBus, KindPattern, Subscription};
pub use cogitator::{Cogitator, CogitatorContext, EmptyCogitator};
pub use config::{ConfigError, ConnectorFactory, FactoryContext};
pub use connector::{
    BackpressureStrategy, ChannelDescriptor, Connector, ConnectorCapabilities, ConnectorContext,
    DeliveryMode, Direction, IdlePolicy, Lifecycle, PanicPolicy, ReplayMode, RestartPolicy,
    SubscribeOptions,
};
pub use envelope::{
    ChannelMetadata, Envelope, EventKind, Payload, PayloadRegistry, Priority, RegistryEntry,
    RegistryError, ReplyChannel, StreamFrame, TrailAction, TrailActor, TrailEntry, TrustLevel,
};
pub use error::{OctoError, OctoResult};
pub use ids::{ChannelId, ConnectorId, EventId, RuleId};
pub use router::{
    Route, RouteAction, RouteId, RoutePredicate, RouteStrategy, Router, RouterContext,
    RuleBasedRouter,
};
pub use runtime::{Octo, OctoBuilder};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;

    use super::*;

    #[test]
    fn build_envelope_with_decorators() {
        let env = Envelope::new(
            ConnectorId::new("telegram"),
            EventKind::from_static("telegram.command"),
            "/help".to_string(),
        )
        .with_target(ConnectorId::new("ops_router"))
        .with_channel(ChannelId::new("owner"))
        .with_priority(Priority::High)
        .with_tag("test", "true")
        .with_channel_metadata(
            ChannelMetadata::new()
                .with_trust(TrustLevel::High)
                .with_priority(Priority::High),
        );

        assert_eq!(env.source.as_str(), "telegram");
        assert_eq!(env.target.as_ref().map(ConnectorId::as_str), Some("ops_router"));
        assert_eq!(env.kind.as_str(), "telegram.command");
        assert_eq!(env.payload_as::<String>(), Some(&"/help".to_string()));
        assert_eq!(env.priority, Priority::High);
        assert_eq!(env.tags.get("test").map(String::as_str), Some("true"));
        assert!(env.channel_metadata.is_some());
    }

    #[test]
    fn filter_matches_kinds_sources_targets() {
        let env = Envelope::new(
            ConnectorId::new("mqtt"),
            EventKind::from_static("mqtt.factory.temperature"),
            42i32,
        );

        assert!(Filter::all().matches(&env));
        assert!(Filter::by_kind("mqtt.**").matches(&env));
        assert!(Filter::by_kind("mqtt.factory.*").matches(&env));
        assert!(!Filter::by_kind("vision.**").matches(&env));

        let f = Filter::all().with_source(ConnectorId::new("mqtt"));
        assert!(f.matches(&env));

        let f = Filter::all().with_source(ConnectorId::new("telegram"));
        assert!(!f.matches(&env));

        // target-based filtering: matches only if target is set on envelope.
        let with_target = env
            .clone()
            .with_target(ConnectorId::new("alerter"));
        assert!(Filter::by_target(ConnectorId::new("alerter")).matches(&with_target));
        assert!(!Filter::by_target(ConnectorId::new("alerter")).matches(&env));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bus_publish_subscribe_roundtrip() {
        let bus = InProcessBus::new(16);
        let mut sub = bus
            .subscribe(Filter::all(), SubscribeOptions::default())
            .await
            .unwrap();

        let env = Envelope::new(
            ConnectorId::new("test"),
            EventKind::from_static("test.event"),
            "hello".to_string(),
        );
        bus.publish(env).await.unwrap();

        let received = sub.next().await.expect("envelope received");
        assert_eq!(received.kind.as_str(), "test.event");
        assert_eq!(received.payload_as::<String>(), Some(&"hello".to_string()));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bus_filter_excludes_other_kinds() {
        let bus = InProcessBus::new(16);
        let mut sub = bus
            .subscribe(Filter::by_kind("vision.**"), SubscribeOptions::default())
            .await
            .unwrap();

        bus.publish(Envelope::new(
            ConnectorId::new("mqtt"),
            EventKind::from_static("mqtt.temperature"),
            22i32,
        ))
        .await
        .unwrap();
        bus.publish(Envelope::new(
            ConnectorId::new("camera"),
            EventKind::from_static("vision.incident.fight"),
            1i32,
        ))
        .await
        .unwrap();

        let received = sub.next().await.expect("envelope received");
        assert_eq!(received.kind.as_str(), "vision.incident.fight");
        assert_eq!(received.payload_as::<i32>(), Some(&1));
    }

    #[test]
    fn lifecycle_transitions_and_predicates() {
        assert!(Lifecycle::Created.can_transition_to(Lifecycle::Registering));
        assert!(!Lifecycle::Created.can_transition_to(Lifecycle::Healthy));
        assert!(Lifecycle::Healthy.is_running());
        assert!(Lifecycle::Stopped.is_terminal());
    }

    #[test]
    fn trail_chain_decorates_envelope() {
        let mut env = Envelope::new(
            ConnectorId::new("camera"),
            EventKind::from_static("vision.incident.fight"),
            (),
        );
        env.push_trail(TrailEntry::new(
            TrailActor::Reflex(RuleId::new("intrusion_alert")),
            TrailAction::Tag {
                added: vec!["high_severity".into()],
            },
        ));
        env.push_trail(TrailEntry::new(
            TrailActor::Cognition {
                backend: "claude-haiku".into(),
            },
            TrailAction::Decision {
                summary: "alert_owner".into(),
            },
        ));

        assert_eq!(env.trail.len(), 2);
    }

    #[test]
    fn capabilities_builder() {
        let caps = ConnectorCapabilities::input_only()
            .with_delivery(DeliveryMode::AtLeastOnce)
            .with_emit_kinds([
                EventKind::from_static("vision.incident.fight"),
                EventKind::from_static("vision.entity.entered_zone"),
            ])
            .with_streaming(true)
            .with_replay(ReplayMode::LastN(100));

        assert_eq!(caps.direction, Direction::InputOnly);
        assert_eq!(caps.delivery, DeliveryMode::AtLeastOnce);
        assert_eq!(caps.event_kinds_emit.len(), 2);
        assert!(caps.streaming);
        assert_eq!(caps.replay, ReplayMode::LastN(100));
    }

    /// One-shot connector that publishes one envelope through `ctx`, then exits.
    /// Used to validate the runtime end-to-end.
    struct OneShot {
        id: ConnectorId,
        capabilities: ConnectorCapabilities,
        kind: EventKind,
        value: i32,
    }

    #[async_trait]
    impl Connector for OneShot {
        fn id(&self) -> &ConnectorId {
            &self.id
        }
        fn capabilities(&self) -> &ConnectorCapabilities {
            &self.capabilities
        }
        async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
            ctx.publish(Envelope::new(
                self.id.clone(),
                self.kind.clone(),
                self.value,
            ))
            .await
        }
    }

    /// Cogitator that counts every observed envelope into a shared atomic.
    /// Used to verify the cogitator is wired into the pipeline.
    struct CountingCogitator {
        id: String,
        count: Arc<std::sync::atomic::AtomicU64>,
    }

    #[async_trait]
    impl Cogitator for CountingCogitator {
        fn id(&self) -> &str {
            &self.id
        }
        async fn run(
            self: Arc<Self>,
            ctx: CogitatorContext,
            mut subscription: Subscription,
        ) -> OctoResult<()> {
            loop {
                tokio::select! {
                    next = subscription.next() => match next {
                        Some(_) => { self.count.fetch_add(1, std::sync::atomic::Ordering::Relaxed); }
                        None => return Ok(()),
                    },
                    _ = ctx.shutdown.cancelled() => return Ok(()),
                }
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cogitator_observes_every_envelope_in_pipeline() {
        let count = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let cogitator = Arc::new(CountingCogitator {
            id: "counter".into(),
            count: Arc::clone(&count),
        });

        let connector = Arc::new(OneShot {
            id: ConnectorId::new("oneshot"),
            capabilities: ConnectorCapabilities::input_only(),
            kind: EventKind::from_static("test.evt"),
            value: 1,
        });

        let octo = Octo::builder()
            .bus_capacity(16)
            .cogitator(cogitator)
            .add_connector(connector)
            .build();

        assert_eq!(octo.cogitator_id(), "counter");

        octo.run().await.unwrap();

        // Cogitator was pre-subscribed before connector spawn, so it MUST
        // have observed the single OneShot envelope.
        assert_eq!(
            count.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "cogitator must observe envelopes published by connectors"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn default_cogitator_is_empty_cogitator() {
        let octo = Octo::builder().build();
        assert_eq!(octo.cogitator_id(), "empty");
    }

    /// Registry integration: a matching payload type for the registered kind
    /// publishes cleanly through the bus.
    #[tokio::test(flavor = "current_thread")]
    async fn registry_allows_matching_payload_through_bus() {
        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        struct Alert {
            text: String,
        }

        let registry = std::sync::Arc::new(
            PayloadRegistry::new()
                .register_codec::<Alert>(EventKind::from_static("alert.text")),
        );

        let bus = InProcessBus::new(8).with_registry(Arc::clone(&registry));
        let mut sub = bus
            .subscribe(Filter::all(), SubscribeOptions::default())
            .await
            .unwrap();

        bus.publish(Envelope::new(
            ConnectorId::new("src"),
            EventKind::from_static("alert.text"),
            Alert {
                text: "ok".into(),
            },
        ))
        .await
        .expect("matching payload publishes");

        let received = sub.next().await.expect("envelope received");
        assert_eq!(received.kind.as_str(), "alert.text");
        assert!(received.payload_as::<Alert>().is_some());
    }

    /// Registry integration: a mismatched payload type for the registered
    /// kind is rejected by the bus at publish-time; the envelope never
    /// reaches subscribers.
    #[tokio::test(flavor = "current_thread")]
    async fn registry_rejects_mismatched_payload() {
        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        struct Alert {
            text: String,
        }

        let registry = std::sync::Arc::new(
            PayloadRegistry::new()
                .register_codec::<Alert>(EventKind::from_static("alert.text")),
        );

        let bus = InProcessBus::new(8).with_registry(Arc::clone(&registry));

        // Publishing a String (not Alert) under kind 'alert.text' must fail.
        let result = bus
            .publish(Envelope::new(
                ConnectorId::new("src"),
                EventKind::from_static("alert.text"),
                "not_an_alert".to_string(),
            ))
            .await;

        assert!(
            matches!(result, Err(OctoError::PayloadValidation(_))),
            "expected PayloadValidation error, got: {result:?}"
        );
    }

    /// Backward compatibility: without a registry the bus accepts any payload type.
    #[tokio::test(flavor = "current_thread")]
    async fn no_registry_accepts_any_payload() {
        let bus = InProcessBus::new(8);
        // No registry attached.
        bus.publish(Envelope::new(
            ConnectorId::new("src"),
            EventKind::from_static("anything.goes"),
            12345i32,
        ))
        .await
        .expect("without a registry, any payload is allowed");
    }

    /// Octo end-to-end: registry attached via builder, a connector emits a
    /// mismatched payload — bus rejects, connector's `publish` returns error.
    /// External subscriber sees nothing.
    #[tokio::test(flavor = "current_thread")]
    async fn octo_builder_with_registry_blocks_bad_publish() {
        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        struct Alert {
            text: String,
        }

        struct BadConnector {
            id: ConnectorId,
            capabilities: ConnectorCapabilities,
        }

        #[async_trait]
        impl Connector for BadConnector {
            fn id(&self) -> &ConnectorId {
                &self.id
            }
            fn capabilities(&self) -> &ConnectorCapabilities {
                &self.capabilities
            }
            async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
                // Wrong type for kind "alert.text" — registry should reject.
                let result = ctx
                    .publish(Envelope::new(
                        self.id.clone(),
                        EventKind::from_static("alert.text"),
                        99i32, // expected Alert, got i32
                    ))
                    .await;
                assert!(matches!(result, Err(OctoError::PayloadValidation(_))));
                Ok(())
            }
        }

        let registry = std::sync::Arc::new(
            PayloadRegistry::new()
                .register_codec::<Alert>(EventKind::from_static("alert.text")),
        );

        let octo = Octo::builder()
            .bus_capacity(8)
            .payload_registry(registry)
            .add_connector(Arc::new(BadConnector {
                id: ConnectorId::new("bad"),
                capabilities: ConnectorCapabilities::input_only(),
            }))
            .build();

        // External subscriber that should NOT receive the bad envelope.
        let mut sub = octo
            .subscribe(Filter::all(), SubscribeOptions::default())
            .await
            .unwrap();
        let observed =
            tokio::spawn(
                async move { tokio::time::timeout(std::time::Duration::from_millis(50), sub.next()).await },
            );

        octo.run().await.unwrap();

        // Nothing reached subscriber (timeout or bus closed without messages).
        match observed.await.unwrap() {
            Ok(None) => {}
            Err(_) => {}
            Ok(Some(env)) => panic!("rejected envelope should not reach subscribers; got: {:?}", env.kind),
        }
    }

    /// End-to-end: router plugged into Octo. Connector publishes a raw event
    /// with no `target`. The router has one matching rule that emits an
    /// action envelope with `target=alerter` and `override_kind=alert.text`.
    /// An external subscriber filtered by target receives the routed envelope.
    #[tokio::test(flavor = "current_thread")]
    async fn router_routes_envelope_via_terminate_rule() {
        use std::collections::HashMap;

        use crate::bus::KindPattern;
        use crate::router::{Route, RouteAction, RoutePredicate, RouteStrategy, RuleBasedRouter};

        #[derive(Debug, Clone)]
        struct Tick(u64);

        struct OneShotEmitter {
            id: ConnectorId,
            capabilities: ConnectorCapabilities,
        }

        #[async_trait]
        impl Connector for OneShotEmitter {
            fn id(&self) -> &ConnectorId {
                &self.id
            }
            fn capabilities(&self) -> &ConnectorCapabilities {
                &self.capabilities
            }
            async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
                // Brief warmup so router subscription is registered first.
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                ctx.publish(Envelope::new(
                    self.id.clone(),
                    EventKind::from_static("vision.incident.detected"),
                    Tick(42),
                ))
                .await
            }
        }

        let alert_target = ConnectorId::new("alerter");

        let router = RuleBasedRouter::builder("test_router")
            .add_route(Route {
                id: "incident_to_alerter".into(),
                priority: Priority::Normal,
                strategy: RouteStrategy::Terminate,
                when: RoutePredicate {
                    kind: Some(KindPattern::new("vision.incident.*")),
                    ..Default::default()
                },
                then: RouteAction {
                    target: alert_target.clone(),
                    override_kind: Some(EventKind::from_static("alert.text")),
                    add_tags: HashMap::new(),
                    copy_payload: true,
                    static_payload: None,
                },
                enabled: true,
            })
            .build();

        let connector = Arc::new(OneShotEmitter {
            id: ConnectorId::new("sensor"),
            capabilities: ConnectorCapabilities::input_only(),
        });

        let octo = Octo::builder()
            .bus_capacity(32)
            .router(router)
            .add_connector(connector)
            .build();

        assert_eq!(octo.router_id(), Some("test_router"));

        let mut sub = octo
            .subscribe(
                Filter::by_target(alert_target.clone()),
                SubscribeOptions::default(),
            )
            .await
            .unwrap();
        let received = tokio::spawn(async move { sub.next().await });

        octo.run().await.unwrap();

        let env = received
            .await
            .unwrap()
            .expect("routed envelope should reach target subscriber");
        assert_eq!(env.kind.as_str(), "alert.text");
        assert_eq!(env.target.as_ref(), Some(&alert_target));
        assert_eq!(env.payload_as::<Tick>().map(|t| t.0), Some(42));
        // Trail records the route's action.
        assert!(env
            .trail
            .iter()
            .any(|t| matches!(&t.actor, TrailActor::Reflex(rid) if rid.as_str() == "incident_to_alerter")));
    }

    /// Without a router, Octo runs as before — the bus does not invent routing.
    #[tokio::test(flavor = "current_thread")]
    async fn octo_without_router_works_unchanged() {
        let octo = Octo::builder().build();
        assert!(octo.router_id().is_none());
        octo.run().await.unwrap();
    }

    /// Streaming protocol smoke-test: a connector emits 3 chunks of one stream
    /// (Open, Chunk, Close), an external subscriber collects them by
    /// `correlation_id` and verifies the assembled text.
    #[tokio::test(flavor = "current_thread")]
    async fn stream_chunks_collect_by_correlation_id() {
        struct ChunkySource {
            id: ConnectorId,
            capabilities: ConnectorCapabilities,
        }

        #[async_trait]
        impl Connector for ChunkySource {
            fn id(&self) -> &ConnectorId {
                &self.id
            }
            fn capabilities(&self) -> &ConnectorCapabilities {
                &self.capabilities
            }
            async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
                let stream_id = EventId::new();
                let kind = EventKind::from_static("text.stream");
                let frames = [
                    (StreamFrame::Open, "hello "),
                    (StreamFrame::Chunk, "from "),
                    (StreamFrame::Close, "stream"),
                ];
                for (frame, text) in frames {
                    ctx.publish(
                        Envelope::new(self.id.clone(), kind.clone(), text.to_string())
                            .with_correlation(stream_id)
                            .with_stream_frame(frame),
                    )
                    .await?;
                }
                Ok(())
            }
        }

        let octo = Octo::builder()
            .bus_capacity(16)
            .add_connector(Arc::new(ChunkySource {
                id: ConnectorId::new("chunky"),
                capabilities: ConnectorCapabilities::input_only(),
            }))
            .build();

        let mut sub = octo
            .subscribe(Filter::all(), SubscribeOptions::default())
            .await
            .unwrap();

        let collector = tokio::spawn(async move {
            let mut chunks: Vec<Arc<Envelope>> = Vec::new();
            while let Some(env) = sub.next().await {
                if env.is_stream() {
                    chunks.push(env.clone());
                    if env.stream == Some(StreamFrame::Close) {
                        break;
                    }
                }
            }
            chunks
        });

        octo.run().await.unwrap();
        let chunks = collector.await.unwrap();

        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].stream, Some(StreamFrame::Open));
        assert_eq!(chunks[1].stream, Some(StreamFrame::Chunk));
        assert_eq!(chunks[2].stream, Some(StreamFrame::Close));

        // All chunks share one correlation_id.
        let cid = chunks[0].correlation_id.expect("Open carries correlation_id");
        assert!(chunks.iter().all(|c| c.correlation_id == Some(cid)));

        // Reassembled text.
        let assembled: String = chunks
            .iter()
            .map(|c| c.payload_as::<String>().cloned().unwrap_or_default())
            .collect();
        assert_eq!(assembled, "hello from stream");
    }


    #[tokio::test(flavor = "current_thread")]
    async fn octo_builder_runs_connector_and_external_subscriber_receives() {
        let connector = Arc::new(OneShot {
            id: ConnectorId::new("oneshot"),
            capabilities: ConnectorCapabilities::input_only()
                .with_emit_kinds([EventKind::from_static("test.one")]),
            kind: EventKind::from_static("test.one"),
            value: 42,
        });

        let octo = Octo::builder()
            .bus_capacity(16)
            .add_connector(connector)
            .build();

        assert_eq!(octo.connector_count(), 1);

        let mut sub = octo
            .subscribe(Filter::all(), SubscribeOptions::default())
            .await
            .unwrap();

        // Subscribe must happen before run() consumes self; spawn a reader.
        let received = tokio::spawn(async move { sub.next().await });

        octo.run().await.unwrap();

        let env = received
            .await
            .unwrap()
            .expect("subscriber received envelope from connector");
        assert_eq!(env.payload_as::<i32>(), Some(&42));
        assert_eq!(env.kind.as_str(), "test.one");
        assert_eq!(env.source.as_str(), "oneshot");
    }

    // ─── Filter::by_correlation + publish_and_await_response ───────────────

    #[test]
    fn filter_by_correlation_matches_only_marked_envelopes() {
        let wanted = EventId::new();
        let other = EventId::new();
        let f = Filter::by_correlation(wanted);

        let env_match = Envelope::new(
            ConnectorId::new("src"),
            EventKind::from_static("test.event.x"),
            (),
        )
        .with_correlation(wanted);
        let env_other = Envelope::new(
            ConnectorId::new("src"),
            EventKind::from_static("test.event.x"),
            (),
        )
        .with_correlation(other);
        let env_none = Envelope::new(
            ConnectorId::new("src"),
            EventKind::from_static("test.event.x"),
            (),
        );

        assert!(f.matches(&env_match));
        assert!(!f.matches(&env_other));
        assert!(!f.matches(&env_none));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn publish_and_await_response_returns_correlated_reply() {
        let bus: Arc<InProcessBus> = Arc::new(InProcessBus::new(16));

        // Pre-subscribed responder; emits a correlated reply for every command.
        let mut responder_sub = bus.subscribe_sync(Filter::by_kind("test.cmd.go"));
        let bus_for_responder = bus.clone();
        tokio::spawn(async move {
            if let Some(cmd) = responder_sub.next().await {
                let reply = Envelope::new(
                    ConnectorId::new("responder"),
                    EventKind::from_static("test.event.done"),
                    "ok".to_string(),
                )
                .with_correlation(cmd.id);
                bus_for_responder.publish(reply).await.unwrap();
            }
        });

        let request = Envelope::new(
            ConnectorId::new("agent"),
            EventKind::from_static("test.cmd.go"),
            "go".to_string(),
        );
        let request_id = request.id;

        let response = bus
            .publish_and_await_response(request, std::time::Duration::from_secs(1))
            .await
            .expect("response within timeout");

        assert_eq!(response.correlation_id, Some(request_id));
        assert_eq!(response.kind.as_str(), "test.event.done");
        assert_eq!(response.payload_as::<String>(), Some(&"ok".to_string()));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn publish_and_await_response_times_out_when_no_responder() {
        let bus: Arc<InProcessBus> = Arc::new(InProcessBus::new(16));

        let request = Envelope::new(
            ConnectorId::new("agent"),
            EventKind::from_static("test.cmd.lonely"),
            (),
        );
        let request_id = request.id;

        let err = bus
            .publish_and_await_response(request, std::time::Duration::from_millis(50))
            .await
            .expect_err("expected timeout");

        match err {
            OctoError::Timeout { correlation_id } => assert_eq!(correlation_id, request_id),
            other => panic!("expected Timeout, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn publish_and_await_response_skips_envelopes_with_other_correlation_ids() {
        let bus: Arc<InProcessBus> = Arc::new(InProcessBus::new(16));

        // Responder emits a *noise* envelope with an unrelated correlation_id first,
        // then the correctly correlated reply. Helper must skip the noise.
        let mut responder_sub = bus.subscribe_sync(Filter::by_kind("test.cmd.go"));
        let bus_for_responder = bus.clone();
        tokio::spawn(async move {
            if let Some(cmd) = responder_sub.next().await {
                let noise = Envelope::new(
                    ConnectorId::new("noise"),
                    EventKind::from_static("test.event.noise"),
                    "noise".to_string(),
                )
                .with_correlation(EventId::new()); // unrelated
                bus_for_responder.publish(noise).await.unwrap();

                let reply = Envelope::new(
                    ConnectorId::new("responder"),
                    EventKind::from_static("test.event.done"),
                    "real".to_string(),
                )
                .with_correlation(cmd.id);
                bus_for_responder.publish(reply).await.unwrap();
            }
        });

        let request = Envelope::new(
            ConnectorId::new("agent"),
            EventKind::from_static("test.cmd.go"),
            (),
        );
        let request_id = request.id;

        let response = bus
            .publish_and_await_response(request, std::time::Duration::from_secs(1))
            .await
            .expect("real reply within timeout");

        assert_eq!(response.correlation_id, Some(request_id));
        assert_eq!(response.kind.as_str(), "test.event.done");
        assert_eq!(response.payload_as::<String>(), Some(&"real".to_string()));
    }
}
