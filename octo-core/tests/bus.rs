use std::sync::Arc;

use octo_core::{
    ChannelMetadata, ConnectorId, Envelope, EventBus, EventId, EventKind, Filter, InProcessBus,
    OctoError, PayloadRegistry, SubscribeOptions, TrustLevel,
};
mod common;

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
    let with_target = env.clone().with_target(ConnectorId::new("alerter"));
    assert!(Filter::by_target(ConnectorId::new("alerter")).matches(&with_target));
    assert!(!Filter::by_target(ConnectorId::new("alerter")).matches(&env));
}

#[test]
fn filter_min_trust_gates_channel_envelopes_only() {
    let f = Filter::all().with_min_trust(TrustLevel::Medium);

    let high = Envelope::new(
        ConnectorId::new("tg"),
        EventKind::from_static("chat.message"),
        (),
    )
    .with_channel_metadata(ChannelMetadata::new().with_trust(TrustLevel::High));
    assert!(f.matches(&high), "trusted channel passes");

    let low = Envelope::new(
        ConnectorId::new("tg"),
        EventKind::from_static("chat.message"),
        (),
    )
    .with_channel_metadata(ChannelMetadata::new().with_trust(TrustLevel::Low));
    assert!(!f.matches(&low), "below-floor channel is dropped");

    let internal = Envelope::new(
        ConnectorId::new("petstore"),
        EventKind::from_static("pet.result"),
        (),
    );
    assert!(
        f.matches(&internal),
        "internal (no metadata) traffic passes"
    );
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

/// Registry integration: a matching payload type for the registered kind
/// publishes cleanly through the bus.
#[tokio::test(flavor = "current_thread")]
async fn registry_allows_matching_payload_through_bus() {
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct Alert {
        text: String,
    }

    let registry = std::sync::Arc::new(
        PayloadRegistry::new().register_codec::<Alert>(EventKind::from_static("alert.text")),
    );

    let bus = InProcessBus::new(8).with_registry(Arc::clone(&registry));
    let mut sub = bus
        .subscribe(Filter::all(), SubscribeOptions::default())
        .await
        .unwrap();

    bus.publish(Envelope::new(
        ConnectorId::new("src"),
        EventKind::from_static("alert.text"),
        Alert { text: "ok".into() },
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
        PayloadRegistry::new().register_codec::<Alert>(EventKind::from_static("alert.text")),
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
