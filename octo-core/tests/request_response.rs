use std::sync::Arc;

use octo_core::{
    ConnectorId, Envelope, EventBus, EventId, EventKind, Filter, InProcessBus, OctoError,
    SubscribeOptions,
};
mod common;

#[tokio::test(flavor = "current_thread")]
async fn publish_and_await_response_returns_correlated_reply() {
    let bus: Arc<InProcessBus> = Arc::new(InProcessBus::new(16));

    // Pre-subscribed responder; emits a correlated reply for every command.
    let mut responder_sub =
        bus.subscribe_sync(Filter::by_kind("test.cmd.go"), SubscribeOptions::default());
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
    let mut responder_sub =
        bus.subscribe_sync(Filter::by_kind("test.cmd.go"), SubscribeOptions::default());
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

// ─── Bus backpressure ──────────────────────────────────────────────────
