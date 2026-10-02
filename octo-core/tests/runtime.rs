use std::sync::Arc;

use async_trait::async_trait;
use octo_core::{
    Connector, ConnectorCapabilities, ConnectorContext, ConnectorId, Envelope, EventKind, Filter,
    Octo, OctoError, OctoResult, PayloadRegistry, SubscribeOptions,
};
mod common;
use common::{CountingCogitator, FlakyConnector, OneShot, SelfRestartConnector};

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

    tokio::time::timeout(std::time::Duration::from_secs(15), octo.run())
        .await
        .expect("runtime must stop")
        .unwrap();

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
        PayloadRegistry::new().register_codec::<Alert>(EventKind::from_static("alert.text")),
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
    let observed = tokio::spawn(async move {
        tokio::time::timeout(std::time::Duration::from_millis(50), sub.next()).await
    });

    tokio::time::timeout(std::time::Duration::from_secs(15), octo.run())
        .await
        .expect("runtime must stop")
        .unwrap();

    // Nothing reached subscriber (timeout or bus closed without messages).
    match observed.await.unwrap() {
        Ok(None) => {}
        Err(_) => {}
        Ok(Some(env)) => panic!(
            "rejected envelope should not reach subscribers; got: {:?}",
            env.kind
        ),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn supervisor_restarts_failed_connector() {
    let attempts = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let conn = Arc::new(FlakyConnector {
        id: ConnectorId::new("flaky"),
        capabilities: ConnectorCapabilities::input_only(),
        attempts: Arc::clone(&attempts),
    });

    let octo = Octo::builder().add_connector(conn).build();
    tokio::time::timeout(std::time::Duration::from_secs(5), octo.run())
        .await
        .expect("runtime should finish in time")
        .expect("run ok");

    assert!(
        attempts.load(std::sync::atomic::Ordering::Relaxed) >= 2,
        "connector should restart after failing once"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn control_signals_restart_connector_then_process() {
    let attempts = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let conn = Arc::new(SelfRestartConnector {
        id: ConnectorId::new("self_restart"),
        capabilities: ConnectorCapabilities::input_only(),
        attempts: Arc::clone(&attempts),
    });

    let octo = Octo::builder().bus_capacity(16).add_connector(conn).build();
    tokio::time::timeout(std::time::Duration::from_secs(5), octo.run())
        .await
        .expect("runtime should finish in time")
        .expect("run ok");

    // First run → restart_connector → second run → restart_process → stop.
    assert_eq!(
        attempts.load(std::sync::atomic::Ordering::Relaxed),
        2,
        "connector should run twice (restart_connector, then restart_process)"
    );
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

    tokio::time::timeout(std::time::Duration::from_secs(15), octo.run())
        .await
        .expect("runtime must stop")
        .unwrap();

    let env = received
        .await
        .unwrap()
        .expect("subscriber received envelope from connector");
    assert_eq!(env.payload_as::<i32>(), Some(&42));
    assert_eq!(env.kind.as_str(), "test.one");
    assert_eq!(env.source.as_str(), "oneshot");
}

// ─── Filter::by_correlation + publish_and_await_response ───────────────
