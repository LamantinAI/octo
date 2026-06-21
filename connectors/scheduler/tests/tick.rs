//! Deterministic timing tests using tokio's paused clock — no wall-clock sleeps.

use std::sync::Arc;
use std::time::Duration;

use octo_connector_scheduler::{SchedulerConnector, Timer};
use octo_core::{Connector, ConnectorContext, EventKind, Filter, InProcessBus};
use tokio_util::sync::CancellationToken;

#[tokio::test(start_paused = true)]
async fn emits_ticks_on_interval_with_increasing_seq() {
    let bus = Arc::new(InProcessBus::new(64));
    let mut sub = bus.subscribe_sync(Filter::by_kind("timer.tick"));

    let token = CancellationToken::new();
    let ctx = ConnectorContext::new(token.child_token(), Arc::clone(&bus));
    let conn = SchedulerConnector::with_timers(
        "sched",
        vec![Timer::interval("pulse", Duration::from_millis(100))],
    );
    let handle = tokio::spawn(async move { conn.run(ctx).await });

    // Advance three intervals' worth of (paused) time.
    tokio::time::advance(Duration::from_millis(350)).await;

    for expected_seq in 1..=3u64 {
        let env = sub.next().await.expect("a tick");
        assert_eq!(env.kind.as_str(), "timer.tick");
        let payload = env
            .payload_as::<serde_json::Value>()
            .expect("json payload");
        assert_eq!(payload["name"], "pulse");
        assert_eq!(payload["seq"], expected_seq);
    }

    token.cancel();
    let _ = handle.await;
}

#[tokio::test(start_paused = true)]
async fn oneshot_fires_once_then_idles() {
    let bus = Arc::new(InProcessBus::new(64));
    let mut sub = bus.subscribe_sync(Filter::by_kind("timer.fire"));

    let token = CancellationToken::new();
    let ctx = ConnectorContext::new(token.child_token(), Arc::clone(&bus));
    let conn = SchedulerConnector::with_timers(
        "sched",
        vec![Timer::oneshot("wake", Duration::from_millis(200))
            .with_kind(EventKind::from_static("timer.fire"))],
    );
    let handle = tokio::spawn(async move { conn.run(ctx).await });

    tokio::time::advance(Duration::from_millis(500)).await;

    let env = sub.next().await.expect("the one-shot fire");
    assert_eq!(env.kind.as_str(), "timer.fire");
    assert_eq!(
        env.payload_as::<serde_json::Value>().unwrap()["seq"],
        1u64
    );
    assert_eq!(env.payload_as::<serde_json::Value>().unwrap()["name"], "wake");

    // No second emission: the connector idles. Shutting down ends it cleanly.
    token.cancel();
    let _ = handle.await;
}

#[tokio::test(start_paused = true)]
async fn max_ticks_caps_emissions() {
    let bus = Arc::new(InProcessBus::new(64));
    let mut sub = bus.subscribe_sync(Filter::by_kind("timer.tick"));

    let token = CancellationToken::new();
    let ctx = ConnectorContext::new(token.child_token(), Arc::clone(&bus));
    let conn = SchedulerConnector::with_timers(
        "sched",
        vec![Timer::interval("capped", Duration::from_millis(100)).with_max_ticks(2)],
    );
    let handle = tokio::spawn(async move { conn.run(ctx).await });

    tokio::time::advance(Duration::from_millis(500)).await;

    assert_eq!(sub.next().await.unwrap().payload_as::<serde_json::Value>().unwrap()["seq"], 1u64);
    assert_eq!(sub.next().await.unwrap().payload_as::<serde_json::Value>().unwrap()["seq"], 2u64);

    // No third emission (capped at 2): advancing further yields nothing.
    tokio::time::advance(Duration::from_millis(300)).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(50), sub.next())
            .await
            .is_err(),
        "no third emission after max_ticks"
    );

    token.cancel();
    let _ = handle.await;
}
