use octo_core::{
    BackpressureStrategy, ChannelId, ConnectorId, Envelope, EventBus, EventKind, Filter,
    InProcessBus, SubscribeOptions,
};
mod common;

/// A subscriber that falls behind on the raw broadcast path drops the
/// oldest — but the drop is **surfaced** (counted), not silent.
#[tokio::test(flavor = "current_thread")]
async fn bus_lag_is_surfaced_not_silent() {
    let bus = InProcessBus::new(16);
    let mut sub = bus.subscribe_sync(Filter::all(), SubscribeOptions::default());

    // Publish far past capacity without draining → the receiver lags.
    for i in 0..100u32 {
        bus.publish(Envelope::new(
            ConnectorId::new("src"),
            EventKind::from_static("x"),
            i,
        ))
        .await
        .unwrap();
    }

    let mut received = 0;
    while let Ok(Some(_)) =
        tokio::time::timeout(std::time::Duration::from_millis(50), sub.next()).await
    {
        received += 1;
        if received >= 16 {
            break;
        }
    }
    assert!(
        sub.lagged_total() > 0,
        "lag must be surfaced, not silently dropped"
    );
    assert!(received <= 16, "only up to capacity survives the overflow");
}

/// A per-subscriber deep buffer (buffer > bus capacity → shim) absorbs a
/// burst with no drops.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bus_shim_deep_buffer_no_drops() {
    let bus = InProcessBus::new(1024);
    // buffer > capacity → shim path with a deep per-subscriber queue.
    let opts = SubscribeOptions::default().with_buffer(4096);
    let mut sub = bus.subscribe_sync(Filter::all(), opts);

    // Burst stays under the broadcast capacity, so nothing is lost upstream.
    for i in 0..1000u32 {
        bus.publish(Envelope::new(
            ConnectorId::new("src"),
            EventKind::from_static("x"),
            i,
        ))
        .await
        .unwrap();
    }

    let mut received = 0;
    while let Ok(Some(_)) =
        tokio::time::timeout(std::time::Duration::from_millis(200), sub.next()).await
    {
        received += 1;
        if received >= 1000 {
            break;
        }
    }
    assert_eq!(received, 1000, "deep buffer absorbs the whole burst");
    assert_eq!(sub.lagged_total(), 0, "no drops with a deep buffer");
}

/// A `Steer` subscriber collapses a superseding sequence on the same channel
/// to just the latest — the steering primitive.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bus_steer_supersedes_pending() {
    let bus = InProcessBus::new(64);
    let opts = SubscribeOptions::default()
        .with_backpressure(BackpressureStrategy::Steer)
        .with_buffer(16);
    let mut sub = bus.subscribe_sync(Filter::all(), opts);

    for i in 1..=5u32 {
        bus.publish(
            Envelope::new(
                ConnectorId::new("src"),
                EventKind::from_static("chat.message"),
                i,
            )
            .with_channel(ChannelId::new("c")),
        )
        .await
        .unwrap();
    }
    // Let the forwarder process all five (collapsing to the latest).
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let env = tokio::time::timeout(std::time::Duration::from_millis(200), sub.next())
        .await
        .expect("an envelope is queued")
        .expect("not closed");
    assert_eq!(
        env.payload_as::<u32>(),
        Some(&5),
        "Steer keeps only the latest message per channel"
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), sub.next())
            .await
            .is_err(),
        "the four superseded messages do not accumulate"
    );
}
