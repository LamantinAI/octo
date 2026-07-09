//! Minimal end-to-end check of the runtime (the "broth").
//!
//! `TickConnector` knows nothing about a specific bus. It's plugged into an
//! [`Octo`] runtime via the builder; the runtime constructs a
//! [`ConnectorContext`] at startup that carries the publish handle.
//!
//! Two subscribers consume from the same runtime bus:
//! - `sub_all`  — `Filter::all()`, prints every envelope.
//! - `sub_kind` — `Filter::by_kind("tick")`, prints only matching kinds.
//!
//! `Tick` is just an application-defined payload type — bus is opaque to it.
//! Subscribers downcast `env.payload` to `Tick` to read the seq.
//!
//! Run:
//!
//! ```text
//! cargo run --example tick_bus
//! ```

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use octo_core::{
    Connector, ConnectorCapabilities, ConnectorContext, ConnectorId, Envelope, EventKind,
    Filter, Octo, OctoResult, SubscribeOptions, TrailAction, TrailActor, TrailEntry,
};
use tokio::time::{interval, MissedTickBehavior};

/// Application-level payload. Octo doesn't know about it; subscribers downcast.
#[derive(Debug, Clone)]
struct Tick {
    seq: u64,
}

struct TickConnector {
    id: ConnectorId,
    capabilities: ConnectorCapabilities,
    period: Duration,
}

impl TickConnector {
    fn new(id: impl Into<String>, period: Duration) -> Arc<Self> {
        let id = ConnectorId::new(id);
        let capabilities = ConnectorCapabilities::input_only()
            .with_emit_kinds([EventKind::from_static("tick")])
            .with_streaming(true);
        Arc::new(Self {
            id,
            capabilities,
            period,
        })
    }
}

#[async_trait]
impl Connector for TickConnector {
    fn id(&self) -> &ConnectorId {
        &self.id
    }

    fn capabilities(&self) -> &ConnectorCapabilities {
        &self.capabilities
    }

    async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
        let mut tick = interval(self.period);
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let kind = EventKind::from_static("tick");
        let mut seq: u64 = 0;

        loop {
            tokio::select! {
                _ = tick.tick() => {
                    let envelope = Envelope::new(self.id.clone(), kind.clone(), Tick { seq })
                        .with_trail(TrailEntry::new(
                            TrailActor::Connector(self.id.clone()),
                            TrailAction::Emit { kind: kind.clone() },
                        ));
                    ctx.publish(envelope).await?;
                    println!("[connector {}] emit seq={}", self.id, seq);
                    seq += 1;
                }
                _ = ctx.shutdown.cancelled() => {
                    println!("[connector {}] shutdown after {} ticks", self.id, seq);
                    return Ok(());
                }
            }
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> OctoResult<()> {
    // Assemble the runtime — no manual bus wiring, no payload generic.
    let octo = Octo::builder()
        .bus_capacity(64)
        .add_connector(TickConnector::new("ticker", Duration::from_millis(500)))
        .build();

    // Hook subscribers + grab the shutdown token *before* running.
    let mut sub_all = octo
        .subscribe(Filter::all(), SubscribeOptions::default())
        .await?;
    let mut sub_kind = octo
        .subscribe(Filter::by_kind("tick"), SubscribeOptions::default())
        .await?;
    let shutdown = octo.shutdown_token();

    let h_all = tokio::spawn(async move {
        while let Some(env) = sub_all.next().await {
            // Downcast payload to the application type.
            let tick = env.payload_as::<Tick>().expect("tick payload");
            println!(
                "  [sub:all]  seq={} ts={} trail_len={}",
                tick.seq,
                env.timestamp.format("%H:%M:%S%.3f"),
                env.trail.len()
            );
        }
        println!("  [sub:all]  closed");
    });
    let h_kind = tokio::spawn(async move {
        while let Some(env) = sub_kind.next().await {
            let tick = env.payload_as::<Tick>().expect("tick payload");
            println!(
                "  [sub:kind] seq={} src={} kind={}",
                tick.seq, env.source, env.kind
            );
        }
        println!("  [sub:kind] closed");
    });

    // Trigger shutdown after ~3 seconds, in parallel with the runtime.
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(3)).await;
        println!("\n[main] sending shutdown signal\n");
        shutdown.cancel();
    });

    // Run all registered connectors until they return.
    octo.run().await?;

    let _ = h_all.await;
    let _ = h_kind.await;

    println!("[main] done");
    Ok(())
}
