//! End-to-end demo: a Fluxion-style smart sensor emits typed `Incident`
//! envelopes (no target). The **router** routes high-severity incidents to
//! the Telegram-style sink based on a declarative rule. The cogitator is
//! left as the default `EmptyCogitator` — it just observes via stderr.
//!
//! Demonstrates:
//! - **Declarative routing via `Router`**: the routing decision lives in the
//!   route table (data), not in a closure inside the cogitator. Sensor stays
//!   dumb; cogitator stays empty; router handles "what goes where".
//! - **Heterogeneous payloads on one bus**: sensor publishes `Incident`,
//!   router preserves the payload (copy_payload), sink downcasts on
//!   arrival.
//! - **Sensor-side severity classification via tags**: sensor adds a
//!   `severity_high` tag for incidents above threshold. Router matches by
//!   tag. Payload-aware predicates in the router itself are future work.
//!
//! Run:
//!
//! ```text
//! cargo run --example incident_to_telegram
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use octo_core::{
    bus::KindPattern, Connector, ConnectorCapabilities, ConnectorContext, ConnectorId, Envelope,
    EventKind, Filter, Octo, OctoResult, Priority, Route, RouteAction, RoutePredicate,
    RouteStrategy, RuleBasedRouter, SubscribeOptions, TrailAction, TrailActor, TrailEntry,
};
use tokio::time::{interval, MissedTickBehavior};

/// Application-level payload — what the smart sensor emits.
#[derive(Debug, Clone)]
struct Incident {
    kind: String,
    severity: u8,
    location: String,
    evidence_uri: Option<String>,
}

const SCRIPT: &[(&str, u8, &str, Option<&str>)] = &[
    (
        "package_delivered",
        2,
        "front_door",
        Some("clip://2026-05-10/12:01:00.mp4"),
    ),
    (
        "unknown_face",
        5,
        "entrance",
        Some("clip://2026-05-10/12:01:30.mp4"),
    ),
    ("loitering", 6, "street", None),
    (
        "motion_after_hours",
        8,
        "back_yard",
        Some("clip://2026-05-10/12:02:15.mp4"),
    ),
];

// ────────────────────────────────────────────────────────────
// Smart sensor — emits Incident with severity tag, no target.
// ────────────────────────────────────────────────────────────

struct IncidentSensor {
    id: ConnectorId,
    capabilities: ConnectorCapabilities,
    period: Duration,
    warmup: Duration,
}

impl IncidentSensor {
    fn new(id: impl Into<String>, period: Duration, warmup: Duration) -> Arc<Self> {
        let id = ConnectorId::new(id);
        let capabilities = ConnectorCapabilities::input_only()
            .with_emit_kinds([EventKind::from_static("vision.incident.detected")])
            .with_streaming(true);
        Arc::new(Self {
            id,
            capabilities,
            period,
            warmup,
        })
    }
}

#[async_trait]
impl Connector for IncidentSensor {
    fn id(&self) -> &ConnectorId {
        &self.id
    }
    fn capabilities(&self) -> &ConnectorCapabilities {
        &self.capabilities
    }
    async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
        tokio::select! {
            _ = tokio::time::sleep(self.warmup) => {}
            _ = ctx.shutdown.cancelled() => {
                println!("[sensor {}] shutdown during warmup", self.id);
                return Ok(());
            }
        }

        let mut tick = interval(self.period);
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let kind = EventKind::from_static("vision.incident.detected");
        let mut idx: usize = 0;

        loop {
            tokio::select! {
                _ = tick.tick() => {
                    let (kind_str, severity, location, evidence) = SCRIPT[idx % SCRIPT.len()];
                    idx += 1;

                    let incident = Incident {
                        kind: kind_str.to_string(),
                        severity,
                        location: location.to_string(),
                        evidence_uri: evidence.map(String::from),
                    };

                    // Sensor adds a severity-class tag. Router matches on tags,
                    // not on payload fields (payload predicates are future work).
                    let mut envelope = Envelope::new(self.id.clone(), kind.clone(), incident)
                        .with_trail(TrailEntry::new(
                            TrailActor::Connector(self.id.clone()),
                            TrailAction::Emit { kind: kind.clone() },
                        ));
                    if severity > 4 {
                        envelope = envelope.with_tag("severity_class", "high");
                    } else {
                        envelope = envelope.with_tag("severity_class", "low");
                    }

                    ctx.publish(envelope).await?;
                    println!(
                        "[sensor {}] emit #{idx}: {kind_str} @ {location} (sev={severity}, class={})",
                        self.id,
                        if severity > 4 { "high" } else { "low" },
                    );
                }
                _ = ctx.shutdown.cancelled() => {
                    println!("[sensor {}] shutdown after {idx} emits", self.id);
                    return Ok(());
                }
            }
        }
    }
}

// ────────────────────────────────────────────────────────────
// Output sink — receives by target, downcasts Incident.
// ────────────────────────────────────────────────────────────

struct TelegramSink {
    id: ConnectorId,
    capabilities: ConnectorCapabilities,
    chat_label: String,
}

impl TelegramSink {
    fn new(id: impl Into<String>, chat_label: impl Into<String>) -> Arc<Self> {
        let id = ConnectorId::new(id);
        let capabilities = ConnectorCapabilities::output_only()
            .with_accept_kinds([EventKind::from_static("vision.incident.detected")]);
        Arc::new(Self {
            id,
            capabilities,
            chat_label: chat_label.into(),
        })
    }
}

#[async_trait]
impl Connector for TelegramSink {
    fn id(&self) -> &ConnectorId {
        &self.id
    }
    fn capabilities(&self) -> &ConnectorCapabilities {
        &self.capabilities
    }
    async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
        let mut sub = ctx
            .subscribe(
                Filter::by_target(self.id.clone()),
                SubscribeOptions::default(),
            )
            .await?;

        let mut delivered: usize = 0;
        loop {
            tokio::select! {
                next = sub.next() => match next {
                    Some(envelope) => {
                        let Some(inc) = envelope.payload_as::<Incident>() else {
                            eprintln!(
                                "[telegram {}] unexpected payload type: {}",
                                self.id, envelope.payload.type_name(),
                            );
                            continue;
                        };
                        delivered += 1;
                        let evidence = inc.evidence_uri.as_deref().unwrap_or("none");
                        let trail_summary = envelope
                            .trail
                            .iter()
                            .map(|t| match &t.actor {
                                TrailActor::Connector(c) => format!("connector:{c}"),
                                TrailActor::Reflex(r) => format!("route:{r}"),
                                TrailActor::Cognition { backend } => format!("cog:{backend}"),
                                TrailActor::External(s) => format!("ext:{s}"),
                            })
                            .collect::<Vec<_>>()
                            .join(" -> ");
                        println!(
                            "[telegram {}] -> chat \"{}\": [{}] {} at {} (sev {}/10) | evidence: {} | trail: {trail_summary}",
                            self.id, self.chat_label,
                            inc.kind, inc.kind, inc.location, inc.severity, evidence,
                        );
                    }
                    None => {
                        println!("[telegram {}] bus closed; delivered {delivered}", self.id);
                        return Ok(());
                    }
                },
                _ = ctx.shutdown.cancelled() => {
                    println!("[telegram {}] shutdown; delivered {delivered}", self.id);
                    return Ok(());
                }
            }
        }
    }
}

// ────────────────────────────────────────────────────────────
// Build router with declarative routes
// ────────────────────────────────────────────────────────────

fn build_router(telegram_id: ConnectorId) -> Arc<RuleBasedRouter> {
    let mut tags_high = HashMap::new();
    tags_high.insert("severity_class".to_string(), "high".to_string());

    RuleBasedRouter::builder("incident_router")
        .add_route(Route {
            id: "high_severity_to_telegram".into(),
            priority: Priority::High,
            strategy: RouteStrategy::Terminate,
            when: RoutePredicate {
                kind: Some(KindPattern::new("vision.incident.*")),
                tags_required: tags_high,
                ..Default::default()
            },
            then: RouteAction {
                target: telegram_id,
                override_kind: None,    // keep incident kind; sink knows how to read Incident
                add_tags: HashMap::new(),
                copy_payload: true,
                static_payload: None,
            },
            enabled: true,
        })
        // Low-severity incidents simply don't match — they stay on the bus
        // and nobody picks them up. Could add an Observe rule to log them
        // through the router, but for the demo we leave them silent.
        .build()
}

// ────────────────────────────────────────────────────────────
// Main
// ────────────────────────────────────────────────────────────

#[tokio::main(flavor = "current_thread")]
async fn main() -> OctoResult<()> {
    let telegram_id = ConnectorId::new("telegram");

    let octo = Octo::builder()
        .bus_capacity(64)
        .router(build_router(telegram_id.clone()))   // ← router decides routing
        .add_connector(IncidentSensor::new(
            "fluxion",
            Duration::from_millis(700),
            Duration::from_millis(100),
        ))
        .add_connector(TelegramSink::new("telegram", "@owner"))
        // cogitator left as default (EmptyCogitator) — just observes
        .build();

    let shutdown = octo.shutdown_token();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(3)).await;
        println!("\n[main] shutdown\n");
        shutdown.cancel();
    });

    octo.run().await?;
    println!("[main] done");
    Ok(())
}
