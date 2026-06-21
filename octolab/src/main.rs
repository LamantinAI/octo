//! octolab — runs the Octo runtime with a real LLM ReAct cogitator that can
//! dispatch to available connectors (env-as-tools).
//!
//! User channel: Telegram if `OCTO_TELEGRAM_TOKEN` is set, else console. Tool
//! connector: petstore (dyn HTTP, `serde_json::Value` payloads). The agent sees
//! petstore in its catalog and routes envelopes to it when the user's request
//! needs it.

mod cogitator;
mod config;
mod console;
mod error;
mod history;
mod llm;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use error::Result;
use octo_connector_http::HttpConnector;
use octo_connector_mqtt::{MqttConnector, MqttSub, PayloadFormat};
use octo_connector_scheduler::{SchedulerConnector, Timer};
use octo_connector_telegram::TelegramConnector;
use octo_core::{
    EventKind, NumOp, Octo, PayloadPredicate, PayloadRegistry, Priority, Route, RouteAction,
    RoutePredicate, RouteStrategy, RuleBasedRouter,
};
use serde_json::Value;

/// Absolute path to the dyn petstore manifest (cwd-independent).
const PETSTORE_MANIFEST: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/../config/connectors/petstore/petstore.toml");

/// Repo-root `.env`, anchored on the manifest so cwd doesn't matter.
const DOTENV_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../.env");

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::from_path(DOTENV_PATH);
    let _ = dotenvy::dotenv();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| {
                    "octolab=info,octo_rig=info,octo_connector_telegram=info,octo_core=warn".into()
                }),
        )
        .init();

    let settings = config::Settings::load()?;
    eprintln!(
        "[octolab] model={} base_url={}",
        settings.model,
        if settings.base_url.is_empty() { "(provider default)" } else { &settings.base_url }
    );

    // ── Tool connector: petstore (dyn HTTP, Value payloads) ──────────────────
    // It self-advertises via its capabilities' description; the cogitator
    // discovers it through the runtime's introspection (ctx.connectors()).
    let petstore = HttpConnector::from_file(PETSTORE_MANIFEST)?;
    let mut registry = petstore.register_payloads(PayloadRegistry::new());
    // Register the event kinds the sensor/timer tentacles and the reflex router
    // produce, all as `serde_json::Value` (so copy-payload routing stays typed).
    for kind in [
        "timer.tick",
        "timer.fire",
        "sensor.anomaly",
        "mqtt.factory.temperature",
    ] {
        registry = registry.register_type::<Value>(EventKind::from_static(kind));
    }
    eprintln!("[octolab] tool connector: {} (live API may be flaky)", petstore.spec().id);

    // ── Per-channel history backend (pluggable: memory / file / …) ───────────
    const HISTORY_MAX: usize = 20;
    let history: Arc<dyn history::HistoryStore> = match settings.history.as_deref() {
        Some(spec) if spec.starts_with("file:") => {
            let dir = &spec["file:".len()..];
            eprintln!("[octolab] history: file ({dir})");
            Arc::new(history::FileHistory::new(dir, HISTORY_MAX)?)
        }
        _ => {
            eprintln!("[octolab] history: in-memory");
            Arc::new(history::InMemoryHistory::new(HISTORY_MAX))
        }
    };
    eprintln!(
        "[octolab] perception: {}",
        settings.perception.as_deref().unwrap_or("addressed")
    );

    // ── Time tentacle: the scheduler lets the agent live in time ─────────────
    // A fast `timer.tick` (ambient, observe-only) and a sparse `timer.fire`
    // (drives proactive cognition when listed in OCTO_ACTIONABLE).
    let wake_secs = settings.wake_secs.unwrap_or(60);
    let scheduler = SchedulerConnector::with_timers(
        "scheduler",
        vec![
            Timer::interval("pulse", Duration::from_secs(10)),
            Timer::interval("wake", Duration::from_secs(wake_secs))
                .with_kind(EventKind::from_static("timer.fire")),
        ],
    );
    eprintln!("[octolab] scheduler: timer.tick every 10s, timer.fire every {wake_secs}s");

    let mut builder = Octo::builder()
        .payload_registry(Arc::new(registry))
        .router(reflex_router())
        .cogitator(cogitator::ReactCogitator::new("react", settings.clone(), history))
        .add_connector(petstore)
        .add_connector(scheduler);

    // ── Sensor tentacle: MQTT, only when a broker is configured ──────────────
    if let Some(host) = settings.mqtt_host.clone() {
        eprintln!("[octolab] mqtt: {host} (subscribing factory/#)");
        let sub = MqttSub::new("factory/#").with_payload(PayloadFormat::Json);
        builder = builder.add_connector(MqttConnector::new("factory", host, 1883, vec![sub]));
    }

    // ── User channel: official Telegram connector, or console fallback ───────
    if let Some(token) = settings.telegram_token.clone() {
        eprintln!("[octolab] channel: telegram");
        builder = builder.add_connector(TelegramConnector::new("telegram", token));
    } else {
        eprintln!("[octolab] channel: console (set OCTO_TELEGRAM_TOKEN for telegram)");
        builder = builder.add_connector(console::ConsoleConnector::new("console"));
    }

    builder.build().run().await?;
    Ok(())
}

/// The reflex tier: a data-driven router that disposes of routine sensor
/// readings deterministically and escalates only anomalies to cognition.
///
/// - `sensor-high` (priority High, Terminate): an `mqtt.factory.temperature`
///   reading with `celsius > 80` is rewritten to `sensor.anomaly` (tagged
///   `severity = high`) — which the cogitator escalates (via `OCTO_ACTIONABLE`).
/// - `sensor-routine` (priority Low, Observe): any other reading is recorded in
///   the trail and goes no further — no LLM.
fn reflex_router() -> Arc<RuleBasedRouter> {
    let high = Route {
        id: "sensor-high".to_string(),
        priority: Priority::High,
        strategy: RouteStrategy::Terminate,
        when: RoutePredicate {
            kind: Some("mqtt.factory.temperature".into()),
            payload: Some(PayloadPredicate {
                pointer: "/celsius".to_string(),
                op: NumOp::Gt,
                value: 80.0,
            }),
            ..Default::default()
        },
        then: RouteAction {
            // Cosmetic: the cogitator escalates by kind, not target.
            target: octo_core::ConnectorId::new("cognition"),
            override_kind: Some(EventKind::from_static("sensor.anomaly")),
            add_tags: HashMap::from([("severity".to_string(), "high".to_string())]),
            copy_payload: true,
            static_payload: None,
        },
        enabled: true,
    };

    let routine = Route {
        id: "sensor-routine".to_string(),
        priority: Priority::Low,
        strategy: RouteStrategy::Observe,
        when: RoutePredicate {
            kind: Some("mqtt.factory.temperature".into()),
            ..Default::default()
        },
        then: RouteAction {
            target: octo_core::ConnectorId::new("observer"),
            override_kind: None,
            add_tags: HashMap::new(),
            copy_payload: true,
            static_payload: None,
        },
        enabled: true,
    };

    RuleBasedRouter::builder("reflex")
        .add_route(high)
        .add_route(routine)
        .build()
}
