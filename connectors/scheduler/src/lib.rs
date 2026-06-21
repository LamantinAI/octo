//! `octo-connector-scheduler` — an input-only connector that emits time.
//!
//! The runtime's pitch is *"a system that lives in time and reacts to the
//! world."* Everything else reacts to *external* events; this connector is the
//! one tentacle whose stimulus is the clock. It emits `timer.tick` (a regular
//! heartbeat, typically observe-only) and `timer.fire` (a sparser cadence meant
//! to *drive cognition* — the agent acting on its own initiative).
//!
//! Zero external deps: pure `tokio::time`. Each [`Timer`] carries its own kind,
//! cadence (recurring `every` or one-shot `after`), an optional extra payload,
//! and an optional `max_ticks` cap. The payload is a `serde_json::Value`
//! (`{ name, seq, at, ... }`), matching how the dyn HTTP connector models its
//! payloads — so it validates cleanly against a [`PayloadRegistry`].
//!
//! ## Wiring
//!
//! In code:
//! ```no_run
//! use std::time::Duration;
//! use octo_connector_scheduler::{SchedulerConnector, Timer};
//! use octo_core::EventKind;
//!
//! let sched = SchedulerConnector::with_timers("heartbeat", vec![
//!     Timer::interval("pulse", Duration::from_secs(1)),                    // timer.tick
//!     Timer::interval("wake", Duration::from_secs(60))                     // timer.fire
//!         .with_kind(EventKind::from_static("timer.fire")),
//! ]);
//! ```
//!
//! Or via TOML (`type = "scheduler"`), registered with
//! `register_connector_type("scheduler", octo_connector_scheduler::factory())`.
//! See `heartbeat.toml` in this crate.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use octo_core::{
    Connector, ConnectorCapabilities, ConnectorContext, ConnectorId, Envelope, EventKind,
    OctoResult, PayloadRegistry,
};
use serde::Deserialize;
use serde_json::{json, Value};

/// One scheduled emission: a recurring interval **or** a one-shot delay.
#[derive(Debug, Clone)]
pub struct Timer {
    /// Human-readable name, carried on the payload as `name`.
    pub name: String,
    /// Event kind to emit. Defaults to `timer.tick` for intervals and
    /// `timer.fire` for one-shots; override with [`with_kind`](Self::with_kind).
    pub kind: EventKind,
    /// Recurring period. Mutually exclusive with `after`.
    pub every: Option<Duration>,
    /// One-shot delay from start. Mutually exclusive with `every`.
    pub after: Option<Duration>,
    /// Extra fields merged into the emitted payload object.
    pub payload: Option<Value>,
    /// Stop after this many emissions (intervals only).
    pub max_ticks: Option<u64>,
}

impl Timer {
    /// A recurring timer emitting `timer.tick` every `period`.
    pub fn interval(name: impl Into<String>, period: Duration) -> Self {
        Self {
            name: name.into(),
            kind: EventKind::from_static("timer.tick"),
            every: Some(period),
            after: None,
            payload: None,
            max_ticks: None,
        }
    }

    /// A one-shot timer emitting `timer.fire` once, `delay` after start.
    pub fn oneshot(name: impl Into<String>, delay: Duration) -> Self {
        Self {
            name: name.into(),
            kind: EventKind::from_static("timer.fire"),
            every: None,
            after: Some(delay),
            payload: None,
            max_ticks: None,
        }
    }

    pub fn with_kind(mut self, kind: EventKind) -> Self {
        self.kind = kind;
        self
    }

    pub fn with_payload(mut self, payload: Value) -> Self {
        self.payload = Some(payload);
        self
    }

    pub fn with_max_ticks(mut self, max: u64) -> Self {
        self.max_ticks = Some(max);
        self
    }

    /// First-fire delay (`every` for intervals, `after` for one-shots).
    fn first_delay(&self) -> Duration {
        self.every
            .or(self.after)
            .unwrap_or_else(|| Duration::from_secs(1))
    }
}

/// An input-only connector that publishes [`Timer`] envelopes on the bus.
pub struct SchedulerConnector {
    id: ConnectorId,
    capabilities: ConnectorCapabilities,
    timers: Vec<Timer>,
}

impl SchedulerConnector {
    /// Build from a set of timers.
    pub fn with_timers(id: impl Into<String>, timers: Vec<Timer>) -> Arc<Self> {
        // Advertise every distinct kind the timers emit.
        let mut emit: Vec<EventKind> = Vec::new();
        for t in &timers {
            if !emit.iter().any(|k| k.as_str() == t.kind.as_str()) {
                emit.push(t.kind.clone());
            }
        }
        let capabilities = ConnectorCapabilities::input_only().with_emit_kinds(emit);
        Arc::new(Self {
            id: ConnectorId::new(id),
            capabilities,
            timers,
        })
    }

    /// Convenience: a single `timer.tick` interval.
    pub fn interval(id: impl Into<String>, period: Duration) -> Arc<Self> {
        let id = id.into();
        Self::with_timers(id.clone(), vec![Timer::interval(id, period)])
    }

    fn register_kinds(&self, mut registry: PayloadRegistry) -> PayloadRegistry {
        for t in &self.timers {
            registry = registry.register_type::<Value>(t.kind.clone());
        }
        registry
    }
}

/// Per-timer mutable scheduling state.
struct TimerState {
    next: tokio::time::Instant,
    seq: u64,
    done: bool,
}

#[async_trait]
impl Connector for SchedulerConnector {
    fn id(&self) -> &ConnectorId {
        &self.id
    }

    fn capabilities(&self) -> &ConnectorCapabilities {
        &self.capabilities
    }

    async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
        let start = tokio::time::Instant::now();
        let mut states: Vec<TimerState> = self
            .timers
            .iter()
            .map(|t| TimerState {
                next: start + t.first_delay(),
                seq: 0,
                done: false,
            })
            .collect();

        loop {
            // The earliest pending deadline across all live timers.
            let next = states
                .iter()
                .enumerate()
                .filter(|(_, s)| !s.done)
                .map(|(i, s)| (i, s.next))
                .min_by_key(|(_, d)| *d);

            let Some((idx, deadline)) = next else {
                // All timers exhausted (all one-shots fired / hit max_ticks):
                // idle until the runtime shuts us down.
                ctx.shutdown.cancelled().await;
                return Ok(());
            };

            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => {
                    let timer = &self.timers[idx];
                    let state = &mut states[idx];
                    state.seq += 1;
                    let env = build_envelope(&self.id, timer, state.seq);
                    if let Err(e) = ctx.publish(env).await {
                        tracing::warn!(connector = %self.id, timer = %timer.name, error = %e, "failed to publish timer envelope");
                    }
                    match timer.every {
                        // Recurring: schedule the next fire (skew-free: from the
                        // deadline, not from now).
                        Some(period) => {
                            state.next = deadline + period;
                            if let Some(max) = timer.max_ticks {
                                if state.seq >= max {
                                    state.done = true;
                                }
                            }
                        }
                        // One-shot: retire it.
                        None => state.done = true,
                    }
                }
                _ = ctx.shutdown.cancelled() => return Ok(()),
            }
        }
    }

    fn register_payloads(&self, registry: PayloadRegistry) -> PayloadRegistry {
        self.register_kinds(registry)
    }
}

/// Build a timer envelope. Payload is `{ name, seq, at }` plus any extra fields
/// the timer carries.
fn build_envelope(id: &ConnectorId, timer: &Timer, seq: u64) -> Envelope {
    let mut payload = json!({
        "name": timer.name,
        "seq": seq,
        "at": chrono::Utc::now().to_rfc3339(),
    });
    if let (Some(obj), Some(Value::Object(extra))) =
        (payload.as_object_mut(), timer.payload.as_ref())
    {
        for (k, v) in extra {
            obj.insert(k.clone(), v.clone());
        }
    }
    Envelope::new(id.clone(), timer.kind.clone(), payload)
}

// ─── TOML factory ───────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct SchedulerSpec {
    #[serde(default)]
    timer: Vec<TimerSpec>,
}

#[derive(Debug, Deserialize)]
struct TimerSpec {
    name: String,
    every_ms: Option<u64>,
    after_ms: Option<u64>,
    kind: Option<String>,
    payload: Option<Value>,
    max_ticks: Option<u64>,
}

impl TimerSpec {
    fn into_timer(self) -> Result<Timer, String> {
        let mut timer = match (self.every_ms, self.after_ms) {
            (Some(ms), None) => Timer::interval(self.name, Duration::from_millis(ms)),
            (None, Some(ms)) => Timer::oneshot(self.name, Duration::from_millis(ms)),
            (Some(_), Some(_)) => {
                return Err(format!(
                    "timer '{}' sets both every_ms and after_ms; pick one",
                    self.name
                ))
            }
            (None, None) => {
                return Err(format!(
                    "timer '{}' sets neither every_ms nor after_ms",
                    self.name
                ))
            }
        };
        if let Some(kind) = self.kind {
            timer = timer.with_kind(EventKind::new(kind));
        }
        if let Some(payload) = self.payload {
            timer = timer.with_payload(payload);
        }
        if let Some(max) = self.max_ticks {
            timer = timer.with_max_ticks(max);
        }
        Ok(timer)
    }
}

/// [`ConnectorFactory`](octo_core::ConnectorFactory) for `type = "scheduler"`.
pub struct SchedulerConnectorFactory;

impl octo_core::ConnectorFactory for SchedulerConnectorFactory {
    fn type_name(&self) -> &str {
        "scheduler"
    }

    fn create(
        &self,
        id: ConnectorId,
        config: &toml::Value,
        _ctx: octo_core::FactoryContext<'_>,
    ) -> Result<Arc<dyn Connector>, Box<dyn std::error::Error + Send + Sync>> {
        let spec: SchedulerSpec = config.clone().try_into()?;
        if spec.timer.is_empty() {
            return Err("scheduler connector has no [[timer]] entries".into());
        }
        let timers = spec
            .timer
            .into_iter()
            .map(TimerSpec::into_timer)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(SchedulerConnector::with_timers(id.as_str().to_string(), timers))
    }
}

/// Factory handle for registration:
/// `register_connector_type("scheduler", octo_connector_scheduler::factory())`.
pub fn factory() -> Arc<dyn octo_core::ConnectorFactory> {
    Arc::new(SchedulerConnectorFactory)
}
