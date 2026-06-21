//! `octo-connector-scheduler` — a managed bidirectional connector that owns
//! **time**: it holds a list of alarms, emits an `alarm.fired` envelope when one
//! is due, and is mutated through `octo.scheduler.*` control envelopes (so the
//! cogitator schedules reminders via the ordinary env-as-tools dispatch path).
//!
//! Shape follows the `notifications.md` / `manageable_actors.md` vault design:
//! state is data, persisted to disk (`<path>`), survives restart, observable on
//! the bus. The cogitator never holds a timer — it just dispatches a command and
//! later perceives `alarm.fired`.
//!
//! ## Control surface (accept kinds)
//! - `octo.scheduler.add_alarm`    payload: [`AddAlarmCmd`]      → `…add_alarm.result { alarm_id }`
//! - `octo.scheduler.cancel_alarm` payload: `{ "alarm_id": … }`  → `…cancel_alarm.result { cancelled }`
//! - `octo.scheduler.list_alarms`  payload: `{}`                 → `…list_alarms.result { alarms: […] }`
//!
//! Replies are correlated by the command's id, so a dispatcher using
//! `publish_and_await_response` gets the result back.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use octo_core::{
    Connector, ConnectorCapabilities, ConnectorContext, ConnectorId, Envelope, EventKind, Filter,
    OctoResult, SubscribeOptions,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

const DEFAULT_EMIT_KIND: &str = "alarm.fired";

/// How an alarm recurs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlarmTrigger {
    /// Fire once at the scheduled time, then drop.
    OneShot,
    /// Fire every `period_secs`, starting `period_secs` after creation, until cancelled.
    Interval { period_secs: u64 },
}

/// A scheduled alarm. `next_fire` is the absolute time of its next emission.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alarm {
    pub id: String,
    pub trigger: AlarmTrigger,
    /// Event kind to emit (default `alarm.fired`).
    pub kind: String,
    /// Payload carried in the emitted envelope (opaque JSON — e.g. the kaeru
    /// task ref + the channel to remind on).
    pub payload: Value,
    /// Tags attached to the emitted envelope.
    pub tags: HashMap<String, String>,
    /// Optional target connector for the emission.
    pub target: Option<String>,
    pub next_fire: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

impl Alarm {
    /// Build the envelope this alarm emits when it fires.
    fn fire_envelope(&self, source: &ConnectorId) -> Envelope {
        let mut env = Envelope::new(
            source.clone(),
            EventKind::new(self.kind.clone()),
            self.payload.clone(),
        )
        .with_tag("alarm_id", self.id.clone());
        for (k, v) in &self.tags {
            env = env.with_tag(k, v.clone());
        }
        if let Some(t) = &self.target {
            env = env.with_target(ConnectorId::new(t.clone()));
        }
        env
    }
}

/// The trigger as the cogitator (or any caller) specifies it on `add_alarm`.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TriggerSpec {
    /// One-shot at an absolute RFC3339 time, e.g. `"2026-06-22T15:00:00Z"`.
    Oneshot { at: String },
    /// Recurring every `period_secs`, until cancelled.
    Interval { period_secs: u64 },
}

/// Payload of an `octo.scheduler.add_alarm` command.
#[derive(Debug, Clone, Deserialize)]
pub struct AddAlarmCmd {
    pub trigger: TriggerSpec,
    #[serde(default = "default_kind")]
    pub kind: String,
    #[serde(default)]
    pub payload: Value,
    #[serde(default)]
    pub tags: HashMap<String, String>,
    #[serde(default)]
    pub target: Option<String>,
}

fn default_kind() -> String {
    DEFAULT_EMIT_KIND.to_string()
}

/// A scheduler connector instance. Holds its alarms in memory and mirrors them
/// to `persistence_path` (atomic write) on every mutation.
pub struct Scheduler {
    id: ConnectorId,
    capabilities: ConnectorCapabilities,
    alarms: Mutex<Vec<Alarm>>,
    persistence_path: PathBuf,
    tick: Duration,
}

const CATALOG: &str = "Schedule reminders / alarms. Command kinds:\n\
 - kind \"octo.scheduler.add_alarm\", payload { trigger: { type: \"interval\", period_secs: <u64> } \
   OR { type: \"oneshot\", at: \"<RFC3339 UTC>\" }, payload: <object carried into alarm.fired>, \
   tags?: {..}, target?: \"<connector>\" } → returns { alarm_id }. \
   Put what you'll need when it fires (e.g. the memory task name and the channel to remind on) into `payload`.\n\
 - kind \"octo.scheduler.cancel_alarm\", payload { alarm_id: \"<id>\" } → stops a recurring reminder.\n\
 - kind \"octo.scheduler.list_alarms\", payload {} → lists active alarms.";

impl Scheduler {
    pub fn new(id: impl Into<String>, persistence_path: impl Into<PathBuf>) -> Arc<Self> {
        let capabilities = ConnectorCapabilities::bidirectional()
            .with_emit_kinds([EventKind::from_static(DEFAULT_EMIT_KIND)])
            .with_accept_kinds([
                EventKind::from_static("octo.scheduler.add_alarm"),
                EventKind::from_static("octo.scheduler.cancel_alarm"),
                EventKind::from_static("octo.scheduler.list_alarms"),
            ])
            .with_description(CATALOG);
        Arc::new(Self {
            id: ConnectorId::new(id),
            capabilities,
            alarms: Mutex::new(Vec::new()),
            persistence_path: persistence_path.into(),
            tick: Duration::from_secs(1),
        })
    }

    fn load(&self) {
        match std::fs::read_to_string(&self.persistence_path) {
            Ok(s) => match serde_json::from_str::<Vec<Alarm>>(&s) {
                Ok(v) => {
                    let n = v.len();
                    *self.alarms.lock().unwrap() = v;
                    tracing::info!(connector = %self.id, alarms = n, "scheduler: loaded state");
                }
                Err(e) => tracing::warn!(error = %e, "scheduler: corrupt state file; starting empty"),
            },
            Err(_) => tracing::info!(connector = %self.id, "scheduler: no state file; starting empty"),
        }
    }

    /// Serialize current alarms and atomically write to disk. Best-effort:
    /// errors are logged, never fatal to the connector.
    fn persist(&self) {
        let snapshot = {
            let alarms = self.alarms.lock().unwrap();
            serde_json::to_string_pretty(&*alarms)
        };
        let Ok(snapshot) = snapshot else { return };
        if let Some(dir) = self.persistence_path.parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        let tmp = self.persistence_path.with_extension("json.tmp");
        if let Err(e) = std::fs::write(&tmp, snapshot).and_then(|_| std::fs::rename(&tmp, &self.persistence_path)) {
            tracing::warn!(error = %e, "scheduler: failed to persist state");
        }
    }

    /// Collect due alarms, advance/drop them, and return the envelopes to emit.
    fn collect_due(&self, now: DateTime<Utc>) -> Vec<Envelope> {
        let mut to_emit = Vec::new();
        let mut drop_ids: Vec<String> = Vec::new();
        {
            let mut alarms = self.alarms.lock().unwrap();
            for a in alarms.iter_mut() {
                if a.next_fire > now {
                    continue;
                }
                to_emit.push(a.fire_envelope(&self.id));
                match a.trigger {
                    AlarmTrigger::OneShot => drop_ids.push(a.id.clone()),
                    AlarmTrigger::Interval { period_secs } => {
                        a.next_fire = now + chrono::Duration::seconds(period_secs as i64);
                    }
                }
            }
            if !drop_ids.is_empty() {
                alarms.retain(|a| !drop_ids.contains(&a.id));
            }
        }
        to_emit
    }

    async fn on_tick(self: &Arc<Self>, ctx: &ConnectorContext) {
        let due = self.collect_due(Utc::now());
        if due.is_empty() {
            return;
        }
        self.persist();
        for env in due {
            tracing::info!(connector = %self.id, kind = %env.kind, "scheduler: alarm fired");
            if let Err(e) = ctx.publish(env).await {
                tracing::warn!(error = %e, "scheduler: failed to emit alarm");
            }
        }
    }

    async fn on_control(self: &Arc<Self>, cmd: Arc<Envelope>, ctx: &ConnectorContext) {
        let (result_kind, body) = match cmd.kind.as_str() {
            "octo.scheduler.add_alarm" => ("octo.scheduler.add_alarm.result", self.add_alarm(&cmd)),
            "octo.scheduler.cancel_alarm" => {
                ("octo.scheduler.cancel_alarm.result", self.cancel_alarm(&cmd))
            }
            "octo.scheduler.list_alarms" => ("octo.scheduler.list_alarms.result", self.list_alarms()),
            other => {
                tracing::warn!(kind = %other, "scheduler: unknown control kind");
                return;
            }
        };
        let reply = Envelope::new(self.id.clone(), EventKind::new(result_kind), body)
            .with_target(cmd.source.clone())
            .with_correlation(cmd.id);
        if let Err(e) = ctx.publish(reply).await {
            tracing::warn!(error = %e, "scheduler: failed to publish control result");
        }
    }

    fn add_alarm(&self, cmd: &Envelope) -> Value {
        let Some(raw) = cmd.payload_as::<Value>().cloned() else {
            return json!({ "error": "add_alarm: expected a JSON payload" });
        };
        let parsed: AddAlarmCmd = match serde_json::from_value(raw) {
            Ok(c) => c,
            Err(e) => return json!({ "error": format!("add_alarm: bad command: {e}") }),
        };
        let now = Utc::now();
        let (trigger, next_fire) = match parsed.trigger {
            TriggerSpec::Oneshot { at } => match DateTime::parse_from_rfc3339(&at) {
                Ok(t) => (AlarmTrigger::OneShot, t.with_timezone(&Utc)),
                Err(e) => return json!({ "error": format!("add_alarm: bad `at` time (need RFC3339): {e}") }),
            },
            TriggerSpec::Interval { period_secs } => {
                if period_secs == 0 {
                    return json!({ "error": "add_alarm: period_secs must be > 0" });
                }
                (
                    AlarmTrigger::Interval { period_secs },
                    now + chrono::Duration::seconds(period_secs as i64),
                )
            }
        };
        let id = uuid::Uuid::now_v7().to_string();
        let alarm = Alarm {
            id: id.clone(),
            trigger,
            kind: parsed.kind,
            payload: parsed.payload,
            tags: parsed.tags,
            target: parsed.target,
            next_fire,
            created_at: now,
        };
        self.alarms.lock().unwrap().push(alarm);
        self.persist();
        tracing::info!(connector = %self.id, alarm_id = %id, %next_fire, "scheduler: alarm added");
        json!({ "alarm_id": id, "next_fire": next_fire.to_rfc3339() })
    }

    fn cancel_alarm(&self, cmd: &Envelope) -> Value {
        let id = cmd
            .payload_as::<Value>()
            .and_then(|v| v.get("alarm_id").and_then(|x| x.as_str()).map(str::to_owned));
        let Some(id) = id else {
            return json!({ "error": "cancel_alarm: expected { alarm_id }" });
        };
        let removed = {
            let mut alarms = self.alarms.lock().unwrap();
            let before = alarms.len();
            alarms.retain(|a| a.id != id);
            before != alarms.len()
        };
        if removed {
            self.persist();
        }
        json!({ "cancelled": removed, "alarm_id": id })
    }

    fn list_alarms(&self) -> Value {
        let alarms = self.alarms.lock().unwrap();
        let items: Vec<Value> = alarms
            .iter()
            .map(|a| {
                json!({
                    "id": a.id,
                    "kind": a.kind,
                    "next_fire": a.next_fire.to_rfc3339(),
                    "trigger": a.trigger,
                    "payload": a.payload,
                })
            })
            .collect();
        json!({ "alarms": items })
    }
}

#[async_trait]
impl Connector for Scheduler {
    fn id(&self) -> &ConnectorId {
        &self.id
    }

    fn capabilities(&self) -> &ConnectorCapabilities {
        &self.capabilities
    }

    async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
        self.load();
        let mut control = ctx
            .subscribe(
                Filter::by_kind("octo.scheduler.*"),
                SubscribeOptions::default(),
            )
            .await?;
        let mut tick = tokio::time::interval(self.tick);
        // Fire any alarms already overdue at startup (FireLate) on the first tick.
        loop {
            tokio::select! {
                _ = tick.tick() => self.on_tick(&ctx).await,
                next = control.next() => match next {
                    Some(env) => self.clone().on_control(env, &ctx).await,
                    None => return Ok(()),
                },
                _ = ctx.shutdown.cancelled() => return Ok(()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use octo_core::{ConnectorId, EventBus, InProcessBus};
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    /// End-to-end over the bus, no LLM: add an interval alarm → it fires with the
    /// configured payload → cancel it → it stops.
    #[tokio::test]
    async fn interval_alarm_fires_and_cancels() {
        let path = std::env::temp_dir().join(format!("octo-sched-test-{}.json", uuid::Uuid::now_v7()));
        let bus = Arc::new(InProcessBus::new(64));
        let shutdown = CancellationToken::new();
        let sched = Scheduler::new("sched", path.clone());

        // Observe alarm emissions before anything starts.
        let mut fires =
            bus.subscribe_sync(Filter::by_kind("alarm.fired"), SubscribeOptions::default());

        let ctx = ConnectorContext::new(shutdown.clone(), Arc::clone(&bus));
        let handle = tokio::spawn(sched.run(ctx));
        // Let the connector register its control subscription before we publish.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Schedule a 1s recurring reminder.
        let add = Envelope::new(
            ConnectorId::new("tester"),
            EventKind::new("octo.scheduler.add_alarm"),
            json!({
                "trigger": { "type": "interval", "period_secs": 1 },
                "payload": { "task": "drink water", "channel": "stdin" }
            }),
        )
        .with_target(ConnectorId::new("sched"));
        let resp = bus
            .publish_and_await_response(add, Duration::from_secs(5))
            .await
            .expect("add_alarm result");
        let alarm_id = resp
            .payload_as::<Value>()
            .and_then(|v| v.get("alarm_id").and_then(Value::as_str).map(str::to_owned))
            .expect("alarm_id in result");

        // It should fire within a couple ticks, carrying its payload + alarm_id tag.
        let fired = tokio::time::timeout(Duration::from_secs(4), fires.next())
            .await
            .expect("alarm fired in time")
            .expect("bus open");
        assert_eq!(fired.kind.as_str(), "alarm.fired");
        let p = fired.payload_as::<Value>().expect("payload");
        assert_eq!(p["task"], "drink water");
        assert_eq!(fired.tags.get("alarm_id").map(String::as_str), Some(alarm_id.as_str()));

        // Cancel it.
        let cancel = Envelope::new(
            ConnectorId::new("tester"),
            EventKind::new("octo.scheduler.cancel_alarm"),
            json!({ "alarm_id": alarm_id }),
        )
        .with_target(ConnectorId::new("sched"));
        let cresp = bus
            .publish_and_await_response(cancel, Duration::from_secs(5))
            .await
            .expect("cancel result");
        assert_eq!(cresp.payload_as::<Value>().unwrap()["cancelled"], true);

        shutdown.cancel();
        let _ = handle.await;
        let _ = std::fs::remove_file(&path);
    }
}
