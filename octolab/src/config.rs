//! Settings assembled by the `config` crate from `OCTO_`-prefixed env vars
//! (loaded from the repo-root `.env` in `main`):
//! - `OCTO_OPENAI_KEY`    → `api_key`
//! - `OCTO_LLM_MODEL`     → `model`
//! - `OCTO_LLM_BASE_URL`  → `base_url` (informational for the openrouter provider)
//! - `OCTO_TELEGRAM_TOKEN`→ `telegram_token` (optional; absent → console connector)
//! - `OCTO_HISTORY`       → `history` (`memory` default | `file:<dir>`)
//! - `OCTO_PERCEPTION`    → `perception` (`addressed` default | `chat` | `all` | glob)
//! - `OCTO_ACTIONABLE`    → `actionable` (comma-separated non-chat kind globs the
//!   agent acts on proactively, e.g. `sensor.anomaly,timer.fire`; default none)
//! - `OCTO_PROACTIVE_TARGET`  → `proactive_target` (connector id a proactive
//!   message is delivered to, e.g. `telegram` / `console`)
//! - `OCTO_PROACTIVE_CHANNEL` → `proactive_channel` (channel id on that connector)

use serde::Deserialize;

use crate::error::Result;

#[derive(Debug, Clone, Deserialize)]
pub struct Settings {
    #[serde(rename = "openai_key")]
    pub api_key: String,
    #[serde(rename = "llm_model")]
    pub model: String,
    #[serde(rename = "llm_base_url", default)]
    pub base_url: String,
    #[serde(rename = "telegram_token", default)]
    pub telegram_token: Option<String>,
    /// History backend: `memory` (default) or `file:<dir>`. (`OCTO_HISTORY`.)
    #[serde(rename = "history", default)]
    pub history: Option<String>,
    /// Perception scope (`OCTO_PERCEPTION`): `addressed` (default) | `chat` |
    /// `all` | a custom event-kind glob. Sets the cogitator's subscription —
    /// how much of the bus it sees (action stays narrow regardless).
    #[serde(rename = "perception", default)]
    pub perception: Option<String>,
    /// Non-chat event kinds the agent acts on *proactively* (`OCTO_ACTIONABLE`),
    /// comma-separated globs (e.g. `sensor.anomaly,timer.fire`). Empty = today's
    /// chat-only behavior. Deliberately narrower than perception.
    #[serde(rename = "actionable", default)]
    pub actionable: Option<String>,
    /// Connector id a proactive (self-initiated) message is delivered to
    /// (`OCTO_PROACTIVE_TARGET`, e.g. `telegram` / `console`).
    #[serde(rename = "proactive_target", default)]
    pub proactive_target: Option<String>,
    /// Channel id on [`proactive_target`](Self::proactive_target) for proactive
    /// messages (`OCTO_PROACTIVE_CHANNEL`, e.g. a Telegram chat id or `stdin`).
    #[serde(rename = "proactive_channel", default)]
    pub proactive_channel: Option<String>,
    /// Seconds between `timer.fire` wakes from the scheduler (`OCTO_WAKE_SECS`,
    /// default 60). Keep it sparse — each fire can cost an LLM call.
    #[serde(rename = "wake_secs", default)]
    pub wake_secs: Option<u64>,
    /// MQTT broker host (`OCTO_MQTT_HOST`). When set, octolab attaches an MQTT
    /// connector subscribed to `factory/#`; absent → no MQTT (self-contained).
    #[serde(rename = "mqtt_host", default)]
    pub mqtt_host: Option<String>,
}

impl Settings {
    pub fn load() -> Result<Self> {
        let cfg = config::Config::builder()
            .add_source(config::Environment::with_prefix("OCTO"))
            .build()?;
        Ok(cfg.try_deserialize()?)
    }
}
