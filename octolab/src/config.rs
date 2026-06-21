//! Settings assembled by the `config` crate from `OCTO_`-prefixed env vars
//! (loaded from the repo-root `.env` in `main`):
//! - `OCTO_OPENAI_KEY`    → `api_key`
//! - `OCTO_LLM_MODEL`     → `model`
//! - `OCTO_LLM_BASE_URL`  → `base_url` (informational for the openrouter provider)
//! - `OCTO_TELEGRAM_TOKEN`→ `telegram_token` (optional; absent → console connector)

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
}

impl Settings {
    pub fn load() -> Result<Self> {
        let cfg = config::Config::builder()
            .add_source(config::Environment::with_prefix("OCTO"))
            .build()?;
        Ok(cfg.try_deserialize()?)
    }
}
