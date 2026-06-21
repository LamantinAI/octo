//! Crate error type — explicit variants with `#[from]` conversions (no
//! `anyhow`), mirroring the `octo_core::OctoError` style.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("config: {0}")]
    Config(#[from] config::ConfigError),

    #[error("octo runtime: {0}")]
    Octo(#[from] octo_core::OctoError),

    #[error("http connector spec: {0}")]
    HttpSpec(#[from] octo_connector_http::SpecError),

    #[error("history: {0}")]
    History(#[from] octo_history::HistoryError),

    #[error("llm client: {0}")]
    LlmClient(#[from] rig::http_client::Error),

    #[error("llm prompt: {0}")]
    LlmPrompt(#[from] rig::completion::PromptError),

    #[error("http fetch: {0}")]
    Fetch(#[from] reqwest::Error),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
