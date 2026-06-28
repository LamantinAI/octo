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

use std::sync::Arc;

use error::Result;
use octo_connector_http::HttpConnector;
use octo_core::{Octo, PayloadRegistry};

/// Absolute path to the dyn petstore manifest (cwd-independent).
const PETSTORE_MANIFEST: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/../config/connectors/petstore/petstore.toml");

/// octolab runtime manifest — config-driven connectors (the Telegram channel).
const OCTO_TOML: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/config/octo.toml");

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
    let registry = petstore.register_payloads(PayloadRegistry::new());
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

    let mut builder = Octo::builder()
        .payload_registry(Arc::new(registry))
        .cogitator(cogitator::ReactCogitator::new("react", settings.clone(), history))
        .add_connector(petstore);

    // ── User channel: config-driven Telegram (with ACL), or console fallback ──
    // With a token present, the Telegram connector is assembled from
    // config/octo.toml via its factory: the token comes from the env, while
    // authorization (the chat allow-list) lives in the connector's own
    // telegram.toml + ACL file. Otherwise, console.
    if settings.telegram_token.is_some() {
        eprintln!("[octolab] channel: telegram (config-driven, ACL)");
        builder = builder
            .register_connector_type("telegram", octo_connector_telegram::factory())
            .from_config_file(OCTO_TOML)?;
    } else {
        eprintln!("[octolab] channel: console (set OCTO_TELEGRAM_TOKEN for telegram)");
        builder = builder.add_connector(console::ConsoleConnector::new("console"));
    }

    builder.build().run().await?;
    Ok(())
}
