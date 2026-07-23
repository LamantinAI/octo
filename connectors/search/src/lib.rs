//! `octo-connector-search` — web search as an env-as-tools organ.
//!
//! The cogitator dispatches a command to this connector's `id` and gets a
//! correlated `search.web.result`. The backend is swappable behind
//! [`SearchBackend`] — **DuckDuckGo** now (account-free, scrape-based), Yandex
//! Search API later — so the command surface is stable while the source changes.
//!
//! Command:
//! - `search.web { query, limit? }` → `{ query, count, results: [{ title, url,
//!   snippet }] }` (limit clamped to `[1, max_limit]`, default `default_limit`).
//!
//! It returns a clean hit list — never raw HTML — so the model spends context on
//! results, not markup. Fetching a page's content is a separate organ.
//!
//! # Dependency: the system libcurl (DuckDuckGo backend only)
//!
//! The `ddg` backend fetches through **libcurl, in-process** (the `curl` crate) —
//! not reqwest, and specifically **the system libcurl, not a vendored build**. This
//! is measured, not incidental; [`ddg`] carries the evidence table. In short: DDG's
//! anti-bot rejects reqwest's TLS fingerprint (`202` + zero results, with rustls and
//! native-tls alike) *and* a vendored libcurl's (handshake dropped), while the system
//! libcurl — the very library the working `curl` command is a shell over — gets `200`
//! and a full page.
//!
//! So this crate needs:
//! - **at build time:** libcurl development files, e.g.
//!   `sudo apt install libcurl4-openssl-dev` (Debian/Ubuntu). Without them `curl-sys`
//!   silently vendors its own libcurl and search breaks at runtime.
//! - **at run time:** `libcurl.so.4`, which is present on every host that has the
//!   `curl` command (same package), so deployments running the forkd sandbox already
//!   satisfy it. Build host and target need compatible libcurl, the same way they
//!   already need compatible glibc.
//!
//! **Build trap:** cargo caches `curl-sys`'s build-script decision. If a build ran
//! before the dev headers were installed, installing them is not enough — run
//! `cargo clean -p curl-sys` and rebuild, else the vendored copy silently persists.
//! The linked version is logged when the connector starts (see [`ddg::libcurl_version`]),
//! so a vendored build is visible rather than a mystery 202 later.
//!
//! A TLS-impersonating client (`rquest`/BoringSSL) is the alternative if this
//! dependency ever needs to go away. Backends that talk to a real API (Yandex Search
//! API, next) carry no such requirement — this is a DuckDuckGo-specific cost of
//! having no official API.

mod ddg;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use octo_core::{
    Connector, ConnectorCapabilities, ConnectorContext, ConnectorFactory, ConnectorId, Envelope,
    EventKind, FactoryContext, Filter, OctoResult, SubscribeOptions,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub use ddg::DdgBackend;

const SEARCH: &str = "search.web";

const CATALOG: &str = "Web search — find pages/URLs for a question. Dispatch a command \
envelope to this connector's id:
- search.web { query, limit? } -> { query, count, results: [{ title, url, snippet }] }
`limit` is optional (default 5, capped). Use it to discover sources, then read the \
promising URLs. Returns a clean hit list, not raw HTML.";

/// One search result.
#[derive(Debug, Clone, Serialize)]
pub struct SearchHit {
    pub title: String,
    pub url: String,
    pub snippet: String,
}

/// A swappable search source. DuckDuckGo now; Yandex Search API later slots in
/// behind the same trait with zero change to the connector or the command surface.
#[async_trait]
pub trait SearchBackend: Send + Sync {
    /// Short backend name for logs/telemetry (e.g. `"ddg"`).
    fn name(&self) -> &str;
    /// Run `query` and return up to `limit` hits (best-first).
    async fn search(&self, query: &str, limit: usize) -> Result<Vec<SearchHit>, String>;
}

/// The search organ: routes `search.web` to a [`SearchBackend`].
pub struct SearchConnector {
    id: ConnectorId,
    capabilities: ConnectorCapabilities,
    backend: Arc<dyn SearchBackend>,
    default_limit: usize,
    max_limit: usize,
}

impl SearchConnector {
    pub fn new(
        id: impl Into<String>,
        backend: Arc<dyn SearchBackend>,
        default_limit: usize,
        max_limit: usize,
    ) -> Arc<Self> {
        let max_limit = max_limit.max(1);
        let capabilities = ConnectorCapabilities::bidirectional()
            .with_accept_kinds([EventKind::from_static(SEARCH)])
            .with_description(CATALOG);
        Arc::new(Self {
            id: ConnectorId::new(id),
            capabilities,
            backend,
            default_limit: default_limit.clamp(1, max_limit),
            max_limit,
        })
    }
}

#[async_trait]
impl Connector for SearchConnector {
    fn id(&self) -> &ConnectorId {
        &self.id
    }

    fn capabilities(&self) -> &ConnectorCapabilities {
        &self.capabilities
    }

    async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
        let mut cmds = ctx
            .subscribe(Filter::by_target(self.id.clone()), SubscribeOptions::default())
            .await?;
        // Log the linked libcurl: a vendored build (see ddg's build trap) shows up
        // here as an unexpected version, instead of as a puzzling 202 later.
        tracing::info!(
            connector = %self.id,
            backend = self.backend.name(),
            libcurl = %ddg::libcurl_version(),
            "search ready"
        );
        loop {
            tokio::select! {
                next = cmds.next() => match next {
                    Some(env) => self.handle(&env, &ctx).await,
                    None => return Ok(()),
                },
                _ = ctx.shutdown.cancelled() => return Ok(()),
            }
        }
    }
}

#[derive(Deserialize)]
struct SearchArgs {
    query: String,
    #[serde(default)]
    limit: Option<usize>,
}

impl SearchConnector {
    async fn handle(&self, env: &Envelope, ctx: &ConnectorContext) {
        if env.kind.as_str() != SEARCH {
            return; // not one of ours
        }
        let payload = env.payload_as::<Value>().cloned().unwrap_or(Value::Null);
        let outcome = self.run_search(payload).await;
        let payload = outcome.unwrap_or_else(|e| json!({ "error": e }));
        let resp = Envelope::new(self.id.clone(), EventKind::new(format!("{SEARCH}.result")), payload)
            .with_correlation(env.id);
        if let Err(e) = ctx.publish(resp).await {
            tracing::warn!(error = %e, "search failed to publish result");
        }
    }

    async fn run_search(&self, payload: Value) -> Result<Value, String> {
        let args: SearchArgs = serde_json::from_value(payload).map_err(|e| format!("bad args: {e}"))?;
        let query = args.query.trim();
        if query.is_empty() {
            return Err("`query` is required".into());
        }
        let limit = args.limit.unwrap_or(self.default_limit).clamp(1, self.max_limit);
        let hits = self.backend.search(query, limit).await?;
        tracing::info!(query, backend = self.backend.name(), count = hits.len(), "search done");
        Ok(json!({ "query": query, "count": hits.len(), "results": hits }))
    }
}

/// [`ConnectorFactory`] for `type = "search"`. Register with
/// `Octo::builder().register_connector_type("search", octo_connector_search::factory())`.
pub struct SearchConnectorFactory;

impl SearchConnectorFactory {
    pub fn new() -> Self {
        Self
    }
}

impl Default for SearchConnectorFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectorFactory for SearchConnectorFactory {
    fn type_name(&self) -> &str {
        "search"
    }

    fn create(
        &self,
        id: ConnectorId,
        config: &toml::Value,
        _ctx: FactoryContext<'_>,
    ) -> Result<Arc<dyn Connector>, Box<dyn std::error::Error + Send + Sync>> {
        let table = config
            .get("connector")
            .ok_or("search: manifest has no [connector] table")?;
        let backend_kind = table.get("backend").and_then(|v| v.as_str()).unwrap_or("ddg");
        let timeout = Duration::from_secs(
            table.get("timeout_secs").and_then(|v| v.as_integer()).unwrap_or(15).max(1) as u64,
        );
        let default_limit = table.get("default_limit").and_then(|v| v.as_integer()).unwrap_or(5).max(1) as usize;
        let max_limit = table.get("max_limit").and_then(|v| v.as_integer()).unwrap_or(10).max(1) as usize;
        let backend: Arc<dyn SearchBackend> = match backend_kind {
            "ddg" => {
                // DDG's `kl` locale (region-language), e.g. "ru-ru" / "us-en"; optional.
                let region = table.get("region").and_then(|v| v.as_str()).map(str::to_string);
                Arc::new(DdgBackend::new(timeout, region)?)
            }
            other => return Err(format!("search: unknown backend `{other}` (have: ddg)").into()),
        };
        Ok(SearchConnector::new(id.as_str(), backend, default_limit, max_limit))
    }
}

/// Convenience factory handle for registration.
pub fn factory() -> Arc<dyn ConnectorFactory> {
    Arc::new(SearchConnectorFactory::new())
}
