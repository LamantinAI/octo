//! `octo-connector-search` — web search as an env-as-tools organ.
//!
//! The cogitator dispatches a command to this connector's `id` and gets a
//! correlated `search.web.result`. The backend is swappable behind
//! [`SearchBackend`] — **DuckDuckGo** now (account-free, scrape-based), Yandex
//! Search API later — so the command surface is stable while the source changes.
//!
//! Command:
//! - `search.web { query, limit?, engine? }` → `{ engine, query, count, results:
//!   [{ title, url, snippet }] }` (limit clamped to `[1, max_limit]`, default
//!   `default_limit`).
//!
//! **Choosing the search system happens at both levels.** The manifest declares
//! which engines exist (`[connector.engines.<name>]`, each with its own settings)
//! and which is `default_engine`; a caller then overrides per call with `engine`.
//! One connector instance can therefore front several systems — DuckDuckGo today,
//! Yandex next — and the model picks per query without any config change.
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

use std::collections::BTreeMap;
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

/// The catalog the model reads. Built per instance so it names the engines this
/// deployment actually has (and which one answers when `engine` is omitted).
fn catalog(engines: &BTreeMap<String, Arc<dyn SearchBackend>>, default_engine: &str, default_limit: usize, max_limit: usize) -> String {
    let names: Vec<&str> = engines.keys().map(String::as_str).collect();
    format!(
        "Web search — find pages/URLs for a question. Dispatch a command envelope to this \
         connector's id:\n\
         - search.web {{ query, limit?, engine? }} -> {{ engine, query, count, results: \
         [{{ title, url, snippet }}] }}\n\
         `limit` is optional (default {default_limit}, max {max_limit}). `engine` picks the \
         search system — available: {}; omit it to use {default_engine}. Use this to discover \
         sources, then read the promising URLs. Returns a clean hit list, not raw HTML.",
        names.join(", ")
    )
}

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

/// The search organ: routes `search.web` to one of its [`SearchBackend`]s.
pub struct SearchConnector {
    id: ConnectorId,
    capabilities: ConnectorCapabilities,
    /// Every engine this instance fronts, keyed by the name callers pass as `engine`.
    engines: BTreeMap<String, Arc<dyn SearchBackend>>,
    /// Which engine answers when the caller omits `engine`.
    default_engine: String,
    default_limit: usize,
    max_limit: usize,
}

impl SearchConnector {
    /// `engines` must be non-empty and contain `default_engine`; the factory checks
    /// both, so a misconfigured manifest fails at startup rather than at query time.
    pub fn new(
        id: impl Into<String>,
        engines: BTreeMap<String, Arc<dyn SearchBackend>>,
        default_engine: impl Into<String>,
        default_limit: usize,
        max_limit: usize,
    ) -> Arc<Self> {
        let max_limit = max_limit.max(1);
        let default_limit = default_limit.clamp(1, max_limit);
        let default_engine = default_engine.into();
        let capabilities = ConnectorCapabilities::bidirectional()
            .with_accept_kinds([EventKind::from_static(SEARCH)])
            .with_description(catalog(&engines, &default_engine, default_limit, max_limit));
        Arc::new(Self {
            id: ConnectorId::new(id),
            capabilities,
            engines,
            default_engine,
            default_limit,
            max_limit,
        })
    }

    /// Engine names this instance can serve, for logs and error messages.
    fn engine_names(&self) -> String {
        self.engines.keys().cloned().collect::<Vec<_>>().join(", ")
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
            engines = %self.engine_names(),
            default_engine = %self.default_engine,
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
    /// Which search system to use; omitted → the manifest's `default_engine`.
    #[serde(default)]
    engine: Option<String>,
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
        let engine = args.engine.as_deref().unwrap_or(&self.default_engine);
        let backend = self.engines.get(engine).ok_or_else(|| {
            format!("unknown engine `{engine}`; available: {}", self.engine_names())
        })?;
        let hits = backend.search(query, limit).await?;
        tracing::info!(query, engine, count = hits.len(), "search done");
        Ok(json!({ "engine": engine, "query": query, "count": hits.len(), "results": hits }))
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
        let timeout = Duration::from_secs(
            table.get("timeout_secs").and_then(|v| v.as_integer()).unwrap_or(15).max(1) as u64,
        );
        let default_limit = table.get("default_limit").and_then(|v| v.as_integer()).unwrap_or(10).max(1) as usize;
        let max_limit = table.get("max_limit").and_then(|v| v.as_integer()).unwrap_or(25).max(1) as usize;

        // Preferred form: one `[connector.engines.<name>]` table per search system,
        // each carrying its own settings. `enabled = false` keeps an engine declared
        // but out of service.
        let mut engines: BTreeMap<String, Arc<dyn SearchBackend>> = BTreeMap::new();
        match table.get("engines").and_then(|v| v.as_table()) {
            Some(declared) => {
                for (name, cfg) in declared {
                    if cfg.get("enabled").and_then(|v| v.as_bool()) == Some(false) {
                        continue;
                    }
                    engines.insert(name.clone(), build_engine(name, cfg, timeout)?);
                }
            }
            // Shorthand for a single-engine deployment: `backend = "ddg"` with its
            // settings inline on [connector].
            None => {
                let name = table.get("backend").and_then(|v| v.as_str()).unwrap_or("ddg");
                engines.insert(name.to_string(), build_engine(name, table, timeout)?);
            }
        }
        if engines.is_empty() {
            return Err("search: no engines enabled — declare at least one".into());
        }

        // Default engine: explicit, else the only/first one declared.
        let default_engine = match table.get("default_engine").and_then(|v| v.as_str()) {
            Some(name) => name.to_string(),
            None => engines.keys().next().cloned().expect("non-empty checked above"),
        };
        if !engines.contains_key(&default_engine) {
            return Err(format!(
                "search: default_engine `{default_engine}` is not among the enabled engines ({})",
                engines.keys().cloned().collect::<Vec<_>>().join(", ")
            )
            .into());
        }
        Ok(SearchConnector::new(id.as_str(), engines, default_engine, default_limit, max_limit))
    }
}

/// Build one engine from its config slice. Add a `match` arm per search system —
/// this is the single place a new backend gets wired in.
fn build_engine(
    name: &str,
    cfg: &toml::Value,
    timeout: Duration,
) -> Result<Arc<dyn SearchBackend>, Box<dyn std::error::Error + Send + Sync>> {
    match name {
        "ddg" => {
            // DDG's `kl` locale (region-language), e.g. "ru-ru" / "us-en"; optional.
            let region = cfg.get("region").and_then(|v| v.as_str()).map(str::to_string);
            Ok(Arc::new(DdgBackend::new(timeout, region)?))
        }
        other => Err(format!(
            "search: unknown engine `{other}` (have: ddg; yandex planned)"
        )
        .into()),
    }
}

/// Convenience factory handle for registration.
pub fn factory() -> Arc<dyn ConnectorFactory> {
    Arc::new(SearchConnectorFactory::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A backend that answers with its own name, so a test can see which one ran.
    struct StubBackend(&'static str);

    #[async_trait]
    impl SearchBackend for StubBackend {
        fn name(&self) -> &str {
            self.0
        }
        async fn search(&self, query: &str, _limit: usize) -> Result<Vec<SearchHit>, String> {
            Ok(vec![SearchHit {
                title: format!("{} answered {query}", self.0),
                url: "https://example.com/".into(),
                snippet: String::new(),
            }])
        }
    }

    fn connector(default_engine: &str) -> Arc<SearchConnector> {
        let mut engines: BTreeMap<String, Arc<dyn SearchBackend>> = BTreeMap::new();
        engines.insert("ddg".into(), Arc::new(StubBackend("ddg")));
        engines.insert("yandex".into(), Arc::new(StubBackend("yandex")));
        SearchConnector::new("search", engines, default_engine, 10, 25)
    }

    #[tokio::test]
    async fn omitting_engine_uses_the_default() {
        let out = connector("ddg").run_search(json!({ "query": "x" })).await.unwrap();
        assert_eq!(out["engine"], "ddg");
    }

    #[tokio::test]
    async fn an_explicit_engine_wins_over_the_default() {
        let out = connector("ddg")
            .run_search(json!({ "query": "x", "engine": "yandex" }))
            .await
            .unwrap();
        assert_eq!(out["engine"], "yandex");
    }

    #[tokio::test]
    async fn an_unknown_engine_names_the_available_ones() {
        let err = connector("ddg")
            .run_search(json!({ "query": "x", "engine": "bing" }))
            .await
            .unwrap_err();
        assert!(err.contains("unknown engine `bing`"), "{err}");
        assert!(err.contains("ddg") && err.contains("yandex"), "{err}");
    }

    #[tokio::test]
    async fn limit_is_clamped_to_max() {
        // max_limit is 25 here; asking for more must not error, just clamp.
        let out = connector("ddg")
            .run_search(json!({ "query": "x", "limit": 500 }))
            .await
            .unwrap();
        assert_eq!(out["count"], 1);
    }

    #[test]
    fn the_catalog_names_engines_and_the_default() {
        let mut engines: BTreeMap<String, Arc<dyn SearchBackend>> = BTreeMap::new();
        engines.insert("ddg".into(), Arc::new(StubBackend("ddg")));
        let text = catalog(&engines, "ddg", 10, 25);
        assert!(text.contains("available: ddg"), "{text}");
        assert!(text.contains("omit it to use ddg"), "{text}");
    }
}
