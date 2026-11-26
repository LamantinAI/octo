//! `octo-connector-search` — web search as an env-as-tools organ.
//!
//! The cogitator dispatches a command to this connector's `id` and gets a
//! correlated `search.web.result`. The backend is swappable behind
//! [`SearchBackend`]. DuckDuckGo, Tavily, Yandex, and Brave
//! implement the same query and result contract.
//!
//! Command:
//! - `search.web { query, limit?, engine?, page? }` → `{ engine, query, count, results:
//!   [{ title, url, snippet }] }` (limit clamped to `[1, max_limit]`, default
//!   `default_limit`).
//! - Browser-backed DuckDuckGo returns `page` and `next_page` (null at the end).
//! - Provider failure → `{ engine, error, alternatives, setup_options }`.
//!   Alternatives are enabled providers. Setup options name supported providers
//!   that can be enabled and what they need.
//!
//! **Choosing the search system happens at both levels.** The manifest declares
//! which engines exist (`[connector.engines.<name>]`, each with its own settings)
//! and which is `default_engine`; a caller then overrides per call with `engine`.
//! A request selects from the engines enabled on that connector.
//!
//! It returns a clean hit list — never raw HTML — so the model spends context on
//! results, not markup. Fetching a page's content is a separate organ.
//!
//! # DuckDuckGo runtime
//!
//! DuckDuckGo can use a shared Moli process or a Firefox WebDriver session.

mod api;
mod backend;
mod brave;
mod ddg;
mod factory;
mod tavily;
mod yandex;

use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use octo_core::{
    Connector, ConnectorCapabilities, ConnectorContext, ConnectorId, Envelope, EventKind, Filter,
    OctoResult, SubscribeOptions,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub use backend::{SearchBackend, SearchHit, SearchPage};
pub use factory::{SearchConnectorFactory, factory, factory_with_moli};

const SEARCH: &str = "search.web";

#[derive(Serialize)]
struct SetupOption {
    engine: &'static str,
    setup: &'static str,
}

const SETUP_OPTIONS: &[SetupOption] = &[
    SetupOption {
        engine: "tavily",
        setup: "Enable Tavily in keyless mode.",
    },
    SetupOption {
        engine: "yandex",
        setup: "Add a Yandex Search API key and Folder ID, then enable Yandex.",
    },
    SetupOption {
        engine: "brave",
        setup: "Add a Brave Search API key with LLM Context access, then enable Brave.",
    },
    SetupOption {
        engine: "ddg",
        setup: "Enable DuckDuckGo. It uses the application's browser runtime.",
    },
];

/// The catalog the model reads. Built per instance so it names the engines this
/// deployment actually has (and which one answers when `engine` is omitted).
fn catalog(
    engines: &BTreeMap<String, Arc<dyn SearchBackend>>,
    default_engine: &str,
    default_limit: usize,
    max_limit: usize,
) -> String {
    let names: Vec<&str> = engines.keys().map(String::as_str).collect();
    format!(
        "Web search — find pages/URLs for a question. Dispatch a command envelope to this \
         connector's id:\n\
         - search.web {{ query, limit?, engine?, page? }} -> {{ engine, query, count, results: \
         [{{ title, url, snippet }}] }}\n\
         `limit` is optional (default {default_limit}, max {max_limit}). `engine` picks the \
         search system — available: {}; omit it to use {default_engine}. Use this to discover \
         sources, then read the promising URLs. DuckDuckGo starts at page 1 \
         and returns up to 10 results per page. To continue, send next_page as page with the \
         same query, limit, and engine. Failures return the engine, error, enabled \
         alternatives, and setup_options for other supported providers. Report the failure \
         and distinguish ready-to-use alternatives from providers that need setup.",
        names.join(", ")
    )
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
            .subscribe(
                Filter::by_target(self.id.clone()),
                SubscribeOptions::default(),
            )
            .await?;
        tracing::info!(
            connector = %self.id,
            engines = %self.engine_names(),
            default_engine = %self.default_engine,
            "search ready"
        );
        loop {
            tokio::select! {
                next = cmds.next() => match next {
                    Some(env) => self.handle(&env, &ctx).await,
                    None => break,
                },
                _ = ctx.shutdown.cancelled() => break,
            }
        }
        for backend in self.engines.values() {
            backend.shutdown().await;
        }
        Ok(())
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
    page: Option<usize>,
}

impl SearchConnector {
    async fn handle(&self, env: &Envelope, ctx: &ConnectorContext) {
        if env.kind.as_str() != SEARCH {
            return; // not one of ours
        }
        let payload = env.payload_as::<Value>().cloned().unwrap_or(Value::Null);
        let outcome = self.run_search(payload).await;
        let payload = outcome.unwrap_or_else(|e| json!({ "error": e }));
        let resp = Envelope::new(
            self.id.clone(),
            EventKind::new(format!("{SEARCH}.result")),
            payload,
        )
        .with_correlation(env.id);
        if let Err(e) = ctx.publish(resp).await {
            tracing::warn!(error = %e, "search failed to publish result");
        }
    }

    async fn run_search(&self, payload: Value) -> Result<Value, String> {
        let args: SearchArgs =
            serde_json::from_value(payload).map_err(|e| format!("bad args: {e}"))?;
        let query = args.query.trim();
        if query.is_empty() {
            return Err("`query` is required".into());
        }
        let page = args.page.unwrap_or(1);
        if page == 0 {
            return Err("`page` starts at 1".into());
        }
        let limit = args
            .limit
            .unwrap_or(self.default_limit)
            .clamp(1, self.max_limit);
        let engine = args.engine.as_deref().unwrap_or(&self.default_engine);
        let backend = self.engines.get(engine).ok_or_else(|| {
            format!(
                "unknown engine `{engine}`; available: {}",
                self.engine_names()
            )
        })?;
        let result = match backend.search_page(query, limit, page).await {
            Ok(result) => result,
            Err(error) => {
                let alternatives: Vec<_> = self
                    .engines
                    .keys()
                    .filter(|name| name.as_str() != engine)
                    .collect();
                let setup_options: Vec<_> = SETUP_OPTIONS
                    .iter()
                    .filter(|option| !self.engines.contains_key(option.engine))
                    .collect();
                return Ok(json!({
                    "engine": engine,
                    "error": error,
                    "alternatives": alternatives,
                    "setup_options": setup_options,
                }));
            }
        };
        let count = result.hits.len();
        tracing::info!(engine, count, "search done");
        let mut response = json!({
            "engine": engine, "query": query, "count": count, "results": result.hits
        });
        if backend.supports_pagination() {
            response["page"] = json!(page);
            response["next_page"] = json!(result.next_page);
        }
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use octo_core::FactoryContext;
    use std::path::Path;

    /// A backend that answers with its own name, so a test can see which one ran.
    struct StubBackend(&'static str);

    #[async_trait]
    impl SearchBackend for StubBackend {
        fn name(&self) -> &str {
            self.0
        }
        async fn search(&self, query: &str, _limit: usize) -> Result<Vec<SearchHit>, String> {
            assert_ne!(self.0, "brave", "alternatives must not run implicitly");
            if self.0 == "failed" {
                return Err("fixture provider failure".into());
            }
            if query == "no matches" {
                return Ok(Vec::new());
            }
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
        let out = connector("ddg")
            .run_search(json!({ "query": "x" }))
            .await
            .unwrap();
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

    #[tokio::test]
    async fn a_failure_reports_ready_and_unconfigured_alternatives() {
        let mut engines: BTreeMap<String, Arc<dyn SearchBackend>> = BTreeMap::new();
        engines.insert("tavily".into(), Arc::new(StubBackend("failed")));
        engines.insert("brave".into(), Arc::new(StubBackend("brave")));
        let payload = json!({"query":"x"});
        let search = SearchConnector::new("search", engines.clone(), "tavily", 10, 25);
        let out = search.run_search(payload.clone()).await.unwrap();
        assert_eq!(out["engine"], "tavily");
        assert_eq!(out["error"], "fixture provider failure");
        assert_eq!(out["alternatives"], json!(["brave"]));
        let setup = out["setup_options"].as_array().unwrap();
        assert_eq!(setup.len(), 2);
        assert_eq!(setup[0]["engine"], "yandex");
        assert!(setup[0]["setup"].as_str().unwrap().contains("Folder ID"));
        engines.remove("brave");
        let search = SearchConnector::new("search", engines, "tavily", 10, 25);
        let out = search.run_search(payload).await.unwrap();
        assert_eq!(out["alternatives"], json!([]));
        let setup = out["setup_options"].as_array().unwrap();
        assert_eq!(setup.len(), 3);
        assert_eq!(setup[0]["engine"], "yandex");
        assert_eq!(setup[1]["engine"], "brave");
        assert!(setup[1]["setup"].as_str().unwrap().contains("API key"));
    }

    #[tokio::test]
    async fn an_empty_result_is_successful() {
        let out = connector("ddg")
            .run_search(json!({"query":"no matches"}))
            .await
            .unwrap();
        assert_eq!(
            out,
            json!({"engine":"ddg","query":"no matches","count":0,"results":[]})
        );
    }

    #[test]
    fn ddg_is_the_default_and_disabled_engines_need_no_credentials() {
        for manifest in [
            "[connector]",
            "[connector]\n[connector.engines.ddg]\n\
             [connector.engines.brave]\nenabled = false\n\
             [connector.engines.yandex]\nenabled = false\n",
        ] {
            let config = manifest.parse().unwrap();
            let search = factory()
                .create(
                    ConnectorId::new("search"),
                    &config,
                    FactoryContext {
                        base_dir: Path::new("."),
                    },
                )
                .unwrap();
            let catalog = search.capabilities().description.as_deref().unwrap();
            assert!(catalog.contains("available: ddg; omit it to use ddg"));
        }
    }

    #[test]
    fn an_explicit_default_still_wins_when_ddg_is_enabled() {
        let config = "[connector]\ndefault_engine = 'tavily'\n\
                      [connector.engines.ddg]\n[connector.engines.tavily]\n"
            .parse()
            .unwrap();
        let search = factory()
            .create(
                ConnectorId::new("search"),
                &config,
                FactoryContext {
                    base_dir: Path::new("."),
                },
            )
            .unwrap();
        let catalog = search.capabilities().description.as_deref().unwrap();
        assert!(catalog.contains("omit it to use tavily"));
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
