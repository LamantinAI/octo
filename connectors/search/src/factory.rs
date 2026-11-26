//! Manifest routing and backend configuration.
use std::{collections::BTreeMap, sync::Arc, time::Duration};

use octo_browser_runtime::MoliRuntime;
use octo_core::{Connector, ConnectorFactory, ConnectorId, FactoryContext};

use super::{
    SearchBackend, SearchConnector, brave::BraveConfig, ddg::DdgBackend, tavily::TavilyBackend,
    yandex::YandexConfig,
};

/// [`ConnectorFactory`] for `type = "search"`. Register with
/// `Octo::builder().register_connector_type("search", octo_connector_search::factory())`.
#[derive(Default)]
pub struct SearchConnectorFactory {
    moli: Option<Arc<MoliRuntime>>,
}

impl SearchConnectorFactory {
    pub fn new() -> Self {
        Self::default()
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
            table
                .get("timeout_secs")
                .and_then(|v| v.as_integer())
                .unwrap_or(15)
                .max(1) as u64,
        );
        let default_limit = table
            .get("default_limit")
            .and_then(|v| v.as_integer())
            .unwrap_or(10)
            .max(1) as usize;
        let max_limit = table
            .get("max_limit")
            .and_then(|v| v.as_integer())
            .unwrap_or(25)
            .max(1) as usize;

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
                    engines.insert(
                        name.clone(),
                        build_engine(name, cfg, timeout, self.moli.as_ref())?,
                    );
                }
            }
            // Shorthand for a single-engine deployment: `backend = "ddg"` with its
            // settings inline on [connector].
            None => {
                let name = table
                    .get("backend")
                    .and_then(|v| v.as_str())
                    .unwrap_or("ddg");
                engines.insert(
                    name.to_string(),
                    build_engine(name, table, timeout, self.moli.as_ref())?,
                );
            }
        }
        if engines.is_empty() {
            return Err("search: no engines enabled — declare at least one".into());
        }

        // Prefer DuckDuckGo when enabled and no default was explicitly chosen.
        let default_engine = match table.get("default_engine").and_then(|v| v.as_str()) {
            Some(name) => name.to_string(),
            None if engines.contains_key("ddg") => "ddg".into(),
            None => engines
                .keys()
                .next()
                .cloned()
                .expect("non-empty checked above"),
        };
        if !engines.contains_key(&default_engine) {
            return Err(format!(
                "search: default_engine `{default_engine}` is not among the enabled engines ({})",
                engines.keys().cloned().collect::<Vec<_>>().join(", ")
            )
            .into());
        }
        Ok(SearchConnector::new(
            id.as_str(),
            engines,
            default_engine,
            default_limit,
            max_limit,
        ))
    }
}

/// Build one engine from its config slice. Add a `match` arm per search system —
/// this is the single place a new backend gets wired in.
fn build_engine(
    name: &str,
    cfg: &toml::Value,
    timeout: Duration,
    moli: Option<&Arc<MoliRuntime>>,
) -> Result<Arc<dyn SearchBackend>, Box<dyn std::error::Error + Send + Sync>> {
    match name {
        "ddg" => {
            // DDG's `kl` locale (region-language), e.g. "ru-ru" / "us-en"; optional.
            let region = cfg
                .get("region")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            let endpoint = cfg
                .get("webdriver_url")
                .map(|value| value.as_str().ok_or("ddg: webdriver_url must be a URL"))
                .transpose()?;
            if let Some(runtime) = moli.filter(|_| endpoint.is_none()) {
                return Ok(Arc::new(DdgBackend::with_moli(
                    runtime.register_consumer(),
                    timeout,
                    region,
                )));
            }
            Ok(Arc::new(DdgBackend::with_firefox(
                endpoint, timeout, region,
            )?))
        }
        "tavily" => Ok(Arc::new(TavilyBackend::new(timeout)?)),
        "yandex" => Ok(Arc::new(YandexConfig::from_table(cfg)?.build(timeout)?)),
        "brave" => Ok(Arc::new(BraveConfig::from_table(cfg)?.build(timeout)?)),
        other => Err(format!(
            "search: unknown engine `{other}` (have: tavily, yandex, brave, ddg)"
        )
        .into()),
    }
}

/// Convenience factory handle for registration.
pub fn factory() -> Arc<dyn ConnectorFactory> {
    Arc::new(SearchConnectorFactory::new())
}

/// Uses the application's shared Moli runtime for DuckDuckGo.
pub fn factory_with_moli(moli: Arc<MoliRuntime>) -> Arc<dyn ConnectorFactory> {
    Arc::new(SearchConnectorFactory { moli: Some(moli) })
}
