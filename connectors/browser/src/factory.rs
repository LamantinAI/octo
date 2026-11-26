use std::{path::PathBuf, sync::Arc, time::Duration};

use octo_core::{Connector, ConnectorFactory, ConnectorId, FactoryContext};

use crate::{BrowserConnector, Engine, MoliRuntime, chrome::Chrome, provisioning::ChromeSettings};

/// Factory for the browser connector.
#[derive(Default)]
pub struct BrowserConnectorFactory {
    moli: Option<Arc<MoliRuntime>>,
}

impl BrowserConnectorFactory {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ConnectorFactory for BrowserConnectorFactory {
    fn type_name(&self) -> &str {
        "browser"
    }

    fn create(
        &self,
        id: ConnectorId,
        config: &toml::Value,
        ctx: FactoryContext<'_>,
    ) -> Result<Arc<dyn Connector>, Box<dyn std::error::Error + Send + Sync>> {
        let table = config
            .get("connector")
            .ok_or("browser: manifest has no [connector] table")?;
        let default_timeout = seconds(table, "timeout_secs", 30);
        let max_timeout = seconds(table, "max_timeout_secs", 90);
        let engine = match engine_name(table, self.moli.is_some())? {
            "moli" => Engine::Moli(self.moli.clone().unwrap_or_default().register_consumer()),
            "chrome" => {
                let settings: ChromeSettings = table.clone().try_into()?;
                settings.validate()?;
                let data_dir = ctx.base_dir.join(
                    table
                        .get("data_dir")
                        .and_then(toml::Value::as_str)
                        .unwrap_or("browser-data"),
                );
                let executable = table
                    .get("executable")
                    .and_then(toml::Value::as_str)
                    .map(PathBuf::from);
                let headless = table
                    .get("headless")
                    .and_then(toml::Value::as_bool)
                    .unwrap_or(true);
                let sandbox = table
                    .get("sandbox")
                    .and_then(toml::Value::as_bool)
                    .unwrap_or(false);
                Engine::Chrome(Box::new(Chrome::new(
                    data_dir, executable, headless, sandbox, settings,
                )))
            }
            name => return Err(format!("browser: unknown engine `{name}`").into()),
        };
        Ok(BrowserConnector::with_engine(
            id.as_str(),
            engine,
            default_timeout,
            max_timeout,
        ))
    }
}

fn seconds(table: &toml::Value, name: &str, fallback: i64) -> Duration {
    let value = table
        .get(name)
        .and_then(toml::Value::as_integer)
        .unwrap_or(fallback);
    Duration::from_secs(value.max(1) as u64)
}

pub(super) fn engine_name(table: &toml::Value, has_moli: bool) -> Result<&str, &'static str> {
    if let Some(engine) = table.get("engine") {
        return engine.as_str().ok_or("browser: engine must be a string");
    }
    let explicit_chrome = table.get("executable").is_some()
        || table.get("chrome_version").is_some()
        || table.get("headless").and_then(toml::Value::as_bool) == Some(false)
        || table.get("sandbox").and_then(toml::Value::as_bool) == Some(true);
    Ok(if has_moli && !explicit_chrome {
        "moli"
    } else {
        "chrome"
    })
}

pub fn factory() -> Arc<dyn ConnectorFactory> {
    Arc::new(BrowserConnectorFactory::new())
}

/// Selects Moli for manifests without an explicit engine or Chrome override.
pub fn factory_with_moli(moli: Arc<MoliRuntime>) -> Arc<dyn ConnectorFactory> {
    Arc::new(BrowserConnectorFactory { moli: Some(moli) })
}
