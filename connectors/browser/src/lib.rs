//! Read a JavaScript-rendered page through Moli or Chrome.
mod chrome;
mod factory;
mod fetch;
mod provisioning;

use std::{path::PathBuf, sync::Arc, time::Duration};

use async_trait::async_trait;
use octo_core::{
    Connector, ConnectorCapabilities, ConnectorContext, ConnectorId, Envelope, EventKind, Filter,
    OctoResult, SubscribeOptions,
};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::time::timeout;
use tracing::{info, warn};

use self::{chrome::Chrome, fetch::render_tab, provisioning::ChromeSettings};

pub use factory::{BrowserConnectorFactory, factory, factory_with_moli};
pub use octo_browser_runtime::{MoliRuntime, MoliSettings};

const FETCH: &str = "browser.fetch";
const CATALOG: &str = "Read a web page after running its JavaScript. Dispatch to this connector:
- browser.fetch { url, html?, timeout_secs? } -> { url, final_url, title, text, html?, status, extraction_status }
Use html: true when you need the serialized DOM. Use the specific URLs worth reading.";

enum Engine {
    Chrome(Box<Chrome>),
    Moli(Arc<MoliRuntime>),
}

impl Engine {
    fn name(&self) -> &'static str {
        match self {
            Self::Chrome(_) => "chrome",
            Self::Moli(_) => "moli",
        }
    }

    async fn fetch(&self, url: &str, html: bool, deadline: Duration) -> Result<Value, String> {
        match self {
            Self::Chrome(chrome) => chrome.fetch(url, html, deadline).await,
            Self::Moli(moli) => timeout(deadline, async {
                let page = moli.new_page().await?;
                render_tab(page.tab(), url, html).await
            })
            .await
            .map_err(|_| "page timeout".to_owned())?,
        }
    }
}

/// Web fetch backed by either a shared Moli runtime or an explicitly configured Chrome.
pub struct BrowserConnector {
    id: ConnectorId,
    capabilities: ConnectorCapabilities,
    engine: Engine,
    default_timeout: Duration,
    max_timeout: Duration,
}

impl BrowserConnector {
    pub fn new(
        id: impl Into<String>,
        data_dir: PathBuf,
        executable: Option<PathBuf>,
        headless: bool,
        sandbox: bool,
        default_timeout: Duration,
        max_timeout: Duration,
    ) -> Arc<Self> {
        let chrome = Chrome::new(
            data_dir,
            executable,
            headless,
            sandbox,
            ChromeSettings::default(),
        );
        Self::with_engine(
            id,
            Engine::Chrome(Box::new(chrome)),
            default_timeout,
            max_timeout,
        )
    }

    fn with_engine(
        id: impl Into<String>,
        engine: Engine,
        default_timeout: Duration,
        max_timeout: Duration,
    ) -> Arc<Self> {
        Arc::new(Self {
            id: ConnectorId::new(id),
            capabilities: ConnectorCapabilities::bidirectional()
                .with_accept_kinds([EventKind::from_static(FETCH)])
                .with_description(CATALOG),
            engine,
            default_timeout,
            max_timeout: max_timeout.max(default_timeout),
        })
    }

    async fn run_fetch(&self, params: Value) -> Value {
        let args: FetchArgs = match serde_json::from_value(params) {
            Ok(args) => args,
            Err(e) => return json!({ "status": "error", "error": format!("bad args: {e}") }),
        };
        let url = args.url.trim();
        if url.is_empty() {
            return json!({ "status": "error", "error": "`url` is required" });
        }
        let deadline = args
            .timeout_secs
            .map(|seconds| Duration::from_secs(seconds.max(1)))
            .unwrap_or(self.default_timeout)
            .min(self.max_timeout);
        match self.engine.fetch(url, args.html, deadline).await {
            Ok(mut page) => {
                page["status"] = json!("ok");
                page
            }
            Err(error) => {
                let status = if error.ends_with("timeout") {
                    "timeout"
                } else {
                    "error"
                };
                json!({ "status": status, "url": url, "error": error })
            }
        }
    }

    async fn handle(&self, env: &Envelope, ctx: &ConnectorContext) {
        if env.kind.as_str() != FETCH {
            return;
        }
        let params = env.payload_as::<Value>().cloned().unwrap_or(Value::Null);
        let out = self.run_fetch(params).await;
        let url = out.get("url").and_then(Value::as_str).unwrap_or("");
        let status = out.get("status").and_then(Value::as_str).unwrap_or("");
        if status == "ok" {
            info!(url, status, "browser fetch done");
        } else {
            let error = out.get("error").and_then(Value::as_str).unwrap_or("");
            warn!(url, status, error, "browser fetch failed");
        }
        let resp = Envelope::new(
            self.id.clone(),
            EventKind::new(format!("{FETCH}.result")),
            out,
        )
        .with_correlation(env.id);
        if let Err(e) = ctx.publish(resp).await {
            warn!(error = %e, "browser failed to publish result");
        }
    }
}

#[derive(Deserialize)]
struct FetchArgs {
    url: String,
    #[serde(default)]
    html: bool,
    timeout_secs: Option<u64>,
}

#[async_trait]
impl Connector for BrowserConnector {
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
        info!(connector = %self.id, engine = self.engine.name(), "browser ready");
        loop {
            tokio::select! {
                next = cmds.next() => match next {
                    Some(env) => self.handle(&env, &ctx).await,
                    None => break,
                },
                _ = ctx.shutdown.cancelled() => break,
            }
        }
        if let Engine::Chrome(chrome) = &self.engine {
            chrome.reset().await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::factory::engine_name;

    #[test]
    fn a_mounted_manifest_with_old_cache_tuning_adopts_moli() {
        let table = "data_dir = 'browser-data'\nchrome_keep_builds = 2\nheadless = true"
            .parse()
            .unwrap();
        assert_eq!(engine_name(&table, true), Ok("moli"));
        assert_eq!(engine_name(&table, false), Ok("chrome"));
    }

    #[test]
    fn explicit_browser_selection_wins() {
        for (config, expected) in [
            ("chrome_version = '153.0.8010.12'", Ok("chrome")),
            ("executable = 'chromium'\nengine = 'moli'", Ok("moli")),
            ("engine = 123", Err("browser: engine must be a string")),
        ] {
            let table = config.parse().unwrap();
            assert_eq!(engine_name(&table, true), expected);
        }
    }

    /// End-to-end: download+launch Chrome and render a page. Network + a Chrome
    /// download (~150 MB, first run only), so ignored by default. Run with
    /// `cargo test -p octo-connector-browser -- --ignored --nocapture`.
    #[tokio::test]
    #[ignore = "launches Chrome (downloads ~150MB on first run)"]
    async fn live_fetch_renders_a_page() {
        let dir = std::env::temp_dir().join("octo-browser-test");
        let conn = BrowserConnector::new(
            "browser",
            dir,
            None,
            true,
            false,
            Duration::from_secs(45),
            Duration::from_secs(90),
        );
        let out = conn
            .run_fetch(json!({ "url": "data:text/html,<title>Browser fixture</title><body><script>document.body.append('rendered evidence')</script>" }))
            .await;
        println!("{}", serde_json::to_string_pretty(&out).unwrap());
        assert_eq!(out["status"], "ok", "fetch should succeed");
        assert!(
            out["title"]
                .as_str()
                .unwrap_or("")
                .contains("Browser fixture"),
            "title: {out}"
        );
        assert!(
            out["text"]
                .as_str()
                .unwrap_or("")
                .contains("rendered evidence"),
            "text should carry the page body"
        );

        let Engine::Chrome(chrome) = &conn.engine else {
            panic!("expected Chrome")
        };
        let browser = chrome.browser().await.unwrap();
        let before: Vec<_> = browser
            .tabs()
            .await
            .iter()
            .map(|t| t.target_id().to_owned())
            .collect();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(30)).await;
        });
        let timed = conn.run_fetch(json!({"url":url,"timeout_secs":1})).await;
        assert_eq!(timed["status"], "timeout", "{timed}");
        timeout(Duration::from_secs(3), async {
            loop {
                if browser
                    .tabs()
                    .await
                    .iter()
                    .all(|t| before.iter().any(|id| id == t.target_id()))
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("timeout must close its new tab");
        assert!(
            chrome.is_cached().await,
            "page timeout must preserve healthy Chrome"
        );
        server.abort();
        let _ = browser.close().await;
    }
}
