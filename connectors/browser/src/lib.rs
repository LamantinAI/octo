//! `octo-connector-browser` — a stealth web-fetch organ (the parser half of the web
//! organ, paired with `octo-connector-search`).
//!
//! Some pages don't yield to a plain HTTP GET: they render with JavaScript, or sit
//! behind an anti-bot wall that fingerprints the client. This connector drives a real
//! headless **Chrome over CDP** via [`zendriver`] (stealth on by default) and returns
//! the rendered result as clean text — never raw bytes through the model.
//!
//! Command (dispatch to this connector's id):
//! - `browser.fetch { url, html?, timeout_secs? }` → `{ url, final_url, title, text,
//!   html?, status }`. `text` is the page's visible text (`document.body.innerText`);
//!   `html` (the serialized DOM) is included only when `html: true` is asked, since it
//!   is large. `status` is `"ok"` / `"timeout"` / `"error"`.
//!
//! The agent orchestrates: search for URLs, then `browser.fetch` the promising ones —
//! reserve the browser for pages a cheap fetch can't handle, since a real Chrome is
//! heavy. One Chrome is launched lazily and reused across fetches (a fresh tab each
//! time); it relaunches if it dies.
//!
//! # Runtime: a Chrome binary
//!
//! With no `executable` in the manifest, the `fetcher` feature downloads a pinned
//! Chrome-for-Testing on first fetch into `data_dir` (NOT the default `~/.cache`, which
//! systemd `ProtectHome` blocks). Chrome's profile lives under `data_dir` too. Set
//! `executable` to point at a pre-provisioned Chrome instead.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use octo_core::{
    Connector, ConnectorCapabilities, ConnectorContext, ConnectorFactory, ConnectorId, Envelope,
    EventKind, FactoryContext, Filter, OctoResult, SubscribeOptions,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::Mutex;
use zendriver::{Browser, Fetcher};

const FETCH: &str = "browser.fetch";

const CATALOG: &str = "Fetch and RENDER a web page in a real (headless, stealth) browser — \
for pages that need JavaScript or sit behind an anti-bot wall, where a plain fetch returns \
nothing. Dispatch a command envelope to this connector's id:
- browser.fetch { url, html?, timeout_secs? } -> { url, final_url, title, text, html?, status }
`text` is the page's visible text; pass `html: true` only if you need the raw DOM (it is \
large). Use this AFTER search, on the specific URLs worth reading — a browser is heavy, so \
don't fetch pages a snippet already answers.";

/// Chrome launch settings + the reused browser handle.
pub struct BrowserConnector {
    id: ConnectorId,
    capabilities: ConnectorCapabilities,
    /// Writable dir for the downloaded Chrome cache + Chrome's profile.
    data_dir: PathBuf,
    /// Explicit Chrome binary; `None` → download via the fetcher into `data_dir`.
    executable: Option<PathBuf>,
    headless: bool,
    /// `false` passes `--no-sandbox` (Chrome's own sandbox needs privileges the service
    /// doesn't have; the OS/systemd is the outer boundary).
    sandbox: bool,
    default_timeout: Duration,
    max_timeout: Duration,
    /// The launched Chrome, reused across fetches; `None` until the first fetch (or
    /// after it died).
    browser: Mutex<Option<Browser>>,
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
        let capabilities = ConnectorCapabilities::bidirectional()
            .with_accept_kinds([EventKind::from_static(FETCH)])
            .with_description(CATALOG);
        Arc::new(Self {
            id: ConnectorId::new(id),
            capabilities,
            data_dir,
            executable,
            headless,
            sandbox,
            default_timeout,
            max_timeout: max_timeout.max(default_timeout),
            browser: Mutex::new(None),
        })
    }

    /// The reused browser, launching one on first use (or after a death). Cheap to
    /// clone (an `Arc` handle).
    async fn browser(&self) -> Result<Browser, String> {
        let mut guard = self.browser.lock().await;
        if let Some(b) = guard.as_ref() {
            return Ok(b.clone());
        }
        let browser = self.launch().await?;
        *guard = Some(browser.clone());
        Ok(browser)
    }

    async fn launch(&self) -> Result<Browser, String> {
        let exe = match &self.executable {
            Some(path) => path.clone(),
            None => Fetcher::new()
                .cache_dir(self.data_dir.join("chrome"))
                .ensure_chrome()
                .await
                .map_err(|e| format!("chrome download failed: {e}"))?,
        };
        tracing::info!(connector = %self.id, exe = %exe.display(), "browser: launching chrome");
        Browser::builder()
            .executable(exe)
            .headless(self.headless)
            .sandbox(self.sandbox)
            .user_data_dir(self.data_dir.join("profile"))
            // /dev/shm is tiny under systemd PrivateTmp; without this Chrome crashes.
            .arg("--disable-dev-shm-usage")
            .arg("--disable-gpu")
            .launch()
            .await
            .map_err(|e| format!("chrome launch failed: {e}"))
    }

    /// Drop the cached browser so the next fetch relaunches (called after a fetch error,
    /// which usually means Chrome died).
    async fn reset(&self) {
        *self.browser.lock().await = None;
    }

    async fn run_fetch(&self, params: Value) -> Value {
        let args: FetchArgs = match serde_json::from_value(params) {
            Ok(a) => a,
            Err(e) => return json!({ "status": "error", "error": format!("bad args: {e}") }),
        };
        let url = args.url.trim();
        if url.is_empty() {
            return json!({ "status": "error", "error": "`url` is required" });
        }
        let timeout = args
            .timeout_secs
            .map(|s| Duration::from_secs(s.max(1)))
            .unwrap_or(self.default_timeout)
            .min(self.max_timeout);

        let browser = match self.browser().await {
            Ok(b) => b,
            Err(e) => return json!({ "status": "error", "url": url, "error": e }),
        };

        match tokio::time::timeout(timeout, fetch_page(&browser, url, args.html)).await {
            Ok(Ok(mut page)) => {
                page["status"] = json!("ok");
                page
            }
            Ok(Err(e)) => {
                self.reset().await; // a failed fetch usually means Chrome died — relaunch next time
                json!({ "status": "error", "url": url, "error": e })
            }
            Err(_) => {
                json!({ "status": "timeout", "url": url, "error": format!("no load in {}s", timeout.as_secs()) })
            }
        }
    }

    async fn handle(&self, env: &Envelope, ctx: &ConnectorContext) {
        if env.kind.as_str() != FETCH {
            return;
        }
        let params = env.payload_as::<Value>().cloned().unwrap_or(Value::Null);
        let out = self.run_fetch(params).await;
        let url = out.get("url").and_then(|v| v.as_str()).unwrap_or("");
        let status = out.get("status").and_then(|v| v.as_str()).unwrap_or("");
        tracing::info!(url, status, "browser fetch done");
        let resp = Envelope::new(self.id.clone(), EventKind::new(format!("{FETCH}.result")), out)
            .with_correlation(env.id);
        if let Err(e) = ctx.publish(resp).await {
            tracing::warn!(error = %e, "browser failed to publish result");
        }
    }
}

#[derive(Deserialize)]
struct FetchArgs {
    url: String,
    #[serde(default)]
    html: bool,
    #[serde(default)]
    timeout_secs: Option<u64>,
}

/// Render one page in a fresh tab and pull out title / visible text / (optional) HTML.
/// The tab is always closed, even on error.
async fn fetch_page(browser: &Browser, url: &str, want_html: bool) -> Result<Value, String> {
    let tab = browser.new_tab().await.map_err(|e| format!("open tab: {e}"))?;
    let extracted = async {
        tab.goto(url).await.map_err(|e| format!("navigate: {e}"))?;
        let title: String = tab.evaluate("document.title").await.unwrap_or_default();
        let text: String = tab
            .evaluate("document.body ? document.body.innerText : ''")
            .await
            .unwrap_or_default();
        let final_url: String =
            tab.evaluate("location.href").await.unwrap_or_else(|_| url.to_string());
        let html = if want_html { tab.content().await.ok() } else { None };
        Ok::<_, String>((title, text, final_url, html))
    }
    .await;
    let _ = tab.close().await; // best-effort; don't mask the real error

    let (title, text, final_url, html) = extracted?;
    let mut out = json!({
        "url": url,
        "final_url": final_url,
        "title": collapse_ws(&title),
        "text": text.trim(),
    });
    if let Some(html) = html {
        out["html"] = json!(html);
    }
    Ok(out)
}

fn collapse_ws(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
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
            .subscribe(Filter::by_target(self.id.clone()), SubscribeOptions::default())
            .await?;
        // Chrome is launched lazily on the first fetch (the download, if any, happens
        // then), so startup stays fast and a browser is only paid for when used.
        tracing::info!(connector = %self.id, data_dir = %self.data_dir.display(), "browser ready (chrome launches on first fetch)");
        loop {
            tokio::select! {
                next = cmds.next() => match next {
                    Some(env) => self.handle(&env, &ctx).await,
                    None => return Ok(()),
                },
                _ = ctx.shutdown.cancelled() => {
                    // Best-effort: close Chrome on shutdown so it doesn't linger.
                    if let Some(browser) = self.browser.lock().await.take() {
                        let _ = browser.close().await;
                    }
                    return Ok(());
                }
            }
        }
    }
}

/// [`ConnectorFactory`] for `type = "browser"`.
pub struct BrowserConnectorFactory;

impl BrowserConnectorFactory {
    pub fn new() -> Self {
        Self
    }
}

impl Default for BrowserConnectorFactory {
    fn default() -> Self {
        Self::new()
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
        // Writable dir for the Chrome download + profile, relative to the manifest.
        let data_dir = ctx
            .base_dir
            .join(table.get("data_dir").and_then(|v| v.as_str()).unwrap_or("browser-data"));
        let executable = table.get("executable").and_then(|v| v.as_str()).map(PathBuf::from);
        let headless = table.get("headless").and_then(|v| v.as_bool()).unwrap_or(true);
        let sandbox = table.get("sandbox").and_then(|v| v.as_bool()).unwrap_or(false);
        let default_timeout = Duration::from_secs(
            table.get("timeout_secs").and_then(|v| v.as_integer()).unwrap_or(30).max(1) as u64,
        );
        let max_timeout = Duration::from_secs(
            table.get("max_timeout_secs").and_then(|v| v.as_integer()).unwrap_or(90).max(1) as u64,
        );
        Ok(BrowserConnector::new(
            id.as_str(),
            data_dir,
            executable,
            headless,
            sandbox,
            default_timeout,
            max_timeout,
        ))
    }
}

/// Convenience factory handle for registration.
pub fn factory() -> Arc<dyn ConnectorFactory> {
    Arc::new(BrowserConnectorFactory::new())
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let out = conn.run_fetch(json!({ "url": "https://example.com" })).await;
        println!("{}", serde_json::to_string_pretty(&out).unwrap());
        assert_eq!(out["status"], "ok", "fetch should succeed");
        assert!(out["title"].as_str().unwrap_or("").contains("Example"), "title: {out}");
        assert!(
            out["text"].as_str().unwrap_or("").contains("Example Domain"),
            "text should carry the page body"
        );
    }
}
