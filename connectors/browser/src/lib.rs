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
//! With no `executable` in the manifest, the `fetcher` feature downloads a managed
//! stable Chrome-for-Testing on first fetch into `data_dir` (NOT the default `~/.cache`, which
//! systemd `ProtectHome` blocks). Chrome's profile lives under `data_dir` too. Set
//! `executable` to point at a pre-provisioned Chrome instead.
//!
//! **The host process needs a writable `HOME`.** zendriver launches Chrome inheriting
//! this process's environment, and Chrome (its crashpad handler, NSS cert store, …)
//! writes under `$HOME` regardless of `--user-data-dir`. A service whose `HOME` is
//! missing or unreadable (e.g. a `--no-create-home` system user, or one blocked by
//! systemd `ProtectHome`) makes Chrome crash on startup with `SIGTRAP`. Point the
//! service's `HOME` at a writable dir (the deploy sets `Environment=HOME=…`).

mod provisioning;

use std::{path::PathBuf, sync::Arc, time::Duration};

use provisioning::{ChromeCache, ChromeSettings};

use async_trait::async_trait;
use octo_core::{
    Connector, ConnectorCapabilities, ConnectorContext, ConnectorFactory, ConnectorId, Envelope,
    EventKind, FactoryContext, Filter, OctoResult, SubscribeOptions,
};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{sync::Mutex, time::timeout};
use tracing::{info, warn};
use zendriver::Browser;

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
    chrome: ChromeCache,
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
            data_dir: data_dir.clone(),
            executable,
            chrome: ChromeCache::new(data_dir.join("chrome"), ChromeSettings::default()),
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
            None => self.chrome.executable().await?,
        };
        info!(connector = %self.id, exe = %exe.display(), "browser: launching chrome");
        let browser = match self.launch_binary(exe).await {
            Ok(browser) => browser,
            Err(error) => {
                let Some(previous) = self.chrome.fallback().filter(|_| self.executable.is_none())
                else {
                    return Err(error);
                };
                warn!(%error, "new Chrome failed to launch; retrying previous working build");
                self.launch_binary(previous).await?
            }
        };
        if self.executable.is_none() {
            self.chrome.prune().await;
        }
        Ok(browser)
    }

    async fn launch_binary(&self, exe: PathBuf) -> Result<Browser, String> {
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
        let browser = self.browser.lock().await.take();
        if let Some(browser) = browser {
            let _ = timeout(Duration::from_secs(5), browser.close()).await;
        }
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
        let deadline = args
            .timeout_secs
            .map(|s| Duration::from_secs(s.max(1)))
            .unwrap_or(self.default_timeout)
            .min(self.max_timeout);

        let browser = match self.browser().await {
            Ok(b) => b,
            Err(e) => return json!({ "status": "error", "url": url, "error": e }),
        };

        match fetch_page(&browser, url, args.html, deadline).await {
            Ok(mut page) => {
                page["status"] = json!("ok");
                page
            }
            Err(e) => {
                if e == "open tab timeout"
                    || e.starts_with("close tab:")
                    || !matches!(
                        timeout(Duration::from_secs(5), browser.version()).await,
                        Ok(Ok(_))
                    )
                {
                    self.reset().await;
                }
                json!({ "status": if e.ends_with("timeout") { "timeout" } else { "error" }, "url": url, "error": e })
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
        if status == "ok" {
            info!(url, status, "browser fetch done");
        } else {
            // Surface WHY: a failed launch/navigate would otherwise be invisible here
            // (the reason only rides in the result payload back to the model).
            let error = out.get("error").and_then(|v| v.as_str()).unwrap_or("");
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
    #[serde(default)]
    timeout_secs: Option<u64>,
}

/// Render one page in a fresh tab and pull out title / visible text / (optional) HTML.
/// The tab is always closed, even on error.
async fn fetch_page(
    browser: &Browser,
    url: &str,
    want_html: bool,
    deadline: Duration,
) -> Result<Value, String> {
    let tab = timeout(deadline, browser.new_tab())
        .await
        .map_err(|_| "open tab timeout".to_owned())?
        .map_err(|e| format!("open tab: {e}"))?;
    let extracted = timeout(deadline, async {
        tab.goto(url).await.map_err(|e| format!("navigate: {e}"))?;
        tab.wait_for_load()
            .await
            .map_err(|e| format!("load: {e}"))?;
        let title: String = tab.evaluate("document.title").await.unwrap_or_default();
        let text: String = tab
            .evaluate("document.body ? document.body.innerText : ''")
            .await
            .map_err(|e| format!("extract text: {e}"))?;
        let final_url: String = tab
            .evaluate("location.href")
            .await
            .unwrap_or_else(|_| url.to_string());
        let html = if want_html {
            tab.content().await.ok()
        } else {
            None
        };
        Ok::<_, String>((title, text, final_url, html))
    })
    .await;
    timeout(Duration::from_secs(5), tab.close())
        .await
        .map_err(|_| "close tab: timeout".to_owned())?
        .map_err(|e| format!("close tab: {e}"))?;

    let (title, text, final_url, html) = extracted.map_err(|_| "page timeout".to_owned())??;
    let mut out = json!({
        "url": url,
        "final_url": final_url,
        "title": collapse_ws(&title),
        "text": text.trim(),
        "extraction_status": if text.trim().is_empty() { "empty" } else { "text" },
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
            .subscribe(
                Filter::by_target(self.id.clone()),
                SubscribeOptions::default(),
            )
            .await?;
        // Chrome is launched lazily on the first fetch (the download, if any, happens
        // then), so startup stays fast and a browser is only paid for when used.
        info!(connector = %self.id, data_dir = %self.data_dir.display(), "browser ready (chrome launches on first fetch)");
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
        let data_dir = ctx.base_dir.join(
            table
                .get("data_dir")
                .and_then(|v| v.as_str())
                .unwrap_or("browser-data"),
        );
        let executable = table
            .get("executable")
            .and_then(|v| v.as_str())
            .map(PathBuf::from);
        let headless = table
            .get("headless")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let sandbox = table
            .get("sandbox")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let default_timeout = Duration::from_secs(
            table
                .get("timeout_secs")
                .and_then(|v| v.as_integer())
                .unwrap_or(30)
                .max(1) as u64,
        );
        let max_timeout = Duration::from_secs(
            table
                .get("max_timeout_secs")
                .and_then(|v| v.as_integer())
                .unwrap_or(90)
                .max(1) as u64,
        );
        let settings: ChromeSettings = table.clone().try_into()?;
        settings.validate()?;
        let mut connector = BrowserConnector::new(
            id.as_str(),
            data_dir,
            executable,
            headless,
            sandbox,
            default_timeout,
            max_timeout,
        );
        let cache = ChromeCache::new(connector.data_dir.join("chrome"), settings);
        Arc::get_mut(&mut connector).unwrap().chrome = cache;
        Ok(connector)
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

        let browser = conn.browser().await.unwrap();
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
            conn.browser.lock().await.is_some(),
            "page timeout must preserve healthy Chrome"
        );
        server.abort();
        let _ = browser.close().await;
    }
}
