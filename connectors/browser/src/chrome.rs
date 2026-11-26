use std::{path::PathBuf, time::Duration};

use serde_json::Value;
use tokio::{sync::Mutex, time::timeout};
use tracing::warn;
use zendriver::Browser;

use crate::{
    fetch::render_tab,
    provisioning::{ChromeCache, ChromeSettings},
};

pub(super) struct Chrome {
    data_dir: PathBuf,
    executable: Option<PathBuf>,
    cache: ChromeCache,
    headless: bool,
    sandbox: bool,
    browser: Mutex<Option<Browser>>,
}

impl Chrome {
    pub(super) fn new(
        data_dir: PathBuf,
        executable: Option<PathBuf>,
        headless: bool,
        sandbox: bool,
        settings: ChromeSettings,
    ) -> Self {
        Self {
            executable,
            cache: ChromeCache::new(data_dir.join("chrome"), settings),
            data_dir,
            headless,
            sandbox,
            browser: Mutex::new(None),
        }
    }

    pub(super) async fn browser(&self) -> Result<Browser, String> {
        let mut guard = self.browser.lock().await;
        if let Some(browser) = guard.as_ref() {
            return Ok(browser.clone());
        }
        let browser = self.launch().await?;
        *guard = Some(browser.clone());
        Ok(browser)
    }

    async fn launch(&self) -> Result<Browser, String> {
        let exe = match &self.executable {
            Some(path) => path.clone(),
            None => self.cache.executable().await?,
        };
        let browser = match self.launch_binary(exe).await {
            Ok(browser) => browser,
            Err(error) => {
                let Some(previous) = self.cache.fallback().filter(|_| self.executable.is_none())
                else {
                    return Err(error);
                };
                warn!(%error, "new Chrome failed to launch; retrying previous working build");
                self.launch_binary(previous).await?
            }
        };
        if self.executable.is_none() {
            self.cache.prune().await;
        }
        Ok(browser)
    }

    async fn launch_binary(&self, exe: PathBuf) -> Result<Browser, String> {
        Browser::builder()
            .executable(exe)
            .headless(self.headless)
            .sandbox(self.sandbox)
            .user_data_dir(self.data_dir.join("profile"))
            .arg("--disable-dev-shm-usage")
            .arg("--disable-gpu")
            .launch()
            .await
            .map_err(|e| format!("chrome launch failed: {e}"))
    }

    pub(super) async fn reset(&self) {
        let browser = self.browser.lock().await.take();
        if let Some(browser) = browser {
            let _ = timeout(Duration::from_secs(5), browser.close()).await;
        }
    }

    pub(super) async fn fetch(
        &self,
        url: &str,
        want_html: bool,
        deadline: Duration,
    ) -> Result<Value, String> {
        let browser = self.browser().await?;
        let result = fetch_page(&browser, url, want_html, deadline).await;
        if let Err(error) = &result {
            if error == "open tab timeout"
                || error.starts_with("close tab:")
                || !matches!(
                    timeout(Duration::from_secs(5), browser.version()).await,
                    Ok(Ok(_))
                )
            {
                self.reset().await;
            }
        }
        result
    }

    #[cfg(test)]
    pub(super) async fn is_cached(&self) -> bool {
        self.browser.lock().await.is_some()
    }
}

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
    let extracted = timeout(deadline, render_tab(&tab, url, want_html)).await;
    timeout(Duration::from_secs(5), tab.close())
        .await
        .map_err(|_| "close tab: timeout".to_owned())?
        .map_err(|e| format!("close tab: {e}"))?;
    extracted.map_err(|_| "page timeout".to_owned())?
}
