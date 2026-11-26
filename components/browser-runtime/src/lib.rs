//! Shared Moli process. Each page owns its own browser context.
mod process;
mod settings;

use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering::Relaxed},
    },
    time::Duration,
};

use tokio::{sync::Mutex, time::timeout};
use zendriver::{Browser, BrowserContext, Tab, stealth::StealthProfile};

use self::process::MoliProcess;

pub use self::settings::MoliSettings;

const HEALTH_TIMEOUT: Duration = Duration::from_secs(2);

/// A process shared by all consumers in a single application instance.
pub struct MoliRuntime {
    settings: MoliSettings,
    configured: AtomicBool,
    running: Mutex<Option<Running>>,
}

struct Running {
    browser: Browser,
    process: MoliProcess,
    identity: Arc<()>,
}

impl Default for MoliRuntime {
    fn default() -> Self {
        Self::new("moli")
    }
}

impl MoliRuntime {
    pub fn new(executable: impl Into<PathBuf>) -> Self {
        Self::from_settings(MoliSettings {
            executable: executable.into(),
            ..MoliSettings::default()
        })
    }

    pub fn with_settings(settings: MoliSettings) -> Result<Self, String> {
        settings.validate()?;
        Ok(Self::from_settings(settings))
    }

    fn from_settings(settings: MoliSettings) -> Self {
        Self {
            settings,
            configured: AtomicBool::new(false),
            running: Mutex::new(None),
        }
    }

    /// Register an active connector that uses this process.
    pub fn register_consumer(self: &Arc<Self>) -> Arc<Self> {
        self.configured.store(true, Relaxed);
        self.clone()
    }

    /// Start the shared process at application startup if a connector uses it.
    pub async fn start_if_configured(&self) -> Result<(), String> {
        if self.configured.load(Relaxed) {
            self.browser().await?;
        }
        Ok(())
    }

    async fn browser(&self) -> Result<(Browser, Arc<()>), String> {
        let mut running = self.running.lock().await;
        if let Some(active) = running.as_mut() {
            // A busy browser can miss the probe deadline while another page is
            // loading. Only a dead child or a failed CDP call warrants replacement.
            if active.process.is_running()?
                && !matches!(
                    timeout(HEALTH_TIMEOUT, active.browser.version()).await,
                    Ok(Err(_))
                )
            {
                return Ok((active.browser.clone(), active.identity.clone()));
            }
        }
        *running = None;
        let deadline = Duration::from_secs(self.settings.startup_timeout_secs);
        let active = timeout(deadline, self.launch())
            .await
            .map_err(|_| "moli: browser startup timeout")??;
        let connection = (active.browser.clone(), active.identity.clone());
        *running = Some(active);
        Ok(connection)
    }

    async fn launch(&self) -> Result<Running, String> {
        let (process, endpoint) = MoliProcess::launch(&self.settings).await?;
        let browser = Browser::builder()
            .stealth(StealthProfile::off())
            .connect(format!("http://{endpoint}"))
            .await
            .map_err(|e| format!("moli: connect: {e}"))?;
        Ok(Running {
            browser,
            process,
            identity: Arc::new(()),
        })
    }

    /// Opens a tab with independent cookies and web storage.
    pub async fn new_page(&self) -> Result<MoliPage, String> {
        let (browser, identity) = self.browser().await?;
        let context = browser
            .create_browser_context()
            .await
            .map_err(|e| format!("moli: create browser context: {e}"))?;
        let tab = context
            .new_tab()
            .await
            .map_err(|e| format!("moli: open tab: {e}"))?;
        Ok(MoliPage {
            tab,
            _context: context,
            identity,
        })
    }

    /// Checks whether a retained page belongs to the active process.
    pub async fn is_current(&self, page: &MoliPage) -> bool {
        self.running.lock().await.as_mut().is_some_and(|active| {
            Arc::ptr_eq(&active.identity, &page.identity)
                && active.process.is_running().unwrap_or(false)
        })
    }
}

/// Dropping the page disposes its browser context and tabs.
pub struct MoliPage {
    tab: Tab,
    _context: BrowserContext,
    identity: Arc<()>,
}

impl MoliPage {
    pub fn tab(&self) -> &Tab {
        &self.tab
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{MoliRuntime, process::listening_on};

    #[test]
    fn startup_requires_a_bound_loopback_port() {
        let address = listening_on("INFO protocol server listening addr=127.0.0.1:32400").unwrap();
        assert_eq!(address.port(), 32400);
        assert!(listening_on("INFO protocol server listening addr=127.0.0.1:0").is_none());
        assert!(listening_on("INFO protocol server listening addr=0.0.0.0:32400").is_none());
        assert!(listening_on("INFO protocol server starting").is_none());
    }

    #[tokio::test]
    async fn startup_runs_only_for_a_registered_connector() {
        let runtime = Arc::new(MoliRuntime::new("missing-moli-fixture"));
        assert_eq!(runtime.start_if_configured().await, Ok(()));
        let _connector = runtime.register_consumer();
        assert!(
            runtime
                .start_if_configured()
                .await
                .unwrap_err()
                .contains("executable not found")
        );
    }
}
