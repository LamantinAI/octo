use std::sync::Arc;

use octo_browser_runtime::{MoliPage, MoliRuntime};
use reqwest::Url;
use serde::de::DeserializeOwned;
use serde_json::json;

use super::webdriver::{Session as FirefoxSession, WebDriver};

pub(super) enum Driver {
    Firefox(WebDriver),
    Moli(Arc<MoliRuntime>),
}

impl Driver {
    pub(super) async fn start(&self) -> Result<Session, String> {
        match self {
            Self::Firefox(driver) => driver.start().await.map(Session::Firefox),
            Self::Moli(runtime) => runtime.new_page().await.map(Session::Moli),
        }
    }

    pub(super) async fn is_current(&self, session: &Session) -> bool {
        match (self, session) {
            (Self::Moli(runtime), Session::Moli(page)) => runtime.is_current(page).await,
            (Self::Firefox(_), Session::Firefox(_)) => true,
            _ => false,
        }
    }
}

pub(super) enum Session {
    Firefox(FirefoxSession),
    Moli(MoliPage),
}

impl Session {
    pub(super) async fn navigate(&self, url: &Url) -> Result<(), String> {
        match self {
            Self::Firefox(session) => session.navigate(url).await,
            Self::Moli(page) => {
                page.tab()
                    .goto(url.as_str())
                    .await
                    .map_err(|e| format!("ddg: navigate: {e}"))?;
                page.tab()
                    .wait_for_load()
                    .await
                    .map_err(|e| format!("ddg: wait for page: {e}"))
            }
        }
    }

    pub(super) async fn evaluate<T: DeserializeOwned>(
        &self,
        script: &str,
        offset: usize,
        limit: usize,
    ) -> Result<T, String> {
        match self {
            Self::Firefox(session) => session.evaluate(script, json!([offset, limit])).await,
            Self::Moli(page) => page
                .tab()
                .evaluate(format!("(function() {{ {script} }})({offset}, {limit})"))
                .await
                .map_err(|e| format!("ddg: extract search results: {e}")),
        }
    }

    pub(super) async fn click(&self, selector: &str) -> Result<(), String> {
        match self {
            Self::Firefox(session) => session.click(selector).await,
            Self::Moli(page) => {
                let clicked: bool = page.tab().evaluate(format!(
                    "(() => {{ const button = document.querySelector({}); \
                     if (!button || button.disabled) return false; button.click(); return true; }})()",
                    json!(selector),
                )).await.map_err(|e| format!("ddg: next page: {e}"))?;
                if clicked {
                    Ok(())
                } else {
                    Err("ddg: next-page button unavailable".into())
                }
            }
        }
    }

    pub(super) async fn close(self) {
        if let Self::Firefox(session) = self {
            session.close().await;
        }
    }
}
