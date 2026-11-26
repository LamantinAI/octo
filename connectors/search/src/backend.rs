use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// One search result.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SearchHit {
    pub title: String,
    pub url: String,
    pub snippet: String,
}

/// A result page and its continuation, if the provider has more hits.
pub struct SearchPage {
    pub hits: Vec<SearchHit>,
    pub next_page: Option<usize>,
}

/// A swappable source behind the shared web-search command.
#[async_trait]
pub trait SearchBackend: Send + Sync {
    /// Short backend name for logs and telemetry.
    fn name(&self) -> &str;

    /// Run a query and return up to `limit` hits, best first.
    async fn search(&self, query: &str, limit: usize) -> Result<Vec<SearchHit>, String>;

    fn supports_pagination(&self) -> bool {
        false
    }

    async fn shutdown(&self) {}

    async fn search_page(
        &self,
        query: &str,
        limit: usize,
        page: usize,
    ) -> Result<SearchPage, String> {
        if page != 1 {
            return Err(format!("{}: pagination is unavailable", self.name()));
        }
        Ok(SearchPage {
            hits: self.search(query, limit).await?,
            next_page: None,
        })
    }
}
