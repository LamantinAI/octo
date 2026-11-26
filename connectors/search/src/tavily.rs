//! Tavily's documented keyless web search.
use std::time::Duration;

use async_trait::async_trait;
use reqwest::{Client, Url};
use serde::Deserialize;
use serde_json::json;

use super::{
    SearchBackend, SearchHit,
    api::{client, hit, request_json},
};

const ENDPOINT: &str = "https://api.tavily.com/search";
const PROVIDER: &str = "tavily";
const MAX_RESULTS: usize = 20;

pub(super) struct TavilyBackend {
    client: Client,
    endpoint: Url,
}

impl TavilyBackend {
    pub(super) fn new(timeout: Duration) -> Result<Self, String> {
        Self::at(timeout, ENDPOINT)
    }

    fn at(timeout: Duration, endpoint: &str) -> Result<Self, String> {
        Ok(Self {
            client: client(PROVIDER, timeout)?,
            endpoint: Url::parse(endpoint).map_err(|_| "tavily: invalid API endpoint")?,
        })
    }
}

#[derive(Deserialize)]
struct Response {
    results: Vec<ResultItem>,
}

#[derive(Deserialize)]
struct ResultItem {
    url: String,
    #[serde(default)]
    title: String,
    #[serde(default)]
    content: String,
}

#[async_trait]
impl SearchBackend for TavilyBackend {
    fn name(&self) -> &str {
        PROVIDER
    }

    async fn search(&self, query: &str, limit: usize) -> Result<Vec<SearchHit>, String> {
        let limit = limit.clamp(1, MAX_RESULTS);
        let request = self
            .client
            .post(self.endpoint.clone())
            .header("x-tavily-access-mode", "keyless")
            .json(
                &json!({"query":query,"max_results":limit,"search_depth":"basic",
                "include_answer":false,"include_raw_content":false}),
            );
        let output: Response = request_json(PROVIDER, request, "keyless access refused").await?;
        Ok(output
            .results
            .into_iter()
            .filter_map(|r| hit(r.url, r.title, r.content))
            .take(limit)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::http::StatusCode;
    use serde_json::json;

    use super::{SearchBackend, TavilyBackend};
    use crate::api::tests::Mock;

    #[tokio::test]
    async fn keyless_search_sends_no_credential_and_limits_returned_results() {
        let mock = Mock::start(
            "/search",
            StatusCode::OK,
            json!({"results":[
                {"url":"javascript:noop()","title":"Invalid","content":"Ignore"},
                {"url":"https://example.com/","title":"Example","content":"A grounded excerpt"}
            ]}),
            |headers, body| {
                assert_eq!(headers["x-tavily-access-mode"], "keyless");
                assert!(!headers.contains_key("authorization"));
                assert_eq!(body["query"], "fixture query");
                assert_eq!(body["max_results"], 20);
                assert_eq!(body["include_raw_content"], false);
            },
        )
        .await;
        let backend = TavilyBackend::at(Duration::from_secs(3), &mock.endpoint).unwrap();
        let hits = backend.search("fixture query", 200).await.unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].snippet, "A grounded excerpt");
    }

    #[tokio::test]
    async fn keyless_quota_error_is_distinct_from_an_empty_result() {
        let mock = Mock::start(
            "/search",
            StatusCode::TOO_MANY_REQUESTS,
            json!({"message":"private diagnostic"}),
            |_, _| {},
        )
        .await;
        let backend = TavilyBackend::at(Duration::from_secs(3), &mock.endpoint).unwrap();
        assert_eq!(
            backend.search("fixture query", 1).await.unwrap_err(),
            "tavily: HTTP 429 (rate limit or quota exceeded)"
        );
    }
}
