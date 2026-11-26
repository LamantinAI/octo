//! Brave Search's LLM Context endpoint returns extracted excerpts with sources.
use std::time::Duration;

use async_trait::async_trait;
use reqwest::{Client, Url, header::HeaderValue};
use serde::Deserialize;
use serde_json::json;

use super::{
    SearchBackend, SearchHit,
    api::{client, hit, request_json, required_env},
};

const ENDPOINT: &str = "https://api.search.brave.com/res/v1/llm/context";
const PROVIDER: &str = "brave";
const MAX_RESULTS: usize = 50;

#[derive(Deserialize)]
#[serde(default)]
pub(super) struct BraveConfig {
    api_key_env: String,
    country: Option<String>,
    search_lang: Option<String>,
}

impl Default for BraveConfig {
    fn default() -> Self {
        Self {
            api_key_env: "BRAVE_API_KEY".into(),
            country: None,
            search_lang: None,
        }
    }
}

impl BraveConfig {
    pub(super) fn from_table(table: &toml::Value) -> Result<Self, String> {
        table
            .clone()
            .try_into()
            .map_err(|_| "brave: invalid engine settings".into())
    }

    pub(super) fn build(self, timeout: Duration) -> Result<BraveBackend, String> {
        let key = required_env(PROVIDER, &self.api_key_env)?;
        self.at(timeout, ENDPOINT, &key)
    }

    fn at(self, timeout: Duration, endpoint: &str, key: &str) -> Result<BraveBackend, String> {
        let mut key = HeaderValue::from_str(key).map_err(|_| "brave: invalid API key header")?;
        key.set_sensitive(true);
        Ok(BraveBackend {
            client: client(PROVIDER, timeout)?,
            endpoint: Url::parse(endpoint).map_err(|_| "brave: invalid API endpoint")?,
            key,
            country: self.country,
            search_lang: self.search_lang,
        })
    }
}

pub(super) struct BraveBackend {
    client: Client,
    endpoint: Url,
    key: HeaderValue,
    country: Option<String>,
    search_lang: Option<String>,
}

#[derive(Deserialize)]
struct Response {
    grounding: Grounding,
}

#[derive(Deserialize)]
struct Grounding {
    #[serde(default)]
    generic: Vec<Source>,
}

#[derive(Deserialize)]
struct Source {
    url: String,
    #[serde(default)]
    title: String,
    #[serde(default)]
    snippets: Vec<String>,
}

#[async_trait]
impl SearchBackend for BraveBackend {
    fn name(&self) -> &str {
        PROVIDER
    }

    async fn search(&self, query: &str, limit: usize) -> Result<Vec<SearchHit>, String> {
        if query.trim().is_empty()
            || query.chars().count() > 600
            || query.split_whitespace().count() > 75
        {
            return Err("brave: query must contain 1–600 characters and at most 75 words".into());
        }
        let limit = limit.clamp(1, MAX_RESULTS);
        let mut body =
            json!({"q":query,"maximum_number_of_urls":limit,"maximum_number_of_tokens":4096});
        if let Some(country) = &self.country {
            body["country"] = json!(country);
        }
        if let Some(lang) = &self.search_lang {
            body["search_lang"] = json!(lang);
        }
        let request = self
            .client
            .post(self.endpoint.clone())
            .header("x-subscription-token", self.key.clone())
            .json(&body);
        let output: Response =
            request_json(PROVIDER, request, "check the API key and Search plan").await?;
        Ok(output
            .grounding
            .generic
            .into_iter()
            .filter_map(|r| hit(r.url, r.title, r.snippets.join("\n")))
            .take(limit)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::http::StatusCode;
    use serde_json::json;

    use super::{BraveConfig, SearchBackend};
    use crate::api::tests::Mock;

    #[tokio::test]
    async fn llm_context_sends_the_key_and_maps_sources_to_search_hits() {
        let mock = Mock::start(
            "/res/v1/llm/context",
            StatusCode::OK,
            json!({"grounding":{"generic":[
                {"url":"https://example.com/","title":"Example","snippets":["One","Two"]}
            ]}}),
            |headers, body| {
                assert_eq!(headers["x-subscription-token"], "fixture-key");
                assert_eq!(body["q"], "fixture query");
                assert_eq!(body["maximum_number_of_urls"], 2);
                assert_eq!(body["search_lang"], "ru");
            },
        )
        .await;
        let backend = BraveConfig {
            search_lang: Some("ru".into()),
            ..BraveConfig::default()
        }
        .at(Duration::from_secs(3), &mock.endpoint, "fixture-key")
        .unwrap();
        let hits = backend.search("fixture query", 2).await.unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].snippet, "One\nTwo");
    }
}
