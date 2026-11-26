//! Synchronous text search via the official Yandex Search API v2.
mod xml;

use std::time::Duration;

use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use reqwest::{
    Client, Url,
    header::{AUTHORIZATION, HeaderValue},
};
use serde::Deserialize;
use serde_json::{Value, json};

use self::xml::parse_results;
use super::{
    SearchBackend, SearchHit,
    api::{client, request_json, required_env},
};

const ENDPOINT: &str = "https://searchapi.api.cloud.yandex.net/v2/web/search";
const PROVIDER: &str = "yandex";
const MAX_RESULTS: usize = 100;

#[derive(Deserialize, Default, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum SearchType {
    #[default]
    Ru,
    Com,
    Tr,
    Kk,
    Be,
    Uz,
}

impl SearchType {
    fn wire(self) -> &'static str {
        match self {
            Self::Ru => "SEARCH_TYPE_RU",
            Self::Com => "SEARCH_TYPE_COM",
            Self::Tr => "SEARCH_TYPE_TR",
            Self::Kk => "SEARCH_TYPE_KK",
            Self::Be => "SEARCH_TYPE_BE",
            Self::Uz => "SEARCH_TYPE_UZ",
        }
    }
}

#[derive(Deserialize)]
#[serde(default)]
pub(super) struct YandexConfig {
    api_key_env: String,
    folder_id_env: String,
    search_type: SearchType,
    region: Option<String>,
}

impl Default for YandexConfig {
    fn default() -> Self {
        Self {
            api_key_env: "YANDEX_SEARCH_API_KEY".into(),
            folder_id_env: "YANDEX_SEARCH_FOLDER_ID".into(),
            search_type: SearchType::Ru,
            region: None,
        }
    }
}

impl YandexConfig {
    pub(super) fn from_table(table: &toml::Value) -> Result<Self, String> {
        // The shorthand form also passes the outer connector fields here.
        table
            .clone()
            .try_into()
            .map_err(|_| "yandex: invalid engine settings".into())
    }

    pub(super) fn build(self, timeout: Duration) -> Result<YandexBackend, String> {
        let api_key = required_env(PROVIDER, &self.api_key_env)?;
        let folder = required_env(PROVIDER, &self.folder_id_env)?;
        self.backend(timeout, ENDPOINT, &api_key, folder)
    }

    fn backend(
        self,
        timeout: Duration,
        endpoint: &str,
        api_key: &str,
        folder: String,
    ) -> Result<YandexBackend, String> {
        if let Some(region) = &self.region
            && (!matches!(self.search_type, SearchType::Ru | SearchType::Tr)
                || region.is_empty()
                || !region.bytes().all(|byte| byte.is_ascii_digit()))
        {
            return Err(
                "yandex: region must be a numeric string and requires search_type ru or tr".into(),
            );
        }
        let mut authorization = HeaderValue::from_str(&format!("Api-Key {api_key}"))
            .map_err(|_| "yandex: invalid API key header")?;
        authorization.set_sensitive(true);
        let client = client(PROVIDER, timeout)?;
        Ok(YandexBackend {
            client,
            authorization,
            folder,
            search_type: self.search_type,
            region: self.region,
            endpoint: Url::parse(endpoint).map_err(|_| "yandex: invalid API endpoint")?,
        })
    }
}

pub(super) struct YandexBackend {
    client: Client,
    endpoint: Url,
    authorization: HeaderValue,
    folder: String,
    search_type: SearchType,
    region: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SearchResponse {
    raw_data: String,
}

impl YandexBackend {
    fn request_body(&self, query: &str, limit: usize) -> Value {
        let mut body = json!({
            "query": {"searchType": self.search_type.wire(), "queryText": query},
            "groupSpec": {"groupMode": "GROUP_MODE_FLAT", "groupsOnPage": limit.to_string()},
            "maxPassages": "2",
            "folderId": self.folder,
            "responseFormat": "FORMAT_XML",
        });
        if let Some(region) = &self.region {
            body["region"] = json!(region);
        }
        body
    }
}

#[async_trait]
impl SearchBackend for YandexBackend {
    fn name(&self) -> &str {
        "yandex"
    }

    async fn search(&self, query: &str, limit: usize) -> Result<Vec<SearchHit>, String> {
        if query.trim().is_empty()
            || query.chars().count() > 400
            || query.split_whitespace().count() > 40
        {
            return Err("yandex: query must contain 1–400 characters and at most 40 words".into());
        }
        let limit = limit.clamp(1, MAX_RESULTS);
        let request = self
            .client
            .post(self.endpoint.clone())
            .header(AUTHORIZATION, self.authorization.clone())
            .json(&self.request_body(query, limit));
        let data: SearchResponse =
            request_json(PROVIDER, request, "check API key, role and folder").await?;
        let xml = STANDARD
            .decode(data.raw_data)
            .map_err(|_| "yandex: invalid Base64 search response")?;
        parse_results(&xml, limit)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::Arc,
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use axum::{
        Json, Router,
        extract::State,
        http::{HeaderMap, StatusCode},
        routing::post,
        serve,
    };
    use base64::{Engine, engine::general_purpose::STANDARD};
    use serde_json::{Value, json};
    use tokio::{net::TcpListener, spawn, task::JoinHandle};

    use super::{SearchBackend, YandexBackend, YandexConfig, parse_results};

    const RESULTS: &[u8] = br#"<?xml version="1.0" encoding="utf-8"?>
        <yandexsearch version="1.0"><response><results><grouping><group>
          <doc><url>https://example.org/?a=1&amp;b=2</url>
          <title>A &amp; B: <hlword>SQLite</hlword> documentation</title>
          <headline>Fallback headline</headline><passages>
            <passage>Read <hlword>transactions</hlword> &amp; locking.</passage>
            <passage>Second fragment.</passage>
          </passages></doc>
          <doc><url>javascript:noop()</url><title>Skip</title></doc>
          <doc><url>https://example.net/</url><headline><![CDATA[Fallback text]]></headline></doc>
        </group></grouping></results></response></yandexsearch>"#;

    #[test]
    fn xml_preserves_rank_and_handles_highlights_entities_and_missing_title() {
        let hits = parse_results(RESULTS, 5).unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].url, "https://example.org/?a=1&b=2");
        assert_eq!(hits[0].title, "A & B: SQLite documentation");
        assert_eq!(
            hits[0].snippet,
            "Read transactions & locking. Second fragment."
        );
        assert_eq!(hits[1].title, hits[1].url);
        assert_eq!(hits[1].snippet, "Fallback text");
        assert_eq!(parse_results(RESULTS, 1).unwrap().len(), 1);
    }

    #[test]
    fn zero_matches_provider_errors_and_truncated_xml_are_distinct() {
        assert!(parse_results(br#"<yandexsearch><response><error code="15">No results</error></response></yandexsearch>"#, 5).unwrap().is_empty());
        assert!(
            parse_results(
                br#"<yandexsearch><response><error code="18"/></response></yandexsearch>"#,
                5
            )
            .unwrap_err()
            .contains("18")
        );
        assert!(parse_results(b"<yandexsearch><response>", 5).is_err());
        assert!(parse_results(b"<html><body>Unexpected response</body></html>", 5).is_err());
    }

    struct StateData {
        calls: AtomicUsize,
        status: StatusCode,
        response: Value,
    }

    struct Mock {
        endpoint: String,
        state: Arc<StateData>,
        task: JoinHandle<()>,
    }

    impl Mock {
        async fn start(status: StatusCode, response: Value) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}/v2/web/search", listener.local_addr().unwrap());
            let state = Arc::new(StateData {
                calls: AtomicUsize::new(0),
                status,
                response,
            });
            let app = Router::new().route("/v2/web/search", post(
                |State(state): State<Arc<StateData>>, headers: HeaderMap, Json(body): Json<Value>| async move {
                    assert_eq!(headers["authorization"], "Api-Key fixture-key");
                    assert_eq!(body["query"], json!({"searchType":"SEARCH_TYPE_RU","queryText":"SQLite transactions"}));
                    assert_eq!(body["groupSpec"], json!({"groupMode":"GROUP_MODE_FLAT","groupsOnPage":"1"}));
                    assert_eq!(body["folderId"], "fixture-folder");
                    assert_eq!(body["responseFormat"], "FORMAT_XML");
                    assert_eq!(body["region"], "225");
                    state.calls.fetch_add(1, Ordering::Relaxed);
                    (state.status, Json(state.response.clone()))
                },
            )).with_state(state.clone());
            let task = spawn(async move { serve(listener, app).await.unwrap() });
            Self {
                endpoint,
                state,
                task,
            }
        }

        fn backend(&self) -> YandexBackend {
            let config = YandexConfig {
                region: Some("225".into()),
                ..YandexConfig::default()
            };
            config
                .backend(
                    Duration::from_secs(3),
                    &self.endpoint,
                    "fixture-key",
                    "fixture-folder".into(),
                )
                .unwrap()
        }
    }
    impl Drop for Mock {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    #[tokio::test]
    async fn api_sends_key_and_group_limit_then_decodes_base64_xml() {
        let mock = Mock::start(StatusCode::OK, json!({"rawData":STANDARD.encode(RESULTS)})).await;
        let result = mock
            .backend()
            .search("SQLite transactions", 1)
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].title, "A & B: SQLite documentation");
        assert_eq!(mock.state.calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn api_reports_auth_and_quota_failures_without_echoing_provider_bodies() {
        for (status, expected) in [
            (StatusCode::FORBIDDEN, "role and folder"),
            (StatusCode::TOO_MANY_REQUESTS, "quota exceeded"),
        ] {
            let mock = Mock::start(status, json!({"message":"opaque-fixture-diagnostic"})).await;
            let error = mock
                .backend()
                .search("SQLite transactions", 1)
                .await
                .unwrap_err();
            assert!(error.contains(expected));
            assert!(!error.contains("opaque-fixture-diagnostic"));
        }
    }

    #[tokio::test]
    async fn oversized_query_is_rejected_before_contacting_the_api() {
        let mock = Mock::start(StatusCode::OK, json!({})).await;
        assert!(mock.backend().search(&"а".repeat(401), 1).await.is_err());
        assert_eq!(mock.state.calls.load(Ordering::Relaxed), 0);
    }
}
