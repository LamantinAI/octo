//! Shared HTTP boundaries for the official search APIs.
use std::{env::var, time::Duration};

use reqwest::{Client, RequestBuilder, Url, redirect::Policy};
use serde::de::DeserializeOwned;
use serde_json::from_slice;

use super::SearchHit;

const MAX_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
const MAX_TITLE_CHARS: usize = 400;
const MAX_SNIPPET_CHARS: usize = 1800;

pub(super) fn client(provider: &str, timeout: Duration) -> Result<Client, String> {
    Client::builder()
        .timeout(timeout)
        .redirect(Policy::none())
        .build()
        .map_err(|_| format!("{provider}: could not build HTTP client"))
}

pub(super) fn required_env(provider: &str, name: &str) -> Result<String, String> {
    if name.is_empty() || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
        return Err(format!(
            "{provider}: credential references must be environment variable names"
        ));
    }
    var(name)
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(|s| s.trim().to_owned())
        .ok_or_else(|| format!("{provider}: environment variable {name} is required"))
}

pub(super) async fn request_json<T: DeserializeOwned>(
    provider: &str,
    request: RequestBuilder,
    access_hint: &str,
) -> Result<T, String> {
    let transport_error = |error: reqwest::Error| {
        if error.is_timeout() {
            format!("{provider}: request timed out")
        } else {
            format!("{provider}: request failed")
        }
    };
    let mut response = request.send().await.map_err(transport_error)?;
    let status = response.status();
    if !status.is_success() {
        // Provider error bodies may echo inputs or credential diagnostics.
        return Err(match status.as_u16() {
            401 | 403 => format!("{provider}: HTTP {} ({access_hint})", status.as_u16()),
            402 | 429 | 432..=434 => format!(
                "{provider}: HTTP {} (rate limit or quota exceeded)",
                status.as_u16()
            ),
            code => format!("{provider}: HTTP {code}"),
        });
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(transport_error)? {
        if chunk.len() > MAX_RESPONSE_BYTES - body.len() {
            return Err(format!("{provider}: API response exceeds the size limit"));
        }
        body.extend_from_slice(&chunk);
    }
    from_slice(&body).map_err(|_| format!("{provider}: invalid API response"))
}

pub(super) fn hit(url: String, title: String, snippet: String) -> Option<SearchHit> {
    let parsed = Url::parse(&url).ok()?;
    if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
        return None;
    }
    let title = title.trim();
    let title = if title.is_empty() {
        url.as_str()
    } else {
        title
    };
    Some(SearchHit {
        title: title.chars().take(MAX_TITLE_CHARS).collect(),
        snippet: snippet.trim().chars().take(MAX_SNIPPET_CHARS).collect(),
        url,
    })
}

#[cfg(test)]
pub(super) mod tests {
    use axum::{
        Json, Router,
        extract::State,
        http::{HeaderMap, StatusCode},
        routing::post,
        serve,
    };
    use serde_json::Value;
    use tokio::{net::TcpListener, spawn, task::JoinHandle};

    type Observe = fn(&HeaderMap, &Value);

    pub(crate) struct Mock {
        pub endpoint: String,
        task: JoinHandle<()>,
    }

    impl Mock {
        pub async fn start(path: &str, status: StatusCode, body: Value, observe: Observe) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}{path}", listener.local_addr().unwrap());
            let app = Router::new()
                .route(
                    path,
                    post(
                        |State((status, result, observe)): State<(StatusCode, Value, Observe)>,
                         headers: HeaderMap,
                         Json(input): Json<Value>| async move {
                            observe(&headers, &input);
                            (status, Json(result))
                        },
                    ),
                )
                .with_state((status, body, observe));
            let task = spawn(async move { serve(listener, app).await.unwrap() });
            Self { endpoint, task }
        }
    }

    impl Drop for Mock {
        fn drop(&mut self) {
            self.task.abort();
        }
    }
}
