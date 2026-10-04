use std::{sync::Arc, time::Duration};

use octo_core::{ConnectorContext, Envelope, EventKind, TrailAction, TrailActor, TrailEntry};
use reqwest::header::{HeaderMap, HeaderValue, USER_AGENT};
use serde_json::Value;

use super::{
    EndpointSpec, HttpConnector, HttpError, envelope_field, json_escape_inner, navigate,
    render_json, urlencode,
};

/// The `User-Agent` sent when the manifest declares none.
const DEFAULT_USER_AGENT: &str = concat!("octo-connector-http/", env!("CARGO_PKG_VERSION"));

impl HttpConnector {
    pub(super) async fn handle(self: Arc<Self>, envelope: Arc<Envelope>, ctx: &ConnectorContext) {
        let cmd_id = envelope.id;

        let emission = match self.dispatch(&envelope).await {
            Ok((kind, payload)) => Envelope::new(self.id.clone(), kind, payload),
            Err(err) => Envelope::new(self.id.clone(), self.spec.error_kind(), err.into_value()),
        }
        .with_correlation(cmd_id);

        let emission_kind = emission.kind.clone();
        let emission = emission.with_trail(TrailEntry::new(
            TrailActor::Connector(self.id.clone()),
            TrailAction::Emit {
                kind: emission_kind,
            },
        ));

        if let Err(e) = ctx.publish(emission).await {
            tracing::warn!(connector = %self.id, error = %e, "failed to publish http response");
        }
    }

    /// Resolve the endpoint, perform the HTTP call, and produce `(response_kind,
    /// payload)` on success.
    pub(super) async fn dispatch(
        &self,
        envelope: &Envelope,
    ) -> Result<(EventKind, Value), HttpError> {
        let endpoint = self
            .spec
            .endpoints
            .iter()
            .find(|e| e.cmd_kind == envelope.kind)
            .ok_or_else(|| {
                HttpError::local(format!("no endpoint for command kind '{}'", envelope.kind))
            })?;

        // Dynamic connectors carry serde_json::Value payloads.
        let payload: &Value = envelope.payload_as::<Value>().ok_or_else(|| {
            HttpError::local(format!(
                "expected serde_json::Value payload for '{}', got {}",
                envelope.kind,
                envelope.payload.type_name()
            ))
        })?;

        let url = self.build_url(endpoint, payload)?;
        let query = self.build_query(endpoint, payload)?;

        // Build the request body for body-carrying methods: render a template
        // if one is declared, else send the command payload verbatim.
        let body: Option<String> = if endpoint.method.has_body() {
            Some(match &endpoint.body_template {
                Some(tmpl) => self.render_template(tmpl, payload, envelope),
                None => serde_json::to_string(payload).unwrap_or_default(),
            })
        } else {
            None
        };

        let response = self.send(endpoint, &url, &query, body.as_deref()).await?;
        let status = response.status();
        let body = response.text().await.unwrap_or_default();

        if !status.is_success() {
            return Err(HttpError::http(status.as_u16(), body));
        }

        // Parse a JSON body into a Value; empty body → Null (e.g. DELETE).
        let value = if body.trim().is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&body).map_err(|e| {
                HttpError::local(format!("response decode error: {e} (body: {body})"))
            })?
        };

        Ok((endpoint.response_kind.clone(), value))
    }

    pub(super) fn build_url(
        &self,
        endpoint: &EndpointSpec,
        payload: &Value,
    ) -> Result<String, HttpError> {
        let mut path = endpoint.path.clone();
        for (name, jp) in &endpoint.path_params {
            let value = jp.extract_one(payload).ok_or_else(|| {
                HttpError::local(format!(
                    "missing path param '{name}' at JSONPath '{}'",
                    jp.as_str()
                ))
            })?;
            path = path.replace(&format!("{{{name}}}"), &urlencode(&value));
        }
        Ok(format!("{}{}", self.spec.base_url, path))
    }

    pub(super) fn build_query(
        &self,
        endpoint: &EndpointSpec,
        payload: &Value,
    ) -> Result<Vec<(String, String)>, HttpError> {
        let mut pairs = Vec::new();
        for (key, jp) in &endpoint.query_params {
            let values = jp.extract(payload);
            if values.is_empty() {
                return Err(HttpError::local(format!(
                    "missing query param '{key}' at JSONPath '{}'",
                    jp.as_str()
                )));
            }
            for v in values {
                pairs.push((key.clone(), v));
            }
        }
        Ok(pairs)
    }

    /// Render a body template, substituting `${...}` placeholders. Simple
    /// string replacement (no conditionals/loops — MVP). Supported namespaces:
    /// `payload.<path>`, `secret.<name>`, `env.<NAME>`,
    /// `envelope.<source|id|kind|timestamp|correlation_id|target>`.
    ///
    /// String values are inserted as JSON-escaped *content* (without the
    /// surrounding quotes — the template author writes those), so
    /// `"text": "${payload.text}"` stays valid JSON for arbitrary strings.
    /// Numbers / bools / objects / arrays render as their JSON literal, so
    /// `"count": ${payload.n}` works unquoted. Missing values render empty.
    pub(super) fn render_template(
        &self,
        template: &str,
        payload: &Value,
        envelope: &Envelope,
    ) -> String {
        let mut out = String::with_capacity(template.len());
        let mut rest = template;
        loop {
            let Some(start) = rest.find("${") else {
                out.push_str(rest);
                break;
            };
            out.push_str(&rest[..start]);
            let after = &rest[start + 2..];
            let Some(end) = after.find('}') else {
                // Unterminated `${` — emit the remainder verbatim.
                out.push_str(&rest[start..]);
                break;
            };
            out.push_str(&self.resolve_token(&after[..end], payload, envelope));
            rest = &after[end + 1..];
        }
        out
    }

    pub(super) fn resolve_token(
        &self,
        token: &str,
        payload: &Value,
        envelope: &Envelope,
    ) -> String {
        let (ns, path) = token.split_once('.').unwrap_or((token, ""));
        match ns {
            "payload" => navigate(payload, path).map(render_json).unwrap_or_default(),
            "secret" => self
                .spec
                .resolve_secret(&format!("${{secret.{path}}}"))
                .map(|s| json_escape_inner(&s))
                .unwrap_or_default(),
            "env" => std::env::var(path)
                .map(|s| json_escape_inner(&s))
                .unwrap_or_default(),
            "envelope" => envelope_field(envelope, path)
                .map(|s| json_escape_inner(&s))
                .unwrap_or_default(),
            // Unknown namespace: leave the token untouched so the mistake shows.
            _ => format!("${{{token}}}"),
        }
    }

    /// Send the request, retrying on configured transient statuses.
    pub(super) async fn send(
        &self,
        endpoint: &EndpointSpec,
        url: &str,
        query: &[(String, String)],
        body: Option<&str>,
    ) -> Result<reqwest::Response, HttpError> {
        let max_attempts = self.spec.retry.as_ref().map_or(1, |r| r.max_attempts);

        let mut attempt = 0;
        loop {
            attempt += 1;
            let response = self.send_once(endpoint, url, query, body).await;

            match response {
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    if attempt < max_attempts && self.should_retry(status) {
                        self.backoff(attempt).await;
                        continue;
                    }
                    return Ok(resp);
                }
                Err(e) => {
                    // Transport error: retry while attempts remain.
                    if attempt < max_attempts {
                        self.backoff(attempt).await;
                        continue;
                    }
                    return Err(HttpError::local(format!("transport error: {e}")));
                }
            }
        }
    }

    pub(super) async fn send_once(
        &self,
        endpoint: &EndpointSpec,
        url: &str,
        query: &[(String, String)],
        body: Option<&str>,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let mut req = self
            .client
            .request(endpoint.method.as_reqwest(), url)
            .headers(self.static_headers());

        if !query.is_empty() {
            req = req.query(query);
        }
        if let Some(body) = body {
            req = req
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body.to_string());
        }
        if let Some(auth) = &self.spec.auth {
            if let Some(secret) = self.spec.resolve_secret(&auth.secret_ref) {
                req = req.header(&auth.header, format!("{}{}", auth.value_prefix, secret));
            }
        }
        if let Some(ms) = self.spec.timeout_ms {
            req = req.timeout(Duration::from_millis(ms));
        }

        req.send().await
    }

    /// Headers every request carries: a default `User-Agent` naming this
    /// connector (some APIs refuse a request without one — reqwest sends none),
    /// then the manifest's `[connector.headers]`, which override it.
    pub(super) fn static_headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::with_capacity(self.spec.headers.len() + 1);
        headers.insert(USER_AGENT, HeaderValue::from_static(DEFAULT_USER_AGENT));
        for (name, value) in &self.spec.headers {
            headers.insert(name.clone(), value.clone());
        }
        headers
    }

    pub(super) fn should_retry(&self, status: u16) -> bool {
        self.spec
            .retry
            .as_ref()
            .is_some_and(|r| r.retry_on_status.contains(&status))
    }

    pub(super) async fn backoff(&self, attempt: u32) {
        let Some(retry) = &self.spec.retry else {
            return;
        };
        // Exponential: initial * 2^(attempt-1), capped.
        let factor = 1u64 << (attempt.saturating_sub(1)).min(16);
        let delay = retry
            .backoff_initial_ms
            .saturating_mul(factor)
            .min(retry.backoff_max_ms);
        tokio::time::sleep(Duration::from_millis(delay)).await;
    }
}
