//! # octo-connector-http
//!
//! A **dynamic, TOML-configured HTTP connector** for the Octo runtime: one
//! Rust crate, many connector instances. Instead of writing a Rust connector
//! per integration, a whole multi-route REST API is described declaratively in
//! a `connector.toml` manifest (endpoints, JSONPath path/query params,
//! JSON-schema models). This is the "sweet spot between OpenClaw skills and MCP
//! servers" position from the `configurable_connectors.md` vault draft.
//!
//! Payloads are [`serde_json::Value`] — dynamic connectors have no static Rust
//! types. Each command kind is registered against `Value` in the shared
//! [`PayloadRegistry`]; model schemas are registered for the LLM tool catalogue.
//!
//! ## What this MVP covers
//!
//! - **Outbound endpoints** — `[[connector.endpoint]]`: a CQRS command kind
//!   (`<ns>.cmd.*`) → one HTTP call → a response kind (`<ns>.event.*`),
//!   correlated through `correlation_id`. Failures emit `<ns>.event.error`.
//! - Path/query parameters extracted from the command payload via a JSONPath
//!   subset ([`jsonpath`]).
//! - Header-based auth, per-request timeout, status-based retry.
//!
//! Inbound `[[connector.listener]]` webhooks are *parsed and validated* but not
//! yet served (Petstore sends none) — see `petstore_case.md`.
//!
//! See the `petstore_case.md`, `configurable_connectors.md` and
//! `runtime_config.md` vault drafts for the full design.

pub mod jsonpath;
pub mod spec;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use octo_core::{
    Connector, ConnectorCapabilities, ConnectorContext, ConnectorFactory, ConnectorId, Envelope,
    EventKind, FactoryContext, Filter, OctoResult, PayloadRegistry, SubscribeOptions, TrailAction,
    TrailActor, TrailEntry,
};
use serde_json::Value;

pub use spec::{
    AuthSpec, EndpointSpec, HttpMethod, HttpSpec, ListenerSpec, RetrySpec, SecretSource, SpecError,
};

/// A dynamic HTTP connector instance, driven entirely by an [`HttpSpec`].
pub struct HttpConnector {
    id: ConnectorId,
    spec: HttpSpec,
    capabilities: ConnectorCapabilities,
    client: reqwest::Client,
}

impl HttpConnector {
    /// Build a connector from a validated [`HttpSpec`] using a default HTTP client.
    pub fn from_spec(spec: HttpSpec) -> Arc<Self> {
        Self::with_client(spec, reqwest::Client::new())
    }

    /// Build a connector from a validated [`HttpSpec`] with a caller-supplied
    /// HTTP client (lets several dyn connectors share a connection pool, and
    /// lets tests inject a client pointed at a mock server).
    pub fn with_client(spec: HttpSpec, client: reqwest::Client) -> Arc<Self> {
        let id = ConnectorId::new(spec.id.clone());

        let accept_kinds: Vec<EventKind> =
            spec.endpoints.iter().map(|e| e.cmd_kind.clone()).collect();
        let mut emit_kinds: Vec<EventKind> =
            spec.endpoints.iter().map(|e| e.response_kind.clone()).collect();
        emit_kinds.push(spec.error_kind());
        emit_kinds.extend(spec.listeners.iter().map(|l| l.emit_kind.clone()));

        let capabilities = ConnectorCapabilities::output_only()
            .with_accept_kinds(accept_kinds)
            .with_emit_kinds(emit_kinds)
            // Advertise as an agent-callable tool: the command kinds + payload
            // fields, so the runtime's introspection surfaces it to a cogitator.
            .with_description(catalog(&spec));

        Arc::new(Self {
            id,
            spec,
            capabilities,
            client,
        })
    }

    /// Load a connector from a `connector.toml` manifest file.
    pub fn from_file(path: impl AsRef<std::path::Path>) -> Result<Arc<Self>, SpecError> {
        Ok(Self::from_spec(HttpSpec::from_toml_file(path)?))
    }

    pub fn spec(&self) -> &HttpSpec {
        &self.spec
    }

    /// Register this connector's kinds and model schemas in the shared
    /// [`PayloadRegistry`]. Builder-style: consumes and returns the registry.
    ///
    /// - every command/response/error kind is registered against
    ///   [`serde_json::Value`] (so strict-mode validation passes and connectors
    ///   can't silently fight over a kind);
    /// - every model in `models_dir` is registered as `<id>.<model>` with its
    ///   JSON schema, for the LLM tool catalogue.
    pub fn register_payloads(&self, mut registry: PayloadRegistry) -> PayloadRegistry {
        for ep in &self.spec.endpoints {
            registry = registry
                .register_type::<Value>(ep.cmd_kind.clone())
                .register_type::<Value>(ep.response_kind.clone());
        }
        registry = registry.register_type::<Value>(self.spec.error_kind());
        for l in &self.spec.listeners {
            registry = registry.register_type::<Value>(l.emit_kind.clone());
        }

        match self.spec.load_models() {
            Ok(models) => {
                for (name, schema) in models {
                    let kind = EventKind::new(format!("{}.{}", self.spec.id, name));
                    registry = registry.register_with_schema::<Value>(kind, schema);
                }
            }
            Err(e) => {
                tracing::warn!(connector = %self.id, error = %e, "failed to load model schemas");
            }
        }
        registry
    }
}

#[async_trait]
impl Connector for HttpConnector {
    fn id(&self) -> &ConnectorId {
        &self.id
    }

    fn capabilities(&self) -> &ConnectorCapabilities {
        &self.capabilities
    }

    async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
        let mut sub = ctx
            .subscribe(Filter::by_target(self.id.clone()), SubscribeOptions::default())
            .await?;

        loop {
            tokio::select! {
                next = sub.next() => match next {
                    Some(envelope) => self.clone().handle(envelope, &ctx).await,
                    None => return Ok(()),
                },
                _ = ctx.shutdown.cancelled() => return Ok(()),
            }
        }
    }

    fn register_payloads(&self, registry: PayloadRegistry) -> PayloadRegistry {
        // Delegate to the inherent method (same logic; usable on a concrete
        // `HttpConnector` too).
        HttpConnector::register_payloads(self, registry)
    }
}

impl HttpConnector {
    async fn handle(self: Arc<Self>, envelope: Arc<Envelope>, ctx: &ConnectorContext) {
        let cmd_id = envelope.id;

        let emission = match self.dispatch(&envelope).await {
            Ok((kind, payload)) => Envelope::new(self.id.clone(), kind, payload),
            Err(err) => Envelope::new(self.id.clone(), self.spec.error_kind(), err.into_value()),
        }
        .with_correlation(cmd_id);

        let emission_kind = emission.kind.clone();
        let emission = emission.with_trail(TrailEntry::new(
            TrailActor::Connector(self.id.clone()),
            TrailAction::Emit { kind: emission_kind },
        ));

        if let Err(e) = ctx.publish(emission).await {
            tracing::warn!(connector = %self.id, error = %e, "failed to publish http response");
        }
    }

    /// Resolve the endpoint, perform the HTTP call, and produce `(response_kind,
    /// payload)` on success.
    async fn dispatch(&self, envelope: &Envelope) -> Result<(EventKind, Value), HttpError> {
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

    fn build_url(&self, endpoint: &EndpointSpec, payload: &Value) -> Result<String, HttpError> {
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

    fn build_query(
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
    fn render_template(&self, template: &str, payload: &Value, envelope: &Envelope) -> String {
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

    fn resolve_token(&self, token: &str, payload: &Value, envelope: &Envelope) -> String {
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
    async fn send(
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

    async fn send_once(
        &self,
        endpoint: &EndpointSpec,
        url: &str,
        query: &[(String, String)],
        body: Option<&str>,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let mut req = self.client.request(endpoint.method.as_reqwest(), url);

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

    fn should_retry(&self, status: u16) -> bool {
        self.spec
            .retry
            .as_ref()
            .is_some_and(|r| r.retry_on_status.contains(&status))
    }

    async fn backoff(&self, attempt: u32) {
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

/// Walk a dot-separated path (`a.b.c`) into a JSON value. An empty path returns
/// the root (so `${payload}` yields the whole payload).
fn navigate<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    if path.is_empty() {
        return Some(value);
    }
    let mut current = value;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }
    Some(current)
}

/// Render a JSON value for insertion into a body template. Strings become their
/// escaped *content* (no surrounding quotes — the template provides those);
/// everything else becomes its JSON literal.
fn render_json(value: &Value) -> String {
    match value {
        Value::String(s) => json_escape_inner(s),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

/// JSON-escape a string and strip the surrounding quotes, yielding content safe
/// to drop inside a `"..."` in a template.
fn json_escape_inner(s: &str) -> String {
    let quoted = serde_json::to_string(s).unwrap_or_else(|_| format!("\"{s}\""));
    quoted[1..quoted.len() - 1].to_string()
}

/// Read a header field of an envelope for `${envelope.<field>}`.
fn envelope_field(envelope: &Envelope, field: &str) -> Option<String> {
    match field {
        "source" => Some(envelope.source.as_str().to_string()),
        "id" => Some(envelope.id.to_string()),
        "kind" => Some(envelope.kind.as_str().to_string()),
        "timestamp" => Some(envelope.timestamp.to_rfc3339()),
        "correlation_id" => envelope.correlation_id.map(|c| c.to_string()),
        "target" => envelope.target.as_ref().map(|t| t.as_str().to_string()),
        _ => None,
    }
}

/// Build the agent-facing tool description from the spec: one line per command
/// kind with the payload fields the LLM must fill (derived from the endpoints'
/// JSONPath path/query params, or "object" for body endpoints). Surfaced via
/// [`ConnectorCapabilities::description`] so the runtime's introspection makes
/// the connector visible to a cogitator without per-agent wiring.
fn catalog(spec: &HttpSpec) -> String {
    spec.endpoints
        .iter()
        .map(|ep| {
            let mut fields: Vec<String> = ep
                .path_params
                .values()
                .chain(ep.query_params.values())
                .filter_map(|jp| jp.as_str().strip_prefix("$.").map(|s| s.to_string()))
                .collect();
            fields.sort();
            fields.dedup();

            let payload = if ep.method.has_body() {
                if fields.is_empty() {
                    "a JSON object with the resource fields".to_string()
                } else {
                    format!("a JSON object with {}", fields.join(", "))
                }
            } else if fields.is_empty() {
                "{} (no fields)".to_string()
            } else {
                fields.join(", ")
            };

            format!("    {} — payload: {}", ep.cmd_kind.as_str(), payload)
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Minimal percent-encoding for path segments (RFC 3986 unreserved set is left
/// as-is, everything else is `%`-escaped). Query values go through reqwest's
/// own encoder, so only path interpolation needs this.
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Internal error carrying enough to render a `<ns>.event.error` payload.
struct HttpError {
    http_status: Option<u16>,
    message: String,
}

impl HttpError {
    fn local(message: impl Into<String>) -> Self {
        Self {
            http_status: None,
            message: message.into(),
        }
    }

    fn http(status: u16, body: String) -> Self {
        Self {
            http_status: Some(status),
            message: format!("http {status}: {body}"),
        }
    }

    /// Render as the `<ns>.event.error` payload shape.
    fn into_value(self) -> Value {
        serde_json::json!({
            "http_status": self.http_status,
            "message": self.message,
        })
    }
}

/// [`ConnectorFactory`] for `type = "http"` connectors. Register it once with
/// [`OctoBuilder::register_connector_type`](octo_core::OctoBuilder::register_connector_type),
/// and every `connector.toml` with `type = "http"` becomes an instance.
pub struct HttpConnectorFactory {
    client: reqwest::Client,
}

impl HttpConnectorFactory {
    /// Factory using a default HTTP client.
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }

    /// Factory whose instances share a caller-supplied HTTP client (connection
    /// pool reuse; or a proxy-free client in tests).
    pub fn with_client(client: reqwest::Client) -> Self {
        Self { client }
    }
}

impl Default for HttpConnectorFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectorFactory for HttpConnectorFactory {
    fn type_name(&self) -> &str {
        "http"
    }

    fn create(
        &self,
        _id: ConnectorId,
        config: &toml::Value,
        ctx: FactoryContext<'_>,
    ) -> Result<Arc<dyn Connector>, Box<dyn std::error::Error + Send + Sync>> {
        // The connector takes its id from the manifest's `[connector] id`,
        // which is the same value the loader passed as `_id`.
        let spec = HttpSpec::from_toml_value(config.clone(), ctx.base_dir)?;
        Ok(HttpConnector::with_client(spec, self.client.clone()))
    }
}

/// Convenience: a boxed-free factory handle for registration.
/// `Octo::builder().register_connector_type("http", octo_connector_http::factory())`.
pub fn factory() -> Arc<dyn ConnectorFactory> {
    Arc::new(HttpConnectorFactory::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::path::Path;

    fn notifier() -> Arc<HttpConnector> {
        let toml = r#"
[connector]
id = "notifier"
type = "http"
base_url = "http://localhost"

[[connector.endpoint]]
cmd_kind = "notify.cmd.send"
method = "POST"
path = "/notify"
response_kind = "notify.event.sent"
"#;
        HttpConnector::from_spec(HttpSpec::from_toml_str(toml, Path::new(".")).unwrap())
    }

    fn env(payload: Value) -> Envelope {
        Envelope::new(
            ConnectorId::new("agent"),
            EventKind::from_static("notify.cmd.send"),
            payload,
        )
    }

    #[test]
    fn template_renders_valid_json_with_static_and_payload_fields() {
        let c = notifier();
        let payload = json!({ "text": "hi \"there\"", "channel": "#ops", "n": 5 });
        let tmpl = r#"{ "text": "${payload.text}", "channel": "${payload.channel}", "count": ${payload.n}, "user": "octo" }"#;

        let rendered = c.render_template(tmpl, &payload, &env(json!({})));
        let parsed: Value = serde_json::from_str(&rendered).expect("rendered body is valid JSON");

        // String with embedded quotes is escaped correctly.
        assert_eq!(parsed["text"], "hi \"there\"");
        assert_eq!(parsed["channel"], "#ops");
        // Bare number placeholder renders as a JSON number, not a string.
        assert_eq!(parsed["count"], 5);
        assert_eq!(parsed["user"], "octo");
    }

    #[test]
    fn template_renders_envelope_fields() {
        let c = notifier();
        let rendered = c.render_template(
            r#"{ "src": "${envelope.source}", "kind": "${envelope.kind}" }"#,
            &json!({}),
            &env(json!({})),
        );
        let parsed: Value = serde_json::from_str(&rendered).unwrap();
        assert_eq!(parsed["src"], "agent");
        assert_eq!(parsed["kind"], "notify.cmd.send");
    }

    #[test]
    fn template_missing_payload_field_renders_empty() {
        let c = notifier();
        let out = c.render_template("${payload.nope}", &json!({ "x": 1 }), &env(json!({})));
        assert_eq!(out, "");
    }

    #[test]
    fn template_nested_payload_path() {
        let c = notifier();
        let out = c.render_template(
            "${payload.a.b}",
            &json!({ "a": { "b": "deep" } }),
            &env(json!({})),
        );
        assert_eq!(out, "deep");
    }

    #[test]
    fn template_unknown_namespace_left_literal() {
        let c = notifier();
        let out = c.render_template("${bogus.x}", &json!({}), &env(json!({})));
        assert_eq!(out, "${bogus.x}");
    }
}
