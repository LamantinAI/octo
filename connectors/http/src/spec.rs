//! Declarative specification of an HTTP connector instance, parsed from a
//! `connector.toml` manifest (see `petstore_case.md` and
//! `configurable_connectors.md` vault drafts).
//!
//! One manifest describes a whole multi-route API: shared properties
//! (`base_url`, `auth`, `retry`, `timeout`) at the `[connector]` level, plus an
//! array of `[[connector.endpoint]]` entries, each mapping one CQRS command
//! kind to one HTTP call and one response kind.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use octo_core::EventKind;
use serde::Deserialize;

use crate::jsonpath::JsonPath;

/// Errors raised while loading / validating a connector manifest.
#[derive(Debug, thiserror::Error)]
pub enum SpecError {
    #[error("reading manifest {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("parsing manifest {path}: {source}")]
    Toml {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },

    #[error("connector type must be 'http', got '{0}'")]
    WrongType(String),

    #[error("endpoint '{cmd_kind}': unknown HTTP method '{method}'")]
    BadMethod { cmd_kind: String, method: String },

    #[error("endpoint '{cmd_kind}': path declares '{{{param}}}' but no path_params.{param} mapping")]
    MissingPathParam { cmd_kind: String, param: String },

    #[error("endpoint '{cmd_kind}': bad JSONPath for '{name}': {source}")]
    BadJsonPath {
        cmd_kind: String,
        name: String,
        #[source]
        source: crate::jsonpath::ParseError,
    },

    #[error("reading model '{path}': {source}")]
    ModelIo {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("parsing model '{path}': {source}")]
    ModelJson {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
}

/// HTTP verb a connector endpoint maps to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Delete,
    Patch,
}

impl HttpMethod {
    fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_uppercase().as_str() {
            "GET" => Some(Self::Get),
            "POST" => Some(Self::Post),
            "PUT" => Some(Self::Put),
            "DELETE" => Some(Self::Delete),
            "PATCH" => Some(Self::Patch),
            _ => None,
        }
    }

    /// Whether this method carries a request body built from the command payload.
    pub fn has_body(self) -> bool {
        matches!(self, Self::Post | Self::Put | Self::Patch)
    }

    pub fn as_reqwest(self) -> reqwest::Method {
        match self {
            Self::Get => reqwest::Method::GET,
            Self::Post => reqwest::Method::POST,
            Self::Put => reqwest::Method::PUT,
            Self::Delete => reqwest::Method::DELETE,
            Self::Patch => reqwest::Method::PATCH,
        }
    }
}

/// Where a secret value comes from. MVP: environment variable only.
#[derive(Debug, Clone)]
pub enum SecretSource {
    Env(String),
}

/// Connector-level authentication (MVP: a single header carrying a secret).
#[derive(Debug, Clone)]
pub struct AuthSpec {
    /// `api_key`, `bearer`, ... — kept as a string; only header-injection is
    /// implemented for the MVP. Richer schemes get dedicated Rust connectors.
    pub kind: String,
    /// Header name to set (e.g. `api-key`, `Authorization`).
    pub header: String,
    /// A `${secret.NAME}` reference resolved against [`HttpSpec::secrets`].
    pub secret_ref: String,
    /// Static prefix prepended to the resolved secret (e.g. `Bearer ` for bearer).
    pub value_prefix: String,
}

/// Retry policy for transient HTTP failures.
#[derive(Debug, Clone)]
pub struct RetrySpec {
    pub max_attempts: u32,
    pub backoff_initial_ms: u64,
    pub backoff_max_ms: u64,
    pub retry_on_status: Vec<u16>,
}

/// One outbound endpoint: CQRS command kind → HTTP call → response kind.
#[derive(Debug, Clone)]
pub struct EndpointSpec {
    pub cmd_kind: EventKind,
    pub method: HttpMethod,
    /// Path template relative to `base_url`, e.g. `/pet/{petId}`.
    pub path: String,
    /// `{placeholder}` → JSONPath into the command payload.
    pub path_params: HashMap<String, JsonPath>,
    /// query key → JSONPath into the command payload.
    pub query_params: HashMap<String, JsonPath>,
    pub request_schema: Option<String>,
    pub response_kind: EventKind,
    pub response_schema: Option<String>,
    /// Optional request-body template. When set (POST/PUT/PATCH), the body is
    /// rendered from this template instead of sending the command payload
    /// verbatim. Placeholders: `${payload.path}`, `${secret.name}`,
    /// `${env.NAME}`, `${envelope.source|id|kind|timestamp|correlation_id}`.
    /// When `None`, the whole command payload is sent as JSON (pass-through).
    pub body_template: Option<String>,
}

/// Inbound webhook listener — parsed but not yet served (Petstore sends none).
/// Captured so the manifest format is complete and validated.
#[derive(Debug, Clone)]
pub struct ListenerSpec {
    pub path: String,
    pub methods: Vec<String>,
    pub emit_kind: EventKind,
    pub emit_schema: Option<String>,
}

/// A fully validated connector specification.
#[derive(Debug, Clone)]
pub struct HttpSpec {
    pub id: String,
    pub base_url: String,
    /// Absolute path to the models directory, if declared.
    pub models_dir: Option<PathBuf>,
    pub auth: Option<AuthSpec>,
    pub retry: Option<RetrySpec>,
    pub timeout_ms: Option<u64>,
    pub endpoints: Vec<EndpointSpec>,
    pub listeners: Vec<ListenerSpec>,
    pub secrets: HashMap<String, SecretSource>,
}

impl HttpSpec {
    /// Load and validate a manifest from a TOML file. `models_dir` (if present)
    /// is resolved relative to the manifest's directory.
    pub fn from_toml_file(path: impl AsRef<Path>) -> Result<Self, SpecError> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).map_err(|source| SpecError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        let base_dir = path.parent().unwrap_or_else(|| Path::new("."));
        Self::from_toml_str(&text, base_dir).map_err(|e| match e {
            SpecError::Toml { source, .. } => SpecError::Toml {
                path: path.to_path_buf(),
                source,
            },
            other => other,
        })
    }

    /// Parse and validate a manifest from TOML text. `base_dir` anchors a
    /// relative `models_dir`.
    pub fn from_toml_str(text: &str, base_dir: &Path) -> Result<Self, SpecError> {
        let raw: RawConfig = toml::from_str(text).map_err(|source| SpecError::Toml {
            path: base_dir.to_path_buf(),
            source,
        })?;
        raw.connector.into_spec(base_dir)
    }

    /// Build from an already-parsed [`toml::Value`] (as a [`ConnectorFactory`]
    /// receives it). `base_dir` anchors a relative `models_dir`.
    ///
    /// [`ConnectorFactory`]: octo_core::ConnectorFactory
    pub fn from_toml_value(value: toml::Value, base_dir: &Path) -> Result<Self, SpecError> {
        let raw: RawConfig = value.try_into().map_err(|source| SpecError::Toml {
            path: base_dir.to_path_buf(),
            source,
        })?;
        raw.connector.into_spec(base_dir)
    }

    /// The connector's common error kind: `<id>.event.error`.
    pub fn error_kind(&self) -> EventKind {
        EventKind::new(format!("{}.event.error", self.id))
    }

    /// Resolve a `${secret.NAME}` reference against [`Self::secrets`] and the
    /// environment. Returns `None` if the reference is malformed, unknown, or
    /// the backing env var is unset.
    pub fn resolve_secret(&self, reference: &str) -> Option<String> {
        let name = reference
            .strip_prefix("${secret.")
            .and_then(|s| s.strip_suffix('}'))?;
        match self.secrets.get(name)? {
            SecretSource::Env(var) => std::env::var(var).ok(),
        }
    }

    /// Load the model JSON schemas declared in `models_dir`. Returns
    /// `(schema_name, schema)` pairs where `schema_name` is the filename stem.
    /// No `models_dir` (or a missing directory) yields an empty list.
    pub fn load_models(&self) -> Result<Vec<(String, serde_json::Value)>, SpecError> {
        let Some(dir) = &self.models_dir else {
            return Ok(Vec::new());
        };
        if !dir.is_dir() {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        let entries = std::fs::read_dir(dir).map_err(|source| SpecError::ModelIo {
            path: dir.clone(),
            source,
        })?;
        for entry in entries {
            let entry = entry.map_err(|source| SpecError::ModelIo {
                path: dir.clone(),
                source,
            })?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
                continue;
            };
            let text = std::fs::read_to_string(&path).map_err(|source| SpecError::ModelIo {
                path: path.clone(),
                source,
            })?;
            let schema: serde_json::Value =
                serde_json::from_str(&text).map_err(|source| SpecError::ModelJson {
                    path: path.clone(),
                    source,
                })?;
            out.push((stem.to_string(), schema));
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }
}

// ─── Raw TOML shapes (deserialization targets) ──────────────────────────────

#[derive(Debug, Deserialize)]
struct RawConfig {
    connector: RawConnector,
}

#[derive(Debug, Deserialize)]
struct RawConnector {
    id: String,
    #[serde(rename = "type")]
    type_: String,
    base_url: String,
    models_dir: Option<String>,
    auth: Option<RawAuth>,
    retry: Option<RawRetry>,
    timeout: Option<RawTimeout>,
    #[serde(default)]
    endpoint: Vec<RawEndpoint>,
    #[serde(default)]
    listener: Vec<RawListener>,
    #[serde(default)]
    secrets: HashMap<String, RawSecret>,
}

#[derive(Debug, Deserialize)]
struct RawAuth {
    #[serde(rename = "type")]
    type_: String,
    header: Option<String>,
    secret_var: Option<String>,
    #[serde(default)]
    value_prefix: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawRetry {
    max_attempts: u32,
    backoff_initial_ms: u64,
    backoff_max_ms: u64,
    #[serde(default)]
    retry_on_status: Vec<u16>,
}

#[derive(Debug, Deserialize)]
struct RawTimeout {
    request_ms: u64,
}

#[derive(Debug, Deserialize)]
struct RawEndpoint {
    cmd_kind: String,
    method: String,
    path: String,
    #[serde(default)]
    path_params: HashMap<String, String>,
    #[serde(default)]
    query_params: HashMap<String, String>,
    request_schema: Option<String>,
    response_kind: String,
    response_schema: Option<String>,
    body_template: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawListener {
    path: String,
    #[serde(default)]
    methods: Vec<String>,
    emit_kind: String,
    emit_schema: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawSecret {
    env: String,
}

impl RawConnector {
    fn into_spec(self, base_dir: &Path) -> Result<HttpSpec, SpecError> {
        if self.type_ != "http" {
            return Err(SpecError::WrongType(self.type_));
        }

        let models_dir = self.models_dir.map(|rel| base_dir.join(rel));

        let auth = self.auth.map(|a| AuthSpec {
            kind: a.type_,
            header: a.header.unwrap_or_else(|| "Authorization".to_string()),
            secret_ref: a.secret_var.unwrap_or_default(),
            value_prefix: a.value_prefix.unwrap_or_default(),
        });

        let retry = self.retry.map(|r| RetrySpec {
            max_attempts: r.max_attempts.max(1),
            backoff_initial_ms: r.backoff_initial_ms,
            backoff_max_ms: r.backoff_max_ms,
            retry_on_status: r.retry_on_status,
        });

        let secrets = self
            .secrets
            .into_iter()
            .map(|(k, v)| (k, SecretSource::Env(v.env)))
            .collect();

        let mut endpoints = Vec::with_capacity(self.endpoint.len());
        for ep in self.endpoint {
            endpoints.push(build_endpoint(ep)?);
        }

        let listeners = self
            .listener
            .into_iter()
            .map(|l| ListenerSpec {
                path: l.path,
                methods: l.methods,
                emit_kind: EventKind::new(l.emit_kind),
                emit_schema: l.emit_schema,
            })
            .collect();

        Ok(HttpSpec {
            id: self.id,
            base_url: resolve_env_templates(&self.base_url)
                .trim_end_matches('/')
                .to_string(),
            models_dir,
            auth,
            retry,
            timeout_ms: self.timeout.map(|t| t.request_ms),
            endpoints,
            listeners,
            secrets,
        })
    }
}

fn build_endpoint(ep: RawEndpoint) -> Result<EndpointSpec, SpecError> {
    let method = HttpMethod::parse(&ep.method).ok_or_else(|| SpecError::BadMethod {
        cmd_kind: ep.cmd_kind.clone(),
        method: ep.method.clone(),
    })?;

    let path_params = parse_param_map(&ep.cmd_kind, ep.path_params)?;
    let query_params = parse_param_map(&ep.cmd_kind, ep.query_params)?;

    // Every `{placeholder}` in the path must have a path_params mapping.
    for placeholder in path_placeholders(&ep.path) {
        if !path_params.contains_key(&placeholder) {
            return Err(SpecError::MissingPathParam {
                cmd_kind: ep.cmd_kind.clone(),
                param: placeholder,
            });
        }
    }

    Ok(EndpointSpec {
        cmd_kind: EventKind::new(ep.cmd_kind),
        method,
        path: ep.path,
        path_params,
        query_params,
        request_schema: ep.request_schema,
        response_kind: EventKind::new(ep.response_kind),
        response_schema: ep.response_schema,
        body_template: ep.body_template,
    })
}

fn parse_param_map(
    cmd_kind: &str,
    raw: HashMap<String, String>,
) -> Result<HashMap<String, JsonPath>, SpecError> {
    raw.into_iter()
        .map(|(name, expr)| {
            JsonPath::parse(&expr)
                .map(|jp| (name.clone(), jp))
                .map_err(|source| SpecError::BadJsonPath {
                    cmd_kind: cmd_kind.to_string(),
                    name,
                    source,
                })
        })
        .collect()
}

/// Substitute `${env.NAME}` occurrences with the corresponding environment
/// variable (empty string if unset). Used for `base_url` so the same manifest
/// can point at different endpoints per environment (prod / staging / a test
/// mock server). Bounded to avoid pathological re-expansion.
fn resolve_env_templates(s: &str) -> String {
    let mut out = s.to_string();
    for _ in 0..50 {
        let Some(start) = out.find("${env.") else {
            break;
        };
        let Some(end_rel) = out[start..].find('}') else {
            break;
        };
        let end = start + end_rel;
        let name = out[start + 6..end].to_string();
        let val = std::env::var(&name).unwrap_or_default();
        out.replace_range(start..=end, &val);
    }
    out
}

/// Extract `{placeholder}` names from a path template.
fn path_placeholders(path: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = path;
    while let Some(open) = rest.find('{') {
        if let Some(close_rel) = rest[open + 1..].find('}') {
            let name = &rest[open + 1..open + 1 + close_rel];
            out.push(name.to_string());
            rest = &rest[open + 1 + close_rel + 1..];
        } else {
            break;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const PETSTORE_TOML: &str = r#"
[connector]
id = "petstore"
type = "http"
base_url = "https://petstore3.swagger.io/api/v3/"
models_dir = "./models"

[connector.auth]
type = "api_key"
header = "api-key"
secret_var = "${secret.petstore_key}"

[connector.retry]
max_attempts = 3
backoff_initial_ms = 500
backoff_max_ms = 5000
retry_on_status = [429, 502, 503, 504]

[connector.timeout]
request_ms = 10000

[[connector.endpoint]]
cmd_kind = "petstore.cmd.add_pet"
method = "POST"
path = "/pet"
request_schema = "pet"
response_kind = "petstore.event.pet_added"
response_schema = "pet"

[[connector.endpoint]]
cmd_kind = "petstore.cmd.fetch_pet"
method = "GET"
path = "/pet/{petId}"
path_params = { petId = "$.id" }
response_kind = "petstore.event.pet_fetched"
response_schema = "pet"

[[connector.endpoint]]
cmd_kind = "petstore.cmd.find_pets_by_status"
method = "GET"
path = "/pet/findByStatus"
query_params = { status = "$.status" }
response_kind = "petstore.event.pets_found"
response_schema = "pet_array"

[connector.secrets]
petstore_key = { env = "PETSTORE_API_KEY" }
"#;

    #[test]
    fn parses_petstore_manifest() {
        let spec = HttpSpec::from_toml_str(PETSTORE_TOML, Path::new("/tmp/petstore")).unwrap();
        assert_eq!(spec.id, "petstore");
        // Trailing slash trimmed.
        assert_eq!(spec.base_url, "https://petstore3.swagger.io/api/v3");
        assert_eq!(spec.endpoints.len(), 3);
        assert_eq!(
            spec.models_dir,
            Some(PathBuf::from("/tmp/petstore/./models"))
        );
        assert_eq!(spec.error_kind().as_str(), "petstore.event.error");

        let fetch = spec
            .endpoints
            .iter()
            .find(|e| e.cmd_kind.as_str() == "petstore.cmd.fetch_pet")
            .unwrap();
        assert_eq!(fetch.method, HttpMethod::Get);
        assert_eq!(fetch.path_params.get("petId").unwrap().as_str(), "$.id");

        let auth = spec.auth.as_ref().unwrap();
        assert_eq!(auth.header, "api-key");
        assert_eq!(auth.secret_ref, "${secret.petstore_key}");

        let retry = spec.retry.as_ref().unwrap();
        assert_eq!(retry.max_attempts, 3);
        assert_eq!(retry.retry_on_status, vec![429, 502, 503, 504]);
        assert_eq!(spec.timeout_ms, Some(10000));
    }

    #[test]
    fn rejects_non_http_type() {
        let toml = r#"
[connector]
id = "x"
type = "mqtt"
base_url = "http://localhost"
"#;
        let err = HttpSpec::from_toml_str(toml, Path::new(".")).unwrap_err();
        assert!(matches!(err, SpecError::WrongType(_)));
    }

    #[test]
    fn rejects_unmapped_path_placeholder() {
        let toml = r#"
[connector]
id = "x"
type = "http"
base_url = "http://localhost"

[[connector.endpoint]]
cmd_kind = "x.cmd.get"
method = "GET"
path = "/thing/{thingId}"
response_kind = "x.event.got"
"#;
        let err = HttpSpec::from_toml_str(toml, Path::new(".")).unwrap_err();
        assert!(matches!(err, SpecError::MissingPathParam { param, .. } if param == "thingId"));
    }

    #[test]
    fn rejects_bad_method() {
        let toml = r#"
[connector]
id = "x"
type = "http"
base_url = "http://localhost"

[[connector.endpoint]]
cmd_kind = "x.cmd.go"
method = "FETCH"
path = "/go"
response_kind = "x.event.went"
"#;
        let err = HttpSpec::from_toml_str(toml, Path::new(".")).unwrap_err();
        assert!(matches!(err, SpecError::BadMethod { .. }));
    }

    #[test]
    fn placeholder_extraction() {
        assert_eq!(path_placeholders("/pet/{petId}"), vec!["petId"]);
        assert_eq!(
            path_placeholders("/a/{x}/b/{y}"),
            vec!["x".to_string(), "y".to_string()]
        );
        assert!(path_placeholders("/no/params").is_empty());
    }
}
