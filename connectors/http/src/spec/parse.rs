use std::{collections::HashMap, path::Path};

use octo_core::EventKind;
use serde::Deserialize;

use super::{
    AuthSpec, EndpointSpec, HttpMethod, HttpSpec, ListenerSpec, RetrySpec, SecretSource, SpecError,
};
use crate::jsonpath::JsonPath;

#[derive(Debug, Deserialize)]
pub(super) struct RawConfig {
    pub(super) connector: RawConnector,
}

#[derive(Debug, Deserialize)]
pub(super) struct RawConnector {
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
    pub(super) fn into_spec(self, base_dir: &Path) -> Result<HttpSpec, SpecError> {
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
pub(super) fn path_placeholders(path: &str) -> Vec<String> {
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
