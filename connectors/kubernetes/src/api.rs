use crate::response::{Failure, compact};
use k8s_openapi::api::core::v1::Pod;
use kube::{
    Api, Client,
    api::{DynamicObject, ListParams, LogParams},
    discovery::{Scope, verbs},
};
use serde_json::{Value, json};

pub const LIST: &str = "kubernetes.list";
pub const GET: &str = "kubernetes.get";
pub const LOGS: &str = "kubernetes.logs";
pub const RESOURCES: &str = "kubernetes.resources";

use crate::discovery::{Cache, catalog, resolve};

fn field<'a>(params: &'a Value, name: &str) -> Result<&'a str, String> {
    params
        .get(name)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("provide non-empty `{name}`"))
}

pub async fn execute(
    cache: &Cache,
    client: Client,
    kind: &str,
    params: &Value,
    default_namespace: Option<&str>,
) -> Result<Value, Failure> {
    for key in ["namespace", "name", "api_version"] {
        if let Some(text) = params.get(key).and_then(Value::as_str) {
            if key == "namespace" && text == "*" {
                continue;
            }
            let parts: Vec<_> = text.split('/').collect();
            if parts.len() > (if key == "api_version" { 2 } else { 1 })
                || parts.iter().any(|s| {
                    s.is_empty()
                        || *s == "."
                        || *s == ".."
                        || !s
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_'))
                })
            {
                return Err(format!("invalid {key}").into());
            }
        }
    }
    let api_version = params
        .get("api_version")
        .map(|_| field(params, "api_version"))
        .transpose()?;
    if kind == RESOURCES {
        return Ok(catalog(
            cache
                .read(&client, api_version, flag(params, "refresh")?)
                .await?,
        ));
    }
    let namespace = match params.get("namespace") {
        Some(Value::String(s)) if s == "*" => None,
        Some(Value::String(s)) if !s.is_empty() => Some(s.as_str()),
        Some(Value::Null) | None => default_namespace,
        _ => return Err("`namespace` must be a non-empty string".into()),
    };
    if kind == LOGS {
        let ns = namespace.ok_or("logs require a namespace")?;
        let name = field(params, "name")?;
        let tail = params
            .get("tail_lines")
            .map(|v| {
                v.as_i64()
                    .filter(|n| (1..=1000).contains(n))
                    .ok_or("`tail_lines` must be between 1 and 1000")
            })
            .transpose()?
            .unwrap_or(200);
        let container = params
            .get("container")
            .map(|_| field(params, "container"))
            .transpose()?;
        let lp = LogParams {
            container: container.map(str::to_owned),
            tail_lines: Some(tail),
            limit_bytes: Some(262144),
            ..Default::default()
        };
        let logs = Api::<Pod>::namespaced(client, ns).logs(name, &lp).await?;
        return Ok(json!({ "namespace": ns, "name": name, "logs": logs }));
    }
    let operation = match kind {
        LIST => verbs::LIST,
        GET => verbs::GET,
        _ => return Err("unsupported command".into()),
    };
    let (ar, capabilities) = resolve(
        cache,
        &client,
        field(params, "resource")?,
        api_version,
        operation,
    )
    .await?;
    let api = match (capabilities.scope, namespace) {
        (Scope::Cluster, _) if params.get("namespace").is_some_and(Value::is_string) => {
            return Err("omit namespace for cluster-scoped resources".into());
        }
        (Scope::Cluster, _) => Api::<DynamicObject>::all_with(client, &ar),
        (Scope::Namespaced, Some(ns)) => Api::<DynamicObject>::namespaced_with(client, ns, &ar),
        (Scope::Namespaced, None) if kind == LIST => Api::<DynamicObject>::all_with(client, &ar),
        (Scope::Namespaced, None) => {
            return Err("get requires a namespace for namespaced resources".into());
        }
    };
    match kind {
        LIST => {
            let limit = params
                .get("limit")
                .map(|v| {
                    v.as_u64()
                        .filter(|n| (1..=500).contains(n))
                        .ok_or("`limit` must be between 1 and 500")
                })
                .transpose()?
                .unwrap_or(100) as u32;
            let mut lp = ListParams::default().limit(limit);
            if let Some(selector) = params
                .get("label_selector")
                .map(|_| field(params, "label_selector"))
                .transpose()?
            {
                lp = lp.labels(selector);
            }
            if let Some(token) = params
                .get("continue")
                .map(|_| field(params, "continue"))
                .transpose()?
            {
                lp = lp.continue_token(token);
            }
            let summary = flag(params, "compact")?;
            let result = api.list(&lp).await?;
            let mut page = serde_json::to_value(result).map_err(|e| e.to_string())?;
            if summary {
                if let Some(items) = page["items"].as_array_mut() {
                    for item in items {
                        *item = compact(item);
                    }
                }
            }
            Ok(page)
        }
        GET => {
            let result = api.get(field(params, "name")?).await?;
            Ok(serde_json::to_value(result).map_err(|e| e.to_string())?)
        }
        _ => Err("unsupported command".into()),
    }
}

fn flag(params: &Value, key: &str) -> Result<bool, Failure> {
    params
        .get(key)
        .map(|v| {
            v.as_bool()
                .ok_or_else(|| Failure::from(format!("{key} must be boolean")))
        })
        .transpose()
        .map(|v| v.unwrap_or(false))
}
