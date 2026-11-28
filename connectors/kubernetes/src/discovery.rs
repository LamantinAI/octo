use crate::response::Failure;
use kube::{
    Client,
    api::ApiResource,
    core::GroupVersion,
    discovery::{ApiCapabilities, Discovery, Scope, pinned_group, verbs},
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

pub type Resource = (ApiResource, ApiCapabilities);

fn version(value: &str) -> Result<GroupVersion, String> {
    let gv: GroupVersion = value
        .parse()
        .map_err(|e| format!("invalid api_version: {e}"))?;
    if gv.version.is_empty() || value.starts_with('/') || value.ends_with('/') {
        return Err("invalid api_version".into());
    }
    Ok(gv)
}

#[derive(Default)]
pub struct Cache(Mutex<HashMap<Option<String>, (Instant, Vec<Resource>)>>);
impl Cache {
    pub async fn read(
        &self,
        client: &Client,
        version: Option<&str>,
        refresh: bool,
    ) -> Result<Vec<Resource>, Failure> {
        let mut entries = self.0.lock().await;
        let key = version.map(str::to_owned);
        entries.retain(|_, (at, _)| at.elapsed() < Duration::from_secs(60));
        if !refresh {
            if let Some((_, values)) = entries.get(&key) {
                return Ok(values.clone());
            }
        }
        let values = discover(client, version).await?;
        if refresh {
            entries.clear();
        }
        entries.insert(key, (Instant::now(), values.clone()));
        Ok(values)
    }
}
async fn discover(client: &Client, api_version: Option<&str>) -> Result<Vec<Resource>, Failure> {
    let mut resources = if let Some(value) = api_version {
        let gv = version(value)?;
        let group = pinned_group(client, &gv).await?;
        group.versioned_resources(&gv.version)
    } else {
        let discovery = Discovery::new(client.clone()).run().await?;
        discovery
            .groups()
            .flat_map(|group| group.resources_by_stability())
            .collect()
    };
    resources.retain(|(_, caps)| {
        caps.supports_operation(verbs::GET) || caps.supports_operation(verbs::LIST)
    });
    resources.sort_by(|a, b| (&a.0.plural, &a.0.api_version).cmp(&(&b.0.plural, &b.0.api_version)));
    Ok(resources)
}

pub async fn resolve(
    cache: &Cache,
    client: &Client,
    resource: &str,
    api_version: Option<&str>,
    operation: &str,
) -> Result<Resource, Failure> {
    // Preserve the original command surface for these common resources.
    let version = api_version.or(match resource {
        "pods" | "services" | "events" => Some("v1"),
        "deployments" => Some("apps/v1"),
        _ => None,
    });
    let mut matches: Vec<Resource> = cache
        .read(client, version, false)
        .await?
        .into_iter()
        .filter(|(ar, _)| ar.plural == resource)
        .collect();
    if matches.len() > 1 {
        let versions = matches
            .iter()
            .map(|(ar, _)| ar.api_version.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        return Err(format!(
            "resource `{resource}` is ambiguous; specify api_version ({versions})"
        )
        .into());
    }
    let result = matches
        .pop()
        .ok_or_else(|| format!("resource `{resource}` not found"))?;
    if !result.1.supports_operation(operation) {
        return Err(format!("resource `{resource}` does not support {operation}").into());
    }
    Ok(result)
}

pub fn catalog(resources: Vec<Resource>) -> Value {
    let items: Vec<Value> = resources.into_iter().map(|(ar, caps)| json!({
        "resource": ar.plural,
        "kind": ar.kind,
        "api_version": ar.api_version,
        "scope": match caps.scope { Scope::Cluster => "cluster", Scope::Namespaced => "namespaced" },
        "get": caps.supports_operation(verbs::GET),
        "list": caps.supports_operation(verbs::LIST),
    })).collect();
    json!({ "resources": items })
}
