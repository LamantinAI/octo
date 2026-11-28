//! Read-only Kubernetes access through the runtime's event bus.
//!
//! Discover Kubernetes resource types and read them, subject to Kubernetes RBAC.
//! See the connector README for command parameters and scoping.

mod api;
mod discovery;
mod response;
use crate::{
    discovery::Cache,
    response::{Failure, bounded},
};

use std::sync::Arc;

use async_trait::async_trait;
use kube::Client;
use octo_core::{
    Connector, ConnectorCapabilities, ConnectorContext, ConnectorFactory, ConnectorId, Envelope,
    EventKind, FactoryContext, Filter, OctoError, OctoResult, SubscribeOptions,
};
use serde_json::Value;

use crate::api::{GET, LIST, LOGS, RESOURCES, execute};

const DESCRIPTION: &str = "Read Kubernetes resources and CRDs, subject to RBAC. kubernetes.resources {api_version?, refresh?} discovers resource names, API versions and scopes. kubernetes.list {resource, api_version?, namespace?, compact?, limit?, continue?, label_selector?}; kubernetes.get {resource, api_version?, namespace?, name}; kubernetes.logs {namespace, name, container?, tail_lines?}. Use plural resource names and api_version to disambiguate; namespace '*' lists across namespaces.";

pub struct KubernetesConnector {
    id: ConnectorId,
    cache: Cache,
    capabilities: ConnectorCapabilities,
    default_namespace: Option<String>,
}

impl KubernetesConnector {
    pub fn new(id: impl Into<String>, default_namespace: Option<String>) -> Arc<Self> {
        Arc::new(Self {
            cache: Cache::default(),
            id: ConnectorId::new(id),
            capabilities: ConnectorCapabilities::bidirectional()
                .with_accept_kinds([LIST, GET, LOGS, RESOURCES].map(EventKind::from_static))
                .with_query(true)
                .with_description(DESCRIPTION),
            default_namespace,
        })
    }

    async fn handle(&self, client: &Client, env: &Envelope, ctx: &ConnectorContext) {
        let kind = env.kind.as_str();
        if ![LIST, GET, LOGS, RESOURCES].contains(&kind) {
            return;
        }
        let params = env.payload_as::<Value>().cloned().unwrap_or(Value::Null);
        let payload = execute(
            &self.cache,
            client.clone(),
            kind,
            &params,
            self.default_namespace.as_deref(),
        )
        .await
        .and_then(bounded)
        .unwrap_or_else(Failure::payload);
        let reply = Envelope::new(
            self.id.clone(),
            EventKind::new(format!("{kind}.result")),
            payload,
        )
        .with_correlation(env.id);
        if let Err(error) = ctx.publish(reply).await {
            tracing::warn!(%error, "kubernetes failed to publish result");
        }
    }
}

#[async_trait]
impl Connector for KubernetesConnector {
    fn id(&self) -> &ConnectorId {
        &self.id
    }
    fn capabilities(&self) -> &ConnectorCapabilities {
        &self.capabilities
    }

    async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
        let client = Client::try_default()
            .await
            .map_err(|e| OctoError::Connector(format!("kubernetes client: {e}")))?;
        let mut commands = ctx
            .subscribe(
                Filter::by_target(self.id.clone()),
                SubscribeOptions::default(),
            )
            .await?;
        loop {
            tokio::select! {
                next = commands.next() => match next {
                    Some(env) => self.handle(&client, &env, &ctx).await,
                    None => return Ok(()),
                },
                _ = ctx.shutdown.cancelled() => return Ok(()),
            }
        }
    }
}

#[derive(Default)]
pub struct KubernetesConnectorFactory;

impl ConnectorFactory for KubernetesConnectorFactory {
    fn type_name(&self) -> &str {
        "kubernetes"
    }
    fn create(
        &self,
        id: ConnectorId,
        config: &toml::Value,
        _ctx: FactoryContext<'_>,
    ) -> Result<Arc<dyn Connector>, Box<dyn std::error::Error + Send + Sync>> {
        let table = config
            .get("connector")
            .and_then(toml::Value::as_table)
            .ok_or("kubernetes: missing [connector] table")?;
        let namespace = match table.get("namespace") {
            None => None,
            Some(toml::Value::String(s)) if !s.is_empty() => Some(s.clone()),
            _ => return Err("kubernetes: namespace must be a non-empty string".into()),
        };
        Ok(KubernetesConnector::new(id.as_str(), namespace))
    }
}

pub fn factory() -> Arc<dyn ConnectorFactory> {
    Arc::new(KubernetesConnectorFactory)
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use kube::{Client, Config};
    use octo_core::{
        Connector, ConnectorContext, ConnectorId, Envelope, EventBus, EventKind, Filter,
        InProcessBus, SubscribeOptions,
    };
    use serde_json::{Value, json};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        time::timeout,
    };
    use tokio_util::sync::CancellationToken;

    use crate::{
        KubernetesConnector,
        api::{GET, LIST, LOGS, RESOURCES, execute as execute_cached},
        discovery::Cache,
    };

    async fn execute(
        client: Client,
        kind: &str,
        params: &Value,
        namespace: Option<&str>,
    ) -> Result<Value, String> {
        execute_cached(&Cache::default(), client, kind, params, namespace)
            .await
            .map_err(|e| e.payload().to_string())
    }

    async fn mock_client(
        status: &str,
        body: &str,
    ) -> (Client, tokio::task::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let status = status.to_owned();
        let body = body.to_owned();
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = vec![0; 8192];
                let n = socket.read(&mut bytes).await.unwrap();
                let request = String::from_utf8_lossy(&bytes[..n]).into_owned();
                let path = request.split_whitespace().nth(1).unwrap_or("");
                let discovery = match path {
                    "/apis" => Some(json!({"kind":"APIGroupList","apiVersion":"v1","groups":[
                        {"name":"apps","versions":[{"groupVersion":"apps/v1","version":"v1"}],"preferredVersion":{"groupVersion":"apps/v1","version":"v1"}},
                        {"name":"example.org","versions":[{"groupVersion":"example.org/v1","version":"v1"}],"preferredVersion":{"groupVersion":"example.org/v1","version":"v1"}},
                        {"name":"duplicate.org","versions":[{"groupVersion":"duplicate.org/v1","version":"v1"}],"preferredVersion":{"groupVersion":"duplicate.org/v1","version":"v1"}}
                    ]})),
                    "/api" => {
                        Some(json!({"kind":"APIVersions","apiVersion":"v1","versions":["v1"]}))
                    }
                    "/api/v1" => Some(json!({"groupVersion":"v1","resources":[
                        {"name":"pods","singularName":"pod","namespaced":true,"kind":"Pod","verbs":["get","list"]},
                        {"name":"nodes","singularName":"node","namespaced":false,"kind":"Node","verbs":["get","list"]},
                        {"name":"secrets","singularName":"secret","namespaced":true,"kind":"Secret","verbs":["get","list"]}
                    ]})),
                    "/apis/apps/v1" => Some(json!({"groupVersion":"apps/v1","resources":[
                        {"name":"deployments","singularName":"deployment","namespaced":true,"kind":"Deployment","verbs":["get","list"]}
                    ]})),
                    "/apis/example.org/v1" => {
                        Some(json!({"groupVersion":"example.org/v1","resources":[
                            {"name":"mice","singularName":"mouse","namespaced":true,"kind":"Mouse","verbs":["get","list"]},
                            {"name":"widgets","singularName":"widget","namespaced":true,"kind":"Widget","verbs":["get","list"]}
                        ]}))
                    }
                    "/apis/duplicate.org/v1" => {
                        Some(json!({"groupVersion":"duplicate.org/v1","resources":[
                            {"name":"widgets","singularName":"widget","namespaced":true,"kind":"Widget","verbs":["get","list"]}
                        ]}))
                    }
                    _ => None,
                };
                requests.push(request);
                let is_discovery = discovery.is_some();
                let response_body = discovery
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| body.clone());
                let response_status = if is_discovery { "200 OK" } else { &status };
                let response = format!(
                    "HTTP/1.1 {response_status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{response_body}",
                    response_body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
                if !is_discovery {
                    break;
                }
            }
            requests
        });
        let config = Config::new(format!("http://{address}").parse().unwrap());
        (Client::try_from(config).unwrap(), server)
    }

    #[tokio::test]
    async fn list_uses_all_namespace_endpoint_and_returns_continuation() {
        let (client, server) = mock_client(
            "200 OK",
            r#"{"apiVersion":"v1","kind":"PodList","metadata":{"continue":"next"},"items":[]}"#,
        )
        .await;
        let result = execute(client, LIST, &json!({"resource":"pods", "limit": 5}), None)
            .await
            .unwrap();
        assert_eq!(result["metadata"]["continue"], "next");
        let requests = timeout(Duration::from_secs(3), server)
            .await
            .unwrap()
            .unwrap();
        let request = requests.last().unwrap();
        assert!(
            request.starts_with("GET /api/v1/pods?") && request.contains("limit=5"),
            "{request}"
        );
    }

    #[tokio::test]
    async fn get_uses_namespace_and_preserves_authorization_error() {
        let (client, server) = mock_client("403 Forbidden", r#"{"kind":"Status","apiVersion":"v1","status":"Failure","message":"forbidden by RBAC","reason":"Forbidden","code":403}"#).await;
        let error = execute(
            client,
            GET,
            &json!({"resource":"deployments", "namespace":"dev", "name":"web"}),
            None,
        )
        .await
        .unwrap_err();
        assert!(error.contains("forbidden by RBAC"), "{error}");
        let requests = timeout(Duration::from_secs(3), server)
            .await
            .unwrap()
            .unwrap();
        let request = requests.last().unwrap();
        assert!(
            request.starts_with("GET /apis/apps/v1/namespaces/dev/deployments/web "),
            "{request}"
        );
    }

    #[tokio::test]
    async fn logs_use_default_namespace_and_bounded_output() {
        let (client, server) = mock_client("200 OK", "hello").await;
        let result = execute(
            client,
            LOGS,
            &json!({"name":"pod", "tail_lines":10}),
            Some("dev"),
        )
        .await
        .unwrap();
        assert_eq!(result["logs"], "hello");
        let requests = timeout(Duration::from_secs(3), server)
            .await
            .unwrap()
            .unwrap();
        let request = requests.last().unwrap();
        assert!(
            request.starts_with("GET /api/v1/namespaces/dev/pods/pod/log?"),
            "{request}"
        );
        assert!(
            request.contains("tailLines=10") && request.contains("limitBytes=262144"),
            "{request}"
        );
    }

    #[tokio::test]
    async fn rejects_unscoped_logs() {
        let (client, server) = mock_client("200 OK", "{}").await;
        assert!(
            execute(client, LOGS, &json!({"name":"pod"}), None)
                .await
                .is_err()
        );
        server.abort();
    }

    #[tokio::test]
    async fn custom_resource_uses_discovered_plural_and_namespace() {
        let (client, server) = mock_client(
            "200 OK",
            r#"{"apiVersion":"example.org/v1","kind":"Mouse","metadata":{"name":"one"}}"#,
        )
        .await;
        execute(client, GET, &json!({"resource":"mice","api_version":"example.org/v1","namespace":"dev","name":"one"}), None).await.unwrap();
        let requests = timeout(Duration::from_secs(3), server)
            .await
            .unwrap()
            .unwrap();
        assert!(
            requests
                .last()
                .unwrap()
                .starts_with("GET /apis/example.org/v1/namespaces/dev/mice/one ")
        );
    }

    #[tokio::test]
    async fn unpinned_resource_is_discovered_across_api_groups() {
        let (client, server) = mock_client(
            "200 OK",
            r#"{"apiVersion":"example.org/v1","kind":"Mouse","metadata":{"name":"one"}}"#,
        )
        .await;
        execute(
            client,
            GET,
            &json!({"resource":"mice","namespace":"dev","name":"one"}),
            None,
        )
        .await
        .unwrap();
        let requests = timeout(Duration::from_secs(3), server)
            .await
            .unwrap()
            .unwrap();
        assert!(
            requests
                .last()
                .unwrap()
                .starts_with("GET /apis/example.org/v1/namespaces/dev/mice/one ")
        );
    }

    #[tokio::test]
    async fn ambiguous_resource_requires_api_version() {
        let (client, server) = mock_client("200 OK", "{}").await;
        let error = execute(client, LIST, &json!({"resource":"widgets"}), None)
            .await
            .unwrap_err();
        assert!(
            error.contains("ambiguous")
                && error.contains("duplicate.org/v1")
                && error.contains("example.org/v1"),
            "{error}"
        );
        server.abort();
    }

    #[tokio::test]
    async fn cluster_scoped_get_ignores_default_namespace() {
        let (client, server) = mock_client(
            "200 OK",
            r#"{"apiVersion":"v1","kind":"Node","metadata":{"name":"node"}}"#,
        )
        .await;
        execute(
            client,
            GET,
            &json!({"resource":"nodes","api_version":"v1","name":"node"}),
            Some("dev"),
        )
        .await
        .unwrap();
        let requests = timeout(Duration::from_secs(3), server)
            .await
            .unwrap()
            .unwrap();
        assert!(
            requests
                .last()
                .unwrap()
                .starts_with("GET /api/v1/nodes/node ")
        );
    }

    #[tokio::test]
    async fn discovery_reports_resources_and_scope() {
        let (client, server) = mock_client("200 OK", "{}").await;
        let result = execute(client, RESOURCES, &json!({"api_version":"v1"}), None)
            .await
            .unwrap();
        assert!(
            result["resources"]
                .as_array()
                .unwrap()
                .iter()
                .any(|r| r["resource"] == "nodes" && r["scope"] == "cluster")
        );
        server.abort();
    }

    #[tokio::test]
    async fn response_is_correlated_to_command() {
        let (client, server) = mock_client(
            "200 OK",
            r#"{"apiVersion":"v1","kind":"PodList","metadata":{},"items":[]}"#,
        )
        .await;
        let connector = KubernetesConnector::new("kubernetes", None);
        let bus = Arc::new(InProcessBus::new(16));
        let ctx = ConnectorContext::new(CancellationToken::new(), bus.clone());
        let mut results = bus
            .subscribe(
                Filter::by_kind("kubernetes.list.result"),
                SubscribeOptions::default(),
            )
            .await
            .unwrap();
        let cmd = Envelope::new(
            ConnectorId::new("test"),
            EventKind::new(LIST),
            json!({"resource":"pods"}),
        );
        connector.handle(&client, &cmd, &ctx).await;
        let reply = timeout(Duration::from_secs(3), results.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reply.correlation_id, Some(cmd.id));
        assert_eq!(reply.payload_as::<Value>().unwrap()["items"], json!([]));
        assert_eq!(connector.id().as_str(), "kubernetes");
        timeout(Duration::from_secs(3), server)
            .await
            .unwrap()
            .unwrap();
    }
}
