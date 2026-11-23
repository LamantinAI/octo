//! Standalone MCP client transport and lifecycle. No bus, Connector, Rig or actor
//! policy dependency. The caller owns tool exposure and administrative grants.
mod call;
mod config;
mod connection;
mod credentials;
mod discovery;
mod error;
mod session;
mod transport;

pub use config::{ClientLimits, ServerConfig};
pub use credentials::{Credentials, EnvironmentCredentials};
pub use discovery::ToolSnapshot;
pub use error::{McpError, Result};
/// These wire types intentionally track this crate's pinned RMCP SDK version.
pub use rmcp::model::{CallToolResult, JsonObject, Tool};

use std::{path::PathBuf, sync::Arc, sync::RwLock, sync::atomic::AtomicU64};

use self::connection::ActiveCalls;
use tokio::{
    sync::{Mutex, Semaphore},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use self::transport::Session;

#[derive(Default)]
struct CloseState {
    started: bool,
    result: Option<Result<()>>,
}
struct ConnectionState {
    epoch: u64,
    session: Option<Session>,
    active: Arc<ActiveCalls>,
}
pub struct ServerSession {
    config: ServerConfig,
    base: PathBuf,
    limits: ClientLimits,
    credentials: Arc<dyn Credentials>,
    state: Mutex<ConnectionState>,
    snapshot: RwLock<Arc<ToolSnapshot>>,
    invalid_epoch: AtomicU64,
    shutdown: CancellationToken,
    calls: Semaphore,
    retired: Mutex<Vec<JoinHandle<Result<()>>>>,
    closing: Mutex<CloseState>,
}

#[cfg(test)]
mod fixture;
#[cfg(test)]
mod stdio;
#[cfg(test)]
mod tests {
    use super::fixture::{Fake, Secrets, http, stateful};
    use super::{
        ClientLimits, EnvironmentCredentials, JsonObject, McpError, ServerConfig, ServerSession,
    };
    use serde_json::{Value, json};
    use std::{collections::BTreeMap, sync::Arc, time::Duration};
    use tempfile::tempdir;
    use tokio::{spawn, time::timeout};
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn http_pagination_arguments_and_structured_nontext_error_results_are_preserved() {
        let fake = Fake::default();
        let server = http(fake.clone(), false).await;
        let base = tempdir().unwrap();
        let client = ServerSession::connect(
            ServerConfig::Http {
                url: server.url.clone(),
                token_env: None,
            },
            base.path(),
            ClientLimits::default(),
            Arc::new(EnvironmentCredentials),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let snapshot = client.snapshot();
        assert_eq!(snapshot.tools.len(), 4);
        assert_eq!(
            snapshot.find("echo").unwrap().input_schema["properties"]["edition"]["const"],
            0
        );
        let arguments = json!({"message":"hello"}).as_object().unwrap().clone();
        let result = client
            .call(
                &snapshot.revision,
                "echo",
                arguments.clone(),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            result.structured_content.unwrap()["arguments"],
            Value::Object(arguments)
        );
        let result = client
            .call(
                &snapshot.revision,
                "result",
                JsonObject::new(),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(result.structured_content.unwrap()["nested"]["value"], 7);
        assert!(
            result
                .content
                .iter()
                .any(|content| content.raw.as_image().is_some())
        );
        let result = client
            .call(
                &snapshot.revision,
                "error",
                JsonObject::new(),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(result.is_error, Some(true));
        assert!(matches!(
            client
                .call("stale", "echo", JsonObject::new(), CancellationToken::new())
                .await,
            Err(McpError::SchemaChanged)
        ));
        assert!(matches!(
            client
                .call(
                    &snapshot.revision,
                    "absent",
                    JsonObject::new(),
                    CancellationToken::new()
                )
                .await,
            Err(McpError::UnknownTool)
        ));
        assert_eq!(fake.0.lock().unwrap().calls.len(), 3);
        client.close().await.unwrap();
        assert!(!client.is_connected().await);
    }
    #[tokio::test]
    async fn incomplete_discovery_duplicates_and_count_byte_limits_fail_without_publishing_partial_contracts()
     {
        for mode in ["cursor", "pages", "duplicate", "count", "bytes"] {
            let fake = Fake::default();
            {
                let mut state = fake.0.lock().unwrap();
                state.repeated_cursor = mode == "cursor";
                state.empty_pages = mode == "pages";
                state.duplicate = mode == "duplicate";
                state.definition_size = if mode == "bytes" { 2048 } else { 0 };
            }
            let server = http(fake, false).await;
            let base = tempdir().unwrap();
            let limits = ClientLimits {
                max_discovery_pages: 3,
                max_tools: if mode == "count" { 1 } else { 512 },
                max_discovery_bytes: if mode == "bytes" {
                    1024
                } else {
                    4 * 1024 * 1024
                },
                ..Default::default()
            };
            let config = ServerConfig::Http {
                url: server.url.clone(),
                token_env: None,
            };
            assert!(
                ServerSession::connect(
                    config,
                    base.path(),
                    limits,
                    Arc::new(EnvironmentCredentials),
                    CancellationToken::new()
                )
                .await
                .is_err(),
                "{mode}"
            );
        }
    }
    #[tokio::test]
    async fn timeout_is_indeterminate_and_next_call_checks_reconnected_schema_without_replay() {
        let fake = Fake::default();
        let server = http(fake.clone(), false).await;
        let base = tempdir().unwrap();
        let client = ServerSession::connect(
            ServerConfig::Http {
                url: server.url.clone(),
                token_env: None,
            },
            base.path(),
            ClientLimits {
                call_timeout_ms: 60,
                ..Default::default()
            },
            Arc::new(EnvironmentCredentials),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let old = client.snapshot();
        assert!(matches!(
            client
                .call(
                    &old.revision,
                    "slow",
                    JsonObject::new(),
                    CancellationToken::new()
                )
                .await,
            Err(McpError::Indeterminate)
        ));
        assert_eq!(fake.0.lock().unwrap().calls, vec!["slow"]);
        fake.0.lock().unwrap().schema = 1;
        let changed = client
            .call(
                &old.revision,
                "echo",
                JsonObject::new(),
                CancellationToken::new(),
            )
            .await;
        assert!(
            matches!(changed, Err(McpError::SchemaChanged)),
            "unexpected contract-check outcome: {:?}",
            changed.err()
        );
        let fresh = client.refresh().await.unwrap();
        assert_ne!(fresh.revision, old.revision);
        client
            .call(
                &fresh.revision,
                "echo",
                JsonObject::new(),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(fake.0.lock().unwrap().calls, vec!["slow", "echo"]);
        client.close().await.unwrap();
    }
    #[tokio::test]
    async fn bearer_references_are_resolved_and_error_metadata_does_not_repeat_secret_values() {
        let fake = Fake::default();
        let server = http(fake, true).await;
        let base = tempdir().unwrap();
        let config = ServerConfig::Http {
            url: server.url.clone(),
            token_env: Some("MCP_TOKEN".into()),
        };
        let missing = ServerSession::connect(
            config.clone(),
            base.path(),
            ClientLimits::default(),
            Arc::new(Secrets(BTreeMap::new())),
            CancellationToken::new(),
        )
        .await
        .err()
        .unwrap();
        assert_eq!(missing, McpError::CredentialUnavailable);
        assert!(!missing.to_string().contains("fixture-secret-value"));
        let client = ServerSession::connect(
            config,
            base.path(),
            ClientLimits::default(),
            Arc::new(Secrets(BTreeMap::from([(
                "MCP_TOKEN".into(),
                "fixture-secret-value".into(),
            )]))),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        client
            .call(
                &client.snapshot().revision,
                "echo",
                JsonObject::new(),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        client.close().await.unwrap();
    }
    #[tokio::test]
    async fn received_oversized_results_are_distinct_from_uncertain_invocations_and_cancellation_fences_new_calls()
     {
        let fake = Fake::default();
        fake.0.lock().unwrap().result_size = 2048;
        let server = http(fake.clone(), false).await;
        let base = tempdir().unwrap();
        let client = ServerSession::connect(
            ServerConfig::Http {
                url: server.url.clone(),
                token_env: None,
            },
            base.path(),
            ClientLimits {
                max_result_bytes: 1024,
                ..Default::default()
            },
            Arc::new(EnvironmentCredentials),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let revision = client.snapshot().revision.clone();
        assert!(matches!(
            client
                .call(
                    &revision,
                    "result",
                    JsonObject::new(),
                    CancellationToken::new()
                )
                .await,
            Err(McpError::ResultTooLarge)
        ));
        let cancel = CancellationToken::new();
        cancel.cancel();
        assert!(matches!(
            client
                .call(&revision, "echo", JsonObject::new(), cancel)
                .await,
            Err(McpError::Cancelled)
        ));
        assert_eq!(fake.0.lock().unwrap().calls, vec!["result"]);
        let owned = client.clone();
        let rev = revision.clone();
        let running = spawn(async move {
            owned
                .call(&rev, "slow", JsonObject::new(), CancellationToken::new())
                .await
        });
        let slow_started = { fake.0.lock().unwrap().slow_started.clone() };
        timeout(Duration::from_secs(3), slow_started.notified())
            .await
            .unwrap();
        client.close().await.unwrap();
        assert!(matches!(
            timeout(Duration::from_secs(3), running)
                .await
                .unwrap()
                .unwrap(),
            Err(McpError::Indeterminate)
        ));
        assert!(matches!(
            client
                .call(
                    &revision,
                    "echo",
                    JsonObject::new(),
                    CancellationToken::new()
                )
                .await,
            Err(McpError::Closed)
        ));
    }
    #[tokio::test]
    async fn interrupting_one_call_reconnects_without_cancelling_a_surviving_call_on_the_retired_generation()
     {
        let fake = Fake::default();
        let server = stateful(fake.clone()).await;
        let base = tempdir().unwrap();
        let client = ServerSession::connect(
            ServerConfig::Http {
                url: server.url.clone(),
                token_env: None,
            },
            base.path(),
            ClientLimits::default(),
            Arc::new(EnvironmentCredentials),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let revision = client.snapshot().revision.clone();
        let (root_started, child_started, child_release) = {
            let state = fake.0.lock().unwrap();
            (
                state.root_started.clone(),
                state.child_started.clone(),
                state.child_release.clone(),
            )
        };
        let cancel = CancellationToken::new();
        let root_client = client.clone();
        let root_revision = revision.clone();
        let root_cancel = cancel.clone();
        let root = spawn(async move {
            root_client
                .call(
                    &root_revision,
                    "slow",
                    json!({"lane":"root"}).as_object().unwrap().clone(),
                    root_cancel,
                )
                .await
        });
        timeout(Duration::from_secs(3), root_started.notified())
            .await
            .unwrap();
        let child_client = client.clone();
        let child_revision = revision.clone();
        let child = spawn(async move {
            child_client
                .call(
                    &child_revision,
                    "echo",
                    json!({"lane":"child"}).as_object().unwrap().clone(),
                    CancellationToken::new(),
                )
                .await
        });
        timeout(Duration::from_secs(3), child_started.notified())
            .await
            .unwrap();
        cancel.cancel();
        assert!(matches!(
            timeout(Duration::from_secs(3), root)
                .await
                .unwrap()
                .unwrap(),
            Err(McpError::Indeterminate)
        ));
        client
            .call(
                &revision,
                "echo",
                json!({"lane":"replacement"}).as_object().unwrap().clone(),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert!(
            !child.is_finished(),
            "the surviving call is still waiting on its own server barrier"
        );
        child_release.notify_one();
        timeout(Duration::from_secs(3), child)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let generations = fake.0.lock().unwrap().generations.clone();
        let root = generations
            .iter()
            .find(|(lane, _)| lane == "root")
            .unwrap()
            .1;
        let child = generations
            .iter()
            .find(|(lane, _)| lane == "child")
            .unwrap()
            .1;
        let replacement = generations
            .iter()
            .find(|(lane, _)| lane == "replacement")
            .unwrap()
            .1;
        assert_eq!(root, child);
        assert_ne!(root, replacement);
        assert_eq!(
            generations
                .iter()
                .filter(|(lane, _)| lane == "root")
                .count(),
            1
        );
        client.close().await.unwrap();
    }
    #[test]
    fn equivalent_tool_order_has_one_contract_revision_and_no_schema_keywords_are_rewritten() {
        use crate::{Tool, discovery::snapshot};
        let schema=json!({"type":"object","$defs":{"choice":{"anyOf":[{"const":"one"},{"const":"two"}]}},"properties":{"input":{"$ref":"#/$defs/choice"}}}).as_object().unwrap().clone();
        let first = Tool::new("namespace:first", "First tool", schema.clone());
        let second = Tool::new("second", "Second tool", schema.clone());
        let one = snapshot(vec![first.clone(), second.clone()]).unwrap();
        let two = snapshot(vec![second, first]).unwrap();
        assert_eq!(one.revision, two.revision);
        assert_eq!(*one.find("namespace:first").unwrap().input_schema, schema);
    }
    #[tokio::test]
    async fn http_redirects_are_not_followed() {
        use axum::{Router, http::StatusCode, http::header::LOCATION, routing::any, serve};
        use tokio::net::TcpListener;
        let fake = Fake::default();
        let target = http(fake.clone(), false).await;
        let destination = target.url.clone();
        let app = Router::new().route(
            "/redirect",
            any(move || {
                let destination = destination.clone();
                async move { (StatusCode::TEMPORARY_REDIRECT, [(LOCATION, destination)]) }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/redirect", listener.local_addr().unwrap());
        let server = spawn(async move {
            serve(listener, app).await.unwrap();
        });
        let dir = tempdir().unwrap();
        let result = ServerSession::connect(
            ServerConfig::Http {
                url,
                token_env: None,
            },
            dir.path(),
            ClientLimits::default(),
            Arc::new(EnvironmentCredentials),
            CancellationToken::new(),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(fake.0.lock().unwrap().connections, 0);
        server.abort();
    }
}
