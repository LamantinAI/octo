//! `octo-connector-storage` — a durable object-store organ (env-as-tools).
//!
//! Where finished artifacts live, as opposed to the ephemeral `octo-code`
//! workspace: the cogitator promotes a result here by dispatching a command to
//! this connector's `id`. The backend is swappable behind [`StorageBackend`] —
//! a local directory now, an S3-compatible store later — so the command surface
//! is cloud-portable from day one.
//!
//! Commands (each replies with a correlated `<kind>.result`):
//! - `storage.put { key, content | content_base64 }` → `{ key, bytes }`
//! - `storage.get { key }` → `{ key, content | content_base64, encoding, bytes }`
//! - `storage.list { prefix? }` → `{ keys: [...] }`
//! - `storage.delete { key }` → `{ deleted: bool }`
//! - `storage.promote { workspace_path, key }` → `{ key, bytes }` (shelf a
//!   workspace file server-side, without round-tripping content through the model)
//! - `storage.checkout { key, workspace_path }` → `{ workspace_path, bytes }`
//!   (bring a stored object back into the workspace for editing)
//!
//! `promote`/`checkout` bridge the ephemeral `octo-code` workspace and durable
//! storage. The workspace root is the same one `octo-code` uses — configured via
//! the manifest `workspace` field, else `$OCTO_CODE_WORKSPACE`, else the default
//! `<tmp>/octo-code` — keeping the two faculties over one directory by config.

mod backend;

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use octo_core::{
    Connector, ConnectorCapabilities, ConnectorContext, ConnectorFactory, ConnectorId, Envelope,
    EventKind, FactoryContext, Filter, OctoResult, SubscribeOptions,
};
use serde_json::{json, Value};

pub use backend::{LocalStorage, StorageBackend, StorageError};

const PUT: &str = "storage.put";
const GET: &str = "storage.get";
const LIST: &str = "storage.list";
const DELETE: &str = "storage.delete";
const PROMOTE: &str = "storage.promote";
const CHECKOUT: &str = "storage.checkout";

const CATALOG: &str = "A durable object store, plus a bridge to the ephemeral code workspace. \
Dispatch a command envelope to this connector's id:
- storage.put { key, content } (or content_base64 for binary) -> { key, bytes }
- storage.get { key } -> { key, content | content_base64, encoding, bytes }
- storage.list { prefix? } -> { keys: [...] }
- storage.delete { key } -> { deleted: bool }
- storage.promote { workspace_path, key } -> shelf a workspace file to durable storage -> { key, bytes }
- storage.checkout { key, workspace_path } -> copy a stored object back into the workspace for editing -> { workspace_path, bytes }";

pub struct StorageConnector {
    id: ConnectorId,
    capabilities: ConnectorCapabilities,
    backend: Arc<dyn StorageBackend>,
    /// Explicit workspace root; when `None`, resolved from the environment.
    workspace: Option<PathBuf>,
}

impl StorageConnector {
    /// A storage organ bound to `backend`, addressable at `id`. The workspace
    /// root (for promote/checkout) is resolved from `$OCTO_CODE_WORKSPACE`.
    pub fn new(id: impl Into<String>, backend: Arc<dyn StorageBackend>) -> Arc<Self> {
        Self::build(id, backend, None)
    }

    /// As [`new`](Self::new), pinning the workspace root explicitly instead of
    /// reading it from the environment.
    pub fn with_workspace(
        id: impl Into<String>,
        backend: Arc<dyn StorageBackend>,
        workspace: impl Into<PathBuf>,
    ) -> Arc<Self> {
        Self::build(id, backend, Some(workspace.into()))
    }

    fn build(
        id: impl Into<String>,
        backend: Arc<dyn StorageBackend>,
        workspace: Option<PathBuf>,
    ) -> Arc<Self> {
        let capabilities = ConnectorCapabilities::bidirectional()
            .with_accept_kinds([
                EventKind::from_static(PUT),
                EventKind::from_static(GET),
                EventKind::from_static(LIST),
                EventKind::from_static(DELETE),
                EventKind::from_static(PROMOTE),
                EventKind::from_static(CHECKOUT),
            ])
            .with_description(CATALOG);
        Arc::new(Self { id: ConnectorId::new(id), capabilities, backend, workspace })
    }
}

#[async_trait]
impl Connector for StorageConnector {
    fn id(&self) -> &ConnectorId {
        &self.id
    }

    fn capabilities(&self) -> &ConnectorCapabilities {
        &self.capabilities
    }

    async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
        let mut cmds = ctx
            .subscribe(Filter::by_target(self.id.clone()), SubscribeOptions::default())
            .await?;
        tracing::info!(connector = %self.id, "storage ready");
        loop {
            tokio::select! {
                next = cmds.next() => match next {
                    Some(env) => self.handle(&env, &ctx).await,
                    None => return Ok(()),
                },
                _ = ctx.shutdown.cancelled() => return Ok(()),
            }
        }
    }
}

impl StorageConnector {
    async fn handle(&self, env: &Envelope, ctx: &ConnectorContext) {
        let params = env.payload_as::<Value>().cloned().unwrap_or(Value::Null);
        let kind = env.kind.as_str();
        let outcome = match kind {
            PUT => self.put(&params).await,
            GET => self.get(&params).await,
            LIST => self.list(&params).await,
            DELETE => self.delete(&params).await,
            PROMOTE => self.promote(&params).await,
            CHECKOUT => self.checkout(&params).await,
            _ => return, // not one of ours
        };
        let payload = outcome.unwrap_or_else(|e| json!({ "error": e }));
        let resp = Envelope::new(self.id.clone(), EventKind::new(format!("{kind}.result")), payload)
            .with_correlation(env.id);
        if let Err(e) = ctx.publish(resp).await {
            tracing::warn!(error = %e, "storage failed to publish result");
        }
    }

    async fn put(&self, params: &Value) -> Result<Value, String> {
        let key = str_field(params, "key")?;
        let bytes = if let Some(text) = params.get("content").and_then(Value::as_str) {
            text.as_bytes().to_vec()
        } else if let Some(b64) = params.get("content_base64").and_then(Value::as_str) {
            BASE64.decode(b64).map_err(|e| format!("bad content_base64: {e}"))?
        } else {
            return Err("provide `content` (text) or `content_base64` (binary)".into());
        };
        self.backend.put(key, &bytes).await.map_err(|e| e.to_string())?;
        Ok(json!({ "key": key, "bytes": bytes.len() }))
    }

    async fn get(&self, params: &Value) -> Result<Value, String> {
        let key = str_field(params, "key")?;
        let bytes = self.backend.get(key).await.map_err(|e| e.to_string())?;
        let out = match String::from_utf8(bytes) {
            Ok(text) => json!({ "key": key, "content": text, "encoding": "utf8", "bytes": text.len() }),
            Err(e) => {
                let raw = e.into_bytes();
                json!({
                    "key": key,
                    "content_base64": BASE64.encode(&raw),
                    "encoding": "base64",
                    "bytes": raw.len(),
                })
            }
        };
        Ok(out)
    }

    async fn list(&self, params: &Value) -> Result<Value, String> {
        let prefix = params.get("prefix").and_then(Value::as_str).unwrap_or("");
        let keys = self.backend.list(prefix).await.map_err(|e| e.to_string())?;
        Ok(json!({ "prefix": prefix, "keys": keys }))
    }

    async fn delete(&self, params: &Value) -> Result<Value, String> {
        let key = str_field(params, "key")?;
        let deleted = self.backend.delete(key).await.map_err(|e| e.to_string())?;
        Ok(json!({ "key": key, "deleted": deleted }))
    }

    /// Copy a workspace file into durable storage, server-side (no round-trip
    /// through the model's context).
    async fn promote(&self, params: &Value) -> Result<Value, String> {
        let workspace_path = str_field(params, "workspace_path")?;
        let key = str_field(params, "key")?;
        let root = self.workspace_root()?;
        let src = backend::resolve_within(&root, workspace_path).map_err(|e| e.to_string())?;
        let bytes = match std::fs::read(&src) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(format!("workspace file `{workspace_path}` not found"));
            }
            Err(e) => return Err(e.to_string()),
        };
        self.backend.put(key, &bytes).await.map_err(|e| e.to_string())?;
        Ok(json!({ "key": key, "workspace_path": workspace_path, "bytes": bytes.len() }))
    }

    /// Copy a stored object back into the workspace for editing.
    async fn checkout(&self, params: &Value) -> Result<Value, String> {
        let key = str_field(params, "key")?;
        let workspace_path = str_field(params, "workspace_path")?;
        let bytes = self.backend.get(key).await.map_err(|e| e.to_string())?;
        let root = self.workspace_root()?;
        let dest = backend::resolve_within(&root, workspace_path).map_err(|e| e.to_string())?;
        backend::write_atomic(&dest, &bytes).map_err(|e| e.to_string())?;
        Ok(json!({ "workspace_path": workspace_path, "key": key, "bytes": bytes.len() }))
    }

    /// The code workspace root: the pinned one, else `$OCTO_CODE_WORKSPACE`, else
    /// the `octo-code` default `<tmp>/octo-code`. Created if missing.
    fn workspace_root(&self) -> Result<PathBuf, String> {
        let root = self
            .workspace
            .clone()
            .or_else(|| std::env::var_os("OCTO_CODE_WORKSPACE").map(PathBuf::from))
            .unwrap_or_else(|| std::env::temp_dir().join("octo-code"));
        std::fs::create_dir_all(&root).map_err(|e| e.to_string())?;
        root.canonicalize().map_err(|e| e.to_string())
    }
}

fn str_field<'a>(params: &'a Value, field: &str) -> Result<&'a str, String> {
    params
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("missing string field `{field}`"))
}

// ── config-driven construction (`type = "storage"`) ─────────────────────────

/// [`ConnectorFactory`] for `type = "storage"`. Register once with
/// `Octo::builder().register_connector_type("storage", octo_connector_storage::factory())`.
pub struct StorageConnectorFactory;

impl StorageConnectorFactory {
    pub fn new() -> Self {
        Self
    }
}

impl Default for StorageConnectorFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectorFactory for StorageConnectorFactory {
    fn type_name(&self) -> &str {
        "storage"
    }

    fn create(
        &self,
        id: ConnectorId,
        config: &toml::Value,
        ctx: FactoryContext<'_>,
    ) -> Result<Arc<dyn Connector>, Box<dyn std::error::Error + Send + Sync>> {
        let table = config
            .get("connector")
            .ok_or("storage: manifest has no [connector] table")?;
        let backend_kind = table.get("backend").and_then(|v| v.as_str()).unwrap_or("local");
        let backend: Arc<dyn StorageBackend> = match backend_kind {
            "local" => {
                let root = table
                    .get("root")
                    .and_then(|v| v.as_str())
                    .ok_or("storage: local backend needs `root`")?;
                // Resolve a relative root against the manifest's directory.
                let path = ctx.base_dir.join(root);
                Arc::new(LocalStorage::new(path)?)
            }
            other => return Err(format!("storage: unknown backend `{other}`").into()),
        };
        // Optional workspace root for promote/checkout; relative to the manifest.
        // When absent, the connector falls back to $OCTO_CODE_WORKSPACE at runtime.
        match table.get("workspace").and_then(|v| v.as_str()) {
            Some(ws) => {
                Ok(StorageConnector::with_workspace(id.as_str(), backend, ctx.base_dir.join(ws)))
            }
            None => Ok(StorageConnector::new(id.as_str(), backend)),
        }
    }
}

/// Convenience factory handle for registration.
pub fn factory() -> Arc<dyn ConnectorFactory> {
    Arc::new(StorageConnectorFactory::new())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use octo_core::{EventBus, InProcessBus};
    use tokio_util::sync::CancellationToken;

    use super::*;

    fn cmd(kind: &str, payload: Value) -> Envelope {
        Envelope::new(ConnectorId::new("test-driver"), EventKind::new(kind), payload)
            .with_target(ConnectorId::new("storage"))
    }

    #[tokio::test]
    async fn through_the_connector() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = Arc::new(LocalStorage::new(tmp.path()).unwrap());
        let bus = Arc::new(InProcessBus::new(64));
        let shutdown = CancellationToken::new();
        let ctx = ConnectorContext::new(shutdown.clone(), Arc::clone(&bus));
        let connector = StorageConnector::new("storage", backend);
        let handle = tokio::spawn(connector.run(ctx));
        tokio::time::sleep(Duration::from_millis(100)).await;

        let put = bus
            .publish_and_await_response(
                cmd(PUT, json!({ "key": "reports/x.md", "content": "hello" })),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        assert_eq!(put.payload_as::<Value>().unwrap()["bytes"], 5);

        let got = bus
            .publish_and_await_response(cmd(GET, json!({ "key": "reports/x.md" })), Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(got.payload_as::<Value>().unwrap()["content"], "hello");

        let listed = bus
            .publish_and_await_response(cmd(LIST, json!({ "prefix": "reports/" })), Duration::from_secs(5))
            .await
            .unwrap();
        let keys = listed.payload_as::<Value>().unwrap()["keys"].clone();
        assert_eq!(keys, json!(["reports/x.md"]));

        let deleted = bus
            .publish_and_await_response(cmd(DELETE, json!({ "key": "reports/x.md" })), Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(deleted.payload_as::<Value>().unwrap()["deleted"], true);

        shutdown.cancel();
        let _ = handle.await;
    }

    #[tokio::test]
    async fn binary_roundtrips_via_base64() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = Arc::new(LocalStorage::new(tmp.path()).unwrap());
        let connector = StorageConnector::new("storage", backend);

        let raw = vec![0u8, 159, 146, 150]; // invalid UTF-8
        let put = connector
            .put(&json!({ "key": "blob.bin", "content_base64": BASE64.encode(&raw) }))
            .await
            .unwrap();
        assert_eq!(put["bytes"], 4);

        let got = connector.get(&json!({ "key": "blob.bin" })).await.unwrap();
        assert_eq!(got["encoding"], "base64");
        assert_eq!(got["content_base64"], BASE64.encode(&raw));
    }

    #[tokio::test]
    async fn promote_and_checkout_bridge_the_workspace() {
        let storage_dir = tempfile::tempdir().unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let backend = Arc::new(LocalStorage::new(storage_dir.path()).unwrap());
        let connector =
            StorageConnector::with_workspace("storage", backend, workspace.path().to_path_buf());

        // Simulate an octo-code write into the workspace.
        std::fs::write(workspace.path().join("draft.md"), b"work in progress").unwrap();

        // promote: workspace -> durable storage.
        let p = connector
            .promote(&json!({ "workspace_path": "draft.md", "key": "reports/draft.md" }))
            .await
            .unwrap();
        assert_eq!(p["bytes"], 16);
        assert_eq!(connector.backend.get("reports/draft.md").await.unwrap(), b"work in progress");

        // checkout: durable storage -> a fresh workspace path.
        let c = connector
            .checkout(&json!({ "key": "reports/draft.md", "workspace_path": "editing/draft.md" }))
            .await
            .unwrap();
        assert_eq!(c["bytes"], 16);
        let back = std::fs::read(workspace.path().join("editing/draft.md")).unwrap();
        assert_eq!(back, b"work in progress");
    }

    #[tokio::test]
    async fn promote_missing_workspace_file_errors() {
        let storage_dir = tempfile::tempdir().unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let backend = Arc::new(LocalStorage::new(storage_dir.path()).unwrap());
        let connector =
            StorageConnector::with_workspace("storage", backend, workspace.path().to_path_buf());
        let e = connector
            .promote(&json!({ "workspace_path": "nope.md", "key": "k" }))
            .await
            .unwrap_err();
        assert!(e.contains("not found"));
    }
}
