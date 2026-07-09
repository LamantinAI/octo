//! `octo-connector-caldav` — a generic CalDAV connector (RFC 4791).
//!
//! One crate, many calendars: a configured instance per account (Yandex,
//! Fastmail, Nextcloud, iCloud, Google), each an env-as-tools organ Albert
//! reaches by dispatching a command to the instance's `id`. Auth via
//! [`octo_http_auth`] (basic app-password now; oauth2 for Google planned).
//!
//! The collection URL is either given explicitly or discovered from a server
//! root ([`CollectionSource`]), so a manifest can be just `base_url` + login +
//! password.
//!
//! Commands (each replies with a correlated `<kind>.result`):
//! - `calendar.list_events { from, to }` → `{ events: [...] }`
//! - `calendar.create_event { title, start, end, description?, location? }` → `{ uid }`
//! - `calendar.delete_event { uid }` → `{ deleted }`

mod dav;

use std::sync::Arc;

use async_trait::async_trait;
use octo_core::{
    Connector, ConnectorCapabilities, ConnectorContext, ConnectorFactory, ConnectorId, Envelope,
    EventId, EventKind, FactoryContext, Filter, OctoResult, SubscribeOptions,
};
use octo_http_auth::{AuthConfig, HttpAuth};
use serde_json::{json, Value};
use tokio::sync::OnceCell;

pub use dav::DavError;

const LIST: &str = "calendar.list_events";
const CREATE: &str = "calendar.create_event";
const DELETE: &str = "calendar.delete_event";

const CATALOG: &str = "A calendar (CalDAV). Dispatch a command envelope to this connector's id:
- calendar.list_events { from: <RFC3339>, to: <RFC3339> } -> { events: [{ uid, title, start, end, location? }] }
- calendar.create_event { title, start: <RFC3339>, end: <RFC3339>, description?, location? } -> { uid }
- calendar.delete_event { uid } -> { deleted: bool }";

/// Where the calendar collection URL comes from.
///
/// A CalDAV collection is an opaque, server-assigned URL (Yandex hands out
/// `.../events-37856064/`), so a user can't reasonably write it by hand.
/// [`Discover`](CollectionSource::Discover) resolves it from a server root the
/// way desktop clients do (PROPFIND principal -> home-set -> pick calendar), so
/// a manifest need only carry `base_url` + login + password.
#[derive(Debug, Clone)]
pub enum CollectionSource {
    /// A known collection URL — used verbatim, no discovery.
    Explicit(String),
    /// Discover from a server root, optionally selecting a calendar by display name.
    Discover { base_url: String, calendar: Option<String> },
}

pub struct CaldavConnector {
    id: ConnectorId,
    capabilities: ConnectorCapabilities,
    source: CollectionSource,
    /// The resolved collection URL, discovered lazily on first use.
    resolved: OnceCell<String>,
    auth: HttpAuth,
    client: reqwest::Client,
}

impl CaldavConnector {
    /// A calendar instance bound to a known CalDAV `collection` URL, authenticated
    /// via `auth`. Uses a default HTTP client.
    pub fn new(id: impl Into<String>, collection: impl Into<String>, auth: AuthConfig) -> Arc<Self> {
        Self::from_source(id, CollectionSource::Explicit(collection.into()), auth, reqwest::Client::new())
    }

    /// A calendar instance that discovers its collection URL from a server root.
    /// If `calendar` is set, the matching display name is chosen; otherwise the
    /// first VEVENT calendar.
    pub fn discovering(
        id: impl Into<String>,
        base_url: impl Into<String>,
        calendar: Option<String>,
        auth: AuthConfig,
    ) -> Arc<Self> {
        let source = CollectionSource::Discover { base_url: base_url.into(), calendar };
        Self::from_source(id, source, auth, reqwest::Client::new())
    }

    /// As [`new`](Self::new), sharing a caller-supplied HTTP client (pool reuse /
    /// a proxy-free client in tests).
    pub fn with_client(
        id: impl Into<String>,
        collection: impl Into<String>,
        auth: AuthConfig,
        client: reqwest::Client,
    ) -> Arc<Self> {
        Self::from_source(id, CollectionSource::Explicit(collection.into()), auth, client)
    }

    /// The general constructor: a [`CollectionSource`] and a shared HTTP client.
    pub fn from_source(
        id: impl Into<String>,
        source: CollectionSource,
        auth: AuthConfig,
        client: reqwest::Client,
    ) -> Arc<Self> {
        let capabilities = ConnectorCapabilities::bidirectional()
            .with_accept_kinds([
                EventKind::from_static(LIST),
                EventKind::from_static(CREATE),
                EventKind::from_static(DELETE),
            ])
            .with_description(CATALOG);
        // An explicit collection is already resolved; discovery fills this later.
        let resolved = OnceCell::new();
        if let CollectionSource::Explicit(url) = &source {
            resolved.set(url.clone()).ok();
        }
        Arc::new(Self {
            id: ConnectorId::new(id),
            capabilities,
            source,
            resolved,
            // The OAuth2 refresh (when configured) shares this connector's client.
            auth: HttpAuth::with_client(auth, client.clone()),
            client,
        })
    }

    /// The collection URL, discovering it once on first use.
    async fn resolve_collection(&self) -> Result<&str, DavError> {
        self.resolved
            .get_or_try_init(|| async {
                match &self.source {
                    CollectionSource::Explicit(url) => Ok(url.clone()),
                    CollectionSource::Discover { base_url, calendar } => {
                        let url = dav::discover_collection(
                            &self.client,
                            base_url,
                            &self.auth,
                            calendar.as_deref(),
                        )
                        .await?;
                        tracing::info!(connector = %self.id, %url, "caldav discovered collection");
                        Ok(url)
                    }
                }
            })
            .await
            .map(String::as_str)
    }
}

#[async_trait]
impl Connector for CaldavConnector {
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
        tracing::info!(connector = %self.id, "caldav ready");
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

impl CaldavConnector {
    async fn handle(&self, env: &Envelope, ctx: &ConnectorContext) {
        let params = env.payload_as::<Value>().cloned().unwrap_or(Value::Null);
        let kind = env.kind.as_str();
        if !matches!(kind, LIST | CREATE | DELETE) {
            return; // not one of ours
        }
        let outcome = match self.resolve_collection().await {
            Ok(collection) => match kind {
                LIST => {
                    let from = params.get("from").and_then(Value::as_str).unwrap_or_default();
                    let to = params.get("to").and_then(Value::as_str).unwrap_or_default();
                    dav::list_events(&self.client, collection, &self.auth, from, to).await
                }
                CREATE => {
                    let uid = EventId::new().to_string();
                    dav::create_event(&self.client, collection, &self.auth, &params, &uid).await
                }
                DELETE => dav::delete_event(&self.client, collection, &self.auth, &params).await,
                _ => unreachable!(),
            },
            Err(e) => Err(e),
        };
        let payload = match outcome {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(kind, error = %e, "caldav command failed");
                json!({ "error": e.to_string() })
            }
        };
        let resp = Envelope::new(self.id.clone(), EventKind::new(format!("{kind}.result")), payload)
            .with_correlation(env.id);
        if let Err(e) = ctx.publish(resp).await {
            tracing::warn!(error = %e, "caldav failed to publish result");
        }
    }
}

// ── config-driven construction (`type = "caldav"`) ──────────────────────────

/// [`ConnectorFactory`] for `type = "caldav"`. Register once with
/// `Octo::builder().register_connector_type("caldav", octo_connector_caldav::factory())`;
/// each manifest becomes one calendar instance.
pub struct CaldavConnectorFactory {
    client: reqwest::Client,
}

impl CaldavConnectorFactory {
    pub fn new() -> Self {
        Self { client: reqwest::Client::new() }
    }
}

impl Default for CaldavConnectorFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectorFactory for CaldavConnectorFactory {
    fn type_name(&self) -> &str {
        "caldav"
    }

    fn create(
        &self,
        id: ConnectorId,
        config: &toml::Value,
        _ctx: FactoryContext<'_>,
    ) -> Result<Arc<dyn Connector>, Box<dyn std::error::Error + Send + Sync>> {
        let table = config
            .get("connector")
            .ok_or("caldav: manifest has no [connector] table")?;
        // Either an explicit `collection` URL, or a `base_url` to discover from
        // (optionally narrowed by `calendar` display name).
        let source = match table.get("collection").and_then(|v| v.as_str()) {
            Some(collection) => CollectionSource::Explicit(collection.to_string()),
            None => {
                let base_url = table
                    .get("base_url")
                    .and_then(|v| v.as_str())
                    .ok_or("caldav: [connector] needs either `collection` or `base_url`")?
                    .to_string();
                let calendar = table.get("calendar").and_then(|v| v.as_str()).map(String::from);
                CollectionSource::Discover { base_url, calendar }
            }
        };
        // AuthConfig reads `auth`/`login`/`password_env` from the same table
        // (extra keys like id/type/collection/base_url are ignored).
        let auth: AuthConfig = table.clone().try_into()?;
        Ok(CaldavConnector::from_source(id.as_str(), source, auth, self.client.clone()))
    }
}

/// Convenience factory handle for registration.
pub fn factory() -> Arc<dyn ConnectorFactory> {
    Arc::new(CaldavConnectorFactory::new())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use octo_core::{EventBus, InProcessBus};
    use octo_http_auth::AuthConfig;
    use tokio_util::sync::CancellationToken;

    use super::*;

    fn cmd(target: &str, kind: &str, payload: Value) -> Envelope {
        Envelope::new(ConnectorId::new("test-driver"), EventKind::new(kind), payload)
            .with_target(ConnectorId::new(target))
    }

    /// Live end-to-end **through the connector**: spin it up on a bus, dispatch
    /// create -> list -> delete envelopes, and read the correlated results. Same
    /// env as the dav live tests; ignored by default.
    #[tokio::test]
    #[ignore]
    async fn live_via_connector() {
        let login = std::env::var("OCTO_TEST_CALDAV_LOGIN").expect("OCTO_TEST_CALDAV_LOGIN");
        let collection =
            std::env::var("OCTO_TEST_CALDAV_COLLECTION").expect("OCTO_TEST_CALDAV_COLLECTION");
        let auth = AuthConfig::Basic { login, password_env: "OCTO_YANDEX_APP_PASSWORD".into() };

        let bus = Arc::new(InProcessBus::new(64));
        let shutdown = CancellationToken::new();
        let ctx = ConnectorContext::new(shutdown.clone(), Arc::clone(&bus));
        let connector = CaldavConnector::new("calendar", collection, auth);
        let handle = tokio::spawn(connector.run(ctx));
        // Let the connector register its by_target subscription before we publish.
        tokio::time::sleep(Duration::from_millis(250)).await;

        // create
        let created = bus
            .publish_and_await_response(
                cmd(
                    "calendar",
                    CREATE,
                    json!({ "title": "Via connector", "start": "2026-07-02T09:00:00Z", "end": "2026-07-02T09:30:00Z" }),
                ),
                Duration::from_secs(20),
            )
            .await
            .expect("create result");
        let uid = created
            .payload_as::<Value>()
            .and_then(|v| v.get("uid"))
            .and_then(Value::as_str)
            .expect("uid in create result")
            .to_string();
        println!("connector create -> uid {uid}");

        // list
        let listed = bus
            .publish_and_await_response(
                cmd("calendar", LIST, json!({ "from": "2026-07-01T00:00:00Z", "to": "2026-07-03T00:00:00Z" })),
                Duration::from_secs(20),
            )
            .await
            .expect("list result");
        let listed = listed.payload_as::<Value>().cloned().unwrap_or(Value::Null);
        println!("connector list -> {}", serde_json::to_string_pretty(&listed).unwrap());
        let found = listed["events"]
            .as_array()
            .map(|a| a.iter().any(|e| e["title"] == "Via connector"))
            .unwrap_or(false);

        // cleanup
        let _ = bus
            .publish_and_await_response(
                cmd("calendar", DELETE, json!({ "uid": uid })),
                Duration::from_secs(20),
            )
            .await;

        shutdown.cancel();
        let _ = handle.await;
        assert!(found, "the event created via the connector should list back");
    }

    /// Live end-to-end through a **discovering** connector: configured with only a
    /// `base_url`, it resolves its collection on first command, then create -> list
    /// -> delete. Same env as `live_via_connector` plus `OCTO_TEST_CALDAV_BASE_URL`.
    #[tokio::test]
    #[ignore]
    async fn live_via_discovering_connector() {
        let login = std::env::var("OCTO_TEST_CALDAV_LOGIN").expect("OCTO_TEST_CALDAV_LOGIN");
        let base_url = std::env::var("OCTO_TEST_CALDAV_BASE_URL").expect("OCTO_TEST_CALDAV_BASE_URL");
        let calendar = std::env::var("OCTO_TEST_CALDAV_CALENDAR").ok();
        let auth = AuthConfig::Basic { login, password_env: "OCTO_YANDEX_APP_PASSWORD".into() };

        let bus = Arc::new(InProcessBus::new(64));
        let shutdown = CancellationToken::new();
        let ctx = ConnectorContext::new(shutdown.clone(), Arc::clone(&bus));
        let connector = CaldavConnector::discovering("calendar", base_url, calendar, auth);
        let handle = tokio::spawn(connector.run(ctx));
        tokio::time::sleep(Duration::from_millis(250)).await;

        let created = bus
            .publish_and_await_response(
                cmd(
                    "calendar",
                    CREATE,
                    json!({ "title": "Discovered", "start": "2026-07-04T09:00:00Z", "end": "2026-07-04T09:30:00Z" }),
                ),
                Duration::from_secs(20),
            )
            .await
            .expect("create result");
        let uid = created
            .payload_as::<Value>()
            .and_then(|v| v.get("uid"))
            .and_then(Value::as_str)
            .expect("uid in create result")
            .to_string();
        println!("discovering connector create -> uid {uid}");

        let listed = bus
            .publish_and_await_response(
                cmd("calendar", LIST, json!({ "from": "2026-07-03T00:00:00Z", "to": "2026-07-05T00:00:00Z" })),
                Duration::from_secs(20),
            )
            .await
            .expect("list result");
        let listed = listed.payload_as::<Value>().cloned().unwrap_or(Value::Null);
        let found = listed["events"]
            .as_array()
            .map(|a| a.iter().any(|e| e["title"] == "Discovered"))
            .unwrap_or(false);

        let _ = bus
            .publish_and_await_response(
                cmd("calendar", DELETE, json!({ "uid": uid })),
                Duration::from_secs(20),
            )
            .await;

        shutdown.cancel();
        let _ = handle.await;
        assert!(found, "the event created via the discovering connector should list back");
    }
}
