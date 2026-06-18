//! Tests for the config loader: `OctoBuilder::register_connector_type` +
//! `from_config_file` bringing up dyn connectors from an `octo.toml` manifest
//! (steps 1–4 of the config-loader MVP).
//!
//! - `config_driven_round_trip` — the full pipeline end-to-end via a fixture
//!   manifest (base_url → local mock through `${env.…}`): loader → factory →
//!   dyn connector → HTTP → bus.
//! - `loads_real_config_manifest` — the *shipped* `config/octo.toml` parses and
//!   wires the petstore connector (no network).
//! - error cases: unknown `type` (no factory), duplicate `id`.

mod mock_http;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use octo_connector_http::HttpConnectorFactory;
use octo_core::{
    ConfigError, Connector, ConnectorCapabilities, ConnectorContext, ConnectorId, Envelope,
    EventKind, Filter, Octo, OctoResult, SubscribeOptions, TrailActor,
};
use serde_json::{json, Value};

use mock_http::{MockServer, Route};

fn real_config() -> String {
    format!("{}/../../config/octo.toml", env!("CARGO_MANIFEST_DIR"))
}

fn fixture_config() -> String {
    format!("{}/tests/fixtures/octo.toml", env!("CARGO_MANIFEST_DIR"))
}

fn router_fixture() -> String {
    format!(
        "{}/tests/fixtures/router_only/octo.toml",
        env!("CARGO_MANIFEST_DIR")
    )
}

/// A no-proxy HTTP client (the sandbox sets HTTP_PROXY).
fn no_proxy_factory() -> Arc<HttpConnectorFactory> {
    Arc::new(HttpConnectorFactory::with_client(
        reqwest::Client::builder().no_proxy().build().unwrap(),
    ))
}

// ─── Test 1: end-to-end through config ──────────────────────────────────────

struct Agent {
    id: ConnectorId,
    capabilities: ConnectorCapabilities,
    target: ConnectorId,
    kinds: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl Connector for Agent {
    fn id(&self) -> &ConnectorId {
        &self.id
    }
    fn capabilities(&self) -> &ConnectorCapabilities {
        &self.capabilities
    }
    async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
        tokio::time::sleep(Duration::from_millis(150)).await;
        let result = self.scenario(&ctx).await;
        ctx.shutdown.cancel();
        result
    }
}

impl Agent {
    fn cmd(&self, kind: &str, payload: Value) -> Envelope {
        Envelope::new(self.id.clone(), EventKind::new(kind.to_string()), payload)
            .with_target(self.target.clone())
    }

    async fn scenario(&self, ctx: &ConnectorContext) -> OctoResult<()> {
        for (kind, payload) in [
            ("petstore.cmd.find_pets_by_status", json!({ "status": "available" })),
            ("petstore.cmd.add_pet", json!({ "name": "octo-cfg-pup", "photoUrls": [] })),
            ("petstore.cmd.delete_pet", json!({ "id": 777 })),
        ] {
            let resp = ctx
                .publish_and_await_response(self.cmd(kind, payload), Duration::from_secs(5))
                .await?;
            self.kinds.lock().unwrap().push(resp.kind.as_str().to_string());
        }
        Ok(())
    }
}

fn routes() -> Vec<Route> {
    vec![
        Route::new("GET", "/pet/findByStatus", 200, |_| {
            json!([{ "id": 10, "name": "doggie", "photoUrls": [] }]).to_string()
        }),
        Route::new("POST", "/pet", 200, |body| {
            let mut pet: Value = serde_json::from_str(body).unwrap_or(json!({}));
            pet["id"] = json!(777);
            pet.to_string()
        }),
        Route::new("DELETE", "/pet/777", 200, |_| String::new()),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn config_driven_round_trip() {
    let server = MockServer::start(routes()).await;
    // base_url in the fixture manifest is `${env.OCTO_HTTP_TEST_BASE}`.
    // SAFETY: set before the connector is built; this var is unique to this test.
    unsafe {
        std::env::set_var("OCTO_HTTP_TEST_BASE", server.base_url());
    }

    let kinds = Arc::new(Mutex::new(Vec::new()));
    let agent = Arc::new(Agent {
        id: ConnectorId::new("agent"),
        capabilities: ConnectorCapabilities::output_only(),
        target: ConnectorId::new("petstore"),
        kinds: Arc::clone(&kinds),
    });

    let octo = Octo::builder()
        .register_connector_type("http", no_proxy_factory())
        .from_config_file(fixture_config())
        .expect("config loads")
        .add_connector(agent)
        .build();

    // The petstore connector came from config; the agent was added in code.
    assert!(octo.connector_ids().contains(&"petstore"));
    assert_eq!(octo.connector_count(), 2);

    octo.run().await.unwrap();

    assert_eq!(
        *kinds.lock().unwrap(),
        vec![
            "petstore.event.pets_found",
            "petstore.event.pet_added",
            "petstore.event.pet_deleted",
        ],
        "every config-driven command produced its response kind"
    );
}

// ─── Test 2: the shipped config/ manifest parses & wires ────────────────────

#[test]
fn loads_real_config_manifest() {
    let octo = Octo::builder()
        .register_connector_type("http", no_proxy_factory())
        .from_config_file(real_config())
        .expect("real config/octo.toml loads")
        .build();

    assert_eq!(octo.connector_ids(), vec!["petstore"]);
}

// ─── Test 3: unknown type without a registered factory ──────────────────────

#[test]
fn unknown_type_without_factory_errors() {
    // No `register_connector_type("http", …)` — the petstore manifest's
    // `type = "http"` cannot be resolved.
    let err = match Octo::builder().from_config_file(real_config()) {
        Err(e) => e,
        Ok(_) => panic!("expected error without an http factory"),
    };
    assert!(
        matches!(err, ConfigError::UnknownConnectorType { ref type_name, .. } if type_name == "http"),
        "got: {err:?}"
    );
}

// ─── Test 4: duplicate connector id across code + config ────────────────────

struct Dummy {
    id: ConnectorId,
    capabilities: ConnectorCapabilities,
}

#[async_trait]
impl Connector for Dummy {
    fn id(&self) -> &ConnectorId {
        &self.id
    }
    fn capabilities(&self) -> &ConnectorCapabilities {
        &self.capabilities
    }
    async fn run(self: Arc<Self>, _ctx: ConnectorContext) -> OctoResult<()> {
        Ok(())
    }
}

// ─── Test 5: a [router] table loaded from octo.toml actually routes ─────────

struct Emitter {
    id: ConnectorId,
    capabilities: ConnectorCapabilities,
}

#[async_trait]
impl Connector for Emitter {
    fn id(&self) -> &ConnectorId {
        &self.id
    }
    fn capabilities(&self) -> &ConnectorCapabilities {
        &self.capabilities
    }
    async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
        // Emit a raw incident with NO target; the config router adds one.
        ctx.publish(Envelope::new(
            self.id.clone(),
            EventKind::from_static("vision.incident.detected"),
            7i32,
        ))
        .await?;
        // Give the router time to process and emit before shutdown.
        tokio::time::sleep(Duration::from_millis(150)).await;
        ctx.shutdown.cancel();
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_loaded_from_toml_routes_envelope() {
    let octo = Octo::builder()
        .from_config_file(router_fixture())
        .expect("router-only config loads")
        .add_connector(Arc::new(Emitter {
            id: ConnectorId::new("sensor"),
            capabilities: ConnectorCapabilities::input_only(),
        }))
        .build();

    // Router came from the TOML table (default id "config").
    assert_eq!(octo.router_id(), Some("config"));

    let alerter = ConnectorId::new("alerter");
    let mut sub = octo
        .subscribe(Filter::by_target(alerter.clone()), SubscribeOptions::default())
        .await
        .unwrap();
    let received = tokio::spawn(async move { sub.next().await });

    octo.run().await.unwrap();

    let env = received
        .await
        .unwrap()
        .expect("config-loaded route should deliver to the alerter");
    assert_eq!(env.kind.as_str(), "alert.text", "override_kind applied");
    assert_eq!(env.target.as_ref(), Some(&alerter));
    assert_eq!(env.payload_as::<i32>(), Some(&7), "copy_payload carried the original");
    assert!(
        env.trail.iter().any(|t| matches!(
            &t.actor,
            TrailActor::Reflex(rid) if rid.as_str() == "incident_to_alerter"
        )),
        "trail records the routing rule"
    );
}

#[test]
fn duplicate_id_errors() {
    let dummy = Arc::new(Dummy {
        id: ConnectorId::new("petstore"), // collides with the config connector
        capabilities: ConnectorCapabilities::output_only(),
    });

    let err = match Octo::builder()
        .register_connector_type("http", no_proxy_factory())
        .add_connector(dummy)
        .from_config_file(real_config())
    {
        Err(e) => e,
        Ok(_) => panic!("expected duplicate-id error"),
    };
    assert!(
        matches!(err, ConfigError::DuplicateConnectorId(ref id) if id.as_str() == "petstore"),
        "got: {err:?}"
    );
}
