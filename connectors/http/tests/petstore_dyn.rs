//! End-to-end test for the dynamic HTTP connector, driven by the real
//! `config/connectors/petstore/petstore.toml` manifest against a local,
//! deterministic mock server (no live Petstore, no extra dependencies).
//!
//! Validates the questions `petstore_case.md` set out to close:
//! 1. a multi-route API described in TOML actually drives HTTP calls;
//! 2. JSONPath path/query params resolve from `serde_json::Value` payloads;
//! 3. model schemas register in `PayloadRegistry` (mixed with statically-typed
//!    connectors — see `registry_mixes_dyn_value_with_static_types`);
//! 4. correlation via `publish_and_await_response` is ergonomic on the caller;
//! 5. the common `petstore.event.error` carries the HTTP failure.

mod mock_http;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use octo_connector_http::HttpConnector;
use octo_core::{
    Connector, ConnectorCapabilities, ConnectorContext, ConnectorId, Envelope, EventKind, Octo,
    OctoResult, PayloadRegistry,
};
use serde_json::{json, Value};

use mock_http::{MockServer, Route};

const PETSTORE_ID: &str = "petstore";
const CALL_TIMEOUT: Duration = Duration::from_secs(5);

fn manifest_path() -> String {
    format!(
        "{}/../../config/connectors/petstore/petstore.toml",
        env!("CARGO_MANIFEST_DIR")
    )
}

#[derive(Default, Debug)]
struct Outcomes {
    find_kind: Option<String>,
    found_count: Option<usize>,
    fetch_kind: Option<String>,
    fetched_name: Option<String>,
    add_kind: Option<String>,
    added_id: Option<i64>,
    delete_kind: Option<String>,
    deleted_payload_is_null: Option<bool>,
    error_kind: Option<String>,
    error_status: Option<u16>,
}

struct Agent {
    id: ConnectorId,
    capabilities: ConnectorCapabilities,
    target: ConnectorId,
    out: Arc<Mutex<Outcomes>>,
}

impl Agent {
    fn new(out: Arc<Mutex<Outcomes>>) -> Arc<Self> {
        Arc::new(Self {
            id: ConnectorId::new("agent"),
            capabilities: ConnectorCapabilities::output_only(),
            target: ConnectorId::new(PETSTORE_ID),
            out,
        })
    }

    fn cmd(&self, kind: &str, payload: Value) -> Envelope {
        Envelope::new(self.id.clone(), EventKind::new(kind.to_string()), payload)
            .with_target(self.target.clone())
    }
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
        // Let the target connector register its subscription before we publish
        // (broadcast bus drops messages with no live receiver). Same warmup the
        // router integration test uses.
        tokio::time::sleep(Duration::from_millis(150)).await;
        let result = self.scenario(&ctx).await;
        // Always signal shutdown so a mid-scenario error surfaces as a failed
        // assertion below rather than hanging the runtime.
        ctx.shutdown.cancel();
        result
    }
}

impl Agent {
    async fn scenario(&self, ctx: &ConnectorContext) -> OctoResult<()> {
        // 1. find_pets_by_status(available) — GET with a query param.
        let resp = ctx
            .publish_and_await_response(
                self.cmd("petstore.cmd.find_pets_by_status", json!({ "status": "available" })),
                CALL_TIMEOUT,
            )
            .await?;
        let find_kind = resp.kind.as_str().to_string();
        let found_count = resp
            .payload_as::<Value>()
            .and_then(|v| v.as_array())
            .map(|a| a.len());
        let sample_id = resp
            .payload_as::<Value>()
            .and_then(|v| v.get(0))
            .and_then(|p| p["id"].as_i64());
        {
            let mut out = self.out.lock().unwrap();
            out.find_kind = Some(find_kind);
            out.found_count = found_count;
        }

        // 2. fetch_pet(sample_id) — GET with a path param.
        if let Some(id) = sample_id {
            let resp = ctx
                .publish_and_await_response(
                    self.cmd("petstore.cmd.fetch_pet", json!({ "id": id })),
                    CALL_TIMEOUT,
                )
                .await?;
            let fetch_kind = resp.kind.as_str().to_string();
            let name = resp
                .payload_as::<Value>()
                .and_then(|v| v["name"].as_str().map(str::to_string));
            let mut out = self.out.lock().unwrap();
            out.fetch_kind = Some(fetch_kind);
            out.fetched_name = name;
        }

        // 3. add_pet — POST with a JSON body built from the payload.
        let resp = ctx
            .publish_and_await_response(
                self.cmd(
                    "petstore.cmd.add_pet",
                    json!({ "name": "octo-pup", "photoUrls": [], "status": "available" }),
                ),
                CALL_TIMEOUT,
            )
            .await?;
        let add_kind = resp.kind.as_str().to_string();
        let added_id = resp.payload_as::<Value>().and_then(|v| v["id"].as_i64());
        {
            let mut out = self.out.lock().unwrap();
            out.add_kind = Some(add_kind);
            out.added_id = added_id;
        }

        // 4. delete_pet — DELETE, no response body → payload Null.
        if let Some(id) = added_id {
            let resp = ctx
                .publish_and_await_response(
                    self.cmd("petstore.cmd.delete_pet", json!({ "id": id })),
                    CALL_TIMEOUT,
                )
                .await?;
            let delete_kind = resp.kind.as_str().to_string();
            let is_null = resp.payload_as::<Value>() == Some(&Value::Null);
            let mut out = self.out.lock().unwrap();
            out.delete_kind = Some(delete_kind);
            out.deleted_payload_is_null = Some(is_null);
        }

        // 5. fetch a missing pet → server 500 → petstore.event.error.
        let resp = ctx
            .publish_and_await_response(
                self.cmd("petstore.cmd.fetch_pet", json!({ "id": 999 })),
                CALL_TIMEOUT,
            )
            .await?;
        let error_kind = resp.kind.as_str().to_string();
        let error_status = resp
            .payload_as::<Value>()
            .and_then(|v| v["http_status"].as_u64().map(|n| n as u16));
        {
            let mut out = self.out.lock().unwrap();
            out.error_kind = Some(error_kind);
            out.error_status = error_status;
        }

        Ok(())
    }
}

fn petstore_routes() -> Vec<Route> {
    vec![
        Route::new("GET", "/pet/findByStatus", 200, |_| {
            json!([
                { "id": 10, "name": "doggie", "photoUrls": [], "status": "available" },
                { "id": 11, "name": "kitty",  "photoUrls": [], "status": "available" }
            ])
            .to_string()
        }),
        Route::new("GET", "/pet/10", 200, |_| {
            json!({ "id": 10, "name": "doggie", "photoUrls": [], "status": "available" }).to_string()
        }),
        Route::new("GET", "/pet/999", 500, |_| "boom".to_string()),
        Route::new("POST", "/pet", 200, |body| {
            // Echo the posted pet back with a server-assigned id.
            let mut pet: Value = serde_json::from_str(body).unwrap_or(json!({}));
            pet["id"] = json!(777);
            pet.to_string()
        }),
        Route::new("DELETE", "/pet/777", 200, |_| String::new()),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn petstore_dyn_round_trip_through_bus() {
    let server = MockServer::start(petstore_routes()).await;

    // Load the real manifest, then point base_url at the mock server.
    let mut spec = octo_connector_http::HttpSpec::from_toml_file(manifest_path())
        .expect("manifest parses");
    spec.base_url = server.base_url();

    // The sandbox sets HTTP_PROXY; bypass it so requests reach the local mock.
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let connector = HttpConnector::with_client(spec, client);

    // Mixed registry: dyn Value kinds + model schemas, validated on the bus.
    let registry = Arc::new(connector.register_payloads(PayloadRegistry::new()));

    let out = Arc::new(Mutex::new(Outcomes::default()));
    let octo = Octo::builder()
        .bus_capacity(64)
        .payload_registry(registry)
        .add_connector(connector)
        .add_connector(Agent::new(Arc::clone(&out)))
        .build();

    octo.run().await.unwrap();

    let out = out.lock().unwrap();
    assert_eq!(out.find_kind.as_deref(), Some("petstore.event.pets_found"));
    assert_eq!(out.found_count, Some(2), "two available pets");
    assert_eq!(out.fetch_kind.as_deref(), Some("petstore.event.pet_fetched"));
    assert_eq!(out.fetched_name.as_deref(), Some("doggie"));
    assert_eq!(out.add_kind.as_deref(), Some("petstore.event.pet_added"));
    assert_eq!(out.added_id, Some(777), "server-assigned id round-trips");
    assert_eq!(out.delete_kind.as_deref(), Some("petstore.event.pet_deleted"));
    assert_eq!(out.deleted_payload_is_null, Some(true), "DELETE → null payload");
    assert_eq!(out.error_kind.as_deref(), Some("petstore.event.error"));
    assert_eq!(out.error_status, Some(500), "missing pet → 500 in event.error");
}

/// Model schemas register against `serde_json::Value`, and that registry also
/// happily holds a statically-typed kind from a (hypothetical) binary
/// connector — different namespaces don't collide. Closes petstore_case Q4.
#[test]
fn registry_mixes_dyn_value_with_static_types() {
    let spec =
        octo_connector_http::HttpSpec::from_toml_file(manifest_path()).expect("manifest parses");
    let connector = HttpConnector::from_spec(spec);

    let registry = connector
        .register_payloads(PayloadRegistry::new())
        // A statically-typed kind from some other (binary) connector.
        .register_type::<String>(EventKind::from_static("telegram.cmd.send_message"));

    // Dyn model schema present and carries its JSON schema.
    let pet_schema = registry.lookup(&EventKind::from_static("petstore.pet"));
    assert!(pet_schema.is_some(), "model schema registered as petstore.pet");
    assert!(pet_schema.unwrap().schema().is_some(), "schema retained");

    // Dyn command kind present as Value; static kind present as String. Both coexist.
    assert!(registry
        .lookup(&EventKind::from_static("petstore.cmd.add_pet"))
        .is_some());
    assert!(registry
        .lookup(&EventKind::from_static("telegram.cmd.send_message"))
        .is_some());
}
