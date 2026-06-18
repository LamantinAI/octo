//! Deterministic round-trip for the **binary** Petstore connector against a
//! local mock server — the reproducible happy-path coverage the live-API
//! example can't guarantee (the free Petstore is frequently down).
//!
//! Exercises typed payloads (`Pet`, `PetIdRequest`, ...), the CQRS
//! `cmd → bus → connector → HTTP → bus → event.*` flow, correlation via
//! `publish_and_await_response`, and the `petstore.event.error` path.

mod mock_http;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use octo_connector_petstore::{
    kinds, register_payloads, ApiError, FindByStatusRequest, Pet, PetIdRequest, PetStatus,
    PetstoreConnector,
};
use octo_core::{
    Connector, ConnectorCapabilities, ConnectorContext, ConnectorId, Envelope, EventKind, Octo,
    OctoResult, PayloadRegistry,
};

use mock_http::{MockServer, Route};

const PETSTORE_ID: &str = "petstore";
const CALL_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Default, Debug)]
struct Outcomes {
    kinds: Vec<String>,
    found_names: Option<Vec<String>>,
    fetched: Option<(i64, String)>,
    added_id: Option<i64>,
    updated_status: Option<PetStatus>,
    deleted_id: Option<i64>,
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

    fn cmd<P: Send + Sync + 'static>(&self, kind: &'static str, payload: P) -> Envelope {
        Envelope::new(self.id.clone(), EventKind::from_static(kind), payload)
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
        // Warmup: let the petstore connector subscribe before we publish.
        tokio::time::sleep(Duration::from_millis(150)).await;
        let result = self.scenario(&ctx).await;
        ctx.shutdown.cancel();
        result
    }
}

impl Agent {
    async fn scenario(&self, ctx: &ConnectorContext) -> OctoResult<()> {
        let record_kind = |resp: &Envelope, out: &Arc<Mutex<Outcomes>>| {
            out.lock().unwrap().kinds.push(resp.kind.as_str().to_string());
        };

        // find_pets_by_status
        let resp = ctx
            .publish_and_await_response(
                self.cmd(
                    kinds::CMD_FIND_BY_STATUS,
                    FindByStatusRequest { status: PetStatus::Available },
                ),
                CALL_TIMEOUT,
            )
            .await?;
        record_kind(&resp, &self.out);
        self.out.lock().unwrap().found_names = resp
            .payload_as::<Vec<Pet>>()
            .map(|pets| pets.iter().map(|p| p.name.clone()).collect());

        // fetch_pet
        let resp = ctx
            .publish_and_await_response(self.cmd(kinds::CMD_FETCH_PET, PetIdRequest { id: 10 }), CALL_TIMEOUT)
            .await?;
        record_kind(&resp, &self.out);
        self.out.lock().unwrap().fetched = resp
            .payload_as::<Pet>()
            .map(|pet| (pet.id.unwrap_or_default(), pet.name.clone()));

        // add_pet
        let new_pet = Pet {
            id: None,
            name: "octo-bench-pup".into(),
            category: None,
            photo_urls: vec![],
            tags: vec![],
            status: Some(PetStatus::Available),
        };
        let resp = ctx
            .publish_and_await_response(self.cmd(kinds::CMD_ADD_PET, new_pet), CALL_TIMEOUT)
            .await?;
        record_kind(&resp, &self.out);
        let added_id = resp.payload_as::<Pet>().and_then(|p| p.id).unwrap_or_default();
        self.out.lock().unwrap().added_id = Some(added_id);

        // update_pet
        let upd = Pet {
            id: Some(added_id),
            name: "octo-bench-pup".into(),
            category: None,
            photo_urls: vec![],
            tags: vec![],
            status: Some(PetStatus::Sold),
        };
        let resp = ctx
            .publish_and_await_response(self.cmd(kinds::CMD_UPDATE_PET, upd), CALL_TIMEOUT)
            .await?;
        record_kind(&resp, &self.out);
        self.out.lock().unwrap().updated_status =
            resp.payload_as::<Pet>().and_then(|p| p.status.clone());

        // delete_pet
        let resp = ctx
            .publish_and_await_response(self.cmd(kinds::CMD_DELETE_PET, PetIdRequest { id: added_id }), CALL_TIMEOUT)
            .await?;
        record_kind(&resp, &self.out);
        self.out.lock().unwrap().deleted_id = resp.payload_as::<PetIdRequest>().map(|r| r.id);

        // error path: fetch missing → 500
        let resp = ctx
            .publish_and_await_response(self.cmd(kinds::CMD_FETCH_PET, PetIdRequest { id: 999 }), CALL_TIMEOUT)
            .await?;
        record_kind(&resp, &self.out);
        self.out.lock().unwrap().error_status =
            resp.payload_as::<ApiError>().and_then(|e| e.http_status);

        Ok(())
    }
}

fn routes() -> Vec<Route> {
    vec![
        Route::new("GET", "/pet/findByStatus", 200, |_| {
            serde_json::json!([
                { "id": 10, "name": "doggie", "photoUrls": [], "status": "available" },
                { "id": 11, "name": "rex",    "photoUrls": [], "status": "available" }
            ])
            .to_string()
        }),
        Route::new("GET", "/pet/10", 200, |_| {
            serde_json::json!({ "id": 10, "name": "doggie", "photoUrls": [], "status": "available" })
                .to_string()
        }),
        Route::new("GET", "/pet/999", 500, |_| "not found".into()),
        Route::new("POST", "/pet", 200, |body| {
            let mut pet: serde_json::Value = serde_json::from_str(body).unwrap_or_default();
            pet["id"] = serde_json::json!(777);
            pet.to_string()
        }),
        Route::new("PUT", "/pet", 200, |body| body.to_string()),
        Route::new("DELETE", "/pet/777", 200, |_| String::new()),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_petstore_round_trip_through_bus() {
    let server = MockServer::start(routes()).await;

    // The sandbox sets HTTP_PROXY; bypass it so requests reach the local mock.
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let connector = PetstoreConnector::builder(PETSTORE_ID)
        .base_url(server.base_url())
        .http_client(client)
        .build();
    let registry = Arc::new(register_payloads(PayloadRegistry::new()));

    let out = Arc::new(Mutex::new(Outcomes::default()));
    let octo = Octo::builder()
        .bus_capacity(64)
        .payload_registry(registry)
        .add_connector(connector)
        .add_connector(Agent::new(Arc::clone(&out)))
        .build();

    octo.run().await.unwrap();

    let out = out.lock().unwrap();
    assert_eq!(
        out.kinds,
        vec![
            kinds::EVT_PETS_FOUND,
            kinds::EVT_PET_FETCHED,
            kinds::EVT_PET_ADDED,
            kinds::EVT_PET_UPDATED,
            kinds::EVT_PET_DELETED,
            kinds::EVT_ERROR,
        ],
        "every command produced its expected response kind in order"
    );
    assert_eq!(
        out.found_names.as_deref(),
        Some(&["doggie".to_string(), "rex".to_string()][..])
    );
    assert_eq!(out.fetched, Some((10, "doggie".to_string())));
    assert_eq!(out.added_id, Some(777));
    assert_eq!(out.updated_status, Some(PetStatus::Sold));
    assert_eq!(out.deleted_id, Some(777));
    assert_eq!(out.error_status, Some(500));
}
