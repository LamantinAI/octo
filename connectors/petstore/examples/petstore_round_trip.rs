//! End-to-end round-trip against the live Petstore API.
//!
//! Demonstrates:
//! - Binary connector subscribes by target, handles `petstore.cmd.*` commands.
//! - Agent connector uses `ctx.publish_and_await_response` to drive a CRUD
//!   scenario through the bus — no direct calls to the connector struct.
//! - Correlation through `correlation_id`; the agent never sees envelopes
//!   meant for other in-flight commands (none here, but the mechanism is the
//!   same that would isolate concurrent agents).
//! - `petstore.event.error` carries [`ApiError`] when HTTP fails.
//!
//! Run (requires network access):
//!
//! ```text
//! cargo run --example petstore_round_trip -p octo-connector-petstore
//! ```
//!
//! The free Petstore instance is occasionally flaky (502/503). Re-run on
//! transient errors — that's also a realistic test of the error path.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use octo_core::{
    Connector, ConnectorCapabilities, ConnectorContext, ConnectorId, Envelope, EventKind, Octo,
    OctoResult, PayloadRegistry,
};
use octo_connector_petstore::{
    kinds, register_payloads, ApiError, FindByStatusRequest, Pet, PetIdRequest, PetStatus,
    PetstoreConnector,
};

const PETSTORE_ID: &str = "petstore";
const AGENT_ID: &str = "agent";

const PER_CALL_TIMEOUT: Duration = Duration::from_secs(15);

struct AgentConnector {
    id: ConnectorId,
    capabilities: ConnectorCapabilities,
    target: ConnectorId,
}

impl AgentConnector {
    fn new(id: impl Into<String>, target: impl Into<String>) -> Arc<Self> {
        let id = ConnectorId::new(id);
        let target = ConnectorId::new(target);
        let capabilities = ConnectorCapabilities::output_only()
            .with_emit_kinds([EventKind::from_static(kinds::CMD_ADD_PET)]);
        Arc::new(Self {
            id,
            capabilities,
            target,
        })
    }

    fn build_envelope<P: Send + Sync + 'static>(&self, kind: &'static str, payload: P) -> Envelope {
        Envelope::new(self.id.clone(), EventKind::from_static(kind), payload)
            .with_target(self.target.clone())
    }
}

#[async_trait]
impl Connector for AgentConnector {
    fn id(&self) -> &ConnectorId {
        &self.id
    }

    fn capabilities(&self) -> &ConnectorCapabilities {
        &self.capabilities
    }

    async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
        let outcome = self.scenario(&ctx).await;
        match outcome {
            Ok(()) => println!("[agent] scenario completed"),
            Err(e) => eprintln!("[agent] scenario failed: {e}"),
        }
        // Signal end-of-scenario so the runtime can shut down.
        ctx.shutdown.cancel();
        Ok(())
    }
}

impl AgentConnector {
    async fn scenario(&self, ctx: &ConnectorContext) -> OctoResult<()> {
        // ── 1. List available pets (GET — usually works on the flaky free server) ─
        println!("[agent] → find_by_status(Available)");
        let response = ctx
            .publish_and_await_response(
                self.build_envelope(
                    kinds::CMD_FIND_BY_STATUS,
                    FindByStatusRequest {
                        status: PetStatus::Available,
                    },
                ),
                PER_CALL_TIMEOUT,
            )
            .await?;
        let sample_id = match response.kind.as_str() {
            kinds::EVT_PETS_FOUND => {
                let pets = response
                    .payload_as::<Vec<Pet>>()
                    .expect("pets_found carries Vec<Pet>");
                println!(
                    "[agent]   found {} pets; first few: {:?}",
                    pets.len(),
                    pets.iter().take(3).map(|p| &p.name).collect::<Vec<_>>()
                );
                pets.iter().find_map(|p| p.id)
            }
            kinds::EVT_ERROR => {
                eprintln!("[agent]   find_by_status failed: {}", api_summary(&response));
                None
            }
            other => panic!("unexpected reply kind: {other}"),
        };

        // ── 2. Fetch one found pet (still GET) ────────────────────────────
        if let Some(id) = sample_id {
            println!("[agent] → fetch_pet({id})");
            let resp = ctx
                .publish_and_await_response(
                    self.build_envelope(kinds::CMD_FETCH_PET, PetIdRequest { id }),
                    PER_CALL_TIMEOUT,
                )
                .await?;
            match resp.kind.as_str() {
                kinds::EVT_PET_FETCHED => {
                    let pet = resp.payload_as::<Pet>().expect("Pet payload");
                    println!(
                        "[agent]   fetched id={:?}, name={}, status={:?}",
                        pet.id, pet.name, pet.status
                    );
                }
                kinds::EVT_ERROR => eprintln!("[agent]   fetch_pet failed: {}", api_summary(&resp)),
                other => panic!("unexpected reply kind: {other}"),
            }
        }

        // ── 3. Add a pet (POST — often 500 on the free server; error-path proof) ─
        let new_pet = Pet {
            id: None,
            name: "octo-bench-pup".to_string(),
            category: None,
            photo_urls: Vec::new(),
            tags: Vec::new(),
            status: Some(PetStatus::Available),
        };
        println!("[agent] → add_pet({})", new_pet.name);
        let resp = ctx
            .publish_and_await_response(
                self.build_envelope(kinds::CMD_ADD_PET, new_pet),
                PER_CALL_TIMEOUT,
            )
            .await?;
        let new_pet_id = match resp.kind.as_str() {
            kinds::EVT_PET_ADDED => {
                let pet = resp.payload_as::<Pet>().expect("Pet payload");
                let id = pet.id.expect("petstore returns id");
                println!("[agent]   added id={id}, status={:?}", pet.status);
                Some(id)
            }
            kinds::EVT_ERROR => {
                eprintln!("[agent]   add_pet failed: {}", api_summary(&resp));
                None
            }
            other => panic!("unexpected reply kind: {other}"),
        };

        // ── 4. Delete the pet we just added — only if step 3 succeeded ─────
        if let Some(id) = new_pet_id {
            println!("[agent] → delete_pet({id})");
            let resp = ctx
                .publish_and_await_response(
                    self.build_envelope(kinds::CMD_DELETE_PET, PetIdRequest { id }),
                    PER_CALL_TIMEOUT,
                )
                .await?;
            match resp.kind.as_str() {
                kinds::EVT_PET_DELETED => println!("[agent]   deleted id={id}"),
                kinds::EVT_ERROR => {
                    eprintln!("[agent]   delete_pet failed: {}", api_summary(&resp))
                }
                other => panic!("unexpected reply kind: {other}"),
            }
        }

        Ok(())
    }
}

fn api_summary(envelope: &Envelope) -> String {
    let err = envelope
        .payload_as::<ApiError>()
        .cloned()
        .unwrap_or(ApiError {
            http_status: None,
            message: "missing ApiError payload".to_string(),
        });
    format!("http {:?}: {}", err.http_status, err.message)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> OctoResult<()> {
    let registry = register_payloads(PayloadRegistry::new());

    let octo = Octo::builder()
        .bus_capacity(64)
        .payload_registry(Arc::new(registry))
        .add_connector(PetstoreConnector::builder(PETSTORE_ID).build())
        .add_connector(AgentConnector::new(AGENT_ID, PETSTORE_ID))
        .build();

    octo.run().await?;
    println!("[main] done");
    Ok(())
}
