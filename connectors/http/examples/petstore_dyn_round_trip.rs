//! End-to-end round-trip of the **dynamic** HTTP connector against the live
//! Petstore API — the dyn twin of `octo-connector-petstore`'s example.
//!
//! The whole Petstore API is described by `config/connectors/petstore/petstore.toml`;
//! no Petstore-specific Rust code exists here. Payloads are `serde_json::Value`.
//!
//! Run (requires network):
//!
//! ```text
//! cargo run --example petstore_dyn_round_trip -p octo-connector-http
//! ```
//!
//! The free Petstore instance is frequently flaky (500/502/503). The error
//! path (`petstore.event.error`) is a realistic, expected outcome — that's the
//! point of the retry policy in the manifest.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use octo_connector_http::HttpConnector;
use octo_core::{
    Connector, ConnectorCapabilities, ConnectorContext, ConnectorId, Envelope, EventKind, Octo,
    OctoResult, PayloadRegistry,
};
use serde_json::{json, Value};

const PETSTORE_ID: &str = "petstore";
const CALL_TIMEOUT: Duration = Duration::from_secs(15);

fn manifest_path() -> String {
    format!(
        "{}/../../config/connectors/petstore/petstore.toml",
        env!("CARGO_MANIFEST_DIR")
    )
}

struct Agent {
    id: ConnectorId,
    capabilities: ConnectorCapabilities,
    target: ConnectorId,
}

impl Agent {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            id: ConnectorId::new("agent"),
            capabilities: ConnectorCapabilities::output_only(),
            target: ConnectorId::new(PETSTORE_ID),
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
        println!("[agent] → find_pets_by_status(available)");
        let resp = ctx
            .publish_and_await_response(
                self.cmd("petstore.cmd.find_pets_by_status", json!({ "status": "available" })),
                CALL_TIMEOUT,
            )
            .await?;
        report("find_pets_by_status", &resp);

        println!("[agent] → add_pet(octo-dyn-pup)");
        let resp = ctx
            .publish_and_await_response(
                self.cmd(
                    "petstore.cmd.add_pet",
                    json!({ "name": "octo-dyn-pup", "photoUrls": [], "status": "available" }),
                ),
                CALL_TIMEOUT,
            )
            .await?;
        report("add_pet", &resp);

        if resp.kind.as_str() == "petstore.event.pet_added" {
            if let Some(id) = resp.payload_as::<Value>().and_then(|v| v["id"].as_i64()) {
                println!("[agent] → delete_pet({id})");
                let resp = ctx
                    .publish_and_await_response(
                        self.cmd("petstore.cmd.delete_pet", json!({ "id": id })),
                        CALL_TIMEOUT,
                    )
                    .await?;
                report("delete_pet", &resp);
            }
        }

        ctx.shutdown.cancel();
        Ok(())
    }
}

fn report(label: &str, env: &Envelope) {
    let payload = env
        .payload_as::<Value>()
        .map(|v| v.to_string())
        .unwrap_or_else(|| "<non-Value payload>".to_string());
    let preview: String = payload.chars().take(200).collect();
    println!("[agent]   {label} → {} : {preview}", env.kind);
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> OctoResult<()> {
    let connector = HttpConnector::from_file(manifest_path()).expect("manifest loads");
    let registry = connector.register_payloads(PayloadRegistry::new());

    let octo = Octo::builder()
        .bus_capacity(64)
        .payload_registry(Arc::new(registry))
        .add_connector(connector)
        .add_connector(Agent::new())
        .build();

    octo.run().await?;
    println!("[main] done");
    Ok(())
}
