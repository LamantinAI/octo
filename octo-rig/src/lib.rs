//! `octo-rig-tool` — a [`rig`] `Tool` that bridges rig's native tool-calling to
//! the **Octo connector system**.
//!
//! A rig-driven model, doing ordinary function-calling, gets one tool:
//! `dispatch_to_connector`. When it calls it, the tool publishes a command
//! envelope onto the Octo bus, awaits the correlated response, and hands the
//! result back to the model. The model's *action space is whatever connectors
//! are registered* — env-as-tools, implemented inside rig's tool loop.
//!
//! ```ignore
//! let tool = OctoDispatchTool::new(ctx.bus(), source_id, catalog);
//! let agent = client.agent(model).preamble(p).tool(tool).build();
//! let answer = agent.prompt(user).multi_turn(5).send().await?;
//! ```

use std::sync::Arc;
use std::time::Duration;

use octo_core::{ConnectorId, Envelope, EventBus, EventKind, InProcessBus};
use rig::completion::ToolDefinition;
use rig::tool::Tool;
use serde::Deserialize;
use serde_json::{json, Value};

/// The octo-code file tools (`read`/`write`/`edit`/`list`/`glob`/`grep`),
/// available behind the `code` feature. Add them to a rig agent alongside
/// [`OctoDispatchTool`]: `agent.tool(ReadTool).tool(WriteTool)...`. Jailed to
/// `$OCTO_CODE_WORKSPACE`.
#[cfg(feature = "code")]
pub use octo_code::{EditTool, GlobTool, GrepTool, ListTool, ReadTool, WriteTool};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(20);

/// A rig tool that dispatches a command to an Octo connector and returns its
/// response. Hold it with the bus handle, the emitting `source` id, and a
/// `catalog` string describing the available connectors (so the model knows
/// what it can call).
#[derive(Clone)]
pub struct OctoDispatchTool {
    bus: Arc<InProcessBus>,
    source: ConnectorId,
    catalog: String,
    timeout: Duration,
}

impl OctoDispatchTool {
    pub fn new(bus: Arc<InProcessBus>, source: ConnectorId, catalog: impl Into<String>) -> Self {
        Self {
            bus,
            source,
            catalog: catalog.into(),
            timeout: DEFAULT_TIMEOUT,
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

/// Arguments the model fills when calling the dispatch tool.
#[derive(Debug, Deserialize)]
pub struct DispatchArgs {
    /// Connector id to address (e.g. `petstore`).
    pub target: String,
    /// Command kind (e.g. `petstore.cmd.find_pets_by_status`).
    pub kind: String,
    /// JSON payload for the command.
    #[serde(default)]
    pub payload: Value,
}

impl Tool for OctoDispatchTool {
    const NAME: &'static str = "dispatch_to_connector";
    type Error = std::convert::Infallible;
    type Args = DispatchArgs;
    type Output = Value;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_string(),
            description: format!(
                "Dispatch a command to an available Octo connector and get its result. \
                 Use this when the user's request needs a connector's data or action. \
                 Available connectors (target → command kinds, with payload fields):\n{}",
                self.catalog
            ),
            parameters: json!({
                "type": "object",
                "properties": {
                    "target": { "type": "string", "description": "connector id" },
                    "kind": { "type": "string", "description": "command kind" },
                    "payload": { "type": "object", "description": "command payload fields" }
                },
                "required": ["target", "kind"]
            }),
        }
    }

    async fn call(&self, args: DispatchArgs) -> Result<Value, Self::Error> {
        let cmd = Envelope::new(
            self.source.clone(),
            EventKind::new(args.kind.clone()),
            args.payload,
        )
        .with_target(ConnectorId::new(args.target.clone()));

        tracing::info!(target = %args.target, kind = %args.kind, "rig tool → dispatch");
        let out = match self.bus.publish_and_await_response(cmd, self.timeout).await {
            Ok(resp) => {
                let body = resp.payload_as::<Value>().cloned().unwrap_or(Value::Null);
                json!({ "kind": resp.kind.as_str(), "result": body })
            }
            // Return the error as data so the model reports it honestly.
            Err(e) => json!({ "error": e.to_string() }),
        };
        Ok(out)
    }
}
