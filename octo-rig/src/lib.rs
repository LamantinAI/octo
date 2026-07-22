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

use std::sync::{Arc, Mutex};
use std::time::Duration;

use octo_core::{
    control::{RESTART_CONNECTOR, RESTART_PROCESS},
    ChannelId, ConnectorId, Envelope, EventBus, EventKind, InProcessBus,
};
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

/// A rig tool that sends a workspace file to the user, by emitting a
/// `chat.send_file { path, filename? }` envelope to the reply connector on a fixed
/// channel. Unlike [`OctoDispatchTool`] this is **fire-and-forget** (no correlated
/// response): the bytes move by reference through the shared workspace, never
/// through the model. The host binds it per-turn with the reply target and the
/// current channel, so the model only names a workspace-relative path.
#[derive(Clone)]
pub struct SendFileTool {
    bus: Arc<InProcessBus>,
    source: ConnectorId,
    target: ConnectorId,
    channel: String,
}

impl SendFileTool {
    pub fn new(
        bus: Arc<InProcessBus>,
        source: ConnectorId,
        target: ConnectorId,
        channel: impl Into<String>,
    ) -> Self {
        Self { bus, source, target, channel: channel.into() }
    }
}

/// Arguments the model fills when sending a file.
#[derive(Debug, Deserialize)]
pub struct SendFileArgs {
    /// Workspace-relative path of the file to send.
    pub path: String,
    /// Optional display name shown to the user.
    #[serde(default)]
    pub filename: Option<String>,
}

impl Tool for SendFileTool {
    const NAME: &'static str = "send_file";
    type Error = std::convert::Infallible;
    type Args = SendFileArgs;
    type Output = Value;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_string(),
            description: "Send a file from the workspace to the user in this chat. Give `path` \
                          relative to the workspace (where the file tools and storage.checkout put \
                          files); `filename` optionally overrides the shown name. The file is sent \
                          by reference — never paste its bytes."
                .to_string(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "workspace-relative path to send" },
                    "filename": { "type": "string", "description": "optional display name" }
                },
                "required": ["path"]
            }),
        }
    }

    async fn call(&self, args: SendFileArgs) -> Result<Value, Self::Error> {
        let mut payload = json!({ "path": args.path });
        if let Some(f) = &args.filename {
            payload["filename"] = json!(f);
        }
        let env = Envelope::new(self.source.clone(), EventKind::from_static("chat.send_file"), payload)
            .with_target(self.target.clone())
            .with_channel(ChannelId::new(self.channel.clone()));

        tracing::info!(path = %args.path, target = %self.target, "rig tool → send_file");
        match self.bus.publish(env).await {
            Ok(()) => Ok(json!({ "ok": true, "sent": args.path })),
            Err(e) => Ok(json!({ "ok": false, "error": e.to_string() })),
        }
    }
}

/// A rig tool that lets an agent request a restart of part of the runtime — a single
/// connector (to reload its manifest) or the whole process (a graceful shutdown; a
/// supervisor such as systemd `Restart=always` revives it with fresh config).
///
/// It **records the request rather than acting on it.** `call` stores the target in a
/// shared slot and returns; the host drains that slot **after the turn's reply has
/// been delivered** and calls [`carry_out_restart`] to perform it. Doing it any other
/// way loses the reply: the runtime begins winding connectors down the instant the
/// `octo.control.*` signal lands, so a `chat.reply` emitted moments later (the model
/// speaks *then* calls the tool) races the teardown and never goes out. Deferring to
/// post-reply removes the race regardless of how long the rest of the turn takes.
/// **Whether to expose the tool, and to whom, is the host's call.** This is what lets
/// an agent apply its own config changes (the gap OpenClaw has).
#[derive(Clone)]
pub struct RestartTool {
    /// Filled with the requested target by [`Tool::call`]; drained by the host once
    /// the reply is sent. `Some("process")` or `Some("<connector id>")`.
    pending: Arc<Mutex<Option<String>>>,
}

impl RestartTool {
    /// Hold the tool with the slot the host reads after the reply is delivered.
    pub fn new(pending: Arc<Mutex<Option<String>>>) -> Self {
        Self { pending }
    }
}

/// Arguments for [`RestartTool`].
#[derive(Debug, Deserialize)]
pub struct RestartArgs {
    /// `"process"` to restart the whole runtime, or a connector id to restart just it.
    pub target: String,
}

impl Tool for RestartTool {
    const NAME: &'static str = "restart";
    type Error = std::convert::Infallible;
    type Args = RestartArgs;
    type Output = Value;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_string(),
            description: "Restart part of the runtime to apply configuration changes. \
                          target=\"process\" restarts the whole assistant — a graceful shutdown; a \
                          supervisor brings it straight back with fresh config (use after editing \
                          albert.toml, or when the owner asks you to reboot). target=\"<connector \
                          id>\" restarts just that connector to reload its manifest. The restart is \
                          carried out right AFTER this reply is delivered, so first tell the user \
                          what you are doing in your reply, then call this once."
                .to_string(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "target": { "type": "string", "description": "\"process\", or a connector id" }
                },
                "required": ["target"]
            }),
        }
    }

    async fn call(&self, args: RestartArgs) -> Result<Value, Self::Error> {
        tracing::info!(target = %args.target, "rig tool → restart (recorded; fires after the reply)");
        if let Ok(mut slot) = self.pending.lock() {
            *slot = Some(args.target.clone());
        }
        Ok(json!({
            "ok": true,
            "restarting": args.target,
            "when": "right after this reply is delivered"
        }))
    }
}

/// Carry out a restart previously recorded by [`RestartTool::call`]. The host calls
/// this **after** the turn's reply has been emitted (ideally with a short grace so the
/// reply flushes). `target == "process"` restarts the whole runtime (`RESTART_PROCESS`);
/// any other value names a connector to reload (`RESTART_CONNECTOR`). The runtime's
/// control listener does the actual work.
pub async fn carry_out_restart(
    bus: &Arc<InProcessBus>,
    source: &ConnectorId,
    target: &str,
) -> Result<(), String> {
    let env = if target == "process" {
        Envelope::new(source.clone(), EventKind::from_static(RESTART_PROCESS), ())
    } else {
        Envelope::new(
            source.clone(),
            EventKind::from_static(RESTART_CONNECTOR),
            target.to_string(),
        )
    };
    tracing::info!(target = %target, "carrying out restart");
    bus.publish(env).await.map_err(|e| e.to_string())
}
