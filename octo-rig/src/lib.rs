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

mod file;
pub use file::{SendFileArgs, SendFileTool};

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use octo_core::{
    ChannelId, ConnectorId, Envelope, EventBus, EventKind, InProcessBus,
    control::{CANCEL, CANCEL_SCOPE_TAG, RESTART_CONNECTOR, RESTART_PROCESS},
};
use rig::{completion::ToolDefinition, tool::Tool};
use serde::Deserialize;
use serde_json::{Value, json};

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
    /// Cancellation scope stamped onto every dispatched command (the `cancel_scope`
    /// tag). A connector honouring `octo.control.cancel` registers in-flight work under
    /// it and aborts on a matching cancel. `None` → commands are not cancellable by
    /// scope. The host sets it per-turn to that turn's id.
    scope: Option<String>,
    channels: HashMap<ConnectorId, ChannelId>,
    origin: Option<(ConnectorId, ChannelId)>,
}

impl OctoDispatchTool {
    pub fn new(bus: Arc<InProcessBus>, source: ConnectorId, catalog: impl Into<String>) -> Self {
        Self {
            bus,
            source,
            catalog: catalog.into(),
            timeout: DEFAULT_TIMEOUT,
            scope: None,
            channels: HashMap::new(),
            origin: None,
        }
    }

    /// Bind host-provided conversation context only to its own connector.
    /// Other organs remain unchannelled unless the assembly explicitly binds them.
    pub fn with_channel_for(mut self, target: ConnectorId, channel: ChannelId) -> Self {
        self.channels.insert(target, channel);
        self
    }

    /// Record where this turn originated, independently of its destination.
    /// These provenance tags do not grant the originating sender's permissions.
    pub fn with_origin(mut self, connector: ConnectorId, channel: ChannelId) -> Self {
        self.origin = Some((connector, channel));
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Tag every dispatched command with this cancellation scope, so an
    /// `octo.control.cancel { scope }` can abort the connector work it started.
    pub fn with_scope(mut self, scope: impl Into<String>) -> Self {
        self.scope = Some(scope.into());
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
    /// Explicit destination channel; otherwise use the host's binding for this target.
    #[serde(default)]
    pub channel: Option<String>,
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
                    "channel": { "type": "string", "description": "Explicit destination channel at the target connector. Omit to use its host-bound default; never confuse it with the source channel." },
                    "payload": { "type": "object", "description": "command payload fields" }
                },
                "required": ["target", "kind"]
            }),
        }
    }

    async fn call(&self, args: DispatchArgs) -> Result<Value, Self::Error> {
        let mut cmd = Envelope::new(
            self.source.clone(),
            EventKind::new(args.kind.clone()),
            args.payload,
        )
        .with_target(ConnectorId::new(args.target.clone()));
        let destination = args.channel.map(ChannelId::new).or_else(|| {
            self.channels
                .get(&ConnectorId::new(args.target.clone()))
                .cloned()
        });
        if let Some(channel) = destination {
            cmd = cmd.with_channel(channel);
        }
        if let Some((connector, channel)) = &self.origin {
            cmd = cmd
                .with_tag("origin.connector", connector.as_str())
                .with_tag("origin.channel", channel.as_str());
        }
        // Stamp the turn's cancel scope so a later octo.control.cancel can reach any
        // long-running connector work this dispatch starts (e.g. a forkd script).
        if let Some(scope) = &self.scope {
            cmd = cmd.with_tag(CANCEL_SCOPE_TAG, scope.clone());
        }

        tracing::info!(target = %args.target, kind = %args.kind, "rig tool → dispatch");
        let out = match self.bus.publish_and_await_response(cmd, self.timeout).await {
            Ok(resp) => {
                let body = resp.payload_as::<Value>().cloned().unwrap_or(Value::Null);
                json!({ "kind": resp.kind.as_str(), "result": body })
            }
            // Return the error as data so the model reports it honestly.
            Err(e) => {
                json!({ "error": e.to_string(), "status":"unknown", "note":"The connector may have completed the action. Verify its state before retrying." })
            }
        };
        Ok(out)
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

/// Publish an `octo.control.cancel { scope }` so every connector honouring it aborts
/// the in-flight work it started under that scope (killing forkd process groups). The
/// scope is the id the host stamped on the turn's dispatches (see
/// [`OctoDispatchTool::with_scope`]). Fire-and-forget: the connectors act on their own.
pub async fn carry_out_cancel(
    bus: &Arc<InProcessBus>,
    source: &ConnectorId,
    scope: &str,
) -> Result<(), String> {
    let env = Envelope::new(
        source.clone(),
        EventKind::from_static(CANCEL),
        scope.to_string(),
    );
    tracing::info!(scope = %scope, "carrying out cancel");
    bus.publish(env).await.map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use octo_core::{Filter, SubscribeOptions};
    use tokio::{spawn, time::timeout};

    #[tokio::test]
    async fn dispatch_binds_channels_only_to_their_connector_and_keeps_scope_and_payload() {
        let bus = Arc::new(InProcessBus::new(16));
        let target = ConnectorId::new("telegram-work");
        let mut requests = bus
            .subscribe(
                Filter::by_source(ConnectorId::new("agent")),
                SubscribeOptions::default(),
            )
            .await
            .unwrap();
        let tool = OctoDispatchTool::new(bus.clone(), ConnectorId::new("agent"), "")
            .with_channel_for(target.clone(), ChannelId::new("-42"))
            .with_origin(target.clone(), ChannelId::new("-42"))
            .with_scope("scope")
            .with_timeout(Duration::from_secs(1));
        let reply_bus = bus.clone();
        let worker = spawn(async move {
            for expected_channel in [Some("-42"), None, Some("other-room"), Some("-99")] {
                let req = timeout(Duration::from_secs(2), requests.next())
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(req.channel.as_ref().map(|c| c.as_str()), expected_channel);
                assert_eq!(
                    req.tags.get(CANCEL_SCOPE_TAG).map(String::as_str),
                    Some("scope")
                );
                assert_eq!(req.payload_as::<Value>().unwrap()["chat"], -77);
                assert_eq!(req.tags["origin.connector"], "telegram-work");
                assert_eq!(req.tags["origin.channel"], "-42");
                assert_eq!(req.source.as_str(), "agent");
                assert!(req.channel_metadata.is_none());
                reply_bus
                    .publish(
                        Envelope::new(
                            req.target.clone().unwrap(),
                            EventKind::new("result"),
                            json!({"ok":true}),
                        )
                        .with_correlation(req.id),
                    )
                    .await
                    .unwrap();
            }
        });
        for (target, channel) in [
            (target.as_str(), None),
            ("robot-arm", None),
            ("another-chat-service", Some("other-room")),
            (target.as_str(), Some("-99")),
        ] {
            let response = tool
                .call(DispatchArgs {
                    target: target.into(),
                    kind: "command".into(),
                    channel: channel.map(str::to_string),
                    payload: json!({"chat":-77}),
                })
                .await
                .unwrap();
            assert_eq!(response["result"]["ok"], true);
        }
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn confirmed_file_tool_returns_the_actual_connector_result() {
        let bus = Arc::new(InProcessBus::new(16));
        let target = ConnectorId::new("telegram");
        let mut requests = bus
            .subscribe(
                Filter::by_target(target.clone()),
                SubscribeOptions::default(),
            )
            .await
            .unwrap();
        let worker = spawn({
            let bus = bus.clone();
            async move {
                let req = timeout(Duration::from_secs(2), requests.next())
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(req.channel.as_ref().unwrap().as_str(), "-42");
                bus.publish(
                    Envelope::new(
                        target,
                        EventKind::new("chat.send_file.result"),
                        json!({"ok":false,"status":"not_sent","error":"denied"}),
                    )
                    .with_correlation(req.id),
                )
                .await
                .unwrap();
            }
        });
        let tool = SendFileTool::new(
            bus,
            ConnectorId::new("agent"),
            ConnectorId::new("telegram"),
            "-42",
        )
        .with_confirmation_timeout(Duration::from_secs(1));
        let result = tool
            .call(SendFileArgs {
                path: "image.png".into(),
                filename: None,
            })
            .await
            .unwrap();
        assert_eq!(result["ok"], false);
        assert_eq!(result["status"], "not_sent");
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn unconfirmed_file_delivery_never_claims_it_was_sent() {
        let bus = Arc::new(InProcessBus::new(8));
        let tool = SendFileTool::new(
            bus,
            ConnectorId::new("agent"),
            ConnectorId::new("legacy"),
            "chat",
        );
        let queued = tool
            .call(SendFileArgs {
                path: "x".into(),
                filename: None,
            })
            .await
            .unwrap();
        assert_eq!(queued["status"], "queued");
        assert!(queued.get("sent").is_none());
        let result = tool
            .with_confirmation_timeout(Duration::from_millis(10))
            .call(SendFileArgs {
                path: "x".into(),
                filename: None,
            })
            .await
            .unwrap();
        assert_eq!(result["status"], "unknown");
        assert!(result.get("sent").is_none());
    }
}
