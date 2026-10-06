use std::{sync::Arc, time::Duration};

use octo_core::{ChannelId, ConnectorId, Envelope, EventBus, EventKind, InProcessBus};
use rig::{completion::ToolDefinition, tool::Tool};
use serde::Deserialize;
use serde_json::{Value, json};

/// A rig tool that sends a workspace file to the user, by emitting a
/// `chat.send_file { path, filename? }` envelope to the reply connector on a fixed
/// channel. The bytes move by reference through the shared workspace, never
/// through the model. Delivery acknowledgements are opt-in for connectors that
/// advertise `chat.send_file.result`; otherwise the result reports queued only. The host binds it per-turn with the reply target and the
/// current channel, so the model only names a workspace-relative path.
#[derive(Clone)]
pub struct SendFileTool {
    bus: Arc<InProcessBus>,
    source: ConnectorId,
    target: ConnectorId,
    channel: String,
    confirmation_timeout: Option<Duration>,
    origin: Option<(ConnectorId, ChannelId)>,
}

impl SendFileTool {
    pub fn new(
        bus: Arc<InProcessBus>,
        source: ConnectorId,
        target: ConnectorId,
        channel: impl Into<String>,
    ) -> Self {
        Self {
            bus,
            source,
            target,
            channel: channel.into(),
            confirmation_timeout: None,
            origin: None,
        }
    }
    /// Host-supplied provenance, independent of the file's destination.
    pub fn with_origin(mut self, connector: ConnectorId, channel: ChannelId) -> Self {
        self.origin = Some((connector, channel));
        self
    }

    /// Await actual delivery when the connector supports correlated results.
    pub fn with_confirmation_timeout(mut self, timeout: Duration) -> Self {
        self.confirmation_timeout = Some(timeout);
        self
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
            description: "Send a file from the workspace to the user in this chat. Check the returned status: sent is confirmed, queued is not a delivery confirmation, and unknown requires checking the destination before retrying. Give `path` \
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
        let mut env = Envelope::new(
            self.source.clone(),
            EventKind::from_static("chat.send_file"),
            payload,
        )
        .with_target(self.target.clone())
        .with_channel(ChannelId::new(self.channel.clone()));

        if let Some((connector, channel)) = &self.origin {
            env = env
                .with_tag("origin.connector", connector.as_str())
                .with_tag("origin.channel", channel.as_str());
        }

        tracing::info!(path = %args.path, target = %self.target, "rig tool → send_file");
        if let Some(timeout) = self.confirmation_timeout {
            return Ok(match self.bus.publish_and_await_response(env, timeout).await {
                Ok(reply) => reply.payload_as::<Value>().cloned().unwrap_or_else(|| json!({"ok":false,"status":"unknown","error":"Invalid delivery acknowledgement; check the destination before retrying."})),
                Err(_) => json!({"ok":false,"status":"unknown","error":"Delivery was not confirmed. The file may already have been sent; check the destination before retrying."}),
            });
        }
        match self.bus.publish(env).await {
            Ok(()) => Ok(json!({ "ok": true, "queued": args.path, "status":"queued" })),
            Err(e) => Ok(json!({ "ok": false, "error": e.to_string() })),
        }
    }
}
