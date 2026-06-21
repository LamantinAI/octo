//! `ConsoleConnector` — a minimal bidirectional connector over stdin/stdout.
//!
//! Lets us exercise the runtime + LLM cogitator without Telegram: each stdin
//! line becomes a `chat.message` (with a `reply_to` recommendation), and every
//! `chat.reply` targeted at us is printed. This is the stand-in for the
//! teloxide Telegram connector that comes next — same envelope shapes.

use std::sync::Arc;

use async_trait::async_trait;
use octo_core::{
    Blob, ChannelId, Connector, ConnectorCapabilities, ConnectorContext, ConnectorId, Envelope,
    EventKind, Filter, OctoResult, ReplyChannel, SubscribeOptions,
};
use tokio::io::{AsyncBufReadExt, BufReader};

const CHANNEL: &str = "stdin";

pub struct ConsoleConnector {
    id: ConnectorId,
    capabilities: ConnectorCapabilities,
}

impl ConsoleConnector {
    pub fn new(id: impl Into<String>) -> Arc<Self> {
        let capabilities = ConnectorCapabilities::bidirectional()
            .with_emit_kinds([EventKind::from_static("chat.message")])
            .with_accept_kinds([EventKind::from_static("chat.reply")]);
        Arc::new(Self {
            id: ConnectorId::new(id),
            capabilities,
        })
    }
}

#[async_trait]
impl Connector for ConsoleConnector {
    fn id(&self) -> &ConnectorId {
        &self.id
    }

    fn capabilities(&self) -> &ConnectorCapabilities {
        &self.capabilities
    }

    async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
        // Subscribe to replies addressed to us before emitting anything.
        let mut replies = ctx
            .subscribe(Filter::by_target(self.id.clone()), SubscribeOptions::default())
            .await?;

        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        println!("octolab ready — type a message (Ctrl-D to quit):");

        loop {
            tokio::select! {
                line = lines.next_line() => match line {
                    Ok(Some(text)) => {
                        if text.trim().is_empty() {
                            continue;
                        }
                        let msg = Envelope::new(
                            self.id.clone(),
                            EventKind::from_static("chat.message"),
                            text,
                        )
                        .with_channel(ChannelId::new(CHANNEL))
                        // Recommendation: deliver the reply back to this channel.
                        .with_reply_to(ReplyChannel::new(ChannelId::new(CHANNEL)));
                        ctx.publish(msg).await?;
                    }
                    Ok(None) => {
                        // EOF (Ctrl-D): drive shutdown.
                        ctx.shutdown.cancel();
                        return Ok(());
                    }
                    Err(e) => {
                        eprintln!("[console] stdin error: {e}");
                        ctx.shutdown.cancel();
                        return Ok(());
                    }
                },
                reply = replies.next() => match reply {
                    Some(env) => {
                        if let Some(blob) = env.payload_as::<Blob>() {
                            // The console can't render images; show a placeholder.
                            println!("🖼  [media: {}, {} bytes]", blob.content_type(), blob.len());
                        } else if let Some(text) = env.payload_as::<String>() {
                            println!("🤖 {text}");
                        }
                    }
                    None => return Ok(()),
                },
                _ = ctx.shutdown.cancelled() => return Ok(()),
            }
        }
    }
}
