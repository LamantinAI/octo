//! Streaming response demo — simulates an LLM cogitator emitting tokens
//! progressively, and a Telegram-style sink that consumes chunks both
//! progressively (live tail) and on completion (final assembled text).
//!
//! Demonstrates:
//! - **Streaming protocol** via `correlation_id` + `StreamFrame::{Open, Chunk, Close}`.
//! - **Heterogeneous sink behaviour** — the same chunk envelopes can be:
//!   - rendered progressively (each chunk: `editMessage`-style),
//!   - or assembled and delivered once (`Close`-triggered final).
//! - **Per-stream state** keyed by `correlation_id` in the sink — multiple
//!   concurrent streams from different sources are kept separate.
//!
//! The "LLM cogitator" here is just a streaming connector with hard-coded
//! token timing — no real LLM. The protocol pieces are what matter.
//!
//! Run:
//!
//! ```text
//! cargo run --example streaming_response
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use octo_core::{
    Connector, ConnectorCapabilities, ConnectorContext, ConnectorId, Envelope, EventId, EventKind,
    Filter, Octo, OctoResult, StreamFrame, SubscribeOptions, TrailAction, TrailActor, TrailEntry,
};

/// "LLM cogitator" pretending to stream tokens. Implemented as a Connector
/// for simplicity — the streaming pattern is independent of which actor type
/// emits the chunks.
struct StreamingResponder {
    id: ConnectorId,
    capabilities: ConnectorCapabilities,
    target: ConnectorId,
    tokens: Vec<&'static str>,
    token_delay: Duration,
}

impl StreamingResponder {
    fn new(
        id: impl Into<String>,
        target: ConnectorId,
        tokens: Vec<&'static str>,
        token_delay: Duration,
    ) -> Arc<Self> {
        let id = ConnectorId::new(id);
        let capabilities = ConnectorCapabilities::input_only()
            .with_emit_kinds([EventKind::from_static("alert.text")]);
        Arc::new(Self {
            id,
            capabilities,
            target,
            tokens,
            token_delay,
        })
    }
}

#[async_trait]
impl Connector for StreamingResponder {
    fn id(&self) -> &ConnectorId {
        &self.id
    }
    fn capabilities(&self) -> &ConnectorCapabilities {
        &self.capabilities
    }
    async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
        // Brief warmup so subscribers register before our first emit.
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
            _ = ctx.shutdown.cancelled() => return Ok(()),
        }

        let stream_id = EventId::new();
        let kind = EventKind::from_static("alert.text");

        for (i, token) in self.tokens.iter().enumerate() {
            let frame = if i == 0 {
                StreamFrame::Open
            } else if i == self.tokens.len() - 1 {
                StreamFrame::Close
            } else {
                StreamFrame::Chunk
            };

            let envelope = Envelope::new(self.id.clone(), kind.clone(), token.to_string())
                .with_target(self.target.clone())
                .with_correlation(stream_id)
                .with_stream_frame(frame)
                .with_trail(TrailEntry::new(
                    TrailActor::Connector(self.id.clone()),
                    TrailAction::Emit { kind: kind.clone() },
                ));

            ctx.publish(envelope).await?;
            println!("[responder {}] emit {:?} \"{}\"", self.id, frame, token);

            tokio::select! {
                _ = tokio::time::sleep(self.token_delay) => {}
                _ = ctx.shutdown.cancelled() => return Ok(()),
            }
        }
        Ok(())
    }
}

/// Telegram-style sink that supports BOTH:
/// - progressive rendering (prints each chunk as it arrives, like editMessage),
/// - completion assembly (prints the full message on Close).
struct ProgressiveSink {
    id: ConnectorId,
    capabilities: ConnectorCapabilities,
}

impl ProgressiveSink {
    fn new(id: impl Into<String>) -> Arc<Self> {
        let id = ConnectorId::new(id);
        let capabilities = ConnectorCapabilities::output_only()
            .with_accept_kinds([EventKind::from_static("alert.text")]);
        Arc::new(Self { id, capabilities })
    }
}

#[async_trait]
impl Connector for ProgressiveSink {
    fn id(&self) -> &ConnectorId {
        &self.id
    }
    fn capabilities(&self) -> &ConnectorCapabilities {
        &self.capabilities
    }
    async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
        let mut sub = ctx
            .subscribe(
                Filter::by_target(self.id.clone()),
                SubscribeOptions::default(),
            )
            .await?;

        // Per-stream buffers keyed by correlation_id.
        let mut streams: HashMap<EventId, String> = HashMap::new();
        let mut completed: usize = 0;

        loop {
            tokio::select! {
                next = sub.next() => match next {
                    Some(envelope) => {
                        let Some(token) = envelope.payload_as::<String>().cloned() else {
                            continue;
                        };
                        let frame = envelope.stream;
                        let cid = envelope.correlation_id;

                        match (frame, cid) {
                            (Some(StreamFrame::Open), Some(id)) => {
                                println!("[sink {}] OPEN  cid={} token=\"{}\"", self.id, id, token);
                                streams.insert(id, token);
                            }
                            (Some(StreamFrame::Chunk), Some(id)) => {
                                let buf = streams.entry(id).or_default();
                                buf.push_str(&token);
                                println!(
                                    "[sink {}] CHUNK cid={} token=\"{}\"  (so far: \"{}\")",
                                    self.id, id, token, buf
                                );
                            }
                            (Some(StreamFrame::Close), Some(id)) => {
                                let mut buf = streams.remove(&id).unwrap_or_default();
                                buf.push_str(&token);
                                completed += 1;
                                println!(
                                    "[sink {}] CLOSE cid={} | final: \"{}\"",
                                    self.id, id, buf
                                );
                            }
                            (Some(StreamFrame::Cancel), Some(id)) => {
                                streams.remove(&id);
                                println!("[sink {}] CANCEL cid={}", self.id, id);
                            }
                            (None, _) => {
                                // Non-stream envelope — handle as normal one-shot.
                                println!("[sink {}] one-shot \"{}\"", self.id, token);
                                completed += 1;
                            }
                            (Some(_), None) => {
                                eprintln!(
                                    "[sink {}] ⚠ stream chunk without correlation_id; dropping",
                                    self.id
                                );
                            }
                        }
                    }
                    None => {
                        println!("[sink {}] bus closed; {} message(s) completed", self.id, completed);
                        return Ok(());
                    }
                },
                _ = ctx.shutdown.cancelled() => {
                    println!("[sink {}] shutdown; {} message(s) completed", self.id, completed);
                    return Ok(());
                }
            }
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> OctoResult<()> {
    let sink_id = ConnectorId::new("telegram");

    let octo = Octo::builder()
        .bus_capacity(64)
        .add_connector(StreamingResponder::new(
            "llm",
            sink_id.clone(),
            vec!["The ", "incident ", "looks ", "routine ", "— ", "no ", "alert ", "needed."],
            Duration::from_millis(200),
        ))
        .add_connector(ProgressiveSink::new("telegram"))
        .build();

    let shutdown = octo.shutdown_token();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(3)).await;
        println!("\n[main] sending shutdown signal\n");
        shutdown.cancel();
    });

    octo.run().await?;
    println!("[main] done");
    Ok(())
}
