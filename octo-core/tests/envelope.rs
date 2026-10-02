use std::sync::Arc;

use async_trait::async_trait;
use octo_core::{
    ChannelId, ChannelMetadata, Connector, ConnectorCapabilities, ConnectorContext, ConnectorId,
    DeliveryMode, Direction, Envelope, EventId, EventKind, Filter, Lifecycle, Octo, OctoResult,
    Priority, ReplayMode, RuleId, StreamFrame, SubscribeOptions, TrailAction, TrailActor,
    TrailEntry, TrustLevel,
};
mod common;

#[test]
fn build_envelope_with_decorators() {
    let env = Envelope::new(
        ConnectorId::new("telegram"),
        EventKind::from_static("telegram.command"),
        "/help".to_string(),
    )
    .with_target(ConnectorId::new("ops_router"))
    .with_channel(ChannelId::new("owner"))
    .with_priority(Priority::High)
    .with_tag("test", "true")
    .with_channel_metadata(
        ChannelMetadata::new()
            .with_trust(TrustLevel::High)
            .with_priority(Priority::High),
    );

    assert_eq!(env.source.as_str(), "telegram");
    assert_eq!(
        env.target.as_ref().map(ConnectorId::as_str),
        Some("ops_router")
    );
    assert_eq!(env.kind.as_str(), "telegram.command");
    assert_eq!(env.payload_as::<String>(), Some(&"/help".to_string()));
    assert_eq!(env.priority, Priority::High);
    assert_eq!(env.tags.get("test").map(String::as_str), Some("true"));
    assert!(env.channel_metadata.is_some());
}

#[test]
fn lifecycle_transitions_and_predicates() {
    assert!(Lifecycle::Created.can_transition_to(Lifecycle::Registering));
    assert!(!Lifecycle::Created.can_transition_to(Lifecycle::Healthy));
    assert!(Lifecycle::Healthy.is_running());
    assert!(Lifecycle::Stopped.is_terminal());
}

#[test]
fn trail_chain_decorates_envelope() {
    let mut env = Envelope::new(
        ConnectorId::new("camera"),
        EventKind::from_static("vision.incident.fight"),
        (),
    );
    env.push_trail(TrailEntry::new(
        TrailActor::Reflex(RuleId::new("intrusion_alert")),
        TrailAction::Tag {
            added: vec!["high_severity".into()],
        },
    ));
    env.push_trail(TrailEntry::new(
        TrailActor::Cognition {
            backend: "claude-haiku".into(),
        },
        TrailAction::Decision {
            summary: "alert_owner".into(),
        },
    ));

    assert_eq!(env.trail.len(), 2);
}

#[test]
fn capabilities_builder() {
    let caps = ConnectorCapabilities::input_only()
        .with_delivery(DeliveryMode::AtLeastOnce)
        .with_emit_kinds([
            EventKind::from_static("vision.incident.fight"),
            EventKind::from_static("vision.entity.entered_zone"),
        ])
        .with_streaming(true)
        .with_replay(ReplayMode::LastN(100));

    assert_eq!(caps.direction, Direction::InputOnly);
    assert_eq!(caps.delivery, DeliveryMode::AtLeastOnce);
    assert_eq!(caps.event_kinds_emit.len(), 2);
    assert!(caps.streaming);
    assert_eq!(caps.replay, ReplayMode::LastN(100));
}

/// Streaming protocol smoke-test: a connector emits 3 chunks of one stream
/// (Open, Chunk, Close), an external subscriber collects them by
/// `correlation_id` and verifies the assembled text.
#[tokio::test(flavor = "current_thread")]
async fn stream_chunks_collect_by_correlation_id() {
    struct ChunkySource {
        id: ConnectorId,
        capabilities: ConnectorCapabilities,
    }

    #[async_trait]
    impl Connector for ChunkySource {
        fn id(&self) -> &ConnectorId {
            &self.id
        }
        fn capabilities(&self) -> &ConnectorCapabilities {
            &self.capabilities
        }
        async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
            let stream_id = EventId::new();
            let kind = EventKind::from_static("text.stream");
            let frames = [
                (StreamFrame::Open, "hello "),
                (StreamFrame::Chunk, "from "),
                (StreamFrame::Close, "stream"),
            ];
            for (frame, text) in frames {
                ctx.publish(
                    Envelope::new(self.id.clone(), kind.clone(), text.to_string())
                        .with_correlation(stream_id)
                        .with_stream_frame(frame),
                )
                .await?;
            }
            Ok(())
        }
    }

    let octo = Octo::builder()
        .bus_capacity(16)
        .add_connector(Arc::new(ChunkySource {
            id: ConnectorId::new("chunky"),
            capabilities: ConnectorCapabilities::input_only(),
        }))
        .build();

    let mut sub = octo
        .subscribe(Filter::all(), SubscribeOptions::default())
        .await
        .unwrap();

    let collector = tokio::spawn(async move {
        let mut chunks: Vec<Arc<Envelope>> = Vec::new();
        while let Some(env) = sub.next().await {
            if env.is_stream() {
                chunks.push(env.clone());
                if env.stream == Some(StreamFrame::Close) {
                    break;
                }
            }
        }
        chunks
    });

    tokio::time::timeout(std::time::Duration::from_secs(15), octo.run())
        .await
        .expect("runtime must stop")
        .unwrap();
    let chunks = collector.await.unwrap();

    assert_eq!(chunks.len(), 3);
    assert_eq!(chunks[0].stream, Some(StreamFrame::Open));
    assert_eq!(chunks[1].stream, Some(StreamFrame::Chunk));
    assert_eq!(chunks[2].stream, Some(StreamFrame::Close));

    // All chunks share one correlation_id.
    let cid = chunks[0]
        .correlation_id
        .expect("Open carries correlation_id");
    assert!(chunks.iter().all(|c| c.correlation_id == Some(cid)));

    // Reassembled text.
    let assembled: String = chunks
        .iter()
        .map(|c| c.payload_as::<String>().cloned().unwrap_or_default())
        .collect();
    assert_eq!(assembled, "hello from stream");
}
