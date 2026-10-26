use std::sync::Arc;

use octo_core::{ConnectorContext, ConnectorId, Envelope};
use serde_json::{Value, json, to_value, to_vec};
use sha2::{Digest, Sha256};
use teloxide::{
    Bot,
    requests::Requester,
    types::{ChatId, Message},
};

use super::delivery::notice;
use super::tracking::Tracker;
use super::{IngressDecision, NativeFrame};
use crate::{TelegramConnector, live::Live, source::MessageSource};

impl NativeFrame {
    pub(crate) fn from_message(
        connector: &ConnectorId,
        bot_id: u64,
        update_id: u32,
        message: &Message,
        source: &MessageSource,
    ) -> Result<Self, String> {
        let raw = to_value(message)
            .map_err(|_| "Cannot encode Telegram message identity.".to_string())?;
        let author = source
            .tags
            .get("sender_id")
            .and_then(|id| id.parse::<i64>().ok());
        let mut body = json!({"bot_id":bot_id,"chat_id":message.chat.id.0,"actor_id":author,
            "sender_chat_id":message.sender_chat.as_ref().map(|s|s.id.0),"text":message.text(),"caption":message.caption(),
            "forwarded":message.forward_origin().is_some(),"reply_to_message_id":message.reply_to_message().map(|m|m.id.0)});
        for key in [
            "photo",
            "voice",
            "audio",
            "video",
            "video_note",
            "document",
            "animation",
            "sticker",
            "dice",
            "location",
            "venue",
            "contact",
            "poll",
        ] {
            if let Some(value) = raw.get(key) {
                body[key] = semantic_media(value);
            }
        }
        Ok(Self {
            connector: connector.clone(),
            bot_id,
            update_id,
            message_id: message.id.0,
            date: message.date.timestamp(),
            chat_id: message.chat.id.0,
            actor_id: source.tags.get("sender_id").and_then(|id| id.parse().ok()),
            chat_type: source
                .tags
                .get("chat_type")
                .cloned()
                .unwrap_or_else(|| "unknown".into()),
            command_text: message
                .forward_origin()
                .is_none()
                .then(|| {
                    source
                        .tags
                        .get("command_text")
                        .filter(|text| {
                            text.starts_with('/') && message.text() == Some(text.as_str())
                        })
                        .cloned()
                })
                .flatten(),
            digest: format!(
                "{:x}",
                Sha256::digest(
                    to_vec(&body)
                        .map_err(|_| "Cannot encode Telegram message identity.".to_string())?
                )
            ),
            media: message.text().is_none(),
        })
    }
}

fn semantic_media(value: &Value) -> Value {
    match value {
        Value::Object(object) => Value::Object(
            object
                .iter()
                .filter(|(key, _)| !matches!(key.as_str(), "file_id" | "file_size"))
                .map(|(key, value)| (key.clone(), semantic_media(value)))
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.iter().map(semantic_media).collect()),
        _ => value.clone(),
    }
}

pub(crate) async fn admit(
    connector: &TelegramConnector,
    bot: &Bot,
    ctx: &ConnectorContext,
    tracker: &Arc<Tracker>,
    live: &Live,
    frame: NativeFrame,
    source: &mut MessageSource,
    provisional: Envelope,
) -> bool {
    let Some(policy) = &connector.policy else {
        source.frames = vec![frame];
        return true;
    };
    let chat = frame.chat_id;
    match policy.admit(frame.clone(), provisional).await {
        Ok(IngressDecision::Accept { envelope, scope }) => {
            if envelope.source != connector.id
                || envelope.channel.as_ref().map(|c| c.as_str()) != Some(chat.to_string().as_str())
            {
                return false;
            }
            match tracker.native(&scope, &frame) {
                Ok(false) => {}
                Ok(true) => return false,
                Err(()) => {
                    notice(
                        bot,
                        tracker,
                        Some(policy),
                        &connector.id,
                        chat,
                        "Temporary message tracking is unavailable.".into(),
                        None,
                    )
                    .await;
                    return false;
                }
            }
            source.pin(envelope, scope, frame);
            true
        }
        Ok(IngressDecision::Control {
            notice: message,
            scope,
            post_events,
            cleanup,
        }) => {
            if let Some(scope) = &scope {
                let _ = tracker.native(scope, &frame);
            }
            if let Some(request) = &cleanup {
                tracker.close(request);
                live.end_scope(request);
            }
            for event in post_events {
                let _ = ctx.publish(event).await;
            }
            if let Some(text) = message {
                notice(
                    bot,
                    tracker,
                    Some(policy),
                    &connector.id,
                    chat,
                    text,
                    scope.clone(),
                )
                .await;
            }
            if let Some(request) = cleanup {
                let tracker = tracker.clone();
                let bot = bot.clone();
                let policy = policy.clone();
                tokio::spawn(async move {
                    let report = tracker.cleanup(&bot, &request).await;
                    policy.cleanup_finished(&report);
                    if report.failed + report.unknown > 0 {
                        let _ = bot
                            .send_message(
                                ChatId(chat),
                                "Some temporary messages could not be removed from Telegram.",
                            )
                            .await;
                    }
                });
            }
            false
        }
        Ok(IngressDecision::Ignore) => false,
        Err(_) => {
            let _ = bot
                .send_message(
                    ChatId(chat),
                    "This message could not be admitted. Please try again.",
                )
                .await;
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{NativeFrame, admit};
    use crate::{
        IngressDecision, IngressScope, NativeBatch, TelegramConnector, TelegramHostPolicy,
        inbound::chat_envelope, live::Live, policy::tracking::Tracker, source::MessageSource,
    };
    use async_trait::async_trait;
    use octo_approval::{Access, Authority, Ingress, Origin};
    use octo_core::{
        ConnectorContext, ConnectorId, Envelope, EventBus, EventKind, Filter, InProcessBus,
        SubscribeOptions,
    };
    use serde_json::{from_value, json};
    use std::{
        collections::HashMap,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };
    use teloxide::{Bot, types::Message};
    use tokio::time::timeout;

    #[derive(Clone)]
    struct HostStamp {
        name: String,
        closed: Arc<AtomicBool>,
    }
    struct Policy {
        current: Mutex<(String, Arc<AtomicBool>)>,
    }
    #[async_trait]
    impl TelegramHostPolicy for Policy {
        async fn admit(
            &self,
            frame: NativeFrame,
            env: Envelope,
        ) -> Result<IngressDecision, String> {
            let mut current = self.current.lock().unwrap();
            if frame.command_text.as_deref() == Some("/end") {
                current.1.store(true, Ordering::SeqCst);
                *current = ("original".into(), Arc::new(AtomicBool::new(false)));
                return Ok(IngressDecision::Control {
                    notice: None,
                    scope: None,
                    post_events: vec![Envelope::new(
                        ConnectorId::new("host"),
                        EventKind::new("host.ended"),
                        true,
                    )],
                    cleanup: None,
                });
            }
            Ok(IngressDecision::Accept {
                envelope: env.with_context(HostStamp {
                    name: current.0.clone(),
                    closed: current.1.clone(),
                }),
                scope: IngressScope {
                    key: current.0.clone(),
                    epoch: "epoch".into(),
                    ephemeral: false,
                    cleanup_scope: None,
                },
            })
        }
        fn allow_publish(&self, env: &Envelope) -> bool {
            env.context
                .get::<HostStamp>()
                .is_none_or(|stamp| !stamp.closed.load(Ordering::SeqCst))
        }
    }
    fn message(id: i32, text: &str) -> Message {
        from_value(json!({"message_id":id,"date":1700000000,"chat":{"id":42,"type":"private","first_name":"Person"},"from":{"id":7,"is_bot":false,"first_name":"Person","username":"name"},"text":text})).unwrap()
    }
    fn source() -> MessageSource {
        MessageSource {
            tags: HashMap::from([
                ("sender_id".into(), "7".into()),
                ("chat_type".into(), "private".into()),
                ("role".into(), "owner".into()),
            ]),
            ..MessageSource::default()
        }
    }
    #[tokio::test]
    async fn plain_factory_preserves_unscoped_ingress_without_policy() {
        let connector = TelegramConnector::new("telegram", "123:FAKE");
        let bus = Arc::new(InProcessBus::new(4));
        let ctx = ConnectorContext::new(Default::default(), bus);
        let tracker = Tracker::new(Default::default());
        let live = Live::new();
        let bot = Bot::new("123:FAKE");
        let mut source = source();
        let frame =
            NativeFrame::from_message(&connector.id, 99, 1, &message(3, "text"), &source).unwrap();
        let provisional = source.clone().apply(chat_envelope(
            &connector.id,
            "42",
            "text".to_string(),
            None,
            None,
        ));
        assert!(
            admit(
                &connector,
                &bot,
                &ctx,
                &tracker,
                &live,
                frame,
                &mut source,
                provisional
            )
            .await
        );
        assert!(source.scope.is_none());
        assert!(source.pinned.is_none());
        let env = source.apply(chat_envelope(
            &connector.id,
            "42",
            "text".to_string(),
            None,
            None,
        ));
        assert_eq!(env.payload_as::<String>().unwrap(), "text");
        assert_eq!(env.context.get::<NativeBatch>().unwrap().frames.len(), 1);
    }

    #[tokio::test]
    async fn full_context_and_all_native_frames_remain_pinned_through_buffer_and_end() {
        let policy = Arc::new(Policy {
            current: Mutex::new(("first".into(), Arc::new(AtomicBool::new(false)))),
        });
        let mut connector = TelegramConnector::new("telegram", "123:FAKE");
        Arc::get_mut(&mut connector).unwrap().policy = Some(policy.clone());
        let bus = Arc::new(InProcessBus::new(32));
        let ctx = ConnectorContext::new(Default::default(), bus.clone());
        let tracker = Tracker::new(Default::default());
        let live = Live::new();
        let bot = Bot::new("123:FAKE");
        let authority = Authority::new(Duration::from_secs(60), 32);
        let mut first = source();
        first.proof = Some(authority.issue(
            Origin {
                principal: "owner".into(),
                connector: "telegram".into(),
                conversation: "42".into(),
                private_destination: "42".into(),
                may_approve: true,
            },
            Access::owner(),
        ));
        for (index, id) in [90, 3].into_iter().enumerate() {
            let mut next = if index == 0 { first.clone() } else { source() };
            if index != 0 {
                next.proof = first.proof.clone();
            }
            let msg = message(id, "payload");
            let frame =
                NativeFrame::from_message(&connector.id, 99, 10 + index as u32, &msg, &next)
                    .unwrap();
            let provisional = next.clone().apply(chat_envelope(
                &connector.id,
                "42",
                "payload".to_string(),
                None,
                None,
            ));
            assert!(
                admit(
                    &connector,
                    &bot,
                    &ctx,
                    &tracker,
                    &live,
                    frame,
                    &mut next,
                    provisional
                )
                .await
            );
            if index == 0 {
                first = next
            } else {
                first.merge(next)
            }
        }
        let ready = first.clone().apply(chat_envelope(
            &connector.id,
            "42",
            "batched payload".to_string(),
            None,
            None,
        ));
        assert_eq!(ready.context.get::<HostStamp>().unwrap().name, "first");
        assert!(ready.context.get::<Ingress>().is_some());
        assert_eq!(
            ready
                .context
                .get::<NativeBatch>()
                .unwrap()
                .frames
                .iter()
                .map(|f| f.message_id)
                .collect::<Vec<_>>(),
            vec![90, 3]
        );
        let mut events = bus
            .subscribe(Filter::by_kind("host.ended"), SubscribeOptions::default())
            .await
            .unwrap();
        let mut ending = source();
        ending.tags.insert("command_text".into(), "/end".into());
        let msg = message(1, "/end");
        let frame = NativeFrame::from_message(&connector.id, 99, 20, &msg, &ending).unwrap();
        let provisional = ending.clone().apply(chat_envelope(
            &connector.id,
            "42",
            "/end".to_string(),
            None,
            None,
        ));
        assert!(
            !admit(
                &connector,
                &bot,
                &ctx,
                &tracker,
                &live,
                frame,
                &mut ending,
                provisional
            )
            .await
        );
        assert!(
            timeout(Duration::from_secs(1), events.next())
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            !policy.allow_publish(&ready),
            "late buffered input is fenced, not reassigned"
        );
        assert_eq!(ready.context.get::<HostStamp>().unwrap().name, "first");
        let mut next = source();
        let msg = message(2, "new input");
        let frame = NativeFrame::from_message(&connector.id, 99, 21, &msg, &next).unwrap();
        let provisional = next.clone().apply(chat_envelope(
            &connector.id,
            "42",
            "new input".to_string(),
            None,
            None,
        ));
        assert!(
            admit(
                &connector,
                &bot,
                &ctx,
                &tracker,
                &live,
                frame,
                &mut next,
                provisional
            )
            .await
        );
        let next = next.apply(chat_envelope(
            &connector.id,
            "42",
            "after navigation".to_string(),
            None,
            None,
        ));
        assert_eq!(next.context.get::<HostStamp>().unwrap().name, "original");
    }
    #[test]
    fn semantic_identity_ignores_display_labels_and_download_ids_but_binds_content() {
        let id = ConnectorId::new("telegram");
        let mut src = source();
        src.tags.insert("command_text".into(), "/tmp".into());
        let mut a = message(5, "/tmp");
        let first = NativeFrame::from_message(&id, 99, 1, &a, &src).unwrap();
        a.from.as_mut().unwrap().first_name = "Changed display".into();
        a.from.as_mut().unwrap().username = Some("changed".into());
        assert_eq!(
            first.digest,
            NativeFrame::from_message(&id, 99, 1, &a, &src)
                .unwrap()
                .digest
        );
        assert_ne!(
            first.digest,
            NativeFrame::from_message(&id, 100, 1, &a, &src)
                .unwrap()
                .digest
        );
        let forwarded:Message=from_value(json!({"message_id":5,"date":1700000000,"chat":{"id":42,"type":"private","first_name":"Person"},"from":{"id":7,"is_bot":false,"first_name":"Person"},"text":"/tmp","forward_origin":{"type":"hidden_user","date":1600000000,"sender_user_name":"Other"}})).unwrap();
        assert!(
            NativeFrame::from_message(&id, 99, 1, &forwarded, &src)
                .unwrap()
                .command_text
                .is_none()
        );
        let photo = |download: &str, unique: &str| {
            from_value::<Message>(json!({"message_id":5,"date":1700000000,"chat":{"id":42,"type":"private","first_name":"Person"},"photo":[{"file_id":download,"file_unique_id":unique,"width":10,"height":10}]})).unwrap()
        };
        assert_eq!(
            NativeFrame::from_message(&id, 99, 1, &photo("old", "same"), &src)
                .unwrap()
                .digest,
            NativeFrame::from_message(&id, 99, 1, &photo("new", "same"), &src)
                .unwrap()
                .digest
        );
        assert_ne!(
            NativeFrame::from_message(&id, 99, 1, &photo("new", "different"), &src)
                .unwrap()
                .digest,
            NativeFrame::from_message(&id, 99, 1, &photo("new", "same"), &src)
                .unwrap()
                .digest
        );
    }
}
