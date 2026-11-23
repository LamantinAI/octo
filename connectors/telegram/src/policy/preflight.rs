//! Verified ingress hooks run before ACL perception, hashing, or content logging.
use std::sync::Arc;

use octo_core::{ConnectorId, EnvelopeContext};
use teloxide::{
    Bot,
    requests::Requester,
    types::{ChatId, Message, MessageId, UserId},
};

use crate::{
    Role, TelegramConnector, TelegramHostPolicy,
    source::{CommandCatalog, GroupSettings, actor_id, addressed, command_name},
};

#[derive(Clone, Debug)]
pub struct VerifiedBot {
    pub connector: ConnectorId,
    pub bot_id: u64,
    pub username: String,
}

/// Native identity for a fresh host role lookup; never inferred from envelopes.
#[derive(Clone, Debug)]
pub struct VerifiedActor {
    pub bot: VerifiedBot,
    pub actor_id: i64,
    pub actor_is_bot: bool,
    pub chat_id: i64,
    pub chat_type: String,
}

/// Intentionally neither Debug nor Serialize: the body may contain credentials.
pub struct PreflightFrame {
    pub bot: VerifiedBot,
    pub update_id: u32,
    pub message_id: i32,
    pub date: i64,
    pub chat_id: i64,
    pub actor_id: Option<i64>,
    pub chat_type: String,
    pub actor_is_bot: bool,
    pub sender_chat_id: Option<i64>,
    pub forwarded: bool,
    pub text: Option<String>,
    pub caption: Option<String>,
    pub command_text: Option<String>,
    pub addressed: bool,
}

pub enum PreflightDecision {
    ContinueLegacy,
    /// The host supplies safe static/user-facing text, never an echoed credential.
    Consume {
        notice: Option<String>,
        delete_message: bool,
    },
    Admit {
        role: Role,
        context: EnvelopeContext,
    },
}

pub enum ActorRoleDecision {
    ContinueLegacy,
    /// None explicitly denies this actor, independently of legacy ACL entries.
    Role(Option<Role>),
}

#[derive(Clone)]
pub(crate) struct HostAuthorized {
    pub connector: ConnectorId,
    pub chat_id: i64,
}

impl PreflightFrame {
    pub(crate) fn from_message(
        bot: VerifiedBot,
        update_id: u32,
        message: &Message,
        settings: &GroupSettings,
        commands: &CommandCatalog,
    ) -> Self {
        let text = message.text().map(str::to_owned);
        let command_text = text
            .as_deref()
            .filter(|body| {
                message.forward_origin().is_none() && command_name(body, &bot.username).is_some()
            })
            .map(str::to_owned);
        Self {
            addressed: message.chat.is_private()
                || addressed(
                    message,
                    UserId(bot.bot_id),
                    &bot.username,
                    &settings.address_names,
                    commands,
                ),
            bot,
            update_id,
            message_id: message.id.0,
            date: message.date.timestamp(),
            chat_id: message.chat.id.0,
            actor_id: actor_id(message),
            chat_type: chat_type(message),
            actor_is_bot: message.from.as_ref().is_some_and(|actor| actor.is_bot),
            sender_chat_id: message.sender_chat.as_ref().map(|chat| chat.id.0),
            forwarded: message.forward_origin().is_some(),
            text,
            caption: message.caption().map(str::to_owned),
            command_text,
        }
    }
}

pub(crate) fn chat_type(message: &Message) -> String {
    if message.chat.is_private() {
        "private"
    } else if message.chat.is_group() {
        "group"
    } else if message.chat.is_supergroup() {
        "supergroup"
    } else {
        "channel"
    }
    .into()
}

pub(crate) async fn preflight(
    connector: &TelegramConnector,
    bot: &Bot,
    frame: PreflightFrame,
) -> Option<Option<(Role, EnvelopeContext)>> {
    let Some(policy) = &connector.policy else {
        return Some(None);
    };
    let chat = frame.chat_id;
    let message = frame.message_id;
    match policy.preflight(frame).await {
        Ok(PreflightDecision::ContinueLegacy) => Some(None),
        Ok(PreflightDecision::Admit { role, mut context }) => {
            context.insert(HostAuthorized {
                connector: connector.id.clone(),
                chat_id: chat,
            });
            Some(Some((role, context)))
        }
        Ok(PreflightDecision::Consume {
            notice,
            delete_message,
        }) => {
            if delete_message {
                let _ = bot.delete_message(ChatId(chat), MessageId(message)).await;
            }
            if let Some(notice) = notice {
                let _ = bot.send_message(ChatId(chat), notice).await;
            }
            None
        }
        Err(_) => {
            let _ = bot
                .send_message(
                    ChatId(chat),
                    "This message could not be authenticated. Please try again.",
                )
                .await;
            None
        }
    }
}

/// Host identity lookup has no access to model-authored envelope metadata.
pub(crate) async fn actor_role(
    policy: Option<&Arc<dyn TelegramHostPolicy>>,
    actor: VerifiedActor,
) -> Result<ActorRoleDecision, String> {
    if actor.actor_is_bot {
        return Ok(ActorRoleDecision::Role(None));
    }
    match policy {
        Some(policy) => policy.actor_role(actor).await,
        None => Ok(ActorRoleDecision::ContinueLegacy),
    }
}

#[cfg(test)]
mod tests {
    use super::{PreflightDecision, PreflightFrame, VerifiedBot};
    use crate::{
        Acl, IngressDecision, IngressScope, NativeFrame, Role, TelegramConnector,
        TelegramHostPolicy, control::outgoing_allowed,
    };
    use async_trait::async_trait;
    use axum::{Json, Router, http::Uri, routing::post, serve};
    use octo_core::{
        ChannelId, ConnectorContext, ConnectorId, Envelope, EnvelopeContext, EventBus, Filter,
        InProcessBus, SubscribeOptions,
    };
    use reqwest::Url;
    use serde_json::{Value, json};
    use std::{
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };
    use teloxide::Bot;
    use tokio::{
        net::TcpListener,
        spawn,
        time::{sleep, timeout},
    };

    #[derive(Clone)]
    struct HostStamp;
    struct Host {
        ready: AtomicBool,
        seen: Mutex<Vec<(i32, bool, bool)>>,
        admitted: Mutex<Vec<i32>>,
    }
    #[async_trait]
    impl TelegramHostPolicy for Host {
        async fn bot_ready(&self, bot: VerifiedBot) -> Result<(), String> {
            assert_eq!(bot.bot_id, 9);
            assert_eq!(bot.username, "test_bot");
            self.ready.store(true, Ordering::SeqCst);
            Ok(())
        }
        async fn preflight(&self, frame: PreflightFrame) -> Result<PreflightDecision, String> {
            assert!(self.ready.load(Ordering::SeqCst));
            self.seen.lock().unwrap().push((
                frame.message_id,
                frame.forwarded,
                frame.caption.is_some(),
            ));
            if frame
                .text
                .as_deref()
                .or(frame.caption.as_deref())
                .is_some_and(|body| body.starts_with("/login"))
            {
                return Ok(PreflightDecision::Consume {
                    notice: None,
                    delete_message: false,
                });
            }
            let mut context = EnvelopeContext::default();
            context.insert(HostStamp);
            Ok(PreflightDecision::Admit {
                role: if frame.chat_id > 0 {
                    Role::Owner
                } else {
                    Role::Guest
                },
                context,
            })
        }
        async fn admit(
            &self,
            frame: NativeFrame,
            envelope: Envelope,
        ) -> Result<IngressDecision, String> {
            assert!(envelope.context.get::<HostStamp>().is_some());
            assert!(!envelope.payload_as::<String>().unwrap().contains("/login"));
            self.admitted.lock().unwrap().push(frame.message_id);
            Ok(IngressDecision::Accept {
                envelope,
                scope: IngressScope {
                    key: frame.chat_id.to_string(),
                    epoch: "epoch".into(),
                    ephemeral: false,
                    cleanup_scope: None,
                },
            })
        }
    }
    fn message(id: i32, chat: i64, body: &str) -> Value {
        json!({"update_id":id,"message":{"message_id":id,"date":1,"from":{"id":7,"is_bot":false,"first_name":"Human"},"chat":if chat>0 {json!({"id":chat,"type":"private","first_name":"Human"})} else {json!({"id":chat,"type":"supergroup","title":"Group"})},"text":body}})
    }
    #[tokio::test]
    async fn preflight_consumes_credentials_before_acl_or_native_digest_and_admits_host_identity() {
        let mut caption = message(2, -42, "unused");
        caption["message"].as_object_mut().unwrap().remove("text");
        caption["message"]["photo"] =
            json!([{"file_id":"fake","file_unique_id":"unique","width":1,"height":1}]);
        caption["message"]["caption"] = json!("/login malformed");
        caption["message"]["forward_origin"] =
            json!({"type":"hidden_user","date":1,"sender_user_name":"Original"});
        let updates = json!([
            message(1, 7, "/login opaque"),
            caption,
            message(3, 7, "ordinary"),
            message(4, -42, "@test_bot ordinary")
        ]);
        let served = Arc::new(AtomicBool::new(false));
        let capture = served.clone();
        let app = Router::new().fallback(post(move |uri:Uri, Json(_):Json<Value>| {
            let method = uri.path().to_ascii_lowercase();
            let served = capture.clone(); let updates = updates.clone();
            async move {
                let result = if method.ends_with("getme") { json!({"id":9,"is_bot":true,"first_name":"Bot","username":"test_bot","can_join_groups":true,"can_read_all_group_messages":false,"supports_inline_queries":false,"has_main_web_app":false}) }
                else if method.ends_with("getwebhookinfo") { json!({"url":"","has_custom_certificate":false,"pending_update_count":0}) }
                else if method.ends_with("getupdates") { if !served.swap(true,Ordering::SeqCst) { updates } else { sleep(Duration::from_millis(10)).await; json!([]) } }
                else { json!(true) };
                Json(json!({"ok":true,"result":result}))
            }
        }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = spawn(async move { serve(listener, app).await.unwrap() });
        let bot = Bot::new("9:FAKE").set_api_url(Url::parse(&format!("http://{address}")).unwrap());
        let host = Arc::new(Host {
            ready: AtomicBool::new(false),
            seen: Mutex::new(vec![]),
            admitted: Mutex::new(vec![]),
        });
        let mut connector = TelegramConnector::with_acl("telegram", "9:FAKE", Acl::new(), None);
        Arc::get_mut(&mut connector).unwrap().policy = Some(host.clone());
        let bus = Arc::new(InProcessBus::new(16));
        let mut events = bus
            .subscribe(Filter::by_kind("chat.message"), SubscribeOptions::default())
            .await
            .unwrap();
        let ctx = ConnectorContext::new(Default::default(), bus);
        let running_ctx = ctx.clone();
        let task = spawn(async move { connector.run_with_bot(running_ctx, bot).await });
        let first = timeout(Duration::from_secs(3), events.next())
            .await
            .unwrap()
            .unwrap();
        let second = timeout(Duration::from_secs(3), events.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            first.channel_metadata.as_ref().unwrap().tags["role"],
            "owner"
        );
        let reply = first
            .as_ref()
            .clone()
            .with_target(ConnectorId::new("telegram"));
        assert!(outgoing_allowed(&None, &Default::default(), &reply));
        let forged_destination = reply.clone().with_channel(ChannelId::new("123"));
        let empty_acl = TelegramConnector::with_acl("test", "9:FAKE", Acl::new(), None);
        assert!(outgoing_allowed(
            &empty_acl.acl,
            &Default::default(),
            &reply
        ));
        assert!(!outgoing_allowed(
            &empty_acl.acl,
            &Default::default(),
            &forged_destination
        ));
        assert_eq!(
            second.channel_metadata.as_ref().unwrap().tags["role"],
            "guest"
        );
        assert_eq!(
            *host.seen.lock().unwrap(),
            vec![
                (1, false, false),
                (2, true, true),
                (3, false, false),
                (4, false, false)
            ]
        );
        assert_eq!(*host.admitted.lock().unwrap(), vec![3, 4]);
        ctx.shutdown.cancel();
        timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        server.abort();
    }
}
