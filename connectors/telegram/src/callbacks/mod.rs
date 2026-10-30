//! Verified transport callbacks, separate from messages and approval authority.
#[cfg(test)]
mod ordering;
mod transport;
pub(crate) use transport::{CallbackInput, Callbacks};

use std::time::Duration;

use octo_core::{ConnectorId, Envelope};

/// Actual Telegram facts only. Possession does not confer native tool authority.
#[derive(Clone, Debug)]
pub struct CallbackFrame {
    pub connector: ConnectorId,
    pub bot_id: u64,
    pub update_id: u32,
    pub query_id: String,
    pub actor_id: u64,
    pub message: Option<CallbackMessage>,
    pub inline_message_id: Option<String>,
    pub data: String,
}
#[derive(Clone, Debug)]
pub struct CallbackMessage {
    pub chat_id: i64,
    pub message_id: i32,
    pub chat_type: String,
    pub accessible: bool,
    pub sender_id: Option<u64>,
}
#[derive(Clone, Debug)]
pub struct CallbackLimits {
    pub max_concurrent: usize,
    pub timeout: Duration,
}
impl Default for CallbackLimits {
    fn default() -> Self {
        Self {
            max_concurrent: 8,
            timeout: Duration::from_secs(5),
        }
    }
}
/// Bounded host response. Events still pass through normal host delivery fences.
pub struct CallbackDecision {
    text: Option<String>,
    show_alert: bool,
    post_events: Vec<Envelope>,
}
impl CallbackDecision {
    pub fn into_parts(self) -> (Option<String>, bool, Vec<Envelope>) {
        (self.text, self.show_alert, self.post_events)
    }
    pub fn new(
        text: Option<String>,
        show_alert: bool,
        post_events: Vec<Envelope>,
    ) -> Result<Self, String> {
        if text
            .as_ref()
            .is_some_and(|text| text.chars().count() > 200 || text.chars().any(char::is_control))
            || post_events.len() > 16
            || post_events
                .iter()
                .any(|event| event.kind.as_str() == "chat.message")
        {
            return Err("callback acknowledgement or event list exceeds control bounds".into());
        }
        Ok(Self {
            text,
            show_alert,
            post_events,
        })
    }
    pub fn unavailable() -> Self {
        Self {
            text: Some("This action is unavailable. Please request a fresh list.".into()),
            show_alert: false,
            post_events: Vec::new(),
        }
    }
}

#[cfg(test)]
pub(crate) mod testing;
#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex, RwLock},
        time::Duration,
    };

    use async_trait::async_trait;
    use octo_approval::Ingress;
    use octo_core::{
        ConnectorContext, ConnectorId, Envelope, EventKind, Filter, InProcessBus, SubscribeOptions,
    };
    use serde_json::{Value, from_value, json, to_value};
    use teloxide::types::CallbackQuery;
    use tokio::{
        join, spawn,
        time::{sleep, timeout},
    };

    use super::{
        CallbackDecision, CallbackFrame, CallbackInput, CallbackLimits, Callbacks,
        testing::{blocked_ack_server, server},
    };
    use crate::{
        Acl, AclState, GroupMode, IngressDecision, NativeFrame, Role, TelegramHostPolicy,
        approval::Cards, source::GroupSettings,
    };

    #[derive(Default)]
    struct Policy {
        frames: Mutex<Vec<CallbackFrame>>,
        slow: bool,
    }
    #[async_trait]
    impl TelegramHostPolicy for Policy {
        async fn admit(&self, _: NativeFrame, _: Envelope) -> Result<IngressDecision, String> {
            Ok(IngressDecision::Ignore)
        }
        async fn callback(&self, frame: CallbackFrame) -> Result<CallbackDecision, String> {
            self.frames.lock().unwrap().push(frame);
            if self.slow {
                sleep(Duration::from_secs(1)).await;
            }
            CallbackDecision::new(
                Some("Opened".into()),
                false,
                vec![Envelope::new(
                    ConnectorId::new("host"),
                    EventKind::new("host.control"),
                    "done".to_string(),
                )],
            )
        }
        fn callback_limits(&self) -> CallbackLimits {
            CallbackLimits {
                max_concurrent: 1,
                timeout: Duration::from_millis(100),
            }
        }
    }
    fn query(data: &str) -> CallbackQuery {
        from_value(json!({"id":"query-1","from":{"id":7,"is_bot":false,"first_name":"Clicker"},"chat_instance":"instance","data":data,"message":{"message_id":17,"date":1,"from":{"id":9,"is_bot":true,"first_name":"Bot"},"chat":{"id":7,"type":"private","first_name":"Clicker"},"text":"old list"}})).unwrap()
    }
    fn input(query: CallbackQuery) -> CallbackInput {
        CallbackInput {
            connector: ConnectorId::new("telegram"),
            bot_id: 9,
            update_id: 101,
            query,
        }
    }
    fn ctx() -> ConnectorContext {
        ConnectorContext::new(Default::default(), Arc::new(InProcessBus::new(32)))
    }
    fn acl() -> Option<Arc<AclState>> {
        let mut acl = Acl::new();
        acl.ensure(7, Role::Guest);
        Some(Arc::new(AclState {
            acl: RwLock::new(acl),
            path: None,
        }))
    }
    #[tokio::test]
    async fn callback_uses_actual_clicker_transport_facts_ack_and_host_events_without_ingress() {
        let server = server().await;
        let ctx = ctx();
        let policy = Arc::new(Policy::default());
        let callbacks = Callbacks::new(Some(policy.clone()));
        let mut events = ctx
            .subscribe(Filter::by_kind("host.control"), SubscribeOptions::default())
            .await
            .unwrap();
        callbacks
            .handle(
                &server.bot,
                input(query("session:opaque")),
                &acl(),
                &GroupSettings::default(),
                &ctx,
            )
            .await;
        let event = timeout(Duration::from_secs(1), events.next())
            .await
            .unwrap()
            .unwrap();
        assert!(event.context.get::<Ingress>().is_none());
        server.wait("answercallbackquery", 1).await;
        let frames = policy.frames.lock().unwrap();
        let frame = &frames[0];
        assert_eq!(frame.actor_id, 7);
        assert_eq!(frame.bot_id, 9);
        assert_eq!(frame.update_id, 101);
        assert_eq!(frame.query_id, "query-1");
        assert_eq!(frame.message.as_ref().unwrap().sender_id, Some(9));
        assert!(frame.message.as_ref().unwrap().accessible);
        assert!(
            server
                .requests
                .lock()
                .unwrap()
                .iter()
                .all(|(method, _)| method.ends_with("answercallbackquery"))
        );
    }
    #[tokio::test]
    async fn missing_inline_only_and_acl_denied_callbacks_are_acknowledged_without_host_effects() {
        let server = server().await;
        let ctx = ctx();
        let policy = Arc::new(Policy::default());
        let callbacks = Callbacks::new(Some(policy.clone()));
        let base = query("session:opaque");
        let mut missing = base.clone();
        missing.message = None;
        let mut inline = missing.clone();
        inline.inline_message_id = Some("inline-only".into());
        let mut foreign = base.clone();
        foreign.from.id.0 = 8;
        let mut robot = base.clone();
        robot.from.is_bot = true;
        for query in [missing, inline, foreign, robot] {
            callbacks
                .handle(
                    &server.bot,
                    input(query),
                    &acl(),
                    &GroupSettings::default(),
                    &ctx,
                )
                .await;
        }
        let denied = Some(Arc::new(AclState {
            acl: RwLock::new(Acl::new()),
            path: None,
        }));
        callbacks
            .handle(
                &server.bot,
                input(base),
                &denied,
                &GroupSettings::default(),
                &ctx,
            )
            .await;
        server.wait("answercallbackquery", 5).await;
        assert!(policy.frames.lock().unwrap().is_empty());
    }
    #[tokio::test]
    async fn inaccessible_message_facts_remain_explicit_and_approval_namespace_never_falls_through()
    {
        let server = server().await;
        let ctx = ctx();
        let policy = Arc::new(Policy::default());
        let callbacks = Callbacks::new(Some(policy.clone()));
        let mut value: Value = to_value(query("session:opaque")).unwrap();
        value["message"]["date"] = json!(0);
        let inaccessible: CallbackQuery = from_value(value).unwrap();
        callbacks
            .handle(
                &server.bot,
                input(inaccessible),
                &acl(),
                &GroupSettings::default(),
                &ctx,
            )
            .await;
        server.wait("answercallbackquery", 1).await;
        assert!(
            !policy.frames.lock().unwrap()[0]
                .message
                .as_ref()
                .unwrap()
                .accessible
        );
        callbacks
            .handle(
                &server.bot,
                input(query("oa:not-a-real-card")),
                &acl(),
                &GroupSettings::default(),
                &ctx,
            )
            .await;
        let cards = Cards::new(Default::default());
        assert!(
            cards
                .callback(
                    &server.bot,
                    &ConnectorId::new("telegram"),
                    &acl(),
                    &None,
                    query("oa:not-a-real-card"),
                    &ctx,
                    Duration::from_secs(1)
                )
                .await
        );
        server.wait("answercallbackquery", 2).await;
        assert_eq!(policy.frames.lock().unwrap().len(), 1);
    }
    #[tokio::test]
    async fn bounded_jobs_busy_and_timeout_each_ack_without_post_events() {
        let server = server().await;
        let ctx = ctx();
        let policy = Arc::new(Policy {
            slow: true,
            ..Policy::default()
        });
        let callbacks = Callbacks::new(Some(policy.clone()));
        let mut events = ctx
            .subscribe(Filter::by_kind("host.control"), SubscribeOptions::default())
            .await
            .unwrap();
        let acl = acl();
        let groups = GroupSettings::default();
        join!(
            callbacks.handle(&server.bot, input(query("one")), &acl, &groups, &ctx),
            callbacks.handle(&server.bot, input(query("two")), &acl, &groups, &ctx)
        );
        server.wait("answercallbackquery", 2).await;
        assert_eq!(policy.frames.lock().unwrap().len(), 1);
        assert!(
            timeout(Duration::from_millis(20), events.next())
                .await
                .is_err()
        );
    }
    #[tokio::test]
    async fn callbacks_respect_current_group_mode_and_actor_acl() {
        let server = server().await;
        let ctx = ctx();
        let policy = Arc::new(Policy::default());
        let callbacks = Callbacks::new(Some(policy.clone()));
        let mut value = to_value(query("group-key")).unwrap();
        value["message"]["chat"] = json!({"id":-42,"type":"supergroup","title":"Group"});
        let query: CallbackQuery = from_value(value).unwrap();
        let mut entries = Acl::new();
        entries.ensure(-42, Role::Trusted);
        let acl = Some(Arc::new(AclState {
            acl: RwLock::new(entries),
            path: None,
        }));
        callbacks
            .handle(
                &server.bot,
                input(query.clone()),
                &acl,
                &GroupSettings::default(),
                &ctx,
            )
            .await;
        assert!(policy.frames.lock().unwrap().is_empty());
        acl.as_ref()
            .unwrap()
            .acl
            .write()
            .unwrap()
            .set_group_mode(-42, GroupMode::All)
            .unwrap();
        callbacks
            .handle(
                &server.bot,
                input(query.clone()),
                &acl,
                &GroupSettings::default(),
                &ctx,
            )
            .await;
        assert_eq!(policy.frames.lock().unwrap().len(), 1);
        acl.as_ref()
            .unwrap()
            .acl
            .write()
            .unwrap()
            .set_group_mode(-42, GroupMode::Allowed)
            .unwrap();
        callbacks
            .handle(
                &server.bot,
                input(query),
                &acl,
                &GroupSettings::default(),
                &ctx,
            )
            .await;
        server.wait("answercallbackquery", 3).await;
        assert_eq!(policy.frames.lock().unwrap().len(), 1);
    }
    #[tokio::test]
    async fn blocked_ack_is_bounded_and_does_not_discard_committed_post_events() {
        let server = blocked_ack_server().await;
        let ctx = ctx();
        let policy = Arc::new(Policy::default());
        let callbacks = Callbacks::new(Some(policy.clone()));
        let mut events = ctx
            .subscribe(Filter::by_kind("host.control"), SubscribeOptions::default())
            .await
            .unwrap();
        let bot = server.bot.clone();
        let owned = ctx.clone();
        let task = spawn(async move {
            callbacks
                .handle(
                    &bot,
                    input(query("key")),
                    &acl(),
                    &GroupSettings::default(),
                    &owned,
                )
                .await
        });
        timeout(Duration::from_secs(1), server.ack_started.notified())
            .await
            .unwrap();
        assert_eq!(policy.frames.lock().unwrap().len(), 1);
        timeout(Duration::from_secs(1), events.next())
            .await
            .unwrap()
            .unwrap();
        timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
    #[test]
    fn decisions_are_bounded_and_cannot_synthesize_native_messages() {
        assert!(CallbackDecision::new(Some("x".repeat(201)), false, vec![]).is_err());
        assert!(
            CallbackDecision::new(
                None,
                false,
                vec![Envelope::new(
                    ConnectorId::new("host"),
                    EventKind::new("chat.message"),
                    "not native".to_string()
                )]
            )
            .is_err()
        );
    }
}
