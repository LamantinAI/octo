use std::{sync::Arc, time::Duration};

use octo_core::{ConnectorContext, ConnectorId, Envelope};
use teloxide::{
    Bot, payloads::AnswerCallbackQuerySetters, requests::Requester, types::CallbackQuery,
};
use tokio::{sync::Semaphore, time::timeout};
use tracing::warn;

use super::{CallbackDecision, CallbackFrame, CallbackMessage};
use crate::{
    Acl, AclState, ActorRoleDecision, GroupMode, TelegramHostPolicy, VerifiedActor, VerifiedBot,
    policy::preflight::actor_role,
    source::{GroupSettings, is_admin},
};

pub(crate) struct CallbackInput {
    pub connector: ConnectorId,
    pub bot_id: u64,
    pub update_id: u32,
    pub query: CallbackQuery,
}
pub(crate) struct Callbacks {
    policy: Option<Arc<dyn TelegramHostPolicy>>,
    jobs: Arc<Semaphore>,
    timeout: Duration,
    bot_identity: Option<VerifiedBot>,
}
impl Callbacks {
    pub(crate) fn new(policy: Option<Arc<dyn TelegramHostPolicy>>) -> Self {
        let limits = policy
            .as_ref()
            .map(|policy| policy.callback_limits())
            .unwrap_or_default();
        Self {
            bot_identity: None,
            policy,
            jobs: Arc::new(Semaphore::new(limits.max_concurrent.clamp(1, 64))),
            timeout: limits
                .timeout
                .clamp(Duration::from_millis(100), Duration::from_secs(10)),
        }
    }
    pub(crate) fn with_identity(mut self, bot: VerifiedBot) -> Self {
        self.bot_identity = Some(bot);
        self
    }
    pub(crate) async fn handle(
        &self,
        bot: &Bot,
        input: CallbackInput,
        acl: &Option<Arc<AclState>>,
        groups: &GroupSettings,
        ctx: &ConnectorContext,
    ) {
        let query = &input.query;
        // The caller routes this namespace exclusively to the approval handler.
        if query
            .data
            .as_deref()
            .is_some_and(|data| data.starts_with("oa:"))
        {
            return;
        }
        let native = frame(&input);
        let host_role = if let Some(message) = &native.message {
            actor_role(
                self.policy.as_ref(),
                VerifiedActor {
                    bot: self.bot_identity.clone().unwrap_or_else(|| VerifiedBot {
                        connector: input.connector.clone(),
                        bot_id: input.bot_id,
                        username: String::new(),
                    }),
                    actor_id: query.from.id.0 as i64,
                    actor_is_bot: query.from.is_bot,
                    chat_id: message.chat_id,
                    chat_type: message.chat_type.clone(),
                },
            )
            .await
        } else {
            Ok(ActorRoleDecision::Role(None))
        };
        let allowed = match host_role {
            Ok(ActorRoleDecision::Role(role)) => {
                role.is_some()
                    && !query.from.is_bot
                    && native.message.as_ref().is_some_and(|message| {
                        message.accessible
                            && (message.chat_type != "private"
                                || message.chat_id == query.from.id.0 as i64)
                    })
            }
            Err(_) => false,
            Ok(ActorRoleDecision::ContinueLegacy) => {
                let acl = acl.as_ref().map(|state| state.acl.read().unwrap());
                allowed(acl.as_deref(), groups, query)
            }
        };
        let valid = query
            .data
            .as_ref()
            .is_some_and(|data| !data.is_empty() && data.len() <= 64);
        if !allowed || !valid || self.policy.is_none() {
            answer(bot, query, CallbackDecision::unavailable(), self.timeout).await;
            return;
        }
        let Ok(permit) = self.jobs.clone().try_acquire_owned() else {
            let decision = CallbackDecision::new(
                Some("Controls are busy. Please try again.".into()),
                false,
                vec![],
            )
            .expect("bounded static response");
            answer(bot, query, decision, self.timeout).await;
            return;
        };
        let policy = self.policy.as_ref().expect("policy checked").clone();
        let frame = frame(&input);
        let _permit = permit;
        // Await state admission in the polling loop: the next native input or
        // callback must not race ahead of a committed host navigation.
        let decision = match timeout(self.timeout, policy.callback(frame)).await {
            Ok(Ok(decision)) => decision,
            _ => CallbackDecision::unavailable(),
        };
        let events = answer(bot, &input.query, decision, self.timeout).await;
        for event in events {
            if policy.allow_publish(&event) && ctx.publish(event).await.is_err() {
                warn!("telegram: callback post-event could not be published");
            }
        }
    }
}
fn allowed(acl: Option<&Acl>, groups: &GroupSettings, query: &CallbackQuery) -> bool {
    if query.from.is_bot {
        return false;
    }
    let Some(message) = &query.message else {
        return false;
    };
    let chat = message.chat();
    let Ok(actor) = i64::try_from(query.from.id.0) else {
        return false;
    };
    if chat.is_private() && actor != chat.id.0 {
        return false;
    }
    let Some(acl) = acl else {
        return true;
    };
    let admin = is_admin(acl, groups, actor);
    if chat.is_private() {
        return acl.role(chat.id.0).is_some() || admin;
    }
    (chat.is_group() || chat.is_supergroup())
        && acl.role(chat.id.0).is_some()
        && (acl.role(actor).is_some() || admin || acl.group_mode(chat.id.0) == GroupMode::All)
}
fn frame(input: &CallbackInput) -> CallbackFrame {
    let query = &input.query;
    let message = query.message.as_ref().map(|message| {
        let chat = message.chat();
        CallbackMessage {
            chat_id: chat.id.0,
            message_id: message.id().0,
            chat_type: if chat.is_private() {
                "private"
            } else if chat.is_group() {
                "group"
            } else if chat.is_supergroup() {
                "supergroup"
            } else {
                "channel"
            }
            .into(),
            accessible: message.regular_message().is_some(),
            sender_id: message
                .regular_message()
                .and_then(|message| message.from.as_ref())
                .map(|user| user.id.0),
        }
    });
    CallbackFrame {
        connector: input.connector.clone(),
        bot_id: input.bot_id,
        update_id: input.update_id,
        query_id: query.id.0.clone(),
        actor_id: query.from.id.0,
        message,
        inline_message_id: query.inline_message_id.clone(),
        data: query.data.clone().unwrap_or_default(),
    }
}
async fn answer(
    bot: &Bot,
    query: &CallbackQuery,
    decision: CallbackDecision,
    wait: Duration,
) -> Vec<Envelope> {
    let mut request = bot
        .answer_callback_query(query.id.clone())
        .show_alert(decision.show_alert);
    if let Some(text) = decision.text {
        request = request.text(text);
    }
    if !matches!(timeout(wait, request).await, Ok(Ok(_))) {
        warn!("telegram: callback acknowledgement failed");
    }
    decision.post_events
}
