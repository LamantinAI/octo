use std::sync::Arc;

use chrono::Utc;
use octo_core::{ChannelId, ConnectorId, Envelope, EventKind};
use teloxide::{Bot, requests::Requester, types::ChatId};

use super::tracking::{MessageRef, Tracker};
use super::{
    DeliveryReport, DeliveryStatus, IngressScope, TelegramHostPolicy, TelegramOutputScope,
};

pub(crate) struct Delivery<'a> {
    pub bot: &'a Bot,
    pub tracker: &'a Arc<Tracker>,
    pub policy: Option<&'a Arc<dyn TelegramHostPolicy>>,
    pub envelope: &'a Envelope,
    pub trusted_notice: bool,
}
impl Delivery<'_> {
    pub(crate) fn scope(&self) -> Option<TelegramOutputScope> {
        if let Some(policy) = self.policy {
            return policy.output_scope(self.envelope);
        }
        self.envelope
            .context
            .get::<TelegramOutputScope>()
            .cloned()
            .or_else(|| {
                self.envelope
                    .context
                    .get::<IngressScope>()
                    .cloned()
                    .map(|scope| TelegramOutputScope {
                        scope,
                        attempt_id: None,
                    })
            })
    }
    pub(crate) fn allowed(&self) -> bool {
        self.policy.is_none_or(|p| p.allow_delivery(self.envelope))
            && self
                .scope()
                .is_none_or(|s| !self.tracker.is_closed(&s.scope))
    }
    pub(crate) async fn report(
        &self,
        chat_id: i64,
        messages: &[MessageRef],
        status: DeliveryStatus,
    ) {
        let status = if messages.iter().any(|m| m.message_id <= 0) {
            DeliveryStatus::Unknown
        } else {
            status
        };
        let scope = self.scope();
        if let Some(scope) = &scope {
            if !self.trusted_notice
                && self
                    .policy
                    .is_some_and(|p| !p.allow_delivery(self.envelope))
            {
                if let Some(request) = Tracker::request(&scope.scope) {
                    self.tracker.close(&request);
                }
            }
            let unknown = [MessageRef {
                chat_id,
                message_id: 0,
                date: Utc::now().timestamp(),
            }];
            let tracked = if messages.is_empty() && status == DeliveryStatus::Unknown {
                &unknown[..]
            } else {
                messages
            };
            if self.tracker.record(&scope.scope, tracked).unwrap_or(false) {
                if let Some(request) = Tracker::request(&scope.scope) {
                    let report = self.tracker.cleanup(self.bot, &request).await;
                    if let Some(policy) = self.policy {
                        policy.cleanup_finished(&report);
                    }
                }
            }
        }
        if let Some(policy) = self.policy {
            policy.delivered(
                self.envelope,
                &DeliveryReport {
                    scope,
                    chat_id,
                    message_ids: messages
                        .iter()
                        .filter(|m| m.message_id > 0)
                        .map(|m| m.message_id)
                        .collect(),
                    status,
                },
            );
        }
    }
}

/// Policy-approved navigation/unsupported-input notices are connector IO, never model turns.
pub(crate) async fn notice(
    bot: &Bot,
    tracker: &Arc<Tracker>,
    policy: Option<&Arc<dyn TelegramHostPolicy>>,
    connector: &ConnectorId,
    chat: i64,
    text: String,
    scope: Option<IngressScope>,
) {
    let mut env = Envelope::new(
        connector.clone(),
        EventKind::new("chat.reply"),
        text.clone(),
    )
    .with_channel(ChannelId::new(chat.to_string()));
    if let Some(scope) = scope {
        env = env.with_context(TelegramOutputScope {
            scope,
            attempt_id: None,
        });
    }
    let delivery = Delivery {
        bot,
        tracker,
        policy,
        envelope: &env,
        trusted_notice: true,
    };
    match bot.send_message(ChatId(chat), text).await {
        Ok(message) => {
            delivery
                .report(
                    chat,
                    &[MessageRef {
                        chat_id: message.chat.id.0,
                        message_id: message.id.0,
                        date: message.date.timestamp(),
                    }],
                    DeliveryStatus::Sent,
                )
                .await
        }
        Err(_) => delivery.report(chat, &[], DeliveryStatus::Unknown).await,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use octo_core::{ConnectorId, Envelope, EventKind};
    use teloxide::Bot;

    use super::Delivery;
    use crate::{
        IngressDecision, IngressScope, NativeFrame, TelegramHostPolicy, TelegramOutputScope,
        policy::tracking::Tracker,
    };

    struct Promoted;
    #[async_trait]
    impl TelegramHostPolicy for Promoted {
        async fn admit(&self, _: NativeFrame, _: Envelope) -> Result<IngressDecision, String> {
            Ok(IngressDecision::Ignore)
        }
        fn output_scope(&self, envelope: &Envelope) -> Option<TelegramOutputScope> {
            let mut scope = envelope.context.get::<TelegramOutputScope>()?.clone();
            scope.scope.ephemeral = false;
            scope.scope.cleanup_scope = None;
            Some(scope)
        }
    }

    #[test]
    fn saved_end_can_release_cleanup_without_muting_pinned_permanent_work() {
        let tracker = Tracker::new(Default::default());
        let bot = Bot::new("123:FAKE");
        let scope = IngressScope {
            key: "session".into(),
            epoch: "epoch".into(),
            ephemeral: true,
            cleanup_scope: Some("route".into()),
        };
        tracker.close(&Tracker::request(&scope).unwrap());
        let env = Envelope::new(ConnectorId::new("host"), EventKind::new("chat.reply"), true)
            .with_context(TelegramOutputScope {
                scope,
                attempt_id: Some("old-attempt".into()),
            });
        let policy: Arc<dyn TelegramHostPolicy> = Arc::new(Promoted);
        let mut delivery = Delivery {
            bot: &bot,
            tracker: &tracker,
            policy: None,
            envelope: &env,
            trusted_notice: false,
        };
        assert!(!delivery.allowed());
        delivery.policy = Some(&policy);
        assert!(delivery.allowed());
        let effective = delivery.scope().unwrap();
        assert_eq!(effective.attempt_id.as_deref(), Some("old-attempt"));
        assert!(!effective.scope.ephemeral);
        assert!(Tracker::request(&effective.scope).is_none());
    }
}
