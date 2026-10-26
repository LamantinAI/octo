use std::{
    fmt::{Debug, Formatter, Result as FmtResult},
    sync::Arc,
};

use octo_core::Envelope;
use teloxide::{Bot, RequestError, types::Message};

use crate::{
    DeliveryStatus, TelegramHostPolicy,
    policy::{
        delivery::Delivery,
        tracking::{MessageRef, Tracker},
    },
};

/// Optional transport lease; never changes the native approval authority proof.
#[derive(Clone)]
pub(super) struct CardDelivery {
    tracker: Arc<Tracker>,
    policy: Option<Arc<dyn TelegramHostPolicy>>,
    envelope: Envelope,
}
impl Debug for CardDelivery {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.write_str("CardDelivery(<scoped transport>)")
    }
}
impl CardDelivery {
    pub fn new(delivery: &Delivery<'_>) -> Self {
        Self {
            tracker: delivery.tracker.clone(),
            policy: delivery.policy.cloned(),
            envelope: delivery.envelope.clone(),
        }
    }
    fn delivery<'a>(&'a self, bot: &'a Bot) -> Delivery<'a> {
        Delivery {
            bot,
            tracker: &self.tracker,
            policy: self.policy.as_ref(),
            envelope: &self.envelope,
            trusted_notice: false,
        }
    }
    pub fn allowed(&self, bot: &Bot) -> bool {
        self.delivery(bot).allowed()
    }
    pub async fn report(&self, bot: &Bot, chat: i64, result: &Result<Message, RequestError>) {
        let messages = result
            .as_ref()
            .ok()
            .map(|message| {
                vec![MessageRef {
                    chat_id: message.chat.id.0,
                    message_id: message.id.0,
                    date: message.date.timestamp(),
                }]
            })
            .unwrap_or_default();
        self.delivery(bot)
            .report(
                chat,
                &messages,
                if result.is_ok() {
                    DeliveryStatus::Sent
                } else {
                    DeliveryStatus::Unknown
                },
            )
            .await;
    }
}
