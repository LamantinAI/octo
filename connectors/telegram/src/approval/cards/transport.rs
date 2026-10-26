use std::{sync::Arc, time::Duration};

use chrono::Utc;
use octo_approval::{ApprovalPresentation, Authority};
use octo_core::{ConnectorContext, ConnectorId, Envelope};
use teloxide::{
    Bot,
    payloads::{AnswerCallbackQuerySetters, EditMessageTextSetters, SendMessageSetters},
    requests::Requester,
    types::{CallbackQuery, ChatId, InputFile},
};
use tracing::warn;

use super::{
    Cards,
    delivery::CardDelivery,
    registry::{Action, Card, Registry, Reject, State},
    render::{BUSY, EXPIRED, FORBIDDEN, UNAVAILABLE, details, keyboard, text},
    request::{current_owner, request, response},
};
use crate::{AclState, policy::delivery::Delivery};

impl Cards {
    /// A native context is mandatory. Invalid trusted routing is suppressed,
    /// rather than falling through to a raw technical String on the main card.
    #[cfg(test)]
    pub async fn present(
        &self,
        bot: &Bot,
        id: &ConnectorId,
        acl: &Option<Arc<AclState>>,
        envelope: &Envelope,
    ) -> bool {
        self.present_scoped(bot, id, acl, envelope, None).await
    }
    pub async fn present_scoped(
        &self,
        bot: &Bot,
        id: &ConnectorId,
        acl: &Option<Arc<AclState>>,
        envelope: &Envelope,
        delivery: Option<&Delivery<'_>>,
    ) -> bool {
        if envelope.context.get::<ApprovalPresentation>().is_none() {
            return false;
        }
        let Some(presentation) = Registry::presentation(envelope, id) else {
            return true;
        };
        let Ok(chat) = presentation.private_destination.parse::<i64>() else {
            return true;
        };
        if !current_owner(acl, chat as u64, chat) {
            return true;
        }
        let mut card = match self
            .registry
            .lock()
            .unwrap()
            .reserve(presentation, Utc::now().timestamp())
        {
            Ok(card) => card,
            Err(_) => return true,
        };
        card.delivery = delivery.map(|delivery| Arc::new(CardDelivery::new(delivery)));
        if let Some(stored) = self.registry.lock().unwrap().cards.get_mut(&card.token) {
            stored.delivery = card.delivery.clone();
        }
        if card.delivery.as_ref().is_some_and(|d| !d.allowed(bot)) {
            self.registry.lock().unwrap().cards.remove(&card.token);
            return true;
        }
        let result = bot
            .send_message(ChatId(card.chat), text(&card))
            .reply_markup(keyboard(&card, Utc::now().timestamp()))
            .await;
        if let Some(delivery) = &card.delivery {
            delivery.report(bot, card.chat, &result).await;
        }
        match result {
            Ok(message) if message.chat.is_private() && message.chat.id.0 == card.chat => self
                .registry
                .lock()
                .unwrap()
                .activate(&card.token, message.id),
            _ => {
                self.registry.lock().unwrap().cards.remove(&card.token);
                warn!("Telegram approval card delivery failed");
            }
        }
        true
    }
    pub async fn callback(
        self: &Arc<Self>,
        bot: &Bot,
        id: &ConnectorId,
        acl: &Option<Arc<AclState>>,
        authority: &Option<Authority>,
        query: CallbackQuery,
        ctx: &ConnectorContext,
        timeout: Duration,
    ) -> bool {
        if !query
            .data
            .as_deref()
            .is_some_and(|data| data.starts_with("oa:"))
        {
            return false;
        }
        let Some(authority) = authority.clone() else {
            let _ = bot.answer_callback_query(query.id).text(FORBIDDEN).await;
            return true;
        };
        let Ok(permit) = self.jobs.clone().try_acquire_owned() else {
            let _ = bot.answer_callback_query(query.id).text(BUSY).await;
            return true;
        };
        let owner = query
            .regular_message()
            .is_some_and(|message| current_owner(acl, query.from.id.0, message.chat.id.0));
        let begun = self
            .registry
            .lock()
            .unwrap()
            .begin(&query, owner, Utc::now().timestamp());
        let (card, action) = match begun {
            Ok(begun) => begun,
            Err(Reject::Expired(card)) => {
                let _ = bot.answer_callback_query(query.id).text(EXPIRED).await;
                edit(bot, &card).await;
                return true;
            }
            Err(Reject::Forbidden) => {
                let _ = bot.answer_callback_query(query.id).text(FORBIDDEN).await;
                return true;
            }
            Err(_) => {
                let _ = bot.answer_callback_query(query.id).text(UNAVAILABLE).await;
                return true;
            }
        };
        let _ = bot.answer_callback_query(query.id.clone()).await;
        let cards = self.clone();
        let bot = bot.clone();
        let id = id.clone();
        let acl = acl.clone();
        let ctx = ctx.clone();
        tokio::spawn(async move {
            let _permit = permit;
            cards
                .process(
                    &bot, &id, &acl, &authority, &query, &card, action, &ctx, timeout,
                )
                .await;
        });
        true
    }
    async fn process(
        &self,
        bot: &Bot,
        id: &ConnectorId,
        acl: &Option<Arc<AclState>>,
        authority: &Authority,
        query: &CallbackQuery,
        card: &Card,
        action: Action,
        ctx: &ConnectorContext,
        timeout: Duration,
    ) {
        if action.is_decision() {
            edit(bot, card).await;
        }
        let envelope = match request(authority, id, acl, query, card, action) {
            Ok(request) => request,
            Err(_) => {
                if action.is_decision() {
                    let updated = self
                        .registry
                        .lock()
                        .unwrap()
                        .finish(&card.token, State::Unknown);
                    if let Some(updated) = updated {
                        edit(bot, &updated).await;
                    }
                }
                return;
            }
        };
        let outcome = ctx
            .publish_and_await_response(envelope, timeout)
            .await
            .ok()
            .and_then(|reply| response(card, &reply).ok());
        let Some((state, body)) = outcome else {
            let next = if action.is_decision() {
                State::Unknown
            } else {
                card.state
            };
            let updated = self.registry.lock().unwrap().finish(&card.token, next);
            if let Some(updated) = updated {
                edit(bot, &updated).await;
            }
            return;
        };
        let updated = self.registry.lock().unwrap().finish(&card.token, state);
        if let Some(updated) = updated {
            edit(bot, &updated).await;
        }
        if action == Action::Details {
            if card.delivery.as_ref().is_some_and(|d| !d.allowed(bot)) {
                return;
            }
            let limit = self.registry.lock().unwrap().settings.max_detail_bytes;
            let result = match details(&body, limit) {
                Ok(text) if text.len() <= 3000 => bot.send_message(ChatId(card.chat), text).await,
                Ok(text) => {
                    let file =
                        InputFile::memory(text.into_bytes()).file_name("operation-details.txt");
                    bot.send_document(ChatId(card.chat), file).await
                }
                Err(message) => bot.send_message(ChatId(card.chat), message).await,
            };
            if let Some(delivery) = &card.delivery {
                delivery.report(bot, card.chat, &result).await;
            }
        }
    }
}
async fn edit(bot: &Bot, card: &Card) {
    if card.delivery.as_ref().is_some_and(|d| !d.allowed(bot)) {
        return;
    }
    if let Some(message) = card.message {
        let result = bot
            .edit_message_text(ChatId(card.chat), message, text(card))
            .reply_markup(keyboard(card, Utc::now().timestamp()))
            .await;
        if let Some(delivery) = &card.delivery {
            delivery.report(bot, card.chat, &result).await;
        }
    }
}
