//! Friendly native approval presentation, separate from backend authorization.
mod registry;
mod render;
mod request;
#[cfg(test)]
mod testing;
mod transport;

use std::sync::{Arc, Mutex};

use tokio::sync::Semaphore;

pub(crate) use registry::CardSettings;
use registry::Registry;

pub(crate) struct Cards {
    registry: Mutex<Registry>,
    jobs: Arc<Semaphore>,
}
impl Cards {
    pub fn new(settings: CardSettings) -> Arc<Self> {
        Arc::new(Self {
            jobs: Arc::new(Semaphore::new(settings.max_cards)),
            registry: Mutex::new(Registry::new(settings)),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, RwLock},
        time::Duration,
    };

    use chrono::Utc;
    use octo_approval::{ApprovalAction, ApprovalPresentation, Authority};
    use octo_core::{
        ChannelId, ConnectorContext, ConnectorId, Envelope, EventBus, EventKind, Filter,
        InProcessBus, SubscribeOptions, control::CancellationToken,
    };
    use serde_json::{Value, from_str, from_value, json, to_value};
    use teloxide::types::{CallbackQuery, MessageId};
    use tokio::time::timeout;

    use super::testing::server;
    use super::{
        CardSettings, Cards,
        registry::{Action, Card, Registry, State},
        render::{details, keyboard, text},
        request::{current_owner, request, response},
    };
    use crate::{Acl, AclState, Role};

    fn presentation() -> ApprovalPresentation {
        ApprovalPresentation {
            executor: "sql".into(),
            operation_id: "operation-1".into(),
            operation_hash: "exact-hash".into(),
            recipient_principal: "telegram:user:7".into(),
            private_destination: "7".into(),
            expires_at: Utc::now().timestamp() + 60,
            action: ApprovalAction::DeleteStore,
            resource: "Nutrition".into(),
            objects: vec!["Meals".into()],
            decision_kind: "sql.approval.decide".into(),
            status_kind: "sql.operation.status".into(),
        }
    }
    fn envelope() -> Envelope {
        Envelope::new(
            ConnectorId::new("sql"),
            EventKind::new("chat.reply"),
            "RAW SQL /data/path hash".to_owned(),
        )
        .with_target(ConnectorId::new("telegram"))
        .with_channel(ChannelId::new("7"))
        .with_context(presentation())
    }
    fn acl(role: Role) -> Option<Arc<AclState>> {
        let mut acl = Acl::new();
        acl.insert(7, role);
        Some(Arc::new(AclState {
            acl: RwLock::new(acl),
            path: None,
        }))
    }
    fn callback(token: &str, action: Action) -> CallbackQuery {
        from_value(json!({"id":"query-1","from":{"id":7,"is_bot":false,"first_name":"Person","language_code":"ru"},"message":{"message_id":17,"date":1,"chat":{"id":7,"type":"private","first_name":"Person"},"from":{"id":900,"is_bot":true,"first_name":"BOT"},"text":"card"},"chat_instance":"private-chat","data":action.data(token)})).unwrap()
    }
    fn active() -> (Registry, Card) {
        let mut registry = Registry::new(CardSettings::default());
        let card = registry
            .reserve(presentation(), Utc::now().timestamp())
            .unwrap();
        registry.activate(&card.token, MessageId(17));
        let card = registry.cards[&card.token].clone();
        (registry, card)
    }
    #[test]
    fn only_native_context_and_exact_executor_recipient_routing_create_cards() {
        let id = ConnectorId::new("telegram");
        let valid = envelope();
        assert!(Registry::presentation(&valid, &id).is_some());
        let spoof = Envelope::new(
            ConnectorId::new("sql"),
            EventKind::new("chat.reply"),
            json!({"action":"delete_store","approved":true,"operation_id":"operation-1"}),
        )
        .with_target(id.clone())
        .with_channel(ChannelId::new("7"));
        assert!(Registry::presentation(&spoof, &id).is_none());
        let mut wrong = valid.clone();
        wrong.source = ConnectorId::new("model");
        assert!(Registry::presentation(&wrong, &id).is_none());
        wrong = valid.clone();
        wrong.channel = Some(ChannelId::new("-42"));
        assert!(Registry::presentation(&wrong, &id).is_none());
        wrong = valid;
        wrong.target = Some(ConnectorId::new("other"));
        assert!(Registry::presentation(&wrong, &id).is_none());
    }
    #[test]
    fn callbacks_bind_actual_clicker_private_chat_message_token_and_current_acl() {
        let (mut registry, card) = active();
        let query = callback(&card.token, Action::Confirm);
        assert!(current_owner(&acl(Role::Owner), query.from.id.0, 7));
        assert!(!current_owner(&acl(Role::Guest), query.from.id.0, 7));
        let now = Utc::now().timestamp();
        for value in [
            {
                let mut value = to_value(&query).unwrap();
                value["from"]["id"] = json!(8);
                value
            },
            {
                let mut value = to_value(&query).unwrap();
                value["message"]["chat"]["id"] = json!(8);
                value
            },
            {
                let mut value = to_value(&query).unwrap();
                value["message"]["message_id"] = json!(18);
                value
            },
            {
                let mut value = to_value(&query).unwrap();
                value["message"]["chat"] = json!({"id":7,"type":"group","title":"group"});
                value
            },
            {
                let mut value = to_value(&query).unwrap();
                value["message"]["date"] = json!(0);
                value
            },
            {
                let mut value = to_value(&query).unwrap();
                value["inline_message_id"] = json!("inline");
                value
            },
            {
                let mut value = to_value(&query).unwrap();
                value["data"] = json!("oa:00000000000000000000000000000000:y");
                value
            },
            {
                let mut value = to_value(&query).unwrap();
                value["data"] = json!(format!("oa:{}:unknown", card.token));
                value
            },
        ] {
            assert!(
                registry
                    .begin(&from_value(value).unwrap(), true, now)
                    .is_err()
            );
        }
        assert!(registry.begin(&query, false, now).is_err());
        // message.from is the bot; only callback.from identifies the human.
        assert_eq!(
            registry.begin(&query, true, now).unwrap().1,
            Action::Confirm
        );
    }
    #[test]
    fn expired_decisions_are_rejected_and_unknown_or_terminal_never_replay() {
        let (mut registry, card) = active();
        let now = Utc::now().timestamp();
        registry
            .begin(&callback(&card.token, Action::Confirm), true, now)
            .unwrap();
        assert_eq!(
            registry
                .begin(&callback(&card.token, Action::Confirm), true, now)
                .unwrap()
                .1,
            Action::Status
        );
        registry.finish(&card.token, State::Unknown);
        assert_eq!(
            registry.finish(&card.token, State::Pending).unwrap().state,
            State::Unknown
        );
        assert_eq!(
            keyboard(&registry.cards[&card.token], now)
                .inline_keyboard
                .len(),
            1
        );
        registry.finish(&card.token, State::Committed);
        assert_eq!(
            registry.finish(&card.token, State::Unknown).unwrap().state,
            State::Committed
        );
        let (mut registry, card) = active();
        assert!(
            registry
                .begin(
                    &callback(&card.token, Action::Confirm),
                    true,
                    card.presentation.expires_at
                )
                .is_err()
        );
        assert_eq!(
            registry
                .begin(
                    &callback(&card.token, Action::Details),
                    true,
                    card.presentation.expires_at + 1
                )
                .unwrap()
                .1,
            Action::Details
        );
    }
    #[test]
    fn details_and_status_use_read_invocations_while_decisions_require_native_human_proof() {
        let (_, card) = active();
        let authority = Authority::new(Duration::from_secs(60), 32);
        let id = ConnectorId::new("telegram");
        let acl = acl(Role::Owner);
        for action in [Action::Details, Action::Status] {
            let request = request(
                &authority,
                &id,
                &acl,
                &callback(&card.token, action),
                &card,
                action,
            )
            .unwrap();
            assert!(authority.consume_decision(&request).is_err());
            assert_eq!(
                authority.consume(&request).unwrap().origin.principal,
                "telegram:user:7"
            );
            if action == Action::Details {
                assert_eq!(
                    request.payload_as::<Value>().unwrap()["include_details"],
                    true
                );
            }
        }
        let decision = request(
            &authority,
            &id,
            &acl,
            &callback(&card.token, Action::Confirm),
            &card,
            Action::Confirm,
        )
        .unwrap();
        assert!(authority.consume(&decision).is_err());
        assert!(authority.consume_decision(&decision).is_ok());
        acl.as_ref()
            .unwrap()
            .acl
            .write()
            .unwrap()
            .insert(7, Role::Trusted);
        assert!(
            request(
                &authority,
                &id,
                &acl,
                &callback(&card.token, Action::Confirm),
                &card,
                Action::Confirm
            )
            .is_err()
        );
    }
    #[test]
    fn rendering_is_english_bounded_and_keeps_technical_data_in_details() {
        let (_, mut card) = active();
        let rendered = text(&card);
        assert!(rendered.contains("Delete store “Nutrition”?"));
        assert!(!rendered.contains("RAW SQL"));
        assert!(!rendered.contains("hash"));
        let buttons = keyboard(&card, Utc::now().timestamp());
        assert_eq!(buttons.inline_keyboard[0][0].text, "Delete");
        let wire = to_value(buttons).unwrap();
        for row in wire["inline_keyboard"].as_array().unwrap() {
            for button in row.as_array().unwrap() {
                assert!(button["callback_data"].as_str().unwrap().len() <= 64);
            }
        }
        card.presentation.resource = "Питание\n\u{202e}".repeat(100);
        assert!(text(&card).len() < 1000);
        assert!(!text(&card).contains('\u{202e}'));
        assert!(
            details(
                &json!({"sql":"DELETE FROM x","database":"/private/path","hash":"abc"}),
                1000
            )
            .unwrap()
            .contains("/private/path")
        );
        assert!(details(&json!({"sql":"a".repeat(1000)}), 100).is_err());
    }
    #[test]
    fn status_checks_response_binding_and_cache_capacity() {
        let (_, card) = active();
        let reply = |body| {
            Envelope::new(
                ConnectorId::new("sql"),
                EventKind::new("sql.operation.status.result"),
                body,
            )
        };
        assert_eq!(
            response(
                &card,
                &reply(json!({"status":"committed","operation_id":"operation-1"}))
            )
            .unwrap()
            .0,
            State::Committed
        );
        assert!(
            response(
                &card,
                &reply(json!({"status":"committed","operation_hash":"changed"}))
            )
            .is_err()
        );
        assert!(
            response(
                &card,
                &reply(json!({"status":"committed","operation_id":"other"}))
            )
            .is_err()
        );
        assert_eq!(
            response(
                &card,
                &reply(json!({"status":"rolled_back","error":"constraint"}))
            )
            .unwrap()
            .0,
            State::RolledBack
        );
        let mut registry = Registry::new(CardSettings {
            max_cards: 1,
            ..CardSettings::default()
        });
        let now = Utc::now().timestamp();
        registry.reserve(presentation(), now).unwrap();
        let mut next = presentation();
        next.operation_id = "second".into();
        assert!(registry.reserve(next.clone(), now).is_err());
        next.expires_at = now + 120;
        assert!(registry.reserve(next, now + 61).is_ok());
        assert!(
            CardSettings {
                max_cards: 0,
                ..CardSettings::default()
            }
            .validate()
            .is_err()
        );
    }

    #[tokio::test]
    async fn mock_bot_gets_native_card_and_details_use_read_proof() {
        let server = server().await;
        let cards = Cards::new(CardSettings::default());
        let id = ConnectorId::new("telegram");
        let acl = acl(Role::Owner);
        let spoof = Envelope::new(
            ConnectorId::new("sql"),
            EventKind::new("chat.reply"),
            json!({"operation_id":"operation-1","approved":true}),
        );
        assert!(!cards.present(&server.bot, &id, &acl, &spoof).await);
        assert!(server.requests.lock().unwrap().is_empty());
        assert!(cards.present(&server.bot, &id, &acl, &envelope()).await);
        let main = server.wait("sendMessage", 1).await;
        assert!(main[0].contains("Delete store"));
        assert!(!main[0].contains("RAW SQL"));
        assert!(!main[0].contains("exact-hash"));
        let token = cards
            .registry
            .lock()
            .unwrap()
            .cards
            .keys()
            .next()
            .unwrap()
            .clone();
        let authority = Authority::new(Duration::from_secs(60), 16);
        let bus = Arc::new(InProcessBus::new(16));
        let ctx = ConnectorContext::new(CancellationToken::new(), bus.clone());
        let mut requests = bus
            .subscribe(
                Filter::by_target(ConnectorId::new("sql")),
                SubscribeOptions::default(),
            )
            .await
            .unwrap();
        cards
            .callback(
                &server.bot,
                &id,
                &acl,
                &Some(authority.clone()),
                callback(&token, Action::Details),
                &ctx,
                Duration::from_secs(1),
            )
            .await;
        let request = timeout(Duration::from_secs(1), requests.next())
            .await
            .unwrap()
            .unwrap();
        assert!(authority.consume_decision(&request).is_err());
        assert!(authority.consume(&request).is_ok());
        assert_eq!(
            request.payload_as::<Value>().unwrap()["include_details"],
            true
        );
        bus.publish(Envelope::new(ConnectorId::new("sql"),EventKind::new("sql.operation.status.result"),json!({"status":"approval_required","operation_id":"operation-1","operation_hash":"exact-hash","details":{"sql":"DELETE FROM private_table"},"database":"/private/path"})).with_correlation(request.id)).await.unwrap();
        let messages = server.wait("sendMessage", 2).await;
        assert!(messages[1].contains("DELETE FROM private_table"));
        assert_eq!(
            cards.registry.lock().unwrap().cards[&token].state,
            State::Pending
        );
    }
    #[tokio::test]
    async fn timeout_and_duplicate_clicks_only_check_status_without_new_claim() {
        let server = server().await;
        let cards = Cards::new(CardSettings::default());
        let id = ConnectorId::new("telegram");
        let acl = acl(Role::Owner);
        cards.present(&server.bot, &id, &acl, &envelope()).await;
        let token = cards
            .registry
            .lock()
            .unwrap()
            .cards
            .keys()
            .next()
            .unwrap()
            .clone();
        let authority = Authority::new(Duration::from_secs(60), 16);
        let bus = Arc::new(InProcessBus::new(16));
        let ctx = ConnectorContext::new(CancellationToken::new(), bus.clone());
        let mut requests = bus
            .subscribe(
                Filter::by_target(ConnectorId::new("sql")),
                SubscribeOptions::default(),
            )
            .await
            .unwrap();
        cards
            .callback(
                &server.bot,
                &id,
                &acl,
                &Some(authority.clone()),
                callback(&token, Action::Confirm),
                &ctx,
                Duration::from_millis(25),
            )
            .await;
        let first = timeout(Duration::from_secs(1), requests.next())
            .await
            .unwrap()
            .unwrap();
        assert!(authority.consume_decision(&first).is_ok());
        server.wait("editMessageText", 2).await;
        assert_eq!(
            cards.registry.lock().unwrap().cards[&token].state,
            State::Unknown
        );
        cards
            .callback(
                &server.bot,
                &id,
                &acl,
                &Some(authority.clone()),
                callback(&token, Action::Confirm),
                &ctx,
                Duration::from_secs(1),
            )
            .await;
        let check = timeout(Duration::from_secs(1), requests.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(check.kind.as_str(), "sql.operation.status");
        assert!(authority.consume_decision(&check).is_err());
        assert!(authority.consume(&check).is_ok());
        bus.publish(Envelope::new(ConnectorId::new("sql"),EventKind::new("sql.operation.status.result"),json!({"status":"approval_required","operation_id":"operation-1","operation_hash":"exact-hash"})).with_correlation(check.id)).await.unwrap();
        let edited = server.wait("editMessageText", 3).await;
        let body: Value = from_str(&edited[2]).unwrap();
        assert_eq!(
            body["reply_markup"]["inline_keyboard"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            cards.registry.lock().unwrap().cards[&token].state,
            State::Unknown
        );
    }
}
