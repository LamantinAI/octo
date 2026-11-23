//! Bounded host-authored plain text controls. Never reconstructed from JSON tags.
use std::{
    error::Error,
    fmt::{Display, Formatter, Result as FmtResult},
};

use teloxide::{
    Bot,
    payloads::SendMessageSetters,
    requests::Requester,
    types::{ChatId, InlineKeyboardButton, InlineKeyboardMarkup},
};
use tracing::warn;

use crate::{
    DeliveryStatus,
    policy::{delivery::Delivery, tracking::MessageRef},
};

#[derive(Clone, Debug)]
pub struct InlineButton {
    label: String,
    data: String,
}
#[derive(Clone, Debug)]
pub struct InlineKeyboard {
    rows: Vec<Vec<InlineButton>>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InlineKeyboardError(&'static str);
impl Display for InlineKeyboardError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.write_str(self.0)
    }
}
impl Error for InlineKeyboardError {}
impl InlineButton {
    pub fn label(&self) -> &str {
        &self.label
    }
    pub fn data(&self) -> &str {
        &self.data
    }
    pub fn new(
        label: impl Into<String>,
        data: impl Into<String>,
    ) -> Result<Self, InlineKeyboardError> {
        let label = label.into();
        let data = data.into();
        if label.trim().is_empty()
            || label.chars().count() > 64
            || label.chars().any(char::is_control)
        {
            return Err(InlineKeyboardError(
                "button label must be a nonempty single line of at most 64 characters",
            ));
        }
        if data.is_empty()
            || data.len() > 64
            || data.starts_with("oa:")
            || data.chars().any(char::is_control)
        {
            return Err(InlineKeyboardError(
                "callback data must be 1..64 bytes outside reserved oa: namespace",
            ));
        }
        Ok(Self { label, data })
    }
}
impl InlineKeyboard {
    pub fn rows(&self) -> &[Vec<InlineButton>] {
        &self.rows
    }
    pub fn new(rows: Vec<Vec<InlineButton>>) -> Result<Self, InlineKeyboardError> {
        if rows.is_empty()
            || rows.len() > 20
            || rows.iter().any(|row| row.is_empty() || row.len() > 8)
            || rows.iter().map(Vec::len).sum::<usize>() > 64
        {
            return Err(InlineKeyboardError(
                "inline keyboard exceeds bounded row/button limits",
            ));
        }
        Ok(Self { rows })
    }
    fn markup(&self) -> InlineKeyboardMarkup {
        InlineKeyboardMarkup::new(self.rows.iter().map(|row| {
            row.iter()
                .map(|button| InlineKeyboardButton::callback(&button.label, &button.data))
        }))
    }
}
/// One plain Telegram message is the complete control surface. Never split it
/// or silently discard markup; oversized/mistyped controls fail before any IO.
pub(crate) async fn send_inline(
    bot: &Bot,
    chat: ChatId,
    text: Option<&str>,
    keyboard: &InlineKeyboard,
    delivery: &Delivery<'_>,
) {
    let Some(text) = text.filter(|text| !text.is_empty() && text.encode_utf16().count() <= 4096)
    else {
        warn!("telegram: invalid or oversized inline-control message");
        delivery.report(chat.0, &[], DeliveryStatus::Failed).await;
        return;
    };
    if delivery.envelope.kind.as_str() != "chat.reply" {
        warn!("telegram: inline controls require chat.reply");
        delivery.report(chat.0, &[], DeliveryStatus::Failed).await;
        return;
    }
    if !delivery.allowed() {
        return;
    }
    match bot
        .send_message(chat, text)
        .reply_markup(keyboard.markup())
        .await
    {
        Ok(sent) => {
            delivery
                .report(
                    chat.0,
                    &[MessageRef {
                        chat_id: sent.chat.id.0,
                        message_id: sent.id.0,
                        date: sent.date.timestamp(),
                    }],
                    DeliveryStatus::Sent,
                )
                .await
        }
        Err(_) => {
            delivery.report(chat.0, &[], DeliveryStatus::Unknown).await;
            warn!("telegram: inline-control submission failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use octo_core::{ConnectorId, Envelope, EventKind};

    use super::{InlineButton, InlineKeyboard, send_inline};
    use crate::{
        DeliveryReport, DeliveryStatus, IngressDecision, NativeFrame, TelegramHostPolicy,
        callbacks::testing::server,
        policy::{delivery::Delivery, tracking::Tracker},
    };
    use teloxide::types::ChatId;

    #[derive(Default)]
    struct Policy(Mutex<Vec<DeliveryReport>>);
    #[async_trait]
    impl TelegramHostPolicy for Policy {
        async fn admit(&self, _: NativeFrame, _: Envelope) -> Result<IngressDecision, String> {
            Ok(IngressDecision::Ignore)
        }
        fn delivered(&self, _: &Envelope, report: &DeliveryReport) {
            self.0.lock().unwrap().push(report.clone());
        }
    }
    fn keyboard() -> InlineKeyboard {
        InlineKeyboard::new(vec![vec![
            InlineButton::new("SQLite <study>", "session:opaque").unwrap(),
        ]])
        .unwrap()
    }
    #[test]
    fn host_keyboard_bounds_and_reserved_approval_namespace_are_enforced() {
        assert!(InlineButton::new("line\nbreak", "key").is_err());
        assert!(InlineButton::new("Valid", "oa:reserved").is_err());
        assert!(InlineButton::new("Valid", "Ж".repeat(33)).is_err());
        assert!(InlineKeyboard::new(vec![]).is_err());
        assert!(
            InlineKeyboard::new(vec![vec![InlineButton::new("Name", "key").unwrap(); 9]]).is_err()
        );
        let fake = Envelope::new(
            ConnectorId::new("model"),
            EventKind::new("chat.reply"),
            "{\"inline_keyboard\":[]}".to_string(),
        )
        .with_tag("inline_keyboard", "pretend");
        assert!(fake.context.get::<InlineKeyboard>().is_none());
    }
    #[tokio::test]
    async fn typed_keyboard_sends_one_literal_plain_message_with_observed_native_receipt() {
        let server = server().await;
        let tracker = Tracker::new(Default::default());
        let policy = Arc::new(Policy::default());
        let host: Arc<dyn TelegramHostPolicy> = policy.clone();
        let env = Envelope::new(
            ConnectorId::new("host"),
            EventKind::new("chat.reply"),
            "<b>Saved chats</b>\nLiteral **names**".to_string(),
        )
        .with_context(keyboard());
        let delivery = Delivery {
            bot: &server.bot,
            tracker: &tracker,
            policy: Some(&host),
            envelope: &env,
            trusted_notice: false,
        };
        send_inline(
            &server.bot,
            ChatId(7),
            env.payload_as::<String>().map(String::as_str),
            env.context.get::<InlineKeyboard>().unwrap(),
            &delivery,
        )
        .await;
        let requests = server.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let body = &requests[0].1;
        assert_eq!(body["text"], "<b>Saved chats</b>\nLiteral **names**");
        assert!(body.get("parse_mode").is_none());
        assert_eq!(
            body["reply_markup"]["inline_keyboard"][0][0]["callback_data"],
            "session:opaque"
        );
        assert_eq!(
            body["reply_markup"]["inline_keyboard"][0][0]["text"],
            "SQLite <study>"
        );
        let reports = policy.0.lock().unwrap();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].status, DeliveryStatus::Sent);
        assert_eq!(reports[0].message_ids, vec![17]);
    }
    #[tokio::test]
    async fn oversized_marked_message_fails_without_split_or_markup_loss() {
        let server = server().await;
        let tracker = Tracker::new(Default::default());
        let policy = Arc::new(Policy::default());
        let host: Arc<dyn TelegramHostPolicy> = policy.clone();
        let env = Envelope::new(
            ConnectorId::new("host"),
            EventKind::new("chat.reply"),
            "Ж".repeat(4097),
        );
        let delivery = Delivery {
            bot: &server.bot,
            tracker: &tracker,
            policy: Some(&host),
            envelope: &env,
            trusted_notice: false,
        };
        send_inline(
            &server.bot,
            ChatId(7),
            env.payload_as::<String>().map(String::as_str),
            &keyboard(),
            &delivery,
        )
        .await;
        assert!(server.requests.lock().unwrap().is_empty());
        assert_eq!(policy.0.lock().unwrap()[0].status, DeliveryStatus::Failed);
    }
}
