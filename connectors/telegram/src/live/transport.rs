use std::sync::Arc;

use octo_core::Envelope;
use teloxide::{
    Bot,
    payloads::{EditMessageTextSetters, SendMessageSetters},
    requests::Requester,
    types::{ChatAction, ChatId, ParseMode},
};
use tokio::{spawn, time::sleep};
use tracing::warn;

use super::{
    Live, LiveKey, STATUS_LINE_MAX, STATUS_MAX_LEN, StatusMsg, TYPING_MAX_REFRESHES,
    TYPING_REFRESH, clip, feedback_open,
};
use crate::{
    DeliveryStatus, TelegramHostPolicy,
    format::esc,
    policy::{
        delivery::Delivery,
        tracking::{MessageRef, Tracker},
    },
};

impl Live {
    /// Start (or restart) the typing indicator for a chat.
    pub(crate) fn start_typing(
        &self,
        bot: Bot,
        chat: ChatId,
        env: &Envelope,
        policy: Option<Arc<dyn TelegramHostPolicy>>,
        tracker: Arc<Tracker>,
    ) {
        if !feedback_open(env) {
            return;
        }
        let key = LiveKey::of(chat, env);
        let mut map = self.typing.lock().unwrap();
        if let Some(old) = map.remove(&key) {
            old.abort();
        }
        let envelope = env.clone();
        let handle = spawn(async move {
            for _ in 0..TYPING_MAX_REFRESHES {
                if !feedback_open(&envelope)
                    || !(Delivery {
                        bot: &bot,
                        tracker: &tracker,
                        policy: policy.as_ref(),
                        envelope: &envelope,
                        trusted_notice: false,
                    })
                    .allowed()
                {
                    break;
                }
                if bot
                    .send_chat_action(chat, ChatAction::Typing)
                    .await
                    .is_err()
                {
                    break;
                }
                if !feedback_open(&envelope) {
                    break;
                }
                sleep(TYPING_REFRESH).await;
            }
        });
        map.insert(key, handle);
    }

    /// Append a progress line to the turn's status message (creating or rolling
    /// it over as needed). Lines render italic; the message is edited in place.
    pub(crate) async fn status(
        &self,
        bot: &Bot,
        chat: ChatId,
        line: &str,
        delivery: &Delivery<'_>,
    ) {
        if !feedback_open(delivery.envelope) || !delivery.allowed() {
            return;
        }
        let key = LiveKey::of(chat, delivery.envelope);
        let line = esc(&clip(line, STATUS_LINE_MAX));
        let existing = self.status.lock().unwrap().get(&key).cloned();

        // Try to grow the tail message in place.
        if let Some(cur) = existing {
            if let Some(&tail) = cur.ids.last() {
                if cur.lines.len() + line.len() < STATUS_MAX_LEN {
                    let lines = format!("{}\n{line}", cur.lines);
                    let edited = bot
                        .edit_message_text(chat, tail, format!("<i>{lines}</i>"))
                        .parse_mode(ParseMode::Html)
                        .await;
                    if !feedback_open(delivery.envelope) {
                        self.end_turn(bot, chat, delivery.envelope);
                        return;
                    }
                    if edited.is_ok() {
                        let mut map = self.status.lock().unwrap();
                        if let Some(entry) = map.get_mut(&key) {
                            entry.lines = lines;
                        }
                        return;
                    }
                    // Edit failed (message deleted / too old) → fall through to a new one.
                }
            }
        }

        // Fresh status message (first of the turn, or a rollover past the size cap).
        // Earlier ids are KEPT so end_turn deletes the whole trace, not just the tail.
        if !feedback_open(delivery.envelope) || !delivery.allowed() {
            return;
        }
        match bot
            .send_message(chat, format!("<i>{line}</i>"))
            .parse_mode(ParseMode::Html)
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
                    .await;
                if !feedback_open(delivery.envelope) || !delivery.allowed() {
                    self.end_turn(bot, chat, delivery.envelope);
                    let _ = bot.delete_message(chat, sent.id).await;
                    return;
                }
                let mut map = self.status.lock().unwrap();
                let entry = map.entry(key).or_insert_with(|| StatusMsg {
                    ids: Vec::new(),
                    lines: String::new(),
                });
                entry.ids.push(sent.id);
                entry.lines = line;
            }
            Err(e) => {
                delivery.report(chat.0, &[], DeliveryStatus::Unknown).await;
                warn!(chat = chat.0, error = %e, "telegram status send failed");
            }
        }
    }
}
