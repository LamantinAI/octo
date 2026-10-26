//! Live-turn feedback — what the chat shows *while* the cogitator is thinking.
//!
//! Two envelope kinds drive it (both addressed to this connector, the chat id
//! on the `channel`, like `chat.reply`):
//!
//! - `chat.typing` (payload ignored) → the Telegram "typing…" indicator, kept
//!   alive by re-sending `sendChatAction` until the turn's reply arrives (the
//!   Bot API shows it ~5 s per call, so a loop refreshes it), bounded so an
//!   abandoned turn can't type forever.
//! - `chat.status` (payload `String`) → one *status message* per turn that
//!   accumulates progress lines ("→ tool …") by editing itself in place — the
//!   openclaw-style live trace of tool use, without flooding the chat.
//!
//! A `chat.reply` or native host `chat.turn.end` ends the feedback: the typing loop
//! stops and the status message
//! (if any) is deleted — the trace is transient scaffolding, not conversation.
//! Native `chat.turn.end` also closes an exact feedback token without a reply.

#[cfg(test)]
mod testing;
mod transport;

use octo_core::Envelope;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use teloxide::{
    Bot,
    requests::Requester,
    types::{ChatId, MessageId},
};
use tokio::{spawn, task::JoinHandle};
use tracing::warn;

use crate::{CleanupRequest, IngressScope, TURN_END, TelegramOutputScope, TurnFeedback};

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct LiveKey {
    chat: i64,
    scope: Option<(String, String, Option<String>)>,
    attempt: Option<String>,
}
impl LiveKey {
    fn of(chat: ChatId, env: &Envelope) -> Self {
        let scope = env
            .context
            .get::<TelegramOutputScope>()
            .cloned()
            .or_else(|| {
                env.context
                    .get::<IngressScope>()
                    .cloned()
                    .map(|scope| TelegramOutputScope {
                        scope,
                        attempt_id: None,
                    })
            });
        Self {
            chat: chat.0,
            scope: scope.as_ref().map(|s| {
                (
                    s.scope.key.clone(),
                    s.scope.epoch.clone(),
                    s.scope.cleanup_scope.clone(),
                )
            }),
            attempt: env
                .context
                .get::<TurnFeedback>()
                .map(|feedback| feedback.id().to_owned())
                .or_else(|| scope.and_then(|s| s.attempt_id)),
        }
    }
    fn cleanup(&self, request: &CleanupRequest) -> bool {
        self.scope.as_ref().is_some_and(|(key, epoch, cleanup)| {
            epoch == &request.epoch && cleanup.as_ref().unwrap_or(key) == &request.scope_key
        })
    }
}

/// Refresh cadence for the typing indicator (the Bot API shows ~5 s per call).
const TYPING_REFRESH: Duration = Duration::from_millis(4500);
/// Upper bound on one turn's typing loop — an abandoned turn stops typing here.
const TYPING_MAX_REFRESHES: u32 = 130; // ≈ 10 minutes
/// Grow the status message up to here, then start a new one (Telegram caps a
/// message at 4096 UTF-16 units; stay well under with the `<i>` wrapper).
const STATUS_MAX_LEN: usize = 3500;
/// One status line is clipped to this many chars — it's a progress trace, not a log.
const STATUS_LINE_MAX: usize = 300;

/// The turn's status trace: every Telegram message it has occupied — rollover past
/// the size cap starts a new one but the earlier ids are kept so the end of the
/// turn can delete the WHOLE trace — plus the accumulated (escaped) lines of the
/// tail message (the one still being edited in place).
#[derive(Clone)]
struct StatusMsg {
    ids: Vec<MessageId>,
    lines: String,
}

/// Per-chat live-turn state, shared by the connector's outbound loop.
#[derive(Default)]
pub(crate) struct Live {
    typing: Mutex<HashMap<LiveKey, JoinHandle<()>>>,
    status: Mutex<HashMap<LiveKey, StatusMsg>>,
}

impl Live {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// The turn's reply is out: stop typing and delete the status message — the
    /// trace is scaffolding for the wait, not part of the conversation.
    pub(crate) fn end_turn(&self, bot: &Bot, chat: ChatId, env: &Envelope) {
        if let Some(feedback) = env.context.get::<TurnFeedback>() {
            feedback.close();
        }
        let key = LiveKey::of(chat, env);
        if let Some(h) = self.typing.lock().unwrap().remove(&key) {
            h.abort();
        }
        if let Some(status) = self.status.lock().unwrap().remove(&key) {
            let bot = bot.clone();
            spawn(async move {
                // Delete every message the trace occupied — rollover may have split it
                // across several; the behaviour must be uniform for short and long turns.
                for id in status.ids {
                    if let Err(e) = bot.delete_message(chat, id).await {
                        warn!(chat = chat.0, error = %e, "telegram status delete failed");
                    }
                }
            });
        }
    }

    /// Consume internal completion before content policy/ACL checks. A JSON-only
    /// tool command cannot mint the native token required to affect live state.
    pub(crate) fn handle_end(&self, bot: &Bot, env: &Envelope) -> bool {
        if env.kind.as_str() != TURN_END {
            return false;
        }
        if let Some(feedback) = env.context.get::<TurnFeedback>() {
            feedback.close();
            if let Some(chat) = env
                .channel
                .as_ref()
                .and_then(|channel| channel.as_str().parse::<i64>().ok())
            {
                self.end_turn(bot, ChatId(chat), env);
            }
        }
        true
    }

    pub(crate) fn end_scope(&self, request: &CleanupRequest) {
        self.typing.lock().unwrap().retain(|key, task| {
            if key.cleanup(request) {
                task.abort();
                false
            } else {
                true
            }
        });
        self.status
            .lock()
            .unwrap()
            .retain(|key, _| !key.cleanup(request));
    }
}

fn feedback_open(env: &Envelope) -> bool {
    env.context
        .get::<TurnFeedback>()
        .is_none_or(TurnFeedback::is_open)
}

/// Clip to at most `max` chars on a char boundary, marking the cut with an ellipsis.
fn clip(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let cut: String = s.chars().take(max).collect();
    format!("{cut}…")
}

#[cfg(test)]
mod tests {
    use std::{future::pending, time::Duration};

    use octo_core::{ChannelId, ConnectorId, EventKind};
    use serde_json::json;
    use tokio::time::timeout;

    use super::testing::server;
    use super::*;
    use crate::policy::{delivery::Delivery, tracking::Tracker};

    fn feedback_env(scope: &str, token: &TurnFeedback) -> Envelope {
        Envelope::new(
            ConnectorId::new("host"),
            EventKind::new("chat.typing"),
            json!({}),
        )
        .with_channel(ChannelId::new("42"))
        .with_context(TelegramOutputScope {
            scope: IngressScope {
                key: scope.into(),
                epoch: "epoch".into(),
                ephemeral: false,
                cleanup_scope: Some(format!("cleanup/{scope}")),
            },
            attempt_id: Some("same-native-request".into()),
        })
        .with_context(token.clone())
    }

    #[test]
    fn clip_respects_char_boundaries() {
        assert_eq!(clip("привет", 10), "привет");
        assert_eq!(clip("привет мир", 6), "привет…");
    }
    #[tokio::test]
    async fn old_final_cannot_remove_new_attempt_or_another_session_status() {
        use crate::{IngressScope, TelegramOutputScope};
        use octo_core::{ConnectorId, Envelope, EventKind};
        let live = Live::default();
        let chat = ChatId(42);
        let env = |scope: &str, attempt: &str| {
            Envelope::new(
                ConnectorId::new("host"),
                EventKind::new("chat.status"),
                String::new(),
            )
            .with_context(TelegramOutputScope {
                scope: IngressScope {
                    key: scope.into(),
                    epoch: "e".into(),
                    ephemeral: true,
                    cleanup_scope: Some(scope.into()),
                },
                attempt_id: Some(attempt.into()),
            })
        };
        let old = LiveKey::of(chat, &env("same", "old"));
        let current = LiveKey::of(chat, &env("same", "current"));
        let other = LiveKey::of(chat, &env("other", "current"));
        let mut state = live.status.lock().unwrap();
        for key in [&old, &current, &other] {
            state.insert(
                key.clone(),
                StatusMsg {
                    ids: vec![],
                    lines: String::new(),
                },
            );
        }
        drop(state);
        live.end_turn(&Bot::new("123:FAKE"), chat, &env("same", "old"));
        let state = live.status.lock().unwrap();
        assert!(state.contains_key(&current));
        assert!(state.contains_key(&other));
        drop(state);
        live.end_scope(&CleanupRequest {
            scope_key: "same".into(),
            epoch: "e".into(),
        });
        let state = live.status.lock().unwrap();
        assert!(!state.contains_key(&current));
        assert!(state.contains_key(&other));
    }

    #[tokio::test]
    async fn native_end_cleans_exact_attempt_even_when_content_is_fenced() {
        let server = server(false).await;
        let live = Live::new();
        let tracker = Tracker::new(Default::default());
        let old = TurnFeedback::new("old-phase");
        let current = TurnFeedback::new("new-phase");
        let other = TurnFeedback::new("other-phase");
        let old_env = feedback_env("same", &old);
        let new_env = feedback_env("same", &current);
        let other_env = feedback_env("other", &other);
        for env in [&old_env, &new_env, &other_env] {
            let key = LiveKey::of(ChatId(42), env);
            live.typing
                .lock()
                .unwrap()
                .insert(key.clone(), spawn(pending()));
            live.status.lock().unwrap().insert(
                key,
                StatusMsg {
                    ids: vec![],
                    lines: String::new(),
                },
            );
        }
        let scope = old_env.context.get::<TelegramOutputScope>().unwrap();
        tracker.close(&Tracker::request(&scope.scope).unwrap());
        let blocked = Delivery {
            bot: &server.bot,
            tracker: &tracker,
            policy: None,
            envelope: &old_env,
            trusted_notice: false,
        };
        assert!(!blocked.allowed());
        let mut end = old_env.clone();
        end.kind = EventKind::new(TURN_END);
        assert!(live.handle_end(&server.bot, &end));
        assert!(live.handle_end(&server.bot, &end));
        assert!(!old.is_open());
        assert!(current.is_open() && other.is_open());
        for state in [
            &live
                .typing
                .lock()
                .unwrap()
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            &live
                .status
                .lock()
                .unwrap()
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
        ] {
            assert!(!state.contains(&LiveKey::of(ChatId(42), &old_env)));
            assert!(state.contains(&LiveKey::of(ChatId(42), &new_env)));
            assert!(state.contains(&LiveKey::of(ChatId(42), &other_env)));
        }
        assert!(server.requests.lock().unwrap().is_empty());
        live.end_turn(&server.bot, ChatId(42), &new_env);
        live.end_turn(&server.bot, ChatId(42), &other_env);
    }

    #[tokio::test]
    async fn end_before_queued_feedback_never_restarts_it_or_blocks_final_reply() {
        let server = server(false).await;
        let live = Live::new();
        let tracker = Tracker::new(Default::default());
        let token = TurnFeedback::new("phase");
        let env = feedback_env("session", &token);
        let mut forged = Envelope::new(
            ConnectorId::new("tool"),
            EventKind::new(TURN_END),
            json!({"attempt_id":"phase"}),
        )
        .with_channel(ChannelId::new("42"));
        assert!(live.handle_end(&server.bot, &forged));
        assert!(token.is_open());
        forged = env.clone();
        forged.kind = EventKind::new(TURN_END);
        live.handle_end(&server.bot, &forged);
        live.start_typing(server.bot.clone(), ChatId(42), &env, None, tracker.clone());
        let delivery = Delivery {
            bot: &server.bot,
            tracker: &tracker,
            policy: None,
            envelope: &env,
            trusted_notice: false,
        };
        live.status(&server.bot, ChatId(42), "late queued status", &delivery)
            .await;
        assert!(live.typing.lock().unwrap().is_empty());
        assert!(live.status.lock().unwrap().is_empty());
        assert!(
            delivery.allowed(),
            "closed feedback never blocks a final content reply"
        );
        assert!(server.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn status_confirmation_after_close_is_deleted_without_reinserting_state() {
        let server = server(true).await;
        let live = Live::new();
        let tracker = Tracker::new(Default::default());
        let token = TurnFeedback::new("old-phase");
        let env = feedback_env("session", &token);
        let worker_live = live.clone();
        let bot = server.bot.clone();
        let worker_env = env.clone();
        let pending = spawn(async move {
            let delivery = Delivery {
                bot: &bot,
                tracker: &tracker,
                policy: None,
                envelope: &worker_env,
                trusted_notice: false,
            };
            worker_live
                .status(&bot, ChatId(42), "in flight", &delivery)
                .await;
        });
        timeout(Duration::from_secs(2), server.started.notified())
            .await
            .unwrap();
        let mut end = env.clone();
        end.kind = EventKind::new(TURN_END);
        live.handle_end(&server.bot, &end);
        server.release.notify_one();
        timeout(Duration::from_secs(2), pending)
            .await
            .unwrap()
            .unwrap();
        server.wait("deletemessage", 1).await;
        let requests = server.requests.lock().unwrap();
        let deleted = requests
            .iter()
            .filter(|(method, _)| method.ends_with("deletemessage"))
            .map(|(_, body)| body["message_id"].as_i64().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(deleted, [17]);
        assert!(live.status.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn normal_reply_ends_token_and_legacy_feedback_can_start_the_next_turn() {
        let server = server(false).await;
        let live = Live::new();
        let tracker = Tracker::new(Default::default());
        let token = TurnFeedback::new("phase");
        let scoped = feedback_env("session", &token);
        live.start_typing(
            server.bot.clone(),
            ChatId(42),
            &scoped,
            None,
            tracker.clone(),
        );
        server.wait("sendchataction", 1).await;
        live.end_turn(&server.bot, ChatId(42), &scoped);
        assert!(!token.is_open());
        let legacy = Envelope::new(
            ConnectorId::new("legacy"),
            EventKind::new("chat.typing"),
            json!({}),
        )
        .with_channel(ChannelId::new("42"));
        for count in [2, 3] {
            live.start_typing(
                server.bot.clone(),
                ChatId(42),
                &legacy,
                None,
                tracker.clone(),
            );
            server.wait("sendchataction", count).await;
            live.end_turn(&server.bot, ChatId(42), &legacy);
        }
        assert!(live.typing.lock().unwrap().is_empty());
    }
}
