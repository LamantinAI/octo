use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};

use chrono::Utc;
use teloxide::{
    Bot,
    requests::Requester,
    types::{ChatId, MessageId},
};
use tokio::time::timeout;

use super::{CleanupReport, CleanupRequest, IngressScope, NativeFrame, TrackingLimits};

#[derive(Clone, Copy, Debug)]
pub(crate) struct MessageRef {
    pub chat_id: i64,
    pub message_id: i32,
    pub date: i64,
}
#[derive(Default)]
struct Scope {
    closed: bool,
    ids: BTreeMap<(i64, i32), MessageRef>,
    overflow: usize,
    chat: Option<i64>,
}
pub(crate) struct Tracker {
    inner: Mutex<HashMap<(String, String), Scope>>,
    limits: TrackingLimits,
}

impl Tracker {
    pub(crate) fn new(limits: TrackingLimits) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(HashMap::new()),
            limits,
        })
    }
    fn key(scope: &IngressScope) -> Option<(String, String)> {
        scope
            .cleanup_scope
            .clone()
            .or_else(|| scope.ephemeral.then(|| scope.key.clone()))
            .map(|key| (key, scope.epoch.clone()))
    }
    /// Return true if these exact IDs belong to an already-closed excursion.
    pub(crate) fn record(&self, scope: &IngressScope, messages: &[MessageRef]) -> Result<bool, ()> {
        let Some(key) = Self::key(scope) else {
            return Ok(false);
        };
        let mut states = self.inner.lock().map_err(|_| ())?;
        if !states.contains_key(&key) && states.len() >= self.limits.max_scopes {
            return Err(());
        }
        let state = states.entry(key).or_default();
        for message in messages {
            if state.chat.is_some_and(|chat| chat != message.chat_id) {
                continue;
            }
            if message.message_id <= 0 {
                state.overflow += 1;
                continue;
            }
            if !state
                .ids
                .contains_key(&(message.chat_id, message.message_id))
                && state.ids.len() >= self.limits.max_ids_per_scope
            {
                state.overflow += 1;
                continue;
            }
            state
                .ids
                .insert((message.chat_id, message.message_id), *message);
        }
        Ok(state.closed)
    }
    pub(crate) fn native(&self, scope: &IngressScope, frame: &NativeFrame) -> Result<bool, ()> {
        if let Some(key) = Self::key(scope) {
            let mut states = self.inner.lock().map_err(|_| ())?;
            if !states.contains_key(&key) && states.len() >= self.limits.max_scopes {
                return Err(());
            }
            let state = states.entry(key).or_default();
            if state.chat.is_some_and(|chat| chat != frame.chat_id) {
                return Err(());
            }
            state.chat = Some(frame.chat_id);
        }
        self.record(
            scope,
            &[MessageRef {
                chat_id: frame.chat_id,
                message_id: frame.message_id,
                date: frame.date,
            }],
        )
    }
    pub(crate) fn close(&self, request: &CleanupRequest) -> bool {
        if let Ok(mut states) = self.inner.lock() {
            let key = (request.scope_key.clone(), request.epoch.clone());
            if !states.contains_key(&key) && states.len() >= self.limits.max_scopes {
                return false;
            }
            states.entry(key).or_default().closed = true;
            return true;
        }
        false
    }
    pub(crate) fn is_closed(&self, scope: &IngressScope) -> bool {
        Self::key(scope)
            .and_then(|key| {
                self.inner
                    .lock()
                    .ok()
                    .and_then(|states| states.get(&key).map(|s| s.closed))
            })
            .unwrap_or(false)
    }
    pub(crate) fn request(scope: &IngressScope) -> Option<CleanupRequest> {
        Self::key(scope).map(|(scope_key, epoch)| CleanupRequest { scope_key, epoch })
    }
    pub(crate) async fn cleanup(&self, bot: &Bot, request: &CleanupRequest) -> CleanupReport {
        if !self.close(request) {
            return CleanupReport {
                requested: 1,
                unknown: 1,
                ..CleanupReport::default()
            };
        }
        let key = (request.scope_key.clone(), request.epoch.clone());
        let (ids, overflow) = self
            .inner
            .lock()
            .ok()
            .and_then(|states| {
                states.get(&key).map(|state| {
                    (
                        state.ids.values().copied().collect::<Vec<_>>(),
                        state.overflow,
                    )
                })
            })
            .unwrap_or_default();
        let mut report = CleanupReport {
            requested: ids.len() + overflow,
            unknown: overflow,
            ..CleanupReport::default()
        };
        let now = Utc::now().timestamp();
        let maximum = self.limits.cleanup_batches.saturating_mul(100);
        let mut groups: BTreeMap<i64, Vec<MessageRef>> = BTreeMap::new();
        for (index, id) in ids.iter().enumerate() {
            if index >= maximum {
                report.unknown += 1;
                continue;
            }
            if id.date > now || now.saturating_sub(id.date) >= 48 * 60 * 60 {
                report.failed += 1;
                continue;
            }
            groups.entry(id.chat_id).or_default().push(*id);
        }
        let mut batches = 0;
        for (chat, messages) in groups {
            for chunk in messages.chunks(100) {
                if batches >= self.limits.cleanup_batches {
                    report.unknown += chunk.len();
                    continue;
                }
                batches += 1;
                let message_ids = chunk
                    .iter()
                    .map(|r| MessageId(r.message_id))
                    .collect::<Vec<_>>();
                match timeout(
                    self.limits.cleanup_timeout,
                    bot.delete_messages(ChatId(chat), message_ids),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        report.deleted += chunk.len();
                        if let Ok(mut states) = self.inner.lock() {
                            if let Some(state) = states.get_mut(&key) {
                                for id in chunk {
                                    state.ids.remove(&(id.chat_id, id.message_id));
                                }
                            }
                        }
                    }
                    Ok(Err(_)) => report.failed += chunk.len(),
                    Err(_) => report.unknown += chunk.len(),
                }
            }
        }
        report
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn scope() -> IngressScope {
        IngressScope {
            key: "temp".into(),
            epoch: "epoch".into(),
            ephemeral: true,
            cleanup_scope: Some("excursion".into()),
        }
    }
    #[test]
    fn exact_ids_survive_save_and_late_ids_remain_closed() {
        let tracker = Tracker::new(TrackingLimits::default());
        let mut scope = scope();
        assert!(
            !tracker
                .record(
                    &scope,
                    &[MessageRef {
                        chat_id: 42,
                        message_id: 9,
                        date: 1
                    }]
                )
                .unwrap()
        );
        let request = Tracker::request(&scope).unwrap();
        tracker.close(&request);
        scope.ephemeral = false;
        assert!(
            tracker
                .record(
                    &scope,
                    &[MessageRef {
                        chat_id: 42,
                        message_id: 2,
                        date: 2
                    }]
                )
                .unwrap()
        );
        let states = tracker.inner.lock().unwrap();
        let state = states.get(&(request.scope_key, request.epoch)).unwrap();
        assert_eq!(
            state.ids.keys().copied().collect::<Vec<_>>(),
            vec![(42, 2), (42, 9)]
        );
        assert!(
            !state.ids.contains_key(&(42, 8)),
            "no numeric-range inference"
        );
    }
    #[tokio::test]
    async fn cleanup_deletes_only_exact_owned_ids_and_handles_late_confirmation() {
        use crate::{DeliveryStatus, TelegramOutputScope, policy::delivery::Delivery};
        use axum::{Json, Router, routing::post, serve};
        use octo_core::{ChannelId, ConnectorId, Envelope, EventKind};
        use reqwest::Url;
        use serde_json::{Value, json};
        use tokio::{net::TcpListener, spawn};
        let requests = Arc::new(Mutex::new(Vec::<Value>::new()));
        let capture = requests.clone();
        let app = Router::new().fallback(post(move |Json(body): Json<Value>| {
            capture.lock().unwrap().push(body);
            async { Json(json!({"ok":true,"result":true})) }
        }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = spawn(async move { serve(listener, app).await.unwrap() });
        let bot =
            Bot::new("123:FAKE").set_api_url(Url::parse(&format!("http://{address}")).unwrap());
        let tracker = Tracker::new(TrackingLimits::default());
        let scope = scope();
        let now = Utc::now().timestamp();
        let frame = NativeFrame {
            connector: ConnectorId::new("telegram"),
            bot_id: 1,
            update_id: 1,
            message_id: 9,
            date: now,
            chat_id: 42,
            actor_id: Some(7),
            chat_type: "private".into(),
            command_text: None,
            digest: "hash".into(),
            media: false,
        };
        tracker.native(&scope, &frame).unwrap();
        tracker
            .record(
                &scope,
                &[MessageRef {
                    chat_id: 43,
                    message_id: 5,
                    date: now,
                }],
            )
            .unwrap();
        let request = Tracker::request(&scope).unwrap();
        tracker.close(&request);
        let env = Envelope::new(
            ConnectorId::new("host"),
            EventKind::new("chat.reply"),
            "late".to_string(),
        )
        .with_channel(ChannelId::new("42"))
        .with_context(TelegramOutputScope {
            scope: scope.clone(),
            attempt_id: Some("old".into()),
        });
        Delivery {
            bot: &bot,
            tracker: &tracker,
            policy: None,
            envelope: &env,
            trusted_notice: false,
        }
        .report(
            42,
            &[MessageRef {
                chat_id: 42,
                message_id: 3,
                date: now,
            }],
            DeliveryStatus::Sent,
        )
        .await;
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0]["chat_id"], 42);
        assert_eq!(requests[0]["message_ids"], json!([3, 9]));
        assert!(
            tracker
                .inner
                .lock()
                .unwrap()
                .get(&(request.scope_key, request.epoch))
                .unwrap()
                .ids
                .is_empty()
        );
        server.abort();
    }
    #[tokio::test]
    async fn cleanup_timeout_is_bounded_and_retains_unknown_ids() {
        use axum::{Json, Router, routing::post, serve};
        use reqwest::Url;
        use serde_json::json;
        use std::time::Duration;
        use tokio::{
            net::TcpListener,
            spawn,
            time::{sleep, timeout},
        };
        let app = Router::new().fallback(post(|| async {
            sleep(Duration::from_secs(10)).await;
            Json(json!({"ok":true,"result":true}))
        }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = spawn(async move { serve(listener, app).await.unwrap() });
        let bot =
            Bot::new("123:FAKE").set_api_url(Url::parse(&format!("http://{address}")).unwrap());
        let tracker = Tracker::new(TrackingLimits {
            cleanup_batches: 1,
            cleanup_timeout: Duration::from_millis(20),
            ..TrackingLimits::default()
        });
        let scope = scope();
        let now = Utc::now().timestamp();
        tracker
            .record(
                &scope,
                &(1..=150)
                    .map(|id| MessageRef {
                        chat_id: 42,
                        message_id: id,
                        date: now,
                    })
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        let request = Tracker::request(&scope).unwrap();
        let report = timeout(Duration::from_secs(1), tracker.cleanup(&bot, &request))
            .await
            .unwrap();
        assert_eq!(
            (
                report.requested,
                report.deleted,
                report.failed,
                report.unknown
            ),
            (150, 0, 0, 150)
        );
        assert_eq!(
            tracker
                .inner
                .lock()
                .unwrap()
                .get(&(request.scope_key, request.epoch))
                .unwrap()
                .ids
                .len(),
            150
        );
        server.abort();
    }
}
