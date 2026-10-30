use octo_core::Blob;
use teloxide::{
    Bot, RequestError,
    payloads::SendMessageSetters,
    requests::Requester,
    types::{ChatId, ParseMode},
};

use tracing::{info, warn};

use crate::{
    DeliveryStatus,
    api::{is_unsupported, send_rich_markdown},
    format::{
        has_media, media_as_links, sanitize_rich, split_for_telegram, split_rich, strip_tags,
        to_telegram_html,
    },
    policy::{delivery::Delivery, tracking::MessageRef},
};

/// Host-only preference for Markdown rendered as native Telegram text/entities.
/// Paragraph gaps become literal newlines rather than rich-message block layout.
#[derive(Clone, Copy, Debug, Default)]
pub struct ClassicMarkdown;

/// How an outgoing file is presented in Telegram.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Outbound {
    /// `sendPhoto`: an inline preview.
    Photo,
    /// `sendVoice`: the play-in-place voice bubble. Telegram only renders OGG/Opus
    /// this way — an MP3 or M4A sent as a voice note comes back as an error.
    Voice,
    /// `sendDocument`: everything else.
    Document,
}

/// Classify an outgoing file by its extension. The name is ours (a workspace path
/// or a caller-chosen `filename`), not a remote-controlled string.
pub(super) fn outbound_kind(filename: &str) -> Outbound {
    let lower = filename.to_ascii_lowercase();
    let has = |exts: &[&str]| exts.iter().any(|ext| lower.ends_with(ext));
    if has(&[".png", ".jpg", ".jpeg", ".webp", ".gif"]) {
        Outbound::Photo
    } else if has(&[".ogg", ".oga", ".opus"]) {
        Outbound::Voice
    } else {
        Outbound::Document
    }
}

/// An audio `Blob` that Telegram will accept as a voice note: OGG/Opus by MIME, or
/// by the filename it was labelled with.
pub(super) fn is_voice_blob(blob: &Blob) -> bool {
    blob.content_type().starts_with("audio/ogg")
        || blob
            .filename()
            .is_some_and(|f| outbound_kind(f) == Outbound::Voice)
}

/// Send a model reply to a chat: a rich message where the server has them
/// (Markdown as-is, laid out by Telegram), dropping to the pre-10.1 HTML
/// renderer and then to plain text if it doesn't — so a reply always lands, and
/// only its formatting degrades.
pub(super) async fn send_reply(
    bot: &Bot,
    chat: ChatId,
    text: &str,
    rich_messages: &mut bool,
    delivery: Option<&Delivery<'_>>,
) {
    if !*rich_messages
        || delivery
            .is_some_and(|delivery| delivery.envelope.context.get::<ClassicMarkdown>().is_some())
    {
        send_html(bot, chat, text, delivery).await;
        return;
    }
    for chunk in split_rich(text) {
        if delivery.is_some_and(|d| !d.allowed()) {
            return;
        }
        let markdown = sanitize_rich(&chunk);
        match send_rich_markdown(bot, chat, &markdown).await {
            Ok(sent) => {
                if let Some(delivery) = delivery {
                    match sent {
                        Some(sent) => delivery.report(chat.0, &[sent], DeliveryStatus::Sent).await,
                        None => delivery.report(chat.0, &[], DeliveryStatus::Unknown).await,
                    }
                }
                info!(%chat, "sent reply (rich)");
            }
            Err(e) => {
                if e.unknown() {
                    if let Some(delivery) = delivery {
                        delivery.report(chat.0, &[], DeliveryStatus::Unknown).await;
                    }
                    return;
                }
                if is_unsupported(e.description()) {
                    warn!(error = %e, "telegram: server has no rich messages; using the HTML renderer from here on");
                    *rich_messages = false;
                    send_html(bot, chat, &chunk, delivery).await;
                    continue;
                }
                // Telegram fetches every media URL itself, and one it can't fetch
                // fails the whole message. Try once more with the media as links,
                // so the text still arrives rich and the pictures stay one tap away.
                if has_media(&chunk) {
                    if delivery.is_some_and(|d| !d.allowed()) {
                        return;
                    }
                    warn!(error = %e, "telegram rich send with media failed; retrying with the media as links");
                    let linked = media_as_links(&chunk);
                    match send_rich_markdown(bot, chat, &sanitize_rich(&linked)).await {
                        Ok(sent) => {
                            if let Some(delivery) = delivery {
                                match sent {
                                    Some(sent) => {
                                        delivery.report(chat.0, &[sent], DeliveryStatus::Sent).await
                                    }
                                    None => {
                                        delivery.report(chat.0, &[], DeliveryStatus::Unknown).await
                                    }
                                }
                            }
                            info!(%chat, "sent reply (rich, media as links)");
                            continue;
                        }
                        Err(e) => {
                            if e.unknown() {
                                if let Some(delivery) = delivery {
                                    delivery.report(chat.0, &[], DeliveryStatus::Unknown).await;
                                }
                                return;
                            }
                            warn!(error = %e, "telegram rich send failed; falling back to HTML")
                        }
                    }
                    send_html(bot, chat, &linked, delivery).await;
                    continue;
                }
                warn!(error = %e, "telegram rich send failed; falling back to HTML");
                send_html(bot, chat, &chunk, delivery).await;
            }
        }
    }
}

/// The pre-10.1 path, and the fallback under a rejected rich send: the model's
/// Markdown rendered into Telegram's HTML subset, split to the message-length
/// limit, and stripped to plain text if the Bot API rejects a chunk's HTML.
pub(super) async fn send_html(
    bot: &Bot,
    chat: ChatId,
    text: &str,
    delivery: Option<&Delivery<'_>>,
) {
    let html = to_telegram_html(text);
    for chunk in split_for_telegram(&html) {
        if delivery.is_some_and(|d| !d.allowed()) {
            return;
        }
        let sent = bot
            .send_message(chat, chunk.clone())
            .parse_mode(ParseMode::Html)
            .await;
        match sent {
            Ok(sent) => {
                if let Some(delivery) = delivery {
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
                }
                info!(%chat, "sent reply");
            }
            Err(e) => {
                if !matches!(
                    e,
                    RequestError::Api(_)
                        | RequestError::RetryAfter(_)
                        | RequestError::MigrateToChatId(_)
                ) {
                    if let Some(delivery) = delivery {
                        delivery.report(chat.0, &[], DeliveryStatus::Unknown).await;
                    }
                    return;
                }
                warn!(error = %e, "telegram HTML send failed; retrying as plain text");
                let plain = strip_tags(&chunk);
                if delivery.is_some_and(|d| !d.allowed()) {
                    return;
                }
                match bot.send_message(chat, plain).await {
                    Ok(sent) => {
                        if let Some(delivery) = delivery {
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
                        }
                    }
                    Err(e2) => {
                        if let Some(delivery) = delivery {
                            delivery.report(chat.0, &[], DeliveryStatus::Unknown).await;
                        }
                        warn!(error = %e2, "telegram plain send failed");
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use axum::{Json, Router, http::StatusCode, routing::post, serve};
    use chrono::Utc;
    use octo_core::{ChannelId, ConnectorId, Envelope, EventKind};
    use reqwest::Url;
    use serde_json::{Value, json};
    use teloxide::{Bot, types::ChatId};
    use tokio::{net::TcpListener, spawn, task::JoinHandle};

    use super::{ClassicMarkdown, send_reply};
    use crate::{
        DeliveryReport, DeliveryStatus, IngressDecision, IngressScope, NativeFrame,
        TelegramHostPolicy, TelegramOutputScope,
        callbacks::testing::server as accepting_server,
        policy::{delivery::Delivery, tracking::Tracker},
    };

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

    async fn server(unknown: bool) -> (Bot, Arc<Mutex<Vec<Value>>>, JoinHandle<()>) {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let capture = requests.clone();
        let app = Router::new().fallback(post(move |Json(body): Json<Value>| {
            let mut requests = capture.lock().unwrap();
            requests.push(body.clone());
            let id = requests.len() as i32;
            let rejected = body.get("rich_message").is_some() || body.get("parse_mode").is_some();
            let response = if unknown {
                (StatusCode::OK, json!({"ok":true,"result":true}))
            } else if rejected {
                (StatusCode::BAD_REQUEST, json!({"ok":false,"error_code":400,"description":"Bad Request: invalid formatting"}))
            } else {
                (StatusCode::OK, json!({"ok":true,"result":{"message_id":id,"date":Utc::now().timestamp(),"chat":{"id":42,"type":"private","first_name":"Person"},"text":"sent"}}))
            };
            async move { (response.0, Json(response.1)) }
        }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = spawn(async move { serve(listener, app).await.unwrap() });
        let bot =
            Bot::new("123:FAKE").set_api_url(Url::parse(&format!("http://{address}")).unwrap());
        (bot, requests, task)
    }

    fn envelope() -> Envelope {
        Envelope::new(ConnectorId::new("host"), EventKind::new("chat.reply"), true)
            .with_channel(ChannelId::new("42"))
            .with_context(TelegramOutputScope {
                scope: IngressScope {
                    key: "session".into(),
                    epoch: "epoch".into(),
                    ephemeral: true,
                    cleanup_scope: Some("route".into()),
                },
                attempt_id: Some("attempt".into()),
            })
    }

    #[tokio::test]
    async fn classic_control_preserves_blank_lines_and_formatting_without_disabling_rich_replies() {
        let server = accepting_server().await;
        let tracker = Tracker::new(Default::default());
        let policy = Arc::new(Policy::default());
        let host: Arc<dyn TelegramHostPolicy> = policy.clone();
        let text = "Chat opened: example.\n\nLatest recorded message:\n\n**A formatted preview**";
        let env = Envelope::new(
            ConnectorId::new("host"),
            EventKind::new("chat.reply"),
            text.to_owned(),
        )
        .with_context(ClassicMarkdown);
        let delivery = Delivery {
            bot: &server.bot,
            tracker: &tracker,
            policy: Some(&host),
            envelope: &env,
            trusted_notice: false,
        };
        let mut rich = true;
        send_reply(&server.bot, ChatId(7), text, &mut rich, Some(&delivery)).await;
        assert!(rich);
        {
            let requests = server.requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            assert!(requests[0].0.ends_with("sendmessage"));
            assert_eq!(requests[0].1["parse_mode"], "HTML");
            assert_eq!(
                requests[0].1["text"],
                "Chat opened: example.\n\nLatest recorded message:\n\n<b>A formatted preview</b>"
            );
            assert!(requests[0].1.get("rich_message").is_none());
        }
        assert_eq!(policy.0.lock().unwrap()[0].status, DeliveryStatus::Sent);
        let ordinary = Envelope::new(
            ConnectorId::new("host"),
            EventKind::new("chat.reply"),
            "**An ordinary answer**".to_owned(),
        );
        let delivery = Delivery {
            envelope: &ordinary,
            ..delivery
        };
        send_reply(
            &server.bot,
            ChatId(7),
            "**An ordinary answer**",
            &mut rich,
            Some(&delivery),
        )
        .await;
        let requests = server.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests[1].0.ends_with("sendrichmessage"));
        assert_eq!(
            requests[1].1["rich_message"]["markdown"],
            "**An ordinary answer**"
        );
    }

    #[tokio::test]
    async fn every_split_and_fallback_tracks_only_confirmed_native_ids() {
        let (bot, requests, task) = server(false).await;
        let policy = Arc::new(Policy::default());
        let host: Arc<dyn TelegramHostPolicy> = policy.clone();
        let tracker = Tracker::new(Default::default());
        let env = envelope();
        let delivery = Delivery {
            bot: &bot,
            tracker: &tracker,
            policy: Some(&host),
            envelope: &env,
            trusted_notice: false,
        };
        let mut rich = true;
        send_reply(
            &bot,
            ChatId(42),
            &"word ".repeat(1800),
            &mut rich,
            Some(&delivery),
        )
        .await;
        let requests = requests.lock().unwrap();
        let expected = requests
            .iter()
            .enumerate()
            .filter(|(_, body)| {
                body.get("rich_message").is_none() && body.get("parse_mode").is_none()
            })
            .map(|(i, _)| i as i32 + 1)
            .collect::<Vec<_>>();
        assert!(expected.len() > 1);
        let reports = policy.0.lock().unwrap();
        assert!(reports.iter().all(|r| r.status == DeliveryStatus::Sent));
        assert_eq!(
            reports
                .iter()
                .flat_map(|r| r.message_ids.clone())
                .collect::<Vec<_>>(),
            expected
        );
        task.abort();
    }

    #[tokio::test]
    async fn unconfirmed_rich_success_is_unknown_and_does_not_fallback() {
        let (bot, requests, task) = server(true).await;
        let policy = Arc::new(Policy::default());
        let host: Arc<dyn TelegramHostPolicy> = policy.clone();
        let tracker = Tracker::new(Default::default());
        let env = envelope();
        let delivery = Delivery {
            bot: &bot,
            tracker: &tracker,
            policy: Some(&host),
            envelope: &env,
            trusted_notice: false,
        };
        send_reply(&bot, ChatId(42), "text", &mut true, Some(&delivery)).await;
        assert_eq!(requests.lock().unwrap().len(), 1);
        let reports = policy.0.lock().unwrap();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].status, DeliveryStatus::Unknown);
        assert!(reports[0].message_ids.is_empty());
        task.abort();
    }
}
