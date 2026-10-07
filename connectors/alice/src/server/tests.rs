//! End-to-end webhook tests against a real in-process bus and a fake agent.

use std::time::Duration;

use axum::{body::Body, http::Request};
use octo_core::{InProcessBus, SubscribeOptions};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

use super::*;
use crate::{AliceConnector, Phrases};

const SECRET: &str = "s3cret-path-0123456789";

fn settings() -> Settings {
    Settings {
        listen: "127.0.0.1:0".parse().unwrap(),
        secret: SECRET.into(),
        skill_id: Some("skill".into()),
        allowed_users: vec!["U".into()],
        allowed_applications: vec![],
        role: "trusted".into(),
        trust: octo_core::TrustLevel::Medium,
        sender_name: "голос".into(),
        reply_wait: Duration::from_millis(300),
        turn_ttl: Duration::from_secs(60),
        max_chars: 120,
        phrases: Phrases::default(),
        continue_words: vec!["дальше".into()],
        exit_words: vec!["хватит".into()],
    }
}

struct Harness {
    router: Router,
    bus: Arc<InProcessBus>,
}

/// Wire a connector's webhook and reply loop to a fresh bus; `agent` answers
/// every `chat.message` with `reply(text)` after `delay`.
async fn harness(delay: Duration, reply: fn(&str) -> String) -> Harness {
    let bus = Arc::new(InProcessBus::new(64));
    let ctx = ConnectorContext::new(CancellationToken::new(), bus.clone());
    let connector = AliceConnector::new(ConnectorId::new("alice"), settings());

    let mut outbound = ctx
        .subscribe(
            octo_core::Filter::by_target(connector.id.clone()),
            SubscribeOptions::default(),
        )
        .await
        .unwrap();
    let parked = connector.clone();
    tokio::spawn(async move {
        while let Some(env) = outbound.next().await {
            parked.on_outbound(&env);
        }
    });

    let mut inbound = ctx
        .subscribe(
            octo_core::Filter::by_kind("chat.message"),
            SubscribeOptions::default(),
        )
        .await
        .unwrap();
    let agent_ctx = ctx.clone();
    tokio::spawn(async move {
        while let Some(env) = inbound.next().await {
            let text = env.payload_as::<String>().cloned().unwrap_or_default();
            assert_eq!(
                env.channel_metadata
                    .as_ref()
                    .unwrap()
                    .tags
                    .get("chat_type")
                    .map(String::as_str),
                Some("voice")
            );
            tokio::time::sleep(delay).await;
            let out = Envelope::new(
                ConnectorId::new("brain"),
                EventKind::from_static("chat.reply"),
                reply(&text),
            )
            .with_target(env.source.clone())
            .with_channel(env.channel.clone().unwrap())
            .with_correlation(env.id);
            agent_ctx.publish(out).await.unwrap();
        }
    });

    let router = router(App {
        id: connector.id.clone(),
        settings: connector.settings.clone(),
        dialogs: connector.dialogs.clone(),
        ctx,
    });
    Harness { router, bus }
}

fn request(user: Option<&str>, new: bool, command: &str, original: &str) -> Value {
    let mut session =
        json!({"new": new, "skill_id": "skill", "application": {"application_id": "A"}});
    if let Some(u) = user {
        session["user"] = json!({"user_id": u});
    }
    json!({"session": session, "request": {"command": command, "original_utterance": original, "type": "SimpleUtterance"}, "version": "1.0"})
}

async fn post(h: &Harness, secret: &str, body: &Value) -> (StatusCode, Value) {
    let resp = h
        .router
        .clone()
        .oneshot(
            Request::post(format!("/alice/{secret}"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

fn said(v: &Value) -> &str {
    v["response"]["text"].as_str().unwrap()
}

#[tokio::test]
async fn fast_reply_is_spoken_in_the_same_request() {
    let h = harness(Duration::from_millis(20), |t| format!("**Ответ**: {t}")).await;
    let (status, v) = post(
        &h,
        SECRET,
        &request(
            Some("U"),
            true,
            "сколько времени",
            "попроси помощника сколько времени",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(said(&v), "Ответ: сколько времени.");
    assert_eq!(v["response"]["end_session"], false);
    assert_eq!(v["version"], "1.0");
}

#[tokio::test]
async fn slow_reply_waits_for_continue_and_dalshe_does_not_reach_the_bus() {
    let h = harness(Duration::from_millis(600), |_| "Готово.".into()).await;
    let (_, v) = post(
        &h,
        SECRET,
        &request(Some("U"), false, "напиши план", "Напиши план"),
    )
    .await;
    assert_eq!(said(&v), Phrases::default().thinking);

    // Still thinking: «дальше» waits out the budget, then says so.
    let mut probe = h.bus.subscribe_sync(
        octo_core::Filter::by_kind("chat.message"),
        SubscribeOptions::default(),
    );
    let (_, v) = post(&h, SECRET, &request(Some("U"), false, "дальше", "Дальше.")).await;
    assert!(said(&v) == "Готово." || said(&v) == Phrases::default().still_thinking);
    if said(&v) != "Готово." {
        tokio::time::sleep(Duration::from_millis(400)).await;
        let (_, v) = post(&h, SECRET, &request(Some("U"), false, "дальше", "Дальше")).await;
        assert_eq!(said(&v), "Готово.");
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(50), probe.next())
            .await
            .is_err(),
        "«дальше» must never be published"
    );
    let (_, v) = post(&h, SECRET, &request(Some("U"), false, "дальше", "дальше")).await;
    assert_eq!(said(&v), Phrases::default().nothing_more);
}

#[tokio::test]
async fn long_reply_is_handed_out_in_pieces() {
    let h = harness(Duration::from_millis(10), |_| {
        "Это длинное предложение номер один. ".repeat(8)
    })
    .await;
    let (_, v) = post(
        &h,
        SECRET,
        &request(Some("U"), false, "расскажи", "Расскажи"),
    )
    .await;
    let more = Phrases::default().more;
    assert!(said(&v).ends_with(&more), "{}", said(&v));
    let mut pieces = 1;
    loop {
        let (_, v) = post(&h, SECRET, &request(Some("U"), false, "дальше", "дальше")).await;
        pieces += 1;
        assert!(said(&v).chars().count() <= 1024);
        if !said(&v).ends_with(&more) {
            break;
        }
    }
    assert!(pieces >= 3);
}

#[tokio::test]
async fn strangers_wrong_secret_ping_and_exit() {
    let h = harness(Duration::from_millis(10), |_| "x".into()).await;
    let mut probe = h.bus.subscribe_sync(
        octo_core::Filter::by_kind("chat.message"),
        SubscribeOptions::default(),
    );

    let (_, v) = post(
        &h,
        SECRET,
        &request(Some("STRANGER"), false, "привет", "привет"),
    )
    .await;
    assert_eq!(said(&v), Phrases::default().denied);
    assert_eq!(v["response"]["end_session"], true);
    let (_, v) = post(&h, SECRET, &request(None, false, "привет", "привет")).await;
    assert_eq!(
        said(&v),
        Phrases::default().denied,
        "device A is not allowed"
    );

    let (status, _) = post(
        &h,
        "wrong-secret-0123456789",
        &request(Some("U"), false, "привет", "привет"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (_, v) = post(&h, SECRET, &request(None, false, "ping", "ping")).await;
    assert_eq!(said(&v), Phrases::default().pong);

    let (_, v) = post(&h, SECRET, &request(Some("U"), true, "", "")).await;
    assert_eq!(said(&v), Phrases::default().greeting);
    let (_, v) = post(&h, SECRET, &request(Some("U"), false, "хватит", "Хватит!")).await;
    assert_eq!(v["response"]["end_session"], true);

    let mut foreign = request(Some("U"), false, "привет", "привет");
    foreign["session"]["skill_id"] = json!("other");
    let (status, _) = post(&h, SECRET, &foreign).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    assert!(
        tokio::time::timeout(Duration::from_millis(50), probe.next())
            .await
            .is_err(),
        "nothing above may reach the agent"
    );
}
