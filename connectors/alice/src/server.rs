//! The webhook: one Yandex request → one spoken answer, within Alice's budget.

use std::{sync::Arc, time::Instant};

use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use octo_core::{
    ChannelId, ChannelMetadata, ConnectorContext, ConnectorId, Envelope, EventKind, ReplyChannel,
};

use crate::{
    AliceConnector, CHAT_MESSAGE,
    config::{Settings, normalize},
    dialog::Dialogs,
    protocol::{AliceRequest, AliceResponse, Identity},
};

#[derive(Clone)]
pub struct App {
    pub id: ConnectorId,
    pub settings: Arc<Settings>,
    pub dialogs: Arc<Dialogs>,
    pub ctx: ConnectorContext,
    /// For handing the rest of a reply to the speaker's cloud voice.
    pub connector: Arc<AliceConnector>,
}

pub fn router(app: App) -> Router {
    Router::new()
        .route("/alice/{secret}", post(webhook))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(Arc::new(app))
}

/// What the speaker wants from this request.
#[derive(Debug, PartialEq, Eq)]
enum Intent {
    /// Yandex's availability probe.
    Ping,
    /// «Алиса, запусти навык …» with nothing else said.
    Launch,
    /// «дальше» — hear the parked reply.
    Continue,
    /// «хватит» / Alice's own `on_interrupt`.
    Exit,
    /// Anything else goes to the agent.
    Say(String),
}

fn classify(req: &AliceRequest, settings: &Settings) -> Intent {
    let command = normalize(&req.request.command);
    if command == "ping" || req.request.original_utterance.trim() == "ping" {
        return Intent::Ping;
    }
    if command == "on_interrupt" || settings.exit_words.contains(&command) {
        return Intent::Exit;
    }
    let text = req.text();
    if text.is_empty() {
        return if req.session.new {
            Intent::Launch
        } else {
            Intent::Continue
        };
    }
    if settings.continue_words.contains(&command) {
        return Intent::Continue;
    }
    Intent::Say(text.to_string())
}

async fn webhook(State(app): State<Arc<App>>, Path(secret): Path<String>, body: Bytes) -> Response {
    let started = Instant::now();
    if !constant_time_eq(secret.as_bytes(), app.settings.secret.as_bytes()) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let req: AliceRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "alice: unparseable request");
            return StatusCode::BAD_REQUEST.into_response();
        }
    };
    let version = req.version.clone();
    if let Some(expected) = &app.settings.skill_id
        && req.session.skill_id != *expected
    {
        tracing::warn!(skill_id = %req.session.skill_id, "alice: request for a foreign skill");
        return StatusCode::FORBIDDEN.into_response();
    }

    let intent = classify(&req, &app.settings);
    let phrases = &app.settings.phrases;
    if intent == Intent::Ping {
        return Json(AliceResponse::say(&phrases.pong, &version)).into_response();
    }

    let Some(identity) = req.identity().filter(|i| allowed(&app.settings, i)) else {
        tracing::warn!(
            user_id = req.session.user.as_ref().map(|u| u.user_id.as_str()).unwrap_or(""),
            application_id = %req.session.application.application_id,
            "alice: speaker not on the allow-list — add the id to allowed_users / allowed_applications to let them in"
        );
        return Json(AliceResponse::bye(&phrases.denied, &version)).into_response();
    };
    let channel = identity.channel();

    let answer = match intent {
        Intent::Ping => unreachable!("answered above"),
        Intent::Exit => AliceResponse::bye(&phrases.bye, &version),
        Intent::Launch => {
            // A parked reply is said right away — this is also how the speaker
            // reopens the skill to deliver a reply (`relaunch`).
            speak_next(&app, &channel, &phrases.greeting, &version)
        }
        Intent::Continue => {
            if app.dialogs.has_queued(&channel) || !app.dialogs.is_thinking(&channel) {
                speak_next(&app, &channel, &phrases.nothing_more, &version)
            } else {
                // Nothing is published here, so this cannot fail.
                let _ = wait_for_reply(&app, &channel, started, None).await;
                speak_next(&app, &channel, &phrases.still_thinking, &version)
            }
        }
        Intent::Say(text) => {
            tracing::info!(channel = %channel, chars = text.chars().count(), "alice: heard");
            let env = message(&app, &identity, &channel, text);
            if let Err(e) = wait_for_reply(&app, &channel, started, Some(env)).await {
                tracing::warn!(error = %e, "alice: publish failed");
                return Json(AliceResponse::say(&phrases.still_thinking, &version)).into_response();
            }
            if app.connector.voice.is_some() {
                handed_over(&app, &channel, &version)
            } else {
                speak_next(&app, &channel, &phrases.thinking, &version)
            }
        }
    };
    Json(answer).into_response()
}

/// With the cloud voice: speak what is ready (the rest follows by push once
/// this answer has been said), or answer with a filler and let the speaker say
/// the reply by itself when it comes.
fn handed_over(app: &App, channel: &str, version: &str) -> AliceResponse {
    match app.dialogs.take_or_hand_over(channel) {
        Some((piece, more)) => {
            if more {
                let rest = app.dialogs.drain(channel);
                app.connector
                    .push(channel, rest, crate::quasar::speaking_time(&piece));
            }
            AliceResponse::say(piece, version)
        }
        None => {
            let filler = app.settings.filler();
            app.connector.start_fillers(channel, filler.to_string());
            let end = app.settings.push.as_ref().is_none_or(|p| p.end_session);
            tracing::info!(channel, "alice: turn handed over to the cloud voice");
            if end {
                AliceResponse::bye(filler, version)
            } else {
                AliceResponse::say(filler, version)
            }
        }
    }
}

fn allowed(settings: &Settings, identity: &Identity<'_>) -> bool {
    match identity {
        Identity::User(id) => settings.allowed_users.iter().any(|u| u == id),
        Identity::Application(id) => settings.allowed_applications.iter().any(|a| a == id),
    }
}

/// The `chat.message` for one heard phrase, shaped like a chat connector's.
fn message(app: &App, identity: &Identity<'_>, channel: &str, text: String) -> Envelope {
    let s = &app.settings;
    let meta = ChannelMetadata::new()
        .with_trust(s.trust)
        .with_tag("role", s.role.clone())
        .with_tag("chat_type", "voice")
        .with_tag("chat_id", channel)
        .with_tag("sender_id", identity.id())
        .with_tag("sender_name", s.sender_name.clone());
    Envelope::new(app.id.clone(), EventKind::from_static(CHAT_MESSAGE), text)
        .with_channel(ChannelId::new(channel))
        .with_reply_to(ReplyChannel::new(ChannelId::new(channel)))
        .with_channel_metadata(meta)
}

/// Optionally start a turn, then wait (until the request's budget runs out)
/// for a reply to be parked. The wake-up is armed before publishing, so a
/// fast reply cannot be missed.
async fn wait_for_reply(
    app: &App,
    channel: &str,
    started: Instant,
    publish: Option<Envelope>,
) -> octo_core::OctoResult<()> {
    let notify = app.dialogs.notify(channel);
    let notified = notify.notified();
    tokio::pin!(notified);
    notified.as_mut().enable();
    if let Some(env) = publish {
        app.dialogs.start_turn(channel, env.id);
        app.ctx.publish(env).await?;
    }
    if !app.dialogs.has_queued(channel) {
        let left = app.settings.reply_wait.saturating_sub(started.elapsed());
        let _ = tokio::time::timeout(left, notified).await;
    }
    Ok(())
}

/// Say the next parked piece (with a «say дальше» hint if more remain), or
/// `fallback` when nothing is parked.
fn speak_next(app: &App, channel: &str, fallback: &str, version: &str) -> AliceResponse {
    match app.dialogs.next_piece(channel) {
        Some((piece, true)) => {
            AliceResponse::say(format!("{piece} {}", app.settings.phrases.more), version)
        }
        Some((piece, false)) => AliceResponse::say(piece, version),
        None => AliceResponse::say(fallback, version),
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests;
