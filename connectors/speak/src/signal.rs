//! Signalling: the call request to ChatGPT Voice — the SDP offer and the frozen session go
//! up in one POST on the subscription token; the SDP answer comes back.

use std::time::Duration;

use octo_openai_auth::Subscription;
use reqwest::{Client as HttpClient, StatusCode};
use serde_json::{json, Value};
use tracing::info;
use uuid::Uuid;

use crate::VOICES;

/// The desktop ChatGPT Voice call endpoint (a client-owned WebRTC call).
pub(crate) const CALL_URL: &str =
    "https://chatgpt.com/backend-api/wham/realtime/calls?intent=quicksilver&architecture=avas";

/// The only model this call accepts.
pub(crate) const MODEL: &str = "gpt-live-1-codex";

/// What the endpoint expects the client to call itself.
pub(crate) const ORIGINATOR: &str = "Codex Desktop";

/// The call-API alpha header the desktop sends for a client-owned call.
pub(crate) const OPENAI_ALPHA: &str = "quicksilver=v2";

/// Frozen session instructions: the call starts speaking on its own, so "read this
/// verbatim" turns it into a TTS engine. The text is appended after `TEXT:\n`.
pub(crate) const INSTRUCTIONS: &str = "You are a text-to-speech engine. As soon as the session starts, \
    read the following text aloud verbatim, word for word, in its original language, with \
    natural intonation. Read ALL of it to the very end, then stay silent. Do not add \
    anything, do not greet, do not comment, do not summarize.\n\nTEXT:\n";

/// The `session` object of the call request. `delegation: client` is what the desktop
/// sends for a client-owned call.
pub(crate) fn session_payload(text: &str, voice: &str) -> Value {
    json!({
        "audio": { "output": { "voice": voice } },
        "delegation": { "type": "client" },
        "initial_items": [],
        "instructions": format!("{INSTRUCTIONS}{text}"),
        "model": MODEL,
    })
}

/// Why a call failed. `Unauthorized` means the server refused the token — the caller can
/// force a refresh and retry once (a revocation ahead of the JWT's `exp`).
#[derive(Debug)]
pub(crate) enum CallError {
    Unauthorized(String),
    Failed(String),
}

impl CallError {
    pub(crate) fn into_message(self) -> String {
        match self {
            Self::Unauthorized(msg) | Self::Failed(msg) => msg,
        }
    }
}

impl From<String> for CallError {
    fn from(msg: String) -> Self {
        Self::Failed(msg)
    }
}

impl From<&str> for CallError {
    fn from(msg: &str) -> Self {
        Self::Failed(msg.to_string())
    }
}

/// POST the SDP offer + the session to the call endpoint on the subscription token,
/// returning the SDP answer. Ports the desktop client's headers/session verbatim.
pub(crate) async fn post_call(
    offer_sdp: &str,
    text: &str,
    voice: &str,
    sub: &Subscription,
) -> Result<String, CallError> {
    let body = json!({ "sdp": offer_sdp, "session": session_payload(text, voice) });
    let resp = HttpClient::new()
        .post(CALL_URL)
        .timeout(Duration::from_secs(60))
        .header("Authorization", format!("Bearer {}", sub.access_token))
        .header("chatgpt-account-id", sub.account_id.as_str())
        .header("originator", ORIGINATOR)
        .header("User-Agent", ORIGINATOR)
        .header("OpenAI-Alpha", OPENAI_ALPHA)
        .header("Thread-Id", Uuid::now_v7().to_string())
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("call request failed: {e}"))?;

    let status = resp.status();
    let used = resp
        .headers()
        .get("x-codex-primary-used-percent")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let text_body = resp.text().await.unwrap_or_default();
    if status == StatusCode::UNAUTHORIZED {
        return Err(CallError::Unauthorized(explain_http(status.as_u16(), &text_body)));
    }
    if !status.is_success() {
        return Err(CallError::Failed(explain_http(status.as_u16(), &text_body)));
    }
    if let Some(pct) = used {
        info!(voice, quota_used_percent = %pct, "speak: call created");
    }

    // The body is the raw SDP answer; tolerate a JSON wrapper ({ "sdp": ... }) just in case.
    let trimmed = text_body.trim_start();
    if trimmed.starts_with("v=") {
        Ok(text_body)
    } else if let Ok(v) = serde_json::from_str::<Value>(&text_body) {
        v.get("sdp")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| format!("no SDP in call response: {}", snippet(&text_body)).into())
    } else {
        Err(format!("call response is neither SDP nor JSON: {}", snippet(&text_body)).into())
    }
}

/// First 200 chars of an error/response body, for a log line that stays readable.
pub(crate) fn snippet(text: &str) -> String {
    text.chars().take(200).collect()
}

/// Map the endpoint's errors to something the agent can act on.
pub(crate) fn explain_http(code: u16, body: &str) -> String {
    match code {
        401 => "401 — the subscription token was refused; sign in again.".into(),
        403 if body.contains("Voice session access denied") => format!(
            "403 Voice session access denied — usually an unknown voice; use one of {VOICES:?}"
        ),
        429 => "429 — the Codex usage window is exhausted; try later.".into(),
        _ => format!("HTTP {code}: {}", snippet(body)),
    }
}

#[cfg(test)]
mod tests {
    use super::{explain_http, session_payload, MODEL};

    #[test]
    fn session_names_the_voice_and_model() {
        let s = session_payload("hello", "cove");
        assert_eq!(s["audio"]["output"]["voice"], "cove");
        assert_eq!(s["delegation"]["type"], "client");
        assert_eq!(s["model"], MODEL);
        assert!(s["instructions"].as_str().unwrap().ends_with("TEXT:\nhello"));
    }

    #[test]
    fn http_errors_are_actionable() {
        assert!(explain_http(401, "").contains("sign in again"));
        assert!(explain_http(403, "Voice session access denied").contains("unknown voice"));
        assert!(explain_http(429, "").contains("usage window"));
    }
}
