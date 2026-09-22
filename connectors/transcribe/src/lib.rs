//! `octo-connector-transcribe` — turn a workspace audio/video file into text via the
//! ChatGPT-subscription dictation endpoint (the same `auth.json` the LLM uses), so
//! transcription costs no API key and no per-minute billing.
//!
//! An env-as-tools organ: dispatch `transcribe.run { path, language? }` and get a
//! correlated `transcribe.run.result { text }` (or `{ error }`). The file is read from
//! the shared workspace (jailed against `..`/absolute escapes); the subscription token
//! comes from the shared [`SubscriptionAuth`] handed to the connector at construction —
//! the SAME refresh-owner the cogitator's LLM path uses, so there is one token owner.
//!
//! The upload itself, [`transcribe`], is public so an assembly can hear a voice message
//! inline (without a dispatch round-trip) through the same code path.
//!
//! Any length. A short audio clip goes up whole. A long recording, or any video, is first
//! cut on its pauses into ~5-minute audio-only chunks (see [`chunk`]; needs ffmpeg on the
//! host), because the endpoint accepts ~23 minutes but silently truncates a long transcript
//! mid-sentence. The chunks go up in parallel and come back merged with `[hh:mm:ss]`
//! timecodes; a chunk that still fails is marked in place rather than failing the whole
//! recording. Without ffmpeg, a file is uploaded whole, as before.

mod chunk;

use crate::chunk::{is_video, merge, upload_chunks, Scratch};

use std::{
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use chrono::Utc;
use octo_core::{
    Connector, ConnectorCapabilities, ConnectorContext, ConnectorId, Envelope, EventKind, Filter,
    OctoResult, SubscribeOptions,
};
use octo_openai_auth::{Subscription, SubscriptionAuth};
use octo_workspace::{read_in_root, resolve_file_in_root, workspace_root};
use reqwest::{Client as HttpClient, StatusCode};
use serde_json::{json, Value};
use tracing::{info, warn};

/// Command kind this connector accepts.
const RUN: &str = "transcribe.run";
/// The desktop ChatGPT app's dictation endpoint (subscription token).
const URL: &str = "https://chatgpt.com/backend-api/transcribe";
/// What the endpoint expects the client to call itself.
const ORIGINATOR: &str = "Codex Desktop";
/// Upload budget: transcription runs ~15-30x real time, so even a long clip lands well
/// inside this; generous because a slow link, not the ASR, is the risk.
const TIMEOUT: Duration = Duration::from_secs(300);

const CATALOG: &str = "Transcribe a recording to text on the ChatGPT subscription. Dispatch to this connector's id:
- transcribe.run { path, language? } -> { text }
  `path` is a workspace-relative audio/video file (e.g. a recording sent to the chat and saved to
  the inbox); `language` is an optional hint like \"ru\" (omitted -> auto-detected). Any length: a
  long recording or a video is split on its pauses and the text comes back with [hh:mm:ss]
  timecodes per chunk (plus `chunks`, `failed`, `truncated_chunks`). Runs ~15-30x real time.";

pub struct TranscribeConnector {
    id: ConnectorId,
    capabilities: ConnectorCapabilities,
    /// Shared subscription auth — one refresh owner with the rest of the runtime.
    auth: Arc<SubscriptionAuth>,
    /// Explicit workspace root; `None` -> resolved from the environment at use.
    workspace: Option<PathBuf>,
}

impl TranscribeConnector {
    /// Construct the connector with the shared subscription-auth handle and an optional
    /// workspace root (the shared file jail the audio is read from).
    pub fn new(
        id: impl Into<String>,
        auth: Arc<SubscriptionAuth>,
        workspace: Option<PathBuf>,
    ) -> Arc<Self> {
        let capabilities = ConnectorCapabilities::bidirectional()
            .with_accept_kinds([EventKind::from_static(RUN)])
            .with_description(CATALOG);
        Arc::new(Self { id: ConnectorId::new(id), capabilities, auth, workspace })
    }

    async fn handle(&self, env: &Envelope, ctx: &ConnectorContext) {
        if env.kind.as_str() != RUN {
            return;
        }
        let params = env.payload_as::<Value>().cloned().unwrap_or(Value::Null);
        let payload = self.run(&params).await.unwrap_or_else(|e| json!({ "error": e }));
        let resp = Envelope::new(self.id.clone(), EventKind::new(format!("{RUN}.result")), payload)
            .with_correlation(env.id);
        if let Err(e) = ctx.publish(resp).await {
            warn!(error = %e, "transcribe: failed to publish result");
        }
    }

    async fn run(&self, params: &Value) -> Result<Value, String> {
        let path = params
            .get("path")
            .and_then(Value::as_str)
            .ok_or("provide `path` (a workspace-relative audio/video file)")?;
        let language = params.get("language").and_then(Value::as_str);

        let root = workspace_root(self.workspace.as_deref()).map_err(|e| e.to_string())?;
        let filename = Path::new(path).file_name().and_then(|s| s.to_str()).unwrap_or("audio");

        // Measure first: a long recording or a video is split on its pauses. Without
        // ffmpeg on the host the file goes up whole, as it always did.
        let file = resolve_file_in_root(&root, path).map_err(|e| e.to_string())?;
        match chunk::duration(&file).await {
            Ok(total) if is_video(filename) || total > chunk::TARGET_SECS * 1.4 => {
                return self.run_chunked(&file, total, language).await;
            }
            Ok(_) => {}
            Err(e) => warn!(error = %e, "transcribe: cannot measure the recording; uploading it whole"),
        }

        let audio = read_in_root(&root, path).map_err(|e| e.to_string())?;
        let sub = self.auth.fresh().await.map_err(|e| e.to_string())?;
        let content_type = content_type_for(filename);
        info!(%path, bytes = audio.len(), "transcribe: run");
        let text = match transcribe(&audio, filename, content_type, language, &sub).await {
            // The server can revoke a token ahead of its `exp`: refresh once and retry.
            Err(TranscribeError::Unauthorized(_)) => {
                warn!("transcribe: token refused; forcing a refresh and retrying once");
                let sub = self.auth.force_refresh().await.map_err(|e| e.to_string())?;
                transcribe(&audio, filename, content_type, language, &sub).await
            }
            other => other,
        }
        .map_err(|e| e.to_string())?;
        Ok(json!({ "text": text }))
    }

    /// Cut the recording on its pauses, upload the chunks in parallel, and merge the text
    /// with a timecode per chunk. A refused token is refreshed once and the refused chunks
    /// retried; any other failure is marked in place.
    async fn run_chunked(&self, file: &Path, total: f64, language: Option<&str>) -> Result<Value, String> {
        let pauses = chunk::pauses(file).await?;
        let spans = chunk::plan(total, &pauses, chunk::TARGET_SECS);
        let scratch = Scratch::new()?;
        let mut chunks = Vec::with_capacity(spans.len());
        for (i, span) in spans.iter().enumerate() {
            chunks.push(chunk::cut(file, *span, scratch.path(), i).await?);
        }
        info!(secs = total as u64, pauses = pauses.len(), chunks = chunks.len(), "transcribe: split on pauses");

        let mut sub = self.auth.fresh().await.map_err(|e| e.to_string())?;
        let mut texts: Vec<Option<Result<String, TranscribeError>>> = chunks.iter().map(|_| None).collect();
        let mut pending: Vec<usize> = (0..chunks.len()).collect();
        for pass in 0..2 {
            let mut refused = Vec::new();
            for (i, result) in upload_chunks(&chunks, &pending, language, &sub).await {
                if matches!(result, Err(TranscribeError::Unauthorized(_))) {
                    refused.push(i);
                }
                texts[i] = Some(result);
            }
            if refused.is_empty() || pass == 1 {
                break;
            }
            // The server can revoke a token ahead of its `exp`: refresh once and retry.
            warn!(chunks = refused.len(), "transcribe: token refused; forcing a refresh and retrying");
            sub = self.auth.force_refresh().await.map_err(|e| e.to_string())?;
            pending = refused;
        }
        merge(&spans, texts, total)
    }
}


#[async_trait]
impl Connector for TranscribeConnector {
    fn id(&self) -> &ConnectorId {
        &self.id
    }

    fn capabilities(&self) -> &ConnectorCapabilities {
        &self.capabilities
    }

    async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
        let mut cmds = ctx
            .subscribe(Filter::by_target(self.id.clone()), SubscribeOptions::default())
            .await?;
        info!(connector = %self.id, "transcribe ready");
        loop {
            tokio::select! {
                next = cmds.next() => match next {
                    Some(env) => self.handle(&env, &ctx).await,
                    None => return Ok(()),
                },
                _ = ctx.shutdown.cancelled() => return Ok(()),
            }
        }
    }
}

/// Why a transcription failed. `Unauthorized` means the server refused the token — the
/// caller can force a refresh and retry once (a revocation ahead of the JWT's `exp`).
#[derive(Debug)]
pub enum TranscribeError {
    Unauthorized(String),
    Failed(String),
}

impl fmt::Display for TranscribeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unauthorized(msg) | Self::Failed(msg) => f.write_str(msg),
        }
    }
}

impl std::error::Error for TranscribeError {}

/// Guess a content type from the file extension — the dictation endpoint is lenient, so
/// this only needs to be plausible.
pub fn content_type_for(filename: &str) -> &'static str {
    let lower = filename.to_ascii_lowercase();
    let ext = |e: &str| lower.ends_with(e);
    if ext(".ogg") || ext(".oga") || ext(".opus") {
        "audio/ogg"
    } else if ext(".mp3") {
        "audio/mpeg"
    } else if ext(".m4a") || ext(".mp4") || ext(".mov") {
        "audio/mp4"
    } else if ext(".wav") {
        "audio/wav"
    } else if ext(".webm") {
        "audio/webm"
    } else {
        "application/octet-stream"
    }
}

/// POST the audio to the dictation endpoint on the subscription token, returning the
/// transcript. `language` is an optional hint (e.g. `"ru"`); omitted, the endpoint detects it.
pub async fn transcribe(
    audio: &[u8],
    filename: &str,
    content_type: &str,
    language: Option<&str>,
    sub: &Subscription,
) -> Result<String, TranscribeError> {
    let boundary = boundary();
    let body = multipart_body(&boundary, audio, filename, content_type, language);

    let resp = HttpClient::new()
        .post(URL)
        .timeout(TIMEOUT)
        .header("Content-Type", format!("multipart/form-data; boundary={boundary}"))
        .header("Authorization", format!("Bearer {}", sub.access_token))
        .header("chatgpt-account-id", sub.account_id.as_str())
        .header("originator", ORIGINATOR)
        .body(body)
        .send()
        .await
        .map_err(|e| TranscribeError::Failed(format!("request failed: {e}")))?;

    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if status == StatusCode::UNAUTHORIZED {
        return Err(TranscribeError::Unauthorized(format!(
            "HTTP 401: the subscription token was refused: {}",
            snippet(&text)
        )));
    }
    if !status.is_success() {
        // An over-long upload comes back as a plain 500; say what is most likely.
        return Err(TranscribeError::Failed(format!(
            "HTTP {status} (audio too long or the subscription token was refused): {}",
            snippet(&text)
        )));
    }
    let parsed: Value = serde_json::from_str(&text).map_err(|e| {
        TranscribeError::Failed(format!("bad response body: {e}: {}", snippet(&text)))
    })?;
    let transcript = parsed
        .get("text")
        .and_then(Value::as_str)
        .ok_or_else(|| TranscribeError::Failed(format!("no `text` in response: {}", snippet(&text))))?
        .trim()
        .to_string();

    if looks_truncated(&transcript) {
        warn!(chars = transcript.len(), "transcript may be truncated (no terminal punctuation)");
    }
    Ok(transcript)
}

/// A unique multipart boundary, shaped like the desktop client's.
fn boundary() -> String {
    format!("----codex-transcribe-{}", Utc::now().timestamp_nanos_opt().unwrap_or_default())
}

/// Assemble the `multipart/form-data` body by hand — one file part plus an optional
/// language field. No `x-codex-base64` header: that one is the Electron bridge's, not the
/// wire's.
fn multipart_body(
    boundary: &str,
    audio: &[u8],
    filename: &str,
    content_type: &str,
    language: Option<&str>,
) -> Vec<u8> {
    let mut body = Vec::with_capacity(audio.len() + 512);
    if let Some(lang) = language {
        body.extend_from_slice(
            format!("--{boundary}\r\nContent-Disposition: form-data; name=\"language\"\r\n\r\n{lang}\r\n")
                .as_bytes(),
        );
    }
    body.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{filename}\"\r\n\
             Content-Type: {content_type}\r\n\r\n"
        )
        .as_bytes(),
    );
    body.extend_from_slice(audio);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    body
}

/// A transcript that ends mid-sentence is the endpoint's silent-truncation signature.
/// Short replies ("Yes.", "OK") are normal, so only longer text counts.
fn looks_truncated(text: &str) -> bool {
    const TERMINAL: [char; 8] = ['.', '!', '?', '…', '"', ')', ':', ';'];
    text.chars().count() > 200 && !text.trim_end().ends_with(TERMINAL)
}

/// First 200 chars of an error/response body, for a log line that stays readable.
fn snippet(text: &str) -> String {
    text.chars().take(200).collect()
}

#[cfg(test)]
mod tests {
    use super::{content_type_for, looks_truncated, multipart_body};

    #[test]
    fn content_type_is_guessed_from_the_extension() {
        assert_eq!(content_type_for("note.OGG"), "audio/ogg");
        assert_eq!(content_type_for("clip.opus"), "audio/ogg");
        assert_eq!(content_type_for("talk.mp3"), "audio/mpeg");
        assert_eq!(content_type_for("rec.m4a"), "audio/mp4");
        assert_eq!(content_type_for("v.webm"), "audio/webm");
        assert_eq!(content_type_for("mystery"), "application/octet-stream");
    }

    #[test]
    fn truncation_is_flagged_only_for_long_unpunctuated_text() {
        assert!(!looks_truncated("Yes."));
        assert!(!looks_truncated(&("word ".repeat(60) + "end.")));
        assert!(looks_truncated(&"word ".repeat(60)));
    }

    #[test]
    fn multipart_carries_the_language_field_and_the_file() {
        let body = multipart_body("B", b"\x00\x01", "a.ogg", "audio/ogg", Some("ru"));
        let s = String::from_utf8_lossy(&body);
        assert!(s.contains("name=\"language\"\r\n\r\nru\r\n"));
        assert!(s.contains("filename=\"a.ogg\""));
        assert!(s.contains("Content-Type: audio/ogg"));
        assert!(s.trim_end().ends_with("--B--"));
        // The raw audio bytes are present between the header and the closing boundary.
        assert!(body.windows(2).any(|w| w == b"\x00\x01"));
    }

    /// LIVE: the full `transcribe.run` path on a long recording — measured, split on its
    /// pauses, uploaded in parallel, merged with timecodes. Ignored by default:
    ///   TRANSCRIBE_LONG=/path/to/long.ogg \
    ///     cargo test -p octo-connector-transcribe live_transcribe_long -- --ignored --nocapture
    #[tokio::test]
    #[ignore = "hits chatgpt.com; needs ffmpeg, a real subscription auth.json and a long recording"]
    async fn live_transcribe_long() {
        use super::TranscribeConnector;
        use octo_openai_auth::SubscriptionAuth;
        use serde_json::json;
        use std::{path::PathBuf, sync::Arc};

        let file = PathBuf::from(std::env::var("TRANSCRIBE_LONG").expect("set TRANSCRIBE_LONG"));
        let auth_path = std::env::var("ALBERT_AUTH_JSON").unwrap_or_else(|_| {
            format!("{}/.codex/auth.json", std::env::var("HOME").expect("HOME"))
        });
        let auth = Arc::new(SubscriptionAuth::new(PathBuf::from(auth_path)));
        let root = file.parent().expect("a parent dir").to_path_buf();
        let name = file.file_name().and_then(|s| s.to_str()).expect("a file name");

        let connector = TranscribeConnector::new("transcribe", auth, Some(root));
        let started = std::time::Instant::now();
        let out = connector.run(&json!({ "path": name, "language": "ru" })).await.expect("transcribed");
        let text = out["text"].as_str().unwrap_or_default();
        println!(
            "\n=== {} chunks, failed {}, truncated {}, {:.0}s wall ===\n{text}\n=== end ===",
            out["chunks"], out["failed"], out["truncated_chunks"], started.elapsed().as_secs_f64()
        );
        assert!(out["chunks"].as_u64().unwrap_or(1) > 1, "a long recording should be split");
        assert_eq!(out["failed"], 0);
    }

    /// LIVE: transcribe a real recording against the subscription dictation endpoint.
    /// Ignored by default (hits the network + needs a real subscription auth.json):
    ///   TRANSCRIBE_FILE=/path/to/audio.ogg \
    ///     cargo test -p octo-connector-transcribe live_transcribe -- --ignored --nocapture
    /// The token store defaults to $HOME/.codex/auth.json (override with ALBERT_AUTH_JSON).
    #[tokio::test]
    #[ignore = "hits chatgpt.com; needs a real subscription auth.json + a voice file"]
    async fn live_transcribe() {
        use super::{content_type_for, transcribe};
        use octo_openai_auth::SubscriptionAuth;
        use std::path::{Path, PathBuf};

        let file = std::env::var("TRANSCRIBE_FILE").expect("set TRANSCRIBE_FILE=/path/to/audio");
        let auth_path = std::env::var("ALBERT_AUTH_JSON").unwrap_or_else(|_| {
            format!("{}/.codex/auth.json", std::env::var("HOME").expect("HOME"))
        });

        let auth = SubscriptionAuth::new(PathBuf::from(auth_path));
        let sub = auth.fresh().await.expect("a fresh subscription token");
        let bytes = std::fs::read(&file).expect("read the audio file");
        let name = Path::new(&file).file_name().and_then(|s| s.to_str()).unwrap_or("audio");

        let text = transcribe(&bytes, name, content_type_for(name), Some("ru"), &sub)
            .await
            .expect("the endpoint transcribes the recording");
        println!("\n=== TRANSCRIPT ({} chars) ===\n{text}\n=== end ===\n", text.len());
        assert!(!text.trim().is_empty(), "transcript should not be empty");
    }
}
