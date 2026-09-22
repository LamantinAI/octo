//! `octo-connector-speak` — turn text into a spoken Ogg/Opus voice note via a native
//! WebRTC call to ChatGPT Voice on the subscription (the same `auth.json` the LLM uses),
//! so speech costs no API key and no per-character billing.
//!
//! An env-as-tools organ: dispatch `speak.run { text, voice? }` and get a correlated
//! `speak.run.result { path }` (or `{ error }`). The `.ogg` is written into the shared
//! workspace (jailed against `..`/absolute escapes); `chat.send_file { path }` on the
//! telegram connector then sends it as a voice note (an `.ogg` goes out via `sendVoice`).
//! The subscription token comes from the shared [`SubscriptionAuth`] handed in at
//! construction — the SAME refresh-owner the cogitator's LLM path uses.
//!
//! HOW IT WORKS. The desktop ChatGPT Voice call is a client-owned WebRTC "call" to
//! GPT-Live whose session instructions are "read this text verbatim"; the model speaks on
//! its own as soon as the session starts, so those instructions turn the call into a TTS
//! engine. We drive the call with [`str0m`] (sans-IO WebRTC): we own the UDP socket and the
//! poll loop. str0m's depacketizer puts reordered packets back in order; a packet that never
//! arrives becomes a zero-length Opus frame, which the player conceals (PLC), so the timeline
//! keeps its length instead of the voice skipping ahead. The Opus frames are remuxed
//! straight into Ogg — no decode/encode.
//!
//! FIRST CUT: one call per `speak.run`, up to ~3000 chars (≈ under two minutes of speech).
//! Longer text should be split by the caller. Non-trickle ICE with a single host candidate
//! off the default route — enough on a public-IP host (the deploy); a srflx candidate via
//! STUN for hosts behind NAT is the follow-up.

use std::{
    io::ErrorKind,
    net::{IpAddr, SocketAddr, UdpSocket},
    path::PathBuf,
    sync::{Arc, Once},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use chrono::Utc;
use octo_core::{
    Connector, ConnectorCapabilities, ConnectorContext, ConnectorId, Envelope, EventKind, Filter,
    OctoResult, SubscribeOptions,
};
use octo_openai_auth::{Subscription, SubscriptionAuth};
use octo_workspace::{workspace_root, write_in_root};
use reqwest::{Client as HttpClient, StatusCode};
use serde_json::{json, Value};
use str0m::{
    change::SdpAnswer,
    format::Codec,
    media::{Direction, Frequency, MediaKind, MediaTime, Mid, Pt},
    net::{Protocol, Receive},
    Candidate, Event, IceConnectionState, Input, Output, Rtc, RtcConfig,
};
use tracing::{info, warn};
use uuid::Uuid;

/// Command kind this connector accepts.
const RUN: &str = "speak.run";
/// The desktop ChatGPT Voice call endpoint (a client-owned WebRTC call).
const CALL_URL: &str =
    "https://chatgpt.com/backend-api/wham/realtime/calls?intent=quicksilver&architecture=avas";
/// The only model this call accepts.
const MODEL: &str = "gpt-live-1-codex";
/// What the endpoint expects the client to call itself.
const ORIGINATOR: &str = "Codex Desktop";
/// The call-API alpha header the desktop sends for a client-owned call.
const OPENAI_ALPHA: &str = "quicksilver=v2";
/// Frozen session instructions: the call starts speaking on its own, so "read this
/// verbatim" turns it into a TTS engine. The text is appended after `TEXT:\n`.
const INSTRUCTIONS: &str = "You are a text-to-speech engine. As soon as the session starts, \
    read the following text aloud verbatim, word for word, in its original language, with \
    natural intonation. Read ALL of it to the very end, then stay silent. Do not add \
    anything, do not greet, do not comment, do not summarize.\n\nTEXT:\n";
/// The voices the endpoint accepts.
const VOICES: [&str; 9] =
    ["cove", "juniper", "maple", "spruce", "ember", "vale", "breeze", "arbor", "sol"];
/// Default voice when the caller does not name one.
const DEFAULT_VOICE: &str = "cove";
/// 833 chars ≈ 30 s of speech; keep one call under ~2 minutes.
const MAX_CHARS: usize = 3000;
/// One 20 ms Opus frame at 48 kHz is 960 samples — the outbound-silence and granule step.
const SAMPLES_PER_FRAME: u64 = 960;
/// A minimal valid 20 ms Opus frame (CELT fullband, near-silence). The call wants a
/// "microphone"; its content is irrelevant to a TTS turn, only the RTP flow matters.
const SILENCE_FRAME: [u8; 3] = [0xf8, 0xff, 0xfe];
/// Give the WebRTC connection this long to produce the first audio frame.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);
/// Hard ceiling on one recording, in case `turn.done` never lands.
const TURN_TIMEOUT: Duration = Duration::from_secs(180);
/// Keep receiving this long after `turn.done`, so the audio tail lands.
const TAIL_GRACE: Duration = Duration::from_millis(1000);
/// Audio packets str0m waits for a reordered one before releasing a gap (default 15).
const REORDER_PACKETS: usize = 25;
/// Fill at most one second of lost packets; a longer hole is a dead link or a pause.
const MAX_FILL_FRAMES: u64 = 50;

const CATALOG: &str = "Speak text aloud as a voice note on the ChatGPT subscription. Dispatch to this connector's id:
- speak.run { text, voice? } -> { path }
  `text` is what to say (up to ~3000 chars per call, about one voice message); `voice` is optional,
  one of cove/juniper/maple/spruce/ember/vale/breeze/arbor/sol (default cove). Returns `path`: a
  workspace-relative Ogg/Opus file; send it with chat.send_file { path } and it arrives as a voice note.";

/// str0m installs a process-wide crypto provider (for DTLS/SRTP); do it once.
static CRYPTO: Once = Once::new();
fn install_crypto() {
    CRYPTO.call_once(|| str0m::crypto::from_feature_flags().install_process_default());
}

pub struct SpeakConnector {
    id: ConnectorId,
    capabilities: ConnectorCapabilities,
    /// Shared subscription auth — one refresh owner with the rest of the runtime.
    auth: Arc<SubscriptionAuth>,
    /// Explicit workspace root; `None` -> resolved from the environment at use.
    workspace: Option<PathBuf>,
}

impl SpeakConnector {
    /// Construct the connector with the shared subscription-auth handle and an optional
    /// workspace root (the shared file jail the `.ogg` is written into).
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
            warn!(error = %e, "speak: failed to publish result");
        }
    }

    async fn run(&self, params: &Value) -> Result<Value, String> {
        let text = params
            .get("text")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|t| !t.is_empty())
            .ok_or("provide `text` (what to say)")?;
        if text.chars().count() > MAX_CHARS {
            return Err(format!(
                "text is {} chars; max {MAX_CHARS} per call — split it into parts",
                text.chars().count()
            ));
        }
        let voice = params.get("voice").and_then(Value::as_str).unwrap_or(DEFAULT_VOICE);
        if !VOICES.contains(&voice) {
            return Err(format!("unknown voice {voice}; use one of {VOICES:?}"));
        }

        let root = workspace_root(self.workspace.as_deref()).map_err(|e| e.to_string())?;
        let sub = self.auth.fresh().await.map_err(|e| e.to_string())?;

        info!(chars = text.chars().count(), %voice, "speak: run");
        let (ogg, transcript) = match synthesize(text, voice, &sub).await {
            // The server can revoke a token ahead of its `exp`: refresh once and retry.
            Err(CallError::Unauthorized(_)) => {
                warn!("speak: token refused; forcing a refresh and retrying once");
                let sub = self.auth.force_refresh().await.map_err(|e| e.to_string())?;
                synthesize(text, voice, &sub).await
            }
            other => other,
        }
        .map_err(CallError::into_message)?;

        let rel = format!("speech-{}.ogg", Utc::now().timestamp_nanos_opt().unwrap_or_default());
        write_in_root(&root, &rel, &ogg).map_err(|e| e.to_string())?;
        info!(path = %rel, bytes = ogg.len(), "speak: wrote voice note");

        let mut out = json!({ "path": rel, "voice": voice, "bytes": ogg.len() });
        if let Some(t) = transcript {
            out["transcript"] = json!(t);
        }
        Ok(out)
    }
}

#[async_trait]
impl Connector for SpeakConnector {
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
        info!(connector = %self.id, "speak ready");
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

/// The `session` object of the call request. `delegation: client` is what the desktop
/// sends for a client-owned call.
fn session_payload(text: &str, voice: &str) -> Value {
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
enum CallError {
    Unauthorized(String),
    Failed(String),
}

impl CallError {
    fn into_message(self) -> String {
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
async fn post_call(
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
fn snippet(text: &str) -> String {
    text.chars().take(200).collect()
}

/// Map the endpoint's errors to something the agent can act on.
fn explain_http(code: u16, body: &str) -> String {
    match code {
        401 => "401 — the subscription token was refused; sign in again.".into(),
        403 if body.contains("Voice session access denied") => format!(
            "403 Voice session access denied — usually an unknown voice; use one of {VOICES:?}"
        ),
        429 => "429 — the Codex usage window is exhausted; try later.".into(),
        _ => format!("HTTP {code}: {}", snippet(body)),
    }
}

/// Run one WebRTC call end-to-end: build the offer, POST it, accept the answer, then drive
/// the sans-IO loop (off the async runtime) collecting Opus frames into Ogg.
async fn synthesize(
    text: &str,
    voice: &str,
    sub: &Subscription,
) -> Result<(Vec<u8>, Option<String>), CallError> {
    install_crypto();

    // Bind to the default-route IP (not 0.0.0.0): the ICE agent only accepts traffic whose
    // destination is one of our host candidates, so the socket's address must BE the
    // candidate. The offer carries it before we POST (non-trickle ICE).
    let socket = UdpSocket::bind(SocketAddr::new(routable_ip()?, 0)).map_err(|e| format!("bind udp: {e}"))?;
    let local = socket.local_addr().map_err(|e| e.to_string())?;

    let mut rtc = RtcConfig::new()
        // A recording has no playout deadline, so hold out a little longer than the default
        // (15) for reordered audio before releasing a gap; not much longer, or frames stuck
        // behind a lost packet near the end of the turn would miss the tail window.
        .set_reordering_size_audio(REORDER_PACKETS)
        .build(Instant::now());
    let candidate = Candidate::host(local, "udp").map_err(|e| format!("host candidate: {e}"))?;
    rtc.add_local_candidate(candidate);

    // One SendRecv audio m-line (the call wants a "mic") + the oai-events data channel.
    let mut api = rtc.sdp_api();
    let mid = api.add_media(MediaKind::Audio, Direction::SendRecv, None, None, None);
    let _cid = api.add_channel("oai-events".to_string());
    let (offer, pending) = api.apply().ok_or("no SDP changes to apply")?;
    let offer_sdp = offer.to_sdp_string();

    let answer_sdp = post_call(&offer_sdp, text, voice, sub).await?;
    let answer = SdpAnswer::from_sdp_string(&answer_sdp)
        .map_err(|e| format!("parse SDP answer: {e}"))?;
    rtc.sdp_api()
        .accept_answer(pending, answer)
        .map_err(|e| format!("accept SDP answer: {e}"))?;

    // The poll loop is blocking (a std UdpSocket with a read timeout); keep it off the
    // async runtime.
    let recorded = tokio::task::spawn_blocking(move || drive_call(rtc, socket, local, mid))
        .await
        .map_err(|e| format!("speak task panicked: {e}"))?;
    Ok(recorded?)
}

/// The sans-IO loop: pump 20 ms outbound silence once connected, collect inbound Opus
/// frames, and stop shortly after `turn.done` (so the audio tail lands). Returns the muxed
/// Ogg and the model's own transcript (if it reported one).
fn drive_call(
    mut rtc: Rtc,
    socket: UdpSocket,
    local: SocketAddr,
    mid: Mid,
) -> Result<(Vec<u8>, Option<String>), String> {
    let opus_pt = opus_pt(&rtc).ok_or("no Opus payload type negotiated")?;

    let mut rec = Recorder::default();
    let mut transcript: Option<String> = None;
    let mut connected = false;
    let mut stop_at: Option<Instant> = None;

    let start = Instant::now();
    let mut out_ts: u64 = 0;
    let mut next_silence = start;
    let mut buf = vec![0u8; 2000];

    loop {
        // Drain poll_output fully after every input; the loop ends when it yields a Timeout.
        let timeout = loop {
            match rtc.poll_output().map_err(|e| format!("rtc: {e}"))? {
                Output::Timeout(t) => break t,
                Output::Transmit(t) => {
                    let _ = socket.send_to(&t.contents, t.destination);
                }
                Output::Event(Event::Connected) => {
                    // ICE + DTLS are up: media writes are no longer dropped. Start the
                    // silence clock now, not at `start`, so there is no catch-up burst.
                    connected = true;
                    next_silence = Instant::now();
                }
                // Before the first connect, str0m may report Disconnected while checks are
                // still running; only a drop of an established call ends the recording.
                Output::Event(Event::IceConnectionStateChange(IceConnectionState::Disconnected))
                    if connected =>
                {
                    warn!(frames = rec.frames.len(), "speak: call disconnected before turn.done");
                    return finish(rec, transcript);
                }
                Output::Event(Event::MediaData(m)) => rec.push(m.time.numer(), &m.data),
                Output::Event(Event::ChannelData(d)) => match parse_oai(&d.data) {
                    Oai::TurnDone(t) => {
                        transcript = t;
                        stop_at.get_or_insert(Instant::now() + TAIL_GRACE);
                    }
                    Oai::Error(e) => return Err(format!("voice session error: {e}")),
                    Oai::Other => {}
                },
                Output::Event(_) => {}
            }
        };

        let now = Instant::now();
        if stop_at.is_some_and(|at| now >= at) {
            info!(frames = rec.frames.len(), filled = rec.filled, "speak: turn.done");
            return finish(rec, transcript);
        }
        if rec.frames.is_empty() && now.duration_since(start) > CONNECT_TIMEOUT {
            return Err(format!(
                "no audio within {} s — WebRTC did not connect",
                CONNECT_TIMEOUT.as_secs()
            ));
        }
        if now.duration_since(start) > TURN_TIMEOUT {
            warn!(frames = rec.frames.len(), "speak: no turn.done within the ceiling — recording may be cut short");
            return finish(rec, transcript);
        }

        // Feed the "microphone": one 20 ms silence frame per due tick.
        if connected {
            while next_silence <= now {
                if let Some(writer) = rtc.writer(mid) {
                    let rtp = MediaTime::new(out_ts, Frequency::FORTY_EIGHT_KHZ);
                    let _ = writer.write(opus_pt, now, rtp, SILENCE_FRAME.to_vec());
                }
                out_ts += SAMPLES_PER_FRAME;
                next_silence += Duration::from_millis(20);
            }
        }

        // Always read the socket (ICE can't complete otherwise), waiting until str0m's own
        // timeout — capped at the next silence tick once connected. The 1 ms floor keeps a
        // past-due timeout from turning this into a busy spin.
        let mut wait = timeout.saturating_duration_since(now);
        if connected {
            wait = wait.min(next_silence.saturating_duration_since(now));
        }
        let wait = wait.clamp(Duration::from_millis(1), Duration::from_millis(20));
        socket.set_read_timeout(Some(wait)).map_err(|e| e.to_string())?;
        match socket.recv_from(&mut buf) {
            Ok((n, source)) => {
                let contents = buf[..n].try_into().map_err(|e| format!("bad datagram: {e}"))?;
                let recv = Receive { proto: Protocol::Udp, source, destination: local, contents };
                rtc.handle_input(Input::Receive(Instant::now(), recv)).map_err(|e| e.to_string())?;
            }
            Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
            Err(e) => return Err(format!("udp recv: {e}")),
        }
        // Timers are driven only by Input::Timeout; under a steady packet stream the read
        // never times out, so feed a due timeout explicitly.
        if Instant::now() >= timeout {
            rtc.handle_input(Input::Timeout(Instant::now())).map_err(|e| e.to_string())?;
        }
    }
}

/// The inbound Opus frames, in order. A hole left by a lost packet (str0m negotiates NACK
/// only for video, so audio loss is not retransmitted) is filled with zero-length frames:
/// per RFC 6716 §3.2.1 the decoder treats those as lost and conceals them, so the timeline
/// keeps its length instead of the voice skipping ahead.
#[derive(Default)]
struct Recorder {
    frames: Vec<Vec<u8>>,
    /// RTP time (48 kHz) the next frame is expected at.
    next_ts: Option<u64>,
    /// How many frames were filled in for lost packets.
    filled: u64,
}

impl Recorder {
    fn push(&mut self, ts: u64, frame: &[u8]) {
        let Some(&toc) = frame.first() else { return };
        // A code-0 TOC alone is one zero-length frame of this frame's duration.
        let filler = toc & 0xfc;
        let step = opus_frame_samples_48k(&[filler]).max(1);
        if let Some(expected) = self.next_ts {
            let missing = ts.saturating_sub(expected) / step;
            // Beyond a second of silence it's a dead link or a pause in the stream, not loss.
            if missing <= MAX_FILL_FRAMES {
                self.frames.extend((0..missing).map(|_| vec![filler]));
                self.filled += missing;
            }
        }
        self.next_ts = Some(ts + opus_frame_samples_48k(frame));
        self.frames.push(frame.to_vec());
    }
}

/// Mux the collected Opus frames into an Ogg/Opus container, or fail if the call produced
/// no audio.
fn finish(rec: Recorder, transcript: Option<String>) -> Result<(Vec<u8>, Option<String>), String> {
    if rec.frames.is_empty() {
        return Err("the call produced no audio".into());
    }
    Ok((build_ogg(&rec.frames)?, transcript))
}

/// The negotiated Opus payload type for outbound writes.
fn opus_pt(rtc: &Rtc) -> Option<Pt> {
    rtc.codec_config()
        .params()
        .iter()
        .find(|p| p.spec().codec == Codec::Opus)
        .map(|p| p.pt())
}

/// The default-route local IP: connect a throwaway UDP socket to a public address (sends
/// nothing) and read back which interface the OS would use.
fn routable_ip() -> Result<IpAddr, String> {
    let probe = UdpSocket::bind("0.0.0.0:0").map_err(|e| e.to_string())?;
    probe.connect("1.1.1.1:80").map_err(|e| format!("probe route: {e}"))?;
    Ok(probe.local_addr().map_err(|e| e.to_string())?.ip())
}

/// The oai-events data-channel messages we act on.
enum Oai {
    TurnDone(Option<String>),
    Error(String),
    Other,
}

fn parse_oai(bytes: &[u8]) -> Oai {
    let Ok(v) = serde_json::from_slice::<Value>(bytes) else {
        return Oai::Other;
    };
    match v.get("type").and_then(Value::as_str) {
        Some("turn.done") => Oai::TurnDone(
            v.get("turn")
                .and_then(|t| t.get("transcript"))
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string),
        ),
        Some("error") => Oai::Error(
            v.get("error")
                .and_then(|e| e.get("message"))
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
        ),
        _ => Oai::Other,
    }
}

/// Mux Opus frames into Ogg/Opus: OpusHead + OpusTags on their own pages, then data pages
/// whose granule position is the cumulative 48 kHz sample count.
fn build_ogg(frames: &[Vec<u8>]) -> Result<Vec<u8>, String> {
    use ogg::writing::{PacketWriteEndInfo, PacketWriter};

    let mut w = PacketWriter::new(Vec::new());
    let serial = 0x0a0b_0c0d;
    w.write_packet(opus_head(), serial, PacketWriteEndInfo::EndPage, 0)
        .map_err(|e| format!("ogg head: {e}"))?;
    w.write_packet(opus_tags(), serial, PacketWriteEndInfo::EndPage, 0)
        .map_err(|e| format!("ogg tags: {e}"))?;

    let mut granule = 0u64;
    let last = frames.len() - 1;
    for (i, frame) in frames.iter().enumerate() {
        granule += opus_frame_samples_48k(frame);
        let inf = if i == last {
            PacketWriteEndInfo::EndStream
        } else {
            PacketWriteEndInfo::NormalPacket
        };
        w.write_packet(frame.clone(), serial, inf, granule).map_err(|e| format!("ogg data: {e}"))?;
    }
    Ok(w.into_inner())
}

/// The 19-byte OpusHead identification header (mono, 48 kHz, no pre-skip since these are
/// captured mid-stream frames, not from an encoder whose lookahead we know).
fn opus_head() -> Vec<u8> {
    let mut h = Vec::with_capacity(19);
    h.extend_from_slice(b"OpusHead");
    h.push(1); // version
    h.push(1); // channel count (mono)
    h.extend_from_slice(&0u16.to_le_bytes()); // pre-skip
    h.extend_from_slice(&48_000u32.to_le_bytes()); // input sample rate
    h.extend_from_slice(&0i16.to_le_bytes()); // output gain
    h.push(0); // channel mapping family
    h
}

/// The OpusTags comment header (a vendor string, no user comments).
fn opus_tags() -> Vec<u8> {
    const VENDOR: &[u8] = b"octo-connector-speak";
    let mut t = Vec::with_capacity(8 + 4 + VENDOR.len() + 4);
    t.extend_from_slice(b"OpusTags");
    t.extend_from_slice(&(VENDOR.len() as u32).to_le_bytes());
    t.extend_from_slice(VENDOR);
    t.extend_from_slice(&0u32.to_le_bytes()); // user comment count
    t
}

/// The number of 48 kHz samples an Opus packet decodes to — parsed from its TOC byte(s)
/// (RFC 6716 §3.1), so the Ogg granule advances by the real duration of each frame.
fn opus_frame_samples_48k(pkt: &[u8]) -> u64 {
    let Some(&toc) = pkt.first() else { return 0 };
    let per_frame = match toc >> 3 {
        0 | 4 | 8 => 480,   // SILK NB/MB/WB 10 ms
        1 | 5 | 9 => 960,   // SILK NB/MB/WB 20 ms
        2 | 6 | 10 => 1920, // SILK NB/MB/WB 40 ms
        3 | 7 | 11 => 2880, // SILK NB/MB/WB 60 ms
        12 | 14 => 480,     // Hybrid SWB/FB 10 ms
        13 | 15 => 960,     // Hybrid SWB/FB 20 ms
        16 | 20 | 24 | 28 => 120, // CELT 2.5 ms
        17 | 21 | 25 | 29 => 240, // CELT 5 ms
        18 | 22 | 26 | 30 => 480, // CELT 10 ms
        _ => 960,           // CELT 20 ms (19/23/27/31)
    };
    let frames = match toc & 0x3 {
        0 => 1,
        1 | 2 => 2,
        _ => pkt.get(1).map(|b| (b & 0x3f) as u64).unwrap_or(1), // code 3: count in the next byte
    };
    per_frame * frames
}

#[cfg(test)]
mod tests {
    use super::{
        build_ogg, explain_http, opus_frame_samples_48k, parse_oai, session_payload, Oai, Recorder,
        SILENCE_FRAME,
    };

    #[test]
    fn frame_samples_are_parsed_from_the_toc() {
        // The silence frame is CELT fullband 20 ms, 1 frame -> 960 samples.
        assert_eq!(opus_frame_samples_48k(&SILENCE_FRAME), 960);
        assert_eq!(opus_frame_samples_48k(&[0x00]), 480); // SILK NB 10 ms
        assert_eq!(opus_frame_samples_48k(&[0x18]), 2880); // config 3 -> SILK NB 60 ms
        assert_eq!(opus_frame_samples_48k(&[0x01]), 960); // config 0 (480), count code 1 -> 2 frames
        assert_eq!(opus_frame_samples_48k(&[]), 0);
    }

    #[test]
    fn a_lost_packet_is_filled_with_a_concealable_frame() {
        let mut rec = Recorder::default();
        rec.push(0, &SILENCE_FRAME);
        rec.push(960 * 3, &SILENCE_FRAME); // the frames at 960 and 1920 were lost
        assert_eq!(rec.frames.len(), 4);
        assert_eq!(rec.frames[1], vec![0xf8]); // TOC only: a zero-length frame -> PLC
        assert_eq!(rec.filled, 2);
        // A hole longer than a second is a pause or a dead link, not loss: left as is.
        rec.push(960 * 4 + 960 * 100, &SILENCE_FRAME);
        assert_eq!(rec.frames.len(), 5);
    }

    #[test]
    fn session_names_the_voice_and_model() {
        let s = session_payload("hello", "cove");
        assert_eq!(s["audio"]["output"]["voice"], "cove");
        assert_eq!(s["delegation"]["type"], "client");
        assert_eq!(s["model"], super::MODEL);
        assert!(s["instructions"].as_str().unwrap().ends_with("TEXT:\nhello"));
    }

    #[test]
    fn oai_events_are_classified() {
        assert!(matches!(
            parse_oai(br#"{"type":"turn.done","turn":{"transcript":"hi"}}"#),
            Oai::TurnDone(Some(t)) if t == "hi"
        ));
        assert!(matches!(parse_oai(br#"{"type":"turn.done","turn":{}}"#), Oai::TurnDone(None)));
        assert!(matches!(parse_oai(br#"{"type":"error","error":{"message":"boom"}}"#), Oai::Error(e) if e == "boom"));
        assert!(matches!(parse_oai(b"not json"), Oai::Other));
    }

    #[test]
    fn http_errors_are_actionable() {
        assert!(explain_http(401, "").contains("sign in again"));
        assert!(explain_http(403, "Voice session access denied").contains("unknown voice"));
        assert!(explain_http(429, "").contains("usage window"));
    }

    #[test]
    fn ogg_has_the_opus_headers_and_all_frames() {
        // Two 20 ms silence frames -> a valid Ogg with OpusHead/OpusTags and 1920 samples.
        let ogg = build_ogg(&[SILENCE_FRAME.to_vec(), SILENCE_FRAME.to_vec()]).unwrap();
        assert_eq!(&ogg[0..4], b"OggS"); // first page capture pattern
        assert!(ogg.windows(8).any(|w| w == b"OpusHead"));
        assert!(ogg.windows(8).any(|w| w == b"OpusTags"));
    }

    /// LIVE: synthesize a real voice note against the ChatGPT Voice call endpoint.
    /// Ignored by default (hits the network, needs a real subscription auth.json, and — for
    /// media to flow — a publicly routable host, so it connects on the deploy but may not
    /// behind NAT):
    ///   SPEAK_TEXT="Hello" SPEAK_OUT=/tmp/speak.ogg \
    ///     cargo test -p octo-connector-speak live_speak -- --ignored --nocapture
    /// The token store defaults to $HOME/.codex/auth.json (override with ALBERT_AUTH_JSON).
    #[tokio::test]
    #[ignore = "hits chatgpt.com; needs a real subscription auth.json + a routable host"]
    async fn live_speak() {
        use super::synthesize;
        use octo_openai_auth::SubscriptionAuth;
        use std::path::PathBuf;

        let text = std::env::var("SPEAK_TEXT")
            .unwrap_or_else(|_| "Hi! This is Albert, speaking straight through the subscription.".into());
        let voice = std::env::var("SPEAK_VOICE").unwrap_or_else(|_| "cove".into());
        let out = std::env::var("SPEAK_OUT").unwrap_or_else(|_| "/tmp/speak.ogg".into());
        let auth_path = std::env::var("ALBERT_AUTH_JSON")
            .unwrap_or_else(|_| format!("{}/.codex/auth.json", std::env::var("HOME").expect("HOME")));

        let auth = SubscriptionAuth::new(PathBuf::from(auth_path));
        let sub = auth.fresh().await.expect("a fresh subscription token");
        let (ogg, transcript) =
            synthesize(&text, &voice, &sub).await.expect("the call synthesizes speech");
        std::fs::write(&out, &ogg).expect("write the ogg");
        println!("\n=== SPOKE {} bytes -> {out} (voice={voice}) ===", ogg.len());
        if let Some(t) = transcript {
            println!("[said] {t}");
        }
        assert!(ogg.len() > 512, "a real recording should be more than a page of Ogg");
    }
}
