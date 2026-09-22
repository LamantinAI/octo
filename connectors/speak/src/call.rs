//! The call itself: the SDP offer/answer POST to ChatGPT Voice, then the sans-IO loop that
//! pumps the outbound "microphone" and records the inbound Opus frames.

use std::{
    io::ErrorKind,
    net::{SocketAddr, UdpSocket},
    sync::Once,
    time::{Duration, Instant},
};

use octo_openai_auth::Subscription;
use serde_json::Value;
use str0m::{
    change::SdpAnswer,
    format::Codec,
    media::{Direction, Frequency, MediaKind, MediaTime, Mid, Pt},
    Candidate, Event, IceConnectionState, Input, Output, Rtc, RtcConfig,
};
use tracing::{debug, info, warn};

use crate::{
    net::{feed, routable_ip, rtp_seq, NetStats},
    ogg::{finish, Recorder, MAX_FILL_FRAMES},
    signal::{post_call, CallError},
};

/// One 20 ms Opus frame at 48 kHz is 960 samples — the outbound-silence and granule step.
pub(crate) const SAMPLES_PER_FRAME: u64 = 960;

/// A minimal valid 20 ms Opus frame (CELT fullband, near-silence). The call wants a
/// "microphone"; its content is irrelevant to a TTS turn, only the RTP flow matters.
pub(crate) const SILENCE_FRAME: [u8; 3] = [0xf8, 0xff, 0xfe];

/// Give the WebRTC connection this long to produce the first audio frame.
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);

/// Hard ceiling on one recording, in case `turn.done` never lands.
pub(crate) const TURN_TIMEOUT: Duration = Duration::from_secs(180);

/// Keep receiving this long after `turn.done`, so the audio tail lands.
pub(crate) const TAIL_GRACE: Duration = Duration::from_millis(1000);

/// Audio packets str0m waits for a reordered one before releasing a gap (default 15).
pub(crate) const REORDER_PACKETS: usize = 25;

/// A second RTP packet this far (or further) BEHIND the first is a sequence restart, not a
/// reorder — see [`drive_call`]'s primer handling.
pub(crate) const RESTART_MIN_BACKSTEP: u16 = 100;

/// str0m installs a process-wide crypto provider (for DTLS/SRTP); do it once.
pub(crate) static CRYPTO: Once = Once::new();
pub(crate) fn install_crypto() {
    CRYPTO.call_once(|| str0m::crypto::from_feature_flags().install_process_default());
}

/// Run one WebRTC call end-to-end: build the offer, POST it, accept the answer, then drive
/// the sans-IO loop (off the async runtime) collecting Opus frames into Ogg.
pub(crate) async fn synthesize(
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
pub(crate) fn drive_call(
    mut rtc: Rtc,
    socket: UdpSocket,
    local: SocketAddr,
    mid: Mid,
) -> Result<(Vec<u8>, Option<String>), String> {
    let opus_pt = opus_pt(&rtc).ok_or("no Opus payload type negotiated")?;

    let mut rec = Recorder::default();
    let mut net = NetStats::default();
    let mut transcript: Option<String> = None;
    let mut connected = false;
    let mut stop_at: Option<Instant> = None;

    let start = Instant::now();
    let mut out_ts: u64 = 0;
    let mut next_silence = start;
    let mut buf = vec![0u8; 2000];
    // The peer opens the audio with a lone "primer" packet (3 bytes of silence), then
    // restarts the stream on the SAME SSRC with a fresh sequence number. When the restart
    // lands numerically behind the primer, str0m's receive register reads every real packet
    // as an old duplicate and drops the whole utterance. So the first RTP packet is held
    // until the second shows which way the sequence went, and a primer the peer abandoned
    // never reaches str0m. (RTP headers are not encrypted under SRTP, so this is readable
    // before str0m.) A forward restart is left alone: str0m accepts it, and dropping the
    // primer there would desync the SRTP rollover counter.
    let mut primer: Option<(u16, Vec<u8>, SocketAddr)> = None;
    let mut rtp_started = false;

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
                Output::Event(Event::MediaData(m)) => {
                    let (first, last) = (**m.seq_range.start(), **m.seq_range.end());
                    debug!(
                        at_ms = start.elapsed().as_millis() as u64,
                        seq = first,
                        ts = m.time.numer(),
                        bytes = m.data.len(),
                        contiguous = m.contiguous,
                        "speak: frame"
                    );
                    // A jump past a second of packets is the peer restarting its sequence
                    // (see the primer handling), not loss.
                    let skipped = rec.last_seq.map_or(0, |prev| first.saturating_sub(prev + 1));
                    if skipped <= MAX_FILL_FRAMES {
                        rec.seq_lost += skipped;
                    }
                    rec.last_seq = Some(last);
                    rec.push(m.time.numer(), &m.data);
                }
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
            info!(
                frames = rec.frames.len(),
                filled = rec.filled,
                seq_lost = rec.seq_lost,
                "speak: turn.done"
            );
            info!(?net, "speak: network");
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
                    match writer.write(opus_pt, now, rtp, SILENCE_FRAME.to_vec()) {
                        Ok(()) => net.mic_sent += 1,
                        Err(e) => {
                            if net.mic_failed == 0 {
                                warn!(error = %e, "speak: writing the outbound silence failed");
                            }
                            net.mic_failed += 1;
                        }
                    }
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
                let datagram = &buf[..n];
                net.count(datagram, source);
                match rtp_seq(datagram) {
                    Some(seq) if !rtp_started => match primer.take() {
                        None => primer = Some((seq, datagram.to_vec(), source)),
                        Some((held_seq, held, held_source)) => {
                            rtp_started = true;
                            if (RESTART_MIN_BACKSTEP..0x8000).contains(&held_seq.wrapping_sub(seq)) {
                                debug!(held_seq, seq, "speak: dropping the stream primer; the peer restarted its sequence");
                            } else {
                                feed(&mut rtc, &held, held_source, local)?;
                            }
                            feed(&mut rtc, datagram, source, local)?;
                        }
                    },
                    _ => feed(&mut rtc, datagram, source, local)?,
                }
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

/// The negotiated Opus payload type for outbound writes.
pub(crate) fn opus_pt(rtc: &Rtc) -> Option<Pt> {
    rtc.codec_config()
        .params()
        .iter()
        .find(|p| p.spec().codec == Codec::Opus)
        .map(|p| p.pt())
}

/// The oai-events data-channel messages we act on.
pub(crate) enum Oai {
    TurnDone(Option<String>),
    Error(String),
    Other,
}

pub(crate) fn parse_oai(bytes: &[u8]) -> Oai {
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

#[cfg(test)]
mod tests {
    use super::{parse_oai, Oai};

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
        use tracing_subscriber::{fmt, EnvFilter};

        // RUST_LOG=octo_connector_speak=debug shows every received frame.
        let _ = fmt().with_env_filter(EnvFilter::from_default_env()).with_test_writer().try_init();

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
