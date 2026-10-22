//! Telling the voice's speech from its silence, so a call ends on what was actually said.
//!
//! The call normally ends on `turn.done`, sent right after the last spoken frame. A session
//! can also connect and then say nothing at all: it streams the 3-byte silence frame (and
//! never `turn.done`) until the call's ceiling, and the "voice note" is minutes of nothing.
//! Speech frames are tens to hundreds of bytes, and a spoken turn carries no silence frames
//! between its words — so a run of silence after speech is its end, and silence with no
//! speech at all is a failed turn, not a recording.

use std::time::{Duration, Instant};

use str0m::channel::ChannelId;

use crate::ogg::is_speech;

/// Fail the turn if this long passes after the first frame with no speech in it. A started
/// turn talks within a second; this leaves room for [`MAX_TURN_STARTS`] attempts.
pub(crate) const NO_SPEECH_TIMEOUT: Duration = Duration::from_secs(20);

/// Re-send the start if the voice is still silent this long after the last one. Measured
/// live, a start is often acknowledged and then not answered; a later one is, and an
/// answered start is heard within a second.
pub(crate) const RESTART_TURN_AFTER: Duration = Duration::from_secs(3);

/// How many times the turn is started before the call is left to the no-speech timeout.
pub(crate) const MAX_TURN_STARTS: u32 = 4;

/// After speech, this much silence without `turn.done` ends the recording.
pub(crate) const END_OF_SPEECH_SILENCE: Duration = Duration::from_secs(3);

/// Silence frames kept after the last spoken one when the tail is trimmed (300 ms).
pub(crate) const TAIL_KEEP_FRAMES: usize = 15;

/// What the inbound audio says about the turn so far.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// Still talking, or not started yet but within the grace.
    Listening,
    /// Spoke, then went quiet for [`END_OF_SPEECH_SILENCE`].
    SpeechEnded,
    /// Audio has flowed for [`NO_SPEECH_TIMEOUT`] without a single spoken frame.
    NoSpeech,
}

/// When the first frame arrived and when the last spoken one did.
#[derive(Default)]
pub(crate) struct SpeechWatch {
    first_audio: Option<Instant>,
    last_speech: Option<Instant>,
}

impl SpeechWatch {
    pub(crate) fn observe(&mut self, frame: &[u8], now: Instant) {
        self.first_audio.get_or_insert(now);
        if is_speech(frame) {
            self.last_speech = Some(now);
        }
    }

    pub(crate) fn heard_speech(&self) -> bool {
        self.last_speech.is_some()
    }

    pub(crate) fn verdict(&self, now: Instant) -> Verdict {
        match (self.last_speech, self.first_audio) {
            (Some(at), _) if now.duration_since(at) >= END_OF_SPEECH_SILENCE => {
                Verdict::SpeechEnded
            }
            (None, Some(at)) if now.duration_since(at) >= NO_SPEECH_TIMEOUT => Verdict::NoSpeech,
            _ => Verdict::Listening,
        }
    }
}

/// When to send the start-turn event: as soon as the events channel opens, then again
/// every [`RESTART_TURN_AFTER`] while the voice is silent, at most [`MAX_TURN_STARTS`]
/// times. The caller stops asking once speech is heard.
pub(crate) struct TurnStart<C: Copy = ChannelId> {
    pub(crate) channel: Option<C>,
    pub(crate) starts: u32,
    last: Option<Instant>,
}

impl<C: Copy> Default for TurnStart<C> {
    fn default() -> Self {
        Self {
            channel: None,
            starts: 0,
            last: None,
        }
    }
}

impl<C: Copy> TurnStart<C> {
    /// The channel to send the start on, if one is due now.
    pub(crate) fn due(&self, now: Instant) -> Option<C> {
        let channel = self.channel?;
        if self.starts >= MAX_TURN_STARTS {
            return None;
        }
        self.last
            .is_none_or(|at| now.duration_since(at) >= RESTART_TURN_AFTER)
            .then_some(channel)
    }

    pub(crate) fn sent(&mut self, now: Instant) {
        self.starts += 1;
        self.last = Some(now);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SILENT: [u8; 3] = [0xd8, 0xff, 0xfe];
    const SPOKEN: [u8; 4] = [0xd8, 0x01, 0x02, 0x03];

    #[test]
    fn the_turn_starts_at_once_and_restarts_while_silent() {
        let t0 = Instant::now();
        let mut turn = TurnStart::<u8>::default();
        assert_eq!(turn.due(t0), None, "no channel yet");
        turn.channel = Some(7);
        assert_eq!(turn.due(t0), Some(7));
        turn.sent(t0);
        assert_eq!(turn.due(t0 + Duration::from_secs(1)), None);
        assert_eq!(turn.due(t0 + RESTART_TURN_AFTER), Some(7));
        for k in 1..MAX_TURN_STARTS {
            turn.sent(t0 + RESTART_TURN_AFTER * k);
        }
        assert_eq!(turn.due(t0 + RESTART_TURN_AFTER * 10), None, "capped");
    }

    #[test]
    fn a_quiet_start_is_not_yet_a_failure() {
        let t0 = Instant::now();
        let mut watch = SpeechWatch::default();
        watch.observe(&SILENT, t0);
        assert_eq!(
            watch.verdict(t0 + Duration::from_secs(1)),
            Verdict::Listening
        );
    }

    #[test]
    fn silence_with_no_speech_fails_the_turn() {
        let t0 = Instant::now();
        let mut watch = SpeechWatch::default();
        for ms in (0..16_000).step_by(20) {
            watch.observe(&SILENT, t0 + Duration::from_millis(ms));
        }
        assert!(!watch.heard_speech());
        assert_eq!(watch.verdict(t0 + NO_SPEECH_TIMEOUT), Verdict::NoSpeech);
    }

    #[test]
    fn silence_after_speech_ends_the_recording() {
        let t0 = Instant::now();
        let mut watch = SpeechWatch::default();
        watch.observe(&SPOKEN, t0);
        watch.observe(&SILENT, t0 + Duration::from_secs(1));
        assert_eq!(
            watch.verdict(t0 + Duration::from_secs(2)),
            Verdict::Listening
        );
        assert_eq!(
            watch.verdict(t0 + END_OF_SPEECH_SILENCE),
            Verdict::SpeechEnded
        );
    }

    #[test]
    fn speech_keeps_the_call_open_past_the_no_speech_grace() {
        let t0 = Instant::now();
        let mut watch = SpeechWatch::default();
        watch.observe(&SILENT, t0);
        let late = t0 + NO_SPEECH_TIMEOUT - Duration::from_secs(1);
        watch.observe(&SPOKEN, late);
        assert_eq!(
            watch.verdict(late + Duration::from_secs(2)),
            Verdict::Listening
        );
    }
}
