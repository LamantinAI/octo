//! Recording: the ordered Opus frames (with lost packets filled for concealment) and the
//! Ogg/Opus mux they end up in — no decode/encode.

/// Fill at most one second of lost packets; a longer hole is a dead link or a pause.
pub(crate) const MAX_FILL_FRAMES: u64 = 50;

/// The inbound Opus frames, in order. A hole left by a lost packet (str0m negotiates NACK
/// only for video, so audio loss is not retransmitted) is filled with zero-length frames:
/// per RFC 6716 §3.2.1 the decoder treats those as lost and conceals them, so the timeline
/// keeps its length instead of the voice skipping ahead.
#[derive(Default)]
pub(crate) struct Recorder {
    pub(crate) frames: Vec<Vec<u8>>,
    /// RTP time (48 kHz) the next frame is expected at.
    pub(crate) next_ts: Option<u64>,
    /// How many frames were filled in for lost packets.
    pub(crate) filled: u64,
    /// Last RTP sequence number seen, and how many numbers were skipped over — the
    /// network-side loss count, to cross-check `filled` against.
    pub(crate) last_seq: Option<u64>,
    pub(crate) seq_lost: u64,
}

impl Recorder {
    pub(crate) fn push(&mut self, ts: u64, frame: &[u8]) {
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
pub(crate) fn finish(rec: Recorder, transcript: Option<String>) -> Result<(Vec<u8>, Option<String>), String> {
    if rec.frames.is_empty() {
        return Err("the call produced no audio".into());
    }
    Ok((build_ogg(&rec.frames)?, transcript))
}

/// Mux Opus frames into Ogg/Opus: OpusHead + OpusTags on their own pages, then data pages
/// whose granule position is the cumulative 48 kHz sample count.
pub(crate) fn build_ogg(frames: &[Vec<u8>]) -> Result<Vec<u8>, String> {
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
pub(crate) fn opus_head() -> Vec<u8> {
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
pub(crate) fn opus_tags() -> Vec<u8> {
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
pub(crate) fn opus_frame_samples_48k(pkt: &[u8]) -> u64 {
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
    use super::{build_ogg, opus_frame_samples_48k, Recorder};
    use crate::call::SILENCE_FRAME;

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
    fn ogg_has_the_opus_headers_and_all_frames() {
        // Two 20 ms silence frames -> a valid Ogg with OpusHead/OpusTags and 1920 samples.
        let ogg = build_ogg(&[SILENCE_FRAME.to_vec(), SILENCE_FRAME.to_vec()]).unwrap();
        assert_eq!(&ogg[0..4], b"OggS"); // first page capture pattern
        assert!(ogg.windows(8).any(|w| w == b"OpusHead"));
        assert!(ogg.windows(8).any(|w| w == b"OpusTags"));
    }
}
