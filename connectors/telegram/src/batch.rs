//! Signal-driven message coalescing.
//!
//! A single typed message reaches cognition **immediately** — no added latency.
//! Coalescing kicks in only when Telegram itself signals that messages belong
//! together and arrive as a back-to-back burst:
//!
//! - **Media albums** — every photo/video of an album shares a `media_group_id`.
//! - **Forwarded bursts** — forwarding several messages sends them as separate
//!   updates, each carrying `forward_origin`; there's no batch id, so a short
//!   quiet window groups them per chat.
//!
//! The connector decides the coalescing key (`mg:<id>` for an album, `fwd:<chat>`
//! for a forward burst, or `None` to emit immediately) and hands buffered parts
//! to the [`Batcher`], which flushes a key once it's been quiet for a (short)
//! debounce or hit a max-wait cap. Front-loading: cognition sees one input.
//!
//! The type is pure and deterministic — callers pass an explicit `now: Instant`
//! — so it is unit-tested without timers or a live Telegram stream.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use octo_core::{Blob, InboundMessage, TrustLevel};

use crate::acl::Role;

/// One buffered inbound part from a chat, in arrival order.
enum Part {
    Text(String),
    Image { blob: Blob, caption: Option<String> },
}

struct Pending {
    chat: String,
    parts: Vec<Part>,
    trust: Option<(Role, TrustLevel)>,
    first: Instant,
    last: Instant,
}

/// What to publish when a buffer is flushed.
pub(crate) enum Emit {
    /// Text (a lone message, or a coalesced text-only burst).
    Text { text: String, caption: Option<String> },
    /// A lone image.
    Image { blob: Blob, caption: Option<String> },
    /// A coalesced burst mixing text and/or multiple images.
    Multipart(InboundMessage),
}

/// A ready-to-publish flush for one chat.
pub(crate) struct Flush {
    pub chat: String,
    pub trust: Option<(Role, TrustLevel)>,
    pub emit: Emit,
}

/// Buffers coalescing bursts keyed by an opaque key and flushes them together.
pub(crate) struct Batcher {
    debounce: Duration,
    max_wait: Duration,
    /// Keyed by the connector's coalescing key (album id or forward-per-chat).
    pending: HashMap<String, Pending>,
}

impl Batcher {
    pub fn new(debounce: Duration, max_wait: Duration) -> Self {
        Self { debounce, max_wait, pending: HashMap::new() }
    }

    /// `false` when the debounce window is zero — coalescing disabled entirely.
    pub fn enabled(&self) -> bool {
        !self.debounce.is_zero()
    }

    pub fn push_text(
        &mut self,
        key: String,
        chat: &str,
        text: String,
        trust: Option<(Role, TrustLevel)>,
        now: Instant,
    ) {
        self.entry(key, chat, trust, now).parts.push(Part::Text(text));
    }

    pub fn push_image(
        &mut self,
        key: String,
        chat: &str,
        blob: Blob,
        caption: Option<String>,
        trust: Option<(Role, TrustLevel)>,
        now: Instant,
    ) {
        self.entry(key, chat, trust, now).parts.push(Part::Image { blob, caption });
    }

    fn entry(
        &mut self,
        key: String,
        chat: &str,
        trust: Option<(Role, TrustLevel)>,
        now: Instant,
    ) -> &mut Pending {
        let pending = self.pending.entry(key).or_insert_with(|| Pending {
            chat: chat.to_string(),
            parts: Vec::new(),
            trust,
            first: now,
            last: now,
        });
        pending.last = now;
        if pending.trust.is_none() {
            pending.trust = trust;
        }
        pending
    }

    /// Flush every buffer whose debounce window has elapsed, or that has been
    /// open longer than `max_wait` (so a steady stream still flushes).
    pub fn drain_due(&mut self, now: Instant) -> Vec<Flush> {
        let due: Vec<String> = self
            .pending
            .iter()
            .filter(|(_, p)| {
                now.duration_since(p.last) >= self.debounce
                    || now.duration_since(p.first) >= self.max_wait
            })
            .map(|(key, _)| key.clone())
            .collect();
        due.into_iter().filter_map(|key| self.pending.remove(&key).map(finish)).collect()
    }

    /// Flush everything (shutdown drain), order unspecified.
    pub fn drain_all(&mut self) -> Vec<Flush> {
        self.pending.drain().map(|(_, p)| finish(p)).collect()
    }
}

/// Convert a buffer's parts into an [`Emit`], preserving single-message shapes
/// and coalescing bursts.
fn finish(mut pending: Pending) -> Flush {
    let chat = pending.chat.clone();
    let trust = pending.trust;
    let emit = if pending.parts.len() == 1 {
        match pending.parts.pop().expect("len == 1") {
            Part::Text(text) => Emit::Text { text, caption: None },
            Part::Image { blob, caption } => Emit::Image { blob, caption },
        }
    } else {
        let mut texts: Vec<String> = Vec::new();
        let mut images: Vec<Blob> = Vec::new();
        for part in pending.parts {
            match part {
                Part::Text(t) => texts.push(t),
                Part::Image { blob, caption } => {
                    if let Some(c) = caption {
                        texts.push(c);
                    }
                    images.push(blob);
                }
            }
        }
        let text = if texts.is_empty() { None } else { Some(texts.join("\n")) };
        if images.is_empty() {
            // Text-only burst → a plain String, no payload-contract change.
            Emit::Text { text: text.unwrap_or_default(), caption: None }
        } else {
            Emit::Multipart(InboundMessage::new(text, images))
        }
    };
    Flush { chat, trust, emit }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEBOUNCE: Duration = Duration::from_millis(500);
    const MAX_WAIT: Duration = Duration::from_secs(3);

    fn batcher() -> Batcher {
        Batcher::new(DEBOUNCE, MAX_WAIT)
    }

    #[test]
    fn disabled_when_debounce_zero() {
        assert!(!Batcher::new(Duration::ZERO, MAX_WAIT).enabled());
        assert!(batcher().enabled());
    }

    #[test]
    fn forward_burst_coalesces_to_one_joined_string() {
        let mut b = batcher();
        let key = "fwd:42".to_string();
        let t0 = Instant::now();
        b.push_text(key.clone(), "42", "one".into(), None, t0);
        b.push_text(key.clone(), "42", "two".into(), None, t0 + Duration::from_millis(80));
        b.push_text(key.clone(), "42", "three".into(), None, t0 + Duration::from_millis(160));

        // Still within the debounce window — nothing due yet.
        assert!(b.drain_due(t0 + Duration::from_millis(200)).is_empty());

        let mut due = b.drain_due(t0 + Duration::from_millis(160) + DEBOUNCE);
        assert_eq!(due.len(), 1);
        let flush = due.pop().unwrap();
        assert_eq!(flush.chat, "42");
        match flush.emit {
            Emit::Text { text, .. } => assert_eq!(text, "one\ntwo\nthree"),
            _ => panic!("expected joined text"),
        }
    }

    #[test]
    fn album_coalesces_to_multipart() {
        let mut b = batcher();
        let key = "mg:abc".to_string();
        let t0 = Instant::now();
        b.push_image(key.clone(), "42", Blob::new(vec![1], "image/jpeg"), Some("cap".into()), None, t0);
        b.push_image(key.clone(), "42", Blob::new(vec![2], "image/jpeg"), None, None, t0 + Duration::from_millis(50));
        b.push_image(key.clone(), "42", Blob::new(vec![3], "image/jpeg"), None, None, t0 + Duration::from_millis(90));

        let mut due = b.drain_due(t0 + Duration::from_millis(90) + DEBOUNCE);
        match due.pop().unwrap().emit {
            Emit::Multipart(msg) => {
                assert_eq!(msg.images.len(), 3);
                assert_eq!(msg.text.as_deref(), Some("cap"));
            }
            _ => panic!("expected multipart"),
        }
    }

    #[test]
    fn lone_buffered_message_stays_single() {
        let mut b = batcher();
        let t0 = Instant::now();
        b.push_text("fwd:42".into(), "42", "hi".into(), None, t0);
        let mut due = b.drain_due(t0 + DEBOUNCE);
        assert!(matches!(due.pop().unwrap().emit, Emit::Text { .. }));
    }

    #[test]
    fn max_wait_flushes_a_steady_stream() {
        let mut b = batcher();
        let key = "fwd:42".to_string();
        let t0 = Instant::now();
        // A part every 300ms never lets the 500ms window elapse...
        for i in 0..20u32 {
            b.push_text(key.clone(), "42", format!("m{i}"), None, t0 + Duration::from_millis(300 * i as u64));
        }
        // ...but max_wait (3s) since `first` forces a flush.
        assert_eq!(b.drain_due(t0 + MAX_WAIT).len(), 1);
    }

    #[test]
    fn distinct_keys_are_independent() {
        let mut b = batcher();
        let t0 = Instant::now();
        b.push_text("fwd:a".into(), "a", "x".into(), None, t0);
        b.push_text("mg:g".into(), "b", "y".into(), None, t0 + Duration::from_millis(300));
        // At t0+DEBOUNCE, the first key is due but the second (newer) is not.
        let due = b.drain_due(t0 + DEBOUNCE);
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].chat, "a");
    }
}
