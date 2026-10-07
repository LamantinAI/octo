//! Per-speaker conversation state that outlives a single HTTP request.
//!
//! Alice waits ~3 s for an answer, an agent turn usually takes longer. So the
//! request that started a turn often returns a filler, and the reply — arriving
//! later on the bus — is either spoken by the speaker on its own (when the cloud
//! voice is configured: the turn is *handed over to push*) or parked here until
//! the speaker says «дальше». Parked replies are split into pieces, one per
//! request.

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

use octo_core::EventId;
use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::speech;

#[derive(Default)]
struct Dialog {
    /// The turn we are waiting on: the id of the `chat.message` that started
    /// it (the assembly correlates its `chat.reply` to it) and when it started.
    pending: Option<(EventId, Instant)>,
    /// The request that started the turn has already answered with a filler:
    /// its reply goes to the speaker's own voice instead of the queue.
    push: bool,
    /// Spoken pieces not yet delivered, oldest first.
    queue: VecDeque<String>,
    /// Woken whenever something lands in `queue`.
    notify: Arc<Notify>,
}

/// Where an arriving reply should go.
#[derive(Debug, PartialEq, Eq)]
pub enum Delivery {
    /// Parked; a waiting or later request will speak it.
    Queued,
    /// Say it through the speaker's cloud voice now.
    Push(String),
}

pub struct Dialogs {
    map: Mutex<HashMap<String, Dialog>>,
    /// A turn not answered within this window is treated as lost (an
    /// interrupted turn sends no reply) — stop saying «ещё думаю».
    turn_ttl: Duration,
    /// Longest piece a webhook answer carries.
    max_chars: usize,
}

impl Dialogs {
    pub fn new(turn_ttl: Duration, max_chars: usize) -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
            turn_ttl,
            max_chars,
        }
    }

    /// The wake-up handle for `channel`. Callers `enable()` a `notified()`
    /// future *before* starting a turn, so a reply racing in is not missed.
    pub fn notify(&self, channel: &str) -> Arc<Notify> {
        self.map
            .lock()
            .entry(channel.to_string())
            .or_default()
            .notify
            .clone()
    }

    /// A new request went to the agent: anything still queued belongs to the
    /// previous exchange and is dropped.
    pub fn start_turn(&self, channel: &str, id: EventId) {
        let mut map = self.map.lock();
        let dialog = map.entry(channel.to_string()).or_default();
        dialog.pending = Some((id, Instant::now()));
        dialog.push = false;
        dialog.queue.clear();
    }

    /// The request ran out of time: take what is already parked, or — when
    /// nothing is — hand the turn over to push. Atomic with [`deliver`], so a
    /// reply racing in is either returned here or pushed, never lost.
    pub fn take_or_hand_over(&self, channel: &str) -> Option<(String, bool)> {
        let mut map = self.map.lock();
        let dialog = map.entry(channel.to_string()).or_default();
        match dialog.queue.pop_front() {
            Some(piece) => Some((piece, !dialog.queue.is_empty())),
            None => {
                dialog.push = true;
                None
            }
        }
    }

    /// A reply arrived (already converted to speech). It is pushed when its
    /// turn was handed over, or when it is unsolicited (a reminder) and
    /// `push_available`; otherwise it is chunked and parked. A reply to the
    /// pending turn closes it.
    pub fn deliver(
        &self,
        channel: &str,
        correlation: Option<EventId>,
        spoken: String,
        push_available: bool,
    ) -> Delivery {
        let mut map = self.map.lock();
        let dialog = map.entry(channel.to_string()).or_default();
        let is_pending = correlation.is_some() && dialog.pending.map(|(id, _)| id) == correlation;
        if is_pending {
            dialog.pending = None;
        }
        let push = push_available && ((is_pending && dialog.push) || correlation.is_none());
        if is_pending {
            dialog.push = false;
        }
        if push {
            return Delivery::Push(spoken);
        }
        dialog.queue.extend(speech::chunk(&spoken, self.max_chars));
        dialog.notify.notify_waiters();
        Delivery::Queued
    }

    /// Park text whose push failed, so «дальше» / the next launch still has it.
    pub fn park(&self, channel: &str, spoken: &str) {
        let mut map = self.map.lock();
        let dialog = map.entry(channel.to_string()).or_default();
        dialog.queue.extend(speech::chunk(spoken, self.max_chars));
        dialog.notify.notify_waiters();
    }

    /// The next piece to say, and whether more are queued after it.
    pub fn next_piece(&self, channel: &str) -> Option<(String, bool)> {
        let mut map = self.map.lock();
        let dialog = map.get_mut(channel)?;
        let piece = dialog.queue.pop_front()?;
        Some((piece, !dialog.queue.is_empty()))
    }

    /// Everything still parked, as one text (for handing to push).
    pub fn drain(&self, channel: &str) -> String {
        let mut map = self.map.lock();
        map.get_mut(channel)
            .map(|d| d.queue.drain(..).collect::<Vec<_>>().join(" "))
            .unwrap_or_default()
    }

    pub fn has_queued(&self, channel: &str) -> bool {
        self.map
            .lock()
            .get(channel)
            .is_some_and(|d| !d.queue.is_empty())
    }

    /// Is a turn still running (started, unanswered, not yet given up on)?
    pub fn is_thinking(&self, channel: &str) -> bool {
        let mut map = self.map.lock();
        let Some(dialog) = map.get_mut(channel) else {
            return false;
        };
        match dialog.pending {
            Some((_, since)) if since.elapsed() < self.turn_ttl => true,
            Some(_) => {
                dialog.pending = None;
                false
            }
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dialogs(ttl_ms: u64) -> Dialogs {
        Dialogs::new(Duration::from_millis(ttl_ms), 20)
    }

    #[test]
    fn correlated_reply_closes_turn_and_queues_pieces() {
        let d = dialogs(60_000);
        let id = EventId::new();
        d.start_turn("c", id);
        assert!(d.is_thinking("c"));
        let r = d.deliver(
            "c",
            Some(id),
            "Раз два три. Четыре пять шесть.".into(),
            true,
        );
        assert_eq!(
            r,
            Delivery::Queued,
            "not handed over → parked even with push"
        );
        assert!(!d.is_thinking("c"));
        assert_eq!(d.next_piece("c"), Some(("Раз два три.".into(), true)));
        assert_eq!(
            d.next_piece("c"),
            Some(("Четыре пять шесть.".into(), false))
        );
        assert_eq!(d.next_piece("c"), None);
    }

    #[test]
    fn handed_over_turn_is_pushed_and_race_is_returned_instead() {
        let d = dialogs(60_000);
        let id = EventId::new();
        d.start_turn("c", id);
        assert_eq!(d.take_or_hand_over("c"), None);
        assert_eq!(
            d.deliver("c", Some(id), "Ответ.".into(), true),
            Delivery::Push("Ответ.".into())
        );
        assert!(!d.has_queued("c"));

        // Reply landed before the request gave up: it is returned, not pushed.
        let id = EventId::new();
        d.start_turn("c", id);
        assert_eq!(
            d.deliver("c", Some(id), "Успел.".into(), true),
            Delivery::Queued
        );
        assert_eq!(d.take_or_hand_over("c"), Some(("Успел.".into(), false)));
    }

    #[test]
    fn unsolicited_reply_is_pushed_only_when_available() {
        let d = dialogs(60_000);
        assert_eq!(
            d.deliver("c", None, "Напоминание.".into(), true),
            Delivery::Push("Напоминание.".into())
        );
        assert_eq!(
            d.deliver("c", None, "Напоминание.".into(), false),
            Delivery::Queued
        );
        assert!(d.has_queued("c"));
        assert_eq!(d.drain("c"), "Напоминание.");
        assert!(!d.has_queued("c"));
    }

    #[test]
    fn handed_over_without_push_parks() {
        let d = dialogs(60_000);
        let id = EventId::new();
        d.start_turn("c", id);
        d.take_or_hand_over("c");
        assert_eq!(
            d.deliver("c", Some(id), "x".into(), false),
            Delivery::Queued
        );
        d.park("c", "y");
        assert_eq!(d.drain("c"), "x y");
    }

    #[test]
    fn new_turn_drops_stale_queue_and_ttl_expires() {
        let d = dialogs(0);
        d.park("c", "старое");
        d.start_turn("c", EventId::new());
        assert!(!d.has_queued("c"));
        assert!(
            !d.is_thinking("c"),
            "zero ttl → the turn is already given up"
        );
    }

    #[tokio::test]
    async fn enabled_waiter_sees_reply_delivered_before_await() {
        let d = dialogs(60_000);
        let notify = d.notify("c");
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        d.park("c", "x");
        tokio::time::timeout(Duration::from_millis(50), notified)
            .await
            .expect("wake-up must not be lost");
    }
}
