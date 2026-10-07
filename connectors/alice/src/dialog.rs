//! Per-speaker conversation state that outlives a single HTTP request.
//!
//! Alice waits ~3 s for an answer, an agent turn usually takes longer. So the
//! request that started a turn often returns a placeholder, and the reply —
//! arriving later on the bus — is parked here until the speaker says «дальше».
//! Long replies are split into pieces and handed out one per request.

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

use octo_core::EventId;
use parking_lot::Mutex;
use tokio::sync::Notify;

#[derive(Default)]
struct Dialog {
    /// The turn we are waiting on: the id of the `chat.message` that started
    /// it (the assembly correlates its `chat.reply` to it) and when it started.
    pending: Option<(EventId, Instant)>,
    /// Spoken pieces not yet delivered, oldest first.
    queue: VecDeque<String>,
    /// Woken whenever something lands in `queue`.
    notify: Arc<Notify>,
}

pub struct Dialogs {
    map: Mutex<HashMap<String, Dialog>>,
    /// A turn not answered within this window is treated as lost (an
    /// interrupted turn sends no reply) — stop saying «ещё думаю».
    turn_ttl: Duration,
}

impl Dialogs {
    pub fn new(turn_ttl: Duration) -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
            turn_ttl,
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
        dialog.queue.clear();
    }

    /// A reply arrived. A reply to the pending turn closes it; an uncorrelated
    /// one (a reminder firing) is queued all the same.
    pub fn deliver(&self, channel: &str, correlation: Option<EventId>, pieces: Vec<String>) {
        let mut map = self.map.lock();
        let dialog = map.entry(channel.to_string()).or_default();
        if correlation.is_some() && dialog.pending.map(|(id, _)| id) == correlation {
            dialog.pending = None;
        }
        dialog.queue.extend(pieces);
        dialog.notify.notify_waiters();
    }

    /// The next piece to say, and whether more are queued after it.
    pub fn next_piece(&self, channel: &str) -> Option<(String, bool)> {
        let mut map = self.map.lock();
        let dialog = map.get_mut(channel)?;
        let piece = dialog.queue.pop_front()?;
        Some((piece, !dialog.queue.is_empty()))
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

    #[test]
    fn correlated_reply_closes_turn_and_queues_pieces() {
        let d = Dialogs::new(Duration::from_secs(60));
        let id = EventId::new();
        d.start_turn("c", id);
        assert!(d.is_thinking("c"));
        d.deliver("c", Some(id), vec!["раз".into(), "два".into()]);
        assert!(!d.is_thinking("c"));
        assert_eq!(d.next_piece("c"), Some(("раз".into(), true)));
        assert_eq!(d.next_piece("c"), Some(("два".into(), false)));
        assert_eq!(d.next_piece("c"), None);
    }

    #[test]
    fn foreign_reply_queues_but_keeps_turn_open() {
        let d = Dialogs::new(Duration::from_secs(60));
        d.start_turn("c", EventId::new());
        d.deliver("c", None, vec!["напоминание".into()]);
        assert!(d.is_thinking("c"));
        assert!(d.has_queued("c"));
    }

    #[test]
    fn new_turn_drops_stale_queue_and_ttl_expires() {
        let d = Dialogs::new(Duration::from_millis(0));
        d.deliver("c", None, vec!["старое".into()]);
        d.start_turn("c", EventId::new());
        assert!(!d.has_queued("c"));
        assert!(
            !d.is_thinking("c"),
            "zero ttl → the turn is already given up"
        );
    }

    #[tokio::test]
    async fn enabled_waiter_sees_reply_delivered_before_await() {
        let d = Dialogs::new(Duration::from_secs(60));
        let notify = d.notify("c");
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        d.deliver("c", None, vec!["x".into()]);
        tokio::time::timeout(Duration::from_millis(50), notified)
            .await
            .expect("wake-up must not be lost");
    }
}
