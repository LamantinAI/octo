//! Per-subscriber bounded queue — the backbone of in-process backpressure.
//!
//! The [`InProcessBus`](crate::bus::InProcessBus) keeps one [`SubscriberInner`]
//! per live subscription and **fans out on publish**: each matching subscriber's
//! [`push`](SubscriberInner::push) applies that subscriber's
//! [`BackpressureStrategy`] when its buffer is full, so a slow consumer can drop
//! (lossy) or block the publisher (lossless) *independently* of every other
//! subscriber. This is the "per-subscriber mpsc fan-out" the old broadcast-based
//! bus only gestured at.
//!
//! Free slots are tracked by a [`tokio::sync::Semaphore`] (permits = free space),
//! which gives correct, fair queueing for multiple concurrent publishers under
//! `Block` with no lost-wakeup races; queued envelopes live in a `VecDeque`, and
//! item-availability is signalled to the single consumer via a [`Notify`].
//!
//! Invariant: `buf.len() == capacity - space.available_permits()` at every
//! lock-release boundary.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::{Notify, Semaphore};

use crate::bus::Filter;
use crate::connector::BackpressureStrategy;
use crate::Envelope;

/// Outcome of a [`SubscriberInner::push`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PushOutcome {
    /// Enqueued for the subscriber.
    Delivered,
    /// Buffer was full; an envelope was dropped per the strategy.
    Dropped,
    /// The subscriber is gone (its `Subscription` was dropped); prune it.
    Closed,
}

/// One live subscription's intake. Shared (`Arc`) between the bus registry (which
/// pushes) and the [`Subscription`](crate::bus::Subscription) (which pops).
pub(crate) struct SubscriberInner {
    /// Slot identity — for log correlation and registry pruning.
    pub(crate) id: u64,
    /// The subscriber's declarative filter. Applied at publish time so a `Block`
    /// subscriber never stalls a publisher on envelopes it never wanted.
    pub(crate) filter: Filter,
    /// Effective strategy (`Throttle`/`Steer` are downgraded to `DropOldest`
    /// before construction — see `InProcessBus::register`).
    strategy: BackpressureStrategy,
    capacity: usize,
    buf: Mutex<VecDeque<Arc<Envelope>>>,
    /// Permits = free slots. Acquired on enqueue, released on dequeue.
    space: Semaphore,
    /// Count of envelopes dropped for this subscriber (lag visibility).
    dropped: AtomicU64,
    /// Set when the `Subscription` is dropped (or the bus closes).
    closed: AtomicBool,
    /// Wakes the consumer when an envelope is enqueued (or on close).
    item_ready: Notify,
}

impl SubscriberInner {
    pub(crate) fn new(
        id: u64,
        filter: Filter,
        strategy: BackpressureStrategy,
        capacity: usize,
    ) -> Arc<Self> {
        let capacity = capacity.max(1);
        Arc::new(Self {
            id,
            filter,
            strategy,
            capacity,
            buf: Mutex::new(VecDeque::with_capacity(capacity)),
            space: Semaphore::new(capacity),
            dropped: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            item_ready: Notify::new(),
        })
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub(crate) fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Enqueue one envelope, honoring the subscriber's backpressure strategy.
    /// `Block` awaits a free slot (and so blocks the publisher); the lossy
    /// strategies never await.
    pub(crate) async fn push(&self, env: Arc<Envelope>) -> PushOutcome {
        if self.is_closed() {
            return PushOutcome::Closed;
        }
        match self.strategy {
            // Lossless: wait for a free slot. This is the only path that blocks
            // the publishing task — intended for delivery-critical subscribers.
            BackpressureStrategy::Block => match self.space.acquire().await {
                Ok(permit) => {
                    permit.forget(); // slot is now occupied; released by `pop`.
                    if self.is_closed() {
                        // Lost the race with close; undo the reservation.
                        self.space.add_permits(1);
                        return PushOutcome::Closed;
                    }
                    self.buf.lock().push_back(env);
                    self.item_ready.notify_one();
                    PushOutcome::Delivered
                }
                Err(_) => PushOutcome::Closed, // semaphore closed → subscription gone
            },
            // Drop the arriving envelope when full.
            BackpressureStrategy::DropNewest => match self.space.try_acquire() {
                Ok(permit) => {
                    permit.forget();
                    self.buf.lock().push_back(env);
                    self.item_ready.notify_one();
                    PushOutcome::Delivered
                }
                Err(tokio::sync::TryAcquireError::NoPermits) => {
                    self.record_drop();
                    PushOutcome::Dropped
                }
                Err(tokio::sync::TryAcquireError::Closed) => PushOutcome::Closed,
            },
            // DropOldest (and downgraded Throttle/Steer): evict oldest to make
            // room, then enqueue newest. Loop because a concurrent consumer or
            // peer publisher may grab the freed slot first.
            _ => loop {
                match self.space.try_acquire() {
                    Ok(permit) => {
                        permit.forget();
                        self.buf.lock().push_back(env);
                        self.item_ready.notify_one();
                        return PushOutcome::Delivered;
                    }
                    Err(tokio::sync::TryAcquireError::NoPermits) => {
                        // Free a slot by dropping the oldest, then retry acquire.
                        let evicted = self.buf.lock().pop_front().is_some();
                        if evicted {
                            self.space.add_permits(1);
                            self.record_drop();
                        }
                        // If nothing was there to evict, a consumer just drained
                        // it and released a permit; the retry will succeed.
                    }
                    Err(tokio::sync::TryAcquireError::Closed) => return PushOutcome::Closed,
                }
            },
        }
    }

    /// Dequeue the next envelope, awaiting one if the buffer is empty. Returns
    /// `None` once the subscription is closed and drained.
    pub(crate) async fn pop(&self) -> Option<Arc<Envelope>> {
        loop {
            if let Some(env) = self.buf.lock().pop_front() {
                self.space.add_permits(1); // freed a slot
                return Some(env);
            }
            if self.is_closed() {
                return None;
            }
            // Empty and open: wait for an enqueue (or a close wakeup). A
            // notify_one issued between the pop attempt and here is retained as a
            // permit, so this can't miss the wakeup.
            self.item_ready.notified().await;
        }
    }

    /// Mark closed and wake both sides: a parked consumer returns `None`, and a
    /// blocked `Block` publisher's `acquire()` resolves to `Err` → `Closed`.
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.space.close();
        self.item_ready.notify_one();
    }

    fn record_drop(&self) {
        let n = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
        // Throttle the log: first drop, then at each power of two.
        if n == 1 || n.is_power_of_two() {
            tracing::warn!(
                subscriber = self.id,
                strategy = ?self.strategy,
                dropped = n,
                capacity = self.capacity,
                "bus subscriber lagging; dropping envelopes"
            );
        }
    }
}
