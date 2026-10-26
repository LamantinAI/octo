use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// Native host feedback lifecycle. Never serialized or granted through chat text
/// or a tool payload. Clones share closure, so queued feedback cannot resurrect.
#[derive(Clone)]
pub struct TurnFeedback {
    id: String,
    open: Arc<AtomicBool>,
}

impl TurnFeedback {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            open: Arc::new(AtomicBool::new(true)),
        }
    }
    pub fn close(&self) {
        self.open.store(false, Ordering::Release);
    }
    pub fn is_open(&self) -> bool {
        self.open.load(Ordering::Acquire)
    }
    pub fn id(&self) -> &str {
        &self.id
    }
}

/// Host-only completion; the envelope must carry its native TurnFeedback token.
pub const TURN_END: &str = "chat.turn.end";
