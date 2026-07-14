//! Coalesced multimodal message payload.
//!
//! When a connector batches a burst of inbound messages into a single input (so
//! the cogitator reacts once rather than per-message), it emits an
//! [`InboundMessage`] carrying the combined text and any attached images. A lone
//! message still travels as a bare `String` or [`Blob`](super::Blob); this type
//! appears only for a genuine multi-part burst, so existing single-payload
//! consumers are unaffected.

use super::Blob;

/// A coalesced multimodal message: combined text plus any attached images.
#[derive(Debug, Clone, Default)]
pub struct InboundMessage {
    /// The combined text of the burst, if any.
    pub text: Option<String>,
    /// Attached images (e.g. photos), in arrival order.
    pub images: Vec<Blob>,
}

impl InboundMessage {
    pub fn new(text: Option<String>, images: Vec<Blob>) -> Self {
        Self { text, images }
    }

    /// A one-line summary for logs/history (does not include image bytes).
    pub fn summary(&self) -> String {
        match (&self.text, self.images.len()) {
            (Some(t), 0) => t.clone(),
            (Some(t), n) => format!("{t} [+{n} image(s)]"),
            (None, n) => format!("[{n} image(s)]"),
        }
    }
}
