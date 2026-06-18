//! Streaming support — multi-chunk envelopes correlated via `correlation_id`.
//!
//! A logical message that's bigger than one envelope (LLM token stream, audio
//! chunks, progressive image, large file) splits into a sequence of envelopes
//! sharing one `correlation_id`. Each carries a [`StreamFrame`] marker that
//! tells subscribers where in the sequence it sits.
//!
//! ## Convention
//!
//! - All chunks of one stream share `Envelope::correlation_id`.
//! - The first chunk carries `StreamFrame::Open`.
//! - Intermediate chunks carry `StreamFrame::Chunk`.
//! - The last chunk carries `StreamFrame::Close`.
//! - Aborted streams emit a single `StreamFrame::Cancel` envelope (the
//!   in-flight chunks before it are best-effort partial data; consumers
//!   should clean up).
//!
//! ## Use cases
//!
//! - **LLM cogitator → output connector.** LLM emits tokens as chunks; a
//!   bidirectional connector (e.g. Telegram) progressively edits the chat
//!   message via `editMessage` calls, replacing the partial text on each
//!   chunk and sending the final text on `Close`.
//! - **Streaming-source connector → memory writer.** Audio capture emits
//!   audio chunks; memory writer accumulates per stream and saves on `Close`.
//!
//! ## Subscriber strategies
//!
//! Two common shapes:
//! 1. **Collect-then-finalise**: buffer chunks per `correlation_id`, deliver
//!    only on `Close`. Suitable for sinks that don't support partial output.
//! 2. **Stream-as-it-arrives**: act on each chunk immediately (e.g. progressive
//!    `editMessage`). Suitable for sinks that support partial output.
//!
//! No special bus machinery is required for either — the protocol fits within
//! the existing concrete `Envelope` shape.

use serde::{Deserialize, Serialize};

/// Position of an envelope within a stream sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StreamFrame {
    /// First chunk of a stream. Establishes the stream identity via
    /// the envelope's `correlation_id`.
    Open,
    /// Middle chunk; more coming.
    Chunk,
    /// Final chunk; the stream is complete.
    Close,
    /// Stream aborted — partial data may have been sent; consumers should
    /// clean up state for this `correlation_id`.
    Cancel,
}

impl StreamFrame {
    /// `true` for `Close` and `Cancel` — the stream is done one way or another.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Close | Self::Cancel)
    }

    /// `true` for `Open` — the stream is just starting.
    pub fn is_initial(self) -> bool {
        matches!(self, Self::Open)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_predicates() {
        assert!(StreamFrame::Close.is_terminal());
        assert!(StreamFrame::Cancel.is_terminal());
        assert!(!StreamFrame::Open.is_terminal());
        assert!(!StreamFrame::Chunk.is_terminal());
    }

    #[test]
    fn initial_predicate() {
        assert!(StreamFrame::Open.is_initial());
        assert!(!StreamFrame::Chunk.is_initial());
        assert!(!StreamFrame::Close.is_initial());
    }
}
