//! Error types for octo-core.
//!
//! Single error enum for all core operations; framework users `?` through it.

use thiserror::Error;

use crate::Lifecycle;

pub type OctoResult<T> = Result<T, OctoError>;

#[derive(Debug, Error)]
pub enum OctoError {
    #[error("bus error: {0}")]
    Bus(String),

    #[error("connector error: {0}")]
    Connector(String),

    #[error("channel error: {0}")]
    Channel(String),

    #[error("invalid lifecycle transition: {from} -> {to}")]
    LifecycleTransition { from: Lifecycle, to: Lifecycle },

    #[error("subscription dropped or closed")]
    SubscriptionClosed,

    #[error("operation cancelled")]
    Cancelled,

    #[error("response timeout (correlation_id = {correlation_id})")]
    Timeout { correlation_id: crate::EventId },

    #[error("bus closed before response arrived")]
    BusClosed,

    #[error("payload validation: {0}")]
    PayloadValidation(#[from] crate::envelope::RegistryError),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Json(#[from] serde_json::Error),

    #[error("other: {0}")]
    Other(Box<dyn std::error::Error + Send + Sync>),
}

impl OctoError {
    pub fn other<E: std::error::Error + Send + Sync + 'static>(e: E) -> Self {
        Self::Other(Box::new(e))
    }
}
