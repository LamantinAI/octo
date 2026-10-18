use sqlx::Error as DriverError;
use thiserror::Error;

use crate::engine::EngineError;

#[derive(Debug, Error)]
pub enum SqlError {
    #[error("SQL authority rejected the request: {0}")]
    Authority(String),
    #[error("invalid SQL connector request: {0}")]
    Invalid(String),
    #[error("store revision changed; describe the store and prepare a new operation")]
    Stale,
    #[error("SQL connector limit reached: {0}")]
    Limit(String),
    #[error("SQL operation failed: {0}")]
    Engine(#[from] EngineError),
    #[error("SQL service persistence failed: {0}")]
    Persistence(#[from] DriverError),
    #[error("SQL connector worker failed")]
    Worker,
}
pub(crate) fn invalid(error: impl ToString) -> SqlError {
    SqlError::Invalid(error.to_string())
}
