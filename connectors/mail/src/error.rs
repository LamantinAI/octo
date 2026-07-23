//! The connector's error type — explicit variants, surfaced to the agent as a
//! `{ "error": "..." }` command result.

use thiserror::Error;

pub(crate) type Result<T> = std::result::Result<T, MailError>;

#[derive(Debug, Error)]
pub(crate) enum MailError {
    #[error("config: {0}")]
    Config(String),
    #[error("imap: {0}")]
    Imap(String),
    #[error("smtp: {0}")]
    Smtp(String),
}
