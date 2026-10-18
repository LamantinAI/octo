use std::{
    collections::BTreeSet,
    io::Error as IoError,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use sqlx::Error as SqlxError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("SQL construct is unsupported: {0}")]
    Unsupported(String),
    #[error("SQL policy denied access: {0}")]
    Denied(String),
    #[error("destructive operation needs a verified one-time approval")]
    ApprovalRequired,
    #[error("operation exceeded a configured limit: {0}")]
    Limit(String),
    #[error("operation cancelled or deadline exceeded")]
    Cancelled,
    #[error("execution requires a caller-owned transaction")]
    TransactionRequired,
    #[error("connection does not belong to this connector file")]
    WrongDatabase,
    #[error("SQLite error: {0}")]
    Sqlite(String),
    #[error(transparent)]
    Driver(#[from] SqlxError),
    #[error(transparent)]
    Io(#[from] IoError),
}

#[derive(Debug, Clone)]
pub struct Settings {
    pub max_rows: usize,
    pub max_bytes: usize,
    pub max_cell_bytes: usize,
    pub max_affected_rows: u64,
    pub timeout: Duration,
    pub native: NativeLimits,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            max_rows: 200,
            max_bytes: 262_144,
            max_cell_bytes: 65_536,
            max_affected_rows: 1000,
            timeout: Duration::from_secs(5),
            native: NativeLimits::default(),
        }
    }
}

/// Configurable native resource limits. ATTACH remains a fixed security denial.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NativeLimits {
    pub max_sql_bytes: usize,
    pub max_columns: i32,
    pub max_expression_depth: i32,
    pub max_trigger_depth: i32,
    pub max_parameters: i32,
    pub max_like_pattern_bytes: i32,
}
impl Default for NativeLimits {
    fn default() -> Self {
        Self {
            max_sql_bytes: 65_536,
            max_columns: 512,
            max_expression_depth: 64,
            max_trigger_depth: 32,
            max_parameters: 512,
            max_like_pattern_bytes: 1024,
        }
    }
}
impl NativeLimits {
    pub fn validate(&self) -> Result<(), EngineError> {
        if !(1..=1_048_576).contains(&self.max_sql_bytes)
            || !(1..=2000).contains(&self.max_columns)
            || !(1..=1000).contains(&self.max_expression_depth)
            || !(1..=1000).contains(&self.max_trigger_depth)
            || !(1..=32766).contains(&self.max_parameters)
            || !(1..=50_000).contains(&self.max_like_pattern_bytes)
        {
            return Err(EngineError::Limit(
                "invalid native resource settings".into(),
            ));
        }
        Ok(())
    }
}

/// Constructed by the trusted catalog from a generated store ID, never SQL input.
#[derive(Debug, Clone)]
pub struct Policy {
    pub namespace: String,
    pub read: bool,
    pub write: bool,
    pub schema: bool,
}
impl Policy {
    pub fn validate(&self) -> Result<(), EngineError> {
        if !self.namespace.starts_with("s_")
            || !self.namespace.ends_with("__")
            || self.namespace.len() < 6
            || !self
                .namespace
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'_')
        {
            return Err(EngineError::Denied("invalid generated namespace".into()));
        }
        Ok(())
    }
    pub(super) fn owns(&self, name: &str) -> bool {
        name.to_ascii_lowercase()
            .starts_with(&self.namespace.to_ascii_lowercase())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Analysis {
    pub objects: BTreeSet<String>,
    pub functions: BTreeSet<String>,
    pub reads: bool,
    pub writes: bool,
    pub schema_changes: bool,
    pub requires_approval: bool,
    pub reasons: BTreeSet<String>,
    /// Tables whose implicit row deletion/rewrite is counted under the same lock.
    pub counted_tables: BTreeSet<String>,
    /// Direct UPDATE targets with a structurally scoped predicate.
    pub scoped_updates: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum SqlParam {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<SqlParam>>,
    /// Native row effects plus pre-counted implicit DROP/column-rewrite rows.
    pub affected_rows: u64,
    pub truncated: bool,
}

/// Trusted executor-only capability; deliberately not deserializable.
#[derive(Debug, Clone, Copy)]
pub struct ExecutionPermit {
    pub(super) approved: bool,
}
impl ExecutionPermit {
    pub fn ordinary() -> Self {
        Self { approved: false }
    }
    /// Only the connector's executor may mint this after consuming a bound grant.
    pub(crate) fn approved() -> Self {
        Self { approved: true }
    }
}

#[derive(Debug, Clone)]
pub struct Cancellation {
    flags: Vec<Arc<AtomicBool>>,
    deadline: Option<Instant>,
}
impl Default for Cancellation {
    fn default() -> Self {
        Self {
            flags: vec![Arc::new(AtomicBool::new(false))],
            deadline: None,
        }
    }
}
impl Cancellation {
    pub fn cancel(&self) {
        for flag in &self.flags {
            flag.store(true, Ordering::Release);
        }
    }
    pub fn is_cancelled(&self) -> bool {
        self.flags.iter().any(|flag| flag.load(Ordering::Acquire))
            || self.deadline.is_some_and(|end| Instant::now() >= end)
    }
    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.deadline
    }
    pub(crate) fn deadline_elapsed(&self) -> bool {
        self.deadline.is_some_and(|end| Instant::now() >= end)
    }
    pub(crate) fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(
            self.deadline
                .map(|old| old.min(deadline))
                .unwrap_or(deadline),
        );
        self
    }
    pub(crate) fn combined(mut self, other: &Self) -> Self {
        self.flags.extend(other.flags.iter().cloned());
        if let Some(deadline) = other.deadline {
            self = self.with_deadline(deadline);
        }
        self
    }
}
