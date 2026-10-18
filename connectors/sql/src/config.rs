use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::{
    engine::{NativeLimits, Settings},
    error::SqlError,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    pub max_stores: usize,
    pub max_rows: usize,
    pub max_bytes: usize,
    pub max_cell_bytes: usize,
    pub max_affected_rows: u64,
    pub timeout_ms: u64,
    pub max_pending: usize,
    pub approval_ttl_seconds: u64,
    pub native: NativeLimits,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_stores: 20,
            max_rows: 200,
            max_bytes: 262_144,
            max_cell_bytes: 65_536,
            max_affected_rows: 1000,
            timeout_ms: 5000,
            max_pending: 32,
            approval_ttl_seconds: 300,
            native: NativeLimits::default(),
        }
    }
}
impl Limits {
    pub(crate) fn validate(&self) -> Result<(), SqlError> {
        if !(1..=20).contains(&self.max_stores)
            || self.max_rows == 0
            || self.max_rows > 10_000
            || self.max_bytes == 0
            || self.max_bytes > 16_777_216
            || self.max_cell_bytes == 0
            || self.max_cell_bytes > self.max_bytes
            || self.max_affected_rows == 0
            || self.timeout_ms == 0
            || self.timeout_ms > 60_000
            || self.max_pending == 0
            || self.max_pending > 1000
            || self.approval_ttl_seconds == 0
            || self.approval_ttl_seconds > 3600
        {
            return Err(SqlError::Invalid(
                "invalid configured limits (max_stores cannot exceed 20)".into(),
            ));
        }
        self.native.validate()?;
        Ok(())
    }
    pub(crate) fn engine(&self) -> Settings {
        Settings {
            max_rows: self.max_rows,
            max_bytes: self.max_bytes,
            max_cell_bytes: self.max_cell_bytes,
            max_affected_rows: self.max_affected_rows,
            timeout: Duration::from_millis(self.timeout_ms),
            native: self.native.clone(),
        }
    }
}
