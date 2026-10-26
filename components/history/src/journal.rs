//! Strict journal contracts. Keys and request namespaces are opaque to this component.
use serde::{Deserialize, Serialize};

use crate::{HistoryError, Result, Turn};

pub const DEFAULT_PAGE_SIZE: usize = 10;
pub const MAX_PAGE_SIZE: usize = 100;

/// A stored row, including the original database timestamp (UTC SQLite text).
/// Raw history methods expose physical IDs; typed session methods expose IDs
/// scoped to their SessionJournal, including across RAM-to-SQL promotion.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JournalRecord {
    pub id: i64,
    pub created_at: String,
    pub turn: Turn,
}

/// Walk backwards through a fixed snapshot; returned records are oldest to newest.
/// Carry `through_id` and `next_before` into subsequent requests. IDs need not be contiguous.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PageRequest {
    pub before_id: Option<i64>,
    pub through_id: Option<i64>,
    pub limit: usize,
}

impl Default for PageRequest {
    fn default() -> Self {
        Self {
            before_id: None,
            through_id: None,
            limit: DEFAULT_PAGE_SIZE,
        }
    }
}

impl PageRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        if !(1..=MAX_PAGE_SIZE).contains(&self.limit)
            || self.before_id.is_some_and(|id| id <= 0)
            || self.through_id.is_some_and(|id| id < 0)
        {
            return Err(HistoryError::InvalidRequest(
                "invalid page limit or cursor".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JournalPage {
    pub records: Vec<JournalRecord>,
    pub next_before: Option<i64>,
    pub through_id: i64,
    pub has_more: bool,
}

/// Caller digest binds request semantics beyond the turns (for example attachment IDs).
/// The backend independently hashes the batch: reusing a digest with changed turns conflicts.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppendRequest {
    pub namespace: String,
    pub idempotency_key: String,
    pub digest: String,
    pub turns: Vec<Turn>,
}

/// Another verified native request identity belonging to the same accepted batch.
/// Its semantic digest is independent; record IDs and batch digest are shared.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestAlias {
    pub namespace: String,
    pub idempotency_key: String,
    pub digest: String,
}
impl RequestAlias {
    pub(crate) fn validate(&self) -> Result<()> {
        for (value, max) in [
            (&self.namespace, 128),
            (&self.idempotency_key, 256),
            (&self.digest, 256),
        ] {
            if value.is_empty() || value.len() > max {
                return Err(HistoryError::InvalidRequest("invalid request alias".into()));
            }
        }
        Ok(())
    }
}

impl AppendRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        for (value, max) in [
            (&self.namespace, 128),
            (&self.idempotency_key, 256),
            (&self.digest, 256),
        ] {
            if value.is_empty() || value.len() > max {
                return Err(HistoryError::InvalidRequest(
                    "empty or oversized request identity".into(),
                ));
            }
        }
        if self.turns.is_empty() {
            return Err(HistoryError::InvalidRequest("empty journal batch".into()));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendReceipt {
    pub record_ids: Vec<i64>,
    /// Last record in this batch, including on retry; not the current journal head.
    pub through_id: i64,
    pub duplicate: bool,
}

/// Content fingerprint is computed from all stored rows and references in one snapshot.
/// It detects changes between operator inventory and migration; it does not infer provenance.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalKeyInventory {
    pub key: String,
    pub turns: u64,
    pub compacts: u64,
    pub receipts: u64,
    pub first_id: Option<i64>,
    pub last_id: Option<i64>,
    pub fingerprint: String,
}

/// Trusted application code verifies legacy ownership and supplies the observed inventory.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KeyRemap {
    pub source: JournalKeyInventory,
    pub target: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RemapReceipt {
    pub source: String,
    pub target: String,
    pub turns: u64,
    pub compacts: u64,
    pub receipts: u64,
    pub duplicate: bool,
}
