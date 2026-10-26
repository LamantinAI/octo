//! Retained, quota-bounded RAM storage. A receipt is volatile, never disk durability.
mod aliases;
mod sessions;
mod snapshot;

use std::{
    collections::HashMap,
    sync::{Mutex, MutexGuard},
};

use async_trait::async_trait;
use chrono::Utc;
use serde_json::to_vec;

use crate::snapshot::SnapshotCompact;
use crate::{
    AppendReceipt, AppendRequest, Compact, ContextWindow, HistoryError, HistoryStore, JournalPage,
    JournalRecord, PageRequest, Result, Session, SessionId, SessionJournal, SessionOrigin,
    StoredTurn, Turn,
};

#[derive(Clone, Debug)]
pub struct MemoryLimits {
    pub max_sessions: usize,
    pub max_turns: usize,
    /// Limit on retained serialized payload bytes, not an allocator-level heap limit.
    pub max_bytes: usize,
}
impl Default for MemoryLimits {
    fn default() -> Self {
        Self {
            max_sessions: 1000,
            max_turns: 100_000,
            max_bytes: 16 * 1024 * 1024,
        }
    }
}
pub struct RetainedMemoryHistory {
    state: Mutex<State>,
    limits: MemoryLimits,
}
#[derive(Default)]
struct State {
    sessions: HashMap<SessionId, CatalogEntry>,
    defaults: HashMap<SessionOrigin, SessionId>,
    journals: HashMap<String, Journal>,
    typed_journals: HashMap<String, SessionJournal>,
    next_turn: i64,
    next_compact: i64,
    next_catalog: i64,
    bytes: usize,
    turns: usize,
}
struct CatalogEntry {
    session: Session,
    sequence: i64,
    creation: String,
    bytes: usize,
}
#[derive(Default)]
struct Journal {
    records: Vec<JournalRecord>,
    compacts: Vec<SnapshotCompact>,
    receipts: HashMap<(String, String), Receipt>,
    bytes: usize,
}
struct Receipt {
    digest: String,
    batch: Vec<u8>,
    receipt: AppendReceipt,
}

pub(super) fn timestamp() -> String {
    Utc::now().format("%Y-%m-%d %H:%M:%S%.f").to_string()
}

impl RetainedMemoryHistory {
    pub fn new(limits: MemoryLimits) -> Result<Self> {
        if limits.max_sessions == 0 || limits.max_turns == 0 || limits.max_bytes == 0 {
            return Err(HistoryError::InvalidRequest(
                "memory limits must be positive".into(),
            ));
        }
        Ok(Self {
            state: Mutex::new(State::default()),
            limits,
        })
    }
    fn lock(&self) -> Result<MutexGuard<'_, State>> {
        self.state
            .lock()
            .map_err(|_| HistoryError::Db("retained memory lock poisoned".into()))
    }
    fn reserve(&self, state: &mut State, bytes: usize, turns: usize) -> Result<()> {
        let next_bytes = state
            .bytes
            .checked_add(bytes)
            .ok_or(HistoryError::QuotaExceeded)?;
        let next_turns = state
            .turns
            .checked_add(turns)
            .ok_or(HistoryError::QuotaExceeded)?;
        if next_bytes > self.limits.max_bytes || next_turns > self.limits.max_turns {
            return Err(HistoryError::QuotaExceeded);
        }
        state.bytes = next_bytes;
        state.turns = next_turns;
        Ok(())
    }
    fn append_locked(
        &self,
        state: &mut State,
        key: &str,
        turns: &[Turn],
        request: Option<&AppendRequest>,
    ) -> Result<AppendReceipt> {
        if key.is_empty() || key.len() > 4096 || turns.is_empty() {
            return Err(HistoryError::InvalidRequest(
                "invalid retained journal batch".into(),
            ));
        }
        let batch = to_vec(turns)?;
        if let Some(input) = request {
            input.validate()?;
            if let Some(stored) = state.journals.get(key).and_then(|j| {
                j.receipts
                    .get(&(input.namespace.clone(), input.idempotency_key.clone()))
            }) {
                if stored.digest != input.digest || stored.batch != batch {
                    return Err(HistoryError::IdempotencyConflict);
                }
                let mut receipt = stored.receipt.clone();
                receipt.duplicate = true;
                return Ok(receipt);
            }
        }
        let created_at = timestamp();
        let records: Vec<_> = turns
            .iter()
            .enumerate()
            .map(|(index, turn)| JournalRecord {
                id: state.next_turn + index as i64 + 1,
                created_at: created_at.clone(),
                turn: turn.clone(),
            })
            .collect();
        let receipt = AppendReceipt {
            record_ids: records.iter().map(|r| r.id).collect(),
            through_id: records.last().expect("nonempty batch").id,
            duplicate: false,
        };
        let mut bytes = to_vec(&records)?.len()
            + if state.journals.contains_key(key) {
                0
            } else {
                key.len()
            };
        if let Some(input) = request {
            bytes += batch.len()
                + input.namespace.len()
                + input.idempotency_key.len()
                + input.digest.len()
                + to_vec(&receipt)?.len();
        }
        self.reserve(state, bytes, records.len())?;
        state.next_turn += records.len() as i64;
        let journal = state.journals.entry(key.into()).or_default();
        journal.bytes += bytes;
        journal.records.extend(records);
        if let Some(input) = request {
            journal.receipts.insert(
                (input.namespace.clone(), input.idempotency_key.clone()),
                Receipt {
                    digest: input.digest.clone(),
                    batch,
                    receipt: receipt.clone(),
                },
            );
        }
        Ok(receipt)
    }
    fn page_locked(&self, state: &State, key: &str, request: PageRequest) -> Result<JournalPage> {
        request.validate()?;
        let journal = state.journals.get(key);
        let head = journal
            .and_then(|j| j.records.last())
            .map(|r| r.id)
            .unwrap_or(0);
        let through_id = request.through_id.unwrap_or(head);
        if through_id > head {
            return Err(HistoryError::InvalidRequest(
                "watermark exceeds journal head".into(),
            ));
        }
        let mut records = journal
            .into_iter()
            .flat_map(|j| j.records.iter().rev())
            .filter(|row| {
                row.id <= through_id && request.before_id.is_none_or(|before| row.id < before)
            })
            .take(request.limit + 1)
            .cloned()
            .collect::<Vec<_>>();
        let has_more = records.len() > request.limit;
        records.truncate(request.limit);
        let next_before = has_more.then(|| records.last().expect("nonempty page").id);
        records.reverse();
        Ok(JournalPage {
            records,
            next_before,
            through_id,
            has_more,
        })
    }
    fn context_locked(state: &State, key: &str) -> ContextWindow {
        let Some(journal) = state.journals.get(key) else {
            return ContextWindow::default();
        };
        let compact = journal.compacts.last().map(|c| Compact {
            id: c.id,
            through_id: c.through_id,
            content: c.content.clone(),
        });
        let boundary = compact.as_ref().map(|c| c.through_id).unwrap_or(0);
        ContextWindow {
            compact,
            messages: journal
                .records
                .iter()
                .filter(|r| r.id > boundary)
                .map(|r| StoredTurn {
                    id: r.id,
                    turn: r.turn.clone(),
                })
                .collect(),
        }
    }
    fn compact_locked(
        &self,
        state: &mut State,
        key: &str,
        previous_id: Option<i64>,
        through_id: i64,
        content: &str,
    ) -> Result<bool> {
        if content.trim().is_empty() {
            return Err(HistoryError::InvalidRequest("empty compact".into()));
        }
        let Some(journal) = state.journals.get(key) else {
            return Ok(false);
        };
        let previous = journal.compacts.last();
        if previous.map(|c| c.id) != previous_id
            || previous.is_some_and(|c| through_id < c.through_id)
            || !journal.records.iter().any(|r| r.id == through_id)
        {
            return Ok(false);
        }
        let compact = SnapshotCompact {
            id: state.next_compact + 1,
            previous_id,
            through_id,
            content: content.into(),
            created_at: timestamp(),
        };
        let bytes = to_vec(&compact)?.len();
        self.reserve(state, bytes, 0)?;
        state.next_compact += 1;
        let journal = state.journals.get_mut(key).expect("existing journal");
        journal.bytes += bytes;
        journal.compacts.push(compact);
        Ok(true)
    }
}
impl Default for RetainedMemoryHistory {
    fn default() -> Self {
        Self::new(MemoryLimits::default()).expect("valid default limits")
    }
}

#[async_trait]
impl HistoryStore for RetainedMemoryHistory {
    fn retains_context(&self) -> bool {
        true
    }
    async fn load(&self, key: &str) -> Vec<Turn> {
        // Compatibility surface only; session callers use strict load_session.
        self.state
            .lock()
            .ok()
            .and_then(|s| {
                s.journals
                    .get(key)
                    .map(|j| j.records.iter().map(|r| r.turn.clone()).collect())
            })
            .unwrap_or_default()
    }
    async fn append(&self, key: &str, turns: &[Turn]) -> Result<()> {
        self.append_locked(&mut *self.lock()?, key, turns, None)
            .map(|_| ())
    }
    async fn journal_page(&self, key: &str, request: PageRequest) -> Result<JournalPage> {
        self.page_locked(&*self.lock()?, key, request)
    }
    async fn append_journal(&self, key: &str, request: &AppendRequest) -> Result<AppendReceipt> {
        self.append_locked(&mut *self.lock()?, key, &request.turns, Some(request))
    }
    async fn context(&self, key: &str) -> Result<ContextWindow> {
        Ok(Self::context_locked(&*self.lock()?, key))
    }
    async fn save_compact(
        &self,
        key: &str,
        previous_id: Option<i64>,
        through_id: i64,
        content: &str,
    ) -> Result<bool> {
        self.compact_locked(&mut *self.lock()?, key, previous_id, through_id, content)
    }
}
