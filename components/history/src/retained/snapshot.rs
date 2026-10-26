use serde_json::from_slice;

use super::{RetainedMemoryHistory, State};
use crate::snapshot::{SnapshotJournal, SnapshotPayload, SnapshotReceipt, batch_digest};
use crate::{
    DiscardReceipt, HistoryError, Result, SessionId, SessionJournal, SessionSnapshot, Turn,
};

impl RetainedMemoryHistory {
    fn export_locked(state: &State, id: &SessionId) -> Result<SessionSnapshot> {
        let entry = state
            .sessions
            .get(id)
            .ok_or(HistoryError::SessionNotFound)?;
        let mut journals = Vec::new();
        for (key, typed) in &state.typed_journals {
            if typed.session_id() != id {
                continue;
            }
            let journal = state
                .journals
                .get(key)
                .ok_or(HistoryError::SessionConflict)?;
            let run_id = match typed {
                SessionJournal::Root(_) => String::new(),
                SessionJournal::Child { run_id, .. } => run_id.clone(),
            };
            let mut receipts = Vec::new();
            for ((namespace, idempotency_key), receipt) in &journal.receipts {
                let batch: Vec<Turn> = from_slice(&receipt.batch)?;
                receipts.push(SnapshotReceipt {
                    namespace: namespace.clone(),
                    idempotency_key: idempotency_key.clone(),
                    digest: receipt.digest.clone(),
                    batch_digest: batch_digest(&batch)?,
                    record_ids: receipt.receipt.record_ids.clone(),
                });
            }
            receipts.sort_by(|a, b| {
                (&a.namespace, &a.idempotency_key).cmp(&(&b.namespace, &b.idempotency_key))
            });
            journals.push(SnapshotJournal {
                run_id,
                records: journal.records.clone(),
                compacts: journal.compacts.clone(),
                receipts,
            });
        }
        journals.sort_by(|a, b| a.run_id.cmp(&b.run_id));
        SessionSnapshot::new(SnapshotPayload {
            session: entry.session.clone(),
            creation: entry.creation.clone(),
            journals,
        })
    }

    /// Atomic owned export under the RAM-store lock. The assembly must also hold its
    /// per-session writer guard across export, SQL commit, router switch and discard.
    /// Only private temporary sessions are eligible; export never removes RAM data.
    pub fn export_session(&self, id: &SessionId) -> Result<SessionSnapshot> {
        Self::export_locked(&*self.lock()?, id)
    }

    /// Remove exactly the exported temporary scope. A late append or metadata update
    /// changes its fingerprint and fails this comparison without deleting anything.
    pub fn discard_session(
        &self,
        id: &SessionId,
        expected_snapshot_fingerprint: &str,
    ) -> Result<DiscardReceipt> {
        let mut state = self.lock()?;
        let snapshot = Self::export_locked(&state, id)?;
        if snapshot.fingerprint() != expected_snapshot_fingerprint {
            return Err(HistoryError::SessionConflict);
        }
        let keys: Vec<_> = state
            .typed_journals
            .iter()
            .filter(|(_, journal)| journal.session_id() == id)
            .map(|(key, _)| key.clone())
            .collect();
        let defaults: Vec<_> = state
            .defaults
            .iter()
            .filter(|(_, session)| *session == id)
            .map(|(origin, _)| origin.clone())
            .collect();
        let entry = state.sessions.remove(id).expect("validated session");
        let mut receipt = DiscardReceipt {
            session_id: id.clone(),
            journals: keys.len(),
            records: 0,
            compacts: 0,
            receipts: 0,
            bytes: entry.bytes,
        };
        for key in keys {
            state.typed_journals.remove(&key);
            let journal = state.journals.remove(&key).expect("validated journal");
            receipt.records += journal.records.len();
            receipt.compacts += journal.compacts.len();
            receipt.receipts += journal.receipts.len();
            receipt.bytes += journal.bytes;
        }
        for origin in defaults {
            state.defaults.remove(&origin);
            receipt.bytes += origin.connector.len() + origin.channel.len() + id.as_str().len();
        }
        state.bytes -= receipt.bytes;
        state.turns -= receipt.records;
        Ok(receipt)
    }
}
