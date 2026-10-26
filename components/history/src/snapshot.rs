//! Owned in-process exports. There is deliberately no public serializer or payload Debug.
use std::collections::{BTreeMap, HashSet};

use chrono::NaiveDateTime;
use serde::Serialize;
use serde_json::{from_str, to_vec};
use sha2::{Digest, Sha256};

use crate::{
    AppendRequest, CreateSession, HistoryError, JournalRecord, Result, Session, SessionAccess,
    SessionId, SessionJournal, SessionKind, Turn,
};

#[derive(Clone)]
pub struct SessionSnapshot {
    pub(crate) payload: SnapshotPayload,
    fingerprint: String,
}
#[derive(Clone, Serialize)]
pub(crate) struct SnapshotPayload {
    pub session: Session,
    pub creation: String,
    pub journals: Vec<SnapshotJournal>,
}
#[derive(Clone, Serialize)]
pub(crate) struct SnapshotJournal {
    pub run_id: String,
    pub records: Vec<JournalRecord>,
    pub compacts: Vec<SnapshotCompact>,
    pub receipts: Vec<SnapshotReceipt>,
}
#[derive(Clone, Serialize)]
pub(crate) struct SnapshotCompact {
    pub id: i64,
    pub previous_id: Option<i64>,
    pub through_id: i64,
    pub content: String,
    pub created_at: String,
}
#[derive(Clone, Serialize)]
pub(crate) struct SnapshotReceipt {
    pub namespace: String,
    pub idempotency_key: String,
    pub digest: String,
    pub batch_digest: String,
    pub record_ids: Vec<i64>,
}

pub(crate) fn batch_digest(turns: &[Turn]) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(to_vec(turns)?)))
}
fn valid_timestamp(value: &str) -> bool {
    NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f").is_ok()
}

impl SessionSnapshot {
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }
    pub fn session(&self) -> &Session {
        &self.payload.session
    }

    pub(crate) fn new(payload: SnapshotPayload) -> Result<Self> {
        let fingerprint = format!("{:x}", Sha256::digest(to_vec(&payload)?));
        let snapshot = Self {
            payload,
            fingerprint,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        let invalid = || HistoryError::InvalidRequest("invalid session snapshot".into());
        if self.fingerprint != format!("{:x}", Sha256::digest(to_vec(&self.payload)?)) {
            return Err(invalid());
        }
        let session = &self.payload.session;
        let creation: CreateSession = from_str(&self.payload.creation)?;
        creation.validate()?;
        if session.kind != SessionKind::Temporary
            || !matches!(session.access, SessionAccess::Private { .. })
            || session.revision < 1
            || !valid_timestamp(&session.created_at)
            || creation.id.as_ref() != Some(&session.id)
            || creation.access != session.access
            || creation.kind != SessionKind::Temporary
            || creation.host != session.host
        {
            return Err(invalid());
        }
        let mut roots = 0;
        let mut runs = HashSet::new();
        let mut previous_run: Option<&str> = None;
        for journal in &self.payload.journals {
            if !runs.insert(&journal.run_id)
                || previous_run.is_some_and(|run| run >= journal.run_id.as_str())
            {
                return Err(invalid());
            }
            previous_run = Some(&journal.run_id);
            if journal.run_id.is_empty() {
                roots += 1;
            } else {
                SessionJournal::Child {
                    session_id: session.id.clone(),
                    run_id: journal.run_id.clone(),
                }
                .storage_key()?;
            }
            let mut records = BTreeMap::new();
            let mut last = 0;
            for record in &journal.records {
                if record.id <= last || !valid_timestamp(&record.created_at) {
                    return Err(invalid());
                }
                last = record.id;
                records.insert(record.id, &record.turn);
            }
            let mut previous = None;
            let mut boundary = 0;
            for compact in &journal.compacts {
                if compact.id <= previous.unwrap_or(0)
                    || compact.previous_id != previous
                    || compact.through_id < boundary
                    || !records.contains_key(&compact.through_id)
                    || compact.content.trim().is_empty()
                    || !valid_timestamp(&compact.created_at)
                {
                    return Err(invalid());
                }
                previous = Some(compact.id);
                boundary = compact.through_id;
            }
            let mut receipt_keys = HashSet::new();
            let mut linked = BTreeMap::new();
            let mut previous_key = None;
            for receipt in &journal.receipts {
                let key = (&receipt.namespace, &receipt.idempotency_key);
                if !receipt_keys.insert(key) || previous_key.is_some_and(|previous| previous >= key)
                {
                    return Err(invalid());
                }
                previous_key = Some(key);
                let mut batch = Vec::new();
                let mut last_id = 0;
                for id in &receipt.record_ids {
                    if *id <= last_id {
                        return Err(invalid());
                    }
                    if let Some((ids, digest)) = linked.get(id) {
                        if *ids != &receipt.record_ids || *digest != &receipt.batch_digest {
                            return Err(invalid());
                        }
                    } else {
                        linked.insert(*id, (&receipt.record_ids, &receipt.batch_digest));
                    }
                    batch.push((**records.get(id).ok_or_else(invalid)?).clone());
                    last_id = *id;
                }
                AppendRequest {
                    namespace: receipt.namespace.clone(),
                    idempotency_key: receipt.idempotency_key.clone(),
                    digest: receipt.digest.clone(),
                    turns: batch.clone(),
                }
                .validate()?;
                if batch_digest(&batch)? != receipt.batch_digest {
                    return Err(invalid());
                }
            }
        }
        if roots != 1 {
            return Err(invalid());
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct PromotionReceipt {
    pub session_id: SessionId,
    pub fingerprint: String,
    pub source_revision: i64,
    /// Durable revision immediately after promotion, including on a later exact retry.
    pub revision: i64,
    pub duplicate: bool,
}
#[derive(Clone, Debug)]
pub struct DiscardReceipt {
    pub session_id: SessionId,
    pub journals: usize,
    pub records: usize,
    pub compacts: usize,
    pub receipts: usize,
    pub bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::SessionSnapshot;
    use crate::{
        AppendRequest, CreateSession, RetainedMemoryHistory, SessionJournal, SessionKind,
        SessionStore, Turn,
    };

    #[tokio::test]
    async fn snapshot_validation_rejects_foreign_receipts_broken_chains_and_timestamps() {
        let ram = RetainedMemoryHistory::default();
        let mut create = CreateSession::private("owner");
        create.kind = SessionKind::Temporary;
        let session = ram.create_session(create).await.unwrap();
        let root = SessionJournal::Root(session.id.clone());
        let receipt = ram
            .append_session(
                &root,
                &AppendRequest {
                    namespace: "tg".into(),
                    idempotency_key: "1".into(),
                    digest: "digest".into(),
                    turns: vec![Turn::user("data")],
                },
            )
            .await
            .unwrap();
        ram.save_session_compact(&root, None, receipt.through_id, "summary")
            .await
            .unwrap();
        let snapshot = ram.export_session(&session.id).unwrap();
        let mut foreign = snapshot.payload.clone();
        foreign.journals[0].receipts[0].record_ids[0] = 999;
        assert!(SessionSnapshot::new(foreign).is_err());
        let mut broken = snapshot.payload.clone();
        broken.journals[0].compacts[0].previous_id = Some(999);
        assert!(SessionSnapshot::new(broken).is_err());
        let mut invalid_time = snapshot.payload.clone();
        invalid_time.journals[0].records[0].created_at = "invented".into();
        assert!(SessionSnapshot::new(invalid_time).is_err());
        let mut changed = snapshot.clone();
        changed.payload.session.title = "changed after fingerprint".into();
        assert!(changed.validate().is_err());
    }
}
