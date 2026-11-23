//! Narrow owner transfer inside an application-owned transaction.
use sqlx::{Acquire, Sqlite, Transaction, query, query_as};

use super::{SqliteHistory, db};
use crate::{HistoryError, Result, SessionAccess, SessionId};

impl SqliteHistory {
    pub async fn private_session_ids_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        owner: &str,
    ) -> Result<Vec<SessionId>> {
        self.require_retained()?;
        SessionAccess::Private {
            owner: owner.into(),
        }
        .validate()?;
        let rows: Vec<(String,)> = query_as("SELECT session_id FROM history_sessions WHERE access_kind='private' AND owner_key=? ORDER BY catalog_id")
            .bind(owner).fetch_all(&mut **tx).await.map_err(db)?;
        rows.into_iter()
            .map(|(id,)| SessionId::parse(&id))
            .collect()
    }

    /// Transfers exactly these private sessions. The caller commits the outer
    /// transaction together with its identity metadata. Journals, defaults, and
    /// creation-request provenance are preserved. Any failure rolls back this set.
    pub async fn reassign_private_owner_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        ids: &[SessionId],
        expected_old: &str,
        new_owner: &str,
    ) -> Result<()> {
        self.require_retained()?;
        SessionAccess::Private {
            owner: expected_old.into(),
        }
        .validate()?;
        SessionAccess::Private {
            owner: new_owner.into(),
        }
        .validate()?;
        let mut savepoint = tx.begin().await.map_err(db)?;
        let result = async {
            // Validate the whole set before mutation, including duplicate IDs.
            for id in ids {
                let session = self.get_session_in(&mut savepoint, id).await?.ok_or(HistoryError::SessionNotFound)?;
                if session.access != (SessionAccess::Private { owner: expected_old.into() }) {
                    return Err(HistoryError::SessionConflict);
                }
            }
            for (index, id) in ids.iter().enumerate() {
                if ids[..index].contains(id) { continue; }
                let changed = query("UPDATE history_sessions SET owner_key=?,revision=revision+1 WHERE session_id=? AND access_kind='private' AND owner_key=?")
                    .bind(new_owner).bind(id.as_str()).bind(expected_old).execute(&mut *savepoint).await.map_err(db)?;
                if changed.rows_affected() != 1 { return Err(HistoryError::SessionConflict); }
            }
            Ok(())
        }.await;
        match result {
            Ok(()) => savepoint.commit().await.map_err(db),
            Err(error) => {
                savepoint.rollback().await.map_err(db)?;
                Err(error)
            }
        }
    }
}
