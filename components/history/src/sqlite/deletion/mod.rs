//! Transactional erasure of one catalog session. Hosts retain their own lifecycle
//! tombstone and rotate default bindings before calling this storage primitive.
use sqlx::{Acquire, Sqlite, Transaction, query, query_as};

use super::{SqliteHistory, db};
use crate::{HistoryError, Result, Session, SessionId, SessionOrigin};

impl SqliteHistory {
    pub async fn default_origins_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        id: &SessionId,
    ) -> Result<Vec<SessionOrigin>> {
        self.require_retained()?;
        let rows: Vec<(String, String)> = query_as(
            "SELECT connector_key,channel_key FROM history_session_defaults WHERE session_id=?",
        )
        .bind(id.as_str())
        .fetch_all(&mut **tx)
        .await
        .map_err(db)?;
        Ok(rows
            .into_iter()
            .map(|(connector, channel)| SessionOrigin { connector, channel })
            .collect())
    }

    /// The caller commits replacement catalog metadata, selectors and erasure in
    /// the same transaction. Access and execution host cannot change implicitly.
    pub async fn replace_default_session_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        origin: &SessionOrigin,
        expected: &SessionId,
        replacement: &Session,
    ) -> Result<()> {
        let current = self
            .get_default_session_in(tx, origin)
            .await?
            .ok_or(HistoryError::SessionNotFound)?;
        if current.id != *expected
            || current.access != replacement.access
            || current.host != replacement.host
            || current.kind != replacement.kind
        {
            return Err(HistoryError::SessionConflict);
        }
        let changed = query("UPDATE history_session_defaults SET session_id=? WHERE connector_key=? AND channel_key=? AND session_id=?")
            .bind(replacement.id.as_str()).bind(&origin.connector).bind(&origin.channel)
            .bind(expected.as_str()).execute(&mut **tx).await.map_err(db)?;
        if changed.rows_affected() != 1 {
            return Err(HistoryError::SessionConflict);
        }
        Ok(())
    }

    /// Remove transcript and metadata while retaining content-free append
    /// identities. A later native lookup returns RequestUnbound, never new work.
    /// Default bindings must already have been replaced within this transaction.
    pub async fn delete_session_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        id: &SessionId,
        expected_revision: i64,
    ) -> Result<Session> {
        self.require_retained()?;
        let mut savepoint = tx.begin().await.map_err(db)?;
        let result = async {
            let session = self.get_session_in(&mut savepoint, id).await?
                .ok_or(HistoryError::SessionNotFound)?;
            if session.revision != expected_revision {
                return Err(HistoryError::RevisionConflict { expected: expected_revision, actual: session.revision });
            }
            let forks = self.fork_dependents_in(&mut savepoint, id).await?;
            if !forks.is_empty() {
                return Err(HistoryError::SessionHasForks(forks.len()));
            }
            if !self.default_origins_in(&mut savepoint, id).await?.is_empty() {
                return Err(HistoryError::SessionConflict);
            }
            let keys: Vec<(String,)> = query_as("SELECT history_key FROM history_session_journals WHERE session_id=?")
                .bind(id.as_str()).fetch_all(&mut *savepoint).await.map_err(db)?;
            for (key,) in keys {
                query("DELETE FROM history_public_compacts WHERE history_key=?").bind(&key).execute(&mut *savepoint).await.map_err(db)?;
                query("DELETE FROM compacts WHERE channel=?").bind(&key).execute(&mut *savepoint).await.map_err(db)?;
                query("DELETE FROM history_public_turns WHERE history_key=?").bind(&key).execute(&mut *savepoint).await.map_err(db)?;
                query("DELETE FROM journal_append_records WHERE turn_id IN (SELECT id FROM turns WHERE channel=?)")
                    .bind(&key).execute(&mut *savepoint).await.map_err(db)?;
                query("DELETE FROM turns WHERE channel=?").bind(&key).execute(&mut *savepoint).await.map_err(db)?;
            }
            query("DELETE FROM history_session_promotions WHERE session_id=?").bind(id.as_str()).execute(&mut *savepoint).await.map_err(db)?;
            query("DELETE FROM history_session_journals WHERE session_id=?").bind(id.as_str()).execute(&mut *savepoint).await.map_err(db)?;
            let deleted = query("DELETE FROM history_sessions WHERE session_id=? AND revision=?")
                .bind(id.as_str()).bind(expected_revision).execute(&mut *savepoint).await.map_err(db)?;
            if deleted.rows_affected() != 1 { return Err(HistoryError::SessionConflict); }
            Ok(session)
        }.await;
        match result {
            Ok(session) => {
                savepoint.commit().await.map_err(db)?;
                Ok(session)
            }
            Err(error) => {
                savepoint.rollback().await.map_err(db)?;
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use sqlx::{query, query_scalar};
    use tempfile::tempdir;

    use crate::{
        AppendRequest, CreateSession, HistoryError, RequestAlias, SessionJournal, SessionOrigin,
        SessionStore, SqliteHistory, Turn,
    };

    #[tokio::test]
    async fn erasure_removes_root_children_compacts_and_keeps_alias_replay_fences() {
        let dir = tempdir().unwrap();
        let history = SqliteHistory::open_retained(dir.path().join("history.sqlite"))
            .await
            .unwrap();
        let session = history
            .create_session(CreateSession::private("owner"))
            .await
            .unwrap();
        let kept = history
            .create_session(CreateSession::private("owner"))
            .await
            .unwrap();
        let root = SessionJournal::Root(session.id.clone());
        let child = SessionJournal::Child {
            session_id: session.id.clone(),
            run_id: "child-run".into(),
        };
        let request = AppendRequest {
            namespace: "batch".into(),
            idempotency_key: "request".into(),
            digest: "digest".into(),
            turns: vec![
                Turn::user("private text"),
                Turn::assistant("private answer"),
            ],
        };
        history
            .append_session_aliased(
                &root,
                &request,
                &[RequestAlias {
                    namespace: "native".into(),
                    idempotency_key: "message".into(),
                    digest: "native-digest".into(),
                }],
            )
            .await
            .unwrap();
        history
            .append_session(
                &child,
                &AppendRequest {
                    namespace: "child".into(),
                    idempotency_key: "work".into(),
                    ..request.clone()
                },
            )
            .await
            .unwrap();
        assert!(
            history
                .save_session_compact(&root, None, 2, "private summary")
                .await
                .unwrap()
        );
        assert!(
            history
                .save_session_compact(&child, None, 2, "private child summary")
                .await
                .unwrap()
        );
        let mut tx = history.begin_journal().await.unwrap();
        history
            .delete_session_in(&mut tx, &session.id, session.revision)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert!(history.get_session(&session.id).await.unwrap().is_none());
        assert!(history.get_session(&kept.id).await.unwrap().is_some());
        for table in [
            "turns",
            "compacts",
            "history_public_turns",
            "history_public_compacts",
            "journal_append_records",
        ] {
            let count: i64 = query_scalar(&format!("SELECT count(*) FROM {table}"))
                .fetch_one(history.pool())
                .await
                .unwrap();
            assert_eq!(count, 0, "{table}");
        }
        assert!(matches!(
            history.find_request("native", "message").await,
            Err(HistoryError::RequestUnbound)
        ));
        assert!(matches!(
            history.find_request("batch", "request").await,
            Err(HistoryError::RequestUnbound)
        ));
        assert!(history.append_session(&root, &request).await.is_err());
        drop(history);
        let reopened = SqliteHistory::open_retained(dir.path().join("history.sqlite"))
            .await
            .unwrap();
        assert!(matches!(
            reopened.find_request("native", "message").await,
            Err(HistoryError::RequestUnbound)
        ));
    }

    #[tokio::test]
    async fn stale_revision_default_rebinding_and_transaction_rollback_preserve_history() {
        let dir = tempdir().unwrap();
        let history = SqliteHistory::open_retained(dir.path().join("history.sqlite"))
            .await
            .unwrap();
        let origin = SessionOrigin {
            connector: "chat".into(),
            channel: "private".into(),
        };
        let session = history
            .ensure_default_session(&origin, CreateSession::private("owner"))
            .await
            .unwrap();
        let mut tx = history.begin_journal().await.unwrap();
        assert!(matches!(
            history
                .delete_session_in(&mut tx, &session.id, session.revision + 1)
                .await,
            Err(HistoryError::RevisionConflict { .. })
        ));
        assert!(matches!(
            history
                .delete_session_in(&mut tx, &session.id, session.revision)
                .await,
            Err(HistoryError::SessionConflict)
        ));
        let replacement = history
            .create_session_in(&mut tx, CreateSession::private("owner"))
            .await
            .unwrap();
        history
            .replace_default_session_in(&mut tx, &origin, &session.id, &replacement)
            .await
            .unwrap();
        history
            .delete_session_in(&mut tx, &session.id, session.revision)
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        assert_eq!(
            history
                .get_default_session(&origin)
                .await
                .unwrap()
                .unwrap()
                .id,
            session.id
        );
        assert!(history.get_session(&session.id).await.unwrap().is_some());
        assert!(
            history
                .get_session(&replacement.id)
                .await
                .unwrap()
                .is_none()
        );
        // A failed storage statement rolls back all erasure work inside its savepoint.
        query("CREATE TRIGGER retain_catalog BEFORE DELETE ON history_sessions BEGIN SELECT RAISE(ABORT, 'fixture failure'); END").execute(history.pool()).await.unwrap();
        let mut tx = history.begin_journal().await.unwrap();
        let replacement = history
            .create_session_in(&mut tx, CreateSession::private("owner"))
            .await
            .unwrap();
        history
            .replace_default_session_in(&mut tx, &origin, &session.id, &replacement)
            .await
            .unwrap();
        assert!(
            history
                .delete_session_in(&mut tx, &session.id, session.revision)
                .await
                .is_err()
        );
        assert!(
            history
                .get_session_in(&mut tx, &session.id)
                .await
                .unwrap()
                .is_some()
        );
        tx.rollback().await.unwrap();
    }
}
