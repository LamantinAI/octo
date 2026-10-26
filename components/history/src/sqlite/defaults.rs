use sqlx::{Acquire, Sqlite, Transaction, query, query_as};

use super::{SqliteHistory, db};
use crate::{CreateSession, HistoryError, Result, Session, SessionId, SessionKind, SessionOrigin};

impl SqliteHistory {
    pub async fn get_default_session_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        origin: &SessionOrigin,
    ) -> Result<Option<Session>> {
        self.require_retained()?;
        origin.validate()?;
        let id: Option<(String,)> = query_as(
            "SELECT session_id FROM history_session_defaults WHERE connector_key=? AND channel_key=?",
        ).bind(&origin.connector).bind(&origin.channel).fetch_optional(&mut **tx).await.map_err(db)?;
        match id {
            Some((id,)) => self.get_session_in(tx, &SessionId::parse(&id)?).await,
            None => Ok(None),
        }
    }

    pub(super) async fn bind_default_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        origin: &SessionOrigin,
        session: &Session,
    ) -> Result<()> {
        origin.validate()?;
        if !origin.accepts(&session.access) || session.kind != SessionKind::Permanent {
            return Err(HistoryError::SessionConflict);
        }
        if let Some(existing) = self.get_default_session_in(tx, origin).await? {
            return if existing.id == session.id {
                Ok(())
            } else {
                Err(HistoryError::SessionConflict)
            };
        }
        query("INSERT INTO history_session_defaults (connector_key,channel_key,session_id) VALUES (?,?,?)")
            .bind(&origin.connector).bind(&origin.channel).bind(session.id.as_str())
            .execute(&mut **tx).await.map_err(db)?;
        Ok(())
    }

    /// Establish the immutable original session independently from active selectors.
    /// Also works for empty new channels, without inventing legacy history records.
    pub async fn ensure_default_session_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        origin: &SessionOrigin,
        request: CreateSession,
    ) -> Result<Session> {
        let mut savepoint = tx.begin().await.map_err(db)?;
        let result = async {
            origin.validate()?;
            request.validate()?;
            if !origin.accepts(&request.access) || request.kind != SessionKind::Permanent {
                return Err(HistoryError::SessionConflict);
            }
            if let Some(existing) = self.get_default_session_in(&mut savepoint, origin).await? {
                return if existing.access == request.access
                    && existing.host == request.host
                    && existing.kind == request.kind
                    && request.id.as_ref().is_none_or(|id| *id == existing.id)
                {
                    Ok(existing)
                } else {
                    Err(HistoryError::SessionConflict)
                };
            }
            let session = self.create_session_in(&mut savepoint, request).await?;
            self.bind_default_in(&mut savepoint, origin, &session)
                .await?;
            Ok(session)
        }
        .await;
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
