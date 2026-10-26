use sqlx::query_as;

use super::{SqliteHistory, db, strict_role_from};
use crate::snapshot::batch_digest;
use crate::{AppendReceipt, HistoryError, RequestBinding, Result, SessionId, SessionJournal, Turn};

impl SqliteHistory {
    pub(super) async fn lookup_request(
        &self,
        namespace: &str,
        request_id: &str,
    ) -> Result<Option<RequestBinding>> {
        if namespace.is_empty()
            || namespace.len() > 128
            || request_id.is_empty()
            || request_id.len() > 256
        {
            return Err(HistoryError::InvalidRequest(
                "invalid request lookup identity".into(),
            ));
        }
        let mut tx = self.begin_journal().await?;
        let unbound:Option<(i64,)>=query_as(
            "SELECT a.id FROM journal_appends a LEFT JOIN history_session_journals j ON j.history_key=a.channel
             LEFT JOIN history_sessions s ON s.session_id=j.session_id
             WHERE a.namespace=? AND a.idempotency_key=? AND s.session_id IS NULL LIMIT 1")
            .bind(namespace).bind(request_id).fetch_optional(&mut *tx).await.map_err(db)?;
        if unbound.is_some() {
            return Err(HistoryError::RequestUnbound);
        }
        let rows:Vec<(String,String,i64,String,String)>=query_as(
            "SELECT s.session_id,j.history_key,a.id,a.request_digest,a.batch_digest FROM journal_appends a
             JOIN history_session_journals j ON j.history_key=a.channel AND j.run_id=''
             JOIN history_sessions s ON s.session_id=j.session_id
             WHERE a.namespace=? AND a.idempotency_key=? LIMIT 2")
            .bind(namespace).bind(request_id).fetch_all(&mut *tx).await.map_err(db)?;
        if rows.len() > 1 {
            return Err(HistoryError::RequestAmbiguous);
        }
        let result = if let Some((id, key, append, digest, stored_batch)) = rows.into_iter().next()
        {
            self.backfill_public_ids_in(&mut tx, &key).await?;
            let rows:Vec<(i64,String,String)>=query_as("SELECT r.turn_id,t.role,t.content FROM journal_append_records r JOIN turns t ON t.id=r.turn_id WHERE r.append_id=? ORDER BY r.ordinal")
                .bind(append).fetch_all(&mut *tx).await.map_err(db)?;
            let mut record_ids = Vec::with_capacity(rows.len());
            let mut batch = Vec::with_capacity(rows.len());
            for (physical, role, content) in rows {
                record_ids.push(self.public_turn_in(&mut tx, &key, physical).await?);
                batch.push(Turn {
                    role: strict_role_from(&role)?,
                    content,
                });
            }
            if digest.is_empty() || digest.len() > 256 || batch_digest(&batch)? != stored_batch {
                return Err(HistoryError::Db("corrupt native request receipt".into()));
            }
            let through_id = *record_ids
                .last()
                .ok_or_else(|| HistoryError::Db("empty native request receipt".into()))?;
            Some(RequestBinding {
                journal: SessionJournal::Root(SessionId::parse(&id)?),
                digest,
                batch_digest: stored_batch,
                receipt: AppendReceipt {
                    record_ids,
                    through_id,
                    duplicate: true,
                },
            })
        } else {
            None
        };
        tx.commit().await.map_err(db)?;
        Ok(result)
    }
}
