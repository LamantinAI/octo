use sqlx::{Acquire, Sqlite, Transaction, query, query_as};

use super::{SqliteHistory, db};
use crate::{AppendReceipt, AppendRequest, HistoryError, RequestAlias, Result, SessionJournal};
use crate::{aliases::verified_aliases, snapshot::batch_digest};

impl SqliteHistory {
    /// Atomically attach aliases to the SAME accepted rows, never duplicate turns.
    pub async fn append_session_aliased_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        journal: &SessionJournal,
        request: &AppendRequest,
        aliases: &[RequestAlias],
    ) -> Result<AppendReceipt> {
        let aliases = verified_aliases(request, aliases)?;
        let mut savepoint = tx.begin().await.map_err(db)?;
        let result=async {
            let receipt=self.append_session_in(&mut savepoint,journal,request).await?;
            let key=self.session_journal_key_in(&mut savepoint,journal,false).await?;
            let digest=batch_digest(&request.turns)?;
            let mut physical=Vec::with_capacity(receipt.record_ids.len());
            for public in &receipt.record_ids {
                let (id,):(i64,)=query_as("SELECT turn_id FROM history_public_turns WHERE history_key=? AND public_id=?")
                    .bind(&key).bind(public).fetch_one(&mut *savepoint).await.map_err(db)?;
                physical.push(id);
            }
            for alias in aliases {
                let inserted=query("INSERT INTO journal_appends(channel,namespace,idempotency_key,request_digest,batch_digest) VALUES (?,?,?,?,?) ON CONFLICT(channel,namespace,idempotency_key) DO NOTHING")
                    .bind(&key).bind(&alias.namespace).bind(&alias.idempotency_key).bind(&alias.digest).bind(&digest)
                    .execute(&mut *savepoint).await.map_err(db)?;
                let (id,stored_digest,stored_batch):(i64,String,String)=query_as("SELECT id,request_digest,batch_digest FROM journal_appends WHERE channel=? AND namespace=? AND idempotency_key=?")
                    .bind(&key).bind(&alias.namespace).bind(&alias.idempotency_key).fetch_one(&mut *savepoint).await.map_err(db)?;
                if stored_digest!=alias.digest||stored_batch!=digest{return Err(HistoryError::IdempotencyConflict)}
                if inserted.rows_affected()==0 {
                    let rows:Vec<(i64,)>=query_as("SELECT turn_id FROM journal_append_records WHERE append_id=? ORDER BY ordinal")
                        .bind(id).fetch_all(&mut *savepoint).await.map_err(db)?;
                    if rows.into_iter().map(|(id,)|id).collect::<Vec<_>>()!=physical{return Err(HistoryError::IdempotencyConflict)}
                }else{
                    for (ordinal,turn) in physical.iter().enumerate(){
                        query("INSERT INTO journal_append_records(append_id,ordinal,turn_id) VALUES (?,?,?)")
                            .bind(id).bind(ordinal as i64).bind(turn).execute(&mut *savepoint).await.map_err(db)?;
                    }
                }
            }
            Ok(receipt)
        }.await;
        match result {
            Ok(receipt) => {
                savepoint.commit().await.map_err(db)?;
                Ok(receipt)
            }
            Err(error) => {
                savepoint.rollback().await.map_err(db)?;
                Err(error)
            }
        }
    }
}
