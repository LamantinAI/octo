//! Retained private sessions share frozen journal prefixes by reference.
use sqlx::{Acquire, Sqlite, Transaction, query, query_as, query_scalar};

use super::{SqliteHistory, db};
use crate::{
    Compact, CreateSession, HistoryError, Result, Session, SessionAccess, SessionId,
    SessionJournal, SessionKind,
};

// Each branch owns IDs strictly above its immediate fork boundary. Intersecting
// cutoffs while walking toward the root yields disjoint visible ranges, even
// when a nested fork is taken at a point inside an inherited prefix.
const VISIBLE_PREFIXES: &str = "WITH RECURSIVE visible(history_key, through_id, depth) AS (
    SELECT ?, 9223372036854775807, 0
    UNION ALL
    SELECT parent.history_key, min(visible.through_id, f.parent_through_id), visible.depth + 1
      FROM visible
      JOIN history_session_journals current ON current.history_key=visible.history_key AND current.run_id=''
      JOIN history_session_forks f ON f.session_id=current.session_id
      JOIN history_session_journals parent ON parent.session_id=f.parent_session_id AND parent.run_id=''
) ";

pub(super) fn visible_sql(query: &str) -> String {
    format!("{VISIBLE_PREFIXES}{query}")
}

impl SqliteHistory {
    /// Create a permanent branch and its immutable boundary under the caller's
    /// write transaction. The host supplies authorization and selection policy.
    pub async fn fork_session_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        parent: &SessionId,
        through: Option<i64>,
        title: String,
    ) -> Result<Session> {
        self.require_retained()?;
        let mut savepoint = tx.begin().await.map_err(db)?;
        let result = self
            .create_fork(&mut savepoint, parent, through, title)
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

    async fn create_fork(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        parent: &SessionId,
        through: Option<i64>,
        title: String,
    ) -> Result<Session> {
        let source = self
            .get_session_in(tx, parent)
            .await?
            .ok_or(HistoryError::SessionNotFound)?;
        let SessionAccess::Private { owner } = source.access else {
            return Err(invalid_source());
        };
        if source.kind != SessionKind::Permanent {
            return Err(invalid_source());
        }
        let key = self
            .session_journal_key_in(tx, &SessionJournal::Root(parent.clone()), false)
            .await?;
        self.backfill_public_ids_in(tx, &key).await?;
        let head = self.effective_turn_head_in(tx, &key).await?;
        let boundary = through.unwrap_or(head);
        if boundary != 0 && self.visible_turn_in(tx, &key, boundary).await?.is_none() {
            return Err(HistoryError::InvalidRequest(
                "fork boundary is outside the source journal".into(),
            ));
        }
        let checkpoint = self
            .effective_compact_in(tx, &key, boundary)
            .await?
            .map(|row| row.0);
        let mut request = CreateSession::private(owner);
        request.host = source.host;
        request.title = title;
        let child = self.create_session_in(tx, request).await?;
        query("INSERT INTO history_session_forks(session_id,parent_session_id,parent_through_id,parent_compact_id) VALUES (?,?,?,?)")
            .bind(child.id.as_str()).bind(parent.as_str()).bind(boundary).bind(checkpoint)
            .execute(&mut **tx).await.map_err(db)?;
        Ok(child)
    }

    /// Archived branches retain their references. Call before any erasure side effects.
    pub async fn fork_dependents_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        id: &SessionId,
    ) -> Result<Vec<SessionId>> {
        let ids: Vec<String> = query_scalar("SELECT session_id FROM history_session_forks WHERE parent_session_id=? ORDER BY session_id")
            .bind(id.as_str()).fetch_all(&mut **tx).await.map_err(db)?;
        ids.into_iter().map(|id| SessionId::parse(&id)).collect()
    }

    /// The upper public turn ID inherited from an ancestor's own root journal.
    /// The caller authorizes the descendant and validates any record-level association.
    pub async fn inherited_root_cutoff(
        &self,
        descendant: &SessionId,
        ancestor: &SessionId,
    ) -> Result<Option<i64>> {
        self.require_retained()?;
        let descendant_key = SessionJournal::Root(descendant.clone()).storage_key()?;
        let ancestor_key = SessionJournal::Root(ancestor.clone()).storage_key()?;
        let sql = visible_sql("SELECT through_id FROM visible WHERE depth>0 AND history_key=?");
        query_scalar(&sql)
            .bind(descendant_key)
            .bind(ancestor_key)
            .fetch_optional(&self.pool)
            .await
            .map_err(db)
    }

    pub(super) async fn fork_floor_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        key: &str,
        turns: bool,
    ) -> Result<i64> {
        let join = if turns {
            "f.parent_through_id"
        } else {
            "m.public_id"
        };
        let sql = format!(
            "SELECT COALESCE((SELECT {join} FROM history_session_journals j
             JOIN history_session_forks f ON f.session_id=j.session_id
             LEFT JOIN history_public_compacts m ON m.compact_id=f.parent_compact_id
             WHERE j.history_key=? AND j.run_id=''),0)"
        );
        query_scalar(&sql)
            .bind(key)
            .fetch_one(&mut **tx)
            .await
            .map_err(db)
    }

    pub(super) async fn effective_turn_head_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        key: &str,
    ) -> Result<i64> {
        let own: i64 = query_scalar(
            "SELECT COALESCE(MAX(public_id),0) FROM history_public_turns WHERE history_key=?",
        )
        .bind(key)
        .fetch_one(&mut **tx)
        .await
        .map_err(db)?;
        Ok(own.max(self.fork_floor_in(tx, key, true).await?))
    }

    /// Ordered from the newest local suffix to the oldest inherited prefix.
    pub(super) async fn visible_segments_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        key: &str,
    ) -> Result<Vec<(String, i64)>> {
        let sql = visible_sql("SELECT history_key,through_id FROM visible ORDER BY depth");
        query_as(&sql)
            .bind(key)
            .fetch_all(&mut **tx)
            .await
            .map_err(db)
    }

    pub(super) async fn visible_turn_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        key: &str,
        public_id: i64,
    ) -> Result<Option<i64>> {
        let sql = visible_sql("SELECT m.turn_id FROM visible v JOIN history_public_turns m ON m.history_key=v.history_key
            WHERE m.public_id=? AND m.public_id<=v.through_id");
        query_scalar(&sql)
            .bind(key)
            .bind(public_id)
            .fetch_optional(&mut **tx)
            .await
            .map_err(db)
    }

    /// Physical ID and public projection of the newest checkpoint visible at the cut.
    pub(super) async fn effective_compact_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        key: &str,
        through: i64,
    ) -> Result<Option<(i64, Compact)>> {
        // The fork checkpoint predates every checkpoint written in the branch.
        // Prefer an indexed local lookup, then access the pinned row by its PK.
        let local = query_as(
            "SELECT c.id,mc.public_id,mt.public_id,c.content
            FROM history_public_compacts mc JOIN compacts c ON c.id=mc.compact_id
            JOIN history_public_turns mt ON mt.turn_id=c.through_id
            WHERE mc.history_key=? AND mt.public_id<=? ORDER BY mc.public_id DESC LIMIT 1",
        )
        .bind(key)
        .bind(through)
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)?;
        let row: Option<(i64, i64, i64, String)> = match local {
            Some(row) => Some(row),
            None => query_as("SELECT c.id,mc.public_id,mt.public_id,c.content
                FROM history_session_journals j JOIN history_session_forks f ON f.session_id=j.session_id
                JOIN compacts c ON c.id=f.parent_compact_id
                JOIN history_public_compacts mc ON mc.compact_id=c.id
                JOIN history_public_turns mt ON mt.turn_id=c.through_id
                WHERE j.history_key=? AND j.run_id='' AND mt.public_id<=?")
                .bind(key).bind(through).fetch_optional(&mut **tx).await.map_err(db)?,
        };
        Ok(row.map(|(physical, id, through_id, content)| {
            (
                physical,
                Compact {
                    id,
                    through_id,
                    content,
                },
            )
        }))
    }
}

fn invalid_source() -> HistoryError {
    HistoryError::InvalidRequest("only saved private sessions can be forked".into())
}
