use async_trait::async_trait;
use serde_json::to_string;
use sqlx::{Acquire, FromRow, Sqlite, Transaction, query, query_as};

use super::{SqliteHistory, db};
use crate::{
    AppendReceipt, AppendRequest, ContextWindow, CreateSession, HistoryError, JournalPage,
    MAX_PAGE_SIZE, PageRequest, Persistence, RequestAlias, RequestBinding, Result, Session,
    SessionAccess, SessionFilter, SessionId, SessionJournal, SessionKind, SessionOrigin,
    SessionPage, SessionState, SessionStore, SessionUpdate, Turn,
};

pub(super) const SESSION_COLUMNS: &str = "session_id,access_kind,owner_key,connector_key,channel_key,host,title,kind,state,created_at,revision";

#[derive(FromRow)]
pub(super) struct SessionRow {
    session_id: String,
    access_kind: String,
    owner_key: Option<String>,
    connector_key: Option<String>,
    channel_key: Option<String>,
    host: String,
    title: String,
    kind: String,
    state: String,
    created_at: String,
    revision: i64,
}
impl SessionRow {
    pub(super) fn decode(self) -> Result<Session> {
        let invalid = || HistoryError::Db("invalid stored session metadata".into());
        let access = match self.access_kind.as_str() {
            "private" => SessionAccess::Private {
                owner: self.owner_key.ok_or_else(invalid)?,
            },
            "group_bound" => SessionAccess::GroupBound {
                connector: self.connector_key.ok_or_else(invalid)?,
                channel: self.channel_key.ok_or_else(invalid)?,
            },
            _ => return Err(invalid()),
        };
        let kind = match self.kind.as_str() {
            "permanent" => SessionKind::Permanent,
            "temporary" => SessionKind::Temporary,
            _ => return Err(invalid()),
        };
        let state = match self.state.as_str() {
            "active" => SessionState::Active,
            "archived" => SessionState::Archived,
            _ => return Err(invalid()),
        };
        Ok(Session {
            id: SessionId::parse(&self.session_id)?,
            access,
            host: self.host,
            title: self.title,
            kind,
            state,
            created_at: self.created_at,
            revision: self.revision,
        })
    }
}

impl SqliteHistory {
    pub async fn get_session_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        id: &SessionId,
    ) -> Result<Option<Session>> {
        self.require_retained()?;
        let statement =
            format!("SELECT {SESSION_COLUMNS} FROM history_sessions WHERE session_id = ?");
        query_as::<_, SessionRow>(&statement)
            .bind(id.as_str())
            .fetch_optional(&mut **tx)
            .await
            .map_err(db)?
            .map(SessionRow::decode)
            .transpose()
    }

    /// Creation and application-owned bindings can commit in the same transaction.
    /// Identity, access and host are immutable through subsequent metadata updates.
    pub async fn create_session_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        request: CreateSession,
    ) -> Result<Session> {
        let mut savepoint = tx.begin().await.map_err(db)?;
        match self.create_session_inner(&mut savepoint, request).await {
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

    async fn create_session_inner(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        mut request: CreateSession,
    ) -> Result<Session> {
        self.require_retained()?;
        request.validate()?;
        if request.kind == SessionKind::Temporary {
            return Err(HistoryError::Unsupported(
                "durable storage of unsaved temporary sessions",
            ));
        }
        let id = request.id.get_or_insert_with(SessionId::new).clone();
        let creation = to_string(&request)?;
        if let Some((stored,)) = query_as::<_, (String,)>(
            "SELECT creation_request FROM history_sessions WHERE session_id = ?",
        )
        .bind(id.as_str())
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)?
        {
            if stored != creation {
                return Err(HistoryError::SessionConflict);
            }
            return self
                .get_session_in(tx, &id)
                .await?
                .ok_or(HistoryError::SessionNotFound);
        }
        let root = SessionJournal::Root(id.clone()).storage_key()?;
        self.check_unmapped(tx, &root).await?;
        let occupied = self.journal_inventory_in(tx, &root).await?;
        if occupied.turns + occupied.compacts + occupied.receipts != 0 {
            return Err(HistoryError::SessionConflict);
        }
        let (kind, owner, connector, channel) = match &request.access {
            SessionAccess::Private { owner } => ("private", Some(owner.as_str()), None, None),
            SessionAccess::GroupBound { connector, channel } => (
                "group_bound",
                None,
                Some(connector.as_str()),
                Some(channel.as_str()),
            ),
        };
        query("INSERT INTO history_sessions (session_id,access_kind,owner_key,connector_key,channel_key,host,title,kind,state,creation_request)
               VALUES (?,?,?,?,?,?,?,'permanent','active',?)")
            .bind(id.as_str()).bind(kind).bind(owner).bind(connector).bind(channel)
            .bind(&request.host).bind(&request.title).bind(creation).execute(&mut **tx).await
            .map_err(|error| if error.as_database_error().is_some_and(|e| e.is_unique_violation()) { HistoryError::SessionConflict } else { db(error) })?;
        query(
            "INSERT INTO history_session_journals (session_id,run_id,history_key) VALUES (?,'',?)",
        )
        .bind(id.as_str())
        .bind(root)
        .execute(&mut **tx)
        .await
        .map_err(db)?;
        let session = self
            .get_session_in(tx, &id)
            .await?
            .ok_or(HistoryError::SessionNotFound)?;
        if let SessionAccess::GroupBound { connector, channel } = &session.access {
            self.bind_default_in(
                tx,
                &SessionOrigin {
                    connector: connector.clone(),
                    channel: channel.clone(),
                },
                &session,
            )
            .await?;
        }
        Ok(session)
    }

    pub async fn update_session_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        id: &SessionId,
        update: SessionUpdate,
    ) -> Result<Session> {
        update.validate()?;
        let current = self
            .get_session_in(tx, id)
            .await?
            .ok_or(HistoryError::SessionNotFound)?;
        if current.revision != update.expected_revision {
            return Err(HistoryError::RevisionConflict {
                expected: update.expected_revision,
                actual: current.revision,
            });
        }
        let title = update.title.unwrap_or(current.title);
        let state = match update.archived {
            Some(true) => "archived",
            Some(false) => "active",
            None => match current.state {
                SessionState::Active => "active",
                SessionState::Archived => "archived",
            },
        };
        let changed = query("UPDATE history_sessions SET title = ?, state = ?, revision = revision + 1 WHERE session_id = ? AND revision = ?")
            .bind(title).bind(state).bind(id.as_str()).bind(update.expected_revision)
            .execute(&mut **tx).await.map_err(db)?;
        if changed.rows_affected() != 1 {
            return Err(HistoryError::SessionConflict);
        }
        self.get_session_in(tx, id)
            .await?
            .ok_or(HistoryError::SessionNotFound)
    }
}

#[async_trait]
impl SessionStore for SqliteHistory {
    fn persistence(&self) -> Persistence {
        Persistence::Durable
    }

    async fn create_session(&self, request: CreateSession) -> Result<Session> {
        let mut tx = self.begin_journal().await?;
        let session = self.create_session_in(&mut tx, request).await?;
        tx.commit().await.map_err(db)?;
        Ok(session)
    }
    async fn get_session(&self, id: &SessionId) -> Result<Option<Session>> {
        let mut tx = self.pool.begin().await.map_err(db)?;
        let session = self.get_session_in(&mut tx, id).await?;
        tx.commit().await.map_err(db)?;
        Ok(session)
    }
    async fn find_request(
        &self,
        namespace: &str,
        request_id: &str,
    ) -> Result<Option<RequestBinding>> {
        self.lookup_request(namespace, request_id).await
    }
    async fn get_default_session(&self, origin: &SessionOrigin) -> Result<Option<Session>> {
        let mut tx = self.pool.begin().await.map_err(db)?;
        let session = self.get_default_session_in(&mut tx, origin).await?;
        tx.commit().await.map_err(db)?;
        Ok(session)
    }
    async fn ensure_default_session(
        &self,
        origin: &SessionOrigin,
        request: CreateSession,
    ) -> Result<Session> {
        let mut tx = self.begin_journal().await?;
        let session = self
            .ensure_default_session_in(&mut tx, origin, request)
            .await?;
        tx.commit().await.map_err(db)?;
        Ok(session)
    }
    async fn list_sessions(
        &self,
        filter: &SessionFilter,
        page: PageRequest,
    ) -> Result<SessionPage> {
        self.require_retained()?;
        page.validate()?;
        let (owner, connector, channel) = match filter {
            SessionFilter::PrivateOwner(owner) => (Some(owner.as_str()), None, None),
            SessionFilter::GroupOrigin { connector, channel } => {
                (None, Some(connector.as_str()), Some(channel.as_str()))
            }
        };
        let predicate = "((? IS NOT NULL AND access_kind='private' AND owner_key=?) OR (? IS NOT NULL AND access_kind='group_bound' AND connector_key=? AND channel_key=?))";
        let mut tx = self.pool.begin().await.map_err(db)?;
        let statement =
            format!("SELECT COALESCE(MAX(catalog_id),0) FROM history_sessions WHERE {predicate}");
        let (head,): (i64,) = query_as(&statement)
            .bind(owner)
            .bind(owner)
            .bind(connector)
            .bind(connector)
            .bind(channel)
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        let through_id = page.through_id.unwrap_or(head);
        if through_id > head {
            return Err(HistoryError::InvalidRequest(
                "session watermark exceeds catalog head".into(),
            ));
        }
        let statement = format!(
            "SELECT {SESSION_COLUMNS} FROM history_sessions WHERE {predicate} AND catalog_id<=? AND (? IS NULL OR catalog_id<?) ORDER BY catalog_id DESC LIMIT ?"
        );
        let mut rows: Vec<SessionRow> = query_as(&statement)
            .bind(owner)
            .bind(owner)
            .bind(connector)
            .bind(connector)
            .bind(channel)
            .bind(through_id)
            .bind(page.before_id)
            .bind(page.before_id)
            .bind((page.limit + 1) as i64)
            .fetch_all(&mut *tx)
            .await
            .map_err(db)?;
        let has_more = rows.len() > page.limit;
        rows.truncate(page.limit);
        let next_before = if has_more {
            let last = rows.last().expect("nonempty catalog page");
            let (cursor,): (i64,) =
                query_as("SELECT catalog_id FROM history_sessions WHERE session_id=?")
                    .bind(&last.session_id)
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(db)?;
            Some(cursor)
        } else {
            None
        };
        let sessions = rows
            .into_iter()
            .map(SessionRow::decode)
            .collect::<Result<Vec<_>>>()?;
        tx.commit().await.map_err(db)?;
        Ok(SessionPage {
            sessions,
            next_before,
            through_id,
            has_more,
        })
    }
    async fn update_session(&self, id: &SessionId, update: SessionUpdate) -> Result<Session> {
        let mut tx = self.begin_journal().await?;
        let session = self.update_session_in(&mut tx, id, update).await?;
        tx.commit().await.map_err(db)?;
        Ok(session)
    }

    async fn append_session(
        &self,
        journal: &SessionJournal,
        request: &AppendRequest,
    ) -> Result<AppendReceipt> {
        let mut tx = self.begin_journal().await?;
        let receipt = self.append_session_in(&mut tx, journal, request).await?;
        tx.commit().await.map_err(db)?;
        Ok(receipt)
    }
    async fn append_session_aliased(
        &self,
        journal: &SessionJournal,
        request: &AppendRequest,
        aliases: &[RequestAlias],
    ) -> Result<AppendReceipt> {
        let mut tx = self.begin_journal().await?;
        let receipt = self
            .append_session_aliased_in(&mut tx, journal, request, aliases)
            .await?;
        tx.commit().await.map_err(db)?;
        Ok(receipt)
    }
    async fn session_page(
        &self,
        journal: &SessionJournal,
        page: PageRequest,
    ) -> Result<JournalPage> {
        let mut tx = self.begin_journal().await?;
        let key = self.session_journal_key_in(&mut tx, journal, false).await?;
        self.backfill_public_ids_in(&mut tx, &key).await?;
        let page = self.public_page_in(&mut tx, &key, page).await?;
        tx.commit().await.map_err(db)?;
        Ok(page)
    }
    async fn session_context(&self, journal: &SessionJournal) -> Result<ContextWindow> {
        let mut tx = self.begin_journal().await?;
        let key = self.session_journal_key_in(&mut tx, journal, false).await?;
        self.backfill_public_ids_in(&mut tx, &key).await?;
        let context = self.public_context_in(&mut tx, &key).await?;
        tx.commit().await.map_err(db)?;
        Ok(context)
    }
    async fn save_session_compact(
        &self,
        journal: &SessionJournal,
        previous_id: Option<i64>,
        through_id: i64,
        content: &str,
    ) -> Result<bool> {
        let mut tx = self.begin_journal().await?;
        let key = self.session_journal_key_in(&mut tx, journal, false).await?;
        self.backfill_public_ids_in(&mut tx, &key).await?;
        let saved = self
            .save_public_compact_in(&mut tx, &key, previous_id, through_id, content)
            .await?;
        tx.commit().await.map_err(db)?;
        Ok(saved)
    }
    async fn load_session(&self, journal: &SessionJournal) -> Result<Vec<Turn>> {
        let mut before_id = None;
        let mut through_id = None;
        let mut blocks = Vec::new();
        loop {
            let page = self
                .session_page(
                    journal,
                    PageRequest {
                        before_id,
                        through_id,
                        limit: MAX_PAGE_SIZE,
                    },
                )
                .await?;
            blocks.push(page.records.into_iter().map(|r| r.turn).collect::<Vec<_>>());
            if !page.has_more {
                break;
            }
            before_id = page.next_before;
            through_id = Some(page.through_id);
        }
        Ok(blocks.into_iter().rev().flatten().collect())
    }
}
