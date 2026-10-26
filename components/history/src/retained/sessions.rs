use async_trait::async_trait;
use serde_json::{to_string, to_vec};

use super::{CatalogEntry, RetainedMemoryHistory, State, timestamp};
use crate::snapshot::batch_digest;
use crate::{
    AppendReceipt, AppendRequest, ContextWindow, CreateSession, HistoryError, JournalPage,
    PageRequest, Persistence, RequestAlias, RequestBinding, Result, Session, SessionAccess,
    SessionFilter, SessionId, SessionJournal, SessionKind, SessionOrigin, SessionPage,
    SessionState, SessionStore, SessionUpdate, Turn,
};

impl RetainedMemoryHistory {
    fn create_locked(
        &self,
        state: &mut State,
        mut request: CreateSession,
        additional_bytes: usize,
    ) -> Result<Session> {
        request.validate()?;
        let id = request.id.get_or_insert_with(SessionId::new).clone();
        let creation = to_string(&request)?;
        if let Some(existing) = state.sessions.get(&id) {
            if existing.creation != creation {
                return Err(HistoryError::SessionConflict);
            }
            let session = existing.session.clone();
            self.reserve(state, additional_bytes, 0)?;
            return Ok(session);
        }
        if matches!(request.access, SessionAccess::GroupBound { .. })
            && state
                .sessions
                .values()
                .any(|s| s.session.access == request.access)
        {
            return Err(HistoryError::SessionConflict);
        }
        if state.sessions.len() >= self.limits.max_sessions {
            return Err(HistoryError::QuotaExceeded);
        }
        if let SessionAccess::GroupBound { connector, channel } = &request.access {
            if state.defaults.contains_key(&SessionOrigin {
                connector: connector.clone(),
                channel: channel.clone(),
            }) {
                return Err(HistoryError::SessionConflict);
            }
        }
        let key = SessionJournal::Root(id.clone()).storage_key()?;
        if state.journals.contains_key(&key) {
            return Err(HistoryError::SessionConflict);
        }
        let binding_bytes = match &request.access {
            SessionAccess::GroupBound { connector, channel } => {
                connector.len() + channel.len() + id.as_str().len()
            }
            SessionAccess::Private { .. } => 0,
        };
        let session = Session {
            id: id.clone(),
            access: request.access,
            host: request.host,
            title: request.title,
            kind: request.kind,
            state: SessionState::Active,
            created_at: timestamp(),
            revision: 1,
        };
        let metadata_bytes = creation.len() + to_vec(&session)?.len();
        self.reserve(
            state,
            metadata_bytes + key.len() + binding_bytes + additional_bytes,
            0,
        )?;
        state.next_catalog += 1;
        if let SessionAccess::GroupBound { connector, channel } = &session.access {
            state.defaults.insert(
                SessionOrigin {
                    connector: connector.clone(),
                    channel: channel.clone(),
                },
                id.clone(),
            );
        }
        state.sessions.insert(
            id,
            CatalogEntry {
                session: session.clone(),
                sequence: state.next_catalog,
                creation,
                bytes: metadata_bytes,
            },
        );
        state.journals.entry(key.clone()).or_default().bytes = key.len();
        state
            .typed_journals
            .insert(key, SessionJournal::Root(session.id.clone()));
        Ok(session)
    }
    fn journal_locked(state: &State, journal: &SessionJournal, create: bool) -> Result<String> {
        if !state.sessions.contains_key(journal.session_id()) {
            return Err(HistoryError::SessionNotFound);
        }
        let key = journal.storage_key()?;
        if state.journals.contains_key(&key) && state.typed_journals.get(&key) != Some(journal) {
            return Err(HistoryError::SessionConflict);
        }
        if !create && state.typed_journals.get(&key) != Some(journal) {
            return Err(HistoryError::SessionNotFound);
        }
        Ok(key)
    }
}

#[async_trait]
impl SessionStore for RetainedMemoryHistory {
    fn persistence(&self) -> Persistence {
        Persistence::Volatile
    }
    async fn create_session(&self, request: CreateSession) -> Result<Session> {
        self.create_locked(&mut *self.lock()?, request, 0)
    }
    async fn get_session(&self, id: &SessionId) -> Result<Option<Session>> {
        Ok(self.lock()?.sessions.get(id).map(|s| s.session.clone()))
    }
    async fn find_request(
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
        let state = self.lock()?;
        let identity = (namespace.to_owned(), request_id.to_owned());
        if state.journals.iter().any(|(key, journal)| {
            !state.typed_journals.contains_key(key) && journal.receipts.contains_key(&identity)
        }) {
            return Err(HistoryError::RequestUnbound);
        }
        let mut found = None;
        for (key, journal) in &state.typed_journals {
            if !matches!(journal, SessionJournal::Root(_)) {
                continue;
            }
            let data = state
                .journals
                .get(key)
                .ok_or(HistoryError::RequestUnbound)?;
            if let Some(stored) = data.receipts.get(&identity) {
                if found.is_some() {
                    return Err(HistoryError::RequestAmbiguous);
                }
                let mut receipt = stored.receipt.clone();
                receipt.duplicate = true;
                let corrupt = || HistoryError::Db("corrupt native request receipt".into());
                let mut batch = Vec::with_capacity(receipt.record_ids.len());
                let mut previous = 0;
                for id in &receipt.record_ids {
                    if *id <= previous {
                        return Err(corrupt());
                    }
                    let index = data
                        .records
                        .binary_search_by_key(id, |row| row.id)
                        .map_err(|_| corrupt())?;
                    batch.push(data.records[index].turn.clone());
                    previous = *id;
                }
                if batch.is_empty()
                    || stored.digest.is_empty()
                    || stored.digest.len() > 256
                    || receipt.through_id != previous
                    || to_vec(&batch)? != stored.batch
                {
                    return Err(corrupt());
                }
                found = Some(RequestBinding {
                    journal: journal.clone(),
                    digest: stored.digest.clone(),
                    batch_digest: batch_digest(&batch)?,
                    receipt,
                });
            }
        }
        Ok(found)
    }
    async fn get_default_session(&self, origin: &SessionOrigin) -> Result<Option<Session>> {
        origin.validate()?;
        let state = self.lock()?;
        Ok(state
            .defaults
            .get(origin)
            .and_then(|id| state.sessions.get(id))
            .map(|s| s.session.clone()))
    }
    async fn ensure_default_session(
        &self,
        origin: &SessionOrigin,
        mut request: CreateSession,
    ) -> Result<Session> {
        origin.validate()?;
        request.validate()?;
        if !origin.accepts(&request.access) || request.kind != SessionKind::Permanent {
            return Err(HistoryError::SessionConflict);
        }
        let mut state = self.lock()?;
        if let Some(session) = state
            .defaults
            .get(origin)
            .and_then(|id| state.sessions.get(id))
            .map(|s| &s.session)
        {
            return if session.access == request.access
                && session.host == request.host
                && session.kind == request.kind
                && request.id.as_ref().is_none_or(|id| *id == session.id)
            {
                Ok(session.clone())
            } else {
                Err(HistoryError::SessionConflict)
            };
        }
        let id = request.id.get_or_insert_with(SessionId::new).clone();
        let binding_bytes = if matches!(request.access, SessionAccess::Private { .. }) {
            origin.connector.len() + origin.channel.len() + id.as_str().len()
        } else {
            0
        };
        let session = self.create_locked(&mut state, request, binding_bytes)?;
        state.defaults.insert(origin.clone(), session.id.clone());
        Ok(session)
    }
    async fn list_sessions(
        &self,
        filter: &SessionFilter,
        page: PageRequest,
    ) -> Result<SessionPage> {
        page.validate()?;
        let state = self.lock()?;
        let head = state
            .sessions
            .values()
            .filter(|s| filter.matches(&s.session))
            .map(|s| s.sequence)
            .max()
            .unwrap_or(0);
        let through_id = page.through_id.unwrap_or(head);
        if through_id > head {
            return Err(HistoryError::InvalidRequest(
                "session watermark exceeds catalog head".into(),
            ));
        }
        let mut rows: Vec<_> = state
            .sessions
            .values()
            .filter(|s| {
                filter.matches(&s.session)
                    && s.sequence <= through_id
                    && page.before_id.is_none_or(|before| s.sequence < before)
            })
            .collect();
        rows.sort_by_key(|s| -s.sequence);
        let has_more = rows.len() > page.limit;
        rows.truncate(page.limit);
        let next_before = has_more.then(|| rows.last().expect("nonempty catalog page").sequence);
        Ok(SessionPage {
            sessions: rows.into_iter().map(|s| s.session.clone()).collect(),
            next_before,
            through_id,
            has_more,
        })
    }
    async fn update_session(&self, id: &SessionId, update: SessionUpdate) -> Result<Session> {
        update.validate()?;
        let mut state = self.lock()?;
        let mut session = state
            .sessions
            .get(id)
            .ok_or(HistoryError::SessionNotFound)?
            .session
            .clone();
        if session.revision != update.expected_revision {
            return Err(HistoryError::RevisionConflict {
                expected: update.expected_revision,
                actual: session.revision,
            });
        }
        let old_bytes = to_vec(&session)?.len();
        if let Some(title) = update.title {
            session.title = title;
        }
        if let Some(archived) = update.archived {
            session.state = if archived {
                SessionState::Archived
            } else {
                SessionState::Active
            };
        }
        session.revision += 1;
        let new_bytes = to_vec(&session)?.len();
        self.reserve(&mut state, new_bytes.saturating_sub(old_bytes), 0)?;
        state.bytes -= old_bytes.saturating_sub(new_bytes);
        let entry = state.sessions.get_mut(id).expect("existing session");
        entry.bytes = entry.bytes + new_bytes - old_bytes;
        entry.session = session.clone();
        Ok(session)
    }
    async fn append_session(
        &self,
        journal: &SessionJournal,
        request: &AppendRequest,
    ) -> Result<AppendReceipt> {
        let mut state = self.lock()?;
        let key = Self::journal_locked(&state, journal, true)?;
        let receipt = self.append_locked(&mut state, &key, &request.turns, Some(request))?;
        state.typed_journals.insert(key, journal.clone());
        Ok(receipt)
    }
    async fn append_session_aliased(
        &self,
        journal: &SessionJournal,
        request: &AppendRequest,
        aliases: &[RequestAlias],
    ) -> Result<AppendReceipt> {
        let mut state = self.lock()?;
        let key = Self::journal_locked(&state, journal, true)?;
        let receipt = self.append_aliased_locked(&mut state, &key, request, aliases)?;
        state.typed_journals.insert(key, journal.clone());
        Ok(receipt)
    }
    async fn session_page(
        &self,
        journal: &SessionJournal,
        page: PageRequest,
    ) -> Result<JournalPage> {
        let state = self.lock()?;
        let key = Self::journal_locked(&state, journal, false)?;
        self.page_locked(&state, &key, page)
    }
    async fn session_context(&self, journal: &SessionJournal) -> Result<ContextWindow> {
        let state = self.lock()?;
        let key = Self::journal_locked(&state, journal, false)?;
        Ok(Self::context_locked(&state, &key))
    }
    async fn save_session_compact(
        &self,
        journal: &SessionJournal,
        previous_id: Option<i64>,
        through_id: i64,
        content: &str,
    ) -> Result<bool> {
        let mut state = self.lock()?;
        let key = Self::journal_locked(&state, journal, false)?;
        self.compact_locked(&mut state, &key, previous_id, through_id, content)
    }
    async fn load_session(&self, journal: &SessionJournal) -> Result<Vec<Turn>> {
        let state = self.lock()?;
        let key = Self::journal_locked(&state, journal, false)?;
        Ok(state
            .journals
            .get(&key)
            .expect("existing journal")
            .records
            .iter()
            .map(|r| r.turn.clone())
            .collect())
    }
}
