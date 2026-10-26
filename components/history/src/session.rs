//! Authoritative session identities and catalogs. Authentication and transport selection
//! belong to the assembly; storage enforces immutable structural bindings.
use std::{
    fmt::{Display, Formatter, Result as FmtResult},
    str::FromStr,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    AppendReceipt, AppendRequest, ContextWindow, HistoryError, HistoryStore, JournalKeyInventory,
    JournalPage, PageRequest, RequestAlias, Result, Turn,
};

#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SessionId(String);

impl SessionId {
    pub fn new() -> Self {
        Self(Uuid::now_v7().to_string())
    }
    pub fn parse(value: &str) -> Result<Self> {
        Uuid::parse_str(value)
            .map(|id| Self(id.to_string()))
            .map_err(|_| HistoryError::InvalidRequest("invalid session ID".into()))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl Default for SessionId {
    fn default() -> Self {
        Self::new()
    }
}
impl Display for SessionId {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        self.0.fmt(f)
    }
}
impl FromStr for SessionId {
    type Err = HistoryError;
    fn from_str(value: &str) -> Result<Self> {
        Self::parse(value)
    }
}
impl TryFrom<String> for SessionId {
    type Error = HistoryError;
    fn try_from(value: String) -> Result<Self> {
        Self::parse(&value)
    }
}
impl From<SessionId> for String {
    fn from(id: SessionId) -> Self {
        id.0
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum SessionAccess {
    Private { owner: String },
    GroupBound { connector: String, channel: String },
}
impl SessionAccess {
    pub(crate) fn validate(&self) -> Result<()> {
        let fields: Vec<&str> = match self {
            Self::Private { owner } => vec![owner],
            Self::GroupBound { connector, channel } => vec![connector, channel],
        };
        if fields
            .iter()
            .any(|value| value.is_empty() || value.len() > 1024)
        {
            return Err(HistoryError::InvalidRequest(
                "invalid session access binding".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionKind {
    #[default]
    Permanent,
    Temporary,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionState {
    Active,
    Archived,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Persistence {
    Durable,
    Volatile,
}

/// Verified physical origin supplied by the assembly. Default bindings never follow
/// mutable active-session selection and are suitable for legacy reminder resolution.
#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionOrigin {
    pub connector: String,
    pub channel: String,
}
impl SessionOrigin {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.connector.is_empty()
            || self.channel.is_empty()
            || self.connector.len() > 1024
            || self.channel.len() > 1024
        {
            return Err(HistoryError::InvalidRequest(
                "invalid default session origin".into(),
            ));
        }
        Ok(())
    }
    pub(crate) fn accepts(&self, access: &SessionAccess) -> bool {
        match access {
            SessionAccess::Private { .. } => true,
            SessionAccess::GroupBound { connector, channel } => {
                self.connector == *connector && self.channel == *channel
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateSession {
    pub id: Option<SessionId>,
    pub access: SessionAccess,
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub kind: SessionKind,
}
fn default_host() -> String {
    "host".into()
}
impl CreateSession {
    pub fn private(owner: impl Into<String>) -> Self {
        Self {
            id: None,
            access: SessionAccess::Private {
                owner: owner.into(),
            },
            host: default_host(),
            title: String::new(),
            kind: SessionKind::Permanent,
        }
    }
    pub(crate) fn validate(&self) -> Result<()> {
        self.access.validate()?;
        if self.host.is_empty()
            || self.host.len() > 256
            || self.title.len() > 4096
            || (matches!(self.access, SessionAccess::GroupBound { .. })
                && self.kind != SessionKind::Permanent)
        {
            return Err(HistoryError::InvalidRequest(
                "invalid session host, title or group kind".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Session {
    pub id: SessionId,
    pub access: SessionAccess,
    pub host: String,
    pub title: String,
    pub kind: SessionKind,
    pub state: SessionState,
    pub created_at: String,
    pub revision: i64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum SessionFilter {
    PrivateOwner(String),
    GroupOrigin { connector: String, channel: String },
}
impl SessionFilter {
    pub(crate) fn matches(&self, session: &Session) -> bool {
        match (self, &session.access) {
            (Self::PrivateOwner(user), SessionAccess::Private { owner }) => user == owner,
            (
                Self::GroupOrigin { connector, channel },
                SessionAccess::GroupBound {
                    connector: c,
                    channel: ch,
                },
            ) => connector == c && channel == ch,
            _ => false,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionPage {
    /// Newest-created sessions first, unlike chronological journal records.
    pub sessions: Vec<Session>,
    pub next_before: Option<i64>,
    pub through_id: i64,
    pub has_more: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionUpdate {
    pub expected_revision: i64,
    pub title: Option<String>,
    pub archived: Option<bool>,
}
impl SessionUpdate {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.expected_revision < 1 || self.title.as_ref().is_some_and(|title| title.len() > 4096)
        {
            return Err(HistoryError::InvalidRequest(
                "invalid session update".into(),
            ));
        }
        Ok(())
    }
}

/// Child execution journals belong to a session, not independent catalog sessions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum SessionJournal {
    Root(SessionId),
    Child {
        session_id: SessionId,
        run_id: String,
    },
}
impl SessionJournal {
    pub fn session_id(&self) -> &SessionId {
        match self {
            Self::Root(id) => id,
            Self::Child { session_id, .. } => session_id,
        }
    }
    /// Compatibility key only; possession is not authorization to access a session.
    pub fn storage_key(&self) -> Result<String> {
        match self {
            Self::Root(id) => Ok(format!("session/{id}")),
            Self::Child { session_id, run_id } => {
                if run_id.is_empty() || run_id.len() > 1024 || run_id.contains('\0') {
                    return Err(HistoryError::InvalidRequest("invalid child run ID".into()));
                }
                Ok(format!("session/{session_id}/child/{run_id}"))
            }
        }
    }
}

/// Verified source assignment supplied by the assembly, never inferred from stored text.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LegacySessionImport {
    pub source: JournalKeyInventory,
    pub origin: SessionOrigin,
    /// `id` is required so catalog creation and migration retries have one stable identity.
    pub session: CreateSession,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LegacySessionReceipt {
    pub session: Session,
    pub turns: u64,
    pub compacts: u64,
    pub receipts: u64,
    pub duplicate: bool,
}

/// Existing native ingress receipt, not authorization to access the bound session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestBinding {
    pub journal: SessionJournal,
    pub digest: String,
    pub batch_digest: String,
    pub receipt: AppendReceipt,
}

#[async_trait]
/// Typed journal IDs and cursors belong to one SessionJournal and survive promotion.
/// Legacy HistoryStore methods retain their separate physical-ID compatibility contract.
pub trait SessionStore: HistoryStore {
    fn persistence(&self) -> Persistence;
    async fn create_session(&self, request: CreateSession) -> Result<Session>;
    async fn get_session(&self, id: &SessionId) -> Result<Option<Session>>;
    /// Typed roots only; ambiguous matches fail closed. The assembly serializes
    /// lookup/admission/append with a native-request gate, not a global SQL constraint.
    async fn find_request(
        &self,
        _namespace: &str,
        _request_id: &str,
    ) -> Result<Option<RequestBinding>> {
        Err(HistoryError::Unsupported("native request lookup"))
    }
    async fn get_default_session(&self, origin: &SessionOrigin) -> Result<Option<Session>>;
    async fn ensure_default_session(
        &self,
        origin: &SessionOrigin,
        request: CreateSession,
    ) -> Result<Session>;
    async fn list_sessions(&self, filter: &SessionFilter, page: PageRequest)
    -> Result<SessionPage>;
    async fn update_session(&self, id: &SessionId, update: SessionUpdate) -> Result<Session>;
    async fn append_session(
        &self,
        journal: &SessionJournal,
        request: &AppendRequest,
    ) -> Result<AppendReceipt>;
    async fn append_session_aliased(
        &self,
        journal: &SessionJournal,
        request: &AppendRequest,
        aliases: &[RequestAlias],
    ) -> Result<AppendReceipt> {
        if !aliases.is_empty() {
            return Err(HistoryError::Unsupported("atomic request aliases"));
        }
        self.append_session(journal, request).await
    }
    async fn session_page(
        &self,
        journal: &SessionJournal,
        page: PageRequest,
    ) -> Result<JournalPage>;
    async fn session_context(&self, journal: &SessionJournal) -> Result<ContextWindow>;
    async fn save_session_compact(
        &self,
        journal: &SessionJournal,
        previous_id: Option<i64>,
        through_id: i64,
        content: &str,
    ) -> Result<bool>;
    async fn load_session(&self, journal: &SessionJournal) -> Result<Vec<Turn>>;
}
