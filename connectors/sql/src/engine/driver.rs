#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::{
    fs::{Metadata, metadata},
    path::{Path, PathBuf},
};

use sqlx::{Connection, SqliteConnection, query, sqlite::SqliteConnectOptions};

use super::{
    Analysis, Cancellation, EngineError, ExecutionPermit, Policy, QueryResult, Settings, SqlParam,
    ast::analyze,
    native::{analyze as native_analyze, execute as native_execute},
};

#[derive(Debug, Clone)]
pub struct Engine {
    path: PathBuf,
    settings: Settings,
    identity: Metadata,
}
impl Engine {
    /// The trusted host supplies the connector's dedicated file path.
    pub async fn open(path: impl AsRef<Path>, settings: Settings) -> Result<Self, EngineError> {
        settings.native.validate()?;
        let path = path.as_ref();
        if path.as_os_str().is_empty() || path == Path::new(":memory:") {
            return Err(EngineError::WrongDatabase);
        }
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .foreign_keys(true)
            .statement_cache_capacity(0)
            .busy_timeout(settings.timeout);
        let mut conn = SqliteConnection::connect_with(&options).await?;
        // Native REPLACE DELETE hooks need recursive triggers enabled.
        query("PRAGMA recursive_triggers=ON")
            .execute(&mut conn)
            .await?;
        conn.close().await?;
        Ok(Self {
            path: path.canonicalize()?,
            identity: metadata(path)?,
            settings,
        })
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
    pub(crate) fn validate_identity(&self) -> Result<(), EngineError> {
        let actual = metadata(&self.path)?;
        #[cfg(unix)]
        if actual.dev() != self.identity.dev() || actual.ino() != self.identity.ino() {
            return Err(EngineError::WrongDatabase);
        }
        #[cfg(not(unix))]
        if actual.created().ok() != self.identity.created().ok() {
            return Err(EngineError::WrongDatabase);
        }
        Ok(())
    }
    pub async fn connect(&self) -> Result<SqliteConnection, EngineError> {
        self.validate_identity()?;
        let options = SqliteConnectOptions::new()
            .filename(&self.path)
            .foreign_keys(true)
            .statement_cache_capacity(0)
            .busy_timeout(self.settings.timeout);
        let mut conn = SqliteConnection::connect_with(&options).await?;
        query("PRAGMA recursive_triggers=ON")
            .execute(&mut conn)
            .await?;
        Ok(conn)
    }
    pub async fn analyze(
        &self,
        conn: &mut SqliteConnection,
        policy: &Policy,
        sql: &str,
    ) -> Result<Analysis, EngineError> {
        self.validate_identity()?;
        let analysis = analyze(sql, policy, &self.settings.native)?;
        let mut handle = conn.lock_handle().await?;
        native_analyze(
            handle.as_raw_handle().as_ptr(),
            &self.path,
            policy,
            sql,
            analysis,
            &self.settings,
        )
    }
    /// Caller MUST roll back its enclosing transaction on every error. A private
    /// savepoint also ensures failed statements cannot leave partial user effects.
    pub async fn execute(
        &self,
        conn: &mut SqliteConnection,
        policy: &Policy,
        sql: &str,
        params: &[SqlParam],
        permit: ExecutionPermit,
        cancellation: Cancellation,
    ) -> Result<QueryResult, EngineError> {
        self.validate_identity()?;
        let analysis = analyze(sql, policy, &self.settings.native)?;
        let mut handle = conn.lock_handle().await?;
        native_execute(
            handle.as_raw_handle().as_ptr(),
            &self.path,
            policy,
            sql,
            params,
            analysis,
            &self.settings,
            permit.approved,
            cancellation,
        )
    }
}
