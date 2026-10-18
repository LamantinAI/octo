#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::{
    env::current_dir,
    fs::{DirBuilder, OpenOptions, metadata, symlink_metadata},
    io::ErrorKind,
    path::{Component, Path, PathBuf},
};

use chrono::Utc;
use sqlx::{Connection, Row, SqliteConnection, query, raw_sql};
use uuid::Uuid;

use super::Database;
use crate::error::{SqlError, invalid};

const MIGRATION: &str = "
CREATE TABLE IF NOT EXISTS __octo_sql_migrations (version INTEGER PRIMARY KEY);
CREATE TABLE IF NOT EXISTS __octo_sql_identity (singleton INTEGER PRIMARY KEY CHECK(singleton=1), id TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS __octo_sql_stores (
 id TEXT PRIMARY KEY, owner TEXT NOT NULL, name TEXT NOT NULL, purpose TEXT NOT NULL,
 tags TEXT NOT NULL, archived INTEGER NOT NULL DEFAULT 0, revision INTEGER NOT NULL,
 fingerprint TEXT NOT NULL, metadata TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS __octo_sql_operations (
 id TEXT PRIMARY KEY, hash TEXT NOT NULL, principal TEXT NOT NULL, store_id TEXT NOT NULL,
 prepared TEXT NOT NULL, scope TEXT NOT NULL, status TEXT NOT NULL,
 expires_at INTEGER NOT NULL, result TEXT, idempotency_key TEXT NOT NULL,
 request_hash TEXT NOT NULL, UNIQUE(principal,store_id,idempotency_key));
CREATE TABLE IF NOT EXISTS __octo_sql_audit (
 id INTEGER PRIMARY KEY, operation_id TEXT NOT NULL, principal TEXT NOT NULL,
 outcome TEXT NOT NULL, detail TEXT NOT NULL, created_at INTEGER NOT NULL, actor TEXT);
CREATE TABLE IF NOT EXISTS __octo_sql_receipts (
 operation_id TEXT PRIMARY KEY, hash TEXT NOT NULL, result TEXT NOT NULL);
";

impl Database {
    pub(crate) async fn cancel_pending(&self, ids: &[String]) -> Result<(), SqlError> {
        if ids.is_empty() {
            return Ok(());
        }
        let mut conn = self.connection.lock().await;
        self.engine.validate_identity()?;
        let mut transaction = conn.begin_with("BEGIN IMMEDIATE").await?;
        for id in ids {
            let result = query("UPDATE __octo_sql_operations SET status='cancelled' WHERE id=? AND status='pending'")
                .bind(id).execute(&mut *transaction).await?;
            if result.rows_affected() > 0 {
                query("INSERT INTO __octo_sql_audit(operation_id,principal,outcome,detail,created_at) SELECT id,principal,'cancelled',hash,? FROM __octo_sql_operations WHERE id=?")
                    .bind(Utc::now().timestamp()).bind(id).execute(&mut *transaction).await?;
            }
        }
        transaction.commit().await?;
        Ok(())
    }
}

pub(super) async fn validate_ownership(conn: &mut SqliteConnection) -> Result<(), SqlError> {
    let objects = query("SELECT name FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'")
        .fetch_all(conn)
        .await?;
    let marked = objects
        .iter()
        .any(|row| row.get::<String, _>("name") == "__octo_sql_identity");
    if !objects.is_empty() && !marked {
        return Err(SqlError::Authority(
            "existing SQLite file belongs to another application; automatic adoption is disabled"
                .into(),
        ));
    }
    Ok(())
}
pub(super) async fn migrate(conn: &mut SqliteConnection) -> Result<(), SqlError> {
    let mut transaction = conn.begin_with("BEGIN IMMEDIATE").await?;
    raw_sql(MIGRATION).execute(&mut *transaction).await?;
    let version: Option<i64> = query("SELECT max(version) FROM __octo_sql_migrations")
        .fetch_one(&mut *transaction)
        .await?
        .get(0);
    if version.is_some_and(|v| v != 1) {
        return Err(invalid("unsupported protected schema migration version"));
    }
    query("INSERT OR IGNORE INTO __octo_sql_migrations(version) VALUES(1)")
        .execute(&mut *transaction)
        .await?;
    query("INSERT OR IGNORE INTO __octo_sql_identity(singleton,id) VALUES(1,?)")
        .bind(Uuid::now_v7().to_string())
        .execute(&mut *transaction)
        .await?;
    query("UPDATE __octo_sql_operations SET status='unknown' WHERE status='claimed'")
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await?;
    Ok(())
}
pub(super) async fn database_identity(conn: &mut SqliteConnection) -> Result<String, SqlError> {
    Ok(
        query("SELECT id FROM __octo_sql_identity WHERE singleton=1")
            .fetch_one(conn)
            .await?
            .get(0),
    )
}
fn resolved(path: &Path) -> Result<PathBuf, SqlError> {
    if path.as_os_str().is_empty() {
        return Err(invalid("database path needs a file name"));
    }
    let absolute = if path.is_absolute() {
        path.to_owned()
    } else {
        current_dir().map_err(invalid)?.join(path)
    };
    let mut resolved = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                resolved.pop();
            }
            Component::Prefix(_) | Component::RootDir => resolved.push(component.as_os_str()),
            Component::Normal(name) => {
                resolved.push(name);
                match symlink_metadata(&resolved) {
                    Ok(_) => resolved = resolved.canonicalize().map_err(invalid)?,
                    Err(error) if error.kind() == ErrorKind::NotFound => {}
                    Err(error) => return Err(invalid(error)),
                }
            }
        }
    }
    Ok(resolved)
}
/// Resolve existing symlinks before mkdir, then check again after canonicalizing
/// the new parent/file. Existing directories and files retain their permissions.
pub(super) fn prepare_path(path: &Path, protected: &[PathBuf]) -> Result<PathBuf, SqlError> {
    let candidate = resolved(path)?;
    check_protected(&candidate, protected)?;
    let name = candidate
        .file_name()
        .ok_or_else(|| invalid("database path needs a file name"))?;
    let parent = candidate
        .parent()
        .ok_or_else(|| invalid("database parent required"))?;
    let mut builder = DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    builder.mode(0o700);
    builder.create(parent).map_err(invalid)?;
    let candidate = parent.canonicalize().map_err(invalid)?.join(name);
    check_protected(&candidate, protected)?;
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    match options.open(&candidate) {
        Ok(_) => {}
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
        Err(error) => return Err(invalid(error)),
    }
    let canonical = candidate.canonicalize().map_err(invalid)?;
    check_protected(&canonical, protected)?;
    Ok(canonical)
}
pub(super) fn check_protected(path: &Path, protected: &[PathBuf]) -> Result<(), SqlError> {
    let candidate = resolved(path)?;
    for internal in protected {
        let internal = resolved(internal)?;
        if candidate.starts_with(&internal) {
            return Err(SqlError::Authority(
                "connector path is an internal database".into(),
            ));
        }
        #[cfg(unix)]
        if let (Ok(left), Ok(right)) = (metadata(&candidate), metadata(&internal)) {
            if left.dev() == right.dev() && left.ino() == right.ino() {
                return Err(SqlError::Authority(
                    "connector file aliases an internal database".into(),
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::{
        fs::{Permissions, set_permissions},
        os::unix::fs::{PermissionsExt, symlink},
    };
    use std::{
        fs::{create_dir, metadata},
        time::Duration,
    };

    use octo_approval::Authority;
    use serde_json::json;
    use sqlx::{Connection, Row, SqliteConnection, query, sqlite::SqliteConnectOptions};
    use tempfile::tempdir;

    use crate::{Database, Limits, database::tests::Fixture};

    #[tokio::test]
    async fn foreign_sqlite_is_never_adopted_or_modified() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("foreign.db");
        let options = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true);
        let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
        query("CREATE TABLE s_abcd__foreign(secret TEXT)")
            .execute(&mut connection)
            .await
            .unwrap();
        query("INSERT INTO s_abcd__foreign VALUES('keep')")
            .execute(&mut connection)
            .await
            .unwrap();
        let authority = Authority::new(Duration::from_secs(60), 10);
        assert!(
            Database::open(&path, Limits::default(), authority, vec![])
                .await
                .is_err()
        );
        let names: Vec<String> = query("SELECT name FROM sqlite_schema WHERE type='table'")
            .fetch_all(&mut connection)
            .await
            .unwrap()
            .iter()
            .map(|r| r.get(0))
            .collect();
        assert_eq!(names, vec!["s_abcd__foreign"]);
        assert_eq!(
            query("SELECT secret FROM s_abcd__foreign")
                .fetch_one(&mut connection)
                .await
                .unwrap()
                .get::<String, _>(0),
            "keep"
        );
    }
    #[tokio::test]
    async fn reopening_claimed_work_keeps_unknown_and_never_replays() {
        let f = Fixture::new(Limits::default()).await;
        let store = f.store().await;
        let table = f.table(&store).await;
        let pending=f.call("sql.execute",json!({"store_id":store,"expected_revision":2,"sql":format!("DELETE FROM {table}"),"idempotency_key":"delete"})).await.unwrap();
        let operation = pending["operation_id"].as_str().unwrap();
        let mut conn = f.db.connection.lock().await;
        query("UPDATE __octo_sql_operations SET status='claimed' WHERE id=?")
            .bind(operation)
            .execute(&mut *conn)
            .await
            .unwrap();
        drop(conn);
        let reopened = Database::open(
            f.dir.path().join("sql.db"),
            Limits::default(),
            f.authority.clone(),
            vec![],
        )
        .await
        .unwrap();
        let decision = f.envelope(
            "sql.approval.decide",
            json!({"operation_id":operation,"approve":true}),
            true,
            "owner",
        );
        assert_eq!(
            reopened.handle(&decision).await.unwrap()["status"],
            "unknown"
        );
        let mut conn = reopened.connection.lock().await;
        assert_eq!(
            query("SELECT count(*) FROM __octo_sql_receipts WHERE operation_id=?")
                .bind(operation)
                .fetch_one(&mut *conn)
                .await
                .unwrap()
                .get::<i64, _>(0),
            0
        );
    }
    #[tokio::test]
    async fn cancelled_pending_work_stays_cancelled_after_reopening() {
        let f = Fixture::new(Limits::default()).await;
        let store = f.store().await;
        let table = f.table(&store).await;
        let pending=f.call("sql.execute",json!({"store_id":store,"expected_revision":2,"sql":format!("DELETE FROM {table}"),"idempotency_key":"cancel"})).await.unwrap();
        let operation = pending["operation_id"].as_str().unwrap().to_owned();
        f.db.cancel_pending(&[operation.clone()]).await.unwrap();
        let reopened = Database::open(
            f.dir.path().join("sql.db"),
            Limits::default(),
            f.authority.clone(),
            vec![],
        )
        .await
        .unwrap();
        assert_eq!(
            reopened
                .handle(&f.envelope(
                    "sql.approval.decide",
                    json!({"operation_id":operation,"approve":true}),
                    true,
                    "owner"
                ))
                .await
                .unwrap()["status"],
            "cancelled"
        );
    }
    #[tokio::test]
    async fn missing_nested_parent_and_file_initialize_privately_and_reopen_preserves_rows() {
        let f = Fixture::new(Limits::default()).await;
        let public = f.dir.path().join("existing-parent");
        create_dir(&public).unwrap();
        #[cfg(unix)]
        set_permissions(&public, Permissions::from_mode(0o755)).unwrap();
        let path = public.join("new/private/data.sqlite");
        let db = Database::open(&path, Limits::default(), f.authority.clone(), vec![])
            .await
            .unwrap();
        assert!(path.is_file());
        #[cfg(unix)]
        {
            assert_eq!(
                metadata(&public).unwrap().permissions().mode() & 0o777,
                0o755
            );
            assert_eq!(
                metadata(public.join("new")).unwrap().permissions().mode() & 0o777,
                0o700
            );
            assert_eq!(
                metadata(public.join("new/private"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
            assert_eq!(metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
        }
        let store = db
            .handle(&f.envelope(
                "sql.store.create",
                json!({"name":"nested","idempotency_key":"create"}),
                false,
                "owner",
            ))
            .await
            .unwrap()["store_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let table = format!("s_{store}__data");
        for (revision, key, sql) in [
            (1, "table", format!("CREATE TABLE {table}(value INTEGER)")),
            (2, "row", format!("INSERT INTO {table} VALUES(7)")),
        ] {
            assert_eq!(db.handle(&f.envelope("sql.execute",json!({"store_id":store,"expected_revision":revision,"idempotency_key":key,"sql":sql}),false,"owner")).await.unwrap()["status"],"committed");
        }
        let reopened = Database::open(&path, Limits::default(), f.authority.clone(), vec![])
            .await
            .unwrap();
        let result=reopened.handle(&f.envelope("sql.execute",json!({"store_id":store,"expected_revision":3,"sql":format!("SELECT value FROM {table}")}),false,"owner")).await.unwrap();
        assert_eq!(
            result["result"]["rows"][0][0],
            json!({"type":"integer","value":7})
        );
    }
    #[tokio::test]
    async fn protected_missing_descendants_are_rejected_before_mkdir() {
        let dir = tempdir().unwrap();
        let protected = dir.path().join("protected-not-created");
        let path = protected.join("nested/deep/sql.db");
        let authority = Authority::new(Duration::from_secs(60), 10);
        assert!(
            Database::open(path, Limits::default(), authority, vec![protected.clone()])
                .await
                .is_err()
        );
        assert!(!protected.exists());
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn symlink_then_parent_component_cannot_escape_protected_check() {
        let dir = tempdir().unwrap();
        let protected = dir.path().join("protected");
        let branch = protected.join("branch");
        create_dir(&protected).unwrap();
        create_dir(&branch).unwrap();
        let alias = dir.path().join("alias");
        symlink(&branch, &alias).unwrap();
        let authority = Authority::new(Duration::from_secs(60), 10);
        assert!(
            Database::open(
                alias.join("../missing/deep/sql.db"),
                Limits::default(),
                authority,
                vec![protected.clone()]
            )
            .await
            .is_err()
        );
        assert!(!protected.join("missing").exists());
        assert!(!dir.path().join("missing").exists());
    }
}
