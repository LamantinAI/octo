#![cfg(feature = "sqlite")]

use std::slice::from_ref;

use octo_history::{
    AppendRequest, CreateSession, HistoryStore, KeyRemap, LegacySessionImport, PageRequest,
    SessionAccess, SessionFilter, SessionId, SessionJournal, SessionKind, SessionOrigin,
    SessionStore, SqliteHistory, Turn,
};
use sqlx::{query, query_as, sqlite::SqlitePoolOptions};

async fn store() -> SqliteHistory {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    SqliteHistory::from_pool(pool).await.unwrap()
}
fn origin() -> SessionOrigin {
    SessionOrigin {
        connector: "telegram".into(),
        channel: "42".into(),
    }
}
fn private() -> CreateSession {
    let mut request = CreateSession::private("verified-owner");
    request.id = Some(SessionId::new());
    request
}
fn request() -> AppendRequest {
    AppendRequest {
        namespace: "telegram:42".into(),
        idempotency_key: "message-1".into(),
        digest: "hash".into(),
        turns: vec![Turn::user("preserve me")],
    }
}

#[tokio::test]
async fn legacy_root_and_child_import_keep_distinct_journals_and_original_default() {
    let h = store().await;
    let receipt = h.append_journal("42", &request()).await.unwrap();
    h.save_compact("42", None, receipt.through_id, "compact")
        .await
        .unwrap();
    h.append(
        "subagent/albert/child-1",
        &[
            Turn::user("recorded child origin"),
            Turn::assistant("child result"),
        ],
    )
    .await
    .unwrap();
    let inventory = h.journal_inventory().await.unwrap();
    let source = inventory.iter().find(|i| i.key == "42").unwrap().clone();
    let child_source = inventory
        .iter()
        .find(|i| i.key.starts_with("subagent/"))
        .unwrap()
        .clone();
    let import = LegacySessionImport {
        source,
        origin: origin(),
        session: private(),
    };
    let imported = h
        .import_legacy_sessions(from_ref(&import))
        .await
        .unwrap()
        .remove(0);
    assert_eq!(
        (imported.turns, imported.compacts, imported.receipts),
        (1, 1, 1)
    );
    let root = SessionJournal::Root(imported.session.id.clone());
    assert_eq!(
        h.session_page(&root, PageRequest::default())
            .await
            .unwrap()
            .records[0]
            .id,
        receipt.through_id
    );
    assert!(h.append_session(&root, &request()).await.unwrap().duplicate);
    assert_eq!(
        h.session_context(&root)
            .await
            .unwrap()
            .compact
            .unwrap()
            .content,
        "compact"
    );
    let remapped = h
        .import_child_journal(&imported.session.id, "albert/child-1", &child_source)
        .await
        .unwrap();
    assert_eq!(remapped.turns, 2);
    let child = SessionJournal::Child {
        session_id: imported.session.id.clone(),
        run_id: "albert/child-1".into(),
    };
    assert_eq!(
        h.load_session(&child).await.unwrap()[1].content,
        "child result"
    );
    assert_eq!(h.load_session(&root).await.unwrap().len(), 1);
    assert!(
        h.import_child_journal(&imported.session.id, "albert/child-1", &child_source)
            .await
            .unwrap()
            .duplicate
    );
    assert!(h.import_legacy_sessions(&[import]).await.unwrap()[0].duplicate);
    h.create_session(private()).await.unwrap();
    assert_eq!(
        h.get_default_session(&origin()).await.unwrap().unwrap().id,
        imported.session.id
    );
    assert_eq!(
        h.list_sessions(
            &SessionFilter::PrivateOwner("verified-owner".into()),
            PageRequest::default()
        )
        .await
        .unwrap()
        .sessions
        .len(),
        2
    );
    assert!(
        query("PRAGMA foreign_key_check")
            .fetch_all(h.pool())
            .await
            .unwrap()
            .is_empty()
    );
    let root_inventory = h
        .journal_inventory()
        .await
        .unwrap()
        .into_iter()
        .find(|i| i.key == root.storage_key().unwrap())
        .unwrap();
    assert!(
        h.remap_journal_keys(&[KeyRemap {
            source: root_inventory,
            target: "unowned".into()
        }])
        .await
        .is_err()
    );
}

#[tokio::test]
async fn import_stale_origin_and_shared_transaction_failures_leave_no_partial_catalog() {
    let h = store().await;
    h.append("42", &[Turn::user("before")]).await.unwrap();
    let source = h.journal_inventory().await.unwrap().remove(0);
    let import = LegacySessionImport {
        source,
        origin: origin(),
        session: private(),
    };
    h.append("42", &[Turn::user("inventory changed")])
        .await
        .unwrap();
    assert!(h.import_legacy_sessions(from_ref(&import)).await.is_err());
    assert!(
        h.get_session(import.session.id.as_ref().unwrap())
            .await
            .unwrap()
            .is_none()
    );
    assert!(h.get_default_session(&origin()).await.unwrap().is_none());
    let mut valid = import;
    valid.source = h.journal_inventory().await.unwrap().remove(0);
    query("CREATE TABLE application_defaults (channel TEXT PRIMARY KEY)")
        .execute(h.pool())
        .await
        .unwrap();
    let mut tx = h.begin_journal().await.unwrap();
    h.import_legacy_sessions_in(&mut tx, &[valid.clone()])
        .await
        .unwrap();
    query("INSERT INTO application_defaults VALUES ('42')")
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert!(h.get_default_session(&origin()).await.unwrap().is_none());
    assert_eq!(h.load("42").await.len(), 2);
    let (count,): (i64,) = query_as("SELECT COUNT(*) FROM application_defaults")
        .fetch_one(h.pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
    h.import_legacy_sessions(&[valid]).await.unwrap();
    let wrong = LegacySessionImport {
        source: h.journal_inventory().await.unwrap().remove(0),
        origin: SessionOrigin {
            connector: "telegram".into(),
            channel: "wrong".into(),
        },
        session: CreateSession {
            id: Some(SessionId::new()),
            access: SessionAccess::GroupBound {
                connector: "telegram".into(),
                channel: "-42".into(),
            },
            host: "host".into(),
            title: "group".into(),
            kind: SessionKind::Permanent,
        },
    };
    assert!(h.import_legacy_sessions(from_ref(&wrong)).await.is_err());
    assert!(
        h.get_session(wrong.session.id.as_ref().unwrap())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        h.get_default_session(&SessionOrigin {
            connector: "telegram".into(),
            channel: "-42".into()
        })
        .await
        .unwrap()
        .is_none()
    );
}

#[tokio::test]
async fn empty_default_and_catalog_metadata_rollback_together() {
    let h = store().await;
    let request = private();
    let mut tx = h.begin_journal().await.unwrap();
    h.ensure_default_session_in(&mut tx, &origin(), request.clone())
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert!(h.get_default_session(&origin()).await.unwrap().is_none());
    assert!(
        h.get_session(request.id.as_ref().unwrap())
            .await
            .unwrap()
            .is_none()
    );
    let created = h
        .ensure_default_session(&origin(), request.clone())
        .await
        .unwrap();
    assert_eq!(created.id, request.id.unwrap());
    let child = SessionJournal::Child {
        session_id: created.id,
        run_id: "albert/child:opaque".into(),
    };
    let mut tx = h.begin_journal().await.unwrap();
    h.append_session_in(&mut tx, &child, &request_journal())
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert!(h.session_context(&child).await.is_err());
}

fn request_journal() -> AppendRequest {
    request()
}

#[tokio::test]
async fn simultaneous_first_ingress_resolves_one_default_session() {
    use std::time::Duration;
    use tempfile::tempdir;
    use tokio::{join, time::timeout};
    let dir = tempdir().unwrap();
    let h = SqliteHistory::open_retained(dir.path().join("history.db"))
        .await
        .unwrap();
    let origin = origin();
    let (a, b) = timeout(Duration::from_secs(10), async {
        join!(
            h.ensure_default_session(&origin, CreateSession::private("owner")),
            h.ensure_default_session(&origin, CreateSession::private("owner"))
        )
    })
    .await
    .unwrap();
    assert_eq!(a.unwrap().id, b.unwrap().id);
    assert_eq!(
        h.list_sessions(
            &SessionFilter::PrivateOwner("owner".into()),
            PageRequest::default()
        )
        .await
        .unwrap()
        .sessions
        .len(),
        1
    );
}
