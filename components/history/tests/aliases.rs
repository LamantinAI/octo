#[cfg(feature = "sqlite")]
use octo_history::SessionKind;
use octo_history::{
    AppendRequest, CreateSession, HistoryError, MemoryLimits, RequestAlias, RetainedMemoryHistory,
    SessionJournal, SessionStore, Turn,
};

fn batch() -> AppendRequest {
    AppendRequest {
        namespace: "aggregate".into(),
        idempotency_key: "batch-1".into(),
        digest: "aggregate semantics".into(),
        turns: vec![
            Turn::user("forward A\nforward B"),
            Turn::assistant("journal"),
        ],
    }
}
fn aliases() -> Vec<RequestAlias> {
    vec![
        RequestAlias {
            namespace: "native".into(),
            idempotency_key: "first".into(),
            digest: "first semantics".into(),
        },
        RequestAlias {
            namespace: "native".into(),
            idempotency_key: "second".into(),
            digest: "second semantics".into(),
        },
    ]
}
async fn contract(store: &dyn SessionStore) {
    let session = store
        .create_session(CreateSession::private("owner"))
        .await
        .unwrap();
    let root = SessionJournal::Root(session.id);
    let first = store
        .append_session_aliased(&root, &batch(), &aliases())
        .await
        .unwrap();
    let a = store
        .find_request("native", "first")
        .await
        .unwrap()
        .unwrap();
    let b = store
        .find_request("native", "second")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(a.receipt.record_ids, first.record_ids);
    assert_eq!(b.receipt.record_ids, first.record_ids);
    assert_eq!(a.batch_digest, b.batch_digest);
    assert_ne!(a.digest, b.digest);
    assert!(
        store
            .append_session_aliased(&root, &batch(), &aliases())
            .await
            .unwrap()
            .duplicate
    );
    assert_eq!(store.load_session(&root).await.unwrap().len(), 2);
    let mut conflict = aliases();
    conflict[1].digest = "changed".into();
    conflict.insert(
        0,
        RequestAlias {
            namespace: "native".into(),
            idempotency_key: "new first".into(),
            digest: "new".into(),
        },
    );
    assert!(matches!(
        store
            .append_session_aliased(&root, &batch(), &conflict)
            .await,
        Err(HistoryError::IdempotencyConflict)
    ));
    assert!(
        store
            .find_request("native", "new first")
            .await
            .unwrap()
            .is_none()
    );
    let different = AppendRequest {
        idempotency_key: "another batch".into(),
        ..batch()
    };
    assert!(
        store
            .append_session_aliased(&root, &different, &aliases())
            .await
            .is_err()
    );
    assert!(
        store
            .find_request("aggregate", "another batch")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(store.load_session(&root).await.unwrap().len(), 2);
}

#[tokio::test]
async fn ram_aliases_are_one_atomic_batch_and_quota_failure_keeps_no_receipts() {
    contract(&RetainedMemoryHistory::default()).await;
    let ram = RetainedMemoryHistory::new(MemoryLimits {
        max_sessions: 2,
        max_turns: 10,
        max_bytes: 2000,
    })
    .unwrap();
    let session = ram
        .create_session(CreateSession::private("owner"))
        .await
        .unwrap();
    let root = SessionJournal::Root(session.id);
    let input = AppendRequest {
        turns: vec![Turn::user("x".repeat(1000))],
        ..batch()
    };
    assert!(matches!(
        ram.append_session_aliased(&root, &input, &aliases()).await,
        Err(HistoryError::QuotaExceeded)
    ));
    assert!(ram.load_session(&root).await.unwrap().is_empty());
    assert!(ram.find_request("native", "first").await.unwrap().is_none());
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_aliases_and_promotion_preserve_constituent_receipts() {
    use octo_history::SqliteHistory;
    use sqlx::{query, sqlite::SqlitePoolOptions};
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    let sql = SqliteHistory::from_pool(pool).await.unwrap();
    contract(&sql).await;
    let ram = RetainedMemoryHistory::default();
    let mut create = CreateSession::private("temp owner");
    create.kind = SessionKind::Temporary;
    let session = ram.create_session(create).await.unwrap();
    let root = SessionJournal::Root(session.id.clone());
    let mut input = batch();
    input.namespace = "temp aggregate".into();
    let mut identities = aliases();
    for alias in &mut identities {
        alias.namespace = "temp native".into();
    }
    ram.append_session_aliased(&root, &input, &identities)
        .await
        .unwrap();
    let snapshot = ram.export_session(&session.id).unwrap();
    let mut tx = sql.begin_journal().await.unwrap();
    sql.promote_session_in(&mut tx, &snapshot, "save aliases")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    for alias in identities {
        assert_eq!(
            ram.find_request(&alias.namespace, &alias.idempotency_key)
                .await
                .unwrap(),
            sql.find_request(&alias.namespace, &alias.idempotency_key)
                .await
                .unwrap()
        );
    }
    let discarded = ram
        .discard_session(&session.id, snapshot.fingerprint())
        .unwrap();
    assert_eq!((discarded.records, discarded.receipts), (2, 3));
    assert!(
        query("PRAGMA foreign_key_check")
            .fetch_all(sql.pool())
            .await
            .unwrap()
            .is_empty()
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sql_alias_write_failure_rolls_back_primary_and_earlier_aliases() {
    use octo_history::SqliteHistory;
    use sqlx::{query, sqlite::SqlitePoolOptions};
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    let sql = SqliteHistory::from_pool(pool).await.unwrap();
    let session = sql
        .create_session(CreateSession::private("owner"))
        .await
        .unwrap();
    let root = SessionJournal::Root(session.id);
    query("CREATE TRIGGER fail_alias BEFORE INSERT ON journal_appends WHEN NEW.namespace='native' AND NEW.idempotency_key='second' BEGIN SELECT RAISE(ABORT,'injected failure'); END").execute(sql.pool()).await.unwrap();
    let mut tx = sql.begin_journal().await.unwrap();
    assert!(
        sql.append_session_aliased_in(&mut tx, &root, &batch(), &aliases())
            .await
            .is_err()
    );
    tx.commit().await.unwrap();
    assert!(sql.load_session(&root).await.unwrap().is_empty());
    assert!(
        sql.find_request("aggregate", "batch-1")
            .await
            .unwrap()
            .is_none()
    );
    assert!(sql.find_request("native", "first").await.unwrap().is_none());
}
