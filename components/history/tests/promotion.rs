#![cfg(feature = "sqlite")]

use octo_history::{
    AppendRequest, ContextWindow, CreateSession, HistoryError, HistoryStore, PageRequest,
    RetainedMemoryHistory, Role, SessionAccess, SessionId, SessionJournal, SessionKind,
    SessionSnapshot, SessionStore, SessionUpdate, SqliteHistory, Turn,
};
use serde_json::to_value;
use sqlx::{query, query_as, sqlite::SqlitePoolOptions};

fn request(namespace: &str, key: &str, text: &str) -> AppendRequest {
    AppendRequest {
        namespace: namespace.into(),
        idempotency_key: key.into(),
        digest: "request digest".into(),
        turns: vec![Turn::user(text)],
    }
}
async fn store() -> SqliteHistory {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    SqliteHistory::from_pool(pool).await.unwrap()
}
async fn temporary(ram: &RetainedMemoryHistory) -> SessionId {
    let mut request = CreateSession::private("verified owner");
    request.kind = SessionKind::Temporary;
    request.title = "Initial title".into();
    ram.create_session(request).await.unwrap().id
}
type ContextIdentity = (Option<(i64, i64, String)>, Vec<(i64, Role, String)>);
fn identity(context: ContextWindow) -> ContextIdentity {
    (
        context.compact.map(|c| (c.id, c.through_id, c.content)),
        context
            .messages
            .into_iter()
            .map(|m| (m.id, m.turn.role, m.turn.content))
            .collect(),
    )
}
async fn complex_snapshot(
    ram: &RetainedMemoryHistory,
) -> (SessionId, SessionJournal, SessionSnapshot) {
    let unrelated = ram
        .create_session(CreateSession::private("other"))
        .await
        .unwrap();
    ram.append_session(
        &SessionJournal::Root(unrelated.id),
        &request("other", "1", "unrelated counter"),
    )
    .await
    .unwrap();
    let id = temporary(ram).await;
    let root = SessionJournal::Root(id.clone());
    for index in 0..4 {
        ram.append_session(
            &root,
            &request("tg", &index.to_string(), &format!("root {index}")),
        )
        .await
        .unwrap();
    }
    let boundary = ram
        .session_page(&root, PageRequest::default())
        .await
        .unwrap()
        .through_id;
    ram.save_session_compact(&root, None, boundary, "first root compact")
        .await
        .unwrap();
    let child = SessionJournal::Child {
        session_id: id.clone(),
        run_id: "albert/child:opaque".into(),
    };
    let child_boundary = ram
        .append_session(&child, &request("tg", "child", "child result"))
        .await
        .unwrap()
        .through_id;
    ram.save_session_compact(&child, None, child_boundary, "child compact")
        .await
        .unwrap();
    let previous = ram
        .session_context(&root)
        .await
        .unwrap()
        .compact
        .unwrap()
        .id;
    let boundary = ram
        .append_session(&root, &request("ui", "same key", "second root pass"))
        .await
        .unwrap()
        .through_id;
    ram.save_session_compact(&root, Some(previous), boundary, "second root compact")
        .await
        .unwrap();
    ram.append_session(&root, &request("tg", "same key", "unsummarized tail"))
        .await
        .unwrap();
    ram.update_session(
        &id,
        SessionUpdate {
            expected_revision: 1,
            title: Some("Retained title".into()),
            archived: None,
        },
    )
    .await
    .unwrap();
    let snapshot = ram.export_session(&id).unwrap();
    (id, child, snapshot)
}

#[tokio::test]
async fn promotion_preserves_public_ids_cursors_context_receipts_and_followup_boundaries() {
    let sql = store().await;
    for index in 0..30 {
        sql.append_journal(
            "legacy rows",
            &request("old", &index.to_string(), "preexisting SQL"),
        )
        .await
        .unwrap();
    }
    let boundary = sql
        .journal_page("legacy rows", PageRequest::default())
        .await
        .unwrap()
        .through_id;
    let mut previous = None;
    for _ in 0..5 {
        sql.save_compact("legacy rows", previous, boundary, "preexisting compact")
            .await
            .unwrap();
        previous = sql
            .context("legacy rows")
            .await
            .unwrap()
            .compact
            .map(|c| c.id);
    }
    let ram = RetainedMemoryHistory::default();
    let (id, child, snapshot) = complex_snapshot(&ram).await;
    let root = SessionJournal::Root(id.clone());
    let page = ram
        .session_page(
            &root,
            PageRequest {
                limit: 2,
                ..PageRequest::default()
            },
        )
        .await
        .unwrap();
    let cursor = PageRequest {
        before_id: page.next_before,
        through_id: Some(page.through_id),
        limit: 2,
    };
    let old_page = ram.session_page(&root, cursor.clone()).await.unwrap();
    let root_before = identity(ram.session_context(&root).await.unwrap());
    let child_before = identity(ram.session_context(&child).await.unwrap());
    let root_full = ram
        .session_page(&root, PageRequest::default())
        .await
        .unwrap();
    let child_full = ram
        .session_page(&child, PageRequest::default())
        .await
        .unwrap();
    assert!(sql.get_session(&id).await.unwrap().is_none());
    let (count,): (i64,) = query_as("SELECT COUNT(*) FROM turns WHERE channel=?")
        .bind(root.storage_key().unwrap())
        .fetch_one(sql.pool())
        .await
        .unwrap();
    assert_eq!(count, 0, "RAM export wrote no SQL transcript");
    let mut tx = sql.begin_journal().await.unwrap();
    let saved = sql
        .promote_session_in(&mut tx, &snapshot, "save request")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        sql.find_request("tg", "0").await.unwrap(),
        ram.find_request("tg", "0").await.unwrap(),
        "promotion keeps the original native request binding"
    );
    assert!(!saved.duplicate);
    assert_eq!(saved.revision, snapshot.session().revision + 1);
    assert_eq!(
        ram.export_session(&id).unwrap().fingerprint(),
        snapshot.fingerprint()
    );
    let permanent = sql.get_session(&id).await.unwrap().unwrap();
    assert_eq!(permanent.kind, SessionKind::Permanent);
    assert_eq!(
        (
            permanent.title,
            permanent.created_at,
            permanent.access,
            permanent.host
        ),
        (
            snapshot.session().title.clone(),
            snapshot.session().created_at.clone(),
            snapshot.session().access.clone(),
            snapshot.session().host.clone()
        )
    );
    assert_eq!(
        to_value(
            sql.session_page(&root, PageRequest::default())
                .await
                .unwrap()
        )
        .unwrap(),
        to_value(root_full).unwrap()
    );
    assert_eq!(
        to_value(
            sql.session_page(&child, PageRequest::default())
                .await
                .unwrap()
        )
        .unwrap(),
        to_value(child_full).unwrap()
    );
    assert_eq!(
        identity(sql.session_context(&root).await.unwrap()),
        root_before
    );
    assert_eq!(
        identity(sql.session_context(&child).await.unwrap()),
        child_before
    );
    assert_eq!(
        to_value(sql.session_page(&root, cursor.clone()).await.unwrap()).unwrap(),
        to_value(&old_page).unwrap()
    );
    let retry = sql
        .append_session(&root, &request("ui", "same key", "second root pass"))
        .await
        .unwrap();
    let original = ram
        .append_session(&root, &request("ui", "same key", "second root pass"))
        .await
        .unwrap();
    assert!(retry.duplicate);
    assert_eq!(retry.record_ids, original.record_ids);
    assert!(matches!(
        sql.append_session(&root, &request("ui", "same key", "different body"))
            .await,
        Err(HistoryError::IdempotencyConflict)
    ));
    let last_public = page.through_id;
    let next = sql
        .append_session(&root, &request("ui", "new", "after promotion"))
        .await
        .unwrap();
    assert_eq!(next.through_id, last_public + 1);
    let old_compact = root_before.0.unwrap();
    assert!(
        sql.save_session_compact(
            &root,
            Some(old_compact.0),
            next.through_id,
            "post-promotion compact"
        )
        .await
        .unwrap()
    );
    let context = sql.session_context(&root).await.unwrap();
    assert_eq!(context.compact.unwrap().id, old_compact.0 + 1);
    assert_eq!(
        to_value(sql.session_page(&root, cursor).await.unwrap()).unwrap(),
        to_value(old_page).unwrap()
    );
    let raw = sql
        .context(&root.storage_key().unwrap())
        .await
        .unwrap()
        .compact
        .unwrap();
    assert_ne!(
        raw.id,
        old_compact.0 + 1,
        "raw API keeps physical SQLite IDs"
    );
    let mut tx = sql.begin_journal().await.unwrap();
    let duplicate = sql
        .promote_session_in(&mut tx, &snapshot, "save request")
        .await
        .unwrap();
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.revision, saved.revision);
    assert!(
        sql.promote_session_in(&mut tx, &snapshot, "changed request")
            .await
            .is_err()
    );
    tx.commit().await.unwrap();
    assert!(
        query("PRAGMA foreign_key_check")
            .fetch_all(sql.pool())
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn nested_promotion_failure_does_not_leave_partial_sql_when_outer_transaction_commits() {
    let sql = store().await;
    let ram = RetainedMemoryHistory::default();
    let (id, _, snapshot) = complex_snapshot(&ram).await;
    query("CREATE TABLE maintenance(value TEXT)")
        .execute(sql.pool())
        .await
        .unwrap();
    query("CREATE TRIGGER fail_child BEFORE INSERT ON turns WHEN NEW.content='child result' BEGIN SELECT RAISE(ABORT,'injected child write failure'); END")
        .execute(sql.pool()).await.unwrap();
    let mut tx = sql.begin_journal().await.unwrap();
    assert!(
        sql.promote_session_in(&mut tx, &snapshot, "request")
            .await
            .is_err()
    );
    query("INSERT INTO maintenance VALUES ('separate operation')")
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(sql.get_session(&id).await.unwrap().is_none());
    assert_eq!(
        ram.export_session(&id).unwrap().fingerprint(),
        snapshot.fingerprint()
    );
    for table in [
        "turns",
        "compacts",
        "journal_appends",
        "history_public_turns",
        "history_public_compacts",
        "history_session_promotions",
    ] {
        let (count,): (i64,) = query_as(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(sql.pool())
            .await
            .unwrap();
        assert_eq!(count, 0);
    }
    query("DROP TRIGGER fail_child")
        .execute(sql.pool())
        .await
        .unwrap();
    let mut tx = sql.begin_journal().await.unwrap();
    assert!(
        !sql.promote_session_in(&mut tx, &snapshot, "request")
            .await
            .unwrap()
            .duplicate
    );
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn unrelated_target_or_reused_promotion_key_cannot_be_overwritten() {
    let sql = store().await;
    let ram = RetainedMemoryHistory::default();
    let (id, _, snapshot) = complex_snapshot(&ram).await;
    let mut occupied = CreateSession::private("different owner");
    occupied.id = Some(id.clone());
    sql.create_session(occupied).await.unwrap();
    let mut tx = sql.begin_journal().await.unwrap();
    assert!(
        sql.promote_session_in(&mut tx, &snapshot, "request")
            .await
            .is_err()
    );
    tx.commit().await.unwrap();
    assert_eq!(
        sql.get_session(&id).await.unwrap().unwrap().access,
        SessionAccess::Private {
            owner: "different owner".into()
        }
    );
    let other = temporary(&ram).await;
    let second = ram.export_session(&other).unwrap();
    let mut tx = sql.begin_journal().await.unwrap();
    sql.promote_session_in(&mut tx, &second, "shared key")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let third = temporary(&ram).await;
    let third_snapshot = ram.export_session(&third).unwrap();
    let mut tx = sql.begin_journal().await.unwrap();
    assert!(
        sql.promote_session_in(&mut tx, &third_snapshot, "shared key")
            .await
            .is_err()
    );
    tx.commit().await.unwrap();
    assert!(sql.get_session(&third).await.unwrap().is_none());
}

#[tokio::test]
async fn promotion_rollback_is_atomic_with_application_markers_and_keeps_ram() {
    let sql = store().await;
    let ram = RetainedMemoryHistory::default();
    let (id, _, snapshot) = complex_snapshot(&ram).await;
    query("CREATE TABLE application_markers(session_id TEXT PRIMARY KEY,mode TEXT)")
        .execute(sql.pool())
        .await
        .unwrap();
    let mut tx = sql.begin_journal().await.unwrap();
    sql.promote_session_in(&mut tx, &snapshot, "request")
        .await
        .unwrap();
    query("INSERT INTO application_markers VALUES (?,'saved')")
        .bind(id.as_str())
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert!(sql.get_session(&id).await.unwrap().is_none());
    assert_eq!(
        ram.export_session(&id).unwrap().fingerprint(),
        snapshot.fingerprint()
    );
    for table in [
        "turns",
        "compacts",
        "journal_appends",
        "history_public_turns",
        "history_public_compacts",
        "history_session_promotions",
        "application_markers",
    ] {
        let (count,): (i64,) = query_as(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(sql.pool())
            .await
            .unwrap();
        assert_eq!(count, 0);
    }
    let mut tx = sql.begin_journal().await.unwrap();
    sql.promote_session_in(&mut tx, &snapshot, "request")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let (before,): (i64,) = query_as("SELECT COUNT(*) FROM turns")
        .fetch_one(sql.pool())
        .await
        .unwrap();
    let mut tx = sql.begin_journal().await.unwrap();
    assert!(
        sql.promote_session_in(&mut tx, &snapshot, "request")
            .await
            .unwrap()
            .duplicate
    );
    tx.commit().await.unwrap();
    let (after,): (i64,) = query_as("SELECT COUNT(*) FROM turns")
        .fetch_one(sql.pool())
        .await
        .unwrap();
    assert_eq!(before, after);
    ram.append_session(
        &SessionJournal::Root(id),
        &request("late", "1", "after snapshot"),
    )
    .await
    .unwrap();
    let changed = ram.export_session(&snapshot.session().id).unwrap();
    let mut tx = sql.begin_journal().await.unwrap();
    assert!(
        sql.promote_session_in(&mut tx, &changed, "request")
            .await
            .is_err()
    );
    tx.commit().await.unwrap();
    let (unchanged,): (i64,) = query_as("SELECT COUNT(*) FROM turns")
        .fetch_one(sql.pool())
        .await
        .unwrap();
    assert_eq!(unchanged, after);
}
