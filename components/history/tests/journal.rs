#![cfg(feature = "sqlite")]

use std::{slice::from_ref, sync::Arc, time::Duration};

use octo_history::{
    AppendRequest, HistoryError, HistoryStore, KeyRemap, PageRequest, SqliteHistory, Turn,
};
use sqlx::{query, query_as, raw_sql, sqlite::SqlitePoolOptions};
use tempfile::tempdir;
use tokio::{join, time::timeout};

fn request(id: &str, text: &str) -> AppendRequest {
    AppendRequest {
        namespace: "verified-transport".into(),
        idempotency_key: id.into(),
        digest: "caller-request-digest".into(),
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

#[tokio::test]
async fn stable_pages_survive_appends_compaction_and_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("history.db");
    let h = SqliteHistory::open_retained(&path).await.unwrap();
    for index in 0..25 {
        h.append_journal("session", &request(&index.to_string(), &index.to_string()))
            .await
            .unwrap();
    }
    let first = h
        .journal_page("session", PageRequest::default())
        .await
        .unwrap();
    assert_eq!(first.records.len(), 10);
    assert_eq!(first.records[0].turn.content, "15");
    assert_eq!(first.records[9].turn.content, "24");
    assert!(first.has_more);
    assert!(first.records.iter().all(|row| !row.created_at.is_empty()));
    let original = (first.records[0].id, first.records[0].created_at.clone());
    h.append_journal("other", &request("1", "another scope"))
        .await
        .unwrap();
    h.append_journal("session", &request("26", "new message"))
        .await
        .unwrap();
    assert!(
        h.save_compact("session", None, first.through_id, "summary")
            .await
            .unwrap()
    );
    let second = h
        .journal_page(
            "session",
            PageRequest {
                before_id: first.next_before,
                through_id: Some(first.through_id),
                ..PageRequest::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(second.records.len(), 10);
    assert_eq!(second.records[0].turn.content, "5");
    assert_eq!(second.records[9].turn.content, "14");
    let last = h
        .journal_page(
            "session",
            PageRequest {
                before_id: second.next_before,
                through_id: Some(first.through_id),
                ..PageRequest::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(last.records.len(), 5);
    assert!(!last.has_more);
    assert!(last.next_before.is_none());
    drop(h);
    let h = SqliteHistory::open_retained(path).await.unwrap();
    let again = h
        .journal_page(
            "session",
            PageRequest {
                through_id: Some(first.through_id),
                ..PageRequest::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        (again.records[0].id, again.records[0].created_at.clone()),
        original
    );
    assert_eq!(h.context("session").await.unwrap().messages.len(), 1);
}

#[tokio::test]
async fn retries_bind_caller_digest_batch_and_namespace() {
    let h = store().await;
    let mut input = request("same", "original");
    input.turns.push(Turn::assistant("answer"));
    let first = h.append_journal("a", &input).await.unwrap();
    assert_eq!(first.record_ids.len(), 2);
    assert!(!first.duplicate);
    h.append_journal("a", &request("later", "later"))
        .await
        .unwrap();
    let retry = h.append_journal("a", &input).await.unwrap();
    assert!(retry.duplicate);
    assert_eq!(retry.record_ids, first.record_ids);
    assert_eq!(retry.through_id, first.through_id);
    let mut changed = input.clone();
    changed.turns[0].content = "changed without changing caller digest".into();
    assert!(matches!(
        h.append_journal("a", &changed).await,
        Err(HistoryError::IdempotencyConflict)
    ));
    changed = input.clone();
    changed.digest = "different semantics".into();
    assert!(matches!(
        h.append_journal("a", &changed).await,
        Err(HistoryError::IdempotencyConflict)
    ));
    changed = input.clone();
    changed.namespace = "another-transport".into();
    assert!(!h.append_journal("a", &changed).await.unwrap().duplicate);
    assert!(!h.append_journal("b", &input).await.unwrap().duplicate);
    assert_eq!(h.load("a").await.len(), 5);
}

#[tokio::test]
async fn concurrent_retries_commit_one_batch() {
    let dir = tempdir().unwrap();
    let h = Arc::new(
        SqliteHistory::open_retained(dir.path().join("history.db"))
            .await
            .unwrap(),
    );
    let input = request("same", "one input");
    let (a, b) = timeout(Duration::from_secs(10), async {
        join!(h.append_journal("a", &input), h.append_journal("a", &input))
    })
    .await
    .unwrap();
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_eq!(a.record_ids, b.record_ids);
    assert_ne!(a.duplicate, b.duplicate);
    assert_eq!(h.load("a").await.len(), 1);
}

#[tokio::test]
async fn transaction_seam_rolls_back_batch_and_composes_with_metadata() {
    let h = store().await;
    query("CREATE TABLE application_metadata (id INTEGER PRIMARY KEY)")
        .execute(h.pool())
        .await
        .unwrap();
    let mut tx = h.begin_journal().await.unwrap();
    query("INSERT INTO application_metadata VALUES (1)")
        .execute(&mut *tx)
        .await
        .unwrap();
    h.append_journal_in(&mut tx, "a", &request("one", "accepted"))
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert!(h.load("a").await.is_empty());
    let (count,): (i64,) = query_as("SELECT COUNT(*) FROM application_metadata")
        .fetch_one(h.pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
    query(
        "CREATE TRIGGER reject_content BEFORE INSERT ON turns WHEN NEW.content = 'reject'
           BEGIN SELECT RAISE(ABORT, 'injected write failure'); END",
    )
    .execute(h.pool())
    .await
    .unwrap();
    let mut tx = h.begin_journal().await.unwrap();
    let mut input = request("batch", "first must roll back");
    input.turns.push(Turn::assistant("reject"));
    assert!(h.append_journal_in(&mut tx, "a", &input).await.is_err());
    query("INSERT INTO application_metadata VALUES (2)")
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(h.load("a").await.is_empty());
    let (count,): (i64,) = query_as("SELECT COUNT(*) FROM journal_appends")
        .fetch_one(h.pool())
        .await
        .unwrap();
    assert_eq!(count, 0, "failed nested batch left no receipt");
    assert!(
        !h.append_journal("a", &request("batch", "retry after rollback"))
            .await
            .unwrap()
            .duplicate
    );
}

#[tokio::test]
async fn upgrade_and_remap_preserve_ids_timestamps_compacts_and_receipts() {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    raw_sql(include_str!("../migrations/0001_init.sql"))
        .execute(&pool)
        .await
        .unwrap();
    raw_sql(include_str!("../migrations/0002_compacts.sql"))
        .execute(&pool)
        .await
        .unwrap();
    query("INSERT INTO turns (id,channel,role,content,created_at) VALUES (7,'legacy','user','old','2001-01-01 00:00:00')")
        .execute(&pool).await.unwrap();
    query(
        "INSERT INTO compacts (id,channel,through_id,content) VALUES (3,'legacy',7,'old compact')",
    )
    .execute(&pool)
    .await
    .unwrap();
    let h = SqliteHistory::from_pool(pool).await.unwrap();
    let input = request("accepted", "recent");
    let receipt = h.append_journal("legacy", &input).await.unwrap();
    let inventory = h.journal_inventory().await.unwrap().remove(0);
    assert_eq!(
        (inventory.turns, inventory.compacts, inventory.receipts),
        (2, 1, 1)
    );
    let mapping = KeyRemap {
        source: inventory.clone(),
        target: "session/canonical".into(),
    };
    let migrated = h.remap_journal_keys(from_ref(&mapping)).await.unwrap();
    assert!(!migrated[0].duplicate);
    let page = h
        .journal_page(&mapping.target, PageRequest::default())
        .await
        .unwrap();
    assert_eq!(
        (page.records[0].id, page.records[0].created_at.as_str()),
        (7, "2001-01-01 00:00:00")
    );
    let context = h.context(&mapping.target).await.unwrap();
    assert_eq!(context.compact.unwrap().id, 3);
    assert_eq!(context.messages[0].id, receipt.record_ids[0]);
    let retry = h.append_journal(&mapping.target, &input).await.unwrap();
    assert!(retry.duplicate);
    assert_eq!(retry.record_ids, receipt.record_ids);
    assert!(h.remap_journal_keys(&[mapping]).await.unwrap()[0].duplicate);
    assert!(matches!(
        h.append("legacy", &[Turn::user("late writer")]).await,
        Err(HistoryError::KeyRemapped { .. })
    ));
    let violations = query("PRAGMA foreign_key_check")
        .fetch_all(h.pool())
        .await
        .unwrap();
    assert!(violations.is_empty());
    let current = h.journal_inventory().await.unwrap().remove(0);
    assert_eq!(current.fingerprint, inventory.fingerprint);
}

#[tokio::test]
async fn stale_or_colliding_remap_is_atomic_and_retryable() {
    let h = store().await;
    h.append_journal("a", &request("1", "a")).await.unwrap();
    h.append_journal("b", &request("1", "b")).await.unwrap();
    let inventory = h.journal_inventory().await.unwrap();
    let a = inventory
        .iter()
        .find(|item| item.key == "a")
        .unwrap()
        .clone();
    let b = inventory
        .iter()
        .find(|item| item.key == "b")
        .unwrap()
        .clone();
    let mappings = [
        KeyRemap {
            source: a.clone(),
            target: "target-a".into(),
        },
        KeyRemap {
            source: b.clone(),
            target: "a".into(),
        },
    ];
    assert!(h.remap_journal_keys(&mappings).await.is_err());
    assert_eq!(h.journal_inventory().await.unwrap(), inventory);
    h.append_journal("a", &request("later", "changed"))
        .await
        .unwrap();
    assert!(
        h.remap_journal_keys(&[KeyRemap {
            source: a,
            target: "target".into()
        }])
        .await
        .is_err()
    );
    assert_eq!(h.load("a").await.len(), 2);
    let mappings = [KeyRemap {
        source: b,
        target: "b-new".into(),
    }];
    let mut tx = h.begin_journal().await.unwrap();
    h.remap_journal_keys_in(&mut tx, &mappings).await.unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(h.load("b").await.len(), 1);
    assert!(h.load("b-new").await.is_empty());
    assert!(!h.remap_journal_keys(&mappings).await.unwrap()[0].duplicate);
}

#[tokio::test]
async fn strict_failures_are_errors_and_capped_mode_is_not_faked() {
    let h = store().await;
    for limit in [0, 101] {
        assert!(
            h.journal_page(
                "a",
                PageRequest {
                    limit,
                    ..PageRequest::default()
                }
            )
            .await
            .is_err()
        );
    }
    h.pool().close().await;
    assert!(h.journal_page("a", PageRequest::default()).await.is_err());
    let dir = tempdir().unwrap();
    let capped = SqliteHistory::open(dir.path().join("capped.db"), 2)
        .await
        .unwrap();
    assert!(matches!(
        capped.append_journal("a", &request("1", "a")).await,
        Err(HistoryError::Unsupported(_))
    ));
    capped
        .append("a", &[Turn::user("1"), Turn::user("2"), Turn::user("3")])
        .await
        .unwrap();
    assert_eq!(capped.load("a").await.len(), 2);
}

#[tokio::test]
async fn remap_write_failure_rolls_back_every_key_even_if_outer_transaction_commits() {
    let h = store().await;
    h.append_journal("a", &request("1", "a")).await.unwrap();
    h.append_journal("b", &request("1", "b")).await.unwrap();
    let inventory = h.journal_inventory().await.unwrap();
    let mappings: Vec<_> = inventory
        .iter()
        .map(|source| KeyRemap {
            source: source.clone(),
            target: format!("target-{}", source.key),
        })
        .collect();
    query(
        "CREATE TRIGGER reject_remap BEFORE UPDATE ON turns WHEN NEW.channel = 'target-b'
           BEGIN SELECT RAISE(ABORT, 'injected remap failure'); END",
    )
    .execute(h.pool())
    .await
    .unwrap();
    query("CREATE TABLE application_metadata (value TEXT)")
        .execute(h.pool())
        .await
        .unwrap();
    let mut tx = h.begin_journal().await.unwrap();
    assert!(h.remap_journal_keys_in(&mut tx, &mappings).await.is_err());
    query("INSERT INTO application_metadata VALUES ('independent')")
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(h.journal_inventory().await.unwrap(), inventory);
    assert!(h.load("target-a").await.is_empty());
    let (count,): (i64,) = query_as("SELECT COUNT(*) FROM journal_key_remaps")
        .fetch_one(h.pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn strict_reads_reject_corrupt_roles_instead_of_reclassifying_them() {
    let h = store().await;
    query("INSERT INTO turns (channel,role,content) VALUES ('legacy','not-a-role','data')")
        .execute(h.pool())
        .await
        .unwrap();
    assert!(
        h.journal_page("legacy", PageRequest::default())
            .await
            .is_err()
    );
    assert!(h.context("legacy").await.is_err());
}

#[tokio::test]
async fn scoped_reads_share_one_connection_and_observe_uncommitted_migration_inventory() {
    let h = store().await;
    timeout(Duration::from_secs(2), async {
        let mut tx = h.begin_journal().await.unwrap();
        h.append_journal_in(&mut tx, "legacy", &request("one", "uncommitted"))
            .await
            .unwrap();
        let inventory = h.journal_inventory_all_in(&mut tx).await.unwrap();
        assert_eq!((inventory.len(), inventory[0].turns), (1, 1));
        assert!(
            h.journal_inventory_prefix_in(&mut tx, "subagent/")
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            h.journal_inventory_prefix_in(&mut tx, "leg").await.unwrap(),
            inventory
        );
        assert!(
            h.journal_inventory_prefix_in(&mut tx, "l_g%")
                .await
                .unwrap()
                .is_empty()
        );
        let page = h
            .journal_page_in(&mut tx, "legacy", PageRequest::default())
            .await
            .unwrap();
        assert_eq!(page.records[0].turn.content, "uncommitted");
        tx.rollback().await.unwrap();
    })
    .await
    .unwrap();
    assert!(h.journal_inventory().await.unwrap().is_empty());
}
