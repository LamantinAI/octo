#![cfg(feature = "sqlite")]

use octo_history::{
    AppendRequest, CreateSession, HistoryError, PageRequest, Session, SessionJournal, SessionStore,
    SessionUpdate, SqliteHistory, Turn,
};
use sqlx::query_scalar;
use tempfile::tempdir;

async fn append(
    history: &SqliteHistory,
    session: &Session,
    label: &str,
    text: &[&str],
) -> Vec<i64> {
    history
        .append_session(
            &SessionJournal::Root(session.id.clone()),
            &AppendRequest {
                namespace: "fork-fixture".into(),
                idempotency_key: label.into(),
                digest: label.into(),
                turns: text.iter().map(|text| Turn::user(*text)).collect(),
            },
        )
        .await
        .unwrap()
        .record_ids
}

async fn fork(history: &SqliteHistory, parent: &Session, through: Option<i64>) -> Session {
    let mut tx = history.begin_journal().await.unwrap();
    let child = history
        .fork_session_in(&mut tx, &parent.id, through, "Branch".into())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    child
}

#[tokio::test]
async fn fork_pins_the_source_compact_and_pages_through_only_the_visible_prefix_after_restart() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("history.db");
    let history = SqliteHistory::open_retained(&path).await.unwrap();
    let parent = history
        .create_session(CreateSession::private("owner"))
        .await
        .unwrap();
    let parent_key = SessionJournal::Root(parent.id.clone());
    assert_eq!(
        append(&history, &parent, "first", &["one", "two", "three"]).await,
        [1, 2, 3]
    );
    assert!(
        history
            .save_session_compact(&parent_key, None, 2, "original checkpoint")
            .await
            .unwrap()
    );
    let child = fork(&history, &parent, None).await;
    let child_key = SessionJournal::Root(child.id.clone());
    let before = history.session_context(&child_key).await.unwrap();
    let saved = before.compact.as_ref().unwrap();
    let newer = history
        .session_context(&parent_key)
        .await
        .unwrap()
        .compact
        .unwrap();
    assert!(
        history
            .save_session_compact(
                &parent_key,
                Some(newer.id),
                2,
                "new checkpoint at same boundary"
            )
            .await
            .unwrap()
    );
    assert_eq!(
        append(&history, &parent, "parent-late", &["parent only"]).await,
        [4]
    );
    assert_eq!(
        append(&history, &child, "child-late", &["child only"]).await,
        [4]
    );
    let hot = history.session_context(&child_key).await.unwrap();
    assert_eq!(hot.compact.unwrap().content, "original checkpoint");
    assert_eq!(
        hot.messages
            .iter()
            .map(|m| m.turn.content.as_str())
            .collect::<Vec<_>>(),
        ["three", "child only"]
    );
    assert!(
        history
            .save_session_compact(
                &child_key,
                Some(saved.id),
                3,
                "child summary covers inherited turn"
            )
            .await
            .unwrap()
    );
    assert!(
        !history
            .save_session_compact(&child_key, Some(saved.id), 4, "stale")
            .await
            .unwrap()
    );
    let nested = fork(&history, &child, Some(2)).await;
    assert_eq!(
        append(&history, &nested, "nested-late", &["nested only"]).await,
        [3]
    );
    assert_eq!(
        history
            .inherited_root_cutoff(&nested.id, &parent.id)
            .await
            .unwrap(),
        Some(2)
    );
    let stored: i64 = query_scalar("SELECT count(*) FROM turns")
        .fetch_one(history.pool())
        .await
        .unwrap();
    assert_eq!(stored, 6);
    drop(history);

    let history = SqliteHistory::open_retained(path).await.unwrap();
    let page = history
        .session_page(
            &child_key,
            PageRequest {
                limit: 1,
                ..PageRequest::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(page.records[0].turn.content, "child only");
    let remainder = history
        .session_page(
            &child_key,
            PageRequest {
                before_id: page.next_before,
                through_id: Some(page.through_id),
                limit: 10,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        remainder
            .records
            .iter()
            .map(|r| r.turn.content.as_str())
            .collect::<Vec<_>>(),
        ["one", "two", "three"]
    );
    let nested_key = SessionJournal::Root(nested.id);
    let nested_turns = history.load_session(&nested_key).await.unwrap();
    assert_eq!(
        nested_turns
            .iter()
            .map(|row| row.content.as_str())
            .collect::<Vec<_>>(),
        ["one", "two", "nested only"]
    );
}

#[tokio::test]
async fn archived_forks_keep_the_parent_and_deleting_a_leaf_releases_its_dependency() {
    let directory = tempdir().unwrap();
    let history = SqliteHistory::open_retained(directory.path().join("history.db"))
        .await
        .unwrap();
    let parent = history
        .create_session(CreateSession::private("owner"))
        .await
        .unwrap();
    append(&history, &parent, "initial", &["shared"]).await;
    let parent_key = SessionJournal::Root(parent.id.clone());
    assert!(
        history
            .save_session_compact(&parent_key, None, 1, "shared summary")
            .await
            .unwrap()
    );
    let child = fork(&history, &parent, None).await;
    let child_key = SessionJournal::Root(child.id.clone());
    let checkpoint = history
        .session_context(&child_key)
        .await
        .unwrap()
        .compact
        .unwrap();
    assert!(
        history
            .save_session_compact(&child_key, Some(checkpoint.id), 1, "local summary")
            .await
            .unwrap()
    );
    let parent = history
        .update_session(
            &parent.id,
            SessionUpdate {
                expected_revision: parent.revision,
                title: None,
                archived: Some(true),
            },
        )
        .await
        .unwrap();
    let child = history
        .update_session(
            &child.id,
            SessionUpdate {
                expected_revision: child.revision,
                title: None,
                archived: Some(true),
            },
        )
        .await
        .unwrap();
    let mut tx = history.begin_journal().await.unwrap();
    assert!(matches!(
        history
            .delete_session_in(&mut tx, &parent.id, parent.revision)
            .await,
        Err(HistoryError::SessionHasForks(1))
    ));
    tx.rollback().await.unwrap();
    assert_eq!(
        history.load_session(&child_key).await.unwrap()[0].content,
        "shared"
    );
    let mut tx = history.begin_journal().await.unwrap();
    history
        .delete_session_in(&mut tx, &child.id, child.revision)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        history.load_session(&parent_key).await.unwrap()[0].content,
        "shared"
    );
    let mut tx = history.begin_journal().await.unwrap();
    history
        .delete_session_in(&mut tx, &parent.id, parent.revision)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(history.get_session(&parent.id).await.unwrap().is_none());
}

#[tokio::test]
async fn concurrent_fork_and_erasure_can_only_commit_one_operation() {
    let directory = tempdir().unwrap();
    let history = SqliteHistory::open_retained(directory.path().join("history.db"))
        .await
        .unwrap();
    let parent = history
        .create_session(CreateSession::private("owner"))
        .await
        .unwrap();
    let attempt_fork = async {
        let mut tx = history.begin_journal().await.unwrap();
        let fork = history
            .fork_session_in(&mut tx, &parent.id, None, "Concurrent fork".into())
            .await?;
        tx.commit().await.unwrap();
        Ok::<_, HistoryError>(fork)
    };
    let attempt_delete = async {
        let mut tx = history.begin_journal().await.unwrap();
        history
            .delete_session_in(&mut tx, &parent.id, parent.revision)
            .await?;
        tx.commit().await.unwrap();
        Ok::<_, HistoryError>(())
    };
    let (fork, deletion) = tokio::join!(attempt_fork, attempt_delete);
    match (fork, deletion) {
        (Ok(child), Err(HistoryError::SessionHasForks(1))) => {
            assert!(history.get_session(&parent.id).await.unwrap().is_some());
            assert!(history.get_session(&child.id).await.unwrap().is_some());
        }
        (Err(HistoryError::SessionNotFound), Ok(())) => {
            assert!(history.get_session(&parent.id).await.unwrap().is_none());
        }
        (fork, deletion) => panic!("unexpected fork and erasure results: {fork:?}, {deletion:?}"),
    }
}
