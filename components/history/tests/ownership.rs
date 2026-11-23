#![cfg(feature = "sqlite")]
use octo_history::{
    AppendRequest, CreateSession, PageRequest, SessionAccess, SessionId, SessionJournal,
    SessionKind, SessionOrigin, SessionStore, SqliteHistory, Turn,
};

#[tokio::test]
async fn owner_transfer_preserves_identity_provenance_and_outer_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteHistory::open_retained(dir.path().join("history.db"))
        .await
        .unwrap();
    let mut original = CreateSession::private("legacy");
    original.id = Some(SessionId::new());
    let session = store.create_session(original.clone()).await.unwrap();
    let journal = SessionJournal::Root(session.id.clone());
    let receipt = store
        .append_session(
            &journal,
            &AppendRequest {
                namespace: "test".into(),
                idempotency_key: "message".into(),
                digest: "digest".into(),
                turns: vec![Turn::user("retained")],
            },
        )
        .await
        .unwrap();
    let before = store
        .session_page(&journal, PageRequest::default())
        .await
        .unwrap();
    let origin = SessionOrigin {
        connector: "transport".into(),
        channel: "42".into(),
    };
    let mut tx = store.begin_journal().await.unwrap();
    store
        .ensure_default_session_in(&mut tx, &origin, original.clone())
        .await
        .unwrap();
    assert_eq!(
        store
            .private_session_ids_in(&mut tx, "legacy")
            .await
            .unwrap(),
        vec![session.id.clone()]
    );
    store
        .reassign_private_owner_in(&mut tx, &[session.id.clone()], "legacy", "account")
        .await
        .unwrap();
    assert_eq!(
        store
            .get_session_in(&mut tx, &session.id)
            .await
            .unwrap()
            .unwrap()
            .access,
        SessionAccess::Private {
            owner: "account".into()
        }
    );
    tx.rollback().await.unwrap();
    assert_eq!(
        store
            .get_session(&session.id)
            .await
            .unwrap()
            .unwrap()
            .access,
        SessionAccess::Private {
            owner: "legacy".into()
        }
    );
    let mut tx = store.begin_journal().await.unwrap();
    store
        .ensure_default_session_in(&mut tx, &origin, original.clone())
        .await
        .unwrap();
    store
        .reassign_private_owner_in(
            &mut tx,
            &[session.id.clone(), session.id.clone()],
            "legacy",
            "account",
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        store
            .get_default_session(&origin)
            .await
            .unwrap()
            .unwrap()
            .id,
        session.id
    );
    assert_eq!(
        store.create_session(original).await.unwrap().id,
        session.id,
        "original creation request stays idempotent"
    );
    let after = store
        .session_page(&journal, PageRequest::default())
        .await
        .unwrap();
    assert_eq!(before.records[0].id, after.records[0].id);
    assert_eq!(
        before.records[0].turn.content,
        after.records[0].turn.content
    );
    let lookup = store
        .find_request("test", "message")
        .await
        .unwrap()
        .unwrap()
        .receipt;
    assert_eq!(lookup.record_ids, receipt.record_ids);
    assert_eq!(lookup.through_id, receipt.through_id);
    assert!(lookup.duplicate);
}

#[tokio::test]
async fn transfer_rejects_wrong_owner_group_and_missing_session_without_partial_change() {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteHistory::open_retained(dir.path().join("history.db"))
        .await
        .unwrap();
    let one = store
        .create_session(CreateSession::private("legacy"))
        .await
        .unwrap();
    let other = store
        .create_session(CreateSession::private("other"))
        .await
        .unwrap();
    let group = store
        .create_session(CreateSession {
            id: None,
            access: SessionAccess::GroupBound {
                connector: "transport".into(),
                channel: "-42".into(),
            },
            host: "host".into(),
            title: String::new(),
            kind: SessionKind::Permanent,
        })
        .await
        .unwrap();
    let mut tx = store.begin_journal().await.unwrap();
    for invalid in [other.id, group.id, SessionId::new()] {
        assert!(
            store
                .reassign_private_owner_in(&mut tx, &[one.id.clone(), invalid], "legacy", "account")
                .await
                .is_err()
        );
        assert_eq!(
            store
                .get_session_in(&mut tx, &one.id)
                .await
                .unwrap()
                .unwrap()
                .access,
            SessionAccess::Private {
                owner: "legacy".into()
            }
        );
    }
    tx.commit().await.unwrap();
}
