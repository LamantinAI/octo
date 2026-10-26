use octo_history::{
    AppendRequest, CreateSession, HistoryError, MemoryLimits, RetainedMemoryHistory, SessionId,
    SessionJournal, SessionKind, SessionStore, SessionUpdate, Turn,
};

fn temporary() -> CreateSession {
    let mut request = CreateSession::private("owner");
    request.id = Some(SessionId::new());
    request.kind = SessionKind::Temporary;
    request
}
fn request(id: &str, text: &str) -> AppendRequest {
    AppendRequest {
        namespace: "transport".into(),
        idempotency_key: id.into(),
        digest: "semantics".into(),
        turns: vec![Turn::user(text)],
    }
}

#[tokio::test]
async fn snapshot_is_deterministic_and_stale_discard_cannot_delete_late_work() {
    let ram = RetainedMemoryHistory::default();
    let session = ram.create_session(temporary()).await.unwrap();
    let root = SessionJournal::Root(session.id.clone());
    let first = ram
        .append_session(&root, &request("1", "first"))
        .await
        .unwrap();
    ram.save_session_compact(&root, None, first.through_id, "summary")
        .await
        .unwrap();
    let child = SessionJournal::Child {
        session_id: session.id.clone(),
        run_id: "albert/child:1".into(),
    };
    ram.append_session(&child, &request("1", "child"))
        .await
        .unwrap();
    let snapshot = ram.export_session(&session.id).unwrap();
    assert_eq!(
        snapshot.fingerprint(),
        ram.export_session(&session.id).unwrap().fingerprint()
    );
    assert!(
        ram.append_session(&root, &request("1", "first"))
            .await
            .unwrap()
            .duplicate
    );
    assert_eq!(
        snapshot.fingerprint(),
        ram.export_session(&session.id).unwrap().fingerprint()
    );
    ram.append_session(&root, &request("2", "late input"))
        .await
        .unwrap();
    assert!(matches!(
        ram.discard_session(&session.id, snapshot.fingerprint()),
        Err(HistoryError::SessionConflict)
    ));
    assert_eq!(ram.load_session(&root).await.unwrap().len(), 2);
    let current = ram.export_session(&session.id).unwrap();
    ram.update_session(
        &session.id,
        SessionUpdate {
            expected_revision: 1,
            title: Some("changed".into()),
            archived: None,
        },
    )
    .await
    .unwrap();
    assert!(
        ram.discard_session(&session.id, current.fingerprint())
            .is_err()
    );
    let current = ram.export_session(&session.id).unwrap();
    let discarded = ram
        .discard_session(&session.id, current.fingerprint())
        .unwrap();
    assert_eq!(
        (
            discarded.journals,
            discarded.records,
            discarded.compacts,
            discarded.receipts
        ),
        (2, 3, 1, 3)
    );
    assert!(ram.get_session(&session.id).await.unwrap().is_none());
    assert!(ram.session_context(&child).await.is_err());
}

#[tokio::test]
async fn discard_frees_only_owned_quota_and_leaves_other_sessions_intact() {
    let ram = RetainedMemoryHistory::new(MemoryLimits {
        max_sessions: 2,
        max_turns: 2,
        max_bytes: 6000,
    })
    .unwrap();
    let other = ram.create_session(temporary()).await.unwrap();
    let other_root = SessionJournal::Root(other.id.clone());
    ram.append_session(&other_root, &request("other", "retain this"))
        .await
        .unwrap();
    let untouched = ram.export_session(&other.id).unwrap();
    for _ in 0..30 {
        let session = ram.create_session(temporary()).await.unwrap();
        let root = SessionJournal::Root(session.id.clone());
        let receipt = ram
            .append_session(&root, &request("one", "payload"))
            .await
            .unwrap();
        ram.save_session_compact(&root, None, receipt.through_id, "compact")
            .await
            .unwrap();
        assert!(matches!(
            ram.append_session(&root, &request("two", "over quota"))
                .await,
            Err(HistoryError::QuotaExceeded)
        ));
        let exported = ram.export_session(&session.id).unwrap();
        let discarded = ram
            .discard_session(&session.id, exported.fingerprint())
            .unwrap();
        assert!(discarded.bytes > "payload".len());
        assert_eq!(
            ram.export_session(&other.id).unwrap().fingerprint(),
            untouched.fingerprint()
        );
    }
    assert_eq!(
        ram.load_session(&other_root).await.unwrap()[0].content,
        "retain this"
    );
}

#[tokio::test]
async fn permanent_ram_sessions_are_not_promotion_or_discard_candidates() {
    let ram = RetainedMemoryHistory::default();
    let session = ram
        .create_session(CreateSession::private("owner"))
        .await
        .unwrap();
    assert!(ram.export_session(&session.id).is_err());
    assert!(ram.discard_session(&session.id, "").is_err());
    assert!(ram.get_session(&session.id).await.unwrap().is_some());
}
