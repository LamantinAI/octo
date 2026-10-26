use octo_history::{
    AppendRequest, CreateSession, HistoryError, RetainedMemoryHistory, SessionJournal,
    SessionStore, Turn,
};

fn request(text: &str) -> AppendRequest {
    AppendRequest {
        namespace: "telegram:private:42".into(),
        idempotency_key: "update-1".into(),
        digest: "semantic digest".into(),
        turns: vec![Turn::user(text)],
    }
}
async fn contract(store: &dyn SessionStore) {
    assert!(
        store
            .find_request("telegram:private:42", "update-1")
            .await
            .unwrap()
            .is_none()
    );
    let a = store
        .create_session(CreateSession::private("owner"))
        .await
        .unwrap();
    let root = SessionJournal::Root(a.id.clone());
    let first = store
        .append_session(&root, &request("native input"))
        .await
        .unwrap();
    let child = SessionJournal::Child {
        session_id: a.id,
        run_id: "albert/child:1".into(),
    };
    store
        .append_session(&child, &request("internal child request"))
        .await
        .unwrap();
    let found = store
        .find_request("telegram:private:42", "update-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.journal, root);
    assert_eq!(found.digest, "semantic digest");
    assert_eq!(found.batch_digest.len(), 64);
    assert_eq!(found.receipt.record_ids, first.record_ids);
    assert!(found.receipt.duplicate);
    let b = store
        .create_session(CreateSession::private("owner"))
        .await
        .unwrap();
    store
        .append_session(&SessionJournal::Root(b.id), &request("second journal"))
        .await
        .unwrap();
    assert!(matches!(
        store.find_request("telegram:private:42", "update-1").await,
        Err(HistoryError::RequestAmbiguous)
    ));
    let unbound = AppendRequest {
        namespace: "native-unmapped".into(),
        ..request("legacy input")
    };
    store
        .append_journal("legacy unassigned", &unbound)
        .await
        .unwrap();
    assert!(matches!(
        store.find_request("native-unmapped", "update-1").await,
        Err(HistoryError::RequestUnbound)
    ));
}

#[tokio::test]
async fn ram_finder_uses_authoritative_root_receipts_and_never_first_match() {
    contract(&RetainedMemoryHistory::default()).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_finder_uses_authoritative_root_receipts_and_rejects_corruption() {
    use octo_history::SqliteHistory;
    use sqlx::{query, sqlite::SqlitePoolOptions};
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    let store = SqliteHistory::from_pool(pool).await.unwrap();
    contract(&store).await;
    query(
        "UPDATE journal_appends SET batch_digest='corrupt' WHERE namespace='telegram:private:42'",
    )
    .execute(store.pool())
    .await
    .unwrap();
    // Remove ambiguity first; the remaining typed receipt must still fail validation.
    query("UPDATE journal_appends SET namespace='different' WHERE id=(SELECT MAX(id) FROM journal_appends WHERE namespace='telegram:private:42')")
        .execute(store.pool()).await.unwrap();
    assert!(
        store
            .find_request("telegram:private:42", "update-1")
            .await
            .is_err()
    );
}
