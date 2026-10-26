use octo_history::{
    AppendRequest, CreateSession, HistoryError, MemoryLimits, PageRequest, Persistence,
    RetainedMemoryHistory, SessionAccess, SessionFilter, SessionId, SessionJournal, SessionKind,
    SessionOrigin, SessionState, SessionStore, SessionUpdate, Turn,
};

fn input(id: &str, text: &str) -> AppendRequest {
    AppendRequest {
        namespace: "transport".into(),
        idempotency_key: id.into(),
        digest: "semantics".into(),
        turns: vec![Turn::user(text)],
    }
}
fn private(owner: &str) -> CreateSession {
    let mut request = CreateSession::private(owner);
    request.id = Some(SessionId::new());
    request
}
fn group() -> CreateSession {
    CreateSession {
        id: Some(SessionId::new()),
        access: SessionAccess::GroupBound {
            connector: "telegram".into(),
            channel: "-42".into(),
        },
        host: "host".into(),
        title: "shared".into(),
        kind: SessionKind::Permanent,
    }
}

async fn catalog_contract(store: &dyn SessionStore) {
    let original = private("owner");
    let created = store.create_session(original.clone()).await.unwrap();
    assert_eq!(created.host, "host");
    let changed = store
        .update_session(
            &created.id,
            SessionUpdate {
                expected_revision: 1,
                title: Some("renamed".into()),
                archived: Some(true),
            },
        )
        .await
        .unwrap();
    assert_eq!(changed.state, SessionState::Archived);
    assert_eq!(changed.revision, 2);
    assert!(matches!(
        store
            .update_session(
                &created.id,
                SessionUpdate {
                    expected_revision: 1,
                    title: Some("stale".into()),
                    archived: None,
                }
            )
            .await,
        Err(HistoryError::RevisionConflict { .. })
    ));
    assert_eq!(
        store.create_session(original.clone()).await.unwrap().id,
        created.id
    );
    let mut collision = original;
    collision.access = SessionAccess::Private {
        owner: "another owner".into(),
    };
    assert!(matches!(
        store.create_session(collision).await,
        Err(HistoryError::SessionConflict)
    ));
    for _ in 0..24 {
        store.create_session(private("owner")).await.unwrap();
    }
    store
        .create_session(private("another owner"))
        .await
        .unwrap();
    let shared = store.create_session(group()).await.unwrap();
    assert!(matches!(
        store.create_session(group()).await,
        Err(HistoryError::SessionConflict)
    ));
    let first = store
        .list_sessions(
            &SessionFilter::PrivateOwner("owner".into()),
            PageRequest::default(),
        )
        .await
        .unwrap();
    assert_eq!(first.sessions.len(), 10);
    assert!(first.has_more);
    assert!(
        first
            .sessions
            .iter()
            .all(|s| matches!(&s.access, SessionAccess::Private { owner } if owner == "owner"))
    );
    store.create_session(private("owner")).await.unwrap();
    let second = store
        .list_sessions(
            &SessionFilter::PrivateOwner("owner".into()),
            PageRequest {
                before_id: first.next_before,
                through_id: Some(first.through_id),
                ..PageRequest::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(second.sessions.len(), 10);
    assert!(
        second
            .sessions
            .iter()
            .all(|s| first.sessions.iter().all(|first| first.id != s.id))
    );
    let exact = store
        .list_sessions(
            &SessionFilter::GroupOrigin {
                connector: "telegram".into(),
                channel: "-42".into(),
            },
            PageRequest::default(),
        )
        .await
        .unwrap();
    assert_eq!(exact.sessions.len(), 1);
    assert_eq!(exact.sessions[0].id, shared.id);
    assert!(
        store
            .list_sessions(
                &SessionFilter::GroupOrigin {
                    connector: "other-telegram".into(),
                    channel: "-42".into()
                },
                PageRequest::default()
            )
            .await
            .unwrap()
            .sessions
            .is_empty()
    );
    let mut temporary_group = group();
    temporary_group.kind = SessionKind::Temporary;
    assert!(store.create_session(temporary_group).await.is_err());
}

async fn journal_contract(store: &dyn SessionStore) {
    let a = store.create_session(private("owner")).await.unwrap();
    let b = store.create_session(private("owner")).await.unwrap();
    let root = SessionJournal::Root(a.id.clone());
    let other_root = SessionJournal::Root(b.id.clone());
    let child = SessionJournal::Child {
        session_id: a.id.clone(),
        run_id: "albert/child-1:existing-id".into(),
    };
    let receipt = store
        .append_session(&root, &input("same", "root"))
        .await
        .unwrap();
    store
        .append_session(&child, &input("same", "child"))
        .await
        .unwrap();
    store
        .append_session(&other_root, &input("same", "other"))
        .await
        .unwrap();
    assert_ne!(
        root, child,
        "public numeric IDs are scoped to their journal"
    );
    assert!(
        store
            .append_session(&root, &input("same", "root"))
            .await
            .unwrap()
            .duplicate
    );
    assert!(matches!(
        store.append_session(&root, &input("same", "changed")).await,
        Err(HistoryError::IdempotencyConflict)
    ));
    assert_eq!(store.load_session(&root).await.unwrap()[0].content, "root");
    assert_eq!(
        store.load_session(&child).await.unwrap()[0].content,
        "child"
    );
    assert_eq!(
        store.load_session(&other_root).await.unwrap()[0].content,
        "other"
    );
    assert!(
        !store
            .save_session_compact(&root, None, receipt.through_id + 1000, "wrong boundary")
            .await
            .unwrap()
    );
    assert!(
        store
            .save_session_compact(&root, None, receipt.through_id, "summary")
            .await
            .unwrap()
    );
    assert!(
        !store
            .save_session_compact(&root, None, receipt.through_id, "stale summary")
            .await
            .unwrap()
    );
    store
        .append_session(&root, &input("next", "tail"))
        .await
        .unwrap();
    let context = store.session_context(&root).await.unwrap();
    assert_eq!(context.compact.unwrap().content, "summary");
    assert_eq!(context.messages.len(), 1);
    assert_eq!(store.load_session(&root).await.unwrap().len(), 2);
    assert_eq!(
        store
            .session_page(&root, PageRequest::default())
            .await
            .unwrap()
            .records
            .len(),
        2
    );
    assert!(
        store
            .session_context(&SessionJournal::Root(SessionId::new()))
            .await
            .is_err()
    );
    assert_eq!(
        store
            .list_sessions(
                &SessionFilter::PrivateOwner("owner".into()),
                PageRequest::default()
            )
            .await
            .unwrap()
            .sessions
            .len(),
        2,
        "child is not a catalog session"
    );
}

async fn default_contract(store: &dyn SessionStore) {
    let origin = SessionOrigin {
        connector: "telegram".into(),
        channel: "42".into(),
    };
    let default = store
        .ensure_default_session(&origin, CreateSession::private("owner"))
        .await
        .unwrap();
    let retry = store
        .ensure_default_session(&origin, CreateSession::private("owner"))
        .await
        .unwrap();
    assert_eq!(default.id, retry.id);
    let another = store.create_session(private("owner")).await.unwrap();
    assert_ne!(default.id, another.id);
    assert_eq!(
        store
            .get_default_session(&origin)
            .await
            .unwrap()
            .unwrap()
            .id,
        default.id
    );
    assert!(
        store
            .ensure_default_session(&origin, CreateSession::private("other"))
            .await
            .is_err()
    );
    let shared = store.create_session(group()).await.unwrap();
    let group_origin = SessionOrigin {
        connector: "telegram".into(),
        channel: "-42".into(),
    };
    assert_eq!(
        store
            .get_default_session(&group_origin)
            .await
            .unwrap()
            .unwrap()
            .id,
        shared.id
    );
    assert!(
        store
            .ensure_default_session(&group_origin, CreateSession::private("owner"))
            .await
            .is_err()
    );
    let wrong_origin = SessionOrigin {
        connector: "telegram".into(),
        channel: "-43".into(),
    };
    assert!(
        store
            .ensure_default_session(&wrong_origin, group())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn retained_ram_catalog_and_journals_match_the_contract() {
    let catalog = RetainedMemoryHistory::default();
    assert_eq!(catalog.persistence(), Persistence::Volatile);
    catalog_contract(&catalog).await;
    journal_contract(&RetainedMemoryHistory::default()).await;
    default_contract(&RetainedMemoryHistory::default()).await;
}

#[tokio::test]
async fn retained_ram_rejects_quota_without_trimming_or_consuming_receipt() {
    let store = RetainedMemoryHistory::new(MemoryLimits {
        max_sessions: 2,
        max_turns: 2,
        max_bytes: 50_000,
    })
    .unwrap();
    let mut request = private("owner");
    request.kind = SessionKind::Temporary;
    let session = store.create_session(request).await.unwrap();
    let root = SessionJournal::Root(session.id);
    store
        .append_session(&root, &input("1", "first"))
        .await
        .unwrap();
    store
        .append_session(&root, &input("2", "second"))
        .await
        .unwrap();
    assert!(matches!(
        store
            .append_session(&root, &input("3", "must not trim"))
            .await,
        Err(HistoryError::QuotaExceeded)
    ));
    assert_eq!(store.load_session(&root).await.unwrap().len(), 2);
    assert!(
        store
            .append_session(&root, &input("1", "first"))
            .await
            .unwrap()
            .duplicate
    );
    assert!(
        RetainedMemoryHistory::default()
            .get_session(root.session_id())
            .await
            .unwrap()
            .is_none()
    );
}

#[test]
fn session_ids_and_tagged_types_roundtrip_and_reject_invalid_ids() {
    use serde_json::{from_str, from_value, to_value};
    let id = SessionId::new();
    assert_eq!(id.to_string().parse::<SessionId>().unwrap(), id);
    assert_eq!(from_value::<SessionId>(to_value(&id).unwrap()).unwrap(), id);
    assert!(from_str::<SessionId>("\"arbitrary/path\"").is_err());
    let journal = SessionJournal::Child {
        session_id: id,
        run_id: "albert/child:1".into(),
    };
    assert_eq!(
        from_value::<SessionJournal>(to_value(&journal).unwrap()).unwrap(),
        journal
    );
    assert!(
        from_value::<SessionFilter>(to_value(SessionFilter::PrivateOwner("owner".into())).unwrap())
            .is_ok()
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_catalog_and_journals_match_the_contract() {
    use octo_history::SqliteHistory;
    use sqlx::sqlite::SqlitePoolOptions;
    for scenario in 0..3 {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let store = SqliteHistory::from_pool(pool).await.unwrap();
        assert_eq!(store.persistence(), Persistence::Durable);
        match scenario {
            0 => catalog_contract(&store).await,
            1 => journal_contract(&store).await,
            _ => default_contract(&store).await,
        }
        let mut temporary = private("owner");
        temporary.kind = SessionKind::Temporary;
        assert!(matches!(
            store.create_session(temporary).await,
            Err(HistoryError::Unsupported(_))
        ));
    }
}
