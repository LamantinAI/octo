//! Scope lifetime tests use authenticated proofs and the real approval journal.
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use octo_approval::{Access, Origin, ScopeBinding, ScopeValidator};
use octo_core::{ConnectorId, Envelope, EventKind};
use serde_json::{Value, from_str, json, to_string};
use sqlx::{Row, query};
use tokio::{spawn, task::yield_now};

use super::{Database, operations::Scope, tests::Fixture};
use crate::{Limits, SqlError};

struct Live(AtomicBool);
impl ScopeValidator for Live {
    fn allows(&self, binding: &ScopeBinding) -> bool {
        binding.opaque_id == "temporary"
            && binding.epoch == "epoch"
            && self.0.load(Ordering::SeqCst)
    }
}
fn scoped(f: &Fixture, kind: &str, payload: Value) -> Envelope {
    let ingress = f.authority.issue(
        Origin {
            principal: "owner".into(),
            connector: "telegram".into(),
            conversation: "1".into(),
            private_destination: "1".into(),
            may_approve: true,
        },
        Access::owner(),
    );
    let ingress = f
        .authority
        .bind_scope(&ingress, ScopeBinding::new("temporary", "epoch").unwrap())
        .unwrap();
    let envelope = Envelope::new(ConnectorId::new("test"), EventKind::new(kind), payload)
        .with_target(ConnectorId::new("sql"));
    if kind == "sql.approval.decide" {
        let proof = f.authority.bind_decision(&ingress, &envelope).unwrap();
        envelope.with_context(proof)
    } else {
        let proof = f.authority.bind(&ingress, &envelope).unwrap();
        envelope.with_context(proof)
    }
}
async fn pending(f: &Fixture, store: &str, table: &str, key: &str) -> String {
    let envelope = scoped(
        f,
        "sql.execute",
        json!({"store_id":store,"expected_revision":2,
        "sql":format!("DELETE FROM {table} WHERE id=?"),"parameters":[{"type":"integer","value":1}],"idempotency_key":key}),
    );
    let result = f.db.handle(&envelope).await.unwrap();
    assert_eq!(result["status"], "approval_required", "{result}");
    result["operation_id"].as_str().unwrap().into()
}
async fn fixture() -> (Fixture, Arc<Live>, String, String) {
    let f = Fixture::new(Limits::default()).await;
    let live = Arc::new(Live(AtomicBool::new(true)));
    f.authority.clone().with_scope_validator(live.clone());
    let store = f.store().await;
    let table = f.table(&store).await;
    (f, live, store, table)
}

#[tokio::test]
async fn closed_original_scope_cannot_be_revived_but_fresh_human_can_reject() {
    let (f, live, store, table) = fixture().await;
    let operation = pending(&f, &store, &table, "delete").await;
    let callback = scoped(
        &f,
        "sql.approval.decide",
        json!({"operation_id":operation,"approve":true}),
    );
    live.0.store(false, Ordering::SeqCst);
    assert!(matches!(
        f.db.handle(&callback).await,
        Err(SqlError::Authority(_))
    ));
    assert!(matches!(
        f.call(
            "sql.approval.decide",
            json!({"operation_id":operation,"approve":true})
        )
        .await,
        Err(SqlError::Authority(_))
    ));
    assert_eq!(
        f.call("sql.operation.status", json!({"operation_id":operation}))
            .await
            .unwrap()["status"],
        "approval_required"
    );
    assert_eq!(
        f.call(
            "sql.approval.decide",
            json!({"operation_id":operation,"approve":false})
        )
        .await
        .unwrap()["status"],
        "rejected"
    );
    assert_eq!(
        f.call(
            "sql.approval.decide",
            json!({"operation_id":operation,"approve":true})
        )
        .await
        .unwrap()["status"],
        "rejected"
    );
}

#[tokio::test]
async fn queue_close_denies_original_scope_even_with_fresh_unscoped_human_proof() {
    let (f, live, store, table) = fixture().await;
    let operation = pending(&f, &store, &table, "queued").await;
    let envelope = f.envelope(
        "sql.approval.decide",
        json!({"operation_id":operation,"approve":true}),
        true,
        "owner",
    );
    let guard = f.db.connection.lock().await;
    let available = f.db.queue.available_permits();
    let db = f.db.clone();
    let task = spawn(async move { db.handle(&envelope).await });
    while f.db.queue.available_permits() == available {
        yield_now().await;
    }
    live.0.store(false, Ordering::SeqCst);
    drop(guard);
    assert!(matches!(task.await.unwrap(), Err(SqlError::Authority(_))));
    assert_eq!(
        f.call("sql.operation.status", json!({"operation_id":operation}))
            .await
            .unwrap()["status"],
        "approval_required"
    );
}

#[tokio::test]
async fn queued_scoped_direct_mutation_is_rechecked_after_proof_consumption() {
    let (f, live, _, _) = fixture().await;
    let envelope = scoped(
        &f,
        "sql.store.create",
        json!({"name":"late","idempotency_key":"late"}),
    );
    let guard = f.db.connection.lock().await;
    let available = f.db.queue.available_permits();
    let db = f.db.clone();
    let task = spawn(async move { db.handle(&envelope).await });
    while f.db.queue.available_permits() == available {
        yield_now().await;
    }
    live.0.store(false, Ordering::SeqCst);
    drop(guard);
    assert!(matches!(task.await.unwrap(), Err(SqlError::Authority(_))));
    let result = f.call("sql.store.list", json!({})).await.unwrap();
    assert_eq!(result["stores"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn admitted_commit_survives_close_and_restored_live_scope_can_execute() {
    let (f, live, store, table) = fixture().await;
    let operation = pending(&f, &store, &table, "saved").await;
    // Simulate a restarted host hydrating this saved binding before connectors.
    let restored = Arc::new(Live(AtomicBool::new(true)));
    f.authority.clone().with_scope_validator(restored.clone());
    let reopened = Database::open(
        f.dir.path().join("sql.db"),
        Limits::default(),
        f.authority.clone(),
        vec![],
    )
    .await
    .unwrap();
    let result = reopened
        .handle(&f.envelope(
            "sql.approval.decide",
            json!({"operation_id":operation,"approve":true}),
            true,
            "owner",
        ))
        .await
        .unwrap();
    assert_eq!(result["status"], "committed");
    live.0.store(false, Ordering::SeqCst);
    restored.0.store(false, Ordering::SeqCst);
    assert_eq!(
        f.call(
            "sql.approval.decide",
            json!({"operation_id":operation,"approve":false})
        )
        .await
        .unwrap()["status"],
        "committed"
    );
}

#[tokio::test]
async fn legacy_none_scope_bytes_are_unchanged_and_copied_row_cannot_remove_binding() {
    let (f, _, store, table) = fixture().await;
    let operation = pending(&f, &store, &table, "tamper").await;
    let mut conn = f.db.connection.lock().await;
    let row = query("SELECT scope,prepared FROM __octo_sql_operations WHERE id=?")
        .bind(&operation)
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    let original: String = row.get("scope");
    let mut scope: Scope = from_str(&original).unwrap();
    scope.scope_binding = None;
    let legacy = to_string(&scope).unwrap();
    assert!(!legacy.contains("scope_binding"));
    assert_eq!(
        to_string(&from_str::<Scope>(&legacy).unwrap()).unwrap(),
        legacy
    );
    query("UPDATE __octo_sql_operations SET scope=? WHERE id=?")
        .bind(legacy)
        .bind(&operation)
        .execute(&mut *conn)
        .await
        .unwrap();
    drop(conn);
    assert!(matches!(
        f.call(
            "sql.approval.decide",
            json!({"operation_id":operation,"approve":true})
        )
        .await,
        Err(SqlError::Authority(_))
    ));
}

#[test]
fn legacy_none_serialization_and_prepared_hash_match_the_original_contract() {
    use octo_approval::{PreparedOperation, operation_hash};
    const LEGACY: &str = r#"{"presentation":{"resource":"","action":"change_data","objects":[]},"principal":"owner","connector":"console","conversation":"local","destination":"local","stores":null,"read":true,"write":true,"schema":false,"delegated":false,"policy_fingerprint":"v1","max_affected_rows":100,"timeout_ms":1000}"#;
    let scope: Scope = from_str(LEGACY).unwrap();
    assert!(scope.scope_binding.is_none());
    assert_eq!(to_string(&scope).unwrap(), LEGACY);
    let operation = PreparedOperation {
        id: "old".into(),
        principal: "owner".into(),
        database_identity: "db".into(),
        store: "store".into(),
        revision: 1,
        scope: LEGACY.into(),
        request: json!({}),
        effects: vec![],
        expires_at: 42,
    };
    assert_eq!(
        operation_hash(&operation).unwrap(),
        "d5c815f878620840c635cfba568052c7ca513061c49b870006238f08ac13dc20"
    );
}
