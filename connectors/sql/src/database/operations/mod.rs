mod decisions;
mod list;
mod projection;
mod scope;
pub(super) use scope::Scope;

use std::time::Instant;

use chrono::Utc;
use octo_approval::{PreparedOperation, VerifiedInvocation, operation_hash};
use serde::Deserialize;
use serde_json::{Value, from_str, json, to_string};
use sha2::{Digest, Sha256};
use sqlx::{Connection, Row, SqliteConnection, query};
use uuid::Uuid;

use super::{
    Database,
    catalog::{Store, StoreMutation},
    presentation::PresentationSnapshot,
    schema::snapshot,
};
use crate::{
    engine::{Analysis, Cancellation},
    error::{SqlError, invalid},
};

#[derive(Clone)]
pub(super) struct Operation {
    pub prepared: PreparedOperation,
    pub hash: String,
    pub status: String,
    pub result: Option<Value>,
    pub scope: Scope,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Decision {
    pub operation_id: String,
    pub approve: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct OperationStatus {
    pub operation_id: String,
    #[serde(default)]
    pub include_details: bool,
    #[serde(default)]
    pub include_approval: bool,
}

pub(super) fn validate_key(key: &str) -> Result<(), SqlError> {
    if key.is_empty() || key.len() > 128 || key.chars().any(char::is_control) {
        return Err(invalid(
            "idempotency_key must be 1..128 printable characters",
        ));
    }
    Ok(())
}
pub(super) fn request_hash(request: &Value) -> String {
    format!("{:x}", Sha256::digest(request.to_string().as_bytes()))
}
impl Database {
    pub(super) fn policy_fingerprint(&self) -> String {
        request_hash(
            &json!({"policy_version":1,"engine":"sqlx0.8.6/sqlite3.46/libsqlite0.30.1/sqlparser0.63.0","limits":self.limits}),
        )
    }
    pub(super) fn effect_summary(&self, analysis: &Analysis, store: &Store) -> Vec<String> {
        let mut effects = analysis.reasons.iter().cloned().collect::<Vec<_>>();
        effects.push(format!(
            "objects: {}",
            analysis
                .objects
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        ));
        effects.push(format!(
            "functions: {}",
            analysis
                .functions
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        ));
        effects.push(format!("schema fingerprint: {}", store.fingerprint));
        effects
    }
    pub(super) async fn operation(
        &self,
        conn: &mut SqliteConnection,
        id: &str,
    ) -> Result<Operation, SqlError> {
        let row = query("SELECT * FROM __octo_sql_operations WHERE id=?")
            .bind(id)
            .fetch_optional(conn)
            .await?
            .ok_or_else(|| SqlError::Authority("operation unavailable".into()))?;
        let prepared: PreparedOperation = from_str(row.get("prepared")).map_err(invalid)?;
        let stored_scope: &str = row.get("scope");
        if prepared.scope != stored_scope
            || operation_hash(&prepared).map_err(invalid)? != row.get::<String, _>("hash")
        {
            return Err(SqlError::Authority(
                "stored operation scope/hash mismatch".into(),
            ));
        }
        Ok(Operation {
            prepared,
            hash: row.get("hash"),
            scope: from_str(row.get("scope")).map_err(invalid)?,
            status: row.get("status"),
            result: row
                .get::<Option<String>, _>("result")
                .map(|v| from_str(&v).map_err(invalid))
                .transpose()?,
        })
    }
    pub(super) async fn reserve(
        &self,
        conn: &mut SqliteConnection,
        store: &Store,
        invocation: &VerifiedInvocation,
        key: &str,
        request: Value,
        analysis: &Analysis,
        pending: bool,
        authorization_deadline: Instant,
    ) -> Result<(Operation, bool), SqlError> {
        validate_key(key)?;
        self.authority
            .check_scope(invocation.scope_binding.as_ref())
            .map_err(SqlError::Authority)?;
        let hash = request_hash(&request);
        let mut transaction = conn.begin_with("BEGIN IMMEDIATE").await?;
        if let Some(row) = query("SELECT id,request_hash FROM __octo_sql_operations WHERE principal=? AND store_id=? AND idempotency_key=?")
            .bind(&invocation.origin.principal).bind(&store.id).bind(key).fetch_optional(&mut *transaction).await? {
            if row.get::<String,_>("request_hash") != hash { return Err(invalid("idempotency key content mismatch")); }
            let operation = self.operation(&mut transaction, row.get("id")).await?;
            transaction.commit().await?;
            return Ok((operation, false));
        }
        let now = Utc::now().timestamp();
        query("UPDATE __octo_sql_operations SET status='expired' WHERE status='pending' AND expires_at<=?")
            .bind(now).execute(&mut *transaction).await?;
        let count: i64 = query("SELECT count(*) FROM __octo_sql_operations WHERE status IN ('pending','claimed','unknown')")
            .fetch_one(&mut *transaction).await?.get(0);
        if count >= self.limits.max_pending as i64 {
            return Err(SqlError::Limit("pending operations".into()));
        }
        let mut scope = Scope::capture(
            invocation,
            self.policy_fingerprint(),
            self.limits.max_affected_rows,
            self.limits.timeout_ms,
        );
        scope.presentation = PresentationSnapshot::capture(store, &request, analysis);
        let remaining_ms = authorization_deadline
            .saturating_duration_since(Instant::now())
            .as_millis()
            .min(i64::MAX as u128) as i64;
        let lifetime_ms = remaining_ms.min(
            self.limits
                .approval_ttl_seconds
                .saturating_mul(1000)
                .min(i64::MAX as u64) as i64,
        );
        let expires_at = Utc::now().timestamp_millis().saturating_add(lifetime_ms) / 1000;
        if lifetime_ms == 0 || expires_at <= Utc::now().timestamp() {
            return Err(SqlError::Authority("invocation lifetime too short".into()));
        }
        let prepared = PreparedOperation {
            id: Uuid::now_v7().to_string(),
            principal: invocation.origin.principal.clone(),
            database_identity: self.identity.clone(),
            store: store.id.clone(),
            revision: store.revision,
            scope: to_string(&scope).map_err(invalid)?,
            request,
            effects: self.effect_summary(analysis, store),
            expires_at,
        };
        let operation_hash = operation_hash(&prepared).map_err(invalid)?;
        let status = if pending { "pending" } else { "claimed" };
        self.authority
            .check_scope(scope.scope_binding.as_ref())
            .map_err(SqlError::Authority)?;
        query("INSERT INTO __octo_sql_operations(id,hash,principal,store_id,prepared,scope,status,expires_at,idempotency_key,request_hash) VALUES(?,?,?,?,?,?,?,?,?,?)")
            .bind(&prepared.id).bind(&operation_hash).bind(&prepared.principal).bind(&prepared.store).bind(to_string(&prepared).map_err(invalid)?)
            .bind(to_string(&scope).map_err(invalid)?).bind(status).bind(prepared.expires_at).bind(key).bind(hash).execute(&mut *transaction).await?;
        query("INSERT INTO __octo_sql_audit(operation_id,principal,outcome,detail,created_at) VALUES(?,?,?,?,?)")
            .bind(&prepared.id).bind(&prepared.principal).bind(status).bind(&operation_hash).bind(now)
            .execute(&mut *transaction).await?;
        transaction.commit().await?;
        Ok((
            Operation {
                prepared,
                hash: operation_hash,
                status: status.into(),
                result: None,
                scope,
            },
            true,
        ))
    }
    pub(super) fn operation_response(&self, operation: &Operation) -> Value {
        if let Some(result) = &operation.result {
            return result.clone();
        }
        if operation.status == "pending" && operation.prepared.expires_at <= Utc::now().timestamp()
        {
            return json!({"status":"expired","operation_id":operation.prepared.id});
        }
        if operation.status == "pending" {
            json!({"status":"approval_required","operation_id":operation.prepared.id,"operation_hash":operation.hash,
                "expires_at":operation.prepared.expires_at,"details":operation.prepared.request,
                "effects":operation.prepared.effects,"max_affected_rows":operation.scope.max_affected_rows,"timeout_ms":operation.scope.timeout_ms,"warning":"No verified backup is provided. Committed destruction may be irreversible."})
        } else {
            json!({"status":operation.status,"operation_id":operation.prepared.id})
        }
    }
    pub(super) async fn prepare_drop(
        &self,
        conn: &mut SqliteConnection,
        request: StoreMutation,
        invocation: &VerifiedInvocation,
        cancellation: Cancellation,
    ) -> Result<Value, SqlError> {
        if !invocation.access.schema || !invocation.access.write {
            return Err(SqlError::Authority(
                "drop requires schema/write capabilities".into(),
            ));
        }
        validate_key(&request.idempotency_key)?;
        let value = json!({"kind":"drop_store","args":request});
        if let Some(row) = query("SELECT id,request_hash FROM __octo_sql_operations WHERE principal=? AND store_id=? AND idempotency_key=?")
            .bind(&invocation.origin.principal).bind(&request.store_id).bind(&request.idempotency_key).fetch_optional(&mut *conn).await? {
            if row.get::<String,_>("request_hash") != request_hash(&value) { return Err(invalid("idempotency key content mismatch")); }
            if !invocation.access.read || !invocation.access.permits(&request.store_id, &invocation.origin.principal, &invocation.origin.principal) { return Err(SqlError::Authority("receipt read unavailable".into())); }
            return Ok(self.operation_response(&self.operation(conn,row.get("id")).await?));
        }
        let store = self.load_store(conn, &request.store_id, invocation).await?;
        let (actual, objects) = snapshot(conn, &store).await?;
        if store.revision != request.expected_revision || actual != store.fingerprint {
            return Err(SqlError::Stale);
        }
        let analysis = Analysis { objects: objects.iter().map(|o| o.name.clone()).collect(), requires_approval: true, schema_changes: true,
            reasons: ["DROP STORE destroys all objects/data and their semantic metadata; no backup is provided".into()].into(), ..Analysis::default() };
        let (operation, _) = self
            .reserve(
                conn,
                &store,
                invocation,
                &request.idempotency_key,
                value,
                &analysis,
                true,
                cancellation
                    .deadline()
                    .unwrap_or_else(|| Instant::now() + invocation.remaining),
            )
            .await?;
        Ok(self.operation_response(&operation))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::json;
    use sqlx::{Row, query};
    use tokio::time::sleep;

    use crate::database::tests::Fixture;
    use crate::{Database, Limits};

    #[tokio::test]
    async fn approvals_expire_reject_audit_and_never_reactivate() {
        let f = Fixture::new(Limits {
            approval_ttl_seconds: 1,
            ..Limits::default()
        })
        .await;
        let store = f.store().await;
        let table = f.table(&store).await;
        let pending = f.call("sql.execute",json!({"store_id":store,"expected_revision":2,"sql":format!("DELETE FROM {table}"),"idempotency_key":"expire"})).await.unwrap();
        sleep(Duration::from_millis(1100)).await;
        let operation = pending["operation_id"].as_str().unwrap();
        let expired = f
            .call(
                "sql.approval.decide",
                json!({"operation_id":operation,"approve":true}),
            )
            .await
            .unwrap();
        assert_eq!(expired["status"], "expired");
        let pending = f.call("sql.execute",json!({"store_id":store,"expected_revision":2,"sql":format!("DELETE FROM {table}"),"idempotency_key":"reject"})).await.unwrap();
        let operation = pending["operation_id"].as_str().unwrap();
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
        let mut conn = f.db.connection.lock().await;
        let row = query("SELECT actor,created_at FROM __octo_sql_audit WHERE operation_id=? AND outcome='rejected'").bind(operation).fetch_one(&mut *conn).await.unwrap();
        assert_eq!(row.get::<String, _>("actor"), "owner");
        assert!(row.get::<i64, _>("created_at") > 0);
    }
    #[tokio::test]
    async fn changed_policy_invalidates_pending_but_preserves_committed_receipt() {
        let f = Fixture::new(Limits::default()).await;
        let store = f.store().await;
        let table = f.table(&store).await;
        let insert = json!({"store_id":store,"expected_revision":2,"sql":format!("INSERT INTO {table} VALUES(1,1.5)"),"idempotency_key":"insert"});
        let committed = f.call("sql.execute", insert.clone()).await.unwrap();
        let pending = f.call("sql.execute",json!({"store_id":store,"expected_revision":3,"sql":format!("DELETE FROM {table}"),"idempotency_key":"delete"})).await.unwrap();
        let changed = Database::open(
            f.dir.path().join("sql.db"),
            Limits {
                max_affected_rows: 10,
                ..Limits::default()
            },
            f.authority.clone(),
            vec![],
        )
        .await
        .unwrap();
        let decision = f.envelope(
            "sql.approval.decide",
            json!({"operation_id":pending["operation_id"],"approve":true}),
            true,
            "owner",
        );
        assert!(changed.handle(&decision).await.is_err());
        assert_eq!(
            changed
                .handle(&f.envelope("sql.execute", insert, false, "owner"))
                .await
                .unwrap(),
            committed
        );
        let mut conn = changed.connection.lock().await;
        query("INSERT INTO __octo_sql_migrations VALUES(2)")
            .execute(&mut *conn)
            .await
            .unwrap();
        drop(conn);
        assert!(
            Database::open(
                f.dir.path().join("sql.db"),
                Limits::default(),
                f.authority.clone(),
                vec![]
            )
            .await
            .is_err()
        );
    }
    #[tokio::test]
    async fn private_projection_preserves_original_full_binding_after_terminal_decision_without_engine_paths()
     {
        use octo_approval::{Access, Origin, ScopeBinding, ScopeValidator};
        use octo_core::{ConnectorId, Envelope, EventKind};
        use std::sync::Arc;
        struct Live;
        impl ScopeValidator for Live {
            fn allows(&self, _binding: &ScopeBinding) -> bool {
                true
            }
        }
        let f = Fixture::new(Limits::default()).await;
        let authority = f.authority.clone().with_scope_validator(Arc::new(Live));
        let store = f.store().await;
        let table = f.table(&store).await;
        let original = ScopeBinding::new("account/account", "origin-epoch").unwrap();
        let human = authority.issue(
            Origin {
                principal: "owner".into(),
                connector: "web".into(),
                conversation: "user:owner".into(),
                private_destination: "user:owner".into(),
                may_approve: true,
            },
            Access::owner(),
        );
        let scoped = authority.bind_scope(&human, original.clone()).unwrap();
        let request=Envelope::new(ConnectorId::new("agent"),EventKind::new("sql.execute"),json!({"store_id":store,"expected_revision":2,"sql":format!("DELETE FROM {table}"),"idempotency_key":"private-projection"})).with_target(ConnectorId::new("sql"));
        let proof = authority.bind(&scoped, &request).unwrap();
        let pending = f.db.handle(&request.with_context(proof)).await.unwrap();
        assert!(pending.get("origin_scope").is_none());
        let operation = pending["operation_id"].as_str().unwrap();
        let queue = f.call("sql.approval.list", json!({})).await.unwrap();
        assert_eq!(queue["items"][0]["origin_scope"], json!(original));
        assert_eq!(queue["items"][0]["decision_connector"], "web");
        let public = f
            .call("sql.operation.status", json!({"operation_id":operation}))
            .await
            .unwrap();
        assert!(public.get("origin_scope").is_none());
        let private = f
            .call(
                "sql.operation.status",
                json!({"operation_id":operation,"include_approval":true,"include_details":true}),
            )
            .await
            .unwrap();
        assert_eq!(private["origin_scope"], json!(original));
        assert_eq!(private["operation_hash"], pending["operation_hash"]);
        assert_eq!(private["can_decide"], true);
        assert!(!private.to_string().contains(f.dir.path().to_str().unwrap()));
        let decision = Envelope::new(
            ConnectorId::new("web"),
            EventKind::new("sql.approval.decide"),
            json!({"operation_id":operation,"approve":true}),
        )
        .with_target(ConnectorId::new("sql"));
        let proof = authority.bind_decision(&human, &decision).unwrap();
        let result = f.db.handle(&decision.with_context(proof)).await.unwrap();
        assert_eq!(result["status"], "committed");
        let after = f
            .call(
                "sql.operation.status",
                json!({"operation_id":operation,"include_approval":true}),
            )
            .await
            .unwrap();
        assert_eq!(after["status"], "committed");
        assert_eq!(after["origin_scope"], json!(original));
        assert_eq!(after["can_decide"], false);
        assert!(
            f.call("sql.approval.list", json!({})).await.unwrap()["items"]
                .as_array()
                .unwrap()
                .is_empty()
        );
    }
    #[tokio::test]
    async fn private_projection_requires_human_private_authority_beyond_regular_status_read_access()
    {
        use octo_approval::{Access, Origin};
        use octo_core::{ConnectorId, Envelope, EventKind};
        let f = Fixture::new(Limits::default()).await;
        let store = f.store().await;
        let table = f.table(&store).await;
        let pending=f.call("sql.execute",json!({"store_id":store,"expected_revision":2,"sql":format!("DELETE FROM {table}"),"idempotency_key":"projection-eligibility"})).await.unwrap();
        for private in [false, true] {
            let ingress = f.authority.issue(
                Origin {
                    principal: "owner".into(),
                    connector: "web".into(),
                    conversation: "worker".into(),
                    private_destination: "worker".into(),
                    may_approve: false,
                },
                Access::owner(),
            );
            let request = Envelope::new(
                ConnectorId::new("agent"),
                EventKind::new("sql.operation.status"),
                json!({"operation_id":pending["operation_id"],"include_approval":private}),
            )
            .with_target(ConnectorId::new("sql"));
            let proof = f.authority.bind(&ingress, &request).unwrap();
            let result = f.db.handle(&request.with_context(proof)).await;
            assert_eq!(result.is_err(), private);
        }
    }
}
