use std::{
    mem::replace,
    time::{Duration, Instant},
};

use chrono::Utc;
use octo_approval::{VerifiedInvocation, operation_hash};
use serde::{Deserialize, Serialize};
use serde_json::{Value, from_str, json};
use sqlx::{Connection, Row, SqliteConnection, query};

use super::{
    Database,
    operations::{Operation, request_hash, validate_key},
    schema::{MetadataDelta, local_identifier, quoted, snapshot, validate_semantics},
};
use crate::{
    engine::{Cancellation, EngineError, ExecutionPermit, SqlParam},
    error::{SqlError, invalid},
};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct QueryRequest {
    pub store_id: String,
    pub expected_revision: u64,
    pub sql: String,
    #[serde(default)]
    pub parameters: Vec<SqlParam>,
    #[serde(default)]
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub metadata: Vec<MetadataDelta>,
    #[serde(default)]
    pub schema_metadata: Option<SchemaMetadata>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SchemaMetadata {
    pub table: String,
    pub semantics: Value,
    pub columns: Vec<(String, Value)>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SchemaRequest {
    pub store_id: String,
    pub expected_revision: u64,
    pub idempotency_key: String,
    pub table: String,
    pub columns: Vec<SchemaColumn>,
    #[serde(default = "empty_semantics")]
    pub semantics: Value,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SchemaColumn {
    pub name: String,
    pub data_type: String,
    #[serde(default)]
    pub primary_key: bool,
    #[serde(default = "nullable")]
    pub nullable: bool,
    #[serde(default = "empty_semantics")]
    pub semantics: Value,
}
fn empty_semantics() -> Value {
    json!({})
}
fn nullable() -> bool {
    true
}

impl Database {
    pub(super) async fn apply_schema(
        &self,
        conn: &mut SqliteConnection,
        request: SchemaRequest,
        invocation: &VerifiedInvocation,
        cancellation: Cancellation,
    ) -> Result<Value, SqlError> {
        local_identifier(&request.table)?;
        validate_semantics(&request.semantics)?;
        if request.columns.is_empty() || request.columns.len() > 128 {
            return Err(invalid("schema needs 1..128 columns"));
        }
        let store = self.load_store(conn, &request.store_id, invocation).await?;
        let mut definitions = Vec::new();
        let mut semantics = Vec::new();
        for column in request.columns {
            local_identifier(&column.name)?;
            let data_type = column.data_type.to_ascii_uppercase();
            if !matches!(
                data_type.as_str(),
                "INTEGER" | "REAL" | "TEXT" | "BLOB" | "NUMERIC"
            ) {
                return Err(invalid("unsupported structured column type"));
            }
            validate_semantics(&column.semantics)?;
            definitions.push(format!(
                "{} {}{}{}",
                quoted(&column.name),
                data_type,
                if column.primary_key {
                    " PRIMARY KEY"
                } else {
                    ""
                },
                if column.nullable { "" } else { " NOT NULL" }
            ));
            semantics.push((column.name, column.semantics));
        }
        let physical = format!("{}{}", store.namespace(), request.table);
        let query = QueryRequest {
            store_id: request.store_id,
            expected_revision: request.expected_revision,
            sql: format!(
                "CREATE TABLE {} ({})",
                quoted(&physical),
                definitions.join(",")
            ),
            parameters: Vec::new(),
            idempotency_key: Some(request.idempotency_key),
            metadata: Vec::new(),
            schema_metadata: Some(SchemaMetadata {
                table: physical,
                semantics: request.semantics,
                columns: semantics,
            }),
        };
        self.prepare_sql(conn, query, invocation, cancellation)
            .await
    }
    pub(super) async fn prepare_sql(
        &self,
        conn: &mut SqliteConnection,
        request: QueryRequest,
        invocation: &VerifiedInvocation,
        cancellation: Cancellation,
    ) -> Result<Value, SqlError> {
        let request_value = json!({"kind":"sql","args":request});
        // Resolve idempotency before revision checks; a successful retry must
        // return its saved receipt even though the write bumped the revision.
        if let Some(key) = &request.idempotency_key {
            validate_key(key)?;
            if let Some(row) = query("SELECT id,request_hash FROM __octo_sql_operations WHERE principal=? AND store_id=? AND idempotency_key=?")
                .bind(&invocation.origin.principal).bind(&request.store_id).bind(key).fetch_optional(&mut *conn).await? {
                if row.get::<String,_>("request_hash") != request_hash(&request_value) { return Err(invalid("idempotency key content mismatch")); }
                let operation = self.operation(conn, row.get("id")).await?;
                if !invocation.access.read { return Err(SqlError::Authority("receipt read unavailable".into())); }
                self.load_store(conn, &request.store_id, invocation).await?;
                return Ok(self.operation_response(&operation));
            }
        }
        let store = self
            .checked_store(
                conn,
                &request.store_id,
                request.expected_revision,
                invocation,
            )
            .await?;
        let policy = store.policy(invocation);
        let analysis = self.engine.analyze(conn, &policy, &request.sql).await?;
        let mutating = analysis.writes || analysis.schema_changes;
        if (!request.metadata.is_empty() || request.schema_metadata.is_some())
            && (!analysis.schema_changes || !invocation.access.schema)
        {
            return Err(SqlError::Authority(
                "SQL metadata delta requires an authorized schema mutation".into(),
            ));
        }
        if !mutating {
            if !request.metadata.is_empty() || request.schema_metadata.is_some() {
                return Err(invalid("metadata requires schema mutation"));
            }
            let mut transaction = conn.begin().await?;
            self.checked_store(
                &mut transaction,
                &request.store_id,
                request.expected_revision,
                invocation,
            )
            .await?;
            let result = self
                .engine
                .execute(
                    &mut transaction,
                    &policy,
                    &request.sql,
                    &request.parameters,
                    ExecutionPermit::ordinary(),
                    cancellation,
                )
                .await;
            match result {
                Ok(result) => {
                    transaction.commit().await?;
                    Ok(json!({"status":"completed","revision":store.revision,"result":result}))
                }
                Err(error) => {
                    let _ = transaction.rollback().await;
                    Err(error.into())
                }
            }
        } else {
            let key = request
                .idempotency_key
                .as_deref()
                .ok_or_else(|| invalid("writes/schema require idempotency_key"))?;
            if cancellation.is_cancelled() {
                return Err(SqlError::Engine(EngineError::Cancelled));
            }
            let (operation, fresh) = self
                .reserve(
                    conn,
                    &store,
                    invocation,
                    key,
                    request_value,
                    &analysis,
                    analysis.requires_approval,
                    cancellation
                        .deadline()
                        .unwrap_or_else(|| Instant::now() + invocation.remaining),
                )
                .await?;
            if analysis.requires_approval || !fresh {
                return Ok(self.operation_response(&operation));
            }
            self.perform(conn, operation, false, cancellation).await
        }
    }
    pub(super) async fn perform(
        &self,
        conn: &mut SqliteConnection,
        operation: Operation,
        approved: bool,
        cancellation: Cancellation,
    ) -> Result<Value, SqlError> {
        let remaining = operation
            .prepared
            .expires_at
            .saturating_mul(1000)
            .saturating_sub(Utc::now().timestamp_millis())
            .max(0) as u64;
        let cancellation =
            cancellation.with_deadline(Instant::now() + Duration::from_millis(remaining));
        let outcome = self
            .perform_transaction(conn, &operation, approved, cancellation)
            .await;
        match outcome {
            Ok(value) => Ok(value),
            Err(error) => {
                // Finish/close the previous connection before proving its
                // outcome. Cancellation never implies that COMMIT was undone.
                let fresh = self.engine.connect().await?;
                let previous = replace(conn, fresh);
                if previous.close().await.is_err() {
                    query("UPDATE __octo_sql_operations SET status='unknown' WHERE id=? AND status!='committed'")
                        .bind(&operation.prepared.id).execute(&mut *conn).await?;
                    return Ok(json!({"status":"unknown","operation_id":operation.prepared.id}));
                }
                if let Some(receipt) =
                    query("SELECT result FROM __octo_sql_receipts WHERE operation_id=? AND hash=?")
                        .bind(&operation.prepared.id)
                        .bind(&operation.hash)
                        .fetch_optional(&mut *conn)
                        .await?
                {
                    return from_str(receipt.get("result")).map_err(invalid);
                }
                // A durable claim is never reverted to pending. If this write
                // fails too, the existing claimed/unknown state blocks replay.
                let result = json!({"status":"rolled_back","operation_id":operation.prepared.id,"error":error.to_string()});
                query("UPDATE __octo_sql_operations SET status='rolled_back',result=? WHERE id=? AND status IN ('claimed','unknown')")
                    .bind(result.to_string()).bind(&operation.prepared.id).execute(&mut *conn).await?;
                query("INSERT INTO __octo_sql_audit(operation_id,principal,outcome,detail,created_at) VALUES(?,?,'rolled_back',?,?)")
                    .bind(&operation.prepared.id).bind(&operation.prepared.principal).bind(&operation.hash).bind(Utc::now().timestamp()).execute(conn).await?;
                Ok(result)
            }
        }
    }
    async fn perform_transaction(
        &self,
        conn: &mut SqliteConnection,
        operation: &Operation,
        approved: bool,
        cancellation: Cancellation,
    ) -> Result<Value, SqlError> {
        self.check_operation_scope(operation)?;
        if operation.scope.policy_fingerprint != self.policy_fingerprint() {
            return Err(SqlError::Stale);
        }
        if operation.prepared.database_identity != self.identity
            || operation_hash(&operation.prepared).map_err(invalid)? != operation.hash
            || operation.prepared.expires_at <= Utc::now().timestamp()
        {
            return Err(SqlError::Authority(
                "operation identity/hash/expiry mismatch".into(),
            ));
        }
        let invocation = operation.scope.invocation();
        let mut transaction = conn.begin_with("BEGIN IMMEDIATE").await?;
        let mut store = self
            .load_store(&mut transaction, &operation.prepared.store, &invocation)
            .await?;
        let (actual, _) = snapshot(&mut transaction, &store).await?;
        if store.revision != operation.prepared.revision || actual != store.fingerprint {
            return Err(SqlError::Stale);
        }
        let status = query("SELECT status FROM __octo_sql_operations WHERE id=?")
            .bind(&operation.prepared.id)
            .fetch_one(&mut *transaction)
            .await?
            .get::<String, _>(0);
        if status != "claimed" && status != "unknown" {
            return Err(SqlError::Authority("operation is not claimed".into()));
        }
        let permit = if approved {
            ExecutionPermit::approved()
        } else {
            ExecutionPermit::ordinary()
        };
        self.check_operation_scope(operation)?;
        let applied = self
            .apply_operation(
                &mut transaction,
                &mut store,
                operation,
                &invocation,
                permit,
                cancellation,
            )
            .await;
        let value = match applied {
            Ok(value) => value,
            Err(error) => {
                let _ = transaction.rollback().await;
                return Err(error);
            }
        };
        if value.to_string().len() > self.limits.max_bytes {
            let _ = transaction.rollback().await;
            return Err(SqlError::Limit("saved result bytes".into()));
        }
        query("INSERT INTO __octo_sql_receipts(operation_id,hash,result) VALUES(?,?,?)")
            .bind(&operation.prepared.id)
            .bind(&operation.hash)
            .bind(value.to_string())
            .execute(&mut *transaction)
            .await?;
        query("UPDATE __octo_sql_operations SET status='committed',result=? WHERE id=?")
            .bind(value.to_string())
            .bind(&operation.prepared.id)
            .execute(&mut *transaction)
            .await?;
        query("INSERT INTO __octo_sql_audit(operation_id,principal,outcome,detail,created_at) VALUES(?,?,'committed',?,?)")
            .bind(&operation.prepared.id).bind(&operation.prepared.principal).bind(&operation.hash).bind(Utc::now().timestamp()).execute(&mut *transaction).await?;
        transaction.commit().await?;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use sqlx::{Row, query};

    use crate::{Limits, database::tests::Fixture, engine::Cancellation};

    #[tokio::test]
    async fn rollback_limit_keeps_receipt_catalog_and_data_atomic() {
        let f = Fixture::new(Limits {
            max_affected_rows: 2,
            ..Limits::default()
        })
        .await;
        let store = f.store().await;
        let table = f.table(&store).await;
        let insert = json!({"store_id":store,"expected_revision":2,"sql":format!("INSERT INTO {table} VALUES(1,1),(2,2),(3,3)"),"idempotency_key":"too-many"});
        let result = f.call("sql.execute", insert.clone()).await.unwrap();
        assert_eq!(result["status"], "rolled_back", "{result}");
        assert_eq!(f.call("sql.execute", insert).await.unwrap(), result);
        let mut conn = f.db.connection.lock().await;
        assert_eq!(
            query(&format!("SELECT count(*) FROM {table}"))
                .fetch_one(&mut *conn)
                .await
                .unwrap()
                .get::<i64, _>(0),
            0
        );
        assert_eq!(
            query("SELECT revision FROM __octo_sql_stores WHERE id=?")
                .bind(&store)
                .fetch_one(&mut *conn)
                .await
                .unwrap()
                .get::<i64, _>(0),
            2
        );
        assert_eq!(
            query("SELECT count(*) FROM __octo_sql_receipts WHERE operation_id=?")
                .bind(result["operation_id"].as_str().unwrap())
                .fetch_one(&mut *conn)
                .await
                .unwrap()
                .get::<i64, _>(0),
            0
        );
    }
    #[tokio::test]
    async fn archived_store_drop_releases_quota_and_keeps_status_receipt() {
        let f = Fixture::new(Limits {
            max_stores: 1,
            ..Limits::default()
        })
        .await;
        let store = f.store().await;
        let table = f.table(&store).await;
        f.call("sql.execute",json!({"store_id":store,"expected_revision":2,"sql":format!("INSERT INTO {table} VALUES(1,1)"),"idempotency_key":"insert"})).await.unwrap();
        let archive = json!({"store_id":store,"expected_revision":3,"idempotency_key":"archive"});
        let archived = f.call("sql.store.archive", archive.clone()).await.unwrap();
        assert_eq!(
            f.call("sql.store.archive", archive.clone()).await.unwrap(),
            archived
        );
        let mut changed = archive;
        changed["expected_revision"] = json!(2);
        assert!(f.call("sql.store.archive", changed).await.is_err());
        let request = json!({"store_id":store,"expected_revision":4,"idempotency_key":"drop"});
        let pending = f.call("sql.store.drop", request.clone()).await.unwrap();
        let result = f
            .call(
                "sql.approval.decide",
                json!({"operation_id":pending["operation_id"],"approve":true}),
            )
            .await
            .unwrap();
        assert_eq!(result["status"], "committed", "{result}");
        assert_eq!(result["affected_rows"], 1);
        assert_eq!(
            f.call(
                "sql.operation.status",
                json!({"operation_id":pending["operation_id"]})
            )
            .await
            .unwrap(),
            result
        );
        assert_eq!(f.call("sql.store.drop", request).await.unwrap(), result);
        assert_eq!(
            f.call(
                "sql.store.create",
                json!({"name":"replacement","idempotency_key":"replacement"})
            )
            .await
            .unwrap()["status"],
            "created"
        );
    }
    #[tokio::test]
    async fn scoped_update_is_bounded_and_broad_update_requires_approval() {
        let f = Fixture::new(Limits::default()).await;
        let store = f.store().await;
        let table = f.table(&store).await;
        f.call("sql.execute",json!({"store_id":store,"expected_revision":2,"sql":format!("INSERT INTO {table} VALUES(1,1),(2,2)"),"idempotency_key":"insert"})).await.unwrap();
        let scoped=f.call("sql.execute",json!({"store_id":store,"expected_revision":3,"sql":format!("UPDATE {table} SET amount=? WHERE id=?"),
            "parameters":[{"type":"real","value":3.0},{"type":"integer","value":1}],"idempotency_key":"scoped"})).await.unwrap();
        assert_eq!(scoped["status"], "committed", "{scoped}");
        assert_eq!(scoped["result"]["affected_rows"], 1);
        for (key, sql) in [
            ("broad", format!("UPDATE {table} SET amount=0")),
            ("constant", format!("UPDATE {table} SET amount=0 WHERE 1=1")),
        ] {
            assert_eq!(
                f.call(
                    "sql.execute",
                    json!({"store_id":store,"expected_revision":4,"sql":sql,"idempotency_key":key})
                )
                .await
                .unwrap()["status"],
                "approval_required"
            );
        }
    }
    #[tokio::test]
    async fn cancellation_before_human_claim_cannot_be_reactivated_by_retry() {
        let f = Fixture::new(Limits::default()).await;
        let store = f.store().await;
        let table = f.table(&store).await;
        let request = json!({"store_id":store,"expected_revision":2,"sql":format!("DELETE FROM {table}"),"idempotency_key":"delete"});
        let cancellation = Cancellation::default();
        let pending =
            f.db.handle_with_cancellation(
                &f.envelope("sql.execute", request.clone(), false, "owner"),
                cancellation.clone(),
            )
            .await
            .unwrap();
        cancellation.cancel();
        f.call("sql.execute", request).await.unwrap();
        let result = f
            .call(
                "sql.approval.decide",
                json!({"operation_id":pending["operation_id"],"approve":true}),
            )
            .await
            .unwrap();
        assert_eq!(result["status"], "cancelled");
        assert_eq!(
            f.call(
                "sql.approval.decide",
                json!({"operation_id":pending["operation_id"],"approve":true})
            )
            .await
            .unwrap()["status"],
            "cancelled"
        );
    }
}
