use super::{Decision, Operation, OperationStatus};
use crate::database::{Database, schema::snapshot};
use crate::{engine::Cancellation, error::SqlError};
use chrono::Utc;
use octo_approval::VerifiedInvocation;
use serde_json::{Value, json};
use sqlx::{Connection, SqliteConnection, query};
impl Database {
    pub(in crate::database) async fn decide(
        &self,
        conn: &mut SqliteConnection,
        request: Decision,
        invocation: &VerifiedInvocation,
        cancellation: Cancellation,
    ) -> Result<Value, SqlError> {
        let mut transaction = conn.begin_with("BEGIN IMMEDIATE").await?;
        let operation = self
            .operation(&mut transaction, &request.operation_id)
            .await?;
        if operation.prepared.principal != invocation.origin.principal
            || invocation.delegated
            || !invocation.origin.may_approve
            || operation.scope.connector != invocation.origin.connector
            || operation.scope.destination != invocation.origin.conversation
        {
            return Err(SqlError::Authority(
                "operation belongs to another human/private channel".into(),
            ));
        }
        if !invocation.access.permits(
            &operation.prepared.store,
            &operation.prepared.principal,
            &invocation.origin.principal,
        ) || (operation.scope.read && !invocation.access.read)
            || (operation.scope.write && !invocation.access.write)
            || (operation.scope.schema && !invocation.access.schema)
        {
            return Err(SqlError::Authority(
                "current operation capabilities unavailable".into(),
            ));
        }
        if operation.status != "pending" {
            return Ok(self.operation_response(&operation));
        }
        if operation.scope.policy_fingerprint != self.policy_fingerprint() {
            return Err(SqlError::Stale);
        }
        if operation.prepared.expires_at <= Utc::now().timestamp() {
            query(
                "UPDATE __octo_sql_operations SET status='expired' WHERE id=? AND status='pending'",
            )
            .bind(&operation.prepared.id)
            .execute(&mut *transaction)
            .await?;
            self.audit_decision(&mut transaction, &operation, invocation, "expired")
                .await?;
            transaction.commit().await?;
            return Ok(json!({"status":"expired","operation_id":operation.prepared.id}));
        }
        let cancellation = match self
            .pending_cancellations
            .lock()
            .await
            .get(&operation.prepared.id)
        {
            Some(original) => cancellation.combined(original),
            None => cancellation,
        };
        if !request.approve || cancellation.is_cancelled() {
            let status = if cancellation.is_cancelled() {
                "cancelled"
            } else {
                "rejected"
            };
            query("UPDATE __octo_sql_operations SET status=? WHERE id=? AND status='pending'")
                .bind(status)
                .bind(&operation.prepared.id)
                .execute(&mut *transaction)
                .await?;
            self.audit_decision(&mut transaction, &operation, invocation, status)
                .await?;
            transaction.commit().await?;
            return Ok(json!({"status":status,"operation_id":operation.prepared.id}));
        }
        let store = self
            .load_store(&mut transaction, &operation.prepared.store, invocation)
            .await?;
        let (actual, _) = snapshot(&mut transaction, &store).await?;
        if store.revision != operation.prepared.revision || actual != store.fingerprint {
            return Err(SqlError::Stale);
        }
        // Durable claim is separate from user SQL. A failed or interrupted
        // execution can never turn this grant back into a pending approval.
        self.check_operation_scope(&operation)?;
        query("UPDATE __octo_sql_operations SET status='claimed' WHERE id=? AND status='pending'")
            .bind(&operation.prepared.id)
            .execute(&mut *transaction)
            .await?;
        self.audit_decision(&mut transaction, &operation, invocation, "approved")
            .await?;
        transaction.commit().await?;
        self.perform(conn, operation, true, cancellation).await
    }
    pub(in crate::database) async fn audit_decision(
        &self,
        conn: &mut SqliteConnection,
        operation: &Operation,
        invocation: &VerifiedInvocation,
        outcome: &str,
    ) -> Result<(), SqlError> {
        let detail = json!({"operation_hash":operation.hash,"connector":invocation.origin.connector,
            "conversation":invocation.origin.conversation});
        query("INSERT INTO __octo_sql_audit(operation_id,principal,outcome,detail,created_at,actor) VALUES(?,?,?,?,?,?)")
            .bind(&operation.prepared.id).bind(&operation.prepared.principal).bind(outcome)
            .bind(detail.to_string()).bind(Utc::now().timestamp()).bind(&invocation.origin.principal)
            .execute(conn).await?;
        Ok(())
    }
    pub(in crate::database) async fn operation_status(
        &self,
        conn: &mut SqliteConnection,
        request: OperationStatus,
        invocation: &VerifiedInvocation,
    ) -> Result<Value, SqlError> {
        let mut operation = self.operation(conn, &request.operation_id).await?;
        if operation.prepared.principal != invocation.origin.principal {
            return Err(SqlError::Authority("operation unavailable".into()));
        }
        if !invocation.access.read
            || !invocation.access.permits(
                &operation.prepared.store,
                &operation.prepared.principal,
                &invocation.origin.principal,
            )
        {
            return Err(SqlError::Authority(
                "current store read access unavailable".into(),
            ));
        }
        if operation.status == "pending" && operation.prepared.expires_at <= Utc::now().timestamp()
        {
            query(
                "UPDATE __octo_sql_operations SET status='expired' WHERE id=? AND status='pending'",
            )
            .bind(&operation.prepared.id)
            .execute(conn)
            .await?;
            operation.status = "expired".into();
        }
        if request.include_approval {
            self.require_private_approval_reader(invocation)?;
            return Ok(self.approval_projection(&operation, request.include_details));
        }
        Ok(if request.include_details {
            self.operation_details(&operation)
        } else {
            self.operation_response(&operation)
        })
    }
}
