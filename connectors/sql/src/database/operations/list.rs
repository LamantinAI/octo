use chrono::Utc;
use octo_approval::VerifiedInvocation;
use serde_json::{Value, json};
use sqlx::{SqliteConnection, query_as};

use super::super::Database;
use crate::error::SqlError;

impl Database {
    /// Human-private pending queue. Ownership and current grants are evaluated
    /// by the executor, not by a transport reading the SQLite file directly.
    pub(in crate::database) async fn pending_operations(
        &self,
        conn: &mut SqliteConnection,
        actor: &VerifiedInvocation,
    ) -> Result<Value, SqlError> {
        self.require_private_approval_reader(actor)?;
        let ids:Vec<(String,)>=query_as("SELECT id FROM __octo_sql_operations WHERE principal=? AND status='pending' AND expires_at>? ORDER BY expires_at,id LIMIT 1000")
            .bind(&actor.origin.principal).bind(Utc::now().timestamp()).fetch_all(&mut *conn).await?;
        let mut items = Vec::new();
        for (id,) in ids {
            let operation = self.operation(conn, &id).await?;
            if !actor.access.permits(
                &operation.prepared.store,
                &operation.prepared.principal,
                &actor.origin.principal,
            ) || self.check_operation_scope(&operation).is_err()
            {
                continue;
            }
            items.push(self.approval_projection(&operation, false));
        }
        Ok(json!({"items":items}))
    }
}
