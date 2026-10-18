use octo_approval::VerifiedInvocation;
use serde_json::{Value, from_value, json, to_value};
use sqlx::{SqliteConnection, query};

use super::{
    Database,
    catalog::Store,
    execute::{QueryRequest, SchemaMetadata},
    operations::Operation,
    schema::{MetadataDelta, quoted, reconcile, snapshot},
};
use crate::{
    engine::{Cancellation, ExecutionPermit},
    error::{SqlError, invalid},
};

impl Database {
    pub(super) async fn apply_operation(
        &self,
        conn: &mut SqliteConnection,
        store: &mut Store,
        operation: &Operation,
        invocation: &VerifiedInvocation,
        permit: ExecutionPermit,
        cancellation: Cancellation,
    ) -> Result<Value, SqlError> {
        let policy = store.policy(invocation);
        if operation.prepared.request["kind"] == "drop_store" {
            if !policy.schema || !policy.write {
                return Err(SqlError::Authority("drop rights unavailable".into()));
            }
            let (_, mut objects) = snapshot(conn, store).await?;
            objects.sort_by_key(|o| match o.kind.as_str() {
                "trigger" => 0,
                "view" => 1,
                "index" => 2,
                _ => 3,
            });
            let mut affected = 0u64;
            for object in objects {
                let kind = match object.kind.as_str() {
                    "table" => "TABLE",
                    "view" => "VIEW",
                    "trigger" => "TRIGGER",
                    "index" => "INDEX",
                    _ => return Err(invalid("unsupported catalog object")),
                };
                let sql = format!("DROP {kind} {}", quoted(&object.name));
                let result = self
                    .engine
                    .execute(conn, &policy, &sql, &[], permit, cancellation.clone())
                    .await?;
                affected = affected.saturating_add(result.affected_rows);
                if affected > self.limits.max_affected_rows {
                    return Err(SqlError::Limit("aggregate store deletion effects".into()));
                }
            }
            query("DELETE FROM __octo_sql_stores WHERE id=?")
                .bind(&store.id)
                .execute(conn)
                .await?;
            Ok(
                json!({"status":"committed","operation_id":operation.prepared.id,"store_id":store.id,"dropped":true,"affected_rows":affected}),
            )
        } else {
            if store.archived {
                return Err(SqlError::Stale);
            }
            let request: QueryRequest =
                from_value(operation.prepared.request["args"].clone()).map_err(invalid)?;
            let analysis = self.engine.analyze(conn, &policy, &request.sql).await?;
            if self.effect_summary(&analysis, store) != operation.prepared.effects {
                return Err(SqlError::Stale);
            }
            let result = self
                .engine
                .execute(
                    conn,
                    &policy,
                    &request.sql,
                    &request.parameters,
                    permit,
                    cancellation,
                )
                .await?;
            reconcile(conn, store, &request.metadata, true).await?;
            if let Some(metadata) = request.schema_metadata {
                self.apply_creation_metadata(conn, store, metadata).await?;
            }
            if store.metadata.to_string().len() > self.limits.max_bytes {
                return Err(SqlError::Limit("semantic catalog bytes".into()));
            }
            Ok(
                json!({"status":"committed","operation_id":operation.prepared.id,"revision":store.revision,"result":to_value(result).map_err(invalid)?}),
            )
        }
    }
    async fn apply_creation_metadata(
        &self,
        conn: &mut SqliteConnection,
        store: &mut Store,
        metadata: SchemaMetadata,
    ) -> Result<(), SqlError> {
        let (_, objects) = snapshot(conn, store).await?;
        let object = objects
            .iter()
            .find(|o| o.name == metadata.table)
            .ok_or_else(|| invalid("creation metadata target missing"))?;
        let mut deltas = vec![MetadataDelta {
            object_id: object.id.clone(),
            column_id: None,
            semantics: metadata.semantics,
        }];
        for (name, semantics) in metadata.columns {
            let column = object
                .columns
                .iter()
                .find(|c| c.name == name)
                .ok_or_else(|| invalid("creation column missing"))?;
            deltas.push(MetadataDelta {
                object_id: object.id.clone(),
                column_id: Some(column.id.clone()),
                semantics,
            });
        }
        reconcile(conn, store, &deltas, false).await
    }
}
