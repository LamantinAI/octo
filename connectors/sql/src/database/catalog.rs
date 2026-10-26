use chrono::Utc;
use octo_approval::VerifiedInvocation;
use serde::{Deserialize, Serialize};
use serde_json::{Value, from_str, json};
use sha2::{Digest, Sha256};
use sqlx::{Connection, Row, SqliteConnection, query, sqlite::SqliteRow};
use uuid::Uuid;

use super::{
    Database,
    operations::{request_hash, validate_key},
    schema::{MetadataDelta, invalidate_semantics, reconcile, reject_unregistered, snapshot},
};
use crate::{
    engine::Policy,
    error::{SqlError, invalid},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Store {
    pub id: String,
    pub owner: String,
    pub name: String,
    pub purpose: String,
    pub tags: Vec<String>,
    pub archived: bool,
    pub revision: u64,
    pub fingerprint: String,
    pub metadata: Value,
}
impl Store {
    pub(super) fn namespace(&self) -> String {
        format!("s_{}__", self.id)
    }
    pub(super) fn policy(&self, invocation: &VerifiedInvocation) -> Policy {
        Policy {
            namespace: self.namespace(),
            read: invocation.access.read,
            write: invocation.access.write,
            schema: invocation.access.schema,
        }
    }
}
fn row_store(row: SqliteRow) -> Result<Store, SqlError> {
    Ok(Store {
        id: row.get("id"),
        owner: row.get("owner"),
        name: row.get("name"),
        purpose: row.get("purpose"),
        tags: from_str(row.get("tags")).map_err(invalid)?,
        archived: row.get::<i64, _>("archived") != 0,
        revision: row.get::<i64, _>("revision") as u64,
        fingerprint: row.get("fingerprint"),
        metadata: from_str(row.get("metadata")).map_err(invalid)?,
    })
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CreateStore {
    pub name: String,
    #[serde(default)]
    pub purpose: String,
    #[serde(default)]
    pub tags: Vec<String>,
    pub idempotency_key: String,
}
#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ListStores {
    #[serde(default)]
    pub query: String,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DescribeStore {
    pub store_id: String,
    #[serde(default)]
    pub tables: Vec<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MetadataRequest {
    pub store_id: String,
    pub expected_revision: u64,
    pub metadata: Vec<MetadataDelta>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StoreMutation {
    pub store_id: String,
    pub expected_revision: u64,
    pub idempotency_key: String,
}

impl Database {
    pub(super) async fn load_store(
        &self,
        conn: &mut SqliteConnection,
        id: &str,
        invocation: &VerifiedInvocation,
    ) -> Result<Store, SqlError> {
        if id.len() != 32 || !id.bytes().all(|c| c.is_ascii_hexdigit()) {
            return Err(invalid("invalid store ID"));
        }
        let store = row_store(
            query("SELECT * FROM __octo_sql_stores WHERE id=?")
                .bind(id)
                .fetch_optional(&mut *conn)
                .await?
                .ok_or_else(|| SqlError::Authority("store unavailable".into()))?,
        )?;
        if !invocation
            .access
            .permits(id, &store.owner, &invocation.origin.principal)
        {
            return Err(SqlError::Authority("store unavailable".into()));
        }
        Ok(store)
    }
    pub(super) async fn checked_store(
        &self,
        conn: &mut SqliteConnection,
        id: &str,
        revision: u64,
        invocation: &VerifiedInvocation,
    ) -> Result<Store, SqlError> {
        let mut store = self.load_store(conn, id, invocation).await?;
        let (actual, objects) = snapshot(conn, &store).await?;
        if actual != store.fingerprint {
            reject_unregistered(&store, &objects)?;
            // Out-of-band schema drift invalidates semantic assumptions. Record
            // the new schema/revision, then require a fresh prepared operation.
            self.authority
                .check_scope(invocation.scope_binding.as_ref())
                .map_err(SqlError::Authority)?;
            invalidate_semantics(&mut store)?;
            reconcile(conn, &mut store, &[], true).await?;
            return Err(SqlError::Stale);
        }
        if store.revision != revision || store.archived {
            return Err(SqlError::Stale);
        }
        Ok(store)
    }
    pub(super) async fn create_store(
        &self,
        conn: &mut SqliteConnection,
        request: CreateStore,
        invocation: &VerifiedInvocation,
    ) -> Result<Value, SqlError> {
        if !invocation.access.create || invocation.delegated {
            return Err(SqlError::Authority("store creation unavailable".into()));
        }
        if request.name.is_empty()
            || request.name.len() > 256
            || request.purpose.len() > 4096
            || request.tags.len() > 32
            || request.tags.iter().any(|s| s.len() > 128)
        {
            return Err(invalid("oversized store description"));
        }
        validate_key(&request.idempotency_key)?;
        let key = format!(
            "create:{}",
            request_hash(&json!([
                invocation.origin.principal,
                request.idempotency_key
            ]))
        );
        let request_json =
            json!({"name":request.name,"purpose":request.purpose,"tags":request.tags});
        let hash = format!("{:x}", Sha256::digest(request_json.to_string().as_bytes()));
        let mut transaction = conn.begin_with("BEGIN IMMEDIATE").await?;
        if let Some(row) = query("SELECT hash,result FROM __octo_sql_receipts WHERE operation_id=?")
            .bind(&key)
            .fetch_optional(&mut *transaction)
            .await?
        {
            if row.get::<String, _>("hash") != hash {
                return Err(invalid("idempotency key content mismatch"));
            }
            return from_str(row.get("result")).map_err(invalid);
        }
        let count: i64 = query("SELECT count(*) FROM __octo_sql_stores")
            .fetch_one(&mut *transaction)
            .await?
            .get(0);
        if count >= self.limits.max_stores as i64 {
            return Err(SqlError::Limit(
                "logical store quota (archived stores count)".into(),
            ));
        }
        let id = Uuid::now_v7().simple().to_string();
        let fingerprint = format!("{:x}", Sha256::digest([]));
        self.authority
            .check_scope(invocation.scope_binding.as_ref())
            .map_err(SqlError::Authority)?;
        query("INSERT INTO __octo_sql_stores(id,owner,name,purpose,tags,revision,fingerprint,metadata) VALUES(?,?,?,?,?,1,?,'[]')")
            .bind(&id).bind(&invocation.origin.principal).bind(&request.name).bind(&request.purpose)
            .bind(json!(request.tags).to_string()).bind(fingerprint).execute(&mut *transaction).await?;
        let result =
            json!({"status":"created","store_id":id,"namespace":format!("s_{id}__"),"revision":1});
        query("INSERT INTO __octo_sql_receipts(operation_id,hash,result) VALUES(?,?,?)")
            .bind(key)
            .bind(hash)
            .bind(result.to_string())
            .execute(&mut *transaction)
            .await?;
        query("INSERT INTO __octo_sql_audit(operation_id,principal,outcome,detail,created_at) VALUES(?,?,'store_created',?,?)")
            .bind(format!("create:{}",request_hash(&json!([invocation.origin.principal,request.idempotency_key]))))
            .bind(&invocation.origin.principal).bind(request_json.to_string()).bind(Utc::now().timestamp()).execute(&mut *transaction).await?;
        transaction.commit().await?;
        Ok(result)
    }
    pub(super) async fn list_stores(
        &self,
        conn: &mut SqliteConnection,
        request: ListStores,
        invocation: &VerifiedInvocation,
    ) -> Result<Value, SqlError> {
        if !invocation.access.read {
            return Err(SqlError::Authority("read unavailable".into()));
        }
        if request.query.len() > 256
            || request
                .cursor
                .as_ref()
                .is_some_and(|c| c.len() != 32 || !c.bytes().all(|b| b.is_ascii_hexdigit()))
        {
            return Err(invalid("invalid search/cursor"));
        }
        let limit = request.limit.unwrap_or(20).min(20);
        if limit == 0 {
            return Err(invalid("zero page size"));
        }
        let needle = request.query.to_lowercase();
        let rows = query("SELECT * FROM __octo_sql_stores WHERE owner=? AND id>? ORDER BY id")
            .bind(&invocation.origin.principal)
            .bind(request.cursor.unwrap_or_default())
            .fetch_all(conn)
            .await?;
        let mut stores = Vec::new();
        for row in rows {
            let store = row_store(row)?;
            if !invocation
                .access
                .permits(&store.id, &store.owner, &invocation.origin.principal)
            {
                continue;
            }
            if !format!("{} {} {}", store.name, store.purpose, store.tags.join(" "))
                .to_lowercase()
                .contains(&needle)
            {
                continue;
            }
            stores.push(json!({"store_id":store.id,"name":store.name,"purpose":store.purpose,"tags":store.tags,"archived":store.archived,"revision":store.revision}));
        }
        let more = stores.len() > limit;
        stores.truncate(limit);
        let next = if more {
            stores.last().and_then(|v| v.get("store_id")).cloned()
        } else {
            None
        };
        Ok(json!({"stores":stores,"next_cursor":next}))
    }
    pub(super) async fn describe_store(
        &self,
        conn: &mut SqliteConnection,
        request: DescribeStore,
        invocation: &VerifiedInvocation,
    ) -> Result<Value, SqlError> {
        if !invocation.access.read {
            return Err(SqlError::Authority("read unavailable".into()));
        }
        let mut store = self.load_store(conn, &request.store_id, invocation).await?;
        let (fingerprint, objects) = snapshot(conn, &store).await?;
        if fingerprint != store.fingerprint {
            reject_unregistered(&store, &objects)?;
            self.authority
                .check_scope(invocation.scope_binding.as_ref())
                .map_err(SqlError::Authority)?;
            invalidate_semantics(&mut store)?;
            reconcile(conn, &mut store, &[], true).await?;
        }
        let mut objects = store
            .metadata
            .as_array()
            .cloned()
            .ok_or_else(|| invalid("catalog metadata corrupted"))?;
        if !request.tables.is_empty() {
            objects.retain(|o| {
                request
                    .tables
                    .iter()
                    .any(|name| o["name"].as_str() == Some(name))
            });
        }
        let result = json!({"store_id":store.id,"name":store.name,"purpose":store.purpose,"tags":store.tags,"archived":store.archived,
            "namespace":store.namespace(),"revision":store.revision,"fingerprint":store.fingerprint,"objects":objects});
        if result.to_string().len() > self.limits.max_bytes {
            return Err(SqlError::Limit(
                "describe output; select tables explicitly".into(),
            ));
        }
        Ok(result)
    }
    pub(super) async fn update_metadata(
        &self,
        conn: &mut SqliteConnection,
        request: MetadataRequest,
        invocation: &VerifiedInvocation,
    ) -> Result<Value, SqlError> {
        if !invocation.access.schema {
            return Err(SqlError::Authority(
                "semantic metadata capability unavailable".into(),
            ));
        }
        let mut transaction = conn.begin_with("BEGIN IMMEDIATE").await?;
        let mut store = self
            .checked_store(
                &mut transaction,
                &request.store_id,
                request.expected_revision,
                invocation,
            )
            .await?;
        self.authority
            .check_scope(invocation.scope_binding.as_ref())
            .map_err(SqlError::Authority)?;
        reconcile(&mut transaction, &mut store, &request.metadata, true).await?;
        if store.metadata.to_string().len() > self.limits.max_bytes {
            return Err(SqlError::Limit("semantic catalog bytes".into()));
        }
        query("INSERT INTO __octo_sql_audit(operation_id,principal,outcome,detail,created_at) VALUES(?,?,'metadata_updated',?,?)")
            .bind(format!("metadata:{}",Uuid::now_v7())).bind(&invocation.origin.principal)
            .bind(json!({"store_id":store.id,"revision":store.revision,"metadata":request.metadata}).to_string())
            .bind(Utc::now().timestamp()).execute(&mut *transaction).await?;
        transaction.commit().await?;
        Ok(json!({"status":"updated","revision":store.revision}))
    }
    pub(super) async fn archive_store(
        &self,
        conn: &mut SqliteConnection,
        request: StoreMutation,
        invocation: &VerifiedInvocation,
    ) -> Result<Value, SqlError> {
        if !invocation.access.write
            || !invocation.access.permits(
                &request.store_id,
                &invocation.origin.principal,
                &invocation.origin.principal,
            )
        {
            return Err(SqlError::Authority(
                "archive write capability unavailable".into(),
            ));
        }
        validate_key(&request.idempotency_key)?;
        let key = format!(
            "archive:{}",
            request_hash(&json!([
                invocation.origin.principal,
                request.store_id,
                request.idempotency_key
            ]))
        );
        let content =
            json!({"store_id":request.store_id,"expected_revision":request.expected_revision});
        let hash = request_hash(&content);
        let mut transaction = conn.begin_with("BEGIN IMMEDIATE").await?;
        if let Some(row) = query("SELECT hash,result FROM __octo_sql_receipts WHERE operation_id=?")
            .bind(&key)
            .fetch_optional(&mut *transaction)
            .await?
        {
            if row.get::<String, _>("hash") != hash {
                return Err(invalid("idempotency key content mismatch"));
            }
            if !invocation.access.read {
                return Err(SqlError::Authority("receipt read unavailable".into()));
            }
            return from_str(row.get("result")).map_err(invalid);
        }
        let store = self
            .checked_store(
                &mut transaction,
                &request.store_id,
                request.expected_revision,
                invocation,
            )
            .await?;
        self.authority
            .check_scope(invocation.scope_binding.as_ref())
            .map_err(SqlError::Authority)?;
        query("UPDATE __octo_sql_stores SET archived=1,revision=revision+1 WHERE id=?")
            .bind(&store.id)
            .execute(&mut *transaction)
            .await?;
        let result = json!({"status":"archived","revision":store.revision+1});
        query("INSERT INTO __octo_sql_receipts(operation_id,hash,result) VALUES(?,?,?)")
            .bind(&key)
            .bind(hash)
            .bind(result.to_string())
            .execute(&mut *transaction)
            .await?;
        query("INSERT INTO __octo_sql_audit(operation_id,principal,outcome,detail,created_at) VALUES(?,?,'store_archived',?,?)")
            .bind(&key).bind(&invocation.origin.principal).bind(content.to_string()).bind(Utc::now().timestamp()).execute(&mut *transaction).await?;
        transaction.commit().await?;
        Ok(result)
    }
}
