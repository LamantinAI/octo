use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Value, from_value, json, to_value};
use sha2::{Digest, Sha256};
use sqlx::{Row, SqliteConnection, query};
use uuid::Uuid;

use super::catalog::Store;
use crate::error::{SqlError, invalid};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Object {
    pub id: String,
    pub name: String,
    pub kind: String,
    pub sql: String,
    pub columns: Vec<Column>,
    pub semantics: Value,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Column {
    pub id: String,
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub primary_key: bool,
    pub semantics: Value,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataDelta {
    pub object_id: String,
    #[serde(default)]
    pub column_id: Option<String>,
    pub semantics: Value,
}
pub(super) fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
pub(super) fn local_identifier(name: &str) -> Result<(), SqlError> {
    if name.is_empty()
        || name.len() > 64
        || !name.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_')
        || !name.as_bytes()[0].is_ascii_alphabetic()
    {
        return Err(invalid(
            "identifier must be 1..64 ASCII letters/digits/underscores, starting with a letter",
        ));
    }
    Ok(())
}
pub(super) async fn snapshot(
    conn: &mut SqliteConnection,
    store: &Store,
) -> Result<(String, Vec<Object>), SqlError> {
    let prefix = store.namespace();
    let rows = query("SELECT name,type,sql FROM sqlite_schema WHERE sql IS NOT NULL ORDER BY name")
        .fetch_all(&mut *conn)
        .await?;
    let old: Vec<Object> = from_value(store.metadata.clone()).map_err(invalid)?;
    let by_name: BTreeMap<&str, &Object> = old.iter().map(|o| (o.name.as_str(), o)).collect();
    let mut objects = Vec::new();
    let mut fingerprint = Sha256::new();
    for row in rows {
        let name: String = row.get("name");
        if !name.starts_with(&prefix) {
            continue;
        }
        let kind: String = row.get("type");
        let sql: String = row.get("sql");
        fingerprint.update(name.as_bytes());
        fingerprint.update([0]);
        fingerprint.update(kind.as_bytes());
        fingerprint.update([0]);
        fingerprint.update(sql.as_bytes());
        fingerprint.update([0]);
        let previous = by_name.get(name.as_str()).copied();
        let mut columns = Vec::new();
        if kind == "table" || kind == "view" {
            let rows = query(&format!("PRAGMA table_xinfo({})", quoted(&name)))
                .fetch_all(&mut *conn)
                .await?;
            for column in rows {
                let column_name: String = column.get("name");
                let previous_column =
                    previous.and_then(|o| o.columns.iter().find(|c| c.name == column_name));
                columns.push(Column {
                    id: previous_column
                        .map(|c| c.id.clone())
                        .unwrap_or_else(|| Uuid::now_v7().to_string()),
                    name: column_name,
                    data_type: column.get("type"),
                    nullable: column.get::<i64, _>("notnull") == 0,
                    primary_key: column.get::<i64, _>("pk") != 0,
                    semantics: previous_column
                        .map(|c| c.semantics.clone())
                        .unwrap_or_else(|| json!({})),
                });
            }
        }
        objects.push(Object {
            id: previous
                .map(|o| o.id.clone())
                .unwrap_or_else(|| Uuid::now_v7().to_string()),
            name,
            kind,
            sql,
            columns,
            semantics: previous
                .map(|o| o.semantics.clone())
                .unwrap_or_else(|| json!({})),
        });
    }
    Ok((format!("{:x}", fingerprint.finalize()), objects))
}
pub(super) fn reject_unregistered(store: &Store, objects: &[Object]) -> Result<(), SqlError> {
    let known: Vec<Object> = from_value(store.metadata.clone()).map_err(invalid)?;
    if objects
        .iter()
        .any(|actual| !known.iter().any(|old| old.name == actual.name))
    {
        return Err(SqlError::Authority("unregistered objects appeared outside the controlled executor; automatic adoption is disabled".into()));
    }
    Ok(())
}
pub(super) fn invalidate_semantics(store: &mut Store) -> Result<(), SqlError> {
    let mut objects: Vec<Object> = from_value(store.metadata.clone()).map_err(invalid)?;
    for object in &mut objects {
        object.semantics = json!({});
        for column in &mut object.columns {
            column.semantics = json!({});
        }
    }
    store.metadata = to_value(objects).map_err(invalid)?;
    Ok(())
}
pub(super) async fn reconcile(
    conn: &mut SqliteConnection,
    store: &mut Store,
    deltas: &[MetadataDelta],
    bump: bool,
) -> Result<(), SqlError> {
    let (fingerprint, mut objects) = snapshot(conn, store).await?;
    for delta in deltas {
        validate_semantics(&delta.semantics)?;
        let object = objects
            .iter_mut()
            .find(|o| o.id == delta.object_id)
            .ok_or_else(|| invalid("metadata object ID is stale or unknown"))?;
        if let Some(column_id) = &delta.column_id {
            let column = object
                .columns
                .iter_mut()
                .find(|c| &c.id == column_id)
                .ok_or_else(|| invalid("metadata column ID is stale or unknown"))?;
            column.semantics = delta.semantics.clone();
        } else {
            object.semantics = delta.semantics.clone();
        }
    }
    store.fingerprint = fingerprint;
    store.metadata = to_value(objects).map_err(invalid)?;
    if bump {
        store.revision += 1;
    }
    query("UPDATE __octo_sql_stores SET fingerprint=?,metadata=?,revision=? WHERE id=?")
        .bind(&store.fingerprint)
        .bind(store.metadata.to_string())
        .bind(store.revision as i64)
        .bind(&store.id)
        .execute(conn)
        .await?;
    Ok(())
}
pub(super) fn validate_semantics(value: &Value) -> Result<(), SqlError> {
    let map = value
        .as_object()
        .ok_or_else(|| invalid("semantics must be an object"))?;
    if value.to_string().len() > 16_384
        || map.keys().any(|key| {
            !matches!(
                key.as_str(),
                "description"
                    | "units"
                    | "null_meaning"
                    | "enumeration"
                    | "time_basis"
                    | "timezone"
                    | "provenance"
                    | "relationships"
            )
        })
    {
        return Err(invalid("unknown or oversized semantic metadata"));
    }
    Ok(())
}
