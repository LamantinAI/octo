use serde::{Deserialize, Serialize};
use serde_json::{Value, to_vec};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OperationState {
    Pending,
    Approved,
    Rejected,
    Cancelled,
    Expired,
    Claimed,
    Committed,
    RolledBack,
    Unknown,
}

/// Immutable operation content. Persistence and execution are owned by its backend.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedOperation {
    pub id: String,
    pub principal: String,
    pub database_identity: String,
    pub store: String,
    pub revision: u64,
    pub scope: String,
    pub request: Value,
    pub effects: Vec<String>,
    pub expires_at: i64,
}

pub fn operation_hash(operation: &PreparedOperation) -> Result<String, String> {
    let bytes = to_vec(operation).map_err(|_| "Cannot encode prepared operation".to_string())?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}
