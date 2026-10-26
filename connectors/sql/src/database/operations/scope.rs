use octo_approval::{Access, Origin, ScopeBinding, Stores, VerifiedInvocation, operation_hash};
use serde::{Deserialize, Serialize};
use serde_json::from_str;

use super::{Database, Operation};
use crate::{
    database::presentation::PresentationSnapshot,
    error::{SqlError, invalid},
};

#[derive(Clone, Serialize, Deserialize)]
pub(in crate::database) struct Scope {
    #[serde(default)]
    pub presentation: PresentationSnapshot,
    pub principal: String,
    pub connector: String,
    pub conversation: String,
    pub destination: String,
    pub stores: Option<Vec<String>>,
    pub read: bool,
    pub write: bool,
    pub schema: bool,
    pub delegated: bool,
    pub policy_fingerprint: String,
    pub max_affected_rows: u64,
    pub timeout_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope_binding: Option<ScopeBinding>,
}
impl Scope {
    pub(in crate::database) fn capture(
        invocation: &VerifiedInvocation,
        policy_fingerprint: String,
        max_affected_rows: u64,
        timeout_ms: u64,
    ) -> Self {
        Self {
            presentation: PresentationSnapshot::default(),
            principal: invocation.origin.principal.clone(),
            connector: invocation.origin.connector.clone(),
            conversation: invocation.origin.conversation.clone(),
            destination: invocation.origin.private_destination.clone(),
            stores: match &invocation.access.stores {
                Stores::Owned => None,
                Stores::Only(ids) => Some(ids.clone()),
            },
            read: invocation.access.read,
            write: invocation.access.write,
            schema: invocation.access.schema,
            delegated: invocation.delegated,
            policy_fingerprint,
            max_affected_rows,
            timeout_ms,
            scope_binding: invocation.scope_binding.clone(),
        }
    }
    pub(in crate::database) fn invocation(&self) -> VerifiedInvocation {
        VerifiedInvocation {
            origin: Origin {
                principal: self.principal.clone(),
                connector: self.connector.clone(),
                conversation: self.conversation.clone(),
                private_destination: self.destination.clone(),
                may_approve: false,
            },
            access: Access {
                stores: self
                    .stores
                    .clone()
                    .map(Stores::Only)
                    .unwrap_or(Stores::Owned),
                read: self.read,
                write: self.write,
                schema: self.schema,
                create: false,
            },
            delegated: self.delegated,
            remaining: Default::default(),
            scope_binding: self.scope_binding.clone(),
        }
    }
}
impl Database {
    /// Validate the original hash-bound scope, never the current human proof.
    pub(in crate::database) fn check_operation_scope(
        &self,
        operation: &Operation,
    ) -> Result<(), SqlError> {
        if operation_hash(&operation.prepared).map_err(invalid)? != operation.hash {
            return Err(SqlError::Authority("stored operation hash mismatch".into()));
        }
        let original: Scope = from_str(&operation.prepared.scope).map_err(invalid)?;
        self.authority
            .check_scope(original.scope_binding.as_ref())
            .map_err(SqlError::Authority)
    }
}
