use chrono::Utc;
use octo_approval::VerifiedInvocation;
use serde_json::{Value, json};

use super::{Database, Operation};
use crate::error::SqlError;

impl Database {
    pub(super) fn require_private_approval_reader(
        &self,
        actor: &VerifiedInvocation,
    ) -> Result<(), SqlError> {
        if actor.delegated
            || !actor.origin.may_approve
            || actor.origin.conversation != actor.origin.private_destination
            || !actor.access.read
        {
            return Err(SqlError::Authority(
                "Human private read access required".into(),
            ));
        }
        Ok(())
    }
    pub(super) fn approval_projection(&self, operation: &Operation, details: bool) -> Value {
        let mut view = json!({"operation_id":operation.prepared.id,"operation_hash":operation.hash,
            "status":operation.status,"expires_at":operation.prepared.expires_at,
            "summary":operation.scope.presentation,"decision_connector":operation.scope.connector,
            "private_destination":operation.scope.destination,"origin_scope":operation.scope.scope_binding,
            "can_decide": operation.status=="pending" && operation.prepared.expires_at>Utc::now().timestamp() && self.check_operation_scope(operation).is_ok()});
        if details {
            // Deliberate human-private whitelist. Engine paths and database
            // identity are not needed to review the exact operation.
            view["details"] = json!({"operation":operation.prepared.request,"effects":operation.prepared.effects,
                "revision":operation.prepared.revision,"operation_hash":operation.hash});
        }
        view
    }
}
