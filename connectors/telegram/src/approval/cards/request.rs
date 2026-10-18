use std::sync::Arc;

use chrono::Utc;
use octo_approval::{Access, Authority, Origin};
use octo_core::{ConnectorId, Envelope, EventKind};
use serde_json::{Value, json};
use teloxide::types::CallbackQuery;

use super::registry::{Action, Card, State};
use crate::{AclState, Role};

pub(super) fn current_owner(acl: &Option<Arc<AclState>>, actor: u64, chat: i64) -> bool {
    let Ok(actor) = i64::try_from(actor) else {
        return false;
    };
    actor == chat
        && acl.as_ref().is_some_and(|state| {
            let acl = state.acl.read().unwrap();
            acl.role(actor) == Some(Role::Owner) && acl.role(chat) == Some(Role::Owner)
        })
}
pub(super) fn request(
    authority: &Authority,
    id: &ConnectorId,
    acl: &Option<Arc<AclState>>,
    query: &CallbackQuery,
    card: &Card,
    action: Action,
) -> Result<Envelope, String> {
    let message = query.regular_message().ok_or("Message is inaccessible")?;
    if !message.chat.is_private()
        || !current_owner(acl, query.from.id.0, message.chat.id.0)
        || query.from.is_bot
        || query.from.id.0 != card.actor
        || message.id != card.message.ok_or("Card is not active")?
        || message.chat.id.0 != card.chat
    {
        return Err("Current owner identity required".into());
    }
    let origin = Origin {
        principal: format!("{}:user:{}", id.as_str(), query.from.id.0),
        connector: id.to_string(),
        conversation: message.chat.id.0.to_string(),
        private_destination: query.from.id.0.to_string(),
        may_approve: true,
    };
    if origin.principal != card.presentation.recipient_principal {
        return Err("Card recipient mismatch".into());
    }
    if action.is_decision() && card.presentation.expires_at <= Utc::now().timestamp() {
        return Err("Confirmation expired".into());
    }
    let ingress = authority.issue(origin, Access::owner());
    let (kind, payload) = match action {
        Action::Confirm | Action::Cancel => (
            card.presentation.decision_kind.clone(),
            json!({"operation_id":card.presentation.operation_id,"approve":action==Action::Confirm}),
        ),
        Action::Details => (
            card.presentation.status_kind.clone(),
            json!({"operation_id":card.presentation.operation_id,"include_details":true}),
        ),
        Action::Status => (
            card.presentation.status_kind.clone(),
            json!({"operation_id":card.presentation.operation_id}),
        ),
    };
    let envelope = Envelope::new(id.clone(), EventKind::new(kind), payload)
        .with_target(ConnectorId::new(&card.presentation.executor));
    if action.is_decision() {
        let proof = authority.bind_decision(&ingress, &envelope)?;
        Ok(envelope.with_context(proof))
    } else {
        let proof = authority.bind(&ingress, &envelope)?;
        Ok(envelope.with_context(proof))
    }
}
pub(super) fn response(card: &Card, reply: &Envelope) -> Result<(State, Value), ()> {
    if reply.source.as_str() != card.presentation.executor {
        return Err(());
    }
    let body = reply.payload_as::<Value>().ok_or(())?;
    if body
        .get("operation_id")
        .is_some_and(|id| id.as_str() != Some(&card.presentation.operation_id))
        || body
            .get("operation_hash")
            .is_some_and(|hash| hash.as_str() != Some(&card.presentation.operation_hash))
    {
        return Err(());
    }
    let state = match body["status"].as_str() {
        Some("pending" | "approval_required") => State::Pending,
        Some("committed") => State::Committed,
        Some("rejected") => State::Rejected,
        Some("cancelled") => State::Cancelled,
        Some("expired") => State::Expired,
        Some("rolled_back") => State::RolledBack,
        Some("claimed" | "unknown" | "approved") => State::Unknown,
        _ => return Err(()),
    };
    if body.get("error").is_some() && state != State::RolledBack {
        return Err(());
    }
    Ok((state, body.clone()))
}
