//! Human decisions enter here from real Telegram updates, never connector JSON.
mod cards;
pub(super) use cards::{CardSettings, Cards};
use std::time::Duration;

use octo_approval::{Access, Authority, Ingress, Origin, Stores};
use octo_core::{ConnectorContext, ConnectorId, Envelope, EventKind};
use serde_json::{Value, json};
use teloxide::{
    Bot,
    requests::Requester,
    types::{ChatId, Message},
};

use crate::{
    Role,
    source::{actor_id, command_name},
};

pub(super) fn message_proof(
    authority: &Authority,
    id: &ConnectorId,
    message: &Message,
    role: Option<Role>,
) -> Option<Ingress> {
    let actor = actor_id(message)?;
    let owner = role == Some(Role::Owner);
    let access = if owner && message.chat.is_private() {
        Access::owner()
    } else {
        Access {
            stores: Stores::Only(vec![]),
            read: false,
            write: false,
            schema: false,
            create: false,
        }
    };
    Some(authority.issue(
        Origin {
            principal: format!("{}:user:{actor}", id.as_str()),
            connector: id.to_string(),
            conversation: message.chat.id.0.to_string(),
            private_destination: actor.to_string(),
            may_approve: owner,
        },
        access,
    ))
}

fn decision(
    authority: &Authority,
    ingress: Option<&Ingress>,
    id: &ConnectorId,
    message: &Message,
    username: &str,
) -> Option<Result<Envelope, String>> {
    let text = message.text()?;
    let command = command_name(text, username)?;
    if !matches!(command, "sqlapprove" | "sqldeny") {
        return None;
    }
    Some((|| {
        if message.forward_origin().is_some() {
            return Err("Forwarded messages cannot approve operations.".into());
        }
        let origin = ingress.ok_or("No authenticated operator identity.")?;
        let parts: Vec<_> = text.split_whitespace().collect();
        if parts.len() != 3 {
            return Err(format!(
                "Use /{command} <connector> <operation_id> in your private chat."
            ));
        }
        let env = Envelope::new(
            id.clone(),
            EventKind::new("sql.approval.decide"),
            json!({"operation_id":parts[2],"approve":command=="sqlapprove"}),
        )
        .with_target(ConnectorId::new(parts[1]));
        let proof = authority.bind_decision(origin, &env)?;
        Ok(env.with_context(proof))
    })())
}

pub(super) async fn handle_decision(
    authority: &Option<Authority>,
    ingress: Option<&Ingress>,
    bot: &Bot,
    id: &ConnectorId,
    message: &Message,
    username: &str,
    ctx: &ConnectorContext,
    timeout: Duration,
) -> bool {
    let Some(authority) = authority else {
        return false;
    };
    let Some(request) = decision(authority, ingress, id, message, username) else {
        return false;
    };
    let answer = match request {
        Err(error) => error,
        Ok(request) => match ctx.publish_and_await_response(request, timeout).await {
            Ok(reply) => {
                let body = reply.payload_as::<Value>().cloned().unwrap_or(Value::Null);
                let status = body.get("status").and_then(Value::as_str).unwrap_or("decision recorded");
                match body.get("error").and_then(Value::as_str) {
                    Some(error) => format!("Database operation: {error}"),
                    None => format!("Database operation: {status}."),
                }
            },
            Err(_) => "The decision was sent, but its execution outcome is unknown. Check the operation status before retrying.".into(),
        },
    };
    let _ = bot.send_message(ChatId(message.chat.id.0), answer).await;
    true
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use octo_approval::{Access, Authority, Origin};
    use octo_core::ConnectorId;
    use serde_json::{from_value, json};
    use teloxide::types::Message;

    use super::{decision, message_proof};
    use crate::Role;

    fn message(text: &str, chat: i64, group: bool) -> Message {
        let mut value = json!({"message_id":1,"date":0,"chat":{"id":chat,"type":"private","first_name":"Person"},
            "from":{"id":7,"is_bot":false,"first_name":"Person"},"text":text});
        if group {
            value["chat"] = json!({"id":chat,"type":"supergroup","title":"Group"});
        }
        from_value(value).unwrap()
    }
    #[test]
    fn only_real_private_owner_command_mints_decision_proof() {
        let authority = Authority::new(Duration::from_secs(60), 8);
        let id = ConnectorId::new("telegram");
        let msg = message("/sqlapprove sql op-1", 7, false);
        let ingress = message_proof(&authority, &id, &msg, Some(Role::Owner)).unwrap();
        let request = decision(&authority, Some(&ingress), &id, &msg, "bot")
            .unwrap()
            .unwrap();
        assert!(authority.consume(&request).is_err());
        assert_eq!(
            authority
                .consume_decision(&request)
                .unwrap()
                .origin
                .principal,
            "telegram:user:7"
        );
        assert!(authority.consume_decision(&request).is_err());
        let group = message("/sqlapprove sql op-1", -42, true);
        let ingress = message_proof(&authority, &id, &group, Some(Role::Owner)).unwrap();
        assert!(
            decision(&authority, Some(&ingress), &id, &group, "bot")
                .unwrap()
                .is_err()
        );
        let guest = message_proof(&authority, &id, &msg, Some(Role::Guest)).unwrap();
        assert!(!guest.access().create);
        assert!(
            decision(&authority, Some(&guest), &id, &msg, "bot")
                .unwrap()
                .is_err()
        );
        let child = authority
            .issue(
                Origin {
                    principal: "telegram:user:7".into(),
                    connector: "telegram".into(),
                    conversation: "7".into(),
                    private_destination: "7".into(),
                    may_approve: true,
                },
                Access::owner(),
            )
            .attenuate(vec![], false, false)
            .unwrap();
        assert!(
            decision(&authority, Some(&child), &id, &msg, "bot")
                .unwrap()
                .is_err()
        );
    }
}
