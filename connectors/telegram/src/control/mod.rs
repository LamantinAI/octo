//! ACL changes are deterministic and re-authorized against the actual sender.
//! Model dispatch supplies no sender metadata and cannot grant itself access.

use std::sync::Arc;

use octo_core::{ConnectorContext, ConnectorId, Envelope, EventKind};
use serde_json::{Value, json};
use teloxide::types::{ChatMemberUpdated, Message};
use tracing::warn;

use super::{ALLOW_CHAT, AclState, GROUP_MODE, LIST_CHATS, REMOVE_CHAT, SEND_FILE};
use crate::{
    Acl, GroupMode, Role,
    source::{GroupSettings, is_admin},
};

fn sender(env: &Envelope) -> Option<i64> {
    env.channel_metadata
        .as_ref()?
        .tags
        .get("sender_id")?
        .parse()
        .ok()
}

fn change<T>(
    state: &AclState,
    apply: impl FnOnce(&mut Acl) -> Result<T, String>,
) -> Result<T, String> {
    let mut acl = state.acl.write().unwrap();
    let mut candidate = acl.clone();
    let result = apply(&mut candidate)?;
    if let Some(path) = &state.path {
        candidate
            .save(path)
            .map_err(|e| format!("ACL was not changed: {e}"))?;
    }
    *acl = candidate;
    Ok(result)
}

fn execute(state: &AclState, settings: &GroupSettings, env: &Envelope) -> Value {
    let Some(actor) = sender(env) else {
        return json!({"ok":false,"error":"missing verified sender"});
    };
    let (owner, admin) = {
        let acl = state.acl.read().unwrap();
        (
            acl.role(actor) == Some(Role::Owner),
            is_admin(&acl, settings, actor),
        )
    };
    if !admin || (env.kind.as_str() == GROUP_MODE && !owner) {
        return json!({"ok":false,"error":"owner permission required for group mode; ACL commands require owner or ACL admin"});
    }
    let payload = env.payload_as::<Value>().cloned().unwrap_or(Value::Null);
    let id = payload["chat_id"].as_i64();
    let result = match env.kind.as_str() {
        LIST_CHATS => {
            let acl = state.acl.read().unwrap();
            let chats: Vec<_> = acl.entries().iter().map(|entry| json!({
                "chat_id":entry.chat_id,"role":entry.role,"group_mode":if entry.chat_id<0 {Some(acl.group_mode(entry.chat_id))} else {None},
            })).collect();
            Ok(json!({"ok":true,"chats":chats}))
        }
        ALLOW_CHAT => id.ok_or("missing chat_id".to_string()).and_then(|id| {
            change(state, |acl| {
                let added = if acl.role(id) == Some(Role::Owner) {
                    false
                } else {
                    acl.insert(id, Role::Trusted)
                };
                Ok(json!({"ok":true,"chat_id":id,"added":added,"role":"trusted"}))
            })
        }),
        REMOVE_CHAT => id.ok_or("missing chat_id".to_string()).and_then(|id| {
            change(state, |acl| {
                if acl.role(id) == Some(Role::Owner) {
                    return Err("owner access is configured in the manifest".into());
                }
                Ok(json!({"ok":true,"chat_id":id,"removed":acl.remove(id)}))
            })
        }),
        GROUP_MODE => id.ok_or("missing chat_id".to_string()).and_then(|id| {
            let mode = match payload["mode"].as_str() {
                Some("all") => GroupMode::All,
                Some("allowed") => GroupMode::Allowed,
                _ => return Err("mode must be all or allowed".into()),
            };
            change(state, |acl| {
                acl.set_group_mode(id, mode).map_err(str::to_string)?;
                Ok(json!({"ok":true,"chat_id":id,"mode":mode}))
            })
        }),
        _ => Err("unknown control command".into()),
    };
    result.unwrap_or_else(|error| json!({"ok":false,"error":error}))
}

pub(super) async fn handle_control(
    acl: &Option<Arc<AclState>>,
    settings: &GroupSettings,
    id: &ConnectorId,
    env: &Envelope,
    ctx: &ConnectorContext,
) {
    let result = acl
        .as_ref()
        .map(|state| execute(state, settings, env))
        .unwrap_or_else(|| json!({"ok":false,"error":"ACL is not configured"}));
    let response = Envelope::new(
        id.clone(),
        EventKind::new(format!("{}.result", env.kind.as_str())),
        result,
    )
    .with_correlation(env.id);
    if let Err(error) = ctx.publish(response).await {
        warn!(%error,"telegram control reply failed");
    }
}

pub(super) fn owner_chats(acl: &Option<Arc<AclState>>) -> Vec<i64> {
    acl.as_ref()
        .map(|state| {
            state
                .acl
                .read()
                .unwrap()
                .entries()
                .iter()
                .filter(|entry| entry.chat_id > 0 && entry.role == Role::Owner)
                .map(|entry| entry.chat_id)
                .collect()
        })
        .unwrap_or_default()
}

pub(super) fn outgoing_allowed(
    acl: &Option<Arc<AclState>>,
    settings: &GroupSettings,
    env: &Envelope,
) -> bool {
    let Some(state) = acl else { return true };
    let chat = if env.kind.as_str() == SEND_FILE {
        env.payload_as::<Value>().and_then(|p| p["chat"].as_i64())
    } else {
        None
    }
    .or_else(|| {
        env.channel
            .as_ref()
            .and_then(|id| id.as_str().parse::<i64>().ok())
    });
    let Some(chat) = chat else { return false };
    let acl = state.acl.read().unwrap();
    if acl.role(chat).is_some() {
        return true;
    }
    // Narrow bootstrap/deny acknowledgement, never a model-generated delivery.
    env.tags.get("control_reply").map(String::as_str) == Some("true")
        && env
            .channel_metadata
            .as_ref()
            .and_then(|m| m.tags.get("chat_id"))
            .and_then(|id| id.parse::<i64>().ok())
            == Some(chat)
        && sender(env).is_some_and(|actor| is_admin(&acl, settings, actor))
}

pub(super) fn register_add(
    acl: &Option<Arc<AclState>>,
    settings: &GroupSettings,
    update: &ChatMemberUpdated,
) {
    if !(update.chat.is_group() || update.chat.is_supergroup())
        || update.old_chat_member.is_present()
        || !update.new_chat_member.is_present()
    {
        return;
    }
    let Some(state) = acl else { return };
    if !is_admin(
        &state.acl.read().unwrap(),
        settings,
        update.from.id.0 as i64,
    ) {
        return;
    }
    if let Err(error) = change(state, |acl| {
        acl.ensure(update.chat.id.0, Role::Trusted);
        Ok(())
    }) {
        warn!(%error,"could not allow group on trusted add");
    }
}

pub(super) fn migrate(acl: &Option<Arc<AclState>>, message: &Message) -> bool {
    let ids = message
        .migrate_to_chat_id()
        .map(|new| (message.chat.id.0, new.0))
        .or_else(|| {
            message
                .migrate_from_chat_id()
                .map(|old| (old.0, message.chat.id.0))
        });
    let Some((old, new)) = ids else { return false };
    if let Some(state) = acl {
        if let Err(error) = change(state, |acl| {
            acl.migrate_group(old, new);
            Ok(())
        }) {
            warn!(%error,"could not migrate group ACL");
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use octo_core::ChannelMetadata;
    use std::sync::RwLock;

    fn state() -> AclState {
        let mut acl = Acl::new();
        acl.ensure(1, Role::Owner);
        AclState {
            acl: RwLock::new(acl),
            path: None,
        }
    }
    fn request(actor: i64) -> Envelope {
        Envelope::new(
            ConnectorId::new("cogitator"),
            EventKind::new(GROUP_MODE),
            json!({"chat_id":-42,"mode":"all"}),
        )
        .with_channel_metadata(ChannelMetadata::new().with_tag("sender_id", actor.to_string()))
    }
    #[test]
    fn only_owner_can_open_group_even_when_an_acl_admin_requests_it() {
        let state = state();
        let settings = GroupSettings {
            admins: vec![2],
            address_names: vec![],
        };
        assert_eq!(execute(&state, &settings, &request(2))["ok"], false);
        assert_eq!(execute(&state, &settings, &request(3))["ok"], false);
        assert_eq!(execute(&state, &settings, &request(1))["ok"], true);
        assert_eq!(state.acl.read().unwrap().group_mode(-42), GroupMode::All);
    }
    #[test]
    fn a_payload_claiming_owner_cannot_forge_sender_metadata() {
        let forged = Envelope::new(
            ConnectorId::new("cogitator"),
            EventKind::new(GROUP_MODE),
            json!({"chat_id":-42,"mode":"all","sender_id":1,"role":"owner"}),
        );
        assert_eq!(
            execute(&state(), &GroupSettings::default(), &forged)["ok"],
            false
        );
    }

    #[test]
    fn deliveries_require_chat_access_but_do_not_require_an_addressing_flag() {
        let state = Arc::new(state());
        state.acl.write().unwrap().insert(-42, Role::Trusted);
        let reply = Envelope::new(
            ConnectorId::new("cogitator"),
            EventKind::new("chat.reply"),
            "task result".to_string(),
        )
        .with_channel(octo_core::ChannelId::new("-42"));
        assert!(outgoing_allowed(
            &Some(state.clone()),
            &GroupSettings::default(),
            &reply
        ));
        state.acl.write().unwrap().remove(-42);
        assert!(!outgoing_allowed(
            &Some(state),
            &GroupSettings::default(),
            &reply
        ));
    }
    #[test]
    fn failed_persistence_does_not_apply_permissions_in_memory() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = state();
        state.path = Some(dir.path().to_path_buf()); // a directory cannot become the ACL file
        assert_eq!(
            execute(&state, &GroupSettings::default(), &request(1))["ok"],
            false
        );
        assert!(state.acl.read().unwrap().role(-42).is_none());
    }
    #[test]
    fn only_owner_or_configured_admin_can_auto_allow_a_group() {
        let settings = GroupSettings {
            admins: vec![2],
            address_names: vec![],
        };
        for actor in [1, 2, 3] {
            let state = Arc::new(state());
            let update: ChatMemberUpdated = serde_json::from_value(json!({
                "chat": {"id":-42,"type":"group","title":"room"},
                "from": {"id":actor,"is_bot":false,"first_name":"Actor"}, "date":0,
                "old_chat_member":{"status":"left","user":{"id":99,"is_bot":true,"first_name":"Bot"}},
                "new_chat_member":{"status":"member","user":{"id":99,"is_bot":true,"first_name":"Bot"}}
            })).unwrap();
            register_add(&Some(state.clone()), &settings, &update);
            assert_eq!(state.acl.read().unwrap().role(-42).is_some(), actor != 3);
        }
    }
}
