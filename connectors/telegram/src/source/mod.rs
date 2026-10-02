//! Verified transport provenance and addressing signals. Whether an unaddressed
//! group event should start cognition remains a decision of the assembly.

use std::collections::{HashMap, HashSet};

use octo_core::{ChannelMetadata, Envelope, TrustLevel};
use serde_json::Value;
use teloxide::types::{Message, MessageEntityKind, UserId};

use crate::{Acl, Role, acl::GroupMode};

#[derive(Clone, Default)]
pub(super) struct GroupSettings {
    pub admins: Vec<i64>,
    pub address_names: Vec<String>,
}

#[derive(Default)]
pub(super) struct CommandCatalog {
    names: HashSet<String>,
    bootstrap: HashSet<String>,
}

impl CommandCatalog {
    pub fn update(&mut self, payload: &Value) {
        self.names = ["commands", "owner_commands"]
            .into_iter()
            .flat_map(|key| payload[key].as_array().into_iter().flatten())
            .filter_map(|entry| entry["command"].as_str())
            .map(str::to_owned)
            .collect();
        self.bootstrap = payload["bootstrap_commands"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .filter(|name| self.names.contains(*name))
            .map(str::to_owned)
            .collect();
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct MessageSource {
    pub tags: HashMap<String, String>,
}

impl MessageSource {
    pub fn apply(self, mut envelope: Envelope) -> Envelope {
        envelope
            .channel_metadata
            .get_or_insert_with(ChannelMetadata::new)
            .tags
            .extend(self.tags);
        envelope
    }

    pub fn merge(&mut self, next: Self) {
        // Album captions may mention the bot only on the first photo.
        let addressed = self.tags.get("addressed").is_some_and(|s| s == "true")
            || next.tags.get("addressed").is_some_and(|s| s == "true");
        self.tags = next.tags;
        self.tags.insert("addressed".into(), addressed.to_string());
    }
}

pub(super) fn actor_id(message: &Message) -> Option<i64> {
    // Anonymous admins and channel posts can carry a synthetic `from` user.
    message
        .sender_chat
        .is_none()
        .then(|| message.from.as_ref().map(|user| user.id.0 as i64))
        .flatten()
}

pub(super) fn is_admin(acl: &Acl, settings: &GroupSettings, actor: i64) -> bool {
    acl.role(actor) == Some(Role::Owner) || settings.admins.contains(&actor)
}

pub(super) fn command_name<'a>(text: &'a str, username: &str) -> Option<&'a str> {
    let word = text.split_whitespace().next()?.strip_prefix('/')?;
    let (name, target) = word
        .split_once('@')
        .map_or((word, None), |(n, t)| (n, Some(t)));
    if target.is_some_and(|target| !target.eq_ignore_ascii_case(username)) {
        return None;
    }
    Some(name)
}

pub(super) fn addressed(
    message: &Message,
    bot: UserId,
    username: &str,
    names: &[String],
    commands: &CommandCatalog,
) -> bool {
    let text = message.text().or_else(|| message.caption()).unwrap_or("");
    if let Some(word) = text
        .split_whitespace()
        .next()
        .and_then(|s| s.strip_prefix('/'))
    {
        if let Some((_, target)) = word.split_once('@') {
            return target.eq_ignore_ascii_case(username);
        }
        if message.forward_origin().is_none() && commands.names.contains(word) {
            return true;
        }
    }
    if message
        .reply_to_message()
        .is_some_and(|reply| reply.from.as_ref().is_some_and(|user| user.id == bot))
    {
        return true;
    }
    if message.forward_origin().is_some() {
        return false;
    }
    let mention = format!("@{username}");
    if message
        .parse_entities()
        .into_iter()
        .flatten()
        .chain(message.parse_caption_entities().into_iter().flatten())
        .any(|entity| match entity.kind() {
            MessageEntityKind::Mention => entity.text().eq_ignore_ascii_case(&mention),
            MessageEntityKind::TextMention { user } => user.id == bot,
            _ => false,
        })
    {
        return true;
    }
    let text = text.trim_start().to_lowercase();
    names
        .iter()
        .map(|name| name.trim().to_lowercase())
        .filter(|name| !name.is_empty())
        .any(|name| {
            text.strip_prefix(&name).is_some_and(|tail| {
                tail.is_empty()
                    || tail.starts_with(|c: char| {
                        c.is_whitespace() || matches!(c, ',' | ':' | '!' | '?' | '.')
                    })
            })
        })
}

/// Authorize chat AND actor independently; a group grant never becomes an owner grant.
pub(super) fn perceive(
    message: &Message,
    acl: Option<&Acl>,
    settings: &GroupSettings,
    bot: UserId,
    username: &str,
    commands: &CommandCatalog,
) -> Option<(Option<(Role, TrustLevel)>, MessageSource)> {
    if message.from.as_ref().is_some_and(|sender| sender.id == bot) {
        return None;
    }
    let text = message.text().or_else(|| message.caption()).unwrap_or("");
    let first = text.split_whitespace().next().unwrap_or("");
    if first.starts_with('/') && first.contains('@') && command_name(text, username).is_none() {
        return None;
    }
    let group = message.chat.is_group() || message.chat.is_supergroup();
    let actor = actor_id(message);
    let admin = acl.is_some_and(|acl| actor.is_some_and(|id| is_admin(acl, settings, id)));
    let called = !group || addressed(message, bot, username, &settings.address_names, commands);
    let command = message
        .forward_origin()
        .is_none()
        .then(|| command_name(text, username))
        .flatten();
    let bootstrap =
        group && admin && called && command.is_some_and(|name| commands.bootstrap.contains(name));
    let mut control_only = false;
    let trust = match acl {
        None => None,
        Some(acl) if group => {
            if acl.role(message.chat.id.0).is_none() {
                if !bootstrap {
                    return None;
                }
                control_only = true;
            }
            let role = actor
                .and_then(|id| acl.role(id))
                .or_else(|| admin.then_some(Role::Trusted))
                .or_else(|| {
                    (acl.group_mode(message.chat.id.0) == GroupMode::All).then_some(Role::Guest)
                })?;
            Some((role, role.trust()))
        }
        Some(acl) => {
            acl.role(message.chat.id.0)
                .or_else(|| admin.then_some(Role::Trusted))?;
            let role = actor.and_then(|id| acl.role(id)).unwrap_or(if admin {
                Role::Trusted
            } else {
                Role::Guest
            });
            Some((role, role.trust()))
        }
    };
    let mut tags = HashMap::from([
        ("chat_id".into(), message.chat.id.0.to_string()),
        (
            "chat_type".into(),
            if message.chat.is_supergroup() {
                "supergroup"
            } else if group {
                "group"
            } else {
                "private"
            }
            .into(),
        ),
        ("message_id".into(), message.id.0.to_string()),
        ("addressed".into(), called.to_string()),
        ("acl_admin".into(), admin.to_string()),
        ("bootstrap_only".into(), control_only.to_string()),
        (
            "forwarded".into(),
            message.forward_origin().is_some().to_string(),
        ),
    ]);
    // Preserve command syntax when reply context is prepended to the payload.
    if command.is_some() && message.text().is_some() {
        tags.insert("command_text".into(), text.into());
    }
    if let Some(title) = message.chat.title() {
        tags.insert("chat_title".into(), title.into());
    }
    if let Some(id) = actor {
        tags.insert("sender_id".into(), id.to_string());
        if let Some(user) = &message.from {
            tags.insert("sender_name".into(), user.full_name());
            if let Some(username) = &user.username {
                tags.insert("sender_username".into(), username.clone());
            }
        }
    }
    if let Some(chat) = &message.sender_chat {
        tags.insert("sender_chat_id".into(), chat.id.0.to_string());
    }
    if let Some(acl) = acl.filter(|_| group) {
        tags.insert(
            "group_mode".into(),
            acl.group_mode(message.chat.id.0).as_str().into(),
        );
    }
    Some((trust, MessageSource { tags }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn message(actor: i64, text: &str) -> Message {
        serde_json::from_value(json!({"message_id":1,"date":0,"chat":{"id":-42,"type":"supergroup","title":"room"},"from":{"id":actor,"is_bot":false,"first_name":"Person"},"text":text})).unwrap()
    }

    #[test]
    fn group_mode_does_not_grant_owner_and_commands_for_other_bots_stay_quiet() {
        let mut acl = Acl::new();
        acl.ensure(1, Role::Owner);
        acl.ensure(-42, Role::Trusted);
        let settings = GroupSettings {
            admins: vec![3],
            address_names: vec!["Альберт".into()],
        };
        let commands = CommandCatalog::default();
        assert!(
            perceive(
                &message(2, "Альберт, привет"),
                Some(&acl),
                &settings,
                UserId(99),
                "albert_bot",
                &commands
            )
            .is_none()
        );
        acl.set_group_mode(-42, GroupMode::All).unwrap();
        let (trust, source) = perceive(
            &message(2, "Альберт, привет"),
            Some(&acl),
            &settings,
            UserId(99),
            "albert_bot",
            &commands,
        )
        .unwrap();
        assert_eq!(trust.unwrap().0, Role::Guest);
        assert_eq!(source.tags["sender_id"], "2");
        assert_eq!(source.tags["addressed"], "true");
        assert!(!addressed(
            &message(2, "/help@other_bot"),
            UserId(99),
            "albert_bot",
            &settings.address_names,
            &commands
        ));
        assert!(!addressed(
            &message(2, "Мы обсуждаем Альберта"),
            UserId(99),
            "albert_bot",
            &settings.address_names,
            &commands
        ));
    }

    #[test]
    fn anonymous_sender_is_never_mistaken_for_the_owner() {
        let mut msg = message(1, "Альберт");
        msg.sender_chat = Some(msg.chat.clone());
        assert_eq!(actor_id(&msg), None);
    }

    #[test]
    fn registered_commands_mentions_and_replies_are_addressing_but_discussion_is_not() {
        let mut commands = CommandCatalog::default();
        commands.update(&json!({"commands":[{"command":"help"}],"owner_commands":[{"command":"groupmode"}],"bootstrap_commands":["groupmode"]}));
        let names = vec!["Альберт".into()];
        assert!(addressed(
            &message(2, "/help"),
            UserId(99),
            "albert_bot",
            &names,
            &commands
        ));
        assert!(addressed(
            &message(2, "/new_skill@albert_bot args"),
            UserId(99),
            "albert_bot",
            &names,
            &commands
        ));
        assert!(!addressed(
            &message(2, "/other_command"),
            UserId(99),
            "albert_bot",
            &names,
            &commands
        ));
        assert!(!addressed(
            &message(2, "Альбертовы советы"),
            UserId(99),
            "albert_bot",
            &names,
            &commands
        ));
        let mut value = serde_json::to_value(message(2, "\u{1f600} @albert_bot привет")).unwrap();
        value["entities"] = json!([{"type":"mention","offset":3,"length":11}]);
        assert!(addressed(
            &serde_json::from_value(value).unwrap(),
            UserId(99),
            "albert_bot",
            &names,
            &commands
        ));
        let mut value = serde_json::to_value(message(2, "да, продолжай")).unwrap();
        value["reply_to_message"] = serde_json::to_value(message(99, "Question")).unwrap();
        assert!(addressed(
            &serde_json::from_value(value).unwrap(),
            UserId(99),
            "albert_bot",
            &names,
            &commands
        ));
    }

    #[test]
    fn only_authorized_bootstrap_commands_cross_an_unlisted_group_boundary() {
        let mut acl = Acl::new();
        acl.ensure(1, Role::Owner);
        let mut commands = CommandCatalog::default();
        commands.update(&json!({"commands":[{"command":"chatinfo"}],"owner_commands":[{"command":"allow"}],"bootstrap_commands":["allow","chatinfo"]}));
        let settings = GroupSettings::default();
        let (_, source) = perceive(
            &message(1, "/allow"),
            Some(&acl),
            &settings,
            UserId(99),
            "bot",
            &commands,
        )
        .unwrap();
        assert_eq!(source.tags["bootstrap_only"], "true");
        assert!(
            perceive(
                &message(2, "/allow"),
                Some(&acl),
                &settings,
                UserId(99),
                "bot",
                &commands
            )
            .is_none()
        );
        assert!(
            perceive(
                &message(1, "hello"),
                Some(&acl),
                &settings,
                UserId(99),
                "bot",
                &commands
            )
            .is_none()
        );
    }
    #[test]
    fn reply_commands_keep_raw_syntax_and_forward_batches_keep_authors_separate() {
        let mut acl = Acl::new();
        acl.ensure(1, Role::Owner);
        let mut commands = CommandCatalog::default();
        commands.update(
            &json!({"commands":[{"command":"chatinfo"}],"bootstrap_commands":["chatinfo"]}),
        );
        let (_, source) = perceive(
            &message(1, "/chatinfo"),
            Some(&acl),
            &GroupSettings::default(),
            UserId(99),
            "bot",
            &commands,
        )
        .unwrap();
        assert_eq!(source.tags["command_text"], "/chatinfo");
        let album = |actor| {
            let mut value = serde_json::to_value(message(actor, "photo caption")).unwrap();
            value["forward_origin"] =
                json!({"type":"hidden_user","date":0,"sender_user_name":"Original"});
            serde_json::from_value::<Message>(value).unwrap()
        };
        assert_ne!(
            crate::inbound::coalesce_key(&album(1), "-42"),
            crate::inbound::coalesce_key(&album(2), "-42")
        );
    }
}
