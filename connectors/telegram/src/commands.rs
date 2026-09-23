//! `chat.set_commands` — publish the bot's command menu (Telegram `setMyCommands`): one
//! list for everyone, and in each owner's chat that list plus the owner-only commands.
//!
//! The assembly sends it (not the model): the menu is part of how the channel is set up.
//! Payload `{ commands: [{ command, description }], owner_commands?: [..] }` → a correlated
//! `chat.set_commands.result { ok, commands, owner_chats }`.

use serde_json::{json, Value};
use teloxide::{
    prelude::*,
    types::{BotCommand, BotCommandScope, ChatId, Recipient},
};

/// Command kind this module handles.
pub(crate) const SET_COMMANDS: &str = "chat.set_commands";
/// Telegram's limits: a command is 1-32 of `a-z0-9_`, its description 1-256 characters.
const MAX_COMMAND: usize = 32;
const MAX_DESCRIPTION: usize = 256;

/// Set the default menu, then each owner chat's (default + owner commands).
pub(crate) async fn set_commands(bot: &Bot, owners: &[i64], payload: &Value) -> Value {
    let common = menu(payload.get("commands"));
    let owner_only = menu(payload.get("owner_commands"));

    if let Err(e) = bot.set_my_commands(common.clone()).await {
        return json!({ "ok": false, "error": format!("setMyCommands: {e}") });
    }
    let mut owner_chats = 0;
    if !owner_only.is_empty() {
        let full: Vec<BotCommand> = common.iter().chain(owner_only.iter()).cloned().collect();
        for &owner in owners {
            let scope = BotCommandScope::Chat { chat_id: Recipient::Id(ChatId(owner)) };
            match bot.set_my_commands(full.clone()).scope(scope).await {
                Ok(_) => owner_chats += 1,
                Err(e) => tracing::warn!(error = %e, "telegram: setMyCommands for an owner chat failed"),
            }
        }
    }
    tracing::info!(commands = common.len(), owner_commands = owner_only.len(), owner_chats, "telegram: command menu set");
    json!({ "ok": true, "commands": common.len(), "owner_chats": owner_chats })
}

/// `[{ command, description }]` → Telegram commands. Telegram's own rules are applied
/// here, not by the sender: a leading `/` is dropped, a description trimmed to 256
/// characters, and a command Telegram can't show (not 1-32 of `a-z0-9_`) is left out of the
/// menu with a warning — it still works when typed.
fn menu(list: Option<&Value>) -> Vec<BotCommand> {
    let Some(items) = list.and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for item in items {
        let command = item.get("command").and_then(Value::as_str).unwrap_or("").trim_start_matches('/');
        let fits = !command.is_empty()
            && command.len() <= MAX_COMMAND
            && command.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
        if !fits {
            tracing::warn!(command, "telegram: command left out of the menu (Telegram takes 1-32 of a-z, 0-9, _)");
            continue;
        }
        let description = item.get("description").and_then(Value::as_str).unwrap_or("").trim();
        let description = if description.is_empty() { command } else { description };
        out.push(BotCommand::new(command, description.chars().take(MAX_DESCRIPTION).collect::<String>()));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::menu;
    use serde_json::json;

    #[test]
    fn a_menu_is_validated_and_normalised() {
        let list = json!([{ "command": "/brief", "description": "Morning brief" }, { "command": "help" }]);
        let m = menu(Some(&list));
        assert_eq!(m[0].command, "brief");
        assert_eq!(m[1].description, "help"); // no description -> the command itself
        // What Telegram can't show is left out, not a failure of the whole menu.
        let mixed = json!([{ "command": "my-command" }, { "command": "x".repeat(33) }, { "command": "ok" }]);
        let m = menu(Some(&mixed));
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].command, "ok");
        assert!(menu(None).is_empty());
    }
}
