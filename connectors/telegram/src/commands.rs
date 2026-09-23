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
    let common = match menu(payload.get("commands")) {
        Ok(m) => m,
        Err(e) => return json!({ "ok": false, "error": e }),
    };
    let owner_only = match menu(payload.get("owner_commands")) {
        Ok(m) => m,
        Err(e) => return json!({ "ok": false, "error": e }),
    };

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

/// `[{ command, description }]` → Telegram commands, validated (a leading `/` is dropped,
/// an over-long description trimmed).
fn menu(list: Option<&Value>) -> Result<Vec<BotCommand>, String> {
    let Some(items) = list.and_then(Value::as_array) else {
        return Ok(Vec::new());
    };
    items
        .iter()
        .map(|item| {
            let command = item.get("command").and_then(Value::as_str).unwrap_or("").trim_start_matches('/');
            let ok = !command.is_empty()
                && command.len() <= MAX_COMMAND
                && command.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
            if !ok {
                return Err(format!("bad command {command:?}: 1-{MAX_COMMAND} of a-z, 0-9, _"));
            }
            let description = item.get("description").and_then(Value::as_str).unwrap_or("").trim();
            let description = if description.is_empty() { command } else { description };
            Ok(BotCommand::new(command, description.chars().take(MAX_DESCRIPTION).collect::<String>()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::menu;
    use serde_json::json;

    #[test]
    fn a_menu_is_validated_and_normalised() {
        let list = json!([{ "command": "/brief", "description": "Morning brief" }, { "command": "help" }]);
        let m = menu(Some(&list)).unwrap();
        assert_eq!(m[0].command, "brief");
        assert_eq!(m[1].description, "help"); // no description -> the command itself
        assert!(menu(Some(&json!([{ "command": "Bad-Name" }]))).is_err());
        assert!(menu(Some(&json!([{ "command": "x".repeat(33) }]))).is_err());
        assert!(menu(None).unwrap().is_empty());
    }
}
