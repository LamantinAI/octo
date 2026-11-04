//! `chat.set_commands` — publish the bot's command menu (Telegram `setMyCommands`): one
//! list for everyone, and in each owner's chat that list plus the owner-only commands.
//!
//! The assembly sends it (not the model): the menu is part of how the channel is set up.
//! Payload `{ commands: [{ command, description }], owner_commands?: [..] }` → a correlated
//! `chat.set_commands.result { ok, commands, owner_chats }`.

mod host;
pub(crate) use host::HostMenus;
pub use host::{CommandMenuScope, TelegramCommand};

use serde_json::{Value, json};
use teloxide::{
    Bot,
    payloads::SetMyCommandsSetters,
    requests::Requester,
    types::{BotCommand, BotCommandScope, ChatId, Recipient},
};

use tracing::{info, warn};

/// Command kind this module handles.
pub(crate) const SET_COMMANDS: &str = "chat.set_commands";
/// Telegram's limits: a command is 1-32 of `a-z0-9_`, its description 1-256 characters.
const MAX_COMMAND: usize = 32;
const MAX_DESCRIPTION: usize = 256;

/// Set the default menu, then each owner chat's (default + owner commands).
pub(crate) async fn set_commands(
    bot: &Bot,
    owners: &[i64],
    payload: &Value,
    managed: bool,
) -> Value {
    if managed {
        return json!({ "ok":true, "managed":true });
    }
    let common = menu(payload.get("commands"));
    let owner_only = menu(payload.get("owner_commands"));

    if let Err(_) = bot.set_my_commands(common.clone()).await {
        return json!({ "ok": false, "error": "Telegram command menu update failed" });
    }
    let mut owner_chats = 0;
    if !owner_only.is_empty() {
        let full: Vec<BotCommand> = common.iter().chain(owner_only.iter()).cloned().collect();
        for &owner in owners {
            let scope = BotCommandScope::Chat {
                chat_id: Recipient::Id(ChatId(owner)),
            };
            match bot.set_my_commands(full.clone()).scope(scope).await {
                Ok(_) => owner_chats += 1,
                Err(_) => {
                    warn!("telegram: setMyCommands for an owner chat failed")
                }
            }
        }
    }
    info!(
        commands = common.len(),
        owner_commands = owner_only.len(),
        owner_chats,
        "telegram: command menu set"
    );
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
        let command = item
            .get("command")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim_start_matches('/');
        let fits = !command.is_empty()
            && command.len() <= MAX_COMMAND
            && command
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
        if !fits {
            warn!(
                command,
                "telegram: command left out of the menu (Telegram takes 1-32 of a-z, 0-9, _)"
            );
            continue;
        }
        let description = item
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim();
        let description = if description.is_empty() {
            command
        } else {
            description
        };
        out.push(BotCommand::new(
            command,
            description
                .chars()
                .take(MAX_DESCRIPTION)
                .collect::<String>(),
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{CommandMenuScope, HostMenus, TelegramCommand, menu, set_commands};
    use crate::{
        IngressDecision, NativeFrame, TelegramHostPolicy, VerifiedActor, VerifiedBot,
        callbacks::testing::server,
    };
    use async_trait::async_trait;
    use axum::{Json, Router, routing::post, serve};
    use octo_core::{ConnectorId, Envelope};
    use reqwest::Url;
    use serde_json::json;
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };
    use teloxide::Bot;
    use tokio::{net::TcpListener, spawn, time::timeout};

    struct Menus {
        registered: AtomicBool,
        enabled: bool,
    }
    #[async_trait]
    impl TelegramHostPolicy for Menus {
        async fn admit(&self, _: NativeFrame, _: Envelope) -> Result<IngressDecision, String> {
            Ok(IngressDecision::Ignore)
        }
        async fn command_menu(
            &self,
            scope: CommandMenuScope,
        ) -> Result<Option<Vec<TelegramCommand>>, String> {
            if !self.enabled {
                return Ok(None);
            }
            let command = match scope {
                CommandMenuScope::GroupChats => return Ok(Some(vec![])),
                CommandMenuScope::PrivateChats => "connect",
                CommandMenuScope::Actor(_) if self.registered.load(Ordering::SeqCst) => "settings",
                CommandMenuScope::Actor(_) => "connect",
            };
            Ok(Some(vec![TelegramCommand {
                command: command.into(),
                description: "Open controls".into(),
            }]))
        }
    }
    fn actor(chat_id: i64) -> VerifiedActor {
        VerifiedActor {
            bot: VerifiedBot {
                connector: ConnectorId::new("telegram"),
                bot_id: 9,
                username: "test_bot".into(),
            },
            actor_id: 7,
            actor_is_bot: false,
            chat_id,
            chat_type: if chat_id > 0 { "private" } else { "supergroup" }.into(),
        }
    }

    #[tokio::test]
    async fn host_menus_clear_stale_defaults_scope_members_cache_and_refresh_changed_roles() {
        let server = server().await;
        let host = Arc::new(Menus {
            registered: AtomicBool::new(true),
            enabled: true,
        });
        let policy: Arc<dyn TelegramHostPolicy> = host.clone();
        let mut menus = HostMenus::default();
        menus.startup(&server.bot, Some(&policy)).await.unwrap();
        menus.refresh(&server.bot, Some(&policy), actor(7)).await;
        menus.refresh(&server.bot, Some(&policy), actor(7)).await;
        menus.refresh(&server.bot, Some(&policy), actor(-42)).await;
        host.registered.store(false, Ordering::SeqCst);
        menus.refresh(&server.bot, Some(&policy), actor(7)).await;
        let requests = server.requests.lock().unwrap();
        assert_eq!(requests.len(), 6, "identical actor menus use the cache");
        let bodies: Vec<_> = requests.iter().map(|(_, body)| body).collect();
        assert_eq!(bodies[0]["scope"]["type"], "default");
        assert_eq!(bodies[0]["commands"], json!([]));
        assert_eq!(bodies[1]["scope"]["type"], "all_private_chats");
        assert_eq!(bodies[1]["commands"][0]["command"], "connect");
        assert_eq!(bodies[2]["scope"]["type"], "all_group_chats");
        assert_eq!(bodies[2]["commands"], json!([]));
        assert_eq!(bodies[3]["scope"]["type"], "chat");
        assert_eq!(bodies[3]["commands"][0]["command"], "settings");
        assert_eq!(bodies[4]["scope"]["type"], "chat_member");
        assert_eq!(bodies[4]["scope"]["user_id"], 7);
        assert_eq!(bodies[5]["commands"][0]["command"], "connect");
    }

    #[tokio::test]
    async fn absent_host_menus_preserve_legacy_without_native_calls() {
        let server = server().await;
        let policy: Arc<dyn TelegramHostPolicy> = Arc::new(Menus {
            registered: AtomicBool::new(false),
            enabled: false,
        });
        let mut menus = HostMenus::default();
        menus.startup(&server.bot, Some(&policy)).await.unwrap();
        menus.refresh(&server.bot, Some(&policy), actor(7)).await;
        assert!(server.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn managed_legacy_menu_requests_ack_without_overwriting_host_scopes() {
        let server = server().await;
        let policy: Arc<dyn TelegramHostPolicy> = Arc::new(Menus {
            registered: AtomicBool::new(false),
            enabled: true,
        });
        let mut menus = HostMenus::default();
        let managed = menus.managed_flag();
        menus.startup(&server.bot, Some(&policy)).await.unwrap();
        let before = server.requests.lock().unwrap().len();
        let payload = json!({"commands":[{"command":"help","description":"Help"}],"owner_commands":[{"command":"settings","description":"Settings"}]});
        let result =
            set_commands(&server.bot, &[7], &payload, managed.load(Ordering::Acquire)).await;
        assert_eq!(result, json!({"ok":true,"managed":true}));
        assert_eq!(server.requests.lock().unwrap().len(), before);
        let legacy = HostMenus::default();
        let result = set_commands(
            &server.bot,
            &[7],
            &payload,
            legacy.managed_flag().load(Ordering::Acquire),
        )
        .await;
        assert_eq!(result["ok"], true);
        assert_eq!(result["owner_chats"], 1);
        assert_eq!(server.requests.lock().unwrap().len(), before + 2);
    }

    #[tokio::test]
    async fn temporary_native_menu_failure_keeps_managed_ack_enabled() {
        let app = Router::new().fallback(post(|| async {
            Json(json!({"ok":false,"error_code":500,"description":"temporary rejection"}))
        }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = spawn(async move { serve(listener, app).await.unwrap() });
        let bot = Bot::new("9:FAKE").set_api_url(Url::parse(&format!("http://{address}")).unwrap());
        let policy: Arc<dyn TelegramHostPolicy> = Arc::new(Menus {
            registered: AtomicBool::new(false),
            enabled: true,
        });
        let mut menus = HostMenus::default();
        let managed = menus.managed_flag();
        assert!(
            timeout(Duration::from_secs(2), menus.startup(&bot, Some(&policy)))
                .await
                .unwrap()
                .is_err()
        );
        assert!(managed.load(Ordering::Acquire));
        let result = set_commands(&bot, &[], &json!({}), managed.load(Ordering::Acquire)).await;
        assert_eq!(result, json!({"ok":true,"managed":true}));
        server.abort();
    }

    #[test]
    fn a_menu_is_validated_and_normalised() {
        let list =
            json!([{ "command": "/brief", "description": "Morning brief" }, { "command": "help" }]);
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
