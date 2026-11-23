//! Optional host-owned menus with bounded successful-delivery caching.
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use teloxide::{
    Bot,
    payloads::SetMyCommandsSetters,
    requests::Requester,
    types::{BotCommand, BotCommandScope, ChatId, Recipient, UserId},
};
use tracing::warn;

use crate::{TelegramHostPolicy, VerifiedActor};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TelegramCommand {
    pub command: String,
    pub description: String,
}

#[derive(Clone, Debug)]
pub enum CommandMenuScope {
    PrivateChats,
    GroupChats,
    Actor(VerifiedActor),
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Key {
    Private,
    Groups,
    Actor(i64, i64),
}

const MAX_CACHED_MENUS: usize = 512;

#[derive(Default)]
pub(crate) struct HostMenus {
    cache: HashMap<Key, Vec<TelegramCommand>>,
    order: VecDeque<Key>,
    managed: Arc<AtomicBool>,
    startup_pending: bool,
}
impl HostMenus {
    pub(crate) fn managed_flag(&self) -> Arc<AtomicBool> {
        self.managed.clone()
    }
    pub(crate) async fn startup(
        &mut self,
        bot: &Bot,
        policy: Option<&Arc<dyn TelegramHostPolicy>>,
    ) -> Result<(), ()> {
        let Some(policy) = policy else { return Ok(()) };
        let private = policy
            .command_menu(CommandMenuScope::PrivateChats)
            .await
            .map_err(|_| ())?;
        if private.is_some() {
            self.managed.store(true, Ordering::Release);
        }
        let groups = policy
            .command_menu(CommandMenuScope::GroupChats)
            .await
            .map_err(|_| ())?;
        if groups.is_some() {
            self.managed.store(true, Ordering::Release);
        }
        if private.is_none() && groups.is_none() {
            return Ok(());
        }
        self.startup_pending = true;
        // Empty scopes can fall back to Default; remove the legacy global menu.
        bot.set_my_commands(Vec::<BotCommand>::new())
            .scope(BotCommandScope::Default)
            .await
            .map_err(|_| ())?;
        if let Some(commands) = private {
            self.apply(
                bot,
                Key::Private,
                BotCommandScope::AllPrivateChats,
                commands,
            )
            .await?;
        }
        if let Some(commands) = groups {
            self.apply(bot, Key::Groups, BotCommandScope::AllGroupChats, commands)
                .await?;
        }
        self.startup_pending = false;
        Ok(())
    }

    pub(crate) async fn refresh(
        &mut self,
        bot: &Bot,
        policy: Option<&Arc<dyn TelegramHostPolicy>>,
        actor: VerifiedActor,
    ) {
        if actor.actor_is_bot || actor.actor_id <= 0 {
            return;
        }
        let Some(policy) = policy else { return };
        if self.startup_pending {
            let _ = self.startup(bot, Some(policy)).await;
        }
        let scope = match actor.chat_type.as_str() {
            "private" if actor.actor_id == actor.chat_id => BotCommandScope::Chat {
                chat_id: Recipient::Id(ChatId(actor.chat_id)),
            },
            "group" | "supergroup" => BotCommandScope::ChatMember {
                chat_id: Recipient::Id(ChatId(actor.chat_id)),
                user_id: UserId(actor.actor_id as u64),
            },
            _ => return,
        };
        let key = Key::Actor(actor.chat_id, actor.actor_id);
        match policy.command_menu(CommandMenuScope::Actor(actor)).await {
            Ok(Some(commands)) => {
                self.managed.store(true, Ordering::Release);
                if self.apply(bot, key, scope, commands).await.is_err() {
                    warn!("telegram: host command menu update failed");
                }
            }
            Ok(None) => {}
            Err(_) => warn!("telegram: host command menu lookup failed"),
        }
    }

    async fn apply(
        &mut self,
        bot: &Bot,
        key: Key,
        scope: BotCommandScope,
        commands: Vec<TelegramCommand>,
    ) -> Result<(), ()> {
        if self.cache.get(&key) == Some(&commands) {
            return Ok(());
        }
        let native = native_menu(&commands)?;
        bot.set_my_commands(native)
            .scope(scope)
            .await
            .map_err(|_| ())?;
        if !self.cache.contains_key(&key) {
            if self.cache.len() >= MAX_CACHED_MENUS {
                if let Some(oldest) = self.order.pop_front() {
                    self.cache.remove(&oldest);
                }
            }
            self.order.push_back(key.clone());
        }
        self.cache.insert(key, commands);
        Ok(())
    }
}

fn native_menu(commands: &[TelegramCommand]) -> Result<Vec<BotCommand>, ()> {
    if commands.len() > 100
        || commands.iter().any(|entry| {
            entry.command.is_empty()
                || entry.command.len() > 32
                || !entry
                    .command
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
                || entry.description.is_empty()
                || entry.description.chars().count() > 256
                || entry.description.chars().any(char::is_control)
        })
    {
        return Err(());
    }
    Ok(commands
        .iter()
        .map(|entry| BotCommand::new(entry.command.clone(), entry.description.clone()))
        .collect())
}
