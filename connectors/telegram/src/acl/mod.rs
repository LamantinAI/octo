//! Channel access-control list for the Telegram connector.
//!
//! A whitelist of `chat_id → role`, loaded from a JSON state file. Unknown chats
//! are denied **at the edge** — their messages never reach the bus, so the
//! cognition layer never sees untrusted input (the strongest prompt-injection
//! boundary, and "don't even respond in unauthorized chats").
//!
//! The file is the *mutable* tier (an owner can extend it at runtime in a later
//! phase); the connector's static config (path to this file, seed owner) lives
//! in the TOML manifest. Same split as the scheduler's `scheduler.json` state.

use std::{collections::HashMap, path::Path};

use octo_core::TrustLevel;
use serde::{Deserialize, Serialize};

/// Role of an allowed chat. The coarse [`TrustLevel`] is stamped on the envelope
/// for generic reflex gating; the precise role rides on a `role` tag for
/// capability checks (e.g. only `owner` may mutate the ACL or schedule).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Owner,
    Trusted,
    Guest,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Owner => "owner",
            Role::Trusted => "trusted",
            Role::Guest => "guest",
        }
    }

    /// Map a role onto the envelope trust gradient.
    pub fn trust(self) -> TrustLevel {
        match self {
            Role::Owner => TrustLevel::High,
            Role::Trusted => TrustLevel::Medium,
            Role::Guest => TrustLevel::Low,
        }
    }
}

/// One ACL entry as stored on disk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AclEntry {
    pub chat_id: i64,
    pub role: Role,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GroupMode {
    #[default]
    Allowed,
    All,
}

impl GroupMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Allowed => "allowed",
            Self::All => "all",
        }
    }
}

#[derive(Serialize, Deserialize)]
struct StoredEntry {
    #[serde(flatten)]
    entry: AclEntry,
    #[serde(default)]
    group_mode: GroupMode,
}

/// In-memory access-control list: `chat_id → role`.
#[derive(Debug, Clone, Default)]
pub struct Acl {
    entries: HashMap<i64, Role>,
    groups: HashMap<i64, GroupMode>,
}

impl Acl {
    pub fn new() -> Self {
        Self::default()
    }

    /// Load from a JSON file (an array of `{chat_id, role}`). A **missing** file
    /// is not an error — it yields an empty ACL (seed the owner via
    /// [`ensure`](Self::ensure) so the bot is reachable on first run).
    pub fn load(path: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let stored = match std::fs::read(path) {
            Ok(bytes) => serde_json::from_slice::<Vec<StoredEntry>>(&bytes)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(Box::new(e)),
        };
        let mut acl = Self::new();
        for item in stored {
            acl.insert(item.entry.chat_id, item.entry.role);
            if item.entry.chat_id < 0 {
                acl.groups.insert(item.entry.chat_id, item.group_mode);
            }
        }
        Ok(acl)
    }

    /// Insert `chat_id` at `role` if absent (used to seed the owner from config).
    pub fn ensure(&mut self, chat_id: i64, role: Role) {
        self.entries.entry(chat_id).or_insert(role);
    }

    /// The role of a chat, or `None` if it is not on the list (denied).
    pub fn role(&self, chat_id: i64) -> Option<Role> {
        self.entries.get(&chat_id).copied()
    }

    /// Add or update a chat's role. Returns `true` if it was newly added.
    pub fn insert(&mut self, chat_id: i64, role: Role) -> bool {
        self.entries.insert(chat_id, role).is_none()
    }

    /// Remove a chat. Returns `true` if it was present.
    pub fn remove(&mut self, chat_id: i64) -> bool {
        self.groups.remove(&chat_id);
        self.entries.remove(&chat_id).is_some()
    }

    pub fn group_mode(&self, chat_id: i64) -> GroupMode {
        self.groups.get(&chat_id).copied().unwrap_or_default()
    }

    pub fn set_group_mode(&mut self, chat_id: i64, mode: GroupMode) -> Result<(), &'static str> {
        if chat_id >= 0 {
            return Err("group mode requires a negative group chat id");
        }
        self.ensure(chat_id, Role::Trusted);
        self.groups.insert(chat_id, mode);
        Ok(())
    }

    /// Telegram's migration event is the authority for the new group id.
    pub fn migrate_group(&mut self, old: i64, new: i64) -> bool {
        if old >= 0 || new >= 0 || old == new {
            return false;
        }
        let Some(role) = self.entries.remove(&old) else {
            return false;
        };
        let mode = self.groups.remove(&old).unwrap_or_default();
        // Do not overwrite a separately configured destination.
        self.entries.entry(new).or_insert(role);
        self.groups.entry(new).or_insert(mode);
        true
    }

    /// Snapshot as a chat-id-sorted list (for persistence and listing).
    pub fn entries(&self) -> Vec<AclEntry> {
        let mut v: Vec<AclEntry> = self
            .entries
            .iter()
            .map(|(&chat_id, &role)| AclEntry { chat_id, role })
            .collect();
        v.sort_by_key(|e| e.chat_id);
        v
    }

    /// Atomically persist to `path`: write a sibling temp file, then rename over
    /// the target (so a crash mid-write can't corrupt the list).
    pub fn save(&self, path: &Path) -> std::io::Result<()> {
        let stored: Vec<_> = self
            .entries()
            .into_iter()
            .map(|entry| StoredEntry {
                group_mode: self.group_mode(entry.chat_id),
                entry,
            })
            .collect();
        let json = serde_json::to_vec_pretty(&stored)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, json)?;
        std::fs::rename(&tmp, path)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_maps_to_trust() {
        assert_eq!(Role::Owner.trust(), TrustLevel::High);
        assert_eq!(Role::Trusted.trust(), TrustLevel::Medium);
    }

    #[test]
    fn parses_entries_and_looks_up_role() {
        let json = br#"[{"chat_id":1,"role":"owner"},{"chat_id":2,"role":"trusted"}]"#;
        let list: Vec<AclEntry> = serde_json::from_slice(json).unwrap();
        let mut acl = Acl::new();
        for e in list {
            acl.ensure(e.chat_id, e.role);
        }
        assert_eq!(acl.role(1), Some(Role::Owner));
        assert_eq!(acl.role(2), Some(Role::Trusted));
        assert_eq!(acl.role(999), None, "unlisted chat is denied");
        assert_eq!(acl.len(), 2);
    }

    #[test]
    fn ensure_does_not_downgrade_existing() {
        let mut acl = Acl::new();
        acl.ensure(1, Role::Owner);
        acl.ensure(1, Role::Trusted); // already present → no change
        assert_eq!(acl.role(1), Some(Role::Owner));
    }

    #[test]
    fn missing_file_is_empty_not_error() {
        let acl = Acl::load(Path::new("/no/such/telegram_acl.json")).unwrap();
        assert!(acl.is_empty());
    }

    #[test]
    fn group_modes_survive_reload_and_migration_without_granting_users_access() {
        let root = std::env::temp_dir().join(format!("octo-group-mode-{}", std::process::id()));
        let path = root.join("acl.json");
        let mut acl = Acl::new();
        acl.ensure(1, Role::Owner);
        acl.set_group_mode(-42, GroupMode::All).unwrap();
        assert!(acl.set_group_mode(123, GroupMode::All).is_err());
        acl.save(&path).unwrap();
        let mut loaded = Acl::load(&path).unwrap();
        assert_eq!(loaded.group_mode(-42), GroupMode::All);
        assert_eq!(loaded.role(123), None);
        assert!(loaded.migrate_group(-42, -10042));
        assert_eq!(loaded.role(-42), None);
        assert_eq!(loaded.group_mode(-10042), GroupMode::All);
        loaded.remove(-10042);
        loaded.insert(-10042, Role::Trusted);
        assert_eq!(loaded.group_mode(-10042), GroupMode::Allowed);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn save_then_reload_round_trip() {
        let dir = std::env::temp_dir().join(format!("octo_acl_test_{}", std::process::id()));
        let path = dir.join("acl.json");
        let mut acl = Acl::new();
        acl.insert(1, Role::Owner);
        acl.insert(2, Role::Trusted);
        assert!(acl.remove(2));
        acl.insert(2, Role::Trusted);
        acl.save(&path).unwrap();

        let reloaded = Acl::load(&path).unwrap();
        assert_eq!(reloaded.role(1), Some(Role::Owner));
        assert_eq!(reloaded.role(2), Some(Role::Trusted));
        assert_eq!(reloaded.len(), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
