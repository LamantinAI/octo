use std::{
    path::PathBuf,
    sync::{Arc, RwLock},
    time::Duration,
};

use octo_core::{
    Connector, ConnectorContext, ConnectorFactory, ConnectorId, Envelope, EventKind, FactoryContext,
};
use serde::Deserialize;
use serde_json::{Value, json};

use super::{
    ALLOW_CHAT, AclState, DEFAULT_DEBOUNCE, DEFAULT_MAX_WAIT, LIST_CHATS, REMOVE_CHAT,
    TelegramConnector,
};
use crate::{Acl, Role};

/// Handle an `octo.telegram.*` control command: mutate the ACL, persist it, and
/// publish a correlated `<kind>.result` reply. Authorization (only the owner may
/// run these) is enforced *upstream* by the dispatching cogitator — the command
/// reaching here is already vouched for; the connector just applies it.
pub(super) async fn handle_control(
    acl: &Option<Arc<AclState>>,
    id: &ConnectorId,
    env: &Envelope,
    ctx: &ConnectorContext,
) {
    let Some(state) = acl else {
        tracing::warn!(kind = %env.kind, "telegram: control command but no ACL configured; ignored");
        return;
    };
    let payload = env.payload_as::<Value>().cloned().unwrap_or(Value::Null);
    let chat_id = payload.get("chat_id").and_then(Value::as_i64);

    let result = match env.kind.as_str() {
        ALLOW_CHAT => match chat_id {
            Some(chat_id) => {
                let role = match payload.get("role").and_then(Value::as_str) {
                    Some("owner") => Role::Owner,
                    _ => Role::Trusted,
                };
                let added = {
                    let mut acl = state.acl.write().unwrap();
                    let added = acl.insert(chat_id, role);
                    persist(&acl, &state.path);
                    added
                };
                tracing::info!(chat_id, role = role.as_str(), added, "telegram: allow_chat");
                json!({ "ok": true, "chat_id": chat_id, "role": role.as_str(), "added": added })
            }
            None => json!({ "ok": false, "error": "missing or non-integer chat_id" }),
        },
        REMOVE_CHAT => match chat_id {
            Some(chat_id) => {
                let removed = {
                    let mut acl = state.acl.write().unwrap();
                    let removed = acl.remove(chat_id);
                    persist(&acl, &state.path);
                    removed
                };
                tracing::info!(chat_id, removed, "telegram: remove_chat");
                json!({ "ok": true, "chat_id": chat_id, "removed": removed })
            }
            None => json!({ "ok": false, "error": "missing or non-integer chat_id" }),
        },
        LIST_CHATS => {
            let chats = state.acl.read().unwrap().entries();
            json!({ "ok": true, "chats": chats })
        }
        _ => json!({ "ok": false, "error": "unknown command" }),
    };

    let resp = Envelope::new(
        id.clone(),
        EventKind::new(format!("{}.result", env.kind.as_str())),
        result,
    )
    .with_correlation(env.id);
    if let Err(e) = ctx.publish(resp).await {
        tracing::warn!(error = %e, "telegram: failed to publish control result");
    }
}

/// The chats the ACL marks as owners (none without an ACL).
pub(super) fn owner_chats(acl: &Option<Arc<AclState>>) -> Vec<i64> {
    acl.as_ref()
        .map(|state| {
            state
                .acl
                .read()
                .unwrap()
                .entries()
                .into_iter()
                .filter(|e| e.role == Role::Owner)
                .map(|e| e.chat_id)
                .collect()
        })
        .unwrap_or_default()
}

/// Persist the ACL to its file if one is configured, logging (not failing) on error.
pub(super) fn persist(acl: &Acl, path: &Option<PathBuf>) {
    if let Some(p) = path {
        if let Err(e) = acl.save(p) {
            tracing::warn!(error = %e, path = %p.display(), "telegram: failed to persist ACL");
        }
    }
}

// ── Config-driven construction (`type = "telegram"` manifest) ────────────────

/// One connector manifest file (`[connector]` table at its root).
#[derive(Debug, Deserialize)]
struct ConnectorFile {
    connector: TelegramConfig,
}

/// Static config from a `type = "telegram"` manifest. The token is a secret, so
/// the manifest names the env var that holds it rather than the value.
#[derive(Debug, Deserialize)]
struct TelegramConfig {
    #[serde(default = "default_token_env")]
    token_env: String,
    /// Path (relative to the manifest) to the JSON ACL file. Absent **and** no
    /// `owner_chat` → no access control (allow all).
    acl_path: Option<String>,
    /// Seed owner chat id — inserted into the ACL at `owner`, so the bot is
    /// reachable on first run even with an empty/absent ACL file.
    owner_chat: Option<i64>,
    /// Coalescing quiet window in ms (default 500; `0` disables coalescing).
    batch_debounce_ms: Option<u64>,
    /// Cap in ms on how long a chat's buffer stays open (default 3000).
    batch_max_wait_ms: Option<u64>,
    /// Shared workspace root (relative to the manifest) for file transfer. Must
    /// match octo-code's. Absent → `$OCTO_CODE_WORKSPACE`, then the default.
    workspace: Option<String>,
}

pub(super) fn default_token_env() -> String {
    "OCTO_TELEGRAM_TOKEN".to_string()
}

/// [`ConnectorFactory`] for `type = "telegram"`. Register once with
/// `Octo::builder().register_connector_type("telegram", octo_connector_telegram::factory())`,
/// and every manifest with `type = "telegram"` becomes an instance.
pub struct TelegramConnectorFactory;

impl ConnectorFactory for TelegramConnectorFactory {
    fn type_name(&self) -> &str {
        "telegram"
    }

    fn create(
        &self,
        id: ConnectorId,
        config: &toml::Value,
        ctx: FactoryContext<'_>,
    ) -> Result<Arc<dyn Connector>, Box<dyn std::error::Error + Send + Sync>> {
        let file: ConnectorFile = config.clone().try_into()?;
        let cfg = file.connector;
        let token = std::env::var(&cfg.token_env)
            .map_err(|_| format!("telegram: env var {} is not set", cfg.token_env))?;

        let debounce = cfg
            .batch_debounce_ms
            .map(Duration::from_millis)
            .unwrap_or(DEFAULT_DEBOUNCE);
        let max_wait = cfg
            .batch_max_wait_ms
            .map(Duration::from_millis)
            .unwrap_or(DEFAULT_MAX_WAIT);
        let workspace: Option<PathBuf> = cfg.workspace.as_ref().map(|w| ctx.base_dir.join(w));

        // No ACL configured → allow-all connector (the playground shape).
        let acl_state = if cfg.acl_path.is_none() && cfg.owner_chat.is_none() {
            None
        } else {
            // Resolve the ACL path (relative to the manifest) once — used to load
            // it now and to persist runtime mutations later.
            let acl_path: Option<PathBuf> = cfg.acl_path.as_ref().map(|p| ctx.base_dir.join(p));
            let mut acl = match &acl_path {
                Some(p) => Acl::load(p)?,
                None => Acl::new(),
            };
            if let Some(owner) = cfg.owner_chat {
                acl.ensure(owner, Role::Owner);
            }
            tracing::info!(connector = %id, allowed = acl.len(), "telegram: access-control list loaded");
            Some(Arc::new(AclState {
                acl: RwLock::new(acl),
                path: acl_path,
            }))
        };
        Ok(TelegramConnector::build(
            id.as_str(),
            token,
            acl_state,
            debounce,
            max_wait,
            workspace,
        ))
    }
}

/// Convenience factory handle for registration.
pub fn factory() -> Arc<dyn ConnectorFactory> {
    Arc::new(TelegramConnectorFactory)
}
