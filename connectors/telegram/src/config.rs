use std::{
    path::PathBuf,
    sync::{Arc, RwLock},
    time::Duration,
};

use octo_core::{Connector, ConnectorFactory, ConnectorId, FactoryContext};
use serde::Deserialize;

use super::{AclState, DEFAULT_DEBOUNCE, DEFAULT_MAX_WAIT, TelegramConnector};
use crate::source::GroupSettings;
use crate::{Acl, Role};

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
    #[serde(default)]
    admins: Vec<i64>,
    #[serde(default)]
    address_names: Vec<String>,
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
        let mut connector =
            TelegramConnector::build(id.as_str(), token, acl_state, debounce, max_wait, workspace);
        let instance = Arc::get_mut(&mut connector).expect("new connector is uniquely owned");
        instance.groups = GroupSettings {
            admins: cfg.admins,
            address_names: cfg.address_names,
        };
        Ok(connector)
    }
}

/// Convenience factory handle for registration.
pub fn factory() -> Arc<dyn ConnectorFactory> {
    Arc::new(TelegramConnectorFactory)
}
