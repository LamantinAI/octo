use std::{collections::BTreeMap, path::Path};

use mcp_http::Url;
use serde::{Deserialize, Serialize};

use crate::{McpError, Result};

#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "transport", rename_all = "snake_case", deny_unknown_fields)]
pub enum ServerConfig {
    Http {
        url: String,
        #[serde(default)]
        token_env: Option<String>,
    },
    Stdio {
        command: String,
        #[serde(default)]
        args: Vec<String>,
        #[serde(default)]
        env: BTreeMap<String, String>,
        #[serde(default = "baseline_environment")]
        inherited_env: Vec<String>,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClientLimits {
    pub connect_timeout_ms: u64,
    pub discovery_timeout_ms: u64,
    pub call_timeout_ms: u64,
    pub shutdown_timeout_ms: u64,
    pub max_tools: usize,
    pub max_discovery_pages: usize,
    pub max_discovery_bytes: usize,
    pub max_argument_bytes: usize,
    pub max_result_bytes: usize,
    pub max_concurrent_calls: usize,
    pub max_retired_connections: usize,
}
impl Default for ClientLimits {
    fn default() -> Self {
        Self {
            connect_timeout_ms: 15000,
            discovery_timeout_ms: 15000,
            call_timeout_ms: 60000,
            shutdown_timeout_ms: 5000,
            max_tools: 512,
            max_discovery_pages: 64,
            max_discovery_bytes: 4 * 1024 * 1024,
            max_argument_bytes: 1024 * 1024,
            max_result_bytes: 32 * 1024 * 1024,
            max_concurrent_calls: 4,
            max_retired_connections: 8,
        }
    }
}
impl ClientLimits {
    pub fn validate(&self) -> Result<()> {
        if [
            self.connect_timeout_ms,
            self.discovery_timeout_ms,
            self.call_timeout_ms,
            self.shutdown_timeout_ms,
        ]
        .iter()
        .any(|value| !(1..=300000).contains(value))
            || !(1..=1024).contains(&self.max_discovery_pages)
            || !(1..=10000).contains(&self.max_tools)
            || !(1024..=64 * 1024 * 1024).contains(&self.max_discovery_bytes)
            || !(1..=32 * 1024 * 1024).contains(&self.max_argument_bytes)
            || !(1024..=128 * 1024 * 1024).contains(&self.max_result_bytes)
            || !(1..=64).contains(&self.max_retired_connections)
            || !(1..=64).contains(&self.max_concurrent_calls)
        {
            return Err(McpError::Configuration);
        }
        Ok(())
    }
}
impl ServerConfig {
    pub fn validate(&self, base: &Path) -> Result<()> {
        if !base.is_absolute() || !base.is_dir() {
            return Err(McpError::Configuration);
        }
        match self {
            Self::Http { url, token_env } => {
                let parsed = Url::parse(url).map_err(|_| McpError::Configuration)?;
                if !matches!(parsed.scheme(), "http" | "https")
                    || parsed.host_str().is_none()
                    || !parsed.username().is_empty()
                    || parsed.password().is_some()
                    || parsed.query().is_some()
                    || parsed.fragment().is_some()
                    || token_env
                        .as_ref()
                        .is_some_and(|name| !credential_name(name))
                {
                    return Err(McpError::Configuration);
                }
            }
            Self::Stdio {
                command,
                args,
                env,
                inherited_env,
            } => {
                if command.trim().is_empty()
                    || command.contains('\0')
                    || args.len() > 256
                    || args.iter().any(|arg| arg.contains('\0'))
                    || env.len() > 64
                    || inherited_env.len() > 64
                    || env
                        .iter()
                        .any(|(key, source)| !credential_name(key) || !credential_name(source))
                    || inherited_env.iter().any(|name| !credential_name(name))
                {
                    return Err(McpError::Configuration);
                }
            }
        }
        Ok(())
    }
}
fn credential_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .chars()
            .enumerate()
            .all(|(i, c)| c.is_ascii_alphabetic() || c == '_' || (i > 0 && c.is_ascii_digit()))
}
fn baseline_environment() -> Vec<String> {
    [
        "PATH",
        "HOME",
        "LANG",
        "TMPDIR",
        "XDG_CONFIG_HOME",
        "XDG_DATA_HOME",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}
