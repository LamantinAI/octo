use std::{
    net::{Ipv4Addr, SocketAddr},
    path::PathBuf,
};

use serde::Deserialize;

/// Settings for the local Moli process and its private CDP listener.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MoliSettings {
    pub executable: PathBuf,
    pub bind: SocketAddr,
    pub startup_timeout_secs: u64,
}

impl Default for MoliSettings {
    fn default() -> Self {
        Self {
            executable: PathBuf::from("moli"),
            bind: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            startup_timeout_secs: 8,
        }
    }
}

impl MoliSettings {
    pub fn validate(&self) -> Result<(), String> {
        if self.executable.as_os_str().is_empty() {
            return Err("browser_runtime.executable must name a binary".into());
        }
        if !self.bind.ip().is_loopback() {
            return Err("browser_runtime.bind must be a loopback address".into());
        }
        if self.startup_timeout_secs == 0 {
            return Err("browser_runtime.startup_timeout_secs must be positive".into());
        }
        Ok(())
    }
}
