//! Resolved mail config: IMAP + SMTP endpoints and credentials. Built from a
//! `type = "mail"` manifest whose values name env vars for the secrets; hosts are
//! required literals (this crate is provider-neutral — no vendor defaults), ports
//! default to the implicit-TLS standards (993/465).

use serde_json::Value;
use toml::Value as Toml;

use crate::error::{MailError, Result};

/// Everything the IMAP/SMTP calls need, secrets already pulled from the env.
#[derive(Debug, Clone)]
pub struct MailConfig {
    pub imap_host: String,
    pub imap_port: u16,
    pub imap_user: String,
    pub imap_pass: String,
    pub mailbox: String,
    pub smtp_host: String,
    pub smtp_port: u16,
    pub smtp_user: String,
    pub smtp_pass: String,
    pub from: String,
}

impl MailConfig {
    /// Parse a `[connector]` manifest table. Keys ending `_env` name an env var
    /// holding the value. `imap_host`/`smtp_host` are REQUIRED literals (no
    /// vendor defaults); ports default to implicit-TLS 993/465, mailbox to INBOX.
    pub(crate) fn from_table(table: &Toml) -> Result<Self> {
        let lit = |key: &str, default: &str| -> String {
            table.get(key).and_then(Toml::as_str).unwrap_or(default).to_string()
        };
        let required = |key: &str| -> Result<String> {
            table
                .get(key)
                .and_then(Toml::as_str)
                .filter(|s| !s.trim().is_empty())
                .map(str::to_string)
                .ok_or_else(|| MailError::Config(format!("manifest missing `{key}`")))
        };
        let port = |key: &str, default: u16| -> Result<u16> {
            match table.get(key) {
                None => Ok(default),
                Some(v) => v
                    .as_integer()
                    .and_then(|i| u16::try_from(i).ok())
                    .ok_or_else(|| MailError::Config(format!("{key} must be a port number"))),
            }
        };
        // A required secret, read from the env var named by `<key>` in the manifest.
        let secret = |key: &str| -> Result<String> {
            let var = table
                .get(key)
                .and_then(Toml::as_str)
                .ok_or_else(|| MailError::Config(format!("manifest missing `{key}`")))?;
            std::env::var(var)
                .map_err(|_| MailError::Config(format!("env var {var} (named by {key}) is not set")))
                .and_then(|s| {
                    if s.trim().is_empty() {
                        Err(MailError::Config(format!("env var {var} is empty")))
                    } else {
                        Ok(s)
                    }
                })
        };
        // An optional secret env: returns None if the manifest key is absent.
        let opt_secret = |key: &str| -> Result<Option<String>> {
            match table.get(key).and_then(Toml::as_str) {
                None => Ok(None),
                Some(var) => std::env::var(var)
                    .map(Some)
                    .map_err(|_| MailError::Config(format!("env var {var} (named by {key}) is not set"))),
            }
        };

        let imap_user = secret("imap_user_env")?;
        let imap_pass = secret("imap_pass_env")?;
        // SMTP creds default to the IMAP ones (most providers share the login);
        // the SMTP host defaults to the IMAP host.
        let imap_host = required("imap_host")?;
        let smtp_user = opt_secret("smtp_user_env")?.unwrap_or_else(|| imap_user.clone());
        let smtp_pass = opt_secret("smtp_pass_env")?.unwrap_or_else(|| imap_pass.clone());
        let from = opt_secret("from_env")?
            .or_else(|| table.get("from").and_then(Toml::as_str).map(String::from))
            .unwrap_or_else(|| imap_user.clone());

        Ok(MailConfig {
            smtp_host: lit("smtp_host", &imap_host),
            imap_host,
            imap_port: port("imap_port", 993)?,
            imap_user,
            imap_pass,
            mailbox: lit("mailbox", "INBOX"),
            smtp_port: port("smtp_port", 465)?,
            smtp_user,
            smtp_pass,
            from,
        })
    }
}

/// Read an optional usize from a command payload.
pub(crate) fn opt_usize(params: &Value, key: &str, default: usize) -> usize {
    params.get(key).and_then(Value::as_u64).map(|n| n as usize).unwrap_or(default)
}
