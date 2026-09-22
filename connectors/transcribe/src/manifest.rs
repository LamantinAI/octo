//! The `transcribe` connector type, declared by a manifest: `[connector] type =
//! "transcribe"`, with optional `language`, `chunk_secs` and `parallel_uploads`.

use std::sync::Arc;

use octo_core::{Connector, ConnectorFactory, ConnectorId, FactoryContext};
use octo_openai_auth::SubscriptionAuth;

use crate::{chunk, TranscribeConnector};

/// Manifest settings; the defaults are what a connector built in code gets.
#[derive(Clone, Debug, PartialEq)]
pub struct Settings {
    /// Language hint a call falls back to (`None` -> the endpoint detects it).
    pub language: Option<String>,
    /// Target chunk length for a long recording, in seconds.
    pub chunk_secs: f64,
    /// Chunks uploaded at once.
    pub parallel_uploads: usize,
}

impl Default for Settings {
    fn default() -> Self {
        Self { language: None, chunk_secs: chunk::TARGET_SECS, parallel_uploads: chunk::PARALLEL_UPLOADS }
    }
}

impl Settings {
    /// Read and validate the settings from a `[connector]` table.
    pub(crate) fn from_table(table: &toml::Value) -> Result<Self, String> {
        let mut s = Self::default();
        if let Some(lang) = table.get("language").and_then(|v| v.as_str()) {
            s.language = Some(lang.to_string());
        }
        if let Some(secs) = table.get("chunk_secs") {
            let secs = secs.as_float().or_else(|| secs.as_integer().map(|i| i as f64)).ok_or("chunk_secs must be a number")?;
            if !(60.0..=chunk::HARD_LIMIT_SECS).contains(&secs) {
                return Err(format!("chunk_secs must be 60..={}", chunk::HARD_LIMIT_SECS));
            }
            s.chunk_secs = secs;
        }
        if let Some(n) = table.get("parallel_uploads") {
            let n = n.as_integer().ok_or("parallel_uploads must be an integer")?;
            if !(1..=8).contains(&n) {
                return Err("parallel_uploads must be 1..=8".into());
            }
            s.parallel_uploads = n as usize;
        }
        Ok(s)
    }
}

/// The `transcribe` connector type: `[connector] type = "transcribe"`, with optional
/// `language`, `chunk_secs` and `parallel_uploads`. Holds the assembly's shared
/// [`SubscriptionAuth`]; without a token store on disk it refuses to build, so a host with
/// no subscription never offers a tool that can only fail.
struct TranscribeFactory {
    auth: Arc<SubscriptionAuth>,
}

impl ConnectorFactory for TranscribeFactory {
    fn type_name(&self) -> &str {
        "transcribe"
    }

    fn create(
        &self,
        id: ConnectorId,
        config: &toml::Value,
        _ctx: FactoryContext<'_>,
    ) -> Result<Arc<dyn Connector>, Box<dyn std::error::Error + Send + Sync>> {
        let table = config.get("connector").ok_or("transcribe: manifest has no [connector] table")?;
        if !self.auth.path().exists() {
            return Err(format!(
                "transcribe: no subscription token at {} — sign in, or remove this manifest",
                self.auth.path().display()
            )
            .into());
        }
        let settings = Settings::from_table(table).map_err(|e| format!("transcribe: {e}"))?;
        Ok(TranscribeConnector::with_settings(id.as_str(), self.auth.clone(), None, settings))
    }
}

/// The factory to register as connector type `transcribe`, sharing `auth`.
pub fn factory(auth: Arc<SubscriptionAuth>) -> Arc<dyn ConnectorFactory> {
    Arc::new(TranscribeFactory { auth })
}

#[cfg(test)]
mod tests {
    use super::Settings;

    #[test]
    fn manifest_settings_are_read_and_bounded() {
        let ok: toml::Value = toml::from_str("language = \"ru\"\nchunk_secs = 240\nparallel_uploads = 2").unwrap();
        let s = Settings::from_table(&ok).unwrap();
        assert_eq!((s.language.as_deref(), s.chunk_secs, s.parallel_uploads), (Some("ru"), 240.0, 2));
        assert_eq!(Settings::from_table(&toml::Value::Table(Default::default())).unwrap(), Settings::default());
        let too_long: toml::Value = toml::from_str("chunk_secs = 2000").unwrap();
        assert!(Settings::from_table(&too_long).is_err());
        let too_many: toml::Value = toml::from_str("parallel_uploads = 20").unwrap();
        assert!(Settings::from_table(&too_many).is_err());
    }
}
