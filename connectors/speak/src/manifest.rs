//! The `speak` connector type, declared by a manifest.

use std::sync::Arc;

use octo_core::{Connector, ConnectorFactory, ConnectorId, FactoryContext};
use octo_openai_auth::SubscriptionAuth;

use crate::{SpeakConnector, DEFAULT_VOICE, VOICES};

/// The `speak` connector type: `[connector] type = "speak"`, with an optional default
/// `voice`. Holds the assembly's shared [`SubscriptionAuth`]; without a token store on disk
/// it refuses to build, so a host with no subscription never offers a tool that can only fail.
pub(crate) struct SpeakFactory {
    auth: Arc<SubscriptionAuth>,
}

impl ConnectorFactory for SpeakFactory {
    fn type_name(&self) -> &str {
        "speak"
    }

    fn create(
        &self,
        id: ConnectorId,
        config: &toml::Value,
        _ctx: FactoryContext<'_>,
    ) -> Result<Arc<dyn Connector>, Box<dyn std::error::Error + Send + Sync>> {
        let table = config.get("connector").ok_or("speak: manifest has no [connector] table")?;
        if !self.auth.path().exists() {
            return Err(format!(
                "speak: no subscription token at {} — sign in, or remove this manifest",
                self.auth.path().display()
            )
            .into());
        }
        let voice = table.get("voice").and_then(|v| v.as_str()).unwrap_or(DEFAULT_VOICE);
        if !VOICES.contains(&voice) {
            return Err(format!("speak: unknown voice {voice:?}; use one of {VOICES:?}").into());
        }
        Ok(SpeakConnector::with_voice(id.as_str(), self.auth.clone(), None, voice))
    }
}

/// The factory to register as connector type `speak`, sharing `auth`.
pub fn factory(auth: Arc<SubscriptionAuth>) -> Arc<dyn ConnectorFactory> {
    Arc::new(SpeakFactory { auth })
}
