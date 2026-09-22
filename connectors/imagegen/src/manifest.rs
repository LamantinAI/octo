//! The `imagegen` connector type, declared by a manifest: `[connector] type = "imagegen"`,
//! with optional `size`, `quality` and `background` defaults.

use std::sync::Arc;

use octo_core::{Connector, ConnectorFactory, ConnectorId, FactoryContext};
use octo_openai_auth::SubscriptionAuth;

use crate::{one_of, validate_size, ImagegenConnector, BACKGROUNDS, QUALITIES};

/// Manifest defaults for the optional call settings; `None` leaves it to the endpoint.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Defaults {
    pub size: Option<String>,
    pub quality: Option<String>,
    pub background: Option<String>,
}

impl Defaults {
    /// Read and validate `size` / `quality` / `background` from a `[connector]` table.
    pub(crate) fn from_table(table: &toml::Value) -> Result<Self, String> {
        let get = |key: &str| table.get(key).and_then(|v| v.as_str()).map(str::to_string);
        Ok(Self {
            size: get("size").map(|s| validate_size(&s)).transpose()?,
            quality: get("quality").map(|q| one_of(&q, &QUALITIES, "quality")).transpose()?,
            background: get("background").map(|b| one_of(&b, &BACKGROUNDS, "background")).transpose()?,
        })
    }
}

/// The `imagegen` connector type: `[connector] type = "imagegen"`, with optional `size`,
/// `quality` and `background` defaults. Holds the assembly's shared [`SubscriptionAuth`], so
/// every instance uses the one token owner. Without a token store on disk it refuses to
/// build, so a host with no subscription never offers a tool that can only fail.
struct ImagegenFactory {
    auth: Arc<SubscriptionAuth>,
}

impl ConnectorFactory for ImagegenFactory {
    fn type_name(&self) -> &str {
        "imagegen"
    }

    fn create(
        &self,
        id: ConnectorId,
        config: &toml::Value,
        _ctx: FactoryContext<'_>,
    ) -> Result<Arc<dyn Connector>, Box<dyn std::error::Error + Send + Sync>> {
        let table = config.get("connector").ok_or("imagegen: manifest has no [connector] table")?;
        if !self.auth.path().exists() {
            return Err(format!(
                "imagegen: no subscription token at {} — sign in, or remove this manifest",
                self.auth.path().display()
            )
            .into());
        }
        let defaults = Defaults::from_table(table).map_err(|e| format!("imagegen: {e}"))?;
        Ok(ImagegenConnector::with_defaults(id.as_str(), self.auth.clone(), None, defaults))
    }
}

/// The factory to register as connector type `imagegen`, sharing `auth`.
pub fn factory(auth: Arc<SubscriptionAuth>) -> Arc<dyn ConnectorFactory> {
    Arc::new(ImagegenFactory { auth })
}

#[cfg(test)]
mod tests {
    use super::Defaults;
    use crate::ImageRequest;
    use serde_json::json;

    fn no_files(_: &str) -> Result<Vec<u8>, String> {
        Err("no files in this test".into())
    }

    #[test]
    fn manifest_defaults_fill_what_a_call_omits() {
        let table: toml::Value = toml::from_str("size = \"1536x1024\"\nquality = \"high\"").unwrap();
        let d = Defaults::from_table(&table).unwrap();
        let r = ImageRequest::from_params(&json!({ "prompt": "x", "quality": "low" }), &d, no_files).unwrap();
        let body = r.body();
        assert_eq!(body["size"], "1536x1024"); // from the manifest
        assert_eq!(body["quality"], "low"); // the call wins
        let bad: toml::Value = toml::from_str("size = \"1000x1000\"").unwrap();
        assert!(Defaults::from_table(&bad).is_err());
    }
}
