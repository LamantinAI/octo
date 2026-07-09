//! Reusable authentication modes for Octo connectors.
//!
//! Two layers:
//! - [`AuthConfig`] — the *config*: a small, deserializable enum via an
//!   `auth = "<mode>"` discriminant (`basic`, `bearer`, `oauth2`, `none`).
//!   Secrets are named env vars, never literals in the manifest.
//! - [`HttpAuth`] — the *runtime*: wraps an [`AuthConfig`], and for `oauth2`
//!   exchanges the refresh token for short-lived access tokens (cached until they
//!   near expiry). Connectors hold an `HttpAuth` and call
//!   [`apply`](HttpAuth::apply) / [`credential`](HttpAuth::credential).
//!
//! Resolution yields a [`Credential`] — an HTTP client turns it into an
//! `Authorization` header, an SMTP client into `AUTH`/`XOAUTH2` — so it's shared
//! by the CalDAV / SMTP / HTTP connectors, and a new provider is a config preset.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("auth: env var `{0}` is not set")]
    MissingSecret(String),

    #[error("auth: oauth2 needs an HttpAuth (token cache); call HttpAuth::credential")]
    OAuth2NeedsRuntime,

    #[error("auth: oauth2 token request failed: {0}")]
    TokenRequest(String),

    #[error("auth: oauth2 token endpoint returned {status}: {body}")]
    TokenRefused { status: u16, body: String },

    #[error("auth: oauth2 token response parse: {0}")]
    TokenParse(String),
}

/// How a connector authenticates. Deserialized from a connector manifest via an
/// `auth = "<mode>"` discriminant, with the mode's fields alongside it:
///
/// ```toml
/// auth = "basic"
/// login = "me@yandex.ru"
/// password_env = "OCTO_YANDEX_APP_PASSWORD"
/// ```
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(tag = "auth", rename_all = "lowercase")]
pub enum AuthConfig {
    /// No authentication.
    #[default]
    None,

    /// HTTP Basic — `Authorization: Basic base64(login:secret)`; also plain SMTP
    /// `AUTH`. The secret (an *app password*, not the account password) is read
    /// from `password_env`.
    Basic { login: String, password_env: String },

    /// A static bearer token read from `token_env` —
    /// `Authorization: Bearer <token>`.
    Bearer { token_env: String },

    /// OAuth2 bearer with refresh (Google et al.): exchange a long-lived refresh
    /// token for short-lived access tokens at `token_url`. Applied via
    /// [`HttpAuth`] (which caches the access token).
    Oauth2 {
        token_url: String,
        client_id: String,
        client_secret_env: String,
        refresh_token_env: String,
    },
}

/// A resolved credential — transport-neutral, so an HTTP client and an SMTP
/// client consume the same thing differently.
#[derive(Debug, Clone)]
pub enum Credential {
    None,
    /// Login + secret (app password): HTTP Basic, or SMTP `AUTH LOGIN`/`PLAIN`.
    Basic { login: String, secret: String },
    /// A bearer access token: HTTP `Authorization: Bearer`, or SMTP `XOAUTH2`.
    Bearer(String),
}

impl AuthConfig {
    /// Resolve the *stateless* modes (`none`/`basic`/`bearer`) from the
    /// environment. `oauth2` errors here — it needs the token cache in
    /// [`HttpAuth`]; use [`HttpAuth::credential`] for any config.
    pub async fn resolve(&self) -> Result<Credential, AuthError> {
        match self {
            AuthConfig::None => Ok(Credential::None),
            AuthConfig::Basic { login, password_env } => Ok(Credential::Basic {
                login: login.clone(),
                secret: env_secret(password_env)?,
            }),
            AuthConfig::Bearer { token_env } => Ok(Credential::Bearer(env_secret(token_env)?)),
            AuthConfig::Oauth2 { .. } => Err(AuthError::OAuth2NeedsRuntime),
        }
    }
}

impl Credential {
    /// The `Authorization` header value for an HTTP request, if any.
    pub fn http_authorization(&self) -> Option<String> {
        match self {
            Credential::None => None,
            Credential::Basic { login, secret } => {
                use base64::engine::general_purpose::STANDARD;
                use base64::Engine;
                let encoded = STANDARD.encode(format!("{login}:{secret}"));
                Some(format!("Basic {encoded}"))
            }
            Credential::Bearer(token) => Some(format!("Bearer {token}")),
        }
    }
}

/// Runtime authentication: an [`AuthConfig`] plus, for `oauth2`, a cached access
/// token and the HTTP client used to refresh it. Cheap to build; hold one per
/// authenticated connector instance.
pub struct HttpAuth {
    config: AuthConfig,
    client: reqwest::Client,
    cache: Mutex<Option<CachedToken>>,
}

struct CachedToken {
    access_token: String,
    expires_at: Instant,
}

/// Refresh a token this long *before* it actually expires (clock skew + latency).
const EXPIRY_LEEWAY: Duration = Duration::from_secs(60);

impl HttpAuth {
    /// Build from an [`AuthConfig`] using a default HTTP client.
    pub fn new(config: AuthConfig) -> Self {
        Self::with_client(config, reqwest::Client::new())
    }

    /// Build sharing a caller-supplied HTTP client (pool reuse; a proxy-free
    /// client in tests).
    pub fn with_client(config: AuthConfig, client: reqwest::Client) -> Self {
        Self { config, client, cache: Mutex::new(None) }
    }

    pub fn config(&self) -> &AuthConfig {
        &self.config
    }

    /// Resolve to a live [`Credential`], refreshing an OAuth2 access token if the
    /// cached one is absent or near expiry.
    pub async fn credential(&self) -> Result<Credential, AuthError> {
        match &self.config {
            AuthConfig::Oauth2 { token_url, client_id, client_secret_env, refresh_token_env } => {
                let token = self
                    .oauth2_token(token_url, client_id, client_secret_env, refresh_token_env)
                    .await?;
                Ok(Credential::Bearer(token))
            }
            stateless => stateless.resolve().await,
        }
    }

    /// Resolve and apply to a `reqwest` request builder (adds `Authorization`, or
    /// nothing for [`AuthConfig::None`]).
    pub async fn apply(
        &self,
        req: reqwest::RequestBuilder,
    ) -> Result<reqwest::RequestBuilder, AuthError> {
        match self.credential().await?.http_authorization() {
            Some(value) => Ok(req.header(reqwest::header::AUTHORIZATION, value)),
            None => Ok(req),
        }
    }

    /// A valid OAuth2 access token — cached, refreshed on demand.
    async fn oauth2_token(
        &self,
        token_url: &str,
        client_id: &str,
        client_secret_env: &str,
        refresh_token_env: &str,
    ) -> Result<String, AuthError> {
        // Fast path: a cached token with time to spare.
        {
            let cache = self.cache.lock().unwrap();
            if let Some(tok) = cache.as_ref() {
                if tok.expires_at > Instant::now() + EXPIRY_LEEWAY {
                    return Ok(tok.access_token.clone());
                }
            }
        }
        // Refresh (no lock held across the await; a concurrent double-refresh is
        // harmless — last write wins).
        let client_secret = env_secret(client_secret_env)?;
        let refresh_token = env_secret(refresh_token_env)?;
        let (access_token, ttl) = self
            .refresh(token_url, client_id, &client_secret, &refresh_token)
            .await?;
        let mut cache = self.cache.lock().unwrap();
        *cache = Some(CachedToken {
            access_token: access_token.clone(),
            expires_at: Instant::now() + ttl,
        });
        Ok(access_token)
    }

    async fn refresh(
        &self,
        token_url: &str,
        client_id: &str,
        client_secret: &str,
        refresh_token: &str,
    ) -> Result<(String, Duration), AuthError> {
        let form = [
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token),
            ("client_id", client_id),
            ("client_secret", client_secret),
        ];
        let resp = self
            .client
            .post(token_url)
            .form(&form)
            .send()
            .await
            .map_err(|e| AuthError::TokenRequest(e.to_string()))?;
        let status = resp.status();
        let body = resp.text().await.map_err(|e| AuthError::TokenRequest(e.to_string()))?;
        if !status.is_success() {
            return Err(AuthError::TokenRefused { status: status.as_u16(), body });
        }
        let parsed: TokenResponse =
            serde_json::from_str(&body).map_err(|e| AuthError::TokenParse(e.to_string()))?;
        let ttl = Duration::from_secs(parsed.expires_in.unwrap_or(3600));
        Ok((parsed.access_token, ttl))
    }
}

impl From<AuthConfig> for HttpAuth {
    fn from(config: AuthConfig) -> Self {
        HttpAuth::new(config)
    }
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    expires_in: Option<u64>,
}

fn env_secret(var: &str) -> Result<String, AuthError> {
    std::env::var(var).map_err(|_| AuthError::MissingSecret(var.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserializes_basic_from_toml() {
        let cfg: AuthConfig = toml::from_str(
            r#"
            auth = "basic"
            login = "me@yandex.ru"
            password_env = "APP_PW"
        "#,
        )
        .unwrap();
        assert!(matches!(cfg, AuthConfig::Basic { .. }));
    }

    #[test]
    fn deserializes_oauth2_from_toml() {
        let cfg: AuthConfig = toml::from_str(
            r#"
            auth = "oauth2"
            token_url = "https://oauth2.googleapis.com/token"
            client_id = "id"
            client_secret_env = "CS"
            refresh_token_env = "RT"
        "#,
        )
        .unwrap();
        assert!(matches!(cfg, AuthConfig::Oauth2 { .. }));
    }

    #[tokio::test]
    async fn basic_resolves_to_base64_header_via_httpauth() {
        unsafe { std::env::set_var("HTTP_AUTH_TEST_PW", "s3cret") };
        let auth = HttpAuth::new(AuthConfig::Basic {
            login: "user".into(),
            password_env: "HTTP_AUTH_TEST_PW".into(),
        });
        let header = auth.credential().await.unwrap().http_authorization().unwrap();
        // base64("user:s3cret") == "dXNlcjpzM2NyZXQ="
        assert_eq!(header, "Basic dXNlcjpzM2NyZXQ=");
    }

    #[tokio::test]
    async fn bearer_resolves_to_bearer_header() {
        unsafe { std::env::set_var("HTTP_AUTH_TEST_TOKEN", "abc123") };
        let auth = HttpAuth::new(AuthConfig::Bearer { token_env: "HTTP_AUTH_TEST_TOKEN".into() });
        let header = auth.credential().await.unwrap().http_authorization().unwrap();
        assert_eq!(header, "Bearer abc123");
    }

    #[tokio::test]
    async fn missing_secret_errors() {
        let auth = HttpAuth::new(AuthConfig::Bearer { token_env: "DEFINITELY_UNSET_XYZ".into() });
        assert!(matches!(auth.credential().await, Err(AuthError::MissingSecret(_))));
    }

    #[test]
    fn none_has_no_header() {
        assert_eq!(Credential::None.http_authorization(), None);
    }

    #[test]
    fn token_response_parses_google_shape() {
        let json = r#"{"access_token":"ya29.a0","expires_in":3599,"token_type":"Bearer","scope":"..."}"#;
        let t: TokenResponse = serde_json::from_str(json).unwrap();
        assert_eq!(t.access_token, "ya29.a0");
        assert_eq!(t.expires_in, Some(3599));
    }

    #[tokio::test]
    async fn oauth2_direct_resolve_errors_without_runtime() {
        let cfg = AuthConfig::Oauth2 {
            token_url: "https://oauth2.googleapis.com/token".into(),
            client_id: "id".into(),
            client_secret_env: "CS".into(),
            refresh_token_env: "RT".into(),
        };
        assert!(matches!(cfg.resolve().await, Err(AuthError::OAuth2NeedsRuntime)));
    }
}
