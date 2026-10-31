use std::time::Duration;

use octo_http_auth::{AuthError, Credential};
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderName, HeaderValue};
use tokio::time::timeout;

use crate::{AuthSpec, HttpConnector, HttpError};

/// Bound token exchange even when the shared HTTP client has no timeout.
const DEFAULT_AUTH_TIMEOUT: Duration = Duration::from_secs(30);

impl HttpConnector {
    pub(super) async fn request_headers(&self) -> Result<HeaderMap, HttpError> {
        let mut headers = self.static_headers();
        let (name, value) = match &self.spec.auth {
            None => return Ok(headers),
            Some(AuthSpec::Header {
                header,
                secret_ref,
                value_prefix,
                ..
            }) => {
                let Some(secret) = self.spec.resolve_secret(secret_ref) else {
                    return Ok(headers);
                };
                let name = HeaderName::from_bytes(header.as_bytes())
                    .map_err(|_| HttpError::local("auth: invalid header name"))?;
                (name, format!("{value_prefix}{secret}"))
            }
            Some(AuthSpec::OAuth2(_)) => {
                let auth = self
                    .auth
                    .as_ref()
                    .ok_or_else(|| HttpError::local("auth: OAuth2 runtime is unavailable"))?;
                let duration = self
                    .spec
                    .timeout_ms
                    .map(Duration::from_millis)
                    .unwrap_or(DEFAULT_AUTH_TIMEOUT);
                let credential = timeout(duration, auth.credential())
                    .await
                    .map_err(|_| HttpError::local("auth: OAuth2 token request timed out"))?
                    .map_err(auth_error)?;
                let Credential::Bearer(token) = credential else {
                    return Err(HttpError::local(
                        "auth: OAuth2 bearer credential is unavailable",
                    ));
                };
                let value = format!("Bearer {token}");
                (AUTHORIZATION, value)
            }
        };
        let mut value = HeaderValue::from_str(&value)
            .map_err(|_| HttpError::local("auth: invalid credential header value"))?;
        value.set_sensitive(true);
        headers.insert(name, value);
        Ok(headers)
    }
}

// Token endpoints can echo secrets in URLs or response bodies. Only expose the
// failure category and status in the connector's error envelope.
fn auth_error(error: AuthError) -> HttpError {
    HttpError::local(match error {
        AuthError::MissingSecret(name) => format!("auth: env var `{name}` is not set"),
        AuthError::TokenRefused { status, .. } => {
            format!("auth: OAuth2 token endpoint returned {status}")
        }
        AuthError::TokenRequest(_) => "auth: OAuth2 token request failed".into(),
        AuthError::TokenParse(_) => "auth: invalid OAuth2 token response".into(),
        AuthError::OAuth2NeedsRuntime => "auth: OAuth2 runtime is unavailable".into(),
    })
}
