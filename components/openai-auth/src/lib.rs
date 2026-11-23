//! OpenAI ChatGPT-**subscription** credential component: a codex-compatible `auth.json`
//! token store with in-place refresh, and a shared, refresh-serialised
//! [`SubscriptionAuth`] handle. Provider-specific (OpenAI / Codex) and deliberately
//! OUTSIDE `octo-core`, so the runtime stays generic — connectors and assemblies that
//! speak the ChatGPT subscription build on it.
//!
//! The store is codex-compatible: `codex login` (and a host's own login flow) write the
//! same shape, and this keeps the access token fresh in place. [`ensure_fresh`] loads the
//! store and, only when the access token is at/near expiry, refreshes it via the OAuth
//! token endpoint and writes the new tokens back; the bearer + account id then flow into
//! the Codex request headers. When the server rejects a token that `exp` still calls valid
//! (revocation), the caller escalates to [`SubscriptionAuth::force_refresh`]. The handle is
//! the only way in: it serialises every refresh behind a lock, so the refresh stays
//! single-owner when one `Arc` is shared across the LLM path and the voice connectors.

mod refresh;
mod storage;
mod subscription;

use std::{fs::read_to_string, path::Path};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, from_slice, from_str, to_string_pretty};
use tracing::{info, warn};

pub use self::subscription::SubscriptionAuth;
use self::{refresh::refresh, storage::write_private};

/// The crate's error — a human-readable message. A token store either yields a usable
/// [`Subscription`] or an error a person can act on (usually: run the login flow).
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct AuthError(pub String);

/// Result over [`AuthError`].
pub type Result<T> = std::result::Result<T, AuthError>;

/// The public first-party Codex OAuth client (shared by `codex` and any assembly's own
/// login flow).
pub const CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
/// The OAuth token endpoint (code exchange + refresh).
pub const TOKEN_URL: &str = "https://auth.openai.com/oauth/token";
/// Refresh once the access token has this little life left (or is already expired).
const REFRESH_WINDOW_SECS: i64 = 300;
/// The sign-in command named in errors unless the assembly supplies its own.
const DEFAULT_LOGIN_HINT: &str = "codex login";

/// The runtime view a Codex request needs: the OAuth access token (-> the
/// `Authorization: Bearer` header) and the account id (-> the mandatory
/// `ChatGPT-Account-ID` header). `plan` is informational (logged at startup).
#[derive(Clone)]
pub struct Subscription {
    pub access_token: String,
    pub account_id: String,
    pub plan: Option<String>,
}

/// The on-disk store — codex-compatible. Any fields codex writes that we don't model
/// (`auth_mode`, `OPENAI_API_KEY`, …) are captured in `extra` and written back
/// untouched, so refreshing in place doesn't clobber codex's own bookkeeping.
#[derive(Serialize, Deserialize)]
pub struct AuthDotJson {
    pub tokens: Tokens,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_refresh: Option<String>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Tokens {
    #[serde(default)]
    pub id_token: String,
    pub access_token: String,
    #[serde(default)]
    pub refresh_token: String,
    #[serde(default)]
    pub account_id: String,
}

impl AuthDotJson {
    /// A fresh store from a just-completed login (no `extra`, stamped `last_refresh`).
    pub fn new(tokens: Tokens) -> Self {
        Self {
            tokens,
            last_refresh: Some(now_rfc3339()),
            extra: Map::new(),
        }
    }

    fn load(path: &Path) -> Result<Self> {
        let text =
            read_to_string(path).map_err(|e| AuthError(format!("read {}: {e}", path.display())))?;
        from_str(&text).map_err(|e| AuthError(format!("parse {}: {e}", path.display())))
    }

    /// Write the store at `0600` — it holds password-equivalent tokens. The mode is
    /// established *before* any token bytes hit disk (no world-readable window), on
    /// both new and pre-existing files.
    pub fn save(&self, path: &Path) -> Result<()> {
        let text =
            to_string_pretty(self).map_err(|e| AuthError(format!("serialize auth.json: {e}")))?;
        write_private(path, text.as_bytes())
            .map_err(|e| AuthError(format!("write {}: {e}", path.display())))
    }
}

/// Load the subscription material, refreshing the access token first if it is at or
/// near expiry. Errors — clearly, pointing at the login command — when the store is
/// missing/empty or a needed refresh can't be done.
async fn ensure_fresh(path: &Path, login: &str) -> Result<Subscription> {
    let mut auth = AuthDotJson::load(path)?;

    if needs_refresh(&auth.tokens.access_token) {
        if auth.tokens.refresh_token.is_empty() {
            return Err(AuthError(format!(
                "access token in {} is expiring and there is no refresh_token; \
                 run `{login}`",
                path.display()
            )));
        }
        info!("refreshing subscription access token");
        match refresh(&auth.tokens.refresh_token, login).await {
            Ok(refreshed) => {
                apply_refresh(&mut auth.tokens, refreshed);
                auth.last_refresh = Some(now_rfc3339());
                auth.save(path)?;
            }
            // We refresh a little before expiry; a transient failure (DNS blip, brief
            // outage) shouldn't fail the turn while the current token is still valid.
            // Only hard-fail once it has actually expired.
            Err(e) if is_expired(&auth.tokens.access_token) => return Err(e),
            Err(e) => warn!(error = %e, "token refresh failed; using the still-valid access token"),
        }
    }

    subscription_from(&auth.tokens)
}

/// Refresh NOW, ignoring what the JWT `exp` claims. The server can revoke an access
/// token ahead of its stated expiry, and [`ensure_fresh`] trusts `exp` — so it would
/// keep serving the revoked token forever. This is the escape hatch for a caller
/// that has just watched the token bounce off the server. A rejected refresh errors
/// through [`refresh`], which already points at the login command.
async fn force_refresh(path: &Path, login: &str) -> Result<Subscription> {
    let mut auth = AuthDotJson::load(path)?;
    if auth.tokens.refresh_token.is_empty() {
        return Err(AuthError(format!(
            "the server no longer accepts the access token in {} and there is no \
             refresh_token; run `{login}`",
            path.display()
        )));
    }
    info!("server rejected the access token ahead of its exp; forcing a refresh");
    let rejected = auth.tokens.access_token.clone();
    let refreshed = refresh(&auth.tokens.refresh_token, login).await?;
    apply_refresh(&mut auth.tokens, refreshed);
    auth.last_refresh = Some(now_rfc3339());
    auth.save(path)?;
    // A refresh response can rotate the refresh/id token while omitting a new access
    // token; `apply_refresh` then keeps the OLD one — i.e. the very token the server
    // just rejected. Persist whatever the server DID rotate (saved above), but don't
    // hand the caller a token we already know is dead: force a re-login rather than
    // let it bounce off the server again.
    if auth.tokens.access_token == rejected {
        return Err(AuthError(format!(
            "the token endpoint did not reissue the access token for {}; it is still \
             the one the server rejected — run `{login}`",
            path.display()
        )));
    }
    subscription_from(&auth.tokens)
}

/// True when the access token expires within [`REFRESH_WINDOW_SECS`] (or already
/// has). If `exp` can't be read we do NOT force a refresh — a live 401 will surface
/// the real problem rather than us guessing.
fn needs_refresh(access_token: &str) -> bool {
    match jwt_claims(access_token)
        .as_ref()
        .and_then(|c| c.get("exp"))
        .and_then(Value::as_i64)
    {
        Some(exp) => exp - Utc::now().timestamp() <= REFRESH_WINDOW_SECS,
        None => false,
    }
}

/// True once the access token's `exp` is in the past. An unreadable `exp` counts as
/// not-expired — the same lenient stance as [`needs_refresh`], letting a live 401
/// surface the real problem rather than us guessing.
fn is_expired(access_token: &str) -> bool {
    matches!(
        jwt_claims(access_token).as_ref().and_then(|c| c.get("exp")).and_then(Value::as_i64),
        Some(exp) if exp <= Utc::now().timestamp()
    )
}

/// The `access_token` -> `Authorization`, `account_id` (stored, else decoded from
/// the token) -> `ChatGPT-Account-ID`.
fn subscription_from(tokens: &Tokens) -> Result<Subscription> {
    if tokens.access_token.is_empty() {
        return Err(AuthError("no access_token in the token store".into()));
    }
    let account_id = if tokens.account_id.is_empty() {
        account_id_from_jwt(&tokens.access_token)
            .ok_or_else(|| AuthError("no account id in the token store or token".into()))?
    } else {
        tokens.account_id.clone()
    };
    Ok(Subscription {
        access_token: tokens.access_token.clone(),
        account_id,
        plan: plan_from_jwt(&tokens.access_token),
    })
}

/// Local test builds capture their refresh destination once; normal builds always
/// use the public provider endpoint.
fn token_endpoint() -> &'static str {
    #[cfg(feature = "local-e2e")]
    {
        use std::{env::var, sync::OnceLock};
        static ENDPOINT: OnceLock<String> = OnceLock::new();
        return ENDPOINT.get_or_init(|| match var("ALBERT_E2E_AUTH_INTERNAL_ORIGIN") {
            Ok(origin) => format!("{}/oauth/token", origin.trim_end_matches('/')),
            Err(_) => TOKEN_URL.into(),
        });
    }
    #[cfg(not(feature = "local-e2e"))]
    TOKEN_URL
}

#[derive(Deserialize)]
struct RefreshResponse {
    access_token: Option<String>,
    refresh_token: Option<String>,
    id_token: Option<String>,
}

/// Apply a refresh response in place, preserving any field the server didn't re-issue
/// and re-deriving the account id from a fresh id_token when present.
fn apply_refresh(tokens: &mut Tokens, r: RefreshResponse) {
    if let Some(access) = r.access_token {
        tokens.access_token = access;
    }
    if let Some(refresh) = r.refresh_token {
        tokens.refresh_token = refresh;
    }
    if let Some(id) = r.id_token {
        if let Some(account) = account_id_from_jwt(&id) {
            tokens.account_id = account;
        }
        tokens.id_token = id;
    }
}

// ── JWT claim reading (best-effort; never used for authorization) ────────────

/// Decode a JWT's payload (the middle segment) into its claims object. This is NOT
/// verification — we only read `exp`/account/plan for refresh timing and UX.
fn jwt_claims(jwt: &str) -> Option<Value> {
    let payload = jwt.split('.').nth(1)?;
    let bytes = URL_SAFE_NO_PAD.decode(payload).ok()?;
    from_slice(&bytes).ok()
}

/// The ChatGPT account id from the OpenAI auth claim namespace.
pub fn account_id_from_jwt(jwt: &str) -> Option<String> {
    jwt_claims(jwt)?
        .get("https://api.openai.com/auth")?
        .get("chatgpt_account_id")?
        .as_str()
        .map(str::to_owned)
}

/// The ChatGPT plan (`plus` / `pro` / `team` / …) from the OpenAI auth claim.
pub fn plan_from_jwt(jwt: &str) -> Option<String> {
    jwt_claims(jwt)?
        .get("https://api.openai.com/auth")?
        .get("chatgpt_plan_type")?
        .as_str()
        .map(str::to_owned)
}

fn now_rfc3339() -> String {
    Utc::now().to_rfc3339()
}

#[cfg(test)]
mod tests {
    use super::{
        AuthDotJson, RefreshResponse, SubscriptionAuth, Tokens, account_id_from_jwt, apply_refresh,
        jwt_claims, plan_from_jwt,
    };

    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    use serde_json::{Value, json};

    /// Assemble a `header.payload.sig` JWT with the given payload (signature is a
    /// throwaway — `jwt_claims` never verifies it).
    fn jwt(payload: &Value) -> String {
        let seg = URL_SAFE_NO_PAD.encode(payload.to_string());
        format!("eyJ.{seg}.sig")
    }

    #[test]
    fn reads_exp_plan_and_account() {
        let token = jwt(&json!({
            "exp": 9_999_999_999_i64,
            "https://api.openai.com/auth": {
                "chatgpt_plan_type": "plus",
                "chatgpt_account_id": "acc-123",
            },
        }));
        let claims = jwt_claims(&token).expect("decodable payload");
        assert_eq!(
            claims.get("exp").and_then(Value::as_i64),
            Some(9_999_999_999)
        );
        assert_eq!(plan_from_jwt(&token).as_deref(), Some("plus"));
        assert_eq!(account_id_from_jwt(&token).as_deref(), Some("acc-123"));
    }

    #[test]
    fn tolerates_non_jwt() {
        assert!(jwt_claims("not-a-jwt").is_none());
        assert!(plan_from_jwt("").is_none());
        assert!(account_id_from_jwt("x.y").is_none());
    }

    fn stored_tokens() -> Tokens {
        Tokens {
            id_token: "old-id".into(),
            access_token: "old-access".into(),
            refresh_token: "old-refresh".into(),
            account_id: "acc-old".into(),
        }
    }

    /// An access-token-only refresh response (the common case) must not clobber the
    /// fields the server didn't re-issue — losing the refresh_token here would turn
    /// the NEXT refresh into a forced re-login.
    #[test]
    fn apply_refresh_keeps_what_the_server_did_not_reissue() {
        let mut tokens = stored_tokens();
        apply_refresh(
            &mut tokens,
            RefreshResponse {
                access_token: Some("new-access".into()),
                refresh_token: None,
                id_token: None,
            },
        );
        assert_eq!(tokens.access_token, "new-access");
        assert_eq!(tokens.refresh_token, "old-refresh");
        assert_eq!(tokens.id_token, "old-id");
        assert_eq!(tokens.account_id, "acc-old");
    }

    #[test]
    fn apply_refresh_rederives_account_from_a_fresh_id_token() {
        let id = jwt(&json!({
            "https://api.openai.com/auth": { "chatgpt_account_id": "acc-new" },
        }));
        let mut tokens = stored_tokens();
        apply_refresh(
            &mut tokens,
            RefreshResponse {
                access_token: None,
                refresh_token: Some("new-refresh".into()),
                id_token: Some(id.clone()),
            },
        );
        assert_eq!(tokens.account_id, "acc-new");
        assert_eq!(tokens.id_token, id);
        assert_eq!(tokens.refresh_token, "new-refresh");
        assert_eq!(tokens.access_token, "old-access");
    }

    /// The force path exists for a live 401 despite an unexpired JWT — so it must
    /// run even when `exp` says the token is fine, and without a refresh_token it
    /// must point the user at re-login (the assembly's own command) rather than 401-looping.
    #[tokio::test]
    async fn force_refresh_without_refresh_token_points_at_login() {
        use std::fs::remove_file;

        let path =
            std::env::temp_dir().join(format!("octo_auth_force_{}.json", std::process::id()));
        AuthDotJson::new(Tokens {
            id_token: String::new(),
            // exp far in the future: irrelevant to the force path by design.
            access_token: jwt(&json!({ "exp": 9_999_999_999_i64 })),
            refresh_token: String::new(),
            account_id: "acc".into(),
        })
        .save(&path)
        .expect("save");

        // No expect_err: Subscription deliberately has no Debug (it holds the token).
        let auth = SubscriptionAuth::new(path.clone()).with_login_hint("albert login");
        let err = match auth.force_refresh().await {
            Ok(_) => panic!("no refresh_token must fail"),
            Err(e) => e,
        };
        let _ = remove_file(&path);
        let msg = err.to_string();
        assert!(
            msg.contains("albert login"),
            "must point at `albert login`, got: {msg}"
        );
    }

    /// The token store lands at `0600`, even when overwriting a pre-existing
    /// world-readable file (the tokens must never be readable at a looser mode).
    #[cfg(unix)]
    #[test]
    fn save_forces_0600() {
        use std::{
            fs::{metadata, remove_file, set_permissions, write},
            os::unix::fs::PermissionsExt,
        };

        let path = std::env::temp_dir().join(format!("octo_auth_test_{}.json", std::process::id()));
        write(&path, b"{}").unwrap();
        set_permissions(&path, PermissionsExt::from_mode(0o644)).unwrap();

        let store = AuthDotJson::new(Tokens {
            id_token: String::new(),
            access_token: "access".into(),
            refresh_token: "refresh".into(),
            account_id: "acc".into(),
        });
        store.save(&path).expect("save");

        let mode = metadata(&path).unwrap().permissions().mode() & 0o777;
        let tmp_left = super::storage::temp_sibling(&path).exists();
        let _ = remove_file(&path);
        assert_eq!(mode, 0o600, "auth.json must be 0600, got {mode:o}");
        assert!(
            !tmp_left,
            "the atomic write must not leave its temp file behind"
        );
    }

    /// The shared provider delegates to the store and is Send + Sync, so one `Arc` can
    /// back the LLM path and the voice connectors. A non-expiring token needs no refresh,
    /// so this touches no network.
    #[tokio::test]
    async fn shared_auth_returns_a_token_without_refresh() {
        use std::fs::remove_file;

        let path =
            std::env::temp_dir().join(format!("octo_shared_auth_{}.json", std::process::id()));
        AuthDotJson::new(Tokens {
            id_token: String::new(),
            access_token: jwt(&json!({
                "exp": 9_999_999_999_i64,
                "https://api.openai.com/auth": { "chatgpt_account_id": "acc-1" },
            })),
            refresh_token: "r".into(),
            account_id: "acc-1".into(),
        })
        .save(&path)
        .expect("save");

        let auth = SubscriptionAuth::new(path.clone());
        let sub = auth
            .fresh()
            .await
            .expect("a non-expiring token needs no refresh");
        assert_eq!(sub.account_id, "acc-1");
        let _ = remove_file(&path);

        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<SubscriptionAuth>();
    }

    /// Live check that the refresh request reaches the token endpoint and a bad
    /// token is handled as a clean rejection (not a TLS/connection failure). Uses a
    /// throwaway bogus token, so it never touches a real refresh token. Ignored by
    /// default: `cargo test -p octo-openai-auth refresh_rejects -- --ignored --nocapture`.
    #[tokio::test]
    #[ignore = "hits auth.openai.com; run with --ignored"]
    async fn refresh_rejects_bad_token() {
        let result = super::refresh("definitely-not-a-valid-refresh-token", "codex login").await;
        assert!(result.is_err(), "a bogus refresh token must be rejected");
        let msg = match result {
            Err(error) => error.to_string(),
            Ok(_) => panic!("a bogus refresh token must be rejected"),
        };
        assert!(
            msg.contains("rejected"),
            "expected a clean HTTP rejection, got: {msg}"
        );
    }
}
