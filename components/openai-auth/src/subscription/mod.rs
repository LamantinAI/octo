use std::{
    future::Future,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use tokio::sync::Mutex;

use crate::{
    AuthDotJson, AuthError, DEFAULT_LOGIN_HINT, Result, Subscription, Tokens, ensure_fresh,
    force_refresh, subscription_from, token_endpoint,
};

/// Reuse a forced refresh that already answered a concurrent rejection.
const FORCE_DEDUPE: Duration = Duration::from_secs(30);

/// A shared, refresh-serialised handle over the subscription token store — the one way
/// in. Every refresh happens under a lock, so it is SINGLE-OWNER: safe to share behind an
/// `Arc` across the cogitator's LLM backend and the voice connectors, because two
/// concurrent callers can never both POST a refresh and invalidate each other's token —
/// the second waits, re-reads the store the first has just refreshed, and returns it
/// without a second POST. The same holds for two callers that hit one revocation (a 401)
/// at once: the second reuses the first's forced refresh.
pub struct SubscriptionAuth {
    path: PathBuf,
    /// The sign-in command errors point at (e.g. an assembly's own `albert login`).
    login_hint: String,
    /// Serialises refreshes; holds when the last forced refresh completed.
    last_forced: Mutex<Option<Instant>>,
}

impl SubscriptionAuth {
    /// A handle over the codex-style `auth.json` at `path`.
    pub fn new(path: PathBuf) -> Self {
        let _ = token_endpoint();
        Self {
            path,
            login_hint: DEFAULT_LOGIN_HINT.into(),
            last_forced: Mutex::new(None),
        }
    }

    /// Name the sign-in command errors point at (default: `codex login`).
    pub fn with_login_hint(mut self, hint: impl Into<String>) -> Self {
        self.login_hint = hint.into();
        self
    }

    /// The token store this handle reads and refreshes.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// A fresh [`Subscription`], refreshing the access token first if it is near expiry.
    pub async fn fresh(&self) -> Result<Subscription> {
        let _serialise = self.last_forced.lock().await;
        ensure_fresh(&self.path, &self.login_hint).await
    }

    /// Install credentials from a completed login, coordinating with every refresh
    /// on this handle. Validation happens before replacing the existing store; a
    /// failed write leaves the forced-refresh deduplication state unchanged.
    /// Share this handle across all credential writers for the same store.
    pub async fn install(&self, tokens: Tokens) -> Result<Subscription> {
        if tokens.access_token.trim().is_empty() || tokens.refresh_token.trim().is_empty() {
            return Err(AuthError(
                "login requires access_token and refresh_token".into(),
            ));
        }
        let sub = subscription_from(&tokens)?;
        if sub.account_id.trim().is_empty() {
            return Err(AuthError("login requires an account id".into()));
        }
        let mut last_forced = self.last_forced.lock().await;
        AuthDotJson::new(tokens).save(&self.path)?;
        *last_forced = None;
        Ok(sub)
    }

    /// Force a refresh now — the escape hatch for a live 401 the JWT `exp` still calls
    /// valid (revocation). Serialised the same way, so it can't race a concurrent `fresh`;
    /// a forced refresh that completed moments ago is reused rather than repeated.
    pub async fn force_refresh(&self) -> Result<Subscription> {
        self.force_refresh_using(|| force_refresh(&self.path, &self.login_hint))
            .await
    }

    async fn force_refresh_using<F, Fut>(&self, refresh: F) -> Result<Subscription>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Subscription>>,
    {
        let mut last_forced = self.last_forced.lock().await;
        if last_forced.is_some_and(|at| at.elapsed() < FORCE_DEDUPE) {
            return ensure_fresh(&self.path, &self.login_hint).await;
        }
        let sub = refresh().await?;
        *last_forced = Some(Instant::now());
        Ok(sub)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env::temp_dir,
        fs::{read, remove_file},
        future::{Future, poll_fn},
        path::PathBuf,
        pin::Pin,
        process::id,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        task::Poll,
        time::{Duration, Instant},
    };

    use tokio::{spawn, sync::oneshot, time::timeout};

    use super::SubscriptionAuth;
    use crate::{AuthDotJson, Tokens, subscription_from};

    static NEXT_FILE: AtomicU64 = AtomicU64::new(0);

    struct Store(PathBuf);

    impl Store {
        fn new() -> Self {
            Self(temp_dir().join(format!(
                "subscription-install-{}-{}.json",
                id(),
                NEXT_FILE.fetch_add(1, Ordering::Relaxed),
            )))
        }
    }

    impl Drop for Store {
        fn drop(&mut self) {
            let _ = remove_file(&self.0);
        }
    }

    fn tokens(account: &str) -> Tokens {
        Tokens {
            id_token: String::new(),
            access_token: format!("synthetic-access-{account}"),
            refresh_token: format!("synthetic-refresh-{account}"),
            account_id: account.into(),
        }
    }

    #[tokio::test]
    async fn install_returns_and_persists_usable_credentials() {
        let store = Store::new();
        let auth = SubscriptionAuth::new(store.0.clone());
        let sub = auth.install(tokens("new")).await.expect("install");
        assert_eq!(sub.account_id, "new");
        assert_eq!(auth.fresh().await.expect("fresh").account_id, "new");
        assert_eq!(
            AuthDotJson::load(&store.0).expect("load").tokens.account_id,
            "new"
        );
    }

    #[tokio::test]
    async fn malformed_install_preserves_store_and_dedupe() {
        let store = Store::new();
        let auth = SubscriptionAuth::new(store.0.clone());
        auth.install(tokens("old")).await.expect("initial install");
        *auth.last_forced.lock().await = Some(Instant::now());
        let before = read(&store.0).expect("read");
        for field in ["access", "refresh", "account"] {
            let mut invalid = tokens("new");
            match field {
                "access" => invalid.access_token = " ".into(),
                "refresh" => invalid.refresh_token.clear(),
                _ => invalid.account_id = " ".into(),
            }
            assert!(auth.install(invalid).await.is_err());
            assert_eq!(read(&store.0).expect("read"), before);
            assert!(auth.last_forced.lock().await.is_some());
        }
    }

    #[tokio::test]
    async fn successful_install_resets_forced_refresh_dedupe() {
        let store = Store::new();
        let auth = SubscriptionAuth::new(store.0.clone());
        *auth.last_forced.lock().await = Some(Instant::now());
        auth.install(tokens("new")).await.expect("install");
        assert!(auth.last_forced.lock().await.is_none());
        let sub = auth
            .force_refresh_using(|| async { subscription_from(&tokens("refreshed")) })
            .await
            .expect("refresh must run after install");
        assert_eq!(sub.account_id, "refreshed");
    }

    #[tokio::test]
    async fn failed_save_preserves_forced_refresh_dedupe() {
        let store = Store::new();
        let auth = SubscriptionAuth::new(store.0.join("missing-parent.json"));
        *auth.last_forced.lock().await = Some(Instant::now());
        assert!(auth.install(tokens("new")).await.is_err());
        assert!(auth.last_forced.lock().await.is_some());
    }

    #[tokio::test]
    async fn older_in_flight_refresh_cannot_overwrite_install() {
        timeout(Duration::from_secs(2), async {
            let store = Store::new();
            let auth = Arc::new(SubscriptionAuth::new(store.0.clone()));
            auth.install(tokens("old")).await.expect("initial install");
            let (loaded_tx, loaded_rx) = oneshot::channel();
            let (release_tx, release_rx) = oneshot::channel();
            let refreshing = Arc::clone(&auth);
            let task = spawn(async move {
                refreshing
                    .force_refresh_using(|| async {
                        let mut old =
                            AuthDotJson::load(refreshing.path()).expect("load old account");
                        loaded_tx.send(()).expect("signal loaded");
                        release_rx.await.expect("release refresh");
                        old.tokens.access_token = "synthetic-refreshed-old".into();
                        old.save(refreshing.path()).expect("save old refresh");
                        subscription_from(&old.tokens)
                    })
                    .await
            });
            loaded_rx.await.expect("refresh has loaded old credentials");
            let mut install = Box::pin(auth.install(tokens("new")));
            // Poll once while the refresh owns the mutex, proving installation is
            // queued behind the older read/network/write transaction.
            poll_fn(|cx| {
                assert!(matches!(Pin::new(&mut install).poll(cx), Poll::Pending));
                Poll::Ready(())
            })
            .await;
            release_tx.send(()).expect("release");
            task.await.expect("join").expect("refresh");
            assert_eq!(install.await.expect("install").account_id, "new");
            assert_eq!(auth.fresh().await.expect("fresh").account_id, "new");
            let persisted = AuthDotJson::load(auth.path()).expect("persisted credentials");
            assert_eq!(persisted.tokens.account_id, "new");
            assert_eq!(persisted.tokens.refresh_token, "synthetic-refresh-new");
        })
        .await
        .expect("contention test must complete");
    }
}
