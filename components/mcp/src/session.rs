use std::{
    fs::canonicalize, mem::take, path::Path, sync::Arc, sync::RwLock, sync::atomic::AtomicU64,
    sync::atomic::Ordering, time::Duration,
};

use crate::connection::{ActiveCalls, CallLease};
use rmcp::{service::Peer, service::RoleClient};
use tokio::{select, spawn, sync::Mutex, sync::Semaphore, time::timeout};
use tokio_util::sync::CancellationToken;

use crate::{
    ClientLimits, ConnectionState, Credentials, McpError, Result, ServerConfig, ServerSession,
    ToolSnapshot, discovery::discover, transport::connect,
};

struct RefreshGuard<'a> {
    invalid: &'a AtomicU64,
    epoch: u64,
    complete: bool,
}
impl Drop for RefreshGuard<'_> {
    fn drop(&mut self) {
        if !self.complete {
            self.invalid.fetch_max(self.epoch, Ordering::AcqRel);
        }
    }
}

impl ServerSession {
    pub async fn connect(
        config: ServerConfig,
        base: &Path,
        limits: ClientLimits,
        credentials: Arc<dyn Credentials>,
        lifecycle: CancellationToken,
    ) -> Result<Arc<Self>> {
        limits.validate()?;
        let base = canonicalize(base).map_err(|_| McpError::Configuration)?;
        config.validate(&base)?;
        let shutdown = lifecycle.child_token();
        let session = select! {biased;_=shutdown.cancelled()=>return Err(McpError::Cancelled),session=connect(&config,&base,&limits,credentials.as_ref())=>session?};
        let snapshot = select! {biased;_=shutdown.cancelled()=>return Err(McpError::Cancelled),snapshot=discover(&session,&limits)=>snapshot?};
        let calls = Semaphore::new(limits.max_concurrent_calls);
        Ok(Arc::new(Self {
            config,
            base,
            limits,
            credentials,
            state: Mutex::new(ConnectionState {
                epoch: 1,
                session: Some(session),
                active: Arc::new(ActiveCalls::default()),
            }),
            snapshot: RwLock::new(Arc::new(snapshot)),
            invalid_epoch: AtomicU64::new(0),
            shutdown,
            calls,
            retired: Mutex::new(Vec::new()),
            closing: Mutex::new(Default::default()),
        }))
    }
    pub fn snapshot(&self) -> Arc<ToolSnapshot> {
        self.snapshot.read().unwrap().clone()
    }
    pub fn is_closed(&self) -> bool {
        self.shutdown.is_cancelled()
    }
    pub fn connected_now(&self) -> Option<bool> {
        self.state.try_lock().ok().map(|state| {
            !self.is_closed()
                && state.epoch > self.invalid_epoch.load(Ordering::Acquire)
                && state
                    .session
                    .as_ref()
                    .is_some_and(|session| !session.is_closed())
        })
    }
    pub async fn is_connected(&self) -> bool {
        let state = self.state.lock().await;
        !self.is_closed()
            && state.epoch > self.invalid_epoch.load(Ordering::Acquire)
            && state
                .session
                .as_ref()
                .is_some_and(|session| !session.is_closed())
    }
    pub(crate) async fn peer(&self, expected: &str) -> Result<(Peer<RoleClient>, u64, CallLease)> {
        if self.is_closed() {
            return Err(McpError::Closed);
        }
        let mut state = self.state.lock().await;
        if self.snapshot().revision != expected {
            return Err(McpError::SchemaChanged);
        }
        let reusable = state.epoch > self.invalid_epoch.load(Ordering::Acquire)
            && state
                .session
                .as_ref()
                .is_some_and(|session| !session.is_closed());
        if !reusable {
            self.retire(&mut state).await?;
            let mut session = select! {biased;_=self.shutdown.cancelled()=>return Err(McpError::Closed),session=connect(&self.config,&self.base,&self.limits,self.credentials.as_ref())=>session?};
            let snapshot = select! {biased;_=self.shutdown.cancelled()=>return Err(McpError::Closed),snapshot=discover(&session,&self.limits)=>snapshot?};
            if snapshot.revision != expected {
                let _ = session
                    .close_with_timeout(Duration::from_millis(self.limits.shutdown_timeout_ms))
                    .await;
                return Err(McpError::SchemaChanged);
            }
            state.epoch = state.epoch.checked_add(1).ok_or(McpError::Protocol)?;
            state.session = Some(session);
            state.active = Arc::new(ActiveCalls::default());
        }
        Ok((
            state
                .session
                .as_ref()
                .ok_or(McpError::Closed)?
                .peer()
                .clone(),
            state.epoch,
            state.active.acquire(),
        ))
    }
    /// Explicitly publishes a new tool contract. An uncertain prior invocation
    /// is never replayed as part of discovery or reconnect.
    pub async fn refresh(&self) -> Result<Arc<ToolSnapshot>> {
        if self.is_closed() {
            return Err(McpError::Closed);
        }
        let mut state = self.state.lock().await;
        let stale = state.epoch <= self.invalid_epoch.load(Ordering::Acquire)
            || state
                .session
                .as_ref()
                .is_none_or(|session| session.is_closed());
        if stale {
            self.retire(&mut state).await?;
            let session = select! {biased;_=self.shutdown.cancelled()=>return Err(McpError::Closed),session=connect(&self.config,&self.base,&self.limits,self.credentials.as_ref())=>session?};
            state.epoch = state.epoch.checked_add(1).ok_or(McpError::Protocol)?;
            state.session = Some(session);
            state.active = Arc::new(ActiveCalls::default());
        }
        let mut refresh = RefreshGuard {
            invalid: &self.invalid_epoch,
            epoch: state.epoch,
            complete: false,
        };
        let snapshot = select! {biased;_=self.shutdown.cancelled()=>return Err(McpError::Closed),snapshot=discover(state.session.as_ref().ok_or(McpError::Closed)?,&self.limits)=>snapshot?};
        let snapshot = Arc::new(snapshot);
        *self.snapshot.write().unwrap() = snapshot.clone();
        refresh.complete = true;
        Ok(snapshot)
    }
    pub async fn close(&self) -> Result<()> {
        self.shutdown.cancel();
        let mut closing = self.closing.lock().await;
        if let Some(result) = closing.result {
            return result;
        }
        if closing.started {
            return Err(McpError::ShutdownTimeout);
        }
        closing.started = true;
        let duration = Duration::from_millis(self.limits.shutdown_timeout_ms);
        let current = self.state.lock().await.session.take();
        let mut tasks = take(&mut *self.retired.lock().await);
        if let Some(mut current) = current {
            tasks.push(spawn(async move {
                match current.close_with_timeout(duration).await {
                    Ok(Some(_)) => Ok(()),
                    Ok(None) => Err(McpError::ShutdownTimeout),
                    Err(_) => Err(McpError::Connection),
                }
            }));
        }
        let result = timeout(duration, async {
            for task in tasks {
                task.await.map_err(|_| McpError::Connection)??;
            }
            Ok(())
        })
        .await
        .map_err(|_| McpError::ShutdownTimeout)
        .and_then(|result| result);
        closing.result = Some(result);
        result
    }
}
