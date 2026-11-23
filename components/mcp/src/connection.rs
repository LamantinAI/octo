use std::{sync::Arc, sync::atomic::AtomicUsize, sync::atomic::Ordering, time::Duration};

use tokio::{select, spawn, sync::Notify};

use crate::{ConnectionState, McpError, Result, ServerSession};

#[derive(Default)]
pub(crate) struct ActiveCalls {
    count: AtomicUsize,
    changed: Notify,
}
pub(crate) struct CallLease {
    active: Arc<ActiveCalls>,
}
impl ActiveCalls {
    pub fn acquire(self: &Arc<Self>) -> CallLease {
        self.count.fetch_add(1, Ordering::AcqRel);
        CallLease {
            active: self.clone(),
        }
    }
    async fn idle(&self) {
        loop {
            let changed = self.changed.notified();
            if self.count.load(Ordering::Acquire) == 0 {
                return;
            }
            changed.await;
        }
    }
}
impl Drop for CallLease {
    fn drop(&mut self) {
        if self.active.count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.active.changed.notify_waiters();
        }
    }
}
impl ServerSession {
    /// The state lock keeps generation selection and lease acquisition atomic.
    /// Retiring a failed root call must not abort a surviving child RPC.
    pub(crate) async fn retire(&self, state: &mut ConnectionState) -> Result<()> {
        if state.session.is_none() {
            return Ok(());
        }
        let mut retired = self.retired.lock().await;
        retired.retain(|task| !task.is_finished());
        if retired.len() >= self.limits.max_retired_connections {
            return Err(McpError::Connection);
        }
        let mut session = state.session.take().ok_or(McpError::Closed)?;
        let active = state.active.clone();
        let shutdown = self.shutdown.clone();
        let duration = Duration::from_millis(self.limits.shutdown_timeout_ms);
        retired.push(spawn(async move {
            select! {biased;_=shutdown.cancelled()=>{},_=active.idle()=>{}}
            match session.close_with_timeout(duration).await {
                Ok(Some(_)) => Ok(()),
                Ok(None) => Err(McpError::ShutdownTimeout),
                Err(_) => Err(McpError::Connection),
            }
        }));
        Ok(())
    }
}
