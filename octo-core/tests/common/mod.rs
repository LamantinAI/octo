#![allow(dead_code)]
use std::sync::Arc;

use async_trait::async_trait;
use octo_core::{
    Cogitator, CogitatorContext, Connector, ConnectorCapabilities, ConnectorContext, ConnectorId,
    Envelope, EventKind, OctoError, OctoResult, RestartPolicy, Subscription,
    control::RESTART_PROCESS,
};

/// One-shot connector that publishes one envelope through `ctx`, then exits.
/// Used to validate the runtime end-to-end.
pub struct OneShot {
    pub id: ConnectorId,
    pub capabilities: ConnectorCapabilities,
    pub kind: EventKind,
    pub value: i32,
}

#[async_trait]
impl Connector for OneShot {
    fn id(&self) -> &ConnectorId {
        &self.id
    }
    fn capabilities(&self) -> &ConnectorCapabilities {
        &self.capabilities
    }
    async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
        ctx.publish(Envelope::new(
            self.id.clone(),
            self.kind.clone(),
            self.value,
        ))
        .await
    }
}

/// Cogitator that counts every observed envelope into a shared atomic.
/// Used to verify the cogitator is wired into the pipeline.
pub struct CountingCogitator {
    pub id: String,
    pub count: Arc<std::sync::atomic::AtomicU64>,
}

#[async_trait]
impl Cogitator for CountingCogitator {
    fn id(&self) -> &str {
        &self.id
    }
    async fn run(
        self: Arc<Self>,
        ctx: CogitatorContext,
        mut subscription: Subscription,
    ) -> OctoResult<()> {
        loop {
            tokio::select! {
                next = subscription.next() => match next {
                    Some(_) => { self.count.fetch_add(1, std::sync::atomic::Ordering::Relaxed); }
                    None => return Ok(()),
                },
                _ = ctx.shutdown.cancelled() => return Ok(()),
            }
        }
    }
}

/// A connector that fails on its first run is restarted by the supervisor;
/// on the second run it exits cleanly and stops the runtime.
pub struct FlakyConnector {
    pub id: ConnectorId,
    pub capabilities: ConnectorCapabilities,
    pub attempts: Arc<std::sync::atomic::AtomicU64>,
}

#[async_trait]
impl Connector for FlakyConnector {
    fn id(&self) -> &ConnectorId {
        &self.id
    }
    fn capabilities(&self) -> &ConnectorCapabilities {
        &self.capabilities
    }
    fn restart_policy(&self) -> RestartPolicy {
        // Fast backoff so the test doesn't wait.
        RestartPolicy::ExponentialBackoff {
            initial_ms: 10,
            max_ms: 50,
            max_attempts: None,
        }
    }
    async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
        let n = self
            .attempts
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        if n == 1 {
            return Err(OctoError::Connector("boom".into()));
        }
        // Recovered — wind the runtime down.
        ctx.publish(Envelope::new(
            self.id.clone(),
            EventKind::from_static(RESTART_PROCESS),
            String::new(),
        ))
        .await?;
        Ok(())
    }
}

/// A connector that emits control signals: on its first run it requests its
/// own restart; on the second it requests a process restart (graceful
/// shutdown). Exercises both `octo.control.*` paths through the runtime.
pub struct SelfRestartConnector {
    pub id: ConnectorId,
    pub capabilities: ConnectorCapabilities,
    pub attempts: Arc<std::sync::atomic::AtomicU64>,
}

#[async_trait]
impl Connector for SelfRestartConnector {
    fn id(&self) -> &ConnectorId {
        &self.id
    }
    fn capabilities(&self) -> &ConnectorCapabilities {
        &self.capabilities
    }
    async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
        let n = self
            .attempts
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        if n == 1 {
            // Restart just me.
            ctx.publish(Envelope::new(
                self.id.clone(),
                EventKind::from_static(octo_core::control::RESTART_CONNECTOR),
                self.id.as_str().to_string(),
            ))
            .await?;
        } else {
            // Restart the whole process (graceful shutdown).
            ctx.publish(Envelope::new(
                self.id.clone(),
                EventKind::from_static(octo_core::control::RESTART_PROCESS),
                (),
            ))
            .await?;
        }
        // Wait until the supervisor (or global shutdown) cancels us.
        ctx.shutdown.cancelled().await;
        Ok(())
    }
}
