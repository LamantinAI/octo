//! Runtime — the assembly that owns the bus, the cogitator and the connectors.
//!
//! Use [`Octo::builder`] to assemble; [`Octo::run`] spawns the cogitator
//! (pre-subscribed) and each registered connector as independent tokio tasks
//! and waits for them all to complete (typically when the shutdown token fires).
//!
//! ## Mental model (GStreamer-style)
//!
//! The connector knows nothing about a specific bus instance. The runtime
//! owns the bus; when a connector is plugged in via the builder, it gets a
//! [`ConnectorContext`] at startup that carries publish / subscribe handles.
//! The cogitator is pre-subscribed by the runtime before any connector
//! publishes — guaranteeing no missed early envelopes.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::{
    bus::{EventBus, Filter, InProcessBus, Subscription},
    config::{self, ConfigError, ConnectorFactory},
    control, Cogitator, CogitatorContext, Connector, ConnectorContext, ConnectorInfo,
    EmptyCogitator, OctoResult, PayloadRegistry, RestartPolicy, Router, RouterContext,
    SubscribeOptions,
};

/// The runtime — owns the in-process bus, the cogitator, the optional router
/// and registered connectors.
pub struct Octo {
    bus: Arc<InProcessBus>,
    cogitator: Arc<dyn Cogitator>,
    router: Option<Arc<dyn Router>>,
    connectors: Vec<Arc<dyn Connector>>,
    shutdown: CancellationToken,
}

impl Octo {
    pub fn builder() -> OctoBuilder {
        OctoBuilder::new()
    }

    /// Subscribe to the runtime's bus from outside (observability, testing,
    /// reflex/cognition layers in sibling crates).
    pub async fn subscribe(
        &self,
        filter: Filter,
        opts: SubscribeOptions,
    ) -> OctoResult<Subscription> {
        self.bus.subscribe(filter, opts).await
    }

    /// Token whose cancellation signals graceful shutdown to all actors.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Number of registered connectors.
    pub fn connector_count(&self) -> usize {
        self.connectors.len()
    }

    /// Ids of all registered connectors (insertion order).
    pub fn connector_ids(&self) -> Vec<&str> {
        self.connectors.iter().map(|c| c.id().as_str()).collect()
    }

    pub fn cogitator_id(&self) -> &str {
        self.cogitator.id()
    }

    pub fn router_id(&self) -> Option<&str> {
        self.router.as_ref().map(|r| r.id())
    }

    /// Spawn the cogitator and (optional) router pre-subscribed, then every
    /// registered connector as tokio tasks; await their completion. Failures
    /// are logged via `tracing::error!`.
    ///
    /// **Lifecycle.** Connectors are the "drivers" — when all of them finish
    /// (gracefully or otherwise), the runtime cancels the shutdown token,
    /// signalling cogitator and router to wind down. Then it awaits their
    /// handles. So the runtime exits cleanly even if their `run` holds a
    /// long-lived subscription loop.
    pub async fn run(self) -> OctoResult<()> {
        // 1) Pre-subscribe and spawn the cogitator. Synchronous subscribe
        //    guarantees the cogitator's bus receiver is registered *before*
        //    any connector task is spawned — no race on early publishes.
        let cog_handle = {
            let cog = Arc::clone(&self.cogitator);
            let id = cog.id().to_string();
            let cog_filter = cog.filter();
            let cog_sub = self.bus.subscribe_sync(cog_filter, cog.subscribe_options());
            let connectors_info: Vec<ConnectorInfo> = self
                .connectors
                .iter()
                .map(|c| ConnectorInfo {
                    id: c.id().clone(),
                    capabilities: c.capabilities().clone(),
                })
                .collect();
            let cog_ctx =
                CogitatorContext::new(self.shutdown.clone(), Arc::clone(&self.bus), connectors_info);

            tokio::spawn(async move {
                if let Err(e) = cog.run(cog_ctx, cog_sub).await {
                    tracing::error!(cogitator = %id, error = %e, "cogitator failed");
                }
            })
        };

        // 2) Pre-subscribe and spawn the router (if any). Same pattern: sync
        //    subscribe before connectors start publishing.
        let router_handle = self.router.as_ref().map(|router| {
            let r = Arc::clone(router);
            let id = r.id().to_string();
            let filter = r.filter();
            let sub = self.bus.subscribe_sync(filter, SubscribeOptions::default());
            let ctx = RouterContext::new(self.shutdown.clone(), Arc::clone(&self.bus));

            tokio::spawn(async move {
                if let Err(e) = r.run(ctx, sub).await {
                    tracing::error!(router = %id, error = %e, "router failed");
                }
            })
        });

        // 3) Per-connector restart signals + a control-plane listener: an
        //    inhabitant emits octo.control.* to restart a connector or the
        //    whole process. The environment carries it out.
        let restart_notifies: HashMap<String, Arc<Notify>> = self
            .connectors
            .iter()
            .map(|c| (c.id().as_str().to_string(), Arc::new(Notify::new())))
            .collect();

        let ctrl_handle = {
            // Control plane is low-rate; default (drop-oldest, visible) is fine.
            // A never-drop criticality lane is a deferred option (see fix brief).
            let mut ctrl_sub = self
                .bus
                .subscribe_sync(Filter::by_kind(control::CONTROL_GLOB), SubscribeOptions::default());
            let ctrl_shutdown = self.shutdown.clone();
            let notifies = restart_notifies.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        next = ctrl_sub.next() => match next {
                            Some(env) => match env.kind.as_str() {
                                control::RESTART_CONNECTOR => {
                                    if let Some(id) = env.payload_as::<String>() {
                                        match notifies.get(id) {
                                            Some(n) => {
                                                tracing::info!(connector = %id, "control: restart_connector");
                                                n.notify_one();
                                            }
                                            None => tracing::warn!(connector = %id, "control: restart_connector for unknown connector"),
                                        }
                                    }
                                }
                                control::RESTART_PROCESS => {
                                    tracing::info!("control: restart_process → graceful shutdown");
                                    ctrl_shutdown.cancel();
                                    return;
                                }
                                _ => {}
                            },
                            None => return,
                        },
                        _ = ctrl_shutdown.cancelled() => return,
                    }
                }
            })
        };

        // 4) Spawn connectors under supervision — restart on failure/panic per
        //    each connector's RestartPolicy (clean exit is not restarted), and
        //    on an explicit control restart signal.
        let mut conn_handles = Vec::with_capacity(self.connectors.len());
        for connector in &self.connectors {
            let conn = Arc::clone(connector);
            let shutdown = self.shutdown.clone();
            let bus = Arc::clone(&self.bus);
            let restart = restart_notifies
                .get(conn.id().as_str())
                .cloned()
                .unwrap_or_else(|| Arc::new(Notify::new()));
            conn_handles.push(tokio::spawn(supervise(conn, shutdown, bus, restart)));
        }

        // 4) Wait for connectors. They drive the lifecycle.
        for h in conn_handles {
            let _ = h.await;
        }

        // 5) All connectors done → signal cogitator + router to wind down.
        //    (No-op if it's already cancelled by the caller.)
        self.shutdown.cancel();

        // 6) Wait for the cogitator to finish flushing.
        let _ = cog_handle.await;

        // 7) Wait for the router, if present.
        if let Some(h) = router_handle {
            let _ = h.await;
        }

        // 8) Wait for the control listener.
        let _ = ctrl_handle.await;

        Ok(())
    }
}

/// Builder for [`Octo`]. Defaults:
/// - bus capacity 1024
/// - fresh shutdown token
/// - [`EmptyCogitator`] as the cogitator (no-op observer)
pub struct OctoBuilder {
    bus_capacity: usize,
    cogitator: Option<Arc<dyn Cogitator>>,
    router: Option<Arc<dyn Router>>,
    connectors: Vec<Arc<dyn Connector>>,
    shutdown: Option<CancellationToken>,
    payload_registry: Option<Arc<PayloadRegistry>>,
    factories: HashMap<String, Arc<dyn ConnectorFactory>>,
}

impl OctoBuilder {
    pub fn new() -> Self {
        Self {
            bus_capacity: 1024,
            cogitator: None,
            router: None,
            connectors: Vec::new(),
            shutdown: None,
            payload_registry: None,
            factories: HashMap::new(),
        }
    }

    /// Register a connector factory under its `type` name, so `octo.toml`
    /// entries with `type = "<name>"` can be instantiated by
    /// [`Self::from_config_file`]. Later registration of the same type wins.
    pub fn register_connector_type(
        mut self,
        type_name: impl Into<String>,
        factory: Arc<dyn ConnectorFactory>,
    ) -> Self {
        self.factories.insert(type_name.into(), factory);
        self
    }

    /// Load an `octo.toml` manifest: apply `[runtime]` settings, scan
    /// `[connectors]` (folder- and flat-style files plus the explicit list),
    /// and instantiate each dyn connector via its registered factory. Each
    /// loaded connector's [`Connector::register_payloads`] is folded into the
    /// payload registry (unless one was already set explicitly).
    ///
    /// This is the **startup-load** path — read once, here. Hot reload is a
    /// separate, later mechanism. Errors if a `type` has no factory, an `id`
    /// collides, or a manifest is malformed.
    pub fn from_config_file(mut self, path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let existing_ids: HashSet<String> = self
            .connectors
            .iter()
            .map(|c| c.id().as_str().to_string())
            .collect();

        let loaded = config::load_config(path.as_ref(), &self.factories, &existing_ids)?;

        if let Some(cap) = loaded.bus_capacity {
            self.bus_capacity = cap.max(1);
        }

        // A `[router]` table builds a RuleBasedRouter — unless one was already
        // set in code (explicit `.router(...)` wins; warn so it's not silent).
        match (self.router.is_some(), loaded.router) {
            (false, Some(router)) => self.router = Some(router),
            (true, Some(_)) => {
                tracing::warn!("router already set in code; ignoring [router] from config");
            }
            (_, None) => {}
        }

        // Auto-register payloads from config-loaded connectors, unless the app
        // already supplied a registry explicitly.
        if self.payload_registry.is_none() {
            let mut registry = PayloadRegistry::new();
            for conn in &loaded.connectors {
                registry = conn.register_payloads(registry);
            }
            self.payload_registry = Some(Arc::new(registry));
        } else {
            tracing::warn!("payload_registry already set; skipping auto-registration from config");
        }

        self.connectors.extend(loaded.connectors);
        Ok(self)
    }

    /// Attach a router. Optional — without it, the runtime has no
    /// declarative routing layer; events flow through the bus, and the
    /// cogitator handles whatever lands.
    ///
    /// `Arc<MyRouter>` coerces to `Arc<dyn Router>` automatically when
    /// `MyRouter: Router`.
    pub fn router(mut self, router: Arc<dyn Router>) -> Self {
        self.router = Some(router);
        self
    }

    /// Attach a payload registry. Optional: without it the bus accepts any
    /// envelope; with it, each publish validates the envelope's payload type
    /// against the registry's entry for its kind. Each connector / cogitator
    /// crate is expected to expose its own `register_payloads(&mut PayloadRegistry)`
    /// helper that the application stitches together at startup.
    pub fn payload_registry(mut self, registry: Arc<PayloadRegistry>) -> Self {
        self.payload_registry = Some(registry);
        self
    }

    /// Bus broadcast capacity. Must be ≥ 1.
    pub fn bus_capacity(mut self, n: usize) -> Self {
        self.bus_capacity = n.max(1);
        self
    }

    /// Override the default cogitator. `Arc<MyCogitator>` coerces to
    /// `Arc<dyn Cogitator>` automatically.
    pub fn cogitator(mut self, cogitator: Arc<dyn Cogitator>) -> Self {
        self.cogitator = Some(cogitator);
        self
    }

    /// Add a connector to the runtime. `Arc<MyConnector>` coerces to
    /// `Arc<dyn Connector>` automatically when `MyConnector: Connector`.
    pub fn add_connector(mut self, connector: Arc<dyn Connector>) -> Self {
        self.connectors.push(connector);
        self
    }

    /// Use an externally-managed shutdown token. Default: a fresh one.
    pub fn shutdown_token(mut self, token: CancellationToken) -> Self {
        self.shutdown = Some(token);
        self
    }

    pub fn build(self) -> Octo {
        let mut bus = InProcessBus::new(self.bus_capacity);
        if let Some(reg) = self.payload_registry {
            bus = bus.with_registry(reg);
        }
        Octo {
            bus: Arc::new(bus),
            cogitator: self
                .cogitator
                .unwrap_or_else(|| EmptyCogitator::new("empty")),
            router: self.router,
            connectors: self.connectors,
            shutdown: self.shutdown.unwrap_or_else(CancellationToken::new),
        }
    }
}

impl Default for OctoBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Supervise one connector. It is restarted on:
/// - **failure / panic** — per its [`RestartPolicy`] (with backoff), and
/// - an explicit **control restart** signal (`restart`) — gracefully (the
///   connector's `ctx.shutdown` is cancelled so it can wind down), no backoff.
///
/// A clean `Ok(())` exit (e.g. on global shutdown) ends supervision. Global
/// shutdown also ends it — no restart while the runtime is winding down.
async fn supervise(
    conn: Arc<dyn Connector>,
    shutdown: CancellationToken,
    bus: Arc<InProcessBus>,
    restart: Arc<Notify>,
) {
    let id = conn.id().clone();
    let policy = conn.restart_policy();
    let mut attempt: u32 = 0;

    loop {
        attempt += 1;
        // A child token so the connector stops on global shutdown OR on a
        // control restart of just this connector.
        let conn_token = shutdown.child_token();
        let ctx = ConnectorContext::new(conn_token.clone(), Arc::clone(&bus));
        let run_fut = std::panic::AssertUnwindSafe(Arc::clone(&conn).run(ctx)).catch_unwind();
        tokio::pin!(run_fut);

        tokio::select! {
            outcome = &mut run_fut => {
                if shutdown.is_cancelled() {
                    return;
                }
                match outcome {
                    Ok(Ok(())) => return, // clean exit — done.
                    Ok(Err(e)) => tracing::error!(connector = %id, error = %e, "connector failed"),
                    Err(_) => tracing::error!(connector = %id, "connector panicked"),
                }
                let (do_restart, delay_ms) = restart_decision(policy, attempt);
                if !do_restart {
                    tracing::warn!(connector = %id, attempts = attempt, "restart policy exhausted; giving up");
                    return;
                }
                tracing::warn!(connector = %id, attempt, delay_ms, "restarting connector (failure)");
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(delay_ms)) => {}
                    _ = shutdown.cancelled() => return,
                }
            }
            _ = restart.notified() => {
                // Intentional restart: cancel this run gracefully, let it finish.
                tracing::info!(connector = %id, "restart requested via control");
                conn_token.cancel();
                let _ = run_fut.await;
                if shutdown.is_cancelled() {
                    return;
                }
                attempt = 0; // not a failure — reset the backoff counter.
            }
        }
    }
}

/// `(should_restart, backoff_ms)` for an attempt under a policy.
fn restart_decision(policy: RestartPolicy, attempt: u32) -> (bool, u64) {
    match policy {
        RestartPolicy::Never => (false, 0),
        RestartPolicy::Always => (true, 0),
        RestartPolicy::MaxAttempts(n) => (attempt < n, 0),
        RestartPolicy::ExponentialBackoff {
            initial_ms,
            max_ms,
            max_attempts,
        } => {
            let allowed = max_attempts.map_or(true, |m| attempt < m);
            let shift = (attempt - 1).min(16);
            let delay = initial_ms.saturating_mul(1u64 << shift).min(max_ms);
            (allowed, delay)
        }
    }
}
