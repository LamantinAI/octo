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

use tokio_util::sync::CancellationToken;

use crate::{
    bus::{EventBus, Filter, InProcessBus, Subscription},
    config::{self, ConfigError, ConnectorFactory},
    Cogitator, CogitatorContext, Connector, ConnectorContext, EmptyCogitator, OctoResult,
    PayloadRegistry, Router, RouterContext, SubscribeOptions,
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
            let cog_sub = self.bus.subscribe_sync(cog_filter);
            let cog_ctx = CogitatorContext::new(self.shutdown.clone(), Arc::clone(&self.bus));

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
            let sub = self.bus.subscribe_sync(filter);
            let ctx = RouterContext::new(self.shutdown.clone(), Arc::clone(&self.bus));

            tokio::spawn(async move {
                if let Err(e) = r.run(ctx, sub).await {
                    tracing::error!(router = %id, error = %e, "router failed");
                }
            })
        });

        // 3) Spawn connectors.
        let mut conn_handles = Vec::with_capacity(self.connectors.len());
        for connector in &self.connectors {
            let conn = Arc::clone(connector);
            let id = conn.id().clone();
            let ctx = ConnectorContext::new(self.shutdown.clone(), Arc::clone(&self.bus));

            conn_handles.push(tokio::spawn(async move {
                if let Err(e) = conn.run(ctx).await {
                    tracing::error!(connector = %id, error = %e, "connector failed");
                }
            }));
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
