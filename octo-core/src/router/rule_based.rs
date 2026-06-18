//! `RuleBasedRouter` — data-driven router that holds a list of `Route`s and
//! applies them to envelopes flowing through the bus.
//!
//! The router is one of the manageable actors (see `manageable_actors` vault
//! draft): its state (the route table) is data, mutable through a typed API.
//! For MVP, mutation goes through methods on the router (no file watcher
//! yet). Later, the runtime config layer (`runtime_config` vault draft) will
//! tie this to TOML file changes.

use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;

use crate::{
    bus::{Filter, Subscription},
    ConnectorId, Envelope, OctoResult, Payload, Priority, RuleId, TrailAction, TrailActor,
    TrailEntry,
};

use super::{Route, RouteId, RouteStrategy, Router, RouterContext};
#[cfg(test)]
use super::RouteAction;

/// Data-driven router. Holds a list of routes and emits action envelopes when
/// they match.
pub struct RuleBasedRouter {
    id: String,
    self_source: ConnectorId,
    state: Arc<RwLock<RouterState>>,
}

#[derive(Default)]
pub struct RouterState {
    pub routes: Vec<Route>,
}

impl RuleBasedRouter {
    pub fn builder(id: impl Into<String>) -> RuleBasedRouterBuilder {
        let id = id.into();
        RuleBasedRouterBuilder {
            self_source: ConnectorId::new(format!("router/{id}")),
            id,
            routes: Vec::new(),
        }
    }

    /// Source attributed to envelopes emitted by this router. Used to skip
    /// self-emissions in the bus loop (avoids feedback).
    pub fn self_source(&self) -> &ConnectorId {
        &self.self_source
    }

    /// Snapshot of the current route table.
    pub fn list_routes(&self) -> Vec<Route> {
        self.state.read().routes.clone()
    }

    /// Add a new route. Returns the id of the inserted route.
    pub fn add_route(&self, route: Route) -> RouteId {
        let id = route.id.clone();
        let mut state = self.state.write();
        if let Some(existing) = state.routes.iter_mut().find(|r| r.id == id) {
            *existing = route;
        } else {
            state.routes.push(route);
            state
                .routes
                .sort_by_key(|r| std::cmp::Reverse(priority_rank(r.priority)));
        }
        id
    }

    /// Remove a route by id. Returns the removed entry if it existed.
    pub fn remove_route(&self, id: &str) -> Option<Route> {
        let mut state = self.state.write();
        let pos = state.routes.iter().position(|r| r.id == id)?;
        Some(state.routes.remove(pos))
    }

    /// Replace all routes atomically.
    pub fn replace_routes(&self, mut routes: Vec<Route>) {
        routes.sort_by_key(|r| std::cmp::Reverse(priority_rank(r.priority)));
        self.state.write().routes = routes;
    }

    /// Enable / disable a single route by id.
    pub fn set_enabled(&self, id: &str, enabled: bool) -> bool {
        let mut state = self.state.write();
        if let Some(route) = state.routes.iter_mut().find(|r| r.id == id) {
            route.enabled = enabled;
            true
        } else {
            false
        }
    }
}

fn priority_rank(p: Priority) -> u8 {
    match p {
        Priority::High => 2,
        Priority::Normal => 1,
        Priority::Low => 0,
    }
}

#[async_trait]
impl Router for RuleBasedRouter {
    fn id(&self) -> &str {
        &self.id
    }

    fn filter(&self) -> Filter {
        Filter::all()
    }

    async fn run(
        self: Arc<Self>,
        ctx: RouterContext,
        mut subscription: Subscription,
    ) -> OctoResult<()> {
        loop {
            tokio::select! {
                next = subscription.next() => match next {
                    Some(envelope) => {
                        // Skip our own emissions (avoid feedback loops).
                        if envelope.source == self.self_source {
                            continue;
                        }
                        self.process(&envelope, &ctx).await?;
                    }
                    None => return Ok(()),
                },
                _ = ctx.shutdown.cancelled() => return Ok(()),
            }
        }
    }
}

impl RuleBasedRouter {
    async fn process(&self, envelope: &Envelope, ctx: &RouterContext) -> OctoResult<()> {
        // Snapshot routes under read lock; release before async emit.
        let routes = self.state.read().routes.clone();

        for route in routes {
            if !route.matches(envelope) {
                continue;
            }

            match route.strategy {
                RouteStrategy::Terminate => {
                    self.emit(&route, envelope, ctx).await?;
                    return Ok(());
                }
                RouteStrategy::Enrich => {
                    self.emit(&route, envelope, ctx).await?;
                    // continue to next route
                }
                RouteStrategy::Observe => {
                    // No emission, but we could record an observability trail
                    // entry. For MVP, just log via tracing.
                    tracing::debug!(
                        router = %self.id,
                        route = %route.id,
                        kind = %envelope.kind,
                        "observed (no emission)"
                    );
                }
            }
        }

        Ok(())
    }

    async fn emit(
        &self,
        route: &Route,
        envelope: &Envelope,
        ctx: &RouterContext,
    ) -> OctoResult<()> {
        let new_kind = route
            .then
            .override_kind
            .clone()
            .unwrap_or_else(|| envelope.kind.clone());

        let payload = if route.then.copy_payload {
            envelope.payload.clone()
        } else if let Some(static_value) = &route.then.static_payload {
            Payload::new(static_value.clone())
        } else {
            // Neither copy nor static set — emit Null JSON value as a safe default.
            Payload::new(serde_json::Value::Null)
        };

        let mut emission = Envelope::from_parts(self.self_source.clone(), new_kind, payload)
            .with_target(route.then.target.clone());

        // Carry correlation_id from the original so request/response and
        // streams can stitch through routing.
        if let Some(cid) = envelope.correlation_id {
            emission = emission.with_correlation(cid);
        }

        // Add tags from the route action.
        for (k, v) in &route.then.add_tags {
            emission = emission.with_tag(k, v);
        }

        // Trail: who routed it, by which rule, what was emitted.
        emission.push_trail(TrailEntry::new(
            TrailActor::Reflex(RuleId::new(route.id.clone())),
            TrailAction::Emit {
                kind: emission.kind.clone(),
            },
        ));

        ctx.publish(emission).await
    }
}

/// Builder for [`RuleBasedRouter`].
pub struct RuleBasedRouterBuilder {
    id: String,
    self_source: ConnectorId,
    routes: Vec<Route>,
}

impl RuleBasedRouterBuilder {
    /// Add a route. Order of `add_route` calls doesn't matter — at `build()`
    /// time routes are sorted by priority.
    pub fn add_route(mut self, route: Route) -> Self {
        self.routes.push(route);
        self
    }

    /// Override the auto-generated `ConnectorId` used as `source` for emissions.
    pub fn self_source(mut self, source: ConnectorId) -> Self {
        self.self_source = source;
        self
    }

    pub fn build(self) -> Arc<RuleBasedRouter> {
        let mut routes = self.routes;
        routes.sort_by_key(|r| std::cmp::Reverse(priority_rank(r.priority)));
        Arc::new(RuleBasedRouter {
            id: self.id,
            self_source: self.self_source,
            state: Arc::new(RwLock::new(RouterState { routes })),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::{bus::KindPattern, router::RoutePredicate};

    fn simple_route(id: &str, kind_glob: &str, target: &str, strategy: RouteStrategy) -> Route {
        Route {
            id: id.into(),
            priority: Priority::Normal,
            strategy,
            when: RoutePredicate {
                kind: Some(KindPattern::new(kind_glob.to_string())),
                ..Default::default()
            },
            then: RouteAction {
                target: ConnectorId::new(target),
                override_kind: None,
                add_tags: HashMap::new(),
                copy_payload: true,
                static_payload: None,
            },
            enabled: true,
        }
    }

    #[test]
    fn add_route_via_builder_then_lookup() {
        let router = RuleBasedRouter::builder("test")
            .add_route(simple_route(
                "r1",
                "vision.**",
                "telegram",
                RouteStrategy::Terminate,
            ))
            .build();
        let routes = router.list_routes();
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].id, "r1");
    }

    #[test]
    fn add_route_at_runtime_sorts_by_priority() {
        let router = RuleBasedRouter::builder("test").build();
        let mut low = simple_route("low", "x.*", "out", RouteStrategy::Terminate);
        low.priority = Priority::Low;
        let mut high = simple_route("high", "x.*", "out", RouteStrategy::Terminate);
        high.priority = Priority::High;

        router.add_route(low);
        router.add_route(high);

        let routes = router.list_routes();
        // High priority must come first.
        assert_eq!(routes[0].id, "high");
        assert_eq!(routes[1].id, "low");
    }

    #[test]
    fn remove_route() {
        let router = RuleBasedRouter::builder("test")
            .add_route(simple_route(
                "r1",
                "x.*",
                "out",
                RouteStrategy::Terminate,
            ))
            .build();

        let removed = router.remove_route("r1");
        assert!(removed.is_some());
        assert!(router.list_routes().is_empty());

        // Removing nonexistent returns None.
        assert!(router.remove_route("nope").is_none());
    }

    #[test]
    fn set_enabled_toggles_route() {
        let router = RuleBasedRouter::builder("test")
            .add_route(simple_route(
                "r1",
                "x.*",
                "out",
                RouteStrategy::Terminate,
            ))
            .build();
        assert!(router.set_enabled("r1", false));
        assert!(!router.list_routes()[0].enabled);
        assert!(router.set_enabled("r1", true));
        assert!(router.list_routes()[0].enabled);
        // Unknown id returns false.
        assert!(!router.set_enabled("nope", false));
    }
}
