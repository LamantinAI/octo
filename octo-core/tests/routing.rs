use std::sync::Arc;

use async_trait::async_trait;
use octo_core::{
    Connector, ConnectorCapabilities, ConnectorContext, ConnectorId, Envelope, EventKind, Filter,
    Octo, OctoResult, Priority, SubscribeOptions, TrailActor,
};
mod common;

/// End-to-end: router plugged into Octo. Connector publishes a raw event
/// with no `target`. The router has one matching rule that emits an
/// action envelope with `target=alerter` and `override_kind=alert.text`.
/// An external subscriber filtered by target receives the routed envelope.
#[tokio::test(flavor = "current_thread")]
async fn router_routes_envelope_via_terminate_rule() {
    use std::collections::HashMap;

    use octo_core::bus::KindPattern;
    use octo_core::router::{Route, RouteAction, RoutePredicate, RouteStrategy, RuleBasedRouter};

    #[derive(Debug, Clone)]
    struct Tick(u64);

    struct OneShotEmitter {
        id: ConnectorId,
        capabilities: ConnectorCapabilities,
    }

    #[async_trait]
    impl Connector for OneShotEmitter {
        fn id(&self) -> &ConnectorId {
            &self.id
        }
        fn capabilities(&self) -> &ConnectorCapabilities {
            &self.capabilities
        }
        async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
            // Brief warmup so router subscription is registered first.
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            ctx.publish(Envelope::new(
                self.id.clone(),
                EventKind::from_static("vision.incident.detected"),
                Tick(42),
            ))
            .await
        }
    }

    let alert_target = ConnectorId::new("alerter");

    let router = RuleBasedRouter::builder("test_router")
        .add_route(Route {
            id: "incident_to_alerter".into(),
            priority: Priority::Normal,
            strategy: RouteStrategy::Terminate,
            when: RoutePredicate {
                kind: Some(KindPattern::new("vision.incident.*")),
                ..Default::default()
            },
            then: RouteAction {
                target: alert_target.clone(),
                override_kind: Some(EventKind::from_static("alert.text")),
                add_tags: HashMap::new(),
                copy_payload: true,
                static_payload: None,
            },
            enabled: true,
        })
        .build();

    let connector = Arc::new(OneShotEmitter {
        id: ConnectorId::new("sensor"),
        capabilities: ConnectorCapabilities::input_only(),
    });

    let octo = Octo::builder()
        .bus_capacity(32)
        .router(router)
        .add_connector(connector)
        .build();

    assert_eq!(octo.router_id(), Some("test_router"));

    let mut sub = octo
        .subscribe(
            Filter::by_target(alert_target.clone()),
            SubscribeOptions::default(),
        )
        .await
        .unwrap();
    let received = tokio::spawn(async move { sub.next().await });

    tokio::time::timeout(std::time::Duration::from_secs(15), octo.run())
        .await
        .expect("runtime must stop")
        .unwrap();

    let env = received
        .await
        .unwrap()
        .expect("routed envelope should reach target subscriber");
    assert_eq!(env.kind.as_str(), "alert.text");
    assert_eq!(env.target.as_ref(), Some(&alert_target));
    assert_eq!(env.payload_as::<Tick>().map(|t| t.0), Some(42));
    // Trail records the route's action.
    assert!(env.trail.iter().any(
        |t| matches!(&t.actor, TrailActor::Reflex(rid) if rid.as_str() == "incident_to_alerter")
    ));
}

/// Without a router, Octo runs as before — the bus does not invent routing.
#[tokio::test(flavor = "current_thread")]
async fn octo_without_router_works_unchanged() {
    let octo = Octo::builder().build();
    assert!(octo.router_id().is_none());
    tokio::time::timeout(std::time::Duration::from_secs(15), octo.run())
        .await
        .expect("runtime must stop")
        .unwrap();
}
