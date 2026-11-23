//! Generic hosted web transport. Applications inject their router and envelope
//! policy; this crate has no accounts, conversation, model or storage semantics.
use std::{future::IntoFuture, net::SocketAddr, sync::Arc, time::Duration};

use async_trait::async_trait;
use axum::{Router, serve};
use octo_core::{
    Connector, ConnectorCapabilities, ConnectorContext, ConnectorId, Envelope, EventKind, Filter,
    OctoError, OctoResult, SubscribeOptions,
};
use tokio::{net::TcpListener, pin, select, time::timeout};

#[async_trait]
pub trait WebApplication: Send + Sync + 'static {
    async fn router(&self, context: ConnectorContext) -> OctoResult<Router>;
    async fn on_event(&self, _context: &ConnectorContext, _envelope: Envelope) -> OctoResult<()> {
        Ok(())
    }
}
#[async_trait]
impl WebApplication for Router {
    async fn router(&self, _context: ConnectorContext) -> OctoResult<Router> {
        Ok(self.clone())
    }
}

pub struct WebConnector {
    id: ConnectorId,
    address: SocketAddr,
    application: Arc<dyn WebApplication>,
    capabilities: ConnectorCapabilities,
    shutdown_grace: Duration,
    event_filter: Option<Filter>,
}
impl WebConnector {
    pub fn new(
        id: impl Into<String>,
        address: SocketAddr,
        application: Arc<dyn WebApplication>,
    ) -> Arc<Self> {
        Arc::new(Self {
            id: ConnectorId::new(id),
            address,
            application,
            capabilities: ConnectorCapabilities::bidirectional(),
            shutdown_grace: Duration::from_secs(5),
            event_filter: None,
        })
    }
    pub fn with_kinds(
        mut self: Arc<Self>,
        emits: impl IntoIterator<Item = EventKind>,
        accepts: impl IntoIterator<Item = EventKind>,
    ) -> Arc<Self> {
        let connector = Arc::get_mut(&mut self).expect("configure before sharing");
        connector.capabilities = ConnectorCapabilities::bidirectional()
            .with_emit_kinds(emits)
            .with_accept_kinds(accepts);
        self
    }
    pub fn with_shutdown_grace(mut self: Arc<Self>, duration: Duration) -> Arc<Self> {
        Arc::get_mut(&mut self)
            .expect("configure before sharing")
            .shutdown_grace = duration;
        self
    }
    /// Applications may observe protocol events across transports. The default
    /// remains this connector's configured targeted accept kinds.
    pub fn with_event_filter(mut self: Arc<Self>, filter: Filter) -> Arc<Self> {
        Arc::get_mut(&mut self)
            .expect("configure before sharing")
            .event_filter = Some(filter);
        self
    }
    /// Host/test seam for a prebound listener, including port-zero allocation.
    pub async fn serve(
        self: Arc<Self>,
        listener: TcpListener,
        context: ConnectorContext,
    ) -> OctoResult<()> {
        let router = select! {
            result = self.application.router(context.clone()) => result?,
            _ = context.shutdown.cancelled() => return Ok(()),
        };
        let mut filter = Filter::by_target(self.id.clone());
        filter.kinds = Some(Vec::new());
        for kind in &self.capabilities.event_kinds_accept {
            filter = filter.with_kind(kind.as_str().to_owned());
        }
        let filter = self.event_filter.clone().unwrap_or(filter);
        let mut subscription = context
            .subscribe(filter, SubscribeOptions::default())
            .await?;
        let shutdown = context.shutdown.clone();
        let server = serve(listener, router)
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .into_future();
        pin!(server);
        loop {
            select! {
                result = &mut server => return result.map_err(|error| OctoError::Connector(error.to_string())),
                _ = context.shutdown.cancelled() => {
                    return match timeout(self.shutdown_grace, &mut server).await {
                        Ok(result) => result.map_err(|error| OctoError::Connector(error.to_string())),
                        Err(_) => Ok(()),
                    };
                }
                event = subscription.next() => {
                    let Some(event) = event else { return Ok(()); };
                    select! {
                        result = self.application.on_event(&context, (*event).clone()) => result?,
                        _ = context.shutdown.cancelled() => continue,
                    }
                }
            }
        }
    }
}

#[async_trait]
impl Connector for WebConnector {
    fn id(&self) -> &ConnectorId {
        &self.id
    }
    fn capabilities(&self) -> &ConnectorCapabilities {
        &self.capabilities
    }
    async fn run(self: Arc<Self>, context: ConnectorContext) -> OctoResult<()> {
        let listener = TcpListener::bind(self.address)
            .await
            .map_err(|error| OctoError::Connector(error.to_string()))?;
        self.serve(listener, context).await
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::pending,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use async_trait::async_trait;
    use axum::{
        Json, Router,
        http::StatusCode,
        routing::{get, post},
    };
    use octo_core::{
        ConnectorContext, ConnectorId, Envelope, EventBus, EventKind, Filter, InProcessBus,
        OctoResult, SubscribeOptions, control::CancellationToken,
    };
    use reqwest::Client;
    use serde_json::{Value, json};
    use tokio::{net::TcpListener, spawn, sync::Notify, time::timeout};

    use super::{WebApplication, WebConnector};

    #[derive(Default)]
    struct Application {
        events: Mutex<Vec<String>>,
        received: Notify,
        block_events: bool,
    }
    #[async_trait]
    impl WebApplication for Application {
        async fn router(&self, context: ConnectorContext) -> OctoResult<Router> {
            Ok(Router::new()
                .route("/health", get(|| async { "ready" }))
                .route(
                    "/input",
                    post(move |Json(value): Json<Value>| {
                        let context = context.clone();
                        async move {
                            context
                                .publish(Envelope::new(
                                    ConnectorId::new("web"),
                                    EventKind::new("demo.input"),
                                    value,
                                ))
                                .await
                                .unwrap();
                            StatusCode::ACCEPTED
                        }
                    }),
                ))
        }
        async fn on_event(&self, _: &ConnectorContext, event: Envelope) -> OctoResult<()> {
            self.events.lock().unwrap().push(event.kind.as_str().into());
            self.received.notify_one();
            if self.block_events {
                pending::<()>().await;
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn hosted_router_and_configured_event_bridge_share_connector_lifecycle() {
        let bus = Arc::new(InProcessBus::new(16));
        let shutdown = CancellationToken::new();
        let context = ConnectorContext::new(shutdown.clone(), bus.clone());
        let mut inputs = bus
            .subscribe(Filter::by_kind("demo.input"), SubscribeOptions::default())
            .await
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let application = Arc::new(Application::default());
        let connector = WebConnector::new("web", address, application.clone()).with_kinds(
            [EventKind::new("demo.input")],
            [EventKind::new("demo.output")],
        );
        let task = spawn(connector.serve(listener, context));
        let client = Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        assert_eq!(
            client
                .get(format!("http://{address}/health"))
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap(),
            "ready"
        );
        assert_eq!(
            client
                .post(format!("http://{address}/input"))
                .json(&json!({"content":"native request"}))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::ACCEPTED
        );
        let input = timeout(Duration::from_secs(2), inputs.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            input.payload_as::<Value>().unwrap()["content"],
            "native request"
        );
        for kind in ["unconfigured.output", "demo.output"] {
            bus.publish(
                Envelope::new(ConnectorId::new("host"), EventKind::new(kind), true)
                    .with_target(ConnectorId::new("web")),
            )
            .await
            .unwrap();
        }
        timeout(Duration::from_secs(2), application.received.notified())
            .await
            .unwrap();
        assert_eq!(
            application.events.lock().unwrap().as_slice(),
            ["demo.output"]
        );
        shutdown.cancel();
        timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn blocked_application_callback_does_not_prevent_shutdown() {
        let bus = Arc::new(InProcessBus::new(16));
        let shutdown = CancellationToken::new();
        let context = ConnectorContext::new(shutdown.clone(), bus.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let application = Arc::new(Application {
            block_events: true,
            ..Default::default()
        });
        let connector = WebConnector::new("web", address, application.clone())
            .with_kinds([], [EventKind::new("demo.output")]);
        let task = spawn(connector.serve(listener, context));
        let client = Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        client
            .get(format!("http://{address}/health"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        bus.publish(
            Envelope::new(
                ConnectorId::new("host"),
                EventKind::new("demo.output"),
                true,
            )
            .with_target(ConnectorId::new("web")),
        )
        .await
        .unwrap();
        timeout(Duration::from_secs(2), application.received.notified())
            .await
            .unwrap();
        shutdown.cancel();
        timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
