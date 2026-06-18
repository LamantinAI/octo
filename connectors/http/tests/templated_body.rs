//! End-to-end: a dyn connector with a `body_template` sends a *transformed*
//! request body (not the raw command payload) over the wire. The mock echoes
//! the received body back, so the test asserts exactly what was sent.

mod mock_http;

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use octo_connector_http::{HttpConnector, HttpSpec};
use octo_core::{
    Connector, ConnectorCapabilities, ConnectorContext, ConnectorId, Envelope, EventKind, Octo,
    OctoResult,
};
use serde_json::{json, Value};

use mock_http::{MockServer, Route};

const MANIFEST: &str = r#"
[connector]
id = "notifier"
type = "http"
base_url = "PLACEHOLDER"

[[connector.endpoint]]
cmd_kind = "notify.cmd.send"
method = "POST"
path = "/notify"
response_kind = "notify.event.sent"
# Reshape {text, channel} into the API's wire format, adding a static field.
body_template = '''
{ "text": "${payload.text}", "channel": "${payload.channel}", "username": "octo-agent" }
'''
"#;

struct Agent {
    id: ConnectorId,
    capabilities: ConnectorCapabilities,
    out: Arc<Mutex<Option<Value>>>,
}

#[async_trait]
impl Connector for Agent {
    fn id(&self) -> &ConnectorId {
        &self.id
    }
    fn capabilities(&self) -> &ConnectorCapabilities {
        &self.capabilities
    }
    async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
        tokio::time::sleep(Duration::from_millis(150)).await;
        let cmd = Envelope::new(
            self.id.clone(),
            EventKind::new("notify.cmd.send"),
            json!({ "text": "high severity!", "channel": "#ops" }),
        )
        .with_target(ConnectorId::new("notifier"));

        if let Ok(resp) = ctx
            .publish_and_await_response(cmd, Duration::from_secs(5))
            .await
        {
            *self.out.lock().unwrap() = resp.payload_as::<Value>().cloned();
        }
        ctx.shutdown.cancel();
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn templated_body_reaches_server_transformed() {
    // The mock echoes the request body it received back as the response.
    let server = MockServer::start(vec![Route::new("POST", "/notify", 200, |body| body.to_string())]).await;

    let mut spec = HttpSpec::from_toml_str(MANIFEST, Path::new(".")).unwrap();
    spec.base_url = server.base_url();
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let connector = HttpConnector::with_client(spec, client);

    let out = Arc::new(Mutex::new(None));
    let octo = Octo::builder()
        .add_connector(connector)
        .add_connector(Arc::new(Agent {
            id: ConnectorId::new("agent"),
            capabilities: ConnectorCapabilities::output_only(),
            out: Arc::clone(&out),
        }))
        .build();

    octo.run().await.unwrap();

    let echoed = out.lock().unwrap().clone().expect("got a response body");
    // The server received the *templated* shape, not the raw {text, channel}.
    assert_eq!(echoed["text"], "high severity!");
    assert_eq!(echoed["channel"], "#ops");
    assert_eq!(
        echoed["username"], "octo-agent",
        "static template field must be present — proves templating, not pass-through"
    );
}
