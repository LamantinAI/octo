mod mock_http;

use std::{
    collections::HashMap,
    env::set_var,
    path::Path,
    sync::{Arc, Mutex, Once},
    time::Duration,
};

use mock_http::{MockServer, Request, Route};
use octo_connector_http::{AuthSpec, HttpConnector, HttpConnectorFactory, HttpSpec};
use octo_core::{
    Connector, ConnectorContext, ConnectorFactory, ConnectorId, Envelope, EventKind,
    FactoryContext, InProcessBus, OctoResult, TrailAction,
};
use octo_http_auth::AuthConfig;
use reqwest::{Client, Url};
use serde_json::{Value, json};
use tokio::{
    net::TcpListener,
    spawn,
    task::{JoinHandle, yield_now},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

const EXAMPLE: &str = include_str!("../examples/google_calendar.toml");
const CALL_TIMEOUT: Duration = Duration::from_secs(3);

fn credentials() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        // Unique test-only names, initialized once and never changed afterward.
        unsafe {
            set_var("OCTO_HTTP_OAUTH_TEST_SECRET", "test secret+&=");
            set_var("OCTO_HTTP_OAUTH_TEST_REFRESH", "test refresh+&=");
        }
    });
}

fn manifest(base_url: &str) -> String {
    EXAMPLE
        .replace("https://www.googleapis.com/calendar/v3", base_url)
        .replace(
            "https://oauth2.googleapis.com/token",
            &format!("{base_url}/token"),
        )
        .replace("GOOGLE_CLIENT_SECRET", "OCTO_HTTP_OAUTH_TEST_SECRET")
        .replace("GOOGLE_REFRESH_TOKEN", "OCTO_HTTP_OAUTH_TEST_REFRESH")
}

fn factory_connector(manifest: &str) -> Arc<dyn Connector> {
    let factory = HttpConnectorFactory::with_client(Client::builder().no_proxy().build().unwrap());
    factory
        .create(
            ConnectorId::new("google_calendar"),
            &manifest.parse().unwrap(),
            FactoryContext {
                base_dir: Path::new("."),
            },
        )
        .unwrap()
}

struct Session {
    ctx: ConnectorContext,
    task: JoinHandle<OctoResult<()>>,
}

impl Session {
    async fn start(connector: Arc<dyn Connector>) -> Self {
        let bus = Arc::new(InProcessBus::new(32));
        let ctx = ConnectorContext::new(CancellationToken::new(), bus.clone());
        let task = spawn(connector.run(ctx.clone()));
        timeout(CALL_TIMEOUT, async {
            while bus.subscriber_count() == 0 {
                yield_now().await;
            }
        })
        .await
        .expect("connector subscribed");
        Self { ctx, task }
    }

    async fn command(&self) -> Arc<Envelope> {
        let cmd = Envelope::new(
            ConnectorId::new("test"),
            EventKind::new("google_calendar.cmd.list_events"),
            json!({"calendar_id": "primary"}),
        )
        .with_target(ConnectorId::new("google_calendar"));
        let id = cmd.id;
        let response = self
            .ctx
            .publish_and_await_response(cmd, CALL_TIMEOUT)
            .await
            .unwrap();
        assert_eq!(response.correlation_id, Some(id));
        assert!(response.trail.iter().any(
            |entry| matches!(&entry.action, TrailAction::Emit { kind } if kind == &response.kind)
        ));
        response
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.ctx.shutdown.cancel();
        self.task.abort();
    }
}

struct Fixture {
    server: MockServer,
    tokens: Arc<Mutex<Vec<Request>>>,
    api: Arc<Mutex<Vec<Request>>>,
}

impl Fixture {
    async fn start(status: u16, token_body: String) -> Self {
        credentials();
        let tokens = Arc::new(Mutex::new(Vec::new()));
        let token_out = tokens.clone();
        let api = Arc::new(Mutex::new(Vec::new()));
        let api_out = api.clone();
        let server = MockServer::start(vec![
            Route::with_request("POST", "/token", status, move |request| {
                token_out.lock().unwrap().push(request.clone());
                token_body.clone()
            }),
            Route::with_request("GET", "/calendars/primary/events", 200, move |request| {
                api_out.lock().unwrap().push(request.clone());
                json!({"items": []}).to_string()
            }),
        ])
        .await;
        Self {
            server,
            tokens,
            api,
        }
    }

    async fn session(&self) -> Session {
        Session::start(factory_connector(&manifest(&self.server.base_url()))).await
    }
}

#[test]
fn parses_google_calendar_oauth2_example() {
    let spec = HttpSpec::from_toml_str(EXAMPLE, Path::new(".")).unwrap();
    let Some(AuthSpec::OAuth2(AuthConfig::Oauth2 {
        token_url,
        client_id,
        client_secret_env,
        refresh_token_env,
    })) = spec.auth
    else {
        panic!("expected shared OAuth2 config");
    };
    assert_eq!(token_url, "https://oauth2.googleapis.com/token");
    assert_eq!(client_id, "YOUR_CLIENT_ID.apps.googleusercontent.com");
    assert_eq!(client_secret_env, "GOOGLE_CLIENT_SECRET");
    assert_eq!(refresh_token_env, "GOOGLE_REFRESH_TOKEN");
}

#[test]
fn rejects_incomplete_or_invalid_oauth2_at_load_time() {
    for line in [
        "token_url = \"https://oauth2.googleapis.com/token\"\n",
        "client_id = \"YOUR_CLIENT_ID.apps.googleusercontent.com\"\n",
        "client_secret_env = \"GOOGLE_CLIENT_SECRET\"\n",
        "refresh_token_env = \"GOOGLE_REFRESH_TOKEN\"\n",
    ] {
        assert!(HttpSpec::from_toml_str(&EXAMPLE.replace(line, ""), Path::new(".")).is_err());
    }
    for url in ["not-a-url", "file:///token", "ftp://example.org/token"] {
        assert!(
            HttpSpec::from_toml_str(
                &EXAMPLE.replace("https://oauth2.googleapis.com/token", url),
                Path::new(".")
            )
            .is_err()
        );
    }
    assert!(
        HttpSpec::from_toml_str(&EXAMPLE.replace("GOOGLE_REFRESH_TOKEN", ""), Path::new("."))
            .is_err()
    );
}

#[tokio::test]
async fn refreshes_and_caches_token_through_bus() {
    let fixture = Fixture::start(
        200,
        json!({
            "access_token": "test-access", "expires_in": 3600, "token_type": "Bearer"
        })
        .to_string(),
    )
    .await;
    let mut spec =
        HttpSpec::from_toml_str(&manifest(&fixture.server.base_url()), Path::new(".")).unwrap();
    spec.headers
        .insert("authorization", "Bearer stale".parse().unwrap());
    let connector = HttpConnector::with_client(spec, Client::builder().no_proxy().build().unwrap());
    let session = Session::start(connector).await;
    for _ in 0..2 {
        assert_eq!(
            session.command().await.kind.as_str(),
            "google_calendar.event.events_listed"
        );
    }
    let tokens = fixture.tokens.lock().unwrap();
    assert_eq!(tokens.len(), 1, "second command reuses the token");
    assert_eq!(
        tokens[0].header("content-type"),
        Some("application/x-www-form-urlencoded")
    );
    let form: HashMap<String, String> =
        Url::parse(&format!("http://example.org/?{}", tokens[0].body))
            .unwrap()
            .query_pairs()
            .into_owned()
            .collect();
    assert_eq!(form["grant_type"], "refresh_token");
    assert_eq!(
        form["client_id"],
        "YOUR_CLIENT_ID.apps.googleusercontent.com"
    );
    assert_eq!(form["client_secret"], "test secret+&=");
    assert_eq!(form["refresh_token"], "test refresh+&=");
    assert_eq!(form.len(), 4);
    let api = fixture.api.lock().unwrap();
    assert_eq!(api.len(), 2);
    for request in api.iter() {
        assert_eq!(request.header("authorization"), Some("Bearer test-access"));
        assert_eq!(
            request
                .headers
                .iter()
                .filter(|(key, _)| key.eq_ignore_ascii_case("authorization"))
                .count(),
            1
        );
        assert!(!request.target.contains("test-access"));
    }
}

#[tokio::test]
async fn refreshes_tokens_inside_expiry_leeway() {
    let fixture = Fixture::start(
        200,
        json!({"access_token": "short-lived", "expires_in": 30}).to_string(),
    )
    .await;
    let session = fixture.session().await;
    for _ in 0..2 {
        assert_eq!(
            session.command().await.kind.as_str(),
            "google_calendar.event.events_listed"
        );
    }
    assert_eq!(fixture.tokens.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn missing_secret_emits_error_without_http_requests() {
    let fixture = Fixture::start(200, "{}".into()).await;
    let text = manifest(&fixture.server.base_url()).replace(
        "OCTO_HTTP_OAUTH_TEST_SECRET",
        "OCTO_HTTP_OAUTH_UNSET_SECRET",
    );
    let session = Session::start(factory_connector(&text)).await;
    let response = session.command().await;
    assert_eq!(response.kind.as_str(), "google_calendar.event.error");
    let payload = response.payload_as::<Value>().unwrap();
    assert_eq!(payload["http_status"], Value::Null);
    assert!(
        payload["message"]
            .as_str()
            .unwrap()
            .contains("OCTO_HTTP_OAUTH_UNSET_SECRET")
    );
    assert!(fixture.tokens.lock().unwrap().is_empty());
    assert!(fixture.api.lock().unwrap().is_empty());
}

#[tokio::test]
async fn token_failures_emit_sanitized_errors_and_can_be_retried_by_later_commands() {
    for (status, body, message) in [
        (
            400,
            r#"{"error":"invalid_grant","error_description":"test secret+&="}"#,
            "auth: OAuth2 token endpoint returned 400",
        ),
        (
            200,
            "invalid token response: test refresh+&=",
            "auth: invalid OAuth2 token response",
        ),
        (
            200,
            r#"{"expires_in":3600}"#,
            "auth: invalid OAuth2 token response",
        ),
        (
            200,
            r#"{"access_token":""}"#,
            "auth: invalid OAuth2 token response",
        ),
    ] {
        let fixture = Fixture::start(status, body.into()).await;
        let session = fixture.session().await;
        for _ in 0..2 {
            let response = session.command().await;
            assert_eq!(response.kind.as_str(), "google_calendar.event.error");
            let payload = response.payload_as::<Value>().unwrap();
            assert_eq!(payload["http_status"], Value::Null);
            assert_eq!(payload["message"], message);
        }
        assert_eq!(fixture.tokens.lock().unwrap().len(), 2);
        assert!(fixture.api.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn token_exchange_uses_connector_timeout() {
    let fixture = Fixture::start(200, "{}".into()).await;
    // Keep a socket open without serving requests so token exchange must time out.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let text = manifest(&fixture.server.base_url())
        .replace(
            &format!("{}/token", fixture.server.base_url()),
            &format!("http://{}/token", listener.local_addr().unwrap()),
        )
        .replace("request_ms = 10000", "request_ms = 20");
    let session = Session::start(factory_connector(&text)).await;
    let response = session.command().await;
    assert_eq!(response.kind.as_str(), "google_calendar.event.error");
    assert_eq!(
        response.payload_as::<Value>().unwrap()["message"],
        "auth: OAuth2 token request timed out"
    );
    assert!(fixture.api.lock().unwrap().is_empty());
}

#[tokio::test]
async fn header_auth_and_unauthenticated_manifests_keep_working() {
    let fixture = Fixture::start(200, "{}".into()).await;
    for auth in [
        "",
        r#"[connector.auth]
type = "bearer"
secret_var = "${secret.access}"
value_prefix = "Bearer "
[connector.secrets]
access = { env = "OCTO_HTTP_OAUTH_TEST_REFRESH" }
"#,
        r#"[connector.auth]
type = "api_key"
header = "api-key"
secret_var = "${secret.access}"
[connector.secrets]
access = { env = "OCTO_HTTP_OAUTH_TEST_REFRESH" }
"#,
    ] {
        let text = format!(
            r#"
[connector]
id = "google_calendar"
type = "http"
base_url = "{}"
{auth}
[[connector.endpoint]]
cmd_kind = "google_calendar.cmd.list_events"
method = "GET"
path = "/calendars/primary/events"
response_kind = "google_calendar.event.events_listed"
"#,
            fixture.server.base_url()
        );
        let session = Session::start(factory_connector(&text)).await;
        assert_eq!(
            session.command().await.kind.as_str(),
            "google_calendar.event.events_listed"
        );
    }
    assert!(fixture.tokens.lock().unwrap().is_empty());
    let requests = fixture.api.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].header("authorization"), None);
    assert_eq!(
        requests[1].header("authorization"),
        Some("Bearer test refresh+&=")
    );
    assert_eq!(requests[2].header("api-key"), Some("test refresh+&="));
}
