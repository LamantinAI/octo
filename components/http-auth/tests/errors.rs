use std::{env::set_var, error::Error, sync::Once, time::Duration};

use octo_http_auth::{AuthConfig, AuthError, HttpAuth};
use reqwest::Client;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    spawn,
    task::JoinHandle,
    time::timeout,
};

const CALL_TIMEOUT: Duration = Duration::from_secs(3);
const SECRETS: [&str; 4] = [
    "private-client-secret",
    "private-refresh-token",
    "private-access-token",
    "private-url-secret",
];

fn auth(token_url: &str, request_timeout: Duration) -> HttpAuth {
    static INIT: Once = Once::new();
    INIT.call_once(|| unsafe {
        set_var("OCTO_SHARED_AUTH_TEST_SECRET", SECRETS[0]);
        set_var("OCTO_SHARED_AUTH_TEST_REFRESH", SECRETS[1]);
    });
    HttpAuth::with_client(
        AuthConfig::Oauth2 {
            token_url: token_url.into(),
            client_id: "example-client".into(),
            client_secret_env: "OCTO_SHARED_AUTH_TEST_SECRET".into(),
            refresh_token_env: "OCTO_SHARED_AUTH_TEST_REFRESH".into(),
        },
        Client::builder()
            .no_proxy()
            .timeout(request_timeout)
            .build()
            .unwrap(),
    )
}

fn assert_sanitized(error: &AuthError) {
    let mut current: Option<&dyn Error> = Some(error);
    while let Some(cause) = current {
        for rendered in [cause.to_string(), format!("{cause:?}")] {
            for secret in SECRETS {
                assert!(!rendered.contains(secret), "error exposed a secret");
            }
            assert!(
                !rendered.contains("http://"),
                "error exposed an endpoint URL"
            );
        }
        current = cause.source();
    }
}

struct TokenEndpoint {
    url: String,
    task: JoinHandle<()>,
}

impl TokenEndpoint {
    async fn start(status: u16, body: &str, length: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "http://{}/token?secret={}",
            listener.local_addr().unwrap(),
            SECRETS[3]
        );
        let response = format!(
            "HTTP/1.1 {status} Test\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n{body}"
        );
        let task = spawn(async move {
            timeout(CALL_TIMEOUT, async {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut received = Vec::new();
                let mut buf = [0; 1024];
                loop {
                    let count = stream.read(&mut buf).await.unwrap();
                    assert!(count > 0, "client closed before sending request");
                    received.extend_from_slice(&buf[..count]);
                    if let Some(end) = received.windows(4).position(|w| w == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&received[..end]);
                        let length: usize = headers
                            .lines()
                            .filter_map(|line| line.split_once(':'))
                            .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                            .unwrap()
                            .1
                            .trim()
                            .parse()
                            .unwrap();
                        if received.len() >= end + 4 + length {
                            break;
                        }
                    }
                }
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.shutdown().await.unwrap();
            })
            .await
            .expect("mock token endpoint completed");
        });
        Self { url, task }
    }

    async fn error(&self) -> AuthError {
        timeout(CALL_TIMEOUT, auth(&self.url, CALL_TIMEOUT).credential())
            .await
            .expect("token exchange completed")
            .unwrap_err()
    }
}

impl Drop for TokenEndpoint {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test]
async fn refusal_retains_status_without_body_even_if_body_is_truncated() {
    let body = format!(
        r#"{{"error":"{}","error_description":"{} {}"}}"#,
        SECRETS[0], SECRETS[1], SECRETS[2]
    );
    for length in [body.len(), body.len() + 100] {
        let endpoint = TokenEndpoint::start(401, &body, length).await;
        let error = endpoint.error().await;
        assert_sanitized(&error);
        assert!(matches!(error, AuthError::TokenRefused { status: 401 }));
        assert_eq!(
            error.to_string(),
            "auth: OAuth2 token endpoint returned 401"
        );
    }
}

#[tokio::test]
async fn invalid_token_responses_retain_only_parse_category_and_location() {
    for (body, category) in [
        (
            format!(
                r#"{{"access_token":"{}","expires_in":"{}"}}"#,
                SECRETS[2], SECRETS[1]
            ),
            "Data",
        ),
        (format!("invalid JSON: {}", SECRETS[0]), "Syntax"),
        (format!("{{\"{}\":", SECRETS[2]), "Eof"),
    ] {
        let endpoint = TokenEndpoint::start(200, &body, body.len()).await;
        let error = endpoint.error().await;
        assert_sanitized(&error);
        assert!(matches!(error, AuthError::TokenParse(_)));
        let message = error.to_string();
        assert!(message.contains(category));
        assert!(message.contains("line 1, column"));
    }
}

#[tokio::test]
async fn truncated_success_response_does_not_expose_body_or_url() {
    let body = SECRETS[2];
    let endpoint = TokenEndpoint::start(200, body, body.len() + 100).await;
    let error = endpoint.error().await;
    assert_sanitized(&error);
    assert!(matches!(error, AuthError::TokenRequest(_)));
}

#[tokio::test]
async fn transport_failures_retain_category_without_url_or_nested_causes() {
    let error = auth("http://[private-url-secret", CALL_TIMEOUT)
        .credential()
        .await
        .unwrap_err();
    assert_sanitized(&error);
    assert!(error.to_string().contains("request construction"));

    // Keeping a socket bound without accepting guarantees a token request timeout.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/{}", listener.local_addr().unwrap(), SECRETS[3]);
    let error = timeout(
        CALL_TIMEOUT,
        auth(&url, Duration::from_millis(30)).credential(),
    )
    .await
    .expect("token exchange timed out")
    .unwrap_err();
    assert_sanitized(&error);
    assert!(error.to_string().contains("timeout"));
}
