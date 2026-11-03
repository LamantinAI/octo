use std::time::Duration;

use reqwest::{Client as HttpClient, redirect::Policy};
use serde_json::{from_slice, json};

use crate::{AuthError, CLIENT_ID, RefreshResponse, Result, token_endpoint};

/// The refresh-token grant (`grant_type=refresh_token`), sent as JSON — matching the
/// codex flow. Returns the new tokens; a non-2xx is a hard error pointing at re-login.
pub(super) async fn refresh(refresh_token: &str, login: &str) -> Result<RefreshResponse> {
    refresh_at(token_endpoint(), refresh_token, login).await
}

async fn refresh_at(endpoint: &str, refresh_token: &str, login: &str) -> Result<RefreshResponse> {
    let client = HttpClient::builder()
        .timeout(Duration::from_secs(20))
        .connect_timeout(Duration::from_secs(5))
        .redirect(Policy::none())
        .build()
        .map_err(|_| AuthError("could not initialize token refresh client".into()))?;
    let mut response = client
        .post(endpoint)
        .json(&json!({
            "client_id": CLIENT_ID,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }))
        .send()
        .await
        .map_err(|_| {
            AuthError(format!(
                "token refresh request failed; run `{login}` if it persists"
            ))
        })?;
    let status = response.status();
    if !status.is_success() {
        return Err(AuthError(format!(
            "token refresh rejected ({status}); run `{login}`"
        )));
    }
    const MAXIMUM: usize = 64 * 1024;
    if response
        .content_length()
        .is_some_and(|length| length > MAXIMUM as u64)
    {
        return Err(AuthError(
            "token refresh response exceeds its size limit".into(),
        ));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| AuthError("could not read token refresh response".into()))?
    {
        if body.len() + chunk.len() > MAXIMUM {
            return Err(AuthError(
                "token refresh response exceeds its size limit".into(),
            ));
        }
        body.extend_from_slice(&chunk);
    }
    from_slice(&body).map_err(|_| AuthError("invalid token refresh response".into()))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        spawn,
        time::timeout,
    };

    use super::refresh_at;

    #[tokio::test]
    async fn refresh_rejection_and_malformed_or_large_responses_are_sanitized() {
        timeout(Duration::from_secs(5), async {
            for (status, body) in [("401 Unauthorized", "private-provider-token".to_owned()),
                ("200 OK", "private-provider-token".to_owned()), ("200 OK", "x".repeat(65_537))] {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let endpoint = format!("http://{}/token", listener.local_addr().unwrap());
                let server = spawn(async move {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let mut request = [0u8; 4096];
                    socket.read(&mut request).await.unwrap();
                    let response = format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                    let _ = socket.write_all(response.as_bytes()).await;
                });
                let error = match refresh_at(&endpoint, "synthetic-refresh", "example login").await {
                    Err(error) => error.to_string(), Ok(_) => panic!("must reject invalid response"),
                };
                assert!(!error.contains("private-provider-token"));
                server.await.unwrap();
            }
        }).await.expect("bounded refresh validation");
    }
}
