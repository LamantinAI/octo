//! A tiny dependency-free mock HTTP/1.1 server for connector tests.
//!
//! Routes are matched on `(method, path)` with the query string stripped. Each
//! route's handler receives the request body and returns the response body.
//! One request per connection (`Connection: close`), which is all `reqwest`
//! needs. Not a general-purpose server — just enough to drive deterministic
//! connector round-trips without pulling in `wiremock`.

#![allow(dead_code)]

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

type Handler = Box<dyn Fn(&str) -> String + Send + Sync>;

/// One mock route: an exact method + path, a status, and a body-producing handler.
pub struct Route {
    method: String,
    path: String,
    status: u16,
    handler: Handler,
}

impl Route {
    pub fn new(
        method: &str,
        path: &str,
        status: u16,
        handler: impl Fn(&str) -> String + Send + Sync + 'static,
    ) -> Self {
        Self {
            method: method.to_ascii_uppercase(),
            path: path.to_string(),
            status,
            handler: Box::new(handler),
        }
    }
}

pub struct MockServer {
    base_url: String,
}

impl MockServer {
    /// Bind to an ephemeral port and start serving `routes` in the background.
    pub async fn start(routes: Vec<Route>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
        let addr = listener.local_addr().unwrap();
        let routes = Arc::new(routes);

        tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    break;
                };
                let routes = Arc::clone(&routes);
                tokio::spawn(async move {
                    let _ = serve_one(socket, &routes).await;
                });
            }
        });

        Self {
            base_url: format!("http://{addr}"),
        }
    }

    pub fn base_url(&self) -> String {
        self.base_url.clone()
    }
}

async fn serve_one(mut socket: TcpStream, routes: &[Route]) -> std::io::Result<()> {
    let (method, target, body) = read_request(&mut socket).await?;
    let path = target.split('?').next().unwrap_or(&target);

    let (status, resp_body) = match routes
        .iter()
        .find(|r| r.method == method && r.path == path)
    {
        Some(route) => (route.status, (route.handler)(&body)),
        None => (404, format!("no mock route for {method} {path}")),
    };

    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "OK",
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n{resp_body}",
        resp_body.len()
    );
    socket.write_all(response.as_bytes()).await?;
    socket.flush().await?;
    Ok(())
}

/// Read one HTTP request: the method, the request target, and the body.
async fn read_request(socket: &mut TcpStream) -> std::io::Result<(String, String, String)> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 1024];

    // Read until the end of headers.
    let header_end = loop {
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            break pos;
        }
        let n = socket.read(&mut chunk).await?;
        if n == 0 {
            break buf.len();
        }
        buf.extend_from_slice(&chunk[..n]);
    };

    let header_text = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let mut lines = header_text.lines();
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_ascii_uppercase();
    let target = parts.next().unwrap_or_default().to_string();

    let content_length = lines
        .filter_map(|l| l.split_once(':'))
        .find(|(k, _)| k.trim().eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.trim().parse::<usize>().ok())
        .unwrap_or(0);

    // Body bytes already read past the header terminator.
    let body_start = header_end + 4;
    let mut body = buf[body_start.min(buf.len())..].to_vec();
    while body.len() < content_length {
        let n = socket.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..n]);
    }

    Ok((method, target, String::from_utf8_lossy(&body).to_string()))
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|w| w == needle)
}
