//! Local protocol fixtures: no providers, credentials or external network.
use std::{collections::BTreeMap, sync::Arc, sync::Mutex, time::Duration};

use axum::{
    Router, extract::Request, http::StatusCode, middleware::Next, middleware::from_fn,
    response::Response, serve,
};
use rmcp::{
    ErrorData, ServerHandler, model::CallToolRequestParams, model::CallToolResult, model::Content,
    model::ListToolsResult, model::PaginatedRequestParams, model::ServerCapabilities,
    model::ServerInfo, model::Tool, service::RequestContext, service::RoleServer,
    transport::streamable_http_server::StreamableHttpServerConfig,
    transport::streamable_http_server::StreamableHttpService,
    transport::streamable_http_server::session::local::LocalSessionManager,
};
use serde_json::json;
use tokio::{net::TcpListener, spawn, sync::Notify, task::JoinHandle, time::sleep};

use crate::Credentials;

#[derive(Default)]
pub struct State {
    pub calls: Vec<String>,
    pub schema: u8,
    pub repeated_cursor: bool,
    pub duplicate: bool,
    pub empty_pages: bool,
    pub result_size: usize,
    pub definition_size: usize,
    pub slow_started: Arc<Notify>,
    pub root_started: Arc<Notify>,
    pub child_started: Arc<Notify>,
    pub child_release: Arc<Notify>,
    pub connections: usize,
    pub generations: Vec<(String, usize)>,
}
#[derive(Clone, Default)]
pub struct Fake(pub Arc<Mutex<State>>, pub usize);
impl ServerHandler for Fake {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
    }
    async fn list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let state = self.0.lock().unwrap();
        if state.repeated_cursor || state.empty_pages {
            let cursor = if state.repeated_cursor {
                "same".into()
            } else {
                request
                    .and_then(|params| params.cursor)
                    .and_then(|value| value.parse::<usize>().ok())
                    .map(|n| (n + 1).to_string())
                    .unwrap_or("1".into())
            };
            return Ok(ListToolsResult {
                tools: vec![],
                next_cursor: Some(cursor),
                ..Default::default()
            });
        }
        let first = request.and_then(|params| params.cursor).is_none();
        let names = if first {
            vec!["echo"]
        } else if state.duplicate {
            vec!["echo"]
        } else {
            vec!["slow", "result", "error"]
        };
        let schema = state.schema;
        let description = if state.definition_size > 0 {
            "x".repeat(state.definition_size)
        } else {
            "local fixture tool".into()
        };
        Ok(ListToolsResult{tools:names.into_iter().map(|name|Tool::new(name,description.clone(),json!({"type":"object","properties":{"edition":{"const":schema}},"additionalProperties":true}).as_object().unwrap().clone())).collect(),next_cursor:first.then(||"page2".into()),..Default::default()})
    }
    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let name = request.name.to_string();
        let size = {
            let mut state = self.0.lock().unwrap();
            state.calls.push(name.clone());
            state.result_size
        };
        let lane = request
            .arguments
            .as_ref()
            .and_then(|args| args.get("lane"))
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .to_owned();
        let (root, child, release) = {
            let mut state = self.0.lock().unwrap();
            state.generations.push((lane.clone(), self.1));
            (
                state.root_started.clone(),
                state.child_started.clone(),
                state.child_release.clone(),
            )
        };
        if lane == "root" {
            root.notify_one();
        }
        if lane == "child" {
            child.notify_one();
            release.notified().await;
        }
        if name == "slow" {
            self.0.lock().unwrap().slow_started.notify_one();
            sleep(Duration::from_millis(200)).await;
        }
        if name == "error" {
            return Ok(CallToolResult::error(vec![Content::text(
                "fixture tool rejection",
            )]));
        }
        if name == "result" {
            let mut result = CallToolResult::structured(
                json!({"nested":{"value":7},"payload":"x".repeat(size)}),
            );
            result.content.push(Content::image("aW1hZ2U=", "image/png"));
            return Ok(result);
        }
        Ok(CallToolResult::structured(
            json!({"name":name,"arguments":request.arguments.unwrap_or_default()}),
        ))
    }
}
pub struct HttpServer {
    pub url: String,
    pub task: JoinHandle<()>,
}
impl Drop for HttpServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn bearer(request: Request, next: Next) -> Result<Response, StatusCode> {
    if request
        .headers()
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        != Some("Bearer fixture-secret-value")
    {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(next.run(request).await)
}
pub async fn http(fake: Fake, auth: bool) -> HttpServer {
    serve_http(fake, auth, false).await
}
pub async fn stateful(fake: Fake) -> HttpServer {
    serve_http(fake, false, true).await
}
async fn serve_http(fake: Fake, auth: bool, stateful: bool) -> HttpServer {
    let mut config = StreamableHttpServerConfig::default();
    config.stateful_mode = stateful;
    config.json_response = true;
    let service = StreamableHttpService::new(
        move || {
            let generation = {
                let mut state = fake.0.lock().unwrap();
                state.connections += 1;
                state.connections
            };
            Ok(Fake(fake.0.clone(), generation))
        },
        Arc::new(LocalSessionManager::default()),
        config,
    );
    let app = Router::new().nest_service("/nested/mcp", service);
    let app = if auth {
        app.layer(from_fn(bearer))
    } else {
        app
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/nested/mcp", listener.local_addr().unwrap());
    let task = spawn(async move {
        serve(listener, app).await.unwrap();
    });
    HttpServer { url, task }
}
pub struct Secrets(pub BTreeMap<String, String>);
impl Credentials for Secrets {
    fn resolve(&self, reference: &str) -> Option<String> {
        self.0.get(reference).cloned()
    }
}
