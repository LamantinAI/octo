use std::time::Duration;

use reqwest::{Client, Method, Url, redirect::Policy};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tokio::{runtime::Handle, time::timeout};

use super::process::DriverProcess;
use crate::api::request_json;

const PROVIDER: &str = "ddg webdriver";
const ELEMENT_ID: &str = "element-6066-11e4-a52e-4f735466cecf";

#[derive(Clone)]
pub(super) struct WebDriver {
    client: Client,
    endpoint: Option<Url>,
    timeout: Duration,
}

#[derive(Clone)]
struct Connection {
    client: Client,
    endpoint: Url,
}

#[derive(Deserialize)]
struct Reply<T> {
    value: T,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct NewSession {
    session_id: String,
}

impl WebDriver {
    pub(super) fn new(endpoint: Option<&str>, timeout: Duration) -> Result<Self, String> {
        let endpoint = endpoint.map(parse_endpoint).transpose()?;
        let client = Client::builder()
            .no_proxy()
            .redirect(Policy::none())
            .timeout(timeout)
            .build()
            .map_err(|_| "ddg: could not build WebDriver client")?;
        Ok(Self {
            client,
            endpoint,
            timeout,
        })
    }

    pub(super) async fn start(&self) -> Result<Session, String> {
        let (endpoint, process) = match &self.endpoint {
            Some(endpoint) => (endpoint.clone(), None),
            None => {
                let (process, addr) = DriverProcess::start().await?;
                (parse_endpoint(&format!("http://{addr}"))?, Some(process))
            }
        };
        let connection = Connection {
            endpoint,
            client: self.client.clone(),
        };
        let millis = self.timeout.as_millis().min(u32::MAX as u128) as u32;
        let reply: NewSession = connection
            .send(
                Method::POST,
                &["session"],
                Some(json!({
                    "capabilities": {"alwaysMatch": {
                        "browserName": "firefox",
                        "pageLoadStrategy": "eager",
                        "moz:firefoxOptions": {"args": ["-headless"]},
                        "timeouts": {"pageLoad": millis, "script": millis}
                    }}
                })),
            )
            .await?;
        Ok(Session {
            connection,
            id: Some(reply.session_id),
            process,
        })
    }
}

fn parse_endpoint(endpoint: &str) -> Result<Url, String> {
    let endpoint = Url::parse(endpoint).map_err(|_| "ddg: invalid WebDriver URL")?;
    if !matches!(endpoint.scheme(), "http" | "https")
        || endpoint.host().is_none()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
    {
        return Err("ddg: WebDriver URL must be an HTTP(S) service address".into());
    }
    Ok(endpoint)
}

impl Connection {
    async fn send<T: DeserializeOwned>(
        &self,
        method: Method,
        segments: &[&str],
        body: Option<Value>,
    ) -> Result<T, String> {
        let mut url = self.endpoint.clone();
        url.path_segments_mut()
            .map_err(|_| "ddg: invalid WebDriver URL path")?
            .pop_if_empty()
            .extend(segments);
        let mut request = self.client.request(method, url);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let reply: Reply<T> = request_json(PROVIDER, request, "service rejected access").await?;
        Ok(reply.value)
    }

    async fn close(&self, id: &str) {
        let path = ["session", id];
        let request = self.send::<Value>(Method::DELETE, &path, None);
        let _ = timeout(Duration::from_secs(2), request).await;
    }
}

pub(super) struct Session {
    connection: Connection,
    id: Option<String>,
    process: Option<DriverProcess>,
}

impl Session {
    pub(super) async fn navigate(&self, url: &Url) -> Result<(), String> {
        self.post::<Value>(&["url"], json!({"url": url.as_str()}))
            .await?;
        Ok(())
    }

    pub(super) async fn evaluate<T: DeserializeOwned>(
        &self,
        script: &str,
        args: Value,
    ) -> Result<T, String> {
        self.post(
            &["execute", "sync"],
            json!({"script": script, "args": args}),
        )
        .await
    }

    pub(super) async fn click(&self, selector: &str) -> Result<(), String> {
        let element: Value = self
            .post(
                &["element"],
                json!({"using": "css selector", "value": selector}),
            )
            .await?;
        let id = element
            .get(ELEMENT_ID)
            .and_then(Value::as_str)
            .ok_or("ddg: WebDriver did not find the next-page button")?;
        self.post::<Value>(&["element", id, "click"], json!({}))
            .await?;
        Ok(())
    }

    async fn post<T: DeserializeOwned>(&self, tail: &[&str], body: Value) -> Result<T, String> {
        let id = self
            .id
            .as_deref()
            .ok_or("ddg: WebDriver session has closed")?;
        let mut path = vec!["session", id];
        path.extend_from_slice(tail);
        self.connection.send(Method::POST, &path, Some(body)).await
    }

    pub(super) async fn close(mut self) {
        if let Some(id) = self.id.take() {
            self.connection.close(&id).await;
        }
        if let Some(process) = self.process.take() {
            process.close().await;
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        if let (Some(id), Ok(runtime)) = (self.id.take(), Handle::try_current()) {
            let connection = self.connection.clone();
            let process = self.process.take();
            runtime.spawn(async move {
                connection.close(&id).await;
                if let Some(process) = process {
                    process.close().await;
                }
            });
        }
    }
}
