use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{Json, Router, http::Uri, routing::post, serve};
use reqwest::Url;
use serde_json::{Value, json};
use teloxide::Bot;
use tokio::{
    net::TcpListener,
    spawn,
    sync::Notify,
    task::JoinHandle,
    time::{sleep, timeout},
};

pub(crate) struct Server {
    pub bot: Bot,
    pub requests: Arc<Mutex<Vec<(String, Value)>>>,
    task: JoinHandle<()>,
    pub ack_started: Arc<Notify>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    pub async fn wait(&self, method: &str, count: usize) {
        timeout(Duration::from_secs(2), async {
            loop {
                if self
                    .requests
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(name, _)| name.ends_with(method))
                    .count()
                    >= count
                {
                    return;
                }
                sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
    }
}
pub(crate) async fn server() -> Server {
    server_mode(false).await
}
pub(crate) async fn blocked_ack_server() -> Server {
    server_mode(true).await
}
async fn server_mode(block_ack: bool) -> Server {
    let ack_started = Arc::new(Notify::new());
    let started = ack_started.clone();
    let gate = Arc::new(Notify::new());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let capture = requests.clone();
    let app=Router::new().fallback(post(move |uri:Uri,Json(body):Json<Value>| {
        let name=uri.path().to_ascii_lowercase();
        capture.lock().unwrap().push((name.clone(),body));
        let started=started.clone(); let gate=gate.clone();
        async move {
            let result=if name.ends_with("sendmessage") {
                json!({"message_id":17,"date":1,"chat":{"id":7,"type":"private","first_name":"Person"},"text":"literal reply"})
            } else {
                if block_ack && name.ends_with("answercallbackquery") {started.notify_one();gate.notified().await;}
                json!(true)
            };
            Json(json!({"ok":true,"result":result}))
        }
    }));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = spawn(async move { serve(listener, app).await.unwrap() });
    Server {
        bot: Bot::new("9:FAKE").set_api_url(Url::parse(&format!("http://{address}")).unwrap()),
        requests,
        task,
        ack_started,
    }
}
