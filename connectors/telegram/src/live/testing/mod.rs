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

pub(super) struct Server {
    pub bot: Bot,
    pub requests: Arc<Mutex<Vec<(String, Value)>>>,
    pub started: Arc<Notify>,
    pub release: Arc<Notify>,
    task: JoinHandle<()>,
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

pub(super) async fn server(block_message: bool) -> Server {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let capture = requests.clone();
    let begin = started.clone();
    let finish = release.clone();
    let app = Router::new().fallback(post(move |uri: Uri, Json(body): Json<Value>| {
        let name = uri.path().to_ascii_lowercase();
        capture.lock().unwrap().push((name.clone(), body));
        let begin = begin.clone(); let finish = finish.clone();
        async move {
            let result = if name.ends_with("sendmessage") {
                begin.notify_one();
                if block_message { finish.notified().await; }
                json!({"message_id":17,"date":1,"chat":{"id":42,"type":"private","first_name":"Person"},"text":"status"})
            } else { json!(true) };
            Json(json!({"ok":true,"result":result}))
        }
    }));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = spawn(async move { serve(listener, app).await.unwrap() });
    Server {
        bot: Bot::new("123:FAKE").set_api_url(Url::parse(&format!("http://{address}")).unwrap()),
        requests,
        started,
        release,
        task,
    }
}
