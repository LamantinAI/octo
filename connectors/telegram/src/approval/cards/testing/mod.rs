use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    Json, Router,
    body::Bytes,
    http::{StatusCode, Uri},
    routing::post,
    serve,
};
use reqwest::Url;
use serde_json::json;
use teloxide::Bot;
use tokio::{net::TcpListener, spawn, sync::Notify, task::JoinHandle, time::timeout};

pub(super) struct Server {
    pub(super) bot: Bot,
    task: JoinHandle<()>,
    pub(super) requests: Arc<Mutex<Vec<(String, String)>>>,
    notify: Arc<Notify>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    pub(super) async fn wait(&self, method: &str, count: usize) -> Vec<String> {
        timeout(Duration::from_secs(2), async {
            loop {
                let notified = self.notify.notified();
                let found = self
                    .requests
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(name, _)| {
                        name.to_ascii_lowercase()
                            .ends_with(&method.to_ascii_lowercase())
                    })
                    .map(|(_, body)| body.clone())
                    .collect::<Vec<_>>();
                if found.len() >= count {
                    return found;
                }
                notified.await;
            }
        })
        .await
        .unwrap()
    }
}
pub(super) async fn server() -> Server {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let capture = requests.clone();
    let notify = Arc::new(Notify::new());
    let notify_in = notify.clone();
    let app=Router::new().fallback(post(move |uri:Uri,body:Bytes|{let name=uri.path().to_owned();capture.lock().unwrap().push((name.clone(),String::from_utf8_lossy(&body).into_owned()));notify_in.notify_one();async move{let result=if name.to_ascii_lowercase().ends_with("answercallbackquery"){json!(true)}else{json!({"message_id":17,"date":1,"chat":{"id":7,"type":"private","first_name":"Person"},"text":"card"})};(StatusCode::OK,Json(json!({"ok":true,"result":result})))}}));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
    let task = spawn(async move {
        serve(listener, app).await.unwrap();
    });
    Server {
        bot: Bot::new("123:LOCAL_TEST").set_api_url(url),
        task,
        requests,
        notify,
    }
}
