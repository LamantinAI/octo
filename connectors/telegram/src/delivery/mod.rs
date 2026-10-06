//! File delivery is a request/reply operation. Every validation, ACL and API
//! outcome produces a correlated result; a queued envelope is not a sent file.
use std::{path::PathBuf, sync::Arc, time::Duration};

use octo_core::{ConnectorContext, ConnectorId, Envelope, EventKind};
use serde_json::{Value, json};
use teloxide::{
    Bot, RequestError,
    payloads::{SendDocumentSetters, SendPhotoSetters, SendVoiceSetters},
    requests::Requester,
    types::{ChatId, InputFile},
};
use tokio::time::timeout;
use tracing::{info, warn};

use crate::{
    AclState, SEND_FILE_RESULT,
    control::outgoing_allowed,
    fs::{load_outgoing, workspace_root},
    outbound::{Outbound, outbound_kind},
    source::GroupSettings,
};

fn failed(status: &str, error: impl Into<String>) -> Value {
    json!({"ok":false,"status":status,"error":error.into()})
}

pub(super) async fn handle_file(
    bot: &Bot,
    workspace: &Option<PathBuf>,
    acl: &Option<Arc<AclState>>,
    groups: &GroupSettings,
    id: &ConnectorId,
    env: &Envelope,
    ctx: &ConnectorContext,
) {
    let result = deliver(bot, workspace, acl, groups, env).await;
    let mut response = Envelope::new(id.clone(), EventKind::from_static(SEND_FILE_RESULT), result)
        .with_correlation(env.id)
        .with_target(env.source.clone());
    if let Some(channel) = &env.channel {
        response = response.with_channel(channel.clone());
    }
    if let Err(error) = ctx.publish(response).await {
        warn!(%error, "file delivery acknowledgement failed");
    }
}

async fn deliver(
    bot: &Bot,
    workspace: &Option<PathBuf>,
    acl: &Option<Arc<AclState>>,
    groups: &GroupSettings,
    env: &Envelope,
) -> Value {
    let Some(params) = env.payload_as::<Value>() else {
        return failed("not_sent", "File command requires a JSON payload.");
    };
    let Some(path) = params
        .get("path")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    else {
        return failed("not_sent", "Missing workspace path.");
    };
    let chat = if let Some(explicit) = params.get("chat") {
        explicit.as_i64()
    } else {
        env.channel.as_ref().and_then(|c| c.as_str().parse().ok())
    };
    let Some(chat) = chat.filter(|id| *id != 0) else {
        return failed(
            "not_sent",
            "Missing or invalid destination chat. Bind a channel or supply chat.",
        );
    };
    if !outgoing_allowed(acl, groups, env) {
        return failed(
            "not_sent",
            "Destination is not allowed by the Telegram ACL.",
        );
    }
    let root = match workspace_root(workspace) {
        Ok(root) => root,
        Err(_) => return failed("not_sent", "Workspace is unavailable."),
    };
    let (bytes, name) = match load_outgoing(&root, path) {
        Ok(file) => file,
        Err(error) => return failed("not_sent", format!("Cannot read workspace file: {error}")),
    };
    let filename = params
        .get("filename")
        .and_then(Value::as_str)
        .unwrap_or(&name)
        .to_string();
    let caption = params.get("caption").and_then(Value::as_str);
    let kind = outbound_kind(&filename);
    let file = InputFile::memory(bytes).file_name(filename);
    let send = async {
        match kind {
            Outbound::Photo => {
                let mut req = bot.send_photo(ChatId(chat), file);
                if let Some(c) = caption {
                    req = req.caption(c);
                }
                req.await
            }
            Outbound::Voice => {
                let mut req = bot.send_voice(ChatId(chat), file);
                if let Some(c) = caption {
                    req = req.caption(c);
                }
                req.await
            }
            Outbound::Document => {
                let mut req = bot.send_document(ChatId(chat), file);
                if let Some(c) = caption {
                    req = req.caption(c);
                }
                req.await
            }
        }
    };
    match timeout(Duration::from_secs(90), send).await {
        Ok(Ok(message)) => {
            info!(chat, %path, kind = ?kind, "sent file");
            json!({"ok":true,"status":"sent","sent":path,"chat_id":message.chat.id.0,"message_id":message.id.0})
        }
        Ok(Err(
            error @ (RequestError::Api(_)
            | RequestError::RetryAfter(_)
            | RequestError::MigrateToChatId(_)),
        )) => failed("not_sent", error.to_string()),
        _ => failed(
            "unknown",
            "Telegram did not confirm delivery. The file may already have been sent; check the chat before retrying.",
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs::write,
        sync::{Mutex, RwLock},
    };

    use axum::{Json, Router, body::Bytes, http::StatusCode, routing::post, serve};
    use octo_core::{ChannelId, EventBus, Filter, InProcessBus, SubscribeOptions};
    use reqwest::Url;
    use tempfile::tempdir;
    use tokio::{net::TcpListener, spawn, task::JoinHandle};

    use super::*;
    use crate::{Acl, Role, SEND_FILE};

    struct Server {
        bot: Bot,
        task: JoinHandle<()>,
        requests: Arc<Mutex<Vec<String>>>,
    }
    impl Drop for Server {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn server(response: Value) -> Server {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let capture = requests.clone();
        let app = Router::new().fallback(post(move |body: Bytes| {
            capture
                .lock()
                .unwrap()
                .push(String::from_utf8_lossy(&body).into_owned());
            let response = response.clone();
            async move { (StatusCode::OK, Json(response)) }
        }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let task = spawn(async move {
            serve(listener, app).await.unwrap();
        });
        Server {
            bot: Bot::new("123:LOCAL_TEST").set_api_url(url),
            task,
            requests,
        }
    }

    fn command(payload: Value) -> Envelope {
        Envelope::new(
            ConnectorId::new("agent"),
            EventKind::new(SEND_FILE),
            payload,
        )
        .with_target(ConnectorId::new("telegram"))
        .with_channel(ChannelId::new("-42"))
    }

    async fn request(
        bot: &Bot,
        workspace: Option<PathBuf>,
        acl: Option<Arc<AclState>>,
        env: &Envelope,
    ) -> Value {
        let bus = Arc::new(InProcessBus::new(16));
        let mut results = bus
            .subscribe(
                Filter::by_kind(SEND_FILE_RESULT),
                SubscribeOptions::default(),
            )
            .await
            .unwrap();
        let ctx = ConnectorContext::new(Default::default(), bus);
        timeout(
            Duration::from_secs(3),
            handle_file(
                bot,
                &workspace,
                &acl,
                &GroupSettings::default(),
                &ConnectorId::new("telegram"),
                env,
                &ctx,
            ),
        )
        .await
        .unwrap();
        let reply = timeout(Duration::from_secs(1), results.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reply.correlation_id, Some(env.id));
        assert_eq!(reply.target.as_ref().unwrap().as_str(), "agent");
        reply.payload_as::<Value>().unwrap().clone()
    }

    #[tokio::test]
    async fn acknowledged_upload_preserves_destination_caption_and_message_id() {
        let server = server(json!({"ok":true,"result":{"message_id":17,"date":1,"chat":{"id":-77,"type":"group","title":"test"},"text":"file"}})).await;
        let dir = tempdir().unwrap();
        write(dir.path().join("photo.jpg"), b"test image").unwrap();
        let env = command(json!({"path":"photo.jpg","chat":-77,"caption":"caption"}));
        let result = request(&server.bot, Some(dir.path().into()), None, &env).await;
        assert_eq!(result["status"], "sent");
        assert_eq!(result["message_id"], 17);
        assert_eq!(result["chat_id"], -77);
        let requests = server.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].contains("-77"));
        assert!(requests[0].contains("caption"));
    }

    #[tokio::test]
    async fn validation_and_acl_failures_are_acknowledged_without_network_io() {
        let bot = Bot::new("123:LOCAL_TEST").set_api_url(Url::parse("http://127.0.0.1:1").unwrap());
        let dir = tempdir().unwrap();
        let mut acl = Acl::new();
        acl.insert(-42, Role::Trusted);
        let acl = Some(Arc::new(AclState {
            acl: RwLock::new(acl),
            path: None,
        }));
        let mut absent = command(json!({"path":"x"}));
        absent.channel = None;
        for env in [
            absent,
            command(json!({})),
            command(json!({"path":"missing"})),
            command(json!({"path":"x","chat":-77})),
            command(json!({"path":"../outside"})),
            command(json!({"path":"x","chat":"invalid"})),
        ] {
            let result = request(&bot, Some(dir.path().into()), acl.clone(), &env).await;
            assert_eq!(result["ok"], false);
            assert_eq!(result["status"], "not_sent");
        }
    }

    #[tokio::test]
    async fn telegram_rejection_and_unparseable_confirmation_have_distinct_outcomes() {
        let dir = tempdir().unwrap();
        write(dir.path().join("file.txt"), b"hello").unwrap();
        for (body, status) in [
            (
                json!({"ok":false,"error_code":400,"description":"Bad Request: chat not found"}),
                "not_sent",
            ),
            (json!({"unexpected":"response"}), "unknown"),
        ] {
            let server = server(body).await;
            let result = request(
                &server.bot,
                Some(dir.path().into()),
                None,
                &command(json!({"path":"file.txt"})),
            )
            .await;
            assert_eq!(result["status"], status);
            assert_eq!(server.requests.lock().unwrap().len(), 1);
            assert!(!result.to_string().contains("LOCAL_TEST"));
        }
    }
}
