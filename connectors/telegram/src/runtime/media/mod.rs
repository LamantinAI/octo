use octo_core::Blob;
use teloxide::{Bot, types::Message};
use tracing::{info, warn};

use crate::{
    TelegramConnector,
    batch::Emit,
    inbound::{
        caption_with_saved, download_bytes, hms, image_document, inbox_name, video_caption,
        video_media, voice_media, with_reply,
    },
    source::MessageSource,
};

/// Called only after host admission and temporary-media rejection. Scope is pinned
/// before every await and checked again before saving any downloaded bytes.
pub(super) async fn receive_media(
    connector: &TelegramConnector,
    bot: &Bot,
    msg: &Message,
    source: &MessageSource,
    reply: &Option<String>,
) -> Option<(Emit, bool)> {
    if source.scope.as_ref().is_some_and(|s| s.ephemeral) {
        return None;
    }
    let chat = msg.chat.id.0.to_string();
    let allowed = || source.allowed(connector.policy.as_ref(), &connector.id, &chat);
    if !allowed() {
        return None;
    }
    if let Some(photo) = msg.photo().and_then(<[_]>::last) {
        match download_bytes(bot, &photo.file.id).await {
            Ok(bytes) => {
                if !allowed() {
                    return None;
                }
                info!(%chat,bytes=bytes.len(),"recv: photo");
                let saved = connector
                    .save_incoming(&inbox_name("photo.jpg"), &bytes)
                    .map_err(|e| warn!(error=%e,"failed to save incoming photo"))
                    .ok();
                let blob = Blob::new(bytes, "image/jpeg").with_filename("photo.jpg");
                let caption = caption_with_saved(
                    with_reply(reply, msg.caption().map(str::to_string)),
                    saved.as_deref(),
                );
                return Some((Emit::Image { blob, caption }, true));
            }
            Err(e) => warn!(error=%e,"telegram photo download failed"),
        }
    } else if let Some((doc, mime)) = image_document(msg) {
        match download_bytes(bot, &doc.file.id).await {
            Ok(bytes) => {
                if !allowed() {
                    return None;
                }
                info!(%chat,bytes=bytes.len(),%mime,"recv: image document");
                let name = doc.file_name.clone().unwrap_or_else(|| "image".into());
                let saved = connector
                    .save_incoming(&inbox_name(&name), &bytes)
                    .map_err(|e| warn!(error=%e,"failed to save incoming image document"))
                    .ok();
                let caption = caption_with_saved(
                    with_reply(reply, msg.caption().map(str::to_string)),
                    saved.as_deref(),
                );
                return Some((
                    Emit::Image {
                        blob: Blob::new(bytes, mime).with_filename(name),
                        caption,
                    },
                    false,
                ));
            }
            Err(e) => warn!(error=%e,"telegram document download failed"),
        }
    } else if let Some(audio) = voice_media(msg) {
        match download_bytes(bot, audio.file_id).await {
            Ok(bytes) => {
                if !allowed() {
                    return None;
                }
                info!(%chat,bytes=bytes.len(),secs=audio.duration_secs,"recv: voice");
                let path = connector
                    .save_incoming(&inbox_name(&audio.filename), &bytes)
                    .map_err(|e| warn!(error=%e,"failed to save incoming voice"))
                    .ok();
                return Some((
                    Emit::Audio {
                        blob: Blob::new(bytes, audio.mime).with_filename(audio.filename),
                        caption: with_reply(reply, msg.caption().map(str::to_string)),
                        duration_secs: Some(audio.duration_secs),
                        path,
                    },
                    false,
                ));
            }
            Err(e) => warn!(error=%e,"telegram voice download failed"),
        }
    } else if let Some(video) = video_media(msg) {
        match download_bytes(bot, video.file_id).await {
            Ok(bytes) => {
                if !allowed() {
                    return None;
                }
                let saved = connector
                    .save_incoming(&inbox_name(&video.filename), &bytes)
                    .map_err(|e| warn!(error=%e,"failed to save incoming video"))
                    .ok();
                info!(%chat,bytes=bytes.len(),secs=video.duration_secs,rel=saved.as_deref().unwrap_or("-"),"recv: video");
                let caption = video_caption(
                    &video,
                    with_reply(reply, msg.caption().map(str::to_string)),
                    saved.as_deref(),
                );
                let thumb = match video.thumbnail {
                    Some(t) => download_bytes(bot, &t.file.id).await.ok(),
                    None => None,
                };
                if !allowed() {
                    return None;
                }
                let emit = match thumb {
                    Some(bytes) => Emit::Image {
                        blob: Blob::new(bytes, "image/jpeg").with_filename("video-thumb.jpg"),
                        caption: Some(caption),
                    },
                    None => Emit::Text {
                        text: caption,
                        caption: None,
                    },
                };
                return Some((emit, false));
            }
            Err(e) => {
                if !allowed() {
                    return None;
                }
                warn!(error=%e,"telegram video download failed");
                return Some((
                    Emit::Text {
                        text: format!(
                            "[received a video ({}) but could not download it — Telegram caps bot downloads at 20 MB]",
                            hms(video.duration_secs)
                        ),
                        caption: None,
                    },
                    false,
                ));
            }
        }
    } else if let Some(doc) = msg.document() {
        let name = doc.file_name.clone().unwrap_or_else(|| "file".into());
        match download_bytes(bot, &doc.file.id).await {
            Ok(bytes) => {
                if !allowed() {
                    return None;
                }
                match connector.save_incoming(&name, &bytes) {
                    Ok(rel) => {
                        info!(%chat,%rel,bytes=bytes.len(),"recv: file");
                        return Some((
                            Emit::Text {
                                text: format!(
                                    "[received file `{name}` — saved to workspace path `{rel}`]"
                                ),
                                caption: None,
                            },
                            false,
                        ));
                    }
                    Err(e) => warn!(error=%e,"failed to save incoming file"),
                }
            }
            Err(e) => warn!(error=%e,"telegram file download failed"),
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::receive_media;
    use crate::runtime::pending::{MediaInput, MediaQueue};
    use crate::{
        IngressDecision, IngressScope, NativeFrame, TelegramConnector, TelegramHostPolicy,
        source::MessageSource,
    };
    use async_trait::async_trait;
    use axum::{
        Json, Router,
        body::Body,
        http::{Request, Response},
        routing::any,
        serve,
    };
    use octo_core::Envelope;
    use reqwest::Url;
    use serde_json::{from_value, json};
    use std::{
        fs::read_dir,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };
    use teloxide::{Bot, types::Message};
    use tempfile::tempdir;
    use tokio::{net::TcpListener, spawn, sync::Notify, time::timeout};
    struct Policy(Arc<AtomicBool>);
    #[async_trait]
    impl TelegramHostPolicy for Policy {
        async fn admit(&self, _: NativeFrame, _: Envelope) -> Result<IngressDecision, String> {
            Ok(IngressDecision::Ignore)
        }
        fn allow_publish(&self, _: &Envelope) -> bool {
            !self.0.load(Ordering::SeqCst)
        }
    }
    fn photo() -> Message {
        from_value(json!({"message_id":3,"date":1700000000,"chat":{"id":42,"type":"private","first_name":"Person"},"photo":[{"file_id":"image","file_unique_id":"stable-image","width":2,"height":2}]})).unwrap()
    }
    #[tokio::test]
    async fn temporary_media_never_starts_network_or_workspace_pipeline() {
        let count = Arc::new(AtomicUsize::new(0));
        let capture = count.clone();
        let app = Router::new().fallback(any(move || {
            capture.fetch_add(1, Ordering::SeqCst);
            async { Json(json!({"ok":false,"description":"must not be called"})) }
        }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = spawn(async move { serve(listener, app).await.unwrap() });
        let bot =
            Bot::new("123:FAKE").set_api_url(Url::parse(&format!("http://{address}")).unwrap());
        let root = tempdir().unwrap();
        let mut connector = TelegramConnector::new("telegram", "123:FAKE");
        Arc::get_mut(&mut connector).unwrap().workspace = Some(root.path().into());
        let source = MessageSource {
            scope: Some(IngressScope {
                key: "temporary".into(),
                epoch: "e".into(),
                ephemeral: true,
                cleanup_scope: None,
            }),
            ..MessageSource::default()
        };
        assert!(
            timeout(
                Duration::from_secs(1),
                receive_media(&connector, &bot, &photo(), &source, &None)
            )
            .await
            .unwrap()
            .is_none()
        );
        assert_eq!(count.load(Ordering::SeqCst), 0);
        assert_eq!(read_dir(root.path()).unwrap().count(), 0);
        server.abort();
    }
    #[tokio::test]
    async fn download_finishing_after_end_is_fenced_before_file_save() {
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let begin = started.clone();
        let finish = release.clone();
        let app=Router::new().fallback(any(move|request:Request<Body>|{let begin=begin.clone();let finish=finish.clone();async move{
            if request.uri().path().to_lowercase().contains("/getfile") {
                Response::builder().header("content-type","application/json").body(Body::from(json!({"ok":true,"result":{"file_id":"image","file_unique_id":"stable-image","file_path":"image.jpg"}}).to_string())).unwrap()
            }else{begin.notify_one();finish.notified().await;Response::new(Body::from("image bytes"))}
        }}));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = spawn(async move { serve(listener, app).await.unwrap() });
        let bot =
            Bot::new("123:FAKE").set_api_url(Url::parse(&format!("http://{address}")).unwrap());
        let closed = Arc::new(AtomicBool::new(false));
        let root = tempdir().unwrap();
        let mut connector = TelegramConnector::new("telegram", "123:FAKE");
        let instance = Arc::get_mut(&mut connector).unwrap();
        instance.workspace = Some(root.path().into());
        instance.policy = Some(Arc::new(Policy(closed.clone())));
        let mut queue = MediaQueue::default();
        queue.push(
            connector,
            bot,
            photo(),
            MediaInput {
                source: MessageSource::default(),
                chat: "42".into(),
                native_base: None,
                key: None,
                buffer: false,
                trust: None,
            },
            None,
        );
        // The polling loop can run another native-control branch while this
        // ordered queue is downloading; End closes the original pinned lease.
        let pending = spawn(async move { queue.next().await.unwrap().result });
        timeout(Duration::from_secs(2), started.notified())
            .await
            .unwrap();
        closed.store(true, Ordering::SeqCst);
        release.notify_one();
        assert!(
            timeout(Duration::from_secs(2), pending)
                .await
                .unwrap()
                .unwrap()
                .is_none()
        );
        assert_eq!(read_dir(root.path()).unwrap().count(), 0);
        server.abort();
    }
}
