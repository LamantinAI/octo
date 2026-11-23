#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use async_trait::async_trait;
    use axum::{Json, Router, http::Uri, routing::post, serve};
    use octo_core::{ConnectorContext, Envelope, InProcessBus};
    use reqwest::Url;
    use serde_json::{Value, json};
    use teloxide::Bot;
    use tokio::{
        net::TcpListener,
        spawn,
        sync::Notify,
        time::{sleep, timeout},
    };

    use crate::{
        CallbackDecision, CallbackFrame, CallbackLimits, IngressDecision, IngressScope,
        NativeFrame, TelegramConnector, TelegramHostPolicy,
    };

    struct Policy {
        state: AtomicUsize,
        entered: Notify,
        release: Notify,
        admitted: Notify,
        order: Mutex<Vec<String>>,
    }
    #[async_trait]
    impl TelegramHostPolicy for Policy {
        async fn admit(
            &self,
            frame: NativeFrame,
            envelope: Envelope,
        ) -> Result<IngressDecision, String> {
            let state = self.state.load(Ordering::SeqCst);
            self.order
                .lock()
                .unwrap()
                .push(format!("native-{}-{state}", frame.message_id));
            if frame.message_id == 19 {
                self.admitted.notify_one();
            }
            Ok(IngressDecision::Accept {
                envelope,
                scope: IngressScope {
                    key: format!("scope-{state}"),
                    epoch: "test".into(),
                    ephemeral: false,
                    cleanup_scope: None,
                },
            })
        }
        fn callback_limits(&self) -> CallbackLimits {
            CallbackLimits {
                max_concurrent: 1,
                timeout: Duration::from_millis(100),
            }
        }
        async fn callback(&self, frame: CallbackFrame) -> Result<CallbackDecision, String> {
            self.order
                .lock()
                .unwrap()
                .push(format!("start-{}", frame.data));
            if frame.data == "B" {
                self.entered.notify_one();
                self.release.notified().await;
            }
            self.state
                .store(if frame.data == "B" { 2 } else { 3 }, Ordering::SeqCst);
            self.order
                .lock()
                .unwrap()
                .push(format!("commit-{}", frame.data));
            CallbackDecision::new(Some("Selected".into()), false, vec![])
        }
    }
    fn callback(update: u32, data: &str) -> Value {
        json!({"update_id":update,"callback_query":{"id":format!("query-{data}"),"from":{"id":7,"is_bot":false,"first_name":"Human"},"chat_instance":"instance","data":data,"message":{"message_id":17,"date":1,"from":{"id":9,"is_bot":true,"first_name":"Bot"},"chat":{"id":7,"type":"private","first_name":"Human"},"text":"old list"}}})
    }
    fn message(update: u32, id: i32) -> Value {
        json!({"update_id":update,"message":{"message_id":id,"date":update,"from":{"id":7,"is_bot":false,"first_name":"Human"},"chat":{"id":7,"type":"private","first_name":"Human"},"text":"ordinary next input"}})
    }
    #[tokio::test]
    async fn polling_orders_callbacks_and_native_input_even_when_acknowledgements_never_return() {
        let ack_gate = Arc::new(Notify::new());
        let ack_count = Arc::new(AtomicUsize::new(0));
        let observed_acks = ack_count.clone();
        let served = Arc::new(AtomicBool::new(false));
        let capture = served.clone();
        let updates = json!([
            callback(1, "B"),
            message(2, 18),
            callback(3, "C"),
            message(4, 19)
        ]);
        let app=Router::new().fallback(post(move |uri:Uri,Json(_body):Json<Value>| {
            let method=uri.path().to_ascii_lowercase();let served=capture.clone();let updates=updates.clone();let ack_gate=ack_gate.clone();let acks=ack_count.clone();
            async move {
                let result=if method.ends_with("getme") {json!({"id":9,"is_bot":true,"first_name":"Bot","username":"test_bot","can_join_groups":true,"can_read_all_group_messages":false,"supports_inline_queries":false,"has_main_web_app":false})}
                    else if method.ends_with("answercallbackquery") {acks.fetch_add(1,Ordering::SeqCst);ack_gate.notified().await;json!(true)}
                    else if method.ends_with("getwebhookinfo") {json!({"url":"","has_custom_certificate":false,"pending_update_count":0})}
                    else if method.ends_with("getupdates") {
                        if !served.swap(true,Ordering::SeqCst) {updates} else {sleep(Duration::from_millis(100)).await;json!([])}
                    } else {json!(true)};
                Json(json!({"ok":true,"result":result}))
            }
        }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = spawn(async move { serve(listener, app).await.unwrap() });
        let bot = Bot::new("9:FAKE").set_api_url(Url::parse(&format!("http://{address}")).unwrap());
        let policy = Arc::new(Policy {
            state: AtomicUsize::new(1),
            entered: Notify::new(),
            release: Notify::new(),
            admitted: Notify::new(),
            order: Mutex::new(vec![]),
        });
        let mut connector = TelegramConnector::new("telegram", "9:FAKE");
        Arc::get_mut(&mut connector).unwrap().policy = Some(policy.clone());
        let ctx = ConnectorContext::new(Default::default(), Arc::new(InProcessBus::new(32)));
        let owned = ctx.clone();
        let task = spawn(async move { connector.run_with_bot(owned, bot).await });
        timeout(Duration::from_secs(2), policy.entered.notified())
            .await
            .unwrap();
        assert_eq!(*policy.order.lock().unwrap(), vec!["start-B"]);
        policy.release.notify_one();
        timeout(Duration::from_secs(2), policy.admitted.notified())
            .await
            .unwrap();
        assert_eq!(
            *policy.order.lock().unwrap(),
            vec![
                "start-B",
                "commit-B",
                "native-18-2",
                "start-C",
                "commit-C",
                "native-19-3"
            ]
        );
        assert_eq!(observed_acks.load(Ordering::SeqCst), 2);
        ctx.shutdown.cancel();
        timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        server.abort();
    }
}
