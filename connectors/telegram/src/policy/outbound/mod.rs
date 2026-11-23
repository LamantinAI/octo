//! Destination authorization uses the same resolved chat as native delivery.
use std::sync::Arc;

use octo_core::Envelope;
use serde_json::Value;

use crate::{
    AclState, SEND_FILE, TelegramHostPolicy, VerifiedBot, control::outgoing_allowed,
    source::GroupSettings,
};

pub(crate) fn destination(envelope: &Envelope) -> Option<i64> {
    let explicit = (envelope.kind.as_str() == SEND_FILE)
        .then(|| {
            envelope
                .payload_as::<Value>()
                .and_then(|payload| payload.get("chat"))
        })
        .flatten();
    let chat = match explicit {
        Some(value) => value.as_i64(),
        None => envelope
            .channel
            .as_ref()
            .and_then(|channel| channel.as_str().parse().ok()),
    };
    chat.filter(|id| *id != 0)
}

pub(crate) async fn outbound_allowed(
    policy: Option<&Arc<dyn TelegramHostPolicy>>,
    bot: &VerifiedBot,
    chat: i64,
    envelope: &Envelope,
    acl: &Option<Arc<AclState>>,
    groups: &GroupSettings,
) -> bool {
    if destination(envelope) != Some(chat) {
        return false;
    }
    match policy {
        Some(policy) => match policy.authorize_outbound(bot.clone(), chat, envelope).await {
            Ok(Some(allowed)) => allowed,
            Ok(None) => outgoing_allowed(acl, groups, envelope),
            Err(_) => false,
        },
        None => outgoing_allowed(acl, groups, envelope),
    }
}

#[cfg(test)]
mod tests {
    use super::{destination, outbound_allowed};
    use crate::{
        Acl, AclState, IngressDecision, NativeFrame, Role, SEND_FILE, TelegramHostPolicy,
        VerifiedBot, source::GroupSettings,
    };
    use async_trait::async_trait;
    use octo_core::{ChannelId, ConnectorId, Envelope, EventKind};
    use serde_json::json;
    use std::sync::{Arc, Mutex, RwLock};

    struct Host {
        decision: Result<Option<bool>, String>,
        seen: Mutex<Vec<i64>>,
    }
    #[async_trait]
    impl TelegramHostPolicy for Host {
        async fn admit(&self, _: NativeFrame, _: Envelope) -> Result<IngressDecision, String> {
            Ok(IngressDecision::Ignore)
        }
        async fn authorize_outbound(
            &self,
            bot: VerifiedBot,
            chat: i64,
            _: &Envelope,
        ) -> Result<Option<bool>, String> {
            assert_eq!(bot.bot_id, 9);
            self.seen.lock().unwrap().push(chat);
            self.decision.clone()
        }
    }
    fn bot() -> VerifiedBot {
        VerifiedBot {
            connector: ConnectorId::new("telegram"),
            bot_id: 9,
            username: "test_bot".into(),
        }
    }
    fn envelope() -> Envelope {
        Envelope::new(
            ConnectorId::new("host"),
            EventKind::new("chat.reply"),
            "report".to_owned(),
        )
        .with_target(ConnectorId::new("telegram"))
        .with_channel(ChannelId::new("7"))
    }
    fn acl(listed: bool) -> Option<Arc<AclState>> {
        let mut acl = Acl::new();
        if listed {
            acl.insert(7, Role::Owner);
        }
        Some(Arc::new(AclState {
            acl: RwLock::new(acl),
            path: None,
        }))
    }
    #[tokio::test]
    async fn host_authorizes_unlisted_delivery_and_denies_revoked_legacy_owner() {
        let groups = GroupSettings::default();
        for (decision, listed, expected) in [
            (Ok(Some(true)), false, true),
            (Ok(Some(false)), true, false),
            (Ok(None), true, true),
            (Ok(None), false, false),
            (Err("private failure".into()), true, false),
        ] {
            let policy: Arc<dyn TelegramHostPolicy> = Arc::new(Host {
                decision,
                seen: Mutex::new(vec![]),
            });
            assert_eq!(
                outbound_allowed(Some(&policy), &bot(), 7, &envelope(), &acl(listed), &groups)
                    .await,
                expected
            );
        }
    }
    #[tokio::test]
    async fn file_override_is_exact_and_invalid_override_never_falls_back_to_channel() {
        let host = Arc::new(Host {
            decision: Ok(Some(true)),
            seen: Mutex::new(vec![]),
        });
        let policy: Arc<dyn TelegramHostPolicy> = host.clone();
        let file = Envelope::new(
            ConnectorId::new("host"),
            EventKind::new(SEND_FILE),
            json!({"path":"report.txt","chat":77}),
        )
        .with_channel(ChannelId::new("7"));
        assert_eq!(destination(&file), Some(77));
        assert!(
            !outbound_allowed(
                Some(&policy),
                &bot(),
                7,
                &file,
                &acl(true),
                &GroupSettings::default()
            )
            .await
        );
        assert!(
            outbound_allowed(
                Some(&policy),
                &bot(),
                77,
                &file,
                &acl(false),
                &GroupSettings::default()
            )
            .await
        );
        assert_eq!(*host.seen.lock().unwrap(), vec![77]);
        let file = Envelope::new(
            ConnectorId::new("host"),
            EventKind::new(SEND_FILE),
            json!({"chat":"invalid"}),
        )
        .with_channel(ChannelId::new("7"));
        assert_eq!(destination(&file), None);
        assert!(
            !outbound_allowed(
                Some(&policy),
                &bot(),
                7,
                &file,
                &acl(true),
                &GroupSettings::default()
            )
            .await
        );
        assert_eq!(*host.seen.lock().unwrap(), vec![77]);
    }
}
