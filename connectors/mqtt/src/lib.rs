//! `octo-connector-mqtt` — an input-only connector that bridges an MQTT broker's
//! topics into bus envelopes.
//!
//! This makes the runtime's canonical `mqtt.factory.*` example real: a message on
//! topic `factory/temperature` becomes an envelope of kind
//! `mqtt.factory.temperature` (the `kind_prefix` plus the topic with `/`→`.`),
//! carrying the broker topic on its `channel`. The body is decoded per the
//! subscription's `payload` mode — `text` (a `String`), `json` (a
//! `serde_json::Value`, so a reflex router can introspect it), or `bytes` (a
//! [`Blob`]).
//!
//! The poll loop mirrors the Telegram connector's inbound loop: on a broker/poll
//! error it returns `Err`, handing reconnection to the runtime supervisor's
//! [`RestartPolicy`](octo_core::RestartPolicy) backoff — the always-on tentacle
//! keeps trying.
//!
//! `rumqttc` is a heavy tree, so this crate is excluded from the workspace's
//! `default-members`; build/test it with `--workspace` (or `-p
//! octo-connector-mqtt`). Live runs need a broker (e.g. `mosquitto`); the
//! topic→envelope mapping is covered by deterministic unit tests that need no
//! broker.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use octo_core::{
    Blob, ChannelId, Connector, ConnectorCapabilities, ConnectorContext, ConnectorId, Envelope,
    EventKind, OctoError, OctoResult,
};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use serde::Deserialize;
use serde_json::Value;

/// How to decode an MQTT message body into an envelope payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PayloadFormat {
    /// UTF-8 text → `String` (lossy).
    #[default]
    Text,
    /// JSON → `serde_json::Value` (`Null` on parse failure).
    Json,
    /// Raw bytes → [`Blob`] (`application/octet-stream`).
    Bytes,
}

/// One topic subscription and how it maps to envelopes.
#[derive(Debug, Clone)]
pub struct MqttSub {
    /// MQTT topic filter (may contain `+` / `#` wildcards).
    pub topic: String,
    /// MQTT QoS level (0/1/2). Defaults to 0.
    pub qos: u8,
    /// Event-kind prefix; the kind is `{prefix}.{topic-with-slashes-as-dots}`.
    pub kind_prefix: String,
    /// Body decoding mode.
    pub payload: PayloadFormat,
}

impl MqttSub {
    /// A subscription to `topic`, defaulting to QoS 0, `mqtt` prefix, text body.
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            qos: 0,
            kind_prefix: "mqtt".to_string(),
            payload: PayloadFormat::Text,
        }
    }

    pub fn with_qos(mut self, qos: u8) -> Self {
        self.qos = qos;
        self
    }

    pub fn with_kind_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.kind_prefix = prefix.into();
        self
    }

    pub fn with_payload(mut self, payload: PayloadFormat) -> Self {
        self.payload = payload;
        self
    }
}

/// An input-only MQTT subscriber connector.
pub struct MqttConnector {
    id: ConnectorId,
    capabilities: ConnectorCapabilities,
    host: String,
    port: u16,
    client_id: String,
    keep_alive: Duration,
    credentials: Option<(String, String)>,
    subs: Vec<MqttSub>,
}

impl MqttConnector {
    /// Build a connector against `host:port` with the given subscriptions.
    pub fn new(
        id: impl Into<String>,
        host: impl Into<String>,
        port: u16,
        subs: Vec<MqttSub>,
    ) -> Arc<Self> {
        let id = ConnectorId::new(id);
        // Advertise one emit glob per distinct prefix (informational).
        let mut emit: Vec<EventKind> = Vec::new();
        for s in &subs {
            let glob = format!("{}.**", s.kind_prefix);
            if !emit.iter().any(|k| k.as_str() == glob) {
                emit.push(EventKind::new(glob));
            }
        }
        let capabilities = ConnectorCapabilities::input_only().with_emit_kinds(emit);
        let client_id = format!("octo-{}", id.as_str());
        Arc::new(Self {
            id,
            capabilities,
            host: host.into(),
            port,
            client_id,
            keep_alive: Duration::from_secs(30),
            credentials: None,
            subs,
        })
    }

    pub fn with_client_id(mut self: Arc<Self>, client_id: impl Into<String>) -> Arc<Self> {
        Arc::get_mut(&mut self)
            .expect("with_client_id before sharing")
            .client_id = client_id.into();
        self
    }

    pub fn with_credentials(
        mut self: Arc<Self>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Arc<Self> {
        Arc::get_mut(&mut self)
            .expect("with_credentials before sharing")
            .credentials = Some((username.into(), password.into()));
        self
    }

    /// Find the first subscription whose filter matches an incoming topic.
    fn match_sub(&self, topic: &str) -> Option<&MqttSub> {
        self.subs.iter().find(|s| topic_matches(&s.topic, topic))
    }
}

#[async_trait]
impl Connector for MqttConnector {
    fn id(&self) -> &ConnectorId {
        &self.id
    }

    fn capabilities(&self) -> &ConnectorCapabilities {
        &self.capabilities
    }

    async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
        let mut opts = MqttOptions::new(self.client_id.clone(), self.host.clone(), self.port);
        opts.set_keep_alive(self.keep_alive);
        if let Some((user, pass)) = &self.credentials {
            opts.set_credentials(user.clone(), pass.clone());
        }

        let (client, mut eventloop) = AsyncClient::new(opts, 64);
        for sub in &self.subs {
            client
                .subscribe(sub.topic.clone(), qos_from(sub.qos))
                .await
                .map_err(|e| OctoError::Connector(format!("mqtt subscribe {}: {e}", sub.topic)))?;
        }
        tracing::info!(connector = %self.id, host = %self.host, port = self.port, subs = self.subs.len(), "mqtt connected");

        loop {
            tokio::select! {
                event = eventloop.poll() => match event {
                    Ok(Event::Incoming(Packet::Publish(p))) => {
                        match self.match_sub(&p.topic) {
                            Some(sub) => {
                                let env = map_publish(&self.id, sub, &p.topic, p.payload);
                                if let Err(e) = ctx.publish(env).await {
                                    tracing::warn!(connector = %self.id, topic = %p.topic, error = %e, "failed to publish mqtt envelope");
                                }
                            }
                            None => tracing::debug!(connector = %self.id, topic = %p.topic, "no matching subscription; ignored"),
                        }
                    }
                    Ok(_) => {} // connection acks, pings, etc.
                    // A poll error → return Err so the supervisor reconnects per RestartPolicy.
                    Err(e) => return Err(OctoError::Connector(format!("mqtt eventloop: {e}"))),
                },
                _ = ctx.shutdown.cancelled() => return Ok(()),
            }
        }
    }
}

/// Map an MQTT publish to an envelope: kind from `{prefix}.{topic}`, channel from
/// the topic, payload decoded per the subscription's mode.
pub fn map_publish(
    source: &ConnectorId,
    sub: &MqttSub,
    topic: &str,
    payload: bytes::Bytes,
) -> Envelope {
    let kind = topic_to_kind(&sub.kind_prefix, topic);
    let env = match sub.payload {
        PayloadFormat::Text => Envelope::new(
            source.clone(),
            kind,
            String::from_utf8_lossy(&payload).into_owned(),
        ),
        PayloadFormat::Json => {
            let value: Value = serde_json::from_slice(&payload).unwrap_or(Value::Null);
            Envelope::new(source.clone(), kind, value)
        }
        PayloadFormat::Bytes => Envelope::new(
            source.clone(),
            kind,
            Blob::new(payload, "application/octet-stream"),
        ),
    };
    env.with_channel(ChannelId::new(topic.to_string()))
}

/// `factory/temperature` + prefix `mqtt` → `mqtt.factory.temperature`.
fn topic_to_kind(prefix: &str, topic: &str) -> EventKind {
    EventKind::new(format!("{prefix}.{}", topic.replace('/', ".")))
}

/// MQTT topic-filter matching: `+` matches one level, `#` matches the remaining
/// tail (including zero levels).
fn topic_matches(filter: &str, topic: &str) -> bool {
    let f: Vec<&str> = filter.split('/').collect();
    let t: Vec<&str> = topic.split('/').collect();
    for (i, seg) in f.iter().enumerate() {
        match *seg {
            "#" => return true,
            "+" => {
                if i >= t.len() {
                    return false;
                }
            }
            literal => {
                if i >= t.len() || t[i] != literal {
                    return false;
                }
            }
        }
    }
    f.len() == t.len()
}

fn qos_from(level: u8) -> QoS {
    match level {
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtMostOnce,
    }
}

// ─── TOML factory ───────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct RawManifest {
    connector: ConnectorTable,
}

#[derive(Debug, Deserialize)]
struct ConnectorTable {
    host: String,
    #[serde(default = "default_port")]
    port: u16,
    client_id: Option<String>,
    keep_alive_secs: Option<u64>,
    username: Option<String>,
    /// Name of an env var holding the password (kept out of the manifest).
    password_env: Option<String>,
    #[serde(default, rename = "sub")]
    subs: Vec<SubTable>,
}

fn default_port() -> u16 {
    1883
}

#[derive(Debug, Deserialize)]
struct SubTable {
    topic: String,
    #[serde(default)]
    qos: u8,
    kind_prefix: Option<String>,
    #[serde(default)]
    payload: PayloadFormat,
}

/// [`ConnectorFactory`](octo_core::ConnectorFactory) for `type = "mqtt"`.
pub struct MqttConnectorFactory;

impl octo_core::ConnectorFactory for MqttConnectorFactory {
    fn type_name(&self) -> &str {
        "mqtt"
    }

    fn create(
        &self,
        id: ConnectorId,
        config: &toml::Value,
        _ctx: octo_core::FactoryContext<'_>,
    ) -> Result<Arc<dyn Connector>, Box<dyn std::error::Error + Send + Sync>> {
        let raw: RawManifest = config.clone().try_into()?;
        let table = raw.connector;
        if table.subs.is_empty() {
            return Err("mqtt connector has no [[connector.sub]] entries".into());
        }
        let subs = table
            .subs
            .into_iter()
            .map(|s| MqttSub {
                topic: s.topic,
                qos: s.qos,
                kind_prefix: s.kind_prefix.unwrap_or_else(|| "mqtt".to_string()),
                payload: s.payload,
            })
            .collect();

        let mut connector = MqttConnector::new(id.as_str().to_string(), table.host, table.port, subs);
        if let Some(cid) = table.client_id {
            connector = connector.with_client_id(cid);
        }
        if let (Some(user), Some(pass_env)) = (table.username, table.password_env) {
            let pass = std::env::var(&pass_env)
                .map_err(|_| format!("password env var '{pass_env}' not set"))?;
            connector = connector.with_credentials(user, pass);
        }
        let _ = table.keep_alive_secs; // reserved; default keep-alive applies
        Ok(connector)
    }
}

/// Factory handle for registration:
/// `register_connector_type("mqtt", octo_connector_mqtt::factory())`.
pub fn factory() -> Arc<dyn octo_core::ConnectorFactory> {
    Arc::new(MqttConnectorFactory)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_to_kind_replaces_slashes() {
        assert_eq!(
            topic_to_kind("mqtt", "factory/temperature").as_str(),
            "mqtt.factory.temperature"
        );
        assert_eq!(topic_to_kind("sensor", "a/b/c").as_str(), "sensor.a.b.c");
    }

    #[test]
    fn topic_filter_matching() {
        assert!(topic_matches("factory/#", "factory/temperature"));
        assert!(topic_matches("factory/#", "factory/line1/temp"));
        assert!(topic_matches("factory/#", "factory")); // # matches zero levels
        assert!(topic_matches("factory/+", "factory/temp"));
        assert!(!topic_matches("factory/+", "factory/line1/temp")); // + is one level
        assert!(!topic_matches("factory/+", "factory")); // + needs a level
        assert!(topic_matches("#", "anything/at/all"));
        assert!(topic_matches("a/b", "a/b"));
        assert!(!topic_matches("a/b", "a/c"));
    }

    #[test]
    fn map_json_publish_carries_value_and_channel() {
        let id = ConnectorId::new("factory");
        let sub = MqttSub::new("factory/#")
            .with_kind_prefix("mqtt")
            .with_payload(PayloadFormat::Json);
        let env = map_publish(
            &id,
            &sub,
            "factory/temperature",
            bytes::Bytes::from_static(b"{\"celsius\":95}"),
        );

        assert_eq!(env.kind.as_str(), "mqtt.factory.temperature");
        assert_eq!(env.channel.as_ref().unwrap().as_str(), "factory/temperature");
        let value = env.payload_as::<Value>().expect("json value");
        assert_eq!(value["celsius"], 95);
    }

    #[test]
    fn map_text_publish_is_a_string() {
        let id = ConnectorId::new("factory");
        let sub = MqttSub::new("factory/#"); // text by default
        let env = map_publish(&id, &sub, "factory/note", bytes::Bytes::from_static(b"hi"));
        assert_eq!(env.payload_as::<String>().map(String::as_str), Some("hi"));
    }

    #[test]
    fn map_bytes_publish_is_a_blob() {
        let id = ConnectorId::new("cam");
        let sub = MqttSub::new("cam/#").with_payload(PayloadFormat::Bytes);
        let env = map_publish(&id, &sub, "cam/frame", bytes::Bytes::from_static(&[1, 2, 3]));
        let blob = env.payload_as::<Blob>().expect("blob");
        assert_eq!(blob.content_type(), "application/octet-stream");
        assert_eq!(blob.bytes().as_ref(), &[1, 2, 3]);
    }
}
