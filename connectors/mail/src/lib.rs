//! `octo-connector-mail` — an IMAP/SMTP mail organ for the Octo runtime.
//!
//! One crate, one mailbox. Reaches the world as **env-as-tools**: a cogitator
//! dispatches a command envelope (`mail.cmd.*`) at this connector and gets a
//! correlated `mail.cmd.*.result` back. Four commands:
//!
//! - `mail.cmd.list`  `{ limit?, query?, folder? }` → recent messages (envelope
//!   only), newest first, optionally substring-filtered on from+subject.
//! - `mail.cmd.read`  `{ uid, max?, folder? }` → one message, full MIME parse
//!   (charset — cp1251 included —, RFC 2047 headers, body pick, attachments,
//!   calendar invites).
//! - `mail.cmd.send`  `{ to, subject, text }` → send a new plain-text message.
//! - `mail.cmd.reply` `{ to, subject, text, in_reply_to?, references? }` → a
//!   threaded reply (the caller passes the original's ids from a prior read).
//!
//! Sending is a real side effect: the *decision* to send is the cogitator's (it
//! confirms with the user first); this connector just carries it out. Built from
//! a `type = "mail"` manifest — IMAP/SMTP hosts/ports are literals, credentials
//! are named env vars (see [`config::MailConfig`]).

mod config;
mod error;
mod imap;
mod mime;
mod smtp;

use std::sync::Arc;

use async_trait::async_trait;
use octo_core::{
    Connector, ConnectorCapabilities, ConnectorContext, ConnectorFactory, ConnectorId, Envelope,
    EventKind, FactoryContext, Filter, OctoResult, SubscribeOptions,
};
use serde_json::{json, Value};

pub use config::MailConfig;
/// Install the rustls CryptoProvider (ring) process-wide. Call once at process
/// startup, before building the runtime, so no sibling connector's TLS races it.
/// See [`imap::ensure_crypto_provider`].
pub use imap::ensure_crypto_provider;

const LIST: &str = "mail.cmd.list";
const READ: &str = "mail.cmd.read";
const SEND: &str = "mail.cmd.send";
const REPLY: &str = "mail.cmd.reply";

/// The env-as-tools catalogue advertised to the runtime (and thus the cogitator).
const CATALOG: &str = "\
Mailbox access over IMAP/SMTP. Commands:
    mail.cmd.list — payload: { limit?, query?, folder? } — recent messages (uid, date, from, subject, flags), newest first; `query` filters on from+subject.
    mail.cmd.read — payload: { uid, max?, folder? } — one message: from/to/subject/date/text (truncated to `max`), attachments, calendarEvents.
    mail.cmd.send — payload: { to, subject, text } — send a new plain-text email (confirm with the user first).
    mail.cmd.reply — payload: { to, subject, text, in_reply_to?, references? } — a threaded reply; pass the original's messageId/references from a prior read.";

pub struct MailConnector {
    id: ConnectorId,
    capabilities: ConnectorCapabilities,
    cfg: MailConfig,
}

impl MailConnector {
    pub fn new(id: impl Into<String>, cfg: MailConfig) -> Arc<Self> {
        let capabilities = ConnectorCapabilities::bidirectional()
            .with_accept_kinds([
                EventKind::from_static(LIST),
                EventKind::from_static(READ),
                EventKind::from_static(SEND),
                EventKind::from_static(REPLY),
            ])
            .with_emit_kinds([
                EventKind::new(format!("{LIST}.result")),
                EventKind::new(format!("{READ}.result")),
                EventKind::new(format!("{SEND}.result")),
                EventKind::new(format!("{REPLY}.result")),
            ])
            .with_description(CATALOG);
        Arc::new(Self { id: ConnectorId::new(id), capabilities, cfg })
    }
}

#[async_trait]
impl Connector for MailConnector {
    fn id(&self) -> &ConnectorId {
        &self.id
    }

    fn capabilities(&self) -> &ConnectorCapabilities {
        &self.capabilities
    }

    async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
        // Pin the rustls CryptoProvider before ANY TLS config is built (IMAP or
        // SMTP) — with two providers in the tree the lazy default panics.
        imap::ensure_crypto_provider();
        let mut cmds = ctx
            .subscribe(Filter::by_target(self.id.clone()), SubscribeOptions::default())
            .await?;
        tracing::info!(connector = %self.id, host = %self.cfg.imap_host, "mail ready");
        loop {
            tokio::select! {
                next = cmds.next() => match next {
                    Some(env) => self.handle(&env, &ctx).await,
                    None => return Ok(()),
                },
                _ = ctx.shutdown.cancelled() => return Ok(()),
            }
        }
    }
}

impl MailConnector {
    async fn handle(&self, env: &Envelope, ctx: &ConnectorContext) {
        let kind = env.kind.as_str();
        if !matches!(kind, LIST | READ | SEND | REPLY) {
            return; // not one of ours
        }
        let params = env.payload_as::<Value>().cloned().unwrap_or(Value::Null);

        let outcome = match kind {
            LIST => {
                let limit = config::opt_usize(&params, "limit", 10).clamp(1, 50);
                let query = params.get("query").and_then(Value::as_str);
                let folder = params.get("folder").and_then(Value::as_str);
                imap::list(&self.cfg, limit, query, folder).await
            }
            READ => match params.get("uid").and_then(Value::as_u64) {
                Some(uid) => {
                    let max = config::opt_usize(&params, "max", 6000);
                    let folder = params.get("folder").and_then(Value::as_str);
                    imap::read(&self.cfg, uid as u32, max, folder).await
                }
                None => Err(error::MailError::Config("read needs a numeric `uid`".into())),
            },
            SEND => smtp::send(&self.cfg, &params).await,
            REPLY => smtp::reply(&self.cfg, &params).await,
            _ => unreachable!(),
        };

        let payload = match outcome {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(kind, error = %e, "mail command failed");
                json!({ "error": e.to_string() })
            }
        };
        let resp = Envelope::new(self.id.clone(), EventKind::new(format!("{kind}.result")), payload)
            .with_correlation(env.id);
        if let Err(e) = ctx.publish(resp).await {
            tracing::warn!(error = %e, "mail failed to publish result");
        }
    }
}

// ── config-driven construction (`type = "mail"`) ────────────────────────────

/// [`ConnectorFactory`] for `type = "mail"`. Register with
/// `Octo::builder().register_connector_type("mail", octo_connector_mail::factory())`.
pub struct MailConnectorFactory;

impl ConnectorFactory for MailConnectorFactory {
    fn type_name(&self) -> &str {
        "mail"
    }

    fn create(
        &self,
        id: ConnectorId,
        config: &toml::Value,
        _ctx: FactoryContext<'_>,
    ) -> Result<Arc<dyn Connector>, Box<dyn std::error::Error + Send + Sync>> {
        let table = config
            .get("connector")
            .ok_or("mail: manifest has no [connector] table")?;
        let cfg = MailConfig::from_table(table)?;
        tracing::info!(connector = %id, host = %cfg.imap_host, "mail: config loaded");
        Ok(MailConnector::new(id.as_str(), cfg))
    }
}

/// Convenience factory handle for registration.
pub fn factory() -> Arc<dyn ConnectorFactory> {
    Arc::new(MailConnectorFactory)
}
