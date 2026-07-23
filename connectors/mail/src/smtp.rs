//! SMTP side: send a new message, or reply to one (subject `Re:`, recipient from
//! the original's Reply-To/From, threaded with In-Reply-To/References). Implicit
//! TLS on 465 via `lettre`'s tokio-rustls transport.

use lettre::message::{header, Mailbox, Message as Email};
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Tokio1Executor};
use serde_json::{json, Value};

use crate::config::MailConfig;
use crate::error::{MailError, Result};

/// Build the async SMTP transport (implicit TLS, authenticated).
fn transport(cfg: &MailConfig) -> Result<AsyncSmtpTransport<Tokio1Executor>> {
    let creds = Credentials::new(cfg.smtp_user.clone(), cfg.smtp_pass.clone());
    let builder = AsyncSmtpTransport::<Tokio1Executor>::relay(&cfg.smtp_host)
        .map_err(|e| MailError::Smtp(format!("relay {}: {e}", cfg.smtp_host)))?
        .port(cfg.smtp_port)
        .credentials(creds);
    Ok(builder.build())
}

/// Send a new plain-text message. Payload: `{ to, subject, text }`.
pub(crate) async fn send(cfg: &MailConfig, params: &Value) -> Result<Value> {
    let to = req_str(params, "to")?;
    let subject = req_str(params, "subject")?;
    let text = req_str(params, "text")?;

    let email = base_builder(cfg, &to)?
        .subject(subject)
        .header(header::ContentType::TEXT_PLAIN)
        .body(text)
        .map_err(|e| MailError::Smtp(format!("build message: {e}")))?;

    deliver(cfg, email, &to).await
}

/// Reply to a message read from IMAP. Payload: `{ text, in_reply_to?, references?,
/// to, subject }` — the cogitator passes the original's Message-ID / References /
/// Reply-To (from a prior `mail.cmd.read`) so we thread correctly and it stays a
/// deliberate two-step action.
pub(crate) async fn reply(cfg: &MailConfig, params: &Value) -> Result<Value> {
    let to = req_str(params, "to")?;
    let text = req_str(params, "text")?;
    let raw_subject = req_str(params, "subject")?;
    let subject = if raw_subject.to_lowercase().starts_with("re:") {
        raw_subject
    } else {
        format!("Re: {raw_subject}")
    };

    let mut builder = base_builder(cfg, &to)?.subject(subject);
    if let Some(mid) = params.get("in_reply_to").and_then(Value::as_str).filter(|s| !s.is_empty()) {
        builder = builder.in_reply_to(mid.to_string());
    }
    if let Some(refs) = params.get("references").and_then(Value::as_str).filter(|s| !s.is_empty()) {
        builder = builder.references(refs.to_string());
    }
    let email = builder
        .header(header::ContentType::TEXT_PLAIN)
        .body(text)
        .map_err(|e| MailError::Smtp(format!("build reply: {e}")))?;

    deliver(cfg, email, &to).await
}

/// Shared message builder: From (the configured identity) + a parsed To.
fn base_builder(cfg: &MailConfig, to: &str) -> Result<lettre::message::MessageBuilder> {
    let from: Mailbox = cfg
        .from
        .parse()
        .map_err(|e| MailError::Config(format!("bad From address `{}`: {e}", cfg.from)))?;
    let to: Mailbox = to
        .parse()
        .map_err(|e| MailError::Smtp(format!("bad To address `{to}`: {e}")))?;
    Ok(Email::builder().from(from).to(to))
}

/// Send and shape the result.
async fn deliver(cfg: &MailConfig, email: Email, to: &str) -> Result<Value> {
    let mailer = transport(cfg)?;
    let resp = mailer
        .send(email)
        .await
        .map_err(|e| MailError::Smtp(format!("send: {e}")))?;
    Ok(json!({
        "sent": true,
        "to": to,
        "code": resp.code().to_string(),
    }))
}

fn req_str(params: &Value, key: &str) -> Result<String> {
    params
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .ok_or_else(|| MailError::Config(format!("missing required field `{key}`")))
}
