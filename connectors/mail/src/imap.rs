//! IMAP side: open an implicit-TLS session, list recent messages, and read one
//! by UID (full MIME parse via `mail-parser`, so charset — cp1251 included —,
//! RFC 2047 headers, and the text/html body pick are handled for us).
//!
//! `async-imap` speaks `futures-io`; a tokio TLS stream is bridged with
//! `tokio_util::compat`. We keep the concrete stream type behind a small alias so
//! the session signatures stay readable.

use std::sync::{Arc, Once};

use async_imap::Session;
use futures::TryStreamExt;
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio_rustls::{rustls, TlsConnector};

use crate::config::MailConfig;
use crate::error::{MailError, Result};

/// The concrete session type. With async-imap's `runtime-tokio` feature the
/// stream bounds are tokio's own `AsyncRead`/`AsyncWrite`, so a tokio-rustls TLS
/// stream is used directly — no futures-io compat shim.
type MailSession = Session<tokio_rustls::client::TlsStream<TcpStream>>;

/// Open a TLS connection, log in, and select the mailbox for reading.
pub(crate) async fn open(cfg: &MailConfig) -> Result<MailSession> {
    let tls = tls_connector()?;
    let server_name = rustls::pki_types::ServerName::try_from(cfg.imap_host.clone())
        .map_err(|_| MailError::Config(format!("invalid IMAP host: {}", cfg.imap_host)))?;

    let tcp = TcpStream::connect((cfg.imap_host.as_str(), cfg.imap_port))
        .await
        .map_err(|e| MailError::Imap(format!("connect {}:{}: {e}", cfg.imap_host, cfg.imap_port)))?;
    let tls_stream = tls
        .connect(server_name, tcp)
        .await
        .map_err(|e| MailError::Imap(format!("TLS handshake: {e}")))?;

    let client = async_imap::Client::new(tls_stream);
    let session = client
        .login(&cfg.imap_user, &cfg.imap_pass)
        .await
        .map_err(|(e, _)| MailError::Imap(format!("login: {e}")))?;
    Ok(session)
}

/// Pin the process-level rustls CryptoProvider to ring, once. The dependency
/// tree carries BOTH providers (aws-lc-rs via reqwest in caldav/http-auth, ring
/// via lettre/async-imap), and with two enabled `ClientConfig::builder()` PANICS
/// at runtime ("could not automatically determine the process-level
/// CryptoProvider").
///
/// This is **process-global** state, so it must run before *any* rustls config
/// is built anywhere in the process — including reqwest's in sibling connectors.
/// The host should call it once at startup (before building the runtime); the
/// connector also calls it from `run()` as belt-and-braces, but that call can
/// lose the race to another connector's first TLS use, so the startup call is
/// the real fix. Idempotent via `Once`.
pub fn ensure_crypto_provider() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// A rustls client config trusting the Mozilla webpki root set.
fn tls_connector() -> Result<TlsConnector> {
    ensure_crypto_provider();
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(TlsConnector::from(Arc::new(config)))
}

/// List recent messages in the mailbox. There is no server-side SEARCH —
/// over-fetch the most recent slice by envelope only, then filter client-side on
/// `query` (a substring over `from` + `subject`), newest first, capped at
/// `limit`. (Server-side SEARCH varies too much across providers to be worth the
/// dialects at this size; the recent window is what a chat assistant needs.)
pub(crate) async fn list(
    cfg: &MailConfig,
    limit: usize,
    query: Option<&str>,
    folder: Option<&str>,
) -> Result<Value> {
    let mut session = open(cfg).await?;
    let mailbox_name = folder.unwrap_or(&cfg.mailbox);
    let mailbox = session
        .select(mailbox_name)
        .await
        .map_err(|e| MailError::Imap(format!("select {mailbox_name}: {e}")))?;

    let exists = mailbox.exists;
    let mut out = Vec::new();
    if exists > 0 {
        // Over-fetch: the most recent max(limit*5, 50) messages by sequence number.
        let window = std::cmp::max(limit * 5, 50) as u32;
        let start = exists.saturating_sub(window) + 1;
        let seq = format!("{start}:*");
        let stream = session
            .fetch(&seq, "(UID ENVELOPE FLAGS INTERNALDATE)")
            .await
            .map_err(|e| MailError::Imap(format!("fetch envelopes: {e}")))?;
        let msgs: Vec<_> = stream
            .try_collect()
            .await
            .map_err(|e| MailError::Imap(format!("collect envelopes: {e}")))?;

        let needle = query.map(|q| q.to_lowercase());
        for m in &msgs {
            let uid = match m.uid {
                Some(u) => u,
                None => continue,
            };
            let env = m.envelope();
            let subject = env
                .and_then(|e| e.subject.as_ref())
                .map(|s| decode_words(s.as_ref()))
                .unwrap_or_default();
            let from = env
                .and_then(|e| e.from.as_ref())
                .and_then(|addrs| addrs.first())
                .map(format_addr)
                .unwrap_or_default();
            let date = m
                .internal_date()
                .map(|d| d.to_rfc3339())
                .unwrap_or_default();
            let flags: Vec<String> = m.flags().map(|f| format!("{f:?}")).collect();

            if let Some(n) = &needle {
                let hay = format!("{from} {subject}").to_lowercase();
                if !hay.contains(n) {
                    continue;
                }
            }
            out.push(json!({
                "uid": uid,
                "date": date,
                "from": from,
                "subject": subject,
                "flags": flags,
            }));
        }
    }
    let _ = session.logout().await;

    // Newest first, capped at `limit`.
    out.reverse();
    out.truncate(limit);
    Ok(json!({
        "mailbox": mailbox_name,
        "count": out.len(),
        "messages": out,
    }))
}

/// Read one message by UID: fetch its raw source and parse the full MIME tree.
pub(crate) async fn read(cfg: &MailConfig, uid: u32, max: usize, folder: Option<&str>) -> Result<Value> {
    let mut session = open(cfg).await?;
    let mailbox_name = folder.unwrap_or(&cfg.mailbox);
    session
        .select(mailbox_name)
        .await
        .map_err(|e| MailError::Imap(format!("select {mailbox_name}: {e}")))?;

    let stream = session
        .uid_fetch(uid.to_string(), "(UID ENVELOPE FLAGS BODY.PEEK[])")
        .await
        .map_err(|e| MailError::Imap(format!("uid fetch: {e}")))?;
    let msgs: Vec<_> = stream
        .try_collect()
        .await
        .map_err(|e| MailError::Imap(format!("collect message: {e}")))?;
    let _ = session.logout().await;

    let msg = msgs
        .iter()
        .find(|m| m.uid == Some(uid))
        .ok_or_else(|| MailError::Imap(format!("uid {uid} not found in {mailbox_name}")))?;
    let source = msg
        .body()
        .ok_or_else(|| MailError::Imap(format!("uid {uid} has no body")))?;

    Ok(crate::mime::parse_message(uid, source, max))
}

/// Render an IMAP envelope address (`Address`) as `Name <local@host>` / `local@host`.
fn format_addr(a: &async_imap::imap_proto::Address) -> String {
    let utf8 = |o: &Option<std::borrow::Cow<[u8]>>| {
        o.as_ref().map(|b| String::from_utf8_lossy(b).into_owned()).unwrap_or_default()
    };
    let name = a.name.as_ref().map(|b| decode_words(b)).unwrap_or_default();
    let mailbox = utf8(&a.mailbox);
    let host = utf8(&a.host);
    let email = if host.is_empty() { mailbox } else { format!("{mailbox}@{host}") };
    if name.is_empty() {
        email
    } else {
        format!("{name} <{email}>")
    }
}

/// Decode RFC 2047 encoded-words (`=?utf-8?B?...?=`) in a raw header value, using
/// `mail-parser`'s decoder; falls back to a lossy UTF-8 read.
fn decode_words(raw: &[u8]) -> String {
    mail_parser::parsers::MessageStream::new(raw)
        .decode_rfc2047()
        .unwrap_or_else(|| String::from_utf8_lossy(raw).into_owned())
}
