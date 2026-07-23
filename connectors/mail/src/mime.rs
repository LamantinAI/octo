//! Turn a raw RFC822 message into the JSON shape the agent reads. `mail-parser`
//! does the heavy lifting — MIME tree walk, transfer-encoding, charset (cp1251 &
//! co.), RFC 2047 headers — we pick the fields and add a tiny VEVENT extractor
//! for calendar invites (title / time / organizer).

use mail_parser::{Address, MessageParser, MimeHeaders};
use serde_json::{json, Value};

/// Parse `source` and shape it: headers, the best text body (plain, else html),
/// truncation flag, attachment list, and any calendar events found.
pub(crate) fn parse_message(uid: u32, source: &[u8], max: usize) -> Value {
    let Some(msg) = MessageParser::default().parse(source) else {
        return json!({ "uid": uid, "error": "could not parse message MIME" });
    };

    let subject = msg.subject().unwrap_or_default().to_string();
    let date = msg.date().map(|d| d.to_rfc3339()).unwrap_or_default();
    let message_id = msg.message_id().unwrap_or_default().to_string();

    // Body: prefer text/plain, fall back to a text rendering of html.
    let raw_body = msg
        .body_text(0)
        .map(|c| c.into_owned())
        .or_else(|| msg.body_html(0).map(|h| html_to_text(&h)))
        .unwrap_or_default();
    let (text, truncated) = truncate(&raw_body, max);

    // Attachments (metadata only — bytes are not shipped through the model).
    let attachments: Vec<Value> = msg
        .attachments()
        .map(|a| {
            json!({
                "filename": a.attachment_name().unwrap_or("attachment"),
                "contentType": a.content_type().map(content_type_string).unwrap_or_default(),
                "size": a.contents().len(),
            })
        })
        .collect();

    let calendar_events = extract_calendar(&msg);

    json!({
        "uid": uid,
        "messageId": message_id,
        "date": date,
        "from": addrs(msg.from()),
        "to": addrs(msg.to()),
        "cc": addrs(msg.cc()),
        "replyTo": addrs(msg.reply_to()),
        "subject": subject,
        "text": text,
        "truncated": truncated,
        "attachments": attachments,
        "calendarEvents": calendar_events,
    })
}

/// Render an address header as a list of `Name <email>` / `email` strings.
fn addrs(a: Option<&Address>) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(addr) = a {
        for a in addr.iter() {
            let name = a.name().unwrap_or_default().trim();
            let email = a.address().unwrap_or_default().trim();
            if email.is_empty() && name.is_empty() {
                continue;
            }
            if name.is_empty() {
                out.push(email.to_string());
            } else if email.is_empty() {
                out.push(name.to_string());
            } else {
                out.push(format!("{name} <{email}>"));
            }
        }
    }
    out
}

fn content_type_string(ct: &mail_parser::ContentType) -> String {
    match ct.subtype() {
        Some(sub) => format!("{}/{}", ct.ctype(), sub),
        None => ct.ctype().to_string(),
    }
}

/// Clip to at most `max` chars on a char boundary; report whether it was cut.
fn truncate(s: &str, max: usize) -> (String, bool) {
    if s.chars().count() <= max {
        (s.to_string(), false)
    } else {
        (s.chars().take(max).collect(), true)
    }
}

/// Very small HTML→text: drop tags, collapse whitespace. `mail-parser` decodes
/// entities already in `body_html`; this only strips markup for the plain view.
fn html_to_text(html: &str) -> String {
    let mut out = String::with_capacity(html.len());
    let mut in_tag = false;
    for c in html.chars() {
        match c {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => out.push(c),
            _ => {}
        }
    }
    // Collapse runs of blank lines / spaces.
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Scan text/calendar parts for a VEVENT and pull the fields the skill surfaces.
fn extract_calendar(msg: &mail_parser::Message) -> Vec<Value> {
    let mut events = Vec::new();
    for part in msg.parts.iter() {
        let is_cal = part
            .content_type()
            .map(|ct| {
                ct.ctype().eq_ignore_ascii_case("text")
                    && ct.subtype().is_some_and(|s| s.eq_ignore_ascii_case("calendar"))
            })
            .unwrap_or(false);
        // Also catch .ics attachments by name.
        let is_ics = part
            .attachment_name()
            .is_some_and(|n| n.to_lowercase().ends_with(".ics"));
        if !(is_cal || is_ics) {
            continue;
        }
        if let Some(text) = part.text_contents().or_else(|| {
            std::str::from_utf8(part.contents()).ok()
        }) {
            if let Some(ev) = parse_vevent(text) {
                events.push(ev);
            }
        }
    }
    events
}

/// Parse the first VEVENT out of an ICS body: unfold continuation lines, then
/// read the handful of properties we report.
fn parse_vevent(ics: &str) -> Option<Value> {
    // Unfold: a line starting with space/tab continues the previous one.
    let mut unfolded: Vec<String> = Vec::new();
    for line in ics.split('\n') {
        let line = line.trim_end_matches('\r');
        if line.starts_with(' ') || line.starts_with('\t') {
            if let Some(last) = unfolded.last_mut() {
                last.push_str(&line[1..]);
            }
        } else {
            unfolded.push(line.to_string());
        }
    }
    if !unfolded.iter().any(|l| l.eq_ignore_ascii_case("BEGIN:VEVENT")) {
        return None;
    }
    let prop = |name: &str| -> Option<String> {
        unfolded.iter().find_map(|l| {
            let key = l.split([';', ':']).next()?;
            if key.eq_ignore_ascii_case(name) {
                l.split_once(':').map(|x| x.1).map(|v| v.trim().to_string())
            } else {
                None
            }
        })
    };
    Some(json!({
        "method": prop("METHOD"),
        "uid": prop("UID"),
        "summary": prop("SUMMARY"),
        "location": prop("LOCATION"),
        "organizer": prop("ORGANIZER").map(|o| o.trim_start_matches("MAILTO:").trim_start_matches("mailto:").to_string()),
        "start": prop("DTSTART"),
        "end": prop("DTEND"),
    }))
}
