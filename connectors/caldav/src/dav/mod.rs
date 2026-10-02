//! The CalDAV protocol operations (RFC 4791): list / create / delete VEVENTs
//! over WebDAV verbs, with iCalendar bodies. Transport-agnostic of the Octo
//! connector — pure request/response functions the connector calls.

use chrono::{DateTime, Utc};
use octo_http_auth::HttpAuth;

mod discovery;
mod event;
mod ical;
use self::{event::build_event_ics, ical::expand_calendar};
pub use discovery::discover_collection;
use serde_json::{Value, json};

/// Cap on recurrence occurrences materialised per series in one `list_events`
/// window — a safety bound against a pathological rule; a normal day/week query
/// yields a handful. If a query ever hits it, we log and report truncation.
const MAX_OCCURRENCES: u16 = 512;

#[derive(Debug, thiserror::Error)]
pub enum DavError {
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("auth: {0}")]
    Auth(#[from] octo_http_auth::AuthError),
    #[error("caldav returned {status}: {body}")]
    Status { status: u16, body: String },
    #[error("bad RFC3339 time `{value}`: {reason}")]
    BadTime { value: String, reason: String },
    #[error("xml parse: {0}")]
    Xml(String),
    #[error("missing field `{0}`")]
    MissingField(&'static str),
    #[error("bad recurrence rule: {0} (an RFC 5545 RRULE, e.g. FREQ=WEEKLY;BYDAY=MO,TU,WE,TH,FR)")]
    BadRecurrence(String),
    #[error("caldav discovery: {0}")]
    Discovery(String),
}

/// `REPORT` a `calendar-query` over `[from, to]` (RFC3339), returning matching
/// VEVENTs as `{ events: [{ uid, title, start, end, location? }] }`.
pub async fn list_events(
    client: &reqwest::Client,
    collection: &str,
    auth: &HttpAuth,
    from: &str,
    to: &str,
    tz: chrono_tz::Tz,
) -> Result<Value, DavError> {
    let start = to_ical_utc(from)?;
    let end = to_ical_utc(to)?;
    let body = format!(
        r#"<?xml version="1.0" encoding="utf-8"?>
<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop><C:calendar-data/></D:prop>
  <C:filter><C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">
    <C:time-range start="{start}" end="{end}"/>
  </C:comp-filter></C:comp-filter></C:filter>
</C:calendar-query>"#
    );
    let method = reqwest::Method::from_bytes(b"REPORT").expect("valid method");
    let req = client
        .request(method, collection)
        .header("Depth", "1")
        .header(
            reqwest::header::CONTENT_TYPE,
            "application/xml; charset=utf-8",
        )
        .body(body);
    let resp = auth.apply(req).await?.send().await?;
    let status = resp.status();
    let text = resp.text().await?;
    if !status.is_success() {
        return Err(DavError::Status {
            status: status.as_u16(),
            body: truncate(&text),
        });
    }

    // The server filters by the same `[start, end]`, but for recurring series it
    // returns the *master* (original DTSTART + RRULE) rather than the occurrence
    // in-window, so we expand recurrences client-side against the window.
    let win_start = to_utc(from)?;
    let win_end = to_utc(to)?;
    let mut events = Vec::new();
    for ical in extract_calendar_data(&text)? {
        expand_calendar(&ical, win_start, win_end, tz, &mut events);
    }
    Ok(json!({ "events": events }))
}

/// `PUT` a new VEVENT to `<collection>/<uid>.ics`. Returns `{ uid }`.
///
/// `default_reminder` is the connector's fallback popup lead time (minutes); a
/// per-event `reminder_minutes` in `params` overrides it. See [`build_event_ics`].
pub async fn create_event(
    client: &reqwest::Client,
    collection: &str,
    auth: &HttpAuth,
    params: &Value,
    uid: &str,
    default_reminder: Option<i64>,
    tz: chrono_tz::Tz,
) -> Result<Value, DavError> {
    let ics = build_event_ics(params, uid, default_reminder, tz)?;

    let url = event_url(collection, uid);
    let req = client
        .put(&url)
        .header(
            reqwest::header::CONTENT_TYPE,
            "text/calendar; charset=utf-8",
        )
        .body(ics);
    let resp = auth.apply(req).await?.send().await?;
    let status = resp.status();
    if !status.is_success() {
        return Err(DavError::Status {
            status: status.as_u16(),
            body: truncate(&resp.text().await.unwrap_or_default()),
        });
    }
    Ok(json!({ "uid": uid }))
}

/// `DELETE <collection>/<uid>.ics`. Returns `{ deleted: bool }`.
pub async fn delete_event(
    client: &reqwest::Client,
    collection: &str,
    auth: &HttpAuth,
    params: &Value,
) -> Result<Value, DavError> {
    let uid = str_field(params, "uid")?;
    let url = event_url(collection, uid);
    let resp = auth.apply(client.delete(&url)).await?.send().await?;
    // 200/204 = gone; 404 = already absent (also "deleted" from the caller's view).
    let status = resp.status();
    let deleted = status.is_success() || status == reqwest::StatusCode::NOT_FOUND;
    Ok(json!({ "deleted": deleted, "status": status.as_u16() }))
}

// ── helpers ──────────────────────────────────────────────────────────────────

// ── recurrence-aware VEVENT expansion ───────────────────────────────────────
//
// A CalDAV `calendar-query` returns, per recurring series, the *master* VEVENT
// (its original DTSTART + RRULE/EXDATE) plus any modified instances as separate
// VEVENTs carrying a RECURRENCE-ID. To answer "what's on in [win_start, win_end]"
// we materialise each master's occurrences in the window (via the `rrule` crate),
// substitute RECURRENCE-ID overrides, and emit every hit at its *real* time. A
// non-recurring event is emitted as-is. On any parse failure we fall back to the
// master's own DTSTART so an event is surfaced rather than silently dropped.

/// A parsed VEVENT: property lines as `(NAME, params, value)` where `params`
/// keeps its leading `;` (e.g. `;TZID=Europe/Moscow`) or is empty.

/// Pull the text inside every `<calendar-data>` element of a `multistatus`.
fn extract_calendar_data(xml: &str) -> Result<Vec<String>, DavError> {
    use quick_xml::events::Event as Xml;
    use quick_xml::reader::Reader;

    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(false);
    let mut out = Vec::new();
    let mut inside = false;
    let mut current = String::new();
    loop {
        match reader.read_event() {
            Ok(Xml::Start(e)) if e.local_name().as_ref() == b"calendar-data" => {
                inside = true;
                current.clear();
            }
            Ok(Xml::End(e)) if e.local_name().as_ref() == b"calendar-data" => {
                inside = false;
                if !current.trim().is_empty() {
                    out.push(std::mem::take(&mut current));
                }
            }
            Ok(Xml::Text(e)) if inside => {
                current.push_str(&e.unescape().unwrap_or_default());
            }
            Ok(Xml::CData(e)) if inside => {
                current.push_str(&String::from_utf8_lossy(&e));
            }
            Ok(Xml::Eof) => break,
            Ok(_) => {}
            Err(e) => return Err(DavError::Xml(e.to_string())),
        }
    }
    Ok(out)
}

fn event_url(collection: &str, uid: &str) -> String {
    format!("{}/{}.ics", collection.trim_end_matches('/'), uid)
}

fn str_field<'a>(params: &'a Value, key: &'static str) -> Result<&'a str, DavError> {
    params
        .get(key)
        .and_then(Value::as_str)
        .ok_or(DavError::MissingField(key))
}

fn to_utc(rfc3339: &str) -> Result<DateTime<Utc>, DavError> {
    DateTime::parse_from_rfc3339(rfc3339)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| DavError::BadTime {
            value: rfc3339.to_string(),
            reason: e.to_string(),
        })
}

/// RFC3339 → iCalendar UTC (`YYYYMMDDTHHMMSSZ`) for a `time-range`.
fn to_ical_utc(rfc3339: &str) -> Result<String, DavError> {
    Ok(to_utc(rfc3339)?.format("%Y%m%dT%H%M%SZ").to_string())
}

fn truncate(s: &str) -> String {
    s.chars().take(500).collect()
}

// ── collection discovery (PROPFIND) ─────────────────────────────────────────

/// Discover a calendar collection URL from a CalDAV server root, the way desktop
/// clients do: `current-user-principal` -> `calendar-home-set` -> list calendars,
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration as ChronoDuration;
    use icalendar::Trigger;

    fn win(from: &str, to: &str) -> (DateTime<Utc>, DateTime<Utc>) {
        (to_utc(from).unwrap(), to_utc(to).unwrap())
    }

    #[test]
    fn ical_utc_round_trips() {
        assert_eq!(
            to_ical_utc("2026-01-15T12:30:00Z").unwrap(),
            "20260115T123000Z"
        );
    }

    /// The serialized value of a "before start" TRIGGER for `minutes`, computed via
    /// the same icalendar path `build_event_ics` uses — so the reminder tests assert
    /// the alarm's lead time without hard-coding chrono's ISO-8601 duration spelling.
    fn trigger_value(minutes: i64) -> String {
        let prop: icalendar::Property =
            Trigger::before_start(ChronoDuration::minutes(minutes)).into();
        prop.value().to_string()
    }

    fn event(reminder: Option<i64>) -> Value {
        let mut e = json!({
            "title": "Drink water",
            "start": "2026-07-15T14:00:00Z",
            "end": "2026-07-15T14:30:00Z",
        });
        if let Some(m) = reminder {
            e["reminder_minutes"] = json!(m);
        }
        e
    }

    #[test]
    fn a_recurring_event_carries_its_rule_in_local_time() {
        let mut e = event(Some(10));
        e["start"] = json!("2026-09-28T06:00:00Z"); // Monday 09:00 in Moscow
        e["end"] = json!("2026-09-28T06:15:00Z");
        e["recurrence"] = json!("RRULE:FREQ=WEEKLY;BYDAY=MO,TU,WE,TH,FR");
        let ics = build_event_ics(&e, "uid-r1", None, chrono_tz::Europe::Moscow).unwrap();
        assert!(
            ics.contains("RRULE:FREQ=WEEKLY;BYDAY=MO,TU,WE,TH,FR"),
            "{ics}"
        );
        assert!(
            ics.contains("DTSTART;TZID=Europe/Moscow:20260928T090000"),
            "{ics}"
        );
        assert!(
            ics.contains("BEGIN:VALARM"),
            "the alarm rides on every occurrence"
        );
    }

    #[test]
    fn a_recurring_event_in_utc_stays_in_utc_and_bad_rules_are_refused() {
        let mut e = event(None);
        e["recurrence"] = json!("FREQ=DAILY;COUNT=5");
        let ics = build_event_ics(&e, "uid-r2", None, chrono_tz::UTC).unwrap();
        assert!(
            ics.contains("RRULE:FREQ=DAILY;COUNT=5") && ics.contains("DTSTART:20260715T140000Z"),
            "{ics}"
        );
        e["recurrence"] = json!("FREQ=SOMETIMES");
        assert!(matches!(
            build_event_ics(&e, "uid-r3", None, chrono_tz::UTC),
            Err(DavError::BadRecurrence(_))
        ));
    }

    #[test]
    fn reminder_attaches_a_display_valarm_before_start() {
        let ics = build_event_ics(&event(Some(10)), "uid-1", None, chrono_tz::UTC).unwrap();
        assert!(
            ics.contains("BEGIN:VALARM"),
            "a reminder should add a VALARM:\n{ics}"
        );
        assert!(
            ics.contains("ACTION:DISPLAY"),
            "the alarm should be a display popup:\n{ics}"
        );
        assert!(
            ics.contains(&trigger_value(10)),
            "trigger should fire 10 min before:\n{ics}"
        );
    }

    #[test]
    fn per_event_reminder_overrides_the_connector_default() {
        // Per-event 5 min must win over the connector's 30 min default.
        let ics = build_event_ics(&event(Some(5)), "uid-2", Some(30), chrono_tz::UTC).unwrap();
        assert!(
            ics.contains(&trigger_value(5)),
            "per-event 5 min should win:\n{ics}"
        );
        assert!(
            !ics.contains(&trigger_value(30)),
            "the 30 min default must not leak in:\n{ics}"
        );
    }

    #[test]
    fn connector_default_reminder_applies_when_event_omits_it() {
        let ics = build_event_ics(&event(None), "uid-3", Some(10), chrono_tz::UTC).unwrap();
        assert!(
            ics.contains("BEGIN:VALARM"),
            "the connector default should add an alarm:\n{ics}"
        );
        assert!(
            ics.contains(&trigger_value(10)),
            "default 10 min lead time:\n{ics}"
        );
    }

    #[test]
    fn no_reminder_yields_a_plain_event() {
        let ics = build_event_ics(&event(None), "uid-4", None, chrono_tz::UTC).unwrap();
        assert!(!ics.contains("VALARM"), "no lead time -> no alarm:\n{ics}");
    }

    #[test]
    fn negative_reminder_suppresses_the_default() {
        // An explicit -1 opts out even though the connector has a default.
        let ics = build_event_ics(&event(Some(-1)), "uid-5", Some(10), chrono_tz::UTC).unwrap();
        assert!(
            !ics.contains("VALARM"),
            "-1 opts out of the default:\n{ics}"
        );
    }

    #[test]
    fn extracts_calendar_data_blob() {
        let xml = r#"<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:response><D:propstat><D:prop>
    <C:calendar-data>BEGIN:VCALENDAR&#13;
BEGIN:VEVENT&#13;
UID:abc-123&#13;
END:VEVENT&#13;
END:VCALENDAR&#13;
</C:calendar-data>
  </D:prop></D:propstat></D:response>
</D:multistatus>"#;
        let blobs = extract_calendar_data(xml).unwrap();
        assert_eq!(blobs.len(), 1);
        assert!(blobs[0].contains("BEGIN:VEVENT"));
    }

    #[test]
    fn single_event_emitted_as_is() {
        let ics = "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:abc-123\r\nSUMMARY:Standup\r\n\
                   DTSTART:20260115T090000Z\r\nDTEND:20260115T091500Z\r\nLOCATION:Room 1\r\n\
                   END:VEVENT\r\nEND:VCALENDAR\r\n";
        let (s, e) = win("2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z");
        let mut out = Vec::new();
        expand_calendar(ics, s, e, chrono_tz::UTC, &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["uid"], "abc-123");
        assert_eq!(out[0]["title"], "Standup");
        assert_eq!(out[0]["start"], "2026-01-15T09:00:00Z");
        assert_eq!(out[0]["end"], "2026-01-15T09:15:00Z");
        assert_eq!(out[0]["location"], "Room 1");
    }

    #[test]
    fn expands_recurring_master_into_window() {
        // A daily standup whose series began in April, queried for one day in July:
        // the server returns the master (April DTSTART + RRULE); we must surface the
        // *July* occurrence at its real Moscow time (10:15 MSK = 07:15Z).
        let ics = "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:daily-1\r\nSUMMARY:Standup\r\n\
                   DTSTART;TZID=Europe/Moscow:20260430T101500\r\n\
                   DTEND;TZID=Europe/Moscow:20260430T103000\r\n\
                   RRULE:FREQ=DAILY\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n";
        let (s, e) = win("2026-07-10T00:00:00Z", "2026-07-11T00:00:00Z");
        let mut out = Vec::new();
        expand_calendar(ics, s, e, chrono_tz::UTC, &mut out);
        assert_eq!(out.len(), 1, "exactly one occurrence in the one-day window");
        assert_eq!(out[0]["title"], "Standup");
        assert_eq!(out[0]["start"], "2026-07-10T07:15:00Z");
        assert_eq!(out[0]["end"], "2026-07-10T07:30:00Z");
    }

    #[test]
    fn recurrence_id_override_replaces_instance() {
        // Master daily at 10:15 MSK; one instance (2026-07-10) moved to 14:00 MSK.
        // The window must show the moved time once — not the original, not both.
        let ics = "BEGIN:VCALENDAR\r\n\
                   BEGIN:VEVENT\r\nUID:daily-2\r\nSUMMARY:Standup\r\n\
                   DTSTART;TZID=Europe/Moscow:20260701T101500\r\n\
                   DTEND;TZID=Europe/Moscow:20260701T103000\r\nRRULE:FREQ=DAILY\r\nEND:VEVENT\r\n\
                   BEGIN:VEVENT\r\nUID:daily-2\r\nSUMMARY:Standup\r\n\
                   RECURRENCE-ID;TZID=Europe/Moscow:20260710T101500\r\n\
                   DTSTART;TZID=Europe/Moscow:20260710T140000\r\n\
                   DTEND;TZID=Europe/Moscow:20260710T143000\r\nEND:VEVENT\r\n\
                   END:VCALENDAR\r\n";
        let (s, e) = win("2026-07-10T00:00:00Z", "2026-07-11T00:00:00Z");
        let mut out = Vec::new();
        expand_calendar(ics, s, e, chrono_tz::UTC, &mut out);
        assert_eq!(out.len(), 1, "override replaces the instance, no duplicate");
        assert_eq!(out[0]["start"], "2026-07-10T11:00:00Z"); // 14:00 MSK, moved
    }

    #[test]
    fn exdate_excludes_occurrence() {
        // Daily series with 2026-07-10 excluded → the window is empty.
        let ics = "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:daily-3\r\nSUMMARY:Standup\r\n\
                   DTSTART;TZID=Europe/Moscow:20260701T101500\r\nRRULE:FREQ=DAILY\r\n\
                   EXDATE;TZID=Europe/Moscow:20260710T101500\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n";
        let (s, e) = win("2026-07-10T00:00:00Z", "2026-07-11T00:00:00Z");
        let mut out = Vec::new();
        expand_calendar(ics, s, e, chrono_tz::UTC, &mut out);
        assert!(out.is_empty(), "EXDATE-excluded day yields nothing");
    }

    #[test]
    fn renders_start_end_in_display_timezone() {
        // Same UTC instant, rendered for Europe/Moscow → local wall-clock + offset.
        let ics = "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:tz-1\r\nSUMMARY:Standup\r\n\
                   DTSTART:20260710T071500Z\r\nDTEND:20260710T074500Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n";
        let (s, e) = win("2026-07-10T00:00:00Z", "2026-07-11T00:00:00Z");
        let mut out = Vec::new();
        expand_calendar(ics, s, e, chrono_tz::Europe::Moscow, &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["start"], "2026-07-10T10:15:00+03:00");
        assert_eq!(out[0]["end"], "2026-07-10T10:45:00+03:00");
    }

    /// Live check against a real CalDAV server. Ignored by default; run with:
    ///   OCTO_YANDEX_APP_PASSWORD=... OCTO_TEST_CALDAV_LOGIN=... \
    ///   OCTO_TEST_CALDAV_COLLECTION=... \
    ///   cargo test -p octo-connector-caldav -- --ignored --nocapture live_list
    #[tokio::test]
    #[ignore]
    async fn live_list() {
        use octo_http_auth::{AuthConfig, HttpAuth};
        let login = std::env::var("OCTO_TEST_CALDAV_LOGIN").expect("OCTO_TEST_CALDAV_LOGIN");
        let collection =
            std::env::var("OCTO_TEST_CALDAV_COLLECTION").expect("OCTO_TEST_CALDAV_COLLECTION");
        let auth = HttpAuth::new(AuthConfig::Basic {
            login,
            password_env: "OCTO_YANDEX_APP_PASSWORD".into(),
        });
        let client = reqwest::Client::new();
        // Window overridable via env (defaults to the calendar year) so the same
        // test can probe a specific day when checking recurrence expansion.
        let from = std::env::var("OCTO_TEST_CALDAV_FROM")
            .unwrap_or_else(|_| "2026-01-01T00:00:00Z".into());
        let to =
            std::env::var("OCTO_TEST_CALDAV_TO").unwrap_or_else(|_| "2027-01-01T00:00:00Z".into());
        let tz: chrono_tz::Tz = std::env::var("OCTO_TEST_CALDAV_TZ")
            .ok()
            .and_then(|t| t.parse().ok())
            .unwrap_or(chrono_tz::UTC);
        let result = list_events(&client, &collection, &auth, &from, &to, tz)
            .await
            .expect("list_events");
        println!(
            "LIVE list_events [{from} .. {to}] ({tz}) -> {}",
            serde_json::to_string_pretty(&result).unwrap()
        );
    }

    /// Live create -> list -> delete round-trip. Ignored; creates and then
    /// deletes one throwaway event on the real calendar. Same env as `live_list`.
    #[tokio::test]
    #[ignore]
    async fn live_roundtrip() {
        use octo_http_auth::{AuthConfig, HttpAuth};
        let login = std::env::var("OCTO_TEST_CALDAV_LOGIN").expect("OCTO_TEST_CALDAV_LOGIN");
        let collection =
            std::env::var("OCTO_TEST_CALDAV_COLLECTION").expect("OCTO_TEST_CALDAV_COLLECTION");
        let auth = HttpAuth::new(AuthConfig::Basic {
            login,
            password_env: "OCTO_YANDEX_APP_PASSWORD".into(),
        });
        let client = reqwest::Client::new();
        let uid = "octo-live-roundtrip-test";
        let params = json!({
            "title": "Octo live test",
            "start": "2026-07-01T10:00:00Z",
            "end": "2026-07-01T11:00:00Z",
            "location": "nowhere",
            "description": "created by octo-connector-caldav live test; safe to delete",
            "reminder_minutes": 15
        });

        let created = create_event(
            &client,
            &collection,
            &auth,
            &params,
            uid,
            Some(10),
            chrono_tz::UTC,
        )
        .await
        .expect("create");
        println!("created -> {created}");

        let listed = list_events(
            &client,
            &collection,
            &auth,
            "2026-06-30T00:00:00Z",
            "2026-07-02T00:00:00Z",
            chrono_tz::UTC,
        )
        .await
        .expect("list");
        println!(
            "listed -> {}",
            serde_json::to_string_pretty(&listed).unwrap()
        );
        let found = listed["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["title"] == "Octo live test");

        let deleted = delete_event(&client, &collection, &auth, &json!({ "uid": uid }))
            .await
            .expect("delete");
        println!("deleted -> {deleted}");

        assert!(found, "created event should appear in the list");
        assert_eq!(deleted["deleted"], true);
    }
}
