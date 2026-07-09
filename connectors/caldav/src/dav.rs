//! The CalDAV protocol operations (RFC 4791): list / create / delete VEVENTs
//! over WebDAV verbs, with iCalendar bodies. Transport-agnostic of the Octo
//! connector — pure request/response functions the connector calls.

use chrono::{DateTime, SecondsFormat, Utc};
use icalendar::{Calendar, CalendarDateTime, Component, Event, EventLike};
use octo_http_auth::HttpAuth;
use serde_json::{json, Map, Value};

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
        .header(reqwest::header::CONTENT_TYPE, "application/xml; charset=utf-8")
        .body(body);
    let resp = auth.apply(req).await?.send().await?;
    let status = resp.status();
    let text = resp.text().await?;
    if !status.is_success() {
        return Err(DavError::Status { status: status.as_u16(), body: truncate(&text) });
    }

    let mut events = Vec::new();
    for ical in extract_calendar_data(&text)? {
        if let Ok(cal) = ical.parse::<Calendar>() {
            for comp in &cal.components {
                if let Some(ev) = comp.as_event() {
                    events.push(event_to_json(ev));
                }
            }
        }
    }
    Ok(json!({ "events": events }))
}

/// `PUT` a new VEVENT to `<collection>/<uid>.ics`. Returns `{ uid }`.
pub async fn create_event(
    client: &reqwest::Client,
    collection: &str,
    auth: &HttpAuth,
    params: &Value,
    uid: &str,
) -> Result<Value, DavError> {
    let title = str_field(params, "title")?;
    let start = to_utc(str_field(params, "start")?)?;
    let end = to_utc(str_field(params, "end")?)?;

    let mut event = Event::new();
    event
        .uid(uid)
        .summary(title)
        .starts(CalendarDateTime::from(start))
        .ends(CalendarDateTime::from(end));
    if let Some(d) = params.get("description").and_then(Value::as_str) {
        event.description(d);
    }
    if let Some(l) = params.get("location").and_then(Value::as_str) {
        event.location(l);
    }
    let ics = Calendar::new().push(event.done()).done().to_string();

    let url = event_url(collection, uid);
    let req = client
        .put(&url)
        .header(reqwest::header::CONTENT_TYPE, "text/calendar; charset=utf-8")
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

fn event_to_json(ev: &Event) -> Value {
    let mut o = Map::new();
    if let Some(v) = ev.property_value("UID") {
        o.insert("uid".into(), json!(v));
    }
    if let Some(v) = ev.property_value("SUMMARY") {
        o.insert("title".into(), json!(v));
    }
    if let Some(v) = ev.property_value("DTSTART") {
        o.insert("start".into(), json!(ical_to_rfc3339(v)));
    }
    if let Some(v) = ev.property_value("DTEND") {
        o.insert("end".into(), json!(ical_to_rfc3339(v)));
    }
    if let Some(v) = ev.property_value("LOCATION") {
        o.insert("location".into(), json!(v));
    }
    Value::Object(o)
}

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
        .map_err(|e| DavError::BadTime { value: rfc3339.to_string(), reason: e.to_string() })
}

/// RFC3339 → iCalendar UTC (`YYYYMMDDTHHMMSSZ`) for a `time-range`.
fn to_ical_utc(rfc3339: &str) -> Result<String, DavError> {
    Ok(to_utc(rfc3339)?.format("%Y%m%dT%H%M%SZ").to_string())
}

/// Best-effort iCalendar UTC (`...Z`) → RFC3339; other forms pass through raw.
fn ical_to_rfc3339(ical: &str) -> String {
    match chrono::NaiveDateTime::parse_from_str(ical, "%Y%m%dT%H%M%SZ") {
        Ok(naive) => DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc)
            .to_rfc3339_opts(SecondsFormat::Secs, true),
        Err(_) => ical.to_string(),
    }
}

fn truncate(s: &str) -> String {
    s.chars().take(500).collect()
}

// ── collection discovery (PROPFIND) ─────────────────────────────────────────

/// Discover a calendar collection URL from a CalDAV server root, the way desktop
/// clients do: `current-user-principal` -> `calendar-home-set` -> list calendars,
/// then pick by display name (if `want_name` is set) or the first VEVENT calendar.
pub async fn discover_collection(
    client: &reqwest::Client,
    base_url: &str,
    auth: &HttpAuth,
    want_name: Option<&str>,
) -> Result<String, DavError> {
    let origin = origin_of(base_url);

    let principal = href_inside(
        &propfind(
            client,
            base_url,
            auth,
            "0",
            r#"<d:propfind xmlns:d="DAV:"><d:prop><d:current-user-principal/></d:prop></d:propfind>"#,
        )
        .await?,
        b"current-user-principal",
    )
    .ok_or_else(|| DavError::Discovery("no current-user-principal".into()))?;

    let home = href_inside(
        &propfind(
            client,
            &resolve_url(&origin, &principal),
            auth,
            "0",
            r#"<d:propfind xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav"><d:prop><c:calendar-home-set/></d:prop></d:propfind>"#,
        )
        .await?,
        b"calendar-home-set",
    )
    .ok_or_else(|| DavError::Discovery("no calendar-home-set".into()))?;

    let list_xml = propfind(
        client,
        &resolve_url(&origin, &home),
        auth,
        "1",
        r#"<d:propfind xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav"><d:prop><d:resourcetype/><d:displayname/><c:supported-calendar-component-set/></d:prop></d:propfind>"#,
    )
    .await?;
    let calendars = parse_calendars(&list_xml);

    let chosen = want_name
        .and_then(|n| calendars.iter().find(|c| c.is_event_calendar() && c.name == n))
        .or_else(|| calendars.iter().find(|c| c.is_event_calendar()))
        .ok_or_else(|| {
            let names: Vec<&str> = calendars.iter().map(|c| c.name.as_str()).collect();
            DavError::Discovery(format!("no VEVENT calendar found; saw: {names:?}"))
        })?;

    Ok(resolve_url(&origin, &chosen.href))
}

async fn propfind(
    client: &reqwest::Client,
    url: &str,
    auth: &HttpAuth,
    depth: &str,
    body: &'static str,
) -> Result<String, DavError> {
    let method = reqwest::Method::from_bytes(b"PROPFIND").expect("valid method");
    let req = client
        .request(method, url)
        .header("Depth", depth)
        .header(reqwest::header::CONTENT_TYPE, "application/xml; charset=utf-8")
        .body(body);
    let resp = auth.apply(req).await?.send().await?;
    let status = resp.status();
    let text = resp.text().await?;
    if !status.is_success() {
        return Err(DavError::Status { status: status.as_u16(), body: truncate(&text) });
    }
    Ok(text)
}

/// One calendar collection from a `calendar-home-set` listing.
struct CalInfo {
    href: String,
    name: String,
    is_calendar: bool,
    supports_vevent: bool,
    is_schedule: bool, // inbox/outbox — never a target
}

impl CalInfo {
    fn is_event_calendar(&self) -> bool {
        self.is_calendar && !self.is_schedule && self.supports_vevent
    }
}

/// The first `<href>` nested inside the named DAV property (e.g. the principal
/// href inside `<current-user-principal>`), skipping the response-level href.
fn href_inside(xml: &str, prop_local: &[u8]) -> Option<String> {
    use quick_xml::events::Event as Xml;
    use quick_xml::reader::Reader;

    let mut reader = Reader::from_str(xml);
    let mut in_prop = false;
    let mut in_href = false;
    let mut href = String::new();
    loop {
        match reader.read_event() {
            Ok(Xml::Start(e)) => {
                let ln = e.local_name();
                if ln.as_ref() == prop_local {
                    in_prop = true;
                } else if in_prop && ln.as_ref() == b"href" {
                    in_href = true;
                    href.clear();
                }
            }
            Ok(Xml::Text(e)) if in_href => href.push_str(&e.unescape().unwrap_or_default()),
            Ok(Xml::End(e)) => {
                let ln = e.local_name();
                if in_href && ln.as_ref() == b"href" {
                    return Some(href.trim().to_string());
                }
                if ln.as_ref() == prop_local {
                    in_prop = false;
                }
            }
            Ok(Xml::Eof) => break,
            Ok(_) => {}
            Err(_) => break,
        }
    }
    None
}

/// Parse a `calendar-home-set` multistatus into calendar collections.
fn parse_calendars(xml: &str) -> Vec<CalInfo> {
    use quick_xml::events::{BytesStart, Event as Xml};
    use quick_xml::reader::Reader;

    fn has_vevent(e: &BytesStart) -> bool {
        e.local_name().as_ref() == b"comp"
            && e.attributes().flatten().any(|a| {
                a.key.local_name().as_ref() == b"name" && a.value.as_ref() == b"VEVENT"
            })
    }

    let mut reader = Reader::from_str(xml);
    let mut out: Vec<CalInfo> = Vec::new();
    let mut cur: Option<CalInfo> = None;
    let mut got_href = false; // capture only the response-level (first) href
    let mut cap: Option<&'static str> = None; // "href" | "displayname"
    loop {
        match reader.read_event() {
            Ok(Xml::Start(e)) => match e.local_name().as_ref() {
                b"response" => {
                    cur = Some(CalInfo {
                        href: String::new(),
                        name: String::new(),
                        is_calendar: false,
                        supports_vevent: false,
                        is_schedule: false,
                    });
                    got_href = false;
                }
                b"href" if cur.is_some() && !got_href => cap = Some("href"),
                b"displayname" if cur.is_some() => cap = Some("displayname"),
                b"calendar" => {
                    if let Some(c) = cur.as_mut() {
                        c.is_calendar = true;
                    }
                }
                b"schedule-inbox" | b"schedule-outbox" => {
                    if let Some(c) = cur.as_mut() {
                        c.is_schedule = true;
                    }
                }
                _ => {
                    if has_vevent(&e) {
                        if let Some(c) = cur.as_mut() {
                            c.supports_vevent = true;
                        }
                    }
                }
            },
            // resourcetype/comp elements are usually empty: <C:calendar/>, <C:comp name="VEVENT"/>
            Ok(Xml::Empty(e)) => match e.local_name().as_ref() {
                b"calendar" => {
                    if let Some(c) = cur.as_mut() {
                        c.is_calendar = true;
                    }
                }
                b"schedule-inbox" | b"schedule-outbox" => {
                    if let Some(c) = cur.as_mut() {
                        c.is_schedule = true;
                    }
                }
                _ => {
                    if has_vevent(&e) {
                        if let Some(c) = cur.as_mut() {
                            c.supports_vevent = true;
                        }
                    }
                }
            },
            Ok(Xml::Text(e)) => {
                if let (Some(c), Some(what)) = (cur.as_mut(), cap) {
                    let t = e.unescape().unwrap_or_default();
                    match what {
                        "href" => c.href.push_str(&t),
                        "displayname" => c.name.push_str(&t),
                        _ => {}
                    }
                }
            }
            Ok(Xml::End(e)) => match e.local_name().as_ref() {
                b"href" => {
                    if cap == Some("href") {
                        got_href = true;
                    }
                    cap = None;
                }
                b"displayname" => cap = None,
                b"response" => {
                    if let Some(c) = cur.take() {
                        out.push(c);
                    }
                }
                _ => {}
            },
            Ok(Xml::Eof) => break,
            Ok(_) => {}
            Err(_) => break,
        }
    }
    out
}

/// `scheme://host[:port]` of a URL, for resolving path-absolute hrefs.
fn origin_of(url: &str) -> String {
    if let Some(after_scheme) = url.find("://") {
        let rest = &url[after_scheme + 3..];
        let host_len = rest.find('/').unwrap_or(rest.len());
        return url[..after_scheme + 3 + host_len].to_string();
    }
    url.trim_end_matches('/').to_string()
}

/// Resolve a possibly path-absolute DAV `href` against the server origin.
fn resolve_url(origin: &str, href: &str) -> String {
    if href.starts_with("http://") || href.starts_with("https://") {
        href.to_string()
    } else if let Some(rest) = href.strip_prefix('/') {
        format!("{}/{}", origin.trim_end_matches('/'), rest)
    } else {
        format!("{}/{}", origin.trim_end_matches('/'), href)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ical_utc_round_trips() {
        assert_eq!(to_ical_utc("2026-01-15T12:30:00Z").unwrap(), "20260115T123000Z");
        assert_eq!(ical_to_rfc3339("20260115T123000Z"), "2026-01-15T12:30:00Z");
        // Non-UTC forms pass through untouched.
        assert_eq!(ical_to_rfc3339("20260115"), "20260115");
    }

    #[test]
    fn extracts_calendar_data_and_parses_event() {
        let xml = r#"<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:response><D:propstat><D:prop>
    <C:calendar-data>BEGIN:VCALENDAR&#13;
BEGIN:VEVENT&#13;
UID:abc-123&#13;
SUMMARY:Standup&#13;
DTSTART:20260115T090000Z&#13;
DTEND:20260115T091500Z&#13;
LOCATION:Room 1&#13;
END:VEVENT&#13;
END:VCALENDAR&#13;
</C:calendar-data>
  </D:prop></D:propstat></D:response>
</D:multistatus>"#;
        let blobs = extract_calendar_data(xml).unwrap();
        assert_eq!(blobs.len(), 1);
        let cal: Calendar = blobs[0].parse().unwrap();
        let ev = cal.components.iter().find_map(|c| c.as_event()).unwrap();
        let json = event_to_json(ev);
        assert_eq!(json["uid"], "abc-123");
        assert_eq!(json["title"], "Standup");
        assert_eq!(json["start"], "2026-01-15T09:00:00Z");
        assert_eq!(json["location"], "Room 1");
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
        let result = list_events(
            &client,
            &collection,
            &auth,
            "2026-01-01T00:00:00Z",
            "2027-01-01T00:00:00Z",
        )
        .await
        .expect("list_events");
        println!("LIVE list_events -> {}", serde_json::to_string_pretty(&result).unwrap());
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
            "description": "created by octo-connector-caldav live test; safe to delete"
        });

        let created = create_event(&client, &collection, &auth, &params, uid).await.expect("create");
        println!("created -> {created}");

        let listed = list_events(
            &client,
            &collection,
            &auth,
            "2026-06-30T00:00:00Z",
            "2026-07-02T00:00:00Z",
        )
        .await
        .expect("list");
        println!("listed -> {}", serde_json::to_string_pretty(&listed).unwrap());
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

    #[test]
    fn finds_href_inside_property() {
        let xml = r#"<multistatus xmlns="DAV:">
  <response>
    <href>/principals/</href>
    <propstat><prop>
      <current-user-principal><href>/principals/users/me/</href></current-user-principal>
    </prop></propstat>
  </response>
</multistatus>"#;
        assert_eq!(
            href_inside(xml, b"current-user-principal").as_deref(),
            Some("/principals/users/me/")
        );
    }

    #[test]
    fn parses_calendar_home_listing() {
        // A Yandex-shaped listing: DAV default namespace, one VEVENT calendar,
        // one VTODO calendar, and a schedule-inbox that must be skipped.
        let xml = r#"<multistatus xmlns="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <response>
    <href>/calendars/me/events-1/</href>
    <propstat><prop>
      <resourcetype><collection/><C:calendar/></resourcetype>
      <displayname>My events</displayname>
      <C:supported-calendar-component-set><C:comp name="VEVENT"/></C:supported-calendar-component-set>
    </prop></propstat>
  </response>
  <response>
    <href>/calendars/me/todos-2/</href>
    <propstat><prop>
      <resourcetype><collection/><C:calendar/></resourcetype>
      <displayname>My todos</displayname>
      <C:supported-calendar-component-set><C:comp name="VTODO"/></C:supported-calendar-component-set>
    </prop></propstat>
  </response>
  <response>
    <href>/calendars/me/inbox/</href>
    <propstat><prop>
      <resourcetype><collection/><C:schedule-inbox/></resourcetype>
      <displayname>Inbox</displayname>
    </prop></propstat>
  </response>
</multistatus>"#;
        let cals = parse_calendars(xml);
        assert_eq!(cals.len(), 3);
        let events: Vec<&CalInfo> = cals.iter().filter(|c| c.is_event_calendar()).collect();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].href, "/calendars/me/events-1/");
        assert_eq!(events[0].name, "My events");
    }

    #[test]
    fn resolves_hrefs_against_origin() {
        assert_eq!(origin_of("https://caldav.yandex.ru/calendars/x/"), "https://caldav.yandex.ru");
        assert_eq!(
            resolve_url("https://caldav.yandex.ru", "/calendars/me/events-1/"),
            "https://caldav.yandex.ru/calendars/me/events-1/"
        );
        assert_eq!(
            resolve_url("https://caldav.yandex.ru", "https://other.example/c/"),
            "https://other.example/c/"
        );
    }

    /// Live discovery against a real server root. Ignored; run with:
    ///   OCTO_YANDEX_APP_PASSWORD=... OCTO_TEST_CALDAV_LOGIN=... \
    ///   OCTO_TEST_CALDAV_BASE_URL=https://caldav.yandex.ru \
    ///   cargo test -p octo-connector-caldav -- --ignored --nocapture live_discover
    #[tokio::test]
    #[ignore]
    async fn live_discover() {
        use octo_http_auth::{AuthConfig, HttpAuth};
        let login = std::env::var("OCTO_TEST_CALDAV_LOGIN").expect("OCTO_TEST_CALDAV_LOGIN");
        let base_url = std::env::var("OCTO_TEST_CALDAV_BASE_URL").expect("OCTO_TEST_CALDAV_BASE_URL");
        let calendar = std::env::var("OCTO_TEST_CALDAV_CALENDAR").ok();
        let auth = HttpAuth::new(AuthConfig::Basic {
            login,
            password_env: "OCTO_YANDEX_APP_PASSWORD".into(),
        });
        let client = reqwest::Client::new();
        let collection = discover_collection(&client, &base_url, &auth, calendar.as_deref())
            .await
            .expect("discover_collection");
        println!("LIVE discover_collection -> {collection}");
        assert!(collection.starts_with("http"));
    }
}
