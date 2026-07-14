//! The CalDAV protocol operations (RFC 4791): list / create / delete VEVENTs
//! over WebDAV verbs, with iCalendar bodies. Transport-agnostic of the Octo
//! connector — pure request/response functions the connector calls.

use chrono::{
    DateTime, Duration as ChronoDuration, NaiveDate, NaiveDateTime, SecondsFormat, TimeZone, Utc,
};
use icalendar::{Alarm, Calendar, CalendarDateTime, Component, Event, EventLike, Trigger};
use octo_http_auth::HttpAuth;
use rrule::RRuleSet;
use serde_json::{json, Map, Value};

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
        .header(reqwest::header::CONTENT_TYPE, "application/xml; charset=utf-8")
        .body(body);
    let resp = auth.apply(req).await?.send().await?;
    let status = resp.status();
    let text = resp.text().await?;
    if !status.is_success() {
        return Err(DavError::Status { status: status.as_u16(), body: truncate(&text) });
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
) -> Result<Value, DavError> {
    let ics = build_event_ics(params, uid, default_reminder)?;

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

/// Build the iCalendar body for a new VEVENT. Split out from the HTTP `PUT` so the
/// event shape — in particular the reminder VALARM — is unit-testable without a
/// server.
///
/// A popup reminder is attached when a lead time resolves: the per-event
/// `reminder_minutes` field if present, else the connector's `default_reminder`.
/// A non-negative value adds a `VALARM;ACTION=DISPLAY` whose `TRIGGER` is that many
/// minutes before the start, relative to `DTSTART` (a negative RFC 5545 duration) —
/// the standard way every CalDAV server (Google, Yandex, Fastmail, Nextcloud,
/// iCloud) raises a notification. A negative value (or no lead time at all) creates
/// a plain event with no alarm.
fn build_event_ics(params: &Value, uid: &str, default_reminder: Option<i64>) -> Result<String, DavError> {
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
    // A per-event `reminder_minutes` overrides the connector default; a non-negative
    // result becomes a display alarm `n` minutes before the start.
    let reminder = params.get("reminder_minutes").and_then(Value::as_i64).or(default_reminder);
    if let Some(minutes) = reminder.filter(|m| *m >= 0) {
        let trigger = Trigger::before_start(ChronoDuration::minutes(minutes));
        event.alarm(Alarm::display(title, trigger));
    }

    Ok(Calendar::new().push(event.done()).done().to_string())
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
struct RawVevent {
    props: Vec<(String, String, String)>,
}

impl RawVevent {
    fn get(&self, name: &str) -> Option<&(String, String, String)> {
        self.props.iter().find(|(n, _, _)| n == name)
    }
    fn value(&self, name: &str) -> Option<&str> {
        self.get(name).map(|(_, _, v)| v.as_str())
    }
    fn all<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a (String, String, String)> {
        self.props.iter().filter(move |(n, _, _)| n == name)
    }
    fn datetime(&self, name: &str) -> Option<DateTime<Utc>> {
        self.get(name).and_then(|(_, p, v)| parse_ical_dt(p, v))
    }
}

/// Parse one VCALENDAR blob, expanding recurrences into the window and pushing
/// each resulting event as JSON onto `out`.
fn expand_calendar(
    ical: &str,
    win_start: DateTime<Utc>,
    win_end: DateTime<Utc>,
    tz: chrono_tz::Tz,
    out: &mut Vec<Value>,
) {
    let vevents = parse_vevents(ical);
    let (masters, overrides): (Vec<_>, Vec<_>) =
        vevents.iter().partition(|ve| ve.get("RECURRENCE-ID").is_none());

    // Slots (uid + original-occurrence instant) that a modified instance replaces,
    // so the master expansion skips them.
    let mut overridden: std::collections::HashSet<(String, i64)> = std::collections::HashSet::new();
    for ov in &overrides {
        if let (Some(uid), Some((_, p, v))) = (ov.value("UID"), ov.get("RECURRENCE-ID")) {
            if let Some(rid) = parse_ical_dt(p, v) {
                overridden.insert((uid.to_string(), rid.timestamp()));
            }
        }
    }

    // Emit modified instances at their (possibly moved) real time, unless cancelled.
    for ov in &overrides {
        if ov.value("STATUS").is_some_and(|s| s.eq_ignore_ascii_case("CANCELLED")) {
            continue;
        }
        if let Some(start) = ov.datetime("DTSTART") {
            if start >= win_start && start <= win_end {
                out.push(emit(ov, start, start + duration_of(ov, start), tz));
            }
        }
    }

    for m in &masters {
        let Some(start) = m.datetime("DTSTART") else { continue };
        let dur = duration_of(m, start);
        let recurring = m.all("RRULE").next().is_some() || m.all("RDATE").next().is_some();
        if !recurring {
            // Single event — the server already constrained it to the window.
            out.push(emit(m, start, start + dur, tz));
            continue;
        }
        match expand_master(m, win_start, win_end) {
            Some(occurrences) => {
                let uid = m.value("UID").unwrap_or_default().to_string();
                for occ in occurrences {
                    if overridden.contains(&(uid.clone(), occ.timestamp())) {
                        continue; // replaced by a RECURRENCE-ID instance emitted above
                    }
                    out.push(emit(m, occ, occ + dur, tz));
                }
            }
            // Couldn't expand (unparseable rule/tz) — surface the master rather
            // than lose the event; its date will be the series origin.
            None => {
                tracing::debug!(uid = m.value("UID").unwrap_or_default(), "caldav: RRULE expansion failed; emitting master as-is");
                out.push(emit(m, start, start + dur, tz));
            }
        }
    }
}

/// Materialise a recurring master's occurrences within `[win_start, win_end]`
/// (inclusive) as UTC instants. Feeds the `rrule` engine the DTSTART/RRULE/
/// EXDATE/RDATE lines verbatim. Returns `None` if the rule can't be parsed.
fn expand_master(
    m: &RawVevent,
    win_start: DateTime<Utc>,
    win_end: DateTime<Utc>,
) -> Option<Vec<DateTime<Utc>>> {
    let (_, dsp, dsv) = m.get("DTSTART")?;
    let (dsp, dsv) = normalize_dtstart(dsp, dsv);
    let mut spec = format!("DTSTART{dsp}:{dsv}");
    for (_, _, v) in m.all("RRULE") {
        spec.push_str(&format!("\nRRULE:{v}"));
    }
    for (_, p, v) in m.all("RDATE") {
        spec.push_str(&format!("\nRDATE{p}:{v}"));
    }
    for (_, p, v) in m.all("EXDATE") {
        spec.push_str(&format!("\nEXDATE{p}:{v}"));
    }

    let set: RRuleSet = spec.parse().ok()?;
    let after = win_start.with_timezone(&rrule::Tz::UTC);
    let before = win_end.with_timezone(&rrule::Tz::UTC);
    let result = set.after(after).before(before).all(MAX_OCCURRENCES);
    if result.limited {
        tracing::warn!(
            uid = m.value("UID").unwrap_or_default(),
            cap = MAX_OCCURRENCES,
            "caldav: recurrence hit the occurrence cap; window may be under-reported"
        );
    }
    Some(result.dates.into_iter().map(|d| d.with_timezone(&Utc)).collect())
}

/// Build the JSON event Albert sees: `{ uid, title, start, end, location? }`,
/// with `start`/`end` as RFC3339 in the configured display timezone `tz` (UTC
/// renders with a `Z`, other zones with their offset, e.g. `+03:00`), so the
/// agent reads local wall-clock times without doing any timezone arithmetic.
fn emit(ve: &RawVevent, start: DateTime<Utc>, end: DateTime<Utc>, tz: chrono_tz::Tz) -> Value {
    let mut o = Map::new();
    if let Some(v) = ve.value("UID") {
        o.insert("uid".into(), json!(v));
    }
    if let Some(v) = ve.value("SUMMARY") {
        o.insert("title".into(), json!(unescape_text(v)));
    }
    let render = |dt: DateTime<Utc>| dt.with_timezone(&tz).to_rfc3339_opts(SecondsFormat::Secs, true);
    o.insert("start".into(), json!(render(start)));
    o.insert("end".into(), json!(render(end)));
    if let Some(v) = ve.value("LOCATION") {
        o.insert("location".into(), json!(unescape_text(v)));
    }
    Value::Object(o)
}

/// Event duration from DTEND (preferred) or DURATION; zero if neither is usable.
fn duration_of(ve: &RawVevent, start: DateTime<Utc>) -> ChronoDuration {
    if let Some(end) = ve.datetime("DTEND") {
        let d = end - start;
        if d >= ChronoDuration::zero() {
            return d;
        }
    }
    ve.value("DURATION")
        .and_then(parse_ical_duration)
        .unwrap_or_else(ChronoDuration::zero)
}

/// Unfold RFC5545 continuation lines and split a VCALENDAR body into its VEVENTs.
fn parse_vevents(ical: &str) -> Vec<RawVevent> {
    // Unfold: a line beginning with a space or tab continues the previous one.
    let mut logical: Vec<String> = Vec::new();
    for raw in ical.split('\n') {
        let line = raw.strip_suffix('\r').unwrap_or(raw);
        if (line.starts_with(' ') || line.starts_with('\t')) && !logical.is_empty() {
            logical.last_mut().unwrap().push_str(&line[1..]);
        } else {
            logical.push(line.to_string());
        }
    }

    let mut out = Vec::new();
    let mut cur: Option<Vec<(String, String, String)>> = None;
    for line in &logical {
        let trimmed = line.trim();
        if trimmed.eq_ignore_ascii_case("BEGIN:VEVENT") {
            cur = Some(Vec::new());
        } else if trimmed.eq_ignore_ascii_case("END:VEVENT") {
            if let Some(props) = cur.take() {
                out.push(RawVevent { props });
            }
        } else if let Some(props) = cur.as_mut() {
            if let Some(p) = parse_property_line(line) {
                props.push(p);
            }
        }
    }
    out
}

/// Split a content line into `(NAME, params, value)`; `params` retains its
/// leading `;`. The name/value boundary is the first colon not inside quotes.
fn parse_property_line(line: &str) -> Option<(String, String, String)> {
    let mut in_quote = false;
    let mut colon = None;
    for (i, c) in line.char_indices() {
        match c {
            '"' => in_quote = !in_quote,
            ':' if !in_quote => {
                colon = Some(i);
                break;
            }
            _ => {}
        }
    }
    let colon = colon?;
    let (name_params, value) = (&line[..colon], &line[colon + 1..]);
    let (name, params) = match name_params.find(';') {
        Some(s) => (&name_params[..s], &name_params[s..]),
        None => (name_params, ""),
    };
    Some((name.to_ascii_uppercase(), params.to_string(), value.to_string()))
}

/// Value of an iCal parameter (e.g. `TZID`) from a `;K=V;K=V` params string.
fn param_value(params: &str, key: &str) -> Option<String> {
    for part in params.trim_start_matches(';').split(';') {
        let mut kv = part.splitn(2, '=');
        if kv.next().is_some_and(|k| k.eq_ignore_ascii_case(key)) {
            return kv.next().map(|v| v.trim_matches('"').to_string());
        }
    }
    None
}

/// Parse an iCal DATE-TIME (or DATE) value to a UTC instant, honouring a `Z`
/// suffix, a `TZID` parameter, or `VALUE=DATE`; floating times are read as UTC.
fn parse_ical_dt(params: &str, value: &str) -> Option<DateTime<Utc>> {
    if param_value(params, "VALUE").is_some_and(|v| v.eq_ignore_ascii_case("DATE")) || !value.contains('T') {
        let date = NaiveDate::parse_from_str(value, "%Y%m%d").ok()?;
        return Some(Utc.from_utc_datetime(&date.and_hms_opt(0, 0, 0)?));
    }
    if let Some(z) = value.strip_suffix('Z') {
        let naive = NaiveDateTime::parse_from_str(z, "%Y%m%dT%H%M%S").ok()?;
        return Some(Utc.from_utc_datetime(&naive));
    }
    let naive = NaiveDateTime::parse_from_str(value, "%Y%m%dT%H%M%S").ok()?;
    if let Some(tz) = param_value(params, "TZID").and_then(|t| t.parse::<chrono_tz::Tz>().ok()) {
        return tz
            .from_local_datetime(&naive)
            .single()
            .or_else(|| tz.from_local_datetime(&naive).earliest())
            .map(|dt| dt.with_timezone(&Utc))
            .or_else(|| Some(Utc.from_utc_datetime(&naive)));
    }
    Some(Utc.from_utc_datetime(&naive)) // floating — best-effort UTC
}

/// Prepare a master's DTSTART for the `rrule` parser: an all-day (`VALUE=DATE`)
/// origin becomes a concrete UTC midnight so occurrences carry a time; other
/// forms (TZID / `Z` / floating) pass through unchanged.
fn normalize_dtstart(params: &str, value: &str) -> (String, String) {
    let is_date =
        param_value(params, "VALUE").is_some_and(|v| v.eq_ignore_ascii_case("DATE")) || !value.contains('T');
    if is_date {
        if let Ok(date) = NaiveDate::parse_from_str(value, "%Y%m%d") {
            return (String::new(), format!("{}T000000Z", date.format("%Y%m%d")));
        }
    }
    (params.to_string(), value.to_string())
}

/// Parse an RFC5545 DURATION (e.g. `PT30M`, `P1DT2H`, `P1W`); `None` if malformed.
fn parse_ical_duration(s: &str) -> Option<ChronoDuration> {
    let neg = s.starts_with('-');
    let body = s.trim_start_matches(['+', '-']).strip_prefix('P')?;
    let (date_part, time_part) = match body.split_once('T') {
        Some((d, t)) => (d, Some(t)),
        None => (body, None),
    };
    let mut secs: i64 = 0;
    let mut acc = String::new();
    for c in date_part.chars() {
        if c.is_ascii_digit() {
            acc.push(c);
        } else {
            let n: i64 = acc.parse().ok()?;
            acc.clear();
            secs += match c {
                'W' => n * 7 * 86_400,
                'D' => n * 86_400,
                _ => return None,
            };
        }
    }
    if let Some(t) = time_part {
        for c in t.chars() {
            if c.is_ascii_digit() {
                acc.push(c);
            } else {
                let n: i64 = acc.parse().ok()?;
                acc.clear();
                secs += match c {
                    'H' => n * 3_600,
                    'M' => n * 60,
                    'S' => n,
                    _ => return None,
                };
            }
        }
    }
    Some(ChronoDuration::seconds(if neg { -secs } else { secs }))
}

/// Unescape an iCal TEXT value (`\,` `\;` `\n` `\\`).
fn unescape_text(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('n') | Some('N') => out.push('\n'),
                Some(other) => out.push(other),
                None => out.push('\\'),
            }
        } else {
            out.push(c);
        }
    }
    out
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

    fn win(from: &str, to: &str) -> (DateTime<Utc>, DateTime<Utc>) {
        (to_utc(from).unwrap(), to_utc(to).unwrap())
    }

    #[test]
    fn ical_utc_round_trips() {
        assert_eq!(to_ical_utc("2026-01-15T12:30:00Z").unwrap(), "20260115T123000Z");
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
    fn reminder_attaches_a_display_valarm_before_start() {
        let ics = build_event_ics(&event(Some(10)), "uid-1", None).unwrap();
        assert!(ics.contains("BEGIN:VALARM"), "a reminder should add a VALARM:\n{ics}");
        assert!(ics.contains("ACTION:DISPLAY"), "the alarm should be a display popup:\n{ics}");
        assert!(ics.contains(&trigger_value(10)), "trigger should fire 10 min before:\n{ics}");
    }

    #[test]
    fn per_event_reminder_overrides_the_connector_default() {
        // Per-event 5 min must win over the connector's 30 min default.
        let ics = build_event_ics(&event(Some(5)), "uid-2", Some(30)).unwrap();
        assert!(ics.contains(&trigger_value(5)), "per-event 5 min should win:\n{ics}");
        assert!(!ics.contains(&trigger_value(30)), "the 30 min default must not leak in:\n{ics}");
    }

    #[test]
    fn connector_default_reminder_applies_when_event_omits_it() {
        let ics = build_event_ics(&event(None), "uid-3", Some(10)).unwrap();
        assert!(ics.contains("BEGIN:VALARM"), "the connector default should add an alarm:\n{ics}");
        assert!(ics.contains(&trigger_value(10)), "default 10 min lead time:\n{ics}");
    }

    #[test]
    fn no_reminder_yields_a_plain_event() {
        let ics = build_event_ics(&event(None), "uid-4", None).unwrap();
        assert!(!ics.contains("VALARM"), "no lead time -> no alarm:\n{ics}");
    }

    #[test]
    fn negative_reminder_suppresses_the_default() {
        // An explicit -1 opts out even though the connector has a default.
        let ics = build_event_ics(&event(Some(-1)), "uid-5", Some(10)).unwrap();
        assert!(!ics.contains("VALARM"), "-1 opts out of the default:\n{ics}");
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
        let from =
            std::env::var("OCTO_TEST_CALDAV_FROM").unwrap_or_else(|_| "2026-01-01T00:00:00Z".into());
        let to =
            std::env::var("OCTO_TEST_CALDAV_TO").unwrap_or_else(|_| "2027-01-01T00:00:00Z".into());
        let tz: chrono_tz::Tz = std::env::var("OCTO_TEST_CALDAV_TZ")
            .ok()
            .and_then(|t| t.parse().ok())
            .unwrap_or(chrono_tz::UTC);
        let result =
            list_events(&client, &collection, &auth, &from, &to, tz).await.expect("list_events");
        println!("LIVE list_events [{from} .. {to}] ({tz}) -> {}", serde_json::to_string_pretty(&result).unwrap());
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

        let created = create_event(&client, &collection, &auth, &params, uid, Some(10)).await.expect("create");
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
