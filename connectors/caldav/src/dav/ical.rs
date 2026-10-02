use chrono::{
    DateTime, Duration as ChronoDuration, NaiveDate, NaiveDateTime, SecondsFormat, TimeZone, Utc,
};
use rrule::RRuleSet;
use serde_json::{Map, Value, json};

use super::MAX_OCCURRENCES;

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
pub(super) fn expand_calendar(
    ical: &str,
    win_start: DateTime<Utc>,
    win_end: DateTime<Utc>,
    tz: chrono_tz::Tz,
    out: &mut Vec<Value>,
) {
    let vevents = parse_vevents(ical);
    let (masters, overrides): (Vec<_>, Vec<_>) = vevents
        .iter()
        .partition(|ve| ve.get("RECURRENCE-ID").is_none());

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
        if ov
            .value("STATUS")
            .is_some_and(|s| s.eq_ignore_ascii_case("CANCELLED"))
        {
            continue;
        }
        if let Some(start) = ov.datetime("DTSTART") {
            if start >= win_start && start <= win_end {
                out.push(emit(ov, start, start + duration_of(ov, start), tz));
            }
        }
    }

    for m in &masters {
        let Some(start) = m.datetime("DTSTART") else {
            continue;
        };
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
                tracing::debug!(
                    uid = m.value("UID").unwrap_or_default(),
                    "caldav: RRULE expansion failed; emitting master as-is"
                );
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
    Some(
        result
            .dates
            .into_iter()
            .map(|d| d.with_timezone(&Utc))
            .collect(),
    )
}

/// Build the JSON event the agent sees: `{ uid, title, start, end, location? }`,
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
    let render = |dt: DateTime<Utc>| {
        dt.with_timezone(&tz)
            .to_rfc3339_opts(SecondsFormat::Secs, true)
    };
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
    Some((
        name.to_ascii_uppercase(),
        params.to_string(),
        value.to_string(),
    ))
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
    if param_value(params, "VALUE").is_some_and(|v| v.eq_ignore_ascii_case("DATE"))
        || !value.contains('T')
    {
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
    let is_date = param_value(params, "VALUE").is_some_and(|v| v.eq_ignore_ascii_case("DATE"))
        || !value.contains('T');
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
