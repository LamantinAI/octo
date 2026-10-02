use chrono::{DateTime, Duration as ChronoDuration, Utc};
use icalendar::{Alarm, Calendar, CalendarDateTime, Component, Event, EventLike, Trigger};
use rrule::RRuleSet;
use serde_json::Value;

use super::{DavError, str_field, to_utc};

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
///
/// An optional `recurrence` (an RFC 5545 RRULE value, e.g. `FREQ=WEEKLY;BYDAY=MO,TU,WE,TH,FR`)
/// makes it a recurring series; the alarm then fires before every occurrence. A recurring
/// event's start/end are written in the connector's timezone (`DTSTART;TZID=…`), not UTC —
/// a series anchored in UTC would slide by an hour across daylight-saving changes.
pub(super) fn build_event_ics(
    params: &Value,
    uid: &str,
    default_reminder: Option<i64>,
    tz: chrono_tz::Tz,
) -> Result<String, DavError> {
    let title = str_field(params, "title")?;
    let start = to_utc(str_field(params, "start")?)?;
    let end = to_utc(str_field(params, "end")?)?;
    let rule = recurrence(params, start, tz)?;

    let mut event = Event::new();
    event.uid(uid).summary(title);
    match &rule {
        Some(rule) if tz != chrono_tz::UTC => {
            let local = |t: DateTime<Utc>| CalendarDateTime::WithTimezone {
                date_time: t.with_timezone(&tz).naive_local(),
                tzid: tz.name().to_string(),
            };
            event
                .starts(local(start))
                .ends(local(end))
                .add_property("RRULE", rule);
        }
        _ => {
            event
                .starts(CalendarDateTime::from(start))
                .ends(CalendarDateTime::from(end));
            if let Some(rule) = &rule {
                event.add_property("RRULE", rule);
            }
        }
    }
    if let Some(d) = params.get("description").and_then(Value::as_str) {
        event.description(d);
    }
    if let Some(l) = params.get("location").and_then(Value::as_str) {
        event.location(l);
    }
    // A per-event `reminder_minutes` overrides the connector default; a non-negative
    // result becomes a display alarm `n` minutes before the start.
    let reminder = params
        .get("reminder_minutes")
        .and_then(Value::as_i64)
        .or(default_reminder);
    if let Some(minutes) = reminder.filter(|m| *m >= 0) {
        let trigger = Trigger::before_start(ChronoDuration::minutes(minutes));
        event.alarm(Alarm::display(title, trigger));
    }

    Ok(Calendar::new().push(event.done()).done().to_string())
}

/// The event's `recurrence` rule (an optional `RRULE:` prefix dropped), checked by the
/// `rrule` engine against the start so a bad rule is refused before anything is written.
fn recurrence(
    params: &Value,
    start: DateTime<Utc>,
    tz: chrono_tz::Tz,
) -> Result<Option<String>, DavError> {
    let Some(raw) = params.get("recurrence").and_then(Value::as_str) else {
        return Ok(None);
    };
    let rule = raw.trim().trim_start_matches("RRULE:").trim().to_string();
    if rule.is_empty() {
        return Ok(None);
    }
    let local = start.with_timezone(&tz).format("%Y%m%dT%H%M%S");
    let spec = format!("DTSTART;TZID={}:{local}\nRRULE:{rule}", tz.name());
    spec.parse::<RRuleSet>()
        .map_err(|e| DavError::BadRecurrence(e.to_string()))?;
    Ok(Some(rule))
}
