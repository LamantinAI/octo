//! Calendar recurrence: a cron expression evaluated in an IANA timezone, so "weekdays at
//! 09:00" stays at 09:00 local time across daylight-saving changes (an `interval` counts
//! seconds from creation and drifts off the clock).

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use croner::Cron;

/// The first time strictly after `after` that `expr` matches in `tz`, as UTC. Also the
/// validation at `add_alarm`: a bad expression or zone is refused there, not when it fires.
pub fn next_after(expr: &str, tz: &str, after: DateTime<Utc>) -> Result<DateTime<Utc>, String> {
    let (cron, zone) = parse(expr, tz)?;
    cron.find_next_occurrence(&after.with_timezone(&zone), false)
        .map(|t| t.with_timezone(&Utc))
        .map_err(|e| format!("cron {expr:?} has no next occurrence: {e}"))
}

fn parse(expr: &str, tz: &str) -> Result<(Cron, Tz), String> {
    let zone: Tz = tz.parse().map_err(|_| format!("unknown timezone {tz:?} (use an IANA name like \"Europe/Moscow\")"))?;
    let cron: Cron = expr
        .parse()
        .map_err(|e| format!("bad cron expression {expr:?}: {e} (5 fields: min hour day-of-month month day-of-week)"))?;
    Ok((cron, zone))
}

#[cfg(test)]
mod tests {
    use super::next_after;
    use chrono::{DateTime, Utc};

    fn utc(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn weekdays_at_nine_skip_the_weekend() {
        // Friday 2026-09-25 10:00 Moscow (07:00 UTC) -> next is Monday 09:00 Moscow.
        let next = next_after("0 9 * * 1-5", "Europe/Moscow", utc("2026-09-25T07:00:00Z")).unwrap();
        assert_eq!(next, utc("2026-09-28T06:00:00Z"));
    }

    #[test]
    fn local_time_holds_across_a_dst_change() {
        // Berlin leaves summer time on 2026-10-25: 09:00 local is 07:00 UTC before, 08:00 after.
        let before = next_after("0 9 * * *", "Europe/Berlin", utc("2026-10-23T12:00:00Z")).unwrap();
        let after = next_after("0 9 * * *", "Europe/Berlin", utc("2026-10-25T12:00:00Z")).unwrap();
        assert_eq!(before, utc("2026-10-24T07:00:00Z"));
        assert_eq!(after, utc("2026-10-26T08:00:00Z"));
    }

    #[test]
    fn the_same_instant_is_not_its_own_next() {
        let at = utc("2026-09-28T06:00:00Z"); // exactly Monday 09:00 Moscow
        assert_eq!(next_after("0 9 * * 1-5", "Europe/Moscow", at).unwrap(), utc("2026-09-29T06:00:00Z"));
    }

    #[test]
    fn bad_input_is_refused_with_a_reason() {
        let now = utc("2026-09-25T07:00:00Z");
        assert!(next_after("0 25 * * *", "UTC", now).unwrap_err().contains("bad cron"));
        assert!(next_after("0 9 * * *", "Mars/Olympus", now).unwrap_err().contains("unknown timezone"));
    }
}
