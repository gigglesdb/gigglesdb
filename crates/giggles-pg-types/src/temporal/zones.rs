//! Explicit, deterministic named-zone conversion. See TEMPORAL_ZONES.md.
use super::*;
use chrono_tz::Tz;
#[path = "tzdata.rs"]
mod embedded;
use std::sync::{Mutex, OnceLock};
fn database(tz: Tz) -> Result<std::sync::Arc<tz::TimeZone>, Error> {
    static CACHE: OnceLock<Mutex<std::collections::HashMap<Tz, std::sync::Arc<tz::TimeZone>>>> =
        OnceLock::new();
    let mut cache = CACHE
        .get_or_init(Default::default)
        .lock()
        .map_err(|_| Error::Unimplemented)?;
    if let Some(v) = cache.get(&tz) {
        return Ok(v.clone());
    }
    let bytes = embedded::FILES
        .iter()
        .find(|(n, _)| {
            *n == match tz {
                Tz::EST5EDT => "America/New_York",
                Tz::PST8PDT => "America/Los_Angeles",
                _ => tz.name(),
            }
        })
        .ok_or(Error::Unimplemented)?
        .1;
    let zone =
        std::sync::Arc::new(tz::TimeZone::from_tz_data(bytes).map_err(|_| Error::Unimplemented)?);
    cache.insert(tz, zone.clone());
    Ok(zone)
}
fn offset(zone: &tz::TimeZone, unix: i64) -> Result<i32, Error> {
    zone.find_local_time_type(unix)
        .map(|v| v.ut_offset())
        .map_err(|_| Error::Unimplemented)
}
const UNIX_EPOCH_DELTA: i64 = 946684800;
/// Names exposed by PostgreSQL 18.6's pinned timezone installation.
pub const TIMEZONE_NAMES: &str = include_str!("timezone_names.txt");
fn named_zone(name: &str) -> Option<(Tz, &'static str)> {
    let canonical = TIMEZONE_NAMES
        .lines()
        .find(|n| n.eq_ignore_ascii_case(name))?;
    let target = match canonical {
        "localtime" => "Etc/UTC",
        "posixrules" => "America/New_York",
        "Factory" => "UTC",
        name => name,
    };
    chrono_tz::TZ_VARIANTS
        .iter()
        .find(|z| z.name() == target)
        .copied()
        .map(|z| (z, canonical))
}
/// Current timezone presentation at an explicit Unix second, without a host clock.
pub fn timezone_info(name: &str, unix: i64) -> Result<(String, i32, bool), Error> {
    if name == "Factory" {
        return Ok(("-00".into(), 0, false));
    }
    let (zone, _) = named_zone(name).ok_or(Error::Syntax)?;
    let db = database(zone)?;
    let info = db.find_local_time_type(unix).map_err(|_| Error::Range)?;
    Ok((
        info.time_zone_designation().into(),
        info.ut_offset(),
        info.is_dst(),
    ))
}
/// Version of the embedded TZif data; chrono-tz supplies zone identifiers only.
pub const TZDATA_VERSION: &str = "2026f";
/// A session zone never consults process settings or the clock.
#[derive(Clone, Copy, Debug)]
pub enum SessionZone {
    Fixed(i32),
    Named(Tz),
}
impl SessionZone {
    /// Distinct historical abbreviations of this session zone, in TZif order.
    /// The first type wins when a designation occurs with multiple offsets.
    pub fn abbreviations(self) -> Result<Vec<(String, i32, bool)>, Error> {
        let Self::Named(zone) = self else {
            return Ok(Vec::new());
        };
        let db = database(zone)?;
        let mut rows = Vec::new();
        for info in db.as_ref().as_ref().local_time_types() {
            let name = info.time_zone_designation();
            if !rows.iter().any(|(n, _, _)| n == name) {
                rows.push((name.to_owned(), info.ut_offset(), info.is_dst()));
            }
        }
        Ok(rows)
    }
    pub fn fixed(seconds_east: i32) -> Result<Self, Error> {
        Ok(Self::Fixed(offset_valid(seconds_east)?))
    }
    pub fn named(name: &str) -> Result<Self, Error> {
        let name = bounded(name)?;
        let zone = named_zone(name)
            .map(|(zone, _)| zone)
            .or(match name {
                "EST5EDT" => Some(Tz::America__New_York),
                "PST8PDT" => Some(Tz::America__Los_Angeles),
                _ => None,
            });
        match zone {
            Some(zone) => Ok(Self::Named(zone)),
            None => posix_offset(name).map(Self::Fixed).ok_or(Error::Syntax),
        }
    }
    /// PostgreSQL TimeZone GUC: numeric hours are east, POSIX strings west.
    pub fn setting(value: &str) -> Result<(Self, String), Error> {
        let value = bounded(value)?;
        if value.is_empty()
            || ["Z", "Zulu", "PST"]
                .iter()
                .any(|v| value.eq_ignore_ascii_case(v))
        {
            return Err(Error::Syntax);
        }
        if let Ok(hours) = value.parse::<f64>() {
            if !hours.is_finite() || hours.abs() >= 168.0 {
                return Err(Error::Range);
            }
            let seconds = (hours * 3600.0).round() as i32;
            return Self::interval_setting(seconds);
        }
        if let Some((zone, label)) = named_zone(value) {
            return Ok((Self::Named(zone), label.into()));
        }
        if value.eq_ignore_ascii_case("EST5EDT") {
            return Ok((Self::Named(Tz::America__New_York), value.into()));
        }
        if value.eq_ignore_ascii_case("PST8PDT") {
            return Ok((Self::Named(Tz::America__Los_Angeles), value.into()));
        }
        let seconds = posix_offset(value).ok_or(Error::Syntax)?;
        Ok((Self::Fixed(seconds), value.into()))
    }
    /// Canonical label for numeric and interval TimeZone assignments.
    pub fn interval_setting(seconds: i32) -> Result<(Self, String), Error> {
        session_offset(seconds)?;
        let east = zone_suffix(seconds);
        let west = zone_suffix(-seconds);
        Ok((Self::Fixed(seconds), format!("<{east}>{west}")))
    }
    fn local_offset(self, local: Timestamp) -> Result<i32, Error> {
        let tz = match self {
            Self::Fixed(v) => return session_offset(v),
            Self::Named(tz) => database(tz)?,
        };
        let unix = local.0.div_euclid(1_000_000) + UNIX_EPOCH_DELTA;
        let view = tz.as_ref().as_ref();
        let mut offsets: Vec<i32> = view
            .local_time_types()
            .iter()
            .map(|t| t.ut_offset())
            .collect();
        // Tail types are normally in the TZif type table, but include them explicitly.
        if let Some(rule) = view.extra_rule() {
            use tz::timezone::TransitionRule;
            match rule {
                TransitionRule::Fixed(t) => offsets.push(t.ut_offset()),
                TransitionRule::Alternate(t) => {
                    offsets.push(t.std().ut_offset());
                    offsets.push(t.dst().ut_offset());
                }
            }
        }
        offsets.sort_unstable();
        offsets.dedup();
        let mut valid = None;
        let mut gap = None;
        for off in offsets {
            let candidate = unix - i64::from(off);
            let actual = offset(&tz, candidate)?;
            if actual == off {
                // Larger UTC instant wins an overlap (PG's post-transition side).
                if valid.is_none_or(|(v, _)| candidate > v) {
                    valid = Some((candidate, off));
                }
            } else if actual > off {
                let mut lo = unix - i64::from(actual);
                let mut hi = candidate;
                if offset(&tz, lo)? != off {
                    continue;
                }
                while hi - lo > 1 {
                    let mid = lo + (hi - lo) / 2;
                    if offset(&tz, mid)? == off {
                        lo = mid;
                    } else {
                        hi = mid;
                    }
                }
                let after = offset(&tz, hi)?;
                if after > off && unix >= hi + i64::from(off) && unix < hi + i64::from(after) {
                    gap = Some(off);
                }
            }
        }
        valid
            .map(|(_, off)| off)
            .or(gap)
            .ok_or(Error::Unimplemented)
            .and_then(offset_valid)
    }
    pub fn to_instant(self, local: Timestamp) -> Result<TimestampTz, Error> {
        if local.0 == i64::MIN || local.0 == i64::MAX {
            return TimestampTz::from_microseconds(local.0);
        }
        let off = self.local_offset(local)?;
        TimestampTz::from_microseconds(
            local
                .0
                .checked_sub(i64::from(off) * 1_000_000)
                .ok_or(Error::Range)?,
        )
    }
    pub fn to_local(self, instant: TimestampTz) -> Result<Timestamp, Error> {
        if instant.0 == i64::MIN || instant.0 == i64::MAX {
            return Timestamp::from_microseconds(instant.0);
        }
        let off = match self {
            Self::Fixed(v) => session_offset(v)?,
            Self::Named(Tz::UTC) => 0,
            Self::Named(tz) => offset(
                database(tz)?.as_ref(),
                instant.0.div_euclid(1_000_000) + UNIX_EPOCH_DELTA,
            )?,
        };
        Timestamp::from_microseconds(
            instant
                .0
                .checked_add(i64::from(off) * 1_000_000)
                .ok_or(Error::Range)?,
        )
    }
}
#[derive(Clone, Copy, Debug)]
pub struct SessionSettings {
    pub date_order: DateOrder,
    pub zone: SessionZone,
}
impl TimestampTz {
    pub fn parse_in_session(s: &str, settings: &SessionSettings) -> Result<Self, Error> {
        let s = bounded(s)?;
        let p = ParserSettings {
            date_order: settings.date_order,
            offset_east: 0,
        };
        if let Some((local, name)) = s.rsplit_once(' ')
            && (name.contains('/') || name.eq_ignore_ascii_case("UTC"))
        {
            return SessionZone::named(name)?.to_instant(Timestamp::parse(local, &p)?);
        }
        let time = s.split_once([' ', 'T']).map(|(_, t)| t).unwrap_or("");
        if time.contains(['+', '-']) || time.ends_with('Z') {
            return Self::parse(s, &p);
        }
        settings.zone.to_instant(Timestamp::parse(s, &p)?)
    }
    pub fn format_in_zone(self, zone: SessionZone) -> Result<String, Error> {
        let local = zone.to_local(self)?;
        if self.0 == i64::MIN || self.0 == i64::MAX {
            return Ok(local.to_string());
        }
        let off = ((i128::from(local.0) - i128::from(self.0)) / 1_000_000) as i32;
        let text = local.to_string();
        let (text, bc) = text
            .strip_suffix(" BC")
            .map_or((text.as_str(), ""), |v| (v, " BC"));
        Ok(format!("{text}{}{bc}", zone_suffix(off)))
    }
}
impl TimeTz {
    /// Date is mandatory for named zones; no current-date fallback.
    pub fn in_zone_on_date(time: Time, date: Date, zone: SessionZone) -> Result<Self, Error> {
        if date == Date::INFINITY || date == Date::NEG_INFINITY {
            return Err(Error::Range);
        }
        let local = Timestamp::from_microseconds(
            i64::from(date.0)
                .checked_mul(USECS_PER_DAY)
                .and_then(|v| v.checked_add(time.0))
                .ok_or(Error::Range)?,
        )?;
        Self::new(time, zone.local_offset(local)?)
    }
}

fn session_offset(seconds: i32) -> Result<i32, Error> {
    if seconds.unsigned_abs() >= 168 * 3600 {
        Err(Error::Range)
    } else {
        Ok(seconds)
    }
}
fn zone_suffix(seconds: i32) -> String {
    let sign = if seconds < 0 { '-' } else { '+' };
    let n = seconds.unsigned_abs();
    let mut s = format!("{sign}{:02}", n / 3600);
    if !n.is_multiple_of(3600) {
        s.push_str(&format!(":{:02}", n / 60 % 60));
    }
    if !n.is_multiple_of(60) {
        s.push_str(&format!(":{:02}", n % 60));
    }
    s
}
fn posix_offset(value: &str) -> Option<i32> {
    let offset = if value.starts_with('<') {
        &value[value.find('>')? + 1..]
    } else {
        let n = value.bytes().take_while(u8::is_ascii_alphabetic).count();
        if n != 0 && n < 3 {
            return None;
        }
        &value[n..]
    };
    let (sign, offset) = if let Some(v) = offset.strip_prefix('-') {
        (-1, v)
    } else {
        (1, offset.strip_prefix('+').unwrap_or(offset))
    };
    let parts = offset.split(':').collect::<Vec<_>>();
    if parts.is_empty()
        || parts.len() > 3
        || parts
            .iter()
            .any(|p| p.is_empty() || !p.bytes().all(|b| b.is_ascii_digit()))
    {
        return None;
    }
    let h = parts[0].parse::<i32>().ok()?;
    let m = parts.get(1).map_or(Some(0), |v| v.parse::<i32>().ok())?;
    let s = parts.get(2).map_or(Some(0), |v| v.parse::<i32>().ok())?;
    if h > 167 || m > 59 || s > 59 {
        return None;
    }
    Some(-sign * (h * 3600 + m * 60 + s))
}

#[cfg(test)]
mod setting_tests {
    use super::*;
    #[test]
    fn posix_numeric_and_named_zones_are_distinct() {
        for (input, label, seconds) in [
            ("+05:30", "+05:30", -19800),
            ("05:30", "05:30", -19800),
            ("UTC+99", "UTC+99", -356400),
            ("UTC-05:30", "UTC-05:30", 19800),
            ("XYZ-3:30", "XYZ-3:30", 12600),
            ("-03", "<-03>+03", -10800),
            ("+5", "<+05>-05", 18000),
            ("-2.5", "<-02:30>+02:30", -9000),
            ("16", "<+16>-16", 57600),
        ] {
            let (zone, shown) = SessionZone::setting(input).unwrap();
            assert_eq!(shown, label);
            let epoch = TimestampTz::from_microseconds(0).unwrap();
            assert_eq!(
                zone.to_local(epoch).unwrap().microseconds(),
                i64::from(seconds) * 1_000_000
            );
            assert_eq!(
                zone.to_instant(zone.to_local(epoch).unwrap()).unwrap(),
                epoch
            );
            assert_eq!(
                SessionZone::setting(&shown)
                    .unwrap()
                    .0
                    .to_local(epoch)
                    .unwrap(),
                zone.to_local(epoch).unwrap()
            );
            assert!(epoch.format_in_zone(zone).is_ok());
        }
        for input in [
            "Not/AZone",
            "Z",
            "zulu",
            "PST",
            "abc",
            "",
            "UTC+168",
            "UTC+03:60",
        ] {
            assert!(SessionZone::setting(input).is_err(), "{input}");
        }
        assert_eq!(
            SessionZone::setting("europe/paris").unwrap().1,
            "Europe/Paris"
        );
        let instant =
            TimestampTz::parse("2024-07-02 03:04:05+00", &ParserSettings::default()).unwrap();
        assert_eq!(
            instant
                .format_in_zone(SessionZone::setting("EST5EDT").unwrap().0)
                .unwrap(),
            "2024-07-01 23:04:05-04"
        );
        assert_eq!(
            instant
                .format_in_zone(SessionZone::setting("PST8PDT").unwrap().0)
                .unwrap(),
            "2024-07-01 20:04:05-07"
        );
    }
}

#[cfg(test)]
mod parity_catalog_tests {
    use super::*;
    #[test]
    fn session_zone_historical_abbreviations() {
        let rows = SessionZone::named("America/New_York")
            .unwrap()
            .abbreviations()
            .unwrap();
        for (name, offset, dst) in [
            ("LMT", -17762, false),
            ("EWT", -14400, true),
            ("EPT", -14400, true),
        ] {
            assert!(rows.contains(&(name.into(), offset, dst)));
        }
        assert!(SessionZone::Fixed(3600).abbreviations().unwrap().is_empty());
        let utc = SessionZone::named("UTC").unwrap().abbreviations().unwrap();
        assert_eq!(utc, vec![("UTC".into(), 0, false)]);
    }
    #[test]
    fn pinned_names_and_current_rows() {
        assert_eq!(TIMEZONE_NAMES.lines().count(), 487);
        for row in include_str!("timezone_rows_20261116.txt").lines() {
            let cols: Vec<_> = row.split('|').collect();
            let (abbrev, seconds, dst) = timezone_info(cols[0], 1794844800).unwrap();
            let abs = seconds.unsigned_abs();
            let shown = format!(
                "{}{:02}:{:02}:{:02}",
                if seconds < 0 { "-" } else { "" },
                abs / 3600,
                abs / 60 % 60,
                abs % 60
            );
            assert_eq!(
                (abbrev.as_str(), shown.as_str(), dst),
                (cols[1], cols[2], cols[3] == "t"),
                "{}",
                cols[0]
            );
            assert_eq!(
                SessionZone::setting(&cols[0].to_ascii_lowercase())
                    .unwrap()
                    .1,
                cols[0]
            );
        }
        for name in ["Zulu", "US/Eastern"] {
            assert!(SessionZone::setting(name).is_err());
        }
    }
}
