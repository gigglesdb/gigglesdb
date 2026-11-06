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
        .find(|(n, _)| *n == tz.name())
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
pub const TZDATA_VERSION: &str = chrono_tz::IANA_TZDB_VERSION;
/// A session zone never consults process settings or the clock.
#[derive(Clone, Copy, Debug)]
pub enum SessionZone {
    Fixed(i32),
    Named(Tz),
}
impl SessionZone {
    pub fn fixed(seconds_east: i32) -> Result<Self, Error> {
        Ok(Self::Fixed(offset_valid(seconds_east)?))
    }
    pub fn named(name: &str) -> Result<Self, Error> {
        let name = bounded(name)?;
        let zone = chrono_tz::TZ_VARIANTS
            .iter()
            .find(|zone| zone.name().eq_ignore_ascii_case(name))
            .copied()
            .ok_or(Error::Syntax)?;
        Ok(Self::Named(zone))
    }
    fn local_offset(self, local: Timestamp) -> Result<i32, Error> {
        let tz = match self {
            Self::Fixed(v) => return offset_valid(v),
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
            Self::Fixed(v) => offset_valid(v)?,
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
        let suffix = TimeTz::new(Time(0), off)?.to_string();
        Ok(format!("{text}{}{bc}", &suffix[8..]))
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
