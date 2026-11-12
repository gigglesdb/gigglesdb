use super::{MappingError, invalid};
use serde::{Deserialize, Serialize};

pub(crate) const DAY: i64 = 86_400_000_000;
const MIN_TIMESTAMP: i64 = -211_813_488_000_000_000;
const END_TIMESTAMP: i64 = 9_223_371_331_200_000_000;

/// Finite days since 2000-01-01, or explicit PG infinity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Date {
    Finite(i32),
    NegativeInfinity,
    PositiveInfinity,
}
/// Finite microseconds since 2000-01-01 (UTC for timestamptz).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Timestamp {
    Finite(i64),
    NegativeInfinity,
    PositiveInfinity,
}

impl Date {
    pub(crate) fn validate(self) -> Result<(), MappingError> {
        if let Self::Finite(v) = self
            && !(-2_451_545..2_145_031_949).contains(&v)
        {
            return Err(invalid("date range"));
        }
        Ok(())
    }
}
impl Timestamp {
    pub(crate) fn validate(self) -> Result<(), MappingError> {
        if let Self::Finite(v) = self
            && !(MIN_TIMESTAMP..END_TIMESTAMP).contains(&v)
        {
            return Err(invalid("timestamp range"));
        }
        Ok(())
    }
}

fn days(y: i64, m: i64, d: i64) -> i64 {
    let y = y - i64::from(m <= 2);
    let era = y.div_euclid(400);
    let yo = y - era * 400;
    let mp = m + if m > 2 { -3 } else { 9 };
    era * 146097 + yo * 365 + yo / 4 - yo / 100 + (153 * mp + 2) / 5 + d - 1 - 730425
}
fn civil(day: i64) -> (i64, i64, i64) {
    let z = day + 730425;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yo = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yo + era * 400;
    let doy = doe - (365 * yo + yo / 4 - yo / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    (y + i64::from(m <= 2), m, d)
}
fn num(s: &str) -> Result<i64, MappingError> {
    s.parse().map_err(|_| invalid("temporal integer"))
}

pub(crate) fn parse_date(s: &str) -> Result<Date, MappingError> {
    match s {
        "infinity" => return Ok(Date::PositiveInfinity),
        "-infinity" => return Ok(Date::NegativeInfinity),
        _ => {}
    }
    let (s, bc) = s.strip_suffix(" BC").map_or((s, false), |s| (s, true));
    let p: Vec<_> = s.split('-').collect();
    if p.len() != 3 {
        return Err(invalid("ISO date required"));
    }
    let y = num(p[0])?;
    let m = num(p[1])?;
    let d = num(p[2])?;
    if !(1..=5_874_898).contains(&y) || !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return Err(invalid("date fields"));
    }
    let y = if bc { 1 - y } else { y };
    let day = days(y, m, d);
    if civil(day) != (y, m, d) {
        return Err(invalid("calendar date"));
    }
    let v = Date::Finite(i32::try_from(day).map_err(|_| invalid("date range"))?);
    v.validate()?;
    Ok(v)
}
pub(crate) fn format_date(d: Date) -> String {
    match d {
        Date::PositiveInfinity => "infinity".into(),
        Date::NegativeInfinity => "-infinity".into(),
        Date::Finite(day) => {
            let (y, m, d) = civil(i64::from(day));
            if y <= 0 {
                format!("{:04}-{m:02}-{d:02} BC", 1 - y)
            } else {
                format!("{y:04}-{m:02}-{d:02}")
            }
        }
    }
}

pub(crate) fn parse_clock(s: &str, time_of_day: bool) -> Result<i64, MappingError> {
    let (negative, body) = s
        .strip_prefix('-')
        .map_or((false, s.strip_prefix('+').unwrap_or(s)), |s| (true, s));
    let p: Vec<_> = body.split(':').collect();
    if p.len() != 3 {
        return Err(invalid("HH:MM:SS required"));
    }
    let h = num(p[0])?;
    let m = num(p[1])?;
    let mut sec = p[2].split('.');
    let seconds = num(sec.next().unwrap_or_default())?;
    let frac = sec.next().unwrap_or_default();
    if sec.next().is_some()
        || frac.len() > 6
        || !frac.bytes().all(|b| b.is_ascii_digit())
        || h < 0
        || !(0..60).contains(&m)
        || !(0..60).contains(&seconds)
    {
        return Err(invalid("clock fields"));
    }
    let micros = if frac.is_empty() {
        0
    } else {
        num(frac)? * 10_i64.pow(6 - frac.len() as u32)
    };
    let v = i128::from(h) * 3_600_000_000
        + i128::from(m) * 60_000_000
        + i128::from(seconds) * 1_000_000
        + i128::from(micros);
    let v = if negative { -v } else { v };
    let v = i64::try_from(v).map_err(|_| invalid("clock overflow"))?;
    if time_of_day && (negative || !(0..=DAY).contains(&v)) {
        return Err(invalid("time range"));
    }
    Ok(v)
}
pub(crate) fn format_clock(v: i64) -> String {
    let a = v.unsigned_abs();
    format!(
        "{}{:02}:{:02}:{:02}.{:06}",
        if v < 0 { "-" } else { "" },
        a / 3_600_000_000,
        a / 60_000_000 % 60,
        a / 1_000_000 % 60,
        a % 1_000_000
    )
}
pub(crate) fn split_zone(s: &str) -> Result<(&str, i32), MappingError> {
    if let Some(s) = s.strip_suffix('Z') {
        return Ok((s, 0));
    }
    let pos = s
        .char_indices()
        .find(|(i, c)| *i > 0 && (*c == '+' || *c == '-'))
        .map(|(i, _)| i)
        .ok_or_else(|| invalid("numeric timezone offset required"))?;
    let zone = &s[pos + 1..];
    let p: Vec<_> = zone.split(':').collect();
    if p.is_empty() || p.len() > 3 {
        return Err(invalid("timezone"));
    }
    let h = num(p[0])?;
    let m = if p.len() > 1 { num(p[1])? } else { 0 };
    let sec = if p.len() > 2 { num(p[2])? } else { 0 };
    if !(0..16).contains(&h) || !(0..60).contains(&m) || !(0..60).contains(&sec) {
        return Err(invalid("timezone range"));
    }
    let offset = (h * 3600 + m * 60 + sec) as i32 * if s.as_bytes()[pos] == b'-' { -1 } else { 1 };
    Ok((&s[..pos], offset))
}
pub(crate) fn format_zone(east: i32) -> String {
    let a = east.unsigned_abs();
    format!(
        "{}{:02}:{:02}:{:02}",
        if east < 0 { '-' } else { '+' },
        a / 3600,
        a / 60 % 60,
        a % 60
    )
}

pub(crate) fn parse_timestamp(s: &str, zoned: bool) -> Result<Timestamp, MappingError> {
    match s {
        "infinity" => return Ok(Timestamp::PositiveInfinity),
        "-infinity" => return Ok(Timestamp::NegativeInfinity),
        _ => {}
    }
    let (s, bc) = s.strip_suffix(" BC").map_or((s, false), |s| (s, true));
    let (date, clock) = s
        .split_once([' ', 'T'])
        .ok_or_else(|| invalid("ISO timestamp required"))?;
    let Date::Finite(day) = parse_date(&format!("{date}{}", if bc { " BC" } else { "" }))? else {
        return Err(invalid("timestamp date"));
    };
    let (clock, offset) = if zoned {
        split_zone(clock)?
    } else {
        (clock, 0)
    };
    let micros = i128::from(day) * i128::from(DAY) + i128::from(parse_clock(clock, true)?)
        - i128::from(offset) * 1_000_000;
    let v = Timestamp::Finite(i64::try_from(micros).map_err(|_| invalid("timestamp overflow"))?);
    v.validate()?;
    Ok(v)
}
pub(crate) fn format_timestamp(t: Timestamp, zoned: bool) -> String {
    match t {
        Timestamp::PositiveInfinity => "infinity".into(),
        Timestamp::NegativeInfinity => "-infinity".into(),
        Timestamp::Finite(t) => {
            let date = format_date(Date::Finite(t.div_euclid(DAY) as i32));
            let (date, bc) = date
                .strip_suffix(" BC")
                .map_or((date.as_str(), ""), |d| (d, " BC"));
            format!(
                "{date} {}{}{bc}",
                format_clock(t.rem_euclid(DAY)),
                if zoned { "+00:00" } else { "" }
            )
        }
    }
}

pub(crate) fn parse_interval(s: &str) -> Result<(i32, i32, i64), MappingError> {
    // IntervalStyle=postgres: independently signed year/month/day and clock.
    let fields: Vec<_> = s.split_whitespace().collect();
    let mut i = 0;
    let (mut months, mut days, mut micros) = (0_i64, 0_i64, 0_i64);
    let (mut seen_year, mut seen_month, mut seen_day, mut seen_clock) =
        (false, false, false, false);
    while i < fields.len() {
        if fields[i].contains(':') {
            if seen_clock {
                return Err(invalid("duplicate interval clock"));
            }
            micros = parse_clock(fields[i], false)?;
            seen_clock = true;
            i += 1;
        } else {
            let n = num(fields[i])?;
            let unit = fields.get(i + 1).ok_or_else(|| invalid("interval unit"))?;
            match *unit {
                "year" | "years" if !seen_year => {
                    months = months
                        .checked_add(
                            n.checked_mul(12)
                                .ok_or_else(|| invalid("interval overflow"))?,
                        )
                        .ok_or_else(|| invalid("interval overflow"))?;
                    seen_year = true;
                }
                "mon" | "mons" if !seen_month => {
                    months = months
                        .checked_add(n)
                        .ok_or_else(|| invalid("interval overflow"))?;
                    seen_month = true;
                }
                "day" | "days" if !seen_day => {
                    days = n;
                    seen_day = true;
                }
                _ => return Err(invalid("postgres interval style required")),
            }
            i += 2;
        }
    }
    if fields.is_empty() {
        return Err(invalid("empty interval"));
    }
    Ok((
        i32::try_from(months).map_err(|_| invalid("interval months"))?,
        i32::try_from(days).map_err(|_| invalid("interval days"))?,
        micros,
    ))
}
