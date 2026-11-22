//! Exact PostgreSQL 18 temporal values. See TEMPORAL.md for the bounded text grammar.
use crate::Error;
use std::{cmp::Ordering, fmt};
pub const USECS_PER_DAY: i64 = 86_400_000_000;
pub const MIN_TIMESTAMP: i64 = -211_813_488_000_000_000;
pub const END_TIMESTAMP: i64 = 9_223_371_331_200_000_000;
pub const MAX_TEMPORAL_TEXT: usize = 256;
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DateOrder {
    #[default]
    Ymd,
    Mdy,
    Dmy,
}
/// Controls the leading-sign convention for the supported interval grammar.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum IntervalStyle {
    #[default]
    Postgres,
    SqlStandard,
}
/// Explicit environment: ISO output, numeric input order, and seconds east of UTC.
#[derive(Clone, Copy, Debug, Default)]
pub struct ParserSettings {
    pub date_order: DateOrder,
    pub offset_east: i32,
}
fn bounded(s: &str) -> Result<&str, Error> {
    if s.len() > MAX_TEMPORAL_TEXT {
        return Err(Error::Limit);
    }
    if !s.is_ascii() || s.as_bytes().contains(&0) {
        return Err(Error::Syntax);
    }
    Ok(s.trim())
}
fn number(s: &str) -> Result<i64, Error> {
    s.parse().map_err(|_| Error::Syntax)
}
fn offset_valid(v: i32) -> Result<i32, Error> {
    if v.abs_diff(0) >= 57600 {
        Err(Error::Range)
    } else {
        Ok(v)
    }
}
// Proleptic Gregorian civil conversion, astronomical year numbering internally.
fn civil_days(y: i64, m: i64, d: i64) -> i64 {
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Date(i32);
impl Date {
    pub const NEG_INFINITY: Self = Self(i32::MIN);
    pub const INFINITY: Self = Self(i32::MAX);
    pub fn from_days(v: i32) -> Result<Self, Error> {
        if v == i32::MIN || v == i32::MAX || (-2451545..2145031949).contains(&v) {
            Ok(Self(v))
        } else {
            Err(Error::Range)
        }
    }
    pub fn days(self) -> i32 {
        self.0
    }
    pub fn from_ymd(year: i32, month: u32, day: u32) -> Result<Self, Error> {
        if year == 0
            || !(-4714..=5874897).contains(&year)
            || !(1..=12).contains(&month)
            || !(1..=31).contains(&day)
        {
            return Err(Error::Range);
        }
        let y = i64::from(if year < 0 { year + 1 } else { year });
        let v = civil_days(y, month.into(), day.into());
        if civil(v) != (y, month.into(), day.into()) {
            return Err(Error::Range);
        }
        Self::from_days(i32::try_from(v).map_err(|_| Error::Range)?)
    }
    pub fn parse(s: &str, settings: &ParserSettings) -> Result<Self, Error> {
        input::date(s, settings.date_order)
    }
    pub fn to_binary(self) -> [u8; 4] {
        self.0.to_be_bytes()
    }
    pub fn from_binary(b: &[u8]) -> Result<Self, Error> {
        Self::from_days(i32::from_be_bytes(b.try_into().map_err(|_| Error::Binary)?))
    }
}
impl fmt::Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::INFINITY => f.write_str("infinity"),
            Self::NEG_INFINITY => f.write_str("-infinity"),
            _ => {
                let (y, m, d) = civil(self.0.into());
                write!(
                    f,
                    "{:04}-{m:02}-{d:02}{}",
                    if y <= 0 { 1 - y } else { y },
                    if y <= 0 { " BC" } else { "" }
                )
            }
        }
    }
}
/// Time of day includes the distinct endpoint 24:00:00.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Time(i64);
impl Time {
    pub fn from_microseconds(v: i64) -> Result<Self, Error> {
        if (0..=USECS_PER_DAY).contains(&v) {
            Ok(Self(v))
        } else {
            Err(Error::Range)
        }
    }
    pub fn microseconds(self) -> i64 {
        self.0
    }
    pub fn parse(s: &str) -> Result<Self, Error> {
        input::time(s, &ParserSettings::default(), false).map(|(time, _)| time)
    }
    pub fn to_binary(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
    pub fn from_binary(b: &[u8]) -> Result<Self, Error> {
        Self::from_microseconds(i64::from_be_bytes(b.try_into().map_err(|_| Error::Binary)?))
    }
}
fn clock(s: &str, wide: bool) -> Result<i64, Error> {
    let (s, sign) = if let Some(s) = s.strip_prefix('-') {
        (s, -1i128)
    } else {
        (s.strip_prefix('+').unwrap_or(s), 1)
    };
    let mut p = s.split(':');
    let h = number(p.next().ok_or(Error::Syntax)?)?;
    let m = number(p.next().ok_or(Error::Syntax)?)?;
    let sec = p.next().unwrap_or("0");
    if p.next().is_some() || h < 0 || !(0..60).contains(&m) {
        return Err(Error::Range);
    }
    let (sec, frac) = sec.split_once('.').unwrap_or((sec, ""));
    let sec = number(sec)?;
    if !(0..=if wide { 59 } else { 60 }).contains(&sec) || !frac.bytes().all(|c| c.is_ascii_digit())
    {
        return Err(Error::Range);
    }
    let mut us = 0i128;
    for i in 0..6 {
        us = us * 10 + i128::from(frac.as_bytes().get(i).map_or(0, |v| v - b'0'));
    }
    if let Some(&r) = frac.as_bytes().get(6)
        && (r > b'5'
            || (r == b'5' && (frac.as_bytes()[7..].iter().any(|&v| v != b'0') || us % 2 == 1)))
    {
        us += 1;
    }
    if !wide && (sign < 0 || h > 24 || (h == 24 && (m != 0 || sec != 0 || us != 0))) {
        return Err(Error::Range);
    }
    i64::try_from(
        sign * ((i128::from(h) * 3600 + i128::from(m) * 60 + i128::from(sec)) * 1_000_000 + us),
    )
    .map_err(|_| Error::Range)
}
fn clock_text(v: i64) -> String {
    let v = i128::from(v);
    let a = v.abs();
    let us = a % 1_000_000;
    let mut s = format!(
        "{}{:02}:{:02}:{:02}",
        if v < 0 { "-" } else { "" },
        a / 3_600_000_000,
        (a / 60_000_000) % 60,
        (a / 1_000_000) % 60
    );
    if us != 0 {
        s.push_str(format!(".{us:06}").trim_end_matches('0'));
    }
    s
}
impl fmt::Display for Time {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&clock_text(self.0))
    }
}
macro_rules! timestamp_type {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(i64);
        impl $name {
            pub const NEG_INFINITY: Self = Self(i64::MIN);
            pub const INFINITY: Self = Self(i64::MAX);
            pub fn from_microseconds(v: i64) -> Result<Self, Error> {
                if v == i64::MIN || v == i64::MAX || (MIN_TIMESTAMP..END_TIMESTAMP).contains(&v) {
                    Ok(Self(v))
                } else {
                    Err(Error::Range)
                }
            }
            pub fn microseconds(self) -> i64 {
                self.0
            }
            pub fn to_binary(self) -> [u8; 8] {
                self.0.to_be_bytes()
            }
            pub fn from_binary(b: &[u8]) -> Result<Self, Error> {
                Self::from_microseconds(i64::from_be_bytes(
                    b.try_into().map_err(|_| Error::Binary)?,
                ))
            }
        }
    };
}
timestamp_type!(Timestamp);
timestamp_type!(TimestampTz);
impl Timestamp {
    pub fn parse(s: &str, p: &ParserSettings) -> Result<Self, Error> {
        Self::from_microseconds(input::stamp(s, p.date_order, SessionZone::Fixed(0), false)?)
    }
}
impl TimestampTz {
    pub fn parse(s: &str, p: &ParserSettings) -> Result<Self, Error> {
        input::zoned_stamp(s, p.date_order, p.offset_east)
    }
}
fn stamp_text(v: i64, tz: bool, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    if v == i64::MIN {
        return f.write_str("-infinity");
    }
    if v == i64::MAX {
        return f.write_str("infinity");
    }
    let date = Date(v.div_euclid(USECS_PER_DAY) as i32).to_string();
    let (date, bc) = date
        .strip_suffix(" BC")
        .map_or((date.as_str(), ""), |d| (d, " BC"));
    write!(
        f,
        "{date} {}{}{bc}",
        Time(v.rem_euclid(USECS_PER_DAY)),
        if tz { "+00" } else { "" }
    )
}
impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        stamp_text(self.0, false, f)
    }
}
impl fmt::Display for TimestampTz {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        stamp_text(self.0, true, f)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TimeTz {
    time: Time,
    offset_east: i32,
}
impl TimeTz {
    pub fn new(time: Time, offset_east: i32) -> Result<Self, Error> {
        Ok(Self {
            time,
            offset_east: offset_valid(offset_east)?,
        })
    }
    pub fn time(self) -> Time {
        self.time
    }
    pub fn offset_east(self) -> i32 {
        self.offset_east
    }
    pub fn parse(s: &str, p: &ParserSettings) -> Result<Self, Error> {
        let (time, offset) = input::time(s, p, true)?;
        Self::new(time, offset)
    }
    pub fn to_binary(self) -> [u8; 12] {
        let mut b = [0; 12];
        b[..8].copy_from_slice(&self.time.to_binary());
        b[8..].copy_from_slice(&(-self.offset_east).to_be_bytes());
        b
    }
    pub fn from_binary(b: &[u8]) -> Result<Self, Error> {
        if b.len() != 12 {
            return Err(Error::Binary);
        }
        let west = i32::from_be_bytes(b[8..].try_into().unwrap());
        Self::new(
            Time::from_binary(&b[..8])?,
            west.checked_neg().ok_or(Error::Range)?,
        )
    }
}
impl Ord for TimeTz {
    fn cmp(&self, o: &Self) -> Ordering {
        (self.time.0 - i64::from(self.offset_east) * 1_000_000)
            .cmp(&(o.time.0 - i64::from(o.offset_east) * 1_000_000))
            .then_with(|| o.offset_east.cmp(&self.offset_east))
    }
}
impl PartialOrd for TimeTz {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl fmt::Display for TimeTz {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let a = self.offset_east.abs();
        write!(
            f,
            "{}{}{:02}",
            self.time,
            if self.offset_east < 0 { "-" } else { "+" },
            a / 3600
        )?;
        if a % 3600 != 0 {
            write!(f, ":{:02}", a / 60 % 60)?;
        }
        if a % 60 != 0 {
            write!(f, ":{:02}", a % 60)?;
        }
        Ok(())
    }
}
/// Components remain distinct even when SQL comparison considers two values equal.
#[derive(Clone, Copy, Debug)]
pub struct Interval {
    months: i32,
    days: i32,
    microseconds: i64,
}
impl Interval {
    pub const NEG_INFINITY: Self = Self {
        months: i32::MIN,
        days: i32::MIN,
        microseconds: i64::MIN,
    };
    pub const INFINITY: Self = Self {
        months: i32::MAX,
        days: i32::MAX,
        microseconds: i64::MAX,
    };
    pub fn new(months: i32, days: i32, microseconds: i64) -> Self {
        Self {
            months,
            days,
            microseconds,
        }
    }
    pub fn months(self) -> i32 {
        self.months
    }
    pub fn days(self) -> i32 {
        self.days
    }
    pub fn microseconds(self) -> i64 {
        self.microseconds
    }
    fn key(self) -> i128 {
        if self.same_components(Self::INFINITY) {
            i128::MAX
        } else if self.same_components(Self::NEG_INFINITY) {
            i128::MIN
        } else {
            (i128::from(self.months) * 30 + i128::from(self.days)) * i128::from(USECS_PER_DAY)
                + i128::from(self.microseconds)
        }
    }
    pub fn same_components(self, o: Self) -> bool {
        self.months == o.months && self.days == o.days && self.microseconds == o.microseconds
    }
    pub fn to_binary(self) -> [u8; 16] {
        let mut b = [0; 16];
        b[..8].copy_from_slice(&self.microseconds.to_be_bytes());
        b[8..12].copy_from_slice(&self.days.to_be_bytes());
        b[12..].copy_from_slice(&self.months.to_be_bytes());
        b
    }
    pub fn from_binary(b: &[u8]) -> Result<Self, Error> {
        if b.len() != 16 {
            return Err(Error::Binary);
        }
        Ok(Self::new(
            i32::from_be_bytes(b[12..].try_into().unwrap()),
            i32::from_be_bytes(b[8..12].try_into().unwrap()),
            i64::from_be_bytes(b[..8].try_into().unwrap()),
        ))
    }
    pub fn parse_with_style(s: &str, style: IntervalStyle) -> Result<Self, Error> {
        let s = bounded(s)?;
        // '-.5' is a bad field in ParseDateTime before any style applies, so keep it 22007.
        if style == IntervalStyle::SqlStandard
            && s.starts_with('-')
            && !s[1..].starts_with('.')
            && !s
                .split_whitespace()
                .skip(1)
                .any(|v| v.starts_with(['+', '-']))
        {
            let v = Self::parse(&s[1..])?;
            return Ok(Self::new(
                v.months.checked_neg().ok_or(Error::Range)?,
                v.days.checked_neg().ok_or(Error::Range)?,
                v.microseconds.checked_neg().ok_or(Error::Range)?,
            ));
        }
        Self::parse(s)
    }
    pub fn parse(s: &str) -> Result<Self, Error> {
        Self::parse_fields(s, false)
    }
    /// `ago` is true while parsing the fields before a trailing `ago`, which counts as a
    /// following field: PostgreSQL then rejects a unitless number ('5 ago' is 22007).
    fn parse_fields(s: &str, ago: bool) -> Result<Self, Error> {
        let s = bounded(s)?;
        if s.eq_ignore_ascii_case("infinity") {
            return Ok(Self::INFINITY);
        }
        if s.eq_ignore_ascii_case("-infinity") {
            return Ok(Self::NEG_INFINITY);
        }
        if let Some(s) = s.strip_suffix(" ago") {
            let v = Self::parse_fields(s.strip_prefix("@ ").unwrap_or(s), true)?;
            return Ok(Self::new(
                v.months.checked_neg().ok_or(Error::Range)?,
                v.days.checked_neg().ok_or(Error::Range)?,
                v.microseconds.checked_neg().ok_or(Error::Range)?,
            ));
        }
        let s = s.strip_prefix("@ ").unwrap_or(s);
        if s.starts_with('P') {
            return Self::parse_iso(s);
        }
        let mut p = interval_tokens(s).into_iter().peekable();
        let mut months = 0i64;
        let mut days = 0i64;
        let mut us = 0i64;
        let mut seen = false;
        let mut fields = 0u16;
        while let Some(v) = p.next() {
            seen = true;
            if v.contains(':') {
                if fields & 0b111_0000 != 0 {
                    return Err(Error::Syntax);
                }
                fields |= 0b111_0000;
                us = us.checked_add(clock(v, true)?).ok_or(Error::Range)?;
                continue;
            }
            if let Some((y, m)) = v.strip_prefix(['+', '-']).unwrap_or(v).split_once('-') {
                if fields & 3 != 0 {
                    return Err(Error::Syntax);
                }
                fields |= 3;
                let y = number(y)?;
                let m = number(m)?;
                if !(0..12).contains(&m) {
                    return Err(Error::Range);
                }
                let sign = if v.starts_with('-') { -1 } else { 1 };
                months = months
                    .checked_add(
                        y.checked_mul(12)
                            .and_then(|v| v.checked_add(m))
                            .and_then(|v| v.checked_mul(sign))
                            .ok_or(Error::Range)?,
                    )
                    .ok_or(Error::Range)?;
                continue;
            }
            // A number immediately before a clock is the SQL day field.
            if p.peek().is_some_and(|next| next.contains(':')) {
                if fields & 4 != 0 {
                    return Err(Error::Syntax);
                }
                fields |= 4;
                days = days.checked_add(number(v)?).ok_or(Error::Range)?;
                continue;
            }
            // PostgreSQL DecodeInterval: an alphabetic next token is the unit (unknown units
            // stay Unimplemented). A unitless number is seconds only as the final field
            // ('0', '1 day 2'); elsewhere ('5 3 minutes', '1 2', '5 ago') it is 22007.
            // PostgreSQL ParseDateTime reads a field that starts with '.' as a number with a
            // zero whole part ('.5', '.' alone is 0); a sign before the '.' ('-.5') stays
            // 22007 because decimal() then sees an empty whole part.
            let padded;
            let v = if v.starts_with('.') {
                padded = format!("0{v}");
                padded.as_str()
            } else {
                v
            };
            let unit = match p.peek().copied() {
                Some(unit) if !unit.is_empty() && unit.chars().all(|c| c.is_ascii_alphabetic()) => {
                    p.next();
                    unit
                }
                None if !ago => "seconds",
                _ => return Err(Error::Syntax),
            };
            let unit = interval_unit(unit).ok_or(Error::Unimplemented)?;
            let mask = match unit {
                "years" => 1,
                "months" => 2,
                "days" => 4,
                "weeks" => 8,
                "hours" => 16,
                "minutes" => 32,
                "seconds" => {
                    // DecodeInterval widens the mask to milli/microseconds only for a
                    // nonzero fraction: '1. seconds 5 milliseconds' is accepted.
                    if v.split_once('.')
                        .is_some_and(|(_, f)| f.bytes().any(|b| b != b'0'))
                    {
                        448
                    } else {
                        64
                    }
                }
                "milliseconds" => 128,
                "microseconds" => 256,
                "decades" => 512,
                "centuries" => 1024,
                _ => 2048,
            };
            if fields & mask != 0 {
                return Err(Error::Syntax);
            }
            fields |= mask;
            let part = Self::unit(v, unit)?;
            months = months
                .checked_add(i64::from(part.months))
                .ok_or(Error::Range)?;
            days = days.checked_add(i64::from(part.days)).ok_or(Error::Range)?;
            us = us.checked_add(part.microseconds).ok_or(Error::Range)?;
        }
        if !seen {
            return Err(Error::Syntax);
        }
        Ok(Self::new(
            i32::try_from(months).map_err(|_| Error::Range)?,
            i32::try_from(days).map_err(|_| Error::Range)?,
            us,
        ))
    }
}
/// PG18.6 interval unit spellings (datetime.c deltatktbl), mapped to the names
/// `Interval::unit` takes. Decades, centuries and millennia keep their own field bits.
fn interval_unit(unit: &str) -> Option<&'static str> {
    Some(match unit {
        "y" | "yr" | "yrs" | "year" | "years" => "years",
        "mon" | "mons" | "month" | "months" => "months",
        "d" | "day" | "days" => "days",
        "w" | "week" | "weeks" => "weeks",
        "h" | "hr" | "hrs" | "hour" | "hours" => "hours",
        "m" | "min" | "mins" | "minute" | "minutes" => "minutes",
        "s" | "sec" | "secs" | "second" | "seconds" => "seconds",
        "ms" | "msec" | "msecs" | "msecond" | "millisecond" | "milliseconds" => "milliseconds",
        "us" | "usec" | "usecs" | "usecond" | "microsecond" | "microseconds" => "microseconds",
        "dec" | "decade" | "decades" => "decades",
        "c" | "cent" | "century" | "centuries" => "centuries",
        "mil" | "mils" | "millennium" | "millennia" => "millennia",
        _ => return None,
    })
}

/// Splits whitespace fields the way PG18.6 ParseDateTime does for a number glued to a
/// unit: '1day' is '1' 'day', '1h30m' is '1' 'h' '30' 'm', '1h+30m' is '1' 'h' '+30' 'm'.
/// A field is split only when every letter run is a known unit; anything else (a clock,
/// a year-month field, '1h-30m', '.5e1', '1dago') stays whole and is rejected as before.
fn interval_tokens(s: &str) -> Vec<&str> {
    let mut out = Vec::new();
    for t in s.split_whitespace() {
        let b = t.as_bytes();
        let body = t.strip_prefix(['+', '-']).unwrap_or(t);
        let simple = !body.contains(['-', '/', ':'])
            && b.iter()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'+' | b'-'))
            && !b
                .windows(2)
                .any(|w| w[0].is_ascii_alphabetic() && w[1] == b'.');
        let mut runs = Vec::new();
        let mut start = 0;
        for i in 1..b.len() {
            let sign = matches!(b[i - 1], b'+' | b'-');
            if b[i] == b'+'
                || (!sign && b[i].is_ascii_alphabetic() != b[i - 1].is_ascii_alphabetic())
            {
                runs.push(&t[start..i]);
                start = i;
            }
        }
        runs.push(&t[start..]);
        if simple
            && runs.len() > 1
            && runs
                .iter()
                .all(|r| !r.as_bytes()[0].is_ascii_alphabetic() || interval_unit(r).is_some())
        {
            out.extend(runs);
        } else {
            out.push(t);
        }
    }
    out
}

impl PartialEq for Interval {
    fn eq(&self, o: &Self) -> bool {
        self.key() == o.key()
    }
}
impl Eq for Interval {}
impl Ord for Interval {
    fn cmp(&self, o: &Self) -> Ordering {
        self.key().cmp(&o.key())
    }
}
impl PartialOrd for Interval {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl fmt::Display for Interval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.same_components(Self::INFINITY) {
            return f.write_str("infinity");
        }
        if self.same_components(Self::NEG_INFINITY) {
            return f.write_str("-infinity");
        }
        let years = self.months / 12;
        let mons = self.months % 12;
        let mut parts = Vec::with_capacity(4);
        let mut negative = false;
        for (n, unit) in [(years, "year"), (mons, "mon"), (self.days, "day")] {
            if n != 0 {
                parts.push(format!(
                    "{}{n} {unit}{}",
                    if negative && n > 0 { "+" } else { "" },
                    if n == 1 { "" } else { "s" }
                ));
                negative = n < 0;
            }
        }
        if self.microseconds != 0 || parts.is_empty() {
            parts.push(format!(
                "{}{}",
                if negative && self.microseconds > 0 {
                    "+"
                } else {
                    ""
                },
                clock_text(self.microseconds)
            ));
        }
        f.write_str(&parts.join(" "))
    }
}

mod input;
mod zones;
pub use zones::*;
mod precision;

mod compatibility;
pub use compatibility::{DateStyle, DateStyleSettings};
pub use compatibility::IntervalField;
pub use compatibility::IntervalOutputStyle;

mod functions;
pub use functions::*;
