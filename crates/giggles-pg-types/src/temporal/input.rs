//! PostgreSQL 18.6 date/time field scanning and decoding.
//! Adapted from datetime.c (PostgreSQL license in timezone_abbreviations.txt).

use super::zones::{InputZone, input_abbreviation};
use super::*;

const JULIAN_EPOCH: i32 = 2_451_545;
const USECS_PER_HOUR: i64 = 3_600_000_000;
const MAX_FIELDS: usize = 25;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Number,
    Date,
    Clock,
    Word,
    Offset,
    Special,
}

#[derive(Clone, Copy)]
struct Field<'a> {
    kind: Kind,
    text: &'a str,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Word {
    Month(i32),
    Weekday,
    Era(bool),
    Meridian(bool),
    Julian,
    IsoTime,
    Ignore,
    Dst,
    Reserved,
    Unknown,
}

// The relevant entries of PostgreSQL's datetktbl, also used to decide
// whether a word followed by a digit starts a timezone or ends a field.
fn keyword(text: &str) -> Word {
    match text {
        "jan" | "january" => Word::Month(1),
        "feb" | "february" => Word::Month(2),
        "mar" | "march" => Word::Month(3),
        "apr" | "april" => Word::Month(4),
        "may" => Word::Month(5),
        "jun" | "june" => Word::Month(6),
        "jul" | "july" => Word::Month(7),
        "aug" | "august" => Word::Month(8),
        "sep" | "sept" | "september" => Word::Month(9),
        "oct" | "october" => Word::Month(10),
        "nov" | "november" => Word::Month(11),
        "dec" | "december" => Word::Month(12),
        "sun" | "sunday" | "mon" | "monday" | "tue" | "tues" | "tuesday" | "wed" | "weds"
        | "wednesday" | "thu" | "thur" | "thurs" | "thursday" | "fri" | "friday" | "sat"
        | "saturday" => Word::Weekday,
        "bc" => Word::Era(true),
        "ad" => Word::Era(false),
        "am" => Word::Meridian(false),
        "pm" => Word::Meridian(true),
        "j" | "jd" | "julian" => Word::Julian,
        "t" => Word::IsoTime,
        "at" | "on" => Word::Ignore,
        "dst" => Word::Dst,
        "allballs" | "epoch" | "infinity" | "today" | "tomorrow" | "yesterday" | "now" | "d"
        | "dow" | "doy" | "h" | "isodow" | "isoyear" | "m" | "mm" | "s" | "y" => Word::Reserved,
        _ => Word::Unknown,
    }
}

fn end_of(bytes: &[u8], start: usize, matches: impl Fn(u8) -> bool) -> usize {
    start
        + bytes[start..]
            .iter()
            .copied()
            .take_while(|&b| matches(b))
            .count()
}

fn digit(b: u8) -> bool {
    b.is_ascii_digit()
}

// ParseDateTime: retain the kind of every field. In particular, punctuated
// dates and zone names share a token kind whose meaning depends on context.
fn fields(s: &str) -> Result<Vec<Field<'_>>, Error> {
    let bytes = s.as_bytes();
    let mut fields = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let start = i;
        let kind = match bytes[i] {
            b'0'..=b'9' => {
                i = end_of(bytes, i, digit);
                match bytes.get(i).copied() {
                    Some(b':') => {
                        i = end_of(bytes, i + 1, |b| digit(b) || matches!(b, b':' | b'.'));
                        Kind::Clock
                    }
                    Some(delim @ (b'-' | b'/' | b'.')) => {
                        i += 1;
                        if bytes.get(i).is_some_and(u8::is_ascii_digit) {
                            i = end_of(bytes, i, digit);
                            if bytes.get(i) == Some(&delim) {
                                i = end_of(bytes, i + 1, |b| digit(b) || b == delim);
                                Kind::Date
                            } else if delim == b'.' {
                                Kind::Number
                            } else {
                                Kind::Date
                            }
                        } else {
                            i = end_of(bytes, i, |b| b.is_ascii_alphanumeric() || b == delim);
                            Kind::Date
                        }
                    }
                    _ => Kind::Number,
                }
            }
            b'.' => {
                i = end_of(bytes, i + 1, digit);
                Kind::Number
            }
            b'a'..=b'z' => {
                i = end_of(bytes, i, |b| b.is_ascii_alphabetic());
                let next = bytes.get(i).copied();
                if matches!(next, Some(b'-' | b'/' | b'.'))
                    || matches!(next, Some(b'+' | b'0'..=b'9'))
                        && keyword(&s[start..i]) == Word::Unknown
                {
                    i = end_of(bytes, i + 1, |b| {
                        b.is_ascii_alphanumeric()
                            || matches!(b, b'+' | b'-' | b'/' | b'_' | b'.' | b':')
                    });
                    Kind::Date
                } else {
                    Kind::Word
                }
            }
            b'+' | b'-' => {
                i = end_of(bytes, i + 1, |b| b.is_ascii_whitespace());
                match bytes.get(i) {
                    Some(b'0'..=b'9') => {
                        i = end_of(bytes, i + 1, |b| {
                            digit(b) || matches!(b, b':' | b'.' | b'-')
                        });
                        Kind::Offset
                    }
                    Some(b'a'..=b'z') => {
                        i = end_of(bytes, i + 1, |b| b.is_ascii_alphabetic());
                        Kind::Special
                    }
                    _ => return Err(Error::Syntax),
                }
            }
            b if b.is_ascii_whitespace() || b.is_ascii_punctuation() => {
                i += 1;
                continue;
            }
            _ => return Err(Error::Syntax),
        };
        if fields.len() == MAX_FIELDS {
            return Err(Error::Syntax);
        }
        fields.push(Field {
            kind,
            text: &s[start..i],
        });
    }
    Ok(fields)
}

#[derive(Default)]
struct Calendar {
    year: Option<i32>,
    month: Option<i32>,
    day: Option<i32>,
    ordinal: Option<i32>,
    julian: Option<Date>,
    short_year: bool,
    text_month: bool,
    bc: Option<bool>,
}

fn integer(text: &str) -> Result<i32, Error> {
    if text.is_empty() || !text.bytes().all(digit) {
        return Err(Error::Syntax);
    }
    text.parse().map_err(|_| Error::Range)
}

fn set<T>(slot: &mut Option<T>, value: T) -> Result<(), Error> {
    if slot.replace(value).is_some() {
        return Err(Error::Syntax);
    }
    Ok(())
}

impl Calendar {
    fn has_fields(&self) -> bool {
        self.year.is_some() || self.month.is_some() || self.day.is_some() || self.julian.is_some()
    }

    fn has_month_day(&self) -> bool {
        self.julian.is_some()
            || self.ordinal.is_some()
            || self.month.is_some() && self.day.is_some()
    }

    fn complete(&self) -> bool {
        self.julian.is_some() || self.year.is_some() && self.has_month_day()
    }

    fn month_name(&mut self, month: i32, separate: bool) -> Result<(), Error> {
        if separate
            && !self.text_month
            && self.day.is_none()
            && let Some(numeric) = self.month.filter(|n| (1..=31).contains(n))
        {
            self.day = Some(numeric);
            self.month = None;
        }
        set(&mut self.month, month)?;
        self.text_month = true;
        Ok(())
    }

    // DecodeNumber: an unambiguous first year overrides DateOrder. A
    // textual month can also disambiguate a previously assumed short year.
    fn number(&mut self, text: &str, order: DateOrder) -> Result<(), Error> {
        self.number_with_length(text, text.len(), order)
    }

    fn number_with_length(
        &mut self,
        text: &str,
        length: usize,
        order: DateOrder,
    ) -> Result<(), Error> {
        let value = integer(text)?;
        let wide_year = length >= 3;
        if self.complete() {
            return Err(Error::Syntax);
        }
        match (self.year, self.month, self.day) {
            (None, None, None) if wide_year || order == DateOrder::Ymd => {
                self.year = Some(value);
                self.short_year = !wide_year;
            }
            (None, None, None) if order == DateOrder::Dmy => self.day = Some(value),
            (None, None, None) => self.month = Some(value),
            (Some(_), None, None) if length == 3 && (1..=366).contains(&value) => {
                self.ordinal = Some(value);
            }
            (Some(_), None, None) | (None, None, Some(_)) => self.month = Some(value),
            (None, Some(_), None) if self.text_month && (wide_year || order == DateOrder::Ymd) => {
                self.year = Some(value);
                self.short_year = !wide_year;
            }
            (Some(year), Some(_), None) if self.text_month && wide_year && self.short_year => {
                self.day = Some(year);
                self.year = Some(value);
                self.short_year = false;
            }
            (None, Some(_), None) | (Some(_), Some(_), None) => self.day = Some(value),
            (None, Some(_), Some(_)) => {
                self.year = Some(value);
                self.short_year = !wide_year;
            }
            _ => return Err(Error::Syntax),
        }
        Ok(())
    }

    // DecodeDate: within a punctuated date, consider the textual month
    // before either numeric field, irrespective of the month's position.
    fn delimited(&mut self, mut text: &str, order: DateOrder) -> Result<(), Error> {
        let mut numeric = Vec::new();
        while !text.is_empty() {
            text = text.trim_start_matches(|c: char| !c.is_ascii_alphanumeric());
            if text.is_empty() {
                return Err(Error::Syntax);
            }
            let numeric_field = text.as_bytes()[0].is_ascii_digit();
            let end = end_of(text.as_bytes(), 0, |b| {
                if numeric_field {
                    digit(b)
                } else {
                    b.is_ascii_alphabetic()
                }
            });
            let field = &text[..end];
            if numeric_field {
                numeric.push(field);
            } else if let Word::Month(month) = keyword(field) {
                self.month_name(month, false)?;
            } else {
                return Err(Error::Syntax);
            }
            // DecodeDate consumes a separator after each subfield.
            text = &text[(end + 1).min(text.len())..];
        }
        for field in numeric {
            self.number(field, order)?;
        }
        if self.complete() {
            Ok(())
        } else {
            Err(Error::Syntax)
        }
    }

    // DecodeNumberField works backwards from the two-digit day and month.
    fn compact(&mut self, text: &str) -> Result<(), Error> {
        let (year, rest) = text.split_at(text.len() - 4);
        let (month, day) = rest.split_at(2);
        set(&mut self.year, integer(year)?)?;
        set(&mut self.month, integer(month)?)?;
        set(&mut self.day, integer(day)?)?;
        self.short_year = year.len() == 2;
        Ok(())
    }

    fn julian(&mut self, text: &str) -> Result<Option<Time>, Error> {
        if self.has_fields() {
            return Err(Error::Syntax);
        }
        let (day, fraction) = text.split_once('.').unwrap_or((text, ""));
        self.julian = Some(Date::from_days(integer(day)? - JULIAN_EPOCH)?);
        if fraction.is_empty() {
            Ok(None)
        } else {
            let fraction: f64 = format!(".{fraction}").parse().map_err(|_| Error::Syntax)?;
            Time::from_microseconds((fraction * USECS_PER_DAY as f64).trunc() as i64).map(Some)
        }
    }

    // ValidateDate: expand short years only for AD, then decode ordinal dates.
    fn finish(&self) -> Result<Option<Date>, Error> {
        if let Some(julian) = self.julian {
            return Ok(Some(julian));
        }
        if self.month.is_some_and(|m| !(1..=12).contains(&m))
            || self.day.is_some_and(|d| !(1..=31).contains(&d))
        {
            return Err(Error::Range);
        }
        let Some(mut year) = self.year else {
            return Ok(None);
        };
        if self.bc == Some(true) {
            if year == 0 {
                return Err(Error::Range);
            }
            year = -year;
        } else if self.short_year {
            year += if year < 70 { 2000 } else { 1900 };
        } else if year == 0 {
            return Err(Error::Range);
        }
        if let Some(ordinal) = self.ordinal {
            if !(-4714..=5_874_897).contains(&year) {
                return Err(Error::Range);
            }
            let astronomical = i64::from(if year < 0 { year + 1 } else { year });
            let days = civil_days(astronomical, 1, 1) + i64::from(ordinal) - 1;
            return Date::from_days(i32::try_from(days).map_err(|_| Error::Range)?).map(Some);
        }
        match (self.month, self.day) {
            (Some(month), Some(day)) => Date::from_ymd(year, month as u32, day as u32).map(Some),
            _ => Ok(None),
        }
    }
}

struct ClockField {
    time: Time,
    hour: i64,
}

impl ClockField {
    fn from_time(time: Time) -> Self {
        Self {
            time,
            hour: time.microseconds() / USECS_PER_HOUR,
        }
    }

    fn with_meridian(self, meridian: Option<bool>) -> Result<Time, Error> {
        let Some(pm) = meridian else {
            return Ok(self.time);
        };
        if self.hour > 12 {
            return Err(Error::Range);
        }
        let adjustment = match (pm, self.hour) {
            (false, 12) => -12,
            (true, 12) | (false, _) => 0,
            (true, _) => 12,
        };
        Time::from_microseconds(self.time.microseconds() + adjustment * USECS_PER_HOUR)
    }
}

fn clock_field(text: &str) -> Result<ClockField, Error> {
    let mut parts = text.split(':');
    let first = parts.next().ok_or(Error::Syntax)?;
    let second = parts.next().ok_or(Error::Syntax)?;
    let third = parts.next();
    if parts.next().is_some() {
        return Err(Error::Syntax);
    }
    // DecodeTime treats a fraction on the second field as mm:ss.frac.
    let (hour, minute, seconds) = if third.is_none() && second.contains('.') {
        ("0", first, second)
    } else {
        (
            first,
            if second.is_empty() { "0" } else { second },
            third.unwrap_or("0"),
        )
    };
    let seconds = if seconds.is_empty() { "0" } else { seconds };
    let us = clock(&format!("{hour}:{minute}:{seconds}"), false)?;
    Ok(ClockField {
        time: Time::from_microseconds(us)?,
        hour: number(hour)?,
    })
}

fn compact_clock(text: &str) -> Result<ClockField, Error> {
    let (digits, fraction) = match text.split_once('.') {
        Some((_, "")) => return Err(Error::Syntax),
        Some((digits, fraction)) => (digits, fraction),
        None => (text, "0"),
    };
    if !matches!(digits.len(), 4 | 6) || !digits.bytes().all(digit) {
        return Err(Error::Syntax);
    }
    let (hour, rest) = digits.split_at(2);
    let (minute, seconds) = rest.split_at(2);
    let seconds = if seconds.is_empty() { "0" } else { seconds };
    clock_field(&format!("{hour}:{minute}:{seconds}.{fraction}"))
}

fn numeric_offset(text: &str) -> Result<i32, Error> {
    let (sign, text) = text.split_at(1);
    let raw = text.trim_start();
    let mut parts = raw.split(':');
    let first = parts.next().unwrap_or_default();
    let read = |s: &str| {
        if s.starts_with('-') && s[1..].bytes().all(digit) {
            return Err(Error::TimeZoneDisplacement);
        }
        integer(s).map_err(|e| match e {
            Error::Range => Error::TimeZoneDisplacement,
            other => other,
        })
    };
    let mut hour = read(first)?;
    let mut minute = 0;
    let mut second = 0;
    if let Some(m) = parts.next() {
        minute = if m.is_empty() { 0 } else { read(m)? };
        if let Some(s) = parts.next() {
            second = if s.is_empty() { 0 } else { read(s)? };
        }
    } else if raw.len() > 2 {
        minute = hour % 100;
        hour /= 100;
    }
    if hour >= 16 || minute >= 60 || second >= 60 {
        return Err(Error::TimeZoneDisplacement);
    }
    if parts.next().is_some() {
        return Err(Error::Syntax);
    }
    let east = hour * 3600 + minute * 60 + second;
    Ok(if sign == "-" { -east } else { east })
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    DateTime,
    Time,
}

struct Decoded {
    date: Option<Date>,
    time: Option<Time>,
    zone: Option<InputZone>,
    dst: bool,
}

impl Decoded {
    fn local(&self) -> Result<i128, Error> {
        let date = self.date.ok_or(Error::Syntax)?;
        Ok(i128::from(date.days()) * i128::from(USECS_PER_DAY)
            + i128::from(self.time.map_or(0, Time::microseconds)))
    }

    fn explicit_offset(&self, discard_dynamic: bool) -> Result<Option<i32>, Error> {
        let Some(zone) = &self.zone else {
            return if self.dst {
                Err(Error::Syntax)
            } else {
                Ok(None)
            };
        };
        if self.dst && !zone.standard_offset() {
            return Err(Error::Syntax);
        }
        if discard_dynamic && self.date.is_none() && matches!(zone, InputZone::Dynamic(..)) {
            return Ok(None);
        }
        let seconds = zone.resolve(self.date, self.time)?;
        Ok(Some(seconds + if self.dst { 3600 } else { 0 }))
    }
}

// DecodeDateTime / DecodeTimeOnly. Keep independent field state because time,
// date, era, and zone may be interleaved (for example Postgres DateStyle).
fn decode(s: &str, order: DateOrder, session: SessionZone, mode: Mode) -> Result<Decoded, Error> {
    let fields = fields(s)?;
    let mut cal = Calendar::default();
    let mut time = None;
    let mut zone = None;
    let mut weekday = None;
    let mut meridian = None;
    let mut fractional_second = None;
    let mut dst = None;
    let mut prefix = None;
    for (index, field) in fields.iter().enumerate() {
        let text = field.text;
        match field.kind {
            Kind::Offset => {
                set(&mut zone, InputZone::Fixed(numeric_offset(text)?, false))?;
            }
            Kind::Clock if prefix.is_none() || prefix == Some(Word::IsoTime) => {
                prefix = None;
                set(&mut time, clock_field(text)?)?;
            }
            Kind::Date | Kind::Number if prefix == Some(Word::Julian) => {
                prefix = None;
                let fraction = if field.kind == Kind::Date {
                    let (day, offset) = text.split_once('-').ok_or(Error::Syntax)?;
                    set(
                        &mut zone,
                        InputZone::Fixed(numeric_offset(&format!("-{offset}"))?, false),
                    )?;
                    cal.julian(day)?;
                    Some(Time(0))
                } else {
                    cal.julian(text)?
                };
                if let Some(clock) = fraction {
                    set(&mut time, ClockField::from_time(clock))?;
                }
            }
            Kind::Date
                if mode == Mode::Time
                    && index == 0
                    && fields.len() >= 2
                    && (fields[1].kind == Kind::Clock
                        || fields.last().unwrap().kind == Kind::Date) =>
            {
                cal.delimited(text, order)?;
            }
            Kind::Date if cal.has_month_day() || mode == Mode::Time || prefix.is_some() => {
                if text.as_bytes()[0].is_ascii_digit() {
                    if time.is_some() || prefix.is_some_and(|p| p != Word::IsoTime) {
                        return Err(Error::Syntax);
                    }
                    let (digits, offset) = text.split_once('-').ok_or(Error::Syntax)?;
                    prefix = None;
                    set(
                        &mut zone,
                        InputZone::Fixed(numeric_offset(&format!("-{offset}"))?, false),
                    )?;
                    set(&mut time, compact_clock(digits)?)?;
                } else {
                    set(
                        &mut zone,
                        InputZone::named(text).ok_or(Error::UnknownTimeZone)?,
                    )?;
                }
            }
            Kind::Date => {
                // DecodeDate rejects a pre-existing weekday in its field mask.
                if weekday.is_some() {
                    return Err(Error::Syntax);
                }
                cal.delimited(text, order)?;
            }
            Kind::Number if mode == Mode::DateTime && cal.complete() && time.is_some() => {
                // DecodeNumberField has no date or time fields left to fill.
                return Err(Error::Syntax);
            }
            Kind::Number if prefix == Some(Word::IsoTime) => {
                prefix = None;
                set(&mut time, compact_clock(text)?)?;
            }
            Kind::Number
                if mode == Mode::Time
                    && index == 0
                    && text.contains('.')
                    && fields.len() >= 2
                    && fields.last().is_some_and(|last| last.kind == Kind::Date) =>
            {
                cal.delimited(text, order)?;
            }
            Kind::Number if mode == Mode::Time || cal.complete() => {
                set(&mut time, compact_clock(text)?)?;
            }
            Kind::Number if !cal.has_fields() && text.contains('.') => {
                if weekday.is_some() {
                    return Err(Error::Syntax);
                }
                cal.delimited(text, order)?
            }
            Kind::Number if text.len() >= 6 && !text.contains('.') => cal.compact(text)?,
            Kind::Number => {
                let (number, fraction) = text
                    .split_once('.')
                    .map_or((text, None), |(n, f)| (n, Some(f)));
                if let Some(fraction) = fraction {
                    if number.len() > 2 {
                        set(&mut time, compact_clock(text)?)?;
                        continue;
                    }
                    let micros = clock_field(&format!("0:0:0.{fraction}"))?
                        .time
                        .microseconds();
                    fractional_second = Some(micros);
                }
                cal.number_with_length(number, text.len(), order)?;
            }
            Kind::Word => {
                if let Some(abbrev) = input_abbreviation(text, session)? {
                    set(&mut zone, abbrev)?;
                    continue;
                }
                match keyword(text) {
                    Word::Month(month) if mode == Mode::DateTime => cal.month_name(month, true)?,
                    // PostgreSQL DecodeTimeOnly has no weekday field.
                    Word::Weekday if mode == Mode::DateTime => set(&mut weekday, ())?,
                    Word::Era(bc) => set(&mut cal.bc, bc)?,
                    Word::Meridian(pm) => set(&mut meridian, pm)?,
                    Word::Ignore => {}
                    Word::Dst => set(&mut dst, ())?,
                    Word::Julian => set(&mut prefix, Word::Julian)?,
                    Word::IsoTime if cal.complete() || mode == Mode::Time => {
                        set(&mut prefix, Word::IsoTime)?;
                    }
                    Word::Unknown => {
                        set(&mut zone, InputZone::named(text).ok_or(Error::Syntax)?)?;
                    }
                    _ => return Err(Error::Syntax),
                }
            }
            _ => return Err(Error::Syntax),
        }
    }
    if prefix.is_some() {
        return Err(Error::Syntax);
    }
    let date = cal.finish()?;
    if mode == Mode::DateTime && date.is_none() || mode == Mode::Time && time.is_none() {
        return Err(Error::Syntax);
    }
    let time = if time.is_some() || meridian.is_some() || fractional_second.is_some() {
        Some(
            time.unwrap_or_else(|| ClockField::from_time(Time(fractional_second.unwrap_or(0))))
                .with_meridian(meridian)?,
        )
    } else {
        None
    };
    Ok(Decoded {
        date,
        time,
        zone,
        dst: dst.is_some(),
    })
}

fn special_date(text: &str) -> Option<Date> {
    match text {
        "infinity" | "+infinity" => Some(Date::INFINITY),
        "-infinity" => Some(Date::NEG_INFINITY),
        "epoch" => Some(Date(-10_957)),
        _ => None,
    }
}

pub(super) fn date(text: &str, order: DateOrder) -> Result<Date, Error> {
    let text = bounded(text)?.to_ascii_lowercase();
    if let Some(date) = special_date(&text) {
        return Ok(date);
    }
    let decoded = decode(&text, order, SessionZone::Fixed(0), Mode::DateTime)?;
    decoded.explicit_offset(false)?;
    decoded.date.ok_or(Error::Syntax)
}

fn decode_time(text: &str, order: DateOrder, zone: SessionZone) -> Result<Decoded, Error> {
    let text = bounded(text)?.to_ascii_lowercase();
    if text == "allballs" {
        return Ok(Decoded {
            date: None,
            time: Some(Time(0)),
            zone: Some(InputZone::Fixed(0, false)),
            dst: false,
        });
    }
    decode(&text, order, zone, Mode::Time)
}

pub(super) fn time(
    text: &str,
    settings: &ParserSettings,
    with_zone: bool,
) -> Result<(Time, i32), Error> {
    let decoded = decode_time(text, settings.date_order, SessionZone::Fixed(0))?;
    let offset = decoded
        .explicit_offset(!with_zone)?
        .unwrap_or(settings.offset_east);
    let offset = if with_zone { offset_valid(offset)? } else { 0 };
    Ok((decoded.time.ok_or(Error::Syntax)?, offset))
}

pub(super) fn time_in_session(
    text: &str,
    settings: &SessionSettings,
    transaction_us: i64,
) -> Result<TimeTz, Error> {
    let mut decoded = decode_time(text, settings.date_order, settings.zone)?;
    if decoded.date.is_none()
        && matches!(decoded.zone, None | Some(InputZone::Dynamic(..)))
    {
        // An undated dynamic abbreviation or implicit zone uses today's
        // date in the session zone and the time supplied in the input.
        let instant = TimestampTz::from_microseconds(transaction_us)?;
        let local = settings.zone.to_local(instant)?;
        let days = i32::try_from(local.0.div_euclid(USECS_PER_DAY)).map_err(|_| Error::Range)?;
        decoded.date = Some(Date::from_days(days)?);
    }
    let offset = match decoded.explicit_offset(false)? {
        Some(offset) => offset,
        None => settings.zone.input_offset(decoded.date, decoded.time)?,
    };
    TimeTz::new(decoded.time.ok_or(Error::Syntax)?, offset)
}

pub(super) fn stamp(
    text: &str,
    order: DateOrder,
    session: SessionZone,
    with_zone: bool,
) -> Result<i64, Error> {
    let text = bounded(text)?.to_ascii_lowercase();
    if let Some(date) = special_date(&text) {
        return Ok(match date {
            Date::INFINITY => i64::MAX,
            Date::NEG_INFINITY => i64::MIN,
            _ => i64::from(date.days()) * USECS_PER_DAY,
        });
    }
    let decoded = decode(&text, order, session, Mode::DateTime)?;
    let local = decoded.local()?;
    let explicit_offset = decoded.explicit_offset(false)?;
    let utc = if with_zone {
        let offset = match explicit_offset {
            Some(offset) => offset,
            None => session.input_offset(decoded.date, decoded.time)?,
        };
        local - i128::from(offset) * 1_000_000
    } else {
        local
    };
    i64::try_from(utc).map_err(|_| Error::Range)
}

pub(super) fn zoned_stamp(text: &str, order: DateOrder, offset: i32) -> Result<TimestampTz, Error> {
    TimestampTz::from_microseconds(stamp(text, order, SessionZone::Fixed(offset), true)?)
}
