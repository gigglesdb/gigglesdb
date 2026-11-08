//! Deterministic temporal functions with explicit zone context.
use super::*;
use crate::Numeric;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TemporalField {
    Microseconds,
    Milliseconds,
    Second,
    Minute,
    Hour,
    Day,
    Month,
    Quarter,
    Week,
    Year,
    Decade,
    Century,
    Millennium,
    IsoYear,
    Dow,
    IsoDow,
    Doy,
    Julian,
    Epoch,
    Timezone,
    TimezoneHour,
    TimezoneMinute,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TemporalFunctionError {
    UnsupportedField,
    NumericOverflow,
    DivisionByZero,
    Range,
    InvalidStride,
    InfiniteStride,
    InfiniteOrigin,
}
type Result<T> = std::result::Result<T, TemporalFunctionError>;
fn range<T>(r: std::result::Result<T, Error>) -> Result<T> {
    r.map_err(|_| TemporalFunctionError::Range)
}
fn integer(v: i128) -> Numeric {
    Numeric::parse(&v.to_string()).expect("bounded integer")
}
fn fraction(v: i128, scale: u32) -> Numeric {
    let a = v.abs();
    Numeric::parse(&format!(
        "{}{}.{:0width$}",
        if v < 0 { "-" } else { "" },
        a / 10i128.pow(scale),
        a % 10i128.pow(scale),
        width = scale as usize
    ))
    .expect("bounded decimal")
}
fn infinity(f: TemporalField, negative: bool) -> Result<Option<Numeric>> {
    use TemporalField::*;
    match f {
        Year | IsoYear | Decade | Century | Millennium | Julian | Epoch => Ok(Some(
            Numeric::parse(if negative { "-Infinity" } else { "Infinity" }).expect("infinity"),
        )),
        Microseconds | Milliseconds | Second | Minute | Hour | Day | Month | Quarter | Week
        | Dow | IsoDow | Doy => Ok(None),
        _ => Err(TemporalFunctionError::UnsupportedField),
    }
}
fn iso(day: i64) -> (i64, i64) {
    let y = civil(day + 3 - (day + 5).rem_euclid(7)).0;
    let jan4 = civil_days(y, 1, 4);
    (
        y,
        (day - (jan4 - (jan4 + 5).rem_euclid(7))).div_euclid(7) + 1,
    )
}
fn clock_extract(f: TemporalField, t: i64) -> Result<Option<Numeric>> {
    use TemporalField::*;
    Ok(Some(match f {
        Hour => integer((t / 3_600_000_000).into()),
        Minute => integer((t / 60_000_000 % 60).into()),
        Second => fraction((t % 60_000_000).into(), 6),
        Milliseconds => fraction((t % 60_000_000).into(), 3),
        Microseconds => integer((t % 60_000_000).into()),
        Epoch => fraction(t.into(), 6),
        _ => return Err(TemporalFunctionError::UnsupportedField),
    }))
}
fn calendar(f: TemporalField, day: i64, time: i64) -> Result<Option<Numeric>> {
    use TemporalField::*;
    let (y, m, d) = civil(day);
    let v = match f {
        Year => {
            if y <= 0 {
                y - 1
            } else {
                y
            }
        }
        Month => m,
        Day => d,
        Quarter => (m - 1) / 3 + 1,
        Decade => y.div_euclid(10),
        Century => {
            if y > 0 {
                (y + 99) / 100
            } else {
                -((100 - y) / 100)
            }
        }
        Millennium => {
            if y > 0 {
                (y + 999) / 1000
            } else {
                -((1000 - y) / 1000)
            }
        }
        IsoYear => {
            let y = iso(day).0;
            if y <= 0 { y - 1 } else { y }
        }
        Week => iso(day).1,
        Dow => (day + 6).rem_euclid(7),
        IsoDow => (day + 5).rem_euclid(7) + 1,
        Doy => day - civil_days(y, 1, 1) + 1,
        Julian => {
            return Ok(Some(range(
                integer(i128::from(day + 2451545)).checked_add(&range(
                    integer(time.into()).checked_div(&integer(USECS_PER_DAY.into())),
                )?),
            )?));
        }
        _ => return clock_extract(f, time),
    };
    Ok(Some(integer(v.into())))
}
impl Date {
    pub fn extract(self, f: TemporalField) -> Result<Option<Numeric>> {
        if matches!(
            f,
            TemporalField::Hour
                | TemporalField::Minute
                | TemporalField::Second
                | TemporalField::Milliseconds
                | TemporalField::Microseconds
        ) {
            return Err(TemporalFunctionError::UnsupportedField);
        }
        if self == Self::INFINITY || self == Self::NEG_INFINITY {
            return infinity(f, self == Self::NEG_INFINITY);
        }
        if f == TemporalField::Epoch {
            return Ok(Some(integer((i128::from(self.0) + 10957) * 86400)));
        }
        match f {
            TemporalField::Hour
            | TemporalField::Minute
            | TemporalField::Second
            | TemporalField::Milliseconds
            | TemporalField::Microseconds => Err(TemporalFunctionError::UnsupportedField),
            _ => calendar(f, self.0.into(), 0),
        }
    }
}
impl Time {
    pub fn extract(self, f: TemporalField) -> Result<Option<Numeric>> {
        clock_extract(f, self.0)
    }
}
impl TimeTz {
    pub fn extract(self, f: TemporalField) -> Result<Option<Numeric>> {
        use TemporalField::*;
        let o = self.offset_east();
        match f {
            Timezone => Ok(Some(integer(o.into()))),
            TimezoneHour => Ok(Some(integer((o / 3600).into()))),
            TimezoneMinute => Ok(Some(integer((o / 60 % 60).into()))),
            Epoch => Ok(Some(fraction(
                i128::from(self.time().0) - i128::from(o) * 1_000_000,
                6,
            ))),
            _ => self.time().extract(f),
        }
    }
}
impl Timestamp {
    pub fn extract(self, f: TemporalField) -> Result<Option<Numeric>> {
        if self == Self::INFINITY || self == Self::NEG_INFINITY {
            return infinity(f, self == Self::NEG_INFINITY);
        }
        if f == TemporalField::Epoch {
            return Ok(Some(timestamp_epoch(self.0)?));
        }
        calendar(
            f,
            self.0.div_euclid(USECS_PER_DAY),
            self.0.rem_euclid(USECS_PER_DAY),
        )
    }
    pub fn date_bin(self, stride: Interval, origin: Self) -> Result<Self> {
        if self == Self::INFINITY || self == Self::NEG_INFINITY {
            return Ok(self);
        }
        range(Self::from_microseconds(bin(self.0, stride, origin.0)?))
    }
}
impl TimestampTz {
    pub fn extract(self, f: TemporalField, zone: SessionZone) -> Result<Option<Numeric>> {
        if self == Self::INFINITY || self == Self::NEG_INFINITY {
            return infinity(f, self == Self::NEG_INFINITY);
        }
        if f == TemporalField::Epoch {
            return Ok(Some(timestamp_epoch(self.0)?));
        }
        let local = range(zone.to_local(self))?;
        let o = (i128::from(local.0) - i128::from(self.0)) / 1_000_000;
        use TemporalField::*;
        match f {
            Timezone => Ok(Some(integer(o))),
            TimezoneHour => Ok(Some(integer(o / 3600))),
            TimezoneMinute => Ok(Some(integer(o / 60 % 60))),
            _ => local.extract(f),
        }
    }
    pub fn date_bin(self, stride: Interval, origin: Self) -> Result<Self> {
        if self == Self::INFINITY || self == Self::NEG_INFINITY {
            return Ok(self);
        }
        range(Self::from_microseconds(bin(self.0, stride, origin.0)?))
    }
}
fn bin(value: i64, stride: Interval, origin: i64) -> Result<i64> {
    if origin == i64::MIN || origin == i64::MAX {
        return Err(TemporalFunctionError::InfiniteOrigin);
    }
    if stride.same_components(Interval::INFINITY) || stride.same_components(Interval::NEG_INFINITY)
    {
        return Err(TemporalFunctionError::InfiniteStride);
    }
    if stride.months != 0 {
        return Err(TemporalFunctionError::InvalidStride);
    }
    let span = i128::from(
        i64::from(stride.days)
            .checked_mul(USECS_PER_DAY)
            .and_then(|v| v.checked_add(stride.microseconds))
            .ok_or(TemporalFunctionError::Range)?,
    );
    if span <= 0 || span > i128::from(i64::MAX) {
        return Err(TemporalFunctionError::InvalidStride);
    }
    let diff = value
        .checked_sub(origin)
        .ok_or(TemporalFunctionError::Range)?;
    i64::try_from(i128::from(origin) + i128::from(diff).div_euclid(span) * span)
        .map_err(|_| TemporalFunctionError::Range)
}
pub fn make_date(year: i32, month: u32, day: u32) -> Result<Date> {
    range(Date::from_ymd(year, month, day))
}
/// Seconds supplied as exact microseconds within the minute.
pub fn make_time(hour: u32, minute: u32, second_microseconds: u32) -> Result<Time> {
    if hour > 24
        || minute > 59
        || second_microseconds >= 60_000_000
        || (hour == 24 && (minute != 0 || second_microseconds != 0))
    {
        return Err(TemporalFunctionError::Range);
    }
    range(Time::from_microseconds(
        i64::from(hour) * 3_600_000_000
            + i64::from(minute) * 60_000_000
            + i64::from(second_microseconds),
    ))
}
pub fn make_timestamp(
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second_microseconds: u32,
) -> Result<Timestamp> {
    let d = make_date(year, month, day)?;
    let t = make_time(hour, minute, second_microseconds)?;
    let v = i128::from(d.days()) * i128::from(USECS_PER_DAY) + i128::from(t.microseconds());
    range(Timestamp::from_microseconds(
        i64::try_from(v).map_err(|_| TemporalFunctionError::Range)?,
    ))
}
impl Interval {
    pub fn extract(self, f: TemporalField) -> Result<Option<Numeric>> {
        use TemporalField::*;
        if self.same_components(Self::INFINITY) || self.same_components(Self::NEG_INFINITY) {
            return match f {
                Hour | Day | Year | Decade | Century | Millennium | Epoch => Ok(Some(
                    Numeric::parse(if self.same_components(Self::NEG_INFINITY) {
                        "-Infinity"
                    } else {
                        "Infinity"
                    })
                    .expect("infinity"),
                )),
                Microseconds | Milliseconds | Second | Minute | Week | Month | Quarter => Ok(None),
                _ => Err(TemporalFunctionError::UnsupportedField),
            };
        }
        let m = i128::from(self.months);
        let d = i128::from(self.days);
        let v = match f {
            Week => d / 7,
            Year => m / 12,
            Month => m % 12,
            Day => d,
            Quarter => {
                if m < 0 {
                    -((-m % 12) / 3 + 1)
                } else {
                    (m % 12) / 3 + 1
                }
            }
            Decade => m / 120,
            Century => m / 1200,
            Millennium => m / 12000,
            Epoch => {
                return Ok(Some(fraction(
                    ((m / 12) * 31_557_600 + (m % 12) * 2_592_000 + d * 86400) * 1_000_000
                        + i128::from(self.microseconds),
                    6,
                )));
            }
            _ => return clock_extract(f, self.microseconds),
        };
        Ok(Some(integer(v)))
    }
    pub fn date_trunc(self, f: TemporalField) -> Result<Self> {
        use TemporalField::*;
        let supported = matches!(
            f,
            Millennium
                | Century
                | Decade
                | Year
                | Quarter
                | Month
                | Day
                | Hour
                | Minute
                | Second
                | Milliseconds
                | Microseconds
        );
        if !supported {
            return Err(TemporalFunctionError::UnsupportedField);
        }
        if self.same_components(Self::INFINITY) || self.same_components(Self::NEG_INFINITY) {
            return Ok(self);
        }
        let (mut m, mut d, mut t) = (self.months, self.days, self.microseconds);
        match f {
            Millennium => {
                m = m / 12000 * 12000;
                d = 0;
                t = 0
            }
            Century => {
                m = m / 1200 * 1200;
                d = 0;
                t = 0
            }
            Decade => {
                m = m / 120 * 120;
                d = 0;
                t = 0
            }
            Year => {
                m = m / 12 * 12;
                d = 0;
                t = 0
            }
            Quarter => {
                m = m / 3 * 3;
                d = 0;
                t = 0
            }
            Month => {
                d = 0;
                t = 0
            }
            Day => t = 0,
            Hour => t = t / 3_600_000_000 * 3_600_000_000,
            Minute => t = t / 60_000_000 * 60_000_000,
            Second => t = t / 1_000_000 * 1_000_000,
            Milliseconds => t = t / 1000 * 1000,
            Microseconds => {}
            _ => unreachable!(),
        }
        Ok(Self::new(m, d, t))
    }
}
impl Timestamp {
    pub fn date_trunc(self, f: TemporalField) -> Result<Self> {
        use TemporalField::*;
        if !matches!(
            f,
            Millennium
                | Century
                | Decade
                | Year
                | Quarter
                | Month
                | Week
                | Day
                | Hour
                | Minute
                | Second
                | Milliseconds
                | Microseconds
        ) {
            return Err(TemporalFunctionError::UnsupportedField);
        }
        if self == Self::INFINITY || self == Self::NEG_INFINITY {
            return Ok(self);
        }
        let day = self.0.div_euclid(USECS_PER_DAY);
        let time = self.0.rem_euclid(USECS_PER_DAY);
        let (mut y, mut m, _) = civil(day);
        let (day, time) = match f {
            Millennium | Century => {
                let n = if f == Millennium { 1000 } else { 100 };
                y = if y > 0 {
                    (y - 1) / n * n + 1
                } else {
                    -((n - y) / n) * n + 1
                };
                m = 1;
                let d = 1;
                (civil_days(y, m, d), 0)
            }
            Decade => {
                y = y.div_euclid(10) * 10;
                (civil_days(y, 1, 1), 0)
            }
            Year => (civil_days(y, 1, 1), 0),
            Quarter => (civil_days(y, (m - 1) / 3 * 3 + 1, 1), 0),
            Month => (civil_days(y, m, 1), 0),
            Week => (day - (day + 5).rem_euclid(7), 0),
            Day => (day, 0),
            Hour => (day, time / 3_600_000_000 * 3_600_000_000),
            Minute => (day, time / 60_000_000 * 60_000_000),
            Second => (day, time / 1_000_000 * 1_000_000),
            Milliseconds => (day, time / 1000 * 1000),
            Microseconds => (day, time),
            _ => unreachable!(),
        };
        let v = i128::from(day) * i128::from(USECS_PER_DAY) + i128::from(time);
        range(Self::from_microseconds(
            i64::try_from(v).map_err(|_| TemporalFunctionError::Range)?,
        ))
    }
}
impl TimestampTz {
    pub fn date_trunc(self, f: TemporalField, zone: SessionZone) -> Result<Self> {
        let local = range(zone.to_local(self))?;
        let truncated = local.date_trunc(f)?;
        // Subday truncation retains the original UTC offset, even in an overlap.
        if matches!(
            f,
            TemporalField::Hour
                | TemporalField::Minute
                | TemporalField::Second
                | TemporalField::Milliseconds
                | TemporalField::Microseconds
        ) {
            if self == Self::INFINITY || self == Self::NEG_INFINITY {
                return Ok(self);
            }
            return range(Self::from_microseconds(
                self.0
                    .checked_sub(local.0 - truncated.0)
                    .ok_or(TemporalFunctionError::Range)?,
            ));
        }
        range(zone.to_instant(truncated))
    }
}

impl Interval {
    pub fn justify_interval(self) -> Result<Self> {
        if self.same_components(Self::INFINITY) || self.same_components(Self::NEG_INFINITY) {
            return Ok(self);
        }
        let (mut m, mut d, mut t) = (
            i64::from(self.months),
            i64::from(self.days),
            self.microseconds,
        );
        if (d > 0 && t > 0) || (d < 0 && t < 0) {
            m = m.checked_add(d / 30).ok_or(TemporalFunctionError::Range)?;
            d %= 30;
            i32::try_from(m).map_err(|_| TemporalFunctionError::Range)?;
        }
        d += t / USECS_PER_DAY;
        t %= USECS_PER_DAY;
        m += d / 30;
        d %= 30;
        i32::try_from(m).map_err(|_| TemporalFunctionError::Range)?;
        if m > 0 && (d < 0 || (d == 0 && t < 0)) {
            d += 30;
            m -= 1;
        } else if m < 0 && (d > 0 || (d == 0 && t > 0)) {
            d -= 30;
            m += 1;
        }
        if d > 0 && t < 0 {
            t += USECS_PER_DAY;
            d -= 1;
        } else if d < 0 && t > 0 {
            t -= USECS_PER_DAY;
            d += 1;
        }
        Ok(Self::new(
            i32::try_from(m).map_err(|_| TemporalFunctionError::Range)?,
            i32::try_from(d).map_err(|_| TemporalFunctionError::Range)?,
            t,
        ))
    }
}

fn float_part(value: Option<Numeric>) -> Result<Option<crate::Float8>> {
    value
        .map(|n| range(crate::Float8::parse(&n.to_string())))
        .transpose()
}
fn timestamp_epoch(t: i64) -> Result<Numeric> {
    let n = i128::from(t) + 946_684_800_000_000;
    if t < i64::MAX - 946_684_800_000_000 {
        Ok(fraction(n, 6))
    } else {
        range(range(integer(n).checked_div(&integer(1_000_000)))?.round(6))
    }
}

fn clock_float(f: TemporalField, t: i64) -> Option<f64> {
    let sec = t / 1_000_000 % 60;
    let frac = t % 1_000_000;
    match f {
        TemporalField::Second => Some(sec as f64 + frac as f64 / 1_000_000.0),
        TemporalField::Milliseconds => Some(sec as f64 * 1000.0 + frac as f64 / 1000.0),
        TemporalField::Epoch => Some(t as f64 / 1_000_000.0),
        _ => None,
    }
}
fn float_value(v: f64) -> Result<Option<crate::Float8>> {
    Ok(Some(crate::Float8::from_bits(v.to_bits())))
}
impl Date {
    pub fn date_part(self, f: TemporalField) -> Result<Option<crate::Float8>> {
        let t = if self == Self::INFINITY {
            Timestamp::INFINITY
        } else if self == Self::NEG_INFINITY {
            Timestamp::NEG_INFINITY
        } else {
            let micros = i64::from(self.0)
                .checked_mul(USECS_PER_DAY)
                .ok_or(TemporalFunctionError::Range)?;
            range(Timestamp::from_microseconds(micros))?
        };
        t.date_part(f)
    }
}
impl Time {
    pub fn date_part(self, f: TemporalField) -> Result<Option<crate::Float8>> {
        if let Some(v) = clock_float(f, self.0) {
            float_value(v)
        } else {
            float_part(self.extract(f)?)
        }
    }
}
impl TimeTz {
    pub fn date_part(self, f: TemporalField) -> Result<Option<crate::Float8>> {
        if f == TemporalField::Epoch {
            return float_value(self.time().0 as f64 / 1_000_000.0 - self.offset_east() as f64);
        }
        if let Some(v) = clock_float(f, self.time().0) {
            float_value(v)
        } else {
            float_part(self.extract(f)?)
        }
    }
}
impl Timestamp {
    pub fn date_part(self, f: TemporalField) -> Result<Option<crate::Float8>> {
        if self == Self::INFINITY || self == Self::NEG_INFINITY {
            return float_part(self.extract(f)?);
        }
        if f == TemporalField::Epoch {
            return float_value(if let Some(n) = self.0.checked_add(946_684_800_000_000) {
                n as f64 / 1_000_000.0
            } else {
                (self.0 as f64 + 946_684_800_000_000.0) / 1_000_000.0
            });
        }
        let t = self.0.rem_euclid(USECS_PER_DAY);
        if f == TemporalField::Julian {
            return float_value(
                (self.0.div_euclid(USECS_PER_DAY) + 2451545) as f64
                    + ((t / 1_000_000) as f64 + (t % 1_000_000) as f64 / 1_000_000.0) / 86400.0,
            );
        }
        if let Some(v) = clock_float(f, t) {
            float_value(v)
        } else {
            float_part(self.extract(f)?)
        }
    }
}
impl TimestampTz {
    pub fn date_part(self, f: TemporalField, z: SessionZone) -> Result<Option<crate::Float8>> {
        if f == TemporalField::Epoch {
            return Timestamp(self.0).date_part(f);
        }
        if matches!(
            f,
            TemporalField::Timezone | TemporalField::TimezoneHour | TemporalField::TimezoneMinute
        ) {
            return float_part(self.extract(f, z)?);
        }
        range(z.to_local(self))?.date_part(f)
    }
}
impl Interval {
    pub fn date_part(self, f: TemporalField) -> Result<Option<crate::Float8>> {
        if self.same_components(Self::INFINITY) || self.same_components(Self::NEG_INFINITY) {
            return float_part(self.extract(f)?);
        }
        if f == TemporalField::Epoch {
            let mut v = self.microseconds as f64 / 1_000_000.0;
            v += 31_557_600.0 * (self.months / 12) as f64;
            v += 2_592_000.0 * (self.months % 12) as f64;
            v += 86400.0 * self.days as f64;
            return float_value(v);
        }
        if let Some(v) = clock_float(f, self.microseconds) {
            float_value(v)
        } else {
            float_part(self.extract(f)?)
        }
    }
}

mod calendar;
pub use calendar::*;

// Interval zones have no calendar component; PostgreSQL uses whole seconds.
fn interval_zone(zone: Interval) -> Result<i32> {
    if zone.months() != 0 || zone.days() != 0 {
        return Err(TemporalFunctionError::Range);
    }
    i32::try_from(zone.microseconds() / 1_000_000).map_err(|_| TemporalFunctionError::Range)
}
impl Timestamp {
    pub fn timezone_interval(self, zone: Interval) -> Result<TimestampTz> {
        if self == Self::INFINITY || self == Self::NEG_INFINITY {
            return range(TimestampTz::from_microseconds(self.microseconds()));
        }
        let seconds = interval_zone(zone)?;
        range(TimestampTz::from_microseconds(
            self.microseconds()
                .checked_sub(i64::from(seconds) * 1_000_000)
                .ok_or(TemporalFunctionError::Range)?,
        ))
    }
}
impl TimestampTz {
    pub fn timezone_interval(self, zone: Interval) -> Result<Timestamp> {
        if self == Self::INFINITY || self == Self::NEG_INFINITY {
            return range(Timestamp::from_microseconds(self.microseconds()));
        }
        let seconds = interval_zone(zone)?;
        range(Timestamp::from_microseconds(
            self.microseconds()
                .checked_add(i64::from(seconds) * 1_000_000)
                .ok_or(TemporalFunctionError::Range)?,
        ))
    }
}
impl TimeTz {
    pub fn timezone_interval(self, zone: Interval) -> Result<Self> {
        let seconds = interval_zone(zone)?;
        let time = (self.time().microseconds()
            + (i64::from(seconds) - i64::from(self.offset_east())) * 1_000_000)
            .rem_euclid(USECS_PER_DAY);
        range(Self::new(range(Time::from_microseconds(time))?, seconds))
    }
}
