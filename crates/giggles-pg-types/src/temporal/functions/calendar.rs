//! Calendar algorithms follow PostgreSQL REL_18_0 timestamp.c/date.c.
//! PostgreSQL source copyright: PostgreSQL Global Development Group;
//! see https://www.postgresql.org/about/licence/ .
use super::*;
fn bad<T>() -> Result<T> {
    Err(TemporalFunctionError::Range)
}
fn finite(i: Interval) -> bool {
    !i.same_components(Interval::INFINITY) && !i.same_components(Interval::NEG_INFINITY)
}
fn rounded_seconds(s: f64) -> Result<i64> {
    let v = (s * 1_000_000.0).round_ties_even();
    if !v.is_finite() || !(-9223372036854775808.0..9223372036854775808.0).contains(&v) {
        return bad();
    }
    Ok(v as i64)
}
/// SQL float8 seconds, rounded like PostgreSQL rint; the exact API is unchanged.
pub fn make_time_float8(hour: i32, minute: i32, seconds: f64) -> Result<Time> {
    let s = rounded_seconds(seconds)?;
    if !(0..=24).contains(&hour) || !(0..60).contains(&minute) || !(0..=60_000_000).contains(&s) {
        return bad();
    }
    range(Time::from_microseconds(
        i64::from(hour) * 3_600_000_000 + i64::from(minute) * 60_000_000 + s,
    ))
}
pub fn make_timestamp_float8(
    year: i32,
    month: u32,
    day: u32,
    hour: i32,
    minute: i32,
    seconds: f64,
) -> Result<Timestamp> {
    let d = make_date(year, month, day)?;
    let t = make_time_float8(hour, minute, seconds)?;
    let v = i128::from(d.days()) * i128::from(USECS_PER_DAY) + i128::from(t.microseconds());
    range(Timestamp::from_microseconds(
        i64::try_from(v).map_err(|_| TemporalFunctionError::Range)?,
    ))
}
#[allow(clippy::too_many_arguments)]
pub fn make_interval(
    years: i32,
    months: i32,
    weeks: i32,
    days: i32,
    hours: i32,
    minutes: i32,
    seconds: f64,
) -> Result<Interval> {
    if !seconds.is_finite() {
        return bad();
    }
    let m = years
        .checked_mul(12)
        .and_then(|v| v.checked_add(months))
        .ok_or(TemporalFunctionError::Range)?;
    let d = weeks
        .checked_mul(7)
        .and_then(|v| v.checked_add(days))
        .ok_or(TemporalFunctionError::Range)?;
    if !(seconds * 1_000_000.0).is_finite() {
        return Err(TemporalFunctionError::NumericOverflow);
    }
    let t = (i64::from(hours) * 3_600_000_000 + i64::from(minutes) * 60_000_000)
        .checked_add(rounded_seconds(seconds)?)
        .ok_or(TemporalFunctionError::Range)?;
    let i = Interval::new(m, d, t);
    if !finite(i) {
        return bad();
    }
    Ok(i)
}
impl Interval {
    pub fn justify_hours(self) -> Result<Self> {
        if !finite(self) {
            return Ok(self);
        }
        let mut d = i32::try_from(i64::from(self.days) + self.microseconds / USECS_PER_DAY)
            .map_err(|_| TemporalFunctionError::Range)?;
        let mut t = self.microseconds % USECS_PER_DAY;
        if d > 0 && t < 0 {
            d -= 1;
            t += USECS_PER_DAY;
        } else if d < 0 && t > 0 {
            d += 1;
            t -= USECS_PER_DAY;
        }
        Ok(Self::new(self.months, d, t))
    }
    pub fn justify_days(self) -> Result<Self> {
        if !finite(self) {
            return Ok(self);
        }
        let mut m = self
            .months
            .checked_add(self.days / 30)
            .ok_or(TemporalFunctionError::Range)?;
        let mut d = self.days % 30;
        if m > 0 && d < 0 {
            m -= 1;
            d += 30;
        } else if m < 0 && d > 0 {
            m += 1;
            d -= 30;
        }
        Ok(Self::new(m, d, self.microseconds))
    }
    pub fn checked_neg(self) -> Result<Self> {
        if self.same_components(Self::INFINITY) {
            return Ok(Self::NEG_INFINITY);
        }
        if self.same_components(Self::NEG_INFINITY) {
            return Ok(Self::INFINITY);
        }
        finite_result(Self::new(
            self.months
                .checked_neg()
                .ok_or(TemporalFunctionError::Range)?,
            self.days
                .checked_neg()
                .ok_or(TemporalFunctionError::Range)?,
            self.microseconds
                .checked_neg()
                .ok_or(TemporalFunctionError::Range)?,
        ))
    }
}
fn finite_result(i: Interval) -> Result<Interval> {
    if finite(i) { Ok(i) } else { bad() }
}
impl Interval {
    fn negate(self) -> Result<Self> {
        self.checked_neg()
    }
    pub fn checked_add(self, o: Self) -> Result<Self> {
        if !finite(self) || !finite(o) {
            if !finite(self) && !finite(o) && !self.same_components(o) {
                return bad();
            }
            return Ok(if !finite(self) { self } else { o });
        }
        finite_result(Self::new(
            self.months
                .checked_add(o.months)
                .ok_or(TemporalFunctionError::Range)?,
            self.days
                .checked_add(o.days)
                .ok_or(TemporalFunctionError::Range)?,
            self.microseconds
                .checked_add(o.microseconds)
                .ok_or(TemporalFunctionError::Range)?,
        ))
    }
    pub fn checked_sub(self, o: Self) -> Result<Self> {
        if !finite(self) || !finite(o) {
            if !finite(self) && !finite(o) && self.same_components(o) {
                return bad();
            }
            return if !finite(self) {
                Ok(self)
            } else {
                o.checked_neg()
            };
        }
        finite_result(Self::new(
            self.months
                .checked_sub(o.months)
                .ok_or(TemporalFunctionError::Range)?,
            self.days
                .checked_sub(o.days)
                .ok_or(TemporalFunctionError::Range)?,
            self.microseconds
                .checked_sub(o.microseconds)
                .ok_or(TemporalFunctionError::Range)?,
        ))
    }
    pub fn mul_f64(self, factor: f64) -> Result<Self> {
        self.scale_f64(factor, false)
    }
    pub fn div_f64(self, factor: f64) -> Result<Self> {
        self.scale_f64(factor, true)
    }
    fn scale_f64(self, factor: f64, divide: bool) -> Result<Self> {
        if divide && factor == 0.0 {
            return Err(TemporalFunctionError::DivisionByZero);
        }
        if factor.is_nan() {
            return bad();
        }
        if !finite(self) {
            if (divide && factor.is_infinite()) || (!divide && factor == 0.0) {
                return bad();
            }
            return if factor < 0.0 {
                self.checked_neg()
            } else {
                Ok(self)
            };
        }
        if factor.is_infinite() {
            if divide {
                return Ok(Self::new(0, 0, 0));
            }
            // PG orders intervals by their 30-day-month total, and that total's sign is the sign here.
            let total = (i128::from(self.months) * 30 + i128::from(self.days))
                * i128::from(USECS_PER_DAY)
                + i128::from(self.microseconds);
            let sign = total.signum();
            if sign == 0 {
                return bad();
            }
            return Ok(if (sign < 0) == factor.is_sign_negative() {
                Self::INFINITY
            } else {
                Self::NEG_INFINITY
            });
        }
        // PostgreSQL computes each quotient directly, preserving float8 rounding.
        let op = |v: f64| if divide { v / factor } else { v * factor };
        let month_value = op(f64::from(self.months));
        let day_value = op(f64::from(self.days));
        if !(-2147483648.0..2147483648.0).contains(&month_value)
            || !(-2147483648.0..2147483648.0).contains(&day_value)
        {
            return bad();
        }
        let month = month_value as i32;
        let mut day = day_value as i32;
        let tsround = |v: f64| (v * 1_000_000.0).round_ties_even() / 1_000_000.0;
        let month_remainder = tsround((month_value - f64::from(month)) * 30.0);
        let mut seconds_remainder = tsround(
            (day_value - f64::from(day) + month_remainder - f64::from(month_remainder as i32))
                * 86400.0,
        );
        if seconds_remainder.abs() >= 86400.0 {
            let extra_days = (seconds_remainder / 86400.0) as i32;
            day = day
                .checked_add(extra_days)
                .ok_or(TemporalFunctionError::Range)?;
            seconds_remainder -= f64::from(extra_days) * 86400.0;
        }
        day = day
            .checked_add(month_remainder as i32)
            .ok_or(TemporalFunctionError::Range)?;
        let time =
            (op(self.microseconds as f64) + seconds_remainder * 1_000_000.0).round_ties_even();
        if !(-9223372036854775808.0..9223372036854775808.0).contains(&time) {
            return bad();
        }
        finite_result(Self::new(month, day, time as i64))
    }
}

fn infinite_add(t: Timestamp, i: Interval) -> Result<Option<Timestamp>> {
    if !finite(i) {
        let v = if i.same_components(Interval::INFINITY) {
            Timestamp::INFINITY
        } else {
            Timestamp::NEG_INFINITY
        };
        if (t == Timestamp::INFINITY || t == Timestamp::NEG_INFINITY) && t != v {
            return bad();
        }
        return Ok(Some(v));
    }
    Ok(
        if t == Timestamp::INFINITY || t == Timestamp::NEG_INFINITY {
            Some(t)
        } else {
            None
        },
    )
}
fn month_length(y: i64, m: i64) -> i64 {
    let (ny, nm) = if m == 12 { (y + 1, 1) } else { (y, m + 1) };
    civil_days(ny, nm, 1) - civil_days(y, m, 1)
}
impl Timestamp {
    pub fn add_interval(self, i: Interval) -> Result<Self> {
        if let Some(v) = infinite_add(self, i)? {
            return Ok(v);
        }
        let mut t = self;
        if i.months != 0 {
            let (y, m, d) = civil(t.0.div_euclid(USECS_PER_DAY));
            // PG checks the intermediate int32 month before normalization.
            i32::try_from(m + i64::from(i.months)).map_err(|_| TemporalFunctionError::Range)?;
            let n = y * 12 + m - 1 + i64::from(i.months);
            let (y, m) = (n.div_euclid(12), n.rem_euclid(12) + 1);
            let day = civil_days(y, m, d.min(month_length(y, m)));
            t = range(Self::from_microseconds(
                i64::try_from(
                    i128::from(day) * i128::from(USECS_PER_DAY)
                        + i128::from(t.0.rem_euclid(USECS_PER_DAY)),
                )
                .map_err(|_| TemporalFunctionError::Range)?,
            ))?;
        }
        let day = t.0.div_euclid(USECS_PER_DAY) + i64::from(i.days);
        if !(0..=i64::from(i32::MAX)).contains(&(day + 2451545)) {
            return bad();
        }
        let v = i128::from(t.0) + i128::from(i.days) * i128::from(USECS_PER_DAY);
        let v = i64::try_from(v).map_err(|_| TemporalFunctionError::Range)?;
        let t = range(Self::from_microseconds(v))?;
        range(Self::from_microseconds(
            t.0.checked_add(i.microseconds)
                .ok_or(TemporalFunctionError::Range)?,
        ))
    }
    pub fn sub_interval(self, i: Interval) -> Result<Self> {
        self.add_interval(i.negate()?)
    }
    pub fn age(self, other: Self) -> Result<Interval> {
        age(self, other, self < other)
    }
}
fn age(a: Timestamp, b: Timestamp, negative: bool) -> Result<Interval> {
    if a == Timestamp::INFINITY
        || a == Timestamp::NEG_INFINITY
        || b == Timestamp::INFINITY
        || b == Timestamp::NEG_INFINITY
    {
        if a == b {
            return bad();
        }
        return Ok(
            if a == Timestamp::INFINITY || b == Timestamp::NEG_INFINITY {
                Interval::INFINITY
            } else {
                Interval::NEG_INFINITY
            },
        );
    }
    let (ay, am, ad) = civil(a.0.div_euclid(USECS_PER_DAY));
    let (by, bm, bd) = civil(b.0.div_euclid(USECS_PER_DAY));
    let sign = if negative { -1 } else { 1 };
    let mut m = (ay - by) * 12 + am - bm;
    let mut d = ad - bd;
    let mut t = a.0.rem_euclid(USECS_PER_DAY) - b.0.rem_euclid(USECS_PER_DAY);
    m *= sign;
    d *= sign;
    t *= sign;
    if t < 0 {
        t += USECS_PER_DAY;
        d -= 1;
    }
    while d < 0 {
        d += if negative {
            month_length(ay, am)
        } else {
            month_length(by, bm)
        };
        m -= 1;
    }
    Ok(Interval::new(
        i32::try_from(m * sign).map_err(|_| TemporalFunctionError::Range)?,
        i32::try_from(d * sign).map_err(|_| TemporalFunctionError::Range)?,
        t * sign,
    ))
}
impl Date {
    fn timestamp(self) -> Result<Timestamp> {
        if self == Self::INFINITY {
            return Ok(Timestamp::INFINITY);
        }
        if self == Self::NEG_INFINITY {
            return Ok(Timestamp::NEG_INFINITY);
        }
        range(Timestamp::from_microseconds(
            i64::from(self.0)
                .checked_mul(USECS_PER_DAY)
                .ok_or(TemporalFunctionError::Range)?,
        ))
    }
    pub fn add_interval(self, i: Interval) -> Result<Timestamp> {
        self.timestamp()?.add_interval(i)
    }
    pub fn sub_interval(self, i: Interval) -> Result<Timestamp> {
        self.timestamp()?.sub_interval(i)
    }
}
impl TimestampTz {
    pub fn add_interval(self, i: Interval, z: SessionZone) -> Result<Self> {
        if let Some(v) = infinite_add(Timestamp(self.0), i)? {
            return Ok(Self(v.0));
        }
        let mut t = self;
        for span in [Interval::new(i.months, 0, 0), Interval::new(0, i.days, 0)] {
            if span.months != 0 || span.days != 0 {
                t = range(z.to_instant(range(z.to_local(t))?.add_interval(span)?))?;
            }
        }
        range(Self::from_microseconds(
            t.0.checked_add(i.microseconds)
                .ok_or(TemporalFunctionError::Range)?,
        ))
    }
    pub fn sub_interval(self, i: Interval, z: SessionZone) -> Result<Self> {
        self.add_interval(i.negate()?, z)
    }
    pub fn age(self, other: Self, z: SessionZone) -> Result<Interval> {
        age(
            range(z.to_local(self))?,
            range(z.to_local(other))?,
            self < other,
        )
    }
}
impl std::str::FromStr for TemporalField {
    type Err = TemporalFunctionError;
    fn from_str(s: &str) -> Result<Self> {
        use TemporalField::*;
        // DecodeUnits/DecodeSpecial use TOKMAXLEN (10) byte comparisons.
        if !s.is_ascii() {
            return Err(TemporalFunctionError::UnsupportedField);
        }
        let s = s.to_ascii_lowercase();
        let s = &s[..s.len().min(10)];
        Ok(match s {
            "microsecon" | "us" | "usec" | "useconds" | "usecs" => Microseconds,
            "millisecon" | "ms" | "msec" | "mseconds" | "msecs" => Milliseconds,
            "s" | "sec" | "second" | "seconds" | "secs" => Second,
            "m" | "min" | "mins" | "minute" | "minutes" => Minute,
            "h" | "hour" | "hours" | "hr" | "hrs" => Hour,
            "d" | "day" | "days" => Day,
            "mon" | "mons" | "month" | "months" => Month,
            "qtr" | "quarter" => Quarter,
            "w" | "week" | "weeks" => Week,
            "y" | "year" | "years" | "yr" | "yrs" => Year,
            "dec" | "decade" | "decades" | "decs" => Decade,
            "c" | "cent" | "centuries" | "century" => Century,
            "mil" | "millennia" | "millennium" | "mils" => Millennium,
            "isoyear" => IsoYear,
            "dow" => Dow,
            "isodow" => IsoDow,
            "doy" => Doy,
            "j" | "jd" | "julian" => Julian,
            "epoch" => Epoch,
            "timezone" => Timezone,
            "timezone_h" => TimezoneHour,
            "timezone_m" => TimezoneMinute,
            _ => return Err(TemporalFunctionError::UnsupportedField),
        })
    }
}

impl Timestamp {
    pub fn diff(self, o: Self) -> Result<Interval> {
        if self == Self::INFINITY
            || self == Self::NEG_INFINITY
            || o == Self::INFINITY
            || o == Self::NEG_INFINITY
        {
            if self == o {
                return bad();
            }
            return Ok(if self == Self::INFINITY || o == Self::NEG_INFINITY {
                Interval::INFINITY
            } else {
                Interval::NEG_INFINITY
            });
        }
        Interval::new(
            0,
            0,
            self.0
                .checked_sub(o.0)
                .ok_or(TemporalFunctionError::Range)?,
        )
        .justify_hours()
    }
}
impl TimestampTz {
    pub fn diff(self, o: Self) -> Result<Interval> {
        Timestamp(self.0).diff(Timestamp(o.0))
    }
}
impl Date {
    pub fn add_days(self, days: i64) -> Result<Self> {
        if self == Self::INFINITY || self == Self::NEG_INFINITY {
            return Ok(self);
        }
        let days = i32::try_from(days).map_err(|_| TemporalFunctionError::Range)?;
        let v = self
            .0
            .checked_add(days)
            .ok_or(TemporalFunctionError::Range)?;
        if v == i32::MIN || v == i32::MAX {
            return bad();
        }
        range(Self::from_days(v))
    }
    pub fn sub_days(self, days: i64) -> Result<Self> {
        self.add_days(days.checked_neg().ok_or(TemporalFunctionError::Range)?)
    }
    pub fn diff(self, o: Self) -> Result<i32> {
        if self == Self::INFINITY
            || self == Self::NEG_INFINITY
            || o == Self::INFINITY
            || o == Self::NEG_INFINITY
        {
            return bad();
        }
        self.0.checked_sub(o.0).ok_or(TemporalFunctionError::Range)
    }
    pub fn add_time(self, t: Time) -> Result<Timestamp> {
        let base = self.timestamp()?;
        if base == Timestamp::INFINITY || base == Timestamp::NEG_INFINITY {
            return Ok(base);
        }
        let v = base
            .0
            .checked_add(t.0)
            .ok_or(TemporalFunctionError::Range)?;
        finite_timestamp(v).map(Timestamp)
    }
    pub fn add_timetz(self, t: TimeTz) -> Result<TimestampTz> {
        if self == Self::INFINITY {
            return Ok(TimestampTz::INFINITY);
        }
        if self == Self::NEG_INFINITY {
            return Ok(TimestampTz::NEG_INFINITY);
        }
        let v = i128::from(self.0) * i128::from(USECS_PER_DAY)
            + i128::from(t.time().microseconds())
            - i128::from(t.offset_east()) * 1_000_000;
        let v = i64::try_from(v).map_err(|_| TemporalFunctionError::Range)?;
        finite_timestamp(v).map(TimestampTz)
    }
}
fn finite_timestamp(v: i64) -> Result<i64> {
    if !(MIN_TIMESTAMP..END_TIMESTAMP).contains(&v) {
        return bad();
    }
    Ok(v)
}
impl Time {
    pub fn add_interval(self, i: Interval) -> Result<Self> {
        self.shift_interval(i, false)
    }
    pub fn sub_interval(self, i: Interval) -> Result<Self> {
        self.shift_interval(i, true)
    }
    fn shift_interval(self, i: Interval, subtract: bool) -> Result<Self> {
        if !finite(i) {
            return bad();
        }
        let delta = i128::from(i.microseconds) * if subtract { -1 } else { 1 };
        let v = (i128::from(self.0) + delta).rem_euclid(i128::from(USECS_PER_DAY));
        Ok(Self(v as i64))
    }
    pub fn diff(self, o: Self) -> Interval {
        Interval::new(0, 0, self.0 - o.0)
    }
    pub fn to_interval(self) -> Interval {
        Interval::new(0, 0, self.0)
    }
}
impl TimeTz {
    pub fn add_interval(self, i: Interval) -> Result<Self> {
        range(Self::new(self.time().add_interval(i)?, self.offset_east()))
    }
    pub fn sub_interval(self, i: Interval) -> Result<Self> {
        range(Self::new(self.time().sub_interval(i)?, self.offset_east()))
    }
}
