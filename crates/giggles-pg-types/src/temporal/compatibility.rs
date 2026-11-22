use super::*;
// Bounded decimal rational: no floating-point conversion or silent precision loss.
fn decimal(s: &str) -> Result<(i128, i128), Error> {
    let negative = s.starts_with('-');
    let s = s.strip_prefix(['+', '-']).unwrap_or(s);
    let (whole, frac) = s.split_once('.').unwrap_or((s, ""));
    if whole.is_empty()
        || !whole
            .bytes()
            .chain(frac.bytes())
            .all(|b| b.is_ascii_digit())
    {
        return Err(Error::Syntax);
    }
    if frac.trim_end_matches('0').len() > 6 || frac.len() > 18 {
        return Err(Error::Unimplemented);
    }
    let den = 10i128.pow(frac.len() as u32);
    let whole: i128 = whole.parse().map_err(|_| Error::Range)?;
    let frac: i128 = if frac.is_empty() {
        0
    } else {
        frac.parse().map_err(|_| Error::Range)?
    };
    let n = whole
        .checked_mul(den)
        .and_then(|v| v.checked_add(frac))
        .ok_or(Error::Range)?;
    Ok((if negative { -n } else { n }, den))
}
fn even(n: i128, d: i128) -> i128 {
    let q = n / d;
    let r = (n % d).abs();
    q + if r * 2 > d || (r * 2 == d && q % 2 != 0) {
        n.signum()
    } else {
        0
    }
}
impl Interval {
    pub(super) fn unit(text: &str, unit: &str) -> Result<Self, Error> {
        // DecodeInterval parses the fractional seconds separately, then truncates ties in
        // fractional microseconds toward zero. Keep the integer part exact even above 2^53.
        if matches!(unit, "second" | "seconds" | "sec" | "secs") {
            let unsigned = text.strip_prefix(['+', '-']).unwrap_or(text);
            if let Some((whole, frac)) = unsigned.split_once('.')
                && frac.trim_end_matches('0').len() > 6
            {
                if whole.is_empty()
                    || frac.len() > 18
                    || !whole
                        .bytes()
                        .chain(frac.bytes())
                        .all(|b| b.is_ascii_digit())
                {
                    return Err(Error::Syntax);
                }
                let whole: i64 = whole.parse().map_err(|_| Error::Range)?;
                let fraction: f64 = format!("0.{frac}").parse().map_err(|_| Error::Syntax)?;
                let scaled = fraction * 1_000_000.0;
                let mut micro = scaled as i64;
                if scaled - micro as f64 > 0.5 {
                    micro += 1;
                }
                let value = whole
                    .checked_mul(1_000_000)
                    .and_then(|v| v.checked_add(micro))
                    .ok_or(Error::Range)?;
                return Ok(Self::new(
                    0,
                    0,
                    if text.starts_with('-') {
                        value.checked_neg().ok_or(Error::Range)?
                    } else {
                        value
                    },
                ));
            }
        }
        let (n, d) = decimal(text)?;
        let mut months = 0i128;
        let mut days = 0i128;
        let mut us = 0i128;
        let multiply = |scale| n.checked_mul(scale).ok_or(Error::Range);
        match unit {
            "year" | "years" => months = even(multiply(12)?, d),
            // AdjustFractYears: rint(fraction * scale * 12) months.
            "decades" => months = even(multiply(120)?, d),
            "centuries" => months = even(multiply(1_200)?, d),
            "millennia" => months = even(multiply(12_000)?, d),
            "mon" | "mons" | "month" | "months" => {
                months = n / d;
                let daynum = (n % d) * 30;
                days = daynum / d;
                let micro = (daynum % d) * i128::from(USECS_PER_DAY);
                if micro % d != 0 {
                    return Err(Error::Unimplemented);
                }
                us = micro / d;
            }
            "week" | "weeks" | "day" | "days" => {
                let n = multiply(if unit.starts_with("week") { 7 } else { 1 })?;
                days = n / d;
                let micro = (n % d) * i128::from(USECS_PER_DAY);
                if micro % d != 0 {
                    return Err(Error::Unimplemented);
                }
                us = micro / d;
            }
            _ => {
                let scale = match unit {
                    "hour" | "hours" => 3_600_000_000,
                    "minute" | "minutes" | "min" | "mins" => 60_000_000,
                    "second" | "seconds" | "sec" | "secs" => 1_000_000,
                    "millisecond" | "milliseconds" => 1_000,
                    "microsecond" | "microseconds" => 1,
                    _ => return Err(Error::Unimplemented),
                };
                let micro = multiply(scale)?;
                // PG's binary-double fractional-unit rounding is not exact decimal rounding.
                // Reject submicrosecond results until that conversion is qualified.
                if micro % d != 0 {
                    return Err(Error::Unimplemented);
                }
                us = micro / d;
            }
        }
        Ok(Self::new(
            i32::try_from(months).map_err(|_| Error::Range)?,
            i32::try_from(days).map_err(|_| Error::Range)?,
            i64::try_from(us).map_err(|_| Error::Range)?,
        ))
    }
    pub(super) fn parse_iso(s: &str) -> Result<Self, Error> {
        // Complete unsigned integer alternative forms. Translate components, not
        // a calendar date: ISO interval months/days need not fit a calendar.
        let body = &s[1..];
        if !body.is_empty()
            && body
                .bytes()
                .all(|b| b.is_ascii_digit() || matches!(b, b'-' | b':' | b'T'))
        {
            let (date, time) = body
                .split_once('T')
                .map_or((body, None), |(d, t)| (d, Some(t)));
            fn parts(s: &str, separator: char, width: usize) -> Result<Vec<&str>, Error> {
                if s.contains(separator) {
                    let p: Vec<_> = s.split(separator).collect();
                    if p.len() != 3 || p.iter().any(|v| v.is_empty()) {
                        return Err(Error::Syntax);
                    }
                    Ok(p)
                } else if s.len() == width {
                    Ok(vec![
                        &s[..width - 4],
                        &s[width - 4..width - 2],
                        &s[width - 2..],
                    ])
                } else {
                    Err(Error::Unimplemented)
                }
            }
            let mut normalized = String::from("P");
            if !date.is_empty() {
                for (v, u) in parts(date, '-', 8)?.iter().zip(['Y', 'M', 'D']) {
                    normalized += v;
                    normalized.push(u);
                }
            }
            if let Some(time) = time {
                normalized.push('T');
                for (v, u) in parts(time, ':', 6)?.iter().zip(['H', 'M', 'S']) {
                    normalized += v;
                    normalized.push(u);
                }
            }
            return Self::parse_iso(&normalized);
        }
        let mut rest = &s[1..];
        let mut time = false;
        let mut result = Self::new(0, 0, 0);
        let mut count = 0;
        while !rest.is_empty() {
            if let Some(r) = rest.strip_prefix('T') {
                if time {
                    return Err(Error::Syntax);
                }
                time = true;
                rest = r;
                if rest.is_empty() {
                    return Err(Error::Syntax);
                }
                continue;
            }
            let end = rest
                .find(|c: char| c.is_ascii_alphabetic())
                .ok_or(Error::Syntax)?;
            let unit = match (time, rest.as_bytes()[end]) {
                (false, b'Y') => "years",
                (false, b'M') => "months",
                (false, b'W') => "weeks",
                (false, b'D') => "days",
                (true, b'H') => "hours",
                (true, b'M') => "minutes",
                (true, b'S') => "seconds",
                _ => return Err(Error::Syntax),
            };
            let v = Self::unit(&rest[..end], unit)?;
            result.months = result.months.checked_add(v.months).ok_or(Error::Range)?;
            result.days = result.days.checked_add(v.days).ok_or(Error::Range)?;
            result.microseconds = result
                .microseconds
                .checked_add(v.microseconds)
                .ok_or(Error::Range)?;
            rest = &rest[end + 1..];
            count += 1;
        }
        if count == 0 {
            return Err(Error::Syntax);
        }
        Ok(result)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DateStyle {
    Iso,
    Sql,
    Postgres,
    German,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DateStyleSettings {
    pub style: DateStyle,
    pub order: DateOrder,
}

impl Default for DateStyleSettings {
    fn default() -> Self {
        Self { style: DateStyle::Iso, order: DateOrder::Mdy }
    }
}

impl std::fmt::Display for DateStyleSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let style = match self.style {
            DateStyle::Iso => "ISO",
            DateStyle::Sql => "SQL",
            DateStyle::Postgres => "Postgres",
            DateStyle::German => "German",
        };
        let order = match self.order {
            DateOrder::Mdy => "MDY",
            DateOrder::Dmy => "DMY",
            DateOrder::Ymd => "YMD",
        };
        write!(f, "{style}, {order}")
    }
}
impl Date {
    pub fn format_with_style(self, style: DateStyle, order: DateOrder) -> String {
        if self == Self::INFINITY || self == Self::NEG_INFINITY {
            return self.to_string();
        }
        let (y, m, d) = civil(i64::from(self.0));
        let year = if y <= 0 { 1 - y } else { y };
        let bc = if y <= 0 { " BC" } else { "" };
        match style {
            DateStyle::Iso => self.to_string(),
            DateStyle::German => format!("{d:02}.{m:02}.{year:04}{bc}"),
            DateStyle::Sql if order == DateOrder::Dmy => format!("{d:02}/{m:02}/{year:04}{bc}"),
            DateStyle::Sql => format!("{m:02}/{d:02}/{year:04}{bc}"),
            DateStyle::Postgres if order == DateOrder::Dmy => {
                format!("{d:02}-{m:02}-{year:04}{bc}")
            }
            DateStyle::Postgres => format!("{m:02}-{d:02}-{year:04}{bc}"),
        }
    }
}
impl Timestamp {
    pub fn format_with_style(self, style: DateStyle, order: DateOrder) -> String {
        if style == DateStyle::Iso || self == Self::INFINITY || self == Self::NEG_INFINITY {
            return self.to_string();
        }
        let day = self.0.div_euclid(USECS_PER_DAY);
        let date = Date(day as i32);
        let clock = Time(self.0.rem_euclid(USECS_PER_DAY));
        let (y, m, d) = civil(day);
        let bc = if y <= 0 { " BC" } else { "" };
        if style == DateStyle::Postgres {
            let weekday =
                ["Sat", "Sun", "Mon", "Tue", "Wed", "Thu", "Fri"][day.rem_euclid(7) as usize];
            let month = [
                "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
            ][(m - 1) as usize];
            let year = if y <= 0 { 1 - y } else { y };
            if order == DateOrder::Dmy {
                format!("{weekday} {d:02} {month} {clock} {year:04}{bc}")
            } else {
                format!("{weekday} {month} {d:02} {clock} {year:04}{bc}")
            }
        } else {
            let text = date.format_with_style(style, order);
            format!("{} {clock}{bc}", text.strip_suffix(" BC").unwrap_or(&text))
        }
    }
}
impl TimeTz {
    /// PostgreSQL timetz output is independent of DateStyle and date order.
    pub fn format_with_style(self, _style: DateStyle, _order: DateOrder) -> String {
        self.to_string()
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IntervalOutputStyle {
    Postgres,
    Iso8601,
    SqlStandard,
    PostgresVerbose,
}
impl Interval {
    pub fn format_with_style(self, style: IntervalOutputStyle) -> String {
        if style == IntervalOutputStyle::Postgres
            || self.same_components(Self::INFINITY)
            || self.same_components(Self::NEG_INFINITY)
        {
            return self.to_string();
        }
        let y = i64::from(self.months) / 12;
        let m = i64::from(self.months) % 12;
        let d = i64::from(self.days);
        let us = i128::from(self.microseconds);
        let h = us / 3_600_000_000;
        let min = (us / 60_000_000) % 60;
        let sec = us % 60_000_000;
        fn seconds(v: i128) -> String {
            let n = v.abs();
            let mut s = format!("{}.{:06}", n / 1_000_000, n % 1_000_000);
            while s.ends_with('0') {
                s.pop();
            }
            if s.ends_with('.') {
                s.pop();
            }
            if v < 0 {
                s.insert(0, '-');
            }
            s
        }
        match style {
            IntervalOutputStyle::Iso8601 => {
                let mut s = String::from("P");
                for (n, u) in [
                    (i128::from(y), "Y"),
                    (i128::from(m), "M"),
                    (i128::from(d), "D"),
                ] {
                    if n != 0 {
                        s += &format!("{n}{u}");
                    }
                }
                if us != 0 {
                    s.push('T');
                    if h != 0 {
                        s += &format!("{h}H");
                    }
                    if min != 0 {
                        s += &format!("{min}M");
                    }
                    if sec != 0 {
                        s += &format!("{}S", seconds(sec));
                    }
                }
                if s == "P" {
                    s = "PT0S".into();
                }
                s
            }
            IntervalOutputStyle::PostgresVerbose => {
                let first = [i128::from(y), i128::from(m), i128::from(d), h, min, sec]
                    .into_iter()
                    .find(|v| *v != 0)
                    .unwrap_or(0);
                let factor = if first < 0 { -1 } else { 1 };
                let mut s = String::from("@");
                for (n, u) in [
                    (i128::from(y), "year"),
                    (i128::from(m), "mon"),
                    (i128::from(d), "day"),
                    (h, "hour"),
                    (min, "min"),
                ] {
                    let n = n * factor;
                    if n != 0 {
                        s += &format!(" {n} {u}{}", if n == 1 { "" } else { "s" });
                    }
                }
                if sec != 0 {
                    let n = sec * factor;
                    s += &format!(
                        " {} sec{}",
                        seconds(n),
                        if n == 1_000_000 { "" } else { "s" }
                    );
                }
                if s == "@" {
                    s += " 0";
                }
                if first < 0 {
                    s += " ago";
                }
                s
            }
            IntervalOutputStyle::SqlStandard => {
                let fields = [i128::from(self.months), i128::from(d), us];
                let mixed = (fields.iter().any(|v| *v < 0) && fields.iter().any(|v| *v > 0))
                    || (self.months != 0 && (d != 0 || us != 0));
                let sign = |v: i128| {
                    if v < 0 {
                        "-"
                    } else if mixed {
                        "+"
                    } else {
                        ""
                    }
                };
                let month = format!("{}{}-{}", sign(i128::from(self.months)), y.abs(), m.abs());
                let abs = us.abs();
                let mut clock = format!(
                    "{}{}:{:02}:{:02}",
                    sign(us),
                    abs / 3_600_000_000,
                    (abs / 60_000_000) % 60,
                    (abs / 1_000_000) % 60
                );
                if abs % 1_000_000 != 0 {
                    let fraction = format!("{:06}", abs % 1_000_000);
                    clock.push('.');
                    clock += fraction.trim_end_matches('0');
                }
                if mixed {
                    format!("{month} {}{} {clock}", sign(i128::from(d)), d.abs())
                } else if self.months != 0 && d == 0 && us == 0 {
                    month
                } else if self.months != 0 {
                    format!("{month} {}{} {clock}", sign(i128::from(d)), d.abs())
                } else if d != 0 {
                    format!(
                        "{}{} {}",
                        sign(i128::from(d)),
                        d.abs(),
                        if !mixed && d < 0 {
                            clock.trim_start_matches('-')
                        } else {
                            &clock
                        }
                    )
                } else if us != 0 {
                    clock
                } else {
                    "0".into()
                }
            }
            IntervalOutputStyle::Postgres => unreachable!(),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IntervalField {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}
impl Interval {
    /// Restrict trailing fields as PostgreSQL interval typmods do. Leading components remain.
    pub fn with_fields(self, last: IntervalField, precision: u32) -> Result<Self, Error> {
        if precision > 6 {
            return Err(Error::Range);
        }
        if self.same_components(Self::INFINITY) || self.same_components(Self::NEG_INFINITY) {
            return Ok(self);
        }
        let mut v = self;
        match last {
            IntervalField::Year => {
                v.months = v.months / 12 * 12;
                v.days = 0;
                v.microseconds = 0;
            }
            IntervalField::Month => {
                v.days = 0;
                v.microseconds = 0;
            }
            IntervalField::Day => v.microseconds = 0,
            IntervalField::Hour => v.microseconds = v.microseconds / 3_600_000_000 * 3_600_000_000,
            IntervalField::Minute => v.microseconds = v.microseconds / 60_000_000 * 60_000_000,
            IntervalField::Second => return v.with_precision(precision),
        }
        Ok(v)
    }
}
