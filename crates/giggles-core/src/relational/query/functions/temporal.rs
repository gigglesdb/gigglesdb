//! Deterministic SQL temporal functions. No host clock or mutable session zone.
//! Zone-sensitive operations currently use the explicit core default UTC contract.
use super::*;
use giggles_pg_types::temporal::{SessionZone, TemporalField as F};
pub(super) fn signature(
    n: &str,
    a: &[BoundScalar],
) -> Option<R<(Vec<Option<DataType>>, DataType)>> {
    let types: Vec<_> = a.iter().map(|a| Some(a.ty.clone())).collect();
    let constructor = match (n, a.len()) {
        ("make_time", 3) => Some((2, DataType::Time)),
        ("make_timestamp", 6) => Some((5, DataType::Timestamp)),
        ("make_timestamptz", 6 | 7) => Some((5, DataType::TimestampTz)),
        ("make_interval", 0..=7) => Some((6, DataType::Interval)),
        _ => None,
    };
    if let Some((seconds, result)) = constructor {
        let types = a
            .iter()
            .enumerate()
            .map(|(i, v)| {
                Some(if i == seconds && number(&v.ty) {
                    v.ty.clone()
                } else if i == seconds {
                    DataType::DoublePrecision
                } else if i == 6 && n == "make_timestamptz" {
                    DataType::Text
                } else {
                    DataType::Integer
                })
            })
            .collect();
        return Some(Ok((types, result)));
    }
    Some(match (n, a.len()) {
        // These PostgreSQL overloads require context this explicit-input profile lacks.
        ("age", 1) if matches!(a[0].ty, DataType::Timestamp | DataType::TimestampTz) => {
            Err(SqlError::Unsupported)
        }
        ("timezone", 2) if a[0].ty == DataType::Text && a[1].ty == DataType::TimeTz => {
            Err(SqlError::Unsupported)
        }
        ("isfinite", 1)
            if matches!(
                a[0].ty,
                DataType::Date | DataType::Timestamp | DataType::TimestampTz | DataType::Interval
            ) =>
        {
            Ok((types, DataType::Boolean))
        }
        ("date_trunc", 3) if a[1].ty == DataType::TimestampTz => Ok((
            vec![
                Some(DataType::Text),
                Some(DataType::TimestampTz),
                Some(DataType::Text),
            ],
            DataType::TimestampTz,
        )),
        ("extract", 2) if a[1].ty.is_temporal() => Ok((
            vec![Some(DataType::Text), Some(a[1].ty.clone())],
            DataType::Numeric,
        )),
        ("timezone", 2)
            if a[0].ty == DataType::Interval
                && matches!(
                    a[1].ty,
                    DataType::Timestamp | DataType::TimestampTz | DataType::TimeTz
                ) =>
        {
            Ok((
                types,
                match a[1].ty {
                    DataType::Timestamp => DataType::TimestampTz,
                    DataType::TimestampTz => DataType::Timestamp,
                    _ => DataType::TimeTz,
                },
            ))
        }
        ("timezone", 2) if matches!(a[1].ty, DataType::Timestamp | DataType::TimestampTz) => Ok((
            vec![Some(DataType::Text), Some(a[1].ty.clone())],
            if a[1].ty == DataType::Timestamp {
                DataType::TimestampTz
            } else {
                DataType::Timestamp
            },
        )),
        ("age", 2)
            if matches!(a[0].ty, DataType::Timestamp | DataType::TimestampTz)
                && a[0].ty == a[1].ty =>
        {
            Ok((types, DataType::Interval))
        }
        ("justify_hours" | "justify_days", 1) if a[0].ty == DataType::Interval => {
            Ok((types, DataType::Interval))
        }
        ("make_date", 3) => Ok((vec![Some(DataType::Integer); 3], DataType::Date)),

        ("date_part", 2) if a[1].ty.is_temporal() => Ok((
            vec![Some(DataType::Text), Some(a[1].ty.clone())],
            DataType::DoublePrecision,
        )),
        ("date_trunc", 2) if a[1].ty == DataType::Date => Ok((
            vec![Some(DataType::Text), Some(DataType::TimestampTz)],
            DataType::TimestampTz,
        )),
        ("date_trunc", 2)
            if matches!(
                a[1].ty,
                DataType::Timestamp | DataType::TimestampTz | DataType::Interval
            ) =>
        {
            Ok((
                vec![Some(DataType::Text), Some(a[1].ty.clone())],
                a[1].ty.clone(),
            ))
        }
        ("date_bin", 3)
            if matches!(a[1].ty, DataType::Timestamp | DataType::TimestampTz)
                && a[0].ty == DataType::Interval
                && a[2].ty == a[1].ty =>
        {
            Ok((types, a[1].ty.clone()))
        }
        ("justify_interval", 1) if a[0].ty == DataType::Interval => Ok((types, DataType::Interval)),
        _ if known_name(n) => Err(SqlError::UndefinedFunction),
        _ => return None,
    })
}
fn field(s: &str) -> R<F> {
    Ok(match s.to_ascii_lowercase().as_str() {
        "microseconds" => F::Microseconds,
        "milliseconds" => F::Milliseconds,
        "second" => F::Second,
        "minute" => F::Minute,
        "hour" => F::Hour,
        "day" => F::Day,
        "month" => F::Month,
        "quarter" => F::Quarter,
        "week" => F::Week,
        "year" => F::Year,
        "decade" => F::Decade,
        "century" => F::Century,
        "millennium" => F::Millennium,
        "isoyear" => F::IsoYear,
        "dow" => F::Dow,
        "isodow" => F::IsoDow,
        "doy" => F::Doy,
        "julian" => F::Julian,
        "epoch" => F::Epoch,
        "timezone" => F::Timezone,
        "timezone_hour" => F::TimezoneHour,
        "timezone_minute" => F::TimezoneMinute,
        _ => return Err(SqlError::InvalidParameterValue),
    })
}
pub(super) fn eval(n: &str, v: &[Value], budget: &mut usize) -> Option<R<Value>> {
    if ![
        "isfinite",
        "date_part",
        "extract",
        "timezone",
        "age",
        "make_date",
        "make_time",
        "make_timestamp",
        "make_timestamptz",
        "make_interval",
        "date_trunc",
        "date_bin",
        "justify_interval",
        "justify_days",
        "justify_hours",
    ]
    .contains(&n)
    {
        return None;
    }
    Some((|| {
        let int = |i: usize| -> R<i32> {
            match v.get(i) {
                Some(Value::Int(x)) => {
                    i32::try_from(*x).map_err(|_| SqlError::DatetimeFieldOverflow)
                }
                None => Ok(0),
                _ => Err(SqlError::Type),
            }
        };
        let seconds = |i: usize| -> R<f64> {
            let Some(x) = v.get(i) else {
                return Ok(0.0);
            };
            match cast_value(x.clone(), &DataType::DoublePrecision, &mut 0)? {
                Value::Float8(x) => Ok(x.value()),
                _ => Err(SqlError::Type),
            }
        };
        let out = match (n, v) {
            ("isfinite", [Value::Date(t)]) => {
                Value::Bool(*t != Date::INFINITY && *t != Date::NEG_INFINITY)
            }
            ("isfinite", [Value::Timestamp(t)]) => {
                Value::Bool(*t != Timestamp::INFINITY && *t != Timestamp::NEG_INFINITY)
            }
            ("isfinite", [Value::TimestampTz(t)]) => {
                Value::Bool(*t != TimestampTz::INFINITY && *t != TimestampTz::NEG_INFINITY)
            }
            ("isfinite", [Value::Interval(t)]) => Value::Bool(
                !t.same_components(Interval::INFINITY)
                    && !t.same_components(Interval::NEG_INFINITY),
            ),
            ("make_time", _) => Value::Time(
                giggles_pg_types::temporal::make_time_float8(int(0)?, int(1)?, seconds(2)?)
                    .map_err(function_error)?,
            ),
            ("make_timestamp" | "make_timestamptz", _) => {
                let t = giggles_pg_types::temporal::make_timestamp_float8(
                    int(0)?,
                    u32::try_from(int(1)?).map_err(|_| SqlError::DatetimeFieldOverflow)?,
                    u32::try_from(int(2)?).map_err(|_| SqlError::DatetimeFieldOverflow)?,
                    int(3)?,
                    int(4)?,
                    seconds(5)?,
                )
                .map_err(function_error)?;
                if n == "make_timestamp" {
                    Value::Timestamp(t)
                } else {
                    let zone = match v.get(6) {
                        Some(Value::Text(z)) => SessionZone::named(z).map_err(zone_error)?,
                        None => SessionZone::Fixed(0),
                        _ => return Err(SqlError::Type),
                    };
                    Value::TimestampTz(zone.to_instant(t).map_err(zone_error)?)
                }
            }
            ("make_interval", _) => Value::Interval(
                giggles_pg_types::temporal::make_interval(
                    int(0)?,
                    int(1)?,
                    int(2)?,
                    int(3)?,
                    int(4)?,
                    int(5)?,
                    seconds(6)?,
                )
                .map_err(function_error)?,
            ),
            ("make_date", [Value::Int(y), Value::Int(m), Value::Int(d)]) => Value::Date(
                giggles_pg_types::temporal::make_date(
                    i32::try_from(*y).map_err(|_| SqlError::DatetimeFieldOverflow)?,
                    u32::try_from(*m).map_err(|_| SqlError::DatetimeFieldOverflow)?,
                    u32::try_from(*d).map_err(|_| SqlError::DatetimeFieldOverflow)?,
                )
                .map_err(function_error)?,
            ),
            ("timezone", [Value::Interval(z), Value::Timestamp(t)]) => Value::TimestampTz(
                t.timezone_interval(*z)
                    .map_err(|e| zone_interval_error(*z, e))?,
            ),
            ("timezone", [Value::Interval(z), Value::TimestampTz(t)]) => Value::Timestamp(
                t.timezone_interval(*z)
                    .map_err(|e| zone_interval_error(*z, e))?,
            ),
            ("timezone", [Value::Interval(z), Value::TimeTz(t)]) => Value::TimeTz(
                t.timezone_interval(*z)
                    .map_err(|e| zone_interval_error(*z, e))?,
            ),
            ("timezone", [Value::Text(z), Value::Timestamp(t)]) => Value::TimestampTz(
                SessionZone::named(z)
                    .map_err(zone_error)?
                    .to_instant(*t)
                    .map_err(zone_error)?,
            ),
            ("timezone", [Value::Text(z), Value::TimestampTz(t)]) => Value::Timestamp(
                SessionZone::named(z)
                    .map_err(zone_error)?
                    .to_local(*t)
                    .map_err(zone_error)?,
            ),
            ("age", [Value::Timestamp(t), Value::Timestamp(o)]) => {
                Value::Interval(t.age(*o).map_err(|_| SqlError::Overflow)?)
            }
            ("age", [Value::TimestampTz(t), Value::TimestampTz(o)]) => Value::Interval(
                t.age(*o, SessionZone::Fixed(0))
                    .map_err(|_| SqlError::Overflow)?,
            ),
            ("justify_days", [Value::Interval(t)]) => {
                Value::Interval(t.justify_days().map_err(|_| SqlError::Overflow)?)
            }
            ("justify_hours", [Value::Interval(t)]) => {
                Value::Interval(t.justify_hours().map_err(|_| SqlError::Overflow)?)
            }
            ("extract", [Value::Text(f), value]) => {
                let f = field(f)?;
                let p = match value {
                    Value::Date(t) => t.extract(f),
                    Value::Time(t) => t.extract(f),
                    Value::TimeTz(t) => t.extract(f),
                    Value::Timestamp(t) => t.extract(f),
                    Value::TimestampTz(t) => t.extract(f, SessionZone::Fixed(0)),
                    Value::Interval(t) => t.extract(f),
                    _ => return Err(SqlError::Type),
                }
                .map_err(function_error)?;
                p.map(Value::Numeric).unwrap_or(Value::Null)
            }

            ("justify_interval", [Value::Interval(i)]) => {
                Value::Interval(i.justify_interval().map_err(|_| SqlError::Overflow)?)
            }
            ("date_bin", [Value::Interval(s), Value::Timestamp(t), Value::Timestamp(o)]) => {
                Value::Timestamp(t.date_bin(*s, *o).map_err(|e| bin_error(*s, e))?)
            }
            (
                "date_bin",
                [
                    Value::Interval(s),
                    Value::TimestampTz(t),
                    Value::TimestampTz(o),
                ],
            ) => Value::TimestampTz(t.date_bin(*s, *o).map_err(|e| bin_error(*s, e))?),
            ("date_trunc", [Value::Text(f), Value::TimestampTz(t), Value::Text(z)]) => {
                Value::TimestampTz(
                    t.date_trunc(field(f)?, SessionZone::named(z).map_err(zone_error)?)
                        .map_err(|e| trunc_error(f, e))?,
                )
            }
            ("date_trunc", [Value::Text(f), Value::Timestamp(t)]) => {
                Value::Timestamp(t.date_trunc(field(f)?).map_err(|e| trunc_error(f, e))?)
            }
            ("date_trunc", [Value::Text(f), Value::TimestampTz(t)]) => Value::TimestampTz(
                t.date_trunc(field(f)?, SessionZone::Fixed(0))
                    .map_err(|e| trunc_error(f, e))?,
            ),
            ("date_trunc", [Value::Text(f), Value::Interval(t)]) => {
                Value::Interval(t.date_trunc(field(f)?).map_err(|e| trunc_error(f, e))?)
            }
            ("date_part", [Value::Text(f), value]) => {
                let f = field(f)?;
                let p = match value {
                    Value::Date(t) => t.date_part(f),
                    Value::Time(t) => t.date_part(f),
                    Value::TimeTz(t) => t.date_part(f),
                    Value::Timestamp(t) => t.date_part(f),
                    Value::TimestampTz(t) => t.date_part(f, SessionZone::Fixed(0)),
                    Value::Interval(t) => t.date_part(f),
                    _ => return Err(SqlError::Type),
                }
                .map_err(function_error)?;
                p.map(Value::Float8).unwrap_or(Value::Null)
            }
            _ => return Err(SqlError::Type),
        };
        charge(budget, &out)?;
        Ok(out)
    })())
}

pub(super) fn known_name(n: &str) -> bool {
    matches!(
        n,
        "extract"
            | "date_part"
            | "date_trunc"
            | "date_bin"
            | "timezone"
            | "age"
            | "make_date"
            | "make_time"
            | "make_timestamp"
            | "make_timestamptz"
            | "make_interval"
            | "isfinite"
            | "justify_hours"
            | "justify_days"
            | "justify_interval"
    )
}
pub(super) fn parse_error(e: giggles_pg_types::Error, interval: bool) -> SqlError {
    use giggles_pg_types::Error as E;
    match e {
        E::Syntax | E::Binary => SqlError::InvalidDatetimeFormat,
        E::TimeZoneDisplacement => SqlError::InvalidTimeZoneDisplacementValue,
        E::UnknownTimeZone => SqlError::InvalidParameterValue,
        E::Range if interval => SqlError::IntervalFieldOverflow,
        E::Range => SqlError::DatetimeFieldOverflow,
        E::Limit => SqlError::Limit,
        E::DivisionByZero => SqlError::DivisionByZero,
        E::Unimplemented => SqlError::Unsupported,
    }
}
fn zone_error(e: giggles_pg_types::Error) -> SqlError {
    use giggles_pg_types::Error as E;
    match e {
        E::Syntax | E::UnknownTimeZone => SqlError::InvalidParameterValue,
        E::Range | E::TimeZoneDisplacement => SqlError::DatetimeFieldOverflow,
        E::Limit => SqlError::Limit,
        E::DivisionByZero => SqlError::DivisionByZero,
        E::Unimplemented | E::Binary => SqlError::Unsupported,
    }
}
pub(super) fn function_error(e: giggles_pg_types::temporal::TemporalFunctionError) -> SqlError {
    use giggles_pg_types::temporal::TemporalFunctionError as E;
    match e {
        E::UnsupportedField => SqlError::Unsupported,
        E::NumericOverflow => SqlError::Overflow,
        E::DivisionByZero => SqlError::DivisionByZero,
        E::Range | E::InvalidStride | E::InfiniteStride | E::InfiniteOrigin => {
            SqlError::DatetimeFieldOverflow
        }
    }
}
fn bin_error(stride: Interval, e: giggles_pg_types::temporal::TemporalFunctionError) -> SqlError {
    if e == giggles_pg_types::temporal::TemporalFunctionError::InvalidStride && stride.months() != 0
    {
        SqlError::Unsupported
    } else {
        function_error(e)
    }
}
fn zone_interval_error(
    zone: Interval,
    e: giggles_pg_types::temporal::TemporalFunctionError,
) -> SqlError {
    if zone.months() != 0 || zone.days() != 0 {
        SqlError::InvalidParameterValue
    } else {
        function_error(e)
    }
}
fn date_trunc_field(unit: &str) -> R<F> {
    // PostgreSQL's deltatktbl compares the first ten bytes of the lowercased unit.
    let unit = unit.as_bytes();
    Ok(match unit[..unit.len().min(10)].to_ascii_lowercase().as_slice() {
        b"microsecon" | b"us" | b"usec" | b"usecs" | b"usecond" | b"useconds" => F::Microseconds,
        b"millisecon" | b"ms" | b"msec" | b"msecs" | b"msecond" | b"mseconds" => F::Milliseconds,
        b"s" | b"sec" | b"secs" | b"second" | b"seconds" => F::Second,
        b"m" | b"min" | b"mins" | b"minute" | b"minutes" => F::Minute,
        b"h" | b"hr" | b"hrs" | b"hour" | b"hours" => F::Hour,
        b"d" | b"day" | b"days" => F::Day,
        b"w" | b"week" | b"weeks" => F::Week,
        b"mon" | b"mons" | b"month" | b"months" => F::Month,
        b"qtr" | b"quarter" => F::Quarter,
        b"y" | b"yr" | b"yrs" | b"year" | b"years" => F::Year,
        b"dec" | b"decs" | b"decade" | b"decades" => F::Decade,
        b"c" | b"cent" | b"century" | b"centuries" => F::Century,
        b"mil" | b"mils" | b"millennium" | b"millennia" => F::Millennium,
        b"timezone" | b"timezone_h" | b"timezone_m" => return Err(SqlError::Unsupported),
        _ => return Err(SqlError::InvalidParameterValue),
    })
}

pub(super) fn trunc_date(values: &[Value], zone: SessionZone, budget: &mut usize) -> R<Value> {
    match values {
        [Value::Null, _] | [_, Value::Null] => Ok(Value::Null),
        [Value::Text(f), Value::TimestampTz(t)] => {
            let value = Value::TimestampTz(
                t.date_trunc(date_trunc_field(f)?, zone)
                    .map_err(function_error)?,
            );
            charge(budget, &value)?;
            Ok(value)
        }
        _ => Err(SqlError::UndefinedFunction),
    }
}

fn trunc_error(f: &str, e: giggles_pg_types::temporal::TemporalFunctionError) -> SqlError {
    if matches!(
        field(f),
        Ok(F::Dow | F::IsoDow | F::Doy | F::IsoYear | F::Epoch)
    ) {
        SqlError::InvalidParameterValue
    } else {
        function_error(e)
    }
}
