//! Assignment casts for literals and explicitly declared parameters.
use super::*;
use crate::relational::clock::ExecutionClock;

pub(in crate::relational) fn compatible(source: &DataType, target: &DataType) -> bool {
    source == target
        || number(source) && number(target)
        || *target == DataType::Text
        || matches!(
            (source, target),
            (DataType::Date, DataType::Timestamp | DataType::TimestampTz)
                | (
                    DataType::Timestamp,
                    DataType::Date | DataType::TimestampTz | DataType::Time
                )
                | (
                    DataType::TimestampTz,
                    DataType::Date | DataType::Timestamp | DataType::Time | DataType::TimeTz
                )
                | (DataType::Time, DataType::TimeTz | DataType::Interval)
                | (DataType::TimeTz | DataType::Interval, DataType::Time)
        )
}

/// Preserve nulls and unknown text input errors; assign typed literals by source type.
pub(in crate::relational) fn literal(
    value: Value,
    target: &DataType,
    clock: Option<&ExecutionClock>,
) -> R<Value> {
    let source = match &value {
        Value::Null => return Ok(value),
        Value::Text(_) if *target == DataType::Text => return Ok(value),
        Value::Text(_) => return crate::relational::query::coerce_clocked(value, target, clock),
        Value::Int(n) if i32::try_from(*n).is_ok() => DataType::Integer,
        Value::Int(_) => DataType::BigInt,
        Value::Numeric(_) => DataType::Numeric,
        Value::Bool(_) => DataType::Boolean,
        Value::Bytes(_) => DataType::Bytes,
        Value::Float4(_) => DataType::Real,
        Value::Float8(_) => DataType::DoublePrecision,
        Value::Date(_) => DataType::Date,
        Value::Time(_) => DataType::Time,
        Value::TimeTz(_) => DataType::TimeTz,
        Value::Timestamp(_) => DataType::Timestamp,
        Value::TimestampTz(_) => DataType::TimestampTz,
        Value::Interval(_) => DataType::Interval,
    };
    convert(value, &source, target, clock)
}

pub(in crate::relational) fn convert(
    value: Value,
    source: &DataType,
    target: &DataType,
    clock: Option<&ExecutionClock>,
) -> R<Value> {
    if !compatible(source, target) {
        return Err(SqlError::Type);
    }
    if source == target || value == Value::Null {
        return Ok(value);
    }
    if let Value::Numeric(n) = &value
        && integer(target)
        && n.scale().is_none()
    {
        return Err(if n.to_string() == "NaN" {
            SqlError::Unsupported
        } else {
            SqlError::Overflow
        });
    }
    const DAY: i64 = 86_400_000_000;
    let date = |micros: i64| {
        let days = if micros == i64::MIN {
            i32::MIN
        } else if micros == i64::MAX {
            i32::MAX
        } else {
            i32::try_from(micros.div_euclid(DAY)).map_err(|_| SqlError::DatetimeFieldOverflow)?
        };
        Date::from_days(days).map_err(pg_error)
    };
    let time = |micros: i64| {
        if micros == i64::MIN || micros == i64::MAX {
            return Err(SqlError::DatetimeFieldOverflow);
        }
        Time::from_microseconds(micros.rem_euclid(DAY)).map_err(pg_error)
    };
    let context = || clock.ok_or(SqlError::Unsupported)?.comparison_context();
    let timestamp = |d: Date| {
        let micros = if d == Date::NEG_INFINITY {
            i64::MIN
        } else if d == Date::INFINITY {
            i64::MAX
        } else {
            i64::from(d.days())
                .checked_mul(DAY)
                .ok_or(SqlError::DatetimeFieldOverflow)?
        };
        Timestamp::from_microseconds(micros).map_err(pg_error)
    };
    Ok(match (value, target) {
        (Value::Date(d), DataType::Timestamp) => Value::Timestamp(timestamp(d)?),
        (Value::Date(d), DataType::TimestampTz) => {
            Value::TimestampTz(context()?.1.to_instant(timestamp(d)?).map_err(pg_error)?)
        }
        (Value::Timestamp(t), DataType::Date) => Value::Date(date(t.microseconds())?),
        (Value::Timestamp(t), DataType::Time) => Value::Time(time(t.microseconds())?),
        (Value::Timestamp(t), DataType::TimestampTz) => {
            Value::TimestampTz(context()?.1.to_instant(t).map_err(pg_error)?)
        }
        (Value::TimestampTz(t), DataType::Timestamp) => {
            Value::Timestamp(context()?.1.to_local(t).map_err(pg_error)?)
        }
        (Value::TimestampTz(t), DataType::Date) => Value::Date(date(
            context()?.1.to_local(t).map_err(pg_error)?.microseconds(),
        )?),
        (Value::TimestampTz(t), DataType::Time) => Value::Time(time(
            context()?.1.to_local(t).map_err(pg_error)?.microseconds(),
        )?),
        (value @ (Value::Float4(_) | Value::Float8(_)), DataType::Text) => {
            session_cast(value, source, target, clock)?
        }
        (Value::TimestampTz(t), DataType::TimeTz) => {
            let local = context()?.1.to_local(t).map_err(pg_error)?;
            let time = time(local.microseconds())?;
            let offset =
                (i128::from(local.microseconds()) - i128::from(t.microseconds())) / 1_000_000;
            Value::TimeTz(
                TimeTz::new(
                    time,
                    i32::try_from(offset).map_err(|_| SqlError::DatetimeFieldOverflow)?,
                )
                .map_err(pg_error)?,
            )
        }
        (Value::Time(t), DataType::TimeTz) => {
            let (transaction, zone) = context()?;
            let local = zone
                .to_local(TimestampTz::from_microseconds(transaction).map_err(pg_error)?)
                .map_err(pg_error)?;
            Value::TimeTz(
                TimeTz::in_zone_on_date(t, date(local.microseconds())?, zone).map_err(pg_error)?,
            )
        }
        (Value::TimeTz(t), DataType::Time) => Value::Time(t.time()),
        (Value::Interval(i), DataType::Time) => Value::Time(
            Time::from_microseconds(i.microseconds().rem_euclid(DAY)).map_err(pg_error)?,
        ),
        (Value::Time(t), DataType::Interval) => {
            Value::Interval(Interval::new(0, 0, t.microseconds()))
        }
        (Value::TimestampTz(t), DataType::Text) => {
            Value::Text(t.format_in_zone(context()?.1).map_err(pg_error)?)
        }
        (value, target) => cast_value(value, target, &mut 0)?,
    })
}
