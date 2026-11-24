//! Assignment casts for literals and explicitly declared parameters.
use super::*;
use crate::relational::clock::ExecutionClock;

pub(in crate::relational) fn compatible(source: &DataType, target: &DataType) -> bool {
    matches!(
        source,
        DataType::Text | DataType::Varchar | DataType::Bpchar
    ) && matches!(
        target,
        DataType::Text | DataType::Varchar | DataType::Bpchar
    ) || source == target
        || number(source) && number(target)
        || matches!(
            target,
            DataType::Text | DataType::Varchar | DataType::Bpchar
        )
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

/// Assignment source metadata. JSON expressions use text values with a distinct OID.
#[derive(Debug, Clone)]
pub(in crate::relational) struct SourceType {
    data_type: DataType,
    json: bool,
}

impl SourceType {
    pub(in crate::relational) fn new(data_type: &DataType, oid: Option<&u32>) -> Self {
        Self {
            data_type: data_type.clone(),
            json: oid == Some(&json_aggregate::OID),
        }
    }

    pub(in crate::relational) fn compatible(&self, target: &DataType) -> bool {
        self.json && *target == DataType::Jsonb || compatible(&self.data_type, target)
    }

    pub(in crate::relational) fn check_column(
        &self,
        state: &Relational,
        column: &ColumnDef,
        source_enum: Option<&TypeDef>,
    ) -> R<()> {
        if let Some(target @ TypeDef::Enum { .. }) = state.column_type(column.id) {
            if source_enum == Some(target) && self.data_type == column.data_type {
                return Ok(());
            }
            let target = user_types::enum_display(target, column.data_type == DataType::TextArray);
            let source = source_enum.map_or_else(
                || if self.json { "json".into() } else { type_display(&self.data_type).into() },
                |ty| user_types::enum_display(ty, self.data_type == DataType::TextArray),
            );
            user_types::view_replace_error(format!(
                r#"column "{}" is of type {target} but expression is of type {source}"#,
                column.name,
            ));
            user_types::sql_error_hint("You will need to rewrite or cast the expression.");
            return Err(SqlError::Type);
        }
        if self.compatible(&column.data_type) { Ok(()) } else { Err(SqlError::Type) }
    }

    pub(in crate::relational) fn convert(
        &self,
        value: Value,
        target: &DataType,
        clock: Option<&ExecutionClock>,
    ) -> R<Value> {
        if self.json && *target == DataType::Jsonb {
            return cast_value(value, target, &mut 0);
        }
        convert(value, &self.data_type, target, clock)
    }
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
    // Character targets convert exactly as text; coerce_to_column applies the typmod.
    let target = if matches!(target, DataType::Varchar | DataType::Bpchar) {
        &DataType::Text
    } else {
        target
    };
    if let Some(value) = text_search_types::cast_typed(&value, source, target) {
        return value;
    }
    if source == target
        || value == Value::Null
        || matches!(
            source,
            DataType::Text | DataType::Varchar | DataType::Bpchar
        ) && matches!(
            target,
            DataType::Text | DataType::Varchar | DataType::Bpchar
        )
    {
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
    let time = |micros: i64| Time::from_microseconds(micros.rem_euclid(DAY)).map_err(pg_error);
    let context = || clock.ok_or(SqlError::Unsupported)?.comparison_context();
    let zone_error = |error| match error {
        giggles_pg_types::Error::Range => SqlError::DatetimeFieldOverflow,
        error => pg_error(error),
    };
    let local_parts = |t: TimestampTz| -> R<(i64, i32)> {
        let zone = context()?.1;
        if matches!(t, TimestampTz::NEG_INFINITY | TimestampTz::INFINITY) {
            return Ok((t.microseconds(), 0));
        }
        let offset = zone.instant_offset(t).map_err(zone_error)?;
        // A local date or time can be valid outside the local timestamp range.
        let local = t
            .microseconds()
            .checked_add(i64::from(offset) * 1_000_000)
            .ok_or(SqlError::DatetimeFieldOverflow)?;
        Ok((local, offset))
    };
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
        Timestamp::from_microseconds(micros).map_err(|_| SqlError::DatetimeFieldOverflow)
    };
    if *target == DataType::Text
        && let Some(text) = presentation_text(&value, clock)
    {
        return text.map(Value::Text);
    }
    Ok(match (value, target) {
        (Value::Date(d), DataType::Timestamp) => Value::Timestamp(timestamp(d)?),
        (Value::Date(d), DataType::TimestampTz) => {
            Value::TimestampTz(context()?.1.to_instant(timestamp(d)?).map_err(zone_error)?)
        }
        (Value::Timestamp(t), DataType::Date) => Value::Date(date(t.microseconds())?),
        (Value::Timestamp(Timestamp::NEG_INFINITY | Timestamp::INFINITY), DataType::Time)
        | (
            Value::TimestampTz(TimestampTz::NEG_INFINITY | TimestampTz::INFINITY),
            DataType::Time | DataType::TimeTz,
        ) => Value::Null,
        (Value::Timestamp(t), DataType::Time) => Value::Time(time(t.microseconds())?),
        (Value::Timestamp(t), DataType::TimestampTz) => {
            Value::TimestampTz(context()?.1.to_instant(t).map_err(zone_error)?)
        }
        (Value::TimestampTz(t), DataType::Timestamp) => {
            Value::Timestamp(context()?.1.to_local(t).map_err(zone_error)?)
        }
        (Value::TimestampTz(t), DataType::Date) => Value::Date(date(local_parts(t)?.0)?),
        (Value::TimestampTz(t), DataType::Time) => Value::Time(time(local_parts(t)?.0)?),
        (value @ (Value::Float4(_) | Value::Float8(_)), DataType::Text) => {
            session_cast(value, source, target, clock)?
        }
        (Value::TimestampTz(t), DataType::TimeTz) => {
            let (local, offset) = local_parts(t)?;
            Value::TimeTz(TimeTz::new(time(local)?, offset).map_err(pg_error)?)
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
        (Value::Interval(i), DataType::Time) => {
            if i.same_components(Interval::INFINITY) || i.same_components(Interval::NEG_INFINITY) {
                return Err(SqlError::DatetimeFieldOverflow);
            }
            Value::Time(time(i.microseconds())?)
        }
        (Value::Time(t), DataType::Interval) => {
            Value::Interval(Interval::new(0, 0, t.microseconds()))
        }
        (value, target) => cast_value(value, target, &mut 0)?,
    })
}
