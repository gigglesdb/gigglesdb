use super::{Body, Failure, Operation};
use crate::relational::{ColumnDef, DataType, TableDef, Value};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Ty {
    Bool,
    Int,
    Text,
    Bpchar,
    Bytes,
    Numeric,
    Real,
    DoublePrecision,
    Date,
    Time,
    TimeTz,
    Timestamp,
    TimestampTz,
    Interval,
}
fn ty(v: &Value) -> Option<Ty> {
    match v {
        Value::Null => None,
        Value::Bool(_) => Some(Ty::Bool),
        Value::Int(_) => Some(Ty::Int),
        Value::Text(_) => Some(Ty::Text),
        Value::Bytes(_) => Some(Ty::Bytes),
        Value::Numeric(_) => Some(Ty::Numeric),
        Value::Float4(_) => Some(Ty::Real),
        Value::Float8(_) => Some(Ty::DoublePrecision),
        Value::Date(_) => Some(Ty::Date),
        Value::Time(_) => Some(Ty::Time),
        Value::TimeTz(_) => Some(Ty::TimeTz),
        Value::Timestamp(_) => Some(Ty::Timestamp),
        Value::TimestampTz(_) => Some(Ty::TimestampTz),
        Value::Interval(_) => Some(Ty::Interval),
    }
}
fn colty(c: &ColumnDef) -> Ty {
    match c.data_type {
        DataType::Boolean => Ty::Bool,
        DataType::SmallInt | DataType::Integer | DataType::BigInt => Ty::Int,
        DataType::Uuid
        | DataType::Jsonb
        | DataType::IntArray
        | DataType::BigIntArray
        | DataType::TextArray
        | DataType::Text
        | DataType::Varchar => Ty::Text,
        DataType::Bpchar => Ty::Bpchar,
        DataType::Bytes => Ty::Bytes,
        DataType::Numeric => Ty::Numeric,
        DataType::Real => Ty::Real,
        DataType::DoublePrecision => Ty::DoublePrecision,
        DataType::Date => Ty::Date,
        DataType::Time => Ty::Time,
        DataType::TimeTz => Ty::TimeTz,
        DataType::Timestamp => Ty::Timestamp,
        DataType::TimestampTz => Ty::TimestampTz,
        DataType::Interval => Ty::Interval,
    }
}
fn check(v: &Value, c: &ColumnDef) -> Result<(), Failure> {
    if *v == Value::Null {
        return if c.not_null { Err(Failure::Type) } else { Ok(()) };
    }
    if ty(v)
        != Some(if c.data_type == DataType::Bpchar {
            Ty::Text
        } else {
            colty(c)
        })
    {
        return Err(Failure::Type);
    }
    if let Value::Int(n) = v {
        match c.data_type {
            DataType::SmallInt if i16::try_from(*n).is_err() => return Err(Failure::Type),
            DataType::Integer if i32::try_from(*n).is_err() => return Err(Failure::Type),
            _ => (),
        }
    }
    Ok(())
}
/// Frozen 8d9b569b binder categories: never follow later binder changes,
/// because retained application outcomes are persisted decisions.
pub(super) fn bind_failure(table: &TableDef, body: &Body) -> Option<Failure> {
    let (key, assignments) = match &body.operation {
        Operation::Read { key } => (key, None),
        Operation::GuardedWrite { key, assignments, .. } => (key, Some(assignments)),
    };
    let mut remaining = crate::operations::MAX_BOUND_VALUE_BYTES;
    let mut charge = |value: &Value| {
        remaining = remaining.checked_sub(crate::relational::value_bytes(value))
            .ok_or(Failure::Limit)?;
        Ok::<(), Failure>(())
    };
    let bind = &mut || -> Result<(), Failure> {
        for (index, value) in table.primary_key.iter().zip(key) {
            charge(value)?;
            let column = &table.columns[*index];
            if colty(column) == Ty::Bpchar {
                return Err(Failure::Unsupported);
            }
            if ty(value).is_some() && ty(value) != Some(colty(column)) {
                return Err(Failure::Type);
            }
        }
        if let Some(assignments) = assignments {
            for value in key {
                charge(value)?;
            }
            for (id, value) in assignments {
                let column = table.columns.iter().find(|column| column.id == *id)
                    .ok_or(Failure::Bind)?;
                charge(value)?;
                check(value, column)?;
            }
        }
        Ok(())
    };
    bind().err()
}
