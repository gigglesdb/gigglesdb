//! Checked PostgreSQL binary values; no dependence on session text formatting.
use crate::mapping::{
    Array, ArrayDimension, Date, MappingError, Numeric, PgType, Timestamp, Value,
};
use tokio_postgres::types::{FromSql, Kind, Type};

pub(super) struct Raw(pub Vec<u8>);
impl<'a> FromSql<'a> for Raw {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self(raw.to_vec()))
    }
    fn accepts(_: &Type) -> bool {
        true
    }
}
fn invalid() -> MappingError {
    MappingError::Invalid("qualified PostgreSQL binary value".into())
}
struct Input<'a>(&'a [u8]);
impl<'a> Input<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], MappingError> {
        let v = self.0.get(..n).ok_or_else(invalid)?;
        self.0 = &self.0[n..];
        Ok(v)
    }
    fn i16(&mut self) -> Result<i16, MappingError> {
        Ok(i16::from_be_bytes(
            self.take(2)?.try_into().map_err(|_| invalid())?,
        ))
    }
    fn i32(&mut self) -> Result<i32, MappingError> {
        Ok(i32::from_be_bytes(
            self.take(4)?.try_into().map_err(|_| invalid())?,
        ))
    }
    fn i64(&mut self) -> Result<i64, MappingError> {
        Ok(i64::from_be_bytes(
            self.take(8)?.try_into().map_err(|_| invalid())?,
        ))
    }
}
pub(super) fn binary(ty: &PgType, wire: &Type, bytes: &[u8]) -> Result<Value, MappingError> {
    let mut input = Input(bytes);
    let value = match ty {
        PgType::Bool => match input.take(1)?[0] {
            0 => Value::Bool(false),
            1 => Value::Bool(true),
            _ => return Err(invalid()),
        },
        PgType::Int2 => Value::Int2(input.i16()?),
        PgType::Int4 => Value::Int4(input.i32()?),
        PgType::Int8 => Value::Int8(input.i64()?),
        PgType::Float4 => Value::Float4(input.i32()? as u32),
        PgType::Float8 => Value::Float8(input.i64()? as u64),
        PgType::Bytea => Value::Bytea(input.take(bytes.len())?.into()),
        PgType::Uuid => Value::Uuid(input.take(16)?.try_into().map_err(|_| invalid())?),
        PgType::Date => Value::Date(match input.i32()? {
            i32::MIN => Date::NegativeInfinity,
            i32::MAX => Date::PositiveInfinity,
            v => Date::Finite(v),
        }),
        PgType::Timestamp | PgType::Timestamptz => {
            let v = match input.i64()? {
                i64::MIN => Timestamp::NegativeInfinity,
                i64::MAX => Timestamp::PositiveInfinity,
                v => Timestamp::Finite(v),
            };
            if *ty == PgType::Timestamp {
                Value::Timestamp(v)
            } else {
                Value::Timestamptz(v)
            }
        }
        PgType::Time => Value::Time(input.i64()?),
        PgType::Timetz => Value::Timetz {
            micros: input.i64()?,
            offset_east: input.i32()?.checked_neg().ok_or_else(invalid)?,
        },
        PgType::Interval => Value::Interval {
            micros: input.i64()?,
            days: input.i32()?,
            months: input.i32()?,
        },
        PgType::Text { .. } | PgType::Varchar { .. } | PgType::Char { .. } | PgType::Json => {
            Value::parse_pg_text(ty, Some(input.take(bytes.len())?))?
        }
        PgType::Jsonb => {
            if input.take(1)? != [1] {
                return Err(invalid());
            }
            Value::parse_pg_text(ty, Some(input.take(bytes.len() - 1)?))?
        }
        PgType::Numeric => Value::Numeric(numeric(&mut input)?),
        PgType::Array(element) => {
            let Kind::Array(element_wire) = wire.kind() else {
                return Err(invalid());
            };
            let dimensions = usize::try_from(input.i32()?).map_err(|_| invalid())?;
            if dimensions > 6 {
                return Err(invalid());
            }
            if !matches!(input.i32()?, 0 | 1) || input.i32()? as u32 != element_wire.oid() {
                return Err(invalid());
            }
            let mut shape = Vec::new();
            let mut count = if dimensions == 0 { 0usize } else { 1 };
            for _ in 0..dimensions {
                let length = u32::try_from(input.i32()?).map_err(|_| invalid())?;
                count = count.checked_mul(length as usize).ok_or_else(invalid)?;
                shape.push(ArrayDimension {
                    length,
                    lower_bound: input.i32()?,
                });
            }
            if count > input.0.len() / 4 {
                return Err(invalid());
            }
            let mut elements = Vec::with_capacity(count);
            for _ in 0..count {
                elements.push(match input.i32()? {
                    -1 => Value::Null,
                    length if length >= 0 => {
                        binary(element, element_wire, input.take(length as usize)?)?
                    }
                    _ => return Err(invalid()),
                });
            }
            Value::Array(Array {
                element_type: element.clone(),
                dimensions: shape,
                elements,
            })
        }
        _ => return Err(invalid()),
    };
    if !input.0.is_empty() {
        return Err(invalid());
    }
    // Exercise mapping validation, including array shape and temporal bounds.
    value.canonical_bytes()?;
    Ok(value)
}
fn numeric(input: &mut Input<'_>) -> Result<Numeric, MappingError> {
    // Protocol ndigits is an unsigned 16-bit count. Very large valid numeric
    // values can exceed 32767 base-10000 groups.
    let count = usize::from(input.i16()? as u16);
    let weight = i32::from(input.i16()?);
    let sign = input.i16()? as u16;
    let scale = u32::try_from(input.i16()?).map_err(|_| invalid())?;
    let mut groups = Vec::with_capacity(count);
    for _ in 0..count {
        let digit = input.i16()?;
        if !(0..10_000).contains(&digit) {
            return Err(invalid());
        }
        groups.push(digit);
    }
    match sign {
        0xc000 => return Ok(Numeric::NAN),
        0xd000 => return Ok(Numeric::POSITIVE_INFINITY),
        0xf000 => return Ok(Numeric::NEGATIVE_INFINITY),
        0 | 0x4000 => {}
        _ => return Err(invalid()),
    }
    let group = |position: i32| -> i16 {
        usize::try_from(weight - position)
            .ok()
            .and_then(|i| groups.get(i).copied())
            .unwrap_or(0)
    };
    let mut digits = String::new();
    for position in (0..=weight.max(0)).rev() {
        digits.push_str(&format!("{:04}", group(position)));
    }
    let integer_len = digits.len();
    for position in 1..=scale.div_ceil(4) {
        digits.push_str(&format!("{:04}", group(-(position as i32))));
    }
    digits.truncate(integer_len + scale as usize);
    Numeric::finite(sign == 0x4000, &digits, scale)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn binary_scalar_edges_and_numeric_scale() {
        assert_eq!(
            binary(&PgType::Int8, &Type::INT8, &i64::MIN.to_be_bytes()).unwrap(),
            Value::Int8(i64::MIN)
        );
        assert_eq!(
            binary(
                &PgType::Float8,
                &Type::FLOAT8,
                &0x7ff8000000000042u64.to_be_bytes()
            )
            .unwrap(),
            Value::Float8(0x7ff8000000000042)
        );
        let fields = [2i16, -1, 0x4000, 6, 12, 3400];
        let bytes: Vec<u8> = fields.into_iter().flat_map(i16::to_be_bytes).collect();
        assert_eq!(
            binary(&PgType::Numeric, &Type::NUMERIC, &bytes).unwrap(),
            Value::Numeric(Numeric::parse("-0.001234").unwrap())
        );
        assert!(binary(&PgType::Int4, &Type::INT4, &[0]).is_err());
    }
}
