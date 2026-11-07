//! Equality-preserving point probes for typed joins.
use super::*;
use crate::relational::clock::ExecutionClock;

impl BoundJoin {
    pub(in crate::relational::query) fn key(&self, row: &Tuple) -> R<Option<Vec<Value>>> {
        self.key
            .iter()
            .map(|(column, outer_ty, inner_ty)| {
                super::join_key(row.get(*column), outer_ty, inner_ty)
            })
            .collect()
    }
    pub(in crate::relational::query) fn matches(&self, row: &Tuple, inner: &[Value]) -> R<bool> {
        for (outer, column, outer_ty, inner_ty) in &self.equalities {
            if !super::join_equal(
                row.get(*outer),
                outer_ty,
                &inner[column.column],
                inner_ty,
                self.clock.as_ref(),
            )? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

fn temporal_type(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Date
            | DataType::Time
            | DataType::TimeTz
            | DataType::Timestamp
            | DataType::TimestampTz
            | DataType::Interval
    )
}

pub(in crate::relational::query) fn join_key_supported(outer: &DataType, inner: &DataType) -> bool {
    if temporal_type(outer) || temporal_type(inner) {
        return false;
    }
    if outer == inner {
        return true;
    }
    if matches!(outer, DataType::Real | DataType::DoublePrecision)
        && (integer(inner) || inner == &DataType::Numeric)
    {
        // Float comparison can match multiple distinct integer/numeric keys.
        return false;
    }
    comparison_type(outer, inner).is_some()
}

pub(in crate::relational::query) fn join_key(
    outer: &Value,
    outer_ty: &DataType,
    inner_ty: &DataType,
) -> R<Option<Value>> {
    if *outer == Value::Null || !join_key_supported(outer_ty, inner_ty) {
        return Ok(None);
    }
    if outer_ty == inner_ty {
        return Ok(Some(outer.clone()));
    }
    if integer(inner_ty) {
        let n = match outer {
            Value::Int(n) => *n,
            Value::Numeric(n) => {
                if n.scale().is_none() {
                    return Ok(None);
                }
                let integral = n.round(0).map_err(pg_error)?;
                if n != &integral {
                    return Ok(None);
                }
                let Ok(n) = integral.to_string().parse::<i64>() else {
                    return Ok(None);
                };
                n
            }
            _ => return Err(SqlError::Type),
        };
        if (inner_ty == &DataType::SmallInt && i16::try_from(n).is_err())
            || (inner_ty == &DataType::Integer && i32::try_from(n).is_err())
        {
            return Ok(None);
        }
        return Ok(Some(Value::Int(n)));
    }
    let candidate = match cast_value(outer.clone(), inner_ty, &mut 0) {
        Ok(v) => v,
        // A float4 key cannot represent a finite float8 comparison operand
        // outside its range, even though that operand is valid for comparison.
        Err(SqlError::Overflow) if inner_ty == &DataType::Real => return Ok(None),
        Err(e) => return Err(e),
    };
    if inner_ty == &DataType::Real && compare_cross(outer, &candidate, None)? != Ordering::Equal {
        return Ok(None);
    }
    Ok(Some(candidate))
}

pub(in crate::relational::query) fn join_equal(
    a: &Value,
    outer_ty: &DataType,
    b: &Value,
    inner_ty: &DataType,
    clock: Option<&ExecutionClock>,
) -> R<bool> {
    if *a == Value::Null || *b == Value::Null {
        return Ok(false);
    }
    if outer_ty == inner_ty {
        return Ok(equal(a, b));
    }
    Ok(compare_cross(a, b, clock)? == Ordering::Equal)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn numeric(s: &str) -> Value {
        Value::Numeric(Numeric::parse(s).unwrap())
    }
    fn key(v: &Value) -> Vec<u8> {
        crate::relational::encode_key(std::slice::from_ref(v)).unwrap()
    }
    #[test]
    fn integer_and_numeric_keys_are_exact() {
        let integers = [DataType::SmallInt, DataType::Integer, DataType::BigInt];
        for outer in &integers {
            for inner in &integers {
                assert!(join_key_supported(outer, inner));
                assert_eq!(
                    join_key(&Value::Int(3), outer, inner),
                    Ok(Some(Value::Int(3)))
                );
            }
            assert_eq!(
                key(&join_key(&Value::Int(3), outer, &DataType::Numeric)
                    .unwrap()
                    .unwrap()),
                key(&numeric("3.0"))
            );
            assert_eq!(
                join_key(&numeric("3.0"), &DataType::Numeric, outer),
                Ok(Some(Value::Int(3)))
            );
            for n in ["3.1", "9223372036854775808", "NaN", "Infinity"] {
                assert_eq!(join_key(&numeric(n), &DataType::Numeric, outer), Ok(None));
            }
        }
        assert_eq!(
            join_key(&Value::Int(32768), &DataType::BigInt, &DataType::SmallInt),
            Ok(None)
        );
        assert_eq!(
            join_key(
                &numeric("2147483648"),
                &DataType::Numeric,
                &DataType::Integer
            ),
            Ok(None)
        );
        let exact = 9_007_199_254_740_993;
        assert_eq!(
            join_key(
                &numeric(&exact.to_string()),
                &DataType::Numeric,
                &DataType::BigInt
            ),
            Ok(Some(Value::Int(exact)))
        );
        assert!(
            join_equal(
                &Value::Int(exact),
                &DataType::BigInt,
                &numeric(&exact.to_string()),
                &DataType::Numeric,
                None
            )
            .unwrap()
        );
        assert!(
            !join_equal(
                &Value::Int(exact),
                &DataType::BigInt,
                &numeric("9007199254740992"),
                &DataType::Numeric,
                None
            )
            .unwrap()
        );
    }
    #[test]
    fn floating_keys_follow_comparison_promotion() {
        for (v, ty) in [
            (Value::Int(3), DataType::BigInt),
            (numeric("3.0"), DataType::Numeric),
            (Value::Float4(Float4::parse("3").unwrap()), DataType::Real),
        ] {
            assert_eq!(
                join_key(&v, &ty, &DataType::DoublePrecision).unwrap(),
                Some(cast_value(v.clone(), &DataType::DoublePrecision, &mut 0).unwrap())
            );
        }
        for ty in [DataType::Real, DataType::DoublePrecision] {
            for inner in [
                DataType::SmallInt,
                DataType::Integer,
                DataType::BigInt,
                DataType::Numeric,
            ] {
                assert!(!join_key_supported(&ty, &inner));
            }
        }
        let f = |s| Value::Float8(Float8::parse(s).unwrap());
        let real = Value::Float4(Float4::parse("0.1").unwrap());
        assert_eq!(
            join_key(&real, &DataType::Real, &DataType::Real),
            Ok(Some(real.clone()))
        );
        for (v, ty) in [
            (Value::Int(3), DataType::Integer),
            (numeric("3.0"), DataType::Numeric),
        ] {
            assert_eq!(
                join_key(&v, &ty, &DataType::Real),
                Ok(Some(Value::Float4(Float4::parse("3").unwrap())))
            );
        }
        assert_eq!(
            join_key(&numeric("0.1"), &DataType::Numeric, &DataType::Real),
            Ok(None)
        );
        assert_eq!(
            join_key(&Value::Int(16_777_217), &DataType::BigInt, &DataType::Real),
            Ok(None)
        );
        assert_eq!(
            join_key(&f("1e100"), &DataType::DoublePrecision, &DataType::Real),
            Ok(None)
        );
        assert_eq!(
            join_key(&f("0.1"), &DataType::DoublePrecision, &DataType::Real),
            Ok(None)
        );
        assert_eq!(
            join_key(&f("3"), &DataType::DoublePrecision, &DataType::Real),
            Ok(Some(Value::Float4(Float4::parse("3").unwrap())))
        );
        assert_eq!(
            key(&join_key(
                &f("-0"),
                &DataType::DoublePrecision,
                &DataType::DoublePrecision
            )
            .unwrap()
            .unwrap()),
            key(&f("0"))
        );
        assert_eq!(
            key(&join_key(
                &f("NaN"),
                &DataType::DoublePrecision,
                &DataType::DoublePrecision
            )
            .unwrap()
            .unwrap()),
            key(&Value::Float8(Float8::from_bits(0x7ff0000000000001)))
        );
        for ty in [
            DataType::Date,
            DataType::Time,
            DataType::TimeTz,
            DataType::Timestamp,
            DataType::TimestampTz,
            DataType::Interval,
        ] {
            assert!(!join_key_supported(&ty, &ty));
        }
        assert!(!join_key_supported(&DataType::Text, &DataType::Integer));
        assert_eq!(
            join_key(&Value::Null, &DataType::Integer, &DataType::BigInt),
            Ok(None)
        );
        assert!(
            !join_equal(
                &Value::Null,
                &DataType::Integer,
                &Value::Null,
                &DataType::Integer,
                None
            )
            .unwrap()
        );
    }
}
