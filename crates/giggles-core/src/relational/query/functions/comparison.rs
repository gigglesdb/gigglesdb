//! Implicit comparison operators, deliberately separate from explicit casts.
use super::*;
use crate::relational::clock::ExecutionClock;
use giggles_pg_types::temporal::{Date, Interval, TimeTz, Timestamp, TimestampTz};

pub(in crate::relational::query) fn comparable(a: &DataType, b: &DataType) -> bool {
    if *a == DataType::Bpchar || *b == DataType::Bpchar {
        return false;
    }
    if matches!(a, DataType::Text | DataType::Varchar)
        && matches!(b, DataType::Text | DataType::Varchar)
    {
        return true;
    }
    a == b || comparison_type(a, b).is_some() || temporal_pair(a, b)
}
fn temporal_pair(a: &DataType, b: &DataType) -> bool {
    matches!(
        (a, b),
        (DataType::Date, DataType::Timestamp | DataType::TimestampTz)
            | (DataType::Timestamp, DataType::Date | DataType::TimestampTz)
            | (DataType::TimestampTz, DataType::Date | DataType::Timestamp)
            | (DataType::Time, DataType::TimeTz | DataType::Interval)
            | (DataType::TimeTz | DataType::Interval, DataType::Time)
    )
}
pub(in crate::relational::query) fn cross_compare(
    op: &str,
    a: BoundScalar,
    b: BoundScalar,
) -> R<Option<BoundScalar>> {
    if !temporal_pair(&a.ty, &b.ty) {
        return Ok(None);
    }
    if !matches!(op, "=" | "<>" | "!=" | "<" | "<=" | ">" | ">=") {
        return Err(SqlError::UndefinedFunction);
    }
    Ok(Some(BoundScalar {
        ty: DataType::Boolean,
        node: Node::CrossCompare(op.to_owned(), Box::new(a), Box::new(b), None),
    }))
}
// Infinities bracket even finite dates and zone conversions outside the
// timestamp domain. i128 keeps the full DATE domain exact.
fn local_value(v: &Value) -> Option<i128> {
    match v {
        Value::Date(d) if *d == Date::NEG_INFINITY => Some(i128::MIN),
        Value::Date(d) if *d == Date::INFINITY => Some(i128::MAX),
        Value::Date(d) => Some(i128::from(d.days()) * 86_400_000_000),
        Value::Timestamp(t) => Some(timestamp_value(t.microseconds())),
        _ => None,
    }
}
fn timestamp_value(v: i64) -> i128 {
    match v {
        i64::MIN => i128::MIN,
        i64::MAX => i128::MAX,
        _ => i128::from(v),
    }
}
fn instant_value(v: &Value, clock: Option<&ExecutionClock>) -> R<i128> {
    if let Value::TimestampTz(t) = v {
        return Ok(timestamp_value(t.microseconds()));
    }
    let local = local_value(v).ok_or(SqlError::UndefinedFunction)?;
    let (_, zone) = clock.ok_or(SqlError::Unsupported)?.comparison_context()?;
    if local == i128::MIN || local == i128::MAX {
        return Ok(local);
    }
    // DATE can extend far beyond timestamp; a timezone cannot bring such a
    // value into the representable timestamp range.
    let timestamp = i64::try_from(local)
        .ok()
        .and_then(|v| Timestamp::from_microseconds(v).ok());
    let Some(timestamp) = timestamp else {
        return Ok(if local < 0 {
            i128::MIN + 1
        } else {
            i128::MAX - 1
        });
    };
    match zone.to_instant(timestamp) {
        Ok(t) => Ok(timestamp_value(t.microseconds())),
        Err(giggles_pg_types::Error::Range) => Ok(if local < 0 {
            i128::MIN + 1
        } else {
            i128::MAX - 1
        }),
        Err(e) => Err(pg_error(e)),
    }
}
pub(in crate::relational::query) fn compare_cross(
    a: &Value,
    b: &Value,
    clock: Option<&ExecutionClock>,
) -> R<Ordering> {
    if let (Some(a), Some(b)) = (local_value(a), local_value(b)) {
        return Ok(a.cmp(&b));
    }
    if matches!(a, Value::TimestampTz(_)) && local_value(b).is_some()
        || matches!(b, Value::TimestampTz(_)) && local_value(a).is_some()
    {
        return Ok(instant_value(a, clock)?.cmp(&instant_value(b, clock)?));
    }
    match (a, b) {
        (Value::Time(t), Value::Interval(i)) => {
            return Ok(Interval::new(0, 0, t.microseconds()).cmp(i));
        }
        (Value::Interval(_), Value::Time(_)) => {
            return compare_cross(b, a, clock).map(Ordering::reverse);
        }
        (Value::Time(t), Value::TimeTz(other)) => {
            let (transaction, zone) = clock.ok_or(SqlError::Unsupported)?.comparison_context()?;
            let local = zone
                .to_local(TimestampTz::from_microseconds(transaction).map_err(pg_error)?)
                .map_err(pg_error)?;
            let date = Date::from_days(
                i32::try_from(local.microseconds().div_euclid(86_400_000_000))
                    .map_err(|_| SqlError::DatetimeFieldOverflow)?,
            )
            .map_err(pg_error)?;
            return Ok(TimeTz::in_zone_on_date(*t, date, zone)
                .map_err(pg_error)?
                .cmp(other));
        }
        (Value::TimeTz(_), Value::Time(_)) => {
            return compare_cross(b, a, clock).map(Ordering::reverse);
        }
        _ => {}
    }
    fn ty(v: &Value) -> Option<DataType> {
        Some(match v {
            Value::Int(_) => DataType::BigInt,
            Value::Numeric(_) => DataType::Numeric,
            Value::Float4(_) => DataType::Real,
            Value::Float8(_) => DataType::DoublePrecision,
            _ => return None,
        })
    }
    if let (Some(a_ty), Some(b_ty)) = (ty(a), ty(b)) {
        let target = comparison_type(&a_ty, &b_ty).ok_or(SqlError::UndefinedFunction)?;
        let a = cast_value(a.clone(), &target, &mut 0)?;
        let b = cast_value(b.clone(), &target, &mut 0)?;
        return Ok(cmp(&a, &b));
    }
    // Same-type comparisons retain the existing ordering implementation.
    if std::mem::discriminant(a) == std::mem::discriminant(b) {
        return Ok(cmp(a, b));
    }
    Err(SqlError::UndefinedFunction)
}

fn row_operands(
    arg: &BoundScalar,
) -> R<(&BoundScalar, &BoundScalar, Option<&ExecutionClock>)> {
    use super::super::BoundFilter;
    let Node::Predicate(filter) = &arg.node else {
        return Err(SqlError::InternalError);
    };
    match filter.as_ref() {
        BoundFilter::Compare(a, _, b) => Ok((a, b, None)),
        BoundFilter::Scalar(BoundScalar { node: Node::CrossCompare(_, a, b, clock), .. }) => {
            Ok((a, b, clock.as_ref()))
        }
        _ => Err(SqlError::InternalError),
    }
}

pub(super) fn check_row_order(op: &str, args: &[BoundScalar]) -> R<()> {
    if matches!(op, "<" | "<=" | ">" | ">=") {
        for arg in args {
            let (left, right, _) = row_operands(arg)?;
            if [left, right].iter().any(|arg| matches!(
                arg.ty,
                DataType::IntArray | DataType::BigIntArray | DataType::TextArray | DataType::UuidArray
                    | DataType::Jsonb | DataType::RegType
            )) {
                return Err(SqlError::Unsupported);
            }
        }
    }
    Ok(())
}

pub(super) fn eval_row_order(
    op: &str,
    args: &[BoundScalar],
    row: &Tuple,
    params: &[Value],
    budget: &mut usize,
) -> R<Value> {
    let distinct = match op {
        "distinct" => Some(true),
        "not_distinct" => Some(false),
        _ => None,
    };
    for arg in args {
        let (a, b, clock) = row_operands(arg)?;
        let left = a.eval(row, params, budget)?;
        let right = b.eval(row, params, budget)?;
        if left == Value::Null || right == Value::Null {
            match distinct {
                None => return Ok(Value::Null),
                Some(result) if left != right => return Ok(Value::Bool(result)),
                Some(_) => continue,
            }
        }
        let order = if temporal_pair(&a.ty, &b.ty) {
            compare_cross(&left, &right, clock)?
        } else {
            collation::compare_pair(a, b, &left, &right)?
        };
        if !order.is_eq() {
            return Ok(Value::Bool(distinct.unwrap_or(order.is_lt() == op.starts_with('<'))));
        }
    }
    Ok(Value::Bool(distinct.map_or_else(|| op.ends_with('='), |result| !result)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relational::clock::StatementClock;
    use giggles_pg_types::temporal::SessionZone;
    fn value(s: &str, ty: DataType) -> Value {
        cast_value(Value::Text(s.into()), &ty, &mut 0).unwrap()
    }
    fn clock(zone: &str, instant: &str) -> ExecutionClock {
        let Value::TimestampTz(t) = value(instant, DataType::TimestampTz) else {
            unreachable!()
        };
        ExecutionClock::new(&StatementClock {
            unavailable: None,
            transaction_us: t.microseconds(),
            statement_us: t.microseconds(),
            zone: Some(SessionZone::named(zone).unwrap()),
            wall: None,
        })
    }
    fn both(a: Value, b: Value, clock: Option<&ExecutionClock>, expected: Ordering) {
        assert_eq!(compare_cross(&a, &b, clock), Ok(expected));
        assert_eq!(compare_cross(&b, &a, clock), Ok(expected.reverse()));
    }
    #[test]
    fn temporal_pairs_and_infinities() {
        let c = clock("UTC", "2024-01-15 00:00:00+00");
        for (a, b) in [
            (DataType::Date, DataType::Timestamp),
            (DataType::Date, DataType::TimestampTz),
            (DataType::Timestamp, DataType::TimestampTz),
        ] {
            both(
                value("2024-01-01", a.clone()),
                value("2024-01-01", b.clone()),
                Some(&c),
                Ordering::Equal,
            );
            for inf in ["infinity", "-infinity"] {
                both(
                    value(inf, a.clone()),
                    value(inf, b.clone()),
                    Some(&c),
                    Ordering::Equal,
                );
            }
        }
        both(
            value("24:00", DataType::Time),
            value("1 day", DataType::Interval),
            None,
            Ordering::Equal,
        );
        both(
            value("5874897-12-31", DataType::Date),
            value("294276-12-31 23:59:59", DataType::Timestamp),
            None,
            Ordering::Greater,
        );
        both(
            value("5874897-12-31", DataType::Date),
            value("infinity", DataType::TimestampTz),
            Some(&c),
            Ordering::Less,
        );
    }
    #[test]
    fn session_zone_dst_and_boundary_overflow() {
        let c = clock("America/Chicago", "2024-01-15 00:00:00+00");
        for (local, utc) in [
            ("2024-03-10 02:30:00", "2024-03-10 08:30:00+00"),
            ("2024-11-03 01:30:00", "2024-11-03 07:30:00+00"),
        ] {
            both(
                value(local, DataType::Timestamp),
                value(utc, DataType::TimestampTz),
                Some(&c),
                Ordering::Equal,
            );
        }
        both(
            value("294276-12-31 23:00:00", DataType::Timestamp),
            value("294276-12-31 23:59:59+00", DataType::TimestampTz),
            Some(&c),
            Ordering::Greater,
        );
        both(
            value("294276-12-31 23:00:00", DataType::Timestamp),
            value("infinity", DataType::TimestampTz),
            Some(&c),
            Ordering::Less,
        );
        let k = clock("Asia/Kolkata", "2024-01-15 00:00:00+00");
        both(
            value("4714-11-24 00:00:00 BC", DataType::Timestamp),
            value("4714-11-24 00:00:00+00 BC", DataType::TimestampTz),
            Some(&k),
            Ordering::Less,
        );
        both(
            value("4714-11-24 00:00:00 BC", DataType::Timestamp),
            value("-infinity", DataType::TimestampTz),
            Some(&k),
            Ordering::Greater,
        );
    }
    #[test]
    fn time_zone_uses_transaction_date() {
        for (instant, offset) in [
            ("2024-01-15 00:00:00+00", "12:00:00-06"),
            ("2024-07-15 00:00:00+00", "12:00:00-05"),
        ] {
            let c = clock("America/Chicago", instant);
            both(
                value("12:00", DataType::Time),
                value(offset, DataType::TimeTz),
                Some(&c),
                Ordering::Equal,
            );
        }
        assert_eq!(
            compare_cross(
                &value("12:00", DataType::Time),
                &value("12:00+00", DataType::TimeTz),
                None
            ),
            Err(SqlError::Unsupported)
        );
    }
    #[test]
    fn numeric_exact_and_float_promotion() {
        both(
            Value::Int(9007199254740993),
            value("9007199254740992", DataType::Numeric),
            None,
            Ordering::Greater,
        );
        for a in [
            Value::Int(3),
            value("3", DataType::Numeric),
            value("3", DataType::Real),
            value("3", DataType::DoublePrecision),
        ] {
            for b in [
                Value::Int(3),
                value("3", DataType::Numeric),
                value("3", DataType::Real),
                value("3", DataType::DoublePrecision),
            ] {
                both(a.clone(), b, None, Ordering::Equal);
            }
        }
    }
    #[test]
    fn node_operators_null_and_clock_attachment() {
        let literal = |ty, v| BoundScalar {
            ty,
            node: Node::Literal(v),
        };
        let row = Tuple {
            source_versions: vec![],
            values: vec![],
            version: 0,
        };
        for (op, expected) in [
            ("=", false),
            ("<>", true),
            ("!=", true),
            ("<", true),
            ("<=", true),
            (">", false),
            (">=", false),
        ] {
            let node = cross_compare(
                op,
                literal(DataType::Date, value("2024-01-01", DataType::Date)),
                literal(
                    DataType::Timestamp,
                    value("2024-01-02", DataType::Timestamp),
                ),
            )
            .unwrap()
            .unwrap();
            assert_eq!(node.eval(&row, &[], &mut 0), Ok(Value::Bool(expected)));
        }
        let node = cross_compare(
            "=",
            literal(DataType::Date, Value::Null),
            literal(
                DataType::TimestampTz,
                value("2024-01-01 00:00:00+00", DataType::TimestampTz),
            ),
        )
        .unwrap()
        .unwrap();
        assert_eq!(node.eval(&row, &[], &mut 0), Ok(Value::Null));
        let mut node = cross_compare(
            "=",
            literal(DataType::Date, value("2024-01-01", DataType::Date)),
            literal(
                DataType::TimestampTz,
                value("2024-01-01 00:00:00+00", DataType::TimestampTz),
            ),
        )
        .unwrap()
        .unwrap();
        assert_eq!(node.eval(&row, &[], &mut 0), Err(SqlError::Unsupported));
        node.attach_clock(Some(&clock("UTC", "2024-01-01 00:00:00+00")));
        assert_eq!(node.eval(&row, &[], &mut 0), Ok(Value::Bool(true)));
    }
    #[test]
    fn overflow_node_inequality() {
        let c = clock("America/Chicago", "2024-01-01 00:00:00+00");
        let row = Tuple {
            source_versions: vec![],
            values: vec![],
            version: 0,
        };
        for (op, expected) in [("=", false), ("<>", true), ("<", true)] {
            let mut node = cross_compare(
                op,
                BoundScalar {
                    ty: DataType::Timestamp,
                    node: Node::Literal(value("294276-12-31 23:00:00", DataType::Timestamp)),
                },
                BoundScalar {
                    ty: DataType::TimestampTz,
                    node: Node::Literal(value("infinity", DataType::TimestampTz)),
                },
            )
            .unwrap()
            .unwrap();
            node.attach_clock(Some(&c));
            assert_eq!(node.eval(&row, &[], &mut 0), Ok(Value::Bool(expected)));
        }
    }
    #[test]
    fn additional_promotion_and_kolkata_cases() {
        for a in [
            Value::Int(9007199254740993),
            value("9007199254740993", DataType::Numeric),
        ] {
            both(
                a,
                value("9007199254740992", DataType::DoublePrecision),
                None,
                Ordering::Equal,
            );
        }
        let k = clock("Asia/Kolkata", "2024-01-01 00:00:00+00");
        for ty in [DataType::Date, DataType::Timestamp] {
            both(
                value("2024-01-01", ty),
                value("2023-12-31 18:30:00+00", DataType::TimestampTz),
                Some(&k),
                Ordering::Equal,
            );
        }
        let d = value("5874897-12-31", DataType::Date);
        both(
            d.clone(),
            value("infinity", DataType::Timestamp),
            None,
            Ordering::Less,
        );
        both(
            d,
            value("294276-12-31 23:59:59+00", DataType::TimestampTz),
            Some(&k),
            Ordering::Greater,
        );
    }
    #[test]
    fn unavailable_clock_and_rejected_pairs() {
        let mut statement = StatementClock {
            unavailable: None,
            transaction_us: 0,
            statement_us: 0,
            zone: None,
            wall: None,
        };
        let a = value("2000-01-01", DataType::Date);
        let b = value("2000-01-01 00:00:00+00", DataType::TimestampTz);
        let missing_zone = ExecutionClock::new(&statement);
        assert_eq!(
            compare_cross(&a, &b, Some(&missing_zone)),
            Err(SqlError::Unsupported)
        );
        // Use an existing concrete caller error and verify it survives intact.
        let error = crate::CoreError::Storage("caller clock unavailable".into());
        statement.unavailable = Some(error.clone());
        let unavailable = ExecutionClock::new(&statement);
        assert_eq!(
            compare_cross(&a, &b, Some(&unavailable)),
            Err(SqlError::Unsupported)
        );
        assert_eq!(unavailable.take_error(), Some(error));
        assert!(!comparable(&DataType::TimeTz, &DataType::Interval));
        assert_eq!(
            compare_cross(
                &value("12:00+00", DataType::TimeTz),
                &value("1 day", DataType::Interval),
                None
            ),
            Err(SqlError::UndefinedFunction)
        );
        assert_eq!(
            comparison_mismatch(&DataType::Date, &DataType::Time),
            SqlError::UndefinedFunction
        );
    }
}
