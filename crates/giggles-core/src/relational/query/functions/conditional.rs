//! Scalar comparison functions and PostgreSQL width_bucket overloads.
use super::*;

pub(in crate::relational::query) const NULLS_VARIADIC: &str = "\0num_nulls_variadic";
pub(in crate::relational::query) const NONNULLS_VARIADIC: &str = "\0num_nonnulls_variadic";

pub(super) fn count_nulls(name: &str) -> bool {
    matches!(
        name,
        "num_nulls" | "num_nonnulls" | NULLS_VARIADIC | NONNULLS_VARIADIC
    )
}

pub(super) fn variadic(name: &str) -> bool {
    matches!(name, NULLS_VARIADIC | NONNULLS_VARIADIC)
}

fn array_element(ty: &DataType) -> R<DataType> {
    match ty {
        DataType::IntArray => Ok(DataType::Integer),
        DataType::BigIntArray => Ok(DataType::BigInt),
        DataType::TextArray => Ok(DataType::Text),
        _ => Err(SqlError::UndefinedFunction),
    }
}

fn nullif_types(a: Option<DataType>, b: Option<DataType>) -> R<(Vec<DataType>, DataType)> {
    use DataType::*;
    let normalize = |ty: DataType| if ty == Varchar { Text } else { ty };
    let a = a.map(normalize);
    let b = b.map(normalize);
    let a = a.clone().or_else(|| b.clone()).unwrap_or(Text);
    let b = b.unwrap_or_else(|| a.clone());
    let (left, right) = match (&a, &b) {
        (a, b) if a == b => (a.clone(), b.clone()),
        (a, b) if integer(a) && integer(b) => (a.clone(), b.clone()),
        (Real, DoublePrecision) | (DoublePrecision, Real) => (a.clone(), b.clone()),
        (Real, b) if number(b) => (Real, DoublePrecision),
        _ => {
            let ty = comparison_type(&a, &b).ok_or(SqlError::UndefinedFunction)?;
            (ty.clone(), ty)
        }
    };
    Ok((vec![left.clone(), right], left))
}

fn bucket_types(types: &[Option<DataType>]) -> R<(Vec<DataType>, DataType)> {
    use DataType::*;
    let signature = match types {
        [operand, Some(array)] => {
            let element = array_element(array)?;
            let operand = match operand {
                None => element,
                Some(t) if *t == element => t.clone(),
                // anycompatible selects a common type for the operand and the
                // element; unlike a mixed comparison, a real operand stays real.
                Some(t @ (Real | DoublePrecision | Numeric)) if integer(&element) => t.clone(),
                Some(BigInt) if element == Integer => BigInt,
                Some(t) if integer(t) && integer(&element) => element,
                Some(Varchar) if element == Text => Text,
                _ => return Err(SqlError::UndefinedFunction),
            };
            vec![operand, array.clone()]
        }
        [operand, low, high, count] => {
            let numbers = [operand.as_ref(), low.as_ref(), high.as_ref()];
            if !numbers.into_iter().flatten().all(number)
                || count
                    .as_ref()
                    .is_some_and(|t| !matches!(t, SmallInt | Integer))
            {
                return Err(SqlError::UndefinedFunction);
            }
            let ty = if numbers.contains(&Some(&Numeric))
                && !numbers
                    .into_iter()
                    .flatten()
                    .any(|t| matches!(t, Real | DoublePrecision))
            {
                Numeric
            } else {
                DoublePrecision
            };
            vec![ty.clone(), ty.clone(), ty, Integer]
        }
        _ => return Err(SqlError::UndefinedFunction),
    };
    Ok((signature, Integer))
}

fn types(
    name: &str,
    args: &[Scalar],
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
    parameters: &[Option<DataType>],
) -> R<(Vec<DataType>, DataType)> {
    if name == "nullif" {
        let [a, b] = args else {
            return Err(SqlError::UndefinedFunction);
        };
        let bind_known = |arg: &Scalar| -> R<Option<BoundScalar>> {
            if arg.aggregate_unknown(parameters) {
                Ok(None)
            } else {
                arg.bind(resolve, tables, parameters).map(Some)
            }
        };
        let (a, b) = (bind_known(a)?, bind_known(b)?);
        let left = a.as_ref().map_or("unknown", json_aggregate::type_name);
        let right = b.as_ref().map_or("unknown", json_aggregate::type_name);
        if left == "json" || right == "json" {
            user_types::view_replace_error(format!("operator does not exist: {left} = {right}"));
            user_types::sql_error_hint(
                "No operator matches the given name and argument types. You might need to add explicit type casts.",
            );
            return Err(SqlError::UndefinedFunction);
        }
        return nullif_types(a.map(|a| a.ty), b.map(|b| b.ty));
    }
    let types = args
        .iter()
        .map(|arg| {
            if name == "width_bucket" && args.len() == 4 && numeric_literal(arg).is_some() {
                Ok(Some(DataType::Numeric))
            } else {
                arithmetic::known(arg, resolve, tables, parameters)
            }
        })
        .collect::<R<Vec<_>>>()?;
    bucket_types(&types)
}

// Leave ordinary literals on the usual expression path. Represent a wider
// standalone numeric as compact text until width_bucket can consume it.
pub(super) fn parse_wide_bucket_literal(p: &mut Parser) -> Option<Scalar> {
    let mut at = p.at;
    let sign = match p.tokens.get(at) {
        Some(Token::Op(op)) if matches!(op.as_str(), "-" | "+") => {
            at += 1;
            op.as_str()
        }
        _ => "",
    };
    let Some(Token::Number(text)) = p.tokens.get(at) else {
        return None;
    };
    if p.tokens.get(at + 1) != Some(&Token::Symbol(',')) {
        return None;
    }
    let text = format!("{sign}{text}");
    if !matches!(Numeric::parse(&text), Err(giggles_pg_types::Error::Limit)) {
        return None;
    }
    p.at = at + 1;
    Some(Scalar::Cast(Box::new(Scalar::Literal(Value::Text(text))), DataType::Numeric))
}

fn numeric_literal(arg: &Scalar) -> Option<&str> {
    if let Scalar::Cast(input, DataType::Numeric) = arg
        && let Scalar::Literal(Value::Text(text)) = input.as_ref()
    {
        Some(text)
    } else {
        None
    }
}

pub(super) fn bind(
    name: &str,
    args: &[Scalar],
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
    parameters: &[Option<DataType>],
) -> R<BoundScalar> {
    let (args, ty) = if count_nulls(name) {
        if args.is_empty() {
            user_types::view_replace_error(format!("function {name}() does not exist"));
            user_types::sql_error_hint(
                "No function matches the given name and argument types. You might need to add explicit type casts.",
            );
            return Err(SqlError::UndefinedFunction);
        }
        let args = args
            .iter()
            .map(|a| a.bind(resolve, tables, parameters))
            .collect::<R<Vec<_>>>()?;
        if variadic(name) {
            let [array] = args.as_slice() else {
                return Err(SqlError::UndefinedFunction);
            };
            if array_element(&array.ty).is_err() {
                user_types::view_replace_error("VARIADIC argument must be an array".into());
                return Err(SqlError::Type);
            }
        }
        (args, DataType::Integer)
    } else {
        let (targets, ty) = types(name, args, resolve, tables, parameters)?;
        let args = args
            .iter()
            .zip(&targets)
            .map(|(arg, target)| {
                let bound = arithmetic::operand(arg, target, resolve, tables, parameters);
                if name == "width_bucket"
                    && *target == DataType::Numeric
                    && matches!(bound, Err(SqlError::Limit | SqlError::Overflow))
                    && let Some(text) = numeric_literal(arg)
                {
                    return Ok(BoundScalar {
                        ty: DataType::Text,
                        node: Node::Literal(Value::Text(text.into())),
                    });
                }
                bound.inspect_err(|error| {
                    if *error == SqlError::InvalidTextRepresentation
                        && let Scalar::Literal(Value::Text(text)) = arg
                        && integer(target)
                    {
                        user_types::view_replace_error(format!(
                            "invalid input syntax for type {}: \"{text}\"",
                            typeof_expr::name(target),
                        ));
                    }
                })
            })
            .collect::<R<Vec<_>>>()?;
        if name == "nullif" {
            collation::pair(&args[0], &args[1])?;
        }
        (args, ty)
    };
    Ok(BoundScalar {
        ty,
        node: Node::Call(name.into(), args),
    })
}

pub(super) fn infer(
    name: &str,
    args: &[Scalar],
    parameters: &mut [Option<DataType>],
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
) -> R<()> {
    for arg in args {
        arg.infer(None, parameters, resolve, tables)?;
    }
    if !count_nulls(name) {
        let (targets, _) = types(name, args, resolve, tables, parameters)?;
        for (arg, target) in args.iter().zip(targets) {
            if arg.aggregate_unknown(parameters) {
                arg.infer(Some(&target), parameters, resolve, tables)?;
            }
        }
    }
    Ok(())
}

pub(super) fn eval(
    name: &str,
    args: &[BoundScalar],
    mut values: Vec<Value>,
    budget: &mut usize,
) -> R<Value> {
    if name == "nullif" {
        let [a, b] = values.as_slice() else {
            return Err(SqlError::InternalError);
        };
        if *a != Value::Null && *b != Value::Null {
            let equal = if args[0].ty == DataType::Jsonb {
                jsonb_mutation::equal(a, b, budget)?
            } else if matches!(
                (a, b),
                (Value::Float4(_), Value::Float8(_)) | (Value::Float8(_), Value::Float4(_))
            ) {
                comparison::compare_cross(a, b, None)?.is_eq()
            } else {
                collation::compare_pair(&args[0], &args[1], a, b)?.is_eq()
            };
            if equal {
                return Ok(Value::Null);
            }
        }
        return Ok(values.swap_remove(0));
    }
    let result = if count_nulls(name) {
        let count_null = matches!(name, "num_nulls" | NULLS_VARIADIC);
        (if variadic(name) {
            match &values[0] {
                Value::Null => return Ok(Value::Null),
                Value::Text(array) => logical_types::array_values(array)?
                    .iter()
                    .filter(|v| v.is_none() == count_null)
                    .count(),
                _ => return Err(SqlError::InternalError),
            }
        } else {
            values
                .iter()
                .filter(|v| (**v == Value::Null) == count_null)
                .count()
        }) as i64
    } else {
        if values.contains(&Value::Null) {
            return Ok(Value::Null);
        }
        match values.as_slice() {
            [operand, Value::Text(array)] => {
                i64::from(array_bucket(operand, array, &args[0], &args[1])?)
            }
            [
                Value::Float8(x),
                Value::Float8(low),
                Value::Float8(high),
                Value::Int(count),
            ] => i64::from(float_bucket(
                x.value(),
                low.value(),
                high.value(),
                *count as i32,
            )?),
            [x, low, high, Value::Int(count)] => {
                i64::from(numeric_bucket([x, low, high], *count as i32)?)
            }
            _ => return Err(SqlError::InternalError),
        }
    };
    let value = Value::Int(result);
    charge(budget, &value)?;
    Ok(value)
}

fn invalid_bucket(message: &str) -> SqlError {
    user_types::view_replace_error(message.into());
    SqlError::InvalidArgumentForWidthBucket
}

fn final_bucket(count: i32) -> R<i32> {
    count.checked_add(1).ok_or_else(|| {
        user_types::view_replace_error("integer out of range".into());
        SqlError::Overflow
    })
}

fn check_bucket(count: i32, nan: bool, finite_bounds: bool, equal_bounds: bool) -> R<()> {
    let message = if count <= 0 {
        "count must be greater than zero"
    } else if nan {
        "operand, lower bound, and upper bound cannot be NaN"
    } else if !finite_bounds {
        "lower and upper bounds must be finite"
    } else if equal_bounds {
        "lower bound cannot equal upper bound"
    } else {
        return Ok(());
    };
    Err(invalid_bucket(message))
}

fn float_bucket(x: f64, mut low: f64, mut high: f64, count: i32) -> R<i32> {
    check_bucket(
        count,
        [x, low, high].iter().any(|v| v.is_nan()),
        low.is_finite() && high.is_finite(),
        low == high,
    )?;
    let descending = low > high;
    if (x < low) != descending && x != low {
        return Ok(0);
    }
    if (!descending && x >= high) || (descending && x <= high) {
        return final_bucket(count);
    }
    if (high - low).is_infinite() {
        low *= 0.5;
        high *= 0.5;
        return Ok(1 + (((x * 0.5 - low) / (high - low) * f64::from(count)) as i32).min(count - 1));
    }
    let fraction = (x - low) / (high - low);
    Ok(1 + ((fraction * f64::from(count)) as i32).min(count - 1))
}

fn numeric_bucket([x, low, high]: [&Value; 3], count: i32) -> R<i32> {
    use giggles_pg_types::{BucketError, BucketOperand, numeric_width_bucket};
    fn operand(value: &Value) -> R<BucketOperand<'_>> {
        match value {
            Value::Numeric(value) => Ok(BucketOperand::Numeric(value)),
            Value::Text(value) => Ok(BucketOperand::Literal(value)),
            _ => Err(SqlError::InternalError),
        }
    }
    numeric_width_bucket([operand(x)?, operand(low)?, operand(high)?], count).map_err(|error| {
        match error {
            BucketError::Input(error) => pg_error(error),
            BucketError::InvalidCount => invalid_bucket("count must be greater than zero"),
            BucketError::Nan => invalid_bucket("operand, lower bound, and upper bound cannot be NaN"),
            BucketError::InfiniteBound => invalid_bucket("lower and upper bounds must be finite"),
            BucketError::EqualBounds => invalid_bucket("lower bound cannot equal upper bound"),
            BucketError::BucketOverflow => {
                user_types::view_replace_error("integer out of range".into());
                SqlError::Overflow
            }
        }
    })
}

fn array_bucket(
    operand: &Value,
    array: &str,
    input: &BoundScalar,
    thresholds: &BoundScalar,
) -> R<i32> {
    let values = logical_types::array_values(array)?;
    if values.iter().any(Option::is_none) {
        user_types::view_replace_error("thresholds array must not contain NULLs".into());
        return Err(SqlError::NullValueNotAllowed);
    }
    let mut low = 0;
    let mut high = values.len();
    let collation = if input.ty == DataType::Text {
        collation::derive([input, thresholds].into_iter())?
    } else {
        None
    };
    while low < high {
        let middle = low + (high - low) / 2;
        let value = values[middle].as_ref().ok_or(SqlError::InternalError)?;
        let order = match operand {
            Value::Int(operand) => {
                operand.cmp(&value.parse().map_err(|_| SqlError::InternalError)?)
            }
            Value::Numeric(operand) => {
                operand.cmp(&Numeric::parse(value).map_err(pg_error)?)
            }
            Value::Float4(operand) => operand.cmp(&Float4::from_bits(
                value.parse::<f32>().map_err(|_| SqlError::InternalError)?.to_bits(),
            )),
            Value::Float8(operand) => operand.cmp(&Float8::from_bits(
                value.parse::<f64>().map_err(|_| SqlError::InternalError)?.to_bits(),
            )),
            Value::Text(operand) => collation.map_or_else(
                || operand.cmp(value),
                |name| {
                    collation::compare(
                        Some(name),
                        &Value::Text(operand.clone()),
                        &Value::Text(value.clone()),
                        &DataType::Text,
                    )
                },
            ),
            _ => return Err(SqlError::InternalError),
        };
        if order.is_lt() {
            high = middle;
        } else {
            low = middle + 1;
        }
    }
    i32::try_from(low).map_err(|_| SqlError::Overflow)
}
