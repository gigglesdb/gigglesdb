//! Containment and overlap of flat UUID arrays. Null elements never match.
use super::*;
use std::collections::BTreeSet;

pub(super) fn eval(op: &str, values: &[Value]) -> R<Value> {
    let [Value::Text(left), Value::Text(right)] = values else {
        return Err(SqlError::Type);
    };
    let (haystack, needles) = if op == "<@" {
        (right, left)
    } else {
        (left, right)
    };
    let haystack: BTreeSet<_> = logical_types::array_values(haystack)?
        .into_iter()
        .flatten()
        .collect();
    let needles = logical_types::array_values(needles)?;
    let contains = |needle: Option<String>| needle.is_some_and(|item| haystack.contains(&item));
    Ok(Value::Bool(if op == "&&" {
        needles.into_iter().any(contains)
    } else {
        needles.into_iter().all(contains)
    }))
}

pub(super) fn cannot_cast(source: &DataType, target: &DataType) -> SqlError {
    user_types::view_replace_error(format!(
        "cannot cast type {} to {}",
        typeof_expr::name(source),
        typeof_expr::name(target),
    ));
    SqlError::CannotCoerce
}

pub(super) fn check_cast(source: &DataType, target: &DataType) -> R<()> {
    if matches!(
        (source, target),
        (
            DataType::UuidArray,
            DataType::IntArray | DataType::BigIntArray
        ) | (
            DataType::IntArray | DataType::BigIntArray,
            DataType::UuidArray
        )
    ) {
        Err(cannot_cast(source, target))
    } else {
        Ok(())
    }
}

// ON predicates bind directly rather than passing through the SELECT apply
// planner, which evaluates ordinary quantified predicates outside of the join.
const EQ_ANY: &str = "\0uuid_eq_any";
const NE_ALL: &str = "\0uuid_ne_all";
const EQ_ALL: &str = "\0uuid_eq_all";

pub(in crate::relational::query) fn name(op: &str, all: bool) -> Option<&'static str> {
    match (op, all) {
        ("=", false) => Some(EQ_ANY),
        ("=", true) => Some(EQ_ALL),
        ("<>" | "!=", true) => Some(NE_ALL),
        _ => None,
    }
}

pub(super) fn is_quantified(name: &str) -> bool {
    matches!(name, EQ_ANY | EQ_ALL | NE_ALL)
}

pub(in crate::relational::query) fn check_element(
    op: &str,
    left: Option<&DataType>,
    right: &DataType,
) -> R<()> {
    if let Some(left) = left
        && (*left == DataType::Uuid || *right == DataType::Uuid)
        && left != right
    {
        return Err(arithmetic::operator_error(
            if op == "!=" { "<>" } else { op },
            Some(Some(left)),
            Some(right),
        ));
    }
    Ok(())
}

pub(super) fn infer_quantified(
    name: &str,
    args: &[Scalar],
    parameters: &mut [Option<DataType>],
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
) -> R<()> {
    let [value, array] = args else {
        return Err(SqlError::InternalError);
    };
    array.infer(None, parameters, resolve, tables)?;
    check_array(name, value, array, resolve, tables, parameters)?;
    value.infer(Some(&DataType::Uuid), parameters, resolve, tables)
}

fn check_array(
    name: &str,
    value: &Scalar,
    array: &Scalar,
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
    parameters: &[Option<DataType>],
) -> R<BoundScalar> {
    let array = array.bind(resolve, tables, parameters)?;
    if let Some(element) = array_constructor::element_type(&array.ty) {
        let left = arithmetic::known(value, resolve, tables, parameters)?;
        check_element(
            if name == NE_ALL { "<>" } else { "=" },
            left.as_ref(),
            &element,
        )?;
    }
    if array.ty != DataType::UuidArray {
        return Err(SqlError::Unsupported);
    }
    Ok(array)
}

pub(super) fn bind_quantified(
    name: &str,
    args: &[Scalar],
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
    parameters: &[Option<DataType>],
) -> R<BoundScalar> {
    let [value, array] = args else {
        return Err(SqlError::InternalError);
    };
    let array = check_array(name, value, array, resolve, tables, parameters)?;
    let value = arithmetic::operand(value, &DataType::Uuid, resolve, tables, parameters)?;
    Ok(BoundScalar {
        ty: DataType::Boolean,
        node: Node::Call(name.into(), vec![value, array]),
    })
}

pub(super) fn eval_quantified(name: &str, values: &[Value]) -> R<Value> {
    let [value, array] = values else {
        return Err(SqlError::InternalError);
    };
    let Value::Text(array) = array else {
        return Ok(Value::Null);
    };
    let elements = logical_types::array_values(array)?;
    let all = name != EQ_ANY;
    if elements.is_empty() {
        return Ok(Value::Bool(all));
    }
    let Value::Text(value) = value else {
        return Ok(Value::Null);
    };
    let decisive = elements.iter().flatten().any(|element| {
        let equal = element == value;
        (equal != (name == NE_ALL)) != all
    });
    if decisive {
        Ok(Value::Bool(!all))
    } else if elements.contains(&None) {
        Ok(Value::Null)
    } else {
        Ok(Value::Bool(all))
    }
}
