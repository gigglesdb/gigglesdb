//! Containment and overlap of flat UUID arrays. Null elements never match.
use super::*;
use std::{cmp::Ordering, collections::BTreeSet};

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
const QUANTIFIERS: &[(&str, &str, &str)] = &[
    ("=", EQ_ANY, EQ_ALL),
    ("<>", "\0enum_ne_any", NE_ALL),
    ("<", "\0enum_lt_any", "\0enum_lt_all"),
    ("<=", "\0enum_le_any", "\0enum_le_all"),
    (">", "\0enum_gt_any", "\0enum_gt_all"),
    (">=", "\0enum_ge_any", "\0enum_ge_all"),
];

pub(in crate::relational::query) fn name(op: &str, all: bool) -> Option<&'static str> {
    let op = if op == "!=" { "<>" } else { op };
    QUANTIFIERS.iter().find_map(|&(symbol, any, every)| {
        (op == symbol).then_some(if all { every } else { any })
    })
}

pub(super) fn is_quantified(name: &str) -> bool {
    QUANTIFIERS.iter().any(|&(_, any, all)| name == any || name == all)
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

/// Bind enum operands while their SQL type identities and unknown-literal
/// syntax are still available. Both enum values use the text-backed storage.
pub(in crate::relational::query) fn bind_enum(
    op: &str,
    left: &Scalar,
    right: &Scalar,
    mut value: BoundScalar,
    mut array: BoundScalar,
) -> R<(BoundScalar, BoundScalar)> {
    let left_enum = value.attached_enum().cloned();
    let right_enum = array.attached_enum().cloned();
    if let Some(operator) = match op {
        "like" | "~~" => Some("~~"),
        "ilike" | "~~*" => Some("~~*"),
        "!~~" | "!~~*" => Some(op),
        _ => None,
    } {
        let lhs = if matches!(left, Scalar::Literal(Value::Text(_) | Value::Null)) {
            "unknown".into()
        } else {
            value.type_name()
        };
        let element = array_constructor::element_type(&array.ty).ok_or(SqlError::Type)?;
        let rhs = right_enum.as_ref().map_or_else(
            || type_display(&element).into(),
            |definition| user_types::enum_display(definition, false),
        );
        return Err(enum_order::missing_operator_types(operator, &lhs, &rhs));
    }
    if value.ty == DataType::Text
        && let Some(definition) = &left_enum
        && let Scalar::Literal(literal @ (Value::Text(_) | Value::Null)) = right
    {
        array = BoundScalar::enum_literal(literal, &DataType::TextArray, definition)?;
    } else if let Some(definition) = &right_enum
        && array.ty == DataType::TextArray
        && let Scalar::Literal(literal @ (Value::Text(_) | Value::Null)) = left
    {
        value = BoundScalar::enum_literal(literal, &DataType::Text, definition)?;
    }
    if value.ty == DataType::Text && array.ty == DataType::TextArray
        && value.attached_enum().is_some() && value.attached_enum() == array.attached_enum()
    {
        return Ok((value, array));
    }
    let element = array_constructor::element_type(&array.ty).ok_or(SqlError::Type)?;
    let element = match array.attached_enum() {
        Some(definition) => BoundScalar::enum_metadata(element, definition),
        None => BoundScalar { ty: element, node: Node::Literal(Value::Null) },
    };
    Err(enum_order::missing_operator(op, &value, &element))
}

pub(in crate::relational::query) fn eval_enum(
    value: Value,
    array: Value,
    definition: &TypeDef,
    op: &str,
    all: bool,
    mut comparison: impl FnMut() -> R<()>,
) -> R<Value> {
    let Value::Text(array) = array else {
        return Ok(Value::Null);
    };
    let TypeDef::Enum { labels, .. } = definition else {
        return Err(SqlError::InternalError);
    };
    let mut unknown = false;
    for element in logical_types::array_values(&array)? {
        comparison()?;
        let (Value::Text(value), Some(element)) = (&value, element) else {
            unknown = true;
            continue;
        };
        let order = enum_order::compare_labels(labels, value, &element);
        let matched = match op {
            "=" => order == Ordering::Equal,
            "<>" | "!=" => order != Ordering::Equal,
            "<" => order == Ordering::Less,
            "<=" => order != Ordering::Greater,
            ">" => order == Ordering::Greater,
            ">=" => order != Ordering::Less,
            _ => return Err(SqlError::Unsupported),
        };
        if matched != all {
            return Ok(Value::Bool(matched));
        }
    }
    Ok(if unknown { Value::Null } else { Value::Bool(all) })
}

fn operator(name: &str) -> &str {
    quantifier(name).0
}

pub(in crate::relational::query) fn quantifier(name: &str) -> (&str, bool) {
    QUANTIFIERS.iter().find_map(|&(op, any, all)| {
        (name == any || name == all).then_some((op, name == all))
    }).expect("recognized array comparison")
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
    value.infer(None, parameters, resolve, tables)?;
    let right = array.bind(resolve, tables, parameters)?;
    if let Some(definition) = right.attached_enum() {
        value.infer_enum_parameter(&DataType::Text, Some(definition), parameters)?;
    }
    if let Ok(left) = value.bind(resolve, tables, parameters)
        && (left.attached_enum().is_some() || right.attached_enum().is_some())
    {
        bind_enum(operator(name), value, array, left, right)?;
        return Ok(());
    }
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
    if !matches!(name, EQ_ANY | EQ_ALL | NE_ALL) {
        return Err(SqlError::Unsupported);
    }
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
    let bound_array = array.bind(resolve, tables, parameters)?;
    let bound_value = value.bind(resolve, tables, parameters)?;
    if bound_value.attached_enum().is_some() || bound_array.attached_enum().is_some() {
        let (value, array) = bind_enum(operator(name), value, array, bound_value, bound_array)?;
        return Ok(BoundScalar {
            ty: DataType::Boolean,
            node: Node::Call(name.into(), vec![value, array]),
        });
    }
    let array = check_array(name, value, array, resolve, tables, parameters)?;
    let value = arithmetic::operand(value, &DataType::Uuid, resolve, tables, parameters)?;
    Ok(BoundScalar {
        ty: DataType::Boolean,
        node: Node::Call(name.into(), vec![value, array]),
    })
}

pub(super) fn eval_quantified(name: &str, values: &[Value], args: &[BoundScalar]) -> R<Value> {
    let ([value, array], [_, right]) = (values, args) else {
        return Err(SqlError::InternalError);
    };
    if let Some(definition) = right.attached_enum() {
        let (operator, all) = quantifier(name);
        return eval_enum(value.clone(), array.clone(), definition, operator, all, || Ok(()));
    }
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
