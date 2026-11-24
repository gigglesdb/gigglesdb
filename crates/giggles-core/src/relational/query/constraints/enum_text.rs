//! PostgreSQL deparses enum-to-text membership as a scalar-array comparison.
use super::*;
use std::cell::RefCell;

pub(super) const MEMBERSHIP: &str = "\0check_enum_text_membership";

#[derive(Default)]
struct Context {
    columns: BTreeSet<String>,
    expand_membership: bool,
    table: String,
}

thread_local! {
    static CONTEXT: RefCell<Context> = RefCell::default();
}

pub(super) struct Scope(Context);

impl Scope {
    pub(super) fn enter(state: &Relational, table: &TableDef, expand_membership: bool) -> Self {
        Self::with_columns(state, table, expand_membership, false)
    }

    pub(super) fn stored(state: &Relational, table: &TableDef, expand_membership: bool) -> Self {
        // An enum-to-text type change leaves the canonical CHECK expression in place.
        Self::with_columns(state, table, expand_membership, true)
    }

    fn with_columns(
        state: &Relational,
        table: &TableDef,
        expand_membership: bool,
        include_strings: bool,
    ) -> Self {
        let columns = table
            .columns
            .iter()
            .filter(|column| match (state.column_type(column.id), &column.data_type) {
                (Some(TypeDef::Enum { .. }), DataType::Text) => true,
                (None, DataType::Text | DataType::Varchar) => include_strings,
                _ => false,
            })
            .map(|column| column.name.clone())
            .collect();
        Self(CONTEXT.replace(Context {
            columns,
            expand_membership,
            table: table.name.clone(),
        }))
    }

    pub(super) fn rename_column(&mut self, old: &str, new: &str) {
        CONTEXT.with_borrow_mut(|context| {
            if context.columns.remove(old) {
                context.columns.insert(new.into());
            }
        });
    }
}

impl Drop for Scope {
    fn drop(&mut self) {
        CONTEXT.replace(std::mem::take(&mut self.0));
    }
}

pub(super) fn varchar_cast(scalar: &Scalar) -> Option<(&ColumnRef, Option<i64>)> {
    let (cast, length) = match scalar {
        Scalar::Call(name, args) if name == functions::CAST_TYPMOD => {
            let [cast, Scalar::Literal(Value::Int(modifier))] = args.as_slice() else {
                return None;
            };
            (cast, Some(modifier - 4))
        }
        cast => (cast, None),
    };
    if let Scalar::Cast(inner, DataType::Varchar) = cast
        && let Scalar::Column(column) = inner.as_ref()
        && is_character_column(column)
    {
        Some((column, length))
    } else {
        None
    }
}

pub(super) fn is_character_column(column: &ColumnRef) -> bool {
    CONTEXT.with_borrow(|context| {
        column.qualifier.as_ref().is_none_or(|name| *name == context.table)
            && context.columns.contains(&column.name)
    })
}

pub(super) fn text_operand(value: &Scalar) -> bool {
    match value {
        Scalar::Cast(inner, DataType::Text) => {
            matches!(inner.as_ref(), Scalar::Column(column) if is_character_column(column))
                || varchar_cast(inner).is_some()
        }
        Scalar::Call(name, args) if name == "lower" => {
            matches!(args.as_slice(), [argument] if text_operand(argument))
        }
        _ => false,
    }
}

pub(super) fn text_literal(value: &Scalar) -> Option<&str> {
    match value {
        Scalar::Literal(Value::Text(value)) => Some(value),
        Scalar::Cast(inner, DataType::Text) => match inner.as_ref() {
            Scalar::Literal(Value::Text(value)) => Some(value),
            _ => None,
        },
        _ => None,
    }
}

fn varchar_text_literal(value: &Scalar) -> Option<&str> {
    if let Scalar::Cast(inner, DataType::Text) = value
        && let Scalar::Cast(literal, DataType::Varchar) = inner.as_ref()
        && let Scalar::Literal(Value::Text(value)) = literal.as_ref()
    {
        Some(value)
    } else {
        None
    }
}

fn filter(lhs: Scalar, array: Scalar, all: bool) -> Filter {
    Filter::Scalar(Scalar::Call(
        MEMBERSHIP.into(),
        vec![lhs, Scalar::Literal(Value::Bool(all)), array],
    ))
}

pub(in crate::relational::query) fn membership(
    lhs: &Scalar,
    values: &[Scalar],
    negated: bool,
) -> Option<Filter> {
    let varchar = varchar_cast(lhs).is_some();
    if !varchar && !text_operand(lhs) {
        return None;
    }
    if !values
        .iter()
        .all(|value| matches!(value, Scalar::Literal(Value::Text(_))))
    {
        return None;
    }
    let lhs = if varchar {
        Scalar::Cast(Box::new(lhs.clone()), DataType::Text)
    } else {
        lhs.clone()
    };
    if values.len() == 1 {
        return Some(Filter::membership(&lhs, values, negated));
    }
    let mut array = Scalar::Call(functions::ARRAY_CONSTRUCTOR.into(), values.to_vec());
    if varchar {
        array = Scalar::Cast(Box::new(array), DataType::TextArray);
    }
    Some(filter(lhs, array, negated))
}

pub(in crate::relational::query) fn rebind(
    lhs: &Scalar,
    rhs: &Scalar,
    operator: &str,
    all: bool,
) -> R<Option<Filter>> {
    let varchar = varchar_cast(lhs).is_some();
    if (!varchar && !text_operand(lhs)) || !matches!((operator, all), ("=", false) | ("<>", true)) {
        return Ok(None);
    }
    let lhs = if varchar {
        Scalar::Cast(Box::new(lhs.clone()), DataType::Text)
    } else {
        lhs.clone()
    };
    let expand_membership = CONTEXT.with_borrow(|context| context.expand_membership);
    let (array, values) = match rhs {
        Scalar::Cast(inner, DataType::TextArray)
            if let Scalar::Literal(Value::Text(text)) = inner.as_ref() =>
        {
            let values = logical_types::array_values(text)?;
            if values.is_empty() || values.iter().any(Option::is_none) {
                return Ok(None);
            }
            let values: Vec<_> = values.into_iter().flatten().collect();
            let text = format!(
                "{{{}}}",
                values
                    .iter()
                    .map(|v| logical_types::array_quote(v))
                    .collect::<Vec<_>>()
                    .join(",")
            );
            (
                Scalar::Cast(
                    Box::new(Scalar::Literal(Value::Text(text))),
                    DataType::TextArray,
                ),
                values,
            )
        }
        rhs => {
            let (array, varchar_array) = match rhs {
                Scalar::Cast(inner, DataType::TextArray) => (inner.as_ref(), true),
                array => (array, varchar),
            };
            let Scalar::Call(name, args) = array else {
                return Ok(None);
            };
            if name != functions::ARRAY_CONSTRUCTOR || args.is_empty() {
                return Ok(None);
            }
            let Some(values) = args
                .iter()
                .map(|arg| {
                    let arg = match arg {
                        Scalar::Cast(inner, DataType::Varchar) if varchar_array => inner.as_ref(),
                        arg => arg,
                    };
                    text_literal(arg)
                        .or_else(|| varchar_text_literal(arg))
                        .map(str::to_owned)
                })
                .collect::<Option<Vec<_>>>()
            else {
                return Ok(None);
            };
            let array = Scalar::Call(
                name.clone(),
                values
                    .iter()
                    .zip(args)
                    .map(|(value, arg)| {
                        if !varchar_array && varchar_text_literal(arg).is_some() {
                            arg.clone()
                        } else {
                            Scalar::Literal(Value::Text(value.clone()))
                        }
                    })
                    .collect(),
            );
            let array = if varchar_array {
                Scalar::Cast(Box::new(array), DataType::TextArray)
            } else {
                array
            };
            (array, values)
        }
    };
    if expand_membership {
        admit_expansion(&lhs, values.len())?;
        let values: Vec<_> = values
            .into_iter()
            .map(|value| {
                Scalar::Cast(
                    Box::new(Scalar::Literal(Value::Text(value))),
                    DataType::Text,
                )
            })
            .collect();
        Ok(Some(Filter::membership(&lhs, &values, all)))
    } else {
        Ok(Some(filter(lhs, array, all)))
    }
}

pub(super) fn deparse_membership(args: &[Scalar]) -> R<String> {
    let [lhs, Scalar::Literal(Value::Bool(all)), rhs] = args else {
        return Err(SqlError::InternalError);
    };
    let op = if *all { "<> ALL" } else { "= ANY" };
    let (array, cast) = match rhs {
        Scalar::Cast(inner, DataType::TextArray) => (inner.as_ref(), true),
        array => (array, false),
    };
    let rhs = match array {
        Scalar::Literal(Value::Text(value)) => format!("'{}'::text[]", value.replace('\'', "''")),
        Scalar::Call(name, values) if name == functions::ARRAY_CONSTRUCTOR => {
            let ty = if cast { "character varying" } else { "text" };
            let values = values
                .iter()
                .map(|value| {
                    if let Some(value) = varchar_text_literal(value) {
                        return Ok(format!(
                            "('{}'::character varying)::text",
                            value.replace('\'', "''")
                        ));
                    }
                    let value = text_literal(value).ok_or(SqlError::InternalError)?;
                    Ok(format!("'{}'::{ty}", value.replace('\'', "''")))
                })
                .collect::<R<Vec<_>>>()?
                .join(", ");
            if cast {
                format!("(ARRAY[{values}])::text[]")
            } else {
                format!("ARRAY[{values}]")
            }
        }
        _ => return Err(SqlError::InternalError),
    };
    Ok(format!("({} {op} ({rhs}))", scalar(lhs)?))
}
