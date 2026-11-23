//! Syntax and assignment coercion for SQL EXECUTE arguments.
use super::*;

pub enum Syntax {
    Untyped(Value),
    Typed,
    Subquery,
}

/// Recognize subquery introductions and distinguish an unknown literal from a
/// typed expression before the projection defaults unknown outputs to text.
pub fn syntax(sql: &str) -> R<Syntax> {
    assert_sql_frontend_allowed();
    let tokens = lex(sql)?;
    if tokens.windows(2).any(|pair| {
        matches!(&pair[0], Token::Symbol('('))
            && matches!(&pair[1], Token::Word(word)
                if matches!(word.as_str(), "select" | "values" | "table" | "with"))
    }) {
        return Ok(Syntax::Subquery);
    }
    let mut expression = tokens.as_slice();
    while let [Token::Symbol('('), inner @ .., Token::Symbol(')')] = expression {
        expression = inner;
    }
    Ok(match expression {
        [Token::String(text)] => Syntax::Untyped(Value::Text(text.clone())),
        [Token::Word(word)] if word == "null" => Syntax::Untyped(Value::Null),
        _ => Syntax::Typed,
    })
}

pub fn assignment_allowed(source: &DataType, target: &DataType) -> bool {
    query::assignment::compatible(source, target)
}

pub(crate) fn coerce(
    value: Value,
    source: &DataType,
    target: &DataType,
    untyped: bool,
    clock: &clock::ExecutionClock,
) -> R<Value> {
    if untyped {
        let input = match (&value, target) {
            (Value::Text(text), DataType::SmallInt) => Some((text.clone(), "smallint")),
            (Value::Text(text), DataType::Integer) => Some((text.clone(), "integer")),
            (Value::Text(text), DataType::BigInt) => Some((text.clone(), "bigint")),
            _ => None,
        };
        query::assignment::literal(value, target, Some(clock)).inspect_err(|error| {
            if *error == SqlError::Overflow
                && let Some((text, name)) = input
            {
                user_types::view_replace_error(format!(
                    r#"value "{text}" is out of range for type {name}"#
                ));
            }
        })
    } else {
        query::assignment::convert(value, source, target, Some(clock)).inspect_err(|error| {
            if *error == SqlError::Overflow {
                let name = match target {
                    DataType::SmallInt => "smallint",
                    DataType::Integer => "integer",
                    DataType::BigInt => "bigint",
                    _ => return,
                };
                user_types::view_replace_error(format!("{name} out of range"));
            }
        })
    }
}
