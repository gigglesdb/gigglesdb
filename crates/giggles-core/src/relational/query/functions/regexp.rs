//! SQL binding and result conversion for PostgreSQL regular expressions.
use super::*;
use std::cell::Cell;
use giggles_pg_regex::api::{self, RegexError, RegexLocale, RegexpContext, ReplaceOptions};

pub(super) const SIMILAR_TO: &str = "\0regexp_similar_to";
pub(super) const NOT_SIMILAR_TO: &str = "\0regexp_not_similar_to";
pub(super) const SUBSTRING_SIMILAR: &str = "\0regexp_substring_similar";

pub(super) fn parse_similar(
    parser: &mut Parser,
    value: Scalar,
    depth: usize,
    aggregate_argument: bool,
) -> R<Scalar> {
    let at = parser.at;
    let negated = parser.word("not");
    if !(parser.word("similar") && parser.word("to")) {
        parser.at = at;
        return Ok(value);
    }
    let pattern = Scalar::parse_binary(parser, depth + 1, aggregate_argument, 0)?;
    let mut args = vec![value, pattern];
    if parser.word("escape") {
        args.push(Scalar::parse_binary(
            parser,
            depth + 1,
            aggregate_argument,
            0,
        )?);
    }
    let next = match parser.tokens.get(parser.at) {
        Some(Token::Word(word)) if word == "not" => parser.at + 1,
        _ => parser.at,
    };
    if matches!(parser.tokens.get(next), Some(Token::Word(word))
        if matches!(word.as_str(), "like" | "ilike" | "similar" | "between" | "in"))
    {
        return Err(super::super::clause_syntax(parser.tokens.get(parser.at)));
    }
    let name = if negated { NOT_SIMILAR_TO } else { SIMILAR_TO };
    Ok(Scalar::Call(name.into(), args))
}

pub(super) fn reject_similar(parser: &Parser) -> R<()> {
    let tokens = &parser.tokens[parser.at..];
    let starts_similar = |tokens: &[Token]| {
        matches!(tokens, [Token::Word(similar), Token::Word(to), ..]
            if similar == "similar" && to == "to")
    };
    if starts_similar(tokens)
        || matches!(tokens, [Token::Word(word), rest @ ..] if word == "not" && starts_similar(rest))
    {
        return Err(super::super::clause_syntax(tokens.first()));
    }
    Ok(())
}

pub(super) fn parse_similar_substring(
    parser: &mut Parser,
    mut args: Vec<Scalar>,
    depth: usize,
    aggregate_argument: bool,
) -> R<Scalar> {
    args.push(Scalar::parse_context(parser, depth, aggregate_argument)?);
    parser.need("escape")?;
    args.push(Scalar::parse_context(parser, depth, aggregate_argument)?);
    parser.punct(')')?;
    Ok(Scalar::Call(SUBSTRING_SIMILAR.into(), args))
}

pub(super) fn context(locale: RegexLocale) -> RegexpContext<'static> {
    RegexpContext {
        locale,
        cancel: Some(&|| crate::cancellation::check().is_err()),
    }
}

pub(super) fn error(error: RegexError) -> SqlError {
    user_types::view_replace_error(error.message);
    user_types::exact_sql_error_detail(error.detail);
    if let Some(hint) = error.hint {
        user_types::sql_error_hint(hint);
    }
    match error.sqlstate {
        "2200C" => SqlError::InvalidUseOfEscapeCharacter,
        "2201B" => SqlError::InvalidRegularExpression,
        "22023" => SqlError::InvalidParameterValue,
        "22025" => SqlError::InvalidEscapeSequence,
        "54000" => SqlError::Limit,
        "57014" => SqlError::QueryCanceled,
        _ => unreachable!("unmapped regex SQLSTATE {}", error.sqlstate),
    }
}

pub(in crate::relational::query) fn set_type(name: &str) -> Option<DataType> {
    match name {
        "regexp_matches" => Some(DataType::TextArray),
        "regexp_split_to_table" => Some(DataType::Text),
        _ => None,
    }
}

#[derive(Clone, Copy)]
pub(in crate::relational::query) enum SetPlacement {
    Where,
    Update,
    Coalesce,
    Case,
    CaseWhen(Option<&'static str>),
}

thread_local! {
    static SET_PLACEMENT: Cell<Option<SetPlacement>> = const { Cell::new(None) };
}

impl SetPlacement {
    pub(in crate::relational::query) fn scope<T>(self, bind: impl FnOnce() -> T) -> T {
        let _restore = self.enter();
        bind()
    }

    pub(super) fn enter(self) -> impl Drop {
        struct Restore(Option<SetPlacement>);
        impl Drop for Restore {
            fn drop(&mut self) {
                SET_PLACEMENT.set(self.0);
            }
        }
        // A clause forbids SRFs even inside conditional expressions.
        let old = SET_PLACEMENT.get();
        if !matches!(old, Some(Self::Where | Self::Update)) {
            SET_PLACEMENT.set(Some(self));
        }
        Restore(old)
    }

    fn error(self) -> SqlError {
        let (message, error) = match self {
            Self::Where => ("set-returning functions are not allowed in WHERE".into(), SqlError::Unsupported),
            Self::Update => ("set-returning functions are not allowed in UPDATE".into(), SqlError::Unsupported),
            Self::Coalesce => (
                "set-returning functions are not allowed in COALESCE".into(),
                SqlError::Unsupported,
            ),
            Self::Case => ("set-returning functions are not allowed in CASE".into(), SqlError::Unsupported),
            Self::CaseWhen(Some(ty)) => (
                format!("argument of CASE/WHEN must be type boolean, not type {ty}"),
                SqlError::Type,
            ),
            Self::CaseWhen(None) => (
                "argument of CASE/WHEN must not return a set".into(),
                SqlError::Type,
            ),
        };
        user_types::view_replace_error(message);
        user_types::exact_sql_error_detail(None);
        if matches!(self, Self::Coalesce | Self::Case) {
            user_types::sql_error_hint(
                "You might be able to move the set-returning function into a LATERAL FROM item.",
            );
        }
        error
    }
}

pub(in crate::relational::query) fn text_type(ty: &DataType) -> bool {
    matches!(ty, DataType::Text | DataType::Varchar | DataType::Bpchar)
}

// The numeric substring overload wins when a position or length is typed as
// integer. With only unknown string literals PostgreSQL selects text.
pub(super) fn substring(types: &[Option<&DataType>]) -> bool {
    (2..=3).contains(&types.len()) && types.iter().all(|ty| ty.is_none_or(text_type))
}

fn signature(name: &str, args: &[Option<&DataType>]) -> Option<R<(&'static [DataType], DataType)>> {
    use DataType::{Boolean, Integer, Text, TextArray};
    let (types, result): (&'static [DataType], DataType) = match name {
        SIMILAR_TO | NOT_SIMILAR_TO | "regexp_like" => (&[Text, Text, Text], Boolean),
        "regexp_count" => (&[Text, Text, Integer, Text], Integer),
        "regexp_instr" => (
            &[Text, Text, Integer, Integer, Integer, Text, Integer],
            Integer,
        ),
        "regexp_substr" => (&[Text, Text, Integer, Integer, Text, Integer], Text),
        "regexp_match" | "regexp_matches" | "regexp_split_to_array" => {
            (&[Text, Text, Text], TextArray)
        }
        "regexp_split_to_table" => (&[Text, Text, Text], Text),
        "regexp_replace" if args.len() == 4 && args[3].is_none_or(text_type) => {
            (&[Text, Text, Text, Text], Text)
        }
        "regexp_replace" => (&[Text, Text, Text, Integer, Integer, Text], Text),
        "similar_to_escape" => (&[Text, Text], Text),
        "similar_escape" => (&[Text, Text], Text),
        SUBSTRING_SIMILAR => (&[Text, Text, Text], Text),
        "substring" if substring(args) => (&[Text, Text, Text], Text),
        _ => return None,
    };
    let minimum = match name {
        "similar_to_escape" => 1,
        "regexp_replace" | SUBSTRING_SIMILAR => 3,
        _ => 2,
    };
    Some(if (minimum..=types.len()).contains(&args.len()) {
        Ok((&types[..args.len()], result))
    } else {
        Err(SqlError::UndefinedFunction)
    })
}

pub(super) fn infer(
    name: &str,
    args: &[Scalar],
    parameters: &mut [Option<DataType>],
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
) -> R<bool> {
    if name != "substring" && signature(name, &[]).is_none() {
        return Ok(false);
    }
    let mut known = Vec::with_capacity(args.len());
    for arg in args {
        arg.infer(None, parameters, resolve, tables)?;
        known.push(if arg.aggregate_unknown(parameters) {
            None
        } else {
            Some(arg.bind(resolve, tables, parameters)?.ty)
        });
    }
    let types = known.iter().map(Option::as_ref).collect::<Vec<_>>();
    let Some(signature) = signature(name, &types) else {
        return Ok(false);
    };
    let (arg_types, _) = signature?;
    if set_type(name).is_some()
        && let Some(placement) = SET_PLACEMENT.get()
    {
        if known.iter().any(|ty| ty.as_ref().is_some_and(|ty| !text_type(ty))) {
            return Err(SqlError::UndefinedFunction);
        }
        return Err(placement.error());
    }
    for ((arg, known), ty) in args.iter().zip(known).zip(arg_types) {
        if known.is_none() {
            arg.infer(Some(ty), parameters, resolve, tables)?;
        }
    }
    Ok(true)
}

pub(super) fn bind(
    name: &str,
    sources: &[Scalar],
    args: &mut [BoundScalar],
    parameters: &[Option<DataType>],
) -> R<Option<DataType>> {
    if name != "substring" && signature(name, &[]).is_none() {
        return Ok(None);
    }
    let unknown = sources
        .iter()
        .map(|arg| arg.aggregate_unknown(parameters))
        .collect::<Vec<_>>();
    let types = args
        .iter()
        .zip(&unknown)
        .map(|(arg, unknown)| (!unknown).then_some(&arg.ty))
        .collect::<Vec<_>>();
    let Some(signature) = signature(name, &types) else {
        return Ok(None);
    };
    let (types, output) = signature?;
    for (i, ((arg, unknown), ty)) in args.iter_mut().zip(unknown).zip(types).enumerate() {
        if arg.ty == *ty
            || i == 0 && matches!(name, SIMILAR_TO | NOT_SIMILAR_TO) && arg.ty == DataType::Bpchar
        {
            continue;
        }
        if unknown && let Node::Literal(value) = &arg.node {
            arg.node = literal_cast_node(value.clone(), ty)?;
            arg.ty = ty.clone();
        } else if (ty == &DataType::Text && text_type(&arg.ty))
            || (ty == &DataType::Integer && arg.ty == DataType::SmallInt)
        {
            let old = BoundScalar {
                ty: std::mem::replace(&mut arg.ty, ty.clone()),
                node: std::mem::replace(&mut arg.node, Node::Literal(Value::Null)),
            };
            arg.node = cast_node(old, ty);
        } else {
            if i == 0 && matches!(name, SIMILAR_TO | NOT_SIMILAR_TO) {
                let op = if name == SIMILAR_TO { "~" } else { "!~" };
                return Err(arithmetic::operator_error(
                    op,
                    Some(Some(&arg.ty)),
                    Some(&DataType::Text),
                ));
            }
            return Err(SqlError::UndefinedFunction);
        }
    }
    if set_type(name).is_some()
        && let Some(placement) = SET_PLACEMENT.get()
    {
        return Err(placement.error());
    }
    Ok(Some(output))
}

fn text(value: &Value) -> R<&str> {
    match value {
        Value::Text(value) => Ok(value),
        _ => Err(SqlError::Type),
    }
}

fn optional_text(values: &[Value], index: usize) -> R<Option<&str>> {
    values
        .get(index)
        .filter(|v| **v != Value::Null)
        .map(text)
        .transpose()
}

fn integer_at(values: &[Value], index: usize, default: i32) -> R<i32> {
    match values.get(index) {
        None => Ok(default),
        Some(Value::Int(value)) => i32::try_from(*value).map_err(|_| SqlError::Overflow),
        _ => Err(SqlError::Type),
    }
}

fn array<'a>(values: impl IntoIterator<Item = Option<&'a str>>) -> Value {
    use crate::relational::logical_types::array_quote;
    let mut literal = String::from("{");
    for value in values {
        if literal.len() > 1 {
            literal.push(',');
        }
        if let Some(value) = value {
            literal.push_str(&array_quote(value));
        } else {
            literal.push_str("NULL");
        }
    }
    literal.push('}');
    Value::Text(literal)
}

fn optional_value(value: Option<impl Into<String>>) -> Value {
    value.map_or(Value::Null, |value| Value::Text(value.into()))
}

fn replace_options(values: &[Value]) -> R<ReplaceOptions<'_>> {
    Ok(match values {
        [_, _, _] => ReplaceOptions::Default,
        [_, _, _, Value::Text(flags)] => ReplaceOptions::Flags(flags),
        [_, _, _, _] => ReplaceOptions::Start(integer_at(values, 3, 1)?),
        _ => ReplaceOptions::Occurrence {
            start: integer_at(values, 3, 1)?,
            n: integer_at(values, 4, 1)?,
            flags: optional_text(values, 5)?,
        },
    })
}

pub(super) fn eval(
    name: &str,
    args: &[BoundScalar],
    values: &[Value],
    budget: &mut usize,
) -> Option<R<Value>> {
    if name == "substring" {
        if !args.get(1).is_some_and(|arg| arg.ty == DataType::Text) {
            return None;
        }
    } else if signature(name, &[]).is_none() {
        return None;
    }
    Some(evaluate(name, args, values).and_then(|value| {
        charge(budget, &value)?;
        Ok(value)
    }))
}

fn evaluate(name: &str, args: &[BoundScalar], values: &[Value]) -> R<Value> {
    if name != "similar_escape" && values.contains(&Value::Null) {
        return Ok(Value::Null);
    }
    // Pattern translation itself does not look at collation.
    if name == "similar_escape" {
        return api::similar_escape(
            optional_text(values, 0)?,
            optional_text(values, 1)?,
            &context(RegexLocale::CUtf8),
        )
        .map(optional_value)
        .map_err(error);
    }
    if name == "similar_to_escape" {
        return api::similar_to_escape(
            text(&values[0])?,
            optional_text(values, 1)?,
            &context(RegexLocale::CUtf8),
        )
        .map(Value::Text)
        .map_err(error);
    }
    if set_type(name).is_some() {
        return Err(SqlError::Unsupported);
    }
    let ctx = collation::regex_context(args)?;
    let source = text(&values[0])?;
    let pattern = text(&values[1])?;
    let result = match name {
        SIMILAR_TO | NOT_SIMILAR_TO => {
            api::similar_to(source, pattern, optional_text(values, 2)?, &ctx)
                .map(|matched| Value::Bool(matched != (name == NOT_SIMILAR_TO)))
        }
        "substring" if values.len() == 2 => {
            api::textregexsubstr(source, pattern, &ctx).map(optional_value)
        }
        "substring" | SUBSTRING_SIMILAR => {
            api::substring_similar(source, pattern, text(&values[2])?, &ctx).map(optional_value)
        }
        "regexp_count" => api::regexp_count(
            source,
            pattern,
            integer_at(values, 2, 1)?,
            optional_text(values, 3)?,
            &ctx,
        )
        .map(|count| Value::Int(i64::from(count))),
        "regexp_instr" => api::regexp_instr(
            source,
            pattern,
            api::InstrOptions {
                start: integer_at(values, 2, 1)?,
                n: integer_at(values, 3, 1)?,
                endoption: integer_at(values, 4, 0)?,
                flags: optional_text(values, 5)?,
                subexpr: integer_at(values, 6, 0)?,
            },
            &ctx,
        )
        .map(|position| Value::Int(i64::from(position))),
        "regexp_like" => {
            api::regexp_like(source, pattern, optional_text(values, 2)?, &ctx).map(Value::Bool)
        }
        "regexp_match" => api::regexp_match(source, pattern, optional_text(values, 2)?, &ctx)
            .map(|captures| captures.map_or(Value::Null, array)),
        "regexp_replace" => api::regexp_replace(
            source,
            pattern,
            text(&values[2])?,
            replace_options(values)?,
            &ctx,
        )
        .map(Value::Text),
        "regexp_substr" => api::regexp_substr(
            source,
            pattern,
            api::SubstrOptions {
                start: integer_at(values, 2, 1)?,
                n: integer_at(values, 3, 1)?,
                flags: optional_text(values, 4)?,
                subexpr: integer_at(values, 5, 0)?,
            },
            &ctx,
        )
        .map(optional_value),
        "regexp_split_to_array" => {
            api::regexp_split_to_array(source, pattern, optional_text(values, 2)?, &ctx)
                .map(|parts| array(parts.into_iter().map(Some)))
        }
        _ => unreachable!("regex function {name}"),
    };
    result.map_err(error)
}

pub(in crate::relational::query) fn emit(
    name: &str,
    args: &[BoundScalar],
    values: &[Value],
    mut emit: impl FnMut(Value) -> R<()>,
) -> R<()> {
    if values.contains(&Value::Null) {
        return Ok(());
    }
    let ctx = collation::regex_context(args)?;
    let argument = |i: usize| {
        text(&values[i]).map(|text| {
            if args[i].ty == DataType::Bpchar {
                text.trim_end_matches(' ')
            } else {
                text
            }
        })
    };
    let source = argument(0)?;
    let pattern = argument(1)?;
    let flags = values.get(2).map(|_| argument(2)).transpose()?;
    match name {
        "regexp_matches" => {
            for captures in api::regexp_matches(source, pattern, flags, &ctx).map_err(error)? {
                emit(array(captures))?;
            }
        }
        "regexp_split_to_table" => {
            for part in api::regexp_split_to_table(source, pattern, flags, &ctx).map_err(error)? {
                emit(Value::Text(part.into()))?;
            }
        }
        _ => unreachable!("regex set function {name}"),
    }
    Ok(())
}
