//! Binding and evaluation for PostgreSQL full-text search signatures.
use super::*;
use crate::relational::text_search_types::{
    self as types, diagnostic, query, vector, with_notices,
};
use giggles_pg_tsearch::{self as ts, Config, Dictionary, Weight};

use DataType::*;

type Signature = (&'static [DataType], DataType, bool);

pub(in crate::relational::query) fn known(name: &str) -> bool {
    matches!(
        name,
        "to_tsvector"
            | "to_tsquery"
            | "plainto_tsquery"
            | "phraseto_tsquery"
            | "websearch_to_tsquery"
            | "setweight"
            | "strip"
            | "ts_delete"
            | "ts_filter"
            | "tsvector_to_array"
            | "array_to_tsvector"
            | "tsquery_phrase"
            | "numnode"
            | "querytree"
            | "ts_rank"
            | "ts_rank_cd"
            | "ts_headline"
            | "ts_lexize"
            | "get_current_ts_config"
            | "ts_rewrite"
            | "\0ts_negate"
            | "\0ts_and"
            | "\0ts_or"
            | "\0ts_match"
            | "\0ts_concat"
            | "\0ts_contains"
            | "\0ts_contained_by"
            | "\0ts_length"
            | "\0ts_phrase"
    )
}

fn signatures(name: &str) -> &'static [Signature] {
    match name {
        "to_tsvector" => &[
            (&[Text], TsVector, false),
            (&[RegConfig, Text], TsVector, true),
        ],
        "to_tsquery" | "plainto_tsquery" | "phraseto_tsquery" | "websearch_to_tsquery" => &[
            (&[Text], TsQuery, false),
            (&[RegConfig, Text], TsQuery, true),
        ],
        "setweight" => &[
            (&[TsVector, PgChar], TsVector, true),
            (&[TsVector, PgChar, TextArray], TsVector, true),
        ],
        "strip" => &[(&[TsVector], TsVector, true)],
        "ts_delete" => &[
            (&[TsVector, Text], TsVector, true),
            (&[TsVector, TextArray], TsVector, true),
        ],
        "ts_filter" => &[(&[TsVector, PgCharArray], TsVector, true)],
        "tsvector_to_array" => &[(&[TsVector], TextArray, true)],
        "array_to_tsvector" => &[(&[TextArray], TsVector, true)],
        "tsquery_phrase" | "\0ts_phrase" => &[
            (&[TsQuery, TsQuery], TsQuery, true),
            (&[TsQuery, TsQuery, Integer], TsQuery, true),
        ],
        "numnode" => &[(&[TsQuery], Integer, true)],
        "querytree" => &[(&[TsQuery], Text, true)],
        "ts_rank" | "ts_rank_cd" => &[
            (&[TsVector, TsQuery], Real, true),
            (&[TsVector, TsQuery, Integer], Real, true),
            (&[RealArray, TsVector, TsQuery], Real, true),
            (&[RealArray, TsVector, TsQuery, Integer], Real, true),
        ],
        "ts_headline" => &[
            (&[Text, TsQuery], Text, false),
            (&[Text, TsQuery, Text], Text, false),
            (&[RegConfig, Text, TsQuery], Text, true),
            (&[RegConfig, Text, TsQuery, Text], Text, true),
        ],
        "ts_rewrite" => &[(&[TsQuery, TsQuery, TsQuery], TsQuery, true)],
        "ts_debug" => &[(&[Text], Text, false), (&[RegConfig, Text], Text, false)],
        "ts_parse" => &[(&[Text, Text], Text, true), (&[Oid, Text], Text, true)],
        "ts_token_type" => &[(&[Text], Text, true), (&[Oid], Text, true)],
        "ts_lexize" => &[(&[RegDictionary, Text], TextArray, true)],
        "get_current_ts_config" => &[(&[], RegConfig, false)],
        "\0ts_length" => &[(&[TsVector], Integer, true)],
        "\0ts_negate" => &[(&[TsQuery], TsQuery, true)],
        "\0ts_and" | "\0ts_or" => &[(&[TsQuery, TsQuery], TsQuery, true)],
        "\0ts_contains" | "\0ts_contained_by" => &[(&[TsQuery, TsQuery], Boolean, true)],
        "\0ts_concat" => &[(&[TsVector, TsVector], TsVector, true)],
        "\0ts_match" => &[
            (&[TsVector, TsQuery], Boolean, true),
            (&[TsQuery, TsVector], Boolean, true),
            (&[Text, TsQuery], Boolean, false),
            (&[Text, Text], Boolean, false),
        ],
        _ => &[],
    }
}

pub(super) fn metadata(name: &str, args: &[DataType], result: &DataType) -> Option<(bool, bool)> {
    signatures(name)
        .iter()
        .find(|(types, ty, _)| *types == args && ty == result)
        .map(|(_, _, immutable)| (*immutable, true))
}

fn compatible(source: &DataType, target: &DataType) -> bool {
    source == target
        || matches!(
            (source, target),
            (Varchar | Bpchar, Text) | (SmallInt, Integer) | (SmallInt | Integer | BigInt, Oid)
        )
}

fn function_error(name: &str, types: &[Option<DataType>], ambiguous: bool) -> SqlError {
    if let Some(op) = match name {
        "\0ts_match" => Some("@@"),
        "\0ts_negate" => Some("!!"),
        "\0ts_phrase" => Some("<->"),
        _ => None,
    } {
        let left = (types.len() == 2).then(|| types[0].as_ref());
        return arithmetic::operator_error(op, left, types.last().and_then(Option::as_ref));
    }
    let args = types
        .iter()
        .map(|ty| ty.as_ref().map_or("unknown", typeof_expr::name))
        .collect::<Vec<_>>()
        .join(", ");
    if ambiguous {
        user_types::sql_error_hint(
            "Could not choose a best candidate function. You might need to add explicit type casts.",
        );
        types::error_message("42725", format!("function {name}({args}) is not unique"))
    } else {
        user_types::sql_error_hint(
            "No function matches the given name and argument types. You might need to add explicit type casts.",
        );
        types::error_message("42883", format!("function {name}({args}) does not exist"))
    }
}

pub(in crate::relational::query) fn aggregate_error(name: &str, ty: &DataType) -> SqlError {
    function_error(name, &[Some(ty.clone())], false)
}

fn choose(
    name: &str,
    args: &[Scalar],
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
    parameters: &[Option<DataType>],
) -> R<&'static Signature> {
    let types = args
        .iter()
        .map(|arg| match arg {
            Scalar::Call(n, _) if n == array_constructor::NAME => Ok(None),
            // Check restricted casts against the selected argument slot in bind.
            Scalar::Cast(_, ty @ (Oid | PgChar | PgCharArray)) => Ok(Some(ty.clone())),
            _ => arithmetic::known(arg, resolve, tables, parameters),
        })
        .collect::<R<Vec<_>>>()?;
    let mut candidates = signatures(name)
        .iter()
        .filter(|(signature, _, _)| {
            signature.len() == args.len()
                && signature
                    .iter()
                    .zip(&types)
                    .zip(args)
                    .all(|((target, source), arg)| {
                        if matches!(arg, Scalar::Call(n, _) if n == array_constructor::NAME) {
                            array_constructor::element_type(target).is_some()
                        } else {
                            source
                                .as_ref()
                                .is_none_or(|source| compatible(source, target))
                        }
                    })
        })
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Err(function_error(name, &types, false));
    }
    if name == "ts_headline" && candidates.len() > 1 && types.iter().all(Option::is_none) {
        return Err(function_error(name, &types, true));
    }
    if candidates.len() > 1 {
        // For unknown strings PostgreSQL prefers the string category; text is
        // also an exact match for the other operand of text @@ unknown.
        let preferred = |sig: &Signature| {
            sig.0
                .iter()
                .zip(&types)
                .filter(|(target, source)| {
                    source
                        .as_ref()
                        .map_or(**target == Text, |source| source == *target)
                })
                .count()
        };
        let best = candidates.iter().map(|sig| preferred(sig)).max().unwrap();
        candidates.retain(|sig| preferred(sig) == best);
    }
    match candidates.as_slice() {
        [signature] => Ok(signature),
        _ => Err(function_error(name, &types, true)),
    }
}

pub(super) fn bind(
    name: &str,
    args: &[Scalar],
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
    parameters: &[Option<DataType>],
) -> R<BoundScalar> {
    let (arg_types, ty, _) = choose(name, args, resolve, tables, parameters)?;
    let args = args
        .iter()
        .zip(*arg_types)
        .map(|(arg, target)| {
            if let Scalar::Call(n, args) = arg
                && n == array_constructor::NAME
            {
                return array_constructor::bind(args, array_constructor::Binding::Cast(target), resolve, tables, parameters);
            }
            if let Scalar::Cast(input, cast_type) = arg
                && cast_type == target
                && matches!(target, Oid | PgChar | PgCharArray)
            {
                return Scalar::bind_cast(input, target, true, resolve, tables, parameters);
            }
            arithmetic::operand(arg, target, resolve, tables, parameters)
        })
        .collect::<R<Vec<_>>>()?;
    Ok(BoundScalar {
        ty: ty.clone(),
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
        if !matches!(arg, Scalar::Call(n, _) if n == array_constructor::NAME) {
            arg.infer(None, parameters, resolve, tables)?;
        }
    }
    let (types, _, _) = choose(name, args, resolve, tables, parameters)?;
    for (arg, target) in args.iter().zip(*types) {
        let hint = (arg.aggregate_unknown(parameters)
            || matches!(arg, Scalar::Call(n, _) if n == array_constructor::NAME))
        .then_some(target);
        arg.infer(hint, parameters, resolve, tables)?;
    }
    Ok(())
}

pub(in crate::relational::query) fn bind_row_args(
    name: &str, args: &[Scalar], parameters: &mut [Option<DataType>],
) -> R<Vec<BoundScalar>> {
    let no_columns = |_: &ColumnRef, _: usize| Err(SqlError::UnknownColumn);
    infer(name, args, parameters, &no_columns, &[])?;
    let bound = bind(name, args, &no_columns, &[], parameters)?;
    let Node::Call(_, args) = bound.node else { unreachable!() };
    Ok(args)
}

pub(in crate::relational::query) fn row_arg_types(
    name: &str, types: &[Option<DataType>],
) -> R<()> {
    if signatures(name).iter().any(|(sig, _, _)| {
        sig.len() == types.len() && sig.iter().zip(types).all(|(target, source)| {
            source.as_ref().is_none_or(|source| compatible(source, target))
        })
    }) {
        Ok(())
    } else {
        Err(function_error(name, types, false))
    }
}

pub(in crate::relational::query) fn text(value: &Value) -> R<&str> {
    match value {
        Value::Text(text) => Ok(text),
        _ => Err(SqlError::Type),
    }
}

fn int(value: &Value) -> R<i64> {
    match value {
        Value::Int(n) => Ok(*n),
        _ => Err(SqlError::Type),
    }
}

pub(in crate::relational::query) fn config(value: &Value) -> R<Config> {
    let oid = int(value)? as u32;
    Config::from_oid(oid).ok_or_else(|| {
        types::error_message(
            "XX000",
            format!("cache lookup failed for text search configuration {oid}"),
        )
    })
}

fn config_arg<'a>(args: &[BoundScalar], values: &'a [Value]) -> R<(Config, &'a [Value])> {
    if args[0].ty == RegConfig {
        Ok((config(&values[0])?, &values[1..]))
    } else {
        Ok((types::default_config()?, values))
    }
}

fn array(value: &Value) -> R<Vec<Option<String>>> {
    logical_types::array_values(text(value)?)
}

fn borrowed(words: &[Option<String>]) -> Vec<Option<&str>> {
    words.iter().map(Option::as_deref).collect()
}

fn array_value<'a>(words: impl IntoIterator<Item = &'a str>) -> Value {
    Value::Text(format!(
        "{{{}}}",
        words
            .into_iter()
            .map(logical_types::array_quote)
            .collect::<Vec<_>>()
            .join(",")
    ))
}

fn eval_rank(name: &str, args: &[BoundScalar], values: &[Value]) -> R<Value> {
    let (weights, values) = if args[0].ty == RealArray {
        let weights = array(&values[0])?;
        let weights = weights
            .iter()
            .map(|value| {
                value
                    .as_deref()
                    .map(|weight| Float4::parse(weight).map(Float4::value).map_err(pg_error))
                    .transpose()
            })
            .collect::<R<Vec<_>>>()?;
        (Some(weights), &values[1..])
    } else {
        (None, values)
    };
    let norm = values.get(2).map(int).transpose()?.unwrap_or(0) as i32;
    let rank = if name == "ts_rank" {
        ts::ts_rank
    } else {
        ts::ts_rank_cd
    };
    let rank = rank(
        &vector(&values[0])?,
        &query(&values[1])?,
        weights.as_deref(),
        norm,
    )
    .map_err(diagnostic)?;
    Ok(Value::Float4(Float4::from_bits(rank.to_bits())))
}

pub(super) fn eval(name: &str, args: &[BoundScalar], values: &[Value]) -> R<Value> {
    let first = || &values[0];
    let second = || &values[1];

    Ok(match name {
        "to_tsvector" => {
            let (config, values) = config_arg(args, values)?;
            Value::Bytes(
                with_notices(ts::to_tsvector(config, text(&values[0])?).map_err(diagnostic)?)
                    .to_storage(),
            )
        }
        "to_tsquery" | "plainto_tsquery" | "phraseto_tsquery" | "websearch_to_tsquery" => {
            let (config, values) = config_arg(args, values)?;
            let build = match name {
                "to_tsquery" => ts::to_tsquery,
                "plainto_tsquery" => ts::plainto_tsquery,
                "phraseto_tsquery" => ts::phraseto_tsquery,
                _ => ts::websearch_to_tsquery,
            };
            Value::Bytes(
                with_notices(build(config, text(&values[0])?).map_err(diagnostic)?).to_storage(),
            )
        }
        "setweight" => {
            let weight = Weight::parse(int(second())? as u8).map_err(diagnostic)?;
            let words = values.get(2).map(array).transpose()?;
            let words = words.as_deref().map(borrowed);
            Value::Bytes(
                vector(first())?
                    .setweight(weight, words.as_deref())
                    .to_storage(),
            )
        }
        "strip" => Value::Bytes(vector(first())?.strip().to_storage()),
        "\0ts_concat" => Value::Bytes(
            vector(first())?
                .concat(&vector(second())?)
                .map_err(diagnostic)?
                .to_storage(),
        ),
        "\0ts_length" => Value::Int(vector(first())?.len() as i64),
        "ts_delete" => {
            let vector = vector(first())?;
            let result = if args[1].ty == TextArray {
                vector.delete(&borrowed(&array(second())?))
            } else {
                vector.delete_lexeme(text(second())?)
            };
            Value::Bytes(result.to_storage())
        }
        "ts_filter" => {
            let weights = array(second())?
                .iter()
                .map(|word| word.as_ref().map(|word| word.bytes().next().unwrap_or(0)))
                .collect::<Vec<_>>();
            Value::Bytes(
                vector(first())?
                    .filter(&weights)
                    .map_err(diagnostic)?
                    .to_storage(),
            )
        }
        "tsvector_to_array" => array_value(vector(first())?.to_array()),
        "array_to_tsvector" => Value::Bytes(
            ts::TsVector::from_array(&borrowed(&array(first())?))
                .map_err(diagnostic)?
                .to_storage(),
        ),
        "\0ts_and" => Value::Bytes(
            query(first())?
                .and(&query(second())?)
                .map_err(diagnostic)?
                .to_storage(),
        ),
        "\0ts_or" => Value::Bytes(
            query(first())?
                .or(&query(second())?)
                .map_err(diagnostic)?
                .to_storage(),
        ),
        "\0ts_negate" => Value::Bytes(query(first())?.negate().map_err(diagnostic)?.to_storage()),
        "tsquery_phrase" | "\0ts_phrase" => {
            let distance = values.get(2).map(int).transpose()?.map(|n| n as i32);
            Value::Bytes(
                query(first())?
                    .phrase(&query(second())?, distance)
                    .map_err(diagnostic)?
                    .to_storage(),
            )
        }
        "\0ts_contains" => Value::Bool(query(first())?.contains(&query(second())?)),
        "\0ts_contained_by" => Value::Bool(query(first())?.contained_by(&query(second())?)),
        "numnode" => Value::Int(query(first())?.numnode() as i64),
        "querytree" => Value::Text(query(first())?.querytree()),
        "\0ts_match" => Value::Bool(match (&args[0].ty, &args[1].ty) {
            (TsVector, TsQuery) => ts::ts_match(&vector(first())?, &query(second())?),
            (TsQuery, TsVector) => ts::ts_match(&vector(second())?, &query(first())?),
            (Text, TsQuery) => with_notices(
                ts::ts_match_text(types::default_config()?, text(first())?, &query(second())?)
                    .map_err(diagnostic)?,
            ),
            (Text, Text) => with_notices(
                ts::ts_match_text_text(types::default_config()?, text(first())?, text(second())?)
                    .map_err(diagnostic)?,
            ),
            _ => return Err(SqlError::Type),
        }),
        "ts_rank" | "ts_rank_cd" => return eval_rank(name, args, values),
        "ts_headline" => {
            let (config, values) = config_arg(args, values)?;
            let options = values.get(2).map(text).transpose()?;
            Value::Text(with_notices(
                ts::ts_headline(config, text(&values[0])?, &query(&values[1])?, options)
                    .map_err(diagnostic)?,
            ))
        }
        "ts_rewrite" => Value::Bytes(
            query(first())?
                .rewrite(&query(second())?, &query(&values[2])?)
                .map_err(diagnostic)?
                .to_storage(),
        ),
        "ts_lexize" => {
            let oid = int(first())? as u32;
            let dict = Dictionary::from_oid(oid).ok_or_else(|| {
                types::error_message(
                    "XX000",
                    format!("cache lookup failed for text search dictionary {oid}"),
                )
            })?;
            array_value(
                ts::ts_lexize(dict, text(second())?)
                    .iter()
                    .map(String::as_str),
            )
        }
        "get_current_ts_config" => Value::Int(types::default_config()?.oid().into()),
        _ => return Err(SqlError::UndefinedFunction),
    })
}
