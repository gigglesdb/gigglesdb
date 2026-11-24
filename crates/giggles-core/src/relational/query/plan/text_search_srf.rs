//! Row shapes and execution for PostgreSQL's text-search inspection functions.
use super::*;
use functions::text_search::{config, text};
use giggles_pg_tsearch as ts;

pub(super) fn known(name: &str) -> bool {
    matches!(name, "ts_debug" | "ts_parse" | "ts_token_type")
}

pub(super) fn columns(name: &str) -> Vec<ColumnDef> {
    use DataType::*;
    let columns: &[(&str, DataType)] = match name {
        "ts_debug" => &[
            ("alias", Text),
            ("description", Text),
            ("token", Text),
            ("dictionaries", RegDictionaryArray),
            ("dictionary", RegDictionary),
            ("lexemes", TextArray),
        ],
        "ts_parse" => &[("tokid", Integer), ("token", Text)],
        "ts_token_type" => &[("tokid", Integer), ("alias", Text), ("description", Text)],
        _ => unreachable!(),
    };
    columns
        .iter()
        .map(|(name, data_type)| ColumnDef {
            id: 0,
            name: (*name).into(),
            data_type: data_type.clone(),
            not_null: false,
        })
        .collect()
}

pub(super) fn run(
    name: &str,
    values: &[Value],
    emit: &mut impl FnMut(Vec<Value>) -> R<()>,
) -> R<()> {
    let string = |s: &str| Value::Text(s.into());
    match name {
        "ts_debug" => {
            let (config, document) = match values {
                [document] => (text_search_types::default_config()?, text(document)?),
                [value @ Value::Int(_), document] => (config(value)?, text(document)?),
                [value, document] => {
                    let value = text_search_types::input(&DataType::RegConfig, text(value)?)?;
                    (config(&value)?, text(document)?)
                }
                _ => unreachable!(),
            };
            for row in ts::ts_debug(config, document) {
                let dictionaries = row
                    .dictionary
                    .map_or_else(|| "{}".into(), |d| format!("{{{}}}", d.name()));
                let dictionary = row
                    .dictionary
                    .map_or(Value::Null, |d| Value::Int(d.oid().into()));
                let lexemes = row.lexemes.map_or(Value::Null, |lexemes| {
                    let lexemes = lexemes
                        .iter()
                        .map(|word| logical_types::array_quote(word))
                        .collect::<Vec<_>>();
                    Value::Text(format!("{{{}}}", lexemes.join(",")))
                });
                emit(vec![
                    string(row.token.kind.alias),
                    string(row.token.kind.description),
                    string(row.token.text),
                    Value::Text(dictionaries),
                    dictionary,
                    lexemes,
                ])?;
            }
        }
        "ts_parse" => {
            for token in ts::ts_parse(parser(&values[0])?, text(&values[1])?)
                .map_err(text_search_types::diagnostic)?
            {
                emit(vec![Value::Int(token.kind.id.into()), string(token.text)])?;
            }
        }
        "ts_token_type" => {
            for kind in
                ts::ts_token_type(parser(&values[0])?).map_err(text_search_types::diagnostic)?
            {
                emit(vec![
                    Value::Int(kind.id.into()),
                    string(kind.alias),
                    string(kind.description),
                ])?;
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}

fn parser(value: &Value) -> R<&str> {
    match value {
        Value::Int(3722) => Ok("default"),
        Value::Int(oid) => Err(text_search_types::error_message(
            "XX000",
            format!("cache lookup failed for text search parser {}", *oid as u32),
        )),
        value => text(value),
    }
}
