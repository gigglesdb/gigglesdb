//! `SECURITY LABEL FOR giggles ON COLUMN t.c IS 'untrusted_text' | NULL`
//! (GD-0061 R3-307, design-3 section 5).
//!
//! Parsing is catalog-free, like COMMENT, so ownership is checked before the
//! label itself. Provider refusals use the admin utility execution path.
use super::*;

/// The only security label provider.
pub(crate) const PROVIDER: &str = "giggles";
/// The only label the provider accepts.
pub(crate) const UNTRUSTED_TEXT: &str = "untrusted_text";

#[derive(Debug, Clone)]
pub(super) struct Label {
    pub table: String,
    column: String,
    label: Option<String>,
}

pub(super) fn claims(tokens: &[Token]) -> bool {
    matches!(tokens, [Token::Word(a), Token::Word(b), ..] if a == "security" && b == "label")
}

pub(super) fn parse(p: &mut Parser, source: &str) -> R<Statement> {
    parse_label(p).map_err(|error| {
        if error == SqlError::Syntax {
            let mut tokens = Vec::new();
            let mut spans = Vec::new();
            if lex_with_spans(source, &mut tokens, false, Some(&mut spans)).is_ok() {
                let message = if let Some(span) = spans.get(p.at) {
                    user_types::sql_error_position(source[..span.start].chars().count() + 1);
                    format!("syntax error at or near \"{}\"", &source[span.clone()])
                } else {
                    user_types::sql_error_position(source.chars().count() + 1);
                    "syntax error at end of input".into()
                };
                return user_types::exact_sql_error(error, message, None);
            }
        }
        error
    })
}

fn parse_label(p: &mut Parser) -> R<Statement> {
    p.need("security")?;
    p.need("label")?;
    let provider = if p.word("for") {
        let provider = match p.tokens.get(p.at) {
            Some(Token::Word(w) | Token::QuotedIdent(w)) => w.clone(),
            Some(Token::String(s)) => s.clone(),
            _ => return Err(SqlError::Syntax),
        };
        p.at += 1;
        Some(provider)
    } else {
        None
    };
    p.need("on")?;
    let column_target = if p.word("column") {
        let mut names = vec![p.raw_name()?];
        while p.sym('.') {
            names.push(p.raw_name()?);
        }
        Some(names)
    } else {
        parse_other_target(p)?;
        None
    };
    p.need("is")?;
    let label = if p.word("null") {
        None
    } else {
        let Some(Token::String(text)) = p.tokens.get(p.at) else {
            return Err(SqlError::Syntax);
        };
        p.at += 1;
        Some(text.clone())
    };
    p.sym(';');
    if p.at != p.tokens.len() {
        return Err(SqlError::Syntax);
    }
    if let Some(mut names) = column_target
        && provider.as_deref().unwrap_or(PROVIDER) == PROVIDER
    {
        let column = names.pop().expect("column target has at least one name");
        let (schema, name) = match names.as_slice() {
            [name] => (None, name),
            [schema, name] => (Some(schema.clone()), name),
            _ => return Err(SqlError::Syntax),
        };
        p.relations
            .push(schemas::Reference::read(name.clone(), schema.clone()));
        let table = schema.map_or_else(|| name.clone(), |s| format!("{s}\0{name}"));
        return Ok(Statement::SecurityLabel(Label {
            table,
            column,
            label,
        }));
    }
    Ok(Statement::AdminUtility(
        admin_utility::Statement::SecurityLabel(provider),
    ))
}

fn parse_other_target(p: &mut Parser) -> R<()> {
    let Some(Token::Word(kind)) = p.tokens.get(p.at) else {
        return Err(SqlError::Syntax);
    };
    let kind = kind.as_str();
    let (extra, qualified, signature) = match kind {
        "aggregate" | "function" | "procedure" | "routine" => (&[][..], true, true),
        "materialized" => (&["view"][..], true, false),
        "foreign" if matches!(p.tokens.get(p.at + 1), Some(Token::Word(next)) if next == "data") => {
            (&["data", "wrapper"][..], false, false)
        }
        "foreign" => (&["table"][..], true, false),
        "text" => (&["search"][..], true, false),
        "event" => (&["trigger"][..], false, false),
        "access" => (&["method"][..], false, false),
        "procedural" => (&["language"][..], false, false),
        "database" | "extension" | "language" | "publication" | "role" | "schema" | "server"
        | "subscription" | "tablespace" => (&[][..], false, false),
        "collation" | "conversion" | "index" | "sequence" | "statistics" | "table" | "view" => {
            (&[][..], true, false)
        }
        "domain" | "type" => {
            p.at += 1;
            if query::transform_builtin_type(p)?.is_none() {
                user_types::TypeReference::parse(p)?;
            }
            return Ok(());
        }
        "large" => {
            p.at += 1;
            p.need("object")?;
            if !matches!(p.tokens.get(p.at), Some(Token::Number(_))) {
                return Err(SqlError::Syntax);
            }
            p.at += 1;
            return Ok(());
        }
        _ => return Err(SqlError::Syntax),
    };
    let text_search = kind == "text";
    let aggregate = kind == "aggregate";
    p.at += 1;
    for keyword in extra {
        p.need(keyword)?;
    }
    if text_search
        && !["configuration", "dictionary", "parser", "template"]
            .iter()
            .any(|kind| p.word(kind))
    {
        return Err(SqlError::Syntax);
    }
    p.raw_name()?;
    while qualified && p.sym('.') {
        p.raw_name()?;
    }
    if signature && (aggregate || matches!(p.tokens.get(p.at), Some(Token::Symbol('(')))) {
        if aggregate && matches!(p.tokens.get(p.at + 1), Some(Token::Op(op)) if op == "*") {
            p.at += 2;
            p.punct(')')?;
        } else {
            routines::arguments(p, true)?;
        }
    }
    Ok(())
}

pub(super) fn refuse(provider: Option<&str>) -> SqlError {
    let message = match provider {
        Some(PROVIDER) => return SqlError::Unsupported,
        Some(name) => format!("security label provider \"{name}\" is not loaded"),
        None => "no security label providers have been loaded".into(),
    };
    user_types::exact_sql_error(SqlError::InvalidParameterValue, message, None)
}

impl Prepared {
    /// Applies a parsed SECURITY LABEL to the post-statement catalog parts.
    /// Any other statement leaves `parts` unchanged.
    pub(crate) fn apply_label(
        &self,
        state: &Relational,
        parts: &mut crate::storage::CatalogParts,
    ) -> R<()> {
        let Statement::SecurityLabel(label) = self.statement.get() else {
            return Ok(());
        };
        self.validate_schemas(state)?;
        let Some(table) = parts.tables.get(&label.table) else {
            return Err(if state.view_names.contains_key(&label.table) {
                SqlError::WrongObjectType
            } else {
                SqlError::UnknownTable
            });
        };
        let column = table
            .logical_columns()
            .find(|c| c.name == label.column)
            .ok_or(SqlError::UnknownColumn)?;
        let key = (table.id, column.id);
        match label.label.as_deref() {
            None => {
                parts.column_labels.remove(&key);
            }
            Some(UNTRUSTED_TEXT) => {
                if !matches!(
                    column.data_type,
                    DataType::Text | DataType::Varchar | DataType::Bpchar
                ) {
                    return Err(SqlError::InvalidParameterValue);
                }
                parts
                    .column_labels
                    .insert(key, crate::storage::UNTRUSTED_TEXT_LABEL);
            }
            Some(_) => return Err(SqlError::InvalidParameterValue),
        }
        Ok(())
    }
}
