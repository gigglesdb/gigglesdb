//! `SECURITY LABEL FOR giggles ON COLUMN t.c IS 'untrusted_text' | NULL`
//! (GD-0061 R3-307, design-3 section 5).
//!
//! Parsing is catalog-free, like COMMENT, so ownership is checked before the
//! label itself. The provider is checked at parse time (PostgreSQL also looks
//! it up first); the label and the column type are checked in `apply_label`.
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

pub(super) fn parse(p: &mut Parser) -> R<Label> {
    if !(p.word("security") && p.word("label")) {
        return Err(SqlError::Syntax);
    }
    if p.word("for") {
        let provider = match p.tokens.get(p.at) {
            Some(Token::Word(w) | Token::QuotedIdent(w)) => w.clone(),
            Some(Token::String(s)) => s.clone(),
            _ => return Err(SqlError::Syntax),
        };
        p.at += 1;
        if provider != PROVIDER {
            return Err(SqlError::InvalidParameterValue);
        }
    }
    p.need("on")?;
    if !p.word("column") {
        // Only column labels exist; other object types have no provider meaning.
        return Err(SqlError::Unsupported);
    }
    let mut names = vec![alter_table::raw_name(p)?];
    while p.sym('.') {
        names.push(alter_table::raw_name(p)?);
    }
    let column = names.pop().ok_or(SqlError::Syntax)?;
    let (schema, name) = match names.as_slice() {
        [name] => (None, name.clone()),
        [schema, name] => (Some(schema.clone()), name.clone()),
        _ => return Err(SqlError::Syntax),
    };
    p.relations.push(schemas::Reference::read(name.clone(), schema.clone()));
    let table = schema.map_or(name.clone(), |s| format!("{s}\0{name}"));
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
    Ok(Label { table, column, label })
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
                parts.column_labels.insert(
                    key,
                    crate::storage::UNTRUSTED_TEXT_LABEL,
                );
            }
            Some(_) => return Err(SqlError::InvalidParameterValue),
        }
        Ok(())
    }
}
