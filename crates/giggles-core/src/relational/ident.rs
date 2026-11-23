//! Identifier resolution shared by the SQL frontends and stored catalogs.
use super::{HIDDEN_ROWID, Parser, R, SqlError, Token};

fn shape(s: &str) -> R<()> {
    if s.is_empty() || s.len() > 63 || s.contains('\0') {
        return Err(SqlError::Syntax);
    }
    Ok(())
}

pub(crate) fn column_ident(s: &str) -> R<()> {
    shape(s)?;
    // The physical heap key uses this name as a persisted marker.
    if s == HIDDEN_ROWID {
        return Err(SqlError::Reserved);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
pub(super) enum ColumnNamePolicy {
    Sql,
    Native,
}

impl ColumnNamePolicy {
    pub(super) fn validate(self, name: &str) -> R<()> {
        match self {
            Self::Sql => column_ident(name),
            Self::Native => ident(name),
        }
    }
}

pub(super) fn ident(s: &str) -> R<()> {
    shape(s)?;
    if [
        "deliveries",
        "giggles",
        "system",
        "information_schema",
        "pg_catalog",
        "repair",
        "internal",
        "tasks",
        "operations",
        "approvals",
        "receipts",
    ]
    .contains(&s)
        || s.starts_with("giggles_")
        || s.starts_with("system_")
        || s.starts_with("pg_")
        || s.starts_with("repair_")
        || s.starts_with("internal_")
    {
        return Err(SqlError::Reserved);
    }
    Ok(())
}

/// Truncate at a UTF-8 boundary, measured in bytes as PostgreSQL identifiers are.
pub(super) fn truncate(value: &str, limit: usize) -> &str {
    let mut end = value.len().min(limit);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}
impl Parser {
    pub(super) fn raw_name(&mut self) -> R<String> {
        let value = match self.tokens.get(self.at) {
            Some(Token::Word(value) | Token::QuotedIdent(value)) => value,
            _ => return Err(SqlError::Syntax),
        };
        let value = truncate(value, 63).to_owned();
        self.at += 1;
        Ok(value)
    }
    pub(super) fn name(&mut self) -> R<String> {
        let value = self.raw_name()?;
        ident(&value)?;
        Ok(value)
    }
    pub(super) fn column_name(&mut self) -> R<String> {
        let value = self.raw_name()?;
        column_ident(&value)?;
        Ok(value)
    }
    pub(super) fn table_name(&mut self) -> R<String> {
        self.table_name_with_schema_error(SqlError::UnknownTable)
    }
    pub(super) fn create_table_name(&mut self) -> R<String> {
        self.table_name_with_schema_error(SqlError::InvalidSchema)
    }
    fn table_name_with_schema_error(&mut self, schema_error: SqlError) -> R<String> {
        let first = self.raw_name()?;
        let (schema, name) = if self.sym('.') {
            (Some(first), self.name()?)
        } else {
            ident(&first)?;
            (None, first)
        };
        self.relations.push(super::schemas::Reference {
            name: name.clone(),
            schema: schema.clone(),
            create: schema_error == SqlError::InvalidSchema,
        });
        Ok(schema.map_or(name.clone(), |s| format!("{s}\0{name}")))
    }
    /// Resolve column, relation.column, or public.relation.column.
    pub(super) fn qualified_name(&mut self) -> R<(Option<String>, String)> {
        let first = self.raw_name()?;
        if !self.sym('.') {
            column_ident(&first)?;
            return Ok((None, first));
        }
        let second = self.raw_name()?;
        if !self.sym('.') {
            ident(&first)?;
            column_ident(&second)?;
            return Ok((Some(first), second));
        }
        self.relations
            .push(super::schemas::Reference::read(second.clone(), Some(first)));
        ident(&second)?;
        Ok((Some(second), self.column_name()?))
    }
}
#[cfg(test)]
#[path = "ident_tests.rs"]
mod tests;
