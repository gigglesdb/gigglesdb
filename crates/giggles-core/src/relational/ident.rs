//! Identifier resolution shared by the SQL frontends and stored catalogs.
use super::{Parser, R, SqlError, Token};

pub(super) fn ident(s: &str) -> R<()> {
    if s.is_empty() || s.len() > 63 || s.contains('\0') {
        return Err(SqlError::Syntax);
    }
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
    fn raw_name(&mut self) -> R<String> {
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
    pub(super) fn table_name(&mut self) -> R<String> {
        self.table_name_with_schema_error(SqlError::UnknownTable)
    }
    pub(super) fn create_table_name(&mut self) -> R<String> {
        self.table_name_with_schema_error(SqlError::InvalidSchema)
    }
    fn table_name_with_schema_error(&mut self, schema_error: SqlError) -> R<String> {
        let first = self.raw_name()?;
        if !self.sym('.') {
            ident(&first)?;
            return Ok(first);
        }
        if first != "public" {
            return Err(schema_error);
        }
        self.name()
    }
    /// Resolve column, relation.column, or public.relation.column.
    pub(super) fn qualified_name(&mut self) -> R<(Option<String>, String)> {
        let first = self.raw_name()?;
        if !self.sym('.') {
            ident(&first)?;
            return Ok((None, first));
        }
        let second = self.raw_name()?;
        if !self.sym('.') {
            ident(&first)?;
            ident(&second)?;
            return Ok((Some(first), second));
        }
        if first != "public" {
            return Err(SqlError::UnknownTable);
        }
        ident(&second)?;
        Ok((Some(second), self.name()?))
    }
}
#[cfg(test)]
#[path = "ident_tests.rs"]
mod tests;
