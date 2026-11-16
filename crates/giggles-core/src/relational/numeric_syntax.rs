//! Core error mapping for the shared PostgreSQL numeric token scanner.
use super::{R, SqlError};
use giggles_pg_types::Error;

pub(super) fn literal(text: &str, start: usize) -> R<(String, usize)> {
    giggles_pg_types::numeric_literal(text, start).map_err(|error| match error {
        Error::Limit => SqlError::Limit,
        Error::Syntax => SqlError::Syntax,
        _ => SqlError::Overflow,
    })
}
