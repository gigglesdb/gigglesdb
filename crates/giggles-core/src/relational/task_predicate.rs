//! Task predicates (GD-0061 R3-104): canonical text, column list and row evaluation.
//!
//! Parsing, binding, determinism and evaluation reuse the CHECK machinery in
//! `query/constraints.rs`; the stored text is our own token-canonical form, not the
//! CHECK deparse. A predicate is bound against its task's table alone, may name only
//! unqualified columns, and must be immutable (no clock, sequence, session or parameter
//! nodes). A row passes only when `eval` returns `Some(true)`; NULL does not pass.
// Callers (task catalog, row predicates, decide) land in later R3 items.
#![cfg_attr(not(test), allow(dead_code))]
use super::*;

/// Validates `sql` as a task predicate on `table` and returns its canonical text:
/// words lower-cased, identifiers quoted only when needed, string literals single-quoted
/// with doubled quotes, one space between tokens. Canonical text is a fixed point.
pub(crate) fn canonical(state: &Relational, table: &TableDef, sql: &str) -> Result<String, SqlError> {
    let tokens = lex(sql)?;
    query::named_args::reject_stored(&tokens)?;
    let text = render(&tokens)?;
    let again = lex(&text)?;
    if render(&again)? != text
        || query::constraint_shape(&tokens)? != query::constraint_shape(&again)?
    {
        return Err(SqlError::Unsupported);
    }
    query::constraint_columns(state, table, &again)?;
    Ok(text)
}

/// Validates `canonical` and lists the table columns it reads, by their catalog names.
pub(crate) fn columns(
    state: &Relational,
    table: &TableDef,
    canonical: &str,
) -> Result<BTreeSet<String>, SqlError> {
    query::constraint_columns(state, table, &lex(canonical)?)
}

/// Evaluates a canonical predicate (from [`canonical`]) on one row of `table`, through
/// the same path CHECK constraints use. `Some(true)` is the only passing result.
pub(crate) fn eval(
    state: &Relational,
    table: &TableDef,
    canonical: &str,
    row: &SqlRow,
) -> Result<Option<bool>, SqlError> {
    query::constraint_eval(state, table, canonical, row)
}

pub(super) fn render(tokens: &[Token]) -> R<String> {
    let mut out = String::new();
    for token in tokens {
        if !out.is_empty() {
            out.push(' ');
        }
        match token {
            Token::Word(word) => out.push_str(word),
            Token::QuotedIdent(name) => out.push_str(&query::constraint_quote(name)),
            Token::String(value) => {
                out.push('\'');
                out.push_str(&value.replace('\'', "''"));
                out.push('\'');
            }
            Token::Number(number) => out.push_str(number),
            Token::Param(_) => return Err(SqlError::Unsupported),
            Token::Symbol(symbol) => out.push(*symbol),
            Token::Op(op) => out.push_str(op),
        }
    }
    Ok(out)
}
