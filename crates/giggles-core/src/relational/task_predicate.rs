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

/// A task predicate checked once against its table and then evaluated per row
/// (GD-0061 R3-614). A thin layer over [`columns`] and [`eval`]: the only predicate
/// rules are R3-104's. Evaluation still goes through the CHECK path, so the
/// predicate is bound against the catalog state given to [`RowPredicate::compile`].
pub(crate) struct RowPredicate<'a> {
    state: &'a Relational,
    table: &'a TableDef,
    canonical: &'a str,
}

impl<'a> RowPredicate<'a> {
    /// Validates `text` (canonical predicate text) as a predicate on `table`.
    pub(crate) fn compile(
        state: &'a Relational,
        text: &'a crate::task::definition::PredicateText,
        table: &'a TableDef,
    ) -> Result<Self, SqlError> {
        columns(state, table, &text.0)?;
        Ok(Self { state, table, canonical: &text.0 })
    }

    /// True only when the predicate is true on `values`; NULL is false.
    pub(crate) fn matches(&self, values: &[Value]) -> Result<bool, SqlError> {
        self.matches_row(&SqlRow { values: values.to_vec(), version: 0 })
    }

    /// As [`RowPredicate::matches`], on a stored row (keeps its version).
    pub(crate) fn matches_row(&self, row: &SqlRow) -> Result<bool, SqlError> {
        Ok(eval(self.state, self.table, self.canonical, row)? == Some(true))
    }
}

fn render(tokens: &[Token]) -> R<String> {
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
