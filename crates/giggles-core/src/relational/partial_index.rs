//! Persistent predicate text and the stable identities of referenced columns.
use super::*;

pub(crate) use query::index_predicate::BoundPredicate;
pub(crate) const MAX_PREDICATE_BYTES: usize = 64 * 1024;

/// The V2 descriptor suffix serializes this struct in field order. DD-0068
/// owns changes to that persisted layout.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Predicate {
    pub(crate) sql: String,
    // In lexicographic order of the distinct referenced names in sql.
    pub(crate) columns: Vec<u64>,
}

impl Predicate {
    pub(super) fn create(state: &Relational, table: &TableDef, tokens: &[Token]) -> R<Self> {
        let predicate = query::index_predicate::create(state, table, tokens, true)?;
        predicate.deparse(state, table)?;
        Ok(predicate)
    }

    pub(super) fn for_conflict(state: &Relational, table: &TableDef, tokens: &[Token]) -> R<Self> {
        query::index_predicate::create(state, table, tokens, false)
    }

    pub(crate) fn bind(&self, state: &Relational, table: &TableDef) -> R<BoundPredicate> {
        query::index_predicate::bind(state, table, self)
    }

    pub(crate) fn implies(&self, required: &Self, state: &Relational, table: &TableDef) -> R<bool> {
        query::index_predicate::implies(self, required, state, table)
    }

    pub(crate) fn deparse(&self, state: &Relational, table: &TableDef) -> R<String> {
        query::index_predicate::deparse(state, table, self)
    }
}

pub(super) fn predicate_tokens(p: &mut Parser, end: &str) -> Vec<Token> {
    let start = p.at;
    let mut depth = 0usize;
    while let Some(token) = p.tokens.get(p.at) {
        match token {
            Token::Symbol(';') if depth == 0 && end == ";" => break,
            Token::Word(word) if depth == 0 && word == end => break,
            Token::Symbol('(' | '[') => depth += 1,
            Token::Symbol(')' | ']') => depth = depth.saturating_sub(1),
            _ => {}
        }
        p.at += 1;
    }
    p.tokens[start..p.at].to_vec()
}

impl Relational {
    pub(crate) fn has_partial_indexes_engine(&self) -> bool {
        self.secondary_descriptors_engine()
            .iter()
            .any(|d| d.predicate.is_some())
    }
}

pub(crate) fn missing_arbiter() -> SqlError {
    user_types::view_replace_error(
        "there is no unique or exclusion constraint matching the ON CONFLICT specification".into(),
    );
    SqlError::InvalidColumnReference
}

pub(crate) fn missing_constraint(table: &TableDef, name: &str) -> SqlError {
    user_types::view_replace_error(format!(
        "constraint \"{name}\" for table \"{}\" does not exist",
        table.name,
    ));
    SqlError::UndefinedObject
}
