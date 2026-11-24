//! SELECT input shared by INSERT and CREATE TABLE AS.
use super::*;

#[derive(Debug, Clone)]
pub(super) enum SelectSource {
    Query(Box<query::Query>),
    Plan(Box<query::plan::Plan>, usize),
}

impl SelectSource {
    pub(super) fn starts(token: Option<&Token>) -> bool {
        matches!(token, Some(Token::Word(word)) if matches!(word.as_str(), "select" | "table" | "with" | "values"))
            || token == Some(&Token::Symbol('('))
    }

    pub(super) fn parse(owner: &mut Parser, end: usize) -> R<Self> {
        let tokens = &owner.tokens[owner.at..end];
        if !Self::starts(tokens.first()) {
            return Err(SqlError::Syntax);
        }
        let parser = |tokens: &[Token]| Parser {
            tokens: tokens.to_vec(),
            at: 0,
            params: BTreeSet::new(),
            relations: vec![],
            privilege_seen: false,
            privilege_casts: false,
        };
        // Keep the ordinary Query path for simple expressions, including
        // parenthesized SELECTs and INSERT's assignment-aware streaming path.
        let mut inner = tokens;
        while let [Token::Symbol('('), .., Token::Symbol(')')] = inner {
            let mut depth = 0usize;
            let close = inner.iter().position(|token| {
                if *token == Token::Symbol('(') {
                    depth += 1;
                } else if *token == Token::Symbol(')') {
                    depth -= 1;
                }
                depth == 0
            });
            if close != Some(inner.len() - 1) {
                break;
            }
            inner = &inner[1..inner.len() - 1];
        }

        let mut p = parser(inner);
        let source = match query::Query::parse(&mut p) {
            Ok(query) if !query.explain => Self::Query(Box::new(query)),
            Ok(_) => return Err(SqlError::Syntax),
            Err(error @ (SqlError::Syntax | SqlError::Unsupported | SqlError::Reserved))
                if query::plan::Plan::claims(tokens) || query::plan::Plan::claims(inner) =>
            {
                p = parser(tokens);
                let plan = query::plan::Plan::parse(&mut p).map_err(|failure| match failure {
                    query::plan::ParseError::Decline => error,
                    query::plan::ParseError::Definite(error) => error,
                })?;
                // These destinations use the read executor. A data-modifying
                // WITH belongs to the outer statement's DML executor.
                if !plan.write_tables().is_empty() {
                    return Err(SqlError::Unsupported);
                }
                let count = p.params.last().copied().unwrap_or(0);
                Self::Plan(Box::new(plan), count)
            }
            Err(error) => return Err(error),
        };
        owner.privilege_seen |= p.privilege_seen;
        owner.params.extend(p.params);
        owner.relations.extend(p.relations);
        owner.at = end;
        Ok(source)
    }

    pub(super) fn query(&self) -> Option<&query::Query> {
        match self {
            Self::Query(query) => Some(query),
            Self::Plan(..) => None,
        }
    }

    pub(super) fn read_tables(&self) -> Vec<String> {
        match self {
            Self::Query(query) => query.read_tables(),
            Self::Plan(plan, _) => plan.read_tables(),
        }
    }

    pub(super) fn authorization_tables(&self, invoker: bool) -> R<Vec<String>> {
        match self {
            Self::Query(query) if invoker => query.invoker_authorization_tables(),
            Self::Query(query) => query.authorization_tables(),
            Self::Plan(plan, _) => plan.authorization_tables(),
        }
    }

    pub(super) fn bind_schema_names(
        &mut self,
        bind: &impl Fn(&mut String),
        state: &Relational,
        path: &[String],
    ) -> R<()> {
        match self {
            Self::Query(query) => query.bind_schema_names(bind, state, path),
            Self::Plan(plan, _) => plan.bind_schema_names(bind, state, path)?,
        }
        Ok(())
    }

    pub(super) fn has_privilege_inquiry(&self) -> bool {
        match self {
            Self::Query(query) => query.has_privilege_inquiry(),
            Self::Plan(plan, _) => plan.has_privilege_inquiry(),
        }
    }

    pub(super) fn sequences(&self) -> Vec<SequenceDependency> {
        match self {
            Self::Query(query) => query.sequence_dependencies(),
            Self::Plan(plan, _) => plan.sequences(),
        }
    }
}
