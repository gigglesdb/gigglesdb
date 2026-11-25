//! Recognize the wire gateway's single advisory call and lower its key expression.
use super::*;

pub(super) const KEY_PREFIX: &str = "\0advisory_key:";

pub(super) fn is_key_expression(name: &str) -> bool {
    name.starts_with(KEY_PREFIX)
}

#[derive(Clone, Copy)]
pub enum AdvisoryResultCast<'a> {
    Sql(&'a str),
    Name,
}

pub struct AdvisoryCall<'a> {
    sql: &'a str,
    name: &'static str,
    open: usize,
    close: usize,
    two_keys: bool,
    column: String,
    result_cast: Option<AdvisoryResultCast<'a>>,
}

impl<'a> AdvisoryCall<'a> {
    pub fn parse(sql: &'a str) -> R<Option<Self>> {
        if !mentions_advisory(sql) {
            return Ok(None);
        }
        let mut tokens = Vec::new();
        let mut spans = Vec::new();
        if lex_with_spans(sql, &mut tokens, false, Some(&mut spans)).is_err() {
            return Ok(None);
        }
        let Some((name, open)) = call_head(&tokens) else {
            return Ok(None);
        };
        let mut nesting = 0;
        let mut commas = 0;
        let Some(close) = (open + 1..tokens.len()).find(|&i| {
            match tokens[i] {
                Token::Symbol(')') if nesting == 0 => return true,
                Token::Symbol('(' | '[') => nesting += 1,
                Token::Symbol(')' | ']') => nesting -= 1,
                Token::Symbol(',') if nesting == 0 => commas += 1,
                _ => {}
            }
            false
        }) else {
            return Ok(None);
        };
        let mut parser = Parser {
            tokens,
            at: close + 1,
            params: BTreeSet::new(),
            relations: Vec::new(),
            privilege_seen: false,
            privilege_casts: false,
        };
        let result_cast = if result_type(&mut parser, "name") {
            Some(AdvisoryResultCast::Name)
        } else if !returns_boolean(name) && result_type(&mut parser, "void") {
            None
        } else if parser.tokens.get(parser.at) == Some(&Token::Symbol(':')) {
            let start = spans[parser.at].start;
            Scalar::parse_advisory_cast(&mut parser, returns_boolean(name))?;
            Some(AdvisoryResultCast::Sql(
                &sql[start..spans[parser.at - 1].end],
            ))
        } else {
            None
        };
        let column = Query::parse_projection_alias(&mut parser)?.unwrap_or_else(|| name.into());
        parser.sym(';');
        if parser.at != parser.tokens.len() {
            return Ok(None);
        }
        Ok(Some(Self {
            sql,
            name,
            open,
            close,
            two_keys: commas == 1,
            column,
            result_cast,
        }))
    }

    /// Classify the SELECT head without binding its arguments or casts.
    pub fn is_select(sql: &str) -> bool {
        mentions_advisory(sql) && lex(sql).is_ok_and(|tokens| call_head(&tokens).is_some())
    }

    pub fn sql(&self) -> &str {
        self.sql
    }
    pub fn name(&self) -> &'static str {
        self.name
    }
    pub fn two_keys(&self) -> bool {
        self.two_keys
    }
    pub fn column(&self) -> &str {
        &self.column
    }
    pub fn result_cast(&self) -> Option<AdvisoryResultCast<'a>> {
        self.result_cast
    }
    pub fn returns_boolean(&self) -> bool {
        returns_boolean(self.name)
    }

    pub(in crate::relational) fn key_tokens(&self, mut tokens: Vec<Token>) -> Vec<Token> {
        tokens.truncate(self.close + 1);
        tokens.splice(
            1..self.open,
            [Token::Word(format!("{KEY_PREFIX}{}", self.name))],
        );
        tokens
    }
}

fn result_type(parser: &mut Parser, target: &str) -> bool {
    let [Token::Symbol(':'), Token::Symbol(':'), rest @ ..] = &parser.tokens[parser.at..] else {
        return false;
    };
    let qualifier = match rest {
        [
            Token::Word(schema) | Token::QuotedIdent(schema),
            Token::Symbol('.'),
            ..,
        ] if schema == "pg_catalog" => 2,
        _ => 0,
    };
    if matches!(rest.get(qualifier), Some(Token::Word(name) | Token::QuotedIdent(name)) if name == target)
    {
        parser.at += qualifier + 3;
        true
    } else {
        false
    }
}

fn mentions_advisory(sql: &str) -> bool {
    sql.as_bytes()
        .windows(9)
        .any(|word| word.eq_ignore_ascii_case(b"advisory_"))
}

fn call_head(tokens: &[Token]) -> Option<(&'static str, usize)> {
    fn identifier(token: Option<&Token>) -> Option<&str> {
        match token {
            Some(Token::Word(name) | Token::QuotedIdent(name)) => Some(name),
            _ => None,
        }
    }
    if !matches!(tokens.first(), Some(Token::Word(word)) if word == "select") {
        return None;
    }
    let qualified = tokens.get(2) == Some(&Token::Symbol('.'));
    if qualified && identifier(tokens.get(1)) != Some("pg_catalog") {
        return None;
    }
    let routine = if qualified { 3 } else { 1 };
    let name = function_name(identifier(tokens.get(routine))?)?;
    (tokens.get(routine + 1) == Some(&Token::Symbol('('))).then_some((name, routine + 1))
}

fn returns_boolean(name: &str) -> bool {
    name.starts_with("pg_try_")
        || matches!(name, "pg_advisory_unlock" | "pg_advisory_unlock_shared")
}

fn function_name(name: &str) -> Option<&'static str> {
    const NAMES: &[&str] = &[
        "pg_advisory_lock",
        "pg_advisory_lock_shared",
        "pg_advisory_xact_lock",
        "pg_advisory_xact_lock_shared",
        "pg_try_advisory_lock",
        "pg_try_advisory_lock_shared",
        "pg_try_advisory_xact_lock",
        "pg_try_advisory_xact_lock_shared",
        "pg_advisory_unlock",
        "pg_advisory_unlock_shared",
        "pg_advisory_unlock_all",
    ];
    NAMES.iter().copied().find(|&candidate| candidate == name)
}
