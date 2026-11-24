//! Build parameterized SQL once. Runtime supplies values in template order.
use super::lexer::{Token, eof, syntax_token};
use super::{ArgIndex, PlCompileError, Reference, Sql};
use crate::relational::{Prepared, SqlError};
use std::ops::Range;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Binding {
    Scalar,
    Record,
    TriggerArguments,
}

pub(super) fn template(
    body: &str,
    tokens: &[Token<'_>],
    prefix: &str,
    omit: Option<Range<usize>>,
    lookup: &dyn Fn(&str) -> Option<Binding>,
) -> Result<Sql, PlCompileError> {
    let start = tokens.first().expect("nonempty SQL fragment").start;
    let end = tokens.last().expect("nonempty SQL fragment").end();
    let mut text = String::from(prefix);
    let mut params = Vec::new();
    let mut copied = start;
    let mut i = 0;
    while i < tokens.len() {
        let token = &tokens[i];
        if let Some(omit) = &omit
            && token.start == omit.start
        {
            text.push_str(&body[copied..omit.start]);
            copied = omit.end;
            i += tokens[i..].partition_point(|token| token.start < omit.end);
            continue;
        }
        if let Some((count, reference)) = reference(body, tokens, i, lookup)? {
            text.push_str(&body[copied..token.start]);
            let slot = if matches!(reference, Reference::Arg(ArgIndex::Expression(_))) {
                None
            } else {
                params.iter().position(|previous| previous == &reference)
            };
            let index = slot.unwrap_or_else(|| {
                params.push(reference);
                params.len() - 1
            });
            use std::fmt::Write;
            write!(text, "${}", index + 1).expect("write to String");
            copied = tokens[i + count - 1].end();
            i += count;
        } else {
            i += 1;
        }
    }
    text.push_str(&body[copied..end]);

    // The relational parser is intentionally a SQL subset. A rejection alone
    // does not establish a PostgreSQL syntax error, and may be semantic.
    if matches!(
        Prepared::parse(&text, 0, 0),
        Err(SqlError::Syntax | SqlError::Unsupported | SqlError::Reserved)
    ) {
        validate_rejected(tokens, prefix, end)?;
    }

    Ok(Sql {
        text: text.trim_end().into(),
        original: body[start..end].into(),
        params: params.into_boxed_slice(),
    })
}

fn reference(
    body: &str,
    tokens: &[Token<'_>],
    i: usize,
    lookup: &dyn Fn(&str) -> Option<Binding>,
) -> Result<Option<(usize, Reference)>, PlCompileError> {
    let token = &tokens[i];
    let Some(name) = token.identifier() else {
        return Ok(None);
    };
    let prev = i.checked_sub(1).and_then(|i| tokens.get(i));
    let next = tokens.get(i + 1);
    // SQL namespace positions are independent of PL variables.
    if prev.is_some_and(|t| {
        matches!(t.raw, "." | "::")
            || ["as", "collate", "join", "update", "into", "using"]
                .iter()
                .any(|kw| t.word(kw))
            || (t.word("from") && !is_distinct_from(tokens, i))
    }) || next.is_some_and(|t| matches!(t.raw, "(" | "=>"))
        || (!token.quoted() && is_sql_keyword(token))
        || is_update_target(tokens, i)
        || is_insert_column(tokens, i)
    {
        return Ok(None);
    }

    let binding = lookup(name);
    if next.is_some_and(|t| t.raw == ".") {
        return Ok(match binding {
            Some(Binding::Record) if tokens.get(i + 3).is_none_or(|t| t.raw != ".") => tokens
                .get(i + 2)
                .and_then(Token::identifier)
                .map(|field| (3, Reference::Field(name.into(), field.into()))),
            _ => None,
        });
    }
    Ok(match binding {
        Some(Binding::Scalar) => Some((1, Reference::Scalar(name.into()))),
        Some(Binding::Record) if next.is_some_and(|t| t.word("is")) => {
            let negated = tokens.get(i + 2).is_some_and(|t| t.word("not"));
            let null = i + 2 + usize::from(negated);
            tokens.get(null).filter(|t| t.word("null")).map(|_| {
                (
                    null - i + 1,
                    Reference::RecordNull {
                        name: name.into(),
                        negated,
                    },
                )
            })
        }
        Some(Binding::TriggerArguments) if next.is_some_and(|t| t.raw == "[") => {
            let mut depth = 1;
            let mut end = i + 2;
            while let Some(t) = tokens.get(end) {
                match t.raw {
                    "[" => depth += 1,
                    "]" => depth -= 1,
                    _ => {}
                }
                if depth == 0 {
                    break;
                }
                end += 1;
            }
            let inner = &tokens[i + 2..end];
            let close = tokens
                .get(end)
                .ok_or_else(|| eof(tokens.last().unwrap().end()))?;
            let index = match inner {
                [] => return Err(syntax_token(close)),
                [n] if n.raw.parse::<i64>().is_ok() => {
                    ArgIndex::Literal(n.raw.parse().expect("integer literal"))
                }
                _ => {
                    ArgIndex::Expression(Box::new(template(body, inner, "SELECT ", None, lookup)?))
                }
            };
            Some((end - i + 1, Reference::Arg(index)))
        }
        _ => None,
    })
}

fn is_distinct_from(tokens: &[Token<'_>], i: usize) -> bool {
    i >= 2 && tokens[i - 2].word("distinct")
}

fn is_update_target(tokens: &[Token<'_>], i: usize) -> bool {
    let Some(prev) = i.checked_sub(1).and_then(|i| tokens.get(i)) else {
        return false;
    };
    if !tokens.get(i + 1).is_some_and(|t| t.raw == "=") {
        return false;
    }
    if prev.word("set") {
        return true;
    }
    if prev.raw != "," {
        return false;
    }
    let mut depth = 0usize;
    for token in tokens[..i - 1].iter().rev() {
        match token.raw {
            ")" => depth += 1,
            "(" if depth == 0 => break,
            "(" => depth -= 1,
            _ if depth != 0 => {}
            _ if token.word("set") => return true,
            _ if token.word("where") || token.word("select") || token.word("returning") => break,
            _ => {}
        }
    }
    false
}

fn is_insert_column(tokens: &[Token<'_>], i: usize) -> bool {
    let Some(prev) = i.checked_sub(1).and_then(|i| tokens.get(i)) else {
        return false;
    };
    if !matches!(prev.raw, "(" | ",") {
        return false;
    }
    let mut start = i - 1;
    while tokens[start].raw == "," && start >= 2 && tokens[start - 1].identifier().is_some() {
        start -= 2;
    }
    if tokens[start].raw != "(" || start < 2 {
        return false;
    }
    let mut relation = start - 1;
    while relation >= 2 && tokens[relation - 1].raw == "." {
        relation -= 2;
    }
    relation >= 2 && tokens[relation - 1].word("into") && tokens[relation - 2].word("insert")
}

fn is_sql_keyword(token: &Token<'_>) -> bool {
    matches!(
        token.identifier(),
        Some(
            "all"
                | "and"
                | "any"
                | "array"
                | "as"
                | "asc"
                | "between"
                | "by"
                | "case"
                | "cast"
                | "collate"
                | "cross"
                | "default"
                | "delete"
                | "desc"
                | "distinct"
                | "else"
                | "end"
                | "except"
                | "exists"
                | "false"
                | "fetch"
                | "filter"
                | "first"
                | "for"
                | "from"
                | "full"
                | "group"
                | "having"
                | "ilike"
                | "in"
                | "inner"
                | "insert"
                | "intersect"
                | "into"
                | "is"
                | "join"
                | "last"
                | "lateral"
                | "left"
                | "like"
                | "limit"
                | "not"
                | "null"
                | "nulls"
                | "offset"
                | "on"
                | "or"
                | "order"
                | "outer"
                | "over"
                | "recursive"
                | "returning"
                | "right"
                | "select"
                | "set"
                | "some"
                | "then"
                | "true"
                | "union"
                | "update"
                | "using"
                | "values"
                | "when"
                | "where"
                | "window"
                | "with"
        )
    )
}

pub(super) fn command(tokens: &[Token<'_>]) -> usize {
    if !tokens[0].word("with") {
        return 0;
    }
    let mut depth = 0usize;
    for (i, t) in tokens.iter().enumerate().skip(1) {
        match t.raw {
            "(" => depth += 1,
            ")" => depth = depth.saturating_sub(1),
            _ if depth == 0
                && ["select", "insert", "update", "delete"]
                    .iter()
                    .any(|word| t.word(word)) =>
            {
                return i;
            }
            _ => {}
        }
    }
    0
}

fn validate_rejected(tokens: &[Token<'_>], prefix: &str, end: usize) -> Result<(), PlCompileError> {
    if prefix.is_empty() {
        let head = command(tokens);
        let required = if tokens[head].word("delete") {
            Some("from")
        } else if tokens[head].word("insert") {
            Some("into")
        } else {
            None
        };
        if let Some(keyword) = required {
            let next = tokens.get(head + 1).ok_or_else(|| eof(end))?;
            if !next.word(keyword) {
                return Err(syntax_token(next));
            }
            if tokens.get(head + 2).is_none() {
                return Err(eof(end));
            }
        }
    }
    for pair in tokens.windows(2) {
        let [clause, next] = pair else { unreachable!() };
        if (clause.word("where") || clause.word("having"))
            && (matches!(next.raw, "," | ")")
                || ["and", "or", "order", "group", "limit", "returning"]
                    .iter()
                    .any(|word| next.word(word)))
        {
            return Err(syntax_token(next));
        }
    }
    let last = tokens.last().expect("nonempty SQL fragment");
    if matches!(last.raw, "+" | "-" | "/" | "=" | "||" | "," | "." | "::")
        || ["and", "or", "where", "having", "into", "returning", "set"]
            .iter()
            .any(|word| last.word(word))
    {
        return Err(eof(end));
    }
    Ok(())
}
