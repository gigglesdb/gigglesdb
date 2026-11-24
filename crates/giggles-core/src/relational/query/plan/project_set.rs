//! Lower a scalar set-returning projection to a lateral FROM function.
use super::*;

fn call(tokens: &[Token], at: usize) -> Option<(&str, usize)> {
    let name = |at| match tokens.get(at) {
        Some(Token::Word(word) | Token::QuotedIdent(word)) => Some(word.as_str()),
        _ => None,
    };
    let (name_at, open) =
        if name(at)? == "pg_catalog" && tokens.get(at + 1) == Some(&Token::Symbol('.')) {
            (at + 2, at + 3)
        } else {
            (at, at + 1)
        };
    let name = name(name_at)?;
    (tokens.get(open) == Some(&Token::Symbol('(')) && srf::projection_function(name))
        .then_some((name, open))
}

pub(super) fn claims(tokens: &[Token]) -> bool {
    tokens
        .iter()
        .enumerate()
        .any(|(i, _)| (i == 0 || tokens[i - 1] != Token::Symbol('.')) && call(tokens, i).is_some())
}

fn word(name: &str) -> Token {
    Token::Word(name.into())
}
fn ident(name: &str) -> Token {
    Token::QuotedIdent(name.into())
}

struct Projection {
    positions: Vec<usize>,
    items: Vec<usize>,
    tail: usize,
}

impl Projection {
    fn calls<'a>(&self, tokens: &'a [Token]) -> impl Iterator<Item = (usize, &'a str, usize)> {
        self.items
            .iter()
            .copied()
            .filter_map(|i| call(tokens, i).map(|(name, open)| (i, name, open)))
    }
}

fn clause(tokens: &[Token], i: usize) -> bool {
    parse::unqualified(tokens, i)
        && ([
            "from", "where", "having", "window", "order", "limit", "offset", "fetch", "for",
        ]
        .iter()
        .any(|name| parse::word(&tokens[i], name))
            || parse::group_clause(tokens, i))
}

pub(super) fn requires_plan(tokens: &[Token]) -> bool {
    projection(tokens)
        .ok()
        .flatten()
        .is_some_and(|projection| projection.calls(tokens).next().is_some())
}

fn projection(tokens: &[Token]) -> R<Option<Projection>> {
    if !tokens
        .first()
        .is_some_and(|token| parse::word(token, "select"))
        || !claims(tokens)
    {
        return Ok(None);
    }

    let positions = parse::top(tokens)?;
    let tail = positions
        .iter()
        .copied()
        .skip(1)
        .find(|&i| {
            clause(tokens, i)
                && !(parse::word(&tokens[i], "from") && parse::word(&tokens[i - 1], "distinct"))
        })
        .unwrap_or(tokens.len());
    let start = if tokens
        .get(1)
        .is_some_and(|t| parse::word(t, "distinct") || parse::word(t, "all"))
    {
        2
    } else {
        1
    };
    let mut items = vec![start];
    let mut array_depth = 0usize;
    for i in positions
        .iter()
        .copied()
        .filter(|&i| (start..tail).contains(&i))
    {
        match tokens[i] {
            Token::Symbol('[') => array_depth += 1,
            Token::Symbol(']') => array_depth = array_depth.saturating_sub(1),
            Token::Symbol(',') if array_depth == 0 => items.push(i + 1),
            _ => {}
        }
    }
    Ok(Some(Projection {
        positions,
        items,
        tail,
    }))
}

pub(super) fn rewrite(tokens: &[Token]) -> R<Option<Vec<Token>>> {
    let Some(projection) = projection(tokens)? else {
        return Ok(None);
    };
    let Projection {
        ref positions,
        ref items,
        tail,
    } = projection;
    let start = items[0];
    let mut calls = projection.calls(tokens);
    let Some((at, name, open)) = calls.next() else {
        return Ok(None);
    };
    // A lateral join can lower one direct scalar SRF. Filters, grouping, and
    // windows need an expansion operator after those stages. A bare star
    // would also expose the synthetic lateral column.
    let aggregates = tokens[start..tail].windows(2).any(|pair| {
        matches!(&pair[0], Token::Word(name) if Aggregate::from_name(name).is_some())
            && pair[1] == Token::Symbol('(')
    });
    if calls.next().is_some()
        || aggregates
        || items
            .iter()
            .any(|&i| tokens.get(i) == Some(&Token::Symbol('*')))
        || positions.iter().copied().any(|i| {
            parse::unqualified(tokens, i)
                && (parse::group_clause(tokens, i)
                    || ["where", "having", "window", "over"]
                        .iter()
                        .any(|name| parse::word(&tokens[i], name)))
        })
    {
        return Err(SqlError::Unsupported);
    }
    let close = subquery_end(tokens, open)?;
    let end = items
        .iter()
        .copied()
        .find(|&i| i > at)
        .map_or(tail, |i| i - 1);
    let mut alias = None;
    if close + 1 < end {
        let mut parser = parse::parser(&tokens[close + 1..end]);
        parser.word("as");
        alias = Some(parser.column_name()?);
        if parser.at != parser.tokens.len() {
            return Err(SqlError::Unsupported);
        }
    }
    let alias = alias.as_deref().unwrap_or(name);
    let fresh = |prefix: &str| {
        (0..)
            .map(|i| format!("{prefix}_{i}"))
            .find(|name| {
                !tokens
                    .iter()
                    .any(|t| matches!(t, Token::Word(w) | Token::QuotedIdent(w) if w == name))
            })
            .expect("finite identifiers")
    };
    let relation = fresh("project_set");
    let value = fresh("project_value");
    let mut rewritten = tokens[..at].to_vec();
    rewritten.extend([
        ident(&relation),
        Token::Symbol('.'),
        ident(&value),
        word("as"),
        ident(alias),
    ]);
    rewritten.extend_from_slice(&tokens[end..tail]);

    let has_from = tokens.get(tail).is_some_and(|t| parse::word(t, "from"));
    let from_end = if has_from {
        positions
            .iter()
            .copied()
            .find(|&i| i > tail && clause(tokens, i))
            .unwrap_or(tokens.len())
    } else {
        tail
    };
    if has_from {
        rewritten.extend_from_slice(&tokens[tail..from_end]);
        rewritten.extend([word("cross"), word("join"), word("lateral")]);
    } else {
        rewritten.push(word("from"));
    }

    rewritten.extend_from_slice(&tokens[at..=close]);
    rewritten.extend([
        word("as"),
        ident(&relation),
        Token::Symbol('('),
        ident(&value),
        Token::Symbol(')'),
    ]);
    rewritten.extend_from_slice(&tokens[from_end..]);
    Ok(Some(rewritten))
}
