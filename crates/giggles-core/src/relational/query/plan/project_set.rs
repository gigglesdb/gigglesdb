//! Lower a scalar set-returning projection to a lateral FROM function.
use super::*;

fn function_call(tokens: &[Token], at: usize) -> Option<(&str, usize)> {
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
    (tokens.get(open) == Some(&Token::Symbol('('))).then_some((name, open))
}

fn call(tokens: &[Token], at: usize) -> Option<(&str, usize)> {
    function_call(tokens, at).filter(|(name, _)| srf::projection_function(name))
}

fn subscripted_regex(tokens: &[Token], at: usize) -> Option<(&str, usize)> {
    if tokens.get(at) != Some(&Token::Symbol('(')) {
        return None;
    }
    let (name, open) = function_call(tokens, at + 1)?;
    if name != "regexp_matches" {
        return None;
    }
    let close = subquery_end(tokens, open).ok()?;
    matches!(tokens.get(close + 1..), Some([Token::Symbol(')'), Token::Symbol('['), ..]))
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
    fn ungrouped_having(&self, tokens: &[Token]) -> bool {
        let mut clauses = self.positions.iter().copied().filter(|&i| i >= self.tail);
        clauses
            .clone()
            .any(|i| parse::unqualified(tokens, i) && parse::word(&tokens[i], "having"))
            && !clauses.any(|i| parse::group_clause(tokens, i))
    }

    fn calls<'a>(
        &self,
        tokens: &'a [Token],
        regex_source: bool,
    ) -> impl Iterator<Item = (usize, usize, &'a str, usize)> {
        self.items.iter().copied().filter_map(move |item| {
            if regex_source {
                subscripted_regex(tokens, item).map(|(name, open)| (item, item + 1, name, open))
            } else {
                call(tokens, item).map(|(name, open)| (item, item, name, open))
            }
        })
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
    projection(tokens, claims(tokens))
        .ok()
        .flatten()
        .is_some_and(|projection| {
            !projection.ungrouped_having(tokens) && projection.calls(tokens, false).next().is_some()
        })
}

fn projection(tokens: &[Token], claimed: bool) -> R<Option<Projection>> {
    if !tokens
        .first()
        .is_some_and(|token| parse::word(token, "select"))
        || !claimed
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

pub(super) fn rewrite(tokens: &[Token], distinct_on: bool) -> R<Option<Vec<Token>>> {
    rewrite_projection(tokens, distinct_on, false, claims(tokens))
}

pub(in crate::relational) fn rewrite_regex_source(tokens: &[Token]) -> R<Option<Vec<Token>>> {
    // Plain regex projections have their own executor. Lower the parenthesized
    // regexp_matches subscript used in a SELECT source through the FROM planner.
    let claimed = tokens.iter().any(
        |token| matches!(token, Token::Word(name) | Token::QuotedIdent(name) if name == "regexp_matches"),
    );
    rewrite_projection(tokens, false, true, claimed)
}

fn rewrite_projection(
    tokens: &[Token],
    distinct_on: bool,
    regex_source: bool,
    claimed: bool,
) -> R<Option<Vec<Token>>> {
    let Some(projection) = projection(tokens, claimed)? else {
        return Ok(None);
    };
    let Projection {
        ref positions,
        ref items,
        tail,
    } = projection;
    let start = items[0];
    let mut calls = projection.calls(tokens, regex_source);
    let Some((item, at, name, open)) = calls.next() else {
        return Ok(None);
    };
    if distinct_on {
        // DISTINCT ON can select its rows before expanding the projection.
        return Err(SqlError::Unsupported);
    }
    if projection.ungrouped_having(tokens) {
        // The ordinary grouping binder diagnoses ungrouped column references.
        return Ok(None);
    }
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
                        .any(|name| parse::word(&tokens[i], name))
                    || regex_source && parse::word(&tokens[i], "for"))
        })
    {
        return Err(SqlError::Unsupported);
    }
    let close = subquery_end(tokens, open)?;
    let end = items
        .iter()
        .copied()
        .find(|&i| i > item)
        .map_or(tail, |i| i - 1);
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
    rewritten.extend([ident(&relation), Token::Symbol('.'), ident(&value)]);
    if regex_source {
        rewritten.extend_from_slice(&tokens[close + 1..end]);
        let mut parser = parse::parser(&rewritten[item..]);
        Scalar::parse_expr(&mut parser, 0, false)?;
        if parser.at == parser.tokens.len() {
            rewritten.extend([word("as"), ident(name)]);
        }
    } else {
        let mut parser = parse::parser(&tokens[close + 1..end]);
        parser.word("as");
        let alias = if close + 1 < end {
            parser.column_name()?
        } else {
            name.to_owned()
        };
        if parser.at != parser.tokens.len() {
            return Err(SqlError::Unsupported);
        }
        rewritten.extend([word("as"), ident(&alias)]);
    }
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
