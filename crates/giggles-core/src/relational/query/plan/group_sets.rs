//! Bounded expansion of grouping syntax; transitions stay in query::grouping.
use super::*;
use parse::{ParseResult, group_clause, top, unqualified, word};
type Key = Vec<Token>;
type Sets = Vec<Vec<Key>>;

#[derive(Debug, Clone)]
pub(super) struct GroupingSets {
    masks: Vec<Vec<bool>>,
    distinct: bool,
}

impl GroupingSets {
    pub(super) fn has_keys(&self) -> bool {
        self.masks.first().is_some_and(|mask| !mask.is_empty())
    }
}

/// A parenthesized scalar is parsed by the ordinary GROUP BY parser.
pub(super) fn parenthesized_list(tokens: &[Token], open: usize) -> bool {
    let leading_parens = tokens[open..]
        .iter()
        .take_while(|token| **token == Token::Symbol('('))
        .count();
    let mut depth = 1;
    let mut comma = false;
    for (at, token) in tokens.iter().enumerate().skip(open + 1) {
        match token {
            Token::Symbol('(') => depth += 1,
            Token::Symbol(')') => {
                depth -= 1;
                if depth == 0 {
                    return (comma || at == open + 1)
                        && tokens.get(at + 1).is_none_or(|next| {
                            matches!(next, Token::Symbol(',' | ';')) || ends_group(next)
                        });
                }
            }
            Token::Symbol(',') if depth <= leading_parens => comma = true,
            _ => {}
        }
    }
    false
}

fn list<'a>(tokens: &'a [Token], following: Option<&Token>) -> R<Vec<&'a [Token]>> {
    if tokens.is_empty() {
        return Ok(vec![]);
    }
    let mut start = 0;
    let mut items = vec![];
    for i in top(tokens)? {
        if tokens[i] == Token::Symbol(',') {
            if start == i {
                return Err(clause_syntax(tokens.get(i)));
            }
            items.push(&tokens[start..i]);
            start = i + 1;
        }
    }
    if start == tokens.len() {
        return Err(clause_syntax(following));
    }
    items.push(&tokens[start..]);
    Ok(items)
}
fn product(a: Sets, b: Sets) -> R<Sets> {
    if a.len().saturating_mul(b.len()) > 4096 {
        return Err(SqlError::Limit);
    }
    Ok(a.into_iter()
        .flat_map(|left| {
            b.iter().map(move |right| {
                let mut set = left.clone();
                for key in right {
                    if !set.contains(key) {
                        set.push(key.clone());
                    }
                }
                set
            })
        })
        .collect())
}
fn expand(
    tokens: &[Token],
    depth: usize,
    following: Option<&Token>,
    has_sets: &mut bool,
) -> R<Sets> {
    if depth >= subplans::MAX_DEPTH {
        return Err(SqlError::Limit);
    }
    let items = list(tokens, following)?;
    let mut result = vec![vec![]];
    for item in items {
        let grouping = item.first().is_some_and(|t| word(t, "grouping"))
            && item.get(1).is_some_and(|t| word(t, "sets"));
        let rollup_or_cube = item
            .first()
            .is_some_and(|t| word(t, "rollup") || word(t, "cube"))
            && item.get(1) == Some(&Token::Symbol('('));
        let sets = if grouping || rollup_or_cube {
            *has_sets = true;
            let open = if grouping { 2 } else { 1 };
            if item.get(open) != Some(&Token::Symbol('(')) {
                return Err(clause_syntax(item.get(open).or(following)));
            }
            let close = subquery_end(item, open)?;
            let after = item.get(close + 1);
            if close == open + 1 || after.is_some() {
                return Err(clause_syntax(after.or(item.get(close))));
            }
            let inner = &item[open + 1..close];
            let following = item.get(close);
            if grouping {
                let mut sets = vec![];
                for item in list(inner, following)? {
                    sets.extend(expand(item, depth + 1, following, has_sets)?);
                    if sets.len() > 4096 {
                        return Err(SqlError::Limit);
                    }
                }
                sets
            } else {
                let units = list(inner, following)?
                    .into_iter()
                    .map(|item| expand(item, depth + 1, following, has_sets))
                    .collect::<R<Vec<_>>>()?;
                let cube = word(&item[0], "cube");
                if units.len() > MAX_COLUMNS || cube && units.len() > 12 {
                    return Err(SqlError::Limit);
                }
                let mut sets = vec![];
                let masks: Vec<usize> = if cube {
                    (0..1usize << units.len()).rev().collect()
                } else {
                    (0..=units.len()).rev().collect()
                };
                for mask in masks {
                    let mut combinations = vec![vec![]];
                    for (i, unit) in units.iter().enumerate() {
                        if if cube { mask & (1 << i) != 0 } else { i < mask } {
                            combinations = product(combinations, unit.clone())?;
                        }
                    }
                    sets.extend(combinations);
                    if sets.len() > 4096 {
                        return Err(SqlError::Limit);
                    }
                }
                sets
            }
        } else if item.first() == Some(&Token::Symbol('('))
            && item.last() == Some(&Token::Symbol(')'))
            && top(item)?.len() == 2
        {
            *has_sets |= item.len() == 2;
            expand(&item[1..item.len() - 1], depth + 1, item.last(), has_sets)?
        } else {
            vec![vec![item.to_vec()]]
        };
        result = product(result, sets)?;
    }
    Ok(result)
}
pub(super) fn rewrite(
    tokens: &mut Vec<Token>,
    following: Option<&Token>,
) -> ParseResult<Option<GroupingSets>> {
    let positions = top(tokens)?;
    let Some(group) = positions
        .iter()
        .copied()
        .find(|&i| group_clause(tokens, i))
    else {
        return Ok(None);
    };
    let by = tokens.get(group + 1);
    if !by.is_some_and(|token| word(token, "by")) {
        return Err(ParseError::Definite(clause_syntax(by.or(following))));
    }
    let start = group + 2;
    let end = positions
        .iter()
        .copied()
        .find(|&i| i >= start && unqualified(tokens, i) && ends_group(&tokens[i]))
        .unwrap_or(tokens.len());
    let body_end = tokens.get(end).or(following);
    let body = &tokens[start..end];
    require_group_operand(body.first(), body.get(1), body_end).map_err(ParseError::Definite)?;
    if body.last() == Some(&Token::Symbol(',')) {
        return Err(ParseError::Definite(clause_syntax(body_end)));
    }
    if !Plan::requires_grouping_rewrite(tokens) {
        return Ok(None);
    }
    let distinct = body.first().is_some_and(|t| word(t, "distinct"));
    let modified = distinct || body.first().is_some_and(|t| word(t, "all"));
    let mut has_sets = false;
    let sets = expand(&body[usize::from(modified)..], 0, body_end, &mut has_sets)
        .map_err(ParseError::Definite)?;
    if tokens.get(end).is_some_and(|t| word(t, "having")) && end + 1 == tokens.len() {
        return Err(ParseError::Definite(clause_syntax(following)));
    }
    let mut keys: Vec<Key> = vec![];
    for set in &sets {
        for key in set {
            if !keys.contains(key) {
                keys.push(key.clone());
            }
        }
    }
    let masks = sets
        .iter()
        .map(|set| keys.iter().map(|key| set.contains(key)).collect())
        .collect();
    let mut replacement = vec![];
    for key in keys {
        if !replacement.is_empty() {
            replacement.push(Token::Symbol(','));
        }
        replacement.extend(key);
    }
    if replacement.first().is_some_and(|t| word(t, "distinct") || word(t, "all")) {
        return Err(ParseError::Definite(clause_syntax(replacement.first())));
    }
    // An empty grouping list needs the grouped binder even without aggregates.
    if replacement.is_empty() {
        replacement.extend(lex("0::int")?);
    }
    tokens.splice(start..end, replacement);
    Ok(has_sets.then_some(GroupingSets { masks, distinct }))
}

fn masked(query: &Query, mask: &[bool]) -> Query {
    let mut query = query.clone();
    let filter = query.filter.take();
    let mut clauses = if let Some(Filter::Clauses(c)) = filter {
        *c
    } else {
        QueryClauses {
            filter,
            ..Default::default()
        }
    };
    clauses.grouping_mask = Some(if mask.is_empty() {
        vec![false; clauses.group.len()]
    } else {
        mask.to_vec()
    });
    query.filter = Some(Filter::Clauses(Box::new(clauses)));
    query
}
pub(super) fn validate(
    query: &Query,
    sets: Option<&GroupingSets>,
    state: &Relational,
    parameters: &[DataType],
) -> R<()> {
    if let Some(sets) = sets {
        for mask in &sets.masks {
            masked(query, mask)
                .bind_projection_seeded(state, parameters.iter().cloned().map(Some).collect())?;
        }
    }
    Ok(())
}
#[expect(clippy::too_many_arguments, reason = "shared query execution context")]
pub(super) fn execute(
    query: &Query,
    sets: Option<&GroupingSets>,
    state: &Relational,
    source: &dyn scan::RowSource,
    params: &[Value],
    parameters: &[Option<DataType>],
    clock: Option<&super::super::super::clock::ExecutionClock>,
    execution: &Execution<'_>,
) -> R<SqlResult> {
    let mut result = SqlResult {
        columns: vec![],
        rows: vec![],
        affected: 0,
    };
    let passes: Vec<_> = sets.map_or_else(|| vec![None], |sets| sets.masks.iter().map(Some).collect());
    let mut seen = BTreeSet::new();
    let scope = RetentionScope::new(execution);
    for mask in passes {
        if execution.cancelled() {
            return Err(SqlError::Aborted);
        }
        let query = mask.map_or_else(|| query.clone(), |mask| masked(query, mask));
        let bound = query
            .bind_projection_seeded(state, parameters.to_vec())?
            .with_clock(clock);
        if sets.is_some_and(|sets| sets.distinct)
            && let Some(mask) = bound.grouping.as_ref().and_then(|g| g.mask())
            && !seen.insert(mask.to_vec())
        {
            continue;
        }
        let rows =
            bound.execute_context(state, source, params, parameters, execution, true, false)?;
        result.columns = rows.columns;
        for row in &rows.rows {
            scope.borrow().charge(
                std::mem::size_of::<SqlRow>()
                    + row.values.len() * std::mem::size_of::<Value>()
                    + row.values.iter().map(value_bytes).sum::<usize>(),
            )?;
        }
        result.rows.extend(rows.rows);
        if result.rows.len() > scan::query_row_limit() {
            return Err(SqlError::Limit);
        }
    }
    Ok(result)
}
