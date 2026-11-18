//! Bounded expansion of grouping syntax; transitions stay in query::grouping.
use super::*;
use parse::{ParseResult, top, word};
type Key = Vec<Token>;
type Sets = Vec<Vec<Key>>;

fn list(tokens: &[Token]) -> R<Vec<&[Token]>> {
    if tokens.is_empty() {
        return Ok(vec![]);
    }
    let mut start = 0;
    let mut items = vec![];
    for i in top(tokens)? {
        if tokens[i] == Token::Symbol(',') {
            items.push(&tokens[start..i]);
            start = i + 1;
        }
    }
    items.push(&tokens[start..]);
    if items.iter().any(|s| s.is_empty()) {
        return Err(SqlError::Syntax);
    }
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
fn expand(tokens: &[Token], depth: usize) -> R<Sets> {
    if depth >= subplans::MAX_DEPTH {
        return Err(SqlError::Limit);
    }
    let items = list(tokens)?;
    let mut result = vec![vec![]];
    for item in items {
        let special = item
            .first()
            .is_some_and(|t| ["grouping", "rollup", "cube"].iter().any(|w| word(t, w)));
        let sets = if special {
            let grouping = word(&item[0], "grouping");
            let open = if grouping { 2 } else { 1 };
            if grouping && !item.get(1).is_some_and(|t| word(t, "sets"))
                || item.get(open) != Some(&Token::Symbol('('))
                || item.last() != Some(&Token::Symbol(')'))
            {
                return Err(SqlError::Syntax);
            }
            let inner = &item[open + 1..item.len() - 1];
            if grouping {
                let mut sets = vec![];
                for item in list(inner)? {
                    sets.extend(expand(item, depth + 1)?);
                    if sets.len() > 4096 {
                        return Err(SqlError::Limit);
                    }
                }
                sets
            } else {
                let units = list(inner)?
                    .into_iter()
                    .map(|item| expand(item, depth + 1))
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
            expand(&item[1..item.len() - 1], depth + 1)?
        } else {
            vec![vec![item.to_vec()]]
        };
        result = product(result, sets)?;
    }
    Ok(result)
}
pub(super) fn rewrite(tokens: &mut Vec<Token>) -> ParseResult<Option<Vec<Vec<bool>>>> {
    let positions = top(tokens)?;
    let Some(group) = positions
        .iter()
        .copied()
        .find(|i| word(&tokens[*i], "group"))
    else {
        return Ok(None);
    };
    let end = positions
        .iter()
        .copied()
        .find(|i| *i > group && ["having", "order"].iter().any(|w| word(&tokens[*i], w)))
        .unwrap_or(tokens.len());
    let body = &tokens[group + 2..end];
    if !body
        .iter()
        .any(|t| ["grouping", "rollup", "cube"].iter().any(|w| word(t, w)))
    {
        return Ok(None);
    }
    let sets = expand(body, 0)?;
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
    // An empty grouping list needs the grouped binder even without aggregates.
    if replacement.is_empty() {
        replacement.extend(lex("0::int")?);
    }
    tokens.splice(group + 2..end, replacement);
    Ok(Some(masks))
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
    masks: Option<&[Vec<bool>]>,
    state: &Relational,
    parameters: &[DataType],
) -> R<()> {
    if let Some(masks) = masks {
        for mask in masks {
            masked(query, mask)
                .bind_projection_seeded(state, parameters.iter().cloned().map(Some).collect())?;
        }
    }
    Ok(())
}
#[expect(clippy::too_many_arguments, reason = "shared query execution context")]
pub(super) fn execute(
    query: &Query,
    masks: Option<&[Vec<bool>]>,
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
    let passes: Vec<_> = masks.map_or_else(|| vec![None], |masks| masks.iter().map(Some).collect());
    let scope = RetentionScope::new(execution);
    for mask in passes {
        if execution.cancelled() {
            return Err(SqlError::Aborted);
        }
        let query = mask.map_or_else(|| query.clone(), |mask| masked(query, mask));
        let bound = query
            .bind_projection_seeded(state, parameters.to_vec())?
            .with_clock(clock);
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
