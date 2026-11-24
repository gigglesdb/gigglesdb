//! Transient SELECT row marks; these are never catalog or row fields.
use super::*;
use scan::{RowLock, RowWait};
#[derive(Debug, Clone)]
pub(super) struct Clause {
    pub mode: RowLock,
    pub wait: RowWait,
    pub names: Vec<String>,
}
fn word(t: Option<&Token>, name: &str) -> bool { matches!(t, Some(Token::Word(s)) if s == name) }
pub(super) fn extract(p: &mut Parser) -> R<Vec<Clause>> {
    let mut clauses = vec![];
    let mut depth = 0usize;
    let mut i = p.at;
    let mut seen_page = false;
    while i < p.tokens.len() {
        match p.tokens[i] {
            Token::Symbol('(') => { depth += 1; if depth > subplans::MAX_DEPTH { return Err(SqlError::Limit); } }
            Token::Symbol(')') => depth = depth.saturating_sub(1),
            _ => {}
        }
        let qualified = i > p.at && p.tokens[i - 1] == Token::Symbol('.');
        if depth == 0 && !qualified && (word(p.tokens.get(i), "limit") || word(p.tokens.get(i), "offset")) {
            seen_page = true;
        }
        if depth != 0 || qualified || !word(p.tokens.get(i), "for") { i += 1; continue; }
        let mut q = Parser { privilege_seen: false, privilege_casts: false, tokens: p.tokens[i..].to_vec(), at: 1, params: BTreeSet::new(), relations: vec![] };
        let mode = if q.word("update") { RowLock::Update }
            else if q.word("no") { q.need("key")?; q.need("update")?; RowLock::NoKeyUpdate }
            else if q.word("share") { RowLock::Share }
            else { q.need("key")?; q.need("share")?; RowLock::KeyShare };
        let mut names = vec![];
        if q.word("of") { loop { names.push(q.name()?); if !q.sym(',') { break; } } }
        let wait = if q.word("nowait") { RowWait::NoWait }
            else if q.word("skip") { q.need("locked")?; RowWait::Skip }
            else { RowWait::Wait };
        if word(q.tokens.get(q.at), "nowait") || word(q.tokens.get(q.at), "skip") {
            let at = if word(q.tokens.get(q.at), "skip") { "SKIP" } else { "NOWAIT" };
            return Err(fail(format!("syntax error at or near \"{at}\""), SqlError::Syntax));
        }
        // PostgreSQL permits pagination on either side, but not split around a mark.
        if seen_page
            && (word(q.tokens.get(q.at), "limit") || word(q.tokens.get(q.at), "offset")) {
            let at = if word(q.tokens.get(q.at), "offset") { "OFFSET" } else { "LIMIT" };
            return Err(fail(format!("syntax error at or near \"{at}\""), SqlError::Syntax));
        }
        p.tokens.drain(i..i + q.at);
        clauses.push(Clause { mode, wait, names });
    }
    Ok(clauses)
}
fn label(mode: RowLock) -> &'static str {
    match mode { RowLock::Update => "FOR UPDATE", RowLock::NoKeyUpdate => "FOR NO KEY UPDATE",
        RowLock::Share => "FOR SHARE", RowLock::KeyShare => "FOR KEY SHARE" }
}
fn fail(message: String, error: SqlError) -> SqlError {
    user_types::view_replace_error(message); error
}
impl Query {
    pub(in crate::relational) fn has_row_locks(&self) -> bool {
        self.clauses().is_some_and(|c| !c.row_locks.is_empty())
    }
}
impl Query {
    pub(in crate::relational) fn row_lock_tables(&self) -> Vec<String> {
        let mut out = vec![];
        if let Some(c) = self.clauses() {
            for clause in &c.row_locks {
                for source in &self.sources {
                    if clause.names.is_empty() || clause.names.contains(&source.alias) { out.push(source.name.clone()); }
                }
            }
        }
        if let Some(q) = self.derived() { out.extend(q.row_lock_tables()); }
        for f in self.where_filter().into_iter().chain(self.having()).chain(self.joins.iter().map(|j| &j.on)) {
            let _ = f.visit_subqueries(&mut |_, q| { out.extend(q.row_lock_tables()); Ok(()) });
        }
        out.sort(); out.dedup(); out
    }
    pub(in crate::relational) fn row_lock_command(&self) -> Option<&'static str> {
        self.clauses().and_then(|c| c.row_locks.iter().map(|c| c.mode).max()).map(|mode| match mode {
            RowLock::Update => "SELECT FOR UPDATE", RowLock::NoKeyUpdate => "SELECT FOR NO KEY UPDATE",
            RowLock::Share => "SELECT FOR SHARE", RowLock::KeyShare => "SELECT FOR KEY SHARE",
        }).or_else(|| self.derived().and_then(|q| q.row_lock_command())).or_else(|| {
            let mut command = None;
            for f in self.where_filter().into_iter().chain(self.having()).chain(self.joins.iter().map(|j| &j.on)) {
                let _ = f.visit_subqueries(&mut |_, q| { command = command.or_else(|| q.row_lock_command()); Ok(()) });
            }
            command
        })
    }
}

pub(super) fn validate(query: &Query) -> R<()> {
    let Some(clauses) = query.clauses() else { return Ok(()); };
    if !clauses.row_locks.is_empty() && let Some(child) = query.derived()
        && (child.limit.is_some() || child.offset().is_some() || child.distinct()
            || !child.group().is_empty() || child.having().is_some()) {
        return Err(SqlError::Unsupported);
    }
    for clause in &clauses.row_locks {
        let reason = if query.distinct() { Some("DISTINCT clause") }
            else if !query.group().is_empty() { Some("GROUP BY clause") }
            else if clauses.having.is_some() { Some("HAVING clause") }
            else if query.items.iter().any(|(item, _)| matches!(item, Item::Aggregate(..))
                || matches!(item, Item::Scalar(s) if grouping::contains_aggregate(s))) { Some("aggregate functions") }
            else if query.items.iter().any(|(item, _)| {
                matches!(item, Item::Scalar(Scalar::Call(name, _)) if functions::regexp::set_type(name).is_some())
            }) { Some("set-returning functions in the target list") }
            else { None };
        if let Some(reason) = reason { return Err(fail(format!("{} is not allowed with {reason}", label(clause.mode)), SqlError::Unsupported)); }
        for name in &clause.names {
            if !query.sources.iter().any(|s| s.alias == *name) {
                return Err(fail(format!("relation \"{name}\" in {} clause not found in FROM clause", label(clause.mode)), SqlError::UnknownTable));
            }
        }
        for (i, join) in query.joins.iter().enumerate() {
            if join.left && (clause.names.is_empty() || clause.names.contains(&query.sources[i + 1].alias)) {
                return Err(fail(format!("{} cannot be applied to the nullable side of an outer join", label(clause.mode)), SqlError::Unsupported));
            }
        }
    }
    Ok(())
}
pub(super) fn lock(bound: &BoundQuery<'_>, mut row: Tuple, source: &dyn scan::RowSource,
    params: &[Value], state: &Relational, execution: &Execution<'_>) -> R<Option<Tuple>> {
    let Some(clauses) = bound.query.clauses() else { return Ok(Some(row)); };
    if clauses.row_locks.is_empty() { return Ok(Some(row)); }
    // Derived marks have already been pushed to the physical child's scan.
    if bound.query.derived().is_some() { return Ok(Some(row)); }
    let mut refreshed = false;
    for (i, table) in bound.tables.iter().take(bound.query.sources.len()).enumerate() {
        let mut mark = None::<(RowLock, RowWait)>;
        for c in &clauses.row_locks {
            if c.names.is_empty() || c.names.contains(&bound.query.sources[i].alias) {
                mark = Some(mark.map_or((c.mode, c.wait), |(mode, wait)| (mode.max(c.mode), wait.max(c.wait))));
            }
        }
        if let Some((mode, wait)) = mark {
            let Some(current) = source.lock_row(table, &row.values[i], mode, wait)? else { return Ok(None); };
            refreshed |= current.version > row.source_versions.get(i).copied().unwrap_or(row.version);
            row.values[i] = current.values;
            row.version = row.version.max(current.version);
        }
    }
    if !refreshed { return Ok(Some(row)); }
    for (i, join) in bound.joins.iter().enumerate() {
        if !join.matches(&row, &row.values[i + 1])?
            || join.on.eval_with_subqueries(&row, params, &mut 0, bound, state, source, execution)? != Some(true) {
            return Ok(None);
        }
    }
    if let Some(filter) = &bound.filter
        && filter.eval_with_subqueries(&row, params, &mut 0, bound, state, source, execution)? != Some(true) {
        return Ok(None);
    }
    Ok(Some(row))
}

/// Planner-only restrictions must survive its deliberate-error boundary.
pub(in crate::relational::query) fn reject_plan(tokens: &[Token]) -> R<()> {
    let mut depth = 0usize;
    let mut top = vec![];
    for (i, t) in tokens.iter().enumerate() {
        if *t == Token::Symbol(')') { depth = depth.saturating_sub(1); }
        if depth == 0 { top.push(i); }
        if *t == Token::Symbol('(') { depth += 1; if depth > subplans::MAX_DEPTH { return Err(SqlError::Limit); } }
    }
    let Some(mark) = top.iter().copied().find(|i| word(tokens.get(*i), "for")) else { return Ok(()); };
    let mode = if word(tokens.get(mark + 1), "no") { RowLock::NoKeyUpdate }
        else if word(tokens.get(mark + 1), "key") { RowLock::KeyShare }
        else if word(tokens.get(mark + 1), "share") { RowLock::Share } else { RowLock::Update };
    let has = |name| top.iter().any(|i| word(tokens.get(*i), name)
        && (*i == 0 || tokens[*i - 1] != Token::Symbol('.')));
    let reason = if has("union") || has("intersect") || has("except") { Some("UNION/INTERSECT/EXCEPT") }
        else if has("window") || top.iter().any(|i| word(tokens.get(*i), "over")
            && tokens.get(*i + 1) == Some(&Token::Symbol('('))) { Some("window functions") }
        else if has("distinct") { Some("DISTINCT clause") }
        else if has("group") { Some("GROUP BY clause") }
        else if has("having") { Some("HAVING clause") }
        else {
            let end = top.iter().copied().find(|i| word(tokens.get(*i), "from")).unwrap_or(mark);
            top.iter().copied().filter(|i| *i < end).any(|i| {
                let known = match &tokens[i] {
                    Token::Word(name) => functions::regexp::set_type(name).is_some()
                        || ["generate_series", "unnest", "generate_subscripts", "jsonb_array_elements",
                            "jsonb_array_elements_text", "jsonb_each", "jsonb_each_text"].contains(&name.as_str()),
                    _ => false,
                };
                known && tokens.get(i + 1) == Some(&Token::Symbol('('))
            }).then_some("set-returning functions in the target list")
        };
    if let Some(reason) = reason { return Err(fail(format!("{} is not allowed with {reason}", label(mode)), SqlError::Unsupported)); }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn discovers_marks_in_nested_where_join_and_having_queries() {
        for sql in [
            "SELECT 1 WHERE EXISTS(SELECT 1 FROM a WHERE id IN (SELECT id FROM b FOR SHARE) FOR UPDATE)",
            "SELECT a.id FROM a JOIN b ON EXISTS(SELECT 1 FROM c FOR UPDATE)",
            "SELECT count(*) FROM a HAVING EXISTS(SELECT 1 FROM c FOR UPDATE)",
        ] {
            let q = Query::parse(&mut Parser { privilege_seen: false, privilege_casts: false, tokens: lex(sql).unwrap(), at: 0, params: BTreeSet::new(), relations: vec![] }).unwrap();
            assert_eq!(q.row_lock_command(), Some("SELECT FOR UPDATE"), "{sql}");
            let expected = if sql.contains("FOR SHARE") { vec!["a", "b"] } else { vec!["c"] };
            assert_eq!(q.row_lock_tables(), expected, "{sql}");
        }
    }
}
