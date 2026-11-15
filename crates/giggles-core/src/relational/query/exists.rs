//! Demand-driven EXISTS uses the caller's source and statement budgets.
use super::*;

pub(super) type ProbeKey = (usize, Option<BoundColumn>, BoundScalar, DataType);

// Only mandatory equalities with row-independent operands can narrow source zero.
pub(super) fn probe_keys(
    query: &Query, tables: &[&TableDef], parameters: &[Option<DataType>],
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
) -> R<Vec<ProbeKey>> {
    fn equalities<'a>(f: &'a Filter, out: &mut Vec<(&'a Scalar, &'a Scalar)>) {
        match f {
            Filter::Compare(a, op, b) if op == "=" => { out.push((a, b)); out.push((b, a)); }
            Filter::Logic(a, true, b) => { equalities(a, out); equalities(b, out); }
            _ => {}
        }
    }
    let mut pairs = vec![];
    if let Some(filter) = query.where_filter() { equalities(filter, &mut pairs); }
    let mut keys = vec![];
    for (column, value) in pairs {
        let Scalar::Column(column) = column else { continue; };
        let column = resolve(column, query.sources.len())?;
        if column.source != 0 { continue; }
        match value {
            Scalar::Column(c) if resolve(c, tables.len())?.source >= query.sources.len() => {}
            Scalar::Literal(_) | Scalar::Param(_) => {}
            _ => continue,
        }
        let outer = if let Scalar::Column(c) = value { Some(resolve(c, tables.len())?) } else { None };
        let ty = &tables[0].columns[column.column].data_type;
        let value = if matches!(value, Scalar::Literal(Value::Text(_) | Value::Null)) {
            Scalar::Cast(Box::new(value.clone()), ty.clone()).bind(resolve, tables, parameters)?
        } else { value.bind(resolve, tables, parameters)? };
        if functions::join_key_supported(&value.ty, ty) {
            keys.push((column.column, outer, value, ty.clone()));
        }
    }
    Ok(keys)
}

// Each operator owns only its charges; child caches remain statement-owned.
pub(super) struct RetentionScope<'a> {
    ledger: &'a RefCell<scan::Retention>,
    bytes: Cell<usize>,
}
impl<'a> RetentionScope<'a> {
    pub(super) fn new(execution: &'a Execution<'_>) -> Self {
        Self { ledger: &execution.retention, bytes: Cell::new(0) }
    }
    pub(super) fn borrow(&self) -> RetentionBorrow<'_> {
        let ledger = self.ledger.borrow_mut();
        let before = ledger.used();
        RetentionBorrow { ledger, before, bytes: &self.bytes }
    }
}
pub(super) struct RetentionBorrow<'a> {
    ledger: std::cell::RefMut<'a, scan::Retention>,
    before: usize,
    bytes: &'a Cell<usize>,
}
impl std::ops::Deref for RetentionBorrow<'_> {
    type Target = scan::Retention;
    fn deref(&self) -> &Self::Target { &self.ledger }
}
impl std::ops::DerefMut for RetentionBorrow<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.ledger }
}
impl Drop for RetentionBorrow<'_> {
    fn drop(&mut self) { self.bytes.set(self.bytes.get() + self.ledger.used() - self.before); }
}
impl Drop for RetentionScope<'_> {
    fn drop(&mut self) { self.ledger.borrow_mut().release(self.bytes.get()); }
}
impl scan::RetentionBudget for RetentionScope<'_> {
    fn charge(&mut self, bytes: usize) -> R<()> { self.borrow().charge(bytes) }
    fn release(&mut self, bytes: usize) { self.borrow().release(bytes); }
}

impl BoundQuery<'_> {
    pub(super) fn outer_values(&self, execution: &Execution<'_>) -> Vec<Vec<Value>> {
        let mut outer = vec![vec![]; self.tables.len().saturating_sub(self.query.sources.len())];
        let frames = execution.frames.borrow();
        for slot in &self.outer_slots {
            outer[slot.slot - self.query.sources.len()] = frames[frames.len() - slot.depth][slot.source].clone();
        }
        outer
    }
    pub(super) fn correlated(&self, depth: usize) -> bool {
        self.outer_slots.iter().any(|s| s.depth > depth)
            || self.subqueries.correlated(depth + 1)
    }
    pub(super) fn exists_input<'s>(&'s self, source: &'s dyn scan::RowSource, params: &[Value],
        outer: &[Vec<Value>], range: &scan::KeyRange) -> R<scan::SourceRows<'s>> {
        if range.is_empty() { return Ok(Box::new(std::iter::empty())); }
        let keys = if source.streams_reductions() {
            let tuple = Tuple { values: vec![], version: 0 };
            self.exists_keys.iter().map(|(column, slot, value, ty)| {
                let operand = slot.map(|c| &outer[c.source - self.query.sources.len()][c.column]);
                let evaluated;
                let operand = if let Some(value) = operand { value }
                    else { evaluated = value.eval(&tuple, params, &mut 0)?; &evaluated };
                Ok((*column, functions::join_key(operand, &value.ty, ty)?))
            }).collect::<R<Vec<_>>>()?
        } else { vec![] };
        if let Some(rows) = source.join_probe(self.tables[0], &keys, true)? { Ok(rows) }
            else { source.scan(self.tables[0], range, scan::Direction::Forward) }
    }
    fn exists(
        &self,
        state: &Relational,
        source: &dyn scan::RowSource,
        params: &[Value],
        execution: &Execution<'_>,
    ) -> R<bool> {
        if execution.cancelled() { return Err(SqlError::Aborted); }
        let number = |e: Option<&Expr>, default, limit| -> R<Option<u64>> {
            let mut value = e.map(|e| self.query.page_value(e, params, limit)).transpose()?;
            if limit && matches!(e, Some(Expr::Param(n)) if execution.declared.get(n-1) == Some(&Some(DataType::Numeric))) {
                value = value.map(|v| assignment::convert(v, &DataType::Numeric, &DataType::BigInt, None)).transpose()?;
            }
            match value {
                Some(Value::Int(n)) if n >= 0 => Ok(Some(n as u64)),
                Some(Value::Int(_)) => Err(if limit { SqlError::InvalidRowCountInLimitClause } else { SqlError::InvalidRowCountInResultOffsetClause }),
                None | Some(Value::Null) => Ok(default),
                _ => Err(SqlError::Limit),
            }
        };
        let offset = number(self.query.offset(), Some(0), false)?.unwrap_or(0);
        if number(self.query.limit.as_ref(), None, true)? == Some(0) { return Ok(false); }
        if let Some(grouping) = &self.grouping {
            return Ok(!grouping.execute(self, state, source, params, execution, true)?.rows.is_empty());
        }
        let scope = RetentionScope::new(execution);
        let outer = self.outer_values(execution);
        let mut cancel = || execution.cancelled();
        let mut meter = scan::Meter::per_row(&mut cancel);
        let mut skipped = 0;
        let mut seen = BTreeSet::new();
        let mut distinct_bytes = 0;
        let result = (|| {
        let mut values = self.items.iter().filter(|_| self.aggregate).map(|item| {
            if matches!(item, BoundItem::Aggregate(a, _) if matches!(a.base(), Aggregate::Count)) {
                Value::Int(0)
            } else { Value::Null }
        }).collect::<Vec<_>>();
        let mut auxiliary = if self.aggregate {
            scope.borrow().charge(aggregate_retained(&values))?;
            aggregate_auxiliary(&self.items, &mut scope.borrow())?
        } else { vec![] };
        let mut work = 0;
        let mut accept = |row: &Tuple| -> R<bool> {
            let qualifies = self.filter.as_ref().map(|f| f.eval_with_subqueries(
                row, params, &mut 0, self, state, source, execution)).transpose()?;
            if !matches!(qualifies, None | Some(Some(true))) { return Ok(false); }
            if self.aggregate {
                let mut retention = scope.borrow();
                let before = aggregate_retained(&values);
                update_aggregates(&self.items, &self.columns, &mut values, &mut auxiliary,
                    &mut retention, row, params, &mut work)?;
                let after = aggregate_retained(&values);
                if after > before { retention.charge(after - before)?; }
                else { retention.release(before - after); }
                return Ok(false);
            }
            if self.query.distinct() && offset > 0 {
                let key = grouping::Key(self.project_tuple(row, params, &mut 0, &mut 0)?.values);
                if seen.contains(&key) { return Ok(false); }
                distinct_bytes += distinct_charge(&key.0, &[], &mut execution.retention.borrow_mut())?;
                seen.insert(key);
            }
            if skipped < offset { skipped += 1; return Ok(false); }
            Ok(true)
        };
        if self.query.sources.is_empty() {
            meter.tick()?;
            let accepted = accept(&Tuple { values: outer, version: 0 })?;
            if !self.aggregate { return Ok(accepted); }
        } else {
            let (empty, bounds) = self.base_bounds(params)?;
            if !empty {
                let range = scan::KeyRange::from_bounds(bounds);
                let input = self.exists_input(source, params, &outer, &range)?;
                for row in join_rows(self, source, input, params, &mut meter, &execution.visits, &outer, true, state, execution) {
                    if accept(&row?)? && !self.aggregate { return Ok(true); }
                }
            }
        }
        if self.aggregate { finish_aggregates(&self.items, &mut values, &auxiliary, &mut work)?; }
        Ok(self.aggregate && offset == 0)
        })();
        execution.retention.borrow_mut().release(distinct_bytes);
        result
    }
}

impl Execution<'_> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn exists_filter(
        &self,
        child: &BoundQuery<'_>,
        key: usize,
        row: &Tuple,
        state: &Relational,
        source: &dyn scan::RowSource,
        params: &[Value],
    ) -> R<bool> {
        let correlated = child.correlated(0);
        if !correlated && let Some(value) = self.exists.borrow().get(&key) { return Ok(*value); }
        let bytes = (std::mem::size_of_val(row.values.as_slice()) + row.values.iter().map(|v| std::mem::size_of_val(v.as_slice())
            + v.iter().map(value_bytes).sum::<usize>()).sum::<usize>())
            .checked_mul(2).ok_or(SqlError::Limit)? + std::mem::size_of::<Vec<Vec<Value>>>();
        self.retention.borrow_mut().charge(bytes)?;
        self.frames.borrow_mut().push(row.values.clone());
        let result = child.exists(state, source, params, self);
        self.frames.borrow_mut().pop();
        self.retention.borrow_mut().release(bytes);
        let value = result?;
        if self.cancelled() { return Err(SqlError::Aborted); }
        if !correlated {
            self.retention.borrow_mut().charge(64)?;
            self.exists.borrow_mut().insert(key, value);
        }
        Ok(value)
    }
}

#[cfg(test)]
impl Prepared {
    pub(crate) fn exists_test_query(&mut self, sql: &str) {
        self.unbound_statement = None;
        self.statement = Statement::Query(Query::parse(&mut Parser {
            tokens: lex(sql).unwrap(), at: 0, params: BTreeSet::new(), relations: vec![],
        }).unwrap());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn state() -> Relational {
        let mut state = Relational::default();
        for sql in ["CREATE TABLE o(id int)", "CREATE TABLE i(k int, v int)",
            "INSERT INTO o VALUES(1),(2),(3),(NULL)",
            "INSERT INTO i VALUES(1,NULL),(2,20),(2,21),(NULL,99)"] {
            let p = Prepared::parse(sql, 1, state.epoch).unwrap();
            state.execute_with_cancel(&p, &[], None, || false).unwrap();
        }
        state
    }
    fn query(sql: &str) -> Query {
        let mut p = Prepared::parse("SELECT 1", 1, 0).unwrap();
        p.exists_test_query(sql);
        let Statement::Query(q) = p.statement else { unreachable!() }; q
    }
    #[test]
    fn oracle_filters_projection_elision_and_nested_scopes() {
        let state = state();
        for (term, count) in [
            ("EXISTS(SELECT NULL FROM i WHERE k=o.id)", 2),
            ("NOT EXISTS(SELECT NULL FROM i WHERE k=o.id)", 2),
            ("EXISTS(SELECT 1/0 FROM i WHERE k=o.id)", 2),
            ("EXISTS(SELECT 1 FROM i LIMIT 0)", 0),
            ("EXISTS(SELECT count(*) FROM i WHERE false)", 4),
            ("EXISTS(SELECT count(*) FROM i WHERE false HAVING count(*)=0)", 4),
            ("EXISTS(SELECT 1 FROM i x WHERE EXISTS(SELECT NULL WHERE x.k=o.id))", 2),
            ("EXISTS(SELECT 1 FROM i o WHERE o.k=1)", 4),
            ("EXISTS(SELECT DISTINCT k FROM i WHERE k=2 OFFSET 1)", 0),
            ("EXISTS(SELECT DISTINCT count(*) FROM i OFFSET 1)", 0),
            ("EXISTS(SELECT count(*) FROM i WHERE k=o.id GROUP BY k HAVING count(*)>0)", 2),
        ] {
            let q = query(&format!("SELECT id FROM o WHERE {term}"));
            assert_eq!(q.bind(&state, 0, &[]).unwrap().execute(&state,
                &scan::LegacyRows(&state), &[], &mut || false).unwrap().rows.len(), count, "{term}");
        }
        assert!(Prepared::parse("SELECT 1 WHERE EXISTS(SELECT NULL)", 1, state.epoch).is_ok());
        for (negation, expected) in [("", vec![Value::Int(1), Value::Int(2)]),
            ("NOT ", vec![Value::Int(3), Value::Null])] {
            let q = query(&format!("SELECT id FROM o WHERE {negation}EXISTS(SELECT v FROM i WHERE k=o.id) ORDER BY id"));
            let rows = q.bind(&state, 0, &[]).unwrap().execute(&state, &scan::LegacyRows(&state), &[], &mut || false).unwrap().rows;
            assert_eq!(rows.into_iter().map(|r| r.values[0].clone()).collect::<Vec<_>>(), expected);
        }
        let q = query("SELECT id FROM o WHERE EXISTS(SELECT 1 FROM i WHERE k=o.id)");
        assert_eq!(q.bind(&state, 0, &[]).unwrap().matching_rows(&state, &scan::LegacyRows(&state), &[],
            &mut scan::Retention::new(scan::retention_limit()), &mut || false).unwrap().len(), 2);
    }
    #[test]
    fn cache_visits_cancellation_and_retention_are_statement_wide() {
        let state = state();
        let source = scan::LegacyRows(&state);
        let q = query("SELECT id FROM o WHERE EXISTS(SELECT 1/0 FROM i)");
        let bound = q.bind(&state, 0, &[]).unwrap();
        let mut cancel = || false;
        let execution = Execution::new(&mut cancel, &[]);
        assert_eq!(bound.execute_context(&state, &source, &[], &[], &execution, false, false).unwrap().rows.len(), 4);
        assert_eq!(execution.visits.get(), 1);
        assert_eq!(bound.execute(&state, &source, &[], &mut || true), Err(SqlError::Aborted));
        for budget in [true, false] {
            let mut cancel = || false;
            let execution = Execution::new(&mut cancel, &[]);
            if budget { execution.visits.set(MAX_JOIN_VISITS); }
            else { *execution.retention.borrow_mut() = scan::Retention::new(1); }
            assert_eq!(bound.execute_context(&state, &source, &[], &[], &execution, false, false), Err(SqlError::Limit));
            assert!(execution.frames.borrow().is_empty());
        }
    }
    #[test]
    fn aggregate_transitions_having_and_scoped_error_release() {
        let state = state();
        for (child, expected) in [
            ("SELECT max(v) FROM i WHERE false", Ok(4)),
            ("SELECT max(v) FROM i HAVING false", Ok(0)),
            ("SELECT sum(1/(k-k)) FROM i", Err(SqlError::DivisionByZero)),
            ("SELECT sum(1/(k-k)) FROM i OFFSET 1", Err(SqlError::DivisionByZero)),
            ("SELECT sum(1/(k-k)) FROM i GROUP BY k", Err(SqlError::DivisionByZero)),
            ("SELECT sum(1/(k-k)) FROM i LIMIT 0", Ok(0)),
        ] {
            let q = query(&format!("SELECT id FROM o WHERE EXISTS({child})"));
            let bound = q.bind(&state, 0, &[]).unwrap();
            let mut cancel = || false;
            let execution = Execution::new(&mut cancel, &[]);
            let actual = bound.execute_context(&state, &scan::LegacyRows(&state), &[], &[], &execution, false, false);
            assert_eq!(actual.map(|r| r.rows.len()), expected, "{child}");
            assert!(execution.frames.borrow().is_empty());
            assert_eq!(execution.retention.borrow().used(), if expected.is_ok() { 64 } else { 0 });
        }
    }
    #[test]
    fn parent_sort_and_aggregate_share_child_budget() {
        let mut state = state();
        for sql in ["CREATE TABLE t(id int PRIMARY KEY, v text)".into(),
            format!("INSERT INTO t VALUES(1,'{}'),(2,'{}')", "x".repeat(700), "y".repeat(700))] {
            let p = Prepared::parse(&sql, 1, state.epoch).unwrap();
            state.execute_with_cancel(&p, &[], None, || false).unwrap();
        }
        for (sql, fails) in [
            ("SELECT id FROM t ORDER BY id+0", false),
            ("SELECT max(v) FROM t", false),
            ("SELECT id FROM t WHERE EXISTS(SELECT 1 FROM t i WHERE i.id=t.id)", false),
            ("SELECT id FROM t WHERE EXISTS(SELECT 1 FROM t i WHERE i.id=t.id) ORDER BY id+0", true),
            ("SELECT max(v) FROM t WHERE EXISTS(SELECT 1 FROM t i WHERE i.id=t.id)", true),
        ] {
            let q = query(sql);
            let mut cancel = || false;
            let execution = Execution::new(&mut cancel, &[]);
            *execution.retention.borrow_mut() = scan::Retention::new(2048);
            let result = q.bind(&state, 0, &[]).unwrap().execute_context(&state, &scan::LegacyRows(&state), &[], &[], &execution, false, false);
            if fails { assert_eq!(result, Err(SqlError::Limit), "{sql}"); }
            else { assert!(result.is_ok(), "{sql}: {result:?}"); }
            assert_eq!(execution.retention.borrow().used(), 0, "{sql}");
        }
    }

}
