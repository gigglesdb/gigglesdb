use super::*;
use bind::{input_state, order_column, order_scalar};
use std::{borrow::Cow, collections::HashSet, hash::{Hash, Hasher}};

/// Each materialized relation retains its allocation charges until its consumer finishes.
pub(super) struct Relation<'a> {
    pub columns: Vec<ColumnDef>,
    pub typmods: Vec<i32>,
    pub rows: Vec<SqlRow>,
    pub affected: u64,
    pub(super) spilled: Option<super::super::super::spill::SpilledRows>,
    pub(super) scope: RetentionScope<'a>,
}
impl<'a> Relation<'a> {
    pub fn new(columns: Vec<ColumnDef>, typmods: Vec<i32>, execution: &'a Execution<'_>) -> Self {
        Self {
            columns,
            typmods,
            rows: vec![],
            affected: 0,
            spilled: None,
            scope: RetentionScope::new(execution),
        }
    }
    pub fn push(&mut self, row: SqlRow) -> R<()> {
        if self.rows.len() >= scan::query_row_limit() {
            return Err(SqlError::Limit);
        }
        self.scope.borrow().charge(row_retention_bytes(&row))?;
        self.rows.push(row);
        Ok(())
    }
}
fn row_retention_bytes(row: &SqlRow) -> usize {
    std::mem::size_of::<SqlRow>()
        + row.values.len() * std::mem::size_of::<Value>()
        + row.values.iter().map(value_bytes).sum::<usize>()
}

#[derive(PartialEq, Eq)]
struct IntegerSetRow(Vec<Value>);

impl Hash for IntegerSetRow {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.len().hash(state);
        for value in &self.0 {
            match value {
                Value::Null => None::<i64>.hash(state),
                Value::Int(value) => Some(*value).hash(state),
                _ => unreachable!("integer UNION has only integer columns"),
            }
        }
    }
}

fn same_types(left: &[ColumnDef], right: &[ColumnDef]) -> bool {
    left.iter().zip(right).all(|(a, b)| a.data_type == b.data_type)
}

fn coerce_set_rows(
    input: &mut Relation<'_>,
    columns: &[ColumnDef],
    clock: Option<&super::super::super::clock::ExecutionClock>,
    execution: &Execution<'_>,
) -> R<()> {
    if same_types(&input.columns, columns) {
        return Ok(());
    }
    for row in &mut input.rows {
        if execution.cancelled() {
            return Err(SqlError::Aborted);
        }
        let before = row.values.iter().map(value_bytes).sum::<usize>();
        for ((value, source), target) in row.values.iter_mut().zip(&input.columns).zip(columns) {
            let source = &source.data_type;
            let target = &target.data_type;
            if source != target {
                *value = if target == &DataType::Oid
                    && let Some(result) = text_search_types::cast_typed(value, source, target)
                {
                    result?
                } else {
                    functions::coerce_clocked(value.clone(), target, clock)?
                };
            }
        }
        let after = row.values.iter().map(value_bytes).sum::<usize>();
        if after > before {
            input.scope.borrow().charge(after - before)?;
        } else {
            input.scope.borrow().release(before - after);
        }
    }
    Ok(())
}

impl scan::RowSource for Relation<'_> {
    fn scan<'s>(
        &'s self,
        _: &TableDef,
        _: &scan::KeyRange,
        _: scan::Direction,
    ) -> R<scan::SourceRows<'s>> {
        if let Some(spill) = &self.spilled {
            return Ok(Box::new(spill.scan()?.map(|row| {
                row.map(|row| scan::SourceRow { key: Cow::Borrowed(&[]), row })
            })));
        }
        Ok(Box::new(self.rows.iter().map(|row| {
            Ok(scan::SourceRow {
                key: Cow::Borrowed(&[]),
                row: Cow::Borrowed(row),
            })
        })))
    }
    fn get<'s>(&'s self, _: &TableDef, _: &[u8]) -> R<Option<scan::SourceRow<'s>>> {
        Err(SqlError::Unsupported)
    }
}
struct SeriesRows<'s, 'e> {
    rows: srf::Series,
    execution: &'s Execution<'e>,
}

impl scan::RowSource for SeriesRows<'_, '_> {
    fn scan<'s>(
        &'s self,
        _: &TableDef,
        range: &scan::KeyRange,
        _: scan::Direction,
    ) -> R<scan::SourceRows<'s>> {
        if range.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }
        Ok(Box::new(self.rows.clone().map(|row| {
            self.execution.comparison()?;
            Ok(scan::SourceRow { key: Cow::Borrowed(&[]), row: Cow::Owned(row) })
        })))
    }

    fn get<'s>(&'s self, _: &TableDef, _: &[u8]) -> R<Option<scan::SourceRow<'s>>> {
        Err(SqlError::Unsupported)
    }
}

impl Plan {
    fn write_series(&self) -> Option<(&Select, &srf::Function)> {
        let Body::Select(select) = &self.body else { return None };
        let join::From::Function { function, .. } = select.from.as_ref()? else { return None };
        (self.ctes.is_empty() && self.order.is_empty() && self.limit.is_none()
            && self.offset.is_none() && self.decorations.grouping_sets.is_none()
            && self.decorations.sample.is_none() && self.decorations.distinct_on == 0
            && select.applies.is_empty() && select.query.pageable_insert() && function.simple_series())
            .then_some((select, function))
    }

    pub(in crate::relational) fn streams_write_source(&self) -> bool {
        self.write_series().is_some()
    }

    pub(in crate::relational) fn stream_write_source<'a>(
        &self,
        state: &Relational,
        params: &[Value],
        declared: &[Option<DataType>],
        clock: Option<&super::super::super::clock::ExecutionClock>,
        cancel: &'a mut dyn FnMut() -> bool,
        visit: &'a mut dyn FnMut(SqlRow) -> R<()>,
    ) -> R<()> {
        let bound = self.bind(state, params.len(), declared)?;
        let (select, function) = bound.plan.write_series().ok_or(SqlError::Unsupported)?;
        let mut parameters: Vec<_> = bound.description.parameters.into_iter().map(Some).collect();
        let shape = select.from.clone().expect("series input").resolve(state, &mut parameters)?;
        let catalog = join::catalog(state, &shape)?;
        let query = join::prepare(&select.query, &shape)?;
        let bound = query.bind_projection_seeded(&catalog, parameters.clone())?.with_clock(clock);
        let rows = function.series(params, &parameters, clock)?;
        let mut execution = Execution::new(cancel, declared);
        execution.write_rows = Some(RefCell::new(visit));
        let source = SeriesRows { rows, execution: &execution };
        bound.execute_context(&catalog, &source, params, declared, &execution, false, true)?;
        Ok(())
    }

    #[expect(clippy::too_many_arguments, reason = "shared query execution context")]
    pub(in crate::relational) fn execute(
        &self,
        state: &Relational,
        source: &dyn scan::RowSource,
        count: usize,
        params: &[Value],
        declared: &[Option<DataType>],
        clock: Option<&super::super::super::clock::ExecutionClock>,
        cancel: &mut dyn FnMut() -> bool,
    ) -> R<SqlResult> {
        let bound = self.bind(state, count, declared)?;
        for (index, (storage, value)) in bound.description.parameters.iter().zip(params).enumerate() {
            enum_parameters::validate(index, storage, value)?;
        }
        let parameters: Vec<_> = bound
            .description
            .parameters
            .iter()
            .cloned()
            .map(Some)
            .collect();
        let execution = Execution::new(cancel, declared);
        let relation = bound
            .plan
            .run(state, source, params, &parameters, clock, &execution)?;
        let mut bytes = 0;
        for row in &relation.rows {
            for value in &row.values {
                charge(&mut bytes, value)?;
            }
        }
        Ok(SqlResult {
            columns: relation.columns.iter().map(|c| c.name.clone()).collect(),
            rows: relation.rows,
            affected: 0,
        })
    }
    pub(super) fn run<'a>(
        &self,
        state: &Relational,
        source: &dyn scan::RowSource,
        params: &[Value],
        parameters: &[Option<DataType>],
        clock: Option<&super::super::super::clock::ExecutionClock>,
        execution: &'a Execution<'_>,
    ) -> R<Relation<'a>> {
        let _metadata = functions::collation::ColumnScope::enter(&[], &[])?;
        if execution.cancelled() {
            return Err(SqlError::Aborted);
        }
        if !self.ctes.is_empty() {
            return cte::run(self, state, source, params, parameters, clock, execution);
        }
        let meta = self.clone().resolve(state, &mut parameters.to_vec())?;
        let mut result = Relation::new(meta.columns, meta.typmods, execution);
        match &self.body {
            Body::Select(select) if !select.applies.is_empty() => {
                result = subquery::run(
                    select,
                    self.decorations.grouping_sets.as_ref(),
                    state,
                    source,
                    params,
                    parameters,
                    clock,
                    execution,
                )?;
            }
            Body::Select(select) => {
                if let Some(from) = &select.from {
                    let shape = from.clone().resolve(state, &mut parameters.to_vec())?;
                    let rows = from.run(state, source, params, parameters, clock, execution)?;
                    let catalog = join::catalog(state, &shape)?;
                    let wrapper = cte::VirtualRows {
                        base: source,
                        relations: vec![(join::INPUT.into(), &rows)],
                    };
                    let query = join::prepare(&select.query, &shape)?;
                    let bound = query
                        .bind_projection_seeded(&catalog, parameters.to_vec())?
                        .with_clock(clock);
                    result.columns = bound.columns.clone();
                    for row in group_sets::execute(&query, self.decorations.grouping_sets.as_ref(), &catalog, &wrapper, params, parameters, clock, execution)?.rows
                    {
                        result.push(row)?;
                    }
                } else {
                    let mut input = select
                        .input
                        .as_ref()
                        .map(|i| {
                            let _limits = execution.subquery_limits();
                            i.query.run(state, source, params, parameters, clock, execution)
                        })
                        .transpose()?;
                    let mut input_meta = select
                        .input
                        .as_ref()
                        .map(|i| i.query.clone().resolve(state, &mut parameters.to_vec()))
                        .transpose()?;
                    if let Some(rows) = &mut input {
                        let meta = input_meta.as_mut().unwrap();
                        window::run(
                            &select.windows,
                            rows,
                            &meta.collations,
                            params,
                            parameters,
                            clock,
                            execution,
                        )?;
                        window::columns(&select.windows, meta, &mut parameters.to_vec())?;
                    }
                    let synthetic = input
                        .as_ref()
                        .map(|rows| {
                            input_state(
                                state,
                                select.input.as_ref().unwrap(),
                                &rows.columns,
                                &rows.typmods,
                            )
                        })
                        .transpose()?;
                    let state = synthetic.as_ref().unwrap_or(state);
                    let _collations = input_meta
                        .as_ref()
                        .map(|meta| {
                            functions::collation::ColumnScope::enter(
                                &state.tables[&select.input.as_ref().unwrap().name].columns,
                                &meta.collations,
                            )
                        })
                        .transpose()?;
                    let wrapper = input.as_ref().map(|rows| cte::VirtualRows {
                        base: source,
                        relations: vec![(select.input.as_ref().unwrap().name.clone(), rows)],
                    });
                    let source: &dyn scan::RowSource = wrapper.as_ref().map_or(source, |rows| rows);
                    let projection = window::prepare(select, state)?;
                    let query = cte::prepare_query(&projection, state)?;
                    let bound = query
                        .bind_projection_seeded(state, parameters.to_vec())?
                        .with_clock(clock);
                    result.columns = bound.columns.clone();
                    let rows = group_sets::execute(&query, self.decorations.grouping_sets.as_ref(), state, source, params, parameters, clock, execution)?;
                    for row in rows.rows {
                        result.push(row)?;
                    }
                }
            }
            Body::Values(rows) => {
                let empty = Tuple {
                    source_versions: vec![],
                    values: vec![],
                    version: 0,
                };
                let mut bytes = 0;
                for row in rows {
                    if execution.cancelled() {
                        return Err(SqlError::Aborted);
                    }
                    let mut values = Vec::with_capacity(row.len());
                    for scalar in row {
                        let mut scalar =
                            scalar.bind(&|_, _| Err(SqlError::UnknownColumn), &[], parameters)?;
                        scalar.attach_clock(clock);
                        values.push(scalar.eval(&empty, params, &mut bytes)?);
                    }
                    result.push(SqlRow { values, version: 0 })?;
                }
            }
            Body::Set { first, rest } => {
                result = Self::run_set(first, rest, state, source, params, parameters, clock, execution)?;
            }
        }
        if let Some(sample) = &self.decorations.sample { sample.apply(&mut result, params, parameters, clock, execution)?; }
        let mut tie_keys = vec![];
        let mut order = vec![];
        if !self.order.is_empty() {
            let table = bind::result_table(&result.columns);
            let _collations =
                functions::collation::ColumnScope::enter(&table.columns, &meta.collations)?;
            order = self
                .order
                .iter()
                .map(|(s, desc, first)| {
                    let scalar = order_scalar(s, &table)?;
                    let mut bound =
                        scalar.bind(&|c, _| order_column(c, &table), &[&table], parameters)?;
                    bound.attach_clock(clock);
                    bound.require_collation()?;
                    Ok((bound, *desc, *first))
                })
                .collect::<R<Vec<_>>>()?;
            let mut keyed = Vec::with_capacity(result.rows.len());
            let mut bytes = 0;
            for row in std::mem::take(&mut result.rows) {
                if execution.cancelled() {
                    return Err(SqlError::Aborted);
                }
                let tuple = Tuple {
                    source_versions: vec![],
                    values: vec![row.values.clone()],
                    version: row.version,
                };
                let keys = order
                    .iter()
                    .map(|(s, _, _)| s.eval(&tuple, params, &mut bytes))
                    .collect::<R<Vec<_>>>()?;
                result.scope.borrow().charge(
                    std::mem::size_of::<(SqlRow, Vec<Value>)>()
                        + keys.len() * std::mem::size_of::<Value>()
                        + keys.iter().map(value_bytes).sum::<usize>(),
                )?;
                keyed.push((row, keys));
            }
            setops::sort(&mut keyed, execution, |a, b| {
                order_values(&a.1, &b.1, &order)
            })?;
            for (row, keys) in keyed { result.rows.push(row); if self.decorations.with_ties { tie_keys.push(keys); } }
        }
        paging::distinct(&mut result, self.decorations.distinct_on, &meta.collations, self.decorations.hidden, &mut tie_keys, execution)?;
        let limit = page(self.limit.as_ref(), true, params, parameters, clock)?;
        let offset = page(self.offset.as_ref(), false, params, parameters, clock)?.unwrap_or(0);
        if offset > 0 {
            if !tie_keys.is_empty() { tie_keys.drain(..offset.min(tie_keys.len() as u64) as usize); }
            result
                .rows
                .drain(..offset.min(result.rows.len() as u64) as usize);
        }
        if let Some(limit) = limit {
            let mut end = usize::try_from(limit).unwrap_or(usize::MAX).min(result.rows.len());
            if self.decorations.with_ties && end > 0 {
                // Peers use the same expression collations as the ORDER BY sort.
                while end < tie_keys.len()
                    && order_values(&tie_keys[end], &tie_keys[end - 1], &order) == Ordering::Equal
                {
                    end += 1;
                }
            }
            result.rows.truncate(end);
        }
        if self.decorations.hidden > 0 {
            let width = result.columns.len() - self.decorations.hidden;
            result.columns.truncate(width); result.typmods.truncate(width);
            for row in &mut result.rows { row.values.truncate(width); }
        }
        Ok(result)
    }

    #[expect(clippy::too_many_arguments, reason = "shared query execution context")]
    fn run_set<'a>(
        first: &Plan,
        rest: &[SetArm],
        state: &Relational,
        source: &dyn scan::RowSource,
        params: &[Value],
        parameters: &[Option<DataType>],
        clock: Option<&super::super::super::clock::ExecutionClock>,
        execution: &'a Execution<'_>,
    ) -> R<Relation<'a>> {
        let mut stages = Vec::with_capacity(rest.len());
        bind::resolve_set(
            &mut first.clone(),
            &mut rest.to_vec(),
            state,
            &mut parameters.to_vec(),
            |meta| {
                let collations = meta.collations.iter().map(|value| {
                    value.as_ref().map(|value| {
                        functions::collation::combined(Some(value), None, value.ty.clone())
                    }).transpose()
                }).collect::<R<Vec<_>>>()?;
                stages.push((meta.columns.clone(), meta.typmods.clone(), collations));
                Ok(())
            },
        )?;
        // Integer equality has a single representation, so a homogeneous UNION
        // can discard duplicates as each arm runs. Other comparisons keep their stage-local
        // types, collations and representative-selection rules.
        let integer_union = rest.len() > 1
            && rest.iter().all(|arm| arm.op == SetOp::Union && !arm.all)
            && stages[0].0.iter().all(|c| integer_type(&c.data_type))
            && stages.windows(2).all(|pair| same_types(&pair[0].0, &pair[1].0));
        if integer_union {
            let (columns, typmods, _) = stages.pop().expect("integer UNION has a set arm");
            let mut out = Relation::new(columns, typmods, execution);
            let mut distinct = HashSet::new();
            // Account for the hash buckets as well as the final row vector.
            const BUCKET_BYTES: usize = 2 * std::mem::size_of::<IntegerSetRow>();
            let row_limit = scan::query_row_limit();
            let mut first_input = Some(first.run(state, source, params, parameters, clock, execution)?);
            for arm in rest {
                let mut right = arm.plan.run(state, source, params, parameters, clock, execution)?;
                if let Some(left) = &mut first_input {
                    coerce_set_rows(left, &out.columns, clock, execution)?;
                }
                coerce_set_rows(&mut right, &out.columns, clock, execution)?;
                let first_rows = first_input.iter_mut().flat_map(|input| input.rows.drain(..));
                for row in first_rows.chain(right.rows) {
                    execution.comparison()?;
                    let bytes = row_retention_bytes(&row) + BUCKET_BYTES;
                    if distinct.insert(IntegerSetRow(row.values)) {
                        if distinct.len() > row_limit {
                            return Err(SqlError::Limit);
                        }
                        out.scope.borrow().charge(bytes)?;
                    }
                }
                first_input = None;
            }
            let bucket_bytes = distinct.len() * BUCKET_BYTES;
            out.rows = distinct.into_iter().map(|IntegerSetRow(values)| SqlRow { values, version: 0 }).collect();
            out.scope.borrow().release(bucket_bytes);
            setops::sort(&mut out.rows, execution, |a, b| {
                a.values.iter().zip(&b.values).map(|(a, b)| cmp(a, b))
                    .find(|order| *order != Ordering::Equal).unwrap_or(Ordering::Equal)
            })?;
            return Ok(out);
        }
        let mut left = first.run(state, source, params, parameters, clock, execution)?;
        for (arm, (columns, typmods, collations)) in rest.iter().zip(stages) {
            let mut right = arm.plan.run(state, source, params, parameters, clock, execution)?;
            coerce_set_rows(&mut left, &columns, clock, execution)?;
            coerce_set_rows(&mut right, &columns, clock, execution)?;
            if arm.op == SetOp::Union && arm.all {
                for row in right.rows {
                    if execution.cancelled() {
                        return Err(SqlError::Aborted);
                    }
                    left.push(row)?;
                }
                left.columns = columns;
                left.typmods = typmods;
            } else {
                let mut out = Relation::new(columns, typmods, execution);
                setops::apply(arm.op, arm.all, left, right, &mut out, execution, &collations)?;
                left = out;
            }
        }
        Ok(left)
    }

}
fn page(
    s: Option<&Scalar>,
    limit: bool,
    params: &[Value],
    parameters: &[Option<DataType>],
    clock: Option<&super::super::super::clock::ExecutionClock>,
) -> R<Option<u64>> {
    let Some(s) = s else {
        return Ok(None);
    };
    let mut bound = Scalar::Cast(Box::new(s.clone()), DataType::BigInt).bind(
        &|_, _| Err(SqlError::UnknownColumn),
        &[],
        parameters,
    )?;
    bound.attach_clock(clock);
    let value = bound.eval(
        &Tuple {
            source_versions: vec![],
            values: vec![],
            version: 0,
        },
        params,
        &mut 0,
    )?;
    match value {
        Value::Null => Ok(None),
        Value::Int(n) if n >= 0 => Ok(Some(n as u64)),
        Value::Int(_) => Err(if limit {
            SqlError::InvalidRowCountInLimitClause
        } else {
            SqlError::InvalidRowCountInResultOffsetClause
        }),
        _ => Err(SqlError::Type),
    }
}
