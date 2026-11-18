use super::*;
use bind::{input_state, order_column, order_scalar};
use std::borrow::Cow;

/// Each materialized relation retains its allocation charges until its consumer finishes.
pub(super) struct Relation<'a> {
    pub columns: Vec<ColumnDef>,
    pub typmods: Vec<i32>,
    pub rows: Vec<SqlRow>,
    pub affected: u64,
    pub(super) scope: RetentionScope<'a>,
}
impl<'a> Relation<'a> {
    pub fn new(columns: Vec<ColumnDef>, typmods: Vec<i32>, execution: &'a Execution<'_>) -> Self {
        Self {
            columns,
            typmods,
            rows: vec![],
            affected: 0,
            scope: RetentionScope::new(execution),
        }
    }
    pub fn push(&mut self, row: SqlRow) -> R<()> {
        if self.rows.len() >= scan::query_row_limit() {
            return Err(SqlError::Limit);
        }
        self.scope.borrow().charge(
            std::mem::size_of::<SqlRow>()
                + row.values.len() * std::mem::size_of::<Value>()
                + row.values.iter().map(value_bytes).sum::<usize>(),
        )?;
        self.rows.push(row);
        Ok(())
    }
}
impl scan::RowSource for Relation<'_> {
    fn scan<'s>(
        &'s self,
        _: &TableDef,
        _: &scan::KeyRange,
        _: scan::Direction,
    ) -> R<scan::SourceRows<'s>> {
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
impl Plan {
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
                result =
                    subquery::run(select, state, source, params, parameters, clock, execution)?;
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
                    for row in group_sets::execute(&query, self.decorations.grouping_sets.as_deref(), &catalog, &wrapper, params, parameters, clock, execution)?.rows
                    {
                        result.push(row)?;
                    }
                } else {
                    let mut input = select
                        .input
                        .as_ref()
                        .map(|i| {
                            i.query
                                .run(state, source, params, parameters, clock, execution)
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
                    let rows = group_sets::execute(&query, self.decorations.grouping_sets.as_deref(), state, source, params, parameters, clock, execution)?;
                    for row in rows.rows {
                        result.push(row)?;
                    }
                }
            }
            Body::Values(rows) => {
                let empty = Tuple {
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
            Body::Set {
                op,
                all,
                left,
                right,
            } => {
                let mut left = left.run(state, source, params, parameters, clock, execution)?;
                let mut right = right.run(state, source, params, parameters, clock, execution)?;
                for input in [&mut left, &mut right] {
                    for row in &mut input.rows {
                        if execution.cancelled() {
                            return Err(SqlError::Aborted);
                        }
                        for (i, v) in row.values.iter_mut().enumerate() {
                            if input.columns[i].data_type != result.columns[i].data_type {
                                *v = functions::coerce_clocked(
                                    v.clone(),
                                    &result.columns[i].data_type,
                                    clock,
                                )?;
                            }
                        }
                    }
                }
                setops::apply(*op, *all, left, right, &mut result, execution)?;
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
