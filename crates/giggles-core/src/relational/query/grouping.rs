//! Bounded grouping plan. Synthetic slots contain only group keys and aggregate
//! transition values; input columns cannot leak into post-group evaluation.
use super::*;

pub(super) struct Grouping {
    keys: Vec<BoundScalar>,
    aggregates: Vec<BoundItem>,
    aggregate_columns: Vec<ColumnDef>,
    outputs: Vec<BoundScalar>,
    output_sources: Vec<Option<BoundColumn>>,
    having: Option<BoundFilter>,
    order: Vec<(BoundScalar, bool, bool)>,
}
fn aggregate(name: &str) -> Option<Aggregate> {
    Some(match name {
        "count" => Aggregate::Count,
        "sum" => Aggregate::Sum,
        "min" => Aggregate::Min,
        "max" => Aggregate::Max,
        _ => return None,
    })
}
pub(super) fn contains_aggregate(s: &Scalar) -> bool {
    match s {
        Scalar::Call(n, a) => aggregate(n).is_some() || a.iter().any(contains_aggregate),
        Scalar::Unary(_, a) | Scalar::Cast(a, _) => contains_aggregate(a),
        Scalar::Binary(_, a, b) => contains_aggregate(a) || contains_aggregate(b),
        Scalar::SubstringOrder(a) => a.iter().any(|a| contains_aggregate(a)),
        _ => false,
    }
}
fn slot(n: usize) -> Scalar {
    Scalar::Column(ColumnRef {
        qualifier: None,
        name: format!("__group_slot_{n}"),
    })
}
fn col(name: String, ty: DataType, not_null: bool) -> ColumnDef {
    ColumnDef {
        id: 0,
        name,
        data_type: ty,
        not_null,
    }
}
fn canonical(
    s: &Scalar,
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    scope: usize,
    tables: &[&TableDef],
) -> R<String> {
    Ok(match s {
        Scalar::Column(c) => {
            let c = resolve(c, scope)?;
            format!("column:{}:{}", c.source, c.column)
        }
        Scalar::Unary(op, a) => format!("unary:{op:?}:{}", canonical(a, resolve, scope, tables)?),
        Scalar::Cast(a, ty) => {
            // PostgreSQL grouping identity drops casts that keep the input type.
            // Parameter-only types are inferred later; unresolved bindings simply
            // retain the cast in the canonical form.
            if let Ok(inner) = a.bind(resolve, tables, &[])
                && inner.ty == *ty
            {
                return canonical(a, resolve, scope, tables);
            }
            format!("cast:{ty:?}:{}", canonical(a, resolve, scope, tables)?)
        }
        Scalar::Binary(op, a, b) => format!(
            "binary:{op:?}:{}:{}",
            canonical(a, resolve, scope, tables)?,
            canonical(b, resolve, scope, tables)?
        ),
        Scalar::Call(n, args) => format!(
            "call:{n}:{:?}",
            args.iter()
                .map(|a| canonical(a, resolve, scope, tables))
                .collect::<R<Vec<_>>>()?
        ),
        Scalar::SubstringOrder(args) => format!(
            "substring:{:?}",
            args.iter()
                .map(|a| canonical(a, resolve, scope, tables))
                .collect::<R<Vec<_>>>()?
        ),
        _ => format!("{s:?}"),
    })
}
struct Lower<'a, F> {
    groups: Vec<String>,
    scope: usize,
    resolve: &'a F,
    tables: &'a [&'a TableDef],
    aggregates: Vec<(Aggregate, Option<Scalar>)>,
}
impl<F: Fn(&ColumnRef, usize) -> R<BoundColumn>> Lower<'_, F> {
    fn scalar(&mut self, s: &Scalar) -> R<Scalar> {
        if !contains_aggregate(s) {
            let key = canonical(s, self.resolve, self.scope, self.tables)?;
            if let Some(n) = self.groups.iter().position(|g| g == &key) {
                return Ok(slot(n));
            }
        }
        Ok(match s {
            Scalar::Column(_) => return Err(SqlError::GroupingError),
            Scalar::Call(n, args) if aggregate(n).is_some() => {
                if args.len() != 1 {
                    return Err(SqlError::UndefinedFunction);
                }
                args[0].reject_nested_aggregate()?;
                let index = self.groups.len() + self.aggregates.len();
                self.aggregates.push((
                    aggregate(n).ok_or(SqlError::UndefinedFunction)?,
                    Some(args[0].clone()),
                ));
                slot(index)
            }
            Scalar::Unary(op, a) => Scalar::Unary(*op, Box::new(self.scalar(a)?)),
            Scalar::Cast(a, ty) => Scalar::Cast(Box::new(self.scalar(a)?), ty.clone()),
            Scalar::Binary(op, a, b) => {
                Scalar::Binary(*op, Box::new(self.scalar(a)?), Box::new(self.scalar(b)?))
            }
            Scalar::Call(n, args) => Scalar::Call(
                n.clone(),
                args.iter().map(|a| self.scalar(a)).collect::<R<Vec<_>>>()?,
            ),
            Scalar::SubstringOrder(args) => Scalar::SubstringOrder([
                Box::new(self.scalar(&args[0])?),
                Box::new(self.scalar(&args[1])?),
                Box::new(self.scalar(&args[2])?),
            ]),
            _ => s.clone(),
        })
    }
    fn filter(&mut self, f: &Filter) -> R<Filter> {
        Ok(match f {
            Filter::Clauses(_) => return Err(SqlError::Unsupported),
            Filter::Scalar(s) => Filter::Scalar(self.scalar(s)?),
            Filter::Compare(a, op, b) => {
                Filter::Compare(self.scalar(a)?, op.clone(), self.scalar(b)?)
            }
            Filter::Null(a, n) => Filter::Null(self.scalar(a)?, *n),
            Filter::Like {
                value,
                pattern,
                escape,
                insensitive,
                negated,
            } => Filter::Like {
                value: self.scalar(value)?,
                pattern: self.scalar(pattern)?,
                escape: escape.as_ref().map(|e| self.scalar(e)).transpose()?,
                insensitive: *insensitive,
                negated: *negated,
            },
            Filter::Not(f) => Filter::Not(Box::new(self.filter(f)?)),
            Filter::Logic(a, and, b) => {
                Filter::Logic(Box::new(self.filter(a)?), *and, Box::new(self.filter(b)?))
            }
        })
    }
}
fn output_scalar(item: &Item) -> R<Scalar> {
    match item {
        Item::Scalar(s) => Ok(s.clone()),
        Item::Aggregate(a, arg) => Ok(Scalar::Call(
            match a {
                Aggregate::Count => "count",
                Aggregate::Sum => "sum",
                Aggregate::Min => "min",
                Aggregate::Max => "max",
            }
            .into(),
            vec![arg.clone().unwrap_or(Scalar::Literal(Value::Int(1)))],
        )),
        Item::Star => Err(SqlError::GroupingError),
    }
}
fn resolve_output(
    s: &Scalar,
    items: &[(Item, Option<String>)],
    input_first: bool,
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    scope: usize,
    tables: &[&TableDef],
) -> R<Scalar> {
    if let Scalar::Literal(Value::Int(n)) = s {
        if *n < 1 || *n as usize > items.len() {
            return Err(SqlError::UnknownColumn);
        }
        return output_scalar(&items[*n as usize - 1].0);
    }
    if let Scalar::Column(c) = s
        && c.qualifier.is_none()
        && (!input_first || matches!(resolve(c, scope), Err(SqlError::UnknownColumn)))
    {
        let mut matches = items
            .iter()
            .filter(|(_, alias)| alias.as_ref() == Some(&c.name));
        if let Some((item, _)) = matches.next() {
            let selected = output_scalar(item)?;
            for (other, _) in matches {
                if canonical(&selected, resolve, scope, tables)?
                    != canonical(&output_scalar(other)?, resolve, scope, tables)?
                {
                    return Err(SqlError::Duplicate);
                }
            }
            return Ok(selected);
        }
    }
    Ok(s.clone())
}
pub(super) fn bind<'a>(
    q: &'a Query,
    tables: &[&'a TableDef],
    joins: Vec<BoundJoin>,
    mut parameters: Vec<Option<DataType>>,
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
) -> R<BoundQuery<'a>> {
    let mut projection = vec![];
    for (item, alias) in &q.items {
        if matches!(item, Item::Star) {
            for (source, table) in q.sources.iter().zip(tables) {
                for (n, column) in table.columns.iter().enumerate() {
                    if table.hidden_rowid() != Some(n) {
                        projection.push((
                            Item::Scalar(Scalar::Column(ColumnRef {
                                qualifier: Some(source.alias.clone()),
                                name: column.name.clone(),
                            })),
                            None,
                        ));
                    }
                }
            }
        } else {
            projection.push((item.clone(), alias.clone()));
        }
    }
    if projection.len() > MAX_COLUMNS {
        return Err(SqlError::Limit);
    }
    // Projection casts can constrain a parameter used as a bare group key.
    for (item, _) in &projection {
        if let Item::Scalar(s) = item
            && !contains_aggregate(s)
        {
            s.infer(None, &mut parameters, resolve, tables)?;
        }
    }
    let groups = q
        .group()
        .iter()
        .map(|s| resolve_output(s, &projection, true, resolve, tables.len(), tables))
        .collect::<R<Vec<_>>>()?;
    for s in &groups {
        s.reject_nested_aggregate()?;
        s.infer(None, &mut parameters, resolve, tables)?;
    }
    if let Some(f) = q.where_filter() {
        f.infer(&mut parameters, resolve, tables)?;
    }
    for expr in q.limit.iter().chain(q.offset()) {
        if let Expr::Param(n) = expr {
            let p = parameters.get_mut(n - 1).ok_or(SqlError::Parameters)?;
            if p.is_none() {
                *p = Some(DataType::BigInt);
            }
        }
    }
    let mut lower = Lower {
        groups: groups
            .iter()
            .map(|s| canonical(s, resolve, tables.len(), tables))
            .collect::<R<Vec<_>>>()?,
        scope: tables.len(),
        resolve,
        tables,
        aggregates: vec![],
    };
    let output_syntax = projection
        .iter()
        .map(|(i, _)| {
            if let Item::Aggregate(Aggregate::Count, None) = i {
                let index = lower.groups.len() + lower.aggregates.len();
                lower.aggregates.push((Aggregate::Count, None));
                Ok(slot(index))
            } else {
                lower.scalar(&output_scalar(i)?)
            }
        })
        .collect::<R<Vec<_>>>()?;
    let having_syntax = q.having().map(|f| lower.filter(f)).transpose()?;
    let order_syntax = q
        .order
        .iter()
        .map(|(s, d, n)| {
            Ok((
                lower.scalar(&resolve_output(
                    s,
                    &projection,
                    false,
                    resolve,
                    tables.len(),
                    tables,
                )?)?,
                *d,
                *n,
            ))
        })
        .collect::<R<Vec<_>>>()?;
    let mut unresolved_count = vec![];
    for (a, arg) in &lower.aggregates {
        let Some(arg) = arg else {
            continue;
        };
        if matches!(a, Aggregate::Count)
            && let Scalar::Param(n) = arg
            && parameters.get(n - 1).is_none_or(Option::is_none)
        {
            unresolved_count.push(*n);
        }
        if matches!(a, Aggregate::Sum) && arg.aggregate_unknown(&parameters) {
            return Err(SqlError::AmbiguousFunction);
        }
        let expected =
            if matches!(a, Aggregate::Min | Aggregate::Max) && arg.aggregate_unknown(&parameters) {
                Some(&DataType::Text)
            } else {
                None
            };
        arg.infer(expected, &mut parameters, resolve, tables)?;
    }
    if unresolved_count
        .iter()
        .any(|n| parameters.get(n - 1).is_some_and(Option::is_some))
    {
        return Err(SqlError::AmbiguousParameter);
    }
    let mut keys = vec![];
    let mut slots = vec![];
    for s in groups {
        let b = s.bind(resolve, tables, &parameters)?;
        slots.push(col(
            format!("__group_slot_{}", slots.len()),
            b.ty.clone(),
            false,
        ));
        keys.push(b);
    }
    let mut aggregates = vec![];
    let mut aggregate_columns = vec![];
    for (a, arg) in lower.aggregates {
        let Some(arg) = arg else {
            let column = col(
                format!("__group_slot_{}", slots.len()),
                DataType::BigInt,
                true,
            );
            slots.push(column.clone());
            aggregate_columns.push(column);
            aggregates.push(BoundItem::Aggregate(Aggregate::Count, None));
            continue;
        };
        if matches!(a, Aggregate::Sum) && arg.aggregate_unknown(&parameters) {
            return Err(SqlError::AmbiguousFunction);
        }
        let expected =
            if matches!(a, Aggregate::Min | Aggregate::Max) && arg.aggregate_unknown(&parameters) {
                Some(&DataType::Text)
            } else {
                None
            };
        arg.infer(expected, &mut parameters, resolve, tables)?;
        let legacy_column = matches!(arg, Scalar::Column(_));
        let b = arg.bind(resolve, tables, &parameters)?;
        let ty = match a {
            Aggregate::Count => DataType::BigInt,
            Aggregate::Sum => match b.ty {
                DataType::SmallInt | DataType::Integer => DataType::BigInt,
                DataType::BigInt | DataType::Numeric => DataType::Numeric,
                DataType::Real => DataType::Real,
                DataType::DoublePrecision => DataType::DoublePrecision,
                _ => {
                    return Err(if legacy_column {
                        SqlError::Type
                    } else {
                        SqlError::UndefinedFunction
                    });
                }
            },
            Aggregate::Min | Aggregate::Max => {
                if !legacy_column && matches!(b.ty, DataType::Boolean | DataType::Bytes) {
                    return Err(SqlError::UndefinedFunction);
                }
                b.ty.clone()
            }
        };
        let column = col(
            format!("__group_slot_{}", slots.len()),
            ty,
            matches!(a, Aggregate::Count),
        );
        slots.push(column.clone());
        aggregate_columns.push(column);
        aggregates.push(BoundItem::Aggregate(a, Some(b)));
    }
    let slot_table = TableDef {
        id: 0,
        name: "__groups".into(),
        columns: slots,
        primary_key: vec![],
    };
    let slot_tables = [&slot_table];
    let slot_resolve = |c: &ColumnRef, _: usize| -> R<BoundColumn> {
        Ok(BoundColumn {
            source: 0,
            column: slot_table
                .columns
                .iter()
                .position(|d| d.name == c.name)
                .ok_or(SqlError::GroupingError)?,
        })
    };
    for s in &output_syntax {
        s.infer(None, &mut parameters, &slot_resolve, &slot_tables)?;
    }
    if let Some(f) = &having_syntax {
        f.infer(&mut parameters, &slot_resolve, &slot_tables)?;
    }
    for (s, _, _) in &order_syntax {
        s.infer(None, &mut parameters, &slot_resolve, &slot_tables)?;
    }
    let outputs = output_syntax
        .iter()
        .map(|s| s.bind(&slot_resolve, &slot_tables, &parameters))
        .collect::<R<Vec<_>>>()?;
    let output_sources = projection
        .iter()
        .map(|(i, _)| match i {
            Item::Scalar(Scalar::Column(c)) => resolve(c, tables.len()).map(Some),
            _ => Ok(None),
        })
        .collect::<R<Vec<_>>>()?;
    let output_names = projection
        .iter()
        .zip(&outputs)
        .map(|((i, _), b)| match i {
            Item::Scalar(s) if !contains_aggregate(s) => {
                Ok(s.bind(resolve, tables, &parameters)?.name().to_owned())
            }
            _ => Ok(b.name().to_owned()),
        })
        .collect::<R<Vec<_>>>()?;
    let columns = outputs
        .iter()
        .zip(&projection)
        .zip(&output_sources)
        .zip(&output_names)
        .map(|(((b, (i, alias)), source), output_name)| {
            let mut column = if let Some(c) = source {
                let mut column = tables[c.source].columns[c.column].clone();
                if c.source > 0 && q.joins[c.source - 1].left {
                    column.not_null = false;
                }
                column
            } else {
                col(
                    match i {
                        Item::Aggregate(a, _) => match a {
                            Aggregate::Count => "count",
                            Aggregate::Sum => "sum",
                            Aggregate::Min => "min",
                            Aggregate::Max => "max",
                        }
                        .into(),
                        _ => output_name.clone(),
                    },
                    b.ty.clone(),
                    matches!(i, Item::Aggregate(Aggregate::Count, _)),
                )
            };
            if let Some(alias) = alias {
                column.name = alias.clone();
            }
            column
        })
        .collect();
    let having = having_syntax
        .as_ref()
        .map(|f| f.bind(&parameters, &slot_resolve, &slot_tables))
        .transpose()?;
    let order = order_syntax
        .iter()
        .map(|(s, d, n)| Ok((s.bind(&slot_resolve, &slot_tables, &parameters)?, *d, *n)))
        .collect::<R<Vec<_>>>()?;
    let filter = q
        .where_filter()
        .map(|f| f.bind(&parameters, resolve, tables))
        .transpose()?;
    let parameters = parameters
        .into_iter()
        .map(|p| p.ok_or(SqlError::Parameters))
        .collect::<R<Vec<_>>>()?;
    Ok(BoundQuery {
        query: q,
        tables: tables.to_vec(),
        joins,
        items: vec![],
        columns,
        parameters,
        predicates: vec![],
        filter,
        order: vec![],
        aggregate: true,
        grouping: Some(Grouping {
            keys,
            aggregates,
            aggregate_columns,
            outputs,
            output_sources,
            having,
            order,
        }),
    })
}
#[derive(Clone)]
struct Key(Vec<Value>);
impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Key {}
impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .iter()
            .zip(&other.0)
            .map(|(a, b)| cmp(a, b))
            .find(|o| *o != Ordering::Equal)
            .unwrap_or_else(|| self.0.len().cmp(&other.0.len()))
    }
}
fn retained(values: &[Value]) -> usize {
    std::mem::size_of_val(values) + values.iter().map(value_bytes).sum::<usize>()
}
fn page_expr(expr: Option<&Expr>, params: &[Value]) -> R<Option<u64>> {
    match expr.map(|e| e.value(params)).transpose()? {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Int(n)) if n >= 0 => Ok(Some(n as u64)),
        _ => Err(SqlError::Limit),
    }
}
impl Grouping {
    fn initial(&self) -> Vec<Value> {
        self.aggregates
            .iter()
            .map(|a| {
                if matches!(a, BoundItem::Aggregate(Aggregate::Count, _)) {
                    Value::Int(0)
                } else {
                    Value::Null
                }
            })
            .collect()
    }
    pub(super) fn execute(
        &self,
        query: &BoundQuery<'_>,
        state: &Relational,
        params: &[Value],
        cancel: &mut impl FnMut() -> bool,
    ) -> R<SqlResult> {
        let page = scan::Page {
            offset: page_expr(query.query.offset(), params)?.unwrap_or(0),
            limit: page_expr(query.query.limit.as_ref(), params)?,
        };
        if query.query.explain {
            return Ok(SqlResult {
                columns: vec!["plan".into()],
                rows: vec![SqlRow {
                    values: vec![Value::Text(format!(
                        "BoundedGroupAggregate; retained_bytes<={}; result_rows<={}; OutputLimit",
                        scan::MAX_RETAINED_BYTES,
                        scan::MAX_QUERY_ROWS
                    ))],
                    version: 0,
                }],
                affected: 0,
            });
        }
        let mut result = SqlResult {
            columns: query.columns.iter().map(|c| c.name.clone()).collect(),
            rows: vec![],
            affected: 0,
        };
        if page.limit == Some(0) {
            if cancel() {
                return Err(SqlError::Aborted);
            }
            return Ok(result);
        }
        let mut groups = BTreeMap::<Key, Vec<Value>>::new();
        let mut retention = scan::Retention::new(scan::MAX_RETAINED_BYTES);
        if self.keys.is_empty() {
            let initial = self.initial();
            retention.charge(retained(&initial) + std::mem::size_of::<Key>())?;
            groups.insert(Key(vec![]), initial);
        }
        let mut aggregate_work = 0;
        {
            let mut meter = scan::Meter::per_row(cancel);
            let mut accept = |row: Tuple| -> R<()> {
                let mut work = 0;
                if let Some(f) = &query.filter
                    && f.eval(&row, params, &mut work)? != Some(true)
                {
                    return Ok(());
                }
                let mut key_work = 0;
                let key = Key(self
                    .keys
                    .iter()
                    .map(|s| s.eval(&row, params, &mut key_work))
                    .collect::<R<Vec<_>>>()?);
                if !groups.contains_key(&key) {
                    if groups.len() >= scan::MAX_QUERY_ROWS {
                        return Err(SqlError::Limit);
                    }
                    let initial = self.initial();
                    retention.charge(
                        retained(&key.0) + retained(&initial) + 6 * std::mem::size_of::<usize>(),
                    )?;
                    groups.insert(key.clone(), initial);
                }
                let values = groups.get_mut(&key).ok_or(SqlError::Limit)?;
                let before = retained(values);
                update_aggregates(
                    &self.aggregates,
                    &self.aggregate_columns,
                    values,
                    &row,
                    params,
                    &mut aggregate_work,
                )?;
                let after = retained(values);
                if after > before {
                    retention.charge(after - before)?;
                } else {
                    retention.release(before - after);
                }
                Ok(())
            };
            if query.tables.is_empty() {
                meter.tick()?;
                accept(Tuple {
                    values: vec![],
                    version: 0,
                })?;
            } else {
                use scan::RowSource;
                let source = scan::LegacyRows(state);
                let range = scan::KeyRange {
                    lo: std::ops::Bound::Unbounded,
                    hi: std::ops::Bound::Unbounded,
                };
                for input in source.scan(query.tables[0], &range, scan::Direction::Forward)? {
                    meter.tick()?;
                    let input = input?;
                    let mut row = Tuple {
                        values: vec![input.row.values.clone()],
                        version: 0,
                    };
                    let mut skip = false;
                    for (i, join) in query.joins.iter().enumerate() {
                        let table = query.tables[i + 1];
                        let key = join
                            .key
                            .iter()
                            .map(|c| row.get(*c).clone())
                            .collect::<Vec<_>>();
                        let found = if key.contains(&Value::Null) {
                            None
                        } else {
                            source.get(table, &encode_key(&key)?)?
                        };
                        let matched = found.filter(|r| {
                            join.equalities
                                .iter()
                                .all(|(a, b)| equal(row.get(*a), &r.row.values[b.column]))
                        });
                        match matched {
                            Some(r) => row.values.push(r.row.values.clone()),
                            None if join.left => {
                                row.values.push(vec![Value::Null; table.columns.len()])
                            }
                            None => {
                                skip = true;
                                break;
                            }
                        }
                    }
                    if !skip {
                        accept(row)?;
                    }
                }
            }
        }
        let mut rows = vec![];
        for (key, values) in groups {
            let mut slots = key.0;
            slots.extend(values);
            let row = Tuple {
                values: vec![slots],
                version: 0,
            };
            let mut work = 0;
            if let Some(h) = &self.having
                && h.eval(&row, params, &mut work)? != Some(true)
            {
                continue;
            }
            let mut work = 0;
            let values = self
                .outputs
                .iter()
                .map(|s| s.eval(&row, params, &mut work))
                .collect::<R<Vec<_>>>()?;
            let mut work = 0;
            let keys = self
                .order
                .iter()
                .map(|(s, _, _)| s.eval(&row, params, &mut work))
                .collect::<R<Vec<_>>>()?;
            retention.charge(retained(&values) + retained(&keys))?;
            rows.push((SqlRow { values, version: 0 }, keys));
        }
        rows.sort_by(|(_, a), (_, b)| {
            for ((_, desc, first), (a, b)) in self.order.iter().zip(a.iter().zip(b)) {
                let ord = match (a, b) {
                    (Value::Null, Value::Null) => Ordering::Equal,
                    (Value::Null, _) => {
                        if *first {
                            Ordering::Less
                        } else {
                            Ordering::Greater
                        }
                    }
                    (_, Value::Null) => {
                        if *first {
                            Ordering::Greater
                        } else {
                            Ordering::Less
                        }
                    }
                    _ => {
                        let o = cmp(a, b);
                        if *desc { o.reverse() } else { o }
                    }
                };
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            Ordering::Equal
        });
        let mut bytes = 0;
        for (row, _) in page.apply(rows.into_iter()) {
            for value in &row.values {
                charge(&mut bytes, value)?;
            }
            result.rows.push(row);
        }
        if cancel() {
            return Err(SqlError::Aborted);
        }
        Ok(result)
    }
    pub(super) fn typmods(&self, state: &Relational, tables: &[&TableDef]) -> Vec<i32> {
        self.outputs
            .iter()
            .zip(&self.output_sources)
            .map(|(s, source)| {
                result_typmod(
                    state,
                    source.map_or(TypmodSource::Scalar(s), |c| {
                        TypmodSource::Column(&tables[c.source].columns[c.column])
                    }),
                )
            })
            .collect()
    }
    pub(super) fn validate_constants(&self, no_parameters: bool, budget: &mut usize) -> R<()> {
        for s in self
            .keys
            .iter()
            .chain(&self.outputs)
            .chain(self.order.iter().map(|(s, _, _)| s))
        {
            s.plan_constant(budget)?;
        }
        if no_parameters {
            for a in &self.aggregates {
                if let BoundItem::Aggregate(_, Some(s)) = a {
                    s.plan_constant(budget)?;
                }
            }
            if let Some(h) = &self.having {
                h.plan_constant(budget)?;
            }
        }
        Ok(())
    }
    pub(super) fn attach_clock(&mut self, clock: Option<&super::super::clock::ExecutionClock>) {
        for s in self
            .keys
            .iter_mut()
            .chain(&mut self.outputs)
            .chain(self.order.iter_mut().map(|(s, _, _)| s))
        {
            s.attach_clock(clock);
        }
        for a in &mut self.aggregates {
            if let BoundItem::Aggregate(_, Some(s)) = a {
                s.attach_clock(clock);
            }
        }
        if let Some(h) = &mut self.having {
            h.attach_clock(clock);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn state(rows: usize) -> Relational {
        let mut state = Relational::default();
        let create = Prepared::parse(
            "CREATE TABLE group_cap (id bigint PRIMARY KEY)",
            1,
            state.epoch,
        )
        .unwrap();
        state
            .execute_with_cancel(&create, &[], None, || false)
            .unwrap();
        let table = &state.tables["group_cap"];
        let data = state.rows.get_mut(&table.id).unwrap();
        for n in 0..rows {
            let values = vec![Value::Int(n as i64)];
            data.insert(encode_key(&values).unwrap(), SqlRow { values, version: 1 });
        }
        state
    }
    fn query(sql: &str) -> Query {
        Query::parse(&mut Parser {
            tokens: lex(sql).unwrap(),
            at: 0,
            params: BTreeSet::new(),
        })
        .unwrap()
    }
    #[test]
    fn grouped_input_and_final_cancel_fences_leave_source_unchanged() {
        let state = state(3);
        let before = bincode::serialize(&state).unwrap();
        let query = query("SELECT id,count(*) FROM group_cap GROUP BY id");
        let bound = query.bind(&state, 0, &[]).unwrap();
        for fence in [3, 5] {
            let mut polls = 0;
            assert_eq!(
                bound.execute(&state, &[], &mut || {
                    polls += 1;
                    polls == fence
                }),
                Err(SqlError::Aborted)
            );
            assert_eq!(polls, fence);
            assert_eq!(bincode::serialize(&state).unwrap(), before);
        }
    }
    #[test]
    fn production_group_count_cap_precedes_paging() {
        // The public stored-row cap is lower; an in-memory source exercises the
        // physical group cap itself, without changing production constants.
        let mut state = state(scan::MAX_QUERY_ROWS + 1);
        let query = query("SELECT count(*) FROM group_cap GROUP BY id LIMIT 1");
        assert_eq!(
            query
                .bind(&state, 0, &[])
                .unwrap()
                .execute(&state, &[], &mut || false),
            Err(SqlError::Limit)
        );
        let table = &state.tables["group_cap"];
        let _ = state.rows.get_mut(&table.id).unwrap().pop_last();
        let result = query
            .bind(&state, 0, &[])
            .unwrap()
            .execute(&state, &[], &mut || false)
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values, vec![Value::Int(1)]);
    }
}
