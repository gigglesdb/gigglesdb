//! Bounded grouping plan. Synthetic slots hold keys, aggregate transitions and
//! columns proven dependent on a fully grouped declared primary key.
use super::*;

pub(super) struct Grouping {
    keys: Vec<BoundScalar>,
    aggregates: Vec<BoundItem>,
    aggregate_filters: Vec<Option<BoundFilter>>,
    direct_inputs: Vec<bool>,
    dependents: Vec<BoundColumn>,
    aggregate_columns: Vec<ColumnDef>,
    outputs: Vec<BoundScalar>,
    output_sources: Vec<Option<BoundColumn>>,
    order_slots: Vec<usize>,
    having: Option<BoundFilter>,
    having_outer: Vec<(BoundColumn, BoundScalar)>,
    order: Vec<(BoundScalar, bool, bool)>,
}
fn aggregate(name: &str) -> Option<Aggregate> {
    Aggregate::from_name(name)
}
pub(super) fn contains_aggregate(s: &Scalar) -> bool {
    match s {
        Scalar::Call(n, a) => {
            n == aggregate_filter::CALL
                || aggregate(n).is_some()
                || a.iter().any(contains_aggregate)
        }
        Scalar::Unary(_, a) | Scalar::Cast(a, _) | Scalar::Domain(a, ..) => contains_aggregate(a),
        Scalar::Binary(_, a, b) => contains_aggregate(a) || contains_aggregate(b),
        Scalar::SubstringOrder(a) => a.iter().any(|a| contains_aggregate(a)),
        Scalar::Predicate(f) => filter_contains_aggregate(f),
        Scalar::Case(c) => {
            c.arms
                .iter()
                .any(|(f, s)| filter_contains_aggregate(f) || contains_aggregate(s))
                || c.otherwise.as_ref().is_some_and(contains_aggregate)
        }
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
        Scalar::Domain(a, name, _) => {
            format!("domain:{name}:{}", canonical(a, resolve, scope, tables)?)
        }
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
        Scalar::Predicate(f) => {
            canonical_fields("predicate", &[canonical_filter(f, resolve, scope, tables)?])
        }
        Scalar::Case(c) => {
            let mut fields = c
                .arms
                .iter()
                .map(|(f, s)| {
                    Ok(canonical_fields(
                        "arm",
                        &[
                            canonical_filter(f, resolve, scope, tables)?,
                            canonical(s, resolve, scope, tables)?,
                        ],
                    ))
                })
                .collect::<R<Vec<_>>>()?;
            fields.push(match &c.otherwise {
                Some(s) => canonical_fields("else", &[canonical(s, resolve, scope, tables)?]),
                None => "no-else".into(),
            });
            canonical_fields("case", &fields)
        }
        _ => format!("{s:?}"),
    })
}
fn canonical_fields(tag: &str, fields: &[String]) -> String {
    let mut out = tag.to_owned();
    for field in fields {
        out.push_str(&format!(":{}:{field}", field.len()));
    }
    out
}
fn canonical_filter(
    f: &Filter,
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    scope: usize,
    tables: &[&TableDef],
) -> R<String> {
    let scalar = |s| canonical(s, resolve, scope, tables);
    let filter = |f| canonical_filter(f, resolve, scope, tables);
    Ok(match f {
        Filter::Clauses(_) | Filter::Exists { .. } => return Err(SqlError::Unsupported),
        Filter::InSubquery {
            value,
            query,
            negated,
        } => canonical_fields(
            "in-subquery",
            &[negated.to_string(), scalar(value)?, format!("{query:?}")],
        ),
        Filter::Scalar(s) => canonical_fields("scalar", &[scalar(s)?]),
        Filter::Null(s, not) => canonical_fields("null", &[not.to_string(), scalar(s)?]),
        Filter::Compare(a, op, b) => {
            canonical_fields("compare", &[op.clone(), scalar(a)?, scalar(b)?])
        }
        Filter::Like {
            value,
            pattern,
            escape,
            insensitive,
            negated,
        } => canonical_fields(
            "like",
            &[
                insensitive.to_string(),
                negated.to_string(),
                scalar(value)?,
                scalar(pattern)?,
                match escape {
                    Some(s) => canonical_fields("escape", &[scalar(s)?]),
                    None => "no-escape".into(),
                },
            ],
        ),
        Filter::Not(f) => canonical_fields("not", &[filter(f)?]),
        Filter::Is(f, target, not) => canonical_fields(
            "is",
            &[
                match target {
                    Some(true) => "true",
                    Some(false) => "false",
                    None => "unknown",
                }
                .into(),
                not.to_string(),
                filter(f)?,
            ],
        ),
        Filter::Logic(a, and, b) => {
            canonical_fields("logic", &[and.to_string(), filter(a)?, filter(b)?])
        }
    })
}
struct Lower<'a, F> {
    groups: Vec<String>,
    scope: usize,
    resolve: &'a F,
    tables: &'a [&'a TableDef],
    aggregates: Vec<(Aggregate, Option<Scalar>, Option<Filter>)>,
    dependent_sources: Vec<bool>,
    dependents: Vec<BoundColumn>,
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
            Scalar::Column(c) => {
                let c = (self.resolve)(c, self.scope)?;
                if !self.dependent_sources[c.source] {
                    return Err(SqlError::GroupingError);
                }
                let index = self
                    .dependents
                    .iter()
                    .position(|d| d.source == c.source && d.column == c.column)
                    .unwrap_or_else(|| {
                        self.dependents.push(c);
                        self.dependents.len() - 1
                    });
                Scalar::Column(ColumnRef {
                    qualifier: None,
                    name: format!("__dependent_slot_{index}"),
                })
            }
            Scalar::Call(n, args) if n == aggregate_filter::CALL => {
                let (a, arg, filter) = aggregate_filter::parts(args, self.resolve, self.tables)?;
                let index = self.groups.len() + self.aggregates.len();
                self.aggregates.push((a, arg, Some(filter)));
                slot(index)
            }
            Scalar::Call(n, args) if aggregate(n).is_some() => {
                if args.len() != 1 {
                    return Err(SqlError::UndefinedFunction);
                }
                reject_nested_aggregate(&args[0])?;
                let index = self.groups.len() + self.aggregates.len();
                self.aggregates.push((
                    aggregate(n).ok_or(SqlError::UndefinedFunction)?,
                    Some(args[0].clone()),
                    None,
                ));
                slot(index)
            }
            Scalar::Unary(op, a) => Scalar::Unary(*op, Box::new(self.scalar(a)?)),
            Scalar::Domain(a, name, ty) => {
                Scalar::Domain(Box::new(self.scalar(a)?), name.clone(), ty.clone())
            }
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
            Scalar::Predicate(f) => Scalar::Predicate(Box::new(self.filter(f)?)),
            Scalar::Case(c) => Scalar::Case(Box::new(functions::CaseExpr {
                arms: c
                    .arms
                    .iter()
                    .map(|(f, s)| Ok((self.filter(f)?, self.scalar(s)?)))
                    .collect::<R<Vec<_>>>()?,
                otherwise: c.otherwise.as_ref().map(|s| self.scalar(s)).transpose()?,
            })),
            _ => s.clone(),
        })
    }
    fn filter(&mut self, f: &Filter) -> R<Filter> {
        Ok(match f {
            Filter::Clauses(_) => return Err(SqlError::Unsupported),
            Filter::InSubquery { value, query, negated } => Filter::InSubquery {
                value: self.scalar(value)?, query: query.clone(), negated: *negated,
            },
            Filter::Exists { .. } => f.clone(),
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
            Filter::Is(f, target, not) => Filter::Is(Box::new(self.filter(f)?), *target, *not),
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
            a.call_name(),
            vec![arg.clone().unwrap_or(Scalar::Literal(Value::Int(1)))],
        )),
        Item::Star | Item::QualifiedStar { .. } => Err(SqlError::GroupingError),
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
pub(super) fn projection(q: &Query, tables: &[&TableDef]) -> R<Vec<(Item, Option<String>)>> {
    let mut projection = vec![];
    for (item, alias) in &q.items {
        if matches!(item, Item::Star | Item::QualifiedStar { .. }) {
            for index in q.star_sources(item)? {
                let source = &q.sources[index];
                let table = tables[index];
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
    Ok(projection)
}
pub(super) fn output_order(
    q: &Query,
    s: &Scalar,
    projection: &[(Item, Option<String>)],
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
) -> R<Scalar> {
    let s = resolve_output(s, projection, false, resolve, tables.len(), tables)?;
    if q.distinct() {
        output_slot(&s, projection, resolve, tables)?;
    }
    Ok(s)
}
pub(super) fn output_slot(
    s: &Scalar,
    projection: &[(Item, Option<String>)],
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    tables: &[&TableDef],
) -> R<usize> {
    let identity = canonical(s, resolve, tables.len(), tables)?;
    for (i, (item, _)) in projection.iter().enumerate() {
        if canonical(&output_scalar(item)?, resolve, tables.len(), tables)? == identity {
            return Ok(i);
        }
    }
    Err(SqlError::InvalidColumnReference)
}
pub(super) fn bind<'a>(
    q: &'a Query,
    tables: &[&'a TableDef],
    mut parameters: Vec<Option<DataType>>,
    resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    mut subqueries: Subplans<'a>,
) -> R<BoundQuery<'a>> {
    let projection = projection(q, tables)?;
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
        aggregate_filter::validate_scalar(s, &parameters, resolve, tables)?;
        reject_nested_aggregate(s)?;
        s.infer(None, &mut parameters, resolve, tables)?;
    }
    if let Some(f) = q.where_filter() {
        f.infer_with_subqueries(&mut parameters, resolve, tables, &subqueries)?;
    }
    for expr in q.limit.iter().chain(q.offset()) {
        if let Expr::Param(n) = expr {
            let p = parameters.get_mut(n - 1).ok_or(SqlError::Parameters)?;
            if p.is_none() {
                *p = Some(DataType::BigInt);
            }
        }
    }
    // FILTER consumes input rows, so validate its routine and Boolean predicate
    // before an unrelated ungrouped output column can mask those errors.
    for (item, _) in &projection {
        if let Item::Scalar(s) | Item::Aggregate(_, Some(s)) = item {
            aggregate_filter::validate_scalar(s, &parameters, resolve, tables)?;
        }
    }
    if let Some(filter) = q.having() {
        aggregate_filter::validate_filter(filter, &parameters, resolve, tables)?;
    }
    for (s, _, _) in &q.order {
        aggregate_filter::validate_scalar(s, &parameters, resolve, tables)?;
    }
    let group_identity = groups
        .iter()
        .map(|s| canonical(s, resolve, tables.len(), tables))
        .collect::<R<Vec<_>>>()?;
    let dependent_sources = tables
        .iter()
        .enumerate()
        .map(|(source, table)| {
            source >= q.sources.len() || table.hidden_rowid().is_none()
                && !table.primary_key.is_empty()
                && table
                    .primary_key
                    .iter()
                    .all(|column| group_identity.contains(&format!("column:{source}:{column}")))
        })
        .collect();
    let mut lower = Lower {
        groups: group_identity,
        scope: tables.len(),
        resolve,
        tables,
        aggregates: vec![],
        dependent_sources,
        dependents: vec![],
    };
    let output_syntax = projection
        .iter()
        .map(|(i, _)| {
            if let Item::Aggregate(Aggregate::Count, None) = i {
                let index = lower.groups.len() + lower.aggregates.len();
                lower.aggregates.push((Aggregate::Count, None, None));
                Ok(slot(index))
            } else {
                lower.scalar(&output_scalar(i)?)
            }
        })
        .collect::<R<Vec<_>>>()?;
    // Validate every correlation against the group keys/dependent columns, then
    // reconstruct the input source shape from post-group values for child frames.
    let mut having_outer_syntax = vec![];
    if let Some(f) = q.having() {
        f.visit_subqueries(&mut |_, child| {
            for c in subqueries.outer_columns(child as *const Query as usize) {
                let scalar = Scalar::Column(ColumnRef {
                    qualifier: Some(q.sources[c.source].alias.clone()),
                    name: tables[c.source].columns[c.column].name.clone(),
                });
                having_outer_syntax.push((c, lower.scalar(&scalar)?));
            }
            Ok(())
        })?;
    }
    let having_syntax = q.having().map(|f| lower.filter(f)).transpose()?;
    if let (Some(original), Some(lowered)) = (q.having(), &having_syntax) {
        let mut keys = vec![];
        original.visit_subqueries(&mut |_, child| { keys.push(child as *const Query as usize); Ok(()) })?;
        let mut keys = keys.into_iter();
        lowered.visit_subqueries(&mut |_, child| {
            subqueries.alias(child as *const Query as usize, keys.next().ok_or(SqlError::Unsupported)?);
            Ok(())
        })?;
    }
    let order_slots = if q.distinct() {
        q.order
            .iter()
            .map(|(s, _, _)| {
                let s = output_order(q, s, &projection, resolve, tables)?;
                output_slot(&s, &projection, resolve, tables)
            })
            .collect::<R<Vec<_>>>()?
    } else {
        vec![]
    };
    let order_syntax = q
        .order
        .iter()
        .map(|(s, d, n)| {
            Ok((
                lower.scalar(&output_order(q, s, &projection, resolve, tables)?)?,
                *d,
                *n,
            ))
        })
        .collect::<R<Vec<_>>>()?;
    let mut unresolved_count = vec![];
    for (a, arg, _) in &lower.aggregates {
        let Some(arg) = arg else {
            continue;
        };
        if matches!(a.base(), Aggregate::Count)
            && let Scalar::Param(n) = arg
            && parameters.get(n - 1).is_none_or(Option::is_none)
        {
            unresolved_count.push(*n);
        }
        if matches!(a.base(), Aggregate::Sum | Aggregate::Avg) && arg.aggregate_unknown(&parameters)
        {
            return Err(SqlError::AmbiguousFunction);
        }
        let expected = if matches!(a.base(), Aggregate::Min | Aggregate::Max)
            && arg.aggregate_unknown(&parameters)
        {
            Some(&DataType::Text)
        } else {
            None
        };
        arg.infer(expected, &mut parameters, resolve, tables)?;
    }
    for (_, _, filter) in &lower.aggregates {
        if let Some(filter) = filter {
            filter.infer(&mut parameters, resolve, tables)?;
        }
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
        if b.ty == DataType::Bpchar {
            return Err(SqlError::Unsupported);
        }
        slots.push(col(
            format!("__group_slot_{}", slots.len()),
            b.ty.clone(),
            false,
        ));
        keys.push(b);
    }
    let mut aggregates = vec![];
    let mut aggregate_columns = vec![];
    let mut aggregate_filters = vec![];
    // V2 DISTINCT inputs spill (R2-PGLIMITS-4), so a direct column read is
    // row-local for them too.
    let direct_inputs = lower.aggregates.iter().map(|(_, arg, _)|
        matches!(arg, Some(Scalar::Column(_))))
        .collect::<Vec<_>>();
    for (a, arg, filter) in lower.aggregates {
        aggregate_filters.push(
            filter
                .as_ref()
                .map(|f| f.bind(&parameters, resolve, tables))
                .transpose()?,
        );
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
        if matches!(a.base(), Aggregate::Sum | Aggregate::Avg) && arg.aggregate_unknown(&parameters)
        {
            return Err(SqlError::AmbiguousFunction);
        }
        let expected = if matches!(a.base(), Aggregate::Min | Aggregate::Max)
            && arg.aggregate_unknown(&parameters)
        {
            Some(&DataType::Text)
        } else {
            None
        };
        arg.infer(expected, &mut parameters, resolve, tables)?;
        let legacy_column = matches!(arg, Scalar::Column(_));
        let b = arg.bind(resolve, tables, &parameters)?;
        if b.ty == DataType::Bpchar {
            return Err(SqlError::Unsupported);
        }
        let ty = match a.base() {
            Aggregate::Count => DataType::BigInt,
            Aggregate::Avg => match b.ty {
                DataType::SmallInt | DataType::Integer | DataType::BigInt | DataType::Numeric => {
                    DataType::Numeric
                }
                DataType::Real | DataType::DoublePrecision => DataType::DoublePrecision,
                _ => return Err(SqlError::UndefinedFunction),
            },
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
            _ => unreachable!(),
        };
        let column = col(
            format!("__group_slot_{}", slots.len()),
            ty,
            matches!(a.base(), Aggregate::Count),
        );
        slots.push(column.clone());
        aggregate_columns.push(column);
        aggregates.push(BoundItem::Aggregate(a, Some(b)));
    }
    for (index, c) in lower.dependents.iter().enumerate() {
        let mut column = tables[c.source].columns[c.column].clone();
        column.name = format!("__dependent_slot_{index}");
        if c.source > 0 && q.joins.get(c.source - 1).is_some_and(|j| j.left) {
            column.not_null = false;
        }
        slots.push(column);
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
        f.infer_with_subqueries(&mut parameters, &slot_resolve, &slot_tables, &subqueries)?;
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
                if let Item::Scalar(Scalar::Column(c)) = i { column.name = c.name.clone(); }
                if c.source > 0 && q.joins.get(c.source - 1).is_some_and(|j| j.left) {
                    column.not_null = false;
                }
                column
            } else {
                col(
                    match i {
                        Item::Aggregate(a, _) => a.name().into(),
                        Item::Scalar(s) if aggregate_filter::kind(s).is_some() => aggregate_filter::kind(s).unwrap().name().into(),
                        _ => output_name.clone(),
                    },
                    b.ty.clone(),
                    matches!(i, Item::Aggregate(kind, _) if matches!(kind.base(), Aggregate::Count))
                        || matches!(i, Item::Scalar(s) if aggregate_filter::kind(s).is_some_and(|a| matches!(a.base(), Aggregate::Count))),
                )
            };
            if let Some(alias) = alias {
                column.name = alias.clone();
            }
            column
        })
        .collect();
    let having_outer = having_outer_syntax.iter().map(|(c, s)|
        Ok((*c, s.bind(&slot_resolve, &slot_tables, &parameters)?))).collect::<R<Vec<_>>>()?;
    // HAVING scalar operands use the final tuple slot, after original sources.
    let having_source = if having_outer.is_empty() { 0 } else { q.sources.len() };
    let having_tables = std::iter::repeat_n(&slot_table, having_source + 1).collect::<Vec<_>>();
    let having_resolve = |c: &ColumnRef, scope| {
        let mut column = slot_resolve(c, scope)?;
        column.source = having_source;
        Ok(column)
    };
    let having = having_syntax
        .as_ref()
        .map(|f| f.bind_with_subqueries(&parameters, &having_resolve, &having_tables, &subqueries))
        .transpose()?;
    let order = order_syntax
        .iter()
        .enumerate()
        .map(|(i, (s, d, n))| {
            let original = output_order(q, &q.order[i].0, &projection, resolve, tables)?
                .bind(resolve, tables, &parameters);
            let bound = s.bind(&slot_resolve, &slot_tables, &parameters)?;
            let bound = if let Some(ty) = original.ok().and_then(|s| s.enum_type(tables)) {
                bound.with_enum(&ty)
            } else { bound };
            Ok((bound, *d, *n))
        })
        .collect::<R<Vec<_>>>()?;
    let filter = q
        .where_filter()
        .map(|f| f.bind_with_subqueries(&parameters, resolve, tables, &subqueries))
        .transpose()?;
    let joins = q.bind_joins(tables, &mut parameters, resolve, &subqueries)?;
    let predicates = q.predicates.iter().filter_map(|(c, op, e)| {
        let c = resolve(c, tables.len()).ok()?;
        let ty = &tables[c.source].columns[c.column].data_type;
        // Range selection is optional; the bound WHERE remains authoritative.
        let safe = match e {
            Expr::Literal(v) => check(v, &tables[c.source].columns[c.column]).is_ok()
                && !comparison_requires_filter(v, ty).ok()?,
            Expr::Param(n) => parameters.get(n - 1) == Some(&Some(ty.clone())),
        };
        (*ty != DataType::Bpchar && safe)
            .then_some((c, op.as_str(), e))
    }).collect();
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
        predicates,
        filter,
        order: vec![],
        aggregate: true,
        subqueries,
        outer_slots: vec![],
        exists_keys: vec![],
        declared: vec![],
        distinct_order_slots: vec![],
        grouping: Some(Grouping {
            keys,
            aggregates,
            aggregate_filters,
            direct_inputs,
            aggregate_columns,
            dependents: lower.dependents,
            outputs,
            output_sources,
            order_slots,
            having,
            having_outer,
            order,
        }),
    })
}
#[derive(Clone)]
pub(super) struct Key(pub(super) Vec<Value>);
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
        cmp_values(&self.0, &other.0)
    }
}
/// Grouping and DISTINCT identity order.
pub(super) fn cmp_values(a: &[Value], b: &[Value]) -> Ordering {
    a.iter()
        .zip(b)
        .map(|(a, b)| cmp(a, b))
        .find(|o| *o != Ordering::Equal)
        .unwrap_or_else(|| a.len().cmp(&b.len()))
}
type GroupInput = (Vec<Value>, Tuple);
fn group_key_cmp(a: &GroupInput, b: &GroupInput) -> Ordering {
    cmp_values(&a.0, &b.0)
}
fn retained(values: &[Value]) -> usize {
    std::mem::size_of_val(values) + values.iter().map(value_bytes).sum::<usize>()
}
fn page_expr(query: &Query, expr: Option<&Expr>, params: &[Value], limit: bool) -> R<Option<u64>> {
    match expr.map(|e| query.page_value(e, params, limit)).transpose()? {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Int(n)) if n >= 0 => Ok(Some(n as u64)),
        Some(Value::Int(_)) => Err(if limit { SqlError::InvalidRowCountInLimitClause } else { SqlError::InvalidRowCountInResultOffsetClause }),
        _ => Err(SqlError::Limit),
    }
}
impl Grouping {
    fn initial(&self) -> Vec<Value> {
        self.aggregates
            .iter()
            .map(|a| {
                if matches!(a, BoundItem::Aggregate(kind, _) if matches!(kind.base(), Aggregate::Count)) {
                    Value::Int(0)
                } else {
                    Value::Null
                }
            })
            .collect()
    }
    #[allow(clippy::too_many_arguments)]
    pub(super) fn execute(
        &self,
        query: &BoundQuery<'_>,
        state: &Relational,
        source: &dyn scan::RowSource,
        params: &[Value],
        execution: &Execution<'_>,
        exists: bool,
        mut output: Output,
    ) -> R<SqlResult> {
        let mut cancel_impl = || execution.cancelled();
        let cancel = &mut cancel_impl;
        let page = scan::Page {
            offset: page_expr(query.query, query.query.offset(), params, false)?.unwrap_or(0),
            limit: page_expr(query.query, query.query.limit.as_ref(), params, true)?,
        };
        if query.query.explain {
            return Ok(SqlResult {
                columns: vec!["plan".into()],
                rows: vec![SqlRow {
                    values: vec![Value::Text(format!(
                        "BoundedGroupAggregate{}; retained_bytes<={}; result_rows<={}; OutputLimit",
                        if query.query.has_in_subquery() {
                            "; InSubquery"
                        } else {
                            ""
                        },
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
        let mut groups = BTreeMap::<Key, (Vec<Value>, Vec<AggregateAuxiliary>)>::new();
        // V2 hash-aggregates in memory up to work_mem (PostgreSQL HashAgg).
        // Past that, rows of groups not in memory sort on the group key
        // (spilling) and stream one group's state at a time; both halves merge
        // in key order. DISTINCT aggregates always take the sorted path.
        let v2 = source.streams_reductions();
        let sort_groups = v2 && !self.keys.is_empty();
        let hash = sort_groups && !self.aggregates.iter().any(|a| matches!(a, BoundItem::Aggregate(a, _) if a.distinct()));
        let mut hash_bytes = 0usize;
        let mut sorter = spill::Sorter::<GroupInput>::new();
        let mut scope = RetentionScope::new(execution);
        let mut retention = scope.borrow();
        if self.keys.is_empty() {
            let initial = self.initial();
            retention.charge(retained(&initial) + std::mem::size_of::<Key>())?;
            let mut auxiliary = aggregate_auxiliary(&self.aggregates, &mut retention)?;
            if source.streams_reductions() {
                for (aux, direct) in auxiliary.iter_mut().zip(&self.direct_inputs) {
                    aux.stream_column = *direct;
                    aux.local_transition = true;
                }
            }
            groups.insert(Key(vec![]), (initial, auxiliary));
        }
        drop(retention);
        let mut aggregate_work = 0;
        let outer = query.outer_values(execution);

        {
            let mut meter = scan::Meter::per_row(cancel);
            let mut accept = |row: &Tuple| -> R<()> {
                let mut work = 0;
                if let Some(f) = &query.filter
                    && f.eval_with_subqueries(
                        row, params, &mut work, query, state, source, execution,
                    )? != Some(true)
                {
                    return Ok(());
                }
                let mut retention = scope.borrow();
                let mut key_work = 0;
                let key = Key(self
                    .keys
                    .iter()
                    .map(|s| s.eval(row, params, &mut key_work))
                    .collect::<R<Vec<_>>>()?);
                if sort_groups && (!hash || (hash_bytes > spill::work_mem() && !groups.contains_key(&key))) {
                    let bytes = retained(&key.0) + std::mem::size_of::<GroupInput>()
                        + row.values.iter().map(|v| retained(v) + std::mem::size_of::<Vec<Value>>()).sum::<usize>();
                    return sorter.push((key.0, row.clone()), bytes, group_key_cmp, &mut *retention);
                }
                if !groups.contains_key(&key) {
                    if !hash && groups.len() >= scan::query_row_limit() {
                        return Err(SqlError::Limit);
                    }
                    let mut initial = self.initial();
                    initial.extend(self.dependents.iter().map(|c| row.get(*c).clone()));
                    let bytes = retained(&key.0) + retained(&initial) + 6 * std::mem::size_of::<usize>();
                    hash_bytes += bytes + self.aggregates.len() * std::mem::size_of::<AggregateAuxiliary>();
                    retention.charge(bytes)?;
                    let mut auxiliary = aggregate_auxiliary(&self.aggregates, &mut retention)?;
                    if source.streams_reductions() {
                        for (aux, direct) in auxiliary.iter_mut().zip(&self.direct_inputs) {
                            aux.stream_column = *direct;
                            aux.local_transition = true;
                        }
                    }
                    groups.insert(key.clone(), (initial, auxiliary));
                }
                let (values, auxiliary) = groups.get_mut(&key).ok_or(SqlError::Limit)?;
                let before = retained(values);
                update_filtered_aggregates(
                    &self.aggregates,
                    &self.aggregate_filters,
                    &self.aggregate_columns,
                    values,
                    auxiliary,
                    &mut retention,
                    row,
                    params,
                    &mut aggregate_work,
                    None,
                )?;
                let after = retained(values);
                if after > before {
                    retention.charge(after - before)?;
                } else {
                    retention.release(before - after);
                }
                Ok(())
            };
            if query.query.sources.is_empty() {
                meter.tick()?;
                accept(&Tuple {
                    values: outer.clone(),
                    version: 0,
                })?;
            } else {
                let range = scan::KeyRange {
                    lo: std::ops::Bound::Unbounded,
                    hi: std::ops::Bound::Unbounded,
                };
                let input = if source.streams_reductions() {
                    let (empty, bounds) = query.base_bounds(params)?;
                    let range = if empty { scan::KeyRange { lo: std::ops::Bound::Excluded(vec![]), hi: std::ops::Bound::Excluded(vec![]) } }
                        else { scan::KeyRange::from_bounds(bounds) };
                    if exists { query.exists_input(source, params, &outer, &range)? }
                    else { query.input_rows(source, params, &range, empty)? }
                } else { source.scan(query.tables[0], &range, scan::Direction::Forward)? };
                for row in join_rows(query, source, input, params, &mut meter, &execution.visits, &outer, exists, state, execution) {
                    accept(&row?)?;
                }
            }
        }
        let mut input_work = 0;
        type Group = (Key, (Vec<Value>, Vec<AggregateAuxiliary>));
        let groups: Box<dyn Iterator<Item = R<Group>> + '_> = if sort_groups {
            let mut sorted = sorter.finish(group_key_cmp, &mut *scope.borrow())?.peekable();
            let sorted_groups = std::iter::from_fn(move || {
                let (key, mut row) = match sorted.next()? {
                    Ok(first) => first,
                    Err(error) => return Some(Err(error)),
                };
                Some((|| {
                    let group_scope = RetentionScope::new(execution);
                    let mut retention = group_scope.borrow();
                    let mut values = self.initial();
                    values.extend(self.dependents.iter().map(|c| row.get(*c).clone()));
                    let mut auxiliary = aggregate_auxiliary(&self.aggregates, &mut retention)?;
                    for (aux, direct) in auxiliary.iter_mut().zip(&self.direct_inputs) {
                        aux.stream_column = *direct;
                        aux.local_transition = true;
                    }
                    loop {
                        update_filtered_aggregates(&self.aggregates, &self.aggregate_filters,
                            &self.aggregate_columns, &mut values, &mut auxiliary, &mut retention,
                            &row, params, &mut input_work, None)?;
                        match sorted.peek() {
                            Some(Ok((next, _))) if cmp_values(next, &key) == Ordering::Equal => {
                                row = sorted.next().ok_or(SqlError::Limit)??.1;
                            }
                            _ => break,
                        }
                    }
                    flush_distinct(&self.aggregates, &self.aggregate_columns, &mut values,
                        &mut auxiliary, &mut retention, &mut input_work)?;
                    Ok((Key(key), (values, auxiliary)))
                })())
            });
            // In-memory and spilled groups have disjoint keys; merge in key order.
            let (mut memory, mut sorted) = (groups.into_iter().peekable(), sorted_groups.peekable());
            Box::new(std::iter::from_fn(move || {
                let from_memory = match (memory.peek(), sorted.peek()) {
                    (None, None) => return None,
                    (Some((a, _)), Some(Ok((b, _)))) => a < b,
                    (head, _) => head.is_some(),
                };
                if from_memory { memory.next().map(Ok) } else { sorted.next() }
            }))
        } else {
            // The no-key v2 group defers DISTINCT aggregate inputs like the sorted path.
            let mut retention = scope.borrow();
            for (values, auxiliary) in groups.values_mut() {
                flush_distinct(&self.aggregates, &self.aggregate_columns, values, auxiliary,
                    &mut retention, &mut input_work)?;
            }
            drop(retention);
            Box::new(groups.into_iter().map(Ok))
        };
        let mut rows = vec![];
        let order = &self.order;
        let order_cmp = |a: &(SqlRow, Vec<Value>), b: &(SqlRow, Vec<Value>)| order_values(&a.1, &b.1, order);
        let mut ordered = spill::Sorter::new();
        let mut distinct = std::collections::BTreeSet::new();
        let mut skipped = 0;
        for group in groups {
            let (key, (mut values, auxiliary)) = group?;
            finish_aggregates(
                &self.aggregates,
                &mut values,
                &auxiliary,
                &mut aggregate_work,
            )?;
            let mut slots = key.0;
            slots.extend(values);
            let row = Tuple {
                values: vec![slots],
                version: 0,
            };
            let mut work = 0;
            if let Some(h) = &self.having {
                if self.having_outer.is_empty() {
                    if h.eval_with_subqueries(&row, params, &mut work, query, state, source, execution)? != Some(true) {
                        continue;
                    }
                } else {
                    let frame_scope = RetentionScope::new(execution);
                    let frame_bytes = (query.query.sources.len() + 1) * std::mem::size_of::<Vec<Value>>()
                        + self.having_outer.iter().map(|(c, _)| (c.column + 1) * std::mem::size_of::<Value>()).sum::<usize>()
                        + retained(&row.values[0]) * (self.having_outer.len() + 1);
                    frame_scope.borrow().charge(frame_bytes)?;
                    let mut values = vec![vec![]; query.query.sources.len()];
                    for (column, scalar) in &self.having_outer {
                        if values[column.source].len() <= column.column {
                            values[column.source].resize(column.column + 1, Value::Null);
                        }
                        values[column.source][column.column] = scalar.eval(&row, params, &mut work)?;
                    }
                    values.push(row.values[0].clone());
                    let having_row = Tuple { values, version: 0 };
                    if h.eval_with_subqueries(&having_row, params, &mut work, query, state, source, execution)? != Some(true) {
                        continue;
                    }
                }
            }
            let mut retention = scope.borrow();
            if exists && (!query.query.distinct() || page.offset == 0) {
                if skipped == page.offset {
                    result.rows.push(SqlRow { values: vec![], version: 0 });
                    return Ok(result);
                }
                skipped += 1;
                continue;
            }
            let mut work = 0;
            let values = self
                .outputs
                .iter()
                .map(|s| s.eval(&row, params, &mut work))
                .collect::<R<Vec<_>>>()?;
            let values = if query.query.distinct() && !v2 {
                let key = Key(values);
                if distinct.contains(&key) {
                    continue;
                }
                distinct_charge(&key.0, &self.order_slots, &mut retention)?;
                let values = key.0.clone();
                distinct.insert(key);
                values
            } else {
                values
            };
            // V2 groups already leave in key order; with no ORDER BY or DISTINCT
            // they go straight to the output instead of through a sort.
            if v2 && self.order.is_empty() && !query.query.distinct() {
                if page.limit.is_some_and(|limit| skipped >= page.offset.saturating_add(limit)) {
                    break;
                }
                if skipped >= page.offset {
                    output.push(SqlRow { values, version: 0 }, &mut result)?;
                }
                skipped += 1;
                continue;
            }
            let mut work = 0;
            let keys = self
                .order
                .iter()
                .enumerate()
                .map(|(i, (s, _, _))| {
                    if query.query.distinct() {
                        Ok(values[self.order_slots[i]].clone())
                    } else {
                        s.eval(&row, params, &mut work)
                    }
                })
                .collect::<R<Vec<_>>>()?;
            let item = (SqlRow { values, version: 0 }, keys);
            if v2 {
                let bytes = keyed_row_bytes(&item);
                ordered.push(item, bytes, order_cmp, &mut *retention)?;
                continue;
            }
            if !query.query.distinct() {
                retention.charge(retained(&item.0.values) + retained(&item.1))?;
            }
            rows.push(item);
        }
        if v2 {
            // ORDER BY over all groups, then DISTINCT keeps each first occurrence.
            let sorted = ordered.finish(order_cmp, &mut scope)?;
            let rows: Box<dyn Iterator<Item = R<(SqlRow, Vec<Value>)>>> = if query.query.distinct() {
                Box::new(spill::unique_external(sorted, keyed_row_bytes,
                    |a: &(SqlRow, Vec<Value>), b: &(SqlRow, Vec<Value>)| cmp_values(&a.0.values, &b.0.values),
                    &mut scope)?)
            } else {
                Box::new(sorted)
            };
            for item in page.apply(rows) {
                output.push(item?.0, &mut result)?;
            }
            output.finish(&mut result, execution)?;
            if cancel() {
                return Err(SqlError::Aborted);
            }
            return Ok(result);
        }
        rows.sort_by(|(_, a), (_, b)| {
            for ((scalar, desc, first), (a, b)) in self.order.iter().zip(a.iter().zip(b)) {
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
                        let o = scalar.compare_values(a, b);
                        if *desc { o.reverse() } else { o }
                    }
                };
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            Ordering::Equal
        });
        for (row, _) in page.apply(rows.into_iter()) {
            output.push(row, &mut result)?;
        }
        output.finish(&mut result, execution)?;
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
        // Validate constant predicate errors before scanning, just like WHERE.
        for filter in self.aggregate_filters.iter().flatten() {
            filter.plan_constant(budget)?;
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
    pub(super) fn attach_clock(
        &mut self,
        clock: Option<&super::super::clock::ExecutionClock>,
        joins: &mut [BoundJoin],
    ) {
        for join in joins {
            join.clock = clock.cloned();
            join.on.attach_clock(clock);
        }
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
        for filter in self.aggregate_filters.iter_mut().flatten() {
            filter.attach_clock(clock);
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
            relations: Vec::new(),
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
                bound.execute(&state, &scan::LegacyRows(&state), &[], &mut || {
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
    fn filtered_aggregate_polls_each_input_and_leaves_source_unchanged() {
        let state = state(32);
        let before = bincode::serialize(&state).unwrap();
        for sql in [
            "SELECT count(*) FILTER (WHERE true) FROM group_cap",
            "SELECT count(*) FILTER (WHERE false) FROM group_cap",
            "SELECT id,count(DISTINCT id) FILTER (WHERE false) FROM group_cap GROUP BY id",
        ] {
            let query = query(sql);
            let bound = query.bind(&state, 0, &[]).unwrap();
            for fence in [2, 5, 20] {
                let mut polls = 0;
                assert_eq!(
                    bound.execute(&state, &scan::LegacyRows(&state), &[], &mut || {
                        polls += 1;
                        polls == fence
                    }),
                    Err(SqlError::Aborted)
                );
                assert_eq!(polls, fence);
                assert_eq!(bincode::serialize(&state).unwrap(), before);
            }
            assert!(
                bound
                    .execute(&state, &scan::LegacyRows(&state), &[], &mut || false)
                    .is_ok()
            );
        }
    }
    #[test]
    fn scan_join_candidate_cancellation_leaves_source_unchanged() {
        let mut state = state(0);
        for sql in [
            "CREATE TABLE group_scan(id bigint PRIMARY KEY,g bigint)",
            "INSERT INTO group_cap VALUES(0),(1),(2)",
            "INSERT INTO group_scan VALUES(11,0),(12,0),(13,0)",
        ] {
            let prepared = Prepared::parse(sql, 1, state.epoch).unwrap();
            state
                .execute_with_cancel(&prepared, &[], None, || false)
                .unwrap();
        }
        let before = bincode::serialize(&state).unwrap();
        let ordinary = query(
            "SELECT group_cap.id,group_scan.id FROM group_cap LEFT JOIN group_scan ON group_cap.id=group_scan.g",
        );
        let ordinary = ordinary.bind(&state, 0, &[]).unwrap();
        // Ordinary execution shares the same per-inner-candidate cancel fences.
        for fence in [3, 4, 5] {
            let mut polls = 0;
            assert_eq!(
                ordinary.execute(&state, &scan::LegacyRows(&state), &[], &mut || {
                    polls += 1;
                    polls == fence
                }),
                Err(SqlError::Aborted)
            );
            assert_eq!(polls, fence);
            assert_eq!(bincode::serialize(&state).unwrap(), before);
        }
        let query = query(
            "SELECT group_cap.id,count(group_scan.id) FROM group_cap LEFT JOIN group_scan ON group_cap.id=group_scan.g GROUP BY group_cap.id ORDER BY group_cap.id",
        );
        let bound = query.bind(&state, 0, &[]).unwrap();
        // Entry and the first left row consume two polls. The next three
        // fences occur inside the right scan, including non-final candidates.
        for fence in [3, 4, 5] {
            let mut polls = 0;
            assert_eq!(
                bound.execute(&state, &scan::LegacyRows(&state), &[], &mut || {
                    polls += 1;
                    polls == fence
                }),
                Err(SqlError::Aborted)
            );
            assert_eq!(polls, fence);
            assert_eq!(bincode::serialize(&state).unwrap(), before);
        }
        let result = bound
            .execute(&state, &scan::LegacyRows(&state), &[], &mut || false)
            .unwrap();
        assert_eq!(
            result
                .rows
                .iter()
                .map(|r| r.values.clone())
                .collect::<Vec<_>>(),
            vec![
                vec![Value::Int(0), Value::Int(3)],
                vec![Value::Int(1), Value::Int(0)],
                vec![Value::Int(2), Value::Int(0)],
            ]
        );
    }
    #[test]
    fn production_group_count_cap_precedes_paging() {
        // The public stored-row cap is lower; an in-memory source exercises the
        // physical group cap itself, without changing production constants.
        let mut state = state(scan::MAX_QUERY_ROWS + 1);
        let query = query("SELECT count(*) FROM group_cap GROUP BY id LIMIT 1");
        assert_eq!(
            query.bind(&state, 0, &[]).unwrap().execute(
                &state,
                &scan::LegacyRows(&state),
                &[],
                &mut || false
            ),
            Err(SqlError::Limit)
        );
        let table = &state.tables["group_cap"];
        let _ = state.rows.get_mut(&table.id).unwrap().pop_last();
        let result = query
            .bind(&state, 0, &[])
            .unwrap()
            .execute(&state, &scan::LegacyRows(&state), &[], &mut || false)
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values, vec![Value::Int(1)]);
    }
}
