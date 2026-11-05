//! Private syntax, binding and bounded physical execution. No persisted types.
use super::*;
use std::cmp::Ordering;

const MAX_WORK: usize = 4096;
const MAX_SOURCES: usize = 8;

#[derive(Debug, Clone)]
struct ColumnRef {
    qualifier: Option<String>,
    name: String,
}
#[derive(Debug, Clone)]
struct Source {
    name: String,
    alias: String,
}
#[derive(Debug, Clone, Copy)]
enum Aggregate {
    Count,
    Sum,
    Min,
    Max,
}
#[derive(Debug, Clone)]
enum Item {
    Column(ColumnRef),
    Aggregate(Aggregate, Option<ColumnRef>),
    Star,
}
#[derive(Debug, Clone)]
struct Join {
    left: bool,
    equalities: Vec<(ColumnRef, ColumnRef)>,
}
#[derive(Debug, Clone)]
pub(super) struct Query {
    pub(super) explain: bool,
    sources: Vec<Source>,
    joins: Vec<Join>,
    items: Vec<(Item, Option<String>)>,
    predicates: Vec<(ColumnRef, String, Expr)>,
    order: Vec<(ColumnRef, bool)>,
    limit: Option<Expr>,
}

impl Parser {
    fn column_ref(&mut self) -> R<ColumnRef> {
        let first = self.name()?;
        if self.sym('.') {
            Ok(ColumnRef {
                qualifier: Some(first),
                name: self.name()?,
            })
        } else {
            Ok(ColumnRef {
                qualifier: None,
                name: first,
            })
        }
    }
    fn source(&mut self) -> R<Source> {
        let name = self.name()?;
        let alias = if self.word("as")
            || matches!(self.tokens.get(self.at), Some(Token::Word(w)) if !["inner", "left", "join", "where", "order", "limit", "group", "having", "on", "right", "full", "cross"].contains(&w.as_str()))
        {
            self.name()?
        } else {
            name.clone()
        };
        Ok(Source { name, alias })
    }
}

impl Query {
    pub(super) fn parse(p: &mut Parser) -> R<Self> {
        let explain = p.word("explain");
        p.need("select")?;
        let mut items = Vec::new();
        loop {
            let item = if p.sym('*') {
                Item::Star
            } else {
                let aggregate = match p.tokens.get(p.at) {
                    Some(Token::Word(w)) if p.tokens.get(p.at + 1) == Some(&Token::Symbol('(')) => {
                        match w.as_str() {
                            "count" => Some(Aggregate::Count),
                            "sum" => Some(Aggregate::Sum),
                            "min" => Some(Aggregate::Min),
                            "max" => Some(Aggregate::Max),
                            _ => None,
                        }
                    }
                    _ => None,
                };
                if let Some(a) = aggregate {
                    p.at += 1;
                    p.punct('(')?;
                    let c = if p.sym('*') {
                        if !matches!(a, Aggregate::Count) {
                            return Err(SqlError::Unsupported);
                        }
                        None
                    } else {
                        Some(p.column_ref()?)
                    };
                    p.punct(')')?;
                    Item::Aggregate(a, c)
                } else {
                    Item::Column(p.column_ref()?)
                }
            };
            let alias = if p.word("as") { Some(p.name()?) } else { None };
            items.push((item, alias));
            if items.len() > MAX_COLUMNS {
                return Err(SqlError::Limit);
            }
            if !p.sym(',') {
                break;
            }
        }
        p.need("from")?;
        let mut sources = vec![p.source()?];
        let mut joins = vec![];
        loop {
            let left = if p.word("left") {
                p.word("outer");
                true
            } else if p.word("inner")
                || matches!(p.tokens.get(p.at), Some(Token::Word(w)) if w == "join")
            {
                false
            } else {
                break;
            };
            p.need("join")?;
            sources.push(p.source()?);
            if sources.len() > MAX_SOURCES {
                return Err(SqlError::Limit);
            }
            p.need("on")?;
            let mut equalities = vec![];
            loop {
                let a = p.column_ref()?;
                if p.tokens.get(p.at) != Some(&Token::Op("=".into())) {
                    return Err(SqlError::Unsupported);
                }
                p.at += 1;
                equalities.push((a, p.column_ref()?));
                if equalities.len() > MAX_COLUMNS {
                    return Err(SqlError::Limit);
                }
                if !p.word("and") {
                    break;
                }
            }
            joins.push(Join { left, equalities });
        }
        let mut predicates = vec![];
        if p.word("where") {
            loop {
                let c = p.column_ref()?;
                let Some(Token::Op(op)) = p.tokens.get(p.at).cloned() else {
                    return Err(SqlError::Syntax);
                };
                p.at += 1;
                if !["=", "<", "<=", ">", ">="].contains(&op.as_str()) {
                    return Err(SqlError::Unsupported);
                }
                predicates.push((c, op, p.expr()?));
                if !p.word("and") {
                    break;
                }
            }
        }
        let mut order = vec![];
        if p.word("order") {
            p.need("by")?;
            loop {
                let c = p.column_ref()?;
                let desc = p.word("desc");
                if !desc {
                    p.word("asc");
                }
                order.push((c, desc));
                if !p.sym(',') {
                    break;
                }
            }
        }
        let limit = if p.word("limit") {
            Some(p.expr()?)
        } else {
            None
        };
        p.sym(';');
        if p.at != p.tokens.len() {
            return Err(SqlError::Unsupported);
        }
        Ok(Self {
            explain,
            sources,
            joins,
            items,
            predicates,
            order,
            limit,
        })
    }
    pub(super) fn target(&self) -> &str {
        if self.sources.iter().all(|s| s.name == self.sources[0].name) {
            &self.sources[0].name
        } else {
            // Existing table-limited gateways must not authorize only the first source.
            "giggles_join"
        }
    }
    pub(super) fn bind<'a>(&'a self, state: &'a Relational, count: usize) -> R<BoundQuery<'a>> {
        unique(self.sources.iter().map(|s| &s.alias))?;
        let tables = self
            .sources
            .iter()
            .map(|s| state.tables.get(&s.name).ok_or(SqlError::UnknownTable))
            .collect::<R<Vec<_>>>()?;
        let resolve = |c: &ColumnRef, scope: usize| -> R<BoundColumn> {
            let mut found = None;
            for (source, (s, t)) in self.sources.iter().zip(&tables).take(scope).enumerate() {
                if c.qualifier.as_ref().is_some_and(|q| q != &s.alias) {
                    continue;
                }
                if let Some(column) = t.columns.iter().position(|v| v.name == c.name) {
                    if found.is_some() {
                        return Err(SqlError::Duplicate);
                    }
                    found = Some(BoundColumn { source, column });
                }
            }
            found.ok_or(SqlError::UnknownColumn)
        };
        let mut joins = vec![];
        for (j, join) in self.joins.iter().enumerate() {
            let right = j + 1;
            let mut keys = BTreeMap::new();
            let mut equalities = vec![];
            for (a, b) in &join.equalities {
                let a = resolve(a, right + 1)?;
                let b = resolve(b, right + 1)?;
                let (outer, inner) = if a.source == right && b.source < right {
                    (b, a)
                } else if b.source == right && a.source < right {
                    (a, b)
                } else {
                    return Err(SqlError::Unsupported);
                };
                if tables[outer.source].columns[outer.column].data_type
                    != tables[inner.source].columns[inner.column].data_type
                {
                    return Err(SqlError::Type);
                }
                if tables[right].primary_key.contains(&inner.column)
                    && keys.insert(inner.column, outer).is_some()
                {
                    return Err(SqlError::Duplicate);
                }
                equalities.push((outer, inner));
            }
            let key = tables[right]
                .primary_key
                .iter()
                .map(|i| keys.get(i).copied().ok_or(SqlError::Unbounded))
                .collect::<R<Vec<_>>>()?;
            joins.push(BoundJoin {
                left: join.left,
                key,
                equalities,
            });
        }
        let mut items = vec![];
        let mut columns = vec![];
        let mut aliases = BTreeSet::new();
        for (item, alias) in &self.items {
            if let Some(a) = alias
                && !aliases.insert(a)
            {
                return Err(SqlError::Duplicate);
            }
            let mut push = |item: BoundItem, mut column: ColumnDef| {
                if let Some(a) = alias {
                    column.name = a.clone();
                }
                items.push(item);
                columns.push(column);
            };
            match item {
                Item::Column(c) => {
                    let c = resolve(c, tables.len())?;
                    let mut d = tables[c.source].columns[c.column].clone();
                    if c.source > 0 && self.joins[c.source - 1].left {
                        d.not_null = false;
                    }
                    push(BoundItem::Column(c), d);
                }
                Item::Aggregate(a, c) => {
                    let c = c.as_ref().map(|c| resolve(c, tables.len())).transpose()?;
                    let ty = c.map(|c| tables[c.source].columns[c.column].data_type.clone());
                    if matches!(a, Aggregate::Sum)
                        && !matches!(
                            ty,
                            Some(DataType::SmallInt | DataType::Integer | DataType::BigInt)
                        )
                    {
                        return Err(SqlError::Type);
                    }
                    let data_type = if matches!(a, Aggregate::Count | Aggregate::Sum) {
                        DataType::BigInt
                    } else {
                        ty.ok_or(SqlError::Type)?
                    };
                    push(
                        BoundItem::Aggregate(*a, c),
                        ColumnDef {
                            id: 0,
                            name: match a {
                                Aggregate::Count => "count",
                                Aggregate::Sum => "sum",
                                Aggregate::Min => "min",
                                Aggregate::Max => "max",
                            }
                            .into(),
                            data_type,
                            not_null: matches!(a, Aggregate::Count),
                        },
                    );
                }
                Item::Star => {
                    if alias.is_some() {
                        return Err(SqlError::Unsupported);
                    }
                    for (source, t) in tables.iter().enumerate() {
                        for (column, d) in t.columns.iter().enumerate() {
                            let mut d = d.clone();
                            if source > 0 && self.joins[source - 1].left {
                                d.not_null = false;
                            }
                            push(BoundItem::Column(BoundColumn { source, column }), d);
                        }
                    }
                }
            }
        }
        if columns.len() > MAX_COLUMNS {
            return Err(SqlError::Limit);
        }
        let aggregate = items.iter().any(|i| matches!(i, BoundItem::Aggregate(..)));
        if aggregate
            && (items.iter().any(|i| matches!(i, BoundItem::Column(_))) || !self.order.is_empty())
        {
            return Err(SqlError::Unsupported);
        }
        let mut parameters = vec![None; count];
        let mut bind_expr = |e: &Expr, ty: &DataType| -> R<()> {
            if let Expr::Param(n) = e {
                let slot = parameters.get_mut(n - 1).ok_or(SqlError::Parameters)?;
                if slot.as_ref().is_some_and(|old| old != ty) {
                    return Err(SqlError::Type);
                }
                *slot = Some(ty.clone());
            } else if let Expr::Literal(v) = e {
                check(
                    v,
                    &ColumnDef {
                        id: 0,
                        name: String::new(),
                        data_type: ty.clone(),
                        not_null: false,
                    },
                )?;
            }
            Ok(())
        };
        let mut predicates = vec![];
        for (c, op, e) in &self.predicates {
            let c = resolve(c, tables.len())?;
            bind_expr(e, &tables[c.source].columns[c.column].data_type)?;
            predicates.push((c, op.as_str(), e));
        }
        if let Some(e) = &self.limit {
            bind_expr(e, &DataType::BigInt)?;
        }
        let parameters = parameters
            .into_iter()
            .map(|p| p.ok_or(SqlError::Parameters))
            .collect::<R<Vec<_>>>()?;
        let order = self
            .order
            .iter()
            .map(|(c, d)| Ok((resolve(c, tables.len())?, *d)))
            .collect::<R<Vec<_>>>()?;
        Ok(BoundQuery {
            query: self,
            tables,
            joins,
            items,
            columns,
            parameters,
            predicates,
            order,
            aggregate,
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct BoundColumn {
    source: usize,
    column: usize,
}
enum BoundItem {
    Column(BoundColumn),
    Aggregate(Aggregate, Option<BoundColumn>),
}
struct BoundJoin {
    left: bool,
    key: Vec<BoundColumn>,
    equalities: Vec<(BoundColumn, BoundColumn)>,
}
pub(super) struct BoundQuery<'a> {
    query: &'a Query,
    tables: Vec<&'a TableDef>,
    joins: Vec<BoundJoin>,
    items: Vec<BoundItem>,
    columns: Vec<ColumnDef>,
    parameters: Vec<DataType>,
    predicates: Vec<(BoundColumn, &'a str, &'a Expr)>,
    order: Vec<(BoundColumn, bool)>,
    aggregate: bool,
}
struct Tuple {
    values: Vec<Vec<Value>>,
    version: u64,
}
impl Tuple {
    fn get(&self, c: BoundColumn) -> &Value {
        &self.values[c.source][c.column]
    }
}
fn cmp(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Int(a), Value::Int(b)) => a.cmp(b),
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Text(a), Value::Text(b)) => a.as_bytes().cmp(b.as_bytes()),
        (Value::Bytes(a), Value::Bytes(b)) => a.cmp(b),
        _ => unreachable!("binder enforces scalar types"),
    }
}
fn equal(a: &Value, b: &Value) -> bool {
    *a != Value::Null && *b != Value::Null && a == b
}
fn charge(bytes: &mut usize, value: &Value) -> R<()> {
    *bytes = bytes
        .checked_add(value_bytes(value))
        .ok_or(SqlError::Limit)?;
    if *bytes > MAX_RESULT_BYTES {
        return Err(SqlError::Limit);
    }
    Ok(())
}
fn fits(rows: &[Tuple]) -> R<()> {
    if rows.len() > MAX_ROWS {
        return Err(SqlError::Limit);
    }
    let bytes = rows
        .iter()
        .flat_map(|r| &r.values)
        .flatten()
        .try_fold(0usize, |n, v| {
            n.checked_add(value_bytes(v)).ok_or(SqlError::Limit)
        })?;
    if bytes > MAX_RESULT_BYTES {
        return Err(SqlError::Limit);
    }
    Ok(())
}
impl BoundQuery<'_> {
    pub(super) fn description(&self) -> Description {
        Description {
            parameters: self.parameters.clone(),
            columns: if self.query.explain {
                vec![ColumnDef {
                    id: 0,
                    name: "plan".into(),
                    data_type: DataType::Text,
                    not_null: true,
                }]
            } else {
                self.columns.clone()
            },
        }
    }
    pub(super) fn execute(&self, state: &Relational, params: &[Value]) -> R<SqlResult> {
        for (v, ty) in params.iter().zip(&self.parameters) {
            check(
                v,
                &ColumnDef {
                    id: 0,
                    name: String::new(),
                    data_type: ty.clone(),
                    not_null: false,
                },
            )?;
        }
        let limit = match &self.query.limit {
            Some(e) => match e.value(params)? {
                Value::Int(n) if (0..=MAX_ROWS as i64).contains(&n) => n as usize,
                _ => return Err(SqlError::Limit),
            },
            None => MAX_ROWS,
        };
        let empty_input = self
            .predicates
            .iter()
            .any(|(_, _, e)| matches!(e.value(params), Ok(Value::Null)));
        let base_predicates = self
            .predicates
            .iter()
            .filter(|(c, _, _)| {
                !empty_input && c.source == 0 && self.tables[0].primary_key.contains(&c.column)
            })
            .map(|(c, op, e)| Predicate {
                column: self.tables[0].columns[c.column].name.clone(),
                op: (*op).into(),
                value: (*e).clone(),
            })
            .collect::<Vec<_>>();
        let bounds = bounds(self.tables[0], &base_predicates, params)?;
        if self.query.explain {
            let mut plan = format!(
                "{}; candidate_work<={MAX_WORK}; intermediate_rows<={MAX_ROWS}; intermediate_bytes<={MAX_RESULT_BYTES}; result_bytes<={MAX_RESULT_BYTES}",
                if empty_input {
                    "EmptyInput"
                } else if bounds.point {
                    "PrimaryKeyLookup"
                } else {
                    "PrimaryKeyRangeScan"
                }
            );
            for j in &self.joins {
                plan.push_str(if j.left {
                    "; LeftPrimaryKeyNestedLoop"
                } else {
                    "; InnerPrimaryKeyNestedLoop"
                });
            }
            if self.aggregate {
                plan.push_str("; GlobalAggregate");
            }
            if !self.order.is_empty() {
                plan.push_str("; BoundedStableSort");
            }
            plan.push_str("; OutputLimit");
            return Ok(SqlResult {
                columns: vec!["plan".into()],
                rows: vec![SqlRow {
                    values: vec![Value::Text(plan)],
                    version: 0,
                }],
                affected: 0,
            });
        }
        let mut work = 0usize;
        let mut rows = vec![];
        if !empty_input && (bounds.low < bounds.high || (bounds.low == bounds.high && bounds.point))
        {
            use std::ops::Bound::{Excluded, Included};
            let lo = if bounds.low_inclusive {
                Included(bounds.low)
            } else {
                Excluded(bounds.low)
            };
            let hi = if bounds.high_inclusive {
                Included(bounds.high)
            } else {
                Excluded(bounds.high)
            };
            for (_, r) in state.rows[&self.tables[0].id].range((lo, hi)) {
                work += 1;
                if work > MAX_WORK {
                    return Err(SqlError::Limit);
                }
                rows.push(Tuple {
                    values: vec![r.values.clone()],
                    version: r.version,
                });
                fits(&rows)?;
            }
        }
        for (i, j) in self.joins.iter().enumerate() {
            let table = self.tables[i + 1];
            let mut next = vec![];
            for mut row in rows {
                work += 1;
                if work > MAX_WORK {
                    return Err(SqlError::Limit);
                }
                let key = j
                    .key
                    .iter()
                    .map(|c| row.get(*c).clone())
                    .collect::<Vec<_>>();
                let found = if key.contains(&Value::Null) {
                    None
                } else {
                    state.rows[&table.id].get(&encode_key(&key)?)
                };
                let matched = found.filter(|r| {
                    j.equalities
                        .iter()
                        .all(|(a, b)| equal(row.get(*a), &r.values[b.column]))
                });
                match matched {
                    Some(r) => row.values.push(r.values.clone()),
                    None if j.left => row.values.push(vec![Value::Null; table.columns.len()]),
                    None => continue,
                }
                row.version = 0;
                next.push(row);
                fits(&next)?;
            }
            rows = next;
        }
        let predicates = self
            .predicates
            .iter()
            .map(|(c, op, e)| Ok((*c, *op, e.value(params)?)))
            .collect::<R<Vec<_>>>()?;
        rows.retain(|r| {
            predicates.iter().all(|(c, op, v)| {
                let a = r.get(*c);
                if *a == Value::Null || *v == Value::Null {
                    return false;
                }
                let ord = cmp(a, v);
                match *op {
                    "=" => ord == Ordering::Equal,
                    "<" => ord == Ordering::Less,
                    "<=" => ord != Ordering::Greater,
                    ">" => ord == Ordering::Greater,
                    ">=" => ord != Ordering::Less,
                    _ => false,
                }
            })
        });
        rows.sort_by(|a, b| {
            for (c, desc) in &self.order {
                let ord = cmp(a.get(*c), b.get(*c));
                if ord != Ordering::Equal {
                    return if *desc { ord.reverse() } else { ord };
                }
            }
            Ordering::Equal
        });
        let mut result = SqlResult {
            columns: self.columns.iter().map(|c| c.name.clone()).collect(),
            rows: vec![],
            affected: 0,
        };
        let mut result_bytes = 0usize;
        if self.aggregate {
            let values = self
                .items
                .iter()
                .map(|item| {
                    let BoundItem::Aggregate(a, c) = item else {
                        unreachable!()
                    };
                    let mut value = if matches!(a, Aggregate::Count) {
                        Value::Int(0)
                    } else {
                        Value::Null
                    };
                    for row in &rows {
                        let v = c.map(|c| row.get(c));
                        if v == Some(&Value::Null) {
                            continue;
                        }
                        match a {
                            Aggregate::Count => {
                                if let Value::Int(n) = &mut value {
                                    *n = n.checked_add(1).ok_or(SqlError::Overflow)?;
                                }
                            }
                            Aggregate::Sum => {
                                let Some(Value::Int(n)) = v else {
                                    return Err(SqlError::Type);
                                };
                                value = Value::Int(match value {
                                    Value::Int(old) => {
                                        old.checked_add(*n).ok_or(SqlError::Overflow)?
                                    }
                                    _ => *n,
                                });
                            }
                            Aggregate::Min | Aggregate::Max => {
                                let v = v.ok_or(SqlError::Type)?;
                                if value == Value::Null
                                    || (matches!(a, Aggregate::Min)
                                        && cmp(v, &value) == Ordering::Less)
                                    || (matches!(a, Aggregate::Max)
                                        && cmp(v, &value) == Ordering::Greater)
                                {
                                    value = v.clone();
                                }
                            }
                        }
                    }
                    charge(&mut result_bytes, &value)?;
                    Ok(value)
                })
                .collect::<R<Vec<_>>>()?;
            result.rows.push(SqlRow { values, version: 0 });
        } else {
            for row in rows {
                let values = self
                    .items
                    .iter()
                    .map(|i| {
                        let BoundItem::Column(c) = i else {
                            unreachable!()
                        };
                        let value = row.get(*c);
                        // Reserve raw output bytes before cloning repeated projections.
                        charge(&mut result_bytes, value)?;
                        Ok(value.clone())
                    })
                    .collect::<R<Vec<_>>>()?;
                result.rows.push(SqlRow {
                    values,
                    version: row.version,
                });
            }
        }
        result.rows.truncate(limit);
        Ok(result)
    }
}
