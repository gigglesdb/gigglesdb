//! Bounded, unbound table reads. Names are exact identifiers, never SQL text.
use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SingleSourceRead {
    pub table: String,
    pub projection: Vec<ReadProjection>,
    pub filter: Option<ReadPredicate>,
}
/// Source of a typed INSERT ... SELECT.
#[derive(Debug, Clone)]
pub enum TypedRead { Single(Box<SingleSourceRead>), Join(Box<TwoSourceRead>) }
#[derive(Debug, Clone)]
pub struct TypedInsertSelect {
    pub table: String,
    pub columns: Vec<String>,
    pub source: TypedRead,
}
/// Typed query supplied to CREATE VIEW; SQL's supported view subset still applies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ViewRead { Single(Box<SingleSourceRead>), Two(Box<TwoSourceRead>) }
impl From<SingleSourceRead> for ViewRead {
    fn from(read: SingleSourceRead) -> Self { Self::Single(Box::new(read)) }
}
impl From<TwoSourceRead> for ViewRead {
    fn from(read: TwoSourceRead) -> Self { Self::Two(Box::new(read)) }
}
/// A table and its exact, explicit source alias.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadSource {
    pub table: String,
    pub alias: String,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadJoinKind { Inner, Left }
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TwoSourceRead {
    pub left: ReadSource,
    pub right: ReadSource,
    pub kind: ReadJoinKind,
    pub on: ReadPredicate,
    /// Additional GROUP BY expressions, including alias-qualified columns.
    pub group_by: Vec<ReadExpr>,
    pub projection: Vec<ReadProjection>,
    pub filter: Option<ReadPredicate>,
}
impl TwoSourceRead {
    pub fn with_clauses(mut self, clauses: ReadClauses) -> Self {
        self.filter = Some(ReadPredicate::Clauses {
            filter: self.filter.map(Box::new), clauses: Box::new(clauses),
        });
        self
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadProjection {
    pub expression: ReadExpr,
    pub alias: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ReadExpr {
    Column(String),
    QualifiedColumn { source: String, column: String },
    Literal(Value),
    Aggregate(ReadAggregate),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ReadAggregate {
    CountStar,
    /// Qualify the column of Count/Sum/Min/Max/Avg (never CountStar).
    Qualified { source: String, aggregate: Box<Self> },
    Count(String),
    Sum(String),
    Min(String),
    Max(String),
    Avg(String),
}
/// Optional clauses, attached once with either read type's `with_clauses`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadClauses {
    pub group_by: Vec<String>,
    pub having: Option<ReadPredicate>,
    pub order_by: Vec<ReadOrder>,
    pub limit: Option<i64>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadOrder {
    pub expression: ReadExpr,
    pub descending: bool,
    /// None uses SQL's default (NULLS FIRST for DESC, NULLS LAST for ASC).
    pub nulls_first: Option<bool>,
}
impl SingleSourceRead {
    /// Preserve the ordinary WHERE filter while attaching post-filter clauses.
    pub fn with_clauses(mut self, clauses: ReadClauses) -> Self {
        self.filter = Some(ReadPredicate::Clauses {
            filter: self.filter.map(Box::new), clauses: Box::new(clauses),
        });
        self
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadCompare {
    Eq, Ne, Lt, Le, Gt, Ge,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ReadPredicate {
    /// Root-only clause bundle; use the read type's `with_clauses`.
    Clauses { filter: Option<Box<Self>>, clauses: Box<ReadClauses> },
    In { expr: ReadExpr, subquery: Box<SingleSourceRead> },
    NotIn { expr: ReadExpr, subquery: Box<SingleSourceRead> },
    Exists(Box<SingleSourceRead>),
    NotExists(Box<SingleSourceRead>),
    Compare(ReadExpr, ReadCompare, ReadExpr),
    /// The boolean negates IS NULL (true means IS NOT NULL).
    IsNull(ReadExpr, bool),
    Not(Box<Self>),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
}
fn name(value: &str) -> R<()> {
    ident(value)?;
    if value.contains('.') {
        return Err(SqlError::Syntax);
    }
    Ok(())
}
// Qualified table names use the same internal key as SQL FROM sources.
fn table_name(value: &str) -> R<String> {
    match value.split_once('.') {
        Some((schema, table)) => {
            for component in [schema, table] {
                ident(component)?;
                if !component.as_bytes()[0].is_ascii_alphabetic()
                    || !component.bytes().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_') {
                    return Err(SqlError::Syntax);
                }
            }
            Ok(format!("{schema}\0{table}"))
        }
        None => { name(value)?; Ok(value.into()) }
    }
}
struct Budget {
    nodes: usize,
    bytes: usize,
}
impl Budget {
    fn aggregate(&mut self, value: ReadAggregate, depth: usize) -> R<(Aggregate, Option<Scalar>)> {
        self.add(0, depth)?;
        let (kind, column) = match value {
            ReadAggregate::Qualified { source, aggregate } => {
                self.add(source.len(), depth)?;
                name(&source)?;
                let (kind, argument) = self.aggregate(*aggregate, depth + 1)?;
                let Some(Scalar::Column(mut column)) = argument else { return Err(SqlError::Syntax); };
                if column.qualifier.is_some() { return Err(SqlError::Syntax); }
                column.qualifier = Some(source);
                return Ok((kind, Some(Scalar::Column(column))));
            }
            ReadAggregate::CountStar => return Ok((Aggregate::Count, None)),
            ReadAggregate::Count(c) => (Aggregate::Count, c),
            ReadAggregate::Sum(c) => (Aggregate::Sum, c),
            ReadAggregate::Min(c) => (Aggregate::Min, c),
            ReadAggregate::Max(c) => (Aggregate::Max, c),
            ReadAggregate::Avg(c) => (Aggregate::Avg, c),
        };
        Ok((kind, Some(self.expr(ReadExpr::Column(column), depth + 1)?)))
    }
    fn add(&mut self, bytes: usize, depth: usize) -> R<()> {
        self.nodes += 1;
        self.bytes = self.bytes.checked_add(bytes).ok_or(SqlError::Limit)?;
        if depth > 32 || self.nodes > MAX_WORK || self.bytes > MAX_SQL {
            return Err(SqlError::Limit);
        }
        Ok(())
    }
    fn expr(&mut self, value: ReadExpr, depth: usize) -> R<Scalar> {
        Ok(match value {
            ReadExpr::Aggregate(value) => {
                let (kind, argument) = self.aggregate(value, depth)?;
                // The SQL parser also lowers post-group COUNT(*) to COUNT(1).
                Scalar::Call(kind.name().into(), vec![argument.unwrap_or(Scalar::Literal(Value::Int(1)))])
            }
            ReadExpr::Column(column) => {
                self.add(column.len(), depth)?;
                name(&column)?;
                Scalar::Column(ColumnRef { qualifier: None, name: column })
            }
            ReadExpr::QualifiedColumn { source, column } => {
                self.add(source.len(), depth)?;
                self.add(column.len(), depth)?;
                name(&source)?;
                name(&column)?;
                Scalar::Column(ColumnRef { qualifier: Some(source), name: column })
            }
            ReadExpr::Literal(value) => {
                let bytes = match &value {
                    Value::Numeric(n) => n.display_byte_bound(),
                    _ => value_bytes(&value),
                };
                self.add(bytes, depth)?;
                Scalar::Literal(value)
            }
        })
    }
    fn predicate(&mut self, value: ReadPredicate, depth: usize) -> R<Filter> {
        self.add(0, depth)?;
        let negated = matches!(&value, ReadPredicate::NotIn { .. } | ReadPredicate::NotExists(_));
        Ok(match value {
            ReadPredicate::Clauses { .. } => return Err(SqlError::Syntax),
            ReadPredicate::In { expr, subquery } | ReadPredicate::NotIn { expr, subquery } => {
                Filter::InSubquery { value: self.expr(expr, depth + 1)?,
                    query: Box::new(lower_single(*subquery, self, depth + 1)?), negated }
            }
            ReadPredicate::Exists(subquery) | ReadPredicate::NotExists(subquery) => {
                Filter::Exists { query: Box::new(lower_single(*subquery, self, depth + 1)?), negated }
            }
            ReadPredicate::Compare(a, op, b) => {
                let op = match op {
                    ReadCompare::Eq => "=", ReadCompare::Ne => "<>",
                    ReadCompare::Lt => "<", ReadCompare::Le => "<=",
                    ReadCompare::Gt => ">", ReadCompare::Ge => ">=",
                };
                Filter::Compare(self.expr(a, depth + 1)?, op.into(), self.expr(b, depth + 1)?)
            }
            ReadPredicate::IsNull(a, negated) => Filter::Null(self.expr(a, depth + 1)?, negated),
            ReadPredicate::Not(a) => Filter::Not(Box::new(self.predicate(*a, depth + 1)?)),
            ReadPredicate::And(a, b) => Filter::Logic(
                Box::new(self.predicate(*a, depth + 1)?), true,
                Box::new(self.predicate(*b, depth + 1)?)),
            ReadPredicate::Or(a, b) => Filter::Logic(
                Box::new(self.predicate(*a, depth + 1)?), false,
                Box::new(self.predicate(*b, depth + 1)?)),
        })
    }
}
pub(super) fn lower(read: SingleSourceRead) -> R<Query> {
    lower_single(read, &mut Budget { nodes: 0, bytes: 0 }, 0)
}
fn lower_single(read: SingleSourceRead, budget: &mut Budget, depth: usize) -> R<Query> {
    lower_sources(budget, depth, vec![Source { alias: read.table.rsplit('.').next().unwrap().into(), name: read.table }],
        None, vec![], read.projection, read.filter)
}
/// `SELECT * FROM` one catalog relation key; native reads of views use it.
pub(in crate::relational) fn lower_relation(key: String) -> R<Query> {
    let (schema, alias) = split_key(&key);
    ident(schema)?;
    ident(alias)?;
    let alias = alias.to_owned();
    Ok(Query { explain: false, sources: vec![Source { name: key, alias }], joins: vec![],
        items: vec![(Item::Star, None)], predicates: vec![], filter: None, order: vec![], limit: None })
}
pub(in crate::relational) fn lower_join(read: TwoSourceRead) -> R<Query> {
    let sources = [read.left, read.right].into_iter().map(|s| {
        name(&s.alias)?;
        Ok(Source { name: s.table, alias: s.alias })
    }).collect::<R<Vec<_>>>()?;
    lower_sources(&mut Budget { nodes: 0, bytes: 0 }, 0, sources, Some((matches!(read.kind, ReadJoinKind::Left), read.on)),
        read.group_by, read.projection, read.filter)
}
fn lower_sources(budget: &mut Budget, depth: usize, mut sources: Vec<Source>, join: Option<(bool, ReadPredicate)>,
    group_by: Vec<ReadExpr>, projection: Vec<ReadProjection>, filter: Option<ReadPredicate>) -> R<Query> {
    budget.add(0, depth)?;
    for source in &mut sources {
        budget.add(source.name.len() + source.alias.len(), depth)?;
        source.name = table_name(&source.name)?;
    }
    let joins = join.map(|(left, on)| -> R<Join> {
        Ok(Join { left, on: budget.predicate(on, depth)? })
    }).transpose()?.into_iter().collect();
    if projection.is_empty() || projection.len() > MAX_COLUMNS {
        return Err(SqlError::Limit);
    }
    let items = projection.into_iter().map(|p| {
        if let Some(alias) = &p.alias {
            name(alias)?;
            budget.add(alias.len(), depth)?;
        }
        let item = match p.expression {
            ReadExpr::Aggregate(value) => {
                let (kind, argument) = budget.aggregate(value, depth)?;
                Item::Aggregate(kind, argument)
            }
            expression => Item::Scalar(budget.expr(expression, depth)?),
        };
        Ok((item, p.alias))
    }).collect::<R<Vec<_>>>()?;
    let (filter, clauses) = match filter {
        Some(ReadPredicate::Clauses { filter, clauses }) => (filter.map(|f| *f), *clauses),
        filter => (filter, ReadClauses::default()),
    };
    if group_by.len().saturating_add(clauses.group_by.len()) > MAX_COLUMNS || clauses.order_by.len() > MAX_COLUMNS {
        return Err(SqlError::Limit);
    }
    let filter = filter.map(|f| budget.predicate(f, depth)).transpose()?;
    let group = group_by.into_iter().chain(clauses.group_by.into_iter().map(ReadExpr::Column))
        .map(|expression| budget.expr(expression, depth)).collect::<R<Vec<_>>>()?;
    let having = clauses.having.map(|f| budget.predicate(f, depth)).transpose()?;
    let order = clauses.order_by.into_iter().map(|o| {
        Ok((budget.expr(o.expression, depth)?, o.descending, o.nulls_first.unwrap_or(o.descending)))
    }).collect::<R<Vec<_>>>()?;
    let limit = clauses.limit.map(|n| -> R<Expr> {
        budget.add(std::mem::size_of::<i64>(), depth)?;
        Ok(Expr::Literal(Value::Int(n)))
    }).transpose()?;
    let filter = if group.is_empty() && having.is_none() { filter } else {
        Some(Filter::Clauses(Box::new(QueryClauses { filter, group, having, ..QueryClauses::default() })))
    };
    Ok(Query { explain: false, sources, joins, items, predicates: vec![], filter, order, limit })
}
