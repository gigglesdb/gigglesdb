//! Bounded, unbound table reads. Names are exact identifiers, never SQL text.
use super::*;

#[derive(Debug, Clone)]
pub struct SingleSourceRead {
    pub table: String,
    pub projection: Vec<ReadProjection>,
    pub filter: Option<ReadPredicate>,
}
/// A table and its exact, explicit source alias.
#[derive(Debug, Clone)]
pub struct ReadSource {
    pub table: String,
    pub alias: String,
}
#[derive(Debug, Clone, Copy)]
pub enum ReadJoinKind { Inner, Left }
#[derive(Debug, Clone)]
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
#[derive(Debug, Clone)]
pub struct ReadProjection {
    pub expression: ReadExpr,
    pub alias: Option<String>,
}
#[derive(Debug, Clone)]
pub enum ReadExpr {
    Column(String),
    QualifiedColumn { source: String, column: String },
    Literal(Value),
    Aggregate(ReadAggregate),
}
#[derive(Debug, Clone)]
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
#[derive(Debug, Clone, Default)]
pub struct ReadClauses {
    pub group_by: Vec<String>,
    pub having: Option<ReadPredicate>,
    pub order_by: Vec<ReadOrder>,
    pub limit: Option<i64>,
}
#[derive(Debug, Clone)]
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
#[derive(Debug, Clone, Copy)]
pub enum ReadCompare {
    Eq, Ne, Lt, Le, Gt, Ge,
}
#[derive(Debug, Clone)]
pub enum ReadPredicate {
    /// Root-only clause bundle; use the read type's `with_clauses`.
    Clauses { filter: Option<Box<Self>>, clauses: Box<ReadClauses> },
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
        Ok(match value {
            ReadPredicate::Clauses { .. } => return Err(SqlError::Syntax),
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
    lower_sources(vec![Source { alias: read.table.clone(), name: read.table }],
        None, vec![], read.projection, read.filter)
}
pub(in crate::relational) fn lower_join(read: TwoSourceRead) -> R<Query> {
    let sources = [read.left, read.right].into_iter().map(|s| {
        name(&s.alias)?;
        Ok(Source { name: s.table, alias: s.alias })
    }).collect::<R<Vec<_>>>()?;
    lower_sources(sources, Some((matches!(read.kind, ReadJoinKind::Left), read.on)),
        read.group_by, read.projection, read.filter)
}
fn lower_sources(sources: Vec<Source>, join: Option<(bool, ReadPredicate)>,
    group_by: Vec<ReadExpr>, projection: Vec<ReadProjection>, filter: Option<ReadPredicate>) -> R<Query> {
    let mut budget = Budget { nodes: 0, bytes: 0 };
    for source in &sources {
        name(&source.name)?;
        budget.add(source.name.len() + source.alias.len(), 0)?;
    }
    let joins = join.map(|(left, on)| -> R<Join> {
        Ok(Join { left, on: budget.predicate(on, 0)? })
    }).transpose()?.into_iter().collect();
    if projection.is_empty() || projection.len() > MAX_COLUMNS {
        return Err(SqlError::Limit);
    }
    let items = projection.into_iter().map(|p| {
        if let Some(alias) = &p.alias {
            name(alias)?;
            budget.add(alias.len(), 0)?;
        }
        let item = match p.expression {
            ReadExpr::Aggregate(value) => {
                let (kind, argument) = budget.aggregate(value, 0)?;
                Item::Aggregate(kind, argument)
            }
            expression => Item::Scalar(budget.expr(expression, 0)?),
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
    let filter = filter.map(|f| budget.predicate(f, 0)).transpose()?;
    let group = group_by.into_iter().chain(clauses.group_by.into_iter().map(ReadExpr::Column))
        .map(|expression| budget.expr(expression, 0)).collect::<R<Vec<_>>>()?;
    let having = clauses.having.map(|f| budget.predicate(f, 0)).transpose()?;
    let order = clauses.order_by.into_iter().map(|o| {
        Ok((budget.expr(o.expression, 0)?, o.descending, o.nulls_first.unwrap_or(o.descending)))
    }).collect::<R<Vec<_>>>()?;
    let limit = clauses.limit.map(|n| -> R<Expr> {
        budget.add(std::mem::size_of::<i64>(), 0)?;
        Ok(Expr::Literal(Value::Int(n)))
    }).transpose()?;
    let filter = if group.is_empty() && having.is_none() { filter } else {
        Some(Filter::Clauses(Box::new(QueryClauses { filter, group, having, ..QueryClauses::default() })))
    };
    Ok(Query { explain: false, sources, joins, items, predicates: vec![], filter, order, limit })
}
