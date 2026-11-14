//! Bounded, unbound leaf reads. Names are exact identifiers, never SQL text.
use super::*;

#[derive(Debug, Clone)]
pub struct SingleSourceRead {
    pub table: String,
    pub projection: Vec<ReadProjection>,
    pub filter: Option<ReadPredicate>,
}
#[derive(Debug, Clone)]
pub struct ReadProjection {
    pub expression: ReadExpr,
    pub alias: Option<String>,
}
#[derive(Debug, Clone)]
pub enum ReadExpr {
    Column(String),
    Literal(Value),
}
#[derive(Debug, Clone, Copy)]
pub enum ReadCompare {
    Eq, Ne, Lt, Le, Gt, Ge,
}
#[derive(Debug, Clone)]
pub enum ReadPredicate {
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
            ReadExpr::Column(column) => {
                self.add(column.len(), depth)?;
                name(&column)?;
                Scalar::Column(ColumnRef { qualifier: None, name: column })
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
    name(&read.table)?;
    if read.projection.is_empty() || read.projection.len() > MAX_COLUMNS {
        return Err(SqlError::Limit);
    }
    let mut budget = Budget { nodes: 1, bytes: read.table.len() };
    let items = read.projection.into_iter().map(|p| {
        if let Some(alias) = &p.alias {
            name(alias)?;
            budget.add(alias.len(), 0)?;
        }
        Ok((Item::Scalar(budget.expr(p.expression, 0)?), p.alias))
    }).collect::<R<Vec<_>>>()?;
    let filter = read.filter.map(|f| budget.predicate(f, 0)).transpose()?;
    Ok(Query { explain: false, sources: vec![Source { alias: read.table.clone(), name: read.table }],
        joins: vec![], items, predicates: vec![], filter, order: vec![], limit: None })
}
