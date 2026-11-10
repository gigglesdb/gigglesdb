//! Private syntax, binding and bounded physical execution. No persisted types.
use super::*;
use std::cell::{Cell, RefCell};
use std::cmp::Ordering;
mod aggregate_filter;
mod functions;
pub(super) use functions::assignment;
pub(super) mod conflict;
mod grouping;
mod insert_values;
mod schema_names;
pub(super) use functions::comparison_requires_filter;
use functions::{BoundScalar, Scalar};
pub(super) use insert_values::InsertExpressions;

pub(super) enum TypmodSource<'a> {
    Column(&'a ColumnDef),
    Scalar(&'a functions::BoundScalar),
    Other,
}

pub(super) fn result_typmod(state: &Relational, source: TypmodSource<'_>) -> i32 {
    match source {
        TypmodSource::Column(column) => {
            if column.id == 0 {
                -1
            } else {
                state.column_typmod(column.id)
            }
        }
        TypmodSource::Scalar(value) => value.clock_typmod(),
        TypmodSource::Other => -1,
    }
}

// Syntax node budget for INSERT expressions and IN/BETWEEN lowering; not a scan work cap.
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
    Avg,
    CountDistinct,
    SumDistinct,
    AvgDistinct,
    MinDistinct,
    MaxDistinct,
    Min,
    Max,
}
impl Aggregate {
    fn base(self) -> Self {
        match self {
            Self::CountDistinct => Self::Count,
            Self::SumDistinct => Self::Sum,
            Self::AvgDistinct => Self::Avg,
            Self::MinDistinct => Self::Min,
            Self::MaxDistinct => Self::Max,
            a => a,
        }
    }
    fn distinct(self) -> bool {
        matches!(
            self,
            Self::CountDistinct
                | Self::SumDistinct
                | Self::AvgDistinct
                | Self::MinDistinct
                | Self::MaxDistinct
        )
    }
    fn with_distinct(self) -> Self {
        match self.base() {
            Self::Count => Self::CountDistinct,
            Self::Sum => Self::SumDistinct,
            Self::Avg => Self::AvgDistinct,
            Self::Min => Self::MinDistinct,
            Self::Max => Self::MaxDistinct,
            _ => unreachable!(),
        }
    }
    fn name(self) -> &'static str {
        match self.base() {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Avg => "avg",
            Self::Min => "min",
            Self::Max => "max",
            _ => unreachable!(),
        }
    }
    fn call_name(self) -> String {
        if self.distinct() {
            format!("\0aggregate_distinct_{}", self.name())
        } else {
            self.name().into()
        }
    }
    fn from_name(name: &str) -> Option<Self> {
        let name = name
            .strip_prefix("public.\0")
            .filter(|name| name.starts_with("\0aggregate_distinct_"))
            .unwrap_or(name);
        let distinct = name.starts_with("\0aggregate_distinct_");
        let name = name.strip_prefix("\0aggregate_distinct_").unwrap_or(name);
        let a = match name {
            "count" => Self::Count,
            "sum" => Self::Sum,
            "avg" => Self::Avg,
            "min" => Self::Min,
            "max" => Self::Max,
            _ => return None,
        };
        Some(if distinct { a.with_distinct() } else { a })
    }
}
#[derive(Debug, Clone)]
enum Item {
    Aggregate(Aggregate, Option<Scalar>),
    Scalar(Scalar),
    Star,
}
#[derive(Debug, Clone)]
struct Join {
    left: bool,
    on: Filter,
}
impl Join {
    // Only equalities required by an AND chain may narrow the candidate set.
    fn equalities(&self) -> Vec<(&ColumnRef, &ColumnRef)> {
        fn collect<'a>(f: &'a Filter, out: &mut Vec<(&'a ColumnRef, &'a ColumnRef)>) {
            match f {
                Filter::Compare(Scalar::Column(a), op, Scalar::Column(b)) if op == "=" => {
                    out.push((a, b))
                }
                Filter::Logic(a, true, b) => {
                    collect(a, out);
                    collect(b, out);
                }
                _ => {}
            }
        }
        let mut out = vec![];
        collect(&self.on, &mut out);
        out
    }
}
#[derive(Debug, Clone)]
pub(super) struct Query {
    pub(super) explain: bool,
    sources: Vec<Source>,
    joins: Vec<Join>,
    items: Vec<(Item, Option<String>)>,
    predicates: Vec<(ColumnRef, String, Expr)>,
    filter: Option<Filter>,
    order: Vec<(Scalar, bool, bool)>,
    limit: Option<Expr>,
}

#[derive(Debug, Clone)]
struct QueryClauses {
    distinct: bool,
    filter: Option<Filter>,
    group: Vec<Scalar>,
    having: Option<Filter>,
    offset: Option<Expr>,
}
#[derive(Debug, Clone)]
enum Filter {
    // Root-only SELECT clause bundle keeps legacy Query constructors stable.
    Clauses(Box<QueryClauses>),
    Scalar(Scalar),
    Compare(Scalar, String, Scalar),
    InSubquery {
        value: Scalar,
        query: Box<Query>,
        negated: bool,
    },
    Null(Scalar, bool),
    Is(Box<Filter>, Option<bool>, bool),
    Like {
        value: Scalar,
        pattern: Scalar,
        escape: Option<Scalar>,
        insensitive: bool,
        negated: bool,
    },
    Not(Box<Filter>),
    Logic(Box<Filter>, bool, Box<Filter>),
}
enum BoundFilter {
    Scalar(BoundScalar),
    Compare(BoundScalar, String, BoundScalar),
    InSubquery {
        value: BoundScalar,
        query: usize,
        column_type: DataType,
        negated: bool,
        clock: Option<super::clock::ExecutionClock>,
    },
    Null(BoundScalar, bool),
    Is(Box<BoundFilter>, Option<bool>, bool),
    Like {
        value: BoundScalar,
        pattern: BoundScalar,
        escape: Option<BoundScalar>,
        insensitive: bool,
        negated: bool,
    },
    Not(Box<BoundFilter>),
    Logic(Box<BoundFilter>, bool, Box<BoundFilter>),
}
/// Lowering IN/BETWEEN clones the lhs once per comparison. Admit the total
/// cloned Scalar nodes against MAX_WORK and the cloned variable payload
/// (literal values, column/qualifier and routine names) against MAX_SQL,
/// measured on the borrowed AST, before anything is cloned.
fn admit_expansion(lhs: &Scalar, copies: usize) -> R<()> {
    fn filter_cost(f: &Filter, nodes: &mut usize, bytes: &mut usize) -> R<()> {
        *nodes = nodes.checked_add(1).ok_or(SqlError::Limit)?;
        match f {
            Filter::Clauses(_) | Filter::InSubquery { .. } => return Err(SqlError::Unsupported),
            Filter::Scalar(s) | Filter::Null(s, _) => cost(s, nodes, bytes)?,
            Filter::Compare(a, op, b) => {
                *bytes = bytes.checked_add(op.len()).ok_or(SqlError::Limit)?;
                cost(a, nodes, bytes)?;
                cost(b, nodes, bytes)?;
            }
            Filter::Like {
                value,
                pattern,
                escape,
                ..
            } => {
                cost(value, nodes, bytes)?;
                cost(pattern, nodes, bytes)?;
                if let Some(s) = escape {
                    cost(s, nodes, bytes)?;
                }
            }
            Filter::Not(f) | Filter::Is(f, _, _) => filter_cost(f, nodes, bytes)?,
            Filter::Logic(a, _, b) => {
                filter_cost(a, nodes, bytes)?;
                filter_cost(b, nodes, bytes)?;
            }
        }
        Ok(())
    }
    fn cost(s: &Scalar, nodes: &mut usize, bytes: &mut usize) -> R<()> {
        *nodes = nodes.checked_add(1).ok_or(SqlError::Limit)?;
        let payload = match s {
            Scalar::Param(_) => 0,
            Scalar::Literal(v) => value_bytes(v),
            Scalar::Column(c) => c.name.len() + c.qualifier.as_ref().map_or(0, String::len),
            Scalar::Cast(inner, _) | Scalar::Unary(_, inner) => return cost(inner, nodes, bytes),
            Scalar::Binary(_, a, b) => {
                cost(a, nodes, bytes)?;
                cost(b, nodes, bytes)?;
                0
            }
            Scalar::Call(name, args) => {
                for a in args {
                    cost(a, nodes, bytes)?;
                }
                name.len()
            }
            Scalar::SubstringOrder(args) => {
                for a in args {
                    cost(a, nodes, bytes)?;
                }
                0
            }
            Scalar::Predicate(f) => {
                filter_cost(f, nodes, bytes)?;
                0
            }
            Scalar::Case(c) => {
                for (f, s) in &c.arms {
                    filter_cost(f, nodes, bytes)?;
                    cost(s, nodes, bytes)?;
                }
                if let Some(s) = &c.otherwise {
                    cost(s, nodes, bytes)?;
                }
                0
            }
        };
        *bytes = bytes.checked_add(payload).ok_or(SqlError::Limit)?;
        Ok(())
    }
    let (mut nodes, mut bytes) = (0usize, 0usize);
    cost(lhs, &mut nodes, &mut bytes)?;
    if nodes.checked_mul(copies).is_none_or(|n| n > MAX_WORK)
        || bytes.checked_mul(copies).is_none_or(|b| b > MAX_SQL)
    {
        return Err(SqlError::Limit);
    }
    Ok(())
}

fn is_subquery_open(tokens: &[Token], at: usize) -> bool {
    tokens.get(at) == Some(&Token::Symbol('('))
        && matches!(tokens.get(at + 1), Some(Token::Word(w)) if w == "select")
}
fn subquery_end(tokens: &[Token], open: usize) -> R<usize> {
    let mut depth = 0usize;
    for (at, token) in tokens.iter().enumerate().skip(open) {
        match token {
            Token::Symbol('(') => depth += 1,
            Token::Symbol(')') => {
                depth -= 1;
                if depth == 0 {
                    return Ok(at);
                }
            }
            _ => {}
        }
    }
    Err(SqlError::Syntax)
}
fn admit_subquery_forms(tokens: &[Token]) -> R<()> {
    let mut at = 0;
    while at < tokens.len() {
        if is_subquery_open(tokens, at) {
            if at == 0 || tokens.get(at - 1) != Some(&Token::Word("in".into())) {
                return Err(SqlError::Unsupported);
            }
            if at >= 2 && tokens[at - 2] == Token::Symbol(')') {
                let mut depth = 1;
                let mut comma = false;
                for open in (0..at - 2).rev() {
                    match &tokens[open] {
                        Token::Symbol(')') => depth += 1,
                        Token::Symbol('(') => {
                            depth -= 1;
                            if depth == 0 {
                                let call = open > 0
                                    && match &tokens[open - 1] {
                                        Token::QuotedIdent(_) => true,
                                        Token::Word(w) => ![
                                            "where", "and", "or", "not", "select", "on", "having",
                                            "by", "when", "then", "else", "in",
                                        ]
                                        .contains(&w.as_str()),
                                        _ => false,
                                    };
                                if comma && !call {
                                    return Err(SqlError::Unsupported);
                                }
                                break;
                            }
                        }
                        Token::Symbol(',') if depth == 1 => comma = true,
                        _ => {}
                    }
                }
            }
            at = subquery_end(tokens, at)? + 1;
        } else {
            at += 1;
        }
    }
    Ok(())
}
fn admit_subquery_depth(tokens: &[Token]) -> R<()> {
    let mut stack = Vec::new();
    let mut nested = 0;
    for (at, token) in tokens.iter().enumerate() {
        match token {
            Token::Symbol('(') => {
                let sub = is_subquery_open(tokens, at);
                stack.push(sub);
                if sub {
                    nested += 1;
                    if nested > 32 {
                        return Err(SqlError::Limit);
                    }
                }
            }
            Token::Symbol(')') if stack.pop() == Some(true) => {
                nested -= 1;
            }
            _ => {}
        }
    }
    Ok(())
}
fn walk_scalar(s: &Scalar, visit: &mut impl FnMut(&Scalar) -> R<()>) -> R<()> {
    visit(s)?;
    match s {
        Scalar::Cast(a, _) | Scalar::Unary(_, a) => walk_scalar(a, visit)?,
        Scalar::Binary(_, a, b) => {
            walk_scalar(a, visit)?;
            walk_scalar(b, visit)?;
        }
        Scalar::Call(_, args) => {
            for a in args {
                walk_scalar(a, visit)?;
            }
        }
        Scalar::SubstringOrder(args) => {
            for a in args {
                walk_scalar(a, visit)?;
            }
        }
        Scalar::Predicate(f) => f.walk_scalars(visit)?,
        Scalar::Case(c) => {
            for (f, a) in &c.arms {
                f.walk_scalars(visit)?;
                walk_scalar(a, visit)?;
            }
            if let Some(a) = &c.otherwise {
                walk_scalar(a, visit)?;
            }
        }
        _ => {}
    }
    Ok(())
}
fn reject_scalar_subquery(s: &Scalar) -> R<()> {
    match s {
        Scalar::Predicate(f) => f.admit_subquery_positions(false)?,
        Scalar::Case(c) => {
            for (f, value) in &c.arms {
                f.admit_subquery_positions(false)?;
                reject_scalar_subquery(value)?;
            }
            if let Some(value) = &c.otherwise {
                reject_scalar_subquery(value)?;
            }
        }
        Scalar::Cast(value, _) | Scalar::Unary(_, value) => reject_scalar_subquery(value)?,
        Scalar::Binary(_, a, b) => {
            reject_scalar_subquery(a)?;
            reject_scalar_subquery(b)?;
        }
        Scalar::Call(_, args) => {
            for value in args {
                reject_scalar_subquery(value)?;
            }
        }
        Scalar::SubstringOrder(args) => {
            for value in args {
                reject_scalar_subquery(value)?;
            }
        }
        Scalar::Literal(_) | Scalar::Param(_) | Scalar::Column(_) => {}
    }
    Ok(())
}
impl Filter {
    fn walk_scalars(&self, visit: &mut impl FnMut(&Scalar) -> R<()>) -> R<()> {
        self.visit_scalars(&mut |s| walk_scalar(s, visit))?;
        self.visit_subqueries(&mut |_, q| q.walk_scalars(visit))
    }
    fn visit_subqueries<'a>(
        &'a self,
        visit: &mut impl FnMut(&'a Scalar, &'a Query) -> R<()>,
    ) -> R<()> {
        match self {
            Self::InSubquery { value, query, .. } => visit(value, query)?,
            Self::Clauses(c) => {
                if let Some(f) = &c.filter {
                    f.visit_subqueries(visit)?;
                }
                if let Some(f) = &c.having {
                    f.visit_subqueries(visit)?;
                }
            }
            Self::Logic(a, _, b) => {
                a.visit_subqueries(visit)?;
                b.visit_subqueries(visit)?;
            }
            Self::Not(f) | Self::Is(f, _, _) => f.visit_subqueries(visit)?,
            _ => {}
        }
        Ok(())
    }
    fn admit_subquery_positions(&self, allowed: bool) -> R<()> {
        self.visit_scalars(&mut reject_scalar_subquery)?;
        self.visit_subqueries(&mut |_, _| {
            if allowed {
                Ok(())
            } else {
                Err(SqlError::Unsupported)
            }
        })
    }
    fn infer_outer(
        &self,
        params: &mut [Option<DataType>],
        resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
        tables: &[&TableDef],
    ) -> R<()> {
        match self {
            Self::InSubquery { .. } => Ok(()),
            Self::Logic(a, _, b) => {
                a.infer_outer(params, resolve, tables)?;
                b.infer_outer(params, resolve, tables)
            }
            Self::Not(f) | Self::Is(f, _, _) => f.infer_outer(params, resolve, tables),
            _ => self.infer(params, resolve, tables),
        }
    }
    fn infer_with_subqueries(
        &self,
        params: &mut [Option<DataType>],
        resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
        tables: &[&TableDef],
        subqueries: &[(usize, BoundQuery<'_>)],
    ) -> R<()> {
        match self {
            Self::InSubquery { value, query, .. } => {
                let bound = subqueries
                    .iter()
                    .find(|(key, _)| *key == query.as_ref() as *const Query as usize)
                    .ok_or(SqlError::Unsupported)?;
                let expected = if matches!(value, Scalar::Param(n) if params.get(n - 1).is_some_and(Option::is_some))
                {
                    None
                } else {
                    Some(&bound.1.columns[0].data_type)
                };
                value.infer(expected, params, resolve, tables)
            }
            Self::Logic(a, _, b) => {
                a.infer_with_subqueries(params, resolve, tables, subqueries)?;
                b.infer_with_subqueries(params, resolve, tables, subqueries)
            }
            Self::Not(f) | Self::Is(f, _, _) => {
                f.infer_with_subqueries(params, resolve, tables, subqueries)
            }
            _ => self.infer(params, resolve, tables),
        }
    }
    fn bind_with_subqueries(
        &self,
        params: &[Option<DataType>],
        resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
        tables: &[&TableDef],
        subqueries: &[(usize, BoundQuery<'_>)],
    ) -> R<BoundFilter> {
        Ok(match self {
            Self::InSubquery {
                value,
                query,
                negated,
            } => {
                let key = query.as_ref() as *const Query as usize;
                let column_type = &subqueries
                    .iter()
                    .find(|(k, _)| *k == key)
                    .ok_or(SqlError::Unsupported)?
                    .1
                    .columns[0]
                    .data_type;
                if *column_type == DataType::Bpchar {
                    return Err(SqlError::Unsupported);
                }
                let value = if matches!(value, Scalar::Literal(Value::Null | Value::Text(_))) {
                    Scalar::Cast(Box::new(value.clone()), column_type.clone())
                } else {
                    value.clone()
                };
                let bound = value.bind(resolve, tables, params)?;
                if bound.ty == DataType::Bpchar {
                    return Err(SqlError::Unsupported);
                }
                // Compare's existing type policy, using a typed parameter as the RHS.
                let mut comparison_params = params.to_vec();
                comparison_params.push(Some(column_type.clone()));
                Filter::Compare(
                    value.clone(),
                    "=".into(),
                    Scalar::Param(comparison_params.len()),
                )
                .bind(&comparison_params, resolve, tables)?;
                BoundFilter::InSubquery {
                    value: bound,
                    query: key,
                    column_type: column_type.clone(),
                    negated: *negated,
                    clock: None,
                }
            }
            Self::Logic(a, and, b) => BoundFilter::Logic(
                Box::new(a.bind_with_subqueries(params, resolve, tables, subqueries)?),
                *and,
                Box::new(b.bind_with_subqueries(params, resolve, tables, subqueries)?),
            ),
            Self::Not(f) => BoundFilter::Not(Box::new(
                f.bind_with_subqueries(params, resolve, tables, subqueries)?,
            )),
            Self::Is(f, target, negated) => BoundFilter::Is(
                Box::new(f.bind_with_subqueries(params, resolve, tables, subqueries)?),
                *target,
                *negated,
            ),
            _ => self.bind(params, resolve, tables)?,
        })
    }
    fn visit_scalars(&self, visit: &mut impl FnMut(&Scalar) -> R<()>) -> R<()> {
        match self {
            Self::Clauses(c) => {
                if let Some(f) = &c.filter {
                    f.visit_scalars(visit)?;
                }
                for s in &c.group {
                    visit(s)?;
                }
                if let Some(f) = &c.having {
                    f.visit_scalars(visit)?;
                }
            }
            Self::Scalar(s) | Self::Null(s, _) | Self::InSubquery { value: s, .. } => visit(s)?,
            Self::Compare(a, _, b) => {
                visit(a)?;
                visit(b)?;
            }
            Self::Like {
                value,
                pattern,
                escape,
                ..
            } => {
                visit(value)?;
                visit(pattern)?;
                if let Some(s) = escape {
                    visit(s)?;
                }
            }
            Self::Not(f) | Self::Is(f, _, _) => f.visit_scalars(visit)?,
            Self::Logic(a, _, b) => {
                a.visit_scalars(visit)?;
                b.visit_scalars(visit)?;
            }
        }
        Ok(())
    }
    fn has_volatile_call(&self) -> bool {
        let mut found = false;
        let _ = self.visit_scalars(&mut |s| {
            found |= s.has_volatile_call();
            Ok(())
        });
        let _ = self.visit_subqueries(&mut |_, q| {
            found |= q.has_volatile_call();
            Ok(())
        });
        found
    }
    fn sequence_dependencies(&self, out: &mut Vec<Option<String>>) {
        let _ = self.visit_scalars(&mut |s| {
            s.sequence_dependencies(out);
            Ok(())
        });
        let _ = self.visit_subqueries(&mut |_, q| {
            out.extend(q.sequence_dependencies());
            Ok(())
        });
    }
    fn has_undefined_routine(&self) -> bool {
        let mut found = false;
        let _ = self.visit_scalars(&mut |s| {
            found |= scalar_has_undefined_routine(s);
            Ok(())
        });
        let _ = self.visit_subqueries(&mut |_, q| {
            found |= q.has_undefined_routine();
            Ok(())
        });
        found
    }
    fn reject_nested_aggregate(&self) -> R<()> {
        self.visit_scalars(&mut |s| s.reject_nested_aggregate())
    }
    fn admit_shape(&self, depth: usize, nodes: &mut usize) -> R<()> {
        if depth > 32 || *nodes == MAX_WORK {
            return Err(SqlError::Limit);
        }
        *nodes += 1;
        match self {
            Self::Clauses(_) => Err(SqlError::Unsupported),
            Self::InSubquery { value, .. } => insert_values::shape(value, depth + 1, nodes),
            Self::Scalar(s) | Self::Null(s, _) => insert_values::shape(s, depth + 1, nodes),
            Self::Compare(a, _, b) => {
                insert_values::shape(a, depth + 1, nodes)?;
                insert_values::shape(b, depth + 1, nodes)
            }
            Self::Like {
                value,
                pattern,
                escape,
                ..
            } => {
                insert_values::shape(value, depth + 1, nodes)?;
                insert_values::shape(pattern, depth + 1, nodes)?;
                if let Some(s) = escape {
                    insert_values::shape(s, depth + 1, nodes)?;
                }
                Ok(())
            }
            Self::Not(f) | Self::Is(f, _, _) => f.admit_shape(depth + 1, nodes),
            Self::Logic(a, _, b) => {
                a.admit_shape(depth + 1, nodes)?;
                b.admit_shape(depth + 1, nodes)
            }
        }
    }
    fn parse(p: &mut Parser, precedence: usize, depth: usize) -> R<Self> {
        Self::parse_context(p, precedence, depth, false)
    }
    fn parse_context(
        p: &mut Parser,
        precedence: usize,
        depth: usize,
        aggregate_argument: bool,
    ) -> R<Self> {
        if depth > 32 {
            return Err(SqlError::Limit);
        }
        let mut f = if p.word("not") {
            Self::Not(Box::new(Self::parse_context(
                p,
                3,
                depth + 1,
                aggregate_argument,
            )?))
        } else {
            let parse_scalar = |p: &mut Parser, depth| {
                if aggregate_argument {
                    Scalar::parse_aggregate_argument(p, depth)
                } else {
                    Scalar::parse(p, depth)
                }
            };
            let a = parse_scalar(p, depth)?;
            let not = matches!(p.tokens.get(p.at), Some(Token::Word(w)) if w == "not")
                && matches!(p.tokens.get(p.at + 1), Some(Token::Word(w)) if w == "in" || w == "between" || w == "like" || w == "ilike");
            if not {
                p.at += 1;
            }
            if matches!(p.tokens.get(p.at), Some(Token::Word(w)) if w == "like" || w == "ilike") {
                let insensitive = p.word("ilike");
                if !insensitive {
                    p.need("like")?;
                }
                let pattern = parse_scalar(p, depth + 1)?;
                let escape = if p.word("escape") {
                    Some(parse_scalar(p, depth + 1)?)
                } else {
                    None
                };
                Self::Like {
                    value: a,
                    pattern,
                    escape,
                    insensitive,
                    negated: not,
                }
            } else if p.word("in") {
                p.punct('(')?;
                if p.sym(')') {
                    return Err(SqlError::Syntax);
                }
                if matches!(p.tokens.get(p.at), Some(Token::Word(w)) if w == "select") {
                    let end = subquery_end(&p.tokens, p.at - 1)?;
                    let mut child = Parser {
                        tokens: p.tokens[p.at..end].to_vec(),
                        at: 0,
                        params: BTreeSet::new(),
                        relations: Vec::new(),
                    };
                    let distinct = child.tokens.get(1) == Some(&Token::Word("distinct".into()));
                    if distinct {
                        child.tokens.remove(1);
                    }
                    let mut query = Query::parse(&mut child)?;
                    if distinct {
                        query.set_distinct();
                    }
                    p.params.extend(child.params);
                    p.relations.extend(child.relations);
                    p.at = end + 1;
                    Self::InSubquery {
                        value: a,
                        query: Box::new(query),
                        negated: not,
                    }
                } else {
                    let mut values = vec![parse_scalar(p, depth + 1)?];
                    while p.sym(',') {
                        values.push(parse_scalar(p, depth + 1)?);
                    }
                    p.punct(')')?;
                    admit_expansion(&a, values.len())?;
                    Self::membership(&a, &values, not)
                }
            } else if p.word("between") {
                if p.word("symmetric") {
                    return Err(SqlError::Unsupported);
                }
                p.word("asymmetric");
                let lower = parse_scalar(p, depth + 1)?;
                p.need("and")?;
                let upper = parse_scalar(p, depth + 1)?;
                admit_expansion(&a, 2)?;
                Self::Logic(
                    Box::new(Self::Compare(
                        a.clone(),
                        if not { "<" } else { ">=" }.into(),
                        lower,
                    )),
                    !not,
                    Box::new(Self::Compare(a, if not { ">" } else { "<=" }.into(), upper)),
                )
            } else if let Some(Token::Op(op)) = p.tokens.get(p.at).cloned() {
                if !["=", "<>", "!=", "<", "<=", ">", ">="].contains(&op.as_str()) {
                    return Err(SqlError::Unsupported);
                }
                p.at += 1;
                if matches!(
                    p.tokens.get(p.at),
                    None | Some(Token::Symbol(')' | ',' | ';'))
                ) {
                    return Err(SqlError::Syntax);
                }
                Self::Compare(a, op, parse_scalar(p, depth + 1)?)
            } else {
                match a {
                    Scalar::Predicate(f) => *f,
                    a => Self::Scalar(a),
                }
            }
        };
        // IS binds more tightly than prefix NOT and the logical operators.
        let mut suffixes = depth;
        while p.word("is") {
            suffixes += 1;
            if suffixes > 32 {
                return Err(SqlError::Limit);
            }
            let negated = p.word("not");
            if p.word("distinct") {
                p.need("from")?;
                let a = match f {
                    Self::Scalar(a) => a,
                    f => Scalar::Predicate(Box::new(f)),
                };
                let b = if aggregate_argument {
                    Scalar::parse_aggregate_argument(p, depth + 1)?
                } else {
                    Scalar::parse(p, depth + 1)?
                };
                if a.has_volatile_call() || b.has_volatile_call() {
                    return Err(SqlError::Unsupported);
                }
                admit_expansion(&a, 2)?;
                admit_expansion(&b, 2)?;
                let equal = Self::Is(
                    Box::new(Self::Compare(a.clone(), "=".into(), b.clone())),
                    Some(true),
                    false,
                );
                let both_null = Self::Logic(
                    Box::new(Self::Null(a, false)),
                    true,
                    Box::new(Self::Null(b, false)),
                );
                let same = Self::Logic(Box::new(equal), false, Box::new(both_null));
                f = if negated {
                    same
                } else {
                    Self::Not(Box::new(same))
                };
            } else {
                let target = if p.word("true") {
                    Some(true)
                } else if p.word("false") {
                    Some(false)
                } else if p.word("unknown") {
                    None
                } else if p.word("null") {
                    if let Self::Scalar(a) = f {
                        f = Self::Null(a, negated);
                        continue;
                    }
                    None
                } else {
                    return Err(SqlError::Syntax);
                };
                f = Self::Is(Box::new(f), target, negated);
            }
        }
        loop {
            let and = matches!(p.tokens.get(p.at), Some(Token::Word(w)) if w == "and");
            let or = matches!(p.tokens.get(p.at), Some(Token::Word(w)) if w == "or");
            let level = if and {
                2
            } else if or {
                1
            } else {
                break;
            };
            if level <= precedence {
                break;
            }
            p.at += 1;
            f = Self::Logic(
                Box::new(f),
                and,
                Box::new(Self::parse_context(
                    p,
                    level,
                    depth + 1,
                    aggregate_argument,
                )?),
            );
        }
        Ok(f)
    }
    // Split the list in half so large IN lists retain logarithmic filter depth.
    // Callers must call admit_expansion first: every entry clones the lhs.
    fn membership(a: &Scalar, values: &[Scalar], not: bool) -> Self {
        if values.len() == 1 {
            return Self::Compare(
                a.clone(),
                if not { "<>" } else { "=" }.into(),
                values[0].clone(),
            );
        }
        let middle = values.len() / 2;
        Self::Logic(
            Box::new(Self::membership(a, &values[..middle], not)),
            not,
            Box::new(Self::membership(a, &values[middle..], not)),
        )
    }
    fn bounds(&self, out: &mut Vec<(ColumnRef, String, Expr)>) {
        match self {
            Self::Is(_, _, _) => (),
            Self::Logic(a, true, b) => {
                a.bounds(out);
                b.bounds(out);
            }
            Self::Compare(Scalar::Column(c), op, b)
                if ["=", "<", "<=", ">", ">="].contains(&op.as_str()) =>
            {
                let e = match b {
                    Scalar::Param(n) => Expr::Param(*n),
                    Scalar::Literal(v) => Expr::Literal(v.clone()),
                    _ => return,
                };
                out.push((c.clone(), op.clone(), e));
            }
            _ => {}
        }
    }
    fn infer(
        &self,
        params: &mut [Option<DataType>],
        resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
        tables: &[&TableDef],
    ) -> R<()> {
        match self {
            Self::Clauses(_) | Self::InSubquery { .. } => Err(SqlError::Unsupported),
            Self::Scalar(a) => a.infer(Some(&DataType::Boolean), params, resolve, tables),
            Self::Null(a, _) => a.infer(None, params, resolve, tables),
            Self::Compare(a, _, b) => {
                a.infer(None, params, resolve, tables)?;
                b.infer(None, params, resolve, tables)?;
                if let Ok(bound) = a.bind(resolve, tables, params)
                    && !matches!(b, Scalar::Param(n) if params.get(n - 1).is_some_and(Option::is_some))
                {
                    b.infer(Some(&bound.ty), params, resolve, tables)?;
                }
                if let Ok(bound) = b.bind(resolve, tables, params)
                    && !matches!(a, Scalar::Param(n) if params.get(n - 1).is_some_and(Option::is_some))
                {
                    a.infer(Some(&bound.ty), params, resolve, tables)?;
                }
                Ok(())
            }
            Self::Like {
                value,
                pattern,
                escape,
                ..
            } => {
                for a in [Some(value), Some(pattern), escape.as_ref()]
                    .into_iter()
                    .flatten()
                {
                    let expected = if matches!(a, Scalar::Param(n) if params.get(n - 1).is_some_and(Option::is_some))
                    {
                        None
                    } else {
                        Some(&DataType::Text)
                    };
                    a.infer(expected, params, resolve, tables)?;
                }
                Ok(())
            }
            Self::Not(a) | Self::Is(a, _, _) => a.infer(params, resolve, tables),
            Self::Logic(a, _, b) => {
                a.infer(params, resolve, tables)?;
                b.infer(params, resolve, tables)
            }
        }
    }
    // Mutation declarations are authoritative only for plain column/parameter
    // comparisons. Other expressions keep their existing inference constraints.
    fn infer_mutation(
        &self,
        params: &mut [Option<DataType>],
        declared: &[Option<DataType>],
        resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
        tables: &[&TableDef],
    ) -> R<()> {
        match self {
            Self::Compare(Scalar::Column(column), _, Scalar::Param(n))
            | Self::Compare(Scalar::Param(n), _, Scalar::Column(column))
                if declared.get(n - 1).is_some_and(Option::is_some) =>
            {
                let column = resolve(column, 0)?;
                infer_mutation_parameter(
                    &Expr::Param(*n),
                    &tables[column.source].columns[column.column].data_type,
                    params,
                    declared,
                    false,
                )
            }
            Self::Logic(a, _, b) => {
                a.infer_mutation(params, declared, resolve, tables)?;
                b.infer_mutation(params, declared, resolve, tables)
            }
            Self::Not(a) | Self::Is(a, _, _) => a.infer_mutation(params, declared, resolve, tables),
            _ => self.infer(params, resolve, tables),
        }
    }
    fn bind(
        &self,
        params: &[Option<DataType>],
        resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
        tables: &[&TableDef],
    ) -> R<BoundFilter> {
        let bind = |a: &Scalar| a.bind(resolve, tables, params);
        Ok(match self {
            Self::Clauses(_) | Self::InSubquery { .. } => return Err(SqlError::Unsupported),
            Self::Scalar(a) => {
                let a = if matches!(a, Scalar::Literal(Value::Null)) {
                    bind(&Scalar::Cast(Box::new(a.clone()), DataType::Boolean))?
                } else {
                    bind(a)?
                };
                if a.ty != DataType::Boolean {
                    return Err(SqlError::Type);
                }
                BoundFilter::Scalar(a)
            }
            Self::Null(a, n) => BoundFilter::Null(bind(a)?, *n),
            Self::Compare(a, op, b) => {
                let mut av = bind(a)?;
                let mut bv = bind(b)?;
                if av.ty == DataType::Bpchar || bv.ty == DataType::Bpchar {
                    return Err(SqlError::Unsupported);
                }
                if av.ty == DataType::Varchar {
                    av.ty = DataType::Text;
                }
                if bv.ty == DataType::Varchar {
                    bv.ty = DataType::Text;
                }
                let unknown =
                    |a: &Scalar| matches!(a, Scalar::Literal(Value::Null | Value::Text(_)));
                if av.ty != bv.ty {
                    if unknown(a) {
                        av = bind(&Scalar::Cast(Box::new(a.clone()), bv.ty.clone()))?;
                    } else if unknown(b) {
                        bv = bind(&Scalar::Cast(Box::new(b.clone()), av.ty.clone()))?;
                    } else if matches!(
                        av.ty,
                        DataType::SmallInt | DataType::Integer | DataType::BigInt
                    ) && matches!(
                        bv.ty,
                        DataType::SmallInt | DataType::Integer | DataType::BigInt
                    ) {
                    } else if let Some(ty) = functions::comparison_type(&av.ty, &bv.ty) {
                        // Rebind syntax, never clone authority-bearing bound nodes.
                        if av.ty != ty {
                            av = bind(&Scalar::Cast(Box::new(a.clone()), ty.clone()))?;
                        }
                        if bv.ty != ty {
                            bv = bind(&Scalar::Cast(Box::new(b.clone()), ty))?;
                        }
                    } else {
                        let mismatch = functions::comparison_mismatch(&av.ty, &bv.ty);
                        return functions::cross_compare(op, av, bv)?
                            .map(BoundFilter::Scalar)
                            .ok_or(mismatch);
                    }
                }
                BoundFilter::Compare(av, op.clone(), bv)
            }
            Self::Like {
                value,
                pattern,
                escape,
                insensitive,
                negated,
            } => {
                let text = |a: &Scalar| {
                    let bound = bind(a)?;
                    if bound.ty == DataType::Bpchar {
                        return Err(SqlError::Unsupported);
                    }
                    if !matches!(bound.ty, DataType::Text | DataType::Varchar)
                        && !matches!(a, Scalar::Literal(Value::Null))
                    {
                        return Err(SqlError::UndefinedFunction);
                    }
                    Ok(bound)
                };
                BoundFilter::Like {
                    value: text(value)?,
                    pattern: text(pattern)?,
                    escape: escape.as_ref().map(text).transpose()?,
                    insensitive: *insensitive,
                    negated: *negated,
                }
            }
            Self::Not(a) => BoundFilter::Not(Box::new(a.bind(params, resolve, tables)?)),
            Self::Is(a, target, not) => {
                BoundFilter::Is(Box::new(a.bind(params, resolve, tables)?), *target, *not)
            }
            Self::Logic(a, and, b) => BoundFilter::Logic(
                Box::new(a.bind(params, resolve, tables)?),
                *and,
                Box::new(b.bind(params, resolve, tables)?),
            ),
        })
    }
}

struct MaterializedSubquery {
    values: Vec<Value>,
    keys: Option<std::collections::HashSet<Vec<u8>>>,
    has_null: bool,
}
struct Execution<'a> {
    cancel: RefCell<&'a mut dyn FnMut() -> bool>,
    visits: Cell<usize>,
    subqueries: RefCell<BTreeMap<usize, MaterializedSubquery>>,
    retention: RefCell<scan::Retention>,
    declared: Vec<Option<DataType>>,
}
impl<'a> Execution<'a> {
    fn new(cancel: &'a mut dyn FnMut() -> bool, declared: &[Option<DataType>]) -> Self {
        Self {
            cancel: RefCell::new(cancel),
            declared: declared.to_vec(),
            visits: Cell::new(0),
            subqueries: RefCell::new(BTreeMap::new()),
            retention: RefCell::new(scan::Retention::new(scan::retention_limit())),
        }
    }
    fn cancelled(&self) -> bool {
        (self.cancel.borrow_mut())()
    }
    fn comparison(&self) -> R<()> {
        charge_candidate(&self.visits)?;
        if self
            .visits
            .get()
            .is_multiple_of(scan::CANCEL_POLL_ROWS as usize)
            && self.cancelled()
        {
            return Err(SqlError::Aborted);
        }
        Ok(())
    }
}
fn charge_candidate(visits: &Cell<usize>) -> R<()> {
    let next = visits.get().checked_add(1).ok_or(SqlError::Limit)?;
    visits.set(next);
    if next > MAX_JOIN_VISITS {
        return Err(SqlError::Limit);
    }
    Ok(())
}
impl BoundFilter {
    #[allow(clippy::too_many_arguments)]
    fn eval_with_subqueries(
        &self,
        row: &Tuple,
        params: &[Value],
        bytes: &mut usize,
        owner: &BoundQuery<'_>,
        state: &Relational,
        source: &dyn scan::RowSource,
        execution: &Execution<'_>,
    ) -> R<Option<bool>> {
        match self {
            Self::InSubquery {
                value,
                query,
                column_type,
                negated,
                clock,
            } => {
                if !execution.subqueries.borrow().contains_key(query) {
                    let child = &owner
                        .subqueries
                        .iter()
                        .find(|(key, _)| key == query)
                        .ok_or(SqlError::Unsupported)?
                        .1;
                    let result = child.execute_context(
                        state,
                        source,
                        params,
                        &execution.declared,
                        execution,
                        true,
                        false,
                    )?;
                    let mut values = Vec::with_capacity(result.rows.len());
                    let mut keys = functions::join_key_supported(&value.ty, column_type)
                        .then(std::collections::HashSet::new);
                    let mut has_null = false;
                    for result_row in result.rows {
                        if execution.cancelled() {
                            return Err(SqlError::Aborted);
                        }
                        let element = result_row
                            .values
                            .into_iter()
                            .next()
                            .ok_or(SqlError::Syntax)?;
                        execution
                            .retention
                            .borrow_mut()
                            .charge(value_bytes(&element) + std::mem::size_of::<Value>())?;
                        has_null |= element == Value::Null;
                        if element != Value::Null
                            && let Some(keys) = &mut keys
                        {
                            let key = encode_key(std::slice::from_ref(&element))?;
                            execution
                                .retention
                                .borrow_mut()
                                .charge(key.len() + 4 * std::mem::size_of::<usize>())?;
                            keys.insert(key);
                        }
                        values.push(element);
                    }
                    execution.subqueries.borrow_mut().insert(
                        *query,
                        MaterializedSubquery {
                            values,
                            keys,
                            has_null,
                        },
                    );
                }
                let lhs = value.eval(row, params, bytes)?;
                let cache = execution.subqueries.borrow();
                let materialized = cache.get(query).ok_or(SqlError::Unsupported)?;
                let member = if materialized.values.is_empty() {
                    Some(false)
                } else if lhs == Value::Null {
                    None
                } else {
                    let found = if let Some(keys) = &materialized.keys {
                        match functions::join_key(&lhs, &value.ty, column_type)? {
                            Some(key) => keys.contains(&encode_key(&[key])?),
                            None => false,
                        }
                    } else {
                        let mut found = false;
                        for element in &materialized.values {
                            execution.comparison()?;
                            if functions::join_equal(
                                &lhs,
                                &value.ty,
                                element,
                                column_type,
                                clock.as_ref(),
                            )? {
                                found = true;
                                break;
                            }
                        }
                        found
                    };
                    if found {
                        Some(true)
                    } else if materialized.has_null {
                        None
                    } else {
                        Some(false)
                    }
                };
                Ok(member.map(|v| v != *negated))
            }
            Self::Logic(a, and, b) => {
                let a =
                    a.eval_with_subqueries(row, params, bytes, owner, state, source, execution)?;
                if a == Some(!*and) {
                    return Ok(a);
                }
                let b =
                    b.eval_with_subqueries(row, params, bytes, owner, state, source, execution)?;
                Ok(if b == Some(!*and) {
                    b
                } else if a.is_none() || b.is_none() {
                    None
                } else {
                    Some(*and)
                })
            }
            Self::Not(f) => Ok(f
                .eval_with_subqueries(row, params, bytes, owner, state, source, execution)?
                .map(|v| !v)),
            Self::Is(f, target, negated) => Ok(Some(
                (f.eval_with_subqueries(row, params, bytes, owner, state, source, execution)?
                    == *target)
                    != *negated,
            )),
            _ => self.eval(row, params, bytes),
        }
    }
    /// Immutable planning only: outer None is dependent; inner None is SQL NULL.
    fn plan_constant(&self, budget: &mut usize) -> R<Option<Option<bool>>> {
        let boolean = |value| match value {
            Value::Bool(v) => Ok(Some(v)),
            Value::Null => Ok(None),
            _ => Err(SqlError::Type),
        };
        match self {
            Self::InSubquery { .. } => Ok(None),
            Self::Scalar(a) => a.plan_constant(budget)?.map(boolean).transpose(),
            Self::Null(a, not) => Ok(a
                .plan_constant(budget)?
                .map(|v| Some((v == Value::Null) != *not))),
            Self::Compare(a, op, b) => {
                let a = a.plan_constant(budget)?;
                let b = b.plan_constant(budget)?;
                Ok(match (a, b) {
                    (Some(Value::Null), Some(_)) | (Some(_), Some(Value::Null)) => Some(None),
                    (Some(a), Some(b)) => {
                        let c = cmp(&a, &b);
                        Some(Some(match op.as_str() {
                            "=" => c == Ordering::Equal,
                            "<>" | "!=" => c != Ordering::Equal,
                            "<" => c == Ordering::Less,
                            "<=" => c != Ordering::Greater,
                            ">" => c == Ordering::Greater,
                            ">=" => c != Ordering::Less,
                            _ => unreachable!(),
                        }))
                    }
                    _ => None,
                })
            }
            Self::Like {
                value,
                pattern,
                escape,
                insensitive,
                negated,
            } => {
                let pattern = pattern.plan_constant(budget)?;
                let escape = match escape {
                    Some(a) => a.plan_constant(budget)?,
                    None => Some(Value::Text("\\".into())),
                };
                if pattern == Some(Value::Null) || escape == Some(Value::Null) {
                    return Ok(Some(None));
                }
                let escape = match (&pattern, &escape) {
                    (Some(pattern), Some(escape)) => parse_like_escape(pattern, escape)?,
                    _ => None,
                };
                let value = value.plan_constant(budget)?;
                match (value, pattern, escape) {
                    (Some(value), Some(pattern), Some(escape)) => Ok(Some(eval_like(
                        value,
                        pattern,
                        escape,
                        *insensitive,
                        *negated,
                        budget,
                    )?)),
                    _ => Ok(None),
                }
            }
            Self::Not(a) => Ok(a.plan_constant(budget)?.map(|a| a.map(|v| !v))),
            Self::Is(a, target, not) => Ok(a
                .plan_constant(budget)?
                .map(|a| Some((a == *target) != *not))),
            Self::Logic(a, and, b) => {
                let a = a.plan_constant(budget)?;
                if a == Some(Some(!*and)) {
                    return Ok(a);
                }
                let b = b.plan_constant(budget)?;
                if b == Some(Some(!*and)) {
                    return Ok(b);
                }
                Ok(match (a, b) {
                    (Some(a), Some(b)) => Some(if a.is_none() || b.is_none() {
                        None
                    } else {
                        Some(*and)
                    }),
                    _ => None,
                })
            }
        }
    }
    fn eval(&self, row: &Tuple, params: &[Value], bytes: &mut usize) -> R<Option<bool>> {
        Ok(match self {
            Self::InSubquery { .. } => return Err(SqlError::Unsupported),
            Self::Scalar(a) => match a.eval(row, params, bytes)? {
                Value::Bool(b) => Some(b),
                Value::Null => None,
                _ => return Err(SqlError::Type),
            },
            Self::Null(a, n) => Some((a.eval(row, params, bytes)? == Value::Null) != *n),
            Self::Like {
                value,
                pattern,
                escape,
                insensitive,
                negated,
            } => {
                let pattern = pattern.eval(row, params, bytes)?;
                let escape = match escape {
                    Some(a) => a.eval(row, params, bytes)?,
                    None => Value::Text("\\".into()),
                };
                let Some(escape) = parse_like_escape(&pattern, &escape)? else {
                    return Ok(None);
                };
                let value = value.eval(row, params, bytes)?;
                eval_like(value, pattern, escape, *insensitive, *negated, bytes)?
            }
            Self::Compare(a, op, b) => {
                let a = a.eval(row, params, bytes)?;
                let b = b.eval(row, params, bytes)?;
                if a == Value::Null || b == Value::Null {
                    None
                } else {
                    let c = cmp(&a, &b);
                    Some(match op.as_str() {
                        "=" => c == Ordering::Equal,
                        "<>" | "!=" => c != Ordering::Equal,
                        "<" => c == Ordering::Less,
                        "<=" => c != Ordering::Greater,
                        ">" => c == Ordering::Greater,
                        ">=" => c != Ordering::Less,
                        _ => unreachable!(),
                    })
                }
            }
            Self::Not(a) => a.eval(row, params, bytes)?.map(|b| !b),
            Self::Is(a, target, not) => Some((a.eval(row, params, bytes)? == *target) != *not),
            Self::Logic(a, and, b) => {
                let a = a.eval(row, params, bytes)?;
                if a == Some(!*and) {
                    a
                } else {
                    let b = b.eval(row, params, bytes)?;
                    if b == Some(!*and) {
                        b
                    } else if a.is_none() || b.is_none() {
                        None
                    } else {
                        Some(*and)
                    }
                }
            }
        })
    }
}

impl Parser {
    fn column_ref(&mut self) -> R<ColumnRef> {
        let (qualifier, name) = self.qualified_name()?;
        Ok(ColumnRef { qualifier, name })
    }
    fn source(&mut self) -> R<Source> {
        let name = self.table_name()?;
        let alias = if self.word("as")
            || matches!(self.tokens.get(self.at), Some(Token::QuotedIdent(_)))
            || matches!(self.tokens.get(self.at), Some(Token::Word(w)) if !["inner", "left", "join", "where", "order", "limit", "group", "having", "on", "right", "full", "cross"].contains(&w.as_str()))
        {
            self.name()?
        } else {
            split_key(&name).1.into()
        };
        Ok(Source { name, alias })
    }
}

// DISTINCT is syntax owned by this query layer; scalar calls retain their
// ordinary argument tree so dependency and authorization walkers see every input.
fn preprocess_aggregates(p: &mut Parser) -> R<()> {
    let mut n = p.at;
    while n + 2 < p.tokens.len() {
        if is_subquery_open(&p.tokens, n) {
            n = subquery_end(&p.tokens, n)? + 1;
            continue;
        }
        let a = match &p.tokens[n] {
            Token::Word(name) | Token::QuotedIdent(name) => Aggregate::from_name(name),
            _ => None,
        };
        let qualified = n > 0 && p.tokens[n - 1] == Token::Symbol('.');
        let catalog = qualified
            && n >= 2
            && matches!(&p.tokens[n - 2], Token::Word(w) | Token::QuotedIdent(w) if w == "pg_catalog");
        if let Some(a) = a
            && p.tokens[n + 1] == Token::Symbol('(')
            && matches!(&p.tokens[n + 2], Token::Word(w) if w == "distinct")
        {
            if matches!(p.tokens.get(n + 3), None | Some(Token::Symbol('*' | ')'))) {
                return Err(SqlError::Syntax);
            }
            if !qualified || catalog {
                p.tokens[n] = Token::Word(a.with_distinct().call_name());
            }
            p.tokens.remove(n + 2);
        }
        n += 1;
    }
    Ok(())
}
fn reject_nested_aggregate(s: &Scalar) -> R<()> {
    if grouping::contains_aggregate(s) {
        Err(SqlError::GroupingError)
    } else {
        s.reject_nested_aggregate()
    }
}
fn filter_contains_aggregate(f: &Filter) -> bool {
    match f {
        Filter::Clauses(c) => {
            c.filter.as_ref().is_some_and(filter_contains_aggregate)
                || c.group.iter().any(grouping::contains_aggregate)
                || c.having.as_ref().is_some_and(filter_contains_aggregate)
        }
        Filter::Scalar(s) | Filter::Null(s, _) | Filter::InSubquery { value: s, .. } => {
            grouping::contains_aggregate(s)
        }
        Filter::Compare(a, _, b) => {
            grouping::contains_aggregate(a) || grouping::contains_aggregate(b)
        }
        Filter::Like {
            value,
            pattern,
            escape,
            ..
        } => {
            grouping::contains_aggregate(value)
                || grouping::contains_aggregate(pattern)
                || escape.as_ref().is_some_and(grouping::contains_aggregate)
        }
        Filter::Not(f) | Filter::Is(f, _, _) => filter_contains_aggregate(f),
        Filter::Logic(a, _, b) => filter_contains_aggregate(a) || filter_contains_aggregate(b),
    }
}
fn scalar_has_undefined_routine(s: &Scalar) -> bool {
    if !grouping::contains_aggregate(s) {
        return s.has_undefined_routine();
    }
    match s {
        Scalar::Call(n, args) if Aggregate::from_name(n).is_some() => {
            args.iter().any(scalar_has_undefined_routine)
        }
        Scalar::Call(n, args) => {
            functions::is_deferred_routine_name(n) || args.iter().any(scalar_has_undefined_routine)
        }
        Scalar::Unary(_, a) | Scalar::Cast(a, _) => scalar_has_undefined_routine(a),
        Scalar::Binary(_, a, b) => {
            scalar_has_undefined_routine(a) || scalar_has_undefined_routine(b)
        }
        Scalar::SubstringOrder(args) => args.iter().any(|a| scalar_has_undefined_routine(a)),
        Scalar::Predicate(f) => f.has_undefined_routine(),
        Scalar::Case(c) => {
            c.arms
                .iter()
                .any(|(f, s)| f.has_undefined_routine() || scalar_has_undefined_routine(s))
                || c.otherwise
                    .as_ref()
                    .is_some_and(scalar_has_undefined_routine)
        }
        Scalar::Literal(_) | Scalar::Param(_) | Scalar::Column(_) => false,
    }
}
impl Query {
    fn walk_scalars(&self, visit: &mut impl FnMut(&Scalar) -> R<()>) -> R<()> {
        for (item, _) in &self.items {
            match item {
                Item::Scalar(s) | Item::Aggregate(_, Some(s)) => walk_scalar(s, visit)?,
                _ => {}
            }
        }
        for j in &self.joins {
            j.on.walk_scalars(visit)?;
        }
        if let Some(f) = &self.filter {
            f.walk_scalars(visit)?;
        }
        for (s, _, _) in &self.order {
            walk_scalar(s, visit)?;
        }
        Ok(())
    }
    fn has_volatile_call(&self) -> bool {
        let mut found = false;
        let _ = self.walk_scalars(&mut |s| {
            found |= s.has_volatile_call();
            Ok(())
        });
        found
    }
    pub(super) fn has_in_subquery(&self) -> bool {
        let mut found = false;
        if let Some(f) = &self.filter {
            let _ = f.visit_subqueries(&mut |_, _| {
                found = true;
                Ok(())
            });
        }
        found
    }
    pub(super) fn subquery_tables(&self) -> Vec<String> {
        let mut out = Vec::new();
        if let Some(f) = &self.filter {
            let _ = f.visit_subqueries(&mut |_, q| {
                for name in q.read_tables() {
                    if !out.contains(&name) {
                        out.push(name);
                    }
                }
                Ok(())
            });
        }
        out
    }
    pub(super) fn read_tables(&self) -> Vec<String> {
        let mut out = Vec::new();
        for name in self
            .sources
            .iter()
            .map(|s| s.name.clone())
            .chain(self.subquery_tables())
        {
            if !out.contains(&name) {
                out.push(name);
            }
        }
        out
    }
    fn used_parameters(&self) -> BTreeSet<usize> {
        let mut out = BTreeSet::new();
        let _ = self.walk_scalars(&mut |s| {
            if let Scalar::Param(n) = s {
                out.insert(*n);
            }
            Ok(())
        });
        self.limit_parameters(&mut out);
        out
    }
    fn limit_parameters(&self, out: &mut BTreeSet<usize>) {
        for e in self.limit.iter().chain(self.offset()) {
            if let Expr::Param(n) = e {
                out.insert(*n);
            }
        }
        if let Some(f) = self.where_filter() {
            let _ = f.visit_subqueries(&mut |_, q| {
                q.limit_parameters(out);
                Ok(())
            });
        }
    }
    fn bind_subqueries<'a>(
        &'a self,
        state: &'a Relational,
        parameters: &mut [Option<DataType>],
        outer: &[Source],
        resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
        tables: &[&TableDef],
    ) -> R<Vec<(usize, BoundQuery<'a>)>> {
        let mut out = Vec::new();
        if let Some(f) = self.where_filter() {
            f.visit_subqueries(&mut |value, q| {
                if let [(Item::Scalar(Scalar::Param(n)), _)] = q.items.as_slice()
                    && parameters.get(n - 1).is_some_and(Option::is_none)
                {
                    let ty = value.bind(resolve, tables, parameters).ok().map(|v| v.ty).unwrap_or(DataType::Text);
                    parameters[*n - 1] = Some(ty);
                }
                let used = q.used_parameters();
                let seed = parameters
                    .iter()
                    .enumerate()
                    .map(|(n, p)| {
                        if p.is_none() && !used.contains(&(n + 1)) {
                            Some(DataType::Text)
                        } else {
                            p.clone()
                        }
                    })
                    .collect();
                let scopes: Vec<_> = self.sources.iter().chain(outer).cloned().collect();
                let bound = q.bind_seeded_inner(state, seed, false, &scopes)?;
                if bound.columns.len() != 1 {
                    return Err(SqlError::Syntax);
                }
                if !matches!(value, Scalar::Param(n) if parameters.get(n - 1).is_some_and(Option::is_some)) {
                    value.infer(Some(&bound.columns[0].data_type), parameters, resolve, tables)?;
                }
                for n in used {
                    if let Some(slot) = parameters.get_mut(n - 1) {
                        *slot = Some(bound.parameters[n - 1].clone());
                    }
                }
                out.push((q as *const Query as usize, bound));
                Ok(())
            })?;
        }
        Ok(out)
    }
    fn distinct(&self) -> bool {
        self.clauses().is_some_and(|c| c.distinct)
    }
    fn set_distinct(&mut self) {
        if let Some(Filter::Clauses(c)) = &mut self.filter {
            c.distinct = true;
        } else {
            self.filter = Some(Filter::Clauses(Box::new(QueryClauses {
                distinct: true,
                filter: self.filter.take(),
                group: vec![],
                having: None,
                offset: None,
            })));
        }
    }
    fn clauses(&self) -> Option<&QueryClauses> {
        match &self.filter {
            Some(Filter::Clauses(c)) => Some(c),
            _ => None,
        }
    }
    fn where_filter(&self) -> Option<&Filter> {
        match &self.filter {
            Some(Filter::Clauses(c)) => c.filter.as_ref(),
            f => f.as_ref(),
        }
    }
    fn group(&self) -> &[Scalar] {
        self.clauses().map_or(&[], |c| &c.group)
    }
    fn having(&self) -> Option<&Filter> {
        self.clauses().and_then(|c| c.having.as_ref())
    }
    fn offset(&self) -> Option<&Expr> {
        self.clauses().and_then(|c| c.offset.as_ref())
    }
    fn parse_items(p: &mut Parser) -> R<Vec<(Item, Option<String>)>> {
        let mut items = Vec::new();
        loop {
            let item_start = p.at;
            let mut item = if p.sym('*') {
                Item::Star
            } else {
                let aggregate = match p.tokens.get(p.at) {
                    Some(Token::Word(w)) if p.tokens.get(p.at + 1) == Some(&Token::Symbol('(')) => {
                        Aggregate::from_name(w)
                    }
                    _ => None,
                };
                if let Some(a) = aggregate {
                    p.at += 1;
                    p.punct('(')?;
                    let c = if p.sym('*') {
                        if a.distinct() {
                            return Err(SqlError::Syntax);
                        }
                        if !matches!(a.base(), Aggregate::Count) {
                            return Err(SqlError::Unsupported);
                        }
                        None
                    } else {
                        Some(Scalar::parse_expr(p, 0, true)?)
                    };
                    p.punct(')')?;
                    Item::Aggregate(a, c)
                } else {
                    Item::Scalar(Scalar::parse_expr(p, 0, false)?)
                }
            };
            if matches!(item, Item::Aggregate(..))
                && (matches!(p.tokens.get(p.at), Some(Token::Op(op)) if ["+", "-", "*", "/", "%", "||", "=", "<>", "!=", "<", "<=", ">", ">="].contains(&op.as_str()))
                    || matches!(p.tokens.get(p.at), Some(Token::Word(w)) if ["and", "or", "is", "not", "between", "in", "like", "ilike"].contains(&w.as_str())))
            {
                let mut skip_until = item_start;
                for n in item_start..p.tokens.len().saturating_sub(3) {
                    if n < skip_until {
                        continue;
                    }
                    if is_subquery_open(&p.tokens, n) {
                        skip_until = subquery_end(&p.tokens, n)? + 1;
                        continue;
                    }
                    if matches!(&p.tokens[n], Token::Word(w) if w == "count")
                        && p.tokens[n + 1] == Token::Symbol('(')
                        && p.tokens[n + 2] == Token::Symbol('*')
                        && p.tokens[n + 3] == Token::Symbol(')')
                    {
                        p.tokens[n + 2] = Token::Number("1".into());
                    }
                }
                p.at = item_start;
                item = Item::Scalar(Scalar::parse_expr(p, 0, true)?);
            }
            let alias = if p.word("as") { Some(p.name()?) } else { None };
            items.push((item, alias));
            if items.len() > MAX_COLUMNS {
                return Err(SqlError::Limit);
            }
            if !p.sym(',') {
                break;
            }
        }
        Ok(items)
    }
    pub(super) fn mutation_target(p: &mut Parser, table: &str) -> R<Self> {
        admit_subquery_depth(&p.tokens[p.at..])?;
        admit_subquery_forms(&p.tokens[p.at..])?;
        let filter = if p.word("where") {
            Some(Filter::parse(p, 0, 0)?)
        } else {
            None
        };
        if let Some(f) = &filter {
            f.admit_subquery_positions(true)?;
        }
        let mut predicates = vec![];
        if let Some(filter) = &filter {
            filter.bounds(&mut predicates);
        }
        Ok(Self {
            explain: false,
            sources: vec![Source {
                name: table.into(),
                alias: split_key(table).1.into(),
            }],
            joins: vec![],
            items: vec![(Item::Star, None)],
            predicates,
            filter,
            order: vec![],
            limit: None,
        })
    }
    /// Infer WHERE parameters without requiring unrelated mutation slots to be typed.
    pub(super) fn infer_mutation_parameters(
        &self,
        state: &Relational,
        parameters: &mut [Option<DataType>],
        declared: &[Option<DataType>],
    ) -> R<()> {
        if self.sources.len() != 1 || !self.joins.is_empty() {
            return Err(SqlError::Unsupported);
        }
        if parameters.len() > MAX_PARAMS {
            return Err(SqlError::Limit);
        }
        let source = &self.sources[0];
        let table = state
            .tables
            .get(&source.name)
            .ok_or(SqlError::UnknownTable)?;
        let resolve = |column: &ColumnRef, _scope: usize| -> R<BoundColumn> {
            if column
                .qualifier
                .as_ref()
                .is_some_and(|name| name != &source.alias)
            {
                return Err(SqlError::UnknownTable);
            }
            let column = table
                .columns
                .iter()
                .position(|c| c.name == column.name)
                .ok_or(SqlError::UnknownColumn)?;
            Ok(BoundColumn { source: 0, column })
        };
        if self.has_in_subquery()
            && let Some(f) = self.where_filter()
        {
            f.infer_outer(parameters, &resolve, &[table])?;
        }
        let subqueries = self.bind_subqueries(state, parameters, &[], &resolve, &[table])?;
        if let Some(filter) = self.where_filter() {
            if self.has_in_subquery() {
                return filter.infer_with_subqueries(parameters, &resolve, &[table], &subqueries);
            }
            // Infer independently of SET to retain undeclared column conflicts.
            // Plain comparisons preserve explicit declarations, including widths.
            let mut inferred = vec![None; parameters.len()];
            let inference = filter.infer_mutation(&mut inferred, declared, &resolve, &[table]);
            for (parameter, inferred) in parameters.iter_mut().zip(inferred) {
                if let Some(inferred) = inferred {
                    if parameter.as_ref().is_some_and(|old| old != &inferred) {
                        return Err(SqlError::Type);
                    }
                    *parameter = Some(inferred);
                }
            }
            match inference {
                Ok(()) | Err(SqlError::AmbiguousFunction | SqlError::IndeterminateDatatype) => {}
                Err(error) => return Err(error),
            }
            // SET can resolve a parameter-only WHERE expression. Unresolved
            // slots remain unresolved here and are rejected by full binding.
            filter.infer(parameters, &resolve, &[table])?;
        }
        Ok(())
    }
    pub(super) fn returning(p: &mut Parser, table: &str) -> R<Self> {
        if p.tokens[p.at..].windows(2).any(
            |w| matches!((&w[0], &w[1]), (Token::Symbol('('), Token::Word(s)) if s == "select"),
        ) {
            return Err(SqlError::Unsupported);
        }
        preprocess_aggregates(p)?;
        let items = Self::parse_items(p)?;
        for (item, _) in &items {
            match item {
                Item::Aggregate(..) => return Err(SqlError::GroupingError),
                Item::Scalar(value) => reject_nested_aggregate(value)?,
                Item::Star => {}
            }
        }
        Ok(Self {
            explain: false,
            sources: vec![Source {
                name: table.into(),
                alias: split_key(table).1.into(),
            }],
            joins: vec![],
            items,
            predicates: vec![],
            filter: None,
            order: vec![],
            limit: None,
        })
    }
    pub(super) fn parse_insert_source(p: &mut Parser) -> R<Self> {
        let distinct = p.tokens.get(p.at + 1) == Some(&Token::Word("distinct".into()));
        if distinct {
            p.tokens.remove(p.at + 1);
        }
        let mut query = Self::parse(p)?;
        if distinct {
            query.set_distinct();
        }
        Ok(query)
    }
    pub(super) fn parse(p: &mut Parser) -> R<Self> {
        admit_subquery_depth(&p.tokens[p.at..])?;
        admit_subquery_forms(&p.tokens[p.at..])?;
        let explain = p.word("explain");
        p.need("select")?;
        preprocess_aggregates(p)?;
        let aggregate_filters = aggregate_filter::normalize(p)?;
        let mut items = Self::parse_items(p)?;
        let mut sources = vec![];
        if p.word("from") {
            sources.push(p.source()?);
        }
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
            let on = Filter::parse(p, 0, 0)?;
            joins.push(Join { left, on });
        }
        let mut filter = if p.word("where") {
            Some(Filter::parse(p, 0, 0)?)
        } else {
            None
        };
        let mut predicates = vec![];
        if let Some(f) = &filter {
            f.bounds(&mut predicates);
        }
        let mut group = vec![];
        if p.word("group") {
            p.need("by")?;
            loop {
                group.push(Scalar::parse_aggregate_argument(p, 0)?);
                if group.len() > MAX_COLUMNS {
                    return Err(SqlError::Limit);
                }
                if !p.sym(',') {
                    break;
                }
            }
        }
        // Scalar's ordinary parser deliberately rejects count(*). In post-group
        // clauses count(*) has the same transition input as count(1).
        let mut skip_until = p.at;
        for n in p.at..p.tokens.len().saturating_sub(3) {
            if n < skip_until {
                continue;
            }
            if is_subquery_open(&p.tokens, n) {
                skip_until = subquery_end(&p.tokens, n)? + 1;
                continue;
            }
            if matches!(&p.tokens[n], Token::Word(w) if w == "count")
                && p.tokens[n + 1] == Token::Symbol('(')
                && p.tokens[n + 2] == Token::Symbol('*')
                && p.tokens[n + 3] == Token::Symbol(')')
            {
                p.tokens[n + 2] = Token::Number("1".into());
            }
        }
        let mut having = if p.word("having") {
            Some(Filter::parse(p, 0, 0)?)
        } else {
            None
        };
        let mut order = vec![];
        if p.word("order") {
            p.need("by")?;
            loop {
                let c = Scalar::parse_expr(p, 0, false)?;
                let desc = p.word("desc");
                if !desc {
                    p.word("asc");
                }
                let nulls_first = if p.word("nulls") {
                    if p.word("first") {
                        true
                    } else {
                        p.need("last")?;
                        false
                    }
                } else {
                    desc
                };
                order.push((c, desc, nulls_first));
                if order.len() > MAX_COLUMNS {
                    return Err(SqlError::Limit);
                }
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
        let offset = if p.word("offset") {
            Some(p.expr()?)
        } else {
            None
        };
        if !aggregate_filters.is_empty() {
            for (item, _) in &mut items {
                match item {
                    Item::Scalar(s) | Item::Aggregate(_, Some(s)) => {
                        aggregate_filter::expand_scalar(s, &aggregate_filters)?
                    }
                    _ => {}
                }
            }
            for f in filter.iter_mut().chain(having.iter_mut()) {
                aggregate_filter::expand_filter(f, &aggregate_filters)?;
            }
            for s in &mut group {
                aggregate_filter::expand_scalar(s, &aggregate_filters)?;
            }
            for (s, _, _) in &mut order {
                aggregate_filter::expand_scalar(s, &aggregate_filters)?;
            }
        }
        for (item, _) in &items {
            match item {
                Item::Scalar(s) | Item::Aggregate(_, Some(s)) => reject_scalar_subquery(s)?,
                _ => {}
            }
        }
        for join in &joins {
            join.on.admit_subquery_positions(false)?;
        }
        if let Some(f) = &filter {
            f.admit_subquery_positions(true)?;
        }
        if let Some(f) = &having {
            f.admit_subquery_positions(false)?;
        }
        for s in &group {
            reject_scalar_subquery(s)?;
        }
        for (s, _, _) in &order {
            reject_scalar_subquery(s)?;
        }
        // OFFSET is admitted by this work item only for grouped output.
        // Preserve the existing rejection boundary for ordinary row queries.
        if offset.is_some()
            && group.is_empty()
            && having.is_none()
            && !items
                .iter()
                .any(|(item, _)| matches!(item, Item::Aggregate(..)))
            && !order
                .iter()
                .any(|(s, _, _)| grouping::contains_aggregate(s))
        {
            return Err(SqlError::Unsupported);
        }
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
            filter: if group.is_empty() && having.is_none() && offset.is_none() {
                filter
            } else {
                Some(Filter::Clauses(Box::new(QueryClauses {
                    distinct: false,
                    filter,
                    group,
                    having,
                    offset,
                })))
            },
            order,
            limit,
        })
    }
    fn bind_joins(
        &self,
        tables: &[&TableDef],
        parameters: &mut [Option<DataType>],
        resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
    ) -> R<Vec<BoundJoin>> {
        let mut joins = vec![];
        for (j, join) in self.joins.iter().enumerate() {
            let right = j + 1;
            let mut keys = BTreeMap::new();
            let mut equalities = vec![];
            let scoped = |c: &ColumnRef, _: usize| resolve(c, right + 1);
            if filter_contains_aggregate(&join.on) {
                return Err(SqlError::GroupingError);
            }
            join.on.infer(parameters, &scoped, tables)?;
            let on = join.on.bind(parameters, &scoped, tables)?;
            for (a, b) in join.equalities() {
                let a = resolve(a, right + 1)?;
                let b = resolve(b, right + 1)?;
                let (outer, inner) = if a.source == right && b.source < right {
                    (b, a)
                } else if b.source == right && a.source < right {
                    (a, b)
                } else {
                    continue;
                };
                let outer_ty = &tables[outer.source].columns[outer.column].data_type;
                let inner_ty = &tables[inner.source].columns[inner.column].data_type;
                if !functions::comparable(outer_ty, inner_ty) {
                    return Err(functions::comparison_mismatch(outer_ty, inner_ty));
                }
                if tables[right].primary_key.contains(&inner.column)
                    && functions::join_key_supported(outer_ty, inner_ty)
                {
                    keys.entry(inner.column)
                        .or_insert((outer, outer_ty.clone(), inner_ty.clone()));
                }
                equalities.push((outer, inner, outer_ty.clone(), inner_ty.clone()));
            }
            let key = tables[right]
                .primary_key
                .iter()
                .map(|i| keys.get(i).cloned())
                .collect::<Option<Vec<_>>>();
            let scan = key.is_none();
            let key = key.unwrap_or_default();
            joins.push(BoundJoin {
                left: join.left,
                scan,
                key,
                equalities,
                on,
                clock: None,
            });
        }
        Ok(joins)
    }
    /// Positive proof for the current bounded AST. Unknown calls keep a target.
    /// No stored functions, subqueries or CTEs are admitted by this proof.
    pub(super) fn is_table_free(&self) -> bool {
        fn scalar(s: &Scalar) -> bool {
            match s {
                Scalar::Literal(_) | Scalar::Param(_) => true,
                Scalar::Column(_) => false,
                Scalar::Cast(s, _) | Scalar::Unary(_, s) => scalar(s),
                Scalar::Binary(_, a, b) => scalar(a) && scalar(b),
                Scalar::SubstringOrder(args) => args.iter().all(|s| scalar(s)),
                Scalar::Predicate(f) => filter(f),
                Scalar::Case(c) => {
                    c.arms.iter().all(|(f, s)| filter(f) && scalar(s))
                        && c.otherwise.as_ref().is_none_or(scalar)
                }
                Scalar::Call(name, args) if name == aggregate_filter::CALL => {
                    let [call, condition] = args.as_slice() else {
                        return false;
                    };
                    (matches!(call, Scalar::Call(name, args) if name == aggregate_filter::COUNT_STAR && args.is_empty())
                        || scalar(call))
                        && aggregate_filter::decode(condition).is_ok_and(|f| filter(&f))
                }
                Scalar::Call(name, args) if name == functions::CAST_TYPMOD => {
                    args.iter().all(scalar)
                }
                Scalar::Call(name, args) => {
                    matches!(
                        name.as_str(),
                        "abs"
                            // Clock functions read only the per-execution StatementClock.
                            | "now"
                            | "transaction_timestamp"
                            | "statement_timestamp"
                            | "clock_timestamp"
                            | "current_timestamp"
                            | "localtimestamp"
                            | "current_date"
                            | "current_time"
                            | "localtime"
                            | "coalesce"
                            | "format"
                            | "greatest"
                            | "least"
                            | "pi"
                            | "lower"
                            | "upper"
                            | "md5"
                            | "overlay"
                            | "ascii"
                            | "chr"
                            | "initcap"
                            | "quote_ident"
                            | "quote_literal"
                            | "quote_nullable"
                            | "to_hex"
                            | "to_bin"
                            | "to_oct"
                            | "length"
                            | "char_length"
                            | "character_length"
                            | "octet_length"
                            | "bit_length"
                            | "reverse"
                            | "starts_with"
                            | "strpos"
                            | "position"
                            | "repeat"
                            | "left"
                            | "right"
                            | "replace"
                            | "translate"
                            | "lpad"
                            | "rpad"
                            | "btrim"
                            | "ltrim"
                            | "rtrim"
                            | "split_part"
                            | "concat"
                            | "concat_ws"
                            | "substring"
                            | "substr"
                            | "encode"
                            | "decode"
                            | "get_byte"
                            | "get_bit"
                            | "set_byte"
                            | "set_bit"
                            | "sha224"
                            | "sha256"
                            | "sha384"
                            | "sha512"
                            | "ceil"
                            | "ceiling"
                            | "floor"
                            | "round"
                            | "trunc"
                            | "sign"
                            | "sqrt"
                            | "exp"
                            | "ln"
                            | "log"
                            | "log10"
                            | "power"
                            | "scale"
                            | "min_scale"
                            | "trim_scale"
                            | "factorial"
                            | "div"
                            | "mod"
                            | "gcd"
                            | "lcm"
                            | "cbrt"
                            | "radians"
                            | "degrees"
                            | "sin"
                            | "cos"
                            | "tan"
                            | "cot"
                            | "asin"
                            | "acos"
                            | "atan"
                            | "atan2"
                            | "sind"
                            | "cosd"
                            | "tand"
                            | "cotd"
                            | "asind"
                            | "acosd"
                            | "atand"
                            | "atan2d"
                            | "sinh"
                            | "cosh"
                            | "tanh"
                            | "asinh"
                            | "acosh"
                            | "atanh"
                            | "date_part"
                            | "extract"
                            | "isfinite"
                            | "date_trunc"
                            | "date_bin"
                            | "age"
                            | "timezone"
                            | "make_date"
                            | "make_time"
                            | "make_timestamp"
                            | "make_timestamptz"
                            | "make_interval"
                            | "justify_days"
                            | "justify_hours"
                            | "justify_interval"
                    ) && args.iter().all(scalar)
                        || (Aggregate::from_name(name).is_some() && args.iter().all(scalar))
                }
            }
        }
        fn filter(f: &Filter) -> bool {
            match f {
                Filter::Clauses(c) => {
                    c.filter.as_ref().is_none_or(filter)
                        && c.group.iter().all(scalar)
                        && c.having.as_ref().is_none_or(filter)
                }
                Filter::InSubquery { value, query, .. } => scalar(value) && query.is_table_free(),
                Filter::Scalar(s) | Filter::Null(s, _) => scalar(s),
                Filter::Compare(a, _, b) => scalar(a) && scalar(b),
                Filter::Like {
                    value,
                    pattern,
                    escape,
                    ..
                } => scalar(value) && scalar(pattern) && escape.as_ref().is_none_or(scalar),
                Filter::Not(f) | Filter::Is(f, _, _) => filter(f),
                Filter::Logic(a, _, b) => filter(a) && filter(b),
            }
        }
        // Exhaustive destructuring forces review when dependency-bearing AST changes.
        let Self {
            explain,
            sources,
            joins,
            items,
            predicates,
            filter: predicate,
            order,
            limit,
        } = self;
        !explain
            && sources.is_empty()
            && joins.iter().all(|j| filter(&j.on))
            && joins.is_empty()
            && predicates.is_empty()
            && items.iter().all(|(item, _)| match item {
                Item::Scalar(s) => scalar(s),
                Item::Aggregate(_, arg) => arg.as_ref().is_none_or(scalar),
                Item::Star => false,
            })
            && predicate.as_ref().is_none_or(filter)
            && order.iter().all(|(s, _, _)| scalar(s))
            && limit.as_ref().is_none_or(|e| match e {
                Expr::Literal(_) | Expr::Param(_) => true,
            })
    }
    /// DD-0068 F9 (GD-0001 Q8, E1048/E1050): table-free SELECT that is not table-free only because of
    /// deferred undefined or unknown-schema routine calls (pg_toast excluded), so binding fails with
    /// 42883/3F000 before any execution. Classification only; grants nothing by itself.
    pub(super) fn deferred_routine_only(&self) -> bool {
        fn scalar(s: &Scalar) -> bool {
            match s {
                Scalar::Literal(_) | Scalar::Param(_) => true,
                Scalar::Column(_) => false,
                Scalar::Cast(s, _) | Scalar::Unary(_, s) => scalar(s),
                Scalar::Binary(_, a, b) => scalar(a) && scalar(b),
                Scalar::SubstringOrder(args) => args.iter().all(|s| scalar(s)),
                Scalar::Predicate(f) => filter_ok(f),
                Scalar::Case(c) => {
                    c.arms.iter().all(|(f, s)| filter_ok(f) && scalar(s))
                        && c.otherwise.as_ref().is_none_or(scalar)
                }
                Scalar::Call(name, args) => {
                    (!functions::is_deferred_routine_name(name)
                        || functions::deferred_routine_binds_for_sql_role(name))
                        && args.iter().all(scalar)
                }
            }
        }
        fn filter_ok(f: &Filter) -> bool {
            match f {
                Filter::Clauses(c) => {
                    c.filter.as_ref().is_none_or(filter_ok)
                        && c.group.iter().all(scalar)
                        && c.having.as_ref().is_none_or(filter_ok)
                }
                Filter::InSubquery { value, query, .. } => scalar(value) && query.is_table_free(),
                Filter::Scalar(s) | Filter::Null(s, _) => scalar(s),
                Filter::Compare(a, _, b) => scalar(a) && scalar(b),
                Filter::Like {
                    value,
                    pattern,
                    escape,
                    ..
                } => scalar(value) && scalar(pattern) && escape.as_ref().is_none_or(scalar),
                Filter::Not(f) | Filter::Is(f, _, _) => filter_ok(f),
                Filter::Logic(a, _, b) => filter_ok(a) && filter_ok(b),
            }
        }
        // Exhaustive destructuring forces review when dependency-bearing AST changes.
        let Self {
            explain,
            sources,
            joins,
            items,
            predicates,
            filter,
            order,
            limit,
        } = self;
        self.subquery_tables().is_empty()
            && !explain
            && sources.is_empty()
            && joins.iter().all(|j| filter_ok(&j.on))
            && joins.is_empty()
            && predicates.is_empty()
            && self.sequence_dependencies().is_empty()
            && self.has_undefined_routine()
            && items.iter().all(|(item, _)| match item {
                Item::Scalar(s) | Item::Aggregate(_, Some(s)) => scalar(s),
                Item::Aggregate(_, None) => true,
                Item::Star => false,
            })
            && filter.as_ref().is_none_or(filter_ok)
            && order.iter().all(|(s, _, _)| scalar(s))
            && limit.as_ref().is_none_or(|e| match e {
                Expr::Literal(_) | Expr::Param(_) => true,
            })
    }
    pub(super) fn sequence_only(&self) -> bool {
        self.sources.is_empty()
            && self.subquery_tables().is_empty()
            && !self.sequence_dependencies().is_empty()
    }
    pub(super) fn sequence_dependencies(&self) -> Vec<Option<String>> {
        fn filter(f: &Filter, out: &mut Vec<Option<String>>) {
            match f {
                Filter::Clauses(c) => {
                    if let Some(f) = &c.filter {
                        filter(f, out);
                    }
                    for s in &c.group {
                        s.sequence_dependencies(out);
                    }
                    if let Some(f) = &c.having {
                        filter(f, out);
                    }
                }
                Filter::InSubquery { value, query, .. } => {
                    value.sequence_dependencies(out);
                    out.extend(query.sequence_dependencies());
                }
                Filter::Scalar(s) | Filter::Null(s, _) => s.sequence_dependencies(out),
                Filter::Compare(a, _, b) => {
                    a.sequence_dependencies(out);
                    b.sequence_dependencies(out);
                }
                Filter::Like {
                    value,
                    pattern,
                    escape,
                    ..
                } => {
                    value.sequence_dependencies(out);
                    pattern.sequence_dependencies(out);
                    if let Some(escape) = escape {
                        escape.sequence_dependencies(out);
                    }
                }
                Filter::Not(f) | Filter::Is(f, _, _) => filter(f, out),
                Filter::Logic(a, _, b) => {
                    filter(a, out);
                    filter(b, out);
                }
            }
        }
        let mut out = Vec::new();
        for join in &self.joins {
            filter(&join.on, &mut out);
        }
        for (item, _) in &self.items {
            match item {
                Item::Scalar(s) | Item::Aggregate(_, Some(s)) => s.sequence_dependencies(&mut out),
                _ => {}
            }
        }
        if let Some(f) = &self.filter {
            filter(f, &mut out);
        }
        for (s, _, _) in &self.order {
            s.sequence_dependencies(&mut out);
        }
        out
    }
    fn has_undefined_routine(&self) -> bool {
        fn filter(f: &Filter) -> bool {
            match f {
                Filter::Clauses(c) => {
                    c.filter.as_ref().is_some_and(filter)
                        || c.group.iter().any(scalar_has_undefined_routine)
                        || c.having.as_ref().is_some_and(filter)
                }
                Filter::InSubquery { value, query, .. } => {
                    scalar_has_undefined_routine(value) || query.has_undefined_routine()
                }
                Filter::Scalar(s) | Filter::Null(s, _) => scalar_has_undefined_routine(s),
                Filter::Compare(a, _, b) => {
                    scalar_has_undefined_routine(a) || scalar_has_undefined_routine(b)
                }
                Filter::Like {
                    value,
                    pattern,
                    escape,
                    ..
                } => {
                    scalar_has_undefined_routine(value)
                        || scalar_has_undefined_routine(pattern)
                        || escape.as_ref().is_some_and(scalar_has_undefined_routine)
                }
                Filter::Not(f) | Filter::Is(f, _, _) => filter(f),
                Filter::Logic(a, _, b) => filter(a) || filter(b),
            }
        }
        self.items.iter().any(|(item, _)| match item {
            Item::Scalar(s) | Item::Aggregate(_, Some(s)) => scalar_has_undefined_routine(s),
            _ => false,
        }) || self.joins.iter().any(|j| filter(&j.on))
            || self.where_filter().is_some_and(filter)
            || self.group().iter().any(scalar_has_undefined_routine)
            || self.having().is_some_and(filter)
            || self
                .order
                .iter()
                .any(|(s, _, _)| scalar_has_undefined_routine(s))
    }
    /// Negative diagnostic classification only; this never grants table-free access.
    pub(super) fn rejects_unsupported_table_free_scalar(&self) -> bool {
        fn scalar(s: &Scalar) -> bool {
            match s {
                Scalar::Literal(_) | Scalar::Param(_) => true,
                Scalar::Column(_) => false,
                Scalar::Cast(s, _) | Scalar::Unary(_, s) => scalar(s),
                Scalar::Binary(_, a, b) => scalar(a) && scalar(b),
                Scalar::Call(_, args) => args.iter().all(scalar),
                Scalar::Predicate(f) => filter(f),
                Scalar::Case(c) => {
                    c.arms.iter().all(|(f, s)| filter(f) && scalar(s))
                        && c.otherwise.as_ref().is_none_or(scalar)
                }
                Scalar::SubstringOrder([first, second, third]) => {
                    scalar(first) && scalar(second) && scalar(third)
                }
            }
        }
        fn operand(s: &Scalar) -> bool {
            reject_nested_aggregate(s).is_ok() && scalar(s)
        }
        fn filter(f: &Filter) -> bool {
            match f {
                Filter::Clauses(c) => {
                    c.filter.as_ref().is_none_or(filter)
                        && c.group.iter().all(operand)
                        && c.having.as_ref().is_none_or(filter)
                }
                Filter::InSubquery { value, query, .. } => operand(value) && query.is_table_free(),
                Filter::Scalar(s) | Filter::Null(s, _) => operand(s),
                Filter::Compare(a, _, b) => operand(a) && operand(b),
                Filter::Like {
                    value,
                    pattern,
                    escape,
                    ..
                } => operand(value) && operand(pattern) && escape.as_ref().is_none_or(operand),
                Filter::Not(f) | Filter::Is(f, _, _) => filter(f),
                Filter::Logic(a, _, b) => filter(a) && filter(b),
            }
        }
        // Keep the authoritative positive proof and its routine registry unchanged.
        // Every dependency-bearing AST field must be reviewed when the AST grows.
        let Self {
            explain,
            sources,
            joins,
            items,
            predicates,
            filter: predicate,
            order,
            limit,
        } = self;
        !self.has_undefined_routine()
            && self.sequence_dependencies().is_empty()
            && !self.is_table_free()
            && !explain
            && sources.is_empty()
            && joins.iter().all(|j| filter(&j.on))
            && joins.is_empty()
            && predicates.is_empty()
            && items.iter().all(|(item, _)| match item {
                Item::Scalar(s) => operand(s),
                Item::Aggregate(_, _) | Item::Star => false,
            })
            && predicate.as_ref().is_none_or(filter)
            && order.iter().all(|(s, _, _)| operand(s))
            && limit.as_ref().is_none_or(|e| match e {
                Expr::Literal(_) | Expr::Param(_) => true,
            })
    }
    // Reuse the parser's mandatory AND bounds; residual SQL is still evaluated.
    pub(super) fn v2_index_predicates(&self, params: &[Value]) -> Vec<(String, String, Value)> {
        if self.sources.len() != 1 || !self.joins.is_empty() || self.has_in_subquery() {
            return Vec::new();
        }
        self.predicates
            .iter()
            .filter_map(|(column, op, expr)| {
                let value = match expr {
                    Expr::Literal(value) => value.clone(),
                    Expr::Param(n) => params.get(n.checked_sub(1)?)?.clone(),
                };
                Some((column.name.clone(), op.clone(), value))
            })
            .collect()
    }
    pub(super) fn target(&self) -> &str {
        if !self.sources.is_empty() && self.sources.iter().all(|s| s.name == self.sources[0].name) {
            &self.sources[0].name
        } else {
            // Existing table-limited gateways must not authorize only the first source.
            "giggles_join"
        }
    }
    pub(super) fn bind<'a>(
        &'a self,
        state: &'a Relational,
        count: usize,
        declared: &[Option<DataType>],
    ) -> R<BoundQuery<'a>> {
        self.bind_declared(state, count, declared, false)
    }
    pub(super) fn bind_description<'a>(
        &'a self,
        state: &'a Relational,
        count: usize,
        declared: &[Option<DataType>],
    ) -> R<BoundQuery<'a>> {
        self.bind_declared(state, count, declared, true)
    }
    fn bind_declared<'a>(
        &'a self,
        state: &'a Relational,
        count: usize,
        declared: &[Option<DataType>],
        classify_plain_columns: bool,
    ) -> R<BoundQuery<'a>> {
        if count > MAX_PARAMS {
            return Err(SqlError::Limit);
        }
        if declared.len() > count {
            return Err(SqlError::Parameters);
        }
        let mut parameters = vec![None; count];
        parameters[..declared.len()].clone_from_slice(declared);
        let mut bound = self.bind_seeded_inner(state, parameters, classify_plain_columns, &[])?;
        bound.declared = declared.to_vec();
        Ok(bound)
    }
    pub(super) fn bind_seeded<'a>(
        &'a self,
        state: &'a Relational,
        parameters: Vec<Option<DataType>>,
    ) -> R<BoundQuery<'a>> {
        self.bind_seeded_inner(state, parameters, false, &[])
    }
    pub(super) fn bind_projection_seeded<'a>(
        &'a self,
        state: &'a Relational,
        parameters: Vec<Option<DataType>>,
    ) -> R<BoundQuery<'a>> {
        self.bind_seeded_inner(state, parameters, true, &[])
    }
    /// Preserve unknown INSERT output literals without treating expanded columns as unknown.
    pub(super) fn insert_outputs(&self, state: &Relational) -> R<Vec<Option<Value>>> {
        if !self.is_table_free() && state.sequence_by_name(self.target()).is_some() {
            return Err(SqlError::Unsupported);
        }
        let mut outputs = Vec::new();
        for (item, _) in &self.items {
            match item {
                Item::Star => {
                    for source in &self.sources {
                        let table = state
                            .tables
                            .get(&source.name)
                            .ok_or(SqlError::UnknownTable)?;
                        outputs.extend(table.logical_columns().map(|_| None));
                    }
                }
                Item::Scalar(Scalar::Literal(value @ (Value::Text(_) | Value::Null))) => {
                    outputs.push(Some(value.clone()));
                }
                _ => outputs.push(None),
            }
        }
        Ok(outputs)
    }
    /// Only a bare output parameter with no other source context takes its target type.
    pub(super) fn bind_insert_source<'a>(
        &'a self,
        state: &'a Relational,
        targets: &[DataType],
        parameters: &mut [Option<DataType>],
        declared: &[Option<DataType>],
    ) -> R<BoundQuery<'a>> {
        let outputs = self.insert_outputs(state)?;
        if outputs.len() != targets.len() {
            return Err(SqlError::Syntax);
        }
        let mut contextual = self.clone();
        for (item, _) in &mut contextual.items {
            if matches!(item, Item::Scalar(Scalar::Param(_))) {
                *item = Item::Scalar(Scalar::Literal(Value::Null));
            }
        }
        let contextual = contextual.used_parameters();
        let mut seed = parameters.to_vec();
        for n in &contextual {
            if declared.get(n - 1).is_none_or(Option::is_none) {
                *seed.get_mut(n - 1).ok_or(SqlError::Parameters)? = None;
            }
        }
        let mut output = 0;
        for (item, _) in &self.items {
            if let Item::Scalar(Scalar::Param(n)) = item
                && !contextual.contains(n)
                && declared.get(n - 1).is_none_or(Option::is_none)
            {
                let slot = seed.get_mut(n - 1).ok_or(SqlError::Parameters)?;
                if slot.as_ref().is_some_and(|old| old != &targets[output]) {
                    return Err(SqlError::Type);
                }
                *slot = Some(targets[output].clone());
            }
            output += if matches!(item, Item::Star) {
                self.sources
                    .iter()
                    .map(|source| {
                        state
                            .tables
                            .get(&source.name)
                            .map(|table| table.logical_columns().count())
                            .ok_or(SqlError::UnknownTable)
                    })
                    .collect::<R<Vec<_>>>()?
                    .into_iter()
                    .sum()
            } else {
                1
            };
        }
        let used = self.used_parameters();
        // Other INSERT clauses can own parameter slots absent from the SELECT.
        for (index, slot) in seed.iter_mut().enumerate() {
            if !used.contains(&(index + 1)) {
                slot.get_or_insert(DataType::Text);
            }
        }
        let mut bound = self.bind_seeded_inner(state, seed, false, &[])?;
        // INSERT source ordinals name expanded output positions, including stars.
        // Grouped queries already resolve ordinals in their grouping plan.
        if bound.grouping.is_none() {
            let mut projection = Vec::new();
            for (item, _) in &self.items {
                match item {
                    Item::Scalar(scalar) => projection.push(Some(scalar)),
                    Item::Star => {
                        let width = bound
                            .tables
                            .iter()
                            .map(|table| table.logical_columns().count())
                            .sum();
                        projection.extend(std::iter::repeat_n(None, width));
                    }
                    Item::Aggregate(..) => projection.push(None),
                }
            }
            let tables = &bound.tables;
            let resolve = |column: &ColumnRef, scope: usize| -> R<BoundColumn> {
                if column.qualifier.as_ref().is_some_and(|qualifier| {
                    !self
                        .sources
                        .iter()
                        .take(scope)
                        .any(|source| &source.alias == qualifier)
                }) {
                    return Err(SqlError::UnknownTable);
                }
                let mut found = None;
                for (source, (syntax, table)) in
                    self.sources.iter().zip(tables).take(scope).enumerate()
                {
                    if column
                        .qualifier
                        .as_ref()
                        .is_some_and(|qualifier| qualifier != &syntax.alias)
                    {
                        continue;
                    }
                    if let Some(column) = table
                        .logical_offsets()
                        .into_iter()
                        .find(|index| table.columns[*index].name == column.name)
                    {
                        if found.is_some() {
                            return Err(SqlError::Duplicate);
                        }
                        found = Some(BoundColumn { source, column });
                    }
                }
                found.ok_or(SqlError::UnknownColumn)
            };
            let parameter_types = bound
                .parameters
                .iter()
                .cloned()
                .map(Some)
                .collect::<Vec<_>>();
            for (index, (scalar, _, _)) in self.order.iter().enumerate() {
                if let Scalar::Literal(Value::Int(position)) = scalar {
                    let position = usize::try_from(*position)
                        .ok()
                        .and_then(|n| n.checked_sub(1))
                        .ok_or(SqlError::Syntax)?;
                    let item = bound.items.get(position).ok_or(SqlError::Syntax)?;
                    bound.order[index].0 = match item {
                        BoundItem::Scalar(_) => projection
                            .get(position)
                            .and_then(|scalar| *scalar)
                            .ok_or(SqlError::Unsupported)?
                            .bind(&resolve, tables, &parameter_types)?,
                        BoundItem::Column(column) => Scalar::Column(ColumnRef {
                            qualifier: None,
                            name: String::new(),
                        })
                        .bind(
                            &|_, _| Ok(*column),
                            &bound.tables,
                            &parameter_types,
                        )?,
                        BoundItem::Aggregate(..) => return Err(SqlError::Unsupported),
                    };
                }
            }
        }
        for ((column, unknown), target) in bound.columns.iter().zip(outputs).zip(targets) {
            if let Some(value) = unknown {
                if !functions::zone_dependent_literal(&value, target) {
                    assignment::literal(value, target, None)?;
                }
            } else if !assignment::compatible(&column.data_type, target) {
                return Err(SqlError::Type);
            }
        }
        for n in used {
            if parameters[n - 1]
                .as_ref()
                .is_some_and(|old| old != &bound.parameters[n - 1])
            {
                return Err(SqlError::Type);
            }
            parameters[n - 1] = Some(bound.parameters[n - 1].clone());
        }
        bound.declared = declared.to_vec();
        Ok(bound)
    }
    fn bind_seeded_inner<'a>(
        &'a self,
        state: &'a Relational,
        mut parameters: Vec<Option<DataType>>,
        classify_plain_columns: bool,
        outer: &[Source],
    ) -> R<BoundQuery<'a>> {
        if parameters.len() > MAX_PARAMS {
            return Err(SqlError::Limit);
        }
        let declared_bytea_projection = parameters.contains(&Some(DataType::Bytes))
            && !self.explain
            && self.sources.is_empty()
            && self.joins.is_empty()
            && self.predicates.is_empty()
            && self.filter.is_none()
            && self.group().is_empty()
            && self.having().is_none()
            && self.offset().is_none()
            && self.order.is_empty()
            && self.limit.is_none()
            && self
                .items
                .iter()
                .all(|(item, _)| matches!(item, Item::Scalar(Scalar::Param(_))));
        unique(self.sources.iter().map(|s| &s.alias))?;
        let tables = self
            .sources
            .iter()
            .map(|s| state.tables.get(&s.name).ok_or(SqlError::UnknownTable))
            .collect::<R<Vec<_>>>()?;
        let missing = |c: &ColumnRef, error| {
            let correlated = if let Some(q) = &c.qualifier {
                !self.sources.iter().any(|s| q == &s.alias)
                    && outer.iter().any(|s| q == &s.alias || q == &s.name)
            } else {
                let has_column = |source: &Source| {
                    state.tables.get(&source.name).is_some_and(|t| {
                        t.columns
                            .iter()
                            .enumerate()
                            .any(|(n, column)| column.name == c.name && t.hidden_rowid() != Some(n))
                    })
                };
                !self.sources.iter().any(has_column) && outer.iter().any(has_column)
            };
            if correlated {
                SqlError::Unsupported
            } else {
                error
            }
        };
        let resolve = |c: &ColumnRef, scope: usize| -> R<BoundColumn> {
            if c.qualifier
                .as_ref()
                .is_some_and(|q| !self.sources.iter().take(scope).any(|s| q == &s.alias))
            {
                return Err(missing(c, SqlError::UnknownTable));
            }
            let mut found = None;
            for (source, (s, t)) in self.sources.iter().zip(&tables).take(scope).enumerate() {
                if c.qualifier.as_ref().is_some_and(|q| q != &s.alias) {
                    continue;
                }
                if let Some(column) = t.columns.iter().enumerate().find_map(|(n, v)| {
                    (v.name == c.name && t.hidden_rowid() != Some(n)).then_some(n)
                }) {
                    if found.is_some() {
                        return Err(SqlError::Duplicate);
                    }
                    found = Some(BoundColumn { source, column });
                }
            }
            found.ok_or_else(|| missing(c, SqlError::UnknownColumn))
        };
        if self.has_in_subquery()
            && let Some(f) = self.where_filter()
        {
            f.infer_outer(&mut parameters, &resolve, &tables)?;
        }
        let subqueries = self.bind_subqueries(state, &mut parameters, outer, &resolve, &tables)?;
        if let Some(filter) = self.where_filter() {
            aggregate_filter::validate_filter(filter, &parameters, &resolve, &tables)?;
        }
        if self.where_filter().is_some_and(filter_contains_aggregate) {
            return Err(SqlError::GroupingError);
        }
        let grouped = self
            .items
            .iter()
            .any(|(i, _)| matches!(i, Item::Scalar(s) if grouping::contains_aggregate(s)))
            || !self.group().is_empty()
            || self.having().is_some()
            || self
                .order
                .iter()
                .any(|(s, _, _)| grouping::contains_aggregate(s))
            || (self
                .items
                .iter()
                .any(|(i, _)| matches!(i, Item::Aggregate(..)))
                && (!self.order.is_empty()
                    || self
                        .items
                        .iter()
                        .any(|(i, _)| !matches!(i, Item::Aggregate(..)))))
            || (self.sources.is_empty()
                && self
                    .items
                    .iter()
                    .any(|(i, _)| matches!(i, Item::Aggregate(..))));
        for (j, join) in self.joins.iter().enumerate() {
            let scoped = |c: &ColumnRef, _: usize| resolve(c, j + 2);
            if filter_contains_aggregate(&join.on) {
                return Err(SqlError::GroupingError);
            }
            aggregate_filter::validate_filter(&join.on, &parameters, &scoped, &tables)?;
            join.on.infer(&mut parameters, &scoped, &tables)?;
        }
        if grouped {
            return grouping::bind(self, &tables, parameters, &resolve, subqueries);
        }
        let mut items = vec![];
        let mut unresolved_count_parameters = Vec::new();
        for (item, _) in &self.items {
            match item {
                Item::Scalar(e) => e.infer(None, &mut parameters, &resolve, &tables)?,
                Item::Aggregate(a, Some(e)) => {
                    reject_nested_aggregate(e)?;
                    if matches!(a.base(), Aggregate::Count)
                        && let Scalar::Param(n) = e
                        && parameters.get(n - 1).is_none_or(Option::is_none)
                    {
                        unresolved_count_parameters.push(*n);
                    }
                    if matches!(a.base(), Aggregate::Sum | Aggregate::Avg)
                        && e.aggregate_unknown(&parameters)
                    {
                        return Err(SqlError::AmbiguousFunction);
                    }
                    let expected = if matches!(a.base(), Aggregate::Min | Aggregate::Max)
                        && e.aggregate_unknown(&parameters)
                    {
                        Some(&DataType::Text)
                    } else {
                        None
                    };
                    e.infer(expected, &mut parameters, &resolve, &tables)?;
                }
                _ => {}
            }
        }
        if let Some(f) = self.where_filter() {
            f.infer_with_subqueries(&mut parameters, &resolve, &tables, &subqueries)?;
        }
        for (e, _, _) in &self.order {
            e.infer(None, &mut parameters, &resolve, &tables)?;
        }
        let bind_expr = |e: &Expr, ty: &DataType, parameters: &mut [Option<DataType>]| -> R<bool> {
            if let Expr::Param(n) = e {
                let slot = parameters.get_mut(n - 1).ok_or(SqlError::Parameters)?;
                if let Some(old) = slot.as_ref().filter(|old| *old != ty) {
                    if functions::comparable(old, ty) {
                        // Cross-type comparisons run in the full filter. A
                        // converted column key can round and miss matching rows.
                        return Ok(false);
                    }
                    return Err(functions::comparison_mismatch(old, ty));
                }
                slot.get_or_insert_with(|| ty.clone());
            } else if let Expr::Literal(v) = e {
                if functions::zone_dependent_literal(v, ty) {
                    return Ok(false);
                }
                if !matches!(v, Value::Null | Value::Text(_)) {
                    let literal = Scalar::Literal(v.clone()).bind(&resolve, &tables, parameters)?;
                    if literal.ty != *ty && functions::comparable(&literal.ty, ty) {
                        return Ok(false);
                    }
                }
                if let Value::Int(value) = v
                    && match ty {
                        DataType::SmallInt => i16::try_from(*value).is_err(),
                        DataType::Integer => i32::try_from(*value).is_err(),
                        _ => false,
                    }
                {
                    // Keep out-of-range integer comparisons in the full filter.
                    return Ok(false);
                }
                let coerced;
                let v = if matches!(v, Value::Text(_)) && *ty == DataType::TimestampTz {
                    coerced = functions::coerce_clocked(v.clone(), ty, None)?;
                    &coerced
                } else {
                    v
                };
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
            Ok(true)
        };
        let mut predicates = vec![];
        for (c, op, e) in &self.predicates {
            let c = resolve(c, tables.len())?;
            if tables[c.source].columns[c.column].data_type == DataType::Bpchar {
                return Err(SqlError::Unsupported);
            }
            if let Expr::Literal(value) = e
                && !matches!(value, Value::Null | Value::Text(_))
            {
                let literal =
                    Scalar::Literal(value.clone()).bind(&resolve, &tables, &parameters)?;
                let column_type = &tables[c.source].columns[c.column].data_type;
                if literal.ty != *column_type && !functions::comparable(&literal.ty, column_type) {
                    return Err(functions::comparison_mismatch(&literal.ty, column_type));
                }
            }
            if let Expr::Literal(value @ Value::Text(_)) = e
                && let Some(Some(_)) = unknown_comparison_literal(
                    value,
                    &tables[c.source].columns[c.column].data_type,
                )?
            {
                // Key bounds resolve the literal the same way (relational::bounds).
                predicates.push((c, op.as_str(), e));
                continue;
            }
            if bind_expr(
                e,
                &tables[c.source].columns[c.column].data_type,
                &mut parameters,
            )? {
                predicates.push((c, op.as_str(), e));
            }
        }
        for e in self.limit.iter().chain(self.offset()) {
            // LIMIT accepts the integer widths by implicit widening. Keep an
            // input type already fixed by the SELECT list.
            if let Expr::Param(n) = e {
                let ty = parameters.get_mut(n - 1).ok_or(SqlError::Parameters)?;
                match ty {
                    Some(
                        DataType::SmallInt
                        | DataType::Integer
                        | DataType::BigInt
                        | DataType::Numeric
                        | DataType::Real
                        | DataType::DoublePrecision,
                    ) => {}
                    None => *ty = Some(DataType::BigInt),
                    _ => return Err(SqlError::Type),
                }
            } else {
                bind_expr(e, &DataType::BigInt, &mut parameters)?;
            }
        }
        if unresolved_count_parameters
            .iter()
            .any(|n| parameters.get(n - 1).is_some_and(Option::is_some))
        {
            return Err(SqlError::AmbiguousParameter);
        }
        // Only a plain projection seeded with bytea needs the catalog fallback's
        // text default. Inferred bytea and other query shapes keep their errors.
        if declared_bytea_projection {
            for (item, _) in &self.items {
                if let Item::Scalar(Scalar::Param(n)) = item {
                    parameters[*n - 1].get_or_insert(DataType::Text);
                }
            }
        }
        let joins = self.bind_joins(&tables, &mut parameters, &resolve)?;
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
                Item::Scalar(e) => {
                    let e = e.bind(&resolve, &tables, &parameters)?;
                    let (source_column, d) = if let Item::Scalar(Scalar::Column(c)) = item {
                        let c = resolve(c, tables.len())?;
                        let mut d = tables[c.source].columns[c.column].clone();
                        if c.source > 0 && self.joins[c.source - 1].left {
                            d.not_null = false;
                        }
                        (Some(c), d)
                    } else {
                        let d = ColumnDef {
                            id: 0,
                            name: e.name().into(),
                            data_type: e.ty.clone(),
                            not_null: false,
                        };
                        (None, d)
                    };
                    let item = match source_column {
                        Some(column) if classify_plain_columns => BoundItem::Column(column),
                        _ => BoundItem::Scalar(e),
                    };
                    push(item, d);
                }
                Item::Aggregate(a, c) => {
                    if matches!(a.base(), Aggregate::Count)
                        && let Some(Scalar::Param(n)) = c
                        && parameters.get(n - 1).is_none_or(Option::is_none)
                    {
                        return Err(SqlError::IndeterminateDatatype);
                    }
                    let legacy_column = matches!(c, Some(Scalar::Column(_)));
                    let c = c
                        .as_ref()
                        .map(|e| e.bind(&resolve, &tables, &parameters))
                        .transpose()?;
                    let ty = c.as_ref().map(|e| e.ty.clone());
                    if ty == Some(DataType::Bpchar) {
                        return Err(SqlError::Unsupported);
                    }
                    if matches!(a.base(), Aggregate::Sum | Aggregate::Avg)
                        && !matches!(
                            ty,
                            Some(
                                DataType::SmallInt
                                    | DataType::Integer
                                    | DataType::BigInt
                                    | DataType::Numeric
                                    | DataType::Real
                                    | DataType::DoublePrecision
                            )
                        )
                    {
                        return Err(if legacy_column && !matches!(a.base(), Aggregate::Avg) {
                            SqlError::Type
                        } else {
                            SqlError::UndefinedFunction
                        });
                    }
                    if !legacy_column
                        && matches!(a.base(), Aggregate::Min | Aggregate::Max)
                        && matches!(ty, Some(DataType::Boolean | DataType::Bytes))
                    {
                        return Err(SqlError::UndefinedFunction);
                    }
                    let data_type = if matches!(a.base(), Aggregate::Avg) {
                        match ty {
                            Some(DataType::Real | DataType::DoublePrecision) => {
                                DataType::DoublePrecision
                            }
                            _ => DataType::Numeric,
                        }
                    } else if matches!(a.base(), Aggregate::Sum) {
                        match ty {
                            Some(DataType::BigInt | DataType::Numeric) => DataType::Numeric,
                            Some(DataType::Real) => DataType::Real,
                            Some(DataType::DoublePrecision) => DataType::DoublePrecision,
                            _ => DataType::BigInt,
                        }
                    } else if matches!(a.base(), Aggregate::Count) {
                        DataType::BigInt
                    } else {
                        ty.ok_or(SqlError::Type)?
                    };
                    push(
                        BoundItem::Aggregate(*a, c),
                        ColumnDef {
                            id: 0,
                            name: a.name().into(),
                            data_type,
                            not_null: matches!(a.base(), Aggregate::Count),
                        },
                    );
                }
                Item::Star => {
                    if tables.is_empty() {
                        return Err(SqlError::Unsupported);
                    }
                    if alias.is_some() {
                        return Err(SqlError::Unsupported);
                    }
                    for (source, t) in tables.iter().enumerate() {
                        for (column, d) in t.columns.iter().enumerate() {
                            if t.hidden_rowid() == Some(column) {
                                continue;
                            }
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
            && (items.iter().any(|i| !matches!(i, BoundItem::Aggregate(..)))
                || !self.order.is_empty())
        {
            return Err(SqlError::Unsupported);
        }
        let filter = self
            .where_filter()
            .map(|f| f.bind_with_subqueries(&parameters, &resolve, &tables, &subqueries))
            .transpose()?;
        let order = self
            .order
            .iter()
            .map(|(e, d, n)| {
                let ordinal;
                let e = if let Scalar::Literal(Value::Int(k)) = e {
                    ordinal = self.order_ordinal(*k, &tables)?;
                    &ordinal
                } else {
                    e
                };
                let bound = e
                    .bind(&resolve, &tables, &parameters)?
                    .enum_sort(state, &tables);
                if bound.ty == DataType::Bpchar {
                    return Err(SqlError::Unsupported);
                }
                Ok((bound, *d, *n))
            })
            .collect::<R<Vec<_>>>()?;
        let parameters = parameters
            .into_iter()
            .map(|p| p.ok_or(SqlError::Parameters))
            .collect::<R<Vec<_>>>()?;
        Ok(BoundQuery {
            query: self,
            tables,
            joins,
            items,
            columns,
            parameters,
            predicates,
            filter,
            order,
            aggregate,
            grouping: None,
            subqueries,
            declared: vec![],
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct BoundColumn {
    source: usize,
    column: usize,
}
enum BoundItem {
    Scalar(BoundScalar),
    Column(BoundColumn),
    Aggregate(Aggregate, Option<BoundScalar>),
}
struct BoundJoin {
    left: bool,
    scan: bool,
    key: Vec<(BoundColumn, DataType, DataType)>,
    equalities: Vec<(BoundColumn, BoundColumn, DataType, DataType)>,
    on: BoundFilter,
    clock: Option<super::clock::ExecutionClock>,
}
pub(super) struct BoundQuery<'a> {
    query: &'a Query,
    tables: Vec<&'a TableDef>,
    joins: Vec<BoundJoin>,
    items: Vec<BoundItem>,
    columns: Vec<ColumnDef>,
    parameters: Vec<DataType>,
    predicates: Vec<(BoundColumn, &'a str, &'a Expr)>,
    filter: Option<BoundFilter>,
    order: Vec<(BoundScalar, bool, bool)>,
    aggregate: bool,
    grouping: Option<grouping::Grouping>,
    subqueries: Vec<(usize, BoundQuery<'a>)>,
    declared: Vec<Option<DataType>>,
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
/// Inner-row candidate visits allowed per statement across all joins. Each visit
/// polls cancellation, so this bounds work, not latency; output and retention
/// caps apply separately.
pub(super) const MAX_JOIN_VISITS: usize = 1 << 24;
// Depth-first streaming expansion retains one candidate per joined source. The
// statement budget caps candidate visits over consumed input, including point hits.
fn join_rows<'a>(
    query: &'a BoundQuery<'_>,
    source: &'a dyn scan::RowSource,
    input: Box<dyn Iterator<Item = R<scan::SourceRow<'a>>> + 'a>,
    params: &'a [Value],
    meter: &'a mut scan::Meter<'_>,
    visits: &'a Cell<usize>,
) -> impl Iterator<Item = R<Tuple>> + 'a {
    struct Frame<'a> {
        rows: Box<dyn Iterator<Item = R<scan::SourceRow<'a>>> + 'a>,
        matched: bool,
        extended: bool,
    }
    let mut frames = vec![Frame {
        rows: input,
        matched: false,
        extended: false,
    }];
    let mut row = Tuple {
        values: vec![],
        version: 0,
    };
    let mut done = false;
    std::iter::from_fn(move || {
        if done {
            return None;
        }
        let next = (|| -> R<Option<Tuple>> {
            loop {
                let Some(depth) = frames.len().checked_sub(1) else {
                    return Ok(None);
                };
                row.values.truncate(depth);
                let candidate = frames[depth].rows.next();
                if let Some(candidate) = candidate {
                    meter.tick()?;
                    if depth > 0 {
                        charge_candidate(visits)?;
                    }
                    let candidate = candidate?;
                    if depth == 0 {
                        row.version = candidate.row.version;
                    }
                    let equality_match = depth == 0
                        || query.joins[depth - 1].matches(&row, &candidate.row.values)?;
                    row.values.push(candidate.row.values.clone());
                    if depth > 0 {
                        let mut work = 0;
                        if !equality_match
                            || query.joins[depth - 1].on.eval(&row, params, &mut work)?
                                != Some(true)
                        {
                            continue;
                        }
                        row.version = 0;
                    }
                    frames[depth].matched = true;
                } else if depth > 0
                    && !frames[depth].matched
                    && !frames[depth].extended
                    && query.joins[depth - 1].left
                {
                    frames[depth].extended = true;
                    row.values
                        .push(vec![Value::Null; query.tables[depth].columns.len()]);
                    row.version = 0;
                } else {
                    frames.pop();
                    continue;
                }
                if depth == query.joins.len() {
                    return Ok(Some(Tuple {
                        values: row.values.clone(),
                        version: row.version,
                    }));
                }
                let join = &query.joins[depth];
                let table = query.tables[depth + 1];
                let rows: Box<dyn Iterator<Item = R<scan::SourceRow<'a>>> + 'a> = if join.scan {
                    source.scan(
                        table,
                        &scan::KeyRange {
                            lo: std::ops::Bound::Unbounded,
                            hi: std::ops::Bound::Unbounded,
                        },
                        scan::Direction::Forward,
                    )?
                } else {
                    let candidate = match join.key(&row)? {
                        Some(key) => source.get(table, &encode_key(&key)?)?,
                        None => None,
                    };
                    Box::new(candidate.into_iter().map(Ok))
                };
                frames.push(Frame {
                    rows,
                    matched: false,
                    extended: false,
                });
            }
        })();
        match next {
            Ok(Some(row)) => Some(Ok(row)),
            Ok(None) => {
                done = true;
                None
            }
            Err(e) => {
                done = true;
                Some(Err(e))
            }
        }
    })
}
fn cmp(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Numeric(a), Value::Numeric(b)) => a.cmp(b),
        (Value::Float4(a), Value::Float4(b)) => a.cmp(b),
        (Value::Float8(a), Value::Float8(b)) => a.cmp(b),
        (Value::Int(a), Value::Int(b)) => a.cmp(b),
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Text(a), Value::Text(b)) => a.as_bytes().cmp(b.as_bytes()),
        (Value::Bytes(a), Value::Bytes(b)) => a.cmp(b),
        (Value::Date(a), Value::Date(b)) => a.cmp(b),
        (Value::Time(a), Value::Time(b)) => a.cmp(b),
        (Value::TimeTz(a), Value::TimeTz(b)) => a.cmp(b),
        (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
        (Value::TimestampTz(a), Value::TimestampTz(b)) => a.cmp(b),
        (Value::Interval(a), Value::Interval(b)) => a.cmp(b),

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
    if *bytes > scan::result_limit() {
        return Err(SqlError::Limit);
    }
    Ok(())
}
impl BoundQuery<'_> {
    fn project_tuple(
        &self,
        row: &Tuple,
        params: &[Value],
        work: &mut usize,
        bytes: &mut usize,
    ) -> R<SqlRow> {
        let values = self
            .items
            .iter()
            .map(|item| match item {
                BoundItem::Scalar(value) => {
                    let value = value.eval(row, params, work)?;
                    charge(bytes, &value)?;
                    Ok(value)
                }
                BoundItem::Column(column) => {
                    let value = row.get(*column);
                    charge(bytes, value)?;
                    Ok(value.clone())
                }
                BoundItem::Aggregate(..) => Err(SqlError::GroupingError),
            })
            .collect::<R<Vec<_>>>()?;
        Ok(SqlRow {
            values,
            version: row.version,
        })
    }
    pub(super) fn project_row(
        &self,
        values: &[Value],
        version: u64,
        params: &[Value],
        work: &mut usize,
        bytes: &mut usize,
    ) -> R<Vec<Value>> {
        payload_bound(values.iter())?;
        Ok(self
            .project_tuple(
                &Tuple {
                    values: vec![values.to_vec()],
                    version,
                },
                params,
                work,
                bytes,
            )?
            .values)
    }
    pub(super) fn validate_immutable_constants(&self) -> R<()> {
        let mut budget = 0;
        if let Some(grouping) = &self.grouping {
            grouping.validate_constants(self.parameters.is_empty(), &mut budget)?;
        }
        if self.parameters.is_empty() {
            for join in &self.joins {
                join.on.plan_constant(&mut budget)?;
            }
        }
        for item in &self.items {
            match item {
                BoundItem::Scalar(value) => {
                    value.plan_constant(&mut budget)?;
                }
                BoundItem::Aggregate(_, Some(value)) if self.parameters.is_empty() => {
                    value.plan_constant(&mut budget)?;
                }
                _ => {}
            }
        }
        for (value, _, _) in &self.order {
            value.plan_constant(&mut budget)?;
        }
        if self.parameters.is_empty()
            && let Some(filter) = &self.filter
        {
            filter.plan_constant(&mut budget)?;
        }
        Ok(())
    }
    pub(super) fn description(&self, state: &Relational) -> Description {
        Description {
            type_oids: vec![],
            parameters: self.parameters.clone(),
            typmods: if self.query.explain {
                vec![result_typmod(state, TypmodSource::Other)]
            } else if let Some(grouping) = &self.grouping {
                grouping.typmods(state, &self.tables)
            } else {
                self.items
                    .iter()
                    .map(|item| {
                        let source = match item {
                            BoundItem::Column(column) => TypmodSource::Column(
                                &self.tables[column.source].columns[column.column],
                            ),
                            BoundItem::Scalar(value) => TypmodSource::Scalar(value),
                            BoundItem::Aggregate(..) => TypmodSource::Other,
                        };
                        result_typmod(state, source)
                    })
                    .collect()
            },
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
    fn base_bounds(&self, params: &[Value]) -> R<(bool, Bounds)> {
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
        let bounds = match bounds(self.tables[0], &base_predicates, params) {
            // A gap in a composite key, or several bounds on one key column, cannot
            // form one key range. Stream the full range; the complete
            // WHERE filter is always re-applied, so the result is unchanged.
            Err(SqlError::Unbounded | SqlError::Unsupported) => {
                bounds(self.tables[0], &[], params)?
            }
            other => other?,
        };
        Ok((empty_input, bounds))
    }
    pub(super) fn cursor_direction(&self) -> Option<scan::Direction> {
        if self.tables.len() != 1
            || !self.joins.is_empty()
            || self.aggregate
            || self.grouping.is_some()
            || self.query.distinct()
            || self.query.explain
            || self.query.has_in_subquery()
        {
            return None;
        }
        if self.order.is_empty() {
            return Some(scan::Direction::Forward);
        }
        let desc = self.order[0].1;
        if self
            .order
            .iter()
            .enumerate()
            .all(|(i, (value, reverse, _))| {
                let scalar = &self.query.order[i].0;
                let ordinal;
                let scalar = if let Scalar::Literal(Value::Int(n)) = scalar {
                    let Ok(resolved) = self.query.order_ordinal(*n, &self.tables) else {
                        return false;
                    };
                    ordinal = resolved;
                    &ordinal
                } else {
                    scalar
                };
                *reverse == desc
                    && matches!(scalar, Scalar::Column(c)
                if self.tables[0].primary_key.get(i).is_some_and(|index|
                    self.tables[0].columns[*index].name == c.name
                    && self.tables[0].columns[*index].data_type == value.ty))
            })
        {
            Some(if desc {
                scan::Direction::Reverse
            } else {
                scan::Direction::Forward
            })
        } else {
            None
        }
    }
    pub(super) fn fetch_cursor(
        &self,
        source: &dyn scan::RowSource,
        params: &[Value],
        progress: &mut super::cursor::Progress,
        max_rows: usize,
        max_bytes: usize,
    ) -> R<Vec<SqlRow>> {
        use std::ops::Bound;
        let direction = self.cursor_direction().ok_or(SqlError::Unsupported)?;
        let volatile = self.query.has_volatile_call();
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
        if !progress.initialized {
            let number = |expr: Option<&Expr>, default, numeric: bool| -> R<Option<u64>> {
                let mut value = expr.map(|e| e.value(params)).transpose()?;
                if numeric
                    && let Some(Expr::Param(n)) = expr
                    && self.declared.get(n - 1) == Some(&Some(DataType::Numeric))
                    && let Some(v) = value.take()
                {
                    value = Some(assignment::convert(
                        v,
                        &DataType::Numeric,
                        &DataType::BigInt,
                        None,
                    )?);
                }
                match value {
                    Some(Value::Int(n)) if n >= 0 => Ok(Some(n as u64)),
                    None | Some(Value::Null) => Ok(default),
                    _ => Err(SqlError::Limit),
                }
            };
            progress.skip = number(self.query.offset(), Some(0), false)?.unwrap_or(0);
            progress.remaining = number(self.query.limit.as_ref(), None, true)?;
            progress.initialized = true;
        }
        let mut output = Vec::new();
        let mut bytes = 0;
        if let Some(row) = progress.pending.take() {
            bytes = row.values.iter().map(value_bytes).sum();
            if bytes > max_bytes {
                progress.pending = Some(row);
                return Err(SqlError::Limit);
            }
            output.push(row);
        }
        if progress.done {
            return Ok(output);
        }
        let (empty, bounds) = self.base_bounds(params)?;
        if empty || progress.remaining == Some(0) {
            progress.done = true;
            return Ok(output);
        }
        let mut range = scan::KeyRange::from_bounds(bounds);
        if let Some(key) = &progress.after {
            match direction {
                scan::Direction::Forward => range.lo = Bound::Excluded(key.clone()),
                scan::Direction::Reverse => range.hi = Bound::Excluded(key.clone()),
            }
        }
        let mut input = source.scan(self.tables[0], &range, direction)?;
        loop {
            crate::cancellation::check().map_err(|_| SqlError::Aborted)?;
            let Some(row) = input.next() else {
                progress.done = true;
                break;
            };
            let row = row?;
            progress.after = Some(row.key.into_owned());
            let tuple = Tuple {
                values: vec![row.row.values.clone()],
                version: row.row.version,
            };
            let mut work = 0;
            if let Some(filter) = &self.filter
                && filter.eval(&tuple, params, &mut work)? != Some(true)
            {
                continue;
            }
            if progress.skip > 0 {
                progress.skip -= 1;
                continue;
            }
            let row = self.project_tuple(&tuple, params, &mut work, &mut 0)?;
            let size = row.values.iter().map(value_bytes).sum::<usize>();
            if let Some(n) = &mut progress.remaining {
                *n -= 1;
            }
            if output.len() == max_rows || bytes.saturating_add(size) > max_bytes {
                progress.pending = Some(row);
                if output.is_empty() {
                    return Err(SqlError::Limit);
                }
                break;
            }
            bytes += size;
            output.push(row);
            if progress.remaining == Some(0) {
                progress.done = true;
                break;
            }
            if volatile {
                break;
            }
        }
        Ok(output)
    }
    /// Collect complete mutation rows in primary-key order, retaining their keys.
    pub(super) fn matching_rows(
        &self,
        state: &Relational,
        source: &dyn scan::RowSource,
        params: &[Value],
        retention: &mut scan::Retention,
        cancel: &mut impl FnMut() -> bool,
    ) -> R<Vec<(Vec<u8>, SqlRow)>> {
        let execution = Execution::new(cancel, &self.declared);
        let mut cancel_impl = || execution.cancelled();
        let cancel = &mut cancel_impl;
        if self.tables.len() != 1 || !self.joins.is_empty() || self.aggregate {
            return Err(SqlError::Unsupported);
        }
        for (value, ty) in params.iter().zip(&self.parameters) {
            check(
                value,
                &ColumnDef {
                    id: 0,
                    name: String::new(),
                    data_type: ty.clone(),
                    not_null: false,
                },
            )?;
        }
        let (empty_input, bounds) = self.base_bounds(params)?;
        let range = if empty_input {
            scan::KeyRange {
                lo: std::ops::Bound::Excluded(Vec::new()),
                hi: std::ops::Bound::Excluded(Vec::new()),
            }
        } else {
            scan::KeyRange::from_bounds(bounds)
        };
        let input = source.scan(self.tables[0], &range, scan::Direction::Forward)?;
        let mut meter = scan::Meter::per_row(cancel);
        let filtered = scan::filtered(input, &mut meter, |row| {
            let mut work = 0;
            let tuple = Tuple {
                values: vec![row.row.values.clone()],
                version: row.row.version,
            };
            if let Some(filter) = &self.filter
                && filter.eval_with_subqueries(
                    &tuple, params, &mut work, self, state, source, &execution,
                )? != Some(true)
            {
                return Ok(None);
            }
            Ok(Some(row))
        });
        let mut rows = Vec::new();
        for row in filtered {
            let row = row?;
            if rows.len() >= scan::query_row_limit().min(super::MAX_ROWS) {
                return Err(SqlError::Limit);
            }
            retention.charge(searched::retained_row_bytes(&row.key, &row.row)?)?;
            rows.push((row.key.into_owned(), row.row.into_owned()));
        }
        Ok(rows)
    }
    #[cfg(test)]
    pub(super) fn execute(
        &self,
        state: &Relational,
        source: &dyn scan::RowSource,
        params: &[Value],
        cancel: &mut impl FnMut() -> bool,
    ) -> R<SqlResult> {
        self.execute_declared(state, source, params, cancel, &[])
    }
    pub(super) fn execute_declared(
        &self,
        state: &Relational,
        source: &dyn scan::RowSource,
        params: &[Value],
        cancel: &mut impl FnMut() -> bool,
        declared: &[Option<DataType>],
    ) -> R<SqlResult> {
        let execution = Execution::new(cancel, declared);
        self.execute_context(state, source, params, declared, &execution, false, false)
    }
    pub(super) fn execute_insert_source(
        &self,
        state: &Relational,
        source: &dyn scan::RowSource,
        params: &[Value],
        cancel: &mut dyn FnMut() -> bool,
        declared: &[Option<DataType>],
    ) -> R<SqlResult> {
        let execution = Execution::new(cancel, declared);
        self.execute_context(state, source, params, declared, &execution, false, true)
    }
    #[expect(
        clippy::too_many_arguments,
        reason = "query execution context and parameter ownership"
    )]
    fn execute_context(
        &self,
        state: &Relational,
        source: &dyn scan::RowSource,
        params: &[Value],
        declared: &[Option<DataType>],
        execution: &Execution<'_>,
        subquery: bool,
        insert_source: bool,
    ) -> R<SqlResult> {
        let mut cancel_impl = || execution.cancelled();
        let cancel = &mut cancel_impl;
        if self.aggregate && cancel() {
            return Err(SqlError::Aborted);
        }
        let used = (subquery || insert_source).then(|| self.query.used_parameters());
        for (index, (v, ty)) in params.iter().zip(&self.parameters).enumerate() {
            if used
                .as_ref()
                .is_some_and(|used| !used.contains(&(index + 1)))
            {
                continue;
            }
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
        if let Some(grouping) = &self.grouping {
            return grouping.execute(self, state, source, params, execution);
        }
        let limit = match &self.query.limit {
            Some(e) => {
                let value = e.value(params)?;
                let value = if matches!(e, Expr::Param(n)
                    if declared.get(n - 1) == Some(&Some(DataType::Numeric)))
                {
                    assignment::convert(value, &DataType::Numeric, &DataType::BigInt, None)?
                } else {
                    value
                };
                match value {
                    Value::Int(n) if n >= 0 => Some(n as u64),
                    Value::Null => None,
                    _ => return Err(SqlError::Limit),
                }
            }
            None => None,
        };
        if self.query.explain && self.tables.is_empty() && self.query.has_in_subquery() {
            return Ok(SqlResult {
                columns: vec!["plan".into()],
                rows: vec![SqlRow {
                    values: vec![Value::Text("TableFree; InSubquery; OutputLimit".into())],
                    version: 0,
                }],
                affected: 0,
            });
        }
        if self.tables.is_empty() {
            if self.aggregate || self.query.explain {
                return Err(SqlError::Unsupported);
            }
            let mut result = SqlResult {
                columns: self.columns.iter().map(|c| c.name.clone()).collect(),
                rows: vec![],
                affected: 0,
            };
            let offset = match self.query.offset() {
                Some(e) => match e.value(params)? {
                    Value::Int(n) if n >= 0 => n as u64,
                    Value::Null => 0,
                    _ => return Err(SqlError::Limit),
                },
                None => 0,
            };
            if limit != Some(0) && offset == 0 {
                let row = Tuple {
                    values: vec![],
                    version: 0,
                };
                let mut work = 0;
                if let Some(f) = &self.filter
                    && f.eval_with_subqueries(
                        &row, params, &mut work, self, state, source, execution,
                    )? != Some(true)
                {
                    return Ok(result);
                }
                for (e, _, _) in &self.order {
                    e.eval(&row, params, &mut work)?;
                }
                let mut output = 0;
                let values = self
                    .items
                    .iter()
                    .map(|i| match i {
                        BoundItem::Scalar(e) => {
                            let v = e.eval(&row, params, &mut work)?;
                            charge(&mut output, &v)?;
                            Ok(v)
                        }
                        _ => Err(SqlError::Unsupported),
                    })
                    .collect::<R<Vec<_>>>()?;
                result.rows.push(SqlRow { values, version: 0 });
            }
            return Ok(result);
        }
        let (empty_input, bounds) = self.base_bounds(params)?;
        if self.query.explain {
            let mut plan = format!(
                "{}; retained_bytes<={}; result_rows<={}; result_bytes<={MAX_RESULT_BYTES}",
                if empty_input {
                    "EmptyInput"
                } else if bounds.point {
                    "PrimaryKeyLookup"
                } else {
                    "PrimaryKeyRangeScan"
                },
                scan::MAX_RETAINED_BYTES,
                scan::MAX_QUERY_ROWS,
            );
            for j in &self.joins {
                plan.push_str(match (j.left, j.scan) {
                    (true, true) => "; LeftScanNestedLoop",
                    (false, true) => "; InnerScanNestedLoop",
                    (true, false) => "; LeftPrimaryKeyNestedLoop",
                    (false, false) => "; InnerPrimaryKeyNestedLoop",
                });
            }
            if self.aggregate {
                plan.push_str("; GlobalAggregate");
            }
            if !self.order.is_empty() {
                plan.push_str(if limit.is_some() {
                    "; TopNSort"
                } else {
                    "; StableSort"
                });
            }
            if self.query.has_in_subquery() {
                plan.push_str("; InSubquery");
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
        // LIMIT 0 yields no rows, so PostgreSQL executes no input, filter, join or
        // aggregate work. Bind-phase immutable constant validation is separate and
        // already ran. Keep the aggregate entry and normal-success cancel fences.
        if limit == Some(0) {
            if self.aggregate && cancel() {
                return Err(SqlError::Aborted);
            }
            return Ok(SqlResult {
                columns: self.columns.iter().map(|c| c.name.clone()).collect(),
                rows: vec![],
                affected: 0,
            });
        }
        let range = if empty_input {
            scan::KeyRange {
                lo: std::ops::Bound::Excluded(Vec::new()),
                hi: std::ops::Bound::Excluded(Vec::new()),
            }
        } else {
            scan::KeyRange::from_bounds(bounds)
        };
        let input = source.scan(self.tables[0], &range, scan::Direction::Forward)?;
        let mut meter = if subquery || self.aggregate || !self.joins.is_empty() {
            scan::Meter::per_row(cancel)
        } else {
            scan::Meter::new(cancel)
        };
        let joined = join_rows(self, source, input, params, &mut meter, &execution.visits);
        let filtered = joined.filter_map(|row| {
            let row = match row {
                Ok(row) => row,
                Err(e) => return Some(Err(e)),
            };
            let mut filter_work = 0;
            match self
                .filter
                .as_ref()
                .map(|f| {
                    f.eval_with_subqueries(
                        &row,
                        params,
                        &mut filter_work,
                        self,
                        state,
                        source,
                        execution,
                    )
                })
                .transpose()
            {
                Ok(None | Some(Some(true))) => Some(Ok(row)),
                Ok(_) => None,
                Err(e) => Some(Err(e)),
            }
        });
        let offset = match self.query.offset() {
            Some(e) => match e.value(params)? {
                Value::Int(n) if n >= 0 => n as u64,
                Value::Null => 0,
                _ => return Err(SqlError::Limit),
            },
            None => 0,
        };
        let page = scan::Page { offset, limit };
        let mut result = SqlResult {
            columns: self.columns.iter().map(|c| c.name.clone()).collect(),
            rows: vec![],
            affected: 0,
        };
        let mut result_bytes = 0usize;
        // Aggregate inputs keep the statement-wide scalar work budget (DD-0068
        // aggregate_scalar_work_budget_is_shared_over_full_input).
        let mut aggregate_work_bytes = 0usize;
        if self.query.distinct() && !self.aggregate {
            let mut seen = std::collections::BTreeSet::new();
            let mut retention = scan::Retention::new(scan::retention_limit());
            let mut unique_rows = Vec::new();
            for row in filtered {
                let row = row?;
                let projected = self.project_tuple(&row, params, &mut 0, &mut 0)?;
                let key = grouping::Key(projected.values.clone());
                if !seen.insert(key) {
                    continue;
                }
                if unique_rows.len() >= scan::query_row_limit() {
                    return Err(SqlError::Limit);
                }
                let keys = self
                    .order
                    .iter()
                    .map(|(s, _, _)| s.eval(&row, params, &mut 0))
                    .collect::<R<Vec<_>>>()?;
                retention.charge(
                    projected.values.iter().map(value_bytes).sum::<usize>() * 2
                        + keys.iter().map(value_bytes).sum::<usize>()
                        + 8 * std::mem::size_of::<Value>(),
                )?;
                unique_rows.push((projected, keys));
            }
            unique_rows.sort_by(|(_, a), (_, b)| order_values(a, b, &self.order));
            for (row, _) in page.apply(unique_rows.into_iter()) {
                for value in &row.values {
                    charge(&mut result_bytes, value)?;
                }
                result.rows.push(row);
            }
            return Ok(result);
        }
        if self.aggregate {
            let mut values = self
                .items
                .iter()
                .map(|item| {
                    if matches!(item, BoundItem::Aggregate(kind, _) if matches!(kind.base(), Aggregate::Count)) {
                        Value::Int(0)
                    } else {
                        Value::Null
                    }
                })
                .collect::<Vec<_>>();
            let mut retention = scan::Retention::new(scan::retention_limit());
            let mut auxiliary = aggregate_auxiliary(&self.items, &mut retention)?;
            retention.charge(aggregate_retained(&values))?;
            for row in filtered {
                let row = row?;
                let before = aggregate_retained(&values);
                update_aggregates(
                    &self.items,
                    &self.columns,
                    &mut values,
                    &mut auxiliary,
                    &mut retention,
                    &row,
                    params,
                    &mut aggregate_work_bytes,
                )?;
                let after = aggregate_retained(&values);
                if after > before {
                    retention.charge(after - before)?;
                } else {
                    retention.release(before - after);
                }
            }
            finish_aggregates(
                &self.items,
                &mut values,
                &auxiliary,
                &mut aggregate_work_bytes,
            )?;
            for value in &values {
                charge(&mut result_bytes, value)?;
            }
            result
                .rows
                .extend(page.apply(std::iter::once(SqlRow { values, version: 0 })));
        } else {
            let rows: Box<dyn Iterator<Item = R<Tuple>> + '_> = if self.order.is_empty() {
                Box::new(filtered)
            } else {
                let keyed = filtered.map(|row| {
                    let row = row?;
                    let mut key_work = 0;
                    let keys = self
                        .order
                        .iter()
                        .map(|(e, _, _)| e.eval(&row, params, &mut key_work))
                        .collect::<R<Vec<_>>>()?;
                    Ok((row, keys))
                });
                let mut retention = scan::Retention::new(scan::retention_limit());
                let sorted = scan::sort_bounded(
                    keyed,
                    page.keep(),
                    |(row, keys)| {
                        row.values
                            .iter()
                            .flatten()
                            .chain(keys)
                            .map(value_bytes)
                            .sum()
                    },
                    |(_, a), (_, b)| {
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
                                    let ord = cmp(a, b);
                                    if *desc { ord.reverse() } else { ord }
                                }
                            };
                            if ord != Ordering::Equal {
                                return ord;
                            }
                        }
                        Ordering::Equal
                    },
                    &mut retention,
                )?;
                Box::new(sorted.into_iter().map(|(row, _)| Ok(row)))
            };
            for row in page.apply(rows) {
                let row = row?;
                let mut scalar_work_bytes = 0usize;
                if result.rows.len() >= scan::query_row_limit() {
                    return Err(SqlError::Limit);
                }
                result.rows.push(self.project_tuple(
                    &row,
                    params,
                    &mut scalar_work_bytes,
                    &mut result_bytes,
                )?);
            }
        }
        if self.aggregate && cancel() {
            return Err(SqlError::Aborted);
        }
        Ok(result)
    }
}

impl Query {
    /// PG18: an integer literal in ORDER BY names that select-list output
    /// (1-based, `*` expanded), not a constant sort key.
    fn order_ordinal(&self, k: i64, tables: &[&TableDef]) -> R<Scalar> {
        let mut n = 0i64;
        for (item, _) in &self.items {
            match item {
                Item::Star => {
                    for (source, table) in self.sources.iter().zip(tables) {
                        for (i, column) in table.columns.iter().enumerate() {
                            if table.hidden_rowid() == Some(i) {
                                continue;
                            }
                            n += 1;
                            if n == k {
                                return Ok(Scalar::Column(ColumnRef {
                                    qualifier: Some(source.alias.clone()),
                                    name: column.name.clone(),
                                }));
                            }
                        }
                    }
                }
                Item::Scalar(s) => {
                    n += 1;
                    if n == k {
                        return Ok(s.clone());
                    }
                }
                Item::Aggregate(..) => {
                    n += 1;
                    if n == k {
                        return Err(SqlError::Unsupported);
                    }
                }
            }
        }
        Err(SqlError::UnknownColumn)
    }
}

fn order_values(a: &[Value], b: &[Value], order: &[(BoundScalar, bool, bool)]) -> Ordering {
    for ((a, b), (_, desc, first)) in a.iter().zip(b).zip(order) {
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
                let ord = cmp(a, b);
                if *desc { ord.reverse() } else { ord }
            }
        };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

/// PG18 resolves an unknown (quoted) literal compared with a column through
/// the column type's input function. `None`: character and bytea columns keep
/// their existing route; `Some(None)`: resolution needs the session clock;
/// `Some(Some(v))`: the typed value.
pub(super) fn unknown_comparison_literal(value: &Value, ty: &DataType) -> R<Option<Option<Value>>> {
    if !matches!(value, Value::Text(_))
        || matches!(
            ty,
            DataType::Text | DataType::Varchar | DataType::Bpchar | DataType::Bytes
        )
    {
        return Ok(None);
    }
    if *ty == DataType::TimestampTz || functions::zone_dependent_literal(value, ty) {
        return Ok(Some(None));
    }
    functions::coerce_clocked(value.clone(), ty, None).map(|v| Some(Some(v)))
}
pub(super) fn coerce_clocked(
    v: Value,
    t: &DataType,
    clock: Option<&super::clock::ExecutionClock>,
) -> R<Value> {
    functions::coerce_clocked(v, t, clock)
}

#[cfg(test)]
mod aggregate_tests;

impl BoundFilter {
    fn attach_clock(&mut self, clock: Option<&super::clock::ExecutionClock>) {
        match self {
            Self::InSubquery {
                value,
                clock: saved,
                ..
            } => {
                value.attach_clock(clock);
                *saved = clock.cloned();
            }
            Self::Scalar(a) | Self::Null(a, _) => a.attach_clock(clock),
            Self::Compare(a, _, b) => {
                a.attach_clock(clock);
                b.attach_clock(clock);
            }
            Self::Like {
                value,
                pattern,
                escape,
                ..
            } => {
                value.attach_clock(clock);
                pattern.attach_clock(clock);
                if let Some(a) = escape {
                    a.attach_clock(clock);
                }
            }
            Self::Not(a) | Self::Is(a, _, _) => a.attach_clock(clock),
            Self::Logic(a, _, b) => {
                a.attach_clock(clock);
                b.attach_clock(clock);
            }
        }
    }
}
impl BoundQuery<'_> {
    pub(in crate::relational) fn with_clock(
        mut self,
        clock: Option<&super::clock::ExecutionClock>,
    ) -> Self {
        self.attach_execution_clock(clock);
        self
    }
    fn attach_execution_clock(&mut self, clock: Option<&super::clock::ExecutionClock>) {
        for (_, subquery) in &mut self.subqueries {
            subquery.attach_execution_clock(clock);
        }
        if let Some(grouping) = &mut self.grouping {
            grouping.attach_clock(clock, &mut self.joins);
        } else {
            for join in &mut self.joins {
                join.clock = clock.cloned();
                join.on.attach_clock(clock);
            }
        }
        for item in &mut self.items {
            match item {
                BoundItem::Scalar(a) | BoundItem::Aggregate(_, Some(a)) => a.attach_clock(clock),
                _ => {}
            }
        }
        if let Some(filter) = &mut self.filter {
            filter.attach_clock(clock);
        }
        for (a, _, _) in &mut self.order {
            a.attach_clock(clock);
        }
    }
}

fn parse_like_escape(pattern: &Value, escape: &Value) -> R<Option<super::like::Escape>> {
    if *pattern == Value::Null || *escape == Value::Null {
        return Ok(None);
    }
    let (Value::Text(_), Value::Text(escape)) = (pattern, escape) else {
        return Err(SqlError::UndefinedFunction);
    };
    Ok(Some(super::like::Escape::parse(escape)?))
}

fn eval_like(
    value: Value,
    pattern: Value,
    escape: super::like::Escape,
    insensitive: bool,
    negated: bool,
    work: &mut usize,
) -> R<Option<bool>> {
    if value == Value::Null {
        return Ok(None);
    }
    let (Value::Text(value), Value::Text(pattern)) = (value, pattern) else {
        return Err(SqlError::UndefinedFunction);
    };
    Ok(Some(
        super::like::like(&value, &pattern, escape, insensitive, work)? != negated,
    ))
}

struct AggregateAuxiliary {
    count: i64,
    distinct: std::collections::BTreeSet<grouping::Key>,
}
fn aggregate_auxiliary(
    items: &[BoundItem],
    retention: &mut scan::Retention,
) -> R<Vec<AggregateAuxiliary>> {
    retention.charge(items.len() * std::mem::size_of::<AggregateAuxiliary>())?;
    Ok(items
        .iter()
        .map(|_| AggregateAuxiliary {
            count: 0,
            distinct: std::collections::BTreeSet::new(),
        })
        .collect())
}
fn aggregate_retained(values: &[Value]) -> usize {
    std::mem::size_of_val(values) + values.iter().map(value_bytes).sum::<usize>()
}
fn finish_aggregates(
    items: &[BoundItem],
    values: &mut [Value],
    auxiliary: &[AggregateAuxiliary],
    work: &mut usize,
) -> R<()> {
    for ((item, value), state) in items.iter().zip(values).zip(auxiliary) {
        if let BoundItem::Aggregate(a, _) = item
            && matches!(a.base(), Aggregate::Avg)
            && state.count != 0
        {
            *value = match std::mem::replace(value, Value::Null) {
                Value::Float8(sum) => Value::Float8(Float8::from_bits(
                    (sum.value() / state.count as f64).to_bits(),
                )),
                sum => functions::aggregate_numeric_average(sum, state.count, work)?,
            };
        }
    }
    Ok(())
}

// Transition inputs and retained state remain explicit at both physical call sites.
#[allow(clippy::too_many_arguments)]
fn update_aggregates(
    items: &[BoundItem],
    columns: &[ColumnDef],
    values: &mut [Value],
    auxiliary: &mut [AggregateAuxiliary],
    retention: &mut scan::Retention,
    row: &Tuple,
    params: &[Value],
    work: &mut usize,
) -> R<()> {
    update_filtered_aggregates(
        items,
        &[],
        columns,
        values,
        auxiliary,
        retention,
        row,
        params,
        work,
    )
}
#[allow(clippy::too_many_arguments)]
fn update_filtered_aggregates(
    items: &[BoundItem],
    filters: &[Option<BoundFilter>],
    columns: &[ColumnDef],
    values: &mut [Value],
    auxiliary: &mut [AggregateAuxiliary],
    retention: &mut scan::Retention,
    row: &Tuple,
    params: &[Value],
    work: &mut usize,
) -> R<()> {
    for (index, item) in items.iter().enumerate() {
        let BoundItem::Aggregate(a, c) = item else {
            return Err(SqlError::Unsupported);
        };
        // Like WHERE, each predicate has a fresh per-row scalar budget. Aggregate
        // arguments retain the statement-wide input budget below.
        if let Some(Some(filter)) = filters.get(index)
            && filter.eval(row, params, &mut 0)? != Some(true)
        {
            continue;
        }
        let evaluated = c.as_ref().map(|e| e.eval(row, params, work)).transpose()?;
        let v = evaluated.as_ref();
        if v == Some(&Value::Null) {
            continue;
        }
        let state = &mut auxiliary[index];
        if a.distinct() {
            let input = evaluated.as_ref().ok_or(SqlError::Type)?;
            let bytes = std::mem::size_of::<Value>()
                + value_bytes(input)
                + 6 * std::mem::size_of::<usize>();
            retention.charge(bytes)?;
            let key = grouping::Key(vec![input.clone()]);
            if !state.distinct.insert(key) {
                retention.release(bytes);
                continue;
            }
        }
        let mut value = std::mem::replace(&mut values[index], Value::Null);
        match a.base() {
            Aggregate::Count => {
                if let Value::Int(n) = &mut value {
                    *n = n.checked_add(1).ok_or(SqlError::Overflow)?;
                }
            }
            Aggregate::Avg => {
                let v = v.ok_or(SqlError::Type)?;
                value = if columns[index].data_type == DataType::Numeric {
                    functions::aggregate_numeric_add(value, v.clone(), work)?
                } else {
                    let n = match v {
                        Value::Float4(n) => f64::from(n.value()),
                        Value::Float8(n) => n.value(),
                        _ => return Err(SqlError::Type),
                    };
                    let sum = if let Value::Float8(old) = value {
                        let sum = old.value() + n;
                        if sum.is_infinite() && old.value().is_finite() && n.is_finite() {
                            return Err(SqlError::Overflow);
                        }
                        sum
                    } else {
                        n
                    };
                    Value::Float8(Float8::from_bits(sum.to_bits()))
                };
                state.count = state.count.checked_add(1).ok_or(SqlError::Overflow)?;
            }
            Aggregate::Sum => {
                let v = v.ok_or(SqlError::Type)?;
                value = match columns[index].data_type {
                    DataType::Numeric => functions::aggregate_numeric_add(
                        value,
                        evaluated.ok_or(SqlError::Type)?,
                        work,
                    )?,
                    DataType::Real => {
                        let Value::Float4(n) = v else {
                            return Err(SqlError::Type);
                        };
                        let sum = if let Value::Float4(old) = value {
                            let sum = old.value() + n.value();
                            if sum.is_infinite() && old.value().is_finite() && n.value().is_finite()
                            {
                                return Err(SqlError::Overflow);
                            }
                            sum
                        } else {
                            n.value()
                        };
                        Value::Float4(Float4::from_bits(sum.to_bits()))
                    }
                    DataType::DoublePrecision => {
                        let Value::Float8(n) = v else {
                            return Err(SqlError::Type);
                        };
                        let sum = if let Value::Float8(old) = value {
                            let sum = old.value() + n.value();
                            if sum.is_infinite() && old.value().is_finite() && n.value().is_finite()
                            {
                                return Err(SqlError::Overflow);
                            }
                            sum
                        } else {
                            n.value()
                        };
                        Value::Float8(Float8::from_bits(sum.to_bits()))
                    }
                    DataType::BigInt => {
                        let Value::Int(n) = v else {
                            return Err(SqlError::Type);
                        };
                        Value::Int(if let Value::Int(old) = value {
                            old.checked_add(*n).ok_or(SqlError::Overflow)?
                        } else {
                            *n
                        })
                    }
                    DataType::Boolean
                    | DataType::SmallInt
                    | DataType::Integer
                    | DataType::Text
                    | DataType::Uuid
                    | DataType::Jsonb
                    | DataType::IntArray
                    | DataType::BigIntArray
                    | DataType::TextArray
                    | DataType::Varchar
                    | DataType::Bpchar
                    | DataType::Bytes
                    | DataType::Date
                    | DataType::Time
                    | DataType::TimeTz
                    | DataType::Timestamp
                    | DataType::TimestampTz
                    | DataType::Interval => return Err(SqlError::Type),
                };
            }
            Aggregate::Min | Aggregate::Max => {
                let v = v.ok_or(SqlError::Type)?;
                if value == Value::Null
                    || (matches!(a.base(), Aggregate::Min) && cmp(v, &value) == Ordering::Less)
                    || (matches!(a.base(), Aggregate::Max) && cmp(v, &value) == Ordering::Greater)
                {
                    value = evaluated.ok_or(SqlError::Type)?;
                }
            }
            _ => unreachable!(),
        }
        values[index] = value;
    }
    Ok(())
}

#[cfg(test)]
mod join_visit_budget_tests {
    use super::*;
    use scan::RowSource;

    #[test]
    fn candidate_budget_is_inclusive_and_shared_across_rows_and_joins() {
        let mut state = Relational::default();
        for sql in [
            "CREATE TABLE budget_l(id int PRIMARY KEY)",
            "CREATE TABLE budget_r(id int PRIMARY KEY)",
            "CREATE TABLE budget_u(id int PRIMARY KEY)",
            "INSERT INTO budget_l VALUES(1),(2)",
            "INSERT INTO budget_r VALUES(1),(2)",
            "INSERT INTO budget_u VALUES(1)",
        ] {
            let prepared = Prepared::parse(sql, 1, state.epoch).unwrap();
            state
                .execute_with_cancel(&prepared, &[], None, || false)
                .unwrap();
        }
        for (sql, candidates, outputs) in [
            (
                "SELECT l.id FROM budget_l l JOIN budget_r r ON true JOIN budget_u u ON true",
                8,
                4,
            ),
            (
                "SELECT l.id FROM budget_l l JOIN budget_r r ON l.id=r.id",
                2,
                2,
            ),
        ] {
            let query = Query::parse(&mut Parser {
                tokens: lex(sql).unwrap(),
                at: 0,
                params: BTreeSet::new(),
                relations: Vec::new(),
            })
            .unwrap();
            let bound = query.bind(&state, 0, &[]).unwrap();
            let source = scan::LegacyRows(&state);
            for remaining in [candidates, candidates - 1] {
                let visits = Cell::new(MAX_JOIN_VISITS - remaining);
                let mut cancel = || false;
                let mut meter = scan::Meter::per_row(&mut cancel);
                let input = source
                    .scan(
                        bound.tables[0],
                        &scan::KeyRange {
                            lo: std::ops::Bound::Unbounded,
                            hi: std::ops::Bound::Unbounded,
                        },
                        scan::Direction::Forward,
                    )
                    .unwrap();
                let result = join_rows(&bound, &source, input, &[], &mut meter, &visits)
                    .collect::<R<Vec<_>>>();
                if remaining == candidates {
                    assert_eq!(result.unwrap().len(), outputs);
                    assert_eq!(visits.get(), MAX_JOIN_VISITS);
                } else {
                    assert!(matches!(result, Err(SqlError::Limit)));
                    assert_eq!(visits.get(), MAX_JOIN_VISITS + 1);
                }
            }
        }
    }
}

#[cfg(test)]
mod average_distinct_retention_tests {
    use super::*;

    fn aggregate_input(a: Aggregate, ty: DataType) -> (Vec<BoundItem>, Vec<ColumnDef>) {
        let input = Scalar::Param(1)
            .bind(&|_, _| Err(SqlError::UnknownColumn), &[], &[Some(ty)])
            .unwrap();
        let result = if matches!(a.base(), Aggregate::Count) {
            DataType::BigInt
        } else {
            DataType::Numeric
        };
        (
            vec![BoundItem::Aggregate(a, Some(input))],
            vec![ColumnDef {
                id: 0,
                name: a.name().into(),
                data_type: result,
                not_null: false,
            }],
        )
    }

    #[test]
    fn distinct_state_hits_production_retention_cap_without_scalar_budget() {
        let (items, columns) = aggregate_input(Aggregate::CountDistinct, DataType::Text);
        let mut retention = scan::Retention::new(scan::retention_limit());
        let mut auxiliary = aggregate_auxiliary(&items, &mut retention).unwrap();
        let mut values = vec![Value::Int(0)];
        let row = Tuple {
            values: vec![],
            version: 0,
        };
        let mut limited = false;
        for n in 0..2400 {
            let input = Value::Text(format!("{n:08}{}", "x".repeat(32 * 1024)));
            // Reset only the scalar evaluation budget to isolate retained state.
            let result = update_aggregates(
                &items,
                &columns,
                &mut values,
                &mut auxiliary,
                &mut retention,
                &row,
                &[input],
                &mut 0,
            );
            if result == Err(SqlError::Limit) {
                limited = true;
                break;
            }
            result.unwrap();
        }
        assert!(limited);
        assert!(retention.used() <= scan::MAX_RETAINED_BYTES);
        assert!(retention.used() > 60 * 1024 * 1024);
        assert_eq!(values, vec![Value::Int(auxiliary[0].distinct.len() as i64)]);
    }

    #[test]
    fn average_distinct_polls_during_global_and_grouped_scans() {
        let mut state = Relational::default();
        for sql in [
            "CREATE TABLE avg_cancel(id int PRIMARY KEY,k int,v int)",
            "INSERT INTO avg_cancel VALUES(1,0,1),(2,0,1),(3,1,2),(4,1,2),(5,0,3),(6,1,3)",
        ] {
            let p = Prepared::parse(sql, 1, state.epoch).unwrap();
            state.execute_with_cancel(&p, &[], None, || false).unwrap();
        }
        let before = bincode::serialize(&state).unwrap();
        for sql in [
            "SELECT avg(DISTINCT v) FROM avg_cancel",
            "SELECT k,avg(DISTINCT v) FROM avg_cancel GROUP BY k",
        ] {
            let p = Prepared::parse(sql, 1, state.epoch).unwrap();
            for fence in [3, 4] {
                let mut polls = 0;
                assert_eq!(
                    state.execute_with_cancel(&p, &[], None, || {
                        polls += 1;
                        polls == fence
                    }),
                    Err(SqlError::Aborted)
                );
                assert_eq!(polls, fence);
                assert_eq!(bincode::serialize(&state).unwrap(), before);
            }
            assert!(state.execute_with_cancel(&p, &[], None, || false).is_ok());
        }
    }

    #[test]
    fn average_state_is_charged_before_allocation_and_numeric_sum_grows() {
        let (items, columns) = aggregate_input(Aggregate::Avg, DataType::Numeric);
        let mut too_small = scan::Retention::new(std::mem::size_of::<AggregateAuxiliary>() - 1);
        assert!(matches!(
            aggregate_auxiliary(&items, &mut too_small),
            Err(SqlError::Limit)
        ));
        assert_eq!(too_small.used(), 0);
        let mut retention = scan::Retention::new(4096);
        let mut auxiliary = aggregate_auxiliary(&items, &mut retention).unwrap();
        assert_eq!(retention.used(), std::mem::size_of::<AggregateAuxiliary>());
        let mut values = vec![Value::Null];
        let row = Tuple {
            values: vec![],
            version: 0,
        };
        let input = Value::Numeric(Numeric::parse("123456789012345678901234567890.123").unwrap());
        let before = aggregate_retained(&values);
        update_aggregates(
            &items,
            &columns,
            &mut values,
            &mut auxiliary,
            &mut retention,
            &row,
            &[input],
            &mut 0,
        )
        .unwrap();
        let growth = aggregate_retained(&values) - before;
        retention.charge(growth).unwrap();
        assert_eq!(
            retention.used(),
            std::mem::size_of::<AggregateAuxiliary>() + growth
        );
        assert_eq!(auxiliary[0].count, 1);
    }
}

#[cfg(test)]
mod deferred_routine_only_tests {
    use super::*;

    fn query(sql: &str) -> Query {
        let prepared = Prepared::parse(sql, 1, 0).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        let Statement::Query(query) = prepared.statement else {
            panic!("expected query: {sql}");
        };
        query
    }

    #[test]
    fn aggregate_filter_classification_walks_conditions_and_private_markers() {
        for sql in [
            "SELECT count(*) FILTER (WHERE true)",
            "SELECT pg_catalog.count(*) FILTER (WHERE false)",
            "SELECT sum(1) FILTER (WHERE NULL)",
            "SELECT count(*) FILTER (WHERE (true AND false) IS NOT UNKNOWN)",
            "SELECT CASE WHEN true THEN count(*) FILTER (WHERE true) ELSE 0 END",
            "SELECT (count(*) FILTER (WHERE true)>0)",
            "SELECT count(*) FILTER (WHERE CASE WHEN true THEN true ELSE false END)",
        ] {
            let query = query(sql);
            assert!(query.is_table_free(), "{sql}");
            assert!(!query.has_undefined_routine(), "{sql}");
            assert!(!query.deferred_routine_only(), "{sql}");
            assert!(!query.rejects_unsupported_table_free_scalar(), "{sql}");
            assert!(query.sequence_dependencies().is_empty(), "{sql}");
        }
        for sql in [
            "SELECT public.count(*) FILTER (WHERE true)",
            "SELECT count(*) FILTER (WHERE nosuchfn())",
            "SELECT count(*) FILTER (WHERE CASE WHEN nosuchfn() THEN true ELSE false END)",
            "SELECT count(*) FILTER (WHERE CASE WHEN true THEN nosuchfn() ELSE false END)",
            "SELECT CASE WHEN count(*) FILTER (WHERE nosuchfn())>0 THEN 1 ELSE 0 END",
        ] {
            let query = query(sql);
            assert!(!query.is_table_free(), "{sql}");
            assert!(query.has_undefined_routine(), "{sql}");
            assert!(query.deferred_routine_only(), "{sql}");
        }
        for sql in [
            "SELECT count(*) FILTER (WHERE pg_toast.nosuchfn())",
            "SELECT count(*) FILTER (WHERE CASE WHEN pg_toast.nosuchfn() THEN true ELSE false END)",
            "SELECT count(*) FILTER (WHERE nosuchfn(x))",
        ] {
            let query = query(sql);
            assert!(query.has_undefined_routine(), "{sql}");
            assert!(!query.deferred_routine_only(), "{sql}");
            assert!(!query.is_table_free(), "{sql}");
        }
        for sql in [
            "SELECT count(*) FILTER (WHERE nextval('s')>0)",
            "SELECT count(*) FILTER (WHERE CASE WHEN nextval('s')>0 THEN true ELSE false END)",
            "SELECT CASE WHEN true THEN count(*) FILTER (WHERE nextval('s')>0) ELSE 0 END",
        ] {
            let query = query(sql);
            assert_eq!(
                query.sequence_dependencies(),
                vec![Some("s".into())],
                "{sql}"
            );
            assert!(!query.is_table_free(), "{sql}");
            assert!(!query.deferred_routine_only(), "{sql}");
        }
    }

    #[test]
    fn deferred_routine_only_classification() {
        for sql in [
            "SELECT nosuchfn()",
            "SELECT \"LOWER\"('a')",
            "SELECT pg_catalog.nosuchfn()",
            "SELECT public.coalesce(1,2)",
            "SELECT nosuchagg(1)",
            "SELECT avg(nosuchfn(1))",
            "SELECT count(DISTINCT nosuchfn(1))",
            "SELECT avg(DISTINCT nosuchfn(1))",
            "SELECT sum(nosuchfn(1))",
            "SELECT nosuchschema.lower('a')",
            "SELECT nosuchschema.lower(1/0)",
            "SELECT count(*), nosuchfn()",
            "SELECT lower(nosuchfn())",
            "SELECT 1 WHERE nosuchfn(1) = 1",
            "SELECT nosuchschema.count(*)",
            "SELECT \"PUBLIC\".nosuchfn(1)",
            "SELECT pg_temp.lower('a')",
            "SELECT information_schema.nosuchfn('a')",
            "SELECT upper(lower(nosuchfn()))",
        ] {
            let p = Prepared::parse(sql, 1, 0).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
            assert!(p.deferred_routine_only(), "{sql}");
        }
        let sql = "SELECT nosuchfn() ORDER BY 1";
        match Prepared::parse(sql, 1, 0) {
            Ok(p) => assert!(p.deferred_routine_only(), "{sql}"),
            Err(e) => eprintln!(
                "F9 skipped {sql}: prepare error code={e:?}; ORDER BY literal not admitted"
            ),
        }
        for sql in [
            "SELECT 1",
            "SELECT lower('a')",
            "SELECT pg_toast.lower('a')",
            "SELECT pg_toast.nosuchfn(1)",
            "SELECT lower(pg_toast.nosuchfn(1))",
            "SELECT avg(1)",
            "SELECT information_schema._pg_expandarray(1)",
            "SELECT nosuchfn(x)",
            "SELECT nosuchfn(x) FROM t",
            "SELECT nosuchfn() FROM t",
            "SELECT t.a,nosuchfn() FROM t JOIN u ON t.a=u.a",
            "EXPLAIN SELECT nosuchfn()",
            "SELECT nextval('s'),nosuchfn()",
            "SELECT *",
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET a=1",
        ] {
            match Prepared::parse(sql, 1, 0) {
                Ok(p) => assert!(!p.deferred_routine_only(), "{sql}"),
                Err(e) => eprintln!(
                    "F9 skipped {sql}: prepare error code={e:?}; no Prepared classification exists"
                ),
            }
        }
    }
}

mod constraints;
pub(super) use constraints::{
    constraint_check, constraint_deparse, constraint_key, constraint_key_supported,
    constraint_types_compatible, constraint_validate,
};

#[cfg(test)]
mod in_subquery_execution_tests {
    use super::*;
    struct CountingSource<'a> {
        rows: scan::LegacyRows<'a>,
        child: u64,
        visits: Cell<usize>,
    }
    impl scan::RowSource for CountingSource<'_> {
        fn scan<'s>(
            &'s self,
            table: &TableDef,
            range: &scan::KeyRange,
            direction: scan::Direction,
        ) -> R<Box<dyn Iterator<Item = R<scan::SourceRow<'s>>> + 's>> {
            let child = table.id == self.child;
            Ok(Box::new(self.rows.scan(table, range, direction)?.inspect(
                move |_| {
                    if child {
                        self.visits.set(self.visits.get() + 1);
                    }
                },
            )))
        }
        fn get<'s>(&'s self, table: &TableDef, key: &[u8]) -> R<Option<scan::SourceRow<'s>>> {
            self.rows.get(table, key)
        }
    }
    fn state() -> Relational {
        let mut state = Relational::default();
        for sql in [
            "CREATE TABLE in_outer(id int PRIMARY KEY)",
            "CREATE TABLE in_inner(id int PRIMARY KEY)",
            "INSERT INTO in_outer VALUES(1),(2),(3)",
            "INSERT INTO in_inner VALUES(1),(2),(3)",
        ] {
            let p = Prepared::parse(sql, 1, state.epoch).unwrap();
            state.execute_with_cancel(&p, &[], None, || false).unwrap();
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
    fn subquery_cache_is_once_per_execution_and_cancel_stops_child_scan() {
        let state = state();
        let query = query("SELECT id FROM in_outer WHERE id IN(SELECT id FROM in_inner)");
        let bound = query.bind(&state, 0, &[]).unwrap();
        let source = CountingSource {
            rows: scan::LegacyRows(&state),
            child: state.tables["in_inner"].id,
            visits: Cell::new(0),
        };
        for visits in [3, 6] {
            assert_eq!(
                bound
                    .execute(&state, &source, &[], &mut || false)
                    .unwrap()
                    .rows
                    .len(),
                3
            );
            assert_eq!(source.visits.get(), visits);
        }
        source.visits.set(0);
        let mut polls = 0;
        assert_eq!(
            bound.execute(&state, &source, &[], &mut || {
                polls += 1;
                polls == 2
            }),
            Err(SqlError::Aborted)
        );
        assert_eq!(source.visits.get(), 2);
        assert_eq!(polls, 2);
    }
    #[test]
    fn linear_membership_budget_is_inclusive_and_shared_with_joins_and_nested_queries() {
        let state = state();
        let query = query(
            "SELECT o.id FROM in_outer o JOIN in_inner j ON o.id=j.id WHERE o.id::double precision IN(SELECT id FROM in_inner WHERE id::double precision IN(SELECT id FROM in_inner LIMIT 1) LIMIT 1) LIMIT 1",
        );
        let bound = query.bind(&state, 0, &[]).unwrap();
        let source = scan::LegacyRows(&state);
        for remaining in [3, 2] {
            let mut cancel = || false;
            let execution = Execution::new(&mut cancel, &[]);
            execution.visits.set(MAX_JOIN_VISITS - remaining);
            let result = bound.execute_context(&state, &source, &[], &[], &execution, false, false);
            if remaining == 3 {
                assert_eq!(result.unwrap().rows.len(), 1);
                assert_eq!(execution.visits.get(), MAX_JOIN_VISITS);
            } else {
                assert_eq!(result, Err(SqlError::Limit));
                assert_eq!(execution.visits.get(), MAX_JOIN_VISITS + 1);
            }
        }
    }
    #[test]
    fn sequence_only_does_not_hide_nested_table_reads() {
        let query = query("SELECT nextval('seq') WHERE 1 IN(SELECT id FROM in_inner)");
        assert!(!query.sequence_only());
        assert!(!query.is_table_free());
        assert!(!query.deferred_routine_only());
        assert_eq!(query.read_tables(), vec!["in_inner"]);
    }
}
