//! Private syntax, binding and bounded physical execution. No persisted types.
use super::*;
use std::cell::{Cell, RefCell};
use std::cmp::Ordering;
mod aggregate_filter;
mod functions;
pub(super) mod typed_read;
pub(super) use functions::assignment;
pub(super) mod conflict;
mod grouping;
mod insert_values;
mod schema_names;
mod subplans;
mod derived;
pub(super) mod views;
mod outer;
mod exists;
mod task_refs;
use subplans::{SubplanMode, Subplans};
use exists::RetentionScope;
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
    QualifiedStar {
        schema: Option<String>,
        relation: String,
    },
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
impl Query {
    pub(super) fn from_single_source_read(read: typed_read::SingleSourceRead) -> R<Self> {
        typed_read::lower(read)
    }
}

#[derive(Debug, Clone, Default)]
struct QueryClauses {
    view_reads: Vec<String>,
    distinct: bool,
    filter: Option<Filter>,
    group: Vec<Scalar>,
    having: Option<Filter>,
    offset: Option<Expr>,
    page_casts: [Option<DataType>; 2],
    // Explicit aliases hide schema-qualified relation names, even `FROM t AS t`.
    aliased_sources: Vec<usize>,
    derived: Option<Box<Query>>,
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
    Exists {
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
    Exists {
        query: usize,
        negated: bool,
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
            Filter::Clauses(_) | Filter::InSubquery { .. } | Filter::Exists { .. } => return Err(SqlError::Unsupported),
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
            Scalar::Cast(inner, _) | Scalar::Domain(inner, ..) | Scalar::Unary(_, inner) => {
                return cost(inner, nodes, bytes);
            }
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
fn admit_subquery_forms(tokens: &[Token], allow_exists: bool) -> R<()> {
    let mut at = 0;
    while at < tokens.len() {
        if is_subquery_open(tokens, at) {
            if at == 0 || !(tokens.get(at - 1) == Some(&Token::Word("in".into()))
                || allow_exists && tokens.get(at - 1) == Some(&Token::Word("exists".into()))
                || tokens.get(at - 1) == Some(&Token::Word("from".into()))) {
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
pub(super) fn admit_subquery_depth(tokens: &[Token]) -> R<()> {
    let mut stack = Vec::new();
    let mut nested = 0;
    for (at, token) in tokens.iter().enumerate() {
        match token {
            Token::Symbol('(') => {
                let sub = is_subquery_open(tokens, at);
                stack.push(sub);
                if sub {
                    nested += 1;
                    if nested > subplans::MAX_DEPTH {
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
        Scalar::Cast(a, _) | Scalar::Domain(a, ..) | Scalar::Unary(_, a) => walk_scalar(a, visit)?,
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
        Scalar::Cast(value, _) | Scalar::Domain(value, ..) | Scalar::Unary(_, value) => {
            reject_scalar_subquery(value)?
        }
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
        visit: &mut impl FnMut(Option<&'a Scalar>, &'a Query) -> R<()>,
    ) -> R<()> {
        match self {
            Self::InSubquery { value, query, .. } => visit(Some(value), query)?,
            Self::Exists { query, .. } => visit(None, query)?,
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
            Self::InSubquery { .. } | Self::Exists { .. } => Ok(()),
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
        subqueries: &Subplans<'_>,
    ) -> R<()> {
        match self {
            Self::Exists { .. } => Ok(()),
            Self::InSubquery { value, query, .. } => {
                let bound = subqueries.get(query.as_ref() as *const Query as usize, SubplanMode::In)?;
                let expected = if matches!(value, Scalar::Param(n) if params.get(n - 1).is_some_and(Option::is_some))
                {
                    None
                } else {
                    Some(&bound.columns[0].data_type)
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
        subqueries: &Subplans<'_>,
    ) -> R<BoundFilter> {
        Ok(match self {
            Self::Exists { query, negated } => {
                let key = subqueries.key(query.as_ref() as *const Query as usize);
                subqueries.get(key, SubplanMode::Exists)?;
                BoundFilter::Exists {
                    query: key,
                    negated: *negated,
                }
            }
            Self::InSubquery {
                value,
                query,
                negated,
            } => {
                let key = subqueries.key(query.as_ref() as *const Query as usize);
                let column_type = &subqueries.get(key, SubplanMode::In)?.columns[0].data_type;
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
            Self::Exists { .. } => {}
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
            Self::Exists { .. } => Ok(()),
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
    fn parse_child(p: &mut Parser) -> R<Box<Query>> {
        let end = subquery_end(&p.tokens, p.at - 1)?;
        let mut child = Parser {
            tokens: p.tokens[p.at..end].to_vec(),
            at: 0,
            params: BTreeSet::new(),
            relations: Vec::new(),
        };
        let query = Query::parse(&mut child)?;
        if query.derived().is_some() { return Err(SqlError::Unsupported); }
        p.params.extend(child.params);
        p.relations.extend(child.relations);
        p.at = end + 1;
        Ok(Box::new(query))
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
            let direct = matches!(p.tokens.get(p.at), Some(Token::Word(w)) if w == "exists");
            let term = Self::parse_context(p, 3, depth + 1, aggregate_argument)?;
            match term {
                Self::Exists { query, negated } if direct => Self::Exists {
                    query,
                    negated: !negated,
                },
                term => Self::Not(Box::new(term)),
            }
        } else if p.word("exists") {
            p.punct('(')?;
            Self::Exists {
                query: Self::parse_child(p)?,
                negated: false,
            }
        } else {
            let parse_scalar = |p: &mut Parser, depth| {
                if aggregate_argument {
                    Scalar::parse_aggregate_argument(p, depth)
                } else {
                    Scalar::parse(p, depth)
                }
            };
            // A comma at the outer parenthesis level distinguishes a row from
            // scalar grouping. Lower rows here, keeping scalar and wire types unchanged.
            let a = if Self::starts_row(p) {
                Scalar::Predicate(Box::new(Self::row_filter(p, depth, aggregate_argument)?))
            } else { parse_scalar(p, depth)? };
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
                    let query = Self::parse_child(p)?;
                    Self::InSubquery {
                        value: a,
                        query,
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
    fn row_filter(p: &mut Parser, depth: usize, aggregate_argument: bool) -> R<Self> {
        let a = Self::row(p, depth, aggregate_argument)?;
        let not = p.word("not");
        let mut rows = Vec::new();
        let inequality = if p.word("in") {
            p.punct('(')?;
            loop {
                rows.push(Self::row(p, depth, aggregate_argument)?);
                if !p.sym(',') { break; }
            }
            p.punct(')')?;
            false
        } else {
            if not { return Err(SqlError::Syntax); }
            let Some(Token::Op(op)) = p.tokens.get(p.at) else { return Err(SqlError::Syntax); };
            let inequality = match op.as_str() {
                "=" => false, "<>" | "!=" => true, _ => return Err(SqlError::Unsupported),
            };
            p.at += 1;
            rows.push(Self::row(p, depth, aggregate_argument)?);
            inequality
        };
        let mut comparisons = Vec::new();
        admit_expansion(&Scalar::Call(String::new(), a.clone()), rows.len())?;
        for b in rows {
            if a.len() != b.len() { return Err(SqlError::Syntax); }
            let fields = a.iter().cloned().zip(b).map(|(a, b)| Self::Compare(a, "=".into(), b)).collect::<Vec<_>>();
            comparisons.push(Self::combine(&fields, true));
        }
        let f = Self::combine(&comparisons, false);
        let f = if not || inequality { Self::Not(Box::new(f)) } else { f };
        f.admit_shape(depth, &mut 0)?;
        // Reuse the normal IS / AND / OR suffix parser without scalar changes.
        Ok(f)
    }
    fn starts_row(p: &Parser) -> bool {
        if p.tokens.get(p.at) != Some(&Token::Symbol('(')) { return false; }
        let mut level = 0;
        for token in &p.tokens[p.at..] {
            match token {
                Token::Symbol('(') => level += 1,
                Token::Symbol(')') => { level -= 1; if level == 0 { break; } }
                Token::Symbol(',') if level == 1 => return true,
                _ => {}
            }
        }
        false
    }
    fn row(p: &mut Parser, depth: usize, aggregate: bool) -> R<Vec<Scalar>> {
        p.punct('(')?;
        let mut fields = vec![Scalar::parse_expr(p, depth + 1, aggregate)?];
        while p.sym(',') { fields.push(Scalar::parse_expr(p, depth + 1, aggregate)?); }
        p.punct(')')?;
        Ok(fields)
    }
    fn combine(filters: &[Self], and: bool) -> Self {
        if filters.len() == 1 { return filters[0].clone(); }
        let middle = filters.len() / 2;
        Self::Logic(Box::new(Self::combine(&filters[..middle], and)), and,
            Box::new(Self::combine(&filters[middle..], and)))
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
            Self::Clauses(_) | Self::InSubquery { .. } | Self::Exists { .. } => Err(SqlError::Unsupported),
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
            Self::Clauses(_) | Self::InSubquery { .. } | Self::Exists { .. } => return Err(SqlError::Unsupported),
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
    exists: RefCell<BTreeMap<usize, bool>>,
    frames: RefCell<Vec<Vec<Vec<Value>>>>,
}
impl<'a> Execution<'a> {
    fn new(cancel: &'a mut dyn FnMut() -> bool, declared: &[Option<DataType>]) -> Self {
        Self {
            cancel: RefCell::new(cancel),
            declared: declared.to_vec(),
            exists: RefCell::new(BTreeMap::new()),
            frames: RefCell::new(Vec::new()),
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
            Self::Exists { query, negated } => Ok(Some(execution.exists_filter(
                owner.subqueries.get(*query, SubplanMode::Exists)?, *query, row, state, source, params,
            )? != *negated)),
            Self::InSubquery {
                value,
                query,
                column_type,
                negated,
                clock,
            } => {
                if !execution.subqueries.borrow().contains_key(query) {
                    let child = owner.subqueries.get(*query, SubplanMode::In)?;
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
            Self::InSubquery { .. } | Self::Exists { .. } => Ok(None),
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
            Self::InSubquery { .. } | Self::Exists { .. } => return Err(SqlError::Unsupported),
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
    fn source(&mut self) -> R<(Source, bool)> {
        let name = self.table_name()?;
        let aliased = self.word("as")
            || matches!(self.tokens.get(self.at), Some(Token::QuotedIdent(_)))
            || matches!(self.tokens.get(self.at), Some(Token::Word(w)) if !["inner", "left", "join", "where", "order", "limit", "offset", "group", "having", "on", "right", "full", "cross"].contains(&w.as_str()));
        let alias = if aliased {
            self.name()?
        } else {
            split_key(&name).1.into()
        };
        Ok((Source { name, alias }, aliased))
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
        Filter::Exists { .. } => false,
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
        Scalar::Unary(_, a) | Scalar::Cast(a, _) | Scalar::Domain(a, ..) => {
            scalar_has_undefined_routine(a)
        }
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
fn integer_width(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::SmallInt | DataType::Integer | DataType::BigInt
    )
}

impl Query {
    pub(super) fn view_projection(&self, state: &Relational) -> R<Vec<ColumnDef>> {
        if self.sources.len() != 1 || !self.joins.is_empty() || !self.predicates.is_empty()
            || self.where_filter().is_some() || !self.order.is_empty() || self.limit.is_some()
            || self.distinct() || !self.group().is_empty() || self.having().is_some()
            || self.offset().is_some() || self.derived().is_some()
            || self.items.iter().any(|(i, _)| !matches!(i, Item::Star | Item::QualifiedStar { .. } | Item::Scalar(Scalar::Column(_))))
        { return Err(SqlError::Unsupported); }
        let mut query = self.clone();
        // Duplicate output names belong to CREATE VIEW's final-name check.
        for (i, (item, alias)) in query.items.iter_mut().enumerate() {
            if matches!(item, Item::Scalar(_)) { *alias = Some(format!("view_output_{i}")); }
        }
        let mut columns = query.bind(state, 0, &[])?.columns;
        let mut offset = 0;
        for (item, alias) in &self.items {
            let count = if matches!(item, Item::Star | Item::QualifiedStar { .. }) {
                state.tables[&self.sources[0].name].logical_columns().count()
            } else { 1 };
            for column in &mut columns[offset..offset + count] {
                let original = state.tables[&self.sources[0].name].logical_columns().find(|c| c.id == column.id).ok_or(SqlError::UnknownColumn)?;
                column.name = alias.as_ref().unwrap_or(&original.name).clone();
            }
            offset += count;
        }
        Ok(columns)
    }
    /// Resolve wildcard scope after schema binding; aliases hide base names.
    fn star_sources(&self, item: &Item) -> R<Vec<usize>> {
        let Item::QualifiedStar { schema, relation } = item else {
            return Ok((0..self.sources.len()).collect());
        };
        let mut found = Vec::new();
        for (index, source) in self.sources.iter().enumerate() {
            let matches = if let Some(schema) = schema {
                let (source_schema, source_name) = split_key(&source.name);
                source_schema == schema
                    && source_name == relation
                    && !self
                        .clauses()
                        .is_some_and(|c| c.aliased_sources.contains(&index))
            } else {
                &source.alias == relation
            };
            if matches {
                found.push(index);
            }
        }
        if found.is_empty() {
            return Err(SqlError::UnknownTable);
        }
        if found.len() > 1 {
            return Err(SqlError::Duplicate);
        }
        Ok(found)
    }

    fn walk_scalars(&self, visit: &mut impl FnMut(&Scalar) -> R<()>) -> R<()> {
        if let Some(q) = self.derived() { q.walk_scalars(visit)?; }
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
    pub(super) fn has_domain_expression(&self) -> bool {
        let mut found = false;
        let _ = self.walk_scalars(&mut |s| {
            found |= matches!(s, Scalar::Domain(..));
            Ok(())
        });
        found
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
        for f in self.filter.iter().chain(self.joins.iter().map(|j| &j.on)) {
            let _ = f.visit_subqueries(&mut |_, _| {
                found = true;
                Ok(())
            });
        }
        found
    }
    pub(super) fn streaming_reduction(&self) -> bool {
        let mut exists = false;
        if let Some(filter) = self.where_filter() {
            let _ = filter.visit_subqueries(&mut |value, _| { exists |= value.is_none(); Ok(()) });
        }
        if exists || self.derived().is_some() { return true; }
        if !self.joins.is_empty() { return true; }
        self.sources.len() == 1
            && !self.distinct() && !self.has_in_subquery()
            && (!self.group().is_empty()
                || self.items.iter().any(|(item, _)| match item {
                    Item::Aggregate(..) => true,
                    Item::Scalar(s) => grouping::contains_aggregate(s),
                    _ => false,
                })
                || !self.order.is_empty())
    }
    pub(super) fn clear_mutation_filter(&mut self) {
        self.filter = None;
        self.predicates.clear();
    }
    /// Only row-local SELECTs can be evaluated independently on source pages.
    pub(super) fn pageable_insert(&self) -> bool {
        self.sources.len() == 1
            && self.joins.is_empty()
            && self.order.is_empty()
            && self.limit.is_none()
            && self.offset().is_none()
            && !self.distinct()
            && self.group().is_empty()
            && self.clauses().is_none_or(|c| c.having.is_none())
            && !self.has_in_subquery()
            && self.derived().is_none()
            && self.items.iter().all(|(item, _)| match item {
                Item::Aggregate(..) => false,
                Item::Scalar(s) => !grouping::contains_aggregate(s),
                Item::Star | Item::QualifiedStar { .. } => true,
            })
    }
    pub(super) fn subquery_tables(&self) -> Vec<String> {
        let mut out = Vec::new();
        if let Some(q) = self.derived() { out.extend(q.read_tables()); }
        for f in self.filter.iter().chain(self.joins.iter().map(|j| &j.on)) {
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
    /// Projection reads only: mutation_target's synthetic star is never inspected.
    pub(super) fn projection_reads_columns(&self) -> bool {
        self.items.iter().any(|(item, _)| match item {
            Item::Star | Item::QualifiedStar { .. } => true,
            Item::Scalar(s) | Item::Aggregate(_, Some(s)) => {
                let mut found = false;
                let _ = walk_scalar(s, &mut |s| {
                    found |= matches!(s, Scalar::Column(_));
                    Ok(())
                });
                found
            }
            _ => false,
        })
    }
    /// Only outer WHERE scalars; child-query reads have their own requirements.
    pub(super) fn mutation_filter_reads_columns(&self) -> bool {
        let mut found = false;
        if let Some(filter) = &self.filter {
            let _ = filter.visit_scalars(&mut |s| walk_scalar(s, &mut |s| {
                found |= matches!(s, Scalar::Column(_));
                Ok(())
            }));
        }
        found
    }
    pub(super) fn authorization_tables(&self) -> R<Vec<String>> {
        Ok(self.dependencies()?.relations.into_iter().collect())
    }
    pub(super) fn read_tables(&self) -> Vec<String> {
        let mut out = Vec::new();
        for name in self
            .sources
            .iter()
            .map(|s| s.name.clone())
            .chain(self.subquery_tables())
            .chain(self.clauses().into_iter().flat_map(|c| c.view_reads.iter().cloned()))
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
        if let Some(q) = self.derived() { q.limit_parameters(out); }
        for e in self.limit.iter().chain(self.offset()) {
            if let Expr::Param(n) = e {
                out.insert(*n);
            }
        }
        for f in self.where_filter().into_iter().chain(self.having()).chain(self.joins.iter().map(|j| &j.on)) {
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
        outer: &[&[Source]],
        resolve: &impl Fn(&ColumnRef, usize) -> R<BoundColumn>,
        tables: &[&TableDef],
    ) -> R<Subplans<'a>> {
        let mut out = Subplans::default();
        if let Some(q) = self.derived() {
            let used = q.used_parameters();
            let seed = parameters.iter().enumerate().map(|(i, p)|
                p.clone().or_else(|| (!used.contains(&(i + 1))).then_some(DataType::Text))).collect();
            let bound = q.bind_seeded_inner(state, seed, false, &[])?;
            for n in used { parameters[n - 1] = Some(bound.parameters[n - 1].clone()); }
            out.register(q as *const Query as usize, SubplanMode::Scalar, bound)?;
        }
        for (f, visible) in self.where_filter().into_iter().chain(self.having()).map(|f| (f, self.sources.len()))
            .chain(self.joins.iter().enumerate().map(|(j, join)| (&join.on, j + 2))) {
            let scoped = |c: &ColumnRef, _: usize| resolve(c, visible);
            f.visit_subqueries(&mut |value, q| {
                if let Some(value) = value
                    && let [(Item::Scalar(Scalar::Param(n)), _)] = q.items.as_slice()
                    && parameters.get(n - 1).is_some_and(Option::is_none)
                {
                    let ty = value.bind(&scoped, tables, parameters).ok().map(|v| v.ty).unwrap_or(DataType::Text);
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
                let scopes: Vec<_> = std::iter::once(&self.sources[..visible]).chain(outer.iter().copied()).collect();
                let bound = q.bind_seeded_inner(state, seed, false, &scopes)?;
                if value.is_some() && bound.columns.len() != 1 {
                    return Err(SqlError::Syntax);
                }
                if let Some(value) = value
                    && !matches!(value, Scalar::Param(n) if parameters.get(n - 1).is_some_and(Option::is_some))
                    && !grouping::contains_aggregate(value) {
                    value.infer(Some(&bound.columns[0].data_type), parameters, &scoped, tables)?;
                }
                for n in used {
                    if let Some(slot) = parameters.get_mut(n - 1) {
                        *slot = Some(bound.parameters[n - 1].clone());
                    }
                }
                let mode = if value.is_some() { SubplanMode::In } else { SubplanMode::Exists };
                out.register(q as *const Query as usize, mode, bound)?;
                Ok(())
            })?;
        }
        Ok(out)
    }
    fn distinct(&self) -> bool {
        self.clauses().is_some_and(|c| c.distinct)
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
    fn parse_page(p: &mut Parser) -> R<(Expr, Option<DataType>)> {
        let start = p.at;
        if let Ok(value) = p.expr()
            && p.tokens.get(p.at) != Some(&Token::Symbol(':'))
        {
            return Ok((value, None));
        }
        p.at = start;
        let value = Scalar::parse_expr(p, 0, false)?;
        match value {
            Scalar::Param(n) => Ok((Expr::Param(n), None)),
            Scalar::Literal(v) => Ok((Expr::Literal(v), None)),
            Scalar::Cast(value, ty) if integer_type(&ty) || ty == DataType::Numeric => {
                let Scalar::Param(n) = *value else { return Err(SqlError::Unsupported) };
                Ok((Expr::Param(n), Some(ty)))
            }
            _ => Err(SqlError::Unsupported),
        }
    }
    fn page_cast(&self, limit: bool) -> Option<&DataType> {
        self.clauses().and_then(|c| c.page_casts[usize::from(!limit)].as_ref())
    }
    fn page_value(&self, expr: &Expr, params: &[Value], limit: bool) -> R<Value> {
        let value = expr.value(params)?;
        if let Some(ty) = self.page_cast(limit) {
            let value = functions::cast_value(value, ty, &mut 0)?;
            return functions::cast_value(value, &DataType::BigInt, &mut 0);
        }
        Ok(value)
    }
    fn parse_items(p: &mut Parser) -> R<Vec<(Item, Option<String>)>> {
        let mut items = Vec::new();
        loop {
            let item_start = p.at;
            let mut item = if p.sym('*') {
                Item::Star
            } else if let Some(item) = Self::parse_qualified_star(p)? {
                item
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
            // PostgreSQL 18 src/include/parser/kwlist.h: AS_LABEL requires AS.
            let bare_label = match p.tokens.get(p.at) {
                Some(Token::QuotedIdent(_)) => true,
                Some(Token::Word(word)) => ![
                    "array",
                    "as",
                    "char",
                    "character",
                    "create",
                    "day",
                    "except",
                    "fetch",
                    "filter",
                    "for",
                    "from",
                    "grant",
                    "group",
                    "having",
                    "hour",
                    "intersect",
                    "into",
                    "isnull",
                    "limit",
                    "minute",
                    "month",
                    "notnull",
                    "offset",
                    "on",
                    "order",
                    "over",
                    "overlaps",
                    "precision",
                    "returning",
                    "second",
                    "to",
                    "union",
                    "varying",
                    "where",
                    "window",
                    "with",
                    "within",
                    "without",
                    "year",
                ]
                .contains(&word.as_str()),
                _ => false,
            };
            if matches!(p.tokens.get(p.at), Some(Token::Word(w)) if w == "year") {
                return Err(SqlError::Syntax);
            }
            let alias = if p.word("as") || bare_label {
                Some(p.name()?)
            } else {
                None
            };
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
    fn parse_qualified_star(p: &mut Parser) -> R<Option<Item>> {
        let tokens = &p.tokens[p.at..];
        let (schema, relation_at, width) = match tokens {
            [
                Token::Word(_) | Token::QuotedIdent(_),
                Token::Symbol('.'),
                Token::Symbol('*'),
                ..,
            ] => (None, p.at, 3),
            [
                Token::Word(schema) | Token::QuotedIdent(schema),
                Token::Symbol('.'),
                Token::Word(_) | Token::QuotedIdent(_),
                Token::Symbol('.'),
                Token::Symbol('*'),
                ..,
            ] => (Some(ident::truncate(schema, 63).to_owned()), p.at + 2, 5),
            _ => return Ok(None),
        };
        let end = p.at + width;
        p.at = relation_at;
        let relation = p.name()?;
        p.at = end;
        Ok(Some(Item::QualifiedStar { schema, relation }))
    }
    pub(super) fn mutation_target(p: &mut Parser, table: &str) -> R<Self> {
        admit_subquery_depth(&p.tokens[p.at..])?;
        admit_subquery_forms(&p.tokens[p.at..], true)?;
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
            inferred[..declared.len()].clone_from_slice(declared);
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
    pub(super) fn normalize_assignment_alias(mut self) -> Self {
        for source in &mut self.sources {
            source.alias = split_key(&source.name).1.into();
        }
        self
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
                Item::Star | Item::QualifiedStar { .. } => {}
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
        Self::parse(p)
    }
    pub(super) fn parse(p: &mut Parser) -> R<Self> {
        admit_subquery_depth(&p.tokens[p.at..])?;
        admit_subquery_forms(&p.tokens[p.at..], true)?;
        let explain = p.word("explain");
        p.need("select")?;
        let distinct = p.word("distinct");
        if distinct && p.word("on") {
            return Err(SqlError::Unsupported);
        }
        preprocess_aggregates(p)?;
        let aggregate_filters = aggregate_filter::normalize(p)?;
        let mut items = Self::parse_items(p)?;
        let mut sources = vec![];
        let mut aliased_sources = vec![];
        let mut derived = None;
        if p.word("from") {
            let (source, aliased) = if is_subquery_open(&p.tokens, p.at) {
                let (source, child) = derived::parse(p, &mut items)?;
                derived = Some(child);
                (source, true)
            } else { p.source()? };
            if aliased {
                aliased_sources.push(0);
            }
            sources.push(source);
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
            let (source, aliased) = p.source()?;
            if aliased {
                aliased_sources.push(sources.len());
            }
            sources.push(source);
            if sources.len() > MAX_SOURCES {
                return Err(SqlError::Limit);
            }
            p.need("on")?;
            let on = Filter::parse(p, 0, 0)?;
            joins.push(Join { left, on });
        }
        if derived.is_some() && !joins.is_empty() { return Err(SqlError::Unsupported); }
        // Only schema-qualified wildcards need explicit-alias metadata.
        if !items.iter().any(|(item, _)| {
            matches!(
                item,
                Item::QualifiedStar {
                    schema: Some(_),
                    ..
                }
            )
        }) {
            aliased_sources.clear();
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
            Some(if p.word("all") {
                (Expr::Literal(Value::Null), None)
            } else {
                Self::parse_page(p)?
            })
        } else {
            None
        };
        let offset = if p.word("offset") {
            Some(Self::parse_page(p)?)
        } else {
            None
        };
        let (limit, limit_cast) = limit.map_or((None, None), |(e, t)| (Some(e), t));
        let (offset, offset_cast) = offset.map_or((None, None), |(e, t)| (Some(e), t));
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
            join.on.admit_subquery_positions(true)?;
        }
        if let Some(f) = &filter {
            f.admit_subquery_positions(true)?;
        }
        if let Some(f) = &having {
            f.admit_subquery_positions(true)?;
        }
        for s in &group {
            reject_scalar_subquery(s)?;
        }
        for (s, _, _) in &order {
            reject_scalar_subquery(s)?;
        }
        p.sym(';');
        if p.at != p.tokens.len() {
            return Err(SqlError::Unsupported);
        }
        if derived.is_some() {
            predicates.clear();
            if let Some(f) = &filter { f.visit_subqueries(&mut |_, _| Err(SqlError::Unsupported))?; }
        }
        Ok(Self {
            explain,
            sources,
            joins,
            items,
            predicates,
            filter: if !distinct
                && group.is_empty()
                && having.is_none()
                && offset.is_none()
                && limit_cast.is_none()
                && aliased_sources.is_empty()
                && derived.is_none()
            {
                filter
            } else {
                Some(Filter::Clauses(Box::new(QueryClauses {
                    view_reads: vec![],
                    distinct,
                    filter,
                    group,
                    having,
                    offset,
                    page_casts: [limit_cast, offset_cast],
                    aliased_sources,
                    derived,
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
        subqueries: &Subplans<'_>,
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
            join.on.infer_with_subqueries(parameters, &scoped, tables, subqueries)?;
            let on = join.on.bind_with_subqueries(parameters, &scoped, tables, subqueries)?;
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
    /// Child queries must independently satisfy the same positive proof.
    pub(super) fn is_table_free(&self) -> bool {
        fn scalar(s: &Scalar) -> bool {
            match s {
                Scalar::Literal(_) | Scalar::Param(_) => true,
                Scalar::Column(_) => false,
                Scalar::Cast(s, _) | Scalar::Domain(s, ..) | Scalar::Unary(_, s) => scalar(s),
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
                Filter::Exists { query, .. } => query.is_table_free(),
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
                Item::Star | Item::QualifiedStar { .. } => false,
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
                Scalar::Cast(s, _) | Scalar::Domain(s, ..) | Scalar::Unary(_, s) => scalar(s),
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
                Filter::Exists { .. } => false,
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
                Item::Star | Item::QualifiedStar { .. } => false,
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
                Filter::Exists { query, .. } => out.extend(query.sequence_dependencies()),
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
        let mut out = self.derived().map_or_else(Vec::new, Query::sequence_dependencies);
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
                Filter::Exists { query, .. } => query.has_undefined_routine(),
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
        // Transaction-state routines must fall through to core's session execution context.
        const TRANSACTION_STATE_ROUTINES: &[&str] = &[
            "pg_current_xact_id",
            "pg_current_xact_id_if_assigned",
            "pg_xact_status",
        ];
        fn scalar(s: &Scalar) -> bool {
            match s {
                Scalar::Literal(_) | Scalar::Param(_) => true,
                Scalar::Column(_) => false,
                Scalar::Cast(s, _) | Scalar::Domain(s, ..) | Scalar::Unary(_, s) => scalar(s),
                Scalar::Binary(_, a, b) => scalar(a) && scalar(b),
                Scalar::Call(name, _) if TRANSACTION_STATE_ROUTINES.contains(&name.as_str()) => {
                    false
                }
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
                Filter::Exists { .. } => false,
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
                Item::Aggregate(_, _) | Item::Star | Item::QualifiedStar { .. } => false,
            })
            && predicate.as_ref().is_none_or(filter)
            && order.iter().all(|(s, _, _)| operand(s))
            && limit.as_ref().is_none_or(|e| match e {
                Expr::Literal(_) | Expr::Param(_) => true,
            })
    }
    // Reuse the parser's mandatory AND bounds; residual SQL is still evaluated.
    pub(super) fn v2_index_predicates(&self, params: &[Value]) -> Vec<(String, String, Value)> {
        if self.clauses().is_some_and(|c| !c.view_reads.is_empty()) { return Vec::new(); }
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
    /// Finite candidate tuples only; the publisher still evaluates the full WHERE.
    /// None preserves the paged scan for unsupported coercions or unbounded keys.
    pub(super) fn mutation_primary_keys(
        &self, state: &Relational, table: &TableDef, params: &[Value],
    ) -> Option<Vec<Vec<Value>>> {
        self.primary_key_candidates(state, table, params, 200).map(|(keys, _)| keys)
    }
    fn primary_key_candidates(
        &self, state: &Relational, table: &TableDef, params: &[Value], max_keys: usize,
    ) -> Option<(Vec<Vec<Value>>, bool)> {
        // Keep one candidate page within the existing 200-row staging budgets.
        fn supported(ty: &DataType) -> bool {
            matches!(ty, DataType::Boolean | DataType::SmallInt | DataType::Integer
                | DataType::BigInt | DataType::Text | DataType::Varchar | DataType::Bytes
                | DataType::Numeric | DataType::Real | DataType::DoublePrecision)
        }
        fn value(s: &Scalar, column: &ColumnDef, params: &[Value]) -> Option<Value> {
            if !supported(&column.data_type) { return None; }
            let v = match s {
                Scalar::Literal(v) => match unknown_comparison_literal(v, &column.data_type).ok()? {
                    Some(v) => v?,
                    None => v.clone(),
                },
                Scalar::Param(n) => params.get(n.checked_sub(1)?)?.clone(),
                _ => return None,
            };
            if v == Value::Null { return None; }
            if check(&v, column).is_err()
                || comparison_requires_filter(&v, &column.data_type).ok()? { return None; }
            Some(v)
        }
        // Skipping a noncandidate must not skip an error/volatile expression that
        // the old scan would evaluate before a later key test. Restrict residuals
        // to total, exact comparisons and NULL/boolean tests over simple columns.
        fn total(f: &Filter, table: &TableDef, params: &[Value]) -> bool {
            let column = |s: &Scalar| match s {
                Scalar::Column(c) => table.columns.iter().find(|d| d.name == c.name),
                _ => None,
            };
            match f {
                Filter::Logic(a, _, b) => total(a, table, params) && total(b, table, params),
                Filter::Not(a) | Filter::Is(a, _, _) => total(a, table, params),
                Filter::Compare(a, _, b) => {
                    if let Some(c) = column(a) {
                        if let Some(d) = column(b) {
                            supported(&c.data_type) && c.data_type == d.data_type
                        } else { value(b, c, params).is_some() }
                    } else if let Some(c) = column(b) { value(a, c, params).is_some() }
                    else { false }
                }
                Filter::Null(s, _) => matches!(s, Scalar::Column(_) | Scalar::Literal(_)),
                Filter::Scalar(Scalar::Literal(Value::Bool(_) | Value::Null)) => true,
                Filter::Scalar(s) => column(s).is_some_and(|c| c.data_type == DataType::Boolean),
                _ => false,
            }
        }
        fn domain(f: &Filter, column: &ColumnDef, params: &[Value], max_keys: usize) -> Option<BTreeMap<Vec<u8>, Value>> {
            match f {
                Filter::Compare(a, op, b) if op == "=" => {
                    let s = match (a, b) {
                        (Scalar::Column(c), s) | (s, Scalar::Column(c)) if c.name == column.name => s,
                        _ => return None,
                    };
                    let v = value(s, column, params)?;
                    Some(BTreeMap::from([(encode_key(std::slice::from_ref(&v)).ok()?, v)]))
                }
                Filter::Logic(a, and, b) => {
                    let (a, b) = (domain(a, column, params, max_keys), domain(b, column, params, max_keys));
                    if *and {
                        // Either mandatory conjunct supplies a sound superset.
                        match (a, b) {
                            (Some(a), Some(b)) => Some(if a.len() <= b.len() { a } else { b }),
                            (a, b) => a.or(b),
                        }
                    } else {
                        let mut out = a?;
                        out.extend(b?);
                        (out.len() <= max_keys).then_some(out)
                    }
                }
                _ => None,
            }
        }
        if self.sources.len() != 1 || !self.joins.is_empty() || self.has_in_subquery()
            || table.hidden_rowid().is_some() || table.primary_key.is_empty()
            || table.columns.iter().any(|c| state.column_type(c.id).is_some()) { return None; }
        let filter = self.filter.as_ref()?;
        // Leave NULL-key comparisons to the full WHERE evaluator, even when
        // another AND conjunct supplies a non-NULL candidate for the same key.
        fn null_key(f: &Filter, table: &TableDef, params: &[Value]) -> bool {
            match f {
                Filter::Logic(a, _, b) => null_key(a, table, params) || null_key(b, table, params),
                Filter::Not(a) | Filter::Is(a, _, _) => null_key(a, table, params),
                Filter::Compare(a, _, b) => {
                    let null = |s: &Scalar| match s {
                        Scalar::Literal(Value::Null) => true,
                        Scalar::Param(n) => n.checked_sub(1).and_then(|i| params.get(i)) == Some(&Value::Null),
                        _ => false,
                    };
                    let key = |s: &Scalar| matches!(s, Scalar::Column(c)
                        if table.primary_key.iter().any(|i| table.columns[*i].name == c.name));
                    key(a) && null(b) || key(b) && null(a)
                }
                _ => false,
            }
        }
        if null_key(filter, table, params) { return None; }
        // A complete existing point bound already skips all noncandidate rows,
        // so arbitrary residuals retain their current evaluation domain there.
        let exact_bounds = table.primary_key.iter().all(|i| {
            let predicates = self.predicates.iter()
                .filter(|(c, _, _)| c.name == table.columns[*i].name).collect::<Vec<_>>();
            let [(.., op, expr)] = predicates.as_slice() else { return false; };
            let scalar = match expr {
                Expr::Literal(v) => Scalar::Literal(v.clone()),
                Expr::Param(n) => Scalar::Param(*n),
            };
            *op == "=" && value(&scalar, &table.columns[*i], params).is_some()
        });
        if !exact_bounds && !total(filter, table, params) { return None; }
        // An exact domain contains each key column once, with only ORs within
        // that column's equality domain. Repeated AND bounds retain evaluation.
        fn exact_column(f: &Filter, table: &TableDef, params: &[Value]) -> Option<usize> {
            match f {
                Filter::Logic(a, false, b) => {
                    let column = exact_column(a, table, params)?;
                    (exact_column(b, table, params)? == column).then_some(column)
                }
                Filter::Compare(a, op, b) if op == "=" => {
                    let (c, s) = match (a, b) {
                        (Scalar::Column(c), s) | (s, Scalar::Column(c)) => (c, s),
                        _ => return None,
                    };
                    let i = *table.primary_key.iter().find(|i| table.columns[**i].name == c.name)?;
                    value(s, &table.columns[i], params)?;
                    Some(i)
                }
                _ => None,
            }
        }
        fn exact_columns(f: &Filter, table: &TableDef, params: &[Value], seen: &mut BTreeSet<usize>) -> bool {
            match f {
                Filter::Logic(a, true, b) => exact_columns(a, table, params, seen)
                    && exact_columns(b, table, params, seen),
                _ => exact_column(f, table, params).is_some_and(|i| seen.insert(i)),
            }
        }
        let mut seen = BTreeSet::new();
        let exact = exact_columns(filter, table, params, &mut seen) && seen.len() == table.primary_key.len();
        let mut tuples = BTreeMap::from([(Vec::new(), Vec::new())]);
        for offset in &table.primary_key {
            let values = domain(filter, &table.columns[*offset], params, max_keys)?;
            if tuples.len().checked_mul(values.len())? > max_keys { return None; }
            let mut next = BTreeMap::new();
            for prefix in tuples.into_values() {
                for v in values.values() {
                    let mut tuple = prefix.clone();
                    tuple.push(v.clone());
                    next.insert(encode_key(&tuple).ok()?, tuple);
                }
            }
            tuples = next;
        }
        Some((tuples.into_values().collect(), exact))
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
                Item::Star | Item::QualifiedStar { .. } => {
                    for source in self.star_sources(item)? {
                        let source = &self.sources[source];
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
            output += if matches!(item, Item::Star | Item::QualifiedStar { .. }) {
                self.star_sources(item)?
                    .into_iter()
                    .map(|source| {
                        let source = &self.sources[source];
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
                    Item::Star | Item::QualifiedStar { .. } => {
                        let width = self
                            .star_sources(item)?
                            .into_iter()
                            .map(|source| bound.tables[source].logical_columns().count())
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
                        BoundItem::Ctid(_) | BoundItem::Aggregate(..) => return Err(SqlError::Unsupported),
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
        outer: &[&[Source]],
    ) -> R<BoundQuery<'a>> {
        if parameters.len() > MAX_PARAMS {
            return Err(SqlError::Limit);
        }
        for (expr, limit) in self.limit.iter().map(|e| (e, true)).chain(self.offset().map(|e| (e, false))) {
            if let (Expr::Param(n), Some(ty)) = (expr, self.page_cast(limit)) {
                parameters.get_mut(n - 1).ok_or(SqlError::Parameters)?.get_or_insert_with(|| ty.clone());
            }
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
            .map(|s| views::table(state, &s.name))
            .collect::<R<Vec<_>>>()?;
        if outer.len() > subplans::MAX_DEPTH {
            return Err(SqlError::Limit);
        }
        let mut tables = tables;
        for scope in outer {
            for source in *scope {
                tables.push(views::table(state, &source.name)?);
            }
        }
        let outer_slots = RefCell::new(Vec::new());
        let resolve = |c: &ColumnRef, scope: usize| {
            let mapped = derived::column(self, c)?;
            outer::resolve(state, self, outer, &tables, mapped.as_ref().unwrap_or(c), scope, &outer_slots)
        };
        if self.has_in_subquery()
            && let Some(f) = self.where_filter()
        {
            f.infer_outer(&mut parameters, &resolve, &tables)?;
        }
        for (j, join) in self.joins.iter().enumerate() {
            let mut has_subquery = false;
            join.on.visit_subqueries(&mut |_, _| { has_subquery = true; Ok(()) })?;
            if has_subquery {
                if filter_contains_aggregate(&join.on) { return Err(SqlError::GroupingError); }
                let scoped = |c: &ColumnRef, _: usize| resolve(c, j + 2);
                join.on.infer_outer(&mut parameters, &scoped, &tables)?;
            }
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
            join.on.infer_with_subqueries(&mut parameters, &scoped, &tables, &subqueries)?;
        }
        if grouped {
            let mut bound = grouping::bind(self, &tables, parameters, &resolve, subqueries)?;
            bound.exists_keys = exists::probe_keys(self, &tables,
                &bound.parameters.iter().cloned().map(Some).collect::<Vec<_>>(), &resolve)?;
            bound.outer_slots = outer_slots.into_inner();
            if bound.outer_slots.is_empty() { bound.tables.truncate(self.sources.len()); }
            return Ok(bound);
        }
        let distinct_order = self
            .distinct()
            .then(|| {
                let projection = grouping::projection(self, &tables)?;
                self.order
                    .iter()
                    .map(|(s, _, _)| {
                        grouping::output_order(self, s, &projection, &resolve, &tables)
                    })
                    .collect::<R<Vec<_>>>()
            })
            .transpose()?;
        let distinct_order_slots = if let Some(order) = &distinct_order {
            let projection = grouping::projection(self, &tables)?;
            order
                .iter()
                .map(|s| grouping::output_slot(s, &projection, &resolve, &tables))
                .collect::<R<Vec<_>>>()?
        } else {
            vec![]
        };
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
        for (i, (e, _, _)) in self.order.iter().enumerate() {
            let e = distinct_order.as_ref().map_or(e, |order| &order[i]);
            e.infer(None, &mut parameters, &resolve, &tables)?;
        }
        let bind_expr = |e: &Expr, ty: &DataType, parameters: &mut [Option<DataType>]| -> R<bool> {
            if let Expr::Param(n) = e {
                let slot = parameters.get_mut(n - 1).ok_or(SqlError::Parameters)?;
                if let Some(old) = slot.as_ref().filter(|old| *old != ty) {
                    if integer_width(old) && integer_width(ty) {
                        return Ok(true);
                    }
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
                    if literal.ty != *ty
                        && functions::comparable(&literal.ty, ty)
                        && !(integer_width(&literal.ty) && integer_width(ty))
                    {
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
        for (e, limit) in self.limit.iter().map(|e| (e, true)).chain(self.offset().map(|e| (e, false))) {
            if self.page_cast(limit).is_some() { continue; }
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
        let joins = self.bind_joins(&tables, &mut parameters, &resolve, &subqueries)?;
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
                    if let Scalar::Column(c) = e
                        && c.name == "ctid"
                    {
                        let column = resolve(c, tables.len())?;
                        if tables[column.source].hidden_rowid() == Some(column.column) {
                            push(BoundItem::Ctid(column), ColumnDef { id: 0, name: "ctid".into(), data_type: DataType::Text, not_null: true });
                            continue;
                        }
                    }
                    let e = e.bind(&resolve, &tables, &parameters)?;
                    let (source_column, d) = if let Item::Scalar(Scalar::Column(c)) = item {
                        let name = c.name.clone();
                        let c = resolve(c, tables.len())?;
                        let mut d = tables[c.source].columns[c.column].clone();
                        if let Some(source) = self.sources.get(c.source) && let Some(view) = views::projection(state, &source.name)? {
                            d = view.columns.iter().find(|(_, offset)| *offset == c.column).ok_or(SqlError::UnknownColumn)?.0.clone();
                        }
                        d.name = name;
                        if c.source > 0 && self.joins.get(c.source - 1).is_some_and(|j| j.left) {
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
                Item::Star | Item::QualifiedStar { .. } => {
                    let sources = self.star_sources(item)?;
                    if self.sources.is_empty() {
                        return Err(SqlError::Unsupported);
                    }
                    if alias.is_some() {
                        return Err(SqlError::Unsupported);
                    }
                    for source in sources {
                        let t = tables[source];
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
            .enumerate()
            .map(|(i, (e, d, n))| {
                let ordinal;
                let e = if let Some(order) = &distinct_order {
                    &order[i]
                } else if let Scalar::Literal(Value::Int(k)) = e {
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
        let exists_keys = exists::probe_keys(self, &tables, &parameters, &resolve)?;
        let parameters = parameters
            .into_iter()
            .map(|p| p.ok_or(SqlError::Parameters))
            .collect::<R<Vec<_>>>()?;
        if outer_slots.borrow().is_empty() { tables.truncate(self.sources.len()); }
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
            exists_keys,
            distinct_order_slots,
            outer_slots: outer_slots.into_inner(),
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
    Ctid(BoundColumn),
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
    distinct_order_slots: Vec<usize>,
    exists_keys: Vec<exists::ProbeKey>,
    outer_slots: Vec<outer::OuterSlot>,
    subqueries: Subplans<'a>,
    declared: Vec<Option<DataType>>,
}
#[derive(Serialize, Deserialize)]
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
#[allow(clippy::too_many_arguments)]
fn join_rows<'a>(
    query: &'a BoundQuery<'_>,
    source: &'a dyn scan::RowSource,
    input: Box<dyn Iterator<Item = R<scan::SourceRow<'a>>> + 'a>,
    params: &'a [Value],
    meter: &'a mut scan::Meter<'_>,
    visits: &'a Cell<usize>,
    outer: &'a [Vec<Value>],
    charge_base: bool,
    state: &'a Relational,
    execution: &'a Execution<'_>,
) -> impl Iterator<Item = R<Tuple>> + 'a {
    // Retain owned point results only for this iterator's lifetime.
    let mut point_cache = std::collections::HashMap::<
        (usize, Vec<u8>), Option<(Vec<u8>, SqlRow)>>::new();
    let mut point_retention = scan::Retention::new(scan::retention_limit());
    let mut cache_full = false;
    #[cfg(test)]
    let cache_disabled = super::v2_join_cache_tests::DISABLED.get();
    #[cfg(not(test))]
    let cache_disabled = false;
    type Buckets = std::collections::HashMap<Vec<u8>, Vec<(Vec<u8>, SqlRow)>>;
    let mut buckets: Vec<Option<Buckets>> = (0..query.joins.len()).map(|_| None).collect();
    let mut bucket_disabled = vec![false; query.joins.len()];
    const BUCKET_AFTER_PROBES: usize = 64;
    let mut bucket_probes = vec![0usize; query.joins.len()];
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
                    if depth > 0 || charge_base {
                        charge_candidate(visits)?;
                    }
                    let candidate = candidate?;
                    if depth == 0 {
                        row.version = candidate.row.version;
                    }
                    let equality_match = depth == 0
                        || query.joins[depth - 1].matches(&row, &candidate.row.values)?;
                    row.values.push(candidate.row.values.clone());
                    row.values.resize_with(query.query.sources.len(), Vec::new);
                    row.values.extend_from_slice(outer);
                    if depth > 0 {
                        let mut work = 0;
                        if !equality_match
                            || query.joins[depth - 1].on.eval_with_subqueries(
                                &row, params, &mut work, query, state, source, execution)?
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
                    row.values.resize_with(query.query.sources.len(), Vec::new);
                    row.values.extend_from_slice(outer);
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
                    let keys = join.equalities.iter().filter(|_| source.streams_reductions()).filter(|(_, _, outer_ty, inner_ty)|
                        functions::join_key_supported(outer_ty, inner_ty))
                        .map(|(outer, inner, outer_ty, inner_ty)|
                            Ok((inner.column, functions::join_key(row.get(*outer), outer_ty, inner_ty)?)))
                        .collect::<R<Vec<_>>>()?;
                    #[cfg(test)]
                    let disabled = super::v2_join_cache_tests::BUCKET_DISABLED.get();
                    #[cfg(not(test))]
                    let disabled = false;
                    bucket_probes[depth] = bucket_probes[depth].saturating_add(1);
                    let eligible = bucket_probes[depth] > BUCKET_AFTER_PROBES && !disabled && !bucket_disabled[depth]
                        && source.streams_reductions() && !keys.is_empty()
                        && keys.len() == join.equalities.len();
                    if eligible && buckets[depth].is_none() {
                        #[cfg(test)]
                        let limit = super::v2_join_cache_tests::BUCKET_LIMIT.get().unwrap_or(scan::retention_limit());
                        #[cfg(not(test))]
                        let limit = scan::retention_limit();
                        let mut retention = scan::Retention::new(limit);
                        let mut built = Buckets::new();
                        for candidate in source.scan(table, &scan::KeyRange {
                            lo: std::ops::Bound::Unbounded,
                            hi: std::ops::Bound::Unbounded,
                        }, scan::Direction::Forward)? {
                            crate::cancellation::check().map_err(|_| SqlError::Aborted)?;
                            let candidate = candidate?;
                            let values = keys.iter().map(|(column, _)| candidate.row.values[*column].clone()).collect::<Vec<_>>();
                            if values.contains(&Value::Null) { continue; }
                            let key = encode_key(&values)?;
                            let bytes = key.len().saturating_add(candidate.key.len())
                                .saturating_add(std::mem::size_of::<(Vec<u8>, SqlRow)>())
                                .saturating_add(candidate.row.values.iter().map(value_bytes).sum::<usize>());
                            if retention.charge(bytes).is_err() {
                                bucket_disabled[depth] = true;
                                break;
                            }
                            built.entry(key).or_default().push((candidate.key.to_vec(), candidate.row.as_ref().clone()));
                        }
                        if !bucket_disabled[depth] {
                            buckets[depth] = Some(built);
                            #[cfg(test)]
                            super::v2_join_cache_tests::BUCKET_BUILDS.set(super::v2_join_cache_tests::BUCKET_BUILDS.get() + 1);
                        }
                    }
                    if eligible && !bucket_disabled[depth] {
                        let values = keys.iter().map(|(_, value)| value.clone().unwrap_or(Value::Null)).collect::<Vec<_>>();
                        let candidates = if values.contains(&Value::Null) { Vec::new() } else {
                            buckets[depth].as_ref().and_then(|b| b.get(&encode_key(&values).ok()?)).cloned().unwrap_or_default()
                        };
                        #[cfg(test)]
                        super::v2_join_cache_tests::BUCKET_PROBES.set(super::v2_join_cache_tests::BUCKET_PROBES.get() + 1);
                        Box::new(candidates.into_iter().map(|(key, row)| Ok(scan::SourceRow {
                            key: std::borrow::Cow::Owned(key), row: std::borrow::Cow::Owned(row),
                        })))
                    } else if let Some(rows) = source.join_probe(table, &keys, false)? { rows } else { source.scan(
                        table,
                        &scan::KeyRange {
                            lo: std::ops::Bound::Unbounded,
                            hi: std::ops::Bound::Unbounded,
                        },
                        scan::Direction::Forward,
                    )? }
                } else {
                    let candidate = match join.key(&row)? {
                        Some(key) => {
                            let key = (depth, encode_key(&key)?);
                            if let Some(cached) = point_cache.get(&key) {
                                cached.as_ref().map(|(key, row)| scan::SourceRow {
                                    key: std::borrow::Cow::Owned(key.clone()),
                                    row: std::borrow::Cow::Owned(row.clone()),
                                })
                            } else {
                                #[cfg(test)]
                                super::v2_join_cache_tests::GETS.set(super::v2_join_cache_tests::GETS.get() + 1);
                                let candidate = source.get(table, &key.1)?;
                                if !cache_full && !cache_disabled {
                                    let bytes = key.1.len().saturating_add(
                                        std::mem::size_of::<((usize, Vec<u8>), Option<(Vec<u8>, SqlRow)>)>()
                                    ).saturating_add(candidate.as_ref().map_or(0, |c| {
                                        c.key.len().saturating_add(c.row.values.iter().map(value_bytes).sum::<usize>())
                                    }));
                                    if point_retention.charge(bytes).is_ok() {
                                        point_cache.insert(key, candidate.as_ref().map(|c| {
                                            (c.key.to_vec(), c.row.as_ref().clone())
                                        }));
                                    } else {
                                        cache_full = true;
                                    }
                                }
                                candidate
                            }
                        },
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
                BoundItem::Ctid(column) => match row.get(*column) {
                    Value::Int(rowid) => {
                        let value = Value::Text(super::ctid::format(*rowid)?);
                        charge(bytes, &value)?;
                        Ok(value)
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(SqlError::Type),
                },
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
            type_oids: if self.items.iter().any(|item| matches!(item, BoundItem::Ctid(_))) {
                self.items.iter().map(|item| if matches!(item, BoundItem::Ctid(_)) { 27 } else { 0 }).collect()
            } else {
                vec![]
            },
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
                            BoundItem::Ctid(_) | BoundItem::Aggregate(..) => TypmodSource::Other,
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
    fn input_rows<'s>(&'s self, source: &'s dyn scan::RowSource, params: &[Value],
        range: &scan::KeyRange, empty: bool) -> R<scan::SourceRows<'s>> {
        let predicates = self.predicates.iter().filter(|(c, _, _)| c.source == 0)
            .map(|(c, op, e)| Ok((c.column, *op, e.value(params)?))).collect::<R<Vec<_>>>()?;
        if !empty && let Some(rows) = source.index_input(self.tables[0], &predicates)? {
            Ok(rows)
        } else { source.scan(self.tables[0], range, scan::Direction::Forward) }
    }
    pub(super) fn cursor_direction(&self) -> Option<scan::Direction> {
        if self.tables.len() != 1
            || !self.joins.is_empty()
            || self.aggregate
            || self.grouping.is_some()
            || self.query.distinct()
            || self.query.explain
            || self.query.has_in_subquery()
            || self.query.derived().is_some()
            || self.query.clauses().is_some_and(|c| !c.view_reads.is_empty())
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
        state: &Relational,
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
                let mut value = expr.map(|e| self.query.page_value(e, params, numeric)).transpose()?;
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
                    Some(Value::Int(_)) => Err(if numeric {
                        SqlError::InvalidRowCountInLimitClause
                    } else {
                        SqlError::InvalidRowCountInResultOffsetClause
                    }),
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
        // Only unordered queries with no useful PK range may change access order.
        let mut indexed = None;
        if self.order.is_empty()
            && range == scan::KeyRange::from_bounds(super::bounds(self.tables[0], &[], params)?)
        {
            let mut predicates = Vec::new();
            // The generic bound list drops even exact integer-width comparisons.
            // Recheck original predicates against this single, already-bound table.
            for (c, op, expr) in &self.query.predicates {
                let offset = col(self.tables[0], &c.name)?;
                let column = &self.tables[0].columns[offset];
                let mut value = expr.value(params)?;
                if let Expr::Literal(literal) = expr
                    && let Some(typed) = unknown_comparison_literal(literal, &column.data_type)?
                {
                    let Some(typed) = typed else { continue };
                    value = typed;
                }
                if matches!(
                    column.data_type,
                    DataType::Boolean
                        | DataType::SmallInt
                        | DataType::Integer
                        | DataType::BigInt
                        | DataType::Text
                        | DataType::Varchar
                        | DataType::Bytes
                        | DataType::Numeric
                        | DataType::Real
                        | DataType::DoublePrecision
                ) && value != Value::Null
                    && check(&value, column).is_ok()
                    && !comparison_requires_filter(&value, &column.data_type)?
                {
                    predicates.push((offset, op.as_str(), value));
                }
            }
            indexed = source.index_scan(self.tables[0], &predicates, progress.after.as_deref())?;
        }
        if let Some(key) = &progress.after {
            match direction {
                scan::Direction::Forward => range.lo = Bound::Excluded(key.clone()),
                scan::Direction::Reverse => range.hi = Bound::Excluded(key.clone()),
            }
        }
        #[cfg(not(test))]
        let points_enabled = true;
        #[cfg(test)]
        let points_enabled = !select_points::DISABLE_SELECT_POINTS.get();
        #[cfg(not(test))]
        let cap = 2048;
        #[cfg(test)]
        let cap = select_points::SELECT_KEY_CAP.get().unwrap_or(2048);
        let mut exact_points = false;
        let mut input = if points_enabled && self.tables.len() == 1 && self.joins.is_empty()
            && let Some((keys, exact)) = self.query.primary_key_candidates(state, self.tables[0], params, cap)
        {
            exact_points = exact;
            let mut keys = keys.iter().map(|key| encode_key(key)).collect::<R<Vec<_>>>()?;
            keys.sort();
            keys.dedup();
            if matches!(direction, scan::Direction::Reverse) { keys.reverse(); }
            let after = progress.after.clone();
            let table = self.tables[0];
            Box::new(keys.into_iter().filter(move |key| after.as_ref().is_none_or(|after| match direction {
                scan::Direction::Forward => key > after,
                scan::Direction::Reverse => key < after,
            })).filter_map(move |key| {
                if crate::cancellation::check().is_err() { return Some(Err(SqlError::Aborted)); }
                #[cfg(test)]
                select_points::SELECT_POINT_READS.with(|reads| reads.set(reads.get() + 1));
                source.get(table, &key).transpose()
            })) as scan::SourceRows<'_>
        } else { match indexed {
            Some(input) => input,
            None => source.scan(self.tables[0], &range, direction)?,
        }};
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
            if exact_points {
                #[cfg(test)]
                select_points::SELECT_FILTER_SKIPS.with(|count| count.set(count.get() + 1));
            }
            if !exact_points && let Some(filter) = &self.filter
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
        let mut rows = Vec::new();
        self.matching_mutation(state, source, params, retention, cancel, false,
            &mut |row| rows.push((row.key.into_owned(), row.row.into_owned())))?;
        Ok(rows)
    }
    /// Freeze only target identities, never complete target/source row images.
    pub(super) fn matching_keys(
        &self,
        state: &Relational,
        source: &dyn scan::RowSource,
        params: &[Value],
    ) -> R<BTreeSet<Vec<u8>>> {
        let mut retention = scan::Retention::new(scan::retention_limit());
        let mut keys = BTreeSet::new();
        self.matching_mutation(state, source, params, &mut retention,
            &mut || crate::cancellation::check().is_err(), true,
            &mut |row| { keys.insert(row.key.into_owned()); })?;
        Ok(keys)
    }
    #[expect(clippy::too_many_arguments, reason = "shared mutation scan and retained representation")]
    fn matching_mutation(
        &self, state: &Relational, source: &dyn scan::RowSource, params: &[Value],
        retention: &mut scan::Retention, cancel: &mut impl FnMut() -> bool, keys_only: bool,
        retain: &mut impl FnMut(scan::SourceRow<'_>),
    ) -> R<()> {
        let execution = Execution::new(cancel, &self.declared);
        execution.retention.borrow_mut().charge(retention.used())?;
        if !self.outer_slots.is_empty() {
            return Err(SqlError::Unsupported); // C4 supplies outer tuples.
        }
        self.subqueries.visit(&mut |mode, child| {
            if mode == SubplanMode::Exists || !child.correlated(0) { Ok(()) } else { Err(SqlError::Unsupported) }
        })?;
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
        for (count, row) in filtered.enumerate() {
            let row = row?;
            if !keys_only && count >= scan::query_row_limit().min(super::MAX_ROWS) {
                return Err(SqlError::Limit);
            }
            let bytes = if keys_only { row.key.len() + 96 }
                else { searched::retained_row_bytes(&row.key, &row.row)? };
            retention.charge(bytes)?;
            execution.retention.borrow_mut().charge(bytes)?;
            retain(row);
        }
        Ok(())
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
        let materialized = self.materialize_derived(state, source, params, declared, execution)?;
        let source: &dyn scan::RowSource = materialized.as_ref().map_or(source, |rows| rows);
        if !self.outer_slots.is_empty() {
            return Err(SqlError::Unsupported); // C4 supplies outer tuples.
        }
        self.subqueries.visit(&mut |mode, child| {
            if mode == SubplanMode::Exists || !child.correlated(0) { Ok(()) } else { Err(SqlError::Unsupported) }
        })?;
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
            return grouping.execute(self, state, source, params, execution, false);
        }
        let offset = match self.query.offset() {
            Some(e) => match self.query.page_value(e, params, false)? {
                Value::Int(n) if n >= 0 => n as u64,
                Value::Int(_) => return Err(SqlError::InvalidRowCountInResultOffsetClause),
                Value::Null => 0,
                _ => return Err(SqlError::Limit),
            },
            None => 0,
        };
        let limit = match &self.query.limit {
            Some(e) => {
                let value = self.query.page_value(e, params, true)?;
                let value = if matches!(e, Expr::Param(n)
                    if declared.get(n - 1) == Some(&Some(DataType::Numeric)))
                {
                    assignment::convert(value, &DataType::Numeric, &DataType::BigInt, None)?
                } else {
                    value
                };
                match value {
                    Value::Int(n) if n >= 0 => Some(n as u64),
                    Value::Int(_) => return Err(SqlError::InvalidRowCountInLimitClause),
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
        #[cfg(not(test))]
        let points_enabled = true;
        #[cfg(test)]
        let points_enabled = !select_points::DISABLE_SELECT_POINTS.get();
        const SELECT_PRIMARY_KEY_CAP: usize = 2048;
        #[cfg(not(test))]
        let select_key_cap = SELECT_PRIMARY_KEY_CAP;
        #[cfg(test)]
        let select_key_cap = select_points::SELECT_KEY_CAP.get().unwrap_or(SELECT_PRIMARY_KEY_CAP);
        let mut exact_points = false;
        let input = if points_enabled && !empty_input && self.tables.len() == 1 && self.joins.is_empty()
            && let Some((keys, exact)) = self.query.primary_key_candidates(state, self.tables[0], params, select_key_cap)
        {
            exact_points = exact;
            let mut keys = keys.iter().map(|key| encode_key(key)).collect::<R<Vec<_>>>()?;
            keys.sort();
            keys.dedup();
            let table = self.tables[0];
            Box::new(keys.into_iter().filter_map(move |key| {
                if crate::cancellation::check().is_err() { return Some(Err(SqlError::Aborted)); }
                #[cfg(test)]
                select_points::SELECT_POINT_READS.with(|reads| reads.set(reads.get() + 1));
                source.get(table, &key).transpose()
            })) as scan::SourceRows<'_>
        } else { self.input_rows(source, params, &range, empty_input)? };
        let mut meter = if subquery || self.aggregate || !self.joins.is_empty() {
            scan::Meter::per_row(cancel)
        } else {
            scan::Meter::new(cancel)
        };
        let joined = join_rows(self, source, input, params, &mut meter, &execution.visits, &[], false, state, execution);
        let filtered = joined.filter_map(|row| {
            let row = match row {
                Ok(row) => row,
                Err(e) => return Some(Err(e)),
            };
            if exact_points {
                #[cfg(test)]
                select_points::SELECT_FILTER_SKIPS.with(|count| count.set(count.get() + 1));
                return Some(Ok(row));
            }
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
        let page = scan::Page { offset, limit };
        let mut result = SqlResult {
            columns: self.columns.iter().map(|c| c.name.clone()).collect(),
            rows: vec![],
            affected: 0,
        };
        let mut result_bytes = 0usize;
        // Computed aggregate inputs keep the statement-wide scalar work budget (DD-0068
        // aggregate_scalar_work_budget_is_shared_over_full_input). On v2, SUM/AVG
        // transitions use a per-value budget like PostgreSQL's constant-size state.
        let mut aggregate_work_bytes = 0usize;
        if self.query.distinct() && !self.aggregate {
            let mut seen = std::collections::BTreeSet::new();
            let mut unique_rows = Vec::new();
            let mut distinct_bytes = 0;
            for row in filtered {
                let row = row?;
                let projected = self.project_tuple(&row, params, &mut 0, &mut 0)?;
                let key = grouping::Key(projected.values);
                if seen.contains(&key) {
                    continue;
                }
                if unique_rows.len() >= scan::query_row_limit() {
                    return Err(SqlError::Limit);
                }
                distinct_bytes += distinct_charge(
                    &key.0,
                    &self.distinct_order_slots,
                    &mut execution.retention.borrow_mut(),
                )?;
                let keys = self
                    .order
                    .iter()
                    .enumerate()
                    .map(|(i, (s, _, _))| {
                        let slot = self.distinct_order_slots[i];
                        if self.columns[slot].data_type != s.ty {
                            s.eval(&row, params, &mut 0)
                        } else {
                            Ok(key.0[slot].clone())
                        }
                    })
                    .collect::<R<Vec<_>>>()?;
                let projected = SqlRow {
                    values: key.0.clone(),
                    version: projected.version,
                };
                seen.insert(key);
                unique_rows.push((projected, keys));
            }
            unique_rows.sort_by(|(_, a), (_, b)| order_values(a, b, &self.order));
            for (row, _) in page.apply(unique_rows.into_iter()) {
                for value in &row.values {
                    charge(&mut result_bytes, value)?;
                }
                result.rows.push(row);
            }
            execution.retention.borrow_mut().release(distinct_bytes);
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
            let scope = RetentionScope::new(execution);
            let mut auxiliary = aggregate_auxiliary(&self.items, &mut scope.borrow())?;
            if source.streams_reductions() {
                for (aux, (item, _)) in auxiliary.iter_mut().zip(&self.query.items) {
                    aux.stream_column = matches!(item, Item::Aggregate(a, Some(Scalar::Column(_))) if !a.distinct());
                    aux.local_transition = true;
                }
            }
            scope.borrow().charge(aggregate_retained(&values))?;
            for row in filtered {
                let row = row?;
                let mut retention = scope.borrow();
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
                    // Buffered table images visit primary keys in order. Preserve
                    // that stable tie order even when a secondary index supplied input.
                    let stable_key = (source.streams_reductions() && limit.is_none())
                        .then(|| pk(self.tables[0], &row.values[0])).transpose()?;
                    Ok((row, keys, stable_key))
                });
                let mut retention = RetentionScope::new(execution);
                let size = |(row, keys, stable_key): &(Tuple, Vec<Value>, Option<Vec<u8>>)| {
                        let payload: usize = row.values
                            .iter()
                            .flatten()
                            .chain(keys)
                            .map(value_bytes)
                            .sum();
                        // Unbounded streamed sorts retain owned tuples rather than a
                        // hydrated table image. Charge their allocation overhead too.
                        let overhead = if source.streams_reductions() && limit.is_none() {
                            std::mem::size_of::<(Tuple, Vec<Value>, Option<Vec<u8>>, usize, usize)>()
                                + row.values.len() * std::mem::size_of::<Vec<Value>>()
                                + (row.values.iter().map(Vec::len).sum::<usize>() + keys.len())
                                    * std::mem::size_of::<Value>()
                                + stable_key.as_ref().map_or(0, Vec::len)
                        } else { 0 };
                        payload + overhead
                    };
                let compare = |(_, a, a_key): &(Tuple, Vec<Value>, Option<Vec<u8>>),
                               (_, b, b_key): &(Tuple, Vec<Value>, Option<Vec<u8>>)| {
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
                        a_key.cmp(b_key)
                    };
                // v2 full sorts spill sorted runs past work_mem (PostgreSQL's
                // external sort); top-N sorts keep only OFFSET + LIMIT rows.
                if source.streams_reductions() && limit.is_none() {
                    let sorted = spill::sort_external(keyed, size, compare, &mut retention)?;
                    Box::new(sorted.map(|item| item.map(|(row, _, _)| row)))
                } else {
                    let sorted = scan::sort_bounded(keyed, page.keep(), size, compare, &mut retention)?;
                    Box::new(sorted.into_iter().map(|(row, _, _)| Ok(row)))
                }
            };
            // A v2 SqlSession statement's own output spills past SPILL_BYTES like
            // a tuplestore, so it has no row or byte ceiling; each row still does.
            let mut output = (source.streams_reductions() && !subquery && !insert_source && spill::armed())
                .then(|| spill::RowBuffer::new(Vec::new()))
                .transpose()
                .map_err(spill::sql_error)?;
            for row in page.apply(rows) {
                let row = row?;
                let mut scalar_work_bytes = 0usize;
                if let Some(output) = output.as_mut() {
                    let row = self.project_tuple(&row, params, &mut scalar_work_bytes, &mut 0)?;
                    output.push(row).map_err(spill::sql_error)?;
                    continue;
                }
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
            if let Some(output) = output {
                output.finish(&mut result).map_err(spill::sql_error)?;
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
                Item::Star | Item::QualifiedStar { .. } => {
                    for index in self.star_sources(item)? {
                        let source = &self.sources[index];
                        let table = tables[index];
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

fn distinct_charge(values: &[Value], slots: &[usize], retention: &mut scan::Retention) -> R<usize> {
    let retained = |values: &[Value]| {
        std::mem::size_of_val(values)
            + values
                .iter()
                .map(|v| match v {
                    Value::Numeric(n) => n.display_byte_bound(),
                    _ => value_bytes(v),
                })
                .sum::<usize>()
    };
    let keys = slots
        .iter()
        .map(|i| retained(&values[*i..=*i]).max(std::mem::size_of::<Value>() + 9))
        .sum::<usize>();
    let bytes = 2 * retained(values)
        + keys
        + 4 * std::mem::size_of::<(SqlRow, Vec<Value>)>()
        + 12 * std::mem::size_of::<grouping::Key>()
        + 12 * std::mem::size_of::<usize>();
    retention.charge(bytes)?;
    Ok(bytes)
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
            Self::Exists { .. } => {}
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
        self.subqueries.attach_clock(clock);
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
    stream_column: bool,
    // V2: SUM/AVG numeric transitions charge a per-value budget, not the
    // statement-wide one, so constant-size running state never hits 54000.
    local_transition: bool,
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
            stream_column: false,
            local_transition: false,
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
                sum => {
                    let mut local = 0;
                    let budget = if state.local_transition { &mut local } else { &mut *work };
                    functions::aggregate_numeric_average(sum, state.count, budget)?
                }
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
        // A fixed-width or numeric column read is one stored value. V2 keeps this
        // scratch row-local; computed arguments retain DD-0068's cumulative budget.
        let mut column_work = 0;
        let evaluated = c.as_ref().map(|e| e.eval(row, params,
            if auxiliary[index].stream_column && matches!(e.ty,
                DataType::SmallInt | DataType::Integer | DataType::BigInt | DataType::Numeric
                | DataType::Boolean | DataType::Real | DataType::DoublePrecision) {
                &mut column_work
            } else { &mut *work })).transpose()?;
        // SUM/AVG keep a running sum and count; on v2 each transition is guarded
        // per value (digit/scale limits, 22003) rather than cumulatively.
        let mut transition_work = 0;
        let transition: &mut usize =
            if auxiliary[index].local_transition { &mut transition_work } else { &mut *work };
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
                    functions::aggregate_numeric_add(value, v.clone(), transition)?
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
                        transition,
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
                let mut execution_cancel = || false;
                let execution = Execution::new(&mut execution_cancel, &[]);
                let result = join_rows(&bound, &source, input, &[], &mut meter, &visits, &[], false, &state, &execution)
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

    #[test]
    fn transaction_status_routing() {
        for sql in [
            "SELECT pg_current_xact_id()",
            "SELECT pg_current_xact_id_if_assigned()",
            "SELECT pg_xact_status(5)",
        ] {
            assert!(!query(sql).rejects_unsupported_table_free_scalar(), "{sql}");
        }
        assert!(query("SELECT random()").rejects_unsupported_table_free_scalar());
    }

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
    constraint_check, constraint_columns, constraint_deparse, constraint_eval, constraint_key,
    constraint_key_supported, constraint_quote, constraint_shape, constraint_types_compatible,
    constraint_validate,
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
    fn duplicates_fit_one_key_and_tiny_budget_fails_before_state_allocation() {
        let state = state();
        let q = query("SELECT DISTINCT id-id FROM in_outer LIMIT 1");
        let bound = q.bind(&state, 0, &[]).unwrap();
        let mut meter = scan::Retention::new(4096);
        let one_key = distinct_charge(&[Value::Int(0)], &[], &mut meter).unwrap();
        for cap in [1, one_key] {
            let mut cancel = || false;
            let execution = Execution::new(&mut cancel, &[]);
            *execution.retention.borrow_mut() = scan::Retention::new(cap);
            let result = bound.execute_context(
                &state,
                &scan::LegacyRows(&state),
                &[],
                &[],
                &execution,
                false,
                false,
            );
            if cap == 1 {
                assert_eq!(result, Err(SqlError::Limit));
            } else {
                assert_eq!(result.unwrap().rows.len(), 1);
            }
            assert_eq!(execution.retention.borrow().used(), 0);
        }
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

#[cfg(test)]
mod integer_bound_tests {
    use super::*;
    #[test]
    fn integer_widths_keep_exact_point_and_range_access() {
        let mut state = Relational::default();
        for key in ["smallint", "integer", "bigint"] {
            let ddl = Prepared::parse(
                &format!("CREATE TABLE t_{key}(id {key} PRIMARY KEY)"),
                1,
                state.epoch,
            )
            .unwrap();
            state
                .execute_with_cancel(&ddl, &[], None, || false)
                .unwrap();
            for ty in [DataType::SmallInt, DataType::Integer, DataType::BigInt] {
                for (predicate, count) in [("id = 10000", 0), ("id = $1", 1), ("id >= $1", 1)] {
                    let p = Prepared::parse(
                        &format!("SELECT id FROM t_{key} WHERE {predicate}"),
                        1,
                        state.epoch,
                    )
                    .unwrap();
                    let Statement::Query(q) = p.statement else {
                        panic!("query")
                    };
                    let declared = vec![Some(ty.clone()); count];
                    let bound = q.bind(&state, count, &declared).unwrap();
                    let (_, bounds) = bound.base_bounds(&vec![Value::Int(10000); count]).unwrap();
                    assert_eq!(bounds.low, encode_key(&[Value::Int(10000)]).unwrap());
                    assert_eq!(bounds.point, !predicate.contains(">="));
                }
            }
        }
    }
}

#[cfg(test)]
mod mutation_primary_key_tests {
    use super::*;
    fn image(ddl: &str) -> Relational {
        let mut state = Relational::default();
        let p = Prepared::parse(ddl, 1, state.epoch).unwrap();
        state.execute(&p, &[], None).unwrap();
        state
    }
    fn keys(state: &Relational, sql: &str, params: &[Value]) -> Option<Vec<Vec<Value>>> {
        let p = Prepared::parse(sql, 1, state.epoch).unwrap();
        searched::target(&p).unwrap().mutation_primary_keys(state, &state.tables["t"], params)
    }
    #[test]
    fn equalities_membership_residuals_and_safe_fallbacks() {
        use Value::{Int, Null};
        let state = image("CREATE TABLE t(id bigint PRIMARY KEY,v bigint)");
        for (sql, params, expected) in [
            ("DELETE FROM t WHERE id=2", vec![], Some(vec![vec![Int(2)]])),
            ("UPDATE t SET v=v+1 WHERE id=$1", vec![Int(3)], Some(vec![vec![Int(3)]])),
            ("DELETE FROM t WHERE 2=id", vec![], Some(vec![vec![Int(2)]])),
            ("DELETE FROM t WHERE id IN(3,1,3,$1) AND v>0", vec![Int(2)], Some(vec![vec![Int(1)],vec![Int(2)],vec![Int(3)]])),
            ("DELETE FROM t WHERE id IN(1,NULL)", vec![], None),
            ("DELETE FROM t WHERE id=1 AND id IN(1,NULL)", vec![], None),
            ("DELETE FROM t WHERE id=NULL", vec![], None),
            ("DELETE FROM t WHERE id=$1", vec![Null], None),
            ("DELETE FROM t WHERE id='2'", vec![], Some(vec![vec![Int(2)]])),
            ("DELETE FROM t WHERE id=$1", vec![Value::Text("bad".into())], None),
            ("DELETE FROM t WHERE id>2", vec![], None),
            ("DELETE FROM t WHERE id=1 OR v=2", vec![], None),
            // With no original point bound, skipping rows must not hide errors.
            ("DELETE FROM t WHERE 1/v>0 AND id IN(1,2)", vec![], None),
            ("DELETE FROM t WHERE $1 IS NULL AND id IN(1,2)", vec![Null], None),
            ("DELETE FROM t WHERE id=1 AND 1/v>0", vec![], Some(vec![vec![Int(1)]])),
            ("DELETE FROM t WHERE id IN(SELECT v FROM t)", vec![], None),
        ] {
            assert_eq!(keys(&state,sql,&params),expected,"{sql}");
        }
        let state = image("CREATE TABLE t(id uuid PRIMARY KEY)");
        assert!(keys(&state,"DELETE FROM t WHERE id='550E8400-E29B-41D4-A716-446655440000'",&[]).is_none());
    }
    #[test]
    fn user_defined_comparison_types_keep_their_existing_path() {
        let mut state=Relational::default();
        for sql in ["CREATE TYPE mood AS ENUM('ok','bad')", "CREATE TABLE t(id bigint PRIMARY KEY,v mood)"] {
            let p=Prepared::parse(sql,1,state.epoch).unwrap();
            state.execute(&p,&[],None).unwrap();
        }
        assert!(keys(&state,"DELETE FROM t WHERE id IN(1,2) AND v=$1",&[Value::Text("unknown".into())]).is_none());
    }
    #[test]
    fn composite_cartesian_expansion_is_ordered_and_capped() {
        use Value::Int;
        let state = image("CREATE TABLE t(a bigint,b bigint,v bigint,PRIMARY KEY(a,b))");
        assert_eq!(keys(&state,"DELETE FROM t WHERE b IN(2,1,2) AND a IN(2,1)",&[]),
            Some(vec![vec![Int(1),Int(1)],vec![Int(1),Int(2)],vec![Int(2),Int(1)],vec![Int(2),Int(2)]]));
        assert!(keys(&state,"DELETE FROM t WHERE a=1",&[]).is_none());
        let list = (0..15).map(|i| i.to_string()).collect::<Vec<_>>().join(",");
        let under = (0..13).map(|i| i.to_string()).collect::<Vec<_>>().join(",");
        assert_eq!(keys(&state,&format!("DELETE FROM t WHERE a IN({under}) AND b IN({list})"),&[]).unwrap().len(),195);
        assert!(keys(&state,&format!("DELETE FROM t WHERE a IN({list}) AND b IN({list})"),&[]).is_none());
    }
}

#[cfg(test)]
#[path = "../storage/v2_select_points_tests.rs"]
pub(crate) mod select_points;
