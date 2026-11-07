//! Private syntax, binding and bounded physical execution. No persisted types.
use super::*;
use std::cmp::Ordering;
mod functions;
mod grouping;
mod insert_values;
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
    equalities: Vec<(ColumnRef, ColumnRef)>,
}
impl Join {
    fn parse_equalities(
        p: &mut Parser,
        equalities: &mut Vec<(ColumnRef, ColumnRef)>,
        depth: usize,
    ) -> R<()> {
        if depth > 32 {
            return Err(SqlError::Limit);
        }
        loop {
            if p.sym('(') {
                Self::parse_equalities(p, equalities, depth + 1)?;
                p.punct(')')?;
            } else {
                let a = p.column_ref()?;
                if p.tokens.get(p.at) != Some(&Token::Op("=".into())) {
                    return Err(SqlError::Unsupported);
                }
                p.at += 1;
                equalities.push((a, p.column_ref()?));
                if equalities.len() > MAX_COLUMNS {
                    return Err(SqlError::Limit);
                }
            }
            if !p.word("and") {
                return Ok(());
            }
        }
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
    Null(Scalar, bool),
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
    Null(BoundScalar, bool),
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
impl Filter {
    fn parse(p: &mut Parser, precedence: usize, depth: usize) -> R<Self> {
        if depth > 32 {
            return Err(SqlError::Limit);
        }
        let mut f = if p.word("not") {
            Self::Not(Box::new(Self::parse(p, 3, depth + 1)?))
        } else {
            // Parenthesized scalar operands and boolean groups share a prefix.
            let start = p.at;
            let params = p.params.clone();
            let scalar = Scalar::parse(p, depth);
            match scalar {
                Ok(a) => {
                    let not = matches!(p.tokens.get(p.at), Some(Token::Word(w)) if w == "not")
                        && matches!(p.tokens.get(p.at + 1), Some(Token::Word(w)) if w == "in" || w == "between" || w == "like" || w == "ilike");
                    if not {
                        p.at += 1;
                    }
                    if p.word("is") {
                        let not = p.word("not");
                        p.need("null")?;
                        Self::Null(a, not)
                    } else if matches!(p.tokens.get(p.at), Some(Token::Word(w)) if w == "like" || w == "ilike")
                    {
                        let insensitive = p.word("ilike");
                        if !insensitive {
                            p.need("like")?;
                        }
                        let pattern = Scalar::parse(p, depth + 1)?;
                        let escape = if p.word("escape") {
                            Some(Scalar::parse(p, depth + 1)?)
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
                        let mut values = vec![Scalar::parse(p, depth + 1)?];
                        while p.sym(',') {
                            values.push(Scalar::parse(p, depth + 1)?);
                        }
                        p.punct(')')?;
                        // Admit the expanded size from the borrowed AST before any clone.
                        admit_expansion(&a, values.len())?;
                        Self::membership(&a, &values, not)
                    } else if p.word("between") {
                        if p.word("symmetric") {
                            return Err(SqlError::Unsupported);
                        }
                        p.word("asymmetric");
                        let lower = Scalar::parse(p, depth + 1)?;
                        p.need("and")?;
                        let upper = Scalar::parse(p, depth + 1)?;
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
                        Self::Compare(a, op, Scalar::parse(p, depth + 1)?)
                    } else {
                        Self::Scalar(a)
                    }
                }
                Err(e) => {
                    p.at = start;
                    p.params = params;
                    if !p.sym('(') {
                        return Err(e);
                    }
                    let f = Self::parse(p, 0, depth + 1)?;
                    p.punct(')')?;
                    f
                }
            }
        };
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
                Box::new(Self::parse(p, level, depth + 1)?),
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
            Self::Clauses(_) => Err(SqlError::Unsupported),
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
            Self::Not(a) => a.infer(params, resolve, tables),
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
            Self::Not(a) => a.infer_mutation(params, declared, resolve, tables),
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
            Self::Clauses(_) => return Err(SqlError::Unsupported),
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
                    if bound.ty != DataType::Text && !matches!(a, Scalar::Literal(Value::Null)) {
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
            Self::Logic(a, and, b) => BoundFilter::Logic(
                Box::new(a.bind(params, resolve, tables)?),
                *and,
                Box::new(b.bind(params, resolve, tables)?),
            ),
        })
    }
}
impl BoundFilter {
    /// Immutable planning only: outer None is dependent; inner None is SQL NULL.
    fn plan_constant(&self, budget: &mut usize) -> R<Option<Option<bool>>> {
        let boolean = |value| match value {
            Value::Bool(v) => Ok(Some(v)),
            Value::Null => Ok(None),
            _ => Err(SqlError::Type),
        };
        match self {
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
            name.clone()
        };
        Ok(Source { name, alias })
    }
}

// DISTINCT is syntax owned by this query layer; scalar calls retain their
// ordinary argument tree so dependency and authorization walkers see every input.
fn preprocess_aggregates(p: &mut Parser) -> R<()> {
    let mut n = p.at;
    while n + 2 < p.tokens.len() {
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
        Filter::Scalar(s) | Filter::Null(s, _) => grouping::contains_aggregate(s),
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
        Filter::Not(f) => filter_contains_aggregate(f),
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
            (n.starts_with("public.\0") || n.starts_with("schema.\0"))
                || args.iter().any(scalar_has_undefined_routine)
        }
        Scalar::Unary(_, a) | Scalar::Cast(a, _) => scalar_has_undefined_routine(a),
        Scalar::Binary(_, a, b) => {
            scalar_has_undefined_routine(a) || scalar_has_undefined_routine(b)
        }
        Scalar::SubstringOrder(args) => args.iter().any(|a| scalar_has_undefined_routine(a)),
        _ => false,
    }
}
impl Query {
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
                        Some(Scalar::parse_aggregate_argument(p, 0)?)
                    };
                    p.punct(')')?;
                    Item::Aggregate(a, c)
                } else {
                    Item::Scalar(Scalar::parse(p, 0)?)
                }
            };
            if matches!(item, Item::Aggregate(..))
                && matches!(p.tokens.get(p.at), Some(Token::Op(op)) if ["+", "-", "*", "/", "%", "||"].contains(&op.as_str()))
            {
                for n in item_start..p.tokens.len().saturating_sub(3) {
                    if matches!(&p.tokens[n], Token::Word(w) if w == "count")
                        && p.tokens[n + 1] == Token::Symbol('(')
                        && p.tokens[n + 2] == Token::Symbol('*')
                        && p.tokens[n + 3] == Token::Symbol(')')
                    {
                        p.tokens[n + 2] = Token::Number("1".into());
                    }
                }
                p.at = item_start;
                item = Item::Scalar(Scalar::parse_aggregate_argument(p, 0)?);
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
        // Keep subqueries outside the same bounded grammar as SELECT.
        if p.tokens[p.at..].windows(2).any(
            |w| matches!((&w[0], &w[1]), (Token::Symbol('('), Token::Word(s)) if s == "select"),
        ) {
            return Err(SqlError::Unsupported);
        }
        let filter = if p.word("where") {
            Some(Filter::parse(p, 0, 0)?)
        } else {
            None
        };
        let mut predicates = vec![];
        if let Some(filter) = &filter {
            filter.bounds(&mut predicates);
        }
        Ok(Self {
            explain: false,
            sources: vec![Source {
                name: table.into(),
                alias: table.into(),
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
        if let Some(filter) = self.where_filter() {
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
                alias: table.into(),
            }],
            joins: vec![],
            items,
            predicates: vec![],
            filter: None,
            order: vec![],
            limit: None,
        })
    }
    pub(super) fn parse(p: &mut Parser) -> R<Self> {
        let explain = p.word("explain");
        p.need("select")?;
        preprocess_aggregates(p)?;
        // Subqueries remain outside this bounded relational grammar.
        if p.tokens[p.at..].windows(2).any(
            |w| matches!((&w[0], &w[1]), (Token::Symbol('('), Token::Word(s)) if s == "select"),
        ) {
            return Err(SqlError::Unsupported);
        }
        let items = Self::parse_items(p)?;
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
            let mut equalities = vec![];
            Join::parse_equalities(p, &mut equalities, 0)?;
            joins.push(Join { left, equalities });
        }
        let filter = if p.word("where") {
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
        for n in p.at..p.tokens.len().saturating_sub(3) {
            if matches!(&p.tokens[n], Token::Word(w) if w == "count")
                && p.tokens[n + 1] == Token::Symbol('(')
                && p.tokens[n + 2] == Token::Symbol('*')
                && p.tokens[n + 3] == Token::Symbol(')')
            {
                p.tokens[n + 2] = Token::Number("1".into());
            }
        }
        let having = if p.word("having") {
            Some(Filter::parse(p, 0, 0)?)
        } else {
            None
        };
        let mut order = vec![];
        if p.word("order") {
            p.need("by")?;
            loop {
                let c = Scalar::parse(p, 0)?;
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
                Filter::Scalar(s) | Filter::Null(s, _) => scalar(s),
                Filter::Compare(a, _, b) => scalar(a) && scalar(b),
                Filter::Like {
                    value,
                    pattern,
                    escape,
                    ..
                } => scalar(value) && scalar(pattern) && escape.as_ref().is_none_or(scalar),
                Filter::Not(f) => filter(f),
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
                Filter::Scalar(s) | Filter::Null(s, _) => scalar(s),
                Filter::Compare(a, _, b) => scalar(a) && scalar(b),
                Filter::Like { value, pattern, escape, .. } => scalar(value) && scalar(pattern) && escape.as_ref().is_none_or(scalar),
                Filter::Not(f) => filter_ok(f),
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
        !explain
            && sources.is_empty()
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
        self.sources.is_empty() && !self.sequence_dependencies().is_empty()
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
                Filter::Not(f) => filter(f, out),
                Filter::Logic(a, _, b) => {
                    filter(a, out);
                    filter(b, out);
                }
            }
        }
        let mut out = Vec::new();
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
                Filter::Not(f) => filter(f),
                Filter::Logic(a, _, b) => filter(a) || filter(b),
            }
        }
        self.items.iter().any(|(item, _)| match item {
            Item::Scalar(s) | Item::Aggregate(_, Some(s)) => scalar_has_undefined_routine(s),
            _ => false,
        }) || self.where_filter().is_some_and(filter)
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
                Filter::Scalar(s) | Filter::Null(s, _) => operand(s),
                Filter::Compare(a, _, b) => operand(a) && operand(b),
                Filter::Like {
                    value,
                    pattern,
                    escape,
                    ..
                } => operand(value) && operand(pattern) && escape.as_ref().is_none_or(operand),
                Filter::Not(f) => filter(f),
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
        self.bind_seeded_inner(state, parameters, classify_plain_columns)
    }
    pub(super) fn bind_seeded<'a>(
        &'a self,
        state: &'a Relational,
        parameters: Vec<Option<DataType>>,
    ) -> R<BoundQuery<'a>> {
        self.bind_seeded_inner(state, parameters, false)
    }
    pub(super) fn bind_projection_seeded<'a>(
        &'a self,
        state: &'a Relational,
        parameters: Vec<Option<DataType>>,
    ) -> R<BoundQuery<'a>> {
        self.bind_seeded_inner(state, parameters, true)
    }
    fn bind_seeded_inner<'a>(
        &'a self,
        state: &'a Relational,
        mut parameters: Vec<Option<DataType>>,
        classify_plain_columns: bool,
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
        let resolve = |c: &ColumnRef, scope: usize| -> R<BoundColumn> {
            if c.qualifier
                .as_ref()
                .is_some_and(|q| !self.sources.iter().take(scope).any(|s| q == &s.alias))
            {
                return Err(SqlError::UnknownTable);
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
            found.ok_or(SqlError::UnknownColumn)
        };
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
                && self.items.iter().any(|(i, _)| matches!(i, Item::Aggregate(..))));
        let mut joins = vec![];
        for (j, join) in self.joins.iter().enumerate() {
            let right = j + 1;
            let mut keys = BTreeMap::new();
            let mut equalities = vec![];
            let mut unsupported_key = false;
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
                let outer_ty = &tables[outer.source].columns[outer.column].data_type;
                let inner_ty = &tables[inner.source].columns[inner.column].data_type;
                if !functions::comparable(outer_ty, inner_ty) {
                    return Err(functions::comparison_mismatch(outer_ty, inner_ty));
                }
                if tables[right].primary_key.contains(&inner.column) {
                    if functions::join_key_supported(outer_ty, inner_ty) {
                        if keys
                            .insert(inner.column, (outer, outer_ty.clone(), inner_ty.clone()))
                            .is_some()
                        {
                            return Err(SqlError::Duplicate);
                        }
                    } else {
                        unsupported_key = true;
                    }
                }
                equalities.push((outer, inner, outer_ty.clone(), inner_ty.clone()));
            }
            let key = tables[right]
                .primary_key
                .iter()
                .map(|i| keys.get(i).cloned())
                .collect::<Option<Vec<_>>>();
            let scan = key.is_none();
            if scan && !grouped {
                return Err(if unsupported_key {
                    SqlError::Unsupported
                } else {
                    SqlError::Unbounded
                });
            }
            let key = key.unwrap_or_default();
            joins.push(BoundJoin {
                left: join.left,
                scan,
                key,
                equalities,
                clock: None,
            });
        }
        if grouped {
            return grouping::bind(self, &tables, joins, parameters, &resolve);
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
            f.infer(&mut parameters, &resolve, &tables)?;
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
            if let Expr::Literal(value) = e
                && !matches!(value, Value::Null | Value::Text(_))
            {
                let literal = Scalar::Literal(value.clone()).bind(&resolve, &tables, &parameters)?;
                let column_type = &tables[c.source].columns[c.column].data_type;
                if literal.ty != *column_type
                    && !functions::comparable(&literal.ty, column_type)
                {
                    return Err(functions::comparison_mismatch(&literal.ty, column_type));
                }
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
            .map(|f| f.bind(&parameters, &resolve, &tables))
            .transpose()?;
        let order = self
            .order
            .iter()
            .map(|(e, d, n)| Ok((e.bind(&resolve, &tables, &parameters)?, *d, *n)))
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
    // Only bounded grouped execution accepts a non-primary-key scan join.
    scan: bool,
    key: Vec<(BoundColumn, DataType, DataType)>,
    equalities: Vec<(BoundColumn, BoundColumn, DataType, DataType)>,
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
    if *bytes > MAX_RESULT_BYTES {
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
    /// Collect complete mutation rows in primary-key order, retaining their keys.
    pub(super) fn matching_rows(
        &self,
        _state: &Relational,
        source: &dyn scan::RowSource,
        params: &[Value],
        retention: &mut scan::Retention,
        cancel: &mut impl FnMut() -> bool,
    ) -> R<Vec<(Vec<u8>, SqlRow)>> {
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
                && filter.eval(&tuple, params, &mut work)? != Some(true)
            {
                return Ok(None);
            }
            Ok(Some(row))
        });
        let mut rows = Vec::new();
        for row in filtered {
            let row = row?;
            if rows.len() >= scan::MAX_QUERY_ROWS.min(super::MAX_ROWS) {
                return Err(SqlError::Limit);
            }
            retention.charge(searched::retained_row_bytes(&row.key, &row.row)?)?;
            rows.push((row.key.into_owned(), row.row.into_owned()));
        }
        Ok(rows)
    }
    pub(super) fn execute(
        &self,
        state: &Relational,
        source: &dyn scan::RowSource,
        params: &[Value],
        cancel: &mut impl FnMut() -> bool,
    ) -> R<SqlResult> {
        if self.aggregate && cancel() {
            return Err(SqlError::Aborted);
        }
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
        if let Some(grouping) = &self.grouping {
            return grouping.execute(self, state, source, params, cancel);
        }
        let limit = match &self.query.limit {
            Some(e) => match e.value(params)? {
                Value::Int(n) if n >= 0 => Some(n as u64),
                Value::Null => None,
                _ => return Err(SqlError::Limit),
            },
            None => None,
        };
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
                    && f.eval(&row, params, &mut work)? != Some(true)
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
                plan.push_str(if limit.is_some() {
                    "; TopNSort"
                } else {
                    "; StableSort"
                });
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
        let mut meter = if self.aggregate {
            scan::Meter::per_row(cancel)
        } else {
            scan::Meter::new(cancel)
        };
        let filtered = scan::filtered(input, &mut meter, |r| {
            let mut filter_work = 0;
            let mut row = Tuple {
                values: vec![r.row.values.clone()],
                version: r.row.version,
            };
            for (i, j) in self.joins.iter().enumerate() {
                let table = self.tables[i + 1];
                let found = match j.key(&row)? {
                    Some(key) => source.get(table, &encode_key(&key)?)?,
                    None => None,
                };
                let matched = match found {
                    Some(r) if j.matches(&row, &r.row.values)? => Some(r),
                    _ => None,
                };
                match matched {
                    Some(r) => row.values.push(r.row.values.clone()),
                    None if j.left => row.values.push(vec![Value::Null; table.columns.len()]),
                    None => return Ok(None),
                }
                row.version = 0;
            }
            if let Some(f) = &self.filter
                && f.eval(&row, params, &mut filter_work)? != Some(true)
            {
                return Ok(None);
            }
            Ok(Some(row))
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
            let mut retention = scan::Retention::new(scan::MAX_RETAINED_BYTES);
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
                let mut retention = scan::Retention::new(scan::MAX_RETAINED_BYTES);
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
                if result.rows.len() >= scan::MAX_QUERY_ROWS {
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

pub(super) fn coerce(v: Value, t: &DataType) -> R<Value> {
    functions::cast_value(v, t, &mut 0)
}

#[cfg(test)]
mod aggregate_tests;

impl BoundFilter {
    fn attach_clock(&mut self, clock: Option<&super::clock::ExecutionClock>) {
        match self {
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
            Self::Not(a) => a.attach_clock(clock),
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
        if let Some(grouping) = &mut self.grouping {
            grouping.attach_clock(clock, &mut self.joins);
        } else {
            for join in &mut self.joins {
                join.clock = clock.cloned();
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
        self
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
    for (index, item) in items.iter().enumerate() {
        let BoundItem::Aggregate(a, c) = item else {
            return Err(SqlError::Unsupported);
        };
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
        let mut retention = scan::Retention::new(scan::MAX_RETAINED_BYTES);
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
            Err(e) => eprintln!("F9 skipped {sql}: prepare error code={e:?}; ORDER BY literal not admitted"),
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
                Err(e) => eprintln!("F9 skipped {sql}: prepare error code={e:?}; no Prepared classification exists"),
            }
        }
    }
}

mod constraints;
pub(super) use constraints::{
    constraint_check, constraint_deparse, constraint_key, constraint_key_supported,
    constraint_types_compatible, constraint_validate,
};
