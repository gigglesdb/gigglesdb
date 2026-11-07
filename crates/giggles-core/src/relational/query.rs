//! Private syntax, binding and bounded physical execution. No persisted types.
use super::*;
use std::cmp::Ordering;
mod functions;
mod insert_values;
use functions::{BoundScalar, Scalar};
pub(super) use insert_values::InsertExpressions;

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
    Min,
    Max,
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
enum Filter {
    Scalar(Scalar),
    Compare(Scalar, String, Scalar),
    Null(Scalar, bool),
    Not(Box<Filter>),
    Logic(Box<Filter>, bool, Box<Filter>),
}
enum BoundFilter {
    Scalar(BoundScalar),
    Compare(BoundScalar, String, BoundScalar),
    Null(BoundScalar, bool),
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
                        && matches!(p.tokens.get(p.at + 1), Some(Token::Word(w)) if w == "in" || w == "between");
                    if not {
                        p.at += 1;
                    }
                    if p.word("is") {
                        let not = p.word("not");
                        p.need("null")?;
                        Self::Null(a, not)
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
                            Box::new(Self::Compare(
                                a,
                                if not { ">" } else { "<=" }.into(),
                                upper,
                            )),
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
            Self::Not(a) => a.infer(params, resolve, tables),
            Self::Logic(a, _, b) => {
                a.infer(params, resolve, tables)?;
                b.infer(params, resolve, tables)
            }
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
                        return Err(SqlError::Type);
                    }
                }
                BoundFilter::Compare(av, op.clone(), bv)
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

impl Query {
    fn parse_items(p: &mut Parser) -> R<Vec<(Item, Option<String>)>> {
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
                        Some(Scalar::parse_aggregate_argument(p, 0)?)
                    };
                    p.punct(')')?;
                    Item::Aggregate(a, c)
                } else {
                    Item::Scalar(Scalar::parse(p, 0)?)
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
        Ok(items)
    }
    pub(super) fn returning(p: &mut Parser, table: &str) -> R<Self> {
        let items = Self::parse_items(p)?;
        for (item, _) in &items {
            match item {
                Item::Aggregate(..) => return Err(SqlError::GroupingError),
                Item::Scalar(value) => value.reject_nested_aggregate()?,
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
        let filter = if p.word("where") {
            Some(Filter::parse(p, 0, 0)?)
        } else {
            None
        };
        let mut predicates = vec![];
        if let Some(f) = &filter {
            f.bounds(&mut predicates);
        }
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
            filter,
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
                }
            }
        }
        fn filter(f: &Filter) -> bool {
            match f {
                Filter::Scalar(s) | Filter::Null(s, _) => scalar(s),
                Filter::Compare(a, _, b) => scalar(a) && scalar(b),
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
                Item::Aggregate(_, _) | Item::Star => false,
            })
            && predicate.as_ref().is_none_or(filter)
            && order.iter().all(|(s, _, _)| scalar(s))
            && limit.as_ref().is_none_or(|e| match e {
                Expr::Literal(_) | Expr::Param(_) => true,
            })
    }
    fn has_undefined_routine(&self) -> bool {
        fn filter(f: &Filter) -> bool {
            match f {
                Filter::Scalar(s) | Filter::Null(s, _) => s.has_undefined_routine(),
                Filter::Compare(a, _, b) => a.has_undefined_routine() || b.has_undefined_routine(),
                Filter::Not(f) => filter(f),
                Filter::Logic(a, _, b) => filter(a) || filter(b),
            }
        }
        self.items.iter().any(|(item, _)| match item {
            Item::Scalar(s) | Item::Aggregate(_, Some(s)) => s.has_undefined_routine(),
            _ => false,
        }) || self.filter.as_ref().is_some_and(filter)
            || self.order.iter().any(|(s, _, _)| s.has_undefined_routine())
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
            s.reject_nested_aggregate().is_ok() && scalar(s)
        }
        fn filter(f: &Filter) -> bool {
            match f {
                Filter::Scalar(s) | Filter::Null(s, _) => operand(s),
                Filter::Compare(a, _, b) => operand(a) && operand(b),
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
        if count > MAX_PARAMS {
            return Err(SqlError::Limit);
        }
        if declared.len() > count {
            return Err(SqlError::Parameters);
        }
        let mut parameters = vec![None; count];
        parameters[..declared.len()].clone_from_slice(declared);
        self.bind_seeded(state, parameters)
    }
    pub(super) fn bind_seeded<'a>(
        &'a self,
        state: &'a Relational,
        mut parameters: Vec<Option<DataType>>,
    ) -> R<BoundQuery<'a>> {
        if parameters.len() > MAX_PARAMS {
            return Err(SqlError::Limit);
        }
        unique(self.sources.iter().map(|s| &s.alias))?;
        let tables = self
            .sources
            .iter()
            .map(|s| state.tables.get(&s.name).ok_or(SqlError::UnknownTable))
            .collect::<R<Vec<_>>>()?;
        let resolve = |c: &ColumnRef, scope: usize| -> R<BoundColumn> {
            if c.qualifier.as_ref().is_some_and(|q| {
                !self.sources.iter().take(scope).any(|s| q == &s.alias)
            }) {
                return Err(SqlError::UnknownTable);
            }
            let mut found = None;
            for (source, (s, t)) in self.sources.iter().zip(&tables).take(scope).enumerate() {
                if c.qualifier
                    .as_ref()
                    .is_some_and(|q| q != &s.alias)
                {
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
        let mut unresolved_count_parameters = Vec::new();
        for (item, _) in &self.items {
            match item {
                Item::Scalar(e) => e.infer(None, &mut parameters, &resolve, &tables)?,
                Item::Aggregate(a, Some(e)) => {
                    e.reject_nested_aggregate()?;
                    if matches!(a, Aggregate::Count)
                        && let Scalar::Param(n) = e
                        && parameters.get(n - 1).is_none_or(Option::is_none)
                    {
                        unresolved_count_parameters.push(*n);
                    }
                    if matches!(a, Aggregate::Sum) && e.aggregate_unknown(&parameters) {
                        return Err(SqlError::AmbiguousFunction);
                    }
                    let expected = if matches!(a, Aggregate::Min | Aggregate::Max)
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
        if let Some(f) = &self.filter {
            f.infer(&mut parameters, &resolve, &tables)?;
        }
        for (e, _, _) in &self.order {
            e.infer(None, &mut parameters, &resolve, &tables)?;
        }
        let mut bind_expr = |e: &Expr, ty: &DataType| -> R<bool> {
            if let Expr::Param(n) = e {
                let slot = parameters.get_mut(n - 1).ok_or(SqlError::Parameters)?;
                if let Some(old) = slot.as_ref().filter(|old| *old != ty) {
                    if functions::comparison_type(old, ty).is_some() {
                        // Mixed numeric comparisons run in the full filter. A
                        // converted column key can round and miss matching rows.
                        return Ok(false);
                    }
                    return Err(SqlError::Type);
                }
                slot.get_or_insert_with(|| ty.clone());
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
            Ok(true)
        };
        let mut predicates = vec![];
        for (c, op, e) in &self.predicates {
            let c = resolve(c, tables.len())?;
            if bind_expr(e, &tables[c.source].columns[c.column].data_type)? {
                predicates.push((c, op.as_str(), e));
            }
        }
        if let Some(e) = &self.limit {
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
                bind_expr(e, &DataType::BigInt)?;
            }
        }
        if unresolved_count_parameters
            .iter()
            .any(|n| parameters.get(n - 1).is_some_and(Option::is_some))
        {
            return Err(SqlError::AmbiguousParameter);
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
                    let d = if let Item::Scalar(Scalar::Column(c)) = item {
                        let c = resolve(c, tables.len())?;
                        let mut d = tables[c.source].columns[c.column].clone();
                        if c.source > 0 && self.joins[c.source - 1].left {
                            d.not_null = false;
                        }
                        d
                    } else {
                        ColumnDef {
                            id: 0,
                            name: e.name().into(),
                            data_type: e.ty.clone(),
                            not_null: false,
                        }
                    };
                    push(BoundItem::Scalar(e), d);
                }
                Item::Aggregate(a, c) => {
                    if matches!(a, Aggregate::Count)
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
                    if matches!(a, Aggregate::Sum)
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
                        return Err(if legacy_column {
                            SqlError::Type
                        } else {
                            SqlError::UndefinedFunction
                        });
                    }
                    if !legacy_column
                        && matches!(a, Aggregate::Min | Aggregate::Max)
                        && matches!(ty, Some(DataType::Boolean | DataType::Bytes))
                    {
                        return Err(SqlError::UndefinedFunction);
                    }
                    let data_type = if matches!(a, Aggregate::Sum) {
                        match ty {
                            Some(DataType::BigInt | DataType::Numeric) => DataType::Numeric,
                            Some(DataType::Real) => DataType::Real,
                            Some(DataType::DoublePrecision) => DataType::DoublePrecision,
                            _ => DataType::BigInt,
                        }
                    } else if matches!(a, Aggregate::Count) {
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
            .filter
            .as_ref()
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
    filter: Option<BoundFilter>,
    order: Vec<(BoundScalar, bool, bool)>,
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
    pub(super) fn execute(
        &self,
        state: &Relational,
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
            if limit != Some(0) {
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
            // form one key range. SELECT then streams the full range; the complete
            // WHERE filter is always re-applied, so the result is unchanged.
            Err(SqlError::Unbounded | SqlError::Unsupported) => {
                bounds(self.tables[0], &[], params)?
            }
            other => other?,
        };
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
                plan.push_str(if limit.is_some() { "; TopNSort" } else { "; StableSort" });
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
        use scan::RowSource;
        let source = scan::LegacyRows(state);
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
        let mut filter_work = 0;
        let filtered = scan::filtered(input, &mut meter, |r| {
            let mut row = Tuple {
                values: vec![r.row.values.clone()],
                version: r.row.version,
            };
            for (i, j) in self.joins.iter().enumerate() {
                let table = self.tables[i + 1];
                let key = j.key.iter().map(|c| row.get(*c).clone()).collect::<Vec<_>>();
                let found = if key.contains(&Value::Null) {
                    None
                } else {
                    source.get(table, &encode_key(&key)?)?
                };
                let matched = found.filter(|r| {
                    j.equalities.iter().all(|(a, b)| equal(row.get(*a), &r.row.values[b.column]))
                });
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
        let page = scan::Page { offset: 0, limit };
        let mut result = SqlResult {
            columns: self.columns.iter().map(|c| c.name.clone()).collect(),
            rows: vec![],
            affected: 0,
        };
        let mut result_bytes = 0usize;
        let mut scalar_work_bytes = 0usize;
        if self.aggregate {
            let mut values = self.items.iter().map(|item| {
                if matches!(item, BoundItem::Aggregate(Aggregate::Count, _)) {
                    Value::Int(0)
                } else {
                    Value::Null
                }
            }).collect::<Vec<_>>();
            for row in filtered {
                let row = row?;
                for (index, item) in self.items.iter().enumerate() {
                    let BoundItem::Aggregate(a, c) = item else {
                        return Err(SqlError::Unsupported);
                    };
                    let evaluated = c
                        .as_ref()
                        .map(|e| e.eval(&row, params, &mut scalar_work_bytes))
                        .transpose()?;
                    let v = evaluated.as_ref();
                    if v == Some(&Value::Null) {
                        continue;
                    }
                    let mut value = std::mem::replace(&mut values[index], Value::Null);
                    match a {
                        Aggregate::Count => {
                            if let Value::Int(n) = &mut value {
                                *n = n.checked_add(1).ok_or(SqlError::Overflow)?;
                            }
                        }
                        Aggregate::Sum => {
                            let v = v.ok_or(SqlError::Type)?;
                            value = match self.columns[index].data_type {
                                DataType::Numeric => functions::aggregate_numeric_add(
                                    value,
                                    evaluated.ok_or(SqlError::Type)?,
                                    &mut scalar_work_bytes,
                                )?,
                                DataType::Real => {
                                    let Value::Float4(n) = v else {
                                        return Err(SqlError::Type);
                                    };
                                    let sum = if let Value::Float4(old) = value {
                                        let sum = old.value() + n.value();
                                        if sum.is_infinite()
                                            && old.value().is_finite()
                                            && n.value().is_finite()
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
                                        if sum.is_infinite()
                                            && old.value().is_finite()
                                            && n.value().is_finite()
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
                                || (matches!(a, Aggregate::Min)
                                    && cmp(v, &value) == Ordering::Less)
                                || (matches!(a, Aggregate::Max)
                                    && cmp(v, &value) == Ordering::Greater)
                            {
                                value = evaluated.ok_or(SqlError::Type)?;
                            }
                        }
                    }
                    values[index] = value;
                }
            }
            for value in &values {
                charge(&mut result_bytes, value)?;
            }
            result.rows.extend(page.apply(std::iter::once(SqlRow { values, version: 0 })));
        } else {
            let rows: Box<dyn Iterator<Item = R<Tuple>> + '_> = if self.order.is_empty() {
                Box::new(filtered)
            } else {
                let mut key_work = 0;
                let keyed = filtered.map(|row| {
                    let row = row?;
                    let keys = self.order.iter()
                        .map(|(e, _, _)| e.eval(&row, params, &mut key_work))
                        .collect::<R<Vec<_>>>()?;
                    Ok((row, keys))
                });
                let mut retention = scan::Retention::new(scan::MAX_RETAINED_BYTES);
                let sorted = scan::sort_bounded(
                    keyed,
                    page.keep(),
                    |(row, keys)| row.values.iter().flatten().chain(keys).map(value_bytes).sum(),
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
