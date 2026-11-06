use crate::{catalog, *};
use a::{BinaryOperator as Op, Expr as A};
use sqlparser::ast::{Visit, Visitor};
use sqlparser::{
    ast as a,
    dialect::PostgreSqlDialect,
    keywords::Keyword,
    parser::Parser,
    tokenizer::{Token, Tokenizer},
};
use std::{collections::BTreeMap, ops::ControlFlow};

#[derive(Debug, Clone)]
pub struct Plan {
    query: Query,
    parameters: usize,
    parameter_types: Vec<PgType>,
    projection_parameters: bool,
}
impl Plan {
    /// Conservative wire widening is safe only when no SELECT projection
    /// (including a derived query) contains a parameter expression.
    pub fn allows_numeric_input_override(&self) -> bool {
        !self.projection_parameters
    }
    pub fn parameter_count(&self) -> usize {
        self.parameters
    }
    /// Inferred PostgreSQL input types for wire ParameterDescription, in $1 order.
    /// Unconstrained inputs default to text. Explicit wire input types are caller policy.
    pub fn parameter_types(&self) -> &[PgType] {
        &self.parameter_types
    }
}
#[derive(Debug, Clone)]
struct Col {
    qualifier: String,
    field: Field,
}
type Schema = Vec<Col>;
#[derive(Debug, Clone, PartialEq)]
struct E {
    kind: K,
    ty: PgType,
}
#[derive(Debug, Clone, PartialEq)]
enum K {
    Literal(Value),
    Column(usize),
    Param(usize),
    Binary(Box<E>, Op, Box<E>),
    Not(Box<E>),
    Neg(Box<E>),
    Is(Box<E>, Value, bool),
    Cast(Box<E>, PgType),
    Regclass(Box<E>),
    Regproc(Box<E>),
    Regoper(Box<E>),
    Case(Option<Box<E>>, Vec<(E, E)>, Box<E>),
    Call(String, Vec<E>),
    Array(Vec<E>),
    Index(Box<E>, Box<E>),
    Member(Box<E>, usize),
    Any(Box<E>, Op, Box<E>),
    In(Box<E>, Vec<E>, bool),
    Subquery(Box<Query>),
    ArrayQuery(Box<Query>),
    ArrayAgg(Box<E>, Vec<Order>),
    RowNumber(Vec<E>, Vec<Order>),
    Expand(Box<E>),
}
#[derive(Debug, Clone, PartialEq)]
struct Order {
    e: E,
    output: Option<usize>,
    asc: bool,
    nulls_first: bool,
}
#[derive(Debug, Clone, PartialEq)]
enum Source {
    Unit,
    Empty,
    Catalog(String),
    Derived(std::sync::Arc<Query>),
    Union(Box<Query>, Box<Query>, bool),
    Series(E, E),
    Unnest(E, bool),
    Scalar(E),
    Join(Box<Source>, Box<Source>, E, bool, usize, bool),
}
#[derive(Debug, Clone, PartialEq)]
struct Query {
    source: Source,
    filter: E,
    projection: Vec<E>,
    fields: Vec<Field>,
    order: Vec<Order>,
    limit: usize,
    distinct: bool,
    expand: Option<E>,
    source_width: usize,
    group: Vec<E>,
    aggregate: bool,
}
struct Compiler {
    parameters: usize,
    qcount: usize,
    sources: usize,
    ctes: BTreeMap<String, std::sync::Arc<Query>>,
}
fn ident(i: &a::Ident) -> String {
    if i.quote_style.is_some() {
        i.value.clone()
    } else {
        i.value.to_ascii_lowercase()
    }
}
fn object(n: &a::ObjectName) -> Vec<String> {
    n.0.iter().map(ident).collect()
}
fn function(n: &a::ObjectName) -> R<String> {
    let p = object(n);
    match p.as_slice() {
        [n] if n != "_pg_expandarray" => Ok(n.clone()),
        [s, n]
            if s == "pg_catalog" && n != "_pg_expandarray"
                || s == "information_schema" && n == "_pg_expandarray" =>
        {
            Ok(n.clone())
        }
        _ => Err(Error::Unsupported),
    }
}
fn literal(v: Value, ty: PgType) -> E {
    E {
        kind: K::Literal(v),
        ty,
    }
}
fn truth() -> E {
    literal(Value::Bool(true), PgType::Bool)
}
fn null() -> E {
    literal(Value::Null, PgType::Text)
}
fn column(i: usize, ty: PgType) -> E {
    E {
        kind: K::Column(i),
        ty,
    }
}
fn binary(l: E, op: Op, r: E) -> E {
    let ty = if matches!(
        op,
        Op::Plus | Op::Minus | Op::Multiply | Op::Divide | Op::Modulo
    ) {
        PgType::Int8
    } else if op == Op::StringConcat {
        PgType::Text
    } else {
        PgType::Bool
    };
    E {
        kind: K::Binary(Box::new(l), op, Box::new(r)),
        ty,
    }
}

/// Parse/classify structurally. No snapshot or authority is cached in a plan.
pub fn plan(sql: &str) -> R<Option<Plan>> {
    if sql.len() > MAX_SQL {
        return Err(Error::Limit);
    }
    // sqlparser's recursion guard does not bound left-associative ASTs:
    // rejecting a deep tree after parsing can still overflow during Drop.
    // Tokenize first (flat storage), then bound tokens that can construct AST
    // layers before allowing the parser to allocate any recursive structure.
    // Strings/comments/quoted identifiers are tokens, never scanned as SQL.
    let tokens = Tokenizer::new(&PostgreSqlDialect {}, sql)
        .tokenize()
        .map_err(|_| Error::Syntax)?;
    let mut count = 0usize;
    let mut constructors = 0usize;
    for token in &tokens {
        if matches!(token, Token::Whitespace(_) | Token::EOF) {
            continue;
        }
        count += 1;
        let leaf = matches!(token,Token::Word(w) if w.keyword==Keyword::NoKeyword)
            || matches!(
                token,
                Token::Number(..)
                    | Token::SingleQuotedString(_)
                    | Token::DoubleQuotedString(_)
                    | Token::DollarQuotedString(_)
                    | Token::EscapedStringLiteral(_)
                    | Token::NationalStringLiteral(_)
                    | Token::HexStringLiteral(_)
                    | Token::Placeholder(_)
                    | Token::Comma
                    | Token::RParen
                    | Token::RBracket
            );
        if !leaf {
            constructors += 1;
        }
        if count > 8192 || constructors > 256 {
            return Err(Error::Limit);
        }
    }
    let statements = Parser::new(&PostgreSqlDialect {})
        .with_recursion_limit(64)
        .with_tokens(tokens)
        .parse_statements()
        .map_err(|_| Error::Syntax)?;
    if statements.is_empty() {
        return Ok(None);
    }
    struct Scan {
        catalog: bool,
        user: bool,
        nodes: usize,
        depth: usize,
        mutation: bool,
        ctes: std::collections::BTreeSet<String>,
    }
    impl Visitor for Scan {
        type Break = Error;
        fn pre_visit_query(&mut self, q: &a::Query) -> ControlFlow<Error> {
            if let Some(with) = &q.with {
                if with.recursive {
                    return ControlFlow::Break(Error::Unsupported);
                }
                for cte in &with.cte_tables {
                    self.ctes.insert(ident(&cte.alias.name));
                }
            }
            ControlFlow::Continue(())
        }
        fn pre_visit_relation(&mut self, n: &a::ObjectName) -> ControlFlow<Error> {
            let p = object(n);
            let is_catalog = match p.as_slice() {
                [n] => catalog::schema(n).is_some(),
                [s, _] => s == "pg_catalog" || s == "information_schema",
                _ => false,
            };
            if is_catalog {
                self.catalog = true
            } else if !matches!(p.as_slice(),[n] if n=="generate_series" || n=="unnest" || n=="pg_available_extension_versions" || self.ctes.contains(n))
            {
                self.user = true
            }
            ControlFlow::Continue(())
        }
        fn pre_visit_expr(&mut self, e: &A) -> ControlFlow<Error> {
            self.nodes += 1;
            self.depth += 1;
            if self.nodes > 4096 || self.depth > 48 {
                return ControlFlow::Break(Error::Limit);
            }
            if let A::Function(f) = e
                && let Ok(n) = function(&f.name)
                && matches!(
                    n.as_str(),
                    "current_database"
                        | "current_schema"
                        | "current_user"
                        | "session_user"
                        | "current_schemas"
                )
            {
                self.catalog = true
            }
            ControlFlow::Continue(())
        }
        fn post_visit_expr(&mut self, _: &A) -> ControlFlow<Error> {
            self.depth -= 1;
            ControlFlow::Continue(())
        }
        fn pre_visit_statement(&mut self, s: &a::Statement) -> ControlFlow<Error> {
            // Clients validate connections with table-free SELECTs. Evaluate
            // their expressions normally rather than matching a SQL prefix or
            // manufacturing a successful response. Relation scanning below
            // still rejects hidden application relations in subqueries.
            if let a::Statement::Query(q) = s
                && let a::SetExpr::Select(select) = q.body.as_ref()
                && select.from.is_empty()
            {
                self.catalog = true;
            }
            if !matches!(s, a::Statement::Query(_)) {
                self.mutation = true
            }
            ControlFlow::Continue(())
        }
    }
    let mut scan = Scan {
        catalog: false,
        user: false,
        nodes: 0,
        depth: 0,
        mutation: false,
        ctes: Default::default(),
    };
    if let ControlFlow::Break(e) = statements.visit(&mut scan) {
        return Err(e);
    }
    if !scan.catalog {
        return Ok(None);
    }
    if scan.mutation || statements.len() != 1 {
        return Err(Error::Mutation);
    }
    if scan.user {
        return Err(Error::MixedRelations);
    }
    let a::Statement::Query(q) = &statements[0] else {
        return Err(Error::Mutation);
    };
    struct ProjectionParameters(bool);
    impl Visitor for ProjectionParameters {
        type Break = ();
        fn pre_visit_query(&mut self, q: &a::Query) -> ControlFlow<()> {
            fn body(body: &a::SetExpr) -> bool {
                if let a::SetExpr::Select(s) = body {
                    struct Parameter(bool);
                    impl Visitor for Parameter {
                        type Break = ();
                        fn pre_visit_expr(&mut self, e: &A) -> ControlFlow<()> {
                            if matches!(e, A::Value(a::Value::Placeholder(_))) {
                                self.0 = true;
                            }
                            ControlFlow::Continue(())
                        }
                    }
                    let mut p = Parameter(false);
                    let _ = s.projection.visit(&mut p);
                    return p.0;
                }
                if let a::SetExpr::SetOperation { left, right, .. } = body {
                    return body_projection(left) || body_projection(right);
                }
                false
            }
            fn body_projection(expr: &a::SetExpr) -> bool {
                body(expr)
            }
            self.0 |= body(&q.body);
            ControlFlow::Continue(())
        }
    }
    let mut projection_parameters = ProjectionParameters(false);
    let _ = q.visit(&mut projection_parameters);
    let mut c = Compiler {
        parameters: 0,
        qcount: 0,
        sources: 0,
        ctes: BTreeMap::new(),
    };
    let query = c.query(q, &vec![])?;
    if query.fields.iter().any(|f| f.data_type == PgType::Record) {
        return Err(Error::Unsupported);
    }
    let mut types = vec![None; c.parameters];
    collect_parameters(&query, &mut types)?;
    Ok(Some(Plan {
        projection_parameters: projection_parameters.0,
        query,
        parameters: c.parameters,
        parameter_types: types
            .into_iter()
            .map(|t| t.unwrap_or(PgType::Text))
            .collect(),
    }))
}
pub fn describe(plan: &Plan) -> R<Vec<Field>> {
    Ok(plan.query.fields.clone())
}
pub fn execute(plan: &Plan, snapshot: &CatalogSnapshot, parameters: &[Value]) -> R<ResultSet> {
    execute_with_startup(plan, snapshot, parameters, None)
}
/// Execute with the hosting service's actual initialization epoch (seconds).
/// Without a supplied timestamp, startup introspection fails explicitly.
pub fn execute_with_startup(
    plan: &Plan,
    snapshot: &CatalogSnapshot,
    parameters: &[Value],
    startup: Option<i64>,
) -> R<ResultSet> {
    if parameters.len() != plan.parameters {
        return Err(Error::Parameters);
    }
    validate_values(parameters)?;
    // Bind errors cannot disappear when an authorized scan happens to be empty.
    let parameters = parameters
        .iter()
        .zip(&plan.parameter_types)
        .map(|(v, t)| cast(v.clone(), *t))
        .collect::<R<Vec<_>>>()?;
    validate_values(&parameters)?;
    let catalogs = catalog::build(snapshot)?;
    let mut rt = Runtime {
        catalogs: &catalogs,
        startup,
        parameters: &parameters,
        work: 0,
    };
    let rows = rt.query(&plan.query, &[])?;
    Ok(ResultSet {
        fields: plan.query.fields.clone(),
        rows,
    })
}
impl Compiler {
    fn query(&mut self, q: &a::Query, outer: &Schema) -> R<Query> {
        let previous = self.ctes.clone();
        let result = self.query_scoped(q, outer);
        self.ctes = previous;
        result
    }
    fn query_scoped(&mut self, q: &a::Query, outer: &Schema) -> R<Query> {
        self.qcount += 1;
        if self.qcount > 32 {
            return Err(Error::Limit);
        }
        if q.fetch.is_some()
            || !q.locks.is_empty()
            || q.offset.is_some()
            || !q.limit_by.is_empty()
            || q.for_clause.is_some()
            || q.settings.is_some()
            || q.format_clause.is_some()
        {
            return Err(Error::Unsupported);
        }
        if let Some(with) = &q.with {
            if with.recursive {
                return Err(Error::Unsupported);
            }
            for cte in &with.cte_tables {
                if !cte.alias.columns.is_empty() || cte.from.is_some() || cte.materialized.is_some()
                {
                    return Err(Error::Unsupported);
                }
                let name = ident(&cte.alias.name);
                if self.ctes.contains_key(&name) || catalog::schema(&name).is_some() {
                    return Err(Error::Unsupported);
                }
                let compiled = self.query(&cte.query, &vec![])?;
                self.ctes.insert(name, std::sync::Arc::new(compiled));
            }
        }
        if let a::SetExpr::SetOperation {
            op: a::SetOperator::Union,
            set_quantifier,
            left,
            right,
        } = q.body.as_ref()
        {
            let distinct = match set_quantifier {
                a::SetQuantifier::All => false,
                a::SetQuantifier::Distinct | a::SetQuantifier::None => true,
                _ => return Err(Error::Unsupported),
            };
            let mut branch = q.clone();
            branch.order_by = None;
            branch.with = None;
            branch.limit = None;
            branch.body = left.clone();
            let l = self.query(&branch, outer)?;
            branch.body = right.clone();
            let r = self.query(&branch, outer)?;
            if l.fields.len() != r.fields.len()
                || l.fields
                    .iter()
                    .zip(&r.fields)
                    .any(|(l, r)| l.data_type != r.data_type)
            {
                return Err(Error::Type);
            }
            let fields = l.fields.clone();
            let projection = fields
                .iter()
                .enumerate()
                .map(|(i, f)| column(i, f.data_type))
                .collect();
            let mut order = vec![];
            if let Some(o) = &q.order_by {
                if o.interpolate.is_some() {
                    return Err(Error::Unsupported);
                }
                for x in &o.exprs {
                    if x.with_fill.is_some() {
                        return Err(Error::Unsupported);
                    }
                    let i = match &x.expr {
                        A::Identifier(n) => {
                            let matches: Vec<_> = fields
                                .iter()
                                .enumerate()
                                .filter(|(_, f)| f.name == ident(n))
                                .map(|(i, _)| i)
                                .collect();
                            if matches.len() != 1 {
                                return Err(Error::UnknownColumn);
                            }
                            matches[0]
                        }
                        A::Value(a::Value::Number(n, _)) => n
                            .parse::<usize>()
                            .ok()
                            .and_then(|i| i.checked_sub(1))
                            .ok_or(Error::UnknownColumn)?,
                        _ => return Err(Error::Unsupported),
                    };
                    let f = fields.get(i).ok_or(Error::UnknownColumn)?;
                    let asc = x.asc.unwrap_or(true);
                    order.push(Order {
                        e: null(),
                        output: Some(i),
                        asc,
                        nulls_first: x.nulls_first.unwrap_or(!asc),
                    });
                    let _ = f;
                }
            }
            let limit = match &q.limit {
                None => MAX_ROWS,
                Some(A::Value(a::Value::Number(n, _))) => {
                    n.parse::<usize>().map_err(|_| Error::Limit)?
                }
                _ => return Err(Error::Unsupported),
            };
            if limit > MAX_ROWS {
                return Err(Error::Limit);
            }
            return Ok(Query {
                source: Source::Union(Box::new(l), Box::new(r), distinct),
                filter: truth(),
                projection,
                source_width: fields.len(),
                fields,
                order,
                limit,
                distinct: false,
                expand: None,
                group: vec![],
                aggregate: false,
            });
        }
        let a::SetExpr::Select(s) = q.body.as_ref() else {
            return Err(Error::Unsupported);
        };
        if s.into.is_some() {
            return Err(Error::Mutation);
        }
        if s.top.is_some()
            || !s.lateral_views.is_empty()
            || s.prewhere.is_some()
            || !matches!(&s.group_by,a::GroupByExpr::Expressions(_,m) if m.is_empty())
            || s.having.is_some()
            || s.qualify.is_some()
            || !s.named_window.is_empty()
            || !s.cluster_by.is_empty()
            || !s.distribute_by.is_empty()
            || !s.sort_by.is_empty()
            || s.value_table_mode.is_some()
            || s.connect_by.is_some()
        {
            return Err(Error::Unsupported);
        }
        if s.distinct.is_some() && !matches!(s.distinct, Some(a::Distinct::Distinct)) {
            return Err(Error::Unsupported);
        }
        let mut source = Source::Unit;
        let mut schema = vec![];
        for from in &s.from {
            let (rhs, rs) = self.table(&from.relation, outer)?;
            if schema.is_empty() {
                source = rhs;
                schema = rs;
            } else {
                let width = rs.len();
                source = Source::Join(
                    Box::new(source),
                    Box::new(rhs),
                    truth(),
                    false,
                    width,
                    false,
                );
                schema.extend(rs)
            }
            for j in &from.joins {
                let mut lateral_schema = schema.clone();
                lateral_schema.extend(outer.clone());
                let (rhs, rs) = self.table(&j.relation, &lateral_schema)?;
                let lateral = matches!(rhs, Source::Unnest(..) | Source::Scalar(_));
                let old_width = schema.len();
                let width = rs.len();
                schema.extend(rs);
                let (constraint, left) = match &j.join_operator {
                    a::JoinOperator::Inner(c) => (c, false),
                    a::JoinOperator::LeftOuter(c) => (c, true),
                    a::JoinOperator::CrossJoin => (&a::JoinConstraint::None, false),
                    _ => return Err(Error::Unsupported),
                };
                let mut on = match constraint {
                    a::JoinConstraint::On(e) => self.expr(e, &schema)?,
                    a::JoinConstraint::None => truth(),
                    a::JoinConstraint::Using(cols) => {
                        let mut on = truth();
                        for n in cols {
                            let name = ident(n);
                            let l = lookup(&schema[..old_width], std::slice::from_ref(&name))?;
                            let r = lookup(&schema[old_width..], &[name])? + old_width;
                            on = binary(
                                on,
                                Op::And,
                                binary(
                                    column(l, schema[l].field.data_type),
                                    Op::Eq,
                                    column(r, schema[r].field.data_type),
                                ),
                            )
                        }
                        on
                    }
                    _ => return Err(Error::Unsupported),
                };
                expect(&mut on, PgType::Bool)?;
                no_set_or_window(&on)?;
                source = Source::Join(Box::new(source), Box::new(rhs), on, left, width, lateral);
            }
        }
        if schema.len() > 256 {
            return Err(Error::Limit);
        }
        let source_width = schema.len();
        schema.extend(outer.clone());
        let mut filter = s
            .selection
            .as_ref()
            .map(|e| self.expr(e, &schema))
            .transpose()?
            .unwrap_or_else(truth);
        expect(&mut filter, PgType::Bool)?;
        no_set_or_window(&filter)?;
        let a::GroupByExpr::Expressions(group, _) = &s.group_by else {
            return Err(Error::Unsupported);
        };
        let group = group
            .iter()
            .map(|e| self.expr(e, &schema))
            .collect::<R<Vec<_>>>()?;
        for e in &group {
            no_set_or_window(e)?;
            if has_aggregate(e) {
                return Err(Error::Unsupported);
            }
        }
        let mut projection = vec![];
        let mut fields = vec![];
        let mut expand = None;
        for item in &s.projection {
            match item {
                a::SelectItem::UnnamedExpr(e) | a::SelectItem::ExprWithAlias { expr: e, .. } => {
                    let e2 = self.expr(e, &schema)?;
                    if has_expand(&e2) && !matches!(&e2.kind, K::Expand(_) | K::Member(_, _)) {
                        return Err(Error::Unsupported);
                    }
                    find_expand(&e2, &mut expand)?;
                    let name = match item {
                        a::SelectItem::ExprWithAlias { alias, .. } => ident(alias),
                        _ => match e {
                            A::Identifier(n) => ident(n),
                            A::CompoundIdentifier(p) => ident(p.last().unwrap()),
                            A::Function(f) => function(&f.name)?,
                            _ => "?column?".into(),
                        },
                    };
                    fields.push(Field {
                        name,
                        data_type: e2.ty,
                    });
                    projection.push(e2);
                }
                a::SelectItem::Wildcard(opts)
                    if *opts == a::WildcardAdditionalOptions::default() =>
                {
                    for (i, c) in schema[..source_width].iter().enumerate() {
                        fields.push(c.field.clone());
                        projection.push(column(i, c.field.data_type))
                    }
                }
                _ => return Err(Error::Unsupported),
            }
        }
        if projection.len() > 128 {
            return Err(Error::Limit);
        }
        let aggregate = !group.is_empty() || projection.iter().any(has_aggregate);
        if aggregate {
            if expand.is_some() {
                return Err(Error::Unsupported);
            }
            for e in &projection {
                validate_group(e, &group)?;
            }
        }
        let mut order = vec![];
        if let Some(o) = &q.order_by {
            if o.interpolate.is_some() {
                return Err(Error::Unsupported);
            }
            for x in &o.exprs {
                if x.with_fill.is_some() {
                    return Err(Error::Unsupported);
                }
                let alias = if let A::Identifier(n) = &x.expr {
                    let matches = fields
                        .iter()
                        .enumerate()
                        .filter(|(_, f)| f.name == ident(n))
                        .map(|(i, _)| i)
                        .collect::<Vec<_>>();
                    if matches.len() > 1 {
                        return Err(Error::AmbiguousColumn);
                    }
                    matches.first().copied()
                } else {
                    None
                };
                let pos = if let A::Value(a::Value::Number(n, _)) = &x.expr {
                    n.parse::<usize>().ok().and_then(|n| n.checked_sub(1))
                } else {
                    None
                };
                let output = alias.or(pos);
                let e = if let Some(i) = output {
                    let e = projection.get(i).ok_or(Error::UnknownColumn)?;
                    literal(Value::Null, e.ty)
                } else {
                    self.expr(&x.expr, &schema)?
                };
                let asc = x.asc.unwrap_or(true);
                no_set_or_window(&e)?;
                if aggregate {
                    validate_group(&e, &group)?;
                }
                order.push(Order {
                    e,
                    output,
                    asc,
                    nulls_first: x.nulls_first.unwrap_or(!asc),
                });
            }
        }
        let limit = match &q.limit {
            None => MAX_ROWS,
            Some(A::Value(a::Value::Number(n, _))) => {
                n.parse::<usize>().map_err(|_| Error::Limit)?
            }
            _ => return Err(Error::Unsupported),
        };
        if limit > MAX_ROWS {
            return Err(Error::Limit);
        }
        Ok(Query {
            source,
            filter,
            projection,
            fields,
            order,
            limit,
            distinct: s.distinct.is_some(),
            expand,
            source_width,
            group,
            aggregate,
        })
    }
    fn table(&mut self, t: &a::TableFactor, outer: &Schema) -> R<(Source, Schema)> {
        self.sources += 1;
        if self.sources > 32 {
            return Err(Error::Limit);
        }
        let (source, fields, alias, default) = match t {
            a::TableFactor::Table {
                name,
                alias,
                args,
                with_hints,
                version,
                with_ordinality,
                partitions,
                json_path,
            } => {
                if !with_hints.is_empty()
                    || version.is_some()
                    || *with_ordinality
                    || !partitions.is_empty()
                    || json_path.is_some()
                {
                    return Err(Error::Unsupported);
                }
                let p = object(name);
                let n = p.last().ok_or(Error::Unsupported)?.clone();
                if p.len() > 2 || p.len() == 2 && p[0] != "pg_catalog" {
                    return Err(Error::Unsupported);
                }
                if let Some(args) = args {
                    if n == "pg_available_extension_versions"
                        && args.args.is_empty()
                        && args.settings.is_none()
                    {
                        (
                            Source::Empty,
                            vec![
                                Field {
                                    name: "name".into(),
                                    data_type: PgType::Name,
                                },
                                Field {
                                    name: "version".into(),
                                    data_type: PgType::Text,
                                },
                            ],
                            alias,
                            n,
                        )
                    } else if n == "unnest" && args.args.len() == 1 && args.settings.is_none() {
                        let mut es = self.args(&args.args, outer)?;
                        let e = es.remove(0);
                        no_set_or_window(&e)?;
                        let ty = array_element(e.ty)?;
                        (
                            Source::Unnest(e, false),
                            vec![Field {
                                name: n.clone(),
                                data_type: ty,
                            }],
                            alias,
                            n,
                        )
                    } else if n == "pg_indexam_has_property" && args.settings.is_none() {
                        let args = self.args(&args.args, outer)?;
                        let mut e = E {
                            ty: call_type(&n, &args)?,
                            kind: K::Call(n.clone(), args),
                        };
                        check_expr(&mut e)?;
                        no_set_or_window(&e)?;
                        let name = alias.as_ref().map_or(n.clone(), |a| ident(&a.name));
                        (
                            Source::Scalar(e),
                            vec![Field {
                                name,
                                data_type: PgType::Bool,
                            }],
                            alias,
                            n,
                        )
                    } else {
                        if n != "generate_series" || args.args.len() != 2 || args.settings.is_some()
                        {
                            return Err(Error::Unsupported);
                        }
                        let mut es = self.args(&args.args, &vec![])?;
                        for e in &mut es {
                            expect(e, PgType::Int4)?;
                            no_set_or_window(e)?;
                        }
                        (
                            Source::Series(es[0].clone(), es[1].clone()),
                            vec![Field {
                                name: n.clone(),
                                data_type: PgType::Int4,
                            }],
                            alias,
                            n,
                        )
                    }
                } else {
                    if p.len() == 1 && self.ctes.contains_key(&n) {
                        let q = self.ctes[&n].clone();
                        let fields = q.fields.clone();
                        (Source::Derived(q), fields, alias, n)
                    } else {
                        (
                            Source::Catalog(n.clone()),
                            catalog::schema(&n).ok_or(Error::Unsupported)?,
                            alias,
                            n,
                        )
                    }
                }
            }
            a::TableFactor::Derived {
                lateral: false,
                subquery,
                alias,
            } => {
                let q = self.query(subquery, &vec![])?;
                let fs = q.fields.clone();
                (
                    Source::Derived(std::sync::Arc::new(q)),
                    fs,
                    alias,
                    String::new(),
                )
            }
            a::TableFactor::UNNEST {
                alias,
                array_exprs,
                with_offset,
                with_offset_alias,
                with_ordinality,
            } => {
                if array_exprs.len() != 1 || *with_offset || with_offset_alias.is_some() {
                    return Err(Error::Unsupported);
                }
                let e = self.expr(&array_exprs[0], outer)?;
                no_set_or_window(&e)?;
                let ty = array_element(e.ty)?;
                let mut fields = vec![Field {
                    name: "unnest".into(),
                    data_type: ty,
                }];
                if *with_ordinality {
                    fields.push(Field {
                        name: "ordinality".into(),
                        data_type: PgType::Int8,
                    });
                }
                (
                    Source::Unnest(e, *with_ordinality),
                    fields,
                    alias,
                    "unnest".into(),
                )
            }
            _ => return Err(Error::Unsupported),
        };
        let mut fields = fields;
        let qualifier = if let Some(a) = alias {
            if !a.columns.is_empty() {
                if a.columns.len() != fields.len() {
                    return Err(Error::Unsupported);
                }
                for (f, c) in fields.iter_mut().zip(&a.columns) {
                    f.name = ident(&c.name)
                }
            }
            ident(&a.name)
        } else {
            default
        };
        Ok((
            source,
            fields
                .into_iter()
                .map(|field| Col {
                    qualifier: qualifier.clone(),
                    field,
                })
                .collect(),
        ))
    }
    fn args(&mut self, args: &[a::FunctionArg], schema: &Schema) -> R<Vec<E>> {
        args.iter()
            .map(|x| match x {
                a::FunctionArg::Unnamed(a::FunctionArgExpr::Expr(e)) => self.expr(e, schema),
                _ => Err(Error::Unsupported),
            })
            .collect()
    }
    fn expr(&mut self, e: &A, schema: &Schema) -> R<E> {
        let mut result = self.expr_inner(e, schema)?;
        check_expr(&mut result)?;
        Ok(result)
    }
    fn expr_inner(&mut self, e: &A, schema: &Schema) -> R<E> {
        // A bounded structural adapter for the installed DataGrip startup query.
        // Match the parsed expression, never a SQL prefix; no general timestamp
        // or PostgreSQL postmaster semantics are claimed by this profile.
        static STARTUP: std::sync::LazyLock<A> = std::sync::LazyLock::new(|| {
            let statements = Parser::parse_sql(
                &PostgreSqlDialect {},
                "SELECT round(extract(epoch from pg_postmaster_start_time() at time zone 'UTC'))",
            )
            .expect("constant SQL");
            let a::Statement::Query(q) = &statements[0] else {
                unreachable!()
            };
            let a::SetExpr::Select(s) = q.body.as_ref() else {
                unreachable!()
            };
            let a::SelectItem::UnnamedExpr(e) = &s.projection[0] else {
                unreachable!()
            };
            e.clone()
        });
        if e == &*STARTUP {
            return Ok(E {
                kind: K::Call("_service_start_epoch".into(), vec![]),
                ty: PgType::Int8,
            });
        }
        let (kind, ty) = match e {
            A::Identifier(n) => {
                let i = lookup(schema, &[ident(n)])?;
                return Ok(column(i, schema[i].field.data_type));
            }
            A::CompoundIdentifier(p) => {
                let i = lookup(schema, &p.iter().map(ident).collect::<Vec<_>>())?;
                return Ok(column(i, schema[i].field.data_type));
            }
            A::Nested(e) => return self.expr(e, schema),
            A::Value(v) => {
                return Ok(match v {
                    a::Value::Null => null(),
                    a::Value::Boolean(b) => literal(Value::Bool(*b), PgType::Bool),
                    a::Value::Number(n, _) => {
                        let i = n.parse::<i64>().map_err(|_| Error::Type)?;
                        literal(
                            Value::Int(i),
                            if i32::try_from(i).is_ok() {
                                PgType::Int4
                            } else {
                                PgType::Int8
                            },
                        )
                    }
                    a::Value::SingleQuotedString(s) | a::Value::EscapedStringLiteral(s) => {
                        literal(Value::Text(s.clone()), PgType::Text)
                    }
                    a::Value::Placeholder(p) => {
                        let n = p
                            .strip_prefix('$')
                            .and_then(|s| s.parse::<usize>().ok())
                            .filter(|n| *n > 0 && *n <= 256)
                            .ok_or(Error::Parameters)?;
                        self.parameters = self.parameters.max(n);
                        E {
                            kind: K::Param(n - 1),
                            ty: PgType::Text,
                        }
                    }
                    _ => return Err(Error::Unsupported),
                });
            }
            A::BinaryOp { left, op, right } => {
                if !matches!(
                    op,
                    Op::Eq
                        | Op::NotEq
                        | Op::Lt
                        | Op::LtEq
                        | Op::Gt
                        | Op::GtEq
                        | Op::And
                        | Op::Or
                        | Op::Plus
                        | Op::Minus
                        | Op::Multiply
                        | Op::Divide
                        | Op::Modulo
                        | Op::StringConcat
                        | Op::PGRegexMatch
                        | Op::PGRegexNotMatch
                ) {
                    return Err(Error::Unsupported);
                }
                return Ok(binary(
                    self.expr(left, schema)?,
                    op.clone(),
                    self.expr(right, schema)?,
                ));
            }
            A::UnaryOp { op, expr } => {
                let e = Box::new(self.expr(expr, schema)?);
                match op {
                    a::UnaryOperator::Not => (K::Not(e), PgType::Bool),
                    a::UnaryOperator::Minus => (K::Neg(e), PgType::Int8),
                    a::UnaryOperator::Plus => return Ok(*e),
                    _ => return Err(Error::Unsupported),
                }
            }
            A::IsNull(e) => (
                K::Is(Box::new(self.expr(e, schema)?), Value::Null, false),
                PgType::Bool,
            ),
            A::IsNotNull(e) => (
                K::Is(Box::new(self.expr(e, schema)?), Value::Null, true),
                PgType::Bool,
            ),
            A::IsTrue(e) => (
                K::Is(Box::new(self.expr(e, schema)?), Value::Bool(true), false),
                PgType::Bool,
            ),
            A::IsFalse(e) => (
                K::Is(Box::new(self.expr(e, schema)?), Value::Bool(false), false),
                PgType::Bool,
            ),
            A::InList {
                expr,
                list,
                negated,
            } => (
                K::In(
                    Box::new(self.expr(expr, schema)?),
                    list.iter()
                        .map(|x| self.expr(x, schema))
                        .collect::<R<Vec<_>>>()?,
                    *negated,
                ),
                PgType::Bool,
            ),
            A::Like {
                expr,
                pattern,
                negated,
                escape_char,
                any: false,
            } => {
                let escape = escape_char.clone().unwrap_or_else(|| "\\".into());
                if escape.chars().count() > 1 {
                    return Err(Error::Type);
                }
                let call = E {
                    kind: K::Call(
                        "like".into(),
                        vec![
                            self.expr(expr, schema)?,
                            self.expr(pattern, schema)?,
                            literal(Value::Text(escape), PgType::Text),
                        ],
                    ),
                    ty: PgType::Bool,
                };
                return Ok(if *negated {
                    E {
                        kind: K::Not(Box::new(call)),
                        ty: PgType::Bool,
                    }
                } else {
                    call
                });
            }
            A::Cast {
                expr,
                data_type,
                format: None,
                kind,
            } => {
                if !matches!(kind, a::CastKind::Cast | a::CastKind::DoubleColon) {
                    return Err(Error::Unsupported);
                }
                let e = Box::new(self.expr(expr, schema)?);
                let name = data_type.to_string().to_ascii_lowercase();
                match name.as_str() {
                    "regclass" => (K::Regclass(e), PgType::Oid),
                    "regproc" => (K::Regproc(e), PgType::Oid),
                    "regoper" => (K::Regoper(e), PgType::Oid),
                    _ => {
                        let ty = cast_type(&name)?;
                        (K::Cast(e, ty), ty)
                    }
                }
            }
            A::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                let operand = operand
                    .as_ref()
                    .map(|e| self.expr(e, schema).map(Box::new))
                    .transpose()?;
                let pairs = conditions
                    .iter()
                    .zip(results)
                    .map(|(c, r)| Ok((self.expr(c, schema)?, self.expr(r, schema)?)))
                    .collect::<R<Vec<_>>>()?;
                let el = else_result
                    .as_ref()
                    .map(|e| self.expr(e, schema))
                    .transpose()?
                    .unwrap_or_else(null);
                let ty = pairs
                    .iter()
                    .find(|(_, e)| !matches!(e.kind, K::Literal(Value::Null)))
                    .map_or(el.ty, |(_, e)| e.ty);
                (K::Case(operand, pairs, Box::new(el)), ty)
            }
            A::Function(f) => {
                if f.filter.is_some()
                    || f.null_treatment.is_some()
                    || !f.within_group.is_empty()
                    || !matches!(f.parameters, a::FunctionArguments::None)
                    || f.uses_odbc_syntax
                {
                    return Err(Error::Unsupported);
                }
                let n = function(&f.name)?;
                if n == "array"
                    && let a::FunctionArguments::Subquery(q) = &f.args
                {
                    if f.over.is_some() {
                        return Err(Error::Unsupported);
                    }
                    let q = self.query(q, schema)?;
                    if q.fields.len() != 1 {
                        return Err(Error::Type);
                    }
                    let ty = array_type(q.fields[0].data_type)?;
                    return Ok(E {
                        kind: K::ArrayQuery(Box::new(q)),
                        ty,
                    });
                }
                if n == "array_agg" {
                    if f.over.is_some() {
                        return Err(Error::Unsupported);
                    }
                    let a::FunctionArguments::List(args) = &f.args else {
                        return Err(Error::Unsupported);
                    };
                    if args.duplicate_treatment.is_some() || args.args.len() != 1 {
                        return Err(Error::Unsupported);
                    }
                    let arg = self.args(&args.args, schema)?.remove(0);
                    no_set_or_window(&arg)?;
                    if has_aggregate(&arg) {
                        return Err(Error::Unsupported);
                    }
                    let mut order = vec![];
                    for clause in &args.clauses {
                        let a::FunctionArgumentClause::OrderBy(orders) = clause else {
                            return Err(Error::Unsupported);
                        };
                        for o in orders {
                            if o.with_fill.is_some() {
                                return Err(Error::Unsupported);
                            }
                            let e = self.expr(&o.expr, schema)?;
                            no_set_or_window(&e)?;
                            if has_aggregate(&e) {
                                return Err(Error::Unsupported);
                            }
                            let asc = o.asc.unwrap_or(true);
                            order.push(Order {
                                e,
                                output: None,
                                asc,
                                nulls_first: o.nulls_first.unwrap_or(!asc),
                            });
                        }
                    }
                    let ty = array_type(arg.ty)?;
                    return Ok(E {
                        kind: K::ArrayAgg(Box::new(arg), order),
                        ty,
                    });
                }
                let args = match &f.args {
                    a::FunctionArguments::None => vec![],
                    a::FunctionArguments::List(l)
                        if l.duplicate_treatment.is_none() && l.clauses.is_empty() =>
                    {
                        self.args(&l.args, schema)?
                    }
                    _ => return Err(Error::Unsupported),
                };
                if n == "row_number" {
                    let Some(a::WindowType::WindowSpec(w)) = &f.over else {
                        return Err(Error::Unsupported);
                    };
                    if !args.is_empty() || w.window_name.is_some() || w.window_frame.is_some() {
                        return Err(Error::Unsupported);
                    }
                    let p = w
                        .partition_by
                        .iter()
                        .map(|e| self.expr(e, schema))
                        .collect::<R<Vec<_>>>()?;
                    let mut o = vec![];
                    for e in &p {
                        no_set_or_window(e)?;
                    }
                    for x in &w.order_by {
                        if x.with_fill.is_some() {
                            return Err(Error::Unsupported);
                        }
                        let asc = x.asc.unwrap_or(true);
                        let expr = self.expr(&x.expr, schema)?;
                        no_set_or_window(&expr)?;
                        o.push(Order {
                            e: expr,
                            output: None,
                            asc,
                            nulls_first: x.nulls_first.unwrap_or(!asc),
                        })
                    }
                    (K::RowNumber(p, o), PgType::Int8)
                } else {
                    if f.over.is_some() {
                        return Err(Error::Unsupported);
                    }
                    if n == "_pg_expandarray" {
                        if args.len() != 1 {
                            return Err(Error::Type);
                        }
                        (K::Expand(Box::new(args[0].clone())), PgType::Record)
                    } else {
                        let ty = call_type(&n, &args)?;
                        (K::Call(n, args), ty)
                    }
                }
            }
            A::Array(arr) => {
                let es = arr
                    .elem
                    .iter()
                    .map(|e| self.expr(e, schema))
                    .collect::<R<Vec<_>>>()?;
                let mut element = es.first().ok_or(Error::Type)?.ty;
                for e in &es {
                    if numeric(element) && numeric(e.ty) {
                        element = if element == PgType::Int8 || e.ty == PgType::Int8 {
                            PgType::Int8
                        } else if element == PgType::Int4 || e.ty == PgType::Int4 {
                            PgType::Int4
                        } else {
                            element
                        };
                    } else if unknown(&es[0]) && !unknown(e) {
                        element = e.ty
                    }
                }
                let ty = match element {
                    PgType::Int2 => PgType::Int2Array,
                    PgType::Int4 => PgType::Int4Array,
                    PgType::Int8 => PgType::Int8Array,
                    PgType::Oid => PgType::OidArray,
                    PgType::Bool => PgType::BoolArray,
                    PgType::Bytea => PgType::ByteaArray,
                    PgType::Name => PgType::NameArray,
                    PgType::Char => PgType::CharArray,
                    PgType::Text => PgType::TextArray,
                    _ => return Err(Error::Unsupported),
                };
                (K::Array(es), ty)
            }
            A::Subscript { expr, subscript } => {
                let a::Subscript::Index { index } = subscript.as_ref() else {
                    return Err(Error::Unsupported);
                };
                let e = self.expr(expr, schema)?;
                let ty = array_element(e.ty)?;
                (
                    K::Index(Box::new(e), Box::new(self.expr(index, schema)?)),
                    ty,
                )
            }
            A::CompositeAccess { expr, key } => {
                let e = self.expr(expr, schema)?;
                if e.ty != PgType::Record {
                    return Err(Error::Type);
                }
                let (i, t) = match ident(key).as_str() {
                    "x" => (0, PgType::Int2),
                    "n" => (1, PgType::Int4),
                    _ => return Err(Error::UnknownColumn),
                };
                (K::Member(Box::new(e), i), t)
            }
            A::AnyOp {
                left,
                compare_op,
                right,
                ..
            } if *compare_op == Op::Eq => (
                K::Any(
                    Box::new(self.expr(left, schema)?),
                    compare_op.clone(),
                    Box::new(self.expr(right, schema)?),
                ),
                PgType::Bool,
            ),
            A::Subquery(q) => {
                let q = self.query(q, schema)?;
                if q.fields.len() != 1 {
                    return Err(Error::Type);
                }
                let ty = q.fields[0].data_type;
                (K::Subquery(Box::new(q)), ty)
            }
            A::Tuple(es) => (
                K::Call(
                    "_row".into(),
                    es.iter()
                        .map(|e| self.expr(e, schema))
                        .collect::<R<Vec<_>>>()?,
                ),
                PgType::Record,
            ),
            _ => return Err(Error::Unsupported),
        };
        Ok(E { kind, ty })
    }
}
fn lookup(schema: &[Col], p: &[String]) -> R<usize> {
    if p.is_empty() || p.len() > 2 {
        return Err(Error::UnknownColumn);
    }
    let found: Vec<usize> = schema
        .iter()
        .enumerate()
        .filter(|(_, c)| c.field.name == p[p.len() - 1] && (p.len() == 1 || c.qualifier == p[0]))
        .map(|(i, _)| i)
        .collect();
    match found.as_slice() {
        [i] => Ok(*i),
        [] => Err(Error::UnknownColumn),
        _ => Err(Error::AmbiguousColumn),
    }
}
fn array_type(t: PgType) -> R<PgType> {
    Ok(match t {
        PgType::Bool => PgType::BoolArray,
        PgType::Int2 => PgType::Int2Array,
        PgType::Int4 => PgType::Int4Array,
        PgType::Int8 => PgType::Int8Array,
        PgType::Oid => PgType::OidArray,
        PgType::Text => PgType::TextArray,
        PgType::Name => PgType::NameArray,
        PgType::Char => PgType::CharArray,
        PgType::Bytea => PgType::ByteaArray,
        _ => return Err(Error::Unsupported),
    })
}
fn has_aggregate(e: &E) -> bool {
    matches!(e.kind, K::ArrayAgg(..)) || children(e).into_iter().any(has_aggregate)
}
fn validate_group(e: &E, group: &[E]) -> R<()> {
    if group.contains(e) || matches!(e.kind, K::ArrayAgg(..)) {
        return Ok(());
    }
    if matches!(
        e.kind,
        K::Column(_) | K::RowNumber(..) | K::Expand(_) | K::Subquery(_) | K::ArrayQuery(_)
    ) {
        return Err(Error::Unsupported);
    }
    for c in children(e) {
        validate_group(c, group)?;
    }
    Ok(())
}
fn cast_type(n: &str) -> R<PgType> {
    Ok(match n {
        "bool" | "boolean" => PgType::Bool,
        "smallint" | "int2" => PgType::Int2,
        "int" | "integer" | "int4" => PgType::Int4,
        "bigint" | "int8" => PgType::Int8,
        "oid" => PgType::Oid,
        "int[]" | "integer[]" | "int4[]" => PgType::Int4Array,
        "oid[]" => PgType::OidArray,
        "text" | "varchar" | "character varying" => PgType::Text,
        "name" => PgType::Name,
        "char" | "\"char\"" => PgType::Char,
        _ => return Err(Error::Unsupported),
    })
}
fn numeric(t: PgType) -> bool {
    matches!(t, PgType::Int2 | PgType::Int4 | PgType::Int8 | PgType::Oid)
}
fn array_element(t: PgType) -> R<PgType> {
    if t == PgType::Int2Vector {
        Ok(PgType::Int2)
    } else if t == PgType::OidVector {
        Ok(PgType::Oid)
    } else {
        t.array_element().ok_or(Error::Type)
    }
}
fn textual(t: PgType) -> bool {
    matches!(t, PgType::Text | PgType::Name | PgType::Char)
}
fn unknown(e: &E) -> bool {
    matches!(
        e.kind,
        K::Literal(Value::Null | Value::Text(_)) | K::Param(_)
    )
}
fn expect(e: &mut E, t: PgType) -> R<()> {
    if unknown(e) {
        if let K::Literal(v) = &e.kind {
            e.kind = K::Literal(cast(v.clone(), t)?)
        }
        e.ty = t;
        return Ok(());
    }
    if e.ty == t || numeric(e.ty) && numeric(t) || textual(e.ty) && textual(t) {
        Ok(())
    } else {
        Err(Error::Type)
    }
}
fn compatible(l: &mut E, r: &mut E) -> R<()> {
    if unknown(l) && !unknown(r) {
        expect(l, r.ty)
    } else {
        expect(r, l.ty)
    }
}
fn common_type<'a>(expressions: impl Iterator<Item = &'a E>) -> R<PgType> {
    let mut result = None;
    for e in expressions.filter(|e| !unknown(e)) {
        result = Some(match result {
            None => e.ty,
            Some(t) if t == e.ty => t,
            Some(t) if numeric(t) && numeric(e.ty) => {
                if matches!(t, PgType::Oid | PgType::Int8)
                    || matches!(e.ty, PgType::Oid | PgType::Int8)
                {
                    PgType::Int8
                } else if t == PgType::Int4 || e.ty == PgType::Int4 {
                    PgType::Int4
                } else {
                    PgType::Int2
                }
            }
            Some(t) if textual(t) && textual(e.ty) => PgType::Text,
            _ => return Err(Error::Type),
        });
    }
    Ok(result.unwrap_or(PgType::Text))
}
fn coerce(e: &mut E, t: PgType) -> R<()> {
    expect(e, t)?;
    if e.ty != t {
        *e = E {
            kind: K::Cast(Box::new(e.clone()), t),
            ty: t,
        };
    }
    Ok(())
}
fn check_expr(e: &mut E) -> R<()> {
    match &mut e.kind {
        K::Binary(l, op, r) => match op {
            Op::And | Op::Or => {
                expect(l, PgType::Bool)?;
                expect(r, PgType::Bool)?;
            }
            Op::Plus | Op::Minus | Op::Multiply | Op::Divide | Op::Modulo => {
                expect(l, PgType::Int8)?;
                expect(r, PgType::Int8)?;
            }
            Op::StringConcat | Op::PGRegexMatch | Op::PGRegexNotMatch => {
                expect(l, PgType::Text)?;
                expect(r, PgType::Text)?;
            }
            _ => compatible(l, r)?,
        },
        K::Not(x) => expect(x, PgType::Bool)?,
        K::Neg(x) => expect(x, PgType::Int8)?,
        K::Is(x, Value::Bool(_), _) => expect(x, PgType::Bool)?,
        K::In(x, es, _) => {
            for r in es {
                compatible(x, r)?
            }
        }
        K::Any(l, _, r) => {
            let ty = array_element(r.ty)?;
            expect(l, ty)?;
        }
        K::Index(_, i) => expect(i, PgType::Int4)?,
        K::Expand(x) => {
            if !matches!(x.ty, PgType::Int2Array | PgType::Int2Vector) {
                return Err(Error::Type);
            }
        }
        K::Array(es) => {
            if !es.is_empty() {
                let ty = array_element(e.ty)?;
                for x in es {
                    coerce(x, ty)?
                }
            }
        }
        K::Case(operand, pairs, el) => {
            let t = common_type(
                pairs
                    .iter()
                    .map(|(_, r)| r)
                    .chain(std::iter::once(el.as_ref())),
            )?;
            for (c, r) in pairs {
                if let Some(o) = operand.as_mut() {
                    compatible(o, c)?
                } else {
                    expect(c, PgType::Bool)?
                }
                coerce(r, t)?
            }
            coerce(el, t)?;
            e.ty = t;
        }
        K::Call(n, args) => match n.as_str() {
            "current_schemas" => expect(&mut args[0], PgType::Bool)?,
            "quote_ident" => expect(&mut args[0], PgType::Text)?,
            "chr" => expect(&mut args[0], PgType::Int4)?,
            "pg_tablespace_location" | "pg_get_partkeydef" => expect(&mut args[0], PgType::Oid)?,
            "replace" | "like" => {
                for x in args {
                    expect(x, PgType::Text)?
                }
            }
            "length" => expect(&mut args[0], PgType::Text)?,
            "array_upper" | "array_lower" => {
                array_element(args[0].ty)?;
                expect(&mut args[1], PgType::Int4)?
            }
            "cardinality" => {
                array_element(args[0].ty)?;
            }
            "pg_get_expr" => {
                expect(&mut args[0], PgType::Text)?;
                expect(&mut args[1], PgType::Oid)?;
            }
            "format_type" | "col_description" => {
                expect(&mut args[0], PgType::Oid)?;
                expect(&mut args[1], PgType::Int4)?;
            }
            "pg_indexam_has_property" => {
                expect(&mut args[0], PgType::Oid)?;
                expect(&mut args[1], PgType::Text)?;
            }
            "pg_get_indexdef" => {
                expect(&mut args[0], PgType::Oid)?;
                expect(&mut args[1], PgType::Int4)?;
                expect(&mut args[2], PgType::Bool)?;
            }
            "obj_description" => {
                expect(&mut args[0], PgType::Oid)?;
                expect(&mut args[1], PgType::Text)?;
            }
            "pg_get_userbyid" | "pg_table_is_visible" | "pg_type_is_visible" => {
                expect(&mut args[0], PgType::Oid)?
            }
            "has_table_privilege" | "has_type_privilege" | "has_schema_privilege" => {
                expect(&mut args[1], PgType::Text)?
            }
            "nullif" => {
                let t = common_type(args.iter())?;
                for x in args {
                    coerce(x, t)?;
                }
                e.ty = t;
            }
            "coalesce" => {
                let t = common_type(args.iter())?;
                for x in args {
                    coerce(x, t)?
                }
                e.ty = t;
            }
            _ => {}
        },
        _ => {}
    }
    Ok(())
}
fn call_type(n: &str, a: &[E]) -> R<PgType> {
    Ok(match (n, a.len()) {
        ("current_database" | "current_schema" | "current_user" | "session_user", 0) => {
            PgType::Name
        }
        ("version", 0) => PgType::Text,
        ("current_schemas", 1) => PgType::NameArray,
        ("replace", 3)
        | ("pg_get_expr", 2)
        | ("format_type", 2)
        | ("obj_description", 2)
        | ("col_description", 2) => PgType::Text,
        ("pg_get_userbyid", 1) => PgType::Name,
        ("pg_get_indexdef", 3) => PgType::Text,
        ("pg_indexam_has_property", 2) => PgType::Bool,
        ("chr", 1) => PgType::Text,
        ("quote_ident" | "pg_tablespace_location" | "pg_get_partkeydef", 1) => PgType::Text,
        ("array_upper" | "array_lower", 2) | ("cardinality" | "length", 1) => PgType::Int4,
        ("nullif", 2) => a[0].ty,
        ("coalesce", n) if n > 0 => a[0].ty,
        ("has_table_privilege" | "has_schema_privilege" | "has_type_privilege", 2)
        | ("pg_table_is_visible" | "pg_type_is_visible", 1) => PgType::Bool,
        _ => return Err(Error::Unsupported),
    })
}
fn find_expand(e: &E, out: &mut Option<E>) -> R<()> {
    match &e.kind {
        K::Expand(a) => {
            if let Some(prior) = out {
                if prior != a.as_ref() {
                    return Err(Error::Unsupported);
                }
            } else {
                *out = Some(*a.clone())
            }
        }
        K::Member(a, _) | K::Cast(a, _) | K::Not(a) | K::Neg(a) => find_expand(a, out)?,
        // SRFs are intentionally limited to a direct projection or field access.
        _ => {
            if children(e).into_iter().any(has_expand) {
                return Err(Error::Unsupported);
            }
        }
    }
    Ok(())
}
fn children(e: &E) -> Vec<&E> {
    match &e.kind {
        K::Binary(a, _, b) | K::Any(a, _, b) | K::Index(a, b) => vec![a, b],
        K::Not(a)
        | K::Neg(a)
        | K::Is(a, _, _)
        | K::Cast(a, _)
        | K::Regclass(a)
        | K::Regproc(a)
        | K::Regoper(a)
        | K::Member(a, _)
        | K::Expand(a) => vec![a],
        K::Call(_, es) | K::Array(es) => es.iter().collect(),
        K::ArrayAgg(e, order) => std::iter::once(e.as_ref())
            .chain(order.iter().map(|o| &o.e))
            .collect(),
        K::In(a, es, _) => std::iter::once(a.as_ref()).chain(es.iter()).collect(),
        K::Case(a, es, b) => a
            .iter()
            .map(|a| a.as_ref())
            .chain(es.iter().flat_map(|(a, b)| [a, b]))
            .chain(std::iter::once(b.as_ref()))
            .collect(),
        K::RowNumber(p, o) => p.iter().chain(o.iter().map(|o| &o.e)).collect(),
        _ => vec![],
    }
}
fn has_expand(e: &E) -> bool {
    matches!(e.kind, K::Expand(_)) || children(e).into_iter().any(has_expand)
}
fn no_set_or_window(e: &E) -> R<()> {
    if matches!(e.kind, K::Expand(_) | K::RowNumber(_, _) | K::ArrayAgg(..)) {
        return Err(Error::Unsupported);
    }
    for c in children(e) {
        no_set_or_window(c)?
    }
    Ok(())
}
fn collect_parameters(q: &Query, types: &mut [Option<PgType>]) -> R<()> {
    fn expr(e: &E, types: &mut [Option<PgType>], hint: Option<PgType>) -> R<()> {
        if let K::Param(i) = e.kind {
            let t = hint.unwrap_or(e.ty);
            let old = types[i];
            types[i] = Some(match old {
                None => t,
                Some(old) if old == t => t,
                Some(old) if textual(old) => t,
                Some(old) if textual(t) => old,
                Some(old) if numeric(old) && numeric(t) => PgType::Int8,
                _ => return Err(Error::Type),
            });
        }
        if let K::Subquery(q) | K::ArrayQuery(q) = &e.kind {
            collect_parameters(q, types)?;
        }
        for c in children(e) {
            expr(
                c,
                types,
                if let K::Cast(_, t) = e.kind {
                    Some(t)
                } else {
                    None
                },
            )?
        }
        Ok(())
    }
    fn source(s: &Source, types: &mut [Option<PgType>]) -> R<()> {
        match s {
            Source::Derived(q) => collect_parameters(q, types)?,
            Source::Union(l, r, _) => {
                collect_parameters(l, types)?;
                collect_parameters(r, types)?;
            }
            Source::Join(l, r, on, _, _, _) => {
                source(l, types)?;
                source(r, types)?;
                expr(on, types, None)?
            }
            Source::Series(a, b) => {
                expr(a, types, None)?;
                expr(b, types, None)?
            }
            Source::Unnest(e, _) | Source::Scalar(e) => expr(e, types, None)?,
            _ => {}
        }
        Ok(())
    }
    source(&q.source, types)?;
    expr(&q.filter, types, None)?;
    for e in &q.projection {
        expr(e, types, None)?
    }
    for o in &q.order {
        expr(&o.e, types, None)?
    }
    for e in &q.group {
        expr(e, types, None)?;
    }
    Ok(())
}

struct Runtime<'a> {
    startup: Option<i64>,
    catalogs: &'a BTreeMap<String, catalog::Relation>,
    parameters: &'a [Value],
    work: usize,
}
fn admit(rows: &[Vec<Value>], row: &[Value], bytes: &mut usize) -> R<()> {
    let n = row.iter().map(Value::bytes).sum::<usize>();
    *bytes = bytes.checked_add(n).ok_or(Error::Limit)?;
    if rows.len() >= MAX_ROWS || *bytes > MAX_BYTES {
        Err(Error::Limit)
    } else {
        Ok(())
    }
}
fn boolean(v: Value) -> R<Option<bool>> {
    match v {
        Value::Null => Ok(None),
        Value::Bool(b) => Ok(Some(b)),
        _ => Err(Error::Type),
    }
}
fn validate_values(values: &[Value]) -> R<()> {
    let mut stack = values.iter().map(|v| (v, 0usize)).collect::<Vec<_>>();
    let mut nodes = 0usize;
    let mut bytes = 0usize;
    while let Some((v, depth)) = stack.pop() {
        nodes += 1;
        if nodes > 4096 || depth > 16 {
            return Err(Error::Limit);
        }
        let n = match v {
            Value::Array(v) | Value::Record(v) => {
                if v.len() > 4096 {
                    return Err(Error::Limit);
                }
                stack.extend(v.iter().map(|v| (v, depth + 1)));
                v.len() * 4 + 8
            }
            _ => v.bytes(),
        };
        bytes = bytes.checked_add(n).ok_or(Error::Limit)?;
        if bytes > MAX_BYTES {
            return Err(Error::Limit);
        }
    }
    Ok(())
}
fn integer(v: Value) -> R<i64> {
    match v {
        Value::Int(i) => Ok(i),
        _ => Err(Error::Type),
    }
}
fn collect_values(iter: impl Iterator<Item = R<Value>>) -> R<Vec<Value>> {
    let mut out = vec![];
    let mut bytes = 0usize;
    for v in iter {
        let v = v?;
        bytes = bytes.checked_add(v.bytes()).ok_or(Error::Limit)?;
        if bytes > MAX_BYTES {
            return Err(Error::Limit);
        }
        out.push(v)
    }
    Ok(out)
}
fn text(v: Value) -> R<String> {
    match v {
        Value::Text(s) => Ok(s),
        _ => Err(Error::Type),
    }
}
impl Runtime<'_> {
    fn charge(&mut self, n: usize) -> R<()> {
        self.work = self.work.checked_add(n).ok_or(Error::Limit)?;
        if self.work > MAX_WORK {
            Err(Error::Limit)
        } else {
            Ok(())
        }
    }
    fn source(&mut self, s: &Source, outer: &[Value]) -> R<Vec<Vec<Value>>> {
        self.charge(1)?;
        match s {
            Source::Unit => Ok(vec![vec![]]),
            Source::Empty => Ok(vec![]),
            Source::Catalog(n) => {
                let r = self.catalogs.get(n).ok_or(Error::Unsupported)?;
                self.charge(r.rows.len())?;
                Ok(r.rows.clone())
            }
            Source::Derived(q) => self.query(q, &[]),
            Source::Union(l, r, distinct) => {
                let mut out = self.query(l, outer)?;
                let rhs = self.query(r, outer)?;
                let mut bytes = out.iter().flatten().map(Value::bytes).sum();
                let mut seen = std::collections::BTreeSet::new();
                if *distinct {
                    out.retain(|row| seen.insert(row.clone()));
                    bytes = out.iter().flatten().map(Value::bytes).sum();
                }
                for row in rhs {
                    self.charge(1)?;
                    if !*distinct || !seen.contains(&row) {
                        admit(&out, &row, &mut bytes)?;
                        if *distinct {
                            seen.insert(row.clone());
                        }
                        out.push(row);
                    }
                }
                Ok(out)
            }
            Source::Series(a, b) => {
                let start = integer(self.eval(a, &[], None, 0, &[], None)?)?;
                let end = integer(self.eval(b, &[], None, 0, &[], None)?)?;
                if end.saturating_sub(start) > 1024 {
                    return Err(Error::Limit);
                }
                Ok((start..=end).map(|i| vec![Value::Int(i)]).collect())
            }
            Source::Scalar(e) => Ok(vec![vec![self.eval(e, outer, None, 0, &[], None)?]]),
            Source::Unnest(e, ordinality) => match self.eval(e, outer, None, 0, &[], None)? {
                Value::Null => Ok(vec![]),
                Value::Array(values) => {
                    let mut out = vec![];
                    let mut bytes = 0;
                    for (i, v) in values.into_iter().enumerate() {
                        self.charge(1)?;
                        let mut row = vec![v];
                        if *ordinality {
                            row.push(Value::Int(i as i64 + 1));
                        }
                        admit(&out, &row, &mut bytes)?;
                        out.push(row);
                    }
                    Ok(out)
                }
                _ => Err(Error::Type),
            },
            Source::Join(l, r, on, left, rwidth, lateral) => {
                let ls = self.source(l, outer)?;
                let rs = if *lateral {
                    vec![]
                } else {
                    self.source(r, outer)?
                };
                let mut out = vec![];
                let mut bytes = 0;
                for l in &ls {
                    let lateral_rows = if *lateral {
                        if l.iter().chain(outer).map(Value::bytes).sum::<usize>() > MAX_BYTES {
                            return Err(Error::Limit);
                        }
                        let args = l.iter().chain(outer).cloned().collect::<Vec<_>>();
                        self.source(r, &args)?
                    } else {
                        vec![]
                    };
                    let rs = if *lateral { &lateral_rows } else { &rs };
                    let mut matched = false;
                    for r in rs {
                        self.charge(1)?;
                        // Candidate is admitted before cloning; it is released before the next candidate.
                        let size = l.iter().chain(r).map(Value::bytes).sum::<usize>();
                        if size > MAX_BYTES {
                            return Err(Error::Limit);
                        }
                        let mut row = l.clone();
                        row.extend(r.iter().cloned());
                        if boolean(self.eval(on, &row, None, 0, &[], None)?)? == Some(true) {
                            matched = true;
                            admit(&out, &row, &mut bytes)?;
                            out.push(row)
                        }
                    }
                    if *left && !matched {
                        let mut row = l.clone();
                        row.extend(std::iter::repeat_n(Value::Null, *rwidth));
                        admit(&out, &row, &mut bytes)?;
                        out.push(row)
                    }
                }
                Ok(out)
            }
        }
    }
    fn query(&mut self, q: &Query, outer: &[Value]) -> R<Vec<Vec<Value>>> {
        let source = self.source(&q.source, outer)?;
        let mut filtered = vec![];
        let mut bytes = 0;
        for mut row in source {
            if row.len() != q.source_width {
                return Err(Error::Type);
            }
            if row.iter().chain(outer).map(Value::bytes).sum::<usize>() > MAX_BYTES {
                return Err(Error::Limit);
            }
            row.extend_from_slice(outer);
            if boolean(self.eval(&q.filter, &row, None, 0, &[], None)?)? == Some(true) {
                admit(&filtered, &row, &mut bytes)?;
                filtered.push(row)
            }
        }
        let mut material = vec![];
        let mut bytes: usize = 0;
        let mut groups: Vec<Vec<Vec<Value>>> = vec![];
        if q.aggregate {
            if q.group.is_empty() {
                groups.push(filtered);
            } else {
                let mut indexed = BTreeMap::<Vec<Value>, Vec<Vec<Value>>>::new();
                let mut key_bytes = 0usize;
                for row in filtered {
                    let key = collect_values(
                        q.group
                            .iter()
                            .map(|e| self.eval(e, &row, None, 0, &[], None)),
                    )?;
                    key_bytes = key_bytes
                        .checked_add(key.iter().map(Value::bytes).sum::<usize>())
                        .ok_or(Error::Limit)?;
                    if key_bytes > MAX_BYTES {
                        return Err(Error::Limit);
                    }
                    indexed.entry(key).or_default().push(row);
                }
                groups.extend(indexed.into_values());
            }
        } else {
            groups.push(filtered);
        }
        for filtered in &groups {
            let empty = vec![Value::Null; q.source_width + outer.len()];
            let count = if q.aggregate { 1 } else { filtered.len() };
            for idx in 0..count {
                let row = filtered.get(idx).unwrap_or(&empty);
                let expansion = if let Some(e) = &q.expand {
                    match self.eval(e, row, None, 0, &[], None)? {
                        Value::Array(v) => v,
                        Value::Null => vec![],
                        _ => return Err(Error::Type),
                    }
                } else {
                    vec![Value::Null]
                };
                for (ei, elem) in expansion.iter().enumerate() {
                    let expanded = q
                        .expand
                        .as_ref()
                        .map(|_| Value::Record(vec![elem.clone(), Value::Int(ei as i64 + 1)]));
                    let mut result = vec![];
                    let mut keys = vec![];
                    for e in &q.projection {
                        let v = self.eval(
                            e,
                            row,
                            expanded.as_ref(),
                            idx,
                            filtered,
                            Some(MAX_BYTES - bytes),
                        )?;
                        let n = v.bytes();
                        if bytes.checked_add(n).ok_or(Error::Limit)? > MAX_BYTES {
                            return Err(Error::Limit);
                        }
                        bytes += n;
                        result.push(v);
                    }
                    for o in &q.order {
                        let v = if let Some(i) = o.output {
                            let v = result.get(i).ok_or(Error::UnknownColumn)?;
                            if v.bytes() > MAX_BYTES - bytes {
                                return Err(Error::Limit);
                            }
                            v.clone()
                        } else {
                            self.eval(
                                &o.e,
                                row,
                                expanded.as_ref(),
                                idx,
                                filtered,
                                Some(MAX_BYTES - bytes),
                            )?
                        };
                        bytes = bytes.checked_add(v.bytes()).ok_or(Error::Limit)?;
                        if bytes > MAX_BYTES {
                            return Err(Error::Limit);
                        }
                        keys.push(v)
                    }
                    if material.len() >= MAX_ROWS {
                        return Err(Error::Limit);
                    }
                    material.push((result, keys));
                }
            }
        }
        self.charge(
            material
                .len()
                .saturating_mul(material.len().max(1).ilog2() as usize + 1)
                .saturating_mul(q.order.len()),
        )?;
        material.sort_by(|l, r| compare_keys(&l.1, &r.1, &q.order));
        let mut rows = vec![];
        let mut seen = std::collections::BTreeSet::new();
        for (r, _) in material {
            if !q.distinct || seen.insert(r.clone()) {
                rows.push(r)
            }
            if rows.len() >= q.limit {
                break;
            }
        }
        if q.limit == 0 {
            rows.clear()
        }
        Ok(rows)
    }
    fn eval(
        &mut self,
        e: &E,
        row: &[Value],
        expanded: Option<&Value>,
        idx: usize,
        all: &[Vec<Value>],
        remaining: Option<usize>,
    ) -> R<Value> {
        self.charge(1)?;
        let mut ev = |e: &E| self.eval(e, row, expanded, idx, all, remaining);
        use Value::{Bool as B, Int as I, Null as N, Text as S};
        let value = match &e.kind {
            K::Literal(v) => {
                if v.bytes() > remaining.unwrap_or(MAX_BYTES) {
                    return Err(Error::Limit);
                }
                self.charge(v.bytes() / 64 + 1)?;
                v.clone()
            }
            K::Column(i) => {
                let v = row.get(*i).ok_or(Error::UnknownColumn)?;
                if v.bytes() > remaining.unwrap_or(MAX_BYTES) {
                    return Err(Error::Limit);
                }
                self.charge(v.bytes() / 64 + 1)?;
                v.clone()
            }
            K::Param(i) => {
                let v = self.parameters.get(*i).ok_or(Error::Parameters)?;
                if v.bytes() > remaining.unwrap_or(MAX_BYTES) {
                    return Err(Error::Limit);
                }
                self.charge(v.bytes() / 64 + 1)?;
                cast(v.clone(), e.ty)?
            }
            K::Binary(l, Op::And, r) => {
                let l = boolean(ev(l)?)?;
                if l == Some(false) {
                    B(false)
                } else {
                    let r = boolean(ev(r)?)?;
                    match (l, r) {
                        (Some(true), Some(true)) => B(true),
                        (_, Some(false)) => B(false),
                        _ => N,
                    }
                }
            }
            K::Binary(l, Op::Or, r) => {
                let l = boolean(ev(l)?)?;
                if l == Some(true) {
                    B(true)
                } else {
                    let r = boolean(ev(r)?)?;
                    match (l, r) {
                        (Some(false), Some(false)) => B(false),
                        (_, Some(true)) => B(true),
                        _ => N,
                    }
                }
            }
            K::Binary(l, op, r) => {
                let l = ev(l)?;
                let r = ev(r)?;
                if matches!(op, Op::PGRegexMatch | Op::PGRegexNotMatch) {
                    self.charge(l.bytes() + r.bytes().saturating_mul(64))?;
                }
                binary_value(l, op, r)?
            }
            K::Not(e) => boolean(ev(e)?)?.map_or(N, |b| B(!b)),
            K::Neg(e) => match ev(e)? {
                N => N,
                I(i) => I(i.checked_neg().ok_or(Error::Type)?),
                _ => return Err(Error::Type),
            },
            K::Is(e, v, neg) => B((ev(e)? == *v) ^ neg),
            K::Cast(e, t) => cast(ev(e)?, *t)?,
            K::Regclass(e) => match ev(e)? {
                N => N,
                S(s) => {
                    let name = s.strip_prefix("pg_catalog.").unwrap_or(&s);
                    I(catalog::relation_oid(name).ok_or(Error::Unsupported)? as i64)
                }
                I(i) => I(i),
                _ => return Err(Error::Type),
            },
            K::Regproc(e) => match ev(e)? {
                N => N,
                S(s) if s == "pg_catalog.array_in" || s == "array_in" => I(750),
                _ => return Err(Error::Unsupported),
            },
            K::Regoper(e) => match ev(e)? {
                N => N,
                _ => return Err(Error::Unsupported),
            },
            K::Case(operand, pairs, el) => {
                let o = operand.as_ref().map(|e| ev(e)).transpose()?;
                let mut value = None;
                for (c, r) in pairs {
                    let c = ev(c)?;
                    let yes = if let Some(o) = &o {
                        *o != N && c != N && *o == c
                    } else {
                        boolean(c)? == Some(true)
                    };
                    if yes {
                        value = Some(ev(r)?);
                        break;
                    }
                }
                match value {
                    Some(v) => v,
                    None => ev(el)?,
                }
            }
            K::Call(n, args) => {
                if n == "coalesce" {
                    let mut out = N;
                    for e in args {
                        let v = ev(e)?;
                        if v != N {
                            out = v;
                            break;
                        }
                    }
                    return Ok(out);
                }
                let mut values = vec![];
                let mut bytes = 0usize;
                for e in args {
                    let v = ev(e)?;
                    bytes = bytes.checked_add(v.bytes()).ok_or(Error::Limit)?;
                    if bytes > MAX_BYTES {
                        return Err(Error::Limit);
                    }
                    values.push(v)
                }
                let value = self.call(n, values)?;
                if matches!(n.as_str(), "array_upper" | "array_lower")
                    && matches!(args[0].ty, PgType::Int2Vector | PgType::OidVector)
                {
                    match value {
                        I(i) => I(i - 1),
                        v => v,
                    }
                } else {
                    value
                }
            }
            K::Array(es) => {
                let mut values = vec![];
                let mut bytes = 8usize;
                for e in es {
                    let v = ev(e)?;
                    bytes = bytes.checked_add(v.bytes() + 4).ok_or(Error::Limit)?;
                    if bytes > MAX_BYTES {
                        return Err(Error::Limit);
                    }
                    values.push(v)
                }
                Value::Array(values)
            }
            K::Index(a, i) => {
                let lower = if matches!(a.ty, PgType::Int2Vector | PgType::OidVector) {
                    0
                } else {
                    1
                };
                let a = ev(a)?;
                let i = ev(i)?;
                match (a, i) {
                    (N, _) | (_, N) => N,
                    (Value::Array(v), I(i)) => {
                        if i >= lower {
                            v.get((i - lower) as usize).cloned().unwrap_or(N)
                        } else {
                            N
                        }
                    }
                    _ => return Err(Error::Type),
                }
            }
            K::Member(e, i) => match ev(e)? {
                N => N,
                Value::Record(v) => v.get(*i).cloned().ok_or(Error::Type)?,
                _ => return Err(Error::Type),
            },
            K::Any(l, op, r) => {
                let l = ev(l)?;
                let r = ev(r)?;
                match r {
                    N => N,
                    Value::Array(v) => {
                        let mut out = B(false);
                        for r in v {
                            match binary_value(l.clone(), op, r)? {
                                B(true) => {
                                    out = B(true);
                                    break;
                                }
                                N => out = N,
                                _ => {}
                            }
                        }
                        out
                    }
                    _ => return Err(Error::Type),
                }
            }
            K::In(lhs, es, negated) => {
                let lhs = ev(lhs)?;
                let mut out = B(false);
                for e in es {
                    match binary_value(lhs.clone(), &Op::Eq, ev(e)?)? {
                        B(true) => {
                            out = B(true);
                            break;
                        }
                        N => out = N,
                        _ => {}
                    }
                }
                if *negated {
                    match out {
                        B(b) => B(!b),
                        _ => N,
                    }
                } else {
                    out
                }
            }
            K::Subquery(q) => {
                let rows = self.query(q, row)?;
                match rows.as_slice() {
                    [] => N,
                    [r] => r[0].clone(),
                    _ => return Err(Error::Type),
                }
            }
            K::ArrayQuery(q) => {
                let rows = self.query(q, row)?;
                Value::Array(rows.into_iter().map(|mut r| r.remove(0)).collect())
            }
            K::ArrayAgg(arg, order) => {
                if all.is_empty() {
                    N
                } else {
                    let mut values = vec![];
                    let mut bytes = 8usize;
                    for r in all {
                        let v = self.eval(arg, r, None, 0, &[], Some(MAX_BYTES - bytes))?;
                        let keys =
                            collect_values(order.iter().map(|o| {
                                self.eval(&o.e, r, None, 0, &[], Some(MAX_BYTES - bytes))
                            }))?;
                        bytes = bytes
                            .checked_add(
                                v.bytes() + 4 + keys.iter().map(Value::bytes).sum::<usize>(),
                            )
                            .ok_or(Error::Limit)?;
                        if bytes > MAX_BYTES {
                            return Err(Error::Limit);
                        }
                        values.push((v, keys));
                    }
                    self.charge(
                        values
                            .len()
                            .saturating_mul(values.len().max(1).ilog2() as usize + 1)
                            .saturating_mul(order.len()),
                    )?;
                    values.sort_by(|l, r| compare_keys(&l.1, &r.1, order));
                    Value::Array(values.into_iter().map(|(v, _)| v).collect())
                }
            }
            K::Expand(_) => expanded.cloned().ok_or(Error::Unsupported)?,
            K::RowNumber(partition, order) => {
                let p = collect_values(partition.iter().map(&mut ev))?;
                let keys = collect_values(order.iter().map(|o| ev(&o.e)))?;
                let mut number = 1;
                for (i, r) in all.iter().enumerate() {
                    self.charge(1)?;
                    let rp = collect_values(
                        partition
                            .iter()
                            .map(|e| self.eval(e, r, None, 0, &[], None)),
                    )?;
                    if rp == p {
                        let rk = collect_values(
                            order.iter().map(|o| self.eval(&o.e, r, None, 0, &[], None)),
                        )?;
                        let cmp = compare_keys(&rk, &keys, order);
                        if cmp.is_lt() || cmp.is_eq() && i < idx {
                            number += 1
                        }
                    }
                }
                I(number)
            }
        };
        if value.bytes() > remaining.unwrap_or(MAX_BYTES) {
            return Err(Error::Limit);
        }
        Ok(value)
    }
    fn call(&mut self, n: &str, v: Vec<Value>) -> R<Value> {
        use Value::{Bool as B, Int as I, Null as N, Text as S};
        self.charge(v.iter().map(Value::bytes).sum::<usize>() / 64 + 1)?;
        if n == "like" {
            self.charge(v[0].bytes() + v[1].bytes().saturating_mul(64))?;
        }
        if n == "coalesce" {
            return Ok(v.into_iter().find(|v| *v != N).unwrap_or(N));
        }
        if n == "nullif" {
            return Ok(if v[0] == v[1] { N } else { v[0].clone() });
        }
        if v.contains(&N) && !(n == "format_type" && v[0] != N) {
            return Ok(N);
        }
        Ok(match n {
            "version" => S(concat!(
                "GigglesDB ",
                env!("CARGO_PKG_VERSION"),
                " (bounded PostgreSQL 14 wire compatibility)"
            )
            .into()),
            "_service_start_epoch" => {
                I(self.startup.filter(|v| *v >= 0).ok_or(Error::Unsupported)?)
            }
            "current_database" | "current_user" | "session_user" => S("giggles".into()),
            "current_schema" => S("public".into()),
            "current_schemas" => {
                let b = boolean(v[0].clone())?.ok_or(Error::Type)?;
                Value::Array(if b {
                    vec![S("pg_catalog".into()), S("public".into())]
                } else {
                    vec![S("public".into())]
                })
            }
            "replace" => {
                let s = text(v[0].clone())?;
                let from = text(v[1].clone())?;
                let to = text(v[2].clone())?;
                let count = if from.is_empty() {
                    s.chars().count() + 1
                } else {
                    s.matches(&from).count()
                };
                if s.len()
                    .checked_add(count.saturating_mul(to.len()))
                    .ok_or(Error::Limit)?
                    > MAX_BYTES
                {
                    return Err(Error::Limit);
                }
                S(s.replace(&from, &to))
            }
            "array_upper" | "array_lower" => {
                if integer(v[1].clone())? != 1 {
                    N
                } else {
                    match &v[0] {
                        Value::Array(a) if !a.is_empty() => I(if n == "array_upper" {
                            a.len() as i64
                        } else {
                            1
                        }),
                        Value::Array(_) => N,
                        _ => return Err(Error::Type),
                    }
                }
            }
            "cardinality" => match &v[0] {
                Value::Array(a) => I(a.len() as i64),
                _ => return Err(Error::Type),
            },
            "length" => I(text(v[0].clone())?.chars().count() as i64),
            "like" => B(like(
                &text(v[0].clone())?,
                &text(v[1].clone())?,
                &text(v[2].clone())?,
            )?),
            "pg_get_userbyid" => {
                if integer(v[0].clone())? == 10 {
                    S("giggles".into())
                } else {
                    N
                }
            }
            "quote_ident" => {
                let name = text(v[0].clone())?;
                // Always quoting is valid identifier SQL and avoids a keyword list.
                S(format!("\"{}\"", name.replace('"', "\"\"")))
            }
            "pg_tablespace_location" | "pg_get_partkeydef" => N,
            "_row" => Value::Record(v),
            "pg_indexam_has_property" => N,
            "pg_get_indexdef" => return Err(Error::Unsupported),
            "chr" => S(char::from_u32(
                u32::try_from(integer(v[0].clone())?).map_err(|_| Error::Type)?,
            )
            .filter(|c| *c != '\0')
            .ok_or(Error::Type)?
            .to_string()),
            "pg_get_expr" => return Err(Error::Unsupported),
            "obj_description" | "col_description" => N,
            "format_type" => {
                let oid = integer(v[0].clone())?;
                S(format_type(oid, &self.catalogs["pg_type"].rows)?)
            }
            "has_table_privilege" | "has_schema_privilege" | "has_type_privilege" => {
                let requested = text(v[1].clone())?;
                let allowed = if n == "has_table_privilege" {
                    "SELECT"
                } else {
                    "USAGE"
                };
                let privileges = requested
                    .split(',')
                    .map(str::trim)
                    .map(str::to_ascii_uppercase)
                    .collect::<Vec<_>>();
                let known = if n == "has_table_privilege" {
                    &[
                        "SELECT",
                        "INSERT",
                        "UPDATE",
                        "DELETE",
                        "TRUNCATE",
                        "REFERENCES",
                        "TRIGGER",
                        "RULE",
                    ][..]
                } else if n == "has_schema_privilege" {
                    &["USAGE", "CREATE"][..]
                } else {
                    &["USAGE"][..]
                };
                if privileges.iter().any(|p| !known.contains(&p.as_str())) {
                    return Err(Error::Unsupported);
                }
                let relation = if n == "has_table_privilege" {
                    "pg_class"
                } else if n == "has_schema_privilege" {
                    "pg_namespace"
                } else {
                    "pg_type"
                };
                B(privileges.iter().any(|p| p == allowed)
                    && self.catalogs[relation]
                        .rows
                        .iter()
                        .any(|r| r[0] == v[0] || r[1] == v[0]))
            }
            "pg_table_is_visible" => B(self.catalogs["pg_class"]
                .rows
                .iter()
                .any(|r| r[0] == v[0] || r[1] == v[0])),
            "pg_type_is_visible" => B(self.catalogs["pg_type"]
                .rows
                .iter()
                .any(|r| r[0] == v[0] || r[1] == v[0])),
            _ => return Err(Error::Unsupported),
        })
    }
}
fn compare_keys(l: &[Value], r: &[Value], order: &[Order]) -> std::cmp::Ordering {
    for ((l, r), o) in l.iter().zip(r).zip(order) {
        let cmp = match (l, r) {
            (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
            (Value::Null, _) => {
                if o.nulls_first {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Greater
                }
            }
            (_, Value::Null) => {
                if o.nulls_first {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }
            }
            _ => {
                if o.asc {
                    l.cmp(r)
                } else {
                    r.cmp(l)
                }
            }
        };
        if !cmp.is_eq() {
            return cmp;
        }
    }
    std::cmp::Ordering::Equal
}
fn format_type(oid: i64, rows: &[Vec<Value>]) -> R<String> {
    let r = rows
        .iter()
        .find(|r| r[0] == Value::Int(oid))
        .ok_or(Error::Unsupported)?;
    if r[12] == Value::Int(750) {
        return Ok(format!("{}[]", format_type(integer(r[10].clone())?, rows)?));
    }
    Ok(match oid {
        16 => "boolean".into(),
        20 => "bigint".into(),
        21 => "smallint".into(),
        23 => "integer".into(),
        18 => "\"char\"".into(),
        _ => {
            let name = text(r[1].clone())?;
            if name
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
                && !name.starts_with(|c: char| c.is_ascii_digit())
            {
                name
            } else {
                format!("\"{}\"", name.replace('"', "\"\""))
            }
        }
    })
}
fn cast(v: Value, t: PgType) -> R<Value> {
    if v == Value::Null {
        return Ok(v);
    }
    use Value::{Bool as B, Int as I, Text as S};
    match t {
        PgType::Text | PgType::Name | PgType::Char => Ok(S(match v {
            S(s) => s,
            I(i) => i.to_string(),
            B(b) => b.to_string(),
            _ => return Err(Error::Type),
        })),
        PgType::Bool => match v {
            B(b) => Ok(B(b)),
            S(s) => match s.as_str() {
                "true" | "t" => Ok(B(true)),
                "false" | "f" => Ok(B(false)),
                _ => Err(Error::Type),
            },
            _ => Err(Error::Type),
        },
        PgType::Int2 | PgType::Int4 | PgType::Int8 | PgType::Oid => {
            let i = match v {
                I(i) => i,
                S(s) => s.parse().map_err(|_| Error::Type)?,
                _ => return Err(Error::Type),
            };
            if match t {
                PgType::Int2 => i16::try_from(i).is_err(),
                PgType::Int4 => i32::try_from(i).is_err(),
                PgType::Oid => u32::try_from(i).is_err(),
                _ => false,
            } {
                return Err(Error::Type);
            }
            Ok(I(i))
        }
        _ => Err(Error::Unsupported),
    }
}
fn binary_value(l: Value, op: &Op, r: Value) -> R<Value> {
    use Value::{Bool as B, Int as I, Null as N, Text as S};
    if l == N || r == N {
        return Ok(N);
    }
    if matches!(op, Op::PGRegexMatch | Op::PGRegexNotMatch) {
        let (l, r) = (text(l)?, text(r)?);
        if r.len() > 1024 {
            return Err(Error::Limit);
        }
        let re = regex::RegexBuilder::new(&r)
            .size_limit(65536)
            .build()
            .map_err(|_| Error::Unsupported)?;
        return Ok(B(re.is_match(&l) ^ (op == &Op::PGRegexNotMatch)));
    }
    if op == &Op::StringConcat {
        let (l, r) = (text(l)?, text(r)?);
        if l.len() + r.len() > MAX_BYTES {
            return Err(Error::Limit);
        }
        return Ok(S(l + &r));
    }
    if matches!(
        op,
        Op::Plus | Op::Minus | Op::Multiply | Op::Divide | Op::Modulo
    ) {
        let (l, r) = (integer(l)?, integer(r)?);
        return Ok(I(match op {
            Op::Plus => l.checked_add(r),
            Op::Minus => l.checked_sub(r),
            Op::Multiply => l.checked_mul(r),
            Op::Divide => l.checked_div(r),
            Op::Modulo => l.checked_rem(r),
            _ => None,
        }
        .ok_or(Error::Type)?));
    }
    // Parameters inferred through typed comparisons may arrive as strings.
    let (l, r) = match (&l, &r) {
        (I(_), S(_)) => (l, cast(r, PgType::Int8)?),
        (S(_), I(_)) => (cast(l, PgType::Int8)?, r),
        _ => (l, r),
    };
    if std::mem::discriminant(&l) != std::mem::discriminant(&r) {
        return Err(Error::Type);
    }
    Ok(B(match op {
        Op::Eq => l == r,
        Op::NotEq => l != r,
        Op::Lt => l < r,
        Op::LtEq => l <= r,
        Op::Gt => l > r,
        Op::GtEq => l >= r,
        _ => return Err(Error::Unsupported),
    }))
}
fn like(s: &str, pattern: &str, escape: &str) -> R<bool> {
    if pattern.len() > 1024 {
        return Err(Error::Limit);
    }
    let mut re = String::from("\\A(?:");
    let mut chars = pattern.chars();
    let esc = escape.chars().next();
    while let Some(c) = chars.next() {
        if Some(c) == esc {
            let c = chars.next().ok_or(Error::Syntax)?;
            re.push_str(&regex::escape(&c.to_string()))
        } else {
            match c {
                '%' => re.push_str("(?s:.*)"),
                '_' => re.push_str("(?s:.)"),
                _ => re.push_str(&regex::escape(&c.to_string())),
            }
        }
    }
    re.push_str(")\\z");
    Ok(regex::RegexBuilder::new(&re)
        .size_limit(65536)
        .build()
        .map_err(|_| Error::Limit)?
        .is_match(s))
}
