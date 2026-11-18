use crate::{catalog, *};
mod flat_union;
mod numeric_literals;
mod join_selection;
mod regtype;
mod viewdef;
pub(crate) mod explain;
mod subscripts;
#[cfg(test)]
mod gui_properties;
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

fn description(rows: &[Vec<Value>], name: &str, args: &[Value]) -> Value {
    use Value::{Int as I, Text as S, Null as N};
    if args.contains(&N) { return N; }
    let (class, subid) = if name == "col_description" { (Some(I(1259)), args[1].clone()) }
        else { (args.get(1).map(|v| match v {
            S(s) => catalog::relation_oid(s).map(|oid| I(i64::from(oid))).unwrap_or(N),
            _ => N,
        }), I(0)) };
    rows.iter().find(|r| r[0] == args[0] && r[2] == subid
        && class.as_ref().is_none_or(|c| r[1] == *c))
        .map_or(N, |r| r[3].clone())
}

#[derive(Debug, Clone)]
pub struct Plan {
    query: Query,
    user_relations: BTreeMap<String, Vec<Field>>,
    parameters: usize,
    parameter_types: Vec<PgType>,
    projection_parameters: bool,
    text_cast_parameters: Vec<bool>,
    view_definitions: bool,
}
impl Plan {
    pub fn refreshes_view_definitions(&self) -> bool {
        self.view_definitions && self.user_relations.is_empty()
    }
    /// Returns false only for a provably relation-free compiled query.
    /// Unknown expression and source forms retain the cached-plan fence.
    pub fn depends_on_relations(&self) -> bool {
        fn independent(e: &E) -> bool {
            match &e.kind {
                K::Literal(_) | K::Param(_) => true,
                K::Call(name, args) if matches!(name.as_str(), "version" | "current_schema") =>
                    args.iter().all(independent),
                _ => false,
            }
        }
        let q = &self.query;
        !self.user_relations.is_empty()
            || !matches!(q.source, Source::Unit | Source::Empty)
            || !independent(&q.filter)
            || !q.projection.iter().all(independent)
            || !q.order.iter().all(|o| independent(&o.e))
            || !q.group.iter().all(independent)
            || q.expand.as_ref().is_some_and(|es| !es.iter().all(independent))
    }
    pub fn user_relations(&self) -> &BTreeMap<String, Vec<Field>> {
        &self.user_relations
    }
    /// Conservative wire widening is safe only when no SELECT projection
    /// (including a derived query) contains a parameter expression.
    pub fn allows_numeric_input_override(&self) -> bool {
        !self.projection_parameters
    }
    /// True only when every occurrence of this parameter is the direct operand
    /// of an explicit text/varchar cast. Its declared input cannot change output types.
    pub fn allows_text_cast_input_override(&self, parameter: usize) -> bool {
        self.text_cast_parameters
            .get(parameter)
            .copied()
            .unwrap_or(false)
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
    namespace: Option<String>,
    field: Field,
    natural_hidden: bool,
    system: bool,
    scope_depth: u8,
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
    IntegerOid(Box<E>, PgType),
    Regclass(Box<E>),
    Regproc(Box<E>),
    Regoper(Box<E>),
    RegisteredDisplay(Box<E>),
    Case(Option<Box<E>>, Vec<(E, E)>, Box<E>),
    Call(String, Vec<E>),
    Array(Vec<E>),
    Index(Box<E>, Box<E>),
    Member(Box<E>, usize),
    Any(Box<E>, Op, Box<E>),
    In(Box<E>, Vec<E>, bool),
    InQuery(Box<E>, Box<Query>, bool),
    Subquery(Box<Query>),
    Exists(Box<Query>, bool),
    Distinct(Box<E>, Box<E>, bool),
    ArrayQuery(Box<Query>),
    ArrayAgg(Box<E>, Vec<Order>),
    Count(Option<Box<E>>),
    RowNumber(Vec<E>, Vec<Order>),
    Expand(Box<E>),
    Unnest(Box<E>),
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
    Cte(std::sync::Arc<Query>),
    Union(Box<Query>, Box<Query>, bool),
    FlatUnion(Vec<Query>),
    Series(E, E),
    Unnest(E, bool),
    ZipUnnest(Vec<E>, bool),
    Scalar(E),
    Options(E),
    Filtered(Box<Source>, E),
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
    distinct_on: Vec<usize>,
    expand: Option<Vec<E>>,
    source_width: usize,
    outer_width: usize,
    group: Vec<E>,
    aggregate: bool,
}
struct Compiler {
    user_relations: BTreeMap<String, Vec<Field>>,
    used_relations: BTreeMap<String, Vec<Field>>,
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
pub fn catalog_identifier(name: &str) -> bool {
    matches!(
        name,
        "pg_catalog"
            | "information_schema"
            | "pg_options_to_table"
            | "pg_get_sequence_data"
            | "version"
            | "pg_postmaster_start_time"
            | "pg_backend_pid"
            | "pg_is_in_recovery"
            | "txid_current"
            | "current_database"
            | "current_schema"
            | "current_user"
            | "session_user"
            | "user"
            | "current_role"
            | "current_catalog"
            | "current_schemas"
    ) || unqualified_catalog(name)
}
fn unqualified_catalog(name: &str) -> bool {
    catalog::relation_namespace(name) == "pg_catalog" && catalog::schema(name).is_some()
}
fn regclass_oid(value: &str, snapshot: &CatalogSnapshot) -> R<u32> {
    if let Ok(oid) = value.parse::<u32>() {
        return Ok(oid);
    }
    if value.len() > MAX_SQL {
        return Err(Error::Unsupported);
    }
    let mut parser = Parser::new(&PostgreSqlDialect {})
        .with_recursion_limit(64)
        .try_with_sql(value)
        .map_err(|_| Error::Unsupported)?;
    let name = parser
        .parse_object_name(false)
        .map_err(|_| Error::Unsupported)?;
    if parser.peek_token().token != Token::EOF
        || name
            .0
            .iter()
            .any(|ident| !matches!(ident.quote_style, None | Some('"')))
    {
        return Err(Error::Unsupported);
    }
    let parts = object(&name);
    let (schema, name) = match parts.as_slice() {
        [name] => (None, name),
        [schema, name] => (Some(schema.as_str()), name),
        _ => return Err(Error::Unsupported),
    };
    let mut path = snapshot.search_path.clone();
    if !path.iter().any(|s| s == "pg_catalog") { path.insert(0, "pg_catalog".into()); }
    for namespace in schema.map(|s| vec![s.to_owned()]).unwrap_or(path) {
        if namespace == catalog::relation_namespace(name)
            && let Some(oid) = catalog::relation_oid(name) { return Ok(oid); }
        if let Some(table) = snapshot.tables.iter().find(|t|
            t.name == *name && snapshot.table_schema(t.id).0 == namespace) {
            return catalog::table_oid(table.id);
        }
    }
    Err(Error::Unsupported)
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
    // Fold safe int4 literal products without widening their PostgreSQL type.
    if op == Op::Multiply && l.ty == PgType::Int4 && r.ty == PgType::Int4
        && let (K::Literal(Value::Int(a)), K::Literal(Value::Int(b))) = (&l.kind, &r.kind)
        && let Some(n) = a.checked_mul(*b).filter(|n| i32::try_from(*n).is_ok()) {
        return literal(Value::Int(n), PgType::Int4);
    }
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
    plan_inner(sql, false, None)
}

/// Classify a gateway query, including table-free catalog introspection.
/// Other scalars must pass the core binder and gateway authority.
pub fn plan_gateway(sql: &str) -> R<Option<Plan>> {
    plan_inner(sql, true, None)
}

/// Bind mixed read-only queries against caller-authorized application schemas.
pub fn plan_gateway_with_relations(
    sql: &str,
    relations: &BTreeMap<String, Vec<Field>>,
) -> R<Option<Plan>> {
    plan_inner(sql, true, Some(relations))
}
struct Scan {
    view_definitions: bool,
    catalog: bool,
    explicit_catalog: bool,
    core_scalar: bool,
    introspection: bool,
    introspection_only: bool,
    has_relations: bool,
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
            for cte in &with.cte_tables {
                self.ctes.insert(ident(&cte.alias.name));
            }
        }
        ControlFlow::Continue(())
    }
    fn pre_visit_table_factor(&mut self, factor: &a::TableFactor) -> ControlFlow<Error> {
        self.has_relations |= !matches!(factor, a::TableFactor::Derived { .. } | a::TableFactor::NestedJoin { .. });
        ControlFlow::Continue(())
    }
    fn pre_visit_relation(&mut self, n: &a::ObjectName) -> ControlFlow<Error> {
        self.has_relations = true;
        let p = object(n);
        let is_catalog = match p.as_slice() {
            [n] => {
                unqualified_catalog(n)
                    || matches!(n.as_str(), "pg_options_to_table" | "pg_get_sequence_data")
            }
            [s, _] => s == "pg_catalog" || s == "information_schema",
            _ => false,
        };
        if is_catalog {
            self.catalog = true;
            self.explicit_catalog = true;
        } else if !matches!(p.as_slice(),[n] if n=="generate_series" || n=="generate_subscripts" || n=="unnest" || n=="aclexplode" || n=="pg_available_extension_versions" || n=="pg_options_to_table" || self.ctes.contains(n))
        {
            self.user = true
        }
        ControlFlow::Continue(())
    }
    fn pre_visit_expr(&mut self, e: &A) -> ControlFlow<Error> {
        // Except for the closed introspection route below, the core binder
        // owns nonliteral scalars without explicit catalog context.
        self.core_scalar |= !matches!(e, A::Value(_) | A::Nested(_));
        // Core's lexer owns prefixed strings, including PostgreSQL escape errors.
        self.core_scalar |= matches!(e, A::Value(a::Value::DollarQuotedString(_)
            | a::Value::EscapedStringLiteral(_) | a::Value::UnicodeStringLiteral(_)));
        // This additional gateway route is deliberately closed: nested scalar
        // calls and base relations must keep their existing core routing.
        // Special SQL function forms (SUBSTRING, TRIM, etc.) are excluded too.
        self.introspection_only &= matches!(e,
            A::Value(_) | A::Nested(_) | A::Identifier(_) | A::CompoundIdentifier(_)
            | A::Function(_) | A::Cast { .. } | A::UnaryOp { .. } | A::BinaryOp { .. }
            | A::IsNull(_) | A::IsNotNull(_));
        if let A::Function(f) = e {
            self.view_definitions |= function(&f.name).is_ok_and(|n| n == "pg_get_viewdef");
            let introspection = function(&f.name).is_ok_and(|n| matches!(n.as_str(),
                "pg_get_indexdef" | "pg_get_constraintdef" | "pg_get_expr"
                | "pg_get_userbyid" | "format_type" | "obj_description" | "pg_collation_actual_version"
                | "col_description" | "shobj_description" | "pg_get_viewdef"
                | "has_table_privilege" | "has_column_privilege"));
            self.introspection |= introspection;
            self.introspection_only &= introspection;
        }
        self.nodes += 1;
        self.depth += 1;
        if self.nodes > 4096 || self.depth > 48 {
            return ControlFlow::Break(Error::Limit);
        }
        if let A::Identifier(n) = e
            && n.quote_style.is_none()
            && matches!(
                ident(n).as_str(),
                "user" | "current_role" | "current_catalog"
            )
        {
            self.catalog = true;
            self.explicit_catalog = true;
        }
        if let A::Function(f) = e
            && let Ok(n) = function(&f.name)
            && matches!(
                n.as_str(),
                "version"
                    | "pg_postmaster_start_time"
                    | "pg_backend_pid"
                    | "pg_is_in_recovery"
                    | "txid_current"
                    | "current_database"
                    | "current_schema"
                    | "current_user"
                    | "session_user"
                    | "user"
                    | "current_role"
                    | "current_catalog"
                    | "current_schemas"
            )
        {
            self.catalog = true;
            self.explicit_catalog = true;
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
impl Scan {
    fn new() -> Self {
        Self {
            view_definitions: false,
            catalog: false,
            explicit_catalog: false,
            core_scalar: false,
            introspection: false,
            introspection_only: true,
            has_relations: false,
            user: false,
            nodes: 0,
            depth: 0,
            mutation: false,
            ctes: Default::default(),
        }
    }
}
// Count all occurrences, including repeated uses and nested queries, so an
// uncast use of the same $n prevents widening that parameter everywhere.
struct TextCastParameters(BTreeMap<usize, usize>, BTreeMap<usize, usize>);
impl TextCastParameters {
    fn flags(&self, parameters: usize) -> Vec<bool> {
        (1..=parameters)
            .map(|i| {
                self.0
                    .get(&i)
                    .is_some_and(|count| self.1.get(&i) == Some(count))
            })
            .collect()
    }
}

impl Visitor for TextCastParameters {
    type Break = ();
    fn pre_visit_expr(&mut self, e: &A) -> ControlFlow<()> {
        fn parameter(e: &A) -> Option<usize> {
            if let A::Value(a::Value::Placeholder(p)) = e {
                p.strip_prefix('$')?.parse().ok()
            } else {
                None
            }
        }
        if let Some(i) = parameter(e) {
            *self.0.entry(i).or_default() += 1;
        }
        if let A::Cast {
            expr,
            data_type,
            format: None,
            kind,
        } = e
            && matches!(kind, a::CastKind::Cast | a::CastKind::DoubleColon)
        {
            let name = data_type.to_string().to_ascii_lowercase();
            let name = name.strip_prefix("pg_catalog.").unwrap_or(&name);
            let mut operand = expr.as_ref();
            while let A::Nested(inner) = operand {
                operand = inner;
            }
            if matches!(name, "text" | "varchar" | "character varying")
                && let Some(i) = parameter(operand)
            {
                *self.1.entry(i).or_default() += 1;
            }
        }
        ControlFlow::Continue(())
    }
}
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

fn constructor(token: &Token) -> bool {
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
    !leaf
}

// A flat SELECT list has sibling expressions, not a left-deep AST. Keep the
// existing constructor bound on each item and on the remaining query, rather
// than charging unrelated output expressions against the same depth guard.
fn bounded_projection(tokens: &[Token]) -> bool {
    let mut depth = 0i32;
    let mut count = 0;
    let mut projections = BTreeMap::new();
    let mut not_run = 0;
    for token in tokens.iter().filter(|t| !matches!(t, Token::Whitespace(_) | Token::EOF)) {
        if matches!(token, Token::Word(w) if matches!(w.keyword, Keyword::UNION | Keyword::INTERSECT | Keyword::EXCEPT)) { return false; }
        if matches!(token, Token::Word(w) if w.keyword == Keyword::SELECT) {
            projections.insert(depth, count);
        }
        if *token == Token::Comma && let Some(base) = projections.get(&depth) { count = *base; }
        if matches!(token, Token::Word(w) if w.keyword == Keyword::FROM) {
            projections.remove(&depth);
        }
        match token {
            Token::LParen | Token::LBracket => depth += 1,
            Token::RParen | Token::RBracket => { projections.remove(&depth); depth -= 1; }
            _ => {}
        }
        if matches!(token, Token::Word(w) if w.keyword == Keyword::NOT) { not_run += 1; } else { not_run = 0; }
        if depth < 0 || not_run > 16 { return false; }
        count += usize::from(constructor(token));
        if count > 256 { return false; }
    }
    depth == 0
}

// psql qualifies its catalog regex patterns with pg_catalog.default. Preserve
// that existing catalog path while core continues to own scalar COLLATE.
fn catalog_default_collation(tokens: &[Token]) -> bool {
    let flat = tokens.iter().filter(|t|!matches!(t,Token::Whitespace(_))).collect::<Vec<_>>();
    let word = |t: &&Token, name: &str| matches!(t, Token::Word(w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(name));
    let catalog_source = flat.windows(4).any(|s| {
        (word(&s[0],"from") || word(&s[0],"join")) && word(&s[1],"pg_catalog")
            && *s[2]==Token::Period && matches!(s[3],Token::Word(w) if catalog::relation_oid(&w.value.to_ascii_lowercase()).is_some())
    });
    catalog_source && flat.iter().enumerate().all(|(i,t)| {
        !word(t,"pg_collation_for") && (!word(t,"collate") || flat.get(i+1..i+4).is_some_and(|s| {
            word(&s[0],"pg_catalog") && *s[1]==Token::Period && word(&s[2],"default")
        }))
    })
}

fn plan_inner(
    sql: &str,
    gateway: bool,
    relations: Option<&BTreeMap<String, Vec<Field>>>,
) -> R<Option<Plan>> {
    if sql.len() > MAX_SQL {
        return Err(Error::Limit);
    }
    let normalized = numeric_literals::normalize(sql)?;
    let sql = normalized.as_ref();
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
        if constructor(token) {
            constructors += 1;
        }
        if count > 8192 {
            return Err(Error::Limit);
        }
    }
    if constructors > 256 && !bounded_projection(&tokens) {
        return flat_union::plan(tokens, gateway);
    }
    // Core carries collation derivation through scalar binding and execution.
    if gateway && tokens.iter().any(|t| matches!(t, Token::Word(w) if w.quote_style.is_none() && (w.value.eq_ignore_ascii_case("collate") || w.value.eq_ignore_ascii_case("pg_collation_for"))))
        && !catalog_default_collation(&tokens) {
        return Ok(None);
    }
    // Core owns pg_typeof expression typing, including nested regtype results.
    if tokens.windows(2).any(|pair| matches!(&pair[0], Token::Word(w) if w.value.eq_ignore_ascii_case("pg_typeof")) && pair[1] == Token::LParen) {
        return Ok(None);
    }
    // sqlparser 0.53 cannot parse OPERATOR(pg_catalog.=) ANY (...).
    // Normalize only this built-in qualified operator, on tokens so literals
    // and comments cannot be rewritten.
    let mut tokens = tokens
        .into_iter()
        .filter(|t| !matches!(t, Token::Whitespace(_)))
        .collect::<Vec<_>>();
    let mut i = 0;
    // sqlparser parses explicit LATERAL calls differently from ordinary table
    // functions, and that path cannot consume WITH ORDINALITY. These supported
    // functions already have implicit-lateral semantics. Normalize only calls
    // in a table position; identifier quoting is preserved, and literals and
    // comments cannot be mistaken for keywords.
    fn token_name(token: Option<&Token>) -> Option<String> {
        let Token::Word(word) = token? else { return None };
        Some(if word.quote_style.is_none() {
            word.value.to_ascii_lowercase()
        } else {
            word.value.clone()
        })
    }
    let mut table_context = vec![false];
    while i < tokens.len() {
        match &tokens[i] {
            Token::LParen => table_context.push(false),
            Token::RParen if table_context.len() > 1 => { table_context.pop(); }
            Token::Word(w) if w.keyword == Keyword::FROM => *table_context.last_mut().unwrap() = true,
            Token::Word(w) if matches!(w.keyword, Keyword::SELECT | Keyword::WHERE | Keyword::GROUP
                | Keyword::HAVING | Keyword::ORDER | Keyword::LIMIT | Keyword::UNION | Keyword::EXCEPT
                | Keyword::INTERSECT | Keyword::WINDOW | Keyword::OFFSET | Keyword::FETCH) =>
                *table_context.last_mut().unwrap() = false,
            _ => {}
        }
        let table_position = i > 0 && ((tokens[i - 1] == Token::Comma && *table_context.last().unwrap())
            || matches!(&tokens[i - 1], Token::Word(w)
                if matches!(w.keyword, Keyword::FROM | Keyword::JOIN)));
        let mut name = i + 1;
        if token_name(tokens.get(name)).as_deref() == Some("pg_catalog")
            && tokens.get(name + 1) == Some(&Token::Period)
        {
            name += 2;
        }
        if table_position
            && matches!(&tokens[i], Token::Word(w) if w.keyword == Keyword::LATERAL)
            && matches!(token_name(tokens.get(name)).as_deref(),
                Some("unnest" | "generate_series" | "generate_subscripts" | "aclexplode"))
            && tokens.get(name + 1) == Some(&Token::LParen)
        {
            tokens.remove(i);
        }
        i += 1;
    }
    let mut i = 0;
    while i + 5 < tokens.len() {
        if matches!(&tokens[i], Token::Word(w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case("operator"))
            && tokens[i + 1] == Token::LParen
            && matches!(&tokens[i + 2], Token::Word(w) if w.value == "pg_catalog")
            && tokens[i + 3] == Token::Period
            && tokens[i + 4] == Token::Eq
            && tokens[i + 5] == Token::RParen
        {
            tokens.splice(i..i + 6, [Token::Eq]);
        }
        i += 1;
    }
    let statements = Parser::new(&PostgreSqlDialect {})
        .with_recursion_limit(64)
        .with_tokens(tokens)
        .parse_statements()
        .map_err(|_| Error::Syntax)?;
    if statements.is_empty() {
        return Ok(None);
    }
    let mut scan = Scan::new();
    if let ControlFlow::Break(e) = statements.visit(&mut scan) {
        return Err(e);
    }
    struct CoreConcat(bool);
    impl a::Visitor for CoreConcat {
        type Break = ();
        fn pre_visit_expr(&mut self, expr: &A) -> ControlFlow<()> {
            fn typed(expr: &A) -> bool {
                match expr {
                    A::Nested(expr) => typed(expr),
                    A::Cast { data_type, .. } => {
                        let name = data_type.to_string().to_ascii_lowercase();
                        name.ends_with(']') || matches!(name.as_str(), "jsonb" | "pg_catalog.jsonb")
                    }
                    _ => false,
                }
            }
            if let A::BinaryOp { left, op: Op::StringConcat, right } = expr {
                self.0 |= typed(left) || typed(right);
            }
            ControlFlow::Continue(())
        }
    }
    let mut core_concat = CoreConcat(false);
    let _ = statements.visit(&mut core_concat);
    if core_concat.0 && !scan.explicit_catalog {
        return Ok(None);
    }
    let introspection = scan.introspection && scan.introspection_only && !scan.has_relations;
    scan.catalog |= introspection;
    if !scan.catalog {
        return Ok(None);
    }
    if scan.mutation || statements.len() != 1 {
        return Err(Error::Mutation);
    }
    if scan.user && relations.is_none() {
        return Err(Error::MixedRelations);
    }
    // Without explicit catalog context the core binder owns scalar semantics.
    if gateway && scan.core_scalar && !scan.explicit_catalog && !introspection {
        return Ok(None);
    }
    let a::Statement::Query(q) = &statements[0] else {
        return Err(Error::Mutation);
    };
    let mut projection_parameters = ProjectionParameters(false);
    let _ = q.visit(&mut projection_parameters);
    let mut c = Compiler {
        user_relations: relations.cloned().unwrap_or_default(),
        used_relations: BTreeMap::new(),
        parameters: 0,
        qcount: 0,
        sources: 0,
        ctes: BTreeMap::new(),
    };
    let query = c.query(q, &vec![])?;
    let mut plan = finish_plan(query, c.parameters, projection_parameters.0)?;
    if let Some(plan) = &mut plan {
        plan.user_relations = c.used_relations;
        plan.view_definitions = scan.view_definitions;
        let mut uses = TextCastParameters(BTreeMap::new(), BTreeMap::new());
        let _ = q.visit(&mut uses);
        plan.text_cast_parameters = uses.flags(plan.parameters);
    }
    Ok(plan)
}
fn finish_plan(
    mut query: Query,
    parameters: usize,
    projection_parameters: bool,
) -> R<Option<Plan>> {
    if query.fields.iter().any(|f| f.data_type == PgType::Record) {
        return Err(Error::Unsupported);
    }
    let mut types = vec![None; parameters];
    collect_parameters(&query, &mut types)?;
    let parameter_types = types
        .into_iter()
        .map(|t| t.unwrap_or(PgType::Text))
        .collect::<Vec<_>>();
    join_selection::place(&mut query, &parameter_types);
    Ok(Some(Plan {
        user_relations: BTreeMap::new(),
        projection_parameters,
        query,
        parameters,
        parameter_types,
        text_cast_parameters: vec![false; parameters],
        view_definitions: false,
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
    let mut catalogs = catalog::build(snapshot)?;
    let mut input_bytes = 0;
    for (name, fields) in &plan.user_relations {
        let relation = snapshot
            .user_relations
            .get(name)
            .ok_or(Error::InvalidSnapshot)?;
        if relation.fields != *fields || catalogs.contains_key(name) {
            return Err(Error::InvalidSnapshot);
        }
        for (i, row) in relation.rows.iter().enumerate() {
            if row.len() != fields.len() {
                return Err(Error::InvalidSnapshot);
            }
            validate_values(row)?;
            admit(&relation.rows[..i], row, &mut input_bytes)?;
        }
        catalogs.insert(
            name.clone(),
            catalog::Relation {
                fields: fields.clone(),
                rows: relation.rows.clone(),
            },
        );
    }
    let mut rt = Runtime {
        catalogs: &catalogs,
        snapshot,
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
fn definitely_empty(q: &Query) -> bool {
    fn source(s: &Source) -> bool {
        match s {
            Source::Empty => true,
            // No stored views/rules exist in this catalog model.
            Source::Catalog(n) => matches!(n.as_str(), "pg_rewrite" | "pg_auth_members"),
            Source::Derived(q) | Source::Cte(q) => definitely_empty(q),
            Source::Filtered(s, _) => source(s),
            Source::Join(l, r, _, left, _, _) => source(l) || !*left && source(r),
            Source::Union(l, r, _) => definitely_empty(l) && definitely_empty(r),
            _ => false,
        }
    }
    q.limit == 0 || !q.aggregate && source(&q.source)
}
fn reorderable_catalog_filter(e: &A) -> bool {
    match e {
        A::Identifier(_) | A::CompoundIdentifier(_) => true,
        A::Value(a::Value::Number(..) | a::Value::SingleQuotedString(_) | a::Value::Boolean(_) | a::Value::Null) => true,
        A::Nested(e) => reorderable_catalog_filter(e),
        A::BinaryOp { left, op: Op::And | Op::Or | Op::Eq | Op::NotEq, right } =>
            reorderable_catalog_filter(left) && reorderable_catalog_filter(right),
        A::Subscript { expr, subscript } => matches!((expr.as_ref(), subscript.as_ref()),
            (A::CompoundIdentifier(name), a::Subscript::Index { index })
                if name.last().is_some_and(|n| matches!(ident(n).as_str(), "conkey" | "confkey"))
                    && matches!(index, A::Identifier(_) | A::CompoundIdentifier(_))),
        _ => false,
    }
}
impl Compiler {
    fn query(&mut self, q: &a::Query, outer: &Schema) -> R<Query> {
        let previous = self.ctes.clone();
        let result = self.query_scoped(q, outer);
        self.ctes = previous;
        result
    }
    fn query_scoped(&mut self, q: &a::Query, outer: &Schema) -> R<Query> {
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
            for cte in &with.cte_tables {
                if !cte.alias.columns.is_empty() || cte.from.is_some() || cte.materialized.is_some()
                {
                    return Err(Error::Unsupported);
                }
                let name = ident(&cte.alias.name);
                if self.ctes.contains_key(&name) || unqualified_catalog(&name) {
                    return Err(Error::Unsupported);
                }
                let compiled = if with.recursive {
                    // Only eliminate recursive steps proven empty by absent catalog
                    // features (views/rules and role memberships).
                    // A potentially productive recursion remains unsupported.
                    let a::SetExpr::SetOperation {
                        op: a::SetOperator::Union,
                        set_quantifier:
                            a::SetQuantifier::None | a::SetQuantifier::All | a::SetQuantifier::Distinct,
                        left,
                        right,
                    } = cte.query.body.as_ref()
                    else {
                        return Err(Error::Unsupported);
                    };
                    if cte.query.with.is_some()
                        || cte.query.order_by.is_some()
                        || cte.query.limit.is_some()
                    {
                        return Err(Error::Unsupported);
                    }
                    let mut branch = cte.query.clone();
                    branch.body = left.clone();
                    let mut seed = self.query(&branch, outer)?;
                    if definitely_empty(&seed) {
                        seed.source = Source::Empty;
                    }
                    self.ctes
                        .insert(name.clone(), std::sync::Arc::new(seed.clone()));
                    branch.body = right.clone();
                    let step = self.query(&branch, outer)?;
                    if !definitely_empty(&step) {
                        return Err(Error::Unsupported);
                    }
                    if seed.fields.iter().map(|f| f.data_type).collect::<Vec<_>>()
                        != step.fields.iter().map(|f| f.data_type).collect::<Vec<_>>()
                    {
                        return Err(Error::Type);
                    }
                    seed
                } else {
                    self.query(&cte.query, &vec![])?
                };
                self.ctes.insert(name, std::sync::Arc::new(compiled));
            }
        }
        self.query_body(&q.body, q.order_by.as_ref(), q.limit.as_ref(), outer)
    }
    fn query_body(
        &mut self,
        body: &a::SetExpr,
        order_by: Option<&a::OrderBy>,
        limit: Option<&A>,
        outer: &Schema,
    ) -> R<Query> {
        if let a::SetExpr::SetOperation {
            op: a::SetOperator::Union,
            set_quantifier,
            left,
            right,
        } = body
        {
            let distinct = match set_quantifier {
                a::SetQuantifier::All => false,
                a::SetQuantifier::Distinct | a::SetQuantifier::None => true,
                _ => return Err(Error::Unsupported),
            };
            // Borrow each subtree: cloning the remaining left-deep chain at
            // every UNION node would use quadratic AST memory.
            let mut l = self.query_body(left, None, None, outer)?;
            let mut r = self.query_body(right, None, None, outer)?;
            if l.fields.len() != r.fields.len() {
                return Err(Error::Type);
            }
            // Resolve each binary UNION before its parent: an all-unknown
            // inner UNION becomes text, just as in PostgreSQL.
            for i in 0..l.fields.len() {
                let ty = union_type(&l.projection[i], &r.projection[i])?;
                for arm in [&mut l, &mut r] {
                    union_coerce(&mut arm.projection[i], ty)?;
                    arm.fields[i].data_type = ty;
                }
            }
            let fields = l.fields.clone();
            let projection = fields
                .iter()
                .enumerate()
                .map(|(i, f)| column(i, f.data_type))
                .collect();
            let mut order = vec![];
            if let Some(o) = order_by {
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
            let limit = match limit {
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
                outer_width: outer.len(),
                fields,
                order,
                limit,
                distinct: false,
                distinct_on: vec![],
                expand: None,
                group: vec![],
                aggregate: false,
            });
        }
        if let a::SetExpr::Values(values) = body {
            // A single VALUES row has exactly SELECT-without-FROM semantics.
            // Retain the enclosing ORDER/LIMIT and the ordinary expression binder.
            if values.rows.len() != 1 || values.rows[0].is_empty() {
                return Err(Error::Unsupported);
            }
            let mut statements =
                Parser::parse_sql(&PostgreSqlDialect {}, "SELECT 1").map_err(|_| Error::Syntax)?;
            let a::Statement::Query(template) = statements.remove(0) else {
                unreachable!()
            };
            let a::SetExpr::Select(mut select) = *template.body else {
                unreachable!()
            };
            select.projection = values.rows[0]
                .iter()
                .cloned()
                .map(a::SelectItem::UnnamedExpr)
                .collect();
            return self.query_body(&a::SetExpr::Select(select), order_by, limit, outer);
        }
        // Count leaf queries, including nested subqueries, against the same
        // 32-query budget. UNION operators do not consume a query: 32 leaves
        // (31 UNION nodes) fit, leaving 13 leaves beyond DataGrip's 19.
        self.qcount += 1;
        if self.qcount > 32 {
            return Err(Error::Limit);
        }
        let a::SetExpr::Select(s) = body else {
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
        let has_natural = s.from.iter().any(|f| {
            f.joins.iter().any(|j| {
                matches!(
                    &j.join_operator,
                    a::JoinOperator::Inner(a::JoinConstraint::Natural)
                        | a::JoinOperator::LeftOuter(a::JoinConstraint::Natural)
                        | a::JoinOperator::RightOuter(a::JoinConstraint::Natural)
                        | a::JoinOperator::FullOuter(a::JoinConstraint::Natural)
                )
            })
        });
        if has_natural && s.from.len() != 1 {
            return Err(Error::Unsupported);
        }
        let mut source = Source::Unit;
        let mut schema = vec![];
        let mut logical_columns: Vec<usize> = vec![];
        let mut inputs: Vec<_> = s.from.iter().collect();
        // Constraint catalogs are small and connect both sides of FK inventory.
        // Start there instead of materializing two independent attribute lists.
        // Restrict reordering to independent comma inputs and explicit outputs;
        // wildcard order and lateral/derived scopes retain their original path.
        if s.selection.as_ref().is_some_and(reorderable_catalog_filter)
            && s.projection.iter().all(|p| matches!(p,
            a::SelectItem::UnnamedExpr(_) | a::SelectItem::ExprWithAlias { .. }))
            && inputs.iter().all(|f| f.joins.is_empty() && matches!(&f.relation,
                a::TableFactor::Table { args, name, .. }
                    if { let p = object(name);
                        (p.len() == 1 || p.len() == 2 && p[0] == "pg_catalog")
                            && p.last().is_some_and(|n| if args.is_none() { unqualified_catalog(n) } else { n == "generate_series" }) } ))
        {
            inputs.sort_by_key(|f| !matches!(&f.relation,
                a::TableFactor::Table { name, args: None, .. }
                    if object(name).last().is_some_and(|n| n == "pg_constraint")));
        }
        for from in inputs {
            let operand_start = schema.len();
            let series = matches!(&from.relation, a::TableFactor::Table { name, args: Some(_), .. }
                if object(name).last().is_some_and(|n| n == "generate_series"));
            if series {
                reject_local_series_reference(&from.relation, &schema)?;
            }
            let mut function_outer = schema.clone();
            append_outer(&mut function_outer, outer)?;
            let table_outer = if matches!(&from.relation, a::TableFactor::UNNEST { .. })
                || matches!(&from.relation, a::TableFactor::Table { name, args: Some(_), .. }
                    if object(name).last().is_some_and(|n| matches!(n.as_str(),
                        "pg_get_sequence_data" | "unnest" | "generate_subscripts" | "aclexplode")))
                || matches!(&from.relation, a::TableFactor::Function { name, .. }
                    if object(name).last().is_some_and(|n| n == "aclexplode"))
            {
                &function_outer
            } else {
                outer
            };
            let (rhs, rs) = self.table(&from.relation, table_outer).map_err(|error| {
                if series && operand_start != 0 && error == Error::UnknownColumn {
                    Error::Unsupported
                } else {
                    error
                }
            })?;
            if schema.is_empty() {
                source = rhs;
                schema = rs;
            } else {
                let width = rs.len();
                let lateral = matches!(rhs, Source::Unnest(..) | Source::ZipUnnest(..));
                source = Source::Join(
                    Box::new(source),
                    Box::new(rhs),
                    truth(),
                    false,
                    width,
                    lateral,
                );
                schema.extend(rs)
            }
            logical_columns.extend((operand_start..schema.len()).filter(|&i| !schema[i].system));
            if has_natural
                && matches!(
                    source,
                    Source::Unnest(..) | Source::ZipUnnest(..) | Source::Options(_) | Source::Scalar(_)
                )
            {
                return Err(Error::Unsupported);
            }
            for j in &from.joins {
                let mut lateral_schema = schema.clone();
                append_outer(&mut lateral_schema, outer)?;
                let series = matches!(&j.relation, a::TableFactor::Table { name, args: Some(_), .. }
                    if object(name).last().is_some_and(|n| n == "generate_series"));
                if series {
                    reject_local_series_reference(&j.relation, &schema)?;
                }
                // Series supports enclosing-query correlation. Same-FROM
                // correlation remains unsupported until Series is a lateral
                // operator with the corresponding work/materialization rules.
                let (rhs, rs) = self
                    .table(&j.relation, if series || matches!(j.relation, a::TableFactor::Derived { .. }) { outer } else { &lateral_schema })
                    .map_err(|error| {
                        if series && error == Error::UnknownColumn {
                            Error::Unsupported
                        } else {
                            error
                        }
                    })?;
                let lateral = matches!(
                    rhs,
                    Source::Unnest(..) | Source::ZipUnnest(..) | Source::Options(_) | Source::Scalar(_)
                );
                if has_natural && lateral {
                    return Err(Error::Unsupported);
                }
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
                    a::JoinConstraint::On(e) => {
                        // Explicit JOIN binds more tightly than comma FROM items.
                        // Do not let the flattened physical source widen ON scope.
                        let mut on_schema = schema.clone();
                        for col in &mut on_schema[..operand_start] {
                            // Keep physical column offsets while removing earlier
                            // comma operands from name resolution, including in
                            // correlated subqueries. Nested aliases still shadow.
                            col.qualifier.clear();
                            col.field.name.clear();
                            col.natural_hidden = true;
                        }
                        append_outer(&mut on_schema, outer)?;
                        self.expr(e, &on_schema).map_err(|error| {
                            if operand_start != 0 && error == Error::UnknownColumn {
                                Error::Unsupported
                            } else {
                                error
                            }
                        })?
                    }
                    a::JoinConstraint::None => truth(),
                    a::JoinConstraint::Natural if !left => {
                        // Logical output order is independent of the physical join row.
                        let right: Vec<_> = (old_width..schema.len())
                            .filter(|&i| !schema[i].system)
                            .collect();
                        for input in [&logical_columns, &right] {
                            let mut names = std::collections::BTreeSet::new();
                            for &i in input {
                                if !names.insert(&schema[i].field.name) {
                                    return Err(Error::Unsupported);
                                }
                            }
                        }
                        let mut common = vec![];
                        let mut right_common = vec![];
                        let mut on = truth();
                        for &l in &logical_columns {
                            if let Some(&r) = right
                                .iter()
                                .find(|&&r| schema[l].field.name == schema[r].field.name)
                            {
                                if schema[l].field.data_type != schema[r].field.data_type {
                                    return Err(Error::Unsupported);
                                }
                                let mut eq = binary(
                                    column(l, schema[l].field.data_type),
                                    Op::Eq,
                                    column(r, schema[r].field.data_type),
                                );
                                check_expr(&mut eq)?;
                                on = binary(on, Op::And, eq);
                                common.push(l);
                                right_common.push(r);
                                schema[r].natural_hidden = true;
                            }
                        }
                        common.extend(
                            logical_columns
                                .iter()
                                .copied()
                                .filter(|i| !common.contains(i))
                                .collect::<Vec<_>>(),
                        );
                        common.extend(right.into_iter().filter(|i| !right_common.contains(i)));
                        logical_columns = common;
                        on
                    }
                    a::JoinConstraint::Using(cols) => {
                        let mut on = truth();
                        for n in cols {
                            let name = ident(n);
                            let l = lookup(
                                &schema[operand_start..old_width],
                                std::slice::from_ref(&name),
                            )? + operand_start;
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
                if !matches!(constraint, a::JoinConstraint::Natural) {
                    logical_columns
                        .extend((old_width..schema.len()).filter(|&i| !schema[i].system));
                }
                expect(&mut on, PgType::Bool)?;
                no_set_or_window(&on)?;
                source = Source::Join(Box::new(source), Box::new(rhs), on, left, width, lateral);
            }
        }
        if schema.len() > 256 {
            return Err(Error::Limit);
        }
        let source_width = schema.len();
        append_outer(&mut schema, outer)?;
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
            .map(|e| self.expr(e, &schema).or_else(|error| {
                if error != Error::UnknownColumn { return Err(error); }
                let A::Identifier(name) = e else { return Err(error); };
                let aliases = s.projection.iter().filter_map(|p| match p {
                    a::SelectItem::ExprWithAlias { expr, alias } if ident(alias) == ident(name) => Some(expr),
                    _ => None,
                }).collect::<Vec<_>>();
                match aliases.as_slice() {
                    [expr] => self.expr(expr, &schema),
                    [] => Err(error),
                    _ => Err(Error::AmbiguousColumn),
                }
            }))
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
                    if has_expand(&e2)
                        && !matches!(&e2.kind, K::Expand(_) | K::Unnest(_) | K::Member(_, _))
                    {
                        return Err(Error::Unsupported);
                    }
                    find_expand(&e2, &mut expand)?;
                    let name = match item {
                        a::SelectItem::ExprWithAlias { alias, .. } => ident(alias),
                        _ => projection_name(e)?,
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
                    for &i in &logical_columns {
                        let c = &schema[i];
                        fields.push(c.field.clone());
                        projection.push(column(i, c.field.data_type))
                    }
                }
                a::SelectItem::QualifiedWildcard(name, opts)
                    if *opts == a::WildcardAdditionalOptions::default() =>
                {
                    let parts = object(name);
                    let [qualifier] = parts.as_slice() else {
                        return Err(Error::Unsupported);
                    };
                    let mut found = false;
                    for (i, c) in schema[..source_width].iter().enumerate() {
                        if c.qualifier == *qualifier && !c.system {
                            found = true;
                            fields.push(c.field.clone());
                            projection.push(column(i, c.field.data_type));
                        }
                    }
                    if !found {
                        return Err(Error::UnknownColumn);
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
        if let Some(o) = order_by {
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
        let distinct_on = if let Some(a::Distinct::On(expressions)) = &s.distinct {
            let mut positions = vec![];
            for expression in expressions {
                let mut expression = expression;
                while let A::Nested(inner) = expression { expression = inner; }
                let e = self.expr(expression, &schema)?;
                // SQL92 positional/alias keys need separate name resolution.
                // Keep this bounded form on projected input columns only.
                if !matches!(e.kind, K::Column(_)) || matches!(expression, A::Identifier(name)
                    if fields.iter().zip(&projection).any(|(f,p)| f.name == ident(name) && *p != e)) {
                    return Err(Error::Unsupported);
                }
                // This bounded form requires projected keys, so key values share
                // the existing projection byte/work accounting.
                positions.push(projection.iter().position(|p| *p == e).ok_or(Error::Unsupported)?);
            }
            let mut remaining = positions.clone();
            for o in &order {
                let e = o.output.map_or(&o.e, |i| &projection[i]);
                if !remaining.is_empty() && !positions.iter().any(|i| projection[*i] == *e) {
                    return Err(Error::Unsupported);
                }
                remaining.retain(|i| projection[*i] != *e);
            }
            positions
        } else {
            vec![]
        };
        let limit = match limit {
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
            distinct_on,
            expand,
            source_width,
            outer_width: outer.len(),
            group,
            aggregate,
        })
    }
    fn acl_source(&mut self, args: &[a::FunctionArg], outer: &Schema) -> R<(Source, Vec<Field>)> {
        if args.len() != 1 {
            return Err(Error::Unsupported);
        }
        let mut args = self.args(args, outer)?;
        expect(&mut args[0], PgType::AclItemArray)?;
        no_set_or_window(&args[0])?;
        Ok((
            Source::Unnest(
                E {
                    kind: K::Call("_aclexplode".into(), args),
                    ty: PgType::AnyArray,
                },
                false,
            ),
            [
                ("grantor", PgType::Oid),
                ("grantee", PgType::Oid),
                ("privilege_type", PgType::Text),
                ("is_grantable", PgType::Bool),
            ]
            .into_iter()
            .map(|(name, data_type)| Field { name: name.into(), data_type })
            .collect(),
        ))
    }
    fn unnest_source(es: Vec<E>, ordinality: bool) -> R<(Source, Vec<Field>)> {
        if es.is_empty() {
            return Err(Error::Unsupported);
        }
        let mut fields = es.iter().map(|e| {
            no_set_or_window(e)?;
            Ok(Field { name: "unnest".into(), data_type: array_element(e.ty)? })
        }).collect::<R<Vec<_>>>()?;
        if ordinality {
            fields.push(Field { name: "ordinality".into(), data_type: PgType::Int8 });
        }
        Ok((Source::ZipUnnest(es, ordinality), fields))
    }
    fn table(&mut self, t: &a::TableFactor, outer: &Schema) -> R<(Source, Schema)> {
        if let a::TableFactor::NestedJoin { table_with_joins: from, alias: None } = t {
            let (mut source, mut schema) = self.table(&from.relation, outer)?;
            for join in &from.joins {
                let (rhs, rs) = self.table(&join.relation, outer)?;
                if matches!(rhs, Source::Unnest(..) | Source::ZipUnnest(..) | Source::Options(_) | Source::Scalar(_)) {
                    return Err(Error::Unsupported);
                }
                let width = rs.len();
                schema.extend(rs);
                let (constraint, left) = match &join.join_operator {
                    a::JoinOperator::Inner(c) => (c, false),
                    a::JoinOperator::LeftOuter(c) => (c, true),
                    a::JoinOperator::CrossJoin => (&a::JoinConstraint::None, false),
                    _ => return Err(Error::Unsupported),
                };
                let mut on = match constraint {
                    a::JoinConstraint::On(e) => {
                        let mut on_schema = schema.clone();
                        append_outer(&mut on_schema, outer)?;
                        self.expr(e, &on_schema)?
                    }
                    a::JoinConstraint::None => truth(),
                    _ => return Err(Error::Unsupported),
                };
                expect(&mut on, PgType::Bool)?;
                no_set_or_window(&on)?;
                source = Source::Join(Box::new(source), Box::new(rhs), on, left, width, false);
            }
            return Ok((source, schema));
        }
        self.sources += 1;
        if self.sources > 32 {
            return Err(Error::Limit);
        }
        let (source, fields, alias, default) = match t {
            a::TableFactor::Function { name, args, alias, .. } => {
                let p = object(name);
                if !matches!(p.as_slice(), [n] if n == "aclexplode")
                    && !matches!(p.as_slice(), [ns, n] if ns == "pg_catalog" && n == "aclexplode")
                {
                    return Err(Error::Unsupported);
                }
                let (source, fields) = self.acl_source(args, outer)?;
                (source, fields, alias, "aclexplode".into())
            }
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
                    || (*with_ordinality && object(name).last().is_none_or(|n| n != "unnest"))
                    || !partitions.is_empty()
                    || json_path.is_some()
                {
                    return Err(Error::Unsupported);
                }
                let p = object(name);
                let n = p.last().ok_or(Error::Unsupported)?.clone();
                let qualified = (p.len() == 2).then(|| format!("\0{}\0{n}", p[0]));
                let user_key = match &qualified {
                    Some(key) if self.user_relations.contains_key(key) => key.clone(),
                    // Preserve the original public-only API for callers that
                    // supply no schema-specific keys for this relation.
                    Some(_)
                        if p[0] == "public"
                            && !self.user_relations.keys().any(|k| {
                                k.starts_with('\0') && k.rsplit('\0').next() == Some(n.as_str())
                            }) =>
                    {
                        n.clone()
                    }
                    Some(key) => key.clone(),
                    None => n.clone(),
                };
                if p.len() > 2 || args.is_some() && p.len() == 2 && p[0] != "pg_catalog" {
                    return Err(Error::Unsupported);
                }
                if args.is_none()
                    && p.len() == 2
                    && !self.user_relations.contains_key(&user_key)
                    && (p[0] == "public" || p[0] != catalog::relation_namespace(&n))
                {
                    return Err(
                        if matches!(p[0].as_str(), "pg_catalog" | "information_schema") {
                            Error::Unsupported
                        } else {
                            Error::MixedRelations
                        },
                    );
                }
                if let Some(args) = args {
                    if n == "aclexplode" && args.settings.is_none() {
                        let (source, fields) = self.acl_source(&args.args, outer)?;
                        (source, fields, alias, n)
                    } else if n == "pg_available_extension_versions"
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
                    } else if n == "pg_show_all_settings" && args.args.is_empty() && args.settings.is_none() {
                        let sql = "SELECT 'default_table_access_method'::text AS name, 'heap'::text AS setting,
                            NULL::text AS unit, 'Client Connection Defaults / Statement Behavior'::text AS category,
                            'Sets the default table access method for new tables.'::text AS short_desc, NULL::text AS extra_desc,
                            'user'::text AS context, 'string'::text AS vartype, 'default'::text AS source,
                            NULL::text AS min_val, NULL::text AS max_val, NULL::text[] AS enumvals,
                            'heap'::text AS boot_val, 'heap'::text AS reset_val, NULL::text AS sourcefile,
                            NULL::integer AS sourceline, false AS pending_restart";
                        let mut statements = Parser::parse_sql(&PostgreSqlDialect {}, sql).map_err(|_| Error::Syntax)?;
                        let a::Statement::Query(q) = statements.remove(0) else { unreachable!() };
                        let query = self.query(&q, &vec![])?;
                        let fields = query.fields.clone();
                        (Source::Derived(std::sync::Arc::new(query)), fields, alias, n)
                    } else if n == "pg_get_sequence_data"
                        && args.args.len() == 1
                        && args.settings.is_none()
                    {
                        let mut es = self.args(&args.args, outer)?;
                        expect(&mut es[0], PgType::Oid)?;
                        no_set_or_window(&es[0])?;
                        if !matches!(&es[0].kind, K::Column(i)
                            if outer[*i].qualifier == "pg_sequence"
                                && outer[*i].field.name == "seqrelid")
                        {
                            return Err(Error::Unsupported);
                        }
                        // The catalog exposes no sequence rows. This PG18
                        // dump inventory therefore has a typed empty result.
                        (
                            Source::Empty,
                            vec![
                                Field {
                                    name: "last_value".into(),
                                    data_type: PgType::Int8,
                                },
                                Field {
                                    name: "is_called".into(),
                                    data_type: PgType::Bool,
                                },
                            ],
                            alias,
                            n,
                        )
                    } else if n == "pg_options_to_table"
                        && args.args.len() == 1
                        && args.settings.is_none()
                    {
                        let mut es = self.args(&args.args, outer)?;
                        let mut e = es.remove(0);
                        expect(&mut e, PgType::TextArray)?;
                        no_set_or_window(&e)?;
                        (
                            Source::Options(e),
                            vec![
                                Field {
                                    name: "option_name".into(),
                                    data_type: PgType::Text,
                                },
                                Field {
                                    name: "option_value".into(),
                                    data_type: PgType::Text,
                                },
                            ],
                            alias,
                            n,
                        )
                    } else if n == "generate_subscripts" && args.settings.is_none() {
                        let e = subscripts::compile(self.args(&args.args, outer)?)?;
                        (
                            Source::Unnest(e, false),
                            vec![Field {
                                name: alias.as_ref().map_or(n.clone(), |a| ident(&a.name)),
                                data_type: PgType::Int4,
                            }],
                            alias,
                            n,
                        )
                    } else if n == "unnest" && args.settings.is_none() {
                        let es = self.args(&args.args, outer)?;
                        let (source, fields) = Self::unnest_source(es, *with_ordinality)?;
                        (source, fields, alias, n)
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
                        let mut es = self.args(&args.args, outer)?;
                        for e in &mut es {
                            expect(e, PgType::Int4)?;
                            no_set_or_window(e)?;
                        }
                        (
                            Source::Series(es[0].clone(), es[1].clone()),
                            vec![Field {
                                name: alias.as_ref().map_or(n.clone(), |a| ident(&a.name)),
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
                        (Source::Cte(q), fields, alias, n)
                    } else if let Some(fields) = self.user_relations.get(&user_key) {
                        self.used_relations.insert(user_key.clone(), fields.clone());
                        (Source::Catalog(user_key), fields.clone(), alias, n)
                    } else {
                        if p.len() == 1 && !unqualified_catalog(&n)
                            || p.len() == 2 && p[0] == "public"
                        {
                            return Err(Error::MixedRelations);
                        }
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
                // A non-lateral derived table sees enclosing queries, but not
                // sibling FROM inputs of its own query.
                let q = self.query(subquery, outer)?;
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
                if *with_offset || with_offset_alias.is_some() {
                    return Err(Error::Unsupported);
                }
                let es = array_exprs.iter().map(|e| self.expr(e, outer)).collect::<R<Vec<_>>>()?;
                let (source, fields) = Self::unnest_source(es, *with_ordinality)?;
                (source, fields, alias, "unnest".into())
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
        let namespace = match &source {
            Source::Catalog(name) if alias.is_none() && catalog::tableoid(name).is_some() => {
                Some(catalog::relation_namespace(name).to_owned())
            }
            _ => None,
        };
        let tableoid = match &source {
            Source::Catalog(name) => catalog::tableoid(name)
                .or_else(|| self.user_relations.contains_key(name).then_some(0)),
            _ => None,
        };
        let catalog_system_columns = matches!(&source,
            Source::Catalog(name) if catalog::tableoid(name).is_some());
        let mut schema: Schema = fields
            .into_iter()
            .map(|field| Col {
                qualifier: qualifier.clone(),
                namespace: namespace.clone(),
                system: catalog_system_columns && field.name == "xmin",
                field,
                natural_hidden: false,
                scope_depth: 0,
            })
            .collect();
        // Physical system slots participate in binding and null extension, but
        // never in wildcard expansion or the ordinary catalog column inventory.
        if tableoid.is_some() {
            schema.push(Col {
                qualifier,
                namespace,
                field: Field {
                    name: "tableoid".into(),
                    data_type: PgType::Oid,
                },
                natural_hidden: false,
                system: true,
                scope_depth: 0,
            });
        }
        Ok((source, schema))
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
            A::Identifier(n)
                if n.quote_style.is_none()
                    && matches!(
                        ident(n).as_str(),
                        "user" | "current_role" | "current_catalog"
                    ) =>
            {
                (K::Call(ident(n), vec![]), PgType::Name)
            }
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
                    a::Value::Number(n, _) if n.contains(['.', 'e', 'E']) || n.parse::<i64>().is_err() => {
                        let numeric =
                            giggles_pg_types::Numeric::parse(n).map_err(|_| Error::Type)?;
                        literal(Value::Numeric(numeric), PgType::Numeric)
                    }
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
                let left = comparison_operand(left);
                let right = comparison_operand(right);
                let op = match op {
                    Op::PGCustomBinaryOperator(parts)
                        if parts.len() == 2 && parts[0] == "pg_catalog" =>
                    {
                        match parts[1].as_str() {
                            "=" => Op::Eq,
                            "~" => Op::PGRegexMatch,
                            "!~" => Op::PGRegexNotMatch,
                            "~*" => Op::PGRegexIMatch,
                            "!~*" => Op::PGRegexNotIMatch,
                            _ => return Err(Error::Unsupported),
                        }
                    }
                    op => op.clone(),
                };
                let comparison = matches!(
                    op,
                    Op::Eq
                        | Op::NotEq
                        | Op::Lt
                        | Op::LtEq
                        | Op::Gt
                        | Op::GtEq
                        | Op::PGRegexMatch
                        | Op::PGRegexNotMatch
                        | Op::PGRegexIMatch
                        | Op::PGRegexNotIMatch
                );
                let mut operand = |expr: &A| -> R<E> {
                    if let A::Collate { expr, collation } = expr {
                        let parts = collation.0.iter().map(ident).collect::<Vec<_>>();
                        if !comparison
                            || !matches!(parts.as_slice(), [name] if name == "C" || name == "default")
                                && !matches!(parts.as_slice(), [schema, name] if schema == "pg_catalog" && (name == "C" || name == "default"))
                        {
                            return Err(Error::Unsupported);
                        }
                        let value = self.expr(expr, schema)?;
                        if !matches!(value.ty, PgType::Text | PgType::Varchar | PgType::Name) {
                            return Err(Error::Type);
                        }
                        Ok(value)
                    } else {
                        self.expr(expr, schema)
                    }
                };
                let lhs = operand(left)?;
                let rhs = operand(right)?;
                // This frontend cannot retain competing explicit collations.
                if let (A::Collate { collation: a, .. }, A::Collate { collation: b, .. }) = (left, right)
                    && object(a).last() != object(b).last()
                {
                    return Err(Error::Unsupported);
                }
                if (matches!(left, A::Collate { .. }) || matches!(right, A::Collate { .. }))
                    && (!matches!(lhs.ty, PgType::Text | PgType::Varchar | PgType::Name)
                        || !matches!(rhs.ty, PgType::Text | PgType::Varchar | PgType::Name))
                {
                    return Err(Error::Type);
                }
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
                        | Op::BitwiseAnd
                        | Op::StringConcat
                        | Op::PGRegexMatch
                        | Op::PGRegexNotMatch
                        | Op::PGRegexIMatch
                        | Op::PGRegexNotIMatch
                ) {
                    return Err(Error::Unsupported);
                }
                if op == Op::StringConcat
                    && (lhs.ty.array_element().is_some() || rhs.ty.array_element().is_some())
                {
                    return Err(Error::Unsupported);
                }
                return Ok(binary(lhs, op, rhs));
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
            A::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let q = self.query(subquery, schema)?;
                if q.fields.len() != 1 {
                    return Err(Error::Type);
                }
                let ty = q.fields[0].data_type;
                let primitive = |t| numeric(t) || textual(t) || t == PgType::Bool;
                if !primitive(ty) {
                    return Err(Error::Type);
                }
                let mut lhs = self.expr(expr, schema)?;
                expect(&mut lhs, ty)?;
                if !primitive(lhs.ty) {
                    return Err(Error::Type);
                }
                (
                    K::InQuery(Box::new(lhs), Box::new(q), *negated),
                    PgType::Bool,
                )
            }
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
            }
            | A::ILike {
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
                        if matches!(e, A::ILike { .. }) {
                            "ilike"
                        } else {
                            "like"
                        }
                        .into(),
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
                let name = data_type.to_string().to_ascii_lowercase();
                let name = name.strip_prefix("pg_catalog.").unwrap_or(&name);
                if matches!(name, "text" | "varchar" | "character varying") {
                    let mut inner = expr.as_ref();
                    while let A::Nested(e) = inner {
                        inner = e;
                    }
                    if let A::Cast {
                        expr: value,
                        data_type: registered,
                        format: None,
                        kind,
                    } = inner
                        && matches!(kind, a::CastKind::Cast | a::CastKind::DoubleColon)
                        && matches!(
                            registered
                                .to_string()
                                .to_ascii_lowercase()
                                .strip_prefix("pg_catalog.")
                                .unwrap_or(&registered.to_string().to_ascii_lowercase()),
                            "regtype" | "regclass" | "regprocedure" | "regoperator"
                        )
                    {
                        let registered = registered.to_string().to_ascii_lowercase();
                        if registered.ends_with("regtype") || registered.ends_with("regclass") {
                            let value = self.expr(inner, schema)?;
                            return Ok(E {
                                kind: K::Call(if registered.ends_with("regclass") { "_regclass_display" } else { "_regtype_display" }.into(), vec![value]),
                                ty: PgType::Text,
                            });
                        }
                        let mut value = self.expr(value, schema)?;
                        // pg_amproc.amproc already has the nominal regproc descriptor.
                        // Admit it only inside this complete textual regprocedure form.
                        if !(value.ty == PgType::Regproc
                            && registered.to_string().eq_ignore_ascii_case("regprocedure"))
                        {
                            expect(&mut value, PgType::Oid)?;
                        }
                        return Ok(E {
                            kind: K::RegisteredDisplay(Box::new(value)),
                            ty: PgType::Text,
                        });
                    }
                }
                let mut e = Box::new(self.expr(expr, schema)?);
                match name {
                    "regtype" if numeric(e.ty) => {
                        coerce(&mut e, PgType::Oid)?;
                        return Ok(*e);
                    }
                    "regtype" => {
                        expect(&mut e, PgType::Text)?;
                        (K::Call("_regtype".into(), vec![*e]), PgType::Oid)
                    }
                    "regclass" => (K::Regclass(e), PgType::Oid),
                    "regcollation" => {
                        expect(&mut e, PgType::Oid)?;
                        (K::Call("_regcollation_display".into(), vec![*e]), PgType::Text)
                    }
                    "regnamespace" => {
                        if let K::Literal(Value::Text(name)) = &e.kind {
                            let oid = match name.as_str() {
                                "pg_catalog" => 11,
                                "public" => 2200,
                                "information_schema" => 13293,
                                _ => return Err(Error::Unsupported),
                            };
                            return Ok(literal(Value::Int(oid), PgType::Oid));
                        }

                        expect(&mut e, PgType::Oid)?;
                        (K::Call("_regnamespace".into(), vec![*e]), PgType::Name)
                    }
                    "regproc" if e.ty == PgType::Regproc => return Ok(*e),
                    "regproc" => (K::Regproc(e), PgType::Oid),
                    "regoper" => (K::Regoper(e), PgType::Oid),
                    _ => {
                        let ty = cast_type(name)?;
                        if e.ty == ty
                            && ty.array_element().is_some()
                            && !matches!(e.kind, K::Param(_))
                        {
                            return Ok(*e);
                        }
                        if matches!(e.kind, K::Param(_))
                            && matches!(ty, PgType::Numeric | PgType::Float4 | PgType::Float8)
                        {
                            e.ty = ty;
                        }
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
            A::Trim { expr, trim_where, trim_what, trim_characters: None } => {
                let name = match trim_where {
                    Some(a::TrimWhereField::Leading) => "ltrim",
                    Some(a::TrimWhereField::Trailing) => "rtrim",
                    _ => "btrim",
                };
                let args = vec![self.expr(expr, schema)?, trim_what.as_ref()
                    .map(|e| self.expr(e, schema)).transpose()?
                    .unwrap_or_else(|| literal(Value::Text(" ".into()), PgType::Text))];
                (K::Call(name.into(), args), PgType::Text)
            }
            A::Substring {
                expr,
                substring_from: Some(from),
                substring_for,
                ..
            } => {
                let mut args = vec![self.expr(expr, schema)?, self.expr(from, schema)?];
                if let Some(length) = substring_for {
                    args.push(self.expr(length, schema)?);
                }
                let name = if textual(args[1].ty) {
                    "_substring_regex"
                } else {
                    "substring"
                };
                (K::Call(name.into(), args), PgType::Text)
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
                if n == "pg_partition_ancestors" {
                    let a::FunctionArguments::List(args) = &f.args else {
                        return Err(Error::Unsupported);
                    };
                    if args.args.len() != 1
                        || args.duplicate_treatment.is_some()
                        || !args.clauses.is_empty()
                        || f.over.is_some()
                    {
                        return Err(Error::Unsupported);
                    }
                    let mut input = self.args(&args.args, schema)?.remove(0);
                    expect(&mut input, PgType::Oid)?;
                    // This snapshot has no partition hierarchy, so there are no ancestors.
                    return Ok(E {
                        kind: K::Unnest(Box::new(E {
                            kind: K::Call("_partition_ancestors".into(), vec![input]),
                            ty: PgType::OidArray,
                        })),
                        ty: PgType::Oid,
                    });
                }
                if n == "count" {
                    let a::FunctionArguments::List(args) = &f.args else {
                        return Err(Error::Unsupported);
                    };
                    if args.args.len() != 1
                        || args.duplicate_treatment.is_some()
                        || !args.clauses.is_empty()
                        || f.over.is_some()
                    {
                        return Err(Error::Unsupported);
                    }
                    let arg = match &args.args[0] {
                        a::FunctionArg::Unnamed(a::FunctionArgExpr::Wildcard) => None,
                        a::FunctionArg::Unnamed(a::FunctionArgExpr::QualifiedWildcard(name)) => {
                            let parts = object(name);
                            let [qualifier] = parts.as_slice() else {
                                return Err(Error::Unsupported);
                            };
                            // A physical row marker survives even when every user field
                            // is NULL, and becomes NULL only through outer-join extension.
                            let (i, col) = schema
                                .iter()
                                .enumerate()
                                .find(|(_, c)| {
                                    c.qualifier == *qualifier
                                        && c.system
                                        && c.field.name == "tableoid"
                                })
                                .ok_or(Error::Unsupported)?;
                            Some(Box::new(column(i, col.field.data_type)))
                        }
                        _ => {
                            let arg = self.args(&args.args, schema)?.remove(0);
                            no_set_or_window(&arg)?;
                            if has_aggregate(&arg) {
                                return Err(Error::Unsupported);
                            }
                            Some(Box::new(arg))
                        }
                    };
                    return Ok(E {
                        kind: K::Count(arg),
                        ty: PgType::Int8,
                    });
                }
                if n == "string_agg" {
                    let a::FunctionArguments::List(args) = &f.args else {
                        return Err(Error::Unsupported);
                    };
                    if args.args.len() != 2
                        || args.duplicate_treatment.is_some()
                        || !args.clauses.is_empty()
                        || f.over.is_some()
                    {
                        return Err(Error::Unsupported);
                    }
                    let mut inputs = self.args(&args.args, schema)?;
                    for input in &mut inputs {
                        expect(input, PgType::Text)?;
                        no_set_or_window(input)?;
                        if has_aggregate(input) {
                            return Err(Error::Unsupported);
                        }
                    }
                    // Constant delimiters cover the bounded catalog introspection form.
                    if !matches!(inputs[1].kind, K::Literal(_)) {
                        return Err(Error::Unsupported);
                    }
                    let delimiter = inputs.pop().unwrap();
                    let values = E {
                        kind: K::ArrayAgg(Box::new(inputs.remove(0)), vec![]),
                        ty: PgType::TextArray,
                    };
                    return Ok(E {
                        kind: K::Call("_string_agg".into(), vec![values, delimiter]),
                        ty: PgType::Text,
                    });
                }
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
                let mut args = match &f.args {
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
                    // Only immutable presets can be folded here. Session GUCs
                    // remain owned by the server's settings evaluator.
                    if n == "current_setting"
                        && (args.len() == 1
                            || (args.len() == 2
                                && matches!(&args[0].kind, K::Literal(Value::Text(name))
                                    if name.eq_ignore_ascii_case("giggles_capabilities"))
                                && matches!(&args[1].kind, K::Literal(Value::Bool(_)))))
                    {
                        let K::Literal(Value::Text(name)) = &args[0].kind else {
                            return Err(Error::Unsupported);
                        };
                        let value = match name.to_ascii_lowercase().as_str() {
                            "giggles_capabilities" => crate::CAPABILITIES,
                            "server_encoding" => "UTF8",
                            "server_version" => "18.6",
                            "server_version_num" => "180006",
                            "max_identifier_length" => "63",
                            "max_index_keys" => "32",
                            "block_size" => "8192",
                            "integer_datetimes" => "on",
                            _ => return Err(Error::Unsupported),
                        };
                        return Ok(literal(Value::Text(value.into()), PgType::Text));
                    }
                    if n == "_pg_expandarray" {
                        if args.len() != 1 {
                            return Err(Error::Type);
                        }
                        (K::Expand(Box::new(args[0].clone())), PgType::Record)
                    } else if n == "aclexplode" && args.len() == 1 {
                        expect(&mut args[0], PgType::AclItemArray)?;
                        no_set_or_window(&args[0])?;
                        (
                            K::Unnest(Box::new(E {
                                kind: K::Call("_aclexplode".into(), args),
                                ty: PgType::AnyArray,
                            })),
                            PgType::Record,
                        )
                    } else if n == "generate_series" && args.len() == 2 {
                        for arg in &mut args {
                            expect(arg, PgType::Int4)?;
                            no_set_or_window(arg)?;
                        }
                        (
                            K::Unnest(Box::new(E {
                                kind: K::Call("_series".into(), args),
                                ty: PgType::Int4Array,
                            })),
                            PgType::Int4,
                        )
                    } else if n == "unnest" {
                        if args.len() != 1 {
                            return Err(Error::Type);
                        }
                        let ty = array_element(args[0].ty)?;
                        no_set_or_window(&args[0])?;
                        (K::Unnest(Box::new(args[0].clone())), ty)
                    } else {
                        if n == "age" && args.len() == 1 {
                            expect(&mut args[0], PgType::Xid)?;
                        }
                        if n == "greatest" && !args.is_empty() {
                            let ty = common_type(args.iter())?;
                            for arg in &mut args {
                                expect(arg, ty)?;
                            }
                        }
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
                if es.iter().filter(|e| !unknown(e)).all(|e| numeric(e.ty)) {
                    element = common_type(es.iter())?;
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
                let ty = if e.ty == PgType::Name {
                    PgType::Char
                } else {
                    array_element(e.ty)?
                };
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
                    "grantor" => (0, PgType::Oid),
                    "grantee" => (1, PgType::Oid),
                    "privilege_type" => (2, PgType::Text),
                    "is_grantable" => (3, PgType::Bool),
                    _ => return Err(Error::UnknownColumn),
                };
                (K::Member(Box::new(e), i), t)
            }
            A::AnyOp {
                left,
                compare_op,
                right,
                ..
            } if *compare_op == Op::Eq
                || matches!(compare_op, Op::PGCustomBinaryOperator(parts) if parts.as_slice() == ["pg_catalog", "="]) =>
            {
                (
                    K::Any(
                        Box::new(self.expr(left, schema)?),
                        Op::Eq,
                        Box::new(self.expr(right, schema)?),
                    ),
                    PgType::Bool,
                )
            }
            A::Exists { subquery, negated } => {
                let q = self.query(subquery, schema)?;
                (K::Exists(Box::new(q), *negated), PgType::Bool)
            }
            A::IsDistinctFrom(l, r) | A::IsNotDistinctFrom(l, r) => (
                K::Distinct(
                    Box::new(self.expr(l, schema)?),
                    Box::new(self.expr(r, schema)?),
                    matches!(e, A::IsNotDistinctFrom(..)),
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
// Scope metadata changes binding precedence, never physical row positions.
fn append_outer(schema: &mut Schema, outer: &Schema) -> R<()> {
    for col in outer {
        let mut col = col.clone();
        col.scope_depth = col.scope_depth.checked_add(1).ok_or(Error::Limit)?;
        if col.scope_depth > 32 {
            return Err(Error::Limit);
        }
        schema.push(col);
    }
    Ok(())
}
fn reject_local_series_reference(factor: &a::TableFactor, local: &[Col]) -> R<()> {
    struct LocalReference<'a>(&'a [Col]);
    impl Visitor for LocalReference<'_> {
        type Break = Error;
        fn pre_visit_expr(&mut self, e: &A) -> ControlFlow<Error> {
            let parts = match e {
                A::Identifier(i) => vec![ident(i)],
                A::CompoundIdentifier(parts) => parts.iter().map(ident).collect(),
                _ => return ControlFlow::Continue(()),
            };
            // Even an ambiguous local name must not fall through to an outer
            // name. A qualified local alias shadows all outer columns with
            // that qualifier, including columns absent from the local table.
            let local_name = if parts.len() == 2 {
                self.0.iter().any(|c| c.qualifier == parts[0])
            } else {
                self.0
                    .iter()
                    .any(|c| c.field.name == parts[0] && !c.natural_hidden)
            };
            if local_name {
                ControlFlow::Break(Error::Unsupported)
            } else {
                ControlFlow::Continue(())
            }
        }
    }
    if let ControlFlow::Break(error) = factor.visit(&mut LocalReference(local)) {
        Err(error)
    } else {
        Ok(())
    }
}

fn lookup(schema: &[Col], p: &[String]) -> R<usize> {
    let (namespace, p) = if p.len() == 3 {
        (Some(&p[0]), &p[1..])
    } else {
        (None, p)
    };
    if p.is_empty() || p.len() > 2 {
        return Err(Error::UnknownColumn);
    }
    // A qualified alias shadows the entire outer alias, even for missing columns.
    let depth = schema
        .iter()
        .filter(|c| {
            if p.len() == 2 {
                c.qualifier == p[0] && namespace.is_none_or(|n| c.namespace.as_ref() == Some(n))
            } else {
                c.field.name == p[0] && !c.natural_hidden
            }
        })
        .map(|c| c.scope_depth)
        .min()
        .ok_or(Error::UnknownColumn)?;
    let mut found = schema
        .iter()
        .enumerate()
        .filter(|(_, c)| {
            c.scope_depth == depth
                && namespace.is_none_or(|n| c.namespace.as_ref() == Some(n))
                && c.field.name == p[p.len() - 1]
                && (p.len() != 1 || !c.natural_hidden)
                && (p.len() == 1 || c.qualifier == p[0])
        })
        .map(|(i, _)| i);
    let first = found.next().ok_or(Error::UnknownColumn)?;
    if found.next().is_some() {
        Err(Error::AmbiguousColumn)
    } else {
        Ok(first)
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
    matches!(e.kind, K::ArrayAgg(..) | K::Count(_)) || children(e).into_iter().any(has_aggregate)
}
fn validate_group(e: &E, group: &[E]) -> R<()> {
    if group.contains(e) || matches!(e.kind, K::ArrayAgg(..) | K::Count(_)) {
        return Ok(());
    }
    if matches!(
        e.kind,
        K::Column(_)
            | K::RowNumber(..)
            | K::Expand(_)
            | K::Unnest(_)
            | K::Subquery(_)
            | K::Exists(..)
            | K::InQuery(..)
            | K::ArrayQuery(_)
    ) {
        return Err(Error::Unsupported);
    }
    for c in children(e) {
        validate_group(c, group)?;
    }
    Ok(())
}
fn cast_type(n: &str) -> R<PgType> {
    let normalized;
    let n = if n.ends_with("[][]") {
        normalized = format!("{}[]", n.trim_end_matches("[]"));
        normalized.as_str()
    } else { n };
    Ok(match n {
        "bool" | "boolean" => PgType::Bool,
        "smallint" | "int2" => PgType::Int2,
        "int" | "integer" | "int4" => PgType::Int4,
        "bigint" | "int8" => PgType::Int8,
        "oid" => PgType::Oid,
        "xid" => PgType::Xid,
        "numeric" | "decimal" => PgType::Numeric,
        "real" | "float4" => PgType::Float4,
        "double precision" | "float8" => PgType::Float8,
        "int[]" | "integer[]" | "int4[]" => PgType::Int4Array,
        "int2[]" | "smallint[]" => PgType::Int2Array,
        "bigint[]" | "int8[]" => PgType::Int8Array,
        "oid[]" => PgType::OidArray,
        "text[]" => PgType::TextArray,
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
// PostgreSQL ACL role names quote punctuation and non-ASCII bytes, but allow
// uppercase letters, digits (including first), and SQL keywords unquoted.
fn acl_role_name(name: &str) -> String {
    if name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
        name.to_owned()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}
fn textual(t: PgType) -> bool {
    matches!(
        t,
        PgType::Text | PgType::Varchar | PgType::Name | PgType::Char
    )
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
                if t == PgType::Oid || e.ty == PgType::Oid {
                    PgType::Oid
                } else if t == PgType::Int8 || e.ty == PgType::Int8 {
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
    // Common results use implicit edges; explicit casts use K::Cast directly.
    if !unknown(e)
        && numeric(e.ty)
        && numeric(t)
        && e.ty != t
        && !matches!(
            (e.ty, t),
            (PgType::Int2, PgType::Int4 | PgType::Int8 | PgType::Oid)
                | (PgType::Int4, PgType::Int8 | PgType::Oid)
                | (PgType::Int8, PgType::Oid)
        )
    {
        return Err(Error::Type);
    }
    expect(e, t)?;
    if e.ty != t {
        *e = E {
            kind: if t == PgType::Oid && matches!(e.ty, PgType::Int2 | PgType::Int4) {
                K::IntegerOid(Box::new(e.clone()), e.ty)
            } else {
                K::Cast(Box::new(e.clone()), t)
            },
            ty: t,
        };
    }
    Ok(())
}
// Keep UNION's numeric promotion separate from the narrower integer-only
// arithmetic and CASE profile. These are PostgreSQL's implicit widening edges.
fn union_numeric_rank(t: PgType) -> Option<u8> {
    Some(match t {
        PgType::Int2 => 0,
        PgType::Int4 => 1,
        PgType::Int8 => 2,
        PgType::Numeric => 3,
        PgType::Float4 => 4,
        PgType::Float8 => 5,
        _ => return None,
    })
}
fn union_type(l: &E, r: &E) -> R<PgType> {
    if !unknown(l)
        && !unknown(r)
        && let (Some(a), Some(b)) = (union_numeric_rank(l.ty), union_numeric_rank(r.ty))
    {
        return Ok(if a >= b { l.ty } else { r.ty });
    }
    common_type([l, r].into_iter())
}
fn union_coerce(e: &mut E, ty: PgType) -> R<()> {
    if !unknown(e)
        && e.ty != ty
        && let (Some(from), Some(to)) = (union_numeric_rank(e.ty), union_numeric_rank(ty))
    {
        if from > to {
            return Err(Error::Type);
        }
        *e = E {
            kind: K::Cast(Box::new(e.clone()), ty),
            ty,
        };
        return Ok(());
    }
    coerce(e, ty)
}
fn check_expr(e: &mut E) -> R<()> {
    match &mut e.kind {
        K::Distinct(l, r, _) => compatible(l, r)?,
        K::Binary(l, op, r) => match op {
            Op::BitwiseAnd => {
                let ty = common_type([l.as_ref(), r.as_ref()].into_iter())?;
                if !matches!(ty, PgType::Int2 | PgType::Int4 | PgType::Int8) { return Err(Error::Type); }
                coerce(l, ty)?;
                coerce(r, ty)?;
                e.ty = ty;
            }
            Op::And | Op::Or => {
                expect(l, PgType::Bool)?;
                expect(r, PgType::Bool)?;
            }
            Op::Plus | Op::Minus | Op::Multiply | Op::Divide | Op::Modulo => {
                expect(l, PgType::Int8)?;
                expect(r, PgType::Int8)?;
            }
            Op::StringConcat
            | Op::PGRegexMatch
            | Op::PGRegexNotMatch
            | Op::PGRegexIMatch
            | Op::PGRegexNotIMatch => {
                expect(l, PgType::Text)?;
                expect(r, PgType::Text)?;
            }
            Op::Eq | Op::NotEq
                if matches!(
                    (l.ty, r.ty),
                    (PgType::Regproc, PgType::Oid) | (PgType::Oid, PgType::Regproc)
                ) => {}
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
            if unknown(r) && r.ty.array_element().is_none() { expect(r, array_type(l.ty)?)?; }
            let ty = array_element(r.ty)?;
            expect(l, ty)?;
        }
        K::Index(_, i) => expect(i, PgType::Int4)?,
        K::Expand(x) => {
            if !matches!(x.ty, PgType::Int2Array | PgType::Int2Vector) {
                return Err(Error::Type);
            }
        }
        K::Unnest(x) => {
            e.ty = if matches!(&x.kind, K::Call(n, _) if n == "_aclexplode") {
                PgType::Record
            } else {
                array_element(x.ty)?
            };
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
            let t = common_type(std::iter::once(el.as_ref()).chain(pairs.iter().map(|(_, r)| r)))?;
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
            "substring" | "_substring_regex" => {
                expect(&mut args[0], PgType::Text)?;
                if n == "_substring_regex" {
                    if args.len() != 2 {
                        return Err(Error::Unsupported);
                    }
                    expect(&mut args[1], PgType::Text)?;
                } else {
                    for arg in &mut args[1..] {
                        expect(arg, PgType::Int4)?;
                    }
                }
            }
            "to_regtype" | "_regtype" => expect(&mut args[0], PgType::Text)?,
            "array_remove" | "array_position" => {
                let element = array_element(args[0].ty)?;
                expect(&mut args[1], element)?;
            }
            "_aclexplode" => expect(&mut args[0], PgType::AclItemArray)?,
            "acldefault" => {
                expect(&mut args[0], PgType::Char)?;
                coerce(&mut args[1], PgType::Oid)?;
            }
            "pg_get_viewdef" => {
                if !textual(args[0].ty) { expect(&mut args[0], PgType::Oid)?; }
                let named = textual(args[0].ty);
                if let Some(pretty) = args.get_mut(1)
                    && (named || pretty.ty != PgType::Int4) { expect(pretty, PgType::Bool)?; }
            }
            "pg_get_ruledef" | "pg_get_triggerdef" => {
                expect(&mut args[0], PgType::Oid)?;
                expect(&mut args[1], PgType::Bool)?;
            }
            "pg_get_function_arguments" | "pg_get_function_result" | "pg_get_function_sqlbody" => {
                expect(&mut args[0], PgType::Oid)?;
            }
            "split_part" => {
                expect(&mut args[0], PgType::Text)?;
                expect(&mut args[1], PgType::Text)?;
                expect(&mut args[2], PgType::Int4)?;
            }
            "current_schemas" => expect(&mut args[0], PgType::Bool)?,
            "encode" => {
                expect(&mut args[0], PgType::Bytea)?;
                expect(&mut args[1], PgType::Text)?;
            }
            "quote_ident" | "quote_literal" => expect(&mut args[0], PgType::Text)?,
            "chr" => expect(&mut args[0], PgType::Int4)?,
            "pg_tablespace_location" | "pg_get_partkeydef" => expect(&mut args[0], PgType::Oid)?,
            "replace" | "translate" | "like" | "ilike" | "btrim" | "ltrim" | "rtrim" => {
                for x in args {
                    expect(x, PgType::Text)?
                }
            }
            "length" | "upper" => expect(&mut args[0], PgType::Text)?,
            "pg_encoding_to_char" => expect(&mut args[0], PgType::Int4)?,
            "array_to_string" => {
                if args[0].ty != PgType::AclItemArray {
                    array_element(args[0].ty)?;
                }
                for arg in &mut args[1..] {
                    expect(arg, PgType::Text)?;
                }
            }
            "array_upper" | "array_lower" | "array_length" => {
                if args[0].ty != PgType::AclItemArray {
                    array_element(args[0].ty)?;
                }
                expect(&mut args[1], PgType::Int4)?
            }
            "cardinality" => {
                if args[0].ty != PgType::AclItemArray {
                    array_element(args[0].ty)?;
                }
            }
            "pg_relation_size" | "pg_total_relation_size" | "pg_table_size" | "pg_indexes_size"
            | "pg_stat_get_numscans" => {
                expect(&mut args[0], PgType::Oid)?;
                if args.len() == 2 { expect(&mut args[1], PgType::Text)?; }
            }
            "pg_relation_is_publishable" | "pg_get_statisticsobjdef_columns" => {
                expect(&mut args[0], PgType::Oid)?
            }
            "pg_get_expr" => {
                if args[0].ty != PgType::PgNodeTree {
                    expect(&mut args[0], PgType::Text)?;
                }
                expect(&mut args[1], PgType::Oid)?;
                if args.len() == 3 {
                    expect(&mut args[2], PgType::Bool)?;
                }
            }
            "format_type" | "col_description" => {
                expect(&mut args[0], PgType::Oid)?;
                expect(&mut args[1], PgType::Int4)?;
            }
            "pg_indexam_has_property" => {
                expect(&mut args[0], PgType::Oid)?;
                expect(&mut args[1], PgType::Text)?;
            }
            "pg_get_constraintdef" => {
                expect(&mut args[0], PgType::Oid)?;
                if args.len() == 2 {
                    expect(&mut args[1], PgType::Bool)?;
                }
            }
            "pg_get_indexdef" => {
                expect(&mut args[0], PgType::Oid)?;
                if args.len() == 3 {
                    expect(&mut args[1], PgType::Int4)?;
                    expect(&mut args[2], PgType::Bool)?;
                }
            }
            "obj_description" | "shobj_description" => {
                expect(&mut args[0], PgType::Oid)?;
                if args.len() == 2 {
                    expect(&mut args[1], PgType::Text)?;
                }
            }
            "pg_get_userbyid" | "pg_table_is_visible" | "pg_type_is_visible" | "pg_collation_actual_version" => {
                expect(&mut args[0], PgType::Oid)?
            }
            "has_table_privilege" | "has_column_privilege" => {
                let last = args.len() - 1;
                expect(&mut args[last], PgType::Text)?;
                for arg in &mut args[..last] {
                    if unknown(arg) || textual(arg.ty) { expect(arg, PgType::Text)?; }
                    else if !matches!(arg.ty, PgType::Oid | PgType::Int2 | PgType::Int4 | PgType::Int8) {
                        return Err(Error::Type);
                    }
                }
            }
            "has_type_privilege"
            | "has_schema_privilege"
            | "has_database_privilege" => expect(&mut args[1], PgType::Text)?,
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
        (
            "current_database" | "current_schema" | "current_user" | "session_user" | "user"
            | "current_role" | "current_catalog",
            0,
        ) => PgType::Name,
        ("to_regtype", 1) => PgType::Oid,
        ("pg_relation_size", 1 | 2) | ("pg_total_relation_size" | "pg_table_size" | "pg_indexes_size" | "pg_stat_get_numscans", 1) => PgType::Int8,
        ("pg_backend_pid", 0) => PgType::Int4,
        ("txid_current", 0) => PgType::Int8,
        ("age", 1) if a[0].ty == PgType::Xid => PgType::Int4,
        ("greatest", n) if n > 0 => common_type(a.iter())?,
        ("concat", n) if n > 0 && a.iter().all(|e| textual(e.ty)) => PgType::Text,
        ("btrim" | "ltrim" | "rtrim", 2) => PgType::Text,
        ("version", 0) | ("split_part", 3) => PgType::Text,
        ("pg_is_in_recovery" | "pg_is_wal_replay_paused", 0) => PgType::Bool,
        ("current_schemas", 1) => PgType::NameArray,
        ("acldefault", 2) => PgType::AclItemArray,
        ("array_remove", 2) => a[0].ty,
        ("array_position", 2) => PgType::Int4,
        ("replace" | "translate", 3)
        | ("substring", 2 | 3)
        | ("pg_get_expr", 2 | 3)
        | ("format_type", 2)
        | ("pg_collation_actual_version", 1)
        | ("obj_description", 1 | 2)
        | ("shobj_description", 2)
        | ("col_description", 2) => PgType::Text,
        ("pg_get_userbyid", 1) => PgType::Name,
        ("pg_encoding_to_char", 1) | ("array_to_string", 2 | 3) => PgType::Text,
        ("pg_get_statisticsobjdef_columns", 1) => PgType::Text,
        ("pg_get_indexdef", 1 | 3) => PgType::Text,
        ("pg_get_constraintdef", 1 | 2) => PgType::Text,
        ("pg_get_viewdef", 1 | 2) | ("pg_get_ruledef" | "pg_get_triggerdef" | "encode", 2)
        | ("pg_get_function_arguments" | "pg_get_function_result" | "pg_get_function_sqlbody", 1) => {
            PgType::Text
        }
        ("pg_indexam_has_property", 2) | ("pg_relation_is_publishable", 1) => PgType::Bool,
        ("chr" | "upper", 1) => PgType::Text,
        ("quote_ident" | "quote_literal" | "pg_tablespace_location" | "pg_get_partkeydef", 1) => {
            PgType::Text
        }
        ("array_upper" | "array_lower" | "array_length", 2) | ("cardinality" | "length", 1) => {
            PgType::Int4
        }
        ("nullif", 2) => a[0].ty,
        ("coalesce", n) if n > 0 => a[0].ty,
        ("has_table_privilege", 2 | 3) | ("has_column_privilege", 3 | 4) => PgType::Bool,
        (
            "has_schema_privilege"
            | "has_type_privilege"
            | "has_database_privilege",
            2,
        )
        | ("pg_table_is_visible" | "pg_type_is_visible", 1) => PgType::Bool,
        _ => return Err(Error::Unsupported),
    })
}
fn find_expand(e: &E, out: &mut Option<Vec<E>>) -> R<()> {
    match &e.kind {
        K::Expand(a) | K::Unnest(a) => {
            let inputs = out.get_or_insert_with(Vec::new);
            if !inputs.contains(a.as_ref()) {
                inputs.push(*a.clone());
            }
        }
        K::Member(a, _) | K::Cast(a, _) | K::IntegerOid(a, _) | K::Not(a) | K::Neg(a) => {
            find_expand(a, out)?
        }
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
        K::Binary(a, _, b) | K::Distinct(a, b, _) | K::Any(a, _, b) | K::Index(a, b) => vec![a, b],
        K::Not(a)
        | K::Neg(a)
        | K::Is(a, _, _)
        | K::Cast(a, _)
        | K::IntegerOid(a, _)
        | K::Regclass(a)
        | K::Regproc(a)
        | K::Regoper(a)
        | K::RegisteredDisplay(a)
        | K::Member(a, _)
        | K::Expand(a)
        | K::Unnest(a) => vec![a],
        K::InQuery(a, _, _) => vec![a],
        K::Count(arg) => arg.iter().map(|a| a.as_ref()).collect(),
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
    matches!(e.kind, K::Expand(_) | K::Unnest(_)) || children(e).into_iter().any(has_expand)
}
fn no_set_or_window(e: &E) -> R<()> {
    if matches!(
        e.kind,
        K::Expand(_) | K::Unnest(_) | K::RowNumber(_, _) | K::ArrayAgg(..) | K::Count(_)
    ) {
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
        if let K::Subquery(q) | K::Exists(q, _) | K::ArrayQuery(q) | K::InQuery(_, q, _) = &e.kind {
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
            Source::Filtered(input, filter) => {
                source(input, types)?;
                expr(filter, types, None)?;
            }
            Source::Derived(q) | Source::Cte(q) => collect_parameters(q, types)?,
            Source::FlatUnion(arms) => {
                for arm in arms {
                    collect_parameters(arm, types)?;
                }
            }
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
            Source::Unnest(e, _) | Source::Options(e) | Source::Scalar(e) => expr(e, types, None)?,
            Source::ZipUnnest(es, _) => {
                for e in es {
                    expr(e, types, None)?;
                }
            }
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
    snapshot: &'a CatalogSnapshot,
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
                let mut rows = r.rows.clone();
                if let Some(oid) = catalog::tableoid(n)
                    .or_else(|| self.snapshot.user_relations.contains_key(n).then_some(0))
                {
                    for row in &mut rows {
                        row.push(Value::Int(i64::from(oid)));
                    }
                }
                Ok(rows)
            }
            Source::Derived(q) => self.query(q, outer),
            Source::Cte(q) => self.query(
                q,
                outer
                    .get(outer.len().checked_sub(q.outer_width).ok_or(Error::Type)?..)
                    .ok_or(Error::Type)?,
            ),
            Source::FlatUnion(arms) => flat_union::execute(self, arms, outer),
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
                let start = integer(self.eval(a, outer, None, 0, &[], None)?)?;
                let end = integer(self.eval(b, outer, None, 0, &[], None)?)?;
                if end.saturating_sub(start) > 1024 {
                    return Err(Error::Limit);
                }
                Ok((start..=end).map(|i| vec![Value::Int(i)]).collect())
            }
            Source::Scalar(e) => Ok(vec![vec![self.eval(e, outer, None, 0, &[], None)?]]),
            Source::Filtered(input, filter) => {
                let rows = self.source(input, outer)?;
                let mut out = vec![];
                let mut bytes = 0;
                for row in rows {
                    if boolean(self.eval(filter, &row, None, 0, &[], None)?)? == Some(true) {
                        admit(&out, &row, &mut bytes)?;
                        out.push(row);
                    }
                }
                Ok(out)
            }
            Source::Options(e) => match self.eval(e, outer, None, 0, &[], None)? {
                Value::Null => Ok(vec![]),
                Value::Array(values) => {
                    let mut rows = Vec::new();
                    let mut bytes = 0;
                    for v in values {
                        let Value::Text(option) = v else {
                            return Err(Error::Unsupported);
                        };
                        self.charge(option.len().saturating_add(1))?;
                        let (name, value) = option.split_once('=').unwrap_or((option.as_str(), ""));
                        let row = vec![Value::Text(name.into()), Value::Text(value.into())];
                        admit(&rows, &row, &mut bytes)?;
                        rows.push(row);
                    }
                    Ok(rows)
                }
                _ => Err(Error::Type),
            },
            Source::ZipUnnest(es, ordinality) => {
                let arrays = es.iter().map(|e| {
                    match self.eval(e, outer, None, 0, &[], None)? {
                        Value::Null => Ok(vec![]),
                        Value::Array(values) => Ok(values),
                        _ => Err(Error::Type),
                    }
                }).collect::<R<Vec<_>>>()?;
                let len = arrays.iter().map(Vec::len).max().unwrap_or(0);
                let mut out = vec![];
                let mut bytes = 0;
                for i in 0..len {
                    self.charge(arrays.len())?;
                    let mut row = arrays.iter().map(|a| a.get(i).cloned().unwrap_or(Value::Null))
                        .collect::<Vec<_>>();
                    if *ordinality {
                        row.push(Value::Int(i as i64 + 1));
                    }
                    admit(&out, &row, &mut bytes)?;
                    out.push(row);
                }
                Ok(out)
            }
            Source::Unnest(e, ordinality) => match self.eval(e, outer, None, 0, &[], None)? {
                Value::Null => Ok(vec![]),
                Value::Array(values) => {
                    let mut out = vec![];
                    let mut bytes = 0;
                    for (i, v) in values.into_iter().enumerate() {
                        self.charge(1)?;
                        // FROM aclexplode exposes the existing evaluator's four
                        // record members as columns; ordinary unnest stays scalar.
                        let mut row = if matches!(&e.kind, K::Call(n, _) if n == "_aclexplode") {
                            let Value::Record(row) = v else {
                                return Err(Error::Type);
                            };
                            if row.len() != 4 {
                                return Err(Error::Type);
                            }
                            row
                        } else {
                            vec![v]
                        };
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
                // Borrow keys and row positions rather than cloning right rows.
                // Buckets retain source order and duplicate keys; the full ON
                // still decides whether each candidate survives admission.
                let indexed = if !*lateral {
                    ls.first().and_then(|row| {
                        join_selection::equality_key(on, row.len() + rwidth, *rwidth)
                    })
                } else {
                    None
                };
                let mut index = BTreeMap::<&Value, Vec<usize>>::new();
                if let Some((_, key)) = indexed {
                    self.charge(rs.len())?;
                    for (position, row) in rs.iter().enumerate() {
                        let value = row.get(key).ok_or(Error::Type)?;
                        if *value != Value::Null {
                            index.entry(value).or_default().push(position);
                        }
                    }
                }
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
                    let candidates: Box<dyn Iterator<Item = &Vec<Value>>> =
                        if let Some((key, _)) = indexed {
                            self.charge(1)?;
                            let key = l.get(key).ok_or(Error::Type)?;
                            Box::new(
                                index
                                    .get(key)
                                    .into_iter()
                                    .flatten()
                                    .map(|position| &rs[*position]),
                            )
                        } else {
                            Box::new(rs.iter())
                        };
                    let mut matched = false;
                    for r in candidates {
                        self.charge(1)?;
                        // Candidate is admitted before cloning; it is released before the next candidate.
                        let size = l.iter().chain(r).chain(outer).map(Value::bytes).sum::<usize>();
                        if size > MAX_BYTES {
                            return Err(Error::Limit);
                        }
                        let mut row = l.clone();
                        row.extend(r.iter().cloned());
                        let width = row.len();
                        row.extend_from_slice(outer);
                        let keep = boolean(self.eval(on, &row, None, 0, &[], None)?)? == Some(true);
                        row.truncate(width);
                        if keep {
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
        // FlatUnion is constructed only by flat_union::plan as a private
        // identity wrapper. Validate that invariant before bypassing projection.
        if let Source::FlatUnion(_) = &q.source {
            flat_union::validate_wrapper(q)?;
            return self.source(&q.source, outer);
        }
        let (bound, work) = join_selection::bind_outer(q, outer);
        self.charge(work)?;
        let (input, filter) = bound.as_ref().map_or((&q.source, &q.filter), |(source,filter)| (source,filter));
        let source = self.source(input, outer)?;
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
            if boolean(self.eval(filter, &row, None, 0, &[], None)?)? == Some(true) {
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
                let mut expansions = vec![];
                let mut expansion_bytes = 0;
                for e in q.expand.iter().flatten() {
                    let values =
                        match self.eval(e, row, None, 0, &[], Some(MAX_BYTES - expansion_bytes))? {
                            Value::Array(v) => v,
                            Value::Null => vec![],
                            _ => return Err(Error::Type),
                        };
                    expansion_bytes += values.iter().map(Value::bytes).sum::<usize>();
                    expansions.push((e, values));
                }
                let count = if q.expand.is_some() {
                    expansions.iter().map(|(_, v)| v.len()).max().unwrap_or(0)
                } else {
                    1
                };
                for ei in 0..count {
                    // PostgreSQL zips target-list SRFs, padding shorter inputs with NULL.
                    let expanded = expansions
                        .iter()
                        .map(|(e, values)| {
                            (
                                *e,
                                values
                                    .get(ei)
                                    .map(|value| {
                                        Value::Record(vec![
                                            value.clone(),
                                            Value::Int(ei as i64 + 1),
                                        ])
                                    })
                                    .unwrap_or(Value::Null),
                            )
                        })
                        .collect::<Vec<_>>();
                    let mut result = vec![];
                    let mut keys = vec![];
                    for e in &q.projection {
                        let v = self.eval(
                            e,
                            row,
                            Some(&expanded),
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
                                Some(&expanded),
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
            if !q.distinct || seen.insert(if q.distinct_on.is_empty() {
                r.clone()
            } else {
                q.distinct_on.iter().map(|i| r[*i].clone()).collect()
            }) {
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
        expanded: Option<&[(&E, Value)]>,
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
                if matches!(
                    op,
                    Op::PGRegexMatch
                        | Op::PGRegexNotMatch
                        | Op::PGRegexIMatch
                        | Op::PGRegexNotIMatch
                ) {
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
            K::IntegerOid(e, source) => integer_oid(ev(e)?, *source)?,
            K::Regclass(e) => match ev(e)? {
                N => N,
                S(s) => I(regclass_oid(&s, self.snapshot)? as i64),
                I(i) => I(i),
                _ => return Err(Error::Type),
            },
            K::Regproc(e) => match ev(e)? {
                N => N,
                S(s) if s == "pg_catalog.array_in" || s == "array_in" => I(750),
                _ => return Err(Error::Unsupported),
            },
            K::RegisteredDisplay(e) => match ev(e)? {
                N => N,
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
                    let v = self.eval(
                        e,
                        row,
                        expanded,
                        idx,
                        all,
                        if n == "encode" { None } else { remaining },
                    )?;
                    bytes = bytes.checked_add(v.bytes()).ok_or(Error::Limit)?;
                    if bytes > MAX_BYTES {
                        return Err(Error::Limit);
                    }
                    values.push(v)
                }
                let value = if matches!(n.as_str(), "upper" | "translate") {
                    if values.contains(&N) {
                        N
                    } else {
                        // Borrow the already budgeted input Values: no sizing clones.
                        let S(input) = &values[0] else {
                            return Err(Error::Type);
                        };
                        let (from, to) = if n == "translate" {
                            let (S(from), S(to)) = (&values[1], &values[2]) else {
                                return Err(Error::Type);
                            };
                            (from.as_str(), to.as_str())
                        } else {
                            ("", "")
                        };
                        let budget = remaining.unwrap_or(MAX_BYTES).min(MAX_BYTES);
                        let mut output = String::new();
                        let mut length = 0usize;
                        // Pass one computes exact UTF-8 bytes and pays for both
                        // passes before reserving any output. No mapping allocation.
                        for sizing in [true, false] {
                            for c in input.chars() {
                                if sizing {
                                    self.charge(2)?;
                                }
                                if n == "upper" {
                                    // Locale-independent Unicode mapping, not PG
                                    // database-locale/collation equivalence.
                                    for mapped in c.to_uppercase() {
                                        if sizing {
                                            self.charge(2)?;
                                            length = length
                                                .checked_add(mapped.len_utf8())
                                                .ok_or(Error::Limit)?;
                                        } else {
                                            output.push(mapped);
                                        }
                                    }
                                } else {
                                    let mut mapped = Some(c);
                                    let mut replacements = to.chars();
                                    for source in from.chars() {
                                        if sizing {
                                            self.charge(2)?;
                                        }
                                        let replacement = replacements.next();
                                        if source == c {
                                            // First duplicate wins; missing to
                                            // characters delete, surplus ignored.
                                            mapped = replacement;
                                            break;
                                        }
                                    }
                                    if let Some(mapped) = mapped {
                                        if sizing {
                                            length = length
                                                .checked_add(mapped.len_utf8())
                                                .ok_or(Error::Limit)?;
                                        } else {
                                            output.push(mapped);
                                        }
                                    }
                                }
                                if sizing
                                    && length
                                        .checked_mul(2)
                                        .and_then(|v| v.checked_add(8))
                                        .ok_or(Error::Limit)?
                                        > budget
                                {
                                    return Err(Error::Limit);
                                }
                            }
                            if sizing {
                                if length
                                    .checked_mul(2)
                                    .and_then(|v| v.checked_add(8))
                                    .ok_or(Error::Limit)?
                                    > budget
                                {
                                    return Err(Error::Limit);
                                }
                                output.try_reserve_exact(length).map_err(|_| Error::Limit)?;
                            }
                        }
                        S(output)
                    }
                } else {
                    self.call(n, values, remaining)?
                };
                if matches!(n.as_str(), "array_upper" | "array_lower" | "array_position")
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
                if a.ty == PgType::Name {
                    return match (ev(a)?, ev(i)?) {
                        (N, _) | (_, N) => Ok(N),
                        (S(name), I(index)) => {
                            // PostgreSQL name subscripts address its fixed 64-byte
                            // buffer, including zero padding; outside it is char zero.
                            let byte = usize::try_from(index)
                                .ok()
                                .filter(|i| *i < 64)
                                .and_then(|i| name.as_bytes().get(i))
                                .copied()
                                .unwrap_or(0);
                            if !byte.is_ascii() {
                                return Err(Error::Unsupported);
                            }
                            Ok(S(if byte == 0 {
                                String::new()
                            } else {
                                char::from(byte).to_string()
                            }))
                        }
                        _ => Err(Error::Type),
                    };
                }
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
            K::InQuery(lhs, q, negated) => {
                let lhs = ev(lhs)?;
                let rows = self.query(q, row)?;
                let mut out = B(false);
                for candidate in &rows {
                    let rhs = &candidate[0];
                    // Account for the bytes inspected before borrowed equality.
                    self.charge(lhs.bytes().saturating_add(rhs.bytes()).saturating_add(1))?;
                    match (&lhs, rhs) {
                        (N, _) | (_, N) => out = N,
                        (B(l), B(r)) if l == r => {
                            out = B(true);
                            break;
                        }
                        (I(l), I(r)) if l == r => {
                            out = B(true);
                            break;
                        }
                        (S(l), S(r)) if l == r => {
                            out = B(true);
                            break;
                        }
                        (B(_), B(_)) | (I(_), I(_)) | (S(_), S(_)) => {}
                        _ => return Err(Error::Type),
                    }
                }
                match out {
                    B(b) => B(b ^ *negated),
                    _ => N,
                }
            }
            K::Exists(q, negated) => {
                // Keep original expressions in the plan for parameter typing,
                // but EXISTS never evaluates its target values or sort keys.
                let mut q = q.as_ref().clone();
                q.projection = vec![literal(Value::Int(1), PgType::Int4)];
                q.fields = vec![Field {
                    name: "exists".into(),
                    data_type: PgType::Int4,
                }];
                q.order.clear();
                q.distinct_on.clear();
                q.limit = q.limit.min(1);
                B(!self.query(&q, row)?.is_empty() ^ *negated)
            }
            K::Distinct(l, r, negated) => {
                let l = ev(l)?;
                let r = ev(r)?;
                self.charge(l.bytes().saturating_add(r.bytes()).saturating_add(1))?;
                B((l != r) ^ *negated)
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
            K::Count(arg) => {
                self.charge(all.len())?;
                let mut count = 0i64;
                for row in all {
                    if let Some(arg) = arg
                        && self.eval(arg, row, None, 0, &[], None)? == N
                    {
                        continue;
                    }
                    count += 1;
                }
                I(count)
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
            K::Expand(input) => expanded
                .and_then(|values| values.iter().find(|(e, _)| *e == input.as_ref()))
                .map(|(_, v)| v.clone())
                .ok_or(Error::Unsupported)?,
            K::Unnest(input) => {
                let value = match expanded
                    .and_then(|values| values.iter().find(|(e, _)| *e == input.as_ref()))
                    .map(|(_, v)| v)
                {
                    Some(N) => &N,
                    Some(Value::Record(fields)) => fields.first().ok_or(Error::Unsupported)?,
                    _ => return Err(Error::Unsupported),
                };
                if value.bytes() > remaining.unwrap_or(MAX_BYTES) {
                    return Err(Error::Limit);
                }
                self.charge(value.bytes() / 64 + 1)?;
                value.clone()
            }
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
    fn acl_allows(&mut self, acl: Value, role: i64, requested: &[(String, bool)]) -> R<bool> {
        let Value::Array(entries) = self.call("_aclexplode", vec![acl], None)? else {
            return Err(Error::Type);
        };
        self.charge(entries.len().saturating_mul(requested.len()))?;
        Ok(entries.iter().any(|entry| match entry {
            Value::Record(row) => {
                (row[1] == Value::Int(role) || row[1] == Value::Int(0))
                    && requested.iter().any(|(name, grant)| {
                        row[2] == Value::Text(name.clone())
                            && (!grant || row[3] == Value::Bool(true))
                    })
            }
            _ => false,
        }))
    }
    fn privilege_function(&mut self, name: &str, values: &[Value]) -> R<Value> {
        use Value::{Bool as B, Int as I, Null as N, Text as S};
        let column = name == "has_column_privilege";
        let explicit_role = values.len() == if column { 4 } else { 3 };
        let table_at = usize::from(explicit_role);
        let privilege_list = text(values.last().unwrap().clone())?;
        let mut requested = vec![];
        for privilege in privilege_list.split(',') {
            let normalized = privilege.trim().to_ascii_uppercase();
            let (name, grant) = normalized
                .strip_suffix(" WITH GRANT OPTION")
                .map_or((normalized.as_str(), false), |n| (n.trim_end(), true));
            let valid = if column {
                matches!(name, "SELECT" | "INSERT" | "UPDATE" | "REFERENCES")
            } else {
                matches!(
                    name,
                    "SELECT"
                        | "INSERT"
                        | "UPDATE"
                        | "DELETE"
                        | "TRUNCATE"
                        | "REFERENCES"
                        | "TRIGGER"
                        | "MAINTAIN"
                )
            };
            if !valid {
                return Err(Error::InvalidPrivilege(format!(
                    "unrecognized privilege type: \"{}\"",
                    privilege.trim()
                )));
            }
            requested.push((name.to_owned(), grant));
        }
        let role_value = if explicit_role {
            values[0].clone()
        } else {
            S(self.snapshot.user.clone())
        };
        self.charge(self.snapshot.roles.len())?;
        let role = match &role_value {
            S(name) if name == "public" => 0,
            S(name) => self
                .snapshot
                .roles
                .iter()
                .find(|r| r.name == *name)
                .map(|r| i64::from(r.oid))
                .ok_or(Error::Unsupported)?,
            I(oid) => *oid,
            _ => return Err(Error::Type),
        };
        let table_value = &values[table_at];
        let oid = match table_value {
            S(name) => match regclass_oid(name, self.snapshot) {
                Ok(oid) => i64::from(oid),
                Err(Error::Unsupported) => return Ok(B(false)),
                Err(e) => return Err(e),
            },
            I(oid) => *oid,
            _ => return Err(Error::Type),
        };
        let relations = &self.catalogs["pg_class"];
        self.charge(relations.rows.len())?;
        let Some(relation) = relations.rows.iter().find(|r| r[0] == I(oid)).cloned() else {
            return Ok(if matches!(table_value, S(_)) {
                B(false)
            } else {
                N
            });
        };
        let owner_index = relations
            .fields
            .iter()
            .position(|f| f.name == "relowner")
            .unwrap();
        let acl_index = relations
            .fields
            .iter()
            .position(|f| f.name == "relacl")
            .unwrap();
        let owner = relation[owner_index].clone();
        let mut column_acl = N;
        if column {
            let attributes = &self.catalogs["pg_attribute"];
            let at = |name: &str| {
                attributes
                    .fields
                    .iter()
                    .position(|f| f.name == name)
                    .unwrap()
            };
            let num = at("attnum");
            let dropped = at("attisdropped");
            let column_value = &values[table_at + 1];
            self.charge(attributes.rows.len())?;
            let attribute = attributes.rows.iter().find(|r| {
                r[0] == I(oid)
                    && r[dropped] == B(false)
                    && match column_value {
                        S(name) => r[1] == S(name.clone()),
                        I(n) => r[num] == I(*n),
                        _ => false,
                    }
            });
            let Some(attribute) = attribute else {
                return if matches!(column_value, S(_)) {
                    Err(Error::UnknownColumn)
                } else {
                    Ok(N)
                };
            };
            column_acl = attribute[at("attacl")].clone();
        }
        if owner == I(role)
            || self
                .snapshot
                .roles
                .iter()
                .any(|r| i64::from(r.oid) == role && r.superuser)
        {
            return Ok(B(true));
        }
        let acl = if relation[acl_index] == N {
            self.call("acldefault", vec![S("r".into()), owner], None)?
        } else {
            relation[acl_index].clone()
        };
        Ok(B(self.acl_allows(acl, role, &requested)?
            || (column
                && self.acl_allows(column_acl, role, &requested)?)))
    }
    fn call(&mut self, n: &str, v: Vec<Value>, remaining: Option<usize>) -> R<Value> {
        use Value::{Bool as B, Int as I, Null as N, Text as S};
        self.charge(v.iter().map(Value::bytes).sum::<usize>() / 64 + 1)?;
        if matches!(n, "_array_subscripts" | "_vector_subscripts") {
            return subscripts::evaluate(self, n, v, remaining);
        }
        if n == "_regcollation_display" {
            if v[0] == N {return Ok(N);}
            if v[0] == I(0) {return Ok(S("-".into()));}
            let collations = &self.catalogs["pg_collation"].rows;
            let namespaces = &self.catalogs["pg_namespace"].rows;
            let mut path = self.snapshot.search_path.clone();
            if !path.iter().any(|n| n == "pg_catalog") {path.insert(0,"pg_catalog".into());}
            self.charge((path.len()+1).saturating_mul(collations.len()+namespaces.len()))?;
            let Some(collation) = collations.iter().find(|r|r[0]==v[0]) else {
                return Ok(S(integer(v[0].clone())?.to_string()));
            };
            let name = catalog::quote_identifier(&text(collation[1].clone())?);
            let visible = path.iter().filter_map(|name| namespaces.iter().find(|r|r[1]==S(name.clone())))
                .find_map(|namespace|collations.iter().find(|r|r[1]==collation[1] && r[2]==namespace[0]))
                .is_some_and(|first|first[0]==v[0]);
            if visible {return Ok(S(name));}
            let schema = namespaces.iter().find(|r|r[0]==collation[2]).ok_or(Error::Type)?;
            return Ok(S(format!("{}.{}",catalog::quote_identifier(&text(schema[1].clone())?),name)));
        }
        if n == "_regclass_display" {
            if v[0] == N { return Ok(N); }
            self.charge(self.catalogs["pg_class"].rows.len() + self.catalogs["pg_namespace"].rows.len())?;
            let Some(relation) = self.catalogs["pg_class"].rows.iter().find(|r| r[0] == v[0]) else {
                return Ok(S(integer(v[0].clone())?.to_string()));
            };
            let name = catalog::quote_identifier(&text(relation[1].clone())?);
            let namespace = self.catalogs["pg_namespace"].rows.iter().find(|r| r[0] == relation[2]).ok_or(Error::Type)?;
            let schema = catalog::quote_identifier(&text(namespace[1].clone())?);
            let visible = self.call("pg_table_is_visible", vec![v[0].clone()], remaining)? == B(true);
            return Ok(S(if visible { name } else { format!("{schema}.{name}") }));
        }
        if matches!(n, "like" | "ilike") {
            self.charge(v[0].bytes() + v[1].bytes().saturating_mul(64))?;
        }
        if n == "coalesce" {
            return Ok(v.into_iter().find(|v| *v != N).unwrap_or(N));
        }
        if n == "greatest" {
            return Ok(v.into_iter().filter(|v| *v != N).max().unwrap_or(N));
        }
        if n == "nullif" {
            return Ok(if v[0] == v[1] { N } else { v[0].clone() });
        }
        if n == "concat" {
            let mut output = String::new();
            for value in v.into_iter().filter(|v| *v != N) {
                let value = text(value)?;
                if output.len().saturating_add(value.len()) > remaining.unwrap_or(MAX_BYTES) {
                    return Err(Error::Limit);
                }
                self.charge(value.len())?;
                output.push_str(&value);
            }
            return Ok(S(output));
        }
        if n == "_string_agg" {
            let Value::Array(values) = &v[0] else {
                return Ok(N);
            };
            if values.iter().all(|v| *v == N) {
                return Ok(N);
            }
        }
        if n == "_aclexplode" && v[0] == N {
            return Ok(Value::Array(vec![]));
        }
        if v.contains(&N)
            && !(n == "format_type" && v[0] != N)
            && !(n == "array_to_string" && v[0] != N && v[1] != N)
            && n != "_string_agg"
            && n != "array_remove"
            && n != "array_position"
        {
            return Ok(N);
        }
        Ok(match n {
            "substring" => {
                let value = text(v[0].clone())?;
                let start = integer(v[1].clone())?;
                let begin = start.saturating_sub(1).max(0) as usize;
                let end = if v.len() == 3 {
                    let length = integer(v[2].clone())?;
                    if length < 0 {
                        return Err(Error::Unsupported);
                    }
                    start.saturating_add(length).saturating_sub(1).max(0) as usize
                } else {
                    value.chars().count()
                };
                self.charge(value.len())?;
                S(value
                    .chars()
                    .skip(begin)
                    .take(end.saturating_sub(begin))
                    .collect())
            }
            "_substring_regex" => {
                let value = text(v[0].clone())?;
                let pattern = text(v[1].clone())?;
                validate_pg_regex(&pattern)?;
                if pattern.contains('|') {
                    return Err(Error::Unsupported);
                }
                self.charge(value.len() + pattern.len().saturating_mul(64))?;
                let regex = regex::RegexBuilder::new(&pattern)
                    .dot_matches_new_line(true)
                    .size_limit(MAX_BYTES)
                    .build()
                    .map_err(|_| Error::Unsupported)?;
                regex
                    .captures(&value)
                    .and_then(|c| c.get(usize::from(regex.captures_len() > 1)))
                    .map(|m| S(m.as_str().into()))
                    .unwrap_or(N)
            }
            "_series" => {
                let start = integer(v[0].clone())?;
                let end = integer(v[1].clone())?;
                if end.saturating_sub(start) > 1024 {
                    return Err(Error::Limit);
                }
                self.charge(end.saturating_sub(start).max(0) as usize + 1)?;
                Value::Array((start..=end).map(I).collect())
            }
            "to_regtype" | "_regtype" => {
                let value = self.regtype(&text(v[0].clone())?)?;
                if value == N && n == "_regtype" {
                    return Err(Error::Unsupported);
                }
                value
            }
            "_regtype_display" => {
                let oid = integer(v[0].clone())?;
                let rows = &self.catalogs["pg_type"].rows;
                self.charge(rows.len())?;
                S(if rows.iter().any(|r| r[0] == I(oid)) {
                    format_type(oid, N, rows)?
                } else {
                    oid.to_string()
                })
            }
            "pg_backend_pid" => I(i64::from(self.snapshot.backend_pid)),
            "age" => {
                let xid = u32::try_from(integer(v[0].clone())?).map_err(|_| Error::Type)?;
                I(i64::from(
                    (self.snapshot.generation as u32).wrapping_sub(xid) as i32,
                ))
            }
            "array_remove" => match &v[0] {
                N => N,
                Value::Array(elements) => {
                    self.charge(elements.len())?;
                    Value::Array(elements.iter().filter(|e| **e != v[1]).cloned().collect())
                }
                _ => return Err(Error::Type),
            },
            "_aclexplode" => {
                let Value::Array(items) = &v[0] else {
                    return Err(Error::Type);
                };
                let roles = &self.catalogs["pg_authid"].rows;
                let mut result = vec![];
                for item in items {
                    let S(item) = item else {
                        return Err(Error::Type);
                    };
                    self.charge(item.len() + roles.len())?;
                    // Match encoded role names, so quoted '=', '/' and doubled
                    // quotes in identifiers cannot be mistaken for separators.
                    let (grantor, acl) = roles
                        .iter()
                        .find_map(|role| {
                            let S(name) = &role[1] else { return None };
                            item.strip_suffix(&format!("/{}", acl_role_name(name)))
                                .map(|acl| (role[0].clone(), acl))
                        })
                        .ok_or(Error::Unsupported)?;
                    let (grantee, privileges) = if let Some(acl) = acl.strip_prefix('=') {
                        (I(0), acl)
                    } else {
                        roles
                            .iter()
                            .find_map(|role| {
                                let S(name) = &role[1] else { return None };
                                acl.strip_prefix(&format!("{}=", acl_role_name(name)))
                                    .map(|privileges| (role[0].clone(), privileges))
                            })
                            .ok_or(Error::Unsupported)?
                    };
                    let mut chars = privileges.chars().peekable();
                    while let Some(privilege) = chars.next() {
                        let name = match privilege {
                            'a' => "INSERT",
                            'r' => "SELECT",
                            'w' => "UPDATE",
                            'd' => "DELETE",
                            'D' => "TRUNCATE",
                            'x' => "REFERENCES",
                            't' => "TRIGGER",
                            'm' => "MAINTAIN",
                            'X' => "EXECUTE",
                            'U' => "USAGE",
                            'C' => "CREATE",
                            'c' => "CONNECT",
                            'T' => "TEMPORARY",
                            _ => return Err(Error::Unsupported),
                        };
                        let grantable = chars.next_if_eq(&'*').is_some();
                        self.charge(1)?;
                        if result.len() >= MAX_ROWS {
                            return Err(Error::Limit);
                        }
                        result.push(Value::Record(vec![
                            grantor.clone(),
                            grantee.clone(),
                            S(name.into()),
                            B(grantable),
                        ]));
                    }
                }
                Value::Array(result)
            }
            "acldefault" => {
                // Defaults describe the object owner's initial rights, independently
                // of the object's explicit ACL, if supplied in the snapshot.
                let privileges = match text(v[0].clone())?.as_str() {
                    "n" => "UC",
                    "r" => "arwdDxtm",
                    "s" => "rwU",
                    "f" => "X",
                    "T" | "l" | "F" | "S" => "U",
                    _ => return Err(Error::Unsupported),
                };
                let oid = integer(v[1].clone())?;
                self.charge(self.snapshot.roles.len() + 16)?;
                let owner = catalog::role_name(self.snapshot, oid)
                    .map(acl_role_name)
                    .unwrap_or_else(|| oid.to_string());
                let grantee = if oid == 0 { "" } else { &owner };
                let mut acl = vec![S(format!("{grantee}={privileges}/{owner}"))];
                if matches!(text(v[0].clone())?.as_str(), "f" | "T" | "l") {
                    acl.insert(0, S(format!("={privileges}/{owner}")));
                }
                Value::Array(acl)
            }
            "encode" => {
                let (Value::Bytes(input), S(format)) = (&v[0], &v[1]) else {
                    return Err(Error::Type);
                };
                if !format.eq_ignore_ascii_case("escape") {
                    return Err(Error::Unsupported);
                }
                // Pay for sizing traversal before inspecting borrowed bytes.
                self.charge(input.len())?;
                let length = input.iter().try_fold(0usize, |length, b| {
                    length
                        .checked_add(match b {
                            0 | 128..=255 => 4,
                            b'\\' => 2,
                            _ => 1,
                        })
                        .ok_or(Error::Limit)
                })?;
                let size = length
                    .checked_mul(2)
                    .and_then(|n| n.checked_add(8))
                    .ok_or(Error::Limit)?;
                if size > remaining.unwrap_or(MAX_BYTES).min(MAX_BYTES) {
                    return Err(Error::Limit);
                }
                // Pay for emission and output before the sole output allocation.
                self.charge(input.len().checked_add(length).ok_or(Error::Limit)?)?;
                let mut output = String::new();
                output.try_reserve_exact(length).map_err(|_| Error::Limit)?;
                for &b in input {
                    match b {
                        0 | 128..=255 => {
                            output.push('\\');
                            output.push(char::from(b'0' + (b >> 6)));
                            output.push(char::from(b'0' + ((b >> 3) & 7)));
                            output.push(char::from(b'0' + (b & 7)));
                        }
                        b'\\' => output.push_str("\\\\"),
                        _ => output.push(char::from(b)),
                    }
                }
                S(output)
            }
            "version" => S(concat!(
                "PostgreSQL 18.6 (GigglesDB ",
                env!("CARGO_PKG_VERSION"),
                "+",
                env!("GIGGLES_BUILD_ID"),
                ")"
            )
            .into()),
            // A standalone GigglesDB server is always a primary, never a
            // standby replaying WAL.
            "pg_is_in_recovery" => B(false),
            "txid_current" => I(i64::from(self.snapshot.generation as u32)),
            "_service_start_epoch" => {
                I(self.startup.filter(|v| *v >= 0).ok_or(Error::Unsupported)?)
            }
            "current_database" | "current_catalog" => S(self.snapshot.database.clone()),
            "current_user" | "session_user" | "current_role" | "user" => {
                S(self.snapshot.user.clone())
            }
            "current_schema" => self
                .snapshot
                .search_path
                .first()
                .map(|s| S(s.clone()))
                .unwrap_or(N),
            "current_schemas" => {
                let b = boolean(v[0].clone())?.ok_or(Error::Type)?;
                let mut path = self.snapshot.search_path.clone();
                if b && !path.iter().any(|s| s == "pg_catalog") {
                    path.insert(0, "pg_catalog".into());
                }
                Value::Array(path.into_iter().map(S).collect())
            }
            "split_part" => {
                let s = text(v[0].clone())?;
                let delimiter = text(v[1].clone())?;
                let index = integer(v[2].clone())?;
                if index == 0 { return Err(Error::Unsupported); }
                let parts = if delimiter.is_empty() { vec![s.as_str()] } else { s.split(&delimiter).collect::<Vec<_>>() };
                let at = if index > 0 { index - 1 } else { parts.len() as i64 + index };
                S(usize::try_from(at).ok().and_then(|at| parts.get(at)).unwrap_or(&"").to_string())
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
            "pg_encoding_to_char" => S(match integer(v[0].clone())? {
                0 => "SQL_ASCII",
                6 => "UTF8",
                _ => "",
            }
            .into()),
            "array_to_string" | "_string_agg" => {
                let Value::Array(values) = &v[0] else {
                    return Err(Error::Type);
                };
                let delimiter = if v[1] == N && n == "_string_agg" {
                    String::new()
                } else {
                    text(v[1].clone())?
                };
                let replacement = v
                    .get(2)
                    .filter(|v| **v != N)
                    .map(|v| text(v.clone()))
                    .transpose()?;
                let mut output = String::new();
                let mut emitted = false;
                for value in values {
                    let item = match value {
                        N => match &replacement {
                            Some(s) => s.clone(),
                            None => continue,
                        },
                        S(s) => s.clone(),
                        I(i) => i.to_string(),
                        B(b) => if *b { "t" } else { "f" }.into(),
                        _ => return Err(Error::Unsupported),
                    };
                    let separator = if emitted { &delimiter } else { "" };
                    emitted = true;
                    let size = output
                        .len()
                        .checked_add(separator.len())
                        .and_then(|s| s.checked_add(item.len()))
                        .ok_or(Error::Limit)?;
                    if size > MAX_BYTES || size > remaining.unwrap_or(MAX_BYTES) {
                        return Err(Error::Limit);
                    }
                    self.charge(separator.len() + item.len())?;
                    output.push_str(separator);
                    output.push_str(&item);
                }
                S(output)
            }
            "array_length" => match (&v[0], &v[1]) {
                (Value::Array(a), I(1)) if !a.is_empty() => {
                    I(i32::try_from(a.len()).map_err(|_| Error::Limit)? as i64)
                }
                (Value::Array(_), I(_)) => N,
                _ => return Err(Error::Type),
            },
            "array_position" => match &v[0] {
                N => N,
                Value::Array(a) => {
                    self.charge(a.len())?;
                    if a.iter().any(|v| matches!(v, Value::Array(_))) { return Err(Error::Unsupported); }
                    a.iter().position(|x| x == &v[1]).map_or(N, |i| I(i as i64 + 1))
                }
                _ => return Err(Error::Type),
            },
            "cardinality" => match &v[0] {
                Value::Array(a) => I(a.len() as i64),
                _ => return Err(Error::Type),
            },
            "length" => I(text(v[0].clone())?.chars().count() as i64),
            "like" | "ilike" => B(like(
                &text(v[0].clone())?,
                &text(v[1].clone())?,
                &text(v[2].clone())?,
                n == "ilike",
            )?),
            "pg_get_userbyid" => {
                let oid = integer(v[0].clone())?;
                self.charge(self.snapshot.roles.len() + 16)?;
                S(catalog::role_name(self.snapshot, oid)
                    .map(str::to_owned)
                    .unwrap_or_else(|| format!("unknown (OID={oid})")))
            }
            "quote_literal" => {
                let S(input) = &v[0] else {
                    return Err(Error::Type);
                };
                let slash = input.contains('\\');
                let length = input
                    .len()
                    .checked_add(input.bytes().filter(|b| matches!(b, b'\'' | b'\\')).count())
                    .and_then(|n| n.checked_add(2 + usize::from(slash)))
                    .ok_or(Error::Limit)?;
                if length
                    .checked_mul(2)
                    .and_then(|n| n.checked_add(8))
                    .ok_or(Error::Limit)?
                    > remaining.unwrap_or(MAX_BYTES).min(MAX_BYTES)
                {
                    return Err(Error::Limit);
                }
                self.charge(input.len().saturating_add(length))?;
                let mut output = String::new();
                output.try_reserve_exact(length).map_err(|_| Error::Limit)?;
                if slash {
                    output.push('E');
                }
                output.push('\'');
                for c in input.chars() {
                    if matches!(c, '\'' | '\\') {
                        output.push(c);
                    }
                    output.push(c);
                }
                output.push('\'');
                S(output)
            }
            "quote_ident" => {
                let name = text(v[0].clone())?;
                // Always quoting is valid identifier SQL and avoids a keyword list.
                S(format!("\"{}\"", name.replace('"', "\"\"")))
            }
            "pg_tablespace_location" => {
                if matches!(v[0], I(1663 | 1664)) {
                    S(String::new())
                } else {
                    N
                }
            }
            "pg_get_partkeydef" => N,
            "btrim" | "ltrim" | "rtrim" => {
                let input = text(v[0].clone())?;
                let characters = text(v[1].clone())?;
                self.charge(input.len().saturating_mul(characters.len().max(1)))?;
                let predicate = |c| characters.contains(c);
                S(match n {
                    "ltrim" => input.trim_start_matches(predicate),
                    "rtrim" => input.trim_end_matches(predicate),
                    _ => input.trim_matches(predicate),
                }.into())
            }
            "_partition_ancestors" => Value::Array(vec![]),
            "_regnamespace" => self.catalogs["pg_namespace"]
                .rows
                .iter()
                .find(|r| r[0] == v[0])
                .map(|r| r[1].clone())
                .unwrap_or(N),
            "pg_relation_is_publishable" => self.catalogs["pg_class"]
                .rows
                .iter()
                .find(|r| r[0] == v[0])
                .map(|r| {
                    B(r[2] == I(2200) && matches!(&r[17], S(kind) if kind == "r" || kind == "p"))
                })
                .unwrap_or(N),
            "pg_get_statisticsobjdef_columns" => N,
            "_row" => Value::Record(v),
            "pg_indexam_has_property" => N,
            "pg_get_constraintdef" => {
                let oid = integer(v[0].clone())?;
                if let Some(def) = catalog::definition(self.snapshot, oid, v.get(1) == Some(&B(true))) { return Ok(S(def)); }
                self.catalogs["pg_constraint"]
                    .rows
                    .iter()
                    .find(|r| {
                        r[0] == I(oid) && matches!(&r[3], S(kind) if kind == "p" || kind == "n")
                    })
                    .and_then(|constraint| match &constraint[20] {
                        Value::Array(keys) => keys
                            .iter()
                            .map(|key| {
                                self.catalogs["pg_attribute"]
                                    .rows
                                    .iter()
                                    .find(|r| r[0] == constraint[8] && r[4] == *key)
                                    .and_then(|r| match &r[1] {
                                        S(name) => Some(catalog::quote_identifier(name)),
                                        _ => None,
                                    })
                            })
                            .collect::<Option<Vec<_>>>()
                            .map(|keys| (constraint[3] == S("n".into()), keys)),
                        _ => None,
                    })
                    .map(|(not_null, keys)| {
                        if not_null {
                            S(format!("NOT NULL {}", keys.join(", ")))
                        } else {
                            S(format!("PRIMARY KEY ({})", keys.join(", ")))
                        }
                    })
                    .unwrap_or(N)
            }
            "pg_get_indexdef" => {
                let oid = integer(v[0].clone())?;
                let column = if v.len() == 1 {
                    0
                } else {
                    integer(v[1].clone())?
                };
                if column == 0 {
                    let relation = self.catalogs["pg_index"]
                        .rows
                        .iter()
                        .find(|r| r[0] == I(oid))
                        .map(|r| r[1].clone());
                    let visible = if v.len() == 3 && v[2] == B(true) {
                        self.call(
                            "pg_table_is_visible",
                            vec![relation.unwrap_or(N)],
                            remaining,
                        )? == B(true)
                    } else {
                        false
                    };
                    let identity = self.catalogs["pg_class"]
                        .rows
                        .iter()
                        .find(|r| r[0] == I(oid) && r[17] == S("i".into()))
                        .and_then(|r| {
                            self.catalogs["pg_namespace"]
                                .rows
                                .iter()
                                .find(|ns| ns[0] == r[2])
                                .map(|ns| (&r[1], &ns[1]))
                        });
                    self.catalogs["pg_indexes"]
                        .rows
                        .iter()
                        .find(|r| Some((&r[2], &r[0])) == identity)
                        .map(|r| match &r[4] {
                            S(definition) if visible => {
                                let index_name = match &r[2] {
                                    S(name) => catalog::quote_identifier(name),
                                    _ => return N,
                                };
                                let prefix = if definition.starts_with("CREATE UNIQUE INDEX ") {
                                    "CREATE UNIQUE INDEX "
                                } else {
                                    "CREATE INDEX "
                                };
                                // Remove qualification only at the table position; an
                                // index identifier may itself contain " ON public.".
                                let offset = prefix.len() + index_name.len() + " ON ".len();
                                S(format!(
                                    "{}{}",
                                    &definition[..offset],
                                    &definition[offset
                                        + text(r[0].clone())
                                            .map(|schema| catalog::quote_identifier(&schema).len()
                                                + 1)
                                            .unwrap_or(0)..]
                                ))
                            }
                            value => value.clone(),
                        })
                        .unwrap_or(N)
                } else {
                    self.catalogs["pg_attribute"]
                        .rows
                        .iter()
                        .find(|r| {
                            r[0] == I(oid)
                                && r[4] == I(column)
                                && self.catalogs["pg_index"]
                                    .rows
                                    .iter()
                                    .any(|i| i[0] == I(oid))
                        })
                        .map(|r| match &r[1] {
                            S(name) => S(catalog::quote_identifier(name)),
                            _ => N,
                        })
                        .unwrap_or_else(|| {
                            if self.catalogs["pg_index"]
                                .rows
                                .iter()
                                .any(|i| i[0] == I(oid))
                            {
                                S(String::new())
                            } else {
                                N
                            }
                        })
                }
            }
            "pg_get_viewdef" => {
                let oid = match &v[0] { S(name) => i64::from(regclass_oid(name, self.snapshot).map_err(|_| Error::UnknownRelation(name.clone()))?), value => integer(value.clone())? };
                let stored = self.snapshot.views.iter().find(|(id, _)| catalog::table_oid(**id).ok().map(i64::from) == Some(oid));
                match stored {
                    Some((_, sql)) => {
                        let pretty = matches!(v.get(1), Some(B(true) | I(_)));
                        let wrap = match v.get(1) { Some(I(n)) => Some(*n), _ => None };
                        S(viewdef::deparse(sql, self.snapshot, pretty, wrap)?)
                    }
                    None => N,
                }
            }
            "pg_get_ruledef"
            | "pg_get_triggerdef"
            | "pg_get_function_arguments"
            | "pg_get_function_result"
            | "pg_get_function_sqlbody" => return Err(Error::Unsupported),
            "chr" => S(char::from_u32(
                u32::try_from(integer(v[0].clone())?).map_err(|_| Error::Type)?,
            )
            .filter(|c| *c != '\0')
            .ok_or(Error::Type)?
            .to_string()),
            "pg_get_expr" => {
                let known = [("pg_attrdef", 3, 1), ("pg_constraint", 27, 8)].iter().any(|(name, expr, rel)|
                    self.catalogs[*name].rows.iter().any(|r| r[*expr] == v[0] && r[*rel] == v[1]));
                if !known { return Err(Error::Unsupported); }
                if v.get(2) == Some(&B(true)) && let S(expr) = &v[0] {
                    S(expr.strip_prefix('(').and_then(|e| e.strip_suffix(')')).unwrap_or(expr).into())
                } else { v[0].clone() }
            },
            "pg_relation_size" | "pg_total_relation_size" | "pg_table_size" | "pg_indexes_size" | "pg_stat_get_numscans" => {
                if n == "pg_relation_size" && v.get(1).is_some_and(|f| !matches!(f, S(s) if matches!(s.as_str(), "main" | "fsm" | "vm" | "init"))) { return Err(Error::Unsupported); }
                // No PostgreSQL heap/index files or scan counters are maintained.
                // Zero measures those surfaces, not the shared engine's disk use.
                if n == "pg_stat_get_numscans" || self.catalogs["pg_class"].rows.iter().any(|r| r[0] == v[0]) { I(0) } else { N }
            }
            "pg_collation_actual_version" => {
                if let Some(row) = self.catalogs["pg_collation"].rows.iter().find(|r| r[0] == v[0]) {
                    if row[4] == S("i".into()) {
                        let locale = text(row[9].clone())?;
                        let rules = if row[10] == N { None } else { Some(text(row[10].clone())?) };
                        S(giggles_icu::Collator::open(&locale, rules.as_deref()).map_err(|_| Error::Unsupported)?.version())
                    } else if row[4] == S("b".into()) { S("1".into()) }
                    else if row[4] == S("c".into()) && row[7] == S("en_US.utf8".into()) { S("2.41".into()) }
                    else { N }
                } else { N }
            }
            "obj_description" | "col_description" => description(&self.catalogs["pg_description"].rows, n, &v),
            "shobj_description" => {
                let class = catalog::relation_oid(&text(v[1].clone())?)
                    .map(|id| I(i64::from(id)))
                    .unwrap_or(N);
                self.catalogs["pg_shdescription"]
                    .rows
                    .iter()
                    .find(|r| r[0] == v[0] && r[1] == class)
                    .map(|r| r[3].clone())
                    .unwrap_or(N)
            }
            "format_type" => {
                let oid = integer(v[0].clone())?;
                S(format_type(
                    oid,
                    v[1].clone(),
                    &self.catalogs["pg_type"].rows,
                )?)
            }
            "has_database_privilege" => {
                let requested = text(v[1].clone())?.to_ascii_uppercase();
                let privileges = requested.split(',').map(str::trim).collect::<Vec<_>>();
                if privileges
                    .iter()
                    .any(|p| !["CREATE", "CONNECT", "TEMP", "TEMPORARY"].contains(p))
                {
                    return Err(Error::Unsupported);
                }
                let database = self.catalogs["pg_database"]
                    .rows
                    .iter()
                    .find(|r| r[0] == v[0] || r[1] == v[0]);
                match database {
                    None => N,
                    Some(database) => B(privileges.iter().any(|p| *p != "CREATE")
                        || self.snapshot.roles.iter().any(|r| {
                            r.name == self.snapshot.user
                                && (r.superuser || I(i64::from(r.oid)) == database[2])
                        })),
                }
            }
            "has_table_privilege" | "has_column_privilege" => self.privilege_function(n, &v)?,
            "has_schema_privilege" | "has_type_privilege" => {
                let requested = text(v[1].clone())?;
                let allowed = "USAGE";
                let privileges = requested
                    .split(',')
                    .map(str::trim)
                    .map(str::to_ascii_uppercase)
                    .collect::<Vec<_>>();
                let known = if n == "has_schema_privilege" {
                    &["USAGE", "CREATE"][..]
                } else { &["USAGE"][..] };
                if privileges.iter().any(|p| !known.contains(&p.as_str())) {
                    return Err(Error::Unsupported);
                }
                let relation = if n == "has_schema_privilege" {
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
            "pg_table_is_visible" => {
                let relations = &self.catalogs["pg_class"].rows;
                self.charge(relations.len())?;
                let Some(relation) = relations.iter().find(|r| r[0] == v[0]) else {
                    return Ok(N);
                };
                let mut path = self.snapshot.search_path.clone();
                if !path.iter().any(|n| n == "pg_catalog") {
                    path.insert(0, "pg_catalog".into());
                }
                let namespaces = &self.catalogs["pg_namespace"].rows;
                self.charge(
                    path.len()
                        .saturating_mul(namespaces.len() + relations.len()),
                )?;
                B(path
                    .iter()
                    .filter_map(|name| namespaces.iter().find(|n| n[1] == S(name.clone())))
                    .find_map(|namespace| {
                        relations
                            .iter()
                            .find(|r| r[1] == relation[1] && r[2] == namespace[0])
                    })
                    .is_some_and(|first| first[0] == v[0]))
            }
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
fn format_type(oid: i64, typmod: Value, rows: &[Vec<Value>]) -> R<String> {
    let r = rows
        .iter()
        .find(|r| r[0] == Value::Int(oid))
        .ok_or(Error::Unsupported)?;
    if r[12] == Value::Int(750) {
        return Ok(format!(
            "{}[]",
            format_type(integer(r[10].clone())?, typmod, rows)?
        ));
    }
    Ok(match oid {
        1043 | 1042 => {
            let bounded = match typmod {
                Value::Int(n) if n >= 4 => Some(n - 4),
                _ => None,
            };
            match (oid, bounded) {
                (1043, Some(n)) => format!("character varying({n})"),
                (1043, None) => "character varying".into(),
                (_, Some(n)) => format!("character({n})"),
                (_, None) if typmod == Value::Null => "character".into(),
                (_, None) => "bpchar".into(),
            }
        }
        16 => "boolean".into(),
        20 => "bigint".into(),
        21 => "smallint".into(),
        23 => "integer".into(),
        700 => "real".into(),
        701 => "double precision".into(),
        1700 => match typmod {
            Value::Int(raw) => i32::try_from(raw)
                .ok()
                .and_then(giggles_pg_types::typmod::NumericTypmod::from_typmod)
                .map_or_else(|| "numeric".into(), |modifier| format!("numeric{modifier}")),
            _ => "numeric".into(),
        },
        1083 | 1266 | 1114 | 1184 => {
            let base = if matches!(oid, 1114 | 1184) { "timestamp" } else { "time" };
            let zone = if matches!(oid, 1266 | 1184) { "with" } else { "without" };
            let precision = match typmod {
                Value::Int(n) if n >= 0 => format!("({n})"),
                _ => String::new(),
            };
            format!("{base}{precision} {zone} time zone")
        }
        13307 => "information_schema.cardinal_number".into(),
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
// PG14 int2->oid sign-extends through int4; int4->oid reinterprets its bits.
// Validate the erased Value's declared source width before reinterpretation.
fn integer_oid(v: Value, source: PgType) -> R<Value> {
    let Value::Int(i) = v else {
        return if v == Value::Null {
            Ok(v)
        } else {
            Err(Error::Type)
        };
    };
    let i = match source {
        PgType::Int2 => i32::from(i16::try_from(i).map_err(|_| Error::Type)?),
        PgType::Int4 => i32::try_from(i).map_err(|_| Error::Type)?,
        _ => return Err(Error::Type),
    };
    Ok(Value::Int(i64::from(u32::from_ne_bytes(i.to_ne_bytes()))))
}
fn float_integer(value: f64) -> R<i64> {
    let rounded = value.round_ties_even();
    if !(-9223372036854775808.0..9223372036854775808.0).contains(&rounded) { return Err(Error::Type); }
    Ok(rounded as i64)
}
fn cast(v: Value, t: PgType) -> R<Value> {
    if v == Value::Null {
        return Ok(v);
    }
    use Value::{Bool as B, Int as I, Text as S};
    match t {
        PgType::NameArray | PgType::TextArray => match v {
            Value::Array(values) => values.into_iter().map(|v| cast(v, array_element(t)?)).collect::<R<Vec<_>>>().map(Value::Array),
            _ => Err(Error::Type),
        },
        PgType::OidArray | PgType::Int2Array | PgType::Int4Array | PgType::Int8Array => match v {
            Value::Array(_) => Err(Error::Unsupported),
            S(s) => {
                let contents = s
                    .strip_prefix('{')
                    .and_then(|s| s.strip_suffix('}'))
                    .ok_or(Error::Type)?;
                let element = array_element(t)?;
                let values = if contents.is_empty() {
                    vec![]
                } else {
                    contents
                        .split(',')
                        .map(|s| cast(S(s.trim().into()), element))
                        .collect::<R<Vec<_>>>()?
                };
                Ok(Value::Array(values))
            }
            _ => Err(Error::Type),
        },
        PgType::AclItemArray => match v {
            S(s) if s == "{}" => Ok(Value::Array(Vec::new())),
            _ => Err(Error::Unsupported),
        },
        PgType::Bytea => match v {
            Value::Bytes(_) => Ok(v),
            _ => Err(Error::Type),
        },
        PgType::Text | PgType::Varchar | PgType::Name | PgType::Char => Ok(S(match v {
            S(s) => s,
            I(i) => i.to_string(),
            B(b) => b.to_string(),
            _ => return Err(Error::Type),
        })),
        PgType::Numeric | PgType::Float4 | PgType::Float8 => {
            match (t, &v) {
                (PgType::Numeric, Value::Numeric(_))
                | (PgType::Float4, Value::Float4(_))
                | (PgType::Float8, Value::Float8(_)) => return Ok(v),
                _ => {}
            }
            let text = match v {
                S(s) => s,
                I(i) => i.to_string(),
                Value::Numeric(n) => n.to_string(),
                Value::Float4(f) => f64::from(f.value()).to_string(),
                Value::Float8(f) => f.value().to_string(),
                _ => return Err(Error::Type),
            };
            match t {
                PgType::Numeric => giggles_pg_types::Numeric::parse(&text).map(Value::Numeric),
                PgType::Float4 => giggles_pg_types::Float4::parse(&text).map(Value::Float4),
                _ => giggles_pg_types::Float8::parse(&text).map(Value::Float8),
            }
            .map_err(|_| Error::Type)
        }
        PgType::Bool => match v {
            B(b) => Ok(B(b)),
            S(s) => match s.as_str() {
                "true" | "t" => Ok(B(true)),
                "false" | "f" => Ok(B(false)),
                _ => Err(Error::Type),
            },
            _ => Err(Error::Type),
        },
        PgType::Xid => match v {
            I(i) => u32::try_from(i)
                .map(|i| I(i.into()))
                .map_err(|_| Error::Type),
            S(s) => parse_xid(&s).map(|i| I(i.into())),
            _ => Err(Error::Type),
        },
        PgType::Int2 | PgType::Int4 | PgType::Int8 | PgType::Oid => {
            let i = match v {
                I(i) => i,
                S(s) => s.parse().map_err(|_| Error::Type)?,
                Value::Float4(f) => float_integer(f64::from(f.value()))?,
                Value::Float8(f) => float_integer(f.value())?,
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
    if op == &Op::BitwiseAnd { return Ok(I(integer(l)? & integer(r)?)); }
    if matches!(
        op,
        Op::PGRegexMatch | Op::PGRegexNotMatch | Op::PGRegexIMatch | Op::PGRegexNotIMatch
    ) {
        let (l, r) = (text(l)?, text(r)?);
        if r.len() > 1024 {
            return Err(Error::Limit);
        }
        validate_pg_regex(&r)?;
        if matches!(op, Op::PGRegexIMatch | Op::PGRegexNotIMatch) && !l.is_ascii() {
            return Err(Error::Unsupported);
        }
        let re = regex::RegexBuilder::new(&r)
            .dot_matches_new_line(true)
            .case_insensitive(matches!(op, Op::PGRegexIMatch | Op::PGRegexNotIMatch))
            .size_limit(65536)
            .build()
            .map_err(|_| Error::Unsupported)?;
        return Ok(B(
            re.is_match(&l) ^ matches!(op, Op::PGRegexNotMatch | Op::PGRegexNotIMatch)
        ));
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
        if matches!(op, Op::Divide | Op::Modulo) && r == 0 {
            return Err(Error::DivisionByZero);
        }
        return Ok(I(match op {
            Op::Plus => l.checked_add(r),
            Op::Minus => l.checked_sub(r),
            Op::Multiply => l.checked_mul(r),
            Op::Divide => l.checked_div(r),
            // PostgreSQL returns zero even for MIN % -1, whose quotient
            // would overflow the signed integer type.
            Op::Modulo if r == -1 => Some(0),
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
fn like(s: &str, pattern: &str, escape: &str, insensitive: bool) -> R<bool> {
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
        .case_insensitive(insensitive)
        .size_limit(65536)
        .build()
        .map_err(|_| Error::Limit)?
        .is_match(s))
}

// PostgreSQL's default ARE mode treats newlines as ordinary characters.
// Admit only the ASCII syntax needed by psql's generated name patterns.
fn validate_pg_regex(pattern: &str) -> R<()> {
    if !pattern.is_ascii() || pattern.contains("(?") {
        return Err(Error::Unsupported);
    }
    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '+' | '?' | '{' | '}' => return Err(Error::Unsupported),
            '\\' => {
                let escaped = chars.next().ok_or(Error::Unsupported)?;
                if !".^$|()[]*+?{}\\".contains(escaped) {
                    return Err(Error::Unsupported);
                }
            }
            '[' => {
                let mut count = 0;
                if chars.peek() == Some(&'^') {
                    chars.next();
                }
                loop {
                    let c = chars.next().ok_or(Error::Unsupported)?;
                    if c == ']' && count > 0 {
                        break;
                    }
                    if c == '[' || c == '\\' || c == '&' || c == '~' || c == ':' || c == '=' {
                        return Err(Error::Unsupported);
                    }
                    count += 1;
                }
            }
            ']' => return Err(Error::Unsupported),
            _ => {}
        }
    }
    Ok(())
}

fn projection_name(expr: &A) -> R<String> {
    Ok(match expr {
        A::Identifier(n) => ident(n),
        A::CompoundIdentifier(p) => ident(p.last().unwrap()),
        A::Function(f) => function(&f.name)?,
        A::Nested(e) | A::Collate { expr: e, .. } => projection_name(e)?,
        A::Cast { expr, data_type, .. } => {
            let mut inner = expr.as_ref();
            while let A::Nested(e) | A::Cast { expr: e, .. } = inner { inner = e; }
            let name = projection_name(inner)?;
            if name != "?column?" { name } else {
                let spelling = data_type.to_string().to_ascii_lowercase();
                let base = spelling.trim_end_matches("[]");
                match base {
                    "integer" | "int" | "int4" => "int4",
                    "bigint" | "int8" => "int8",
                    "smallint" | "int2" => "int2",
                    "boolean" => "bool",
                    "real" => "float4",
                    "double precision" => "float8",
                    _ => base,
                }.to_owned()
            }
        },
        A::Case {
            else_result: Some(e),
            ..
        } => {
            let name = projection_name(e)?;
            if name == "?column?" {
                "case".into()
            } else {
                name
            }
        }
        A::Case { .. } => "case".into(),
        A::Subquery(q) => {
            if let a::SetExpr::Select(s) = q.body.as_ref() {
                match s.projection.first() {
                    Some(a::SelectItem::UnnamedExpr(e)) => projection_name(e)?,
                    Some(a::SelectItem::ExprWithAlias { alias, .. }) => ident(alias),
                    _ => "?column?".into(),
                }
            } else {
                "?column?".into()
            }
        }
        _ => "?column?".into(),
    })
}

fn comparison_operand(mut expr: &A) -> &A {
    while let A::Nested(inner) = expr {
        expr = inner;
    }
    expr
}

// PostgreSQL 18 quote_identifier quotes all keyword categories except
// UNRESERVED_KEYWORD. Source: REL_18_STABLE/src/include/parser/kwlist.h

#[cfg(test)]
mod relation_dependency_tests {
    use super::*;

    #[test]
    fn conservative_dependencies() {
        for sql in ["SELECT 1 n", "SELECT current_schema()", "SELECT version()"] {
            assert!(!plan(sql).unwrap().unwrap().depends_on_relations(), "{sql}");
        }
        for sql in [
            "SELECT oid FROM pg_catalog.pg_class",
            "SELECT (SELECT count(*) FROM pg_catalog.pg_class)",
            "SELECT * FROM (SELECT oid FROM pg_catalog.pg_class) x",
            "WITH x AS (SELECT oid FROM pg_catalog.pg_class) SELECT * FROM x",
        ] {
            assert!(plan(sql).unwrap().unwrap().depends_on_relations(), "{sql}");
        }
    }
}

#[cfg(test)]
mod pgparity_cast_names {
    use super::*;
    #[test]
    fn cast_names_use_internal_element_type() {
        for (expr, name) in [("'{1,2}'::integer[]", "int4"), ("'{a}'::text[]", "text"), ("'{1}'::int8[]", "int8"), ("'{1}'::int[][]", "int4"), ("1::integer", "int4"), ("1::bigint", "int8"), ("1::numeric", "numeric")] {
            let p = plan(&format!("SELECT {expr}")).unwrap().unwrap();
            assert_eq!(crate::describe(&p).unwrap()[0].name, name, "{expr}");
        }
    }
}

#[cfg(test)]
mod pgparity_concat_routing {
    use super::*;
    #[test]
    fn typed_concat_declines_to_core() {
        for sql in ["SELECT '{a}'::text[] || '{b}'", "SELECT 1 || '{2}'::int[]", "SELECT '{}'::jsonb || '1'", "SELECT '{}'::jsonb || '{}'::jsonb"] {
            assert!(plan(sql).unwrap().is_none(), "{sql}");
            assert!(plan_gateway(sql).unwrap().is_none(), "{sql}");
        }
    }
}

#[cfg(test)]
mod pgparity_literal_routing {
    use super::*;
    #[test]
    fn prefixed_strings_decline_to_the_core_lexer() {
        for sql in ["SELECT $$it's a label$$", "SELECT $tag$it's a label$tag$",
            r"SELECT E'it\'s\tvalue\\ok'", r"SELECT U&'caf\00e9'",
            "SELECT $$42$$::int", "SELECT coalesce($$42$$, 'x')"] {
            assert!(plan_gateway(sql).unwrap().is_none(), "{sql}");
        }
        // Plain numeric and quoted literal routing stays with the catalog path.
        for sql in ["SELECT 1", "SELECT .5", "SELECT 'it''s a label'"] {
            assert!(plan_gateway(sql).unwrap().is_some(), "{sql}");
        }
    }
}
