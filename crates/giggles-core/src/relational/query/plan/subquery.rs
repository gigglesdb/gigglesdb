//! Apply operators expose child results as hidden scalar columns. Lexical outer
//! references are substituted with typed values, never statement parameters.
use super::*;
use exec::Relation;

#[derive(Debug, Clone)]
pub(super) enum Right {
    Query(Box<Plan>),
    Array(Scalar),
}
#[derive(Debug, Clone)]
pub(super) struct Apply {
    name: String,
    right: Right,
    comparison: Option<(Scalar, String, bool)>, // lhs, operator, ALL
    exists: bool,
    contextual: bool,
}
impl Apply {
    pub fn visit_queries(&self, f: &mut impl FnMut(&Query)) {
        if let Right::Query(q) = &self.right {
            q.visit_queries(f);
        }
    }
    pub fn visit_scalars(&self, f: &mut impl FnMut(&Scalar)) {
        match &self.right {
            Right::Query(q) => q.visit_scalars(f),
            Right::Array(s) => f(s),
        }
        if let Some((s, _, _)) = &self.comparison {
            f(s);
        }
    }
    pub fn bind_schema_names(
        &mut self,
        bind: &impl Fn(&mut String),
        state: &Relational,
        path: &[String],
    ) -> R<()> {
        match &mut self.right {
            Right::Query(q) => q.bind_schema_names(bind, state, path)?,
            Right::Array(s) => schema_names::scalar(s, bind, state, path),
        }
        if let Some((s, _, _)) = &mut self.comparison {
            schema_names::scalar(s, bind, state, path);
        }
        Ok(())
    }
    pub fn remap(&mut self, alias: &str, name: &str) {
        if let Right::Query(q) = &mut self.right {
            cte::remap(q, alias, name);
        }
    }
}
fn scalar(tokens: &[Token]) -> R<Scalar> {
    let mut p = parse::parser(tokens);
    let s = Scalar::parse_expr(&mut p, 0, false)?;
    if p.at != p.tokens.len() {
        return Err(SqlError::Syntax);
    }
    Ok(s)
}
pub(in crate::relational::query) fn quantified(tokens: &[Token]) -> bool {
    tokens.windows(3).any(|t| {
        (matches!(&t[0], Token::Op(_)) || parse::word(&t[0], "like") || parse::word(&t[0], "ilike"))
            && ["any", "all", "some"].iter().any(|w| parse::word(&t[1], w))
            && t[2] == Token::Symbol('(')
    })
}
fn fresh(tokens: &[Token], next: &mut usize) -> String {
    loop {
        let name = format!("plan_apply_{}", *next);
        *next += 1;
        if !tokens
            .iter()
            .any(|t| matches!(t,Token::Word(w)|Token::QuotedIdent(w) if w == &name))
        {
            return name;
        }
    }
}
pub(super) fn rewrite(
    tokens: &[Token],
    owner: &mut Parser,
    depth: usize,
    next: &mut usize,
) -> parse::ParseResult<(Vec<Token>, Vec<Apply>)> {
    let positions = parse::top(tokens)?;
    let from = positions
        .iter()
        .copied()
        .find(|i| parse::word(&tokens[*i], "from"));
    let end = from.map(|start| {
        positions
            .iter()
            .copied()
            .find(|i| {
                *i > start
                    && ["where", "group", "having", "order", "limit", "offset"]
                        .iter()
                        .any(|w| parse::word(&tokens[*i], w))
            })
            .unwrap_or(tokens.len())
    });
    let mut out = vec![];
    let mut applies = vec![];
    let mut i = 0;
    while i < tokens.len() {
        if from.is_some_and(|f| i == f) {
            let end = end.unwrap();
            out.extend_from_slice(&tokens[i..end]);
            i = end;
            continue;
        }
        let quant = ["any", "some", "all"]
            .iter()
            .any(|w| parse::word(&tokens[i], w))
            && tokens.get(i + 1) == Some(&Token::Symbol('('))
            && out.last().is_some_and(|t| {
                matches!(t, Token::Op(_)) || parse::word(t, "like") || parse::word(t, "ilike")
            });
        let in_query = parse::word(&tokens[i], "in") && is_subquery_open(tokens, i + 1);
        if quant || in_query {
            let open = i + 1;
            let close = subquery_end(tokens, open)?;
            let mut op_at = out.len();
            let (op, all) = if in_query {
                let negated = out.last().is_some_and(|t| parse::word(t, "not"));
                if negated {
                    op_at -= 1;
                }
                (if negated { "<>" } else { "=" }.into(), negated)
            } else {
                op_at = op_at.checked_sub(1).ok_or(SqlError::Syntax)?;
                let mut op = match &out[op_at] {
                    Token::Op(op) => op.clone(),
                    Token::Word(w) if w == "like" || w == "ilike" => w.clone(),
                    _ => return Err(SqlError::Syntax.into()),
                };
                if (op == "like" || op == "ilike")
                    && op_at > 0
                    && parse::word(&out[op_at - 1], "not")
                {
                    op = if op == "like" { "!~~" } else { "!~~*" }.into();
                    op_at -= 1;
                }
                (op, parse::word(&tokens[i], "all"))
            };
            // Logical operators and clause boundaries belong outside the lhs.
            let mut start = 0;
            let mut nesting = 0usize;
            let mut cases = 0usize;
            for at in (0..op_at).rev() {
                match &out[at] {
                    Token::Symbol(')' | ']') => nesting += 1,
                    Token::Symbol('(' | '[') if nesting > 0 => nesting -= 1,
                    Token::Symbol('(' | '[') if cases == 0 => {
                        start = at + 1;
                        break;
                    }
                    t if nesting == 0 && parse::word(t, "end") => cases += 1,
                    t if nesting == 0 && parse::word(t, "case") && cases > 0 => cases -= 1,
                    t if nesting == 0
                        && cases == 0
                        && (*t == Token::Symbol(',')
                            || [
                                "select", "where", "having", "on", "and", "or", "not", "when",
                                "then", "else", "by", "distinct",
                            ]
                            .iter()
                            .any(|w| parse::word(t, w))) =>
                    {
                        start = at + 1;
                        break;
                    }
                    _ => {}
                }
            }
            let lhs = scalar(&out[start..op_at])?;
            let inner = &tokens[open + 1..close];
            let right = if inner.first().is_some_and(|t| {
                ["select", "values", "with", "table"]
                    .iter()
                    .any(|w| parse::word(t, w))
            }) {
                Right::Query(Box::new(parse::child_expression(inner, owner, depth, next)?))
            } else {
                if depth >= subplans::MAX_DEPTH {
                    return Err(SqlError::Limit.into());
                }
                let (inner, nested) = rewrite(inner, owner, depth + 1, next)?;
                applies.extend(nested);
                Right::Array(scalar(&inner)?)
            };
            let name = fresh(tokens, next);
            out.truncate(start);
            out.push(Token::QuotedIdent(name.clone()));
            applies.push(Apply {
                name,
                right,
                comparison: Some((lhs, op, all)),
                exists: false,
                contextual: false,
            });
            i = close + 1;
            continue;
        }
        let exists = parse::word(&tokens[i], "exists") && is_subquery_open(tokens, i + 1);
        let open = if exists { i + 1 } else { i };
        if is_subquery_open(tokens, open) {
            let close = subquery_end(tokens, open)?;
            let child = parse::child_expression(&tokens[open + 1..close], owner, depth, next)?;
            let name = fresh(tokens, next);
            out.push(Token::QuotedIdent(name.clone()));
            applies.push(Apply {
                name,
                right: Right::Query(Box::new(child)),
                comparison: None,
                exists,
                contextual: false,
            });
            i = close + 1;
            continue;
        }
        out.push(tokens[i].clone());
        i += 1;
    }
    Ok((out, applies))
}

// The mutable walkers stay in the planner. Scalar remains the authority for
// binding, coercion and evaluation of every ordinary expression.
pub(super) fn map_scalar(s: &mut Scalar, f: &mut impl FnMut(&mut Scalar) -> R<()>) -> R<()> {
    match s {
        Scalar::Predicate(p) => map_filter(p, f)?,
        Scalar::Case(c) => {
            for (p, s) in &mut c.arms {
                map_filter(p, f)?;
                map_scalar(s, f)?;
            }
            if let Some(s) = &mut c.otherwise {
                map_scalar(s, f)?;
            }
        }
        Scalar::Unary(_, s) | Scalar::Cast(s, _) | Scalar::Domain(s, ..) => map_scalar(s, f)?,
        Scalar::Binary(_, a, b) => {
            map_scalar(a, f)?;
            map_scalar(b, f)?;
        }
        Scalar::Call(_, args) => {
            for s in args {
                map_scalar(s, f)?;
            }
        }
        Scalar::SubstringOrder(args) => {
            for s in args {
                map_scalar(s, f)?;
            }
        }
        _ => {}
    }
    f(s)
}
fn map_filter(p: &mut Filter, f: &mut impl FnMut(&mut Scalar) -> R<()>) -> R<()> {
    match p {
        Filter::Clauses(c) => {
            for p in c.filter.iter_mut().chain(&mut c.having) {
                map_filter(p, f)?;
            }
            for s in &mut c.group {
                map_scalar(s, f)?;
            }
        }
        Filter::Scalar(s) | Filter::Null(s, _) => map_scalar(s, f)?,
        Filter::Compare(a, _, b) => {
            map_scalar(a, f)?;
            map_scalar(b, f)?;
        }
        Filter::Like {
            value,
            pattern,
            escape,
            ..
        } => {
            map_scalar(value, f)?;
            map_scalar(pattern, f)?;
            if let Some(s) = escape {
                map_scalar(s, f)?;
            }
        }
        Filter::Not(p) | Filter::Is(p, ..) => map_filter(p, f)?,
        Filter::Logic(a, _, b) => {
            map_filter(a, f)?;
            map_filter(b, f)?;
        }
        Filter::InSubquery { value, query, .. } => {
            map_scalar(value, f)?;
            map_query(query, f)?;
        }
        Filter::Exists { query, .. } => map_query(query, f)?,
    }
    Ok(())
}
fn map_query(q: &mut Query, f: &mut impl FnMut(&mut Scalar) -> R<()>) -> R<()> {
    for (item, _) in &mut q.items {
        if let Item::Scalar(s) | Item::Aggregate(_, Some(s)) = item {
            map_scalar(s, f)?;
        }
    }
    for (s, _, _) in &mut q.order {
        map_scalar(s, f)?;
    }
    for p in q
        .filter
        .iter_mut()
        .chain(q.joins.iter_mut().map(|j| &mut j.on))
    {
        map_filter(p, f)?;
    }
    Ok(())
}

/// Parser placeholders become unspellable identifiers before lexical binding,
/// so even an unmentioned user column cannot capture an inner apply result.
pub(super) fn internalize(select: &mut Select) -> R<Vec<(String, String)>> {
    let names: Vec<_> = select
        .applies
        .iter()
        .filter(|a| !a.name.starts_with('\0'))
        .map(|a| (a.name.clone(), format!("\0{}", a.name)))
        .collect();
    map_query(&mut select.query, &mut |s| rename_column(s, &names))?;
    for a in &mut select.applies {
        if let Some((_, name)) = names.iter().find(|(old, _)| *old == a.name) {
            a.name = name.clone();
        }
        if let Some((s, _, _)) = &mut a.comparison {
            rename_scalar(s, &names)?;
        }
        if let Right::Array(s) = &mut a.right {
            rename_scalar(s, &names)?;
        }
    }
    Ok(names)
}
fn rename_column(s: &mut Scalar, names: &[(String, String)]) -> R<()> {
    if let Scalar::Column(c) = s
        && c.qualifier.is_none()
        && let Some((_, name)) = names.iter().find(|(old, _)| *old == c.name)
    {
        c.name = name.clone();
    }
    Ok(())
}
pub(super) fn rename_scalar(s: &mut Scalar, names: &[(String, String)]) -> R<()> {
    map_scalar(s, &mut |s| rename_column(s, names))
}

pub(super) fn empty_shape() -> join::Shape {
    join::Shape {
        columns: vec![],
        typmods: vec![],
        namespace: join::Namespace::default(),
    }
}
pub(super) fn outer_scalar(s: &mut Scalar, shape: &join::Shape, row: Option<&SqlRow>) -> R<bool> {
    let mut used = false;
    map_scalar(s, &mut |s| {
        if let Scalar::Column(c) = s {
            match shape.namespace.resolve(c) {
                Ok(n) => {
                    *s = typed(
                        row.map_or(Value::Null, |r| r.values[n].clone()),
                        &shape.columns[n].data_type,
                    );
                    used = true;
                }
                Err(SqlError::UnknownColumn)
                    if c.qualifier.as_ref().is_some_and(|q| {
                        shape
                            .namespace
                            .qualified
                            .iter()
                            .any(|(alias, _)| alias == q)
                    }) =>
                {
                    return Err(SqlError::UnknownColumn);
                }
                Err(SqlError::UnknownColumn | SqlError::UnknownTable) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    })?;
    Ok(used)
}
fn typed(v: Value, ty: &DataType) -> Scalar {
    Scalar::Cast(Box::new(Scalar::Literal(v)), ty.clone())
}

/// Bind an enclosing lexical scope through all descendants, respecting aliases
/// and column names shadowed by any intervening query level.
pub(super) fn instantiate(
    plan: &Plan,
    shape: &join::Shape,
    row: Option<&SqlRow>,
    state: &Relational,
    parameters: &[Option<DataType>],
) -> R<(Plan, bool)> {
    instantiate_scoped(plan, shape, row, state, parameters, &[])
}
pub(super) fn instantiate_scoped(
    plan: &Plan,
    shape: &join::Shape,
    row: Option<&SqlRow>,
    state: &Relational,
    parameters: &[Option<DataType>],
    blocked: &[(String, Vec<String>)],
) -> R<(Plan, bool)> {
    fn visit(
        p: &mut Plan,
        shape: &join::Shape,
        row: Option<&SqlRow>,
        state: &Relational,
        parameters: &[Option<DataType>],
        blocked: &[(String, Vec<String>)],
        used: &mut bool,
    ) -> R<()> {
        for c in &mut p.ctes {
            visit(&mut c.query, shape, row, state, parameters, blocked, used)?;
        }
        let mut block = blocked.to_vec();
        if let Body::Select(s) = &p.body {
            if let Some(from) = &s.from {
                from.locals(state, parameters, &mut block);
            } else {
                for src in &s.query.sources {
                    block.push((
                        src.alias.clone(),
                        state
                            .tables
                            .get(&src.name)
                            .map(|t| t.columns.iter().map(|c| c.name.clone()).collect())
                            .unwrap_or_default(),
                    ));
                }
            }
        }
        let mut replace = |s: &mut Scalar| -> R<()> {
            if let Scalar::Column(c) = s {
                let shadowed = if let Some(q) = &c.qualifier {
                    block.iter().any(|(a, _)| a == q)
                } else {
                    block.iter().any(|(_, cols)| cols.contains(&c.name))
                };
                if !shadowed {
                    *used |= outer_scalar(s, shape, row)?;
                }
            }
            Ok(())
        };
        for (s, _, _) in &mut p.order {
            map_scalar(s, &mut replace)?;
        }
        for s in p.limit.iter_mut().chain(&mut p.offset) {
            map_scalar(s, &mut replace)?;
        }
        match &mut p.body {
            Body::Values(rows) => {
                for s in rows.iter_mut().flatten() {
                    map_scalar(s, &mut replace)?;
                }
            }
            Body::Set { left, right, .. } => {
                visit(left, shape, row, state, parameters, blocked, used)?;
                visit(right, shape, row, state, parameters, blocked, used)?;
            }
            Body::Select(s) => {
                map_query(&mut s.query, &mut replace)?;
                // Correlation can turn an indexed outer-column predicate into
                // a constant. The full filter remains the source of truth.
                s.query.predicates.clear();
                for a in &mut s.applies {
                    if let Some((s, _, _)) = &mut a.comparison {
                        map_scalar(s, &mut replace)?;
                    }
                    if let Right::Array(s) = &mut a.right {
                        map_scalar(s, &mut replace)?;
                    }
                }
                for a in &mut s.applies {
                    if let Right::Query(q) = &mut a.right {
                        let mut nested = false;
                        visit(q, shape, row, state, parameters, &block, &mut nested)?;
                        a.contextual |= nested;
                        *used |= nested;
                    }
                }
                if let Some(i) = &mut s.input {
                    visit(&mut i.query, shape, row, state, parameters, blocked, used)?;
                }
                if let Some(from) = &mut s.from {
                    from.instantiate(shape, row, state, parameters, blocked, used)?;
                }
            }
        }
        Ok(())
    }
    let mut p = plan.clone();
    let mut used = false;
    visit(&mut p, shape, row, state, parameters, blocked, &mut used)?;
    Ok((p, used))
}

fn input_shape(
    s: &Select,
    state: &Relational,
    parameters: &mut [Option<DataType>],
) -> R<join::Shape> {
    s.from
        .as_ref()
        .map(|f| f.clone().resolve(state, parameters))
        .transpose()
        .map(|s| s.unwrap_or_else(empty_shape))
}
fn bind_scalar(
    s: &Scalar,
    shape: &join::Shape,
    parameters: &mut [Option<DataType>],
) -> R<BoundScalar> {
    let table = TableDef {
        id: 0,
        name: join::INPUT.into(),
        columns: shape.columns.clone(),
        primary_key: vec![],
    };
    let resolve = |c: &ColumnRef, _| {
        shape
            .namespace
            .resolve(c)
            .map(|column| BoundColumn { source: 0, column })
    };
    let mut s = s.clone();
    map_scalar(&mut s, &mut |s| {
        if let Scalar::Column(c) = s
            && let Some((_, n)) = shape.namespace.hidden.iter().find(|(name, n)| {
                *name == c.name
                    && shape
                        .columns
                        .get(n + 1)
                        .is_some_and(|col| col.name == format!("{}_many", name))
            })
        {
            *s = Scalar::Case(Box::new(functions::CaseExpr {
                arms: vec![(
                    Filter::Scalar(join::col(n + 1)),
                    cardinality(&shape.columns[*n].data_type),
                )],
                otherwise: Some(join::col(*n)),
            }));
        }
        Ok(())
    })?;
    s.infer(None, parameters, &resolve, &[&table])?;
    s.bind(&resolve, &[&table], parameters)
}
fn comparison(lhs: Scalar, op: &str, rhs: Scalar) -> Filter {
    match op {
        "like" | "~~" | "ilike" | "~~*" | "!~~" | "!~~*" => Filter::Like {
            value: lhs,
            pattern: rhs,
            escape: None,
            insensitive: op.ends_with('*') || op == "ilike",
            negated: op.starts_with('!'),
        },
        _ => Filter::Compare(lhs, op.into(), rhs),
    }
}
fn prepare(
    s: &Select,
    state: &Relational,
    parameters: &mut [Option<DataType>],
    cached: Option<&BTreeMap<String, Cached>>,
) -> R<(Query, join::Shape)> {
    let mut shape = input_shape(s, state, parameters)?;
    let mut names = vec![];
    let mut constants = BTreeMap::new();
    for a in &s.applies {
        let (ty, name) = match &a.right {
            Right::Query(q) => {
                let (mut q, correlated) = instantiate(q, &shape, None, state, parameters)?;
                let meta = q.resolve(state, parameters)?;
                if !a.exists && meta.columns.len() != 1 {
                    return Err(SqlError::Syntax);
                }
                let col = meta.columns.first().ok_or(SqlError::Syntax)?;
                if !correlated && !a.contextual && a.comparison.is_none() && !a.exists {
                    let value = cached.and_then(|c| c.get(&a.name));
                    let literal = typed(
                        value
                            .and_then(|v| v.values.first())
                            .cloned()
                            .unwrap_or(Value::Null),
                        &col.data_type,
                    );
                    constants.insert(
                        a.name.clone(),
                        if value.is_some_and(|v| v.many || v.values.len() > 1) {
                            cardinality(&col.data_type)
                        } else {
                            literal
                        },
                    );
                }
                (col.data_type.clone(), col.name.clone())
            }
            Right::Array(s) => (
                srf::element(&bind_scalar(s, &shape, parameters)?.ty)?,
                "?column?".into(),
            ),
        };
        if let Some((lhs, op, _)) = &a.comparison {
            let table = TableDef {
                id: 0,
                name: join::INPUT.into(),
                columns: shape.columns.clone(),
                primary_key: vec![],
            };
            let resolve = |c: &ColumnRef, _| {
                shape
                    .namespace
                    .resolve(c)
                    .map(|column| BoundColumn { source: 0, column })
            };
            let cmp = comparison(lhs.clone(), op, typed(Value::Null, &ty));
            cmp.infer(parameters, &resolve, &[&table])?;
            cmp.bind(parameters, &resolve, &[&table])?;
        }
        let ty = if a.comparison.is_some() || a.exists {
            DataType::Boolean
        } else {
            ty
        };
        let n = shape.columns.len();
        for (name, ty) in [
            (a.name.clone(), ty.clone()),
            (format!("{}_many", a.name), DataType::Boolean),
        ] {
            shape
                .namespace
                .hidden
                .push((name.clone(), shape.columns.len()));
            shape.columns.push(ColumnDef {
                id: 0,
                name,
                data_type: ty,
                not_null: false,
            });
            shape.typmods.push(-1);
        }
        names.push((
            a.name.clone(),
            n,
            if a.exists {
                "exists".into()
            } else if a.comparison.is_some() {
                "?column?".into()
            } else {
                name
            },
        ));
    }
    let mut query = s.query.clone();
    for (item, alias) in &mut query.items {
        if alias.is_none()
            && let Item::Scalar(Scalar::Column(c)) = item
            && let Some((_, _, name)) = names.iter().find(|(n, _, _)| *n == c.name)
        {
            *alias = Some(name.clone());
        }
    }
    map_query(&mut query, &mut |s| {
        if let Scalar::Column(c) = s
            && let Some((_, n, _)) = names.iter().find(|(name, _, _)| *name == c.name)
        {
            if let Some(value) = constants.get(&c.name) {
                *s = value.clone();
                return Ok(());
            }
            *s = Scalar::Case(Box::new(functions::CaseExpr {
                arms: vec![(
                    Filter::Scalar(join::col(n + 1)),
                    cardinality(&shape.columns[*n].data_type),
                )],
                otherwise: Some(join::col(*n)),
            }));
        }
        Ok(())
    })?;
    if query.sources.is_empty() {
        query.sources.push(Source {
            name: join::INPUT.into(),
            alias: "plan_apply".into(),
        });
    }
    Ok((join::prepare(&query, &shape)?, shape))
}
pub(super) fn resolve(
    s: &Select,
    state: &Relational,
    parameters: &mut [Option<DataType>],
) -> R<bind::Meta> {
    let (q, shape) = prepare(s, state, parameters, None)?;
    let catalog = join::catalog(state, &shape)?;
    let bound = q.bind_projection_seeded(&catalog, parameters.to_vec())?;
    let mut d = bound.description(&catalog);
    for column in &mut d.columns {
        column.id = 0;
    }
    for (slot, ty) in parameters.iter_mut().zip(d.parameters) {
        *slot = Some(ty);
    }
    Ok(bind::Meta {
        unknown: vec![false; d.columns.len()],
        columns: d.columns,
        typmods: d.typmods,
        type_oids: d.type_oids,
    })
}

#[derive(Clone)]
pub(in crate::relational::query) struct Cached {
    values: Vec<Value>,
    ty: DataType,
    null_array: bool,
    many: bool,
}
impl Cached {
    fn bytes(&self) -> usize {
        self.values.len() * std::mem::size_of::<Value>()
            + self.values.iter().map(value_bytes).sum::<usize>()
    }
}

pub(super) fn run<'a>(
    s: &Select,
    state: &Relational,
    source: &dyn scan::RowSource,
    params: &[Value],
    parameters: &[Option<DataType>],
    clock: Option<&super::super::super::clock::ExecutionClock>,
    execution: &'a Execution<'_>,
) -> R<Relation<'a>> {
    let (_, shape) = prepare(s, state, &mut parameters.to_vec(), None)?;
    let input = if let Some(from) = &s.from {
        from.run(state, source, params, parameters, clock, execution)?
    } else {
        let mut r = Relation::new(vec![], vec![], execution);
        r.push(SqlRow {
            values: vec![],
            version: 0,
        })?;
        r
    };
    let base_shape = input_shape(s, state, &mut parameters.to_vec())?;
    for a in &s.applies {
        if a.contextual
            || a.exists
            || a.comparison.is_some()
            || execution.plan_applies.borrow().contains_key(&a.name)
        {
            continue;
        }
        if let Right::Query(child) = &a.right {
            let (child, correlated) = instantiate(child, &base_shape, None, state, parameters)?;
            if !correlated {
                let rhs = query_values(
                    &child, state, source, params, parameters, clock, execution, true,
                )?;
                execution
                    .retention
                    .borrow_mut()
                    .charge(rhs.bytes() + a.name.len() + 128)?;
                execution
                    .plan_applies
                    .borrow_mut()
                    .insert(a.name.clone(), rhs);
            }
        }
    }
    let mut rows = Relation::new(shape.columns.clone(), shape.typmods.clone(), execution);
    for row in &input.rows {
        execution.comparison()?;
        let scratch = RetentionScope::new(execution);
        scratch.borrow().charge(
            row.values.len() * std::mem::size_of::<Value>()
                + row.values.iter().map(value_bytes).sum::<usize>(),
        )?;
        let mut values = row.values.clone();
        for a in &s.applies {
            let evaluated = (|| -> R<(Value, bool)> {
                let (child, correlated) = match &a.right {
                    Right::Query(child) => {
                        let (q, c) = instantiate(
                            child,
                            &shape,
                            Some(&SqlRow {
                                values: values.clone(),
                                version: row.version,
                            }),
                            state,
                            parameters,
                        )?;
                        (Some(q), c || a.contextual)
                    }
                    Right::Array(_) => (None, true),
                };
                let cached = if !correlated {
                    execution.plan_applies.borrow().get(&a.name).cloned()
                } else {
                    None
                };
                let rhs = if let Some(cached) = cached {
                    cached
                } else {
                    let rhs = if let Some(child) = child {
                        query_values(
                            &child,
                            state,
                            source,
                            params,
                            parameters,
                            clock,
                            execution,
                            !a.exists && a.comparison.is_none(),
                        )?
                    } else if let Right::Array(array) = &a.right {
                        let mut array = bind_scalar(array, &shape, &mut parameters.to_vec())?;
                        let ty = srf::element(&array.ty)?;
                        array.attach_clock(clock);
                        let value = array.eval(
                            &Tuple {
                                values: vec![values.clone()],
                                version: row.version,
                            },
                            params,
                            &mut 0,
                        )?;
                        let null_array = value == Value::Null;
                        let values = srf::array_values(value, &ty)?;
                        Cached {
                            values,
                            ty,
                            null_array,
                            many: false,
                        }
                    } else {
                        unreachable!()
                    };
                    if !correlated {
                        execution
                            .retention
                            .borrow_mut()
                            .charge(rhs.bytes() + a.name.len() + 128)?;
                        execution
                            .plan_applies
                            .borrow_mut()
                            .insert(a.name.clone(), rhs.clone());
                    }
                    rhs
                };
                scratch.borrow().charge(rhs.bytes())?;
                let (value, many) = if a.exists {
                    (Value::Bool(!rhs.values.is_empty()), false)
                } else if let Some((lhs, op, all)) = &a.comparison {
                    if rhs.null_array {
                        (Value::Null, false)
                    } else {
                        let syntax = lhs;
                        let mut lhs = bind_scalar(lhs, &shape, &mut parameters.to_vec())?;
                        lhs.attach_clock(clock);
                        let lv = lhs.eval(
                            &Tuple {
                                values: vec![values.clone()],
                                version: row.version,
                            },
                            params,
                            &mut 0,
                        )?;
                        let lt = lhs.ty;
                        let mut unknown = false;
                        let mut outcome = *all;
                        for rv in rhs.values {
                            execution.comparison()?;
                            let left = if matches!(
                                syntax,
                                Scalar::Literal(Value::Text(_) | Value::Null)
                            ) {
                                Scalar::Literal(lv.clone())
                            } else {
                                typed(lv.clone(), &lt)
                            };
                            let f = comparison(left, op, typed(rv, &rhs.ty));
                            let mut bound =
                                f.bind(parameters, &|_, _| Err(SqlError::UnknownColumn), &[])?;
                            bound.attach_clock(clock);
                            match bound.eval(
                                &Tuple {
                                    values: vec![],
                                    version: 0,
                                },
                                params,
                                &mut 0,
                            )? {
                                None => unknown = true,
                                Some(b) if b != *all => {
                                    outcome = b;
                                    unknown = false;
                                    break;
                                }
                                _ => {}
                            }
                        }
                        (
                            if unknown {
                                Value::Null
                            } else {
                                Value::Bool(outcome)
                            },
                            false,
                        )
                    }
                } else {
                    (
                        rhs.values.first().cloned().unwrap_or(Value::Null),
                        rhs.many || rhs.values.len() > 1,
                    )
                };
                Ok((value, many))
            })();
            // A cardinality error is consumed lazily with the enclosing scalar.
            let (value, many) = match evaluated {
                Ok(v) => v,
                Err(SqlError::CardinalityViolation) => (Value::Null, true),
                Err(e) => return Err(e),
            };
            values.extend([value, Value::Bool(many)]);
        }
        rows.push(SqlRow {
            values,
            version: row.version,
        })?;
    }
    let (q, _) = prepare(
        s,
        state,
        &mut parameters.to_vec(),
        Some(&execution.plan_applies.borrow()),
    )?;
    let catalog = join::catalog(state, &shape)?;
    let source = cte::VirtualRows {
        base: source,
        relations: vec![(join::INPUT.into(), &rows)],
    };
    let bound = q
        .bind_projection_seeded(&catalog, parameters.to_vec())?
        .with_clock(clock);
    let output = bound.execute_context(
        &catalog, &source, params, parameters, execution, true, false,
    )?;
    let d = bound.description(&catalog);
    let mut result = Relation::new(d.columns, d.typmods, execution);
    for row in output.rows {
        result.push(row)?;
    }
    Ok(result)
}

fn cardinality(ty: &DataType) -> Scalar {
    Scalar::Call("\0plan_cardinality".into(), vec![typed(Value::Null, ty)])
}
#[expect(clippy::too_many_arguments, reason = "shared query execution context")]
fn query_values(
    child: &Plan,
    state: &Relational,
    source: &dyn scan::RowSource,
    params: &[Value],
    parameters: &[Option<DataType>],
    clock: Option<&super::super::super::clock::ExecutionClock>,
    execution: &Execution<'_>,
    scalar: bool,
) -> R<Cached> {
    match child.run(state, source, params, parameters, clock, execution) {
        Ok(rows) => {
            let ty = rows
                .columns
                .first()
                .ok_or(SqlError::Syntax)?
                .data_type
                .clone();
            let scratch = RetentionScope::new(execution);
            scratch.borrow().charge(
                rows.rows
                    .iter()
                    .map(|r| std::mem::size_of::<Value>() + value_bytes(&r.values[0]))
                    .sum(),
            )?;
            let values = rows.rows.into_iter().map(|r| r.values[0].clone()).collect();
            Ok(Cached {
                values,
                ty,
                null_array: false,
                many: false,
            })
        }
        Err(SqlError::CardinalityViolation) if scalar => {
            let d = child.describe(state, parameters.len(), parameters)?;
            Ok(Cached {
                values: vec![],
                ty: d.columns[0].data_type.clone(),
                null_array: false,
                many: true,
            })
        }
        Err(e) => Err(e),
    }
}

pub(super) fn outer_scoped(
    s: &mut Scalar,
    shape: &join::Shape,
    row: Option<&SqlRow>,
    blocked: &[(String, Vec<String>)],
) -> R<bool> {
    let mut used = false;
    map_scalar(s, &mut |s| {
        if let Scalar::Column(c) = s {
            let shadowed = if let Some(q) = &c.qualifier {
                blocked.iter().any(|(a, _)| a == q)
            } else {
                blocked.iter().any(|(_, cols)| cols.contains(&c.name))
            };
            if !shadowed {
                used |= outer_scalar(s, shape, row)?;
            }
        }
        Ok(())
    })?;
    Ok(used)
}

// Route only membership queries that actually capture an enclosing source.
// Catalog names are already resolved here; no scalar binding or evaluation is
// performed before the caller's usual authorization and classification steps.
type Lexical = Vec<(String, Vec<String>)>;
fn local_names(query: &Query, state: &Relational) -> Vec<String> {
    let mut out = vec![];
    for (item, alias) in &query.items {
        if let Some(alias) = alias {
            out.push(alias.clone());
            continue;
        }
        match item {
            Item::Scalar(Scalar::Column(c)) => out.push(c.name.clone()),
            Item::Aggregate(a, _) => out.push(a.name().into()),
            Item::Scalar(Scalar::Call(name, _)) if !name.starts_with('\0') => {
                out.push(name.clone())
            }
            Item::Star | Item::QualifiedStar { .. } => {
                if let Some(derived) = query.derived() {
                    out.extend(local_names(derived, state));
                } else {
                    for source in &query.sources {
                        if let Ok(t) = views::table(state, &source.name) {
                            out.extend(t.logical_columns().map(|c| c.name.clone()));
                        }
                    }
                }
            }
            _ => out.push("?column?".into()),
        }
    }
    out
}
fn locals(query: &Query, state: &Relational) -> Lexical {
    query
        .sources
        .iter()
        .enumerate()
        .map(|(i, s)| {
            let columns = if i == 0
                && let Some(derived) = query.derived()
            {
                local_names(derived, state)
            } else {
                views::table(state, &s.name)
                    .map(|t| t.logical_columns().map(|c| c.name.clone()).collect())
                    .unwrap_or_default()
            };
            (s.alias.clone(), columns)
        })
        .collect()
}

fn local_column(c: &ColumnRef, scope: &Lexical) -> bool {
    if let Some(q) = &c.qualifier {
        scope.iter().any(|(a, _)| a == q)
    } else {
        scope.iter().any(|(_, cols)| cols.contains(&c.name))
    }
}
fn children<'a>(query: &'a Query, out: &mut Vec<(bool, &'a Query)>) {
    fn filter<'a>(f: &'a Filter, out: &mut Vec<(bool, &'a Query)>) {
        match f {
            Filter::InSubquery { query, .. } => out.push((true, query)),
            Filter::Exists { query, .. } => out.push((false, query)),
            Filter::Not(f) | Filter::Is(f, ..) => filter(f, out),
            Filter::Logic(a, _, b) => {
                filter(a, out);
                filter(b, out);
            }
            Filter::Clauses(c) => {
                for f in c.filter.iter().chain(&c.having) {
                    filter(f, out);
                }
            }
            _ => {}
        }
    }
    for f in query.filter.iter().chain(query.joins.iter().map(|j| &j.on)) {
        filter(f, out);
    }
}
fn captures(query: &Query, state: &Relational, outer: &[Lexical], blocked: &[Lexical]) -> bool {
    let mut block = blocked.to_vec();
    block.push(locals(query, state));
    let mut captured = false;
    let mut inspect = |s: &Scalar| {
        if let Scalar::Column(c) = s
            && !block.iter().rev().any(|scope| local_column(c, scope))
            && outer.iter().rev().any(|scope| local_column(c, scope))
        {
            captured = true;
        }
        Ok(())
    };
    for (item, _) in &query.items {
        if let Item::Scalar(s) | Item::Aggregate(_, Some(s)) = item {
            let _ = walk_scalar(s, &mut inspect);
        }
    }
    for (s, _, _) in &query.order {
        let _ = walk_scalar(s, &mut inspect);
    }
    for f in query.filter.iter().chain(query.joins.iter().map(|j| &j.on)) {
        let _ = f.visit_scalars(&mut |s| walk_scalar(s, &mut inspect));
    }
    let mut nested = vec![];
    children(query, &mut nested);
    captured
        || nested
            .into_iter()
            .any(|(_, q)| captures(q, state, outer, &block))
}
fn needs_membership_apply(query: &Query, state: &Relational, outer: &[Lexical]) -> bool {
    let mut scopes = outer.to_vec();
    scopes.push(locals(query, state));
    let mut nested = vec![];
    children(query, &mut nested);
    nested.into_iter().any(|(membership, q)| {
        (membership && captures(q, state, &scopes, &[]))
            || needs_membership_apply(q, state, &scopes)
    })
}
impl Plan {
    pub(in crate::relational) fn correlated_in(query: &Query, state: &Relational) -> Option<Self> {
        // HAVING children consume the reconstructed group frame. Turning them
        // into input apply columns would both run them per row and make their
        // synthetic results look like ungrouped input columns.
        if query.explain || query.having().is_some() || !query.has_in_subquery()
            || !needs_membership_apply(query, state, &[]) {
            return None;
        }
        Some(promote(query.clone(), &mut 0))
    }
}
fn promote(mut query: Query, next: &mut usize) -> Plan {
    fn lower(f: &mut Filter, applies: &mut Vec<Apply>, next: &mut usize) {
        match f {
            Filter::InSubquery {
                value,
                query,
                negated,
            } => {
                let child = promote((**query).clone(), next);
                let name = format!("\0plan_apply_{}", *next);
                *next += 1;
                applies.push(Apply {
                    name: name.clone(),
                    right: Right::Query(Box::new(child)),
                    comparison: Some((
                        value.clone(),
                        if *negated { "<>" } else { "=" }.into(),
                        *negated,
                    )),
                    exists: false,
                    contextual: false,
                });
                *f = Filter::Scalar(Scalar::Column(ColumnRef {
                    qualifier: None,
                    name,
                }));
            }
            Filter::Exists { query, negated } => {
                let child = promote((**query).clone(), next);
                let name = format!("\0plan_apply_{}", *next);
                *next += 1;
                applies.push(Apply {
                    name: name.clone(),
                    right: Right::Query(Box::new(child)),
                    comparison: None,
                    exists: true,
                    contextual: false,
                });
                let value = Filter::Scalar(Scalar::Column(ColumnRef {
                    qualifier: None,
                    name,
                }));
                *f = if *negated {
                    Filter::Not(Box::new(value))
                } else {
                    value
                };
            }
            Filter::Not(f) | Filter::Is(f, ..) => lower(f, applies, next),
            Filter::Logic(a, _, b) => {
                lower(a, applies, next);
                lower(b, applies, next);
            }
            Filter::Clauses(c) => {
                for f in c.filter.iter_mut().chain(&mut c.having) {
                    lower(f, applies, next);
                }
            }
            _ => {}
        }
    }
    let parameters = query.used_parameters();
    let mut applies = vec![];
    if let Some(f) = &mut query.filter {
        lower(f, &mut applies, next);
    }
    let sources = query.sources.clone();
    let mut from = None;
    for (i, source) in sources.into_iter().enumerate() {
        let leaf = Query {
            explain: false,
            sources: vec![source.clone()],
            joins: vec![],
            items: vec![(Item::Star, None)],
            predicates: vec![],
            filter: None,
            order: vec![],
            limit: None,
        };
        let leaf_plan = if i == 0
            && let Some(derived) = query.derived()
        {
            promote(derived.clone(), next)
        } else {
            Plan::new(Body::Select(Box::new(Select {
                query: leaf,
                parameters: BTreeSet::new(),
                applies: vec![],
                input: None,
                from: None,
            })))
        };
        let right = join::From::Leaf {
            query: Box::new(leaf_plan),
            alias: source.alias,
            aliases: vec![],
        };
        from = Some(if let Some(left) = from {
            let join = &query.joins[i - 1];
            join::From::Join {
                left: Box::new(left),
                right: Box::new(right),
                kind: if join.left {
                    join::Kind::Left
                } else {
                    join::Kind::Inner
                },
                condition: join::Condition::On(Box::new(join.on.clone())),
            }
        } else {
            right
        });
    }
    if from.is_some() {
        query.sources = vec![Source {
            name: join::INPUT.into(),
            alias: "plan_join".into(),
        }];
        query.joins.clear();
        query.predicates.clear();
        if let Some(Filter::Clauses(c)) = &mut query.filter {
            c.derived = None;
        }
    }
    Plan::new(Body::Select(Box::new(Select {
        query,
        parameters,
        applies,
        input: None,
        from,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use Value::{Bool as B, Int as I, Null as N};
    fn execute(sql: &str) -> R<SqlResult> {
        let mut state = Relational::default();
        let p = Prepared::parse(sql, 1, 0)?;
        state.execute(&p, &[], None)
    }
    fn rows(sql: &str) -> Vec<Vec<Value>> {
        execute(sql)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
            .rows
            .into_iter()
            .map(|r| r.values)
            .collect()
    }
    #[test]
    fn scalar_cardinality_and_lexical_levels() {
        assert_eq!(rows("SELECT (SELECT 1)"), vec![vec![I(1)]]);
        assert_eq!(rows("SELECT (SELECT 1 WHERE false)"), vec![vec![N]]);
        assert!(rows("SELECT (SELECT 1 UNION ALL SELECT 2) WHERE false").is_empty());
        assert_eq!(
            rows("SELECT CASE WHEN false THEN (SELECT 1 UNION ALL SELECT 2) ELSE 3 END"),
            vec![vec![I(3)]]
        );
        assert_eq!(
            execute("SELECT (SELECT 1 UNION ALL SELECT 2)"),
            Err(SqlError::CardinalityViolation)
        );
        assert_eq!(
            rows("SELECT t.n,(SELECT (SELECT t.n+1)) FROM (VALUES(1),(2)) t(n) ORDER BY t.n"),
            vec![vec![I(1), I(2)], vec![I(2), I(3)]]
        );
        let p = Prepared::parse("SELECT (SELECT 1,2)", 1, 0).unwrap();
        assert_eq!(
            p.describe(&Relational::default()).err(),
            Some(SqlError::Syntax)
        );
    }
    #[test]
    fn synthetic_names_cannot_capture_outer_user_columns() {
        assert_eq!(
            rows(
                "SELECT t.id,(SELECT (SELECT 1)) FROM (VALUES(7,99),(8,88)) t(id,plan_apply_0) ORDER BY t.id"
            ),
            vec![vec![I(7), I(1)], vec![I(8), I(1)]]
        );
        assert_eq!(
            rows(
                "SELECT t.id,s.n FROM (VALUES(7,99)) t(id,plan_apply_0) CROSS JOIN generate_series(1,(SELECT 1)) s(n)"
            ),
            vec![vec![I(7), I(1)]]
        );
    }
    #[test]
    fn quantified_null_empty_and_comparisons() {
        for (term, value) in [
            ("NULL::int = ANY('{}'::int[])", B(false)),
            ("NULL::int = ALL('{}'::int[])", B(true)),
            ("1 = ANY('{2,NULL}'::int[])", N),
            ("1 <> ALL('{2,NULL}'::int[])", N),
            ("NULL::int NOT IN (SELECT 1 WHERE false)", B(true)),
            ("2 > SOME(ARRAY[1,3])", B(true)),
            ("2 < ALL(SELECT 3 UNION ALL SELECT 4)", B(true)),
        ] {
            assert_eq!(rows(&format!("SELECT {term}")), vec![vec![value]], "{term}");
        }
        assert_eq!(
            rows(
                "SELECT t.n,t.n=ANY(SELECT 1 UNION ALL SELECT 3) FROM (VALUES(1),(2),(3)) t(n) ORDER BY t.n"
            ),
            vec![
                vec![I(1), B(true)],
                vec![I(2), B(false)],
                vec![I(3), B(true)]
            ]
        );
        assert!(rows("SELECT 1 WHERE false AND 1=ANY(ARRAY[1])").is_empty());
        assert_eq!(
            rows("SELECT 'a' LIKE ANY(ARRAY['a','b'])"),
            vec![vec![B(true)]]
        );
        assert_eq!(
            rows("SELECT 'a' NOT LIKE ALL(ARRAY['b','c'])"),
            vec![vec![B(true)]]
        );
    }
    #[test]
    fn nested_scalar_quantified_arguments() {
        assert_eq!(
            rows("SELECT (SELECT 2)=ANY(ARRAY[1,(SELECT 2)])"),
            vec![vec![B(true)]]
        );
        assert_eq!(
            rows(
                "SELECT CASE WHEN false THEN (SELECT 1 UNION ALL SELECT 2)=ANY(ARRAY[1]) ELSE true END"
            ),
            vec![vec![B(true)]]
        );
    }
    #[test]
    fn scalar_expression_positions_and_empty_aggregate() {
        assert_eq!(rows("SELECT abs((SELECT -2))"), vec![vec![I(2)]]);
        assert_eq!(
            rows("SELECT count(*) FROM (VALUES(1)) t(n) WHERE false HAVING (SELECT 1)=1"),
            vec![vec![I(0)]]
        );
        assert_eq!(
            rows("SELECT count(*),(SELECT 2) FROM (VALUES(1)) t(n) WHERE false"),
            vec![vec![I(0), I(2)]]
        );
        assert_eq!(
            rows("SELECT t.n FROM (VALUES(1),(2)) t(n) ORDER BY (SELECT -t.n)"),
            vec![vec![I(2)], vec![I(1)]]
        );
        assert_eq!(
            rows("SELECT (SELECT 1 UNION ALL SELECT 2 LIMIT 1)"),
            vec![vec![I(1)]]
        );
    }
    #[test]
    fn correlated_membership_where_keeps_uncorrelated_path() {
        let mut state = Relational::default();
        {
            let mut run = |sql: &str| {
                let p = Prepared::parse(sql, 1, state.epoch).unwrap();
                state.execute(&p, &[], None).unwrap()
            };
            for sql in [
                "CREATE TABLE iw_t(id int PRIMARY KEY,v text)",
                "CREATE TABLE iw_u(id int PRIMARY KEY,x int)",
                "INSERT INTO iw_t VALUES(1,'a'),(2,'b'),(3,'c')",
                "INSERT INTO iw_u VALUES(2,0),(10,1)",
            ] {
                run(sql);
            }
        }
        for (sql, expected) in [
            (
                "SELECT id FROM iw_t t WHERE id IN(SELECT id FROM iw_u u WHERE u.id=t.id)",
                vec![vec![I(2)]],
            ),
            (
                "SELECT v FROM iw_t t WHERE id IN(SELECT id FROM iw_u u WHERE v='a')",
                vec![],
            ),
            (
                "SELECT id FROM iw_t t WHERE id IN(SELECT id FROM (SELECT id FROM iw_u) u WHERE u.id=t.id)",
                vec![vec![I(2)]],
            ),
        ] {
            let p = Prepared::parse(sql, 1, state.epoch).unwrap();
            assert!(
                p.bind_schemas(&state).unwrap().is_planner_statement(),
                "{sql}"
            );
            assert_eq!(
                state
                    .execute(&p, &[], None)
                    .unwrap()
                    .rows
                    .into_iter()
                    .map(|r| r.values)
                    .collect::<Vec<_>>(),
                expected,
                "{sql}"
            );
        }
        for sql in [
            "SELECT id FROM iw_t t WHERE id IN(SELECT id FROM iw_u)",
            "SELECT id FROM iw_t t WHERE id IN(SELECT id FROM iw_u u WHERE EXISTS(SELECT 1 FROM iw_u t WHERE t.id=u.id))",
        ] {
            let p = Prepared::parse(sql, 1, state.epoch).unwrap();
            assert!(
                !p.bind_schemas(&state).unwrap().is_planner_statement(),
                "{sql}"
            );
            assert_eq!(
                state.execute(&p, &[], None).unwrap().rows[0].values,
                vec![I(2)],
                "{sql}"
            );
        }
    }
    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn correlated_membership_v2_hydrates_all_sources() {
        let parent = std::env::temp_dir().canonicalize().unwrap();
        let path = parent.join(format!("b11-apply-v2-{}", std::process::id()));
        {
            let admin =
                crate::Admin::create_engine(&path, crate::Limits::default(), crate::Engine::V2)
                    .unwrap();
            let mut s = admin.sql_session();
            for sql in [
                "CREATE TABLE iv_t(id int PRIMARY KEY)",
                "CREATE TABLE iv_u(id int PRIMARY KEY)",
                "INSERT INTO iv_t VALUES(1),(2)",
                "INSERT INTO iv_u VALUES(2)",
            ] {
                s.execute(sql, &[], None).unwrap();
            }
            let p = s.prepare("SELECT t.id FROM iv_t t WHERE t.id IN(SELECT u.id FROM iv_u u WHERE u.id=t.id)").unwrap();
            assert_eq!(p.table(), Some("giggles_join"));
            s.describe(&p).unwrap();
            let result = s.execute_prepared(&p, &[], None).unwrap();
            assert_eq!(
                result
                    .rows
                    .into_iter()
                    .map(|r| r.values)
                    .collect::<Vec<_>>(),
                vec![vec![I(2)]]
            );
            assert_eq!(
                s.execute(
                    "SELECT t.id,(SELECT u.id FROM iv_u u WHERE u.id=t.missing) FROM iv_t t",
                    &[],
                    None
                ),
                Err(crate::CoreError::Sql(SqlError::UnknownColumn))
            );
        }
        let resolved = path.canonicalize().unwrap();
        assert!(resolved.starts_with(&parent));
        std::fs::remove_dir_all(resolved).unwrap();
    }
    #[test]
    fn correlated_having_uses_group_frames_and_rejects_ungrouped_columns() {
        let mut state = Relational::default();
        let mut run = |sql: &str| {
            let p = Prepared::parse(sql, 1, state.epoch)?;
            state.execute(&p, &[], None)
        };
        for sql in [
            "CREATE TABLE orders(id int PRIMARY KEY,item text,qty int,price numeric)",
            "CREATE TABLE items(id text PRIMARY KEY,name text)",
            "INSERT INTO orders VALUES(1,'a',1,1.5),(2,'a',2,2.5),(3,'b',5,NULL),(4,'c',NULL,3)",
            "INSERT INTO items VALUES('a','A'),('b','B'),('d','D')",
        ] {
            run(sql).unwrap();
        }
        // PG18.6: each child captures orders.item from the completed group.
        for predicate in [
            "item IN(SELECT id FROM items WHERE items.id=orders.item)",
            "EXISTS(SELECT 1 FROM items WHERE items.id=orders.item)",
            "item IN(SELECT id FROM items WHERE EXISTS(SELECT 1 FROM items i WHERE i.id=orders.item))",
        ] {
            let sql = format!("SELECT item,count(*) FROM orders GROUP BY item::text HAVING {predicate} ORDER BY item");
            assert_eq!(
                run(&sql).unwrap_or_else(|e| panic!("{sql}: {e:?}")).rows
                    .into_iter().map(|r| r.values).collect::<Vec<_>>(),
                vec![vec![Value::Text("a".into()), I(2)], vec![Value::Text("b".into()), I(1)]],
                "{sql}",
            );
        }
        for (predicate, expected) in [
            ("item NOT IN(SELECT NULL::text FROM items WHERE items.id=orders.item)",
                vec![vec![Value::Text("c".into()), I(1)]]),
            ("NULL::text NOT IN(SELECT id FROM items WHERE items.id=orders.item)",
                vec![vec![Value::Text("c".into()), I(1)]]),
            ("item IN(SELECT orders.item)",
                vec![vec![Value::Text("a".into()), I(2)], vec![Value::Text("b".into()), I(1)],
                    vec![Value::Text("c".into()), I(1)]]),
        ] {
            let sql = format!("SELECT item,count(*) FROM orders GROUP BY item HAVING {predicate} ORDER BY item");
            assert_eq!(run(&sql).unwrap_or_else(|e| panic!("{sql}: {e:?}")).rows
                .into_iter().map(|r| r.values).collect::<Vec<_>>(), expected, "{sql}");
        }
        for predicate in [
            "item IN(SELECT id FROM items WHERE items.id=orders.qty::text)",
            "EXISTS(SELECT 1 FROM items WHERE items.id=orders.qty::text)",
            // PG18.6 rejects count(*) in this inner WHERE with 42803; it
            // does not implicitly become an aggregate of the outer group.
            "EXISTS(SELECT 1 FROM items WHERE items.id=orders.item AND count(*)>1)",
        ] {
            let sql = format!("SELECT item,count(*) FROM orders GROUP BY item HAVING {predicate}");
            assert_eq!(run(&sql), Err(SqlError::GroupingError), "{sql}");
        }
    }
    #[test]
    fn apply_dependencies_and_cancellation() {
        let p =
            Prepared::parse("SELECT (SELECT b.id FROM b WHERE b.id=a.id) FROM a", 1, 0).unwrap();
        let mut reads = p.read_tables();
        reads.sort();
        let mut auth = p.authorization_tables().unwrap();
        auth.sort();
        assert_eq!(reads, vec!["a", "b"]);
        assert_eq!(auth, vec!["a", "b"]);
        assert!(p.write_tables().is_empty());
        assert_eq!(p.table(), Some("giggles_join"));
        assert_eq!(p.storage_table(), None);
        let mut state = Relational::default();
        let p = Prepared::parse("SELECT (SELECT 1)", 1, 0).unwrap();
        assert_eq!(
            state.execute_with_cancel(&p, &[], None, || true),
            Err(SqlError::Aborted)
        );
    }
    #[test]
    fn lateral_functions_and_null_extension() {
        assert_eq!(
            rows("SELECT * FROM generate_series(1,(SELECT 2))"),
            vec![vec![I(1)], vec![I(2)]]
        );
        assert_eq!(
            rows(
                "SELECT t.n,g.x FROM (VALUES(1),(2)) t(n),generate_series(1,(SELECT t.n)) g(x) ORDER BY t.n,g.x"
            ),
            vec![vec![I(1), I(1)], vec![I(2), I(1)], vec![I(2), I(2)]]
        );
        assert_eq!(
            rows("SELECT * FROM unnest((SELECT ARRAY[1,NULL])) WITH ORDINALITY"),
            vec![vec![I(1), I(1)], vec![N, I(2)]]
        );
        assert_eq!(
            rows("SELECT n FROM generate_series(3,1,-1) s(n) ORDER BY n DESC"),
            vec![vec![I(3)], vec![I(2)], vec![I(1)]]
        );
        assert_eq!(
            execute("SELECT * FROM generate_series(1,2,0)"),
            Err(SqlError::InvalidParameterValue)
        );
        assert_eq!(
            rows("SELECT * FROM unnest(ARRAY[1,NULL,3]) WITH ORDINALITY"),
            vec![vec![I(1), I(1)], vec![N, I(2)], vec![I(3), I(3)]]
        );
        assert_eq!(
            rows(
                "SELECT t.n,s.x FROM (VALUES(1),(2)) t(n) LEFT JOIN LATERAL (SELECT t.n AS x WHERE t.n=1) s ON true ORDER BY t.n"
            ),
            vec![vec![I(1), I(1)], vec![I(2), N]]
        );
        let p = Prepared::parse(
            "SELECT * FROM generate_series(1::bigint,2) WITH ORDINALITY",
            1,
            0,
        )
        .unwrap();
        assert!(
            p.describe(&Relational::default())
                .unwrap()
                .columns
                .iter()
                .all(|c| c.data_type == DataType::BigInt)
        );
    }
}
