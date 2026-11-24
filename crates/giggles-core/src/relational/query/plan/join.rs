//! General FROM trees, with a namespace independent of the physical row layout.
use super::*;
use exec::Relation;
use std::collections::HashMap;

pub(super) const INPUT: &str = "\0plan_join";

#[derive(Debug, Clone, Copy)]
pub(super) enum Kind {
    Inner,
    Left,
    Right,
    Full,
    // MERGE uses full-join candidate selection with unrestricted ON predicates.
    MergeFull,
}
#[derive(Debug, Clone)]
pub(super) enum Condition {
    On(Box<Filter>),
    Using(Vec<String>, Option<String>),
    Natural,
    Cross,
}
#[derive(Debug, Clone)]
pub(super) enum From {
    Function {
        function: srf::Function,
        alias: String,
        aliases: Vec<String>,
    },
    Lateral(Box<From>),
    Leaf {
        query: Box<Plan>,
        alias: String,
        aliases: Vec<String>,
    },
    Join {
        left: Box<From>,
        right: Box<From>,
        kind: Kind,
        condition: Condition,
    },
    Alias {
        input: Box<From>,
        alias: String,
        aliases: Vec<String>,
    },
}

/// Visible columns control unqualified resolution and `*`; qualified columns
/// retain the original sides even when USING produces a separate merged value.
#[derive(Debug, Clone, Default)]
pub(in crate::relational::query) struct Namespace {
    pub(super) visible: Vec<(String, usize)>,
    pub(super) hidden: Vec<(String, usize)>,
    pub(super) scalar_rows: Vec<(String, usize)>,
    pub(super) qualified: Vec<(String, Vec<(String, usize)>)>,
}
impl Namespace {
    pub(in crate::relational::query) fn resolve(&self, c: &ColumnRef) -> R<usize> {
        if c.qualifier.is_none()
            && let Some(n) = c
                .name
                .strip_prefix("\0join_column_")
                .and_then(|n| n.parse().ok())
        {
            return if self.hidden.iter().any(|(_, i)| *i == n)
                || self.visible.iter().any(|(_, i)| *i == n)
                || self
                    .qualified
                    .iter()
                    .any(|(_, cols)| cols.iter().any(|(_, i)| *i == n))
            {
                Ok(n)
            } else {
                Err(SqlError::UnknownColumn)
            };
        }
        if c.qualifier.is_none()
            && let Some((_, n)) = self.hidden.iter().find(|(name, _)| *name == c.name)
        {
            return Ok(*n);
        }
        let columns = if let Some(alias) = &c.qualifier {
            &self
                .qualified
                .iter()
                .find(|(a, _)| a == alias)
                .ok_or(SqlError::UnknownTable)?
                .1
        } else {
            &self.visible
        };
        let mut found = columns.iter().filter(|(name, _)| *name == c.name);
        let n = match found.next() {
            Some((_, n)) => *n,
            None if c.qualifier.is_none() => {
                return self.scalar_rows.iter().find(|(name, _)| name == &c.name)
                    .map(|(_, n)| *n).ok_or(SqlError::UnknownColumn);
            }
            None => return Err(SqlError::UnknownColumn),
        };
        if found.next().is_some() {
            return Err(SqlError::AmbiguousColumn);
        }
        Ok(n)
    }
    fn combine(&self, right: &Self, offset: usize) -> R<Self> {
        if self
            .qualified
            .iter()
            .any(|(a, _)| right.qualified.iter().any(|(b, _)| a == b))
        {
            return Err(SqlError::DuplicateAlias);
        }
        let mut out = self.clone();
        out.scalar_rows.extend(right.scalar_rows.iter().map(|(n, i)| (n.clone(), i + offset)));
        out.hidden
            .extend(right.hidden.iter().map(|(n, i)| (n.clone(), i + offset)));
        out.visible
            .extend(right.visible.iter().map(|(n, i)| (n.clone(), i + offset)));
        out.qualified
            .extend(right.qualified.iter().map(|(a, cols)| {
                (
                    a.clone(),
                    cols.iter().map(|(n, i)| (n.clone(), i + offset)).collect(),
                )
            }));
        Ok(out)
    }
}
pub(super) struct Shape {
    pub columns: Vec<ColumnDef>,
    pub typmods: Vec<i32>,
    pub collations: Vec<Option<BoundScalar>>,
    pub(super) namespace: Namespace,
}
struct JoinShape {
    shape: Shape,
    on: Filter,
    merged: Vec<(usize, usize, DataType, bool)>,
}

fn alias(p: &mut Parser) -> R<Option<(String, Vec<String>)>> {
    let explicit = p.word("as");
    let present = explicit
        || matches!(p.tokens.get(p.at), Some(Token::QuotedIdent(_)))
        || matches!(p.tokens.get(p.at), Some(Token::Word(w)) if !["natural", "inner", "left", "right", "full", "cross", "join", "on", "using", "tablesample"].contains(&w.as_str()));
    if !present {
        return Ok(None);
    }
    let name = p.name()?;
    let mut columns = vec![];
    if p.sym('(') {
        loop {
            columns.push(p.column_name()?);
            if columns.len() > MAX_COLUMNS {
                return Err(SqlError::Limit);
            }
            if !p.sym(',') {
                break;
            }
        }
        if !p.sym(')') {
            return Err(SqlError::Syntax);
        }
    }
    Ok(Some((name, columns)))
}
fn starts_query(mut tokens: &[Token]) -> R<bool> {
    // A derived set expression can begin with a parenthesized arm. A
    // grouped join beginning with a subquery still belongs to FROM parsing.
    while tokens.first() == Some(&Token::Symbol('(')) {
        let close = subquery_end(tokens, 0)?;
        if tokens.get(close + 1).is_some_and(|token| {
            !["union", "intersect", "except", "order", "limit", "offset", "fetch"]
                .iter().any(|word| parse::word(token, word))
        }) {
            return Ok(false);
        }
        tokens = &tokens[1..close];
    }
    Ok(tokens.first().is_some_and(|token| {
        ["select", "values", "table", "with"].iter().any(|word| parse::word(token, word))
    }))
}

fn item(
    p: &mut Parser,
    owner: &mut Parser,
    depth: usize,
    next: &mut usize,
) -> parse::ParseResult<From> {
    if depth > subplans::MAX_DEPTH {
        return Err(SqlError::Limit.into());
    }
    if p.word("lateral") {
        let parenthesized = p.tokens.get(p.at) == Some(&Token::Symbol('('));
        let query = parenthesized
            && p.tokens.get(p.at + 1).is_some_and(|t| {
                ["select", "values", "table", "with"]
                    .iter()
                    .any(|w| parse::word(t, w))
            });
        if depth >= subplans::MAX_DEPTH {
            return Err(SqlError::Limit.into());
        }
        let input = item(p, owner, depth + 1, next)?;
        if !query && (parenthesized || !matches!(input, From::Function { .. })) {
            return Err(ParseError::Definite(SqlError::Syntax));
        }
        return Ok(From::Lateral(Box::new(input)));
    }
    if p.sym('(') {
        // Admit the child before reserving another join-tree or query frame.
        if depth >= subplans::MAX_DEPTH {
            return Err(SqlError::Limit.into());
        }
        let open = p.at - 1;
        let end = subquery_end(&p.tokens, open)?;
        let inner = &p.tokens[p.at..end];
        let mut result = if starts_query(inner)? {
            let alias = format!("\0unnamed_subquery_{next}");
            *next += 1;
            From::Leaf {
                query: Box::new(parse::child_expression(inner, p.tokens.get(end), owner, depth, next)?),
                alias,
                aliases: vec![],
            }
        } else {
            let mut child = parse::parser(inner);
            let from = explicit(&mut child, owner, depth + 1, next)?;
            if child.at != child.tokens.len() {
                return Err(SqlError::Syntax.into());
            }
            from
        };
        p.at = end + 1;
        if let Some((alias, aliases)) = alias(p)? {
            if let From::Leaf {
                alias: a,
                aliases: c,
                ..
            } = &mut result
            {
                *a = alias;
                *c = aliases;
            } else {
                result = From::Alias {
                    input: Box::new(result),
                    alias,
                    aliases,
                };
            }
        }
        return Ok(result);
    }
    let begin = p.at;
    let refs = p.relations.len();
    let name = p.table_name()?;
    if p.tokens.get(p.at) == Some(&Token::Symbol('(')) {
        p.relations.truncate(refs);
        let function = srf::parse(p, &name, owner, depth, next).map_err(ParseError::Definite)?;
        let (alias, aliases) =
            alias(p)?.unwrap_or_else(|| (split_key(&name).1.to_string(), vec![]));
        return Ok(From::Function {
            function,
            alias,
            aliases,
        });
    }
    let table_tokens = p.tokens[begin..p.at].to_vec();
    let (alias, aliases) = alias(p)?.unwrap_or_else(|| (split_key(&name).1.to_string(), vec![]));
    let sample = paging::Sample::parse(p)?;
    let mut tokens = vec![
        Token::Word("select".into()),
        Token::Symbol('*'),
        Token::Word("from".into()),
    ];
    tokens.extend(table_tokens);
    let mut leaf = parse::parser(&tokens);
    let query = Query::parse(&mut leaf)?;
    owner.relations.extend(leaf.relations);
    let mut plan = Plan::new(Body::Select(Box::new(Select {
            applies: vec![],
            windows: vec![],
            query,
            parameters: BTreeSet::new(),
            input: None,
            from: None,
        })));
    plan.decorations.sample = sample.map(Box::new);
    Ok(From::Leaf {
        query: Box::new(plan),
        alias,
        aliases,
    })
}
fn explicit(
    p: &mut Parser,
    owner: &mut Parser,
    depth: usize,
    next: &mut usize,
) -> parse::ParseResult<From> {
    let mut left = item(p, owner, depth, next)?;
    loop {
        let natural = p.word("natural");
        let cross = p.word("cross");
        if natural && cross {
            return Err(SqlError::Syntax.into());
        }
        let kind = if p.word("left") {
            Kind::Left
        } else if p.word("right") {
            Kind::Right
        } else if p.word("full") {
            Kind::Full
        } else {
            let inner = p.word("inner");
            if !natural
                && !cross
                && !inner
                && !p.tokens.get(p.at).is_some_and(|t| parse::word(t, "join"))
            {
                break;
            }
            Kind::Inner
        };
        if !matches!(kind, Kind::Inner) {
            p.word("outer");
        }
        p.need("join")?;
        if depth >= subplans::MAX_DEPTH {
            return Err(SqlError::Limit.into());
        }
        let right = item(p, owner, depth + 1, next)?;
        let condition = if natural {
            Condition::Natural
        } else if cross {
            Condition::Cross
        } else if p.word("using") {
            if !p.sym('(') {
                return Err(SqlError::Syntax.into());
            }
            let mut columns = vec![];
            loop {
                let name = p.name()?;
                if columns.contains(&name) {
                    return Err(SqlError::DuplicateColumn.into());
                }
                columns.push(name);
                if columns.len() > MAX_COLUMNS {
                    return Err(SqlError::Limit.into());
                }
                if !p.sym(',') {
                    break;
                }
            }
            if !p.sym(')') {
                return Err(SqlError::Syntax.into());
            }
            let alias = if p.word("as") { Some(p.name()?) } else { None };
            Condition::Using(columns, alias)
        } else {
            p.need("on")?;
            let on = Filter::parse(p, 0, 0)?;
            // This is a recognized PostgreSQL restriction, not a planner decline.
            if matches!(kind, Kind::Full) && !required_equality(&on) && !constant_condition(&on) {
                return Err(ParseError::Definite(SqlError::Unsupported));
            }
            Condition::On(Box::new(on))
        };
        left = joined(left, right, kind, condition)?;
    }
    Ok(left)
}
fn list(
    p: &mut Parser,
    owner: &mut Parser,
    depth: usize,
    next: &mut usize,
) -> parse::ParseResult<From> {
    let mut left = explicit(p, owner, depth, next)?;
    while p.sym(',') {
        let right = explicit(p, owner, depth, next)?;
        left = joined(left, right, Kind::Inner, Condition::Cross)?;
    }
    Ok(left)
}
fn joined(left: From, right: From, kind: Kind, condition: Condition) -> R<From> {
    fn height(from: &From) -> usize {
        match from {
            From::Leaf { .. } | From::Function { .. } => 1,
            From::Lateral(input) => height(input) + 1,
            From::Alias { input, .. } => height(input) + 1,
            From::Join { left, right, .. } => height(left).max(height(right)) + 1,
        }
    }
    // Keep materialized operator recursion within the query nesting budget.
    if height(&left).max(height(&right)) >= subplans::MAX_DEPTH {
        return Err(SqlError::Limit);
    }
    Ok(From::Join {
        left: Box::new(left),
        right: Box::new(right),
        kind,
        condition,
    })
}
pub(super) fn parse_select(
    tokens: &[Token],
    following: Option<&Token>,
    owner: &mut Parser,
    depth: usize,
    next: &mut usize,
) -> parse::ParseResult<Option<Plan>> {
    let positions = parse::top(tokens)?;
    let Some(start) = positions
        .iter()
        .copied()
        .find(|i| parse::word(&tokens[*i], "from"))
    else {
        return Ok(None);
    };
    let end = positions
        .iter()
        .copied()
        .find(|i| {
            *i > start
                && (parse::group_clause(tokens, *i)
                    || ["where", "having", "order", "limit", "offset"]
                        .iter()
                        .any(|w| parse::word(&tokens[*i], w)))
        })
        .unwrap_or(tokens.len());
    let mut p = parse::parser(&tokens[start + 1..end]);
    let mut from = list(&mut p, owner, depth, next)?;
    if p.at != p.tokens.len() {
        return Err(SqlError::Unsupported.into());
    }
    owner.relations.extend(p.relations);
    let mut rewritten = tokens[..start + 1].to_vec();
    rewritten.push(Token::QuotedIdent("plan_join".into()));
    rewritten.extend_from_slice(&tokens[end..]);
    let mut p = parse::parser(&rewritten);
    let mut query = Query::parse_with_following(&mut p, following)?;
    // Remove only our placeholder, retaining real references with the same
    // spelling in projection qualifiers or WHERE subqueries.
    if let Some(i) = p
        .relations
        .iter()
        .position(|r| r.schema.is_none() && r.name == "plan_join")
    {
        p.relations.remove(i);
    }
    owner.relations.extend(p.relations);
    query.sources[0].name = INPUT.into();
    if let Some(filter) = query.where_filter() {
        from.push_where_equalities(filter);
    }
    let parameters = tokens
        .iter()
        .filter_map(|t| {
            if let Token::Param(n) = t {
                Some(*n)
            } else {
                None
            }
        })
        .collect();
    Ok(Some(Plan::new(Body::Select(Box::new(Select {
        applies: vec![],
        windows: vec![],
        query,
        parameters,
        input: None,
        from: Some(from),
    })))))
}

impl From {
    /// Only pure inner trees admit this pushdown: an outer join could create
    /// additional NULL-extended rows when an input row is removed early.
    fn push_where_equalities(&mut self, filter: &Filter) {
        fn inner(from: &From) -> bool {
            match from {
                From::Leaf { .. } | From::Function { .. } => true,
                From::Lateral(input) => inner(input),
                From::Alias { input, .. } => inner(input),
                From::Join {
                    left, right, kind, ..
                } => matches!(kind, Kind::Inner) && inner(left) && inner(right),
            }
        }
        fn has(from: &From, alias: &str) -> bool {
            match from {
                From::Leaf { alias: a, .. }
                | From::Alias { alias: a, .. }
                | From::Function { alias: a, .. } => a == alias,
                From::Lateral(input) => has(input, alias),
                From::Join { left, right, .. } => has(left, alias) || has(right, alias),
            }
        }
        fn push(from: &mut From, a: &str, b: &str, filter: &Filter) {
            let From::Join {
                left,
                right,
                condition,
                ..
            } = from
            else {
                return;
            };
            if has(left, a) && has(left, b) {
                push(left, a, b, filter);
            } else if has(right, a) && has(right, b) {
                push(right, a, b, filter);
            } else if has(left, a) && has(right, b) || has(left, b) && has(right, a) {
                match condition {
                    Condition::Cross => *condition = Condition::On(Box::new(filter.clone())),
                    Condition::On(on) => {
                        **on = Filter::Logic(on.clone(), true, Box::new(filter.clone()))
                    }
                    _ => {}
                }
            }
        }
        fn visit(from: &mut From, filter: &Filter) {
            match filter {
                Filter::Logic(a, true, b) => {
                    visit(from, a);
                    visit(from, b);
                }
                Filter::Compare(Scalar::Column(a), op, Scalar::Column(b)) if op == "=" => {
                    if let (Some(a), Some(b)) = (&a.qualifier, &b.qualifier) {
                        push(from, a, b, filter);
                    }
                }
                _ => {}
            }
        }
        if inner(self) {
            visit(self, filter);
        }
    }
    pub(super) fn visit_queries(&self, f: &mut impl FnMut(&Query)) {
        match self {
            Self::Leaf { query, .. } => query.visit_queries(f),
            Self::Join {
                left,
                right,
                condition,
                ..
            } => {
                left.visit_queries(f);
                right.visit_queries(f);
                if let Condition::On(on) = condition {
                    let _ = on.visit_subqueries(&mut |_, query| {
                        f(query);
                        Ok(())
                    });
                }
            }
            Self::Function { function, .. } => {
                if let Some(input) = &function.input {
                    input.visit_queries(f);
                }
            }
            Self::Lateral(input) | Self::Alias { input, .. } => input.visit_queries(f),
        }
    }
    pub(super) fn visit_scalars(&self, f: &mut impl FnMut(&Scalar)) {
        match self {
            Self::Leaf { query, .. } => query.visit_scalars(f),
            Self::Join {
                left,
                right,
                condition,
                ..
            } => {
                left.visit_scalars(f);
                right.visit_scalars(f);
                if let Condition::On(on) = condition {
                    f(&Scalar::Predicate(on.clone()));
                }
            }
            Self::Function { function, .. } => {
                if let Some(input) = &function.input {
                    input.visit_scalars(f);
                }
                for s in &function.args {
                    f(s);
                }
            }
            Self::Lateral(input) | Self::Alias { input, .. } => input.visit_scalars(f),
        }
    }
    pub(super) fn remap(&mut self, alias: &str, name: &str) {
        match self {
            Self::Leaf { query, .. } => cte::remap(query, alias, name),
            Self::Join { left, right, .. } => {
                left.remap(alias, name);
                right.remap(alias, name);
            }
            Self::Function { function, .. } => {
                if let Some(input) = &mut function.input {
                    cte::remap(input, alias, name);
                }
            }
            Self::Lateral(input) | Self::Alias { input, .. } => input.remap(alias, name),
        }
    }
    pub(super) fn bind_schema_names(
        &mut self,
        bind: &impl Fn(&mut String),
        state: &Relational,
        path: &[String],
    ) -> R<()> {
        match self {
            Self::Leaf { query, .. } => query.bind_schema_names(bind, state, path),
            Self::Join {
                left,
                right,
                condition,
                ..
            } => {
                left.bind_schema_names(bind, state, path)?;
                right.bind_schema_names(bind, state, path)?;
                if let Condition::On(on) = condition {
                    let mut scalar = Scalar::Predicate(on.clone());
                    schema_names::scalar(&mut scalar, bind, state, path);
                    if let Scalar::Predicate(filter) = scalar {
                        *on = filter;
                    }
                }
                Ok(())
            }
            Self::Function { function, .. } => {
                if let Some(input) = &mut function.input {
                    input.bind_schema_names(bind, state, path)?;
                }
                for s in &mut function.args {
                    schema_names::scalar(s, bind, state, path);
                }
                Ok(())
            }
            Self::Lateral(input) | Self::Alias { input, .. } => {
                input.bind_schema_names(bind, state, path)
            }
        }
    }
    pub(super) fn resolve(
        &mut self,
        state: &Relational,
        parameters: &mut [Option<DataType>],
    ) -> R<Shape> {
        match self {
            Self::Function {
                function,
                alias,
                aliases,
            } => {
                let (mut columns, typmods) = function.resolve(state, parameters)?;
                if aliases.is_empty() && function.uses_relation_alias()
                    && columns.len() == 1 + usize::from(function.ordinality())
                {
                    columns[0].name = alias.clone();
                }
                let mut shape = leaf_shape(columns, typmods, alias, aliases)?;
                if function.scalar_element_row() {
                    shape.namespace.scalar_rows.push((alias.clone(), 0));
                }
                shape.collations = function.collations(state, parameters)?;
                Ok(shape)
            }
            Self::Lateral(input) => input.resolve(state, parameters),
            Self::Leaf {
                query,
                alias,
                aliases,
            } => {
                let meta = query.resolve(state, parameters)?;
                let mut shape = leaf_shape(meta.columns, meta.typmods, alias, aliases)?;
                shape.collations = meta.collations.into_iter().map(|m| m.map(|m| functions::collation::materialized(Some(&m),m.ty.clone())).transpose()).collect::<R<_>>()?;
                Ok(shape)
            }
            Self::Alias {
                input,
                alias,
                aliases,
            } => {
                let shape = input.resolve(state, parameters)?;
                alias_shape(shape, alias, aliases)
            }
            Self::Join {
                left,
                right,
                kind,
                condition,
            } => {
                let left = left.resolve(state, parameters)?;
                let right = if right.lateral() {
                    let mut right = (**right).clone();
                    right.instantiate(&left, None, state, parameters, &[], &mut false)?;
                    right.resolve(state, parameters)?
                } else {
                    right.resolve(state, parameters)?
                };
                Ok(join_shape(left, right, *kind, condition, parameters)?.shape)
            }
        }
    }
    pub(super) fn validate_constants(
        &self,
        state: &Relational,
        parameters: &[Option<DataType>],
    ) -> R<()> {
        match self {
            Self::Leaf { query, .. } => query.validate_constants(state, parameters),
            Self::Function { .. } => Ok(()),
            Self::Lateral(input) | Self::Alias { input, .. } => {
                input.validate_constants(state, parameters)
            }
            Self::Join {
                left,
                right,
                kind,
                condition,
            } => {
                if right.lateral() {
                    return Ok(());
                }
                left.validate_constants(state, parameters)?;
                right.validate_constants(state, parameters)?;
                let mut parameters = parameters.to_vec();
                let a = left.clone().resolve(state, &mut parameters)?;
                let b = right.clone().resolve(state, &mut parameters)?;
                let spec = join_shape(a, b, *kind, condition, &mut parameters)?;
                bound_on(&spec, &parameters)?.plan_constant(&mut 0)?;
                Ok(())
            }
        }
    }
}
pub(super) fn leaf_shape(
    mut columns: Vec<ColumnDef>,
    typmods: Vec<i32>,
    alias: &str,
    aliases: &[String],
) -> R<Shape> {
    if aliases.len() > columns.len() {
        user_types::view_replace_error(format!(
            r#"table "{alias}" has {} columns available but {} columns specified"#,
            columns.len(), aliases.len()
        ));
        return Err(SqlError::InvalidColumnReference);
    }
    for (column, alias) in columns.iter_mut().zip(aliases) {
        column.name = alias.clone();
    }
    let visible: Vec<_> = columns
        .iter()
        .enumerate()
        .map(|(i, c)| (c.name.clone(), i))
        .collect();
    Ok(Shape {
        collations: (0..columns.len()).map(|_| None).collect(),
        columns,
        typmods,
        namespace: Namespace {
            hidden: vec![],
            scalar_rows: vec![],
            qualified: vec![(alias.into(), visible.clone())],
            visible,
        },
    })
}
fn alias_shape(mut shape: Shape, alias: &str, aliases: &[String]) -> R<Shape> {
    if aliases.len() > shape.namespace.visible.len() {
        return Err(SqlError::InvalidColumnReference);
    }
    for ((name, _), alias) in shape.namespace.visible.iter_mut().zip(aliases) {
        *name = alias.clone();
    }
    shape.namespace.scalar_rows.clear();
    shape.namespace.qualified = vec![(alias.into(), shape.namespace.visible.clone())];
    Ok(shape)
}
pub(super) fn col(n: usize) -> Scalar {
    Scalar::Column(ColumnRef {
        qualifier: None,
        name: format!("\0join_column_{n}"),
    })
}
fn required_equality(filter: &Filter) -> bool {
    match filter {
        Filter::Compare(_, op, _) => op == "=",
        Filter::Logic(a, true, b) => required_equality(a) || required_equality(b),
        _ => false,
    }
}
fn constant_condition(filter: &Filter) -> bool {
    let resolve = |_: &ColumnRef, _: usize| Err(SqlError::UnknownColumn);
    filter
        .bind(&[], &resolve, &[])
        .and_then(|f| f.plan_constant(&mut 0))
        .is_ok_and(|v| v.is_some())
}
fn full_joinable(
    filter: &Filter,
    shape: &Shape,
    offset: usize,
    parameters: &[Option<DataType>],
) -> R<bool> {
    match filter {
        Filter::Compare(a, op, b) if op == "=" => {
            let table = table(shape);
            let sides = |s: &Scalar| -> R<u8> {
                let mask = Cell::new(0);
                s.bind(
                    &|c, _| {
                        let n = shape.namespace.resolve(c)?;
                        mask.set(mask.get() | if n < offset { 1 } else { 2 });
                        Ok(BoundColumn {
                            source: 0,
                            column: n,
                        })
                    },
                    &[&table],
                    parameters,
                )?;
                Ok(mask.get())
            };
            Ok(matches!((sides(a)?, sides(b)?), (1, 2) | (2, 1)))
        }
        Filter::Logic(a, true, b) => Ok(full_joinable(a, shape, offset, parameters)?
            || full_joinable(b, shape, offset, parameters)?),
        _ => Ok(false),
    }
}
fn join_shape(
    mut left: Shape,
    right: Shape,
    kind: Kind,
    condition: &Condition,
    parameters: &mut [Option<DataType>],
) -> R<JoinShape> {
    let offset = left.columns.len();
    let mut namespace = left.namespace.combine(&right.namespace, offset)?;
    let names = match condition {
        Condition::Natural => left
            .namespace
            .visible
            .iter()
            .filter(|(n, _)| right.namespace.visible.iter().any(|(r, _)| r == n))
            .map(|(n, _)| n.clone())
            .collect(),
        Condition::Using(names, _) => names.clone(),
        _ => vec![],
    };
    let mut merged = vec![];
    let mut visible = vec![];
    let mut on = Filter::Scalar(Scalar::Literal(Value::Bool(true)));
    for name in &names {
        let c = ColumnRef {
            qualifier: None,
            name: name.clone(),
        };
        let a = left.namespace.resolve(&c)?;
        let b = right.namespace.resolve(&c)?;
        let ty = super::bind::common(
            [
                left.columns[a].data_type.clone(),
                right.columns[b].data_type.clone(),
            ]
            .into_iter(),
        )?;
        let eq = Filter::Compare(col(a), "=".into(), col(offset + b));
        on = Filter::Logic(Box::new(on), true, Box::new(eq));
        merged.push((a, b, ty, false));
        visible.push((
            name.clone(),
            offset + right.columns.len() + merged.len() - 1,
        ));
    }
    if matches!(kind, Kind::Right | Kind::Full | Kind::MergeFull) {
        for c in &mut left.columns {
            c.not_null = false;
        }
    }
    let mut right_columns = right.columns;
    if matches!(kind, Kind::Left | Kind::Full | Kind::MergeFull) {
        for c in &mut right_columns {
            c.not_null = false;
        }
    }
    left.collations.resize_with(offset, || None);
    let mut right_collations = right.collations;
    right_collations.resize_with(right_columns.len(), || None);
    left.collations.extend(right_collations);
    left.columns.extend(right_columns);
    left.typmods.extend(right.typmods);
    for ((a, b, ty, prefer_right), name) in merged.iter_mut().zip(&names) {
        let same = left.columns[*a].data_type == left.columns[offset + *b].data_type
            && left.typmods[*a] == left.typmods[offset + *b];
        let typmod = if same { left.typmods[*a] } else { -1 };
        // PostgreSQL prefers an uncoerced Var for INNER USING. Its chosen
        // value also supplies the output collation; only FULL coalesces sides.
        let left_var = left.columns[*a].data_type == *ty && left.typmods[*a] == typmod;
        let right_var = left.columns[offset + *b].data_type == *ty
            && left.typmods[offset + *b] == typmod;
        *prefer_right = matches!(kind, Kind::Right)
            || matches!(kind, Kind::Inner) && matches!(ty, DataType::Text | DataType::Varchar)
                && !left_var && right_var;
        let (a_meta, b_meta) = if matches!(kind, Kind::Full | Kind::MergeFull) {
            (left.collations[*a].as_ref(), left.collations[offset + *b].as_ref())
        } else if *prefer_right {
            (None, left.collations[offset + *b].as_ref())
        } else {
            (left.collations[*a].as_ref(), None)
        };
        let metadata = functions::collation::combined(a_meta, b_meta, ty.clone())?;
        left.collations.push(Some(metadata));
        left.columns.push(ColumnDef {
            id: 0,
            name: name.clone(),
            data_type: ty.clone(),
            not_null: false,
        });
        left.typmods.push(typmod);
    }
    visible.extend(
        namespace
            .visible
            .iter()
            .filter(|(n, _)| !names.contains(n))
            .cloned(),
    );
    namespace.visible = visible;
    if let Condition::Using(_, Some(alias)) = condition {
        if namespace.qualified.iter().any(|(a, _)| a == alias) {
            return Err(SqlError::DuplicateAlias);
        }
        namespace
            .qualified
            .push((alias.clone(), namespace.visible[..names.len()].to_vec()));
    }
    if let Condition::On(filter) = condition {
        on = *filter.clone();
    }
    let shape = Shape {
        collations: left.collations,
        columns: left.columns,
        typmods: left.typmods,
        namespace,
    };
    let table = table(&shape);
    let _collations = functions::collation::ColumnScope::enter(&table.columns, &shape.collations)?;
    let resolve = |c: &ColumnRef, _: usize| {
        shape
            .namespace
            .resolve(c)
            .map(|column| BoundColumn { source: 0, column })
    };
    if filter_contains_aggregate(&on) {
        return Err(SqlError::GroupingError);
    }
    on.infer(parameters, &resolve, &[&table])?;
    on.bind(parameters, &resolve, &[&table])?;
    if matches!(kind, Kind::Full)
        && matches!(condition, Condition::On(_))
        && !full_joinable(&on, &shape, offset, parameters)?
        && !constant_condition(&on)
    {
        return Err(SqlError::Unsupported);
    }
    Ok(JoinShape { shape, on, merged })
}
fn table(shape: &Shape) -> TableDef {
    TableDef {
        id: 0,
        name: INPUT.into(),
        columns: shape
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let mut c = c.clone();
                c.id = u64::MAX - i as u64;
                c
            })
            .collect(),
        primary_key: vec![],
    }
}
fn bound_on(spec: &JoinShape, parameters: &[Option<DataType>]) -> R<BoundFilter> {
    let table = table(&spec.shape);
    let _collations =
        functions::collation::ColumnScope::enter(&table.columns, &spec.shape.collations)?;
    spec.on.bind(
        parameters,
        &|c, _| {
            spec.shape
                .namespace
                .resolve(c)
                .map(|column| BoundColumn { source: 0, column })
        },
        &[&table],
    )
}
pub(super) fn catalog(base: &Relational, shape: &Shape) -> R<Relational> {
    let mut state = cte::catalog(base);
    let mut columns = shape.columns.clone();
    for (i, c) in columns.iter_mut().enumerate() {
        c.name = format!("\0join_column_{i}");
    }
    cte::install_collated(
        &mut state,
        INPUT,
        &columns,
        &shape.typmods,
        &shape.collations,
    )?;
    Ok(state)
}
fn star_columns<'a>(item: &Item, shape: &'a Shape) -> R<Option<&'a [(String, usize)]>> {
    Ok(match item {
        Item::Star => Some(shape.namespace.visible.as_slice()),
        Item::QualifiedStar {
            schema: None,
            relation,
        } => Some(
            shape
                .namespace
                .qualified
                .iter()
                .find(|(a, _)| a == relation)
                .ok_or(SqlError::UnknownTable)?
                .1.as_slice(),
        ),
        Item::QualifiedStar { .. } => return Err(SqlError::UnknownTable),
        _ => None,
    })
}

pub(super) fn projection_item_indices(query: &Query, shape: &Shape) -> R<Vec<usize>> {
    let mut outputs = Vec::new();
    for (i, (item, _)) in query.items.iter().enumerate() {
        let width = star_columns(item, shape)?.map_or(1, <[_]>::len);
        outputs.extend(std::iter::repeat_n(i, width));
    }
    Ok(outputs)
}

pub(super) fn prepare(query: &Query, shape: &Shape) -> R<Query> {
    let mut query = query.clone();
    let mut items = vec![];
    for (item, alias) in &query.items {
        let columns = star_columns(item, shape)?;
        if let Some(columns) = columns {
            for (name, n) in columns {
                items.push((Item::Scalar(col(*n)), Some(name.clone())));
            }
        } else {
            items.push((item.clone(), alias.clone()));
        }
    }
    query.items = items;
    // Keep ORDER BY positions and names until binding, after star expansion.
    query.predicates.clear();
    if !matches!(query.filter, Some(Filter::Clauses(_))) {
        query.filter = Some(Filter::Clauses(Box::new(QueryClauses {
            filter: query.filter.take(),
            ..Default::default()
        })));
    }
    if let Some(Filter::Clauses(c)) = &mut query.filter {
        c.join_namespace = Some(shape.namespace.clone());
    }
    Ok(query)
}

impl From {
    pub(super) fn run<'a>(
        &self,
        state: &Relational,
        source: &dyn scan::RowSource,
        params: &[Value],
        parameters: &[Option<DataType>],
        clock: Option<&super::super::super::clock::ExecutionClock>,
        execution: &'a Execution<'_>,
    ) -> R<Relation<'a>> {
        if execution.cancelled() {
            return Err(SqlError::Aborted);
        }
        let Self::Join {
            left,
            right,
            kind,
            condition,
        } = self
        else {
            return match self {
                Self::Leaf { query, .. } => {
                    query.run(state, source, params, parameters, clock, execution)
                }
                Self::Function { function, .. } => {
                    function.run(state, source, params, parameters, clock, execution)
                }
                Self::Lateral(input) | Self::Alias { input, .. } => {
                    input.run(state, source, params, parameters, clock, execution)
                }
                _ => unreachable!(),
            };
        };
        if right.lateral() {
            return lateral_run(
                left, right, *kind, condition, state, source, params, parameters, clock, execution,
            );
        }
        let mut inferred = parameters.to_vec();
        let a = left.clone().resolve(state, &mut inferred)?;
        let b = right.clone().resolve(state, &mut inferred)?;
        let offset = a.columns.len();
        let right_width = b.columns.len();
        let spec = join_shape(a, b, *kind, condition, &mut inferred)?;
        let mut on = bound_on(&spec, parameters)?;
        on.attach_clock(clock);
        let equalities = (super::super::Join {
            left: false,
            on: spec.on.clone(),
        })
        .equalities()
        .into_iter()
        .map(|(a, b)| {
            let a = spec.shape.namespace.resolve(a)?;
            let b = spec.shape.namespace.resolve(b)?;
            Ok(if a < offset && b >= offset && b < offset + right_width {
                Some((a, b - offset))
            } else if b < offset && a >= offset && a < offset + right_width {
                Some((b, a - offset))
            } else {
                None
            })
        })
        .collect::<R<Vec<_>>>()?;
        let keys: Vec<_> = equalities
            .into_iter()
            .flatten()
            .filter(|(a, b)| {
                functions::join_key_supported(
                    &spec.shape.columns[*a].data_type,
                    &spec.shape.columns[offset + b].data_type,
                )
            })
            .collect();
        let key_metadata = keys
            .iter()
            .map(|(a, b)| {
                functions::collation::combined(
                    spec.shape.collations.get(*a).and_then(Option::as_ref),
                    spec.shape
                        .collations
                        .get(offset + b)
                        .and_then(Option::as_ref),
                    spec.shape.columns[*a].data_type.clone(),
                )
                .map(Some)
            })
            .collect::<R<Vec<_>>>()?;
        for metadata in key_metadata.iter().flatten() { metadata.check_comparison_collation()?; }
        let a = left.run(state, source, params, parameters, clock, execution)?;
        let b = right.run(state, source, params, parameters, clock, execution)?;
        let scratch = RetentionScope::new(execution);
        scratch
            .borrow()
            .charge(b.rows.len() * (std::mem::size_of::<bool>() + 1))?;
        let mut matched = vec![false; b.rows.len()];
        let mut hash: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
        let key = |row: &SqlRow, left: bool| -> R<Option<Vec<u8>>> {
            keys.iter()
                .map(|(a, b)| {
                    let ty = &spec.shape.columns[offset + b].data_type;
                    if left {
                        functions::join_key(&row.values[*a], &spec.shape.columns[*a].data_type, ty)
                    } else {
                        functions::join_key(&row.values[*b], ty, ty)
                    }
                })
                .collect::<R<Option<Vec<_>>>>()?
                .map(|values| {
                    encode_key(
                        &values
                            .into_iter()
                            .zip(&key_metadata)
                            .map(|(v, m)| functions::collation::equality_key(v, m.as_ref()))
                            .collect::<R<Vec<_>>>()?,
                    )
                })
                .transpose()
        };
        if !keys.is_empty() {
            for (i, row) in b.rows.iter().enumerate() {
                execution.comparison()?;
                if let Some(key) = key(row, false)? {
                    scratch.borrow().charge(128 + key.len())?;
                    hash.entry(key).or_default().push(i);
                }
            }
        }
        // Reserve temporary tuple clones (including merged values) as well as
        // retained input/output relations and hash buckets.
        let max_bytes = |rows: &Relation<'_>| {
            rows.rows
                .iter()
                .map(|r| r.values.iter().map(value_bytes).sum::<usize>())
                .max()
                .unwrap_or(0)
        };
        scratch.borrow().charge(
            2 * (max_bytes(&a)
                + max_bytes(&b)
                + spec.shape.columns.len() * std::mem::size_of::<Value>()),
        )?;
        let mut out = Relation::new(
            spec.shape.columns.clone(),
            spec.shape.typmods.clone(),
            execution,
        );
        let mut bytes = 0;
        for row in &a.rows {
            execution.comparison()?;
            let probe = if keys.is_empty() {
                None
            } else {
                key(row, true)?
            };
            let indices = probe
                .as_ref()
                .and_then(|k| hash.get(k))
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let scan = if keys.is_empty() {
                0..b.rows.len()
            } else {
                0..0
            };
            let mut found = false;
            for i in indices.iter().copied().chain(scan) {
                execution.comparison()?;
                let values = pair(Some(row), Some(&b.rows[i]), offset, right_width);
                let tuple = Tuple {
                    source_versions: vec![],
                    values: vec![values],
                    version: row.version.max(b.rows[i].version),
                };
                if on.eval(&tuple, params, &mut bytes)? == Some(true) {
                    found = true;
                    matched[i] = true;
                    emit(
                        tuple.values.into_iter().next().unwrap(),
                        tuple.version,
                        &spec,
                        *kind,
                        offset,
                        clock,
                        &mut out,
                    )?;
                }
            }
            if !found && matches!(kind, Kind::Left | Kind::Full | Kind::MergeFull) {
                emit(
                    pair(Some(row), None, offset, right_width),
                    row.version,
                    &spec,
                    *kind,
                    offset,
                    clock,
                    &mut out,
                )?;
            }
        }
        if matches!(kind, Kind::Right | Kind::Full | Kind::MergeFull) {
            for (row, matched) in b.rows.iter().zip(matched) {
                execution.comparison()?;
                if !matched {
                    emit(
                        pair(None, Some(row), offset, right_width),
                        row.version,
                        &spec,
                        *kind,
                        offset,
                        clock,
                        &mut out,
                    )?;
                }
            }
        }
        Ok(out)
    }
}
fn pair(a: Option<&SqlRow>, b: Option<&SqlRow>, aw: usize, bw: usize) -> Vec<Value> {
    let mut out = a.map_or_else(|| vec![Value::Null; aw], |r| r.values.clone());
    out.extend(b.map_or_else(|| vec![Value::Null; bw], |r| r.values.clone()));
    out
}
fn emit(
    mut values: Vec<Value>,
    version: u64,
    spec: &JoinShape,
    kind: Kind,
    offset: usize,
    clock: Option<&super::super::super::clock::ExecutionClock>,
    out: &mut Relation<'_>,
) -> R<()> {
    for (a, b, ty, prefer_right) in &spec.merged {
        let value = if *prefer_right {
            &values[offset + b]
        } else {
            match kind {
                Kind::Full | Kind::MergeFull if values[*a] == Value::Null => &values[offset + b],
                _ => &values[*a],
            }
        };
        values.push(functions::coerce_clocked(value.clone(), ty, clock)?);
    }
    out.push(SqlRow { values, version })
}

impl From {
    fn lateral(&self) -> bool {
        match self {
            Self::Function { .. } | Self::Lateral(_) => true,
            Self::Alias { input, .. } => input.lateral(),
            Self::Join { left, right, .. } => left.lateral() || right.lateral(),
            _ => false,
        }
    }
    pub(super) fn locals(
        &self,
        state: &Relational,
        parameters: &[Option<DataType>],
        out: &mut Vec<(String, Vec<String>)>,
    ) {
        match self {
            Self::Join { left, right, .. } => {
                left.locals(state, parameters, out);
                right.locals(state, parameters, out);
            }
            Self::Lateral(input) => input.locals(state, parameters, out),
            Self::Function { function, alias, aliases } => {
                let names = function.local_column_names(state, parameters, alias, aliases);
                out.push((alias.clone(), names));
            }
            Self::Leaf { alias, .. } | Self::Alias { alias, .. } => {
                let columns = self
                    .clone()
                    .resolve(state, &mut parameters.to_vec())
                    .map(|s| s.namespace.visible.into_iter().map(|(n, _)| n).collect())
                    .unwrap_or_default();
                out.push((alias.clone(), columns));
            }
        }
    }
    pub(super) fn instantiate(
        &mut self,
        shape: &Shape,
        row: Option<&SqlRow>,
        state: &Relational,
        parameters: &[Option<DataType>],
        blocked: &[(String, Vec<String>)],
        used: &mut bool,
    ) -> R<()> {
        match self {
            Self::Leaf { query, .. } => {
                let (p, c) =
                    subquery::instantiate_scoped(query, shape, row, state, parameters, blocked)?;
                **query = p;
                *used |= c;
            }
            Self::Function { function, .. } => {
                if let Some(input) = &mut function.input {
                    let (p, c) = subquery::instantiate_scoped(
                        input, shape, row, state, parameters, blocked,
                    )?;
                    **input = p;
                    *used |= c;
                }
                for s in &mut function.args {
                    *used |= subquery::outer_scoped(s, shape, row, blocked)?;
                }
            }
            Self::Alias { input, .. } | Self::Lateral(input) => {
                input.instantiate(shape, row, state, parameters, blocked, used)?
            }
            Self::Join {
                left,
                right,
                condition,
                ..
            } => {
                left.instantiate(shape, row, state, parameters, blocked, used)?;
                right.instantiate(shape, row, state, parameters, blocked, used)?;
                if let Condition::On(on) = condition {
                    let mut s = Scalar::Predicate(on.clone());
                    *used |= subquery::outer_scoped(&mut s, shape, row, blocked)?;
                    if let Scalar::Predicate(on2) = s {
                        *on = on2;
                    }
                }
            }
        }
        Ok(())
    }
}
#[expect(clippy::too_many_arguments, reason = "shared query execution context")]
fn lateral_run<'a>(
    left: &From,
    right: &From,
    kind: Kind,
    condition: &Condition,
    state: &Relational,
    source: &dyn scan::RowSource,
    params: &[Value],
    parameters: &[Option<DataType>],
    clock: Option<&super::super::super::clock::ExecutionClock>,
    execution: &'a Execution<'_>,
) -> R<Relation<'a>> {
    if matches!(kind, Kind::Right | Kind::Full | Kind::MergeFull) {
        return Err(SqlError::Unsupported);
    }
    let mut inferred = parameters.to_vec();
    let a = left.clone().resolve(state, &mut inferred)?;
    let mut probe = right.clone();
    probe.instantiate(&a, None, state, &inferred, &[], &mut false)?;
    let b = probe.resolve(state, &mut inferred)?;
    let aw = a.columns.len();
    let bw = b.columns.len();
    let spec = join_shape(
        left.clone().resolve(state, &mut inferred)?,
        b,
        kind,
        condition,
        &mut inferred,
    )?;
    let mut on = bound_on(&spec, parameters)?;
    on.attach_clock(clock);
    let mut out = Relation::new(
        spec.shape.columns.clone(),
        spec.shape.typmods.clone(),
        execution,
    );
    let left = left.run(state, source, params, parameters, clock, execution)?;
    for row in &left.rows {
        execution.comparison()?;
        let mut bound = right.clone();
        bound.instantiate(&a, Some(row), state, parameters, &[], &mut false)?;
        let right = bound.run(state, source, params, parameters, clock, execution)?;
        let mut matched = false;
        for r in &right.rows {
            execution.comparison()?;
            let tuple = Tuple {
                source_versions: vec![],
                values: vec![pair(Some(row), Some(r), aw, bw)],
                version: row.version.max(r.version),
            };
            if on.eval(&tuple, params, &mut 0)? == Some(true) {
                matched = true;
                emit(
                    tuple.values.into_iter().next().unwrap(),
                    tuple.version,
                    &spec,
                    kind,
                    aw,
                    clock,
                    &mut out,
                )?;
            }
        }
        if !matched && matches!(kind, Kind::Left) {
            emit(
                pair(Some(row), None, aw, bw),
                row.version,
                &spec,
                kind,
                aw,
                clock,
                &mut out,
            )?;
        }
    }
    Ok(out)
}

impl Clone for Shape {
    fn clone(&self) -> Self {
        Self {
            columns: self.columns.clone(),
            typmods: self.typmods.clone(),
            namespace: self.namespace.clone(),
            collations: self
                .collations
                .iter()
                .map(|m| {
                    m.as_ref().map(|m| {
                        functions::collation::combined(Some(m), None, m.ty.clone())
                            .expect("resolved derivation")
                    })
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use Value::{Int as I, Null as N, Text as T};
    fn run(state: &mut Relational, sql: &str) -> SqlResult {
        let p = Prepared::parse(sql, 1, state.epoch).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        state
            .execute(&p, &[], None)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
    }
    fn state() -> Relational {
        let mut s = Relational::default();
        run(
            &mut s,
            "CREATE TABLE ja(id int PRIMARY KEY,x int,label text)",
        );
        run(
            &mut s,
            "CREATE TABLE jb(id int PRIMARY KEY,y int,label text)",
        );
        run(&mut s, "INSERT INTO ja VALUES(1,10,'a'),(2,20,'b')");
        run(&mut s, "INSERT INTO jb VALUES(2,200,'c'),(3,300,'d')");
        s
    }
    fn rows(result: SqlResult) -> Vec<Vec<Value>> {
        result.rows.into_iter().map(|r| r.values).collect()
    }
    #[test]
    fn kinds_null_extension_and_qualified_names() {
        let mut s = state();
        for (kind, expected) in [
            ("INNER", vec![vec![I(2), I(2)]]),
            ("LEFT", vec![vec![I(1), N], vec![I(2), I(2)]]),
            ("RIGHT", vec![vec![I(2), I(2)], vec![N, I(3)]]),
            ("FULL", vec![vec![I(1), N], vec![I(2), I(2)], vec![N, I(3)]]),
        ] {
            let outer = if kind == "INNER" { "" } else { "OUTER" };
            assert_eq!(
                rows(run(
                    &mut s,
                    &format!(
                        "SELECT a.id,b.id FROM (ja a {kind} {outer} JOIN jb b ON a.id=b.id) ORDER BY a.id,b.id"
                    )
                )),
                expected,
                "{kind}"
            );
        }
        assert_eq!(
            rows(run(
                &mut s,
                "SELECT a.id,b.id FROM ja a CROSS JOIN jb b ORDER BY a.id,b.id"
            )),
            vec![
                vec![I(1), I(2)],
                vec![I(1), I(3)],
                vec![I(2), I(2)],
                vec![I(2), I(3)]
            ]
        );
    }
    #[test]
    fn using_natural_order_and_merge_values() {
        let mut s = state();
        let result = run(
            &mut s,
            "SELECT * FROM ja FULL JOIN jb USING(id) ORDER BY id",
        );
        assert_eq!(result.columns, ["id", "x", "label", "y", "label"]);
        assert_eq!(
            rows(result),
            vec![
                vec![I(1), I(10), T("a".into()), N, N],
                vec![I(2), I(20), T("b".into()), I(200), T("c".into())],
                vec![I(3), N, N, I(300), T("d".into())]
            ]
        );
        assert_eq!(
            rows(run(
                &mut s,
                "SELECT id,ja.id,jb.id,j.id FROM ja RIGHT JOIN jb USING(id) AS j ORDER BY id"
            )),
            vec![vec![I(2), I(2), I(2), I(2)], vec![I(3), N, I(3), I(3)]]
        );
        assert_eq!(
            run(&mut s, "SELECT ja.*,jb.* FROM ja FULL JOIN jb USING(id)").columns,
            ["id", "x", "label", "id", "y", "label"]
        );
        assert_eq!(
            run(&mut s, "SELECT * FROM ja NATURAL JOIN ja b").columns,
            ["id", "x", "label"]
        );
        assert_eq!(
            run(
                &mut s,
                "SELECT * FROM (VALUES(1)) a(x) NATURAL JOIN (VALUES(2)) b(y)"
            )
            .rows
            .len(),
            1
        );
        assert_eq!(
            rows(run(
                &mut s,
                "SELECT * FROM (VALUES(NULL::int),(1)) a(id) NATURAL FULL JOIN (VALUES(NULL::int),(1),(2)) b(id) ORDER BY id"
            )),
            vec![vec![I(1)], vec![I(2)], vec![N], vec![N]]
        );
    }
    #[test]
    fn errors_precedence_and_task_classification() {
        let s = state();
        for (sql, error) in [
            (
                "SELECT * FROM ja FULL JOIN jb ON ja.id<jb.id",
                SqlError::Unsupported,
            ),
            ("SELECT id FROM ja CROSS JOIN jb", SqlError::AmbiguousColumn),
            (
                "SELECT * FROM ja a CROSS JOIN jb a",
                SqlError::DuplicateAlias,
            ),
            ("SELECT * FROM ja JOIN jb USING(x)", SqlError::UnknownColumn),
            (
                "SELECT * FROM ja a,ja b JOIN jb c ON a.id=c.id",
                SqlError::UnknownTable,
            ),
        ] {
            let result = Prepared::parse(sql, 1, s.epoch).and_then(|p| p.describe(&s));
            assert!(
                matches!(result,Err(ref e) if *e == error),
                "{sql}: {result:?}"
            );
        }
        assert!(matches!(
            Prepared::parse("SELECT * FROM ja FULL JOIN jb ON ja.id<jb.id", 1, 0),
            Err(SqlError::Unsupported)
        ));
        for (error, code) in [
            (SqlError::Unsupported, "0A000"),
            (SqlError::AmbiguousColumn, "42702"),
            (SqlError::DuplicateAlias, "42712"),
            (SqlError::UnknownColumn, "42703"),
        ] {
            assert_eq!(crate::sqlstate(&crate::CoreError::Sql(error)), code);
        }
        assert_eq!(
            Prepared::task_class_of_sql("SELECT * FROM ja CROSS JOIN jb").unwrap(),
            crate::task::StatementClass::Other
        );
        assert_eq!(
            Prepared::task_class_of_sql("SELECT * FROM ja CROSS JOIN information_schema.tables")
                .unwrap(),
            // GD-0061: agents admit Catalog, and the planner path has no task
            // catalog filtering or row scope yet, so every planner statement is
            // Other in a task session, catalog relations included.
            crate::task::StatementClass::Other
        );
    }
    #[test]
    fn comma_explicit_tree_aliases_cte_values_and_downstream_clauses() {
        let mut s = state();
        run(&mut s, "CREATE TABLE jc(id int PRIMARY KEY)");
        run(&mut s, "INSERT INTO jc VALUES(2)");
        let p = Prepared::parse(
            "SELECT a.id FROM ja a CROSS JOIN jb b WHERE b.id IN(SELECT id FROM jc) ORDER BY a.id",
            1,
            s.epoch,
        )
        .unwrap();
        assert!(p.read_tables().contains(&"jc".to_string()));
        assert!(p.relations.iter().any(|r| r.name == "jc"));
        assert!(p.relations.iter().all(|r| r.name != "plan_join"));
        assert_eq!(
            rows(s.execute(&p, &[], None).unwrap()),
            vec![vec![I(1)], vec![I(2)]]
        );
        assert_eq!(
            rows(run(
                &mut s,
                "SELECT a.id,b.id,c.id FROM ja a,jb b JOIN ja c ON b.id=c.id ORDER BY a.id"
            )),
            vec![vec![I(1), I(2), I(2)], vec![I(2), I(2), I(2)]]
        );
        assert_eq!(
            rows(run(
                &mut s,
                "WITH q AS (SELECT id FROM jb) SELECT a.id,q.id FROM ja a FULL JOIN q ON a.id=q.id ORDER BY a.id,q.id"
            )),
            vec![vec![I(1), N], vec![I(2), I(2)], vec![N, I(3)]]
        );
        assert_eq!(
            rows(run(
                &mut s,
                "SELECT a.id,v.n FROM ja a JOIN (VALUES(2),(3)) AS v(n) ON a.id=v.n"
            )),
            vec![vec![I(2), I(2)]]
        );
        assert_eq!(
            rows(run(
                &mut s,
                "SELECT d.child,d.name FROM (SELECT a.id,b.label FROM ja a JOIN jb b ON a.id=b.id) AS d(child,name)"
            )),
            vec![vec![I(2), T("c".into())]]
        );
        assert_eq!(
            rows(run(
                &mut s,
                "SELECT j.k FROM (ja FULL JOIN jb USING(id)) AS j(k) ORDER BY j.k"
            )),
            vec![vec![I(1)], vec![I(2)], vec![I(3)]]
        );
        assert_eq!(
            rows(run(
                &mut s,
                "SELECT a.id,count(*) AS n,sum(b.y) AS total FROM ja a CROSS JOIN jb b WHERE b.y>0 GROUP BY a.id HAVING count(*)>1 ORDER BY a.id DESC LIMIT 1"
            )),
            vec![vec![I(2), I(2), I(500)]]
        );
        assert_eq!(
            rows(run(
                &mut s,
                "SELECT DISTINCT a.id FROM ja a CROSS JOIN jb b ORDER BY a.id DESC OFFSET 1"
            )),
            vec![vec![I(1)]]
        );
        assert_eq!(
            rows(run(&mut s, "SELECT * FROM (VALUES(1)),(VALUES(2))")),
            vec![vec![I(1), I(2)]]
        );
        assert_eq!(
            rows(run(
                &mut s,
                "SELECT a.key,b.id FROM ja AS a(key),jb b WHERE a.key=b.id"
            )),
            vec![vec![I(2), I(2)]]
        );
        let p = Prepared::parse("SELECT a.id,b.id FROM ja a FULL JOIN jb b ON a.id=b.id AND b.y>$1 WHERE a.id IS NULL OR a.id>$2 ORDER BY a.id,b.id",1,s.epoch).unwrap();
        assert_eq!(
            p.describe(&s).unwrap().parameters,
            vec![DataType::Integer, DataType::Integer]
        );
        assert_eq!(
            rows(s.execute(&p, &[I(100), I(1)], None).unwrap()),
            vec![vec![I(2), I(2)], vec![N, I(3)]]
        );
    }
    #[test]
    fn hashing_twelve_sources_and_shared_budgets() {
        let mut s = state();
        let mut sql = "SELECT a0.id FROM ja a0 CROSS JOIN (VALUES(1)) v(n)".to_string();
        for n in 1..12 {
            sql.push_str(&format!(" JOIN ja a{n} ON a{n}.id=a{}.id", n - 1));
        }
        sql.push_str(" ORDER BY a0.id");
        assert_eq!(rows(run(&mut s, &sql)), vec![vec![I(1)], vec![I(2)]]);
        let from = (0..12)
            .map(|n| format!("ja a{n}"))
            .collect::<Vec<_>>()
            .join(",");
        let filter = (1..12)
            .map(|n| format!("a{n}.id=a{}.id", n - 1))
            .collect::<Vec<_>>()
            .join(" AND ");
        let mut parser =
            parse::parser(&lex(&format!("SELECT a0.id FROM {from} WHERE {filter}")).unwrap());
        let plan = Plan::parse(&mut parser).unwrap();
        let mut cancel = || false;
        let execution = Execution::new(&mut cancel, &[]);
        assert_eq!(
            plan.run(&s, &scan::LegacyRows(&s), &[], &[], None, &execution)
                .unwrap()
                .rows
                .len(),
            2
        );
        assert!(
            execution.visits.get() < 1000,
            "comma equality joins must probe keys"
        );
        let p = Prepared::parse("SELECT * FROM ja FULL JOIN jb USING(id)", 1, s.epoch).unwrap();
        assert_eq!(p.read_tables(), vec!["ja".to_string(), "jb".to_string()]);
        let Statement::Plan(plan) = p.statement.get() else {
            panic!("expected plan");
        };
        assert_eq!(
            plan.execute(&s, &scan::LegacyRows(&s), 0, &[], &[], None, &mut || true),
            Err(SqlError::Aborted)
        );
        let mut cancel = || false;
        let execution = Execution::new(&mut cancel, &[]);
        *execution.retention.borrow_mut() = scan::Retention::new(32);
        assert!(matches!(
            plan.run(&s, &scan::LegacyRows(&s), &[], &[], None, &execution),
            Err(SqlError::Limit)
        ));
        assert_eq!(execution.retention.borrow().used(), 0);
        *execution.retention.borrow_mut() = scan::Retention::new(scan::retention_limit());
        execution.visits.set(MAX_JOIN_VISITS);
        assert!(matches!(
            plan.run(&s, &scan::LegacyRows(&s), &[], &[], None, &execution),
            Err(SqlError::Limit)
        ));
    }
}
