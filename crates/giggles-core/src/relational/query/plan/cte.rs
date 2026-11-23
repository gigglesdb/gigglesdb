//! Statement-local materialized tables, including recursive working tables.
use super::*;
use bind::Meta;
use exec::Relation;

#[derive(Debug, Clone)]
pub(super) struct Cte {
    name: String,
    aliases: Vec<String>,
    pub query: Plan,
    pub dml: Option<Box<Prepared>>,
    dml_parameters: BTreeSet<usize>,
    recursive: bool,
    search: Option<Search>,
    cycle: Option<Cycle>,
}
#[derive(Debug, Clone)]
struct Search {
    depth: bool,
    by: Vec<String>,
    column: String,
}
#[derive(Debug, Clone)]
struct Cycle {
    by: Vec<String>,
    column: String,
    path: String,
    mark: Scalar,
    default: Scalar,
}

struct Scope {
    definitions: Vec<(String, Cte)>,
    recursive: bool,
    names: BTreeSet<String>,
}

pub(super) fn parse(
    tokens: &[Token],
    following: Option<&Token>,
    owner: &mut Parser,
    depth: usize,
    next: &mut usize,
) -> parse::ParseResult<Plan> {
    // WITH bodies form a continuation chain. Store those scopes on the heap
    // so scalar parsing in the innermost body has the same stack as a root query.
    let mut remaining = tokens;
    let mut depth = depth;
    let mut scopes = Vec::new();
    loop {
        let (scope, at) = parse_scope(remaining, owner, depth, next)?;
        scopes.push(scope);
        remaining = &remaining[at..];
        if depth >= subplans::MAX_DEPTH {
            return Err(SqlError::Limit.into());
        }
        depth += 1;
        if !remaining.first().is_some_and(|t| parse::word(t, "with")) {
            break;
        }
    }
    let mut body = parse::expression(remaining, following, owner, depth, next)?;
    for scope in scopes.into_iter().rev() {
        body = finish(scope.definitions, scope.recursive, scope.names, body, owner)?;
    }
    Ok(body)
}

fn parse_scope(
    tokens: &[Token],
    owner: &mut Parser,
    depth: usize,
    next: &mut usize,
) -> parse::ParseResult<(Scope, usize)> {
    let mut p = parse::parser(tokens);
    p.need("with")?;
    let recursive = p.word("recursive");
    let mut definitions = vec![];
    let mut names = BTreeSet::new();
    loop {
        let alias = p.name()?;
        if !names.insert(alias.clone()) {
            return Err(ParseError::Definite(SqlError::DuplicateTable));
        }
        let aliases = if p.sym('(') {
            names_list(&mut p, true)?
        } else {
            vec![]
        };
        p.need("as")?;
        if p.word("not") {
            p.need("materialized")?;
        } else {
            p.word("materialized");
        }
        if !p.sym('(') {
            return Err(SqlError::Syntax.into());
        }
        let open = p.at - 1;
        let close = subquery_end(&p.tokens, open)?;
        let tokens = &p.tokens[open + 1..close];
        let dml = if tokens.first().is_some_and(|t| {
            ["insert", "update", "delete", "merge"]
                .iter()
                .any(|w| parse::word(t, w))
        }) {
            if depth != 0 {
                return Err(ParseError::Definite(SqlError::Unsupported));
            }
            Some(parse_dml(tokens, owner)?)
        } else {
            None
        };
        let query = if dml.is_some() {
            Plan::new(Body::Values(vec![]))
        } else {
            parse::child_expression(tokens, p.tokens.get(close), owner, depth, next)?
        };
        let dml_parameters = tokens
            .iter()
            .filter_map(|t| match t {
                Token::Param(n) => Some(*n),
                _ => None,
            })
            .collect();
        p.at = close + 1;
        let search = if p.word("search") {
            let depth = if p.word("depth") {
                true
            } else {
                p.need("breadth")?;
                false
            };
            p.need("first")?;
            p.need("by")?;
            let by = names_list(&mut p, false)?;
            p.need("set")?;
            Some(Search {
                depth,
                by,
                column: p.name()?,
            })
        } else {
            None
        };
        let cycle = if p.word("cycle") {
            let by = names_list(&mut p, false)?;
            p.need("set")?;
            let column = p.name()?;
            let (mark, default) = if p.word("to") {
                let mark = Scalar::parse_expr(&mut p, 0, false)?;
                p.need("default")?;
                (mark, Scalar::parse_expr(&mut p, 0, false)?)
            } else {
                (
                    Scalar::Literal(Value::Bool(true)),
                    Scalar::Literal(Value::Bool(false)),
                )
            };
            p.need("using")?;
            Some(Cycle {
                by,
                column,
                path: p.name()?,
                mark,
                default,
            })
        } else {
            None
        };
        let name = format!("\0plan_cte_{}", *next);
        *next += 1;
        definitions.push((
            alias,
            Cte {
                name,
                aliases,
                query,
                dml,
                dml_parameters,
                recursive: false,
                search,
                cycle,
            },
        ));
        if definitions.len() > max_statement_tokens() {
            return Err(SqlError::Limit.into());
        }
        if !p.sym(',') {
            break;
        }
    }
    Ok((
        Scope {
            definitions,
            recursive,
            names,
        },
        p.at,
    ))
}

fn finish(
    mut definitions: Vec<(String, Cte)>,
    recursive: bool,
    names: BTreeSet<String>,
    mut body: Plan,
    owner: &mut Parser,
) -> parse::ParseResult<Plan> {
    for i in 0..definitions.len() {
        let scope = if recursive { definitions.len() } else { i };
        for j in 0..scope {
            let (alias, cte) = &definitions[j];
            let (alias, name) = (alias.clone(), cte.name.clone());
            remap(&mut definitions[i].1.query, &alias, &name);
            if let Some(dml) = &mut definitions[i].1.dml {
                dml::remap(dml, &alias, &name);
            }
        }
        let name = definitions[i].1.name.clone();
        definitions[i].1.recursive = references(&definitions[i].1.query, &name) > 0;
        if definitions[i].1.recursive {
            recursive_parts(&definitions[i].1).map_err(ParseError::Definite)?;
        } else if definitions[i].1.search.is_some() || definitions[i].1.cycle.is_some() {
            return Err(ParseError::Definite(SqlError::InvalidRecursion));
        }
        // A forward reference is either mutual recursion or an unsupported dependency cycle.
        for (_, later) in &definitions[i + 1..] {
            if references(&definitions[i].1.query, &later.name) > 0 {
                return Err(ParseError::Definite(SqlError::InvalidRecursion));
            }
        }
    }
    for (alias, cte) in &definitions {
        remap(&mut body, alias, &cte.name);
    }
    for (_, cte) in &definitions {
        if cte.dml.as_ref().is_some_and(|p| p.returning().is_none() && !matches!(&p.statement, Statement::Plan(plan) if (plan.mutation.as_ref().is_some_and(|m| m.has_returning()) || plan.merge.as_ref().is_some_and(|m| m.has_returning())))) {
            let referenced = references(&body, &cte.name) > 0
                || definitions.iter().any(|(_, other)| {
                    references(&other.query, &cte.name) > 0
                        || other
                            .dml
                            .as_ref()
                            .is_some_and(|p| p.read_tables().contains(&cte.name))
                });
            if referenced {
                return Err(ParseError::Definite(SqlError::Unsupported));
            }
        }
    }
    // The new WITH scope encloses any WITH in its body.
    let mut ctes: Vec<_> = definitions.into_iter().map(|(_, c)| c).collect();
    ctes.append(&mut body.ctes);
    body.ctes = ctes;
    // References used only as virtual table names must not enter schema authorization.
    let base_tables = body.read_tables();
    owner.relations.retain(|r| {
        r.schema.is_some() || !names.contains(&r.name) || base_tables.contains(&r.name)
    });
    Ok(body)
}
// Keep Prepared construction out of every recursive WITH parse frame.
fn parse_dml(tokens: &[Token], owner: &mut Parser) -> R<Box<Prepared>> {
    Ok(Box::new(dml::parse(tokens, owner)?))
}

fn names_list(p: &mut Parser, close: bool) -> R<Vec<String>> {
    let mut names = vec![];
    loop {
        names.push(p.name()?);
        if names.len() > MAX_COLUMNS {
            return Err(SqlError::Limit);
        }
        if !p.sym(',') {
            break;
        }
    }
    if close && !p.sym(')') {
        return Err(SqlError::Syntax);
    }
    Ok(names)
}
pub(super) fn remap(plan: &mut Plan, alias: &str, name: &str) {
    if let Some(m) = &mut plan.mutation { m.remap(alias, name); }
    if let Some(m) = &mut plan.merge { m.remap(alias, name); }
    for cte in &mut plan.ctes {
        remap(&mut cte.query, alias, name);
        if let Some(dml) = &mut cte.dml {
            dml::remap(dml, alias, name);
        }
    }
    match &mut plan.body {
        Body::Select(s) => {
            remap_query(&mut s.query, alias, name);
            for apply in &mut s.applies { apply.remap(alias, name); }
            if let Some(i) = &mut s.input {
                remap(&mut i.query, alias, name);
            }
            if let Some(from) = &mut s.from {
                from.remap(alias, name);
            }
        }
        Body::Set { left, right, .. } => {
            remap(left, alias, name);
            remap(right, alias, name);
        }
        _ => {}
    }
}
pub(super) fn remap_query(q: &mut Query, alias: &str, name: &str) {
    for source in &mut q.sources {
        if source.name == alias {
            source.name = name.into();
        }
    }
    for (item, _) in &mut q.items {
        if let Item::Scalar(s) | Item::Aggregate(_, Some(s)) = item {
            remap_scalar(s, alias, name);
        }
    }
    for (s, _, _) in &mut q.order {
        remap_scalar(s, alias, name);
    }
    for f in q
        .filter
        .iter_mut()
        .chain(q.joins.iter_mut().map(|j| &mut j.on))
    {
        remap_filter(f, alias, name);
    }
}
fn remap_scalar(s: &mut Scalar, alias: &str, name: &str) {
    match s {
        Scalar::Predicate(f) => remap_filter(f, alias, name),
        Scalar::Case(c) => {
            for (f, s) in &mut c.arms {
                remap_filter(f, alias, name);
                remap_scalar(s, alias, name);
            }
            if let Some(s) = &mut c.otherwise {
                remap_scalar(s, alias, name);
            }
        }
        Scalar::Unary(_, s) | Scalar::Cast(s, _) | Scalar::Domain(s, ..) => {
            remap_scalar(s, alias, name)
        }
        Scalar::Binary(_, a, b) => {
            remap_scalar(a, alias, name);
            remap_scalar(b, alias, name);
        }
        Scalar::Call(_, args) => {
            for s in args {
                remap_scalar(s, alias, name);
            }
        }
        Scalar::SubstringOrder(args) => {
            for s in args {
                remap_scalar(s, alias, name);
            }
        }
        _ => {}
    }
}
fn remap_filter(f: &mut Filter, alias: &str, name: &str) {
    match f {
        Filter::Clauses(c) => {
            if let Some(q) = &mut c.derived {
                remap_query(q, alias, name);
            }
            for f in c.filter.iter_mut().chain(&mut c.having) {
                remap_filter(f, alias, name);
            }
            for s in &mut c.group {
                remap_scalar(s, alias, name);
            }
        }
        Filter::Exists { query, .. } => remap_query(query, alias, name),
        Filter::InSubquery { value, query, .. } => {
            remap_scalar(value, alias, name);
            remap_query(query, alias, name);
        }
        Filter::Not(f) | Filter::Is(f, _, _) => remap_filter(f, alias, name),
        Filter::Logic(a, _, b) => {
            remap_filter(a, alias, name);
            remap_filter(b, alias, name);
        }
        Filter::Compare(a, _, b) => {
            remap_scalar(a, alias, name);
            remap_scalar(b, alias, name);
        }
        Filter::Scalar(s) | Filter::Null(s, _) => remap_scalar(s, alias, name),
        Filter::Like {
            value,
            pattern,
            escape,
            ..
        } => {
            remap_scalar(value, alias, name);
            remap_scalar(pattern, alias, name);
            if let Some(s) = escape {
                remap_scalar(s, alias, name);
            }
        }
    }
}
fn references(plan: &Plan, name: &str) -> usize {
    fn query_references(q: &Query, name: &str) -> usize {
        let mut n = q.sources.iter().filter(|s| s.name == name).count();
        if let Some(child) = q.derived() {
            n += query_references(child, name);
        }
        for filter in q.filter.iter().chain(q.joins.iter().map(|j| &j.on)) {
            let _ = filter.visit_subqueries(&mut |_, child| {
                n += query_references(child, name);
                Ok(())
            });
        }
        n
    }
    let mut n = 0;
    plan.visit_queries(&mut |q| n += query_references(q, name));
    n
}
fn recursive_parts(cte: &Cte) -> R<(&Plan, &Plan, bool)> {
    if !cte.query.ctes.is_empty()
        || !cte.query.order.is_empty()
        || cte.query.limit.is_some()
        || cte.query.offset.is_some()
    {
        return Err(SqlError::InvalidRecursion);
    }
    let Body::Set {
        op: SetOp::Union,
        all,
        left,
        right,
    } = &cte.query.body
    else {
        return Err(SqlError::InvalidRecursion);
    };
    if references(left, &cte.name) != 0 || references(right, &cte.name) != 1 {
        return Err(SqlError::InvalidRecursion);
    }
    let mut invalid = false;
    right.visit_queries(&mut |q| {
        if let Some(i) = q.sources.iter().position(|s| s.name == cte.name) {
            invalid |= q.derived().is_some()
                || !q.group().is_empty()
                || q.having().is_some()
                || q.items
                    .iter()
                    .any(|(item, _)| matches!(item, Item::Aggregate(..)))
                || i > 0 && q.joins[i - 1].left;
        }
    });
    if invalid {
        return Err(SqlError::InvalidRecursion);
    }
    Ok((left, right, *all))
}

/// A rows-free catalog overlay preserves the base metadata used by scalar binding.
pub(super) fn catalog(base: &Relational) -> Relational {
    Relational {
        tables: base.tables.clone(),
        view_names: base.view_names.clone(),
        ext: base.ext.clone(),
        epoch: base.epoch,
        generation: base.generation,
        ..Relational::default()
    }
}
pub(super) fn install(
    state: &mut Relational,
    name: &str,
    columns: &[ColumnDef],
    typmods: &[i32],
) -> R<()> {
    let mut id = state
        .tables
        .values()
        .flat_map(|t| std::iter::once(t.id).chain(t.columns.iter().map(|c| c.id)))
        .chain(state.ext.columns.keys().copied())
        .filter(|id| *id < (1 << 62))
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or(SqlError::Limit)?;
    let table_id = id;
    let mut columns = columns.to_vec();
    for (i, column) in columns.iter_mut().enumerate() {
        id = id.checked_add(1).ok_or(SqlError::Limit)?;
        column.id = id;
        if let Some(typmod) = typmods.get(i).filter(|m| **m >= 0) {
            std::sync::Arc::make_mut(&mut state.ext).columns.insert(
                id,
                catalog_ext::ColumnExt {
                    typmod: Some(*typmod),
                    ..Default::default()
                },
            );
        }
    }
    std::sync::Arc::make_mut(&mut state.tables).insert(
        name.into(),
        TableDef {
            id: table_id,
            name: name.into(),
            columns,
            primary_key: vec![],
        },
    );
    Ok(())
}
fn aliases(meta: &mut Meta, names: &[String]) -> R<()> {
    if names.len() > meta.columns.len() {
        return Err(SqlError::InvalidColumnReference);
    }
    for (column, name) in meta.columns.iter_mut().zip(names) {
        column.name = name.clone();
    }
    for c in &mut meta.columns {
        c.id = 0;
        c.not_null = false;
    }
    Ok(())
}
fn indices(names: &[String], columns: &[ColumnDef]) -> R<Vec<usize>> {
    names
        .iter()
        .map(|name| {
            let matches: Vec<_> = columns
                .iter()
                .enumerate()
                .filter(|(_, c)| c.name == *name)
                .map(|(i, _)| i)
                .collect();
            match matches.as_slice() {
                [i] => Ok(*i),
                [] => Err(SqlError::UnknownColumn),
                _ => Err(SqlError::AmbiguousColumn),
            }
        })
        .collect()
}
fn extra(meta: &mut Meta, name: &str, ty: DataType) -> R<()> {
    if meta.columns.iter().any(|c| c.name == name) {
        return Err(SqlError::DuplicateColumn);
    }
    meta.columns.push(ColumnDef {
        id: 0,
        name: name.into(),
        data_type: ty,
        not_null: false,
    });
    meta.typmods.push(-1);
    meta.unknown.push(false);
    Ok(())
}
fn generated(cte: &Cte, meta: &mut Meta, parameters: &mut [Option<DataType>]) -> R<()> {
    if let Some(search) = &cte.search {
        indices(&search.by, &meta.columns)?;
        extra(meta, &search.column, DataType::Text)?;
    }
    if let Some(cycle) = &cte.cycle {
        indices(&cycle.by, &meta.columns)?;
        let types = Plan::new(Body::Values(vec![
            vec![cycle.mark.clone()],
            vec![cycle.default.clone()],
        ]))
        .resolve(&Relational::default(), parameters)?;
        extra(meta, &cycle.column, types.columns[0].data_type.clone())?;
        extra(meta, &cycle.path, DataType::Text)?;
    }
    for (name, oid) in cte
        .search
        .iter()
        .map(|s| (s.column.as_str(), if s.depth { 2287 } else { 2249 }))
        .chain(cte.cycle.iter().map(|c| (c.path.as_str(), 2287)))
    {
        let i = meta.columns.iter().position(|c| c.name == name).unwrap();
        extra(meta, &order_name(i, oid), DataType::BigInt)?;
    }
    Ok(())
}
pub(super) fn resolve(
    ctes: &mut [Cte],
    state: &Relational,
    parameters: &mut [Option<DataType>],
) -> R<Relational> {
    let mut state = catalog(state);
    for cte in ctes {
        let mut meta = if let Some(dml) = &mut cte.dml {
            let mut meta = dml::resolve(dml, &state, parameters, &cte.dml_parameters)?;
            aliases(&mut meta, &cte.aliases)?;
            meta
        } else if cte.recursive {
            recursive_parts(cte)?;
            let Body::Set { left, right, .. } = &mut cte.query.body else {
                unreachable!()
            };
            let mut meta = left.resolve(&state, parameters)?;
            if meta.columns.iter().any(|c| c.data_type == DataType::Bpchar) {
                return Err(SqlError::Unsupported);
            }
            aliases(&mut meta, &cte.aliases)?;
            install_collated(
                &mut state,
                &cte.name,
                &meta.columns,
                &meta.typmods,
                &meta.collations,
            )?;
            let step = right.resolve(&state, parameters)?;
            if meta.columns.len() != step.columns.len() {
                return Err(SqlError::Syntax);
            }
            for (i, column) in meta.columns.iter().enumerate() {
                if column.data_type != step.columns[i].data_type
                    || meta.typmods[i] != step.typmods[i]
                {
                    return Err(SqlError::Type);
                }
                let original = meta.collations.get(i).and_then(Option::as_ref);
                let combined = functions::collation::combined(
                    original,
                    step.collations.get(i).and_then(Option::as_ref),
                    column.data_type.clone(),
                )?;
                combined.require_collation()?;
                let seed = original.map(BoundScalar::explicit_collation).transpose()?.flatten().unwrap_or("default");
                let overall = combined.explicit_collation()?.unwrap_or("default");
                if seed != overall {
                    user_types::view_replace_error(format!(
                        "recursive query \"{}\" column {} has collation {} in non-recursive term but collation {} overall",
                        cte.name, i + 1, functions::collation::display_name(seed), functions::collation::display_name(overall)
                    ));
                    return Err(SqlError::CollationMismatch);
                }
            }
            meta
        } else {
            let mut meta = cte.query.resolve(&state, parameters)?;
            aliases(&mut meta, &cte.aliases)?;
            meta
        };
        generated(cte, &mut meta, parameters)?;
        install_collated(
            &mut state,
            &cte.name,
            &meta.columns,
            &meta.typmods,
            &meta.collations,
        )?;
    }
    Ok(state)
}

pub(super) fn validate_constants(
    cte: &Cte,
    state: &Relational,
    parameters: &[Option<DataType>],
) -> R<()> {
    if let Some(dml) = &cte.dml {
        let mut dml = (**dml).clone();
        dml.epoch = state.epoch;
        dml.params = parameters.len();
        dml.parameter_types = parameters.to_vec();
        return dml.validate_immutable_constants(state);
    }
    if cte.recursive {
        let (seed, step, _) = recursive_parts(cte)?;
        let description = seed.describe(state, parameters.len(), parameters)?;
        let mut meta = Meta {
            unknown: vec![false; description.columns.len()],
            columns: description.columns,
            typmods: description.typmods,
            type_oids: description.type_oids,
            collations: vec![],
        };
        aliases(&mut meta, &cte.aliases)?;
        let mut state = catalog(state);
        install_collated(
            &mut state,
            &cte.name,
            &meta.columns,
            &meta.typmods,
            &meta.collations,
        )?;
        seed.validate_constants(&state, parameters)?;
        step.validate_constants(&state, parameters)
    } else {
        cte.query.validate_constants(state, parameters)
    }
}

/// Virtual scans are intercepted by name; every other access goes to the real source.
pub(super) struct VirtualRows<'a, 'e> {
    pub base: &'a dyn scan::RowSource,
    pub relations: Vec<(String, &'a Relation<'e>)>,
}
impl scan::RowSource for VirtualRows<'_, '_> {
    fn lock_row(&self, table: &TableDef, values: &[Value], mode: scan::RowLock, wait: scan::RowWait)
        -> R<Option<SqlRow>> { self.base.lock_row(table, values, mode, wait) }

    fn streams_reductions(&self) -> bool {
        self.base.streams_reductions()
    }
    fn scan<'s>(
        &'s self,
        table: &TableDef,
        range: &scan::KeyRange,
        dir: scan::Direction,
    ) -> R<scan::SourceRows<'s>> {
        if let Some((_, rows)) = self
            .relations
            .iter()
            .rev()
            .find(|(name, _)| *name == table.name)
        {
            return rows.scan(table, range, dir);
        }
        self.base.scan(table, range, dir)
    }
    fn get<'s>(&'s self, table: &TableDef, key: &[u8]) -> R<Option<scan::SourceRow<'s>>> {
        if self.relations.iter().any(|(name, _)| *name == table.name) {
            return Ok(None);
        }
        self.base.get(table, key)
    }
    fn join_probe<'s>(
        &'s self,
        table: &TableDef,
        keys: &[(usize, Option<Value>)],
        first: bool,
    ) -> R<Option<scan::SourceRows<'s>>> {
        if self.relations.iter().any(|(name, _)| *name == table.name) {
            return Ok(None);
        }
        self.base.join_probe(table, keys, first)
    }
    fn index_input<'s>(
        &'s self,
        table: &TableDef,
        predicates: &[(usize, &str, Value)],
    ) -> R<Option<scan::SourceRows<'s>>> {
        if self.relations.iter().any(|(name, _)| *name == table.name) {
            return Ok(None);
        }
        self.base.index_input(table, predicates)
    }
    fn index_scan<'s>(
        &'s self,
        table: &TableDef,
        predicates: &[(usize, &str, Value)],
        after: Option<&[u8]>,
    ) -> R<Option<scan::SourceRows<'s>>> {
        if self.relations.iter().any(|(name, _)| *name == table.name) {
            return Ok(None);
        }
        self.base.index_scan(table, predicates, after)
    }
}
pub(super) fn run<'e>(
    plan: &Plan,
    state: &Relational,
    source: &dyn scan::RowSource,
    params: &[Value],
    parameters: &[Option<DataType>],
    clock: Option<&super::super::super::clock::ExecutionClock>,
    execution: &'e Execution<'_>,
) -> R<Relation<'e>> {
    run_inner(
        plan, state, source, params, parameters, clock, execution, None,
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "shared statement snapshot and mutation context"
)]
pub(super) fn run_inner<'e>(
    plan: &Plan,
    state: &Relational,
    source: &dyn scan::RowSource,
    params: &[Value],
    parameters: &[Option<DataType>],
    clock: Option<&super::super::super::clock::ExecutionClock>,
    execution: &'e Execution<'_>,
    mut writes: Option<(
        &mut Relational,
        &mut Vec<crate::relational::constraints::Event>,
    )>,
) -> R<Relation<'e>> {
    let _metadata = functions::collation::ColumnScope::enter(&[], &[])?;
    let mut catalog = catalog(state);
    let mut materialized: Vec<(String, Relation<'e>)> = vec![];
    for cte in &plan.ctes {
        let wrapper = VirtualRows {
            base: source,
            relations: materialized.iter().map(|(n, r)| (n.clone(), r)).collect(),
        };
        let mut rows = if let Some(dml) = &cte.dml {
            let (target, events) = writes.as_mut().ok_or(SqlError::Unsupported)?;
            dml::execute(
                dml, state, &catalog, &wrapper, target, events, params, parameters, clock,
                execution,
            )?
        } else if cte.recursive {
            recurse(
                cte, &catalog, &wrapper, params, parameters, clock, execution,
            )?
        } else {
            cte.query
                .run(&catalog, &wrapper, params, parameters, clock, execution)?
        };
        if !cte.recursive {
            for (c, n) in rows.columns.iter_mut().zip(&cte.aliases) {
                c.name = n.clone();
            }
        }
        let metadata = if let Some(dml)=&cte.dml {
            dml::resolve(&mut dml.clone(), &catalog, &mut parameters.to_vec(), &cte.dml_parameters)?
        } else if cte.recursive {
            recursive_parts(cte)?.0.clone().resolve(&catalog,&mut parameters.to_vec())?
        } else { cte.query.clone().resolve(&catalog, &mut parameters.to_vec())? };
        install_collated(
            &mut catalog,
            &cte.name,
            &rows.columns,
            &rows.typmods,
            &metadata.collations,
        )?;
        materialized.push((cte.name.clone(), rows));
    }
    let wrapper = VirtualRows {
        base: source,
        relations: materialized.iter().map(|(n, r)| (n.clone(), r)).collect(),
    };
    let mut body = plan.clone();
    body.ctes.clear();
    if body.mutation.is_some() || body.merge.is_some() {
        let (target,events)=writes.as_mut().ok_or(SqlError::Unsupported)?;
        let prepared=Prepared::engine(Statement::Plan(body),0,state.epoch);
        return dml::execute(&prepared,state,&catalog,&wrapper,target,events,params,parameters,clock,execution);
    }
    body.run(&catalog, &wrapper, params, parameters, clock, execution)
}
type PathKey = Vec<Vec<Value>>;
type OrderKeys = (Option<PathKey>, Option<PathKey>);
#[derive(Clone)]
struct Ancestor {
    search: PathKey,
    cycle: PathKey,
}
fn key(values: &[Value], indices: &[usize]) -> Vec<Value> {
    indices.iter().map(|i| values[*i].clone()).collect()
}
fn record(values: &[Value]) -> R<String> {
    values
        .iter()
        .map(|v| {
            if *v == Value::Null {
                return Ok(String::new());
            }
            let text = match functions::coerce_clocked(v.clone(), &DataType::Text, None)? {
                Value::Text(s) => s,
                _ => return Err(SqlError::Type),
            };
            Ok(
                if text.is_empty()
                    || text
                        .chars()
                        .any(|c| matches!(c, '(' | ')' | ',' | '"' | '\\') || c.is_whitespace())
                {
                    format!("\"{}\"", text.replace('\\', "\\\\").replace('"', "\"\""))
                } else {
                    text
                },
            )
        })
        .collect::<R<Vec<_>>>()
        .map(|parts| format!("({})", parts.join(",")))
}
fn path_text(path: &[Vec<Value>]) -> R<Value> {
    let parts = path
        .iter()
        .map(|row| {
            record(row).map(|s| format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"")))
        })
        .collect::<R<Vec<_>>>()?;
    Ok(Value::Text(format!("{{{}}}", parts.join(","))))
}
fn recurse<'e>(
    cte: &Cte,
    state: &Relational,
    source: &dyn scan::RowSource,
    params: &[Value],
    parameters: &[Option<DataType>],
    clock: Option<&super::super::super::clock::ExecutionClock>,
    execution: &'e Execution<'_>,
) -> R<Relation<'e>> {
    let (seed, step, all) = recursive_parts(cte)?;
    let seed_metadata = seed.clone().resolve(state,&mut parameters.to_vec())?;
    let seed = seed.run(state, source, params, parameters, clock, execution)?;
    let mut meta = Meta {
        columns: seed.columns.clone(),
        typmods: seed.typmods.clone(),
        type_oids: vec![],
        unknown: vec![false; seed.columns.len()],
        collations: seed_metadata.collations,
    };
    aliases(&mut meta, &cte.aliases)?;
    let base_columns = meta.columns.clone();
    let base_typmods = meta.typmods.clone();
    if cte.search.is_none() && cte.cycle.is_none() {
        return recurse_plain(
            cte,
            step,
            all,
            seed,
            base_columns,
            base_typmods,
            &meta.collations,
            state,
            source,
            params,
            parameters,
            clock,
            execution,
        );
    }
    let search_by = cte
        .search
        .as_ref()
        .map(|s| indices(&s.by, &meta.columns))
        .transpose()?
        .unwrap_or_default();
    let cycle_by = cte
        .cycle
        .as_ref()
        .map(|c| indices(&c.by, &meta.columns))
        .transpose()?
        .unwrap_or_default();
    generated(cte, &mut meta, &mut parameters.to_vec())?;
    meta.columns
        .retain(|c| !c.name.starts_with("\0plan_order_"));
    meta.typmods.truncate(meta.columns.len());
    let cycle_values = cte
        .cycle
        .as_ref()
        .map(|c| -> R<(Value, Value)> {
            let ty =
                &meta.columns[base_columns.len() + usize::from(cte.search.is_some())].data_type;
            let eval = |s: &Scalar| {
                let mut bound = Scalar::Cast(Box::new(s.clone()), ty.clone()).bind(
                    &|_, _| Err(SqlError::UnknownColumn),
                    &[],
                    parameters,
                )?;
                bound.attach_clock(clock);
                bound.eval(
                    &Tuple {
                        source_versions: vec![],
                        values: vec![],
                        version: 0,
                    },
                    params,
                    &mut 0,
                )
            };
            Ok((eval(&c.mark)?, eval(&c.default)?))
        })
        .transpose()?;
    let mut output = Relation::new(meta.columns, meta.typmods, execution);
    let seen_scope = RetentionScope::new(execution);
    let mut ancestor_scope = RetentionScope::new(execution);
    let mut order_keys: Vec<OrderKeys> = vec![];
    let mut frontier = seed;
    ancestor_scope
        .borrow()
        .charge(frontier.rows.len() * std::mem::size_of::<Ancestor>())?;
    let mut ancestors = vec![
        Ancestor {
            search: vec![],
            cycle: vec![]
        };
        frontier.rows.len()
    ];
    let mut seen: Vec<Vec<Value>> = vec![];
    let mut state = catalog(state);
    install_collated(&mut state, &cte.name, &base_columns, &base_typmods, &meta.collations)?;
    while !frontier.rows.is_empty() {
        let next_scope = RetentionScope::new(execution);
        let mut next = Relation::new(base_columns.clone(), base_typmods.clone(), execution);
        let mut next_ancestors = vec![];
        for (row, parent) in frontier.rows.iter().zip(&ancestors) {
            execution.comparison()?;
            let mut ancestor = parent.clone();
            let mut values = row.values.clone();
            if let Some(search) = &cte.search {
                ancestor.search.push(key(&values, &search_by));
                values.push(if search.depth {
                    path_text(&ancestor.search)?
                } else {
                    let mut breadth = vec![Value::Int(ancestor.search.len() as i64 - 1)];
                    breadth.extend(key(&values, &search_by));
                    Value::Text(record(&breadth)?)
                });
            }
            let mut cycle = false;
            if let Some((mark, default)) = &cycle_values {
                let current = key(&row.values, &cycle_by);
                for previous in &ancestor.cycle {
                    execution.comparison()?;
                    cycle |= previous.iter().zip(&current).enumerate().all(|(i,(a,b))| meta.collations.get(cycle_by[i]).and_then(Option::as_ref).map_or_else(||cmp(a,b),|m|m.compare_values(a,b)) == Ordering::Equal);
                }
                ancestor.cycle.push(current);
                values.push(if cycle { mark.clone() } else { default.clone() });
                values.push(path_text(&ancestor.cycle)?);
            }
            if !all {
                let mut duplicate = false;
                for previous in &seen {
                    execution.comparison()?;
                    if super::super::grouping::Key::metadata(previous.clone(),&meta.collations) == super::super::grouping::Key::metadata(values.clone(),&meta.collations) {
                        duplicate = true;
                        break;
                    }
                }
                if duplicate {
                    continue;
                }
                // The output owns the retained values; this additional key is also charged.
                seen_scope.borrow().charge(
                    32 + values.len() * std::mem::size_of::<Value>()
                        + values.iter().map(value_bytes).sum::<usize>(),
                )?;
                seen.push(values.clone());
            }
            let search_key = cte.search.as_ref().map(|s| {
                if s.depth {
                    ancestor.search.clone()
                } else {
                    let mut breadth = vec![Value::Int(ancestor.search.len() as i64 - 1)];
                    breadth.extend(key(&row.values, &search_by));
                    vec![breadth]
                }
            });
            let cycle_key = cte.cycle.as_ref().map(|_| ancestor.cycle.clone());
            for path in search_key.iter().chain(cycle_key.iter()) {
                charge_path(&seen_scope, path)?;
            }
            seen_scope.borrow().charge(std::mem::size_of::<(
                Option<Vec<Vec<Value>>>,
                Option<Vec<Vec<Value>>>,
            )>())?;
            order_keys.push((search_key, cycle_key));
            output.push(SqlRow { values, version: 0 })?;
            if cycle {
                continue;
            }
            let mut working = Relation::new(base_columns.clone(), base_typmods.clone(), execution);
            working.push(row.clone())?;
            let wrapper = VirtualRows {
                base: source,
                relations: vec![(cte.name.clone(), &working)],
            };
            let children = step.run(&state, &wrapper, params, parameters, clock, execution)?;
            for child in children.rows {
                next.push(child)?;
                charge_path(&next_scope, &ancestor.search)?;
                charge_path(&next_scope, &ancestor.cycle)?;
                next_scope
                    .borrow()
                    .charge(std::mem::size_of::<Ancestor>())?;
                next_ancestors.push(ancestor.clone());
            }
        }
        frontier = next;
        ancestors = next_ancestors;
        ancestor_scope = next_scope;
    }
    drop(ancestor_scope);
    for (which, column, oid) in cte
        .search
        .iter()
        .map(|s| (0, s.column.as_str(), if s.depth { 2287 } else { 2249 }))
        .chain(cte.cycle.iter().map(|c| (1, c.path.as_str(), 2287)))
    {
        let i = output
            .columns
            .iter()
            .position(|c| c.name == column)
            .unwrap();
        seen_scope.borrow().charge(
            output.rows.len() * (std::mem::size_of::<usize>() + std::mem::size_of::<i64>()),
        )?;
        // Breadth-first records start with a numeric depth, followed by the
        // search fields. Depth-first and cycle paths contain only key fields.
        let mut collations = vec![];
        if which == 0 && cte.search.as_ref().is_some_and(|s| !s.depth) {
            collations.push(None);
        }
        let by = if which == 0 { &search_by } else { &cycle_by };
        collations.extend(by.iter().map(|i| meta.collations.get(*i).and_then(Option::as_ref)));
        let mut sorted: Vec<usize> = (0..output.rows.len()).collect();
        setops::sort(&mut sorted, execution, |a, b| {
            let path = |i: usize| {
                if which == 0 {
                    order_keys[i].0.as_ref().unwrap()
                } else {
                    order_keys[i].1.as_ref().unwrap()
                }
            };
            path_cmp(path(*a), path(*b), &collations)
        })?;
        let mut ranks = vec![0i64; sorted.len()];
        let mut rank = 0;
        for (offset, index) in sorted.iter().enumerate() {
            if offset > 0 {
                let a = if which == 0 {
                    order_keys[*index].0.as_ref().unwrap()
                } else {
                    order_keys[*index].1.as_ref().unwrap()
                };
                let b = if which == 0 {
                    order_keys[sorted[offset - 1]].0.as_ref().unwrap()
                } else {
                    order_keys[sorted[offset - 1]].1.as_ref().unwrap()
                };
                if path_cmp(a, b, &collations) != Ordering::Equal {
                    rank += 1;
                }
            }
            ranks[*index] = rank;
        }
        output.columns.push(ColumnDef {
            id: 0,
            name: order_name(i, oid),
            data_type: DataType::BigInt,
            not_null: false,
        });
        output.typmods.push(-1);
        output
            .scope
            .borrow()
            .charge(output.rows.len() * std::mem::size_of::<Value>())?;
        for (row, rank) in output.rows.iter_mut().zip(ranks) {
            row.values.push(Value::Int(rank));
        }
    }
    Ok(output)
}

/// Ordinary recursion evaluates the recursive term against the entire current
/// working table, with UNION deduplication before that table becomes visible.
#[expect(clippy::too_many_arguments, reason = "shared query execution context")]
fn recurse_plain<'e>(
    cte: &Cte,
    step: &Plan,
    all: bool,
    mut frontier: Relation<'e>,
    columns: Vec<ColumnDef>,
    typmods: Vec<i32>,
    collations: &[Option<BoundScalar>],
    state: &Relational,
    source: &dyn scan::RowSource,
    params: &[Value],
    parameters: &[Option<DataType>],
    clock: Option<&super::super::super::clock::ExecutionClock>,
    execution: &'e Execution<'_>,
) -> R<Relation<'e>> {
    let mut state = catalog(state);
    install_collated(&mut state, &cte.name, &columns, &typmods,collations)?;
    let mut output = Relation::new(columns.clone(), typmods.clone(), execution);
    loop {
        let mut working = Relation::new(columns.clone(), typmods.clone(), execution);
        for row in frontier.rows {
            execution.comparison()?;
            if !all {
                let mut duplicate = false;
                for previous in &output.rows {
                    execution.comparison()?;
                    if super::super::grouping::Key::metadata(previous.values.clone(),collations) == super::super::grouping::Key::metadata(row.values.clone(),collations) {
                        duplicate = true;
                        break;
                    }
                }
                if duplicate {
                    continue;
                }
            }
            working.push(row.clone())?;
            output.push(row)?;
        }
        if working.rows.is_empty() {
            break;
        }
        let wrapper = VirtualRows {
            base: source,
            relations: vec![(cte.name.clone(), &working)],
        };
        frontier = step.run(&state, &wrapper, params, parameters, clock, execution)?;
    }
    Ok(output)
}

fn charge_path(scope: &RetentionScope<'_>, path: &[Vec<Value>]) -> R<()> {
    scope.borrow().charge(
        std::mem::size_of_val(path)
            + path
                .iter()
                .map(|r| {
                    r.len() * std::mem::size_of::<Value>()
                        + r.iter().map(value_bytes).sum::<usize>()
                })
                .sum::<usize>(),
    )
}
fn path_cmp(a: &[Vec<Value>], b: &[Vec<Value>], collations: &[Option<&BoundScalar>]) -> Ordering {
    for (a, b) in a.iter().zip(b) {
        for (i, (a, b)) in a.iter().zip(b).enumerate() {
            // PostgreSQL record comparison sorts NULL fields after non-NULLs.
            let order = match (a, b) {
                (Value::Null, Value::Null) => Ordering::Equal,
                (Value::Null, _) => Ordering::Greater,
                (_, Value::Null) => Ordering::Less,
                _ => collations.get(i).copied().flatten().map_or_else(|| cmp(a, b), |m| m.compare_values(a, b)),
            };
            if order != Ordering::Equal {
                return order;
            }
        }
    }
    a.len().cmp(&b.len())
}
fn order_name(index: usize, oid: u32) -> String {
    format!("\0plan_order_{index}_{oid}")
}
fn order_info(name: &str) -> Option<(usize, u32)> {
    let (i, oid) = name.strip_prefix("\0plan_order_")?.split_once('_')?;
    Some((i.parse().ok()?, oid.parse().ok()?))
}
/// Scalar evaluates the public record's text representation. Ordering uses a
/// private bigint rank computed from the typed record fields, never text order.
/// The private columns do not participate in star expansion.
pub(super) fn prepare_query(query: &Query, state: &Relational) -> R<Query> {
    let mut query = query.clone();
    if !query.sources.iter().any(|s| {
        state
            .tables
            .get(&s.name)
            .is_some_and(|t| t.columns.iter().any(|c| order_info(&c.name).is_some()))
    }) {
        return Ok(query);
    }
    let mut items = vec![];
    for (item, alias) in &query.items {
        if matches!(item, Item::Star | Item::QualifiedStar { .. }) {
            for source in query.star_sources(item)? {
                let table = views::table(state, &query.sources[source].name)?;
                for column in table
                    .logical_columns()
                    .filter(|c| order_info(&c.name).is_none())
                {
                    items.push((
                        Item::Scalar(Scalar::Column(ColumnRef {
                            qualifier: Some(query.sources[source].alias.clone()),
                            name: column.name.clone(),
                        })),
                        None,
                    ));
                }
            }
        } else {
            items.push((item.clone(), alias.clone()));
        }
    }
    query.items = items;
    for (scalar, _, _) in &mut query.order {
        let projected = match scalar {
            Scalar::Literal(Value::Int(n)) => usize::try_from(*n)
                .ok()
                .and_then(|n| n.checked_sub(1))
                .and_then(|i| query.items.get(i))
                .map(|(item, _)| item),
            Scalar::Column(c) if c.qualifier.is_none() => query
                .items
                .iter()
                .find(|(_, alias)| alias.as_ref() == Some(&c.name))
                .map(|(item, _)| item),
            _ => None,
        };
        let column = match projected {
            Some(Item::Scalar(Scalar::Column(c))) => Some(c.clone()),
            _ => {
                if let Scalar::Column(c) = scalar {
                    Some(c.clone())
                } else {
                    None
                }
            }
        };
        if let Some(c) = column {
            let mut matches = vec![];
            for source in &query.sources {
                if c.qualifier.as_ref().is_some_and(|a| *a != source.alias) {
                    continue;
                }
                let table = views::table(state, &source.name)?;
                for (i, _) in table
                    .columns
                    .iter()
                    .enumerate()
                    .filter(|(_, col)| col.name == c.name)
                {
                    if let Some(hidden) = table
                        .columns
                        .iter()
                        .find(|hidden| order_info(&hidden.name).is_some_and(|(n, _)| n == i))
                    {
                        matches.push(ColumnRef {
                            qualifier: Some(source.alias.clone()),
                            name: hidden.name.clone(),
                        });
                    }
                }
            }
            if let [column] = matches.as_slice() {
                *scalar = Scalar::Column(column.clone());
            }
        }
    }
    Ok(query)
}
pub(super) fn projection_oids(bound: &BoundQuery<'_>) -> Vec<u32> {
    bound
        .items
        .iter()
        .map(|item| {
            let BoundItem::Column(c) = item else {
                return 0;
            };
            bound.tables[c.source]
                .columns
                .iter()
                .find_map(|col| {
                    order_info(&col.name)
                        .filter(|(i, _)| *i == c.column)
                        .map(|(_, oid)| oid)
                })
                .unwrap_or(0)
        })
        .collect()
}

pub(super) fn install_collated(
    state: &mut Relational,
    name: &str,
    columns: &[ColumnDef],
    typmods: &[i32],
    metadata: &[Option<BoundScalar>],
) -> R<()> {
    install(state, name, columns, typmods)?;
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1 << 62);
    let table = std::sync::Arc::make_mut(&mut state.tables).get_mut(name).unwrap();
    for c in &mut table.columns {
        let old = c.id;
        c.id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if let Some(ext) = std::sync::Arc::make_mut(&mut state.ext).columns.remove(&old) {
            std::sync::Arc::make_mut(&mut state.ext).columns.insert(c.id, ext);
        }
    }
    functions::collation::register(&table.columns, metadata)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn run(state: &mut Relational, sql: &str) -> SqlResult {
        let p = Prepared::parse(sql, 1, state.epoch).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        state
            .execute(&p, &[], None)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
    }
    fn rows(sql: &str) -> Vec<Vec<Value>> {
        run(&mut Relational::default(), sql)
            .rows
            .into_iter()
            .map(|r| r.values)
            .collect()
    }
    #[test]
    fn recursive_series_and_union_termination() {
        use Value::Int as I;
        assert_eq!(
            rows(
                "WITH RECURSIVE x(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM x WHERE n<5) SELECT n FROM x ORDER BY n"
            ),
            (1..=5).map(|n| vec![I(n)]).collect::<Vec<_>>()
        );
        assert_eq!(
            rows("WITH RECURSIVE x(n) AS (VALUES(1),(1) UNION SELECT n FROM x) SELECT n FROM x"),
            vec![vec![I(1)]]
        );
        assert_eq!(
            rows(
                "WITH RECURSIVE x(n) AS (VALUES(1),(1) UNION ALL SELECT n+1 FROM x WHERE n<2) SELECT n FROM x ORDER BY n"
            ),
            vec![vec![I(1)], vec![I(1)], vec![I(2)], vec![I(2)]]
        );
    }
    #[test]
    fn cte_shadowing_chaining_nested_scope_and_dependencies() {
        let mut state = Relational::default();
        run(&mut state, "CREATE TABLE cte_base(n int PRIMARY KEY)");
        run(&mut state, "INSERT INTO cte_base VALUES(99)");
        let sql = "WITH cte_base(n) AS (VALUES(1)), b AS (SELECT n+1 AS n FROM cte_base) SELECT * FROM b UNION ALL SELECT * FROM (WITH b(n) AS (VALUES(3)) SELECT * FROM b) nested ORDER BY n";
        assert_eq!(
            run(&mut state, sql)
                .rows
                .into_iter()
                .map(|r| r.values)
                .collect::<Vec<_>>(),
            vec![vec![Value::Int(2)], vec![Value::Int(3)]]
        );
        let p = Prepared::parse(sql, 1, state.epoch).unwrap();
        assert!(p.read_tables().is_empty());
        assert!(p.authorization_tables().unwrap().is_empty());
        let p = Prepared::parse(
            "WITH x AS (SELECT * FROM cte_base) SELECT * FROM x",
            1,
            state.epoch,
        )
        .unwrap();
        assert_eq!(p.read_tables(), vec!["cte_base"]);
        assert_eq!(p.authorization_tables().unwrap(), p.read_tables());
    }
    #[test]
    fn cte_reused_once_and_joined_to_base_table() {
        let mut state = Relational::default();
        run(&mut state, "CREATE TABLE cte_paid(n int PRIMARY KEY)");
        run(&mut state, "INSERT INTO cte_paid VALUES(1)");
        struct CountScans<'a> {
            base: scan::LegacyRows<'a>,
            count: Cell<usize>,
        }
        impl scan::RowSource for CountScans<'_> {
            fn scan<'s>(
                &'s self,
                table: &TableDef,
                range: &scan::KeyRange,
                dir: scan::Direction,
            ) -> R<scan::SourceRows<'s>> {
                self.count.set(self.count.get() + 1);
                self.base.scan(table, range, dir)
            }
            fn get<'s>(&'s self, table: &TableDef, key: &[u8]) -> R<Option<scan::SourceRow<'s>>> {
                self.base.get(table, key)
            }
        }
        let mut parser = parse::parser(
            &lex("WITH x AS (SELECT n FROM cte_paid) SELECT a.n,b.n FROM x a JOIN x b ON a.n=b.n")
                .unwrap(),
        );
        let plan = Plan::parse(&mut parser).unwrap();
        let source = CountScans {
            base: scan::LegacyRows(&state),
            count: Cell::new(0),
        };
        let result = plan
            .execute(&state, &source, 0, &[], &[], None, &mut || false)
            .unwrap();
        assert_eq!(result.rows[0].values, vec![Value::Int(1), Value::Int(1)]);
        assert_eq!(source.count.get(), 1);
        run(
            &mut state,
            "CREATE TABLE cte_nodes(id int PRIMARY KEY,parent int)",
        );
        run(
            &mut state,
            "INSERT INTO cte_nodes VALUES(1,NULL),(2,1),(3,1),(4,2)",
        );
        let sql = "WITH RECURSIVE x(id,depth) AS (SELECT id,0 FROM cte_nodes WHERE parent IS NULL UNION ALL SELECT t.id,x.depth+1 FROM cte_nodes t JOIN x ON t.parent=x.id) SELECT * FROM x ORDER BY depth,id";
        assert_eq!(
            run(&mut state, sql)
                .rows
                .into_iter()
                .map(|r| r.values)
                .collect::<Vec<_>>(),
            [(1, 0), (2, 1), (3, 1), (4, 2)]
                .into_iter()
                .map(|(a, b)| vec![Value::Int(a), Value::Int(b)])
                .collect::<Vec<_>>()
        );
    }
    #[test]
    fn search_uses_typed_record_order_and_hides_internal_columns() {
        let mut state = Relational::default();
        run(
            &mut state,
            "CREATE TABLE search_nodes(id int PRIMARY KEY,parent int)",
        );
        run(
            &mut state,
            "INSERT INTO search_nodes VALUES(1,NULL),(10,1),(2,1),(3,2)",
        );
        for (kind, expected) in [("DEPTH", vec![1, 2, 3, 10]), ("BREADTH", vec![1, 2, 10, 3])] {
            let sql = format!(
                "WITH RECURSIVE t(id,parent) AS (SELECT id,parent FROM search_nodes WHERE parent IS NULL UNION ALL SELECT n.id,n.parent FROM search_nodes n JOIN t ON n.parent=t.id) SEARCH {kind} FIRST BY id SET visit SELECT id FROM t ORDER BY visit"
            );
            assert_eq!(
                run(&mut state, &sql)
                    .rows
                    .into_iter()
                    .map(|r| r.values)
                    .collect::<Vec<_>>(),
                expected
                    .into_iter()
                    .map(|n| vec![Value::Int(n)])
                    .collect::<Vec<_>>()
            );
        }
        assert_eq!(run(&mut state,"WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM t WHERE n<12) SEARCH DEPTH FIRST BY n SET visit SELECT n,visit AS k FROM t ORDER BY k DESC LIMIT 1").rows[0].values[0],Value::Int(12));
        let p = Prepared::parse("WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM t WHERE n<2) SEARCH BREADTH FIRST BY n SET visit SELECT visit FROM t ORDER BY 1",1,state.epoch).unwrap();
        assert_eq!(p.describe(&state).unwrap().type_oids, vec![2249]);
        let result = run(
            &mut state,
            "WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM t WHERE n<2) SEARCH DEPTH FIRST BY n SET visit SELECT * FROM t ORDER BY visit",
        );
        assert_eq!(result.columns, vec!["n", "visit"]);
        assert_eq!(
            result.rows[1].values,
            vec![Value::Int(2), Value::Text("{\"(1)\",\"(2)\"}".into())]
        );
    }
    #[test]
    fn cycle_marks_the_repeated_row_and_stops_that_path() {
        assert_eq!(
            rows(
                "WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT n FROM t) CYCLE n SET cyclic USING path SELECT n,cyclic FROM t ORDER BY path"
            ),
            vec![
                vec![Value::Int(1), Value::Bool(false)],
                vec![Value::Int(1), Value::Bool(true)]
            ]
        );
        assert_eq!(
            rows(
                "WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT n FROM t) CYCLE n SET cyclic TO 'yes' DEFAULT 'no' USING path SELECT cyclic FROM t ORDER BY path"
            ),
            vec![
                vec![Value::Text("no".into())],
                vec![Value::Text("yes".into())]
            ]
        );
    }
    #[test]
    fn invalid_recursive_references_and_type_mismatch() {
        for (sql, error) in [
            (
                "WITH RECURSIVE t(n) AS (SELECT n FROM t) SELECT * FROM t",
                SqlError::InvalidRecursion,
            ),
            (
                "WITH RECURSIVE t(n) AS (SELECT n FROM t UNION SELECT 1) SELECT * FROM t",
                SqlError::InvalidRecursion,
            ),
            (
                "WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT a.n FROM t a JOIN t b ON a.n=b.n) SELECT * FROM t",
                SqlError::InvalidRecursion,
            ),
            (
                "WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT n::bigint FROM t) SELECT * FROM t",
                SqlError::Type,
            ),
        ] {
            let result =
                Prepared::parse(sql, 1, 0).and_then(|p| p.describe(&Relational::default()));
            assert!(
                matches!(result,Err(ref e) if *e == error),
                "{sql}: {result:?}"
            );
        }
        assert_eq!(
            crate::sqlstate(&crate::CoreError::Sql(SqlError::InvalidRecursion)),
            "42P19"
        );
    }
    #[test]
    fn recursive_cancellation_and_retention() {
        let mut p = parse::parser(
            &lex("WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM t) SELECT * FROM t")
                .unwrap(),
        );
        let plan = Plan::parse(&mut p).unwrap();
        let state = Relational::default();
        let mut cancel = || true;
        assert_eq!(
            plan.execute(
                &state,
                &scan::LegacyRows(&state),
                0,
                &[],
                &[],
                None,
                &mut cancel
            ),
            Err(SqlError::Aborted)
        );
        let mut cancel = || false;
        let execution = Execution::new(&mut cancel, &[]);
        *execution.retention.borrow_mut() = scan::Retention::new(2048);
        assert!(matches!(
            plan.run(
                &state,
                &scan::LegacyRows(&state),
                &[],
                &[],
                None,
                &execution
            ),
            Err(SqlError::Limit)
        ));
        assert_eq!(execution.retention.borrow().used(), 0);
    }
}
