//! Joined mutations retain their statement snapshot and reuse the ordinary publisher.
use super::*;
use std::borrow::Cow;
type Publisher<'a> = dyn FnMut(&Prepared) -> R<()> + 'a;
type Point = (Vec<u8>, Vec<(String, Expr)>);

fn collect_point(table: &TableDef, point: &Prepared) -> R<Point> {
    let (predicates, sets) = match point.statement.get() {
        Statement::Update { predicates, sets, .. } => (predicates, sets.clone()),
        Statement::Delete { predicates, .. } => (predicates, vec![]),
        _ => return Err(SqlError::Unsupported),
    };
    Ok((bounds(table, predicates, &[])?.low, sets))
}

fn batch_assignments(points: Vec<Point>) -> R<Vec<(String, super::super::super::assign::Value)>> {
    let mut columns: BTreeMap<String, BTreeMap<Vec<u8>, Value>> = BTreeMap::new();
    for (key, sets) in points {
        for (name, value) in sets {
            columns.entry(name).or_default().insert(key.clone(), value.value(&[])?);
        }
    }
    Ok(columns.into_iter().map(|(name, values)| {
        (name, super::super::super::assign::Value::Rows(std::sync::Arc::new(values)))
    }).collect())
}
pub(super) const KEY: &str = "\0mutation_key";
#[derive(Debug, Clone)]
pub(in crate::relational) struct Mutation {
    pub target: String,
    pub(super) alias: String,
    pub(super) deleting: bool,
    pub(super) from: join::From,
    pub(super) predicate: Plan,
    pub(super) groups: Vec<(Vec<String>, Option<Plan>)>,
    pub(super) qualified_target: Option<(String, String)>,
    pub(super) returning: Option<Plan>,
    pub(super) images: [String; 2],
}
impl Plan {
    pub(in crate::relational) fn mutation_command(&self) -> Option<&'static str> {
        self.mutation.as_ref().map(|m| m.command())
            .or_else(|| self.merge.as_ref().map(|_| "MERGE"))
            .or_else(|| self.insert.as_ref().map(|_| "INSERT"))
    }
}
impl Plan {
    pub(in crate::relational) fn requires_planned_update(tokens: &[Token]) -> bool {
        if !tokens.first().is_some_and(|token| parse::word(token, "update")) {
            return false;
        }
        let Ok(top) = parse::top(tokens) else {
            return false;
        };
        let Some(set) = top.iter().copied().find(|i| parse::word(&tokens[*i], "set")) else {
            return false;
        };
        let end = top.iter().copied().find(|i| *i > set && parse::word(&tokens[*i], "returning"))
            .unwrap_or(tokens.len());
        has_scalar_subquery(&tokens[..end], set + 1)
            || qualified_assignment(&tokens[set + 1..end])
            || nested_target_source(&tokens[1..set], &tokens[set + 1..end])
    }
}

fn qualified_assignment(tokens: &[Token]) -> bool {
    assignment_target_starts(tokens).is_ok_and(|starts| {
        starts.into_iter().any(|i| tokens.get(i + 1) == Some(&Token::Symbol('.')))
    })
}

fn nested_target_source(target_tokens: &[Token], tokens: &[Token]) -> bool {
    let mut target = parse::parser(target_tokens);
    target.word("only");
    let Ok(name) = target.table_name() else { return false };
    target.sym('*');
    if target.at == target.tokens.len() {
        return false;
    }
    let table = split_key(&name).1;
    (0..tokens.len()).filter(|i| is_subquery_open(tokens, *i)).any(|i| {
        let Ok(close) = subquery_end(tokens, i) else { return false };
        let inner = &tokens[i + 1..close];
        parse::top(inner).is_ok_and(|positions| positions.into_iter().any(|i| {
            if !["from", "join"].iter().any(|word| parse::word(&inner[i], word)) {
                return false;
            }
            let mut from = parse::parser(&inner[i + 1..]);
            from.table_name().is_ok_and(|local| split_key(&local).1 == table)
        }))
    })
}

fn has_scalar_subquery(tokens: &[Token], start: usize) -> bool {
    (start..tokens.len()).any(|i| {
        is_subquery_open(tokens, i)
            && !["exists", "in", "any", "some", "all"]
                .iter().any(|word| parse::word(&tokens[i - 1], word))
    })
}

pub(super) fn claims(tokens: &[Token]) -> bool {
    let update = tokens.first().is_some_and(|t| parse::word(t, "update"));
    let delete = tokens.first().is_some_and(|t| parse::word(t, "delete"));
    if !update && !delete {
        return false;
    }
    let Ok(top) = parse::top(tokens) else {
        return false;
    };
    let end = top
        .iter()
        .copied()
        .find(|i| parse::word(&tokens[*i], "where") || parse::word(&tokens[*i], "returning"))
        .unwrap_or(tokens.len());
    if delete {
        if top.iter().any(|i| {
            *i >= 3
                && *i < end
                && parse::word(&tokens[*i], "using")
                && tokens.get(i - 1) != Some(&Token::Symbol('.'))
        }) {
            return true;
        }
        if end <= 2
            || !parse::word(&tokens[1], "from")
            || !tokens.get(end).is_some_and(|t| parse::word(t, "where"))
        {
            return false;
        }
        let returning = top
            .iter()
            .copied()
            .find(|i| *i > end && parse::word(&tokens[*i], "returning"))
            .unwrap_or(tokens.len());
        // As with UPDATE, bind nested references to an aliased target in their own scope.
        return nested_target_source(&tokens[2..end], &tokens[end + 1..returning]);
    }
    let Some(set) = top
        .iter()
        .copied()
        .find(|i| parse::word(&tokens[*i], "set"))
    else {
        return false;
    };
    let returning = top.iter().copied().find(|i| *i > set && parse::word(&tokens[*i], "returning"))
        .unwrap_or(tokens.len());
    Plan::requires_planned_update(&tokens[..returning]) || top.iter().any(|i| {
        *i > set
            && *i < end
            && parse::word(&tokens[*i], "from")
            && !parse::word(&tokens[*i - 1], "distinct")
    }) || top.windows(2).any(|pair| {
        pair[1] < end
            && pair[0] >= set
            && (parse::word(&tokens[pair[0]], "set") || tokens[pair[0]] == Token::Symbol(','))
            && tokens[pair[1]] == Token::Symbol('(')
    })
}
// The legacy alias normalizer may rewrite column references, never SET targets.
pub(in crate::relational) fn assignment_target_starts(tokens: &[Token]) -> R<Vec<usize>> {
    let mut targets = vec![0];
    for index in parse::top(tokens)? {
        if ["from", "where", "returning"].iter().any(|word| parse::word(&tokens[index], word))
            && (index == 0 || !parse::word(&tokens[index - 1], "distinct"))
        {
            break;
        }
        if tokens[index] == Token::Symbol(',') {
            targets.push(index + 1);
        }
    }
    Ok(targets)
}

pub(super) fn splits(tokens: &[Token]) -> R<Vec<&[Token]>> {
    let mut start = 0;
    let mut out = vec![];
    for i in parse::top(tokens)? {
        if tokens[i] == Token::Symbol(',') {
            out.push(&tokens[start..i]);
            start = i + 1;
        }
    }
    out.push(&tokens[start..]);
    Ok(out)
}
pub(super) fn projection(
    tokens: &[Token],
    owner: &mut Parser,
    depth: usize,
    next: &mut usize,
) -> parse::ParseResult<Plan> {
    let mut t = vec![Token::Word("select".into())];
    t.extend_from_slice(tokens);
    let mut plan = parse::child_expression(&t, None, owner, depth, next)?;
    if let Body::Select(select) = &mut plan.body {
        for (item, alias) in &mut select.query.items {
            if alias.is_none() && let Item::Scalar(Scalar::Column(c)) = item {
                *alias = Some(c.name.clone());
            }
        }
    }
    Ok(plan)
}
pub(super) fn parse(
    tokens: &[Token],
    owner: &mut Parser,
    depth: usize,
    next: &mut usize,
) -> parse::ParseResult<Plan> {
    if depth >= subplans::MAX_DEPTH {
        return Err(SqlError::Limit.into());
    }
    let mut p = parse::parser(tokens);
    let deleting = p.word("delete");
    if deleting {
        p.need("from")?;
    } else {
        p.need("update")?;
    }
    p.word("only");
    let target = p.table_name()?;
    p.sym('*');
    let alias = if p.word("as") || matches!(p.tokens.get(p.at), Some(Token::Word(w)) if !["set", "using", "where", "returning"].contains(&w.as_str()))
        || matches!(p.tokens.get(p.at), Some(Token::QuotedIdent(_)))
    {
        p.name()?
    } else {
        split_key(&target).1.into()
    };
    let positions = parse::top(tokens)?;
    let returning_at = positions
        .iter()
        .copied()
        .find(|i| *i >= p.at && parse::word(&tokens[*i], "returning"))
        .unwrap_or(tokens.len());
    let where_at = positions
        .iter()
        .copied()
        .find(|i| *i >= p.at && *i < returning_at && parse::word(&tokens[*i], "where"))
        .unwrap_or(returning_at);
    let from_at = positions
        .iter()
        .copied()
        .find(|i| {
            *i >= p.at
                && *i < where_at
                && parse::word(&tokens[*i], if deleting { "using" } else { "from" })
                && (deleting || !parse::word(&tokens[*i-1], "distinct"))
        })
        .unwrap_or(where_at);
    let mut groups = vec![];
    let mut qualified_target = None;
    if !deleting {
        p.need("set")?;
        for set in splits(&tokens[p.at..from_at])? {
            let mut a = parse::parser(set);
            let tuple = a.tokens.first() == Some(&Token::Symbol('('));
            let names = if tuple { a.names()? } else { vec![a.column_name()?] };
            if !tuple && a.sym('.') {
                let field = a.column_name()?;
                qualified_target.get_or_insert_with(|| (names[0].clone(), field));
                while a.sym('.') {
                    a.column_name()?;
                }
            }
            if a.tokens.get(a.at) != Some(&Token::Op("=".into())) {
                return Err(SqlError::Syntax.into());
            }
            a.at += 1;
            let rhs = &a.tokens[a.at..];
            if !tuple {
                let value = if rhs.len() == 1 && parse::word(&rhs[0], "default") {
                    None
                } else {
                    Some(projection(rhs, owner, depth, next)?)
                };
                groups.push((names, value));
            } else {
                let row = rhs.first().is_some_and(|t| parse::word(t, "row"));
                let rhs = if row { &rhs[1..] } else { rhs };
                if rhs.first() != Some(&Token::Symbol('('))
                    || rhs.last() != Some(&Token::Symbol(')'))
                {
                    return Err(ParseError::Definite(SqlError::Unsupported));
                }
                let rhs = &rhs[1..rhs.len() - 1];
                if rhs.first().is_some_and(|t| {
                    ["select", "with", "values", "table"]
                        .iter()
                        .any(|w| parse::word(t, w))
                }) {
                    groups.push((
                        names,
                        Some(parse::child_expression(rhs, None, owner, depth, next)?),
                    ));
                } else {
                    let fields = splits(rhs)?;
                    if !row && fields.len() == 1 {
                        return Err(ParseError::Definite(SqlError::Unsupported));
                    }
                    if fields.len() != names.len() {
                        return Err(ParseError::Definite(SqlError::Syntax));
                    }
                    for (name, value) in names.into_iter().zip(fields) {
                        groups.push((
                            vec![name],
                            if value.len() == 1 && parse::word(&value[0], "default") {
                                None
                            } else {
                                Some(projection(value, owner, depth, next)?)
                            },
                        ));
                    }
                }
            }
        }
    }
    let mut from_tokens = vec![
        Token::Word("select".into()),
        Token::Symbol('*'),
        Token::Word("from".into()),
    ];
    // Preserve schema-qualified spelling for the shared schema binder.
    let (schema, name) = split_key(&target);
    if schema != "public" {
        from_tokens.extend([Token::QuotedIdent(schema.into()), Token::Symbol('.')]);
    }
    from_tokens.extend([
        Token::QuotedIdent(name.into()),
        Token::Word("as".into()),
        Token::QuotedIdent(alias.clone()),
    ]);
    if from_at < where_at {
        from_tokens.push(Token::Symbol(','));
        from_tokens.extend_from_slice(&tokens[from_at + 1..where_at]);
    }
    let from_plan =
        join::parse_select(&from_tokens, None, owner, depth, next)?.ok_or(ParseError::Decline)?;
    let Body::Select(select) = from_plan.body else {
        return Err(ParseError::Decline);
    };
    let from = select.from.ok_or(ParseError::Decline)?;
    let mut filter = vec![Token::Word("true".into())];
    if where_at < returning_at {
        filter.extend_from_slice(&tokens[where_at..returning_at]);
    }
    let predicate = projection(&filter, owner, depth, next)?;
    let mut images = ["old".into(), "new".into()];
    let returning = if returning_at < tokens.len() {
        let mut r = parse::parser(&tokens[returning_at + 1..]);
        if r.word("with") {
            r.punct('(')?;
            loop {
                let image = if r.word("old") {
                    0
                } else {
                    r.need("new")?;
                    1
                };
                r.need("as")?;
                images[image] = r.name()?;
                if !r.sym(',') {
                    break;
                }
            }
            r.punct(')')?;
        }
        Some(projection(&r.tokens[r.at..], owner, depth, next)?)
    } else {
        None
    };
    owner.relations.extend(p.relations);
    let mut plan = Plan::new(Body::Values(vec![]));
    plan.mutation = Some(Box::new(Mutation {
        target,
        alias,
        deleting,
        from,
        predicate,
        groups,
        qualified_target,
        returning,
        images,
    }));
    Ok(plan)
}
fn missing_assignment_column(plan: &Plan) {
    let Body::Select(select) = &plan.body else { return };
    if !select.applies.is_empty() || select.from.is_some() || !select.query.sources.is_empty() {
        return;
    }
    let mut query = select.query.clone();
    let mut column = None;
    let _ = subquery::map_query(&mut query, &mut |scalar| {
        if column.is_none() && let Scalar::Column(c) = scalar && c.qualifier.is_none() {
            column = Some(c.name.clone());
        }
        Ok(())
    });
    if let Some(name) = column {
        user_types::view_replace_error(format!(r#"column "{name}" does not exist"#));
    }
}

impl Mutation {
    pub(super) fn has_returning(&self) -> bool {
        self.returning.is_some()
    }
    pub(super) fn remap(&mut self, alias: &str, name: &str) {
        fn sources(from: &mut join::From, alias: &str, name: &str) {
            if let join::From::Join { left, right, .. } = from {
                sources(left, alias, name);
                right.remap(alias, name);
            }
        }
        sources(&mut self.from, alias, name);
        cte::remap(&mut self.predicate, alias, name);
        for plan in self
            .groups
            .iter_mut()
            .filter_map(|(_, p)| p.as_mut())
            .chain(self.returning.as_mut())
        {
            cte::remap(plan, alias, name);
        }
    }
    pub(super) fn command(&self) -> &'static str {
        if self.deleting { "DELETE" } else { "UPDATE" }
    }
    pub(super) fn sequences(&self) -> Vec<SequenceDependency> {
        let mut out = vec![];
        self.from
            .visit_queries(&mut |q| out.extend(q.sequence_dependencies()));
        self.from
            .visit_scalars(&mut |s| s.sequence_dependencies(&mut out));
        for p in self
            .groups
            .iter()
            .filter_map(|(_, p)| p.as_ref())
            .chain(Some(&self.predicate))
            .chain(self.returning.as_ref())
        {
            out.extend(p.sequences());
        }
        out
    }
    pub(super) fn read_tables(&self) -> Vec<String> {
        let mut out = vec![self.target.clone()];
        self.from.visit_queries(&mut |q| {
            for name in q.read_tables() {
                if !name.starts_with("\0plan_") && !out.contains(&name) {
                    out.push(name);
                }
            }
        });
        for plan in self
            .groups
            .iter()
            .filter_map(|(_, p)| p.as_ref())
            .chain(Some(&self.predicate))
            .chain(self.returning.as_ref())
        {
            for name in plan.read_tables() {
                if !out.contains(&name) {
                    out.push(name);
                }
            }
        }
        out
    }
    pub(super) fn bind_schema_names(
        &mut self,
        bind: &impl Fn(&mut String),
        state: &Relational,
        path: &[String],
    ) -> R<()> {
        bind(&mut self.target);
        self.from.bind_schema_names(bind, state, path)?;
        self.predicate.bind_schema_names(bind, state, path)?;
        for p in self
            .groups
            .iter_mut()
            .filter_map(|(_, p)| p.as_mut())
            .chain(self.returning.as_mut())
        {
            p.bind_schema_names(bind, state, path)?;
        }
        Ok(())
    }
    pub(super) fn catalog(&self, state: &Relational) -> R<Relational> {
        let mut catalog = state.clone();
        let table = std::sync::Arc::make_mut(&mut catalog.tables).get_mut(&self.target)
            .ok_or(SqlError::UnknownTable)?;
        table.columns.push(ColumnDef {
            id: 0,
            name: KEY.into(),
            data_type: DataType::Bytes,
            not_null: false,
        });
        Ok(catalog)
    }
    pub(super) fn returning_shape(&self, shape: &join::Shape, table: &TableDef) -> join::Shape {
        let mut out = shape.clone();
        for alias in &self.images {
            let mut qualified = vec![];
            for column in table.logical_columns() {
                qualified.push((column.name.clone(), out.columns.len()));
                let mut column = column.clone();
                column.not_null = false;
                let metadata = shape.namespace.qualified.iter().find(|(a,_)| a==&self.alias).and_then(|(_,cols)| cols.iter().find(|(name,_)|name==&column.name)).and_then(|(_,i)|shape.collations.get(*i)).and_then(Option::as_ref);
                out.collations.push(metadata.map(|m| functions::collation::combined(Some(m),None,column.data_type.clone()).expect("resolved derivation")));
                out.columns.push(column);
                out.typmods.push(-1);
            }
            out.namespace.qualified.push((alias.clone(), qualified));
            out.namespace
                .visible
                .push((alias.clone(), out.columns.len()));
            out.columns.push(ColumnDef {
                id: 0,
                name: alias.clone(),
                data_type: DataType::Text,
                not_null: false,
            });
            out.typmods.push(-1);
            out.collations.push(None);
        }
        out
    }
    pub(super) fn resolve(
        &mut self,
        state: &Relational,
        parameters: &mut [Option<DataType>],
    ) -> R<bind::Meta> {
        let catalog = self.catalog(state)?;
        let shape = self.from.resolve(&catalog, parameters)?;
        self.resolve_shape(state, parameters, &shape)
    }
    pub(super) fn resolve_shape(&mut self, state: &Relational, parameters: &mut [Option<DataType>], shape: &join::Shape) -> R<bind::Meta> {
        let table = &state.tables[&self.target];
        if let Some((column, field)) = &self.qualified_target {
            if let Some(c) = table.columns.iter().find(|c| &c.name == column) {
                let ty = defaults::type_name(&c.data_type);
                user_types::view_replace_error(format!(
                    r#"cannot assign to field "{field}" of column "{column}" because its type {ty} is not a composite type"#
                ));
                return Err(SqlError::Type);
            }
            user_types::view_replace_error(format!(
                r#"column "{column}" of relation "{}" does not exist"#, split_key(&self.target).1
            ));
            user_types::sql_error_hint("SET target columns cannot be qualified with the relation name.");
            return Err(SqlError::UnknownColumn);
        }
        let names = self
            .groups
            .iter()
            .flat_map(|(names, _)| names)
            .collect::<Vec<_>>();
        if super::super::super::assign::duplicate_names(names.iter().copied()) {
            return Err(SqlError::Syntax);
        }
        for (names, plan) in &mut self.groups {
            for name in names.iter() {
                let i = super::super::super::col(table, name)?;
                if plan.is_some() {
                    defaults::check_explicit(state, &table.columns[i])?;
                }
            }
            if let Some(plan) = plan {
                if let Body::Select(s) = &plan.body {
                    for ((item, _), name) in s.query.items.iter().zip(names.iter()) {
                        if let Item::Scalar(Scalar::Param(n)) = item
                            && parameters[*n - 1].is_none() {
                                parameters[*n - 1] = Some(
                                    table.columns[super::super::super::col(table, name)?]
                                        .data_type
                                        .clone(),
                                );
                        }
                    }
                }
                let (mut instantiated, _) =
                    subquery::instantiate(plan, shape, None, state, parameters)?;
                let meta = instantiated.resolve(state, parameters).inspect_err(|error| {
                    if *error == SqlError::UnknownColumn {
                        missing_assignment_column(&instantiated);
                    }
                })?;
                if meta.columns.len() != names.len() {
                    return Err(SqlError::Syntax);
                }
                for (i, (column, name)) in meta.columns.iter().zip(names.iter()).enumerate() {
                    if meta.unknown[i].is_none()
                        && !assignment::SourceType::new(&column.data_type, meta.type_oids.get(i))
                            .compatible(&table.columns[super::super::super::col(table, name)?].data_type)
                    {
                        return Err(SqlError::Type);
                    }
                }
            }
        }
        let (mut filter, _) =
            subquery::instantiate(&self.predicate, shape, None, state, parameters)?;
        filter.resolve(state, parameters)?;
        if let Some(returning) = &self.returning {
            let shape = self.returning_shape(shape, table);
            let (mut plan, _) = subquery::instantiate(returning, &shape, None, state, parameters)?;
            let mut meta = plan.resolve(state, parameters)?;
            if let Body::Select(select) = &returning.body {
                meta.type_oids.resize(meta.columns.len(), 0);
                for (oid, (item, _)) in meta.type_oids.iter_mut().zip(&select.query.items) {
                    if matches!(item,Item::Scalar(Scalar::Column(c)) if c.qualifier.is_none() && self.images.contains(&c.name))
                    {
                        *oid = type_oid(table.id)?.checked_add(3).ok_or(SqlError::Limit)?;
                    }
                }
            }
            Ok(meta)
        } else {
            Ok(bind::Meta {
                columns: vec![],
                typmods: vec![],
                type_oids: vec![],
                unknown: vec![],
                collations: vec![],
            })
        }
    }

    #[expect(clippy::too_many_arguments, reason = "paged mutation context")]
    pub(in crate::relational) fn paged(
        &self,
        state: &mut Relational,
        prepared: &Prepared,
        params: &[Value],
        clock: Option<&super::super::super::clock::ExecutionClock>,
        sink: &mut impl super::super::super::row_sink::RowSink,
        version_end: u64,
        deferred: Option<&mut super::super::super::DeferralState>,
    ) -> Result<SqlResult, crate::CoreError> {
        let _metadata = functions::collation::ColumnScope::enter(&[], &[]).map_err(crate::CoreError::Sql)?;
        use super::super::super::returning;
        let mut snapshot = state.clone();
        let mut retention = scan::Retention::new(scan::retention_limit());
        for name in self.read_tables() {
            let Some(table) = snapshot.tables.get(&name) else {
                continue;
            };
            let rows = snapshot
                .rows
                .get_mut(&table.id)
                .ok_or(crate::CoreError::Sql(SqlError::UnknownTable))?;
            sink.scan(table, &mut |row| {
                let key = super::super::super::pk(table, &row.values)?;
                retention.charge(super::super::super::searched::retained_row_bytes(
                    &key, &row,
                )?)?;
                rows.insert(key, row);
                Ok(())
            })?;
        }
        snapshot.secondary = super::super::super::secondary_index::Sidecar::default();
        let table = snapshot.tables[&self.target].clone();
        let description = prepared
            .describe(&snapshot)
            .map_err(crate::CoreError::Sql)?;
        let mut points = vec![];
        let mut collect = |point: &Prepared| -> R<()> {
            points.push(collect_point(&table, point)?);
            Ok(())
        };
        let output = self
            .execute_inner(
                &mut snapshot,
                prepared,
                params,
                clock,
                &mut || crate::cancellation::check().is_err(),
                &mut vec![],
                None,
                Some(&mut collect),
            )
            .map_err(crate::CoreError::Sql)?;
        let keys = points
            .iter()
            .map(|(k, _)| k.clone())
            .collect::<BTreeSet<_>>();
        if keys.is_empty() {
            return Ok(output);
        }
        let search = Some(Box::new(Query::mutation_input(&self.target, keys)));
        let returning = if self.returning.is_some() {
            let rows = points
                .iter()
                .map(|(k, _)| k.clone())
                .zip(output.rows)
                .collect();
            Some(returning::Returning::materialized(
                &self.target,
                description,
                rows,
            ))
        } else {
            None
        };
        let statement = if self.deleting {
            Statement::Delete {
                name: self.target.clone(),
                predicates: vec![],
                search,
                returning,
            }
        } else {
            let sets = batch_assignments(points).map_err(crate::CoreError::Sql)?;
            Statement::UpdateExpressions {
                name: self.target.clone(),
                sets,
                predicates: vec![],
                search,
                returning,
            }
        };
        let mut generated = Prepared::engine(statement, prepared.domain, state.epoch);
        generated.search_path = prepared.search_path.clone();
        state.execute_with_sink(&generated, &[], None, clock, sink, version_end, deferred)
    }
    fn bulk_target(&self, state: &Relational) -> Option<TableDef> {
        if self.deleting {
            return None;
        }
        let table = state.tables.get(&self.target)?;
        let assigned = |column: usize| self.groups.iter().any(|(names, _)| names.contains(&table.columns[column].name));
        // Without key or constraint changes, all computed postimages can go
        // through the ordinary searched UPDATE publisher in one batch.
        if table.primary_key.iter().any(|i| assigned(*i))
            || state.ext.constraints.get(&table.id).is_some_and(|constraints| {
                constraints.iter().any(|c| !matches!(c.kind, super::super::super::catalog_ext::ConstraintKind::PrimaryKey))
            })
            || state.ext.constraints.values().flatten().any(|c| {
                matches!(c.kind, super::super::super::catalog_ext::ConstraintKind::ForeignKey { ref_table, .. } if ref_table == table.id)
            })
            || state.secondary.descriptors().iter().any(|d| d.table == table.id && d.unique)
            || table.columns.iter().enumerate().any(|(i, c)| assigned(i)
                && (c.not_null || state.column_type(c.id).is_some()))
        {
            return None;
        }
        Some(table.clone())
    }

    #[expect(clippy::too_many_arguments, reason = "statement mutation context")]
    pub(super) fn execute(
        &self,
        state: &mut Relational,
        prepared: &Prepared,
        params: &[Value],
        clock: Option<&super::super::super::clock::ExecutionClock>,
        cancel: &mut dyn FnMut() -> bool,
        events: &mut Vec<super::super::super::constraints::Event>,
        injected_source: Option<&dyn scan::RowSource>,
    ) -> R<SqlResult> {
        let Some(table) = injected_source.is_none().then(|| self.bulk_target(state)).flatten() else {
            return self.execute_inner(state, prepared, params, clock, cancel, events, injected_source, None);
        };
        let mut points = vec![];
        let output = self.execute_inner(state, prepared, params, clock, cancel, events, None, Some(&mut |point| {
            points.push(collect_point(&table, point)?);
            Ok(())
        }))?;
        let mut points = points.into_iter();
        loop {
            let chunk: Vec<_> = points.by_ref().take(MAX_ROWS).collect();
            if chunk.is_empty() {
                break;
            }
            let keys = chunk.iter().map(|(key, _)| key.clone()).collect();
            let mut batch = Prepared::engine(Statement::UpdateExpressions {
                name: self.target.clone(),
                sets: batch_assignments(chunk)?,
                predicates: vec![],
                search: Some(Box::new(Query::mutation_input(&self.target, keys))),
                returning: None,
            }, prepared.domain, state.epoch);
            batch.search_path = prepared.search_path.clone();
            state.execute_statement_inner(&batch, &[], None, &mut *cancel, clock, None, events)?;
        }
        Ok(output)
    }
    #[expect(
        clippy::too_many_arguments,
        reason = "statement snapshot and optional paged publisher"
    )]
    fn execute_inner(
        &self,
        state: &mut Relational,
        prepared: &Prepared,
        params: &[Value],
        clock: Option<&super::super::super::clock::ExecutionClock>,
        cancel: &mut dyn FnMut() -> bool,
        events: &mut Vec<super::super::super::constraints::Event>,
        injected_source: Option<&dyn scan::RowSource>,
        mut publish: Option<&mut Publisher<'_>>,
    ) -> R<SqlResult> {
        let _metadata = functions::collation::ColumnScope::enter(&[], &[])?;
        let description = prepared.describe(state)?;
        let parameters = description
            .parameters
            .iter()
            .cloned()
            .map(Some)
            .collect::<Vec<_>>();
        for (value, ty) in params.iter().zip(&description.parameters) {
            super::super::super::check(
                value,
                &ColumnDef {
                    id: 0,
                    name: String::new(),
                    data_type: ty.clone(),
                    not_null: false,
                },
            )?;
        }
        let snapshot = state.clone();
        let catalog = self.catalog(&snapshot)?;
        let table = &snapshot.tables[&self.target];
        let shape = self
            .from
            .clone()
            .resolve(&catalog, &mut parameters.clone())?;
        let legacy = scan::LegacyRows(&snapshot);
        let base = injected_source.unwrap_or(&legacy);
        let source = Keys {
            base,
            target: table.id,
        };
        let execution = Execution::new(cancel, &parameters);
        let joined = self
            .from
            .run(&catalog, &source, params, &parameters, clock, &execution)?;
        let key_index = shape.namespace.resolve(&ColumnRef {
            qualifier: Some(self.alias.clone()),
            name: KEY.into(),
        })?;
        let mut seen = BTreeSet::new();
        let retained = RetentionScope::new(&execution);
        let mut result_bytes = 0;
        let mut result = SqlResult {
            columns: description.columns.into_iter().map(|c| c.name).collect(),
            rows: vec![],
            affected: 0,
        };
        let returning_shape = self.returning_shape(&shape, table);
        let direct = self.groups.iter().map(|(names, plan)| match (names.as_slice(), plan) {
            ([_], Some(plan)) => subquery::ScalarProjection::bind(plan, &shape, &snapshot, &parameters),
            _ => Ok(None),
        }).collect::<R<Vec<_>>>()?;
        let apply_context = subquery::ApplyContext {
            state: &snapshot, source: base, params, parameters: &parameters, clock, execution: &execution,
        };
        for matched in &joined.rows {
            if execution.cancelled() {
                return Err(SqlError::Aborted);
            }
            let (filter, _) = subquery::instantiate(
                &self.predicate,
                &shape,
                Some(matched),
                &snapshot,
                &parameters,
            )?;
            if filter
                .run(&snapshot, base, params, &parameters, clock, &execution)?
                .rows
                .is_empty()
            {
                continue;
            }
            let Value::Bytes(key) = &matched.values[key_index] else {
                return Err(SqlError::Type);
            };
            if seen.contains(key) {
                continue;
            }
            retained
                .borrow()
                .charge(key.len() + std::mem::size_of::<Vec<u8>>())?;
            seen.insert(key.clone());
            let old = snapshot.rows[&table.id]
                .get(key)
                .ok_or(SqlError::UnknownColumn)?;
            let mut new = old.clone();
            let mut sets = vec![];
            for ((names, plan), direct) in self.groups.iter().zip(&direct) {
                let values = if let Some(direct) = direct {
                    let value = direct.evaluate(&shape, matched, &apply_context)?;
                    let target = &table.columns[super::super::super::col(table, &names[0])?].data_type;
                    vec![direct.source_type.convert(value, target, clock)?]
                } else if let Some(plan) = plan {
                    let (mut plan, _) =
                        subquery::instantiate(plan, &shape, Some(matched), &snapshot, &parameters)?;
                    let meta = plan.resolve(&snapshot, &mut parameters.clone())?;
                    let rows = plan.run(&snapshot, base, params, &parameters, clock, &execution)?;
                    if rows.rows.len() > 1 {
                        return Err(subquery::cardinality_error());
                    }
                    if let Some(row) = rows.rows.first() {
                        row.values
                            .iter()
                            .zip(&meta.columns)
                            .zip(names)
                            .enumerate()
                            .map(|(i, ((v, c), n))| {
                                let target =
                                    &table.columns[super::super::super::col(table, n)?].data_type;
                                if meta.unknown[i].is_some() {
                                    assignment::literal(v.clone(), target, clock)
                                } else {
                                    assignment::SourceType::new(&c.data_type, meta.type_oids.get(i))
                                        .convert(v.clone(), target, clock)
                                }
                            })
                            .collect::<R<Vec<_>>>()?
                    } else {
                        vec![Value::Null; names.len()]
                    }
                } else {
                    names
                        .iter()
                        .map(|name| {
                            defaults::evaluate(
                                &snapshot,
                                &table.columns[super::super::super::col(table, name)?],
                                clock,
                            )
                        })
                        .collect::<R<Vec<_>>>()?
                };
                for (name, value) in names.iter().zip(values) {
                    let i = super::super::super::col(table, name)?;
                    let value =
                        super::super::super::coerce_to_column(&snapshot, &table.columns[i], value)?;
                    new.values[i] = value.clone();
                    sets.push((name.clone(), Expr::Literal(value)));
                }
            }
            // Reuse point UPDATE/DELETE, including immediate PK/UNIQUE checks and events.
            // A one-row input also preserves hidden heap row IDs.
            let mut point = Prepared::engine(
                if self.deleting {
                    Statement::Delete {
                        name: self.target.clone(),
                        predicates: vec![],
                        search: None,
                        returning: None,
                    }
                } else {
                    Statement::Update {
                        name: self.target.clone(),
                        sets,
                        predicates: vec![],
                        search: None,
                        returning: None,
                    }
                },
                prepared.domain,
                state.epoch,
            );
            let predicates = table
                .primary_key
                .iter()
                .map(|i| Predicate {
                    column: table.columns[*i].name.clone(),
                    op: "=".into(),
                    value: Expr::Literal(old.values[*i].clone()),
                })
                .collect();
            match point.statement.get_mut() {
                Statement::Update { predicates: p, .. }
                | Statement::Delete { predicates: p, .. } => *p = predicates,
                _ => unreachable!(),
            }
            point.search_path = prepared.search_path.clone();
            if let Some(publish) = publish.as_mut() {
                retained
                    .borrow()
                    .charge(super::super::super::searched::retained_row_bytes(
                        key, &new,
                    )?)?;
                publish(&point)?;
            } else {
                state.execute_statement_inner(
                    &point,
                    &[],
                    None,
                    || execution.cancelled(),
                    clock,
                    None,
                    events,
                )?;
            }
            result.affected += 1;
            if let Some(returning) = &self.returning {
                let mut input = matched.clone();
                if !self.deleting {
                    for (name, index) in &shape
                        .namespace
                        .qualified
                        .iter()
                        .find(|(a, _)| a == &self.alias)
                        .ok_or(SqlError::UnknownTable)?
                        .1
                    {
                        if let Ok(i) = super::super::super::col(table, name) {
                            input.values[*index] = new.values[i].clone();
                        }
                    }
                }
                for image in [Some(old), if self.deleting { None } else { Some(&new) }] {
                    let values = table
                        .logical_offsets()
                        .iter()
                        .map(|i| image.map_or(Value::Null, |r| r.values[*i].clone()))
                        .collect::<Vec<_>>();
                    input.values.extend(values.iter().cloned());
                    input.values.push(if image.is_some() {
                        Value::Text(super::record(&values)?)
                    } else {
                        Value::Null
                    });
                }
                let (plan, _) = subquery::instantiate(
                    returning,
                    &returning_shape,
                    Some(&input),
                    &snapshot,
                    &parameters,
                )?;
                for row in plan
                    .run(&snapshot, base, params, &parameters, clock, &execution)?
                    .rows
                {
                    if result.rows.len() >= scan::query_row_limit() {
                        return Err(SqlError::Limit);
                    }
                    let mut bytes = std::mem::size_of::<SqlRow>();
                    for value in &row.values {
                        charge(&mut result_bytes, value)?;
                        bytes += std::mem::size_of::<Value>() + value_bytes(value);
                    }
                    retained.borrow().charge(bytes)?;
                    result.rows.push(row);
                }
            }
        }
        Ok(result)
    }
}
pub(super) struct Keys<'a> {
    pub(super) base: &'a dyn scan::RowSource,
    pub(super) target: u64,
}
impl scan::RowSource for Keys<'_> {
    fn scan<'s>(
        &'s self,
        table: &TableDef,
        range: &scan::KeyRange,
        direction: scan::Direction,
    ) -> R<scan::SourceRows<'s>> {
        let rows = self.base.scan(table, range, direction)?;
        let is_target = table.id == self.target;
        Ok(Box::new(rows.map(move |row| {
            let mut row = row?;
            if is_target {
                let mut owned = row.row.into_owned();
                owned.values.push(Value::Bytes(row.key.to_vec()));
                row.row = Cow::Owned(owned);
            }
            Ok(row)
        })))
    }
    fn get<'s>(&'s self, table: &TableDef, key: &[u8]) -> R<Option<scan::SourceRow<'s>>> {
        self.base.get(table, key).map(|row| {
            row.map(|mut row| {
                if table.id == self.target {
                    let mut owned = row.row.into_owned();
                    owned.values.push(Value::Bytes(row.key.to_vec()));
                    row.row = Cow::Owned(owned);
                }
                row
            })
        })
    }
}

#[cfg(all(test, feature = "engine-rocksdb"))]
mod tests {
    use super::*;
    use crate::{Admin, Engine, Limits, SqlSession};
    struct Fixture {
        admin: Option<Admin>,
        path: std::path::PathBuf,
    }
    impl Fixture {
        fn new() -> Self {
            let mut nonce = [0; 16];
            getrandom::fill(&mut nonce).unwrap();
            let path = std::env::temp_dir()
                .join(format!("plan-mutation-{:032x}", u128::from_be_bytes(nonce)));
            Self {
                admin: Some(Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap()),
                path,
            }
        }
        fn session(&self) -> SqlSession {
            self.admin.as_ref().unwrap().sql_session()
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            self.admin.take();
            std::fs::remove_dir_all(&self.path).unwrap();
        }
    }
    fn run(s: &mut SqlSession, sql: &str) -> SqlResult {
        s.execute(sql, &[], None)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
    }
    fn values(s: &mut SqlSession, sql: &str) -> Vec<Vec<Value>> {
        run(s, sql).rows.into_iter().map(|r| r.values).collect()
    }
    fn seeded() -> (Fixture, SqlSession) {
        let f = Fixture::new();
        let mut s = f.session();
        run(
            &mut s,
            "CREATE TABLE m(id int PRIMARY KEY,a int,b text DEFAULT 'z')",
        );
        run(&mut s, "INSERT INTO m VALUES(1,10,'x'),(2,20,'y')");
        (f, s)
    }
    #[test]
    fn mutation_claims_only_from_using_and_assignment_targets() {
        for sql in [
            "UPDATE t SET a=1 WHERE a IS DISTINCT FROM 2",
            "UPDATE t SET a=(1)=1",
            "UPDATE t SET a=id IS DISTINCT FROM 2",
            "DELETE FROM t WHERE a IS DISTINCT FROM 2",
        ] {
            assert!(!Plan::claims(&lex(sql).unwrap()), "{sql}");
        }
        for sql in [
            "UPDATE t SET a=1,(b,c)=(2,3)",
            "UPDATE t SET a=1 FROM s",
            "DELETE FROM t USING s",
        ] {
            assert!(Plan::claims(&lex(sql).unwrap()), "{sql}");
        }
    }
    #[test]
    fn joined_update_once_and_returning_source() {
        let (_f, mut s) = seeded();
        let r = run(
            &mut s,
            "UPDATE m t SET a=s.v FROM (VALUES(1,11),(1,12)) s(id,v) WHERE t.id=s.id RETURNING t.id,s.v,a",
        );
        assert_eq!(r.affected, 1);
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0].values[1], r.rows[0].values[2]);
        assert_eq!(
            run(
                &mut s,
                "UPDATE m SET a=7 FROM (VALUES(99)) s(id) WHERE m.id=s.id"
            )
            .affected,
            0
        );
    }
    #[test]
    fn delete_join_and_self_join() {
        let (_f, mut s) = seeded();
        assert_eq!(run(&mut s,"DELETE FROM m t USING (VALUES(2)) s(id) JOIN (VALUES(2)) u(id) ON s.id=u.id WHERE t.id=s.id RETURNING t.id,s.id").rows[0].values,vec![Value::Int(2),Value::Int(2)]);
        assert_eq!(
            run(&mut s, "DELETE FROM m USING m g WHERE m.id=g.id").affected,
            1
        );
    }
    #[test]
    fn row_forms_correlated_empty_and_errors() {
        let (_f, mut s) = seeded();
        assert_eq!(
            values(
                &mut s,
                "UPDATE m SET (a,b)=(5,'q') WHERE id=1 RETURNING a,b"
            ),
            vec![vec![Value::Int(5), Value::Text("q".into())]]
        );
        assert_eq!(
            values(
                &mut s,
                "UPDATE m SET (a,b)=ROW(6,'r') WHERE id=1 RETURNING a,b"
            ),
            vec![vec![Value::Int(6), Value::Text("r".into())]]
        );
        assert_eq!(
            values(
                &mut s,
                "UPDATE m t SET (a,b)=(SELECT s.a,s.b FROM m s WHERE s.id=t.id+1) WHERE t.id=1 RETURNING a,b"
            ),
            vec![vec![Value::Int(20), Value::Text("y".into())]]
        );
        assert_eq!(
            values(
                &mut s,
                "UPDATE m t SET (a,b)=(SELECT s.a,s.b FROM m s WHERE s.id=99) WHERE t.id=1 RETURNING a,b"
            ),
            vec![vec![Value::Null, Value::Null]]
        );
        for (sql, code) in [
            ("UPDATE m SET (a,b)=(SELECT a,b FROM m)", "21000"),
            ("UPDATE m SET (a,b)=(5)", "0A000"),
            ("UPDATE m SET (a)=(7)", "0A000"),
            ("UPDATE m SET (a,b)=(1,2,3)", "42601"),
        ] {
            let e = s.execute(sql, &[], None).unwrap_err();
            assert_eq!(crate::sqlstate(&e), code, "{sql}: {e:?}");
        }
    }
    #[test]
    fn images_for_all_commands() {
        let (_f, mut s) = seeded();
        assert_eq!(
            values(
                &mut s,
                "UPDATE m SET a=a+1 WHERE id=1 RETURNING WITH (OLD AS o,NEW AS n) o.a,n.a,a"
            ),
            vec![vec![Value::Int(10), Value::Int(11), Value::Int(11)]]
        );
        assert_eq!(
            values(
                &mut s,
                "DELETE FROM m WHERE id=2 RETURNING old.id,new.id,old,new"
            ),
            vec![vec![
                Value::Int(2),
                Value::Null,
                Value::Text("(2,20,y)".into()),
                Value::Null
            ]]
        );
        assert_eq!(
            values(
                &mut s,
                "INSERT INTO m VALUES(3,30,'z') RETURNING old.id,new.id,old,new"
            ),
            vec![vec![
                Value::Null,
                Value::Int(3),
                Value::Null,
                Value::Text("(3,30,z)".into())
            ]]
        );
        assert_eq!(
            values(
                &mut s,
                "UPDATE m SET a=s.v FROM (VALUES(1,40)) s(id,v) WHERE m.id=s.id RETURNING old.a,new.a,a"
            ),
            vec![vec![Value::Int(11), Value::Int(40), Value::Int(40)]]
        );
    }
    #[test]
    fn cascades_from_joined_mutations() {
        let (_f, mut s) = seeded();
        run(
            &mut s,
            "CREATE TABLE child(id int PRIMARY KEY,p int REFERENCES m(id) ON UPDATE CASCADE ON DELETE CASCADE)",
        );
        run(&mut s, "INSERT INTO child VALUES(1,1),(2,2)");
        run(
            &mut s,
            "UPDATE m SET id=s.n FROM (VALUES(1,3)) s(id,n) WHERE m.id=s.id",
        );
        assert_eq!(
            values(&mut s, "SELECT p FROM child ORDER BY id"),
            vec![vec![Value::Int(3)], vec![Value::Int(2)]]
        );
        run(
            &mut s,
            "DELETE FROM m USING (VALUES(3)) s(id) WHERE m.id=s.id",
        );
        assert_eq!(
            values(&mut s, "SELECT * FROM child"),
            vec![vec![Value::Int(2), Value::Int(2)]]
        );
    }
    #[test]
    fn no_action_fallback_keeps_only_selected_target_rows() {
        let (_f, mut s) = seeded();
        run(
            &mut s,
            "CREATE TABLE ref_m(id int PRIMARY KEY,p int REFERENCES m(id))",
        );
        run(&mut s, "INSERT INTO ref_m VALUES(1,2)");
        assert_eq!(
            run(
                &mut s,
                "DELETE FROM m USING (VALUES(1)) source(id) WHERE m.id=source.id RETURNING m.id"
            )
            .affected,
            1
        );
        assert_eq!(
            values(&mut s, "SELECT id FROM m"),
            vec![vec![Value::Int(2)]]
        );
    }
    #[test]
    fn multirow_expressions_defaults_parameters_and_conflict() {
        let (_f, mut s) = seeded();
        let r = s
            .execute(
                "INSERT INTO m VALUES($1,abs(-3),DEFAULT),($2,4+5,upper('v')) RETURNING id,b",
                &[Value::Int(3), Value::Int(4)],
                None,
            )
            .unwrap();
        assert_eq!(
            r.rows.iter().map(|r| r.values.clone()).collect::<Vec<_>>(),
            vec![
                vec![Value::Int(3), Value::Text("z".into())],
                vec![Value::Int(4), Value::Text("V".into())]
            ]
        );
        run(
            &mut s,
            "INSERT INTO m VALUES(3,2*6,DEFAULT),(5,5+5,DEFAULT) ON CONFLICT(id) DO UPDATE SET a=excluded.a",
        );
        assert_eq!(
            values(&mut s, "SELECT a FROM m WHERE id=3"),
            vec![vec![Value::Int(12)]]
        );
    }

    #[test]
    fn joined_mutations_use_v2_pages_with_indexes_and_heap_keys() {
        let f = Fixture::new();
        let mut s = f.session();
        run(&mut s, "CREATE TABLE big(id int PRIMARY KEY,a int)");
        run(&mut s, "CREATE INDEX big_a ON big(a)");
        for start in [1, 551] {
            let rows = (start..start + 550)
                .map(|i| format!("({i},{i})"))
                .collect::<Vec<_>>()
                .join(",");
            run(&mut s, &format!("INSERT INTO big VALUES {rows}"));
        }
        assert_eq!(
            run(
                &mut s,
                "UPDATE big SET a=a+s.v FROM (VALUES(1)) s(v) RETURNING id,a"
            )
            .affected,
            1100
        );
        assert_eq!(
            values(&mut s, "SELECT count(*) FROM big WHERE a>1"),
            vec![vec![Value::Int(1100)]]
        );
        assert_eq!(
            run(
                &mut s,
                "DELETE FROM big USING (VALUES(1)) s(v) WHERE big.a>s.v"
            )
            .affected,
            1100
        );
        run(&mut s, "CREATE TABLE heap(a int,b text)");
        run(&mut s, "INSERT INTO heap VALUES(1,'x'),(1,'y')");
        assert_eq!(
            run(
                &mut s,
                "UPDATE heap SET b=s.v FROM (VALUES('z')) s(v) RETURNING old.b,new.b"
            )
            .affected,
            2
        );
        assert_eq!(
            run(
                &mut s,
                "DELETE FROM heap USING (VALUES(1)) s(a) WHERE heap.a=s.a"
            )
            .affected,
            2
        );
    }
    #[test]
    fn joined_dml_dependencies_and_cte_input() {
        let (_f, mut s) = seeded();
        run(&mut s, "CREATE TABLE src(id int PRIMARY KEY,v int)");
        run(&mut s, "INSERT INTO src VALUES(1,55)");
        let p = Prepared::parse("UPDATE m SET a=src.v FROM src WHERE m.id=src.id", 1, 0).unwrap();
        assert_eq!(p.command(), "UPDATE");
        assert!(!p.is_read());
        assert_eq!(p.write_tables(), vec![("m".into(), "UPDATE")]);
        assert!(p.read_tables().contains(&"m".into()));
        assert!(p.read_tables().contains(&"src".into()));
        assert_eq!(p.task_class(), crate::task::StatementClass::Other);
        assert_eq!(
            values(
                &mut s,
                "WITH source AS (SELECT * FROM src), d AS (UPDATE m SET a=source.v FROM source WHERE m.id=source.id RETURNING m.id,a) SELECT * FROM d"
            ),
            vec![vec![Value::Int(1), Value::Int(55)]]
        );
    }
    #[test]
    fn joined_target_lock_matches_plain_update() {
        let (f, mut a) = seeded();
        let mut b = f.session();
        run(&mut a, "BEGIN");
        run(
            &mut a,
            "UPDATE m SET a=s.v FROM (VALUES(1,90)) s(id,v) WHERE m.id=s.id",
        );
        assert!(
            a.held_table_locks()
                .iter()
                .any(|(_, _, mode, _)| *mode == "RowExclusiveLock")
        );
        run(&mut b, "BEGIN");
        let e = b
            .lock_tables(&["m".into()], crate::SqlTableLockMode::Share, true)
            .unwrap_err();
        assert_eq!(crate::sqlstate(&e), "55P03");
        run(&mut b, "ROLLBACK");
        run(&mut a, "ROLLBACK");
        assert_eq!(run(&mut b, "UPDATE m SET a=0 WHERE id=1").affected, 1);
    }
}

impl Mutation {
    pub(super) fn has_privilege_inquiry(&self) -> bool {
        let mut found = false;
        self.from.visit_queries(&mut |q| found |= q.has_privilege_inquiry());
        self.from.visit_scalars(&mut |s| found |= s.has_privilege_inquiry());
        found || self.groups.iter().filter_map(|(_,p)| p.as_ref()).chain(Some(&self.predicate))
            .chain(self.returning.as_ref()).any(Plan::has_privilege_inquiry)
    }
}
