//! MERGE selects from one full-join snapshot and publishes ordinary DML actions.
use super::*;
use mutation::{Mutation, projection, splits};
const PRESENCE: &str = "\0merge_source";

type Publisher<'a> = dyn FnMut(&Prepared, &TableDef, Option<(&[u8], &SqlRow)>) -> R<SqlResult> + 'a;
type ReturningSink<'a> = dyn FnMut(SqlRow) -> R<()> + 'a;
use super::super::super::{row_sink, spill};
use std::borrow::Cow;
type Groups = Vec<(Vec<String>, Option<Plan>)>;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Matched,
    Target,
    Source,
}
#[derive(Debug, Clone)]
enum Action {
    Update(Groups),
    Delete,
    Nothing,
    Insert {
        columns: Vec<String>,
        values: Vec<Option<Plan>>,
        overriding: Option<bool>,
    },
}
impl Action {
    fn command(&self) -> Option<&'static str> {
        match self {
            Self::Update(_) => Some("UPDATE"),
            Self::Delete => Some("DELETE"),
            Self::Insert { .. } => Some("INSERT"),
            Self::Nothing => None,
        }
    }
}
#[derive(Debug, Clone)]
struct When {
    kind: Kind,
    condition: Option<Plan>,
    action: Action,
}
#[derive(Debug, Clone)]
pub(in crate::relational) struct Merge {
    input: Mutation,
    action_name: String,
    clauses: Vec<When>,
}
pub(super) fn claims(tokens: &[Token]) -> bool {
    tokens.first().is_some_and(|t| parse::word(t, "merge"))
}
pub(super) fn action_call(tokens: &[Token]) -> bool {
    tokens.windows(2).any(|t| {
        matches!(&t[0],Token::Word(w)|Token::QuotedIdent(w) if w=="merge_action")
            && t[1] == Token::Symbol('(')
    })
}
fn target_tokens(target: &str, alias: &str) -> Vec<Token> {
    let (schema, name) = split_key(target);
    let mut t = vec![];
    if schema != "public" {
        t.extend([Token::QuotedIdent(schema.into()), Token::Symbol('.')]);
    }
    t.extend([
        Token::QuotedIdent(name.into()),
        Token::Word("as".into()),
        Token::QuotedIdent(alias.into()),
    ]);
    t
}
fn fresh(tokens: &[Token], label: &str, next: &mut usize) -> String {
    loop {
        let name = format!("plan_merge_{label}_{next}");
        *next += 1;
        if !tokens
            .iter()
            .any(|t| matches!(t, Token::Word(w) | Token::QuotedIdent(w) if w == &name))
        {
            return name;
        }
    }
}
// CASE clauses are scalar syntax, even when they are outside parentheses.
fn clause_top(tokens: &[Token]) -> R<Vec<usize>> {
    let mut cases = 0usize;
    Ok(parse::top(tokens)?
        .into_iter()
        .filter(|i| {
            if parse::word(&tokens[*i], "case") {
                cases += 1;
                false
            } else if cases > 0 && parse::word(&tokens[*i], "end") {
                cases -= 1;
                false
            } else {
                cases == 0
            }
        })
        .collect())
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
    let presence_name = fresh(tokens, "presence", next);
    let action_name = fresh(tokens, "action", next);
    let mut p = parse::parser(tokens);
    p.need("merge")?;
    p.need("into")?;
    p.word("only");
    let target = p.table_name()?;
    p.sym('*');
    let alias = if p.word("as") || !p.tokens.get(p.at).is_some_and(|t| parse::word(t, "using")) {
        p.name()?
    } else {
        split_key(&target).1.into()
    };
    p.need("using")?;
    let top = clause_top(tokens)?;
    let returning_start = top
        .iter()
        .copied()
        .find(|i| parse::word(&tokens[*i], "returning"))
        .unwrap_or(tokens.len());
    let context = &tokens[2..returning_start];
    if action_call(context)
        && !context
            .windows(2)
            .any(|t| parse::word(&t[0], "merge") && parse::word(&t[1], "into"))
    {
        return Err(ParseError::Definite(SqlError::Syntax));
    }
    let first_when = top
        .iter()
        .copied()
        .find(|i| *i >= p.at && parse::word(&tokens[*i], "when"))
        .ok_or(ParseError::Decline)?;
    // The MERGE ON is the last top-level ON, after any source joins' ONs.
    let on = top
        .iter()
        .copied()
        .rfind(|i| *i >= p.at && *i < first_when && parse::word(&tokens[*i], "on"))
        .ok_or(ParseError::Decline)?;
    let mut from = vec![
        Token::Word("select".into()),
        Token::Symbol('*'),
        Token::Word("from".into()),
    ];
    from.extend(target_tokens(&target, &alias));
    from.extend([
        Token::Word("full".into()),
        Token::Word("join".into()),
        Token::Symbol('('),
    ]);
    from.extend_from_slice(&tokens[p.at..on]);
    from.extend([
        Token::Word("cross".into()),
        Token::Word("join".into()),
        Token::Symbol('('),
        Token::Word("values".into()),
        Token::Symbol('('),
        Token::Word("true".into()),
        Token::Symbol(')'),
        Token::Symbol(')'),
        Token::Word("as".into()),
        Token::QuotedIdent(presence_name.clone()),
        Token::Symbol('('),
        Token::QuotedIdent(presence_name.clone()),
        Token::Symbol(')'),
        Token::Symbol(')'),
        Token::Word("on".into()),
    ]);
    // MERGE permits general ON predicates, unlike SQL FULL JOIN's
    // merge/hash-join restriction. The planner's materialized operator can
    // evaluate them directly once the FROM tree has been parsed.
    from.push(Token::Word("true".into()));
    let mut on_parser = parse::parser(&tokens[on + 1..first_when]);
    let on_condition = Filter::parse(&mut on_parser, 0, 0)?;
    if on_parser.at != on_parser.tokens.len() {
        return Err(ParseError::Decline);
    }
    owner.relations.extend(on_parser.relations);
    let plan = join::parse_select(&from, None, owner, depth, next)?.ok_or(ParseError::Decline)?;
    let Body::Select(select) = plan.body else {
        return Err(ParseError::Decline);
    };
    let mut from = select.from.ok_or(ParseError::Decline)?;
    // Replace the collision-free parser spelling with an unspellable internal name.
    let join::From::Join {
        right,
        condition,
        kind,
        ..
    } = &mut from
    else {
        return Err(ParseError::Decline);
    };
    *kind = join::Kind::MergeFull;
    *condition = join::Condition::On(Box::new(on_condition));
    let join::From::Join { right, .. } = right.as_mut() else {
        return Err(ParseError::Decline);
    };
    let join::From::Leaf {
        alias: marker_alias,
        aliases,
        ..
    } = right.as_mut()
    else {
        return Err(ParseError::Decline);
    };
    *marker_alias = PRESENCE.into();
    *aliases = vec![PRESENCE.into()];
    let returning = top
        .iter()
        .copied()
        .find(|i| *i > first_when && parse::word(&tokens[*i], "returning"))
        .unwrap_or(tokens.len());
    let whens = top
        .iter()
        .copied()
        .filter(|i| *i >= first_when && *i < returning && parse::word(&tokens[*i], "when"))
        .chain(Some(returning))
        .collect::<Vec<_>>();
    let mut clauses = vec![];
    let mut unconditional = vec![];
    for pair in whens.windows(2) {
        let mut w = parse::parser(&tokens[pair[0] + 1..pair[1]]);
        let kind = if w.word("not") {
            w.need("matched")?;
            if w.word("by") {
                if w.word("source") {
                    Kind::Source
                } else {
                    w.need("target")?;
                    Kind::Target
                }
            } else {
                Kind::Target
            }
        } else {
            w.need("matched")?;
            Kind::Matched
        };
        if unconditional.contains(&kind) {
            return Err(ParseError::Definite(SqlError::Syntax));
        }
        let then = clause_top(&w.tokens)?
            .into_iter()
            .find(|i| *i >= w.at && parse::word(&w.tokens[*i], "then"))
            .ok_or(ParseError::Decline)?;
        let condition = if w.word("and") {
            Some(projection(
                &[
                    &[Token::Word("true".into()), Token::Word("where".into())][..],
                    &w.tokens[w.at..then],
                ]
                .concat(),
                owner,
                depth,
                next,
            )?)
        } else {
            if w.at != then {
                return Err(ParseError::Decline);
            }
            unconditional.push(kind);
            None
        };
        w.at = then + 1;
        let action = if w.word("do") {
            w.need("nothing")?;
            Action::Nothing
        } else if w.word("delete") {
            if kind == Kind::Target {
                return Err(ParseError::Definite(SqlError::Syntax));
            }
            Action::Delete
        } else if w.word("update") {
            if kind == Kind::Target {
                return Err(ParseError::Definite(SqlError::Syntax));
            }
            w.need("set")?;
            // Use the UPDATE parser for row assignments, defaults and correlated subqueries.
            let mut t = vec![Token::Word("update".into())];
            t.extend(target_tokens(&target, &alias));
            t.push(Token::Word("set".into()));
            t.extend_from_slice(&w.tokens[w.at..]);
            let m = mutation::parse(&t, owner, depth, next)?
                .mutation
                .ok_or(ParseError::Decline)?;
            w.at = w.tokens.len();
            Action::Update(m.groups)
        } else if w.word("insert") {
            if kind != Kind::Target {
                return Err(ParseError::Definite(SqlError::Syntax));
            }
            let columns = if w.tokens.get(w.at) == Some(&Token::Symbol('(')) {
                w.names()?
            } else {
                vec![]
            };
            let overriding = if w.word("overriding") {
                let system = if w.word("system") {
                    true
                } else {
                    w.need("user")?;
                    false
                };
                w.need("value")?;
                Some(system)
            } else {
                None
            };
            let values = if w.word("default") {
                w.need("values")?;
                vec![]
            } else {
                w.need("values")?;
                w.punct('(')?;
                if w.tokens.last() != Some(&Token::Symbol(')')) {
                    return Err(ParseError::Decline);
                }
                let values = splits(&w.tokens[w.at..w.tokens.len() - 1])?
                    .into_iter()
                    .map(|t| {
                        if t.len() == 1 && parse::word(&t[0], "default") {
                            Ok(None)
                        } else {
                            projection(t, owner, depth, next).map(Some)
                        }
                    })
                    .collect::<parse::ParseResult<Vec<_>>>()?;
                w.at = w.tokens.len();
                values
            };
            Action::Insert {
                columns,
                values,
                overriding,
            }
        } else {
            return Err(ParseError::Decline);
        };
        if w.at != w.tokens.len() {
            return Err(ParseError::Decline);
        }
        clauses.push(When {
            kind,
            condition,
            action,
        });
    }
    let mut images = ["old".into(), "new".into()];
    let returning = if returning < tokens.len() {
        let mut r = parse::parser(&tokens[returning + 1..]);
        if r.word("with") {
            r.punct('(')?;
            loop {
                let n = if r.word("old") {
                    0
                } else {
                    r.need("new")?;
                    1
                };
                r.need("as")?;
                images[n] = r.name()?;
                if !r.sym(',') {
                    break;
                }
            }
            r.punct(')')?;
        }
        let mut t = vec![];
        let mut i = r.at;
        while i < r.tokens.len() {
            if parse::word(&r.tokens[i], "merge_action")
                && r.tokens.get(i + 1) == Some(&Token::Symbol('('))
                && r.tokens.get(i + 2) == Some(&Token::Symbol(')'))
                && (i == 0 || r.tokens[i - 1] != Token::Symbol('.'))
            {
                t.push(Token::QuotedIdent(action_name.clone()));
                i += 3;
            } else {
                t.push(r.tokens[i].clone());
                i += 1;
            }
        }
        let mut plan = projection(&t, owner, depth, next)?;
        if let Body::Select(s) = &mut plan.body {
            for (item, alias) in &mut s.query.items {
                if matches!(item,Item::Scalar(Scalar::Column(c)) if c.name==action_name)
                    && alias.as_deref() == Some(action_name.as_str())
                {
                    *alias = Some("merge_action".into());
                }
            }
        }
        Some(plan)
    } else {
        None
    };
    owner.relations.extend(p.relations);
    let mut plan = Plan::new(Body::Values(vec![]));
    plan.merge = Some(Box::new(Merge {
        action_name,
        input: Mutation {
            target,
            alias,
            deleting: false,
            from,
            predicate: projection(&[Token::Word("true".into())], owner, depth, next)?,
            groups: vec![],
            qualified_target: None,
            returning,
            images,
        },
        clauses,
    }));
    Ok(plan)
}
impl Merge {
    pub(super) fn has_returning(&self) -> bool {
        self.input.has_returning()
    }
    pub(super) fn references_cte(&self, name: &str) -> bool {
        cte::mutation_references(&self.input, name) != 0
            || self.plans().any(|plan| cte::references(plan, name) != 0)
    }

    fn plans(&self) -> impl Iterator<Item = &Plan> {
        self.clauses.iter().flat_map(|w| {
            w.condition.iter().chain(match &w.action {
                Action::Update(g) => g.iter().filter_map(|(_, p)| p.as_ref()).collect::<Vec<_>>(),
                Action::Insert { values, .. } => values.iter().flatten().collect(),
                _ => vec![],
            })
        })
    }
    fn map_plans(&mut self, mut f: impl FnMut(&mut Plan) -> R<()>) -> R<()> {
        for w in &mut self.clauses {
            if let Some(p) = &mut w.condition {
                f(p)?;
            }
            match &mut w.action {
                Action::Update(g) => {
                    for (_, p) in g {
                        if let Some(p) = p {
                            f(p)?;
                        }
                    }
                }
                Action::Insert { values, .. } => {
                    for p in values.iter_mut().flatten() {
                        f(p)?;
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
    pub(super) fn read_tables(&self) -> Vec<String> {
        self.input
            .read_tables()
            .into_iter()
            .chain(self.plans().flat_map(Plan::read_tables))
            .filter(|n| !n.starts_with("\0plan_"))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }
    pub(super) fn sequences(&self) -> Vec<SequenceDependency> {
        self.input
            .sequences()
            .into_iter()
            .chain(self.plans().flat_map(Plan::sequences))
            .collect()
    }
    fn implicit_sequences(&self, state: &Relational) -> R<Vec<Option<String>>> {
        let mut out = vec![];
        for w in &self.clauses {
            match &w.action {
                Action::Insert {
                    columns,
                    values,
                    overriding,
                } => out.extend(
                    sql_insert::Insert::merge_row(
                        self.input.target.clone(),
                        columns.clone(),
                        values
                            .iter()
                            .map(|p| p.as_ref().map(|_| Value::Null))
                            .collect(),
                        *overriding,
                    )
                    .implicit_sequences(state)?,
                ),
                Action::Update(groups) => {
                    let table = state
                        .tables
                        .get(&self.input.target)
                        .ok_or(SqlError::UnknownTable)?;
                    for (names, p) in groups {
                        if p.is_none() {
                            for name in names {
                                if let Some(sql) = defaults::default_sql(
                                    state,
                                    &table.columns[super::super::super::col(table, name)?],
                                ) {
                                    out.extend(defaults::dependencies(&sql)?.iter().cloned().map(Some));
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(out)
    }
    pub(super) fn write_tables(&self) -> Vec<(String, &'static str)> {
        let mut commands = self
            .clauses
            .iter()
            .filter_map(|w| w.action.command())
            .collect::<BTreeSet<_>>();
        // DO NOTHING still acquires the target's writer lock and is a DML statement.
        if commands.is_empty() {
            commands.insert("MERGE");
        }
        commands
            .into_iter()
            .map(|c| (self.input.target.clone(), c))
            .collect()
    }
    pub(super) fn collect_update_columns<'a>(&'a self, table: &str, columns: &mut BTreeSet<&'a str>) {
        if self.input.target == table {
            for clause in &self.clauses {
                if let Action::Update(groups) = &clause.action {
                    columns.extend(groups.iter().flat_map(|(names, _)| names.iter().map(String::as_str)));
                }
            }
        }
    }

    pub(super) fn remap(&mut self, alias: &str, name: &str) {
        self.input.remap(alias, name);
        self.map_plans(|p| {
            cte::remap(p, alias, name);
            Ok(())
        })
        .expect("infallible remapping");
    }
    pub(super) fn bind_schema_names(
        &mut self,
        bind: &impl Fn(&mut String),
        state: &Relational,
        path: &[String],
    ) -> R<()> {
        self.input.bind_schema_names(bind, state, path)?;
        self.map_plans(|p| p.bind_schema_names(bind, state, path))
    }
    fn shape(&self, state: &Relational, parameters: &mut [Option<DataType>]) -> R<join::Shape> {
        self.input
            .from
            .clone()
            .resolve(&self.input.catalog(state)?, parameters)
    }
    fn action_shape(&self, shape: &join::Shape, kind: Kind) -> join::Shape {
        let mut out = shape.clone();
        if kind != Kind::Matched {
            let allowed = shape
                .namespace
                .qualified
                .iter()
                .filter(|(a, _)| (a == &self.input.alias) == (kind == Kind::Source))
                .cloned()
                .collect::<Vec<_>>();
            let indices = allowed
                .iter()
                .flat_map(|(_, cols)| cols.iter().map(|(_, i)| *i))
                .collect::<BTreeSet<_>>();
            out.namespace.visible.retain(|(_, i)| indices.contains(i));
            out.namespace.hidden.retain(|(_, i)| indices.contains(i));
            out.namespace.qualified = allowed;
        }
        out
    }
    fn returning_shape(&self, shape: &join::Shape, table: &TableDef) -> join::Shape {
        let mut out = self.input.returning_shape(shape, table);
        out.namespace
            .hidden
            .push((self.action_name.clone(), out.columns.len()));
        out.columns.push(ColumnDef {
            id: 0,
            name: self.action_name.clone(),
            data_type: DataType::Text,
            not_null: false,
        });
        out.typmods.push(-1);
        out.collations.push(None);
        out
    }
    fn returning(&self, shape: &join::Shape) -> Option<Plan> {
        let mut plan = self.input.returning.clone()?;
        // Expand stars before correlated substitution; synthetic keys never escape.
        let image_records = shape
            .namespace
            .visible
            .iter()
            .rev()
            .take(2)
            .map(|(_, i)| *i)
            .collect::<BTreeSet<_>>();
        if let Body::Select(s) = &mut plan.body {
            let mut items = vec![];
            for (item, alias) in &s.query.items {
                let columns = match item {
                    Item::Star => Some(&shape.namespace.visible),
                    Item::QualifiedStar { relation, .. } => shape
                        .namespace
                        .qualified
                        .iter()
                        .find(|(a, _)| a == relation)
                        .map(|(_, c)| c),
                    _ => None,
                };
                if let Some(cols) = columns {
                    for (name, i) in cols {
                        if !name.starts_with('\0') && !image_records.contains(i) {
                            items.push((
                                Item::Scalar(Scalar::Column(ColumnRef {
                                    qualifier: None,
                                    name: format!("\0join_column_{i}"),
                                })),
                                Some(name.clone()),
                            ));
                        }
                    }
                } else {
                    items.push((item.clone(), alias.clone()));
                }
            }
            s.query.items = items;
        }
        Some(plan)
    }
    pub(super) fn resolve(
        &mut self,
        state: &Relational,
        parameters: &mut [Option<DataType>],
    ) -> R<bind::Meta> {
        let shape = self.shape(state, parameters)?;
        let table = &state.tables[&self.input.target];
        for w in &self.clauses {
            let action_shape = self.action_shape(&shape, w.kind);
            if let Some(p) = &w.condition {
                let (mut p, _) = subquery::instantiate(p, &action_shape, None, state, parameters)?;
                p.resolve(state, parameters)?;
            }
            match &w.action {
                Action::Update(g) => {
                    let mut m = self.input.clone();
                    m.groups = g.clone();
                    m.returning = None;
                    m.resolve_shape(state, parameters, &action_shape)?;
                }
                Action::Insert {
                    columns,
                    values,
                    overriding,
                } => {
                    let template = sql_insert::Insert::merge_row(
                        self.input.target.clone(),
                        columns.clone(),
                        values
                            .iter()
                            .map(|v| v.as_ref().map(|_| Value::Null))
                            .collect(),
                        *overriding,
                    );
                    template.describe(state, 0, &[])?;
                    let names = if columns.is_empty() {
                        table
                            .logical_columns()
                            .map(|c| c.name.clone())
                            .take(values.len())
                            .collect::<Vec<_>>()
                    } else {
                        columns.clone()
                    };
                    for (name, p) in names.iter().zip(values) {
                        if let Some(p) = p {
                            if overriding.is_none() {
                                defaults::check_explicit(
                                    state,
                                    &table.columns[super::super::super::col(table, name)?],
                                )?;
                            }
                            check_value(p, name, &action_shape, state, table, parameters)?;
                        }
                    }
                }
                _ => {}
            }
        }
        let returning_shape = self.returning_shape(&shape, table);
        if let Some(p) = self.returning(&returning_shape) {
            let (mut p, _) = subquery::instantiate(&p, &returning_shape, None, state, parameters)?;
            let mut meta = p.resolve(state, parameters)?;
            if let Some(original) = &self.input.returning
                && let Body::Select(s) = &original.body
            {
                meta.type_oids.resize(meta.columns.len(), 0);
                for (oid, (item, _)) in meta.type_oids.iter_mut().zip(&s.query.items) {
                    if matches!(item,Item::Scalar(Scalar::Column(c)) if c.qualifier.is_none() && self.input.images.contains(&c.name))
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
    fn paged_eligible(&self, state: &Relational) -> bool {
        state.tables.contains_key(&self.input.target)
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
        let mut snapshot = state.clone();
        let mut retained = scan::Retention::new(scan::WRITE_SOURCE_BYTES);
        row_sink::freeze_tables(&mut snapshot, self.read_tables(), sink,
            &mut |bytes| retained.charge(bytes))?;
        let mut work = row_sink::paged_dml::ActionWork::new(deferred.as_deref().cloned().unwrap_or_default())?;
        work.track_inserts();
        let read_state = state.clone();
        let result = self.paged_actions(state, &read_state, prepared, params, clock, sink,
            version_end, &mut work, &scan::LegacyRows(&snapshot), false)?;
        work.complete(state, sink, clock, version_end, deferred)?;
        Ok(result)
    }

    #[expect(clippy::too_many_arguments, reason = "paged MERGE with a shared mutation queue")]
    pub(in crate::relational) fn paged_actions(
        &self, state: &mut Relational, read_state: &Relational, prepared: &Prepared, params: &[Value],
        clock: Option<&super::super::super::clock::ExecutionClock>, sink: &mut impl row_sink::RowSink,
        version_end: u64, work: &mut row_sink::paged_dml::ActionWork,
        source: &dyn scan::RowSource, sibling_writes: bool,
    ) -> Result<SqlResult, crate::CoreError> {
        let descriptors = sink.indexes();
        let mut output = spill::RowBuffer::new(Vec::new())?;
        let mut source_error = None;
        let mut output_error = None;
        let mut publish = |point: &Prepared, table: &TableDef, old: Option<(&[u8], &SqlRow)>| {
            row_sink::publish_plan_point(state, sink, work, point, table, old,
                clock, &descriptors, version_end, sibling_writes).map_err(|error| {
                source_error = Some(error);
                SqlError::Aborted
            })
        };
        let mut emit = |row| output.push(row).map_err(|error| {
            output_error = Some(error);
            SqlError::Aborted
        });
        let mut read_state = read_state.clone();
        let result = scan::with_cursor_limit(scan::WRITE_SOURCE_BYTES, || self.execute_inner(
            &mut read_state,
            prepared,
            params,
            clock,
            &mut || crate::cancellation::check().is_err(),
            &mut vec![],
            Some(source),
            Some(&mut publish),
            Some(&mut emit),
        ));
        if let Some(error) = source_error.or(output_error) {
            return Err(error);
        }
        let mut result = result.map_err(crate::CoreError::Sql)?;
        output.finish(&mut result)?;
        Ok(result)
    }
    #[expect(
        clippy::too_many_arguments,
        reason = "shared mutation execution context"
    )]
    pub(super) fn execute(
        &self,
        state: &mut Relational,
        prepared: &Prepared,
        params: &[Value],
        clock: Option<&super::super::super::clock::ExecutionClock>,
        cancel: &mut dyn FnMut() -> bool,
        events: &mut Vec<super::super::super::constraints::Event>,
        injected: Option<&dyn scan::RowSource>,
    ) -> R<SqlResult> {
        self.execute_inner(
            state, prepared, params, clock, cancel, events, injected, None, None,
        )
    }
    #[expect(
        clippy::too_many_arguments,
        reason = "shared mutation execution context"
    )]
    fn execute_inner(
        &self,
        state: &mut Relational,
        prepared: &Prepared,
        params: &[Value],
        clock: Option<&super::super::super::clock::ExecutionClock>,
        cancel: &mut dyn FnMut() -> bool,
        events: &mut Vec<super::super::super::constraints::Event>,
        injected: Option<&dyn scan::RowSource>,
        mut publish: Option<&mut Publisher<'_>>,
        mut emit: Option<&mut ReturningSink<'_>>,
    ) -> R<SqlResult> {
        let _metadata = functions::collation::ColumnScope::enter(&[], &[])?;
        let description = prepared.describe(state)?;
        let parameters = description
            .parameters
            .into_iter()
            .map(Some)
            .collect::<Vec<_>>();
        let snapshot = state.clone();
        let table = &snapshot.tables[&self.input.target];
        let shape = self.shape(&snapshot, &mut parameters.clone())?;
        let catalog = self.input.catalog(&snapshot)?;
        let legacy = scan::LegacyRows(&snapshot);
        let base = injected.unwrap_or(&legacy);
        let source = mutation::Keys {
            base,
            target: table.id,
        };
        let execution = Execution::new(cancel, &parameters);
        let joined;
        let mut candidates;
        let rows: spill::StoreIter<'_, SqlRow> = if publish.is_some() {
            // Resolve all join candidates before publishing any writes. Spool
            // them in join order so the join inputs and hash need no space
            // for a second full copy of the candidate rows.
            candidates = spill::Store::new();
            let mut spool = |row| {
                let bytes = super::super::super::searched::retained_row_bytes(&[], &row)?;
                candidates.push(row, bytes)
            };
            self.input.from.run_with_output(&catalog, &source, params, &parameters,
                clock, &execution, Some(&mut spool))?;
            candidates.finish()?;
            candidates.iter()?
        } else {
            joined = self.input.from.run(&catalog, &source, params, &parameters, clock, &execution)?;
            Box::new(joined.rows.iter().map(|row| Ok(Cow::Borrowed(row))))
        };
        let key_index = shape.namespace.resolve(&ColumnRef {
            qualifier: Some(self.input.alias.clone()),
            name: mutation::KEY.into(),
        })?;
        let presence = shape.namespace.resolve(&ColumnRef {
            qualifier: Some(PRESENCE.into()),
            name: PRESENCE.into(),
        })?;
        let returning_shape = self.returning_shape(&shape, table);
        let returning = self.returning(&returning_shape);
        let retained = RetentionScope::new(&execution);
        let mut seen = BTreeSet::new();
        let mut bytes = 0;
        let mut result = SqlResult {
            columns: description.columns.into_iter().map(|c| c.name).collect(),
            rows: vec![],
            affected: 0,
        };
        for matched in rows {
            let matched = &*matched?;
            if execution.cancelled() {
                return Err(SqlError::Aborted);
            }
            let key = match &matched.values[key_index] {
                Value::Bytes(k) => Some(k),
                Value::Null => None,
                _ => return Err(SqlError::Type),
            };
            let kind = if key.is_none() {
                Kind::Target
            } else if matched.values[presence] == Value::Null {
                Kind::Source
            } else {
                Kind::Matched
            };
            let action_shape = self.action_shape(&shape, kind);
            let mut selected = None;
            for w in self.clauses.iter().filter(|w| w.kind == kind) {
                if let Some(p) = &w.condition {
                    let (p, _) = subquery::instantiate(
                        p,
                        &action_shape,
                        Some(matched),
                        &snapshot,
                        &parameters,
                    )?;
                    if p.run(&snapshot, base, params, &parameters, clock, &execution)?
                        .rows
                        .is_empty()
                    {
                        continue;
                    }
                }
                selected = Some(&w.action);
                break;
            }
            let Some(action) = selected else {
                continue;
            };
            let Some(command) = action.command() else {
                continue;
            };
            if let Some(key) = key {
                if !seen.insert(key.clone()) {
                    return Err(SqlError::CardinalityViolation);
                }
                retained
                    .borrow()
                    .charge(key.len() + std::mem::size_of::<Vec<u8>>())?;
            }
            let input_row;
            let old = if publish.is_some() {
                input_row = key.map(|key| base.get(table, key)).transpose()?.flatten();
                input_row.as_ref().map(|row| row.row.as_ref())
            } else {
                key.and_then(|key| snapshot.rows[&table.id].get(key))
            };
            let mut new = old.cloned();
            let statement = match action {
                Action::Update(groups) => {
                    let old = old.ok_or(SqlError::UnknownColumn)?;
                    let row = new.as_mut().ok_or(SqlError::UnknownColumn)?;
                    let mut sets = vec![];
                    for (names, p) in groups {
                        let values = if let Some(p) = p {
                            evaluate(
                                p,
                                names,
                                &action_shape,
                                matched,
                                &snapshot,
                                table,
                                base,
                                params,
                                &parameters,
                                clock,
                                &execution,
                            )?
                        } else {
                            names
                                .iter()
                                .map(|name| {
                                    defaults::evaluate(
                                        state,
                                        &table.columns[super::super::super::col(table, name)?],
                                        clock,
                                    )
                                })
                                .collect::<R<Vec<_>>>()?
                        };
                        for (name, value) in names.iter().zip(values) {
                            let i = super::super::super::col(table, name)?;
                            let value = super::super::super::coerce_to_column(
                                state,
                                &table.columns[i],
                                value,
                            )?;
                            row.values[i] = value.clone();
                            sets.push((name.clone(), Expr::Literal(value)));
                        }
                    }
                    Statement::Update {
                        name: self.input.target.clone(),
                        sets,
                        predicates: predicates(table, old),
                        search: None,
                        returning: None,
                    }
                }
                Action::Delete => Statement::Delete {
                    name: self.input.target.clone(),
                    predicates: predicates(table, old.ok_or(SqlError::UnknownColumn)?),
                    search: None,
                    returning: None,
                },
                Action::Insert {
                    columns,
                    values,
                    overriding,
                } => {
                    let names = if columns.is_empty() {
                        table
                            .logical_columns()
                            .take(values.len())
                            .map(|c| c.name.clone())
                            .collect::<Vec<_>>()
                    } else {
                        columns.clone()
                    };
                    let values = names
                        .iter()
                        .zip(values)
                        .map(|(name, p)| {
                            if *overriding == Some(false)
                                && state
                                    .column_identity(
                                        table.columns[super::super::super::col(table, name)?].id,
                                    )
                                    .is_some()
                            {
                                return Ok(None);
                            }
                            p.as_ref()
                                .map(|p| {
                                    evaluate(
                                        p,
                                        std::slice::from_ref(name),
                                        &action_shape,
                                        matched,
                                        &snapshot,
                                        table,
                                        base,
                                        params,
                                        &parameters,
                                        clock,
                                        &execution,
                                    )
                                    .map(|v| v[0].clone())
                                })
                                .transpose()
                        })
                        .collect::<R<Vec<_>>>()?;
                    let mut insert = sql_insert::Insert::merge_row(
                        self.input.target.clone(),
                        columns.clone(),
                        values,
                        *overriding,
                    );
                    insert.returning = super::super::super::returning::Returning::parse(
                        &mut parse::parser(&[Token::Word("returning".into()), Token::Symbol('*')]),
                        &self.input.target,
                    )?;
                    Statement::InsertMulti(Box::new(insert))
                }
                Action::Nothing => unreachable!(),
            };
            let mut point = Prepared::engine(statement, prepared.domain, state.epoch);
            point.search_path = prepared.search_path.clone();
            let output = if let Some(publish) = publish.as_mut() {
                publish(&point, table, key.zip(old).map(|(key, row)| (key.as_slice(), row)))?
            } else {
                state.execute_statement_inner(
                    &point,
                    &[],
                    None,
                    || execution.cancelled(),
                    clock,
                    None,
                    events,
                )?
            };
            if publish.is_some() && output.affected == 0 {
                return Err(SqlError::CardinalityViolation);
            }
            if command == "INSERT" {
                new = output.rows.first().map(|row| {
                    let mut physical = SqlRow {
                        values: vec![Value::Null; table.columns.len()],
                        version: row.version,
                    };
                    for (i, value) in table.logical_offsets().into_iter().zip(&row.values) {
                        physical.values[i] = value.clone();
                    }
                    physical
                });
            } else if command == "DELETE" {
                new = None;
            }
            result.affected += output.affected;
            if let Some(returning) = &returning {
                let mut input = matched.clone();
                let current = new.as_ref().or(old);
                for (name, index) in &shape
                    .namespace
                    .qualified
                    .iter()
                    .find(|(a, _)| a == &self.input.alias)
                    .ok_or(SqlError::UnknownTable)?
                    .1
                {
                    if let Ok(i) = super::super::super::col(table, name) {
                        input.values[*index] = current.map_or(Value::Null, |r| r.values[i].clone());
                    }
                }
                for image in [old, new.as_ref()] {
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
                input.values.push(Value::Text(command.into()));
                let (p, _) = subquery::instantiate(
                    returning,
                    &returning_shape,
                    Some(&input),
                    &snapshot,
                    &parameters,
                )?;
                for row in p
                    .run(&snapshot, base, params, &parameters, clock, &execution)?
                    .rows
                {
                    if let Some(emit) = emit.as_mut() {
                        emit(row)?;
                        continue;
                    }
                    if result.rows.len() >= scan::query_row_limit() {
                        return Err(SqlError::Limit);
                    }
                    let mut retained_bytes = std::mem::size_of::<SqlRow>();
                    for v in &row.values {
                        charge(&mut bytes, v)?;
                        retained_bytes += std::mem::size_of::<Value>() + value_bytes(v);
                    }
                    retained.borrow().charge(retained_bytes)?;
                    result.rows.push(row);
                }
            }
        }
        Ok(result)
    }
}
impl Plan {
    pub(in crate::relational) fn paged_merge(&self, state: &Relational) -> Option<&Merge> {
        self.merge
            .as_deref()
            .filter(|m| self.ctes.is_empty() && m.paged_eligible(state))
    }
    pub(in crate::relational) fn implicit_sequences(
        &self,
        state: &Relational,
        count: usize,
        declared: &[Option<DataType>],
    ) -> R<Vec<Option<String>>> {
        let mut out = self
            .merge
            .as_ref()
            .map(|m| m.implicit_sequences(state))
            .transpose()?
            .unwrap_or_default();
        if let Some(insert) = &self.insert {
            let mut parameters = vec![None; count];
            parameters.get_mut(..declared.len()).ok_or(SqlError::Parameters)?
                .clone_from_slice(declared);
            let catalog = cte::resolve(&mut self.ctes.clone(), state, &mut parameters)?;
            let mut prepared = insert.prepared.clone();
            prepared.parameter_types = parameters;
            out.extend(prepared.implicit_sequences(&catalog)?);
        }
        for cte in &self.ctes {
            // Other DML keeps its existing dependency path; MERGE's literal
            // templates need no virtual CTE catalog to discover defaults.
            if let Some(p) = &cte.dml {
                if let Statement::Plan(plan) = p.statement.get() {
                    out.extend(plan.implicit_sequences(state, count, declared)?);
                }
            } else {
                out.extend(cte.query.implicit_sequences(state, count, declared)?);
            }
        }
        Ok(out)
    }
}
fn predicates(table: &TableDef, row: &SqlRow) -> Vec<Predicate> {
    table
        .primary_key
        .iter()
        .map(|i| Predicate {
            column: table.columns[*i].name.clone(),
            op: "=".into(),
            value: Expr::Literal(row.values[*i].clone()),
        })
        .collect()
}
fn check_value(
    p: &Plan,
    name: &str,
    shape: &join::Shape,
    state: &Relational,
    table: &TableDef,
    parameters: &mut [Option<DataType>],
) -> R<()> {
    // INSERT's identity rules were already checked by the INSERT template.
    let (mut q, _) = subquery::instantiate(p, shape, None, state, parameters)?;
    if let Body::Select(s) = &mut q.body {
        for (item, _) in &s.query.items {
            if let Item::Scalar(Scalar::Param(n)) = item {
                parameters[*n - 1].get_or_insert(
                    table.columns[super::super::super::col(table, name)?]
                        .data_type
                        .clone(),
                );
            }
        }
    }
    let meta = q.resolve(state, parameters)?;
    if meta.columns.len() != 1 {
        return Err(SqlError::Syntax);
    }
    if meta.unknown[0].is_none()
        && !assignment::compatible(
            &meta.columns[0].data_type,
            &table.columns[super::super::super::col(table, name)?].data_type,
        )
    {
        return Err(SqlError::Type);
    }
    Ok(())
}
#[expect(clippy::too_many_arguments, reason = "snapshot assignment evaluation")]
fn evaluate(
    p: &Plan,
    names: &[String],
    shape: &join::Shape,
    row: &SqlRow,
    state: &Relational,
    table: &TableDef,
    source: &dyn scan::RowSource,
    params: &[Value],
    parameters: &[Option<DataType>],
    clock: Option<&super::super::super::clock::ExecutionClock>,
    execution: &Execution<'_>,
) -> R<Vec<Value>> {
    let (mut p, _) = subquery::instantiate(p, shape, Some(row), state, parameters)?;
    let meta = p.resolve(state, &mut parameters.to_vec())?;
    let rows = p.run(state, source, params, parameters, clock, execution)?;
    if rows.rows.len() > 1 {
        return Err(SqlError::CardinalityViolation);
    }
    let Some(row) = rows.rows.first() else {
        return Ok(vec![Value::Null; names.len()]);
    };
    row.values
        .iter()
        .zip(&meta.columns)
        .zip(names)
        .enumerate()
        .map(|(i, ((value, c), name))| {
            let target = &table.columns[super::super::super::col(table, name)?].data_type;
            if meta.unknown[i].is_some() {
                assignment::literal(value.clone(), target, clock)
            } else {
                assignment::convert(value.clone(), &c.data_type, target, clock)
            }
        })
        .collect()
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
                .join(format!("plan-merge-{:032x}", u128::from_be_bytes(nonce)));
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
        s.execute_at(
            sql,
            &[],
            None,
            &crate::relational::StatementClock {
                unavailable: None,
                transaction_us: 1,
                statement_us: 1,
                zone: None,
                wall: None,
            },
        )
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
    fn ordered_actions_images_and_command() {
        let (_f, mut s) = seeded();
        let sql = "MERGE INTO m t USING (VALUES(1,11),(3,30)) s(id,v) ON t.id=s.id WHEN MATCHED AND s.v<0 THEN DELETE WHEN MATCHED THEN UPDATE SET a=s.v WHEN NOT MATCHED BY TARGET THEN INSERT(id,a) VALUES(s.id,s.v) WHEN NOT MATCHED BY SOURCE AND t.id=2 THEN DELETE RETURNING WITH(OLD AS o,NEW AS n) merge_action(),t.id,s.v,o.a,n.a";
        let p = Prepared::parse(sql, 1, 0).unwrap();
        assert_eq!(p.command(), "MERGE");
        assert_eq!(p.task_class(), crate::task::StatementClass::Other);
        assert!(!p.is_read());
        assert_eq!(p.v2_class(), V2Class::Dml);
        assert_eq!(p.write_tables().len(), 3);
        let r = run(&mut s, sql);
        assert_eq!(r.affected, 3);
        assert_eq!(r.columns, vec!["merge_action", "id", "v", "a", "a"]);
        assert!(r.rows.iter().any(|r| r.values
            == vec![
                Value::Text("UPDATE".into()),
                Value::Int(1),
                Value::Int(11),
                Value::Int(10),
                Value::Int(11)
            ]));
        assert!(r.rows.iter().any(|r| r.values
            == vec![
                Value::Text("INSERT".into()),
                Value::Int(3),
                Value::Int(30),
                Value::Null,
                Value::Int(30)
            ]));
        assert!(r.rows.iter().any(|r| r.values
            == vec![
                Value::Text("DELETE".into()),
                Value::Int(2),
                Value::Null,
                Value::Int(20),
                Value::Null
            ]));
        assert_eq!(
            values(&mut s, "SELECT id,a FROM m ORDER BY id"),
            vec![
                vec![Value::Int(1), Value::Int(11)],
                vec![Value::Int(3), Value::Int(30)]
            ]
        );
    }
    #[test]
    fn nothing_and_by_source_update_and_defaults() {
        let (_f, mut s) = seeded();
        assert_eq!(run(&mut s,"MERGE INTO m t USING (VALUES(1),(3)) s(id) ON t.id=s.id WHEN MATCHED THEN DO NOTHING WHEN NOT MATCHED THEN DO NOTHING WHEN NOT MATCHED BY SOURCE THEN DO NOTHING").affected,0);
        assert_eq!(run(&mut s,"MERGE INTO m t USING (VALUES(1),(3)) s(id) ON t.id=s.id WHEN MATCHED AND t.a=10 THEN DELETE WHEN NOT MATCHED BY SOURCE THEN UPDATE SET (a,b)=(21,DEFAULT) WHEN NOT MATCHED THEN INSERT(id,a,b) VALUES(s.id,DEFAULT,DEFAULT)").affected,3);
        assert_eq!(
            values(&mut s, "SELECT id,a,b FROM m ORDER BY id"),
            vec![
                vec![Value::Int(2), Value::Int(21), Value::Text("z".into())],
                vec![Value::Int(3), Value::Null, Value::Text("z".into())]
            ]
        );
    }
    #[test]
    fn cardinality_and_forbidden_source_references() {
        let (_f, mut s) = seeded();
        for (sql, code) in [
            (
                "MERGE INTO m t USING(VALUES(1,1),(1,2)) s(id,v) ON t.id=s.id WHEN MATCHED THEN UPDATE SET a=s.v",
                "21000",
            ),
            (
                "MERGE INTO m t USING(VALUES(99)) s(id) ON t.id=s.id WHEN NOT MATCHED BY SOURCE THEN UPDATE SET a=s.id",
                "42P01",
            ),
            (
                "MERGE INTO m t USING(VALUES(99)) s(id) ON t.id=s.id WHEN NOT MATCHED BY SOURCE AND s.id=99 THEN DELETE",
                "42P01",
            ),
            ("SELECT merge_action()", "42601"),
            ("WITH q AS(SELECT merge_action()) SELECT * FROM q", "42601"),
            (
                "MERGE INTO m t USING(VALUES(1)) s(id) ON merge_action()='UPDATE' WHEN MATCHED THEN DELETE",
                "42601",
            ),
            (
                "MERGE INTO m t USING(VALUES(1)) s(id) ON t.id=s.id WHEN MATCHED THEN UPDATE SET b=merge_action()",
                "42601",
            ),
            ("UPDATE m SET b=merge_action()", "42601"),
            (
                "MERGE INTO m t USING(VALUES(99)) s(id) ON t.id=s.id WHEN MATCHED THEN DO NOTHING WHEN MATCHED THEN DELETE",
                "42601",
            ),
        ] {
            let e = s.execute(sql, &[], None).unwrap_err();
            assert_eq!(crate::sqlstate(&e), code, "{sql}: {e:?}");
        }
        assert_eq!(
            values(&mut s, "SELECT a FROM m ORDER BY id"),
            vec![vec![Value::Int(10)], vec![Value::Int(20)]]
        );
    }
    #[test]
    fn identity_overriding_and_default_values() {
        let f = Fixture::new();
        let mut s = f.session();
        run(
            &mut s,
            "CREATE TABLE ident(id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,b text DEFAULT 'draft')",
        );
        assert_eq!(
            values(
                &mut s,
                "MERGE INTO ident t USING(VALUES(1)) s(id) ON t.id=s.id WHEN NOT MATCHED THEN INSERT DEFAULT VALUES RETURNING t.id,t.b"
            ),
            vec![vec![Value::Int(1), Value::Text("draft".into())]]
        );
        assert_eq!(
            values(
                &mut s,
                "MERGE INTO ident t USING(VALUES(50)) s(id) ON t.id=s.id WHEN NOT MATCHED THEN INSERT(id) OVERRIDING SYSTEM VALUE VALUES(s.id) RETURNING t.id"
            ),
            vec![vec![Value::Int(50)]]
        );
        assert_eq!(
            values(
                &mut s,
                "MERGE INTO ident t USING(VALUES(60)) s(id) ON t.id=s.id WHEN NOT MATCHED THEN INSERT(id) OVERRIDING USER VALUE VALUES(s.id) RETURNING t.id"
            ),
            vec![vec![Value::Int(2)]]
        );
        let e=s.execute("MERGE INTO ident t USING(VALUES(70)) s(id) ON t.id=s.id WHEN NOT MATCHED THEN INSERT(id) VALUES(s.id)",&[],None).unwrap_err();
        assert_eq!(crate::sqlstate(&e), "428C9");
    }
    #[test]
    fn source_forms_self_join_cte_and_parameters() {
        let (_f, mut s) = seeded();
        run(&mut s, "CREATE TABLE src(id int PRIMARY KEY,v int)");
        run(&mut s, "INSERT INTO src VALUES(1,55)");
        for source in [
            "src s",
            "(SELECT * FROM src) s",
            "src s JOIN(VALUES(1)) j(k) ON s.id=j.k",
        ] {
            assert_eq!(run(&mut s,&format!("MERGE INTO m t USING {source} ON t.id=s.id WHEN MATCHED THEN UPDATE SET a=s.v")).affected,1);
        }
        assert_eq!(
            run(
                &mut s,
                "MERGE INTO m t USING m s ON t.id=s.id WHEN MATCHED THEN UPDATE SET a=s.a+1"
            )
            .affected,
            2
        );
        let p = Prepared::parse(
            "MERGE INTO m t USING src s ON t.id=s.id WHEN MATCHED THEN UPDATE SET a=s.v",
            1,
            0,
        )
        .unwrap();
        assert!(p.read_tables().contains(&"src".into()));
        assert!(p.authorization_tables().unwrap().contains(&"src".into()));
        assert_eq!(
            values(
                &mut s,
                "WITH source AS(SELECT * FROM src), changes AS(MERGE INTO m t USING source s ON t.id=s.id WHEN MATCHED THEN UPDATE SET a=s.v RETURNING merge_action() AS action,t.id) SELECT * FROM changes"
            ),
            vec![vec![Value::Text("UPDATE".into()), Value::Int(1)]]
        );
        let r=s.execute("WITH changes AS(MERGE INTO m t USING(VALUES($1::int)) s(id) ON t.id=s.id WHEN MATCHED THEN UPDATE SET a=$2 RETURNING new.a) SELECT * FROM changes",&[Value::Int(1),Value::Int(77)],None).unwrap();
        assert_eq!(r.rows[0].values, vec![Value::Int(77)]);
        assert_eq!(run(&mut s,"WITH src AS(VALUES(3,33)) MERGE INTO m t USING src s(id,v) ON t.id=s.id WHEN NOT MATCHED THEN INSERT(id,a) VALUES(s.id,s.v)").affected,1);
    }
    #[test]
    fn cascade_and_heap_keys() {
        let (_f, mut s) = seeded();
        run(
            &mut s,
            "CREATE TABLE child(id int PRIMARY KEY,p int REFERENCES m(id) ON DELETE CASCADE)",
        );
        run(&mut s, "INSERT INTO child VALUES(1,1),(2,2)");
        assert_eq!(
            run(
                &mut s,
                "MERGE INTO m t USING(VALUES(1)) s(id) ON t.id=s.id WHEN MATCHED THEN DELETE"
            )
            .affected,
            1
        );
        assert_eq!(
            values(&mut s, "SELECT id FROM child"),
            vec![vec![Value::Int(2)]]
        );
        run(&mut s, "CREATE TABLE heap(a int,b text)");
        run(&mut s, "INSERT INTO heap VALUES(1,'x'),(1,'y')");
        assert_eq!(run(&mut s,"MERGE INTO heap t USING(VALUES(1)) s(id) ON t.a=s.id WHEN MATCHED THEN UPDATE SET b='z'").affected,2);
        assert_eq!(
            run(
                &mut s,
                "MERGE INTO heap t USING(VALUES(1)) s(id) ON t.a=s.id WHEN MATCHED THEN DELETE"
            )
            .affected,
            2
        );
    }
    #[test]
    fn paged_target_and_insert_returning_stars() {
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
        assert_eq!(run(&mut s,"MERGE INTO big t USING(VALUES(1)) s(v) ON true WHEN MATCHED THEN UPDATE SET a=t.a+s.v").affected,1100);
        assert_eq!(
            values(&mut s, "SELECT count(*) FROM big WHERE a>1"),
            vec![vec![Value::Int(1100)]]
        );
        assert_eq!(
            run(
                &mut s,
                "MERGE INTO big t USING(VALUES(1)) s(v) ON true WHEN MATCHED THEN DELETE"
            )
            .affected,
            1100
        );
        assert_eq!(
            values(
                &mut s,
                "MERGE INTO big t USING(VALUES(3,30)) s(id,v) ON t.id=s.id WHEN NOT MATCHED THEN INSERT VALUES(s.id,s.v) RETURNING t.*"
            ),
            vec![vec![Value::Int(3), Value::Int(30)]]
        );
        run(&mut s, "CREATE TABLE heap_insert(a int)");
        assert_eq!(
            values(
                &mut s,
                "MERGE INTO heap_insert t USING(VALUES(4)) s(a) ON t.a=s.a WHEN NOT MATCHED THEN INSERT VALUES(s.a) RETURNING old.a,new.a,t.a"
            ),
            vec![vec![Value::Null, Value::Int(4), Value::Int(4)]]
        );
    }
    #[test]
    fn stars_preserve_columns_named_like_images() {
        let f = Fixture::new();
        let mut s = f.session();
        run(&mut s, "CREATE TABLE named(old int,new int)");
        assert_eq!(
            values(
                &mut s,
                "MERGE INTO named t USING(VALUES(4,5)) s(a,b) ON false WHEN NOT MATCHED THEN INSERT VALUES(s.a,s.b) RETURNING t.*,old.*,new.*"
            ),
            vec![vec![
                Value::Int(4),
                Value::Int(5),
                Value::Null,
                Value::Null,
                Value::Int(4),
                Value::Int(5)
            ]]
        );
    }
    #[test]
    fn case_expressions_do_not_split_merge_clauses() {
        let (_f, mut s) = seeded();
        assert_eq!(
            values(
                &mut s,
                "MERGE INTO m t USING(VALUES(1,5)) s(id,v) ON CASE WHEN t.id=s.id THEN true ELSE false END WHEN MATCHED AND CASE WHEN s.v>0 THEN true ELSE false END THEN UPDATE SET a=CASE WHEN s.v=5 THEN 7 ELSE 8 END RETURNING a"
            ),
            vec![vec![Value::Int(7)]]
        );
    }
    #[test]
    fn target_lock_matches_update() {
        let (f, mut a) = seeded();
        let mut b = f.session();
        run(&mut a, "BEGIN");
        run(
            &mut a,
            "MERGE INTO m t USING(VALUES(1)) s(id) ON t.id=s.id WHEN MATCHED THEN UPDATE SET a=90",
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

impl Merge {
    pub(super) fn has_privilege_inquiry(&self) -> bool {
        self.input.has_privilege_inquiry() || self.plans().any(Plan::has_privilege_inquiry)
    }
}
