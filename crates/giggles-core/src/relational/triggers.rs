//! Trigger definitions, dependency maintenance and the firing worker's lookup.
use super::*;
use routines::{Name, RoutineDiagnostic, error, finish, quote, spans, starts};

pub(crate) const INSERT: u8 = 4;
pub(crate) const DELETE: u8 = 8;
pub(crate) const UPDATE: u8 = 16;
const TRUNCATE: u8 = 32;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Trigger {
    pub id: u64,
    pub table: u64,
    pub name: String,
    pub routine: u64,
    pub events: u8,
    pub before: bool,
    pub row: bool,
    pub enabled: u8,
    pub update_columns: Vec<u64>,
    pub args: Vec<String>,
    pub when: Option<String>,
    pub when_columns: Vec<u64>,
}

#[derive(Clone, Debug)]
pub(super) enum Ddl {
    Create(Box<Create>),
    Drop {
        table: String,
        name: String,
        optional: bool,
    },
    Rename {
        table: String,
        name: String,
        new: String,
    },
    Unsupported {
        what: &'static str,
        table: String,
    },
}
#[derive(Clone, Debug)]
pub(super) struct Create {
    table: String,
    trigger: Trigger,
    replace: bool,
    routine: Name,
    update_columns: Vec<String>,
    when_tokens: Vec<Token>,
}
#[derive(Clone, Debug)]
pub(super) enum Target {
    All,
    Name(String),
}

impl Ddl {
    pub(super) fn table(&self) -> &str {
        match self {
            Self::Create(c) => &c.table,
            Self::Drop { table, .. }
            | Self::Rename { table, .. }
            | Self::Unsupported { table, .. } => table,
        }
    }
    pub(super) fn bind_table(&mut self, resolve: impl FnOnce(&mut String)) {
        match self {
            Self::Create(c) => resolve(&mut c.table),
            Self::Drop { table, .. }
            | Self::Rename { table, .. }
            | Self::Unsupported { table, .. } => resolve(table),
        }
    }
    pub(super) fn command(&self) -> &'static str {
        match self {
            Self::Create(_) | Self::Unsupported { .. } => "CREATE TRIGGER",
            Self::Drop { .. } => "DROP TRIGGER",
            Self::Rename { .. } => "ALTER TRIGGER",
        }
    }
}

pub(super) fn claims(tokens: &[Token]) -> bool {
    [
        ["create", "trigger"].as_slice(),
        &["create", "constraint", "trigger"],
        &["create", "or", "replace", "trigger"],
        &["create", "or", "replace", "constraint", "trigger"],
        &["drop", "trigger"],
        &["alter", "trigger"],
    ]
    .into_iter()
    .any(|words| starts(tokens, words))
}
pub(super) fn parse(p: &mut Parser, source: &str) -> R<Ddl> {
    if p.word("drop") {
        p.need("trigger")?;
        let optional = p.word("if");
        if optional {
            p.need("exists")?;
        }
        let name = p.name()?;
        p.need("on")?;
        let table = p.table_name()?;
        if !p.word("cascade") {
            p.word("restrict");
        }
        finish(p)?;
        return Ok(Ddl::Drop {
            table,
            name,
            optional,
        });
    }
    if p.word("alter") {
        p.need("trigger")?;
        let name = p.name()?;
        p.need("on")?;
        let table = p.table_name()?;
        p.need("rename")?;
        p.need("to")?;
        let new = p.name()?;
        finish(p)?;
        return Ok(Ddl::Rename { table, name, new });
    }
    p.need("create")?;
    let replace = p.word("or");
    if replace {
        p.need("replace")?;
    }
    let constraint = p.word("constraint");
    p.need("trigger")?;
    let name = p.name()?;
    let (before, instead) = if p.word("before") {
        (true, false)
    } else if p.word("after") {
        (false, false)
    } else {
        p.need("instead")?;
        p.need("of")?;
        (false, true)
    };
    let mut events = 0;
    let mut update_columns = Vec::new();
    let locations = spans(source)?;
    loop {
        let at = p.at;
        let event = if p.word("insert") {
            INSERT
        } else if p.word("delete") {
            DELETE
        } else if p.word("update") {
            UPDATE
        } else {
            p.need("truncate")?;
            TRUNCATE
        };
        if events & event != 0 {
            let span = &locations[at];
            let mut diagnostic = RoutineDiagnostic::new(
                "42601",
                format!(
                    "duplicate trigger events specified at or near \"{}\"",
                    &source[span.clone()]
                ),
            );
            diagnostic.position = Some(source[..span.start].chars().count() + 1);
            return Err(diagnostic.error());
        }
        events |= event;
        if event == UPDATE && p.word("of") {
            loop {
                update_columns.push(p.name()?);
                if !p.sym(',') {
                    break;
                }
            }
        }
        if !p.word("or") {
            break;
        }
    }
    p.need("on")?;
    let table = p.table_name()?;
    for (refused, what) in [
        (constraint, "CONSTRAINT triggers"),
        (instead, "INSTEAD OF triggers"),
        (events & TRUNCATE != 0, "TRUNCATE triggers"),
        (p.word("referencing"), "REFERENCING transition tables"),
    ] {
        if refused {
            return Ok(Ddl::Unsupported { what, table });
        }
    }
    let row = if p.word("for") {
        p.word("each");
        if p.word("row") {
            true
        } else {
            p.need("statement")?;
            false
        }
    } else {
        false
    };
    let mut when_tokens = Vec::new();
    let when = if p.word("when") {
        p.punct('(')?;
        let start = p.at;
        let mut depth = 1;
        loop {
            match p.tokens.get(p.at) {
                Some(Token::Symbol('(')) => depth += 1,
                Some(Token::Symbol(')')) => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                None => return Err(SqlError::Syntax),
                _ => {}
            }
            p.at += 1;
        }
        if p.at == start {
            return Err(SqlError::Syntax);
        }
        when_tokens.extend_from_slice(&p.tokens[start..p.at]);
        query::validate_trigger_condition(&when_tokens)?;
        let text = source[locations[start - 1].end..locations[p.at].start].to_owned();
        p.at += 1;
        Some(text)
    } else {
        None
    };
    p.need("execute")?;
    if !p.word("function") {
        p.need("procedure")?;
    }
    let routine = Name::parse(p)?;
    p.punct('(')?;
    let mut args = Vec::new();
    if !p.sym(')') {
        loop {
            let arg = match p.tokens.get(p.at) {
                Some(Token::Word(v) | Token::QuotedIdent(v)) => ident::truncate(v, 63).into(),
                Some(Token::String(v)) => v.clone(),
                Some(Token::Number(v)) => v
                    .parse::<i32>()
                    .map_or_else(|_| v.clone(), |n| n.to_string()),
                _ => return Err(SqlError::Syntax),
            };
            if arg.contains('\0') || args.len() >= i16::MAX as usize {
                return Err(SqlError::Limit);
            }
            args.push(arg);
            p.at += 1;
            if !p.sym(',') {
                break;
            }
        }
        p.punct(')')?;
    }
    finish(p)?;
    Ok(Ddl::Create(Box::new(Create {
        table,
        replace,
        routine,
        update_columns,
        when_tokens,
        trigger: Trigger {
            id: 0,
            table: 0,
            name,
            routine: 0,
            events,
            before,
            row,
            enabled: b'O',
            update_columns: vec![],
            args,
            when,
            when_columns: vec![],
        },
    })))
}

pub(super) fn parse_state(p: &mut Parser) -> R<(Target, u8)> {
    let enabled = if p.word("enable") {
        if p.word("replica") {
            b'R'
        } else if p.word("always") {
            b'A'
        } else {
            b'O'
        }
    } else {
        p.need("disable")?;
        b'D'
    };
    p.need("trigger")?;
    let target = if p.word("all") || p.word("user") {
        Target::All
    } else {
        Target::Name(p.name()?)
    };
    Ok((target, enabled))
}
fn missing_trigger(name: &str, table: &str) -> SqlError {
    error(
        "42704",
        format!("trigger \"{name}\" for table \"{table}\" does not exist"),
    )
}
fn duplicate_trigger(name: &str, table: &str) -> SqlError {
    error(
        "42710",
        format!("trigger \"{name}\" for relation \"{table}\" already exists"),
    )
}
pub(super) fn set_enabled(
    state: &mut Relational,
    table: &TableDef,
    target: &Target,
    enabled: u8,
) -> R<()> {
    let entries = Arc::make_mut(&mut state.ext).triggers.get_mut(&table.id);
    match target {
        Target::All => {
            for trigger in entries.into_iter().flat_map(BTreeMap::values_mut) {
                trigger.enabled = enabled;
            }
        }
        Target::Name(name) => {
            let trigger = entries
                .and_then(|t| t.get_mut(name))
                .ok_or_else(|| missing_trigger(name, &table.name))?;
            trigger.enabled = enabled;
        }
    }
    Ok(())
}

fn relation_name(key: &str) -> String {
    let (schema, name) = split_key(key);
    let name = name.trim_start_matches('\0');
    if schema.is_empty() || schema == "public" {
        name.into()
    } else {
        format!("{schema}.{name}")
    }
}
pub(super) fn execute(state: &mut Relational, prepared: &Prepared, ddl: &Ddl) -> R<()> {
    if let Ddl::Unsupported { what, .. } = ddl {
        return Err(error("0A000", format!("{what} are not supported")));
    }
    let key = ddl.table();
    let table = match state.tables.get(key) {
        Some(table) => table,
        None if matches!(ddl, Ddl::Drop { optional: true, .. }) => {
            user_types::sql_notice(
                format!(
                    "relation \"{}\" does not exist, skipping",
                    relation_name(key)
                ),
                "00000",
            );
            return Ok(());
        }
        None if state.view_names.contains_key(key) => {
            let mut diagnostic =
                RoutineDiagnostic::new("42809", format!("\"{}\" is a view", split_key(key).1));
            if matches!(ddl, Ddl::Create(c) if c.trigger.row) {
                diagnostic.detail =
                    Some("Views cannot have row-level BEFORE or AFTER triggers.".into());
            }
            return Err(diagnostic.error());
        }
        None => {
            return Err(error(
                "42P01",
                format!("relation \"{}\" does not exist", relation_name(key)),
            ));
        }
    };
    let table_id = table.id;
    let table_name = &table.name;
    let triggers = state.ext.triggers.get(&table_id);
    match ddl {
        Ddl::Create(create) => {
            let mut trigger = create.trigger.clone();
            let routine = state.find_routine(&create.routine, &prepared.search_path);
            let Some((function, def)) = routine else {
                if create
                    .routine
                    .schema
                    .as_deref()
                    .is_none_or(|s| s == "pg_catalog")
                    && query::known_zero_argument_function(&create.routine.name)
                {
                    return Err(error(
                        "42P17",
                        format!(
                            "function {} must return type trigger",
                            create.routine.display()
                        ),
                    ));
                }
                return Err(error(
                    "42883",
                    format!("function {}() does not exist", create.routine.display()),
                ));
            };
            if def.return_type != routines::TRIGGER_TYPE {
                return Err(error(
                    "42P17",
                    format!(
                        "function {} must return type trigger",
                        create.routine.display()
                    ),
                ));
            }
            let existing = triggers.and_then(|entries| entries.get(&trigger.name));
            if existing.is_some() && !create.replace {
                return Err(duplicate_trigger(&trigger.name, table_name));
            }
            for name in &create.update_columns {
                let id = column_id(table, name)?;
                if trigger.update_columns.contains(&id) {
                    return Err(error(
                        "42701",
                        format!("column \"{name}\" specified more than once"),
                    ));
                }
                trigger.update_columns.push(id);
            }
            bind_when(table, &mut trigger, &create.when_tokens)?;
            trigger.table = table_id;
            trigger.routine = function;
            trigger.id = if let Some(previous) = existing {
                previous.id
            } else {
                allocate(&mut state.next_id)?
            };
            type_oid(trigger.id)?;
            Arc::make_mut(&mut state.ext)
                .triggers
                .entry(table_id)
                .or_default()
                .insert(trigger.name.clone(), trigger);
        }
        Ddl::Drop { name, optional, .. } => {
            if triggers.is_none_or(|entries| !entries.contains_key(name)) {
                if *optional {
                    user_types::sql_notice(
                        format!(
                            "trigger \"{name}\" for relation \"{table_name}\" does not exist, skipping"
                        ),
                        "00000",
                    );
                    return Ok(());
                }
                return Err(missing_trigger(name, table_name));
            }
            let ext = Arc::make_mut(&mut state.ext);
            let entries = ext.triggers.get_mut(&table_id).unwrap();
            entries.remove(name);
            if entries.is_empty() {
                ext.triggers.remove(&table_id);
            }
        }
        Ddl::Rename { name, new, .. } => {
            let entries = Arc::make_mut(&mut state.ext)
                .triggers
                .get_mut(&table_id)
                .ok_or_else(|| missing_trigger(name, table_name))?;
            if !entries.contains_key(name) {
                return Err(missing_trigger(name, table_name));
            }
            if entries.contains_key(new) {
                return Err(duplicate_trigger(new, table_name));
            }
            let mut trigger = entries.remove(name).unwrap();
            trigger.name.clone_from(new);
            entries.insert(new.clone(), trigger);
        }
        Ddl::Unsupported { .. } => unreachable!(),
    }
    Ok(())
}
fn column_id(table: &TableDef, name: &str) -> R<u64> {
    table
        .columns
        .iter()
        .find(|c| c.name == name && c.name != HIDDEN_ROWID)
        .map(|c| c.id)
        .ok_or_else(|| {
            error(
                "42703",
                format!(
                    "column \"{name}\" of relation \"{}\" does not exist",
                    table.name
                ),
            )
        })
}
fn bind_when(table: &TableDef, trigger: &mut Trigger, tokens: &[Token]) -> R<()> {
    for window in tokens.windows(3) {
        let [
            Token::Word(side) | Token::QuotedIdent(side),
            Token::Symbol('.'),
            Token::Word(column) | Token::QuotedIdent(column),
        ] = window
        else {
            continue;
        };
        let failure = if !matches!(side.as_str(), "old" | "new") {
            return Err(error(
                "42P01",
                format!("missing FROM-clause entry for table \"{side}\""),
            ));
        } else if !trigger.row {
            Some("statement trigger's WHEN condition cannot reference column values")
        } else if side == "old" && trigger.events & INSERT != 0 {
            Some("INSERT trigger's WHEN condition cannot reference OLD values")
        } else if side == "new" && trigger.events & DELETE != 0 {
            Some("DELETE trigger's WHEN condition cannot reference NEW values")
        } else {
            None
        };
        if let Some(message) = failure {
            return Err(error("42P17", message));
        }
        let id = column_id(table, column)?;
        if !trigger.when_columns.contains(&id) {
            trigger.when_columns.push(id);
        }
    }
    Ok(())
}
fn table_description(state: &Relational, table: &TableDef) -> String {
    let name = quote(&table.name);
    let schema = state.table_schema(table.id);
    if schema == "public" {
        name
    } else {
        format!("{}.{name}", quote(schema))
    }
}
pub(super) fn routine_dependents(state: &Relational, routine: u64) -> Vec<String> {
    let mut deps: Vec<_> = state
        .tables
        .values()
        .flat_map(|table| {
            state
                .ext
                .triggers
                .get(&table.id)
                .into_iter()
                .flat_map(BTreeMap::values)
                .filter(move |trigger| trigger.routine == routine)
                .map(move |trigger| {
                    (
                        trigger.id,
                        format!(
                            "trigger {} on table {}",
                            trigger.name,
                            table_description(state, table)
                        ),
                    )
                })
        })
        .collect();
    deps.sort_by_key(|(id, _)| *id);
    deps.into_iter().map(|(_, text)| text).collect()
}
pub(super) fn rename_when_column(
    state: &mut Relational,
    table: u64,
    column: &ColumnDef,
    new: &str,
) -> R<()> {
    if !state.ext.triggers.get(&table).is_some_and(|entries| {
        entries
            .values()
            .any(|t| t.when_columns.contains(&column.id))
    }) {
        return Ok(());
    }
    let entries = Arc::make_mut(&mut state.ext)
        .triggers
        .get_mut(&table)
        .unwrap();
    let name = quote(new);
    for trigger in entries
        .values_mut()
        .filter(|t| t.when_columns.contains(&column.id))
    {
        let Some(condition) = &mut trigger.when else {
            continue;
        };
        let mut tokens = Vec::new();
        let mut locations = Vec::new();
        lex_with_spans(condition, &mut tokens, false, Some(&mut locations))?;
        for (at, window) in tokens.windows(3).enumerate().rev() {
            if matches!(window,
                [Token::Word(side) | Token::QuotedIdent(side), Token::Symbol('.'), Token::Word(old) | Token::QuotedIdent(old)]
                if matches!(side.as_str(), "new" | "old") && *old == column.name)
            {
                condition.replace_range(locations[at + 2].clone(), &name);
            }
        }
    }
    Ok(())
}
pub(super) fn drop_column(
    state: &mut Relational,
    table: &TableDef,
    column: &ColumnDef,
    cascade: bool,
) -> R<()> {
    let Some(entries) = state.ext.triggers.get(&table.id) else {
        return Ok(());
    };
    let depends = |trigger: &Trigger| {
        trigger.update_columns.contains(&column.id) || trigger.when_columns.contains(&column.id)
    };
    let table_name = table_description(state, table);
    let column_name = quote(&column.name);
    let mut dependents: Vec<_> = entries.values().filter(|t| depends(t)).collect();
    dependents.sort_by_key(|t| t.id);
    if dependents.is_empty() {
        return Ok(());
    }
    if !cascade {
        let mut diagnostic = RoutineDiagnostic::new(
            "2BP01",
            format!(
                "cannot drop column {column_name} of table {table_name} because other objects depend on it"
            ),
        );
        diagnostic.detail = Some(dependents.iter().map(|t| format!("trigger {} on table {table_name} depends on column {column_name} of table {table_name}", t.name)).collect::<Vec<_>>().join("\n"));
        diagnostic.hint = Some(routines::DEPENDENCY_HINT.into());
        return Err(diagnostic.error());
    }
    routines::cascade_notices(
        dependents
            .iter()
            .map(|t| format!("drop cascades to trigger {} on table {table_name}", t.name))
            .collect(),
    );
    let ext = Arc::make_mut(&mut state.ext);
    let entries = ext.triggers.get_mut(&table.id).unwrap();
    entries.retain(|_, t| !depends(t));
    if entries.is_empty() {
        ext.triggers.remove(&table.id);
    }
    Ok(())
}

impl Trigger {
    fn enabled_for_role(&self, replica: bool) -> bool {
        matches!(
            (self.enabled, replica),
            (b'A', _) | (b'O', false) | (b'R', true)
        )
    }
}

impl Prepared {
    /// The SET list is sufficient to exclude UPDATE OF triggers and FK
    /// actions for simple updates. Plans can have several distinct SET lists.
    fn plain_update_columns(&self, table: &TableDef) -> Option<BTreeSet<u64>> {
        let names = match self.statement.get() {
            Statement::Update { sets, .. } => {
                sets.iter().map(|(name, _)| name).collect::<BTreeSet<_>>()
            }
            Statement::UpdateExpressions { sets, .. } => {
                sets.iter().map(|(name, _)| name).collect()
            }
            _ => return None,
        };
        Some(
            table
                .columns
                .iter()
                .filter(|c| names.contains(&c.name))
                .map(|c| c.id)
                .collect(),
        )
    }
}

impl Relational {
    /// Refuse a write before publication when it could invoke a trigger on
    /// the target or on a table reached by a referential action.
    pub(crate) fn check_unimplemented_trigger_firing(
        &self,
        prepared: &Prepared,
        replica: bool,
        copy: bool,
    ) -> R<()> {
        use catalog_ext::{Action, ConstraintKind};

        if self.ext.triggers.is_empty() {
            return Ok(());
        }
        for (name, command) in prepared.write_tables() {
            let Some(table) = self.tables.get(&name) else {
                continue;
            };
            let event = match command {
                "INSERT" => INSERT,
                "UPDATE" => UPDATE,
                "DELETE" => DELETE,
                // A DO NOTHING-only MERGE still must not take a path that
                // is unimplemented for targets with active trigger definitions.
                "MERGE" => INSERT | UPDATE | DELETE,
                _ => continue,
            };
            let updated = if event == UPDATE {
                prepared.plain_update_columns(table)
            } else {
                None
            };
            let mut pending = vec![(table.id, event, updated)];
            let mut seen = BTreeSet::new();
            while let Some((id, event, updated)) = pending.pop() {
                if !seen.insert((id, event, updated.clone())) {
                    continue;
                }
                let fires = self
                    .ext
                    .triggers
                    .get(&id)
                    .into_iter()
                    .flat_map(BTreeMap::values)
                    .any(|trigger| {
                        trigger.enabled_for_role(replica)
                            && trigger.events & event != 0
                            && (event != UPDATE
                                || trigger.update_columns.is_empty()
                                || updated.as_ref().is_none_or(|columns| {
                                    trigger
                                        .update_columns
                                        .iter()
                                        .any(|column| columns.contains(column))
                                }))
                    });
                if fires {
                    let command = if copy {
                        "COPY FROM"
                    } else if prepared.command() == "MERGE" {
                        "MERGE"
                    } else {
                        command
                    };
                    return Err(error(
                        "0A000",
                        format!("{command} on a table with triggers is not supported"),
                    ));
                }
                if replica || !matches!(event, UPDATE | DELETE) {
                    continue;
                }
                for (&child, constraints) in &self.ext.constraints {
                    for constraint in constraints {
                        let ConstraintKind::ForeignKey {
                            ref_table,
                            columns,
                            ref_columns,
                            on_delete,
                            on_update,
                            ..
                        } = &constraint.kind
                        else {
                            continue;
                        };
                        if *ref_table != id
                            || updated.as_ref().is_some_and(|updated| {
                                !ref_columns.iter().any(|column| updated.contains(column))
                            })
                        {
                            continue;
                        }
                        match if event == DELETE {
                            on_delete
                        } else {
                            on_update
                        } {
                            Action::Cascade if event == DELETE => {
                                pending.push((child, DELETE, None))
                            }
                            Action::Cascade | Action::SetNull | Action::SetDefault => pending
                                .push((child, UPDATE, Some(columns.iter().copied().collect()))),
                            Action::NoAction | Action::Restrict => (),
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Match adopted PostgreSQL event bits, timing/level and replication role.
    /// UPDATE OF and WHEN are evaluated by the firing caller.
    #[allow(dead_code, reason = "consumed by the trigger firing worker")]
    pub(crate) fn enabled_triggers(
        &self,
        table: u64,
        event: u8,
        before: bool,
        row: bool,
        replica: bool,
    ) -> impl Iterator<Item = &Trigger> {
        self.ext
            .triggers
            .get(&table)
            .into_iter()
            .flat_map(BTreeMap::values)
            .filter(move |t| {
                t.events & event != 0
                    && t.before == before
                    && t.row == row
                    && t.enabled_for_role(replica)
            })
    }
}
