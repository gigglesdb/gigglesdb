//! Constraint DDL and statement-atomic referential enforcement.
use super::*;
#[derive(Debug, Clone)]
pub(super) enum Definition {
    Unique {
        name: Option<String>,
        columns: Vec<String>,
    },
    Check {
        name: Option<String>,
        expression: Vec<Token>,
        not_valid: bool,
    },
    ForeignKey {
        name: Option<String>,
        columns: Vec<String>,
        parent: String,
        resolution_error: Option<SqlError>,
        references: Vec<String>,
        on_delete: catalog_ext::Action,
        on_update: catalog_ext::Action,
        not_valid: bool,
        deferral: Deferral,
    },
}
// CREATE's accepted key specs are immediate and NULLS DISTINCT. Keep the PK
// first, then the first occurrence of each ordered UNIQUE key (PG18).
pub(super) fn canonicalize_create(
    keys: &[String],
    primary_name: &Option<String>,
    definitions: &[Definition],
) -> (Option<String>, Vec<Definition>) {
    let mut primary_name = primary_name.clone();
    let mut kept = Vec::new();
    for definition in definitions {
        if let Definition::Unique { name, columns } = definition {
            if !keys.is_empty() && columns == keys {
                if primary_name.is_none() {
                    primary_name = name.clone();
                }
                continue;
            }
            if let Some(Definition::Unique { name: earlier, .. }) = kept.iter_mut().find(
                |d| matches!(d, Definition::Unique { columns: earlier, .. } if earlier == columns),
            ) {
                if earlier.is_none() {
                    *earlier = name.clone();
                }
                continue;
            }
        }
        kept.push(definition.clone());
    }
    (primary_name, kept)
}
#[derive(Debug, Clone, Default)]
pub(crate) struct DeferralState {
    // Transaction-local trigger mode; never part of the persisted catalog.
    pub(crate) replica: bool,
    pub(crate) modes: BTreeMap<u64, bool>,
    pub(crate) pending: BTreeMap<(u64, u64), BTreeSet<Vec<u8>>>,
}
impl DeferralState {
    fn deferred(&self, id: u64, kind: &ConstraintKind) -> bool {
        matches!(kind, ConstraintKind::ForeignKey { deferral, .. } if *deferral != Deferral::NotDeferrable && self.modes.get(&id).copied().unwrap_or(*deferral == Deferral::DeferrableDeferred))
    }
}
pub(super) fn parse_check(p: &mut Parser, name: Option<String>) -> R<Definition> {
    p.punct('(')?;
    let start = p.at;
    let mut depth = 1;
    while depth > 0 {
        match p.tokens.get(p.at) {
            Some(Token::Symbol('(')) => depth += 1,
            Some(Token::Symbol(')')) => depth -= 1,
            None => return Err(SqlError::Syntax),
            _ => (),
        }
        p.at += 1;
    }
    let expression = p.tokens[start..p.at - 1].to_vec();
    if expression.is_empty() {
        return Err(SqlError::Syntax);
    }
    let not_valid = p.word("not");
    if not_valid {
        p.need("valid")?;
    }
    Ok(Definition::Check {
        name,
        expression,
        not_valid,
    })
}
pub(super) fn parse_foreign(
    p: &mut Parser,
    name: Option<String>,
    columns: Vec<String>,
) -> R<Definition> {
    let first = alter_table::raw_name(p)?;
    let (parent, mut resolution_error) = if p.sym('.') {
        let parent = alter_table::raw_name(p)?;
        let error = ident::ident(&parent).err();
        p.relations.push(schemas::Reference::read(
            parent.clone(),
            Some(first.clone()),
        ));
        (format!("{first}\0{parent}"), error)
    } else {
        let error = ident::ident(&first).err();
        p.relations
            .push(schemas::Reference::read(first.clone(), None));
        (first, error)
    };
    let references = if p.tokens.get(p.at) == Some(&Token::Symbol('(')) {
        alter_table::raw_names(p)?
    } else {
        vec![]
    };
    if resolution_error.is_none() {
        resolution_error = references.iter().find_map(|name| ident::ident(name).err());
    }
    let (mut on_delete, mut on_update) =
        (catalog_ext::Action::NoAction, catalog_ext::Action::NoAction);
    let mut deferrable = false;
    let mut deferred = false;
    let mut not_valid = false;
    loop {
        if p.word("on") {
            let delete = if p.word("delete") {
                true
            } else {
                p.need("update")?;
                false
            };
            let action = if p.word("cascade") {
                catalog_ext::Action::Cascade
            } else if p.word("restrict") {
                catalog_ext::Action::Restrict
            } else if p.word("no") {
                p.need("action")?;
                catalog_ext::Action::NoAction
            } else if p.word("set") {
                if p.word("null") {
                    catalog_ext::Action::SetNull
                } else {
                    p.need("default")?;
                    catalog_ext::Action::SetDefault
                }
            } else {
                return Err(SqlError::Syntax);
            };
            if delete {
                on_delete = action;
            } else {
                on_update = action;
            }
        } else if p.word("deferrable") {
            deferrable = true;
        } else if p.word("initially") {
            deferred = if p.word("deferred") {
                true
            } else {
                p.need("immediate")?;
                false
            };
        } else if p.word("match") {
            p.need("simple")?;
        } else if p.tokens.get(p.at) == Some(&Token::Word("not".into()))
            && matches!(p.tokens.get(p.at+1), Some(Token::Word(w)) if w == "valid" || w == "deferrable")
        {
            p.at += 1;
            if p.word("valid") {
                not_valid = true;
            } else {
                p.need("deferrable")?;
                deferrable = false;
            }
        } else {
            break;
        }
    }
    if deferred && !deferrable {
        return Err(SqlError::Syntax);
    }
    Ok(Definition::ForeignKey {
        name,
        columns,
        parent,
        resolution_error,
        references,
        on_delete,
        on_update,
        not_valid,
        deferral: if deferred {
            Deferral::DeferrableDeferred
        } else if deferrable {
            Deferral::DeferrableImmediate
        } else {
            Deferral::NotDeferrable
        },
    })
}
fn id_positions(table: &TableDef, ids: &[u64]) -> R<Vec<usize>> {
    ids.iter()
        .map(|id| {
            table
                .columns
                .iter()
                .position(|c| c.id == *id)
                .ok_or(SqlError::UnknownColumn)
        })
        .collect()
}
fn table_by_id(state: &Relational, id: u64) -> R<&TableDef> {
    state
        .tables
        .values()
        .find(|t| t.id == id)
        .ok_or(SqlError::UnknownTable)
}
fn named_ids(table: &TableDef, names: &[String]) -> R<Vec<u64>> {
    names
        .iter()
        .map(|n| col(table, n).map(|i| table.columns[i].id))
        .collect()
}
// Select once at installation; persisted bindings are never reconsidered.
fn referenced_key(
    state: &Relational, parent: &TableDef, columns: &[u64], omitted: bool,
) -> R<super::catalog_ext::ReferencedKey> {
    use super::catalog_ext::ReferencedKey;
    let same_set = |a: &[u64], b: &[u64]| {
        a.len() == b.len() && a.iter().all(|id| b.contains(id))
    };
    let pk: Vec<_> = parent.primary_key.iter().map(|i| parent.columns[*i].id).collect();
    let primary = parent.hidden_rowid().is_none() && same_set(&pk, columns);
    if omitted {
        return if primary { Ok(ReferencedKey::PrimaryKey) } else { Err(SqlError::UndefinedObject) };
    }
    let mut candidates = Vec::new();
    if primary {
        candidates.push((state.primary_key_order(parent.id), ReferencedKey::PrimaryKey));
    }
    candidates.extend(state.secondary_descriptors_engine().iter()
        .filter(|d| d.table == parent.id && d.unique && same_set(&d.columns, columns))
        .map(|d| (d.id, ReferencedKey::Secondary(d.id))));
    candidates.into_iter().min_by_key(|(order, _)| *order)
        .map(|(_, target)| target).ok_or(SqlError::InvalidForeignKey)
}
fn key_values(row: &SqlRow, positions: &[usize]) -> Vec<Value> {
    positions.iter().map(|i| row.values[*i].clone()).collect()
}
fn fk_row(state: &Relational, table: &TableDef, row: &SqlRow, kind: &ConstraintKind) -> R<()> {
    let ConstraintKind::ForeignKey {
        columns,
        ref_table,
        ref_columns,
        ..
    } = kind
    else {
        return Ok(());
    };
    let child_positions = id_positions(table, columns)?;
    let key = key_values(row, &child_positions);
    if key.contains(&Value::Null) {
        return Ok(());
    }
    let parent = table_by_id(state, *ref_table)?;
    let positions = id_positions(parent, ref_columns)?;
    let key = key
        .iter()
        .zip(child_positions.iter().zip(&positions))
        .map(|(value, (child, parent_at))| {
            query::constraint_key(
                value,
                &table.columns[*child].data_type,
                &parent.columns[*parent_at].data_type,
            )
        })
        .collect::<R<Option<Vec<_>>>>()?
        .ok_or(SqlError::ForeignKeyViolation)?;
    if !constraint_lookup::exists(state, parent, ref_columns, &key, &mut || {
        crate::cancellation::check().is_err()
    })? {
        return Err(SqlError::ForeignKeyViolation);
    }
    Ok(())
}
pub(super) fn install_unique(
    state: &mut Relational,
    table: &TableDef,
    explicit: &Option<String>,
    columns: &[String],
    cancel: &mut impl FnMut() -> bool,
) -> R<()> {
    let ids = named_ids(table, columns)?;
    if let Some(explicit) = explicit {
        ident::ident(explicit)?;
    }
    let name = explicit.clone().unwrap_or_else(|| {
        state.choose_constraint_name(
            &state.table_key(table),
            &columns.iter().map(String::as_str).collect::<Vec<_>>(),
            AutoName::Key,
        )
    });
    if state.relation_name_taken(&relation_key(state.table_schema(table.id), &name)) {
        return Err(SqlError::DuplicateTable);
    }
    if state.constraint_name_taken(table.id, &name) {
        return Err(SqlError::DuplicateObject);
    }
    let epoch = state.epoch;
    let index = state.create_secondary_index_populated_engine(table, epoch, &name, &ids, true, None, &mut *cancel)?;
    let id = allocate(&mut state.next_id)?;
    state.ext.constraints.entry(table.id).or_default().push(Constraint {
        id, name, kind: ConstraintKind::Unique { columns: ids, index },
    });
    Relational::normalize_ext(&mut state.ext);
    // The surrounding CREATE/ALTER owns the single schema epoch advance.
    state.epoch = epoch;
    Ok(())
}
pub(super) fn install(
    state: &mut Relational,
    table: &TableDef,
    definition: &Definition,
    cancel: &mut impl FnMut() -> bool,
) -> R<()> {
    let (explicit, names, suffix, kind) = match definition {
        Definition::Unique { name, columns } => {
            return install_unique(state, table, name, columns, cancel);
        }
        Definition::Check {
            name,
            expression,
            not_valid,
        } => {
            let expr = query::constraint_deparse(expression)?;
            query::constraint_validate(state, table, &expr)?;
            let referenced = table
                .columns
                .iter()
                .filter(|c| {
                    expression
                        .iter()
                        .any(|t| matches!(t,Token::Word(w)|Token::QuotedIdent(w) if *w==c.name))
                })
                .map(|c| c.name.clone())
                .collect::<Vec<_>>();
            let names = if referenced.len() == 1 {
                referenced
            } else {
                vec![]
            };
            (
                name,
                names,
                AutoName::Check,
                ConstraintKind::Check {
                    expr: format!(
                        "CHECK ({expr}){}",
                        if *not_valid { " NOT VALID" } else { "" }
                    ),
                    not_valid: *not_valid,
                },
            )
        }
        Definition::ForeignKey {
            name,
            columns,
            parent,
            resolution_error,
            references,
            on_delete,
            on_update,
            not_valid,
            deferral,
        } => {
            if let Some(error) = resolution_error {
                return Err(error.clone());
            }
            let parent = state.tables.get(parent).ok_or(SqlError::UnknownTable)?;
            let refs = if references.is_empty() {
                if parent.hidden_rowid().is_some() {
                    return Err(SqlError::UndefinedObject);
                }
                parent
                    .primary_key
                    .iter()
                    .filter(|i| Some(**i) != parent.hidden_rowid())
                    .map(|i| parent.columns[*i].name.clone())
                    .collect()
            } else {
                references.clone()
            };
            if columns.len() != refs.len() || columns.is_empty() {
                return Err(SqlError::InvalidForeignKey);
            }
            let ids = named_ids(table, columns)?;
            let ref_ids = named_ids(parent, &refs)?;
            unique(&ids)?;
            unique(&ref_ids).map_err(|_| SqlError::InvalidForeignKey)?;
            let target = referenced_key(state, parent, &ref_ids, references.is_empty())?;
            for (a, b) in ids.iter().zip(&ref_ids) {
                let a = &table.columns[table
                    .columns
                    .iter()
                    .position(|c| c.id == *a)
                    .ok_or(SqlError::UnknownColumn)?]
                .data_type;
                let b = &parent.columns[parent
                    .columns
                    .iter()
                    .position(|c| c.id == *b)
                    .ok_or(SqlError::UnknownColumn)?]
                .data_type;
                if !query::constraint_types_compatible(a, b) {
                    // 42804, PG: foreign key constraint "…" cannot be implemented.
                    return Err(SqlError::Type);
                }
            }
            (
                name,
                columns.clone(),
                AutoName::Fkey,
                ConstraintKind::ForeignKey {
                    columns: ids,
                    ref_table: parent.id,
                    ref_columns: ref_ids,
                    on_delete: on_delete.clone(),
                    on_update: on_update.clone(),
                    not_valid: *not_valid,
                    deferral: *deferral,
                    target,
                },
            )
        }
    };
    let name = explicit.clone().unwrap_or_else(|| {
        state.choose_constraint_name(
            &state.table_key(table),
            &names.iter().map(String::as_str).collect::<Vec<_>>(),
            suffix,
        )
    });
    if let ConstraintKind::ForeignKey { columns, ref_table, ref_columns, .. } = &kind {
        let parent = table_by_id(state, *ref_table)?;
        for (a, b) in columns.iter().zip(ref_columns) {
            if (!state.deterministic_column(*a) || !state.deterministic_column(*b))
                && state.column_collation(*a) != state.column_collation(*b) {
                let label = |id| -> R<String> {
                    Ok(match state.column_collation(id) {
                        None => "default".into(),
                        Some(column_collations::Reference::Builtin(oid)) => column_collations::builtin(*oid).ok_or(SqlError::Type)?,
                        Some(column_collations::Reference::User(id)) => state.ext.collations.get(id).ok_or(SqlError::Type)?.name.clone(),
                    })
                };
                let a_name = &table.columns.iter().find(|c| c.id == *a).ok_or(SqlError::UnknownColumn)?.name;
                let b_name = &parent.columns.iter().find(|c| c.id == *b).ok_or(SqlError::UnknownColumn)?.name;
                user_types::view_replace_error(format!("foreign key constraint \"{name}\" cannot be implemented"));
                user_types::sql_error_detail(format!("Key columns \"{a_name}\" of the referencing table and \"{b_name}\" of the referenced table have incompatible collations: \"{}\" and \"{}\".  If either collation is nondeterministic, then both collations have to be the same.", label(*a)?, label(*b)?));
                return Err(SqlError::CollationMismatch);
            }
        }
    }
    ident(&name)?;
    if state.constraint_name_taken(table.id, &name) {
        return Err(SqlError::DuplicateObject);
    }
    let skip = matches!(
        &kind,
        ConstraintKind::Check {
            not_valid: true,
            ..
        } | ConstraintKind::ForeignKey {
            not_valid: true,
            ..
        }
    );
    if !skip {
        validate_kind(state, table, &kind, None, cancel)?;
    }
    let id = allocate(&mut state.next_id)?;
    state
        .ext
        .constraints
        .entry(table.id)
        .or_default()
        .push(Constraint { id, name, kind });
    Relational::normalize_ext(&mut state.ext);
    Ok(())
}
fn validate_kind(
    state: &Relational,
    table: &TableDef,
    kind: &ConstraintKind,
    keys: Option<&BTreeSet<Vec<u8>>>,
    cancel: &mut impl FnMut() -> bool,
) -> R<()> {
    for (key, row) in state.rows.get(&table.id).ok_or(SqlError::Type)? {
        if keys.is_some_and(|keys| !keys.contains(key)) {
            continue;
        }
        if cancel() {
            return Err(SqlError::Aborted);
        }
        match kind {
            ConstraintKind::Check { expr, .. } => query::constraint_check(state, table, expr, row)?,
            ConstraintKind::ForeignKey { .. } => fk_row(state, table, row, kind)?,
            ConstraintKind::PrimaryKey | ConstraintKind::Unique { .. } => (),
        }
    }
    Ok(())
}
pub(super) fn validate_named(
    state: &mut Relational,
    table: &TableDef,
    name: &str,
    cancel: &mut impl FnMut() -> bool,
) -> R<()> {
    let constraint = state
        .ext
        .constraints
        .get(&table.id)
        .and_then(|cs| cs.iter().find(|c| c.name == name))
        .ok_or(SqlError::UndefinedObject)?
        .clone();
    validate_kind(state, table, &constraint.kind, None, cancel)?;
    let constraint = state
        .ext
        .constraints
        .get_mut(&table.id)
        .and_then(|cs| cs.iter_mut().find(|c| c.id == constraint.id))
        .ok_or(SqlError::UndefinedObject)?;
    match &mut constraint.kind {
        ConstraintKind::Check { expr, not_valid } => {
            *not_valid = false;
            if let Some(without) = expr.strip_suffix(" NOT VALID") {
                *expr = without.to_owned();
            }
        }
        ConstraintKind::ForeignKey { not_valid, .. } => *not_valid = false,
        _ => (),
    }
    Ok(())
}
/// Persisted bindings are authoritative; legacy Unknown bindings block by column set.
pub(super) fn key_dependents(
    state: &Relational, table: u64, key: catalog_ext::ReferencedKey, columns: &[u64],
) -> (Vec<(u64, u64)>, bool) {
    let mut known = Vec::new();
    let mut unknown = false;
    for (&child, constraints) in &state.ext.constraints {
        for c in constraints {
            if let ConstraintKind::ForeignKey { ref_table, ref_columns, target, .. } = &c.kind {
                if *ref_table != table { continue; }
                if *target == catalog_ext::ReferencedKey::Unknown {
                    unknown |= ref_columns.iter().copied().collect::<BTreeSet<_>>()
                        == columns.iter().copied().collect::<BTreeSet<_>>();
                } else if *target == key { known.push((child, c.id)); }
            }
        }
    }
    (known, unknown)
}
pub(super) fn drop_named(
    state: &mut Relational,
    table: &TableDef,
    name: &str,
    optional: bool,
) -> R<()> {
    drop_named_cascade(state, table, name, optional, false)
}
pub(super) fn drop_named_cascade(
    state: &mut Relational,
    table: &TableDef,
    name: &str,
    optional: bool,
    cascade: bool,
) -> R<()> {
    if table.hidden_rowid().is_none()
        && constraint_primary_key::primary_key_name(state, table).as_deref() == Some(name)
    {
        return Err(SqlError::Unsupported);
    }
    if let Some(Constraint { kind: ConstraintKind::Unique { index, .. }, .. }) =
        state.table_constraints(table.id).iter().find(|c| c.name == name) {
        let index = *index;
        let columns = &state.secondary_descriptors_engine().iter().find(|d| d.id == index).ok_or(SqlError::Type)?.columns;
        let (dependents, unknown) = key_dependents(
            state, table.id, catalog_ext::ReferencedKey::Secondary(index), columns,
        );
        if unknown {
            return Err(if cascade { SqlError::Unsupported } else { SqlError::DependentObjects });
        }
        if !dependents.is_empty() && !cascade {
            return Err(SqlError::DependentObjects);
        }
        for (child, id) in dependents {
            let child = state.tables.values().find(|t| t.id == child).ok_or(SqlError::Type)?.clone();
            let name = state.table_constraints(child.id).iter().find(|c| c.id == id)
                .ok_or(SqlError::Type)?.name.clone();
            drop_named(state, &child, &name, false)?;
        }
        let descriptors = state.secondary_descriptors_engine().iter()
            .filter(|d| d.id != index).cloned().collect::<Vec<_>>();
        state.secondary = state.secondary_build_complete_engine(&descriptors, &mut || false)?;
    }
    let cs = state.ext.constraints.entry(table.id).or_default();
    if let Some(at) = cs.iter().position(|c| c.name == name) {
        if matches!(cs[at].kind, ConstraintKind::PrimaryKey) {
            return Err(SqlError::Unsupported);
        }
        cs.remove(at);
    } else if !optional {
        return Err(SqlError::UndefinedObject);
    }
    if cs.is_empty() {
        state.ext.constraints.remove(&table.id);
    }
    Relational::normalize_ext(&mut state.ext);
    Ok(())
}

#[derive(Clone)]
pub(crate) struct Event {
    pub(crate) table: u64,
    pub(crate) old: SqlRow,
    pub(crate) new: Option<SqlRow>,
}
pub(super) fn enforce(
    state: &mut Relational,
    before: &Relational,
    captured: Vec<Event>,
    deferred: &mut DeferralState,
    cancel: &mut impl FnMut() -> bool,
    clock: Option<&clock::ExecutionClock>,
) -> R<()> {
    enforce_with_rows(
        state,
        std::borrow::Cow::Borrowed(before),
        captured,
        deferred,
        cancel,
        clock,
        None,
    )
    .map(|_| ())
}
type LoadRows<'a> = dyn FnMut(&mut Relational, &mut Relational) -> R<()> + 'a;
pub(super) fn enforce_with_rows<'a>(
    state: &mut Relational,
    mut before: std::borrow::Cow<'a, Relational>,
    captured: Vec<Event>,
    deferred: &mut DeferralState,
    cancel: &mut impl FnMut() -> bool,
    clock: Option<&clock::ExecutionClock>,
    mut load: Option<&mut LoadRows<'_>>,
) -> R<std::borrow::Cow<'a, Relational>> {
    // DDL may have removed constraints with queued checks or explicit modes.
    deferred.modes.retain(|id, _| state.ext.constraints.values().flatten().any(|c| c.id == *id));
    deferred.pending.retain(|(id, table), _| state.table_constraints(*table).iter().any(|c| c.id == *id));
    // UNIQUE is enforced by its index; it must not activate row-event checks.
    if !state.ext.constraints.values().flatten().any(|c| matches!(c.kind,
        ConstraintKind::Check { .. } | ConstraintKind::ForeignKey { .. })) {
        return Ok(before);
    }
    if let Some(load) = load.as_mut() {
        load(state, before.to_mut())?;
    }
    let mut touched_old = captured
        .iter()
        .filter_map(|event| {
            event
                .new
                .as_ref()
                .map(|new| ((event.table, new.version), event.old.clone()))
        })
        .collect::<BTreeMap<_, _>>();
    let captured_old = captured
        .iter()
        .map(|e| (e.table, e.old.version))
        .collect::<BTreeSet<_>>();
    if captured.len() > MAX_ROWS {
        return Err(SqlError::Limit);
    }
    let mut events = std::collections::VecDeque::from(captured);
    for (id, rows) in &before.rows {
        let Some(after) = state.rows.get(id) else {
            continue;
        };
        for (key, old) in rows {
            if cancel() {
                return Err(SqlError::Aborted);
            }
            if captured_old.contains(&(*id, old.version))
                || after.get(key).is_some_and(|r| r.values == old.values)
            {
                continue;
            }
            if events.len() >= MAX_ROWS {
                return Err(SqlError::Limit);
            }
            events.push_back(Event {
                table: *id,
                old: old.clone(),
                new: after.get(key).cloned(),
            });
        }
    }
    let mut scheduled = events.len();
    let mut visited = BTreeSet::new();
    let mut affected = 0usize;
    let mut parent_checks: BTreeMap<(u64, u64), BTreeSet<Vec<u8>>> = BTreeMap::new();
    while let Some(event) = events.pop_front() {
        if let Some(load) = load.as_mut() {
            load(state, before.to_mut())?;
        }
        if cancel() {
            return Err(SqlError::Aborted);
        }
        let signature = (
            event.table,
            event.old.version,
            event.new.as_ref().map(|r| r.version),
        );
        if !visited.insert(signature) {
            continue;
        }
        // Pending checks follow logical rows when their physical primary keys move.
        // Do not enqueue an unchanged NOT VALID FK unless it was already pending.
        let event_table = table_by_id(state, event.table)?;
        let old_pk = pk(event_table, &event.old.values)?;
        let new_pk = event
            .new
            .as_ref()
            .map(|row| pk(event_table, &row.values))
            .transpose()?;
        for checks in [&mut deferred.pending, &mut parent_checks] {
            for ((_, table_id), keys) in checks {
                if *table_id == event.table
                    && keys.remove(&old_pk)
                    && let Some(new_pk) = &new_pk
                {
                    keys.insert(new_pk.clone());
                }
            }
        }
        affected = affected.checked_add(1).ok_or(SqlError::Limit)?;
        if affected > MAX_ROWS {
            return Err(SqlError::Limit);
        }
        if deferred.replica {
            continue;
        }
        let mut incoming = state
            .ext
            .constraints
            .iter()
            .flat_map(|(table, cs)| {
                cs.iter().filter_map(move |c| match &c.kind {
                    ConstraintKind::ForeignKey { ref_table, .. } if *ref_table == event.table => {
                        Some((*table, c.clone()))
                    }
                    _ => None,
                })
            })
            .collect::<Vec<_>>();
        // IDs preserve FK definition order across child tables and ALTER TABLE.
        incoming.sort_by_key(|(_, constraint)| constraint.id);
        for (child_id, constraint) in incoming {
            let ConstraintKind::ForeignKey {
                columns,
                ref_columns,
                on_delete,
                on_update,
                ..
            } = &constraint.kind
            else {
                unreachable!()
            };
            let parent = table_by_id(state, event.table)?.clone();
            let ref_positions = id_positions(&parent, ref_columns)?;
            let old_key = key_values(&event.old, &ref_positions);
            let new_key = event.new.as_ref().map(|r| key_values(r, &ref_positions));
            if new_key.as_ref() == Some(&old_key) || old_key.contains(&Value::Null) {
                continue;
            }
            let child = table_by_id(state, child_id)?.clone();
            let positions = id_positions(&child, columns)?;
            let parent_types: Vec<_> = ref_positions
                .iter()
                .map(|i| parent.columns[*i].data_type.clone())
                .collect();
            let children = constraint_lookup::matching_rows_typed(
                state,
                &child,
                columns,
                &old_key,
                &parent_types,
                cancel,
            )?;
            let action = if event.new.is_some() {
                on_update
            } else {
                on_delete
            };
            if matches!(action, catalog_ext::Action::Restrict) && !children.is_empty() {
                // The statement already fails: report an earlier immediate
                // NO ACTION trigger first, without changing deferred checks.
                for ((id, table_id), keys) in &parent_checks {
                    let prior = state.ext.constraints[table_id]
                        .iter()
                        .find(|c| c.id == *id)
                        .unwrap();
                    if !deferred.deferred(*id, &prior.kind) {
                        validate_kind(
                            state,
                            table_by_id(state, *table_id)?,
                            &prior.kind,
                            Some(keys),
                            cancel,
                        )?;
                    }
                }
                return Err(SqlError::RestrictViolation);
            }
            if matches!(action, catalog_ext::Action::NoAction) {
                parent_checks
                    .entry((constraint.id, child_id))
                    .or_default()
                    .extend(children.iter().map(|(key, _)| key.clone()));
                continue;
            }
            if matches!(action, catalog_ext::Action::Restrict) {
                continue;
            }
            for (old_pk, old) in children {
                if cancel() {
                    return Err(SqlError::Aborted);
                }
                if state
                    .rows
                    .get(&child_id)
                    .and_then(|rs| rs.get(&old_pk))
                    .is_none_or(|r| r.version != old.version)
                {
                    continue;
                }
                // Reserve the logical action before constructing or publishing it.
                // Counting only popped events would let fan-out overfill the queue.
                scheduled = scheduled.checked_add(1).ok_or(SqlError::Limit)?;
                if scheduled > MAX_ROWS {
                    return Err(SqlError::Limit);
                }
                let new = match action {
                    catalog_ext::Action::Cascade if event.new.is_none() => None,
                    _ => {
                        let mut row = old.clone();
                        for (i, position) in positions.iter().enumerate() {
                            row.values[*position] = match action {
                                catalog_ext::Action::Cascade => {
                                    new_key.as_ref().ok_or(SqlError::Type)?[i].clone()
                                }
                                catalog_ext::Action::SetNull => Value::Null,
                                catalog_ext::Action::SetDefault => {
                                    defaults::evaluate(state, &child.columns[*position], clock)?
                                }
                                _ => unreachable!(),
                            };
                        }
                        row_check(&child, &row.values)?;
                        row.version = allocate(&mut state.next_version)?;
                        Some(row)
                    }
                };
                let new_pk = new.as_ref().map(|r| pk(&child, &r.values)).transpose()?;
                let changes = if new_pk.as_ref().is_some_and(|k| *k != old_pk) {
                    vec![
                        BatchRowChange {
                            table: child_id,
                            old: Some((&old_pk, &old)),
                            new: None,
                        },
                        BatchRowChange {
                            table: child_id,
                            old: None,
                            new: new_pk
                                .as_ref()
                                .zip(new.as_ref())
                                .map(|(k, r)| (k.as_slice(), r)),
                        },
                    ]
                } else {
                    vec![BatchRowChange {
                        table: child_id,
                        old: Some((&old_pk, &old)),
                        new: new_pk
                            .as_ref()
                            .zip(new.as_ref())
                            .map(|(k, r)| (k.as_slice(), r)),
                    }]
                };
                state.secondary_apply_batch_engine(
                    state.epoch,
                    &changes,
                    state.next_version,
                    &mut *cancel,
                )?;
                if let Some(new) = &new {
                    touched_old.insert((child_id, new.version), old.clone());
                }
                events.push_back(Event {
                    table: child_id,
                    old,
                    new,
                });
            }
        }
    }
    if let Some(load) = load.as_mut() {
        load(state, before.to_mut())?;
    }
    for (table_id, cs) in &state.ext.constraints {
        let table = table_by_id(state, *table_id)?;
        let rows = state.rows.get(table_id).ok_or(SqlError::Type)?;
        let changed = rows
            .iter()
            .filter_map(|(key, row)| {
                if before
                    .rows
                    .get(table_id)
                    .and_then(|rs| rs.get(key))
                    .is_none_or(|r| r.version != row.version)
                {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect::<BTreeSet<_>>();
        for constraint in cs {
            if deferred.replica && matches!(constraint.kind, ConstraintKind::ForeignKey { .. }) {
                continue;
            }
            let changed = if let ConstraintKind::ForeignKey { columns, .. } = &constraint.kind {
                let positions = id_positions(table, columns)?;
                changed
                    .iter()
                    .filter(|key| {
                        let row = &rows[*key];
                        let old = touched_old
                            .get(&(*table_id, row.version))
                            .or_else(|| before.rows.get(table_id).and_then(|rows| rows.get(*key)));
                        old.is_none_or(|old| {
                            positions
                                .iter()
                                .any(|index| old.values[*index] != row.values[*index])
                        })
                    })
                    .cloned()
                    .collect::<BTreeSet<_>>()
            } else {
                changed.clone()
            };
            if deferred.deferred(constraint.id, &constraint.kind) {
                let pending = deferred
                    .pending
                    .entry((constraint.id, *table_id))
                    .or_default();
                pending.extend(changed.iter().cloned());
                if let Some(keys) = parent_checks.get(&(constraint.id, *table_id)) {
                    pending.extend(keys.iter().cloned());
                }
            } else {
                let mut keys = changed.clone();
                if let Some(parent) = parent_checks.get(&(constraint.id, *table_id)) {
                    keys.extend(parent.iter().cloned());
                }
                validate_kind(state, table, &constraint.kind, Some(&keys), cancel)?;
            }
        }
    }
    Ok(before)
}
impl Relational {
    pub(crate) fn check_insert_candidate(&self, table: &TableDef, values: &[Value]) -> R<()> {
        if let Some(constraints) = self.ext.constraints.get(&table.id) {
            let row = SqlRow {
                values: values.to_vec(),
                version: 0,
            };
            for constraint in constraints {
                if let ConstraintKind::Check { expr, .. } = &constraint.kind {
                    query::constraint_check(self, table, expr, &row)?;
                }
            }
        }
        Ok(())
    }
    pub(crate) fn validate_deferred(
        &self,
        deferred: &mut DeferralState,
        mut cancel: impl FnMut() -> bool,
    ) -> R<()> {
        for ((id, table_id), keys) in &deferred.pending {
            if let Some(c) = self
                .ext
                .constraints
                .get(table_id)
                .and_then(|cs| cs.iter().find(|c| c.id == *id))
            {
                validate_kind(
                    self,
                    table_by_id(self, *table_id)?,
                    &c.kind,
                    Some(keys),
                    &mut cancel,
                )?;
            }
        }
        deferred.pending.clear();
        Ok(())
    }
    pub(crate) fn set_constraints(
        &self,
        deferred: &mut DeferralState,
        names: &[String],
        is_deferred: bool,
        mut cancel: impl FnMut() -> bool,
    ) -> R<()> {
        let mut candidate = deferred.clone();
        let mut found = BTreeSet::new();
        let mut ids = BTreeSet::new();
        for cs in self.ext.constraints.values() {
            for c in cs {
                if names.is_empty() || names.contains(&c.name) {
                    found.insert(c.name.clone());
                    match &c.kind {
                        ConstraintKind::ForeignKey { deferral, .. }
                            if *deferral != Deferral::NotDeferrable =>
                        {
                            candidate.modes.insert(c.id, is_deferred);
                            ids.insert(c.id);
                        }
                        _ if is_deferred && !names.is_empty() => {
                            return Err(SqlError::WrongObjectType);
                        }
                        _ => (),
                    }
                }
            }
        }
        // Plain primary keys have a synthesized name without an ext record.
        // Resolve the actual name, including names retained across table renames.
        for table in self.tables.values() {
            if let Some(name) = self.primary_key_constraint_name(table)
                && names.contains(&name)
            {
                if is_deferred {
                    return Err(SqlError::WrongObjectType);
                }
                found.insert(name);
            }
        }
        if names.iter().any(|n| !found.contains(n)) {
            return Err(SqlError::UndefinedObject);
        }
        if !is_deferred {
            for ((id, table_id), keys) in &candidate.pending {
                if ids.contains(id)
                    && let Some(c) = self
                        .ext
                        .constraints
                        .get(table_id)
                        .and_then(|cs| cs.iter().find(|c| c.id == *id))
                {
                    validate_kind(
                        self,
                        table_by_id(self, *table_id)?,
                        &c.kind,
                        Some(keys),
                        &mut cancel,
                    )?;
                }
            }
            candidate.pending.retain(|(id, _), _| !ids.contains(id));
        }
        *deferred = candidate;
        Ok(())
    }
}
