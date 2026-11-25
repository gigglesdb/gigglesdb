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
type ParentDetails = BTreeMap<Vec<u8>, Arc<[Value]>>;

#[derive(Debug, Clone, Default)]
pub(crate) struct DeferralState {
    // Transaction-local trigger mode; never part of the persisted catalog.
    pub(crate) replica: bool,
    pub(crate) modes: BTreeMap<u64, bool>,
    pub(crate) pending: BTreeMap<(u64, u64), BTreeSet<Vec<u8>>>,
    pub(super) parent_details: BTreeMap<(u64, u64), ParentDetails>,
}
impl DeferralState {
    fn deferred(&self, id: u64, kind: &ConstraintKind) -> bool {
        matches!(kind, ConstraintKind::ForeignKey { deferral, .. } if *deferral != Deferral::NotDeferrable && self.modes.get(&id).copied().unwrap_or(*deferral == Deferral::DeferrableDeferred))
    }

    pub(super) fn prune_parent_details(&mut self) {
        self.parent_details.retain(|check, _| self.pending.contains_key(check));
    }

    pub(super) fn move_parent_details(&mut self, table: u64, old: &[u8], new: Option<&[u8]>) {
        for ((_, table_id), details) in &mut self.parent_details {
            if *table_id == table
                && let Some(values) = details.remove(old)
                && let Some(key) = new
            {
                details.insert(key.to_vec(), values);
            }
        }
    }

    pub(crate) fn parent_detail_bytes(&self) -> usize {
        self.parent_details.values().flat_map(|details| details.iter())
            .fold(0usize, |bytes, (key, values)| {
                values.iter().fold(bytes.saturating_add(key.len()).saturating_add(96),
                    |bytes, value| bytes.saturating_add(value_bytes(value)))
            })
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
        resolution_error = references.iter().find_map(|name| column_ident(name).err());
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
        .filter(|d| d.table == parent.id && d.unique && d.predicate.is_none() && same_set(&d.columns, columns))
        .map(|d| (d.id, ReferencedKey::Secondary(d.id))));
    candidates.into_iter().min_by_key(|(order, _)| *order)
        .map(|(_, target)| target).ok_or(SqlError::InvalidForeignKey)
}
fn key_values(row: &SqlRow, positions: &[usize]) -> Vec<Value> {
    positions.iter().map(|i| row.values[*i].clone()).collect()
}
fn fk_row(
    state: &Relational, table: &TableDef, row: &SqlRow, kind: &ConstraintKind, name: Option<&str>,
    parent_failure: Option<(&str, &[Value])>,
) -> R<()> {
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
    let missing = || {
        if let Some(name) = name {
            missing_parent(state, table, parent, name, &child_positions, row)
        } else if let Some((name, values)) = parent_failure {
            still_referenced(state, table, parent, name, &positions, values, false)
        } else {
            SqlError::ForeignKeyViolation
        }
    };
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
        .ok_or_else(missing)?;
    if !constraint_lookup::exists(state, parent, ref_columns, &key, &mut || {
        crate::cancellation::check().is_err()
    })? {
        return Err(missing());
    }
    Ok(())
}
/// A child row failed to find a matching referenced key.
fn missing_parent(
    state: &Relational,
    child: &TableDef,
    parent: &TableDef,
    name: &str,
    positions: &[usize],
    row: &SqlRow,
) -> SqlError {
    user_types::view_replace_error(format!(
        "insert or update on table \"{}\" violates foreign key constraint \"{}\"",
        child.name, name,
    ));
    let redacted = format!("Key is not present in table \"{}\".", parent.name);
    let names = positions.iter().map(|i| child.columns[*i].name.as_str())
        .collect::<Vec<_>>().join(", ");
    let values = positions.iter().map(|i| collation_keys::detail_text(&row.values[*i]))
        .collect::<Option<Vec<_>>>();
    let detail = values.map_or_else(|| redacted.clone(), |values| format!(
        "Key ({names})=({}) is not present in table \"{}\".", values.join(", "), parent.name,
    ));
    user_types::sql_foreign_key_detail(DuplicateKeyDetail {
        table_id: child.id,
        schema: state.table_schema(child.id).to_owned(),
        table: child.name.clone(),
        constraint: name.to_owned(),
        values: detail,
    }, redacted);
    SqlError::ForeignKeyViolation
}

pub(super) fn still_referenced(
    state: &Relational, child: &TableDef, parent: &TableDef, name: &str,
    positions: &[usize], values: &[Value], restrict: bool,
) -> SqlError {
    let setting = if restrict { "RESTRICT setting of " } else { "" };
    let still = if restrict { "" } else { "still " };
    user_types::view_replace_error(format!(
        "update or delete on table \"{}\" violates {setting}foreign key constraint \"{}\" on table \"{}\"",
        parent.name, name, child.name,
    ));
    let redacted = format!("Key is {still}referenced from table \"{}\".", child.name);
    let names = positions.iter().map(|i| parent.columns[*i].name.as_str())
        .collect::<Vec<_>>().join(", ");
    let values = values.iter().map(collation_keys::detail_text).collect::<Option<Vec<_>>>();
    let detail = values.map_or_else(|| redacted.clone(), |values| format!(
        "Key ({names})=({}) is {still}referenced from table \"{}\".", values.join(", "), child.name,
    ));
    user_types::sql_error_names(state, child, None, Some(name));
    user_types::sql_foreign_key_detail(DuplicateKeyDetail {
        table_id: parent.id,
        schema: state.table_schema(parent.id).to_owned(),
        table: parent.name.clone(),
        constraint: name.to_owned(),
        values: detail,
    }, redacted);
    if restrict { SqlError::RestrictViolation } else { SqlError::ForeignKeyViolation }
}

pub(super) fn duplicate_constraint(table: &TableDef, name: &str) -> SqlError {
    user_types::view_replace_error(format!(
        "constraint \"{name}\" for relation \"{}\" already exists", table.name,
    ));
    SqlError::DuplicateObject
}

pub(super) fn install_unique(
    state: &mut Relational,
    table: &TableDef,
    explicit: &Option<String>,
    columns: &[String],
    cancel: &mut impl FnMut() -> bool,
) -> R<()> {
    let ids = named_ids(table, columns)?;
    if ids.iter().any(|id| state.generated_column(*id).is_some_and(|generation| !generation.stored)) {
        user_types::view_replace_error("unique constraints on virtual generated columns are not supported".into());
        return Err(SqlError::Unsupported);
    }
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
    if sql_index::name_taken(state, &relation_key(state.table_schema(table.id), &name)) {
        return Err(sql_index::duplicate_relation(&name));
    }
    if state.constraint_name_taken(table.id, &name) {
        return Err(duplicate_constraint(table, &name));
    }
    let epoch = state.epoch;
    let index = state.create_secondary_index_populated_engine(table, epoch, &name, &ids, true, None, None, &mut *cancel)?;
    let id = allocate(&mut state.next_id)?;
    std::sync::Arc::make_mut(&mut state.ext).constraints.entry(table.id).or_default().push(Constraint {
        id, name, kind: ConstraintKind::Unique { columns: ids, index },
    });
    Relational::normalize_ext(std::sync::Arc::make_mut(&mut state.ext));
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
            let (expr, referenced) =
                query::constraint_table_deparse(state, table, expression)?;
            query::constraint_validate(state, table, &expr)?;
            let names = if referenced.len() == 1 {
                referenced.into_iter().collect()
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
            if ids.iter().chain(&ref_ids).any(|id| state.generated_column(*id).is_some_and(|generation| !generation.stored)) {
                user_types::view_replace_error("foreign key constraints on virtual generated columns are not supported".into());
                return Err(SqlError::Unsupported);
            }
            if ids.iter().any(|id| state.generated_column(*id).is_some()) {
                let event = if matches!(on_update, Action::Cascade | Action::SetNull | Action::SetDefault) {
                    Some("UPDATE")
                } else if matches!(on_delete, Action::SetNull | Action::SetDefault) {
                    Some("DELETE")
                } else {
                    None
                };
                if let Some(event) = event {
                    user_types::view_replace_error(format!("invalid ON {event} action for foreign key constraint containing generated column"));
                    return Err(SqlError::Syntax);
                }
            }
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
        return Err(duplicate_constraint(table, &name));
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
        validate_kind(state, table, &name, &kind, None, None, cancel)?;
    }
    let id = allocate(&mut state.next_id)?;
    std::sync::Arc::make_mut(&mut state.ext)
        .constraints.entry(table.id)
        .or_default()
        .push(Constraint { id, name, kind });
    Relational::normalize_ext(std::sync::Arc::make_mut(&mut state.ext));
    Ok(())
}
pub(super) fn rename_check_column(
    state: &mut Relational,
    table: &TableDef,
    old: &str,
    new: &str,
) -> R<()> {
    let mut replacements = BTreeMap::new();
    for constraint in state.table_constraints(table.id) {
        if let ConstraintKind::Check { expr, .. } = &constraint.kind
            && let Some(renamed) = query::constraint_rename(state, table, expr, old, new)?
        {
            replacements.insert(constraint.id, renamed);
        }
    }
    if !replacements.is_empty()
        && let Some(constraints) = Arc::make_mut(&mut state.ext).constraints.get_mut(&table.id)
    {
        for constraint in constraints {
            if let ConstraintKind::Check { expr, .. } = &mut constraint.kind
                && let Some(renamed) = replacements.remove(&constraint.id)
            {
                *expr = renamed;
            }
        }
    }
    Ok(())
}

pub(super) fn drop_check_column(state: &mut Relational, table: &TableDef, column: &str) -> R<()> {
    let mut removed = BTreeSet::new();
    for constraint in state.table_constraints(table.id) {
        if let ConstraintKind::Check { expr, .. } = &constraint.kind
            && query::constraint_references(state, table, expr, column)?
        {
            removed.insert(constraint.id);
        }
    }
    if !removed.is_empty()
        && let Some(constraints) = Arc::make_mut(&mut state.ext).constraints.get_mut(&table.id)
    {
        constraints.retain(|constraint| !removed.contains(&constraint.id));
    }
    Ok(())
}

struct CheckDiagnostics<'a> {
    changed: &'a BTreeSet<Vec<u8>>,
    written: &'a [usize],
    parents: Option<&'a ParentDetails>,
}

fn validate_kind(
    state: &Relational,
    table: &TableDef,
    name: &str,
    kind: &ConstraintKind,
    keys: Option<&BTreeSet<Vec<u8>>>,
    diagnostic: Option<CheckDiagnostics<'_>>,
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
            ConstraintKind::Check { expr, .. } => {
                let written = keys.map(|_| diagnostic.as_ref().map_or(&[][..], |d| d.written));
                check_row(state, table, name, expr, row, written)?;
            }
            ConstraintKind::ForeignKey { .. } => {
                let child_failure = diagnostic.as_ref().filter(|d| d.changed.contains(key)).map(|_| name);
                let parent_failure = diagnostic.as_ref().and_then(|d| d.parents)
                    .and_then(|parents| parents.get(key)).map(|values| (name, values.as_ref()));
                fk_row(state, table, row, kind, child_failure, parent_failure)?;
            }
            ConstraintKind::PrimaryKey | ConstraintKind::Unique { .. } => (),
        }
    }
    Ok(())
}
fn check_row(
    state: &Relational,
    table: &TableDef,
    name: &str,
    expr: &str,
    row: &SqlRow,
    written: Option<&[usize]>,
) -> R<()> {
    query::constraint_check(state, table, expr, row).inspect_err(|error| {
        if *error != SqlError::CheckViolation {
            return;
        }
        user_types::sql_error_names(state, table, None, Some(name));
        let Some(written) = written else {
            user_types::view_replace_error(format!(
                "check constraint \"{name}\" of relation \"{}\" is violated by some row",
                table.name,
            ));
            return;
        };
        user_types::view_replace_error(format!(
            "new row for relation \"{}\" violates check constraint \"{name}\"",
            table.name,
        ));
        let is_virtual = |id| state.generated_column(id).is_some_and(|generation| !generation.stored);
        user_types::sql_failing_check_detail(
            DuplicateKeyDetail {
                table_id: table.id,
                schema: state.table_schema(table.id).into(),
                table: table.name.clone(),
                constraint: name.into(),
                values: logical_types::failing_row_detail_with(table, &row.values, is_virtual),
            },
            logical_types::failing_row_written_detail(table, &row.values, written, is_virtual),
        );
    })
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
    validate_kind(state, table, &constraint.name, &constraint.kind, None, None, cancel)?;
    let constraint = std::sync::Arc::make_mut(&mut state.ext)
        .constraints.get_mut(&table.id)
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
    drop_named_cascade(state, table, name, optional, false, &mut || false)
}
pub(super) fn drop_named_cascade(
    state: &mut Relational,
    table: &TableDef,
    name: &str,
    optional: bool,
    cascade: bool,
    cancel: &mut impl FnMut() -> bool,
) -> R<()> {
    if constraint_primary_key::primary_key_name(state, table).as_deref() == Some(name) {
        return constraint_primary_key::drop(state, table, name, cascade, cancel);
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
    let Some(at) = state.table_constraints(table.id).iter().position(|c| c.name == name) else {
        let message = format!("constraint \"{name}\" of relation \"{}\" does not exist", table.name);
        if optional {
            user_types::sql_notice(format!("{message}, skipping"), "42704");
            return Ok(());
        }
        user_types::view_replace_error(message);
        return Err(SqlError::UndefinedObject);
    };
    let cs = std::sync::Arc::make_mut(&mut state.ext).constraints.entry(table.id).or_default();
    cs.remove(at);
    if cs.is_empty() {
        std::sync::Arc::make_mut(&mut state.ext).constraints.remove(&table.id);
    }
    Relational::normalize_ext(std::sync::Arc::make_mut(&mut state.ext));
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
        None,
    )
    .map(|_| ())
}
type LoadRows<'a> = dyn FnMut(&mut Relational, &mut Relational) -> R<()> + 'a;
pub(super) fn insert_check_columns(state: &Relational, prepared: &Prepared) -> Option<(u64, Vec<usize>)> {
    let table = state.tables.get(prepared.table()?)?;
    if !state.table_constraints(table.id).iter().any(|constraint| matches!(constraint.kind, ConstraintKind::Check { .. })) {
        return None;
    }
    let offsets = match prepared.statement.get() {
        Statement::Insert { columns, values, .. } => insert_value_indexes(table, columns, values.len()),
        Statement::InsertExpressions(values) => values.supplied_indexes(table),
        Statement::InsertMulti(insert) => insert.plain_insert_check_columns(state, table, &prepared.parameter_types),
        _ => return None,
    };
    offsets.ok().map(|offsets| (table.id, offsets))
}

#[expect(clippy::too_many_arguments, reason = "statement constraints with optional row loader and INSERT diagnostics")]
pub(super) fn enforce_with_rows<'a>(
    state: &mut Relational,
    mut before: std::borrow::Cow<'a, Relational>,
    captured: Vec<Event>,
    deferred: &mut DeferralState,
    cancel: &mut impl FnMut() -> bool,
    clock: Option<&clock::ExecutionClock>,
    insert_columns: Option<(u64, Vec<usize>)>,
    mut load: Option<&mut LoadRows<'_>>,
) -> R<std::borrow::Cow<'a, Relational>> {
    // DDL may have removed constraints with queued checks or explicit modes.
    deferred.modes.retain(|id, _| state.ext.constraints.values().flatten().any(|c| c.id == *id));
    deferred.pending.retain(|(id, table), _| state.table_constraints(*table).iter().any(|c| c.id == *id));
    deferred.prune_parent_details();
    // UNIQUE is enforced by its index; it must not activate row-event checks.
    if !state.ext.constraints.values().flatten().any(|c| matches!(c.kind,
        ConstraintKind::Check { .. } | ConstraintKind::ForeignKey { .. })) {
        return Ok(before);
    }
    if let Some(load) = load.as_mut() {
        load(state, before.to_mut())?;
    }
    let type_rewrite = !Arc::ptr_eq(&before.ext, &state.ext) && before.ext.types != state.ext.types;
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
        let (rekey, old_offsets) = if before.epoch != state.epoch {
            let old_table = table_by_id(&before, *id)?;
            let new_table = table_by_id(state, *id)?;
            let same_layout = old_table.columns.iter().map(|column| column.id)
                .eq(new_table.columns.iter().map(|column| column.id));
            let old_offsets = (!same_layout).then(|| new_table.columns.iter()
                .map(|column| old_table.columns.iter().position(|old| old.id == column.id))
                .collect::<Vec<_>>());
            (old_table.hidden_rowid().is_some() != new_table.hidden_rowid().is_some(), old_offsets)
        } else {
            (false, None)
        };
        // A DDL rewrite preserves row versions. Match by version when a key
        // moves, so a physical rekey does not look like a referential delete.
        let by_version = if rekey || type_rewrite {
            after.iter().map(|(key, row)| (row.version, (key, row))).collect::<BTreeMap<_, _>>()
        } else {
            BTreeMap::new()
        };
        if rekey {
            for ((_, table), keys) in &mut deferred.pending {
                if table == id {
                    *keys = keys.iter().filter_map(|key| {
                        let old = rows.get(key)?;
                        by_version.get(&old.version).map(|(new, _)| (*new).clone())
                    }).collect();
                }
            }
            for ((_, table), details) in &mut deferred.parent_details {
                if table == id {
                    *details = std::mem::take(details).into_iter().filter_map(|(key, values)| {
                        let old = rows.get(&key)?;
                        by_version.get(&old.version).map(|(new, _)| ((*new).clone(), values))
                    }).collect();
                }
            }
            continue;
        }
        for (key, old) in rows {
            if cancel() {
                return Err(SqlError::Aborted);
            }
            if captured_old.contains(&(*id, old.version)) {
                continue;
            }
            let new = by_version.get(&old.version).map(|(_, row)| *row).or_else(|| after.get(key));
            // ADD/DROP COLUMN moves the heap key's physical offset. Compare
            // the same attributes and carry postimage values for new columns,
            // whose constraints were already validated by ALTER TABLE.
            let old = if let Some(offsets) = &old_offsets {
                std::borrow::Cow::Owned(SqlRow {
                    version: old.version,
                    values: offsets.iter().enumerate().map(|(index, offset)| {
                        offset.and_then(|offset| old.values.get(offset))
                            .or_else(|| new.and_then(|row| row.values.get(index)))
                            .cloned().unwrap_or(Value::Null)
                    }).collect(),
                })
            } else {
                std::borrow::Cow::Borrowed(old)
            };
            if new.is_some_and(|r| r.values == old.values) {
                continue;
            }
            if events.len() >= MAX_ROWS {
                return Err(SqlError::Limit);
            }
            events.push_back(Event {
                table: *id,
                old: old.into_owned(),
                new: new.cloned(),
            });
        }
    }
    let mut scheduled = events.len();
    let mut visited = BTreeSet::new();
    let mut affected = 0usize;
    let mut parent_checks: BTreeMap<(u64, u64), BTreeSet<Vec<u8>>> = BTreeMap::new();
    let mut parent_details = BTreeMap::<_, ParentDetails>::new();
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
        for ((_, table_id), details) in &mut parent_details {
            if *table_id == event.table
                && let Some(values) = details.remove(&old_pk)
                && let Some(new_pk) = &new_pk
            {
                details.insert(new_pk.clone(), values);
            }
        }
        deferred.move_parent_details(event.table, &old_pk, new_pk.as_deref());
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
            let collated = ref_columns.iter().any(|id| !state.deterministic_column(*id));
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
                            &prior.name,
                            &prior.kind,
                            Some(keys),
                            Some(CheckDiagnostics {
                                changed: &BTreeSet::new(),
                                written: &[],
                                parents: parent_details.get(&(*id, *table_id)),
                            }),
                            cancel,
                        )?;
                    }
                }
                if collated {
                    return Err(still_referenced(
                        state, &child, &parent, &constraint.name, &ref_positions, &old_key, true,
                    ));
                }
                return Err(SqlError::RestrictViolation);
            }
            if matches!(action, catalog_ext::Action::NoAction) {
                if collated && !children.is_empty() {
                    let details = parent_details.entry((constraint.id, child_id)).or_default();
                    let values = Arc::from(old_key);
                    for (key, _) in &children {
                        details.entry(key.clone()).or_insert_with(|| Arc::clone(&values));
                    }
                }
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
                                    // Runs as the referencing table's owner (GD-0084 Q24).
                                    let action = clock.map(|clock| clock.with_sequence_origin(
                                        clock::SequenceOrigin::ReferentialAction(child_id)));
                                    defaults::evaluate(state, &child.columns[*position], action.as_ref())?
                                }
                                _ => unreachable!(),
                            };
                        }
                        state.materialize_generated(&child, &mut row.values)?;
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
        let written = insert_columns.as_ref().filter(|(id, _)| id == table_id)
            .map_or(&[][..], |(_, offsets)| offsets.as_slice());
        let before_rows = before.rows.get(table_id);
        // A renamed enum label can change a physical key without changing
        // the row's logical identity. Such rows are not CHECK postimages.
        let prior_versions = if type_rewrite {
            before_rows.into_iter().flat_map(|rows| rows.values().map(|row| row.version))
                .collect::<BTreeSet<_>>()
        } else {
            BTreeSet::new()
        };
        let changed = rows
            .iter()
            .filter_map(|(key, row)| {
                if !prior_versions.contains(&row.version) && before_rows
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
                let check = (constraint.id, *table_id);
                let pending = deferred
                    .pending
                    .entry(check)
                    .or_default();
                pending.extend(changed.iter().cloned());
                if let Some(keys) = parent_checks.get(&check) {
                    pending.extend(keys.iter().cloned());
                }
                if let Some(details) = parent_details.remove(&check) {
                    deferred.parent_details.entry(check).or_default().extend(
                        details.into_iter().filter(|(key, _)| !changed.contains(key)),
                    );
                }
                if let Some(details) = deferred.parent_details.get_mut(&check) {
                    details.retain(|key, _| !changed.contains(key));
                }
            } else {
                let mut keys = changed.clone();
                if let Some(parent) = parent_checks.get(&(constraint.id, *table_id)) {
                    keys.extend(parent.iter().cloned());
                }
                validate_kind(state, table, &constraint.name, &constraint.kind, Some(&keys),
                    Some(CheckDiagnostics {
                        changed: &changed,
                        written,
                        parents: parent_details.get(&(constraint.id, *table_id)),
                    }), cancel)?;
            }
        }
    }
    Ok(before)
}
impl Relational {
    pub(crate) fn check_insert_candidate(&self, table: &TableDef, values: &[Value]) -> R<()> {
        self.check_written_candidate(table, values, &[])
    }

    pub(super) fn check_written_candidate(&self, table: &TableDef, values: &[Value], written: &[usize]) -> R<()> {
        if let Some(constraints) = self.ext.constraints.get(&table.id) {
            let row = SqlRow {
                values: values.to_vec(),
                version: 0,
            };
            for constraint in constraints {
                if let ConstraintKind::Check { expr, .. } = &constraint.kind {
                    check_row(self, table, &constraint.name, expr, &row, Some(written))?;
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
                    &c.name,
                    &c.kind,
                    Some(keys),
                    Some(CheckDiagnostics {
                        changed: &BTreeSet::new(),
                        written: &[],
                        parents: deferred.parent_details.get(&(*id, *table_id)),
                    }),
                    &mut cancel,
                )?;
            }
        }
        deferred.pending.clear();
        deferred.parent_details.clear();
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
                        &c.name,
                        &c.kind,
                        Some(keys),
                        Some(CheckDiagnostics {
                            changed: &BTreeSet::new(),
                            written: &[],
                            parents: candidate.parent_details.get(&(*id, *table_id)),
                        }),
                        &mut cancel,
                    )?;
                }
            }
            candidate.pending.retain(|(id, _), _| !ids.contains(id));
            candidate.prune_parent_details();
        }
        *deferred = candidate;
        Ok(())
    }
}
