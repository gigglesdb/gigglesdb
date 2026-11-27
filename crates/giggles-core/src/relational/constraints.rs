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
pub(super) type PendingReferences = BTreeMap<Vec<u8>, PendingReference>;

#[derive(Debug, Clone, Default)]
pub(crate) enum PendingReference {
    #[default]
    Child,
    Parent(Arc<[Value]>),
}

impl PendingReference {
    pub(super) fn parent_values(&self) -> Option<&[Value]> {
        match self {
            Self::Child => None,
            Self::Parent(values) => Some(values),
        }
    }

    fn retained_bytes(&self) -> usize {
        // Charge the complete diagnostic tuple to each obligation, even if
        // several children share its Arc.
        self.parent_values().map_or(0, |values| values.iter().fold(16usize, |bytes, value| {
            bytes.saturating_add(size_of::<Value>()).saturating_add(value_bytes(value))
        }))
    }
}

fn pending_key_bytes(key: &[u8]) -> usize {
    key.len() + 96
}

pub(super) fn record_pending(
    retention: &mut scan::Retention, keys: &mut PendingReferences,
    key: Vec<u8>, reference: PendingReference,
) -> R<()> {
    match keys.entry(key) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            retention.charge(pending_key_bytes(entry.key()).saturating_add(reference.retained_bytes()))?;
            entry.insert(reference);
        }
        std::collections::btree_map::Entry::Occupied(mut entry) => {
            let old = entry.get().retained_bytes();
            let new = reference.retained_bytes();
            retention.charge(new.saturating_sub(old))?;
            retention.release(old.saturating_sub(new));
            entry.insert(reference);
        }
    }
    Ok(())
}

pub(super) fn release_pending(
    retention: &mut scan::Retention, key: &[u8], reference: &PendingReference,
) {
    retention.release(pending_key_bytes(key).saturating_add(reference.retained_bytes()));
}

#[derive(Debug, Clone, Default)]
pub(crate) struct DeferralState {
    // Transaction-local trigger mode; never part of the persisted catalog.
    pub(crate) replica: bool,
    pub(crate) modes: BTreeMap<u64, bool>,
    pub(crate) pending: BTreeMap<(u64, u64), PendingReferences>,
}
impl DeferralState {
    pub(super) fn deferred(&self, id: u64, kind: &ConstraintKind) -> bool {
        matches!(kind, ConstraintKind::ForeignKey { deferral, .. } if *deferral != Deferral::NotDeferrable && self.modes.get(&id).copied().unwrap_or(*deferral == Deferral::DeferrableDeferred))
    }

    pub(crate) fn parent_detail_bytes(&self) -> usize {
        self.pending.values().flatten().fold(0usize, |bytes, (_, reference)| {
            if let PendingReference::Parent(values) = reference {
                values.iter().fold(bytes, |bytes, value| bytes.saturating_add(value_bytes(value)))
            } else {
                bytes
            }
        })
    }

    pub(crate) fn v2_pending_bytes(&self) -> usize {
        self.pending.values().flatten().fold(0usize, |bytes, (key, reference)| {
            bytes.saturating_add(pending_key_bytes(key)).saturating_add(reference.retained_bytes())
        })
    }

    pub(crate) fn check_v2_pending_bytes(bytes: usize) -> R<()> {
        scan::Retention::new(scan::WRITE_SOURCE_BYTES).charge(bytes)
    }

    pub(super) fn pending_retention(&self) -> R<scan::Retention> {
        let mut retention = scan::Retention::new(scan::WRITE_SOURCE_BYTES);
        retention.charge(self.v2_pending_bytes())?;
        Ok(retention)
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
pub(super) fn id_positions(table: &TableDef, ids: &[u64]) -> R<Vec<usize>> {
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
// Parent-side RI triggers compare the stored images, before applying SQL equality.
pub(super) fn key_image_eq(old: &Value, new: &Value) -> bool {
    match (old, new) {
        (Value::Numeric(old), Value::Numeric(new)) => old == new && old.scale() == new.scale(),
        (Value::Float4(old), Value::Float4(new)) => old.to_bits() == new.to_bits(),
        (Value::Float8(old), Value::Float8(new)) => old.to_bits() == new.to_bits(),
        _ => old == new,
    }
}

pub(super) fn row_image_eq(old: &[Value], new: &[Value]) -> bool {
    old.len() == new.len() && old.iter().zip(new).all(|(old, new)| key_image_eq(old, new))
}

pub(super) fn cascade_value(
    state: &Relational, parent: &ColumnDef, child: &ColumnDef, value: &Value,
) -> R<Value> {
    if same_fk_type(state, parent, child) {
        return Ok(value.clone());
    }
    if matches!(value, Value::Numeric(n) if n.scale().is_none())
        && matches!(child.data_type, DataType::SmallInt | DataType::Integer | DataType::BigInt)
    {
        // PostgreSQL reports 0A000 when a cascade casts a special numeric to integer.
        return Err(SqlError::Unsupported);
    }
    let value = if matches!(child.data_type, DataType::Text | DataType::Varchar) {
        key_value(&parent.data_type, value).into_owned()
    } else {
        value.clone()
    };
    let value = query::assignment::convert(value, &parent.data_type, &child.data_type, None)?;
    coerce_to_column(state, child, value)
}

pub(super) fn same_fk_type(state: &Relational, parent: &ColumnDef, child: &ColumnDef) -> bool {
    parent.data_type == child.data_type && state.column_typmod(parent.id) == state.column_typmod(child.id)
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
                query::FkKeyDirection::ToParent,
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
pub(super) fn missing_parent(
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

pub(super) fn check_drop_dependencies(
    state: &Relational, tables: &[&TableDef], target_count: usize, path: &[String],
    views: Option<&BTreeMap<u64, crate::storage::SqlViewDef>>,
) -> R<()> {
    let dropping = tables.iter().map(|table| table.id).collect::<BTreeSet<_>>();
    let display = |schema: &str, name: &str| {
        let key = relation_key(schema, name);
        let visible = state.resolve_relation_key(name, path) == key;
        let name = query::quote_identifier(name);
        if visible { name } else { format!("{}.{name}", query::quote_identifier(schema)) }
    };
    let table_name = |table: &TableDef| display(state.table_schema(table.id), &table.name);
    let mut foreign_keys = BTreeMap::<u64, Vec<_>>::new();
    for (id, constraints) in state.ext.constraints.iter().filter(|(id, _)| !dropping.contains(id)) {
        for constraint in constraints {
            if let ConstraintKind::ForeignKey { ref_table, .. } = &constraint.kind
                && dropping.contains(ref_table)
            {
                foreign_keys.entry(*ref_table).or_default().push((constraint.id, *id, &constraint.name));
            }
        }
    }
    let mut view_children = BTreeMap::<u64, Vec<_>>::new();
    for (&id, view) in views.into_iter().flatten() {
        for dependency in &view.dependencies {
            view_children.entry(dependency.relation).or_default().push(id);
        }
    }
    let mut seen_views = BTreeSet::new();
    let mut dependencies = Vec::new();
    let mut complete = true;
    // PostgreSQL reports a multi-target drop from the last target back.
    for table in tables.iter().rev() {
        let mut roots = view_children.get(&table.id).into_iter().flatten().map(|&id| (id, None))
            .chain(foreign_keys.get(&table.id).into_iter().flatten()
                .map(|&(id, child, name)| (id, Some((child, name)))))
            .collect::<Vec<_>>();
        if roots.is_empty() { continue; }
        roots.sort_unstable_by_key(|&(id, _)| id);
        let parent = format!("table {}", table_name(table));
        let mut remaining = roots.into_iter().rev()
            .map(|(id, fk)| (id, fk, parent.clone())).collect::<Vec<_>>();
        while let Some((id, fk, parent)) = remaining.pop() {
            if let Some((child, name)) = fk {
                let child = table_by_id(state, child)?;
                dependencies.push(format!("constraint {name} on table {} depends on {parent}", table_name(child)));
            } else if seen_views.insert(id) {
                let view = &views.expect("view dependencies come from the view catalog")[&id];
                let Some(schema) = state.routine_schema(view.schema) else { complete = false; continue; };
                let view = format!("view {}", display(schema, &view.name));
                dependencies.push(format!("{view} depends on {parent}"));
                if let Some(children) = view_children.get(&id) {
                    remaining.extend(children.iter().rev().map(|&id| (id, None, view.clone())));
                }
            }
        }
    }
    if dependencies.is_empty() && complete {
        return Ok(());
    }
    let message = if target_count == 1 {
        format!("cannot drop table {} because other objects depend on it", table_name(tables[0]))
    } else {
        "cannot drop desired object(s) because other objects depend on them".into()
    };
    user_types::view_replace_error(message);
    if complete {
        const MAX_CLIENT_DEPENDENCIES: usize = 100;
        let omitted = dependencies.len().saturating_sub(MAX_CLIENT_DEPENDENCIES);
        if omitted != 0 {
            dependencies.truncate(MAX_CLIENT_DEPENDENCIES);
            let plural = if omitted == 1 { "" } else { "s" };
            dependencies.push(format!("and {omitted} other object{plural} (see server log for list)"));
        }
        user_types::sql_error_detail(dependencies.join("\n"));
    } else {
        user_types::exact_sql_error_detail(None);
    }
    user_types::sql_error_hint("Use DROP ... CASCADE to drop the dependent objects too.");
    Err(SqlError::DependentObjects)
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
        validate_kind(state, table, &name, &kind, None, cancel)?;
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
    references: Option<&'a PendingReferences>,
    deferred_children: bool,
}

fn validate_kind(
    state: &Relational,
    table: &TableDef,
    name: &str,
    kind: &ConstraintKind,
    diagnostic: Option<CheckDiagnostics<'_>>,
    cancel: &mut impl FnMut() -> bool,
) -> R<()> {
    for (key, row) in state.rows.get(&table.id).ok_or(SqlError::Type)? {
        if diagnostic.as_ref().is_some_and(|d| {
            !d.changed.contains(key) && d.references.is_none_or(|refs| !refs.contains_key(key))
        }) {
            continue;
        }
        if cancel() {
            return Err(SqlError::Aborted);
        }
        match kind {
            ConstraintKind::Check { expr, .. } => {
                let written = diagnostic.as_ref().map(|d| d.written);
                check_row(state, table, name, expr, row, written)?;
            }
            ConstraintKind::ForeignKey { .. } => {
                let parent_failure = diagnostic.as_ref().and_then(|d| d.references)
                    .and_then(|references| references.get(key))
                    .and_then(PendingReference::parent_values).map(|values| (name, values));
                let child_failure = diagnostic.as_ref().filter(|d| {
                    parent_failure.is_none() && (d.changed.contains(key) || d.deferred_children)
                }).map(|_| name);
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
    validate_kind(state, table, &constraint.name, &constraint.kind, None, cancel)?;
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
pub(super) fn column_has_foreign_key(state: &Relational, table: u64, column: u64) -> bool {
    state.ext.constraints.iter().any(|(&child, constraints)| {
        constraints.iter().any(|constraint| {
            let ConstraintKind::ForeignKey { columns, ref_table, ref_columns, .. } = &constraint.kind
            else {
                return false;
            };
            (child == table && columns.contains(&column))
                || (*ref_table == table && ref_columns.contains(&column))
        })
    })
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
/// Follow an FK postimage back to its statement preimage, even when a direct
/// update or a referential action changes its physical key. None marks an
/// inserted row that was subsequently rewritten by an action.
#[derive(Default)]
pub(super) struct FkPreimages {
    by_version: BTreeMap<(u64, u64), Option<Arc<SqlRow>>>,
    captured_old: BTreeSet<(u64, u64)>,
    cascade_checks: BTreeMap<(u64, u64), BTreeSet<u64>>,
    dml: bool,
}

impl FkPreimages {
    fn record(&mut self, table: u64, old: &SqlRow, new: &SqlRow, original: Option<&SqlRow>) {
        let original = self.by_version.get(&(table, old.version)).cloned()
            .unwrap_or_else(|| original.cloned().map(Arc::new));
        self.by_version.insert((table, new.version), original);
        if let Some(checks) = self.cascade_checks.remove(&(table, old.version)) {
            self.cascade_checks.insert((table, new.version), checks);
        }
    }

    pub(super) fn requires_check(&self, table: u64, key: &[u8], row: &SqlRow,
        before: Option<&super::row_sink::DdlRows>, positions: &[usize], constraint: u64,
    ) -> bool {
        if self.cascade_checks.get(&(table, row.version)).is_some_and(|ids| ids.contains(&constraint)) {
            return true;
        }
        let original = self.by_version.get(&(table, row.version)).map_or_else(
            || before.and_then(|rows| rows.get(key)).filter(|old| {
                // DML records every logical update. A new version without a
                // preimage is an insert, even if it reuses a vacated key.
                old.version == row.version || !self.dml
            }),
            |old| old.as_deref(),
        );
        original.is_none_or(|old| positions.iter().any(|&i| old.values[i] != row.values[i]))
    }
}

type LoadImageRows<'a> = dyn FnMut(&mut Relational, &mut Relational, &FkPreimages) -> R<()> + 'a;

pub(super) struct LoadRows<'a> {
    pub(super) refresh: &'a mut LoadImageRows<'a>,
    pub(super) concurrent_actions: bool,
    pub(super) dml: bool,
}
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
    mut load: Option<LoadRows<'_>>,
) -> R<std::borrow::Cow<'a, Relational>> {
    // DDL may have removed constraints with queued checks or explicit modes.
    deferred.modes.retain(|id, _| state.ext.constraints.values().flatten().any(|c| c.id == *id));
    deferred.pending.retain(|(id, table), _| state.table_constraints(*table).iter().any(|c| c.id == *id));
    // UNIQUE is enforced by its index; it must not activate row-event checks.
    if !state.ext.constraints.values().flatten().any(|c| matches!(c.kind,
        ConstraintKind::Check { .. } | ConstraintKind::ForeignKey { .. })) {
        return Ok(before);
    }
    let type_rewrite = !Arc::ptr_eq(&before.ext, &state.ext) && before.ext.types != state.ext.types;
    if captured.len() > MAX_ROWS {
        return Err(SqlError::Limit);
    }
    let mut preimages = FkPreimages {
        dml: load.as_ref().is_some_and(|loader| loader.dml),
        ..Default::default()
    };
    for event in &captured {
        preimages.captured_old.insert((event.table, event.old.version));
        if let Some(new) = &event.new {
            let table = table_by_id(state, event.table)?;
            let key = pk(table, &event.old.values)?;
            let original = before.rows.get(&event.table).and_then(|rows| rows.get(&key))
                .filter(|original| original.version == event.old.version);
            preimages.record(event.table, &event.old, new, original);
        }
    }
    if let Some(load) = load.as_mut() {
        (load.refresh)(state, before.to_mut(), &preimages)?;
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
                    *keys = std::mem::take(keys).into_iter().filter_map(|(key, reference)| {
                        let old = rows.get(&key)?;
                        by_version.get(&old.version).map(|(new, _)| ((*new).clone(), reference))
                    }).collect();
                }
            }
            continue;
        }
        for (key, old) in rows {
            if cancel() {
                return Err(SqlError::Aborted);
            }
            if preimages.captured_old.contains(&(*id, old.version)) {
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
            if new.is_some_and(|r| row_image_eq(&r.values, &old.values)) {
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
    let mut parent_checks = BTreeMap::<_, PendingReferences>::new();
    while let Some(event) = events.pop_front() {
        if let Some(load) = load.as_mut() {
            (load.refresh)(state, before.to_mut(), &preimages)?;
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
                    && let Some(reference) = keys.remove(&old_pk)
                    && let Some(new_pk) = &new_pk
                {
                    keys.insert(new_pk.clone(), reference);
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
            if old_key.contains(&Value::Null) {
                continue;
            }
            if new_key.as_ref().is_some_and(|key| row_image_eq(&old_key, key)) {
                continue;
            }
            let child = table_by_id(state, child_id)?.clone();
            let positions = id_positions(&child, columns)?;
            let cross_type = ref_positions.iter().zip(&positions).any(|(&p, &c)| {
                !same_fk_type(state, &parent.columns[p], &child.columns[c])
            });
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
                            &prior.name,
                            &prior.kind,
                            Some(CheckDiagnostics {
                                changed: &BTreeSet::new(),
                                written: &[],
                                references: Some(keys),
                                deferred_children: false,
                            }),
                            cancel,
                        )?;
                    }
                }
                return Err(still_referenced(
                    state, &child, &parent, &constraint.name, &ref_positions, &old_key, true,
                ));
            }
            if matches!(action, catalog_ext::Action::NoAction) {
                if !children.is_empty() {
                    let reference = PendingReference::Parent(Arc::from(old_key));
                    let pending = parent_checks.entry((constraint.id, child_id)).or_default();
                    for (key, _) in children {
                        pending.entry(key).or_insert_with(|| reference.clone());
                    }
                }
                continue;
            }
            if matches!(action, catalog_ext::Action::Restrict) {
                continue;
            }
            let recheck_default = *action == catalog_ext::Action::SetDefault && !children.is_empty();
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
                                    cascade_value(
                                        state, &parent.columns[ref_positions[i]], &child.columns[*position],
                                        &new_key.as_ref().ok_or(SqlError::Type)?[i],
                                    )?
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
                        row_check_with_diagnostics(state, &child, &row.values)?;
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
                let next_version = state.next_version;
                // A transient RI image may contain already committed witnesses
                // from a later reservation. Keep this statement's own allocator
                // in its range while validating those preimages.
                let ceiling = if load.as_ref().is_some_and(|loader| loader.concurrent_actions) {
                    let last = state.rows.values().flat_map(BTreeMap::values)
                        .map(|row| row.version).max().unwrap_or(0);
                    next_version.max(last.checked_add(1).ok_or(SqlError::Overflow)?)
                } else {
                    next_version
                };
                state.secondary_apply_batch_engine(state.epoch, &changes, ceiling, &mut *cancel)?;
                state.next_version = next_version;
                if let Some(new) = &new {
                    let original = before.rows.get(&child_id).and_then(|rows| rows.get(&old_pk))
                        .filter(|original| original.version == old.version);
                    preimages.record(child_id, &old, new, original);
                    if *action == Action::Cascade && cross_type {
                        // Assignment can round back to the old child key, which
                        // may no longer reference a parent.
                        preimages.cascade_checks.entry((child_id, new.version)).or_default()
                            .insert(constraint.id);
                    }
                }
                events.push_back(Event {
                    table: child_id,
                    old,
                    new,
                });
            }
            // PostgreSQL runs a parent-side NO ACTION check after SET DEFAULT,
            // including when the resulting reference compares equal to the old one.
            if recheck_default
                && !constraint_lookup::exists(state, &parent, ref_columns, &old_key, cancel)?
                && !constraint_lookup::matching_rows_typed(
                    state, &child, columns, &old_key, &parent_types, cancel,
                )?.is_empty()
            {
                return Err(still_referenced(
                    state, &child, &parent, &constraint.name, &ref_positions, &old_key, false,
                ));
            }
        }
    }
    if let Some(load) = load.as_mut() {
        (load.refresh)(state, before.to_mut(), &preimages)?;
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
                    .filter(|key| preimages.requires_check(*table_id, key, &rows[*key], before_rows, &positions, constraint.id))
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
                if let Some(keys) = parent_checks.remove(&check) {
                    pending.extend(keys);
                }
                pending.extend(changed.into_iter().map(|key| (key, PendingReference::Child)));
            } else {
                let references = parent_checks.get(&(constraint.id, *table_id));
                validate_kind(state, table, &constraint.name, &constraint.kind,
                    Some(CheckDiagnostics {
                        changed: &changed,
                        written,
                        references,
                        deferred_children: false,
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
                    Some(CheckDiagnostics {
                        changed: &BTreeSet::new(),
                        written: &[],
                        references: Some(keys),
                        deferred_children: true,
                    }),
                    &mut cancel,
                )?;
            }
        }
        deferred.pending.clear();
        Ok(())
    }
    pub(super) fn constraint_modes(
        &self, modes: &mut BTreeMap<u64, bool>, names: &[String], path: &[String], is_deferred: bool,
    ) -> R<BTreeSet<u64>> {
        let mut ids = BTreeSet::new();
        let deferrable = |constraint: &Constraint| matches!(
            constraint.kind,
            ConstraintKind::ForeignKey { deferral, .. } if deferral != Deferral::NotDeferrable
        );
        if names.is_empty() {
            ids.extend(self.ext.constraints.values().flatten().filter(|c| deferrable(c)).map(|c| c.id));
        }
        for name in names {
            let (schema, name) = if let Some((schema, name)) = name.split_once('\0') {
                if !matches!(schema, "public" | "pg_catalog" | "information_schema" | "pg_toast")
                    && !self.ext.schemas.contains_key(schema)
                {
                    user_types::view_replace_error(format!(r#"schema "{schema}" does not exist"#));
                    return Err(SqlError::InvalidSchema);
                }
                (Some(schema), name)
            } else {
                (None, name.as_str())
            };
            let mut found = false;
            let implicit_path = if schema.is_some() { &[][..] } else { path };
            for schema in schema.into_iter().chain(implicit_path.iter().map(String::as_str)) {
                for table in self.tables.values().filter(|table| self.table_schema(table.id) == schema) {
                    let reject = || {
                        user_types::view_replace_error(format!(r#"constraint "{name}" is not deferrable"#));
                        Err(SqlError::WrongObjectType)
                    };
                    for constraint in self.table_constraints(table.id).iter().filter(|c| c.name == name) {
                        found = true;
                        if deferrable(constraint) {
                            ids.insert(constraint.id);
                        } else if is_deferred {
                            return reject();
                        }
                    }
                    // Plain primary keys have a synthesized name without an ext record.
                    if self.primary_key_constraint_name(table).as_deref() == Some(name) {
                        found = true;
                        if is_deferred {
                            return reject();
                        }
                    }
                }
                if found { break; }
            }
            if !found {
                user_types::view_replace_error(format!(r#"constraint "{name}" does not exist"#));
                return Err(SqlError::UndefinedObject);
            }
        }
        modes.extend(ids.iter().map(|&id| (id, is_deferred)));
        Ok(ids)
    }

    pub(crate) fn set_constraints(
        &self, deferred: &mut DeferralState, names: &[String], path: &[String],
        is_deferred: bool, mut cancel: impl FnMut() -> bool,
    ) -> R<()> {
        let mut candidate = deferred.clone();
        let ids = self.constraint_modes(&mut candidate.modes, names, path, is_deferred)?;
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
                        Some(CheckDiagnostics {
                            changed: &BTreeSet::new(),
                            written: &[],
                            references: Some(keys),
                            deferred_children: true,
                        }),
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
