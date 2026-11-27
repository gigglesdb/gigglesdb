//! Bounded statement images; all pages share the caller's transaction/savepoint.
use super::*;

fn moves_primary_key(state: &Relational, prepared: &Prepared) -> bool {
    let Some(table) = prepared.table().and_then(|name| state.tables.get(name)) else { return false; };
    let names = match prepared.statement.get() {
        Statement::Update { sets, .. } => sets.iter().map(|(name, _)| name).collect::<Vec<_>>(),
        Statement::UpdateExpressions { sets, .. } => sets.iter().map(|(name, _)| name).collect(),
        _ => return false,
    };
    table.primary_key.iter().any(|i| state.column_written_by(table, &table.columns[*i],
        |name| names.iter().any(|written| *written == name)))
}

pub(super) fn eligible(state: &Relational, prepared: &Prepared) -> bool {
    let Some(table) = prepared.table().and_then(|name| state.tables.get(name)) else {
        return false;
    };
    if state.tables.values().any(|t| state.has_nondeterministic_keys(t)) { return false; }
    let sets = match prepared.statement.get() {
        Statement::Update { sets, .. } => sets.iter().map(|(name, _)| name).collect::<Vec<_>>(),
        Statement::UpdateExpressions { sets, .. } => sets.iter().map(|(name, _)| name).collect(),
        Statement::Delete { .. } => vec![],
        _ => return false,
    };
    // A stable primary key makes the forward cursor Halloween-safe.
    if table
        .primary_key
        .iter()
        .any(|i| state.column_written_by(table, &table.columns[*i],
            |name| sets.iter().any(|written| *written == name)))
    {
        return false;
    }
    // Outgoing references can be checked after all pages. Incoming actions and
    // self references retain their existing executors.
    !state.ext.constraints.iter().any(|(owner, constraints)| {
        constraints.iter().any(|constraint| {
            let ConstraintKind::ForeignKey { columns, ref_table, ref_columns, .. } = &constraint.kind else { return false; };
            let changed = |ids: &[u64]| ids.iter().any(|id| table.columns.iter()
                .any(|column| column.id == *id && state.column_written_by(table, column,
                    |name| sets.iter().any(|written| *written == name))));
            let deleting = matches!(prepared.statement.get(), Statement::Delete { .. });
            (*owner == table.id && !deleting && *ref_table == table.id && changed(columns))
                || (*ref_table == table.id && (deleting || changed(ref_columns)))
        })
    })
}

fn point_keys(state: &Relational, prepared: &Prepared, params: &[Value],
    unique: &[secondary_index::Descriptor],
) -> Option<Vec<Vec<Value>>> {
    let table = state.tables.get(prepared.table()?)?;
    let mut point_keys = if prepared.native_delete.is_some() { native_lock_key(state, prepared, table) } else { searched::target(prepared)
        .and_then(|q| q.mutation_primary_keys(state, table, params)) };
    // Preserve the page executor's RETURNING and UNIQUE error order.
    let returning = match prepared.statement.get() {
        Statement::Update { returning, .. } | Statement::UpdateExpressions { returning, .. }
            => returning.is_some(),
        _ => false,
    };
    if returning && point_keys.as_ref().is_some_and(|keys| keys.len() > 1) {
        point_keys = None;
    }
    if point_keys.as_ref().is_some_and(|keys| keys.len() > 1 || returning && !keys.is_empty()) {
        let changed = match prepared.statement.get() {
            Statement::Update { sets, .. } => sets.iter().map(|(name, _)| name).collect::<Vec<_>>(),
            Statement::UpdateExpressions { sets, .. } => sets.iter().map(|(name, _)| name).collect(),
            _ => Vec::new(),
        };
        if unique.iter().any(|d| d.columns.iter().any(|id| table.columns.iter()
            .any(|c| c.id == *id && state.column_written_by(table, c,
                |name| changed.iter().any(|written| *written == name))))) {
            point_keys = None;
        }
    }
    point_keys
}

// Native batches carry an OR of complete literal keys separately from the
// searched input-key filter. Validate every arm with the single-key classifier
// and deduplicate by the canonical codec; any inexact arm keeps the table lock.
fn native_lock_key(state: &Relational, prepared: &Prepared, table: &TableDef) -> Option<Vec<Vec<Value>>> {
    if let Some(rows) = &prepared.native_delete {
        let mut keys = BTreeMap::new();
        for (key, _) in rows {
            let single = Prepared::delete_engine(table, key.clone(), prepared.domain, state).ok()?;
            let key = native_lock_key(state, &single, table)?.pop()?;
            keys.insert(encode_key(&key).ok()?, key);
        }
        return Some(keys.into_values().collect());
    }
    if searched::target(prepared).is_some() || table.primary_key.is_empty()
        || table.hidden_rowid().is_some() { return None; }
    let predicates = match prepared.statement.get() {
        Statement::Update { predicates, search: None, .. }
        | Statement::Delete { predicates, search: None, .. } => predicates,
        _ => return None,
    };
    if predicates.len() != table.primary_key.len() { return None; }
    let key = table.primary_key.iter().map(|i| {
        let column = &table.columns[*i];
        if !state.deterministic_column(column.id) { return None; }
        let mut matches = predicates.iter().filter(|p| p.column == column.name);
        let Predicate { op, value: Expr::Literal(value), .. } = matches.next()? else { return None; };
        if op != "=" || matches.next().is_some() { return None; }
        query::primary_key_literal_value(value, column)
    }).collect::<Option<Vec<_>>>()?;
    Some(vec![key])
}

/// Statically resolved conflict witness; the caller supplies runtime lock policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Arbiter { PrimaryKey, Unique(u64) }
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UpsertLockPlan { pub(crate) arbiter: Arbiter, pub(crate) rows: usize }

/// Only proposed explicit primary keys and non-NULL UNIQUE target rows.
pub(crate) struct UpsertLockRows {
    pub(crate) primary_keys: Vec<Vec<Value>>,
    pub(crate) unique: Option<(secondary_index::Descriptor, Vec<SqlRow>)>,
}

impl Prepared {
    /// Conservative prelock decision before the new row image is available.
    pub(crate) fn mutation_changes_key(&self, state: &Relational, table: &TableDef,
        indexes: &[secondary_index::Descriptor]) -> bool {
        let names = match self.statement.get() {
            Statement::Delete { .. } => return true,
            Statement::Update { sets, .. } => sets.iter().map(|(name, _)| name).collect::<Vec<_>>(),
            Statement::UpdateExpressions { sets, .. } => sets.iter().map(|(name, _)| name).collect(),
            _ => return false,
        };
        table.columns.iter().enumerate().any(|(i, c)| state.column_written_by(table, c,
            |name| names.iter().any(|written| *written == name))
            && (table.primary_key.contains(&i) || indexes.iter().any(|d|
                d.table == table.id && d.unique && d.columns.contains(&c.id))))
    }

    pub(crate) fn upsert_lock_rows(&self, state: &Relational,
        indexes: &[secondary_index::Descriptor], params: &[Value],
        clock: Option<&clock::ExecutionClock>,
    ) -> Option<UpsertLockRows> {
        let plan = self.upsert_lock_plan(state, indexes)?;
        let Statement::InsertMulti(insert) = self.statement.get() else { return None; };
        let table = state.tables.get(&insert.table)?;
        if state.has_generated_columns(table) { return None; }
        let descriptor = match plan.arbiter {
            Arbiter::PrimaryKey => None,
            Arbiter::Unique(id) => Some(indexes.iter().find(|d| d.id == id)?.clone()),
        };
        let target = descriptor.as_ref().map(|d| d.columns.clone()).unwrap_or_else(||
            table.primary_key.iter().map(|i| table.columns[*i].id).collect());
        let rows = insert.lock_values(state, &target, params, &self.parameter_types, clock)?;
        let mut primary_keys = Vec::new();
        let mut witnesses = Vec::new();
        for row in rows {
            if let Some(key) = table.primary_key.iter().map(|i| row[*i].clone())
                .collect::<Option<Vec<_>>>() {
                if key.contains(&Value::Null) { return None; }
                primary_keys.push(key);
            }
            if descriptor.is_some() && !table.columns.iter().enumerate()
                .any(|(i, c)| target.contains(&c.id) && row[i] == Some(Value::Null)) {
                witnesses.push(SqlRow { values: row.into_iter()
                    .map(|v| v.unwrap_or(Value::Null)).collect(), version: 0 });
            }
        }
        Some(UpsertLockRows { primary_keys, unique: descriptor.map(|d| (d, witnesses)) })
    }

    /// Conservative static admission only; no isolation or storage decisions.
    pub(crate) fn upsert_lock_plan(&self, state: &Relational,
        indexes: &[secondary_index::Descriptor]) -> Option<UpsertLockPlan> {
        let Statement::InsertMulti(insert) = self.statement.get() else { return None; };
        if insert.source.is_some() || insert.row_count() > paged_insert::SHARED_WRITE_ROWS { return None; }
        let sql_insert::LockConflict { columns, constraint, sets } = insert.lock_conflict()?;
        let table = state.tables.get(&insert.table)?;
        if state.has_nondeterministic_keys(table) { return None; }
        let primary = table.primary_key.iter().map(|i| table.columns[*i].id).collect::<BTreeSet<_>>();
        let unique = indexes.iter().filter(|d| d.table == table.id && d.unique).collect::<Vec<_>>();
        if unique.iter().any(|d| d.predicate.is_some()) { return None; }
        let target: BTreeSet<u64> = if let Some(names) = columns {
            let ids = names.iter().map(|name| table.columns.iter().find(|c| &c.name == name).map(|c| c.id))
                .collect::<Option<BTreeSet<_>>>()?;
            if ids.len() != names.len() { return None; }
            ids
        } else {
            let name = constraint?;
            if state.primary_key_constraint_name(table).as_deref() == Some(name) { primary.clone() }
            else { unique.iter().find(|d| split_key(&d.name).1 == name)?.columns.iter().copied().collect() }
        };
        let matches = unique.iter().filter(|d| d.columns.iter().copied().collect::<BTreeSet<_>>() == target)
            .copied().collect::<Vec<_>>();
        let arbiter = if target == primary { Arbiter::PrimaryKey }
            else if matches.len() == 1 { Arbiter::Unique(matches[0].id) } else { return None; };
        if unique.iter().any(|d| !matches!(arbiter, Arbiter::Unique(id) if id == d.id)) { return None; }
        let changed = |ids: &[u64]| ids.iter().any(|id| table.columns.iter()
            .any(|c| c.id == *id && sets.contains(&c.name.as_str())));
        if changed(&primary.iter().copied().collect::<Vec<_>>())
            || unique.iter().any(|d| changed(&d.columns)) { return None; }
        for (owner, constraints) in &state.ext.constraints {
            for c in constraints {
                let ConstraintKind::ForeignKey { columns, ref_table, ref_columns, deferral, .. } = &c.kind else { continue; };
                if *owner != table.id && *ref_table != table.id { continue; }
                if owner == ref_table || *deferral != Deferral::NotDeferrable
                    || *owner == table.id && changed(columns)
                    || *ref_table == table.id && changed(ref_columns) { return None; }
            }
        }
        // The bounded default grammar exposes sequence dependencies only for
        // nextval, optionally wrapped in unary negation or casts. Other defaults
        // require an explicitly supplied PK, except these exact UUID generators.
        if table.primary_key.iter().any(|i| {
            let column = &table.columns[*i];
            !insert.lock_supplies(&column.name) && defaults::default_sql(state, column)
                .is_none_or(|sql| !defaults::dependencies(&sql).is_ok_and(|names| !names.is_empty())
                    && !matches!(sql.trim(), "gen_random_uuid()" | "pg_catalog.gen_random_uuid()"))
        }) { return None; }
        Some(UpsertLockPlan { arbiter, rows: insert.row_count() })
    }

    /// Bounded point candidates, excluding all UNIQUE-changing updates.
    /// The storage bridge must also check its data-dependent retention fallback.
    pub(crate) fn mutation_lock_keys(&self, state: &Relational, params: &[Value],
        indexes: &[secondary_index::Descriptor],
    ) -> Option<Vec<Vec<Value>>> {
        if !eligible(state, self) { return None; }
        let table = state.tables.get(self.table()?)?;
        let unique = indexes.iter().filter(|d| d.table == table.id && d.unique).cloned().collect::<Vec<_>>();
        let changed = match self.statement.get() {
            Statement::Update { sets, .. } => sets.iter().map(|(name, _)| name).collect::<Vec<_>>(),
            Statement::UpdateExpressions { sets, .. } => sets.iter().map(|(name, _)| name).collect(),
            _ => Vec::new(),
        };
        if unique.iter().any(|d| d.columns.iter().any(|id| table.columns.iter()
            .any(|c| c.id == *id && changed.contains(&&c.name)))) { return None; }
        if self.native_delete.is_some() || searched::target(self).is_none() { native_lock_key(state, self, table) }
        else { point_keys(state, self, params, &unique) }
    }

    /// Incoming-FK DELETE prelocks, independent of publisher eligibility.
    pub(crate) fn fk_delete_lock_keys(&self, state: &Relational, params: &[Value]) -> Option<Vec<Vec<Value>>> {
        if !matches!(self.statement.get(), Statement::Delete { .. }) { return None; }
        let table = state.tables.get(self.table()?)?;
        let mut component = BTreeSet::from([table.id]);
        loop {
            let before = component.len();
            for (owner, constraints) in &state.ext.constraints {
                for c in constraints {
                    if let ConstraintKind::ForeignKey { ref_table, .. } = &c.kind
                        && (component.contains(owner) || component.contains(ref_table)) {
                        component.insert(*owner); component.insert(*ref_table);
                    }
                }
            }
            if component.len() == before { break; }
        }
        let mut edges = BTreeMap::<u64, BTreeSet<u64>>::new();
        let mut incoming = false;
        for (owner, constraints) in &state.ext.constraints {
            if !component.contains(owner) { continue; }
            for c in constraints {
                if let ConstraintKind::ForeignKey { ref_table, deferral, on_delete, .. } = &c.kind {
                    if *deferral != Deferral::NotDeferrable || owner == ref_table { return None; }
                    if *ref_table == table.id {
                        incoming = true;
                        if matches!(on_delete, catalog_ext::Action::SetDefault) { return None; }
                    }
                    edges.entry(*owner).or_default().insert(*ref_table);
                }
            }
        }
        // Remove sinks repeatedly: a remaining directed graph contains a cycle.
        while !edges.is_empty() {
            let sinks = edges.iter().filter(|(_, targets)| targets.iter().all(|t| !edges.contains_key(t)))
                .map(|(id, _)| *id).collect::<Vec<_>>();
            if sinks.is_empty() { return None; }
            for id in sinks { edges.remove(&id); }
        }
        if !incoming { return None; }
        self.delete_point_keys(state, params, table)
    }

    /// A point cascade may prelock its parent even if a descendant is deferred
    /// or the graph has a cycle. SET DEFAULT retains its existing admission.
    pub(crate) fn cascade_delete_point_keys(&self, state: &Relational, params: &[Value]) -> Option<Vec<Vec<Value>>> {
        if !matches!(self.statement.get(), Statement::Delete { .. }) { return None; }
        let table = state.tables.get(self.table()?)?;
        if state.ext.constraints.values().flatten().any(|constraint| {
            matches!(constraint.kind, ConstraintKind::ForeignKey { ref_table, on_delete: catalog_ext::Action::SetDefault, .. }
                if ref_table == table.id)
        }) { return None; }
        self.delete_point_keys(state, params, table)
    }

    fn delete_point_keys(&self, state: &Relational, params: &[Value], table: &TableDef) -> Option<Vec<Vec<Value>>> {
        let keys = if self.native_delete.is_some() { native_lock_key(state, self, table)? }
        else if let Some(query) = searched::target(self) {
            query.mutation_primary_keys(state, table, params)?
        } else { native_lock_key(state, self, table)? };
        // Native targets were already admitted by the request budget. Keep the
        // SQL cap; both paths still prelock parents in physical order and use
        // the storage bridge's retention upgrade and existing child locks.
        (self.native_delete.is_some() || keys.len() <= 200).then_some(keys)
    }

    /// Plain VALUES appends use physical latest-owner PK/UNIQUE checks.
    pub(crate) fn plain_values_append(&self, state: &Relational) -> bool {
        self.plain_values_append_with_local_cascade(state, false)
    }

    pub(crate) fn plain_values_append_with_local_cascade(&self, state: &Relational, local: bool) -> bool {
        let Some(table) = self.table().and_then(|name| state.tables.get(name)) else { return false; };
        if state.has_nondeterministic_keys(table) { return false; }
        if state.ext.constraints.get(&table.id).into_iter().flatten().any(|c| {
            matches!(&c.kind, ConstraintKind::ForeignKey { ref_table, on_delete, .. }
                if *ref_table == table.id && !(local && matches!(on_delete,
                    catalog_ext::Action::Cascade | catalog_ext::Action::SetNull)))
        }) { return false; }
        matches!(self.statement.get(), Statement::Insert { .. } | Statement::InsertExpressions(_))
            || matches!(self.statement.get(), Statement::InsertMulti(i) if i.append_only() && i.source.is_none() && (self.native_insert_many || i.row_count() <= paged_insert::SHARED_WRITE_ROWS))
    }
}

pub(super) fn execute(
    state: &mut Relational,
    description: &StatementDescription<'_>,
    params: &[Value],
    clock: Option<&clock::ExecutionClock>,
    sink: &mut impl RowSink,
    version_end: u64,
    deferred: Option<&mut DeferralState>,
) -> Result<SqlResult, CoreError> {
    let prepared = description.prepared();
    let table = state.tables[prepared.table().expect("mutation target")].clone();
    // Analyze even an empty table before opening the cursor. The outer
    // statement rolls back all pages, including failures on the final page.
    let mut result = state
        .execute_statement_described(
            description,
            params,
            None,
            || crate::cancellation::check().is_err(),
            clock,
            None,
            &mut Vec::new(),
        )
        .map_err(CoreError::Sql)?;
    let (template, keys) = selection(state, description, params, clock, sink)?;
    let page_description = StatementDescription::new(&template);
    let unique = sink.indexes().into_iter().filter(|d| d.table == table.id && d.unique).collect::<Vec<_>>();
    let description = page_memo(state, &unique, keys.is_some(), description, &page_description);
    let mut pending = deferred.as_deref().cloned().unwrap_or_default();
    let mut immediate = BTreeMap::<u64, VersionBits>::new();
    let first_version = state.next_version;
    let mut checks = pending.pending_retention().map_err(CoreError::Sql)?;
    let mut output = spill::RowBuffer::new(std::mem::take(&mut result.rows))?;
    let point_keys = (keys.is_none() || prepared.native_delete.is_some())
        .then(|| point_keys(state, prepared, params, &unique)).flatten();
    // External UNIQUE witnesses are checked after each old page. Combining keys
    // can mask that error with a later CHECK, and even one row's RETURNING can
    // mask a page-local duplicate. Preserve the old path in those cases.
    sink.mutation_pages(&table, point_keys.as_deref(), &mut |mut rows| {
        let mut unchanged = DdlRows::new();
        if let Some(keys) = &keys {
            rows.retain(|key, row| {
                if keys.contains(key) { true } else { unchanged.insert(key.clone(), row.clone()); false }
            });
        }
        let mut page = state.clone();
        let before = rows.clone();
        page.rows.insert(table.id, rows);
        page.suppress_foreign_keys();
        // Retain only page-local UNIQUE witnesses for exact immediate error
        // order. Non-unique entries are maintained solely by the physical sink.
        page.secondary = page.secondary_build_complete_engine(&unique,
            || crate::cancellation::check().is_err()).map_err(CoreError::Sql)?;
        let batch = page
            .execute_statement_described(
                description,
                params,
                None,
                || crate::cancellation::check().is_err(),
                clock,
                None,
                &mut Vec::new(),
            )
            .map_err(CoreError::Sql)?;
        if page.next_version > version_end {
            return Err(CoreError::Sql(SqlError::Limit));
        }
        state.next_version = page.next_version;
        result.affected += batch.affected;
        output.extend(batch.rows)?;
        for (key, new) in &page.rows[&table.id] {
            if pending.replica { continue; }
            for c in state.ext.constraints.get(&table.id).into_iter().flatten() {
                let ConstraintKind::ForeignKey { columns, deferral, .. } = &c.kind else { continue; };
                if !columns.iter().any(|id| {
                    let i = table.columns.iter().position(|column| column.id == *id).unwrap();
                    new.values[i] != before[key].values[i]
                }) { continue; }
                let deferred = *deferral != Deferral::NotDeferrable && pending.modes.get(&c.id)
                    .copied().unwrap_or(*deferral == Deferral::DeferrableDeferred);
                if deferred {
                    constraints::record_pending(&mut checks, pending.pending.entry((c.id, table.id)).or_default(),
                        key.clone(), constraints::PendingReference::Child).map_err(CoreError::Sql)?;
                } else {
                    // These pages have stable primary keys and no incoming actions.
                    // Mark postimage versions without retaining every check key.
                    immediate.entry(c.id).or_default().insert(new.version - first_version, &mut checks)
                        .map_err(CoreError::Sql)?;
                }
            }
        }
        unchanged.extend(page.rows.remove(&table.id).unwrap_or_default());
        Ok(unchanged)
    })?;
    if !immediate.is_empty() {
        constraint_rows::check_pending_pages(state, &table, sink, |rows| {
            let mut page = DeferralState::default();
            for (&id, versions) in &immediate {
                let keys = rows.iter().filter(|(_, row)| row.version.checked_sub(first_version)
                    .is_some_and(|offset| versions.contains(offset)))
                    .map(|(key, _)| (key.clone(), constraints::PendingReference::Child))
                    .collect::<constraints::PendingReferences>();
                if !keys.is_empty() {
                    page.pending.insert((id, table.id), keys);
                }
            }
            page
        })?;
    }
    if deferred.is_none() { paged_pending(state, &mut pending, &[], &[], false, sink)?; }
    if let Some(deferred) = deferred { *deferred = pending; }
    output.finish(&mut result)?;
    Ok(result)
}

// Complete predicate evaluation precedes publication, including self-subqueries.
type MutationSelection<'a> = (std::borrow::Cow<'a, Prepared>, Option<BTreeSet<Vec<u8>>>);
fn selection<'a>(state: &Relational, description: &StatementDescription<'a>, params: &[Value],
    clock: Option<&clock::ExecutionClock>, sink: &mut impl RowSink,
) -> Result<MutationSelection<'a>, CoreError> {
    selection_from(state, description, params, clock, sink, None)
}

fn selection_from<'a>(
    state: &Relational, description: &StatementDescription<'a>, params: &[Value],
    clock: Option<&clock::ExecutionClock>, sink: &mut impl RowSink,
    source: Option<&dyn scan::RowSource>,
) -> Result<MutationSelection<'a>, CoreError> {
    let prepared = description.prepared();
    let supplied = searched::target(prepared).and_then(query::Query::mutation_input_keys);
    let keys = if supplied.is_some() { supplied } else { searched::target(prepared).filter(|q| moves_primary_key(state, prepared) || q.has_in_subquery())
        .map(|_| scan::with_cursor_limit(scan::WRITE_SOURCE_BYTES, || {
            if let Some(source) = source {
                state.mutation_keys(description, params, clock, source).map_err(CoreError::Sql)
            } else {
                sink.mutation_keys(state, description, params, clock)
            }
        })).transpose()? };
    let template = if keys.is_some() {
        let mut template = prepared.clone();
        template.parameter_types = description.input().describe(state).map_err(CoreError::Sql)?
            .parameters.iter().cloned().map(Some).collect();
        template.unbound_statement = None;
        match template.statement.get_mut() {
            Statement::Update { predicates, search, .. }
            | Statement::UpdateExpressions { predicates, search, .. }
            | Statement::Delete { predicates, search, .. } => {
                predicates.clear();
                search.as_mut().expect("mutation target").clear_mutation_filter();
            }
            _ => unreachable!(),
        }
        std::borrow::Cow::Owned(template)
    } else {
        std::borrow::Cow::Borrowed(prepared)
    };
    Ok((template, keys))
}

// Only index names can change binding between preflight and a page. An index
// on the search path can shadow an unqualified relation.
fn page_memo<'a, 'p>(
    state: &Relational,
    page_indexes: &[secondary_index::Descriptor],
    rewritten: bool,
    preflight: &'a StatementDescription<'p>,
    page: &'a StatementDescription<'p>,
) -> &'a StatementDescription<'p> {
    if rewritten {
        return page;
    }
    fn relevant_names<'d>(
        indexes: &'d [secondary_index::Descriptor],
        prepared: &Prepared,
    ) -> BTreeSet<&'d str> {
        indexes
            .iter()
            .filter(|index| {
                let (schema, name) = split_key(&index.name);
                prepared.search_path.iter().any(|s| s == schema)
                    && prepared
                        .relations
                        .iter()
                        .any(|r| r.schema.is_none() && r.name == name)
            })
            .map(|index| index.name.as_str())
            .collect()
    }

    let prepared = preflight.prepared();
    if relevant_names(state.secondary.descriptors(), prepared)
        == relevant_names(page_indexes, prepared)
    {
        preflight
    } else {
        page
    }
}

// PK moves use the same bounded publisher and referential-action queue.
pub(super) fn cascade_eligible(state: &Relational, prepared: &Prepared) -> bool {
    if eligible(state, prepared)
        || !matches!(prepared.statement.get(), Statement::Delete { .. } | Statement::Update { .. } | Statement::UpdateExpressions { .. })
    {
        return false;
    }
    let Some(target) = prepared.table().and_then(|name| state.tables.get(name)) else { return false; };
    if state.tables.values().any(|t| state.has_nondeterministic_keys(t)) { return false; }
    let mut connected = BTreeSet::from([(target.id, !matches!(prepared.statement.get(), Statement::Delete { .. }))]);
    let pk_move = moves_primary_key(state, prepared);
    let mut paged = pk_move;
    loop {
        let count = connected.len();
        for (child_id, cs) in &state.ext.constraints {
            for c in cs {
                let ConstraintKind::ForeignKey { ref_table, columns, ref_columns, on_delete, on_update, .. } = &c.kind else { continue; };
                let events = connected.iter().filter(|(id, _)| id == ref_table).copied().collect::<Vec<_>>();
                for (_, update) in events {
                    let action = if update { on_update } else { on_delete };
                    // PK moves already support cross-type NO ACTION probes.
                    if *action == catalog_ext::Action::NoAction && pk_move { continue; }
                    let child = state.tables.values().find(|t| t.id == *child_id).unwrap();
                    let parent = state.tables.values().find(|t| t.id == *ref_table).unwrap();
                    if columns.iter().zip(ref_columns).any(|(a, b)| {
                        let ty = &child.columns.iter().find(|c| c.id == *a).unwrap().data_type;
                        ty != &parent.columns.iter().find(|c| c.id == *b).unwrap().data_type
                            || !matches!(ty, DataType::Boolean | DataType::SmallInt | DataType::Integer
                                | DataType::BigInt | DataType::Text | DataType::Varchar | DataType::Bytes
                                | DataType::Uuid)
                    }) { return false; }
                    paged = true;
                    if matches!(action, catalog_ext::Action::Cascade | catalog_ext::Action::SetNull | catalog_ext::Action::SetDefault) {
                        connected.insert((*child_id, update || *action != catalog_ext::Action::Cascade));
                    }
                }
            }
        }
        if count == connected.len() { return paged; }
    }
}

pub(super) fn cascade(
    state: &mut Relational, description: &StatementDescription<'_>, params: &[Value],
    clock: Option<&clock::ExecutionClock>, sink: &mut impl RowSink,
    deferred: Option<&mut DeferralState>, version_end: u64,
) -> Result<SqlResult, CoreError> {
    let mut work = ActionWork::new(deferred.as_deref().cloned().unwrap_or_default())?;
    work.check_parent_in_trigger_order = moves_primary_key(state, description.prepared());
    let result = cascade_pages(state, description, params, clock, sink, version_end, &mut work, None)?;
    work.drain(state, sink, clock, version_end, false, true)?;
    work.check_removed_keys(state, sink)?;
    paged_pending(state, &mut work.immediate, &[], &[], false, sink)?;
    if deferred.is_none() { paged_pending(state, &mut work.pending, &[], &[], false, sink)?; }
    if let Some(d) = deferred { *d = work.pending; }
    Ok(result)
}

#[expect(clippy::too_many_arguments, reason = "shared DML publication and optional CTE input")]
pub(super) fn cascade_pages(
    state: &mut Relational, description: &StatementDescription<'_>, params: &[Value],
    clock: Option<&clock::ExecutionClock>, sink: &mut impl RowSink,
    version_end: u64, work: &mut ActionWork, cte: Option<(&dyn scan::RowSource, u64)>,
) -> Result<SqlResult, CoreError> {
    let prepared = description.prepared();
    let pk_move = moves_primary_key(state, prepared);
    let table = state.tables[prepared.table().unwrap()].clone();
    let descriptors = sink.indexes();
    let mut result = state.execute_statement_described(description, params, None,
        || crate::cancellation::check().is_err(), clock, None, &mut Vec::new()).map_err(CoreError::Sql)?;
    let (template, keys) = selection_from(state, description, params, clock, sink, cte.map(|(source, _)| source))?;
    let page_description = StatementDescription::new(&template);
    let description = page_memo(state, &descriptors, keys.is_some(), description, &page_description);
    let prepared = description.prepared();
    let first_version = cte.map_or(state.next_version, |(_, floor)| floor);
    let mut output = spill::RowBuffer::new(std::mem::take(&mut result.rows))?;
    // Native input keys remain present after selection clears the filter. Use
    // the same canonical, deduplicated keys as prelocking, without its SQL cap.
    // The sink reads the post-lock statement snapshot; retain no cross-call image.
    let candidates = if !pk_move && (prepared.native_delete.is_some() || keys.is_none()) {
        let point_keys = if prepared.native_delete.is_some() {
            native_lock_key(state, prepared, &table)
        } else {
            searched::target(prepared).and_then(|q| q.mutation_primary_keys(state, &table, params))
        };
        point_keys
            .map(|keys| sink.mutation_candidates(&table, &keys)).transpose()?.flatten()
    } else { None };
    let point = candidates.is_some();
    let mut candidates = candidates;
    let mut after = None;
    loop {
        let mut rows = if point {
            let Some(rows) = candidates.take() else { break; };
            rows
        } else {
            let Some((last, rows)) = sink.read_page(&table, after)? else { break; };
            after = Some(last);
            rows
        };
        // Own writes can move a primary key ahead of the physical cursor.
        rows.retain(|key, row| row.version < first_version && keys.as_ref().is_none_or(|keys| keys.contains(key)));
        // Immediate PK/UNIQUE checks precede evaluation of the next logical row.
        // Single-row publication also sees keys vacated by earlier rows/pages.
        let batches = if pk_move {
            rows.into_iter().map(|entry| DdlRows::from([entry])).collect::<Vec<_>>()
        } else { vec![rows] };
        for rows in batches {
            let mut page = state.clone();
            page.rows.insert(table.id, rows.clone());
            page.suppress_foreign_keys();
            rebuild(&mut page, &descriptors)?;
            let mut events = Vec::new();
            let batch = page.execute_statement_described(description, params, None,
                || crate::cancellation::check().is_err(), clock, None, &mut events).map_err(CoreError::Sql)?;
            result.affected += batch.affected;
            output.extend(batch.rows)?;
            state.next_version = page.next_version;
            work.publish(state, sink, &table, rows, &page.rows[&table.id], events, None, &descriptors, version_end)?;
        }
    }
    output.finish(&mut result)?;
    Ok(result)
}


/// A bit per reserved version, allocated only when that version needs a mark.
#[derive(Default)]
struct VersionBits(Vec<u64>);

impl VersionBits {
    fn contains(&self, offset: u64) -> bool {
        self.0.get((offset / 64) as usize)
            .is_some_and(|word| word & (1 << (offset % 64)) != 0)
    }

    fn insert(&mut self, offset: u64, retention: &mut scan::Retention) -> R<()> {
        let index = (offset / 64) as usize;
        if index >= self.0.capacity() {
            let capacity = (index + 1).next_power_of_two();
            retention.charge((capacity - self.0.capacity()) * size_of::<u64>())?;
            self.0.reserve_exact(capacity - self.0.len());
        }
        if index >= self.0.len() {
            self.0.resize(index + 1, 0);
        }
        self.0[index] |= 1 << (offset % 64);
        Ok(())
    }
}

/// Distinguish existing-row postimages from inserts in the statement's version
/// range. Track changed references by version until all pages and actions can
/// supply witnesses, including forward self-references.
pub(super) struct UpsertChecks {
    table: u64,
    versions: std::ops::Range<u64>,
    updates: VersionBits,
    references: BTreeMap<u64, VersionBits>,
    has_inserts: bool,
}

impl UpsertChecks {
    pub(super) fn new(table: u64, versions: std::ops::Range<u64>) -> Self {
        Self { table, versions, updates: VersionBits::default(),
            references: BTreeMap::new(), has_inserts: false }
    }

    pub(super) fn is_insert(&self, version: u64) -> bool {
        self.versions.contains(&version) && !self.updates.contains(version - self.versions.start)
    }

    pub(super) fn has_inserts(&self) -> bool {
        self.has_inserts
    }

    fn record_update(&mut self, old: u64, new: u64, retention: &mut scan::Retention) -> R<bool> {
        if self.is_insert(old) { return Ok(false); }
        let offset = new - self.versions.start;
        self.updates.insert(offset, retention)?;
        if self.versions.contains(&old) {
            for versions in self.references.values_mut() {
                if versions.contains(old - self.versions.start) {
                    versions.insert(offset, retention)?;
                }
            }
        }
        Ok(true)
    }

    fn record_reference(&mut self, constraint: u64, version: u64, retention: &mut scan::Retention) -> R<()> {
        self.references.entry(constraint).or_default().insert(version - self.versions.start, retention)
    }

    fn needs_reference_check(&self, versions: &VersionBits, version: u64) -> bool {
        self.versions.contains(&version) && versions.contains(version - self.versions.start)
    }
}

/// A statement-end NO ACTION check tracks the referenced key. Cascades
/// can delete or rekey every original child witness before this check runs.
#[derive(serde::Serialize, serde::Deserialize)]
struct RemovedKey {
    child: u64,
    constraint: u64,
    values: Vec<Value>,
}

type ActionEvent = (u64, SqlRow, Option<SqlRow>);

fn action_bytes(old: &SqlRow, new: Option<&SqlRow>) -> R<usize> {
    let old = searched::retained_row_bytes(&[], old)?;
    let new = new.map(|row| searched::retained_row_bytes(&[], row)).transpose()?.unwrap_or(0);
    old.checked_add(new).and_then(|bytes| bytes.checked_add(size_of::<ActionEvent>()))
        .ok_or(SqlError::Limit)
}

// Row versions of bulk writes tend to be contiguous. Sparse blocks also
// handle old versions spread across unrelated writes without reserving gaps.
const VISITED_VERSIONS: u64 = u64::BITS as u64;
const VISITED_BLOCK_BYTES: usize = size_of::<((u64, u64), u64)>() + 40;

struct IncomingKey {
    parent_positions: Vec<usize>,
    child_positions: Vec<usize>,
    equality: collation_keys::Key,
    tuple: Option<Vec<u8>>,
    probe: Option<Vec<Value>>,
}

impl IncomingKey {
    fn new(
        state: &Relational, parent: &TableDef, child: &TableDef, ref_columns: &[u64],
        parent_positions: Vec<usize>, child_positions: Vec<usize>, values: &[Value],
    ) -> R<Option<Self>> {
        let equality = collation_keys::Key::new(state, parent, ref_columns, None)?;
        let tuple = equality.values(values)?;
        let positions = || parent_positions.iter().zip(&child_positions);
        let probe = if !equality.nondeterministic() && positions().all(|(&p, &c)| {
            query::constraint_key_supported(&child.columns[c].data_type, &parent.columns[p].data_type, query::FkKeyDirection::ToChild)
        }) {
            let converted = positions().zip(values).map(|((&p, &c), value)| {
                query::constraint_key(value, &child.columns[c].data_type, &parent.columns[p].data_type, query::FkKeyDirection::ToChild)
            }).collect::<R<Option<Vec<_>>>>()?;
            let Some(converted) = converted else { return Ok(None); };
            Some(converted)
        } else {
            None
        };
        Ok(Some(Self { parent_positions, child_positions, equality, tuple, probe }))
    }

    fn matches(&self, parent: &TableDef, child: &TableDef, row: &SqlRow) -> R<bool> {
        let converted = self.child_positions.iter().zip(&self.parent_positions).map(|(&a, &b)| {
            query::constraint_key(&row.values[a], &child.columns[a].data_type,
                &parent.columns[b].data_type, query::FkKeyDirection::ToParent)
        }).collect::<R<Option<Vec<_>>>>()?;
        Ok(converted.as_ref().map(|v| self.equality.values(v)).transpose()?.flatten() == self.tuple)
    }

    fn child_exists(
        &self, sink: &mut impl RowSink, parent: &TableDef, child: &TableDef, columns: &[u64],
    ) -> Result<bool, CoreError> {
        let mut after = None;
        loop {
            let probe = self.probe.as_deref().map(|values| (columns, values));
            let page = sink.fk_action_page(child, probe, after)?;
            let Some((last, rows)) = page else { return Ok(false); };
            after = Some(last);
            for row in rows.values() {
                if self.probe.is_some() || self.matches(parent, child, row).map_err(CoreError::Sql)? {
                    return Ok(true);
                }
            }
        }
    }
}

pub(in crate::relational) struct ActionWork {
    track_inserts: bool,
    queue: spill::Replay<ActionEvent>,
    visited: BTreeMap<(u64, u64), u64>,
    retention: scan::Retention,
    pending_retention: scan::Retention,
    pending: DeferralState,
    immediate: DeferralState,
    removed_keys: spill::Replay<RemovedKey>,
    check_parent_in_trigger_order: bool,
    pub(super) upsert: Option<UpsertChecks>,
}
impl ActionWork {
    pub(in crate::relational) fn new(pending: DeferralState) -> Result<Self, CoreError> {
        let pending_retention = pending.pending_retention().map_err(CoreError::Sql)?;
        Ok(Self { queue: spill::Replay::default(), visited: BTreeMap::new(),
            retention: scan::Retention::new(scan::retention_limit().min(scan::MAX_RETAINED_BYTES)), pending_retention,
            pending, immediate: DeferralState::default(), removed_keys: spill::Replay::default(),
            track_inserts: false, check_parent_in_trigger_order: false, upsert: None })
    }

    pub(in crate::relational) fn track_inserts(&mut self) {
        self.track_inserts = true;
    }

    pub(in crate::relational) fn complete(
        mut self, state: &mut Relational, sink: &mut impl RowSink,
        clock: Option<&clock::ExecutionClock>, version_end: u64,
        deferred: Option<&mut DeferralState>,
    ) -> Result<(), CoreError> {
        // All direct mutations are visible now. Evaluate immediate NO ACTION
        // in trigger order relative to later CASCADE and SET actions.
        self.check_parent_in_trigger_order = true;
        // MERGE and writable CTEs share this queue across direct writes.
        // Use per-row probes for their combined trigger queue.
        self.drain(state, sink, clock, version_end, false, false)?;
        self.check_removed_keys(state, sink)?;
        self.finish(state, sink, deferred)?;
        Ok(())
    }

    fn mark_visited(&mut self, table: u64, version: u64) -> R<bool> {
        use std::collections::btree_map::Entry;
        let word = match self.visited.entry((table, version / VISITED_VERSIONS)) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                self.retention.charge(VISITED_BLOCK_BYTES)?;
                entry.insert(0)
            }
        };
        let bit = 1 << (version % VISITED_VERSIONS);
        let first = *word & bit == 0;
        *word |= bit;
        Ok(first)
    }

    pub(super) fn finish(mut self, state: &Relational, sink: &mut impl RowSink,
        deferred: Option<&mut DeferralState>,
    ) -> Result<Option<UpsertChecks>, CoreError> {
        let updates = self.upsert.as_ref().map(|upsert| (upsert, &mut self.pending, &mut self.pending_retention));
        Self::validate_pending(state, sink, &self.immediate, updates)?;
        if let Some(deferred) = deferred { *deferred = self.pending; }
        else { Self::validate_pending(state, sink, &self.pending, None)?; }
        Ok(self.upsert)
    }

    // Outgoing upsert checks select final rows by version. Build their physical
    // keys one page at a time, retaining only transaction-deferred obligations.
    fn validate_pending(state: &Relational, sink: &mut impl RowSink, pending: &DeferralState,
        mut updates: Option<(&UpsertChecks, &mut DeferralState, &mut scan::Retention)>,
    ) -> Result<(), CoreError> {
        for table in state.tables.values() {
            let upsert = updates.as_ref().map(|(upsert, ..)| *upsert).filter(|upsert| upsert.table == table.id);
            let checks = state.table_constraints(table.id).iter().filter_map(|c| {
                let check = (c.id, table.id);
                let keys = pending.pending.get(&check).filter(|keys| !keys.is_empty());
                let versions = upsert.and_then(|upsert| Some((upsert, upsert.references.get(&c.id)?)));
                (keys.is_some() || versions.is_some()).then_some((c, check, keys, versions))
            }).collect::<Vec<_>>();
            if checks.is_empty() { continue; }
            let mut after = None;
            while let Some((last, rows)) = sink.fk_read_page(table, after.take())? {
                after = Some(last);
                for &(c, check, keys, versions) in &checks {
                    let matches_version = |row: &SqlRow| versions.is_some_and(|(upsert, versions)|
                        upsert.needs_reference_check(versions, row.version));
                    let deferred = if versions.is_some()
                        && let Some((_, deferred, retention)) = &mut updates
                        && deferred.deferred(c.id, &c.kind)
                    {
                        let keys = deferred.pending.entry(check).or_default();
                        for (key, _) in rows.iter().filter(|(_, row)| matches_version(row)) {
                            constraints::record_pending(retention, keys, key.clone(),
                                constraints::PendingReference::Child).map_err(CoreError::Sql)?;
                        }
                        true
                    } else { false };
                    Self::validate_references(state, table, c, keys,
                        rows.iter().filter(|(key, row)| keys.is_some_and(|keys| keys.contains_key(*key))
                            || !deferred && matches_version(row)), sink)?;
                }
            }
        }
        Ok(())
    }

    // Probe final physical parents directly, using the same KEY SHARE and
    // snapshot checks for queued actions and version-selected updates.
    fn validate_references<'a>(
        state: &Relational, table: &TableDef, c: &Constraint,
        references: Option<&constraints::PendingReferences>,
        rows: impl Iterator<Item = (&'a Vec<u8>, &'a SqlRow)>, sink: &mut impl RowSink,
    ) -> Result<(), CoreError> {
        let ConstraintKind::ForeignKey { columns, ref_table, ref_columns, .. } = &c.kind
            else { return Ok(()); };
        let mut rows = rows.peekable();
        if rows.peek().is_none() { return Ok(()); }
        let parent = state.tables.values().find(|t| t.id == *ref_table)
            .ok_or(CoreError::Sql(SqlError::UnknownTable))?;
        let positions = columns.iter().zip(ref_columns).map(|(child, parent_id)| (
            table.columns.iter().position(|c| c.id == *child).unwrap(),
            parent.columns.iter().position(|c| c.id == *parent_id).unwrap(),
        )).collect::<Vec<_>>();
        let equality = collation_keys::Key::new(state, parent, ref_columns, None)
            .map_err(CoreError::Sql)?;
        for (key, row) in rows {
            crate::cancellation::check()?;
            if positions.iter().any(|(i, _)| row.values[*i] == Value::Null) { continue; }
            let missing = || {
                if let Some(parent_values) = references.and_then(|references| references.get(key))
                    .and_then(constraints::PendingReference::parent_values)
                {
                    let parent_positions = positions.iter().map(|(_, i)| *i).collect::<Vec<_>>();
                    constraints::still_referenced(state, table, parent, &c.name, &parent_positions,
                        parent_values, false)
                } else {
                    let child_positions = positions.iter().map(|(i, _)| *i).collect::<Vec<_>>();
                    constraints::missing_parent(state, table, parent, &c.name, &child_positions, row)
                }
            };
            let values = positions.iter().map(|(i, j)| query::constraint_key(
                &row.values[*i], &table.columns[*i].data_type, &parent.columns[*j].data_type, query::FkKeyDirection::ToParent,
            )).collect::<R<Option<Vec<_>>>>().map_err(CoreError::Sql)?
                .ok_or_else(|| CoreError::Sql(missing()))?;
            let mut found = false;
            let tuple = equality.values(&values).map_err(CoreError::Sql)?;
            sink.fk_lookup(parent, ref_columns,
                if equality.nondeterministic() { None } else { Some(&values) }, true,
                &mut |candidate| Ok(equality.tuple(&candidate.values)? == tuple),
                &mut |_| { found = true; Ok(()) })?;
            if !found { return Err(CoreError::Sql(missing())); }
        }
        Ok(())
    }
    fn check_removed_key(
        state: &Relational, sink: &mut impl RowSink, check: &RemovedKey,
    ) -> Result<(), CoreError> {
        let child = state.tables.values().find(|t| t.id == check.child).unwrap();
        let c = state.table_constraints(check.child).iter().find(|c| c.id == check.constraint).unwrap();
        let ConstraintKind::ForeignKey { columns, ref_table, ref_columns, .. } = &c.kind
            else { unreachable!() };
        let parent = state.tables.values().find(|t| t.id == *ref_table).unwrap();
        let positions = |table: &TableDef, ids: &[u64]| ids.iter()
            .map(|id| table.columns.iter().position(|c| c.id == *id).unwrap()).collect();
        let Some(key) = IncomingKey::new(
            state, parent, child, ref_columns, positions(parent, ref_columns),
            positions(child, columns), &check.values,
        ).map_err(CoreError::Sql)? else { return Ok(()); };
        if !key.child_exists(sink, parent, child, columns)? { return Ok(()); }
        let mut found = false;
        let exact = (!key.equality.nondeterministic()).then_some(check.values.as_slice());
        sink.fk_lookup(parent, ref_columns, exact, true,
            &mut |candidate| Ok(key.equality.tuple(&candidate.values)? == key.tuple),
            &mut |_| { found = true; Ok(()) })?;
        if !found {
            return Err(CoreError::Sql(constraints::still_referenced(
                state, child, parent, &c.name, &key.parent_positions, &check.values, false,
            )));
        }
        Ok(())
    }

    fn check_removed_keys(&mut self, state: &Relational, sink: &mut impl RowSink) -> Result<(), CoreError> {
        for check in std::mem::take(&mut self.removed_keys).read()? {
            Self::check_removed_key(state, sink, &check?)?;
        }
        Ok(())
    }

    pub(super) fn drain(&mut self, state: &mut Relational, sink: &mut impl RowSink,
        clock: Option<&clock::ExecutionClock>, version_end: u64,
        statement_end: bool, batch_child_probes: bool,
    ) -> Result<(), CoreError> {
        let descriptors = sink.indexes();
        let mut incoming = state.ext.constraints.iter().flat_map(|(id, cs)|
            cs.iter().map(move |c| (*id, c.clone()))).collect::<Vec<_>>();
        incoming.sort_by_key(|(_, c)| c.id);
        let mut empty_by_fk = BTreeMap::<(u64, u64), Vec<usize>>::new();
        let mut queued = std::mem::take(&mut self.queue).read()?;
        let mut batch = std::collections::VecDeque::new();
        let mut queue_position = 0;
        loop {
            if batch.is_empty() {
                empty_by_fk.clear();
                queue_position = 0;
                let mut bytes = 0usize;
                // The RI fast path looks ahead only within this bounded batch.
                // Newly scheduled actions follow every event already queued.
                while batch.is_empty() || bytes < spill::work_mem() {
                    let Some(event) = queued.next().transpose()? else { break; };
                    bytes = bytes.saturating_add(action_bytes(&event.1, event.2.as_ref()).map_err(CoreError::Sql)?);
                    batch.push_back(event);
                }
                if batch.is_empty() {
                    if self.queue.is_empty() { break; }
                    queued = std::mem::take(&mut self.queue).read()?;
                    continue;
                }
            }
            let (parent_id, old, new) = batch.pop_front().expect("nonempty action batch");
            queue_position += 1;
            crate::cancellation::check()?;
            let parent = state.tables.values().find(|t| t.id == parent_id).unwrap().clone();
            for (child_id, c) in &incoming {
                let ConstraintKind::ForeignKey { ref_table, columns, ref_columns, on_delete, on_update, .. } = &c.kind else { continue; };
                if *ref_table != parent_id { continue; }
                let parent_positions = ref_columns.iter()
                    .map(|id| parent.columns.iter().position(|c| c.id == *id).unwrap())
                    .collect::<Vec<_>>();
                let values = parent_positions.iter().map(|i| old.values[*i].clone()).collect::<Vec<_>>();
                let new_values = new.as_ref().map(|row| parent_positions.iter()
                    .map(|i| row.values[*i].clone()).collect::<Vec<_>>());
                if values.contains(&Value::Null)
                    || new_values.as_ref().is_some_and(|new| constraints::row_image_eq(&values, new))
                {
                    continue;
                }
                let action = if new.is_some() { on_update } else { on_delete };
                let no_action = *action == catalog_ext::Action::NoAction;
                let deferred = no_action && self.pending.deferred(c.id, &c.kind);
                let immediate_no_action = no_action && !deferred && !statement_end;
                if immediate_no_action && (new.is_none() || self.check_parent_in_trigger_order) {
                    // Direct writes are visible. Check the key before later
                    // CASCADE or SET triggers can erase a surviving witness.
                    Self::check_removed_key(state, sink, &RemovedKey {
                        child: *child_id, constraint: c.id, values,
                    })?;
                    continue;
                }
                let child = state.tables.values().find(|t| t.id == *child_id).unwrap().clone();
                let child_positions = columns.iter()
                    .map(|id| child.columns.iter().position(|c| c.id == *id).unwrap())
                    .collect::<Vec<_>>();
                let Some(key) = IncomingKey::new(
                    state, &parent, &child, ref_columns, parent_positions, child_positions, &values,
                ).map_err(CoreError::Sql)? else { continue; };
                let references_old_key = |row: &SqlRow| key.matches(&parent, &child, row);
                let restrict = *action == catalog_ext::Action::Restrict;
                if immediate_no_action {
                    // Keep one old key per event until writing actions
                    // finish, independent of child fan-out.
                    let check = RemovedKey { child: *child_id, constraint: c.id, values };
                    let bytes = size_of::<RemovedKey>() + check.values.iter()
                        .map(|v| size_of::<Value>() + value_bytes(v)).sum::<usize>();
                    self.removed_keys.push(check, bytes)?;
                    continue;
                }
                let mut parent_values = None;
                let set_default = *action == catalog_ext::Action::SetDefault;
                let mut still_references_old_key = false;
                let mut after = None;
                let first_version = state.next_version;
                let writes = matches!(action,
                    catalog_ext::Action::Cascade | catalog_ext::Action::SetNull | catalog_ext::Action::SetDefault);
                // Transaction-deferred obligations identify children by PK.
                // Lock the current witnesses against a concurrent rekey and
                // restart discovery if a scanned witness moved before locking.
                let row_locks = deferred || writes && sink.row_locked_cascade();
                if batch_child_probes && *child_id != parent_id && let Some(probe) = &key.probe {
                    let empty = empty_by_fk.entry((*child_id, c.id)).or_insert_with(|| {
                            // Speculative conversion errors stay queued.
                            let queued = batch.iter().enumerate().filter_map(|(i, (id, old, new))| {
                            if *id != parent_id { return None; }
                            let values = key.parent_positions.iter().map(|&p| old.values[p].clone()).collect::<Vec<_>>();
                            if values.contains(&Value::Null) { return None; }
                            let new_values = new.as_ref().map(|row| key.parent_positions.iter()
                                .map(|&p| row.values[p].clone()).collect::<Vec<_>>());
                            if new_values.as_ref().is_some_and(|new| constraints::row_image_eq(&values, new)) { return None; }
                            let converted = key.parent_positions.iter().zip(&key.child_positions)
                                .zip(&values).map(|((&p, &c), value)| {
                                query::constraint_key(value, &child.columns[c].data_type, &parent.columns[p].data_type, query::FkKeyDirection::ToChild)
                            }).collect::<R<Option<Vec<_>>>>().ok().flatten()?;
                            Some((queue_position + 1 + i, converted))
                        });
                        let mut keys = std::iter::once((queue_position, probe.clone())).chain(queued);
                        sink.fk_empty_children(&child, columns, restrict || row_locks, &mut keys)
                    });
                    if empty.binary_search(&queue_position).is_ok() { continue; }
                }
                if restrict {
                    if key.child_exists(sink, &parent, &child, columns)? {
                        // Preserve a prior NO ACTION error when this RESTRICT
                        // trigger stops the action queue before it could finish.
                        self.check_removed_keys(state, sink)?;
                        return Err(CoreError::Sql(constraints::still_referenced(
                            state, &child, &parent, &c.name, &key.parent_positions, &values, true,
                        )));
                    }
                    continue;
                }
                loop {
                    // A directional cast or nondeterministic collation can make
                    // an exact probe incomplete. Compare physical pages instead.
                    let page = if row_locks {
                        sink.fk_action_page(&child, key.probe.as_deref().map(|v| (columns.as_slice(), v)), after)?
                    } else if let Some(probe) = &key.probe {
                        sink.fk_child_page(&child, columns, probe, after)?
                    } else {
                        sink.fk_read_page(&child, after)?
                    };
                    let Some((last, mut rows)) = page else { break; };
                    after = Some(last);
                    if key.probe.is_none() {
                        let mut matching = DdlRows::new();
                        for (pk, row) in rows {
                            // A cascade can move a matching child ahead of the
                            // cursor. Do not act on this scan's own writes again.
                            if writes && (first_version..version_end).contains(&row.version) { continue; }
                            if references_old_key(&row).map_err(CoreError::Sql)? {
                                matching.insert(pk, row);
                            }
                        }
                        rows = matching;
                    }
                    let updating = new.is_some() || *action != catalog_ext::Action::Cascade;
                    if row_locks {
                        let mut locked = DdlRows::new();
                        let mut restart = false;
                        for (pk, row) in rows {
                            if writes && (first_version..version_end).contains(&row.version) { continue; }
                            let mode = if deferred { FkRowLock::KeyShare }
                                else if updating { FkRowLock::Update(columns) } else { FkRowLock::Delete };
                            let current = sink.lock_fk_action_row(&child, &row, mode)?;
                            // A concurrent PK update can preserve the FK and
                            // move the old row behind the cursor. Its old PK
                            // could even have been reused by another row.
                            restart |= current.as_ref() != Some(&row);
                            if let Some(current) = current
                                && references_old_key(&current).map_err(CoreError::Sql)?
                            {
                                locked.insert(pk, current);
                            }
                        }
                        if restart { after = None; }
                        rows = locked;
                    }
                    if *action == catalog_ext::Action::NoAction {
                        let pending = if deferred { &mut self.pending } else { &mut self.immediate };
                        let check = (c.id, *child_id);
                        if !rows.is_empty() && parent_values.is_none() {
                            parent_values = Some(Arc::<[Value]>::from(values.clone()));
                        }
                        for pk in rows.into_keys() {
                            let reference = constraints::PendingReference::Parent(
                                Arc::clone(parent_values.as_ref().expect("matched child has a parent key")),
                            );
                            constraints::record_pending(&mut self.pending_retention,
                                pending.pending.entry(check).or_default(), pk, reference).map_err(CoreError::Sql)?;
                        }
                        continue;
                    }
                    let mut changed = DdlRows::new();
                    let mut events = Vec::new();
                    if updating {
                        for old in rows.values() {
                            let mut row = old.clone();
                            for (i, &position) in key.child_positions.iter().enumerate() {
                                row.values[position] = match action {
                                    catalog_ext::Action::Cascade => constraints::cascade_value(
                                        state, &parent.columns[key.parent_positions[i]], &child.columns[position],
                                        &new_values.as_ref().unwrap()[i],
                                    ).map_err(CoreError::Sql)?,
                                    catalog_ext::Action::SetNull => Value::Null,
                                    // Runs as the referencing table's owner (GD-0084 Q24).
                                    catalog_ext::Action::SetDefault => defaults::evaluate(state, &child.columns[position],
                                        clock.map(|clock| clock.with_sequence_origin(
                                            clock::SequenceOrigin::ReferentialAction(*child_id))).as_ref()).map_err(CoreError::Sql)?,
                                    _ => unreachable!(),
                                };
                            }
                            state.materialize_generated(&child, &mut row.values).map_err(CoreError::Sql)?;
                            row_check_with_diagnostics(state, &child, &row.values).map_err(CoreError::Sql)?;
                            state.check_insert_candidate(&child, &row.values).map_err(CoreError::Sql)?;
                            if set_default && !still_references_old_key {
                                still_references_old_key = references_old_key(&row).map_err(CoreError::Sql)?;
                            }
                            row.version = allocate(&mut state.next_version).map_err(CoreError::Sql)?;
                            events.push(constraints::Event { table: child.id, old: old.clone(), new: Some(row.clone()) });
                            if changed.insert(pk(&child, &row.values).map_err(CoreError::Sql)?, row).is_some() {
                                return Err(CoreError::Sql(SqlError::Duplicate));
                            }
                        }
                    }
                    let wrote_child = !rows.is_empty();
                    self.publish(state, sink, &child, rows, &changed, events, (!set_default).then_some(c.id), &descriptors, version_end)?;
                    if wrote_child {
                        // Keep the attempted checks, but discard every proof for
                        // this table. The action may have added a new reference.
                        for (_, empty) in empty_by_fk.range_mut((*child_id, 0)..=(*child_id, u64::MAX)) {
                            *empty = Vec::new();
                        }
                    }
                }
                // SET DEFAULT has its own immediate NO ACTION check of the
                // old key. Keep child-side checks for references to new keys.
                if still_references_old_key {
                    let mut found = false;
                    let exact = (!key.equality.nondeterministic()).then_some(values.as_slice());
                    sink.fk_lookup(&parent, ref_columns, exact, true,
                        &mut |candidate| Ok(key.equality.tuple(&candidate.values)? == key.tuple),
                        &mut |_| { found = true; Ok(()) })?;
                    if !found {
                        return Err(CoreError::Sql(constraints::still_referenced(
                            state, &child, &parent, &c.name, &key.parent_positions, &values, false,
                        )));
                    }
                }
            }
        }
        // The next drain cannot encounter the old versions of this chain.
        self.retention.release(self.visited.len() * VISITED_BLOCK_BYTES);
        self.visited.clear();
        Ok(())
    }

    fn queue_child_check(&mut self, table: u64, key: &[u8], constraint: &Constraint,
        by_version: bool,
    ) -> Result<(), CoreError> {
        let pending = if self.pending.deferred(constraint.id, &constraint.kind) {
            &mut self.pending
        } else {
            &mut self.immediate
        };
        let check = (constraint.id, table);
        if by_version {
            if let Some(keys) = pending.pending.get_mut(&check) && keys.contains_key(key) {
                constraints::record_pending(&mut self.pending_retention, keys, key.to_vec(),
                    constraints::PendingReference::Child).map_err(CoreError::Sql)?;
            }
        } else {
            constraints::record_pending(&mut self.pending_retention, pending.pending.entry(check).or_default(), key.to_vec(),
                constraints::PendingReference::Child).map_err(CoreError::Sql)?;
        }
        Ok(())
    }

    fn move_pending_key(retention: &mut scan::Retention, keys: &mut constraints::PendingReferences,
        old: &[u8], new: Option<&[u8]>,
    ) -> Result<(), CoreError> {
        if new == Some(old) { return Ok(()); }
        let Some(reference) = keys.remove(old) else { return Ok(()); };
        constraints::release_pending(retention, old, &reference);
        if let Some(new) = new
            && (!matches!(reference, constraints::PendingReference::Child) || !keys.contains_key(new))
        {
            constraints::record_pending(retention, keys, new.to_vec(), reference).map_err(CoreError::Sql)?;
        }
        Ok(())
    }

    #[expect(clippy::too_many_arguments, reason = "bounded action page and statement reservation")]
    pub(super) fn publish(&mut self, state: &Relational, sink: &mut impl RowSink, table: &TableDef,
        before: DdlRows, after: &DdlRows, mut events: Vec<constraints::Event>, force: Option<u64>,
        descriptors: &[secondary_index::Descriptor], version_end: u64,
    ) -> Result<(), CoreError> {
        if state.next_version > version_end { return Err(CoreError::Sql(SqlError::Limit)); }
        let captured = events.iter().map(|e| e.old.version).collect::<BTreeSet<_>>();
        for (key, old) in &before {
            if !captured.contains(&old.version) && after.get(key) != Some(old) {
                events.push(constraints::Event { table: table.id, old: old.clone(), new: after.get(key).cloned() });
            }
        }
        let updated = if self.track_inserts {
            events.iter().filter_map(|event| event.new.as_ref().map(|row| row.version)).collect()
        } else {
            BTreeSet::new()
        };
        let outgoing = state.table_constraints(table.id);
        let incoming = state.ext.constraints.values().flatten().any(|c|
            matches!(&c.kind, ConstraintKind::ForeignKey { ref_table, .. } if *ref_table == table.id));
        let has_outgoing = outgoing.iter().any(|c| matches!(c.kind, ConstraintKind::ForeignKey { .. }));
        for constraints::Event { old, new, .. } in events {
            let mut upsert = self.upsert.as_mut().filter(|upsert| upsert.table == table.id);
            let existing = if let (Some(upsert), Some(row)) = (&mut upsert, &new) {
                upsert.record_update(old.version, row.version, &mut self.retention).map_err(CoreError::Sql)?
            } else { false };
            let track_upsert = upsert.is_some();
            if force.is_none() && new.as_ref().is_some_and(|row| constraints::row_image_eq(&old.values, &row.values))
                || !(incoming || has_outgoing) { continue; }
            let key = &pk(table, &old.values).map_err(CoreError::Sql)?;
            if !self.mark_visited(table.id, old.version).map_err(CoreError::Sql)? { continue; }
            let new_key = new.as_ref().map(|r| pk(table, &r.values)).transpose().map_err(CoreError::Sql)?;
            for checks in [&mut self.pending.pending, &mut self.immediate.pending] {
                for ((_, id), keys) in checks {
                    if *id == table.id {
                        Self::move_pending_key(&mut self.pending_retention, keys, key, new_key.as_deref())?;
                    }
                }
            }
            if self.pending.replica { continue; }
            if let Some(row) = &new {
                for c in outgoing {
                    let ConstraintKind::ForeignKey { columns, .. } = &c.kind else { continue; };
                    // A cast can keep the child's old key after the parent moved.
                    // Queue that recheck in the same mode as a changed reference.
                    let changed = force == Some(c.id) || columns.iter().any(|id| {
                        let i = table.columns.iter().position(|c| c.id == *id).unwrap();
                        row.values[i] != old.values[i]
                    });
                    if !changed { continue; }
                    if existing {
                        self.upsert.as_mut().unwrap().record_reference(c.id, row.version, &mut self.retention)
                            .map_err(CoreError::Sql)?;
                    }
                    // Inserts, including action-rewritten inserts, are checked
                    // by the append scan. Explicit action rechecks keep their timing.
                    self.queue_child_check(table.id, new_key.as_ref().unwrap(), c, track_upsert)?;
                }
            }
            if incoming {
                let bytes = action_bytes(&old, new.as_ref()).map_err(CoreError::Sql)?;
                self.queue.push((table.id, old, new), bytes)?;
            }
        }
        if self.track_inserts && !self.pending.replica {
            for (key, _) in after.iter().filter(|(key, row)| {
                !before.contains_key(*key) && !updated.contains(&row.version)
            }) {
                for constraint in state.table_constraints(table.id) {
                    if matches!(constraint.kind, ConstraintKind::ForeignKey { .. }) {
                        self.queue_child_check(table.id, key, constraint, false)?;
                    }
                }
            }
        }
        if let Some(upsert) = self.upsert.as_mut().filter(|upsert| upsert.table == table.id)
            && !upsert.has_inserts
        {
            upsert.has_inserts = after.iter().any(|(key, row)|
                upsert.is_insert(row.version) && before.get(key) != Some(row));
        }
        sink.check_occupancy(table, &before, after)?;
        sink.apply_indexes(table, Some(table), &before, after, descriptors)?;
        sink.apply(table, &before, after)
    }
}

#[cfg(test)]
mod upsert_fk_tests {
    use super::*;

    #[test]
    fn action_spill_replays_in_trigger_order() {
        scan::with_cursor_limit(512, || {
            let first_run = (0..20).map(|version| {
                (1, SqlRow { version, values: vec![Value::Int(version as i64)] }, None)
            });
            let mut queue = spill::Replay::<ActionEvent>::default();
            for event in first_run.clone() {
                let bytes = action_bytes(&event.1, None).unwrap();
                queue.push(event, bytes).unwrap();
            }
            let replayed = queue.read().unwrap().collect::<Result<Vec<_>, _>>().unwrap();
            assert_eq!(replayed, first_run.collect::<Vec<_>>());
        });
    }

    #[test]
    fn visited_versions_charge_a_block_once() {
        let mut work = ActionWork::new(DeferralState::default()).unwrap();
        assert!(work.mark_visited(1, 256).unwrap());
        assert!(work.mark_visited(1, 257).unwrap());
        assert!(!work.mark_visited(1, 256).unwrap());
        assert_eq!(work.retention.used(), VISITED_BLOCK_BYTES);
        assert!(work.mark_visited(1, 512).unwrap());
        assert!(work.mark_visited(2, 256).unwrap());
        assert_eq!(work.retention.used(), 3 * VISITED_BLOCK_BYTES);
    }

    struct AppendSink;

    impl RowSink for AppendSink {
        fn scan(&mut self, _: &TableDef, _: &mut dyn FnMut(SqlRow) -> R<()>) -> Result<(), CoreError> {
            panic!("publish should not scan for parents");
        }

        fn apply(&mut self, _: &TableDef, _: &DdlRows, _: &DdlRows) -> Result<(), CoreError> {
            Ok(())
        }
    }

    #[test]
    fn inserted_rows_do_not_grow_the_upsert_fk_retention() {
        let mut state = Relational::default();
        for sql in [
            "CREATE TABLE parent(id int PRIMARY KEY)",
            "CREATE TABLE child(id int PRIMARY KEY, p int REFERENCES parent(id))",
        ] {
            let ddl = Prepared::parse(sql, 1, state.epoch).unwrap();
            state.execute(&ddl, &[], None).unwrap();
        }
        let table = &state.tables["child"];
        let start = state.next_version;
        let end = start + 1_000;
        let mut actions = ActionWork::new(DeferralState::default()).unwrap();
        actions.upsert = Some(UpsertChecks::new(table.id, start..end));
        let mut sink = AppendSink;
        for first in (start..end).step_by(200) {
            let rows = (first..first + 200).map(|version| {
                let values = vec![Value::Int(version as i64), Value::Int(1)];
                (pk(table, &values).unwrap(), SqlRow { values, version })
            }).collect();
            actions.publish(&state, &mut sink, table, DdlRows::new(), &rows,
                Vec::new(), None, &[], end).unwrap();
            assert_eq!(actions.retention.used(), 0);
            assert!(actions.immediate.pending.is_empty());
            assert!(actions.pending.pending.is_empty());
        }
        let checks = actions.upsert.unwrap();
        assert!(checks.has_inserts());
        assert!((start..end).all(|version| checks.is_insert(version)));
        assert_eq!(checks.updates.0.capacity(), 0);
        assert!(checks.references.is_empty());
    }

    #[test]
    fn moving_an_fk_check_replaces_its_key_charge() {
        let old = vec![1];
        let new = vec![2; 8];
        let mut keys = BTreeMap::new();
        let mut retention = scan::Retention::new(1024);
        constraints::record_pending(&mut retention, &mut keys, old.clone(),
            constraints::PendingReference::Parent(Arc::from([Value::Int(1)]))).unwrap();
        let new_bytes = retention.used() + new.len() - old.len();
        ActionWork::move_pending_key(&mut retention, &mut keys, &old, Some(&new)).unwrap();
        assert_eq!(keys.len(), 1);
        assert!(matches!(keys.get(&new), Some(constraints::PendingReference::Parent(_))));
        assert_eq!(retention.used(), new_bytes);
        ActionWork::move_pending_key(&mut retention, &mut keys, &new, None).unwrap();
        assert!(keys.is_empty());
        assert_eq!(retention.used(), 0);
    }
}

#[cfg(test)]
mod page_memo_tests {
    use super::*;

    fn catalog() -> Relational {
        let mut state = Relational::default();
        for sql in [
            "CREATE SCHEMA a",
            "CREATE TABLE t(id bigint PRIMARY KEY, n bigint)",
            "CREATE TABLE u(id bigint PRIMARY KEY)",
            "CREATE TABLE v(id bigint PRIMARY KEY)",
            "CREATE TABLE a.indexed(id bigint PRIMARY KEY, n bigint)",
            "CREATE INDEX u ON a.indexed(n)",
            "CREATE INDEX v ON a.indexed(n)",
            "INSERT INTO t VALUES (1, 10), (2, 20)",
            "INSERT INTO u VALUES (2)",
        ] {
            let prepared = Prepared::parse(sql, 1, state.epoch).unwrap();
            state.execute(&prepared, &[], None).unwrap();
        }
        state
    }

    #[test]
    fn page_memo_reuses_description_when_index_names_are_reordered() {
        let state = catalog();
        let mut prepared = Prepared::parse(
            "UPDATE t SET n = 30 WHERE id IN (SELECT id FROM u) AND id IN (SELECT id FROM v) RETURNING id",
            1,
            state.epoch,
        )
        .unwrap();
        prepared.set_schema_context(vec!["public".into(), "a".into()], "giggles".into());
        let preflight = StatementDescription::new(&prepared);
        let page = StatementDescription::new(&prepared);
        let mut indexes = state.secondary_descriptors_engine().to_vec();
        assert_eq!(indexes.len(), 2);
        indexes.reverse();

        let selected = page_memo(&state, &indexes, false, &preflight, &page);
        assert!(std::ptr::eq(selected, &preflight));
    }

    #[test]
    fn page_memo_rebinds_when_an_index_shadows_an_unqualified_relation() {
        let shadowed = catalog();
        let mut unshadowed = shadowed.clone();
        rebuild(&mut unshadowed, &[]).unwrap();
        let mut prepared = Prepared::parse(
            "UPDATE t SET n = 30 WHERE id IN (SELECT id FROM u) RETURNING id",
            1,
            shadowed.epoch,
        )
        .unwrap();
        prepared.set_schema_context(vec!["a".into(), "public".into()], "giggles".into());
        assert_eq!(
            shadowed.resolve_relation_key("u", &prepared.search_path),
            relation_key("a", "u")
        );
        assert_eq!(unshadowed.resolve_relation_key("u", &prepared.search_path), "u");

        let preflight = StatementDescription::new(&prepared);
        let page = StatementDescription::new(&prepared);
        let selected = page_memo(&shadowed, &[], false, &preflight, &page);
        assert!(std::ptr::eq(selected, &page));
        let mut fresh = unshadowed.clone();
        let expected = fresh.execute(&prepared, &[], None).unwrap();
        assert_eq!(expected.affected, 1);
        assert_eq!(expected.rows[0].values, [Value::Int(2)]);
        searched::DESCRIBE_CALLS.set(0);
        let actual = unshadowed
            .execute_statement_described(selected, &[], None, || false, None, None, &mut vec![])
            .unwrap();
        assert_eq!(actual, expected);
        assert_eq!(searched::DESCRIBE_CALLS.get(), 1);

        // A populated memo from the unshadowed image cannot describe the
        // shadowed image. Its error must match an independent describe.
        let fresh = StatementDescription::new(&prepared);
        let selected = page_memo(
            &unshadowed,
            shadowed.secondary_descriptors_engine(),
            false,
            &page,
            &fresh,
        );
        assert!(std::ptr::eq(selected, &fresh));
        let expected = prepared.describe(&shadowed).unwrap_err();
        assert_eq!(expected, SqlError::UnknownTable);
        assert_eq!(selected.input().describe(&shadowed).unwrap_err(), expected);
    }
}

#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "statement_description_tests.rs"]
mod statement_description_tests;

#[cfg(test)]
#[path = "upsert_classifier_tests.rs"]
mod upsert_classifier_tests;

#[cfg(test)]
mod native_lock_tests {
    use super::*;
    fn sql(state: &mut Relational, sql: &str) {
        let p = Prepared::parse(sql, 1, state.epoch).unwrap();
        state.execute(&p, &[], None).unwrap();
    }
    fn literal_key(ty: &str, value: Value) -> Option<Vec<Vec<Value>>> {
        let mut state = Relational::default();
        // DDL rejects some unsupported PK types before the helper can see them.
        // Build their table descriptor directly to exercise its conservative gate.
        let unsupported = match ty {
            "char(3)" => Some(DataType::Bpchar),
            "date" => Some(DataType::Date),
            "time" => Some(DataType::Time),
            "timestamp" => Some(DataType::Timestamp),
            "timestamptz" => Some(DataType::TimestampTz),
            _ => None,
        };
        let ddl_ty = if unsupported.is_some() { "bigint" } else { ty };
        sql(&mut state, &format!("CREATE TABLE t(id {ddl_ty} PRIMARY KEY)"));
        let mut table = state.tables["t"].clone();
        if let Some(ty) = unsupported { table.columns[0].data_type = ty; }
        let p = Prepared::engine(Statement::Delete { name: "t".into(),
            predicates: vec![Predicate { column: "id".into(), op: "=".into(),
                value: Expr::Literal(value) }], search: None, returning: None }, 1, state.epoch);
        native_lock_key(&state, &p, &table)
    }
    #[test]
    fn native_batch_complete_keys_deduplicate_and_keep_fallbacks() {
        let mut state = Relational::default();
        sql(&mut state, "CREATE TABLE t(id bigint, other bigint, PRIMARY KEY(id,other))");
        let table = &state.tables["t"];
        let rows = vec![(vec![Value::Int(3),Value::Int(4)],1),
            (vec![Value::Int(1),Value::Int(2)],2),(vec![Value::Int(3),Value::Int(4)],1)];
        let p = Prepared::delete_many_engine(table,rows,1,&state).unwrap();
        assert_eq!(native_lock_key(&state,&p,table),Some(vec![vec![Value::Int(1),Value::Int(2)],vec![Value::Int(3),Value::Int(4)]]));
        let mut inexact = p.clone();
        inexact.native_delete.as_mut().unwrap()[1].0[0] = Value::Numeric(Numeric::parse("1.5").unwrap());
        assert_eq!(native_lock_key(&state,&inexact,table),None);
        let mut partial = p;
        partial.native_delete.as_mut().unwrap()[1].0.pop();
        assert_eq!(native_lock_key(&state,&partial,table),None);
    }
    #[test]
    fn integer_boundaries_unknown_literals_and_numeric_fallback() {
        for (ty, min, max, outside) in [
            ("smallint", i16::MIN as i64, i16::MAX as i64, "40000"),
            ("integer", i32::MIN as i64, i32::MAX as i64, "2147483648"),
            ("bigint", i64::MIN, i64::MAX, "9223372036854775808"),
        ] {
            for n in [min, max, 5] {
                for value in [Value::Int(n), Value::Text(n.to_string())] {
                    assert_eq!(literal_key(ty, value), Some(vec![vec![Value::Int(n)]]), "{ty}: {n}");
                }
            }
            for value in [outside.to_string(), (i128::from(min) - 1).to_string()] {
                assert_eq!(literal_key(ty, Value::Text(value.clone())), None, "{ty}: {value}");
            }
            for value in ["1.0", "1.5"] {
                // Numeric/integer comparison needs its full filter; never round a lock key.
                assert_eq!(literal_key(ty, Value::Numeric(Numeric::parse(value).unwrap())), None);
            }
        }
        assert_eq!(literal_key("smallint", Value::Int(40000)), None);
        assert_eq!(literal_key("integer", Value::Int(2147483648)), None);
    }
    #[test]
    fn text_boolean_and_unsupported_key_types() {
        assert_eq!(literal_key("varchar(8)", Value::Text("a ".into())),
            Some(vec![vec![Value::Text("a ".into())]]));
        for b in [false, true] {
            assert_eq!(literal_key("boolean", Value::Bool(b)), Some(vec![vec![Value::Bool(b)]]));
            assert_eq!(literal_key("boolean", Value::Text(b.to_string())), Some(vec![vec![Value::Bool(b)]]));
        }
        for (ty, value) in [("char(3)", "a "), ("date", "2026-01-01"),
            ("time", "12:00:00"), ("timestamp", "2026-01-01 12:00:00"),
            ("timestamptz", "2026-01-01 12:00:00+00")] {
            assert_eq!(literal_key(ty, Value::Text(value.into())), None, "{ty}");
        }
    }
    #[test]
    fn numeric_scale_signed_zero_and_nan_key_identity() {
        for (ty, a, b) in [("numeric", "1.0", "1.00"), ("numeric", "0", "-0"),
            ("numeric", "NaN", "NaN"), ("real", "0", "-0"),
            ("double precision", "0", "-0"), ("real", "NaN", "NaN"),
            ("double precision", "NaN", "NaN")] {
            let mut encoded = Vec::new();
            for text in [a, b] {
                let expected = match ty {
                    "numeric" => Value::Numeric(Numeric::parse(text).unwrap()),
                    "real" => Value::Float4(Float4::from_bits(text.parse::<f32>().unwrap().to_bits())),
                    _ => Value::Float8(Float8::from_bits(text.parse::<f64>().unwrap().to_bits())),
                };
                let keys = literal_key(ty, Value::Text(text.into())).unwrap();
                assert_eq!(keys, vec![vec![expected.clone()]], "{ty}: {text}");
                assert_eq!(literal_key(ty, expected), Some(keys.clone()));
                encoded.push(crate::storage::engine::codec::pk_bytes(&keys[0]).unwrap());
            }
            // SQL-equal values must acquire the same physical row lock.
            assert_eq!(encoded[0], encoded[1], "{ty}: {a} = {b}");
        }
    }
    #[test]
    fn duplicate_partial_and_composite_disjunction_fallback() {
        let mut state = Relational::default();
        sql(&mut state, "CREATE TABLE t(id bigint, other bigint, PRIMARY KEY(id,other))");
        let table = &state.tables["t"];
        let predicate = |column: &str, n| Predicate { column: column.into(), op: "=".into(),
            value: Expr::Literal(Value::Int(n)) };
        for predicates in [vec![predicate("id", 1)],
            vec![predicate("id", 1), predicate("id", 2)]] {
            let p = Prepared::engine(Statement::Delete { name: "t".into(), predicates,
                search: None, returning: None }, 1, state.epoch);
            assert_eq!(native_lock_key(&state, &p, table), None);
        }
        for text in ["DELETE FROM t WHERE (id = 1 AND other = 2) OR (id = 3 AND other = 4)",
            "DELETE FROM t WHERE id IN (1,3) AND other = 2"] {
            let p = Prepared::parse(text, 1, state.epoch).unwrap();
            assert_eq!(native_lock_key(&state, &p, table), None, "{text}");
        }
        sql(&mut state, "CREATE TABLE single(id bigint PRIMARY KEY)");
        let table = &state.tables["single"];
        let p = Prepared::engine(Statement::Delete { name: "single".into(),
            predicates: vec![predicate("id", 1), predicate("id", 2)],
            search: None, returning: None }, 1, state.epoch);
        assert_eq!(native_lock_key(&state, &p, table), None);
    }
    #[test]
    fn physical_key_identity_and_exact_literal_shape() {
        for (ty, literal) in [("smallint", "7"), ("integer", "7"), ("bigint", "7"),
            ("numeric", "7.00"), ("real", "-0.0"), ("double precision", "7.25"),
            ("text", "'hello'"), ("bytea", "'bytes'")] {
            let mut state = Relational::default();
            sql(&mut state, &format!("CREATE TABLE t(a {ty}, b bigint, n bigint, PRIMARY KEY(b,a))"));
            sql(&mut state, &format!("INSERT INTO t VALUES({literal},2,0)"));
            let table = state.tables["t"].clone();
            let row = state.rows[&table.id].values().next().unwrap();
            let key = table.primary_key.iter().map(|i| row.values[*i].clone()).collect::<Vec<_>>();
            for delete in [false, true] {
                let mut p = if delete { Prepared::delete_engine(&table, key.clone(), 1, &state) }
                    else { Prepared::update_engine(&table, key.clone(), vec![(2, Value::Int(1))], 1, &state) }.unwrap();
                let keys = p.mutation_lock_keys(&state, &[], &[]).unwrap();
                assert_eq!(keys, vec![key.clone()], "{ty}");
                let predicates = match p.statement.get_mut() {
                    Statement::Update { predicates, .. } | Statement::Delete { predicates, .. } => predicates,
                    _ => unreachable!(),
                };
                predicates.reverse();
                let bound = bounds(&table, predicates, &[]).unwrap();
                assert!(bound.point);
                assert_eq!(crate::storage::engine::codec::pk_bytes(&keys[0]).unwrap(), bound.low);
                predicates.push(predicates[0].clone());
                assert!(p.mutation_lock_keys(&state, &[], &[]).is_none());
                let predicates = match p.statement.get_mut() {
                    Statement::Update { predicates, .. } | Statement::Delete { predicates, .. } => predicates,
                    _ => unreachable!(),
                };
                predicates.pop(); predicates[0].value = Expr::Literal(Value::Null);
                assert!(p.mutation_lock_keys(&state, &[], &[]).is_none());
            }
        }
    }
    #[test]
    fn nondeterministic_unique_changing_and_cyclic_keep_exclusive() {
        let mut state = Relational::default();
        sql(&mut state, "CREATE COLLATION ci(provider=icu,locale='und-u-ks-level2',deterministic=false)");
        sql(&mut state, "CREATE TABLE t(id text COLLATE ci PRIMARY KEY,n bigint)");
        let table = state.tables["t"].clone();
        let p = Prepared::delete_engine(&table, vec![Value::Text("a".into())], 1, &state).unwrap();
        assert!(native_lock_key(&state, &p, &table).is_none());
        let mut state = Relational::default();
        sql(&mut state, "CREATE TABLE t(id bigint PRIMARY KEY,n bigint UNIQUE)");
        let table = state.tables["t"].clone();
        let p = Prepared::update_engine(&table, vec![Value::Int(1)], vec![(1,Value::Int(2))], 1, &state).unwrap();
        assert!(p.mutation_lock_keys(&state, &[], state.secondary_descriptors_engine()).is_none());
        sql(&mut state, "CREATE TABLE c(id bigint PRIMARY KEY,p bigint REFERENCES t(id))");
        sql(&mut state, "ALTER TABLE t ADD FOREIGN KEY(n) REFERENCES c(id)");
        let p = Prepared::delete_engine(&table, vec![Value::Int(1)], 1, &state).unwrap();
        assert!(p.fk_delete_lock_keys(&state, &[]).is_none());
    }
    #[test]
    fn unsupported_and_fk_admission_gates() {
        let mut state = Relational::default();
        sql(&mut state, "CREATE TABLE t(id uuid PRIMARY KEY,n bigint)");
        let table = state.tables["t"].clone();
        let p = Prepared::engine(Statement::Delete { name: "t".into(),
            predicates: vec![Predicate { column: "id".into(), op: "=".into(),
                value: Expr::Literal(Value::Text("00000000-0000-0000-0000-000000000001".into())) }],
            search: None, returning: None }, 1, state.epoch);
        assert!(native_lock_key(&state, &p, &table).is_none());
        assert!(p.mutation_lock_keys(&state, &[], &[]).is_none());
        for action in ["RESTRICT", "NO ACTION", "CASCADE", "SET NULL", "SET DEFAULT"] {
            for deferred in [false, true] {
                let mut state = Relational::default();
                sql(&mut state, "CREATE TABLE p(id bigint PRIMARY KEY,n bigint)");
                sql(&mut state, &format!("CREATE TABLE c(id bigint PRIMARY KEY,p bigint REFERENCES p(id) ON DELETE {action} {} )",
                    if deferred { "DEFERRABLE INITIALLY DEFERRED" } else { "" }));
                let table = state.tables["p"].clone();
                let p = Prepared::delete_engine(&table, vec![Value::Int(1)], 1, &state).unwrap();
                assert!(p.mutation_lock_keys(&state, &[], &[]).is_none());
                assert_eq!(p.fk_delete_lock_keys(&state, &[]).is_some(), !deferred && action != "SET DEFAULT");
            }
        }
    }
}
