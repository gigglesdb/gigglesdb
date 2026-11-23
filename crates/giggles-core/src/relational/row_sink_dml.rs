//! Bounded statement images; all pages share the caller's transaction/savepoint.
use super::*;

fn moves_primary_key(state: &Relational, prepared: &Prepared) -> bool {
    let Some(table) = prepared.table().and_then(|name| state.tables.get(name)) else { return false; };
    let names = match &prepared.statement {
        Statement::Update { sets, .. } => sets.iter().map(|(name, _)| name).collect::<Vec<_>>(),
        Statement::UpdateExpressions { sets, .. } => sets.iter().map(|(name, _)| name).collect(),
        _ => return false,
    };
    table.primary_key.iter().any(|i| names.contains(&&table.columns[*i].name))
}

pub(super) fn eligible(state: &Relational, prepared: &Prepared) -> bool {
    let Some(table) = prepared.table().and_then(|name| state.tables.get(name)) else {
        return false;
    };
    if state.tables.values().any(|t| state.has_nondeterministic_keys(t)) { return false; }
    let sets = match &prepared.statement {
        Statement::Update { sets, .. } => sets.iter().map(|(name, _)| name).collect::<Vec<_>>(),
        Statement::UpdateExpressions { sets, .. } => sets.iter().map(|(name, _)| name).collect(),
        Statement::Delete { .. } => vec![],
        _ => return false,
    };
    // A stable primary key makes the forward cursor Halloween-safe.
    if table
        .primary_key
        .iter()
        .any(|i| sets.contains(&&table.columns[*i].name))
    {
        return false;
    }
    // Outgoing references can be checked after all pages. Incoming actions and
    // self references retain their existing executors.
    !state.ext.constraints.iter().any(|(owner, constraints)| {
        constraints.iter().any(|constraint| {
            let ConstraintKind::ForeignKey { columns, ref_table, ref_columns, .. } = &constraint.kind else { return false; };
            let changed = |ids: &[u64]| ids.iter().any(|id| table.columns.iter()
                .any(|column| column.id == *id && sets.contains(&&column.name)));
            let deleting = matches!(prepared.statement, Statement::Delete { .. });
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
    let returning = match &prepared.statement {
        Statement::Update { returning, .. } | Statement::UpdateExpressions { returning, .. }
            => returning.is_some(),
        _ => false,
    };
    if returning && point_keys.as_ref().is_some_and(|keys| keys.len() > 1) {
        point_keys = None;
    }
    if point_keys.as_ref().is_some_and(|keys| keys.len() > 1 || returning && !keys.is_empty()) {
        let changed = match &prepared.statement {
            Statement::Update { sets, .. } => sets.iter().map(|(name, _)| name).collect::<Vec<_>>(),
            Statement::UpdateExpressions { sets, .. } => sets.iter().map(|(name, _)| name).collect(),
            _ => Vec::new(),
        };
        if unique.iter().any(|d| d.columns.iter().any(|id| table.columns.iter()
            .any(|c| c.id == *id && changed.contains(&&c.name)))) {
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
    let predicates = match &prepared.statement {
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
    pub(crate) fn mutation_changes_key(&self, table: &TableDef,
        indexes: &[secondary_index::Descriptor]) -> bool {
        let names = match &self.statement {
            Statement::Delete { .. } => return true,
            Statement::Update { sets, .. } => sets.iter().map(|(name, _)| name).collect::<Vec<_>>(),
            Statement::UpdateExpressions { sets, .. } => sets.iter().map(|(name, _)| name).collect(),
            _ => return false,
        };
        table.columns.iter().enumerate().any(|(i, c)| names.contains(&&c.name)
            && (table.primary_key.contains(&i) || indexes.iter().any(|d|
                d.table == table.id && d.unique && d.columns.contains(&c.id))))
    }

    pub(crate) fn upsert_lock_rows(&self, state: &Relational,
        indexes: &[secondary_index::Descriptor], params: &[Value],
        clock: Option<&clock::ExecutionClock>,
    ) -> Option<UpsertLockRows> {
        let plan = self.upsert_lock_plan(state, indexes)?;
        let Statement::InsertMulti(insert) = &self.statement else { return None; };
        let table = state.tables.get(&insert.table)?;
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
        let Statement::InsertMulti(insert) = &self.statement else { return None; };
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
        let changed = match &self.statement {
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
        if !matches!(self.statement, Statement::Delete { .. }) { return None; }
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
        let Some(table) = self.table().and_then(|name| state.tables.get(name)) else { return false; };
        if state.has_nondeterministic_keys(table) { return false; }
        if state.ext.constraints.get(&table.id).into_iter().flatten().any(|c|
            matches!(c.kind, ConstraintKind::ForeignKey { ref_table, .. } if ref_table == table.id)) {
            return false;
        }
        matches!(&self.statement, Statement::Insert { .. } | Statement::InsertExpressions(_))
            || matches!(&self.statement, Statement::InsertMulti(i) if i.append_only() && i.source.is_none() && (self.native_insert_many || i.row_count() <= paged_insert::SHARED_WRITE_ROWS))
    }
}

pub(super) fn execute(
    state: &mut Relational,
    prepared: &Prepared,
    params: &[Value],
    clock: Option<&clock::ExecutionClock>,
    sink: &mut impl RowSink,
    version_end: u64,
    deferred: Option<&mut DeferralState>,
) -> Result<SqlResult, CoreError> {
    let table = state.tables[prepared.table().expect("mutation target")].clone();
    // Analyze even an empty table before opening the cursor. No writes escape
    // the outer statement savepoint, including failures on the final page.
    let mut result = state
        .execute_statement_inner(
            prepared,
            params,
            None,
            || crate::cancellation::check().is_err(),
            clock,
            None,
            &mut Vec::new(),
        )
        .map_err(CoreError::Sql)?;
    let (template, keys) = selection(state, prepared, params, clock, sink)?;
    let unique = sink.indexes().into_iter().filter(|d| d.table == table.id && d.unique).collect::<Vec<_>>();
    let mut pending = deferred.as_deref().cloned().unwrap_or_default();
    let mut immediate = DeferralState::default();
    let mut checks = scan::Retention::new(scan::retention_limit());
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
            .execute_statement_inner(
                &template,
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
                checks.charge(key.len() + 96).map_err(CoreError::Sql)?;
                let deferred = *deferral != Deferral::NotDeferrable && pending.modes.get(&c.id)
                    .copied().unwrap_or(*deferral == Deferral::DeferrableDeferred);
                let target = if deferred { &mut pending } else { &mut immediate };
                target.pending.entry((c.id, table.id)).or_default().insert(key.clone());
            }
        }
        unchanged.extend(page.rows.remove(&table.id).unwrap_or_default());
        Ok(unchanged)
    })?;
    paged_pending(state, &mut immediate, &[], false, sink)?;
    if deferred.is_none() { paged_pending(state, &mut pending, &[], false, sink)?; }
    if let Some(deferred) = deferred { *deferred = pending; }
    output.finish(&mut result)?;
    Ok(result)
}

// Complete predicate evaluation precedes publication, including self-subqueries.
type MutationSelection = (Prepared, Option<BTreeSet<Vec<u8>>>);
fn selection(state: &Relational, prepared: &Prepared, params: &[Value],
    clock: Option<&clock::ExecutionClock>, sink: &mut impl RowSink,
) -> Result<MutationSelection, CoreError> {
    let supplied = searched::target(prepared).and_then(query::Query::mutation_input_keys);
    let keys = if supplied.is_some() { supplied } else { searched::target(prepared).filter(|q| moves_primary_key(state, prepared) || q.has_in_subquery())
        .map(|_| sink.mutation_keys(state, prepared, params, clock)).transpose()? };
    let mut template = prepared.clone();
    if keys.is_some() {
        template.parameter_types = prepared.describe(state).map_err(CoreError::Sql)?
            .parameters.into_iter().map(Some).collect();
        template.unbound_statement = None;
        match &mut template.statement {
            Statement::Update { predicates, search, .. }
            | Statement::UpdateExpressions { predicates, search, .. }
            | Statement::Delete { predicates, search, .. } => {
                predicates.clear();
                search.as_mut().expect("mutation target").clear_mutation_filter();
            }
            _ => unreachable!(),
        }
    }
    Ok((template, keys))
}

// PK moves use the same bounded publisher and referential-action queue.
pub(super) fn cascade_eligible(state: &Relational, prepared: &Prepared) -> bool {
    if eligible(state, prepared)
        || !matches!(prepared.statement, Statement::Delete { .. } | Statement::Update { .. } | Statement::UpdateExpressions { .. })
    {
        return false;
    }
    let Some(target) = prepared.table().and_then(|name| state.tables.get(name)) else { return false; };
    if state.tables.values().any(|t| state.has_nondeterministic_keys(t)) { return false; }
    let mut connected = BTreeSet::from([(target.id, !matches!(prepared.statement, Statement::Delete { .. }))]);
    let pk_move = moves_primary_key(state, prepared);
    let mut cascade = pk_move;
    loop {
        let count = connected.len();
        for (child_id, cs) in &state.ext.constraints {
            for c in cs {
                let ConstraintKind::ForeignKey { ref_table, columns, ref_columns, on_delete, on_update, .. } = &c.kind else { continue; };
                let events = connected.iter().filter(|(id, _)| id == ref_table).copied().collect::<Vec<_>>();
                for (_, update) in events {
                    let action = if update { on_update } else { on_delete };
                    if *action == catalog_ext::Action::NoAction {
                        if pk_move { continue; }
                        return false;
                    }
                    let child = state.tables.values().find(|t| t.id == *child_id).unwrap();
                    let parent = state.tables.values().find(|t| t.id == *ref_table).unwrap();
                    if columns.iter().zip(ref_columns).any(|(a, b)| {
                        let ty = &child.columns.iter().find(|c| c.id == *a).unwrap().data_type;
                        ty != &parent.columns.iter().find(|c| c.id == *b).unwrap().data_type
                            || !matches!(ty, DataType::Boolean | DataType::SmallInt | DataType::Integer
                                | DataType::BigInt | DataType::Text | DataType::Varchar | DataType::Bytes)
                    }) { return false; }
                    cascade |= *action != catalog_ext::Action::Restrict;
                    if *action != catalog_ext::Action::Restrict {
                        connected.insert((*child_id, update || *action != catalog_ext::Action::Cascade));
                    }
                }
            }
        }
        if count == connected.len() { return cascade; }
    }
}

pub(super) fn cascade(
    state: &mut Relational, prepared: &Prepared, params: &[Value],
    clock: Option<&clock::ExecutionClock>, sink: &mut impl RowSink,
    deferred: Option<&mut DeferralState>, version_end: u64,
) -> Result<SqlResult, CoreError> {
    let pk_move = moves_primary_key(state, prepared);
    let pending = deferred.as_deref().cloned().unwrap_or_default();
    let table = state.tables[prepared.table().unwrap()].clone();
    let descriptors = sink.indexes();
    let mut result = state.execute_statement_inner(prepared, params, None,
        || crate::cancellation::check().is_err(), clock, None, &mut Vec::new()).map_err(CoreError::Sql)?;
    let (template, keys) = selection(state, prepared, params, clock, sink)?;
    let prepared = &template;
    let first_version = state.next_version;
    let mut work = ActionWork::new(pending);
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
            let batch = page.execute_statement_inner(prepared, params, None,
                || crate::cancellation::check().is_err(), clock, None, &mut events).map_err(CoreError::Sql)?;
            result.affected += batch.affected;
            output.extend(batch.rows)?;
            state.next_version = page.next_version;
            work.publish(state, sink, &table, rows, page.rows.remove(&table.id).unwrap(), events, None, &descriptors, version_end)?;
        }
    }
    work.drain(state, sink, clock, &descriptors, version_end, false)?;
    paged_pending(state, &mut work.immediate, &[], false, sink)?;
    if deferred.is_none() { paged_pending(state, &mut work.pending, &[], false, sink)?; }
    if let Some(d) = deferred { *d = work.pending; }
    output.finish(&mut result)?;
    Ok(result)
}


pub(super) struct ActionWork {
    queue: std::collections::VecDeque<(u64, SqlRow, Option<SqlRow>)>,
    visited: BTreeSet<(u64, u64)>,
    retention: scan::Retention,
    pending: DeferralState,
    immediate: DeferralState,
}
impl ActionWork {
    pub(super) fn new(pending: DeferralState) -> Self {
        Self { queue: std::collections::VecDeque::new(), visited: BTreeSet::new(),
            retention: scan::Retention::new(scan::retention_limit()), pending,
            immediate: DeferralState::default() }
    }
    pub(super) fn finish(mut self, state: &Relational, sink: &mut impl RowSink,
        deferred: Option<&mut DeferralState>,
    ) -> Result<(), CoreError> {
        Self::validate_pending(state, &mut self.immediate, sink)?;
        if let Some(deferred) = deferred { *deferred = self.pending; }
        else { Self::validate_pending(state, &mut self.pending, sink)?; }
        Ok(())
    }
    // Action checks already carry child keys. Probe final physical parents
    // directly instead of certifying a transient parent sidecar per child.
    fn validate_pending(state: &Relational, pending: &mut DeferralState,
        sink: &mut impl RowSink,
    ) -> Result<(), CoreError> {
        for table in state.tables.values() {
            let checks = pending.pending.iter().filter_map(|((id, table_id), keys)| {
                if *table_id != table.id { return None; }
                let c = state.ext.constraints.get(table_id)?.iter().find(|c| c.id == *id)?;
                Some((c, keys))
            }).collect::<Vec<_>>();
            if checks.is_empty() { continue; }
            let mut after = None;
            while let Some((last, rows)) = sink.fk_read_page(table, after.take())? {
                after = Some(last);
                for (c, keys) in &checks {
                    let ConstraintKind::ForeignKey { columns, ref_table, ref_columns, .. } = &c.kind
                        else { continue; };
                    let parent = state.tables.values().find(|t| t.id == *ref_table)
                        .ok_or(CoreError::Sql(SqlError::UnknownTable))?;
                    let positions = columns.iter().zip(ref_columns).map(|(child, parent_id)| (
                        table.columns.iter().position(|c| c.id == *child).unwrap(),
                        parent.columns.iter().position(|c| c.id == *parent_id).unwrap(),
                    )).collect::<Vec<_>>();
                    let equality = collation_keys::Key::new(state, parent, ref_columns, None)
                        .map_err(CoreError::Sql)?;
                    for (key, row) in &rows {
                        if !keys.contains(key) { continue; }
                        crate::cancellation::check()?;
                        if positions.iter().any(|(i, _)| row.values[*i] == Value::Null) { continue; }
                        let values = positions.iter().map(|(i, j)| query::constraint_key(
                            &row.values[*i], &table.columns[*i].data_type, &parent.columns[*j].data_type,
                        )).collect::<R<Option<Vec<_>>>>().map_err(CoreError::Sql)?
                            .ok_or(CoreError::Sql(SqlError::ForeignKeyViolation))?;
                        let mut found = false;
                        let tuple = equality.values(&values).map_err(CoreError::Sql)?;
                        sink.fk_lookup(parent, ref_columns,
                            if equality.nondeterministic() { None } else { Some(&values) }, true,
                            &mut |candidate| Ok(equality.tuple(&candidate.values)? == tuple),
                            &mut |_| {
                            found = true;
                            Ok(())
                        })?;
                        if !found { return Err(CoreError::Sql(SqlError::ForeignKeyViolation)); }
                    }
                }
            }
        }
        pending.pending.clear();
        Ok(())
    }
    pub(super) fn drain(&mut self, state: &mut Relational, sink: &mut impl RowSink,
        clock: Option<&clock::ExecutionClock>, descriptors: &[secondary_index::Descriptor],
        version_end: u64, statement_end: bool,
    ) -> Result<(), CoreError> {
        let mut incoming = state.ext.constraints.iter().flat_map(|(id, cs)|
            cs.iter().map(move |c| (*id, c.clone()))).collect::<Vec<_>>();
        incoming.sort_by_key(|(_, c)| c.id);
        while let Some((parent_id, old, new)) = self.queue.pop_front() {
            crate::cancellation::check()?;
            let parent = state.tables.values().find(|t| t.id == parent_id).unwrap().clone();
            for (child_id, c) in &incoming {
                let ConstraintKind::ForeignKey { ref_table, columns, ref_columns, on_delete, on_update, .. } = &c.kind else { continue; };
                if *ref_table != parent_id { continue; }
                let values = ref_columns.iter().map(|id| old.values[parent.columns.iter().position(|c| c.id == *id).unwrap()].clone()).collect::<Vec<_>>();
                let new_values = new.as_ref().map(|row| ref_columns.iter().map(|id| row.values[parent.columns.iter().position(|c| c.id == *id).unwrap()].clone()).collect::<Vec<_>>());
                if values.contains(&Value::Null) || new_values.as_ref() == Some(&values) { continue; }
                let action = if new.is_some() { on_update } else { on_delete };
                let child = state.tables.values().find(|t| t.id == *child_id).unwrap().clone();
                let equality = collation_keys::Key::new(state, &parent, ref_columns, None)
                    .map_err(CoreError::Sql)?;
                let tuple = equality.values(&values).map_err(CoreError::Sql)?;
                let mut no_action = DeferralState::default();
                let mut after = None;
                let first_version = state.next_version;
                loop {
                    // Byte indexes cannot find every ICU-equal child. Read bounded
                    // physical pages at the RI snapshot, then compare parent keys.
                    let page = if equality.nondeterministic() {
                        sink.fk_read_page(&child, after)?
                    } else { sink.fk_child_page(&child, columns, &values, after)? };
                    let Some((last, mut rows)) = page else { break; };
                    after = Some(last);
                    if equality.nondeterministic() {
                        let mut matching = DdlRows::new();
                        for (key, row) in rows {
                            // A cascade can move an ICU-equal child ahead of the
                            // cursor. Do not act on this scan's own writes again.
                            if row.version >= first_version { continue; }
                            let converted = columns.iter().zip(ref_columns).map(|(a, b)| {
                                let a = child.columns.iter().position(|c| c.id == *a).unwrap();
                                let b = parent.columns.iter().position(|c| c.id == *b).unwrap();
                                query::constraint_key(&row.values[a], &child.columns[a].data_type,
                                    &parent.columns[b].data_type)
                            }).collect::<R<Option<Vec<_>>>>().map_err(CoreError::Sql)?;
                            if converted.as_ref().map(|v| equality.values(v)).transpose()
                                .map_err(CoreError::Sql)?.flatten() == tuple {
                                matching.insert(key, row);
                            }
                        }
                        rows = matching;
                    }
                    if !rows.is_empty() && *action == catalog_ext::Action::Restrict {
                        return Err(CoreError::Sql(SqlError::RestrictViolation));
                    }
                    if *action == catalog_ext::Action::NoAction {
                        let ConstraintKind::ForeignKey { deferral, .. } = &c.kind else { unreachable!() };
                        let deferred = *deferral != Deferral::NotDeferrable && self.pending.modes.get(&c.id)
                            .copied().unwrap_or(*deferral == Deferral::DeferrableDeferred);
                        let pending = if deferred { &mut self.pending } else if statement_end { &mut self.immediate } else { &mut no_action };
                        for key in rows.keys() {
                            self.retention.charge(key.len() + 96).map_err(CoreError::Sql)?;
                            pending.pending.entry((c.id, *child_id)).or_default().insert(key.clone());
                        }
                        continue;
                    }
                    let mut changed = DdlRows::new();
                    let mut events = Vec::new();
                    if new.is_some() || *action != catalog_ext::Action::Cascade {
                        for old in rows.values() {
                            let mut row = old.clone();
                            for (i, id) in columns.iter().enumerate() {
                                let position = child.columns.iter().position(|c| c.id == *id).unwrap();
                                row.values[position] = match action {
                                    catalog_ext::Action::Cascade => new_values.as_ref().unwrap()[i].clone(),
                                    catalog_ext::Action::SetNull => Value::Null,
                                    // Runs as the referencing table's owner (GD-0084 Q24).
                                    catalog_ext::Action::SetDefault => defaults::evaluate(state, &child.columns[position],
                                        clock.map(|clock| clock.with_sequence_origin(
                                            clock::SequenceOrigin::ReferentialAction(*child_id))).as_ref()).map_err(CoreError::Sql)?,
                                    _ => unreachable!(),
                                };
                            }
                            row_check(&child, &row.values).map_err(CoreError::Sql)?;
                            state.check_insert_candidate(&child, &row.values).map_err(CoreError::Sql)?;
                            row.version = allocate(&mut state.next_version).map_err(CoreError::Sql)?;
                            events.push(constraints::Event { table: child.id, old: old.clone(), new: Some(row.clone()) });
                            if changed.insert(pk(&child, &row.values).map_err(CoreError::Sql)?, row).is_some() {
                                return Err(CoreError::Sql(SqlError::Duplicate));
                            }
                        }
                    }
                    self.publish(state, sink, &child, rows, changed, events, Some(c.id), descriptors, version_end)?;
                }
                // Definition order decides NO ACTION versus a later RESTRICT.
                if !no_action.pending.is_empty() { paged_pending(state, &mut no_action, &[], false, sink)?; }
            }
            self.retention.release(searched::retained_row_bytes(&[], &old).map_err(CoreError::Sql)?
                + new.as_ref().map(|r| searched::retained_row_bytes(&[], r)).transpose().map_err(CoreError::Sql)?.unwrap_or(0));
        }
        // A drained page cannot encounter its old versions again. Keep the
        // cycle guard bounded to the page and its referential-action chain.
        self.retention.release(self.visited.len() * 96);
        self.visited.clear();
        Ok(())
    }
    #[expect(clippy::too_many_arguments, reason = "bounded action page and statement reservation")]
    pub(super) fn publish(&mut self, state: &Relational, sink: &mut impl RowSink, table: &TableDef,
        before: DdlRows, after: DdlRows, mut events: Vec<constraints::Event>, force: Option<u64>, descriptors: &[secondary_index::Descriptor], version_end: u64,
    ) -> Result<(), CoreError> {
        if state.next_version > version_end { return Err(CoreError::Sql(SqlError::Limit)); }
        let captured = events.iter().map(|e| e.old.version).collect::<BTreeSet<_>>();
        for (key, old) in &before {
            if !captured.contains(&old.version) && after.get(key) != Some(old) {
                events.push(constraints::Event { table: table.id, old: old.clone(), new: after.get(key).cloned() });
            }
        }
        for constraints::Event { old, new, .. } in events {
            if force.is_none() && new.as_ref().is_some_and(|row| row.values == old.values)
                || !state.ext.constraints.iter().any(|(owner, cs)| cs.iter().any(|c|
                    matches!(&c.kind, ConstraintKind::ForeignKey { ref_table, .. }
                        if *owner == table.id || *ref_table == table.id))) { continue; }
            let key = &pk(table, &old.values).map_err(CoreError::Sql)?;
            if self.visited.contains(&(table.id, old.version)) { continue; }
            self.retention.charge(searched::retained_row_bytes(&[], &old).map_err(CoreError::Sql)?
                + new.as_ref().map(|r| searched::retained_row_bytes(&[], r)).transpose().map_err(CoreError::Sql)?.unwrap_or(0)
                + 96).map_err(CoreError::Sql)?;
            self.visited.insert((table.id, old.version));
            let new_key = new.as_ref().map(|r| pk(table, &r.values)).transpose().map_err(CoreError::Sql)?;
            for checks in [&mut self.pending.pending, &mut self.immediate.pending] {
                for ((_, id), keys) in checks {
                    if *id == table.id && keys.remove(key) && let Some(new_key) = &new_key {
                        self.retention.charge(new_key.len() + 96).map_err(CoreError::Sql)?;
                        keys.insert(new_key.clone());
                    }
                }
            }
            if self.pending.replica { continue; }
            if let Some(row) = &new {
                for c in state.ext.constraints.get(&table.id).into_iter().flatten() {
                    let ConstraintKind::ForeignKey { columns, deferral, .. } = &c.kind else { continue; };
                    let changed = columns.iter().any(|id| {
                        let i = table.columns.iter().position(|c| c.id == *id).unwrap();
                        row.values[i] != old.values[i]
                    });
                    if force != Some(c.id) && !changed { continue; }
                    // SET DEFAULT rechecks an unchanged reference immediately, even when deferred.
                    let deferred = changed && *deferral != Deferral::NotDeferrable && self.pending.modes.get(&c.id)
                        .copied().unwrap_or(*deferral == Deferral::DeferrableDeferred);
                    let pending = if deferred { &mut self.pending } else { &mut self.immediate };
                    let key = new_key.as_ref().unwrap();
                    self.retention.charge(key.len() + 96).map_err(CoreError::Sql)?;
                    pending.pending.entry((c.id, table.id)).or_default().insert(key.clone());
                }
            }
            self.queue.push_back((table.id, old.clone(), new));
        }
        sink.check_occupancy(table, &before, &after)?;
        sink.apply_indexes(table, Some(table), &before, &after, descriptors)?;
        sink.apply(table, &before, &after)
    }
}

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
                let predicates = match &mut p.statement {
                    Statement::Update { predicates, .. } | Statement::Delete { predicates, .. } => predicates,
                    _ => unreachable!(),
                };
                predicates.reverse();
                let bound = bounds(&table, predicates, &[]).unwrap();
                assert!(bound.point);
                assert_eq!(crate::storage::engine::codec::pk_bytes(&keys[0]).unwrap(), bound.low);
                predicates.push(predicates[0].clone());
                assert!(p.mutation_lock_keys(&state, &[], &[]).is_none());
                let predicates = match &mut p.statement {
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
