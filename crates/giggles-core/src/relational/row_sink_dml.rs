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
    let mut output = scan::Retention::new(MAX_RESULT_BYTES);
    sink.ddl_pages(&table, &table, true, &mut |mut rows| {
        let mut unchanged = DdlRows::new();
        if let Some(keys) = &keys {
            rows.retain(|key, row| {
                if keys.contains(key) { true } else { unchanged.insert(key.clone(), row.clone()); false }
            });
        }
        let mut page = state.clone();
        let before = rows.clone();
        page.rows.insert(table.id, rows);
        for constraints in page.ext.constraints.values_mut() {
            constraints.retain(|c| !matches!(c.kind, ConstraintKind::ForeignKey { .. }));
        }
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
        for row in &batch.rows {
            output
                .charge(searched::retained_row_bytes(&[], row).map_err(CoreError::Sql)?)
                .map_err(CoreError::Sql)?;
        }
        result.affected += batch.affected;
        result.rows.extend(batch.rows);
        for (key, new) in &page.rows[&table.id] {
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
    Ok(result)
}

// Complete predicate evaluation precedes publication, including self-subqueries.
type MutationSelection = (Prepared, Option<BTreeSet<Vec<u8>>>);
fn selection(state: &Relational, prepared: &Prepared, params: &[Value],
    clock: Option<&clock::ExecutionClock>, sink: &mut impl RowSink,
) -> Result<MutationSelection, CoreError> {
    let keys = searched::target(prepared).filter(|q| moves_primary_key(state, prepared) || q.has_in_subquery())
        .map(|_| sink.mutation_keys(state, prepared, params, clock)).transpose()?;
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
    if state.ext.constraints.values().flatten().any(|c| matches!(c.kind, ConstraintKind::ForeignKey { .. })) {
        sink.check_fk_snapshot()?;
    }
    let pk_move = moves_primary_key(state, prepared);
    let pending = deferred.as_deref().cloned().unwrap_or_default();
    let table = state.tables[prepared.table().unwrap()].clone();
    let descriptors = sink.indexes();
    let mut result = state.execute_statement_inner(prepared, params, None,
        || crate::cancellation::check().is_err(), clock, None, &mut Vec::new()).map_err(CoreError::Sql)?;
    let (template, keys) = selection(state, prepared, params, clock, sink)?;
    let prepared = &template;
    let first_version = state.next_version;
    let mut work = ActionWork { queue: std::collections::VecDeque::new(),
        retention: scan::Retention::new(scan::retention_limit()), visited: BTreeSet::new(), pending, immediate: DeferralState::default() };
    let mut output = scan::Retention::new(MAX_RESULT_BYTES);
    let mut after = None;
    while let Some((last, mut rows)) = sink.read_page(&table, after)? {
        after = Some(last);
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
            for cs in page.ext.constraints.values_mut() {
                cs.retain(|c| !matches!(c.kind, ConstraintKind::ForeignKey { .. }));
            }
            rebuild(&mut page, &descriptors)?;
            let mut events = Vec::new();
            let batch = page.execute_statement_inner(prepared, params, None,
                || crate::cancellation::check().is_err(), clock, None, &mut events).map_err(CoreError::Sql)?;
            for row in &batch.rows {
                output.charge(searched::retained_row_bytes(&[], row).map_err(CoreError::Sql)?).map_err(CoreError::Sql)?;
            }
            result.affected += batch.affected;
            result.rows.extend(batch.rows);
            state.next_version = page.next_version;
            work.publish(state, sink, &table, rows, page.rows.remove(&table.id).unwrap(), events, None, &descriptors, version_end)?;
        }
    }
    while let Some((parent_id, old, new)) = work.queue.pop_front() {
        crate::cancellation::check()?;
        let parent = state.tables.values().find(|t| t.id == parent_id).unwrap().clone();
        let mut incoming = state.ext.constraints.iter().flat_map(|(id, cs)| cs.iter().map(move |c| (*id, c.clone()))).collect::<Vec<_>>();
        incoming.sort_by_key(|(_, c)| c.id);
        for (child_id, c) in incoming {
                let ConstraintKind::ForeignKey { ref_table, columns, ref_columns, on_delete, on_update, .. } = &c.kind else { continue; };
                if *ref_table != parent_id { continue; }
                let values = ref_columns.iter().map(|id| old.values[parent.columns.iter().position(|c| c.id == *id).unwrap()].clone()).collect::<Vec<_>>();
                let new_values = new.as_ref().map(|row| ref_columns.iter().map(|id| row.values[parent.columns.iter().position(|c| c.id == *id).unwrap()].clone()).collect::<Vec<_>>());
                if values.contains(&Value::Null) || new_values.as_ref() == Some(&values) { continue; }
                let action = if new.is_some() { on_update } else { on_delete };
                let child = state.tables.values().find(|t| t.id == child_id).unwrap().clone();
                let mut no_action = DeferralState::default();
                let mut after = None;
                while let Some((last, rows)) = sink.child_page(&child, columns, &values, after)? {
                    after = Some(last);
                    if !rows.is_empty() && *action == catalog_ext::Action::Restrict {
                        return Err(CoreError::Sql(SqlError::RestrictViolation));
                    }
                    if *action == catalog_ext::Action::NoAction {
                        let ConstraintKind::ForeignKey { deferral, .. } = &c.kind else { unreachable!() };
                        let deferred = *deferral != Deferral::NotDeferrable && work.pending.modes.get(&c.id)
                            .copied().unwrap_or(*deferral == Deferral::DeferrableDeferred);
                        let pending = if deferred { &mut work.pending } else { &mut no_action };
                        for key in rows.keys() {
                            work.retention.charge(key.len() + 96).map_err(CoreError::Sql)?;
                            pending.pending.entry((c.id, child_id)).or_default().insert(key.clone());
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
                                    catalog_ext::Action::SetDefault => defaults::evaluate(state, &child.columns[position], clock).map_err(CoreError::Sql)?,
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
                    work.publish(state, sink, &child, rows, changed, events, Some(c.id), &descriptors, version_end)?;
                }
                // Definition order decides NO ACTION versus a later RESTRICT.
                if !no_action.pending.is_empty() { paged_pending(state, &mut no_action, &[], false, sink)?; }
        }
        work.retention.release(searched::retained_row_bytes(&[], &old).map_err(CoreError::Sql)?
            + new.as_ref().map(|r| searched::retained_row_bytes(&[], r)).transpose().map_err(CoreError::Sql)?.unwrap_or(0));
    }
    paged_pending(state, &mut work.immediate, &[], false, sink)?;
    if deferred.is_none() { paged_pending(state, &mut work.pending, &[], false, sink)?; }
    if let Some(d) = deferred { *d = work.pending; }
    Ok(result)
}


struct ActionWork {
    queue: std::collections::VecDeque<(u64, SqlRow, Option<SqlRow>)>,
    visited: BTreeSet<(u64, u64)>,
    retention: scan::Retention,
    pending: DeferralState,
    immediate: DeferralState,
}
impl ActionWork {
    #[expect(clippy::too_many_arguments, reason = "bounded action page and statement reservation")]
    fn publish(&mut self, state: &Relational, sink: &mut impl RowSink, table: &TableDef,
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
