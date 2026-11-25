//! Hydrate only FK-connected witnesses, preserving staged deletions and updates.
use super::*;

pub(super) fn rebuild(
    state: &mut Relational,
    ds: &[secondary_index::Descriptor],
) -> Result<(), CoreError> {
    state.secondary = state
        .secondary_build_complete_engine(ds, || crate::cancellation::check().is_err())
        .map_err(CoreError::Sql)?;
    Ok(())
}

/// Include indirect action targets, including cycles, without treating outgoing
/// existence witnesses as writable tables.
pub(super) fn write_targets(
    state: &Relational, prepared: &Prepared, edges: &BTreeMap<u64, Vec<Constraint>>,
) -> BTreeSet<u64> {
    let mut targets = prepared.table().into_iter().map(str::to_owned)
        .chain(prepared.write_tables().into_iter().map(|(name, _)| name))
        .filter_map(|name| state.tables.get(&name).map(|table| table.id))
        .collect::<BTreeSet<_>>();
    loop {
        let count = targets.len();
        for (child, constraints) in edges {
            for constraint in constraints {
                if let ConstraintKind::ForeignKey { ref_table, on_delete, on_update, .. } = &constraint.kind
                    && targets.contains(ref_table)
                    && [on_delete, on_update].iter().any(|action| matches!(action,
                        catalog_ext::Action::Cascade | catalog_ext::Action::SetNull | catalog_ext::Action::SetDefault))
                {
                    targets.insert(*child);
                }
            }
        }
        if targets.len() == count { return targets; }
    }
}

#[expect(clippy::too_many_arguments, reason = "FK hydration shares statement images, budget and write targets")]
pub(super) fn hydrate(
    seeds: &Relational,
    edges: &BTreeMap<u64, Vec<Constraint>>,
    before: &mut Relational,
    staged: &mut Relational,
    sink: &mut impl RowSink,
    budget: &mut scan::Retention,
    parents_only: bool,
    write_targets: &BTreeSet<u64>,
) -> Result<usize, CoreError> {
    hydrate_with_probes(
        Some(seeds), edges, before, staged, sink, budget, parents_only, write_targets, None,
    )
}

/// Completed probes are only carried across passes of one metadata-only ALTER.
/// The first positive exact lookup locks the parent and supplies its witness.
#[derive(Default)]
pub(super) struct ParentProbes {
    witnessed: BTreeMap<(u64, u64), BTreeSet<Vec<u8>>>,
    count: usize,
}

impl ParentProbes {
    fn remember(&mut self, probe_key: (u64, u64), key: Vec<u8>) {
        // Bound extra memory independently of the lookup and retention limits.
        if self.count < 1024 && self.witnessed.entry(probe_key).or_default().insert(key) {
            self.count += 1;
        }
    }
}

#[expect(clippy::too_many_arguments, reason = "the DDL caller supplies statement-local probe evidence")]
pub(super) fn hydrate_with_probes(
    seeds: Option<&Relational>,
    edges: &BTreeMap<u64, Vec<Constraint>>,
    before: &mut Relational,
    staged: &mut Relational,
    sink: &mut impl RowSink,
    budget: &mut scan::Retention,
    parents_only: bool,
    write_targets: &BTreeSet<u64>,
    mut parent_probes: Option<&mut ParentProbes>,
) -> Result<usize, CoreError> {
    if edges.is_empty() {
        return Ok(0);
    }
    let source_tables = seeds.map_or(&staged.tables, |seeds| &seeds.tables);
    let mut source_by_id = BTreeMap::new();
    let mut target_by_id = BTreeMap::new();
    for table in source_tables.values() {
        source_by_id.entry(table.id).or_insert(table);
    }
    for table in before.tables.values() {
        target_by_id.entry(table.id).or_insert(table);
    }
    let mut added = 0;
    for (child_id, cs) in edges {
        for c in cs {
            let ConstraintKind::ForeignKey {
                columns,
                ref_table,
                ref_columns, on_delete, on_update, deferral, ..
            } = &c.kind
            else {
                continue;
            };
            for (source_id, source_columns, target_id, target_columns, parent) in [
                (*child_id, columns, *ref_table, ref_columns, true),
                (*ref_table, ref_columns, *child_id, columns, false),
            ] {
                if parents_only && !parent {
                    continue;
                }
                let Some(&source) = source_by_id.get(&source_id) else {
                    continue;
                };
                let Some(&target) = target_by_id.get(&target_id) else {
                    continue;
                };
                let positions = source_columns
                    .iter()
                    .map(|id| {
                        source
                            .columns
                            .iter()
                            .position(|c| c.id == *id)
                            .ok_or(CoreError::Sql(SqlError::UnknownColumn))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let targets = target_columns
                    .iter()
                    .map(|id| {
                        target
                            .columns
                            .iter()
                            .position(|c| c.id == *id)
                            .ok_or(CoreError::Sql(SqlError::UnknownColumn))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let exact_parent = parent_probes.is_some() && parent
                    && target.primary_key.iter().map(|i| target.columns[*i].id)
                        .eq(target_columns.iter().copied())
                    && target_columns.iter().all(|id| before.deterministic_column(*id));
                let probe_key = (c.id, source_id);
                let witnessed_rows = parent_probes.as_ref().filter(|_| exact_parent)
                    .and_then(|probes| probes.witnessed.get(&probe_key));
                // Only changed parent keys need incoming witnesses. Rows loaded
                // merely to prove an outgoing FK must not expand into every sibling.
                let rows = if parent {
                    seeds.iter().map(|seeds| &seeds.rows)
                        .chain([&before.rows, &staged.rows])
                        .filter_map(|rows| rows.get(&source_id))
                        .flat_map(|rows| rows.iter())
                        .filter(|(key, _)| !witnessed_rows.is_some_and(|known| known.contains(*key)))
                        .map(|(key, row)| (exact_parent.then(|| key.clone()), row.clone()))
                        .collect::<Vec<_>>()
                } else {
                    before.rows.get(&source_id).into_iter().flat_map(|rows| rows.iter())
                        .filter(|(key, row)| staged.rows.get(&source_id).and_then(|rows| rows.get(*key))
                            .is_none_or(|new| positions.iter().any(|i| row.values[*i] != new.values[*i])))
                        .map(|(_, row)| (None, row.clone())).collect()
                };
                let mut keys = BTreeSet::new();
                for (source_key, row) in rows {
                    crate::cancellation::check()?;
                    let values = positions
                        .iter()
                        .map(|i| row.values[*i].clone())
                        .collect::<Vec<_>>();
                    if values.contains(&Value::Null) {
                        continue;
                    }
                    let supported = positions.iter().zip(&targets).all(|(a, b)| {
                        query::constraint_key_supported(
                            &source.columns[*a].data_type,
                            &target.columns[*b].data_type,
                        )
                    });
                    let converted = if supported {
                        values
                            .iter()
                            .zip(positions.iter().zip(&targets))
                            .map(|(v, (a, b))| {
                                query::constraint_key(
                                    v,
                                    &source.columns[*a].data_type,
                                    &target.columns[*b].data_type,
                                )
                            })
                            .collect::<R<Option<Vec<_>>>>()
                            .map_err(CoreError::Sql)?
                    } else {
                        None
                    };
                    if supported && converted.is_none() {
                        continue;
                    }
                    if parent && !supported {
                        return Err(CoreError::Sql(SqlError::Type));
                    }
                    // Deduplicate encoded probes without relying on Value's SQL order.
                    let signature = converted
                        .as_ref()
                        .map(|v| encode_key(v))
                        .transpose()
                        .map_err(CoreError::Sql)?;
                    if !keys.insert(signature) {
                        continue;
                    }
                    // RESTRICT and immediate NO ACTION need an existence witness,
                    // not every sibling. Keep action queues/deferred parents unchanged.
                    let existence = !parent && [on_delete, on_update].iter().all(|action|
                        matches!(action, catalog_ext::Action::Restrict)
                        || matches!(action, catalog_ext::Action::NoAction)
                            && *deferral == catalog_ext::Deferral::NotDeferrable);
                    let mut witnessed = false;
                    let collated = target_columns.iter().any(|id| !before.deterministic_column(*id));
                    let source_equality = collation_keys::Key::new(before, source, source_columns, None).map_err(CoreError::Sql)?;
                    sink.fk_lookup(
                        target,
                        target_columns,
                        if collated { None } else { converted.as_deref() },
                        parent,
                        &mut |row| {
                            if !collated { return Ok(true); }
                            let candidate = targets.iter().zip(&positions).map(|(a, b)|
                                if parent { Ok(Some(row.values[*a].clone())) } else {
                                    query::constraint_key(&row.values[*a], &target.columns[*a].data_type, &source.columns[*b].data_type)
                                }).collect::<R<Option<Vec<_>>>>()?;
                            Ok(candidate.as_ref().map(|v| source_equality.values(v)).transpose()?.flatten()
                                == source_equality.values(&values)?)
                        },
                        &mut |row| {
                            if existence && witnessed { return Ok(()); }
                            let key = pk(target, &row.values)?;
                            // A scan fallback may include nonmatching keys. Compare
                            // in the FK's supported child -> parent direction.
                            let effective = staged.rows.get(&target.id).and_then(|rows| rows.get(&key));
                            let candidate = effective.unwrap_or(&row);
                            if existence && before.rows[&target.id].contains_key(&key) && effective.is_none() {
                                return Ok(());
                            }
                            if collated {
                                let candidate_values = targets.iter().zip(&positions).map(|(a,b)|
                                    if parent { Ok(Some(candidate.values[*a].clone())) } else {
                                        query::constraint_key(&candidate.values[*a], &target.columns[*a].data_type, &source.columns[*b].data_type)
                                    }).collect::<R<Option<Vec<_>>>>()?;
                                if candidate_values.as_ref().map(|v| source_equality.values(v)).transpose()?.flatten()
                                    != source_equality.values(&values)? { return Ok(()); }
                            } else if !parent {
                                for ((a, b), value) in targets.iter().zip(&positions).zip(&values) {
                                    if query::constraint_key(&candidate.values[*a], &target.columns[*a].data_type,
                                        &source.columns[*b].data_type)?.as_ref() != Some(value) {
                                        return Ok(());
                                    }
                                }
                            }
                            witnessed = true;
                            let old = before
                                .rows
                                .get_mut(&target.id)
                                .ok_or(SqlError::UnknownTable)?;
                            if old.contains_key(&key) {
                                return Ok(());
                            }
                            budget.charge(searched::retained_row_bytes(&key, &row)?)?;
                            if old.len() >= MAX_ROWS {
                                return Err(SqlError::Limit);
                            }
                            // Only tables this statement can rewrite constrain its allocator:
                            // direct write targets and transitive referential-action children.
                            // An existence-only parent can have a newer reserved range,
                            // just like a source row, without consuming our reservation.
                            if write_targets.contains(&target.id) {
                                before.next_version = before
                                    .next_version
                                    .max(row.version.checked_add(1).ok_or(SqlError::Overflow)?);
                                staged.next_version = staged.next_version.max(before.next_version);
                            }
                            staged
                                .rows
                                .get_mut(&target.id)
                                .ok_or(SqlError::UnknownTable)?
                                .entry(key.clone())
                                .or_insert_with(|| row.clone());
                            old.insert(key, row);
                            added += 1;
                            Ok(())
                        },
                    )?;
                    if witnessed && let Some(key) = source_key
                        && let Some(probes) = parent_probes.as_mut()
                    {
                        probes.remember(probe_key, key);
                    }
                }
            }
        }
    }
    Ok(added)
}

/// Validate transaction-local pending keys in physical pages, including own writes.
pub(super) fn paged_pending(
    state: &Relational, deferred: &mut DeferralState, names: &[String], mode: bool,
    sink: &mut impl RowSink,
) -> Result<SqlResult, CoreError> {
    let mut selected = DeferralState { replica: deferred.replica, modes: deferred.modes.clone(), ..Default::default() };
    state.set_constraints(&mut selected, names, mode, || crate::cancellation::check().is_err())
        .map_err(CoreError::Sql)?;
    if !mode {
        for table in state.tables.values() {
            let checks = deferred.pending.iter().filter(|((id, table_id), _)| *table_id == table.id
                && state.ext.constraints.get(table_id).is_some_and(|cs| cs.iter().any(|c|
                    c.id == *id && (names.is_empty() || names.contains(&c.name))))).collect::<Vec<_>>();
            if checks.is_empty() { continue; }
            let mut after = None;
            while let Some((last, rows)) = sink.fk_read_page(table, after.take())? {
                after = Some(last);
                let mut pending = DeferralState { replica: deferred.replica, modes: deferred.modes.clone(), ..Default::default() };
                for ((id, table_id), keys) in &checks {
                    let keys = rows.keys().filter(|key| keys.contains(*key)).cloned().collect::<BTreeSet<_>>();
                    if !keys.is_empty() {
                        let check = (*id, *table_id);
                        if let Some(details) = deferred.parent_details.get(&check) {
                            let filtered = keys.iter().filter_map(|key| {
                                Some((key.clone(), Arc::clone(details.get(key)?)))
                            }).collect();
                            pending.parent_details.insert(check, filtered);
                        }
                        pending.pending.insert(check, keys);
                    }
                }
                if pending.pending.is_empty() { continue; }
                let mut page = state.clone();
                page.rows.insert(table.id, rows);
                let mut before = state.clone();
                let mut retention = scan::Retention::new(scan::retention_limit());
                while hydrate(&page.clone(), &state.ext.constraints, &mut before, &mut page,
                    sink, &mut retention, true, &BTreeSet::new())? > 0 {}
                rebuild(&mut page, &sink.indexes())?;
                page.validate_deferred(&mut pending, || crate::cancellation::check().is_err())
                    .map_err(CoreError::Sql)?;
            }
        }
        deferred.pending.retain(|(id, table_id), _| !state.ext.constraints.get(table_id)
            .is_some_and(|cs| cs.iter().any(|c| c.id == *id && (names.is_empty() || names.contains(&c.name)))));
        deferred.prune_parent_details();
    }
    deferred.modes = selected.modes;
    Ok(SqlResult::default())
}
