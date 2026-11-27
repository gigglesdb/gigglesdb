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

pub(super) enum ParentRows<'a> {
    /// Schema validation needs old and new rows, including untouched ones.
    Images(Option<&'a Relational>),
    Changed(&'a constraints::FkPreimages),
    Pending(&'a DeferralState),
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
    parent_rows: ParentRows<'_>,
    edges: &BTreeMap<u64, Vec<Constraint>>,
    before: &mut Relational,
    staged: &mut Relational,
    sink: &mut impl RowSink,
    budget: &mut scan::Retention,
    parents_only: bool,
    write_targets: &BTreeSet<u64>,
    mut parent_probes: Option<&mut ParentProbes>,
    deferred: Option<&DeferralState>,
) -> Result<usize, CoreError> {
    if edges.is_empty() {
        return Ok(0);
    }
    let seeds = match parent_rows {
        ParentRows::Images(seeds) => seeds,
        _ => None,
    };
    let source_tables = seeds.map_or(&staged.tables, |seeds| &seeds.tables);
    let mut source_by_id = BTreeMap::new();
    let mut target_by_id = BTreeMap::new();
    for table in source_tables.values() {
        source_by_id.entry(table.id).or_insert(table);
    }
    for table in before.tables.values() {
        target_by_id.entry(table.id).or_insert(table);
    }
    let concurrent_actions = sink.row_locked_cascade();
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
                if parents_only && !parent
                    || parent && deferred.is_some_and(|state| state.deferred(c.id, &c.kind))
                {
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
                let (child_table, parent_table, direction) = if parent {
                    (source, target, query::FkKeyDirection::ToParent)
                } else {
                    (target, source, query::FkKeyDirection::ToChild)
                };
                let key_types = |source_at: usize, target_at: usize| {
                    let (child_at, parent_at) = if parent {
                        (source_at, target_at)
                    } else {
                        (target_at, source_at)
                    };
                    (&child_table.columns[child_at].data_type, &parent_table.columns[parent_at].data_type)
                };
                let exact_parent = parent_probes.is_some() && parent
                    && target.primary_key.iter().map(|i| target.columns[*i].id)
                        .eq(target_columns.iter().copied())
                    && target_columns.iter().all(|id| before.deterministic_column(*id));
                let probe_key = (c.id, source_id);
                let witnessed_rows = parent_probes.as_ref().filter(|_| exact_parent)
                    .and_then(|probes| probes.witnessed.get(&probe_key));
                let rows = if parent {
                    match &parent_rows {
                        ParentRows::Images(_) => seeds.iter().map(|seeds| &seeds.rows)
                            .chain([&before.rows, &staged.rows])
                            .filter_map(|rows| rows.get(&source_id))
                            .flat_map(|rows| rows.iter())
                            .filter(|(key, _)| !witnessed_rows.is_some_and(|known| known.contains(*key)))
                            .map(|(key, row)| (exact_parent.then(|| key.clone()), row.clone()))
                            .collect::<Vec<_>>(),
                        ParentRows::Changed(preimages) => staged.rows.get(&source_id)
                            .into_iter().flatten()
                            .filter(|(key, row)| preimages.requires_check(source_id, key, row,
                                before.rows.get(&source_id), &positions, c.id))
                            .map(|(_, row)| (None, row.clone()))
                            .collect(),
                        ParentRows::Pending(pending) => pending.pending.get(&probe_key)
                            .into_iter().flat_map(BTreeMap::keys)
                            .filter_map(|key| staged.rows.get(&source_id)?.get(key))
                            .map(|row| (None, row.clone()))
                            .collect(),
                    }
                } else {
                    // Only changed parent keys need incoming witnesses.
                    before.rows.get(&source_id).into_iter().flat_map(|rows| rows.iter())
                        .filter(|(key, row)| staged.rows.get(&source_id).and_then(|rows| rows.get(*key))
                            .is_none_or(|new| positions.iter().any(|i|
                                !constraints::key_image_eq(&row.values[*i], &new.values[*i]))))
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
                        let (child, parent) = key_types(*a, *b);
                        query::constraint_key_supported(child, parent, direction)
                    });
                    let converted = if supported {
                        values
                            .iter()
                            .zip(positions.iter().zip(&targets))
                            .map(|(v, (a, b))| {
                                let (child, parent) = key_types(*a, *b);
                                query::constraint_key(v, child, parent, direction)
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
                    let probe_values = converted.as_deref().unwrap_or(&values);
                    let canonical = probe_values.iter().zip(&targets)
                        .map(|(value, &i)| {
                            if supported {
                                key_value(&target.columns[i].data_type, value)
                            } else {
                                std::borrow::Cow::Borrowed(value)
                            }
                        }).collect::<Vec<_>>();
                    let refs = || canonical.iter().map(std::borrow::Cow::as_ref);
                    let signature = if probe_values.iter().any(Value::is_temporal) {
                        secondary_index_adapter::encode_tuple(refs())
                    } else {
                        encode_key_iter(refs())
                    }.map_err(CoreError::Sql)?;
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
                    let equality = if parent {
                        collation_keys::Key::new(before, target, target_columns, None)
                    } else {
                        collation_keys::Key::new(before, source, source_columns, None)
                    }.map_err(CoreError::Sql)?;
                    let tuple = equality.values(if parent { converted.as_deref().unwrap() } else { &values })
                        .map_err(CoreError::Sql)?;
                    let matches_child = |row: &SqlRow| -> R<bool> {
                        let candidate = targets.iter().zip(&positions).map(|(a, b)| {
                            query::constraint_key(&row.values[*a], &target.columns[*a].data_type,
                                &source.columns[*b].data_type, query::FkKeyDirection::ToParent)
                        }).collect::<R<Option<Vec<_>>>>()?;
                        Ok(candidate.as_ref().map(|v| equality.values(v)).transpose()?.flatten() == tuple)
                    };
                    let matches_target = |row: &SqlRow| -> R<bool> {
                        if parent { Ok(equality.tuple(&row.values)? == tuple) } else { matches_child(row) }
                    };
                    if !parent && concurrent_actions {
                        use catalog_ext::Action;
                        let source_key = pk(source, &row.values).map_err(CoreError::Sql)?;
                        let removed = !staged.rows[&source_id].contains_key(&source_key);
                        let action = if removed { on_delete } else { on_update };
                        // A rekey also removes the old physical key. In that
                        // case protect candidates of either writing action.
                        let writes = |action: &Action| matches!(action, Action::Cascade | Action::SetNull | Action::SetDefault);
                        let mode = if writes(action) || removed && writes(on_update) {
                            Some(if removed && *on_delete == Action::Cascade {
                                FkRowLock::Delete
                            } else {
                                FkRowLock::Update(target_columns)
                            })
                        } else if *action == Action::NoAction && *deferral != catalog_ext::Deferral::NotDeferrable {
                            // Pending references are stored by child PK. Keep a
                            // concurrent rekey from moving one past commit validation.
                            Some(FkRowLock::KeyShare)
                        } else {
                            None
                        };
                        if let Some(mode) = mode {
                            added += lock_action_witnesses(
                                target, target_columns, if collated { None } else { converted.as_deref() },
                                mode, before.rows.get_mut(&target_id).unwrap(),
                                staged.rows.get_mut(&target_id).unwrap(), sink, budget, matches_child,
                            )?;
                            continue;
                        }
                    }
                    sink.fk_lookup(
                        target,
                        target_columns,
                        if collated { None } else { converted.as_deref() },
                        parent,
                        &mut |row| if collated || !supported { matches_target(row) } else { Ok(true) },
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
                            if (collated || !parent) && !matches_target(candidate)? {
                                return Ok(());
                            }
                            witnessed = true;
                            // An append may already be physically visible. Keep
                            // staged inserts out of the statement's preimage.
                            if parent && matches!(parent_rows, ParentRows::Changed(_)) && effective.is_some() {
                                return Ok(());
                            }
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
                            if !concurrent_actions && write_targets.contains(&target.id) {
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

/// The in-memory fallback can already contain a candidate from the SQL input
/// snapshot. Reconcile unchanged preimages with the latest physical witness,
/// while leaving direct writes and earlier referential actions staged.
#[expect(clippy::too_many_arguments, reason = "FK probe, staged images, and statement retention")]
fn lock_action_witnesses(
    table: &TableDef, columns: &[u64], probe: Option<&[Value]>, mode: FkRowLock<'_>,
    old: &mut DdlRows, rows: &mut DdlRows, sink: &mut impl RowSink,
    budget: &mut scan::Retention, matches: impl Fn(&SqlRow) -> R<bool>,
) -> Result<usize, CoreError> {
    let mut candidates = DdlRows::new();
    let mut temporary = scan::Retention::new(scan::retention_limit());
    let mut add_candidate = |row: SqlRow| {
        let key = pk(table, &row.values)?;
        if old.get(&key) != rows.get(&key) || candidates.contains_key(&key) {
            return Ok(());
        }
        if candidates.len() >= MAX_ROWS { return Err(SqlError::Limit); }
        temporary.charge(searched::retained_row_bytes(&key, &row)?)?;
        candidates.insert(key, row);
        Ok(())
    };
    sink.fk_lookup(table, columns, probe, false, &mut |row| matches(row), &mut add_candidate)?;
    // A child may have left the key before the latest probe, but still be
    // present in both statement images (for example a self-referential DELETE).
    for row in old.values() {
        if matches(row).map_err(CoreError::Sql)? {
            add_candidate(row.clone()).map_err(CoreError::Sql)?;
        }
    }
    let mut updated = 0;
    for (key, candidate) in candidates {
        let current = sink.lock_fk_action_row(table, &candidate, mode)?;
        if current.as_ref() == old.get(&key) {
            // The probe's row changed before its lock. A PK move can preserve
            // the FK, so repeat discovery at a fresh snapshot.
            updated += usize::from(current.as_ref() != Some(&candidate));
            continue;
        }
        if let Some(row) = old.remove(&key) {
            // Reconciliation must charge the replacement's size too.
            budget.release(searched::retained_row_bytes(&key, &row).map_err(CoreError::Sql)?);
        }
        if let Some(row) = current
            && (rows.contains_key(&key) || matches(&row).map_err(CoreError::Sql)?)
        {
            if old.len() >= MAX_ROWS { return Err(CoreError::Sql(SqlError::Limit)); }
            budget.charge(searched::retained_row_bytes(&key, &row).map_err(CoreError::Sql)?)
                .map_err(CoreError::Sql)?;
            rows.insert(key.clone(), row.clone());
            old.insert(key, row);
        } else {
            rows.remove(&key);
        }
        updated += 1;
    }
    Ok(updated)
}

/// Validate transaction-local pending keys in physical pages, including own writes.
pub(super) fn paged_pending(
    state: &Relational, deferred: &mut DeferralState, names: &[String], path: &[String], mode: bool,
    sink: &mut impl RowSink,
) -> Result<SqlResult, CoreError> {
    let mut modes = deferred.modes.clone();
    let ids = state.constraint_modes(&mut modes, names, path, mode).map_err(CoreError::Sql)?;
    let selected = |id: &u64| names.is_empty() || ids.contains(id);
    if !mode {
        for table in state.tables.values() {
            let checks = deferred.pending.iter().filter(|((id, table_id), _)| *table_id == table.id
                && selected(id)).collect::<Vec<_>>();
            if checks.is_empty() { continue; }
            check_pending_pages(state, table, sink, |rows| {
                let mut pending = DeferralState { replica: deferred.replica, modes: deferred.modes.clone(), ..Default::default() };
                for ((id, table_id), keys) in &checks {
                    let keys = rows.keys().filter_map(|key| {
                        Some((key.clone(), keys.get(key)?.clone()))
                    }).collect::<constraints::PendingReferences>();
                    if !keys.is_empty() {
                        pending.pending.insert((*id, *table_id), keys);
                    }
                }
                pending
            })?;
        }
        deferred.pending.retain(|(id, _), _| !selected(id));
    }
    deferred.modes = modes;
    Ok(SqlResult::default())
}

pub(super) fn check_pending_pages(
    state: &Relational, table: &TableDef, sink: &mut impl RowSink,
    mut pending_for: impl FnMut(&DdlRows) -> DeferralState,
) -> Result<(), CoreError> {
    let mut after = None;
    while let Some((last, rows)) = sink.fk_read_page(table, after.take())? {
        after = Some(last);
        let mut pending = pending_for(&rows);
        if pending.pending.is_empty() { continue; }
        let mut page = state.clone();
        page.rows.insert(table.id, rows);
        let mut before = state.clone();
        let mut retention = scan::Retention::new(scan::retention_limit());
        hydrate_with_probes(ParentRows::Pending(&pending), &state.ext.constraints,
            &mut before, &mut page, sink, &mut retention, true, &BTreeSet::new(), None, None)?;
        rebuild(&mut page, &sink.indexes())?;
        page.validate_deferred(&mut pending, || crate::cancellation::check().is_err())
            .map_err(CoreError::Sql)?;
    }
    Ok(())
}
