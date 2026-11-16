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

pub(super) fn hydrate(
    seeds: &Relational,
    edges: &BTreeMap<u64, Vec<Constraint>>,
    before: &mut Relational,
    staged: &mut Relational,
    sink: &mut impl RowSink,
    budget: &mut scan::Retention,
    parents_only: bool,
) -> Result<usize, CoreError> {
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
                let Some(source) = seeds.tables.values().find(|t| t.id == source_id) else {
                    continue;
                };
                let Some(target) = before.tables.values().find(|t| t.id == target_id).cloned()
                else {
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
                // Only changed parent keys need incoming witnesses. Rows loaded
                // merely to prove an outgoing FK must not expand into every sibling.
                let rows = if parent {
                    [seeds, &*before, &*staged].into_iter()
                        .filter_map(|s| s.rows.get(&source_id))
                        .flat_map(|rows| rows.values()).cloned().collect::<Vec<_>>()
                } else {
                    before.rows.get(&source_id).into_iter().flat_map(|rows| rows.iter())
                        .filter(|(key, row)| staged.rows.get(&source_id).and_then(|rows| rows.get(*key))
                            .is_none_or(|new| positions.iter().any(|i| row.values[*i] != new.values[*i])))
                        .map(|(_, row)| row.clone()).collect()
                };
                let mut keys = BTreeSet::new();
                for row in rows {
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
                    sink.fk_lookup(
                        &target,
                        target_columns,
                        converted.as_deref(),
                        parent,
                        &mut |row| {
                            if existence && witnessed { return Ok(()); }
                            let key = pk(&target, &row.values)?;
                            // A scan fallback may include nonmatching keys. Compare
                            // in the FK's supported child -> parent direction.
                            let effective = staged.rows.get(&target.id).and_then(|rows| rows.get(&key));
                            let candidate = effective.unwrap_or(&row);
                            if existence && before.rows[&target.id].contains_key(&key) && effective.is_none() {
                                return Ok(());
                            }
                            if !parent {
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
                            before.next_version = before
                                .next_version
                                .max(row.version.checked_add(1).ok_or(SqlError::Overflow)?);
                            staged.next_version = staged.next_version.max(before.next_version);
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
    let mut selected = DeferralState { modes: deferred.modes.clone(), pending: BTreeMap::new() };
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
                let mut pending = DeferralState { modes: deferred.modes.clone(), pending: BTreeMap::new() };
                for ((id, table_id), keys) in &checks {
                    let keys = rows.keys().filter(|key| keys.contains(*key)).cloned().collect::<BTreeSet<_>>();
                    if !keys.is_empty() { pending.pending.insert((*id, *table_id), keys); }
                }
                if pending.pending.is_empty() { continue; }
                let mut page = state.clone();
                page.rows.insert(table.id, rows);
                let mut before = state.clone();
                let mut retention = scan::Retention::new(scan::retention_limit());
                while hydrate(&page.clone(), &state.ext.constraints, &mut before, &mut page,
                    sink, &mut retention, true)? > 0 {}
                rebuild(&mut page, &sink.indexes())?;
                page.validate_deferred(&mut pending, || crate::cancellation::check().is_err())
                    .map_err(CoreError::Sql)?;
            }
        }
        deferred.pending.retain(|(id, table_id), _| !state.ext.constraints.get(table_id)
            .is_some_and(|cs| cs.iter().any(|c| c.id == *id && (names.is_empty() || names.contains(&c.name)))));
    }
    deferred.modes = selected.modes;
    Ok(SqlResult::default())
}
