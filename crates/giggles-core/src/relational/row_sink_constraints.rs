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
) -> Result<usize, CoreError> {
    let mut added = 0;
    for (child_id, cs) in edges {
        for c in cs {
            let ConstraintKind::ForeignKey {
                columns,
                ref_table,
                ref_columns,
                ..
            } = &c.kind
            else {
                continue;
            };
            for (source_id, source_columns, target_id, target_columns, parent) in [
                (*child_id, columns, *ref_table, ref_columns, true),
                (*ref_table, ref_columns, *child_id, columns, false),
            ] {
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
                let rows = [seeds, &*before, &*staged]
                    .into_iter()
                    .filter_map(|s| s.rows.get(&source_id))
                    .flat_map(|rows| rows.values())
                    .cloned()
                    .collect::<Vec<_>>();
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
                    sink.lookup(
                        &target,
                        target_columns,
                        converted.as_deref(),
                        parent,
                        &mut |row| {
                            let key = pk(&target, &row.values)?;
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
