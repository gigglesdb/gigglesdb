//! Bounded constraint lookups over authoritative primary and secondary keys.
use super::*;

type Rows = Vec<(Vec<u8>, SqlRow)>;

fn tick(cancel: &mut impl FnMut() -> bool) -> R<()> {
    if cancel() {
        Err(SqlError::Aborted)
    } else {
        Ok(())
    }
}
fn positions(table: &TableDef, columns: &[u64], values: &[Value]) -> R<Vec<usize>> {
    if columns.is_empty() || columns.len() != values.len() || columns.len() > MAX_COLUMNS {
        return Err(SqlError::InvalidForeignKey);
    }
    let mut result = Vec::new();
    for id in columns {
        let at = table
            .columns
            .iter()
            .position(|c| c.id == *id)
            .ok_or(SqlError::UnknownColumn)?;
        if result.contains(&at) {
            return Err(SqlError::InvalidForeignKey);
        }
        result.push(at);
    }
    Ok(result)
}
fn permutation(wanted: &[u64], columns: &[u64]) -> Option<Vec<usize>> {
    wanted
        .iter()
        .map(|id| columns.iter().position(|c| c == id))
        .collect()
}
fn primary_key(table: &TableDef, columns: &[u64], values: &[Value]) -> R<Option<Vec<u8>>> {
    if table.hidden_rowid().is_some() || table.primary_key.len() != columns.len() {
        return Ok(None);
    }
    let ids: Vec<_> = table
        .primary_key
        .iter()
        .map(|i| table.columns[*i].id)
        .collect();
    let Some(order) = permutation(&ids, columns) else {
        return Ok(None);
    };
    encode_key(&order.iter().map(|i| values[*i].clone()).collect::<Vec<_>>()).map(Some)
}
fn indexed(
    state: &Relational,
    table: &TableDef,
    columns: &[u64],
    values: &[Value],
    unique: bool,
    cancel: &mut impl FnMut() -> bool,
) -> R<Option<Rows>> {
    let descriptor = state
        .secondary_descriptors_engine()
        .iter()
        .filter(|d| {
            d.table == table.id && d.predicate.is_none() && (!unique || d.unique && d.columns.len() == columns.len())
        })
        .filter(|d| {
            d.columns.len() >= columns.len()
                && permutation(&d.columns[..columns.len()], columns).is_some()
        })
        .min_by_key(|d| d.id);
    let Some(descriptor) = descriptor else {
        return Ok(None);
    };
    let order = permutation(&descriptor.columns[..columns.len()], columns).ok_or(SqlError::Type)?;
    secondary_index_adapter::tuple_len(order.iter().map(|i| &values[*i]))?;
    let lower = secondary_index_adapter::encode_tuple(order.iter().map(|i| &values[*i]))?;
    let upper = secondary_index_keys::successor(lower.clone());
    let (sidecar, base) = state.secondary_access_engine(table, state.epoch, &mut *cancel)?;
    let hits = sidecar
        .lookup(descriptor.id, Some(&lower), upper.as_deref(), &mut *cancel)
        .map_err(|e| match e {
            secondary_index::Error::Cancelled => SqlError::Aborted,
            secondary_index::Error::Limit => SqlError::Limit,
            _ => SqlError::Type,
        })?;
    if hits.len() > MAX_ROWS {
        return Err(SqlError::Limit);
    }
    let mut rows = Vec::with_capacity(hits.len());
    for (_, key) in hits {
        tick(cancel)?;
        rows.push((key.to_vec(), base.get(key).ok_or(SqlError::Type)?.clone()));
    }
    tick(cancel)?;
    Ok(Some(rows))
}

/// Parent lookup requires an exact PK or UNIQUE column set. Column order may differ.
/// The secondary sidecar must describe the same snapshot as the base rows.
pub(super) fn exists(
    state: &Relational,
    table: &TableDef,
    columns: &[u64],
    values: &[Value],
    cancel: &mut impl FnMut() -> bool,
) -> R<bool> {
    tick(cancel)?;
    positions(table, columns, values)?;
    if columns.iter().any(|id| !state.deterministic_column(*id)) {
        return Ok(!matching_rows(state, table, columns, values, cancel)?.is_empty());
    }
    if let Some(key) = primary_key(table, columns, values)? {
        let result = state
            .rows
            .get(&table.id)
            .ok_or(SqlError::UnknownTable)?
            .contains_key(&key);
        tick(cancel)?;
        return Ok(result);
    }
    indexed(state, table, columns, values, true, cancel)?
        .map(|rows| !rows.is_empty())
        .ok_or(SqlError::InvalidForeignKey)
}

/// Child lookup uses a matching index prefix, otherwise scans at most MAX_ROWS.
/// Returned keys and row images belong to the same immutable snapshot.
pub(super) fn matching_rows_typed(
    state: &Relational,
    table: &TableDef,
    columns: &[u64],
    values: &[Value],
    parent_types: &[DataType],
    cancel: &mut impl FnMut() -> bool,
) -> R<Rows> {
    tick(cancel)?;
    let offsets = positions(table, columns, values)?;
    if parent_types.len() != values.len() {
        return Err(SqlError::InvalidForeignKey);
    }
    if offsets
        .iter()
        .zip(parent_types)
        .all(|(i, parent)| query::constraint_key_supported(
            &table.columns[*i].data_type, parent, query::FkKeyDirection::ToChild,
        ))
    {
        let converted = values
            .iter()
            .zip(parent_types.iter().zip(&offsets))
            .map(|(value, (parent, i))| {
                query::constraint_key(value, &table.columns[*i].data_type, parent, query::FkKeyDirection::ToChild)
            })
            .collect::<R<Option<Vec<_>>>>()?;
        return match converted {
            Some(values) => matching_rows(state, table, columns, &values, cancel),
            None => Ok(vec![]),
        };
    }
    // Some join keys are directional (for example integer -> float). A parent
    // float cannot identify one integer key, so compare each child in the
    // supported child -> parent direction instead of probing an invalid key.
    let base = state.rows.get(&table.id).ok_or(SqlError::UnknownTable)?;
    if base.len() > MAX_ROWS {
        return Err(SqlError::Limit);
    }
    // A nondeterministic collation is identical on both sides of the FK.
    let equality = collation_keys::Key::new(state, table, columns, None)?;
    let tuple = equality.values(values)?;
    let mut result = Vec::new();
    for (key, row) in base {
        tick(cancel)?;
        let converted = offsets.iter().zip(parent_types).map(|(i, parent)| {
            query::constraint_key(&row.values[*i], &table.columns[*i].data_type, parent, query::FkKeyDirection::ToParent)
        }).collect::<R<Option<Vec<_>>>>()?;
        if let Some(converted) = converted
            && equality.values(&converted)? == tuple
        {
            result.push((key.clone(), row.clone()));
        }
    }
    tick(cancel)?;
    Ok(result)
}

pub(super) fn matching_rows(
    state: &Relational,
    table: &TableDef,
    columns: &[u64],
    values: &[Value],
    cancel: &mut impl FnMut() -> bool,
) -> R<Rows> {
    tick(cancel)?;
    positions(table, columns, values)?;
    let base = state.rows.get(&table.id).ok_or(SqlError::UnknownTable)?;
    let equality = collation_keys::Key::new(state, table, columns, None)?;
    if !equality.nondeterministic() && let Some(key) = primary_key(table, columns, values)? {
        let result = base
            .get(&key)
            .map(|row| vec![(key, row.clone())])
            .unwrap_or_default();
        tick(cancel)?;
        return Ok(result);
    }
    if !equality.nondeterministic() && let Some(rows) = indexed(state, table, columns, values, false, cancel)? {
        return Ok(rows);
    }
    if base.len() > MAX_ROWS {
        return Err(SqlError::Limit);
    }
    let mut result = Vec::new();
    let wanted = equality.values(values)?;
    for (key, row) in base {
        tick(cancel)?;
        if wanted.is_some() && equality.tuple(&row.values)? == wanted {
            result.push((key.clone(), row.clone()));
        }
    }
    tick(cancel)?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn run(state: &mut Relational, sql: &str) {
        let p = Prepared::parse(sql, 1, state.epoch).unwrap();
        state.execute(&p, &[], None).unwrap();
    }
    #[test]
    fn composite_primary_unique_and_child_prefix_permutations() {
        let mut state = Relational::default();
        run(
            &mut state,
            "CREATE TABLE p(a int,b int,c int,PRIMARY KEY(a,b))",
        );
        run(&mut state, "INSERT INTO p VALUES(1,2,3),(4,5,6)");
        let table = state.tables["p"].clone();
        let ids = |names: &[usize]| {
            names
                .iter()
                .map(|i| table.columns[*i].id)
                .collect::<Vec<_>>()
        };
        assert!(
            exists(
                &state,
                &table,
                &ids(&[1, 0]),
                &[Value::Int(2), Value::Int(1)],
                &mut || false
            )
            .unwrap()
        );
        assert!(
            !exists(
                &state,
                &table,
                &ids(&[1, 0]),
                &[Value::Int(2), Value::Int(4)],
                &mut || false
            )
            .unwrap()
        );
        run(&mut state, "CREATE UNIQUE INDEX p_cb ON p(c,b)");
        assert!(
            exists(
                &state,
                &table,
                &ids(&[1, 2]),
                &[Value::Int(2), Value::Int(3)],
                &mut || false
            )
            .unwrap()
        );
        assert_eq!(
            matching_rows(&state, &table, &ids(&[2]), &[Value::Int(3)], &mut || false)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            matching_rows(&state, &table, &ids(&[1]), &[Value::Int(2)], &mut || false)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            matching_rows(&state, &table, &ids(&[1]), &[Value::Int(2)], &mut || true),
            Err(SqlError::Aborted)
        );
    }
}
