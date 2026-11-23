//! MVCC secondary entries. The catalog publishes descriptors and their feature bits.
use super::{
    codec,
    kv::{Direction, Keyspace},
    txn::Transaction,
};
use crate::{
    CoreError,
    relational::{self, SqlError, SqlRow, TableDef, Value, secondary_index::Descriptor},
};
use bincode::Options;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, ops::Bound, time::Instant};

// Independent wire DTO: future changes to the legacy sidecar cannot alter v2.
#[derive(Serialize, Deserialize)]
struct DescriptorDto {
    id: u64,
    table: u64,
    name: String,
    columns: Vec<u64>,
    key_encoding: u32,
    unique: bool,
}
pub(crate) fn descriptor_key(id: u64) -> Vec<u8> {
    let mut key = vec![0x10];
    key.extend(id.to_be_bytes());
    key
}
pub(crate) fn encode_descriptor(d: &Descriptor) -> Result<Vec<u8>, CoreError> {
    let mut bytes = vec![1];
    let wire = DescriptorDto {
        id: d.id,
        table: d.table,
        name: d.name.clone(),
        columns: d.columns.clone(),
        key_encoding: d.key_encoding,
        unique: d.unique,
    };
    bytes.extend(crate::storage::encode(&wire)?);
    if let Some(predicate) = &d.predicate {
        bytes.extend(crate::storage::encode(predicate)?);
    }
    Ok(bytes)
}
pub(crate) fn decode_descriptor(bytes: &[u8]) -> Result<Descriptor, CoreError> {
    let Some((1, wire)) = bytes.split_first() else {
        return Err(CoreError::Corrupt("index descriptor version".into()));
    };
    let mut base = std::io::Cursor::new(wire);
    let d: DescriptorDto = crate::storage::codec().deserialize_from(&mut base)
        .map_err(|err| CoreError::Corrupt(err.to_string()))?;
    let suffix = &wire[base.position() as usize..];
    let predicate: Option<relational::partial_index::Predicate> =
        if suffix.is_empty() { None } else { Some(crate::storage::decode(suffix)?) };
    if predicate.as_ref().is_some_and(|p| {
        p.sql.is_empty() || p.sql.len() > relational::partial_index::MAX_PREDICATE_BYTES
            || p.columns.len() > relational::secondary_index::MAX_ARITY
            || p.columns.iter().enumerate().any(|(i, id)| *id == 0 || p.columns[..i].contains(id))
    }) {
        return Err(CoreError::Corrupt("invalid index predicate".into()));
    }
    if d.id == 0
        || d.table == 0
        || d.key_encoding != 1
        || d.columns.is_empty()
        || d.columns.len() > relational::secondary_index::MAX_ARITY
    {
        return Err(CoreError::Corrupt("invalid index descriptor".into()));
    }
    let (schema, name) = relational::split_key(&d.name);
    if schema.is_empty()
        || schema.len() > 63
        || name.is_empty()
        || name.len() > 63
        || name.contains('\0')
    {
        return Err(CoreError::Corrupt("invalid index name".into()));
    }
    Ok(Descriptor {
        id: d.id,
        table: d.table,
        name: d.name,
        columns: d.columns,
        key_encoding: d.key_encoding,
        unique: d.unique,
        predicate,
    })
}
pub(crate) fn validate_descriptor(d: &Descriptor, table: &TableDef) -> Result<(), CoreError> {
    decode_descriptor(&encode_descriptor(d)?)?;
    validate_descriptor_columns(d, table)
}

/// Cross-record checks for a descriptor that has already passed decoding.
pub(super) fn validate_descriptor_columns(d: &Descriptor, table: &TableDef) -> Result<(), CoreError> {
    if d.table != table.id
        || d.predicate.as_ref().is_some_and(|p| p.columns.iter().any(|id| !table.columns.iter().any(|c| c.id == *id)))
        || d.columns.iter().enumerate().any(|(i, id)| {
            d.columns[..i].contains(id) || !table.columns.iter().any(|c| c.id == *id)
        })
    {
        return Err(CoreError::Corrupt("index column absent or repeated".into()));
    }
    Ok(())
}
pub(crate) fn descriptors(txn: &mut Transaction, table: u64) -> Result<Vec<Descriptor>, CoreError> {
    txn.scan(
        Keyspace::Catalog,
        Bound::Included(&[16]),
        Bound::Excluded(&[17]),
        Direction::Forward,
        None,
    )?
    .map(|pair| {
        let (key, bytes) = pair?;
        let d = decode_descriptor(&bytes)?;
        if key != descriptor_key(d.id) {
            return Err(CoreError::Corrupt("index key/id mismatch".into()));
        }
        Ok(d)
    })
    .filter(|d| d.as_ref().map_or(true, |d| d.table == table))
    .collect()
}
pub(crate) fn entries(
    state: Option<&relational::Relational>,
    table: &TableDef,
    rows: &BTreeMap<Vec<u8>, SqlRow>,
    ds: &[Descriptor],
) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, CoreError> {
    entries_for(Predicates::Bind(state), table, rows, ds, false)
}

pub(crate) fn build_entries(
    state: &relational::Relational,
    table: &TableDef,
    rows: &BTreeMap<Vec<u8>, SqlRow>,
    index: &Descriptor,
) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, CoreError> {
    entries_for(Predicates::Bind(Some(state)), table, rows, std::slice::from_ref(index), true)
}

#[derive(Clone, Copy)]
enum Predicates<'a> {
    Bind(Option<&'a relational::Relational>),
    Cached(&'a super::catalog::IndexPredicateImage),
}

fn entries_for(
    predicates: Predicates<'_>,
    table: &TableDef,
    rows: &BTreeMap<Vec<u8>, SqlRow>,
    ds: &[Descriptor],
    build: bool,
) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, CoreError> {
    let mut entries = BTreeMap::new();
    for d in ds.iter().filter(|d| d.table == table.id) {
        validate_descriptor(d, table)?;
        let bound;
        let predicate = match (&d.predicate, predicates) {
            (None, _) => None,
            (Some(_), Predicates::Cached(image)) => Some(image.get(table, d)?),
            (Some(predicate), Predicates::Bind(state)) => {
                bound = predicate.bind(state.ok_or(CoreError::Sql(SqlError::InternalError))?, table).map_err(CoreError::Sql)?;
                Some(&bound)
            }
        };
        let offsets = d
            .columns
            .iter()
            .map(|id| {
                table
                    .columns
                    .iter()
                    .position(|c| c.id == *id)
                    .ok_or_else(|| CoreError::Corrupt("index column absent".into()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        for row in rows.values() {
            crate::cancellation::check()?;
            if predicate.as_ref().map(|p| p.matches(&row.values)).transpose().map_err(CoreError::Sql)? == Some(false) {
                continue;
            }
            let values = offsets
                .iter()
                .map(|i| row.values[*i].clone())
                .collect::<Vec<_>>();
            let pk = codec::pk_bytes(
                &table
                    .primary_key
                    .iter()
                    .map(|i| row.values[*i].clone())
                    .collect::<Vec<_>>(),
            )?;
            let mut key = d.id.to_be_bytes().to_vec();
            key.extend(codec::index_tuple_bytes(
                &values,
                &vec![codec::Direction::Asc; values.len()],
            )?);
            // SQL UNIQUE permits multiple NULL tuples; their physical entries
            // use the same PK suffix as non-unique indexes.
            if !d.unique || values.contains(&Value::Null) {
                key.extend(&pk);
            }
            if entries.insert(key, pk).is_some() {
                return Err(CoreError::Sql(relational::collation_keys::index_duplicate(
                    table, d, &row.values, build,
                )));
            }
        }
    }
    Ok(entries)
}
pub(crate) fn occupancy_duplicate(
    table: &TableDef,
    rows: &BTreeMap<Vec<u8>, SqlRow>,
    ds: &[Descriptor],
    key: &[u8],
    pk: &[u8],
    build: bool,
) -> CoreError {
    let index = ds.iter().find(|d| d.unique && key.starts_with(&d.id.to_be_bytes()));
    // The native single-row publisher uses an empty key for its one row.
    let row = rows.get(pk).or_else(|| rows.get(&[][..]));
    CoreError::Sql(match (index, row) {
        (Some(index), Some(row)) => relational::collation_keys::index_duplicate(
            table, index, &row.values, build,
        ),
        _ => SqlError::Duplicate,
    })
}

/// Lock in physical order, then check the latest committed owner after waiting.
/// Callers own the statement savepoint, so row and index failures roll back together.
#[allow(clippy::too_many_arguments)]
pub(crate) fn apply(
    txn: &mut Transaction,
    state: Option<&relational::Relational>,
    table: &TableDef,
    after_table: Option<&TableDef>,
    before: &BTreeMap<Vec<u8>, SqlRow>,
    after: &BTreeMap<Vec<u8>, SqlRow>,
    old_ds: &[Descriptor],
    new_ds: &[Descriptor],
    deadline: Option<Instant>,
    cancel: &dyn Fn() -> bool,
) -> Result<(), CoreError> {
    let image;
    let predicates = if state.is_none() && old_ds.iter().chain(new_ds).any(|d| d.predicate.is_some()) {
        image = txn.index_predicates()?;
        Predicates::Cached(&image)
    } else {
        Predicates::Bind(state)
    };
    let old = entries_for(predicates, table, before, old_ds, false)?;
    let new = match after_table {
        Some(table) => entries_for(predicates, table, after, new_ds, false)?,
        None => BTreeMap::new(),
    };
    let changes = old
        .keys()
        .chain(new.keys())
        .filter(|key| old.get(*key) != new.get(*key))
        .map(|key| (key.clone(), new.get(key).cloned()))
        .collect::<BTreeMap<_, _>>();
    for key in changes.keys() {
        let target = txn
            .lock_for_write(Keyspace::Index, key, deadline, cancel)
            .map_err(crate::task::limits::lock_error)?;
        if target.current.is_some() && target.current.as_ref() != old.get(key) {
            return Err(match (after_table, new.get(key)) {
                (Some(table), Some(pk)) => occupancy_duplicate(table, after, new_ds, key, pk, false),
                _ => CoreError::Sql(SqlError::Duplicate),
            });
        }
    }
    if !changes.is_empty() {
        txn.require_features(super::meta::SECONDARY_INDEXES);
    }
    for (key, value) in changes {
        match value {
            Some(pk) => txn.put(Keyspace::Index, key, pk)?,
            None => txn.delete(Keyspace::Index, key)?,
        }
    }
    Ok(())
}
/// Lock proposed UNIQUE owners without publishing, preserving FK error precedence.
pub(crate) fn check_occupancy(
    txn: &mut Transaction, state: &relational::Relational, table: &TableDef, before: &BTreeMap<Vec<u8>, SqlRow>,
    after: &BTreeMap<Vec<u8>, SqlRow>, ds: &[Descriptor],
) -> Result<(), CoreError> {
    let old = entries(Some(state), table, before, ds)?;
    for (key, pk) in entries(Some(state), table, after, ds)? {
        if old.get(&key) == Some(&pk) { continue; }
        let target = txn
            .lock_for_write(Keyspace::Index, &key, crate::task::limits::lock_deadline(),
                &|| crate::cancellation::check().is_err())
            .map_err(crate::task::limits::lock_error)?;
        if target.current.is_some() && target.current.as_ref() != old.get(&key) {
            return Err(occupancy_duplicate(table, after, ds, &key, &pk, false));
        }
    }
    Ok(())
}

/// Row-store operations use the same entry publisher as SQL statements.
pub(crate) fn maintain(
    txn: &mut Transaction,
    table: &TableDef,
    before: Option<&super::rows::Row>,
    after: Option<&super::rows::Row>,
    deadline: Option<Instant>,
    cancel: &dyn Fn() -> bool,
) -> Result<(), CoreError> {
    let ds = descriptors(txn, table.id)?;
    if ds.is_empty() {
        return Ok(());
    }
    let rows = |row: Option<&super::rows::Row>| {
        row.map(|r| {
            (
                Vec::new(),
                SqlRow {
                    values: r.values.clone(),
                    version: r.version,
                },
            )
        })
        .into_iter()
        .collect()
    };
    apply(
        txn,
        None,
        table,
        Some(table),
        &rows(before),
        &rows(after),
        &ds,
        &ds,
        deadline,
        cancel,
    )
}

/// Point/range access over encoded index tuples, followed by snapshot PK reads.
/// Included/excluded tuple bounds apply to every PK suffix in that tuple.
pub(crate) fn scan(
    txn: &mut Transaction,
    table: &super::catalog::TableEntry,
    descriptor: &Descriptor,
    lower: Bound<Vec<u8>>,
    upper: Bound<Vec<u8>>,
) -> Result<Vec<(Vec<u8>, super::rows::Row)>, CoreError> {
    let mut rows = Vec::new();
    let mut retention = relational::scan::Retention::new(relational::scan::retention_limit());
    visit(txn, table, descriptor, lower, upper, &mut |pk, row| {
        retention.charge(row.values.iter().map(relational::value_bytes).sum::<usize>())
            .map_err(CoreError::Sql)?;
        if rows.len() >= relational::MAX_ROWS { return Err(CoreError::Limit); }
        rows.push((pk, row));
        Ok(())
    })?;
    Ok(rows)
}

/// Page index witnesses before fetching their rows, releasing the scan borrow.
pub(crate) fn visit(
    txn: &mut Transaction,
    table: &super::catalog::TableEntry,
    descriptor: &Descriptor,
    lower: Bound<Vec<u8>>,
    upper: Bound<Vec<u8>>,
    visitor: &mut dyn FnMut(Vec<u8>, super::rows::Row) -> Result<(), CoreError>,
) -> Result<(), CoreError> {
    let mut after = None;
    loop {
        after = visit_page(txn, table, descriptor, lower.clone(), upper.clone(), after, visitor, 200)?;
        if after.is_none() { return Ok(()); }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn visit_page(
    txn: &mut Transaction,
    table: &super::catalog::TableEntry,
    descriptor: &Descriptor,
    lower: Bound<Vec<u8>>,
    upper: Bound<Vec<u8>>,
    after: Option<Vec<u8>>,
    visitor: &mut dyn FnMut(Vec<u8>, super::rows::Row) -> Result<(), CoreError>,
    page_rows: usize,
) -> Result<Option<Vec<u8>>, CoreError> {
    validate_descriptor(descriptor, &table.def)?;
    let image;
    let predicates = if descriptor.predicate.is_some() {
        image = txn.index_predicates()?;
        Predicates::Cached(&image)
    } else {
        Predicates::Bind(None)
    };
    let prefix = descriptor.id.to_be_bytes().to_vec();
    let prefix_key = |tuple: Vec<u8>| {
        let mut key = prefix.clone();
        key.extend(tuple);
        key
    };
    let lo = match lower {
        Bound::Unbounded => Bound::Included(prefix.clone()),
        Bound::Included(tuple) => Bound::Included(prefix_key(tuple)),
        Bound::Excluded(tuple) => match successor(prefix_key(tuple)) {
            Some(key) => Bound::Included(key),
            None => return Ok(None),
        },
    };
    let hi = match upper {
        Bound::Unbounded => successor(prefix.clone()).map_or(Bound::Unbounded, Bound::Excluded),
        Bound::Included(tuple) => {
            successor(prefix_key(tuple)).map_or(Bound::Unbounded, Bound::Excluded)
        }
        Bound::Excluded(tuple) => Bound::Excluded(prefix_key(tuple)),
    };
    let after = after.map_or(lo, Bound::Excluded);
        let mut retention = relational::scan::Retention::new(relational::scan::retention_limit());
        let mut entries = Vec::new();
        for pair in txn.scan(Keyspace::Index, after.as_ref().map(Vec::as_slice),
            hi.as_ref().map(Vec::as_slice), Direction::Forward, Some(page_rows))? {
            let (key, pk) = pair?;
            // A page is a batch, not a result: a full page budget ends the page
            // early (the caller resumes after its last key); only a single
            // oversized entry fails.
            match retention.charge(key.len().saturating_add(pk.len()).saturating_add(64)) {
                Err(error) if entries.is_empty() => return Err(CoreError::Sql(error)),
                Err(_) => break,
                Ok(()) => entries.push((key, pk)),
            }
        }
        let last = entries.last().map(|(key, _)| key.clone());
        let mut visited = None;
        for (key, pk) in entries {
        crate::cancellation::check()?;
        if !key.starts_with(&prefix) {
            break;
        }
        let mut row_key = table.def.id.to_be_bytes().to_vec();
        row_key.extend(&pk);
        let bytes = txn
            .get(Keyspace::Rows, &row_key)?
            .ok_or_else(|| CoreError::Corrupt("dangling index entry".into()))?;
        let charged = retention.charge(bytes.len().saturating_mul(2).saturating_add(
            table.def.columns.len().saturating_mul(std::mem::size_of::<Value>()),
        ));
        match charged {
            Err(error) if visited.is_none() => return Err(CoreError::Sql(error)),
            Err(_) => return Ok(visited),
            Ok(()) => {}
        }
        let row = super::rows::decode(&row_key, &bytes, table.def.columns.len())?;
        let witness = BTreeMap::from([(
            Vec::new(),
            SqlRow {
                values: row.values.clone(),
                version: row.version,
            },
        )]);
        let expected = entries_for(predicates, &table.def, &witness, std::slice::from_ref(descriptor), false)?;
        if expected.get(&key) != Some(&pk) {
            return Err(CoreError::Corrupt("index entry disagrees with row".into()));
        }
        visitor(pk, row)?;
        visited = Some(key);
        }
    Ok(last)
}
pub(crate) fn successor(mut prefix: Vec<u8>) -> Option<Vec<u8>> {
    while let Some(last) = prefix.pop() {
        if let Some(next) = last.checked_add(1) {
            prefix.push(next);
            return Some(prefix);
        }
    }
    None
}
