//! MVCC secondary entries. Only new index records require feature bit 2.
use super::{
    codec,
    kv::{Direction, Keyspace},
    txn::Transaction,
};
use crate::{
    CoreError,
    relational::{self, SqlError, SqlRow, TableDef, Value, secondary_index::Descriptor},
};
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
    bytes.extend(crate::storage::encode(&DescriptorDto {
        id: d.id,
        table: d.table,
        name: d.name.clone(),
        columns: d.columns.clone(),
        key_encoding: d.key_encoding,
        unique: d.unique,
    })?);
    Ok(bytes)
}
pub(crate) fn decode_descriptor(bytes: &[u8]) -> Result<Descriptor, CoreError> {
    if bytes.first() != Some(&1) {
        return Err(CoreError::Corrupt("index descriptor version".into()));
    }
    let d: DescriptorDto = crate::storage::decode(&bytes[1..])?;
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
    })
}
pub(crate) fn validate_descriptor(d: &Descriptor, table: &TableDef) -> Result<(), CoreError> {
    decode_descriptor(&encode_descriptor(d)?)?;
    if d.table != table.id
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
    table: &TableDef,
    rows: &BTreeMap<Vec<u8>, SqlRow>,
    ds: &[Descriptor],
) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, CoreError> {
    let mut entries = BTreeMap::new();
    for d in ds.iter().filter(|d| d.table == table.id) {
        validate_descriptor(d, table)?;
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
                return Err(CoreError::Sql(SqlError::Duplicate));
            }
        }
    }
    Ok(entries)
}
/// Lock in physical order, then check the latest committed owner after waiting.
/// Callers own the statement savepoint, so row and index failures roll back together.
#[allow(clippy::too_many_arguments)]
pub(crate) fn apply(
    txn: &mut Transaction,
    table: &TableDef,
    after_table: Option<&TableDef>,
    before: &BTreeMap<Vec<u8>, SqlRow>,
    after: &BTreeMap<Vec<u8>, SqlRow>,
    old_ds: &[Descriptor],
    new_ds: &[Descriptor],
    deadline: Option<Instant>,
    cancel: &dyn Fn() -> bool,
) -> Result<(), CoreError> {
    let old = entries(table, before, old_ds)?;
    let new = match after_table {
        Some(table) => entries(table, after, new_ds)?,
        None => BTreeMap::new(),
    };
    let changes = old
        .keys()
        .chain(new.keys())
        .filter(|key| old.get(*key) != new.get(*key))
        .map(|key| (key.clone(), new.get(key).cloned()))
        .collect::<BTreeMap<_, _>>();
    for key in changes.keys() {
        let target = txn.lock_for_write(Keyspace::Index, key, deadline, cancel)?;
        if target.current.is_some() && target.current.as_ref() != old.get(key) {
            return Err(CoreError::Sql(SqlError::Duplicate));
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
    validate_descriptor(descriptor, &table.def)?;
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
            None => return Ok(Vec::new()),
        },
    };
    let hi = match upper {
        Bound::Unbounded => successor(prefix.clone()).map_or(Bound::Unbounded, Bound::Excluded),
        Bound::Included(tuple) => {
            successor(prefix_key(tuple)).map_or(Bound::Unbounded, Bound::Excluded)
        }
        Bound::Excluded(tuple) => Bound::Excluded(prefix_key(tuple)),
    };
    let mut retention = relational::scan::Retention::new(relational::scan::retention_limit());
    let mut entries = Vec::new();
    for pair in txn.scan(
        Keyspace::Index,
        lo.as_ref().map(Vec::as_slice),
        hi.as_ref().map(Vec::as_slice),
        Direction::Forward,
        Some(relational::MAX_ROWS + 1),
    )? {
        let (key, pk) = pair?;
        retention
            .charge(key.len().saturating_add(pk.len()).saturating_add(64))
            .map_err(CoreError::Sql)?;
        entries.push((key, pk));
    }
    if entries.len() > relational::MAX_ROWS {
        return Err(CoreError::Limit);
    }
    let mut rows = Vec::with_capacity(entries.len());
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
        retention
            .charge(
                bytes.len().saturating_mul(2).saturating_add(
                    table
                        .def
                        .columns
                        .len()
                        .saturating_mul(std::mem::size_of::<Value>()),
                ),
            )
            .map_err(CoreError::Sql)?;
        let row = super::rows::decode(&row_key, &bytes, table.def.columns.len())?;
        let witness = BTreeMap::from([(
            Vec::new(),
            SqlRow {
                values: row.values.clone(),
                version: row.version,
            },
        )]);
        let expected = self::entries(&table.def, &witness, std::slice::from_ref(descriptor))?;
        if expected.get(&key) != Some(&pk) {
            return Err(CoreError::Corrupt("index entry disagrees with row".into()));
        }
        rows.push((pk, row));
    }
    Ok(rows)
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
