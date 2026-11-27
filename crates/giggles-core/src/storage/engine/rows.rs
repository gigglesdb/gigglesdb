//! Typed rows over transaction-owned MVCC reads and writes.
//!
//! Keys contain the table ID and the primary-key tuple in schema order. Row
//! checksums bind to that user key; timestamp suffixes belong to MVCC alone.
use super::{
    catalog::TableEntry,
    codec,
    kv::{Direction, Keyspace},
    seq::SequenceAllocator,
    txn::{Transaction, TxnScan},
};
use crate::{
    CoreError,
    relational::{self, DataType, SqlError, Value},
};
use std::{ops::Bound, sync::Arc, time::Instant};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Row {
    pub(crate) values: Vec<Value>,
    pub(crate) version: u64,
}

/// Share the engine's allocator across all row stores and transactions.
/// A heap descriptor must expose exactly one BigInt primary-key column; callers
/// supply Null in that column on insert so the store can assign its row ID.
pub(crate) struct RowStore {
    seq: Arc<SequenceAllocator>,
}

impl RowStore {
    pub(crate) fn new(seq: Arc<SequenceAllocator>) -> Self {
        Self { seq }
    }

    pub(crate) fn get(
        &self,
        txn: &mut Transaction,
        entry: &TableEntry,
        pk: &[Value],
    ) -> Result<Option<Row>, CoreError> {
        let key = user_key(entry, pk)?;
        txn.get(Keyspace::Rows, &key)?
            .map(|value| decode(&key, &value, entry.def.columns.len()))
            .transpose()
    }

    pub(crate) fn insert(
        &self,
        txn: &mut Transaction,
        entry: &TableEntry,
        mut values: Vec<Value>,
        deadline: Option<Instant>,
        cancel: &dyn Fn() -> bool,
    ) -> Result<Row, CoreError> {
        validate_descriptor(entry)?;
        if values.len() != entry.def.columns.len() {
            return Err(CoreError::Sql(SqlError::Type));
        }
        if entry.heap {
            let index = entry.def.primary_key[0];
            if values[index] != Value::Null {
                return Err(CoreError::Sql(SqlError::Type));
            }
            let id = self.seq.next(1, entry.def.id)?;
            values[index] =
                Value::Int(i64::try_from(id).map_err(|_| CoreError::Sql(SqlError::Overflow))?);
        }
        relational::row_check(&entry.def, &values).map_err(CoreError::Sql)?;
        let pk: Vec<_> = entry
            .def
            .primary_key
            .iter()
            .map(|&index| values[index].clone())
            .collect();
        let key = user_key(entry, &pk)?;
        let target = txn.lock_for_write(Keyspace::Rows, &key, deadline, cancel)?;
        if target.current.is_some() {
            return Err(CoreError::Sql(SqlError::Duplicate));
        }
        let version = self.seq.next(3, 0)?;
        let encoded = codec::encode_row(&key, version, &values)?;
        super::indexes::maintain(
            txn,
            &entry.def,
            None,
            Some(&Row {
                values: values.clone(),
                version,
            }),
            deadline,
            cancel,
        )?;
        txn.put(Keyspace::Rows, key, encoded)?;
        Ok(Row { values, version })
    }

    // Keep the frozen row-store API's explicit transaction and write controls.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn update(
        &self,
        txn: &mut Transaction,
        entry: &TableEntry,
        pk: &[Value],
        sets: &[(usize, Value)],
        expected_version: Option<u64>,
        deadline: Option<Instant>,
        cancel: &dyn Fn() -> bool,
    ) -> Result<Option<Row>, CoreError> {
        let key = user_key(entry, pk)?;
        let target = txn.lock_for_write(Keyspace::Rows, &key, deadline, cancel)?;
        let Some(encoded) = target.current else {
            return Ok(None);
        };
        let current = decode(&key, &encoded, entry.def.columns.len())?;
        check_version(&current, expected_version)?;
        let mut values = current.values.clone();
        let mut seen = std::collections::BTreeSet::new();
        for (index, value) in sets {
            if !seen.insert(*index) {
                return Err(CoreError::Sql(SqlError::Duplicate));
            }
            let column = entry
                .def
                .columns
                .get(*index)
                .ok_or(CoreError::Sql(SqlError::UnknownColumn))?;
            if entry.def.primary_key.contains(index) {
                return Err(CoreError::Sql(SqlError::Unsupported));
            }
            relational::check(value, column).map_err(CoreError::Sql)?;
            values[*index] = value.clone();
        }
        if values == current.values {
            return Ok(Some(current));
        }
        relational::row_check(&entry.def, &values).map_err(CoreError::Sql)?;
        let version = self.seq.next(3, 0)?;
        let encoded = codec::encode_row(&key, version, &values)?;
        super::indexes::maintain(
            txn,
            &entry.def,
            Some(&current),
            Some(&Row {
                values: values.clone(),
                version,
            }),
            deadline,
            cancel,
        )?;
        txn.put(Keyspace::Rows, key, encoded)?;
        Ok(Some(Row { values, version }))
    }

    pub(crate) fn delete(
        &self,
        txn: &mut Transaction,
        entry: &TableEntry,
        pk: &[Value],
        expected_version: Option<u64>,
        deadline: Option<Instant>,
        cancel: &dyn Fn() -> bool,
    ) -> Result<Option<Row>, CoreError> {
        let key = user_key(entry, pk)?;
        let target = txn.lock_for_write(Keyspace::Rows, &key, deadline, cancel)?;
        let Some(encoded) = target.current else {
            return Ok(None);
        };
        let current = decode(&key, &encoded, entry.def.columns.len())?;
        check_version(&current, expected_version)?;
        // MVCC owns deletion envelopes. A codec-level tombstone is never stored.
        super::indexes::maintain(txn, &entry.def, Some(&current), None, deadline, cancel)?;
        txn.delete(Keyspace::Rows, key)?;
        Ok(Some(current))
    }

    /// Nonempty bounds contain complete encoded primary-key tuples, without the
    /// table ID. Empty bounds sort before every valid tuple. The scan borrows
    /// only the transaction and decodes one owned row per next() call.
    pub(crate) fn scan<'a>(
        &self,
        txn: &'a mut Transaction,
        entry: &TableEntry,
        lo: Bound<Vec<u8>>,
        hi: Bound<Vec<u8>>,
        direction: Direction,
        limit: Option<usize>,
    ) -> Result<RowScan<'a>, CoreError> {
        validate_descriptor(entry)?;
        let prefix = entry.def.id.to_be_bytes();
        let lo = match lo {
            Bound::Unbounded => Bound::Included(prefix.to_vec()),
            Bound::Included(pk) | Bound::Excluded(pk) if pk.is_empty() => {
                Bound::Included(prefix.to_vec())
            }
            bound => bound.map(|pk| prefixed(&prefix, &pk)),
        };
        let hi = match hi {
            Bound::Unbounded => entry.def.id.checked_add(1).map_or(Bound::Unbounded, |id| {
                Bound::Excluded(id.to_be_bytes().to_vec())
            }),
            Bound::Included(pk) | Bound::Excluded(pk) if pk.is_empty() => {
                Bound::Excluded(prefix.to_vec())
            }
            bound => bound.map(|pk| prefixed(&prefix, &pk)),
        };
        let inner = txn.scan(
            Keyspace::Rows,
            lo.as_ref().map(Vec::as_slice),
            hi.as_ref().map(Vec::as_slice),
            direction,
            limit,
        )?;
        Ok(RowScan {
            inner,
            prefix,
            columns: entry.def.columns.len(),
            done: false,
        })
    }
}

pub(crate) struct RowScan<'a> {
    inner: TxnScan<'a>,
    prefix: [u8; 8],
    columns: usize,
    done: bool,
}

impl Iterator for RowScan<'_> {
    type Item = Result<(Vec<u8>, Row), CoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let (key, value) = match self.inner.next()? {
            Ok(pair) => pair,
            Err(error) => {
                self.done = true;
                return Some(Err(error.into()));
            }
        };
        // Also enforce the prefix explicitly when table u64::MAX has no finite
        // exclusive successor, and reject any accidental adjacent-table entry.
        if !key.starts_with(&self.prefix) {
            self.done = true;
            return None;
        }
        let row = decode(&key, &value, self.columns);
        if row.is_err() {
            self.done = true;
        }
        Some(row.map(|row| (key[8..].to_vec(), row)))
    }
}

fn prefixed(prefix: &[u8; 8], pk: &[u8]) -> Vec<u8> {
    let mut key = prefix.to_vec();
    key.extend_from_slice(pk);
    key
}

fn validate_descriptor(entry: &TableEntry) -> Result<(), CoreError> {
    let def = &entry.def;
    if def.primary_key.is_empty()
        || def.primary_key.iter().enumerate().any(|(position, index)| {
            *index >= def.columns.len() || def.primary_key[..position].contains(index)
        })
    {
        return Err(CoreError::Corrupt("row descriptor primary key".into()));
    }
    if entry.heap
        && (def.primary_key.len() != 1
            || def.columns[def.primary_key[0]].data_type != DataType::BigInt)
    {
        return Err(CoreError::Corrupt("heap row descriptor primary key".into()));
    }
    Ok(())
}

fn user_key(entry: &TableEntry, pk: &[Value]) -> Result<Vec<u8>, CoreError> {
    validate_descriptor(entry)?;
    if pk.len() != entry.def.primary_key.len() {
        return Err(CoreError::Sql(SqlError::Unbounded));
    }
    for (value, &index) in pk.iter().zip(&entry.def.primary_key) {
        // Primary-key nullability is enforced even for a malformed descriptor
        // with a nullable column, before the codec can report NullInKey.
        if *value == Value::Null {
            return Err(CoreError::Sql(SqlError::Null));
        }
        relational::check(value, &entry.def.columns[index]).map_err(CoreError::Sql)?;
    }
    Ok(prefixed(&entry.def.id.to_be_bytes(), &codec::typed_pk_bytes(&entry.def, pk)?))
}

pub(super) fn decode(key: &[u8], encoded: &[u8], columns: usize) -> Result<Row, CoreError> {
    let record = codec::decode_row(key, encoded)?;
    if record.tombstone {
        return Err(CoreError::Corrupt("row record contains a tombstone".into()));
    }
    if record.values.len() > columns {
        return Err(CoreError::Corrupt(
            "row has more columns than its schema".into(),
        ));
    }
    let mut values = record.values;
    values.resize(columns, Value::Null);
    Ok(Row {
        values,
        version: record.row_version,
    })
}

fn check_version(row: &Row, expected: Option<u64>) -> Result<(), CoreError> {
    if expected.is_some_and(|version| version != row.version) {
        return Err(CoreError::Sql(SqlError::Version));
    }
    Ok(())
}

#[cfg(test)]
#[path = "rows_tests.rs"]
mod tests;
