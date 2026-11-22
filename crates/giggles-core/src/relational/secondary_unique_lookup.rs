//! Immediate exact-ID UNIQUE lookup; no durable binding or mutation authority.
//! Limits describe conservative per-call work/copy reservations, not CPU or
//! allocator accounting. Corruption refuses with the existing SqlError::Type.
//! The explicit callback is trusted, bounded O(1) and non-reentrant; its own
//! work is outside this model. No ambient cancellation state is consulted.
use super::secondary_index::{self as si, Descriptor};
use super::secondary_index_adapter::{encode_tuple, tuple_len};
use super::*;

pub(crate) struct Limits {
    /// Conservative copy/temporary-byte reservation, at most 64 MiB.
    pub bytes: usize,
    /// Includes two complete 1.2-billion-unit source-validation reservations;
    /// at most 3.6 billion units. This is deliberately conservative composition.
    pub work: usize,
    /// Conservative all-source rows * descriptor-count reservation, <=4096.
    pub candidates: usize,
}
const MAX_BYTES: usize = 64 * 1024 * 1024;
const MAX_WORK: usize = si::MAX_WORK * 3;
fn add(a: usize, b: usize) -> R<usize> {
    a.checked_add(b).ok_or(SqlError::Limit)
}
fn mul(a: usize, b: usize) -> R<usize> {
    a.checked_mul(b).ok_or(SqlError::Limit)
}
fn cap(a: usize, b: usize) -> R<()> {
    if a > b { Err(SqlError::Limit) } else { Ok(()) }
}
fn tick(cancel: &mut impl FnMut() -> bool) -> R<()> {
    if cancel() {
        Err(SqlError::Aborted)
    } else {
        Ok(())
    }
}
fn raw(v: &Value) -> usize {
    match v {
        Value::Text(s) => s.len(),
        Value::Bytes(b) => b.len(),
        Value::Numeric(_) => giggles_pg_types::MAX_TEXT,
        _ => 16,
    }
}
fn table_shape(t: &TableDef) -> R<usize> {
    cap(t.name.len(), 63)?;
    cap(t.columns.len(), MAX_COLUMNS)?;
    cap(t.primary_key.len(), MAX_COLUMNS)?;
    let mut n = add(t.name.len(), mul(t.columns.len(), 128)?)?;
    for c in &t.columns {
        cap(c.name.len(), 63)?;
        n = add(n, c.name.len())?;
    }
    Ok(n)
}
fn descriptor_shape(d: &Descriptor) -> R<usize> {
    cap(d.name.len(), si::MAX_NAME)?;
    cap(d.columns.len(), si::MAX_ARITY)?;
    add(d.name.len(), mul(d.columns.len(), 8)?)
}
fn legacy(c: &ColumnDef) -> R<()> {
    match c.data_type {
        DataType::Boolean
        | DataType::SmallInt
        | DataType::Integer
        | DataType::BigInt
        | DataType::Text
        | DataType::Varchar
        | DataType::Uuid
        | DataType::Bytes => Ok(()),
        _ => Err(SqlError::Unsupported),
    }
}
impl Relational {
    /// `expected` pins table/id/ordered columns/encoding/unique (name is not
    /// identity). Returns the authoritative map's borrowed PK and row/version.
    /// NULL skips only after full source, metadata and type validation.
    pub(crate) fn secondary_unique_lookup_engine<'a>(
        &'a self,
        table: &TableDef,
        epoch: u64,
        expected: &Descriptor,
        equal: &[Value],
        limits: &Limits,
        mut cancel: impl FnMut() -> bool,
    ) -> R<Option<(&'a [u8], &'a SqlRow)>> {
        cap(limits.bytes, MAX_BYTES)?;
        cap(limits.work, MAX_WORK)?;
        cap(limits.candidates, si::MAX_CANDIDATES)?;
        // Entire borrowed witness is admitted before equality, membership,
        // canonical sizing (which scans bytes), allocation or user callbacks.
        cap(equal.len(), MAX_COLUMNS)?;
        let mut bytes = add(table_shape(table)?, descriptor_shape(expected)?)?;
        let mut eq_bytes = 0;
        for v in equal {
            eq_bytes = add(eq_bytes, raw(v))?;
            cap(eq_bytes, crate::relational::max_value_bytes())?;
        }
        bytes = add(bytes, eq_bytes)?;
        let ds = self.secondary.descriptors();
        for (name, t) in self.tables.iter() {
            cap(name.len(), 127)?;
            bytes = add(bytes, table_shape(t)?)?;
        }
        for d in ds {
            bytes = add(bytes, descriptor_shape(d)?)?;
        }
        let mut rows = 0;
        for map in self.rows.values() {
            rows = add(rows, map.len())?;
            cap(rows, si::MAX_ROWS)?;
            for (key, row) in map {
                cap(key.len(), si::MAX_COMPONENT)?;
                cap(row.values.len(), MAX_COLUMNS)?;
                let mut rb = 0;
                for v in &row.values {
                    rb = add(rb, raw(v))?;
                    cap(rb, crate::relational::max_value_bytes())?;
                }
                bytes = add(bytes, add(key.len(), rb)?)?;
            }
        }
        // Reserve all source certification work and temporary vectors before
        // entering the existing bridge. Extra source copy allowance includes
        // canonical tuple duplication, PK suffixes and substrate lookup output.
        let candidates = mul(rows, ds.len())?;
        cap(candidates, limits.candidates)?;
        bytes = add(mul(bytes, 64)?, mul(candidates, 32)?)?;
        cap(bytes, limits.bytes)?;
        let work = add(si::MAX_WORK * 2, mul(bytes, 16)?)?;
        cap(work, limits.work)?;
        if expected.table != table.id
            || !expected.unique
            || expected.columns.is_empty()
            || equal.len() != expected.columns.len()
        {
            return Err(SqlError::Unsupported);
        }
        // No variable-length comparison until all witness reservations above.
        let d = ds
            .iter()
            .find(|d| d.id == expected.id)
            .ok_or(SqlError::Unsupported)?;
        if d.table != expected.table
            || d.columns != expected.columns
            || d.key_encoding != expected.key_encoding
            || !d.unique
        {
            return Err(SqlError::Unsupported);
        }
        for (id, v) in expected.columns.iter().zip(equal) {
            let c = table
                .columns
                .iter()
                .find(|c| c.id == *id)
                .ok_or(SqlError::UnknownColumn)?;
            legacy(c)?;
            check(v, c)?;
        }
        if table.primary_key.is_empty() {
            return Err(SqlError::Type);
        }
        for p in &table.primary_key {
            legacy(table.columns.get(*p).ok_or(SqlError::Type)?)?;
        }
        tick(&mut cancel)?;
        let (sidecar, base) = self.secondary_access_engine(table, epoch, &mut cancel)?;
        if equal.iter().any(|v| matches!(v, Value::Null)) {
            tick(&mut cancel)?;
            return Ok(None);
        }
        let n = tuple_len(equal.iter())?;
        cap(n, si::MAX_COMPONENT)?;
        let tuple = encode_tuple(equal.iter())?;
        tick(&mut cancel)?;
        // Full bounded selected map, no fabricated tuple-prefix upper endpoint.
        let entries = sidecar
            .lookup(d.id, None, None, &mut cancel)
            .map_err(|e| match e {
                si::Error::Limit => SqlError::Limit,
                si::Error::Cancelled => SqlError::Aborted,
                _ => SqlError::Type,
            })?;
        let mut winner = None;
        for (key, pk) in entries {
            tick(&mut cancel)?;
            if !key.starts_with(&tuple) {
                continue;
            }
            if key.get(tuple.len()..) != Some(pk) || winner.is_some() {
                return Err(SqlError::Type);
            }
            let (pk, row) = base.get_key_value(pk).ok_or(SqlError::Type)?;
            winner = Some((pk.as_slice(), row));
        }
        tick(&mut cancel)?;
        Ok(winner)
    }
}

#[cfg(test)]
#[path = "secondary_unique_lookup_tests.rs"]
mod tests;
