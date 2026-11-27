//! Immediate exact-ID UNIQUE lookup; no durable binding or mutation authority.
//! Limits describe conservative per-call work/copy reservations, not CPU or
//! allocator accounting. Corruption refuses with the existing SqlError::Type.
//! The explicit callback is trusted, bounded O(1) and non-reentrant; its own
//! work is outside this model. No ambient cancellation state is consulted.
use super::secondary_index::{self as si, Descriptor};
use super::secondary_index_adapter::{ValidatedSecondary, encode_tuple, tuple_len};
use super::*;

pub(crate) struct Limits {
    /// Conservative copy/temporary-byte reservation, at most 64 MiB.
    pub bytes: usize,
    /// At most 3.6 billion units. Every call still reserves two complete
    /// 1.2-billion-unit source validations, even when it reuses a proof.
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
    cap(t.column_slots(), MAX_TABLE_COLUMNS)?;
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
fn supported(c: &ColumnDef) -> R<()> {
    si::Kind::for_type(&c.data_type)
        .map(|_| ())
        .ok_or(SqlError::Unsupported)
}
type BaseRows = BTreeMap<Vec<u8>, SqlRow>;
type Winner<'a> = Option<(&'a [u8], &'a SqlRow)>;

struct SourceAdmission {
    bytes: usize,
    candidates: usize,
    max_row_bytes: usize,
}

fn probe_bytes(
    table: &TableDef,
    expected: &Descriptor,
    equal: &[Value],
    limits: &Limits,
) -> R<usize> {
    cap(limits.bytes, MAX_BYTES)?;
    cap(limits.work, MAX_WORK)?;
    cap(limits.candidates, si::MAX_CANDIDATES)?;
    // Entire borrowed witness is admitted before equality, membership,
    // canonical sizing (which scans bytes), allocation or user callbacks.
    cap(equal.len(), MAX_COLUMNS)?;
    let bytes = add(table_shape(table)?, descriptor_shape(expected)?)?;
    let mut eq_bytes = 0;
    for v in equal {
        eq_bytes = add(eq_bytes, raw(v))?;
        cap(eq_bytes, crate::relational::max_value_bytes())?;
    }
    add(bytes, eq_bytes)
}

impl SourceAdmission {
    fn new(source: &Relational) -> R<Self> {
        let mut bytes = 0;
        let ds = source.secondary.descriptors();
        for (name, t) in source.tables.iter() {
            cap(name.len(), 127)?;
            bytes = add(bytes, table_shape(t)?)?;
        }
        for d in ds {
            bytes = add(bytes, descriptor_shape(d)?)?;
        }
        // Admission remains unpolled and caps the source at si::MAX_ROWS
        // before walking its rows.
        let mut rows = 0;
        let mut max_row_bytes = 0;
        for map in source.rows.values() {
            rows = add(rows, map.len())?;
            cap(rows, si::MAX_ROWS)?;
            for (key, row) in map {
                cap(key.len(), si::MAX_COMPONENT)?;
                cap(row.values.len(), MAX_STORED_COLUMNS)?;
                let mut rb = 0;
                for v in &row.values {
                    rb = add(rb, raw(v))?;
                    cap(rb, crate::relational::max_value_bytes())?;
                }
                max_row_bytes = max_row_bytes.max(rb);
                bytes = add(bytes, add(key.len(), rb)?)?;
            }
        }
        Ok(Self {
            bytes,
            candidates: mul(rows, ds.len())?,
            max_row_bytes,
        })
    }

    fn reserve(&self, probe_bytes: usize, limits: &Limits) -> R<()> {
        cap(self.max_row_bytes, crate::relational::max_value_bytes())?;
        let bytes = add(probe_bytes, self.bytes)?;
        // Reserve all source certification work and temporary vectors before
        // entering the existing bridge. Extra source copy allowance includes
        // canonical tuple duplication, PK suffixes and substrate lookup output.
        let candidates = self.candidates;
        cap(candidates, limits.candidates)?;
        let bytes = add(mul(bytes, 64)?, mul(candidates, 32)?)?;
        cap(bytes, limits.bytes)?;
        let work = add(si::MAX_WORK * 2, mul(bytes, 16)?)?;
        cap(work, limits.work)
    }
}

fn check_probe(
    table: &TableDef,
    expected: &Descriptor,
    equal: &[Value],
    ds: &[Descriptor],
) -> R<u64> {
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
        supported(c)?;
        check(v, c)?;
    }
    if table.primary_key.is_empty() {
        return Err(SqlError::Type);
    }
    for p in &table.primary_key {
        let c = table.columns.get(*p).ok_or(SqlError::Type)?;
        if c.data_type.is_temporal() {
            return Err(SqlError::Unsupported);
        }
        supported(c)?;
    }
    Ok(d.id)
}

fn probe_tuple(equal: &[Value], cancel: &mut impl FnMut() -> bool) -> R<Option<Vec<u8>>> {
    if equal.iter().any(|v| matches!(v, Value::Null)) {
        tick(cancel)?;
        return Ok(None);
    }
    let n = tuple_len(equal.iter())?;
    cap(n, si::MAX_COMPONENT)?;
    let tuple = encode_tuple(equal.iter())?;
    tick(cancel)?;
    Ok(Some(tuple))
}

fn lookup_error(error: si::Error) -> SqlError {
    match error {
        si::Error::Limit => SqlError::Limit,
        si::Error::Cancelled => SqlError::Aborted,
        _ => SqlError::Type,
    }
}

fn accept_entry<'a>(
    base: &'a BaseRows,
    tuple: &[u8],
    key: &[u8],
    pk: &[u8],
    winner: &mut Winner<'a>,
) -> R<()> {
    if key.get(tuple.len()..) != Some(pk) || winner.is_some() {
        return Err(SqlError::Type);
    }
    let (pk, row) = base.get_key_value(pk).ok_or(SqlError::Type)?;
    *winner = Some((pk.as_slice(), row));
    Ok(())
}

/// Statement-local admission and table access for one validated source.
/// Both are acquired at the first probe that needs them, independently.
pub(crate) struct UniqueLookup<'s, 't> {
    validated: ValidatedSecondary<'s>,
    table: &'t TableDef,
    epoch: u64,
    admission: Option<SourceAdmission>,
    base: Option<&'s BaseRows>,
    #[cfg(test)]
    reference: bool,
}

impl<'s, 't> UniqueLookup<'s, 't> {
    pub(crate) fn new(validated: ValidatedSecondary<'s>, table: &'t TableDef, epoch: u64) -> Self {
        Self {
            validated,
            table,
            epoch,
            admission: None,
            base: None,
            #[cfg(test)]
            reference: false,
        }
    }

    #[cfg(test)]
    pub(crate) fn use_reference(&mut self, reference: bool) {
        self.reference = reference;
    }

    pub(crate) fn rows(&mut self, mut cancel: impl FnMut() -> bool) -> R<&'s BaseRows> {
        #[cfg(test)]
        if self.reference {
            return self
                .validated
                .source()
                .secondary_access_engine(self.table, self.epoch, cancel)
                .map(|(_, base)| base);
        }
        self.access_rows(&mut cancel).map(|(base, _)| base)
    }

    fn access_rows(&mut self, cancel: &mut impl FnMut() -> bool) -> R<(&'s BaseRows, bool)> {
        let source = self.validated.source();
        if self.base.is_some() {
            tick(cancel)?;
        } else {
            source.secondary_access_fence(self.table, self.epoch, cancel)?;
        }
        // The access fence polls even on a warm lookup. Recheck the effective
        // budgets after that callback, where the legacy access would validate.
        let reuse_proof = self.validated.matches_context();
        if !reuse_proof {
            source.secondary_validate_engine(cancel)?;
        }
        let base = self
            .base
            .or_else(|| source.rows.get(&self.table.id))
            .ok_or(SqlError::UnknownTable)?;
        self.base = Some(base);
        Ok((base, reuse_proof))
    }

    pub(crate) fn lookup(
        &mut self,
        expected: &Descriptor,
        equal: &[Value],
        limits: &Limits,
        mut cancel: impl FnMut() -> bool,
    ) -> R<Winner<'s>> {
        #[cfg(test)]
        if self.reference {
            return self.validated.source().secondary_unique_lookup_engine(
                self.table, self.epoch, expected, equal, limits, cancel,
            );
        }
        let bytes = probe_bytes(self.table, expected, equal, limits)?;
        let admission = match &self.admission {
            Some(admission) => admission,
            None => self
                .admission
                .insert(SourceAdmission::new(self.validated.source())?),
        };
        admission.reserve(bytes, limits)?;
        let sidecar = self.validated.sidecar();
        let index = check_probe(self.table, expected, equal, sidecar.descriptors())?;
        tick(&mut cancel)?;
        let (base, reuse_proof) = self.access_rows(&mut cancel)?;
        if !reuse_proof {
            return lookup_uncertified(sidecar, base, index, equal, &mut cancel);
        }
        let Some(tuple) = probe_tuple(equal, &mut cancel)? else {
            return Ok(None);
        };
        let entries = self
            .validated
            .lookup_prefix(index, &tuple)
            .map_err(lookup_error)?;
        let mut winner = None;
        for (key, pk) in entries {
            tick(&mut cancel)?;
            accept_entry(base, &tuple, key, pk, &mut winner)?;
        }
        tick(&mut cancel)?;
        Ok(winner)
    }
}

impl Relational {
    /// Immediate exact-ID lookup with full per-call validation. Returns the
    /// authoritative map's borrowed PK and row/version.
    #[cfg(test)]
    pub(crate) fn secondary_unique_lookup_engine(
        &self,
        table: &TableDef,
        epoch: u64,
        expected: &Descriptor,
        equal: &[Value],
        limits: &Limits,
        mut cancel: impl FnMut() -> bool,
    ) -> R<Winner<'_>> {
        let bytes = probe_bytes(table, expected, equal, limits)?;
        SourceAdmission::new(self)?.reserve(bytes, limits)?;
        let index = check_probe(table, expected, equal, self.secondary.descriptors())?;
        tick(&mut cancel)?;
        let (sidecar, base) = self.secondary_access_engine(table, epoch, &mut cancel)?;
        lookup_uncertified(sidecar, base, index, equal, &mut cancel)
    }
}

fn lookup_uncertified<'a>(
    sidecar: &si::Sidecar,
    base: &'a BaseRows,
    index: u64,
    equal: &[Value],
    cancel: &mut impl FnMut() -> bool,
) -> R<Winner<'a>> {
    let Some(tuple) = probe_tuple(equal, cancel)? else {
        return Ok(None);
    };
    let entries = sidecar
        .lookup(index, None, None, &mut *cancel)
        .map_err(lookup_error)?;
    let mut winner = None;
    for (key, pk) in entries {
        tick(cancel)?;
        if key.starts_with(&tuple) {
            accept_entry(base, &tuple, key, pk, &mut winner)?;
        }
    }
    tick(cancel)?;
    Ok(winner)
}

#[cfg(test)]
#[path = "secondary_unique_lookup_tests.rs"]
mod tests;
