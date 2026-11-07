//! UNINTEGRATED GD42 sidecar substrate. No runtime module declaration, storage integration,
//! format/envelope allocation, SQL/native access, or CREATE capability is added.
//!
//! Engine-only proof boundary: the caller supplies a complete (never SELECT-page)
//! snapshot of ALL base rows for ALL referenced tables, including empty tables via
//! Catalog. Each row supplies exactly one tuple per index on its table. The caller
//! proves stable ID ownership, snapshot consistency, canonical encode_key(values)
//! tuples in descriptor order, type/range/nullability checks (NULL sorts first),
//! binary text/bytes order, and a complete non-null immutable encode_key(PK) suffix.
//! Individual tuple and PK arities satisfy the engine encoder separately; their
//! sum need not be <=64. Bytes are NOT self-authenticating storage credentials:
//! this module checks exact recomputation relative to these trusted inputs, not
//! whether arbitrary bytes encode valid values. Never expose these inputs to SQL,
//! untrusted deserialization, storage opening, or a public authority boundary.
//!
//! Catalog is a borrowed resolver into the authoritative table catalog, not a
//! second catalog. Kind admission is explicit and independent of Value variants.
//! All mutation calls stage locally and publish only after final cancellation.
//! The caller must atomically publish base rows and sidecar in its transaction,
//! perform generation/epoch conflict checks, and gate storage/replicas separately.
//! Serde is a deterministic model representation ONLY, no durable format promise.
//! Use codec::decode_validated for bounded UNPUBLISHED candidate payload v1.
//! Its revision is independent of the storage envelope; namespace8 reservation
//! does not enable indexed outer formats7..10. No storage opening is added.
//! Complete snapshot/stable references/scalar/null proofs remain caller duties.
#[path = "secondary_index/codec.rs"]
pub(crate) mod codec;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub(crate) const MAX_INDEXES_PER_TABLE: usize = 8;
pub(crate) const MAX_INDEXES: usize = 128;
pub(crate) const MAX_NAME: usize = 128;
pub(crate) const MAX_ARITY: usize = 64;
pub(crate) const MAX_COMPONENT: usize = 131_264;
pub(crate) const MAX_KEY: usize = 262_528;
pub(crate) const MAX_ENTRY: usize = 393_792;
pub(crate) const MAX_ROWS: usize = 1024;
pub(crate) const MAX_CANDIDATES: usize = 4096;
pub(crate) const MAX_BYTES: usize = 16 * 1024 * 1024;
/// Conservative worst-case work units, not elapsed time. Preflight counts one
/// unit per scalar check plus worst-case byte comparisons/copies. 64 comparison
/// passes per entry byte covers BTree construction/equality and lookup traversal;
/// quadratic descriptor/row identity scans charge full compared byte bounds.
/// Each CRUD call admits TWO such budgets including cross-snapshot work;
/// validation admits one. The 64 byte passes include stored-state preflight,
/// staging, exact equality, and BTree comparisons; scalar work includes the
/// preflight itself. Allocator internals and caller Catalog/closure cost are
/// outside the model (caller must keep both callbacks bounded/non-reentrant).
pub(crate) const MAX_WORK: usize = 1_200_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Error {
    Limit,
    Descriptor,
    Rows,
    Corrupt,
    ImmutablePk,
    Duplicate,
    Cancelled,
}
type Result<T> = std::result::Result<T, Error>;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Boolean,
    SmallInt,
    Integer,
    BigInt,
    Text,
    Bytes,
}
impl Kind {
    /// Exact engine type names only: Numeric/Real/DoublePrecision and future
    /// unknown types reject, even if Value gains additional variants.
    pub(crate) fn from_engine_name(name: &str) -> Result<Self> {
        match name {
            "Boolean" => Ok(Self::Boolean),
            "SmallInt" => Ok(Self::SmallInt),
            "Integer" => Ok(Self::Integer),
            "BigInt" => Ok(Self::BigInt),
            "Text" => Ok(Self::Text),
            "Bytes" => Ok(Self::Bytes),
            _ => Err(Error::Descriptor),
        }
    }
}
pub(crate) trait Catalog {
    fn table_exists(&self, table: u64) -> bool;
    /// Return None for unknown/unsupported column kinds. Nullable supported kinds
    /// are allowed; caller checks each row against authoritative nullability.
    fn column_kind(&self, table: u64, column: u64) -> Option<Kind>;
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Descriptor {
    pub id: u64,
    pub table: u64,
    /// Flat, globally unique exact UTF-8 name; schema resolution is not provided.
    pub name: String,
    pub columns: Vec<u64>,
    /// Existing engine encode_key v1, NOT a sidecar/envelope version.
    pub key_encoding: u32,
    pub unique: bool,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Tuple<'a> {
    pub index: u64,
    pub bytes: &'a [u8],
    /// Trusted typed projection: any NULL component makes this tuple distinct.
    pub has_null: bool,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BaseRow<'a> {
    pub table: u64,
    pub pk: &'a [u8],
    pub tuples: &'a [Tuple<'a>],
}
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[cfg_attr(test, derive(Serialize, Deserialize))]
pub(crate) struct Sidecar {
    descriptors: Vec<Descriptor>,
    /// Index ID -> (tuple || complete PK -> complete PK payload).
    entries: BTreeMap<u64, BTreeMap<Vec<u8>, Vec<u8>>>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Counts {
    pub rows: usize,
    pub candidates: usize,
    pub bytes: usize,
    pub work: usize,
}
fn add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b).ok_or(Error::Limit)
}
fn mul(a: usize, b: usize) -> Result<usize> {
    a.checked_mul(b).ok_or(Error::Limit)
}
fn cap(n: usize, maximum: usize) -> Result<()> {
    if n > maximum {
        Err(Error::Limit)
    } else {
        Ok(())
    }
}
fn tick(cancel: &mut impl FnMut() -> bool) -> Result<()> {
    if cancel() {
        Err(Error::Cancelled)
    } else {
        Ok(())
    }
}
fn component(bytes: &[u8]) -> Result<()> {
    if bytes.is_empty() {
        return Err(Error::Rows);
    }
    cap(bytes.len(), MAX_COMPONENT)
}
fn entry_size(tuple: usize, pk: usize) -> Result<usize> {
    cap(tuple, MAX_COMPONENT)?;
    cap(pk, MAX_COMPONENT)?;
    let key = add(tuple, pk)?;
    cap(key, MAX_KEY)?;
    let size = add(key, pk)?;
    cap(size, MAX_ENTRY)?;
    Ok(size)
}
/// No owned byte/descriptor copies occur during preflight. Resolver must be pure,
/// bounded O(1) per call and reference the same immutable catalog snapshot.
fn preflight(
    ds: &[Descriptor],
    rows: &[BaseRow<'_>],
    catalog: &impl Catalog,
    cancel: &mut impl FnMut() -> bool,
) -> Result<Counts> {
    cap(ds.len(), MAX_INDEXES)?;
    cap(rows.len(), MAX_ROWS)?;
    // R1: admit every borrowed descriptor/row/tuple shape before comparisons.
    for d in ds {
        cap(d.name.len(), MAX_NAME)?;
        cap(d.columns.len(), MAX_ARITY)?;
    }
    for row in rows {
        witness_shape(row)?;
    }
    let mut work = mul(mul(ds.len(), ds.len())?, MAX_NAME + MAX_ARITY)?;
    // Charge actual possible compared PK bytes for every row identity pair.
    for (i, d) in ds.iter().enumerate() {
        tick(cancel)?;
        cap(d.name.len(), MAX_NAME)?;
        cap(d.columns.len(), MAX_ARITY)?;
        if d.id == 0
            || d.table == 0
            || d.name.is_empty()
            || d.columns.is_empty()
            || d.key_encoding != 1
            || !catalog.table_exists(d.table)
        {
            return Err(Error::Descriptor);
        }
        for prior in &ds[..i] {
            if prior.id == d.id || prior.name == d.name {
                return Err(Error::Descriptor);
            }
        }
        cap(
            ds.iter().filter(|x| x.table == d.table).count(),
            MAX_INDEXES_PER_TABLE,
        )?;
        for (j, col) in d.columns.iter().enumerate() {
            if *col == 0
                || d.columns[..j].contains(col)
                || catalog.column_kind(d.table, *col).is_none()
            {
                return Err(Error::Descriptor);
            }
        }
        work = add(work, mul(d.columns.len(), MAX_ARITY)?)?;
    }
    let mut candidates = 0usize;
    let mut bytes = 0usize;
    for (i, row) in rows.iter().enumerate() {
        tick(cancel)?;
        component(row.pk)?;
        if row.table == 0 || !catalog.table_exists(row.table) {
            return Err(Error::Rows);
        }
        cap(row.tuples.len(), MAX_INDEXES_PER_TABLE)?;
        for prior in &rows[..i] {
            work = add(work, add(1, row.pk.len().min(prior.pk.len()))?)?;
            cap(work, MAX_WORK)?;
            if row.table == prior.table && row.pk == prior.pk {
                return Err(Error::Rows);
            }
        }
        let count = ds.iter().filter(|d| d.table == row.table).count();
        if row.tuples.len() != count {
            return Err(Error::Rows);
        }
        for (j, tuple) in row.tuples.iter().enumerate() {
            tick(cancel)?;
            if row.tuples[..j].iter().any(|x| x.index == tuple.index)
                || !ds
                    .iter()
                    .any(|d| d.id == tuple.index && d.table == row.table)
            {
                return Err(Error::Rows);
            }
            component(tuple.bytes)?;
            candidates = add(candidates, 1)?;
            cap(candidates, MAX_CANDIDATES)?;
            bytes = add(bytes, entry_size(tuple.bytes.len(), row.pk.len())?)?;
            cap(bytes, MAX_BYTES)?;
        }
    }
    work = add(work, mul(bytes, 64)?)?;
    work = add(work, mul(rows.len(), MAX_INDEXES * MAX_INDEXES_PER_TABLE)?)?;
    cap(work, MAX_WORK)?;
    Ok(Counts {
        rows: rows.len(),
        candidates,
        bytes,
        work,
    })
}
// Admit every witness's shape before cancellation or byte comparisons. This
// first pass only reads lengths; an oversized later witness cannot hide behind
// an earlier mismatch/cancellation. Descriptor mapping is checked by preflight.
fn witness_shape(row: &BaseRow<'_>) -> Result<()> {
    cap(row.pk.len(), MAX_COMPONENT)?;
    cap(row.tuples.len(), MAX_INDEXES_PER_TABLE)?;
    for tuple in row.tuples {
        cap(tuple.bytes.len(), MAX_COMPONENT)?;
    }
    Ok(())
}
fn row_equal(a: &BaseRow<'_>, b: &BaseRow<'_>, cancel: &mut impl FnMut() -> bool) -> Result<bool> {
    tick(cancel)?;
    if a.table != b.table || a.pk != b.pk || a.tuples.len() != b.tuples.len() {
        return Ok(false);
    }
    for x in a.tuples {
        let mut found = false;
        for y in b.tuples {
            tick(cancel)?;
            if x == y {
                found = true;
                break;
            }
        }
        if !found {
            return Ok(false);
        }
    }
    Ok(true)
}
fn contains_row(
    rows: &[BaseRow<'_>],
    target: &BaseRow<'_>,
    exact: bool,
    cancel: &mut impl FnMut() -> bool,
) -> Result<bool> {
    for row in rows {
        tick(cancel)?;
        if if exact {
            row_equal(row, target, cancel)?
        } else {
            row.table == target.table && row.pk == target.pk
        } {
            return Ok(true);
        }
    }
    Ok(false)
}
fn witness_work(row: &BaseRow<'_>, searches: usize) -> Result<usize> {
    // All membership/identity searches plus Update PK equality. Each exact
    // search may scan 8x8 tuple pairs; charge 32 passes over both witnesses'
    // byte sums (each tuple appears in at most 8 comparisons per search).
    let mut bytes = add(1, row.pk.len())?;
    for tuple in row.tuples {
        bytes = add(bytes, add(1, tuple.bytes.len())?)?;
    }
    mul(mul(bytes, 32)?, add(searches, 1)?)
}
impl Sidecar {
    pub(crate) fn descriptors(&self) -> &[Descriptor] {
        &self.descriptors
    }
    /// Fully builds in temporary memory; self is unchanged on every failure.
    pub(crate) fn build(
        &mut self,
        ds: &[Descriptor],
        rows: &[BaseRow<'_>],
        catalog: &impl Catalog,
        mut cancel: impl FnMut() -> bool,
    ) -> Result<Counts> {
        let counts = preflight(ds, rows, catalog, &mut cancel)?;
        let staged = Self::stage(ds, rows, &mut cancel)?;
        tick(&mut cancel)?;
        *self = staged;
        Ok(counts)
    }
    fn stage(
        ds: &[Descriptor],
        rows: &[BaseRow<'_>],
        cancel: &mut impl FnMut() -> bool,
    ) -> Result<Self> {
        let mut ordered: Vec<_> = ds.iter().collect();
        ordered.sort_by_key(|d| d.id);
        let mut staged = Self {
            descriptors: Vec::with_capacity(ds.len()),
            entries: BTreeMap::new(),
        };
        for d in ordered {
            tick(cancel)?;
            staged.descriptors.push(d.clone());
            staged.entries.insert(d.id, BTreeMap::new());
        }
        let mut owners: BTreeMap<u64, BTreeMap<&[u8], &[u8]>> = BTreeMap::new();
        for row in rows {
            for tuple in row.tuples {
                tick(cancel)?;
                if ds.iter().any(|d| d.id == tuple.index && d.unique) && !tuple.has_null {
                    let owner = owners.entry(tuple.index).or_default();
                    if let Some(prior) = owner.insert(tuple.bytes, row.pk)
                        && prior != row.pk
                    {
                        return Err(Error::Duplicate);
                    }
                }
                let mut key = Vec::with_capacity(add(tuple.bytes.len(), row.pk.len())?);
                key.extend_from_slice(tuple.bytes);
                key.extend_from_slice(row.pk);
                let map = staged.entries.get_mut(&tuple.index).ok_or(Error::Rows)?;
                if map.insert(key, row.pk.to_vec()).is_some() {
                    return Err(Error::Rows);
                }
            }
        }
        Ok(staged)
    }
    /// Preflight malformed decoded state BEFORE recomputation/copy. Unknown maps,
    /// oversized/extra entries, wrong payloads, missing/dangling/wrong-index keys
    /// all reject. External decoder must separately bound allocation/input bytes.
    fn stored_preflight(&self) -> Result<()> {
        cap(self.descriptors.len(), MAX_INDEXES)?;
        cap(self.entries.len(), MAX_INDEXES)?;
        let mut count = 0;
        let mut bytes = 0;
        for map in self.entries.values() {
            count = add(count, map.len())?;
            cap(count, MAX_CANDIDATES)?;
            for (key, pk) in map {
                cap(key.len(), MAX_KEY)?;
                component(pk)?;
                let size = add(key.len(), pk.len())?;
                cap(size, MAX_ENTRY)?;
                bytes = add(bytes, size)?;
                cap(bytes, MAX_BYTES)?;
            }
        }
        Ok(())
    }
    pub(crate) fn validate(
        &self,
        rows: &[BaseRow<'_>],
        catalog: &impl Catalog,
        mut cancel: impl FnMut() -> bool,
    ) -> Result<Counts> {
        self.stored_preflight()?;
        let counts = preflight(&self.descriptors, rows, catalog, &mut cancel)?;
        let expected = Self::stage(&self.descriptors, rows, &mut cancel)?;
        tick(&mut cancel)?;
        if *self != expected {
            return Err(Error::Corrupt);
        }
        Ok(counts)
    }
    /// Atomic CRUD against complete before/after snapshots. Exactly the stated
    /// row may change; PK updates reject. The caller commits matching base rows.
    pub(crate) fn crud(
        &mut self,
        before: &[BaseRow<'_>],
        after: &[BaseRow<'_>],
        change: Change<'_>,
        catalog: &impl Catalog,
        cancel: impl FnMut() -> bool,
    ) -> Result<Counts> {
        self.crud_with_work_limit(before, after, change, catalog, cancel, mul(MAX_WORK, 2)?)
    }
    fn crud_with_work_limit(
        &mut self,
        before: &[BaseRow<'_>],
        after: &[BaseRow<'_>],
        change: Change<'_>,
        catalog: &impl Catalog,
        mut cancel: impl FnMut() -> bool,
        work_limit: usize,
    ) -> Result<Counts> {
        let (old, new) = match change {
            Change::Insert(new) => (None, Some(new)),
            Change::Delete(old) => (Some(old), None),
            Change::Update { old, new } => (Some(old), Some(new)),
        };
        for row in old.iter().chain(new.iter()) {
            witness_shape(row)?;
        }
        tick(&mut cancel)?;
        self.stored_preflight()?;
        let mut admitted_work = 0;
        for row in old.iter().chain(new.iter()) {
            tick(&mut cancel)?;
            let admission = preflight(
                &self.descriptors,
                std::slice::from_ref(row),
                catalog,
                &mut cancel,
            )?;
            admitted_work = add(admitted_work, admission.work)?;
            admitted_work = add(
                admitted_work,
                witness_work(row, add(before.len(), after.len())?)?,
            )?;
            cap(admitted_work, work_limit)?;
        }
        // Both snapshots are checked before ANY descriptor/key/payload copy.
        let prior = preflight(&self.descriptors, before, catalog, &mut cancel)?;
        let counts = preflight(&self.descriptors, after, catalog, &mut cancel)?;
        // Cross-snapshot identity searches occur twice, plus membership tests.
        // Full tuple equality is charged at 16 passes per staged input byte.
        let mut cross_work = add(admitted_work, add(prior.work, counts.work)?)?;
        cap(cross_work, work_limit)?;
        for a in before {
            tick(&mut cancel)?;
            for b in after {
                cross_work = add(cross_work, mul(add(1, a.pk.len().min(b.pk.len()))?, 4)?)?;
                cap(cross_work, work_limit)?;
            }
        }
        cross_work = add(cross_work, mul(add(prior.bytes, counts.bytes)?, 16)?)?;
        cap(cross_work, work_limit)?;
        tick(&mut cancel)?;
        if let (Some(old), Some(new)) = (old, new)
            && (old.table != new.table || old.pk != new.pk)
        {
            return Err(Error::ImmutablePk);
        }
        // Row tuple order and complete input row order remain immaterial.
        for (rows, others, witness) in [(before, after, old), (after, before, new)] {
            for row in rows {
                tick(&mut cancel)?;
                if let Some(w) = witness.filter(|w| row.table == w.table && row.pk == w.pk) {
                    if !row_equal(row, &w, &mut cancel)? {
                        return Err(Error::Rows);
                    }
                } else if !contains_row(others, row, true, &mut cancel)? {
                    return Err(Error::Rows);
                }
            }
        }
        if let Some(w) = old
            && (!contains_row(before, &w, true, &mut cancel)?
                || (new.is_none() && contains_row(after, &w, false, &mut cancel)?))
        {
            return Err(Error::Rows);
        }
        if let Some(w) = new
            && (!contains_row(after, &w, true, &mut cancel)?
                || (old.is_none() && contains_row(before, &w, false, &mut cancel)?))
        {
            return Err(Error::Rows);
        }
        let expected = Self::stage(&self.descriptors, before, &mut cancel)?;
        if *self != expected {
            return Err(Error::Corrupt);
        }
        drop(expected); // Only one staged entry set may occupy the 16 MiB quota.
        let staged = Self::stage(&self.descriptors, after, &mut cancel)?;
        tick(&mut cancel)?;
        *self = staged;
        Ok(Counts {
            work: cross_work,
            ..counts
        })
    }
    /// Ordered borrowed key/payload lookup. Bounds are FULL combined encoded keys
    /// (not tuple-prefix endpoints); caller proves legal bounds. No copies. This
    /// is a substrate route only, not a SQL/native scan or an explain capability.
    pub(crate) fn lookup<'a>(
        &'a self,
        index: u64,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        mut cancel: impl FnMut() -> bool,
    ) -> Result<Vec<(&'a [u8], &'a [u8])>> {
        if let Some(b) = lower {
            cap(b.len(), MAX_KEY)?;
        }
        if let Some(b) = upper {
            cap(b.len(), MAX_KEY)?;
        }
        if lower.zip(upper).is_some_and(|(a, b)| a > b) {
            return Err(Error::Rows);
        }
        let map = self.entries.get(&index).ok_or(Error::Descriptor)?;
        // [lower, upper); limits are already invariant, but enforce on decoded data.
        self.stored_preflight()?;
        let mut out = Vec::new();
        use std::ops::Bound::{Excluded, Included, Unbounded};
        let bounds = (
            lower.map_or(Unbounded, Included),
            upper.map_or(Unbounded, Excluded),
        );
        for (key, pk) in map.range::<[u8], _>(bounds) {
            tick(&mut cancel)?;
            out.push((key.as_slice(), pk.as_slice()));
        }
        tick(&mut cancel)?;
        Ok(out)
    }
}
#[derive(Debug, Clone, Copy)]
pub(crate) enum Change<'a> {
    Insert(BaseRow<'a>),
    Delete(BaseRow<'a>),
    Update { old: BaseRow<'a>, new: BaseRow<'a> },
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Cat;
    impl Catalog for Cat {
        fn table_exists(&self, id: u64) -> bool {
            (1..=128).contains(&id)
        }
        fn column_kind(&self, table: u64, col: u64) -> Option<Kind> {
            (self.table_exists(table) && (1..=64).contains(&col)).then_some(Kind::Text)
        }
    }
    fn d(id: u64, table: u64) -> Descriptor {
        Descriptor {
            id,
            table,
            name: format!("i{id}"),
            columns: vec![1],
            key_encoding: 1,
            unique: false,
        }
    }
    fn r<'a>(pk: &'a [u8], tuples: &'a [Tuple<'a>]) -> BaseRow<'a> {
        BaseRow {
            table: 1,
            pk,
            tuples,
        }
    }
    fn build(ds: &[Descriptor], rows: &[BaseRow<'_>]) -> Sidecar {
        let mut s = Sidecar::default();
        s.build(ds, rows, &Cat, || false).unwrap();
        s
    }
    #[test]
    fn oversized_witnesses_reject_before_comparison_or_cancel() {
        let huge = vec![9; MAX_COMPONENT + 1];
        let mut different = huge.clone();
        different[0] = 8;
        let a = r(&huge, &[]);
        for b in [r(&huge.clone(), &[]), r(&different, &[])] {
            let mut sidecar = build(&[d(1, 1)], &[]);
            let saved = sidecar.clone();
            assert_eq!(
                sidecar.crud(
                    &[],
                    &[],
                    Change::Update { old: a, new: b },
                    &Cat,
                    || panic!("oversized witness must reject first")
                ),
                Err(Error::Limit)
            );
            assert_eq!(sidecar, saved);
        }
        let too_many = [Tuple {
            index: 1,
            bytes: b"x",
            has_null: false,
        }; MAX_INDEXES_PER_TABLE + 1];
        let large_tuple = [Tuple {
            index: 1,
            bytes: &huge,
            has_null: false,
        }];
        for row in [a, r(b"p", &too_many), r(b"p", &large_tuple)] {
            for change in [Change::Insert(row), Change::Delete(row)] {
                let mut sidecar = build(&[d(1, 1)], &[]);
                let saved = sidecar.clone();
                assert_eq!(
                    sidecar.crud(&[], &[], change, &Cat, || true),
                    Err(Error::Limit)
                );
                assert_eq!(sidecar, saved);
            }
        }
    }
    #[test]
    fn admitted_witness_mapping_and_cancellation_are_atomic() {
        let mut sidecar = build(&[d(1, 1)], &[]);
        let saved = sidecar.clone();
        let unknown = [Tuple {
            index: 2,
            bytes: b"x",
            has_null: false,
        }];
        for row in [
            r(b"p", &unknown),
            r(b"p", &[]),
            BaseRow {
                table: 129,
                pk: b"p",
                tuples: &[],
            },
        ] {
            for change in [Change::Insert(row), Change::Delete(row)] {
                assert_eq!(
                    sidecar.crud(&[], &[], change, &Cat, || false),
                    Err(Error::Rows)
                );
                assert_eq!(sidecar, saved);
            }
        }
        // Empty snapshots used to provide zero cancellation checkpoints.
        let empty = r(b"p", &[]);
        for change in [
            Change::Insert(empty),
            Change::Delete(empty),
            Change::Update {
                old: empty,
                new: empty,
            },
        ] {
            assert_eq!(
                sidecar.crud(&[], &[], change, &Cat, || true),
                Err(Error::Cancelled)
            );
            assert_eq!(sidecar, saved);
        }
    }
    #[test]
    fn witness_work_exhausts_combined_budget_without_publication() {
        // No indexes: snapshot bytes are zero, but PK membership work is real.
        // Each snapshot's pair work fits; combined CRUD work exceeds its cap.
        let pks: Vec<_> = (0..110u32)
            .map(|i| {
                let mut pk = vec![9; MAX_COMPONENT];
                pk[..4].copy_from_slice(&i.to_be_bytes());
                pk
            })
            .collect();
        let rows: Vec<_> = pks.iter().map(|pk| r(pk, &[])).collect();
        let mut sidecar = build(&[], &rows);
        let saved = sidecar.clone();
        assert_eq!(
            sidecar.crud(
                &rows,
                &rows,
                Change::Update {
                    old: rows[0],
                    new: rows[0]
                },
                &Cat,
                || false
            ),
            Err(Error::Limit)
        );
        assert_eq!(sidecar, saved);
        let row = r(b"p", &[]);
        let mut sidecar = build(&[], &[row]);
        let saved = sidecar.clone();
        assert_eq!(
            sidecar.crud_with_work_limit(
                &[row],
                &[row],
                Change::Update { old: row, new: row },
                &Cat,
                || false,
                3000
            ),
            Err(Error::Limit)
        );
        assert_eq!(sidecar, saved);
        assert!(
            sidecar
                .crud_with_work_limit(
                    &[row],
                    &[row],
                    Change::Update { old: row, new: row },
                    &Cat,
                    || false,
                    8192
                )
                .is_ok()
        );
    }
    #[test]
    fn explicit_kind_admission() {
        for k in ["Boolean", "SmallInt", "Integer", "BigInt", "Text", "Bytes"] {
            assert!(Kind::from_engine_name(k).is_ok());
        }
        for k in [
            "Numeric",
            "Real",
            "DoublePrecision",
            "Float4",
            "future",
            "text",
            "",
        ] {
            assert_eq!(Kind::from_engine_name(k), Err(Error::Descriptor));
        }
    }
    #[test]
    fn descriptor_identity_name_ownership_and_kind() {
        let a = d(1, 1);
        let mut bad = d(2, 1);
        bad.name = a.name.clone();
        assert_eq!(
            Sidecar::default().build(&[a.clone(), bad], &[], &Cat, || false),
            Err(Error::Descriptor)
        );
        assert_eq!(
            Sidecar::default().build(&[a.clone(), a.clone()], &[], &Cat, || false),
            Err(Error::Descriptor)
        );
        for bad in [
            Descriptor {
                table: 129,
                ..a.clone()
            },
            Descriptor { id: 0, ..a.clone() },
            Descriptor {
                columns: vec![1, 1],
                ..a.clone()
            },
            Descriptor {
                columns: vec![65],
                ..a.clone()
            },
            Descriptor {
                key_encoding: 2,
                ..a.clone()
            },
            Descriptor {
                columns: vec![],
                ..a.clone()
            },
        ] {
            assert_eq!(
                Sidecar::default().build(&[bad], &[], &Cat, || false),
                Err(Error::Descriptor)
            );
        }
        struct Unsupported;
        impl Catalog for Unsupported {
            fn table_exists(&self, _: u64) -> bool {
                true
            }
            fn column_kind(&self, _: u64, _: u64) -> Option<Kind> {
                None
            }
        }
        assert_eq!(
            Sidecar::default().build(&[a], &[], &Unsupported, || false),
            Err(Error::Descriptor)
        );
    }
    #[test]
    fn descriptor_bounds_exact_and_over() {
        let mut a = d(1, 1);
        a.name = "é".repeat(64);
        a.columns = (1..=64).collect();
        build(&[a.clone()], &[]);
        a.name.push('a');
        assert_eq!(
            Sidecar::default().build(&[a.clone()], &[], &Cat, || false),
            Err(Error::Limit)
        );
        a.name.pop();
        a.columns.push(65);
        assert_eq!(
            Sidecar::default().build(&[a], &[], &Cat, || false),
            Err(Error::Limit)
        );
        let ds: Vec<_> = (1..=128).map(|i| d(i, (i - 1) / 8 + 1)).collect();
        build(&ds, &[]);
        let mut over = ds.clone();
        over.push(d(129, 17));
        assert_eq!(
            Sidecar::default().build(&over, &[], &Cat, || false),
            Err(Error::Limit)
        );
        build(&(1..=8).map(|i| d(i, 1)).collect::<Vec<_>>(), &[]);
        assert_eq!(
            Sidecar::default().build(
                &(1..=9).map(|i| d(i, 1)).collect::<Vec<_>>(),
                &[],
                &Cat,
                || false
            ),
            Err(Error::Limit)
        );
    }
    #[test]
    fn component_combined_entry_arithmetic_bounds() {
        assert_eq!(entry_size(MAX_COMPONENT, MAX_COMPONENT), Ok(MAX_ENTRY));
        assert_eq!(MAX_COMPONENT * 2, MAX_KEY);
        assert_eq!(entry_size(MAX_COMPONENT + 1, 1), Err(Error::Limit));
        assert_eq!(entry_size(1, MAX_COMPONENT + 1), Err(Error::Limit));
        assert_eq!(entry_size(usize::MAX, 1), Err(Error::Limit));
        assert_eq!(add(usize::MAX, 1), Err(Error::Limit));
        assert_eq!(mul(usize::MAX, 2), Err(Error::Limit));
        let pk = vec![2; MAX_COMPONENT];
        let key = vec![3; MAX_COMPONENT];
        build(
            &[d(1, 1)],
            &[r(
                &pk,
                &[Tuple {
                    index: 1,
                    bytes: &key,
                    has_null: false,
                }],
            )],
        );
    }
    #[test]
    fn row_candidate_and_total_bytes_bounds() {
        let pks: Vec<_> = (0..1025u32).map(|i| i.to_be_bytes()).collect();
        let ts: Vec<_> = (1..=4)
            .map(|index| Tuple {
                index,
                bytes: &b"x"[..],
                has_null: false,
            })
            .collect();
        let ds: Vec<_> = (1..=4).map(|i| d(i, 1)).collect();
        let rows: Vec<_> = pks.iter().map(|pk| r(pk, &ts)).collect();
        assert_eq!(
            preflight(&ds, &rows[..1024], &Cat, &mut || false)
                .unwrap()
                .candidates,
            4096
        );
        assert_eq!(
            preflight(&ds, &rows, &Cat, &mut || false),
            Err(Error::Limit)
        );
        let five: Vec<_> = (1..=5)
            .map(|index| Tuple {
                index,
                bytes: &b"x"[..],
                has_null: false,
            })
            .collect();
        let five_ds: Vec<_> = (1..=5).map(|i| d(i, 1)).collect();
        let more: Vec<_> = pks[..820].iter().map(|pk| r(pk, &five)).collect();
        assert_eq!(
            preflight(&five_ds, &more, &Cat, &mut || false),
            Err(Error::Limit)
        );
        let four_rows: Vec<_> = pks[..1023].iter().map(|pk| r(pk, &ts)).collect();
        // Give the final table five indexes; earlier table needs only four.
        let mut mixed_ds = ds.clone();
        mixed_ds.extend((5..=9).map(|id| d(id, 2)));
        let last_tuples: Vec<_> = (5..=9)
            .map(|index| Tuple {
                index,
                bytes: b"x",
                has_null: false,
            })
            .collect();
        let mut exact_over = four_rows;
        exact_over.push(BaseRow {
            table: 2,
            pk: &pks[1023],
            tuples: &last_tuples,
        });
        assert_eq!(
            preflight(&mixed_ds, &exact_over, &Cat, &mut || false),
            Err(Error::Limit)
        );
        // Raw bytes here test resource accounting only, NOT legal engine encodings.
        let mut buffers = vec![vec![7; MAX_COMPONENT]; 127];
        let last = MAX_BYTES - 127 * (MAX_COMPONENT + 8) - 8;
        buffers.push(vec![7; last]);
        let tuples: Vec<_> = buffers
            .iter()
            .map(|b| {
                [Tuple {
                    index: 1,
                    bytes: b,
                    has_null: false,
                }]
            })
            .collect();
        let rows: Vec<_> = tuples
            .iter()
            .enumerate()
            .map(|(i, t)| r(&pks[i], t))
            .collect();
        assert_eq!(
            preflight(&[d(1, 1)], &rows, &Cat, &mut || false)
                .unwrap()
                .bytes,
            MAX_BYTES
        );
        drop(rows);
        drop(tuples);
        buffers.last_mut().unwrap().push(7);
        let tuples: Vec<_> = buffers
            .iter()
            .map(|b| {
                [Tuple {
                    index: 1,
                    bytes: b,
                    has_null: false,
                }]
            })
            .collect();
        let rows: Vec<_> = tuples
            .iter()
            .enumerate()
            .map(|(i, t)| r(&pks[i], t))
            .collect();
        assert_eq!(
            preflight(&[d(1, 1)], &rows, &Cat, &mut || false),
            Err(Error::Limit)
        );
    }
    #[test]
    fn exact_set_corruption_and_incomplete_input() {
        let ts = [Tuple {
            index: 1,
            bytes: b"\x03a\0\0",
            has_null: false,
        }];
        let rows = [r(b"\x01\x01", &ts)];
        let s = build(&[d(1, 1)], &rows);
        s.validate(&rows, &Cat, || false).unwrap();
        let mut variants = Vec::new();
        let mut x = s.clone();
        x.entries.clear();
        variants.push(x);
        let mut x = s.clone();
        x.entries.get_mut(&1).unwrap().clear();
        variants.push(x);
        let mut x = s.clone();
        x.entries
            .get_mut(&1)
            .unwrap()
            .insert(b"wrong".to_vec(), b"dangling".to_vec());
        variants.push(x);
        let mut x = s.clone();
        *x.entries.get_mut(&1).unwrap().values_mut().next().unwrap() = b"wrong-pk".to_vec();
        variants.push(x);
        let mut x = s.clone();
        let map = x.entries.remove(&1).unwrap();
        x.entries.insert(2, map);
        variants.push(x);
        let mut x = s.clone();
        x.entries.insert(2, BTreeMap::new());
        variants.push(x);
        for x in variants {
            assert_eq!(x.validate(&rows, &Cat, || false), Err(Error::Corrupt));
        }
        assert_eq!(s.validate(&[], &Cat, || false), Err(Error::Corrupt));
        assert_eq!(
            Sidecar::default().build(&[d(1, 1)], &[rows[0], rows[0]], &Cat, || false),
            Err(Error::Rows)
        );
        assert_eq!(
            Sidecar::default().build(&[d(1, 1)], &[r(b"p", &[])], &Cat, || false),
            Err(Error::Rows)
        );
        assert_eq!(
            Sidecar::default().build(
                &[d(1, 1)],
                &[r(
                    b"p",
                    &[Tuple {
                        index: 2,
                        bytes: b"t",
                        has_null: false
                    }]
                )],
                &Cat,
                || false
            ),
            Err(Error::Rows)
        );
    }
    #[test]
    fn crud_order_ties_null_and_binary_proof_fixtures() {
        // Canonical existing encode_key: NULL=00, bool=true=0101;
        // byte-string [0] = 04 00ff 0000. Complete PKs are bool encodings.
        let a = [Tuple {
            index: 1,
            bytes: b"\0",
            has_null: false,
        }];
        let b = [Tuple {
            index: 1,
            bytes: b"\0",
            has_null: false,
        }];
        let c = [Tuple {
            index: 1,
            bytes: b"\x04\0\xff\0\0",
            has_null: false,
        }];
        let old = r(b"\x01\0", &a);
        let next = r(b"\x01\x01", &b);
        let mut s = build(&[d(1, 1)], &[old]);
        s.crud(&[old], &[next, old], Change::Insert(next), &Cat, || false)
            .unwrap();
        let found = s.lookup(1, None, None, || false).unwrap();
        assert_eq!(
            found.iter().map(|x| x.1).collect::<Vec<_>>(),
            vec![old.pk, next.pk]
        );
        assert_eq!(
            s.lookup(1, Some(found[1].0), None, || false).unwrap().len(),
            1
        );
        assert_eq!(
            s.lookup(1, None, Some(found[1].0), || false).unwrap().len(),
            1
        );
        let updated = r(old.pk, &c);
        s.crud(
            &[old, next],
            &[updated, next],
            Change::Update { old, new: updated },
            &Cat,
            || false,
        )
        .unwrap();
        assert_eq!(s.lookup(1, None, None, || false).unwrap()[0].1, next.pk);
        s.crud(
            &[updated, next],
            &[updated],
            Change::Delete(next),
            &Cat,
            || false,
        )
        .unwrap();
        s.validate(&[updated], &Cat, || false).unwrap();
        let saved = s.clone();
        assert_eq!(
            s.crud(
                &[updated],
                &[next],
                Change::Update {
                    old: updated,
                    new: next
                },
                &Cat,
                || false
            ),
            Err(Error::ImmutablePk)
        );
        assert_eq!(s, saved);
        assert_eq!(
            s.crud(&[updated], &[], Change::Insert(next), &Cat, || false),
            Err(Error::Rows)
        );
        assert_eq!(s, saved);
    }
    #[test]
    fn deterministic_serialization_independent_of_input_order() {
        let a = [
            Tuple {
                index: 2,
                bytes: b"a",
                has_null: false,
            },
            Tuple {
                index: 1,
                bytes: b"b",
                has_null: false,
            },
        ];
        let b = [a[1], a[0]];
        let s = build(&[d(2, 1), d(1, 1)], &[r(b"p", &a), r(b"q", &b)]);
        let t = build(&[d(1, 1), d(2, 1)], &[r(b"q", &a), r(b"p", &b)]);
        assert_eq!(
            bincode::serialize(&s).unwrap(),
            bincode::serialize(&t).unwrap()
        );
        let decoded: Sidecar = bincode::deserialize(&bincode::serialize(&s).unwrap()).unwrap();
        decoded
            .validate(&[r(b"p", &a), r(b"q", &b)], &Cat, || false)
            .unwrap();
    }
    #[test]
    fn explicit_work_limit_and_crud_corruption_are_atomic() {
        // PK comparisons alone exhaust work before copying any staged bytes.
        let pks: Vec<_> = (0..200u32)
            .map(|i| {
                let mut p = vec![9; MAX_COMPONENT];
                p[..4].copy_from_slice(&i.to_be_bytes());
                p
            })
            .collect();
        let rows: Vec<_> = pks.iter().map(|p| r(p, &[])).collect();
        assert_eq!(
            preflight(&[], &rows, &Cat, &mut || false),
            Err(Error::Limit)
        );
        let t = [Tuple {
            index: 1,
            bytes: b"t",
            has_null: false,
        }];
        let row = r(b"p", &t);
        let mut s = build(&[d(1, 1)], &[row]);
        s.entries.get_mut(&1).unwrap().clear();
        let saved = s.clone();
        assert_eq!(
            s.crud(&[row], &[], Change::Delete(row), &Cat, || false),
            Err(Error::Corrupt)
        );
        assert_eq!(s, saved);
        let duplicate = [
            Tuple {
                index: 1,
                bytes: b"a",
                has_null: false,
            },
            Tuple {
                index: 1,
                bytes: b"b",
                has_null: false,
            },
        ];
        assert_eq!(
            Sidecar::default().build(&[d(1, 1), d(2, 1)], &[r(b"p", &duplicate)], &Cat, || false),
            Err(Error::Rows)
        );
        assert_eq!(
            s.lookup(1, Some(b"z"), Some(b"a"), || false),
            Err(Error::Rows)
        );
    }
    #[test]
    fn cancellation_at_every_checkpoint_preserves_prior_state() {
        let a = [Tuple {
            index: 1,
            bytes: b"a",
            has_null: false,
        }];
        let b = [Tuple {
            index: 1,
            bytes: b"b",
            has_null: false,
        }];
        let before = [r(b"p", &a)];
        let after = [r(b"p", &b)];
        let seed = build(&[d(1, 1)], &before);
        let mut calls = 0;
        seed.clone()
            .build(&[d(1, 1)], &after, &Cat, || {
                calls += 1;
                false
            })
            .unwrap();
        for stop in 1..=calls {
            let mut s = seed.clone();
            let mut n = 0;
            assert_eq!(
                s.build(&[d(1, 1)], &after, &Cat, || {
                    n += 1;
                    n == stop
                }),
                Err(Error::Cancelled)
            );
            assert_eq!(s, seed);
        }
        calls = 0;
        seed.clone()
            .crud(
                &before,
                &after,
                Change::Update {
                    old: before[0],
                    new: after[0],
                },
                &Cat,
                || {
                    calls += 1;
                    false
                },
            )
            .unwrap();
        for stop in 1..=calls {
            let mut s = seed.clone();
            let mut n = 0;
            assert_eq!(
                s.crud(
                    &before,
                    &after,
                    Change::Update {
                        old: before[0],
                        new: after[0]
                    },
                    &Cat,
                    || {
                        n += 1;
                        n == stop
                    }
                ),
                Err(Error::Cancelled)
            );
            assert_eq!(s, seed);
        }
        let mut s = seed.clone();
        let huge = vec![1; MAX_COMPONENT + 1];
        assert_eq!(
            s.build(&[d(1, 1)], &[r(&huge, &a)], &Cat, || false),
            Err(Error::Limit)
        );
        assert_eq!(s, seed);
        assert_eq!(s.lookup(1, None, None, || true), Err(Error::Cancelled));
    }
}
