//! Finite, engine-only prerequisite; no UNIQUE publication or write enforcement.
//! Source provenance is the engine's validated Relational construction/load path,
//! not arbitrary serde authentication. Recheck selected metadata/rows locally;
//! never call validate(), whose sidecar path consults ambient cancellation.
//! Budgets model conservative algorithmic byte/copy/work counts, not allocator
//! internals or CPU time. The explicit callback must be bounded O(1), trusted,
//! non-reentrant. Its own work is outside this model. No ambient scope is changed.
use super::secondary_index::{self as si, Descriptor, Kind};
use super::*;

#[derive(Clone, Copy, Debug)]
pub(crate) struct Budget {
    pub rows: usize,
    pub candidates: usize,
    pub bytes: usize,
    pub work: usize,
}
pub(super) const HARD: Budget = Budget {
    rows: MAX_ROWS,
    candidates: MAX_ROWS,
    bytes: si::MAX_BYTES,
    work: si::MAX_WORK,
};

/// No rebinding, consumer, serialization, Clone or ready flag. Private typed
/// borrows pin the exact descriptor and original complete snapshot. Equal epoch
/// values never authenticate another source. Safe mutation requires ending this
/// proof's borrow. Publication must design its own reviewed lifetime protocol.
pub(crate) struct UniqueReadiness<'a> {
    _state: &'a Relational,
    _table: &'a TableDef,
    _rows: &'a BTreeMap<Vec<u8>, SqlRow>,
    _descriptor: &'a Descriptor,
    _epoch: u64,
    _generation: u64,
}

impl UniqueReadiness<'_> {
    /// Trusted consumer: matching numbers alone cannot rebind a borrowed proof.
    pub(super) fn consume_for(
        self,
        state: &Relational,
        descriptor: &Descriptor,
        epoch: u64,
        generation: u64,
    ) -> R<()> {
        if !std::ptr::eq(self._state, state)
            || !std::ptr::eq(self._descriptor, descriptor)
            || !state.tables.values().any(|t| std::ptr::eq(t, self._table))
            || !state
                .rows
                .get(&descriptor.table)
                .is_some_and(|r| std::ptr::eq(r, self._rows))
            || epoch != self._epoch
            || epoch != state.epoch
            || generation != self._generation
            || generation != state.generation
        {
            return Err(SqlError::StaleSchema);
        }
        Ok(())
    }
}

fn add(a: usize, b: usize) -> R<usize> {
    a.checked_add(b).ok_or(SqlError::Limit)
}
fn mul(a: usize, b: usize) -> R<usize> {
    a.checked_mul(b).ok_or(SqlError::Limit)
}
fn cap(n: usize, max: usize) -> R<()> {
    if n > max {
        Err(SqlError::Limit)
    } else {
        Ok(())
    }
}
#[derive(Default, Debug)]
struct Counts {
    rows: usize,
    candidates: usize,
    bytes: usize,
    work: usize,
}
struct Meter<F> {
    budget: Budget,
    counts: Counts,
    cancel: F,
}
impl<F: FnMut() -> bool> Meter<F> {
    fn work(&mut self, n: usize) -> R<()> {
        self.counts.work = add(self.counts.work, n)?;
        cap(self.counts.work, self.budget.work)
    }
    fn bytes(&mut self, n: usize) -> R<()> {
        self.counts.bytes = add(self.counts.bytes, n)?;
        cap(self.counts.bytes, self.budget.bytes)
    }
    fn checkpoint(&mut self) -> R<()> {
        self.work(1)?;
        if (self.cancel)() {
            Err(SqlError::Aborted)
        } else {
            Ok(())
        }
    }
}
fn legacy(ty: &DataType) -> R<Kind> {
    let name = match ty {
        DataType::Boolean => "Boolean",
        DataType::SmallInt => "SmallInt",
        DataType::Integer => "Integer",
        DataType::BigInt => "BigInt",
        DataType::Text => "Text",
        DataType::Varchar => "Text",
        DataType::Bpchar => return Err(SqlError::Unsupported),
        DataType::Bytes => "Bytes",
        _ => return Err(SqlError::Unsupported),
    };
    Kind::from_engine_name(name).map_err(|_| SqlError::Unsupported)
}
// Numeric's private representation is constructed by validated pg-types APIs:
// coefficient <= MAX_DIGITS, scale <= MAX_SCALE. Display invokes absolute-value
// cloning, base-10 conversion, bounded padding and insertion. MAX_TEXT^2 is a
// deliberately conservative quadratic work reserve for these bounded integer
// conversion operations, not a literal instruction/time guarantee. Four text
// buffers also reserve intermediate copies before row_check formats the value.
// No formatting, comparisons or owned scalar copies happen in this function.
fn raw(v: &Value) -> usize {
    match v {
        Value::Text(s) => s.len(),
        Value::Bytes(b) => b.len(),
        Value::Numeric(_) => giggles_pg_types::MAX_TEXT,
        Value::Float4(_) | Value::Date(_) => 4,
        Value::Float8(_) | Value::Time(_) | Value::Timestamp(_) | Value::TimestampTz(_) => 8,
        Value::TimeTz(_) => 12,
        Value::Interval(_) => 16,
        _ => 9,
    }
}
fn key_bound(v: &Value) -> R<usize> {
    match v {
        Value::Null => Ok(1),
        Value::Bool(_) => Ok(2),
        Value::Int(_) => Ok(9),
        Value::Text(s) => add(mul(s.len(), 2)?, 3),
        Value::Bytes(b) => add(mul(b.len(), 2)?, 3),
        _ => Err(SqlError::Unsupported),
    }
}
// Only length admission and fixed-ID selection. Checkpoints keep admission
// cancellable; each individual shape is admitted before its checkpoint, and
// the whole source is admitted before any variable comparisons/format/copies.
fn admit<F: FnMut() -> bool>(state: &Relational, d: &Descriptor, meter: &mut Meter<F>) -> R<()> {
    cap(d.name.len(), si::MAX_NAME)?;
    cap(d.columns.len(), MAX_COLUMNS)?;
    cap(state.tables.len(), MAX_TABLES)?;
    cap(state.rows.len(), MAX_TABLES)?;
    meter.bytes(add(d.name.len(), mul(d.columns.len(), 8)?)?)?;
    meter.work(add(1, d.columns.len())?)?;
    meter.checkpoint()?;
    for (name, t) in &state.tables {
        cap(name.len(), si::MAX_NAME)?;
        cap(t.name.len(), si::MAX_NAME)?;
        cap(t.columns.len(), MAX_COLUMNS)?;
        cap(t.primary_key.len(), MAX_COLUMNS)?;
        meter.bytes(add(
            add(name.len(), t.name.len())?,
            mul(t.columns.len(), 32)?,
        )?)?;
        meter.work(add(1, t.primary_key.len())?)?;
        meter.checkpoint()?;
        for c in &t.columns {
            cap(c.name.len(), si::MAX_NAME)?;
            meter.bytes(c.name.len())?;
            meter.work(1)?;
            meter.checkpoint()?;
        }
    }
    let rows = state.rows.get(&d.table).ok_or(SqlError::Type)?;
    cap(rows.len(), meter.budget.rows)?;
    cap(rows.len(), meter.budget.candidates)?;
    meter.counts.rows = rows.len();
    meter.counts.candidates = rows.len();
    for (key, row) in rows {
        cap(key.len(), si::MAX_COMPONENT)?;
        cap(row.values.len(), MAX_COLUMNS)?;
        meter.bytes(key.len())?;
        meter.work(1)?;
        meter.checkpoint()?;
        let mut size = 0;
        for v in &row.values {
            size = add(size, raw(v))?;
            cap(size, MAX_ROW_BYTES)?;
            meter.bytes(raw(v))?;
            meter.work(add(1, raw(v))?)?;
            if matches!(v, Value::Numeric(_)) {
                meter.bytes(mul(giggles_pg_types::MAX_TEXT, 4)?)?;
                meter.work(mul(giggles_pg_types::MAX_TEXT, giggles_pg_types::MAX_TEXT)?)?;
            }
            meter.checkpoint()?;
        }
    }
    Ok(())
}
fn metadata<'a, F: FnMut() -> bool>(
    state: &'a Relational,
    d: &Descriptor,
    meter: &mut Meter<F>,
) -> R<&'a TableDef> {
    if state.format != 1 {
        return Err(SqlError::Unsupported);
    }
    if state.rows.len() != state.tables.len() {
        return Err(SqlError::Type);
    }
    // All shapes admitted. Reserve bounded worst-case ID/name comparisons and
    // scans independently of row count; no empty-table bypass.
    let slots = mul(state.tables.len(), MAX_COLUMNS + 1)?;
    meter.work(mul(mul(slots, slots)?, 2)?)?;
    meter.work(mul(
        mul(state.tables.len(), MAX_COLUMNS * MAX_COLUMNS)?,
        si::MAX_NAME,
    )?)?;
    let mut ids = Vec::new();
    let mut selected = None;
    for (name, t) in &state.tables {
        meter.checkpoint()?;
        ident(name)?;
        if name != &t.name
            || t.id == 0
            || t.id >= state.next_id
            || ids.contains(&t.id)
            || t.columns.is_empty()
            || t.primary_key.is_empty()
            || !state.rows.contains_key(&t.id)
        {
            return Err(SqlError::Type);
        }
        ids.push(t.id);
        for (i, c) in t.columns.iter().enumerate() {
            meter.checkpoint()?;
            t.validate_column_ident(i)?;
            if c.id == 0
                || c.id >= state.next_id
                || ids.contains(&c.id)
                || t.columns[..i].iter().any(|p| p.name == c.name)
            {
                return Err(SqlError::Type);
            }
            ids.push(c.id);
        }
        for (j, p) in t.primary_key.iter().enumerate() {
            if *p >= t.columns.len() || !t.columns[*p].not_null {
                return Err(SqlError::Null);
            }
            if t.primary_key[..j].contains(p) {
                return Err(SqlError::Type);
            }
        }
        if t.id == d.table {
            selected = Some(t);
        }
    }
    let t = selected.ok_or(SqlError::UnknownTable)?;
    if d.id == 0
        || ids.contains(&d.id)
        || d.name.is_empty()
        || d.columns.is_empty()
        || d.key_encoding != 1
    {
        return Err(SqlError::Type);
    }
    ident(&d.name)?;
    for p in &t.primary_key {
        legacy(&t.columns[*p].data_type)?;
    }
    for (i, id) in d.columns.iter().enumerate() {
        meter.checkpoint()?;
        if *id == 0 || d.columns[..i].contains(id) {
            return Err(SqlError::Type);
        }
        let col = t
            .columns
            .iter()
            .find(|c| c.id == *id)
            .ok_or(SqlError::UnknownColumn)?;
        legacy(&col.data_type)?;
    }
    Ok(t)
}

impl Relational {
    /// Trusted complete-map validation only. No public consumer exists yet.
    pub(crate) fn unique_readiness_engine<'a>(
        &'a self,
        descriptor: &'a Descriptor,
        expected_epoch: u64,
        expected_generation: u64,
        budget: Budget,
        cancel: impl FnMut() -> bool,
    ) -> R<UniqueReadiness<'a>> {
        validate(
            self,
            descriptor,
            expected_epoch,
            expected_generation,
            budget,
            cancel,
        )
        .map(|(p, _)| p)
    }
}
fn validate<'a>(
    state: &'a Relational,
    d: &'a Descriptor,
    epoch: u64,
    generation: u64,
    budget: Budget,
    cancel: impl FnMut() -> bool,
) -> R<(UniqueReadiness<'a>, Counts)> {
    cap(budget.rows, HARD.rows)?;
    cap(budget.candidates, HARD.candidates)?;
    cap(budget.bytes, HARD.bytes)?;
    cap(budget.work, HARD.work)?;
    let mut m = Meter {
        budget,
        counts: Counts::default(),
        cancel,
    };
    admit(state, d, &mut m)?;
    if epoch != state.epoch || generation != state.generation {
        return Err(SqlError::StaleSchema);
    }
    let t = metadata(state, d, &mut m)?;
    let rows = state.rows.get(&t.id).ok_or(SqlError::Type)?;
    let positions: Vec<_> = d
        .columns
        .iter()
        .map(|id| {
            t.columns
                .iter()
                .position(|c| c.id == *id)
                .expect("validated stable column")
        })
        .collect();
    // Reserve all possible encoding/copy and pair-comparison work before any
    // scalar check can format Numeric, PK cloning, or variable comparisons.
    let mut pair_bytes = 0;
    for (key, row) in rows {
        m.checkpoint()?;
        if row.values.len() != t.columns.len() {
            return Err(SqlError::Type);
        }
        let mut p = 0;
        let mut u = 0;
        for i in &t.primary_key {
            p = add(p, key_bound(&row.values[*i])?)?;
        }
        for i in &positions {
            u = add(u, key_bound(&row.values[*i])?)?;
        }
        cap(p, si::MAX_COMPONENT)?;
        cap(u, si::MAX_COMPONENT)?;
        let encoded = add(p, u)?;
        m.bytes(mul(encoded, 4)?)?;
        m.work(mul(add(encoded, key.len())?, 64)?)?;
        pair_bytes = add(pair_bytes, add(encoded, 2)?)?;
        m.work(mul(row.values.len(), MAX_COLUMNS)?)?;
    }
    m.work(mul(pair_bytes, rows.len().saturating_sub(1))?)?;
    let mut versions = Vec::new();
    let mut tuples = Vec::new();
    for (key, row) in rows {
        m.checkpoint()?;
        row_check(t, &row.values)?;
        // PK helper is the engine's canonical full immutable identity encoder.
        if pk(t, &row.values)? != *key || row.version == 0 || row.version >= state.next_version {
            return Err(SqlError::Version);
        }
        for version in &versions {
            m.checkpoint()?;
            if *version == row.version {
                return Err(SqlError::Version);
            }
        }
        versions.push(row.version);
        let tuple = if positions
            .iter()
            .any(|i| matches!(row.values[*i], Value::Null))
        {
            None
        } else {
            let mut encoded = Vec::new();
            for i in &positions {
                encoded.extend(encode_key(std::slice::from_ref(&row.values[*i]))?);
            }
            Some(encoded)
        };
        tuples.push(tuple);
    }
    // All canonical identities validated before any Duplicate classification.
    for (i, tuple) in tuples.iter().enumerate() {
        m.checkpoint()?;
        for other in &tuples[..i] {
            m.checkpoint()?;
            if let (Some(a), Some(b)) = (tuple, other)
                && a == b
            {
                return Err(SqlError::Duplicate);
            }
        }
    }
    m.checkpoint()?; // after complete validation
    let proof = UniqueReadiness {
        _state: state,
        _table: t,
        _rows: rows,
        _descriptor: d,
        _epoch: epoch,
        _generation: generation,
    };
    m.checkpoint()?; // final return fence
    Ok((proof, m.counts))
}

#[cfg(test)]
#[path = "secondary_index_unique_readiness_tests.rs"]
mod tests;
