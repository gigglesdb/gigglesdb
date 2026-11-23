//! Streaming row sources and pipeline primitives (GD-0058 E3). Wired into query execution by B2.

use super::*;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::ops::Bound;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Direction {
    Forward,
    #[allow(dead_code)] // WO-C2 will select reverse scans for ordered pagination.
    Reverse,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KeyRange {
    pub(crate) lo: Bound<Vec<u8>>,
    pub(crate) hi: Bound<Vec<u8>>,
}
impl KeyRange {
    #[cfg(test)]
    pub(crate) fn all() -> Self {
        Self {
            lo: Bound::Unbounded,
            hi: Bound::Unbounded,
        }
    }
    #[cfg(test)]
    pub(crate) fn point(key: Vec<u8>) -> Self {
        Self {
            lo: Bound::Included(key.clone()),
            hi: Bound::Included(key),
        }
    }
    pub(crate) fn is_empty(&self) -> bool {
        match (&self.lo, &self.hi) {
            (Bound::Included(a), Bound::Included(b)) => a > b,
            (Bound::Included(a) | Bound::Excluded(a), Bound::Included(b) | Bound::Excluded(b)) => {
                a >= b
            }
            _ => false,
        }
    }
}
impl KeyRange {
    pub(super) fn from_bounds(b: Bounds) -> KeyRange {
        if b.low > b.high || (b.low == b.high && !b.point) {
            return KeyRange {
                lo: Bound::Excluded(b.low.clone()),
                hi: Bound::Excluded(b.low),
            };
        }
        KeyRange {
            lo: if b.low_inclusive {
                Bound::Included(b.low)
            } else {
                Bound::Excluded(b.low)
            },
            hi: if b.high_inclusive {
                Bound::Included(b.high)
            } else {
                Bound::Excluded(b.high)
            },
        }
    }
}
pub(crate) struct SourceRow<'a> {
    #[allow(dead_code)] // WO-C2 pagination will retain the source key.
    pub(crate) key: Cow<'a, [u8]>,
    pub(crate) row: Cow<'a, SqlRow>,
}
pub(crate) type SourceRows<'s> = Box<dyn Iterator<Item = R<SourceRow<'s>>> + 's>;
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum RowLock { KeyShare, Share, NoKeyUpdate, Update }
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum RowWait { Wait, Skip, NoWait }
pub(crate) trait RowSource {
    fn lock_row(&self, _table: &TableDef, _values: &[Value], _mode: RowLock, _wait: RowWait)
        -> R<Option<SqlRow>> { Err(SqlError::Unsupported) }

    fn streams_reductions(&self) -> bool { false }
    /// Optional indexed inner candidates; None preserves the scan fallback.
    /// `first` requests one-row pages for demand-driven EXISTS.
    fn join_probe<'s>(&'s self, _table: &TableDef, _keys: &[(usize, Option<Value>)], _first: bool)
        -> R<Option<SourceRows<'s>>> { Ok(None) }
    /// Optional paged statement input; resume positions remain private to it.
    fn index_input<'s>(&'s self, _table: &TableDef, _predicates: &[(usize, &str, Value)])
        -> R<Option<SourceRows<'s>>> { Ok(None) }
    /// Optional unordered secondary access. Keys are opaque resume positions.
    fn index_scan<'s>(
        &'s self,
        _table: &TableDef,
        _predicates: &[(usize, &str, Value)],
        _after: Option<&[u8]>,
    ) -> R<Option<SourceRows<'s>>> {
        Ok(None)
    }
    /// Lazily reads rows only when pulled, ordered by key forward or reverse.
    fn scan<'s>(
        &'s self,
        table: &TableDef,
        range: &KeyRange,
        dir: Direction,
    ) -> R<Box<dyn Iterator<Item = R<SourceRow<'s>>> + 's>>;
    fn get<'s>(&'s self, table: &TableDef, key: &[u8]) -> R<Option<SourceRow<'s>>>;
    fn contains(&self, table: &TableDef, key: &[u8]) -> R<bool> {
        Ok(self.get(table, key)?.is_some())
    }
}
pub(crate) struct LegacyRows<'a>(pub(crate) &'a Relational);
impl RowSource for LegacyRows<'_> {
    fn index_input<'s>(&'s self, table: &TableDef, predicates: &[(usize, &str, Value)])
        -> R<Option<SourceRows<'s>>> {
        super::secondary_index_access::sql_temporal_candidates(self.0, table, predicates)
    }
    fn scan<'s>(
        &'s self,
        table: &TableDef,
        range: &KeyRange,
        dir: Direction,
    ) -> R<Box<dyn Iterator<Item = R<SourceRow<'s>>> + 's>> {
        let rows = self.0.rows.get(&table.id).ok_or(SqlError::UnknownTable)?;
        if range.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }
        let it = rows.range((range.lo.clone(), range.hi.clone()));
        let row = |(key, row): (&'s Vec<u8>, &'s SqlRow)| {
            Ok(SourceRow {
                key: Cow::Borrowed(key.as_slice()),
                row: Cow::Borrowed(row),
            })
        };
        Ok(match dir {
            Direction::Forward => Box::new(it.map(row)),
            Direction::Reverse => Box::new(it.rev().map(row)),
        })
    }
    fn get<'s>(&'s self, table: &TableDef, key: &[u8]) -> R<Option<SourceRow<'s>>> {
        let rows = self.0.rows.get(&table.id).ok_or(SqlError::UnknownTable)?;
        Ok(rows.get_key_value(key).map(|(key, row)| SourceRow {
            key: Cow::Borrowed(key.as_slice()),
            row: Cow::Borrowed(row),
        }))
    }
}
/// A materialized statement keeps its established scan semantics while row
/// marks acquire physical locks and refresh through the transaction source.
pub(crate) struct LockedSnapshot<'a> {
    pub(crate) snapshot: LegacyRows<'a>,
    pub(crate) locks: &'a dyn RowSource,
}
impl RowSource for LockedSnapshot<'_> {
    fn streams_reductions(&self) -> bool { true }
    fn lock_row(&self, table: &TableDef, values: &[Value], mode: RowLock, wait: RowWait)
        -> R<Option<SqlRow>> { self.locks.lock_row(table, values, mode, wait) }
    fn scan<'s>(&'s self, table: &TableDef, range: &KeyRange, dir: Direction) -> R<SourceRows<'s>> {
        self.snapshot.scan(table, range, dir)
    }
    fn get<'s>(&'s self, table: &TableDef, key: &[u8]) -> R<Option<SourceRow<'s>>> {
        self.snapshot.get(table, key)
    }
}
pub(crate) const CANCEL_POLL_ROWS: u64 = 64;
pub(crate) struct Meter<'c> {
    cancel: &'c mut dyn FnMut() -> bool,
    rows: u64,
    every: u64,
}
impl<'c> Meter<'c> {
    pub(crate) fn new(cancel: &'c mut dyn FnMut() -> bool) -> Self {
        Self {
            cancel,
            rows: 0,
            every: CANCEL_POLL_ROWS,
        }
    }
    /// Poll cancel before every input row. SQL aggregates keep DD-0068's fence
    /// contract: entry, every input row, then a final normal-success fence.
    pub(crate) fn per_row(cancel: &'c mut dyn FnMut() -> bool) -> Self {
        Self {
            cancel,
            rows: 0,
            every: 1,
        }
    }
    pub(crate) fn tick(&mut self) -> R<()> {
        self.rows = self.rows.checked_add(1).ok_or(SqlError::Limit)?;
        if self.rows.is_multiple_of(self.every) && (self.cancel)() {
            return Err(SqlError::Aborted);
        }
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn rows(&self) -> u64 {
        self.rows
    }
}
/// The legacy database is bounded to 16 MiB serialized; 64 MiB safely bounds
/// retained in-memory estimates. V2 spilling is tracked by GD-0054 F14.
pub(crate) const MAX_RETAINED_BYTES: usize = 64 * 1024 * 1024;
/// Bounds SQL SqlResult vectors until track F F10 cursors and track D portal
/// row limits are available. The independent result byte cap remains in force.
pub(crate) const MAX_QUERY_ROWS: usize = 65_536;
pub(crate) struct Retention {
    used: usize,
    cap: usize,
    #[cfg(test)]
    peak: usize,
}
impl Retention {
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            used: 0,
            cap,
            #[cfg(test)]
            peak: 0,
        }
    }
    pub(crate) fn charge(&mut self, bytes: usize) -> R<()> {
        let used = self.used.checked_add(bytes).ok_or(SqlError::Limit)?;
        if used > self.cap {
            return Err(SqlError::Limit);
        }
        self.used = used;
        #[cfg(test)]
        {
            self.peak = self.peak.max(used);
        }
        Ok(())
    }
    pub(crate) fn release(&mut self, bytes: usize) {
        self.used = self.used.saturating_sub(bytes);
    }
    pub(crate) fn used(&self) -> usize {
        self.used
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct Page {
    pub(crate) offset: u64,
    pub(crate) limit: Option<u64>,
}
impl Page {
    pub(crate) fn keep(&self) -> Option<usize> {
        self.limit
            .map(|n| usize::try_from(self.offset.saturating_add(n)).unwrap_or(usize::MAX))
    }
    pub(crate) fn apply<I: Iterator>(self, it: I) -> impl Iterator<Item = I::Item> {
        let mut it = it.skip(usize::try_from(self.offset).unwrap_or(usize::MAX));
        let mut remaining = self.limit.map(|n| usize::try_from(n).unwrap_or(usize::MAX));
        std::iter::from_fn(move || {
            if remaining == Some(0) {
                return None;
            }
            let item = it.next()?;
            if let Some(n) = remaining.as_mut() {
                *n -= 1;
            }
            Some(item)
        })
    }
}
pub(crate) fn filtered<'s, 'm, T>(
    mut rows: Box<dyn Iterator<Item = R<SourceRow<'s>>> + 's>,
    meter: &'m mut Meter<'_>,
    mut keep: impl FnMut(SourceRow<'s>) -> R<Option<T>> + 'm,
) -> impl Iterator<Item = R<T>> + 'm
where
    's: 'm,
{
    let mut done = false;
    std::iter::from_fn(move || {
        if done {
            return None;
        }
        loop {
            let result = match rows.next() {
                Some(Ok(row)) => meter.tick().and_then(|()| keep(row)),
                Some(Err(e)) => Err(e),
                None => {
                    done = true;
                    return None;
                }
            };
            match result {
                Ok(Some(v)) => return Some(Ok(v)),
                Ok(None) => {}
                Err(e) => {
                    done = true;
                    return Some(Err(e));
                }
            }
        }
    })
}

pub(crate) trait RetentionBudget {
    fn charge(&mut self, bytes: usize) -> R<()>;
    fn release(&mut self, bytes: usize);
}
impl RetentionBudget for Retention {
    fn charge(&mut self, bytes: usize) -> R<()> { Retention::charge(self, bytes) }
    fn release(&mut self, bytes: usize) { Retention::release(self, bytes); }
}

pub(crate) fn sort_bounded<T>(
    rows: impl Iterator<Item = R<T>>,
    keep: Option<usize>,
    size: impl Fn(&T) -> usize,
    cmp: impl Fn(&T, &T) -> Ordering,
    retention: &mut impl RetentionBudget,
) -> R<Vec<T>> {
    if keep == Some(0) {
        return Ok(Vec::new());
    }
    let mut retained = 0;
    let result = (|| {
        let mut heap: Vec<(T, usize, usize)> = Vec::new();
        let compare =
            |a: &(T, usize, usize), b: &(T, usize, usize)| cmp(&a.0, &b.0).then(a.1.cmp(&b.1));
        let mut sequence = 0usize;
        for row in rows {
            let row = row?;
            let current_sequence = sequence;
            sequence = sequence.checked_add(1).ok_or(SqlError::Limit)?;
            let sequence = current_sequence;
            if let Some(k) = keep
                && heap.len() == k
            {
                if let Some(top) = heap.first()
                    && cmp(&row, &top.0).then(sequence.cmp(&top.1)) != Ordering::Less
                {
                    continue;
                }
                let bytes = size(&row);
                if let Some(top) = heap.first_mut() {
                    retention.release(top.2);
                    retained -= top.2;
                    retention.charge(bytes)?;
                    retained += bytes;
                    *top = (row, sequence, bytes);
                }
                let mut parent = 0usize;
                while let Some(left) = parent.checked_mul(2).and_then(|n| n.checked_add(1)) {
                    if left >= heap.len() {
                        break;
                    }
                    let right = left + 1;
                    let child = if right < heap.len()
                        && compare(&heap[right], &heap[left]) == Ordering::Greater
                    {
                        right
                    } else {
                        left
                    };
                    if compare(&heap[child], &heap[parent]) != Ordering::Greater {
                        break;
                    }
                    heap.swap(parent, child);
                    parent = child;
                }
                continue;
            }
            let bytes = size(&row);
            retention.charge(bytes)?;
            retained += bytes;
            heap.push((row, sequence, bytes));
            if keep.is_some() {
                let mut child = heap.len() - 1;
                while child > 0 {
                    let parent = (child - 1) / 2;
                    if compare(&heap[child], &heap[parent]) != Ordering::Greater {
                        break;
                    }
                    heap.swap(parent, child);
                    child = parent;
                }
            }
        }
        heap.sort_by(compare);
        Ok(heap.into_iter().map(|(row, _, _)| row).collect())
    })();
    retention.release(retained);
    result
}

#[cfg(test)]
#[path = "scan_tests.rs"]
mod tests;

// Cursor execution has an independent retention budget. Native Vec-returning
// entry points continue to use the original result and retention limits.
thread_local! {
    static CURSOR_LIMIT: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}
pub(crate) fn with_cursor_limit<T>(limit: usize, f: impl FnOnce() -> T) -> T {
    struct Restore(Option<usize>);
    impl Drop for Restore {
        fn drop(&mut self) {
            CURSOR_LIMIT.with(|c| c.set(self.0));
        }
    }
    let _restore = Restore(CURSOR_LIMIT.with(|c| c.replace(Some(limit))));
    f()
}
pub(crate) fn result_limit() -> usize {
    CURSOR_LIMIT.with(|c| c.get().unwrap_or(MAX_RESULT_BYTES))
}
pub(crate) fn retention_limit() -> usize {
    CURSOR_LIMIT.with(|c| c.get().unwrap_or(MAX_RETAINED_BYTES))
}
pub(crate) fn query_row_limit() -> usize {
    super::spill::query_row_limit(CURSOR_LIMIT.with(|c| {
        c.get()
            .map_or(MAX_QUERY_ROWS, |n| n / std::mem::size_of::<SqlRow>())
    }))
}
