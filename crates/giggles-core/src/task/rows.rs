//! Visible rows of a task session (GD-0061 R3-305, design-3 section 3).
//!
//! The visible-rows predicate is applied at the row source, not added to the
//! statement's WHERE clause: [`Scoped`] wraps the statement's `RowSource`, and
//! every row it pulls from the task table is counted as scanned and dropped
//! unless the predicate accepts it. No statement expression ever receives a row
//! outside the scope, so an expression that would fail on such a row never runs
//! on it. `index_scan` and `index_input` add the predicate's `pushdown()` so a
//! secondary index on a scope column narrows the input.
//!
//! Wrapped here: the SELECT cursor (`Cursor::fetch`) and the buffered read
//! (`v2_reduction::Source` in `execute_v2`) (R3-305); the searched UPDATE's key
//! selection (`MutationRows` in `SqlRows::mutation_keys`), the sink reads the
//! mutation pages come from (`SqlRows` carries the scope), and native requests
//! (`visit_native_page`, `visit_native_key`, `complete_table_engine`) (R3-306).
//! An UPDATE of a row outside the scope therefore matches zero rows, on the
//! SQL path and the native path alike; the protected commit's policy check
//! (`decide`) stays as the backstop.
use std::cell::{Cell, RefCell};
use std::ops::Bound;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::context::TaskContext;
use super::definition::TaskDef;
use super::error::{DenyRule, TaskError};
use crate::relational::scan::{Direction, KeyRange, RowSource, SourceRow, SourceRows};
use crate::relational::{Relational, RowPredicate, SqlError, SqlRow, TableDef, Value};
use crate::storage::engine::txn::Transaction;
use crate::storage::v2::V2Db;
use crate::storage::CoreError;

type R<T> = Result<T, SqlError>;

/// Row counts of one scoped read: rows pulled from the task table, and rows of
/// them the visible-rows predicate accepted. For the read log and load limits
/// (R3-308, R3-402); never sent to the agent.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RowCounts {
    pub scanned: u64,
    pub read: u64,
}

/// Per-read counters. The statement's limits are checked in the thread's
/// meter (`limits::count_scanned`, `limits::count_read`, R3-402).
#[derive(Debug, Default)]
pub(crate) struct StatementMeter {
    scanned: Cell<u64>,
    read: Cell<u64>,
    /// Rows of `read` already added to the session's pending `rows_read`.
    charged: Cell<u64>,
    /// Rows of `scanned` already added to the session's pending count (R3-309).
    scanned_taken: Cell<u64>,
}

impl StatementMeter {
    /// Rows read since the last call (R3-308 charges them as `rows_read`).
    pub(crate) fn take_read_delta(&self) -> u64 {
        let read = self.read.get();
        let delta = read.saturating_sub(self.charged.get());
        self.charged.set(read);
        delta
    }
    /// Rows scanned since the last call (the read log's `rows_scanned`).
    pub(crate) fn take_scanned_delta(&self) -> u64 {
        let scanned = self.scanned.get();
        let delta = scanned.saturating_sub(self.scanned_taken.get());
        self.scanned_taken.set(scanned);
        delta
    }
    fn scanned(&self) {
        self.scanned.set(self.scanned.get().saturating_add(1));
    }
    fn read(&self) {
        self.read.set(self.read.get().saturating_add(1));
    }
    pub(crate) fn counts(&self) -> RowCounts {
        RowCounts {
            scanned: self.scanned.get(),
            read: self.read.get(),
        }
    }
}

thread_local! {
    static LAST: Cell<Option<RowCounts>> = const { Cell::new(None) };
}

/// Publishes a scoped read's counts for this thread (a cursor publishes its
/// running totals after each fetch).
pub(crate) fn record(counts: RowCounts) {
    LAST.set(Some(counts));
}

/// The counts of the latest scoped read on this thread, cleared by the call.
/// `None` when no task-session read ran on this thread since the last call.
pub fn take_last_counts() -> Option<RowCounts> {
    LAST.take()
}

/// The visible-rows scope of one task statement.
#[derive(Clone, Debug)]
pub(crate) struct Scope {
    predicate: Arc<RowPredicate>,
}

impl Scope {
    pub(crate) fn table(&self) -> u64 {
        self.predicate.table()
    }

    pub(crate) fn key_range(&self) -> Option<&KeyRange> {
        self.predicate.key_range()
    }

    pub(crate) fn prefix_values(&self, table: &TableDef) -> Vec<Value> {
        if self.key_range().is_none() { return Vec::new(); }
        table.primary_key.iter().map_while(|i| self.predicate.pushdown().iter()
            .find(|(column, op, _)| column == i && *op == "=")
            .map(|(_, _, value)| value.clone())).collect()
    }

    pub(crate) fn contains_key(&self, key: &[u8]) -> bool {
        self.key_range().is_none_or(|range| !intersect(range, &KeyRange {
            lo: Bound::Included(key.to_vec()), hi: Bound::Included(key.to_vec()),
        }).is_empty())
    }

    /// Counts one row pulled from the task table and keeps it only when the
    /// predicate accepts it. A predicate that fails to evaluate hides the row
    /// (fail closed). Past the statement's `rows_scanned`, or past its
    /// rows-read allowance, the row stops the statement (R3-402); the row is
    /// counted either way, so the read log holds the actual count.
    pub(crate) fn keep(&self, row: &SqlRow, meter: &StatementMeter) -> Result<bool, TaskError> {
        meter.scanned();
        super::limits::count_scanned()?;
        let keep = self.predicate.accepts_row(row).unwrap_or(false);
        if keep {
            meter.read();
            super::limits::count_read()?;
        }
        Ok(keep)
    }
}

/// Bound predicates by `(task, def_version, catalog version)`, on the v2
/// database handle. A statement whose catalog version is unknown (a private
/// catalog overlay or the old-snapshot fallback) binds without caching.
#[derive(Default)]
pub(crate) struct PredicateCache(Mutex<HashMap<(u64, u64, u64), Arc<RowPredicate>>>);

/// Entries kept before the cache is cleared; versions only move forward, so
/// old entries are never needed again once the catalog moves on.
const CACHE_ENTRIES: usize = 256;

impl PredicateCache {
    fn get_or_bind(
        &self,
        key: Option<(u64, u64, u64)>,
        bind: impl FnOnce() -> Result<RowPredicate, TaskError>,
    ) -> Result<Arc<RowPredicate>, TaskError> {
        let Some(key) = key else {
            return bind().map(Arc::new);
        };
        if let Some(found) = self.lock().get(&key) {
            return Ok(found.clone());
        }
        let bound = Arc::new(bind()?);
        let mut cache = self.lock();
        if cache.len() >= CACHE_ENTRIES {
            cache.clear();
        }
        Ok(cache.entry(key).or_insert(bound).clone())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<(u64, u64, u64), Arc<RowPredicate>>> {
        self.0.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

}

#[cfg(test)]
thread_local! {
    /// Predicate binds on this thread, cached or not.
    static BINDS: Cell<usize> = const { Cell::new(0) };
}

/// The scope of a task statement reading at `txn`'s snapshot, with `image` the
/// catalog image the statement binds on. Fails closed: a missing or revoked
/// task is `Revoked`, a changed definition or a predicate that no longer binds
/// is `DefinitionChanged`, and a missing table is `OtherTable`.
pub(crate) fn scope(
    db: &V2Db,
    txn: &mut Transaction,
    image: &Relational,
    ctx: &TaskContext,
) -> Result<Scope, CoreError> {
    scope_with(db, txn, ctx, |_| Ok(image))
}

/// [`scope`] for a caller without a statement image (native requests): the
/// catalog image at `txn`'s snapshot is built only when the predicate is not
/// cached.
#[cfg_attr(not(feature = "engine-rocksdb"), allow(dead_code))]
pub(crate) fn scope_at(
    db: &V2Db,
    txn: &mut Transaction,
    ctx: &TaskContext,
) -> Result<Scope, CoreError> {
    scope_with(db, txn, ctx, |txn| {
        db.catalog().snapshot(txn)?.image().map_err(CoreError::Sql)
    })
}

fn scope_with<I: std::borrow::Borrow<Relational>>(
    db: &V2Db,
    txn: &mut Transaction,
    ctx: &TaskContext,
    image: impl FnOnce(&mut Transaction) -> Result<I, CoreError>,
) -> Result<Scope, CoreError> {
    let view = db.catalog().committed_view(txn)?;
    let def = match view.tasks.get(&ctx.task.0) {
        Some(def) if !def.revoked => def.clone(),
        _ => return Err(TaskError::Revoked.into()),
    };
    if def.def_version != ctx.bound_def_version {
        return Err(TaskError::Denied(DenyRule::DefinitionChanged).into());
    }
    if super::limits::wants_read_allowance() {
        super::limits::set_read_allowance(read_allowance(txn, &def));
    }
    let key = db
        .catalog()
        .snapshot_version(txn)
        .map(|version| (def.id.0, def.def_version, version.0));
    let predicate = db.row_predicates.get_or_bind(key, || {
        #[cfg(test)]
        BINDS.set(BINDS.get() + 1);
        let image = image(txn)?;
        let image = image.borrow();
        let table = image
            .tables
            .values()
            .find(|table| table.id == def.table)
            .ok_or(TaskError::Denied(DenyRule::OtherTable))?;
        RowPredicate::bind(image, table, &def.visible_rows.0)
            .map_err(|_| TaskError::Denied(DenyRule::DefinitionChanged))
    })?;
    Ok(Scope { predicate })
}

/// The scope of `def` itself, bound against `image` without the cache and
/// without a task session: the exposure report's live count (R3-501).
pub(crate) fn scope_of_def(image: &Relational, def: &TaskDef) -> Result<Scope, CoreError> {
    let table = image
        .tables
        .values()
        .find(|table| table.id == def.table)
        .ok_or(TaskError::Denied(DenyRule::OtherTable))?;
    let predicate = RowPredicate::bind(image, table, &def.visible_rows.0)
        .map_err(|_| TaskError::Denied(DenyRule::DefinitionChanged))?;
    Ok(Scope {
        predicate: Arc::new(predicate),
    })
}

/// The rows the task may still read: its `rows_read` cap less the committed
/// head, read at the latest committed state without entering the read set.
/// A head that cannot be read counts as unused; the debit at release still
/// holds the cap.
fn read_allowance(txn: &Transaction, def: &super::definition::TaskDef) -> u64 {
    let head = txn
        .kernel_latest_committed(&super::records::disclosure_head_key(def.id))
        .ok()
        .flatten()
        .and_then(|value| super::records::DisclosureHead::decode(&value).ok());
    let used = head.map_or(0, |head| head.rows_read);
    def.disclosure_caps.rows_read.saturating_sub(used)
}

/// A `RowSource` that only yields the task's visible rows.
pub(crate) struct Scoped<'a> {
    inner: &'a dyn RowSource,
    scope: &'a Scope,
    meter: &'a StatementMeter,
    error: RefCell<Option<TaskError>>,
}

impl<'a> Scoped<'a> {
    pub(crate) fn new(inner: &'a dyn RowSource, scope: &'a Scope, meter: &'a StatementMeter) -> Self {
        Self {
            inner,
            scope,
            meter,
            error: RefCell::new(None),
        }
    }

    /// The task error behind a failed read, if the wrapper raised it. Callers
    /// check it before the inner source's own error slot.
    pub(crate) fn take_error(&self) -> Option<TaskError> {
        self.error.borrow_mut().take()
    }

    /// A task statement reads only its task table. Unreachable once the column
    /// rules (R3-303) run before execution.
    fn table(&self, table: &TableDef) -> R<()> {
        if table.id == self.scope.table() {
            return Ok(());
        }
        *self.error.borrow_mut() = Some(TaskError::Denied(DenyRule::OtherTable));
        Err(SqlError::Unsupported)
    }

    /// Whether to yield `row`; a limit it trips is kept for `take_error`
    /// and stops the read.
    fn keep(&self, row: &SourceRow<'_>) -> R<bool> {
        self.scope.keep(&row.row, self.meter).map_err(|error| {
            *self.error.borrow_mut() = Some(error);
            SqlError::Unsupported
        })
    }

    fn filter<'s>(&'s self, rows: SourceRows<'s>) -> SourceRows<'s> {
        Box::new(rows.filter_map(move |row| match row {
            Ok(row) => match self.keep(&row) {
                Ok(true) => Some(Ok(row)),
                Ok(false) => None,
                Err(error) => Some(Err(error)),
            },
            Err(error) => Some(Err(error)),
        }))
    }

    fn with_pushdown<'p>(&self, predicates: &[(usize, &'p str, Value)]) -> Vec<(usize, &'p str, Value)> {
        let mut all = predicates.to_vec();
        all.extend(
            self.scope
                .predicate
                .pushdown()
                .iter()
                .map(|(column, op, value)| (*column, *op, value.clone())),
        );
        all
    }
}

/// Intersect inclusive/exclusive bounds without widening equal endpoints.
pub(crate) fn intersect(a: &KeyRange, b: &KeyRange) -> KeyRange {
    fn tighter(a: &Bound<Vec<u8>>, b: &Bound<Vec<u8>>, lower: bool) -> Bound<Vec<u8>> {
        match (a, b) {
            (Bound::Unbounded, _) => b.clone(),
            (_, Bound::Unbounded) => a.clone(),
            (Bound::Included(x) | Bound::Excluded(x), Bound::Included(y) | Bound::Excluded(y)) => {
                if x == y {
                    if matches!(a, Bound::Excluded(_)) { a.clone() } else { b.clone() }
                } else if (x > y) == lower { a.clone() } else { b.clone() }
            }
        }
    }
    KeyRange { lo: tighter(&a.lo, &b.lo, true), hi: tighter(&a.hi, &b.hi, false) }
}

impl RowSource for Scoped<'_> {
    fn streams_reductions(&self) -> bool {
        self.inner.streams_reductions()
    }

    fn join_probe<'s>(
        &'s self,
        table: &TableDef,
        keys: &[(usize, Option<Value>)],
        first: bool,
    ) -> R<Option<SourceRows<'s>>> {
        self.table(table)?;
        // A probe can choose an index outside the primary-key scope. Let the
        // caller fall back to our bounded scan instead.
        if self.scope.predicate.key_range().is_some() { return Ok(None); }
        Ok(self
            .inner
            .join_probe(table, keys, first)?
            .map(|rows| self.filter(rows)))
    }

    fn index_input<'s>(
        &'s self,
        table: &TableDef,
        predicates: &[(usize, &str, Value)],
    ) -> R<Option<SourceRows<'s>>> {
        self.table(table)?;
        if self.scope.predicate.key_range().is_some() { return Ok(None); }
        let predicates = self.with_pushdown(predicates);
        Ok(self
            .inner
            .index_input(table, &predicates)?
            .map(|rows| self.filter(rows)))
    }

    fn index_scan<'s>(
        &'s self,
        table: &TableDef,
        predicates: &[(usize, &str, Value)],
        after: Option<&[u8]>,
    ) -> R<Option<SourceRows<'s>>> {
        self.table(table)?;
        if self.scope.predicate.key_range().is_some() { return Ok(None); }
        let predicates = self.with_pushdown(predicates);
        Ok(self
            .inner
            .index_scan(table, &predicates, after)?
            .map(|rows| self.filter(rows)))
    }

    fn scan<'s>(&'s self, table: &TableDef, range: &KeyRange, dir: Direction) -> R<SourceRows<'s>> {
        self.table(table)?;
        let range = self.scope.predicate.key_range().map_or_else(
            || range.clone(), |scope| intersect(range, scope));
        if range.is_empty() { return Ok(Box::new(std::iter::empty())); }
        Ok(self.filter(self.inner.scan(table, &range, dir)?))
    }

    fn get<'s>(&'s self, table: &TableDef, key: &[u8]) -> R<Option<SourceRow<'s>>> {
        self.table(table)?;
        if !self.scope.contains_key(key) { return Ok(None); }
        match self.inner.get(table, key)? {
            Some(row) if self.keep(&row)? => Ok(Some(row)),
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
#[path = "rows_tests.rs"]
mod tests;

#[cfg(test)]
mod range_tests {
    use super::*;

    #[test]
    fn outside_prefix_point_and_empty_scan_do_not_read_source() {
        struct Unread;
        impl RowSource for Unread {
            fn scan<'s>(&'s self, _: &TableDef, _: &KeyRange, _: Direction) -> R<SourceRows<'s>> {
                panic!("empty intersection must not scan");
            }
            fn get<'s>(&'s self, _: &TableDef, _: &[u8]) -> R<Option<SourceRow<'s>>> {
                panic!("outside key must not be read");
            }
        }
        let mut state = Relational::default();
        let prepared = crate::relational::Prepared::parse(
            "CREATE TABLE items (merchant BIGINT, id INT, PRIMARY KEY (merchant, id))",
            1, state.epoch).unwrap();
        state.execute(&prepared, &[], None).unwrap();
        let table = &state.tables["items"];
        let scope = Scope { predicate: Arc::new(RowPredicate::bind(&state, table, "merchant = 42").unwrap()) };
        let meter = StatementMeter::default();
        let source = Scoped::new(&Unread, &scope, &meter);
        let key = crate::relational::encode_key(&[Value::Int(7), Value::Int(1)]).unwrap();
        assert!(source.get(table, &key).unwrap().is_none());
        assert_eq!(source.scan(table, &KeyRange::point(key), Direction::Forward).unwrap().count(), 0);
        assert_eq!(meter.counts(), RowCounts::default());
    }

    #[test]
    fn intersection_keeps_stricter_equal_endpoints_and_empty_ranges() {
        let scope = KeyRange { lo: Bound::Included(vec![2]), hi: Bound::Excluded(vec![4]) };
        assert_eq!(intersect(&KeyRange::all(), &scope), scope);
        assert_eq!(intersect(&scope, &KeyRange::all()), scope);
        let caller = KeyRange { lo: Bound::Excluded(vec![2]), hi: Bound::Included(vec![4]) };
        assert_eq!(intersect(&caller, &scope), KeyRange {
            lo: Bound::Excluded(vec![2]), hi: Bound::Excluded(vec![4]),
        });
        assert_eq!(intersect(&scope, &caller), intersect(&caller, &scope));
        assert!(intersect(&scope, &KeyRange::point(vec![4])).is_empty());
        assert!(intersect(&scope, &KeyRange::point(vec![1])).is_empty());
        assert!(!intersect(&scope, &KeyRange::point(vec![2])).is_empty());
    }
}
