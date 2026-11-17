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
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::context::TaskContext;
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

/// Per-statement counters (counts only; limits come with R3-402).
#[derive(Debug, Default)]
pub(crate) struct StatementMeter {
    scanned: Cell<u64>,
    read: Cell<u64>,
}

impl StatementMeter {
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

    /// Counts one row pulled from the task table and keeps it only when the
    /// predicate accepts it. A predicate that fails to evaluate hides the row
    /// (fail closed).
    pub(crate) fn keep(&self, row: &SqlRow, meter: &StatementMeter) -> bool {
        meter.scanned();
        let keep = self.predicate.accepts_row(row).unwrap_or(false);
        if keep {
            meter.read();
        }
        keep
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

    fn keep(&self, row: &SourceRow<'_>) -> bool {
        self.scope.keep(&row.row, self.meter)
    }

    fn filter<'s>(&'s self, rows: SourceRows<'s>) -> SourceRows<'s> {
        Box::new(rows.filter(move |row| match row {
            Ok(row) => self.keep(row),
            Err(_) => true,
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
        let predicates = self.with_pushdown(predicates);
        Ok(self
            .inner
            .index_scan(table, &predicates, after)?
            .map(|rows| self.filter(rows)))
    }

    fn scan<'s>(&'s self, table: &TableDef, range: &KeyRange, dir: Direction) -> R<SourceRows<'s>> {
        self.table(table)?;
        Ok(self.filter(self.inner.scan(table, range, dir)?))
    }

    fn get<'s>(&'s self, table: &TableDef, key: &[u8]) -> R<Option<SourceRow<'s>>> {
        self.table(table)?;
        Ok(self.inner.get(table, key)?.filter(|row| self.keep(row)))
    }
}

#[cfg(test)]
#[path = "rows_tests.rs"]
mod tests;
