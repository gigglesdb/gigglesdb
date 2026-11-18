//! Complete, bounded materializations tied to one active transaction revision.
use super::{CoreError, SqlSession};
use crate::relational::{MAX_ROWS, Scan, SqlError, SqlRow, TableDef};
use crate::task::rows::{Scope, StatementMeter};
use crate::task::TaskError;

#[derive(Debug, PartialEq, Eq)]
struct Snapshot {
    domain: u128,
    epoch: u64,
    transaction: u128,
    revision: u64,
    read_timestamp: Option<u64>,
    table: TableDef,
}

/// Only a successful full-table proof can construct this value. In particular,
/// ordinary paged scans and row vectors cannot be promoted to complete results.
/// It is consumed once, after checking the current transaction and revision.
#[derive(Debug)]
pub(crate) struct CompleteRelation {
    snapshot: Snapshot,
    rows: Vec<SqlRow>,
}

thread_local! {
    // (decoded, retained) rows of native filtered complete input on this thread.
    static NATIVE_INPUT_ROWS: std::cell::Cell<(u64, u64)> = const { std::cell::Cell::new((0, 0)) };
}

/// Rows decoded and rows retained (handed to native evaluation) by native v2
/// filtered and keyed input on this thread since it started. Diagnostics
/// only: two thread-local counter updates per call, no behaviour depends on
/// it. It is public because integration tests in other crates assert it.
pub fn native_input_rows() -> (u64, u64) {
    NATIVE_INPUT_ROWS.with(|n| n.get())
}

/// `visit` restricted to a task's visible rows (R3-306); every row offered is
/// counted on `meter`, and a row past the statement's limits stops the visit
/// (R3-402). Without a scope it visits every row.
fn scoped<'v, E: 'v>(
    scope: Option<&'v Scope>,
    meter: &'v StatementMeter,
    visit: &'v mut impl FnMut(&SqlRow) -> Result<(), E>,
    map: fn(CoreError) -> E,
) -> impl FnMut(&SqlRow) -> Result<(), E> + 'v {
    move |row| match scope.map(|scope| scope.keep(row, meter)) {
        Some(Ok(false)) => Ok(()),
        Some(Err(error)) => Err(map(error.into())),
        _ => visit(row),
    }
}

/// Logical bytes a retained native input row is charged.
fn retained_bytes(row: &SqlRow) -> usize {
    row.values.iter().fold(std::mem::size_of::<SqlRow>(), |bytes, value| {
        bytes.saturating_add(std::mem::size_of_val(value) + crate::relational::value_bytes(value))
    })
}

/// Shared complete native input retention for physical tables and expanded views.
/// The producer visits every row, while only the filtered top bound is retained.
pub(super) fn retain_native_rows<E, C>(
    keep: &mut impl FnMut(&SqlRow) -> Result<bool, E>,
    bound: (usize, Option<C>),
    map: fn(CoreError) -> E,
    visit_rows: impl FnOnce(&mut dyn FnMut(&SqlRow) -> Result<(), E>) -> Result<(), E>,
) -> Result<Vec<SqlRow>, E>
where
    C: Fn(&SqlRow, &SqlRow) -> std::cmp::Ordering + Copy,
{
    let (limit, order) = bound;
    let cmp = |a: &SqlRow, b: &SqlRow| order.map_or(std::cmp::Ordering::Equal, |cmp| cmp(a, b));
    let threshold = limit.saturating_mul(2).max(64);
    let mut rows = Vec::new();
    let mut retained = crate::relational::scan::Retention::new(
        crate::relational::scan::MAX_RETAINED_BYTES,
    );
    let mut sorter = (order.is_some() && limit > 10_000)
        .then(crate::relational::spill::Sorter::new);
    let mut visit = |row: &SqlRow| {
        crate::cancellation::check().map_err(map)?;
        NATIVE_INPUT_ROWS.with(|n| n.set((n.get().0 + 1, n.get().1)));
        if !keep(row)? {
            return Ok(());
        }
        if let Some(sorter) = &mut sorter {
            return sorter.push(row.clone(), retained_bytes(row), cmp, &mut retained)
                .map_err(|e| map(CoreError::Sql(e)));
        }
        retained.charge(retained_bytes(row)).map_err(|e| map(CoreError::Sql(e)))?;
        rows.push(row.clone());
        if rows.len() >= threshold {
            rows.sort_by(cmp);
            for dropped in rows.drain(limit..) {
                retained.release(retained_bytes(&dropped));
            }
        }
        Ok(())
    };
    visit_rows(&mut visit)?;
    crate::cancellation::check().map_err(map)?;
    if let Some(sorter) = sorter {
        let sorted = sorter.finish(cmp, &mut retained).map_err(|e| map(CoreError::Sql(e)))?;
        for row in sorted.take(limit) {
            let row = row.map_err(|e| map(CoreError::Sql(e)))?;
            retained.charge(retained_bytes(&row)).map_err(|e| map(CoreError::Sql(e)))?;
            rows.push(row);
        }
    }
    if rows.len() > limit {
        rows.sort_by(cmp);
        rows.truncate(limit);
    }
    NATIVE_INPUT_ROWS.with(|n| n.set((n.get().0, n.get().1 + rows.len() as u64)));
    Ok(rows)
}

impl SqlSession {
    /// V2 read input is complete without imposing the legacy table capacity.
    /// The physical visitor decodes one row at a time; native order/filter
    /// evaluation retains the resulting relation, independently of output pages.
    pub(crate) fn complete_read_table_engine(
        &mut self,
        table: &TableDef,
    ) -> Result<CompleteRelation, CoreError> {
        if !self.is_v2_engine() {
            return self.complete_table_engine(table);
        }
        let result = (|| {
            let snapshot = self.complete_snapshot(table)?;
            let mut rows = Vec::new();
            let mut retained = crate::relational::scan::Retention::new(
                crate::relational::scan::MAX_RETAINED_BYTES,
            );
            self.visit_native_page(table, &mut |row| {
                retained.charge(std::mem::size_of::<SqlRow>()).map_err(CoreError::Sql)?;
                for value in &row.values {
                    retained.charge(std::mem::size_of_val(value) + crate::relational::value_bytes(value))
                        .map_err(CoreError::Sql)?;
                }
                rows.push(row.clone());
                Ok(())
            }, |error| error)?;
            Ok(CompleteRelation { snapshot, rows })
        })();
        if result.is_err() {
            self.abort();
        }
        result
    }

    /// V2 complete input restricted to the rows `keep` accepts. Rows are
    /// decoded one at a time and only kept rows are retained, so the logical
    /// budget applies to the matching set. Key batches and integer ranges can
    /// narrow the physical input instead of scanning the table. The caller must re-apply
    /// its filter: the relation is complete only with respect to `keep`.
    ///
    /// `bound` keeps at most `limit` rows: whenever the retained set reaches
    /// twice that, `order` stably reorders it and the tail is released. A
    /// stable order of the whole set followed by `limit` rows keeps exactly
    /// these rows, so the caller's result is unchanged. Ordered bounds above
    /// 10,000 use external sort, charging only the first `limit` rows on readback.
    pub(crate) fn complete_read_table_where<E, C>(
        &mut self,
        table: &TableDef,
        access: &crate::operations::NativeAccess,
        keep: &mut impl FnMut(&SqlRow) -> Result<bool, E>,
        bound: (usize, Option<C>),
        map: fn(CoreError) -> E,
    ) -> Result<CompleteRelation, E>
    where
        C: Fn(&SqlRow, &SqlRow) -> std::cmp::Ordering + Copy,
    {
        let result = (|| {
            if !self.is_v2_engine() {
                return Err(map(CoreError::Sql(SqlError::Unsupported)));
            }
            let snapshot = self.complete_snapshot(table).map_err(map)?;
            let indexed = matches!(access, crate::operations::NativeAccess::Index(..));
            let sort_error = std::cell::RefCell::new(None);
            let cmp = |a: &SqlRow, b: &SqlRow| {
                bound.1.map_or(std::cmp::Ordering::Equal, |cmp| cmp(a, b)).then_with(|| {
                    let key = |row: &SqlRow| {
                        crate::storage::engine::codec::pk_bytes(
                            &table.primary_key.iter().map(|i| row.values[*i].clone()).collect::<Vec<_>>()
                        ).map_err(|error| *sort_error.borrow_mut() = Some(error)).ok()
                    };
                    key(a).cmp(&key(b))
                })
            };
            let effective = (bound.0, (indexed || bound.1.is_some()).then_some(|a: &SqlRow, b: &SqlRow| {
                if indexed { cmp(a, b) } else { bound.1.unwrap()(a, b) }
            }));
            let mut rows = retain_native_rows(keep, effective, map, |visit| match access {
                crate::operations::NativeAccess::Index(d, lo, hi) =>
                    self.visit_native_range_inner(table, lo.clone(), hi.clone(), Some(d), &mut |row| visit(row), map),
                crate::operations::NativeAccess::Keys(keys) =>
                    self.visit_native_keys(table, keys, &mut |row| visit(row), map),
                crate::operations::NativeAccess::Range(lo, hi) =>
                    self.visit_native_range(table, lo.clone(), hi.clone(), &mut |row| visit(row), map),
                crate::operations::NativeAccess::Empty => self.visit_native_keys(table, &[], &mut |row| visit(row), map),
                crate::operations::NativeAccess::Full => self.visit_native_page(table, &mut |row| visit(row), map),
            })?;
            if indexed { rows.sort_by(cmp); }
            if let Some(error) = sort_error.into_inner() {
                return Err(map(error.into()));
            }
            Ok(CompleteRelation { snapshot, rows })
        })();
        if result.is_err() {
            self.abort();
        }
        result
    }

    /// V2 keyed input: the snapshot certificate without rows. Callers find
    /// the existing rows they depend on with `native_key_rows`, so an insert
    /// or upsert reads one key per proposed row rather than the whole table.
    pub(crate) fn complete_keyed_engine(
        &mut self,
        table: &TableDef,
    ) -> Result<CompleteRelation, CoreError> {
        let result = (|| {
            if !self.is_v2_engine() {
                return Err(CoreError::Sql(SqlError::Unsupported));
            }
            Ok(CompleteRelation { snapshot: self.complete_snapshot(table)?, rows: Vec::new() })
        })();
        if result.is_err() {
            self.abort();
        }
        result
    }

    /// The existing V2 rows with these full primary keys, read by key lookup
    /// in the statement snapshot. Missing keys contribute no row. One
    /// snapshot check before and after the batch fences every lookup, so the
    /// cost is O(keys), not O(keys x catalog).
    pub(crate) fn native_key_rows(
        &mut self,
        table: &TableDef,
        keys: impl IntoIterator<Item = Vec<crate::relational::Value>>,
    ) -> Result<Vec<SqlRow>, CoreError> {
        let result = (|| {
            if !self.is_v2_engine() {
                return Err(CoreError::Sql(SqlError::Unsupported));
            }
            let snapshot = self.complete_snapshot(table)?;
            let mut rows: Vec<SqlRow> = Vec::new();
            #[cfg(feature = "engine-rocksdb")]
            if let super::Backend::V2(db) = &self.admin.inner.backend {
                let db = db.clone();
                for key in keys {
                    self.visit_native_key_v2(&db, table, &key, &mut |row: &SqlRow| {
                        NATIVE_INPUT_ROWS.with(|n| n.set((n.get().0 + 1, n.get().1 + 1)));
                        rows.push(row.clone());
                        Ok(())
                    }, |e| e)?;
                }
                if self.complete_snapshot(table)? != snapshot {
                    return Err(CoreError::Sql(SqlError::StaleSchema));
                }
                return Ok(rows);
            }
            let _ = (keys, &mut rows, snapshot);
            Err(CoreError::Sql(SqlError::Unsupported))
        })();
        if result.is_err() {
            self.abort();
        }
        result
    }

    /// The visible rows of a native request in a task session (GD-0061
    /// R3-306), or `None` outside one. A task reads only its task table
    /// (`OtherTable`), and tasks exist only on v2 (`Revoked` elsewhere).
    fn native_task_scope(&mut self, table: &TableDef)
        -> Result<Option<crate::task::rows::Scope>, CoreError> {
        let Some(ctx) = self.task.clone() else { return Ok(None) };
        #[cfg(feature = "engine-rocksdb")]
        if let super::Backend::V2(db) = &self.admin.inner.backend {
            let db = db.clone();
            let txn = self.v2_transaction.as_mut().ok_or(CoreError::Sql(SqlError::Transaction))?;
            let scope = crate::task::rows::scope_at(&db, txn, &ctx)?;
            if scope.table() != table.id {
                return Err(TaskError::Denied(crate::task::DenyRule::OtherTable).into());
            }
            return Ok(Some(scope));
        }
        let _ = (ctx, table);
        Err(TaskError::Revoked.into())
    }

    /// Visit a sorted key batch in one statement snapshot and task scope.
    /// In a task session a row outside the visible rows is not visited.
    fn visit_native_keys<E>(
        &mut self, table: &TableDef, keys: &[Vec<crate::relational::Value>],
        visit: &mut impl FnMut(&SqlRow) -> Result<(), E>,
        map: fn(CoreError) -> E,
    ) -> Result<(), E> {
        #[cfg(feature = "engine-rocksdb")]
        if let super::Backend::V2(db) = &self.admin.inner.backend {
            let db = db.clone();
            let snapshot = self.complete_snapshot(table).map_err(map)?;
            let scope = self.native_task_scope(table).map_err(map)?;
            let meter = StatementMeter::default();
            let visited: Result<(), E> = (|| {
                for key in keys {
                    if scope.as_ref().is_some_and(|scope| crate::relational::encode_key(key).is_ok_and(|key| !scope.contains_key(&key))) { continue; }
                    self.visit_native_key_v2(&db, table, key,
                        &mut scoped(scope.as_ref(), &meter, visit, map), map)?;
                }
                Ok(())
            })();
            if scope.is_some() {
                crate::task::rows::record(meter.counts());
                self.note_task_rows_read(&meter);
            }
            visited?;
            if self.complete_snapshot(table).map_err(map)? != snapshot {
                return Err(map(CoreError::Sql(SqlError::StaleSchema)));
            }
            return Ok(());
        }
        let _ = (table, keys, visit);
        Err(map(CoreError::Sql(SqlError::Unsupported)))
    }

    /// Stream one statement snapshot without asserting full-table boundedness.
    /// In a task session only visible rows are visited; every row pulled is
    /// counted (R3-306).
    pub(crate) fn visit_native_page<E>(
        &mut self, table: &TableDef,
        visit: &mut impl FnMut(&SqlRow) -> Result<(), E>,
        map: fn(CoreError) -> E,
    ) -> Result<(), E> {
        self.visit_native_range(table, std::ops::Bound::Unbounded, std::ops::Bound::Unbounded, visit, map)
    }

    fn visit_native_range<E>(
        &mut self, table: &TableDef,
        lo: std::ops::Bound<Vec<u8>>, hi: std::ops::Bound<Vec<u8>>,
        visit: &mut impl FnMut(&SqlRow) -> Result<(), E>, map: fn(CoreError) -> E,
    ) -> Result<(), E> {
        self.visit_native_range_inner(table, lo, hi, None, visit, map)
    }

    #[allow(clippy::too_many_arguments)]
    fn visit_native_range_inner<E>(
        &mut self, table: &TableDef,
        lo: std::ops::Bound<Vec<u8>>, hi: std::ops::Bound<Vec<u8>>,
        index: Option<&crate::relational::secondary_index::Descriptor>,
        visit: &mut impl FnMut(&SqlRow) -> Result<(), E>, map: fn(CoreError) -> E,
    ) -> Result<(), E> {
        let snapshot = self.complete_snapshot(table).map_err(map)?;
        let scope = self.native_task_scope(table).map_err(map)?;
        let meter = StatementMeter::default();
        let visit = &mut scoped(scope.as_ref(), &meter, visit, map);
        #[cfg(feature = "engine-rocksdb")]
        let visited = if let super::Backend::V2(db) = &self.admin.inner.backend {
            let db = db.clone();
            if let Some(range) = scope.as_ref().and_then(|scope| scope.key_range()) {
                let range = if index.is_some() { range.clone() } else {
                    crate::task::rows::intersect(range, &crate::relational::scan::KeyRange { lo, hi })
                };
                if range.is_empty() { Ok(()) } else {
                    self.visit_native_range_v2(&db, table, range.lo, range.hi, visit, map)
                }
            } else if let Some(index) = index {
                self.visit_native_index_v2(&db, table, (index, lo, hi), visit, map)
            } else { self.visit_native_range_v2(&db, table, lo, hi, visit, map) }
        } else {
            self.visit_native_page_legacy(table, visit, map)
        };
        #[cfg(not(feature = "engine-rocksdb"))]
        let _ = (lo, hi, index);
        #[cfg(not(feature = "engine-rocksdb"))]
        let visited = self.visit_native_page_legacy(table, visit, map);
        if scope.is_some() {
            crate::task::rows::record(meter.counts());
            self.note_task_rows_read(&meter);
        }
        visited?;
        if self.complete_snapshot(table).map_err(map)? != snapshot {
            return Err(map(CoreError::Sql(SqlError::StaleSchema)));
        }
        Ok(())
    }

    fn visit_native_page_legacy<E>(
        &self, table: &TableDef, visit: &mut impl FnMut(&SqlRow) -> Result<(), E>,
        map: fn(CoreError) -> E,
    ) -> Result<(), E> {
        use crate::relational::scan::{Direction, KeyRange, LegacyRows, RowSource};
        let state = self.staged.as_ref().ok_or_else(|| map(CoreError::Sql(SqlError::Transaction)))?;
        let source = LegacyRows(&state.sql);
        let range = KeyRange { lo: std::ops::Bound::Unbounded, hi: std::ops::Bound::Unbounded };
        for row in source.scan(table, &range, Direction::Forward).map_err(|e| map(CoreError::Sql(e)))? {
            crate::cancellation::check().map_err(map)?;
            visit(&row.map_err(|e| map(CoreError::Sql(e)))?.row)?;
        }
        Ok(())
    }

    fn complete_snapshot(&mut self, table: &TableDef) -> Result<Snapshot, CoreError> {
        if self.aborted {
            return Err(CoreError::Sql(SqlError::Aborted));
        }
        if self.is_v2_engine() {
            #[cfg(feature = "engine-rocksdb")]
            self.v2_transaction.as_mut()
                .ok_or(CoreError::Sql(SqlError::Transaction))?.snapshot_ts();
            let (domain, epoch) = self.snapshot_identity_engine()?;
            let transaction = self.transaction_id.ok_or(CoreError::Sql(SqlError::Transaction))?;
            if !self.catalog_engine()?.1.contains(table) {
                return Err(CoreError::Sql(SqlError::StaleSchema));
            }
            if table.hidden_rowid().is_some() {
                return Err(CoreError::Sql(SqlError::Unsupported));
            }
            return Ok(Snapshot { domain, epoch, transaction,
                revision: self.staged_revision,
                read_timestamp: self.v2_transaction.as_ref()
                    .and_then(|txn| txn.current_snapshot_ts()).map(|ts| ts.0),
                table: table.clone() });
        }
        let staged = self
            .staged
            .as_ref()
            .ok_or(CoreError::Sql(SqlError::Transaction))?;
        let transaction = self
            .transaction_id
            .ok_or(CoreError::Sql(SqlError::Transaction))?;
        crate::cancellation::check()?;
        if staged.sql.tables.get(&staged.sql.table_key(table)) != Some(table) {
            return Err(CoreError::Sql(SqlError::StaleSchema));
        }
        if table.hidden_rowid().is_some() {
            return Err(CoreError::Sql(SqlError::Unsupported));
        }
        Ok(Snapshot {
            domain: staged.view.domain,
            epoch: staged.sql.epoch,
            transaction,
            revision: self.staged_revision,
            read_timestamp: None,
            table: table.clone(),
        })
    }

    pub(crate) fn complete_table_engine(
        &mut self,
        table: &TableDef,
    ) -> Result<CompleteRelation, CoreError> {
        let result = (|| {
            let snapshot = self.complete_snapshot(table)?;
            let scope = self.native_task_scope(table)?;
            let key = self.inspect_metadata(|s| Ok(s.sql.table_key(table)))?;
            let rows = self
                .scan_engine(Scan {
                    table: key.clone(),
                    projection: vec![],
                    prefix: scope.as_ref().map_or_else(Vec::new, |scope| scope.prefix_values(table)),
                    lower: None,
                    upper: None,
                    reverse: false,
                    limit: MAX_ROWS,
                })?
                .rows;
            if rows.len() == MAX_ROWS {
                // The page bound is not a table capacity. Prove that the final
                // full primary key equals this same snapshot's actual maximum.
                let maximum = self.scan_engine(Scan {
                    table: key,
                    projection: table
                        .primary_key
                        .iter()
                        .map(|index| table.columns[*index].name.clone())
                        .collect(),
                    prefix: scope.as_ref().map_or_else(Vec::new, |scope| scope.prefix_values(table)),
                    lower: None,
                    upper: None,
                    reverse: true,
                    limit: 1,
                })?;
                let last = rows.last().ok_or(CoreError::Limit)?;
                let last_key: Vec<_> = table
                    .primary_key
                    .iter()
                    .map(|index| last.values[*index].clone())
                    .collect();
                if maximum.rows.len() != 1 || maximum.rows[0].values != last_key {
                    return Err(CoreError::Limit);
                }
            }
            if self.complete_snapshot(table)? != snapshot {
                return Err(CoreError::Sql(SqlError::StaleSchema));
            }
            // R3-306: a task session's complete input is its visible rows.
            let rows = match &scope {
                None => rows,
                Some(scope) => {
                    let meter = StatementMeter::default();
                    let mut kept = Vec::new();
                    let mut stopped = None;
                    for row in rows {
                        match scope.keep(&row, &meter) {
                            Ok(true) => kept.push(row),
                            Ok(false) => {}
                            Err(error) => {
                                stopped = Some(error);
                                break;
                            }
                        }
                    }
                    crate::task::rows::record(meter.counts());
                    self.note_task_rows_read(&meter);
                    if let Some(error) = stopped {
                        return Err(error.into());
                    }
                    kept
                }
            };
            Ok(CompleteRelation { snapshot, rows })
        })();
        if result.is_err() {
            self.abort();
        }
        result
    }

    pub(crate) fn consume_complete_engine(
        &mut self,
        relation: CompleteRelation,
        table: &TableDef,
    ) -> Result<Vec<SqlRow>, CoreError> {
        let result = (|| {
            if self.complete_snapshot(table)? != relation.snapshot {
                return Err(CoreError::Sql(SqlError::StaleSchema));
            }
            Ok(relation.rows)
        })();
        if result.is_err() {
            self.abort();
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Admin, Cancellation, Limits,
        relational::{
            MAX_RESULT_BYTES, MAX_ROW_BYTES, Prepared, Value as V, with_sql_parser_forbidden,
        },
    };

    fn fixture(count: usize, note_bytes: usize) -> (Admin, SqlSession, TableDef) {
        let root = crate::test_artifact_root();
        std::fs::create_dir_all(&root).unwrap();
        let mut token = [0u8; 16];
        getrandom::fill(&mut token).unwrap();
        let path = root.join(format!(
            "complete-relation-{:032x}",
            u128::from_be_bytes(token)
        ));
        let admin = Admin::create(path, Limits::default()).unwrap();
        let mut session = admin.sql_session();
        session
            .execute(
                "CREATE TABLE complete_items (tenant BIGINT, id BIGINT, note TEXT NOT NULL, PRIMARY KEY (tenant,id))",
                &[],
                None,
            )
            .unwrap();
        let table = admin.catalog().unwrap().1.remove(0);
        // Seed boundary fixtures in one trusted storage transaction. Tests below
        // exercise session writes separately, including revision invalidation.
        if count != 0 {
            admin
                .inner
                .change(|state| {
                    for id in 0..count {
                        let prepared = Prepared::insert_engine(
                            &table,
                            vec![
                                V::Int(7),
                                V::Int(id as i64),
                                V::Text("x".repeat(note_bytes)),
                            ],
                            state.view.domain,
                            &state.sql,
                        )
                        .map_err(CoreError::Sql)?;
                        state
                            .sql
                            .execute(&prepared, &[], None)
                            .map_err(CoreError::Sql)?;
                    }
                    Ok(())
                })
                .unwrap();
        }
        (admin, session, table)
    }

    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn ordered_native_read_spills_matching_input_and_releases_runs() {
        use crate::operations::{Bindings, Engine, Request, Sort, ValueExpr};
        use crate::relational::spill;
        use crate::storage::{Backend, engine::catalog::TableEntry};

        const COUNT: usize = 18_000;
        const LIMIT: usize = 10_001;
        let mut token = [0; 16];
        getrandom::fill(&mut token).unwrap();
        let path = crate::test_artifact_root()
            .join(format!("native-sort-{:032x}", u128::from_be_bytes(token),));
        let spill_dir = path.join("spills");
        std::fs::create_dir_all(&spill_dir).unwrap();
        let admin =
            Admin::create_engine(path.join("db"), Limits::default(), crate::Engine::V2).unwrap();
        let mut session = admin.sql_session();
        session
            .execute(
                "CREATE TABLE items (id BIGINT PRIMARY KEY, note TEXT)",
                &[],
                None,
            )
            .unwrap();
        let table = admin.catalog().unwrap().1.remove(0);
        let entry = TableEntry {
            primary_index_birth: None,
            def: table,
            heap: false,
            columns_ext: Default::default(),
        };
        let Backend::V2(db) = &admin.inner.backend else {
            panic!("v2")
        };
        let note = "x".repeat(4096);
        let row_bytes = retained_bytes(&SqlRow {
            version: 1,
            values: vec![V::Int(0), V::Text(note.clone())],
        });
        assert!(row_bytes * COUNT > crate::relational::scan::MAX_RETAINED_BYTES);
        assert!(row_bytes * LIMIT < crate::relational::scan::MAX_RETAINED_BYTES);
        for start in (0..COUNT).step_by(200) {
            let mut txn = db.begin(crate::storage::Isolation::ReadCommitted);
            for id in start..(start + 200).min(COUNT) {
                db.rows
                    .insert(
                        &mut txn,
                        &entry,
                        vec![V::Int(id as i64), V::Text(note.clone())],
                        None,
                        &|| false,
                    )
                    .unwrap();
            }
            txn.commit().unwrap();
        }
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                spill::set_thread_dir(None);
            }
        }
        let _reset = Reset;
        spill::set_thread_dir(Some(spill_dir.clone()));
        let engine = Engine::new(admin.clone());
        let request = Request::Read {
            table: "items".into(),
            filter: None,
            order: vec![Sort {
                field: "id".into(),
                descending: true,
            }],
            limit: Some(ValueExpr::Literal(V::Int(LIMIT as i64))),
            projection: vec!["id".into()],
        };
        let result = with_sql_parser_forbidden(|| engine.execute(&request, &Bindings::new())).unwrap();
        assert_eq!(result.matched, COUNT);
        assert!(!result.complete);
        assert_eq!(result.rows.len(), LIMIT);
        for (offset, row) in result.rows.iter().enumerate() {
            assert_eq!(row.values, vec![V::Int((COUNT - 1 - offset) as i64)]);
        }
        assert_eq!(std::fs::read_dir(&spill_dir).unwrap().count(), 0);
        // A readback that exceeds retention must also remove every sorted run.
        let mut all_rows = request.clone();
        let Request::Read { limit, .. } = &mut all_rows else {
            unreachable!()
        };
        *limit = Some(ValueExpr::Literal(V::Int(COUNT as i64)));
        assert_eq!(
            engine.execute(&all_rows, &Bindings::new()),
            Err(crate::operations::Error::Sql {
                sqlstate: "54000".into(),
            })
        );
        assert_eq!(std::fs::read_dir(&spill_dir).unwrap().count(), 0);
        drop(engine);
        drop(session);
        drop(admin);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn native_view_reads_filter_complete_spilled_input_and_clean_up() {
        use crate::operations::{
            Bindings, Compare, Engine, Expr, Projection, Request, Sort, ValueExpr,
        };
        use crate::relational::spill;

        let mut token = [0; 16];
        getrandom::fill(&mut token).unwrap();
        let path = crate::test_artifact_root()
            .join(format!("native-view-{:032x}", u128::from_be_bytes(token)));
        let spill_dir = path.join("spills");
        std::fs::create_dir_all(&spill_dir).unwrap();
        let admin =
            Admin::create_engine(path.join("db"), Limits::default(), crate::Engine::V2).unwrap();
        let mut session = admin.sql_session();
        session
            .execute(
                "CREATE TABLE items (id BIGINT PRIMARY KEY, note TEXT)",
                &[],
                None,
            )
            .unwrap();
        let insert = session.prepare("INSERT INTO items VALUES ($1,$2)").unwrap();
        for start in (0..6000).step_by(100) {
            session.execute("BEGIN", &[], None).unwrap();
            for id in start..start + 100 {
                session
                    .execute_prepared(&insert, &[V::Int(id), V::Text("x".repeat(1000))], None)
                    .unwrap();
            }
            session.execute("COMMIT", &[], None).unwrap();
        }
        session
            .execute(
                "CREATE VIEW item_view AS SELECT id,note FROM items",
                &[],
                None,
            )
            .unwrap();
        session
            .execute("CREATE VIEW small_view AS SELECT id FROM items", &[], None)
            .unwrap();
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                spill::set_thread_dir(None);
            }
        }
        let _reset = Reset;
        spill::set_thread_dir(Some(spill_dir.clone()));
        let engine = Engine::new(admin.clone());
        let filter = Expr::Compare(
            Box::new(Expr::Field("id".into())),
            Compare::Gt,
            Box::new(Expr::Value(ValueExpr::Literal(V::Int(5996)))),
        );
        for (table, filter, limit, matched, complete) in [
            ("item_view", Some(filter.clone()), 5, 3, true),
            ("item_view", None, 5, 6000, false),
            ("small_view", None, 10, 6000, false),
            ("item_view", None, 0, 6000, false),
        ] {
            let request = Request::Read {
                table: table.into(),
                filter,
                order: vec![Sort {
                    field: "id".into(),
                    descending: true,
                }],
                limit: Some(ValueExpr::Literal(V::Int(limit))),
                projection: vec!["id".into()],
            };
            let (result, escaped) = spill::streaming(|| engine.execute(&request, &Bindings::new()));
            assert!(
                escaped.is_none(),
                "only the native projection may reach the outer caller"
            );
            let result = result.unwrap();
            assert_eq!(result.matched, matched);
            assert_eq!(result.complete, complete);
            assert_eq!(result.columns, vec!["id"]);
            assert_eq!(result.rows.len(), (limit as usize).min(matched));
            for (offset, row) in result.rows.iter().enumerate() {
                assert_eq!(row.values, vec![V::Int(5999 - offset as i64)]);
            }
            assert_eq!(std::fs::read_dir(&spill_dir).unwrap().count(), 0);
        }
        let expressions = Request::ReadExpressions {
            table: "item_view".into(),
            filter: Some(filter),
            order: vec![Sort {
                field: "id".into(),
                descending: true,
            }],
            limit: Some(ValueExpr::Literal(V::Int(2))),
            projection: vec![Projection {
                expression: Expr::Field("id".into()),
                alias: Some("key".into()),
            }],
        };
        let result = engine.execute(&expressions, &Bindings::new()).unwrap();
        assert_eq!((result.matched, result.complete), (3, false));
        assert_eq!(result.columns, vec!["key"]);
        assert_eq!(
            result
                .rows
                .iter()
                .map(|r| r.values.clone())
                .collect::<Vec<_>>(),
            vec![vec![V::Int(5999)], vec![V::Int(5998)]]
        );
        // Input may spill, but a native operation's projected reply remains bounded.
        assert_eq!(
            engine
                .execute(
                    &Request::Read {
                        table: "item_view".into(),
                        filter: None,
                        order: vec![],
                        limit: Some(ValueExpr::Literal(V::Int(6000))),
                        projection: vec!["note".into()],
                    },
                    &Bindings::new()
                ),
            Err(crate::operations::Error::Limit)
        );
        assert_eq!(std::fs::read_dir(&spill_dir).unwrap().count(), 0);
        drop(engine);

        // Fail after expansion has spilled, both on a filter error and on cancellation.
        for cancel in [false, true] {
            session.begin_engine().unwrap();
            let view = session.native_view_engine("item_view").unwrap().unwrap();
            let cancellation = Cancellation::default();
            let mut visited = 0;
            let result = cancellation.scope(|| {
                session.native_view_rows_where(
                    &view,
                    &mut |_| {
                        visited += 1;
                        assert_ne!(std::fs::read_dir(&spill_dir).unwrap().count(), 0);
                        if cancel {
                            cancellation.cancel();
                            Ok(true)
                        } else {
                            Err(CoreError::Sql(SqlError::UndefinedFunction))
                        }
                    },
                    (5, None::<fn(&SqlRow, &SqlRow) -> std::cmp::Ordering>),
                    |e| e,
                )
            });
            assert!(visited > 0);
            assert!(matches!(result, Err(CoreError::Cancelled)) == cancel);
            if !cancel {
                assert!(matches!(
                    result,
                    Err(CoreError::Sql(SqlError::UndefinedFunction))
                ));
            }
            assert_eq!(std::fs::read_dir(&spill_dir).unwrap().count(), 0);
            session.rollback();
        }
        drop(session);
        drop(admin);
        std::fs::remove_dir_all(path).unwrap();
    }

    fn page(session: &mut SqlSession, table: &TableDef) -> Vec<SqlRow> {
        session
            .scan(Scan {
                table: table.name.clone(),
                projection: vec![],
                prefix: vec![],
                lower: None,
                upper: None,
                reverse: false,
                limit: MAX_ROWS,
            })
            .unwrap()
            .rows
    }

    #[test]
    fn heap_complete_relations_and_typed_session_wrappers_are_unsupported() {
        let (_, mut session, keyed) = fixture(0, 1);
        session
            .execute("CREATE TABLE heap_items (note TEXT)", &[], None)
            .unwrap();
        let heap = session
            .catalog_engine()
            .unwrap()
            .1
            .into_iter()
            .find(|table| table.name == "heap_items")
            .unwrap();
        with_sql_parser_forbidden(|| {
            session.begin_engine().unwrap();
            let complete = session.complete_table_engine(&keyed).unwrap();
            assert!(
                session
                    .consume_complete_engine(complete, &keyed)
                    .unwrap()
                    .is_empty()
            );
            assert!(matches!(
                session.complete_table_engine(&heap),
                Err(CoreError::Sql(SqlError::Unsupported))
            ));
            assert!(session.is_aborted());
            session.rollback();
            for operation in 0..4 {
                session.begin_engine().unwrap();
                let result = match operation {
                    0 => session.insert_engine(&heap, vec![]).map(|_| ()),
                    1 => session.update_engine(&heap, vec![], vec![], 1).map(|_| ()),
                    2 => session.delete_engine(&heap, vec![], 1).map(|_| ()),
                    _ => session.row_version_engine(&heap, &[]).map(|_| ()),
                };
                assert_eq!(result, Err(CoreError::Sql(SqlError::Unsupported)));
                session.rollback();
            }
            session.begin_engine().unwrap();
            session
                .insert_engine(&keyed, vec![V::Int(7), V::Int(1), V::Text("one".into())])
                .unwrap();
            let key = vec![V::Int(7), V::Int(1)];
            let version = session.row_version_engine(&keyed, &key).unwrap();
            session
                .update_engine(
                    &keyed,
                    key.clone(),
                    vec![(2, V::Text("two".into()))],
                    version,
                )
                .unwrap();
            let version = session.row_version_engine(&keyed, &key).unwrap();
            session.delete_engine(&keyed, key, version).unwrap();
            session.rollback();
        });
    }

    #[test]
    fn empty_and_exactly_full_relations_are_complete_but_a_page_is_not_a_proof() {
        for count in [0, MAX_ROWS, MAX_ROWS + 1] {
            let (admin, mut session, table) = fixture(count, 1);
            let before = admin.inner.inspect(|state| Ok(state.sql.clone())).unwrap();
            with_sql_parser_forbidden(|| {
                session.begin_engine().unwrap();
                // Scan remains a paged API even when the table has 1025 rows.
                assert_eq!(page(&mut session, &table).len(), count.min(MAX_ROWS));
                let complete = session.complete_table_engine(&table);
                if count > MAX_ROWS {
                    assert!(matches!(complete, Err(CoreError::Limit)));
                    assert!(session.is_aborted());
                } else {
                    let rows = session
                        .consume_complete_engine(complete.unwrap(), &table)
                        .unwrap();
                    assert_eq!(rows.len(), count);
                    if count != 0 {
                        // All rows share their first key component. Comparing
                        // only that component would incorrectly accept 1025.
                        assert_eq!(rows.last().unwrap().values[1], V::Int((count - 1) as i64));
                    }
                }
                session.rollback();
            });
            assert_eq!(
                admin.inner.inspect(|state| Ok(state.sql.clone())).unwrap(),
                before
            );
        }
    }

    #[test]
    fn result_byte_overflow_aborts_without_changing_rows_or_versions() {
        let count = MAX_RESULT_BYTES / MAX_ROW_BYTES + 1;
        let (admin, mut session, table) = fixture(count, MAX_ROW_BYTES - 18);
        let before = admin.inner.inspect(|state| Ok(state.sql.clone())).unwrap();
        with_sql_parser_forbidden(|| {
            session.begin_engine().unwrap();
            assert!(matches!(
                session.complete_table_engine(&table),
                Err(CoreError::Sql(SqlError::Limit))
            ));
            assert!(session.is_aborted());
            session.rollback();
        });
        assert_eq!(
            admin.inner.inspect(|state| Ok(state.sql.clone())).unwrap(),
            before
        );
    }

    #[test]
    fn completeness_requires_an_active_nonaborted_transaction_and_exact_descriptor() {
        let (_, mut session, table) = fixture(1, 1);
        assert!(matches!(
            session.complete_table_engine(&table),
            Err(CoreError::Sql(SqlError::Transaction))
        ));
        session.begin_engine().unwrap();
        let relation = session.complete_table_engine(&table).unwrap();
        session.abort();
        assert!(matches!(
            session.complete_table_engine(&table),
            Err(CoreError::Sql(SqlError::Aborted))
        ));
        assert_eq!(
            session.consume_complete_engine(relation, &table),
            Err(CoreError::Sql(SqlError::Aborted))
        );
        session.rollback();

        session.begin_engine().unwrap();
        let mut malformed = table.clone();
        malformed.primary_key = vec![usize::MAX];
        assert!(matches!(
            session.complete_table_engine(&malformed),
            Err(CoreError::Sql(SqlError::StaleSchema))
        ));
        assert!(session.is_aborted());
        session.rollback();

        session
            .execute(
                "CREATE TABLE other_items (id BIGINT PRIMARY KEY)",
                &[],
                None,
            )
            .unwrap();
        session.begin_engine().unwrap();
        let other = session
            .catalog_engine()
            .unwrap()
            .1
            .into_iter()
            .find(|table| table.name == "other_items")
            .unwrap();
        let relation = session.complete_table_engine(&table).unwrap();
        assert_eq!(
            session.consume_complete_engine(relation, &other),
            Err(CoreError::Sql(SqlError::StaleSchema))
        );
        assert!(session.is_aborted());
    }

    #[test]
    fn complete_relations_expire_on_transaction_exit_and_cannot_cross_sessions() {
        let (admin, mut session, table) = fixture(1, 1);
        for commit in [false, true] {
            session.begin_engine().unwrap();
            let relation = session.complete_table_engine(&table).unwrap();
            if commit {
                session.commit_engine().unwrap();
            } else {
                session.rollback();
            }
            session.begin_engine().unwrap();
            assert_eq!(
                session.consume_complete_engine(relation, &table),
                Err(CoreError::Sql(SqlError::StaleSchema))
            );
            session.rollback();
        }

        session.begin_engine().unwrap();
        let relation = session.complete_table_engine(&table).unwrap();
        let mut other = admin.sql_session();
        other.begin_engine().unwrap();
        assert_eq!(
            other.consume_complete_engine(relation, &table),
            Err(CoreError::Sql(SqlError::StaleSchema))
        );
        assert!(other.is_aborted());
        assert!(!session.is_aborted());

        let relation = session.complete_table_engine(&table).unwrap();
        session.rollback();
        assert_eq!(
            session.consume_complete_engine(relation, &table),
            Err(CoreError::Sql(SqlError::Transaction))
        );
    }

    #[test]
    fn successful_sql_writes_and_ddl_invalidate_prior_materializations() {
        let (admin, mut session, table) = fixture(1, 1);
        let before = admin.inner.inspect(|state| Ok(state.sql.clone())).unwrap();
        for statement in [
            "INSERT INTO complete_items VALUES (7,1,'new')",
            "UPDATE complete_items SET note='updated' WHERE tenant=7 AND id=0",
            "UPDATE complete_items SET note='x' WHERE tenant=7 AND id=0",
            "DELETE FROM complete_items WHERE tenant=7 AND id=0",
            "CREATE TABLE other_items (id BIGINT PRIMARY KEY)",
        ] {
            session.begin_engine().unwrap();
            let relation = session.complete_table_engine(&table).unwrap();
            session.execute(statement, &[], None).unwrap();
            assert_eq!(
                session.consume_complete_engine(relation, &table),
                Err(CoreError::Sql(SqlError::StaleSchema)),
                "{statement}"
            );
            assert!(session.is_aborted());
            session.rollback();
        }
        assert_eq!(
            admin.inner.inspect(|state| Ok(state.sql.clone())).unwrap(),
            before
        );
    }

    #[test]
    fn reads_preserve_materializations_but_typed_writes_invalidate_them() {
        let (_, mut session, table) = fixture(1, 1);
        with_sql_parser_forbidden(|| {
            session.begin_engine().unwrap();
            let relation = session.complete_table_engine(&table).unwrap();
            assert_eq!(page(&mut session, &table).len(), 1);
            assert_eq!(session.catalog_engine().unwrap().1, vec![table.clone()]);
            assert_eq!(
                session
                    .consume_complete_engine(relation, &table)
                    .unwrap()
                    .len(),
                1
            );
            let relation = session.complete_table_engine(&table).unwrap();
            session
                .insert_engine(&table, vec![V::Int(7), V::Int(1), V::Text("new".into())])
                .unwrap();
            assert_eq!(
                session.consume_complete_engine(relation, &table),
                Err(CoreError::Sql(SqlError::StaleSchema))
            );
        });
    }

    #[test]
    fn cancellation_rejects_production_and_consumption_of_complete_relations() {
        let (admin, mut session, table) = fixture(1, 1);
        let before = admin.inner.inspect(|state| Ok(state.sql.clone())).unwrap();
        let cancelled = Cancellation::default();
        assert!(cancelled.cancel());
        session.begin_engine().unwrap();
        assert!(matches!(
            cancelled.scope(|| session.complete_table_engine(&table)),
            Err(CoreError::Cancelled)
        ));
        assert!(session.is_aborted());
        session.rollback();

        session.begin_engine().unwrap();
        let relation = session.complete_table_engine(&table).unwrap();
        assert_eq!(
            cancelled.scope(|| session.consume_complete_engine(relation, &table)),
            Err(CoreError::Cancelled)
        );
        assert!(session.is_aborted());
        session.rollback();
        assert_eq!(
            admin.inner.inspect(|state| Ok(state.sql.clone())).unwrap(),
            before
        );
    }

    #[test]
    fn completeness_uses_the_transaction_snapshot_despite_concurrent_commits() {
        let (admin, mut session, table) = fixture(MAX_ROWS, 1);
        session.begin_engine().unwrap();
        let concurrent = admin.clone();
        std::thread::spawn(move || {
            concurrent
                .sql_session()
                .execute(
                    "INSERT INTO complete_items VALUES (7,1024,'concurrent')",
                    &[],
                    None,
                )
                .unwrap();
        })
        .join()
        .unwrap();
        with_sql_parser_forbidden(|| {
            let relation = session.complete_table_engine(&table).unwrap();
            assert_eq!(
                session
                    .consume_complete_engine(relation, &table)
                    .unwrap()
                    .len(),
                MAX_ROWS
            );
            assert_eq!(
                session.commit_engine(),
                Err(CoreError::Sql(SqlError::Conflict))
            );
            session.rollback();
            session.begin_engine().unwrap();
            assert!(matches!(
                session.complete_table_engine(&table),
                Err(CoreError::Limit)
            ));
            assert!(session.is_aborted());
        });
    }

    #[test]
    fn revision_overflow_aborts_before_accepting_a_stage_and_resets_on_rollback() {
        let (admin, mut session, table) = fixture(1, 1);
        session.begin_engine().unwrap();
        session.staged_revision = u64::MAX;
        let before = session.staged.as_ref().unwrap().sql.clone();
        assert_eq!(
            session.insert_engine(&table, vec![V::Int(7), V::Int(1), V::Text("new".into())]),
            Err(CoreError::Sql(SqlError::Overflow))
        );
        assert!(session.is_aborted());
        assert_eq!(session.staged.as_ref().unwrap().sql, before);
        session.rollback();
        assert_eq!(session.staged_revision, 0);
        session.begin_engine().unwrap();
        assert_eq!(session.staged_revision, 0);
        assert_eq!(page(&mut session, &table).len(), 1);
        assert_eq!(
            admin.inner.inspect(|state| Ok(state.sql.clone())).unwrap(),
            before
        );
    }
}
