//! Bounded pages from the statement transaction, including its write overlay.
use super::*;
use relational::scan::{self, RowSource};
use std::{borrow::Cow, cell::RefCell, collections::VecDeque};

#[cfg(test)]
thread_local! { pub(super) static SCANNED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

pub(super) struct Source<'a> {
    rows: RefCell<SqlRows<'a>>,
    pub(super) error: RefCell<Option<CoreError>>,
}
impl<'a> Source<'a> {
    pub(super) fn new(rows: SqlRows<'a>) -> Self {
        Self {
            rows: RefCell::new(rows),
            error: RefCell::new(None),
        }
    }
    fn error(&self, error: CoreError) -> SqlError {
        *self.error.borrow_mut() = Some(error);
        SqlError::Unsupported
    }
    #[cfg(test)]
    fn observed(&self, table: &TableDef) {
        AFTER_TABLE_SCAN.with_borrow_mut(|hook| {
            if let Some(hook) = hook { hook(&table.name); }
        });
    }
}
impl Source<'_> {
    fn pages<'s>(
        &'s self,
        table: &TableDef,
        range: &scan::KeyRange,
        dir: scan::Direction,
        index: Option<IndexRange>,
        page_rows: usize,
    ) -> Result<scan::SourceRows<'s>, SqlError> {
        if range.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }
        let entry = TableEntry {
            def: table.clone(),
            heap: self.rows.borrow().parts.heap.contains(&table.id),
            columns_ext: BTreeMap::new(),
        };
        let (mut lo, mut hi) = (range.lo.clone(), range.hi.clone());
        let mut after = None;
        let mut page = VecDeque::new();
        let mut done = false;
        #[cfg(test)]
        let mut observed = false;
        Ok(Box::new(std::iter::from_fn(move || {
            if let Some(row) = page.pop_front() {
                return Some(Ok(row));
            }
            if done {
                return None;
            }
            let loaded = (|| -> Result<(), CoreError> {
                crate::cancellation::check()?;
                let mut rows = self.rows.borrow_mut();
                let mut retention = scan::Retention::new(scan::retention_limit());
                let mut accept = |key: Vec<u8>, row: engine::rows::Row| {
                    #[cfg(test)]
                    SCANNED.update(|n| n + 1);
                    let row = SqlRow {
                        values: row.values,
                        version: row.version,
                    };
                    retention
                        .charge(
                            relational::row_sink::retained_bytes(&key, &row)
                                .map_err(CoreError::Sql)?,
                        )
                        .map_err(CoreError::Sql)?;
                    page.push_back(scan::SourceRow {
                        key: Cow::Owned(key),
                        row: Cow::Owned(row),
                    });
                    Ok(())
                };
                if let Some((index, lower, upper)) = &index {
                    after = engine::indexes::visit_page(
                        rows.txn,
                        &entry,
                        index,
                        lower.clone(),
                        upper.clone(),
                        after.take(),
                        &mut accept, page_rows,
                    )?;
                    done = after.is_none();
                    #[cfg(test)]
                    USED_INDEX_SCAN.set(true);
                } else {
                    let direction = match dir {
                        scan::Direction::Forward => engine::kv::Direction::Forward,
                        scan::Direction::Reverse => engine::kv::Direction::Reverse,
                    };
                    let db = rows.db;
                    for pair in db.rows.scan(
                        rows.txn,
                        &entry,
                        lo.clone(),
                        hi.clone(),
                        direction,
                        Some(page_rows),
                    )? {
                        let (key, row) = pair?;
                        match dir {
                            scan::Direction::Forward => lo = Bound::Excluded(key.clone()),
                            scan::Direction::Reverse => hi = Bound::Excluded(key.clone()),
                        }
                        accept(key, row)?;
                    }
                }
                Ok(())
            })();
            if let Err(error) = loaded {
                done = true;
                page.clear();
                return Some(Err(self.error(error)));
            }
            #[cfg(test)]
            if !observed { observed = true; self.observed(&entry.def); }
            let row = page.pop_front();
            if row.is_none() {
                done = true;
            }
            row.map(Ok)
        })))
    }
}
impl RowSource for Source<'_> {
    fn streams_reductions(&self) -> bool { true }
    fn scan<'s>(&'s self, table: &TableDef, range: &scan::KeyRange, dir: scan::Direction)
        -> Result<scan::SourceRows<'s>, SqlError> {
        let rows = self.rows.borrow();
        let index = rows.scan_index(table);
        let mut range = range.clone();
        if range.lo == Bound::Unbounded && range.hi == Bound::Unbounded
            && table.primary_key.iter().all(|i| rows.parts.ext.columns
                .get(&table.columns[*i].id).is_none_or(|c| c.type_id.is_none()))
            && let Some(primary) = relational::row_sink::reduction_range(table, rows.prepared, rows.params) {
            range = primary;
        }
        drop(rows);
        self.pages(table, &range, dir, index, 200)
    }
    fn index_input<'s>(&'s self, table: &TableDef, predicates: &[(usize, &str, relational::Value)])
        -> Result<Option<scan::SourceRows<'s>>, SqlError> {
        let predicates = predicates.iter().map(|(i, op, value)|
            (table.columns[*i].name.clone(), (*op).to_owned(), value.clone())).collect::<Vec<_>>();
        let index = self.rows.borrow().scan_index_predicates(table, &predicates);
        index.map(|index| self.pages(table, &scan::KeyRange { lo: Bound::Unbounded, hi: Bound::Unbounded },
            scan::Direction::Forward, Some(index), 200)).transpose()
    }
    fn join_probe<'s>(&'s self, table: &TableDef, keys: &[(usize, Option<relational::Value>)], first: bool)
        -> Result<Option<scan::SourceRows<'s>>, SqlError> {
        let page_rows = if first { 1 } else { 200 };
        let rows = self.rows.borrow();
        let primary = table.primary_key.iter().map(|i| table.columns[*i].id).collect::<Vec<_>>();
        let prefix = |ids: &[u64]| ids.iter().take_while(|id| keys.iter().any(|(i, _)|
            table.columns[*i].id == **id && rows.parts.ext.columns.get(id).is_none_or(|c| c.type_id.is_none())))
            .copied().collect::<Vec<_>>();
        let mut ids = prefix(&primary);
        let mut index = None;
        for descriptor in rows.parts.indexes.iter().filter(|d| d.table == table.id) {
            let candidate = prefix(&descriptor.columns);
            if candidate.len() > ids.len() { ids = candidate; index = Some(descriptor.clone()); }
        }
        if ids.is_empty() { return Ok(None); }
        let values = ids.iter().map(|id| keys.iter().find(|(i, _)| table.columns[*i].id == *id)
            .unwrap().1.clone()).collect::<Option<Vec<_>>>();
        drop(rows);
        let Some(values) = values else { return Ok(Some(Box::new(std::iter::empty()))); };
        // Combined outer keys can exceed one row's key budget; retain the scan fallback.
        let range = if let Some(descriptor) = index.as_ref() {
            let Ok(tuple) = codec::index_tuple_bytes(&values, &vec![codec::Direction::Asc; values.len()])
                else { return Ok(None); };
            return self.pages(table, &scan::KeyRange { lo: Bound::Unbounded, hi: Bound::Unbounded },
                scan::Direction::Forward, Some((descriptor.clone(), Bound::Included(tuple.clone()), Bound::Included(tuple))), page_rows).map(Some);
        } else {
            let Ok(prefix) = codec::pk_bytes(&values) else { return Ok(None); };
            scan::KeyRange { lo: Bound::Included(prefix.clone()),
                hi: engine::indexes::successor(prefix).map_or(Bound::Unbounded, Bound::Excluded) }
        };
        self.pages(table, &range, scan::Direction::Forward, None, page_rows).map(Some)
    }
    fn get<'s>(
        &'s self,
        table: &TableDef,
        key: &[u8],
    ) -> Result<Option<scan::SourceRow<'s>>, SqlError> {
        let mut rows = self.rows.borrow_mut();
        let entry = TableEntry {
            def: table.clone(),
            heap: rows.parts.heap.contains(&table.id),
            columns_ext: BTreeMap::new(),
        };
        let db = rows.db;
        let result = db
            .rows
            .scan(
                rows.txn,
                &entry,
                Bound::Included(key.to_vec()),
                Bound::Included(key.to_vec()),
                engine::kv::Direction::Forward,
                Some(1),
            )
            .map_err(|e| self.error(e))?
            .next()
            .transpose()
            .map_err(|e| self.error(e))?;
        drop(rows);
        #[cfg(test)]
        self.observed(table);
        Ok(result.map(|(key, row)| scan::SourceRow {
            key: Cow::Owned(key),
            row: Cow::Owned(SqlRow {
                values: row.values,
                version: row.version,
            }),
        }))
    }
}
