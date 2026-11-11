//! Bounded pages from the statement transaction, including its write overlay.
use super::*;
use relational::scan::{self, RowSource};
use std::{borrow::Cow, cell::RefCell, collections::VecDeque};

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
}
impl RowSource for Source<'_> {
    fn streams_reductions(&self) -> bool { true }
    fn scan<'s>(
        &'s self,
        table: &TableDef,
        range: &scan::KeyRange,
        dir: scan::Direction,
    ) -> Result<scan::SourceRows<'s>, SqlError> {
        if range.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }
        let entry = TableEntry {
            def: table.clone(),
            heap: self.rows.borrow().parts.heap.contains(&table.id),
            columns_ext: BTreeMap::new(),
        };
        let index = self.rows.borrow().scan_index(table);
        let mut range = range.clone();
        if range.lo == Bound::Unbounded && range.hi == Bound::Unbounded {
            let rows = self.rows.borrow();
            if table.primary_key.iter().all(|i| {
                rows.parts
                    .ext
                    .columns
                    .get(&table.columns[*i].id)
                    .is_none_or(|c| c.type_id.is_none())
            }) && let Some(primary) =
                relational::row_sink::reduction_range(table, rows.prepared, rows.params)
            {
                range = primary;
            }
        }
        if range.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }
        let (mut lo, mut hi) = (range.lo, range.hi);
        let mut after = None;
        let mut page = VecDeque::new();
        let mut done = false;
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
                        &mut accept,
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
                        Some(200),
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
            let row = page.pop_front();
            if row.is_none() {
                done = true;
            }
            row.map(Ok)
        })))
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
        Ok(result.map(|(key, row)| scan::SourceRow {
            key: Cow::Owned(key),
            row: Cow::Owned(SqlRow {
                values: row.values,
                version: row.version,
            }),
        }))
    }
}
