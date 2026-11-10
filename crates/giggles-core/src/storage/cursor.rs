//! Snapshot-backed SQL result cursors. Each fetch retains at most one lookahead row.
use super::*;
use relational::{
    SqlError, SqlRow, Value,
    cursor::Plan,
    scan::{self, RowSource},
};
use std::{borrow::Cow, cell::RefCell, collections::VecDeque, ops::Bound};

pub const DEFAULT_CURSOR_BUFFER_BYTES: usize = 256 * 1024 * 1024;
pub struct Cursor {
    data: Data,
    clock: Option<relational::clock::ExecutionClock>,
}
enum Data {
    Scan {
        image: Box<relational::Relational>,
        source: Option<V2Snapshot>,
        plan: Box<Plan>,
    },
    Buffered(VecDeque<SqlRow>),
}
impl Cursor {
    /// Fetch a bounded chunk. A row larger than max_bytes returns 54000 without
    /// dropping that row. Zero budgets are rejected. Exhaustion is sticky.
    pub fn fetch(&mut self, max_rows: usize, max_bytes: usize) -> Result<Vec<SqlRow>, CoreError> {
        crate::cancellation::check()?;
        if max_rows == 0 || max_bytes == 0 {
            return Err(CoreError::Sql(SqlError::Limit));
        }
        match &mut self.data {
            Data::Buffered(rows) => {
                let mut result = Vec::new();
                let mut bytes = 0usize;
                while result.len() < max_rows {
                    let Some(row) = rows.front() else {
                        break;
                    };
                    let size = row
                        .values
                        .iter()
                        .map(relational::value_bytes)
                        .sum::<usize>();
                    if bytes.saturating_add(size) > max_bytes {
                        if result.is_empty() {
                            return Err(CoreError::Sql(SqlError::Limit));
                        }
                        break;
                    }
                    bytes += size;
                    result.push(rows.pop_front().expect("front exists"));
                }
                Ok(result)
            }
            Data::Scan {
                image,
                source,
                plan,
            } => {
                let legacy = scan::LegacyRows(image);
                let result = plan.fetch(
                    image,
                    source
                        .as_ref()
                        .map_or(&legacy as &dyn RowSource, |s| s as &dyn RowSource),
                    max_rows,
                    max_bytes,
                );
                if let Some(error) = source.as_ref().and_then(|s| s.error.borrow_mut().take()) {
                    return Err(error);
                }
                if let Some(error) = self.clock.as_ref().and_then(|clock| clock.take_error()) {
                    return Err(error);
                }
                result.map_err(CoreError::Sql)
            }
        }
    }
    pub fn is_exhausted(&self) -> bool {
        match &self.data {
            Data::Buffered(rows) => rows.is_empty(),
            Data::Scan { plan, .. } => plan.progress.exhausted(),
        }
    }
}
impl SqlSession {
    pub fn open_cursor(
        &mut self,
        p: &relational::Prepared,
        params: &[Value],
    ) -> Result<Cursor, CoreError> {
        self.open_cursor_inner(p, params, None, DEFAULT_CURSOR_BUFFER_BYTES)
    }
    pub fn open_cursor_at(
        &mut self,
        p: &relational::Prepared,
        params: &[Value],
        clock: &relational::StatementClock,
    ) -> Result<Cursor, CoreError> {
        self.guard_clock(clock)?;
        self.open_cursor_inner(p, params, Some(clock), DEFAULT_CURSOR_BUFFER_BYTES)
    }
    /// Set the independent budget for statement shapes that require buffering.
    pub fn open_cursor_with_buffer_limit(
        &mut self,
        p: &relational::Prepared,
        params: &[Value],
        max_buffer_bytes: usize,
    ) -> Result<Cursor, CoreError> {
        self.open_cursor_inner(p, params, None, max_buffer_bytes)
    }
    fn open_cursor_inner(
        &mut self,
        p: &relational::Prepared,
        params: &[Value],
        clock: Option<&relational::StatementClock>,
        buffer_limit: usize,
    ) -> Result<Cursor, CoreError> {
        relational::assert_sql_frontend_allowed();
        let result = (|| {
            crate::cancellation::check()?;
            if self.aborted {
                return Err(CoreError::Sql(SqlError::Aborted));
            }
            if p.command() != "SELECT" {
                return Err(CoreError::Sql(SqlError::Unsupported));
            }
            if p.transaction.is_some() && p.transaction != self.transaction_id {
                return Err(CoreError::Sql(SqlError::StaleSchema));
            }
            let mut p = p.clone();
            p.set_schema_context(self.search_path.clone(), self.schema_user.clone());
            let context = clock.map(|c| {
                relational::clock::ExecutionClock::new(c)
                    .with_extra_float_digits(self.extra_float_digits)
            });
            let (image, source, context) = match &self.admin.inner.backend {
                Backend::V2(db) => {
                    // The existing transaction API cannot fork its write overlay.
                    // Keep read-your-writes correct via the bounded bridge fallback.
                    if self
                        .v2_transaction
                        .as_ref()
                        .is_some_and(|t| t.writes_keyspace(engine::kv::Keyspace::Rows))
                    {
                        return self.buffer_cursor(&p, params, clock, buffer_limit);
                    }
                    if p.domain != u128::from_be_bytes(db.meta.domain) || p.epoch != 0 {
                        return Err(CoreError::Sql(SqlError::StaleSchema));
                    }
                    let mut implicit = None;
                    let txn = self.v2_transaction.as_mut().unwrap_or_else(|| {
                        implicit.insert(db.begin(engine::txn::Isolation::ReadCommitted))
                    });
                    let parts = db.catalog().parts(txn)?;
                    for name in p.read_tables() {
                        let name = parts.tables.keys().find(|key| **key == name).cloned();
                        if let Some(name) = name {
                            db.catalog()
                                .resolve(
                                    txn,
                                    &name,
                                    engine::locks::LockMode::AccessShare,
                                    None,
                                    &|| crate::cancellation::check().is_err(),
                                )
                                .map_err(|e| match e {
                                    engine::catalog::CatalogError::Core(e) => e,
                                    engine::catalog::CatalogError::Txn(e) => e.into(),
                                })?;
                        }
                    }
                    let guard = db.sequencer.snapshot();
                    txn.begin_statement();
                    let ts = txn.snapshot_ts();
                    let parts = db.catalog().parts(txn)?;
                    let next_id = parts
                        .tables
                        .values()
                        .flat_map(|t| std::iter::once(t.id).chain(t.columns.iter().map(|c| c.id)))
                        .chain(parts.ext.constraints.values().flatten().map(|c| c.id))
                        .chain(parts.ext.sequences.keys().copied())
                        .chain(parts.ext.types.keys().copied())
                        .max()
                        .unwrap_or(0)
                        .checked_add(1)
                        .ok_or(CoreError::Limit)?;
                    let image =
                        relational::Relational::catalog_image(parts.tables, parts.ext, next_id, 1)
                            .map_err(CoreError::Sql)?;
                    // Sequence-bearing selects use the established execution path.
                    if !p.sequences().is_empty() {
                        return self.buffer_cursor(&p, params, clock, buffer_limit);
                    }
                    (
                        image,
                        Some(V2Snapshot {
                            db: db.clone(),
                            _guard: guard,
                            ts,
                            error: RefCell::new(None),
                        }),
                        context,
                    )
                }
                Backend::Legacy(_) => {
                    let s = match &self.staged {
                        Some(s) => s.clone(),
                        None => self.admin.inner.inspect(|s| Ok(s.clone()))?,
                    };
                    if p.domain != s.view.domain {
                        return Err(CoreError::Sql(SqlError::StaleSchema));
                    }
                    // Sequence callbacks publish independently of the row snapshot.
                    let baseline = if self.staged.is_some() {
                        self.sequence_baseline.clone()
                    } else {
                        s.sql
                            .catalog_ext()
                            .sequences
                            .iter()
                            .map(|(&id, d)| (id, d.incarnation))
                            .collect()
                    };
                    let context = context.map(|c| {
                        c.with_sequences(self.admin.inner.sequence_callback(
                            s.clone(),
                            baseline,
                            self.pending_sequences.clone(),
                            self.sequence_values.clone(),
                            self.sequence_tables.clone(),
                            self.search_path.clone(),
                        ))
                    });
                    (s.sql, None, context)
                }
            };
            match Plan::new(&image, &p, params, context.clone()).map_err(CoreError::Sql)? {
                Some(plan) => Ok(Cursor {
                    data: Data::Scan {
                        image: Box::new(image),
                        source,
                        plan: Box::new(plan),
                    },
                    clock: context,
                }),
                None => self.buffer_cursor(&p, params, clock, buffer_limit),
            }
        })();
        if result.is_err() {
            self.abort();
        }
        result
    }
    fn buffer_cursor(
        &mut self,
        p: &relational::Prepared,
        params: &[Value],
        clock: Option<&relational::StatementClock>,
        limit: usize,
    ) -> Result<Cursor, CoreError> {
        let result = scan::with_cursor_limit(limit, || match clock {
            Some(clock) => self.execute_prepared_at(p, params, None, clock),
            None => self.execute_prepared(p, params, None),
        })?;
        Ok(Cursor {
            data: Data::Buffered(result.rows.into()),
            clock: None,
        })
    }
}
struct V2Snapshot {
    db: Arc<v2::V2Db>,
    _guard: engine::commit::SnapshotHandle,
    ts: engine::hlc::Ts,
    error: RefCell<Option<CoreError>>,
}
impl V2Snapshot {
    fn error(&self, error: impl Into<CoreError>) -> SqlError {
        *self.error.borrow_mut() = Some(error.into());
        SqlError::Unsupported
    }
}
impl RowSource for V2Snapshot {
    fn scan<'s>(
        &'s self,
        table: &relational::TableDef,
        range: &scan::KeyRange,
        dir: scan::Direction,
    ) -> Result<Box<dyn Iterator<Item = Result<scan::SourceRow<'s>, SqlError>> + 's>, SqlError>
    {
        if range.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }
        let prefix = table.id.to_be_bytes();
        let prefixed = |key: &Vec<u8>| {
            let mut full = prefix.to_vec();
            full.extend(key);
            full
        };
        let lo = match &range.lo {
            Bound::Unbounded => Bound::Included(prefix.to_vec()),
            bound => bound.as_ref().map(prefixed),
        };
        let hi = match &range.hi {
            Bound::Unbounded => table.id.checked_add(1).map_or(Bound::Unbounded, |id| {
                Bound::Excluded(id.to_be_bytes().to_vec())
            }),
            bound => bound.as_ref().map(prefixed),
        };
        let dir = match dir {
            scan::Direction::Forward => engine::kv::Direction::Forward,
            scan::Direction::Reverse => engine::kv::Direction::Reverse,
        };
        let iter = engine::mvcc::VersionedRange::new(
            self.db.kv.as_ref(),
            engine::kv::Keyspace::Rows,
            lo.as_ref().map(Vec::as_slice),
            hi.as_ref().map(Vec::as_slice),
            dir,
            self.ts,
        )
        .map_err(|e| self.error(e))?;
        let columns = table.columns.len();
        Ok(Box::new(iter.map(move |pair| {
            let (key, value) = pair.map_err(|e| self.error(e))?;
            if !key.starts_with(&prefix) {
                return Err(self.error(CoreError::Corrupt("cursor table prefix".into())));
            }
            let row = engine::codec::decode_row(&key, &value).map_err(|e| self.error(e))?;
            if row.tombstone || row.values.len() > columns {
                return Err(self.error(CoreError::Corrupt("cursor row arity".into())));
            }
            let mut values = row.values;
            values.resize(columns, Value::Null);
            Ok(scan::SourceRow {
                key: Cow::Owned(key[8..].to_vec()),
                row: Cow::Owned(SqlRow {
                    values,
                    version: row.row_version,
                }),
            })
        })))
    }
    fn get<'s>(
        &'s self,
        table: &relational::TableDef,
        key: &[u8],
    ) -> Result<Option<scan::SourceRow<'s>>, SqlError> {
        self.scan(
            table,
            &scan::KeyRange {
                lo: Bound::Included(key.to_vec()),
                hi: Bound::Included(key.to_vec()),
            },
            scan::Direction::Forward,
        )?
        .next()
        .transpose()
    }
}

#[cfg(all(test, feature = "engine-rocksdb"))]
mod tests {
    use super::*;
    #[test]
    fn v2_streams_twenty_mib_and_preserves_snapshot() {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path =
            std::env::temp_dir().join(format!("cursor-v2-{:032x}", u128::from_be_bytes(nonce)));
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        let mut s = admin.sql_session();
        s.execute(
            "CREATE TABLE t (id bigint PRIMARY KEY, body text)",
            &[],
            None,
        )
        .unwrap();
        let Backend::V2(db) = &admin.inner.backend else {
            unreachable!()
        };
        let mut txn = db.begin(engine::txn::Isolation::ReadCommitted);
        let entry = engine::catalog::TableEntry {
            def: db.catalog().parts(&mut txn).unwrap().tables["t"].clone(),
            heap: false,
            columns_ext: BTreeMap::new(),
        };
        drop(txn);
        let body = "x".repeat(56 * 1024);
        let insert = |id| {
            let mut txn = db.begin(engine::txn::Isolation::ReadCommitted);
            db.rows
                .insert(
                    &mut txn,
                    &entry,
                    vec![Value::Int(id), Value::Text(body.clone())],
                    None,
                    &|| false,
                )
                .unwrap();
            txn.commit().unwrap();
        };
        // Populate through the typed row store: the existing SQL write bridge
        // independently bounds its complete mutation image.
        for id in 0..370 {
            insert(id);
        }
        let p = s.prepare("SELECT id,body FROM t ORDER BY id").unwrap();
        let mut c = s.open_cursor(&p, &[]).unwrap();
        insert(370);
        let mut seen = 0;
        while !c.is_exhausted() {
            let rows = c.fetch(7, 128 * 1024).unwrap();
            assert!(rows.len() <= 2);
            for row in rows {
                assert_eq!(
                    row.values,
                    vec![Value::Int(seen), Value::Text(body.clone())]
                );
                seen += 1;
            }
        }
        assert_eq!(seen, 370);
        assert!(370 * body.len() > 20 * 1024 * 1024);
        drop(c);
        drop(s);
        drop(admin);
        std::fs::remove_dir_all(path).unwrap();
    }
}
