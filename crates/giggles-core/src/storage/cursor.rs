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
    /// The task statement's release tag (GD-0061 R3-301); `None` outside a task session.
    tag: Option<Arc<crate::task::release::ReleaseTag>>,
    /// The task's visible rows (GD-0061 R3-305); `None` outside a task session
    /// and for buffered cursors, whose rows were scoped when they were read.
    scope: Option<crate::task::rows::Scope>,
    meter: crate::task::rows::StatementMeter,
}
enum Data {
    Scan {
        image: Arc<relational::Relational>,
        source: Option<V2Snapshot>,
        plan: Box<Plan>,
    },
    Buffered(VecDeque<SqlRow>),
    /// A v2 result past the in-memory threshold, read from its temp file.
    Spilled(relational::spill::SpilledRows, Option<SqlRow>),
}
fn row_size(row: &SqlRow) -> usize {
    row.values.iter().map(relational::value_bytes).sum()
}
impl Cursor {
    /// Fetch a bounded chunk. A row larger than max_bytes returns 54000 without
    /// dropping that row. Zero budgets are rejected. Exhaustion is sticky.
    ///
    /// Plain exit for trusted local callers: a task session's cursor is refused
    /// before it advances. Task callers use `fetch_unreleased`.
    pub fn fetch(&mut self, max_rows: usize, max_bytes: usize) -> Result<Vec<SqlRow>, CoreError> {
        if self.tag.is_some() {
            return Err(crate::task::release::refusal());
        }
        self.fetch_unreleased(max_rows, max_bytes)?.untasked()
    }
    /// `fetch`, with the chunk held until `SqlSession::release_page`.
    pub fn fetch_unreleased(
        &mut self,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<crate::task::release::Unreleased<Vec<SqlRow>>, CoreError> {
        let rows = self.fetch_rows(max_rows, max_bytes)?;
        Ok(crate::task::release::Unreleased::new(rows, self.tag.clone()))
    }
    fn fetch_rows(&mut self, max_rows: usize, max_bytes: usize) -> Result<Vec<SqlRow>, CoreError> {
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
                    let size = row_size(row);
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
            Data::Spilled(rows, pending) => {
                let mut result = Vec::new();
                let mut bytes = 0usize;
                while result.len() < max_rows {
                    let row = match pending.take() {
                        Some(row) => row,
                        None => match rows.next() {
                            Some(row) => row?,
                            None => break,
                        },
                    };
                    let size = row_size(&row);
                    if bytes.saturating_add(size) > max_bytes {
                        *pending = Some(row);
                        if result.is_empty() {
                            return Err(CoreError::Sql(SqlError::Limit));
                        }
                        break;
                    }
                    bytes += size;
                    result.push(row);
                }
                Ok(result)
            }
            Data::Scan {
                image,
                source,
                plan,
            } => {
                let legacy = scan::LegacyRows(image);
                let inner = source
                    .as_ref()
                    .map_or(&legacy as &dyn RowSource, |s| s as &dyn RowSource);
                // A task cursor reads through the visible-rows wrapper (R3-305).
                let scoped = self
                    .scope
                    .as_ref()
                    .map(|scope| crate::task::rows::Scoped::new(inner, scope, &self.meter));
                let result = plan.fetch(
                    image,
                    scoped.as_ref().map_or(inner, |s| s as &dyn RowSource),
                    max_rows,
                    max_bytes,
                );
                if let Some(scoped) = &scoped {
                    crate::task::rows::record(self.meter.counts());
                    if let Some(tag) = &self.tag {
                        tag.add_meter(&self.meter);
                    }
                    if let Some(error) = scoped.take_error() {
                        return Err(error.into());
                    }
                }
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
            Data::Spilled(rows, pending) => pending.is_none() && rows.is_empty(),
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
        // The statement's tag, kept until the outcome is known so a failure
        // is logged as the statement's outcome (GD-0061 R3-309).
        let mut started: Option<Arc<crate::task::release::ReleaseTag>> = None;
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
            // A task cursor's tag carries the output plan; `SELECT *` runs
            // expanded to the task's columns (GD-0061 R3-308). A denied
            // statement is refused here, before any row is read.
            let (tag, expanded) = self.statement_tag(&p)?;
            started = tag.clone();
            if let Some(expanded) = expanded {
                p = expanded;
            }
            // A buffered fallback reruns the statement under its own tag:
            // this one then logs nothing.
            let discard = |tag: &Option<Arc<crate::task::release::ReleaseTag>>| {
                if let Some(tag) = tag {
                    tag.discard();
                }
            };
            let context = clock.map(|c| {
                relational::clock::ExecutionClock::new(c)
                    .with_extra_float_digits(self.extra_float_digits)
            });
            let (image, source, context, scope) = match &self.admin.inner.backend {
                Backend::V2(db) => {
                    // The snapshot source below reads the local store directly; a cluster
                    // gateway buffers the result of the normal execution path instead
                    // (GD-0075 P7-2; streaming over ranges is later work).
                    if db.cluster_services().is_some() {
                        discard(&tag);
                        return self.buffer_cursor(&p, params, clock, buffer_limit);
                    }
                    // The existing transaction API cannot fork its write overlay.
                    // Keep read-your-writes correct via the bounded bridge fallback.
                    // SERIALIZABLE reads must enter the read set validated at COMMIT,
                    // which the snapshot source below bypasses (GD-0075 R5-76).
                    if self.v2_transaction.as_ref().is_some_and(|t| {
                        t.writes_keyspace(engine::kv::Keyspace::Rows)
                            || t.writes_keyspace(engine::kv::Keyspace::Index)
                            || t.isolation() == engine::txn::Isolation::Serializable
                    }) {
                        discard(&tag);
                        return self.buffer_cursor(&p, params, clock, buffer_limit);
                    }
                    if p.domain != u128::from_be_bytes(db.meta.domain) || p.epoch != 0 {
                        return Err(CoreError::Sql(SqlError::StaleSchema));
                    }
                    let mut implicit = None;
                    let txn = self.v2_transaction.as_mut().unwrap_or_else(|| {
                        implicit.insert(db.begin(engine::txn::Isolation::ReadCommitted))
                    });
                    let catalog = db.catalog().snapshot(txn)?;
                    let parts = &catalog.parts;
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
                    let catalog = db.catalog().snapshot(txn)?;
                    // Cursors allocate no catalog IDs. Match metadata snapshots:
                    // include every validated schema/type/enum-label ID without
                    // duplicating the catalog allocator's object inventory.
                    let image = catalog.cursor_image().map_err(CoreError::Sql)?;
                    // Sequence-bearing selects use the established execution path.
                    if !p.sequences().is_empty() {
                        discard(&tag);
                        return self.buffer_cursor(&p, params, clock, buffer_limit);
                    }
                    // A task statement that reads a table sees only the task's
                    // visible rows (GD-0061 R3-305), bound at this snapshot.
                    let scope = match &self.task {
                        Some(ctx) if !p.read_tables().is_empty() => {
                            Some(crate::task::rows::scope(db, txn, &image, ctx)?)
                        }
                        _ => None,
                    };
                    (
                        image,
                        Some(V2Snapshot {
                            #[cfg(test)]
                            index_reads: std::cell::Cell::new(0),
                            indexes: catalog.parts.indexes.clone(),
                            db: db.clone(),
                            _guard: guard,
                            ts,
                            error: RefCell::new(None),
                        }),
                        context,
                        scope,
                    )
                }
                Backend::Legacy(_) => {
                    // Tasks are v2 only: a task context here was never opened
                    // from a task catalog.
                    if self.task.is_some() {
                        return Err(crate::task::TaskError::Revoked.into());
                    }
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
                    (Arc::new(s.sql), None, context, None)
                }
            };
            match Plan::new(&image, &p, params, context.clone()).map_err(CoreError::Sql)? {
                Some(plan) => Ok(Cursor {
                    data: Data::Scan {
                        image,
                        source,
                        plan: Box::new(plan),
                    },
                    clock: context,
                    tag,
                    scope,
                    meter: Default::default(),
                }),
                None => {
                    discard(&tag);
                    self.buffer_cursor(&p, params, clock, buffer_limit)
                }
            }
        })();
        if let Err(error) = &result {
            if let Some(tag) = &started {
                tag.fail(error);
            }
            self.abort();
        }
        drop(started);
        result
    }
    fn buffer_cursor(
        &mut self,
        p: &relational::Prepared,
        params: &[Value],
        clock: Option<&relational::StatementClock>,
        limit: usize,
    ) -> Result<Cursor, CoreError> {
        // A task statement's spill is handed only to this tagged stream: the
        // cursor keeps the statement's tag, so every spilled page goes through
        // `release_page` (masking, validity, debit) like a buffered one, and
        // its rows were scoped when the statement read them (GD-0061).
        let (result, spilled) = relational::spill::streaming_tagged(|| {
            scan::with_cursor_limit(limit, || match clock {
                Some(clock) => self.execute_prepared_at_unreleased(p, params, None, clock),
                None => self.execute_prepared_unreleased(p, params, None),
            })
        });
        let (result, tag) = result?.into_parts();
        Ok(Cursor {
            data: match spilled {
                Some(rows) => Data::Spilled(rows, None),
                None => Data::Buffered(result.rows.into()),
            },
            clock: None,
            tag,
            scope: None,
            meter: Default::default(),
        })
    }
}
struct V2Snapshot {
    #[cfg(test)]
    index_reads: std::cell::Cell<usize>,
    indexes: Vec<relational::secondary_index::Descriptor>,
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
    fn index_scan<'s>(
        &'s self,
        table: &relational::TableDef,
        predicates: &[(usize, &str, Value)],
        after: Option<&[u8]>,
    ) -> Result<Option<scan::SourceRows<'s>>, SqlError> {
        let Some(index) = self.indexes.iter().find(|d| {
            d.table == table.id
                && predicates
                    .iter()
                    .any(|(c, _, _)| d.columns.first() == Some(&table.columns[*c].id))
        }) else {
            return Ok(None);
        };
        let prefix = index.id.to_be_bytes().to_vec();
        let mut lo = prefix.clone();
        // NULL sorts last in this codec and cannot satisfy these comparisons.
        let mut hi = prefix.clone();
        hi.push(0xff);
        for (_, op, value) in predicates
            .iter()
            .filter(|(c, _, _)| index.columns.first() == Some(&table.columns[*c].id))
        {
            let mut key = prefix.clone();
            key.extend(
                engine::codec::index_tuple_bytes(
                    std::slice::from_ref(value),
                    &[engine::codec::Direction::Asc],
                )
                .map_err(|e| self.error(e))?,
            );
            let next = || engine::indexes::successor(key.clone()).expect("index prefix");
            match *op {
                "=" => {
                    lo = lo.max(key.clone());
                    hi = hi.min(next());
                }
                ">=" => lo = lo.max(key.clone()),
                ">" => lo = lo.max(next()),
                "<=" => hi = hi.min(next()),
                "<" => hi = hi.min(key.clone()),
                _ => unreachable!(),
            }
        }
        let lower = after.map_or(Bound::Included(lo.clone()), |key| {
            if key >= lo.as_slice() {
                Bound::Excluded(key.to_vec())
            } else {
                Bound::Included(lo.clone())
            }
        });
        let iter = engine::mvcc::VersionedRange::new(
            self.db.kv.as_ref(),
            engine::kv::Keyspace::Index,
            lower.as_ref().map(Vec::as_slice),
            Bound::Excluded(hi.as_slice()),
            engine::kv::Direction::Forward,
            self.ts,
        )
        .map_err(|e| self.error(e))?;
        let table = table.clone();
        Ok(Some(Box::new(iter.map(move |pair| {
            crate::cancellation::check().map_err(|e| self.error(e))?;
            let (key, pk) = pair.map_err(|e| self.error(e))?;
            #[cfg(test)]
            self.index_reads.set(self.index_reads.get() + 1);
            let mut row = self.get(&table, &pk)?.ok_or_else(|| {
                self.error(CoreError::Corrupt("dangling cursor index entry".into()))
            })?;
            row.key = Cow::Owned(key);
            Ok(row)
        }))))
    }
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
    fn secondary_cursor_matches_scan_and_resumes() {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path = std::env::temp_dir().join(format!("cursor-index-{:032x}", u128::from_be_bytes(nonce)));
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        let mut s = admin.sql_session();
        for sql in ["CREATE TABLE t (id bigint PRIMARY KEY, email text UNIQUE, user_id bigint)",
            "CREATE INDEX users ON t (user_id, email)"] {
            s.execute(sql, &[], None).unwrap();
        }
        let Backend::V2(db) = &admin.inner.backend else { unreachable!() };
        let mut txn = db.begin(engine::txn::Isolation::ReadCommitted);
        let entry = engine::catalog::TableEntry {
            primary_index_birth: None,
            def: db.catalog().parts(&mut txn).unwrap().tables["t"].clone(),
            heap: false, columns_ext: BTreeMap::new(),
        };
        let start = std::time::Instant::now();
        for id in 0..50_000 {
            db.rows.insert(&mut txn, &entry, vec![Value::Int(id),
                if id == 49_999 { Value::Null } else { Value::Text(format!("e{id}")) },
                if id == 49_999 { Value::Null } else { Value::Int(id / 4) }], None, &|| false).unwrap();
        }
        txn.commit().unwrap();
        eprintln!("50k index fixture: {:?}", start.elapsed());
        let drain = |mut cursor: Cursor| {
            let mut rows = Vec::new();
            while !cursor.is_exhausted() { rows.extend(cursor.fetch(7, 4096).unwrap()); }
            (rows, cursor)
        };
        for (predicate, count, indexed) in [
            ("email = 'e7'", 1, true), ("email = 'absent'", 0, true),
            ("user_id = 7", 4, true), ("user_id < 6", 24, true),
            ("user_id BETWEEN 3 AND 5", 12, true),
            ("user_id = 7 AND id > '28'", 3, false),
            ("user_id = 7 AND email <> 'e28'", 3, true),
            ("email IS NULL", 1, false), ("user_id < 300", 1200, true),
            ("user_id IS NULL", 1, false), ("user_id >= 12499", 3, true),
            ("user_id = '7'", 4, true), ("user_id > 8 AND user_id < 2", 0, true),
            ("user_id = 7 ORDER BY id DESC", 4, false),
            ("user_id = 7.1", 0, false), ("user_id < 2.5", 12, false),
        ] {
            let p = s.prepare(&format!("SELECT id FROM t WHERE {predicate}")).unwrap();
            let start = std::time::Instant::now();
            let (mut actual, cursor) = drain(s.open_cursor(&p, &[]).unwrap());
            let elapsed = start.elapsed();
            let Data::Scan { source: Some(source), .. } = &cursor.data else { panic!("streaming") };
            let reads = if predicate.contains("email <>") { 4 } else { count };
            if indexed && count > 0 { assert_eq!(source.index_reads.get(), reads, "{predicate}"); }
            if !indexed { assert_eq!(source.index_reads.get(), 0, "{predicate}"); }
            let mut scan = s.open_cursor(&p, &[]).unwrap();
            let Data::Scan { source: Some(source), .. } = &mut scan.data else { unreachable!() };
            source.indexes.clear();
            let start = std::time::Instant::now();
            let (mut expected, _) = drain(scan);
            eprintln!("{predicate}: index {elapsed:?}, scan {:?}", start.elapsed());
            assert_eq!(actual.len(), count, "{predicate}");
            if !predicate.contains("ORDER BY") {
                actual.sort_by_key(|row| format!("{:?}", row.values));
                expected.sort_by_key(|row| format!("{:?}", row.values));
            }
            assert_eq!(actual, expected, "{predicate}");
        }
        // The existing SQL write bridge caps indexed mutation images at 1024 rows.
        for sql in ["CREATE TABLE u (id bigint PRIMARY KEY, email text UNIQUE, user_id bigint)",
            "CREATE INDEX u_users ON u (user_id)",
            "INSERT INTO u VALUES (28,'e28',7),(29,'e29',7),(30,'e30',7),(31,'e31',7)"] {
            s.execute(sql, &[], None).unwrap();
        }
        let mut writer = admin.sql_session();
        writer.execute("BEGIN", &[], None).unwrap();
        writer.execute("INSERT INTO u VALUES (50000, 'pending', 7)", &[], None).unwrap();
        let p = s.prepare("SELECT id FROM u WHERE user_id = 7").unwrap();
        let cursor = s.open_cursor(&p, &[]).unwrap();
        assert_eq!(drain(s.open_cursor(&p, &[]).unwrap()).0.len(), 4);
        let own = writer.prepare("SELECT id FROM u WHERE email = 'pending'").unwrap();
        assert_eq!(drain(writer.open_cursor(&own, &[]).unwrap()).0.len(), 1);
        writer.execute("COMMIT", &[], None).unwrap();
        assert_eq!(drain(cursor).0.len(), 4);
        assert_eq!(drain(s.open_cursor(&p, &[]).unwrap()).0.len(), 5);
        drop(writer); drop(s); drop(admin);
        std::fs::remove_dir_all(path).unwrap();
    }
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
            primary_index_birth: None,
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
