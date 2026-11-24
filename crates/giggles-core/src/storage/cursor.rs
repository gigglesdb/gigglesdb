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
    value_budget: usize,
    statement_tokens: usize,
    wide_values: bool,
    engine: Option<CursorEngine>,
    /// The task statement's release tag (GD-0061 R3-301); `None` outside a task session.
    tag: Option<Arc<crate::task::release::ReleaseTag>>,
    /// The task's visible rows (GD-0061 R3-305); `None` outside a task session
    /// and for buffered cursors, whose rows were scoped when they were read.
    scope: Option<crate::task::rows::Scope>,
    meter: crate::task::rows::StatementMeter,
    /// The task statement's slots and time limit (GD-0061 R3-401), held from
    /// open until the cursor is exhausted, expires or is dropped; the guard
    /// records when the cursor was opened.
    guard: Option<crate::task::limits::StatementGuard>,
    /// The error that closed this task cursor: a fetch after the statement
    /// time (57014, R3-401) or past a row limit (54T01, R3-402).
    closed: Option<CoreError>,
}
impl Drop for Cursor {
    fn drop(&mut self) {
        let data = std::mem::replace(&mut self.data, Data::Buffered(VecDeque::new()));
        drop(data);
        drop(self.clock.take());
        drop(self.tag.take());
        drop(self.engine.take());
    }
}

enum CursorEngine {
    // Drop the lease before the slot owner. The snapshot and clock have
    // already been dropped by the cursor.
    Streaming { lease: EngineLease, _owner: Admin },
    Materialized(GenerationTag),
}
impl CursorEngine {
    fn check(&self) -> Result<(), CoreError> {
        match self {
            Self::Streaming { lease, .. } => lease.check(),
            Self::Materialized(tag) => tag.check_materialized(),
        }
    }
}

enum Data {
    Scan {
        image: Arc<relational::Relational>,
        volatile: Arc<Mutex<relational::volatile::SessionVolatile>>,
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
impl SqlSession {
    /// Fetch with the current owned path while retaining the cursor's catalog and identity.
    pub fn fetch_cursor_unreleased(&self, cursor: &mut Cursor, max_rows: usize, max_bytes: usize)
        -> Result<crate::task::release::Unreleased<Vec<SqlRow>>, CoreError> {
        if let Some(clock) = &cursor.clock {
            clock.set_privilege_search_path(&self.search_path);
        }
        cursor.fetch_unreleased(max_rows, max_bytes)
    }
}
impl Cursor {
    /// Fetch a bounded chunk. V2 admits one oversized row alone; legacy retains
    /// its hard page limit. Zero budgets are rejected. Exhaustion is sticky.
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
    /// A task cursor's fetch runs within its statement time: a fetch after
    /// `opened_at + statement_ms` fails with 57014 and closes the cursor,
    /// which gives its slots back, as exhaustion does (GD-0061 R3-401). A
    /// fetch that trips the rows-scanned limit or the rows-read allowance
    /// closes it the same way (R3-402); later fetches repeat the error.
    fn fetch_rows(&mut self, max_rows: usize, max_bytes: usize) -> Result<Vec<SqlRow>, CoreError> {
        if let Some(error) = &self.closed {
            return Err(error.clone());
        }
        if let Some(engine) = &self.engine
            && let Err(error) = engine.check()
        {
            self.close(error.clone());
            return Err(error);
        }
        if self.guard.as_ref().is_some_and(|guard| guard.expired()) {
            self.close(CoreError::Cancelled);
            return Err(CoreError::Cancelled);
        }
        let active = self
            .guard
            .as_ref()
            .map(crate::task::limits::StatementGuard::activate);
        let result = self.fetch_page(max_rows, max_bytes);
        drop(active);
        match &result {
            Err(_) if self.guard.as_ref().is_some_and(|guard| guard.expired()) => {
                // The scan meter reports cancellation as SqlError::Aborted.
                // Return the deadline error we store, including on this first
                // fetch, rather than leaking 25P02 from an autocommit read.
                self.close(CoreError::Cancelled);
                return Err(CoreError::Cancelled);
            }
            Err(error @ CoreError::Task(crate::task::TaskError::BudgetExceeded(_)))
                if self.guard.is_some() =>
            {
                self.close(error.clone())
            }
            Ok(_) if self.is_exhausted() => self.guard = None,
            _ => {}
        }
        result
    }
    /// Closes a task cursor that failed with `error`: its rows, snapshot and
    /// slots go now, and its statement is logged with the error.
    fn close(&mut self, error: CoreError) {
        self.guard = None;
        self.data = Data::Buffered(VecDeque::new());
        self.clock = None;
        if let Some(tag) = &self.tag {
            tag.fail(&error);
        }
        self.engine = None;
        self.closed = Some(error);
    }
    fn fetch_page(&mut self, max_rows: usize, max_bytes: usize) -> Result<Vec<SqlRow>, CoreError> {
        let _budget = relational::ValueBudget::enter(self.value_budget, self.wide_values);
        let _statement_budget = relational::StatementBudget::enter(self.statement_tokens, self.wide_values);
        crate::cancellation::check()?;
        if max_rows == 0 || max_bytes == 0 {
            return Err(CoreError::Sql(SqlError::Limit));
        }
        let wide = self.wide_values;
        match &mut self.data {
            Data::Buffered(rows) => {
                let mut result = Vec::new();
                let mut bytes = 0usize;
                while result.len() < max_rows {
                    let Some(row) = rows.front() else {
                        break;
                    };
                    let size = row_size(row);
                    if bytes.saturating_add(size) > max_bytes && !(wide && result.is_empty()) {
                        if result.is_empty() {
                            return Err(CoreError::Sql(SqlError::Limit));
                        }
                        break;
                    }
                    bytes += size;
                    result.push(rows.pop_front().expect("front exists"));
                    if bytes >= max_bytes { break; }
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
                    if bytes.saturating_add(size) > max_bytes && !(wide && result.is_empty()) {
                        *pending = Some(row);
                        if result.is_empty() {
                            return Err(CoreError::Sql(SqlError::Limit));
                        }
                        break;
                    }
                    bytes += size;
                    result.push(row);
                    // Do not deserialize another wide row after filling this page.
                    if bytes >= max_bytes { break; }
                }
                Ok(result)
            }
            Data::Scan {
                image,
                volatile,
                source,
                plan,
            } => {
                let _volatile = relational::volatile::Scope::enter(volatile);
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
        self.open_cursor_inner(p, params, None, DEFAULT_CURSOR_BUFFER_BYTES, true)
    }
    pub fn open_cursor_at(
        &mut self,
        p: &relational::Prepared,
        params: &[Value],
        clock: &relational::StatementClock,
    ) -> Result<Cursor, CoreError> {
        self.open_cursor_inner(p, params, Some(clock), DEFAULT_CURSOR_BUFFER_BYTES, true)
    }

    /// Open a one-shot statement without looking up or retaining a cursor binding.
    pub fn open_uncached_cursor_at(
        &mut self,
        p: &relational::Prepared,
        params: &[Value],
        clock: &relational::StatementClock,
    ) -> Result<Cursor, CoreError> {
        self.open_cursor_inner(p, params, Some(clock), DEFAULT_CURSOR_BUFFER_BYTES, false)
    }

    #[cfg(feature = "test-cursor-bindings")]
    #[doc(hidden)]
    pub fn test_cursor_query_binds() -> usize {
        relational::cursor::query_binds()
    }

    /// A weak probe of the binding's lifetime, including after it leaves the slot.
    #[cfg(feature = "test-cursor-bindings")]
    #[doc(hidden)]
    pub fn test_cursor_binding_liveness(&self) -> Option<impl Fn() -> bool + use<>> {
        self.cursor_plan_cache.binding_liveness()
    }
    /// Set the independent budget for statement shapes that require buffering.
    pub fn open_cursor_with_buffer_limit(
        &mut self,
        p: &relational::Prepared,
        params: &[Value],
        max_buffer_bytes: usize,
    ) -> Result<Cursor, CoreError> {
        self.open_cursor_inner(p, params, None, max_buffer_bytes, true)
    }
    fn open_cursor_inner(
        &mut self,
        p: &relational::Prepared,
        params: &[Value],
        clock: Option<&relational::StatementClock>,
        buffer_limit: usize,
        use_plan_cache: bool,
    ) -> Result<Cursor, CoreError> {
        if let Some(clock) = clock {
            self.guard_clock(clock)?;
        }
        let _date_order = relational::clock::LiteralDateOrder::enter(p.literal_date_order);
        let _budget = relational::ValueBudget::enter(self.value_budget(), matches!(self.admin.inner.backend, Backend::V2(_)));
        let _statement_budget = relational::StatementBudget::enter(self.max_statement_tokens, matches!(self.admin.inner.backend, Backend::V2(_)));
        relational::assert_sql_frontend_allowed();
        // The statement's tag, kept until the outcome is known so a failure
        // is logged as the statement's outcome (GD-0061 R3-309).
        let mut started: Option<Arc<crate::task::release::ReleaseTag>> = None;
        // Its slots (GD-0061 R3-401): handed to the cursor, or released here
        // on error or unwind.
        let mut held: Option<crate::task::limits::StatementGuard> = None;
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
            let lease = if self.is_v2_engine() { Some(self.v2_lease(Holder::Cursor)?) } else { None };
            let db = lease.as_deref();
            Self::check_prepared_generation(p, db)?;
            let mut p = p.clone();
            p.set_schema_context(self.search_path.clone(), self.schema_user.clone());
            // A task cursor's tag carries the output plan; `SELECT *` runs
            // expanded to the task's columns (GD-0061 R3-308). A denied
            // statement is refused here, before any row is read.
            let (tag, expanded) = self.statement_tag_leased(&p, db)?;
            started = tag.clone();
            held = crate::task::limits::StatementGuard::enter_leased(self, db)?;
            let _active = held
                .as_ref()
                .map(crate::task::limits::StatementGuard::activate);
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
                    .with_text_settings(self.date_style, self.bytea_output)
                    .with_extra_float_digits(self.extra_float_digits)
            });
            let (image, source, context, scope, plan_catalog) = match &self.admin.inner.backend {
                Backend::V2(_) => {
                    let db = db.expect("V2 cursor is leased");
                    // Stream through the local store or the gateway's paged range reader.
                    // Correctness-sensitive statement shapes still use bounded buffering.
                    // The existing transaction API cannot fork its write overlay.
                    // Keep read-your-writes correct via the bounded bridge fallback.
                    // SERIALIZABLE reads must enter the read set validated at COMMIT,
                    // which the snapshot source below bypasses (GD-0075 R5-76).
                    if self.v2_transaction.as_ref().is_some_and(|t| {
                        t.writes_keyspace(engine::kv::Keyspace::Rows)
                            || t.writes_keyspace(engine::kv::Keyspace::Index)
                            || t.isolation() == engine::txn::Isolation::Serializable
                    }) {
                        if use_plan_cache {
                            self.cursor_plan_cache.clear();
                        }
                        discard(&tag);
                        return self.buffer_cursor(&p, params, clock, buffer_limit, lease);
                    }
                    // V2 prepare normalizes the presentation epoch to zero, including
                    // gateway catalog-version epochs; match normal v2 execution.
                    if p.domain != u128::from_be_bytes(db.meta.domain) || p.epoch != 0 {
                        return Err(CoreError::Sql(SqlError::StaleSchema));
                    }
                    // The implicit read transaction drops on every exit (including errors
                    // and fallbacks), releasing cluster locks, liveness and heartbeats.
                    // The cursor retains only the fixed-timestamp reader below.
                    let mut implicit = None;
                    let txn = self.v2_transaction.as_mut().unwrap_or_else(|| {
                        implicit.insert(db.begin(engine::txn::Isolation::ReadCommitted))
                    });
                    let catalog = db.catalog().snapshot(txn)?;
                    if use_plan_cache {
                        self.cursor_plan_cache.observe(catalog.cursor_plan_owners.as_ref());
                    }
                    let parts = &catalog.parts;
                    for name in p.read_tables() {
                        let name = parts.tables.keys().find(|key| **key == name).cloned();
                        if let Some(name) = name {
                            db.catalog()
                                .resolve(
                                    txn,
                                    &name,
                                    engine::locks::LockMode::AccessShare,
                                    crate::task::limits::lock_deadline(),
                                    &|| crate::cancellation::check().is_err() || db.check().is_err(),
                                )
                                .map_err(|e| match e {
                                    engine::catalog::CatalogError::Core(e) => e,
                                    engine::catalog::CatalogError::Txn(e) => {
                                        crate::task::limits::lock_error(e)
                                    }
                                })?;
                        }
                    }
                    let guard = db.sequencer.snapshot();
                    txn.begin_statement();
                    let ts = txn.snapshot_ts();
                    if let Some(error) = txn.snapshot_error() {
                        return Err(error.clone().into());
                    }
                    let catalog = db.catalog().snapshot(txn)?;
                    if use_plan_cache {
                        self.cursor_plan_cache.observe(catalog.cursor_plan_owners.as_ref());
                    }
                    // Cursors allocate no catalog IDs. Match metadata snapshots:
                    // include every validated schema/type/enum-label ID without
                    // duplicating the catalog allocator's object inventory.
                    let image = catalog.cursor_image().map_err(CoreError::Sql)?;
                    // Sequence-bearing selects use the established execution path.
                    if !p.sequences().is_empty() {
                        if use_plan_cache {
                            self.cursor_plan_cache.clear();
                        }
                        discard(&tag);
                        return self.buffer_cursor(&p, params, clock, buffer_limit, lease);
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
                        image.clone(),
                        Some(V2Snapshot {
                            #[cfg(test)]
                            index_reads: std::cell::Cell::new(0),
                            indexes: catalog.parts.indexes.iter().filter(|d| {
                                d.predicate.is_none()
                                    && !catalog.parts.ext.column_collations.contains_key(&d.id)
                                    && d.columns.iter().all(|id|image.deterministic_column(*id) && image.byte_ordered(*id))
                            }).cloned().collect(),
                            kv: db.cluster_services().map_or_else(
                                || db.kv.clone() as Arc<dyn engine::kv::KvRead>,
                                |services| services.reader(ts),
                            ),
                            _guard: guard,
                            ts,
                            error: RefCell::new(None),
                        }),
                        context,
                        scope,
                        Some(catalog),
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
                            Arc::new(s.clone()),
                            baseline,
                            self.pending_sequences.clone(),
                            self.sequence_values.clone(),
                            self.sequence_tables.clone(),
                            self.search_path.clone(),
                        ))
                    });
                    (Arc::new(s.sql), None, context, None, None)
                }
            };
            let context = context.map(|clock| match self.privilege_callback.as_ref().filter(|_| p.has_privilege_inquiry()) {
                Some(callback) => clock.with_privileges(callback.clone(), privilege_provider(
                    PrivilegeImage::Catalog(image.clone()), self.search_path.clone(), self.v2_transaction.as_ref(), self.admin.roles(), db)),
                None => clock,
            });
            let cache = plan_catalog.as_ref()
                .and_then(|catalog| catalog.cursor_plan_owners.as_ref())
                .filter(|_| use_plan_cache && self.task.is_none())
                .map(|owners| (&mut self.cursor_plan_cache, owners));
            match Plan::new(&image, &p, params, context.clone(), cache).map_err(CoreError::Sql)? {
                Some(plan) => Ok(Cursor {
                    engine: lease
                        .map(|pin| Ok::<_, CoreError>(CursorEngine::Streaming {
                            lease: pin.into_holder(Holder::Cursor)?,
                            _owner: self.admin.clone(),
                        }))
                        .transpose()?,
                    data: Data::Scan {
                        image,
                        volatile: self.volatile.clone(),
                        source,
                        plan: Box::new(plan),
                    },
                    value_budget: self.value_budget(), statement_tokens: self.max_statement_tokens,
            wide_values: matches!(self.admin.inner.backend, Backend::V2(_)),
                    clock: context,
                    tag,
                    scope,
                    meter: Default::default(),
                    guard: None,
                    closed: None,
                }),
                None => {
                    discard(&tag);
                    self.buffer_cursor(&p, params, clock, buffer_limit, lease)
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
        result.map(|mut cursor| {
            if !cursor.is_exhausted() {
                cursor.guard = held.take();
            }
            cursor
        })
    }
    pub(super) fn buffer_cursor(
        &mut self,
        p: &relational::Prepared,
        params: &[Value],
        clock: Option<&relational::StatementClock>,
        limit: usize,
        lease: Option<SessionLease>,
    ) -> Result<Cursor, CoreError> {
        let db = lease.as_deref();
        if let Some(db) = db { db.check()?; }
        // A task statement's spill is handed only to this tagged stream: the
        // cursor keeps the statement's tag, so every spilled page goes through
        // `release_page` (masking, validity, debit) like a buffered one, and
        // its rows were scoped when the statement read them (GD-0061).
        // Embedded materialized cursors may fetch held rows after owner drop.
        // Task release and readlog completion still need a live continuation
        // for the debit and audit record, and are fenced once the owner drops.
        let (result, spilled) = relational::spill::streaming_tagged(|| {
            scan::with_cursor_limit(limit, || match clock {
                Some(clock) => {
                    self.guard_clock(clock)?;
                    self.execute_prepared_at_leased(p, params, None, clock, db, None)
                },
                None => self.execute_bound_clocked_leased(p, params, None, None, db, None),
            })
        });
        let (result, tag) = result?.into_parts();
        Ok(Cursor {
            engine: db.map(|db| CursorEngine::Materialized(db.tag())),
            data: match spilled {
                Some(rows) => Data::Spilled(rows, None),
                None => Data::Buffered(result.rows.into()),
            },
            value_budget: self.value_budget(), statement_tokens: self.max_statement_tokens,
            wide_values: matches!(self.admin.inner.backend, Backend::V2(_)),
            clock: None,
            tag,
            scope: None,
            meter: Default::default(),
            guard: None,
            closed: None,
        })
    }
}
struct V2Snapshot {
    #[cfg(test)]
    index_reads: std::cell::Cell<usize>,
    indexes: Vec<relational::secondary_index::Descriptor>,
    kv: Arc<dyn engine::kv::KvRead>,
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
            self.kv.as_ref(),
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
            self.kv.as_ref(),
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
        let prefix = table.id.to_be_bytes();
        let mut full_key = Vec::with_capacity(prefix.len() + key.len());
        full_key.extend_from_slice(&prefix);
        full_key.extend_from_slice(key);
        let Some(payload) = engine::mvcc::read_at(
            self.kv.as_ref(),
            engine::kv::Keyspace::Rows,
            &full_key,
            self.ts,
        )
        .map_err(|e| self.error(e))?
        else {
            return Ok(None);
        };
        let row = engine::codec::decode_row(&full_key, &payload).map_err(|e| self.error(e))?;
        let columns = table.columns.len();
        if row.tombstone || row.values.len() > columns {
            return Err(self.error(CoreError::Corrupt("cursor row arity".into())));
        }
        let mut values = row.values;
        values.resize(columns, Value::Null);
        full_key.drain(..prefix.len());
        Ok(Some(scan::SourceRow {
            key: Cow::Owned(full_key),
            row: Cow::Owned(SqlRow {
                values,
                version: row.row_version,
            }),
        }))
    }
}

#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "cursor_point_tests.rs"]
mod point_tests;

#[cfg(all(test, feature = "engine-rocksdb"))]
mod tests {
    use super::*;

    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn spilled_cursor_releases_the_directory_and_keeps_its_rows() {
        let path = crate::test_artifact_root().join("detached-spilled-cursor");
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        let mut session = admin.sql_session();
        session.execute("CREATE TABLE t(id INT PRIMARY KEY, payload TEXT)", &[], None).unwrap();
        let payload = Value::Text("x".repeat(512 * 1024));
        for id in 1..=3 {
            session.execute("INSERT INTO t VALUES ($1,$2)", &[Value::Int(id), payload.clone()], None).unwrap();
        }
        let p = session.prepare("SELECT payload FROM t ORDER BY id").unwrap();
        let lease = session.v2_lease(Holder::Cursor).unwrap();
        let mut cursor = session.buffer_cursor(&p, &[], None, DEFAULT_CURSOR_BUFFER_BYTES, Some(lease)).unwrap();
        assert!(matches!(cursor.data, Data::Spilled(..)));
        drop(session);
        drop(admin);

        let reopened = Admin::open(&path, Limits::default()).unwrap();
        assert_eq!(cursor.fetch(1, 1024 * 1024).unwrap()[0].values, [payload]);
        drop(reopened);
        drop(cursor);
        std::fs::remove_dir_all(path).unwrap();
    }
    fn buffered_cursor(rows: Vec<SqlRow>) -> Cursor {
        Cursor {
            data: Data::Buffered(rows.into()), clock: None, statement_tokens: 0,
            value_budget: relational::DEFAULT_MAX_VALUE_BYTES,
            wide_values: true,
            engine: None,
            tag: None, scope: None, meter: Default::default(), guard: None, closed: None,
        }
    }
    #[test]
    fn v2_pages_keep_caller_limit_and_admit_one_wide_row() {
        let row = |size| SqlRow { values: vec![Value::Text("x".repeat(size))], version: 0 };
        let rows = vec![row(64 * 1024); 256];
        // Cover both in-memory and disk-backed result pages.
        let (_, spilled) = relational::spill::streaming(|| relational::spill::statement(|| {
            let mut result = relational::SqlResult::default();
            relational::spill::RowBuffer::new(rows.clone())?.finish(&mut result)?;
            Ok(result)
        }).unwrap());
        let mut buffered = buffered_cursor(rows);
        let mut disk = buffered_cursor(Vec::new());
        disk.data = Data::Spilled(spilled.expect("16 MiB spills"), None);
        for cursor in [&mut buffered, &mut disk] {
            let mut total = 0;
            let mut pages = 0;
            while !cursor.is_exhausted() {
                let page = cursor.fetch(256, 1024 * 1024).unwrap();
                assert!(page.iter().map(row_size).sum::<usize>() <= 1024 * 1024);
                total += page.len(); pages += 1;
            }
            assert_eq!(total, 256);
            assert!(pages >= 16);
        }
        let mut cursor = buffered_cursor(vec![row(2 * 1024 * 1024), row(2 * 1024 * 1024)]);
        assert_eq!(cursor.fetch(256, 1024 * 1024).unwrap().len(), 1);
        assert_eq!(cursor.fetch(256, 1024 * 1024).unwrap().len(), 1);
        let (_, spilled) = relational::spill::streaming(|| relational::spill::statement(|| {
            let mut result = relational::SqlResult::default();
            relational::spill::RowBuffer::new(vec![row(2 * 1024 * 1024); 4])?.finish(&mut result)?;
            Ok(result)
        }).unwrap());
        cursor.data = Data::Spilled(spilled.unwrap(), None);
        for remaining in (0..4).rev() {
            assert_eq!(cursor.fetch(256, 1024 * 1024).unwrap().len(), 1);
            let Data::Spilled(rows, pending) = &cursor.data else { unreachable!() };
            assert!(pending.is_none(), "no second oversized row retained");
            assert_eq!(rows.len(), remaining);
        }
    }
    #[test]
    fn v2_zero_fetch_budget_stays_invalid() {
        for limit in [relational::MAX_ROW_BYTES, relational::DEFAULT_MAX_VALUE_BYTES] {
            let mut cursor = Cursor {
                data: Data::Buffered(VecDeque::from([SqlRow { values: vec![Value::Int(1)], version: 0 }])),
                clock: None, statement_tokens: 0, value_budget: limit, wide_values: true,
                engine: None,
                tag: None, scope: None, meter: Default::default(), guard: None, closed: None,
            };
            assert!(matches!(cursor.fetch(1, 0), Err(CoreError::Sql(SqlError::Limit))));
            assert_eq!(cursor.fetch(1, 9).unwrap()[0].values, vec![Value::Int(1)]);
        }
    }
    #[test]
    fn repeats_cursor_binding_on_the_same_catalog_image_and_rebinds_after_ddl() {
        let path = crate::test_artifact_root().join("cursor-execute-binding-reuse");
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        let mut session = admin.sql_session();
        session.execute("CREATE TABLE t(id bigint PRIMARY KEY)", &[], None).unwrap();
        session.execute("INSERT INTO t VALUES (1),(2)", &[], None).unwrap();
        let prepared = session.prepare("SELECT * FROM t WHERE id = $1").unwrap();

        let before = relational::cursor::query_binds();
        let mut first = session.open_cursor(&prepared, &[Value::Int(1)]).unwrap();
        assert_eq!(first.fetch(1, 1024).unwrap()[0].values, [Value::Int(1)]);
        drop(first);
        let mut second = session.open_cursor(&prepared, &[Value::Int(2)]).unwrap();
        assert_eq!(relational::cursor::query_binds() - before, 1);
        assert_eq!(second.fetch(1, 1024).unwrap()[0].values, [Value::Int(2)]);
        drop(second);

        session.execute("ALTER TABLE t ADD COLUMN payload text", &[], None).unwrap();
        let mut after_ddl = session.open_cursor(&prepared, &[Value::Int(1)]).unwrap();
        assert_eq!(relational::cursor::query_binds() - before, 2);
        assert_eq!(after_ddl.fetch(1, 1024).unwrap()[0].values, [Value::Int(1), Value::Null]);
        drop((after_ddl, session, admin));
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn idle_session_does_not_retain_pruned_cursor_image() {
        let path = crate::test_artifact_root().join("cursor-cache-prune");
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        let mut reader = admin.sql_session();
        let mut writer = admin.sql_session();
        writer.execute("CREATE TABLE t(id int PRIMARY KEY)", &[], None).unwrap();
        let p = reader.prepare("SELECT id FROM t").unwrap();

        let cursor = reader.open_cursor(&p, &[]).unwrap();
        let Data::Scan { image, .. } = &cursor.data else { panic!("streaming cursor") };
        let old_image = Arc::downgrade(image);
        drop(cursor);
        assert!(old_image.upgrade().is_some());

        writer.execute("ALTER TABLE t ADD COLUMN payload text", &[], None).unwrap();
        admin.test_v2().catalog().prune().unwrap();
        assert!(old_image.upgrade().is_none());
        // The reader has remained connected and made no observation of the DDL.
        assert!(reader.open_cursor(&p, &[]).unwrap().fetch(1, 1024).unwrap().is_empty());
        drop((reader, writer, admin));
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn private_cursor_image_is_never_cached_and_rollback_rebinds() {
        let path = crate::test_artifact_root().join("cursor-cache-rollback");
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        let mut session = admin.sql_session();
        session.execute("CREATE TABLE t(id int PRIMARY KEY)", &[], None).unwrap();
        session.execute("INSERT INTO t VALUES (1)", &[], None).unwrap();
        let p = session.prepare("SELECT * FROM t").unwrap();

        let before = relational::cursor::query_binds();
        drop(session.open_cursor(&p, &[]).unwrap());
        session.execute("BEGIN", &[], None).unwrap();
        session.execute("CREATE SCHEMA private_schema", &[], None).unwrap();
        let mut private = session.open_cursor(&p, &[]).unwrap();
        assert_eq!(private.fetch(1, 1024).unwrap()[0].values, [Value::Int(1)]);
        let Data::Scan { image, .. } = &private.data else { panic!("streaming cursor") };
        assert!(image.catalog_ext().schemas.contains_key("private_schema"));
        let private_image = Arc::downgrade(image);
        drop(private);
        // The transaction's private catalog snapshot retains this image until
        // rollback. Prove that the binding is not cached by opening again on
        // that same image and requiring another bind.
        let again = session.open_cursor(&p, &[]).unwrap();
        let Data::Scan { image, .. } = &again.data else { panic!("streaming cursor") };
        assert!(std::ptr::eq(private_image.as_ptr(), Arc::as_ptr(image)));
        assert_eq!(relational::cursor::query_binds() - before, 3);
        drop(again);

        session.execute("ROLLBACK", &[], None).unwrap();
        assert!(private_image.upgrade().is_none());
        let mut restored = session.open_cursor(&p, &[]).unwrap();
        assert_eq!(restored.fetch(1, 1024).unwrap()[0].values, [Value::Int(1)]);
        assert_eq!(relational::cursor::query_binds() - before, 4);
        drop(restored);

        // A catalog-changing transaction must also clear the slot at its end
        // if no cursor or metadata lookup observed its private image.
        session.execute("BEGIN", &[], None).unwrap();
        session.execute("CREATE SCHEMA temporary_schema", &[], None).unwrap();
        session.rollback();
        drop(session.open_cursor(&p, &[]).unwrap());
        assert_eq!(relational::cursor::query_binds() - before, 5);
        drop((session, admin));
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn closing_a_statement_releases_only_its_cached_binding() {
        let path = crate::test_artifact_root().join("cursor-cache-close");
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        let mut session = admin.sql_session();
        session.execute("CREATE TABLE t(id int PRIMARY KEY)", &[], None).unwrap();
        session.execute("INSERT INTO t VALUES (1)", &[], None).unwrap();
        let p = session.prepare("SELECT id FROM t").unwrap();
        let other = session.prepare("SELECT id FROM t").unwrap();

        let before = relational::cursor::query_binds();
        let mut existing = session.open_cursor(&p, &[]).unwrap();
        session.forget_cursor_plan(&other);
        drop(session.open_cursor(&p, &[]).unwrap());
        assert_eq!(relational::cursor::query_binds() - before, 1);
        session.forget_cursor_plan(&p);
        drop(session.open_cursor(&p, &[]).unwrap());
        assert_eq!(relational::cursor::query_binds() - before, 2);
        assert_eq!(existing.fetch(1, 1024).unwrap()[0].values, [Value::Int(1)]);

        drop((existing, session, admin));
        std::fs::remove_dir_all(path).unwrap();
    }

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
        let db = admin.test_v2();
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
        drop(db);
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
        let db = admin.test_v2();
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
        drop(db);
        drop(admin);
        std::fs::remove_dir_all(path).unwrap();
    }
}
