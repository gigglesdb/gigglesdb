//! SQL bridge: relational statement checks publish through v2 catalog and row sinks.
use super::*;
use engine::{catalog::{CatalogParts, CatalogSnapshot}, txn::Isolation};
use engine::{catalog::TableEntry, codec, kv::Keyspace, locks::LockMode, txn::Transaction};
use relational::{
    SqlError, SqlRow, TableDef,
    row_sink::{DdlRows, RowSink},
};
use std::{collections::BTreeSet, ops::Bound};
/// Encoded primary-key bounds of a read (GD-0075 P8-0).
type KeyBounds = (Bound<Vec<u8>>, Bound<Vec<u8>>);
/// One side of an integer key range: (value, inclusive).
type KeySide = (i64, bool);
#[path = "v2_sql_sequences.rs"]
pub(super) mod sql_sequences;
#[path = "v2_sql_xact.rs"]
mod sql_xact;
#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "v2_xact_tests.rs"]
mod xact_tests;
#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "v2_xact_clock_tests.rs"]
mod v2_xact_clock_tests;
#[path = "v2_reduction.rs"]
mod reduction;
#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "v2_sql_comment_tests.rs"]
mod comment_tests;

#[cfg(test)]
thread_local! {
    static UPSERT_SHARED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static UPSERT_UPGRADE: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static FK_DELETE_RETENTION: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
    static FK_DELETE_SHARED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static FK_DELETE_UPGRADE: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

impl SqlSession {

    /// BEGIN without parsing SQL; legacy and task sessions retain the SQL path.
    pub fn begin_block(&mut self) -> Result<(), CoreError> {
        self.control_block(false)
    }

    /// COMMIT without parsing SQL. Like SQL COMMIT, an aborted block is refused.
    pub fn commit_block(&mut self) -> Result<(), CoreError> {
        self.control_block(true)
    }

    /// Clocked counterpart of the direct transaction controls.
    pub fn control_block_at(&mut self, commit: bool, clock: &relational::StatementClock) -> Result<(), CoreError> {
        self.guard_clock(clock)?;
        let result = self.control_block(commit);
        if result.is_ok() && !commit && clock.unavailable.is_none() {
            self.transaction_clock_us = Some(clock.transaction_us);
        }
        result
    }

    /// Immutable transaction-control syntax for caller authorization, without SQL parsing.
    pub fn block_control(commit: bool) -> relational::Prepared {
        let mut prepared = if commit { relational::Prepared::commit_engine(0, 0) }
        else { relational::Prepared::begin_engine(0, 0) };
        prepared.mark_implicit_authorization_block(commit);
        prepared
    }

    /// Whether transaction controls can bypass the legacy SQL engine.
    pub fn has_native_transaction_blocks(&self) -> bool {
        matches!(self.admin.inner.backend, Backend::V2(_))
    }

    fn control_block(&mut self, commit: bool) -> Result<(), CoreError> {
        self.refuse_task_session()?;
        if !self.has_native_transaction_blocks() {
            return self.execute(if commit { "COMMIT" } else { "BEGIN" }, &[], None).map(drop);
        }
        // Shares execute_v2's copy/abort/ticket/cancellation/error cleanup guards.
        self.native_implicit_control(!commit, Holder::Transaction)
    }

    pub(crate) fn begin_v2_transaction(
        &mut self, db: &EngineLease,
    ) -> Result<(), CoreError> {
        let lease = db.child(Holder::Transaction)?;
        self.begin_v2_transaction_owned(lease)
    }

    pub(super) fn begin_v2_transaction_owned(&mut self, db: EngineLease) -> Result<(), CoreError> {
        let inner = self.admin.inner.clone();
        if self.v2_transaction.is_some() {
            return Err(CoreError::Sql(SqlError::Transaction));
        }
        let mut token = [0u8; 16];
        getrandom::fill(&mut token).map_err(io)?;
        let mut txn = db.begin(Isolation::ReadCommitted);
        self.sequence_baseline = db
            .catalog()
            .snapshot(&mut txn)?
            .parts.ext
            .sequences
            .iter()
            .map(|(&id, def)| (id, def.incarnation))
            .collect();
        // The baseline read used a READ COMMITTED snapshot; the
        // requested level takes its own at the first statement.
        txn.set_isolation(self.isolation.engine())?;
        inner
            .sql_transactions
            .try_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                if n < relational::MAX_TRANSACTIONS {
                    Some(n + 1)
                } else {
                    None
                }
            })
            .map_err(|_| CoreError::Busy)?;
        txn.attach_lease(db);
        self.v2_transaction = Some(txn);
        self.transaction_id = Some(u128::from_be_bytes(token));
        Ok(())
    }

    pub(super) fn commit_v2_transaction(
        &mut self, db: &EngineLease, ticket: &mut Ticket<'_>,
    ) -> Result<(), CoreError> {
        let inner = self.admin.inner.clone();
        let txn = self.v2_transaction.take().ok_or(CoreError::Sql(SqlError::Transaction))?;
        inner.sql_transactions.fetch_sub(1, Ordering::AcqRel);
        self.publish_v2_sql(db, &inner, txn, true, ticket)
    }

    // Preserve explicit transaction cleanup before acknowledgement (including a
    // lost response), while sharing publication with statement autocommit.
    fn publish_v2_sql(&mut self, db: &EngineLease, inner: &Inner, txn: Transaction, explicit: bool, ticket: &mut Ticket<'_>) -> Result<(), CoreError> {
        // Statement autocommit historically samples the crash point before attaching.
        let point = if explicit { None } else { Some(*inner.crash.lock().map_err(|_| CoreError::Fenced)?) };
        let mut txn = txn;
        let validation = (if explicit {
            validate_pending(db, &mut txn, &mut self.deferral)
        } else { Ok(()) })
            .and_then(|()| {
                if let Some(guard) = self.xact.lock()
                    .map_err(|_| CoreError::Fenced)?.take()
                {
                    txn.attach_xact(guard)?;
                }
                Ok(())
            });
        if validation.is_err() {
            if explicit { self.rollback(); }
            return validation;
        }
        let point = match point {
            Some(point) => point,
            None => *inner.crash.lock().map_err(|_| CoreError::Fenced)?,
        };
        let eligible = txn.local_durable_tail_eligible()
            && self.pending_sequences.lock().map_err(|_| CoreError::Fenced)?.is_empty();
        let committed = (|| {
            // Settlement can wait for a sequence slot or durable reservation.
            // Finish it before publishing both statements and transaction blocks.
            sql_sequences::settle(db, &self.sequence_unsettled)?;
            if explicit {
                sql_sequences::stage_pending(db, &mut txn, &self.pending_sequences)?;
            }
            let mut release_admission = || ticket.release();
            commit_sql(db, txn, point, eligible.then_some(&mut release_admission))?;
            let installed = if explicit {
                self.install_committed_sequence_counters(db).map_err(|error| {
                    CoreError::Storage(format!("could not install committed sequence counters: {error}"))
                })
            } else {
                Ok(())
            };
            Ok::<_, CoreError>(installed)
        })();
        if explicit { self.rollback_without_settling(); }
        let installed = committed?;
        // Publication reached the acknowledgement seam even if cache installation failed.
        let acknowledged = acknowledge_sql_commit(point);
        installed.and(acknowledged)
    }

    fn install_committed_sequence_counters(&self, db: &v2::V2Db) -> Result<(), CoreError> {
        // Pending incarnations enter the live cache only after durable ack.
        // A session finding no live entry may already have loaded and advanced
        // the durable counter. Keep any such same-incarnation entry.
        for (id, counter) in self.pending_sequences
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
        {
            let slot = {
                let mut live = db.sql_counters.lock().map_err(|_| CoreError::Fenced)?;
                live.entry(*id).or_default().clone()
            };
            // Cancellation after publication cannot prevent cache maintenance.
            let mut current = slot.counter.lock().map_err(|_| CoreError::Fenced)?;
            if db.catalog().current_local_view()?.sequences.get(id)
                .is_some_and(|def| def.incarnation == counter.incarnation)
                && current.counter.as_ref().is_none_or(|current| current.incarnation != counter.incarnation) {
                sql_sequences::install_and_wake(&slot, &mut current, counter.clone())?;
            }
        }
        Ok(())
    }

    #[cfg(feature = "engine-rocksdb")]
    pub(super) fn visit_native_range_v2<E>(
        &mut self, db: &EngineLease, table: &TableDef,
        lo: Bound<Vec<u8>>, hi: Bound<Vec<u8>>,
        visit: &mut impl FnMut(&SqlRow) -> Result<(), E>, map: fn(CoreError) -> E,
    ) -> Result<(), E> {
        sql_sequences::settle(db, &self.sequence_unsettled).map_err(map)?;
        let _ticket = self.admin.inner.enter_sql().map_err(map)?;
        let txn = self.v2_transaction.as_mut()
            .ok_or_else(|| map(CoreError::Sql(SqlError::Transaction)))?;
        let entry = TableEntry { primary_index_birth: None, def: table.clone(), heap: false, columns_ext: BTreeMap::new() };
        // User-defined SQL types can order differently from their storage class.
        let (lo, hi) = if let [index] = table.primary_key.as_slice()
            && (!matches!(lo, Bound::Unbounded) || !matches!(hi, Bound::Unbounded))
            && db.catalog().snapshot(txn).map_err(map)?.parts.ext.columns
                .get(&table.columns[*index].id).is_some_and(|c| c.type_id.is_some())
        {
            (Bound::Unbounded, Bound::Unbounded)
        } else { (lo, hi) };
        for pair in db.rows.scan(txn, &entry, lo, hi,
            engine::kv::Direction::Forward, None).map_err(map)? {
            crate::cancellation::check().map_err(map)?;
            let (_, row) = pair.map_err(map)?;
            visit(&SqlRow { values: row.values, version: row.version })?;
        }
        Ok(())
    }

    pub(crate) fn native_index_access(&mut self, table: &TableDef,
        predicates: &[(String, String, relational::Value)]) -> Result<crate::operations::NativeAccess, CoreError> {
        let result = (|| {
            if !self.is_v2_engine() {
                return Ok(crate::operations::NativeAccess::Full);
            }
            let db = self.v2_lease(Holder::Sql)?;
            let txn = self.v2_transaction.as_mut().ok_or(CoreError::Sql(SqlError::Transaction))?;
            let catalog = db.catalog().snapshot(txn)?;
            Ok(SqlRows::index_range(&catalog.parts, table, predicates)
                .map_or(crate::operations::NativeAccess::Full, |(d, lo, hi)| crate::operations::NativeAccess::Index(d, lo, hi)))
        })();
        if result.is_err() { self.abort(); }
        result
    }

    #[cfg(feature = "engine-rocksdb")]
    pub(super) fn visit_native_index_v2<E>(
        &mut self, db: &EngineLease, table: &TableDef,
        range: (&relational::secondary_index::Descriptor, Bound<Vec<u8>>, Bound<Vec<u8>>),
        visit: &mut impl FnMut(&SqlRow) -> Result<(), E>, map: fn(CoreError) -> E,
    ) -> Result<(), E> {
        sql_sequences::settle(db, &self.sequence_unsettled).map_err(map)?;
        let _ticket = self.admin.inner.enter_sql().map_err(map)?;
        let txn = self.v2_transaction.as_mut().ok_or_else(|| map(CoreError::Sql(SqlError::Transaction)))?;
        let catalog = db.catalog().snapshot(txn).map_err(map)?;
        let entry = TableEntry {
            primary_index_birth: catalog.parts.ext.primary_index_birth.get(&table.id).copied(),
            def: table.clone(), heap: catalog.parts.heap.contains(&table.id), columns_ext: BTreeMap::new(),
        };
        // The engine callback has a CoreError signature; preserve native filter errors separately.
        let mut error = None;
        let result = engine::indexes::visit(txn, &entry, range.0, range.1, range.2, &mut |_, row| {
            crate::cancellation::check()?;
            if let Err(e) = visit(&SqlRow { values: row.values, version: row.version }) {
                error = Some(e);
                return Err(CoreError::Sql(SqlError::Unsupported));
            }
            Ok(())
        });
        if let Some(error) = error { return Err(error); }
        result.map_err(map)
    }

    #[cfg(feature = "engine-rocksdb")]
    pub(super) fn visit_native_key_v2<E>(
        &mut self, db: &EngineLease, table: &TableDef, key: &[relational::Value],
        visit: &mut impl FnMut(&SqlRow) -> Result<(), E>, map: fn(CoreError) -> E,
    ) -> Result<(), E> {
        sql_sequences::settle(db, &self.sequence_unsettled).map_err(map)?;
        let _ticket = self.admin.inner.enter_sql().map_err(map)?;
        crate::cancellation::check().map_err(map)?;
        let txn = self.v2_transaction.as_mut()
            .ok_or_else(|| map(CoreError::Sql(SqlError::Transaction)))?;
        let entry = TableEntry { primary_index_birth: None, def: table.clone(), heap: false, columns_ext: BTreeMap::new() };
        if let Some(row) = db.rows.get(txn, &entry, key).map_err(map)? {
            visit(&SqlRow { values: row.values, version: row.version })?;
        }
        Ok(())
    }

    pub(super) fn insert_sequence_callback(
        &self,
        db: &EngineLease,
        image: Arc<relational::Relational>,
    ) -> Result<relational::clock::SequenceCallback, CoreError> {
        let guard = sql_sequences::Guard::new(&self.sequence_authority, self.admin.roles(),
            self.v2_transaction.as_ref(), &image);
        Ok(sql_sequences::callback(CallbackLease::new(db, self.v2_transaction.as_ref()), image, self.sequence_baseline.clone(),
            self.pending_sequences.clone(), self.sequence_values.clone(), self.sequence_unsettled.clone(), self.search_path.clone(),
            Arc::new(guard), false))
    }

    // Typed scans deliberately retain the active statement snapshot. In
    // particular, the completeness forward/max probes must never refresh it.
    #[cfg(feature = "engine-rocksdb")]
    pub(super) fn scan_v2_engine(
        &mut self, db: &EngineLease, scan: relational::Scan,
    ) -> Result<relational::SqlResult, CoreError> {
        let _ticket = self.admin.inner.enter_sql()?;
        crate::cancellation::check()?;
        if self.aborted { return Err(CoreError::Sql(SqlError::Aborted)); }
        let txn = self.v2_transaction.as_mut()
            .ok_or(CoreError::Sql(SqlError::Transaction))?;
        let catalog = db.catalog().snapshot(txn)?;
        let mut image = (*catalog.image().map_err(CoreError::Sql)?).clone();
        let prepared = relational::Prepared::scan(scan,
            u128::from_be_bytes(db.meta.domain), &image).map_err(CoreError::Sql)?;
        let result = image.execute_with_sink(&prepared, &[], None, None,
            &mut SqlRows { db, txn, parts: &catalog.parts, catalog: &catalog, writing: false,
                conflict_floor: None, prepared: &prepared, params: &[], effects: None, scope: None },
            u64::MAX, None)?;
        crate::cancellation::check()?;
        sql_sequences::settle(db, &self.sequence_unsettled)?;
        Ok(result)
    }

    pub(super) fn prepare_v2(
        &mut self,
        sql: &str,
        db: &EngineLease,
    ) -> Result<relational::Prepared, CoreError> {
        relational::assert_sql_frontend_allowed();
        let result = relational::Prepared::parse(sql, u128::from_be_bytes(db.meta.domain), 0)
            .map_err(CoreError::Sql)
            .and_then(|mut p| {
                if self.aborted && !p.allows_aborted_transaction() {
                    return Err(CoreError::Sql(relational::SqlError::Aborted));
                }
                p.transaction = self.transaction_id;
                p.engine_generation = Some(db.prepared_generation());
                p.set_schema_context(self.search_path.clone(), self.schema_user.clone());
                if self.authorization_bindings && self.task.is_none() {
                    p.enable_sql_authorization_binding();
                }
                // Match legacy preparation: authority sees bound dependency names,
                // while semantic errors remain deferred until after authorization.
                if let Ok(bound) = self.bind_relation_context_leased(&p, Some(db)) {
                    p = bound;
                }
                Ok(p)
            });
        if result.is_err() {
            self.abort();
        }
        result
    }

    pub(super) fn normalize_copy_v2(
        &mut self,
        db: &EngineLease,
        prepared: &relational::Prepared,
        input: relational::CopyInput,
        clock: &relational::StatementClock,
    ) -> Result<(relational::Prepared, Vec<Vec<relational::Value>>, u64), CoreError> {
        if let Err(error) = Self::check_prepared_generation(prepared, Some(db)) {
            // The stale plan cannot settle sequence obligations on this engine.
            self.abort_transaction(false);
            return Err(error);
        }
        self.bind_sequence_generation(db);
        let result = (|| {
            let inner = self.admin.inner.clone();
            let _ticket = inner.enter_sql()?;
            self.guard_clock(clock)?;
            crate::cancellation::check()?;
            if self.aborted {
                return Err(CoreError::Sql(SqlError::Aborted));
            }
            if prepared.domain != u128::from_be_bytes(db.meta.domain)
                || prepared.epoch != 0
                || (prepared.transaction.is_some() && prepared.transaction != self.transaction_id)
            {
                return Err(CoreError::Sql(SqlError::StaleSchema));
            }
            let mut state = db.metadata_state(self.v2_transaction.as_ref())?;
            state.sql.epoch = 0;
            let image = Arc::new(state.sql.clone());
            let guard = sql_sequences::Guard::new(&self.sequence_authority, self.admin.roles(),
                self.v2_transaction.as_ref(), &image);
            let callback = sql_sequences::callback(CallbackLease::new(db, self.v2_transaction.as_ref()), image.clone(),
                self.sequence_baseline.clone(), self.pending_sequences.clone(),
                self.sequence_values.clone(), self.sequence_unsettled.clone(), self.search_path.clone(), Arc::new(guard), false);
            let allowed = self.sequence_tables.clone();
            let path = self.search_path.clone();
            let callback = Arc::new(move |operation: &str, text: &str, value, called, origin| {
                if let Some(allowed) = &allowed {
                    if allowed.is_empty() { return Err(CoreError::Policy(Error::Scope)); }
                    let name = sequences::sequence_name(text).map_err(CoreError::Sql)?;
                    let name = image.resolve_relation_key(&name, &path);
                    let definition = image.catalog_ext().sequences.values().find(|s| s.name == name)
                        .ok_or(CoreError::Sql(SqlError::UnknownTable))?;
                    let owner = definition.owned_by.and_then(|(id, _)| image.tables.values().find(|t| t.id == id));
                    if !allowed.contains(&name) && owner.is_none_or(|table| !allowed.contains(&image.table_key(table))) {
                        return Err(CoreError::Policy(Error::Scope));
                    }
                }
                callback(operation, text, value, called, origin)
            });
            let context = relational::clock::ExecutionClock::new(clock)
                .with_text_settings(self.date_style, self.bytea_output)
                .with_extra_float_digits(self.extra_float_digits)
                .with_sequences(callback)
                .with_xact(sql_xact::callback(
                    CallbackLease::new(db, self.v2_transaction.as_ref()),
                    self.xact.clone(),
                    self.v2_transaction.as_ref().map(|txn| txn.id()),
                ));
            let _volatile = relational::volatile::Scope::enter(&self.volatile);
            let output = state.sql.normalize_copy(prepared, input, Some(&context));
            crate::cancellation::check()?;
            if let Some(error) = context.take_error() {
                return Err(error);
            }
            output.map_err(CoreError::Sql)
        })();
        // Successful normalized rows go to COPY FROM ingest, never to a client;
        // COPY reports a count, and its statement end or commit settles them.
        if result.is_err() {
            // abort() settles best-effort; surface a settle failure instead of a
            // value-bearing error whose value is not yet durably covered.
            let settled = sql_sequences::settle(db, &self.sequence_unsettled);
            self.abort();
            settled?;
        }
        result
    }

    pub(super) fn execute_v2(
        &mut self,
        db: &EngineLease,
        p: &relational::Prepared,
        params: &[relational::Value],
        expected_version: Option<u64>,
        clock: Option<&relational::clock::ExecutionClock>,
        check_reply: Option<&mut SqlReplyCheck<'_>>,
    ) -> Result<relational::SqlResult, CoreError> {
        Self::check_prepared_generation(p, Some(db))?;
        let autocommit =
            self.v2_transaction.is_none() && self.protected.is_none() && p.control().is_none();
        let result = self.execute_v2_inner(db, p, params, expected_version, clock, check_reply);
        if result.is_ok() && (autocommit || p.control() == Some("commit")) {
            // publish_v2_sql settled all obligations before submitting the commit.
            return result;
        }
        // Result columns also identify spilled/streamed rows absent from rows.
        let should_settle = result.is_err()
            || result.as_ref().is_ok_and(|result| !result.columns.is_empty() || !result.rows.is_empty())
            || self.v2_transaction.is_none()
            || p.control() == Some("rollback");
        if should_settle && let Err(error) = sql_sequences::settle(db, &self.sequence_unsettled)
        {
            self.abort();
            if !result.as_ref().is_err_and(|error| crate::sqlstate(error) == "08007") {
                return Err(error);
            }
        }
        result
    }

    #[inline(never)]
    fn statement_sequence_guard(
        authority: &SequenceAuthority,
        roles: roles::RoleCatalog,
        db: &EngineLease,
        txn: &Transaction,
        image: &relational::Relational,
        pin_images: bool,
        execution: Option<&mut sql_sequences::DefaultExecution>,
    ) -> Arc<sql_sequences::Guard> {
        let mut guard = sql_sequences::Guard::new(authority, roles, Some(txn), image);
        if pin_images && db.cluster_services().is_none() {
            guard.pin_default_images(txn);
        }
        if let Some(execution) = execution {
            return execution.bind(db, txn, guard);
        }
        Arc::new(guard)
    }

    fn execute_v2_inner(
        &mut self,
        db: &EngineLease,
        p: &relational::Prepared,
        params: &[relational::Value],
        expected_version: Option<u64>,
        clock: Option<&relational::clock::ExecutionClock>,
        check_reply: Option<&mut SqlReplyCheck<'_>>,
    ) -> Result<relational::SqlResult, CoreError> {
        use relational::{SqlError as E, V2Class};
        p.clear_view_diagnostics();
        // R3-308: where a task read's accepted rows go (pending `rows_read`);
        // the protected commit's reads are not disclosure.
        let read_state = if self.protected.is_none() { self.task_state.clone() } else { None };
        if self
            .copy_input
            .as_ref()
            .is_some_and(|input| !input.executing)
        {
            self.abort();
            return Err(CoreError::Sql(E::Transaction));
        }
        if self.aborted && !p.allows_aborted_transaction() {
            return Err(CoreError::Sql(E::Aborted));
        }
        let inner = self.admin.inner.clone();
        let mut ticket = match inner.enter_sql() {
            Ok(ticket) => ticket,
            Err(error) => {
                self.abort();
                return Err(error);
            }
        };
        let cancelled = || crate::cancellation::check().is_err() || db.check().is_err();
        let result = (|| {
            crate::cancellation::check()?;
            if p.domain != u128::from_be_bytes(db.meta.domain)
                || p.epoch != 0
                || (p.transaction.is_some() && p.transaction != self.transaction_id)
            {
                return Err(CoreError::Sql(E::StaleSchema));
            }
            let unsupported = |feature| CoreError::EngineUnsupported {
                engine: "v2",
                feature,
            };
            match p.v2_class() {
                V2Class::Control(control) => {
                    if !params.is_empty() || expected_version.is_some() {
                        return Err(CoreError::Sql(E::Parameters));
                    }
                    match control {
                        "begin" => {
                            self.begin_v2_transaction(db)?;
                        }
                        "savepoint" | "release" | "rollback_to" => {
                            let txn = self
                                .v2_transaction
                                .as_mut()
                                .ok_or(CoreError::Sql(E::NoActiveTransaction))?;
                            let name = p.savepoint_name().expect("named transaction control");
                            if control == "savepoint" {
                                if self.v2_savepoints.len() >= 64 {
                                    return Err(CoreError::Sql(E::Limit));
                                }
                                self.copy_freeze.push(Default::default());
                                self.v2_savepoints.push((
                                    name.to_owned(),
                                    txn.savepoint(),
                                    self.deferral.clone(),
                                ));
                            } else {
                                let index = self
                                    .v2_savepoints
                                    .iter()
                                    .rposition(|(saved, _, _)| saved == name)
                                    .ok_or(CoreError::Sql(E::InvalidSavepoint))?;
                                let mark = self.v2_savepoints[index].1.clone();
                                if control == "release" {
                                    txn.release(mark)?;
                                    self.release_copy_freeze(index);
                                    self.v2_savepoints.truncate(index);
                                } else {
                                    txn.rollback_to_releasing_locks(&mark)?;
                                    self.deferral = self.v2_savepoints[index].2.clone();
                                    self.rollback_copy_freeze(index);
                                    self.v2_savepoints.truncate(index + 1);
                                    self.aborted = false;
                                }
                            }
                        }
                        "commit" => {
                            self.commit_v2_transaction(db, &mut ticket)?;
                            return Ok(Default::default());
                        }
                        "rollback" => {
                            if self.v2_transaction.is_none() {
                                return Err(CoreError::Sql(E::Transaction));
                            }
                            self.rollback();
                        }
                        _ => unreachable!("v2 classification limits transaction controls"),
                    }
                    Ok(Default::default())
                }
                class @ (V2Class::Ddl | V2Class::Dml | V2Class::Read) => {
                    let ddl = matches!(class, V2Class::Ddl);
                    let writing = !matches!(class, V2Class::Read);
                    if ddl && expected_version.is_some() {
                        return Err(CoreError::Sql(E::Parameters));
                    }
                    let mut implicit = None;
                    let protected = self.protected.is_some();
                    let explicit = self.v2_transaction.is_some() && !protected;
                    let (txn, mut protected_effects) = match (&mut self.protected, &mut self.v2_transaction) {
                        (Some(run), _) => (&mut run.txn, Some(&mut run.effects)),
                        (None, Some(txn)) => (txn, None),
                        (None, None) => (implicit.insert(db.begin(self.isolation.engine())), None),
                    };
                    if p.row_lock_command().is_some() && matches!(txn.services(), engine::txn::TxnServices::Cluster(_)) {
                        return Err(CoreError::Sql(E::Unsupported));
                    }
                    if !self.copy_input.as_ref().is_some_and(|input| input.started) {
                        txn.begin_statement();
                    }
                    let mut freeze_marks = Vec::new();
                    let mark = txn.statement_savepoint();
                    let result = (|| {
                        let catalog_lock_mode = if ddl {
                            LockMode::AccessExclusive
                        } else {
                            LockMode::RowExclusive
                        };
                        // DDL stages a catalog-wide image (including schema cascades and
                        // new FK edges), so retain its exclusive catalog barrier. DML
                        // shares this sentinel regardless of whether any FK exists.
                        if writing {
                            // Phase 11: the cluster's DDL lock before the local barrier, so a
                            // DDL that is its transaction's first write never holds Table(0)
                            // while another gateway's DDL runs (after earlier DML it holds
                            // Table(0) RowExclusive while waiting; the deadlock detector then
                            // breaks any cycle).
                            if ddl {
                                txn.ddl_begin(&cancelled)?;
                            }
                            txn.acquire_lock(
                                engine::locks::LockKey::Table(0),
                                catalog_lock_mode,
                                crate::task::limits::lock_deadline(),
                                &cancelled,
                            ).map_err(crate::task::limits::lock_error)?;
                            // Refresh AFTER waiting: DDL may have introduced an FK.
                            if !self.copy_input.as_ref().is_some_and(|input| input.started) {
                                txn.begin_statement();
                            }
                        }
                        // Resolve and lock relations before choosing the final statement
                        // snapshot/image; readers retain MVCC snapshot visibility.
                        let catalog = db.catalog().snapshot(txn)?;
                        let parts = &catalog.parts;
                        // Bind from the original references, preserving explicit public
                        // qualification even when another schema shadows the bare name.
                        let binding = catalog.image()
                        .map_err(|error| match error {
                            E::Unsupported => CoreError::Sql(E::Unsupported),
                            error => {
                                CoreError::Corrupt(format!("invalid stored v2 catalog: {error:?}"))
                            }
                        })?;
                        let bound = p.bind_schemas(&binding).map_err(CoreError::Sql)?;
                        let target_name = bound.storage_table().map(str::to_owned);
                        if writing && !ddl && target_name.as_ref().is_some_and(|n| binding.view_names.contains_key(n)) { return Err(CoreError::Sql(E::Unsupported)); }
                        let mut names = bound.read_tables().into_iter().collect::<BTreeSet<_>>();
                        let truncating = bound.command() == "TRUNCATE TABLE";
                        if truncating {
                            names.extend(bound.truncate_tables(&binding).map_err(CoreError::Sql)?);
                        }
                        if let Some(name) = &target_name {
                            names.insert(name.clone());
                        }
                        let write_ids = bound.write_tables().into_iter().filter_map(|(name, _)| parts.tables.get(&name).map(|t| t.id)).collect::<BTreeSet<_>>();
                        if bound.write_tables().iter().any(|(name, _)| binding.view_names.contains_key(name)) {
                            return Err(CoreError::Sql(E::Unsupported));
                        }
                        let target_id = target_name
                            .as_ref()
                            .and_then(|name| parts.tables.get(name))
                            .map(|t| t.id);
                        // Local FK witnesses use RowKey locks. Only writing referential
                        // actions retain the full component's Share/RowExclusive locks.
                        // Cluster and DDL keep their existing component protocol.
                        let component_locks = ddl
                            || matches!(txn.services(), engine::txn::TxnServices::Cluster(_))
                            || bound.write_tables().iter().any(|(name, command)| {
                                let Some(table) = parts.tables.get(name) else { return false; };
                                parts.ext.constraints.values().flatten().any(|c| {
                                    let relational::catalog_ext::ConstraintKind::ForeignKey {
                                        ref_table, on_delete, on_update, ..
                                    } = &c.kind else { return false; };
                                    let action = match *command {
                                        "DELETE" => on_delete,
                                        "UPDATE" => on_update,
                                        _ => return false,
                                    };
                                    *ref_table == table.id && matches!(action,
                                        relational::catalog_ext::Action::Cascade
                                        | relational::catalog_ext::Action::SetNull
                                        | relational::catalog_ext::Action::SetDefault)
                                })
                            });
                        let mut connected = target_id.into_iter().chain(write_ids.iter().copied()).collect::<BTreeSet<_>>();
                        if writing && component_locks {
                            let mut neighbors = BTreeMap::<_, Vec<_>>::new();
                            for (&child, constraints) in &parts.ext.constraints {
                                for constraint in constraints {
                                    if let relational::catalog_ext::ConstraintKind::ForeignKey {
                                        ref_table, ..
                                    } = &constraint.kind {
                                        neighbors.entry(child).or_default().push(*ref_table);
                                        neighbors.entry(*ref_table).or_default().push(child);
                                    }
                                }
                            }
                            let mut pending: Vec<_> = connected.iter().copied().collect();
                            while let Some(table) = pending.pop() {
                                for &neighbor in neighbors.get(&table).into_iter().flatten() {
                                    if connected.insert(neighbor) {
                                        pending.push(neighbor);
                                    }
                                }
                            }
                        }
                        let row_lock_names = bound.row_lock_tables().into_iter().collect::<BTreeSet<_>>();
                        let mut locks = BTreeMap::new();
                        for name in &names {
                            if let Some(id) = binding.view_names.get(name).map(|(id, _)| id) {
                                locks.insert(*id, if ddl && target_name.as_ref() == Some(name) { LockMode::AccessExclusive } else { LockMode::AccessShare });
                            }
                            if let Some(table) = parts.tables.get(name) {
                                locks.insert(
                                    table.id,
                                    if truncating {
                                        LockMode::AccessExclusive
                                    } else if row_lock_names.contains(name) {
                                        LockMode::RowShare
                                    } else {
                                        LockMode::AccessShare
                                    },
                                );
                            }
                        }
                        if writing {
                            for id in connected {
                                locks.insert(
                                    id,
                                    if ddl {
                                        LockMode::AccessExclusive
                                    } else if Some(id) == target_id || write_ids.contains(&id) {
                                        LockMode::RowExclusive
                                    } else {
                                        LockMode::Share
                                    },
                                );
                            }
                        }
                        for id in bound.view_drop_locks(&binding, parts) {
                            locks.insert(id, LockMode::AccessExclusive);
                        }
                        // A single table-id order includes read sources and FK witnesses.
                        let table_names = parts
                            .tables
                            .iter()
                            .map(|(name, table)| (table.id, name))
                            .collect::<BTreeMap<_, _>>();
                        let exclusive: Vec<u64> = locks
                            .iter()
                            .filter(|(_, mode)| ddl && **mode == LockMode::AccessExclusive)
                            .map(|(id, _)| *id)
                            .collect();
                        for (id, mode) in locks {
                            if !table_names.contains_key(&id) {
                                txn.acquire_lock(engine::locks::LockKey::Table(id), mode, crate::task::limits::lock_deadline(), &cancelled).map_err(crate::task::limits::lock_error)?;
                                continue;
                            }
                            db.catalog()
                                .resolve(txn, table_names[&id], mode, crate::task::limits::lock_deadline(), &|| {
                                    crate::cancellation::check().is_err()
                                })
                                .map_err(|e| match e {
                                    engine::catalog::CatalogError::Core(e) => e,
                                    engine::catalog::CatalogError::Txn(e) => crate::task::limits::lock_error(e),
                                })?;
                        }
                        // A WITH statement can own several mutation targets.
                        if writing && !ddl {
                            for id in write_ids.iter().filter(|id| Some(**id) != target_id) {
                                txn.lock_sql_sentinel(*id, if txn.isolation() == Isolation::ReadCommitted && bound.row_lock_command().is_none() {
                                    LockMode::Exclusive
                                } else { LockMode::Shared }, &cancelled)?;
                            }
                        }
                        if writing
                            && !ddl
                            && let Some(table) =
                                target_name.as_ref().and_then(|name| parts.tables.get(name))
                        {
                            // Shared bounded writers still exclude every READ COMMITTED
                            // paged, unsupported conflict/action and COPY writer. Actual
                            // target rows are locked in physical order BEFORE refreshing.
                            // Expected-version point writes join this path under RC:
                            // the row lock precedes the fresh snapshot, so the point
                            // UPDATE/DELETE version checks see the latest committed row.
                            // One row is at most MAX_ROW_BYTES (64 KiB), so production
                            // retention cannot overflow and upgrade these requests.
                            let candidates = (self.copy_input.is_none()
                                && txn.isolation() == Isolation::ReadCommitted)
                                .then(|| bound.fk_delete_lock_keys(&binding, params)
                                    .or_else(|| bound.mutation_lock_keys(&binding, params, &parts.indexes)))
                                .flatten();
                            let append = self.copy_input.is_none()
                                && txn.isolation() == Isolation::ReadCommitted
                                && expected_version.is_none()
                                && bound.plain_values_append(&binding);
                            #[cfg(test)]
                            let candidates = if DISABLE_MUTATION_POINTS.get() { None } else { candidates };
                            #[cfg(test)]
                            if candidates.is_some() && bound.fk_delete_lock_keys(&binding, params).is_some() {
                                FK_DELETE_SHARED.set(FK_DELETE_SHARED.get() + 1);
                            }
                            let upsert = (expected_version.is_none()
                                && self.copy_input.is_none()
                                && txn.isolation() == Isolation::ReadCommitted).then(|| {
                                let rows = bound.upsert_lock_rows(&binding, &parts.indexes, params, clock)?;
                                let physical = rows.primary_keys.iter().map(|key| {
                                    let mut physical = table.id.to_be_bytes().to_vec();
                                    physical.extend(codec::pk_bytes(key).ok()?);
                                    Some(physical)
                                }).collect::<Option<BTreeSet<_>>>()?;
                                let witnesses = if let Some((descriptor, rows)) = rows.unique {
                                    // UNIQUE non-NULL keys do not include the PK. Use the
                                    // occupancy encoder with a PK-free projection so fresh
                                    // defaults stay unevaluated; discard its owner values.
                                    let mut projection = table.clone();
                                    projection.primary_key.clear();
                                    let mut keys = BTreeSet::new();
                                    for row in rows {
                                        let rows = BTreeMap::from([(Vec::new(), row)]);
                                        keys.extend(engine::indexes::entries(None, &projection, &rows,
                                            std::slice::from_ref(&descriptor)).ok()?.into_keys());
                                    }
                                    keys
                                } else { BTreeSet::new() };
                                Some((physical, witnesses))
                            }).flatten();
                            // REPEATABLE READ and SERIALIZABLE writers lock rows, not the
                            // table: lock_for_write fails with 40001 on any row changed
                            // since their fixed snapshot, so concurrent writers of other
                            // rows proceed, as in PostgreSQL (GD-0075 R5-76). READ
                            // COMMITTED paged writers re-read latest rows and need the
                            // exclusive sentinel.
                            let fixed_snapshot = txn.isolation() != Isolation::ReadCommitted
                                && expected_version.is_none()
                                && self.copy_input.is_none();
                            // C4 (R3-607): a protected run takes the Shared sentinel. The
                            // lock queue is FIFO (engine/locks.rs), so a run waiting for
                            // Exclusive behind one long application transaction would
                            // stall every later application writer of the table, against
                            // spec section 5. Row locks in `SqlRows::apply` and read-set
                            // validation at commit keep the run correct.
                            let collated = binding.has_nondeterministic_keys(table);
                            let shared = !collated && (fixed_snapshot || append || candidates.is_some() || upsert.is_some()
                                || protected || bound.row_lock_command().is_some());
                            txn.lock_sql_sentinel(table.id,
                                if shared { LockMode::Shared } else { LockMode::Exclusive },
                                &cancelled)?;
                            // Cluster FK hydration must follow the parent's Exclusive lock,
                            // including when the transaction keeps a fixed snapshot.
                            if fixed_snapshot
                                && matches!(txn.services(), engine::txn::TxnServices::Cluster(_))
                                && let Some(keys) = bound.fk_delete_lock_keys(&binding, params)
                            {
                                let physical = keys.iter().map(|key| {
                                    let mut physical = table.id.to_be_bytes().to_vec();
                                    physical.extend(codec::pk_bytes(key)?);
                                    Ok(physical)
                                }).collect::<Result<BTreeSet<_>, CoreError>>()?;
                                for key in physical {
                                    txn.lock_for_write(Keyspace::Rows, &key, crate::task::limits::lock_deadline(),
                                        &cancelled).map_err(crate::task::limits::lock_error)?;
                                }
                            }
                            let key_prelock = matches!(txn.services(), engine::txn::TxnServices::Local { .. })
                                && bound.mutation_changes_key(table, &parts.indexes);
                            if let Some(keys) = candidates {
                                let physical = keys.iter().map(|key| {
                                    let mut physical = table.id.to_be_bytes().to_vec();
                                    physical.extend(codec::pk_bytes(key)?);
                                    Ok(physical)
                                }).collect::<Result<BTreeSet<_>, CoreError>>()?;
                                let retention_limit = relational::scan::retention_limit();
                                #[cfg(test)]
                                let retention_limit = FK_DELETE_RETENTION.get().unwrap_or(retention_limit);
                                let mut retention = relational::scan::Retention::new(retention_limit);
                                let mut fits = true;
                                for key in physical {
                                    if key_prelock {
                                        txn.lock_row_write_group(table.id, &key[8..],
                                            crate::task::limits::lock_deadline(), &cancelled)
                                            .map_err(crate::task::limits::lock_error)?;
                                    }
                                    let target = txn.lock_for_write(Keyspace::Rows, &key, crate::task::limits::lock_deadline(),
                                        &cancelled).map_err(crate::task::limits::lock_error)?;
                                    if let Some(bytes) = target.current {
                                        // Locked targets cannot grow before the final
                                        // snapshot. Reuse the publisher's exact budget.
                                        let charge = (|| {
                                            let record = codec::decode_row(&key, &bytes)?;
                                            if record.tombstone || record.values.len() > table.columns.len() {
                                                return Err(CoreError::Corrupt("invalid v2 SQL row witness".into()));
                                            }
                                            let mut row = SqlRow { values: record.values, version: record.row_version };
                                            row.values.resize(table.columns.len(), relational::Value::Null);
                                            charge_mutation_point(table, &row, &mut retention)
                                        })();
                                        fits &= charge.unwrap_or(false);
                                    }
                                }
                                if !fits {
                                    #[cfg(test)]
                                    if bound.fk_delete_lock_keys(&binding, params).is_some() {
                                        FK_DELETE_UPGRADE.set(FK_DELETE_UPGRADE.get() + 1);
                                    }
                                    // The point publisher would scan pages. Upgrade
                                    // before taking its snapshot; never refresh mid-scan.
                                    txn.lock_sql_sentinel(table.id, LockMode::Exclusive,
                                        &cancelled)?;
                                }
                            }
                            if let Some((mut physical, witnesses)) = upsert {
                                #[cfg(test)]
                                UPSERT_SHARED.set(UPSERT_SHARED.get() + 1);
                                // Peek owners at a provisional snapshot, without staging.
                                txn.begin_statement();
                                for key in &witnesses {
                                    if let Some(owner) = txn.get(Keyspace::Index, key)? {
                                        let mut row = table.id.to_be_bytes().to_vec();
                                        row.extend(owner);
                                        physical.insert(row);
                                    }
                                }
                                for key in &physical {
                                    txn.lock_for_write(Keyspace::Rows, key, crate::task::limits::lock_deadline(),
                                        &cancelled).map_err(crate::task::limits::lock_error)?;
                                }
                                let mut upgrade = false;
                                for key in witnesses {
                                    let target = txn.lock_for_write(Keyspace::Index, &key, crate::task::limits::lock_deadline(),
                                        &cancelled).map_err(crate::task::limits::lock_error)?;
                                    if let Some(owner) = target.current {
                                        let mut row = table.id.to_be_bytes().to_vec();
                                        row.extend(owner);
                                        upgrade |= !physical.contains(&row);
                                    }
                                }
                                if upgrade {
                                    #[cfg(test)]
                                    UPSERT_UPGRADE.set(UPSERT_UPGRADE.get() + 1);
                                    txn.lock_sql_sentinel(table.id, LockMode::Exclusive,
                                        &cancelled)?;
                                }
                            }
                        }
                        // Phase 11: other gateways drain their writers and the readers of
                        // `exclusive` (as locals did above) before the snapshot is chosen.
                        if ddl {
                            txn.ddl_close(&exclusive, &cancelled)?;
                        }
                        // Choose ONE snapshot after every potentially blocking lock.
                        // No refresh occurs while staging any of the statement's rows.
                        if protected {
                            // C1/C2/C5: one fresh snapshot after every lock wait, then
                            // coarse Catalog tracking for the rest of the run. A later
                            // statement of a run that already restarted and wrote
                            // (the undo dry run's per-row restores, R3-802) keeps the
                            // run's snapshot; read validation at commit covers it.
                            if !(txn.snapshot_restarted() && txn.has_buffered()) {
                                txn.restart_snapshot()?;
                            }
                            txn.track_range(Keyspace::Catalog, Bound::Unbounded, Bound::Unbounded)?;
                        } else if !self.copy_input.as_ref().is_some_and(|input| input.started) {
                            txn.begin_statement();
                        }
                        let catalog = db.catalog().snapshot(txn)?;
                        let old = &catalog.parts;
                        if p.schema_ddl().is_some_and(|(name, _, create)| !create && old.sql_views.values().any(|v|
                            if v.schema == 0 { name == "public" } else { old.ext.schemas.get(name).is_some_and(|s| s.id == v.schema) })) {
                            return Err(CoreError::Sql(E::DependentObjects));
                        }
                        // This bounds every ID E1 DDL allocates per statement.
                        let ids = if ddl {
                            db.reserve_sql_ids(txn, 3 * relational::MAX_COLUMNS as u64 + 64)?
                        } else {
                            // DML/reads allocate no catalog objects. Derive only the
                            // validation ceiling, without a durable ID reservation.
                            let floor = catalog.allocation_floor().map_err(CoreError::Sql)?;
                            floor..floor
                        };
                        let versions = if matches!(class, V2Class::Dml) || p.is_create_as() {
                            if let Some(range) = self
                                .copy_input
                                .as_ref()
                                .and_then(|input| input.versions.clone())
                            {
                                range
                            } else {
                                let range = db.sequences.reserve_range(3, 0, 1_000_001)?;
                                if let Some(input) = self.copy_input.as_mut() {
                                    input.versions = Some(range.clone());
                                }
                                range
                            }
                        } else {
                            1..u64::MAX
                        };
                        #[cfg(test)]
                        if matches!(class, V2Class::Dml) {
                            AFTER_DML_VERSION_RESERVATION.with(|hook| {
                                let callback = hook.borrow_mut().take();
                                if let Some(callback) = callback { callback(); }
                            });
                        }
                        let catalog_image = catalog.image()
                        .map_err(|error| match error {
                            E::Unsupported => CoreError::Sql(E::Unsupported),
                            error => {
                                CoreError::Corrupt(format!("invalid stored v2 catalog: {error:?}"))
                            }
                        })?;
                        let mut image = catalog_image.catalog_image_for_statement(ids.start, versions.start);
                        p.check_extension_views(&image, old.sql_views.values().map(|v| (v.name.as_str(), v.select.as_str()))).map_err(CoreError::Sql)?;
                        let context = clock.map(|clock| {
                            let guard = Self::statement_sequence_guard(
                                &self.sequence_authority, self.admin.roles(), db, txn, &catalog_image,
                                self.authorization_bindings && self.copy_input.is_none(),
                                self.sequence_default_execution.as_mut(),
                            );
                            // DML holds Table(0) RowExclusive and DDL holds it AccessExclusive
                            // through execution, keeping committed sequence incarnations stable.
                            // COPY normalization and gateways use their live path.
                            let cache_defaults = writing && self.copy_input.is_none()
                                && db.cluster_services().is_none() && guard.can_pin_default_images(txn);
                            #[cfg(debug_assertions)]
                            debug_assert!(!cache_defaults || matches!(
                                txn.services(),
                                engine::txn::TxnServices::Local { locks, .. } if locks.holds_mode(
                                    txn.id(), engine::locks::LockKey::Table(0), catalog_lock_mode,
                                )
                            ));
                            clock.with_sequences(sql_sequences::callback(
                                CallbackLease::new(db, Some(txn)),
                                catalog_image.clone(),
                                self.sequence_baseline.clone(),
                                self.pending_sequences.clone(),
                                self.sequence_values.clone(),
                                self.sequence_unsettled.clone(),
                                self.search_path.clone(),
                                guard,
                                cache_defaults,
                            ))
                            .with_xact(sql_xact::callback(
                                CallbackLease::new(db, Some(txn)),
                                self.xact.clone(),
                                Some(txn.id()),
                            ))
                        });
                        let context = context.map(|clock| match self.privilege_callback.as_ref().filter(|_| p.has_privilege_inquiry()) {
                            Some(callback) => clock.with_privileges(callback.clone(), privilege_provider(
                                PrivilegeImage::Catalog(catalog_image.clone()), self.search_path.clone(), Some(txn), self.admin.roles(), Some(db))),
                            None => clock,
                        });
                        if let Some(input) = self.copy_input.as_mut() {
                            let table = bound.table().ok_or(CoreError::Sql(E::Unsupported))?;
                            if input.table.as_ref().is_some_and(|saved| saved != table) {
                                return Err(CoreError::Sql(E::Transaction));
                            }
                            input.table = Some(table.to_owned());
                            let floor = *input.floor.get_or_insert(versions.start);
                            image.copy_input_page(
                                &bound,
                                &mut SqlRows {
                                    conflict_floor: None,
                                    db,
                                    txn,
                                    parts: old,
                                    catalog: &catalog,
                                    writing,
                                    prepared: &bound,
                                    params,
                                    effects: None,
                                    scope: None,
                                },
                                floor..versions.start,
                                input.finishing,
                                &mut self.deferral,
                                context.as_ref(),
                            )?;
                            if input.finishing {
                                return Ok(Default::default());
                            }
                        }
                        // GD-0061 R3-305: a task statement's table read sees only the
                        // task's visible rows. It runs through the scoped row source;
                        // a read that does not use a row source is refused.
                        // R3-306: a task DML statement (the protected commit binds
                        // the executor's task for its run) reads its target through
                        // the same scope, so rows outside it are never matched or
                        // rewritten.
                        let scope = match &self.task {
                            Some(ctx) if !writing && !bound.read_tables().is_empty() => {
                                if !bound.task_source_read() {
                                    return Err(crate::task::TaskError::NotAllowedInTaskSession(
                                        crate::task::StatementClass::Other,
                                    )
                                    .into());
                                }
                                Some(crate::task::rows::scope(db, txn, &image, ctx)?)
                            }
                            Some(ctx) if writing && !ddl => {
                                Some(crate::task::rows::scope(db, txn, &image, ctx)?)
                            }
                            _ => None,
                        };
                        let meter = crate::task::rows::StatementMeter::default();
                        // Plain view reads also need the physical source: their
                        // expanded output must not inherit the legacy image cap.
                        let reads_view = names.iter().any(|name| binding.view_names.contains_key(name));
                        if !writing && (scope.is_some() || image.streaming_reduction(&bound) || reads_view) {
                            let source = reduction::Source::new(SqlRows {
                                conflict_floor: None, db, txn, parts: old, catalog: &catalog,
                                writing, prepared: &bound, params, effects: None, scope: None,
                            });
                            let scoped = scope.as_ref().map(|scope|
                                crate::task::rows::Scoped::new(&source, scope, &meter));
                            let result = image.execute_reduction(
                                p, params, expected_version, context.as_ref(),
                                scoped.as_ref().map_or(
                                    &source as &dyn relational::scan::RowSource,
                                    |s| s as &dyn relational::scan::RowSource));
                            if let Some(scoped) = &scoped {
                                crate::task::rows::record(meter.counts());
                                if let Some(state) = &read_state { state.add_meter(&meter); }
                                if let Some(error) = scoped.take_error() { return Err(error.into()); }
                            }
                            if let Some(error) = source.error.take() { return Err(error); }
                            if let Some(error) = context.as_ref().and_then(|c| c.take_error()) { return Err(error); }
                            return result.map_err(CoreError::Sql);
                        }
                        bound.check_view_drop(&image).map_err(CoreError::Sql)?;
                        let freeze_before = bound.copy_freeze_created(&image);
                        let result = if !bound.is_collation_ddl() && matches!(bound.command(), "COMMENT" | "SECURITY LABEL" | "CREATE VIEW" | "DROP VIEW") { Ok(Default::default()) } else { image.execute_with_sink(
                            p,
                            params,
                            expected_version,
                            context.as_ref(),
                            &mut SqlRows {
                                conflict_floor: None,
                                db,
                                txn,
                                parts: old,
                                catalog: &catalog,
                                writing,
                                prepared: &bound,
                                params,
                                effects: protected_effects.as_deref_mut(),
                                scope: scope.as_ref().map(|scope| (scope, &meter)),
                            },
                            versions.end,
                            if self.transaction_id.is_some() {
                                Some(&mut self.deferral)
                            } else {
                                None
                            },
                        ) };
                        if scope.is_some() {
                            crate::task::rows::record(meter.counts());
                            if let Some(state) = &read_state { state.add_meter(&meter); }
                        }
                        if let Some(error) = context.as_ref().and_then(|clock| clock.take_error()) {
                            return Err(error);
                        }
                        let result = result?;
                        if let Some(range) = self
                            .copy_input
                            .as_mut()
                            .and_then(|input| input.versions.as_mut())
                        {
                            range.start = image.catalog_parts().3;
                        }
                        if !ddl {
                            return Ok(result);
                        }
                        // RESTART WITH does not change the recorded START. Carry its
                        // seed separately, using the bound name and post-DDL START.
                        let restart_seed = bound.sequence_restart_seed(&image);
                        let (tables, ext, next_id, _) = image.catalog_parts();
                        if p.truncate_restarts_identity() || restart_seed.is_some() {
                            let mut values =
                                self.sequence_values.lock().map_err(|_| CoreError::Fenced)?;
                            for (&id, definition) in &ext.sequences {
                                if let Some(before) = old.ext.sequences.get(&id)
                                    && before.incarnation != definition.incarnation
                                {
                                    // The existing sequence key codec binds counters to an
                                    // incarnation. Retire the old reservation in the same
                                    // transaction so reopen never loads it against the new definition.
                                    txn.delete_counter(engine::catalog_codec::counter_key(
                                        id,
                                        before.incarnation,
                                    ))?;
                                    if let Some(value) =
                                        values.current.get(&(id, before.incarnation)).copied()
                                    {
                                        values.current.insert((id, definition.incarnation), value);
                                    }
                                }
                            }
                        }
                        if next_id > ids.end {
                            return Err(CoreError::Corrupt(
                                "v2 DDL exceeded ID reservation".into(),
                            ));
                        }
                        let checked = check_post_image(image.tables.clone(), ext.clone(), next_id)?;
                        let heap = tables
                            .values()
                            .filter(|def| def.hidden_rowid().is_some())
                            .map(|def| def.id)
                            .collect();
                        let indexes = image.secondary_descriptors_engine().to_vec();
                        let mut new = CatalogParts {
                            sql_views: old.sql_views.clone(),
                            comments: old.comments.iter()
                                .filter(|((relation, column), _)| {
                                    old.sql_views.get(relation).is_some_and(|v| *column == 0 || v.columns.iter().any(|c| c.id == *column)) || tables.values().any(|table| table.id == *relation
                                        && (*column == 0 || table.columns.iter().any(|c| c.id == *column)))
                                })
                                .map(|(owner, text)| (*owner, text.clone()))
                                .collect(),
                            // Labels are dropped with their column or table.
                            column_labels: old.column_labels.iter()
                                .filter(|((relation, column), _)| tables.values().any(|table| table.id == *relation
                                    && table.columns.iter().any(|c| c.id == *column)))
                                .map(|(owner, flags)| (*owner, *flags))
                                .collect(),
                            tables,
                            heap,
                            ext,
                            indexes,
                        };
                        bound.apply_comment(&image, &mut new).map_err(CoreError::Sql)?;
                        bound.apply_label(&image, &mut new).map_err(CoreError::Sql)?;
                        bound.apply_view(&image, &mut new, ids.start).map_err(CoreError::Sql)?;
                        txn.reclaim_after_commit |=
                            old.tables
                                .iter()
                                .any(|(name, table)| {
                                    new.tables.get(name).is_none_or(|next| next.id != table.id)
                                        && !new.tables.values().any(|next| next.id == table.id)
                                })
                                || {
                                    let new_indexes = new.indexes.iter().map(|index| index.id)
                                        .collect::<BTreeSet<_>>();
                                    old.indexes.iter().any(|index| !new_indexes.contains(&index.id))
                                };
                        let new = Arc::new(new);
                        db.catalog().apply_sql_diff(txn, old, new.clone(), checked, &ids, &|| {
                            crate::cancellation::check().is_err()
                        })?;
                        roles::engine::assign_created(txn, old, &new, self.ddl_owner.as_deref())?;
                        if let Some((id, value)) = restart_seed {
                            let definition = &new.ext.sequences[&id];
                            let counter = sequences::SeqCounter {
                                incarnation: definition.incarnation,
                                last_value: value,
                                is_called: false,
                                log_hw: value,
                                stamp: 0,
                            };
                            if self.transaction_id.is_some() {
                                // Keep just the final incarnation until COMMIT: repeated
                                // RESTARTs must not stage counters for superseded definitions.
                                self.pending_sequences
                                    .lock()
                                    .map_err(|_| CoreError::Fenced)?
                                    .insert(id, counter);
                            } else {
                                txn.put_counter(
                                    engine::catalog_codec::counter_key(id, definition.incarnation),
                                    engine::catalog_codec::encode_counter(&counter, definition)?,
                                )?;
                            }
                        }
                        freeze_marks = bound.copy_freeze_marks(&image, freeze_before);
                        Ok(result)
                    })();
                    if result.is_err() {
                        txn.rollback_to(&mark).map_err(CoreError::from)?;
                    }
                    txn.release(mark).map_err(CoreError::from)?;
                    if self.v2_transaction.is_none() && !protected && let Ok(result) = &result {
                        if writing {
                            check_autocommit_reply(check_reply, result)?;
                        }
                        let txn = implicit.take().expect("implicit transaction");
                        self.publish_v2_sql(db, &inner, txn, false, &mut ticket)?;
                        if writing && result.rows.is_empty() && !result.columns.is_empty() {
                            relational::spill::mark_committed();
                        }
                    }
                    if explicit && result.is_ok() {
                        self.copy_freeze.last_mut().unwrap().extend(freeze_marks);
                    }
                    result
                }
                V2Class::Refused(feature) => Err(unsupported(feature)),
            }
        })();
        let result = result.map_err(|error| {
            if p.schema_ddl().is_some_and(|(_, _, create)| create)
                && error == CoreError::Sql(E::DuplicateObject)
            {
                CoreError::DuplicateSchema
            } else {
                error
            }
        });
        if result.is_err() {
            self.abort();
        }
        if self.v2_transaction.is_none() {
            self.xact.lock().unwrap_or_else(|e| e.into_inner()).take();
            self.transaction_clock_us = None;
        }
        result
    }
}

// SQL publication only: counter reservations and non-SQL engine commits do not
// use this seam. None adds only the crash-point read and untaken fault branches.
fn commit_sql(
    db: &v2::V2Db, txn: Transaction, point: CrashPoint,
    release_admission: Option<&mut dyn FnMut()>,
) -> Result<(), CoreError> {
    if point == CrashPoint::BeforeCommit {
        crash_now();
    }
    if point == CrashPoint::ErrorBeforeCommit {
        return Err(CoreError::Storage("injected before commit".into()));
    }
    let reclaim = txn.reclaim_after_commit;
    db.catalog().commit_sql_ddl(txn, release_admission)?;
    if point == CrashPoint::AfterCommit {
        crash_now();
    }
    if reclaim {
        // Durable publication must retain its successful acknowledgement.
        let _ = db.reclaim_background();
    }
    Ok(())
}

fn acknowledge_sql_commit(point: CrashPoint) -> Result<(), CoreError> {
    // ErrorAfterDecisionCommit is inapplicable: v2 has no kernel decisions.
    // Publish live sequence positions and clear the explicit transaction before
    // reporting a lost response, just as for a successfully acknowledged commit.
    if point == CrashPoint::ErrorAfterCommit {
        return Err(CoreError::Storage("injected lost commit response".into()));
    }
    if point == CrashPoint::BeforeAck {
        crash_now();
    }
    Ok(())
}

pub(super) fn validate_pending(
    db: &v2::V2Db,
    txn: &mut Transaction,
    deferred: &mut relational::DeferralState,
) -> Result<(), CoreError> {
    if deferred.pending.is_empty() {
        return Ok(());
    }
    let catalog = db.catalog().snapshot(txn)?;
    let parts = &catalog.parts;
    let mut image = catalog.cursor_image().map_err(CoreError::Sql)?
        .catalog_image_for_statement(u64::MAX, u64::MAX);
    let p = relational::Prepared::parse(
        "SET CONSTRAINTS ALL IMMEDIATE",
        u128::from_be_bytes(db.meta.domain),
        0,
    )
    .map_err(CoreError::Sql)?;
    image.execute_with_sink(
        &p,
        &[],
        None,
        None,
        &mut SqlRows {
            conflict_floor: None,
            db,
            txn,
            parts,
            catalog: &catalog,
            writing: true,
            prepared: &p,
            params: &[],
            effects: None,
            scope: None,
        },
        u64::MAX,
        Some(deferred),
    )?;
    Ok(())
}

/// DD-0068 X3: validate the post-statement image before `apply_diff` can write
/// catalog changes. Unsupported features and invalid images must fail while the
/// statement can still roll back without publishing an unenforced catalog.
fn check_post_image(
    tables: Arc<BTreeMap<String, relational::TableDef>>,
    ext: relational::catalog_ext::CatalogExt,
    next_id: u64,
) -> Result<relational::Relational, CoreError> {
    relational::Relational::catalog_image_shared(tables, ext, next_id, 1)
        .map_err(|error| match error {
            relational::SqlError::Unsupported => CoreError::EngineUnsupported {
                engine: "v2",
                feature: "SQL statement",
            },
            error => CoreError::Corrupt(format!("v2 DDL produced an invalid catalog: {error:?}")),
        })
}

#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "v2_sql_tests.rs"]
mod tests;

#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "v2_writer_lock_tests.rs"]
mod writer_lock_tests;
#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "v2_fk_delete_lock_tests.rs"]
mod fk_delete_lock_tests;
#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "v2_upsert_lock_tests.rs"]
mod upsert_lock_tests;

/// Transient statement images use legacy keys. Only this sink translates them
/// into the existing v2 key/row codec; it never persists relational image bytes.
type IndexRange = (relational::secondary_index::Descriptor, Bound<Vec<u8>>, Bound<Vec<u8>>);

struct SqlRows<'a> {
    db: &'a v2::V2Db,
    txn: &'a mut Transaction,
    parts: &'a CatalogParts,
    /// Owns these parts and the immutable occupancy image.
    catalog: &'a CatalogSnapshot,
    writing: bool,
    conflict_floor: Option<u64>,
    prepared: &'a relational::Prepared,
    params: &'a [relational::Value],
    /// Effects collector of a protected run (R3-609); `None` on every
    /// ordinary path.
    effects: Option<&'a mut crate::task::effects::Effects>,
    /// The visible rows of a task statement and its meter (R3-306): the
    /// mutation reads drop task-table rows outside them. `None` outside a
    /// task session.
    scope: Option<(&'a crate::task::rows::Scope, &'a crate::task::rows::StatementMeter)>,
}

fn charge_mutation_point(table: &TableDef, row: &SqlRow,
    retention: &mut relational::scan::Retention,
) -> Result<bool, CoreError> {
    let key = relational::row_sink::row_key(table, row).map_err(CoreError::Sql)?;
    let bytes = relational::row_sink::retained_bytes(&key, row).map_err(CoreError::Sql)?;
    Ok(retention.charge(bytes).is_ok() && retention.used() <= 256 * 1024)
}

impl<'a> SqlRows<'a> {
    /// The task scope when `table` is the task table (R3-306). Other tables
    /// are FK and integrity reads, which see every row.
    fn scope_of(&self, table: &TableDef)
        -> Option<(&'a crate::task::rows::Scope, &'a crate::task::rows::StatementMeter)> {
        self.scope.filter(|(scope, _)| scope.table() == table.id)
    }
    /// Drops the rows of a mutation page outside the task's visible rows, so
    /// neither the statement's expressions nor its writes see them.
    /// A row past the statement's rows-scanned limit or rows-read allowance
    /// stops the statement (R3-402).
    fn retain_scope(&self, table: &TableDef, rows: &mut DdlRows) -> Result<(), CoreError> {
        if let Some((scope, meter)) = self.scope_of(table) {
            let mut stopped = None;
            rows.retain(|_, row| {
                stopped.is_none()
                    && scope.keep(row, meter).unwrap_or_else(|error| {
                        stopped = Some(error);
                        false
                    })
            });
            if let Some(error) = stopped {
                return Err(error.into());
            }
        }
        Ok(())
    }
}

fn fk_referenced(parts: &CatalogParts, table: u64) -> bool {
    parts.ext.constraints.values().flatten().any(|c| matches!(c.kind,
        relational::catalog_ext::ConstraintKind::ForeignKey { ref_table, .. } if ref_table == table))
}

fn fk_key_changed(table: &TableDef, indexes: &[relational::secondary_index::Descriptor],
    old: &[relational::Value], new: &[relational::Value]) -> bool {
    table.columns.iter().enumerate().any(|(i, c)| old[i] != new[i]
        && (table.primary_key.contains(&i) || indexes.iter().any(|d|
            d.table == table.id && d.unique && d.columns.contains(&c.id))))
}

impl SqlRows<'_> {
    /// Whether the committed version of `row` is newer than `ts`.
    fn fk_changed(&self, table: &TableDef, row: &[relational::Value], ts: engine::hlc::Ts)
        -> Result<bool, CoreError> {
        let pk = table.primary_key.iter().map(|i| row[*i].clone()).collect::<Vec<_>>();
        let mut physical = table.id.to_be_bytes().to_vec();
        physical.extend(codec::pk_bytes(&pk)?);
        Ok(self.txn.changed_since(Keyspace::Rows, &physical, ts)?)
    }
    /// Follow every committed version since the fixed snapshot. KEY SHARE may
    /// follow non-key updates, but cannot resurrect a deleted or re-keyed tuple.
    fn fk_key_changed_since(&mut self, table: &TableDef, row: &SqlRow)
        -> Result<bool, CoreError> {
        let pk = table.primary_key.iter().map(|i| row.values[*i].clone()).collect::<Vec<_>>();
        let mut physical = table.id.to_be_bytes().to_vec();
        physical.extend(codec::pk_bytes(&pk)?);
        let ts = self.txn.snapshot_ts();
        let engine::txn::TxnServices::Local { seq, .. } = self.txn.services() else {
            unreachable!("local FK crosscheck");
        };
        let start = codec::user_key_seek_at(&physical, engine::hlc::Ts(u64::MAX));
        let end = codec::user_key_seek_at(&physical, ts);
        let versions = seq.kv().range(Keyspace::Rows, Bound::Included(&start),
            Bound::Excluded(&end), engine::kv::Direction::Forward)
            .map_err(engine::txn::TxnError::from)?;
        for version in versions {
            crate::cancellation::check()?;
            let (_, bytes) = version.map_err(engine::txn::TxnError::from)?;
            let Some(bytes) = engine::mvcc::decode(&bytes).map_err(engine::txn::TxnError::from)? else {
                return Ok(true);
            };
            let record = codec::decode_row(&physical, bytes)?;
            let mut values = record.values;
            if record.tombstone { return Ok(true); }
            if values.len() > table.columns.len() {
                return Err(CoreError::Corrupt("invalid v2 SQL row witness".into()));
            }
            values.resize(table.columns.len(), relational::Value::Null);
            if fk_key_changed(table, &self.parts.indexes, &row.values, &values) {
                return Ok(true);
            }
        }
        Ok(false)
    }
    /// Whether the statement snapshot was taken after every FK table lock:
    /// READ COMMITTED, or a protected run that restarted its snapshot there
    /// (GD-0061 C2/C3).
    fn statement_snapshot_is_latest(&self) -> bool {
        self.txn.isolation() == Isolation::ReadCommitted || self.txn.snapshot_restarted()
    }
    /// Runs `read` at the latest committed state when the snapshot is fixed
    /// (REPEATABLE READ, SERIALIZABLE), as PostgreSQL's RI queries for
    /// referencing rows do: a referencing row committed since the snapshot is a
    /// witness (23503/23001), and an action that writes it fails with 40001 in
    /// lock_for_write. FK reads refresh even at READ COMMITTED, since a
    /// row-key lock wait can follow the statement snapshot.
    fn at_latest<T>(&mut self, read: impl FnOnce(&mut Self) -> Result<T, CoreError>)
        -> Result<T, CoreError> {
        self.txn.begin_latest();
        let result = read(self);
        self.txn.end_latest();
        result
    }
}

impl RowSink for SqlRows<'_> {
    fn mutation_keys(
        &mut self, state: &relational::Relational, prepared: &relational::Prepared,
        params: &[relational::Value], clock: Option<&relational::clock::ExecutionClock>,
    ) -> Result<BTreeSet<Vec<u8>>, CoreError> {
        let source = MutationRows { db: self.db, txn: std::cell::RefCell::new(&mut *self.txn),
            parts: self.parts, error: std::cell::RefCell::new(None),
            #[cfg(test)]
            target: state.tables[prepared.table().expect("mutation target")].id,
        };
        // R3-306: a task statement selects its keys from the visible rows only.
        let scoped = self.scope.map(|(scope, meter)| crate::task::rows::Scoped::new(&source, scope, meter));
        let result = state.mutation_keys(prepared, params, clock,
            scoped.as_ref().map_or(&source as &dyn relational::scan::RowSource,
                |scoped| scoped as &dyn relational::scan::RowSource));
        if let Some(error) = scoped.as_ref().and_then(|scoped| scoped.take_error()) { return Err(error.into()); }
        drop(scoped);
        if let Some(error) = source.error.into_inner() { return Err(error); }
        result.map_err(CoreError::Sql)
    }
    fn stable_fk_ddl_reads(&self) -> bool {
        matches!(self.txn.services(), engine::txn::TxnServices::Local { locks, .. }
            if self.txn.isolation() == Isolation::ReadCommitted
                && locks.holds_mode(self.txn.id(), engine::locks::LockKey::Table(0), LockMode::AccessExclusive))
    }

    // FK row locks (GD-0084 A2): Local child checks find latest parents, take
    // RowKey Shared (KEY SHARE), then read latest again. Fixed snapshots retain
    // their witnesses and crosscheck every committed version for key changes/deletes.
    // RowKey is separate from Row: ordinary writes act like NO KEY UPDATE,
    // while deletes/key changes take RowKey Exclusive (FOR UPDATE) BEFORE Row.
    // Writing referential actions retain component table locks. Cluster keeps
    // its existing protocol below.
    fn fk_lookup(
        &mut self, table: &TableDef, columns: &[u64], values: Option<&[relational::Value]>,
        parent: bool, matches: &mut dyn FnMut(&SqlRow) -> Result<bool, SqlError>,
        visit: &mut dyn FnMut(SqlRow) -> Result<(), SqlError>,
    ) -> Result<(), CoreError> {
        if matches!(self.txn.services(), engine::txn::TxnServices::Cluster(_)) {
            let ts = self.txn.snapshot_ts();
            if !parent {
                return self.at_latest(|rows| rows.lookup_all(table, columns, values, false, &mut |row| {
                    if matches(&row)? { visit(row)?; }
                    Ok(())
                }));
            }
            if values.is_none() {
                // ICU-equal parents have no exact byte probe. Shared sentinels
                // are lazy write intents in a cluster, so take the eager parent
                // sentinel to keep matching owners stable through child commit.
                self.txn.lock_sql_sentinel(table.id, LockMode::Exclusive,
                    &|| crate::cancellation::check().is_err())?;
            }
            if let Some(values) = values {
                let primary = table.primary_key.iter().map(|i| table.columns[*i].id)
                    .eq(columns.iter().copied());
                let lock = if primary {
                    let mut key = table.id.to_be_bytes().to_vec();
                    key.extend(codec::pk_bytes(values)?);
                    Some((Keyspace::Rows, key))
                } else if let Some(index) = self.parts.indexes.iter()
                    .find(|d| d.table == table.id && d.unique && d.predicate.is_none() && d.columns == columns)
                {
                    let mut key = index.id.to_be_bytes().to_vec();
                    key.extend(codec::index_tuple_bytes(values,
                        &vec![codec::Direction::Asc; values.len()])?);
                    Some((Keyspace::Index, key))
                } else {
                    // TODO(GD-0075): lock FK parents without an exact primary/unique key.
                    None
                };
                if let Some((ks, key)) = lock {
                    self.txn.lock_key_share(ks, &key, crate::task::limits::lock_deadline(),
                        &|| crate::cancellation::check().is_err()).map_err(crate::task::limits::lock_error)?;
                }
            }
            let fixed = !self.statement_snapshot_is_latest();
            let mut seen = Vec::new();
            if fixed {
                // Keep the snapshot witness too: a parent deleted during the lock
                // wait must produce 40001 even though the latest lookup is empty.
                self.lookup_all(table, columns, values, true, &mut |row| {
                    if !matches(&row)? { return Ok(()); }
                    seen.push(row.values);
                    Ok(())
                })?;
            }
            self.at_latest(|rows| rows.lookup_all(table, columns, values, true, &mut |row| {
                if !matches(&row)? { return Ok(()); }
                if fixed { seen.push(row.values.clone()); }
                visit(row)
            }))?;
            for values in seen {
                if self.fk_changed(table, &values, ts)? {
                    return Err(CoreError::Sql(SqlError::Conflict));
                }
            }
            return Ok(());
        }
        if !parent {
            // A parent writer may have waited for a child's KEY SHARE after
            // choosing its statement snapshot. Probe children after that wait.
            self.txn.begin_latest();
            let result = self.lookup_all(table, columns, values, false, &mut |row| {
                if matches(&row)? { visit(row)?; }
                Ok(())
            });
            self.txn.end_latest();
            return result;
        }
        let fixed = !self.statement_snapshot_is_latest();
        let mut snapshot = Vec::new();
        if fixed {
            let reads = self.txn.replace_reads(Default::default());
            let result = self.lookup_all(table, columns, values, true, &mut |row| {
                if matches(&row)? { snapshot.push(row); }
                Ok(())
            });
            self.txn.replace_reads(reads);
            result?;
        }
        let mut locked = BTreeSet::new();
        let deadline = crate::task::limits::lock_deadline();
        let latest = loop {
            crate::cancellation::check()?;
            let parents = self.at_latest(|rows| {
                let mut parents = Vec::new();
                rows.lookup_all(table, columns, values, true, &mut |row| {
                    if matches(&row)? { parents.push(row); }
                    Ok(())
                })?;
                Ok(parents)
            })?;
            let keys = parents.iter().map(|row| codec::pk_bytes(&table.primary_key.iter()
                .map(|i| row.values[*i].clone()).collect::<Vec<_>>()))
                .collect::<Result<BTreeSet<_>, _>>()?;
            if keys.is_subset(&locked) { break parents; }
            // A lock wait can replace a UNIQUE witness with a different PK.
            // Discard these probe rows and recheck until every returned witness
            // owns its own KEY SHARE lock. Never pass probe rows to the visitor.
            drop(parents);
            for key in keys.difference(&locked) {
                crate::cancellation::check()?;
                if deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
                    return Err(crate::task::limits::lock_error(engine::txn::TxnError::LockTimeout));
                }
                self.txn.lock_row_key(table.id, key, LockMode::Shared, deadline,
                    &|| crate::cancellation::check().is_err())
                    .map_err(crate::task::limits::lock_error)?;
            }
            locked.extend(keys);
        };
        if fixed {
            for row in snapshot {
                if self.fk_key_changed_since(table, &row)?
                    || !latest.iter().any(|current| !fk_key_changed(table, &self.parts.indexes,
                    &row.values, &current.values)) {
                    return Err(CoreError::Sql(SqlError::Conflict));
                }
                visit(row).map_err(CoreError::Sql)?;
            }
        } else {
            for row in latest { visit(row).map_err(CoreError::Sql)?; }
        }
        Ok(())
    }

    fn fk_child_page(
        &mut self, table: &TableDef, columns: &[u64], values: &[relational::Value],
        after: Option<Vec<u8>>,
    ) -> Result<Option<(Vec<u8>, DdlRows)>, CoreError> {
        self.at_latest(|rows| rows.child_page(table, columns, values, after))
    }
    fn fk_read_page(&mut self, table: &TableDef, after: Option<Vec<u8>>)
        -> Result<Option<(Vec<u8>, DdlRows)>, CoreError> {
        self.at_latest(|rows| rows.read_page_all(table, after, None))
    }

    fn streams_index_builds(&self) -> bool {
        true
    }
    fn with_row_source(
        &mut self,
        read: &mut dyn FnMut(&dyn relational::scan::RowSource) -> Result<(), SqlError>,
    ) -> Result<(), CoreError> {
        let source = reduction::Source::new(SqlRows {
            conflict_floor: None, db: self.db, txn: &mut *self.txn, parts: self.parts, catalog: self.catalog,
            writing: self.writing, prepared: self.prepared, params: self.params,
            // The INSERT source is only read; protected write effects are
            // recorded by the insert pages that follow.
            effects: None,
            scope: None,
        });
        // R3-306: a task statement's INSERT source reads only the task's
        // visible rows (any other table is refused by the wrapper).
        let scoped = self.scope.map(|(scope, meter)| crate::task::rows::Scoped::new(&source, scope, meter));
        let result = read(scoped.as_ref().map_or(&source as &dyn relational::scan::RowSource,
            |scoped| scoped as &dyn relational::scan::RowSource));
        if let Some(error) = scoped.as_ref().and_then(|scoped| scoped.take_error()) { return Err(error.into()); }
        drop(scoped);
        if let Some(error) = source.error.take() { return Err(error); }
        result.map_err(CoreError::Sql)
    }
    /// One page of the mutation target; a task statement's page holds only
    /// visible rows (R3-306). The resume key is the physical last key, so an
    /// emptied page still advances.
    fn read_page(
        &mut self,
        table: &TableDef,
        after: Option<Vec<u8>>,
    ) -> Result<Option<(Vec<u8>, DdlRows)>, CoreError> {
        #[cfg(test)]
        MUTATION_PAGE_READS.set(MUTATION_PAGE_READS.get() + 1);
        let range = self.scope_of(table).and_then(|(scope, _)| scope.key_range()).cloned();
        let page = self.read_page_all(table, after, range.as_ref())?;
        page.map(|(last, mut rows)| {
            self.retain_scope(table, &mut rows)?;
            Ok((last, rows))
        })
        .transpose()
    }
    fn mutation_candidates(&mut self, table: &TableDef, keys: &[Vec<relational::Value>])
        -> Result<Option<DdlRows>, CoreError> {
        let mut rows = self.mutation_candidates_all(table, keys)?;
        if let Some(rows) = rows.as_mut() {
            self.retain_scope(table, rows)?;
        }
        Ok(rows)
    }
    fn mutation_pages(
        &mut self,
        table: &TableDef,
        keys: Option<&[Vec<relational::Value>]>,
        page: &mut dyn FnMut(DdlRows) -> Result<DdlRows, CoreError>,
    ) -> Result<(), CoreError> {
        #[cfg(test)]
        let keys = if DISABLE_MUTATION_POINTS.get() { None } else { keys };
        let Some(keys) = keys else { return self.ddl_pages(table, table, true, page); };
        let entry = TableEntry { primary_index_birth: self.parts.ext.primary_index_birth.get(&table.id).copied(), def: table.clone(), heap: self.parts.heap.contains(&table.id),
            columns_ext: BTreeMap::new() };
        let mut rows = DdlRows::new();
        let mut retention = relational::scan::Retention::new(relational::scan::retention_limit());
        for key in keys {
            crate::cancellation::check()?;
            #[cfg(test)]
            MUTATION_POINT_READS.set(MUTATION_POINT_READS.get() + 1);
            if self.scope_of(table).is_some_and(|(scope, _)|
                relational::encode_key(key).is_ok_and(|key| !scope.contains_key(&key))) { continue; }
            if let Some(row) = self.db.rows.get(self.txn, &entry, key)? {
                #[cfg(test)]
                MUTATION_ROWS_READ.set(MUTATION_ROWS_READ.get() + 1);
                let row = SqlRow { values: row.values, version: row.version };
                // R3-306: a key outside the task's visible rows matches nothing.
                if let Some((scope, meter)) = self.scope_of(table)
                    && !scope.keep(&row, meter)?
                {
                    continue;
                }
                let key = relational::row_sink::row_key(table, &row).map_err(CoreError::Sql)?;
                // Sparse targets can aggregate wider rows than any old page.
                // Leave room for the eight UNIQUE sidecars and their work meters;
                // fall back before invoking the callback or publishing a write.
                if !charge_mutation_point(table, &row, &mut retention)? {
                    return self.ddl_pages(table, table, true, page);
                }
                rows.insert(key, row);
            }
        }
        #[cfg(test)]
        {
            if let Some(hook) = AFTER_MUTATION_PAGE.with_borrow_mut(Option::take) { hook(); }
        }
        let rewritten = page(rows.clone())?;
        self.apply_indexes(table, Some(table), &rows, &rewritten, &self.indexes())?;
        self.apply(table, &rows, &rewritten)
    }
    fn ddl_pages(
        &mut self,
        table: &TableDef,
        after: &TableDef,
        mutating: bool,
        page: &mut dyn FnMut(DdlRows) -> Result<DdlRows, CoreError>,
    ) -> Result<(), CoreError> {
        let entry = TableEntry {
            primary_index_birth: self.parts.ext.primary_index_birth.get(&table.id).copied(),
            def: table.clone(),
            heap: self.parts.heap.contains(&table.id),
            columns_ext: BTreeMap::new(),
        };
        let range = self.scope_of(table).and_then(|(scope, _)| scope.key_range()).cloned();
        let mut lower = range.as_ref().map_or(Bound::Unbounded, |r| r.lo.clone());
        let upper = range.map_or(Bound::Unbounded, |r| r.hi);
        loop {
            crate::cancellation::check()?;
            let rows = self
                .db
                .rows
                .scan(
                    self.txn,
                    &entry,
                    lower,
                    upper.clone(),
                    engine::kv::Direction::Forward,
                    Some(200),
                )?
                .map(|pair| {
                    pair.map(|(key, row)| {
                        (
                            key,
                            SqlRow {
                                values: row.values,
                                version: row.version,
                            },
                        )
                    })
                })
                .collect::<Result<BTreeMap<_, _>, _>>()?;
            let Some(last) = rows.last_key_value().map(|(key, _)| key.clone()) else {
                break;
            };
            lower = Bound::Excluded(last);
            let mut rows = rows
                .into_values()
                .map(|row| relational::row_sink::row_key(table, &row).map(|key| (key, row)))
                .collect::<Result<BTreeMap<_, _>, _>>()
                .map_err(CoreError::Sql)?;
            #[cfg(test)]
            {
                MUTATION_ROWS_READ.set(MUTATION_ROWS_READ.get() + rows.len());
                if let Some(hook) = AFTER_MUTATION_PAGE.with_borrow_mut(Option::take) { hook(); }
            }
            // R3-306: a task statement's mutation page holds only visible rows.
            if mutating && self.scope_of(table).is_some() {
                self.retain_scope(table, &mut rows)?;
                if rows.is_empty() {
                    continue;
                }
            }
            let rewritten = page(rows.clone())?;
            if mutating {
                self.apply_indexes(table, Some(after), &rows, &rewritten, &self.indexes())?;
                self.apply(table, &rows, &rewritten)?;
            } else if table.columns.len() != after.columns.len() {
                // The codec supplies NULL, not a frozen missing-column default.
                // Rewrite bounded pages in the OUTER transaction: rows and the
                // new layout become visible together, including after a crash.
                // Its existing write-set budget still bounds large rewrites.
                // Existing index tuples and primary-key bytes are unchanged.
                self.apply(table, &rows, &BTreeMap::new())?;
                self.apply(after, &BTreeMap::new(), &rewritten)?;
            }
        }
        Ok(())
    }
    fn backfill_index(
        &mut self,
        table: &TableDef,
        index: &relational::secondary_index::Descriptor,
    ) -> Result<(), CoreError> {
        let entry = TableEntry {
            primary_index_birth: self.parts.ext.primary_index_birth.get(&table.id).copied(),
            def: table.clone(),
            heap: self.parts.heap.contains(&table.id),
            columns_ext: BTreeMap::new(),
        };
        let mut lower = Bound::Unbounded;
        loop {
            crate::cancellation::check()?;
            let rows = self
                .db
                .rows
                .scan(
                    self.txn,
                    &entry,
                    lower,
                    Bound::Unbounded,
                    engine::kv::Direction::Forward,
                    Some(200),
                )?
                .map(|pair| {
                    pair.map(|(key, row)| {
                        (
                            key,
                            SqlRow {
                                values: row.values,
                                version: row.version,
                            },
                        )
                    })
                })
                .collect::<Result<BTreeMap<_, _>, _>>()?;
            let Some(last) = rows.last_key_value().map(|(key, _)| key.clone()) else {
                break;
            };
            lower = Bound::Excluded(last);
            let image = self.catalog.image().map_err(CoreError::Sql)?;
            let entries = engine::indexes::build_entries(&image, table, &rows, index)?;
            // The outer transaction holds the DDL table locks. Chunk commits
            // publish only entries under a fresh, durably reserved ID; queries
            // cannot discover it until the outer catalog commit succeeds.
            // Rollback/crash can leave unreachable entries, never a partial
            // visible index. No catalog or row bytes are committed by a chunk.
            let mut chunk = self.db.begin(Isolation::ReadCommitted);
            chunk.require_features(engine::meta::SECONDARY_INDEXES);
            for (key, pk) in entries {
                let target = chunk.lock_for_write(Keyspace::Index, &key, crate::task::limits::lock_deadline(), &|| {
                    crate::cancellation::check().is_err()
                }).map_err(crate::task::limits::lock_error)?;
                if target.current.is_some() {
                    return Err(engine::indexes::occupancy_duplicate(
                        table, &rows, std::slice::from_ref(index), &key, &pk, true,
                    ));
                }
                chunk.put(Keyspace::Index, key, pk)?;
            }
            chunk.commit()?;
            #[cfg(test)]
            AFTER_INDEX_CHUNK.with_borrow(|hook| {
                if let Some(hook) = hook {
                    hook();
                }
            });
            #[cfg(test)]
            if CRASH_AFTER_INDEX_CHUNK.replace(false) {
                crash_now();
            }
        }
        Ok(())
    }
    /// The general DML path's target lookup: in a task statement, rows of the
    /// task table outside the visible rows are dropped (R3-306). FK witness
    /// and conflict lookups use the unscoped `lookup_all`.
    fn lookup(
        &mut self,
        table: &TableDef,
        columns: &[u64],
        values: Option<&[relational::Value]>,
        unique: bool,
        visit: &mut dyn FnMut(SqlRow) -> Result<(), SqlError>,
    ) -> Result<(), CoreError> {
        let scope = self.scope_of(table);
        let mut stopped = None;
        let result = self.lookup_all(table, columns, values, unique, &mut |row| {
            if let Some((scope, meter)) = scope {
                match scope.keep(&row, meter) {
                    Ok(true) => {}
                    Ok(false) => return Ok(()),
                    Err(error) => {
                        stopped = Some(error);
                        return Err(SqlError::Unsupported);
                    }
                }
            }
            visit(row)
        });
        match stopped {
            Some(error) => Err(error.into()),
            None => result,
        }
    }
    fn child_page(&mut self, table: &TableDef, columns: &[u64], values: &[relational::Value],
        after: Option<Vec<u8>>) -> Result<Option<(Vec<u8>, DdlRows)>, CoreError> {
        crate::cancellation::check()?;
        let primary = table.primary_key.iter().map(|i| table.columns[*i].id).collect::<Vec<_>>();
        let matches = |ids: &[u64]| ids.len() >= columns.len()
            && ids[..columns.len()].iter().all(|id| columns.contains(id));
        let primary_matches = matches(&primary) && !values.contains(&relational::Value::Null);
        let descriptor = self.parts.indexes.iter().find(|d| d.table == table.id
            && d.predicate.is_none() && d.columns.len() == columns.len() && matches(&d.columns))
            .or_else(|| (!primary_matches).then(|| self.parts.indexes.iter()
                .find(|d| d.table == table.id && d.predicate.is_none() && matches(&d.columns))).flatten());
        let page = if descriptor.is_some() || primary_matches {
            let ids = descriptor.map_or(primary.as_slice(), |d| d.columns.as_slice());
            let ordered = ids[..columns.len()].iter().map(|id|
                values[columns.iter().position(|c| c == id).unwrap()].clone()).collect::<Vec<_>>();
            let entry = TableEntry { primary_index_birth: self.parts.ext.primary_index_birth.get(&table.id).copied(), def: table.clone(), heap: self.parts.heap.contains(&table.id), columns_ext: BTreeMap::new() };
            let mut rows = DdlRows::new();
            let mut collect = |row: engine::rows::Row| {
                let row = SqlRow { values: row.values, version: row.version };
                rows.insert(relational::row_sink::row_key(table, &row).map_err(CoreError::Sql)?, row);
                Ok(())
            };
            let last = if let Some(d) = descriptor {
                let tuple = codec::index_tuple_bytes(&ordered, &vec![codec::Direction::Asc; ordered.len()])?;
                // Included upper tuple bounds already use the prefix successor,
                // covering every trailing index component and PK suffix.
                engine::indexes::visit_page(self.txn, &entry, d,
                    Bound::Included(tuple.clone()), Bound::Included(tuple), after,
                    &mut |_, row| collect(row), 200)?
            } else {
                // pk_bytes concatenates components without an arity/header suffix.
                // Scan the same transaction so snapshot and own writes agree.
                let prefix = codec::pk_bytes(&ordered)?;
                let upper = engine::indexes::successor(prefix.clone())
                    .map_or(Bound::Unbounded, Bound::Excluded);
                let mut last = None;
                for pair in self.db.rows.scan(self.txn, &entry,
                    after.map_or(Bound::Included(prefix), Bound::Excluded), upper,
                    engine::kv::Direction::Forward, Some(200))? {
                    let (key, row) = pair?;
                    last = Some(key);
                    collect(row)?;
                }
                last
            };
            last.map(|last| (last, rows))
        } else {
            self.read_page(table, after)?
        };
        let Some((last, mut rows)) = page else { return Ok(None); };
        rows.retain(|_, row| columns.iter().zip(values).all(|(id, value)|
            row.values[table.columns.iter().position(|c| c.id == *id).unwrap()] == *value));
        Ok(Some((last, rows)))
    }

    fn begin_conflict_pages(&mut self, floor: u64) {
        self.conflict_floor = Some(floor);
    }
    fn collated_lookup(
        &mut self,
        table: &TableDef,
        key: &relational::collation_keys::Key,
        tuples: &BTreeSet<Vec<u8>>,
        arbiter: bool,
        visit: &mut dyn FnMut(SqlRow) -> Result<(), SqlError>,
    ) -> Result<(), CoreError> {
        let floor = self.conflict_floor;
        self.lookup_all(table, &[], None, true, &mut |row| {
            if key.tuple(&row.values)?.is_some_and(|tuple| tuples.contains(&tuple)) {
                if arbiter && floor.is_some_and(|v| row.version >= v) {
                    return Err(relational::conflict_duplicate_target());
                }
                visit(row)?;
            }
            Ok(())
        })
    }
    fn conflict_lookup(
        &mut self,
        table: &TableDef,
        columns: &[u64],
        values: &[relational::Value],
        visit: &mut dyn FnMut(SqlRow) -> Result<(), SqlError>,
    ) -> Result<(), CoreError> {
        let floor = self.conflict_floor;
        self.lookup_all(table, columns, Some(values), true, &mut |row| {
            if floor.is_some_and(|v| row.version >= v) { return Err(relational::conflict_duplicate_target()); }
            visit(row)
        })
    }
    fn indexes(&self) -> Vec<relational::secondary_index::Descriptor> {
        self.parts.indexes.clone()
    }
    fn apply_indexes(
        &mut self,
        table: &TableDef,
        after_table: Option<&TableDef>,
        before: &BTreeMap<Vec<u8>, SqlRow>,
        after: &BTreeMap<Vec<u8>, SqlRow>,
        descriptors: &[relational::secondary_index::Descriptor],
    ) -> Result<(), CoreError> {
        let image = self.catalog.image().map_err(CoreError::Sql)?;
        engine::indexes::apply(
            self.txn,
            Some(&image),
            table,
            after_table,
            before,
            after,
            &self.parts.indexes,
            descriptors,
            crate::task::limits::lock_deadline(),
            &|| crate::cancellation::check().is_err(),
        )?;
        if let Some(new) = after_table
            && (new.primary_key != table.primary_key || new.columns.len() != table.columns.len())
        {
            // ADD/DROP COLUMN and ADD PRIMARY KEY can shift the heap key or
            // change its values. Validate old witnesses with the old layout,
            // then publish the rewritten rows with the new descriptor.
            self.apply(table, before, &BTreeMap::new())?;
            self.apply(new, &BTreeMap::new(), after)?;
        }
        Ok(())
    }

    fn scan(
        &mut self,
        table: &TableDef,
        visit: &mut dyn FnMut(SqlRow) -> Result<(), SqlError>,
    ) -> Result<(), CoreError> {
        // R3-306: the general DML path loads a task statement's target
        // through here; rows outside the visible rows are not loaded.
        // A row past the statement's limits stops the scan (R3-402).
        let scope = self.scope_of(table);
        let stopped = std::cell::Cell::new(None);
        let visit = &mut |row: SqlRow| {
            if let Some((scope, meter)) = scope {
                match scope.keep(&row, meter) {
                    Ok(true) => {}
                    Ok(false) => return Ok(()),
                    Err(error) => {
                        stopped.set(Some(error));
                        return Err(SqlError::Unsupported);
                    }
                }
            }
            visit(row)
        };
        let result = (|| -> Result<(), CoreError> {
            let entry = TableEntry {
                primary_index_birth: self.parts.ext.primary_index_birth.get(&table.id).copied(),
                def: table.clone(),
                heap: self.parts.heap.contains(&table.id),
                columns_ext: BTreeMap::new(),
            };
            if let Some((lo, hi)) = self.primary_key_bounds(table) {
                // GD-0075 P8-0: mandatory comparisons on an integer primary key read only the rows
                // in their key range (the full WHERE is applied afterwards), so a point or key-range
                // SELECT on a cluster gateway waits only on prepared writes inside that range.
                for pair in self.db.rows.scan(
                    self.txn,
                    &entry,
                    lo,
                    hi,
                    engine::kv::Direction::Forward,
                    None,
                )? {
                    let (_, row) = pair?;
                    visit(SqlRow {
                        values: row.values,
                        version: row.version,
                    })
                    .map_err(CoreError::Sql)?;
                }
                return Ok(());
            }
            let candidate = self.scan_index(table);
            let Some((descriptor, lo, hi)) = candidate else {
                return self.scan_all(&entry, visit);
            };
            engine::indexes::visit(self.txn, &entry, &descriptor, lo, hi, &mut |_, row| {
                visit(SqlRow {
                    values: row.values,
                    version: row.version,
                })
                .map_err(CoreError::Sql)
            })?;
            #[cfg(test)]
            USED_INDEX_SCAN.set(true);
            #[cfg(test)]
            AFTER_TABLE_SCAN.with(|hook| {
                if let Some(hook) = hook.borrow_mut().as_mut() {
                    hook(&table.name);
                }
            });
            Ok(())
        })();
        match stopped.take() {
            Some(error) => Err(error.into()),
            None => result,
        }
    }
    fn check_occupancy(
        &mut self,
        table: &TableDef,
        before: &BTreeMap<Vec<u8>, SqlRow>,
        after: &BTreeMap<Vec<u8>, SqlRow>,
    ) -> Result<(), CoreError> {
        // Protect changed parent keys before checking children, including
        // fixed snapshots and searched mutations without candidate prelocks.
        if matches!(self.txn.services(), engine::txn::TxnServices::Local { .. })
            && fk_referenced(self.parts, table.id) {
            let keys = before.iter().filter(|(key, old)| after.get(*key).is_none_or(|new|
                fk_key_changed(table, &self.parts.indexes, &old.values, &new.values)))
                .map(|(_, row)| codec::pk_bytes(&table.primary_key.iter()
                    .map(|i| row.values[*i].clone()).collect::<Vec<_>>()))
                .collect::<Result<BTreeSet<_>, _>>()?;
            for key in keys {
                self.txn.lock_row_write_group(table.id, &key,
                    crate::task::limits::lock_deadline(), &|| crate::cancellation::check().is_err())
                    .map_err(crate::task::limits::lock_error)?;
                let mut physical = table.id.to_be_bytes().to_vec();
                physical.extend(key);
                // Fixed-snapshot writers must detect a changed parent before
                // reporting any child witness discovered after this wait.
                self.txn.lock_for_write(Keyspace::Rows, &physical,
                    crate::task::limits::lock_deadline(), &|| crate::cancellation::check().is_err())
                    .map_err(crate::task::limits::lock_error)?;
            }
        }
        debug_assert!(std::ptr::eq(self.parts, &self.catalog.parts));
        let image = self.catalog.image().map_err(CoreError::Sql)?;
        #[cfg(all(test, feature = "engine-rocksdb"))]
        OCCUPANCY_IMAGE.with(|hook| {
            if let Some(check) = hook.borrow_mut().take() {
                check(self.catalog, &image);
            }
        });
        for (key, row) in after {
            if before.contains_key(key) {
                continue;
            }
            let mut physical = table.id.to_be_bytes().to_vec();
            physical.extend(codec::pk_bytes(
                &table
                    .primary_key
                    .iter()
                    .map(|i| row.values[*i].clone())
                    .collect::<Vec<_>>(),
            )?);
            if self
                .txn
                .lock_for_write(Keyspace::Rows, &physical, crate::task::limits::lock_deadline(), &|| {
                    crate::cancellation::check().is_err()
                }).map_err(crate::task::limits::lock_error)?
                .current
                .is_some()
            {
                return Err(CoreError::Sql(image.primary_duplicate(table, &row.values)));
            }
        }
        if image.has_nondeterministic_keys(table) {
            // UNIQUE checks see committed owners even with a fixed transaction
            // snapshot. The exclusive SQL sentinel serializes collation probes.
            image.validate_collated_unique(table, after, &self.parts.indexes,
                &mut || crate::cancellation::check().is_err()).map_err(CoreError::Sql)?;
            let keys = image.collated_unique_keys(table, &self.parts.indexes).map_err(CoreError::Sql)?
                .into_iter().map(|(key, name)| {
                    let mut tuples = BTreeMap::new();
                    for (_, row) in after.iter().filter(|(pk, row)| before.get(*pk) != Some(*row)) {
                        if let Some(tuple) = key.tuple(&row.values)? {
                            tuples.insert(tuple, &row.values);
                        }
                    }
                    Ok((key, name, tuples))
                }).collect::<Result<Vec<_>, SqlError>>().map_err(CoreError::Sql)?;
            // Scan without retaining the table: only this publication page's
            // proposals consume memory, even beyond the legacy row cap.
            self.at_latest(|source| source.lookup_all(table, &[], None, true, &mut |row| {
                let pk = relational::row_sink::row_key(table, &row)?;
                // Only replaced/deleted owners are excluded. An unchanged
                // snapshot witness may have different values in latest state.
                if before.get(&pk) != after.get(&pk) { return Ok(()); }
                for (key, name, tuples) in &keys {
                    if let Some(tuple) = key.tuple(&row.values)?
                        && let Some(proposed) = tuples.get(&tuple)
                    {
                        return Err(key.duplicate_detail(table, name, proposed, false)
                            .unwrap_or_else(|| relational::collation_keys::duplicate(name)));
                    }
                }
                Ok(())
            }))?;
        }
        engine::indexes::check_occupancy(self.txn, &image, table, before, after, &self.parts.indexes)
    }
    fn apply(
        &mut self,
        table: &TableDef,
        before: &BTreeMap<Vec<u8>, SqlRow>,
        after: &BTreeMap<Vec<u8>, SqlRow>,
    ) -> Result<(), CoreError> {
        if !self.writing {
            return Ok(());
        }
        let mut changes = BTreeMap::new();
        for key in before.keys().chain(after.keys()) {
            let old = before.get(key);
            let new = after.get(key);
            if old == new {
                continue;
            }
            let row = old.or(new).expect("changed row");
            let pk = table
                .primary_key
                .iter()
                .map(|i| row.values[*i].clone())
                .collect::<Vec<_>>();
            let mut physical = table.id.to_be_bytes().to_vec();
            physical.extend(codec::pk_bytes(&pk)?);
            changes.insert(physical, (old, new));
        }
        // Acquire and validate every witness before buffering any write, with
        // deterministic physical key order for re-key and searched mutations.
        for (key, (old, new)) in &changes {
            crate::cancellation::check()?;
            if matches!(self.txn.services(), engine::txn::TxnServices::Local { .. })
                && old.is_some_and(|old| new.is_none_or(|new|
                    fk_key_changed(table, &self.parts.indexes, &old.values, &new.values))) {
                self.txn.lock_row_write_group(table.id, &key[8..],
                    crate::task::limits::lock_deadline(), &|| crate::cancellation::check().is_err())
                    .map_err(crate::task::limits::lock_error)?;
            }
            let target = self.txn.lock_for_write(Keyspace::Rows, key, crate::task::limits::lock_deadline(), &|| {
                crate::cancellation::check().is_err()
            }).map_err(crate::task::limits::lock_error)?;
            match (old, target.current) {
                (None, Some(_)) => {
                    if !relational::collation_keys::primary_has_uuid(table) {
                        return Err(CoreError::Sql(SqlError::Duplicate));
                    }
                    let row = new.expect("appended row");
                    let image = self.catalog.image().map_err(CoreError::Sql)?;
                    return Err(CoreError::Sql(image.primary_duplicate(table, &row.values)));
                }
                (Some(old), Some(bytes)) => {
                    let record = codec::decode_row(key, &bytes)?;
                    let mut values = record.values;
                    if record.tombstone || values.len() > table.columns.len() {
                        return Err(CoreError::Corrupt("invalid v2 SQL row witness".into()));
                    }
                    values.resize(table.columns.len(), relational::Value::Null);
                    if values != old.values || record.row_version != old.version {
                        return Err(CoreError::Sql(SqlError::Conflict));
                    }
                }
                (Some(_), None) => return Err(CoreError::Sql(SqlError::Conflict)),
                (None, None) => (),
            }
        }
        for (key, (_, new)) in &changes {
            crate::cancellation::check()?;
            match new {
                Some(row) => self.txn.put(
                    Keyspace::Rows,
                    key.clone(),
                    codec::encode_row(key, row.version, &row.values)?,
                )?,
                None => self.txn.delete(Keyspace::Rows, key.clone())?,
            }
            #[cfg(test)]
            if FAIL_AFTER_ROW_WRITE.replace(false) {
                return Err(CoreError::Cancelled);
            }
        }
        if let Some(effects) = self.effects.as_deref_mut() {
            for (key, (old, new)) in &changes {
                effects.record(table.id, key, *old, *new)?;
            }
        }
        Ok(())
    }
}

/// Native reads share SQL's index selection, tuple bounds and MVCC visitor.
/// Do not begin a statement here: the complete input already pinned its snapshot.
pub(super) fn secondary_read_native(
    db: &v2::V2Db, txn: &mut Transaction, table: &TableDef,
    request: &relational::secondary_index_access::Request<'_>,
) -> Result<relational::SqlResult, CoreError> {
    use relational::{Value, MAX_COLUMNS, MAX_RESULT_BYTES, MAX_ROWS};
    let unsupported = CoreError::Sql(SqlError::Unsupported);
    crate::cancellation::check()?;
    let catalog = db.catalog().snapshot(txn)?;
    let image = catalog.image().map_err(CoreError::Sql)?;
    if image.tables.get(&image.table_key(table)) != Some(table) {
        return Err(CoreError::Sql(SqlError::StaleSchema));
    }
    if table.hidden_rowid().is_some() || request.descending || !request.residuals.is_empty()
        || request.equal.is_empty() && request.range.is_none()
    { return Err(unsupported); }
    if request.equal.len() > MAX_COLUMNS || request.projection.len() > MAX_COLUMNS
        || request.max_rows > MAX_ROWS || request.max_bytes > MAX_RESULT_BYTES
    { return Err(CoreError::Sql(SqlError::Limit)); }
    let offset = |id| table.columns.iter().position(|c| c.id == id)
        .ok_or(CoreError::Sql(SqlError::UnknownColumn));
    let mut predicates = Vec::new();
    let mut checks = Vec::new();
    let mut input_bytes = 0usize;
    let mut predicate = |id, op: &str, value: &Value| -> Result<(), CoreError> {
        let i = offset(id)?;
        let column = &table.columns[i];
        if relational::secondary_index::Kind::for_type(&column.data_type).is_none()
            || matches!(value, Value::Null)
            || catalog.parts.ext.columns.get(&id).is_some_and(|c| c.type_id.is_some())
        { return Err(CoreError::Sql(SqlError::Unsupported)); }
        // A comparison operand need not fit the stored column (for example,
        // SMALLINT > 32768). Leave such predicates to the complete fallback.
        if relational::check(value, column).is_err() {
            return Err(CoreError::Sql(SqlError::Unsupported));
        }
        input_bytes = input_bytes.checked_add(relational::value_bytes(value))
            .ok_or(CoreError::Sql(SqlError::Limit))?;
        if input_bytes >= relational::MAX_ROW_BYTES { return Err(CoreError::Sql(SqlError::Limit)); }
        checks.push((i, op.to_owned(), codec::index_tuple_bytes(
            std::slice::from_ref(value), &[codec::Direction::Asc])?));
        predicates.push((column.name.clone(), op.to_owned(), value.clone()));
        Ok(())
    };
    for (id, value) in request.equal { predicate(*id, "=", value)?; }
    if let Some(range) = request.range {
        if range.lower.is_none() && range.upper.is_none() { return Err(unsupported); }
        if let Some(e) = range.lower { predicate(range.column, if e.inclusive { ">=" } else { ">" }, e.value)?; }
        if let Some(e) = range.upper { predicate(range.column, if e.inclusive { "<=" } else { "<" }, e.value)?; }
    }
    let (descriptor, lo, hi) = SqlRows::index_range(&catalog.parts, table, &predicates)
        .ok_or(unsupported)?;
    let projection = request.projection.iter().map(|id| offset(*id)).collect::<Result<Vec<_>, _>>()?;
    let mut bytes = projection.iter().map(|i| table.columns[*i].name.len()).sum::<usize>();
    if bytes > request.max_bytes { return Err(CoreError::Sql(SqlError::Limit)); }
    let mut result = relational::SqlResult {
        columns: projection.iter().map(|i| table.columns[*i].name.clone()).collect(),
        ..Default::default()
    };
    let entry = TableEntry { primary_index_birth: catalog.parts.ext.primary_index_birth.get(&table.id).copied(), def: table.clone(), heap: false, columns_ext: BTreeMap::new() };
    engine::indexes::visit(txn, &entry, &descriptor, lo, hi, &mut |_, row| {
        crate::cancellation::check()?;
        // SQL selects on the leading index column. Check every native term,
        // including remaining equality columns and a composite range suffix.
        for (i, op, bound) in &checks {
            if row.values[*i] == Value::Null { return Ok(()); }
            let value = codec::index_tuple_bytes(std::slice::from_ref(&row.values[*i]), &[codec::Direction::Asc])?;
            let matches = match op.as_str() {
                "=" => value == *bound, ">" => value > *bound, ">=" => value >= *bound,
                "<" => value < *bound, "<=" => value <= *bound, _ => unreachable!(),
            };
            if !matches { return Ok(()); }
        }
        if result.rows.len() >= request.max_rows { return Err(CoreError::Sql(SqlError::Limit)); }
        for i in &projection {
            let size = match &row.values[*i] {
                Value::Numeric(_) => giggles_pg_types::MAX_TEXT,
                Value::Float4(_) => 4, Value::Float8(_) => 8,
                value if value.is_temporal() => relational::value_bytes(value),
                Value::Text(s) => s.len(), Value::Bytes(b) => b.len(), _ => 9,
            };
            bytes = bytes.checked_add(size).ok_or(CoreError::Sql(SqlError::Limit))?;
            if bytes > request.max_bytes { return Err(CoreError::Sql(SqlError::Limit)); }
        }
        result.rows.push(SqlRow { values: projection.iter().map(|i| row.values[*i].clone()).collect(),
            version: row.version });
        Ok(())
    })?;
    crate::cancellation::check()?;
    Ok(result)
}

/// Short transaction borrows permit nested physical scans with bounded pages.
/// Every scan reads the same statement snapshot plus transaction own writes.
struct MutationRows<'a> {
    db: &'a v2::V2Db,
    txn: std::cell::RefCell<&'a mut Transaction>,
    parts: &'a CatalogParts,
    error: std::cell::RefCell<Option<CoreError>>,
    #[cfg(test)]
    target: u64,
}
impl relational::scan::RowSource for MutationRows<'_> {
    fn scan<'s>(&'s self, table: &TableDef, range: &relational::scan::KeyRange,
        direction: relational::scan::Direction,
    ) -> Result<relational::scan::SourceRows<'s>, SqlError> {
        use std::borrow::Cow;
        #[cfg(test)]
        if table.id != self.target {
            AFTER_TABLE_SCAN.with(|hook| {
                if let Some(hook) = hook.borrow_mut().as_mut() { hook(&table.name); }
            });
        }
        let entry = TableEntry { primary_index_birth: self.parts.ext.primary_index_birth.get(&table.id).copied(), def: table.clone(), heap: self.parts.heap.contains(&table.id),
            columns_ext: BTreeMap::new() };
        let (mut lo, mut hi) = (range.lo.clone(), range.hi.clone());
        let forward = direction == relational::scan::Direction::Forward;
        let mut pending = Vec::new().into_iter();
        let mut done = range.is_empty();
        Ok(Box::new(std::iter::from_fn(move || loop {
            if let Some((key, row)) = pending.next() {
                return Some(Ok(relational::scan::SourceRow { key: Cow::Owned(key), row: Cow::Owned(row) }));
            }
            if done { return None; }
            let page = (|| {
                crate::cancellation::check()?;
                self.db.rows.scan(&mut self.txn.borrow_mut(), &entry, lo.clone(), hi.clone(),
                    if forward { engine::kv::Direction::Forward } else { engine::kv::Direction::Reverse }, Some(200))?
                    .collect::<Result<Vec<_>, CoreError>>()
            })();
            match page {
                Ok(rows) => {
                    let Some((last, _)) = rows.last() else { done = true; continue; };
                    if forward { lo = Bound::Excluded(last.clone()); } else { hi = Bound::Excluded(last.clone()); }
                    let rows = rows.into_iter().map(|(_, row)| {
                        let row = SqlRow { values: row.values, version: row.version };
                        relational::row_sink::row_key(&entry.def, &row).map(|key| (key, row))
                    }).collect::<Result<Vec<_>, _>>();
                    match rows {
                        Ok(rows) => pending = rows.into_iter(),
                        Err(error) => { done = true; return Some(Err(error)); }
                    }
                }
                Err(error) => {
                    done = true;
                    *self.error.borrow_mut() = Some(error);
                    return Some(Err(SqlError::Unsupported));
                }
            }
        })))
    }
    fn get<'s>(&'s self, table: &TableDef, key: &[u8]) -> Result<Option<relational::scan::SourceRow<'s>>, SqlError> {
        self.scan(table, &relational::scan::KeyRange { lo: Bound::Included(key.to_vec()),
            hi: Bound::Included(key.to_vec()) }, relational::scan::Direction::Forward)?.next().transpose()
    }
}

impl SqlRows<'_> {
    fn read_page_all(
        &mut self,
        table: &TableDef,
        after: Option<Vec<u8>>,
        range: Option<&relational::scan::KeyRange>,
    ) -> Result<Option<(Vec<u8>, DdlRows)>, CoreError> {
        crate::cancellation::check()?;
        let entry = TableEntry {
            primary_index_birth: self.parts.ext.primary_index_birth.get(&table.id).copied(),
            def: table.clone(),
            heap: self.parts.heap.contains(&table.id),
            columns_ext: BTreeMap::new(),
        };
        let incoming = relational::scan::KeyRange {
            lo: after.map_or(Bound::Unbounded, Bound::Excluded), hi: Bound::Unbounded,
        };
        let range = range.map_or_else(|| incoming.clone(), |range| crate::task::rows::intersect(range, &incoming));
        if range.is_empty() { return Ok(None); }
        let rows = self
            .db
            .rows
            .scan(
                self.txn,
                &entry,
                range.lo,
                range.hi,
                engine::kv::Direction::Forward,
                Some(200),
            )?
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let Some(last) = rows.last_key_value().map(|(key, _)| key.clone()) else {
            return Ok(None);
        };
        let rows = rows
            .into_values()
            .map(|row| {
                let row = SqlRow {
                    values: row.values,
                    version: row.version,
                };
                relational::row_sink::row_key(table, &row).map(|key| (key, row))
            })
            .collect::<Result<_, _>>()
            .map_err(CoreError::Sql)?;
        Ok(Some((last, rows)))
    }
    fn mutation_candidates_all(&mut self, table: &TableDef, keys: &[Vec<relational::Value>])
        -> Result<Option<DdlRows>, CoreError> {
        #[cfg(test)]
        if DISABLE_MUTATION_POINTS.get() { return Ok(None); }
        let entry = TableEntry { primary_index_birth: self.parts.ext.primary_index_birth.get(&table.id).copied(), def: table.clone(), heap: self.parts.heap.contains(&table.id),
            columns_ext: BTreeMap::new() };
        let mut rows = DdlRows::new();
        let mut retention = relational::scan::Retention::new(relational::scan::retention_limit());
        for key in keys {
            crate::cancellation::check()?;
            #[cfg(test)]
            MUTATION_POINT_READS.set(MUTATION_POINT_READS.get() + 1);
            if self.scope_of(table).is_some_and(|(scope, _)|
                relational::encode_key(key).is_ok_and(|key| !scope.contains_key(&key))) { continue; }
            if let Some(row) = self.db.rows.get(self.txn, &entry, key)? {
                #[cfg(test)]
                MUTATION_ROWS_READ.set(MUTATION_ROWS_READ.get() + 1);
                let row = SqlRow { values: row.values, version: row.version };
                if !charge_mutation_point(table, &row, &mut retention)? { return Ok(None); }
                rows.insert(relational::row_sink::row_key(table, &row).map_err(CoreError::Sql)?, row);
            }
        }
        Ok(Some(rows))
    }
    fn lookup_all(
        &mut self,
        table: &TableDef,
        columns: &[u64],
        values: Option<&[relational::Value]>,
        unique: bool,
        visit: &mut dyn FnMut(SqlRow) -> Result<(), SqlError>,
    ) -> Result<(), CoreError> {
        let entry = TableEntry {
            primary_index_birth: self.parts.ext.primary_index_birth.get(&table.id).copied(),
            def: table.clone(),
            heap: self.parts.heap.contains(&table.id),
            columns_ext: BTreeMap::new(),
        };
        let Some(values) = values else {
            return self.scan_all(&entry, visit);
        };
        if !unique {
            let mut after = None;
            while let Some((last, rows)) = self.child_page(table, columns, values, after)? {
                after = Some(last);
                for row in rows.into_values() {
                    visit(row).map_err(CoreError::Sql)?;
                }
            }
            return Ok(());
        }
        let primary = table
            .primary_key
            .iter()
            .map(|i| table.columns[*i].id)
            .collect::<Vec<_>>();
        let order = |ids: &[u64]| {
            ids.iter()
                .map(|id| columns.iter().position(|c| c == id))
                .collect::<Option<Vec<_>>>()
        };
        let rows = if primary.len() == columns.len()
            && let Some(order) = order(&primary)
        {
            self.db
                .rows
                .get(
                    self.txn,
                    &entry,
                    &order.iter().map(|i| values[*i].clone()).collect::<Vec<_>>(),
                )?
                .into_iter()
                .collect::<Vec<_>>()
        } else if let Some((d, order)) = self
            .parts
            .indexes
            .iter()
            .filter(|d| {
                d.table == table.id
                    && d.predicate.is_none()
                    && d.columns.len() >= columns.len()
                    && (!unique || d.unique && d.columns.len() == columns.len())
            })
            .find_map(|d| order(&d.columns[..columns.len()]).map(|order| (d, order)))
        {
            let values = order.iter().map(|i| values[*i].clone()).collect::<Vec<_>>();
            let tuple =
                codec::index_tuple_bytes(&values, &vec![codec::Direction::Asc; values.len()])?;
            return engine::indexes::visit(
                self.txn,
                &entry,
                d,
                Bound::Included(tuple.clone()),
                Bound::Included(tuple),
                &mut |_, row| {
                    visit(SqlRow {
                        values: row.values,
                        version: row.version,
                    })
                    .map_err(CoreError::Sql)
                },
            );
        } else if unique {
            return Err(CoreError::Sql(SqlError::InvalidForeignKey));
        } else {
            return self.scan_all(&entry, visit);
        };
        for row in rows {
            visit(SqlRow {
                values: row.values,
                version: row.version,
            })
            .map_err(CoreError::Sql)?;
        }
        Ok(())
    }
    /// Encoded bounds from a read statement's mandatory comparisons (=, <, <=, >, >=) on a
    /// one-column integer primary key, if it has any (a heap table's row id is not one).
    fn primary_key_bounds(&self, table: &TableDef) -> Option<KeyBounds> {
        if let Some(range) = self.scope_of(table).and_then(|(scope, _)| scope.key_range()) {
            return Some((range.lo.clone(), range.hi.clone()));
        }
        use relational::{DataType, Value};
        let [index] = table.primary_key.as_slice() else {
            return None;
        };
        let column = table.columns.get(*index)?;
        if self.writing
            || self.parts.heap.contains(&table.id)
            || !matches!(
                column.data_type,
                DataType::SmallInt | DataType::Integer | DataType::BigInt
            )
            || self
                .parts
                .ext
                .columns
                .get(&column.id)
                .is_some_and(|c| c.type_id.is_some())
        {
            return None;
        }
        // (value, inclusive) for each side; the tighter bound wins.
        let (mut lo, mut hi): (Option<KeySide>, Option<KeySide>) = (None, None);
        let tighter_lo = |old: Option<KeySide>, new: KeySide| match old {
            Some(old) if old.0 > new.0 || (old.0 == new.0 && !old.1) => Some(old),
            _ => Some(new),
        };
        let tighter_hi = |old: Option<KeySide>, new: KeySide| match old {
            Some(old) if old.0 < new.0 || (old.0 == new.0 && !old.1) => Some(old),
            _ => Some(new),
        };
        for (name, op, value) in self.prepared.v2_index_predicates(self.params) {
            let Value::Int(n) = value else { continue };
            if name != column.name || relational::check(&value, column).is_err() {
                continue;
            }
            match op.as_str() {
                "=" => {
                    lo = tighter_lo(lo, (n, true));
                    hi = tighter_hi(hi, (n, true));
                }
                ">" => lo = tighter_lo(lo, (n, false)),
                ">=" => lo = tighter_lo(lo, (n, true)),
                "<" => hi = tighter_hi(hi, (n, false)),
                "<=" => hi = tighter_hi(hi, (n, true)),
                _ => {}
            }
        }
        if lo.is_none() && hi.is_none() {
            return None;
        }
        let bound = |side: Option<KeySide>| -> Option<Bound<Vec<u8>>> {
            Some(match side {
                None => Bound::Unbounded,
                Some((n, inclusive)) => {
                    let key = codec::pk_bytes(&[Value::Int(n)]).ok()?;
                    if inclusive {
                        Bound::Included(key)
                    } else {
                        Bound::Excluded(key)
                    }
                }
            })
        };
        Some((bound(lo)?, bound(hi)?))
    }
    fn scan_index(&self, table: &TableDef) -> Option<IndexRange> {
        self.scan_index_predicates(table, &self.prepared.v2_index_predicates(self.params))
    }
    fn scan_index_predicates(&self, table: &TableDef, predicates: &[(String, String, relational::Value)]) -> Option<IndexRange> {
        (!self.writing).then(|| Self::index_range(self.parts, table, predicates)).flatten()
    }
    fn index_range(parts: &CatalogParts, table: &TableDef, predicates: &[(String, String, relational::Value)]) -> Option<IndexRange> {
        (!predicates.is_empty())
            .then(|| {
                parts.indexes.iter().find_map(|d| {
                    if d.table != table.id || d.predicate.is_some() {
                        return None;
                    }
                    let column = table.columns.iter().find(|c| c.id == d.columns[0])?;
                    // User types can have a SQL order different from their storage type.
                    if parts
                        .ext
                        .columns
                        .get(&column.id)
                        .is_some_and(|c| c.type_id.is_some())
                    {
                        return None;
                    }
                    let binding=parts.ext.column_collations.get(&column.id);
                    let reference=match binding {Some(relational::column_collations::Binding::Column(r))=>Some(r),_=>None};
                    let deterministic= !matches!(reference,Some(relational::column_collations::Reference::User(id)) if parts.ext.collations.get(id).is_some_and(|c|!c.deterministic));
                    let byte_ordered=reference.is_none_or(|r|matches!(r,relational::column_collations::Reference::Builtin(100|811|950|951|962|6411|12359)));
                    // An explicitly collated index can serve only the same semantics as the column.
                    if let Some(relational::column_collations::Binding::Index(rs))=parts.ext.column_collations.get(&d.id)
                        && rs.first().and_then(Option::as_ref)!=reference {return None;
                    }
                    if !deterministic {return None;}
                    let (mut lower, mut upper) = (None, None);
                    let mut usable = false;
                    for (name, op, value) in predicates {
                        if *name != column.name
                            || *value == relational::Value::Null
                            || relational::check(value, column).is_err()
                        {
                            continue;
                        }
                        if op != "=" && !byte_ordered {continue;}
                        let tuple = codec::index_tuple_bytes(
                            std::slice::from_ref(value),
                            &[codec::Direction::Asc],
                        )
                        .ok()?;
                        // Intersect mandatory AND predicates in half-open tuple
                        // space so both sides of a narrow range reach the store.
                        let (lo, hi) = match op.as_str() {
                            "=" => (Some(tuple.clone()), engine::indexes::successor(tuple)),
                            ">" => (Some(engine::indexes::successor(tuple)?), None),
                            ">=" => (Some(tuple), None),
                            "<" => (None, Some(tuple)),
                            "<=" => (None, engine::indexes::successor(tuple)),
                            _ => continue,
                        };
                        usable = true;
                        if let Some(lo) = lo {
                            lower = Some(lower.map_or(lo.clone(), |old: Vec<u8>| old.max(lo)));
                        }
                        if let Some(hi) = hi {
                            upper = Some(upper.map_or(hi.clone(), |old: Vec<u8>| old.min(hi)));
                        }
                    }
                    if let (Some(lo), Some(hi)) = (&lower, &upper)
                        && lo > hi
                    {
                        lower = upper.clone();
                    }
                    usable.then(|| (d.clone(), lower.map_or(Bound::Unbounded, Bound::Included),
                        upper.map_or(Bound::Unbounded, Bound::Excluded)))
                })
            })
            .flatten()
    }
    fn scan_all(
        &mut self,
        entry: &TableEntry,
        visit: &mut dyn FnMut(SqlRow) -> Result<(), SqlError>,
    ) -> Result<(), CoreError> {
        for pair in self.db.rows.scan(
            self.txn,
            entry,
            Bound::Unbounded,
            Bound::Unbounded,
            engine::kv::Direction::Forward,
            None,
        )? {
            let (_, row) = pair?;
            visit(SqlRow {
                values: row.values,
                version: row.version,
            })
            .map_err(CoreError::Sql)?;
        }
        #[cfg(test)]
        AFTER_TABLE_SCAN.with(|hook| {
            if let Some(hook) = hook.borrow_mut().as_mut() {
                hook(&entry.def.name);
            }
        });
        Ok(())
    }
}

#[cfg(test)]
type TableScanHook = Box<dyn FnMut(&str)>;

#[cfg(test)]
thread_local! {
    static DISABLE_MUTATION_POINTS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    pub(super) static MUTATION_PAGE_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    pub(super) static MUTATION_POINT_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static MUTATION_ROWS_READ: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static AFTER_MUTATION_PAGE: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
    static AFTER_INDEX_CHUNK: std::cell::RefCell<Option<Box<dyn Fn()>>> = const { std::cell::RefCell::new(None) };
    static CRASH_AFTER_INDEX_CHUNK: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static AFTER_TABLE_SCAN: std::cell::RefCell<Option<TableScanHook>> = const { std::cell::RefCell::new(None) };
    pub(crate) static USED_INDEX_SCAN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FAIL_AFTER_ROW_WRITE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Cross-checks a protected run's effects against its buffered row writes
/// (R3-609, D-D): the two key sets must be equal.
// Called by `protected_commit` (R3-610); until it lands only tests do.
#[cfg_attr(not(all(test, feature = "engine-rocksdb")), allow(dead_code))]
pub(crate) fn cross_check_effects(
    effects: &crate::task::effects::Effects,
    txn: &Transaction,
) -> Result<(), CoreError> {
    effects.cross_check(txn.written_keys(Keyspace::Rows))
}

/// The native point update that restores one row (GD-0061 D8, R3-802),
/// prepared against the catalog at `txn`'s snapshot: `key` is the primary
/// key's values and `sets` the `(column offset, value)` pairs to write.
pub(crate) fn task_restore_statement(
    db: &v2::V2Db,
    txn: &Transaction,
    table: &relational::TableDef,
    key: Vec<relational::Value>,
    sets: Vec<(usize, relational::Value)>,
) -> Result<relational::Prepared, CoreError> {
    let mut state = db.metadata_state(Some(txn))?;
    state.sql.epoch = 0;
    relational::Prepared::update_engine(table, key, sets, state.view.domain, &state.sql)
        .map_err(CoreError::Sql)
}

impl SqlSession {
    /// C1, the protected-statement seam: runs one DML statement inside the
    /// caller's transaction and never commits. The run is parked on the
    /// session for the duration and handed back with the result.
    // Called by `protected_commit` (R3-610); until it lands only tests do.
    #[cfg_attr(not(all(test, feature = "engine-rocksdb")), allow(dead_code))]
    pub(crate) fn execute_protected_v2(
        &mut self,
        db: &EngineLease,
        run: ProtectedRun,
        p: &relational::Prepared,
        params: &[relational::Value],
        clock: Option<&relational::clock::ExecutionClock>,
    ) -> (ProtectedRun, Result<relational::SqlResult, CoreError>) {
        self.execute_protected_v2_expecting(db, run, p, params, None, clock)
    }

    /// [`Self::execute_protected_v2`] with an `expected_version` for a native
    /// point write (the undo restores, GD-0061 D8, R3-802).
    pub(crate) fn execute_protected_v2_expecting(
        &mut self,
        db: &EngineLease,
        run: ProtectedRun,
        p: &relational::Prepared,
        params: &[relational::Value],
        expected_version: Option<u64>,
        clock: Option<&relational::clock::ExecutionClock>,
    ) -> (ProtectedRun, Result<relational::SqlResult, CoreError>) {
        if let Err(error) = db.check() {
            return (run, Err(error));
        }
        self.bind_sequence_generation(db);
        if self.v2_transaction.is_some()
            || self.staged.is_some()
            || self.copy_input.is_some()
            || self.protected.is_some()
            || !matches!(p.v2_class(), relational::V2Class::Dml)
        {
            return (run, Err(CoreError::Sql(SqlError::Transaction)));
        }
        // A failed statement rolls its writes back to the statement mark, so
        // its partial effects (earlier pages) must be dropped too.
        let before = (!run.effects.is_empty()).then(|| run.effects.clone());
        self.protected = Some(run);
        let _volatile = relational::volatile::Scope::enter(&self.volatile);
        let result = self.execute_v2(db, p, params, expected_version, clock, None);
        let mut run = self.protected.take().expect("parked protected run");
        if result.is_err() {
            run.effects = before.unwrap_or_default();
        }
        (run, result)
    }
}

#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "v2_protected_seam_tests.rs"]
mod protected_seam_tests;

#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "v2_protected_sentinel_tests.rs"]
mod protected_sentinel_tests;

#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "v2_protected_validation_tests.rs"]
mod protected_validation_tests;

#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "v2_protected_fk_tests.rs"]
mod protected_fk_tests;

#[cfg(test)]
thread_local! {
    pub(super) static AFTER_DML_VERSION_RESERVATION: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

#[cfg(all(test, feature = "engine-rocksdb"))]
type OccupancyImageCheck = Box<dyn FnOnce(&CatalogSnapshot, &Arc<relational::Relational>)>;

#[cfg(all(test, feature = "engine-rocksdb"))]
thread_local! {
    static OCCUPANCY_IMAGE: std::cell::RefCell<Option<OccupancyImageCheck>> = const { std::cell::RefCell::new(None) };
}
