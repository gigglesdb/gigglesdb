//! SQL bridge: relational statement checks publish through v2 catalog and row sinks.
use super::*;
use engine::{catalog::CatalogParts, txn::Isolation};
use engine::{catalog::TableEntry, codec, kv::Keyspace, locks::LockMode, txn::Transaction};
use relational::{
    SqlError, SqlRow, TableDef, relation_key,
    row_sink::{DdlRows, RowSink},
    split_key,
};
use std::{collections::BTreeSet, ops::Bound};
#[path = "v2_sql_sequences.rs"]
mod sql_sequences;
#[path = "v2_reduction.rs"]
mod reduction;

impl SqlSession {
    pub(super) fn prepare_v2(
        &mut self,
        sql: &str,
        domain: u128,
    ) -> Result<relational::Prepared, CoreError> {
        relational::assert_sql_frontend_allowed();
        let result = relational::Prepared::parse(sql, domain, 0)
            .map_err(CoreError::Sql)
            .and_then(|mut p| {
                if self.aborted && !p.allows_aborted_transaction() {
                    return Err(CoreError::Sql(relational::SqlError::Aborted));
                }
                p.transaction = self.transaction_id;
                p.set_schema_context(self.search_path.clone(), self.schema_user.clone());
                // Match legacy preparation: authority sees bound dependency names,
                // while semantic errors remain deferred until after authorization.
                if let Ok(bound) = self.bind_relation_context(&p) {
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
        db: &Arc<v2::V2Db>,
        prepared: &relational::Prepared,
        rows: Vec<Vec<relational::Value>>,
        clock: &relational::StatementClock,
    ) -> Result<(relational::Prepared, Vec<Vec<relational::Value>>), CoreError> {
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
            let context = relational::clock::ExecutionClock::new(clock)
                .with_extra_float_digits(self.extra_float_digits)
                .with_sequences(sql_sequences::callback(
                    db.clone(),
                    state.sql.clone(),
                    self.sequence_baseline.clone(),
                    self.pending_sequences.clone(),
                    self.sequence_values.clone(),
                ));
            let output = state.sql.normalize_copy(prepared, rows, Some(&context));
            crate::cancellation::check()?;
            if let Some(error) = context.take_error() {
                return Err(error);
            }
            output.map_err(CoreError::Sql)
        })();
        if result.is_err() {
            self.abort();
        }
        result
    }

    pub(super) fn execute_v2(
        &mut self,
        db: &Arc<v2::V2Db>,
        p: &relational::Prepared,
        params: &[relational::Value],
        expected_version: Option<u64>,
        clock: Option<&relational::clock::ExecutionClock>,
    ) -> Result<relational::SqlResult, CoreError> {
        use relational::{SqlError as E, V2Class};
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
        let _ticket = match inner.enter_sql() {
            Ok(ticket) => ticket,
            Err(error) => {
                self.abort();
                return Err(error);
            }
        };
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
                            if self.v2_transaction.is_some() {
                                return Err(CoreError::Sql(E::Transaction));
                            }
                            let mut token = [0u8; 16];
                            getrandom::fill(&mut token).map_err(io)?;
                            let mut txn = db.begin(Isolation::ReadCommitted);
                            self.sequence_baseline = db
                                .catalog()
                                .parts(&mut txn)?
                                .ext
                                .sequences
                                .iter()
                                .map(|(&id, def)| (id, def.incarnation))
                                .collect();
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
                            self.v2_transaction = Some(txn);
                            self.transaction_id = Some(u128::from_be_bytes(token));
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
                                    self.v2_savepoints.truncate(index);
                                } else {
                                    txn.rollback_to(&mark)?;
                                    self.deferral = self.v2_savepoints[index].2.clone();
                                    self.v2_savepoints.truncate(index + 1);
                                    self.aborted = false;
                                }
                            }
                        }
                        "commit" => {
                            // Publish live sequence positions before another session can
                            // allocate from the newly committed durable reservation.
                            let mut live = crate::cancellation::lock(&db.sql_counters)?;
                            let txn = self
                                .v2_transaction
                                .take()
                                .ok_or(CoreError::Sql(E::Transaction))?;
                            inner.sql_transactions.fetch_sub(1, Ordering::AcqRel);
                            let mut txn = txn;
                            let validation = validate_pending(db, &mut txn, &mut self.deferral);
                            if validation.is_err() {
                                drop(live);
                                self.rollback();
                                return validation.map(|()| Default::default());
                            }
                            let point = *inner.crash.lock().map_err(|_| CoreError::Fenced)?;
                            let committed =
                                sql_sequences::stage_pending(db, &mut txn, &self.pending_sequences)
                                    .and_then(|()| commit_sql(db, txn, point));
                            if committed.is_ok() {
                                live.extend(
                                    self.pending_sequences
                                        .lock()
                                        .unwrap_or_else(|e| e.into_inner())
                                        .clone(),
                                );
                            }
                            drop(live);
                            self.rollback();
                            committed?;
                            acknowledge_sql_commit(point)?;
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
                    let txn = match &mut self.v2_transaction {
                        Some(txn) => txn,
                        None => implicit.insert(db.begin(Isolation::ReadCommitted)),
                    };
                    if !self.copy_input.as_ref().is_some_and(|input| input.started) {
                        txn.begin_statement();
                    }
                    let mark = txn.savepoint();
                    let result = (|| {
                        // DDL stages a catalog-wide image (including schema cascades and
                        // new FK edges), so retain its exclusive catalog barrier. DML
                        // shares this sentinel regardless of whether any FK exists.
                        if writing {
                            txn.acquire_lock(
                                engine::locks::LockKey::Table(0),
                                if ddl {
                                    LockMode::AccessExclusive
                                } else {
                                    LockMode::RowExclusive
                                },
                                None,
                                &|| crate::cancellation::check().is_err(),
                            )?;
                            // Refresh AFTER waiting: DDL may have introduced an FK.
                            if !self.copy_input.as_ref().is_some_and(|input| input.started) {
                                txn.begin_statement();
                            }
                        }
                        // Lock before choosing the statement snapshot/image. SQL mutations
                        // serialize per table; readers retain MVCC snapshot visibility.
                        let parts = db.catalog().parts(txn)?;
                        // Bind from the original references, preserving explicit public
                        // qualification even when another schema shadows the bare name.
                        let mut binding = relational::Relational::catalog_image(
                            parts.tables.clone(),
                            parts.ext.clone(),
                            u64::MAX,
                            u64::MAX,
                        )
                        .map_err(|error| match error {
                            E::Unsupported => CoreError::Sql(E::Unsupported),
                            error => {
                                CoreError::Corrupt(format!("invalid stored v2 catalog: {error:?}"))
                            }
                        })?;
                        binding
                            .catalog_indexes(&parts.indexes)
                            .map_err(CoreError::Sql)?;
                        let bound = p.bind_schemas(&binding).map_err(CoreError::Sql)?;
                        let target_name = bound.table().map(str::to_owned);
                        let mut names = bound.read_tables().into_iter().collect::<BTreeSet<_>>();
                        let truncating = bound.command() == "TRUNCATE TABLE";
                        if truncating {
                            names.extend(bound.truncate_tables(&binding).map_err(CoreError::Sql)?);
                        }
                        if let Some(name) = &target_name {
                            names.insert(name.clone());
                        }
                        let target_id = target_name
                            .as_ref()
                            .and_then(|name| parts.tables.get(name))
                            .map(|t| t.id);
                        // Include the whole FK component: cascades can mutate children
                        // and probe their other parents. Share conflicts with each
                        // table's RowExclusive writer, while parent witnesses can share.
                        let mut connected = target_id.into_iter().collect::<BTreeSet<_>>();
                        if writing {
                            loop {
                                let count = connected.len();
                                for (&child, constraints) in &parts.ext.constraints {
                                    for c in constraints {
                                        if let relational::catalog_ext::ConstraintKind::ForeignKey {
                                            ref_table,
                                            ..
                                        } = &c.kind
                                            && (connected.contains(&child)
                                                || connected.contains(ref_table))
                                        {
                                            connected.extend([child, *ref_table]);
                                        }
                                    }
                                }
                                if connected.len() == count {
                                    break;
                                }
                            }
                        }
                        let mut locks = BTreeMap::new();
                        for name in &names {
                            if let Some(table) = parts.tables.get(name) {
                                locks.insert(
                                    table.id,
                                    if truncating {
                                        LockMode::AccessExclusive
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
                                    } else if Some(id) == target_id {
                                        LockMode::RowExclusive
                                    } else {
                                        LockMode::Share
                                    },
                                );
                            }
                        }
                        // A single table-id order includes read sources and FK witnesses.
                        let table_names = parts
                            .tables
                            .iter()
                            .map(|(name, table)| (table.id, name))
                            .collect::<BTreeMap<_, _>>();
                        for (id, mode) in locks {
                            db.catalog()
                                .resolve(txn, table_names[&id], mode, None, &|| {
                                    crate::cancellation::check().is_err()
                                })
                                .map_err(|e| match e {
                                    engine::catalog::CatalogError::Core(e) => e,
                                    engine::catalog::CatalogError::Txn(e) => e.into(),
                                })?;
                        }
                        if writing
                            && !ddl
                            && let Some(table) =
                                target_name.as_ref().and_then(|name| parts.tables.get(name))
                        {
                            // Empty PK cannot name a real row. Serialize SQL writers
                            // only on the destination; source readers remain MVCC.
                            txn.lock_for_write(
                                Keyspace::Rows,
                                &table.id.to_be_bytes(),
                                None,
                                &|| crate::cancellation::check().is_err(),
                            )?;
                        }
                        // Choose ONE snapshot after every potentially blocking lock.
                        // No refresh occurs while staging any of the statement's rows.
                        if !self.copy_input.as_ref().is_some_and(|input| input.started) {
                            txn.begin_statement();
                        }
                        let old = db.catalog().parts(txn)?;
                        // This bounds every ID E1 DDL allocates per statement.
                        let ids = if ddl {
                            db.reserve_sql_ids(txn, 3 * relational::MAX_COLUMNS as u64 + 64)?
                        } else {
                            // DML/reads allocate no catalog objects. Derive only the
                            // validation ceiling, without a durable ID reservation.
                            let floor = old
                                .tables
                                .values()
                                .flat_map(|table| {
                                    std::iter::once(table.id)
                                        .chain(table.columns.iter().map(|column| column.id))
                                })
                                .chain(old.indexes.iter().map(|index| index.id))
                                .chain(old.ext.constraints.values().flatten().map(|c| c.id))
                                .chain(old.ext.sequences.keys().copied())
                                .chain(old.ext.types.iter().map(|(&id, ty)| match ty {
                                    relational::TypeDef::Enum { labels, .. } => {
                                        id.saturating_add(labels.len() as u64)
                                    }
                                    _ => id,
                                }))
                                .chain(old.ext.schemas.values().map(|schema| schema.id))
                                .max()
                                .unwrap_or(0)
                                .checked_add(1)
                                .ok_or(CoreError::Sql(E::Overflow))?;
                            floor..floor
                        };
                        let versions = if matches!(class, V2Class::Dml) {
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
                        let mut image = relational::Relational::catalog_image(
                            old.tables.clone(),
                            old.ext.clone(),
                            ids.start,
                            versions.start,
                        )
                        .map_err(|error| match error {
                            E::Unsupported => CoreError::Sql(E::Unsupported),
                            error => {
                                CoreError::Corrupt(format!("invalid stored v2 catalog: {error:?}"))
                            }
                        })?;
                        image
                            .catalog_indexes(&old.indexes)
                            .map_err(CoreError::Sql)?;
                        let context = clock.map(|clock| {
                            clock.with_sequences(sql_sequences::callback(
                                db.clone(),
                                image.clone(),
                                self.sequence_baseline.clone(),
                                self.pending_sequences.clone(),
                                self.sequence_values.clone(),
                            ))
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
                                    parts: &old,
                                    writing,
                                    prepared: &bound,
                                    params,
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
                        if !writing && image.streaming_reduction(&bound) {
                            let source = reduction::Source::new(SqlRows {
                                conflict_floor: None, db, txn, parts: &old,
                                writing, prepared: &bound, params,
                            });
                            let result = image.execute_reduction(
                                p, params, expected_version, context.as_ref(), &source);
                            if let Some(error) = source.error.take() { return Err(error); }
                            if let Some(error) = context.as_ref().and_then(|c| c.take_error()) { return Err(error); }
                            return result.map_err(CoreError::Sql);
                        }
                        let result = image.execute_with_sink(
                            p,
                            params,
                            expected_version,
                            context.as_ref(),
                            &mut SqlRows {
                                conflict_floor: None,
                                db,
                                txn,
                                parts: &old,
                                writing,
                                prepared: &bound,
                                params,
                            },
                            versions.end,
                            if self.transaction_id.is_some() {
                                Some(&mut self.deferral)
                            } else {
                                None
                            },
                        );
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
                        let (tables, ext, next_id, _) = image.catalog_parts();
                        if p.truncate_restarts_identity() {
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
                                        values.get(&(id, before.incarnation)).copied()
                                    {
                                        values.insert((id, definition.incarnation), value);
                                    }
                                }
                            }
                        }
                        if next_id > ids.end {
                            return Err(CoreError::Corrupt(
                                "v2 DDL exceeded ID reservation".into(),
                            ));
                        }
                        check_post_image(&tables, &ext, next_id)?;
                        let heap = tables
                            .values()
                            .filter(|def| def.hidden_rowid().is_some())
                            .map(|def| def.id)
                            .collect();
                        let indexes = image.secondary_descriptors_engine().to_vec();
                        let new = CatalogParts {
                            tables,
                            heap,
                            ext,
                            indexes,
                        };
                        txn.reclaim_after_commit |=
                            old.tables
                                .values()
                                .any(|table| !new.tables.values().any(|next| next.id == table.id))
                                || old.indexes.iter().any(|index| {
                                    !new.indexes.iter().any(|next| next.id == index.id)
                                });
                        db.catalog().apply_diff(txn, &old, &new, &ids, None, &|| {
                            crate::cancellation::check().is_err()
                        })?;
                        roles::engine::assign_created(txn, &old, &new, self.ddl_owner.as_deref())?;
                        Ok(result)
                    })();
                    if result.is_err() {
                        txn.rollback_to(&mark).map_err(CoreError::from)?;
                    }
                    txn.release(mark).map_err(CoreError::from)?;
                    if self.v2_transaction.is_none() && result.is_ok() {
                        let point = *inner.crash.lock().map_err(|_| CoreError::Fenced)?;
                        commit_sql(db, implicit.take().expect("implicit transaction"), point)?;
                        acknowledge_sql_commit(point)?;
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
            self.transaction_clock_us = None;
        }
        result
    }
}

// SQL publication only: counter reservations and non-SQL engine commits do not
// use this seam. None adds only the crash-point read and untaken fault branches.
fn commit_sql(db: &v2::V2Db, txn: Transaction, point: CrashPoint) -> Result<(), CoreError> {
    if point == CrashPoint::BeforeCommit {
        crash_now();
    }
    if point == CrashPoint::ErrorBeforeCommit {
        return Err(CoreError::Storage("injected before commit".into()));
    }
    let reclaim = txn.reclaim_after_commit;
    db.catalog().commit_ddl(txn).map_err(CoreError::from)?;
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

fn validate_pending(
    db: &v2::V2Db,
    txn: &mut Transaction,
    deferred: &mut relational::DeferralState,
) -> Result<(), CoreError> {
    if deferred.pending.is_empty() {
        return Ok(());
    }
    let parts = db.catalog().parts(txn)?;
    let mut image = relational::Relational::catalog_image(
        parts.tables.clone(),
        parts.ext.clone(),
        u64::MAX,
        u64::MAX,
    )
    .map_err(CoreError::Sql)?;
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
            parts: &parts,
            writing: true,
            prepared: &p,
            params: &[],
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
    tables: &BTreeMap<String, relational::TableDef>,
    ext: &relational::catalog_ext::CatalogExt,
    next_id: u64,
) -> Result<(), CoreError> {
    relational::Relational::catalog_image(tables.clone(), ext.clone(), next_id, 1)
        .map(|_| ())
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

/// Transient statement images use legacy keys. Only this sink translates them
/// into the existing v2 key/row codec; it never persists relational image bytes.
type IndexRange = (relational::secondary_index::Descriptor, Bound<Vec<u8>>, Bound<Vec<u8>>);

struct SqlRows<'a> {
    db: &'a v2::V2Db,
    txn: &'a mut Transaction,
    parts: &'a CatalogParts,
    writing: bool,
    conflict_floor: Option<u64>,
    prepared: &'a relational::Prepared,
    params: &'a [relational::Value],
}
impl RowSink for SqlRows<'_> {
    fn check_fk_snapshot(&self) -> Result<(), CoreError> {
        if self.txn.isolation() != Isolation::ReadCommitted {
            return Err(CoreError::Sql(SqlError::Conflict));
        }
        Ok(())
    }

    fn streams_index_builds(&self) -> bool {
        true
    }
    fn read_page(
        &mut self,
        table: &TableDef,
        after: Option<Vec<u8>>,
    ) -> Result<Option<(Vec<u8>, DdlRows)>, CoreError> {
        crate::cancellation::check()?;
        let entry = TableEntry {
            def: table.clone(),
            heap: self.parts.heap.contains(&table.id),
            columns_ext: BTreeMap::new(),
        };
        let rows = self
            .db
            .rows
            .scan(
                self.txn,
                &entry,
                after.map_or(Bound::Unbounded, Bound::Excluded),
                Bound::Unbounded,
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
    fn ddl_pages(
        &mut self,
        table: &TableDef,
        after: &TableDef,
        mutating: bool,
        page: &mut dyn FnMut(DdlRows) -> Result<DdlRows, CoreError>,
    ) -> Result<(), CoreError> {
        let entry = TableEntry {
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
            let rows = rows
                .into_values()
                .map(|row| relational::row_sink::row_key(table, &row).map(|key| (key, row)))
                .collect::<Result<BTreeMap<_, _>, _>>()
                .map_err(CoreError::Sql)?;
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
            let entries = engine::indexes::entries(table, &rows, std::slice::from_ref(index))?;
            // The outer transaction holds the DDL table locks. Chunk commits
            // publish only entries under a fresh, durably reserved ID; queries
            // cannot discover it until the outer catalog commit succeeds.
            // Rollback/crash can leave unreachable entries, never a partial
            // visible index. No catalog or row bytes are committed by a chunk.
            let mut chunk = self.db.begin(Isolation::ReadCommitted);
            chunk.require_features(engine::meta::SECONDARY_INDEXES);
            for (key, pk) in entries {
                let target = chunk.lock_for_write(Keyspace::Index, &key, None, &|| {
                    crate::cancellation::check().is_err()
                })?;
                if target.current.is_some() {
                    return Err(CoreError::Sql(SqlError::Duplicate));
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
    fn lookup(
        &mut self,
        table: &TableDef,
        columns: &[u64],
        values: Option<&[relational::Value]>,
        unique: bool,
        visit: &mut dyn FnMut(SqlRow) -> Result<(), SqlError>,
    ) -> Result<(), CoreError> {
        let entry = TableEntry {
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
    fn child_page(&mut self, table: &TableDef, columns: &[u64], values: &[relational::Value],
        after: Option<Vec<u8>>) -> Result<Option<(Vec<u8>, DdlRows)>, CoreError> {
        crate::cancellation::check()?;
        let primary = table.primary_key.iter().map(|i| table.columns[*i].id).collect::<Vec<_>>();
        let matches = |ids: &[u64]| ids.len() >= columns.len()
            && ids[..columns.len()].iter().all(|id| columns.contains(id));
        let primary_matches = matches(&primary) && !values.contains(&relational::Value::Null);
        let descriptor = self.parts.indexes.iter().find(|d| d.table == table.id
            && d.columns.len() == columns.len() && matches(&d.columns))
            .or_else(|| (!primary_matches).then(|| self.parts.indexes.iter()
                .find(|d| d.table == table.id && matches(&d.columns))).flatten());
        let page = if descriptor.is_some() || primary_matches {
            let ids = descriptor.map_or(primary.as_slice(), |d| d.columns.as_slice());
            let ordered = ids[..columns.len()].iter().map(|id|
                values[columns.iter().position(|c| c == id).unwrap()].clone()).collect::<Vec<_>>();
            let entry = TableEntry { def: table.clone(), heap: self.parts.heap.contains(&table.id), columns_ext: BTreeMap::new() };
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
                    &mut |_, row| collect(row))?
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
    fn conflict_lookup(
        &mut self,
        table: &TableDef,
        columns: &[u64],
        values: &[relational::Value],
        visit: &mut dyn FnMut(SqlRow) -> Result<(), SqlError>,
    ) -> Result<(), CoreError> {
        let floor = self.conflict_floor;
        self.lookup(table, columns, Some(values), true, &mut |row| {
            if floor.is_some_and(|v| row.version >= v) { return Err(SqlError::CardinalityViolation); }
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
        engine::indexes::apply(
            self.txn,
            table,
            after_table,
            before,
            after,
            &self.parts.indexes,
            descriptors,
            None,
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
        let entry = TableEntry {
            def: table.clone(),
            heap: self.parts.heap.contains(&table.id),
            columns_ext: BTreeMap::new(),
        };
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
    }
    fn check_occupancy(
        &mut self,
        table: &TableDef,
        before: &BTreeMap<Vec<u8>, SqlRow>,
        after: &BTreeMap<Vec<u8>, SqlRow>,
    ) -> Result<(), CoreError> {
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
                .lock_for_write(Keyspace::Rows, &physical, None, &|| {
                    crate::cancellation::check().is_err()
                })?
                .current
                .is_some()
            {
                return Err(CoreError::Sql(SqlError::Duplicate));
            }
        }
        engine::indexes::check_occupancy(self.txn, table, before, after, &self.parts.indexes)
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
        for (key, (old, _)) in &changes {
            crate::cancellation::check()?;
            let target = self.txn.lock_for_write(Keyspace::Rows, key, None, &|| {
                crate::cancellation::check().is_err()
            })?;
            match (old, target.current) {
                (None, Some(_)) => return Err(CoreError::Sql(SqlError::Duplicate)),
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
        for (key, (_, new)) in changes {
            crate::cancellation::check()?;
            match new {
                Some(row) => self.txn.put(
                    Keyspace::Rows,
                    key.clone(),
                    codec::encode_row(&key, row.version, &row.values)?,
                )?,
                None => self.txn.delete(Keyspace::Rows, key)?,
            }
            #[cfg(test)]
            if FAIL_AFTER_ROW_WRITE.replace(false) {
                return Err(CoreError::Cancelled);
            }
        }
        Ok(())
    }
}

impl SqlRows<'_> {
    fn scan_index(&self, table: &TableDef) -> Option<IndexRange> {
        let predicates = self.prepared.v2_index_predicates(self.params);
        (!self.writing)
            .then(|| {
                self.parts.indexes.iter().find_map(|d| {
                    if d.table != table.id {
                        return None;
                    }
                    let column = table.columns.iter().find(|c| c.id == d.columns[0])?;
                    // User types can have a SQL order different from their storage type.
                    if self
                        .parts
                        .ext
                        .columns
                        .get(&column.id)
                        .is_some_and(|c| c.type_id.is_some())
                    {
                        return None;
                    }
                    predicates.iter().find_map(|(name, op, value)| {
                        if *name != column.name
                            || *value == relational::Value::Null
                            || relational::check(value, column).is_err()
                        {
                            return None;
                        }
                        let tuple = codec::index_tuple_bytes(
                            std::slice::from_ref(value),
                            &[codec::Direction::Asc],
                        )
                        .ok()?;
                        let (lo, hi) = match op.as_str() {
                            "=" => (Bound::Included(tuple.clone()), Bound::Included(tuple)),
                            ">" => (Bound::Excluded(tuple), Bound::Unbounded),
                            ">=" => (Bound::Included(tuple), Bound::Unbounded),
                            "<" => (Bound::Unbounded, Bound::Excluded(tuple)),
                            "<=" => (Bound::Unbounded, Bound::Included(tuple)),
                            _ => return None,
                        };
                        Some((d.clone(), lo, hi))
                    })
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
    static AFTER_INDEX_CHUNK: std::cell::RefCell<Option<Box<dyn Fn()>>> = const { std::cell::RefCell::new(None) };
    static CRASH_AFTER_INDEX_CHUNK: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static AFTER_TABLE_SCAN: std::cell::RefCell<Option<TableScanHook>> = const { std::cell::RefCell::new(None) };
    static USED_INDEX_SCAN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FAIL_AFTER_ROW_WRITE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
