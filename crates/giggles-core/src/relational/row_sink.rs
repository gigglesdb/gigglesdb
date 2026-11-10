//! Bounded multi-table statement staging over an external row store.
//!
//! The legacy publisher checks complete pre/postimages (including immediate PK
//! occupancy, witness ownership, work and byte limits). Reuse those checks in a
//! transient statement image, then publish the validated difference through the
//! caller's transaction. This adds no fields to the serialized Relational image.
use super::*;
use crate::CoreError;

pub(crate) type DdlRows = BTreeMap<Vec<u8>, SqlRow>;

pub(crate) fn row_key(table: &TableDef, row: &SqlRow) -> R<Vec<u8>> {
    pk(table, &row.values)
}

pub(crate) trait RowSink {
    /// FK witnesses need a fresh snapshot after acquiring their table locks.
    fn check_fk_snapshot(&self) -> Result<(), CoreError> {
        Ok(())
    }

    fn scan(
        &mut self,
        table: &TableDef,
        visit: &mut dyn FnMut(SqlRow) -> R<()>,
    ) -> Result<(), CoreError>;

    /// Exact constraint key; None requests the bounded typed-comparison fallback.
    fn lookup(
        &mut self,
        table: &TableDef,
        _columns: &[u64],
        _values: Option<&[Value]>,
        _unique: bool,
        visit: &mut dyn FnMut(SqlRow) -> R<()>,
    ) -> Result<(), CoreError> {
        self.scan(table, visit)
    }

    fn indexes(&self) -> Vec<secondary_index::Descriptor> {
        Vec::new()
    }
    fn streams_index_builds(&self) -> bool {
        false
    }
    fn ddl_pages(
        &mut self,
        _table: &TableDef,
        _after: &TableDef,
        _mutating: bool,
        _page: &mut dyn FnMut(DdlRows) -> Result<DdlRows, CoreError>,
    ) -> Result<(), CoreError> {
        Err(CoreError::Sql(SqlError::Unsupported))
    }
    fn backfill_index(
        &mut self,
        _table: &TableDef,
        _index: &secondary_index::Descriptor,
    ) -> Result<(), CoreError> {
        Err(CoreError::Sql(SqlError::Unsupported))
    }
    fn apply_indexes(
        &mut self,
        _table: &TableDef,
        _after_table: Option<&TableDef>,
        _before: &BTreeMap<Vec<u8>, SqlRow>,
        _after: &BTreeMap<Vec<u8>, SqlRow>,
        _descriptors: &[secondary_index::Descriptor],
    ) -> Result<(), CoreError> {
        Ok(())
    }
    /// Publish the difference atomically; the caller owns a statement savepoint.
    fn apply(
        &mut self,
        table: &TableDef,
        before: &BTreeMap<Vec<u8>, SqlRow>,
        after: &BTreeMap<Vec<u8>, SqlRow>,
    ) -> Result<(), CoreError>;
}

impl Relational {
    #[expect(
        clippy::too_many_arguments,
        reason = "statement inputs plus transaction-local deferral"
    )]
    pub(crate) fn execute_with_sink(
        &mut self,
        prepared: &Prepared,
        params: &[Value],
        expected: Option<u64>,
        clock: Option<&clock::ExecutionClock>,
        sink: &mut impl RowSink,
        version_end: u64,
        deferred: Option<&mut DeferralState>,
    ) -> Result<SqlResult, CoreError> {
        let bound = prepared.bind_schemas(self).map_err(CoreError::Sql)?;
        let prepared = &bound;
        if expected.is_none() && sink.streams_index_builds() && paged_dml::eligible(self, prepared)
        {
            return paged_dml::execute(self, prepared, params, clock, sink, version_end);
        }
        let streamed_ddl = match &prepared.statement {
            Statement::DropIndex(_) | Statement::DropTable { .. } => true,
            Statement::AlterTable(alter) => alter.actions.iter().all(|action| {
                matches!(
                    action,
                    alter_table::Action::Add(..)
                        | alter_table::Action::NotNull(..)
                        | alter_table::Action::Constraint(constraints::Definition::Check { .. })
                )
            }),
            _ => false,
        };
        if streamed_ddl && sink.streams_index_builds() {
            let original = self.clone();
            let result = self
                .execute_statement_inner(
                    prepared,
                    params,
                    expected,
                    || crate::cancellation::check().is_err(),
                    clock,
                    None,
                    &mut Vec::new(),
                )
                .map_err(CoreError::Sql)?;
            if let Statement::AlterTable(alter) = &prepared.statement {
                let table = &original.tables[&alter.table];
                sink.ddl_pages(table, &self.tables[&alter.table], false, &mut |rows| {
                    let mut page = original.clone();
                    page.rows.insert(table.id, rows);
                    page.execute_statement_inner(
                        prepared,
                        params,
                        expected,
                        || crate::cancellation::check().is_err(),
                        clock,
                        None,
                        &mut Vec::new(),
                    )
                    .map_err(CoreError::Sql)?;
                    Ok(page.rows.remove(&table.id).unwrap_or_default())
                })?;
            }
            // Dropped rows/index entries are unreachable once the outer catalog
            // transaction commits; physical reclamation is snapshot-safe GC work.
            return Ok(result);
        }
        let index_build = match &prepared.statement {
            Statement::CreateIndex(_) => true,
            Statement::AlterTable(alter) => alter.actions.iter().all(|action| {
                matches!(
                    action,
                    alter_table::Action::Constraint(constraints::Definition::Unique { .. })
                        | alter_table::Action::Unique(_, _)
                )
            }),
            _ => false,
        };
        if index_build && sink.streams_index_builds() {
            let old = sink.indexes();
            let result = self
                .execute_statement_inner(
                    prepared,
                    params,
                    expected,
                    || crate::cancellation::check().is_err(),
                    clock,
                    None,
                    &mut Vec::new(),
                )
                .map_err(CoreError::Sql)?;
            for index in self.secondary_descriptors_engine() {
                if !old.iter().any(|old| old.id == index.id) {
                    let table = self
                        .tables
                        .values()
                        .find(|table| table.id == index.table)
                        .ok_or(CoreError::Sql(SqlError::UnknownTable))?;
                    sink.backfill_index(table, index)?;
                }
            }
            return Ok(result);
        }
        // Keep the mutation target separate from the read set: a join's first
        // source must never be published, and INSERT ... SELECT still needs its
        // destination's complete occupancy image for immediate PK checks.
        let table = (!matches!(prepared.v2_class(), V2Class::Read))
            .then(|| {
                prepared
                    .table()
                    .and_then(|name| self.tables.get(name))
                    .cloned()
            })
            .flatten();
        let mut names = if let Statement::TypeDdl(user_types::Ddl::Drop {
            names,
            cascade: true,
            ..
        }) = &prepared.statement
        {
            let ids: BTreeSet<_> = names
                .iter()
                .filter_map(|name| {
                    self.resolve_type(name, &prepared.search_path)
                        .ok()
                        .flatten()
                        .map(|(id, _)| id)
                })
                .collect();
            self.tables
                .iter()
                .filter(|(_, table)| {
                    table.columns.iter().any(|column| {
                        self.ext
                            .columns
                            .get(&column.id)
                            .and_then(|c| c.type_id)
                            .is_some_and(|id| ids.contains(&id))
                    })
                })
                .map(|(key, _)| key.clone())
                .collect()
        } else if matches!(prepared.v2_class(), V2Class::Ddl) {
            Vec::new()
        } else {
            prepared.read_tables()
        };
        if let Some(table) = &table
            && !names.contains(&self.table_key(table))
        {
            names.push(self.table_key(table));
        }
        for (_, table_id) in deferred.as_ref().into_iter().flat_map(|d| d.pending.keys()) {
            if let Some(table) = self.tables.values().find(|t| t.id == *table_id) {
                let name = self.table_key(table);
                if !names.contains(&name) {
                    names.push(name);
                }
            }
        }
        // One budget across ALL scanned relations, including nested subqueries.
        // The target preimage and legacy execution retain A1's separate bounds.
        let mut retention = scan::Retention::new(scan::retention_limit());
        for name in names {
            let Some(table) = self.tables.get(&name) else {
                // Binding owns SQL error precedence (including unknown tables).
                continue;
            };
            let is_target = prepared.table() == Some(self.table_key(table).as_str());
            let rows = self
                .rows
                .get_mut(&table.id)
                .ok_or(CoreError::Sql(SqlError::UnknownTable))?;
            sink.scan(table, &mut |row| {
                if crate::cancellation::check().is_err() {
                    return Err(SqlError::Aborted);
                }
                // Only destination rows constrain its version allocator. A
                // concurrent source writer may hold a newer reserved range.
                if is_target {
                    self.next_version = self
                        .next_version
                        .max(row.version.checked_add(1).ok_or(SqlError::Overflow)?);
                }
                let key = pk(table, &row.values)?;
                retention.charge(searched::retained_row_bytes(&key, &row)?)?;
                if rows.insert(key, row).is_some() {
                    return Err(SqlError::Duplicate);
                }
                Ok(())
            })?;
        }
        self.secondary = self
            .secondary_build_complete_engine(&sink.indexes(), || {
                crate::cancellation::check().is_err()
            })
            .map_err(CoreError::Sql)?;
        let writing = !matches!(prepared.v2_class(), V2Class::Read);
        let mut before = self.clone();
        let mut edges = self.ext.constraints.clone();
        // Resolve a new ALTER FK before its existing-row validation executes.
        if let Statement::AlterTable(alter) = &prepared.statement {
            let mut empty = self.clone();
            empty.rows.values_mut().for_each(BTreeMap::clear);
            empty.secondary = empty
                .secondary_build_complete_engine(&sink.indexes(), || {
                    crate::cancellation::check().is_err()
                })
                .map_err(CoreError::Sql)?;
            for action in &alter.actions {
                if let alter_table::Action::Constraint(
                    definition @ constraints::Definition::ForeignKey { .. },
                ) = action
                    && let Some(table) = empty.tables.get(&alter.table).cloned()
                {
                    constraints::install(&mut empty, &table, definition, &mut || {
                        crate::cancellation::check().is_err()
                    })
                    .map_err(CoreError::Sql)?;
                    edges = empty.ext.constraints.clone();
                }
            }
        }
        if writing {
            while hydrate(
                &self.clone(),
                &edges,
                &mut before,
                self,
                sink,
                &mut retention,
            )? > 0
            {}
        }
        rebuild(&mut before, &sink.indexes())?;
        rebuild(self, &sink.indexes())?;
        let mut pending = deferred.as_deref().cloned().unwrap_or_default();
        let result = if let Some((names, mode)) = prepared.set_constraints() {
            if deferred.is_none() {
                return Err(CoreError::Sql(SqlError::NoActiveTransaction));
            }
            if !params.is_empty() || expected.is_some() {
                return Err(CoreError::Sql(SqlError::Parameters));
            }
            self.set_constraints(&mut pending, names, mode, || {
                crate::cancellation::check().is_err()
            })
            .map_err(CoreError::Sql)?;
            SqlResult::default()
        } else {
            let mut events = Vec::new();
            let result = self
                .execute_statement_inner(
                    prepared,
                    params,
                    expected,
                    || crate::cancellation::check().is_err(),
                    clock,
                    None,
                    &mut events,
                )
                .map_err(CoreError::Sql)?;
            if writing {
                let descriptors = self.secondary_descriptors_engine().to_vec();
                let mut source_error = None;
                let enforced = constraints::enforce_with_rows(
                    self,
                    std::borrow::Cow::Owned(before),
                    events,
                    &mut pending,
                    &mut || crate::cancellation::check().is_err(),
                    clock,
                    Some(&mut |state, old| {
                        let result = (|| {
                            loop {
                                let seeds = state.clone();
                                if hydrate(
                                    &seeds,
                                    &seeds.ext.constraints,
                                    old,
                                    state,
                                    sink,
                                    &mut retention,
                                )? == 0
                                {
                                    break;
                                }
                                rebuild(old, &sink.indexes())?;
                                rebuild(state, &descriptors)?;
                            }
                            Ok(())
                        })();
                        result.map_err(|error| {
                            source_error = Some(error);
                            SqlError::Aborted
                        })
                    }),
                );
                if let Some(error) = source_error {
                    return Err(error);
                }
                before = enforced.map_err(CoreError::Sql)?.into_owned();
                if deferred.is_none() {
                    self.validate_deferred(&mut pending, || crate::cancellation::check().is_err())
                        .map_err(CoreError::Sql)?;
                }
            }
            result
        };
        if self.next_version > version_end {
            return Err(CoreError::Corrupt(
                "v2 SQL exceeded row-version reservation".into(),
            ));
        }
        if writing {
            for table in before.tables.values() {
                let old = &before.rows[&table.id];
                let empty = BTreeMap::new();
                let after = self.rows.get(&table.id).unwrap_or(&empty);
                let after_table = self.tables.values().find(|t| t.id == table.id);
                if old == after
                    && after_table == Some(table)
                    && before.secondary_descriptors_engine() == self.secondary_descriptors_engine()
                {
                    continue;
                }
                sink.apply_indexes(
                    table,
                    after_table,
                    old,
                    after,
                    self.secondary_descriptors_engine(),
                )?;
                if after_table.is_none_or(|new| {
                    new.primary_key == table.primary_key && new.columns.len() == table.columns.len()
                }) {
                    sink.apply(table, old, after)?;
                }
            }
        }
        if let Some(deferred) = deferred {
            *deferred = pending;
        }
        Ok(result)
    }
}

#[path = "row_sink_constraints.rs"]
mod constraint_rows;
#[path = "row_sink_dml.rs"]
mod paged_dml;
use constraint_rows::{hydrate, rebuild};

#[cfg(test)]
mod tests {
    use super::*;

    struct WideSources {
        scans: Vec<(String, usize)>,
    }
    impl RowSink for WideSources {
        fn scan(
            &mut self,
            table: &TableDef,
            visit: &mut dyn FnMut(SqlRow) -> R<()>,
        ) -> Result<(), CoreError> {
            self.scans.push((table.name.clone(), 0));
            // Each table is below both MAX_ROWS and 64 MiB. Together they exceed
            // the bridge's shared budget; reject before running the cross join.
            for id in 0..600 {
                self.scans.last_mut().unwrap().1 += 1;
                visit(SqlRow {
                    values: vec![Value::Int(id), Value::Text("x".repeat(60 * 1024))],
                    version: 1,
                })
                .map_err(CoreError::Sql)?;
            }
            Ok(())
        }
        fn apply(
            &mut self,
            _: &TableDef,
            _: &BTreeMap<Vec<u8>, SqlRow>,
            _: &BTreeMap<Vec<u8>, SqlRow>,
        ) -> Result<(), CoreError> {
            panic!("read-only statements must never publish");
        }
    }

    #[test]
    fn staging_budget_is_shared_across_tables() {
        let mut image = Relational::default();
        for sql in [
            "CREATE TABLE a (id bigint PRIMARY KEY,v text)",
            "CREATE TABLE b (id bigint PRIMARY KEY,v text)",
        ] {
            let p = Prepared::parse(sql, 1, image.epoch).unwrap();
            image.execute(&p, &[], None).unwrap();
        }
        let p = Prepared::parse("SELECT a.id,b.id FROM a JOIN b ON true", 1, image.epoch).unwrap();
        let mut sink = WideSources { scans: Vec::new() };
        assert_eq!(
            image.execute_with_sink(&p, &[], None, None, &mut sink, u64::MAX, None),
            Err(CoreError::Sql(SqlError::Limit))
        );
        assert_eq!(sink.scans[0], ("a".into(), 600));
        assert_eq!(sink.scans[1].0, "b");
        assert!((1..600).contains(&sink.scans[1].1));
    }
}
