//! Bounded multi-table statement staging over an external row store.
//!
//! The legacy publisher checks complete pre/postimages (including immediate PK
//! occupancy, witness ownership, work and byte limits). Reuse those checks in a
//! transient statement image, then publish the validated difference through the
//! caller's transaction. This adds no fields to the serialized Relational image.
use super::*;
use crate::CoreError;

pub(crate) type DdlRows = BTreeMap<Vec<u8>, SqlRow>;

impl Prepared {
    // Insert pages replace input rows after schema binding. Preserve
    // their target identity when the page is bound again: a bare storage key
    // denotes public here, even when search_path prefers another schema.
    fn preserve_page_target(&mut self) {
        let mut statement = self.statement.get().clone();
        let name = match &mut statement {
            Statement::InsertMulti(insert) => &mut insert.table,
            _ => unreachable!("paged mutation target"),
        };
        if !name.contains('\0') {
            *name = format!("public\0{name}");
        }
        self.unbound_statement = Some(std::sync::Arc::new(statement));
    }
}

pub(crate) fn retained_bytes(key: &[u8], row: &SqlRow) -> R<usize> {
    searched::retained_row_bytes(key, row)
}

pub(crate) fn reduction_range(table: &TableDef, p: &Prepared, params: &[Value]) -> Option<scan::KeyRange> {
    let predicates = p.v2_index_predicates(params).into_iter().filter_map(|(column, op, value)| {
        let c = table.columns.iter().find(|c| c.name == column)?;
        if !table.primary_key.iter().any(|i| table.columns[*i].id == c.id)
            || op != "=" && c.data_type.is_array()
            || check(&value, c).is_err()
            || query::comparison_requires_filter(&value, &c.data_type).ok()? { return None; }
        Some(Predicate { column, op, value: Expr::Literal(value) })
    }).collect::<Vec<_>>();
    bounds(table, &predicates, params).ok().map(scan::KeyRange::from_bounds)
}

pub(crate) fn row_key(table: &TableDef, row: &SqlRow) -> R<Vec<u8>> {
    pk(table, &row.values)
}

pub(crate) trait RowSink {
    fn mutation_keys(
        &mut self,
        _state: &Relational,
        _prepared: &Prepared,
        _params: &[Value],
        _clock: Option<&clock::ExecutionClock>,
    ) -> Result<BTreeSet<Vec<u8>>, CoreError> {
        Err(CoreError::Sql(SqlError::Unsupported))
    }
    /// FK witness lookup: `parent` probes the referenced row of a child row,
    /// otherwise the referencing rows of a changed parent key. Sinks whose
    /// snapshot can predate the FK table locks (REPEATABLE READ, SERIALIZABLE)
    /// read witnesses as PostgreSQL's RI checks do (GD-0075 R5-76).
    fn fk_lookup(
        &mut self,
        table: &TableDef,
        columns: &[u64],
        values: Option<&[Value]>,
        parent: bool,
        matches: &mut dyn FnMut(&SqlRow) -> R<bool>,
        visit: &mut dyn FnMut(SqlRow) -> R<()>,
    ) -> Result<(), CoreError> {
        self.lookup(table, columns, values, parent, &mut |row| {
            if matches(&row)? { visit(row)?; }
            Ok(())
        })
    }
    /// Whether this sink keeps exact FK parent lookups stable during a DDL
    /// statement. The first lookup must still perform all checks and locks.
    fn stable_fk_ddl_reads(&self) -> bool {
        false
    }

    /// `read_page` of referencing rows queued for a NO ACTION check, read like
    /// `fk_lookup`'s referencing rows.
    fn fk_read_page(
        &mut self,
        table: &TableDef,
        after: Option<Vec<u8>>,
    ) -> Result<Option<(Vec<u8>, DdlRows)>, CoreError> {
        self.read_page(table, after)
    }
    /// `child_page` for FK action discovery, read like `fk_lookup`'s
    /// referencing rows.
    fn fk_child_page(
        &mut self,
        table: &TableDef,
        columns: &[u64],
        values: &[Value],
        after: Option<Vec<u8>>,
    ) -> Result<Option<(Vec<u8>, DdlRows)>, CoreError> {
        self.child_page(table, columns, values, after)
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

    /// One physical FK page, with an opaque resume key and transaction own writes.
    fn child_page(
        &mut self,
        _table: &TableDef,
        _columns: &[u64],
        _values: &[Value],
        _after: Option<Vec<u8>>,
    ) -> Result<Option<(Vec<u8>, DdlRows)>, CoreError> {
        Err(CoreError::Sql(SqlError::Unsupported))
    }

    fn begin_conflict_pages(&mut self, _floor: u64) {}
    fn conflict_lookup(
        &mut self,
        table: &TableDef,
        columns: &[u64],
        values: &[Value],
        visit: &mut dyn FnMut(SqlRow) -> R<()>,
    ) -> Result<(), CoreError> {
        self.lookup(table, columns, Some(values), true, visit)
    }

    /// Byte indexes cannot probe ICU equality. Retain only matching owners.
    fn collated_lookup(
        &mut self,
        table: &TableDef,
        key: &collation_keys::Key,
        tuples: &BTreeSet<Vec<u8>>,
        _arbiter: bool,
        visit: &mut dyn FnMut(SqlRow) -> R<()>,
    ) -> Result<(), CoreError> {
        self.scan(table, &mut |row| {
            if key.tuple(&row.values)?.is_some_and(|tuple| tuples.contains(&tuple)) {
                visit(row)?;
            }
            Ok(())
        })
    }

    fn indexes(&self) -> Vec<secondary_index::Descriptor> {
        Vec::new()
    }
    fn streams_index_builds(&self) -> bool {
        false
    }
    /// Runs `read` over a paging source of the statement's transaction (v2).
    fn with_row_source(
        &mut self,
        _read: &mut dyn FnMut(&dyn scan::RowSource) -> R<()>,
    ) -> Result<(), CoreError> {
        Err(CoreError::Sql(SqlError::Unsupported))
    }
    fn read_page(
        &mut self,
        _table: &TableDef,
        _after: Option<Vec<u8>>,
    ) -> Result<Option<(Vec<u8>, DdlRows)>, CoreError> {
        Err(CoreError::Sql(SqlError::Unsupported))
    }
    /// Read bounded candidates without publishing; other sinks retain the cursor.
    fn mutation_candidates(&mut self, _table: &TableDef, _keys: &[Vec<Value>])
        -> Result<Option<DdlRows>, CoreError> { Ok(None) }

    /// Optional exact PK candidates; other sinks retain the existing page scan.
    fn mutation_pages(
        &mut self,
        table: &TableDef,
        _keys: Option<&[Vec<Value>]>,
        page: &mut dyn FnMut(DdlRows) -> Result<DdlRows, CoreError>,
    ) -> Result<(), CoreError> {
        self.ddl_pages(table, table, true, page)
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
    /// Physical occupancy is checked before end-of-statement FK actions.
    fn check_occupancy(
        &mut self,
        _table: &TableDef,
        _before: &DdlRows,
        _after: &DdlRows,
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
    pub(crate) fn streaming_reduction(&self, p: &Prepared) -> bool {
        matches!(p.statement.get(), Statement::Query(q) if q.streaming_reduction())
    }

    pub(crate) fn execute_reduction(
        &mut self,
        p: &Prepared,
        params: &[Value],
        expected: Option<u64>,
        clock: Option<&clock::ExecutionClock>,
        source: &dyn scan::RowSource,
    ) -> R<SqlResult> {
        self.execute_statement_inner(p, params, expected,
            || crate::cancellation::check().is_err(), clock, Some(source), &mut Vec::new())
    }

    pub(crate) fn mutation_keys(
        &self, prepared: &Prepared, params: &[Value],
        clock: Option<&clock::ExecutionClock>, source: &dyn scan::RowSource,
    ) -> R<BTreeSet<Vec<u8>>> {
        let parameters = searched::describe(prepared, self)?.parameters.into_iter().map(Some).collect();
        searched::target(prepared).ok_or(SqlError::Unsupported)?
            .bind_seeded(self, parameters)?.with_clock(clock).matching_keys(self, source, params)
    }
    pub(crate) fn copy_input_page(
        &mut self,
        prepared: &Prepared,
        sink: &mut impl RowSink,
        range: std::ops::Range<u64>,
        finish: bool,
        deferred: &mut DeferralState,
        clock: Option<&clock::ExecutionClock>,
    ) -> Result<(), CoreError> {
        let table = self
            .tables
            .get(
                prepared
                    .table()
                    .ok_or(CoreError::Sql(SqlError::Unsupported))?,
            )
            .ok_or(CoreError::Sql(SqlError::UnknownTable))?
            .clone();
        if finish {
            if !self
                .ext
                .constraints
                .get(&table.id)
                .into_iter()
                .flatten()
                .any(|c| matches!(c.kind, ConstraintKind::ForeignKey { .. }))
            {
                return Ok(());
            }
            paged_insert::validate_append(
                self,
                &table,
                range.start,
                range.end,
                sink,
                Some(deferred),
                clock,
            )
        } else {
            self.suppress_foreign_keys();
            Ok(())
        }
    }

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
        self.execute_with_sink_actions(prepared, params, expected, clock, sink, version_end, deferred, None)
    }

    #[expect(clippy::too_many_arguments, reason = "statement inputs plus paged referential actions")]
    fn execute_with_sink_actions(
        &mut self, prepared: &Prepared, params: &[Value], expected: Option<u64>,
        clock: Option<&clock::ExecutionClock>, sink: &mut impl RowSink, version_end: u64,
        deferred: Option<&mut DeferralState>, actions: Option<&mut paged_dml::ActionWork>,
    ) -> Result<SqlResult, CoreError> {
        let returning_scope = sink.streams_index_builds().then(spill::returning_unbounded);
        let bound = prepared.bind_schemas(self).map_err(CoreError::Sql)?;
        let prepared = &bound;
        prepared.validate_schemas(self).map_err(CoreError::Sql)?;
        // Native witnesses are request prechecks: stale/missing wins over any
        // row's constraint error. Read the final post-lock statement snapshot,
        // before any page can publish, without retaining row images.
        if let Some(targets) = &prepared.native_delete {
            let table = &self.tables[prepared.table().unwrap()];
            sink.with_row_source(&mut |source| {
                for (key, version) in targets {
                    let key = encode_key(key)?;
                    if source.get(table, &key)?.is_none_or(|row| row.row.version != *version) {
                        return Err(SqlError::Version);
                    }
                }
                Ok(())
            })?;
        }

        if let Statement::Truncate { restart, .. } = prepared.statement.get() {
            if !params.is_empty() || expected.is_some() {
                return Err(CoreError::Sql(SqlError::Parameters));
            }
            let names = prepared.truncate_tables(self).map_err(CoreError::Sql)?;
            for name in &names {
                let table = self.tables[name].clone();
                sink.ddl_pages(&table, &table, true, &mut |_| Ok(BTreeMap::new()))?;
            }
            if *restart {
                prepared
                    .truncate_restart(self, &names)
                    .map_err(CoreError::Sql)?;
            }
            return Ok(SqlResult::default());
        }
        if sink.streams_index_builds()
            && let Some((names, mode)) = prepared.set_constraints()
        {
            let deferred = deferred.ok_or(CoreError::Sql(SqlError::NoActiveTransaction))?;
            if !params.is_empty() || expected.is_some() {
                return Err(CoreError::Sql(SqlError::Parameters));
            }
            return paged_pending(self, deferred, names, mode, sink);
        }
        if expected.is_none()
            && sink.streams_index_builds()
            && matches!(prepared.statement.get(), Statement::InsertMulti(_))
            && paged_insert::eligible(self, prepared)
        {
            return paged_insert::execute(
                self,
                prepared,
                params,
                clock,
                sink,
                version_end,
                deferred,
            );
        }
        if expected.is_none() && sink.streams_index_builds()
            && let Statement::Plan(plan)=prepared.statement.get()
            && let Some(merge)=plan.paged_merge(self)
        {
            return merge.paged(self,prepared,params,clock,sink,version_end,deferred);
        }
        if expected.is_none() && sink.streams_index_builds()
            && let Statement::Plan(plan)=prepared.statement.get()
            && let Some(mutation)=plan.paged_mutation()
        {
            return mutation.paged(self,prepared,params,clock,sink,version_end,deferred);
        }
        if expected.is_none() && sink.streams_index_builds() && paged_dml::cascade_eligible(self, prepared) {
            return paged_dml::cascade(self, prepared, params, clock, sink, deferred, version_end);
        }
        let append = sink.streams_index_builds()
            && match prepared.statement.get() {
                Statement::Insert { .. } | Statement::InsertExpressions(_) => true,
                Statement::InsertMulti(i) => i.append_only() && i.source.is_none(),
                _ => false,
            };
        if expected.is_none() && sink.streams_index_builds() && paged_dml::eligible(self, prepared)
        {
            return paged_dml::execute(self, prepared, params, clock, sink, version_end, deferred);
        }
        let streamed_ddl = match prepared.statement.get() {
            Statement::DropIndex(_) | Statement::DropTable { .. } => true,
            Statement::AlterTable(alter) => alter.actions.iter().all(|action| {
                matches!(
                    action,
                    alter_table::Action::Type(..)
                        | alter_table::Action::Add(..)
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
            if let Statement::AlterTable(alter) = prepared.statement.get() {
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
        let index_build = match prepared.statement.get() {
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
        let collated = self.tables.values().any(|t| self.has_nondeterministic_keys(t))
            || matches!(prepared.statement.get(), Statement::CreateIndex(i) if i.collations.iter().any(Option::is_some));
        if index_build && sink.streams_index_builds() && !collated {
            let old: BTreeSet<_> = sink.indexes().iter().map(|index| index.id).collect();
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
                if !old.contains(&index.id) {
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
        let mut normalized = prepared.clone();
        let conflict_rows = if sink.streams_index_builds() {
            if let Statement::InsertMulti(insert) = normalized.statement.get_mut() {
                insert
                    .witness_rows(self, params, &prepared.parameter_types, clock)
                    .map_err(CoreError::Sql)?
            } else {
                None
            }
        } else {
            None
        };
        if conflict_rows.is_some() {
            normalized.preserve_page_target();
        }
        let prepared = &normalized;
        let arbiter = match prepared.statement.get() {
            Statement::InsertMulti(insert) if conflict_rows.is_some() =>
                insert.conflict_arbiter_columns(self, &sink.indexes()),
            _ => None,
        };
        let mut names = if let Some(names)=prepared.collation_drop_tables(self) {names} else if let Statement::TypeDdl(user_types::Ddl::Drop {
            names,
            cascade: true,
            ..
        }) = prepared.statement.get()
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
        } else if matches!(prepared.v2_class(), V2Class::Ddl) && !matches!(prepared.statement.get(),Statement::Create {as_query:Some(_),..}) {
            Vec::new()
        } else {
            prepared.read_tables()
        };
        if let Some(table) = &table
            && !names.contains(&self.table_key(table))
        {
            names.push(self.table_key(table));
        }
        for (_, table_id) in deferred
            .as_ref()
            .filter(|_| !sink.streams_index_builds())
            .into_iter()
            .flat_map(|d| d.pending.keys())
        {
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
            let is_target = prepared.table() == Some(self.table_key(table).as_str())
                || prepared.write_tables().iter().any(|(name, _)| *name == self.table_key(table));
            // The physical publisher checks PK/UNIQUE occupancy against the
            // transaction, including earlier pages. Inserts need no preimage.
            if append && is_target {
                continue;
            }
            let mut keys = Vec::new();
            if is_target && conflict_rows.is_some() {
                let columns = table.primary_key.iter().map(|i| table.columns[*i].id).collect::<Vec<_>>();
                let primary = collation_keys::Key::new(self, table, &columns, None).map_err(CoreError::Sql)?;
                keys.push((columns, primary, true));
                let actual = match prepared.statement.get() {
                    Statement::InsertMulti(insert) => insert.arbiter_predicate(self, table).map_err(CoreError::Sql)?,
                    _ => None,
                };
                for descriptor in sink.indexes().into_iter().filter(|d| d.table == table.id && d.unique) {
                    let inferred = match (&descriptor.predicate, &actual) {
                        (Some(required), Some(actual)) => actual.implies(required, self, table).map_err(CoreError::Sql)?,
                        (Some(_), None) => false,
                        (None, _) => true,
                    };
                    let key = collation_keys::Key::for_index(self, table, &descriptor).map_err(CoreError::Sql)?;
                    keys.push((descriptor.columns, key, inferred));
                }
            }
            let rows = self
                .rows
                .get_mut(&table.id)
                .ok_or(CoreError::Sql(SqlError::UnknownTable))?;
            let point = (is_target && !collated)
                .then(|| point_target(prepared, table, params, &sink.indexes()))
                .flatten();
            let mut visit = |row: SqlRow| {
                if crate::cancellation::check().is_err() {
                    return Err(SqlError::Aborted);
                }
                let key = pk(table, &row.values)?;
                if conflict_rows.is_some() && rows.contains_key(&key) {
                    return Ok(());
                }
                // Only destination rows constrain its version allocator. A
                // concurrent source writer may hold a newer reserved range.
                if is_target {
                    self.next_version = self
                        .next_version
                        .max(row.version.checked_add(1).ok_or(SqlError::Overflow)?);
                }
                retention.charge(searched::retained_row_bytes(&key, &row)?)?;
                if rows.insert(key, row).is_some() {
                    return Err(SqlError::Duplicate);
                }
                Ok(())
            };
            if is_target && let Some(proposals) = &conflict_rows {
                for (columns, key, inferred) in &keys {
                    if !key.nondeterministic() && !key.partial() { continue; }
                    let tuples = proposals.iter().map(|row| key.tuple(row))
                        .collect::<R<Vec<_>>>().map_err(CoreError::Sql)?
                        .into_iter().flatten().collect();
                    let arbiter = *inferred && arbiter.as_ref().is_some_and(|ids| ids.len() == columns.len()
                        && columns.iter().all(|id| ids.contains(id)));
                    sink.collated_lookup(table, key, &tuples, arbiter, &mut visit)?;
                }
                for proposal in proposals {
                    for (columns, key, _) in &keys {
                        if key.nondeterministic() || key.partial() { continue; }
                        let values = columns
                            .iter()
                            .map(|id| {
                                proposal[table
                                    .columns
                                    .iter()
                                    .position(|c| c.id == *id)
                                    .expect("index column")]
                                .clone()
                            })
                            .collect::<Vec<_>>();
                        if !values.contains(&Value::Null) {
                            // Only the arbiter can affect the same row twice (21000).
                            // Other unique keys retain ordinary uniqueness checks (23505).
                            if arbiter.as_ref().is_some_and(|ids| ids.len() == columns.len()
                                && columns.iter().all(|id| ids.contains(id))) {
                                sink.conflict_lookup(table, columns, &values, &mut visit)?;
                            } else {
                                sink.lookup(table, columns, Some(&values), true, &mut visit)?;
                            }
                        }
                    }
                }
            } else if let Some((columns, values)) = point {
                sink.lookup(table, &columns, Some(&values), true, &mut visit)?;
            } else {
                sink.scan(table, &mut visit)?;
            }
        }
        if matches!(prepared.v2_class(), V2Class::Read) && sink.streams_index_builds() {
            // SqlRows already selected from the physical snapshot (including
            // transaction-local writes). Legacy query execution consumes the
            // hydrated row maps; it has no need for a transient index sidecar.
            self.secondary = secondary_index::Sidecar::default();
            return self
                .execute_statement_inner(
                    prepared,
                    params,
                    expected,
                    || crate::cancellation::check().is_err(),
                    clock,
                    None,
                    &mut Vec::new(),
                )
                .map_err(CoreError::Sql);
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
        if let Statement::AlterTable(alter) = prepared.statement.get() {
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
        let write_targets = constraint_rows::write_targets(self, prepared, &edges);
        // This ALTER adds metadata without rewriting either row image. A
        // successful exact parent lookup can serve later passes over the same
        // FK edge while the sink holds the DDL and parent-key locks.
        let stable_fk = matches!(prepared.statement.get(), Statement::AlterTable(alter)
            if matches!(alter.actions.as_slice(), [alter_table::Action::Constraint(
                constraints::Definition::ForeignKey { .. }
            )])) && sink.stable_fk_ddl_reads();
        let mut parent_probes = stable_fk.then(constraint_rows::ParentProbes::default);
        if writing && actions.is_none() {
            loop {
                let seeds = (!stable_fk).then(|| self.clone());
                if hydrate_with_probes(
                    seeds.as_ref(),
                    &edges,
                    &mut before,
                    self,
                    sink,
                    &mut retention,
                    append,
                    &write_targets,
                    parent_probes.as_mut(),
                )? == 0 {
                    break;
                }
            }
        }
        rebuild(&mut before, &sink.indexes())?;
        rebuild(self, &sink.indexes())?;
        let mut pending = deferred.as_deref().cloned().unwrap_or_default();
        let mut result = if let Some((names, mode)) = prepared.set_constraints() {
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
            let result = if prepared.row_lock_command().is_some() {
                let mut result = None;
                let snapshot = self.clone();
                sink.with_row_source(&mut |source| {
                    let source = scan::LockedSnapshot { snapshot: scan::LegacyRows(&snapshot), locks: source };
                    result = Some(self.execute_statement_inner(prepared, params, expected,
                        || crate::cancellation::check().is_err(), clock, Some(&source), &mut events)?);
                    Ok(())
                })?;
                result.ok_or(CoreError::Sql(SqlError::InternalError))?
            } else {
                self.execute_statement_inner(prepared, params, expected,
                    || crate::cancellation::check().is_err(), clock, None, &mut events)
                    .map_err(CoreError::Sql)?
            };
            if let Some(actions) = actions {
                let table = table.as_ref().expect("conflict page target");
                actions.publish(self, sink, table, before.rows.remove(&table.id).unwrap(),
                    self.rows[&table.id].clone(), events, None, &sink.indexes(), version_end)?;
                return Ok(result);
            }
            if writing {
                if !append
                    && let Some(table) = &table
                    && matches!(prepared.v2_class(), V2Class::Dml)
                {
                    sink.check_occupancy(table, &before.rows[&table.id], &self.rows[&table.id])?;
                }
                if table.is_none() && matches!(prepared.v2_class(), V2Class::Dml) {
                    for (name, _) in prepared.write_tables() {
                        let table = self.tables.get(&name).ok_or(CoreError::Sql(SqlError::UnknownTable))?;
                        sink.check_occupancy(table, &before.rows[&table.id], &self.rows[&table.id])?;
                    }
                }
                if append {
                    let table = table.as_ref().expect("append target");
                    let old = &before.rows[&table.id];
                    let new = &self.rows[&table.id];
                    if self.has_nondeterministic_keys(table) {
                        sink.check_occupancy(table, old, new)?;
                    }
                    // Preserve immediate PK/UNIQUE error precedence before
                    // end-of-statement constraints. The savepoint owns rollback.
                    sink.apply_indexes(
                        table,
                        Some(table),
                        old,
                        new,
                        self.secondary_descriptors_engine(),
                    )?;
                    sink.apply(table, old, new)?;
                }
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
                                let seeds = (!stable_fk).then(|| state.clone());
                                let ext = state.ext.clone();
                                if hydrate_with_probes(
                                    seeds.as_ref(),
                                    &ext.constraints,
                                    old,
                                    state,
                                    sink,
                                    &mut retention,
                                    append,
                                    &write_targets,
                                    parent_probes.as_mut(),
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
            let mut after_tables = BTreeMap::new();
            for table in self.tables.values() {
                after_tables.entry(table.id).or_insert(table);
            }
            let same_indexes =
                before.secondary_descriptors_engine() == self.secondary_descriptors_engine();
            for table in before.tables.values() {
                if append && prepared.table() == Some(before.table_key(table).as_str()) {
                    continue;
                }
                let old = &before.rows[&table.id];
                let empty = BTreeMap::new();
                let after = self.rows.get(&table.id).unwrap_or(&empty);
                let after_table = after_tables.get(&table.id).copied();
                if old == after && after_table == Some(table) && same_indexes {
                    continue;
                }
                if collated && after_table == Some(table) { sink.check_occupancy(table, old, after)?; }
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
        if writing {
            debug_assert!(before.rows.keys().copied().eq(
                before.tables.values().map(|table| table.id).collect::<BTreeSet<_>>()
            ));
            for table in self.tables.values() {
                let rows = &self.rows[&table.id];
                if !rows.is_empty() && !before.rows.contains_key(&table.id) {
                    sink.apply(table, &BTreeMap::new(), rows)?;
                }
            }
        }
        if let Some(deferred) = deferred {
            *deferred = pending;
        }
        if prepared.returning().is_some()
            && returning_scope.as_ref().is_some_and(spill::ReturningScope::outermost)
        {
            spill::RowBuffer::new(std::mem::take(&mut result.rows))?.finish(&mut result)?;
        }
        Ok(result)
    }
}

// Only the parser's complete conjunction is eligible. Every primary-key column
// needs an exact, type-checked equality; the executor still applies all residuals.
fn point_target(
    prepared: &Prepared,
    table: &TableDef,
    params: &[Value],
    _indexes: &[secondary_index::Descriptor],
) -> Option<(Vec<u64>, Vec<Value>)> {
    let predicates = match prepared.statement.get() {
        Statement::Update { predicates, .. }
        | Statement::UpdateExpressions { predicates, .. }
        | Statement::Delete { predicates, .. } => predicates,
        _ => return None,
    };
    if table.hidden_rowid().is_some() || table.primary_key.is_empty() {
        return None;
    }
    let mut columns = Vec::new();
    let mut values = Vec::new();
    for offset in &table.primary_key {
        let column = &table.columns[*offset];
        // Typed textual/numeric keys can need SQL canonicalization. Leave
        // their complete-image path in place until probes bind typed values.
        if !matches!(
            column.data_type,
            DataType::Boolean
                | DataType::SmallInt
                | DataType::Integer
                | DataType::BigInt
                | DataType::Text
                | DataType::Varchar
                | DataType::Bytes
        ) {
            return None;
        }
        let value = predicates.iter().find_map(|p| {
            (p.column == column.name && p.op == "=")
                .then(|| p.value.value(params).ok())
                .flatten()
        })?;
        if value == Value::Null || check(&value, column).is_err() {
            return None;
        }
        columns.push(column.id);
        values.push(value);
    }
    Some((columns, values))
}

#[path = "row_sink_constraints.rs"]
mod constraint_rows;
#[path = "row_sink_dml.rs"]
mod paged_dml;
#[path = "row_sink_insert.rs"]
mod paged_insert;
use constraint_rows::{hydrate, hydrate_with_probes, paged_pending, rebuild};

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
    fn point_staging_requires_complete_conjunction_and_exact_key_types() {
        let mut image = Relational::default();
        let ddl = Prepared::parse(
            "CREATE TABLE t(a bigint,b bigint,v bigint,PRIMARY KEY(a,b))",
            1,
            image.epoch,
        )
        .unwrap();
        image.execute(&ddl, &[], None).unwrap();
        let table = image.tables.values().next().unwrap();
        let mut typed = table.clone();
        typed.columns[0].data_type = DataType::Uuid;
        let uuid = Prepared::parse(
            "DELETE FROM t WHERE a='550E8400-E29B-41D4-A716-446655440000' AND b=2",
            1,
            image.epoch,
        )
        .unwrap();
        assert!(point_target(&uuid, &typed, &[], &[]).is_none());
        for (sql, eligible) in [
            ("DELETE FROM t WHERE a=1 AND b=2", true),
            ("UPDATE t SET v=3 WHERE a=1 AND b=2 AND v=4", true),
            ("DELETE FROM t WHERE a=1", false),
            ("DELETE FROM t WHERE a=1 OR b=2", false),
            ("UPDATE t SET a=3 WHERE a=1 AND b=2", true),
        ] {
            let p = Prepared::parse(sql, 1, image.epoch).unwrap();
            assert_eq!(
                point_target(&p, table, &[], &[]).is_some(),
                eligible,
                "{sql}"
            );
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
