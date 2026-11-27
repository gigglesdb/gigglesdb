//! Append pages share the outer statement snapshot, locks and savepoint.
use super::*;

/// Shared-write admission matches the bounded plain-VALUES publication page.
pub(crate) const SHARED_WRITE_ROWS: usize = 200;

pub(super) fn eligible(state: &Relational, prepared: &Prepared) -> bool {
    if prepared.table().and_then(|name| state.tables.get(name)).is_none() {
        return false;
    }
    if let Statement::InsertMulti(i) = prepared.statement.get()
        && !i.append_only()
        && !i.can_witness(state) {
        return false;
    }
    matches!(prepared.statement.get(), Statement::InsertMulti(i) if i.needs_pages()
        && i.source.as_ref().is_none_or(|source| match source {
            sql_insert::Source::Plan(..) => true,
            sql_insert::Source::Query(query) => stored_source(state, query),
        }))
}

/// Row-local SELECTs of another table are evaluated one source page at a time.
fn paged_source(state: &Relational, insert: &sql_insert::Insert, query: &query::Query) -> bool {
    query.pageable_insert()
        && query.read_tables().first().is_some_and(|name| state.tables.contains_key(name))
        && !query.read_tables().contains(&insert.table)
}

fn stored_source(state: &Relational, query: &query::Query) -> bool {
    let tables = query.read_tables();
    !tables.is_empty() && tables.iter().all(|name| state.tables.contains_key(name)
        || matches!(query::views::projection(state, name), Ok(Some(_))))
}

pub(super) fn execute<S: RowSink>(
    state: &mut Relational,
    description: &StatementDescription<'_>,
    params: &[Value],
    clock: Option<&clock::ExecutionClock>,
    sink: &mut S,
    version_end: u64,
    deferred: Option<&mut DeferralState>,
) -> Result<SqlResult, CoreError> {
    execute_with_input(state, description, params, clock, sink, version_end, deferred, None)
}

#[expect(clippy::too_many_arguments, reason = "shared INSERT page and optional CTE input")]
pub(super) fn execute_with_input<S: RowSink>(
    state: &mut Relational,
    description: &StatementDescription<'_>,
    params: &[Value],
    clock: Option<&clock::ExecutionClock>,
    sink: &mut S,
    version_end: u64,
    mut deferred: Option<&mut DeferralState>,
    cte: Option<(&dyn scan::RowSource, &mut paged_dml::ActionWork)>,
) -> Result<SqlResult, CoreError> {
    let prepared = description.prepared();
    let Statement::InsertMulti(insert) = prepared.statement.get() else {
        unreachable!()
    };
    let mut template = prepared.clone();
    template.parameter_types = description.input().describe(state).map_err(CoreError::Sql)?
        .parameters.iter().cloned().map(Some).collect();
    *template.statement.get_mut() = Statement::InsertMulti(Box::new(insert.batch(0, 0)));
    let mut output = spill::RowBuffer::new(Vec::new())?;
    let mut result = SqlResult::default();
    let mut after = None;
    let mut start = 0;
    let mut insert_defaults = defaults::InsertDefaults::new(SHARED_WRITE_ROWS);
    let first_version = state.next_version;
    if cte.is_none() && insert.updates_conflicts() { sink.set_conflict_floor(Some(first_version)); }
    let table = state.tables[&insert.table].clone();
    let foreign_keys = state.ext.constraints.get(&table.id).into_iter().flatten()
        .any(|c| matches!(c.kind, ConstraintKind::ForeignKey { .. }));
    let (source, mut shared_actions) = cte.unzip();
    let mut actions = (shared_actions.is_none() && (foreign_keys && insert.updates_conflicts()
        || !insert.stable_referenced_keys(state, &table)))
        .then(|| paged_dml::ActionWork::new(deferred.as_deref().cloned().unwrap_or_default())).transpose()?;
    if foreign_keys && insert.updates_conflicts() && let Some(actions) = &mut actions {
        actions.upsert = Some(paged_dml::UpsertChecks::new(table.id, first_version..version_end));
    }
    let mut returned = scan::Retention::new(scan::WRITE_SOURCE_BYTES);
    let mut publish = |state: &mut Relational, sink: &mut S,
                       input: sql_insert::Insert| -> Result<(), CoreError> {
        let mut page = state.clone();
        // Insert checks see all input at statement end. Upsert publication
        // tracks which versions came from updates, including action rewrites.
        if foreign_keys && actions.is_none() && shared_actions.is_none() {
            std::sync::Arc::make_mut(&mut page.ext).constraints.get_mut(&table.id).expect("FK constraints")
                .retain(|c| !matches!(c.kind, ConstraintKind::ForeignKey { .. }));
        }
        let mut batch = template.clone();
        *batch.statement.get_mut() = Statement::InsertMulti(Box::new(input));
        batch.preserve_page_target();
        let part = page.execute_with_sink_actions(
            SinkStatement::Unbound(&batch),
            params,
            None,
            clock,
            sink,
            version_end,
            deferred.as_deref_mut(),
            shared_actions.as_deref_mut().or(actions.as_mut()),
            Some(&mut insert_defaults),
        )?;
        state.next_version = page.next_version;
        if let Some(actions) = &mut actions {
            actions.drain(state, sink, clock, version_end, true, true)?;
        }
        result.columns = part.columns;
        result.affected += part.affected;
        for row in &part.rows {
            if insert.source.is_some() {
                let bytes = searched::retained_row_bytes(&[], row).map_err(CoreError::Sql)?;
                returned.charge(bytes).map_err(CoreError::Sql)?;
            }
        }
        output.extend(part.rows)?;
        Ok(())
    };

    if let Some(plan) = insert.streaming_plan() {
        // The source has no table dependencies. Its lazy cursor and the
        // physical writer can advance together under one statement savepoint.
        let catalog = state.clone();
        let (template, parameters) = insert.stream_template(&catalog, params, &prepared.parameter_types).map_err(CoreError::Sql)?;
        let mut rows = Vec::with_capacity(SHARED_WRITE_ROWS);
        let mut write_error = None;
        let mut emitted = false;
        let streamed = plan.stream_write_source(
            &catalog, params, &parameters, clock, &mut || crate::cancellation::check().is_err(),
            &mut |row| {
                emitted = true;
                rows.push(row);
                if rows.len() == SHARED_WRITE_ROWS {
                    let input = template.literal_page(std::mem::replace(&mut rows, Vec::with_capacity(SHARED_WRITE_ROWS)));
                    if let Err(error) = publish(state, sink, input) {
                        write_error = Some(error);
                        return Err(SqlError::Aborted);
                    }
                }
                Ok(())
            },
        );
        if let Some(error) = write_error { return Err(error); }
        streamed.map_err(CoreError::Sql)?;
        // An empty page also binds the RETURNING header for a zero-row source.
        if !rows.is_empty() || insert.returning.is_some() && !emitted {
            publish(state, sink, template.literal_page(rows))?;
        }
    } else {
        let query = insert.source.as_ref().and_then(sql_insert::Source::query)
            .filter(|query| source.is_none() && paged_source(state, insert, query));
        let stored = if insert.source.is_some() && query.is_none() {
                let mut stored = None;
                let image = &*state;
                let mut store = |source: &dyn scan::RowSource| {
                    stored = Some(insert.source_store(image, params, &prepared.parameter_types, clock, source)?);
                    Ok(())
                };
                if let Some(source) = source { store(source).map_err(CoreError::Sql)?; }
                else { sink.with_row_source(&mut store)?; }
                stored
        } else {
            None
        };
        let mut stored_rows = match &stored {
            Some((_, result, store)) => Some(query::subquery_rows(result, store.as_ref()).map_err(CoreError::Sql)?),
            None => None,
        };
        let mut published = false;
        loop {
            let (input, done) = if let (Some((template, ..)), Some(rows)) = (&stored, stored_rows.as_mut()) {
                crate::cancellation::check()?;
                let page = rows.by_ref().take(SHARED_WRITE_ROWS)
                    .map(|row| row.map(std::borrow::Cow::into_owned))
                    .collect::<R<Vec<_>>>().map_err(CoreError::Sql)?;
                let done = page.is_empty();
                if done && (published || insert.returning.is_none()) {
                    break;
                }
                (template.literal_page(page), done)
            } else if let Some(query) = query {
                let table = &state.tables[&query.read_tables()[0]];
                let next = sink.read_page(table, after.take())?;
                let done = next.is_none();
                let (last, rows) = next.unwrap_or_default();
                after = Some(last);
                let mut source = state.clone();
                source.rows.insert(table.id, rows);
                (
                    insert
                        .source_page(&source, params, &prepared.parameter_types, clock)
                        .map_err(CoreError::Sql)?,
                    done,
                )
            } else {
                if start == insert.row_count() {
                    break;
                }
                let end = (start + SHARED_WRITE_ROWS).min(insert.row_count());
                let input = insert.batch(start, end);
                start = end;
                (input, false)
            };

            publish(state, sink, input)?;
            published = true;
            if done {
                break;
            }
        }
    }
    let upsert = match actions {
        Some(actions) => actions.finish(state, sink, deferred.as_deref_mut())?,
        None => None,
    };
    if source.is_none() && foreign_keys && upsert.as_ref().is_none_or(paged_dml::UpsertChecks::has_inserts) {
        validate_append(state, &table, first_version..state.next_version, upsert.as_ref(), sink, deferred, clock)?;
    }
    output.finish(&mut result)?;
    Ok(result)
}

pub(super) fn validate_append(state: &Relational, table: &TableDef, versions: std::ops::Range<u64>,
    upsert: Option<&paged_dml::UpsertChecks>, sink: &mut impl RowSink,
    deferred: Option<&mut DeferralState>, clock: Option<&clock::ExecutionClock>,
) -> Result<(), CoreError> {
        if deferred.as_ref().is_some_and(|state| state.replica) { return Ok(()); }
        let immediate_mode = deferred.is_none();
        let mut immediate = DeferralState::default();
        let pending = match deferred { Some(deferred) => deferred, None => &mut immediate };
        let mut pending_retention = pending.pending_retention().map_err(CoreError::Sql)?;
        let mut after = None;
        loop {
            // The upsert's RI checks must also see writes outside a task's
            // visible rows, as they did when they were checked by pending key.
            let next = if upsert.is_some() {
                sink.fk_read_page(table, after.take())?
            } else {
                sink.read_page(table, after.take())?
            };
            let Some((last, mut rows)) = next else { break; };
            after = Some(last);
            rows.retain(|_, row| versions.contains(&row.version)
                && upsert.is_none_or(|upsert| upsert.is_insert(row.version)));
            if rows.is_empty() { continue; }
            let mut page = state.clone();
            page.rows.insert(table.id, rows);
            let mut before = state.clone();
            let mut retention = scan::Retention::new(scan::retention_limit());
            // Only new rows need outgoing witnesses. Hydrated parents are identical
            // in before/page; recursively expanding them walks self-FK chains.
            // A paged upsert records deferred obligations without probing
            // their parents until SET CONSTRAINTS or COMMIT.
            let deferred_probes = (!immediate_mode && (upsert.is_some() || sink.defer_fk_parent_probes()))
                .then_some(&*pending);
            hydrate_with_probes(ParentRows::Changed(&constraints::FkPreimages::default()), &state.ext.constraints, &mut before, &mut page,
                sink, &mut retention, true, &BTreeSet::new(), None, deferred_probes)?;
            rebuild(&mut page, &sink.indexes())?;
            let mut checks = DeferralState { modes: pending.modes.clone(), ..DeferralState::default() };
            constraints::enforce(&mut page, &before, Vec::new(), &mut checks,
                &mut || crate::cancellation::check().is_err(), clock).map_err(CoreError::Sql)?;
            for (check, keys) in checks.pending {
                let pending = pending.pending.entry(check).or_default();
                for (key, reference) in keys {
                    constraints::record_pending(&mut pending_retention, pending, key, reference)
                        .map_err(CoreError::Sql)?;
                }
            }
        }
        if immediate_mode {
            paged_pending(state, &mut immediate, &[], &[], false, sink)?;
        }
    Ok(())
}
