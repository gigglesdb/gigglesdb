//! Append pages share the outer statement snapshot, locks and savepoint.
use super::*;

/// Shared-write admission matches the bounded plain-VALUES publication page.
pub(crate) const SHARED_WRITE_ROWS: usize = 200;

pub(super) fn eligible(state: &Relational, prepared: &Prepared) -> bool {
    if prepared.table().and_then(|name| state.tables.get(name)).is_none() {
        return false;
    }
    if let Statement::InsertMulti(i) = &prepared.statement
        && !i.append_only()
        && !i.can_witness(state) {
        return false;
    }
    matches!(&prepared.statement, Statement::InsertMulti(i) if i.needs_pages()
        && i.source.as_ref().is_none_or(|q| paged_source(state, i, q) || stored_source(state, q)))
}

/// Row-local SELECTs of another table are evaluated one source page at a time.
fn paged_source(state: &Relational, insert: &sql_insert::Insert, query: &query::Query) -> bool {
    query.pageable_insert()
        && query.read_tables().first().is_some_and(|name| state.tables.contains_key(name))
        && !query.read_tables().contains(&insert.table)
}

/// Other sources (ORDER BY, DISTINCT, joins, grouping, the target itself)
/// are read completely into a spill store before the first page is inserted,
/// so the insert never sees its own rows (PostgreSQL's statement snapshot).
fn stored_source(state: &Relational, query: &query::Query) -> bool {
    let tables = query.read_tables();
    !tables.is_empty() && tables.iter().all(|name| state.tables.contains_key(name)
        || matches!(query::views::projection(state, name), Ok(Some(_))))
}

pub(super) fn execute(
    state: &mut Relational,
    prepared: &Prepared,
    params: &[Value],
    clock: Option<&clock::ExecutionClock>,
    sink: &mut impl RowSink,
    version_end: u64,
    mut deferred: Option<&mut DeferralState>,
) -> Result<SqlResult, CoreError> {
    let Statement::InsertMulti(insert) = &prepared.statement else {
        unreachable!()
    };
    let mut template = prepared.clone();
    template.parameter_types = prepared.describe(state).map_err(CoreError::Sql)?.parameters.into_iter().map(Some).collect();
    template.statement = Statement::InsertMulti(Box::new(insert.batch(0, 0)));
    let mut output = spill::RowBuffer::new(Vec::new())?;
    let mut result = SqlResult::default();
    let mut after = None;
    let mut start = 0;
    let first_version = state.next_version;
    if insert.updates_conflicts() { sink.begin_conflict_pages(first_version); }
    let table = state.tables[&insert.table].clone();
    let mut actions = (!insert.stable_referenced_keys(state, &table))
        .then(|| paged_dml::ActionWork::new(deferred.as_deref().cloned().unwrap_or_default()));
    let foreign_keys = state.ext.constraints.get(&table.id).into_iter().flatten()
        .any(|c| matches!(c.kind, ConstraintKind::ForeignKey { .. }));
    let stored = match &insert.source {
        Some(query) if !paged_source(state, insert, query) => {
            let mut stored = None;
            let image = &*state;
            sink.with_row_source(&mut |source| {
                stored = Some(insert.source_store(image, params, &prepared.parameter_types, clock, source)?);
                Ok(())
            })?;
            stored
        }
        _ => None,
    };
    let mut stored_rows = match &stored {
        Some((_, result, store)) => Some(query::subquery_rows(result, store.as_ref()).map_err(CoreError::Sql)?),
        None => None,
    };
    loop {
        let (input, done) = if let (Some((template, ..)), Some(rows)) = (&stored, stored_rows.as_mut()) {
            crate::cancellation::check()?;
            let page = rows.by_ref().take(SHARED_WRITE_ROWS)
                .map(|row| row.map(std::borrow::Cow::into_owned))
                .collect::<R<Vec<_>>>().map_err(CoreError::Sql)?;
            if page.is_empty() {
                break;
            }
            (template.literal_page(page), false)
        } else if let Some(query) = &insert.source {
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

        let mut page = state.clone();
        // Outgoing FK checks see all input at statement end. Referenced-key
        // updates instead publish through the bounded referential-action queue.
        if foreign_keys && actions.is_none() {
            page.ext.constraints.get_mut(&table.id).expect("FK constraints")
                .retain(|c| !matches!(c.kind, ConstraintKind::ForeignKey { .. }));
        }
        let mut batch = template.clone();
        batch.statement = Statement::InsertMulti(Box::new(input));
        batch.preserve_page_target();
        let part = page.execute_with_sink_actions(
            &batch,
            params,
            None,
            clock,
            sink,
            version_end,
            deferred.as_deref_mut(),
            actions.as_mut(),
        )?;
        state.next_version = page.next_version;
        if let Some(actions) = &mut actions {
            actions.drain(state, sink, clock, &sink.indexes(), version_end, true)?;
        }
        result.columns = part.columns;
        result.affected += part.affected;
        output.extend(part.rows)?;
        if done {
            break;
        }
    }
    if let Some(actions) = actions {
        actions.finish(state, sink, deferred.as_deref_mut())?;
    }
    if foreign_keys {
        validate_append(state, &table, first_version, state.next_version, sink, deferred, clock)?;
    }
    output.finish(&mut result)?;
    Ok(result)
}

pub(crate) fn validate_append(state: &Relational, table: &TableDef, first_version: u64,
    version_end: u64, sink: &mut impl RowSink, deferred: Option<&mut DeferralState>, clock: Option<&clock::ExecutionClock>,
) -> Result<(), CoreError> {
        if deferred.as_ref().is_some_and(|state| state.replica) { return Ok(()); }
        let immediate_mode = deferred.is_none();
        let mut immediate = DeferralState::default();
        let pending = match deferred { Some(deferred) => deferred, None => &mut immediate };
        let mut after = None;
        while let Some((last, mut rows)) = sink.read_page(table, after.take())? {
            after = Some(last);
            rows.retain(|_, row| row.version >= first_version && row.version < version_end);
            if rows.is_empty() { continue; }
            let inserted = rows.keys().cloned().collect::<Vec<_>>();
            let mut page = state.clone();
            page.rows.insert(table.id, rows);
            let mut before = state.clone();
            let mut retention = scan::Retention::new(scan::retention_limit());
            // Only new rows need outgoing witnesses. Hydrated parents are identical
            // in before/page; recursively expanding them walks self-FK chains.
            hydrate(&page.clone(), &state.ext.constraints, &mut before, &mut page,
                sink, &mut retention, true)?;
            // A new self-FK row can also witness another new row's parent.
            // Hydration must not make that seed look unchanged and skip its FK.
            for key in inserted {
                before.rows.get_mut(&table.id).expect("append table").remove(&key);
            }
            rebuild(&mut page, &sink.indexes())?;
            constraints::enforce(&mut page, &before, Vec::new(), pending,
                &mut || crate::cancellation::check().is_err(), clock).map_err(CoreError::Sql)?;
        }
        if immediate_mode {
            paged_pending(state, &mut immediate, &[], false, sink)?;
        }
    Ok(())
}
