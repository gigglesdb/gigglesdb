//! Bounded statement images; all pages share the caller's transaction/savepoint.
use super::*;

pub(super) fn eligible(state: &Relational, prepared: &Prepared) -> bool {
    let Some(table) = prepared.table().and_then(|name| state.tables.get(name)) else {
        return false;
    };
    let sets = match &prepared.statement {
        Statement::Update { sets, .. } => sets.iter().map(|(name, _)| name).collect::<Vec<_>>(),
        Statement::UpdateExpressions { sets, .. } => sets.iter().map(|(name, _)| name).collect(),
        Statement::Delete { .. } => vec![],
        _ => return false,
    };
    // A stable primary key makes the forward cursor Halloween-safe. Subqueries
    // need their complete statement snapshot, not a page-local relation image.
    if table
        .primary_key
        .iter()
        .any(|i| sets.contains(&&table.columns[*i].name))
        || searched::target(prepared).is_some_and(query::Query::has_in_subquery)
    {
        return false;
    }
    // Keep referential actions on the existing complete-image executor until
    // their work queue and lookup hydration can also be paged atomically.
    !state.ext.constraints.iter().any(|(owner, constraints)| {
        constraints.iter().any(|constraint| {
            matches!(constraint.kind, ConstraintKind::ForeignKey { ref_table, .. }
                if *owner == table.id || ref_table == table.id)
        })
    })
}

pub(super) fn execute(
    state: &mut Relational,
    prepared: &Prepared,
    params: &[Value],
    clock: Option<&clock::ExecutionClock>,
    sink: &mut impl RowSink,
    version_end: u64,
) -> Result<SqlResult, CoreError> {
    let table = state.tables[prepared.table().expect("mutation target")].clone();
    // Analyze even an empty table before opening the cursor. No writes escape
    // the outer statement savepoint, including failures on the final page.
    let mut result = state
        .execute_statement_inner(
            prepared,
            params,
            None,
            || crate::cancellation::check().is_err(),
            clock,
            None,
            &mut Vec::new(),
        )
        .map_err(CoreError::Sql)?;
    let descriptors = sink.indexes();
    let mut output = scan::Retention::new(MAX_RESULT_BYTES);
    sink.ddl_pages(&table, &table, true, &mut |rows| {
        let mut page = state.clone();
        page.rows.insert(table.id, rows);
        page.secondary = page
            .secondary_build_complete_engine(&descriptors, || crate::cancellation::check().is_err())
            .map_err(CoreError::Sql)?;
        let batch = page
            .execute_statement_inner(
                prepared,
                params,
                None,
                || crate::cancellation::check().is_err(),
                clock,
                None,
                &mut Vec::new(),
            )
            .map_err(CoreError::Sql)?;
        if page.next_version > version_end {
            return Err(CoreError::Sql(SqlError::Limit));
        }
        state.next_version = page.next_version;
        for row in &batch.rows {
            output
                .charge(searched::retained_row_bytes(&[], row).map_err(CoreError::Sql)?)
                .map_err(CoreError::Sql)?;
        }
        result.affected += batch.affected;
        result.rows.extend(batch.rows);
        Ok(page.rows.remove(&table.id).unwrap_or_default())
    })?;
    Ok(result)
}

// DELETE-only slice: updated referential actions retain the complete-image path.
pub(super) fn cascade_eligible(state: &Relational, prepared: &Prepared) -> bool {
    if !matches!(prepared.statement, Statement::Delete { .. })
        || searched::target(prepared).is_some_and(query::Query::has_in_subquery) {
        return false;
    }
    let Some(target) = prepared.table().and_then(|name| state.tables.get(name)) else { return false; };
    let mut connected = BTreeSet::from([target.id]);
    let mut cascade = false;
    loop {
        let count = connected.len();
        for (child_id, cs) in &state.ext.constraints {
            for c in cs {
                if let ConstraintKind::ForeignKey { ref_table, columns, ref_columns, on_delete, .. } = &c.kind
                    && connected.contains(ref_table) {
                    if !matches!(on_delete, catalog_ext::Action::Cascade | catalog_ext::Action::Restrict) {
                        return false;
                    }
                    let child = state.tables.values().find(|t| t.id == *child_id).unwrap();
                    let parent = state.tables.values().find(|t| t.id == *ref_table).unwrap();
                    if columns.iter().zip(ref_columns).any(|(a, b)| {
                        let ty = &child.columns.iter().find(|c| c.id == *a).unwrap().data_type;
                        ty != &parent.columns.iter().find(|c| c.id == *b).unwrap().data_type
                            || !matches!(ty, DataType::Boolean | DataType::SmallInt | DataType::Integer
                                | DataType::BigInt | DataType::Text | DataType::Varchar | DataType::Bytes)
                    }) {
                        return false;
                    }
                    cascade |= *on_delete == catalog_ext::Action::Cascade;
                    connected.insert(*child_id);
                }
            }
        }
        if count == connected.len() { return cascade; }
    }
}

pub(super) fn delete_cascade(
    state: &mut Relational, prepared: &Prepared, params: &[Value],
    clock: Option<&clock::ExecutionClock>, sink: &mut impl RowSink,
    deferred: Option<&mut DeferralState>,
) -> Result<SqlResult, CoreError> {
    sink.check_fk_snapshot()?;
    let pending = deferred.as_deref().cloned().unwrap_or_default();
    let table = state.tables[prepared.table().unwrap()].clone();
    let descriptors = sink.indexes();
    let mut result = state.execute_statement_inner(prepared, params, None,
        || crate::cancellation::check().is_err(), clock, None, &mut Vec::new()).map_err(CoreError::Sql)?;
    let mut work = DeleteWork { queue: std::collections::VecDeque::new(),
        retention: scan::Retention::new(scan::retention_limit()), visited: BTreeSet::new(), pending };
    let mut output = scan::Retention::new(MAX_RESULT_BYTES);
    let mut after = None;
    while let Some((last, rows)) = sink.read_page(&table, after)? {
        after = Some(last);
        let mut page = state.clone();
        page.rows.insert(table.id, rows.clone());
        rebuild(&mut page, &descriptors)?;
        let batch = page.execute_statement_inner(prepared, params, None,
            || crate::cancellation::check().is_err(), clock, None, &mut Vec::new()).map_err(CoreError::Sql)?;
        for row in &batch.rows {
            output.charge(searched::retained_row_bytes(&[], row).map_err(CoreError::Sql)?).map_err(CoreError::Sql)?;
        }
        result.affected += batch.affected;
        result.rows.extend(batch.rows);
        let deleted = rows.into_iter().filter(|(key, _)| !page.rows[&table.id].contains_key(key)).collect();
        work.publish(sink, &table, deleted, &descriptors)?;
    }
    while let Some((parent_id, old)) = work.queue.pop_front() {
        crate::cancellation::check()?;
        let parent = state.tables.values().find(|t| t.id == parent_id).unwrap();
        for (child_id, cs) in &state.ext.constraints {
            for c in cs {
                let ConstraintKind::ForeignKey { ref_table, columns, ref_columns, on_delete, .. } = &c.kind else { continue; };
                if *ref_table != parent_id { continue; }
                let values = ref_columns.iter().map(|id| old.values[parent.columns.iter().position(|c| c.id == *id).unwrap()].clone()).collect::<Vec<_>>();
                if values.contains(&Value::Null) { continue; }
                let child = state.tables.values().find(|t| t.id == *child_id).unwrap();
                let mut after = None;
                while let Some((last, rows)) = sink.child_page(child, columns, &values, after)? {
                    after = Some(last);
                    if !rows.is_empty() && *on_delete == catalog_ext::Action::Restrict {
                        return Err(CoreError::Sql(SqlError::RestrictViolation));
                    }
                    work.publish(sink, child, rows, &descriptors)?;
                }
            }
        }
        work.retention.release(searched::retained_row_bytes(&[], &old).map_err(CoreError::Sql)?);
    }
    if let Some(d) = deferred { *d = work.pending; }
    Ok(result)
}


struct DeleteWork {
    queue: std::collections::VecDeque<(u64, SqlRow)>,
    visited: BTreeSet<(u64, Vec<u8>)>,
    retention: scan::Retention,
    pending: DeferralState,
}
impl DeleteWork {
    fn publish(&mut self, sink: &mut impl RowSink, table: &TableDef, rows: DdlRows,
        descriptors: &[secondary_index::Descriptor]) -> Result<(), CoreError> {
        let mut fresh = DdlRows::new();
        for (key, row) in rows {
            if self.visited.contains(&(table.id, key.clone())) { continue; }
            // Charge the queue image and visited key before retaining either.
            self.retention.charge(searched::retained_row_bytes(&key, &row).map_err(CoreError::Sql)?
                + key.len() + 96).map_err(CoreError::Sql)?;
            self.visited.insert((table.id, key.clone()));
            for ((_, id), keys) in &mut self.pending.pending {
                if *id == table.id { keys.remove(&key); }
            }
            self.queue.push_back((table.id, row.clone()));
            fresh.insert(key, row);
        }
        sink.apply_indexes(table, Some(table), &fresh, &DdlRows::new(), descriptors)?;
        sink.apply(table, &fresh, &DdlRows::new())
    }
}
