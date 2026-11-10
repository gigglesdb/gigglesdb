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
