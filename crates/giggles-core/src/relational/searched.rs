//! Searched mutations stage every postimage before publishing one batch.
use super::secondary_index_adapter::BatchRowChange;
use super::*;

pub(super) fn parse_target(
    p: &mut Parser,
    name: &str,
) -> R<(Vec<Predicate>, Option<Box<query::Query>>)> {
    let start = p.at;
    let initial_parameters = p.params.clone();
    let legacy = p.predicates(name);
    let legacy_parameters = p.params.clone();
    let legacy_end = p.at;
    p.at = start;
    p.params = initial_parameters;
    match query::Query::mutation_target(p, name) {
        Ok(target) => {
            let predicates = match legacy {
                Ok(predicates) if p.at == legacy_end => predicates,
                _ => vec![],
            };
            Ok((predicates, Some(Box::new(target))))
        }
        Err(_) => {
            p.at = legacy_end;
            p.params = legacy_parameters;
            legacy.map(|predicates| (predicates, None))
        }
    }
}

pub(super) fn target(prepared: &Prepared) -> Option<&query::Query> {
    match &prepared.statement {
        Statement::Update { search, .. }
        | Statement::UpdateExpressions { search, .. }
        | Statement::Delete { search, .. } => search.as_deref(),
        _ => None,
    }
}

#[expect(clippy::too_many_arguments, reason = "mirrors the execute arm that owns these inputs")]
pub(super) fn route(
    state: &mut Relational,
    prepared: &Prepared,
    params: &[Value],
    expected: Option<u64>,
    returning: &Option<returning::Plan<'_>>,
    bounds: R<()>,
    cancel: &mut impl FnMut() -> bool,
    clock: Option<&clock::ExecutionClock>,
) -> R<SqlResult> {
    if expected.is_some() {
        return Err(SqlError::Unbounded);
    }
    if let Err(error) = bounds
        && !matches!(error, SqlError::Unbounded | SqlError::Unsupported)
    {
        return Err(error);
    }
    let target = target(prepared).ok_or(SqlError::Unbounded)?;
    let table = state
        .tables
        .get(prepared.table().ok_or(SqlError::UnknownTable)?)
        .ok_or(SqlError::UnknownTable)?;
    let table_id = table.id;
    let parameters = describe(prepared, state)?
        .parameters
        .into_iter()
        .map(Some)
        .collect();
    let bound = target.bind_seeded(state, parameters)?.with_clock(clock);
    let updating = matches!(
        prepared.statement,
        Statement::Update { .. } | Statement::UpdateExpressions { .. }
    );
    let assignments = if updating {
        Some(assign::Plan::new(table, state, prepared, params, clock)?)
    } else {
        None
    };
    let mut retention = scan::Retention::new(scan::MAX_RETAINED_BYTES);
    let matched = bound.matching_rows(state, params, &mut retention, cancel)?;
    let mut next_version = state.next_version;
    let mut postimages = Vec::with_capacity(matched.len());
    for (key, old) in &matched {
        if cancel() {
            return Err(SqlError::Aborted);
        }
        if let Some(assignments) = &assignments {
            let mut row = old.clone();
            for (index, value) in assignments.evaluate(table, old, params, cancel)? {
                row.values[index] = value;
            }
            row_check(table, &row.values)?;
            retention.charge(retained_row_bytes(key, &row)?)?;
            row.version = allocate(&mut next_version)?;
            postimages.push(row);
        }
    }
    let projected = if updating {
        postimages.iter().collect::<Vec<_>>()
    } else {
        matched.iter().map(|(_, row)| row).collect()
    };
    let returned = returning::project(returning, state, params, &projected)?;
    let changes = matched
        .iter()
        .enumerate()
        .map(|(index, (key, old))| BatchRowChange {
            table: table_id,
            old: Some((key.as_slice(), old)),
            new: if updating {
                Some((key.as_slice(), &postimages[index]))
            } else {
                None
            },
        })
        .collect::<Vec<_>>();
    let mut result = state.secondary_apply_batch_engine(state.epoch, &changes, next_version, cancel)?;
    result.affected = matched.len() as u64;
    returning::attach(returning, &mut result, returned);
    Ok(result)
}

pub(super) fn retained_row_bytes(key: &[u8], row: &SqlRow) -> R<usize> {
    row.values.iter().try_fold(
        key.len()
            .checked_add(std::mem::size_of::<(Vec<u8>, SqlRow)>())
            .ok_or(SqlError::Limit)?,
        |bytes, value| {
            bytes
                .checked_add(std::mem::size_of::<Value>())
                .and_then(|bytes| bytes.checked_add(value_bytes(value)))
                .ok_or(SqlError::Limit)
        },
    )
}

pub(super) fn describe(prepared: &Prepared, state: &Relational) -> R<Description> {
    let table = state
        .tables
        .get(prepared.table().ok_or(SqlError::UnknownTable)?)
        .ok_or(SqlError::UnknownTable)?;
    let mut parameters = vec![None; prepared.params];
    if prepared.parameter_types.len() > parameters.len() {
        return Err(SqlError::Parameters);
    }
    parameters[..prepared.parameter_types.len()].clone_from_slice(&prepared.parameter_types);
    let infer = |value: &Expr, ty: &DataType, parameters: &mut [Option<DataType>]| -> R<()> {
        infer_mutation_parameter(value, ty, parameters, &prepared.parameter_types, true)
    };
    match &prepared.statement {
        Statement::Update { sets, .. } => {
            for (name, value) in sets {
                infer(
                    value,
                    &table.columns[col(table, name)?].data_type,
                    &mut parameters,
                )?;
            }
        }
        Statement::UpdateExpressions { sets, .. } => {
            if sets.len() > MAX_COLUMNS {
                return Err(SqlError::Limit);
            }
            let indexes = sets
                .iter()
                .map(|(name, _)| {
                    let index = col(table, name)?;
                    if table.primary_key.contains(&index) {
                        return Err(SqlError::Unsupported);
                    }
                    Ok(index)
                })
                .collect::<R<Vec<_>>>()?;
            unique(indexes.iter())?;
            for ((_, value), index) in sets.iter().zip(indexes) {
                let ty = &table.columns[index].data_type;
                match value {
                    assign::Value::Plain(value) => infer(value, ty, &mut parameters)?,
                    assign::Value::Scalar(value) => {
                        value.infer_assignment(table, ty, &mut parameters)?;
                    }
                }
            }
        }
        _ => {}
    }
    let target = target(prepared).ok_or(SqlError::Unbounded)?;
    target.infer_mutation_parameters(state, &mut parameters, &prepared.parameter_types)?;
    if let Some(returning) = prepared.returning() {
        let (columns, parameters, typmods) = returning.describe(state, parameters)?;
        let seed = parameters.iter().cloned().map(Some).collect::<Vec<_>>();
        target.bind_description(state, prepared.params, &seed)?;
        Ok(Description {
            columns,
            parameters,
            typmods,
        })
    } else {
        let bound = target.bind_description(state, prepared.params, &parameters)?;
        Ok(Description {
            columns: vec![],
            parameters: bound.description(state).parameters,
            typmods: vec![],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> Relational {
        let mut state = Relational::default();
        for sql in [
            "CREATE TABLE searched_cancel (id BIGINT PRIMARY KEY, n BIGINT)",
            "INSERT INTO searched_cancel VALUES (1, 10)",
            "INSERT INTO searched_cancel VALUES (2, 20)",
            "INSERT INTO searched_cancel VALUES (3, 30)",
        ] {
            let prepared = Prepared::parse(sql, 1, state.epoch).unwrap();
            state.execute(&prepared, &[], None).unwrap();
        }
        state
    }

    #[test]
    fn searched_heap_mutations_preserve_hidden_rowids_and_keys() {
        let mut state = Relational::default();
        for sql in [
            "CREATE TABLE searched_heap (n BIGINT)",
            "INSERT INTO searched_heap VALUES (10)",
            "INSERT INTO searched_heap VALUES (20)",
            "INSERT INTO searched_heap VALUES (30)",
        ] {
            let prepared = Prepared::parse(sql, 1, state.epoch).unwrap();
            state.execute(&prepared, &[], None).unwrap();
        }
        let table = &state.tables["searched_heap"];
        let table_id = table.id;
        let rowid = table.hidden_rowid().unwrap();
        let original = state.rows[&table_id].clone();
        let prepared = Prepared::parse(
            "UPDATE searched_heap SET n = n + 1 WHERE n >= 10",
            1,
            state.epoch,
        )
        .unwrap();
        assert_eq!(state.execute(&prepared, &[], None).unwrap().affected, 3);
        assert_eq!(
            state.rows[&table_id].keys().collect::<Vec<_>>(),
            original.keys().collect::<Vec<_>>(),
        );
        for (key, old) in &original {
            let updated = &state.rows[&table_id][key];
            assert_eq!(updated.values[rowid], old.values[rowid]);
            assert!(updated.version > old.version);
        }
        let updated = state.rows[&table_id].clone();
        let prepared = Prepared::parse(
            "DELETE FROM searched_heap WHERE n = 21",
            1,
            state.epoch,
        )
        .unwrap();
        assert_eq!(state.execute(&prepared, &[], None).unwrap().affected, 1);
        assert_eq!(state.rows[&table_id].len(), 2);
        for (key, survivor) in &state.rows[&table_id] {
            assert_eq!(survivor, &updated[key]);
            assert_eq!(survivor.values[rowid], original[key].values[rowid]);
        }
        state.validate().unwrap();
    }

    #[test]
    fn searched_postimage_retention_limit_never_publishes() {
        let state = fixture();
        let original = state.clone();
        let prepared = Prepared::parse(
            "UPDATE searched_cancel SET n = n + 1 WHERE n = 10",
            1,
            state.epoch,
        )
        .unwrap();
        let table = &state.tables["searched_cancel"];
        let (key, old) = state.rows[&table.id].iter().next().unwrap();
        let mut retention = scan::Retention::new(retained_row_bytes(key, old).unwrap());
        let parameters = describe(&prepared, &state)
            .unwrap()
            .parameters
            .into_iter()
            .map(Some)
            .collect();
        let bound = target(&prepared).unwrap().bind_seeded(&state, parameters).unwrap();
        let matched = bound
            .matching_rows(&state, &[], &mut retention, &mut || false)
            .unwrap();
        assert_eq!(matched.len(), 1);
        let (key, old) = &matched[0];
        let assignments = assign::Plan::new(table, &state, &prepared, &[], None).unwrap();
        let mut postimage = old.clone();
        for (index, value) in assignments
            .evaluate(table, old, &[], &mut || false)
            .unwrap()
        {
            postimage.values[index] = value;
        }
        row_check(table, &postimage.values).unwrap();
        assert_ne!(postimage.values, old.values);
        assert_eq!(
            retention.charge(retained_row_bytes(key, &postimage).unwrap()),
            Err(SqlError::Limit),
        );
        assert_eq!(state, original);
    }

    #[test]
    fn searched_update_polls_and_cancellation_never_publishes() {
        let original = fixture();
        let prepared = Prepared::parse(
            "UPDATE searched_cancel SET n = n + 1 WHERE n >= 10 RETURNING id, n",
            1,
            original.epoch,
        )
        .unwrap();
        let mut successful = original.clone();
        let mut polls = 0;
        let result = successful
            .execute_with_cancel(&prepared, &[], None, || {
                polls += 1;
                false
            })
            .unwrap();
        assert_eq!(result.affected, 3);
        assert_eq!(result.rows.len(), 3);
        assert!(polls >= 3);
        for stop_at in 1..=polls {
            let mut state = original.clone();
            let mut current = 0;
            let result = state.execute_with_cancel(&prepared, &[], None, || {
                current += 1;
                current == stop_at
            });
            assert_eq!(result, Err(SqlError::Aborted), "poll {stop_at}");
            assert_eq!(state, original, "poll {stop_at}");
        }
    }
}
