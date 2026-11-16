//! Exact-primary-key conflict handling over one complete staged snapshot.
use super::*;

pub(super) struct Plan {
    pub(super) columns: Vec<usize>,
    pub(super) expressions: Vec<Vec<Bound>>,
    pub(super) action: BoundConflict,
    pub(super) returning: Option<Vec<BoundProjection>>,
}

enum Change<'a> {
    Insert,
    Skip,
    Update {
        existing: usize,
        values: Vec<(usize, Cow<'a, Value>)>,
        changed: bool,
    },
}
impl Change<'_> {
    fn changed(&self) -> bool {
        match self {
            Self::Insert => true,
            Self::Skip => false,
            Self::Update { changed, .. } => *changed,
        }
    }
}

fn postimage<'a>(
    change: &'a Change<'_>,
    incoming: &'a [Cow<'_, Value>],
    existing: &'a [SqlRow],
    index: usize,
) -> &'a Value {
    match change {
        Change::Insert => incoming[index].as_ref(),
        Change::Update {
            existing: row,
            values,
            ..
        } => values
            .iter()
            .find(|(column, _)| *column == index)
            .map_or(&existing[*row].values[index], |(_, value)| value.as_ref()),
        Change::Skip => unreachable!("skipped tuple has no postimage"),
    }
}

pub(super) fn run(
    session: &mut SqlSession,
    table: &TableDef,
    plan: Plan,
    existing: Vec<SqlRow>,
    mut retained: Option<&mut usize>,
) -> Result<Outcome> {
    let mut incoming = Vec::new();
    let mut remaining_input = MAX_INSERT_BYTES;
    for expressions in &plan.expressions {
        crate::cancellation::check().map_err(core)?;
        let mut row = vec![Cow::Owned(Value::Null); table.columns.len()];
        for (index, expression) in plan.columns.iter().zip(expressions) {
            row[*index] = Cow::Owned(
                session
                    .coerce_column(&table.columns[*index], expression.eval(&[])?.into_owned())?,
            );
        }
        let mut remaining_row = crate::relational::MAX_ROW_BYTES;
        for (value, column) in row.iter().zip(&table.columns) {
            check(value, column)?;
            charge_value(&mut remaining_row, value)?;
            charge_value(&mut remaining_input, value)?;
        }
        incoming.push(row);
    }

    // All incoming payloads passed admission before any key payload is copied.
    let mut keys = Vec::new();
    let mut unique = BTreeSet::new();
    for (input_index, row) in incoming.iter().enumerate() {
        crate::cancellation::check().map_err(core)?;
        let key = table
            .primary_key
            .iter()
            .map(|index| row[*index].as_ref().clone())
            .collect::<Vec<_>>();
        let encoded =
            crate::relational::encode_key(&key).map_err(|error| core(CoreError::Sql(error)))?;
        if !unique.insert(encoded.clone()) {
            return Err(Error::DuplicateTarget);
        }
        keys.push((encoded, key, input_index));
    }
    let existing = if session.is_v2_engine() {
        // Keyed input: the only rows an upsert can match share a proposed key.
        session.native_key_rows(table, keys.iter().map(|(_, key, _)| key.clone())).map_err(core)?
    } else {
        existing
    };
    let mut existing_keys = BTreeMap::new();
    for (index, row) in existing.iter().enumerate() {
        crate::cancellation::check().map_err(core)?;
        let key = table
            .primary_key
            .iter()
            .map(|column| row.values[*column].clone())
            .collect::<Vec<_>>();
        let encoded =
            crate::relational::encode_key(&key).map_err(|error| core(CoreError::Sql(error)))?;
        existing_keys.insert(encoded, index);
    }
    let targets = keys
        .iter()
        .map(|(key, _, _)| existing_keys.get(key).copied())
        .collect::<Vec<_>>();
    let inserts = targets.iter().filter(|target| target.is_none()).count();
    if existing.len().checked_add(inserts).ok_or(Error::Limit)? > Context::max_read_rows(session) {
        return Err(Error::Limit);
    }

    // Every conflicted winner must pass its guard before any conflict SET RHS
    // runs. Incoming expressions necessarily precede identity resolution.
    if let BoundConflict::Update { guard, .. } = &plan.action {
        for (input_index, target) in targets.iter().enumerate() {
            crate::cancellation::check().map_err(core)?;
            if let Some(target) = target {
                let row = &existing[*target];
                let proposal = &incoming[input_index];
                if *guard.eval_conflict(&|index| &row.values[index], &|index| {
                    proposal[index].as_ref()
                })? != Value::Bool(true)
                {
                    return Err(Error::ConditionFailed);
                }
            }
        }
    }

    let mut changes = Vec::new();
    let mut remaining_mutation = MAX_MUTATION_BYTES;
    for (input_index, target) in targets.iter().enumerate() {
        crate::cancellation::check().map_err(core)?;
        let proposal = &incoming[input_index];
        let change = match (target, &plan.action) {
            (None, _) => Change::Insert,
            (Some(_), BoundConflict::Nothing) => Change::Skip,
            (Some(target), BoundConflict::Update { values, .. }) => {
                let row = &existing[*target];
                let evaluated: Vec<(usize, Cow<'_, Value>)> = values
                    .iter()
                    .map(|(index, expression)| {
                        Ok((
                            *index,
                            Cow::Owned(
                                session.coerce_column(
                                    &table.columns[*index],
                                    expression
                                        .eval_conflict(&|index| &row.values[index], &|index| {
                                            proposal[index].as_ref()
                                        })?
                                        .into_owned(),
                                )?,
                            ),
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let changed = evaluated
                    .iter()
                    .any(|(index, value)| value.as_ref() != &row.values[*index]);
                Change::Update {
                    existing: *target,
                    values: evaluated,
                    changed,
                }
            }
        };
        if !matches!(change, Change::Skip) {
            let mut remaining_row = crate::relational::MAX_ROW_BYTES;
            for (index, column) in table.columns.iter().enumerate() {
                let value = postimage(&change, proposal, &existing, index);
                check(value, column)?;
                charge_value(&mut remaining_row, value)?;
                // One budget covers all inserted/updated/no-op final rows.
                charge_value(&mut remaining_mutation, value)?;
            }
        }
        changes.push(change);
    }

    let mut out = Outcome {
        matched: incoming.len(),
        affected: changes
            .iter()
            .filter(|change| !matches!(change, Change::Skip))
            .count(),
        changed: changes.iter().filter(|change| change.changed()).count(),
        conflict_results: Some(
            changes
                .iter()
                .enumerate()
                .map(|(input_index, change)| ConflictTupleOutcome {
                    input_index,
                    status: if matches!(change, Change::Skip) {
                        ConflictStatus::Skipped
                    } else {
                        ConflictStatus::Success
                    },
                    changed: change.changed(),
                    returned_row: None,
                })
                .collect(),
        ),
        ..Outcome::default()
    };
    keys.sort_by(|left, right| left.0.cmp(&right.0));
    if let Some(projection) = &plan.returning {
        out.columns = projection
            .iter()
            .map(|item| item.column.name.clone())
            .collect();
        let mut remaining = crate::relational::MAX_RESULT_BYTES;
        let mut projected = Vec::new();
        for (_, _, input_index) in &keys {
            crate::cancellation::check().map_err(core)?;
            let change = &changes[*input_index];
            if matches!(change, Change::Skip) {
                continue;
            }
            let values = project_returning(
                projection,
                &|index| postimage(change, &incoming[*input_index], &existing, index),
                &mut remaining,
            )?;
            let version = match change {
                Change::Update {
                    existing: index, ..
                } => existing[*index].version,
                Change::Insert => 0,
                Change::Skip => unreachable!("skipped tuple has no RETURN"),
            };
            out.conflict_results.as_mut().expect("upsert outcomes")[*input_index].returned_row =
                Some(projected.len());
            projected.push((version, values));
        }
        for (_, values) in &projected {
            for value in values {
                charge_retained(&mut retained, value)?;
            }
        }
        // All RETURN and retention admission precedes any payload copy/stage.
        out.rows = projected
            .into_iter()
            .map(|(version, values)| SqlRow {
                version,
                values: values.into_iter().map(Cow::into_owned).collect(),
            })
            .collect();
    }

    crate::cancellation::check().map_err(core)?;
    for (_, key, input_index) in &keys {
        crate::cancellation::check().map_err(core)?;
        let change = &changes[*input_index];
        let result = match change {
            Change::Insert => session.insert_engine(
                table,
                incoming[*input_index]
                    .iter()
                    .map(|value| value.as_ref().clone())
                    .collect(),
            ),
            Change::Update {
                existing: index,
                values,
                changed: true,
            } => session.update_engine(
                table,
                key.clone(),
                values
                    .iter()
                    .map(|(index, value)| (*index, value.as_ref().clone()))
                    .collect(),
                existing[*index].version,
            ),
            Change::Skip | Change::Update { changed: false, .. } => continue,
        }
        .map_err(core)?;
        if result.affected != 1 {
            return Err(Error::Conflict);
        }
        if let Some(output_index) =
            out.conflict_results.as_ref().expect("upsert outcomes")[*input_index].returned_row
        {
            out.rows[output_index].version =
                session.row_version_engine(table, key).map_err(core)?;
        }
    }
    Ok(out)
}
