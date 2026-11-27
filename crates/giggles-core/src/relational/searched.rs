//! Searched mutations stage every postimage before publishing one batch.
//! PostgreSQL uses physical heap order and GigglesDB key order for immediate PK and secondary UNIQUE checks; they agree for ascending key loads.
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
        Err(SqlError::Limit)
            if p.tokens[start..].windows(3).any(|tokens| {
                matches!(tokens, [Token::Word(in_word), Token::Symbol('('), Token::Word(select)]
                    if in_word == "in" && select == "select")
            }) =>
        {
            Err(SqlError::Limit)
        }
        Err(_) => {
            p.at = legacy_end;
            p.params = legacy_parameters;
            legacy.map(|predicates| (predicates, None))
        }
    }
}

pub(super) fn target(prepared: &Prepared) -> Option<&query::Query> {
    match prepared.statement.get() {
        Statement::Update { search, .. }
        | Statement::UpdateExpressions { search, .. }
        | Statement::Delete { search, .. } => search.as_deref(),
        _ => None,
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "mirrors the execute arm that owns these inputs"
)]
pub(super) fn route(
    state: &mut Relational,
    description: &StatementDescriptionInput<'_>,
    params: &[Value],
    expected: Option<u64>,
    returning: &Option<returning::Plan<'_>>,
    bounds: R<()>,
    cancel: &mut impl FnMut() -> bool,
    clock: Option<&clock::ExecutionClock>,
    injected_source: Option<&dyn scan::RowSource>,
    events: &mut Vec<constraints::Event>,
) -> R<SqlResult> {
    if let Err(error) = bounds
        && !matches!(error, SqlError::Unbounded | SqlError::Unsupported)
    {
        return Err(if expected.is_some() { SqlError::Unbounded } else { error });
    }
    let prepared = description.prepared();
    let target = target(prepared).ok_or(SqlError::Unbounded)?;
    let name = prepared.table().ok_or(SqlError::UnknownTable)?;
    let table = state.tables.get(name).ok_or_else(|| missing_relation(name))?;
    if expected.is_some() {
        let predicates = match prepared.statement.get() {
            Statement::Update { predicates, .. }
            | Statement::UpdateExpressions { predicates, .. }
            | Statement::Delete { predicates, .. } => predicates,
            _ => return Err(SqlError::Unbounded),
        };
        // Version-guarded SQL is still restricted to a complete PK equality.
        // The bound proves that shape. The scan locates the collated key.
        if target.has_in_subquery()
            || table.primary_key.iter().all(|i| state.deterministic_column(table.columns[*i].id))
            || !super::bounds(table, predicates, params).is_ok_and(|bound| bound.point)
        {
            return Err(SqlError::Unbounded);
        }
    }
    let table_id = table.id;
    let parameters = description.searched_parameters(state)?;
    let bound = target.bind_seeded(state, parameters)?.with_clock(clock);
    let updating = matches!(
        prepared.statement.get(),
        Statement::Update { .. } | Statement::UpdateExpressions { .. }
    );
    let assignments = if updating {
        Some(assign::Plan::new(table, state, params, clock, description)?)
    } else {
        None
    };
    let written = assignments.iter().flat_map(assign::Plan::written_offsets).collect::<Vec<_>>();
    let mut retention = scan::Retention::new(scan::MAX_RETAINED_BYTES);
    let legacy = scan::LegacyRows(&*state);
    let source = injected_source.unwrap_or(&legacy);
    let matched = bound.matching_rows(state, source, params, &mut retention, cancel)?;
    if let Some(want) = expected
        && !matches!(matched.as_slice(), [(_, row)] if row.version == want)
    {
        return Err(SqlError::Version);
    }
    // Compare against the statement's selected row image, after writer locks.
    // The sink checks every target (including absent keys) before paging writes.
    if let Some(expected) = &prepared.native_delete {
        let versions = expected.iter().map(|(key, version)|
            Ok((encode_primary_key(table, key)?, *version))).collect::<R<BTreeMap<_, _>>>()?;
        for (key, row) in &matched {
            if versions.get(key) != Some(&row.version) { return Err(SqlError::Version); }
        }
    }
    let mut next_version = state.next_version;
    let mut postimages = Vec::with_capacity(matched.len());
    let mut postkeys = Vec::with_capacity(matched.len());
    let mut vacated = BTreeSet::new();
    let mut unique = updating
        .then(|| secondary_index_adapter::UpdateUnique::new(state, table, cancel))
        .transpose()?;
    for (key, old) in &matched {
        if cancel() {
            return Err(SqlError::Aborted);
        }
        if let Some(assignments) = &assignments {
            let mut row = old.clone();
            for (index, value) in assignments.evaluate(table, old, params, cancel)? {
                row.values[index] = value;
            }
            state.materialize_generated_written(table, &mut row.values, &written)?;
            row_check_written(state, table, &row.values, &written)?;
            state.check_written_candidate(table, &row.values, &written)?;
            let new_key = pk(table, &row.values)?;
            // Immediate uniqueness is checked in logical evaluation order. A later
            // matched row still occupies its key until that row is evaluated.
            let primary_taken = postkeys.contains(&new_key)
                || new_key != *key
                    && source.contains(table, &new_key)?
                    && !vacated.contains(new_key.as_slice());
            if primary_taken && !state.v2_unique_reporting {
                return Err(state.primary_duplicate(table, &row.values));
            }
            unique.as_mut().expect("UPDATE occupancy").visit(
                table,
                Some((key, old)),
                &new_key,
                &row.values,
                primary_taken,
                cancel,
            )?;
            vacated.insert(key.as_slice());
            retention.charge(retained_row_bytes(&new_key, &row)?)?;
            row.version = allocate(&mut next_version)?;
            postkeys.push(new_key);
            postimages.push(row);
        }
    }
    let pairs = matched.iter().enumerate().map(|(i, (_, old))|
        (Some(old), if updating { Some(&postimages[i]) } else { None })).collect::<Vec<_>>();
    let returned = returning::project_pairs(returning, state, params, &pairs)?;
    // Consolidate physical changes by key, keeping the central publisher's
    // immutable-key witness contract even when logical rows move between keys.
    let final_by_key = postkeys
        .iter()
        .zip(&postimages)
        .map(|(key, row)| (key.as_slice(), row))
        .collect::<BTreeMap<_, _>>();
    let extra = if updating {
        let mut extra = 0;
        for key in &postkeys {
            if !source.contains(table, key)? {
                extra += 1;
            }
        }
        extra
    } else {
        0
    };
    if matched
        .len()
        .checked_add(extra)
        .is_none_or(|n| n > MAX_ROWS)
    {
        return Err(SqlError::Limit);
    }
    let mut changes = Vec::with_capacity(matched.len() + extra);
    for (key, old) in &matched {
        changes.push(BatchRowChange {
            table: table_id,
            old: Some((key.as_slice(), old)),
            new: final_by_key
                .get(key.as_slice())
                .map(|row| (key.as_slice(), *row)),
        });
    }
    for (key, row) in postkeys.iter().zip(&postimages) {
        if !source.contains(table, key)? {
            changes.push(BatchRowChange {
                table: table_id,
                old: None,
                new: Some((key.as_slice(), row)),
            });
        }
    }
    for ((_, old), new) in matched.iter().zip(&postimages) {
        events.push(constraints::Event {
            table: table_id,
            old: old.clone(),
            new: Some(new.clone()),
        });
    }
    let mut result =
        state.secondary_apply_batch_engine(state.epoch, &changes, next_version, cancel)?;
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

#[cfg(test)]
thread_local! {
    pub(super) static DESCRIBE_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

pub(super) fn describe(prepared: &Prepared, state: &Relational) -> R<Description> {
    #[cfg(test)]
    DESCRIBE_CALLS.set(DESCRIBE_CALLS.get() + 1);

    let name = prepared.table().ok_or(SqlError::UnknownTable)?;
    let table = state.tables.get(name).ok_or_else(|| missing_relation(name))?;
    let mut parameters = vec![None; prepared.params];
    if prepared.parameter_types.len() > parameters.len() {
        return Err(SqlError::Parameters);
    }
    parameters[..prepared.parameter_types.len()].clone_from_slice(&prepared.parameter_types);
    let infer = |value: &Expr, column: &ColumnDef, parameters: &mut [Option<DataType>]| -> R<()> {
        enum_parameters::infer_column(value, column, state, parameters)?;
        infer_mutation_parameter(value, &column.data_type, parameters, &prepared.parameter_types, true)
    };
    match prepared.statement.get() {
        Statement::Update { sets, .. } => {
            for (name, value) in sets {
                infer(
                    value,
                    &table.columns[target_col(table, name)?],
                    &mut parameters,
                )?;
            }
        }
        Statement::UpdateExpressions { sets, .. } => {
            if sets.len() > MAX_TABLE_COLUMNS {
                return Err(SqlError::Limit);
            }
            let indexes = assign::indexes(sets, table, state)?;
            for ((_, value), index) in sets.iter().zip(indexes) {
                let ty = &table.columns[index].data_type;
                match value {
                    assign::Value::Default | assign::Value::Rows(_) => {}
                    assign::Value::Plain(value) => infer(value, &table.columns[index], &mut parameters)?,
                    assign::Value::Scalar(value) => {
                        value.infer_assignment(
                            state,
                            &{
                                let mut target = table.clone();
                                target.name =
                                    table.name.rsplit('\0').next().unwrap_or(&table.name).into();
                                target
                            },
                            ty,
                            &mut parameters,
                        )?;
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
            type_oids: vec![],
            columns,
            parameters,
            typmods,
        })
    } else {
        let bound = target.bind_description(state, prepared.params, &parameters)?;
        Ok(Description {
            type_oids: vec![],
            columns: vec![],
            parameters: bound.description(state).parameters,
            typmods: vec![],
        })
    }
}

impl Prepared {
    pub(crate) fn validate_native_delete_image(&self, state: &Relational) -> R<()> {
        if let Some(targets) = &self.native_delete {
            let table = &state.tables[self.table().unwrap()];
            let source = scan::LegacyRows(state);
            for (key, version) in targets {
                if scan::RowSource::get(&source, table, &encode_primary_key(table, key)?)?
                    .is_none_or(|row| row.row.version != *version) {
                    return Err(SqlError::Version);
                }
            }
        }
        Ok(())
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
    fn nonpaged_update_returning_shares_description_with_assignment_and_search() {
        let mut state = fixture();
        let prepared = Prepared::parse(
            "UPDATE searched_cancel SET n = n + $1 WHERE n >= $2 RETURNING id, n",
            1,
            state.epoch,
        )
        .unwrap()
        .with_parameter_types(&[Some(DataType::BigInt), None])
        .unwrap();

        DESCRIBE_CALLS.set(0);
        let result = state
            .execute(&prepared, &[Value::Int(2), Value::Int(20)], None)
            .unwrap();
        assert_eq!(DESCRIBE_CALLS.get(), 1);
        assert_eq!(result.affected, 2);
        assert_eq!(result.columns, ["id", "n"]);
        assert_eq!(result.rows[0].values, [Value::Int(2), Value::Int(22)]);
        assert_eq!(result.rows[1].values, [Value::Int(3), Value::Int(32)]);
    }

    #[test]
    fn searched_route_and_mutation_keys_keep_the_ambient_date_order() {
        use giggles_pg_types::temporal::DateOrder;

        let mut state = Relational::default();
        for sql in [
            "CREATE TABLE t(id bigint PRIMARY KEY, d date, n bigint)",
            "INSERT INTO t VALUES (1, '2020-01-13', 10), (2, '2020-01-14', 20)",
        ] {
            let prepared = Prepared::parse(sql, 1, state.epoch).unwrap();
            state.execute(&prepared, &[], None).unwrap();
        }
        let prepared = {
            let _order = clock::LiteralDateOrder::enter(DateOrder::Dmy);
            Prepared::parse(
                "UPDATE t SET n = n + $1 WHERE d = '13/01/2020' RETURNING id, n",
                1,
                state.epoch,
            )
            .unwrap()
        };
        let _order = clock::LiteralDateOrder::enter(DateOrder::Mdy);
        let memo = StatementDescription::new(&prepared);
        let input = memo.bind_schemas(&state).unwrap();
        assert_eq!(input.describe(&state).unwrap().parameters, [DataType::BigInt]);

        let expected = describe(input.prepared(), &state).unwrap_err();
        assert_eq!(expected, SqlError::DatetimeFieldOverflow);
        let params = [Value::Int(2)];
        DESCRIBE_CALLS.set(0);
        assert_eq!(
            state.execute(&prepared, &params, None).unwrap_err(),
            expected
        );
        assert_eq!(DESCRIBE_CALLS.get(), 2, "describe in both date orders");

        let bound_memo = StatementDescription::new(input.prepared());
        bound_memo.input().describe(&state).unwrap();
        DESCRIBE_CALLS.set(0);
        assert_eq!(
            state
                .mutation_keys(&bound_memo, &params, None, &scan::LegacyRows(&state))
                .unwrap_err(),
            expected
        );
        assert_eq!(DESCRIBE_CALLS.get(), 1, "describe in the ambient date order");
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
        let prepared =
            Prepared::parse("DELETE FROM searched_heap WHERE n = 21", 1, state.epoch).unwrap();
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
        let bound = target(&prepared)
            .unwrap()
            .bind_seeded(&state, parameters)
            .unwrap();
        let matched = bound
            .matching_rows(
                &state,
                &scan::LegacyRows(&state),
                &[],
                &mut retention,
                &mut || false,
            )
            .unwrap();
        assert_eq!(matched.len(), 1);
        let (key, old) = &matched[0];
        let description = StatementDescription::new(&prepared);
        let input = description.input();
        let assignments = assign::Plan::new(table, &state, &[], None, &input).unwrap();
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
