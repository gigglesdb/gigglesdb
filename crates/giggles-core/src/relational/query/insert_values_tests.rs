use super::*;
fn state(indexed: bool) -> Relational {
    let mut state = Relational::default();
    let p = Prepared::parse(
        "CREATE TABLE ins (id BIGINT PRIMARY KEY, txt TEXT, n INTEGER)",
        1,
        state.epoch,
    )
    .unwrap();
    state.execute_with_cancel(&p, &[], None, || false).unwrap();
    if indexed {
        let table = state.tables["ins"].clone();
        state
            .create_secondary_index_engine(
                &table,
                state.epoch,
                "txt_index",
                &[table.columns[1].id],
                || false,
            )
            .unwrap();
    }
    let p = Prepared::parse("INSERT INTO ins VALUES(1,'before',2)", 1, state.epoch).unwrap();
    state.execute_with_cancel(&p, &[], None, || false).unwrap();
    state
}
fn same_rows(before: &Relational, after: &Relational) {
    assert_eq!(before.rows, after.rows);
    assert_eq!(before.generation, after.generation);
    assert_eq!(before.tables, after.tables);
    assert_eq!(before.secondary, after.secondary);
    assert_eq!(before.epoch, after.epoch);
    assert_eq!(before.next_version, after.next_version);
    assert_eq!(before.next_id, after.next_id);
}
#[test]
fn insert_candidate_callback_index_error_and_budget_atomicity() {
    let mut state = state(true);
    let before = state.clone();
    for sql in [
        "INSERT INTO ins VALUES(1,lower('DUP'),3)",
        "INSERT INTO ins VALUES(2,lower('NO'),abs(-2147483648))",
        "INSERT INTO ins VALUES(2,repeat('x',65536),3)",
    ] {
        let p = Prepared::parse(sql, 1, state.epoch).unwrap();
        assert_eq!(p.table(), Some("ins"));
        assert_eq!(p.command(), "INSERT");
        assert!(state.execute_with_cancel(&p, &[], None, || false).is_err());
        same_rows(&before, &state);
        state.secondary_validate_engine(|| false).unwrap();
    }
    let p = Prepared::parse(
        "INSERT INTO ins VALUES(2,lower('PURE'),abs(-4))",
        1,
        state.epoch,
    )
    .unwrap();
    assert_eq!(
        state.execute_with_cancel(&p, &[], None, || true),
        Err(SqlError::Aborted)
    );
    same_rows(&before, &state);
    let cancellation = crate::Cancellation::default();
    cancellation.cancel();
    let result = cancellation
        .scope(|| state.execute_with_cancel(&p, &[], None, || false))
        .unwrap();
    assert_eq!(result.affected, 1);
    state.secondary_validate_engine(|| false).unwrap();
}
#[test]
fn insert_candidate_expected_version_and_constant_parameter_phases() {
    let mut no_index = state(false);
    let p = Prepared::parse(
        "INSERT INTO ins VALUES(2,lower('A'),abs(-2147483648))",
        1,
        no_index.epoch,
    )
    .unwrap();
    assert_eq!(
        no_index.execute_with_cancel(&p, &[], Some(1), || true),
        Err(SqlError::Unsupported)
    );
    assert_eq!(
        p.validate_immutable_constants(&no_index),
        Err(SqlError::Overflow)
    );
    let p = Prepared::parse(
        "INSERT INTO ins VALUES(2,lower('A'),abs($1::integer))",
        1,
        no_index.epoch,
    )
    .unwrap();
    assert_eq!(
        p.describe(&no_index).unwrap().parameters,
        vec![DataType::Integer]
    );
    assert_eq!(p.validate_immutable_constants(&no_index), Ok(()));
    assert_eq!(
        no_index.execute_with_cancel(&p, &[Value::Int(i32::MIN.into())], None, || false),
        Err(SqlError::Overflow)
    );
    // This existing custom-plan parameter phase differs from PG Bind; no Bind parameter evaluation is added.
    let mut indexed = state(true);
    let p = Prepared::parse("INSERT INTO ins VALUES(2,lower('A'),1)", 1, indexed.epoch).unwrap();
    assert_eq!(
        indexed.execute_with_cancel(&p, &[], Some(1), || true),
        Err(SqlError::Aborted)
    );
}
#[test]
fn insert_candidate_rejects_postfix_depth_columns_aggregates_and_context() {
    let state = state(false);
    let deep = format!(
        "INSERT INTO ins VALUES(2,{} ,1)",
        "'a'".to_string() + &"::text".repeat(70)
    );
    assert!(matches!(
        Prepared::parse(&deep, 1, state.epoch),
        Err(SqlError::Limit)
    ));
    for sql in [
        "INSERT INTO ins VALUES(2,lower('A'),id)",
        "INSERT INTO ins VALUES(2,lower('A'),sum(1))",
        "INSERT INTO ins VALUES(2,date_trunc('day',timestamptz '2000-01-01 UTC')::text,1)",
    ] {
        let result = Prepared::parse(sql, 1, state.epoch).and_then(|p| p.describe(&state));
        assert!(result.is_err(), "{sql}");
    }
}

#[test]
fn insert_values_count_star_uses_grouping_error() {
    let state = state(false);
    let error = Prepared::parse(
        "INSERT INTO ins VALUES(2,lower('A'),count(*))",
        1,
        state.epoch,
    )
    .err()
    .unwrap();
    eprintln!("COUNT(*) VALUES classification: {error:?}");
    assert_eq!(error, SqlError::GroupingError);
}

fn unique_state() -> Relational {
    let mut state = Relational::default();
    let p = Prepared::parse(
        "CREATE TABLE ins (id BIGINT PRIMARY KEY, txt TEXT, n INTEGER)",
        1,
        state.epoch,
    )
    .unwrap();
    state.execute_with_cancel(&p, &[], None, || false).unwrap();
    let table = state.tables["ins"].clone();
    let index = state
        .create_secondary_index_internal_engine(
            &table,
            state.epoch,
            "txt_unique",
            &[table.columns[1].id],
            true,
            || false,
        )
        .unwrap();
    assert!(
        state
            .secondary_descriptors_engine()
            .iter()
            .any(|d| d.id == index && d.unique)
    );
    let p = Prepared::parse("INSERT INTO ins VALUES(1,'before',2)", 1, state.epoch).unwrap();
    state.execute_with_cancel(&p, &[], None, || false).unwrap();
    state
}

#[test]
fn insert_values_admission_and_apply_callback_differential() {
    for (case, base, literal, expression, duplicate) in [
        (
            "nonunique_success",
            state(true),
            "INSERT INTO ins VALUES(2,'after',4)",
            "INSERT INTO ins VALUES(2,lower('AFTER'),abs(-4))",
            false,
        ),
        (
            "unique_duplicate",
            unique_state(),
            "INSERT INTO ins VALUES(2,'before',7)",
            "INSERT INTO ins VALUES(2,lower('BEFORE'),abs(-7))",
            true,
        ),
    ] {
        let mut old_state = base.clone();
        let mut new_state = base.clone();
        let old = Prepared::parse(literal, 1, base.epoch).unwrap();
        let new = Prepared::parse(expression, 1, base.epoch).unwrap();
        assert!(matches!(old.statement, Statement::Insert { .. }));
        assert!(matches!(new.statement, Statement::InsertExpressions(_)));
        let mut old_calls = 0;
        let old_result = old_state.execute_with_cancel(&old, &[], None, || {
            old_calls += 1;
            false
        });
        let mut new_calls = 0;
        let new_result = new_state.execute_with_cancel(&new, &[], None, || {
            new_calls += 1;
            false
        });
        assert_eq!(old_result, new_result);
        assert!(old_calls > 0);
        // The expression tuple has exactly three cells; its only extra callback
        // sites are normalization entry, each cell, and the completion fence.
        assert_eq!(new_calls, old_calls + 3 + 2);
        same_rows(&old_state, &new_state);
        if duplicate {
            assert_eq!(new_result, Err(SqlError::Duplicate));
            same_rows(&base, &new_state);
        } else {
            assert_eq!(new_result.unwrap().affected, 1);
        }
        old_state.secondary_validate_engine(|| false).unwrap();
        new_state.secondary_validate_engine(|| false).unwrap();
        eprintln!(
            "INSERT_GUARD_COUNTS case={case} old={old_calls} new={new_calls} cells=3 expected_delta=5"
        );
    }
}

#[test]
fn insert_values_existing_unique_descriptor_error_keeps_complete_state() {
    let mut state = unique_state();
    let before = state.clone();
    for (sql, expected) in [
        (
            "INSERT INTO ins VALUES(2,lower('BEFORE'),abs(-7))",
            SqlError::Duplicate,
        ),
        (
            "INSERT INTO ins VALUES(2,lower('NEW'),abs(-2147483648))",
            SqlError::Overflow,
        ),
    ] {
        let p = Prepared::parse(sql, 1, state.epoch).unwrap();
        assert_eq!(
            state.execute_with_cancel(&p, &[], None, || false),
            Err(expected)
        );
        same_rows(&before, &state);
        state.secondary_validate_engine(|| false).unwrap();
    }
}

#[test]
fn insert_values_runtime_parameter_kinds_widths_and_nulls_match_query_contract() {
    let mut state = Relational::default();
    let create = Prepared::parse(
        "CREATE TABLE arg_kinds(id BIGINT PRIMARY KEY,n INTEGER,small SMALLINT,d DOUBLE PRECISION,txt TEXT)",
        1,
        state.epoch,
    )
    .unwrap();
    state
        .execute_with_cancel(&create, &[], None, || false)
        .unwrap();
    let table = state.tables["arg_kinds"].clone();
    state
        .create_secondary_index_engine(
            &table,
            state.epoch,
            "arg_text",
            &[table.columns[4].id],
            || false,
        )
        .unwrap();
    let before = state.clone();
    let insert = Prepared::parse(
        "INSERT INTO arg_kinds(id,txt,n) VALUES($1,lower($2),abs($3))",
        1,
        state.epoch,
    )
    .unwrap();
    assert_eq!(
        insert.describe(&state).unwrap().parameters,
        vec![DataType::BigInt, DataType::Text, DataType::DoublePrecision]
    );
    for wrong in [
        Value::Int(-2),
        Value::Float4(Float4::from_bits((-2.5f32).to_bits())),
        Value::Bool(false),
    ] {
        let query = Prepared::parse("SELECT abs($1)", 1, state.epoch).unwrap();
        assert_eq!(
            state.execute_with_cancel(&query, std::slice::from_ref(&wrong), None, || false),
            Err(SqlError::Type)
        );
        let result = state.execute_with_cancel(
            &insert,
            &[Value::Int(2), Value::Text("wrong".into()), wrong],
            None,
            || false,
        );
        assert_eq!(result, Err(SqlError::Type));
        same_rows(&before, &state);
    }
    for (cast, wrong) in [("integer", i64::MAX), ("smallint", 32768)] {
        let insert = Prepared::parse(
            &format!("INSERT INTO arg_kinds(id,txt,n) VALUES($1,lower($2),abs($3::{cast}))"),
            1,
            state.epoch,
        )
        .unwrap();
        let query = Prepared::parse(&format!("SELECT abs($1::{cast})"), 1, state.epoch).unwrap();
        assert_eq!(
            state.execute_with_cancel(&query, &[Value::Int(wrong)], None, || false),
            Err(SqlError::Type)
        );
        assert_eq!(
            state.execute_with_cancel(
                &insert,
                &[
                    Value::Int(2),
                    Value::Text("width".into()),
                    Value::Int(wrong)
                ],
                None,
                || false
            ),
            Err(SqlError::Type)
        );
        same_rows(&before, &state);
    }
    assert_eq!(
        state.execute_with_cancel(
            &insert,
            &[
                Value::Int(2),
                Value::Text("unused".into()),
                Value::Null,
                Value::Bool(false)
            ],
            None,
            || false
        ),
        Err(SqlError::Parameters)
    );
    assert_eq!(
        state.execute_with_cancel(
            &insert,
            &[Value::Int(2), Value::Text("missing".into())],
            None,
            || false
        ),
        Err(SqlError::Parameters)
    );
    same_rows(&before, &state);
    let typed = Prepared::parse(
        "INSERT INTO arg_kinds(id,txt,d) VALUES($1,lower($2),abs($3))",
        1,
        state.epoch,
    )
    .unwrap();
    for (id, value) in [
        (2, Value::Null),
        (3, Value::Float8(Float8::from_bits(f64::NAN.to_bits()))),
    ] {
        assert_eq!(
            state
                .execute_with_cancel(
                    &typed,
                    &[
                        Value::Int(id),
                        Value::Text(format!("typed{id}")),
                        value.clone()
                    ],
                    None,
                    || false
                )
                .unwrap()
                .affected,
            1
        );
        let row = &state.rows[&table.id]
            .values()
            .find(|row| row.values[0] == Value::Int(id))
            .unwrap()
            .values;
        assert_eq!(row[3], value);
    }
    state.secondary_validate_engine(|| false).unwrap();
}
