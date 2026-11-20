use super::*;

fn prepare(state: &Relational, sql: &str) -> Prepared {
    Prepared::parse(sql, 1, state.epoch).unwrap()
}
fn execute(state: &mut Relational, sql: &str, params: &[Value]) -> R<SqlResult> {
    let prepared = prepare(state, sql);
    state.execute_with_cancel(&prepared, params, None, || false)
}
fn state() -> Relational {
    let mut state = Relational::default();
    execute(
        &mut state,
        "CREATE TABLE r (id BIGINT PRIMARY KEY, n INTEGER, txt TEXT)",
        &[],
    )
    .unwrap();
    state
}

#[test]
fn v2_projection_removes_row_count_cap_but_keeps_single_row_bound() {
    let state = state();
    let prepared = prepare(&state, "DELETE FROM r RETURNING txt");
    let returning = prepared.returning().unwrap();
    let row = [Value::Int(1), Value::Int(2), Value::Text("x".into())];
    let rows = vec![row.as_slice(); MAX_ROWS + 1];
    assert_eq!(returning.project(&state, &[], &rows), Err(SqlError::Limit));
    {
        let _scope = spill::returning_unbounded();
        assert_eq!(returning.project(&state, &[], &rows).unwrap().len(), rows.len());
        let wide = [Value::Int(1), Value::Int(2), Value::Text("x".repeat(40_000))];
        let projection = prepare(&state, &format!("DELETE FROM r RETURNING {}", ["txt"; 27].join(",")));
        assert_eq!(projection.returning().unwrap().project(&state, &[], &[&wide]), Err(SqlError::Limit));
    }
    assert_eq!(returning.project(&state, &[], &rows), Err(SqlError::Limit));
}

#[test]
fn returning_uses_select_metadata_and_new_old_row_versions() {
    let mut state = state();
    let insert = prepare(
        &state,
        "INSERT INTO r VALUES ($1, $2, 'hello') RETURNING *, upper(txt) AS label, $2 AS number",
    );
    let description = insert.describe(&state).unwrap();
    assert_eq!(
        description.parameters,
        vec![DataType::BigInt, DataType::Integer]
    );
    assert_eq!(
        description
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        vec!["id", "n", "txt", "label", "number"]
    );
    assert_eq!(description.columns[4].data_type, DataType::Integer);
    let result = state
        .execute_with_cancel(&insert, &[Value::Int(1), Value::Int(4)], None, || false)
        .unwrap();
    assert_eq!(result.affected, 1);
    assert_eq!(
        result.rows[0].values,
        vec![
            Value::Int(1),
            Value::Int(4),
            Value::Text("hello".into()),
            Value::Text("HELLO".into()),
            Value::Int(4)
        ]
    );
    let inserted_version = result.rows[0].version;
    let updated = execute(
        &mut state,
        "UPDATE r SET n = 8 WHERE id = 1 RETURNING r.n",
        &[],
    )
    .unwrap();
    assert_eq!(updated.rows[0].values, vec![Value::Int(8)]);
    assert!(updated.rows[0].version > inserted_version);
    let deleted = execute(&mut state, "DELETE FROM r WHERE id = 1 RETURNING n", &[]).unwrap();
    assert_eq!(deleted.rows[0], updated.rows[0]);
    assert_eq!(deleted.affected, 1);
    let empty = execute(
        &mut state,
        "DELETE FROM r WHERE id = 1 RETURNING n AS value",
        &[],
    )
    .unwrap();
    assert_eq!(empty.columns, vec!["value"]);
    assert!(empty.rows.is_empty());
    assert_eq!(empty.affected, 0);
}

#[test]
fn returning_errors_precede_mutation_and_bind_even_without_rows() {
    let mut state = state();
    execute(&mut state, "INSERT INTO r VALUES (1, 4, 'hello')", &[]).unwrap();
    let before = state.clone();
    for sql in [
        "INSERT INTO r VALUES (2, 3, 'x') RETURNING missing",
        "UPDATE r SET n = 5 WHERE id = 1 RETURNING missing",
        "DELETE FROM r WHERE id = 1 RETURNING missing",
        "UPDATE r SET n = 5 WHERE id = 9 RETURNING missing",
    ] {
        let prepared = prepare(&state, sql);
        assert_eq!(
            prepared.describe(&state).unwrap_err(),
            SqlError::UnknownColumn
        );
        assert_eq!(
            state.execute_with_cancel(&prepared, &[], None, || false),
            Err(SqlError::UnknownColumn)
        );
        assert_eq!(state, before);
    }
    for aggregate in ["count(*)", "sum(n)", "min(n)", "max(n)"] {
        assert_eq!(
            Prepared::parse(
                &format!("DELETE FROM r WHERE id = 1 RETURNING {aggregate}"),
                1,
                state.epoch
            )
            .unwrap_err(),
            SqlError::GroupingError
        );
    }
    assert_eq!(
        execute(
            &mut state,
            "UPDATE r SET n = 9 WHERE id = 1 RETURNING abs(-2147483648)",
            &[]
        ),
        Err(SqlError::Overflow)
    );
    assert_eq!(state, before);
}

#[test]
fn returning_projection_reuses_binder_and_admits_total_output() {
    let mut state = state();
    let mut parser = Parser { privilege_seen: false, privilege_casts: false,
        tokens: lex("RETURNING id, upper(txt) AS label").unwrap(),
        at: 0,
        params: BTreeSet::new(),
        relations: Vec::new(),
    };
    let returning = Returning::parse(&mut parser, "r").unwrap().unwrap();
    let input = vec![Value::Int(3), Value::Int(4), Value::Text("hello".into())];
    let output = returning.project(&state, &[], &[&input]).unwrap();
    assert_eq!(
        output[0].values,
        vec![Value::Int(3), Value::Text("HELLO".into())]
    );
    let text = "x".repeat(40_000);
    execute(
        &mut state,
        "INSERT INTO r VALUES (1, 4, $1)",
        &[Value::Text(text)],
    )
    .unwrap();
    let before = state.clone();
    let projection = std::iter::repeat_n("txt", 32).collect::<Vec<_>>().join(",");
    assert_eq!(
        execute(
            &mut state,
            &format!("DELETE FROM r WHERE id = 1 RETURNING {projection}"),
            &[]
        ),
        Err(SqlError::Limit)
    );
    assert_eq!(state, before);
}

#[test]
fn returning_identifiers_and_excluded_insert_forms() {
    let mut state = Relational::default();
    execute(
        &mut state,
        "CREATE TABLE returning (id BIGINT PRIMARY KEY, returning TEXT)",
        &[],
    )
    .unwrap();
    let result = execute(
        &mut state,
        "INSERT INTO returning VALUES (1, 'yes') RETURNING returning.returning",
        &[],
    )
    .unwrap();
    assert_eq!(result.rows[0].values, vec![Value::Text("yes".into())]);
    let result = execute(
        &mut state,
        "INSERT INTO returning VALUES (2, 'a'), (3, 'b') RETURNING id",
        &[],
    )
    .unwrap();
    assert_eq!(result.affected, 2);
    assert_eq!(
        result
            .rows
            .iter()
            .map(|row| row.values.clone())
            .collect::<Vec<_>>(),
        vec![vec![Value::Int(2)], vec![Value::Int(3)]]
    );
    let result = execute(
        &mut state,
        "INSERT INTO returning VALUES (2, 'a') ON CONFLICT DO NOTHING RETURNING id",
        &[],
    )
    .unwrap();
    assert_eq!(result.affected, 0);
    assert!(result.rows.is_empty());
    assert_eq!(result.columns, ["id"]);
}

#[test]
fn scalar_insert_preserves_original_parameter_slots_for_returning() {
    let mut state = state();
    let prepared = prepare(
        &state,
        "INSERT INTO r VALUES ($2, abs($4::integer), upper($3)) RETURNING $1::integer AS tag, id, n, txt, $2 AS repeated_id, $3 AS input_text",
    );
    let description = prepared.describe(&state).unwrap();
    prepared.validate_immutable_constants(&state).unwrap();
    assert_eq!(
        description.parameters,
        vec![
            DataType::Integer,
            DataType::BigInt,
            DataType::Text,
            DataType::Integer
        ]
    );
    let result = state
        .execute_with_cancel(
            &prepared,
            &[
                Value::Int(7),
                Value::Int(3),
                Value::Text("hello".into()),
                Value::Int(-4),
            ],
            None,
            || false,
        )
        .unwrap();
    assert_eq!(
        result.rows[0].values,
        vec![
            Value::Int(7),
            Value::Int(3),
            Value::Int(4),
            Value::Text("HELLO".into()),
            Value::Int(3),
            Value::Text("hello".into())
        ]
    );
    let unresolved = prepare(
        &state,
        "INSERT INTO r VALUES ($2, 1, upper($3)) RETURNING $1",
    );
    assert_eq!(
        unresolved.describe(&state).unwrap_err(),
        SqlError::Parameters
    );
    let before = state.clone();
    assert_eq!(
        state.execute_with_cancel(
            &unresolved,
            &[Value::Int(7), Value::Int(4), Value::Text("x".into())],
            None,
            || false
        ),
        Err(SqlError::Parameters)
    );
    assert_eq!(state, before);
}
