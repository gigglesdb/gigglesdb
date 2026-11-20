//! GD-0058 heap-table (hidden rowid) unit tests. E3-owned.
use super::*;

fn run(state: &mut Relational, sql: &str) -> SqlResult {
    let prepared = Prepared::parse(sql, 1, state.epoch).unwrap();
    state.execute(&prepared, &[], None).unwrap()
}

fn fixture() -> Relational {
    let mut state = Relational::default();
    run(&mut state, "CREATE TABLE h (a BIGINT, b TEXT)");
    state
}

#[test]
fn heap_insert_allocates_one_version_and_update_preserves_rowid() {
    let mut state = fixture();
    assert!(state.contains_heap()); // Empty heap tables count for the format gate.
    let table = state.tables["h"].clone();
    let mut prior = 0;
    for _ in 0..3 {
        let next = state.next_version;
        run(&mut state, "INSERT INTO h VALUES (1, 'x')");
        assert_eq!(state.next_version, next + 1);
        let row = state.rows[&table.id].values().last().unwrap();
        assert_eq!(row.version, next);
        assert_eq!(row.values[2], Value::Int(i64::try_from(next).unwrap()));
        assert!(row.version > prior);
        prior = row.version;
    }
    let key = Value::Int(1);
    let encoded = encode_key(std::slice::from_ref(&key)).unwrap();
    let original = state.rows[&table.id][&encoded].clone();
    // Exercise the internal mutation path; native typed heap access is rejected.
    let update = Prepared::engine(
        Statement::Update {
            name: table.name.clone(),
            sets: vec![("b".into(), Expr::Literal(Value::Text("z".into())))],
            predicates: vec![Predicate {
                column: "giggles_rowid".into(),
                op: "=".into(),
                value: Expr::Literal(key),
            }],
            search: None,
            returning: None,
        },
        1,
        state.epoch,
    );
    state.execute(&update, &[], None).unwrap();
    let updated = &state.rows[&table.id][&encoded];
    assert_eq!(updated.values[2], original.values[2]);
    assert!(updated.version > original.version);
    assert_eq!(updated.values[1], Value::Text("z".into()));
    state.validate().unwrap();
}

#[test]
fn heap_rowid_overflow_is_checked_without_publication() {
    let mut state = fixture();
    state.next_version = i64::MAX as u64 + 1;
    let before = state.clone();
    let insert = Prepared::parse("INSERT INTO h VALUES (1, 'x')", 1, state.epoch).unwrap();
    assert_eq!(state.execute(&insert, &[], None), Err(SqlError::Overflow));
    assert_eq!(state, before);
}

#[test]
fn heap_marker_is_exact_and_invalid_offsets_are_safe() {
    let state = fixture();
    let table = &state.tables["h"];
    assert_eq!(table.hidden_rowid(), Some(2));
    assert_eq!(table.logical_offsets(), [0, 1]);
    for keys in [vec![], vec![2, 0], vec![usize::MAX], vec![0]] {
        let mut malformed = table.clone();
        malformed.primary_key = keys;
        assert_eq!(malformed.hidden_rowid(), None);
    }
    for column in [
        ColumnDef {
            name: "rowid".into(),
            ..table.columns[2].clone()
        },
        ColumnDef {
            data_type: DataType::Integer,
            ..table.columns[2].clone()
        },
        ColumnDef {
            not_null: false,
            ..table.columns[2].clone()
        },
    ] {
        let mut malformed = table.clone();
        malformed.columns[2] = column;
        assert_eq!(malformed.hidden_rowid(), None);
    }
    state.validate().unwrap();
    let mut malformed = state.clone();
    std::sync::Arc::make_mut(&mut malformed.tables).get_mut("h").unwrap().columns[2].not_null = false;
    assert_eq!(malformed.validate(), Err(SqlError::Reserved));
    let mut malformed = state.clone();
    std::sync::Arc::make_mut(&mut malformed.tables).get_mut("h").unwrap().primary_key = vec![usize::MAX];
    assert_eq!(malformed.validate(), Err(SqlError::Reserved));
    let mut malformed = state;
    std::sync::Arc::make_mut(&mut malformed.tables).get_mut("h").unwrap().columns[0].name = "giggles_other".into();
    assert_eq!(malformed.validate(), Err(SqlError::Reserved));
}

#[test]
fn native_create_still_requires_keys_with_original_error_precedence() {
    let state = Relational::default();
    let column = ColumnDef {
        id: 0,
        name: "a".into(),
        data_type: DataType::BigInt,
        not_null: true,
    };
    let create = |name: &str, columns: Vec<ColumnDef>| {
        Prepared::create_engine(name.into(), columns, vec![], 1, &state).unwrap_err()
    };
    assert_eq!(create("native", vec![column.clone()]), SqlError::Unbounded);
    assert_eq!(
        create("native", vec![column.clone(); MAX_COLUMNS]),
        SqlError::Unbounded
    );
    assert_eq!(create("native", vec![]), SqlError::Limit);
    assert_eq!(
        create("native", vec![column.clone(); MAX_COLUMNS + 1]),
        SqlError::Limit
    );
    assert_eq!(create("giggles_reserved", vec![]), SqlError::Reserved);
    assert_eq!(
        create(
            "native",
            vec![ColumnDef {
                name: "giggles_reserved".into(),
                ..column
            }]
        ),
        SqlError::Unbounded
    );
}

#[test]
fn legacy_select_rejects_heap_before_descriptors_or_rows() {
    let mut state = fixture();
    let prepared = Prepared::engine(
        Statement::Select {
            name: "h".into(),
            projection: vec![],
            predicates: vec![],
            order: vec![],
            limit: Some(Expr::Literal(Value::Int(10))),
        },
        1,
        state.epoch,
    );
    assert_eq!(prepared.describe(&state).unwrap_err(), SqlError::Unsupported);
    assert_eq!(
        state.execute(&prepared, &[], None),
        Err(SqlError::Unsupported)
    );
}

// Moved from sql_insert.rs (GD-0042 file) to keep E3 test churn in E3-owned files.
mod heap_allocation_tests {
    use super::*;

    fn run(state: &mut Relational, sql: &str) -> R<SqlResult> {
        let prepared = Prepared::parse(sql, 1, state.epoch)?;
        state.execute(&prepared, &[], None)
    }

    fn fixture() -> Relational {
        let mut state = Relational::default();
        run(&mut state, "CREATE TABLE h (a BIGINT, b TEXT)").unwrap();
        state
    }

    #[test]
    fn heap_multi_row_versions_follow_input_order_with_one_allocation_each() {
        for suffix in ["", " ON CONFLICT DO NOTHING"] {
            let mut state = fixture();
            let first = state.next_version;
            run(
                &mut state,
                &format!("INSERT INTO h VALUES (30, NULL), (10, 'x'), (20, NULL){suffix}"),
            )
            .unwrap();
            assert_eq!(state.next_version, first + 3);
            let table = &state.tables["h"];
            for (index, (key, row)) in state.rows[&table.id].iter().enumerate() {
                let version = first + index as u64;
                let rowid = Value::Int(i64::try_from(version).unwrap());
                assert_eq!(row.version, version);
                assert_eq!(row.values[2], rowid);
                assert_eq!(*key, encode_key(&[rowid]).unwrap());
                assert_eq!(row.values[0], Value::Int([30, 10, 20][index]));
                assert!(row.version < state.next_version);
            }
            state.validate().unwrap();
            state.secondary_validate_engine(|| false).unwrap();
        }
    }

    #[test]
    fn heap_conflict_update_keeps_winner_key_and_rowid_with_mixed_inserts() {
        let mut state = fixture();
        run(&mut state, "CREATE UNIQUE INDEX unique_a ON h (a)").unwrap();
        run(&mut state, "INSERT INTO h VALUES (1, 'old')").unwrap();
        let table = state.tables["h"].clone();
        let (key, original) = state.rows[&table.id].iter().next().unwrap();
        let key = key.clone();
        let original = original.clone();
        let first = state.next_version;
        assert_eq!(
            run(
                &mut state,
                "INSERT INTO h VALUES (3, NULL), (1, 'new'), (2, 'two') ON CONFLICT (a) DO UPDATE SET b=EXCLUDED.b",
            )
            .unwrap()
            .affected,
            3
        );
        assert_eq!(state.next_version, first + 3);
        let updated = &state.rows[&table.id][&key];
        assert_eq!(updated.values[2], original.values[2]);
        assert_eq!(updated.values[1], Value::Text("new".into()));
        assert_eq!(updated.version, first);
        for (value, version) in [(3, first + 1), (2, first + 2)] {
            let rowid = Value::Int(i64::try_from(version).unwrap());
            let row = &state.rows[&table.id][&encode_key(std::slice::from_ref(&rowid)).unwrap()];
            assert_eq!(row.version, version);
            assert_eq!(row.values[0], Value::Int(value));
            assert_eq!(row.values[2], rowid);
        }
        let first = state.next_version;
        assert_eq!(
            run(
                &mut state,
                "INSERT INTO h VALUES (1, 'skip'), (4, NULL), (4, 'skip') ON CONFLICT DO NOTHING",
            )
            .unwrap()
            .affected,
            1
        );
        assert_eq!(state.next_version, first + 1);
        let rowid = Value::Int(i64::try_from(first).unwrap());
        let inserted = &state.rows[&table.id][&encode_key(std::slice::from_ref(&rowid)).unwrap()];
        assert_eq!(inserted.version, first);
        assert_eq!(inserted.values, [Value::Int(4), Value::Null, rowid]);
        let before = state.clone();
        assert_eq!(
            run(
                &mut state,
                "INSERT INTO h VALUES (1, 'skip'), (4, 'skip') ON CONFLICT DO NOTHING",
            )
            .unwrap()
            .affected,
            0
        );
        assert_eq!(state, before);
        state.validate().unwrap();
        state.secondary_validate_engine(|| false).unwrap();
    }

    #[test]
    fn heap_multi_row_rowid_overflow_never_publishes_a_partial_batch() {
        for suffix in [
            "",
            " ON CONFLICT DO NOTHING",
            " ON CONFLICT (a) DO UPDATE SET b=EXCLUDED.b",
        ] {
            let mut state = fixture();
            run(&mut state, "CREATE UNIQUE INDEX unique_a ON h (a)").unwrap();
            run(&mut state, "INSERT INTO h VALUES (1, 'old')").unwrap();
            // The first proposed row fits; the second rowid cannot fit i64.
            state.next_version = i64::MAX as u64;
            let before = state.clone();
            assert_eq!(
                run(
                    &mut state,
                    &format!("INSERT INTO h VALUES (2, NULL), (3, 'x'){suffix}"),
                ),
                Err(SqlError::Overflow)
            );
            assert_eq!(state, before);
        }
        let mut state = fixture();
        run(&mut state, "CREATE UNIQUE INDEX unique_a ON h (a)").unwrap();
        run(&mut state, "INSERT INTO h VALUES (1, 'old')").unwrap();
        state.next_version = i64::MAX as u64;
        let before = state.clone();
        assert_eq!(
            run(
                &mut state,
                "INSERT INTO h VALUES (1, 'new'), (2, NULL) ON CONFLICT (a) DO UPDATE SET b=EXCLUDED.b",
            ),
            Err(SqlError::Overflow)
        );
        assert_eq!(state, before);
        state.next_version = i64::MAX as u64 + 1;
        let rowid = state.rows[&state.tables["h"].id]
            .values()
            .next()
            .unwrap()
            .values[2]
            .clone();
        // UPDATE allocates a version but does not convert it into a new rowid.
        run(
            &mut state,
            "INSERT INTO h VALUES (1, 'new') ON CONFLICT (a) DO UPDATE SET b=EXCLUDED.b",
        )
        .unwrap();
        let updated = state.rows[&state.tables["h"].id].values().next().unwrap();
        assert_eq!(updated.values[2], rowid);
        assert_eq!(updated.version, i64::MAX as u64 + 1);
    }
}
