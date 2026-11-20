use super::*;

fn fixture(values: Vec<Vec<Value>>) -> (Relational, Descriptor) {
    let t = TableDef {
        id: 1,
        name: "things".into(),
        columns: vec![
            ColumnDef {
                id: 2,
                name: "id".into(),
                data_type: DataType::BigInt,
                not_null: true,
            },
            ColumnDef {
                id: 3,
                name: "a".into(),
                data_type: DataType::Text,
                not_null: false,
            },
            ColumnDef {
                id: 4,
                name: "b".into(),
                data_type: DataType::Bytes,
                not_null: false,
            },
        ],
        primary_key: vec![0],
    };
    let mut state = Relational {
        next_id: 5,
        next_version: values.len() as u64 + 1,
        epoch: 7,
        generation: 12,
        ..Relational::default()
    };
    let rows = values
        .into_iter()
        .enumerate()
        .map(|(i, values)| {
            (
                pk(&t, &values).unwrap(),
                SqlRow {
                    values,
                    version: i as u64 + 1,
                },
            )
        })
        .collect();
    state.rows.insert(1, rows);
    std::sync::Arc::make_mut(&mut state.tables).insert(t.name.clone(), t);
    (
        state,
        Descriptor {
            id: 5,
            table: 1,
            name: "unique_ab".into(),
            columns: vec![3, 4],
            key_encoding: 1,
            unique: false,
        },
    )
}
fn row(id: i64, a: Value, b: Value) -> Vec<Value> {
    vec![Value::Int(id), a, b]
}
fn r(id: i64, a: &str, b: &[u8]) -> Vec<Value> {
    row(id, Value::Text(a.into()), Value::Bytes(b.into()))
}
fn result(state: &Relational, d: &Descriptor) -> R<Counts> {
    validate(state, d, state.epoch, state.generation, HARD, || false).map(|(_, c)| c)
}
fn error(state: &Relational, d: &Descriptor) -> SqlError {
    result(state, d).unwrap_err()
}
fn raw_state(state: &Relational) -> Vec<u8> {
    bincode::serialize(state).unwrap()
}

#[test]
fn complete_zero_one_two_rows_and_exact_borrowed_sources() {
    for n in 0..=2 {
        let (state, d) = fixture((0..n).map(|i| r(i, &i.to_string(), &[i as u8])).collect());
        let before = raw_state(&state);
        let (proof, counts) = validate(&state, &d, 7, 12, HARD, || false).unwrap();
        assert_eq!((counts.rows, counts.candidates), (n as usize, n as usize));
        assert!(std::ptr::eq(proof._state, &state));
        assert!(std::ptr::eq(proof._descriptor, &d));
        assert!(std::ptr::eq(proof._table, &state.tables["things"]));
        assert!(std::ptr::eq(proof._rows, &state.rows[&1]));
        assert_eq!((proof._epoch, proof._generation), (7, 12));
        assert_eq!(before, raw_state(&state));
    }
}

#[test]
fn composite_duplicate_across_distinct_full_primary_keys_is_duplicate() {
    let (state, d) = fixture(vec![
        r(i64::MIN, "same", b"same"),
        r(i64::MAX, "same", b"same"),
    ]);
    let before = raw_state(&state);
    assert_eq!(error(&state, &d), SqlError::Duplicate);
    assert_eq!(before, raw_state(&state));
}

#[test]
fn repeated_null_in_each_component_never_conflicts() {
    for values in [
        vec![
            row(1, Value::Null, Value::Bytes(vec![0])),
            row(2, Value::Null, Value::Bytes(vec![0])),
        ],
        vec![
            row(1, Value::Text("a".into()), Value::Null),
            row(2, Value::Text("a".into()), Value::Null),
        ],
        vec![
            row(1, Value::Null, Value::Null),
            row(2, Value::Null, Value::Null),
        ],
    ] {
        let (state, d) = fixture(values);
        assert_eq!(result(&state, &d).unwrap().rows, 2);
    }
}

// Independent logical oracle: compare typed fields, never encode_key or encoded
// bytes. Enumerate repeated/escaped values and NULL in both positions.
#[test]
fn typed_tuple_oracle_matches_canonical_text_binary_escaping() {
    let a = [
        Value::Null,
        Value::Text("".into()),
        Value::Text("\0".into()),
        Value::Text("a\0z".into()),
    ];
    let b = [
        Value::Null,
        Value::Bytes(vec![]),
        Value::Bytes(vec![0]),
        Value::Bytes(vec![0, 255, 0]),
    ];
    for x in 0..16 {
        for y in 0..16 {
            let av = &a[x / 4];
            let bv = &b[x % 4];
            let cv = &a[y / 4];
            let dv = &b[y % 4];
            let duplicate =
                ![av, bv, cv, dv].iter().any(|v| matches!(v, Value::Null)) && av == cv && bv == dv;
            let (state, mut d) = fixture(vec![
                row(-1, av.clone(), bv.clone()),
                row(9, cv.clone(), dv.clone()),
            ]);
            assert_eq!(
                result(&state, &d).err(),
                duplicate.then_some(SqlError::Duplicate)
            );
            d.columns.reverse();
            assert_eq!(
                result(&state, &d).err(),
                duplicate.then_some(SqlError::Duplicate)
            );
            let (proof, _) = if duplicate {
                continue;
            } else {
                validate(&state, &d, 7, 12, HARD, || false).unwrap()
            };
            assert_eq!(proof._descriptor.columns, [4, 3]);
        }
    }
}

#[test]
fn boolean_and_integer_boundaries_use_legacy_admission() {
    for (ty, lo, hi) in [
        (DataType::SmallInt, i16::MIN as i64, i16::MAX as i64),
        (DataType::Integer, i32::MIN as i64, i32::MAX as i64),
        (DataType::BigInt, i64::MIN, i64::MAX),
    ] {
        let (mut state, d) = fixture(vec![
            row(1, Value::Int(lo), Value::Bool(false)),
            row(2, Value::Int(hi), Value::Bool(true)),
        ]);
        let t = std::sync::Arc::make_mut(&mut state.tables).get_mut("things").unwrap();
        t.columns[1].data_type = ty;
        t.columns[2].data_type = DataType::Boolean;
        assert!(result(&state, &d).is_ok());
    }
    let (mut state, d) = fixture(vec![row(
        1,
        Value::Int(i16::MAX as i64 + 1),
        Value::Bytes(vec![]),
    )]);
    std::sync::Arc::make_mut(&mut state.tables).get_mut("things").unwrap().columns[1].data_type = DataType::SmallInt;
    assert_eq!(error(&state, &d), SqlError::Type);
}

#[test]
fn unsupported_declared_keys_reject_even_empty() {
    for ty in [
        DataType::Numeric,
        DataType::Real,
        DataType::DoublePrecision,
        DataType::Date,
        DataType::Time,
        DataType::TimeTz,
        DataType::Timestamp,
        DataType::TimestampTz,
        DataType::Interval,
    ] {
        let (mut state, d) = fixture(vec![]);
        std::sync::Arc::make_mut(&mut state.tables).get_mut("things").unwrap().columns[1].data_type = ty;
        assert_eq!(error(&state, &d), SqlError::Unsupported);
    }
}

#[test]
fn exact_stable_ownership_and_metadata_reject_mismatch() {
    let (state, d) = fixture(vec![]);
    for columns in [vec![], vec![0], vec![3, 3], vec![999]] {
        let mut changed = d.clone();
        changed.columns = columns;
        assert!(result(&state, &changed).is_err());
    }
    let mut changed = d.clone();
    changed.table = 2;
    assert_eq!(error(&state, &changed), SqlError::Type);
    changed = d.clone();
    changed.key_encoding = 2;
    assert_eq!(error(&state, &changed), SqlError::Type);
    changed = d.clone();
    changed.id = 2;
    assert_eq!(error(&state, &changed), SqlError::Type);
    let mut bad = state.clone();
    std::sync::Arc::make_mut(&mut bad.tables).get_mut("things").unwrap().name = "elsewhere".into();
    assert_eq!(error(&bad, &d), SqlError::Type);
    bad = state.clone();
    std::sync::Arc::make_mut(&mut bad.tables).get_mut("things").unwrap().columns[1].id = 2;
    assert_eq!(error(&bad, &d), SqlError::Type);
    bad = state.clone();
    std::sync::Arc::make_mut(&mut bad.tables).get_mut("things").unwrap().primary_key = vec![0, 0];
    assert_eq!(error(&bad, &d), SqlError::Type);
    bad = state.clone();
    bad.next_id = 4;
    assert_eq!(error(&bad, &d), SqlError::Type);
    bad = state.clone();
    bad.format = 2;
    assert_eq!(error(&bad, &d), SqlError::Unsupported);
    bad = state.clone();
    bad.rows.clear();
    assert_eq!(error(&bad, &d), SqlError::Type);
}

#[test]
fn malformed_primary_identities_and_versions_are_separate_from_unique_conflict() {
    let (state, d) = fixture(vec![r(1, "x", b"y"), r(2, "x", b"y")]);
    let mut bad = state.clone();
    let map = bad.rows.get_mut(&1).unwrap();
    let (_, v) = map.pop_first().unwrap();
    map.insert(vec![255], v);
    assert_eq!(error(&bad, &d), SqlError::Version);
    bad = state.clone();
    bad.rows
        .get_mut(&1)
        .unwrap()
        .values_mut()
        .next()
        .unwrap()
        .values[0] = Value::Int(2);
    assert_eq!(error(&bad, &d), SqlError::Version);
    for version in [0, 3, u64::MAX] {
        bad = state.clone();
        bad.rows
            .get_mut(&1)
            .unwrap()
            .values_mut()
            .next()
            .unwrap()
            .version = version;
        assert_eq!(error(&bad, &d), SqlError::Version);
    }
    bad = state.clone();
    for row in bad.rows.get_mut(&1).unwrap().values_mut() {
        row.version = 1;
    }
    assert_eq!(error(&bad, &d), SqlError::Version);
    bad = state.clone();
    bad.rows
        .get_mut(&1)
        .unwrap()
        .values_mut()
        .next()
        .unwrap()
        .values
        .pop();
    assert_eq!(error(&bad, &d), SqlError::Type);
}

#[test]
fn exact_budget_and_each_one_less_and_usize_max_refuse() {
    let (state, d) = fixture(vec![r(1, "one", b"one"), r(2, "two", b"two")]);
    let counts = result(&state, &d).unwrap();
    let exact = Budget {
        rows: counts.rows,
        candidates: counts.candidates,
        bytes: counts.bytes,
        work: counts.work,
    };
    assert!(validate(&state, &d, 7, 12, exact, || false).is_ok());
    for which in 0..4 {
        let mut b = exact;
        match which {
            0 => b.rows -= 1,
            1 => b.candidates -= 1,
            2 => b.bytes -= 1,
            _ => b.work -= 1,
        }
        assert_eq!(
            validate(&state, &d, 7, 12, b, || false).err().unwrap(),
            SqlError::Limit
        );
        let mut b = HARD;
        match which {
            0 => b.rows = usize::MAX,
            1 => b.candidates = usize::MAX,
            2 => b.bytes = usize::MAX,
            _ => b.work = usize::MAX,
        }
        assert_eq!(
            validate(&state, &d, 7, 12, b, || false).err().unwrap(),
            SqlError::Limit
        );
    }
    assert_eq!(add(usize::MAX, 1), Err(SqlError::Limit));
    assert_eq!(mul(usize::MAX, 2), Err(SqlError::Limit));
}

#[test]
fn complete_row_hard_limit_and_n_plus_one() {
    let (state, d) = fixture((0..1024).map(|i| r(i, &i.to_string(), b"b")).collect());
    assert_eq!(result(&state, &d).unwrap().rows, 1024);
    let (state, d) = fixture((0..1025).map(|i| r(i, &i.to_string(), b"b")).collect());
    assert_eq!(error(&state, &d), SqlError::Limit);
}

#[test]
fn oversized_later_payload_and_descriptor_admit_before_comparisons() {
    let (mut state, d) = fixture(vec![r(1, "duplicate", b"b"), r(2, "duplicate", b"b")]);
    state
        .rows
        .get_mut(&1)
        .unwrap()
        .values_mut()
        .last()
        .unwrap()
        .values[2] = Value::Bytes(vec![0; MAX_ROW_BYTES + 1]);
    assert_eq!(error(&state, &d), SqlError::Limit);
    let mut huge = d.clone();
    huge.name = "x".repeat(si::MAX_NAME + 1);
    let mut calls = 0;
    assert_eq!(
        validate(&state, &huge, 7, 12, HARD, || {
            calls += 1;
            true
        })
        .err()
        .unwrap(),
        SqlError::Limit
    );
    assert_eq!(calls, 0);
}

#[test]
fn cancel_at_every_checkpoint_leaves_raw_state_and_counters_unchanged() {
    let (state, d) = fixture(vec![r(1, "one", b"one"), r(2, "two", b"two")]);
    let before = raw_state(&state);
    let mut total = 0;
    validate(&state, &d, 7, 12, HARD, || {
        total += 1;
        false
    })
    .unwrap();
    assert!(total > 20);
    for stop in 1..=total {
        let mut n = 0;
        assert_eq!(
            validate(&state, &d, 7, 12, HARD, || {
                n += 1;
                n == stop
            })
            .err()
            .unwrap(),
            SqlError::Aborted
        );
        assert_eq!(n, stop);
        assert_eq!(raw_state(&state), before);
    }
}

#[test]
fn explicit_false_ignores_cancelled_ambient_and_preserves_it() {
    let (state, d) = fixture(vec![r(1, "a", b"b")]);
    let scope = crate::Cancellation::new(None);
    assert!(scope.cancel());
    scope.scope(|| {
        assert!(crate::cancellation::check().is_err());
        assert!(
            state
                .unique_readiness_engine(&d, 7, 12, HARD, || false)
                .is_ok()
        );
        assert!(crate::cancellation::check().is_err());
    });
}

#[test]
fn stale_expected_epoch_generation_reject_and_distinct_source_is_not_rebound() {
    let (state, d) = fixture(vec![]);
    for (epoch, generation) in [(8, 12), (7, 13)] {
        assert_eq!(
            validate(&state, &d, epoch, generation, HARD, || false)
                .err()
                .unwrap(),
            SqlError::StaleSchema
        );
    }
    let other = state.clone();
    let proof = state
        .unique_readiness_engine(&d, 7, 12, HARD, || false)
        .unwrap();
    assert!(!std::ptr::eq(proof._state, &other));
    // Compile-fail fixture retained with artifacts proves mutation and private
    // proof/source access cannot form a promotion API; no fake consumer here.
}

#[test]
fn typed_nonkeys_stay_lossless_including_bounded_numeric() {
    let values = [
        Value::Numeric(Numeric::parse("-123456789.00100").unwrap()),
        Value::Float4(Float4::from_bits(0x80000000)),
        Value::Float8(Float8::from_bits(0x7ff8000000000001)),
    ];
    for (ty, value) in [DataType::Numeric, DataType::Real, DataType::DoublePrecision]
        .into_iter()
        .zip(values)
    {
        let (mut state, mut d) = fixture(vec![row(1, Value::Text("key".into()), value)]);
        d.columns = vec![3];
        std::sync::Arc::make_mut(&mut state.tables).get_mut("things").unwrap().columns[2].data_type = ty;
        let before = raw_state(&state);
        assert!(result(&state, &d).is_ok());
        assert_eq!(before, raw_state(&state));
    }
}

#[test]
fn raw_row_bytes_exact_and_n_plus_one_and_oversized_primary_shape() {
    let (state, d) = fixture(vec![r(1, &"x".repeat(MAX_ROW_BYTES - 9), b"")]);
    assert!(result(&state, &d).is_ok());
    let (state, d) = fixture(vec![r(1, &"x".repeat(MAX_ROW_BYTES - 8), b"")]);
    assert_eq!(error(&state, &d), SqlError::Limit);
    let (mut state, d) = fixture(vec![r(1, "a", b"b")]);
    let row = state.rows.get_mut(&1).unwrap().pop_first().unwrap().1;
    state
        .rows
        .get_mut(&1)
        .unwrap()
        .insert(vec![0; si::MAX_COMPONENT + 1], row);
    assert_eq!(error(&state, &d), SqlError::Limit);
}

#[test]
fn admission_cancels_before_later_rows_and_numeric_reservation_is_finite() {
    let (state, d) = fixture((0..1024).map(|i| r(i, "a", b"b")).collect());
    let mut calls = 0;
    assert_eq!(
        validate(&state, &d, 7, 12, HARD, || {
            calls += 1;
            calls == 7
        })
        .err()
        .unwrap(),
        SqlError::Aborted
    );
    assert_eq!(calls, 7);
    let n = Value::Numeric(Numeric::parse(&"9".repeat(giggles_pg_types::MAX_DIGITS)).unwrap());
    let (mut state, mut d) = fixture(vec![row(1, Value::Text("a".into()), n.clone())]);
    d.columns = vec![3];
    std::sync::Arc::make_mut(&mut state.tables).get_mut("things").unwrap().columns[2].data_type = DataType::Numeric;
    let before = raw_state(&state);
    assert!(result(&state, &d).is_ok());
    assert_eq!(before, raw_state(&state));
    let t = &state.tables["things"];
    let values = row(2, Value::Text("b".into()), n);
    let key = pk(t, &values).unwrap();
    state
        .rows
        .get_mut(&1)
        .unwrap()
        .insert(key, SqlRow { values, version: 2 });
    state.next_version = 3;
    assert_eq!(error(&state, &d), SqlError::Limit);
}

#[test]
fn trusted_consumer_refuses_equal_numbers_on_another_source_or_descriptor() {
    let (state, d) = fixture(vec![]);
    let other = state.clone();
    let other_d = d.clone();
    let proof = state
        .unique_readiness_engine(&d, state.epoch, state.generation, HARD, || false)
        .unwrap();
    assert_eq!(
        proof.consume_for(&other, &d, other.epoch, other.generation),
        Err(SqlError::StaleSchema)
    );
    let proof = state
        .unique_readiness_engine(&d, state.epoch, state.generation, HARD, || false)
        .unwrap();
    assert_eq!(
        proof.consume_for(&state, &other_d, state.epoch, state.generation),
        Err(SqlError::StaleSchema)
    );
    let proof = state
        .unique_readiness_engine(&d, state.epoch, state.generation, HARD, || false)
        .unwrap();
    assert_eq!(
        proof.consume_for(&state, &d, state.epoch, state.generation + 1),
        Err(SqlError::StaleSchema)
    );
    let proof = state
        .unique_readiness_engine(&d, state.epoch, state.generation, HARD, || false)
        .unwrap();
    proof
        .consume_for(&state, &d, state.epoch, state.generation)
        .unwrap();
}
