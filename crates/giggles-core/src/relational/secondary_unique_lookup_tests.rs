use super::*;

fn limits() -> Limits {
    Limits {
        bytes: MAX_BYTES,
        work: MAX_WORK,
        candidates: si::MAX_CANDIDATES,
    }
}
fn fixture() -> (Relational, TableDef, Descriptor) {
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
                name: "pk2".into(),
                data_type: DataType::Text,
                not_null: true,
            },
            ColumnDef {
                id: 4,
                name: "a".into(),
                data_type: DataType::Text,
                not_null: false,
            },
            ColumnDef {
                id: 5,
                name: "b".into(),
                data_type: DataType::Bytes,
                not_null: false,
            },
            ColumnDef {
                id: 6,
                name: "payload".into(),
                data_type: DataType::Numeric,
                not_null: false,
            },
        ],
        primary_key: vec![0, 1],
    };
    let mut s = Relational {
        epoch: 7,
        next_id: 10,
        next_version: 2,
        generation: 12,
        ..Relational::default()
    };
    let values = vec![
        Value::Int(1),
        Value::Text("p\0".into()),
        Value::Text("".into()),
        Value::Bytes(vec![]),
        Value::Numeric(Numeric::parse("1.25").unwrap()),
    ];
    s.rows.insert(
        1,
        [(
            pk(&t, &values).unwrap(),
            SqlRow {
                values,
                version: 37,
            },
        )]
        .into(),
    );
    std::sync::Arc::make_mut(&mut s.tables).insert(t.name.clone(), t.clone());
    let cols = [4, 5];
    s.secondary_configure_test(t.id, "decoy_nonunique", &cols, || false)
        .unwrap();
    s.secondary_configure_test(t.id, "decoy_unique", &cols, || false)
        .unwrap();
    let id = s
        .secondary_configure_test(t.id, "selected", &cols, || false)
        .unwrap();
    mark_unique(&mut s);
    let d = s
        .secondary
        .descriptors()
        .iter()
        .find(|d| d.id == id)
        .unwrap()
        .clone();
    (s, t, d)
}
fn eq() -> [Value; 2] {
    [Value::Text("".into()), Value::Bytes(vec![])]
}
fn lookup<'a>(
    s: &'a Relational,
    t: &TableDef,
    d: &Descriptor,
    e: &[Value],
) -> R<Option<(&'a [u8], &'a SqlRow)>> {
    with_sql_parser_forbidden(|| {
        s.secondary_unique_lookup_engine(t, s.epoch, d, e, &limits(), || false)
    })
}
fn frozen(s: &Relational) -> (Vec<u8>, Vec<u8>, u64, u64, u64, u64) {
    (
        bincode::serialize(&(s.tables.as_ref(), &s.rows)).unwrap(),
        bincode::serialize(&s.secondary).unwrap(),
        s.epoch,
        s.generation,
        s.next_id,
        s.next_version,
    )
}
#[test]
fn e618_distinguishing_exact_id_borrowed_composite_pk_version_and_nohit() {
    let (s, t, d) = fixture();
    let before = frozen(&s);
    let (key, row) = lookup(&s, &t, &d, &eq()).unwrap().unwrap();
    let (actual_key, actual_row) = s.rows[&1].first_key_value().unwrap();
    assert!(std::ptr::eq(key.as_ptr(), actual_key.as_ptr()));
    assert!(std::ptr::eq(row, actual_row));
    assert_eq!(row.version, 37);
    assert!(
        lookup(
            &s,
            &t,
            &d,
            &[Value::Text("nohit".into()), Value::Bytes(vec![])]
        )
        .unwrap()
        .is_none()
    );
    assert_eq!(frozen(&s), before);
}
#[test]
fn exact_identity_gates_including_null_and_empty_source() {
    let (s, t, d) = fixture();
    for empty in [false, true] {
        let mut s = s.clone();
        if empty {
            s.rows.get_mut(&1).unwrap().clear();
            s.secondary = si::Sidecar::default();

            // Recreate valid empty descriptors without fabricating runtime fields.
            for (name, unique) in [("nonunique", false), ("first", true), ("selected", true)] {
                s.create_secondary_index_internal_engine(
                    &t,
                    s.epoch,
                    name,
                    &[4, 5],
                    unique,
                    || false,
                )
                .unwrap();
            }
        }
        let actual = s.secondary.descriptors().last().unwrap().clone();
        if empty {
            assert!(lookup(&s, &t, &actual, &eq()).unwrap().is_none());
        }
        for e in [eq(), [Value::Null, Value::Null]] {
            for which in 0..7 {
                let mut bad = actual.clone();
                match which {
                    0 => bad.id = u64::MAX,
                    1 => bad.table += 1,
                    2 => bad.unique = false,
                    3 => bad.columns.reverse(),
                    4 => bad.key_encoding += 1,
                    5 => bad.id = s.secondary.descriptors()[0].id,
                    _ => bad.columns.pop().map(|_| ()).unwrap(),
                };
                assert!(lookup(&s, &t, &bad, &e).is_err(), "{which} empty={empty}");
            }
            assert_eq!(
                s.secondary_unique_lookup_engine(&t, s.epoch + 1, &actual, &e, &limits(), || false),
                Err(SqlError::StaleSchema)
            );
            let mut bad = t.clone();
            bad.columns[0].name = "altered".into();
            assert!(lookup(&s, &bad, &actual, &e).is_err());
        }
    }
    assert!(d.id > s.secondary.descriptors()[0].id);
}
#[test]
fn null_first_second_all_still_validate_types_and_source() {
    let (s, t, d) = fixture();
    for e in [
        [Value::Null, Value::Bytes(vec![])],
        [Value::Text("".into()), Value::Null],
        [Value::Null, Value::Null],
    ] {
        assert!(lookup(&s, &t, &d, &e).unwrap().is_none());
        let mut bad = s.clone();
        bad.rows.get_mut(&1).unwrap().clear();
        assert!(lookup(&bad, &t, &d, &e).is_err());
        for typ in [
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
            for position in [0, 2, 3] {
                let mut bad = t.clone();
                bad.columns[position].data_type = typ.clone();
                assert!(lookup(&s, &bad, &d, &e).is_err());
            }
        }
    }
}
#[test]
fn all_six_legacy_kinds_and_empty_binary_values() {
    for (typ, value) in [
        (DataType::Boolean, Value::Bool(false)),
        (DataType::SmallInt, Value::Int(i16::MIN.into())),
        (DataType::Integer, Value::Int(i32::MAX.into())),
        (DataType::BigInt, Value::Int(i64::MIN)),
        (DataType::Text, Value::Text("".into())),
        (DataType::Bytes, Value::Bytes(vec![])),
    ] {
        let (mut s, mut t, _) = fixture();
        s.secondary = si::Sidecar::default();
        t.columns[2].data_type = typ;
        std::sync::Arc::make_mut(&mut s.tables).insert(t.name.clone(), t.clone());
        s.rows
            .get_mut(&1)
            .unwrap()
            .values_mut()
            .next()
            .unwrap()
            .values[2] = value.clone();
        let id = s
            .secondary_configure_test(t.id, "typed", &[4], || false)
            .unwrap();
        mark_unique(&mut s);
        let d = s
            .secondary
            .descriptors()
            .iter()
            .find(|d| d.id == id)
            .unwrap();
        assert!(lookup(&s, &t, d, &[value]).unwrap().is_some());
    }
}
#[test]
fn huge_witnesses_on_empty_source_reject_before_callback() {
    let (mut s, t, d) = fixture();
    s.rows.get_mut(&1).unwrap().clear();
    let mut calls = 0;
    for which in 0..6 {
        let mut t = t.clone();
        let mut d = d.clone();
        let mut e = eq().to_vec();
        match which {
            0 => e[0] = Value::Text("x".repeat(MAX_ROW_BYTES + 1)),
            1 => d.name = "x".repeat(si::MAX_NAME + 1),
            2 => t.name = "x".repeat(64),
            3 => t.columns[0].name = "x".repeat(64),
            4 => d.columns = vec![4; 65],
            _ => e = vec![Value::Null; 65],
        };
        assert_eq!(
            s.secondary_unique_lookup_engine(&t, s.epoch, &d, &e, &limits(), || {
                calls += 1;
                false
            }),
            Err(SqlError::Limit)
        );
    }
    assert_eq!(calls, 0);
}
#[test]
fn cancellation_every_stage_and_ambient_independence() {
    let (s, t, d) = fixture();
    let before = frozen(&s);
    let mut calls = 0;
    s.secondary_unique_lookup_engine(&t, s.epoch, &d, &eq(), &limits(), || {
        calls += 1;
        false
    })
    .unwrap();
    for stop in 1..=calls {
        let mut n = 0;
        assert_eq!(
            s.secondary_unique_lookup_engine(&t, s.epoch, &d, &eq(), &limits(), || {
                n += 1;
                n == stop
            }),
            Err(SqlError::Aborted)
        );
        assert_eq!(frozen(&s), before);
    }
    let scope = crate::Cancellation::new(None);
    assert!(scope.cancel());
    scope.scope(|| {
        assert!(crate::cancellation::check().is_err());
        assert!(lookup(&s, &t, &d, &eq()).unwrap().is_some());
        assert!(crate::cancellation::check().is_err());
    });
}
#[test]
fn finite_bytes_work_candidates_exact_boundaries_and_overflow() {
    let (s, t, d) = fixture();
    // Find the exact conservative admission boundary independently for each quota.
    for axis in 0..3 {
        let mut lo = 0;
        let mut hi = match axis {
            0 => MAX_BYTES,
            1 => MAX_WORK,
            _ => si::MAX_CANDIDATES,
        };
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let mut l = limits();
            match axis {
                0 => l.bytes = mid,
                1 => l.work = mid,
                _ => l.candidates = mid,
            };
            if s.secondary_unique_lookup_engine(&t, s.epoch, &d, &eq(), &l, || false)
                .is_ok()
            {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        let mut l = limits();
        match axis {
            0 => l.bytes = lo,
            1 => l.work = lo,
            _ => l.candidates = lo,
        };
        assert!(
            s.secondary_unique_lookup_engine(&t, s.epoch, &d, &eq(), &l, || false)
                .is_ok()
        );
        match axis {
            0 => l.bytes -= 1,
            1 => l.work -= 1,
            _ => l.candidates -= 1,
        };
        let mut calls = 0;
        assert_eq!(
            s.secondary_unique_lookup_engine(&t, s.epoch, &d, &eq(), &l, || {
                calls += 1;
                false
            }),
            Err(SqlError::Limit)
        );
        assert_eq!(calls, 0);
    }
    assert_eq!(add(usize::MAX, 1), Err(SqlError::Limit));
    assert_eq!(mul(usize::MAX, 2), Err(SqlError::Limit));
    let mut l = limits();
    l.work = usize::MAX;
    assert_eq!(
        s.secondary_unique_lookup_engine(&t, s.epoch, &d, &eq(), &l, || panic!("callback")),
        Err(SqlError::Limit)
    );
}
#[test]
fn private_forged_missing_base_multiple_and_inconsistent_payload_refuse() {
    let (s, t, d) = fixture();
    // Test-only serde permits forging without widening substrate fields.

    for mode in 0..3 {
        let mut bad = s.clone();
        if mode == 0 {
            bad.rows.get_mut(&1).unwrap().clear();
        } else {
            // bincode mirror preserves the private sidecar's existing test shape.
            type Entries = BTreeMap<u64, BTreeMap<Vec<u8>, Vec<u8>>>;
            let (ds, mut entries): (Vec<Descriptor>, Entries) =
                bincode::deserialize(&bincode::serialize(&s.secondary).unwrap()).unwrap();
            let map = entries.get_mut(&d.id).unwrap();
            let (key, pk) = map.first_key_value().unwrap();
            let mut key = key.clone();
            let mut pk = pk.clone();
            if mode == 1 {
                key.push(0);
                map.insert(key, pk);
            } else {
                pk.push(0);
                let key = key.clone();
                map.insert(key, pk);
            }
            bad.secondary =
                bincode::deserialize(&bincode::serialize(&(ds, entries)).unwrap()).unwrap();
        }
        assert!(lookup(&bad, &t, &d, &eq()).is_err());
        assert!(lookup(&bad, &t, &d, &[Value::Null, Value::Null]).is_err());
    }
}

fn mark_unique(s: &mut Relational) {
    type Entries = BTreeMap<u64, BTreeMap<Vec<u8>, Vec<u8>>>;
    let (mut ds, entries): (Vec<Descriptor>, Entries) =
        bincode::deserialize(&bincode::serialize(&s.secondary).unwrap()).unwrap();
    let len = ds.len();
    for (i, d) in ds.iter_mut().enumerate() {
        d.unique = len == 1 || i > 0;
    }
    s.secondary = bincode::deserialize(&bincode::serialize(&(ds, entries)).unwrap()).unwrap();
}

#[test]
fn key_types_are_checked_even_on_an_empty_source_and_null_probe() {
    let (mut s, t, d) = fixture();
    s.rows.get_mut(&1).unwrap().clear();
    // Empty the test mirror's maps while retaining exact descriptor identities.
    type Entries = BTreeMap<u64, BTreeMap<Vec<u8>, Vec<u8>>>;
    let (ds, mut maps): (Vec<Descriptor>, Entries) =
        bincode::deserialize(&bincode::serialize(&s.secondary).unwrap()).unwrap();
    for map in maps.values_mut() {
        map.clear();
    }
    s.secondary = bincode::deserialize(&bincode::serialize(&(ds, maps)).unwrap()).unwrap();
    assert!(lookup(&s, &t, &d, &eq()).unwrap().is_none());
    for typ in [
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
        for pos in [0, 2, 3] {
            let mut s = s.clone();
            let mut t = t.clone();
            t.columns[pos].data_type = typ.clone();
            std::sync::Arc::make_mut(&mut s.tables).insert(t.name.clone(), t.clone());
            if typ.is_temporal() && pos != 0 {
                assert_eq!(lookup(&s, &t, &d, &eq()), Err(SqlError::Type));
                assert!(lookup(&s, &t, &d, &[Value::Null, Value::Null]).unwrap().is_none());
                continue;
            }
            for e in [eq(), [Value::Null, Value::Null]] {
                let mut calls = 0;
                assert_eq!(
                    s.secondary_unique_lookup_engine(&t, s.epoch, &d, &e, &limits(), || {
                        calls += 1;
                        false
                    }),
                    Err(SqlError::Unsupported)
                );
                assert_eq!(calls, 0);
            }
        }
    }
}

#[test]
fn borrowed_source_shape_admission_and_finite_limit_overflows_precede_callback() {
    let (s, t, d) = fixture();
    for which in 0..5 {
        let mut bad = s.clone();
        match which {
            0 => std::sync::Arc::make_mut(&mut bad.tables).get_mut("things").unwrap().name = "x".repeat(64),
            1 => std::sync::Arc::make_mut(&mut bad.tables).get_mut("things").unwrap().columns[4].name = "x".repeat(64),
            2 => {
                bad.rows
                    .get_mut(&1)
                    .unwrap()
                    .values_mut()
                    .next()
                    .unwrap()
                    .values[4] = Value::Text("x".repeat(MAX_ROW_BYTES + 1))
            }
            3 => {
                bad.rows
                    .get_mut(&1)
                    .unwrap()
                    .values_mut()
                    .next()
                    .unwrap()
                    .values = vec![Value::Null; 65]
            }
            _ => {
                std::sync::Arc::make_mut(&mut bad.tables).get_mut("things").unwrap().primary_key = vec![0; 65];
            }
        }
        let mut calls = 0;
        assert_eq!(
            bad.secondary_unique_lookup_engine(&t, bad.epoch, &d, &eq(), &limits(), || {
                calls += 1;
                false
            }),
            Err(SqlError::Limit)
        );
        assert_eq!(calls, 0);
    }
    for axis in 0..3 {
        let mut l = limits();
        match axis {
            0 => l.bytes = usize::MAX,
            1 => l.work = usize::MAX,
            _ => l.candidates = usize::MAX,
        };
        assert_eq!(
            s.secondary_unique_lookup_engine(&t, s.epoch, &d, &eq(), &l, || panic!(
                "callback before admission"
            )),
            Err(SqlError::Limit)
        );
    }
}
