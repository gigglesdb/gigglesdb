use super::*;

fn sql(s: &mut Relational, text: &str, params: &[Value]) -> R<SqlResult> {
    let p = Prepared::parse(text, 1, s.epoch)?;
    s.execute(&p, params, None)
}
fn base() -> Relational {
    let mut s = Relational::default();
    sql(
        &mut s,
        "CREATE TABLE items (id BIGINT PRIMARY KEY, tag TEXT, payload BYTEA)",
        &[],
    )
    .unwrap();
    s
}
fn configure(s: &mut Relational) -> u64 {
    let t = s.tables["items"].clone();
    s.secondary_configure_test(t.id, "by_tag", &[t.columns[1].id], || false)
        .unwrap()
}
fn entries(s: &Relational, index: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
    s.secondary
        .lookup(index, None, None, || false)
        .unwrap()
        .into_iter()
        .map(|(a, b)| (a.to_vec(), b.to_vec()))
        .collect()
}
fn insert(s: &mut Relational, id: i64, tag: Value) {
    sql(
        s,
        "INSERT INTO items VALUES ($1,$2,$3)",
        &[Value::Int(id), tag, Value::Bytes(vec![])],
    )
    .unwrap();
}
#[test]
fn sql_native_insert_update_delete_and_null_ties() {
    let mut s = base();
    let index = configure(&mut s);
    let epoch = s.epoch;
    insert(&mut s, 2, Value::Null);
    let t = s.tables["items"].clone();
    let p = Prepared::insert_engine(
        &t,
        vec![Value::Int(1), Value::Null, Value::Bytes(vec![])],
        1,
        &s,
    )
    .unwrap();
    with_sql_parser_forbidden(|| s.execute(&p, &[], None)).unwrap();
    let pk1 = encode_key(&[Value::Int(1)]).unwrap();
    let pk2 = encode_key(&[Value::Int(2)]).unwrap();
    assert_eq!(
        entries(&s, index),
        vec![
            ([vec![0], pk1.clone()].concat(), pk1.clone()),
            ([vec![0], pk2.clone()].concat(), pk2.clone())
        ]
    );
    let version = s.row_version_engine(&t, &[Value::Int(1)]).unwrap();
    let p = Prepared::update_engine(
        &t,
        vec![Value::Int(1)],
        vec![(1, Value::Text("a\0b".into()))],
        1,
        &s,
    )
    .unwrap();
    with_sql_parser_forbidden(|| s.execute(&p, &[], Some(version))).unwrap();
    assert_eq!(
        entries(&s, index)[1].0,
        [encode_key(&[Value::Text("a\0b".into())]).unwrap(), pk1].concat()
    );
    sql(&mut s, "DELETE FROM items WHERE id=2", &[]).unwrap();
    let p = Prepared::delete_engine(&t, vec![Value::Int(1)], 1, &s).unwrap();
    with_sql_parser_forbidden(|| s.execute(&p, &[], None)).unwrap();
    assert!(entries(&s, index).is_empty());
    assert_eq!(s.epoch, epoch);
    s.secondary_validate_engine(|| false).unwrap();
}
#[test]
fn errors_and_noops_leave_rows_counters_and_sidecar_exact() {
    let mut s = base();
    configure(&mut s);
    insert(&mut s, 1, Value::Text("a".into()));
    let before = s.clone();
    assert_eq!(
        sql(&mut s, "INSERT INTO items VALUES (1,'x',NULL)", &[]),
        Err(SqlError::Duplicate)
    );
    assert_eq!(s, before);
    assert_eq!(
        sql(&mut s, "UPDATE items SET tag=2 WHERE id=1", &[]),
        Err(SqlError::Type)
    );
    assert_eq!(s, before);
    let p = Prepared::parse("UPDATE items SET tag='b' WHERE id=1", 1, s.epoch).unwrap();
    assert_eq!(s.execute(&p, &[], Some(u64::MAX)), Err(SqlError::Version));
    assert_eq!(s, before);
    assert_eq!(
        sql(&mut s, "UPDATE items SET tag='b' WHERE id=99", &[])
            .unwrap()
            .affected,
        0
    );
    assert_eq!(
        sql(&mut s, "DELETE FROM items WHERE id=99", &[])
            .unwrap()
            .affected,
        0
    );
    assert_eq!(s, before);
    assert_eq!(
        sql(&mut s, "UPDATE items SET id=2 WHERE id=1", &[]),
        Err(SqlError::Unsupported)
    );
    assert_eq!(s, before);
}
#[test]
fn helper_update_size_and_version_failure_roll_back_without_outer_transaction() {
    for indexed in [false, true] {
        let mut s = base();
        if indexed {
            configure(&mut s);
        }
        insert(&mut s, 1, Value::Text("a".repeat(40_000)));
        let before = s.clone();
        assert_eq!(
            sql(
                &mut s,
                "UPDATE items SET payload=$1 WHERE id=1",
                &[Value::Bytes(vec![1; 40_000])]
            ),
            Err(SqlError::Limit)
        );
        assert_eq!(s, before);
        s.next_version = u64::MAX;
        let before = s.clone();
        assert_eq!(
            sql(&mut s, "UPDATE items SET tag='changed' WHERE id=1", &[]),
            Err(SqlError::Overflow)
        );
        assert_eq!(s, before);
    }
}
#[test]
fn create_counter_overflow_is_atomic() {
    let mut s = Relational {
        next_id: u64::MAX - 1,
        ..Relational::default()
    };
    let before = s.clone();
    assert_eq!(
        sql(
            &mut s,
            "CREATE TABLE a (id BIGINT PRIMARY KEY, tag TEXT)",
            &[]
        ),
        Err(SqlError::Overflow)
    );
    assert_eq!(s, before);
}
#[test]
fn borrowed_witness_admission_mismatch_and_immutable_pk() {
    let mut s = base();
    configure(&mut s);
    insert(&mut s, 1, Value::Text("a".into()));
    let t = s.tables["items"].clone();
    let key = encode_key(&[Value::Int(1)]).unwrap();
    let actual = s.rows[&t.id][&key].clone();
    let mut wrong = actual.clone();
    wrong.values[1] = Value::Text("wrong".into());
    let before = s.clone();
    assert_eq!(
        s.secondary_stage_change_with(t.id, Some((&key, &wrong)), None, &mut || false),
        Err(SqlError::Conflict)
    );
    let huge = vec![0; si::MAX_COMPONENT + 1];
    assert_eq!(
        s.secondary_stage_change_with(t.id, Some((&huge, &actual)), None, &mut || true),
        Err(SqlError::Limit)
    );
    let mut replacement = actual.clone();
    replacement.values[0] = Value::Int(2);
    let next = encode_key(&[Value::Int(2)]).unwrap();
    assert_eq!(
        s.secondary_stage_change_with(
            t.id,
            Some((&key, &actual)),
            Some((&next, &replacement)),
            &mut || false
        ),
        Err(SqlError::Unsupported)
    );
    assert_eq!(s, before);
}
#[test]
fn disjoint_base_drift_cannot_publish_a_sidecar_change() {
    let mut s = base();
    configure(&mut s);
    insert(&mut s, 1, Value::Text("a".into()));
    insert(&mut s, 2, Value::Text("b".into()));
    let t = s.tables["items"].clone();
    let key2 = encode_key(&[Value::Int(2)]).unwrap();
    s.rows
        .get_mut(&t.id)
        .unwrap()
        .get_mut(&key2)
        .unwrap()
        .values[1] = Value::Text("drift".into());
    let before = s.clone();
    assert!(sql(&mut s, "UPDATE items SET tag='x' WHERE id=1", &[]).is_err());
    assert_eq!(s, before);
}
#[test]
fn cancellation_early_late_and_current_scope_publish_nothing() {
    let mut s = base();
    configure(&mut s);
    insert(&mut s, 1, Value::Text("a".into()));
    let t = s.tables["items"].clone();
    let key = encode_key(&[Value::Int(1)]).unwrap();
    let old = s.rows[&t.id][&key].clone();
    let mut new = old.clone();
    new.values[1] = Value::Text("b".into());
    let mut count = 0;
    s.secondary_stage_change_with(t.id, Some((&key, &old)), Some((&key, &new)), &mut || {
        count += 1;
        false
    })
    .unwrap();
    assert!(count > 10);
    let before = s.clone();
    for stop in [1, count / 2, count] {
        let mut seen = 0;
        assert_eq!(
            s.secondary_stage_change_with(
                t.id,
                Some((&key, &old)),
                Some((&key, &new)),
                &mut || {
                    seen += 1;
                    seen >= stop
                }
            ),
            Err(SqlError::Aborted)
        );
        assert_eq!(s, before);
    }
    let cancel = crate::Cancellation::new(None);
    cancel.cancel();
    assert_eq!(
        cancel.scope(|| sql(&mut s, "DELETE FROM items WHERE id=1", &[])),
        Err(SqlError::Aborted)
    );
    assert_eq!(s, before);
}
#[test]
fn typed_index_and_typed_primary_keys_refused_even_empty() {
    for ty in ["NUMERIC", "REAL", "DOUBLE PRECISION"] {
        for pk in [false, true] {
            let mut s = Relational::default();
            let create = if pk {
                format!("CREATE TABLE a (id {ty} PRIMARY KEY, tag TEXT)")
            } else {
                format!("CREATE TABLE a (id BIGINT PRIMARY KEY, tag {ty})")
            };
            sql(&mut s, &create, &[]).unwrap();
            let t = s.tables["a"].clone();
            let before = s.clone();
            assert_eq!(
                s.secondary_configure_test(t.id, "by_tag", &[t.columns[1].id], || false),
                Err(SqlError::Unsupported)
            );
            assert_eq!(s, before);
        }
    }
}
#[test]
fn full_snapshot_exact_row_cap_not_a_select_page() {
    let mut s = base();
    for id in 0..si::MAX_ROWS {
        insert(&mut s, id as i64, Value::Null);
    }
    let mut too_many = s.clone();
    insert(&mut too_many, si::MAX_ROWS as i64, Value::Null);
    let t = too_many.tables["items"].clone();
    let before = too_many.clone();
    assert_eq!(
        too_many.secondary_configure_test(t.id, "by_tag", &[t.columns[1].id], || false),
        Err(SqlError::Limit)
    );
    assert_eq!(too_many, before);
    configure(&mut s);
    let before = s.clone();
    assert_eq!(
        sql(&mut s, "INSERT INTO items VALUES (1024,NULL,NULL)", &[]),
        Err(SqlError::Limit)
    );
    assert_eq!(s, before);
    s.secondary_validate_engine(|| false).unwrap();
}
#[test]
fn canonical_six_types_binary_null_and_complete_pk_suffix() {
    let mut s = Relational::default();
    sql(&mut s,"CREATE TABLE a (id BIGINT PRIMARY KEY, b BOOLEAN, s SMALLINT, i INTEGER, n BIGINT, t TEXT, x BYTEA)",&[]).unwrap();
    let t = s.tables["a"].clone();
    let cols = t.columns.iter().skip(1).map(|c| c.id).collect::<Vec<_>>();
    let index = s
        .secondary_configure_test(t.id, "all_types", &cols, || false)
        .unwrap();
    let values = vec![
        Value::Int(i64::MIN),
        Value::Bool(false),
        Value::Int(i16::MIN as i64),
        Value::Int(i32::MAX as i64),
        Value::Int(i64::MAX),
        Value::Text("\0Ã©\0".into()),
        Value::Bytes(vec![0, 255, 0]),
    ];
    let p = Prepared::insert_engine(&t, values.clone(), 1, &s).unwrap();
    s.execute(&p, &[], None).unwrap();
    let pk = encode_key(&values[..1]).unwrap();
    assert_eq!(
        entries(&s, index),
        vec![([encode_key(&values[1..]).unwrap(), pk.clone()].concat(), pk)]
    );
}
#[test]
fn combined_arity_128_and_zero_escaped_boundary() {
    let mut s = Relational::default();
    let columns = (0..64)
        .map(|i| ColumnDef {
            id: 0,
            name: format!("c{i}"),
            data_type: DataType::BigInt,
            not_null: true,
        })
        .collect::<Vec<_>>();
    let keys = columns.iter().map(|c| c.name.clone()).collect();
    let p = Prepared::create_engine("wide".into(), columns, keys, 1, &s).unwrap();
    s.execute(&p, &[], None).unwrap();
    let t = s.tables["wide"].clone();
    let cols = t.columns.iter().map(|c| c.id).collect::<Vec<_>>();
    let index = s
        .secondary_configure_test(t.id, "wide_index", &cols, || false)
        .unwrap();
    let values = vec![Value::Int(0); 64];
    let p = Prepared::insert_engine(&t, values.clone(), 1, &s).unwrap();
    s.execute(&p, &[], None).unwrap();
    let key = encode_key(&values).unwrap();
    assert_eq!(
        entries(&s, index),
        vec![([key.clone(), key.clone()].concat(), key)]
    );
    let mut s = Relational::default();
    sql(&mut s, "CREATE TABLE b (id BYTEA PRIMARY KEY)", &[]).unwrap();
    let t = s.tables["b"].clone();
    let index = s
        .secondary_configure_test(t.id, "zero_index", &[t.columns[0].id], || false)
        .unwrap();
    let values = vec![Value::Bytes(vec![0; MAX_ROW_BYTES])];
    let p = Prepared::insert_engine(&t, values.clone(), 1, &s).unwrap();
    s.execute(&p, &[], None).unwrap();
    let key = encode_key(&values).unwrap();
    assert_eq!(
        entries(&s, index),
        vec![([key.clone(), key.clone()].concat(), key)]
    );
}
#[test]
fn descriptor_limits_and_epoch_fences_are_atomic() {
    let mut s = base();
    let t = s.tables["items"].clone();
    let stale = Prepared::parse("INSERT INTO items VALUES(1,NULL,NULL)", 1, s.epoch).unwrap();
    for n in 0..si::MAX_INDEXES_PER_TABLE {
        s.secondary_configure_test(t.id, &format!("idx{n}"), &[t.columns[1].id], || false)
            .unwrap();
    }
    let before = s.clone();
    assert_eq!(
        s.secondary_configure_test(t.id, "extra", &[t.columns[1].id], || false),
        Err(SqlError::Limit)
    );
    assert_eq!(s, before);
    assert_eq!(s.execute(&stale, &[], None), Err(SqlError::StaleSchema));
    assert!(matches!(
        s.secondary_access_engine(&t, s.epoch - 1, || false),
        Err(SqlError::StaleSchema)
    ));
    let mut wrong = t.clone();
    wrong.columns[1].id += 1;
    assert!(matches!(
        s.secondary_access_engine(&wrong, s.epoch, || false),
        Err(SqlError::StaleSchema)
    ));
    let (_, rows) = s.secondary_access_engine(&t, s.epoch, || false).unwrap();
    assert!(rows.is_empty());
}
#[test]
fn legacy_bincode_exact_and_runtime_persistence_fails_closed() {
    #[derive(Serialize)]
    struct Legacy<'a> {
        format: u32,
        generation: u64,
        epoch: u64,
        next_id: u64,
        next_version: u64,
        tables: &'a BTreeMap<String, TableDef>,
        rows: &'a BTreeMap<u64, BTreeMap<Vec<u8>, SqlRow>>,
    }
    let mut s = base();
    insert(&mut s, 1, Value::Null);
    let legacy = Legacy {
        format: s.format,
        generation: s.generation,
        epoch: s.epoch,
        next_id: s.next_id,
        next_version: s.next_version,
        tables: &s.tables,
        rows: &s.rows,
    };
    let bytes = bincode::serialize(&s).unwrap();
    assert_eq!(bytes, bincode::serialize(&legacy).unwrap());
    let decoded: Relational = bincode::deserialize(&bytes).unwrap();
    assert_eq!(s, decoded);
    configure(&mut s);
    assert!(bincode::serialize(&s).is_err());
    // Empty indexed tables still require indexed persistence features.
    let mut empty = base();
    configure(&mut empty);
    assert!(bincode::serialize(&empty).is_err());
}

#[test]
fn candidate_cap_replacement_and_delete_remain_legal() {
    let mut s = base();
    for id in 0..512 {
        insert(&mut s, id, Value::Null);
    }
    let t = s.tables["items"].clone();
    for i in 0..8 {
        s.secondary_configure_test(t.id, &format!("idx{i}"), &[t.columns[1].id], || false)
            .unwrap();
    }
    // 512 x 8 = the exact 4096 candidate quota, not 4096 + 8 on UPDATE.
    assert_eq!(
        sql(&mut s, "UPDATE items SET tag='x' WHERE id=1", &[])
            .unwrap()
            .affected,
        1
    );
    let before = s.clone();
    assert_eq!(
        sql(&mut s, "INSERT INTO items VALUES(513,NULL,NULL)", &[]),
        Err(SqlError::Limit)
    );
    assert_eq!(s, before);
    assert_eq!(
        sql(&mut s, "DELETE FROM items WHERE id=1", &[])
            .unwrap()
            .affected,
        1
    );
    s.secondary_validate_engine(|| false).unwrap();
}
#[test]
fn typed_nonkey_payload_is_preserved_and_catalog_and_key_corruption_refuse() {
    let mut s = Relational::default();
    sql(
        &mut s,
        "CREATE TABLE a (id BIGINT PRIMARY KEY, tag TEXT, n NUMERIC)",
        &[],
    )
    .unwrap();
    let t = s.tables["a"].clone();
    let index = s
        .secondary_configure_test(t.id, "tag_index", &[t.columns[1].id], || false)
        .unwrap();
    let p = Prepared::insert_engine(
        &t,
        vec![
            Value::Int(1),
            Value::Null,
            Value::Numeric(Numeric::parse("1.2300").unwrap()),
        ],
        1,
        &s,
    )
    .unwrap();
    s.execute(&p, &[], None).unwrap();
    assert_eq!(entries(&s, index).len(), 1);
    s.validate().unwrap();
    // Same-length but noncanonical base key is not trusted as an encoded PK.
    let key = encode_key(&[Value::Int(1)]).unwrap();
    let row = s.rows.get_mut(&t.id).unwrap().remove(&key).unwrap();
    let wrong = encode_key(&[Value::Int(2)]).unwrap();
    s.rows.get_mut(&t.id).unwrap().insert(wrong, row);
    assert_eq!(s.secondary_validate_engine(|| false), Err(SqlError::Type));
}

#[test]
fn snapshot_byte_and_work_budgets_reject_before_materialization() {
    let mut s = base();
    for id in 0..128 {
        insert(&mut s, id, Value::Text("a".repeat(65_500)));
    }
    let t = s.tables["items"].clone();
    let ds = (0..3)
        .map(|i| Descriptor {
            id: s.next_id + i,
            table: t.id,
            name: format!("idx{i}"),
            columns: vec![t.columns[1].id],
            key_encoding: 1,
            unique: false,
        })
        .collect::<Vec<_>>();
    // One 8 MiB snapshot fits; three indexes exceed 16 MiB independently of
    // the row/candidate caps. No canonical snapshot allocation is needed.
    admit(&s, &ds[..1], NONE, &mut || false).unwrap();
    assert_eq!(admit(&s, &ds, NONE, &mut || false), Err(SqlError::Limit));
    let mut s = Relational::default();
    sql(
        &mut s,
        "CREATE TABLE a (id TEXT PRIMARY KEY, tag BIGINT)",
        &[],
    )
    .unwrap();
    let t = s.tables["a"].clone();
    for id in 0..800 {
        let value = Value::Text(format!("{id:04}{}", "x".repeat(2044)));
        let p = Prepared::insert_engine(&t, vec![value, Value::Int(0)], 1, &s).unwrap();
        s.execute(&p, &[], None).unwrap();
    }
    let ds = [Descriptor {
        id: s.next_id,
        table: t.id,
        name: "idx".into(),
        columns: vec![t.columns[1].id],
        key_encoding: 1,
        unique: false,
    }];
    // Only ~3.3 MiB, 800 rows and 800 candidates, but quadratic identity
    // comparison work on the 2048-byte PK exceeds the finite work budget.
    assert_eq!(admit(&s, &ds, NONE, &mut || false), Err(SqlError::Limit));
}

fn unique_base() -> Relational {
    let mut s = base();
    let t = s.tables["items"].clone();
    s.create_secondary_index_internal_engine(
        &t,
        s.epoch,
        "unique_tag",
        &[t.columns[1].id],
        true,
        || false,
    )
    .unwrap();
    s
}
#[test]
fn unique_direct_crud_null_release_and_failure_atomicity() {
    let mut s = unique_base();
    insert(&mut s, 1, Value::Text("a".into()));
    let before = s.clone();
    assert_eq!(
        sql(&mut s, "INSERT INTO items VALUES (2,'a',NULL)", &[]),
        Err(SqlError::Duplicate)
    );
    assert_eq!(s, before);
    insert(&mut s, 2, Value::Null);
    insert(&mut s, 3, Value::Null);
    let before = s.clone();
    assert_eq!(
        sql(&mut s, "UPDATE items SET tag='a' WHERE id=2", &[]),
        Err(SqlError::Duplicate)
    );
    assert_eq!(s, before);
    sql(&mut s, "DELETE FROM items WHERE id=1", &[]).unwrap();
    sql(&mut s, "UPDATE items SET tag='a' WHERE id=2", &[]).unwrap();
    s.secondary_validate_engine(|| false).unwrap();
}
fn batch_rows(s: &Relational) -> (u64, Vec<Vec<u8>>, Vec<SqlRow>) {
    let id = s.tables["items"].id;
    let (keys, rows) = s.rows[&id]
        .iter()
        .map(|(k, r)| (k.clone(), r.clone()))
        .unzip();
    (id, keys, rows)
}
#[test]
fn unique_real_batch_swap_survivor_duplicate_stale_and_immutable() {
    let mut s = unique_base();
    insert(&mut s, 1, Value::Text("a".into()));
    insert(&mut s, 2, Value::Text("b".into()));
    insert(&mut s, 3, Value::Text("c".into()));
    let (id, keys, old) = batch_rows(&s);
    let mut new = old.clone();
    new[0].values[1] = Value::Text("b".into());
    new[1].values[1] = Value::Text("a".into());
    let next = s.next_version;
    new[0].version = next;
    new[1].version = next + 1;
    let changes = [
        RowChange {
            table: id,
            old: Some((&keys[0], &old[0])),
            new: Some((&keys[0], &new[0])),
        },
        RowChange {
            table: id,
            old: Some((&keys[1], &old[1])),
            new: Some((&keys[1], &new[1])),
        },
    ];
    let mut calls = 0;
    let mut probe = s.clone();
    probe
        .secondary_apply_statement_with(
            StatementProposal {
                changes: &changes,
                next_version: next + 2,
            },
            || {
                calls += 1;
                false
            },
        )
        .unwrap();
    assert_eq!(probe.rows[&id][&keys[0]].values[1], Value::Text("b".into()));
    for stop in 1..=calls {
        let mut candidate = s.clone();
        let mut n = 0;
        assert_eq!(
            candidate.secondary_apply_statement_with(
                StatementProposal {
                    changes: &changes,
                    next_version: next + 2
                },
                || {
                    n += 1;
                    n == stop
                }
            ),
            Err(SqlError::Aborted)
        );
        assert_eq!(candidate, s, "cancel {stop}");
    }
    s.secondary_apply_statement_with(
        StatementProposal {
            changes: &changes,
            next_version: next + 2,
        },
        || false,
    )
    .unwrap();
    let before = s.clone();
    // Exact stale raw witness, despite unchanged canonical PK.
    assert_eq!(
        s.secondary_apply_statement_with(
            StatementProposal {
                changes: &changes,
                next_version: next + 2
            },
            || false
        ),
        Err(SqlError::Conflict)
    );
    assert_eq!(s, before);
    let (id, keys, old) = batch_rows(&s);
    let mut new = old[0].clone();
    new.values[1] = Value::Text("c".into());
    new.version = s.next_version;
    let next = s.next_version + 1;
    let c = RowChange {
        table: id,
        old: Some((&keys[0], &old[0])),
        new: Some((&keys[0], &new)),
    };
    assert_eq!(
        s.secondary_apply_statement_with(
            StatementProposal {
                changes: &[c],
                next_version: next
            },
            || false
        ),
        Err(SqlError::Duplicate)
    );
    assert_eq!(s, before);
    let changes = [
        RowChange {
            table: id,
            old: Some((&keys[0], &old[0])),
            new: None,
        },
        RowChange {
            table: id,
            old: Some((&keys[0], &old[0])),
            new: None,
        },
    ];
    assert_eq!(
        s.secondary_apply_statement_with(
            StatementProposal {
                changes: &changes,
                next_version: next
            },
            || false
        ),
        Err(SqlError::Duplicate)
    );
    let changes = [RowChange {
        table: id,
        old: Some((&keys[0], &old[0])),
        new: Some((&keys[1], &new)),
    }];
    assert_eq!(
        s.secondary_apply_statement_with(
            StatementProposal {
                changes: &changes,
                next_version: next
            },
            || false
        ),
        Err(SqlError::Unsupported)
    );
    assert_eq!(s, before);
}
#[test]
fn unique_composite_null_each_position_and_new_batch_conflicts() {
    let mut s = base();
    let t = s.tables["items"].clone();
    s.create_secondary_index_internal_engine(
        &t,
        s.epoch,
        "composite",
        &[t.columns[1].id, t.columns[2].id],
        true,
        || false,
    )
    .unwrap();
    for statement in [
        "INSERT INTO items VALUES (1,NULL,NULL)",
        "INSERT INTO items VALUES (2,NULL,NULL)",
        "INSERT INTO items VALUES (3,'x',NULL)",
        "INSERT INTO items VALUES (4,'x',NULL)",
    ] {
        sql(&mut s, statement, &[]).unwrap();
    }
    let (id, keys, old) = batch_rows(&s);
    let next = s.next_version;
    let mut new = old.clone();
    for (i, row) in new[..2].iter_mut().enumerate() {
        row.values[1] = Value::Text("x".into());
        row.values[2] = Value::Bytes(vec![0]);
        row.version = next + i as u64;
    }
    let changes = [
        RowChange {
            table: id,
            old: Some((&keys[0], &old[0])),
            new: Some((&keys[0], &new[0])),
        },
        RowChange {
            table: id,
            old: Some((&keys[1], &old[1])),
            new: Some((&keys[1], &new[1])),
        },
    ];
    let before = s.clone();
    assert_eq!(
        s.secondary_apply_statement_with(
            StatementProposal {
                changes: &changes,
                next_version: next + 2
            },
            || false
        ),
        Err(SqlError::Duplicate)
    );
    assert_eq!(s, before);
    // First component NULL with a nonNULL second component also stays distinct.
    for row in &mut new[..2] {
        row.values[1] = Value::Null;
    }
    let changes = [
        RowChange {
            table: id,
            old: Some((&keys[0], &old[0])),
            new: Some((&keys[0], &new[0])),
        },
        RowChange {
            table: id,
            old: Some((&keys[1], &old[1])),
            new: Some((&keys[1], &new[1])),
        },
    ];
    s.secondary_apply_statement_with(
        StatementProposal {
            changes: &changes,
            next_version: next + 2,
        },
        || false,
    )
    .unwrap();
}
#[test]
fn unique_whole_proposal_shape_before_stale_comparison_and_noop_cancel() {
    let mut s = unique_base();
    insert(&mut s, 1, Value::Null);
    let (id, keys, old) = batch_rows(&s);
    let mut stale = old[0].clone();
    stale.version += 1;
    let mut huge = old[0].clone();
    huge.values[1] = Value::Text("x".repeat(MAX_ROW_BYTES + 1));
    let changes = [
        RowChange {
            table: id,
            old: Some((&keys[0], &stale)),
            new: None,
        },
        RowChange {
            table: id,
            old: None,
            new: Some((&keys[0], &huge)),
        },
    ];
    let before = s.clone();
    let next = s.next_version;
    assert_eq!(
        s.secondary_apply_statement_with(
            StatementProposal {
                changes: &changes,
                next_version: next
            },
            || false
        ),
        Err(SqlError::Limit)
    );
    assert_eq!(s, before);
    assert_eq!(
        s.secondary_apply_statement_with(
            StatementProposal {
                changes: &[],
                next_version: next
            },
            || true
        ),
        Err(SqlError::Aborted)
    );
    assert_eq!(s, before);
}

#[test]
fn unique_batch_count_exact_limit_and_postimage_serialization_identity() {
    let mut s = unique_base();
    let id = s.tables["items"].id;
    let next = s.next_version;
    let changes = (0..MAX_ROWS)
        .map(|_| RowChange {
            table: id,
            old: None,
            new: None,
        })
        .collect::<Vec<_>>();
    let staged = s
        .stage_statement_with(
            StatementProposal {
                changes: &changes,
                next_version: next,
            },
            &mut || false,
        )
        .unwrap();
    let before = bincode::serialize(&StatementNamespace5 {
        source: &s,
        replacements: &staged.rows,
        next_version: next,
    })
    .unwrap();
    assert_eq!(before, s.secondary_persistence_engine(|| false).unwrap().0);
    assert_eq!(
        s.secondary_apply_statement_with(
            StatementProposal {
                changes: &changes,
                next_version: next
            },
            || false
        )
        .unwrap()
        .affected,
        0
    );
    let mut over = changes;
    over.push(RowChange {
        table: id,
        old: None,
        new: None,
    });
    let old = s.clone();
    assert_eq!(
        s.secondary_apply_statement_with(
            StatementProposal {
                changes: &over,
                next_version: next
            },
            || false
        ),
        Err(SqlError::Limit)
    );
    assert_eq!(s, old);
}

#[test]
fn unique_typed_nonkeys_lossless_and_raw_numeric_scale_witness() {
    let mut s = Relational::default();
    sql(
        &mut s,
        "CREATE TABLE items (id BIGINT PRIMARY KEY, tag TEXT, n NUMERIC, m NUMERIC)",
        &[],
    )
    .unwrap();
    let t = s.tables["items"].clone();
    s.create_secondary_index_internal_engine(&t, s.epoch, "u", &[t.columns[1].id], true, || false)
        .unwrap();
    sql(&mut s, "INSERT INTO items VALUES (1,'a',1.00,2.000)", &[]).unwrap();
    sql(&mut s, "UPDATE items SET tag='b' WHERE id=1", &[]).unwrap();
    let (id, keys, old) = batch_rows(&s);
    let mut stale = old[0].clone();
    stale.values[2] = Value::Numeric(Numeric::parse("1.0").unwrap());
    assert_eq!(stale, old[0]); // Numeric value equality is deliberately insufficient.
    let before = s.clone();
    let next = s.next_version;
    assert_eq!(
        s.secondary_apply_statement_with(
            StatementProposal {
                changes: &[RowChange {
                    table: id,
                    old: Some((&keys[0], &stale)),
                    new: None
                }],
                next_version: next
            },
            || false
        ),
        Err(SqlError::Conflict)
    );
    assert_eq!(s, before);
    assert_eq!(
        s.rows[&id][&keys[0]].values[2],
        Value::Numeric(Numeric::parse("1.00").unwrap())
    );
    let (base, payload) = s.secondary_persistence_engine(|| false).unwrap();
    let mut restored: Relational = bincode::deserialize(&base).unwrap();
    restored
        .secondary_restore_engine(
            si::codec::decode_pending(&payload, || false).unwrap(),
            || false,
        )
        .unwrap();
    assert_eq!(
        restored.secondary_persistence_engine(|| false).unwrap(),
        (base, payload)
    );
}

#[test]
fn statement_namespace_follows_catalog_extension_wire_shape() {
    use bincode::Options;
    let mut state = base();
    insert(&mut state, 1, Value::Text("one".into()));
    let column = state.tables["items"].columns[1].id;
    for default in [None, Some("'default'".into())] {
        state.set_column_default(column, default).unwrap();
        let replacements = BTreeMap::new();
        let image = StatementNamespace5 {
            source: &state,
            replacements: &replacements,
            next_version: state.next_version,
        };
        assert_eq!(
            bincode::serialize(&image).unwrap(),
            bincode::serialize(&LegacyNamespace5(&state)).unwrap()
        );
        assert_eq!(
            bincode::serialize(&image).unwrap(),
            bincode::serialize(&state).unwrap()
        );
        let codec = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_big_endian()
            .reject_trailing_bytes();
        assert_eq!(
            codec.serialize(&image).unwrap(),
            codec.serialize(&LegacyNamespace5(&state)).unwrap()
        );
        assert_eq!(
            codec.serialize(&image).unwrap(),
            codec.serialize(&state).unwrap()
        );
    }
}
