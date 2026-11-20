use super::*;

fn sql(s: &mut Relational, text: &str, params: &[Value]) -> R<SqlResult> {
    let p = Prepared::parse(text, 1, s.epoch)?;
    s.execute(&p, params, None)
}
fn setup() -> (Relational, TableDef) {
    let mut s = Relational::default();
    sql(&mut s, "CREATE TABLE items (a BIGINT, b TEXT, tag BYTEA, score SMALLINT, payload TEXT, PRIMARY KEY (a,b))", &[]).unwrap();
    let t = s.tables["items"].clone();
    s.secondary_configure_test(
        t.id,
        "by_tag_score",
        &[t.columns[2].id, t.columns[3].id],
        || false,
    )
    .unwrap();
    (s, t)
}
fn insert(s: &mut Relational, a: i64, b: &str, tag: Value, score: Value, payload: &str) {
    sql(
        s,
        "INSERT INTO items VALUES ($1,$2,$3,$4,$5)",
        &[
            Value::Int(a),
            Value::Text(b.into()),
            tag,
            score,
            Value::Text(payload.into()),
        ],
    )
    .unwrap();
}
fn req<'a>(equal: &'a [(u64, Value)], projection: &'a [u64]) -> Request<'a> {
    Request {
        equal,
        range: None,
        projection,
        descending: false,
        residuals: &[],
        max_rows: MAX_ROWS,
        max_bytes: MAX_RESULT_BYTES,
    }
}
fn read(s: &Relational, t: &TableDef, r: &Request<'_>) -> R<SqlResult> {
    with_sql_parser_forbidden(|| s.secondary_read_engine(t, s.epoch, r, || false))
}
fn ids(result: SqlResult) -> Vec<i64> {
    result
        .rows
        .iter()
        .map(|r| match r.values[0] {
            Value::Int(n) => n,
            _ => panic!(),
        })
        .collect()
}
#[test]
fn actual_composite_prefix_binary_null_ties_and_base_versions() {
    let (mut s, t) = setup();
    insert(
        &mut s,
        3,
        "z",
        Value::Bytes(vec![0, 255]),
        Value::Int(7),
        "three",
    );
    insert(
        &mut s,
        1,
        "z",
        Value::Bytes(vec![0, 255]),
        Value::Int(7),
        "one-z",
    );
    insert(
        &mut s,
        1,
        "a\0",
        Value::Bytes(vec![0, 255]),
        Value::Int(7),
        "one-a",
    );
    insert(
        &mut s,
        8,
        "",
        Value::Bytes(vec![0, 255, 0]),
        Value::Int(7),
        "outside",
    );
    insert(&mut s, 9, "", Value::Null, Value::Null, "null");
    let p = [t.columns[0].id, t.columns[4].id, t.columns[4].id];
    let eq = [(t.columns[2].id, Value::Bytes(vec![0, 255]))];
    let result = read(&s, &t, &req(&eq, &p)).unwrap();
    assert_eq!(ids(result.clone()), vec![1, 1, 3]);
    assert_eq!(result.rows[0].values[1], Value::Text("one-a".into()));
    for row in &result.rows {
        assert_eq!(row.values[1], row.values[2]);
        assert!(
            s.rows[&t.id]
                .values()
                .any(|base| base.version == row.version && base.values[4] == row.values[1])
        );
    }
    let eq = [(t.columns[2].id, Value::Null)];
    assert_eq!(ids(read(&s, &t, &req(&eq, &p)).unwrap()), vec![9]);
}
#[test]
fn every_range_inclusive_exclusive_and_full_pk_tie_boundary() {
    let (mut s, t) = setup();
    for (id, score) in [(1, -1), (2, 0), (3, 0), (4, 1), (5, 2)] {
        insert(&mut s, id, "", Value::Bytes(vec![]), Value::Int(score), "");
    }
    insert(&mut s, 6, "", Value::Bytes(vec![]), Value::Null, "");
    insert(&mut s, 7, "", Value::Bytes(vec![0]), Value::Int(0), "");
    let eq = [(t.columns[2].id, Value::Bytes(vec![]))];
    let p = [t.columns[0].id];
    let lo = Value::Int(0);
    let hi = Value::Int(1);
    for (li, ui, want) in [
        (true, true, vec![2, 3, 4]),
        (true, false, vec![2, 3]),
        (false, true, vec![4]),
        (false, false, vec![]),
    ] {
        let mut r = req(&eq, &p);
        r.range = Some(Range {
            column: t.columns[3].id,
            lower: Some(Endpoint {
                value: &lo,
                inclusive: li,
            }),
            upper: Some(Endpoint {
                value: &hi,
                inclusive: ui,
            }),
        });
        assert_eq!(ids(read(&s, &t, &r).unwrap()), want);
    }
    let null = Value::Null;
    let mut r = req(&eq, &p);
    r.range = Some(Range {
        column: t.columns[3].id,
        lower: Some(Endpoint {
            value: &null,
            inclusive: false,
        }),
        upper: Some(Endpoint {
            value: &lo,
            inclusive: true,
        }),
    });
    assert_eq!(ids(read(&s, &t, &r).unwrap()), vec![1, 2, 3]);
    r.range.as_mut().unwrap().lower = Some(Endpoint {
        value: &hi,
        inclusive: true,
    });
    r.range.as_mut().unwrap().upper = Some(Endpoint {
        value: &lo,
        inclusive: true,
    });
    assert!(read(&s, &t, &r).unwrap().rows.is_empty());
}
#[test]
fn unprefixed_first_range_and_exhausted_scalar_carry() {
    let (mut s, t) = setup();
    insert(
        &mut s,
        1,
        "",
        Value::Bytes(vec![1]),
        Value::Int(i16::MAX.into()),
        "",
    );
    insert(&mut s, 2, "", Value::Bytes(vec![1, 0]), Value::Int(0), "");
    let eq = [(t.columns[2].id, Value::Bytes(vec![1]))];
    let p = [t.columns[0].id];
    let max = Value::Int(i16::MAX.into());
    let mut r = req(&eq, &p);
    r.range = Some(Range {
        column: t.columns[3].id,
        lower: Some(Endpoint {
            value: &max,
            inclusive: true,
        }),
        upper: Some(Endpoint {
            value: &max,
            inclusive: true,
        }),
    });
    assert_eq!(ids(read(&s, &t, &r).unwrap()), vec![1]);
    r.range.as_mut().unwrap().lower.as_mut().unwrap().inclusive = false;
    assert!(read(&s, &t, &r).unwrap().rows.is_empty());
    let v = Value::Bytes(vec![1]);
    let mut r = req(&[], &p);
    r.range = Some(Range {
        column: t.columns[2].id,
        lower: None,
        upper: Some(Endpoint {
            value: &v,
            inclusive: true,
        }),
    });
    assert_eq!(ids(read(&s, &t, &r).unwrap()), vec![1]);
}
#[test]
fn updates_deletes_fetch_current_base_projection_and_version() {
    let (mut s, t) = setup();
    insert(&mut s, 1, "k", Value::Bytes(vec![1]), Value::Int(1), "old");
    let eq = [(t.columns[2].id, Value::Bytes(vec![1]))];
    let p = [t.columns[4].id];
    let before = read(&s, &t, &req(&eq, &p)).unwrap().rows[0].clone();
    sql(
        &mut s,
        "UPDATE items SET payload='new',score=2 WHERE a=1 AND b='k'",
        &[],
    )
    .unwrap();
    let after = read(&s, &t, &req(&eq, &p)).unwrap().rows[0].clone();
    assert_eq!(after.values, vec![Value::Text("new".into())]);
    assert_ne!(after.version, before.version);
    sql(
        &mut s,
        "UPDATE items SET tag=$1 WHERE a=1 AND b='k'",
        &[Value::Bytes(vec![2])],
    )
    .unwrap();
    assert!(read(&s, &t, &req(&eq, &p)).unwrap().rows.is_empty());
    let eq = [(t.columns[2].id, Value::Bytes(vec![2]))];
    assert_eq!(read(&s, &t, &req(&eq, &p)).unwrap().rows.len(), 1);
    sql(&mut s, "DELETE FROM items WHERE a=1 AND b='k'", &[]).unwrap();
    assert!(read(&s, &t, &req(&eq, &p)).unwrap().rows.is_empty());
}
#[test]
fn unsupported_shape_types_stale_schema_and_other_table_reject() {
    let (mut s, t) = setup();
    let p = [t.columns[0].id];
    let nonprefix = [(t.columns[3].id, Value::Int(1))];
    assert_eq!(
        read(&s, &t, &req(&nonprefix, &p)),
        Err(SqlError::Unsupported)
    );
    assert_eq!(read(&s, &t, &req(&[], &p)), Err(SqlError::Unsupported));
    let eq = [(t.columns[2].id, Value::Bytes(vec![]))];
    let mut r = req(&eq, &p);
    r.descending = true;
    assert_eq!(read(&s, &t, &r), Err(SqlError::Unsupported));
    r.descending = false;
    r.residuals = &[()];
    assert_eq!(read(&s, &t, &r), Err(SqlError::Unsupported));
    let wrong = [(t.columns[2].id, Value::Int(0))];
    assert_eq!(read(&s, &t, &req(&wrong, &p)), Err(SqlError::Type));
    let mut stale = t.clone();
    stale.columns[0].name = "different".into();
    assert_eq!(read(&s, &stale, &req(&eq, &p)), Err(SqlError::StaleSchema));
    assert_eq!(
        s.secondary_read_engine(&t, s.epoch - 1, &req(&eq, &p), || false),
        Err(SqlError::StaleSchema)
    );
    let typed = [(
        t.columns[2].id,
        Value::Numeric(Numeric::parse("1").unwrap()),
    )];
    assert_eq!(read(&s, &t, &req(&typed, &p)), Err(SqlError::Unsupported));
    sql(
        &mut s,
        "CREATE TABLE other (id BIGINT PRIMARY KEY, tag BYTEA)",
        &[],
    )
    .unwrap();
    let other = s.tables["other"].clone();
    let eq = [(other.columns[1].id, Value::Bytes(vec![]))];
    let p = [other.columns[0].id];
    assert_eq!(read(&s, &other, &req(&eq, &p)), Err(SqlError::Unsupported));
}
#[test]
fn aggregate_output_row_caps_and_cancellation_are_atomic() {
    let (mut s, t) = setup();
    insert(&mut s, 1, "", Value::Null, Value::Null, "abcd");
    insert(&mut s, 2, "", Value::Null, Value::Null, "efgh");
    let eq = [(t.columns[2].id, Value::Null)];
    let p = [t.columns[4].id, t.columns[4].id];
    let before = s.clone();
    let mut r = req(&eq, &p);
    r.max_rows = 1;
    assert_eq!(read(&s, &t, &r), Err(SqlError::Limit));
    r.max_rows = 2;
    r.max_bytes = 29;
    assert_eq!(read(&s, &t, &r), Err(SqlError::Limit)); // names14 + payload16
    r.max_bytes = 30;
    assert_eq!(read(&s, &t, &r).unwrap().rows.len(), 2);
    let mut calls = 0;
    s.secondary_read_engine(&t, s.epoch, &r, || {
        calls += 1;
        false
    })
    .unwrap();
    // Every cancellation checkpoint, including after final projection, discards
    // local output. No successful partial page and no state changes.
    for at in 1..=calls {
        let mut n = 0;
        assert_eq!(
            s.secondary_read_engine(&t, s.epoch, &r, || {
                n += 1;
                n == at
            }),
            Err(SqlError::Aborted),
            "checkpoint {at}"
        );
    }
    assert_eq!(s, before);
}
#[test]
fn borrowed_oversize_rejects_before_cancel_or_copy_even_empty() {
    let (s, t) = setup();
    let p = [t.columns[0].id];
    let eq = [(t.columns[2].id, Value::Bytes(vec![0; MAX_ROW_BYTES]))];
    assert_eq!(
        s.secondary_read_engine(&t, s.epoch, &req(&eq, &p), || panic!(
            "oversize before callback"
        )),
        Err(SqlError::Limit)
    );
}

#[test]
fn binary_text_boolean_and_global_maximum_full_pk_bounds() {
    let mut s = Relational::default();
    sql(
        &mut s,
        "CREATE TABLE texts (id BIGINT PRIMARY KEY, tag TEXT, flag BOOLEAN)",
        &[],
    )
    .unwrap();
    let t = s.tables["texts"].clone();
    s.secondary_configure_test(
        t.id,
        "by_text_flag",
        &[t.columns[1].id, t.columns[2].id],
        || false,
    )
    .unwrap();
    for (id, tag, flag) in [
        (i64::MIN, "a", Value::Null),
        (0, "a", Value::Bool(false)),
        (i64::MAX, "a", Value::Bool(true)),
        (3, "a\0", Value::Bool(false)),
        (4, "a\0b", Value::Bool(false)),
        (5, "b", Value::Bool(false)),
    ] {
        sql(
            &mut s,
            "INSERT INTO texts VALUES ($1,$2,$3)",
            &[Value::Int(id), Value::Text(tag.into()), flag],
        )
        .unwrap();
    }
    let p = [t.columns[0].id];
    let eq = [(t.columns[1].id, Value::Text("a".into()))];
    assert_eq!(
        ids(read(&s, &t, &req(&eq, &p)).unwrap()),
        vec![i64::MIN, 0, i64::MAX]
    );
    let flag = Value::Bool(true);
    let mut r = req(&eq, &p);
    r.range = Some(Range {
        column: t.columns[2].id,
        lower: Some(Endpoint {
            value: &flag,
            inclusive: true,
        }),
        upper: Some(Endpoint {
            value: &flag,
            inclusive: true,
        }),
    });
    assert_eq!(ids(read(&s, &t, &r).unwrap()), vec![i64::MAX]);
    r.range.as_mut().unwrap().lower.as_mut().unwrap().inclusive = false;
    assert!(read(&s, &t, &r).unwrap().rows.is_empty());
    // All finite scalar components exhausted: upper is unbounded, still includes
    // the greatest real PK tie, and exclusive lower yields an empty result.
    let mut s = Relational::default();
    sql(
        &mut s,
        "CREATE TABLE ints (id BIGINT PRIMARY KEY, n BIGINT NOT NULL)",
        &[],
    )
    .unwrap();
    let t = s.tables["ints"].clone();
    s.secondary_configure_test(t.id, "by_n", &[t.columns[1].id], || false)
        .unwrap();
    sql(
        &mut s,
        "INSERT INTO ints VALUES ($1,$2)",
        &[Value::Int(i64::MAX), Value::Int(i64::MAX)],
    )
    .unwrap();
    let eq = [(t.columns[1].id, Value::Int(i64::MAX))];
    let p = [t.columns[0].id];
    assert_eq!(ids(read(&s, &t, &req(&eq, &p)).unwrap()), vec![i64::MAX]);
    let max = Value::Int(i64::MAX);
    let mut r = req(&[], &p);
    r.range = Some(Range {
        column: t.columns[1].id,
        lower: Some(Endpoint {
            value: &max,
            inclusive: false,
        }),
        upper: None,
    });
    assert!(read(&s, &t, &r).unwrap().rows.is_empty());
}

#[test]
fn canonical_oracle_all_nullable_range_pairs_and_inclusivity() {
    let (mut s, t) = setup();
    let scores = [
        Value::Null,
        Value::Int(i16::MIN.into()),
        Value::Int(-1),
        Value::Int(0),
        Value::Int(i16::MAX.into()),
    ];
    for (i, score) in scores.iter().enumerate() {
        for (j, tag) in [vec![], vec![0], vec![0, 255], vec![255]]
            .into_iter()
            .enumerate()
        {
            insert(
                &mut s,
                (i * 4 + j) as i64,
                "a",
                Value::Bytes(tag.clone()),
                score.clone(),
                "",
            );
            insert(
                &mut s,
                (i * 4 + j) as i64,
                "b",
                Value::Bytes(tag),
                score.clone(),
                "",
            );
        }
    }
    let p = [t.columns[0].id, t.columns[1].id];
    for tag in [vec![], vec![0], vec![0, 255], vec![255]] {
        let eq = [(t.columns[2].id, Value::Bytes(tag.clone()))];
        for lo in &scores {
            for hi in &scores {
                for li in [false, true] {
                    for ui in [false, true] {
                        let mut r = req(&eq, &p);
                        r.range = Some(Range {
                            column: t.columns[3].id,
                            lower: Some(Endpoint {
                                value: lo,
                                inclusive: li,
                            }),
                            upper: Some(Endpoint {
                                value: hi,
                                inclusive: ui,
                            }),
                        });
                        let low = encode_key(std::slice::from_ref(lo)).unwrap();
                        let high = encode_key(std::slice::from_ref(hi)).unwrap();
                        let mut expected = Vec::new();
                        for (pk, row) in &s.rows[&t.id] {
                            if row.values[2] != Value::Bytes(tag.clone()) {
                                continue;
                            }
                            let key = encode_key(&row.values[3..4]).unwrap();
                            if (key > low || (li && key == low))
                                && (key < high || (ui && key == high))
                            {
                                let mut order = encode_key(&row.values[2..4]).unwrap();
                                order.extend(pk);
                                expected.push((
                                    order,
                                    SqlRow {
                                        values: vec![row.values[0].clone(), row.values[1].clone()],
                                        version: row.version,
                                    },
                                ));
                            }
                        }
                        expected.sort_by(|a, b| a.0.cmp(&b.0));
                        assert_eq!(
                            read(&s, &t, &r).unwrap().rows,
                            expected.into_iter().map(|(_, r)| r).collect::<Vec<_>>()
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn empty_typed_pk_or_index_schema_is_rejected() {
    for pos in [0, 2] {
        let (mut s, _) = setup();
        std::sync::Arc::make_mut(&mut s.tables).get_mut("items").unwrap().columns[pos].data_type = DataType::Numeric;
        let t = s.tables["items"].clone();
        let p = [t.columns[4].id];
        let eq = [(t.columns[2].id, Value::Null)];
        assert_eq!(read(&s, &t, &req(&eq, &p)), Err(SqlError::Unsupported));
    }
}

#[test]
fn typed_nonkey_projection_is_preserved_with_conservative_admission() {
    let mut s = Relational::default();
    sql(
        &mut s,
        "CREATE TABLE nums (id BIGINT PRIMARY KEY, tag TEXT, n NUMERIC)",
        &[],
    )
    .unwrap();
    let t = s.tables["nums"].clone();
    s.secondary_configure_test(t.id, "by_tag", &[t.columns[1].id], || false)
        .unwrap();
    let n = Value::Numeric(Numeric::parse("123.45").unwrap());
    sql(
        &mut s,
        "INSERT INTO nums VALUES ($1,$2,$3)",
        &[Value::Int(1), Value::Text("a".into()), n.clone()],
    )
    .unwrap();
    let p = [t.columns[2].id];
    let eq = [(t.columns[1].id, Value::Text("a".into()))];
    let mut r = req(&eq, &p);
    let out = read(&s, &t, &r).unwrap();
    assert_eq!(out.rows[0].values, vec![n]);
    r.max_bytes = giggles_pg_types::MAX_TEXT;
    assert_eq!(read(&s, &t, &r), Err(SqlError::Limit));
}

#[test]
fn request_lookup_work_cap_rejects_even_when_result_would_be_empty() {
    let (mut s, t) = setup();
    for i in 0..256 {
        insert(&mut s, i, "", Value::Null, Value::Null, "");
    }
    let eq = [(t.columns[2].id, Value::Bytes(vec![255; 40_000]))];
    let p = [t.columns[0].id];
    // The large full endpoints repeated through tree comparison work exceed the
    // independent access budget even though no stored tag matches the prefix.
    assert_eq!(read(&s, &t, &req(&eq, &p)), Err(SqlError::Limit));
}

#[test]
fn individual_tuple_and_pk_arity_64_allow_combined_128() {
    let mut s = Relational::default();
    let names: Vec<_> = (0..64).map(|i| format!("c{i}")).collect();
    let columns = names
        .iter()
        .map(|n| format!("{n} BIGINT"))
        .collect::<Vec<_>>()
        .join(",");
    sql(
        &mut s,
        &format!(
            "CREATE TABLE wide ({columns}, PRIMARY KEY ({}))",
            names.join(",")
        ),
        &[],
    )
    .unwrap();
    let t = s.tables["wide"].clone();
    let ids: Vec<_> = t.columns.iter().map(|c| c.id).collect();
    s.secondary_configure_test(t.id, "wide_index", &ids, || false)
        .unwrap();
    let values: Vec<_> = (0..64).map(|i| Value::Int(i - 32)).collect();
    let params = (1..=64)
        .map(|i| format!("${i}"))
        .collect::<Vec<_>>()
        .join(",");
    sql(
        &mut s,
        &format!("INSERT INTO wide VALUES ({params})"),
        &values,
    )
    .unwrap();
    let eq: Vec<_> = ids.iter().copied().zip(values.clone()).collect();
    let result = read(&s, &t, &req(&eq, &ids)).unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values, values);
    assert_eq!(
        result.rows[0].version,
        s.rows[&t.id].values().next().unwrap().version
    );
}
