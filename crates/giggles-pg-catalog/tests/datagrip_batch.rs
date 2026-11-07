use giggles_pg_catalog::*;

fn snapshot() -> CatalogSnapshot {
    CatalogSnapshot {
        tables: vec![TableDef {
            id: 7,
            name: "inventory".into(),
            columns: vec![
                ColumnDef {
                    id: 1,
                    name: "id".into(),
                    data_type: ScalarType::BigInt,
                    not_null: true,
                },
                ColumnDef {
                    id: 2,
                    name: "body".into(),
                    data_type: ScalarType::Text,
                    not_null: false,
                },
            ],
            primary_key: vec![0],
        }],
        ..CatalogSnapshot::default()
    }
}
fn run(sql: &str, args: &[Value]) -> ResultSet {
    let p = plan(sql)
        .unwrap_or_else(|e| panic!("plan {e:?}: {sql}"))
        .expect("catalog");
    execute(&p, &snapshot(), args).unwrap_or_else(|e| panic!("execute {e:?}: {sql}"))
}
#[test]
fn captured_expanded_global_discovery() {
    let cases = [
        include_str!("fixtures/datagrip_batch_1.sql"),
        include_str!("fixtures/datagrip_batch_2.sql"),
        include_str!("fixtures/datagrip_batch_3.sql"),
        include_str!("fixtures/datagrip_batch_4.sql"),
        include_str!("fixtures/datagrip_batch_5.sql"),
        include_str!("fixtures/datagrip_batch_6.sql"),
        include_str!("fixtures/datagrip_batch_7.sql"),
        include_str!("fixtures/datagrip_batch_8.sql"),
        include_str!("fixtures/datagrip_batch_9.sql"),
        include_str!("fixtures/datagrip_batch_10.sql"),
        include_str!("fixtures/datagrip_batch_11.sql"),
        include_str!("fixtures/datagrip_batch_12.sql"),
        include_str!("fixtures/datagrip_batch_13.sql"),
    ];
    for (i, sql) in cases.iter().enumerate() {
        let r = run(sql, &[]);
        let expected = match i + 1 {
            2 => 1,
            5 => 1,
            11 => 2,
            _ => 0,
        };
        assert_eq!(r.rows.len(), expected, "case {}", i + 1);
    }
}
#[test]
fn bounded_union_ctes_aggregate_array_queries() {
    let r = run(
        "WITH x AS (SELECT oid,relname FROM pg_class WHERE relnamespace=$1::oid AND relkind='r') SELECT array_agg(relname ORDER BY oid) FROM x",
        &[Value::Int(2200)],
    );
    assert_eq!(
        r.rows,
        vec![vec![Value::Array(vec![Value::Text("inventory".into())])]]
    );
    let r = run(
        "SELECT array(SELECT unnest FROM unnest(ARRAY[3,1,2]) WHERE unnest>1 ORDER BY unnest)",
        &[],
    );
    assert_eq!(
        r.rows,
        vec![vec![Value::Array(vec![Value::Int(2), Value::Int(3)])]]
    );
    let r = run("SELECT array_agg(oid) FROM pg_extension", &[]);
    assert_eq!(r.rows, vec![vec![Value::Null]]);
    let r = run(
        "SELECT relnamespace,array_agg(oid ORDER BY oid) FROM pg_class WHERE relnamespace=2200 GROUP BY relnamespace",
        &[],
    );
    assert_eq!(r.rows[0][0], Value::Int(2200));
    assert_eq!(
        r.rows[0][1],
        Value::Array(vec![Value::Int(65564), Value::Int(65565)])
    );
    assert_eq!(
        run(
            "SELECT oid FROM pg_database UNION SELECT oid FROM pg_database",
            &[]
        )
        .rows
        .len(),
        1
    );
    assert_eq!(
        run(
            "SELECT oid FROM pg_database UNION ALL SELECT oid FROM pg_database",
            &[]
        )
        .rows
        .len(),
        2
    );
}
#[test]
fn union_projection_parameter_override_stays_fail_closed() {
    for sql in [
        "SELECT $1::oid UNION ALL SELECT oid FROM pg_namespace",
        "WITH q AS (SELECT $1::oid UNION ALL SELECT oid FROM pg_namespace) SELECT * FROM q",
    ] {
        assert!(!plan(sql).unwrap().unwrap().allows_numeric_input_override());
    }
}
#[test]
fn captured_application_discovery() {
    let cases = [
        include_str!("fixtures/datagrip_RetrieveExistentTables.sql"),
        include_str!("fixtures/datagrip_RetrieveTables.sql"),
        include_str!("fixtures/datagrip_RetrieveColumns.sql"),
        include_str!("fixtures/datagrip_RetrieveIndices.sql"),
        include_str!("fixtures/datagrip_RetrieveConstraints.sql"),
        include_str!("fixtures/datagrip_RetrieveExistentConstraints.sql"),
        include_str!("fixtures/datagrip_RetrieveIndexColumns.sql"),
    ];
    for (i, sql) in cases.iter().enumerate() {
        let r = run(sql, &[Value::Int(2200)]);
        assert_eq!(r.rows.len(), if i == 2 { 2 } else { 1 }, "case {i}");
        let get = |row: usize, name: &str| {
            r.rows[row][r.fields.iter().position(|f| f.name == name).expect(name)].clone()
        };
        match i {
            0 => assert_eq!(r.rows, vec![vec![Value::Int(65564)]]),
            1 => {
                assert_eq!(get(0, "table_name"), Value::Text("inventory".into()));
                assert_eq!(get(0, "table_id"), Value::Int(65564));
                assert_eq!(get(0, "table_state_number"), Value::Null);
                assert_eq!(get(0, "ancestors"), Value::Null);
                assert_eq!(get(0, "successors"), Value::Null);
                assert_eq!(get(0, "is_partition"), Value::Bool(false));
                assert_eq!(get(0, "owner"), Value::Text("giggles".into()));
            }
            2 => {
                for (row, name, ty, mandatory) in [(0, "id", 20, true), (1, "body", 25, false)] {
                    assert_eq!(get(row, "column_name"), Value::Text(name.into()));
                    assert_eq!(get(row, "column_position"), Value::Int(row as i64 + 1));
                    assert_eq!(get(row, "type_id"), Value::Int(ty));
                    assert_eq!(get(row, "mandatory"), Value::Bool(mandatory));
                    assert_eq!(get(row, "column_default_expression"), Value::Null);
                    assert_eq!(get(row, "column_state_number"), Value::Null);
                    assert_eq!(get(row, "column_is_inherited"), Value::Bool(false));
                }
            }
            3 => {
                assert_eq!(get(0, "index_name"), Value::Text("inventory_pkey".into()));
                assert_eq!(get(0, "is_primary"), Value::Bool(true));
                assert_eq!(get(0, "is_unique"), Value::Bool(true));
                assert_eq!(get(0, "index_id"), Value::Int(65565));
                assert_eq!(get(0, "access_method_id"), Value::Null);
            }
            4 => {
                assert_eq!(get(0, "con_name"), Value::Text("inventory_pkey".into()));
                assert_eq!(get(0, "con_kind"), Value::Text("p".into()));
                assert_eq!(get(0, "con_columns"), Value::Array(vec![Value::Int(1)]));
                assert_eq!(get(0, "con_state_id"), Value::Null);
                assert_eq!(get(0, "con_expression"), Value::Null);
                assert_eq!(get(0, "excl_operators_str"), Value::Array(vec![]));
            }
            5 => assert_eq!(r.rows, vec![vec![Value::Int(65566)]]),
            6 => {
                for name in ["col_idx", "column_position"] {
                    assert_eq!(get(0, name), Value::Int(1));
                }
                for name in ["column_options", "collation", "opclass"] {
                    assert_eq!(get(0, name), Value::Int(0));
                }
                for name in ["expression", "can_order", "collation_str", "opclass_str"] {
                    assert_eq!(get(0, name), Value::Null);
                }
            }
            _ => unreachable!(),
        }
        let p = plan(sql).unwrap().unwrap();
        assert!(
            execute(&p, &CatalogSnapshot::default(), &[Value::Int(2200)])
                .unwrap()
                .rows
                .is_empty(),
            "hidden table case {i}"
        );
    }
}
#[test]
fn vectors_numeric_descriptors_and_composite_pk_order() {
    let r = run(
        "SELECT indclass[0],indclass[1],array_lower(indclass,1),array_upper(indclass,1),indcollation FROM pg_index",
        &[],
    );
    assert_eq!(r.fields[4].data_type, PgType::OidVector);
    assert_eq!(
        r.rows,
        vec![vec![
            Value::Int(0),
            Value::Null,
            Value::Int(0),
            Value::Int(0),
            Value::Array(vec![Value::Int(0)])
        ]]
    );
    let mut sn = snapshot();
    sn.tables[0].primary_key = vec![1, 0];
    let p = plan(include_str!("fixtures/datagrip_RetrieveIndexColumns.sql"))
        .unwrap()
        .unwrap();
    let r = execute(&p, &sn, &[Value::Int(2200)]).unwrap();
    assert_eq!(
        r.rows.iter().map(|row| row[3].clone()).collect::<Vec<_>>(),
        vec![Value::Int(2), Value::Int(1)]
    );
    for sql in [
        "SELECT CASE WHEN false THEN 1::int2 ELSE 40000::int8 END",
        "SELECT coalesce(NULL::int2,40000::int8)",
        "SELECT nullif(1::int2,40000::int8)",
    ] {
        let r = run(sql, &[]);
        assert_eq!(r.fields[0].data_type, PgType::Int8);
        assert_eq!(
            r.rows[0][0],
            Value::Int(if sql.contains("nullif") { 1 } else { 40000 })
        );
    }
}
#[test]
fn new_query_forms_preserve_fail_closed_boundaries() {
    for sql in [
        "SELECT oid FROM pg_database UNION ALL SELECT id FROM hidden",
        "WITH q AS (SELECT oid FROM pg_database) SELECT * FROM q JOIN hidden ON true",
        "WITH q AS (SELECT oid FROM pg_database) DELETE FROM pg_database",
        "WITH RECURSIVE q AS (SELECT oid FROM pg_database) SELECT * FROM q",
        "SELECT made_up(oid) FROM pg_extension",
        "SELECT * FROM pg_catalog.made_up UNION SELECT * FROM pg_extension",
        "SELECT oid,array_agg(oid) FROM pg_database",
        "SELECT oid FROM pg_database WHERE array_agg(oid) IS NULL",
        "SELECT array_agg(array_agg(oid)) FROM pg_database",
        "SELECT oid FROM pg_extension ORDER BY $1 LIMIT $2",
    ] {
        assert!(plan(sql).is_err(), "{sql}");
    }
    let p = plan(include_str!("fixtures/datagrip_RetrieveColumns.sql"))
        .unwrap()
        .unwrap();
    assert_eq!(execute(&p, &snapshot(), &[]), Err(Error::Parameters));
    assert_eq!(
        execute(&p, &snapshot(), &[Value::Text("bad".into())]),
        Err(Error::Type)
    );
    let mut invalid = snapshot();
    invalid.tables[0].primary_key = vec![2];
    assert_eq!(
        execute(&p, &invalid, &[Value::Int(2200)]),
        Err(Error::InvalidSnapshot)
    );
    let sql = ["SELECT generate_series FROM pg_database CROSS JOIN generate_series(0,1024)"; 8]
        .join(" UNION ALL ");
    let p = plan(&sql).unwrap().unwrap();
    assert_eq!(execute(&p, &snapshot(), &[]), Err(Error::Limit));
    let p = plan("SELECT array_agg($1::text) FROM pg_database CROSS JOIN generate_series(1,1024)")
        .unwrap()
        .unwrap();
    assert_eq!(
        execute(&p, &snapshot(), &[Value::Text("x".repeat(8192))]),
        Err(Error::Limit)
    );
}
