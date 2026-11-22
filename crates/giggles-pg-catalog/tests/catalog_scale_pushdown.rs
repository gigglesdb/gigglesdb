use giggles_pg_catalog::*;

const GET_COLUMNS: &str = include_str!("fixtures/pgjdbc_42_7_3_getcolumns.sql");

fn fixture_snapshot(count: usize) -> CatalogSnapshot {
    let tables = (0..count)
        .map(|i| TableDef {
            id: i as u64 + 1,
            name: if i == 0 {
                "r0d_jdbc_items".into()
            } else {
                format!("scale_{i:03}")
            },
            columns: (0..if i == 0 { 2 } else { 8 })
                .map(|c| ColumnDef {
                    id: c + 1,
                    name: if i == 0 {
                        ["id", "name"][c as usize].into()
                    } else {
                        format!("col_{c}")
                    },
                    data_type: if i == 0 && c == 1 {
                        ScalarType::Text
                    } else {
                        ScalarType::Integer
                    },
                    not_null: c == 0,
                })
                .collect(),
            primary_key: vec![0],
        })
        .collect();
    CatalogSnapshot {
        tables,
        ..CatalogSnapshot::default()
    }
}

fn run(sql: &str, snapshot: &CatalogSnapshot, params: &[Value]) -> Result<ResultSet, Error> {
    execute(
        &plan_gateway(sql)?.expect("catalog statement"),
        snapshot,
        params,
    )
}

#[test]
fn jdbc_getcolumns_matches_every_pg18_cell_at_scale() {
    let oracle: Vec<Vec<Value>> = include_str!("fixtures/pg18_getcolumns.psv")
        .lines()
        .skip(1)
        .take(2)
        .map(|line| {
            line.split('|')
                .enumerate()
                .map(|(i, cell)| {
                    if cell.is_empty() {
                        Value::Null
                    } else if i == 4 {
                        Value::Bool(cell == "t")
                    } else if matches!(i, 3 | 5..=8 | 13) {
                        Value::Int(cell.parse().unwrap())
                    } else {
                        Value::Text(cell.into())
                    }
                })
                .collect()
        })
        .collect();
    assert!(oracle.iter().all(|row| row.len() == 15));
    for count in [1, 25, 128] {
        let result = run(GET_COLUMNS, &fixture_snapshot(count), &[])
            .unwrap_or_else(|e| panic!("{count} tables: {e:?}"));
        assert_eq!(result.rows, oracle, "{count} tables");
        assert_eq!(result.fields.len(), 15);
        assert_eq!(
            result
                .fields
                .iter()
                .map(|f| f.data_type)
                .collect::<Vec<_>>(),
            vec![
                PgType::Name,
                PgType::Name,
                PgType::Name,
                PgType::Oid,
                PgType::Bool,
                PgType::Int4,
                PgType::Int2,
                PgType::Int4,
                PgType::Int8,
                PgType::Char,
                PgType::Char,
                PgType::Text,
                PgType::Text,
                PgType::Oid,
                PgType::Char
            ]
        );
    }
}

#[test]
fn every_captured_driver_statement_executes_at_128_tables() {
    let snapshot = fixture_snapshot(128);
    let oid = run(
        "SELECT oid FROM pg_class WHERE relname='r0d_jdbc_items'",
        &fixture_snapshot(1),
        &[],
    )
    .unwrap()
    .rows[0][0]
        .clone();
    let Value::Int(oid) = oid else {
        panic!("table oid")
    };
    let mut queries = Vec::new();
    let jdbc: Vec<_> = include_str!("fixtures/pgjdbc_42_7_3_catalog_statements.sql")
        .split("-- params:")
        .skip(1)
        .collect();
    assert_eq!(jdbc.len(), 14);
    for (i, block) in jdbc.iter().enumerate() {
        let (params, sql) = block.split_once('\n').unwrap();
        // The capture contains a parameter record for every statement; all are empty.
        assert!(
            params.trim().is_empty(),
            "unbound captured parameters: {params}"
        );
        let mut sql = sql.to_string();
        for captured in [
            16840, 16848, 16854, 16862, 16868, 16874, 16880, 16886, 16892,
        ] {
            sql = sql.replace(&captured.to_string(), &oid.to_string());
        }
        queries.push((format!("pgjdbc {:02}", i + 1), sql));
    }
    let psql: Vec<_> = include_str!("fixtures/psql18_describe_queries.sql")
        .split("-- query ")
        .skip(1)
        .collect();
    assert_eq!(psql.len(), 34);
    for block in psql {
        let (tag, sql) = block.split_once('\n').unwrap();
        queries.push((
            format!("psql {}", tag.trim()),
            sql.replace("'16384'", &format!("'{oid}'")),
        ));
    }
    let npgsql: Vec<_> = include_str!("fixtures/npgsql_9_0_4_startup.sql")
        .split_inclusive(';')
        .filter(|s| !s.trim().is_empty())
        .collect();
    assert_eq!(npgsql.len(), 4);
    for (i, sql) in npgsql.iter().enumerate() {
        queries.push((format!("npgsql {}", i + 1), sql.to_string()));
    }
    let mut failures = Vec::new();
    for (tag, sql) in queries {
        match run(&sql, &snapshot, &[]) {
            Ok(result) => eprintln!("{tag}: OK {} rows", result.rows.len()),
            Err(error) => {
                eprintln!("{tag}: ERROR {error:?}");
                failures.push(format!("{tag}: {error:?}"));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "all captured queries must execute:\n{}",
        failures.join("\n")
    );
}

#[test]
fn snapshot_projects_129_tables_within_budget() {
    let sql = "SELECT count(*) FROM pg_class WHERE relnamespace=2200 AND relkind='r'";
    assert_eq!(
        run(sql, &fixture_snapshot(129), &[]).unwrap().rows,
        vec![vec![Value::Int(129)]]
    );
}

#[test]
fn null_side_where_and_right_only_left_on_preserve_semantics() {
    let snapshot = fixture_snapshot(1);
    assert!(run("SELECT t.oid FROM pg_type t LEFT JOIN pg_type e ON e.oid=t.oid WHERE t.oid=23 AND e.oid IS NULL", &snapshot, &[]).unwrap().rows.is_empty());
    for predicate in ["e.oid=23", "e.oid IS NOT NULL", "NOT (e.oid IS NULL)"] {
        let sql = format!(
            "SELECT t.oid FROM pg_type t LEFT JOIN pg_type e ON e.oid=0 WHERE t.oid=23 AND {predicate}"
        );
        assert!(
            run(&sql, &snapshot, &[]).unwrap().rows.is_empty(),
            "{predicate}"
        );
    }
    for on in ["e.oid=23", "e.oid IS NULL", "e.oid=NULL", "NULL"] {
        let sql =
            format!("SELECT t.oid,e.oid FROM pg_type t LEFT JOIN pg_type e ON {on} WHERE t.oid=23");
        let expected = if on == "e.oid=23" {
            Value::Int(23)
        } else {
            Value::Null
        };
        assert_eq!(
            run(&sql, &snapshot, &[]).unwrap().rows,
            vec![vec![Value::Int(23), expected]],
            "{on}"
        );
        let equivalent = format!(
            "SELECT t.oid,e.oid FROM pg_type t LEFT JOIN (SELECT e.oid FROM pg_type e WHERE {on}) e ON true WHERE t.oid=23"
        );
        assert_eq!(
            run(&sql, &snapshot, &[]).unwrap().rows,
            run(&equivalent, &snapshot, &[]).unwrap().rows,
            "{on}"
        );
    }
}

#[test]
fn like_forms_and_parameters_select_before_join_materialization() {
    // Stay within the existing snapshot cap to isolate optimizer behavior.
    let snapshot = fixture_snapshot(128);
    for predicate in [
        "c.relname LIKE 'r0d_jdbc_items'",
        "c.relname ILIKE 'R0D_JDBC_ITEMS'",
        "c.relname NOT LIKE 'scale_%' AND c.relname='r0d_jdbc_items'",
        "c.relname NOT ILIKE 'SCALE_%' AND c.relname='r0d_jdbc_items'",
        "c.relname LIKE $1",
        "c.relname ILIKE $1",
        "NOT (c.relname NOT LIKE $1)",
        "NOT (c.relname NOT ILIKE $1)",
    ] {
        let sql = format!(
            "SELECT c.relname FROM pg_class c LEFT JOIN pg_attribute a ON true WHERE {predicate}"
        );
        let params = if predicate.contains("$1") {
            vec![Value::Text("r0d_jdbc_items".into())]
        } else {
            vec![]
        };
        let result = run(&sql, &snapshot, &params).unwrap();
        assert!(!result.rows.is_empty(), "{predicate}");
        assert!(
            result
                .rows
                .iter()
                .all(|r| r == &vec![Value::Text("r0d_jdbc_items".into())]),
            "{predicate}"
        );
    }
}

#[test]
fn derived_window_filter_does_not_change_row_numbers() {
    let result = run("SELECT attname,attnum FROM (SELECT a.attname,row_number() OVER (PARTITION BY a.attrelid ORDER BY a.attnum) AS attnum FROM pg_attribute a JOIN pg_class c ON a.attrelid=c.oid WHERE c.relname='r0d_jdbc_items') q WHERE attname='name'", &fixture_snapshot(1), &[]).unwrap();
    assert_eq!(
        result.rows,
        vec![vec![Value::Text("name".into()), Value::Int(8)]]
    );
}

#[test]
fn nested_join_on_and_correlated_outer_columns_keep_their_scopes() {
    let snapshot = fixture_snapshot(1);
    for on in ["e.oid=23", "e.oid IS NULL", "e.oid=NULL"] {
        let sql = format!(
            "SELECT t.oid,e.oid,n.oid FROM pg_type t LEFT JOIN pg_type e ON {on} JOIN pg_type n ON n.oid=t.oid WHERE t.oid=23"
        );
        let e = if on == "e.oid=23" {
            Value::Int(23)
        } else {
            Value::Null
        };
        assert_eq!(
            run(&sql, &snapshot, &[]).unwrap().rows,
            vec![vec![Value::Int(23), e, Value::Int(23)]],
            "{on}"
        );
    }
    let sql = "SELECT t.oid,(SELECT e.oid FROM pg_type e LEFT JOIN pg_type n ON n.oid=e.oid AND n.oid=23 WHERE e.oid=t.oid) FROM pg_type t WHERE t.oid=23";
    assert_eq!(
        run(sql, &snapshot, &[]).unwrap().rows,
        vec![vec![Value::Int(23), Value::Int(23)]]
    );
}

#[test]
fn equality_join_candidates_preserve_duplicates_nulls_and_residual_on() {
    // Catalog-backed singleton branches keep this a recognized catalog query.
    let left = "SELECT 1 AS key,1 AS tag FROM pg_type WHERE oid=23 UNION ALL SELECT 1,2 UNION ALL SELECT NULL::int4,3 UNION ALL SELECT 3,4";
    let right = "SELECT 1 AS key,1 AS tag FROM pg_type WHERE oid=23 UNION ALL SELECT 1,2 UNION ALL SELECT 1,3 UNION ALL SELECT NULL::int4,40 UNION ALL SELECT 2,50";
    let matched = vec![
        vec![Value::Int(1), Value::Int(1), Value::Int(2)],
        vec![Value::Int(1), Value::Int(1), Value::Int(3)],
        vec![Value::Int(2), Value::Int(1), Value::Int(1)],
        vec![Value::Int(2), Value::Int(1), Value::Int(3)],
    ];
    for join in ["JOIN", "LEFT JOIN"] {
        let sql = format!(
            "SELECT l.tag,r.key,r.tag FROM ({left}) l {join} ({right}) r ON l.key=r.key AND l.tag<>r.tag ORDER BY l.tag,r.tag"
        );
        let mut expected = matched.clone();
        if join == "LEFT JOIN" {
            expected.push(vec![Value::Int(3), Value::Null, Value::Null]);
            expected.push(vec![Value::Int(4), Value::Null, Value::Null]);
        }
        assert_eq!(
            run(&sql, &fixture_snapshot(1), &[]).unwrap().rows,
            expected,
            "{join}"
        );
    }
}
