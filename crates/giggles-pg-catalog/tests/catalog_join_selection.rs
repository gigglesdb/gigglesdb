use giggles_pg_catalog::*;

fn snapshot() -> CatalogSnapshot {
    CatalogSnapshot {
        tables: vec![TableDef {
            id: 7,
            name: "inventory".into(),
            columns: vec![ColumnDef {
                id: 11,
                name: "id".into(),
                data_type: ScalarType::Integer,
                not_null: true,
            }],
            primary_key: vec![0],
        }],
    }
}
fn run(sql: &str, params: &[Value]) -> Result<ResultSet, Error> {
    execute(&plan(sql)?.expect("catalog query"), &snapshot(), params)
}

#[test]
fn expanded_catalog_and_jdbc_lookups() {
    let types = run("SELECT oid FROM pg_type", &[]).unwrap();
    assert!(types.rows.len() >= 87);
    for oid in [24, 194, 1007] {
        assert!(types.rows.contains(&vec![Value::Int(oid)]));
    }
    let delimiter = run(
        include_str!("fixtures/jdbc_array_delimiter.sql"),
        &[Value::Int(1007)],
    )
    .unwrap();
    assert_eq!(delimiter.fields[0].data_type, PgType::Char);
    assert_eq!(delimiter.rows, vec![vec![Value::Text(",".into())]]);
    let element = run(
        include_str!("fixtures/jdbc_array_element.sql"),
        &[Value::Int(1007)],
    )
    .unwrap();
    assert_eq!(
        element.rows,
        vec![vec![
            Value::Int(23),
            Value::Bool(true),
            Value::Text("pg_catalog".into()),
            Value::Text("int4".into())
        ]]
    );
}

#[test]
fn where_matches_explicit_on_with_aliases_nulls_and_parameters() {
    for (predicate, params) in [
        ("t.oid=$1 AND t.typelem=e.oid", vec![Value::Int(1007)]),
        (
            "t.oid=$1 AND t.typelem=e.oid AND t.oid=$1",
            vec![Value::Int(1007)],
        ),
        (
            "t.oid=$3 AND t.typelem=e.oid",
            vec![Value::Null, Value::Null, Value::Int(1007)],
        ),
        ("t.oid=$1 AND t.typelem=e.oid", vec![Value::Null]),
        ("t.oid=$1 AND t.typelem=e.oid", vec![Value::Int(0)]),
        ("t.oid=$1 AND t.typelem=e.oid", vec![Value::Int(999999)]),
        ("t.typname=e.typname AND t.oid=23", vec![]),
        (
            "t.typnotnull=e.typnotnull AND t.oid=23 AND e.oid=23",
            vec![],
        ),
    ] {
        let projection = "SELECT t.oid AS type_id,e.typname AS element";
        let selected = run(
            &format!("{projection} FROM pg_type t,pg_type e WHERE {predicate}"),
            &params,
        )
        .unwrap();
        let joined = run(
            &format!("{projection} FROM pg_type t JOIN pg_type e ON {predicate}"),
            &params,
        )
        .unwrap();
        assert_eq!(selected, joined, "{predicate}");
    }
    assert!(matches!(
        run(
            "SELECT t.oid FROM pg_type t,pg_type e WHERE t.oid=$1 AND t.typelem=e.oid",
            &[]
        ),
        Err(Error::Parameters)
    ));
    assert!(matches!(
        run(
            "SELECT t.oid FROM pg_type t,pg_type e WHERE t.oid=$1 AND t.typelem=e.oid",
            &[Value::Text("bad".into())]
        ),
        Err(Error::Type)
    ));
}

#[test]
fn null_on_counterexample_remains_empty_success() {
    let predicate = "1=1 AND 1=1 AND 1=1 AND 1=1 AND 1=1 AND 1=1 AND 1=1 AND 1=1";
    let sql = format!(
        "SELECT t.oid FROM pg_type t JOIN generate_series(1,1000) b ON NULL WHERE {predicate}"
    );
    assert!(run(&sql, &[]).unwrap().rows.is_empty());
    assert!(matches!(
        run(
            &format!(
                "SELECT t.oid FROM pg_type t JOIN generate_series(1,1000) b ON NULL AND ({predicate}) WHERE true"
            ),
            &[]
        ),
        Err(Error::Limit)
    ));
}

#[test]
fn promoted_parameter_keeps_original_cast_error_priority() {
    // Global $1 is int8 but the filter leaf is OID. Moving it would cast per
    // candidate and report Type before the old oversized intermediate Limit.
    let sql = "SELECT $1::int8 FROM pg_type t,pg_type e WHERE t.oid=$1 AND t.typelem=e.oid";
    let p = plan(sql).unwrap().unwrap();
    assert_eq!(p.parameter_types(), &[PgType::Int8]);
    assert!(!p.allows_numeric_input_override());
    assert!(matches!(
        execute(&p, &snapshot(), &[Value::Int(-1)]),
        Err(Error::Limit)
    ));
}

#[test]
fn excluded_filters_and_join_shapes_keep_materialization() {
    for sql in [
        "SELECT t.oid FROM pg_type t LEFT JOIN pg_type e ON true WHERE t.oid=23 AND e.oid=23",
        "SELECT t.oid FROM pg_type t JOIN pg_type e ON t.oid=e.oid WHERE t.oid=23 OR e.oid=23",
        "SELECT t.oid FROM pg_type t,pg_type e WHERE t.oid=23 OR e.oid=23",
        "SELECT t.oid FROM pg_type t,pg_type e WHERE t.oid::int8=23",
        "SELECT t.oid FROM pg_type t,pg_type e WHERE upper(t.typname)='INT4'",
        "SELECT t.oid FROM pg_type t,pg_type e WHERE 1/0=0",
    ] {
        // The nontruth selective ON avoids the oversized cross product.
        if sql.contains("ON t.oid=e.oid") {
            assert_eq!(run(sql, &[]).unwrap().rows, vec![vec![Value::Int(23)]]);
        } else {
            let result = run(sql, &[]);
            assert!(matches!(result, Err(Error::Limit)), "{sql}: {result:?}");
        }
    }
    assert!(
        run(
            "SELECT t.oid FROM pg_type t JOIN pg_type e ON false WHERE t.oid=e.oid",
            &[]
        )
        .unwrap()
        .rows
        .is_empty()
    );
    let lateral = run(
        "SELECT t.oid,x.n FROM pg_type t CROSS JOIN unnest(ARRAY[t.oid]) x(n) WHERE t.oid=x.n",
        &[],
    )
    .unwrap();
    assert_eq!(lateral.rows.len(), 89);
    let correlated = run("SELECT t.oid FROM pg_type t WHERE t.oid=(SELECT e.oid FROM pg_type e,pg_namespace n WHERE e.oid=t.oid AND n.oid=11) ORDER BY t.oid", &[]).unwrap();
    assert_eq!(correlated.rows.len(), 89);
}

#[test]
fn actual_result_and_work_limits_still_reject_without_a_result() {
    // All pairs survive; the typed full-row result is still over 4 MiB.
    assert!(matches!(
        run("SELECT t.oid FROM pg_type t,pg_type e WHERE 1=1", &[]),
        Err(Error::Limit)
    ));
    // Zero survivors keep byte/row materialization small but spend >2M work.
    let sql = "SELECT t.oid FROM (SELECT oid FROM pg_type LIMIT 1) t CROSS JOIN generate_series(1,1000) a CROSS JOIN generate_series(1,1000) b WHERE 1=0";
    assert!(matches!(run(sql, &[]), Err(Error::Limit)));
    assert_eq!(MAX_WORK, 2_000_000);
    assert_eq!(MAX_BYTES, 4 * 1024 * 1024);
}
