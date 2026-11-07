use giggles_core::{Admin, Limits, relational::*};
fn fresh() -> std::path::PathBuf {
    let root = std::env::temp_dir().join("dd68-aggregate-scalars");
    std::fs::create_dir_all(&root).unwrap();
    root.join(format!(
        "aggregate-{}-{:?}-{}",
        std::process::id(),
        std::thread::current().id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
#[test]
fn nested_scalar_aggregate_values_types_and_empty_input() {
    let a = Admin::create(fresh(), Limits::default()).unwrap();
    let mut s = a.sql_session();
    s.execute(
        "CREATE TABLE agg (id bigint primary key, amount integer, label text)",
        &[],
        None,
    )
    .unwrap();
    for sql in [
        "INSERT INTO agg VALUES (1,-2,'Ab')",
        "INSERT INTO agg VALUES (2,NULL,NULL)",
        "INSERT INTO agg VALUES (3,3,'C')",
    ] {
        s.execute(sql, &[], None).unwrap();
    }
    let q = "SELECT sum(abs(amount)), min(lower(label)), max(length(lower(label))), count(NULL), count(*), sum(amount::bigint) FROM agg";
    let p = s.prepare(q).unwrap();
    assert_eq!(
        s.describe(&p)
            .unwrap()
            .columns
            .iter()
            .map(|c| c.data_type.clone())
            .collect::<Vec<_>>(),
        vec![
            DataType::BigInt,
            DataType::Text,
            DataType::Integer,
            DataType::BigInt,
            DataType::BigInt,
            DataType::Numeric
        ]
    );
    assert_eq!(
        s.execute_prepared(&p, &[], None).unwrap().rows[0].values,
        vec![
            Value::Int(5),
            Value::Text("ab".into()),
            Value::Int(2),
            Value::Int(0),
            Value::Int(3),
            Value::Numeric(Numeric::parse("1").unwrap())
        ]
    );
    let empty = s.execute(&format!("{q} WHERE FALSE"), &[], None).unwrap();
    assert_eq!(
        empty.rows[0].values,
        vec![
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Int(0),
            Value::Int(0),
            Value::Null
        ]
    );
    assert!(
        s.execute(&format!("{q} LIMIT 0"), &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
    assert_eq!(
        s.execute("SELECT sum(abs(amount)) FROM agg LIMIT 1", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Int(5)]
    );
}
#[test]
fn aggregate_parameter_metadata_and_repeated_execution() {
    let a = Admin::create(fresh(), Limits::default()).unwrap();
    let mut s = a.sql_session();
    s.execute(
        "CREATE TABLE agg (id bigint primary key, amount integer)",
        &[],
        None,
    )
    .unwrap();
    s.execute("INSERT INTO agg VALUES (1,-2)", &[], None)
        .unwrap();
    let p = s
        .prepare("SELECT sum(abs($1::integer)), count($1::integer) FROM agg")
        .unwrap();
    let d = s.describe(&p).unwrap();
    assert_eq!(d.parameters, vec![DataType::Integer]);
    assert_eq!(
        d.columns
            .iter()
            .map(|c| c.data_type.clone())
            .collect::<Vec<_>>(),
        vec![DataType::BigInt, DataType::BigInt]
    );
    for (v, expected) in [
        (Value::Int(-7), vec![Value::Int(7), Value::Int(1)]),
        (Value::Null, vec![Value::Null, Value::Int(0)]),
    ] {
        assert_eq!(
            s.execute_prepared(&p, &[v], None).unwrap().rows[0].values,
            expected
        );
    }
    for q in [
        "SELECT sum(NULL) FROM agg",
        "SELECT sum('1') FROM agg",
        "SELECT sum($1) FROM agg",
        "SELECT count($1) FROM agg",
        "SELECT sum(sum(amount)) FROM agg",
    ] {
        let p = s.prepare(q).unwrap();
        assert!(s.describe(&p).is_err(), "{q}");
    }
}

#[test]
fn aggregate_numeric_width_scale_carry_and_special_values() {
    let a = Admin::create(fresh(), Limits::default()).unwrap();
    let mut s = a.sql_session();
    s.execute("CREATE TABLE agg (id bigint primary key)", &[], None)
        .unwrap();
    for q in ["INSERT INTO agg VALUES (1)", "INSERT INTO agg VALUES (2)"] {
        s.execute(q, &[], None).unwrap();
    }
    for (e, text, ty) in [
        ("2147483647::integer", "4294967294", DataType::BigInt),
        (
            "9223372036854775807::bigint",
            "18446744073709551614",
            DataType::Numeric,
        ),
        (
            "1.00000000000000000001::numeric",
            "2.00000000000000000002",
            DataType::Numeric,
        ),
        (
            "99999999999999999999::numeric",
            "199999999999999999998",
            DataType::Numeric,
        ),
        ("'NaN'::numeric", "NaN", DataType::Numeric),
    ] {
        let q = format!("SELECT sum({e}) FROM agg");
        let p = s.prepare(&q).unwrap();
        assert_eq!(s.describe(&p).unwrap().columns[0].data_type, ty);
        let v = s
            .execute_prepared(&p, &[], None)
            .unwrap()
            .rows
            .remove(0)
            .values
            .remove(0);
        match v {
            Value::Numeric(n) => assert_eq!(n.to_string(), text),
            Value::Int(n) => assert_eq!(n.to_string(), text),
            _ => panic!("{q}"),
        }
    }
    for (expr, oid) in [
        ("count($1::integer), sum($1)", DataType::Integer),
        ("min($1), sum($1::integer)", DataType::Text),
        ("sum($1::integer), min($1)", DataType::Integer),
    ] {
        let p = s.prepare(&format!("SELECT {expr} FROM agg")).unwrap();
        assert_eq!(s.describe(&p).unwrap().parameters, vec![oid]);
    }
    let p = s
        .prepare("SELECT sum($1), count($1::integer) FROM agg")
        .unwrap();
    assert!(s.describe(&p).is_err());
}
#[test]
fn aggregate_null_temporal_float_and_overflow_contract() {
    let a = Admin::create(fresh(), Limits::default()).unwrap();
    let mut s = a.sql_session();
    s.execute(
        "CREATE TABLE agg (id bigint primary key, amount integer, label text)",
        &[],
        None,
    )
    .unwrap();
    s.execute("INSERT INTO agg VALUES (1,NULL,NULL)", &[], None)
        .unwrap();
    for q in [
        "SELECT sum(abs(amount)), min(lower(label)), max(length(label)) FROM agg",
        "SELECT sum(NULL::numeric), min(NULL), max(NULL) FROM agg",
    ] {
        assert_eq!(
            s.execute(q, &[], None).unwrap().rows[0].values,
            vec![Value::Null, Value::Null, Value::Null]
        );
    }
    for (e, expected) in [
        (
            "sum('-0'::float4)",
            Value::Float4(Float4::from_bits((-0.0f32).to_bits())),
        ),
        (
            "sum('-0'::float8)",
            Value::Float8(Float8::from_bits((-0.0f64).to_bits())),
        ),
    ] {
        assert_eq!(
            s.execute(&format!("SELECT {e} FROM agg"), &[], None)
                .unwrap()
                .rows[0]
                .values,
            vec![expected]
        );
    }
    let p = s
        .prepare("SELECT min(date_trunc('day','2025-01-02 03:04:05'::timestamp)) FROM agg")
        .unwrap();
    assert_eq!(
        s.describe(&p).unwrap().columns[0].data_type,
        DataType::Timestamp
    );
    assert_eq!(s.execute_prepared(&p, &[], None).unwrap().rows.len(), 1);
    for q in [
        "SELECT sum(TRUE) FROM agg",
        "SELECT min(TRUE) FROM agg",
        "SELECT sum(coalesce(NULL,NULL)) FROM agg",
    ] {
        let p = s.prepare(q).unwrap();
        assert!(s.describe(&p).is_err(), "{q}");
    }
    s.execute("INSERT INTO agg VALUES (2,-2147483648,'x')", &[], None)
        .unwrap();
    assert!(
        s.execute("SELECT sum(abs(amount)) FROM agg", &[], None)
            .is_err()
    );
    let p = s
        .prepare("SELECT sum(abs(-2147483648)) FROM agg WHERE FALSE")
        .unwrap();
    assert!(s.validate_immutable_constants(&p).is_err());
    assert_eq!(
        s.execute("SELECT count(*) FROM agg", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Int(2)]
    );
}
#[test]
fn aggregate_scalar_work_budget_is_shared_over_full_input() {
    let a = Admin::create(fresh(), Limits::default()).unwrap();
    let mut s = a.sql_session();
    s.execute(
        "CREATE TABLE agg (id bigint primary key, label text)",
        &[],
        None,
    )
    .unwrap();
    for id in 0..24 {
        s.execute(
            &format!("INSERT INTO agg VALUES ({id},'0123456789abcdef')"),
            &[],
            None,
        )
        .unwrap();
    }
    let error = s
        .execute(
            "SELECT sum(length(repeat(label,4096))) FROM agg LIMIT 1",
            &[],
            None,
        )
        .unwrap_err();
    assert_eq!(error, giggles_core::CoreError::Sql(SqlError::Limit));
    assert_eq!(
        s.execute("SELECT count(*) FROM agg", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Int(24)]
    );
}
#[test]
fn aggregate_legacy_column_contract_uses_ast_and_survives_reopen() {
    let path = fresh();
    {
        let a = Admin::create(&path, Limits::default()).unwrap();
        let mut s = a.sql_session();
        s.execute(
            "CREATE TABLE agg (id bigint primary key, label text, payload bytea)",
            &[],
            None,
        )
        .unwrap();
        s.execute(
            "INSERT INTO agg VALUES (1,'x',$1)",
            &[Value::Bytes(vec![0, 255])],
            None,
        )
        .unwrap();
        for q in [
            "SELECT sum(label) FROM agg",
            "SELECT sum((a.label)) FROM agg a",
        ] {
            let p = s.prepare(q).unwrap();
            assert_eq!(
                s.describe(&p).unwrap_err(),
                giggles_core::CoreError::Sql(SqlError::Type)
            );
        }
        let p = s.prepare("SELECT sum(label::text) FROM agg").unwrap();
        assert_eq!(
            s.describe(&p).unwrap_err(),
            giggles_core::CoreError::Sql(SqlError::UndefinedFunction)
        );
        assert_eq!(
            s.execute(
                "SELECT min((a.payload)),max(a.payload) FROM agg a",
                &[],
                None
            )
            .unwrap()
            .rows[0]
                .values,
            vec![Value::Bytes(vec![0, 255]), Value::Bytes(vec![0, 255])]
        );
    }
    let a = Admin::open(&path, Limits::default()).unwrap();
    let mut s = a.sql_session();
    assert_eq!(
        s.execute("SELECT sum(abs(id)) FROM agg", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Numeric(Numeric::parse("1").unwrap())]
    );
}
#[test]
fn nested_count_star_context_and_quoted_resolution_boundaries() {
    let a = Admin::create(fresh(), Limits::default()).unwrap();
    let mut s = a.sql_session();
    s.execute(
        "CREATE TABLE agg (id bigint primary key, count integer)",
        &[],
        None,
    )
    .unwrap();
    for expr in [
        "count(*)",
        "pg_catalog.count(*)",
        "\"count\"(*)",
        "\"pg_catalog\".\"count\"(*)",
        "abs(count(*))",
        "(count(*))::bigint",
    ] {
        let p = s.prepare(&format!("SELECT sum({expr}) FROM agg")).unwrap();
        assert_eq!(
            s.describe(&p).unwrap_err(),
            giggles_core::CoreError::Sql(SqlError::GroupingError)
        );
    }
    for q in [
        "SELECT count(*) FROM agg",
        "SELECT count('count(*)') FROM agg",
        "SELECT sum(count) FROM agg",
    ] {
        s.execute(q, &[], None).unwrap();
    }
    for q in [
        "SELECT sum(\"COUNT\"(*)) FROM agg",
        "SELECT sum(public.count(*)) FROM agg",
        "SELECT sum(missing.count(*)) FROM agg",
        "SELECT abs(count(*)) FROM agg",
    ] {
        let e = match s.prepare(q) {
            Err(e) => e,
            Ok(p) => s.describe(&p).unwrap_err(),
        };
        assert_ne!(
            e,
            giggles_core::CoreError::Sql(SqlError::GroupingError),
            "{q}"
        );
    }
    let q = format!(
        "SELECT sum({}id{}) FROM agg",
        "(".repeat(40),
        ")".repeat(40)
    );
    assert!(s.prepare(&q).is_err());
    let q = format!("SELECT sum(id{}) FROM agg", "::integer".repeat(40));
    assert!(s.prepare(&q).is_err());
}
