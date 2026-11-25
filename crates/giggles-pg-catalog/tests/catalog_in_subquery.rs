use giggles_pg_catalog::*;

fn run(sql: &str) -> Result<ResultSet, Error> {
    execute(
        &plan(sql)?.expect("catalog query"),
        &CatalogSnapshot::default(),
        &[],
    )
}

#[test]
fn three_valued_truth_tables() {
    for (lhs, rhs, expected) in [
        (
            "NULL::int4",
            "SELECT n.oid FROM pg_namespace n WHERE false",
            Value::Bool(false),
        ),
        (
            "NULL::int4",
            "SELECT n.oid FROM pg_namespace n WHERE n.oid=11",
            Value::Null,
        ),
        (
            "11",
            "SELECT n.oid FROM pg_namespace n WHERE n.oid=11 UNION ALL SELECT NULL::oid",
            Value::Bool(true),
        ),
        (
            "12",
            "SELECT n.oid FROM pg_namespace n WHERE n.oid=11 UNION ALL SELECT NULL::oid",
            Value::Null,
        ),
        (
            "12",
            "SELECT n.oid FROM pg_namespace n WHERE n.oid=11",
            Value::Bool(false),
        ),
        (
            "11",
            "SELECT NULL::oid UNION ALL SELECT n.oid FROM pg_namespace n WHERE n.oid=11",
            Value::Bool(true),
        ),
    ] {
        for not in [false, true] {
            let sql = format!(
                "SELECT {lhs} {}IN ({rhs}) FROM pg_namespace z WHERE z.oid=11",
                if not { "NOT " } else { "" }
            );
            let expected = match &expected {
                Value::Bool(b) => Value::Bool(*b ^ not),
                v => v.clone(),
            };
            let out = run(&sql).unwrap();
            assert_eq!(out.fields[0].data_type, PgType::Bool);
            assert_eq!(out.rows, vec![vec![expected]], "{sql}");
        }
    }
}

#[test]
fn correlated_and_uncorrelated_ctes() {
    let out = run("WITH n AS (SELECT n.oid FROM pg_namespace n WHERE n.oid IN (11,2200)) SELECT a.oid,a.oid IN (SELECT b.oid FROM n b WHERE b.oid=a.oid),a.oid NOT IN (SELECT b.oid FROM n b WHERE b.oid=11) FROM n a ORDER BY a.oid").unwrap();
    assert_eq!(
        out.rows,
        vec![
            vec![Value::Int(11), Value::Bool(true), Value::Bool(false)],
            vec![Value::Int(2200), Value::Bool(true), Value::Bool(true)]
        ]
    );
}

#[test]
fn parameters_and_projection_override_guard() {
    let p = plan("SELECT z.oid FROM pg_namespace z WHERE $1 IN (SELECT n.oid FROM pg_namespace n WHERE n.oid=$2)").unwrap().unwrap();
    assert_eq!(p.parameter_types(), &[PgType::Oid, PgType::Oid]);
    assert!(p.allows_numeric_input_override());
    assert_eq!(
        execute(
            &p,
            &CatalogSnapshot::default(),
            &[Value::Int(11), Value::Int(11)]
        )
        .unwrap()
        .rows
        .len(),
        4
    );
    let p = plan("SELECT z.oid FROM pg_namespace z WHERE z.oid IN (SELECT $1::oid FROM pg_namespace n WHERE n.oid=11)").unwrap().unwrap();
    assert!(!p.allows_numeric_input_override());
    assert_eq!(p.parameter_types(), &[PgType::Oid]);
}

#[test]
fn primitive_families_and_legacy_predicates() {
    for sql in [
        "SELECT true IN (SELECT true FROM pg_namespace n WHERE n.oid=11) FROM pg_namespace z WHERE z.oid=11",
        "SELECT 'pg_catalog'::text IN (SELECT n.nspname FROM pg_namespace n WHERE n.oid=11) FROM pg_namespace z WHERE z.oid=11",
        "SELECT 11::int8 IN (SELECT n.oid FROM pg_namespace n WHERE n.oid=11) FROM pg_namespace z WHERE z.oid=11",
        "SELECT 'x'::char IN (SELECT 'x'::name FROM pg_namespace n WHERE n.oid=11) FROM pg_namespace z WHERE z.oid=11",
        "SELECT 11=ANY(ARRAY[11,12]),11 IN (11,12) FROM pg_namespace z WHERE z.oid=11",
    ] {
        let out = run(sql).unwrap();
        assert!(out.rows[0].iter().all(|v| *v == Value::Bool(true)), "{sql}");
    }
}

#[test]
fn field_type_authority_and_mutation_refusals() {
    for sql in [
        "SELECT 11 IN (SELECT n.oid,n.nspname FROM pg_namespace n) FROM pg_namespace",
        "SELECT ARRAY[11] IN (SELECT ARRAY[11] FROM pg_namespace) FROM pg_namespace",
        "SELECT 11::numeric IN (SELECT 11::numeric FROM pg_namespace) FROM pg_namespace",
        "SELECT 11::float8 IN (SELECT 11::float8 FROM pg_namespace) FROM pg_namespace",
        "SELECT 11 IN (SELECT t.prosupport FROM pg_proc t) FROM pg_namespace",
        "SELECT (11,12) IN (SELECT (11,12) FROM pg_namespace) FROM pg_namespace",
        "SELECT true IN (SELECT n.oid FROM pg_namespace n) FROM pg_namespace",
        "SELECT 11 IN (SELECT oid FROM secret_table) FROM pg_namespace",
        "SELECT 11 IN (SELECT oid FROM pg_unknown) FROM pg_namespace",
        "WITH x AS (DELETE FROM pg_namespace RETURNING oid) SELECT 11 IN (SELECT oid FROM x)",
    ] {
        assert!(plan(sql).is_err(), "{sql}");
    }
}

#[test]
fn shared_work_and_output_limits_return_no_result() {
    for sql in [
        "SELECT a.oid IN (SELECT b.oid FROM pg_type b WHERE b.oid=a.oid) FROM pg_type a,pg_type c",
        "SELECT oid IN (SELECT a.oid FROM pg_type a,pg_type b) FROM pg_namespace",
        "SELECT oid IN (SELECT 11 FROM generate_series(1,8192)) FROM pg_namespace,generate_series(1,8192)",
    ] {
        assert_eq!(run(sql), Err(Error::Limit), "{sql}");
    }
    let sql = format!(
        "SELECT 'x' IN (SELECT '{}' FROM generate_series(1,8192)) FROM pg_namespace",
        "x".repeat(1024)
    );
    assert_eq!(run(&sql), Err(Error::Limit));
    assert_eq!(
        run(
            "SELECT a.n IN (SELECT b.n FROM generate_series(1,1000) b(n) WHERE b.n=0) FROM generate_series(1,1000) a(n),pg_namespace p WHERE p.oid=11"
        ),
        Err(Error::Limit)
    );
}
