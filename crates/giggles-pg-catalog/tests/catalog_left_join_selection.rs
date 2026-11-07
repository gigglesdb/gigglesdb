use giggles_pg_catalog::*;

fn run(sql: &str) -> Result<ResultSet, Error> {
    execute(&plan(sql)?.unwrap(), &CatalogSnapshot::default(), &[])
}

#[test]
fn preserved_input_selection_precedes_outer_join_materialization() {
    let r = run("SELECT t.oid,e.oid FROM pg_type t LEFT JOIN pg_type e ON e.oid=0 WHERE t.oid=23")
        .unwrap();
    assert_eq!(r.rows, vec![vec![Value::Int(23), Value::Null]]);
    let r = run("SELECT t.oid,e.oid FROM pg_type t,pg_type e LEFT JOIN pg_type n ON n.oid=0 WHERE t.oid=23 AND e.oid=23").unwrap();
    assert_eq!(r.rows, vec![vec![Value::Int(23), Value::Int(23)]]);
    let r = run("SELECT t.oid,e.oid,n.oid FROM pg_type t LEFT JOIN pg_type e ON e.oid=0 LEFT JOIN pg_type n ON n.oid=0 WHERE t.oid=23").unwrap();
    assert_eq!(r.rows, vec![vec![Value::Int(23), Value::Null, Value::Null]]);
}

#[test]
fn null_extended_selection_stays_above_join() {
    assert!(
        run(
            "SELECT t.oid FROM pg_type t LEFT JOIN pg_type e ON e.oid=0 WHERE t.oid=23 AND e.oid=23"
        )
        .unwrap()
        .rows
        .is_empty()
    );
    assert!(matches!(
        run("SELECT t.oid FROM pg_type t LEFT JOIN pg_type e ON true WHERE e.oid=23"),
        Err(Error::Limit)
    ));
}

#[test]
fn explicit_join_on_cannot_reference_prior_comma_items() {
    assert!(matches!(
        plan("SELECT a.oid FROM pg_type a,pg_type b,pg_type c LEFT JOIN pg_type d ON a.oid=d.oid"),
        Err(Error::Unsupported)
    ));
    assert!(
        plan("SELECT a.oid FROM pg_type a,pg_type b,pg_type c LEFT JOIN pg_type d ON c.oid=d.oid")
            .is_ok()
    );
}

#[test]
fn preserved_comparison_in_and_immutable_call_push_below_left_join() {
    for predicate in [
        "t.oid<24 AND t.oid>22",
        "t.oid IN (23)",
        "upper(t.typname)='INT4'",
        "NOT (t.oid<>23)",
        "t.oid IS NOT NULL AND t.oid=23",
    ] {
        let sql =
            format!("SELECT t.oid FROM pg_type t LEFT JOIN pg_type e ON true WHERE {predicate}");
        let r = run(&sql).unwrap();
        assert!(
            r.rows.iter().all(|row| row == &vec![Value::Int(23)]),
            "{predicate}"
        );
        assert!(!r.rows.is_empty());
    }
}

#[test]
fn single_values_row_can_participate_in_catalog_union() {
    let r = run("SELECT oid FROM pg_class WHERE false UNION ALL VALUES (23::oid)").unwrap();
    assert_eq!(r.rows, vec![vec![Value::Int(23)]]);
}

#[test]
fn nested_on_subquery_alias_shadows_prior_comma_operand() {
    assert!(plan("SELECT a.oid FROM pg_type a,pg_type b LEFT JOIN pg_type c ON c.oid=(SELECT a.oid FROM pg_type a WHERE a.oid=23)").is_ok());
    assert!(matches!(
        plan("SELECT a.oid FROM pg_type a,pg_type b LEFT JOIN pg_type c ON c.oid=(SELECT a.oid)"),
        Err(Error::Unsupported)
    ));
}

#[test]
fn preserved_regex_pushes_but_subquery_and_session_call_stay_above() {
    let r =
        run("SELECT t.oid FROM pg_type t LEFT JOIN pg_type e ON true WHERE t.typname ~ '^int4$'")
            .unwrap();
    assert!(!r.rows.is_empty());
    assert!(r.rows.iter().all(|row| row == &vec![Value::Int(23)]));
    for predicate in ["t.oid=(SELECT 23::oid)", "current_user='giggles'"] {
        let sql =
            format!("SELECT t.oid FROM pg_type t LEFT JOIN pg_type e ON true WHERE {predicate}");
        assert!(matches!(run(&sql), Err(Error::Limit)), "{predicate}");
    }
}

#[test]
fn series_source_can_bind_a_correlated_outer_argument_and_scalar_alias() {
    let r = run("SELECT (SELECT s FROM generate_series(t.oid::int4,t.oid::int4) s) FROM pg_type t WHERE t.oid=23").unwrap();
    assert_eq!(r.rows, vec![vec![Value::Int(23)]]);
}

#[test]
fn same_from_series_correlation_remains_unsupported() {
    for sql in [
        "SELECT t.oid,s FROM pg_type t CROSS JOIN generate_series(t.oid::int4,t.oid::int4) s WHERE t.oid=23",
        "SELECT t.oid,s FROM pg_type t,generate_series(t.oid::int4,t.oid::int4) s WHERE t.oid=23",
    ] {
        assert!(matches!(plan(sql), Err(Error::Unsupported)), "{sql}");
    }
}

#[test]
fn local_series_alias_shadow_cannot_fall_through_to_outer_query() {
    for sql in [
        "SELECT (SELECT s FROM pg_type t CROSS JOIN generate_series(t.oid::int4,t.oid::int4) s) FROM pg_type t WHERE t.oid=23",
        "SELECT (SELECT s FROM pg_type t,generate_series(t.oid::int4,t.oid::int4) s) FROM pg_type t WHERE t.oid=23",
        "SELECT (SELECT s FROM pg_type x CROSS JOIN generate_series(oid::int4,oid::int4) s) FROM pg_type t WHERE t.oid=23",
    ] {
        assert!(matches!(plan(sql), Err(Error::Unsupported)), "{sql}");
    }
}
