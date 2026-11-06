use giggles_pg_catalog::{CatalogSnapshot, Error, execute, plan};

#[test]
fn table_free_keepalive_selects_are_real_typed_queries() {
    use giggles_pg_catalog::{PgType, Value};
    for sql in [
        "SELECT 1",
        "/* keep alive */ select 1;",
        "SELECT 1 AS alive",
    ] {
        let p = plan(sql)
            .unwrap()
            .expect("table-free SELECT must route to evaluator");
        let r = execute(&p, &CatalogSnapshot::default(), &[]).unwrap();
        assert_eq!(r.fields[0].data_type, PgType::Int4);
        assert_eq!(r.rows, vec![vec![Value::Int(1)]]);
    }
    let p = plan("SELECT 1 WHERE false").unwrap().unwrap();
    assert!(
        execute(&p, &CatalogSnapshot::default(), &[])
            .unwrap()
            .rows
            .is_empty()
    );
    assert!(plan("SELECT 1; SELECT 2").is_err());
    assert!(plan("SELECT 1; DELETE FROM app").is_err());
    assert!(plan("SELECT pg_sleep(1)").is_err());
    assert!(plan("SELECT id FROM app").unwrap().is_none());
}

#[test]
fn zero_limit_never_returns_a_row_even_for_identity_or_derived_sources() {
    for sql in [
        "SELECT current_database() LIMIT 0",
        "SELECT relname FROM pg_catalog.pg_class LIMIT 0",
        "SELECT x.relname FROM (SELECT relname FROM pg_catalog.pg_class LIMIT 0) x",
    ] {
        let p = plan(sql).unwrap().unwrap();
        assert!(
            execute(&p, &CatalogSnapshot::default(), &[])
                .unwrap()
                .rows
                .is_empty(),
            "{sql}"
        );
    }
}

#[test]
fn unsupported_distinct_on_is_not_silently_treated_as_full_row_distinct() {
    assert!(matches!(
        plan(
            "SELECT DISTINCT ON (relkind) relkind, relname FROM pg_catalog.pg_class ORDER BY relkind, relname"
        ),
        Err(Error::Unsupported)
    ));
}
