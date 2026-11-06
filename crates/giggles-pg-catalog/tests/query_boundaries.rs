use giggles_pg_catalog::{CatalogSnapshot, Error, execute, plan};

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
