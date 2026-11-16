use giggles_pg_catalog::explain_query;

#[test]
fn shares_nonpretty_view_expression_deparse() {
    for (predicate, expected) in [
        ("active", "active"),
        ("NOT active", "(NOT active)"),
        ("v = 'a'", "(v = 'a'::text)"),
        ("id = 1", "(id = 1)"),
    ] {
        let shape = explain_query(&format!(
            "EXPLAIN (COSTS OFF) SELECT id FROM ex WHERE {predicate}"
        ))
        .unwrap()
        .unwrap();
        assert_eq!(shape.filter.as_deref(), Some(expected));
    }
}

#[test]
fn other_explain_options_keep_the_existing_route() {
    for sql in [
        "EXPLAIN SELECT 1",
        "EXPLAIN (COSTS OFF, VERBOSE) SELECT id FROM ex",
        "EXPLAIN (COSTS OFF, FORMAT JSON) SELECT 1",
        "EXPLAIN ANALYZE SELECT 1",
    ] {
        assert!(explain_query(sql).unwrap().is_none(), "{sql}");
    }
}
