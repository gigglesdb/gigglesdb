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
fn explain_option_routes() {
    // Host-dependent costs and timing remain unsupported.
    for sql in ["EXPLAIN SELECT 1", "EXPLAIN ANALYZE SELECT 1"] {
        assert!(
            matches!(explain_query(sql), Err(giggles_pg_catalog::Error::Unsupported)),
            "{sql}"
        );
    }

    // PG18: "Seq Scan on public.ex\n  Output: id". The server renders these
    // rows from the catalog shape; explain_query itself returns no text rows.
    let verbose = explain_query("EXPLAIN (COSTS OFF, VERBOSE) SELECT id FROM ex")
        .unwrap()
        .unwrap();
    assert!(verbose.verbose);
    assert!(!verbose.json);
    assert!(!verbose.analyze);
    assert_eq!(verbose.table.as_deref(), Some("ex"));
    assert_eq!(verbose.output, ["id"]);

    // PG18 JSON:
    // [
    //   {
    //     "Plan": {
    //       "Node Type": "Result",
    //       "Parallel Aware": false,
    //       "Async Capable": false,
    //       "Disabled": false
    //     }
    //   }
    // ]
    let json = explain_query("EXPLAIN (COSTS OFF, FORMAT JSON) SELECT 1")
        .unwrap()
        .unwrap();
    assert!(json.json);
    assert!(!json.verbose);
    assert!(!json.analyze);
    assert_eq!(json.table, None);
    assert_eq!(json.output, ["1"]);
}
