fn dd68_deferred_surface_redacted(message: &str, detail: &str) -> bool {
    [
        "SELECT",
        "nosuchfn",
        "LOWER",
        "pg_catalog",
        "public",
        "coalesce",
        "nosuchagg",
        "avg",
        "count",
        "WHERE",
        "nosuchschema",
        "lower",
        "information_schema",
        "_pg_expandarray",
        "pg_toast",
        "FROM other",
        "nextval",
        "deferred_secret_id_seq",
        "'a'",
        "1/0",
        "1,2",
    ]
    .iter()
    .all(|secret| !message.contains(secret) && !detail.contains(secret))
}

#[tokio::test]
async fn dd68_deferred_routine_surface_app_four_paths() {
    let f = Fixture::new().await;
    let mut setup = f.admin.sql_session();
    setup
        .execute(
            "CREATE TABLE deferred_secret (id serial PRIMARY KEY)",
            &[],
            None,
        )
        .unwrap();
    drop(setup);
    let (pg, conn) = f.pg(f.app.clone()).await;
    let mut raw = raw_pg(&f).await;
    // DD-0068 F9: pgwire follows PG 18 (42883/3F000) for deferred-routine-only queries; native surfaces keep 42501.
    let probes = [
        ("SELECT nosuchfn()", "42883"),
        ("SELECT \"LOWER\"('a')", "42883"),
        ("SELECT pg_catalog.nosuchfn()", "42883"),
        ("SELECT public.coalesce(1,2)", "42883"),
        ("SELECT nosuchagg(1)", "42883"),
        ("SELECT avg(nosuchfn(1))", "42883"),
        ("SELECT count(DISTINCT nosuchfn(1))", "42883"),
        ("SELECT count(*), nosuchfn()", "42883"),
        ("SELECT 1 WHERE nosuchfn(1)=1", "42883"),
        ("SELECT nosuchschema.lower('a')", "3F000"),
        ("SELECT nosuchschema.lower(1/0)", "3F000"),
        ("SELECT information_schema._pg_expandarray(1)", "0A000"),
        ("SELECT pg_toast.lower('a')", "42501"),
        // Deliberately fail closed on a column operand; PG 18 gives 42703.
        ("SELECT nosuchfn(x)", "42501"),
        ("SELECT nosuchfn(x) FROM other", "42501"),
        (
            "SELECT nextval('deferred_secret_id_seq'), nosuchfn()",
            "42501",
        ),
    ];
    for (probe, (sql, expected)) in probes.iter().enumerate() {
        for (method, result) in [
            ("simple_query", pg.simple_query(sql).await.map(|_| ())),
            ("prepare", pg.prepare(sql).await.map(|_| ())),
        ] {
            let error = result.expect_err(&format!("method={method} probe={probe} sql={sql}"));
            let db = error.as_db_error().unwrap();
            assert!(
                dd68_deferred_surface_redacted(db.message(), db.detail().unwrap_or("")),
                "method={method} probe={probe}: SQL leaked"
            );
            assert_eq!(
                db.code().code(),
                *expected,
                "method={method} probe={probe} sql={sql}"
            );
        }
        for statement_describe in [false, true] {
            pg_send(&mut raw, b'P', &parse("", sql)).await;
            let method = if statement_describe {
                pg_send(&mut raw, b'D', b"S\0").await;
                "raw_statement_describe"
            } else {
                pg_send(&mut raw, b'B', &bind("", "", &[], false)).await;
                pg_send(&mut raw, b'D', b"P\0").await;
                pg_send(&mut raw, b'E', &execute("", 0)).await;
                "raw_portal_execute"
            };
            let frames = pg_sync(&mut raw).await;
            let errors: Vec<_> = frames.iter().filter(|(tag, _)| *tag == b'E').collect();
            assert_eq!(errors.len(), 1, "method={method} probe={probe}");
            let field = |tag: u8| {
                errors[0]
                    .1
                    .split(|byte| *byte == 0)
                    .find_map(|part| part.strip_prefix(&[tag]))
                    .map(|part| String::from_utf8_lossy(part).into_owned())
                    .unwrap_or_default()
            };
            assert!(
                dd68_deferred_surface_redacted(&field(b'M'), &field(b'D')),
                "method={method} probe={probe}: SQL leaked"
            );
            assert_eq!(
                field(b'C'),
                *expected,
                "method={method} probe={probe} sql={sql}"
            );
            assert_eq!(
                frames.last().map(|(tag, _)| *tag),
                Some(b'Z'),
                "method={method} probe={probe}"
            );
        }
    }
    drop(raw);
    drop(pg);
    conn.await.unwrap();
    f.stop().await;
}

#[tokio::test]
async fn dd68_deferred_routine_surface_task_simple_query_keeps_authority() {
    let f = Fixture::new().await;
    let (pg, conn) = f.pg(f.task.clone()).await;
    for sql in ["SELECT nosuchfn()", "SELECT nosuchschema.lower('a')"] {
        let error = pg.simple_query(sql).await.unwrap_err();
        let db = error.as_db_error().unwrap();
        assert!(
            dd68_deferred_surface_redacted(db.message(), db.detail().unwrap_or("")),
            "task sql={sql}: SQL leaked"
        );
        assert_eq!(db.code().code(), "42501", "task sql={sql}");
    }
    drop(pg);
    conn.await.unwrap();
    f.stop().await;
}
