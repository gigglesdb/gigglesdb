#[tokio::test]
async fn quoting_text_parse0_describe_bind_null_and_scoped_errors() {
    let f = Fixture::new().await;
    let mut raw = raw_pg(&f).await;
    let mut report = Vec::new();
    for (query, input, expected) in [
        (
            "SELECT pg_catalog.\"quote_ident\"($1)",
            Some("select"),
            Some("\"select\""),
        ),
        ("SELECT quote_ident($1)", Some("abort"), Some("abort")),
        (
            "SELECT quote_ident($1)",
            Some("\u{00e9}"),
            Some("\"\u{00e9}\""),
        ),
        (
            "SELECT quote_literal($1)",
            Some(r"a'\b"),
            Some(r"E'a''\\b'"),
        ),
        (
            "SELECT quote_nullable($1)",
            Some(r"a'\b"),
            Some(r"E'a''\\b'"),
        ),
        ("SELECT quote_nullable($1)", None, Some("NULL")),
        ("SELECT quote_literal($1)", None, None),
        ("SELECT quote_ident($1)", None, None),
    ] {
        pg_send(&mut raw, b'P', &parse("quote", query)).await;
        pg_send(&mut raw, b'D', b"Squote\0").await;
        let describe = pg_sync(&mut raw).await;
        assert!(
            !describe.iter().any(|f| f.0 == b'E'),
            "{query}: {describe:?}"
        );
        assert!(describe.iter().any(|f| f.0 == b'1'));
        let types = &describe.iter().find(|f| f.0 == b't').unwrap().1;
        assert_eq!(&types[..2], &1u16.to_be_bytes());
        assert_eq!(&types[2..], &25u32.to_be_bytes());
        let columns = &describe.iter().find(|f| f.0 == b'T').unwrap().1;
        assert_eq!(&columns[..2], &1u16.to_be_bytes());
        let end = columns[2..].iter().position(|b| *b == 0).unwrap() + 2;
        assert_eq!(&columns[end + 7..end + 11], &25u32.to_be_bytes());
        for binary in [false, true] {
            pg_send(&mut raw, b'B', &bind("", "quote", &[input], binary)).await;
            pg_send(&mut raw, b'E', &execute("", 0)).await;
            let frames = pg_sync(&mut raw).await;
            assert!(!frames.iter().any(|f| f.0 == b'E'), "{query}: {frames:?}");
            assert!(frames.iter().any(|f| f.0 == b'2'));
            let row = &frames.iter().find(|f| f.0 == b'D').unwrap().1;
            assert_eq!(&row[..2], &1u16.to_be_bytes());
            if let Some(expected) = expected {
                assert_eq!(&row[2..6], &(expected.len() as i32).to_be_bytes());
                assert_eq!(&row[6..], expected.as_bytes());
            } else {
                assert_eq!(&row[2..], &(-1i32).to_be_bytes());
            }
            report.push(serde_json::json!({"query":query,"input":input,"expected":expected,"binary_result":binary,"describe":describe.iter().map(|f|serde_json::json!({"tag":char::from(f.0).to_string(),"hex":hex::encode(&f.1)})).collect::<Vec<_>>(),"bind_execute":frames.iter().map(|f|serde_json::json!({"tag":char::from(f.0).to_string(),"hex":hex::encode(&f.1)})).collect::<Vec<_>>() }));
        }
        pg_send(&mut raw, b'C', b"Squote\0").await;
        assert!(!pg_sync(&mut raw).await.iter().any(|f| f.0 == b'E'));
    }
    let before_errors = f.admin.snapshot().unwrap();
    for (query, state) in [
        ("SELECT quote_ident(1)", "42883"),
        ("SELECT quote_ident()", "42883"),
        ("SELECT quote_literal(1)", "0A000"),
        ("SELECT quote_nullable(NULL::integer)", "0A000"),
        ("SELECT quote_literal($1::bytea)", "0A000"),
        ("SELECT quote_ident('a','b')", "42883"),
        ("SELECT quote_nullable(missing,1) FROM app", "42703"),
        ("SELECT quote_ident($1::text,1)", "42883"),
        ("SELECT public.quote_ident('a')", "42501"),
        ("SELECT \"QUOTE_IDENT\"('a')", "0A000"),
        ("SELECT \"QUOTE_IDENT\"('a') FROM app", "0A000"),
        // Unknown arity retains generic parameter finalization (PG42883).
        // Missing no-FROM columns fail the positive authority proof; only
        // the authorized FROM case reaches the inherited priority gap (PG42703).
        ("SELECT quote_literal($1,1)", "42804"),
        ("SELECT quote_literal($1,missing)", "42501"),
        ("SELECT quote_literal($1,missing) FROM app", "42804"),
    ] {
        let actual = temporal_state_extended(&mut raw, query).await;
        report.push(
            serde_json::json!({"query":query,"expected_profile_state":state,"actual":actual}),
        );
        let trace_root = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
            .join("dd68")
            .join("dd68-quoting-functions-20261106")
            .join("wire-traces");
        std::fs::create_dir_all(&trace_root).unwrap();
        std::fs::write(
            trace_root.join(format!("quote-wire-progress-{}.json", std::process::id())),
            serde_json::to_vec_pretty(&report).unwrap(),
        )
        .unwrap();
        assert_eq!(actual["sqlstate"], state, "{query}: {actual}");
        assert_eq!(actual["stage"], "parse");
        assert_eq!(actual["data_rows"], 0);
        assert_eq!(actual["tags"], serde_json::json!(["E", "Z"]));
    }
    assert_eq!(f.admin.snapshot().unwrap(), before_errors);
    let root = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("dd68")
        .join("dd68-quoting-functions-20261106")
        .join("wire-traces");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join(format!("quote-wire-{}.json", std::process::id()));
    std::fs::write(&path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    eprintln!("QUOTING_WIRE_MARKER_v2={}", path.display());
    pg_send(&mut raw, b'X', &[]).await;
    drop(raw);
    f.stop().await;
}

#[tokio::test]
async fn quoting_application_rows_aggregates_and_task_authority() {
    let f = Fixture::new().await;
    let (pg, conn) = f.pg(f.app.clone()).await;
    pg.execute("INSERT INTO app(id,body) VALUES(1,$1)", &[&Some("select")])
        .await
        .unwrap();
    pg.execute(
        "INSERT INTO app(id,body) VALUES(2,$1)",
        &[&Option::<&str>::None],
    )
    .await
    .unwrap();
    let rows=pg.query("SELECT quote_ident(body),quote_literal(body),quote_nullable(body) FROM app ORDER BY id",&[]).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>(0), "\"select\"");
    assert_eq!(rows[0].get::<_, String>(1), "'select'");
    assert_eq!(rows[1].get::<_, Option<String>>(0), None);
    assert_eq!(rows[1].get::<_, Option<String>>(1), None);
    assert_eq!(rows[1].get::<_, String>(2), "NULL");
    let counts = pg
        .query_one(
            "SELECT count(quote_nullable(body)),count(quote_literal(body)) FROM app",
            &[],
        )
        .await
        .unwrap();
    assert_eq!((counts.get::<_, i64>(0), counts.get::<_, i64>(1)), (2, 1));
    let row = pg
        .query_one(
            "SELECT quote_nullable(body) FROM app WHERE quote_nullable(body)='NULL'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "NULL");
    let before = f.admin.snapshot().unwrap();
    for query in [
        "SELECT quote_ident(body) FROM other",
        "SELECT quote_nullable(NULL) FROM other",
        "SELECT quote_literal('x') FROM other",
        "SELECT sum(length(quote_nullable(NULL)))",
    ] {
        assert_eq!(
            code_from_character_error(pg.prepare(query).await.unwrap_err()),
            "42501",
            "{query}"
        );
    }
    assert_eq!(f.admin.snapshot().unwrap(), before);
    drop(pg);
    conn.await.unwrap();
    let mut task = prepared_network::raw_task(&f).await;
    for query in [
        "SELECT quote_ident('safe')",
        "SELECT quote_literal('x')",
        "SELECT quote_nullable(NULL)",
        "SELECT quote_literal(body) FROM app",
    ] {
        let actual = temporal_state_extended(&mut task, query).await;
        assert_eq!(actual["sqlstate"], "42501");
        assert_eq!(actual["stage"], "parse");
        assert_eq!(actual["data_rows"], 0);
        assert_eq!(actual["tags"], serde_json::json!(["E", "Z"]));
    }
    assert_eq!(f.admin.snapshot().unwrap(), before);
    pg_send(&mut task, b'X', &[]).await;
    drop(task);
    f.stop().await;
}

#[tokio::test]
async fn quoting_constant_expansion_budget_bind_is_readonly() {
    use sha2::Digest;
    let f = Fixture::new().await;
    let mut raw = raw_pg(&f).await;
    let before = f.admin.snapshot().unwrap();
    let mut report = vec![];
    for suffix in ["", " WHERE FALSE", " LIMIT 0"] {
        let query = format!(
            "SELECT quote_ident('{}'){suffix}",
            "\"".repeat(giggles_core::relational::MAX_ROW_BYTES / 2)
        );
        let actual = temporal_state_extended(&mut raw, &query).await;
        report.push(serde_json::json!({"query_sha256":format!("{:x}",sha2::Sha256::digest(query.as_bytes())),"query_bytes":query.len(),"suffix":suffix,"actual":actual,"comparison":"Giggles resource cap only, not PG error parity"}));
        assert_eq!(actual["sqlstate"], "54000");
        assert_eq!(actual["stage"], "bind");
        assert_eq!(actual["data_rows"], 0);
    }
    let root = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("dd68")
        .join("dd68-quoting-functions-20261106")
        .join("wire-traces");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(
        root.join(format!("quote-budget-{}.json", std::process::id())),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    assert_eq!(f.admin.snapshot().unwrap(), before);
    pg_send(&mut raw, b'X', &[]).await;
    drop(raw);
    f.stop().await;
}
