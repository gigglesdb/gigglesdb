#[tokio::test]
async fn integer_formats_pg18_width_binary_bind_and_relational_contexts() {
    let f = Fixture::new().await;
    let (pg, connection) = f.pg(f.app.clone()).await;
    let p = pg
        .prepare(
            "SELECT to_hex($1::integer),to_hex($2::bigint),to_bin($1::integer),to_oct($2::bigint)",
        )
        .await
        .unwrap();
    assert_eq!(
        p.params().iter().map(|t| t.oid()).collect::<Vec<_>>(),
        vec![23, 20]
    );
    assert!(p.columns().iter().all(|c| c.type_().oid() == 25));
    for (small, big, expected) in [
        (
            Some(-1i32),
            Some(-1i64),
            [
                Some("ffffffff"),
                Some("ffffffffffffffff"),
                Some("11111111111111111111111111111111"),
                Some("1777777777777777777777"),
            ],
        ),
        (Some(0), Some(0), [Some("0"); 4]),
        (
            Some(i32::MIN),
            Some(i64::MIN),
            [
                Some("80000000"),
                Some("8000000000000000"),
                Some("10000000000000000000000000000000"),
                Some("1000000000000000000000"),
            ],
        ),
        (None, None, [None; 4]),
    ] {
        let rows = pg.query(&p, &[&small, &big]).await.unwrap();
        for (index, expected) in expected.into_iter().enumerate() {
            assert_eq!(rows[0].get::<_, Option<String>>(index).as_deref(), expected);
        }
    }
    for name in ["to_hex", "to_bin", "to_oct"] {
        let prepared = pg
            .prepare(&format!("SELECT pg_catalog.\"{name}\"(-1::integer)"))
            .await
            .unwrap();
        assert_eq!(prepared.columns()[0].type_().oid(), 25);
        assert!(
            !pg.query(&prepared, &[]).await.unwrap()[0]
                .get::<_, String>(0)
                .is_empty()
        );
    }
    pg.execute("INSERT INTO app(id,body,num) VALUES(1,'format',-1)", &[])
        .await
        .unwrap();
    let row = pg
        .query_one(
            "SELECT min(to_hex(num)),max(to_bin(id)),count(to_oct(num)) FROM app",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "ffffffff");
    assert_eq!(row.get::<_, String>(1), "1");
    assert_eq!(row.get::<_, i64>(2), 1);
    assert_eq!(pg.query_one("SELECT to_hex(num) FROM app WHERE id=1 AND to_oct(num)='37777777777' ORDER BY to_bin(num)",&[]).await.unwrap().get::<_,String>(0),"ffffffff");
    drop(pg);
    connection.await.unwrap();
    f.stop().await;
}

#[tokio::test]
async fn integer_formats_parse_ambiguity_child_priority_and_authority() {
    let f = Fixture::new().await;
    let mut raw = raw_pg(&f).await;
    for name in ["to_hex", "to_bin", "to_oct"] {
        for argument in ["$1", "NULL", "'15'", "1::smallint", "NULL::smallint"] {
            let query = format!("SELECT {name}({argument})");
            let result = temporal_state_extended(&mut raw, &query).await;
            assert_eq!(result["sqlstate"], "42725", "{query}: {result}");
            assert_eq!(result["stage"], "parse");
            assert_eq!(result["tags"], serde_json::json!(["E", "Z"]));
        }
        let result =
            temporal_state_extended(&mut raw, &format!("SELECT {name}(missing,1) FROM app")).await;
        assert_eq!(result["sqlstate"], "42703");
        assert_eq!(result["stage"], "parse");
    }
    pg_send(&mut raw, b'X', &[]).await;
    drop(raw);
    let (app, app_connection) = f.pg(f.app.clone()).await;
    for query in [
        "SELECT to_hex(TRUE)",
        "SELECT to_bin(1::numeric)",
        "SELECT to_oct()",
    ] {
        assert_eq!(
            code(app.prepare(query).await.unwrap_err()),
            "42883",
            "{query}"
        );
    }
    for query in ["SELECT to_hex(id) FROM other", "SELECT min(to_hex(1))"] {
        assert_eq!(
            code(app.prepare(query).await.unwrap_err()),
            "42501",
            "{query}"
        );
    }
    // DD-0068 F9: pgwire follows PG 18 (42883/3F000) for deferred-routine-only queries; native surfaces keep 42501.
    assert_eq!(
        code(app.prepare("SELECT public.to_hex(1)").await.unwrap_err()),
        "42883"
    );
    assert_eq!(
        code(app.prepare("SELECT \"TO_HEX\"(1)").await.unwrap_err()),
        "42883"
    );
    assert_eq!(
        code(
            app.prepare("SELECT \"TO_HEX\"(1) FROM app")
                .await
                .unwrap_err()
        ),
        "42883"
    );
    drop(app);
    app_connection.await.unwrap();
    let (task, task_connection) = f.pg(f.task.clone()).await;
    for query in [
        "SELECT to_hex(1)",
        "SELECT to_bin(1)",
        "SELECT to_oct(id) FROM app",
    ] {
        assert_eq!(
            code(task.prepare(query).await.unwrap_err()),
            "42501",
            "{query}"
        );
    }
    drop(task);
    task_connection.await.unwrap();
    f.stop().await;
}
#[tokio::test]
async fn integer_text_decode_pg18_original57_regression_corpus() {
    let f = Fixture::new().await;
    let mut raw = raw_pg(&f).await;
    let corpus: serde_json::Value = serde_json::from_slice(
        &std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/dd68/integer-formats-wire-classification-cases.json"
        ))
        .unwrap(),
    )
    .unwrap();
    let mut report = Vec::new();
    for case in corpus["cases"].as_array().unwrap() {
        let query = case["query"].as_str().unwrap();
        let parameter = case["parameter"].as_str().unwrap();
        pg_send(&mut raw, b'P', &parse("", query)).await;
        pg_send(&mut raw, b'D', b"S\0").await;
        let described = pg_sync(&mut raw).await;
        assert!(described.iter().any(|f| f.0 == b'1'));
        let mut types = vec![0, 1];
        types.extend((case["oid"].as_u64().unwrap() as u32).to_be_bytes());
        assert!(
            described.iter().any(|f| f.0 == b't' && f.1 == types),
            "{case}"
        );
        pg_send(&mut raw, b'B', &bind("", "", &[Some(parameter)], false)).await;
        let bound = pg_sync(&mut raw).await;
        let actual = temporal_state_frames(&bound, "bind");
        report.push(serde_json::json!({"case":case,"observed":actual,"frames":bound.iter().map(|f|serde_json::json!({"tag":char::from(f.0).to_string(),"hex":hex::encode(&f.1)})).collect::<Vec<_>>()}));
        assert_eq!(
            actual["sqlstate"], case["pg_sqlstate"],
            "{case}: {actual}"
        );
        assert_eq!(
            bound.iter().any(|f| f.0 == b'2'),
            case["pg_sqlstate"].is_null()
        );
    }
    let root = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("dd68")
        .join("dd68-integer-formats-20261106")
        .join("qualification")
        .join("traces");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(
        root.join(format!("text-classification-{}.json", std::process::id())),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    println!(
        "original57_pg18_regression_cases={}, historical_retained_grammar_gaps={}",
        corpus["eligible"], corpus["known_gaps"]
    );
    pg_send(&mut raw, b'X', &[]).await;
    drop(raw);
    f.stop().await;
}

#[tokio::test]
async fn integer_text_overflow_aborts_transaction_without_portal_or_effects() {
    let f = Fixture::new().await;
    let mut raw = raw_pg(&f).await;
    assert_eq!(
        temporal_state_frames(&temporal_state_cycle(&mut raw, "BEGIN").await, "query")["ready"],
        "T"
    );
    assert!(
        !temporal_state_cycle(&mut raw, "INSERT INTO app(id,body) VALUES(777,'rollback')")
            .await
            .iter()
            .any(|f| f.0 == b'E')
    );
    pg_send(&mut raw, b'P', &parse("", "SELECT $1::integer")).await;
    pg_send(&mut raw, b'D', b"S\0").await;
    assert!(pg_sync(&mut raw).await.iter().any(|f| f.0 == b'1'));
    pg_send(
        &mut raw,
        b'B',
        &bind("width_error", "", &[Some("2147483648")], false),
    )
    .await;
    let error = pg_sync(&mut raw).await;
    let observed = temporal_state_frames(&error, "bind");
    assert_eq!(observed["sqlstate"], "22003");
    assert_eq!(observed["ready"], "E");
    assert!(!error.iter().any(|f| f.0 == b'2'));
    assert_eq!(
        temporal_state_frames(&temporal_state_cycle(&mut raw, "SELECT 1").await, "query")["sqlstate"],
        "25P02"
    );
    temporal_state_cycle(&mut raw, "ROLLBACK").await;
    assert_eq!(
        temporal_state_frames(
            &temporal_state_cycle(&mut raw, "SELECT id FROM app WHERE id=777 LIMIT 1").await,
            "query"
        )["data_rows"],
        0
    );
    let mut execute = Vec::new();
    cstr("width_error", &mut execute);
    execute.extend(0u32.to_be_bytes());
    pg_send(&mut raw, b'E', &execute).await;
    assert_eq!(
        temporal_state_frames(&pg_sync(&mut raw).await, "execute")["sqlstate"],
        "34000"
    );
    pg_send(&mut raw, b'P', &parse("", "SELECT $1::integer")).await;
    pg_send(&mut raw, b'D', b"S\0").await;
    pg_sync(&mut raw).await;
    pg_send(&mut raw, b'B', &bind("width_ok", "", &[Some("65")], false)).await;
    let mut execute = Vec::new();
    cstr("width_ok", &mut execute);
    execute.extend(0u32.to_be_bytes());
    pg_send(&mut raw, b'E', &execute).await;
    let completed = pg_sync(&mut raw).await;
    assert!(completed.iter().any(|f| f.0 == b'2'));
    assert_eq!(temporal_state_frames(&completed, "execute")["data_rows"], 1);
    pg_send(&mut raw, b'X', &[]).await;
    drop(raw);
    f.stop().await;
}
