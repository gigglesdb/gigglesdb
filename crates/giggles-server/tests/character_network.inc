#[tokio::test]
async fn character_pg18_utf8_c_describe_repeated_binary_bind_and_errors() {
    let f = Fixture::new().await;
    let (pg, connection) = f.pg(f.app.clone()).await;
    let ascii = pg.prepare("SELECT pg_catalog.\"ascii\"($1)").await.unwrap();
    assert_eq!(ascii.params()[0].oid(), 25);
    assert_eq!(ascii.columns()[0].type_().oid(), 23);
    for (input, expected) in [
        (Some(""), Some(0i32)),
        (Some("é"), Some(233)),
        (Some("😀rest"), Some(128512)),
        (None, None),
    ] {
        assert_eq!(
            pg.query(&ascii, &[&input]).await.unwrap()[0].get::<_, Option<i32>>(0),
            expected
        );
    }
    let initcap = pg.prepare("SELECT initcap($1)").await.unwrap();
    assert_eq!(initcap.params()[0].oid(), 25);
    assert_eq!(initcap.columns()[0].type_().oid(), 25);
    for (input, expected) in [
        (Some("éHELLO_123ABC"), Some("éHello_123abc")),
        (Some("áBC Z"), Some("ÁBc Z")),
        (None, None),
    ] {
        assert_eq!(
            pg.query(&initcap, &[&input]).await.unwrap()[0]
                .get::<_, Option<String>>(0)
                .as_deref(),
            expected
        );
    }
    let chr = pg.prepare("SELECT chr($1)").await.unwrap();
    assert_eq!(chr.params()[0].oid(), 23);
    assert_eq!(chr.columns()[0].type_().oid(), 25);
    for (input, expected) in [
        (Some(65i32), Some("A")),
        (Some(128512), Some("😀")),
        (None, None),
    ] {
        assert_eq!(
            pg.query(&chr, &[&input]).await.unwrap()[0]
                .get::<_, Option<String>>(0)
                .as_deref(),
            expected
        );
    }
    for (value, code) in [
        (-1i32, "22023"),
        (0, "54000"),
        (55296, "54000"),
        (1114112, "54000"),
    ] {
        assert_eq!(
            code_from_character_error(pg.query(&chr, &[&value]).await.unwrap_err()),
            code
        );
    }
    for query in [
        "SELECT ascii(1)",
        "SELECT initcap(TRUE)",
        "SELECT chr(65::bigint)",
        "SELECT chr()",
        "SELECT initcap('a','b')",
    ] {
        assert_eq!(
            code_from_character_error(pg.prepare(query).await.unwrap_err()),
            "42883",
            "{query}"
        );
    }
    // Table-free unknown routines fail authority; authorized FROM reaches binding.
    assert_eq!(
        code_from_character_error(pg.prepare("SELECT \"ASCII\"('a')").await.unwrap_err()),
        "42501"
    );
    assert_eq!(
        code_from_character_error(
            pg.prepare("SELECT \"ASCII\"('a') FROM app")
                .await
                .unwrap_err()
        ),
        "42883"
    );
    for query in [
        "SELECT chr(missing,1) FROM app",
        "SELECT ascii(missing,1) FROM app",
        "SELECT initcap(missing,1) FROM app",
        "SELECT chr(true,missing) FROM app",
        "SELECT chr(abs(missing),1) FROM app",
        "SELECT ascii(1::boolean,missing) FROM app",
    ] {
        assert_eq!(
            code_from_character_error(pg.prepare(query).await.unwrap_err()),
            "42703",
            "{query}"
        );
    }
    pg.execute(
        "INSERT INTO app(id,body,num) VALUES(1,'éHELLO',128512)",
        &[],
    )
    .await
    .unwrap();
    let row = pg
        .query_one(
            "SELECT ascii(body),initcap(body),chr(num) FROM app WHERE id=1",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, i32>(0), 233);
    assert_eq!(row.get::<_, String>(1), "éHello");
    assert_eq!(row.get::<_, String>(2), "😀");
    drop(pg);
    connection.await.unwrap();
    f.stop().await;
}

fn code_from_character_error(error: tokio_postgres::Error) -> String {
    error.code().unwrap().code().into()
}

#[tokio::test]
async fn character_constant_bind_errors_and_authority_controls() {
    let f = Fixture::new().await;
    let mut raw = raw_pg(&f).await;
    for (argument, code) in [
        ("-1", "22023"),
        ("0", "54000"),
        ("55296", "54000"),
        ("1114112", "54000"),
    ] {
        for suffix in ["", " WHERE FALSE", " LIMIT 0"] {
            let query = format!("SELECT chr({argument}){suffix}");
            let result = temporal_state_extended(&mut raw, &query).await;
            assert_eq!(result["stage"], "bind", "{query}: {result}");
            assert_eq!(result["sqlstate"], code, "{query}: {result}");
            assert_eq!(result["data_rows"], 0);
        }
    }
    pg_send(&mut raw, b'X', &[]).await;
    drop(raw);
    let (app, app_connection) = f.pg(f.app.clone()).await;
    for query in [
        "SELECT ascii(body) FROM other",
        "SELECT initcap(body) FROM other",
        "SELECT chr(0) FROM other",
        "SELECT sum(ascii('a'))",
    ] {
        assert_eq!(
            code_from_character_error(app.prepare(query).await.unwrap_err()),
            "42501",
            "{query}"
        );
    }
    // Namespace and input literal admission remain the existing bounded profile.
    assert_eq!(
        code_from_character_error(app.prepare("SELECT public.ascii('a')").await.unwrap_err()),
        "42501"
    );
    drop(app);
    app_connection.await.unwrap();
    let (task, task_connection) = f.pg(f.task.clone()).await;
    for query in [
        "SELECT ascii('a')",
        "SELECT initcap('a')",
        "SELECT chr(65)",
        "SELECT ascii(body) FROM app",
    ] {
        assert_eq!(
            code_from_character_error(task.prepare(query).await.unwrap_err()),
            "42501",
            "{query}"
        );
    }
    drop(task);
    task_connection.await.unwrap();
    f.stop().await;
}
#[tokio::test]
async fn character_integer_width_admission_actual_text_bind() {
    let f = Fixture::new().await;
    let mut raw = raw_pg(&f).await;
    let root = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("dd68")
        .join("dd68-integer-formats-20261106")
        .join("character-width-wire")
        .join("traces");
    std::fs::create_dir_all(&root).unwrap();
    let mut report = Vec::new();
    let mut mismatches = Vec::new();
    for (query, parameter, oid, expected) in [
        ("SELECT chr($1::smallint)", "-32769", 21u32, Some("22003")),
        ("SELECT chr($1::smallint)", "32768", 21, Some("22003")),
        ("SELECT chr($1::integer)", "-2147483649", 23, Some("22003")),
        ("SELECT chr($1::integer)", "2147483648", 23, Some("22003")),
        ("SELECT chr($1::smallint)", "65", 21, None),
        ("SELECT chr($1::integer)", "65", 23, None),
    ] {
        pg_send(&mut raw, b'P', &parse("", query)).await;
        pg_send(&mut raw, b'D', b"S\0").await;
        let describe = pg_sync(&mut raw).await;
        assert!(
            describe.iter().any(|f| f.0 == b'1'),
            "{query}: {describe:?}"
        );
        let mut parameter_types = vec![0, 1];
        parameter_types.extend(oid.to_be_bytes());
        assert!(
            describe
                .iter()
                .any(|f| f.0 == b't' && f.1 == parameter_types),
            "{query}: {describe:?}"
        );
        pg_send(&mut raw, b'B', &bind("", "", &[Some(parameter)], false)).await;
        let bound = pg_sync(&mut raw).await;
        let observed = temporal_state_frames(&bound, "bind");
        report.push(serde_json::json!({"query":query,"parameter":parameter,"parameter_oid":oid,"describe":describe.iter().map(|f|serde_json::json!({"tag":char::from(f.0).to_string(),"hex":hex::encode(&f.1)})).collect::<Vec<_>>(),"bind":bound.iter().map(|f|serde_json::json!({"tag":char::from(f.0).to_string(),"hex":hex::encode(&f.1)})).collect::<Vec<_>>(),"observed":observed}));
        std::fs::write(
            root.join(format!("width-{}.json", std::process::id())),
            serde_json::to_vec_pretty(&report).unwrap(),
        )
        .unwrap();
        if let Some(code) = expected {
            if observed["sqlstate"] != code {
                mismatches.push(format!("{query} {parameter}: {observed}, expected {code}"));
            }
            assert!(!bound.iter().any(|f| f.0 == b'2'));
        } else {
            assert!(observed["sqlstate"].is_null(), "{query}: {observed}");
            assert!(bound.iter().any(|f| f.0 == b'2'));
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    pg_send(&mut raw, b'X', &[]).await;
    drop(raw);
    f.stop().await;
}
