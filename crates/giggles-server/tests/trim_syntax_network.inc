#[tokio::test]
async fn trim_standard_forms_describe_binary_bind_bytea_and_rows() {
    let f = Fixture::new().await;
    let (pg, connection) = f.pg(f.app.clone()).await;
    let prepared = pg
        .prepare("SELECT TRIM(BOTH $1 FROM $2),TRIM(LEADING $1 FROM $2),TRIM(TRAILING $1 FROM $2)")
        .await
        .unwrap();
    assert_eq!(
        prepared
            .params()
            .iter()
            .map(|t| t.oid())
            .collect::<Vec<_>>(),
        vec![25, 25]
    );
    assert_eq!(
        prepared
            .columns()
            .iter()
            .map(|c| c.name())
            .collect::<Vec<_>>(),
        vec!["btrim", "ltrim", "rtrim"]
    );
    assert!(prepared.columns().iter().all(|c| c.type_().oid() == 25));
    for (chars, input, want) in [
        (
            Some("xy"),
            Some("xyabcxy"),
            [Some("abc"), Some("abcxy"), Some("xyabc")],
        ),
        (Some("é"), Some("éXé"), [Some("X"), Some("Xé"), Some("éX")]),
        (None, Some("abc"), [None; 3]),
        (Some("x"), None, [None; 3]),
    ] {
        let row = pg.query_one(&prepared, &[&chars, &input]).await.unwrap();
        for (i, want) in want.into_iter().enumerate() {
            assert_eq!(row.get::<_, Option<String>>(i).as_deref(), want);
        }
    }
    assert_eq!(
        pg.query_one("SELECT TRIM(FROM '  abc  '),TRIM('xxabxx','x')", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "abc"
    );
    let bytes = pg
        .prepare("SELECT TRIM(BOTH $1::bytea FROM $2::bytea)")
        .await
        .unwrap();
    assert_eq!(
        bytes.params().iter().map(|t| t.oid()).collect::<Vec<_>>(),
        vec![17, 17]
    );
    assert_eq!(bytes.columns()[0].type_().oid(), 17);
    assert_eq!(
        pg.query_one(&bytes, &[&vec![32u8], &vec![32u8, 32, 97, 98, 32]])
            .await
            .unwrap()
            .get::<_, Vec<u8>>(0),
        vec![97u8, 98]
    );
    // PG18: function pg_catalog.btrim(bytea) does not exist (42883).
    assert_eq!(
        code(
            pg.prepare("SELECT TRIM(decode('20','hex'))")
                .await
                .unwrap_err()
        ),
        "42883"
    );
    pg.execute("INSERT INTO app(id,body) VALUES(1,'xxAbxx')", &[])
        .await
        .unwrap();
    let row = pg
        .query_one(
            "SELECT min(TRIM(BOTH 'x' FROM body)),count(TRIM(body)) FROM app",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "Ab");
    assert_eq!(row.get::<_, i64>(1), 1);
    assert_eq!(pg.query_one("SELECT TRIM('x' FROM body) FROM app WHERE id=1 AND TRIM(LEADING 'x' FROM body)='Abxx' ORDER BY TRIM(body)",&[]).await.unwrap().get::<_,String>(0),"Ab");
    drop(pg);
    connection.await.unwrap();
    f.stop().await;
}

#[tokio::test]
async fn trim_construct_parse_errors_routine_nonactivation_and_authority() {
    let f = Fixture::new().await;
    let mut raw = raw_pg(&f).await;
    for query in [
        "SELECT TRIM()",
        "SELECT TRIM(FROM)",
        "SELECT TRIM(BOTH 'a',)",
        "SELECT TRIM('a',FROM 'b')",
        "SELECT TRIM(FROM leading)",
        "SELECT TRIM(BOTH 'a' FROM)",
        "SELECT pg_catalog.trim('x' FROM 'xxa')",
        "SELECT \"trim\"('x' FROM 'xxa')",
    ] {
        let result = temporal_state_extended(&mut raw, query).await;
        assert_eq!(result["sqlstate"], "42601", "{query}: {result}");
        assert_eq!(result["stage"], "parse");
        assert_eq!(result["tags"], serde_json::json!(["E", "Z"]));
    }
    pg_send(&mut raw, b'X', &[]).await;
    drop(raw);
    let (pg, connection) = f.pg(f.app.clone()).await;
    // DD-0068 F9: pgwire follows PG 18 (42883/3F000) for deferred-routine-only queries; native surfaces keep 42501.
    for query in ["SELECT \"trim\"(' x ')", "SELECT pg_catalog.trim(' x ')"] {
        assert_eq!(
            code(pg.prepare(query).await.unwrap_err()),
            "42883",
            "{query}"
        );
    }
    for query in ["SELECT TRIM(body) FROM other"] {
        assert_eq!(
            code(pg.prepare(query).await.unwrap_err()),
            "42501",
            "{query}"
        );
    }
    // PG18.6: no-FROM aggregate evaluates the implicit row (DD-0068 F11)
    let rows = pg.query("SELECT min(TRIM(' x '))", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "x");
    for query in [
        "SELECT \"trim\"(' x ') FROM app",
        "SELECT pg_catalog.trim(' x ') FROM app",
    ] {
        assert_eq!(
            code(pg.prepare(query).await.unwrap_err()),
            "42883",
            "{query}"
        );
    }
    assert_eq!(
        code(
            pg.prepare("SELECT min(TRIM(BOTH 'x' FROM count(*))) FROM app")
                .await
                .unwrap_err()
        ),
        "42803"
    );
    drop(pg);
    connection.await.unwrap();
    let (task, task_connection) = f.pg(f.task.clone()).await;
    for query in ["SELECT TRIM(' x ')", "SELECT TRIM(body) FROM app"] {
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
