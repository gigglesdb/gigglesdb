#[tokio::test]
async fn md5_overload_parse0_binary_bind_result_and_authority() {
    let f = Fixture::new().await;
    let mut raw = raw_pg(&f).await;
    for (query, input, oid, expected) in [
        (
            "SELECT pg_catalog.\"md5\"($1)",
            "abc",
            25u32,
            "900150983cd24fb0d6963f7d28e17f72",
        ),
        (
            "SELECT pg_catalog.\"md5\"($1::bytea)",
            "\\x00ff80",
            17,
            "4fd64cb80c3f86e1e9d0f559af8b370f",
        ),
    ] {
        pg_send(&mut raw, b'P', &parse("md5", query)).await;
        pg_send(&mut raw, b'D', b"Smd5\0").await;
        let frames = pg_sync(&mut raw).await;
        assert!(!frames.iter().any(|x| x.0 == b'E'), "{frames:?}");
        let parameters = &frames.iter().find(|x| x.0 == b't').unwrap().1;
        assert_eq!(&parameters[..2], &1u16.to_be_bytes());
        assert_eq!(&parameters[2..], &oid.to_be_bytes());
        for binary in [false, true] {
            pg_send(&mut raw, b'B', &bind("", "md5", &[Some(input)], binary)).await;
            pg_send(&mut raw, b'D', b"P\0").await;
            pg_send(&mut raw, b'E', &execute("", 0)).await;
            let frames = pg_sync(&mut raw).await;
            assert!(!frames.iter().any(|x| x.0 == b'E'), "{frames:?}");
            let row = &frames.iter().find(|x| x.0 == b'D').unwrap().1;
            assert_eq!(&row[..2], &1u16.to_be_bytes());
            assert_eq!(&row[2..6], &32i32.to_be_bytes());
            assert_eq!(&row[6..], expected.as_bytes());
        }
        pg_send(&mut raw, b'B', &bind("", "md5", &[None], true)).await;
        pg_send(&mut raw, b'E', &execute("", 0)).await;
        let frames = pg_sync(&mut raw).await;
        assert_eq!(
            &frames.iter().find(|x| x.0 == b'D').unwrap().1[2..],
            &(-1i32).to_be_bytes()
        );
        pg_send(&mut raw, b'C', b"Smd5\0").await;
        assert!(!pg_sync(&mut raw).await.iter().any(|x| x.0 == b'E'));
    }
    for (query, code) in [
        ("SELECT md5()", "42883"),
        ("SELECT md5(1)", "42883"),
        ("SELECT md5(TRUE)", "42883"),
        ("SELECT md5('a','b')", "42883"),
        ("SELECT public.md5('abc')", "42501"),
        ("SELECT pg_catalog.unknown_md5('abc')", "0A000"),
        ("SELECT pg_catalog.unknown_md5('abc') FROM app", "0A000"),
    ] {
        let response = temporal_state_extended(&mut raw, query).await;
        assert_eq!(response["sqlstate"], code, "{query}: {response}");
    }
    // Preserve the inherited unresolved-parameter phase result as an observation.
    let wrong_arity = temporal_state_extended(&mut raw, "SELECT md5($1,$2)").await;
    eprintln!("md5 untyped wrong arity observed: {wrong_arity}");
    assert_eq!(wrong_arity["error_responses"], 1);
    assert_eq!(wrong_arity["data_rows"], 0);
    pg_send(&mut raw, b'X', &[]).await;
    drop(raw);
    let (pg, connection) = f.pg(f.app.clone()).await;
    pg.execute("INSERT INTO app(id,body) VALUES(1,'abc')", &[])
        .await
        .unwrap();
    assert_eq!(
        pg.query_one("SELECT md5(body) FROM app WHERE id=1", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "900150983cd24fb0d6963f7d28e17f72"
    );
    // tokio-postgres sends the typed parameter in binary format.
    let maximum = vec![b'a'; 65536];
    let prepared = pg.prepare("SELECT md5($1::bytea)").await.unwrap();
    assert_eq!(prepared.params()[0].oid(), 17);
    assert_eq!(prepared.columns()[0].type_().oid(), 25);
    assert_eq!(
        pg.query_one(&prepared, &[&maximum])
            .await
            .unwrap()
            .get::<_, String>(0),
        "2d61aa54b58c2e94403fb092c3dbc027"
    );
    drop(pg);
    connection.await.unwrap();
    let mut task = prepared_network::raw_task(&f).await;
    let before = f.admin.snapshot().unwrap();
    for query in ["SELECT md5('abc')", "SELECT md5(body) FROM app"] {
        let response = temporal_state_extended(&mut task, query).await;
        assert_eq!(response["sqlstate"], "42501");
        assert_eq!(response["stage"], "parse");
        assert_eq!(response["data_rows"], 0);
        assert_eq!(response["tags"], serde_json::json!(["E", "Z"]));
    }
    assert_eq!(f.admin.snapshot().unwrap(), before);
    pg_send(&mut task, b'X', &[]).await;
    drop(task);
    f.stop().await;
}
