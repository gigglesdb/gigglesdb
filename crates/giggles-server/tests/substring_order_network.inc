#[tokio::test]
async fn substring_order_numeric_parse0_binary_metadata_rows_and_authority() {
    let f = Fixture::new().await;
    let (pg, connection) = f.pg(f.app.clone()).await;
    for (query, oid) in [
        ("SELECT SUBSTRING($1 FOR $2::smallint FROM $2::integer)", 23),
        ("SELECT SUBSTRING($1 FOR $2::integer FROM $2::smallint)", 21),
    ] {
        let p = pg.prepare(query).await.unwrap();
        assert_eq!(
            p.params().iter().map(|t| t.oid()).collect::<Vec<_>>(),
            vec![25, oid]
        );
        assert_eq!(p.columns()[0].name(), "substring");
        assert_eq!(p.columns()[0].type_().oid(), 25);
        let small = 2i16;
        let integer = 2i32;
        let parameter: &(dyn tokio_postgres::types::ToSql + Sync) =
            if oid == 21 { &small } else { &integer };
        let row = pg.query_one(&p, &[&"abcdef", parameter]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "bc");
    }
    let p = pg
        .prepare("SELECT SUBSTRING($1::bytea FOR $2 FROM $3)")
        .await
        .unwrap();
    assert_eq!(
        p.params().iter().map(|t| t.oid()).collect::<Vec<_>>(),
        vec![17, 23, 23]
    );
    assert_eq!(
        pg.query_one(&p, &[&b"abcdef".to_vec(), &2i32, &3i32])
            .await
            .unwrap()
            .get::<_, Vec<u8>>(0),
        b"cd"
    );
    pg.execute("INSERT INTO app(id,body) VALUES(1,'abcdef')", &[])
        .await
        .unwrap();
    assert_eq!(
        pg.query_one(
            "SELECT min(SUBSTRING(body FOR 2 FROM 3)),count(SUBSTRING(body FOR 2 FROM 3)) FROM app",
            &[]
        )
        .await
        .unwrap()
        .get::<_, String>(0),
        "cd"
    );
    for query in ["SELECT SUBSTRING(body FOR 2 FROM 3) FROM other"] {
        assert_eq!(
            code(pg.prepare(query).await.unwrap_err()),
            "42501",
            "{query}"
        );
    }
    // PG18.6: no-FROM aggregate evaluates the implicit row (DD-0068 F11)
    let rows = pg
        .query("SELECT min(SUBSTRING('abc' FOR 1 FROM 1))", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "a");
    // PG supports this regex overload; current bounded profile explicitly does not.
    assert_eq!(
        code(
            pg.prepare("SELECT SUBSTRING($1 FOR $2 FROM $3)")
                .await
                .unwrap_err()
        ),
        "0A000"
    );
    drop(pg);
    connection.await.unwrap();
    let (task, connection) = f.pg(f.task.clone()).await;
    for query in [
        "SELECT SUBSTRING('abc' FOR 1 FROM 1)",
        "SELECT SUBSTRING(body FOR 1 FROM 1) FROM app",
    ] {
        assert_eq!(code(task.prepare(query).await.unwrap_err()), "42501");
    }
    drop(task);
    connection.await.unwrap();
    f.stop().await;
}

#[tokio::test]
async fn substring_order_negative_length_bind_phase_no_portal_and_transaction() {
    let f = Fixture::new().await;
    let mut raw = raw_pg(&f).await;
    let mut report = vec![];
    for query in [
        "SELECT SUBSTRING('abc' FOR -1 FROM 1)",
        "SELECT SUBSTRING('abc'::bytea FOR -1 FROM 1)",
        "SELECT substr('abc',1,-1)",
    ] {
        let observed = temporal_state_extended(&mut raw, query).await;
        assert_eq!(observed["sqlstate"], "22011", "{query}: {observed}");
        assert_eq!(observed["stage"], "bind");
        assert_eq!(observed["data_rows"], 0);
        report.push(serde_json::json!({"query":query,"observed":observed}));
    }
    pg_send(&mut raw, b'E', &execute("", 0)).await;
    let frames = pg_sync(&mut raw).await;
    assert_eq!(
        temporal_state_frames(&frames, "execute")["sqlstate"],
        "34000"
    );
    temporal_state_cycle(&mut raw, "BEGIN").await;
    assert_eq!(
        temporal_state_extended(&mut raw, "SELECT SUBSTRING('abc' FOR -1 FROM 1)").await["sqlstate"],
        "22011"
    );
    assert_eq!(
        temporal_state_frames(&temporal_state_cycle(&mut raw, "SELECT 1").await, "simple")["sqlstate"],
        "25P02"
    );
    temporal_state_cycle(&mut raw, "ROLLBACK").await;
    assert!(
        temporal_state_extended(&mut raw, "SELECT SUBSTRING('abc' FOR 1 FROM 1)").await["sqlstate"]
            .is_null()
    );
    let root = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("dd68")
        .join("dd68-substring-order-20261106")
        .join("wire-traces");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(
        root.join(format!("negative-{}.json", std::process::id())),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    pg_send(&mut raw, b'X', &[]).await;
    drop(raw);
    f.stop().await;
}

#[tokio::test]
async fn substring_order_raw_parameter_bind_is_pure_and_execute_error_is_declared_gap() {
    let f = Fixture::new().await;
    let mut raw = raw_pg(&f).await;
    let query = "SELECT SUBSTRING($1 FOR $2::integer FROM $3)";
    pg_send(&mut raw, b'P', &parse("", query)).await;
    pg_send(&mut raw, b'D', b"S\0").await;
    let described = pg_sync(&mut raw).await;
    let mut oids = vec![0, 3];
    for oid in [25u32, 23, 23] {
        oids.extend(oid.to_be_bytes());
    }
    assert!(described.iter().any(|f| f.0 == b't' && f.1 == oids));
    pg_send(
        &mut raw,
        b'B',
        &bind("p", "", &[Some("abcdef"), Some("-1"), Some("3")], false),
    )
    .await;
    pg_send(&mut raw, b'E', &execute("p", 0)).await;
    let frames = pg_sync(&mut raw).await;
    assert!(frames.iter().any(|f| f.0 == b'2'));
    let bound = frames
        .iter()
        .take_while(|f| f.0 == b'2')
        .cloned()
        .collect::<Vec<_>>();
    assert!(!bound.iter().any(|f| matches!(f.0, b'D' | b'E')));
    assert_eq!(
        temporal_state_frames(&frames, "execute")["sqlstate"],
        "22011"
    );
    assert!(!frames.iter().any(|f| f.0 == b'D'));
    // Actual PG18 custom planning raises this parameter error at Bind; current adapter stays pure.
    let root = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("dd68")
        .join("dd68-substring-order-20261106")
        .join("wire-traces");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join(format!("parameter-{}.json",std::process::id())),serde_json::to_vec_pretty(&serde_json::json!({"query":query,"parameter_oids":[25,23,23],"describe":described.iter().map(|f|serde_json::json!({"tag":char::from(f.0).to_string(),"hex":hex::encode(&f.1)})).collect::<Vec<_>>(),"bind":temporal_state_frames(&bound,"bind"),"execute":temporal_state_frames(&frames,"execute"),"pg_phase":"bind","current_phase":"execute","parity":false})).unwrap()).unwrap();
    pg_send(&mut raw, b'X', &[]).await;
    drop(raw);
    f.stop().await;
}
