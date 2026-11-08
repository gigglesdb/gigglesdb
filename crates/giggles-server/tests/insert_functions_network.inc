async fn insert_functions_extended(
    raw: &mut tokio_rustls::client::TlsStream<TcpStream>,
    query: &str,
    params: &[Option<&str>],
) -> serde_json::Value {
    pg_send(raw, b'P', &parse("", query)).await;
    pg_send(raw, b'D', b"S\0").await;
    let frames = pg_sync(raw).await;
    if frames.iter().any(|frame| frame.0 == b'E') {
        return temporal_state_frames(&frames, "parse");
    }
    let parameters = &frames.iter().find(|frame| frame.0 == b't').unwrap().1;
    let types: Vec<_> = parameters[2..]
        .as_chunks::<4>()
        .0
        .iter()
        .map(|bytes| u32::from_be_bytes(*bytes))
        .collect();
    assert!(frames.iter().any(|frame| frame.0 == b'n'), "{query}");
    assert!(!frames.iter().any(|frame| frame.0 == b'T'), "{query}");
    pg_send(raw, b'B', &bind("", "", params, false)).await;
    // Keep Bind and Execute in one implicit transaction; Sync ends portal life.
    pg_send(raw, b'E', &execute("", 0)).await;
    let frames = pg_sync(raw).await;
    let phase = if frames.iter().any(|frame| frame.0 == b'2') {
        "execute"
    } else {
        "bind"
    };
    let mut result = temporal_state_frames(&frames, phase);
    result["parameter_oids"] = serde_json::json!(types);
    result["commands"] = serde_json::json!(
        frames
            .iter()
            .filter(|frame| frame.0 == b'C')
            .map(|frame| std::str::from_utf8(&frame.1[..frame.1.len() - 1]).unwrap())
            .collect::<Vec<_>>()
    );
    result
}

fn insert_functions_rows(f: &Fixture) -> giggles_core::relational::SqlResult {
    f.admin
        .sql_session()
        .execute("SELECT * FROM app ORDER BY id LIMIT 1024", &[], None)
        .unwrap()
}

#[tokio::test]
async fn insert_functions_prepared_metadata_effects_errors_and_rollback() {
    let f = Fixture::new().await;
    let mut raw = raw_pg(&f).await;
    for (query, params, expected) in [
        (
            "INSERT INTO app(id,body,num) VALUES($1,lower($2),abs($3))",
            vec![Some("3"), Some("HELLO"), Some("-2.5")],
            vec![20, 25, 701],
        ),
        (
            "INSERT INTO app(id,num) VALUES($1,$2)",
            vec![Some("4"), Some("5")],
            vec![20, 23],
        ),
        (
            "INSERT INTO app(id,body,num) VALUES($1,lower($2),coalesce(NULL,$3::integer))",
            vec![Some("5"), None, None],
            vec![20, 25, 23],
        ),
        (
            "INSERT INTO app(id,body,num) VALUES(6,lower('DEAD'),coalesce(7,abs(-2147483648)))",
            vec![],
            vec![],
        ),
    ] {
        let response = insert_functions_extended(&mut raw, query, &params).await;
        assert_eq!(response["sqlstate"], serde_json::Value::Null, "{response}");
        assert_eq!(response["parameter_oids"], serde_json::json!(expected));
        assert_eq!(response["commands"], serde_json::json!(["INSERT 0 1"]));
        assert_eq!(response["data_rows"], 0);
    }
    let rows = insert_functions_rows(&f);
    assert_eq!(rows.rows.len(), 4);
    assert_eq!(
        rows.rows[0].values[0],
        giggles_core::relational::Value::Int(3)
    );
    assert_eq!(
        rows.rows[0].values[1],
        giggles_core::relational::Value::Text("hello".into())
    );
    assert_eq!(
        rows.rows[0].values[4],
        giggles_core::relational::Value::Int(2)
    );
    assert_eq!(
        rows.rows[2].values[1],
        giggles_core::relational::Value::Null
    );
    assert_eq!(
        rows.rows[2].values[4],
        giggles_core::relational::Value::Null
    );
    for (query, code, phase) in [
        (
            "INSERT INTO app(id,num) VALUES(20,count(*))",
            "42803",
            "parse",
        ),
        (
            "INSERT INTO app(id,body,num) VALUES(20,lower('X'),id)",
            "42703",
            "parse",
        ),
        (
            "INSERT INTO app(id,num) VALUES(20,lower('2'))",
            "42804",
            "parse",
        ),
        (
            "INSERT INTO app(id,body) VALUES(abs(-9223372036854775808),lower('X'))",
            "22003",
            "bind",
        ),
        (
            "INSERT INTO app(id,body) VALUES(3,lower('DUPLICATE'))",
            "23505",
            "execute",
        ),
    ] {
        let response = insert_functions_extended(&mut raw, query, &[]).await;
        assert_eq!(response["sqlstate"], code, "{query}: {response}");
        assert_eq!(response["stage"], phase, "{query}: {response}");
        assert_eq!(response["data_rows"], 0);
        assert_eq!(insert_functions_rows(&f), rows);
    }
    // The inherited parameter/custom-plan timing differs from PG Bind.
    let response = insert_functions_extended(
        &mut raw,
        "INSERT INTO app(id,body) VALUES(abs($1::bigint),lower('X'))",
        &[Some("-9223372036854775808")],
    )
    .await;
    assert_eq!(response["sqlstate"], "22003");
    assert_eq!(response["stage"], "execute");
    assert_eq!(insert_functions_rows(&f), rows);
    // F18: unknown literals use the column's input function; PG18.6 gives INSERT 0 1 with num=2.
    let response =
        insert_functions_extended(&mut raw, "INSERT INTO app(id,num) VALUES(50,'2')", &[]).await;
    assert_eq!(response["sqlstate"], serde_json::Value::Null);
    assert_eq!(response["commands"], serde_json::json!(["INSERT 0 1"]));
    let response = insert_functions_extended(&mut raw, "DELETE FROM app WHERE id=50", &[]).await;
    assert_eq!(response["sqlstate"], serde_json::Value::Null);
    assert_eq!(insert_functions_rows(&f), rows);
    temporal_state_cycle(&mut raw, "BEGIN").await;
    let response = insert_functions_extended(
        &mut raw,
        "INSERT INTO app(id,body) VALUES(101,lower('ROLLBACK'))",
        &[],
    )
    .await;
    assert_eq!(response["commands"], serde_json::json!(["INSERT 0 1"]));
    temporal_state_cycle(&mut raw, "ROLLBACK").await;
    assert_eq!(insert_functions_rows(&f), rows);
    temporal_state_cycle(&mut raw, "BEGIN").await;
    insert_functions_extended(
        &mut raw,
        "INSERT INTO app(id,body) VALUES(102,lower('ABORT'))",
        &[],
    )
    .await;
    let response = insert_functions_extended(
        &mut raw,
        "INSERT INTO app(id,body) VALUES(abs(-9223372036854775808),lower('X'))",
        &[],
    )
    .await;
    assert_eq!(response["sqlstate"], "22003");
    let frames = temporal_state_cycle(&mut raw, "SELECT id FROM app LIMIT 1").await;
    assert!(has_code(&frames, "25P02"));
    temporal_state_cycle(&mut raw, "ROLLBACK").await;
    assert_eq!(insert_functions_rows(&f), rows);
    pg_send(&mut raw, b'X', &[]).await;
    drop(raw);
    f.stop().await;
}

#[tokio::test]
async fn insert_functions_target_task_and_binding_revocation_precedence() {
    let f = Fixture::new().await;
    let before = insert_functions_rows(&f);
    let mut raw = raw_pg(&f).await;
    for query in [
        "INSERT INTO other(id,body) VALUES(1,lower('X'))",
        "INSERT INTO other(id,body) VALUES(abs(-9223372036854775808),lower('X'))",
    ] {
        let response = insert_functions_extended(&mut raw, query, &[]).await;
        assert_eq!(response["sqlstate"], "42501");
        assert_eq!(response["stage"], "parse");
        assert_eq!(response["data_rows"], 0);
    }
    let mut task = prepared_network::raw_task(&f).await;
    let authority = f.admin.snapshot().unwrap();
    for query in [
        "INSERT INTO app(id,body) VALUES(1,lower('X'))",
        "INSERT INTO app(id,body) VALUES(abs(-9223372036854775808),lower('X'))",
    ] {
        let response = insert_functions_extended(&mut task, query, &[]).await;
        assert_eq!(response["sqlstate"], "42501");
        assert_eq!(response["stage"], "parse");
        assert_eq!(response["data_rows"], 0);
    }
    assert_eq!(f.admin.snapshot().unwrap(), authority);
    pg_send(&mut task, b'X', &[]).await;
    drop(task);
    pg_send(
        &mut raw,
        b'P',
        &parse(
            "revoke_insert",
            "INSERT INTO app(id,body) VALUES(9,lower('REVOKED'))",
        ),
    )
    .await;
    assert!(!pg_sync(&mut raw).await.iter().any(|frame| frame.0 == b'E'));
    pg_send(
        &mut raw,
        b'B',
        &bind("revoke_portal", "revoke_insert", &[], false),
    )
    .await;
    pg_send(&mut raw, b'H', &[]).await;
    assert_eq!(pg_read(&mut raw).await.0, b'2');
    f.service.reload_bindings(vec![]).unwrap();
    pg_send(&mut raw, b'E', &execute("revoke_portal", 0)).await;
    // Credential revocation retains the existing immediate connection-close fence.
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(5), raw.read_u8())
            .await
            .unwrap()
            .is_err()
    );
    assert_eq!(insert_functions_rows(&f), before);
    drop(raw);
    f.stop().await;
}
