#[tokio::test]
async fn count_parameter_pg18_parse0_error_order_and_typed_bind() {
    let f = Fixture::new().await;
    let mut raw = raw_pg(&f).await;
    for query in [
        "SELECT count($1),sum($1::integer) FROM app",
        "SELECT count($1),count($1::integer) FROM app",
        "SELECT count($1) FROM app WHERE $1::integer=1",
        "SELECT count($2),count($1),sum($1::integer) FROM app",
        "SELECT count($1),count($2),sum($2::integer) FROM app",
    ] {
        let result = temporal_state_extended(&mut raw, query).await;
        assert_eq!(result["stage"], "parse", "{query}: {result}");
        assert_eq!(result["sqlstate"], "42P08", "{query}: {result}");
        assert_eq!(result["tags"], serde_json::json!(["E", "Z"]));
        assert_eq!(result["ready"], "I");
        assert_eq!(result["data_rows"], 0);
    }
    for (query, code) in [
        ("SELECT count($1),count($1) FROM app", "42P18"),
        ("SELECT count($1),sum($2::integer) FROM app", "42P18"),
    ] {
        let result = temporal_state_extended(&mut raw, query).await;
        assert_eq!(result["sqlstate"], code, "{query}: {result}");
    }
    // PG18.6: count(*) counts the implicit row (DD-0068 F11).
    let result = temporal_state_extended(&mut raw, "SELECT count(*)").await;
    assert!(result["sqlstate"].is_null(), "{result}");
    assert_eq!(result["data_rows"], 1);
    pg_send(&mut raw, b'X', &[]).await;
    drop(raw);
    let (pg, connection) = f.pg(f.app.clone()).await;
    pg.execute("INSERT INTO app(id,body) VALUES(1,'count')", &[])
        .await
        .unwrap();
    for query in [
        "SELECT sum($1::integer),count($1) FROM app",
        "SELECT count($1::integer),count($1) FROM app",
    ] {
        let prepared = pg.prepare(query).await.unwrap();
        assert_eq!(prepared.params()[0].oid(), 23);
        assert_eq!(prepared.columns()[1].type_().oid(), 20);
        for (value, expected) in [(Some(7i32), 1i64), (None, 0)] {
            let rows = pg.query(&prepared, &[&value]).await.unwrap();
            assert_eq!(rows[0].get::<_, i64>(1), expected);
        }
    }
    drop(pg);
    connection.await.unwrap();
    f.stop().await;
}
#[tokio::test]
async fn count_parameter_pg18_limit_parse0_and_typed_bind() {
    let f = Fixture::new().await;
    let mut raw = raw_pg(&f).await;
    for (query, code) in [
        ("SELECT count($1) FROM app LIMIT $1", "42P08"),
        (
            "SELECT count($1),count($1::bigint) FROM app LIMIT $1",
            "42P08",
        ),
        ("SELECT count($1) FROM app LIMIT $2", "42P18"),
    ] {
        let result = temporal_state_extended(&mut raw, query).await;
        assert_eq!(result["stage"], "parse", "{query}: {result}");
        assert_eq!(result["sqlstate"], code, "{query}: {result}");
        assert_eq!(result["tags"], serde_json::json!(["E", "Z"]));
    }
    pg_send(&mut raw, b'X', &[]).await;
    drop(raw);
    let (pg, connection) = f.pg(f.app.clone()).await;
    pg.execute("INSERT INTO app(id,body) VALUES(1,'limit')", &[])
        .await
        .unwrap();
    let p = pg
        .prepare("SELECT count($1::bigint) FROM app LIMIT $1")
        .await
        .unwrap();
    assert_eq!(p.params()[0].oid(), 20);
    assert_eq!(pg.query(&p, &[&1i64]).await.unwrap()[0].get::<_, i64>(0), 1);
    assert!(pg.query(&p, &[&0i64]).await.unwrap().is_empty());
    drop(p);
    drop(pg);
    connection.await.unwrap();
    f.stop().await;
}
#[tokio::test]
async fn count_parameter_authority_and_transaction_precede_new_class() {
    let f = Fixture::new().await;
    let (app, app_connection) = f.pg(f.app.clone()).await;
    for query in ["SELECT count($1),sum($1::integer) FROM other"] {
        assert_eq!(
            code(app.prepare(query).await.unwrap_err()),
            "42501",
            "{query}"
        );
    }
    // PG18.6: implicit-row parameter resolution reports the conflict (DD-0068 F11).
    assert_eq!(
        code(
            app.prepare("SELECT count($1),sum($1::integer)")
                .await
                .unwrap_err()
        ),
        "42P08"
    );
    // E1a (799adca) accepts the public. qualifier, so table authority denies `other`.
    assert_eq!(
        code(
            app.prepare("SELECT count($1),sum($1::integer) FROM public.other")
                .await
                .unwrap_err()
        ),
        "42501"
    );
    app.batch_execute("BEGIN").await.unwrap();
    app.execute("INSERT INTO app(id,body) VALUES(77,'rollback')", &[])
        .await
        .unwrap();
    assert_eq!(
        code(
            app.prepare("SELECT count($1),sum($1::integer) FROM app")
                .await
                .unwrap_err()
        ),
        "42P08"
    );
    assert_eq!(
        code(
            app.query("SELECT count(*) FROM app", &[])
                .await
                .unwrap_err()
        ),
        "25P02"
    );
    app.batch_execute("ROLLBACK").await.unwrap();
    assert!(
        app.query("SELECT id FROM app WHERE id=77 LIMIT 1", &[])
            .await
            .unwrap()
            .is_empty()
    );
    drop(app);
    app_connection.await.unwrap();
    let (task, task_connection) = f.pg(f.task.clone()).await;
    assert_eq!(
        code(
            task.prepare("SELECT count($1),sum($1::integer) FROM app")
                .await
                .unwrap_err()
        ),
        "42501"
    );
    drop(task);
    task_connection.await.unwrap();
    f.stop().await;
}
