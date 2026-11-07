#[tokio::test]
async fn aggregate_scalar_pg18_rows_metadata_and_bind_errors() {
    let f = Fixture::new().await;
    let (pg, conn) = f.pg(f.app.clone()).await;
    pg.execute("INSERT INTO app (id, body) VALUES (1,'Ab')", &[])
        .await
        .unwrap();
    pg.execute("INSERT INTO app (id, body) VALUES (2,'C')", &[])
        .await
        .unwrap();
    let p = pg.prepare("SELECT sum(abs(id)), min(lower(body)), max(length(body)), count(NULL), count(*) FROM app").await.unwrap();
    assert_eq!(
        p.columns()
            .iter()
            .map(|c| c.type_().oid())
            .collect::<Vec<_>>(),
        vec![1700, 25, 23, 20, 20]
    );
    let rows = pg.query(&p, &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(1), "ab");
    assert_eq!(rows[0].get::<_, i32>(2), 2);
    assert_eq!(rows[0].get::<_, i64>(3), 0);
    assert_eq!(rows[0].get::<_, i64>(4), 2);
    for (q, code) in [
        ("SELECT sum(NULL) FROM app", "42725"),
        ("SELECT sum($1) FROM app", "42725"),
        ("SELECT sum(sum(id)) FROM app", "42803"),
        ("SELECT sum(count(*)) FROM app", "42803"),
        ("SELECT sum(pg_catalog.count(*)) FROM app", "42803"),
        ("SELECT sum(\"count\"(*)) FROM app", "42803"),
        ("SELECT sum(\"COUNT\"(*)) FROM app", "42883"),
    ] {
        let e = pg.prepare(q).await.unwrap_err();
        assert_eq!(e.code().unwrap().code(), code, "{q}");
    }
    drop(pg);
    conn.await.unwrap();
    f.stop().await;
}
#[tokio::test]
async fn aggregate_scalar_pg18_immutable_error_stage_and_authority() {
    let f = Fixture::new().await;
    let mut s = raw_pg(&f).await;
    for q in [
        "SELECT sum(abs(-2147483648)) FROM app WHERE FALSE",
        "SELECT sum(abs(-2147483648)) FROM app LIMIT 0",
    ] {
        let r = temporal_state_extended(&mut s, q).await;
        assert_eq!(r["stage"], "bind", "{q}: {r}");
        assert_eq!(r["sqlstate"], "22003", "{q}: {r}");
    }
    let r = temporal_state_extended(&mut s, "SELECT count(*)").await;
    // PG18.6: application count(*) counts the implicit row (DD-0068 F11).
    assert!(r["sqlstate"].is_null(), "{r}");
    assert_eq!(r["data_rows"], 1);
    pg_send(&mut s, b'X', &[]).await;
    drop(s);
    let (pg, conn) = f.pg(f.app.clone()).await;
    // PG18.6: no-FROM aggregates evaluate one implicit row (DD-0068 F11).
    let p = pg
        .prepare("SELECT count(*), sum(1), max('b')")
        .await
        .unwrap();
    assert_eq!(
        p.columns().iter().map(|c| c.type_().oid()).collect::<Vec<_>>(),
        vec![20, 20, 25]
    );
    let rows = pg.query(&p, &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i64>(0), 1);
    assert_eq!(rows[0].get::<_, i64>(1), 1);
    assert_eq!(rows[0].get::<_, String>(2), "b");
    drop(p);
    drop(pg);
    conn.await.unwrap();
    let (task, conn) = f.pg(f.task.clone()).await;
    assert_eq!(
        code(task.simple_query("SELECT count(*)").await.unwrap_err()),
        "42501"
    );
    assert_eq!(
        code(task.prepare("SELECT count(*)").await.unwrap_err()),
        "42501"
    );
    drop(task);
    conn.await.unwrap();
    f.stop().await;
}
#[tokio::test]
async fn aggregate_scalar_parameter_binary_namespace_repeated_bind_and_no_effects() {
    let f = Fixture::new().await;
    let (pg, conn) = f.pg(f.app.clone()).await;
    pg.execute("INSERT INTO app (id,body) VALUES (1,'Ab')", &[])
        .await
        .unwrap();
    let p = pg
        .prepare("SELECT sum(pg_catalog.abs($1::integer)), count($1::integer) FROM app")
        .await
        .unwrap();
    assert_eq!(p.params()[0].oid(), 23);
    assert_eq!(p.columns()[0].type_().oid(), 20);
    for (param, total, count) in [
        (Some(-7i32), Some(7i64), 1i64),
        (None, None, 0),
        (Some(3), Some(3), 1),
    ] {
        let rows = pg.query(&p, &[&param]).await.unwrap();
        assert_eq!(rows[0].get::<_, Option<i64>>(0), total);
        assert_eq!(rows[0].get::<_, i64>(1), count);
    }
    assert_eq!(
        pg.query_one("SELECT count(*) FROM app", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    for (q, state) in [
        ("SELECT count($1) FROM app", "42P18"),
        ("SELECT sum(TRUE) FROM app", "42883"),
        ("SELECT min(TRUE) FROM app", "42883"),
    ] {
        assert_eq!(
            pg.prepare(q).await.unwrap_err().code().unwrap().code(),
            state
        );
    }
    drop(p);
    drop(pg);
    conn.await.unwrap();
    f.stop().await;
}
#[tokio::test]
async fn aggregate_scalar_bind_error_aborts_transaction_without_effects() {
    let f = Fixture::new().await;
    let mut s = raw_pg(&f).await;
    temporal_state_cycle(&mut s, "BEGIN").await;
    temporal_state_cycle(&mut s, "INSERT INTO app (id,body) VALUES (777,'rollback')").await;
    let r =
        temporal_state_extended(&mut s, "SELECT sum(abs(-2147483648)) FROM app WHERE FALSE").await;
    assert_eq!(r["stage"], "bind");
    assert_eq!(r["sqlstate"], "22003");
    assert_eq!(r["ready"], "E");
    let r = temporal_state_frames(
        &temporal_state_cycle(&mut s, "SELECT count(*) FROM app").await,
        "query",
    );
    assert_eq!(r["sqlstate"], "25P02");
    temporal_state_cycle(&mut s, "ROLLBACK").await;
    let r = temporal_state_frames(
        &temporal_state_cycle(&mut s, "SELECT id FROM app WHERE id=777 LIMIT 1").await,
        "query",
    );
    assert!(r["sqlstate"].is_null());
    assert_eq!(r["data_rows"], 0);
    pg_send(&mut s, b'X', &[]).await;
    drop(s);
    f.stop().await;
}
