#[tokio::test]
async fn filter_planning_pg18_bind_execute_ordered_errors() {
    let f = Fixture::new().await;
    let mut s = raw_pg(&f).await;
    for (query, error) in [
        ("SELECT 1 WHERE TRUE AND abs(-2147483648)=0", true),
        ("SELECT 1 WHERE abs(-2147483648)=0 AND TRUE", true),
        ("SELECT 1 WHERE FALSE AND abs(-2147483648)=0", false),
        ("SELECT 1 WHERE abs(-2147483648)=0 AND FALSE", true),
        ("SELECT 1 WHERE NULL::boolean AND abs(-2147483648)=0", true),
        ("SELECT 1 WHERE abs(-2147483648)=0 AND NULL::boolean", true),
        ("SELECT 1 WHERE TRUE OR abs(-2147483648)=0", false),
        ("SELECT 1 WHERE abs(-2147483648)=0 OR TRUE", true),
        ("SELECT 1 WHERE FALSE OR abs(-2147483648)=0", true),
        ("SELECT 1 WHERE abs(-2147483648)=0 OR FALSE", true),
        ("SELECT 1 WHERE NULL::boolean OR abs(-2147483648)=0", true),
        ("SELECT 1 WHERE abs(-2147483648)=0 OR NULL::boolean", true),
        ("SELECT 1 WHERE NOT NULL::boolean", false),
        ("SELECT 1 WHERE NOT TRUE", false),
        ("SELECT 1 WHERE NOT FALSE", false),
        ("SELECT 1 WHERE NULL::integer=1", false),
        ("SELECT 1 WHERE coalesce(1,abs(-2147483648))=1", false),
        (
            "SELECT 1 WHERE coalesce(NULL::integer,abs(-2147483648))=1",
            true,
        ),
        (
            "SELECT 1 WHERE FALSE AND (TRUE AND abs(-2147483648)=0)",
            false,
        ),
        (
            "SELECT 1 WHERE (FALSE AND TRUE) AND abs(-2147483648)=0",
            false,
        ),
        (
            "SELECT 1 WHERE NULL::boolean AND FALSE AND abs(-2147483648)=0",
            false,
        ),
    ] {
        let r = temporal_state_extended(&mut s, query).await;
        if error {
            assert_eq!(r["stage"], "bind", "{query}: {r}");
            assert_eq!(r["sqlstate"], "22003", "{query}: {r}");
        } else {
            assert!(r["sqlstate"].is_null(), "{query}: {r}");
        }
        let simple = temporal_state_frames(&temporal_state_cycle(&mut s, query).await, "query");
        if error {
            assert_eq!(simple["sqlstate"], "22003", "{query}");
        } else {
            assert!(simple["sqlstate"].is_null(), "{query}: {simple}");
        }
    }
    pg_send(&mut s, b'X', &[]).await;
    drop(s);
    f.stop().await;
}

#[tokio::test]
async fn filter_planning_truth_rows_and_limit_zero_controls() {
    let f = Fixture::new().await;
    let mut s = raw_pg(&f).await;
    for (query, state, rows) in [
        ("SELECT 1 WHERE TRUE AND TRUE", None, 1),
        ("SELECT 1 WHERE TRUE AND FALSE", None, 0),
        ("SELECT 1 WHERE TRUE AND NULL::boolean", None, 0),
        ("SELECT 1 WHERE FALSE AND TRUE", None, 0),
        ("SELECT 1 WHERE FALSE AND FALSE", None, 0),
        ("SELECT 1 WHERE FALSE AND NULL::boolean", None, 0),
        ("SELECT 1 WHERE NULL::boolean AND TRUE", None, 0),
        ("SELECT 1 WHERE NULL::boolean AND FALSE", None, 0),
        ("SELECT 1 WHERE NULL::boolean AND NULL::boolean", None, 0),
        ("SELECT 1 WHERE TRUE OR TRUE", None, 1),
        ("SELECT 1 WHERE TRUE OR FALSE", None, 1),
        ("SELECT 1 WHERE TRUE OR NULL::boolean", None, 1),
        ("SELECT 1 WHERE FALSE OR TRUE", None, 1),
        ("SELECT 1 WHERE FALSE OR FALSE", None, 0),
        ("SELECT 1 WHERE FALSE OR NULL::boolean", None, 0),
        ("SELECT 1 WHERE NULL::boolean OR TRUE", None, 1),
        ("SELECT 1 WHERE NULL::boolean OR FALSE", None, 0),
        ("SELECT 1 WHERE NULL::boolean OR NULL::boolean", None, 0),
        (
            "SELECT 1 FROM app WHERE id=0 AND abs(-2147483648)=0 LIMIT 0",
            Some("22003"),
            0,
        ),
        (
            "SELECT 1 FROM app WHERE FALSE AND abs(id)=0 LIMIT 0",
            None,
            0,
        ),
        (
            "SELECT 1 FROM app WHERE abs(id)=0 AND FALSE LIMIT 0",
            None,
            0,
        ),
        (
            "SELECT 1 FROM app WHERE NOT (abs(-2147483648)=0 AND FALSE) LIMIT 0",
            Some("22003"),
            0,
        ),
        (
            "SELECT 1 FROM app WHERE date_trunc('nonsense',timestamp '2000-01-01') IS NULL LIMIT 0",
            Some("22023"),
            0,
        ),
        (
            "SELECT 1 FROM app WHERE date_trunc('nonsense',timestamptz '2000-01-01 UTC') IS NULL LIMIT 0",
            None,
            0,
        ),
        (
            "SELECT 1 FROM app WHERE abs(-2147483648) IS NULL LIMIT 0",
            Some("22003"),
            0,
        ),
        (
            "SELECT 1 FROM app WHERE abs(-2147483648)=NULL::integer LIMIT 0",
            Some("22003"),
            0,
        ),
        (
            "SELECT 1 FROM app WHERE coalesce(NULL::integer,1)=1 LIMIT 0",
            None,
            0,
        ),
    ] {
        let r = temporal_state_extended(&mut s, query).await;
        match state {
            Some(state) => {
                assert_eq!(r["sqlstate"], state, "{query}: {r}");
                assert_eq!(r["stage"], "bind", "{query}: {r}");
            }
            None => {
                assert!(r["sqlstate"].is_null(), "{query}: {r}");
                assert_eq!(r["data_rows"], rows, "{query}: {r}");
            }
        }
        let frames = temporal_state_cycle(&mut s, query).await;
        if state.is_none() {
            let data: Vec<_> = frames.iter().filter(|x| x.0 == b'D').collect();
            assert_eq!(data.len(), rows);
            for row in data {
                assert_eq!(row.1, vec![0, 1, 0, 0, 0, 1, b'1']);
            }
        }
    }
    pg_send(&mut s, b'X', &[]).await;
    drop(s);
    f.stop().await;
}

#[tokio::test]
async fn filter_planning_portals_abort_and_frame_precedence() {
    let f = Fixture::new().await;
    let mut s = raw_pg(&f).await;
    pg_send(
        &mut s,
        b'P',
        &parse("filter_bad", "SELECT 1 WHERE abs(-2147483648)=0"),
    )
    .await;
    assert!(!pg_sync(&mut s).await.iter().any(|x| x.0 == b'E'));
    let mut bad = bind("bad", "filter_bad", &[], false);
    bad.push(1);
    pg_send(&mut s, b'B', &bad).await;
    assert_eq!(
        temporal_state_frames(&pg_sync(&mut s).await, "bind")["sqlstate"],
        "08P01"
    );
    temporal_state_cycle(&mut s, "BEGIN").await;
    let v = temporal_state_cycle(
        &mut s,
        "INSERT INTO app(id,body) VALUES(888,'rollback-filter')",
    )
    .await;
    assert!(!v.iter().any(|x| x.0 == b'E'));
    pg_send(&mut s, b'B', &bind("bad", "filter_bad", &[], false)).await;
    let v = temporal_state_frames(&pg_sync(&mut s).await, "bind");
    assert_eq!(v["sqlstate"], "22003");
    assert_eq!(v["ready"], "E");
    let v = temporal_state_frames(&temporal_state_cycle(&mut s, "SELECT 1").await, "query");
    assert_eq!(v["sqlstate"], "25P02");
    temporal_state_cycle(&mut s, "ROLLBACK").await;
    let v = temporal_state_frames(
        &temporal_state_cycle(&mut s, "SELECT id FROM app WHERE id=888 LIMIT 1").await,
        "query",
    );
    assert_eq!(v["data_rows"], 0);
    pg_send(&mut s, b'D', b"Pbad\0").await;
    assert_eq!(
        temporal_state_frames(&pg_sync(&mut s).await, "describe")["sqlstate"],
        "34000"
    );
    temporal_state_cycle(&mut s, "BEGIN").await;
    for query in [
        "SELECT 1 WHERE FALSE AND abs(-2147483648)=0",
        "SELECT 1 WHERE TRUE OR abs(-2147483648)=0",
        "SELECT 1 WHERE $1::boolean AND abs(-2147483648)=0",
    ] {
        pg_send(&mut s, b'P', &parse("good", query)).await;
        assert!(!pg_sync(&mut s).await.iter().any(|x| x.0 == b'E'));
        let params = if query.contains('$') {
            vec![Some("false")]
        } else {
            vec![]
        };
        for name in ["one", "two"] {
            pg_send(&mut s, b'B', &bind(name, "good", &params, false)).await;
            assert!(pg_sync(&mut s).await.iter().any(|x| x.0 == b'2'));
            let mut e = vec![];
            cstr(name, &mut e);
            e.extend(0u32.to_be_bytes());
            pg_send(&mut s, b'E', &e).await;
            assert!(!pg_sync(&mut s).await.iter().any(|x| x.0 == b'E'));
            let mut c = vec![b'P'];
            cstr(name, &mut c);
            pg_send(&mut s, b'C', &c).await;
            pg_sync(&mut s).await;
        }
        pg_send(&mut s, b'C', b"Sgood\0").await;
        pg_sync(&mut s).await;
    }
    temporal_state_cycle(&mut s, "ROLLBACK").await;
    // NULL parameter cannot trigger validation-time constant inference.
    pg_send(
        &mut s,
        b'P',
        &parse(
            "param_null",
            "SELECT 1 WHERE $1::boolean AND abs(-2147483648)=0",
        ),
    )
    .await;
    pg_sync(&mut s).await;
    pg_send(
        &mut s,
        b'B',
        &bind("null_portal", "param_null", &[None], false),
    )
    .await;
    assert!(pg_sync(&mut s).await.iter().any(|x| x.0 == b'2'));
    pg_send(&mut s, b'X', &[]).await;
    drop(s);
    f.stop().await;
}

#[tokio::test]
async fn filter_planning_authority_precedes_immutable_errors() {
    let f = Fixture::new().await;
    let (pg, connection) = f.pg(f.app.clone()).await;
    for query in [
        "SELECT 1 FROM other WHERE abs(-2147483648)=0 LIMIT 1",
        "SELECT 1 FROM app a INNER JOIN other o ON a.id=o.id WHERE abs(-2147483648)=0 LIMIT 1",
    ] {
        let e = pg.prepare(query).await.unwrap_err();
        assert_eq!(e.code().unwrap().code(), "42501", "{query}: {e}");
    }
    drop(pg);
    connection.await.unwrap();
    let (pg, connection) = f.pg(f.task.clone()).await;
    let e = pg
        .prepare("SELECT 1 FROM app WHERE abs(-2147483648)=0 LIMIT 1")
        .await
        .unwrap_err();
    assert_eq!(e.code().unwrap().code(), "42501");
    drop(pg);
    connection.await.unwrap();
    let mut s = raw_pg(&f).await;
    pg_send(
        &mut s,
        b'P',
        &parse(
            "before_revoke",
            "SELECT 1 FROM app WHERE abs(-2147483648)=0 LIMIT 1",
        ),
    )
    .await;
    assert!(!pg_sync(&mut s).await.iter().any(|x| x.0 == b'E'));
    f.service.reload_bindings(vec![]).unwrap();
    pg_send(&mut s, b'B', &bind("revoked", "before_revoke", &[], false)).await;
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(5), s.read_u8())
            .await
            .unwrap()
            .is_err()
    );
    drop(s);
    f.stop().await;
}
