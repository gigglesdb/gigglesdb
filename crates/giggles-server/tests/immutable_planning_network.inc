#[tokio::test]
async fn immutable_planning_bind_validation_and_failed_portal_cleanup() {
    let f = Fixture::new().await;
    let mut s = raw_pg(&f).await;
    // PostgreSQL controls: immutable timestamp overload folds, stable timestamptz stays Execute.
    for (q, stage) in [
        ("SELECT make_time(24,1,0) LIMIT 0", "bind"),
        (
            "SELECT date_trunc('nonsense',timestamp '2000-01-01')",
            "bind",
        ),
        (
            "SELECT date_trunc('nonsense',timestamptz '2000-01-01 UTC')",
            "execute",
        ),
        ("SELECT make_timestamptz(2000,13,1,0,0,0,'UTC')", "execute"),
    ] {
        let result = temporal_state_extended(&mut s, q).await;
        assert_eq!(result["stage"], stage, "{q}");
        assert_eq!(result["data_rows"], 0);
        assert_eq!(result["ready"], "I");
    }
    pg_send(&mut s, b'P', &parse("fold", "SELECT make_time(24,1,0)")).await;
    assert!(!pg_sync(&mut s).await.iter().any(|x| x.0 == b'E'));
    // Whole-frame validation must win over constant errors.
    let mut malformed = bind("bad", "fold", &[], false);
    malformed.push(1);
    pg_send(&mut s, b'B', &malformed).await;
    let r = temporal_state_frames(&pg_sync(&mut s).await, "bind");
    assert_eq!(r["sqlstate"], "08P01");
    // Failed Bind never publishes its named portal.
    pg_send(&mut s, b'B', &bind("bad", "fold", &[], false)).await;
    assert_eq!(
        temporal_state_frames(&pg_sync(&mut s).await, "bind")["sqlstate"],
        "22008"
    );
    pg_send(&mut s, b'D', b"Pbad\0").await;
    assert_eq!(
        temporal_state_frames(&pg_sync(&mut s).await, "describe")["sqlstate"],
        "34000"
    );
    temporal_state_cycle(&mut s, "BEGIN").await;
    // Valid null/stable/immutable controls survive repeated Bind and multiple portals.
    for (query, params, expected) in [
        ("SELECT coalesce(1,abs(-2147483648))", vec![], Some("1")),
        ("SELECT make_time(NULL,1,0)", vec![], None),
        (
            "SELECT date_trunc('day',timestamptz '2000-01-01 UTC')",
            vec![],
            Some("2000-01-01 00:00:00+00"),
        ),
        (
            "SELECT coalesce($1::integer,abs(-2147483648))",
            vec![Some("1")],
            Some("1"),
        ),
    ] {
        pg_send(&mut s, b'P', &parse("", query)).await;
        assert!(!pg_sync(&mut s).await.iter().any(|x| x.0 == b'E'));
        for name in ["", "one", "two"] {
            pg_send(&mut s, b'B', &bind(name, "", &params, false)).await;
            let frames = pg_sync(&mut s).await;
            assert!(frames.iter().any(|x| x.0 == b'2'));
            let mut payload = Vec::new();
            cstr(name, &mut payload);
            payload.extend(0u32.to_be_bytes());
            pg_send(&mut s, b'E', &payload).await;
            let frames = pg_sync(&mut s).await;
            assert!(!frames.iter().any(|x| x.0 == b'E'), "{query}: {frames:?}");
            let row = &frames.iter().find(|x| x.0 == b'D').unwrap().1;
            assert_eq!(&row[..2], &[0, 1]);
            let length = i32::from_be_bytes(row[2..6].try_into().unwrap());
            match expected {
                None => assert_eq!(length, -1),
                Some(value) => {
                    assert_eq!(length as usize, value.len());
                    assert_eq!(&row[6..], value.as_bytes());
                }
            }
            let mut payload = vec![b'P'];
            cstr(name, &mut payload);
            pg_send(&mut s, b'C', &payload).await;
            assert!(!pg_sync(&mut s).await.iter().any(|x| x.0 == b'E'));
        }
    }
    temporal_state_cycle(&mut s, "ROLLBACK").await;
    pg_send(&mut s, b'X', &[]).await;
    drop(s);
    f.stop().await;
}
