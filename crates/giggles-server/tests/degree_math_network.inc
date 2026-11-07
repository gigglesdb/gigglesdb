#[tokio::test]
async fn degree_math_parse0_describe_bind_execute_and_authority() {
    let f = Fixture::new().await;
    let mut raw = raw_pg(&f).await;
    let settings = temporal_state_cycle(&mut raw, "SET extra_float_digits=1").await;
    assert!(!settings.iter().any(|x| x.0 == b'E'));
    for name in [
        "sind", "cosd", "tand", "cotd", "asind", "acosd", "atand", "atan2d",
    ] {
        let args = if name == "atan2d" { "$1,$2" } else { "$1" };
        let query = format!("SELECT pg_catalog.\"{name}\"({args})");
        pg_send(&mut raw, b'P', &parse("degree", &query)).await;
        pg_send(&mut raw, b'D', b"Sdegree\0").await;
        let frames = pg_sync(&mut raw).await;
        assert!(!frames.iter().any(|x| x.0 == b'E'), "{query}: {frames:?}");
        let expected = if name == "atan2d" { 2 } else { 1 };
        let parameter_types = &frames.iter().find(|x| x.0 == b't').unwrap().1;
        assert_eq!(
            u16::from_be_bytes(parameter_types[..2].try_into().unwrap()),
            expected
        );
        for oid in parameter_types[2..].as_chunks::<4>().0 {
            assert_eq!(u32::from_be_bytes(*oid), 701);
        }
        let params = if name == "atan2d" {
            vec![Some("1"), Some("1")]
        } else {
            vec![Some("0.5")]
        };
        for binary in [false, true] {
            pg_send(&mut raw, b'B', &bind("", "degree", &params, binary)).await;
            pg_send(&mut raw, b'D', b"P\0").await;
            pg_send(&mut raw, b'E', &execute("", 0)).await;
            let frames = pg_sync(&mut raw).await;
            assert!(!frames.iter().any(|x| x.0 == b'E'), "{query}: {frames:?}");
            let row = &frames.iter().find(|x| x.0 == b'D').unwrap().1;
            let corpus: serde_json::Value = serde_json::from_slice(
                &std::fs::read(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/tests/fixtures/dd68/degree-math-wire-values.json"
                ))
                .unwrap(),
            )
            .unwrap();
            let observed = &corpus[name];
            if binary {
                assert_eq!(i32::from_be_bytes(row[2..6].try_into().unwrap()), 8);
                assert_eq!(
                    row[6..]
                        .iter()
                        .map(|b| format!("{b:02x}"))
                        .collect::<String>(),
                    observed["hex"].as_str().unwrap()
                );
            } else {
                assert_eq!(
                    std::str::from_utf8(&row[6..]).unwrap(),
                    observed["text"].as_str().unwrap()
                );
            }
        }
        pg_send(
            &mut raw,
            b'B',
            &bind("", "degree", &vec![None; expected as usize], true),
        )
        .await;
        pg_send(&mut raw, b'E', &execute("", 0)).await;
        let frames = pg_sync(&mut raw).await;
        assert_eq!(
            &frames.iter().find(|x| x.0 == b'D').unwrap().1[2..],
            &(-1i32).to_be_bytes()
        );
        pg_send(&mut raw, b'C', b"Sdegree\0").await;
        assert!(!pg_sync(&mut raw).await.iter().any(|x| x.0 == b'E'));
    }
    for (query, code) in [
        ("SELECT sind()", "42883"),
        ("SELECT sind(TRUE)", "42883"),
        ("SELECT sind('30'::text)", "42883"),
        ("SELECT atan2d(1)", "42883"),
        ("SELECT asind(2)", "22003"),
        ("SELECT sind('Infinity')", "22003"),
        ("SELECT public.sind(30)", "42501"),
        // Existing no-FROM authority classification rejects unknown routines.
        ("SELECT pg_catalog.unknown_degree(1)", "0A000"),
        ("SELECT pg_catalog.unknown_degree(1) FROM app", "0A000"),
    ] {
        let result = temporal_state_extended(&mut raw, query).await;
        assert_eq!(result["sqlstate"], code, "{query}: {result}");
    }
    pg_send(&mut raw, b'X', &[]).await;
    drop(raw);
    let (pg, connection) = f.pg(f.app.clone()).await;
    pg.execute("INSERT INTO app(id,body) VALUES(30,'degree')", &[])
        .await
        .unwrap();
    assert_eq!(
        pg.query_one("SELECT sind(id) FROM app WHERE id=30", &[])
            .await
            .unwrap()
            .get::<_, f64>(0),
        0.5
    );
    assert_eq!(
        pg.query_one("SELECT sind('30')", &[])
            .await
            .unwrap()
            .get::<_, f64>(0),
        0.5
    );
    drop(pg);
    connection.await.unwrap();
    let mut task = prepared_network::raw_task(&f).await;
    let before = f.admin.snapshot().unwrap();
    // Task identities retain the existing SQL authority gate, including pure calls.
    for query in ["SELECT sind(30)", "SELECT sind(id) FROM app"] {
        let response = temporal_state_extended(&mut task, query).await;
        assert_eq!(response["sqlstate"], "42501", "{response}");
        assert_eq!(response["stage"], "parse", "{response}");
        assert_eq!(response["data_rows"], 0, "{response}");
        assert_eq!(response["tags"], serde_json::json!(["E", "Z"]));
    }
    assert_eq!(f.admin.snapshot().unwrap(), before);
    pg_send(&mut task, b'X', &[]).await;
    drop(task);
    f.stop().await;
}
