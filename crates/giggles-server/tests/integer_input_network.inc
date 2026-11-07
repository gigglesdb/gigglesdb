#[tokio::test]
async fn integer_input_pg18_text_bind_actual_tls_corpus() {
    let f = Fixture::new().await;
    let mut raw = raw_pg(&f).await;
    let cases: serde_json::Value = serde_json::from_slice(
        &std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/dd68/integer-input-wire-corpus.json"
        ))
        .unwrap(),
    )
    .unwrap();
    let mut report = Vec::new();
    for case in cases.as_array().unwrap() {
        pg_send(&mut raw, b'P', &parse("", case["query"].as_str().unwrap())).await;
        pg_send(&mut raw, b'D', b"S\0").await;
        let described = pg_sync(&mut raw).await;
        assert!(described.iter().any(|f| f.0 == b'1'), "{case}");
        let mut types = vec![0, 1];
        types.extend((case["oid"].as_u64().unwrap() as u32).to_be_bytes());
        assert!(
            described.iter().any(|f| f.0 == b't' && f.1 == types),
            "{case}"
        );
        let description = described.iter().find(|f| f.0 == b'T').unwrap();
        assert_eq!(&description.1[..2], &[0, 1], "{case}");
        let field_name_end = description.1[2..].iter().position(|b| *b == 0).unwrap() + 2;
        let oid_start = field_name_end + 1 + 6;
        assert_eq!(
            &description.1[oid_start..oid_start + 4],
            &(case["oid"].as_u64().unwrap() as u32).to_be_bytes(),
            "{case}"
        );
        assert_eq!(&description.1[description.1.len() - 2..], &[0, 0], "{case}");
        assert_eq!(temporal_state_frames(&described, "describe")["ready"], "I");
        assert!(!described.iter().any(|f| f.0 == b'E'), "{case}");
        pg_send(
            &mut raw,
            b'B',
            &bind("", "", &[Some(case["parameter"].as_str().unwrap())], false),
        )
        .await;
        // Bind and Execute share one Sync cycle: autocommit Sync closes portals.
        // On a Bind error, Execute is ignored until Sync and cannot replace it.
        pg_send(&mut raw, b'E', &[0, 0, 0, 0, 0]).await;
        let bound = pg_sync(&mut raw).await;
        let stage = if case["sqlstate"].is_null() {
            "execute"
        } else {
            "bind"
        };
        let actual = temporal_state_frames(&bound, stage);
        assert_eq!(actual["sqlstate"], case["sqlstate"], "{case}: {actual}");
        assert_eq!(actual["ready"], "I", "{case}");
        assert_eq!(
            bound.iter().any(|f| f.0 == b'2'),
            case["sqlstate"].is_null(),
            "{case}"
        );
        let frames = bound;
        if case["sqlstate"].is_null() {
            assert_eq!(actual["data_rows"], 1, "{case}");
            assert_eq!(actual["error_responses"], 0, "{case}");
            assert!(
                frames.iter().any(|f| f.0 == b'C' && f.1 == b"SELECT 1\0"),
                "{case}"
            );
            let row = frames.iter().find(|f| f.0 == b'D').unwrap();
            let text = case["value"].as_str().unwrap().as_bytes();
            let mut expected = vec![0, 1];
            expected.extend((text.len() as u32).to_be_bytes());
            expected.extend(text);
            assert_eq!(row.1, expected, "{case}");
        } else {
            assert_eq!(actual["error_responses"], 1, "{case}");
            assert_eq!(actual["data_rows"], 0, "{case}");
            assert!(!frames.iter().any(|f| f.0 == b'C'), "{case}");
        }
        report.push(serde_json::json!({"case":case,"observed":actual,"frames":frames.iter().map(|f|serde_json::json!({"tag":char::from(f.0).to_string(),"hex":hex::encode(&f.1)})).collect::<Vec<_>>()}));
    }
    let root = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("dd68")
        .join("dd68-integer-input-20261106");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(
        root.join(format!("tls-corpus-{}.json", std::process::id())),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    println!(
        "integer_input_actual_tls_cases={}, parity_cases={}, nul_admission_controls={}",
        cases.as_array().unwrap().len(),
        cases
            .as_array()
            .unwrap()
            .iter()
            .filter(|c| c["exclusion"].is_null())
            .count(),
        cases
            .as_array()
            .unwrap()
            .iter()
            .filter(|c| !c["exclusion"].is_null())
            .count()
    );
    pg_send(&mut raw, b'X', &[]).await;
    drop(raw);
    f.stop().await;
}

#[tokio::test]
async fn integer_input_errors_abort_without_portal_or_effects_actual_tls() {
    let f = Fixture::new().await;
    let mut raw = raw_pg(&f).await;
    for (text, expected) in [
        (" 2147483648 ", "22003"),
        ("0x80000000", "22003"),
        ("2147483648x", "22P02"),
        ("21474836480x", "22003"),
        ("1__0", "22P02"),
    ] {
        assert_eq!(
            temporal_state_frames(&temporal_state_cycle(&mut raw, "BEGIN").await, "query")["ready"],
            "T"
        );
        assert!(
            !temporal_state_cycle(&mut raw, "INSERT INTO app(id,body) VALUES(778,'rollback')")
                .await
                .iter()
                .any(|f| f.0 == b'E')
        );
        pg_send(&mut raw, b'P', &parse("", "SELECT $1::integer")).await;
        pg_send(&mut raw, b'D', b"S\0").await;
        assert!(pg_sync(&mut raw).await.iter().any(|f| f.0 == b'1'));
        pg_send(
            &mut raw,
            b'B',
            &bind("integer_rejected", "", &[Some(text)], false),
        )
        .await;
        let bound = pg_sync(&mut raw).await;
        let observed = temporal_state_frames(&bound, "bind");
        assert_eq!(observed["sqlstate"], expected, "{text}");
        assert_eq!(observed["ready"], "E");
        assert!(!bound.iter().any(|f| f.0 == b'2'));
        assert_eq!(
            temporal_state_frames(&temporal_state_cycle(&mut raw, "SELECT 1").await, "query")["sqlstate"],
            "25P02"
        );
        assert_eq!(
            temporal_state_frames(&temporal_state_cycle(&mut raw, "ROLLBACK").await, "query")["ready"],
            "I"
        );
        assert_eq!(
            temporal_state_frames(
                &temporal_state_cycle(&mut raw, "SELECT id FROM app WHERE id=778 LIMIT 1").await,
                "query"
            )["data_rows"],
            0
        );
        let mut execute = Vec::new();
        cstr("integer_rejected", &mut execute);
        execute.extend(0u32.to_be_bytes());
        pg_send(&mut raw, b'E', &execute).await;
        assert_eq!(
            temporal_state_frames(&pg_sync(&mut raw).await, "execute")["sqlstate"],
            "34000"
        );
    }
    pg_send(&mut raw, b'X', &[]).await;
    drop(raw);
    f.stop().await;
}
