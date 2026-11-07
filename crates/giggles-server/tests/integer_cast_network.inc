fn integer_cast_wire_state(frames: &[(u8, Vec<u8>)]) -> Option<String> {
    frames
        .iter()
        .find(|f| f.0 == b'E')
        .and_then(|f| f.1.split(|b| *b == 0).find(|x| x.first() == Some(&b'C')))
        .map(|x| String::from_utf8(x[1..].to_vec()).unwrap())
}

#[tokio::test]
async fn integer_cast_pg18_421_actual_tls_metadata_values_states_and_phases() {
    let cases: serde_json::Value = serde_json::from_slice(
        &std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/dd68/integer-cast-oracle.json"
        ))
        .unwrap(),
    )
    .unwrap();
    let f = Fixture::new().await;
    let mut raw = raw_pg(&f).await;
    let mut report = Vec::new();
    let mut unexpected = 0;
    let mut phase_gaps = 0;
    let mut known_gaps = 0;
    for c in cases.as_array().unwrap() {
        pg_send(&mut raw, b'P', &parse("", c["query"].as_str().unwrap())).await;
        pg_send(&mut raw, b'D', b"S\0").await;
        let described = pg_sync(&mut raw).await;
        let mut frames = described.clone();
        let mut phase = "parse-describe";
        let mut error = integer_cast_wire_state(&described);
        let mut metadata_matches = true;
        if error.is_none() {
            if let Some(types) = c["observation"]["parameterTypes"].as_array() {
                let mut expected = (types.len() as u16).to_be_bytes().to_vec();
                for ty in types {
                    expected.extend((ty.as_u64().unwrap() as u32).to_be_bytes());
                }
                metadata_matches &= described.iter().any(|f| f.0 == b't' && f.1 == expected);
                let row = described.iter().find(|f| f.0 == b'T').unwrap();
                metadata_matches &= row.1[..2] == [0, 1];
                let name_end = row.1[2..].iter().position(|b| *b == 0).unwrap() + 2;
                let oid_start = name_end + 1 + 6;
                metadata_matches &= row.1[oid_start..oid_start + 4]
                    == (c["observation"]["resultTypes"][0]["oid"].as_u64().unwrap() as u32)
                        .to_be_bytes();
            }
            let mut b = vec![0];
            cstr("", &mut b);
            let params = c["parameters"].as_array().unwrap();
            b.extend((params.len() as u16).to_be_bytes());
            for p in params {
                b.extend(u16::from(p["format"] == "binary").to_be_bytes());
            }
            b.extend((params.len() as u16).to_be_bytes());
            for p in params {
                let bytes = if p["format"] == "binary" {
                    hex::decode(p["hex"].as_str().unwrap()).unwrap()
                } else {
                    p["text"].as_str().unwrap().as_bytes().to_vec()
                };
                b.extend((bytes.len() as u32).to_be_bytes());
                b.extend(bytes);
            }
            b.extend(0u16.to_be_bytes());
            pg_send(&mut raw, b'B', &b).await;
            pg_send(&mut raw, b'E', &[0, 0, 0, 0, 0]).await;
            let executed = pg_sync(&mut raw).await;
            phase = if executed.iter().any(|f| f.0 == b'2') {
                "execute"
            } else {
                "bind"
            };
            error = integer_cast_wire_state(&executed);
            frames.extend(executed);
        }
        let pg = &c["observation"];
        let mut matches = metadata_matches && error.as_deref() == pg["sqlstate"].as_str();
        if pg["status"] == "ok" && error.is_none() {
            let row = frames.iter().find(|f| f.0 == b'D');
            let value = pg["rows"][0][0].as_str().unwrap().as_bytes();
            let mut expected = vec![0, 1];
            expected.extend((value.len() as u32).to_be_bytes());
            expected.extend(value);
            matches &= row.is_some_and(|f| f.1 == expected)
                && frames.iter().any(|f| f.0 == b'C' && f.1 == b"SELECT 1\0");
        }
        let known = c["kind"] == "unknown-function-argument" && pg["sqlstate"] == "42725";
        if !matches {
            if known {
                known_gaps += 1;
            } else {
                unexpected += 1;
            }
        }
        let phase_matches = phase == c["stage"].as_str().unwrap();
        if !phase_matches {
            phase_gaps += 1;
        }
        assert_eq!(frames.iter().rev().find(|f| f.0 == b'Z').unwrap().1, b"I");
        report.push(serde_json::json!({"case":c,"actual_sqlstate":error,"actual_phase":phase,"pg_phase":c["stage"],"phase_matches":phase_matches,"metadata_matches":metadata_matches,"value_state_matches":matches,"known_overload_gap":known,"frames":frames.iter().map(|f|serde_json::json!({"tag":char::from(f.0).to_string(),"hex":hex::encode(&f.1)})).collect::<Vec<_>>()}));
    }
    let root = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("dd68")
        .join("dd68-integer-cast-design-20261106");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(
        root.join(format!("tls-cast-{}.json", std::process::id())),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    println!(
        "cast_tls_cases={}, unexpected_value_state_metadata_gaps={unexpected}, known_overload_gaps={known_gaps}, phase_gaps={phase_gaps}",
        report.len()
    );
    pg_send(&mut raw, b'X', &[]).await;
    drop(raw);
    f.stop().await;
    assert_eq!(unexpected, 0);
}

#[tokio::test]
async fn integer_cast_text_error_aborts_without_portal_or_effects_tls() {
    let f = Fixture::new().await;
    let mut raw = raw_pg(&f).await;
    let mut report = Vec::new();
    for (text, state) in [("1__0", "22P02"), ("21474836480x", "22003")] {
        assert_eq!(
            temporal_state_frames(&temporal_state_cycle(&mut raw, "BEGIN").await, "query")["ready"],
            "T"
        );
        assert!(
            !temporal_state_cycle(&mut raw, "INSERT INTO app(id,body) VALUES(779,'rollback')")
                .await
                .iter()
                .any(|f| f.0 == b'E')
        );
        pg_send(&mut raw, b'P', &parse("", "SELECT $1::text::integer")).await;
        pg_send(&mut raw, b'D', b"S\0").await;
        assert!(pg_sync(&mut raw).await.iter().any(|f| f.0 == b'1'));
        pg_send(&mut raw, b'B', &bind("cast_bad", "", &[Some(text)], false)).await;
        let bound = pg_sync(&mut raw).await;
        assert!(bound.iter().any(|f| f.0 == b'2'));
        assert!(integer_cast_wire_state(&bound).is_none());
        assert_eq!(temporal_state_frames(&bound, "bind")["ready"], "T");
        let mut execute = Vec::new();
        cstr("cast_bad", &mut execute);
        execute.extend(0u32.to_be_bytes());
        pg_send(&mut raw, b'E', &execute).await;
        let error = pg_sync(&mut raw).await;
        assert_eq!(integer_cast_wire_state(&error).as_deref(), Some(state));
        assert!(!error.iter().any(|f| f.0 == b'2'));
        assert_eq!(temporal_state_frames(&error, "execute")["ready"], "E");
        assert!(!error.iter().any(|f| matches!(f.0, b'D' | b'C')));
        report.push(serde_json::json!({"text":text,"sqlstate":state,"pg_error_phase":"bind","actual_error_phase":"execute","phase_matches":false,"bind_frames":bound.iter().map(|f|serde_json::json!({"tag":char::from(f.0).to_string(),"hex":hex::encode(&f.1)})).collect::<Vec<_>>(),"execute_frames":error.iter().map(|f|serde_json::json!({"tag":char::from(f.0).to_string(),"hex":hex::encode(&f.1)})).collect::<Vec<_>>()}));
        assert_eq!(
            integer_cast_wire_state(&temporal_state_cycle(&mut raw, "SELECT 1").await).as_deref(),
            Some("25P02")
        );
        temporal_state_cycle(&mut raw, "ROLLBACK").await;
        assert_eq!(
            temporal_state_frames(
                &temporal_state_cycle(&mut raw, "SELECT id FROM app WHERE id=779 LIMIT 1").await,
                "query"
            )["data_rows"],
            0
        );
        let mut execute = Vec::new();
        cstr("cast_bad", &mut execute);
        execute.extend(0u32.to_be_bytes());
        pg_send(&mut raw, b'E', &execute).await;
        assert_eq!(
            integer_cast_wire_state(&pg_sync(&mut raw).await).as_deref(),
            Some("34000")
        );
    }
    let root = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("dd68")
        .join("dd68-integer-cast-design-20261106");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(
        root.join(format!("tls-cast-rollback-{}.json", std::process::id())),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    pg_send(&mut raw, b'X', &[]).await;
    drop(raw);
    f.stop().await;
}
