// DD68 POSITION PG18 wire corpus with closed, documented deviations.
fn position_wire_oids(payload: &[u8]) -> Vec<u32> {
    let fields = u16::from_be_bytes(payload[..2].try_into().unwrap());
    let mut at = 2;
    let mut oids = Vec::new();
    for _ in 0..fields {
        at += payload[at..].iter().position(|b| *b == 0).unwrap() + 1;
        oids.push(u32::from_be_bytes(
            payload[at + 6..at + 10].try_into().unwrap(),
        ));
        at += 18;
    }
    oids
}

// (case, Some(SQLSTATE) or None = PG18 SQLSTATE, stage, rationale)
const POSITION_WIRE_DEVIATIONS: &[(u64, Option<&str>, &str, &str)] = &[
    (
        51,
        Some("42883"),
        "parse",
        "GD-0001 Q8 decided (DD-0068 F9): pgwire follows PG18 42883 for deferred-routine-only queries; native surfaces keep 42501.",
    ),
    (
        52,
        Some("42883"),
        "parse",
        "GD-0001 Q8 decided (DD-0068 F9): pgwire follows PG18 42883 for deferred-routine-only queries; native surfaces keep 42501.",
    ),
    (
        71,
        Some("42501"),
        "parse",
        "GD-0001 Q8 (open): application-session authority rejects table-free aggregates. PG18 returns 2.",
    ),
    (
        72,
        Some("42501"),
        "parse",
        "GD-0001 Q8 (open): application-session authority rejects table-free aggregates. PG18 returns 2.",
    ),
    (
        76,
        Some("0A000"),
        "parse",
        "A declared parameter type that conflicts with the inferred POSITION signature is rejected as unsupported before lookup. PG18: 42883.",
    ),
    (
        77,
        Some("0A000"),
        "parse",
        "A declared parameter type that conflicts with the inferred POSITION signature is rejected as unsupported before lookup. PG18: 42883.",
    ),
    (
        89,
        Some("42501"),
        "parse",
        "GD-0001 Q8 (open): outer aggregate authority rejects before nested aggregate analysis. PG18: 42883.",
    ),
];

fn position_assert_wire_case(
    case: &serde_json::Value,
    binary: bool,
    frames: &[(u8, Vec<u8>)],
    stage: &str,
    parameter_oids: Option<Vec<u32>>,
    result_oids: Option<Vec<u32>>,
    deviation: Option<(Option<&str>, &str, &str)>,
) {
    let actual = temporal_state_frames(frames, stage);
    let pg = &case["pg"][usize::from(binary)];
    let (code, expected_stage) = deviation
        .map(|(code, stage, rationale)| {
            assert!(!rationale.is_empty());
            (
                code.map_or_else(|| pg["sqlstate"].clone(), |code| serde_json::json!(code)),
                stage,
            )
        })
        .unwrap_or_else(|| (pg["sqlstate"].clone(), pg["stage"].as_str().unwrap()));
    assert_eq!(actual["sqlstate"], code, "{case}: {actual}");
    if !code.is_null() {
        assert_eq!(actual["stage"], expected_stage, "{case}: {actual}");
        assert_eq!(actual["data_rows"], 0, "{case}: {actual}");
    }
    if deviation.is_none() {
        assert_eq!(
            serde_json::to_value(parameter_oids).unwrap(),
            pg["parameter_oids"],
            "{case}"
        );
        assert_eq!(
            serde_json::to_value(result_oids).unwrap(),
            pg["result_oids"],
            "{case}"
        );
        let rows: Vec<_> = frames
            .iter()
            .filter(|f| f.0 == b'D')
            .map(|f| hex::encode(&f.1))
            .collect();
        assert_eq!(serde_json::to_value(rows).unwrap(), pg["rows"], "{case}");
    }
}

#[tokio::test]
async fn position_wire_cases_match_pg18() {
    let f = Fixture::new().await;
    let mut raw = raw_pg(&f).await;
    let corpus: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/dd68/position-wire-cases.json")).unwrap();
    assert_eq!(corpus["cases"].as_array().unwrap().len(), 88);
    let mut exercised = BTreeSet::new();
    let mut total = 0;
    for case in corpus["cases"].as_array().unwrap() {
        let id = case["id"].as_u64().unwrap();
        for binary in [false, true] {
            let query = case["query"].as_str().unwrap();
            let oids: Vec<_> = case["oids"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_u64().unwrap() as u32)
                .collect();
            pg_send(&mut raw, b'P', &parse_oids("", query, &oids)).await;
            pg_send(&mut raw, b'D', b"S\0").await;
            let described = pg_sync(&mut raw).await;
            let parameter_oids = described.iter().find(|f| f.0 == b't').map(|f| {
                f.1[2..]
                    .as_chunks::<4>()
                    .0
                    .iter()
                    .map(|b| u32::from_be_bytes(*b))
                    .collect::<Vec<_>>()
            });
            let result_oids = described
                .iter()
                .find(|f| f.0 == b'T')
                .map(|f| position_wire_oids(&f.1));
            let (frames, stage) = if described.iter().any(|f| f.0 == b'E') {
                (
                    described.clone(),
                    if described.iter().any(|f| f.0 == b'1') {
                        "describe"
                    } else {
                        "parse"
                    },
                )
            } else {
                let mut payload = vec![0, 0];
                let binary_inputs = case["binary_inputs"] == true;
                if binary_inputs {
                    payload.extend(1u16.to_be_bytes());
                    payload.extend(1u16.to_be_bytes());
                } else {
                    payload.extend(0u16.to_be_bytes());
                }
                let params = case["params"].as_array().unwrap();
                payload.extend((params.len() as u16).to_be_bytes());
                for value in params {
                    if value.is_null() {
                        payload.extend((-1i32).to_be_bytes());
                    } else {
                        let bytes = if binary_inputs {
                            hex::decode(value.as_str().unwrap()).unwrap()
                        } else {
                            value.as_str().unwrap().as_bytes().to_vec()
                        };
                        payload.extend((bytes.len() as i32).to_be_bytes());
                        payload.extend(bytes);
                    }
                }
                payload.extend(1u16.to_be_bytes());
                payload.extend(u16::from(binary).to_be_bytes());
                pg_send(&mut raw, b'B', &payload).await;
                pg_send(&mut raw, b'E', &execute("", 0)).await;
                let frames = pg_sync(&mut raw).await;
                let stage = if frames.iter().any(|f| f.0 == b'2') {
                    "execute"
                } else {
                    "bind"
                };
                (frames, stage)
            };
            let deviation = POSITION_WIRE_DEVIATIONS
                .iter()
                .find(|d| d.0 == id)
                .map(|d| {
                    exercised.insert(id);
                    (d.1, d.2, d.3)
                });
            position_assert_wire_case(
                case,
                binary,
                &frames,
                stage,
                parameter_oids,
                result_oids,
                deviation,
            );
            total += 1;
        }
    }
    assert_eq!(total, 176);
    assert_eq!(
        exercised,
        POSITION_WIRE_DEVIATIONS.iter().map(|d| d.0).collect()
    );
    pg_send(&mut raw, b'X', &[]).await;
    drop(raw);
    f.stop().await;
}

#[tokio::test]
async fn position_binary_input_cases_match_pg18() {
    let corpus: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/dd68/position-wire-cases.json")).unwrap();
    assert_eq!(corpus["binary_input_cases"].as_array().unwrap().len(), 4);
    let f = Fixture::new().await;
    let mut raw = raw_pg(&f).await;
    let mut total = 0;
    for case in corpus["binary_input_cases"].as_array().unwrap() {
        for binary in [false, true] {
            let oids: Vec<_> = case["oids"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_u64().unwrap() as u32)
                .collect();
            let query = case["query"].as_str().unwrap();
            pg_send(&mut raw, b'P', &parse_oids("", query, &oids)).await;
            pg_send(&mut raw, b'D', b"S\0").await;
            let described = pg_sync(&mut raw).await;
            let parameter_oids = described.iter().find(|f| f.0 == b't').map(|f| {
                f.1[2..]
                    .as_chunks::<4>()
                    .0
                    .iter()
                    .map(|b| u32::from_be_bytes(*b))
                    .collect::<Vec<_>>()
            });
            let result_oids = described
                .iter()
                .find(|f| f.0 == b'T')
                .map(|f| position_wire_oids(&f.1));
            let (frames, stage) = if described.iter().any(|f| f.0 == b'E') {
                (
                    described.clone(),
                    if described.iter().any(|f| f.0 == b'1') {
                        "describe"
                    } else {
                        "parse"
                    },
                )
            } else {
                let mut payload = vec![0, 0];
                payload.extend(1u16.to_be_bytes());
                payload.extend(1u16.to_be_bytes());
                let params = case["params"].as_array().unwrap();
                payload.extend((params.len() as u16).to_be_bytes());
                for p in params {
                    if p.is_null() {
                        payload.extend((-1i32).to_be_bytes());
                    } else {
                        let bytes = hex::decode(p.as_str().unwrap()).unwrap();
                        payload.extend((bytes.len() as i32).to_be_bytes());
                        payload.extend(bytes);
                    }
                }
                payload.extend(1u16.to_be_bytes());
                payload.extend(u16::from(binary).to_be_bytes());
                pg_send(&mut raw, b'B', &payload).await;
                pg_send(&mut raw, b'E', &execute("", 0)).await;
                let frames = pg_sync(&mut raw).await;
                let stage = if frames.iter().any(|f| f.0 == b'2') {
                    "execute"
                } else {
                    "bind"
                };
                (frames, stage)
            };
            position_assert_wire_case(
                case,
                binary,
                &frames,
                stage,
                parameter_oids,
                result_oids,
                None,
            );
            total += 1;
        }
    }
    assert_eq!(total, 8);
    pg_send(&mut raw, b'X', &[]).await;
    drop(raw);
    f.stop().await;
}

async fn position_control_extended(
    raw: &mut tokio_rustls::client::TlsStream<TcpStream>,
    query: &str,
    params: &[Option<&str>],
) -> serde_json::Value {
    pg_send(raw, b'P', &parse("", query)).await;
    pg_send(raw, b'D', b"S\0").await;
    let described = pg_sync(raw).await;
    let (frames, stage) = if described.iter().any(|f| f.0 == b'E') {
        (
            described.clone(),
            if described.iter().any(|f| f.0 == b'1') {
                "describe"
            } else {
                "parse"
            },
        )
    } else {
        pg_send(raw, b'B', &bind("", "", params, false)).await;
        pg_send(raw, b'E', &execute("", 0)).await;
        let frames = pg_sync(raw).await;
        let stage = if frames.iter().any(|f| f.0 == b'2') {
            "execute"
        } else {
            "bind"
        };
        (frames, stage)
    };
    serde_json::json!({"query":query,"params":params,"actual":temporal_state_frames(&frames,stage),"described_frames":described.iter().map(|f|serde_json::json!({"tag":char::from(f.0).to_string(),"hex":hex::encode(&f.1)})).collect::<Vec<_>>(),"frames":frames.iter().map(|f|serde_json::json!({"tag":char::from(f.0).to_string(),"hex":hex::encode(&f.1)})).collect::<Vec<_>>()})
}

fn position_state_digest(f: &Fixture, label: &str) -> String {
    hex::encode(
        f.admin
            .export(f.path.join(format!("position-state-{label}.export")))
            .unwrap(),
    )
}

fn position_control_metadata(frames: &serde_json::Value) -> (serde_json::Value, serde_json::Value) {
    let (mut parameters, mut results) = (serde_json::Value::Null, serde_json::Value::Null);
    if let Some(frames) = frames.as_array() {
        for frame in frames {
            let payload = hex::decode(frame["hex"].as_str().unwrap()).unwrap();
            if frame["tag"] == "t" {
                parameters = serde_json::json!(
                    payload[2..]
                        .as_chunks::<4>()
                        .0
                        .iter()
                        .map(|b| u32::from_be_bytes(*b))
                        .collect::<Vec<_>>()
                );
            }
            if frame["tag"] == "T" {
                results = serde_json::json!(position_wire_oids(&payload));
            }
        }
    }
    (parameters, results)
}

#[tokio::test]
async fn position_controls_match_contract() {
    let f = Fixture::new().await;
    let mut raw = raw_pg(&f).await;
    f.admin
        .sql_session()
        .execute(
            "INSERT INTO app(id,body,data) VALUES(1,'aé😀z',decode('1000ff20','hex'))",
            &[],
            None,
        )
        .unwrap();
    let policy: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/dd68/position-controls.json")).unwrap();
    let cases = policy["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 35);
    let mut report = Vec::new();
    for case in &cases[..16] {
        report
            .push(position_control_extended(&mut raw, case["query"].as_str().unwrap(), &[]).await);
    }
    let order: Vec<_> = cases
        .iter()
        .filter(|case| !case["pg18"].is_null())
        .collect();
    assert_eq!(
        order
            .iter()
            .map(|case| case["index"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        (16..24).collect::<Vec<_>>()
    );
    for case in order {
        let params: Vec<_> = case["params"]
            .as_array()
            .unwrap()
            .iter()
            .map(serde_json::Value::as_str)
            .collect();
        report.push(
            position_control_extended(&mut raw, case["query"].as_str().unwrap(), &params).await,
        );
    }
    let huge = format!("'{}'", "x".repeat(65_537));
    let before_admission = position_state_digest(&f, "before-admission");
    for query in [
        format!("INSERT INTO app(id,num) VALUES(2,position({huge} IN 'x'))"),
        format!("INSERT INTO app(id,num) VALUES(2,position('x' IN {huge}))"),
    ] {
        report.push(position_control_extended(&mut raw, &query, &[]).await);
    }
    let after_admission = position_state_digest(&f, "after-admission");
    report.push(
        position_control_extended(
            &mut raw,
            "INSERT INTO app(id,num) VALUES(1,position('b' IN 'abc'))",
            &[],
        )
        .await,
    );
    let after_position_duplicate = position_state_digest(&f, "after-position-duplicate");
    report.push(
        position_control_extended(&mut raw, "INSERT INTO app(id,num) VALUES(1,2)", &[]).await,
    );
    let after_failed_insert = position_state_digest(&f, "after-failed-insert");
    pg_send(
        &mut raw,
        b'P',
        &parse("position_bad_bytea", "SELECT position($1 IN $2::bytea)"),
    )
    .await;
    let prepared = pg_sync(&mut raw).await;
    pg_send(
        &mut raw,
        b'B',
        &bind(
            "position_missing_portal",
            "position_bad_bytea",
            &[Some("\\xgg"), Some("\\x00")],
            false,
        ),
    )
    .await;
    pg_send(&mut raw, b'E', &execute("position_missing_portal", 0)).await;
    let failed = pg_sync(&mut raw).await;
    pg_send(&mut raw, b'E', &execute("position_missing_portal", 0)).await;
    let absent = pg_sync(&mut raw).await;
    report.push(serde_json::json!({"control":"bad-bytea-no-portal","prepared":temporal_state_frames(&prepared,"parse"),"actual":temporal_state_frames(&failed,"bind"),"absent":temporal_state_frames(&absent,"execute"),"bind_complete":failed.iter().any(|f|f.0==b'2')}));
    temporal_state_cycle(&mut raw, "BEGIN").await;
    report.push(
        position_control_extended(
            &mut raw,
            "INSERT INTO app(id,num) VALUES(2,position('😀' IN 'aé😀z'))",
            &[],
        )
        .await,
    );
    report.push(
        position_control_extended(
            &mut raw,
            "SELECT position($1 IN $2::bytea)",
            &[Some("\\xgg"), Some("\\x00")],
        )
        .await,
    );
    report.push(position_control_extended(&mut raw, "SELECT position('b' IN 'abc')", &[]).await);
    let rollback = temporal_state_cycle(&mut raw, "ROLLBACK").await;
    let after_rollback = position_state_digest(&f, "after-rollback");
    pg_send(&mut raw, b'X', &[]).await;
    drop(raw);
    let mut task = prepared_network::raw_task(&f).await;
    for query in [
        "SELECT position('b' IN 'abc')",
        "SELECT position('b' IN body) FROM app",
    ] {
        report.push(position_control_extended(&mut task, query, &[]).await);
    }
    pg_send(&mut task, b'X', &[]).await;
    drop(task);
    let mut bindings: Vec<Binding> =
        serde_json::from_slice(&std::fs::read(f.path.join("bindings.json")).unwrap()).unwrap();
    let Role::Application { tables, .. } = &mut bindings[0].role else {
        panic!("application fixture")
    };
    *tables = BTreeSet::from(["other".into()]);
    f.service.reload_bindings(bindings).unwrap();
    let mut denied = raw_pg(&f).await;
    report.push(
        position_control_extended(&mut denied, "SELECT position('b' IN body) FROM app", &[]).await,
    );
    pg_send(&mut denied, b'X', &[]).await;
    drop(denied);
    let observed = &report;
    assert_eq!(observed.len(), 35);
    for (actual, expected) in observed.iter().zip(policy["cases"].as_array().unwrap()) {
        // Controls 24 and 25 store their 65,537-byte literal as {huge}.
        let expected_query = expected["query"]
            .as_str()
            .map(|query| query.replace("{huge}", &huge));
        assert_eq!(
            actual["query"].as_str(),
            expected_query.as_deref(),
            "control {} query",
            expected["index"]
        );
        for field in ["params", "control"] {
            assert_eq!(
                actual[field], expected[field],
                "control {} {field}",
                expected["index"]
            );
        }
        assert_eq!(actual["actual"], expected["expected_actual"]);
        if !expected["pg18"].is_null() {
            assert_eq!(
                expected["pg18"]["sqlstate"],
                expected["expected_actual"]["sqlstate"]
            );
            if expected["pg18"]["stage"] != expected["expected_actual"]["stage"] {
                assert!(!expected["note"].as_str().unwrap().is_empty());
            }
        }
        for (actual_field, expected_field) in [
            ("absent", "expected_absent"),
            ("bind_complete", "expected_bind_complete"),
            ("prepared", "expected_prepared"),
        ] {
            assert_eq!(actual[actual_field], expected[expected_field]);
        }
        let rows: Vec<_> = actual["frames"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|f| f["tag"] == "D")
            .map(|f| f["hex"].as_str().unwrap())
            .collect();
        assert_eq!(serde_json::json!(rows), expected["expected_rows"]);
        let (parameters, results) = position_control_metadata(&actual["described_frames"]);
        assert_eq!(parameters, expected["expected_parameter_oids"]);
        assert_eq!(results, expected["expected_result_oids"]);
    }
    assert_eq!(before_admission, after_admission);
    assert_eq!(after_admission, after_position_duplicate);
    assert_eq!(after_position_duplicate, after_failed_insert);
    assert_eq!(after_failed_insert, after_rollback);
    assert_eq!(temporal_state_frames(&rollback, "query")["ready"], "I");
    f.stop().await;
}

fn position_pause_decision(
    admin: &Admin,
    at: giggles_core::DecisionPoint,
) -> (
    tokio::sync::oneshot::Receiver<giggles_core::Cancellation>,
    std::sync::mpsc::Sender<()>,
) {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let tx = std::sync::Mutex::new(Some(tx));
    let (release, wait) = std::sync::mpsc::channel();
    let wait = std::sync::Mutex::new(wait);
    admin.set_decision_hook(Some(Arc::new(move |point| {
        if point == at
            && let Some(tx) = tx.lock().unwrap().take()
        {
            let _ = tx.send(giggles_core::Cancellation::current().unwrap());
            wait.lock()
                .unwrap()
                .recv_timeout(std::time::Duration::from_secs(8))
                .unwrap();
        }
    })));
    (rx, release)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn position_cancellation_and_revocation() {
    for point in [
        giggles_core::DecisionPoint::BeforeDecision,
        giggles_core::DecisionPoint::Decided,
        giggles_core::DecisionPoint::Committed,
    ] {
        let f = Fixture::new().await;
        let before = position_state_digest(&f, "cancel-before");
        let (pg, connection) = f.pg(f.app.clone()).await;
        let token = pg.cancel_token();
        let (entered, release) = position_pause_decision(&f.admin, point);
        let job = tokio::spawn(async move {
            let result = pg
                .execute(
                    "INSERT INTO app(id,num) VALUES(2,position('😀' IN 'aé😀z'))",
                    &[],
                )
                .await;
            (pg, result)
        });
        let context = tokio::time::timeout(std::time::Duration::from_secs(8), entered)
            .await
            .unwrap()
            .unwrap();
        token
            .cancel_query(tokio_postgres_rustls::MakeRustlsConnect::new(
                (*f.app).clone(),
            ))
            .await
            .unwrap();
        if point == giggles_core::DecisionPoint::BeforeDecision {
            tokio::time::timeout(std::time::Duration::from_secs(8), async {
                while context.check().is_ok() {
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
            })
            .await
            .unwrap();
        }
        release.send(()).unwrap();
        let (pg, result) = tokio::time::timeout(std::time::Duration::from_secs(8), job)
            .await
            .unwrap()
            .unwrap();
        f.admin.set_decision_hook(None);
        let state = result
            .as_ref()
            .err()
            .and_then(|e| e.code())
            .map(|s| s.code().to_string());
        let after = position_state_digest(&f, "cancel-after");
        let rows = pg
            .query("SELECT num FROM app WHERE id=2 LIMIT 1", &[])
            .await
            .unwrap();
        let values: Vec<_> = rows.iter().map(|r| r.get::<_, i32>(0)).collect();
        if point == giggles_core::DecisionPoint::BeforeDecision {
            assert_eq!(state.as_deref(), Some("57014"));
            assert_eq!(result.ok(), None);
            assert!(values.is_empty());
            assert_eq!(before, after);
        } else {
            assert_eq!(state, None);
            assert_eq!(result.ok(), Some(1));
            assert_eq!(values, vec![3]);
            assert_ne!(before, after);
        }
        drop(pg);
        connection.await.unwrap();
        f.stop().await;
    }
    let f = Fixture::new().await;
    let mut raw = raw_pg(&f).await;
    let before = position_state_digest(&f, "revoke-before");
    pg_send(
        &mut raw,
        b'P',
        &parse("position_revoked", "SELECT position('b' IN 'abc')"),
    )
    .await;
    let prepared = pg_sync(&mut raw).await;
    assert!(!prepared.iter().any(|f| f.0 == b'E'));
    pg_send(
        &mut raw,
        b'B',
        &bind("position_revoked_portal", "position_revoked", &[], false),
    )
    .await;
    pg_send(&mut raw, b'H', &[]).await;
    let bound = pg_read(&mut raw).await;
    f.service.reload_bindings(vec![]).unwrap();
    pg_send(&mut raw, b'E', &execute("position_revoked_portal", 0)).await;
    let read = tokio::time::timeout(std::time::Duration::from_secs(5), raw.read_u8()).await;
    let outcome = match read {
        Ok(Err(_)) => "closed".to_string(),
        Ok(Ok(tag)) => format!("tag-{}", char::from(tag)),
        Err(_) => "timeout".into(),
    };
    let after = position_state_digest(&f, "revoke-after");
    assert_eq!(bound.0, b'2');
    assert_eq!(outcome, "closed");
    assert_eq!(before, after);
    drop(raw);
    f.stop().await;
}

#[tokio::test]
async fn position_current_text_nul_bind_witness() {
    let f = Fixture::new().await;
    let mut raw = raw_pg(&f).await;
    for binary in [false, true] {
        pg_send(&mut raw, b'P', &parse("", "SELECT position($1 IN $2)")).await;
        pg_send(&mut raw, b'D', b"S\0").await;
        let described = pg_sync(&mut raw).await;
        assert!(!described.iter().any(|f| f.0 == b'E'));
        pg_send(
            &mut raw,
            b'B',
            &bind("", "", &[Some("\0"), Some("abc")], binary),
        )
        .await;
        pg_send(&mut raw, b'E', &execute("", 0)).await;
        let frames = pg_sync(&mut raw).await;
        let actual = temporal_state_frames(&frames, "bind");
        assert_eq!(actual["sqlstate"], "22021");
        assert_eq!(actual["ready"], "I");
        assert_eq!(actual["data_rows"], 0);
        assert!(!frames.iter().any(|f| matches!(f.0, b'2' | b'D' | b'C')));
    }
    pg_send(&mut raw, b'X', &[]).await;
    drop(raw);
    f.stop().await;
}

#[tokio::test]
async fn position_consumer_focus_cases() {
    let policy: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/dd68/position-controls.json")).unwrap();
    let wire: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/dd68/position-wire-cases.json")).unwrap();
    let f = Fixture::new().await;
    let mut raw = raw_pg(&f).await;
    f.admin
        .sql_session()
        .execute(
            "INSERT INTO app(id,body,data) VALUES(1,'aé😀z',decode('1000ff20','hex'))",
            &[],
            None,
        )
        .unwrap();
    for binary in [false, true] {
        let expected = wire["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["id"] == 68)
            .unwrap();
        let actual =
            position_control_extended(&mut raw, expected["query"].as_str().unwrap(), &[]).await;
        let pg = &expected["pg"][usize::from(binary)];
        assert_eq!(actual["actual"]["sqlstate"], pg["sqlstate"]);
        assert_eq!(actual["actual"]["stage"], pg["stage"]);
        let expected = wire["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["id"] == 81)
            .unwrap();
        pg_send(
            &mut raw,
            b'P',
            &parse("", expected["query"].as_str().unwrap()),
        )
        .await;
        pg_send(&mut raw, b'D', b"S\0").await;
        let described = pg_sync(&mut raw).await;
        assert!(!described.iter().any(|f| f.0 == b'E'));
        let payload = bind("", "", &[Some("\\xgg"), Some("\\x00")], binary);
        pg_send(&mut raw, b'B', &payload).await;
        pg_send(&mut raw, b'E', &execute("", 0)).await;
        let frames = pg_sync(&mut raw).await;
        let actual = temporal_state_frames(&frames, "bind");
        let pg = &expected["pg"][usize::from(binary)];
        assert_eq!(actual["sqlstate"], pg["sqlstate"]);
        assert_eq!(actual["stage"], pg["stage"]);
    }
    let before28 = position_state_digest(&f, "focus-before28");
    let query = policy["cases"][30]["query"].as_str().unwrap();
    pg_send(&mut raw, b'P', &parse("position_bad_bytea", query)).await;
    let prepared = pg_sync(&mut raw).await;
    assert_eq!(
        temporal_state_frames(&prepared, "parse"),
        policy["cases"][28]["expected_prepared"]
    );
    let payload = bind(
        "position_missing_portal",
        "position_bad_bytea",
        &[Some("\\xgg"), Some("\\x00")],
        false,
    );
    pg_send(&mut raw, b'B', &payload).await;
    pg_send(&mut raw, b'E', &execute("position_missing_portal", 0)).await;
    let failed28 = pg_sync(&mut raw).await;
    assert_eq!(
        temporal_state_frames(&failed28, "bind"),
        policy["cases"][28]["expected_actual"]
    );
    assert!(!failed28.iter().any(|f| f.0 == b'2'));
    pg_send(&mut raw, b'E', &execute("position_missing_portal", 0)).await;
    let absent = pg_sync(&mut raw).await;
    assert_eq!(
        temporal_state_frames(&absent, "execute"),
        policy["cases"][28]["expected_absent"]
    );
    let after28 = position_state_digest(&f, "focus-after28");
    assert_eq!(before28, after28);
    let before_begin = position_state_digest(&f, "focus-before-BEGIN");
    temporal_state_cycle(&mut raw, "BEGIN").await;
    let insert = position_control_extended(
        &mut raw,
        "INSERT INTO app(id,num) VALUES(2,position('😀' IN 'aé😀z'))",
        &[],
    )
    .await;
    assert_eq!(insert["actual"], policy["cases"][29]["expected_actual"]);
    pg_send(&mut raw, b'P', &parse("", query)).await;
    pg_send(&mut raw, b'D', b"S\0").await;
    let described = pg_sync(&mut raw).await;
    assert!(!described.iter().any(|f| f.0 == b'E'));
    let payload = bind("", "", &[Some("\\xgg"), Some("\\x00")], false);
    pg_send(&mut raw, b'B', &payload).await;
    pg_send(&mut raw, b'E', &execute("", 0)).await;
    let failed30 = pg_sync(&mut raw).await;
    assert_eq!(
        temporal_state_frames(&failed30, "bind"),
        policy["cases"][30]["expected_actual"]
    );
    assert!(!failed30.iter().any(|f| f.0 == b'2'));
    let aborted = position_control_extended(&mut raw, "SELECT position('b' IN 'abc')", &[]).await;
    assert_eq!(aborted["actual"], policy["cases"][31]["expected_actual"]);
    let rollback = temporal_state_cycle(&mut raw, "ROLLBACK").await;
    assert_eq!(temporal_state_frames(&rollback, "query")["ready"], "I");
    let after_rollback = position_state_digest(&f, "focus-after-rollback");
    assert_eq!(before_begin, after_rollback);
    pg_send(&mut raw, b'X', &[]).await;
    drop(raw);
    f.stop().await;
}
