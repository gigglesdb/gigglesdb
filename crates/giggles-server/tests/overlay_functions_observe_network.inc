// DD68 OVERLAY PG18 wire corpus with closed, documented deviations.
fn overlay_result_oids(payload: &[u8]) -> Vec<u32> {
    let fields = u16::from_be_bytes(payload[..2].try_into().unwrap()) as usize;
    let mut at = 2;
    let mut result = Vec::with_capacity(fields);
    for _ in 0..fields {
        at += payload[at..].iter().position(|b| *b == 0).unwrap() + 1;
        result.push(u32::from_be_bytes(
            payload[at + 6..at + 10].try_into().unwrap(),
        ));
        at += 18;
    }
    result
}
// (case, Some(SQLSTATE) or None = PG18 SQLSTATE, stage, rationale)
const OVERLAY_WIRE_DEVIATIONS: &[(usize, Option<&str>, &str, &str)] = &[
    (
        111,
        Some("42804"),
        "parse",
        "Inconsistent parameter type deduction is reported as 42804; no 42P08 mapping. PG18: 42P08.",
    ),
    (
        118,
        None,
        "execute",
        "Parameter-dependent evaluation runs at Execute; PG18 custom planning reports the same SQLSTATE at Bind.",
    ),
    (
        129,
        Some("42883"),
        "parse",
        "GD-0001 Q8 decided (DD-0068 F9): pgwire follows PG18 42883 for deferred-routine-only queries; native surfaces keep 42501.",
    ),
    (
        147,
        Some("42501"),
        "parse",
        "GD-0001 Q8 (open): application-session authority rejects a table-free column reference before name resolution. PG18: 42703.",
    ),
    (
        148,
        Some("42501"),
        "parse",
        "GD-0001 Q8 (open): application-session authority rejects a table-free column reference before name resolution. PG18: 42703.",
    ),
    (
        149,
        Some("42501"),
        "parse",
        "GD-0001 Q8 (open): application-session authority rejects a table-free column reference before name resolution. PG18: 42703.",
    ),
];
const OVERLAY_ADVERSARIAL_DEVIATIONS: &[(&str, Option<&str>, &str, &str)] = &[
    (
        "overlay_int8_outside_int4",
        None,
        "execute",
        "Parameter-dependent evaluation runs at Execute; PG18 custom planning reports the same SQLSTATE at Bind.",
    ),
    (
        "overlay_min_min",
        None,
        "execute",
        "Parameter-dependent evaluation runs at Execute; PG18 custom planning reports the same SQLSTATE at Bind.",
    ),
    (
        "overlay_min_max",
        None,
        "execute",
        "Parameter-dependent evaluation runs at Execute; PG18 custom planning reports the same SQLSTATE at Bind.",
    ),
    (
        "overlay_max_max",
        None,
        "execute",
        "Parameter-dependent evaluation runs at Execute; PG18 custom planning reports the same SQLSTATE at Bind.",
    ),
    (
        "overlay_one_max",
        None,
        "execute",
        "Parameter-dependent evaluation runs at Execute; PG18 custom planning reports the same SQLSTATE at Bind.",
    ),
    (
        "overlay_zero_max",
        None,
        "execute",
        "Parameter-dependent evaluation runs at Execute; PG18 custom planning reports the same SQLSTATE at Bind.",
    ),
    (
        "overlay_zero_min",
        None,
        "execute",
        "Parameter-dependent evaluation runs at Execute; PG18 custom planning reports the same SQLSTATE at Bind.",
    ),
    (
        "overlay_upper",
        Some("42883"),
        "parse",
        "GD-0001 Q8 decided (DD-0068 F9): pgwire follows PG18 42883 for deferred-routine-only queries; native surfaces keep 42501.",
    ),
    (
        "overlay_column",
        Some("42601"),
        "parse",
        "Derived table FROM (VALUES ...) alias(column) is not supported by the parser. PG18 returns the row.",
    ),
    (
        "overlay_cast_order",
        Some("22P02"),
        "execute",
        "Parameter-dependent cast evaluation runs at Execute; PG18 custom planning reports 22P02 at Bind.",
    ),
];

fn overlay_assert_case(
    case: &serde_json::Value,
    frames: &[(u8, Vec<u8>)],
    stage: &str,
    parameter_oids: Option<Vec<u32>>,
    result_oids: Option<Vec<u32>>,
    rows_key: &str,
    deviation: Option<(Option<&str>, &str, &str)>,
) {
    let actual = temporal_state_frames(frames, stage);
    let pg = &case["pg"];
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
        assert_eq!(serde_json::to_value(rows).unwrap(), pg[rows_key], "{case}");
    }
}
#[tokio::test]
async fn overlay_wire_cases_match_pg18() {
    let f = Fixture::new().await;
    let mut raw = raw_pg(&f).await;
    let corpus: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/dd68/overlay-wire-cases.json")).unwrap();
    assert_eq!(corpus["cases"].as_array().unwrap().len(), 156);
    let mut exercised = BTreeSet::new();
    let mut total = 0;
    for case in corpus["cases"].as_array().unwrap() {
        let index = case["index"].as_u64().unwrap() as usize;
        for binary in [false, true] {
            let query = case["query"].as_str().unwrap();
            let params: Vec<_> = case["params"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str())
                .collect();
            pg_send(&mut raw, b'P', &parse("", query)).await;
            pg_send(&mut raw, b'D', b"S\0").await;
            let described = pg_sync(&mut raw).await;
            let parse_error = described.iter().any(|f| f.0 == b'E');
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
                .map(|f| overlay_result_oids(&f.1));
            let (frames, stage) = if parse_error {
                (described.clone(), "parse")
            } else {
                pg_send(&mut raw, b'B', &bind("", "", &params, binary)).await;
                pg_send(&mut raw, b'E', &execute("", 0)).await;
                let frames = pg_sync(&mut raw).await;
                let stage = if frames.iter().any(|f| f.0 == b'2') {
                    "execute"
                } else {
                    "bind"
                };
                (frames, stage)
            };
            let deviation = OVERLAY_WIRE_DEVIATIONS
                .iter()
                .find(|d| d.0 == index)
                .map(|d| {
                    exercised.insert(index);
                    (d.1, d.2, d.3)
                });
            overlay_assert_case(
                case,
                &frames,
                stage,
                parameter_oids,
                result_oids,
                if binary { "rows_binary" } else { "rows_text" },
                deviation,
            );
            total += 1;
        }
    }
    assert_eq!(total, 312);
    assert_eq!(
        exercised,
        OVERLAY_WIRE_DEVIATIONS.iter().map(|d| d.0).collect()
    );
    pg_send(&mut raw, b'X', &[]).await;
    drop(raw);
    f.stop().await;
    println!("OVERLAY 312 wire observations passed");
}
#[tokio::test]
async fn overlay_adversarial_cases_match_pg18() {
    let f = Fixture::new().await;
    let mut raw = raw_pg(&f).await;
    let corpus: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/dd68/overlay-adversarial-cases.json")).unwrap();
    assert_eq!(corpus["cases"].as_array().unwrap().len(), 42);
    let mut exercised = BTreeSet::new();
    let mut total = 0;
    // Include short binary integer input and declared int8-to-int4 overflow;
    // only parameter-dependent cast timing differs from PG18 below.
    for case in corpus["cases"].as_array().unwrap() {
        let id = case["id"].as_str().unwrap();
        let query = case["query"].as_str().unwrap();
        let oids: Vec<_> = case["declared_oids"]
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
            .map(|f| overlay_result_oids(&f.1));
        let (frames, stage) = if described.iter().any(|f| f.0 == b'E') {
            (described.clone(), "parse")
        } else {
            let mut payload = vec![0, 0];
            let formats = case["parameter_formats"].as_array().unwrap();
            payload.extend((formats.len() as u16).to_be_bytes());
            for f in formats {
                payload.extend((f.as_u64().unwrap() as u16).to_be_bytes());
            }
            let parameters = case["parameters"].as_array().unwrap();
            payload.extend((parameters.len() as u16).to_be_bytes());
            for p in parameters {
                if p.is_null() {
                    payload.extend((-1i32).to_be_bytes());
                } else {
                    let bytes = hex::decode(p["hex"].as_str().unwrap()).unwrap();
                    payload.extend((bytes.len() as i32).to_be_bytes());
                    payload.extend(bytes);
                }
            }
            payload.extend(1u16.to_be_bytes());
            payload.extend((case["result_format"].as_u64().unwrap() as u16).to_be_bytes());
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
        let deviation = OVERLAY_ADVERSARIAL_DEVIATIONS
            .iter()
            .find(|d| d.0 == id)
            .map(|d| {
                exercised.insert(id);
                (d.1, d.2, d.3)
            });
        overlay_assert_case(
            case,
            &frames,
            stage,
            parameter_oids,
            result_oids,
            "rows",
            deviation,
        );
        total += 1;
    }
    assert_eq!(total, 42);
    assert_eq!(
        exercised,
        OVERLAY_ADVERSARIAL_DEVIATIONS.iter().map(|d| d.0).collect()
    );
    pg_send(&mut raw, b'X', &[]).await;
    drop(raw);
    f.stop().await;
    println!("OVERLAY 42 wire observations passed");
}
#[tokio::test]
async fn overlay_authority_bind_failure_portal_and_transaction_controls() {
    let f = Fixture::new().await;
    let mut raw = raw_pg(&f).await;
    let before = f.admin.snapshot().unwrap();
    for (query, code) in [
        ("SELECT overlay()", "42883"),                         // PG18: 42883.
        ("SELECT overlay(TRUE,'X',2)", "42883"),               // PG18: 42883.
        ("SELECT public.overlay('abc','X',2)", "42883"),       // PG18: 42883; DD-0068 F9 (Q8 decided)
        ("SELECT pg_catalog.unknown_overlay('abc')", "42883"), // PG18: 42883; DD-0068 F9 (Q8 decided)
        ("SELECT pg_catalog.unknown_overlay('abc') FROM app", "42883"), // PG18: 42883.
        ("SELECT overlay(body,'X',2) FROM other", "42501"),    // PG18: 42P01; authority Q8.
        ("SELECT overlay(missing,'X',2)", "42501"),            // PG18: 42703; authority Q8.
    ] {
        let actual = temporal_state_extended(&mut raw, query).await;
        assert_eq!(actual["sqlstate"], code, "{query}: {actual}");
        assert_eq!(actual["stage"], "parse");
        assert_eq!(actual["data_rows"], 0);
    }
    pg_send(
        &mut raw,
        b'P',
        &parse("bad_overlay", "SELECT overlay('abc','X',0)"),
    )
    .await;
    assert!(!pg_sync(&mut raw).await.iter().any(|f| f.0 == b'E'));
    pg_send(
        &mut raw,
        b'B',
        &bind("bad_overlay_portal", "bad_overlay", &[], false),
    )
    .await;
    pg_send(&mut raw, b'E', &execute("bad_overlay_portal", 0)).await;
    let frames = pg_sync(&mut raw).await;
    let actual = temporal_state_frames(&frames, "bind");
    assert_eq!(actual["sqlstate"], "22011");
    assert!(!frames.iter().any(|f| f.0 == b'2'));
    assert_eq!(actual["data_rows"], 0);
    assert_eq!(f.admin.snapshot().unwrap(), before);
    pg_send(&mut raw, b'E', &execute("bad_overlay_portal", 0)).await;
    let absent = temporal_state_frames(&pg_sync(&mut raw).await, "execute");
    assert_eq!(absent["sqlstate"], "34000");
    assert!(
        !temporal_state_cycle(&mut raw, "BEGIN")
            .await
            .iter()
            .any(|f| f.0 == b'E')
    );
    assert!(
        !temporal_state_cycle(
            &mut raw,
            "INSERT INTO app(id,body) VALUES(730001,'rollback')"
        )
        .await
        .iter()
        .any(|f| f.0 == b'E')
    );
    let bad = temporal_state_extended(&mut raw, "SELECT overlay('abc','X',0)").await;
    assert_eq!(bad["sqlstate"], "22011");
    assert_eq!(bad["stage"], "bind");
    assert_eq!(bad["ready"], "E");
    let aborted = temporal_state_extended(&mut raw, "SELECT overlay('abc','X',2)").await;
    assert_eq!(aborted["sqlstate"], "25P02");
    assert!(
        !temporal_state_cycle(&mut raw, "ROLLBACK")
            .await
            .iter()
            .any(|f| f.0 == b'E')
    );
    assert_eq!(f.admin.snapshot().unwrap(), before);
    pg_send(&mut raw, b'X', &[]).await;
    drop(raw);
    let mut task = prepared_network::raw_task(&f).await;
    for query in [
        "SELECT overlay('abc','X',2)",
        "SELECT overlay(body,'X',2) FROM app",
    ] {
        let actual = temporal_state_extended(&mut task, query).await;
        assert_eq!(actual["sqlstate"], "42501");
        assert_eq!(actual["stage"], "parse");
        assert_eq!(actual["data_rows"], 0);
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
    let mut revoked = raw_pg(&f).await;
    let denied = temporal_state_extended(&mut revoked, "SELECT overlay(body,'X',2) FROM app").await;
    assert_eq!(denied["sqlstate"], "42501");
    assert_eq!(denied["data_rows"], 0);
    pg_send(&mut revoked, b'X', &[]).await;
    drop(revoked);
    assert_eq!(f.admin.snapshot().unwrap(), before);
    f.stop().await;
    println!("OVERLAY authority, portal and transaction controls passed");
}
