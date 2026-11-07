use giggles_core::{Admin, CoreError, Limits, relational::*};

fn session() -> (Admin, giggles_core::SqlSession) {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let root = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("dd68")
        .join("dd68-position-functions-20261106")
        .join("fixtures");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join(format!(
        "position-{}-{:?}-{}-{}",
        std::process::id(),
        std::thread::current().id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let admin = Admin::create(path, Limits::default()).unwrap();
    let session = admin.sql_session();
    (admin, session)
}

fn bytes(text: &str) -> Vec<u8> {
    text.strip_prefix("\\x")
        .unwrap()
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|b| u8::from_str_radix(std::str::from_utf8(b).unwrap(), 16).unwrap())
        .collect()
}

#[test]
fn position_curated_pg18_core_results_metadata_and_exact_error_contracts() {
    let (_admin, mut session) = session();
    let corpus: serde_json::Value = serde_json::from_slice(
        &std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/dd68/position-curated-v2.json"
        ))
        .unwrap(),
    )
    .unwrap();
    let (mut strict_matches, mut profile_gaps, mut excluded) = (0, 0, 0);
    for case in corpus["cases"].as_array().unwrap() {
        let id = case["id"].as_u64().unwrap();
        if !case["oids"].as_array().unwrap().is_empty()
            || case["setup"].is_array()
            || id == 81
            || id == 82
        {
            assert!((73..=86).contains(&id), "unapproved exclusion case {id}");
            excluded += 1;
            continue;
        }
        let query = case["query"].as_str().unwrap();
        let observation = (|| {
            let prepared = session.prepare(query)?;
            let description = session.describe(&prepared)?;
            let params: Vec<_> = case["params"]
                .as_array()
                .unwrap()
                .iter()
                .zip(&description.parameters)
                .map(|(v, t)| {
                    if v.is_null() {
                        Value::Null
                    } else {
                        match t {
                            DataType::Bytes => Value::Bytes(bytes(v.as_str().unwrap())),
                            DataType::Text => Value::Text(v.as_str().unwrap().into()),
                            _ => panic!("unexpected parameter {t:?}"),
                        }
                    }
                })
                .collect();
            session.validate_immutable_constants(&prepared)?;
            let result = session.execute_prepared(&prepared, &params, None)?;
            Ok::<_, CoreError>((description, result))
        })();
        let reference = &case["observations"][0]["observation"];
        // These inherited profile gaps are not strict PostgreSQL matches.
        // Each override pins the exact SQL and actual PG outcome before Core.
        let gap = match id {
            51 => Some((
                r#"SELECT "POSITION"('abc','b')"#,
                "error",
                Some("42883"),
                "parse",
                SqlError::UndefinedFunction,
            )),
            // Core matches PG 42883; server authorization failure is 42501 (E939).
            52 => Some((
                "SELECT public.position('abc','b')",
                "error",
                Some("42883"),
                "parse",
                SqlError::UndefinedFunction,
            )),
            65 => Some((
                "SELECT strpos('abc'::bytea,'b'::bytea)",
                "error",
                Some("42883"),
                "parse",
                SqlError::Type,
            )),
            71 => Some((
                "SELECT max(position('b' IN 'abc'))",
                "ok",
                None,
                "execute",
                SqlError::Unsupported,
            )),
            72 => Some((
                "SELECT position(max('b') IN 'abc')",
                "ok",
                None,
                "execute",
                SqlError::Unsupported,
            )),
            89 => Some((
                "SELECT max(position(count(*) IN 'abc'))",
                "error",
                Some("42883"),
                "parse",
                SqlError::GroupingError,
            )),
            _ => None,
        };
        let expected_error = if let Some((sql, status, state, phase, error)) = gap {
            profile_gaps += 1;
            assert_eq!(query, sql, "profile-gap SQL drift case {id}");
            assert_eq!(
                reference["status"], status,
                "profile-gap status drift case {id}"
            );
            assert_eq!(
                reference["sqlstate"].as_str(),
                state,
                "profile-gap SQLSTATE drift case {id}"
            );
            assert_eq!(
                reference["phase"], phase,
                "profile-gap phase drift case {id}"
            );
            if status == "ok" {
                assert_eq!(
                    reference["rows"],
                    serde_json::json!([["2"]]),
                    "profile-gap result drift case {id}"
                );
                assert_eq!(reference["resultTypes"][0]["oid"], 23);
            }
            Some(error)
        } else {
            strict_matches += 1;
            if reference["status"] == "error" {
                Some(match reference["sqlstate"].as_str().unwrap() {
                    "42601" => SqlError::Syntax,
                    "42883" => SqlError::UndefinedFunction,
                    "54000" => SqlError::Limit,
                    "22023" => {
                        assert_eq!(id, 68, "only reviewed literal bytea case68");
                        assert_eq!(query, r"SELECT position('\xgg' IN NULL::bytea)");
                        assert_eq!(reference["phase"], "parse");
                        SqlError::InvalidParameterValue
                    }
                    state => panic!("unclassified reference SQLSTATE {state}: {case}"),
                })
            } else {
                None
            }
        };
        if let Some(expected) = expected_error {
            assert_eq!(
                observation.unwrap_err(),
                CoreError::Sql(expected),
                "case {id}: {query}"
            );
        } else {
            let (description, result) = observation.unwrap();
            let expected_types: Vec<_> = reference["parameterTypes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| match v.as_u64().unwrap() {
                    17 => DataType::Bytes,
                    25 => DataType::Text,
                    oid => panic!("unexpected OID {oid}"),
                })
                .collect();
            assert_eq!(description.parameters, expected_types, "case {id}");
            assert_eq!(reference["resultTypes"][0]["oid"], 23, "case {id}");
            assert_eq!(
                description.columns[0].data_type,
                DataType::Integer,
                "case {id}"
            );
            assert_eq!(
                description.columns[0].name,
                reference["resultTypes"][0]["name"].as_str().unwrap(),
                "case {id}"
            );
            let expected: Vec<_> = reference["rows"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| {
                    if r[0].is_null() {
                        Value::Null
                    } else {
                        Value::Int(r[0].as_str().unwrap().parse().unwrap())
                    }
                })
                .collect();
            let actual: Vec<_> = result
                .rows
                .into_iter()
                .map(|r| r.values[0].clone())
                .collect();
            assert_eq!(actual, expected, "case {id}: {query}");
        }
    }
    assert_eq!((strict_matches, profile_gaps, excluded), (72, 6, 14));
}

#[test]
fn position_columns_nested_insert_and_aggregate_authority_controls() {
    let (_admin, mut session) = session();
    session
        .execute(
            "CREATE TABLE position_cols(id BIGINT PRIMARY KEY,txt TEXT,b BYTEA)",
            &[],
            None,
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO position_cols VALUES(1,'aé😀z',decode('1000ff20','hex'))",
            &[],
            None,
        )
        .unwrap();
    for (q, expected) in [
        ("SELECT position('😀' IN txt) FROM position_cols", 3),
        (
            "SELECT position(decode('00ff','hex') IN b) FROM position_cols",
            2,
        ),
        ("SELECT position(txt IN 'aé😀z') FROM position_cols", 1),
        (
            "SELECT position('1' IN position('😀' IN txt)::text) FROM position_cols",
            0,
        ),
    ] {
        let p = session.prepare(q).unwrap();
        let description = session.describe(&p).unwrap();
        assert_eq!(description.columns[0].data_type, DataType::Integer);
        assert_eq!(
            session.execute_prepared(&p, &[], None).unwrap().rows[0].values,
            vec![Value::Int(expected)],
            "{q}"
        );
    }
    let p = session
        .prepare("SELECT position('a' IN missing) FROM position_cols")
        .unwrap();
    assert_eq!(
        session.describe(&p).unwrap_err(),
        CoreError::Sql(SqlError::UnknownColumn)
    );
    session
        .execute(
            "CREATE TABLE position_insert(id BIGINT PRIMARY KEY,n INTEGER)",
            &[],
            None,
        )
        .unwrap();
    session
        .execute(
            "INSERT INTO position_insert VALUES(1,position('😀' IN 'aé😀z'))",
            &[],
            None,
        )
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT n FROM position_insert WHERE id=1", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Int(3)]
    );
    let (_aggregate_admin, mut session) = self::session();
    for q in [
        "SELECT max(position('b' IN 'abc'))",
        "SELECT position(max('b') IN 'abc')",
    ] {
        let p = session.prepare(q).unwrap();
        if q.starts_with("SELECT max(") {
            session.describe(&p).unwrap();
            assert_eq!(
                session.execute_prepared(&p, &[], None).unwrap_err(),
                CoreError::Sql(SqlError::Unsupported)
            );
        } else {
            assert_eq!(
                session.describe(&p).unwrap_err(),
                CoreError::Sql(SqlError::Unsupported)
            );
        }
    }
}

#[test]
fn position_parser_local_syntax_depth_and_insert_literal_traversal() {
    let (_admin, mut session) = session();
    for q in [
        "SELECT position(*)",
        "SELECT max(position(*))",
        "SELECT position()",
        "SELECT position('x' IN)",
        "SELECT position('x','xyz')",
        "SELECT position('x' IN 'xyz','z')",
    ] {
        assert_eq!(
            session.prepare(q).unwrap_err(),
            CoreError::Sql(SqlError::Syntax),
            "{q}"
        );
    }
    let mut expression = "'x'".to_string();
    for _ in 0..34 {
        expression = format!("position('x' IN {expression})::text");
    }
    assert_eq!(
        session
            .prepare(&format!("SELECT {expression}"))
            .unwrap_err(),
        CoreError::Sql(SqlError::Limit)
    );
    session
        .execute(
            "CREATE TABLE position_shape(id BIGINT PRIMARY KEY,n INTEGER)",
            &[],
            None,
        )
        .unwrap();
    let huge = format!("'{}'", "x".repeat(65_537));
    let leaf = format!("concat({})", vec!["'x'"; 256].join(","));
    let wide = format!("concat({})", vec![leaf.as_str(); 20].join(","));
    for value in [huge, wide] {
        for q in [
            format!("INSERT INTO position_shape VALUES(1,position({value} IN 'x'))"),
            format!("INSERT INTO position_shape VALUES(1,position('x' IN {value}))"),
        ] {
            assert_eq!(
                session.prepare(&q).unwrap_err(),
                CoreError::Sql(SqlError::Limit)
            );
        }
    }
}

#[test]
fn position_pg18_binary_results_and_core_coverage_exclusions_are_explicit() {
    let corpus: serde_json::Value = serde_json::from_slice(
        &std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/dd68/position-curated-v2.json"
        ))
        .unwrap(),
    )
    .unwrap();
    let mut rows = 0;
    for case in corpus["cases"].as_array().unwrap() {
        let text = &case["observations"][0]["observation"];
        let binary = &case["observations"][1]["observation"];
        assert_eq!(text["status"], binary["status"], "case {}", case["id"]);
        if text["status"] == "ok" {
            assert_eq!(text["resultTypes"][0]["oid"], 23);
            assert_eq!(binary["resultTypes"][0]["oid"], 23);
            let v = &text["rows"][0][0];
            let b = &binary["rows"][0][0];
            if v.is_null() {
                assert!(b.is_null());
            } else {
                let v: i32 = v.as_str().unwrap().parse().unwrap();
                assert_eq!(
                    b["hex"].as_str().unwrap(),
                    v.to_be_bytes()
                        .iter()
                        .map(|v| format!("{v:02x}"))
                        .collect::<String>()
                );
            }
            rows += 1;
        } else {
            assert_eq!(text["sqlstate"], binary["sqlstate"]);
            assert_eq!(text["phase"], binary["phase"]);
            assert_ne!(
                text["sqlstate"], "34000",
                "failed harness attempt must never qualify"
            );
        }
    }
    assert_eq!(corpus["cases"].as_array().unwrap().len(), 92);
    assert_eq!(rows, 60);
    // Declared OIDs, raw binary inputs, malformed bytea Bind and invalid UTF8
    // are PostgreSQL wire evidence only; this Core test supplies admitted Values.
}

#[test]
fn position_pg18_source_first_error_and_strict_null_precedence() {
    let (_admin, mut session) = session();
    let cases: serde_json::Value = serde_json::from_slice(
        &std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/dd68/position-order-supplement.json"
        ))
        .unwrap(),
    )
    .unwrap();
    for case in cases.as_array().unwrap() {
        let query = case["query"].as_str().unwrap();
        let expected = match case["observation"]["sqlstate"].as_str().unwrap() {
            "22003" => SqlError::Overflow,
            "22P02" => SqlError::InvalidTextRepresentation,
            state => panic!("unexpected SQLSTATE {state}"),
        };
        let result = (|| {
            let p = session.prepare(query)?;
            session.describe(&p)?;
            session.validate_immutable_constants(&p)?;
            let params: Vec<_> = case["params"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| {
                    if v.is_null() {
                        Value::Null
                    } else {
                        Value::Text(v.as_str().unwrap().into())
                    }
                })
                .collect();
            session.execute_prepared(&p, &params, None)
        })();
        assert_eq!(result.unwrap_err(), CoreError::Sql(expected), "{query}");
    }
    for q in [
        "SELECT position('x' IN 'xyz')",
        "SELECT position($1 IN $2)",
        "SELECT pg_catalog.position('xyz','x')",
    ] {
        assert_eq!(
            session.prepare(q).unwrap().table(),
            None,
            "pure explicit-input call: {q}"
        );
    }
    assert!(
        session
            .prepare("SELECT max(position('x' IN 'xyz'))")
            .unwrap()
            .table()
            .is_some(),
        "aggregate authority unchanged"
    );
    assert!(
        session
            .prepare("SELECT position('x' IN txt) FROM position_cols")
            .unwrap()
            .table()
            .is_some(),
        "column dependency retained"
    );
}
