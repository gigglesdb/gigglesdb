use giggles_core::{Admin, CoreError, Limits, SqlSession, relational::*};

fn session() -> (Admin, SqlSession) {
    let root = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("dd68")
        .join("dd68-integer-formats-20261106")
        .join("fixtures");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join(format!(
        "formats-{}-{:?}-{}",
        std::process::id(),
        std::thread::current().id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let admin = Admin::create(path, Limits::default()).unwrap();
    let session = admin.sql_session();
    (admin, session)
}

fn corpus() -> serde_json::Value {
    serde_json::from_slice(
        &std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/dd68/integer-formats-curated.json"
        ))
        .unwrap(),
    )
    .unwrap()
}

fn ty(oid: u64) -> DataType {
    match oid {
        20 => DataType::BigInt,
        23 => DataType::Integer,
        25 => DataType::Text,
        _ => panic!("unexpected OID {oid}"),
    }
}

#[test]
fn integer_formats_match_pg18_overloads_width_values_and_metadata() {
    let (_admin, mut session) = session();
    let corpus = corpus();
    let mut compared = 0;
    for case in corpus["cases"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|case| case["eligible"] == true)
    {
        let query = case["query"].as_str().unwrap();
        let reference = &case["observation"];
        let prepared = session.prepare(query).unwrap();
        if reference["parameterTypes"].is_null() {
            let expected = match reference["sqlstate"].as_str().unwrap() {
                "42725" => SqlError::AmbiguousFunction,
                "42883" => SqlError::UndefinedFunction,
                "42703" => SqlError::UnknownColumn,
                state => panic!("unclassified {state}: {case}"),
            };
            assert_eq!(
                session.describe(&prepared).unwrap_err(),
                CoreError::Sql(expected),
                "{case}"
            );
        } else {
            let description = session.describe(&prepared).unwrap();
            let parameters: Vec<_> = reference["parameterTypes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|n| ty(n.as_u64().unwrap()))
                .collect();
            assert_eq!(description.parameters, parameters, "{case}");
            let result_types: Vec<_> = reference["resultTypes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|c| ty(c["oid"].as_u64().unwrap()))
                .collect();
            assert_eq!(
                description
                    .columns
                    .iter()
                    .map(|c| c.data_type.clone())
                    .collect::<Vec<_>>(),
                result_types,
                "{case}"
            );
            let params: Vec<_> = case["params"]
                .as_array()
                .unwrap()
                .iter()
                .map(|p| {
                    if p.is_null() {
                        Value::Null
                    } else {
                        Value::Int(p.as_str().unwrap().parse().unwrap())
                    }
                })
                .collect();
            session.validate_immutable_constants(&prepared).unwrap();
            let actual: Vec<_> = session
                .execute_prepared(&prepared, &params, None)
                .unwrap()
                .rows
                .into_iter()
                .map(|r| r.values)
                .collect();
            let expected: Vec<Vec<_>> = reference["rows"]
                .as_array()
                .unwrap()
                .iter()
                .map(|row| {
                    row.as_array()
                        .unwrap()
                        .iter()
                        .zip(&result_types)
                        .map(|(v, t)| {
                            if v.is_null() {
                                Value::Null
                            } else if *t == DataType::Text {
                                Value::Text(v.as_str().unwrap().into())
                            } else {
                                Value::Int(v.as_str().unwrap().parse().unwrap())
                            }
                        })
                        .collect()
                })
                .collect();
            assert_eq!(actual, expected, "{case}");
        }
        compared += 1;
    }
    assert_eq!(compared, corpus["eligible"].as_u64().unwrap() as usize);
}

#[test]
fn integer_format_existing_error_profile_gaps_remain_visible() {
    let (_admin, mut session) = session();
    let corpus = corpus();
    let mut gaps = 0;
    for case in corpus["cases"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|case| case["eligible"] == false)
    {
        let expected = if matches!(
            case["query"].as_str().unwrap(),
            "SELECT to_bin('bad'::integer)"
                | "SELECT to_hex('bad'::integer)"
                | "SELECT to_oct('bad'::integer)"
        ) {
            assert_eq!(case["observation"]["sqlstate"], "22P02");
            SqlError::InvalidTextRepresentation
        } else {
            match case["known_gap_core"].as_str().unwrap() {
                "Unsupported" => SqlError::Unsupported,
                "Parameters" => SqlError::Parameters,
                "Type" => SqlError::Type,
                _ => panic!("{case}"),
            }
        };
        let result = session
            .prepare(case["query"].as_str().unwrap())
            .and_then(|p| session.describe(&p));
        assert_eq!(
            result.unwrap_err(),
            CoreError::Sql(expected),
            "known PG mismatch, not parity credit: {case}"
        );
        gaps += 1;
    }
    assert_eq!(gaps, corpus["known_gaps"].as_u64().unwrap() as usize);
}

#[test]
fn integer_format_row_aggregate_filter_and_parameter_width_contexts() {
    let (_admin, mut session) = session();
    session
        .execute(
            "CREATE TABLE fmt(id bigint primary key, i integer, b bigint)",
            &[],
            None,
        )
        .unwrap();
    session
        .execute("INSERT INTO fmt VALUES(1,-1,-1)", &[], None)
        .unwrap();
    session
        .execute("INSERT INTO fmt VALUES(2,NULL,NULL)", &[], None)
        .unwrap();
    let result = session
        .execute(
            "SELECT to_hex(i),to_hex(b),to_bin(i),to_oct(b) FROM fmt ORDER BY id",
            &[],
            None,
        )
        .unwrap();
    assert_eq!(
        result.rows[0].values,
        vec![
            Value::Text("ffffffff".into()),
            Value::Text("ffffffffffffffff".into()),
            Value::Text("11111111111111111111111111111111".into()),
            Value::Text("1777777777777777777777".into())
        ]
    );
    assert_eq!(result.rows[1].values, vec![Value::Null; 4]);
    assert_eq!(
        session
            .execute(
                "SELECT min(to_hex(i)),max(to_hex(b)),count(to_bin(i)) FROM fmt",
                &[],
                None
            )
            .unwrap()
            .rows[0]
            .values,
        vec![
            Value::Text("ffffffff".into()),
            Value::Text("ffffffffffffffff".into()),
            Value::Int(1)
        ]
    );
    assert_eq!(session.execute("SELECT to_hex(i) FROM fmt WHERE to_bin(i)='11111111111111111111111111111111' ORDER BY to_oct(b)", &[], None).unwrap().rows.len(),1);
    let p = session
        .prepare("SELECT to_hex($1::integer),to_hex($1::bigint),to_oct($2::bigint)")
        .unwrap();
    assert_eq!(
        session.describe(&p).unwrap().parameters,
        vec![DataType::Integer, DataType::BigInt]
    );
    session.validate_immutable_constants(&p).unwrap();
    assert_eq!(
        session
            .execute_prepared(&p, &[Value::Int(-1), Value::Int(-1)], None)
            .unwrap()
            .rows[0]
            .values,
        vec![
            Value::Text("ffffffff".into()),
            Value::Text("ffffffffffffffff".into()),
            Value::Text("1777777777777777777777".into())
        ]
    );
    assert_eq!(
        session
            .execute_prepared(&p, &[Value::Null, Value::Null], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Null; 3]
    );
}
