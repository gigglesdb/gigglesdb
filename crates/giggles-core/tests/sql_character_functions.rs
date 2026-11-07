use giggles_core::{Admin, CoreError, Limits, relational::*};

fn session() -> (Admin, giggles_core::SqlSession) {
    let root = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("dd68")
        .join("dd68-character-functions-20261106")
        .join("fixtures");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join(format!(
        "character-{}-{:?}-{}",
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

fn data_type(oid: u64) -> DataType {
    match oid {
        23 => DataType::Integer,
        25 => DataType::Text,
        _ => panic!("unexpected oracle OID {oid}"),
    }
}

#[test]
fn character_curated_pg18_utf8_c_oracle_values_metadata_and_errors() {
    let (_admin, mut session) = session();
    let corpus: serde_json::Value = serde_json::from_slice(
        &std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/dd68/character-curated-v3.json"
        ))
        .unwrap(),
    )
    .unwrap();
    for case in corpus["cases"].as_array().unwrap() {
        let query = case["query"].as_str().unwrap();
        let reference = &case["observation"];
        if case["known_gap_core"] == "Unsupported" {
            continue;
        }
        let prepared = session.prepare(query).unwrap();
        if reference["parameterTypes"].is_null() {
            let expected = match reference["sqlstate"].as_str().unwrap() {
                "42883" => SqlError::UndefinedFunction,
                "42703" => SqlError::UnknownColumn,
                state => panic!("unclassified parse SQLSTATE {state}: {case}"),
            };
            assert_eq!(
                session.describe(&prepared).unwrap_err(),
                CoreError::Sql(expected.clone()),
                "{case}"
            );
            continue;
        }
        let description = session.describe(&prepared).unwrap();
        let expected_parameters: Vec<_> = reference["parameterTypes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| data_type(n.as_u64().unwrap()))
            .collect();
        assert_eq!(description.parameters, expected_parameters, "{case}");
        let result_type = data_type(reference["resultTypes"][0]["oid"].as_u64().unwrap());
        assert_eq!(description.columns[0].data_type, result_type, "{case}");
        let params: Vec<_> = case["params"]
            .as_array()
            .unwrap()
            .iter()
            .zip(&expected_parameters)
            .map(|(value, ty)| {
                if value.is_null() {
                    Value::Null
                } else if *ty == DataType::Integer {
                    Value::Int(value.as_str().unwrap().parse().unwrap())
                } else {
                    Value::Text(value.as_str().unwrap().into())
                }
            })
            .collect();
        let result = match session.validate_immutable_constants(&prepared) {
            Ok(()) => session.execute_prepared(&prepared, &params, None),
            Err(error) => Err(error),
        };
        if reference["status"] == "error" {
            let expected = match reference["sqlstate"].as_str().unwrap() {
                "22023" => SqlError::InvalidParameterValue,
                "54000" => SqlError::Limit,
                other => panic!("unclassified oracle error {other}: {case}"),
            };
            assert_eq!(result.unwrap_err(), CoreError::Sql(expected), "{case}");
        } else {
            let expected: Vec<_> = reference["rows"]
                .as_array()
                .unwrap()
                .iter()
                .map(|row| {
                    let value = &row[0];
                    if value.is_null() {
                        Value::Null
                    } else if result_type == DataType::Integer {
                        Value::Int(value.as_str().unwrap().parse().unwrap())
                    } else {
                        Value::Text(value.as_str().unwrap().into())
                    }
                })
                .collect();
            let actual: Vec<_> = result
                .unwrap()
                .rows
                .into_iter()
                .map(|row| row.values[0].clone())
                .collect();
            assert_eq!(actual, expected, "{case}");
        }
    }
}

#[test]
fn character_known_error_contract_gaps_are_explicit() {
    let (_admin, mut session) = session();
    for query in [
        "SELECT \"ASCII\"('a')",
        "SELECT \"INITCAP\"('a')",
        "SELECT \"CHR\"(65)",
    ] {
        let p = session.prepare(query).unwrap();
        assert_eq!(
            session.describe(&p).unwrap_err(),
            CoreError::Sql(SqlError::UndefinedFunction),
            "PG42883: {query}"
        );
    }
    // Core public calls match PostgreSQL 42883; server authorization failures
    // use 42501 under E939 to avoid revealing routine existence.
    for query in [
        "SELECT public.ascii('a')",
        "SELECT public.initcap('a')",
        "SELECT public.chr(65)",
    ] {
        let prepared = session.prepare(query).unwrap();
        assert_eq!(
            session.describe(&prepared).unwrap_err(),
            CoreError::Sql(SqlError::UndefinedFunction)
        );
        assert_eq!(
            session.execute_prepared(&prepared, &[], None).unwrap_err(),
            CoreError::Sql(SqlError::UndefinedFunction)
        );
    }
    for query in [
        "SELECT chr($1,1)",
        "SELECT ascii($1,1)",
        "SELECT initcap($1,1)",
    ] {
        let p = session.prepare(query).unwrap();
        assert_eq!(
            session.describe(&p).unwrap_err(),
            CoreError::Sql(SqlError::Parameters),
            "PG42883 versus current unknown parameter model: {query}"
        );
    }
    let p = session.prepare("SELECT chr($1::integer,1)").unwrap();
    assert_eq!(
        session.describe(&p).unwrap_err(),
        CoreError::Sql(SqlError::UndefinedFunction)
    );
}

#[test]
fn character_constants_validate_without_evaluating_parameters() {
    let (_admin, mut session) = session();
    for (argument, expected) in [
        ("-1", SqlError::InvalidParameterValue),
        ("0", SqlError::Limit),
        ("55296", SqlError::Limit),
        ("1114112", SqlError::Limit),
    ] {
        for suffix in ["", " WHERE FALSE", " LIMIT 0"] {
            let query = format!("SELECT chr({argument}){suffix}");
            let prepared = session.prepare(&query).unwrap();
            assert_eq!(
                session.validate_immutable_constants(&prepared).unwrap_err(),
                CoreError::Sql(expected.clone()),
                "{query}"
            );
        }
    }
    for query in [
        "SELECT chr($1)",
        "SELECT chr($1) WHERE FALSE",
        "SELECT chr($1) LIMIT 0",
    ] {
        let prepared = session.prepare(query).unwrap();
        assert_eq!(
            session.describe(&prepared).unwrap().parameters,
            vec![DataType::Integer]
        );
        session.validate_immutable_constants(&prepared).unwrap();
    }
}

#[test]
fn character_functions_work_in_rows_aggregates_filters_and_repeated_parameters() {
    let (_admin, mut session) = session();
    session
        .execute(
            "CREATE TABLE chars(id bigint primary key, label text, code integer)",
            &[],
            None,
        )
        .unwrap();
    session
        .execute("INSERT INTO chars VALUES(1,'éHELLO',128512)", &[], None)
        .unwrap();
    session
        .execute("INSERT INTO chars VALUES(2,NULL,NULL)", &[], None)
        .unwrap();
    let result = session
        .execute(
            "SELECT ascii(label),initcap(label),chr(code) FROM chars ORDER BY id",
            &[],
            None,
        )
        .unwrap();
    assert_eq!(
        result.rows[0].values,
        vec![
            Value::Int(233),
            Value::Text("éHello".into()),
            Value::Text("😀".into())
        ]
    );
    assert_eq!(result.rows[1].values, vec![Value::Null; 3]);
    assert_eq!(
        session
            .execute(
                "SELECT sum(ascii(label)),min(initcap(label)),max(chr(code)) FROM chars",
                &[],
                None
            )
            .unwrap()
            .rows[0]
            .values,
        vec![
            Value::Int(233),
            Value::Text("éHello".into()),
            Value::Text("😀".into())
        ]
    );
    let rows = session
        .execute(
            "SELECT initcap(label) FROM chars WHERE ascii(label)=233 ORDER BY initcap(label)",
            &[],
            None,
        )
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 1);
    let prepared = session
        .prepare("SELECT ascii(initcap($1)),initcap($1),chr($2)")
        .unwrap();
    assert_eq!(
        session.describe(&prepared).unwrap().parameters,
        vec![DataType::Text, DataType::Integer]
    );
    for (text, code, expected) in [
        (
            Value::Text("abc".into()),
            Value::Int(65),
            vec![
                Value::Int(65),
                Value::Text("Abc".into()),
                Value::Text("A".into()),
            ],
        ),
        (Value::Null, Value::Null, vec![Value::Null; 3]),
    ] {
        assert_eq!(
            session
                .execute_prepared(&prepared, &[text, code], None)
                .unwrap()
                .rows[0]
                .values,
            expected
        );
    }
}
