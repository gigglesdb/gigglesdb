use giggles_core::{Admin, CoreError, Limits, SqlSession, relational::*};

#[test]
fn trim_escaping_quoted_nul_and_cast_depth_preserve_existing_lexical_contract() {
    let (_admin, mut session) = session();
    assert_eq!(
        session
            .execute("SELECT TRIM('''' FROM '''abc''')", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Text("abc".into())]
    );
    for query in [
        "SELECT \"tr\"\"im\"(' x ')",
        "SELECT pg_catalog.\"tr\"\"im\"(' x ')",
    ] {
        let p = session.prepare(query).unwrap();
        assert_eq!(
            session.describe(&p).unwrap_err(),
            CoreError::Sql(SqlError::UndefinedFunction),
            "quoted routine must not become construct: {query}"
        );
    }
    for query in ["SELECT TRIM(\"a\0b\")", "SELECT \"tri\0m\"('x')"] {
        assert!(
            matches!(
                session.prepare(query),
                Err(CoreError::Sql(SqlError::Syntax))
            ),
            "quoted identifier NUL must retain lexical rejection"
        );
    }
    for count in [30, 31, 32] {
        let operand = format!("' x '{}", "::text".repeat(count));
        assert_eq!(
            session.execute(&format!("SELECT TRIM({operand})"), &[], None),
            session.execute(&format!("SELECT btrim({operand})"), &[], None)
        );
    }
}

fn session() -> (Admin, SqlSession) {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let root = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("dd68")
        .join("dd68-trim-syntax-20261106")
        .join("fixtures");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join(format!(
        "trim-{}-{:?}-{}-{}",
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
fn corpus() -> serde_json::Value {
    serde_json::from_slice(
        &std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/dd68/trim-curated.json"
        ))
        .unwrap(),
    )
    .unwrap()
}
fn ty(oid: u64) -> DataType {
    match oid {
        17 => DataType::Bytes,
        25 => DataType::Text,
        _ => panic!("unexpected OID {oid}"),
    }
}
fn expected(value: &serde_json::Value, ty: &DataType) -> Value {
    if value.is_null() {
        return Value::Null;
    }
    let text = value.as_str().unwrap();
    if *ty == DataType::Text {
        Value::Text(text.into())
    } else {
        let hex = text.strip_prefix("\\x").unwrap();
        Value::Bytes(
            (0..hex.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
                .collect(),
        )
    }
}

#[test]
fn standard_trim_default_direction_and_argument_order() {
    let (_admin, mut session) = session();
    for (query, want) in [
        ("SELECT TRIM('  abc  ')", "abc"),
        ("SELECT TRIM(FROM '  abc  ')", "abc"),
        ("SELECT TRIM(BOTH 'x' FROM 'xxabxx')", "ab"),
        ("SELECT TRIM(LEADING 'x' FROM 'xxabxx')", "abxx"),
        ("SELECT TRIM(TRAILING 'x' FROM 'xxabxx')", "xxab"),
        ("SELECT TRIM('xxabxx','x')", "ab"),
    ] {
        assert_eq!(
            session.execute(query, &[], None).unwrap().rows[0].values,
            vec![Value::Text(want.into())],
            "{query}"
        );
    }
}

#[test]
fn trim_pg18_curated_syntax_unicode_bytea_rows_and_metadata() {
    let (_admin, mut session) = session();
    session
        .execute(
            "CREATE TABLE trim_input(id integer primary key,label text)",
            &[],
            None,
        )
        .unwrap();
    session
        .execute("INSERT INTO trim_input VALUES(1,'xxAbxx')", &[], None)
        .unwrap();
    session
        .execute("INSERT INTO trim_input VALUES(2,NULL)", &[], None)
        .unwrap();
    let data = corpus();
    let mut compared = 0;
    for case in data["cases"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|c| c["eligible"] == true)
    {
        let query = case["query"].as_str().unwrap();
        let reference = &case["observation"];
        if reference["parameterTypes"].is_null() {
            let result = session.prepare(query).and_then(|p| session.describe(&p));
            let error = match reference["sqlstate"].as_str().unwrap() {
                "42601" => SqlError::Syntax,
                "42703" => SqlError::UnknownColumn,
                "42883" => SqlError::UndefinedFunction,
                state => panic!("unclassified {state}: {case}"),
            };
            assert_eq!(result.unwrap_err(), CoreError::Sql(error), "{case}");
        } else {
            let prepared = session.prepare(query).unwrap();
            let description = session.describe(&prepared).unwrap();
            let parameter_types: Vec<_> = reference["parameterTypes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| ty(v.as_u64().unwrap()))
                .collect();
            assert_eq!(description.parameters, parameter_types, "{case}");
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
            assert_eq!(
                description
                    .columns
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>(),
                reference["resultTypes"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|c| c["name"].as_str().unwrap())
                    .collect::<Vec<_>>(),
                "{case}"
            );
            let params: Vec<_> = case["params"]
                .as_array()
                .unwrap()
                .iter()
                .zip(&parameter_types)
                .map(|(v, t)| expected(v, t))
                .collect();
            session.validate_immutable_constants(&prepared).unwrap();
            let actual: Vec<_> = session
                .execute_prepared(&prepared, &params, None)
                .unwrap()
                .rows
                .into_iter()
                .map(|r| r.values)
                .collect();
            let want: Vec<Vec<_>> = reference["rows"]
                .as_array()
                .unwrap()
                .iter()
                .map(|row| {
                    row.as_array()
                        .unwrap()
                        .iter()
                        .zip(&result_types)
                        .map(|(v, t)| expected(v, t))
                        .collect()
                })
                .collect();
            assert_eq!(actual, want, "{case}");
        }
        compared += 1;
    }
    assert_eq!(compared, data["eligible"].as_u64().unwrap() as usize);
}

#[test]
fn trim_existing_callee_and_namespace_error_gaps_are_explicit() {
    let (_admin, mut session) = session();
    let data = corpus();
    let mut gaps = 0;
    for case in data["cases"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|c| c["eligible"] == false)
    {
        let query = case["query"].as_str().unwrap();
        let result = session.prepare(query).and_then(|p| session.describe(&p));
        let want = match case["known_gap_core"].as_str().unwrap() {
            // F10: PG18 reports every curated wrong-type and wrong-arity
            // TRIM callee as 42883; core now resolves the same way.
            "Type" if case["observation"]["sqlstate"] == "42883" => SqlError::UndefinedFunction,
            "Type" => SqlError::Type,
            "Unsupported" => SqlError::Unsupported,
            _ => panic!("{case}"),
        };
        assert_eq!(
            result.unwrap_err(),
            CoreError::Sql(want),
            "PG error or retained gap mismatch: {case}"
        );
        gaps += 1;
    }
    assert_eq!(gaps, data["known_gaps"].as_u64().unwrap() as usize);
}

#[test]
fn trim_depth_argument_limits_and_aggregate_context_match_normalized_calls() {
    let (_admin, mut session) = session();
    for depth in [31, 32, 33] {
        let mut trim = "' x '".to_string();
        let mut normalized = trim.clone();
        for _ in 0..depth {
            trim = format!("TRIM({trim})");
            normalized = format!("btrim({normalized})");
        }
        assert_eq!(
            session.execute(&format!("SELECT {trim}"), &[], None),
            session.execute(&format!("SELECT {normalized}"), &[], None)
        );
    }
    for count in [63, 64] {
        let sources = vec!["'a'"; count].join(",");
        let normalized = format!("SELECT btrim({sources},'x')");
        let trim = format!("SELECT TRIM('x' FROM {sources})");
        assert_eq!(
            session.execute(&trim, &[], None),
            session.execute(&normalized, &[], None)
        );
    }
    session
        .execute(
            "CREATE TABLE trim_agg(id bigint primary key,label text)",
            &[],
            None,
        )
        .unwrap();
    for query in [
        "SELECT min(TRIM(BOTH 'x' FROM count(*))) FROM trim_agg",
        "SELECT min(TRIM(LEADING 'x' FROM pg_catalog.count(*))) FROM trim_agg",
    ] {
        let p = session.prepare(query).unwrap();
        assert_eq!(
            session.describe(&p).unwrap_err(),
            CoreError::Sql(SqlError::GroupingError),
            "{query}"
        );
    }
}
