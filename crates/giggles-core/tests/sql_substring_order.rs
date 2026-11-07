use giggles_core::{Admin, CoreError, Limits, relational::*};
fn session() -> (Admin, giggles_core::SqlSession) {
    let root = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("dd68")
        .join("dd68-substring-order-20261106")
        .join("fixtures");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join(format!(
        "corpus-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let admin = Admin::create(path, Limits::default()).unwrap();
    let session = admin.sql_session();
    (admin, session)
}
fn bytes(s: &str) -> Vec<u8> {
    let s = s.strip_prefix("\\x").unwrap();
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}
fn ty(oid: u64) -> DataType {
    match oid {
        17 => DataType::Bytes,
        21 => DataType::SmallInt,
        23 => DataType::Integer,
        25 => DataType::Text,
        _ => panic!("unexpectedOID{oid}"),
    }
}
#[test]
fn substring_order_pg18_raw326_ledger_numeric_eligible_comparison() {
    let (_a, mut s) = session();
    let cases: serde_json::Value = serde_json::from_slice(
        &std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/dd68/substring-curated326-v3.json"
        ))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(cases.as_array().unwrap().len(), 326);
    let mut checked = 0;
    for c in cases
        .as_array()
        .unwrap()
        .iter()
        .filter(|c| c["eligible"] == true)
    {
        checked += 1;
        let q = c["query"].as_str().unwrap();
        let ob = &c["observation"];
        let result = (|| {
            let p = s.prepare(q)?;
            let d = s.describe(&p)?;
            let types: Vec<_> = ob["parameterTypes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| ty(v.as_u64().unwrap()))
                .collect();
            assert_eq!(d.parameters, types, "{c}");
            let values: Vec<_> = c["params"]
                .as_array()
                .unwrap()
                .iter()
                .zip(types)
                .map(|(v, t)| {
                    if v.is_null() {
                        Value::Null
                    } else {
                        match t {
                            DataType::Integer | DataType::SmallInt => {
                                Value::Int(v.as_str().unwrap().parse().unwrap())
                            }
                            DataType::Bytes => Value::Bytes(bytes(v.as_str().unwrap())),
                            _ => Value::Text(v.as_str().unwrap().into()),
                        }
                    }
                })
                .collect();
            s.validate_immutable_constants(&p)?;
            s.execute_prepared(&p, &values, None)
        })();
        if ob["status"] == "error" {
            let e = match ob["sqlstate"].as_str().unwrap() {
                "22011" => SqlError::SubstringError,
                "42883" => SqlError::UndefinedFunction,
                "22P02" => SqlError::InvalidTextRepresentation,
                "42703" => SqlError::UnknownColumn,
                _ => unreachable!(),
            };
            assert_eq!(result.unwrap_err(), CoreError::Sql(e), "{c}");
        } else {
            let result = result.unwrap_or_else(|e| panic!("{c}: {e:?}"));
            let expected: Vec<_> = ob["rows"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| {
                    if r[0].is_null() {
                        Value::Null
                    } else if ob["resultTypes"][0]["oid"] == 17 {
                        Value::Bytes(bytes(r[0].as_str().unwrap()))
                    } else {
                        Value::Text(r[0].as_str().unwrap().into())
                    }
                })
                .collect();
            assert_eq!(
                result
                    .rows
                    .into_iter()
                    .map(|r| r.values[0].clone())
                    .collect::<Vec<_>>(),
                expected,
                "{c}"
            );
        }
    }
    assert_eq!(checked, 73);
}

#[test]
fn substring_order_relational_contexts_and_grammar_limits() {
    let (_a, mut s) = session();
    s.execute("CREATE TABLE f(id BIGINT PRIMARY KEY,txt TEXT)", &[], None)
        .unwrap();
    s.execute("INSERT INTO f VALUES(1,'abcdef')", &[], None)
        .unwrap();
    assert_eq!(
        s.execute(
            "SELECT min(SUBSTRING(txt FOR 2 FROM 3)),count(SUBSTRING(txt FOR 2 FROM 3)) FROM f",
            &[],
            None
        )
        .unwrap()
        .rows[0]
            .values,
        vec![Value::Text("cd".into()), Value::Int(1)]
    );
    for q in [
        "SELECT SUBSTRING('abc' FOR FROM 1)",
        "SELECT SUBSTRING('abc' FOR 1 FROM)",
        "SELECT SUBSTRING('abc' FOR 1 FROM",
        "SELECT SUBSTRING('abc' FOR 1 FROM,1)",
        "SELECT SUBSTRING('abc' FOR 1 FROM 1 FOR 1)",
        "SELECT pg_catalog.substring('abc' FOR 1 FROM 1)",
        "SELECT \"substring\"('abc' FOR 1 FROM 1)",
    ] {
        assert!(
            matches!(s.prepare(q), Err(CoreError::Sql(SqlError::Syntax))),
            "{q}"
        );
    }
    let q = "SELECT min(SUBSTRING('abc' FOR count(*) FROM 1)) FROM f";
    assert!(matches!(
        s.prepare(q).and_then(|p| s.describe(&p)),
        Err(CoreError::Sql(SqlError::GroupingError))
    ));
    for depth in [33, 64] {
        let q = format!(
            "SELECT {}'abc'{} FOR 1 FROM 1)",
            "SUBSTRING(".repeat(depth),
            " FOR 1 FROM 1)".repeat(depth - 1)
        );
        assert!(s.prepare(&q).is_err());
    }
}
#[test]
fn substring_order_numeric_proposal_metadata_and_outputs() {
    let root = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("dd68")
        .join("dd68-substring-order-20261106")
        .join("fixtures");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join(format!(
        "numeric-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let admin = Admin::create(path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    for (q, types, values, want) in [
        (
            "SELECT SUBSTRING($1 FOR $2::integer FROM $3)",
            vec![DataType::Text, DataType::Integer, DataType::Integer],
            vec![Value::Text("abcdef".into()), Value::Int(2), Value::Int(3)],
            Value::Text("cd".into()),
        ),
        (
            "SELECT SUBSTRING($1 FOR $2::smallint FROM $2::integer)",
            vec![DataType::Text, DataType::Integer],
            vec![Value::Text("abcdef".into()), Value::Int(2)],
            Value::Text("bc".into()),
        ),
        (
            "SELECT SUBSTRING($1 FOR $2::integer FROM $2::smallint)",
            vec![DataType::Text, DataType::SmallInt],
            vec![Value::Text("abcdef".into()), Value::Int(2)],
            Value::Text("bc".into()),
        ),
        (
            "SELECT SUBSTRING($1::bytea FOR $2 FROM $3)",
            vec![DataType::Bytes, DataType::Integer, DataType::Integer],
            vec![
                Value::Bytes(b"abcdef".to_vec()),
                Value::Int(2),
                Value::Int(3),
            ],
            Value::Bytes(b"cd".to_vec()),
        ),
    ] {
        let p = s.prepare(q).unwrap();
        assert_eq!(s.describe(&p).unwrap().parameters, types, "{q}");
        assert_eq!(
            s.execute(q, &values, None).unwrap().rows[0].values,
            vec![want],
            "{q}"
        );
    }
    for (q, error) in [
        (
            "SELECT SUBSTRING('abcdef' FOR 2::bigint FROM 3)",
            SqlError::UndefinedFunction,
        ),
        (
            "SELECT SUBSTRING($1 FOR $2 FROM $3), to_hex($2)",
            SqlError::UndefinedFunction,
        ),
        (
            "SELECT SUBSTRING('abc' FOR missing_length FROM missing_start)",
            SqlError::UnknownColumn,
        ),
        // PG resolves regex and succeeds; this candidate explicitly lacks that family.
        ("SELECT SUBSTRING($1 FOR $2 FROM $3)", SqlError::Unsupported),
        // PG42703 is retained in mixed.json; existing infer defers plain column lookup.
        (
            "SELECT SUBSTRING($1 FOR $2 FROM $3), missing",
            SqlError::Unsupported,
        ),
    ] {
        let p = s.prepare(q).unwrap();
        assert_eq!(s.describe(&p).unwrap_err(), CoreError::Sql(error), "{q}");
    }
}
