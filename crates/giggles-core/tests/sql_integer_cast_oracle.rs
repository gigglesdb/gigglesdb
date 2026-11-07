use giggles_core::{Admin, CoreError, Limits, relational::*};

fn state(e: CoreError) -> String {
    match e {
        CoreError::Sql(SqlError::InvalidTextRepresentation) => "22P02",
        CoreError::Sql(SqlError::Overflow) => "22003",
        CoreError::Sql(SqlError::Unsupported) => "0A000",
        CoreError::Sql(SqlError::Type | SqlError::Parameters) => "42804",
        CoreError::Sql(SqlError::AmbiguousFunction) => "42725",
        CoreError::Sql(SqlError::UndefinedFunction) => "42883",
        CoreError::Sql(SqlError::Limit) => "54000",
        _ => return format!("{e:?}"),
    }
    .into()
}
fn oid(t: &DataType) -> u32 {
    match t {
        DataType::SmallInt => 21,
        DataType::Integer => 23,
        DataType::BigInt => 20,
        DataType::Text => 25,
        DataType::Real => 700,
        DataType::DoublePrecision => 701,
        DataType::Numeric => 1700,
        _ => 0,
    }
}

#[test]
fn pg18_integer_cast_421_actual_core_cases() {
    let cases: serde_json::Value = serde_json::from_slice(
        &std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/dd68/integer-cast-oracle.json"
        ))
        .unwrap(),
    )
    .unwrap();
    let root = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("dd68")
        .join("dd68-integer-cast-design-20261106");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join(format!("cast-oracle-{}", std::process::id()));
    let admin = Admin::create(path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    let mut report = Vec::new();
    let mut unexpected = 0;
    let mut known_gaps = 0;
    for c in cases.as_array().unwrap() {
        let mut stage = "prepare";
        let mut metadata = serde_json::Value::Null;
        let actual = (|| -> Result<Vec<Vec<String>>, String> {
            let p = session
                .prepare(c["query"].as_str().unwrap())
                .map_err(state)?;
            stage = "describe";
            let d = session.describe(&p).map_err(state)?;
            metadata = serde_json::json!({"parameterTypes":d.parameters.iter().map(oid).collect::<Vec<_>>(),"resultTypes":d.columns.iter().map(|c|oid(&c.data_type)).collect::<Vec<_>>()});
            stage = "validate-immutable";
            session.validate_immutable_constants(&p).map_err(state)?;
            let mut params = Vec::new();
            if let Some(p) = c["parameters"].as_array().unwrap().first() {
                params.push(match c["kind"].as_str().unwrap() {
                    "typed-float-cast" => {
                        let bits =
                            u64::from_str_radix(c["source"]["binary_hex"].as_str().unwrap(), 16)
                                .unwrap();
                        if c["source"]["type"] == "real" {
                            Value::Float4(Float4::from_bits(bits as u32))
                        } else {
                            Value::Float8(Float8::from_bits(bits))
                        }
                    }
                    "typed-numeric-cast" => {
                        Value::Numeric(Numeric::parse(p["text"].as_str().unwrap()).unwrap())
                    }
                    _ => Value::Text(p["text"].as_str().unwrap().into()),
                });
            }
            stage = "execute";
            let rows = session.execute_prepared(&p, &params, None).map_err(state)?;
            Ok(rows
                .rows
                .into_iter()
                .map(|r| {
                    r.values
                        .into_iter()
                        .map(|v| match v {
                            Value::Int(n) => n.to_string(),
                            Value::Text(s) => s,
                            other => panic!("{other:?}"),
                        })
                        .collect()
                })
                .collect())
        })();
        let pg = &c["observation"];
        let expected = if pg["status"] == "ok" {
            Ok(serde_json::from_value::<Vec<Vec<String>>>(pg["rows"].clone()).unwrap())
        } else {
            Err(pg["sqlstate"].as_str().unwrap().to_owned())
        };
        let mut matches = actual == expected;
        if !pg["parameterTypes"].is_null() && !metadata.is_null() {
            matches &= metadata["parameterTypes"] == pg["parameterTypes"];
            let expected_oids: Vec<_> = pg["resultTypes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| r["oid"].clone())
                .collect();
            matches &= metadata["resultTypes"] == serde_json::json!(expected_oids);
        }
        let known = c["kind"] == "unknown-function-argument" && pg["sqlstate"] == "42725";
        if !matches {
            if known {
                known_gaps += 1;
            } else {
                unexpected += 1;
            }
        }
        report.push(serde_json::json!({"case":c,"actual":format!("{actual:?}"),"actual_core_stage":stage,"pg_stage":c["stage"],"metadata":metadata,"matches":matches,"known_overload_gap":known}));
    }
    std::fs::write(
        std::env::var("INTEGER_CAST_ORACLE_REPORT").unwrap_or_else(|_| {
            let root = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
                .join("dd68")
                .join("dd68-integer-cast-design-20261106");
            std::fs::create_dir_all(&root).unwrap();
            root.join(format!("core-cast-report-{}.json", std::process::id()))
                .to_string_lossy()
                .into_owned()
        }),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    println!(
        "core_cases={}, unexpected_value_state_metadata_gaps={unexpected}, known_overload_gaps={known_gaps}; core_API_stages_are_not_pgwire_phases",
        report.len()
    );
    assert_eq!(unexpected, 0);
}

#[test]
fn wrong_kind_typed_parameters_preserve_type_error_and_no_effects() {
    let root = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("dd68")
        .join("dd68-integer-cast-design-20261106");
    std::fs::create_dir_all(&root).unwrap();
    let admin = Admin::create(
        root.join(format!("cast-wrong-kind-{}", std::process::id())),
        Limits::default(),
    )
    .unwrap();
    let mut session = admin.sql_session();
    session
        .execute("CREATE TABLE cast_kind(id integer primary key)", &[], None)
        .unwrap();
    let p = session
        .prepare("INSERT INTO cast_kind VALUES($1::integer)")
        .unwrap();
    for wrong in [
        Value::Text("65".into()),
        Value::Float8(Float8::from_bits(65f64.to_bits())),
        Value::Numeric(Numeric::parse("65").unwrap()),
    ] {
        assert_eq!(
            session.execute_prepared(&p, &[wrong], None).unwrap_err(),
            CoreError::Sql(SqlError::Type)
        );
        assert!(
            session
                .execute("SELECT id FROM cast_kind LIMIT 1", &[], None)
                .unwrap()
                .rows
                .is_empty()
        );
    }
}
