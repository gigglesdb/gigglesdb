use giggles_core::{Admin, CoreError, Limits, relational::*};
use serde_json::{Value as Json, json};
use std::path::PathBuf;
fn unique_path(kind: &str) -> PathBuf {
    let root = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("dd68")
        .join("dd68-quoting-functions-20261106")
        .join(kind);
    std::fs::create_dir_all(&root).unwrap();
    root.join(format!(
        "{}-{:?}-{}",
        std::process::id(),
        std::thread::current().id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
#[test]
fn quoting_nullable_null_and_keyword_controls() {
    let admin = Admin::create(unique_path("fixtures"), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    for (query, value) in [
        ("SELECT quote_nullable(NULL)", Value::Text("NULL".into())),
        ("SELECT quote_literal(NULL)", Value::Null),
        (
            "SELECT quote_ident('select')",
            Value::Text("\"select\"".into()),
        ),
        ("SELECT quote_ident('abort')", Value::Text("abort".into())),
    ] {
        let p = session.prepare(query).unwrap();
        let d = session.describe(&p).unwrap();
        assert_eq!(d.columns[0].data_type, DataType::Text);
        session.validate_immutable_constants(&p).unwrap();
        assert_eq!(
            session.execute_prepared(&p, &[], None).unwrap().rows[0].values,
            vec![value]
        );
    }
}

fn quoting_oid(ty: &DataType) -> u32 {
    match ty {
        DataType::Text => 25,
        DataType::Integer => 23,
        DataType::BigInt => 20,
        other => panic!("unexpected type {other:?}"),
    }
}
fn quoting_error(stage: &str, e: CoreError) -> Json {
    let (kind, state) = match &e {
        CoreError::Sql(error) => (
            format!("{error:?}"),
            match error {
                SqlError::Unsupported => Some("0A000"),
                SqlError::UndefinedFunction => Some("42883"),
                SqlError::UnknownColumn => Some("42703"),
                SqlError::Limit => Some("54000"),
                SqlError::Type => Some("42804"),
                _ => None,
            },
        ),
        other => (format!("{other:?}"), None),
    };
    json!({"status":"error","stage":stage,"core_error":format!("{e:?}"),"error":kind,"is_exact_parameters":e==CoreError::Sql(SqlError::Parameters),"mapped_core_state":state})
}
fn quoting_observe(session: &mut giggles_core::SqlSession, case: &Json) -> Json {
    for setup in case["core_setup"].as_array().unwrap() {
        if let Err(e) = session.execute(setup.as_str().unwrap(), &[], None) {
            return quoting_error("setup", e);
        }
    }
    let p = match session.prepare(case["query"].as_str().unwrap()) {
        Ok(p) => p,
        Err(e) => return quoting_error("prepare", e),
    };
    let d = match session.describe(&p) {
        Ok(d) => d,
        Err(e) => return quoting_error("describe", e),
    };
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
    let result = match session.validate_immutable_constants(&p) {
        Ok(()) => session
            .execute_prepared(&p, &params, None)
            .map_err(|e| quoting_error("execute", e)),
        Err(e) => Err(quoting_error("constant_validation", e)),
    };
    let mut result = match result {
        Ok(r) => {
            let rows: Vec<Vec<Option<String>>> = r
                .rows
                .into_iter()
                .map(|r| {
                    r.values
                        .into_iter()
                        .map(|v| match v {
                            Value::Null => None,
                            Value::Text(s) => Some(s),
                            Value::Int(n) => Some(n.to_string()),
                            other => panic!("unexpected value{other:?}"),
                        })
                        .collect()
                })
                .collect();
            json!({"status":"ok","rows":rows})
        }
        Err(e) => e,
    };
    result["parameterTypes"] = json!(d.parameters.iter().map(quoting_oid).collect::<Vec<_>>());
    result["resultOids"] = json!(
        d.columns
            .iter()
            .map(|c| quoting_oid(&c.data_type))
            .collect::<Vec<_>>()
    );
    result
}
#[test]
fn quoting_pg18_exact_text_oids_states_and_retained_scope_gaps() {
    let fixture: Json = serde_json::from_str(include_str!("fixtures/quoting-pg18.json")).unwrap();
    assert_eq!(fixture["case_count"], 697);
    let admin = Admin::create(unique_path("oracle-fixtures"), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    let mut records = vec![];
    let mut mismatches = vec![];
    let (mut parity, mut gaps) = (0, 0);
    for c in fixture["cases"].as_array().unwrap() {
        let actual = quoting_observe(&mut session, c);
        let pg = &c["observation"];
        let mut diff = vec![];
        if c["classification"] == "parity" {
            parity += 1;
            if actual["status"] != pg["status"] {
                diff.push(format!("status PG{pg} core{actual}"));
            }
            if pg["status"] == "ok" && actual["rows"] != pg["rows"] {
                diff.push(format!(
                    "exact text rows PG{} core{}",
                    pg["rows"], actual["rows"]
                ));
            }
            if pg["status"] == "error" && actual["mapped_core_state"] != pg["sqlstate"] {
                diff.push(format!(
                    "PG observed state{} versus Core adapter interpretation{}",
                    pg["sqlstate"], actual["mapped_core_state"]
                ));
            }
            if pg["parameterTypes"].is_array() {
                if actual["parameterTypes"] != pg["parameterTypes"] {
                    diff.push(format!(
                        "parameter OIDs PG{} core{}",
                        pg["parameterTypes"], actual["parameterTypes"]
                    ));
                }
                let oids: Vec<_> = pg["resultTypes"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|o| o["oid"].clone())
                    .collect();
                if actual["resultOids"] != json!(oids) {
                    diff.push(format!(
                        "result OIDs PG{} core{}",
                        json!(oids),
                        actual["resultOids"]
                    ));
                }
            }
        } else {
            gaps += 1;
            if c["classification"] == "inherited_mixed_priority_gap"
                && actual["is_exact_parameters"] != true
            {
                diff.push(format!("native CoreError::Sql(Parameters) required for declared mixed-priority gap: {actual}"));
            }
            if actual["status"] != "error" || actual["error"] != c["expected_core_error"] {
                diff.push(format!(
                    "declared scoped gap expected{} actual{actual}",
                    c["expected_core_error"]
                ));
            }
        }
        if !diff.is_empty() {
            mismatches.push(json!({"id":c["id"],"query":c["query"],"params":c["params"],"classification":c["classification"],"diff":diff}));
        }
        records.push(json!({"id":c["id"],"scope":c["scope"],"classification":c["classification"],"core":actual,"pg":pg}));
    }
    assert_eq!(parity + gaps, 697);
    let report = json!({"count":697,"parity_candidates":parity,"gap_controls":gaps,"mismatches":mismatches,"cases":records,"profile":"Text overloads, quote_all_identifiers=off only; ANYELEMENT/nondefaultGUC and inherited namespace/unknown-param gaps retained. Core mapped states are not observed wire states."});
    let path = unique_path("reports").with_extension("json");
    std::fs::write(&path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    println!("QUOTING_CORE_REPORT={}", path.display());
    assert!(
        report["mismatches"].as_array().unwrap().is_empty(),
        "{}",
        report["mismatches"]
    );
}

#[test]
fn quoting_byte_expansion_caps_constants_and_legacy_strict_nulls() {
    let admin = Admin::create(unique_path("resource-fixtures"), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    for (name, input, bytes) in [
        ("quote_ident", "a".repeat(MAX_ROW_BYTES), MAX_ROW_BYTES),
        (
            "quote_ident",
            "\"".repeat((MAX_ROW_BYTES - 2) / 2),
            MAX_ROW_BYTES,
        ),
        (
            "quote_literal",
            "'".repeat((MAX_ROW_BYTES - 2) / 2),
            MAX_ROW_BYTES,
        ),
    ] {
        let p = session.prepare(&format!("SELECT {name}($1)")).unwrap();
        let value = Value::Text(input);
        let original = value.clone();
        let result = session
            .execute_prepared(&p, std::slice::from_ref(&value), None)
            .unwrap();
        assert!(matches!(&result.rows[0].values[0],Value::Text(s) if s.len()==bytes));
        assert_eq!(value, original);
    }
    for name in ["quote_ident", "quote_literal", "quote_nullable"] {
        let p = session.prepare(&format!("SELECT {name}($1)")).unwrap();
        let input = Value::Text(if name == "quote_ident" {
            "\"".repeat(MAX_ROW_BYTES / 2)
        } else {
            "'".repeat(MAX_ROW_BYTES / 2)
        });
        let original = input.clone();
        assert_eq!(
            session
                .execute_prepared(&p, std::slice::from_ref(&input), None)
                .unwrap_err(),
            CoreError::Sql(SqlError::Limit)
        );
        assert_eq!(input, original);
    }
    for suffix in ["", " WHERE FALSE", " LIMIT 0"] {
        let q = format!(
            "SELECT quote_ident('{}'){suffix}",
            "\"".repeat(MAX_ROW_BYTES / 2)
        );
        let p = session.prepare(&q).unwrap();
        session.describe(&p).unwrap();
        assert_eq!(
            session.validate_immutable_constants(&p).unwrap_err(),
            CoreError::Sql(SqlError::Limit)
        );
    }
    let p=session.prepare("SELECT ascii(NULL),md5(NULL::text),quote_ident(NULL),quote_literal(NULL),quote_nullable(NULL)").unwrap();
    assert_eq!(
        session.execute_prepared(&p, &[], None).unwrap().rows[0].values,
        vec![
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Text("NULL".into())
        ]
    );
}

#[test]
fn quoting_shared_projection_budget_rejects_before_any_result() {
    let admin = Admin::create(unique_path("shared-budget-fixtures"), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    let query = format!(
        "SELECT {}",
        std::iter::repeat_n("quote_ident($1)", 17)
            .collect::<Vec<_>>()
            .join(",")
    );
    let p = session.prepare(&query).unwrap();
    let input = Value::Text("a".repeat(MAX_ROW_BYTES));
    let original = input.clone();
    assert_eq!(
        session
            .execute_prepared(&p, std::slice::from_ref(&input), None)
            .unwrap_err(),
        CoreError::Sql(SqlError::Limit)
    );
    assert_eq!(input, original);
}
