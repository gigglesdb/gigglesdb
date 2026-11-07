use giggles_core::{Admin, CoreError, Limits, relational::*};
use serde_json::{Value as Json, json};
use std::path::PathBuf;

fn unique_path(kind: &str) -> PathBuf {
    let root = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("dd68")
        .join("dd68-character-adversarial-20261106")
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

fn type_oid(ty: &DataType) -> u32 {
    match ty {
        DataType::SmallInt => 21,
        DataType::Integer => 23,
        DataType::BigInt => 20,
        DataType::Text => 25,
        DataType::Boolean => 16,
        DataType::Numeric => 1700,
        other => panic!("unplanned adversarial type {other:?}"),
    }
}

fn text(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Text(s) => Some(s.clone()),
        Value::Int(n) => Some(n.to_string()),
        Value::Bool(v) => Some(if *v { "t" } else { "f" }.into()),
        other => panic!("unplanned adversarial result {other:?}"),
    }
}

fn error(stage: &str, error: CoreError) -> Json {
    let (class, state) = match &error {
        CoreError::Sql(e) => (
            format!("{e:?}"),
            match e {
                SqlError::Unsupported => Some("0A000"),
                SqlError::Syntax => Some("42601"),
                SqlError::Type => Some("42804"),
                SqlError::Overflow => Some("22003"),
                SqlError::Limit => Some("54000"),
                SqlError::UndefinedFunction => Some("42883"),
                SqlError::InvalidParameterValue => Some("22023"),
                SqlError::UnknownColumn => Some("42703"),
                _ => None,
            },
        ),
        other => (format!("{other:?}"), None),
    };
    json!({"status":"error","stage":stage,"error":class,"core_error":format!("{error:?}"),"is_exact_sql_type":error==CoreError::Sql(SqlError::Type),"mapped_core_sqlstate":state})
}

fn observe(session: &mut giggles_core::SqlSession, case: &Json) -> Json {
    for setup in case["setup"].as_array().unwrap() {
        if let Err(e) = session.execute(setup.as_str().unwrap(), &[], None) {
            return error("setup", e);
        }
    }
    let prepared = match session.prepare(case["query"].as_str().unwrap()) {
        Ok(p) => p,
        Err(e) => return error("prepare", e),
    };
    let description = match session.describe(&prepared) {
        Ok(d) => d,
        Err(e) => return error("describe", e),
    };
    let parameters: Vec<_> = description.parameters.iter().map(type_oid).collect();
    let columns: Vec<_> = description
        .columns
        .iter()
        .map(|c| type_oid(&c.data_type))
        .collect();
    let reference = &case["observation"];
    let parameter_types = reference["parameterTypes"].as_array();
    let params: Vec<_> = case["params"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
        .map(|(i, value)| {
            if value.is_null() {
                Value::Null
            } else if parameter_types
                .and_then(|t| t.get(i))
                .and_then(Json::as_u64)
                .is_some_and(|oid| matches!(oid, 20 | 21 | 23))
            {
                Value::Int(value.as_str().unwrap().parse().unwrap())
            } else {
                Value::Text(value.as_str().unwrap().into())
            }
        })
        .collect();
    // This explicit pure check models PG constant validation at Bind; it must
    // never evaluate parameter values. Core API phases are not wire phases.
    let result = match session.validate_immutable_constants(&prepared) {
        Ok(()) => session
            .execute_prepared(&prepared, &params, None)
            .map_err(|e| error("execute", e)),
        Err(e) => Err(error("constant_validation", e)),
    };
    let mut observed = match result {
        Ok(result) => {
            let rows: Vec<Vec<_>> = result
                .rows
                .iter()
                .map(|r| r.values.iter().map(text).collect())
                .collect();
            json!({"status":"ok","stage":"execute","rows":rows})
        }
        Err(e) => e,
    };
    observed["parameterTypes"] = json!(parameters);
    observed["resultOids"] = json!(columns);
    observed
}

#[test]
fn character_adversarial_pg18_all_cases_values_oids_errors_and_visible_gaps() {
    let fixture: Json =
        serde_json::from_str(include_str!("fixtures/character-adversarial-pg18.json")).unwrap();
    assert_eq!(fixture["case_count"], 210);
    assert_eq!(fixture["planned_parity"], 198);
    assert_eq!(fixture["typed_core_admission_controls"], 4);
    assert_eq!(fixture["known_gaps"], 6);
    assert_eq!(fixture["pg_only_input_domain"], 2);
    assert_eq!(fixture["identity"]["rows"][0][1], "UTF8");
    assert_eq!(fixture["identity"]["rows"][0][2], "C");
    assert_eq!(fixture["identity"]["rows"][0][3], "C");
    let admin = Admin::create(unique_path("fixtures"), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    let mut observations = Vec::new();
    let mut mismatches = Vec::new();
    let (mut parity, mut typed_admission, mut gaps, mut excluded) = (0, 0, 0, 0);
    for case in fixture["cases"].as_array().unwrap() {
        let id = case["id"].as_str().unwrap();
        let class = case["classification"].as_str().unwrap();
        if class == "pg_only_input_domain" {
            // PG's protocol rejects NUL before function execution. Do not pass
            // that input to the core API and call it PostgreSQL compatibility.
            assert_eq!(case["observation"]["sqlstate"], "22021");
            assert!(
                case["params"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|v| v.as_str().is_some_and(|s| s.contains('\0')))
            );
            excluded += 1;
            observations.push(
                json!({"id":id,"classification":class,"core":"not_executed_PG_input_domain"}),
            );
            continue;
        }
        let mut actual = observe(&mut session, case);
        let expected = &case["observation"];
        let mut diffs = Vec::new();
        if class == "core_typed_admission_control" {
            typed_admission += 1;
            // PostgreSQL receives text Bind input here; this Core API call
            // instead supplies an invalid declared-width Value::Int. Assert
            // its native enum exactly, without comparing mapped wire states.
            actual
                .as_object_mut()
                .unwrap()
                .remove("mapped_core_sqlstate");
            actual["not_protocol_comparable"] = json!(true);
            if actual["status"] != "error" || actual["is_exact_sql_type"] != true {
                diffs.push(format!(
                    "native Core API admission expected CoreError::Sql(Type), actual {actual}"
                ));
            }
            assert_eq!(expected["sqlstate"], "22003");
        } else if class == "known_gap" || class == "known_gap_setup" {
            gaps += 1;
            let expected_error = if case["id"] == "ADV_GAP_CAST_TEXT" {
                assert_eq!(case["query"], "SELECT chr('oops'::integer)");
                assert_eq!(expected["sqlstate"], "22P02");
                json!("InvalidTextRepresentation")
            } else {
                case["expected_core_error"].clone()
            };
            if actual["status"] != "error" || actual["error"] != expected_error {
                diffs.push(format!(
                    "declared inherited-gap profile expected {}, actual {actual}",
                    expected_error
                ));
            }
            if class == "known_gap_setup" && actual["stage"] != "setup" {
                diffs.push("domain gap must occur in setup".into());
            }
        } else {
            assert_eq!(class, "parity");
            parity += 1;
            if actual["status"] != expected["status"] {
                diffs.push(format!("status: PG {expected}, core {actual}"));
            }
            if expected["status"] == "ok" && actual["rows"] != expected["rows"] {
                diffs.push(format!(
                    "exact ordered text/null rows: PG {}, core {}",
                    expected["rows"], actual["rows"]
                ));
            }
            if expected["status"] == "error"
                && actual["mapped_core_sqlstate"] != expected["sqlstate"]
            {
                diffs.push(format!(
                    "PG wire SQLSTATE {}, adapter-mapped Core API state {} (not an observed core wire state)",
                    expected["sqlstate"], actual["mapped_core_sqlstate"]
                ));
            }
            if expected["parameterTypes"].is_array() {
                if actual["parameterTypes"] != expected["parameterTypes"] {
                    diffs.push(format!(
                        "parameter OIDs: PG {}, core {}",
                        expected["parameterTypes"], actual["parameterTypes"]
                    ));
                }
                let expected_oids: Vec<_> = expected["resultTypes"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|c| c["oid"].clone())
                    .collect();
                if actual["resultOids"] != json!(expected_oids) {
                    diffs.push(format!(
                        "result OIDs: PG {}, core {}",
                        json!(expected_oids),
                        actual["resultOids"]
                    ));
                }
            }
        }
        if !diffs.is_empty() {
            mismatches.push(json!({"id":id,"query":case["query"],"params":case["params"],"classification":class,"differences":diffs}));
        }
        observations.push(json!({"id":id,"classification":class,"core":actual,"pg":expected}));
    }
    assert_eq!((parity, typed_admission, gaps, excluded), (198, 4, 6, 2));
    let report = json!({"fixture_count":210,"parity_candidates":parity,"typed_core_admission_controls":typed_admission,"known_gap_profiles":gaps,"PG_only_input_domain_exclusions":excluded,"mismatches":mismatches,"cases":observations,"limitations":["UTF8 libc C/Ctype only","Known gaps and invalid typed Core API admission controls are retained and not certified as parity","Core API phases and mapped_core_sqlstate interpretation do not certify observed pgwire Bind/Execute phase or SQLSTATE parity","Frozen source-v3 product only; not latest shared workspace"]});
    let path = unique_path("reports").with_extension("json");
    std::fs::write(&path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    println!("CHARACTER_ADVERSARIAL_REPORT={}", path.display());
    assert!(
        report["mismatches"].as_array().unwrap().is_empty(),
        "adversarial mismatches: {} (report {})",
        report["mismatches"],
        path.display()
    );
}

#[test]
fn character_giggles_byte_caps_and_failed_writes_preserve_rows_and_inputs() {
    let admin = Admin::create(unique_path("budget-fixtures"), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute(
            "CREATE TABLE adv_budget (id bigint primary key,label text)",
            &[],
            None,
        )
        .unwrap();
    session
        .execute("INSERT INTO adv_budget VALUES(1,'sentinel')", &[], None)
        .unwrap();
    for length in [MAX_ROW_BYTES, MAX_ROW_BYTES + 1] {
        let input = Value::Text("a".repeat(length));
        let original = input.clone();
        let p = session.prepare("SELECT initcap($1)").unwrap();
        let result = session.execute_prepared(&p, std::slice::from_ref(&input), None);
        if length == MAX_ROW_BYTES {
            let value = &result.unwrap().rows[0].values[0];
            assert!(matches!(value,Value::Text(s) if s.len()==length && s.starts_with('A')));
        } else {
            assert_eq!(result.unwrap_err(), CoreError::Sql(SqlError::Limit));
        }
        assert_eq!(input, original);
    }
    let oversize = Value::Text("a".repeat(MAX_ROW_BYTES + 1));
    // Function-valued SET is supported, and byte caps still apply.
    let original = oversize.clone();
    let p = session
        .prepare("UPDATE adv_budget SET label=initcap($1) WHERE id=1")
        .unwrap();
    assert_eq!(
        session
            .execute_prepared(&p, std::slice::from_ref(&oversize), None)
            .unwrap_err(),
        CoreError::Sql(SqlError::Limit)
    );
    assert_eq!(
        session
            .execute("SELECT label FROM adv_budget WHERE id=1", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Text("sentinel".into())]
    );
    assert_eq!(oversize, original);
    let p = session
        .prepare("UPDATE adv_budget SET label=chr($1::smallint) WHERE id=1")
        .unwrap();
    session
        .execute_prepared(&p, &[Value::Int(65)], None)
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT label FROM adv_budget WHERE id=1", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Text("A".into())]
    );
    session
        .execute("UPDATE adv_budget SET label='sentinel' WHERE id=1", &[], None)
        .unwrap();
    let p = session
        .prepare("UPDATE adv_budget SET label=$1 WHERE id=1")
        .unwrap();
    assert_eq!(
        session
            .execute_prepared(&p, std::slice::from_ref(&oversize), None)
            .unwrap_err(),
        CoreError::Sql(SqlError::Limit)
    );
    assert_eq!(
        session
            .execute("SELECT label FROM adv_budget WHERE id=1", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Text("sentinel".into())]
    );
    // This is a documented Giggles result-byte limit, not PG error parity.
    let query = format!(
        "SELECT {}",
        std::iter::repeat_n("initcap($1)", 17)
            .collect::<Vec<_>>()
            .join(",")
    );
    let p = session.prepare(&query).unwrap();
    assert_eq!(
        session
            .execute_prepared(&p, &[Value::Text("a".repeat(MAX_ROW_BYTES))], None)
            .unwrap_err(),
        CoreError::Sql(SqlError::Limit)
    );
}

#[test]
fn character_projection_large_join_is_cancellable() {
    use giggles_core::Cancellation;

    let admin = Admin::create(unique_path("work-fixtures"), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute(
            "CREATE TABLE adv_work (id bigint primary key,label text,code integer)",
            &[],
            None,
        )
        .unwrap();
    session.execute("BEGIN", &[], None).unwrap();
    let insert = session
        .prepare("INSERT INTO adv_work VALUES($1,$2,$3)")
        .unwrap();
    for id in 0..1024 {
        session
            .execute_prepared(
                &insert,
                &[Value::Int(id), Value::Text("abc".into()), Value::Int(65)],
                None,
            )
            .unwrap();
    }
    session.execute("COMMIT", &[], None).unwrap();
    // Large indexed joins remain executable beyond the former candidate-work cap.
    let at_cap = "SELECT ascii(a.label),initcap(d.label),chr(a.code) FROM adv_work a JOIN adv_work b ON b.id=a.id JOIN adv_work c ON c.id=b.id JOIN adv_work d ON d.id=c.id";
    let result = session.execute(at_cap, &[], None).unwrap();
    assert_eq!(result.rows.len(), 1024);
    assert!(result.rows.iter().all(|r| r.values
        == vec![
            Value::Int(97),
            Value::Text("Abc".into()),
            Value::Text("A".into())
        ]));
    let beyond_cap = format!("{at_cap} JOIN adv_work e ON e.id=d.id");
    let result = session.execute(&beyond_cap, &[], None);
    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(result.rows.len(), 1024);
    assert!(result.rows.iter().all(|r| r.values
        == vec![
            Value::Int(97),
            Value::Text("Abc".into()),
            Value::Text("A".into())
        ]));
    let c = Cancellation::default();
    assert!(c.cancel());
    // A pre-cancelled scope can reject before query execution or admission.
    let r = c.scope(|| session.execute(&beyond_cap, &[], None));
    assert_eq!(r.unwrap_err(), CoreError::Cancelled);
    assert_eq!(
        session
            .execute("SELECT count(*) FROM adv_work", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Int(1024)]
    );
}
