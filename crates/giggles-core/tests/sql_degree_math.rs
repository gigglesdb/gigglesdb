use giggles_core::{relational::*, *};
use std::sync::atomic::{AtomicU64, Ordering};

fn db() -> (Admin, SqlSession) {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let path = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("dd68")
        .join("dd68-degree-math-20261106")
        .join("fixtures")
        .join(format!(
            "{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
    std::fs::create_dir_all(std::path::Path::new(&path).parent().unwrap()).unwrap();
    let admin = Admin::create(path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute(
            "CREATE TABLE degree_input (id BIGINT PRIMARY KEY)",
            &[],
            None,
        )
        .unwrap();
    session
        .execute("INSERT INTO degree_input VALUES (1)", &[], None)
        .unwrap();
    (admin, session)
}

#[test]
fn degree_trig_exact_pinned_pg18_text_and_binary() {
    let (_admin, mut session) = db();
    let corpus: serde_json::Value = serde_json::from_slice(
        &std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/dd68/degree-math-oracle.json"
        ))
        .unwrap(),
    )
    .unwrap();
    let mut mismatches = Vec::new();
    for row in corpus.as_array().unwrap() {
        let expr = row["expression"].as_str().unwrap();
        let result = session
            .execute(&format!("SELECT {expr} FROM degree_input"), &[], None)
            .unwrap();
        let Value::Float8(value) = result.rows[0].values[0] else {
            panic!("{expr}")
        };
        let hex = format!("{:016x}", value.value().to_bits());
        if hex != row["hex"].as_str().unwrap() || value.to_text() != row["text"].as_str().unwrap() {
            mismatches.push(format!(
                "{expr}: actual {hex}/{} expected {}/{}",
                value.to_text(),
                row["hex"],
                row["text"]
            ));
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}

#[test]
fn degree_trig_parameters_null_domain_and_arity() {
    let (_admin, mut session) = db();
    for name in [
        "sind", "cosd", "tand", "cotd", "asind", "acosd", "atand", "atan2d",
    ] {
        let args = if name == "atan2d" { "$1,$2" } else { "$1" };
        let p = session
            .prepare(&format!(
                "SELECT pg_catalog.\"{name}\"({args}) FROM degree_input"
            ))
            .unwrap();
        let d = session.describe(&p).unwrap();
        assert_eq!(
            d.parameters,
            vec![DataType::DoublePrecision; if name == "atan2d" { 2 } else { 1 }]
        );
        assert_eq!(d.columns[0].data_type, DataType::DoublePrecision);
        let result = session
            .execute_prepared(&p, &vec![Value::Null; d.parameters.len()], None)
            .unwrap();
        assert_eq!(result.rows[0].values, vec![Value::Null]);
        for args in ["", "1,2,3"] {
            assert_eq!(
                session.execute(
                    &format!("SELECT {name}({args}) FROM degree_input"),
                    &[],
                    None
                ),
                Err(CoreError::Sql(SqlError::UndefinedFunction))
            );
        }
        let args = if name == "atan2d" { "TRUE,1" } else { "TRUE" };
        assert_eq!(
            session.execute(
                &format!("SELECT {name}({args}) FROM degree_input"),
                &[],
                None
            ),
            Err(CoreError::Sql(SqlError::UndefinedFunction))
        );
    }
    for expr in [
        "sind('Infinity')",
        "cosd('-Infinity')",
        "tand('Infinity')",
        "cotd('Infinity')",
        "asind(2)",
        "acosd(-2)",
    ] {
        assert_eq!(
            session.execute(&format!("SELECT {expr} FROM degree_input"), &[], None),
            Err(CoreError::Sql(SqlError::Overflow)),
            "{expr}"
        );
    }
}
