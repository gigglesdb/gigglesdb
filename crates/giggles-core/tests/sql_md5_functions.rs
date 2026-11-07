use giggles_core::{relational::*, *};
use std::sync::atomic::{AtomicU64, Ordering};

fn db() -> (Admin, SqlSession) {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let path = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("dd68")
        .join("dd68-md5-functions-20261106")
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
        .execute("CREATE TABLE md5_input (id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    session
        .execute("INSERT INTO md5_input VALUES(1)", &[], None)
        .unwrap();
    (admin, session)
}

#[test]
fn md5_both_overloads_match_exact_pg18_vectors_and_parameter_types() {
    let (_admin, mut session) = db();
    let corpus: serde_json::Value = serde_json::from_slice(
        &std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/dd68/md5-oracle.json"
        ))
        .unwrap(),
    )
    .unwrap();
    for row in corpus.as_array().unwrap() {
        let bytes = row["kind"] == "bytea";
        let query = if bytes {
            "SELECT pg_catalog.\"md5\"($1::bytea) FROM md5_input"
        } else {
            "SELECT pg_catalog.\"md5\"($1) FROM md5_input"
        };
        let prepared = session.prepare(query).unwrap();
        let description = session.describe(&prepared).unwrap();
        assert_eq!(
            description.parameters,
            vec![if bytes {
                DataType::Bytes
            } else {
                DataType::Text
            }]
        );
        assert_eq!(description.columns[0].data_type, DataType::Text);
        let input = row["input"].as_str().unwrap();
        let value = if bytes {
            Value::Bytes(
                input
                    .as_bytes()
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                    .collect(),
            )
        } else {
            Value::Text(input.into())
        };
        let result = session.execute_prepared(&prepared, &[value], None).unwrap();
        assert_eq!(
            result.rows[0].values,
            vec![Value::Text(row["expected"].as_str().unwrap().into())]
        );
        let null = session
            .execute_prepared(&prepared, &[Value::Null], None)
            .unwrap();
        assert_eq!(null.rows[0].values, vec![Value::Null]);
    }
}

#[test]
fn md5_unknown_literals_casts_arity_and_invalid_types() {
    let (_admin, mut session) = db();
    for expression in ["md5('abc')", "md5('abc'::text)", "md5('abc'::bytea)"] {
        let result = session
            .execute(&format!("SELECT {expression} FROM md5_input"), &[], None)
            .unwrap();
        assert_eq!(
            result.rows[0].values,
            vec![Value::Text("900150983cd24fb0d6963f7d28e17f72".into())]
        );
    }
    for expression in ["md5()", "md5(1)", "md5(TRUE)", "md5('a','b')"] {
        assert_eq!(
            session.execute(&format!("SELECT {expression} FROM md5_input"), &[], None),
            Err(CoreError::Sql(SqlError::UndefinedFunction)),
            "{expression}"
        );
    }
    for expression in ["md5(missing,1)", "md5(TRUE,missing)"] {
        assert_eq!(
            session.execute(&format!("SELECT {expression} FROM md5_input"), &[], None),
            Err(CoreError::Sql(SqlError::UnknownColumn)),
            "{expression}"
        );
    }
}
