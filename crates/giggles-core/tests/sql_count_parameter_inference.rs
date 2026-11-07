use giggles_core::{Admin, CoreError, Limits, relational::*};

#[test]
fn count_parameter_limit_inference_matches_pg18() {
    let admin = Admin::create(fresh(), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute("CREATE TABLE agg(id bigint primary key)", &[], None)
        .unwrap();
    for id in [1, 2] {
        session
            .execute(&format!("INSERT INTO agg VALUES({id})"), &[], None)
            .unwrap();
    }
    for sql in [
        "SELECT count($1) FROM agg LIMIT $1",
        "SELECT count($1),count($1::bigint) FROM agg LIMIT $1",
    ] {
        let p = session.prepare(sql).unwrap();
        assert!(
            matches!(
                session.describe(&p),
                Err(CoreError::Sql(SqlError::AmbiguousParameter))
            ),
            "{sql}"
        );
    }
    let p = session
        .prepare("SELECT count($1) FROM agg LIMIT $2")
        .unwrap();
    assert!(matches!(
        session.describe(&p),
        Err(CoreError::Sql(SqlError::IndeterminateDatatype))
    ));
    let p = session
        .prepare("SELECT count($1::bigint) FROM agg LIMIT $1")
        .unwrap();
    assert_eq!(
        session.describe(&p).unwrap().parameters,
        vec![DataType::BigInt]
    );
    assert_eq!(
        session
            .execute_prepared(&p, &[Value::Int(1)], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Int(2)]
    );
    assert!(
        session
            .execute_prepared(&p, &[Value::Int(0)], None)
            .unwrap()
            .rows
            .is_empty()
    );
    let p = session
        .prepare("SELECT sum($1::integer),count($1) FROM agg LIMIT $1")
        .unwrap();
    assert_eq!(
        session.describe(&p).unwrap().parameters,
        vec![DataType::Integer]
    );
    assert_eq!(
        session
            .execute_prepared(&p, &[Value::Int(1)], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Value::Int(2), Value::Int(2)]
    );
}

fn fresh() -> std::path::PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let root = std::env::temp_dir().join("dd68-count-inference");
    std::fs::create_dir_all(&root).unwrap();
    root.join(format!(
        "count-{}-{:?}-{}-{}",
        std::process::id(),
        std::thread::current().id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ))
}

#[test]
fn count_parameter_inference_obeys_pg18_occurrence_and_error_precedence() {
    let admin = Admin::create(fresh(), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session
        .execute("CREATE TABLE agg(id bigint primary key)", &[], None)
        .unwrap();
    for id in [1, 2] {
        session
            .execute(&format!("INSERT INTO agg VALUES({id})"), &[], None)
            .unwrap();
    }
    for sql in [
        "SELECT count($1),sum($1::integer) FROM agg",
        "SELECT count($1),min($1::integer) FROM agg",
        "SELECT count($1),max($1::integer) FROM agg",
        "SELECT count($1),count($1::integer) FROM agg",
        "SELECT count($1) FROM agg WHERE $1::integer=1",
        "SELECT count($2),count($1),sum($1::integer) FROM agg",
        "SELECT count($1),count($2),sum($1::integer) FROM agg",
        "SELECT count($1),count($1),sum($1::integer) FROM agg",
        "SELECT count($2),count($1),sum($2::integer) FROM agg",
        "SELECT count($1),count($2),sum($2::integer) FROM agg",
    ] {
        let prepared = session.prepare(sql).unwrap();
        assert!(
            matches!(
                session.describe(&prepared),
                Err(CoreError::Sql(SqlError::AmbiguousParameter))
            ),
            "{sql}"
        );
    }
    for sql in [
        "SELECT count($1) FROM agg",
        "SELECT count($1),count($1) FROM agg",
        "SELECT count($1),sum($2::integer) FROM agg",
        "SELECT count($2),count($1) FROM agg",
    ] {
        let prepared = session.prepare(sql).unwrap();
        assert!(
            matches!(
                session.describe(&prepared),
                Err(CoreError::Sql(SqlError::IndeterminateDatatype))
            ),
            "{sql}"
        );
    }
    for (sql, expected) in [
        (
            "SELECT sum($1::integer),count($1) FROM agg",
            vec![Value::Int(2), Value::Int(2)],
        ),
        (
            "SELECT min($1::integer),count($1) FROM agg",
            vec![Value::Int(1), Value::Int(2)],
        ),
        (
            "SELECT count($1::integer),count($1) FROM agg",
            vec![Value::Int(2), Value::Int(2)],
        ),
    ] {
        let prepared = session.prepare(sql).unwrap();
        assert_eq!(
            session.describe(&prepared).unwrap().parameters,
            vec![DataType::Integer],
            "{sql}"
        );
        assert_eq!(
            session
                .execute_prepared(&prepared, &[Value::Int(1)], None)
                .unwrap()
                .rows[0]
                .values,
            expected,
            "{sql}"
        );
        assert_eq!(
            session
                .execute_prepared(&prepared, &[Value::Null], None)
                .unwrap()
                .rows[0]
                .values[1],
            Value::Int(0),
            "{sql}"
        );
    }
    for sql in ["SELECT min($1) FROM agg", "SELECT max($1) FROM agg"] {
        let prepared = session.prepare(sql).unwrap();
        assert_eq!(
            session.describe(&prepared).unwrap().parameters,
            vec![DataType::Text]
        );
        assert_eq!(
            session
                .execute_prepared(&prepared, &[Value::Text("a".into())], None)
                .unwrap()
                .rows[0]
                .values,
            vec![Value::Text("a".into())]
        );
    }
}
