#![cfg(feature = "engine-rocksdb")]
use giggles_core::{Admin, CoreError, Limits, operations::*, relational::Value};
use std::path::PathBuf;

fn setup() -> PathBuf {
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).unwrap();
    let path = std::env::temp_dir().join(format!(
        "embedded-fence-{:032x}",
        u128::from_be_bytes(nonce)
    ));
    let a = Admin::create_engine(&path, Limits::default(), giggles_core::Engine::V2).unwrap();
    for sql in [
        "CREATE TABLE items(id BIGINT PRIMARY KEY,n BIGINT)",
        "INSERT INTO items VALUES (1,10)",
        "CREATE SEQUENCE sq",
    ] {
        a.sql_session().execute(sql, &[], None).unwrap();
    }
    assert_eq!(a.install_write_fence(b"holder", "embedded").unwrap(), 1);
    drop(a);
    path
}
fn refused(e: CoreError) {
    assert_eq!(e, CoreError::WriteFenced { epoch: 1 });
    assert_eq!(giggles_core::sqlstate(&e), "25006");
    assert_eq!(
        giggles_core::local::failure(CoreError::WriteFenced { epoch: 1 }).code,
        22
    );
    assert_eq!(e.to_string(), "database is write-fenced (epoch 1)");
}
fn sequence(
    a: &Admin,
    sql: &str,
) -> std::result::Result<giggles_core::relational::SqlResult, CoreError> {
    let clock = giggles_core::relational::StatementClock {
        unavailable: None,
        transaction_us: 100,
        statement_us: 100,
        zone: Some(giggles_pg_types::temporal::SessionZone::Fixed(0)),
        wall: None,
    };
    a.sql_session().execute_at(sql, &[], None, &clock)
}
fn rows(a: &Admin) -> Vec<Vec<Value>> {
    a.sql_session()
        .execute("SELECT * FROM items ORDER BY id", &[], None)
        .unwrap()
        .rows
        .into_iter()
        .map(|r| r.values)
        .collect()
}
const WRITES: &[&str] = &[
    "INSERT INTO items VALUES (2,20)",
    "UPDATE items SET n=99 WHERE id=1",
    "DELETE FROM items WHERE id=1",
    "CREATE TABLE allowed(id BIGINT PRIMARY KEY)",
    "ALTER TABLE items ADD COLUMN extra BIGINT",
    "DROP TABLE items",
];
#[test]
fn non_holder_sql_and_ddl_refused_without_changes() {
    let path = setup();
    let a = Admin::open(&path, Limits::default()).unwrap();
    let before = rows(&a);
    let catalog = a.catalog().unwrap();
    for sql in WRITES {
        refused(a.sql_session().execute(sql, &[], None).expect_err(sql));
        assert_eq!(rows(&a), before, "{sql}");
        assert_eq!(a.catalog().unwrap(), catalog, "{sql}");
    }
    drop(a);
    std::fs::remove_dir_all(path).unwrap();
}
fn native_writes() -> Vec<Request> {
    let id = Expr::Compare(
        Box::new(Expr::Field("id".into())),
        Compare::Eq,
        Box::new(Expr::Value(ValueExpr::Literal(Value::Int(1)))),
    );
    vec![
        Request::Insert {
            table: "items".into(),
            values: vec![
                ("id".into(), ValueExpr::Literal(Value::Int(3))),
                ("n".into(), ValueExpr::Literal(Value::Int(30))),
            ],
        },
        Request::Update {
            table: "items".into(),
            filter: id.clone(),
            guard: None,
            values: vec![("n".into(), ValueExpr::Literal(Value::Int(11)))],
            intent: Intent::One,
        },
        Request::Delete {
            table: "items".into(),
            filter: id,
            guard: None,
            cap: None,
        },
    ]
}
fn native_read(filter: Option<Expr>) -> Request {
    Request::Read {
        table: "items".into(),
        filter,
        order: vec![],
        limit: None,
        projection: vec!["id".into(), "n".into()],
    }
}
#[test]
fn non_holder_native_refused_get_and_scan_allowed() {
    let path = setup();
    let a = Admin::open(&path, Limits::default()).unwrap();
    let e = giggles_core::operations::Engine::new(a.clone());
    let bindings = Bindings::new();
    let before = rows(&a);
    for request in native_writes() {
        // The native facade wraps non-SQL CoreError variants as Error::Core.
        assert_eq!(
            e.execute(&request, &bindings).unwrap_err(),
            Error::Core(CoreError::WriteFenced { epoch: 1 }.to_string())
        );
        assert_eq!(rows(&a), before);
    }
    assert_eq!(
        e.execute(&native_read(None), &bindings).unwrap().rows[0].values,
        before[0]
    );
    let get = native_read(Some(Expr::Compare(
        Box::new(Expr::Field("id".into())),
        Compare::Eq,
        Box::new(Expr::Value(ValueExpr::Literal(Value::Int(1)))),
    )));
    assert_eq!(
        e.execute(&get, &bindings).unwrap().rows[0].values,
        before[0]
    );
    drop(e);
    drop(a);
    std::fs::remove_dir_all(path).unwrap();
}
fn copy(a: &Admin) -> std::result::Result<giggles_core::relational::SqlResult, CoreError> {
    let mut s = a.sql_session();
    let (_, p) = s.prepare_copy("COPY items (id,n) FROM STDIN").unwrap();
    s.ingest_copy(&p, vec![vec![Value::Int(4), Value::Int(40)]])
}
#[test]
fn non_holder_copy_and_sequences_refused_without_changes() {
    let path = setup();
    let a = Admin::open(&path, Limits::default()).unwrap();
    let before = rows(&a);
    refused(copy(&a).unwrap_err());
    assert_eq!(rows(&a), before);
    for sql in ["SELECT nextval('sq')", "SELECT setval('sq',99)"] {
        refused(sequence(&a, sql).unwrap_err());
    }
    drop(a);
    let a = Admin::open_as_fence_holder(&path, Limits::default(), b"holder").unwrap();
    assert_eq!(
        sequence(&a, "SELECT nextval('sq')").unwrap().rows[0].values,
        [Value::Int(1)]
    );
    drop(a);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn sequence_slot_refill_preserves_durable_counter() {
    let path = setup();
    let a = Admin::open_as_fence_holder(&path, Limits::default(), b"holder").unwrap();
    sequence(&a, "SELECT setval('sq',99,false)").unwrap();
    drop(a);
    let a = Admin::open(&path, Limits::default()).unwrap();
    let before = rows(&a);
    // A reopened slot starts empty, so nextval must refill its reservation.
    // Repeating the refusal also proves the failed refill did not publish a slot.
    for sql in [
        "SELECT nextval('sq')",
        "SELECT nextval('sq')",
        "SELECT setval('sq',500)",
    ] {
        refused(sequence(&a, sql).unwrap_err());
        assert_eq!(rows(&a), before);
    }
    a.release_write_fence(b"holder", 1).unwrap();
    drop(a);
    let a = Admin::open(&path, Limits::default()).unwrap();
    assert_eq!(
        sequence(&a, "SELECT nextval('sq')").unwrap().rows[0].values,
        [Value::Int(99)]
    );
    assert_eq!(rows(&a), before);
    drop(a);
    std::fs::remove_dir_all(path).unwrap();
}
#[test]
fn holder_performs_all_write_paths() {
    let path = setup();
    let a = Admin::open_as_fence_holder(&path, Limits::default(), b"holder").unwrap();
    for sql in &WRITES[..3] {
        assert_eq!(a.sql_session().execute(sql, &[], None).unwrap().affected, 1);
    }
    // Restore id=1 after SQL DELETE so each native mutation matches a row.
    a.sql_session()
        .execute("INSERT INTO items VALUES (1,10)", &[], None)
        .unwrap();
    let e = giggles_core::operations::Engine::new(a.clone());
    for request in native_writes() {
        assert_eq!(e.execute(&request, &Bindings::new()).unwrap().affected, 1);
    }
    assert_eq!(
        rows(&a),
        vec![
            vec![Value::Int(2), Value::Int(20)],
            vec![Value::Int(3), Value::Int(30)]
        ]
    );
    assert_eq!(copy(&a).unwrap().affected, 1);
    assert_eq!(rows(&a).len(), 3);
    for (sql, value) in [
        ("SELECT nextval('sq')", 1),
        ("SELECT setval('sq',99)", 99),
        ("SELECT nextval('sq')", 100),
    ] {
        assert_eq!(
            sequence(&a, sql).unwrap().rows[0].values,
            [Value::Int(value)]
        );
    }
    for sql in &WRITES[3..] {
        a.sql_session().execute(sql, &[], None).unwrap();
    }
    assert!(a.write_fence_status().unwrap().unwrap().fenced);
    drop(e);
    drop(a);
    std::fs::remove_dir_all(path).unwrap();
}
#[test]
fn release_reinstall_advances_epoch_and_rejects_stale_credentials() {
    let path = setup();
    let a = Admin::open(&path, Limits::default()).unwrap();
    a.release_write_fence(b"holder", 1).unwrap();
    let status = a.write_fence_status().unwrap().unwrap();
    assert!(!status.fenced);
    assert_eq!(status.epoch, 1);
    a.sql_session()
        .execute("INSERT INTO items VALUES (2,20)", &[], None)
        .unwrap();
    assert_eq!(a.install_write_fence(b"next", "second").unwrap(), 2);
    for (token, epoch) in [(b"holder".as_slice(), 1), (b"next".as_slice(), 1)] {
        assert_eq!(
            a.release_write_fence(token, epoch),
            Err(CoreError::WriteFenced { epoch: 2 })
        );
    }
    let status = a.write_fence_status().unwrap().unwrap();
    assert!(status.fenced);
    assert_eq!((status.epoch, status.label.as_str()), (2, "second"));
    a.release_write_fence(b"next", 2).unwrap();
    drop(a);
    let a = Admin::open(&path, Limits::default()).unwrap();
    a.sql_session()
        .execute("INSERT INTO items VALUES (3,30)", &[], None)
        .unwrap();
    assert_eq!(rows(&a).len(), 3);
    drop(a);
    std::fs::remove_dir_all(path).unwrap();
}
