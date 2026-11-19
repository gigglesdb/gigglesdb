//! Defaults must cache syntax without retaining a catalog, type or evaluated value.
use giggles_core::{
    Admin, CoreError, Limits, SqlSession,
    relational::{SqlError, SqlResult, StatementClock, Value},
};
use giggles_pg_types::temporal::SessionZone;

struct Db {
    admin: Option<Admin>,
    path: std::path::PathBuf,
}
impl Db {
    fn new() -> Self {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path =
            std::env::temp_dir().join(format!("default-cache-{:032x}", u128::from_le_bytes(nonce)));
        Self {
            admin: Some(Admin::create(&path, Limits::default()).unwrap()),
            path,
        }
    }
    fn session(&self) -> SqlSession {
        self.admin.as_ref().unwrap().sql_session()
    }
}
impl Drop for Db {
    fn drop(&mut self) {
        drop(self.admin.take());
        std::fs::remove_dir_all(&self.path).unwrap();
    }
}
fn clock(us: i64) -> StatementClock {
    StatementClock {
        unavailable: None,
        transaction_us: us,
        statement_us: us,
        zone: Some(SessionZone::Fixed(0)),
        wall: None,
    }
}
fn run(s: &mut SqlSession, sql: &str) -> SqlResult {
    s.execute_at(sql, &[], None, &clock(100))
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
}
fn inserted(s: &mut SqlSession, table: &str) -> Vec<Value> {
    run(
        s,
        &format!("INSERT INTO {table} DEFAULT VALUES RETURNING *"),
    )
    .rows[0]
        .values
        .clone()
}
#[test]
fn set_and_drop_default() {
    let db = Db::new();
    let mut s = db.session();
    run(&mut s, "CREATE TABLE t(v int DEFAULT 1)");
    assert_eq!(inserted(&mut s, "t"), vec![Value::Int(1)]);
    run(&mut s, "ALTER TABLE t ALTER COLUMN v SET DEFAULT 2");
    assert_eq!(inserted(&mut s, "t"), vec![Value::Int(2)]);
    run(&mut s, "ALTER TABLE t ALTER COLUMN v DROP DEFAULT");
    assert_eq!(inserted(&mut s, "t"), vec![Value::Null]);
}
#[test]
fn sequence_advances_and_ddl_remains_live() {
    let db = Db::new();
    let mut s = db.session();
    run(&mut s, "CREATE SEQUENCE seq");
    run(&mut s, "CREATE TABLE t(v int DEFAULT nextval('seq'))");
    assert_eq!(inserted(&mut s, "t"), vec![Value::Int(1)]);
    assert_eq!(inserted(&mut s, "t"), vec![Value::Int(2)]);
    assert_eq!(
        s.execute_at(
            "ALTER SEQUENCE seq RENAME TO renamed",
            &[],
            None,
            &clock(100)
        ),
        Err(CoreError::Sql(SqlError::Unsupported))
    );
    assert_eq!(inserted(&mut s, "t"), vec![Value::Int(3)]);
    assert_eq!(
        s.execute_at("DROP SEQUENCE seq", &[], None, &clock(100)),
        Err(CoreError::Sql(SqlError::DependentObjects))
    );
    assert_eq!(inserted(&mut s, "t"), vec![Value::Int(4)]);
    run(&mut s, "DROP SEQUENCE seq CASCADE");
    assert_eq!(inserted(&mut s, "t"), vec![Value::Null]);
    run(&mut s, "CREATE SEQUENCE seq START WITH 40");
    run(
        &mut s,
        "ALTER TABLE t ALTER COLUMN v SET DEFAULT nextval('seq')",
    );
    assert_eq!(inserted(&mut s, "t"), vec![Value::Int(40)]);
}
#[test]
fn clock_default_is_evaluated_per_statement() {
    let db = Db::new();
    let mut s = db.session();
    run(
        &mut s,
        "CREATE TABLE t(v timestamptz DEFAULT statement_timestamp())",
    );
    let sql = "INSERT INTO t DEFAULT VALUES RETURNING v";
    let first = s.execute_at(sql, &[], None, &clock(100)).unwrap().rows[0]
        .values
        .clone();
    let second = s.execute_at(sql, &[], None, &clock(200)).unwrap().rows[0]
        .values
        .clone();
    assert_ne!(first, second);
    let third = s.execute_at(sql, &[], None, &clock(100)).unwrap().rows[0]
        .values
        .clone();
    assert_eq!(first, third);
}
#[test]
fn alter_type_rebinds_existing_default() {
    let db = Db::new();
    let mut s = db.session();
    run(&mut s, "CREATE TABLE t(v int DEFAULT '1')");
    assert_eq!(inserted(&mut s, "t"), vec![Value::Int(1)]);
    run(&mut s, "ALTER TABLE t ALTER COLUMN v TYPE text");
    assert_eq!(inserted(&mut s, "t"), vec![Value::Text("1".into())]);
}
#[test]
fn sessions_interleave_catalog_versions() {
    let db = Db::new();
    let mut a = db.session();
    let mut b = db.session();
    run(&mut a, "CREATE TABLE t(v int DEFAULT 1)");
    assert_eq!(inserted(&mut a, "t"), vec![Value::Int(1)]);
    assert_eq!(inserted(&mut b, "t"), vec![Value::Int(1)]);
    run(&mut b, "ALTER TABLE t ALTER COLUMN v SET DEFAULT 2");
    assert_eq!(inserted(&mut a, "t"), vec![Value::Int(2)]);
    assert_eq!(inserted(&mut b, "t"), vec![Value::Int(2)]);
    run(&mut a, "ALTER TABLE t ALTER COLUMN v SET DEFAULT 1");
    assert_eq!(inserted(&mut b, "t"), vec![Value::Int(1)]);
    assert_eq!(inserted(&mut a, "t"), vec![Value::Int(1)]);
}
#[test]
fn same_text_is_assigned_per_destination_type() {
    let db = Db::new();
    let mut s = db.session();
    run(
        &mut s,
        "CREATE TABLE t(i int DEFAULT '1', s text DEFAULT '1')",
    );
    for _ in 0..3 {
        assert_eq!(
            inserted(&mut s, "t"),
            vec![Value::Int(1), Value::Text("1".into())]
        );
    }
}

#[test]
fn volatile_clock_is_evaluated_per_row_and_statement() {
    use std::sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    };
    let db = Db::new();
    let mut s = db.session();
    run(
        &mut s,
        "CREATE TABLE t(id int, v timestamptz DEFAULT clock_timestamp())",
    );
    let calls = Arc::new(AtomicI64::new(100));
    let mut execution_clock = clock(100);
    let wall_calls = calls.clone();
    execution_clock.wall = Some(Arc::new(move || {
        Ok(wall_calls.fetch_add(1, Ordering::SeqCst))
    }));
    for sql in [
        "INSERT INTO t(id) VALUES (1),(2) RETURNING v",
        "INSERT INTO t(id) VALUES (3),(4) RETURNING v",
    ] {
        let rows = s.execute_at(sql, &[], None, &execution_clock).unwrap().rows;
        assert_eq!(rows.len(), 2);
        assert_ne!(rows[0].values, rows[1].values);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 104);
}

#[test]
fn old_transaction_keeps_its_catalog_default() {
    let db = Db::new();
    let mut old = db.session();
    let mut current = db.session();
    run(&mut current, "CREATE TABLE t(v int DEFAULT 1)");
    assert_eq!(inserted(&mut current, "t"), vec![Value::Int(1)]);
    run(&mut old, "BEGIN");
    assert_eq!(inserted(&mut old, "t"), vec![Value::Int(1)]);
    run(&mut current, "ALTER TABLE t ALTER COLUMN v SET DEFAULT 2");
    assert_eq!(inserted(&mut current, "t"), vec![Value::Int(2)]);
    assert_eq!(inserted(&mut old, "t"), vec![Value::Int(1)]);
    run(&mut old, "ROLLBACK");
    assert_eq!(inserted(&mut old, "t"), vec![Value::Int(2)]);
}

#[test]
fn identical_null_default_text_is_assigned_per_destination_type() {
    let db = Db::new();
    let mut s = db.session();
    run(
        &mut s,
        "CREATE TABLE t(i int DEFAULT NULL, s text DEFAULT NULL, n numeric DEFAULT NULL)",
    );
    for _ in 0..3 {
        assert_eq!(
            inserted(&mut s, "t"),
            vec![Value::Null, Value::Null, Value::Null]
        );
    }
    let typed = run(
        &mut s,
        "INSERT INTO t DEFAULT VALUES RETURNING pg_typeof(i)::text, pg_typeof(s)::text, pg_typeof(n)::text",
    );
    assert_eq!(
        typed.rows[0].values,
        vec![
            Value::Text("integer".into()),
            Value::Text("text".into()),
            Value::Text("numeric".into())
        ]
    );
}
