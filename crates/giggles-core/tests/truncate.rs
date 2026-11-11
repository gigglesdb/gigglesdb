mod common;
use giggles_core::relational::{SqlError, StatementClock, Value};
use giggles_core::{Admin, CoreError, Engine, Limits, SqlSession};

fn run(s: &mut SqlSession, sql: &str) {
    s.execute(sql, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
}
fn count(s: &mut SqlSession, table: &str) -> i64 {
    let result = s
        .execute(&format!("SELECT count(*) FROM {table}"), &[], None)
        .unwrap();
    let Value::Int(n) = result.rows[0].values[0] else {
        panic!("count")
    };
    n
}
fn exercise(engine: Engine) {
    let path = common::fresh("truncate");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    let mut s = admin.sql_session();
    run(
        &mut s,
        "CREATE TABLE t(id integer PRIMARY KEY, v integer UNIQUE)",
    );
    for sql in [
        "TRUNCATE t",
        "TRUNCATE TABLE t",
        "TRUNCATE ONLY t",
        "TRUNCATE TABLE ONLY t *",
        "TRUNCATE t,t CONTINUE IDENTITY RESTRICT",
    ] {
        run(&mut s, "INSERT INTO t VALUES(1,2)");
        let prepared = s.prepare(sql).unwrap();
        assert_eq!(prepared.command(), "TRUNCATE TABLE");
        assert!(s.describe(&prepared).unwrap().columns.is_empty());
        s.execute_prepared(&prepared, &[], None).unwrap();
        assert_eq!(count(&mut s, "t"), 0);
    }
    run(&mut s, "CREATE INDEX t_v ON t(v)");
    run(&mut s, "INSERT INTO t VALUES(1,2)");
    assert_eq!(
        s.execute("TRUNCATE t,missing", &[], None),
        Err(CoreError::Sql(SqlError::UnknownTable))
    );
    assert_eq!(count(&mut s, "t"), 1);
    run(&mut s, "BEGIN");
    run(&mut s, "TRUNCATE t");
    assert_eq!(count(&mut s, "t"), 0);
    let mut other = admin.sql_session();
    run(&mut s, "ROLLBACK");
    assert_eq!(count(&mut s, "t"), 1);
    run(&mut s, "BEGIN");
    run(&mut s, "SAVEPOINT before_clear");
    run(&mut s, "TRUNCATE t");
    run(&mut s, "ROLLBACK TO SAVEPOINT before_clear");
    assert_eq!(count(&mut s, "t"), 1);
    run(&mut s, "TRUNCATE t");
    run(&mut s, "COMMIT");
    assert_eq!(count(&mut other, "t"), 0);
    run(&mut s, "INSERT INTO t VALUES(1,2)");
    assert_eq!(count(&mut s, "t"), 1);

    run(&mut s, "CREATE SCHEMA app");
    run(
        &mut s,
        "CREATE TABLE app.\"Quoted Table\"(id integer PRIMARY KEY)",
    );
    run(&mut s, "INSERT INTO app.\"Quoted Table\" VALUES(1)");
    run(&mut s, "TRUNCATE TABLE app.\"Quoted Table\"");
    s.set_search_path("app,public", "giggles");
    run(&mut s, "INSERT INTO \"Quoted Table\" VALUES(1)");
    run(&mut s, "TRUNCATE \"Quoted Table\", public.t");
    assert_eq!(count(&mut s, "app.\"Quoted Table\""), 0);
    s.set_search_path("public", "giggles");

    run(&mut s, "CREATE TABLE parent(id integer PRIMARY KEY)");
    run(
        &mut s,
        "CREATE TABLE child(id integer PRIMARY KEY,p integer REFERENCES parent(id))",
    );
    run(
        &mut s,
        "CREATE TABLE grandchild(id integer PRIMARY KEY,c integer REFERENCES child(id))",
    );
    run(&mut s, "INSERT INTO parent VALUES(1)");
    run(&mut s, "INSERT INTO child VALUES(2,1)");
    run(&mut s, "INSERT INTO grandchild VALUES(3,2)");
    let prepared = s.prepare("TRUNCATE parent").unwrap();
    assert!(s.describe(&prepared).unwrap().columns.is_empty());
    assert_eq!(
        s.execute("TRUNCATE parent", &[], None),
        Err(CoreError::Sql(SqlError::Unsupported))
    );
    assert_eq!(count(&mut s, "parent"), 1);
    run(&mut s, "TRUNCATE parent CASCADE");
    for table in ["parent", "child", "grandchild"] {
        assert_eq!(count(&mut s, table), 0);
    }
    run(&mut s, "TRUNCATE parent,child,grandchild RESTRICT");
    run(
        &mut s,
        "CREATE TABLE self_ref(id integer PRIMARY KEY,p integer REFERENCES self_ref(id))",
    );
    run(&mut s, "INSERT INTO self_ref VALUES(1,1)");
    run(&mut s, "TRUNCATE self_ref");
    assert_eq!(count(&mut s, "self_ref"), 0);

    for (table, definition) in [
        ("serial_t", "id serial PRIMARY KEY"),
        ("bigserial_t", "id bigserial PRIMARY KEY"),
        (
            "identity_t",
            "id bigint GENERATED ALWAYS AS IDENTITY (START WITH 7) PRIMARY KEY",
        ),
    ] {
        run(&mut s, &format!("CREATE TABLE {table}({definition})"));
        let initial = if table == "identity_t" { 7 } else { 1 };
        let next = |s: &mut SqlSession| {
            let result = s
                .execute_at(
                    &format!("INSERT INTO {table} DEFAULT VALUES RETURNING id"),
                    &[],
                    None,
                    &StatementClock {
                        unavailable: None,
                        transaction_us: 100,
                        statement_us: 100,
                        zone: None,
                        wall: None,
                    },
                )
                .unwrap();
            result.rows[0].values[0].clone()
        };
        assert_eq!(next(&mut s), Value::Int(initial));
        run(&mut s, &format!("TRUNCATE {table} CONTINUE IDENTITY"));
        assert_eq!(next(&mut s), Value::Int(initial + 1));
        run(&mut s, "BEGIN");
        run(&mut s, "SAVEPOINT seq_before");
        run(&mut s, &format!("TRUNCATE {table} RESTART IDENTITY"));
        assert_eq!(next(&mut s), Value::Int(initial));
        run(&mut s, "ROLLBACK TO seq_before");
        assert_eq!(next(&mut s), Value::Int(initial + 2));
        run(&mut s, "ROLLBACK");
        run(&mut s, &format!("TRUNCATE {table} RESTART IDENTITY"));
        assert_eq!(next(&mut s), Value::Int(initial));
    }
    assert_eq!(
        s.execute("TRUNCATE missing", &[], None),
        Err(CoreError::Sql(SqlError::UnknownTable))
    );
    run(&mut s, "CREATE SEQUENCE seq");
    assert_eq!(
        s.execute("TRUNCATE seq", &[], None),
        Err(CoreError::Sql(SqlError::WrongObjectType))
    );
    // No views exist in the catalog; sequences exercise the non-table relation path.
    run(&mut s, "TRUNCATE t");
    drop(other);
    drop(s);
    drop(admin);
    let admin = Admin::open(&path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    assert_eq!(count(&mut s, "t"), 0);
}
#[test]
fn legacy() {
    exercise(Engine::Legacy);
}
#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2() {
    exercise(Engine::V2);
}

fn large(engine: Engine) {
    let path = common::fresh("truncate-large");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    let mut s = admin.sql_session();
    run(
        &mut s,
        "CREATE TABLE large_t(id integer PRIMARY KEY,v integer)",
    );
    run(&mut s, "BEGIN");
    for page in 0..100 {
        let values = (0..200)
            .map(|n| {
                let n = page * 200 + n;
                format!("({n},{n})")
            })
            .collect::<Vec<_>>()
            .join(",");
        run(&mut s, &format!("INSERT INTO large_t VALUES {values}"));
    }
    run(&mut s, "COMMIT");
    assert_eq!(
        s.execute("SELECT id FROM large_t WHERE id=19999", &[], None)
            .unwrap()
            .rows[0]
            .values[0],
        Value::Int(19999)
    );
    run(&mut s, "TRUNCATE large_t");
    assert_eq!(count(&mut s, "large_t"), 0);
    run(&mut s, "INSERT INTO large_t VALUES(1,1)");
}
#[test]
fn legacy_large() {
    large(Engine::Legacy);
}
#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_large() {
    large(Engine::V2);
}
