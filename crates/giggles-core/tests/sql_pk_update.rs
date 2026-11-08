use giggles_core::{
    Admin, CoreError, Limits, SqlSession,
    relational::{SqlError as E, SqlResult, Value as V},
};
use std::time::{SystemTime, UNIX_EPOCH};
fn run(s: &mut SqlSession, sql: &str) -> SqlResult {
    s.execute_at(
        sql,
        &[],
        None,
        &giggles_core::relational::StatementClock {
            unavailable: None,
            transaction_us: 100,
            statement_us: 100,
            zone: None,
            wall: None,
        },
    )
    .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
}
fn values(s: &mut SqlSession, sql: &str) -> Vec<Vec<V>> {
    run(s, sql).rows.into_iter().map(|r| r.values).collect()
}
fn err(s: &mut SqlSession, sql: &str, e: E) {
    assert_eq!(s.execute(sql, &[], None), Err(CoreError::Sql(e)), "{sql}");
}
#[test]
fn pg186_primary_key_updates_are_immediate_atomic_and_durable() {
    let path = std::env::temp_dir().join(format!(
        "sql-pk-update-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    run(&mut s, "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT)");
    run(&mut s, "INSERT INTO t VALUES(1,'a'),(2,'b'),(3,'c')");
    assert_eq!(run(&mut s, "UPDATE t SET id=id+10").affected, 3);
    let before = values(&mut s, "SELECT * FROM t ORDER BY id");
    err(&mut s, "UPDATE t SET id=id+1", E::Duplicate);
    assert_eq!(values(&mut s, "SELECT * FROM t ORDER BY id"), before);
    err(&mut s, "UPDATE t SET id=99", E::Duplicate);
    assert_eq!(values(&mut s, "SELECT * FROM t ORDER BY id"), before);
    let moved = run(&mut s, "UPDATE t SET id=id-1 RETURNING id,v");
    assert_eq!(
        moved
            .rows
            .iter()
            .map(|r| r.values[0].clone())
            .collect::<Vec<_>>(),
        vec![V::Int(10), V::Int(11), V::Int(12)]
    );
    err(&mut s, "UPDATE t SET id=12 WHERE id=11", E::Duplicate);
    err(&mut s, "UPDATE t SET id=NULL WHERE id=11", E::Null);
    assert_eq!(run(&mut s, "UPDATE t SET id=id").affected, 3);
    assert_eq!(
        run(&mut s, "UPDATE t SET id=100 WHERE id=11 RETURNING id,v").rows[0].values,
        vec![V::Int(100), V::Text("b".into())]
    );
    run(
        &mut s,
        "CREATE TABLE k(a INTEGER,b INTEGER,v TEXT,PRIMARY KEY(a,b))",
    );
    run(&mut s, "INSERT INTO k VALUES(1,1,'x'),(1,2,'y')");
    err(&mut s, "UPDATE k SET b=b+1 WHERE a=1", E::Duplicate);
    run(&mut s, "UPDATE k SET b=5 WHERE b=1");
    assert_eq!(
        values(&mut s, "SELECT b FROM k ORDER BY b"),
        vec![vec![V::Int(2)], vec![V::Int(5)]]
    );
    run(&mut s, "CREATE TABLE s(id SERIAL PRIMARY KEY,v TEXT)");
    run(&mut s, "INSERT INTO s(v) VALUES('p'),('q')");
    run(&mut s, "UPDATE s SET id=10 WHERE v='p'");
    run(&mut s, "INSERT INTO s(v) VALUES('r')");
    assert_eq!(
        values(&mut s, "SELECT id FROM s ORDER BY id"),
        vec![vec![V::Int(2)], vec![V::Int(3)], vec![V::Int(10)]]
    );
    run(
        &mut s,
        "CREATE TABLE u(id INTEGER PRIMARY KEY,e TEXT UNIQUE)",
    );
    run(&mut s, "CREATE INDEX u_e ON u(e)");
    run(&mut s, "INSERT INTO u VALUES(1,'x'),(2,'y')");
    run(&mut s, "UPDATE u SET id=3,e='z' WHERE id=1");
    run(
        &mut s,
        "INSERT INTO u VALUES(1,'x') ON CONFLICT(id) DO UPDATE SET id=9",
    );
    assert_eq!(
        run(
            &mut s,
            "INSERT INTO u VALUES(2,'ignored') ON CONFLICT(id) DO UPDATE SET id=9 RETURNING id,e"
        )
        .rows[0]
            .values,
        vec![V::Int(9), V::Text("y".into())]
    );
    run(&mut s, "BEGIN");
    run(&mut s, "UPDATE u SET id=50 WHERE id=9");
    run(&mut s, "ROLLBACK");
    run(&mut s, "BEGIN");
    run(&mut s, "SAVEPOINT p");
    run(&mut s, "UPDATE u SET id=50 WHERE id=9");
    run(&mut s, "ROLLBACK TO p");
    run(&mut s, "COMMIT");
    assert_eq!(
        values(&mut s, "SELECT id FROM u WHERE e='y'"),
        vec![vec![V::Int(9)]]
    );
    run(&mut s, "CREATE TABLE parent(id INTEGER PRIMARY KEY)");
    run(&mut s, "INSERT INTO parent VALUES(1)");
    run(
        &mut s,
        "CREATE TABLE child(id INTEGER PRIMARY KEY,p INTEGER REFERENCES parent(id))",
    );
    run(&mut s, "INSERT INTO child VALUES(1,1)");
    run(&mut s, "UPDATE child SET id=id+10");
    assert_eq!(
        values(&mut s, "SELECT id FROM child"),
        vec![vec![V::Int(11)]]
    );
    run(&mut s, "CREATE TABLE free(id INTEGER PRIMARY KEY)");
    run(&mut s, "INSERT INTO free VALUES(1),(2)");
    run(&mut s, "CREATE TABLE referenced(id INTEGER PRIMARY KEY)");
    run(&mut s, "INSERT INTO referenced VALUES(1),(2)");
    run(
        &mut s,
        "CREATE TABLE refs(id INTEGER PRIMARY KEY,p INTEGER REFERENCES referenced(id) ON UPDATE CASCADE)",
    );
    run(&mut s, "INSERT INTO refs VALUES(1,1),(2,2)");
    for table in ["free", "referenced"] {
        run(&mut s, &format!("UPDATE {table} SET id=id+10"));
        err(&mut s, &format!("UPDATE {table} SET id=id+1"), E::Duplicate);
    }
    assert_eq!(
        values(&mut s, "SELECT id FROM free ORDER BY id"),
        values(&mut s, "SELECT id FROM referenced ORDER BY id")
    );
    assert_eq!(
        values(&mut s, "SELECT p FROM refs ORDER BY p"),
        vec![vec![V::Int(11)], vec![V::Int(12)]]
    );
    drop(s);
    drop(admin);
    let admin = Admin::open(&path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    assert_eq!(
        values(&mut s, "SELECT id FROM u WHERE e='y'"),
        vec![vec![V::Int(9)]]
    );
    assert!(values(&mut s, "SELECT id FROM u WHERE id=2").is_empty());
    drop(s);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
