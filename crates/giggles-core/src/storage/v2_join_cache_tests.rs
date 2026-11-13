//! Point join cache equivalence.
use super::*;
thread_local! {
    pub(crate) static DISABLED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    pub(crate) static GETS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
fn run(state: &mut Relational, sql: &str) -> SqlResult {
    let p = Prepared::parse(sql, 1, state.epoch).unwrap();
    state.execute(&p, &[], None).unwrap()
}
#[test]
fn cached_joins_preserve_rows() {
    let mut state = Relational::default();
    for sql in [
        "CREATE TABLE orders(id bigint PRIMARY KEY)",
        "CREATE TABLE products(id bigint PRIMARY KEY,price bigint)",
        "CREATE TABLE items(id bigint PRIMARY KEY,oid bigint,pid bigint,qty bigint)",
        "INSERT INTO orders VALUES(1),(2)",
        "INSERT INTO products VALUES(1,10),(2,20)",
        "INSERT INTO items VALUES(1,1,1,2),(2,1,1,3),(3,2,99,1),(4,2,99,1),(5,2,NULL,1),(6,2,2,1)",
    ] { run(&mut state, sql); }
    for sql in [
        "SELECT o.id,sum(i.qty*p.price) FROM items i JOIN orders o ON o.id=i.oid JOIN products p ON p.id=i.pid GROUP BY o.id",
        "SELECT i.id,p.price FROM items i LEFT JOIN products p ON p.id=i.pid",
    ] {
        DISABLED.set(false); GETS.set(0);
        let cached = run(&mut state, sql).rows;
        let cached_gets = GETS.get();
        DISABLED.set(true); GETS.set(0);
        let uncached = run(&mut state, sql).rows;
        let uncached_gets = GETS.get();
        DISABLED.set(false);
        assert_eq!(cached, uncached);
        assert!(cached_gets < uncached_gets, "{cached_gets} vs {uncached_gets}");
    }
}

#[test]
#[cfg(feature = "engine-rocksdb")]
fn v2_transaction_own_writes_are_visible() {
    use crate::storage::{Admin, Engine, Limits};
    let path = std::env::temp_dir().join(format!("join-cache-{}", std::process::id()));
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let mut session = admin.sql_session();
    for sql in ["CREATE TABLE p(id bigint PRIMARY KEY,v bigint)", "CREATE TABLE t(id bigint PRIMARY KEY,p bigint)", "BEGIN", "INSERT INTO p VALUES(1,10)", "INSERT INTO t VALUES(1,1),(2,1),(3,99),(4,99),(5,NULL)"] {
        session.execute(sql, &[], None).unwrap();
    }
    let sql = "SELECT t.id,p.v FROM t LEFT JOIN p ON p.id=t.p";
    DISABLED.set(false); GETS.set(0);
    let cached = session.execute(sql, &[], None).unwrap().rows;
    assert_eq!(GETS.get(), 2);
    DISABLED.set(true); GETS.set(0);
    let plain = session.execute(sql, &[], None).unwrap().rows;
    DISABLED.set(false);
    assert_eq!(GETS.get(), 4);
    assert_eq!(cached, plain);
    assert_eq!(cached.len(), 5);
    session.execute("ROLLBACK", &[], None).unwrap();
    drop(session); drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
