#![cfg(feature = "engine-rocksdb")]
use giggles_core::{*, relational::{SqlResult, Value as V}};
fn run(s: &mut SqlSession, sql: &str) -> SqlResult {
    s.execute(sql, &[], None).unwrap_or_else(|e| panic!("{sql}: {e:?}"))
}
fn count(s: &mut SqlSession, term: &str, expected: i64) {
    assert_eq!(run(s, &format!("SELECT count(*) FROM users u WHERE {term}")).rows[0].values,
        vec![V::Int(expected)], "{term}");
}
#[test]
fn indexed_correlation_residuals_prefixes_nulls_and_overlay() {
    let path = std::env::temp_dir().join(format!("c9-probes-{}", std::process::id()));
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let mut s = admin.sql_session();
    for sql in ["CREATE TABLE users(id int)", "INSERT INTO users VALUES(1),(2),(3),(NULL)",
        "CREATE TABLE orders(id int PRIMARY KEY,user_id int,v int)",
        "CREATE INDEX orders_user ON orders(user_id,v)",
        "INSERT INTO orders VALUES(1,1,0),(2,1,10),(3,2,20),(4,NULL,99)",
        "CREATE TABLE prefix(k int,n int,v int,PRIMARY KEY(k,n))",
        "INSERT INTO prefix VALUES(1,1,10),(1,2,0),(2,1,20)",
        "CREATE TABLE heap(k int)", "INSERT INTO heap VALUES(1),(2)"] { run(&mut s, sql); }
    for (term, expected) in [
        ("EXISTS(SELECT 1 FROM orders o WHERE o.user_id=u.id)", 2),
        ("NOT EXISTS(SELECT 1 FROM orders o WHERE o.user_id=u.id)", 2),
        ("EXISTS(SELECT 1 FROM orders o WHERE o.user_id=u.id AND v>15)", 1),
        ("EXISTS(SELECT 1 FROM orders o WHERE u.id=o.user_id AND v=10)", 1),
        ("EXISTS(SELECT 1 FROM orders o WHERE o.id=u.id)", 3),
        ("EXISTS(SELECT 1 FROM prefix p WHERE p.k=u.id AND p.v>15)", 1),
        ("EXISTS(SELECT 1 FROM prefix p WHERE p.k=u.id AND p.n=2)", 1),
        ("EXISTS(SELECT count(*) FROM orders o WHERE o.user_id=u.id GROUP BY user_id HAVING count(*)>1)", 1),
        ("EXISTS(SELECT 1 FROM heap h WHERE h.k=u.id)", 2),
        ("EXISTS(SELECT 1 FROM orders o WHERE o.user_id=u.id OR o.v=99)", 4),
        ("EXISTS(SELECT 1 FROM orders o WHERE o.user_id=u.id AND EXISTS(SELECT 1 FROM prefix p WHERE p.k=o.user_id AND p.k=u.id AND p.n=2))", 1),
        ("EXISTS(SELECT 1 FROM orders o WHERE o.user_id=u.id AND o.user_id=3)", 0),
    ] { count(&mut s, term, expected); }
    let prepared = s.prepare("SELECT count(*) FROM users u WHERE EXISTS(SELECT 1 FROM prefix p WHERE p.k=u.id AND p.n=$1)").unwrap();
    for (param, expected) in [(V::Int(1),2), (V::Int(2),1), (V::Null,0)] {
        assert_eq!(s.execute_prepared(&prepared, &[param], None).unwrap().rows[0].values, vec![V::Int(expected)]);
    }
    run(&mut s, "BEGIN");
    run(&mut s, "INSERT INTO orders VALUES(5,3,30)");
    count(&mut s, "EXISTS(SELECT 1 FROM orders o WHERE o.user_id=u.id)", 3);
    run(&mut s, "UPDATE orders SET user_id=3 WHERE id=3");
    count(&mut s, "EXISTS(SELECT 1 FROM orders o WHERE o.user_id=u.id)", 2);
    run(&mut s, "DELETE FROM orders WHERE user_id=1");
    count(&mut s, "NOT EXISTS(SELECT 1 FROM orders o WHERE o.user_id=u.id)", 3);
    run(&mut s, "ROLLBACK");
    count(&mut s, "EXISTS(SELECT 1 FROM orders o WHERE o.user_id=u.id)", 2);
    drop(s); drop(admin); std::fs::remove_dir_all(path).unwrap();
}
fn scale(users: i64) {
    let path = std::env::temp_dir().join(format!("c9-scale-{users}-{}", std::process::id()));
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let mut s = admin.sql_session();
    for sql in ["CREATE TABLE users(id int PRIMARY KEY)",
        "CREATE TABLE orders(id int PRIMARY KEY,user_id int)",
        "CREATE INDEX orders_user ON orders(user_id)"] { run(&mut s, sql); }
    for (table, rows) in [("users", users), ("orders", users*4)] {
        for start in (1..=rows).step_by(100) {
            let values = (start..start+100).map(|id| if table == "users" { format!("({id})") }
                else { format!("({id},{})", (id-1)%users+1) }).collect::<Vec<_>>().join(",");
            run(&mut s, &format!("INSERT INTO {table} VALUES{values}"));
        }
    }
    let now = std::time::Instant::now();
    let result = run(&mut s, "SELECT u.id FROM users u WHERE EXISTS(SELECT 1 FROM orders o WHERE o.user_id=u.id)");
    let elapsed = now.elapsed();
    eprintln!("C9_SCALE users={users} orders={} elapsed={elapsed:?}", users*4);
    assert_eq!(result.rows.len(), users as usize);
    count(&mut s, "NOT EXISTS(SELECT 1 FROM orders o WHERE o.user_id=u.id)", 0);
    if users == 50_000 {
        run(&mut s, "DROP INDEX orders_user");
        let now = std::time::Instant::now();
        let result = s.execute("SELECT count(*) FROM users u WHERE EXISTS(SELECT 1 FROM orders o WHERE o.user_id=u.id)", &[], None);
        eprintln!("C9_FALLBACK elapsed={:?} result={result:?}", now.elapsed());
        assert_eq!(result, Err(CoreError::Sql(relational::SqlError::Limit)));
    }
    drop(s); drop(admin); std::fs::remove_dir_all(path).unwrap();
}
#[test]
fn indexed_correlation_5k_20k() { scale(5_000); }
#[test]
#[ignore = "full audit scale timing qualification"]
fn indexed_correlation_50k_200k() { scale(50_000); }
