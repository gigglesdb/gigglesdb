use super::*;
use relational::Value::{Int, Null};
use std::time::Instant;

fn values(s: &mut SqlSession, sql: &str) -> Vec<Vec<relational::Value>> {
    s.execute(sql, &[], None).unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .rows.into_iter().map(|r| r.values).collect()
}

fn audit(users: i64) {
    let f = Fixture::new();
    f.execute("CREATE TABLE users(id bigint PRIMARY KEY)");
    f.execute("CREATE TABLE orders(id bigint PRIMARY KEY, user_id bigint REFERENCES users(id), status bigint)");
    f.execute("CREATE TABLE order_items(order_id bigint REFERENCES orders(id), sku bigint, PRIMARY KEY(order_id,sku))");
    let parts = f.parts();
    for (name, count) in [("users", users), ("orders", users * 4), ("order_items", users * 12)] {
        let entry = TableEntry { primary_index_birth: None, def: parts.tables[name].clone(), heap: false, columns_ext: BTreeMap::new() };
        for start in (0..count).step_by(1000) {
            let mut txn = f.db().begin(Isolation::ReadCommitted);
            for id in start..(start + 1000).min(count) {
                let row = match name {
                    "users" => vec![Int(id)],
                    "orders" => vec![Int(id), Int(id % users), Int(id % 4)],
                    _ => vec![Int(id / 3), Int(id % 3)],
                };
                f.db().rows.insert(&mut txn, &entry, row, None, &|| false).unwrap();
            }
            txn.commit().unwrap();
        }
    }
    f.execute("CREATE INDEX orders_user ON orders(user_id)");
    let mut s = f.admin().sql_session();
    let selected = users / 5;
    let started = Instant::now();
    assert_eq!(values(&mut s, &format!("SELECT u.id,o.id,i.sku FROM users u JOIN orders o ON o.user_id=u.id JOIN order_items i ON i.order_id=o.id WHERE u.id={selected} ORDER BY o.id,i.sku")),
        (0..4).flat_map(|n| (0..3).map(move |sku| vec![Int(selected), Int(selected + n * users), Int(sku)])).collect::<Vec<_>>());
    eprintln!("audit {users}/{} /{} filtered {:?}", users * 4, users * 12, started.elapsed());
    let started = Instant::now();
    assert_eq!(values(&mut s, "SELECT o.status,count(*) FROM users u JOIN orders o ON o.user_id=u.id JOIN order_items i ON i.order_id=o.id GROUP BY o.status ORDER BY o.status"),
        (0..4).map(|status| vec![Int(status), Int(users * 3)]).collect::<Vec<_>>());
    eprintln!("audit {users} count GROUP BY {:?}", started.elapsed());
    assert_eq!(values(&mut s, "SELECT count(*) FROM orders o JOIN order_items i ON i.order_id=o.id WHERE o.user_id=1000"), vec![vec![Int(12)]]);
    f.execute("CREATE INDEX orders_status ON orders(status)");
    assert_eq!(values(&mut s, "SELECT count(*) FROM users u JOIN orders o ON o.status=u.id WHERE u.id=0"), vec![vec![Int(users)]]);
}

#[test]
fn audit_scale() { audit(5000); }

#[test]
#[ignore = "full audit performance qualification"]
fn full_audit_scale() { audit(50000); }

#[test]
fn parity_nulls_residuals_and_own_writes() {
    let f = Fixture::new();
    let path = f.path.with_extension("legacy");
    let legacy = Admin::create_engine(&path, Limits::default(), Engine::Legacy).unwrap();
    let mut old = legacy.sql_session();
    let mut new = f.admin().sql_session();
    for sql in [
        "CREATE TABLE a(id bigint PRIMARY KEY, v bigint)",
        "CREATE TABLE b(id bigint PRIMARY KEY, v bigint, extra bigint)",
        "CREATE INDEX bv ON b(v,extra)",
        "INSERT INTO a VALUES(1,10),(2,20),(3,NULL),(4,99)",
        "INSERT INTO b VALUES(1,10,8),(2,10,7),(3,NULL,6),(4,20,5)",
        "SELECT a.id,b.id FROM a LEFT JOIN b ON a.v=b.v AND b.extra>7 ORDER BY a.id,b.id",
        "SELECT a.id,b.id FROM a JOIN b ON b.extra=a.id ORDER BY a.id,b.id",
        "SELECT a.id,b.id FROM a JOIN b ON b.id=a.id ORDER BY a.id",
        "SELECT a.id,b.id FROM a LEFT JOIN b ON b.v=a.v WHERE b.id=1 ORDER BY a.id",
        "SELECT a.id,b.id FROM a JOIN b ON a.id=b.id OR a.v=b.v ORDER BY a.id,b.id",
        "SELECT DISTINCT a.id FROM a JOIN b ON a.v=b.v ORDER BY a.id",
        "SELECT a.id,b.id FROM a JOIN b ON a.v=b.v WHERE a.id IN(SELECT id FROM a WHERE id<3) ORDER BY a.id,b.id",
        "SELECT a.id,b.id FROM a LEFT JOIN b ON a.v=b.v AND b.extra=a.id ORDER BY a.id,b.id",
        "CREATE TABLE c(id bigint PRIMARY KEY, v numeric)",
        "CREATE INDEX cv ON c(v)",
        "INSERT INTO c VALUES(1,10),(2,10.5),(3,NULL),(4,999999999999999999999999)",
        "SELECT c.id,b.id FROM c LEFT JOIN b ON c.v=b.v ORDER BY c.id,b.id",
        "SELECT b.id,c.id FROM b LEFT JOIN c ON c.v=b.v ORDER BY b.id,c.id",
        "BEGIN",
        "INSERT INTO a VALUES(5,50)",
        "INSERT INTO b VALUES(5,50,9)",
        "UPDATE b SET v=20 WHERE id=1",
        "DELETE FROM b WHERE id=4",
        "SELECT a.id,b.id FROM a LEFT JOIN b ON a.v=b.v ORDER BY a.id,b.id",
        "SELECT a.id,b.id FROM a JOIN b ON a.id=b.id ORDER BY a.id",
        "ROLLBACK",
    ] {
        assert_eq!(new.execute(sql, &[], None), old.execute(sql, &[], None), "{sql}");
    }
    assert_eq!(values(&mut new, "SELECT a.id,b.id FROM a LEFT JOIN b ON a.v=b.v WHERE a.id=4"), vec![vec![Int(4), Null]]);
    // Separate legal outer rows may form a prefix larger than one legal row.
    for session in [&mut old, &mut new] {
        for sql in ["CREATE TABLE wide_a(id bigint PRIMARY KEY,v text)",
            "CREATE TABLE wide_b(id bigint PRIMARY KEY,v text)",
            "CREATE TABLE wide_inner(x text,y text,n bigint,PRIMARY KEY(x,y,n))"] {
            session.execute(sql, &[], None).unwrap();
        }
        let v = relational::Value::Text("v".repeat(relational::MAX_ROW_BYTES * 3 / 4));
        for sql in ["INSERT INTO wide_a VALUES(1,$1)", "INSERT INTO wide_b VALUES(1,$1)"] {
            session.execute(sql, std::slice::from_ref(&v), None).unwrap();
        }
    }
    for (kind, count) in [("JOIN", 0), ("LEFT JOIN", 1)] {
        let sql = format!("SELECT count(*) FROM wide_a a JOIN wide_b b ON a.id=b.id {kind} wide_inner p ON p.x=a.v AND p.y=b.v");
        let expected = vec![vec![Int(count)]];
        assert_eq!(values(&mut new, &sql), expected);
        assert_eq!(values(&mut old, &sql), expected);
    }
    drop(old);
    drop(legacy);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn unindexed_visit_limit_and_equality_buckets() {
    let f = Fixture::new();
    f.execute("CREATE TABLE a(id bigint PRIMARY KEY, v bigint)");
    f.execute("CREATE TABLE b(id bigint PRIMARY KEY, v bigint)");
    let parts = f.parts();
    let mut txn = f.db().begin(Isolation::ReadCommitted);
    for name in ["a", "b"] {
        let entry = TableEntry { primary_index_birth: None, def: parts.tables[name].clone(), heap: false, columns_ext: BTreeMap::new() };
        for id in 0..4097 {
            f.db().rows.insert(&mut txn, &entry, vec![Int(id), Int(id)], None, &|| false).unwrap();
        }
    }
    txn.commit().unwrap();
    let mut session = f.admin().sql_session();
    assert_eq!(values(&mut session, "SELECT count(*) FROM a JOIN b ON a.v=b.v"), vec![vec![Int(4097)]]);
    // Equality buckets avoid the Cartesian scan, while the original path retains its visit bound.
    relational::v2_join_cache_tests::BUCKET_DISABLED.set(true);
    let uncached = session.execute("SELECT count(*) FROM a JOIN b ON a.v=b.v", &[], None);
    relational::v2_join_cache_tests::BUCKET_DISABLED.set(false);
    assert_eq!(uncached, Err(CoreError::Sql(SqlError::Limit)));
    // A non-equality join cannot use buckets and must still enforce MAX_JOIN_VISITS.
    assert_eq!(session.execute("SELECT count(*) FROM a JOIN b ON a.v <= b.v", &[], None), Err(CoreError::Sql(SqlError::Limit)));
}
