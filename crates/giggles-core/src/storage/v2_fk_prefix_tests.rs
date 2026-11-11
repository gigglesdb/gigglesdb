use super::*;
use relational::Value::{self, Int};

fn seed(f: &Fixture, name: &str, count: usize, values: impl Fn(i64) -> Vec<Value>) {
    let entry = TableEntry { def: f.parts().tables[name].clone(), heap: false, columns_ext: BTreeMap::new() };
    for start in (0..count).step_by(200) {
        let mut txn = f.db().begin(Isolation::ReadCommitted);
        for n in start..(start + 200).min(count) {
            f.db().rows.insert(&mut txn, &entry, values(n as i64), None, &|| false).unwrap();
        }
        txn.commit().unwrap();
    }
}

fn count(s: &mut SqlSession, table: &str, expected: i64) {
    let p = s.prepare(&format!("SELECT count(*) FROM {table}")).unwrap();
    let mut cursor = s.open_cursor(&p, &[]).unwrap();
    let rows = cursor.fetch(1, 1024 * 1024).unwrap().into_iter().map(|r| r.values).collect::<Vec<_>>();
    assert_eq!(rows, vec![vec![Int(expected)]]);
}

#[test]
fn audit_primary_prefix_cascades_and_rollback() {
    let f = Fixture::new();
    let users = if std::env::var_os("PX_FULL_AUDIT").is_some() { 50_000 } else { 5000 };
    f.execute("CREATE TABLE users(id bigint PRIMARY KEY)");
    f.execute("CREATE TABLE orders(id bigint PRIMARY KEY, user_id bigint REFERENCES users(id) ON DELETE CASCADE)");
    f.execute("CREATE INDEX by_user ON orders(user_id)");
    f.execute("CREATE TABLE items(order_id bigint REFERENCES orders(id) ON DELETE CASCADE, sku bigint, PRIMARY KEY(order_id,sku))");
    seed(&f, "users", users, |n| vec![Int(n)]);
    seed(&f, "orders", users * 4, |n| vec![Int(n), Int(n / 4)]);
    seed(&f, "items", users * 12, |n| vec![Int(n / 3), Int(n % 3)]);
    let mut s = f.admin().sql_session();
    assert_eq!(s.execute("UPDATE orders SET id=999999 WHERE id=0", &[], None), Err(CoreError::Sql(SqlError::ForeignKeyViolation)));
    for deleted in [1, 500] {
        s.execute("BEGIN", &[], None).unwrap();
        s.execute("INSERT INTO items VALUES(0,99)", &[], None).unwrap();
        let start = std::time::Instant::now();
        assert_eq!(s.execute(&format!("DELETE FROM users WHERE id<{deleted}"), &[], None).unwrap().affected, deleted);
        eprintln!("PX {deleted}-user PK-prefix cascade ({users} users): {:?}", start.elapsed());
        for (table, per_user) in [("users", 1), ("orders", 4), ("items", 12)] {
            count(&mut s, table, (users as i64 - deleted as i64) * per_user);
        }
        s.execute("ROLLBACK", &[], None).unwrap();
        count(&mut s, "items", users as i64 * 12);
    }
    f.execute("CREATE TABLE blocked(id bigint PRIMARY KEY, ord bigint, sku bigint, FOREIGN KEY(ord,sku) REFERENCES items(order_id,sku) ON DELETE RESTRICT)");
    f.execute("INSERT INTO blocked VALUES(0,1999,2)");
    assert_eq!(s.execute("DELETE FROM users WHERE id<500", &[], None), Err(CoreError::Sql(SqlError::RestrictViolation)));
    for (table, per_user) in [("users", 1), ("orders", 4), ("items", 12)] {
        count(&mut s, table, users as i64 * per_user);
    }
    s.execute("DELETE FROM blocked", &[], None).unwrap();
    let deleted = if users == 50_000 { 4999 } else { 500 };
    let start = std::time::Instant::now();
    assert_eq!(s.execute(&format!("DELETE FROM users WHERE id<{deleted}"), &[], None).unwrap().affected, deleted);
    eprintln!("PX committed {deleted}-user PK-prefix cascade ({users} users): {:?}", start.elapsed());
    for (table, per_user) in [("users", 1), ("orders", 4), ("items", 12)] {
        count(&mut s, table, (users as i64 - deleted as i64) * per_user);
    }
}

#[test]
fn primary_prefix_restrict_no_action_reordered_keys_and_own_writes() {
    for (action, error) in [("RESTRICT", SqlError::RestrictViolation), ("NO ACTION", SqlError::ForeignKeyViolation)] {
        let f = Fixture::new();
        f.execute("CREATE TABLE p(a bigint, b text, PRIMARY KEY(a,b))");
        f.execute(&format!("CREATE TABLE c(a bigint, b text, sku bigint, PRIMARY KEY(b,a,sku), FOREIGN KEY(a,b) REFERENCES p(a,b) ON DELETE {action})"));
        f.execute("INSERT INTO p VALUES(1,'x'),(2,'xy')");
        let mut s = f.admin().sql_session();
        s.execute("BEGIN", &[], None).unwrap();
        s.execute("INSERT INTO c VALUES(1,'x',1),(2,'xy',1)", &[], None).unwrap();
        s.execute("SAVEPOINT check_fk", &[], None).unwrap();
        assert_eq!(s.execute("DELETE FROM p WHERE a=1", &[], None), Err(CoreError::Sql(error.clone())));
        s.execute("ROLLBACK TO check_fk", &[], None).unwrap();
        assert_eq!(s.execute("UPDATE p SET a=3 WHERE a=1", &[], None), Err(CoreError::Sql(SqlError::ForeignKeyViolation)));
        s.execute("ROLLBACK TO check_fk", &[], None).unwrap();
        s.execute("DELETE FROM c WHERE a=1", &[], None).unwrap();
        assert_eq!(s.execute("DELETE FROM p WHERE a=1", &[], None).unwrap().affected, 1);
        count(&mut s, "c", 1);
        s.execute("ROLLBACK", &[], None).unwrap();
        count(&mut s, "p", 2);
        count(&mut s, "c", 0);
    }
}

#[test]
fn secondary_prefix_set_null_pages_moving_keys_and_rollback() {
    let f = Fixture::new();
    f.execute("CREATE TABLE p(id bigint PRIMARY KEY)");
    f.execute("INSERT INTO p VALUES(1),(2)");
    f.execute("CREATE TABLE c(id bigint PRIMARY KEY, p bigint REFERENCES p(id) ON DELETE SET NULL)");
    f.execute("CREATE INDEX by_parent_suffix ON c(p,id)");
    seed(&f, "c", 451, |n| vec![Int(n), Int(1)]);
    f.execute("INSERT INTO c VALUES(999,2),(1000,NULL)");
    let mut s = f.admin().sql_session();
    s.execute("BEGIN", &[], None).unwrap();
    s.execute("UPDATE c SET p=2 WHERE id=0", &[], None).unwrap();
    s.execute("INSERT INTO c VALUES(1001,1)", &[], None).unwrap();
    assert_eq!(s.execute("DELETE FROM p WHERE id=1", &[], None).unwrap().affected, 1);
    count(&mut s, "c WHERE p IS NULL", 452);
    count(&mut s, "c WHERE p=2", 2);
    s.execute("ROLLBACK", &[], None).unwrap();
    count(&mut s, "p", 2);
    count(&mut s, "c WHERE p=1", 451);
    count(&mut s, "c WHERE p IS NULL", 1);
}

#[test]
fn primary_prefix_pages_resume_after_moving_or_deleted_keys() {
    let f = Fixture::new();
    f.execute("CREATE TABLE p(id bigint PRIMARY KEY)");
    f.execute("INSERT INTO p VALUES(1),(2)");
    f.execute("CREATE TABLE c(p bigint REFERENCES p(id) ON DELETE CASCADE ON UPDATE CASCADE, sku bigint, PRIMARY KEY(p,sku))");
    seed(&f, "c", 451, |n| vec![Int(1), Int(n)]);
    f.execute("INSERT INTO c VALUES(2,0)");
    let mut s = f.admin().sql_session();
    s.execute("BEGIN", &[], None).unwrap();
    s.execute("INSERT INTO c VALUES(1,1000)", &[], None).unwrap();
    assert_eq!(s.execute("UPDATE p SET id=3 WHERE id=1", &[], None).unwrap().affected, 1);
    count(&mut s, "c WHERE p=3", 452);
    assert_eq!(s.execute("DELETE FROM p WHERE id=3", &[], None).unwrap().affected, 1);
    count(&mut s, "c", 1);
    s.execute("ROLLBACK", &[], None).unwrap();
    count(&mut s, "c WHERE p=1", 451);
}

#[test]
fn encoded_tuple_prefix_is_contiguous() {
    for prefix in [vec![Int(-1)], vec![Value::Text("x\0y".into()), Int(1)], vec![Value::Bytes(vec![0, 255])]] {
        let encoded = codec::pk_bytes(&prefix).unwrap();
        assert_eq!(encoded, codec::index_tuple_bytes(&prefix, &vec![codec::Direction::Asc; prefix.len()]).unwrap());
        let upper = engine::indexes::successor(encoded.clone()).unwrap();
        for suffix in [Int(i64::MIN), Int(0), Int(i64::MAX), Value::Text("tail".into())] {
            let mut full = prefix.clone();
            full.push(suffix);
            let key = codec::pk_bytes(&full).unwrap();
            assert!(key.starts_with(&encoded) && encoded <= key && key < upper);
        }
    }
}
