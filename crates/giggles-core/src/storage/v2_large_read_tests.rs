//! Indexed statement images above the legacy sidecar row limit.
use super::*;
use relational::Value::{Int, Text};

fn seed(f: &Fixture, name: &str, count: usize) {
    let entry = TableEntry {
        primary_index_birth: None,
        def: f.parts().tables[name].clone(),
        heap: false,
        columns_ext: BTreeMap::new(),
    };
    for start in (0..count).step_by(200) {
        let mut txn = f.db().begin(Isolation::ReadCommitted);
        for n in start..(start + 200).min(count) {
            let values = if name == "users" {
                vec![Int(n as i64), Text(format!("u{n}@test"))]
            } else {
                vec![Int(n as i64), Int((n / 4) as i64), Int((n % 2) as i64)]
            };
            f.db()
                .rows
                .insert(&mut txn, &entry, values, None, &|| false)
                .unwrap();
        }
        txn.commit().unwrap();
    }
}

fn buffered(session: &mut SqlSession, sql: &str) -> Vec<Vec<relational::Value>> {
    let p = session.prepare(sql).unwrap();
    let mut cursor = session
        .open_cursor(&p, &[])
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    cursor
        .fetch(100, 1024 * 1024)
        .unwrap()
        .into_iter()
        .map(|r| r.values)
        .collect()
}

#[test]
fn single_insert_above_1024_and_50000_keeps_physical_uniqueness() {
    for count in [1024, 50_000] {
        let f = Fixture::new();
        f.execute("CREATE TABLE users (id bigint PRIMARY KEY, email text UNIQUE)");
        seed(&f, "users", count);
        let mut s = f.admin().sql_session();
        assert_eq!(
            values(
                &mut s,
                "INSERT INTO users VALUES (50001,'new@test') RETURNING id"
            ),
            vec![vec![Int(50001)]]
        );
        assert_eq!(
            s.execute("INSERT INTO users VALUES (50002,'u0@test')", &[], None),
            Err(CoreError::Sql(SqlError::Duplicate))
        );
        assert!(values(&mut s, "SELECT id FROM users WHERE id=50002").is_empty());
        assert_eq!(
            s.execute(
                "UPDATE users SET email='updated@test' WHERE id=50001",
                &[],
                None
            )
            .unwrap()
            .affected,
            1
        );
        assert_eq!(
            s.execute("DELETE FROM users WHERE id=50001", &[], None)
                .unwrap()
                .affected,
            1
        );
        assert_eq!(
            buffered(&mut s, "SELECT count(*) FROM users"),
            vec![vec![Int(count as i64)]]
        );
    }
}

#[test]
fn indexed_50000_users_and_200000_orders_buffer_without_sidecar() {
    for users in [12, 50_000] {
        let orders = users * 4;
        let f = Fixture::new();
        f.execute("CREATE TABLE users (id bigint PRIMARY KEY, email text UNIQUE)");
        f.execute("CREATE TABLE orders (id bigint PRIMARY KEY, user_id bigint REFERENCES users(id), status bigint)");
        f.execute("CREATE INDEX by_user ON orders(user_id)");
        seed(&f, "users", users);
        seed(&f, "orders", orders);
        let mut s = f.admin().sql_session();
        assert_eq!(
            buffered(&mut s, "SELECT count(*) FROM orders"),
            vec![vec![Int(orders as i64)]]
        );
        assert_eq!(
            buffered(
                &mut s,
                "SELECT status,sum(user_id::integer) FROM orders GROUP BY status ORDER BY status"
            ),
            vec![
                vec![Int(0), Int((users * (users - 1)) as i64)],
                vec![Int(1), Int((users * (users - 1)) as i64)]
            ]
        );
        assert_eq!(
            buffered(&mut s, "SELECT count(DISTINCT user_id) FROM orders"),
            vec![vec![Int(users as i64)]]
        );
        let expected = (0..8).map(|n| vec![Int(n), Int(n / 4)]).collect::<Vec<_>>();
        assert_eq!(
            buffered(
                &mut s,
                "SELECT orders.id,users.id FROM orders JOIN users ON orders.user_id=users.id WHERE orders.id<8 ORDER BY orders.id"
            ),
            expected
        );
        assert_eq!(
            buffered(
                &mut s,
                "SELECT count(*) FROM orders WHERE user_id IN (SELECT id FROM users WHERE id<2)"
            ),
            vec![vec![Int(8)]]
        );
        assert_eq!(
            buffered(
                &mut s,
                "SELECT user_id FROM orders ORDER BY user_id DESC LIMIT 20"
            ),
            (users as i64 - 5..users as i64)
                .rev()
                .flat_map(|n| vec![vec![Int(n)]; 4])
                .collect::<Vec<_>>()
        );
        assert_eq!(
            s.execute(
                &format!("UPDATE orders SET status=1 WHERE id={}", orders - 1),
                &[],
                None
            )
            .unwrap()
            .affected,
            1
        );
        assert_eq!(
            s.execute(
                &format!("DELETE FROM orders WHERE id={}", orders - 1),
                &[],
                None
            )
            .unwrap()
            .affected,
            1
        );
        // Both the physical scan and the hydrated executor must see our own writes.
        s.execute("BEGIN", &[], None).unwrap();
        s.execute(
            &format!("INSERT INTO orders VALUES ({orders},0,0)"),
            &[],
            None,
        )
        .unwrap();
        assert_eq!(
            buffered(&mut s, "SELECT count(*) FROM orders"),
            vec![vec![Int(orders as i64)]]
        );
        s.execute("ROLLBACK", &[], None).unwrap();
    }
}
#[test]
fn sql_unique_writer_waits_and_rejects_committed_occupant_at_scale() {
    use std::sync::mpsc;
    use std::time::Duration;
    let f = Fixture::new();
    f.execute("CREATE TABLE users (id bigint PRIMARY KEY, email text UNIQUE)");
    seed(&f, "users", 50_000);
    let mut first = f.admin().sql_session();
    first.execute("BEGIN", &[], None).unwrap();
    first
        .execute("INSERT INTO users VALUES (50000,'race@test')", &[], None)
        .unwrap();
    let admin = f.admin().clone();
    let (start, started) = mpsc::channel();
    let (done, finished) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let mut s = admin.sql_session();
        start.send(()).unwrap();
        done.send(s.execute("INSERT INTO users VALUES (50001,'race@test')", &[], None))
            .unwrap();
    });
    started.recv_timeout(Duration::from_secs(10)).unwrap();
    assert!(finished.recv_timeout(Duration::from_millis(100)).is_err());
    first.execute("COMMIT", &[], None).unwrap();
    assert_eq!(
        finished.recv_timeout(Duration::from_secs(30)).unwrap(),
        Err(CoreError::Sql(SqlError::Duplicate))
    );
    worker.join().unwrap();
    assert!(values(&mut first, "SELECT id FROM users WHERE id=50001").is_empty());
}

#[test]
fn physical_conflicts_and_fk_witnesses_at_app_scale() {
    let f = Fixture::new();
    f.execute("CREATE TABLE users (id bigint PRIMARY KEY, email text UNIQUE)");
    f.execute("CREATE TABLE orders (id bigint PRIMARY KEY, user_id bigint REFERENCES users(id), status bigint)");
    f.execute("CREATE INDEX by_user ON orders(user_id)");
    seed(&f, "users", 50_000);
    seed(&f, "orders", 200_000);
    let mut s = f.admin().sql_session();
    for (id, email, action, affected) in [
        (0, "u0@test", "DO NOTHING", 0),
        (50000, "new@test", "DO NOTHING", 1),
        (0, "u0@test", "DO UPDATE SET email=excluded.email", 1),
        (50001, "newer@test", "DO UPDATE SET email=excluded.email", 1),
        (50002, "u1@test", "DO UPDATE SET email=excluded.email", 1),
    ] {
        assert_eq!(s.execute(&format!("INSERT INTO users VALUES ({id},'{email}') ON CONFLICT(email) {action}"),
            &[], None).unwrap().affected, affected);
    }
    assert!(values(&mut s, "SELECT id FROM users WHERE id=50002").is_empty());
    let batch = (0..2000).map(|n| format!("({n},'batch{n}@test')")).collect::<Vec<_>>().join(",");
    assert_eq!(s.execute(&format!("INSERT INTO users VALUES {batch} ON CONFLICT(id) DO UPDATE SET email=excluded.email"),
        &[], None).unwrap().affected, 2000);
    assert_eq!(values(&mut s, "SELECT email FROM users WHERE id=1999"), vec![vec![Text("batch1999@test".into())]]);
    // A conflict from an earlier page must retain the bounded planner's duplicate-target error.
    let batch = (0..201).map(|n| format!("({},'changed{}@test')", n % 200, n % 200)).collect::<Vec<_>>().join(",");
    assert_eq!(s.execute(&format!("INSERT INTO users VALUES {batch} ON CONFLICT(id) DO UPDATE SET email=excluded.email"),
        &[], None), Err(CoreError::Sql(SqlError::CardinalityViolation)));
    assert_eq!(values(&mut s, "SELECT email FROM users WHERE id=0"), vec![vec![Text("batch0@test".into())]]);
    assert_eq!(s.execute("INSERT INTO orders VALUES (200000,0,0)", &[], None).unwrap().affected, 1);
    assert_eq!(s.execute("INSERT INTO orders VALUES (200001,999999,0)", &[], None),
        Err(CoreError::Sql(SqlError::ForeignKeyViolation)));
    assert!(values(&mut s, "SELECT id FROM orders WHERE id=200001").is_empty());
    assert_eq!(s.execute("UPDATE orders SET user_id=1 WHERE id=200000", &[], None).unwrap().affected, 1);
    assert_eq!(s.execute("UPDATE orders SET user_id=999999 WHERE id=200000", &[], None),
        Err(CoreError::Sql(SqlError::ForeignKeyViolation)));
    assert_eq!(values(&mut s, "SELECT user_id FROM orders WHERE id=200000"), vec![vec![Int(1)]]);
    assert_eq!(s.execute("DELETE FROM users WHERE id=0", &[], None),
        Err(CoreError::Sql(SqlError::ForeignKeyViolation)));
    assert_eq!(values(&mut s, "SELECT id FROM users WHERE id=0"), vec![vec![Int(0)]]);
    assert_eq!(s.execute("UPDATE users SET id=999999 WHERE id=0", &[], None),
        Err(CoreError::Sql(SqlError::ForeignKeyViolation)));
    assert!(values(&mut s, "SELECT id FROM users WHERE id=999999").is_empty());
    assert_eq!(s.execute("UPDATE users SET email='unique-change@test' WHERE id=49999", &[], None).unwrap().affected, 1);
    assert_eq!(buffered(&mut s, "SELECT count(*) FROM orders WHERE user_id>=0"), vec![vec![Int(200001)]]);
    f.execute("CREATE TABLE restricted(id bigint PRIMARY KEY, user_id bigint REFERENCES users(id) ON DELETE RESTRICT)");
    s.execute("INSERT INTO restricted VALUES (1,0)", &[], None).unwrap();
    // PG18.6: orders' NO ACTION FK was defined before restricted's RESTRICT FK.
    assert_eq!(s.execute("DELETE FROM users WHERE id=0", &[], None), Err(CoreError::Sql(SqlError::ForeignKeyViolation)));
    s.execute("BEGIN", &[], None).unwrap();
    s.execute("INSERT INTO users VALUES (60000,'own@test')", &[], None).unwrap();
    s.execute("INSERT INTO users VALUES (60001,'own@test') ON CONFLICT(email) DO NOTHING", &[], None).unwrap();
    s.execute("INSERT INTO orders VALUES (200001,60000,0)", &[], None).unwrap();
    assert!(values(&mut s, "SELECT id FROM users WHERE id=60001").is_empty());
    s.execute("ROLLBACK", &[], None).unwrap();
    assert!(values(&mut s, "SELECT id FROM orders WHERE id=200001").is_empty());
}

#[test]
fn identity_partial_cast_and_select_upserts_probe_once() {
    let f = Fixture::new();
    f.execute("CREATE TABLE users (id bigint PRIMARY KEY, email text UNIQUE)");
    seed(&f, "users", 50_000);
    f.execute("CREATE TABLE identity_users (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, email text UNIQUE, marker bigint DEFAULT 7)");
    let mut s = f.admin().sql_session();
    let clock = relational::StatementClock { unavailable: None, transaction_us: 1,
        statement_us: 1, zone: None, wall: None };
    let batch = (0..2000).map(|n| format!("('u{n}@test'::text)")).collect::<Vec<_>>().join(",");
    assert_eq!(s.execute_at(&format!("INSERT INTO identity_users(email) VALUES {batch} ON CONFLICT(email) DO UPDATE SET marker=excluded.marker"),
        &[], None, &clock).unwrap().affected, 2000);
    let explicit = (1..=2000).map(|n| format!("({n},'u{}@test')", n-1)).collect::<Vec<_>>().join(",");
    assert_eq!(s.execute_at(&format!("INSERT INTO identity_users(id,email) OVERRIDING SYSTEM VALUE VALUES {explicit} ON CONFLICT(id) DO UPDATE SET marker=excluded.marker"),
        &[], None, &clock).unwrap().affected, 2000);
    assert_eq!(s.execute_at("INSERT INTO identity_users(email) SELECT email FROM users WHERE id<2000 ON CONFLICT(email) DO UPDATE SET marker=excluded.marker",
        &[], None, &clock).unwrap().affected, 2000);
    assert_eq!(s.execute_at("INSERT INTO identity_users(email) VALUES ('next') ON CONFLICT(email) DO NOTHING RETURNING id,marker", &[], None, &clock).unwrap().rows.into_iter().map(|r| r.values).collect::<Vec<_>>(),
        vec![vec![Int(4001), Int(7)]]);
    let repeated = (0..201).map(|n| format!("('u{}@test')", n % 200)).collect::<Vec<_>>().join(",");
    assert_eq!(s.execute_at(&format!("INSERT INTO identity_users(email) VALUES {repeated} ON CONFLICT(email) DO UPDATE SET marker=9"),
        &[], None, &clock), Err(CoreError::Sql(SqlError::CardinalityViolation)));
    assert_eq!(values(&mut s, "SELECT marker FROM identity_users WHERE id=1"), vec![vec![Int(7)]]);
}

#[test]
fn large_immediate_fk_fanout_uses_existence_witnesses() {
    let f = Fixture::new();
    f.execute("CREATE TABLE users (id bigint PRIMARY KEY, email text UNIQUE)");
    f.execute("CREATE TABLE orders (id bigint PRIMARY KEY, user_id bigint REFERENCES users(id), status bigint)");
    seed(&f, "users", 50_000);
    seed(&f, "orders", 200_000);
    f.execute("CREATE TABLE fanout (id bigint PRIMARY KEY, user_id bigint REFERENCES users(id))");
    f.execute("CREATE INDEX fanout_user ON fanout(user_id)");
    let mut s = f.admin().sql_session();
    assert_eq!(s.execute("INSERT INTO fanout SELECT id,0 FROM orders", &[], None).unwrap().affected, 200_000);
    assert_eq!(s.execute("DELETE FROM users WHERE id=0", &[], None), Err(CoreError::Sql(SqlError::ForeignKeyViolation)));
    assert_eq!(values(&mut s, "SELECT id FROM users WHERE id=0"), vec![vec![Int(0)]]);
    s.execute("DROP INDEX fanout_user", &[], None).unwrap();
    assert_eq!(s.execute("DELETE FROM users WHERE id=0", &[], None), Err(CoreError::Sql(SqlError::ForeignKeyViolation)));
    f.execute("CREATE TABLE restrict_parent (id bigint PRIMARY KEY)");
    f.execute("INSERT INTO restrict_parent VALUES (0)");
    f.execute("CREATE TABLE restricted (id bigint PRIMARY KEY, user_id bigint REFERENCES restrict_parent(id) ON DELETE RESTRICT)");
    f.execute("CREATE INDEX restricted_user ON restricted(user_id)");
    s.execute("INSERT INTO restricted SELECT id,0 FROM orders WHERE id<2000", &[], None).unwrap();
    assert_eq!(s.execute("DELETE FROM restrict_parent WHERE id=0", &[], None), Err(CoreError::Sql(SqlError::RestrictViolation)));
    assert_eq!(buffered(&mut s, "SELECT count(*) FROM fanout"), vec![vec![Int(200_000)]]);
}

#[test]
fn deferred_200000_pending_keys_validate_in_pages_and_rollback() {
    let f = Fixture::new();
    f.execute("CREATE TABLE users (id bigint PRIMARY KEY, email text UNIQUE)");
    f.execute("CREATE TABLE orders (id bigint PRIMARY KEY, user_id bigint, status bigint)");
    seed(&f, "users", 50_000);
    seed(&f, "orders", 200_000);
    f.execute("CREATE TABLE p (id bigint PRIMARY KEY)");
    f.execute("CREATE TABLE pending (id bigint PRIMARY KEY, p bigint, CONSTRAINT pending_fk FOREIGN KEY(p) REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)");
    let mut s = f.admin().sql_session();
    s.execute("BEGIN", &[], None).unwrap();
    assert_eq!(s.execute("INSERT INTO pending SELECT id,0 FROM orders", &[], None).unwrap().affected, 200_000);
    s.execute("SAVEPOINT before_check", &[], None).unwrap();
    assert_eq!(s.execute("SET CONSTRAINTS pending_fk IMMEDIATE", &[], None), Err(CoreError::Sql(SqlError::ForeignKeyViolation)));
    s.execute("ROLLBACK TO before_check", &[], None).unwrap();
    assert_eq!(buffered(&mut s, "SELECT count(*) FROM pending"), vec![vec![Int(200_000)]]);
    s.execute("INSERT INTO p VALUES (0)", &[], None).unwrap();
    s.execute("SET CONSTRAINTS pending_fk IMMEDIATE", &[], None).unwrap();
    s.execute("ROLLBACK", &[], None).unwrap();
    assert_eq!(buffered(&mut s, "SELECT count(*) FROM pending"), vec![vec![Int(0)]]);
    s.execute("BEGIN", &[], None).unwrap();
    s.execute("INSERT INTO p VALUES (0)", &[], None).unwrap();
    s.execute("INSERT INTO pending SELECT id,0 FROM orders", &[], None).unwrap();
    s.execute("COMMIT", &[], None).unwrap();
    assert_eq!(buffered(&mut s, "SELECT count(*) FROM pending"), vec![vec![Int(200_000)]]);
}


fn seed_cascade(f: &Fixture, name: &str, count: usize, parent: impl Fn(usize) -> usize) {
    let entry = TableEntry { primary_index_birth: None, def: f.parts().tables[name].clone(), heap: false, columns_ext: BTreeMap::new() };
    for start in (0..count).step_by(200) {
        let mut txn = f.db().begin(Isolation::ReadCommitted);
        for n in start..(start + 200).min(count) {
            f.db().rows.insert(&mut txn, &entry, vec![Int(n as i64), Int(parent(n) as i64)], None, &|| false).unwrap();
        }
        txn.commit().unwrap();
    }
}

#[test]
fn cascade_5000_users_three_levels_returning_and_late_rollback() {
    let f = Fixture::new();
    f.execute("CREATE TABLE users (id bigint PRIMARY KEY, email text UNIQUE)");
    f.execute("CREATE TABLE orders (id bigint PRIMARY KEY, user_id bigint REFERENCES users(id) ON DELETE CASCADE, status bigint)");
    f.execute("CREATE INDEX by_user ON orders(user_id)");
    f.execute("CREATE TABLE items (id bigint PRIMARY KEY, order_id bigint REFERENCES orders(id) ON DELETE CASCADE)");
    f.execute("CREATE INDEX by_order ON items(order_id)");
    seed(&f, "users", 6000);
    seed(&f, "orders", 24000);
    seed_cascade(&f, "items", 72000, |n| n / 3);
    f.execute("CREATE TABLE blocked (id bigint PRIMARY KEY, item_id bigint REFERENCES items(id) ON DELETE RESTRICT)");
    f.execute("INSERT INTO blocked VALUES (1,59999)");
    let mut s = f.admin().sql_session();
    assert_eq!(s.execute("DELETE FROM users WHERE id<5000 RETURNING id", &[], None), Err(CoreError::Sql(SqlError::RestrictViolation)));
    for (table, count) in [("users",6000), ("orders",24000), ("items",72000)] {
        assert_eq!(buffered(&mut s, &format!("SELECT count(*) FROM {table}")), vec![vec![Int(count)]]);
    }
    s.execute("DELETE FROM blocked", &[], None).unwrap();
    eprintln!("X8 cascade-start");
    let start = std::time::Instant::now();
    let deleted = s.execute("DELETE FROM users WHERE id<5000 RETURNING id", &[], None).unwrap();
    eprintln!("X8 5000-user/20000-order/60000-item cascade: {:?}", start.elapsed());
    assert_eq!(deleted.affected, 5000);
    assert_eq!(deleted.rows.into_iter().map(|r| r.values).collect::<Vec<_>>(), (0..5000).map(|n| vec![Int(n)]).collect::<Vec<_>>());
    for (table, count) in [("users",1000), ("orders",4000), ("items",12000)] {
        assert_eq!(buffered(&mut s, &format!("SELECT count(*) FROM {table}")), vec![vec![Int(count)]]);
    }
    assert_eq!(s.execute("DELETE FROM users WHERE id=5000 RETURNING id", &[], None).unwrap().affected, 1);
    assert_eq!(buffered(&mut s, "SELECT count(*) FROM items"), vec![vec![Int(11988)]]);
}

#[test]
fn cascade_large_fanout_self_reference_cycle_and_own_writes() {
    let f = Fixture::new();
    f.execute("CREATE TABLE nodes (id bigint PRIMARY KEY, parent bigint REFERENCES nodes(id) ON DELETE CASCADE)");
    f.execute("CREATE INDEX by_parent ON nodes(parent)");
    seed_cascade(&f, "nodes", 1500, |_| 0);
    let mut s = f.admin().sql_session();
    s.execute("BEGIN", &[], None).unwrap();
    s.execute("INSERT INTO nodes VALUES (1500,0)", &[], None).unwrap();
    assert_eq!(s.execute("DELETE FROM nodes WHERE id=0 RETURNING id", &[], None).unwrap().affected, 1);
    assert_eq!(buffered(&mut s, "SELECT count(*) FROM nodes"), vec![vec![Int(0)]]);
    s.execute("ROLLBACK", &[], None).unwrap();
    assert_eq!(buffered(&mut s, "SELECT count(*) FROM nodes"), vec![vec![Int(1500)]]);
    f.execute("CREATE TABLE a(id bigint PRIMARY KEY, b bigint)");
    f.execute("CREATE TABLE b(id bigint PRIMARY KEY, a bigint REFERENCES a(id) ON DELETE CASCADE)");
    f.execute("ALTER TABLE a ADD FOREIGN KEY(b) REFERENCES b(id) ON DELETE CASCADE");
    seed_cascade(&f, "a", 1500, |n| n);
    seed_cascade(&f, "b", 1500, |n| n);
    assert_eq!(s.execute("DELETE FROM a RETURNING id", &[], None).unwrap().affected, 1500);
    assert_eq!(buffered(&mut s, "SELECT count(*) FROM b"), vec![vec![Int(0)]]);
}


#[test]
fn cascade_diamond_deduplicates_and_unindexed_fanout_pages() {
    let f = Fixture::new();
    f.execute("CREATE TABLE p(id bigint PRIMARY KEY)");
    f.execute("CREATE TABLE c(id bigint PRIMARY KEY, p bigint REFERENCES p(id) ON DELETE CASCADE)");
    f.execute("CREATE TABLE d(id bigint PRIMARY KEY, p bigint REFERENCES p(id) ON DELETE CASCADE)");
    f.execute("CREATE TABLE leaf(id bigint PRIMARY KEY, c bigint REFERENCES c(id) ON DELETE CASCADE, d bigint REFERENCES d(id) ON DELETE CASCADE)");
    f.execute("INSERT INTO p VALUES(0)");
    seed_cascade(&f, "c", 1500, |_| 0);
    seed_cascade(&f, "d", 1500, |_| 0);
    let mut s = f.admin().sql_session();
    s.execute("INSERT INTO leaf SELECT id,id,id FROM c", &[], None).unwrap();
    assert_eq!(s.execute("DELETE FROM p", &[], None).unwrap().affected, 1);
    for table in ["p", "c", "d", "leaf"] {
        assert_eq!(buffered(&mut s, &format!("SELECT count(*) FROM {table}")), vec![vec![Int(0)]]);
    }
}

#[test]
fn x8b_set_null_5000_parents_own_writes_and_rollback() {
    let f = Fixture::new();
    f.execute("CREATE TABLE p(id bigint PRIMARY KEY, unused bigint)");
    f.execute("CREATE TABLE c(id bigint PRIMARY KEY, p bigint REFERENCES p(id) ON DELETE SET NULL)");
    f.execute("CREATE INDEX c_parent ON c(p)");
    seed_cascade(&f, "p", 5000, |n| n);
    seed_cascade(&f, "c", 20000, |n| n / 4);
    let mut s = f.admin().sql_session();
    s.execute("BEGIN", &[], None).unwrap();
    s.execute("INSERT INTO c VALUES(20000,4999)", &[], None).unwrap();
    let start = std::time::Instant::now();
    assert_eq!(s.execute("DELETE FROM p", &[], None).unwrap().affected, 5000);
    eprintln!("X8b 5000 parents x 4 children SET NULL: {:?}", start.elapsed());
    assert_eq!(buffered(&mut s, "SELECT count(*) FROM c WHERE p IS NULL"), vec![vec![Int(20001)]]);
    s.execute("ROLLBACK", &[], None).unwrap();
    assert_eq!(buffered(&mut s, "SELECT count(*) FROM p"), vec![vec![Int(5000)]]);
    assert_eq!(buffered(&mut s, "SELECT count(*) FROM c WHERE p IS NOT NULL"), vec![vec![Int(20000)]]);
}

#[test]
fn x8b_set_actions_large_fanout_delete_and_update() {
    for action in ["SET NULL", "SET DEFAULT"] {
        for mutation in ["DELETE FROM p WHERE id=0", "UPDATE p SET id=1 WHERE id=0"] {
            let f = Fixture::new();
            f.execute("CREATE TABLE p(id bigint PRIMARY KEY)");
            f.execute("INSERT INTO p VALUES(-1),(0)");
            f.execute(&format!("CREATE TABLE c(id bigint PRIMARY KEY, p bigint DEFAULT -1 REFERENCES p(id) ON DELETE {action} ON UPDATE {action})"));
            f.execute("CREATE INDEX c_parent ON c(p)");
            seed_cascade(&f, "c", 1500, |_| 0);
            let mut s = f.admin().sql_session();
            assert_eq!(s.execute(mutation, &[], None).unwrap().affected, 1);
            let predicate = if action == "SET NULL" { "p IS NULL" } else { "p=-1" };
            assert_eq!(buffered(&mut s, &format!("SELECT count(*) FROM c WHERE {predicate}")), vec![vec![Int(1500)]]);
        }
    }
}

#[test]
fn x8b_update_cascade_two_levels_and_moving_cursor() {
    let f = Fixture::new();
    f.execute("CREATE TABLE p(id bigint PRIMARY KEY, unused bigint)");
    f.execute("CREATE TABLE c(id bigint PRIMARY KEY REFERENCES p(id) ON UPDATE CASCADE, unused bigint)");
    f.execute("CREATE TABLE leaf(id bigint PRIMARY KEY, p bigint REFERENCES c(id) ON UPDATE CASCADE)");
    f.execute("CREATE INDEX leaf_parent ON leaf(p)");
    seed_cascade(&f, "p", 1500, |n| n);
    seed_cascade(&f, "c", 1500, |n| n);
    seed_cascade(&f, "leaf", 4500, |n| n / 3);
    let mut s = f.admin().sql_session();
    assert_eq!(s.execute("UPDATE p SET id=id+10000 RETURNING id", &[], None).unwrap().affected, 1500);
    assert_eq!(buffered(&mut s, "SELECT count(*) FROM c WHERE id>=10000"), vec![vec![Int(1500)]]);
    assert_eq!(buffered(&mut s, "SELECT count(*) FROM leaf WHERE p>=10000"), vec![vec![Int(4500)]]);
    assert_eq!(s.execute("UPDATE p SET id=10001 WHERE id=10000", &[], None), Err(CoreError::Sql(SqlError::Duplicate)));
}

#[test]
fn x8b_single_parent_update_cascade_large_two_level_fanout() {
    let f = Fixture::new();
    f.execute("CREATE TABLE p(id bigint PRIMARY KEY)");
    f.execute("INSERT INTO p VALUES(0)");
    f.execute("CREATE TABLE c(id bigint PRIMARY KEY, p bigint REFERENCES p(id) ON UPDATE CASCADE, UNIQUE(id,p))");
    f.execute("CREATE INDEX c_parent ON c(p)");
    f.execute("CREATE TABLE leaf(id bigint PRIMARY KEY, c bigint, p bigint, FOREIGN KEY(c,p) REFERENCES c(id,p) ON UPDATE CASCADE)");
    f.execute("CREATE INDEX leaf_parent ON leaf(c,p)");
    seed_cascade(&f, "c", 1500, |_| 0);
    let mut s = f.admin().sql_session();
    s.execute("INSERT INTO leaf SELECT id,id,0 FROM c", &[], None).unwrap();
    assert_eq!(s.execute("UPDATE p SET id=1", &[], None).unwrap().affected, 1);
    for table in ["c", "leaf"] {
        assert_eq!(buffered(&mut s, &format!("SELECT count(*) FROM {table} WHERE p=1")), vec![vec![Int(1500)]]);
    }
}

#[test]
fn x8b_unchanged_default_and_deferred_autocommit_still_check_parent() {
    for default in [0, 9999] {
        let f = Fixture::new();
        f.execute("CREATE TABLE p(id bigint PRIMARY KEY)");
        f.execute("INSERT INTO p VALUES(0)");
        f.execute(&format!("CREATE TABLE c(id bigint PRIMARY KEY, p bigint DEFAULT {default} REFERENCES p(id) ON DELETE SET DEFAULT DEFERRABLE INITIALLY DEFERRED)"));
        f.execute("CREATE INDEX c_parent ON c(p)");
        seed_cascade(&f, "c", 1500, |_| 0);
        let mut s = f.admin().sql_session();
        assert_eq!(s.execute("DELETE FROM p", &[], None), Err(CoreError::Sql(SqlError::ForeignKeyViolation)));
        assert_eq!(buffered(&mut s, "SELECT count(*) FROM c WHERE p=0"), vec![vec![Int(1500)]]);
        s.execute("BEGIN", &[], None).unwrap();
        let result = s.execute("DELETE FROM p", &[], None);
        if default == 0 {
            assert_eq!(result, Err(CoreError::Sql(SqlError::ForeignKeyViolation)));
        } else {
            assert_eq!(result.unwrap().affected, 1);
        }
        s.execute("ROLLBACK", &[], None).unwrap();
    }
}

#[test]
fn x8b_late_set_action_failure_rolls_back_all_pages() {
    for (action, bad_type, error) in [
        ("SET NULL", "bigint NOT NULL", SqlError::Null),
        ("SET DEFAULT", "bigint DEFAULT 9999", SqlError::ForeignKeyViolation),
    ] {
        let f = Fixture::new();
        f.execute("CREATE TABLE p(id bigint PRIMARY KEY)");
        f.execute("INSERT INTO p VALUES(-1),(0)");
        f.execute(&format!("CREATE TABLE c(id bigint PRIMARY KEY, p bigint DEFAULT -1 REFERENCES p(id) ON DELETE {action} ON UPDATE {action})"));
        f.execute("CREATE INDEX c_parent ON c(p)");
        seed_cascade(&f, "c", 1500, |_| 0);
        f.execute(&format!("CREATE TABLE bad(id bigint PRIMARY KEY, p {bad_type} REFERENCES p(id) ON DELETE {action} ON UPDATE {action})"));
        f.execute("INSERT INTO bad VALUES(1,0)");
        let mut s = f.admin().sql_session();
        for sql in ["DELETE FROM p WHERE id=0", "UPDATE p SET id=1 WHERE id=0"] {
            assert_eq!(s.execute(sql, &[], None), Err(CoreError::Sql(error.clone())));
            assert_eq!(buffered(&mut s, "SELECT count(*) FROM p WHERE id=0"), vec![vec![Int(1)]]);
            assert_eq!(buffered(&mut s, "SELECT count(*) FROM c WHERE p=0"), vec![vec![Int(1500)]]);
            assert_eq!(buffered(&mut s, "SELECT p FROM bad"), vec![vec![Int(0)]]);
        }
    }
}
