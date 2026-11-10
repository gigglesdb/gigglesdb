//! Indexed statement images above the legacy sidecar row limit.
use super::*;
use relational::Value::{Int, Text};

fn seed(f: &Fixture, name: &str, count: usize) {
    let entry = TableEntry {
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
