//! Paged DML regressions use real row-store publishers for large fixtures.
use super::*;
use relational::Value::Int;

fn row(f: &Fixture, id: i64) -> Option<engine::rows::Row> {
    let entry = TableEntry {
        def: f.parts().tables["t"].clone(),
        heap: false,
        columns_ext: BTreeMap::new(),
    };
    let mut txn = f.db().begin(Isolation::ReadCommitted);
    f.db().rows.get(&mut txn, &entry, &[Int(id)]).unwrap()
}

#[test]
fn update_delete_200_000_with_and_without_indexes() {
    for index in [
        "",
        "CREATE INDEX ix ON t(id)",
        "CREATE UNIQUE INDEX ix ON t(v)",
    ] {
        let f = super::large_ddl::large_table(200_000, false, false);
        if index.contains("UNIQUE") {
            // PG18 succeeds for v + 1 with gaps; a dense UNIQUE sequence fails.
            f.execute("UPDATE t SET v = v * 2");
        }
        if !index.is_empty() {
            f.execute(index);
        }
        let mut session = f.admin().sql_session();
        for sql in [
            "UPDATE t SET v = v + 1",
            "UPDATE t SET v = v + 1 WHERE v >= 0",
        ] {
            session.execute("BEGIN", &[], None).unwrap();
            let before = row(&f, 199999).unwrap();
            let result = session.execute(sql, &[], None).unwrap();
            assert_eq!(result.affected, 200_000, "{index}: {sql}");
            // Other sessions see the original row until the statement commits.
            assert_eq!(row(&f, 199999).unwrap(), before);
            session.execute("ROLLBACK", &[], None).unwrap();
            assert_eq!(row(&f, 199999).unwrap(), before);
        }
        if index.contains("UNIQUE") {
            assert_eq!(
                session.execute("UPDATE t SET v = v + 400", &[], None),
                Err(CoreError::Sql(SqlError::Duplicate))
            );
        }
        for sql in ["DELETE FROM t WHERE v >= 0", "DELETE FROM t"] {
            session.execute("BEGIN", &[], None).unwrap();
            assert_eq!(session.execute(sql, &[], None).unwrap().affected, 200_000);
            session.execute("ROLLBACK", &[], None).unwrap();
            assert!(row(&f, 199999).is_some());
        }
        assert_eq!(
            session
                .execute("UPDATE t SET v = v + 1", &[], None)
                .unwrap()
                .affected,
            200_000
        );
        assert_eq!(row(&f, 0).unwrap().values, vec![Int(0), Int(1)]);
        let expected = if index.contains("UNIQUE") {
            399999
        } else {
            200000
        };
        assert_eq!(
            row(&f, 199999).unwrap().values,
            vec![Int(199999), Int(expected)]
        );
        assert_eq!(
            session
                .execute("DELETE FROM t", &[], None)
                .unwrap()
                .affected,
            200_000
        );
        assert!(row(&f, 0).is_none());
        assert!(row(&f, 199999).is_none());
        let mut txn = f.db().begin(Isolation::ReadCommitted);
        assert!(
            txn.scan(
                Keyspace::Index,
                Bound::Unbounded,
                Bound::Unbounded,
                Direction::Forward,
                None
            )
            .unwrap()
            .next()
            .is_none()
        );
    }
}

#[test]
fn dense_unique_increment_has_pg18_immediate_conflict() {
    let f = super::large_ddl::large_table(2050, false, false);
    f.execute("CREATE UNIQUE INDEX ix ON t(v)");
    assert_eq!(
        f.admin()
            .sql_session()
            .execute("UPDATE t SET v = v + 1", &[], None),
        Err(CoreError::Sql(SqlError::Duplicate))
    );
    assert_eq!(row(&f, 0).unwrap().values, vec![Int(0), Int(0)]);
}

#[test]
fn failure_on_last_page_rolls_back_200_000_updates() {
    let mut f = super::large_ddl::large_table(200_000, false, false);
    f.execute("CREATE UNIQUE INDEX ix ON t(v)");
    let first = row(&f, 0).unwrap();
    let last = row(&f, 199999).unwrap();
    assert_eq!(
        f.admin().sql_session().execute(
            "UPDATE t SET v = CASE WHEN id = 199999 THEN 200001 ELSE v + 200000 END",
            &[],
            None
        ),
        Err(CoreError::Sql(SqlError::Duplicate))
    );
    assert_eq!(row(&f, 0).unwrap(), first);
    assert_eq!(row(&f, 199999).unwrap(), last);
    f.reopen();
    assert_eq!(row(&f, 0).unwrap().values, first.values);
    assert_eq!(row(&f, 199999).unwrap().values, last.values);
}

#[test]
fn returning_exceeds_legacy_row_cap_and_keeps_output_budget() {
    let f = super::large_ddl::large_table(2050, false, true);
    let result = f
        .admin()
        .sql_session()
        .execute("UPDATE t SET v = v + 1 RETURNING id, v", &[], None)
        .unwrap();
    assert_eq!(result.rows.len(), 2050);
    assert_eq!(result.affected, 2050);
    let result = f
        .admin()
        .sql_session()
        .execute("DELETE FROM t RETURNING id", &[], None)
        .unwrap();
    assert_eq!(result.rows.len(), 2050);
    let f = super::large_ddl::large_table(20_000, false, false);
    let before = row(&f, 0).unwrap();
    assert_eq!(
        f.admin()
            .sql_session()
            .execute("UPDATE t SET v = v + 1 RETURNING id, v", &[], None),
        Err(CoreError::Sql(SqlError::Limit))
    );
    assert_eq!(row(&f, 0).unwrap(), before);
}

fn count(f: &Fixture, name: &str) -> usize {
    let entry = TableEntry {
        def: f.parts().tables[name].clone(), heap: false, columns_ext: BTreeMap::new(),
    };
    let mut txn = f.db().begin(Isolation::ReadCommitted);
    f.db().rows.scan(&mut txn, &entry, Bound::Unbounded, Bound::Unbounded,
        Direction::Forward, None).unwrap().map(Result::unwrap).count()
}

#[test]
fn audit_indexed_updates_and_snapshot_in_subqueries() {
    audit_updates(5_000, 20_000);
}

#[test]
#[ignore = "full audit-scale paged mutation timing; run explicitly"]
fn audit_indexed_updates_full_scale() {
    audit_updates(50_000, 200_000);
}

fn audit_updates(users: i64, orders: i64) {
    use relational::Value::Text;
    let f = Fixture::new();
    f.execute("CREATE TABLE users(id bigint PRIMARY KEY, email text UNIQUE)");
    f.execute("CREATE TABLE orders(id bigint PRIMARY KEY, user_id bigint REFERENCES users(id), status text)");
    f.execute("CREATE INDEX orders_user ON orders(user_id)");
    f.execute("CREATE TABLE items(id bigint PRIMARY KEY, order_id bigint REFERENCES orders(id) ON DELETE CASCADE)");
    f.execute("CREATE TABLE bare(id bigint PRIMARY KEY, v bigint)");
    let parts = f.parts();
    for (name, count) in [("users", users), ("orders", orders), ("bare", 4)] {
        let entry = TableEntry { def: parts.tables[name].clone(), heap: false, columns_ext: BTreeMap::new() };
        for start in (0..count).step_by(200) {
            let mut txn = f.db().begin(Isolation::ReadCommitted);
            for id in start..(start + 200).min(count) {
                let values = match name {
                    "users" => vec![Int(id), Text(format!("u{id}"))],
                    "orders" => vec![Int(id), Int(id % users), Text("old".into())],
                    _ => vec![Int(id), Int(0)],
                };
                f.db().rows.insert(&mut txn, &entry, values, None, &|| false).unwrap();
            }
            txn.commit().unwrap();
        }
    }
    f.execute("INSERT INTO items VALUES(1,7)");
    f.execute("CREATE TABLE precedence(id bigint PRIMARY KEY, v bigint UNIQUE CHECK(v<10))");
    f.execute("INSERT INTO precedence VALUES(0,1),(1,2),(2,9)");
    assert_eq!(f.admin().sql_session().execute("UPDATE precedence SET v=v+1", &[], None), Err(CoreError::Sql(SqlError::Duplicate)));
    let mut s = f.admin().sql_session();
    let mut run = |sql: &str, affected| {
        let start = std::time::Instant::now();
        assert_eq!(s.execute(sql, &[], None).unwrap().affected, affected, "{sql}");
        println!("audit users={users} orders={orders}: {sql}: {:?}", start.elapsed());
    };
    run(&format!("UPDATE orders SET status='x' WHERE id<{users}"), users as u64);
    run(&format!("UPDATE orders SET user_id=7 WHERE id<{users}"), users as u64);
    // Physical index entries include every moved row and exclude old owners.
    let values = s.execute("SELECT id FROM orders WHERE user_id=0", &[], None).unwrap();
    assert_eq!(values.rows.iter().map(|r| r.values.clone()).collect::<Vec<_>>(),
        vec![vec![Int(users)], vec![Int(users * 2)], vec![Int(users * 3)]]);
    assert_eq!(s.execute("SELECT count(*) FROM orders WHERE user_id=7", &[], None).unwrap().rows[0].values, vec![Int(users + 3)]);
    let started = std::time::Instant::now();
    assert_eq!(s.execute("UPDATE orders SET status='y' WHERE id IN (SELECT id FROM orders WHERE user_id=7)", &[], None).unwrap().affected, (users + 3) as u64);
    println!("audit self IN elapsed={:?}", started.elapsed());
    let started = std::time::Instant::now();
    assert_eq!(s.execute("UPDATE bare SET v=v+1 WHERE id IN (SELECT id FROM users WHERE id<3)", &[], None).unwrap().affected, 3);
    println!("audit cross IN elapsed={:?}", started.elapsed());
    assert_eq!(s.execute(&format!("UPDATE users SET email=CASE WHEN id={} THEN 'n0' ELSE 'n'||id::text END", users - 1), &[], None), Err(CoreError::Sql(SqlError::Duplicate)));
    assert_eq!(s.execute("SELECT count(*) FROM users WHERE email LIKE 'u%'", &[], None).unwrap().rows[0].values, vec![Int(users)]);
    assert_eq!(s.execute(&format!("UPDATE orders SET user_id=CASE WHEN id={} THEN {} ELSE 8 END WHERE id<{}", users - 1, users + 1, users), &[], None), Err(CoreError::Sql(SqlError::ForeignKeyViolation)));
    assert_eq!(s.execute("SELECT count(*) FROM orders WHERE user_id=7", &[], None).unwrap().rows[0].values, vec![Int(users + 3)]);
    s.execute("BEGIN", &[], None).unwrap();
    s.execute("INSERT INTO users VALUES(-1,'own')", &[], None).unwrap();
    s.execute("INSERT INTO bare VALUES(-1,0)", &[], None).unwrap();
    assert_eq!(s.execute("UPDATE bare SET v=v+1 WHERE id IN (SELECT id FROM users WHERE email='own')", &[], None).unwrap().affected, 1);
    assert_eq!(s.execute("UPDATE orders SET user_id=9 WHERE id IN (SELECT id FROM orders WHERE user_id=7)", &[], None).unwrap().affected, (users + 3) as u64);
    assert_eq!(s.execute("DELETE FROM bare WHERE id IN (SELECT id FROM users WHERE email='own')", &[], None).unwrap().affected, 1);
    assert_eq!(s.execute("DELETE FROM orders WHERE id IN (SELECT id FROM orders WHERE user_id=9)", &[], None).unwrap().affected, (users + 6) as u64);
    assert_eq!(s.execute("SELECT count(*) FROM items", &[], None).unwrap().rows[0].values, vec![Int(0)]);
    s.execute("ROLLBACK", &[], None).unwrap();
    assert_eq!(s.execute("SELECT count(*) FROM items", &[], None).unwrap().rows[0].values, vec![Int(1)]);
    assert_eq!(s.execute("SELECT count(*) FROM orders WHERE user_id=7", &[], None).unwrap().rows[0].values, vec![Int(users + 3)]);
    assert_eq!(s.execute("SELECT count(*) FROM users WHERE id=-1", &[], None).unwrap().rows[0].values, vec![Int(0)]);
}

#[test]
fn insert_select_200_000_and_late_failure() {
    for index in [
        "",
        "CREATE INDEX ix ON dst(v)",
        "CREATE UNIQUE INDEX ix ON dst(v)",
    ] {
        let f = super::large_ddl::large_table(200_000, false, false);
        f.execute("CREATE TABLE dst (id bigint PRIMARY KEY, v bigint)");
        if !index.is_empty() {
            f.execute(index);
        }
        let mut s = f.admin().sql_session();
        s.execute("BEGIN", &[], None).unwrap();
        assert_eq!(
            s.execute("INSERT INTO dst SELECT id,v FROM t", &[], None)
                .unwrap()
                .affected,
            200_000
        );
        s.execute("ROLLBACK", &[], None).unwrap();
        assert_eq!(count(&f, "dst"), 0);
        // The final input conflicts with the first row after 999 successful pages.
        assert_eq!(
            s.execute(
                "INSERT INTO dst SELECT CASE WHEN id = 199999 THEN 0 ELSE id END,v FROM t",
                &[],
                None
            ),
            Err(CoreError::Sql(SqlError::Duplicate))
        );
        assert_eq!(count(&f, "dst"), 0);
        if index.contains("UNIQUE") {
            assert_eq!(
                s.execute(
                    "INSERT INTO dst SELECT id,CASE WHEN id = 199999 THEN 0 ELSE v END FROM t",
                    &[],
                    None
                ),
                Err(CoreError::Sql(SqlError::Duplicate))
            );
            assert_eq!(count(&f, "dst"), 0);
        }
        assert_eq!(
            s.execute("INSERT INTO dst SELECT id,v FROM t", &[], None)
                .unwrap()
                .affected,
            200_000
        );
        assert_eq!(count(&f, "dst"), 200_000);
    }
}

#[test]
fn large_values_keeps_legacy_admission_and_atomicity() {
    let f = Fixture::new();
    f.execute("CREATE TABLE t (id bigint PRIMARY KEY, v bigint)");
    f.execute("CREATE UNIQUE INDEX ix ON t(v)");
    let values = (0..2050)
        .map(|id| format!("({id},{id})"))
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!("INSERT INTO t VALUES {values}");
    let mut legacy = relational::Relational::default();
    let p = relational::Prepared::parse(
        "CREATE TABLE t (id bigint PRIMARY KEY, v bigint)",
        1,
        legacy.epoch,
    )
    .unwrap();
    legacy.execute(&p, &[], None).unwrap();
    let p = relational::Prepared::parse(&sql, 1, legacy.epoch).unwrap();
    assert_eq!(legacy.execute(&p, &[], None), Err(SqlError::Limit));
    let mut s = f.admin().sql_session();
    assert_eq!(
        s.execute(&format!("{sql},(2050,0)"), &[], None),
        Err(CoreError::Sql(SqlError::Duplicate))
    );
    assert_eq!(count(&f, "t"), 0);
    assert_eq!(s.execute(&sql, &[], None).unwrap().affected, 2050);
}

#[test]
fn insert_200_000_fk_children_and_late_missing_parent() {
    let f = super::large_ddl::large_table(200_000, false, false);
    f.execute("CREATE TABLE p (id bigint PRIMARY KEY)");
    f.execute("INSERT INTO p VALUES (1)");
    f.execute("CREATE TABLE c (id bigint PRIMARY KEY, pid bigint REFERENCES p(id))");
    let mut s = f.admin().sql_session();
    assert_eq!(
        s.execute(
            "INSERT INTO c SELECT id,CASE WHEN id = 199999 THEN 2 ELSE 1 END FROM t",
            &[],
            None
        ),
        Err(CoreError::Sql(SqlError::ForeignKeyViolation))
    );
    assert_eq!(count(&f, "c"), 0);
    // Immediate uniqueness precedes a missing-parent trigger queued earlier.
    assert_eq!(s.execute("INSERT INTO c SELECT CASE WHEN id = 199999 THEN 0 ELSE id END,CASE WHEN id = 0 THEN 2 ELSE 1 END FROM t", &[], None),
        Err(CoreError::Sql(SqlError::Duplicate)));
    assert_eq!(count(&f, "c"), 0);
    assert_eq!(
        s.execute("INSERT INTO c SELECT id,1 FROM t", &[], None)
            .unwrap()
            .affected,
        200_000
    );
    assert_eq!(
        s.execute("SELECT count(*) FROM c", &[], None).unwrap().rows[0].values,
        vec![Int(200_000)]
    );
}

#[test]
#[ignore = "one-million-row DML qualification; run explicitly"]
fn million_row_update() {
    let f = super::large_ddl::large_table(1_000_000, false, false);
    let started = std::time::Instant::now();
    let result = f
        .admin()
        .sql_session()
        .execute("UPDATE t SET v = v + 1", &[], None)
        .unwrap();
    assert_eq!(result.affected, 1_000_000);
    assert_eq!(
        row(&f, 999999).unwrap().values,
        vec![Int(999999), Int(1_000_000)]
    );
    println!("1M UPDATE elapsed={:?}", started.elapsed());
    let memory = std::process::Command::new("powershell.exe")
        .args([
            "-NoProfile",
            "-Command",
            &format!("(Get-Process -Id {}).PeakWorkingSet64", std::process::id()),
        ])
        .output()
        .unwrap();
    println!(
        "1M process peak RSS bytes={}",
        String::from_utf8_lossy(&memory.stdout).trim()
    );
}

#[test]
fn primary_key_moves_stream_audit_subqueries_and_own_writes() {
    let f = Fixture::new();
    f.execute("CREATE TABLE bare(id bigint PRIMARY KEY)");
    f.execute("CREATE TABLE users(id bigint PRIMARY KEY, email text UNIQUE)");
    for (name, count) in [("bare", 100_000), ("users", 50_000)] {
        let entry = TableEntry { def: f.parts().tables[name].clone(), heap: false, columns_ext: BTreeMap::new() };
        for start in (0..count).step_by(200) {
            let mut txn = f.db().begin(Isolation::ReadCommitted);
            for id in start..(start + 200).min(count) {
                let mut values = vec![Int(id)];
                if name == "users" { values.push(relational::Value::Text(format!("u{id}"))); }
                f.db().rows.insert(&mut txn, &entry, values, None, &|| false).unwrap();
            }
            txn.commit().unwrap();
        }
    }
    let mut s = f.admin().sql_session();
    let sql = "UPDATE bare SET id=id WHERE id IN (SELECT id FROM users WHERE id=10000)";
    let started = std::time::Instant::now();
    assert_eq!(s.execute(sql, &[], None).unwrap().affected, 1);
    println!("PK audit bare=100000 users=50000: {:?}", started.elapsed());
    assert_eq!(s.execute("UPDATE bare SET id=id WHERE EXISTS (SELECT id FROM users WHERE id=10000)", &[], None).unwrap().affected, 100_000);
    s.execute("BEGIN", &[], None).unwrap();
    s.execute("INSERT INTO users VALUES(-1,'own')", &[], None).unwrap();
    s.execute("INSERT INTO bare VALUES(-1)", &[], None).unwrap();
    assert_eq!(s.execute("UPDATE bare SET id=-2 WHERE id IN (SELECT id FROM users WHERE email='own')", &[], None).unwrap().affected, 1);
    assert_eq!(s.execute("SELECT id FROM bare WHERE id=-2", &[], None).unwrap().rows[0].values, vec![Int(-2)]);
    s.execute("ROLLBACK", &[], None).unwrap();
    assert_eq!(count(&f, "bare"), 100_000);
}

#[test]
fn primary_key_moves_keep_indexes_and_immediate_collision_rollback() {
    let f = super::large_ddl::large_table(50_000, false, true);
    let mut s = f.admin().sql_session();
    assert_eq!(s.execute("UPDATE t SET id=id+1000000 WHERE id<5000", &[], None).unwrap().affected, 5000);
    assert!(row(&f, 0).is_none());
    assert_eq!(row(&f, 1004999).unwrap().values, vec![Int(1004999), Int(4999)]);
    assert_eq!(s.execute("SELECT id FROM t WHERE v=4999", &[], None).unwrap().rows[0].values, vec![Int(1004999)]);
    assert_eq!(s.execute("SELECT count(*) FROM t WHERE v<5000", &[], None).unwrap().rows[0].values, vec![Int(5000)]);
    // Several pages publish before the final collision; all must roll back.
    assert_eq!(s.execute("UPDATE t SET id=CASE WHEN id=1004999 THEN 5000 ELSE id+1000000 END WHERE id>=1000000", &[], None), Err(CoreError::Sql(SqlError::Duplicate)));
    assert_eq!(row(&f, 1000000).unwrap().values, vec![Int(1000000), Int(0)]);
    assert!(row(&f, 2000000).is_none());
    assert_eq!(s.execute("SELECT id FROM t WHERE v=0", &[], None).unwrap().rows[0].values, vec![Int(1000000)]);
    f.execute("CREATE TABLE dense(id bigint PRIMARY KEY)");
    f.execute("INSERT INTO dense VALUES(1),(2),(3)");
    for sql in ["UPDATE dense SET id=id+1", "UPDATE dense SET id=4-id"] {
        assert_eq!(s.execute(sql, &[], None), Err(CoreError::Sql(SqlError::Duplicate)), "{sql}");
    }
    assert_eq!(s.execute("UPDATE dense SET id=id-1", &[], None).unwrap().affected, 3);
    assert_eq!(s.execute("SELECT id FROM dense ORDER BY id", &[], None).unwrap().rows.iter().map(|r| r.values.clone()).collect::<Vec<_>>(), vec![vec![Int(0)],vec![Int(1)],vec![Int(2)]]);
}

#[test]
fn primary_key_moves_keep_incoming_and_outgoing_foreign_keys() {
    for action in ["NO ACTION", "CASCADE"] {
        let f = super::large_ddl::large_table(2050, false, true);
        f.execute(&format!("CREATE TABLE child(id bigint PRIMARY KEY, parent bigint REFERENCES t(id) ON UPDATE {action})"));
        f.execute("INSERT INTO child VALUES(1,0),(2,2049)");
        let mut s = f.admin().sql_session();
        let moved = s.execute("UPDATE t SET id=id+1000000", &[], None);
        if action == "NO ACTION" {
            assert_eq!(moved, Err(CoreError::Sql(SqlError::ForeignKeyViolation)));
            assert!(row(&f, 0).is_some());
            assert!(row(&f, 1000000).is_none());
        } else {
            assert_eq!(moved.unwrap().affected, 2050);
            assert_eq!(s.execute("SELECT parent FROM child ORDER BY id", &[], None).unwrap().rows.iter().map(|r| r.values.clone()).collect::<Vec<_>>(), vec![vec![Int(1000000)],vec![Int(1002049)]]);
        }
    }
    let f = Fixture::new();
    f.execute("CREATE TABLE parent(id bigint PRIMARY KEY)");
    f.execute("CREATE TABLE child(id bigint PRIMARY KEY REFERENCES parent(id))");
    f.execute("INSERT INTO parent VALUES(1),(2)");
    f.execute("INSERT INTO child VALUES(1)");
    let mut s = f.admin().sql_session();
    assert_eq!(s.execute("UPDATE child SET id=2", &[], None).unwrap().affected, 1);
    assert_eq!(s.execute("UPDATE child SET id=3", &[], None), Err(CoreError::Sql(SqlError::ForeignKeyViolation)));
    assert_eq!(s.execute("SELECT id FROM child", &[], None).unwrap().rows[0].values, vec![Int(2)]);
    f.execute("CREATE TABLE deferred_child(id bigint PRIMARY KEY, parent bigint REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)");
    f.execute("INSERT INTO deferred_child VALUES(1,1)");
    s.execute("BEGIN", &[], None).unwrap();
    s.execute("UPDATE parent SET id=10 WHERE id=1", &[], None).unwrap();
    s.execute("UPDATE deferred_child SET parent=10", &[], None).unwrap();
    s.execute("COMMIT", &[], None).unwrap();
}
