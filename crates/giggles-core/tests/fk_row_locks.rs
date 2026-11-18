//! PostgreSQL 18.6 oracle: J:/gigglesdb-work/perf-c/oracle/README.md.
//! Core BEGIN syntax has no isolation clause; set_isolation is the server-equivalent API.
#![cfg(feature = "engine-rocksdb")]
mod common;
use giggles_core::{
    Admin, CoreError, Engine, Limits, SqlIsolation, SqlSession,
    relational::{SqlError, Value},
};
use std::{
    path::PathBuf,
    sync::{Arc, Barrier, mpsc},
    thread,
    time::{Duration, Instant},
};

struct Fixture {
    admin: Option<Admin>,
    path: PathBuf,
}
impl Fixture {
    fn new(setup: &str) -> Self {
        let path = common::fresh("fk-row-locks");
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        let mut s = admin.sql_session();
        for sql in setup.split(';').filter(|sql| !sql.trim().is_empty()) {
            ok(&mut s, sql);
        }
        drop(s);
        Self {
            admin: Some(admin),
            path,
        }
    }
    fn admin(&self) -> &Admin {
        self.admin.as_ref().unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.admin.take();
        std::fs::remove_dir_all(&self.path).unwrap();
    }
}
fn execute(s: &mut SqlSession, sql: &str) -> Result<(), CoreError> {
    // Match PostgreSQL/server COMMIT of an aborted block (the core API requires ROLLBACK).
    let sql = if sql == "COMMIT" && s.is_aborted() {
        "ROLLBACK"
    } else {
        sql
    };
    s.execute(sql, &[], None).map(|_| ())
}
fn ok(s: &mut SqlSession, sql: &str) {
    execute(s, sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
}
fn check(result: &Result<(), CoreError>, expected: &str, sql: &str) {
    match expected {
        "OK" => assert_eq!(result, &Ok(()), "{sql}"),
        "23503" => assert_eq!(
            result,
            &Err(CoreError::Sql(SqlError::ForeignKeyViolation)),
            "{sql}"
        ),
        "40001" => assert!(
            matches!(
                result,
                Err(CoreError::Sql(SqlError::Conflict | SqlError::Version))
            ),
            "{sql}: {result:?}"
        ),
        "40P01" => assert_eq!(result, &Err(CoreError::Deadlock), "{sql}"),
        _ => panic!("unexpected oracle code {expected}"),
    }
}
fn rows(s: &mut SqlSession, sql: &str) -> Vec<Vec<i64>> {
    s.execute(sql, &[], None)
        .unwrap()
        .rows
        .into_iter()
        .map(|r| {
            r.values
                .into_iter()
                .map(|v| match v {
                    Value::Int(n) => n,
                    other => panic!("{other:?}"),
                })
                .collect()
        })
        .collect()
}
struct Case {
    setup: &'static str,
    first: &'static str,
    second: &'static str,
    extra: Option<&'static str>,
    rollback: bool,
    wait: bool,
    snapshot: bool,
    // A first, B second, A extra, A end, B end, respectively.
    expected: [&'static str; 5],
    parents: &'static [i64],
    children: &'static [i64],
}
fn run(c: Case, level: SqlIsolation) {
    let f = Fixture::new(c.setup);
    let mut a = f.admin().sql_session();
    a.set_isolation(level);
    ok(&mut a, "BEGIN");
    if c.snapshot {
        ok(&mut a, "SELECT count(*) FROM u");
    }
    let first = if c.snapshot {
        Ok(())
    } else {
        execute(&mut a, c.first)
    };
    let admin = f.admin().clone();
    let (started, ready) = mpsc::channel();
    let (tx, rx) = mpsc::channel();
    let (finish, release) = mpsc::channel();
    let worker = thread::spawn(move || {
        let mut b = admin.sql_session();
        b.set_isolation(level);
        let begin = execute(&mut b, "BEGIN");
        started.send(()).unwrap();
        let second = execute(&mut b, c.second);
        tx.send((begin, second)).unwrap();
        // Snapshot case requires B's COMMIT before A's INSERT.
        if !c.snapshot {
            release.recv().unwrap();
        }
        execute(&mut b, "COMMIT")
    });
    ready.recv_timeout(Duration::from_secs(10)).unwrap();
    let early = rx.recv_timeout(if c.wait {
        Duration::from_millis(150)
    } else {
        Duration::from_secs(10)
    });
    let waited = matches!(early, Err(mpsc::RecvTimeoutError::Timeout));
    let extra = c.extra.map(|sql| execute(&mut a, sql)).unwrap_or(Ok(()));
    let (first, end_a, second, end_b) = if c.snapshot {
        let (begin, second) = early.expect("snapshot writer blocked");
        check(&begin, "OK", "B BEGIN");
        let end_b = worker.join().unwrap();
        let first = execute(&mut a, c.first);
        let end_a = execute(&mut a, "COMMIT");
        (first, end_a, second, end_b)
    } else {
        let end_a = execute(&mut a, if c.rollback { "ROLLBACK" } else { "COMMIT" });
        let (begin, second) = early.unwrap_or_else(|_| {
            rx.recv_timeout(Duration::from_secs(10))
                .expect("B failed to finish")
        });
        check(&begin, "OK", "B BEGIN");
        finish.send(()).unwrap();
        (first, end_a, second, worker.join().unwrap())
    };
    let mut s = f.admin().sql_session();
    let parents = rows(&mut s, "SELECT id,code,v FROM p ORDER BY id");
    let children = rows(&mut s, "SELECT id,p FROM c ORDER BY id");
    let key = if c.setup.contains("REFERENCES p(code)") {
        1
    } else {
        0
    };
    for child in &children {
        assert!(
            parents.iter().any(|p| p[key] == child[1]),
            "orphan: {child:?}"
        );
    }
    // Check after releasing transactions and joining workers, including on known failures.
    assert_eq!(
        waited, c.wait,
        "second statement wait mismatch at {level:?}"
    );
    for (result, expected, sql) in [
        (&first, c.expected[0], c.first),
        (&second, c.expected[1], c.second),
        (&extra, c.expected[2], "A extra"),
        (&end_a, c.expected[3], "A end"),
        (&end_b, c.expected[4], "B COMMIT"),
    ] {
        check(result, expected, sql);
    }
    assert_eq!(parents.into_iter().flatten().collect::<Vec<_>>(), c.parents);
    assert_eq!(
        children.into_iter().flatten().collect::<Vec<_>>(),
        c.children
    );
}

#[test]
fn users_orders_workload() {
    let f = Fixture::new(
        "CREATE TABLE users(id int PRIMARY KEY,v int); CREATE TABLE orders(id int PRIMARY KEY,user_id int REFERENCES users(id))",
    );
    let mut s = f.admin().sql_session();
    for id in 0..64 {
        ok(&mut s, &format!("INSERT INTO users VALUES({id},0)"));
    }
    let barrier = Arc::new(Barrier::new(32));
    let workers: Vec<_> = (0..32).map(|lane| {
        let admin = f.admin().clone(); let barrier = barrier.clone();
        thread::spawn(move || {
            let mut s = admin.sql_session(); let mut seed = lane as u64 + 1; let mut n = 0;
            barrier.wait(); let start = Instant::now();
            while start.elapsed() < Duration::from_secs(2) {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                let user = (seed >> 32) % 64;
                let sql = match n % 3 {
                    0 => {
                        let existing = rows(&mut s, "SELECT id FROM users ORDER BY id");
                        if existing.is_empty() { n += 1; continue; }
                        let user = existing[(seed as usize) % existing.len()][0];
                        format!("INSERT INTO orders VALUES({}, {user})", lane * 1_000_000 + n)
                    },
                    1 => format!("DELETE FROM users WHERE id={user} AND NOT EXISTS (SELECT 1 FROM orders WHERE user_id={user})"),
                    _ => format!("UPDATE users SET v=v+1 WHERE id={user}"),
                };
                if let Err(e) = execute(&mut s, &sql) {
                    assert!(matches!(e, CoreError::Sql(SqlError::ForeignKeyViolation | SqlError::Conflict | SqlError::Version) | CoreError::Deadlock), "{sql}: {e:?}");
                }
                n += 1;
            }
            n
        })
    }).collect();
    let results: Vec<_> = workers.into_iter().map(|worker| worker.join()).collect();
    for result in results {
        assert!(result.unwrap() > 0);
    }
    let users = rows(&mut s, "SELECT id FROM users");
    let orders = rows(&mut s, "SELECT user_id FROM orders");
    for order in orders {
        assert!(users.contains(&order), "orphan: {order:?}");
    }
}
// GENERATED ORACLE CASES
#[test]
fn cascade_child_then_parent_delete_read_committed() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id) ON DELETE CASCADE); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "INSERT INTO c VALUES(1,1);",
            second: "DELETE FROM p WHERE id=1;",
            extra: None,
            rollback: false,
            wait: true,
            snapshot: false,
            expected: ["OK", "OK", "OK", "OK", "OK"],
            parents: &[],
            children: &[],
        },
        SqlIsolation::ReadCommitted,
    );
}

#[test]
fn cascade_child_then_parent_delete_repeatable_read() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id) ON DELETE CASCADE); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "INSERT INTO c VALUES(1,1);",
            second: "DELETE FROM p WHERE id=1;",
            extra: None,
            rollback: false,
            wait: true,
            snapshot: false,
            expected: ["OK", "40001", "OK", "OK", "OK"],
            parents: &[1, 10, 0],
            children: &[1, 1],
        },
        SqlIsolation::RepeatableRead,
    );
}

#[test]
fn cascade_child_then_parent_delete_serializable() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id) ON DELETE CASCADE); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "INSERT INTO c VALUES(1,1);",
            second: "DELETE FROM p WHERE id=1;",
            extra: None,
            rollback: false,
            wait: true,
            snapshot: false,
            expected: ["OK", "40001", "OK", "OK", "OK"],
            parents: &[1, 10, 0],
            children: &[1, 1],
        },
        SqlIsolation::Serializable,
    );
}

#[test]
fn cascade_parent_delete_then_child_read_committed() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id) ON DELETE CASCADE); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0); INSERT INTO c VALUES(1,1);",
            first: "DELETE FROM p WHERE id=1;",
            second: "INSERT INTO c VALUES(2,1);",
            extra: None,
            rollback: false,
            wait: true,
            snapshot: false,
            expected: ["OK", "23503", "OK", "OK", "OK"],
            parents: &[],
            children: &[],
        },
        SqlIsolation::ReadCommitted,
    );
}

#[test]
fn cascade_parent_delete_then_child_repeatable_read() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id) ON DELETE CASCADE); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0); INSERT INTO c VALUES(1,1);",
            first: "DELETE FROM p WHERE id=1;",
            second: "INSERT INTO c VALUES(2,1);",
            extra: None,
            rollback: false,
            wait: true,
            snapshot: false,
            expected: ["OK", "40001", "OK", "OK", "OK"],
            parents: &[],
            children: &[],
        },
        SqlIsolation::RepeatableRead,
    );
}

#[test]
fn cascade_parent_delete_then_child_serializable() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id) ON DELETE CASCADE); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0); INSERT INTO c VALUES(1,1);",
            first: "DELETE FROM p WHERE id=1;",
            second: "INSERT INTO c VALUES(2,1);",
            extra: None,
            rollback: false,
            wait: true,
            snapshot: false,
            expected: ["OK", "40001", "OK", "OK", "OK"],
            parents: &[],
            children: &[],
        },
        SqlIsolation::Serializable,
    );
}

#[test]
fn child_after_committed_nonkey_update_read_committed() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "INSERT INTO c VALUES(1,1);",
            second: "UPDATE p SET v=v+1 WHERE id=1;",
            extra: None,
            rollback: false,
            wait: false,
            snapshot: true,
            expected: ["OK", "OK", "OK", "OK", "OK"],
            parents: &[1, 10, 1],
            children: &[1, 1],
        },
        SqlIsolation::ReadCommitted,
    );
}

#[test]
fn child_after_committed_nonkey_update_repeatable_read() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "INSERT INTO c VALUES(1,1);",
            second: "UPDATE p SET v=v+1 WHERE id=1;",
            extra: None,
            rollback: false,
            wait: false,
            snapshot: true,
            expected: ["OK", "OK", "OK", "OK", "OK"],
            parents: &[1, 10, 1],
            children: &[1, 1],
        },
        SqlIsolation::RepeatableRead,
    );
}

#[test]
fn child_after_committed_nonkey_update_serializable() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "INSERT INTO c VALUES(1,1);",
            second: "UPDATE p SET v=v+1 WHERE id=1;",
            extra: None,
            rollback: false,
            wait: false,
            snapshot: true,
            expected: ["OK", "OK", "OK", "OK", "OK"],
            parents: &[1, 10, 1],
            children: &[1, 1],
        },
        SqlIsolation::Serializable,
    );
}

#[test]
fn child_vs_parent_nonkey_update_read_committed() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "INSERT INTO c VALUES(1,1);",
            second: "UPDATE p SET v=v+1 WHERE id=1;",
            extra: None,
            rollback: false,
            wait: false,
            snapshot: false,
            expected: ["OK", "OK", "OK", "OK", "OK"],
            parents: &[1, 10, 1],
            children: &[1, 1],
        },
        SqlIsolation::ReadCommitted,
    );
}

#[test]
fn child_vs_parent_nonkey_update_repeatable_read() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "INSERT INTO c VALUES(1,1);",
            second: "UPDATE p SET v=v+1 WHERE id=1;",
            extra: None,
            rollback: false,
            wait: false,
            snapshot: false,
            expected: ["OK", "OK", "OK", "OK", "OK"],
            parents: &[1, 10, 1],
            children: &[1, 1],
        },
        SqlIsolation::RepeatableRead,
    );
}

#[test]
fn child_vs_parent_nonkey_update_serializable() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "INSERT INTO c VALUES(1,1);",
            second: "UPDATE p SET v=v+1 WHERE id=1;",
            extra: None,
            rollback: false,
            wait: false,
            snapshot: false,
            expected: ["OK", "OK", "OK", "OK", "OK"],
            parents: &[1, 10, 1],
            children: &[1, 1],
        },
        SqlIsolation::Serializable,
    );
}

#[test]
fn deadlock_read_committed() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "INSERT INTO c VALUES(1,1);",
            second: "DELETE FROM p WHERE id=1;",
            extra: Some("UPDATE p SET v=1 WHERE id=1;"),
            rollback: false,
            wait: true,
            snapshot: false,
            expected: ["OK", "23503", "OK", "OK", "OK"],
            parents: &[1, 10, 1],
            children: &[1, 1],
        },
        SqlIsolation::ReadCommitted,
    );
}

#[test]
fn deadlock_repeatable_read() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "INSERT INTO c VALUES(1,1);",
            second: "DELETE FROM p WHERE id=1;",
            extra: Some("UPDATE p SET v=1 WHERE id=1;"),
            rollback: false,
            wait: true,
            snapshot: false,
            expected: ["OK", "40001", "OK", "OK", "OK"],
            parents: &[1, 10, 1],
            children: &[1, 1],
        },
        SqlIsolation::RepeatableRead,
    );
}

#[test]
fn deadlock_serializable() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "INSERT INTO c VALUES(1,1);",
            second: "DELETE FROM p WHERE id=1;",
            extra: Some("UPDATE p SET v=1 WHERE id=1;"),
            rollback: false,
            wait: true,
            snapshot: false,
            expected: ["OK", "40001", "OK", "OK", "OK"],
            parents: &[1, 10, 1],
            children: &[1, 1],
        },
        SqlIsolation::Serializable,
    );
}

#[test]
fn fk_to_unique_key_update_read_committed() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(code)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "INSERT INTO c VALUES(1,10);",
            second: "UPDATE p SET code=20 WHERE id=1;",
            extra: None,
            rollback: false,
            wait: true,
            snapshot: false,
            expected: ["OK", "23503", "OK", "OK", "OK"],
            parents: &[1, 10, 0],
            children: &[1, 10],
        },
        SqlIsolation::ReadCommitted,
    );
}

#[test]
fn fk_to_unique_key_update_repeatable_read() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(code)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "INSERT INTO c VALUES(1,10);",
            second: "UPDATE p SET code=20 WHERE id=1;",
            extra: None,
            rollback: false,
            wait: true,
            snapshot: false,
            expected: ["OK", "23503", "OK", "OK", "OK"],
            parents: &[1, 10, 0],
            children: &[1, 10],
        },
        SqlIsolation::RepeatableRead,
    );
}

#[test]
fn fk_to_unique_key_update_serializable() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(code)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "INSERT INTO c VALUES(1,10);",
            second: "UPDATE p SET code=20 WHERE id=1;",
            extra: None,
            rollback: false,
            wait: true,
            snapshot: false,
            expected: ["OK", "23503", "OK", "OK", "OK"],
            parents: &[1, 10, 0],
            children: &[1, 10],
        },
        SqlIsolation::Serializable,
    );
}

#[test]
fn fk_to_unique_nonkey_update_read_committed() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(code)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "INSERT INTO c VALUES(1,10);",
            second: "UPDATE p SET v=1 WHERE id=1;",
            extra: None,
            rollback: false,
            wait: false,
            snapshot: false,
            expected: ["OK", "OK", "OK", "OK", "OK"],
            parents: &[1, 10, 1],
            children: &[1, 10],
        },
        SqlIsolation::ReadCommitted,
    );
}

#[test]
fn fk_to_unique_nonkey_update_repeatable_read() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(code)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "INSERT INTO c VALUES(1,10);",
            second: "UPDATE p SET v=1 WHERE id=1;",
            extra: None,
            rollback: false,
            wait: false,
            snapshot: false,
            expected: ["OK", "OK", "OK", "OK", "OK"],
            parents: &[1, 10, 1],
            children: &[1, 10],
        },
        SqlIsolation::RepeatableRead,
    );
}

#[test]
fn fk_to_unique_nonkey_update_serializable() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(code)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "INSERT INTO c VALUES(1,10);",
            second: "UPDATE p SET v=1 WHERE id=1;",
            extra: None,
            rollback: false,
            wait: false,
            snapshot: false,
            expected: ["OK", "OK", "OK", "OK", "OK"],
            parents: &[1, 10, 1],
            children: &[1, 10],
        },
        SqlIsolation::Serializable,
    );
}

#[test]
fn inflight_child_then_parent_delete_read_committed() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "INSERT INTO c VALUES(1,1);",
            second: "DELETE FROM p WHERE id=1;",
            extra: None,
            rollback: false,
            wait: true,
            snapshot: false,
            expected: ["OK", "23503", "OK", "OK", "OK"],
            parents: &[1, 10, 0],
            children: &[1, 1],
        },
        SqlIsolation::ReadCommitted,
    );
}

#[test]
fn inflight_child_then_parent_delete_repeatable_read() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "INSERT INTO c VALUES(1,1);",
            second: "DELETE FROM p WHERE id=1;",
            extra: None,
            rollback: false,
            wait: true,
            snapshot: false,
            expected: ["OK", "23503", "OK", "OK", "OK"],
            parents: &[1, 10, 0],
            children: &[1, 1],
        },
        SqlIsolation::RepeatableRead,
    );
}

#[test]
fn inflight_child_then_parent_delete_serializable() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "INSERT INTO c VALUES(1,1);",
            second: "DELETE FROM p WHERE id=1;",
            extra: None,
            rollback: false,
            wait: true,
            snapshot: false,
            expected: ["OK", "23503", "OK", "OK", "OK"],
            parents: &[1, 10, 0],
            children: &[1, 1],
        },
        SqlIsolation::Serializable,
    );
}

#[test]
fn inflight_child_then_parent_key_update_read_committed() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "INSERT INTO c VALUES(1,1);",
            second: "UPDATE p SET id=2 WHERE id=1;",
            extra: None,
            rollback: false,
            wait: true,
            snapshot: false,
            expected: ["OK", "23503", "OK", "OK", "OK"],
            parents: &[1, 10, 0],
            children: &[1, 1],
        },
        SqlIsolation::ReadCommitted,
    );
}

#[test]
fn inflight_child_then_parent_key_update_repeatable_read() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "INSERT INTO c VALUES(1,1);",
            second: "UPDATE p SET id=2 WHERE id=1;",
            extra: None,
            rollback: false,
            wait: true,
            snapshot: false,
            expected: ["OK", "23503", "OK", "OK", "OK"],
            parents: &[1, 10, 0],
            children: &[1, 1],
        },
        SqlIsolation::RepeatableRead,
    );
}

#[test]
fn inflight_child_then_parent_key_update_serializable() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "INSERT INTO c VALUES(1,1);",
            second: "UPDATE p SET id=2 WHERE id=1;",
            extra: None,
            rollback: false,
            wait: true,
            snapshot: false,
            expected: ["OK", "23503", "OK", "OK", "OK"],
            parents: &[1, 10, 0],
            children: &[1, 1],
        },
        SqlIsolation::Serializable,
    );
}

#[test]
fn inflight_parent_delete_rollback_then_child_read_committed() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "DELETE FROM p WHERE id=1;",
            second: "INSERT INTO c VALUES(1,1);",
            extra: None,
            rollback: true,
            wait: true,
            snapshot: false,
            expected: ["OK", "OK", "OK", "OK", "OK"],
            parents: &[1, 10, 0],
            children: &[1, 1],
        },
        SqlIsolation::ReadCommitted,
    );
}

#[test]
fn inflight_parent_delete_rollback_then_child_repeatable_read() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "DELETE FROM p WHERE id=1;",
            second: "INSERT INTO c VALUES(1,1);",
            extra: None,
            rollback: true,
            wait: true,
            snapshot: false,
            expected: ["OK", "OK", "OK", "OK", "OK"],
            parents: &[1, 10, 0],
            children: &[1, 1],
        },
        SqlIsolation::RepeatableRead,
    );
}

#[test]
fn inflight_parent_delete_rollback_then_child_serializable() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "DELETE FROM p WHERE id=1;",
            second: "INSERT INTO c VALUES(1,1);",
            extra: None,
            rollback: true,
            wait: true,
            snapshot: false,
            expected: ["OK", "OK", "OK", "OK", "OK"],
            parents: &[1, 10, 0],
            children: &[1, 1],
        },
        SqlIsolation::Serializable,
    );
}

#[test]
fn inflight_parent_delete_then_child_read_committed() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "DELETE FROM p WHERE id=1;",
            second: "INSERT INTO c VALUES(1,1);",
            extra: None,
            rollback: false,
            wait: true,
            snapshot: false,
            expected: ["OK", "23503", "OK", "OK", "OK"],
            parents: &[],
            children: &[],
        },
        SqlIsolation::ReadCommitted,
    );
}

#[test]
fn inflight_parent_delete_then_child_repeatable_read() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "DELETE FROM p WHERE id=1;",
            second: "INSERT INTO c VALUES(1,1);",
            extra: None,
            rollback: false,
            wait: true,
            snapshot: false,
            expected: ["OK", "40001", "OK", "OK", "OK"],
            parents: &[],
            children: &[],
        },
        SqlIsolation::RepeatableRead,
    );
}

#[test]
fn inflight_parent_delete_then_child_serializable() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "DELETE FROM p WHERE id=1;",
            second: "INSERT INTO c VALUES(1,1);",
            extra: None,
            rollback: false,
            wait: true,
            snapshot: false,
            expected: ["OK", "40001", "OK", "OK", "OK"],
            parents: &[],
            children: &[],
        },
        SqlIsolation::Serializable,
    );
}

#[test]
fn inflight_parent_key_update_then_child_read_committed() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "UPDATE p SET id=2 WHERE id=1;",
            second: "INSERT INTO c VALUES(1,1);",
            extra: None,
            rollback: false,
            wait: true,
            snapshot: false,
            expected: ["OK", "23503", "OK", "OK", "OK"],
            parents: &[2, 10, 0],
            children: &[],
        },
        SqlIsolation::ReadCommitted,
    );
}

#[test]
fn inflight_parent_key_update_then_child_repeatable_read() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "UPDATE p SET id=2 WHERE id=1;",
            second: "INSERT INTO c VALUES(1,1);",
            extra: None,
            rollback: false,
            wait: true,
            snapshot: false,
            expected: ["OK", "40001", "OK", "OK", "OK"],
            parents: &[2, 10, 0],
            children: &[],
        },
        SqlIsolation::RepeatableRead,
    );
}

#[test]
fn inflight_parent_key_update_then_child_serializable() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "UPDATE p SET id=2 WHERE id=1;",
            second: "INSERT INTO c VALUES(1,1);",
            extra: None,
            rollback: false,
            wait: true,
            snapshot: false,
            expected: ["OK", "40001", "OK", "OK", "OK"],
            parents: &[2, 10, 0],
            children: &[],
        },
        SqlIsolation::Serializable,
    );
}

#[test]
fn parent_nonkey_update_vs_child_read_committed() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "UPDATE p SET v=v+1 WHERE id=1;",
            second: "INSERT INTO c VALUES(1,1);",
            extra: None,
            rollback: false,
            wait: false,
            snapshot: false,
            expected: ["OK", "OK", "OK", "OK", "OK"],
            parents: &[1, 10, 1],
            children: &[1, 1],
        },
        SqlIsolation::ReadCommitted,
    );
}

#[test]
fn parent_nonkey_update_vs_child_repeatable_read() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "UPDATE p SET v=v+1 WHERE id=1;",
            second: "INSERT INTO c VALUES(1,1);",
            extra: None,
            rollback: false,
            wait: false,
            snapshot: false,
            expected: ["OK", "OK", "OK", "OK", "OK"],
            parents: &[1, 10, 1],
            children: &[1, 1],
        },
        SqlIsolation::RepeatableRead,
    );
}

#[test]
fn parent_nonkey_update_vs_child_serializable() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "UPDATE p SET v=v+1 WHERE id=1;",
            second: "INSERT INTO c VALUES(1,1);",
            extra: None,
            rollback: false,
            wait: false,
            snapshot: false,
            expected: ["OK", "OK", "OK", "OK", "OK"],
            parents: &[1, 10, 1],
            children: &[1, 1],
        },
        SqlIsolation::Serializable,
    );
}

#[test]
fn parent_unique_col_update_vs_child_read_committed() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "INSERT INTO c VALUES(1,1);",
            second: "UPDATE p SET code=20 WHERE id=1;",
            extra: None,
            rollback: false,
            wait: true,
            snapshot: false,
            expected: ["OK", "OK", "OK", "OK", "OK"],
            parents: &[1, 20, 0],
            children: &[1, 1],
        },
        SqlIsolation::ReadCommitted,
    );
}

#[test]
fn parent_unique_col_update_vs_child_repeatable_read() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "INSERT INTO c VALUES(1,1);",
            second: "UPDATE p SET code=20 WHERE id=1;",
            extra: None,
            rollback: false,
            wait: true,
            snapshot: false,
            expected: ["OK", "OK", "OK", "OK", "OK"],
            parents: &[1, 20, 0],
            children: &[1, 1],
        },
        SqlIsolation::RepeatableRead,
    );
}

#[test]
fn parent_unique_col_update_vs_child_serializable() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "INSERT INTO c VALUES(1,1);",
            second: "UPDATE p SET code=20 WHERE id=1;",
            extra: None,
            rollback: false,
            wait: true,
            snapshot: false,
            expected: ["OK", "OK", "OK", "OK", "OK"],
            parents: &[1, 20, 0],
            children: &[1, 1],
        },
        SqlIsolation::Serializable,
    );
}

#[test]
fn two_children_same_parent_read_committed() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "INSERT INTO c VALUES(1,1);",
            second: "INSERT INTO c VALUES(2,1);",
            extra: None,
            rollback: false,
            wait: false,
            snapshot: false,
            expected: ["OK", "OK", "OK", "OK", "OK"],
            parents: &[1, 10, 0],
            children: &[1, 1, 2, 1],
        },
        SqlIsolation::ReadCommitted,
    );
}

#[test]
fn two_children_same_parent_repeatable_read() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "INSERT INTO c VALUES(1,1);",
            second: "INSERT INTO c VALUES(2,1);",
            extra: None,
            rollback: false,
            wait: false,
            snapshot: false,
            expected: ["OK", "OK", "OK", "OK", "OK"],
            parents: &[1, 10, 0],
            children: &[1, 1, 2, 1],
        },
        SqlIsolation::RepeatableRead,
    );
}

#[test]
fn two_children_same_parent_serializable() {
    run(
        Case {
            setup: "CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id)); CREATE TABLE u(id int); INSERT INTO p VALUES(1,10,0);",
            first: "INSERT INTO c VALUES(1,1);",
            second: "INSERT INTO c VALUES(2,1);",
            extra: None,
            rollback: false,
            wait: false,
            snapshot: false,
            expected: ["OK", "OK", "OK", "OK", "OK"],
            parents: &[1, 10, 0],
            children: &[1, 1, 2, 1],
        },
        SqlIsolation::Serializable,
    );
}
