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

// These review schedules are not in the captured oracle matrix. Source basis:
// PostgreSQL REL_18_6 src/backend/executor/nodeLockRows.c (TM_Updated/TM_Deleted)
// and src/backend/access/heap/heapam.c (heap_lock_tuple, LockTupleKeyShare).
// PG18 heap_lock_tuple/ExecLockRows reject a
// deleted/key-updated snapshot tuple, but follow non-key update chains.
fn fixed_snapshot_chain(level: SqlIsolation, changes: &[&str], expected: &str) {
    let f = Fixture::new("CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(code)); INSERT INTO p VALUES(1,10,0)");
    let mut child = f.admin().sql_session();
    child.set_isolation(level);
    ok(&mut child, "BEGIN");
    ok(&mut child, "SELECT * FROM p");
    let mut writer = f.admin().sql_session();
    // Autocommit changes expose individual versions; explicit BEGIN/COMMIT
    // also exercises multiple changes folded into one committed version.
    for sql in changes { ok(&mut writer, sql); }
    let result = execute(&mut child, "INSERT INTO c VALUES(1,10)");
    let end = execute(&mut child, "COMMIT");
    check(&result, expected, "snapshot FK check");
    check(&end, "OK", "snapshot end");
    let children = rows(&mut writer, "SELECT id,p FROM c");
    assert_eq!(children.len(), usize::from(expected == "OK"));
}

#[test]
fn delete_reinsert_identical_repeatable_read() {
    fixed_snapshot_chain(SqlIsolation::RepeatableRead,
        &["DELETE FROM p WHERE id=1", "INSERT INTO p VALUES(1,10,0)"], "40001");
}
#[test]
fn delete_reinsert_identical_serializable() {
    fixed_snapshot_chain(SqlIsolation::Serializable,
        &["DELETE FROM p WHERE id=1", "INSERT INTO p VALUES(1,10,0)"], "40001");
}
#[test]
fn key_change_away_and_back_repeatable_read() {
    fixed_snapshot_chain(SqlIsolation::RepeatableRead,
        &["UPDATE p SET code=20 WHERE id=1", "UPDATE p SET code=10 WHERE id=1"], "40001");
}
#[test]
fn nonkey_update_chain_repeatable_read() {
    fixed_snapshot_chain(SqlIsolation::RepeatableRead,
        &["UPDATE p SET v=1 WHERE id=1", "UPDATE p SET v=2 WHERE id=1"], "OK");
}

#[test]
fn replacement_unique_witness_is_key_share_protected() {
    let f = Fixture::new("CREATE TABLE p(id int PRIMARY KEY, code int UNIQUE, v int); CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(code)); INSERT INTO p VALUES(1,10,0)");
    let mut writer = f.admin().sql_session();
    ok(&mut writer, "BEGIN");
    ok(&mut writer, "UPDATE p SET code=20 WHERE id=1");
    let admin = f.admin().clone();
    let (started, ready) = mpsc::channel();
    let (inserted, result) = mpsc::channel();
    let (finish, release) = mpsc::channel();
    let child = thread::spawn(move || {
        let mut s = admin.sql_session();
        ok(&mut s, "BEGIN");
        started.send(()).unwrap();
        let insert = execute(&mut s, "INSERT INTO c VALUES(1,10)");
        inserted.send(insert).unwrap();
        release.recv().unwrap();
        execute(&mut s, "COMMIT")
    });
    ready.recv_timeout(Duration::from_secs(10)).unwrap();
    let early = result.recv_timeout(Duration::from_millis(150));
    ok(&mut writer, "INSERT INTO p VALUES(2,10,0)");
    ok(&mut writer, "COMMIT");
    let insert = early.clone().unwrap_or_else(|_| result.recv_timeout(Duration::from_secs(10)).unwrap());
    let admin = f.admin().clone();
    let (deleted, deletion) = mpsc::channel();
    let deleter = thread::spawn(move || {
        let mut s = admin.sql_session();
        let delete = execute(&mut s, "DELETE FROM p WHERE id=2");
        deleted.send(delete.clone()).unwrap();
        delete
    });
    let premature_delete = deletion.recv_timeout(Duration::from_millis(150));
    finish.send(()).unwrap();
    let end = child.join().unwrap();
    let delete = deleter.join().unwrap();
    // PG may reject the original vanished witness with 23503. If the lookup
    // follows its replacement, that replacement must remain locked until end.
    assert!(matches!(early, Err(mpsc::RecvTimeoutError::Timeout)));
    check(&end, "OK", "child end");
    match insert {
        Ok(()) => {
            assert!(matches!(premature_delete, Err(mpsc::RecvTimeoutError::Timeout)));
            check(&delete, "23503", "replacement delete");
        }
        Err(CoreError::Sql(SqlError::ForeignKeyViolation)) => check(&delete, "OK", "replacement delete"),
        other => panic!("unexpected child result: {other:?}"),
    }
    let parents = rows(&mut writer, "SELECT code FROM p");
    for child in rows(&mut writer, "SELECT p FROM c") {
        assert!(parents.contains(&child), "orphan: {child:?}");
    }
}

#[test]
#[ignore = "GD-0084 A2 known divergence: a single-transaction delete+reinsert of identical parent keys is indistinguishable from a non-key update; PostgreSQL 18.6 raises 40001"]
fn single_transaction_delete_reinsert_identical_repeatable_read() {
    fixed_snapshot_chain(SqlIsolation::RepeatableRead,
        &["BEGIN", "DELETE FROM p WHERE id=1", "INSERT INTO p VALUES(1,10,0)", "COMMIT"], "40001");
}
#[test]
#[ignore = "GD-0084 A2 known divergence: a single-transaction delete+reinsert of identical parent keys is indistinguishable from a non-key update; PostgreSQL 18.6 raises 40001"]
fn single_transaction_delete_reinsert_identical_serializable() {
    fixed_snapshot_chain(SqlIsolation::Serializable,
        &["BEGIN", "DELETE FROM p WHERE id=1", "INSERT INTO p VALUES(1,10,0)", "COMMIT"], "40001");
}
#[test]
#[ignore = "GD-0084 A2 known divergence: a single-transaction delete+reinsert of identical parent keys is indistinguishable from a non-key update; PostgreSQL 18.6 raises 40001"]
fn single_transaction_key_change_away_and_back_repeatable_read() {
    fixed_snapshot_chain(SqlIsolation::RepeatableRead,
        &["BEGIN", "UPDATE p SET code=20 WHERE id=1", "UPDATE p SET code=10 WHERE id=1", "COMMIT"], "40001");
}

#[test]
fn self_reference_to_an_insert_does_not_hide_its_missing_parent() {
    let fixture = Fixture::new(
        "CREATE TABLE sr4(id int PRIMARY KEY, parent int REFERENCES sr4(id));
         INSERT INTO sr4 VALUES (1,NULL)",
    );
    let mut session = fixture.admin().sql_session();
    for sql in [
        "INSERT INTO sr4 VALUES (2,3),(3,99)",
        "INSERT INTO sr4 VALUES (2,3),(3,99) ON CONFLICT (id) DO UPDATE SET parent=excluded.parent",
        "INSERT INTO sr4 SELECT g, CASE WHEN g=3 THEN 99 WHEN g=2 THEN 3 END
         FROM generate_series(2,202) g WHERE g<>99
         ON CONFLICT (id) DO UPDATE SET parent=excluded.parent",
    ] {
        let error = session.execute(sql, &[], None).unwrap_err();
        assert_eq!(giggles_core::sqlstate(&error), "23503", "{sql}: {error:?}");
        assert_eq!(rows(&mut session, "SELECT id FROM sr4"), [vec![1]]);
    }
}

#[test]
fn paged_upsert_checks_final_references_and_carries_deferred_checks_on_rekey() {
    let fixture = Fixture::new(
        "CREATE TABLE sr4(id int PRIMARY KEY, parent int REFERENCES sr4(id));
         INSERT INTO sr4 VALUES (1,NULL);
         CREATE TABLE dp(id int PRIMARY KEY);
         CREATE TABLE dc(id int PRIMARY KEY, p int REFERENCES dp(id) DEFERRABLE INITIALLY DEFERRED);
         INSERT INTO dp VALUES(1); INSERT INTO dc VALUES(1,1)",
    );
    let mut session = fixture.admin().sql_session();
    ok(&mut session, "INSERT INTO sr4 SELECT g, CASE WHEN g=1 THEN 202 END
        FROM generate_series(1,202) g ON CONFLICT (id) DO UPDATE SET parent=excluded.parent");
    assert_eq!(rows(&mut session, "SELECT parent FROM sr4 WHERE id=1"), [vec![202]]);

    ok(&mut session, "BEGIN");
    ok(&mut session, "INSERT INTO dc SELECT g, CASE WHEN g=1 THEN 99 ELSE 1 END
        FROM generate_series(1,202) g ON CONFLICT (id) DO UPDATE SET id=303, p=excluded.p");
    ok(&mut session, "UPDATE dc SET id=304 WHERE id=303");
    let error = session.execute("COMMIT", &[], None).unwrap_err();
    assert_eq!(giggles_core::sqlstate(&error), "23503", "{error:?}");
    assert_eq!(rows(&mut session, "SELECT id FROM dc"), [vec![1]]);
}

#[test]
fn paged_upsert_retains_outbound_checks_when_cascade_rewrites_the_row() {
    let fixture = Fixture::new(
        "CREATE TABLE p(id int PRIMARY KEY);
         CREATE TABLE ar(id int PRIMARY KEY, parent int REFERENCES ar(id) ON UPDATE CASCADE,
             p int REFERENCES p(id));
         INSERT INTO p VALUES(1); INSERT INTO ar VALUES(1,NULL,1)",
    );
    let mut session = fixture.admin().sql_session();
    let upsert = |invalid| format!(
        "INSERT INTO ar SELECT g, CASE WHEN g=2 THEN 1 END, CASE WHEN g=2 THEN {invalid} ELSE 1 END
         FROM generate_series(1,201) g ORDER BY g DESC
         ON CONFLICT (id) DO UPDATE SET id=CASE WHEN ar.id=1 THEN 1000 ELSE ar.id END, p=excluded.p"
    );
    let error = session.execute(&upsert(99), &[], None).unwrap_err();
    assert_eq!(giggles_core::sqlstate(&error), "23503", "{error:?}");
    assert_eq!(rows(&mut session, "SELECT id FROM ar"), [vec![1]]);

    ok(&mut session, "INSERT INTO ar VALUES (2,1,1)");
    let error = session.execute(&upsert(99), &[], None).unwrap_err();
    assert_eq!(giggles_core::sqlstate(&error), "23503", "{error:?}");
    assert_eq!(rows(&mut session, "SELECT id FROM ar ORDER BY id"), [vec![1], vec![2]]);
    ok(&mut session, &upsert(1));
    assert_eq!(rows(&mut session, "SELECT parent FROM ar WHERE id=2"), [vec![1000]]);
}
