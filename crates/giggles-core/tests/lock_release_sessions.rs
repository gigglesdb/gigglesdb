//! Real-session coverage for transaction-end and savepoint lock release.
#![cfg(feature = "engine-rocksdb")]

mod common;

use giggles_core::relational::{SqlResult, Value};
use giggles_core::{Admin, Cancellation, Engine, Limits, SqlSession, SqlTableLockMode, sqlstate};
use std::path::PathBuf;
use std::sync::{Arc, Barrier, mpsc};
use std::thread;
use std::time::{Duration, Instant};

const WAIT: Duration = Duration::from_secs(8);

struct Fixture {
    admin: Option<Arc<Admin>>,
    path: PathBuf,
}

impl Fixture {
    fn new(tables: usize, rows: usize) -> Self {
        let path = common::fresh("lock-release-sessions");
        let admin = Arc::new(Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap());
        let mut setup = admin.sql_session();
        let values = (1..=rows)
            .map(|id| format!("({id},0)"))
            .collect::<Vec<_>>()
            .join(",");
        for table in 0..tables {
            run(
                &mut setup,
                &format!("CREATE TABLE t{table}(id int PRIMARY KEY, v int)"),
            );
            run(&mut setup, &format!("INSERT INTO t{table} VALUES {values}"));
        }
        Self {
            admin: Some(admin),
            path,
        }
    }

    fn admin(&self) -> &Arc<Admin> {
        self.admin.as_ref().unwrap()
    }

    fn queued(&self, count: usize) {
        let deadline = Instant::now() + WAIT;
        loop {
            if self.admin().waiting_lock_count() == count {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "expected {count} queued requests"
            );
            thread::sleep(Duration::from_millis(1));
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.admin.take();
        if !thread::panicking() {
            std::fs::remove_dir_all(&self.path).unwrap();
        }
    }
}

fn run(session: &mut SqlSession, sql: &str) -> SqlResult {
    session
        .execute(sql, &[], None)
        .unwrap_or_else(|err| panic!("{sql}: {err:?}"))
}

fn ints(result: SqlResult) -> Vec<i64> {
    result
        .rows
        .into_iter()
        .flat_map(|row| row.values)
        .map(|value| match value {
            Value::Int(n) => n,
            other => panic!("expected integer, got {other:?}"),
        })
        .collect()
}

type Worker<T> = (thread::JoinHandle<()>, mpsc::Receiver<T>);

fn start<T: Send + 'static>(
    fixture: &Fixture,
    f: impl FnOnce(&mut SqlSession) -> T + Send + 'static,
) -> Worker<T> {
    let (send, recv) = mpsc::channel();
    let admin = fixture.admin().clone();
    let worker = thread::spawn(move || {
        let cancel = Cancellation::new(None);
        cancel.set_lock_timeout(Some(WAIT * 2));
        let result = cancel.scope(|| f(&mut admin.sql_session()));
        send.send(result).unwrap();
    });
    (worker, recv)
}

fn finish_before<T>(worker: Worker<T>, deadline: Instant) -> T {
    let result = worker
        .1
        .recv_timeout(deadline.saturating_duration_since(Instant::now()))
        .expect("SQL session failed to finish");
    worker.0.join().unwrap();
    result
}

fn finish<T>(worker: Worker<T>) -> T {
    finish_before(worker, Instant::now() + WAIT)
}

#[test]
fn waiter_wakes_after_commit_or_rollback() {
    for (waiter_sql, increment) in [
        ("UPDATE t0 SET v=v+1 WHERE id=1 RETURNING v", 1),
        ("SELECT v FROM t0 WHERE id=1 FOR UPDATE", 0),
        ("SELECT v FROM t0 WHERE id=1 FOR SHARE", 0),
    ] {
        for (end, committed_value) in [("COMMIT", 10), ("ROLLBACK", 0)] {
            let fixture = Fixture::new(1, 1);
            let (ready, held) = mpsc::channel();
            let (end_txn, end_request) = mpsc::channel();
            let holder = start(&fixture, move |session| {
                run(session, "BEGIN");
                run(session, "UPDATE t0 SET v=10 WHERE id=1");
                ready.send(()).unwrap();
                run(session, end_request.recv_timeout(WAIT).unwrap());
            });
            held.recv_timeout(WAIT).unwrap();
            let waiter = start(&fixture, move |session| {
                run(session, "BEGIN");
                let result = ints(run(session, waiter_sql));
                run(session, "COMMIT");
                result
            });
            fixture.queued(1);
            end_txn.send(end).unwrap();
            finish(holder);
            assert_eq!(
                finish(waiter),
                [committed_value + increment],
                "{waiter_sql} / {end}"
            );
            assert_eq!(fixture.admin().waiting_lock_count(), 0);
        }
    }
}

#[test]
fn crossed_row_updates_report_one_deadlock() {
    let fixture = Fixture::new(1, 9);
    // Vary the pair so the sessions exercise different hash placements.
    for other in 2..=9 {
        let start_update = |first, second| {
            let (ready, held) = mpsc::channel();
            let (cross, proceed) = mpsc::channel();
            let worker = start(&fixture, move |session| {
                run(session, "BEGIN");
                let update = |id| format!("UPDATE t0 SET v=v+1 WHERE id={id}");
                run(session, &update(first));
                ready.send(()).unwrap();
                proceed.recv_timeout(WAIT).unwrap();
                match session.execute(&update(second), &[], None) {
                    Ok(_) => {
                        run(session, "COMMIT");
                        None
                    }
                    Err(err) => {
                        run(session, "ROLLBACK");
                        Some(sqlstate(&err))
                    }
                }
            });
            held.recv_timeout(WAIT).unwrap();
            (worker, cross)
        };
        let (a, cross_a) = start_update(1, other);
        let (b, cross_b) = start_update(other, 1);
        cross_a.send(()).unwrap();
        fixture.queued(1);
        cross_b.send(()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        assert_eq!(finish_before(b, deadline), Some("40P01"));
        assert_eq!(finish_before(a, deadline), None);
        assert_eq!(fixture.admin().waiting_lock_count(), 0);
    }
}

#[test]
fn rollback_to_releases_only_post_savepoint_rows() {
    let fixture = Fixture::new(1, 2);
    let (ready, held) = mpsc::channel();
    let (action, next) = mpsc::channel();
    let holder = start(&fixture, move |session| {
        run(session, "BEGIN");
        run(session, "UPDATE t0 SET v=10 WHERE id=1");
        run(session, "SAVEPOINT s");
        run(session, "UPDATE t0 SET v=20 WHERE id=2");
        ready.send(()).unwrap();
        run(session, next.recv_timeout(WAIT).unwrap());
        run(session, next.recv_timeout(WAIT).unwrap());
    });
    held.recv_timeout(WAIT).unwrap();
    let before = start(&fixture, |session| {
        ints(run(session, "SELECT v FROM t0 WHERE id=1 FOR SHARE"))
    });
    let after = start(&fixture, |session| {
        ints(run(session, "SELECT v FROM t0 WHERE id=2 FOR SHARE"))
    });
    fixture.queued(2);
    action.send("ROLLBACK TO s").unwrap();
    assert_eq!(finish(after), [0]);
    assert_eq!(fixture.admin().waiting_lock_count(), 1);
    action.send("COMMIT").unwrap();
    assert_eq!(finish(before), [10]);
    finish(holder);
}

#[test]
fn commit_wakes_waiters_across_many_rows_and_tables() {
    let fixture = Fixture::new(2, 100);
    let (ready, held) = mpsc::channel();
    let (commit, proceed) = mpsc::channel();
    let holder = start(&fixture, move |session| {
        run(session, "BEGIN");
        run(session, "UPDATE t0 SET v=v+1");
        run(session, "UPDATE t1 SET v=v+1");
        ready.send(()).unwrap();
        proceed.recv_timeout(WAIT).unwrap();
        run(session, "COMMIT");
    });
    held.recv_timeout(WAIT).unwrap();
    let waiters = (0..2)
        .flat_map(|table| [1, 37, 93].map(move |row| (table, row)))
        .map(|(table, row)| {
            start(&fixture, move |session| {
                ints(run(
                    session,
                    &format!("SELECT v FROM t{table} WHERE id={row} FOR UPDATE"),
                ))
            })
        })
        .collect::<Vec<_>>();
    fixture.queued(waiters.len());
    commit.send(()).unwrap();
    finish(holder);
    for waiter in waiters {
        assert_eq!(finish(waiter), [1]);
    }
    assert_eq!(fixture.admin().waiting_lock_count(), 0);
}

#[test]
fn hot_row_sessions_finish_and_keep_exact_counts() {
    const SESSIONS: usize = 16;
    const ITERATIONS: usize = 24;
    const TABLES: usize = 3;
    const ROWS: usize = 4;
    const KEYS: usize = TABLES * ROWS;

    let fixture = Fixture::new(TABLES, ROWS);
    let start_together = Arc::new(Barrier::new(SESSIONS + 1));
    let workers = (0..SESSIONS)
        .map(|worker| {
            let start_together = start_together.clone();
            start(&fixture, move |session| {
                let mut rng = (worker as u64 + 1) * 987_619;
                let mut increments = [0; KEYS];
                let mut next_key = || {
                    rng ^= rng << 13;
                    rng ^= rng >> 7;
                    rng ^= rng << 17;
                    rng as usize % KEYS
                };
                start_together.wait();
                for _ in 0..ITERATIONS {
                    let keys = [next_key(), next_key(), next_key()];
                    loop {
                        run(session, "BEGIN");
                        let updates = keys.iter().try_for_each(|&key| {
                            let (table, id) = (key / ROWS, key % ROWS + 1);
                            session
                                .execute(
                                    &format!("UPDATE t{table} SET v=v+1 WHERE id={id}"),
                                    &[],
                                    None,
                                )
                                .map(|_| ())
                        });
                        match updates {
                            Ok(()) => {
                                run(session, "COMMIT");
                                for key in keys {
                                    increments[key] += 1;
                                }
                                break;
                            }
                            Err(err) if sqlstate(&err) == "40P01" => {
                                run(session, "ROLLBACK");
                            }
                            Err(err) => panic!("unexpected update error: {err:?}"),
                        }
                    }
                }
                increments
            })
        })
        .collect::<Vec<_>>();

    let deadline = Instant::now() + Duration::from_secs(45);
    start_together.wait();
    let mut totals = [0; KEYS];
    for worker in workers {
        for (total, increment) in totals.iter_mut().zip(finish_before(worker, deadline)) {
            *total += increment;
        }
    }
    assert_eq!(
        totals.iter().sum::<i64>(),
        (SESSIONS * ITERATIONS * 3) as i64
    );
    assert_eq!(fixture.admin().waiting_lock_count(), 0);

    let mut verify = fixture.admin().sql_session();
    run(&mut verify, "BEGIN");
    for table in 0..TABLES {
        // NOWAIT probes each row and takes the table's strongest lock to
        // detect residual row or table holders without waiting for cleanup.
        let result = run(
            &mut verify,
            &format!("SELECT v FROM t{table} ORDER BY id FOR UPDATE NOWAIT"),
        );
        assert_eq!(ints(result), totals[table * ROWS..(table + 1) * ROWS]);
        verify
            .lock_tables(
                &[format!("t{table}")],
                SqlTableLockMode::AccessExclusive,
                true,
            )
            .unwrap();
    }
    run(&mut verify, "ROLLBACK");
}
