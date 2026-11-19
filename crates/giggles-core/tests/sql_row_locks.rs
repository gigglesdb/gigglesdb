#![cfg(feature = "engine-rocksdb")]
mod common;
use giggles_core::{Admin, Engine, Limits, SqlSession};
use giggles_core::relational::{SqlResult, Value};
fn run(s: &mut SqlSession, sql: &str) -> SqlResult {
    s.execute(sql, &[], None).unwrap_or_else(|e| panic!("{sql}: {e:?}"))
}
fn fixture() -> (Admin, std::path::PathBuf) {
    let path = common::fresh("row-locks");
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let mut s = admin.sql_session();
    run(&mut s, "CREATE TABLE lk(id int PRIMARY KEY, u int UNIQUE, v int)");
    run(&mut s, "INSERT INTO lk VALUES(1,10,0),(2,20,0),(3,30,0)");
    (admin, path)
}
#[test]
fn four_by_four_nowait_matrix() {
    let (admin, path) = fixture();
    let modes = ["KEY SHARE", "SHARE", "NO KEY UPDATE", "UPDATE"];
    for (h, held) in modes.iter().enumerate() {
        for (r, requested) in modes.iter().enumerate() {
            let mut a = admin.sql_session(); let mut b = admin.sql_session();
            run(&mut a, "BEGIN"); run(&mut b, "BEGIN");
            run(&mut a, &format!("SELECT id FROM lk WHERE id=1 FOR {held}"));
            let result = b.execute(&format!("SELECT id FROM lk WHERE id=1 FOR {requested} NOWAIT"), &[], None);
            let conflict = h == 3 || r == 3 || h == 2 && r != 0 || r == 2 && h != 0;
            if conflict { assert_eq!(giggles_core::sqlstate(result.as_ref().unwrap_err()), "55P03"); }
            assert_eq!(result.is_err(), conflict, "held {held}, requested {requested}: {result:?}");
            run(&mut a, "ROLLBACK"); run(&mut b, "ROLLBACK");
        }
    }
    drop(admin); std::fs::remove_dir_all(path).unwrap();
}
#[test]
fn skip_locked_fills_limit_and_offset_consumes_locks() {
    let (admin, path) = fixture();
    let mut a = admin.sql_session(); let mut b = admin.sql_session();
    run(&mut a, "BEGIN"); run(&mut b, "BEGIN");
    run(&mut a, "SELECT id FROM lk WHERE id=1 FOR UPDATE");
    let result = run(&mut b, "SELECT id FROM lk ORDER BY id LIMIT 2 FOR UPDATE SKIP LOCKED");
    assert_eq!(result.rows.iter().map(|r| r.values.clone()).collect::<Vec<_>>(), vec![vec![Value::Int(2)], vec![Value::Int(3)]]);
    run(&mut a, "ROLLBACK"); run(&mut b, "ROLLBACK");
    run(&mut a, "BEGIN");
    let result = run(&mut a, "SELECT id FROM lk ORDER BY id LIMIT 1 OFFSET 1 FOR UPDATE");
    assert_eq!(result.rows[0].values, vec![Value::Int(2)]);
    assert!(b.execute("SELECT id FROM lk WHERE id=1 FOR UPDATE NOWAIT", &[], None).is_err());
    run(&mut b, "SELECT id FROM lk WHERE id=3 FOR UPDATE NOWAIT");
    run(&mut a, "ROLLBACK");
    drop(a); drop(b); drop(admin); std::fs::remove_dir_all(path).unwrap();
}

fn limited(s: &mut SqlSession, sql: &str) -> Result<SqlResult, giggles_core::CoreError> {
    let cancel = giggles_core::Cancellation::new(None);
    cancel.set_lock_timeout(Some(std::time::Duration::from_millis(40)));
    cancel.scope(|| s.execute(sql, &[], None))
}
#[test]
fn writer_strengths() {
    let (admin, path) = fixture();
    for held in ["KEY SHARE", "SHARE", "NO KEY UPDATE", "UPDATE"] {
        for (sql, key_change) in [("UPDATE lk SET v=v+1 WHERE id=1", false),
            ("UPDATE lk SET u=u WHERE id=1", false), ("UPDATE lk SET u=99 WHERE id=1", true),
            ("UPDATE lk SET id=99 WHERE id=1", true), ("DELETE FROM lk WHERE id=1", true)] {
            let mut a = admin.sql_session(); let mut b = admin.sql_session();
            run(&mut a, "BEGIN"); run(&mut b, "BEGIN");
            run(&mut a, &format!("SELECT id FROM lk WHERE id=1 FOR {held}"));
            let result = limited(&mut b, sql);
            let conflict = held != "KEY SHARE" || key_change;
            assert_eq!(result.is_err(), conflict, "held {held}, {sql}: {result:?}");
            if let Err(e) = result { assert_eq!(giggles_core::sqlstate(&e), "55P03", "{e:?}"); }
            run(&mut a, "ROLLBACK"); run(&mut b, "ROLLBACK");
        }
    }
    drop(admin); std::fs::remove_dir_all(path).unwrap();
}
#[test]
fn wait_refresh_and_holder_end() {
    use std::{sync::mpsc, thread, time::Duration};
    for (change, end, fixed, expected_rows) in [
        ("", "COMMIT", false, 1), ("", "ROLLBACK", false, 1), ("", "COMMIT", true, 1),
        ("UPDATE lk SET v=1 WHERE id=1", "COMMIT", false, 0),
        ("DELETE FROM lk WHERE id=1", "COMMIT", false, 0),
        ("UPDATE lk SET v=1 WHERE id=1", "COMMIT", true, 0),
    ] {
        let (admin, path) = fixture();
        let mut a = admin.sql_session(); let mut b = admin.sql_session();
        if fixed { b.set_isolation(giggles_core::SqlIsolation::RepeatableRead); }
        run(&mut a, "BEGIN"); run(&mut b, "BEGIN");
        run(&mut b, "SELECT * FROM lk WHERE id=1");
        run(&mut a, "SELECT id FROM lk WHERE id=1 FOR UPDATE");
        if !change.is_empty() { run(&mut a, change); }
        let (send, receive) = mpsc::channel();
        let handle = thread::spawn(move || {
            let cancel = giggles_core::Cancellation::new(None);
            cancel.set_lock_timeout(Some(Duration::from_secs(3)));
            let result = cancel.scope(|| b.execute("SELECT id FROM lk WHERE id=1 AND v=0 FOR UPDATE", &[], None));
            send.send(result).unwrap(); b
        });
        assert!(receive.recv_timeout(Duration::from_millis(50)).is_err());
        run(&mut a, end);
        let result = receive.recv_timeout(Duration::from_secs(4)).unwrap();
        if fixed && !change.is_empty() { assert_eq!(giggles_core::sqlstate(&result.unwrap_err()), "40001"); }
        else { assert_eq!(result.unwrap().rows.len(), expected_rows); }
        let mut b = handle.join().unwrap(); run(&mut b, "ROLLBACK");
        drop(a); drop(b); drop(admin); std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn syntax_and_restrictions() {
    let (admin, path) = fixture(); let mut s = admin.sql_session();
    for (sql, code, message) in [
        ("SELECT id FROM lk a FOR SHARE OF lk", "42P01", "relation \"lk\" in FOR SHARE clause not found in FROM clause"),
        ("SELECT DISTINCT id FROM lk FOR UPDATE", "0A000", "FOR UPDATE is not allowed with DISTINCT clause"),
        ("SELECT id FROM lk GROUP BY id FOR UPDATE", "0A000", "FOR UPDATE is not allowed with GROUP BY clause"),
        ("SELECT count(*) FROM lk FOR UPDATE", "0A000", "FOR UPDATE is not allowed with aggregate functions"),
        ("SELECT id FROM lk HAVING true FOR UPDATE", "0A000", "FOR UPDATE is not allowed with HAVING clause"),
        ("SELECT row_number() OVER () FROM lk FOR UPDATE", "0A000", "FOR UPDATE is not allowed with window functions"),
        ("SELECT generate_series(1,2) FOR UPDATE", "0A000", "FOR UPDATE is not allowed with set-returning functions in the target list"),
        ("SELECT id FROM lk UNION SELECT id FROM lk FOR UPDATE", "0A000", "FOR UPDATE is not allowed with UNION/INTERSECT/EXCEPT"),
        ("SELECT a.id FROM lk a LEFT JOIN lk b ON a.id=b.id FOR UPDATE", "0A000", "FOR UPDATE cannot be applied to the nullable side of an outer join"),
    ] {
        let error = s.execute(sql, &[], None).unwrap_err();
        assert_eq!(giggles_core::sqlstate(&error), code, "{sql}: {error:?}");
        assert_eq!(giggles_core::relational::take_sql_error_message().as_deref(), Some(message), "{sql}");
    }
    for sql in ["SELECT id FROM lk LIMIT 1 FOR UPDATE OFFSET 2", "SELECT id FROM lk FOR UPDATE NOWAIT SKIP LOCKED"] {
        let error = s.execute(sql, &[], None).unwrap_err(); assert_eq!(giggles_core::sqlstate(&error), "42601", "{sql}: {error:?}");
    }
    run(&mut s, "SELECT 1 FOR UPDATE");
    run(&mut s, "SELECT id FROM lk FOR UPDATE FOR SHARE");
    run(&mut s, "SELECT a.id FROM lk a LEFT JOIN lk b ON a.id=b.id FOR UPDATE OF a");
    drop(s); drop(admin); std::fs::remove_dir_all(path).unwrap();
}
#[test]
fn derived_marks_keep_unprojected_key() {
    let (admin, path) = fixture(); let mut a = admin.sql_session(); let mut b = admin.sql_session();
    run(&mut a, "BEGIN");
    let result = run(&mut a, "SELECT s.v FROM (SELECT v FROM lk WHERE id=1) s FOR UPDATE OF s OFFSET 0");
    assert_eq!(result.rows[0].values, vec![Value::Int(0)]);
    let error = b.execute("SELECT id FROM lk WHERE id=1 FOR KEY SHARE NOWAIT", &[], None).unwrap_err();
    assert_eq!(giggles_core::sqlstate(&error), "55P03");
    run(&mut a, "ROLLBACK");
    drop(a); drop(b); drop(admin); std::fs::remove_dir_all(path).unwrap();
}
#[test]
fn lock_timeout_and_nowait_leave_no_partial_lock() {
    let (admin, path) = fixture(); let mut a = admin.sql_session(); let mut b = admin.sql_session(); let mut c = admin.sql_session();
    run(&mut a, "BEGIN"); run(&mut b, "BEGIN");
    run(&mut a, "SELECT id FROM lk WHERE id=1 FOR NO KEY UPDATE");
    let error = b.execute("SELECT id FROM lk WHERE id=1 FOR UPDATE NOWAIT", &[], None).unwrap_err();
    assert_eq!(giggles_core::sqlstate(&error), "55P03");
    run(&mut c, "SELECT id FROM lk WHERE id=1 FOR KEY SHARE NOWAIT");
    run(&mut b, "ROLLBACK");
    let error = limited(&mut b, "SELECT id FROM lk WHERE id=1 FOR SHARE").unwrap_err();
    assert_eq!(giggles_core::sqlstate(&error), "55P03");
    assert_eq!(giggles_core::relational::take_sql_error_message().as_deref(), Some("canceling statement due to lock timeout"));
    run(&mut a, "ROLLBACK");
    drop(a); drop(b); drop(c); drop(admin); std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn queue_corpus_schema() {
    let (admin, path) = fixture(); let mut a = admin.sql_session(); let mut b = admin.sql_session();
    run(&mut a, "CREATE TABLE jobs(id int PRIMARY KEY, label text)");
    run(&mut a, "INSERT INTO jobs VALUES(1,'root'),(2,'root')");
    run(&mut a, "BEGIN"); run(&mut b, "BEGIN");
    let sql = "WITH next_job AS (SELECT id FROM jobs WHERE label='root' ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1), claimed AS (UPDATE jobs SET label='running' FROM next_job WHERE jobs.id=next_job.id RETURNING jobs.id,jobs.label) SELECT * FROM claimed ORDER BY id";
    let first = run(&mut a, sql); let second = limited(&mut b, sql).unwrap();
    assert_eq!(first.rows[0].values, vec![Value::Int(1), Value::Text("running".into())]);
    assert_eq!(second.rows[0].values, vec![Value::Int(2), Value::Text("running".into())]);
    run(&mut a, "COMMIT"); run(&mut b, "COMMIT");
    drop(a); drop(b); drop(admin); std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn marked_exists_fails_closed_and_nested_marks_are_discovered() {
    let (admin, path) = fixture(); let mut a = admin.sql_session(); let mut b = admin.sql_session();
    run(&mut a, "BEGIN");
    run(&mut a, "SELECT id FROM lk WHERE id=1 FOR UPDATE");
    let error = b.execute("SELECT 1 WHERE EXISTS(SELECT 1 FROM lk WHERE id=1 FOR UPDATE NOWAIT)", &[], None).unwrap_err();
    assert_eq!(giggles_core::sqlstate(&error), "0A000");
    let error = b.execute("SELECT 1 WHERE 1 IN (SELECT id FROM lk WHERE id=1 FOR UPDATE NOWAIT)", &[], None).unwrap_err();
    assert!(matches!(giggles_core::sqlstate(&error), "55P03" | "0A000"));
    let error = b.execute("SELECT (SELECT id FROM lk WHERE id=1 FOR UPDATE NOWAIT)", &[], None).unwrap_err();
    assert!(matches!(giggles_core::sqlstate(&error), "55P03" | "0A000"));
    run(&mut a, "ROLLBACK");
    drop(a); drop(b); drop(admin); std::fs::remove_dir_all(path).unwrap();
}
#[test]
fn paginated_derived_mark_fails_before_skipping_locked_row() {
    let (admin, path) = fixture(); let mut a = admin.sql_session(); let mut b = admin.sql_session();
    run(&mut a, "BEGIN"); run(&mut a, "SELECT id FROM lk WHERE id=1 FOR UPDATE");
    for suffix in ["FOR UPDATE SKIP LOCKED", "FOR UPDATE OF s SKIP LOCKED"] {
        let error = b.execute(&format!("SELECT s.id FROM (SELECT id FROM lk ORDER BY id LIMIT 1) s {suffix}"), &[], None).unwrap_err();
        assert_eq!(giggles_core::sqlstate(&error), "0A000");
    }
    run(&mut a, "ROLLBACK");
    drop(a); drop(b); drop(admin); std::fs::remove_dir_all(path).unwrap();
}
#[test]
fn unchanged_row_evaluates_volatile_filter_once() {
    let (admin, path) = fixture(); let mut s = admin.sql_session();
    run(&mut s, "CREATE SEQUENCE lk_seq");
    let clock = giggles_core::relational::StatementClock {
        unavailable: None, transaction_us: 100, statement_us: 100, zone: None, wall: None,
    };
    let result = s.execute_at("SELECT id FROM lk WHERE id=1 AND nextval('lk_seq')=1 FOR UPDATE", &[], None, &clock).unwrap();
    assert_eq!(result.rows[0].values, vec![Value::Int(1)]);
    let result = s.execute_at("SELECT currval('lk_seq')", &[], None, &clock).unwrap();
    assert_eq!(result.rows[0].values, vec![Value::Int(1)]);
    drop(s); drop(admin); std::fs::remove_dir_all(path).unwrap();
}
#[test]
fn refreshed_row_rechecks_join_and_derived_filter() {
    use std::{sync::mpsc, thread, time::Duration};
    for sql in [
        "SELECT a.id,a.v,b.v FROM lk a JOIN lk b ON a.v=b.v WHERE a.id=1 AND b.id=2 FOR UPDATE OF a",
        "SELECT s.id FROM (SELECT id,v FROM lk WHERE id=1 AND v=0) s FOR UPDATE",
    ] {
        let (admin, path) = fixture(); let mut a = admin.sql_session(); let mut b = admin.sql_session();
        run(&mut a, "BEGIN"); run(&mut a, "UPDATE lk SET v=1 WHERE id=1");
        let (send, receive) = mpsc::channel();
        let handle = thread::spawn(move || {
            let cancel = giggles_core::Cancellation::new(None);
            cancel.set_lock_timeout(Some(Duration::from_secs(3)));
            let result = cancel.scope(|| b.execute(sql, &[], None));
            send.send(result).unwrap(); b
        });
        assert!(receive.recv_timeout(Duration::from_millis(100)).is_err());
        run(&mut a, "COMMIT");
        assert!(receive.recv_timeout(Duration::from_secs(4)).unwrap().unwrap().rows.is_empty(), "{sql}");
        let b = handle.join().unwrap();
        drop(a); drop(b); drop(admin); std::fs::remove_dir_all(path).unwrap();
    }
}

// PostgreSQL 18.6 oracle: lock/l5-oracle.log (two psql sessions).
#[test]
fn fk_child_insert_obeys_select_key_lock_strength() {
    let (admin, path) = fixture();
    let mut setup = admin.sql_session();
    run(&mut setup, "CREATE TABLE child(id int PRIMARY KEY, parent int REFERENCES lk(id))");
    for mode in ["UPDATE", "NO KEY UPDATE", "KEY SHARE"] {
        let mut a = admin.sql_session();
        let mut b = admin.sql_session();
        run(&mut a, "BEGIN"); run(&mut b, "BEGIN");
        run(&mut a, &format!("SELECT id FROM lk WHERE id=1 FOR {mode}"));
        let result = limited(&mut b, "INSERT INTO child VALUES(1,1)");
        if mode == "UPDATE" {
            assert_eq!(giggles_core::sqlstate(&result.unwrap_err()), "55P03");
        } else { result.unwrap(); }
        run(&mut a, "ROLLBACK"); run(&mut b, "ROLLBACK");
    }
    drop(setup); drop(admin); std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn fk_insert_and_for_update_wait_in_both_orders_without_deadlock() {
    use std::{sync::mpsc, thread, time::Duration};
    for (reverse, delete) in [(false, false), (false, true), (true, false)] {
        let (admin, path) = fixture();
        let mut a = admin.sql_session();
        let mut b = admin.sql_session();
        run(&mut a, "CREATE TABLE child(id int PRIMARY KEY, parent int REFERENCES lk(id))");
        run(&mut a, "BEGIN"); run(&mut b, "BEGIN");
        let select = "SELECT id FROM lk WHERE id=1 FOR UPDATE";
        let insert = "INSERT INTO child VALUES(1,1)";
        run(&mut a, if reverse { insert } else { select });
        let (send, receive) = mpsc::channel();
        let handle = thread::spawn(move || {
            let cancel = giggles_core::Cancellation::new(None);
            cancel.set_lock_timeout(Some(Duration::from_secs(3)));
            let result = cancel.scope(|| b.execute(if reverse { select } else { insert }, &[], None));
            send.send(result).unwrap(); b
        });
        assert!(receive.recv_timeout(Duration::from_millis(50)).is_err());
        if delete { run(&mut a, "DELETE FROM lk WHERE id=1"); }
        run(&mut a, "COMMIT");
        let result = receive.recv_timeout(Duration::from_secs(4)).unwrap();
        if delete { assert_eq!(giggles_core::sqlstate(&result.unwrap_err()), "23503"); }
        else { result.unwrap(); }
        let mut b = handle.join().unwrap(); run(&mut b, "ROLLBACK");
        drop(a); drop(b); drop(admin); std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn delete_and_for_update_wait_in_both_orders_without_deadlock() {
    use std::{sync::mpsc, thread, time::Duration};
    for reverse in [false, true] {
        let (admin, path) = fixture();
        let mut a = admin.sql_session(); let mut b = admin.sql_session();
        run(&mut a, "BEGIN"); run(&mut b, "BEGIN");
        let select = "SELECT id FROM lk WHERE id=1 FOR UPDATE";
        let delete = "DELETE FROM lk WHERE id=1";
        run(&mut a, if reverse { delete } else { select });
        let (send, receive) = mpsc::channel();
        let handle = thread::spawn(move || {
            let cancel = giggles_core::Cancellation::new(None);
            cancel.set_lock_timeout(Some(Duration::from_secs(3)));
            let result = cancel.scope(|| b.execute(if reverse { select } else { delete }, &[], None));
            send.send(result).unwrap(); b
        });
        assert!(receive.recv_timeout(Duration::from_millis(50)).is_err());
        run(&mut a, "ROLLBACK");
        receive.recv_timeout(Duration::from_secs(4)).unwrap().unwrap();
        let mut b = handle.join().unwrap(); run(&mut b, "ROLLBACK");
        drop(a); drop(b); drop(admin); std::fs::remove_dir_all(path).unwrap();
    }
}

// PostgreSQL 18.6, oracle/upgrade-l6.txt: an existing locker upgrades without
// waiting behind a later UPDATE locker. The waiter sees the committed version.
#[test]
fn existing_locker_upgrades_past_waiting_group() {
    use std::{sync::mpsc, thread, time::{Duration, Instant}};
    for (held, writer, referencing_fk) in [
        ("NO KEY UPDATE", false, false), ("SHARE", false, false),
        ("KEY SHARE", false, false), ("", true, false), ("", true, true),
    ] {
        let (admin, path) = fixture();
        let mut a = admin.sql_session(); let mut b = admin.sql_session();
        if referencing_fk {
            run(&mut a, "CREATE TABLE child(id int PRIMARY KEY, parent int REFERENCES lk(id))");
        }
        run(&mut a, "BEGIN"); run(&mut b, "BEGIN");
        if writer { run(&mut a, "UPDATE lk SET v=v+1 WHERE id=1"); }
        else { run(&mut a, &format!("SELECT id FROM lk WHERE id=1 FOR {held}")); }
        let (send, receive) = mpsc::channel();
        let handle = thread::spawn(move || {
            let cancel = giggles_core::Cancellation::new(None);
            cancel.set_lock_timeout(Some(Duration::from_secs(5)));
            let result = cancel.scope(|| b.execute(if writer { "DELETE FROM lk WHERE id=1" }
                else { "SELECT id FROM lk WHERE id=1 FOR UPDATE" }, &[], None));
            send.send(result).unwrap(); b
        });
        wait_for_lock(&admin);
        let mut c = admin.sql_session();
        // PG18.6 oracle/upgrade-l6b.txt: compatible lockers bypass B's
        // provisional K, but SHARE and UPDATE still conflict with A.
        if held == "NO KEY UPDATE" {
            run(&mut c, "BEGIN");
            assert_eq!(run(&mut c, "SELECT id FROM lk WHERE id=1 FOR KEY SHARE NOWAIT").rows.len(), 1);
            run(&mut c, "COMMIT");
            for mode in ["SHARE", "UPDATE"] {
                let error = c.execute(&format!("SELECT id FROM lk WHERE id=1 FOR {mode} NOWAIT"), &[], None).unwrap_err();
                assert_eq!(giggles_core::sqlstate(&error), "55P03");
            }
        }
        let start = Instant::now();
        let upgraded = limited(&mut a, if writer { "DELETE FROM lk WHERE id=1" }
            else { "SELECT id FROM lk WHERE id=1 FOR UPDATE" }).unwrap();
        assert!(start.elapsed() < Duration::from_secs(1));
        if writer { assert_eq!(upgraded.affected, 1); }
        else { assert_eq!(upgraded.rows.len(), 1); }
        // Once the SELECT upgrade is held, PG also rejects KEY SHARE NOWAIT.
        if !writer {
            let error = c.execute("SELECT id FROM lk WHERE id=1 FOR KEY SHARE NOWAIT", &[], None).unwrap_err();
            assert_eq!(giggles_core::sqlstate(&error), "55P03");
        }
        assert!(receive.try_recv().is_err());
        run(&mut a, "COMMIT");
        let waited = receive.recv_timeout(Duration::from_secs(6)).unwrap().unwrap();
        if writer { assert_eq!(waited.affected, 0); }
        else { assert_eq!(waited.rows.len(), 1); }
        let mut b = handle.join().unwrap(); run(&mut b, "COMMIT");
        drop(a); drop(b); drop(c); drop(admin); std::fs::remove_dir_all(path).unwrap();
    }
}

fn wait_for_lock(admin: &Admin) {
    let until = std::time::Instant::now() + std::time::Duration::from_secs(4);
    while admin.waiting_lock_count() == 0 {
        assert!(std::time::Instant::now() < until, "request did not queue");
        std::thread::yield_now();
    }
}

// PostgreSQL 18.6: lock/oracle/upgrade-l6c.txt, with pg_stat_activity wait proof.
#[test]
fn retained_bypasser_upgrades_while_update_group_revalidates() {
    use std::{sync::mpsc, thread, time::Duration};
    let (admin, path) = fixture();
    let mut a = admin.sql_session(); let mut b = admin.sql_session(); let mut c = admin.sql_session();
    run(&mut a, "BEGIN"); run(&mut b, "BEGIN"); run(&mut c, "BEGIN");
    run(&mut a, "SELECT id FROM lk WHERE id=1 FOR NO KEY UPDATE");
    let (send, receive) = mpsc::channel();
    let handle = thread::spawn(move || {
        let cancel = giggles_core::Cancellation::new(None);
        cancel.set_lock_timeout(Some(Duration::from_secs(8)));
        send.send(cancel.scope(|| b.execute("SELECT id FROM lk WHERE id=1 FOR UPDATE", &[], None))).unwrap(); b
    });
    wait_for_lock(&admin);
    run(&mut c, "SELECT id FROM lk WHERE id=1 FOR KEY SHARE NOWAIT");
    run(&mut a, "COMMIT");
    wait_for_lock(&admin);
    assert_eq!(run(&mut a, "SELECT id FROM lk WHERE id=1 FOR KEY SHARE NOWAIT").rows.len(), 1);
    assert_eq!(limited(&mut c, "SELECT id FROM lk WHERE id=1 FOR NO KEY UPDATE").unwrap().rows.len(), 1);
    assert!(receive.try_recv().is_err());
    run(&mut c, "COMMIT");
    assert_eq!(receive.recv_timeout(Duration::from_secs(9)).unwrap().unwrap().rows.len(), 1);
    let mut b = handle.join().unwrap(); run(&mut b, "COMMIT");
    drop(a); drop(b); drop(c); drop(admin); std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn timed_out_update_group_leaves_no_exclusion_after_rollback_to() {
    use std::{sync::mpsc, thread, time::Duration};
    let (admin, path) = fixture();
    let mut a = admin.sql_session(); let mut b = admin.sql_session(); let mut c = admin.sql_session();
    run(&mut a, "BEGIN"); run(&mut b, "BEGIN"); run(&mut b, "SAVEPOINT s"); run(&mut c, "BEGIN");
    run(&mut a, "SELECT id FROM lk WHERE id=1 FOR NO KEY UPDATE");
    let (send, receive) = mpsc::channel();
    let handle = thread::spawn(move || {
        let cancel = giggles_core::Cancellation::new(None);
        cancel.set_lock_timeout(Some(Duration::from_secs(2)));
        send.send(cancel.scope(|| b.execute("SELECT id FROM lk WHERE id=1 FOR UPDATE", &[], None))).unwrap(); b
    });
    wait_for_lock(&admin);
    run(&mut c, "SELECT id FROM lk WHERE id=1 FOR KEY SHARE NOWAIT");
    assert_eq!(giggles_core::sqlstate(&receive.recv_timeout(Duration::from_secs(4)).unwrap().unwrap_err()), "55P03");
    let mut b = handle.join().unwrap(); run(&mut b, "ROLLBACK TO s");
    run(&mut a, "COMMIT");
    assert_eq!(run(&mut a, "SELECT id FROM lk WHERE id=1 FOR KEY SHARE NOWAIT").rows.len(), 1);
    let (send, receive) = mpsc::channel();
    let handle = thread::spawn(move || {
        let cancel = giggles_core::Cancellation::new(None);
        cancel.set_lock_timeout(Some(Duration::from_secs(8)));
        send.send(cancel.scope(|| b.execute("DELETE FROM lk WHERE id=1", &[], None))).unwrap(); b
    });
    wait_for_lock(&admin); assert!(receive.try_recv().is_err());
    run(&mut c, "COMMIT");
    assert_eq!(receive.recv_timeout(Duration::from_secs(9)).unwrap().unwrap().affected, 1);
    let mut b = handle.join().unwrap(); run(&mut b, "ROLLBACK");
    drop(a); drop(b); drop(c); drop(admin); std::fs::remove_dir_all(path).unwrap();
}

// PG18.6: l7-oracle.txt and l7b-oracle.txt.
#[test]
fn plain_statements_then_savepoint_release_only_post_savepoint_locks() {
    let (admin, path) = fixture();
    let mut a = admin.sql_session(); let mut b = admin.sql_session();
    run(&mut a, "BEGIN");
    run(&mut a, "UPDATE lk SET v=7 WHERE id=1");
    run(&mut a, "SAVEPOINT s");
    run(&mut a, "UPDATE lk SET v=8 WHERE id=2");
    let error = a.execute("SELECT 1/0", &[], None).unwrap_err();
    assert_eq!(giggles_core::sqlstate(&error), "22012");
    run(&mut a, "ROLLBACK TO s");

    run(&mut b, "BEGIN");
    let cancel = giggles_core::Cancellation::new(None);
    cancel.set_lock_timeout(Some(std::time::Duration::from_secs(1)));
    cancel.scope(|| b.execute("UPDATE lk SET v=9 WHERE id=2", &[], None)).unwrap();
    let error = cancel.scope(|| b.execute("UPDATE lk SET v=9 WHERE id=1", &[], None)).unwrap_err();
    assert_eq!(giggles_core::sqlstate(&error), "55P03");
    run(&mut a, "COMMIT"); run(&mut b, "ROLLBACK");
    drop(a); drop(b); drop(admin); std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn savepoint_releases_all_select_modes_and_reverts_only_upgrades() {
    let (admin, path) = fixture();
    for held in ["KEY SHARE", "SHARE", "NO KEY UPDATE", "UPDATE"] {
        let mut a = admin.sql_session(); let mut b = admin.sql_session();
        run(&mut a, "BEGIN"); run(&mut a, "SAVEPOINT s");
        run(&mut a, &format!("SELECT id FROM lk WHERE id=1 FOR {held}"));
        run(&mut a, "ROLLBACK TO s");
        run(&mut b, "SELECT id FROM lk WHERE id=1 FOR UPDATE NOWAIT");
        run(&mut a, "ROLLBACK");
    }
    let mut a = admin.sql_session(); let mut b = admin.sql_session();
    run(&mut a, "BEGIN"); run(&mut a, "SELECT id FROM lk WHERE id=1 FOR KEY SHARE");
    run(&mut a, "SAVEPOINT s"); run(&mut a, "SELECT id FROM lk WHERE id=1 FOR UPDATE");
    run(&mut a, "ROLLBACK TO s");
    for mode in ["KEY SHARE", "SHARE", "NO KEY UPDATE"] {
        run(&mut b, &format!("SELECT id FROM lk WHERE id=1 FOR {mode} NOWAIT"));
    }
    let error = b.execute("SELECT id FROM lk WHERE id=1 FOR UPDATE NOWAIT", &[], None).unwrap_err();
    assert_eq!(giggles_core::sqlstate(&error), "55P03");
    run(&mut a, "ROLLBACK");
    drop(a); drop(b); drop(admin); std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn savepoint_nested_release_error_and_reuse() {
    let (admin, path) = fixture(); let mut a = admin.sql_session(); let mut b = admin.sql_session();
    run(&mut a, "BEGIN"); run(&mut a, "SAVEPOINT s1");
    run(&mut a, "SELECT id FROM lk WHERE id=1 FOR UPDATE");
    run(&mut a, "SAVEPOINT s2"); run(&mut a, "SELECT id FROM lk WHERE id=2 FOR UPDATE");
    run(&mut a, "ROLLBACK TO s2");
    run(&mut b, "SELECT id FROM lk WHERE id=2 FOR UPDATE NOWAIT");
    assert!(b.execute("SELECT id FROM lk WHERE id=1 FOR UPDATE NOWAIT", &[], None).is_err());
    run(&mut a, "SELECT id FROM lk WHERE id=2 FOR UPDATE"); run(&mut a, "RELEASE s2");
    assert!(b.execute("SELECT id FROM lk WHERE id=2 FOR UPDATE NOWAIT", &[], None).is_err());
    run(&mut a, "ROLLBACK TO s1");
    for _ in 0..2 {
        run(&mut b, "SELECT id FROM lk FOR UPDATE NOWAIT");
        run(&mut a, "SELECT id FROM lk WHERE id=1 FOR UPDATE");
        assert!(a.execute("SELECT 1/0", &[], None).is_err());
        run(&mut a, "ROLLBACK TO s1");
    }
    run(&mut a, "ROLLBACK");
    drop(a); drop(b); drop(admin); std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn savepoint_rollback_wakes_select_and_writer_waiters() {
    use std::{sync::mpsc, thread, time::Duration};
    for (holder, waiter) in [
        ("SELECT id FROM lk WHERE id=1 FOR UPDATE", "SELECT id FROM lk WHERE id=1 FOR UPDATE"),
        ("SELECT id FROM lk WHERE id=1 FOR NO KEY UPDATE", "SELECT id FROM lk WHERE id=1 FOR NO KEY UPDATE"),
        ("UPDATE lk SET v=8 WHERE id=1", "UPDATE lk SET v=9 WHERE id=1"),
        ("DELETE FROM lk WHERE id=1", "DELETE FROM lk WHERE id=1"),
        ("INSERT INTO lk VALUES(5,50,0)", "INSERT INTO lk VALUES(5,51,0)"),
        ("INSERT INTO lk VALUES(5,50,0)", "INSERT INTO lk VALUES(6,50,0)"),
    ] {
        let (admin, path) = fixture(); let mut a = admin.sql_session(); let mut b = admin.sql_session();
        run(&mut a, "BEGIN"); run(&mut a, "SAVEPOINT s"); run(&mut a, holder);
        run(&mut b, "BEGIN");
        let (send, receive) = mpsc::channel();
        let handle = thread::spawn(move || {
            let cancel = giggles_core::Cancellation::new(None);
            cancel.set_lock_timeout(Some(Duration::from_secs(6)));
            send.send(cancel.scope(|| b.execute(waiter, &[], None))).unwrap(); b
        });
        wait_for_lock(&admin); assert!(receive.try_recv().is_err());
        run(&mut a, "ROLLBACK TO s");
        receive.recv_timeout(Duration::from_secs(7)).unwrap().unwrap();
        let mut b = handle.join().unwrap(); run(&mut b, "ROLLBACK"); run(&mut a, "ROLLBACK");
        drop(a); drop(b); drop(admin); std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn savepoint_child_insert_releases_parent_key_share() {
    let (admin, path) = fixture(); let mut a = admin.sql_session(); let mut b = admin.sql_session();
    run(&mut a, "CREATE TABLE sp_child(id int PRIMARY KEY, parent int REFERENCES lk(id))");
    run(&mut a, "BEGIN"); run(&mut a, "SAVEPOINT s");
    run(&mut a, "INSERT INTO sp_child VALUES(1,1)");
    assert!(b.execute("SELECT id FROM lk WHERE id=1 FOR UPDATE NOWAIT", &[], None).is_err());
    run(&mut a, "ROLLBACK TO s");
    run(&mut b, "SELECT id FROM lk WHERE id=1 FOR UPDATE NOWAIT"); run(&mut a, "ROLLBACK");
    drop(a); drop(b); drop(admin); std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn savepoint_upgrade_rollback_keeps_preexisting_conflicting_waiter_queued() {
    use std::{sync::mpsc, thread, time::Duration};
    let (admin, path) = fixture(); let mut a = admin.sql_session(); let mut b = admin.sql_session();
    run(&mut a, "BEGIN"); run(&mut a, "SELECT id FROM lk WHERE id=1 FOR KEY SHARE");
    run(&mut a, "SAVEPOINT s"); run(&mut a, "SELECT id FROM lk WHERE id=1 FOR UPDATE");
    let (send, receive) = mpsc::channel();
    let handle = thread::spawn(move || {
        let cancel = giggles_core::Cancellation::new(None);
        cancel.set_lock_timeout(Some(Duration::from_secs(6)));
        send.send(cancel.scope(|| b.execute("SELECT id FROM lk WHERE id=1 FOR UPDATE", &[], None))).unwrap(); b
    });
    wait_for_lock(&admin);
    run(&mut a, "ROLLBACK TO s");
    assert_eq!(admin.waiting_lock_count(), 1);
    assert!(receive.try_recv().is_err());
    run(&mut a, "COMMIT");
    receive.recv_timeout(Duration::from_secs(7)).unwrap().unwrap();
    let b = handle.join().unwrap();
    drop(a); drop(b); drop(admin); std::fs::remove_dir_all(path).unwrap();
}
