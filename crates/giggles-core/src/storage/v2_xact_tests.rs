use super::*;
use crate::relational::{SqlResult, StatementClock, Value as V};

fn execute(s: &mut SqlSession, sql: &str) -> Result<SqlResult, CoreError> {
    s.execute_at(
        sql,
        &[],
        None,
        &StatementClock {
            unavailable: None,
            transaction_us: 100,
            statement_us: 100,
            zone: Some(giggles_pg_types::temporal::SessionZone::Fixed(0)),
            wall: None,
        },
    )
}
fn run(s: &mut SqlSession, sql: &str) -> SqlResult {
    execute(s, sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"))
}
fn scalar(s: &mut SqlSession, sql: &str) -> V {
    run(s, sql).rows[0].values[0].clone()
}
fn xid(s: &mut SqlSession) -> i64 {
    let V::Int(id) = scalar(s, "SELECT pg_current_xact_id()") else {
        panic!("expected xid")
    };
    id
}
fn status(s: &mut SqlSession, id: i64, expected: &str) {
    assert_eq!(
        scalar(s, &format!("SELECT pg_xact_status({id})")),
        V::Text(expected.into())
    );
}
fn directory() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "v2-xact-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

#[test]
fn transaction_lifecycle_and_implicit_cleanup() {
    let path = directory();
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let mut s = admin.sql_session();
    let mut other = admin.sql_session();
    run(&mut s, "BEGIN");
    assert_eq!(
        scalar(&mut s, "SELECT pg_current_xact_id_if_assigned() IS NULL"),
        V::Bool(true)
    );
    let x = xid(&mut s);
    assert_eq!(xid(&mut s), x);
    run(&mut s, "SAVEPOINT s");
    run(&mut s, "ROLLBACK TO s");
    assert_eq!(xid(&mut s), x);
    status(&mut s, x, "in progress");
    status(&mut other, x, "in progress");
    run(&mut s, "COMMIT");
    status(&mut other, x, "committed");
    run(&mut s, "BEGIN");
    let y = xid(&mut s);
    run(&mut s, "ROLLBACK");
    status(&mut other, y, "aborted");
    run(&mut s, "BEGIN");
    let failed = xid(&mut s);
    assert!(execute(&mut s, "SELECT 1 / 0").is_err());
    status(&mut other, failed, "in progress");
    run(&mut s, "ROLLBACK");
    status(&mut other, failed, "aborted");
    let z = xid(&mut s);
    status(&mut other, z, "committed");
    assert_eq!(
        scalar(&mut s, "SELECT pg_current_xact_id_if_assigned()"),
        V::Null
    );
    assert!(execute(&mut s, "SELECT pg_current_xact_id() / 0").is_err());
    assert!(s.xact.lock().unwrap().is_none());
    assert_eq!(
        scalar(&mut s, "SELECT pg_current_xact_id_if_assigned()"),
        V::Null
    );
    run(&mut s, "CREATE TABLE t(id int)");
    run(&mut s, "BEGIN");
    run(&mut s, "INSERT INTO t VALUES(1)");
    let written = xid(&mut s);
    run(&mut s, "COMMIT");
    status(&mut other, written, "committed");
    assert_eq!(scalar(&mut other, "SELECT id FROM t"), V::Int(1));
    drop(other);
    drop(s);
    drop(admin);
    let admin = Admin::open(&path, Limits::default()).unwrap();
    let mut s = admin.sql_session();
    let next = xid(&mut s);
    assert!(next > [x, y, failed, z, written].into_iter().max().unwrap());
    assert_eq!(
        scalar(&mut s, "SELECT pg_xact_status(0) IS NULL"),
        V::Bool(true)
    );
    status(&mut s, 1, "committed");
    assert_eq!(
        scalar(&mut s, "SELECT pg_xact_status(NULL) IS NULL"),
        V::Bool(true)
    );
    let error = execute(&mut s, &format!("SELECT pg_xact_status({})", next + 100000)).unwrap_err();
    assert_eq!(crate::sqlstate::sqlstate(&error), "22023");
    drop(s);
    drop(admin);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn commit_reply_loss_and_failure_are_durable() {
    for (point, expected, present) in [
        (CrashPoint::ErrorAfterCommit, "committed", true),
        (CrashPoint::ErrorBeforeCommit, "aborted", false),
    ] {
        let path = directory();
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        let mut s = admin.sql_session();
        run(&mut s, "CREATE TABLE t(id int)");
        run(&mut s, "BEGIN");
        run(&mut s, "INSERT INTO t VALUES(7)");
        let x = xid(&mut s);
        admin.set_crash_point(point).unwrap();
        assert!(execute(&mut s, "COMMIT").is_err());
        drop(s);
        drop(admin);
        let admin = Admin::open(&path, Limits::default()).unwrap();
        let mut s = admin.sql_session();
        status(&mut s, x, expected);
        let rows = run(&mut s, "SELECT id FROM t").rows;
        assert_eq!(rows.len(), usize::from(present));
        if present {
            assert_eq!(rows[0].values, vec![V::Int(7)]);
        }
        assert!(xid(&mut s) > x);
        drop(s);
        drop(admin);
        fs::remove_dir_all(path).unwrap();
    }
}
