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

#[test]
fn direct_blocks_match_sql_controls() {
    let path = directory();
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let mut observer = admin.sql_session();
    run(&mut observer, "CREATE TABLE direct_blocks (id INT PRIMARY KEY)");
    for direct in [false, true] {
        let mut s = admin.sql_session();
        if direct { s.begin_block().unwrap(); } else { run(&mut s, "BEGIN"); }
        assert!(s.in_transaction());
        run(&mut s, if direct { "INSERT INTO direct_blocks VALUES (2)" } else { "INSERT INTO direct_blocks VALUES (1)" });
        let before = run(&mut observer, "SELECT id FROM direct_blocks").rows.len();
        if direct { s.commit_block().unwrap(); } else { run(&mut s, "COMMIT"); }
        assert!(!s.in_transaction());
        assert_eq!(run(&mut observer, "SELECT id FROM direct_blocks").rows.len(), before + 1);
        if direct { s.begin_block().unwrap(); } else { run(&mut s, "BEGIN"); }
        assert!(execute(&mut s, "SELECT 1 / 0").is_err());
        assert!(s.is_aborted());
        let error = if direct { s.commit_block() } else { execute(&mut s, "COMMIT").map(drop) };
        assert_eq!(error, Err(CoreError::Sql(SqlError::Aborted)));
        assert!(s.in_transaction() && s.is_aborted());
        run(&mut s, "ROLLBACK");
        assert!(!s.in_transaction() && !s.is_aborted());
    }
}

#[test]
fn preparation_boundary_advances_only_read_committed() {
    for (level, columns) in [("READ COMMITTED", 2), ("REPEATABLE READ", 1)] {
        let path = directory();
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        let mut s = admin.sql_session();
        let mut other = admin.sql_session();
        run(&mut s, "CREATE TABLE t(a int)");
        let prepared = s.prepare("SELECT * FROM t").unwrap();
        let before = s.statement_catalog_version();
        assert!(before.is_some());
        s.set_isolation(if level == "READ COMMITTED" {
            SqlIsolation::ReadCommitted
        } else {
            SqlIsolation::RepeatableRead
        });
        run(&mut s, "BEGIN");
        // Choose a snapshot without acquiring a relation lock that blocks DDL.
        s.v2_transaction.as_mut().unwrap().snapshot_ts();
        run(&mut other, "ALTER TABLE t ADD COLUMN b int");
        assert_eq!(s.describe(&prepared).unwrap().columns.len(), 1);
        assert_eq!(s.statement_catalog_version(), before);
        s.begin_prepared_statement();
        if level == "READ COMMITTED" {
            assert_eq!(s.statement_catalog_version(), other.statement_catalog_version());
            assert_ne!(s.statement_catalog_version(), before);
        } else {
            assert_eq!(s.statement_catalog_version(), before);
        }
        let fresh = s.prepare("SELECT * FROM t").unwrap();
        assert_eq!(
            s.describe(&fresh).unwrap().columns.len(),
            columns,
            "{level}"
        );
        run(&mut s, "ROLLBACK");
        assert_eq!(s.statement_catalog_version(), other.statement_catalog_version());
        run(&mut s, "BEGIN");
        run(&mut s, "CREATE TABLE private_ddl(a int)");
        assert_eq!(s.statement_catalog_version(), None);
        assert_eq!(s.prepared_catalog_state(), PreparedCatalogState::PrivateDdl);
        run(&mut s, "ROLLBACK");
        drop(other);
        drop(s);
        drop(admin);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn authorization_binding_identifies_the_exact_source_and_catalog() {
    let path = directory();
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let mut session = admin.sql_session();
    session.enable_authorization_bindings();
    run(&mut session, "CREATE TABLE t(a int)");
    let prepared = session.prepare("SELECT a FROM t WHERE a = $1").unwrap();
    let version = session.statement_catalog_version().unwrap();
    let key = prepared.authorization_plan_key(version).unwrap();

    assert!(key.matches(&session.bind_relation_context(&prepared).unwrap(), version));
    let independent = session.prepare("SELECT a FROM t WHERE a = $1").unwrap();
    assert!(!key.matches(&independent, version));
    let typed = prepared.clone().with_parameter_types(&[Some(relational::DataType::BigInt)]).unwrap();
    assert!(!key.matches(&typed, version));

    session.set_search_path("alternate, public", "giggles");
    let rebound = session.bind_relation_context(&prepared).unwrap();
    assert!(!key.matches(&rebound, version));
    run(&mut session, "ALTER TABLE t ADD COLUMN b int");
    let next = session.statement_catalog_version().unwrap();
    assert_ne!(next, version);
    let rebound = session.bind_relation_context(&prepared).unwrap();
    assert!(rebound.authorization_plan_key(next).is_some());
    assert!(!key.matches(&rebound, version));

    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn portal_view_locks_expand_from_statement_snapshot() {
    let path = directory();
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let mut s = admin.sql_session();
    for sql in ["CREATE TABLE t(a int)", "CREATE VIEW v AS SELECT a FROM t",
        "CREATE VIEW v2 AS SELECT a FROM v", "BEGIN"] { run(&mut s, sql); }
    s.begin_prepared_statement();
    let prepared = s.prepare("SELECT * FROM v2").unwrap();
    let generation = s.statement_catalog_version();
    assert!(s.lock_prepared_relations(&prepared, s.prepared_bind_observation().unwrap()).unwrap());
    assert_eq!(s.statement_catalog_version(), generation);
    let locks = s.held_table_locks();
    assert_eq!(locks.iter().filter(|(_, _, mode, _)| *mode == "AccessShareLock").count(), 3);
    let missing = s.prepare("SELECT * FROM missing").unwrap();
    assert_eq!(s.lock_prepared_relations(&missing, s.prepared_bind_observation().unwrap()), Err(CoreError::Sql(SqlError::UnknownTable)));
    assert!(!s.is_aborted(), "authorization must still run before exposing the lock error");
    run(&mut s, "ROLLBACK");
    drop(s);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
