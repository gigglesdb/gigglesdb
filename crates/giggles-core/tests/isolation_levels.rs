//! GD-0075 R5-76: v2 SQL sessions run the requested isolation level. Expected
//! outcomes come from PostgreSQL 18.6 (J:/gigglesdb-work/r5/e/oracle/matrix.out);
//! interleavings that make PostgreSQL wait live in giggles-server's network tests.
#![cfg(feature = "engine-rocksdb")]
mod common;
use giggles_core::relational::Value;
use giggles_core::{Admin, CoreError, Engine, Limits, SqlIsolation, SqlSession, sqlstate};

fn admin(name: &str, setup: &str) -> Admin {
    let admin =
        Admin::create_engine(common::fresh(name), Limits::default(), Engine::V2).unwrap();
    let mut s = admin.sql_session();
    for sql in setup.split(';').filter(|sql| !sql.trim().is_empty()) {
        s.execute(sql, &[], None).unwrap();
    }
    admin
}
fn session(admin: &Admin, level: SqlIsolation) -> SqlSession {
    let mut s = admin.sql_session();
    s.set_isolation(level);
    s
}
fn ok(s: &mut SqlSession, sql: &str) {
    s.execute(sql, &[], None).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
}
fn int(s: &mut SqlSession, sql: &str) -> i64 {
    match &s.execute(sql, &[], None).unwrap().rows[0].values[0] {
        Value::Int(n) => *n,
        other => panic!("{sql}: {other:?}"),
    }
}
fn code(result: Result<giggles_core::relational::SqlResult, CoreError>) -> &'static str {
    sqlstate(&result.expect_err("expected an error"))
}
/// PostgreSQL commits a read-only transaction whose reads were overwritten
/// since its snapshot. Divergence (R5-77, optimistic validation): the engine
/// validates read-only SERIALIZABLE transactions too and fails COMMIT with 40001
/// (engine txn_tests pin this, e.g. fekete_read_only_snapshot_anomaly_*).
fn commit_reader(s: &mut SqlSession, level: SqlIsolation) {
    let commit = s.execute("COMMIT", &[], None);
    if level == SqlIsolation::Serializable {
        assert_eq!(code(commit), "40001");
    } else {
        commit.unwrap();
    }
}
const LEVELS: [SqlIsolation; 3] = [
    SqlIsolation::ReadCommitted,
    SqlIsolation::RepeatableRead,
    SqlIsolation::Serializable,
];

#[test]
fn nonrepeatable_read_and_snapshot_taken_at_first_statement() {
    for level in LEVELS {
        let admin = admin("iso-nrr", "CREATE TABLE t (id INT PRIMARY KEY, v INT); INSERT INTO t VALUES (1, 0)");
        let (mut a, mut b) = (session(&admin, level), session(&admin, SqlIsolation::ReadCommitted));
        ok(&mut a, "BEGIN");
        // PostgreSQL takes the snapshot at the first statement, not at BEGIN.
        ok(&mut b, "UPDATE t SET v = 9 WHERE id = 1");
        assert_eq!(int(&mut a, "SELECT v FROM t WHERE id = 1"), 9, "{level:?}");
        ok(&mut b, "UPDATE t SET v = 10 WHERE id = 1");
        let again = int(&mut a, "SELECT v FROM t WHERE id = 1");
        assert_eq!(again, if level == SqlIsolation::ReadCommitted { 10 } else { 9 }, "{level:?}");
        commit_reader(&mut a, level);
    }
}

#[test]
fn phantom_read() {
    for level in LEVELS {
        let admin = admin("iso-phantom", "CREATE TABLE t (id INT PRIMARY KEY, g INT); INSERT INTO t VALUES (1, 1)");
        let (mut a, mut b) = (session(&admin, level), session(&admin, SqlIsolation::ReadCommitted));
        ok(&mut a, "BEGIN");
        assert_eq!(int(&mut a, "SELECT count(*) FROM t WHERE g = 1"), 1);
        ok(&mut b, "INSERT INTO t VALUES (2, 1)");
        let again = int(&mut a, "SELECT count(*) FROM t WHERE g = 1");
        assert_eq!(again, if level == SqlIsolation::ReadCommitted { 2 } else { 1 }, "{level:?}");
        commit_reader(&mut a, level);
    }
}

#[test]
fn update_of_a_row_changed_since_the_snapshot() {
    for level in LEVELS {
        let admin = admin("iso-lost", "CREATE TABLE t (id INT PRIMARY KEY, v INT); INSERT INTO t VALUES (1, 0)");
        let (mut a, mut b) = (session(&admin, level), session(&admin, level));
        ok(&mut a, "BEGIN");
        ok(&mut b, "BEGIN");
        assert_eq!(int(&mut a, "SELECT v FROM t WHERE id = 1"), 0);
        assert_eq!(int(&mut b, "SELECT v FROM t WHERE id = 1"), 0);
        ok(&mut a, "UPDATE t SET v = v + 1 WHERE id = 1");
        ok(&mut a, "COMMIT");
        let update = b.execute("UPDATE t SET v = v + 1 WHERE id = 1", &[], None);
        if level == SqlIsolation::ReadCommitted {
            update.unwrap();
            ok(&mut b, "COMMIT");
        } else {
            // could not serialize access due to concurrent update
            assert_eq!(code(update), "40001", "{level:?}");
            assert_eq!(code(b.execute("SELECT 1", &[], None)), "25P02");
            ok(&mut b, "ROLLBACK");
        }
        let mut c = admin.sql_session();
        let expected = if level == SqlIsolation::ReadCommitted { 2 } else { 1 };
        assert_eq!(int(&mut c, "SELECT v FROM t WHERE id = 1"), expected, "{level:?}");
    }
}

#[test]
fn write_skew_fails_only_at_serializable() {
    for level in LEVELS {
        let admin = admin(
            "iso-skew",
            "CREATE TABLE doctors (id INT PRIMARY KEY, oncall BOOLEAN); INSERT INTO doctors VALUES (1, true), (2, true)",
        );
        let (mut a, mut b) = (session(&admin, level), session(&admin, level));
        ok(&mut a, "BEGIN");
        ok(&mut b, "BEGIN");
        assert_eq!(int(&mut a, "SELECT count(*) FROM doctors WHERE oncall"), 2);
        assert_eq!(int(&mut b, "SELECT count(*) FROM doctors WHERE oncall"), 2);
        ok(&mut a, "UPDATE doctors SET oncall = false WHERE id = 1");
        ok(&mut b, "UPDATE doctors SET oncall = false WHERE id = 2");
        ok(&mut a, "COMMIT");
        let commit = b.execute("COMMIT", &[], None);
        let mut c = admin.sql_session();
        let left = int(&mut c, "SELECT count(*) FROM doctors WHERE oncall");
        if level == SqlIsolation::Serializable {
            // could not serialize access due to read/write dependencies among transactions
            assert_eq!(code(commit), "40001");
            assert!(!b.in_transaction());
            assert_eq!(left, 1);
        } else {
            commit.unwrap();
            assert_eq!(left, 0, "{level:?}");
        }
    }
}

#[test]
fn phantom_write_skew_fails_only_at_serializable() {
    for level in LEVELS {
        let admin = admin("iso-pskew", "CREATE TABLE t (id INT PRIMARY KEY, g INT); INSERT INTO t VALUES (1, 1), (2, 2)");
        let (mut a, mut b) = (session(&admin, level), session(&admin, level));
        ok(&mut a, "BEGIN");
        ok(&mut b, "BEGIN");
        assert_eq!(int(&mut a, "SELECT count(*) FROM t WHERE g = 1"), 1);
        assert_eq!(int(&mut b, "SELECT count(*) FROM t WHERE g = 2"), 1);
        ok(&mut a, "INSERT INTO t VALUES (3, 2)");
        ok(&mut b, "INSERT INTO t VALUES (4, 1)");
        ok(&mut a, "COMMIT");
        let commit = b.execute("COMMIT", &[], None);
        let mut c = admin.sql_session();
        let rows = int(&mut c, "SELECT count(*) FROM t");
        if level == SqlIsolation::Serializable {
            assert_eq!(code(commit), "40001");
            assert_eq!(rows, 3);
        } else {
            commit.unwrap();
            assert_eq!(rows, 4, "{level:?}");
        }
    }
}

#[test]
fn read_only_anomaly_aborts_the_writer() {
    // Fekete's read-only anomaly. PostgreSQL fails B's UPDATE; optimistic
    // validation fails B at COMMIT, with the same outcome.
    for level in LEVELS {
        let admin = admin("iso-ro", "CREATE TABLE acct (id INT PRIMARY KEY, bal INT); INSERT INTO acct VALUES (1, 0), (2, 0)");
        let (mut a, mut b, mut c) =
            (session(&admin, level), session(&admin, level), session(&admin, level));
        ok(&mut b, "BEGIN");
        assert_eq!(int(&mut b, "SELECT sum(bal) FROM acct"), 0);
        ok(&mut a, "BEGIN");
        ok(&mut a, "UPDATE acct SET bal = bal + 20 WHERE id = 2");
        ok(&mut a, "COMMIT");
        ok(&mut c, "BEGIN");
        assert_eq!(int(&mut c, "SELECT bal FROM acct WHERE id = 1"), 0);
        assert_eq!(int(&mut c, "SELECT bal FROM acct WHERE id = 2"), 20);
        ok(&mut c, "COMMIT");
        let update = b.execute("UPDATE acct SET bal = bal - 11 WHERE id = 1", &[], None);
        update.unwrap();
        let commit = b.execute("COMMIT", &[], None);
        let mut d = admin.sql_session();
        let one = int(&mut d, "SELECT bal FROM acct WHERE id = 1");
        if level == SqlIsolation::Serializable {
            assert_eq!(code(commit), "40001");
            assert_eq!(one, 0);
        } else {
            commit.unwrap();
            assert_eq!(one, -11, "{level:?}");
        }
    }
}

#[test]
fn update_of_a_row_deleted_since_the_snapshot() {
    for level in LEVELS {
        let admin = admin("iso-del", "CREATE TABLE t (id INT PRIMARY KEY, v INT); INSERT INTO t VALUES (1, 0)");
        let (mut a, mut b) = (session(&admin, level), session(&admin, level));
        ok(&mut a, "BEGIN");
        ok(&mut b, "BEGIN");
        assert_eq!(int(&mut a, "SELECT v FROM t WHERE id = 1"), 0);
        assert_eq!(int(&mut b, "SELECT v FROM t WHERE id = 1"), 0);
        ok(&mut a, "DELETE FROM t WHERE id = 1");
        ok(&mut a, "COMMIT");
        let update = b.execute("UPDATE t SET v = 5 WHERE id = 1", &[], None);
        if level == SqlIsolation::ReadCommitted {
            assert_eq!(update.unwrap().affected, 0);
            ok(&mut b, "COMMIT");
        } else {
            // could not serialize access due to concurrent delete
            assert_eq!(code(update), "40001", "{level:?}");
            ok(&mut b, "ROLLBACK");
        }
    }
}

#[test]
fn set_transaction_isolation_before_and_after_the_first_statement() {
    let admin = admin("iso-set", "CREATE TABLE t (id INT PRIMARY KEY, v INT); INSERT INTO t VALUES (1, 0)");
    let (mut a, mut b) = (session(&admin, SqlIsolation::ReadCommitted), admin.sql_session());
    ok(&mut a, "BEGIN");
    a.set_transaction_isolation(SqlIsolation::RepeatableRead).unwrap();
    assert_eq!(int(&mut a, "SELECT v FROM t WHERE id = 1"), 0);
    ok(&mut b, "UPDATE t SET v = 3 WHERE id = 1");
    assert_eq!(int(&mut a, "SELECT v FROM t WHERE id = 1"), 0);
    ok(&mut a, "INSERT INTO t VALUES (2, 4)");
    // Writes pin the level; the pgwire layer reports 25001 before reaching this.
    assert!(a.set_transaction_isolation(SqlIsolation::Serializable).is_err());
    ok(&mut a, "COMMIT");
    // The session default applies again to the next transaction.
    ok(&mut a, "BEGIN");
    assert_eq!(int(&mut a, "SELECT v FROM t WHERE id = 1"), 3);
    ok(&mut b, "UPDATE t SET v = 5 WHERE id = 1");
    assert_eq!(int(&mut a, "SELECT v FROM t WHERE id = 1"), 5);
    ok(&mut a, "COMMIT");
    // Outside a transaction there is nothing to re-level.
    a.set_transaction_isolation(SqlIsolation::Serializable).unwrap();
}

#[test]
fn statements_outside_a_block_run_the_session_level() {
    // A single serializable statement validates its reads at commit like any other.
    let admin = admin("iso-implicit", "CREATE TABLE t (id INT PRIMARY KEY, v INT); INSERT INTO t VALUES (1, 0), (2, 0)");
    let mut a = session(&admin, SqlIsolation::Serializable);
    ok(&mut a, "UPDATE t SET v = v + 2 WHERE id = 1");
    assert_eq!(int(&mut a, "SELECT v FROM t WHERE id = 1"), 2);
}

#[test]
fn foreign_keys_at_every_level() {
    for level in LEVELS {
        let admin = admin(
            "iso-fk",
            "CREATE TABLE p (id INT PRIMARY KEY); CREATE TABLE c (id INT PRIMARY KEY, p INT REFERENCES p (id)); INSERT INTO p VALUES (1), (2)",
        );
        let mut a = session(&admin, level);
        ok(&mut a, "BEGIN");
        ok(&mut a, "INSERT INTO c VALUES (1, 1)");
        assert_eq!(code(a.execute("INSERT INTO c VALUES (2, 3)", &[], None)), "23503", "{level:?}");
        ok(&mut a, "ROLLBACK");
        ok(&mut a, "BEGIN");
        ok(&mut a, "INSERT INTO c VALUES (1, 1)");
        ok(&mut a, "DELETE FROM p WHERE id = 2");
        ok(&mut a, "UPDATE c SET p = 1 WHERE id = 1");
        ok(&mut a, "COMMIT");
    }
}

#[test]
fn foreign_key_witnesses_changed_since_the_snapshot() {
    // PostgreSQL 18.6 (J:/gigglesdb-work/r5/e/oracle/fk-race.out): the parent of
    // a child is read at the snapshot (40001 if changed since); referencing rows
    // of a parent key are read at the latest committed state.
    for level in [SqlIsolation::RepeatableRead, SqlIsolation::Serializable] {
        let admin = admin(
            "iso-fk-race",
            "CREATE TABLE p (id INT PRIMARY KEY); CREATE TABLE c (id INT PRIMARY KEY, p INT REFERENCES p (id));              CREATE TABLE k (id INT PRIMARY KEY, p INT REFERENCES p (id) ON DELETE CASCADE); CREATE TABLE u (id INT PRIMARY KEY);              INSERT INTO p VALUES (1), (2), (3), (6), (7); INSERT INTO c VALUES (7, 6)",
        );
        let (mut a, mut b) = (session(&admin, level), admin.sql_session());
        let begin = |a: &mut SqlSession| {
            ok(a, "BEGIN");
            assert_eq!(int(a, "SELECT count(*) FROM u"), 0);
        };
        // A child insert whose parent was deleted after the snapshot.
        begin(&mut a);
        ok(&mut b, "DELETE FROM p WHERE id = 1");
        assert_eq!(code(a.execute("INSERT INTO c VALUES (1, 1)", &[], None)), "40001", "{level:?}");
        ok(&mut a, "ROLLBACK");
        // A parent inserted after the snapshot stays invisible.
        begin(&mut a);
        ok(&mut b, "INSERT INTO p VALUES (4)");
        assert_eq!(code(a.execute("INSERT INTO c VALUES (3, 4)", &[], None)), "23503", "{level:?}");
        ok(&mut a, "ROLLBACK");
        // A parent delete sees a child inserted after the snapshot.
        begin(&mut a);
        ok(&mut b, "INSERT INTO c VALUES (2, 2)");
        assert_eq!(code(a.execute("DELETE FROM p WHERE id = 2", &[], None)), "23503", "{level:?}");
        ok(&mut a, "ROLLBACK");
        // ... and not one deleted after it.
        begin(&mut a);
        ok(&mut b, "DELETE FROM c WHERE id = 7");
        ok(&mut a, "DELETE FROM p WHERE id = 6");
        ok(&mut a, "COMMIT");
        // A cascade that would delete a child inserted after the snapshot.
        begin(&mut a);
        ok(&mut b, "INSERT INTO k VALUES (1, 3)");
        assert_eq!(code(a.execute("DELETE FROM p WHERE id = 3", &[], None)), "40001", "{level:?}");
        ok(&mut a, "ROLLBACK");
        // Writes elsewhere since the snapshot leave the checks alone.
        begin(&mut a);
        ok(&mut b, "INSERT INTO p VALUES (5)");
        ok(&mut b, "INSERT INTO c VALUES (5, 5)");
        ok(&mut a, "INSERT INTO c VALUES (3, 3)");
        ok(&mut a, "DELETE FROM p WHERE id = 4");
        ok(&mut a, "COMMIT");
        assert_eq!(int(&mut b, "SELECT count(*) FROM c"), 3);
        assert_eq!(int(&mut b, "SELECT count(*) FROM p"), 4);
    }
}

#[test]
fn write_skew_through_primary_key_reads_fails_at_serializable() {
    for level in LEVELS {
        let admin = admin("iso-pk-skew", "CREATE TABLE t (id INT PRIMARY KEY, v INT); INSERT INTO t VALUES (1, 0), (2, 0)");
        let (mut a, mut b) = (session(&admin, level), session(&admin, level));
        ok(&mut a, "BEGIN");
        ok(&mut b, "BEGIN");
        assert_eq!(int(&mut a, "SELECT v FROM t WHERE id = 1"), 0);
        assert_eq!(int(&mut b, "SELECT v FROM t WHERE id = 2"), 0);
        ok(&mut a, "UPDATE t SET v = 1 WHERE id = 2");
        ok(&mut b, "UPDATE t SET v = 1 WHERE id = 1");
        ok(&mut a, "COMMIT");
        let commit = b.execute("COMMIT", &[], None);
        if level == SqlIsolation::Serializable {
            assert_eq!(code(commit), "40001");
        } else {
            commit.unwrap();
        }
    }
}
