//! Isolation levels through cluster gateways (GD-0075 Phase 8, R5-70..R5-72): two gateways over
//! one in-process cluster of real range engines. Expected outcomes are PostgreSQL 18.6's
//! (J:/gigglesdb-work/r5/e/oracle); where a lock makes a gateway wait, the waiting statement
//! runs on its own thread.
use super::local::LocalCluster;
use super::*;
use crate::relational::{SqlResult, StatementClock, Value};
use crate::storage::{SqlSession, engine::kv::Keyspace};
use crate::storage::range_api::{RangeSpace, range_key};
use std::sync::mpsc;
use std::time::Duration;

fn clock() -> StatementClock {
    StatementClock {
        unavailable: None,
        transaction_us: 100,
        statement_us: 100,
        zone: Some(giggles_pg_types::temporal::SessionZone::Fixed(0)),
        wall: None,
    }
}

/// Each of the first tables' rows and index entries in their own ranges.
fn splits() -> Vec<Vec<u8>> {
    let mut splits: Vec<Vec<u8>> = [RangeSpace::Rows, RangeSpace::Index]
        .into_iter()
        .flat_map(|space| (1..=12_u64).map(move |id| range_key(space, &id.to_be_bytes())))
        .collect();
    splits.sort();
    splits
}

fn gateway(cluster: &Arc<LocalCluster>, node: u16) -> Gateway {
    let config = super::super::txn::ClusterConfig {
        node,
        heartbeat: Duration::from_millis(200),
        ..Default::default()
    };
    let deadlock = DeadlockConfig {
        deadlock_timeout: Duration::from_millis(200),
        lock_wait_backstop: Duration::from_secs(20),
        tick: Duration::from_millis(50),
    };
    let gateway = attach(
        cluster.clone(),
        config,
        Some((cluster.clone(), deadlock)),
        [9; 16],
        Limits::default(),
    )
    .unwrap();
    cluster.register_gateway(u64::from(node), gateway.services.gateway_service());
    gateway
}

fn exec(session: &mut SqlSession, sql: &str) -> Result<SqlResult, crate::CoreError> {
    session.execute_at(sql, &[], None, &clock())
}

fn ok(session: &mut SqlSession, sql: &str) -> SqlResult {
    exec(session, sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"))
}

fn int(session: &mut SqlSession, sql: &str) -> i64 {
    match &ok(session, sql).rows[0].values[0] {
        Value::Int(n) => *n,
        other => panic!("{sql}: {other:?}"),
    }
}

fn code(result: Result<SqlResult, crate::CoreError>) -> &'static str {
    crate::sqlstate(&result.expect_err("expected an error"))
}

const LEVELS: [&str; 3] = ["READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"];

/// BEGIN at `level`, as giggles-server does for `BEGIN ISOLATION LEVEL <level>` (the SQL
/// layer takes the session's level; pgwire parses the clause).
fn begin(session: &mut SqlSession, level: &str) {
    session.set_isolation(match level {
        "READ COMMITTED" => crate::SqlIsolation::ReadCommitted,
        "REPEATABLE READ" => crate::SqlIsolation::RepeatableRead,
        "SERIALIZABLE" => crate::SqlIsolation::Serializable,
        other => panic!("level {other}"),
    });
    ok(session, "BEGIN");
}

/// One cluster, two gateways (nodes 1 and 2), `setup` run on the first.
fn two(setup: &str) -> (Arc<LocalCluster>, Gateway, Gateway) {
    let cluster = LocalCluster::new(&splits());
    let a = gateway(&cluster, 1);
    let b = gateway(&cluster, 2);
    let mut s = a.admin.sql_session();
    for sql in setup.split(';').filter(|sql| !sql.trim().is_empty()) {
        ok(&mut s, sql);
    }
    (cluster, a, b)
}

/// Runs `first` on its own thread, gives it `head_start` to finish (or block), then runs
/// `second` here, and returns both results.
fn race(
    first: &mut SqlSession,
    first_sql: &str,
    second: &mut SqlSession,
    second_sql: &str,
    head_start: Duration,
) -> (Result<(), String>, Result<(), String>) {
    std::thread::scope(|scope| {
        let (tx, rx) = mpsc::channel();
        scope.spawn(move || {
            let _ = tx.send(
                exec(first, first_sql)
                    .map(|_| ())
                    .map_err(|e| crate::sqlstate(&e).to_string()),
            );
        });
        let early = rx.recv_timeout(head_start).ok();
        let second = exec(second, second_sql)
            .map(|_| ())
            .map_err(|e| crate::sqlstate(&e).to_string());
        let first = early.unwrap_or_else(|| rx.recv_timeout(Duration::from_secs(30)).unwrap());
        (first, second)
    })
}

/// R5-70: each requested level runs in a cluster. A second read sees a commit made through
/// another gateway in between only at READ COMMITTED; a read-only SERIALIZABLE transaction
/// commits although what it read changed (PostgreSQL commits it too).
#[test]
fn each_level_runs_through_a_gateway() {
    for level in LEVELS {
        let (_cluster, a, b) =
            two("CREATE TABLE t (id INT PRIMARY KEY, v INT); INSERT INTO t VALUES (1, 0)");
        let (mut sa, mut sb) = (a.admin.sql_session(), b.admin.sql_session());
        begin(&mut sa, level);
        assert_eq!(int(&mut sa, "SELECT v FROM t WHERE id = 1"), 0);
        ok(&mut sb, "UPDATE t SET v = 9 WHERE id = 1");
        let again = int(&mut sa, "SELECT v FROM t WHERE id = 1");
        let want = if level == "READ COMMITTED" { 9 } else { 0 };
        assert_eq!(again, want, "{level}");
        ok(&mut sa, "COMMIT");
        assert_eq!(int(&mut sa, "SELECT v FROM t WHERE id = 1"), 9);
    }
}

/// R5-70: lost update. At REPEATABLE READ and SERIALIZABLE the second writer of a row changed
/// since its snapshot fails 40001; at READ COMMITTED it updates the new version.
#[test]
fn lost_update_fails_above_read_committed() {
    for level in LEVELS {
        let (_cluster, a, b) =
            two("CREATE TABLE t (id INT PRIMARY KEY, v INT); INSERT INTO t VALUES (1, 0)");
        let (mut sa, mut sb) = (a.admin.sql_session(), b.admin.sql_session());
        begin(&mut sa, level);
        assert_eq!(int(&mut sa, "SELECT v FROM t WHERE id = 1"), 0);
        ok(&mut sb, "UPDATE t SET v = v + 1 WHERE id = 1");
        let update = exec(&mut sa, "UPDATE t SET v = v + 1 WHERE id = 1");
        if level == "READ COMMITTED" {
            update.unwrap();
            ok(&mut sa, "COMMIT");
            assert_eq!(int(&mut sb, "SELECT v FROM t WHERE id = 1"), 2);
        } else {
            assert_eq!(code(update), "40001", "{level}");
            let _ = exec(&mut sa, "ROLLBACK");
            assert_eq!(int(&mut sb, "SELECT v FROM t WHERE id = 1"), 1);
        }
    }
}

/// R5-71: write skew through primary-key reads on two gateways. Each transaction reads both
/// rows and updates one. PostgreSQL commits the first COMMIT and fails the second with 40001.
/// Here the first COMMIT waits (the other transaction holds a row it read) and the second
/// COMMIT's shared lock closes the cycle: the second fails 40001 and the first commits. At the
/// lower levels both commit.
#[test]
fn write_skew_through_two_gateways_fails_only_at_serializable() {
    for level in LEVELS {
        let (_cluster, a, b) = two(
            "CREATE TABLE doctors (id INT PRIMARY KEY, oncall INT); INSERT INTO doctors VALUES (1, 1), (2, 1)",
        );
        let (mut sa, mut sb) = (a.admin.sql_session(), b.admin.sql_session());
        for s in [&mut sa, &mut sb] {
            begin(s, level);
            let on = int(s, "SELECT oncall FROM doctors WHERE id = 1")
                + int(s, "SELECT oncall FROM doctors WHERE id = 2");
            assert_eq!(on, 2);
        }
        ok(&mut sa, "UPDATE doctors SET oncall = 0 WHERE id = 1");
        ok(&mut sb, "UPDATE doctors SET oncall = 0 WHERE id = 2");
        let (first, second) = race(
            &mut sa,
            "COMMIT",
            &mut sb,
            "COMMIT",
            Duration::from_millis(300),
        );
        let left = int(
            &mut a.admin.sql_session(),
            "SELECT count(*) FROM doctors WHERE oncall = 1",
        );
        if level == "SERIALIZABLE" {
            assert_eq!(first, Ok(()), "first COMMIT");
            assert_eq!(second, Err("40001".to_string()), "second COMMIT");
            assert_eq!(left, 1);
        } else {
            assert_eq!((first, second), (Ok(()), Ok(())), "{level}");
            assert_eq!(left, 0, "{level}");
        }
    }
}

/// R5-71: a SERIALIZABLE writer whose point read was overwritten (and committed) since its
/// snapshot fails at COMMIT, as on one node; the row it would have written stays unwritten. A
/// writer whose reads did not change commits, here across two ranges. PostgreSQL commits the
/// first writer too (one read/write dependency, no cycle): published divergence (b), E151.
#[test]
fn a_serializable_writer_fails_when_what_it_read_changed() {
    let (_cluster, a, b) = two(
        "CREATE TABLE x (id INT PRIMARY KEY, v INT); CREATE TABLE y (id INT PRIMARY KEY, v INT); INSERT INTO x VALUES (1, 0); INSERT INTO y VALUES (1, 0)",
    );
    let (mut sa, mut sb) = (a.admin.sql_session(), b.admin.sql_session());
    begin(&mut sa, "SERIALIZABLE");
    assert_eq!(int(&mut sa, "SELECT v FROM x WHERE id = 1"), 0);
    ok(&mut sb, "UPDATE x SET v = 5 WHERE id = 1");
    ok(&mut sa, "UPDATE y SET v = 1 WHERE id = 1");
    assert_eq!(code(exec(&mut sa, "COMMIT")), "40001");
    assert_eq!(int(&mut sb, "SELECT v FROM y WHERE id = 1"), 0);
    begin(&mut sa, "SERIALIZABLE");
    assert_eq!(int(&mut sa, "SELECT v FROM x WHERE id = 1"), 5);
    ok(&mut sa, "UPDATE y SET v = 2 WHERE id = 1");
    ok(&mut sa, "COMMIT");
    assert_eq!(int(&mut sb, "SELECT v FROM y WHERE id = 1"), 2);
}

/// Runs the two-transaction skew pattern through two gateways: both BEGIN at `level`, run
/// their reads, then their writes, then A's COMMIT (on its own thread, as it may wait) and B's.
fn skew(
    setup: &str,
    level: &str,
    reads: [&str; 2],
    writes: [&str; 2],
    check: &str,
) -> (Result<(), String>, Result<(), String>, i64) {
    let (_cluster, a, b) = two(setup);
    let (mut sa, mut sb) = (a.admin.sql_session(), b.admin.sql_session());
    for (s, read) in [(&mut sa, reads[0]), (&mut sb, reads[1])] {
        begin(s, level);
        ok(s, read);
    }
    ok(&mut sa, writes[0]);
    ok(&mut sb, writes[1]);
    let (first, second) = race(
        &mut sa,
        "COMMIT",
        &mut sb,
        "COMMIT",
        Duration::from_millis(300),
    );
    let left = int(&mut a.admin.sql_session(), check);
    (first, second, left)
}

/// R5-72: write skew through scans (PostgreSQL 18 matrix `write_skew`): each transaction counts
/// the doctors on call and takes one off. The first COMMIT's gap lock over the table waits for
/// the other's row; the second COMMIT's gap lock closes the cycle and fails 40001.
#[test]
fn write_skew_through_scans_fails_only_at_serializable() {
    for level in LEVELS {
        let (first, second, left) = skew(
            "CREATE TABLE doctors (id INT PRIMARY KEY, oncall INT); INSERT INTO doctors VALUES (1, 1), (2, 1)",
            level,
            ["SELECT count(*) FROM doctors WHERE oncall = 1"; 2],
            [
                "UPDATE doctors SET oncall = 0 WHERE id = 1",
                "UPDATE doctors SET oncall = 0 WHERE id = 2",
            ],
            "SELECT count(*) FROM doctors WHERE oncall = 1",
        );
        if level == "SERIALIZABLE" {
            assert_eq!((first, second, left), (Ok(()), Err("40001".into()), 1));
        } else {
            assert_eq!((first, second, left), (Ok(()), Ok(()), 0), "{level}");
        }
    }
}

/// R5-72: phantom write skew (matrix `phantom_write_skew`): each transaction counts one group
/// and inserts a row into the other's group. Only SERIALIZABLE fails the second COMMIT.
#[test]
fn phantom_write_skew_fails_only_at_serializable() {
    for level in LEVELS {
        let (first, second, rows) = skew(
            "CREATE TABLE t (id INT PRIMARY KEY, g INT); INSERT INTO t VALUES (1, 1), (2, 2)",
            level,
            [
                "SELECT count(*) FROM t WHERE g = 1",
                "SELECT count(*) FROM t WHERE g = 2",
            ],
            ["INSERT INTO t VALUES (3, 2)", "INSERT INTO t VALUES (4, 1)"],
            "SELECT count(*) FROM t",
        );
        if level == "SERIALIZABLE" {
            assert_eq!((first, second, rows), (Ok(()), Err("40001".into()), 3));
        } else {
            assert_eq!((first, second, rows), (Ok(()), Ok(()), 4), "{level}");
        }
    }
}

/// R5-72: a SERIALIZABLE writer whose scan would now return another row (an insert committed
/// into its interval since its snapshot) fails at COMMIT; one whose scanned interval is
/// unchanged commits. PostgreSQL commits the first writer (no cycle): divergence (b), E151.
#[test]
fn a_serializable_writer_fails_when_its_scan_gained_a_row() {
    let (_cluster, a, b) = two(
        "CREATE TABLE t (id INT PRIMARY KEY, g INT); CREATE TABLE log (id INT PRIMARY KEY, n INT); INSERT INTO t VALUES (1, 1)",
    );
    let (mut sa, mut sb) = (a.admin.sql_session(), b.admin.sql_session());
    begin(&mut sa, "SERIALIZABLE");
    assert_eq!(int(&mut sa, "SELECT count(*) FROM t WHERE g = 1"), 1);
    ok(&mut sb, "INSERT INTO t VALUES (2, 1)");
    ok(&mut sa, "INSERT INTO log VALUES (1, 1)");
    assert_eq!(code(exec(&mut sa, "COMMIT")), "40001");
    assert_eq!(int(&mut sb, "SELECT count(*) FROM log"), 0);
    begin(&mut sa, "SERIALIZABLE");
    assert_eq!(int(&mut sa, "SELECT count(*) FROM t WHERE g = 1"), 2);
    ok(&mut sa, "INSERT INTO log VALUES (1, 2)");
    ok(&mut sa, "COMMIT");
    assert_eq!(int(&mut sb, "SELECT n FROM log WHERE id = 1"), 2);
}

/// R5-72 (review S8): phantom write skew through a secondary index: each scan reads index
/// entries, so the gap locks are on the index's keys.
#[test]
fn phantom_write_skew_through_an_index_fails_at_serializable() {
    let (first, second, rows) = skew(
        "CREATE TABLE t (id INT PRIMARY KEY, g INT); CREATE INDEX t_g ON t (g); INSERT INTO t VALUES (1, 1), (2, 2)",
        "SERIALIZABLE",
        [
            "SELECT count(*) FROM t WHERE g = 1",
            "SELECT count(*) FROM t WHERE g = 2",
        ],
        ["INSERT INTO t VALUES (3, 2)", "INSERT INTO t VALUES (4, 1)"],
        "SELECT count(*) FROM t",
    );
    assert_eq!((first, second, rows), (Ok(()), Err("40001".into()), 3));
}

/// R5-72 (review S8): a table whose rows span two ranges. Each scan's gap lock is taken in both
/// ranges; the two COMMITs' locks cross ranges, so the cycle is found by the distributed
/// detector, and the second COMMIT fails 40001.
#[test]
fn phantom_write_skew_across_two_ranges_of_one_table() {
    use crate::relational::Value;
    use crate::storage::engine::codec;
    // The first table created here gets id 1; split its rows at id 50.
    let mut at = 1u64.to_be_bytes().to_vec();
    at.extend(codec::pk_bytes(&[Value::Int(50)]).unwrap());
    let mut splits = splits();
    splits.push(range_key(RangeSpace::Rows, &at));
    splits.sort();
    let cluster = LocalCluster::new(&splits);
    let a = gateway(&cluster, 1);
    let b = gateway(&cluster, 2);
    let mut s = a.admin.sql_session();
    ok(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, g INT)");
    ok(&mut s, "INSERT INTO t VALUES (10, 1), (90, 2)");
    let key = |id: i64| {
        let mut key = 1u64.to_be_bytes().to_vec();
        key.extend(codec::pk_bytes(&[Value::Int(id)]).unwrap());
        key
    };
    assert_ne!(
        cluster.range_of(RangeSpace::Rows, &key(10)),
        cluster.range_of(RangeSpace::Rows, &key(90)),
        "table t must be the first table (id 1), its rows split at id 50"
    );
    let (mut sa, mut sb) = (a.admin.sql_session(), b.admin.sql_session());
    begin(&mut sa, "SERIALIZABLE");
    begin(&mut sb, "SERIALIZABLE");
    assert_eq!(int(&mut sa, "SELECT count(*) FROM t WHERE g = 1"), 1);
    assert_eq!(int(&mut sb, "SELECT count(*) FROM t WHERE g = 2"), 1);
    ok(&mut sa, "INSERT INTO t VALUES (20, 2)");
    ok(&mut sb, "INSERT INTO t VALUES (80, 1)");
    let (first, second) = race(
        &mut sa,
        "COMMIT",
        &mut sb,
        "COMMIT",
        Duration::from_millis(300),
    );
    assert_eq!((first, second), (Ok(()), Err("40001".to_string())));
    assert_eq!(int(&mut s, "SELECT count(*) FROM t"), 3);
}

/// HD-4a: three primary-key reads validate their grants, with and without a newer commit.
#[test]
fn point_reads_validate_from_lock_grants() {
    for changed in [false, true] {
        let (_cluster, a, b) = two(
            "CREATE TABLE t (id INT PRIMARY KEY, v INT); CREATE TABLE log (id INT PRIMARY KEY, v INT); INSERT INTO t VALUES (1, 0), (2, 0), (3, 0)",
        );
        let (mut sa, mut sb) = (a.admin.sql_session(), b.admin.sql_session());
        begin(&mut sa, "SERIALIZABLE");
        for id in 1..=3 {
            assert_eq!(int(&mut sa, &format!("SELECT v FROM t WHERE id = {id}")), 0);
        }
        if changed {
            ok(&mut sb, "UPDATE t SET v = 9 WHERE id = 2");
        }
        ok(&mut sa, "INSERT INTO log VALUES (1, 1)");
        let result = exec(&mut sa, "COMMIT");
        if changed {
            assert_eq!(code(result), "40001");
        } else {
            result.unwrap();
        }
        assert_eq!(
            int(&mut sb, "SELECT count(*) FROM log"),
            i64::from(!changed)
        );
    }
}

/// The SQL gateway has no validation-config setter: exercise the grant helper directly.
#[test]
fn point_grants_consume_the_shared_validation_budget() {
    use crate::storage::engine::{
        hlc::Ts,
        ssi_validate::ValidationConfig,
        txn::{SerializationReason, TxnError},
    };
    let mut config = ValidationConfig { scan_budget: 2 };
    // Missing keys and versions at the snapshot both cost one unit.
    config.validate_locked_point(None, Ts(10)).unwrap();
    config.validate_locked_point(Some(Ts(10)), Ts(10)).unwrap();
    assert_eq!(
        config.scan_budget, 0,
        "no budget remains for interval versions"
    );
    let error = config
        .validate_locked_point(Some(Ts(9)), Ts(10))
        .unwrap_err();
    assert!(matches!(
        error,
        TxnError::Serialization(SerializationReason::ValidationBudget)
    ));
    assert_eq!(crate::sqlstate(&crate::CoreError::from(error)), "40001");
    let mut config = ValidationConfig { scan_budget: 2 };
    assert!(matches!(
        config.validate_locked_point(Some(Ts(11)), Ts(10)),
        Err(TxnError::Serialization(
            SerializationReason::ReadWriteConflict
        ))
    ));
}

/// HD-4b: five point reads in one range use the batch validation path.
#[test]
fn five_point_reads_validate_in_one_range() {
    for changed in [false, true] {
        let (_cluster, a, b) = two(
            "CREATE TABLE t (id INT PRIMARY KEY, v INT); CREATE TABLE log (id INT PRIMARY KEY, v INT); INSERT INTO t VALUES (1, 0), (2, 0), (3, 0), (4, 0), (5, 0)",
        );
        let (mut sa, mut sb) = (a.admin.sql_session(), b.admin.sql_session());
        begin(&mut sa, "SERIALIZABLE");
        for id in 1..=5 {
            assert_eq!(int(&mut sa, &format!("SELECT v FROM t WHERE id = {id}")), 0);
            // SQL uses an interval scan here. Record actual point reads too,
            // so this test exercises batched point validation at commit.
            let mut key = 1_u64.to_be_bytes().to_vec();
            key.extend(crate::storage::engine::codec::pk_bytes(&[Value::Int(id)]).unwrap());
            assert!(
                sa.v2_transaction
                    .as_mut()
                    .unwrap()
                    .get(Keyspace::Rows, &key)
                    .unwrap()
                    .is_some()
            );
        }
        if changed {
            ok(&mut sb, "UPDATE t SET v = 9 WHERE id = 3");
        }
        ok(&mut sa, "INSERT INTO log VALUES (1, 1)");
        let result = exec(&mut sa, "COMMIT");
        assert_eq!(a.services.batch_counts(), (1, 0));
        if changed {
            assert_eq!(code(result), "40001");
        } else {
            result.unwrap();
        }
        assert_eq!(
            int(&mut sb, "SELECT count(*) FROM log"),
            i64::from(!changed)
        );
    }
}

/// A partially granted batch falls back to the existing waiting lock path.
#[test]
fn point_batch_falls_back_and_waits_for_open_writer() {
    for commit in [false, true] {
        let (_cluster, a, b) = two(
            "CREATE TABLE t (id INT PRIMARY KEY, v INT); CREATE TABLE log (id INT PRIMARY KEY, v INT); INSERT INTO t VALUES (1, 0), (2, 0), (3, 0), (4, 0), (5, 0)",
        );
        let (mut sa, mut sb) = (a.admin.sql_session(), b.admin.sql_session());
        begin(&mut sa, "SERIALIZABLE");
        for id in 1..=5 {
            assert_eq!(int(&mut sa, &format!("SELECT v FROM t WHERE id = {id}")), 0);
            // SQL uses an interval scan here. Record actual point reads too,
            // so this test exercises batched point validation at commit.
            let mut key = 1_u64.to_be_bytes().to_vec();
            key.extend(crate::storage::engine::codec::pk_bytes(&[Value::Int(id)]).unwrap());
            assert!(
                sa.v2_transaction
                    .as_mut()
                    .unwrap()
                    .get(Keyspace::Rows, &key)
                    .unwrap()
                    .is_some()
            );
        }
        ok(&mut sa, "INSERT INTO log VALUES (1, 1)");
        begin(&mut sb, "READ COMMITTED");
        ok(&mut sb, "UPDATE t SET v = 9 WHERE id = 3");
        std::thread::scope(|scope| {
            let (tx, rx) = mpsc::channel();
            scope.spawn(move || {
                tx.send(
                    exec(&mut sa, "COMMIT")
                        .map(|_| ())
                        .map_err(|e| crate::sqlstate(&e).to_string()),
                )
                .unwrap();
            });
            assert!(
                matches!(
                    rx.recv_timeout(Duration::from_millis(300)),
                    Err(mpsc::RecvTimeoutError::Timeout)
                ),
                "commit must wait for the open writer"
            );
            ok(&mut sb, if commit { "COMMIT" } else { "ROLLBACK" });
            let result = rx.recv_timeout(Duration::from_secs(30)).unwrap();
            assert_eq!(result, if commit { Err("40001".into()) } else { Ok(()) });
        });
        assert_eq!(a.services.batch_counts(), (0, 1));
        assert_eq!(int(&mut sb, "SELECT count(*) FROM log"), i64::from(!commit));
    }
}

/// HD-4d: SQL equality intervals alone reach batched point validation.
#[test]
fn sql_primary_key_reads_use_batch_validation() {
    for changed in [false, true] {
        let (_cluster, a, b) = two(
            "CREATE TABLE t (id INT PRIMARY KEY, v INT); CREATE TABLE log (id INT PRIMARY KEY, v INT); INSERT INTO t VALUES (1, 0), (2, 0), (3, 0)",
        );
        let (mut sa, mut sb) = (a.admin.sql_session(), b.admin.sql_session());
        begin(&mut sa, "SERIALIZABLE");
        for id in 1..=3 {
            assert_eq!(int(&mut sa, &format!("SELECT v FROM t WHERE id = {id}")), 0);
        }
        if changed {
            ok(&mut sb, "UPDATE t SET v = 9 WHERE id = 2");
        }
        ok(&mut sa, "INSERT INTO log VALUES (1, 1)");
        let result = exec(&mut sa, "COMMIT");
        assert_eq!(a.services.batch_counts(), (1, 0));
        if changed {
            assert_eq!(code(result), "40001");
        } else {
            result.unwrap();
        }
        assert_eq!(
            int(&mut sb, "SELECT count(*) FROM log"),
            i64::from(!changed)
        );
    }
}

/// Non-point SQL ranges must still protect gaps after equality reads are converted.
#[test]
fn sql_primary_key_batch_preserves_range_phantoms() {
    let (_cluster, a, b) = two(
        "CREATE TABLE t (id INT PRIMARY KEY, v INT); CREATE TABLE log (id INT PRIMARY KEY, v INT); INSERT INTO t VALUES (1, 0), (2, 0), (3, 0), (10, 0), (20, 0)",
    );
    let (mut sa, mut sb) = (a.admin.sql_session(), b.admin.sql_session());
    begin(&mut sa, "SERIALIZABLE");
    for id in 1..=3 {
        assert_eq!(int(&mut sa, &format!("SELECT v FROM t WHERE id = {id}")), 0);
    }
    assert_eq!(
        int(&mut sa, "SELECT count(*) FROM t WHERE id BETWEEN 10 AND 20"),
        2
    );
    ok(&mut sb, "INSERT INTO t VALUES (15, 9)");
    ok(&mut sa, "INSERT INTO log VALUES (1, 1)");
    assert_eq!(code(exec(&mut sa, "COMMIT")), "40001");
    assert_eq!(a.services.batch_counts(), (1, 0));
    assert_eq!(int(&mut sb, "SELECT count(*) FROM log"), 0);
}
