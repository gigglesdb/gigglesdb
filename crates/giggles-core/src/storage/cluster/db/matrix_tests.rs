//! Isolation anomaly matrix through cluster gateways (GD-0075 Phase 8, R5-74): every case of
//! PostgreSQL 18.6's matrix (J:/gigglesdb-work/r5/e/oracle and J:/gigglesdb-work/r5/p8/oracle)
//! at READ COMMITTED, REPEATABLE READ and SERIALIZABLE, with sessions A, B and C on gateways 1,
//! 2 and 3 of one in-process cluster. Steps run in order, as the PostgreSQL driver ran them: each
//! goes to its session's thread and gets `STEP_WAIT`; a step still running then is waiting (`~`)
//! and the driver moves on (that session's later steps queue behind it). Every step's outcome
//! and the final query must be PostgreSQL's, or a divergence written next to the expectation it
//! changes (published in GD-0075 E95, E151; letters as in the R5-74 brief).
use super::local::LocalCluster;
use super::*;
use crate::relational::{SqlResult, StatementClock, Value};
use crate::storage::SqlSession;
use crate::storage::range_api::{RangeSpace, range_key};
use std::collections::BTreeMap;
use std::sync::mpsc;
use std::time::{Duration, Instant};

const STEP_WAIT: Duration = Duration::from_millis(400);
const END_WAIT: Duration = Duration::from_secs(30);
const LEVELS: [&str; 3] = ["READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"];

/// Session, SQL (`@LEVEL@` is the level) and the expected outcome: a SELECT's rows joined by `,`
/// (columns by `|`), else the command tag, or `E:` and the SQLSTATE; `~` first where PostgreSQL
/// waited on a lock. `x;y;z` when it differs per level (READ COMMITTED; REPEATABLE READ;
/// SERIALIZABLE). `pg or ours` admits a listed divergence.
type Step = (char, &'static str, &'static str);

struct Case {
    setup: &'static str,
    steps: &'static [Step],
    /// The final query on a fresh session, and its rows (same encoding).
    fin: (&'static str, &'static str),
}

const T_V: &str = "CREATE TABLE t (id INT PRIMARY KEY, v INT); INSERT INTO t VALUES (1, 0)";
const BEGIN_A: Step = ('A', "BEGIN ISOLATION LEVEL @LEVEL@", "BEGIN");
const BEGIN_B: Step = ('B', "BEGIN ISOLATION LEVEL @LEVEL@", "BEGIN");
const P_C: &str = "CREATE TABLE p (id INT PRIMARY KEY); CREATE TABLE c (id INT PRIMARY KEY, p INT REFERENCES p (id)); INSERT INTO p VALUES (1), (2)";
/// PostgreSQL's final query is `SELECT 'p', id FROM p UNION ALL SELECT 'c', id FROM c ORDER BY
/// 1, 2`; a gateway rejects UNION ALL (finding, `union_all_through_a_gateway`), so the same rows
/// come from two queries here.
const P_C_FIN: &str = "SELECT 'c', id FROM c ORDER BY 2; SELECT 'p', id FROM p ORDER BY 2";

const LOST_UPDATE: Case = Case {
    setup: T_V,
    steps: &[
        BEGIN_A,
        BEGIN_B,
        ('A', "SELECT v FROM t WHERE id = 1", "0"),
        ('B', "SELECT v FROM t WHERE id = 1", "0"),
        ('A', "UPDATE t SET v = v + 1 WHERE id = 1", "UPDATE 1"),
        (
            'B',
            "UPDATE t SET v = v + 1 WHERE id = 1",
            "~UPDATE 1;~E:40001;~E:40001",
        ),
        ('A', "COMMIT", "COMMIT"),
        ('B', "COMMIT", "COMMIT;ROLLBACK;ROLLBACK"),
    ],
    fin: ("SELECT v FROM t", "2;1;1"),
};

const WRITE_SKEW: Case = Case {
    setup: "CREATE TABLE doctors (id INT PRIMARY KEY, oncall BOOLEAN); INSERT INTO doctors VALUES (1, true), (2, true)",
    steps: &[
        BEGIN_A,
        BEGIN_B,
        ('A', "SELECT count(*) FROM doctors WHERE oncall", "2"),
        ('B', "SELECT count(*) FROM doctors WHERE oncall", "2"),
        (
            'A',
            "UPDATE doctors SET oncall = false WHERE id = 1",
            "UPDATE 1",
        ),
        (
            'B',
            "UPDATE doctors SET oncall = false WHERE id = 2",
            "UPDATE 1",
        ),
        // (a): A's COMMIT waits for B's row in the interval it scanned, until B's COMMIT fails.
        ('A', "COMMIT", "COMMIT;COMMIT;COMMIT or ~COMMIT"),
        ('B', "COMMIT", "COMMIT;COMMIT;E:40001"),
    ],
    fin: ("SELECT count(*) FROM doctors WHERE oncall", "0;0;1"),
};

const WRITE_SKEW_ROW_READS: Case = Case {
    setup: "CREATE TABLE t (id INT PRIMARY KEY, v INT); INSERT INTO t VALUES (1, 0), (2, 0)",
    steps: &[
        BEGIN_A,
        BEGIN_B,
        ('A', "SELECT v FROM t WHERE id = 1", "0"),
        ('B', "SELECT v FROM t WHERE id = 2", "0"),
        ('A', "UPDATE t SET v = 1 WHERE id = 2", "UPDATE 1"),
        ('B', "UPDATE t SET v = 1 WHERE id = 1", "UPDATE 1"),
        // (a): A's COMMIT waits for B's write to the row A read, until B's COMMIT fails.
        ('A', "COMMIT", "COMMIT;COMMIT;COMMIT or ~COMMIT"),
        ('B', "COMMIT", "COMMIT;COMMIT;E:40001"),
    ],
    fin: ("SELECT sum(v) FROM t", "2;2;1"),
};

const PHANTOM_READ: Case = Case {
    setup: "CREATE TABLE t (id INT PRIMARY KEY, g INT); INSERT INTO t VALUES (1, 1)",
    steps: &[
        BEGIN_A,
        ('A', "SELECT count(*) FROM t WHERE g = 1", "1"),
        ('B', "INSERT INTO t VALUES (2, 1)", "INSERT 0 1"),
        ('A', "SELECT count(*) FROM t WHERE g = 1", "2;1;1"),
        ('A', "COMMIT", "COMMIT"),
    ],
    fin: ("SELECT count(*) FROM t", "2"),
};

const PHANTOM_WRITE_SKEW: Case = Case {
    setup: "CREATE TABLE t (id INT PRIMARY KEY, g INT); INSERT INTO t VALUES (1, 1), (2, 2)",
    steps: &[
        BEGIN_A,
        BEGIN_B,
        ('A', "SELECT count(*) FROM t WHERE g = 1", "1"),
        ('B', "SELECT count(*) FROM t WHERE g = 2", "1"),
        ('A', "INSERT INTO t VALUES (3, 2)", "INSERT 0 1"),
        ('B', "INSERT INTO t VALUES (4, 1)", "INSERT 0 1"),
        // (a): A's COMMIT waits for B's insert into the interval A scanned.
        ('A', "COMMIT", "COMMIT;COMMIT;COMMIT or ~COMMIT"),
        ('B', "COMMIT", "COMMIT;COMMIT;E:40001"),
    ],
    fin: ("SELECT count(*) FROM t", "4;4;3"),
};

const NONREPEATABLE_READ: Case = Case {
    setup: T_V,
    steps: &[
        BEGIN_A,
        ('A', "SELECT v FROM t WHERE id = 1", "0"),
        ('B', "UPDATE t SET v = 7 WHERE id = 1", "UPDATE 1"),
        ('A', "SELECT v FROM t WHERE id = 1", "7;0;0"),
        ('A', "COMMIT", "COMMIT"),
    ],
    fin: ("SELECT v FROM t", "7"),
};

const SNAPSHOT_AT_FIRST_STATEMENT: Case = Case {
    setup: T_V,
    steps: &[
        BEGIN_A,
        ('B', "UPDATE t SET v = 9 WHERE id = 1", "UPDATE 1"),
        ('A', "SELECT v FROM t WHERE id = 1", "9"),
        ('B', "UPDATE t SET v = 10 WHERE id = 1", "UPDATE 1"),
        ('A', "SELECT v FROM t WHERE id = 1", "10;9;9"),
        ('A', "COMMIT", "COMMIT"),
    ],
    fin: ("SELECT v FROM t", "10"),
};

const READ_ONLY_ANOMALY: Case = Case {
    setup: "CREATE TABLE acct (id INT PRIMARY KEY, bal INT); INSERT INTO acct VALUES (1, 0), (2, 0)",
    steps: &[
        BEGIN_B,
        ('B', "SELECT sum(bal) FROM acct", "0"),
        BEGIN_A,
        (
            'A',
            "UPDATE acct SET bal = bal + 20 WHERE id = 2",
            "UPDATE 1",
        ),
        ('A', "COMMIT", "COMMIT"),
        ('C', "BEGIN ISOLATION LEVEL @LEVEL@ READ ONLY", "BEGIN"),
        ('C', "SELECT id, bal FROM acct ORDER BY id", "1|0,2|20"),
        ('C', "COMMIT", "COMMIT"),
        // (d): PostgreSQL fails B's UPDATE; B's COMMIT fails here (same final state).
        (
            'B',
            "UPDATE acct SET bal = bal - 11 WHERE id = 1",
            "UPDATE 1;UPDATE 1;E:40001 or UPDATE 1",
        ),
        ('B', "COMMIT", "COMMIT;COMMIT;ROLLBACK or E:40001"),
    ],
    fin: (
        "SELECT id, bal FROM acct ORDER BY id",
        "1|-11,2|20;1|-11,2|20;1|0,2|20",
    ),
};

const DELETE_CONFLICT: Case = Case {
    setup: T_V,
    steps: &[
        BEGIN_A,
        BEGIN_B,
        ('A', "SELECT v FROM t WHERE id = 1", "0"),
        ('B', "SELECT v FROM t WHERE id = 1", "0"),
        ('A', "DELETE FROM t WHERE id = 1", "DELETE 1"),
        (
            'B',
            "UPDATE t SET v = 5 WHERE id = 1",
            "~UPDATE 0;~E:40001;~E:40001",
        ),
        ('A', "COMMIT", "COMMIT"),
        ('B', "SELECT v FROM t WHERE id = 1", ";E:25P02;E:25P02"),
        ('B', "COMMIT", "COMMIT;ROLLBACK;ROLLBACK"),
    ],
    fin: ("SELECT count(*) FROM t", "0"),
};

const INSERT_CONFLICT: Case = Case {
    setup: "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
    steps: &[
        BEGIN_A,
        BEGIN_B,
        ('A', "SELECT count(*) FROM t", "0"),
        ('B', "SELECT count(*) FROM t", "0"),
        ('A', "INSERT INTO t VALUES (1, 1)", "INSERT 0 1"),
        // (e): at REPEATABLE READ the key committed since B's snapshot may fail 40001.
        (
            'B',
            "INSERT INTO t VALUES (1, 2)",
            "~E:23505;~E:23505 or ~E:40001;~E:40001",
        ),
        ('A', "COMMIT", "COMMIT"),
        ('B', "COMMIT", "ROLLBACK"),
    ],
    fin: ("SELECT v FROM t", "1"),
};

const DELETE_RECREATE: Case = Case {
    setup: T_V,
    steps: &[
        BEGIN_A,
        ('A', "SELECT v FROM t WHERE id = 1", "0"),
        ('B', "DELETE FROM t WHERE id = 1", "DELETE 1"),
        ('B', "INSERT INTO t VALUES (1, 5)", "INSERT 0 1"),
        ('A', "SELECT v FROM t WHERE id = 1", "5;0;0"),
        (
            'A',
            "UPDATE t SET v = v + 1 WHERE id = 1",
            "UPDATE 1;E:40001;E:40001",
        ),
        ('A', "COMMIT", "COMMIT;ROLLBACK;ROLLBACK"),
    ],
    fin: ("SELECT v FROM t", "6;5;5"),
};

const PARENT_DELETE_THEN_CHILD_INSERT: Case = Case {
    setup: P_C,
    steps: &[
        BEGIN_A,
        BEGIN_B,
        ('A', "SELECT count(*) FROM p", "2"),
        ('B', "SELECT count(*) FROM c", "0"),
        ('A', "DELETE FROM p WHERE id = 1", "DELETE 1"),
        // At SERIALIZABLE B's INSERT neither waits nor fails; its COMMIT fails 40001 instead
        // (its foreign-key read of p 1 was deleted and committed: (b), at COMMIT as in (d);
        // same final state). At READ COMMITTED and REPEATABLE READ: finding, see the tests.
        (
            'B',
            "INSERT INTO c VALUES (1, 1)",
            "~E:23503;~E:40001;~E:40001 or INSERT 0 1",
        ),
        ('A', "COMMIT", "COMMIT"),
        ('B', "COMMIT", "ROLLBACK;ROLLBACK;ROLLBACK or E:40001"),
    ],
    fin: (P_C_FIN, "p|2"),
};

const CHILD_INSERT_THEN_PARENT_DELETE: Case = Case {
    setup: P_C,
    steps: &[
        BEGIN_A,
        BEGIN_B,
        ('A', "SELECT count(*) FROM p", "2"),
        ('B', "SELECT count(*) FROM c", "0"),
        ('B', "INSERT INTO c VALUES (1, 1)", "INSERT 0 1"),
        // (c) at SERIALIZABLE: A's DELETE does not wait for B's uncommitted child; the COMMITs
        // then deadlock and the younger B fails 40001, so p 1 goes instead of c 1 coming. At
        // READ COMMITTED and REPEATABLE READ: finding, see the tests.
        (
            'A',
            "DELETE FROM p WHERE id = 1",
            "~E:23503;~E:23503;~E:23503 or DELETE 1",
        ),
        ('B', "COMMIT", "COMMIT;COMMIT;COMMIT or ~E:40001 or E:40001"),
        ('A', "COMMIT", "ROLLBACK;ROLLBACK;ROLLBACK or COMMIT"),
    ],
    fin: (P_C_FIN, "c|1,p|1,p|2;c|1,p|1,p|2;c|1,p|1,p|2 or p|2"),
};

const COLLATED_P_C: &str = "CREATE COLLATION ci (provider=icu,locale='und-u-ks-level2',deterministic=false); CREATE TABLE p (id TEXT COLLATE ci PRIMARY KEY); CREATE TABLE c (id INT PRIMARY KEY, p TEXT COLLATE ci REFERENCES p(id)); INSERT INTO p VALUES ('one'),('two')";
const COLLATED_PARENT_DELETE_THEN_CHILD_INSERT: Case = Case {
    setup: COLLATED_P_C,
    steps: &[
        BEGIN_A, BEGIN_B,
        ('A', "SELECT count(*) FROM p", "2"),
        ('B', "SELECT count(*) FROM c", "0"),
        ('A', "DELETE FROM p WHERE id='one'", "DELETE 1"),
        ('B', "INSERT INTO c VALUES (1,'ONE')", "~E:23503;~E:40001;~E:40001"),
        ('A', "COMMIT", "COMMIT"),
        ('B', "COMMIT", "ROLLBACK"),
    ],
    fin: (P_C_FIN, "p|two"),
};
const COLLATED_CHILD_INSERT_THEN_PARENT_DELETE: Case = Case {
    setup: COLLATED_P_C,
    steps: &[
        BEGIN_A, BEGIN_B,
        ('A', "SELECT count(*) FROM p", "2"),
        ('B', "SELECT count(*) FROM c", "0"),
        ('B', "INSERT INTO c VALUES (1,'ONE')", "INSERT 0 1"),
        ('A', "DELETE FROM p WHERE id='one'", "~E:23503"),
        ('B', "COMMIT", "COMMIT"),
        ('A', "COMMIT", "ROLLBACK"),
    ],
    fin: (P_C_FIN, "c|1,p|one,p|two"),
};

/// The outcomes `spec` admits at `level`: PostgreSQL's first, then any listed divergence.
fn admitted(spec: &str, level: usize) -> Vec<&str> {
    let per_level: Vec<&str> = spec.split(';').collect();
    let spec = if per_level.len() == 3 {
        per_level[level]
    } else {
        assert_eq!(per_level.len(), 1, "{spec}");
        per_level[0]
    };
    spec.split(" or ").collect()
}

fn clock() -> StatementClock {
    StatementClock {
        unavailable: None,
        transaction_us: 100,
        statement_us: 100,
        zone: Some(giggles_pg_types::temporal::SessionZone::Fixed(0)),
        wall: None,
    }
}

/// Each of the first tables' rows and index entries in their own ranges (as
/// serializable_tests.rs), so the two tables of the foreign-key cases sit in different ranges.
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

fn render_rows(result: &SqlResult) -> String {
    let value = |v: &Value| match v {
        Value::Null => String::new(),
        Value::Bool(b) => String::from(if *b { "t" } else { "f" }),
        Value::Int(n) => n.to_string(),
        Value::Text(s) => s.clone(),
        other => format!("{other:?}"),
    };
    let rows: Vec<String> = result
        .rows
        .iter()
        .map(|row| row.values.iter().map(value).collect::<Vec<_>>().join("|"))
        .collect();
    rows.join(",")
}

/// One session's statements, in PostgreSQL's terms (rows, command tag or `E:` SQLSTATE).
/// Core does not parse `BEGIN ISOLATION LEVEL`: the level is set on the session first, as
/// giggles-server does; `READ ONLY` is dropped (a read-only transaction here only reads). COMMIT
/// of a failed block runs ROLLBACK and reports it, as giggles-server and PostgreSQL do.
struct Session {
    sql: SqlSession,
    level: crate::SqlIsolation,
}

impl Session {
    fn step(&mut self, sql: &str) -> String {
        let mut verb = sql.split_whitespace().next().unwrap_or_default();
        if verb == "COMMIT" && self.sql.is_aborted() {
            verb = "ROLLBACK";
        }
        let result = match verb {
            "BEGIN" => {
                self.sql.set_isolation(self.level);
                self.sql.execute_at("BEGIN", &[], None, &clock())
            }
            "ROLLBACK" => self.sql.execute_at("ROLLBACK", &[], None, &clock()),
            _ => self.sql.execute_at(sql, &[], None, &clock()),
        };
        match &result {
            Err(e) => format!("E:{}", crate::sqlstate(e)),
            Ok(r) => match verb {
                "SELECT" => render_rows(r),
                "BEGIN" | "COMMIT" | "ROLLBACK" => verb.into(),
                "INSERT" => format!("INSERT 0 {}", r.affected),
                _ => format!("{verb} {}", r.affected),
            },
        }
    }

    /// The rows of `sql`'s queries (`;`-separated), concatenated.
    fn rows(&mut self, sql: &str) -> String {
        let rows: Vec<String> = sql
            .split(';')
            .map(|query| self.step(query.trim()))
            .collect();
        let rows: Vec<&str> = rows
            .iter()
            .map(String::as_str)
            .filter(|r| !r.is_empty())
            .collect();
        rows.join(",")
    }
}

/// Runs `case` at `level` and returns its transcript (step, SQL, PostgreSQL, ours) and the
/// mismatches outside the admitted outcomes.
fn run(case: &Case, level: usize) -> (Vec<String>, Vec<String>) {
    let cluster = LocalCluster::new(&splits());
    let gateways: Vec<Gateway> = (1..=3).map(|node| gateway(&cluster, node)).collect();
    let mut setup = gateways[0].admin.sql_session();
    for sql in case.setup.split(';').filter(|sql| !sql.trim().is_empty()) {
        setup
            .execute_at(sql, &[], None, &clock())
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
    let isolation = [
        crate::SqlIsolation::ReadCommitted,
        crate::SqlIsolation::RepeatableRead,
        crate::SqlIsolation::Serializable,
    ][level];
    let (done_tx, done) = mpsc::channel::<(usize, String)>();
    let mut inputs = BTreeMap::new();
    let mut threads = Vec::new();
    for (name, gateway) in ['A', 'B', 'C'].into_iter().zip(&gateways) {
        let (tx, rx) = mpsc::channel::<(usize, String)>();
        inputs.insert(name, tx);
        let mut session = Session {
            sql: gateway.admin.sql_session(),
            level: isolation,
        };
        let done_tx = done_tx.clone();
        threads.push(std::thread::spawn(move || {
            for (i, sql) in rx {
                let _ = done_tx.send((i, session.step(&sql)));
            }
        }));
    }
    drop(done_tx);
    let mut outcomes: BTreeMap<usize, String> = BTreeMap::new();
    let mut waited = vec![false; case.steps.len()];
    for (i, (name, sql, _)) in case.steps.iter().enumerate() {
        inputs[name]
            .send((i, sql.replace("@LEVEL@", LEVELS[level])))
            .unwrap();
        let deadline = Instant::now() + STEP_WAIT;
        while !outcomes.contains_key(&i) {
            let left = deadline.saturating_duration_since(Instant::now());
            match done.recv_timeout(left) {
                Ok((j, out)) => {
                    outcomes.insert(j, out);
                }
                Err(_) => {
                    waited[i] = true;
                    break;
                }
            }
        }
    }
    drop(inputs);
    let deadline = Instant::now() + END_WAIT;
    while outcomes.len() < case.steps.len() {
        let left = deadline.saturating_duration_since(Instant::now());
        let Ok((j, out)) = done.recv_timeout(left) else {
            break;
        };
        outcomes.insert(j, out);
    }
    if outcomes.len() == case.steps.len() {
        for thread in threads {
            thread.join().unwrap();
        }
    }
    let mut transcript = Vec::new();
    let mut mismatches = Vec::new();
    let mut compare = |label: String, spec: &str, ours: String| {
        let admitted = admitted(spec, level);
        transcript.push(format!("{label}: pg {:?} ours {ours:?}", admitted[0]));
        if !admitted.contains(&ours.as_str()) {
            mismatches.push(format!("{label}: admitted {admitted:?}, got {ours:?}"));
        }
    };
    for (i, (name, sql, spec)) in case.steps.iter().enumerate() {
        let out = outcomes
            .get(&i)
            .cloned()
            .unwrap_or_else(|| "TIMEOUT".into());
        let ours = if waited[i] { format!("~{out}") } else { out };
        compare(format!("[{}] {name}: {sql}", i + 1), spec, ours);
    }
    let mut last = Session {
        sql: gateways[0].admin.sql_session(),
        level: isolation,
    };
    let fin = last.rows(case.fin.0);
    compare(format!("final: {}", case.fin.0), case.fin.1, fin);
    (transcript, mismatches)
}

fn check(case: &Case, level: usize) {
    let (transcript, mismatches) = run(case, level);
    // With --nocapture: which outcome (PostgreSQL's or a divergence) each step had.
    eprintln!("{}:\n{}", LEVELS[level], transcript.join("\n"));
    assert!(
        mismatches.is_empty(),
        "{}: mismatches with PostgreSQL 18.6:\n{}\ntranscript:\n{}",
        LEVELS[level],
        mismatches.join("\n"),
        transcript.join("\n")
    );
}

macro_rules! matrix {
    ($($(#[$meta:meta])* $name:ident: $case:ident, $level:expr;)*) => {
        $( $(#[$meta])* #[test] fn $name() { check(&$case, $level); } )*
    };
}

matrix! {
    lost_update_rc: LOST_UPDATE, 0;
    lost_update_rr: LOST_UPDATE, 1;
    lost_update_ser: LOST_UPDATE, 2;
    write_skew_rc: WRITE_SKEW, 0;
    write_skew_rr: WRITE_SKEW, 1;
    write_skew_ser: WRITE_SKEW, 2;
    write_skew_row_reads_rc: WRITE_SKEW_ROW_READS, 0;
    write_skew_row_reads_rr: WRITE_SKEW_ROW_READS, 1;
    write_skew_row_reads_ser: WRITE_SKEW_ROW_READS, 2;
    phantom_read_rc: PHANTOM_READ, 0;
    phantom_read_rr: PHANTOM_READ, 1;
    phantom_read_ser: PHANTOM_READ, 2;
    phantom_write_skew_rc: PHANTOM_WRITE_SKEW, 0;
    phantom_write_skew_rr: PHANTOM_WRITE_SKEW, 1;
    phantom_write_skew_ser: PHANTOM_WRITE_SKEW, 2;
    nonrepeatable_read_rc: NONREPEATABLE_READ, 0;
    nonrepeatable_read_rr: NONREPEATABLE_READ, 1;
    nonrepeatable_read_ser: NONREPEATABLE_READ, 2;
    snapshot_at_first_statement_rc: SNAPSHOT_AT_FIRST_STATEMENT, 0;
    snapshot_at_first_statement_rr: SNAPSHOT_AT_FIRST_STATEMENT, 1;
    snapshot_at_first_statement_ser: SNAPSHOT_AT_FIRST_STATEMENT, 2;
    read_only_anomaly_rc: READ_ONLY_ANOMALY, 0;
    read_only_anomaly_rr: READ_ONLY_ANOMALY, 1;
    read_only_anomaly_ser: READ_ONLY_ANOMALY, 2;
    delete_conflict_rc: DELETE_CONFLICT, 0;
    delete_conflict_rr: DELETE_CONFLICT, 1;
    delete_conflict_ser: DELETE_CONFLICT, 2;
    insert_conflict_rc: INSERT_CONFLICT, 0;
    insert_conflict_rr: INSERT_CONFLICT, 1;
    insert_conflict_ser: INSERT_CONFLICT, 2;
    delete_recreate_rc: DELETE_RECREATE, 0;
    delete_recreate_rr: DELETE_RECREATE, 1;
    delete_recreate_ser: DELETE_RECREATE, 2;
    parent_delete_then_child_insert_rc: PARENT_DELETE_THEN_CHILD_INSERT, 0;
    parent_delete_then_child_insert_rr: PARENT_DELETE_THEN_CHILD_INSERT, 1;
    parent_delete_then_child_insert_ser: PARENT_DELETE_THEN_CHILD_INSERT, 2;
    child_insert_then_parent_delete_rc: CHILD_INSERT_THEN_PARENT_DELETE, 0;
    child_insert_then_parent_delete_rr: CHILD_INSERT_THEN_PARENT_DELETE, 1;
    child_insert_then_parent_delete_ser: CHILD_INSERT_THEN_PARENT_DELETE, 2;
    collated_parent_delete_then_child_insert_rc: COLLATED_PARENT_DELETE_THEN_CHILD_INSERT, 0;
    collated_parent_delete_then_child_insert_rr: COLLATED_PARENT_DELETE_THEN_CHILD_INSERT, 1;
    collated_parent_delete_then_child_insert_ser: COLLATED_PARENT_DELETE_THEN_CHILD_INSERT, 2;
    collated_child_insert_then_parent_delete_rc: COLLATED_CHILD_INSERT_THEN_PARENT_DELETE, 0;
    collated_child_insert_then_parent_delete_rr: COLLATED_CHILD_INSERT_THEN_PARENT_DELETE, 1;
    collated_child_insert_then_parent_delete_ser: COLLATED_CHILD_INSERT_THEN_PARENT_DELETE, 2;
}

/// PostgreSQL's final query of the foreign-key cases, through a gateway.
#[test]
#[ignore = "GD-0075 R5-74 finding: a cluster gateway rejects UNION ALL (0A000)"]
fn union_all_through_a_gateway() {
    let cluster = LocalCluster::new(&splits());
    let gateway = gateway(&cluster, 1);
    let mut session = Session {
        sql: gateway.admin.sql_session(),
        level: crate::SqlIsolation::ReadCommitted,
    };
    session.rows(P_C);
    session.rows("INSERT INTO c VALUES (1, 1)");
    let union = "SELECT 'p', id FROM p UNION ALL SELECT 'c', id FROM c ORDER BY 1, 2";
    assert_eq!(session.rows(union), "c|1,p|1,p|2");
}
