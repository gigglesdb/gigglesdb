#![cfg(feature = "engine-rocksdb")]
//! Isolation anomaly matrix through cluster gateways at the process level (GD-0075 Phase 8,
//! R5-74). LONG: one 3-node cluster of server processes (tens of seconds to start) runs every
//! case of PostgreSQL 18.6's matrix (J:/gigglesdb-work/r5/e/oracle, J:/gigglesdb-work/r5/p8/oracle)
//! at READ COMMITTED, REPEATABLE READ and SERIALIZABLE, each case and level over its own tables
//! (name suffix), sessions A, B and C on nodes 1, 2 and 3 with real `BEGIN ISOLATION LEVEL`.
//! Steps run in order as the PostgreSQL driver ran them: each goes to its session's task and
//! gets `STEP_WAIT`; a step still running then is waiting (`~`) and the driver moves on (that
//! session's later steps queue behind it). Every outcome and the final query must be
//! PostgreSQL's, or a divergence written next to the expectation it changes (GD-0075 E95,
//! E151; letters as in the R5-74 brief). Also R5-70: a gateway reports and runs the level set
//! by `BEGIN ISOLATION LEVEL`, `SET TRANSACTION` and `SET SESSION CHARACTERISTICS`.
use crate::cluster_harness::ClusterProcs;
use std::{collections::BTreeMap, time::Duration};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_postgres::{Client, SimpleQueryMessage};

/// PostgreSQL's driver gave each step 400 ms. A statement through server processes on a loaded
/// host can take longer than that without waiting on any lock, so steps get 2 s here; every
/// lock wait in these cases lasts until a later step ends the transaction holding the lock.
const STEP_WAIT: Duration = Duration::from_secs(2);
const END_WAIT: Duration = Duration::from_secs(30);
const LEVELS: [&str; 3] = ["READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"];

/// Session, SQL (`@LEVEL@` is the level) and the expected outcome: a SELECT's rows joined by `,`
/// (columns by `|`), else the command tag, or `E:` and the SQLSTATE; `~` first where PostgreSQL
/// waited on a lock. `x;y;z` when it differs per level (READ COMMITTED; REPEATABLE READ;
/// SERIALIZABLE). `pg or ours` admits a listed divergence.
type Step = (char, &'static str, &'static str);

struct Case {
    name: &'static str,
    setup: &'static str,
    steps: &'static [Step],
    /// The final query on a fresh session, and its rows (same encoding).
    fin: (&'static str, &'static str),
}

const T_V: &str = "CREATE TABLE t (id int PRIMARY KEY, v int); INSERT INTO t VALUES (1, 0);";
const BEGIN_A: Step = ('A', "BEGIN ISOLATION LEVEL @LEVEL@", "BEGIN");
const BEGIN_B: Step = ('B', "BEGIN ISOLATION LEVEL @LEVEL@", "BEGIN");
const P_C: &str = "CREATE TABLE p (id INT PRIMARY KEY); CREATE TABLE c (id INT PRIMARY KEY, p INT REFERENCES p (id)); INSERT INTO p VALUES (1), (2);";
/// The isolation cases concatenate two queries; `foreign_key_findings` separately checks
/// PostgreSQL's UNION ALL final query, `P_C_UNION`.
const P_C_FIN: &str = "SELECT 'c', id FROM c ORDER BY 2; SELECT 'p', id FROM p ORDER BY 2";
const P_C_UNION: &str = "SELECT 'p', id FROM p UNION ALL SELECT 'c', id FROM c ORDER BY 1, 2";
/// Former findings (GD-0075 R5-74): concurrent parent DELETE and child INSERT left an orphan
/// child at READ COMMITTED and REPEATABLE READ. Regressions run by `foreign_key_findings`.
const FINDINGS: [(&str, usize); 4] = [
    ("parent_delete_then_child_insert", 0),
    ("parent_delete_then_child_insert", 1),
    ("child_insert_then_parent_delete", 0),
    ("child_insert_then_parent_delete", 1),
];
/// In a SERIALIZABLE write skew both COMMITs may wait (a): each takes shared locks on what the
/// other wrote; the second COMMIT's wait ends when deadlock detection fails it (40001).
const SKEW_COMMITS: [Step; 2] = [
    ('A', "COMMIT", "COMMIT;COMMIT;COMMIT or ~COMMIT"),
    ('B', "COMMIT", "COMMIT;COMMIT;E:40001 or ~E:40001"),
];

/// The foreign-key cases first, so p and c get table ids the harness splits into their own ranges.
const CASES: [Case; 13] = [
    Case {
        name: "parent_delete_then_child_insert",
        setup: P_C,
        steps: &[
            BEGIN_A,
            BEGIN_B,
            ('A', "SELECT count(*) FROM p", "2"),
            ('B', "SELECT count(*) FROM c", "0"),
            ('A', "DELETE FROM p WHERE id = 1", "DELETE 1"),
            // At SERIALIZABLE B's INSERT neither waits nor fails; its COMMIT fails 40001
            // instead (its foreign-key read of p 1 was deleted and committed: (b), at COMMIT as
            // in (d); same final state). READ COMMITTED, REPEATABLE READ: FINDINGS.
            (
                'B',
                "INSERT INTO c VALUES (1, 1)",
                "~E:23503;~E:40001;~E:40001 or INSERT 0 1",
            ),
            ('A', "COMMIT", "COMMIT"),
            ('B', "COMMIT", "ROLLBACK;ROLLBACK;ROLLBACK or E:40001"),
        ],
        fin: (P_C_FIN, "p|2"),
    },
    Case {
        name: "child_insert_then_parent_delete",
        setup: P_C,
        steps: &[
            BEGIN_A,
            BEGIN_B,
            ('A', "SELECT count(*) FROM p", "2"),
            ('B', "SELECT count(*) FROM c", "0"),
            ('B', "INSERT INTO c VALUES (1, 1)", "INSERT 0 1"),
            // (c) at SERIALIZABLE: A's DELETE does not wait for B's uncommitted child; the
            // COMMITs then deadlock and the younger B fails 40001, so p 1 goes instead of c 1
            // coming. READ COMMITTED, REPEATABLE READ: FINDINGS.
            (
                'A',
                "DELETE FROM p WHERE id = 1",
                "~E:23503;~E:23503;~E:23503 or DELETE 1",
            ),
            ('B', "COMMIT", "COMMIT;COMMIT;COMMIT or ~E:40001 or E:40001"),
            ('A', "COMMIT", "ROLLBACK;ROLLBACK;ROLLBACK or COMMIT"),
        ],
        fin: (P_C_FIN, "c|1,p|1,p|2;c|1,p|1,p|2;c|1,p|1,p|2 or p|2"),
    },
    Case {
        name: "lost_update",
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
    },
    Case {
        name: "write_skew",
        setup: "CREATE TABLE doctors (id int PRIMARY KEY, oncall boolean); INSERT INTO doctors VALUES (1, true), (2, true);",
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
            SKEW_COMMITS[0],
            SKEW_COMMITS[1],
        ],
        fin: ("SELECT count(*) FROM doctors WHERE oncall", "0;0;1"),
    },
    Case {
        name: "write_skew_row_reads",
        setup: "CREATE TABLE t (id int PRIMARY KEY, v int); INSERT INTO t VALUES (1, 0), (2, 0);",
        steps: &[
            BEGIN_A,
            BEGIN_B,
            ('A', "SELECT v FROM t WHERE id = 1", "0"),
            ('B', "SELECT v FROM t WHERE id = 2", "0"),
            ('A', "UPDATE t SET v = 1 WHERE id = 2", "UPDATE 1"),
            ('B', "UPDATE t SET v = 1 WHERE id = 1", "UPDATE 1"),
            SKEW_COMMITS[0],
            SKEW_COMMITS[1],
        ],
        fin: ("SELECT sum(v) FROM t", "2;2;1"),
    },
    Case {
        name: "phantom_read",
        setup: "CREATE TABLE t (id int PRIMARY KEY, g int); INSERT INTO t VALUES (1, 1);",
        steps: &[
            BEGIN_A,
            ('A', "SELECT count(*) FROM t WHERE g = 1", "1"),
            ('B', "INSERT INTO t VALUES (2, 1)", "INSERT 0 1"),
            ('A', "SELECT count(*) FROM t WHERE g = 1", "2;1;1"),
            ('A', "COMMIT", "COMMIT"),
        ],
        fin: ("SELECT count(*) FROM t", "2"),
    },
    Case {
        name: "phantom_write_skew",
        setup: "CREATE TABLE t (id int PRIMARY KEY, g int); INSERT INTO t VALUES (1, 1), (2, 2);",
        steps: &[
            BEGIN_A,
            BEGIN_B,
            ('A', "SELECT count(*) FROM t WHERE g = 1", "1"),
            ('B', "SELECT count(*) FROM t WHERE g = 2", "1"),
            ('A', "INSERT INTO t VALUES (3, 2)", "INSERT 0 1"),
            ('B', "INSERT INTO t VALUES (4, 1)", "INSERT 0 1"),
            SKEW_COMMITS[0],
            SKEW_COMMITS[1],
        ],
        fin: ("SELECT count(*) FROM t", "4;4;3"),
    },
    Case {
        name: "nonrepeatable_read",
        setup: T_V,
        steps: &[
            BEGIN_A,
            ('A', "SELECT v FROM t WHERE id = 1", "0"),
            ('B', "UPDATE t SET v = 7 WHERE id = 1", "UPDATE 1"),
            ('A', "SELECT v FROM t WHERE id = 1", "7;0;0"),
            ('A', "COMMIT", "COMMIT"),
        ],
        fin: ("SELECT v FROM t", "7"),
    },
    Case {
        name: "snapshot_at_first_statement",
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
    },
    Case {
        name: "read_only_anomaly",
        setup: "CREATE TABLE acct (id int PRIMARY KEY, bal int); INSERT INTO acct VALUES (1, 0), (2, 0);",
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
    },
    Case {
        name: "delete_conflict",
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
    },
    Case {
        name: "insert_conflict",
        setup: "CREATE TABLE t (id int PRIMARY KEY, v int);",
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
    },
    Case {
        name: "delete_recreate",
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
    },
];

/// The outcomes `spec` admits at `level`: PostgreSQL's first, then any listed divergence.
fn admitted(spec: &str, level: usize) -> Vec<&str> {
    let per_level: Vec<&str> = spec.split(';').collect();
    let spec = match per_level.len() {
        3 => per_level[level],
        1 => per_level[0],
        _ => panic!("{spec}"),
    };
    spec.split(" or ").collect()
}

/// `sql` with every table name of `tables` (outside string literals) suffixed.
fn suffixed(sql: &str, tables: &[String], suffix: &str) -> String {
    let (mut out, mut word, mut quoted) = (String::new(), String::new(), false);
    for ch in sql.chars().chain(std::iter::once(' ')) {
        if !quoted && (ch.is_ascii_alphanumeric() || ch == '_') {
            word.push(ch);
            continue;
        }
        if tables.contains(&word) {
            word.push_str(suffix);
        }
        out.push_str(&std::mem::take(&mut word));
        quoted ^= ch == '\'';
        out.push(ch);
    }
    out.pop();
    out
}

/// How a session sets the level: `BEGIN ISOLATION LEVEL x`; `SET SESSION CHARACTERISTICS AS
/// TRANSACTION ISOLATION LEVEL x` then `BEGIN`; or `BEGIN` then `SET TRANSACTION ISOLATION LEVEL
/// x`, as two queries or (SET TRANSACTION) batched in one simple query. Not `SET SESSION
/// CHARACTERISTICS ...; BEGIN` batched: the oracle has no such run (the gateway then runs the
/// previous level, as PostgreSQL may too: its implicit transaction starts before the SET).
#[derive(Clone, Copy, Debug, PartialEq)]
enum Form {
    Begin,
    Session,
    SetTransaction,
    SetTransactionBatched,
}

/// One session's statements, in PostgreSQL's terms (rows, command tag or `E:` SQLSTATE).
struct Session {
    client: Client,
    form: Form,
    /// Inside a transaction block, and whether a statement in it failed (COMMIT then reports
    /// ROLLBACK, as PostgreSQL's tag does; tokio-postgres keeps only the row count).
    block: bool,
    failed: bool,
}

impl Session {
    async fn step(&mut self, sql: &str) -> String {
        let verb = sql.split_whitespace().next().unwrap_or_default().to_owned();
        let queries: Vec<String> = match sql.strip_prefix("BEGIN ISOLATION LEVEL ") {
            Some(rest) if self.form != Form::Begin => {
                let (level, begin) = match rest.strip_suffix(" READ ONLY") {
                    Some(level) => (level, "BEGIN READ ONLY"),
                    None => (rest, "BEGIN"),
                };
                let session =
                    format!("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {level}");
                let transaction = format!("SET TRANSACTION ISOLATION LEVEL {rest}");
                match self.form {
                    Form::Session => vec![session, begin.into()],
                    Form::SetTransaction => vec!["BEGIN".into(), transaction],
                    _ => vec![format!("BEGIN; {transaction}")],
                }
            }
            _ => vec![sql.to_owned()],
        };
        let mut result = Ok(Vec::new());
        for query in queries {
            result = self.client.simple_query(&query).await;
            if result.is_err() {
                break;
            }
        }
        let out = match &result {
            Err(e) => match e.code() {
                Some(state) => format!("E:{}", state.code()),
                None => format!("E:{e}"),
            },
            Ok(messages) => {
                let rows: Vec<String> = messages
                    .iter()
                    .filter_map(|m| match m {
                        SimpleQueryMessage::Row(row) => Some(
                            (0..row.len())
                                .map(|i| row.get(i).unwrap_or(""))
                                .collect::<Vec<_>>()
                                .join("|"),
                        ),
                        _ => None,
                    })
                    .collect();
                let count = messages
                    .iter()
                    .find_map(|m| match m {
                        SimpleQueryMessage::CommandComplete(n) => Some(*n),
                        _ => None,
                    })
                    .unwrap_or_default();
                match verb.as_str() {
                    "SELECT" => rows.join(","),
                    "BEGIN" => "BEGIN".into(),
                    "COMMIT" if self.failed => "ROLLBACK".into(),
                    "COMMIT" => "COMMIT".into(),
                    "INSERT" => format!("INSERT 0 {count}"),
                    _ => format!("{verb} {count}"),
                }
            }
        };
        match verb.as_str() {
            "BEGIN" => (self.block, self.failed) = (result.is_ok(), false),
            "COMMIT" | "ROLLBACK" => (self.block, self.failed) = (false, false),
            _ => self.failed |= self.block && result.is_err(),
        }
        out
    }
}

/// Runs `case` at `level` over its own tables and returns its transcript and the mismatches
/// outside the admitted outcomes.
async fn run(
    procs: &ClusterProcs,
    case: &Case,
    level: usize,
    form: Form,
) -> (Vec<String>, Vec<String>) {
    let tables: Vec<String> = case
        .setup
        .split("CREATE TABLE ")
        .skip(1)
        .map(|rest| rest.split_whitespace().next().unwrap().to_owned())
        .collect();
    let suffix = format!(
        "_{}_{}_{}",
        case.name,
        ["rc", "rr", "ser"][level],
        form as u8
    );
    let setup_client = procs.client(0).await;
    setup_client
        .batch_execute(&suffixed(case.setup, &tables, &suffix))
        .await
        .unwrap_or_else(|e| panic!("{} setup: {e:?}", case.name));
    drop(setup_client);
    let (done_tx, mut done) = mpsc::unbounded_channel::<(usize, String)>();
    let mut inputs = BTreeMap::new();
    for (i, name) in ['A', 'B', 'C'].into_iter().enumerate() {
        let (tx, mut rx) = mpsc::unbounded_channel::<(usize, String)>();
        inputs.insert(name, tx);
        let mut session = Session {
            client: procs.client(i).await,
            form,
            block: false,
            failed: false,
        };
        let done_tx = done_tx.clone();
        tokio::spawn(async move {
            while let Some((i, sql)) = rx.recv().await {
                let _ = done_tx.send((i, session.step(&sql).await));
            }
        });
    }
    drop(done_tx);
    let mut outcomes: BTreeMap<usize, String> = BTreeMap::new();
    let mut waited = vec![false; case.steps.len()];
    for (i, (name, sql, _)) in case.steps.iter().enumerate() {
        let sql = suffixed(&sql.replace("@LEVEL@", LEVELS[level]), &tables, &suffix);
        inputs[name].send((i, sql)).unwrap();
        let deadline = Instant::now() + STEP_WAIT;
        while !outcomes.contains_key(&i) {
            match tokio::time::timeout_at(deadline, done.recv()).await {
                Ok(Some((j, out))) => {
                    outcomes.insert(j, out);
                }
                _ => {
                    waited[i] = true;
                    break;
                }
            }
        }
    }
    drop(inputs);
    let deadline = Instant::now() + END_WAIT;
    while outcomes.len() < case.steps.len() {
        let Ok(Some((j, out))) = tokio::time::timeout_at(deadline, done.recv()).await else {
            break;
        };
        outcomes.insert(j, out);
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
        client: procs.client(0).await,
        form,
        block: false,
        failed: false,
    };
    let fin = last.step(&suffixed(case.fin.0, &tables, &suffix)).await;
    compare(format!("final: {}", case.fin.0), case.fin.1, fin);
    (transcript, mismatches)
}

/// Runs each (case, level) in `form`; returns every mismatch with its transcript.
async fn matrix(procs: &ClusterProcs, runs: &[(&Case, usize)], form: Form) -> Vec<String> {
    let mut failures = Vec::new();
    for &(case, level) in runs {
        let name = LEVELS[level];
        let started = std::time::Instant::now();
        let (transcript, mismatches) = run(procs, case, level, form).await;
        eprintln!(
            "{} / {name} / {form:?}: {} mismatch(es), {:?}",
            case.name,
            mismatches.len(),
            started.elapsed()
        );
        if !mismatches.is_empty() {
            failures.push(format!(
                "{} / {name} / {form:?}:\n{}\ntranscript:\n{}",
                case.name,
                mismatches.join("\n"),
                transcript.join("\n")
            ));
        }
    }
    failures
}

async fn show(client: &Client, query: &str) -> String {
    let messages = client.simple_query(query).await.unwrap();
    let mut values = messages.iter().filter_map(|m| match m {
        SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap().to_owned()),
        _ => None,
    });
    values.next().unwrap_or_else(|| panic!("{query}: no row"))
}

/// R5-70 on a gateway: SHOW reports the level each form sets, in PostgreSQL's spelling.
async fn reports_each_level(procs: &ClusterProcs) {
    let client = procs.client(1).await;
    let current = "SHOW transaction_isolation";
    let default = "SHOW default_transaction_isolation";
    assert_eq!(show(&client, current).await, "read committed");
    for level in ["read committed", "repeatable read", "serializable"] {
        for begin in [
            format!("BEGIN ISOLATION LEVEL {}", level.to_uppercase()),
            format!(
                "BEGIN; SET TRANSACTION ISOLATION LEVEL {}",
                level.to_uppercase()
            ),
        ] {
            client.batch_execute(&begin).await.unwrap();
            assert_eq!(show(&client, current).await, level, "{begin}");
            assert_eq!(show(&client, default).await, "read committed", "{begin}");
            client.batch_execute("ROLLBACK").await.unwrap();
            assert_eq!(show(&client, current).await, "read committed", "{begin}");
        }
        let set = format!(
            "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {}",
            level.to_uppercase()
        );
        client.batch_execute(&set).await.unwrap();
        assert_eq!(show(&client, current).await, level, "{set}");
        assert_eq!(show(&client, default).await, level, "{set}");
        client.batch_execute("BEGIN").await.unwrap();
        assert_eq!(show(&client, current).await, level, "{set}; BEGIN");
        client.batch_execute("COMMIT").await.unwrap();
        client
            .batch_execute("RESET default_transaction_isolation")
            .await
            .unwrap();
    }
}

/// Every (case, level) of `names` (all cases when empty) for which `keep` holds.
fn runs(names: &[&str], keep: impl Fn(&str, usize) -> bool) -> Vec<(&'static Case, usize)> {
    CASES
        .iter()
        .filter(|case| names.is_empty() || names.contains(&case.name))
        .flat_map(|case| (0..3).map(move |level| (case, level)))
        .filter(|&(case, level)| keep(case.name, level))
        .collect()
}

/// The whole matrix with `BEGIN ISOLATION LEVEL`, then the level-sensitive cases again with the
/// level set by `SET SESSION CHARACTERISTICS` and by `SET TRANSACTION` (R5-70: behaviour agrees).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn anomaly_matrix_through_three_gateways() {
    let procs = ClusterProcs::new(3).await;
    reports_each_level(&procs).await;
    let passing = |name: &str, level| !FINDINGS.iter().any(|&(n, l)| (n, l) == (name, level));
    let mut failures = matrix(&procs, &runs(&[], passing), Form::Begin).await;
    let again = [
        "nonrepeatable_read",
        "write_skew_row_reads",
        "read_only_anomaly",
    ];
    for form in [
        Form::Session,
        Form::SetTransaction,
        Form::SetTransactionBatched,
    ] {
        failures.extend(matrix(&procs, &runs(&again, passing), form).await);
    }
    assert!(
        failures.is_empty(),
        "mismatches with PostgreSQL 18.6:\n{}",
        failures.join("\n\n")
    );
}

/// Regressions asserting PostgreSQL's results: the foreign-key cases at READ COMMITTED and
/// REPEATABLE READ, and PostgreSQL's UNION ALL final query.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn foreign_key_findings() {
    let procs = ClusterProcs::new(3).await;
    let client = procs.client(0).await;
    client
        .batch_execute(&format!("{P_C} INSERT INTO c VALUES (1, 1);"))
        .await
        .unwrap();
    let mut session = Session {
        client,
        form: Form::Begin,
        block: false,
        failed: false,
    };
    let union = session.step(P_C_UNION).await;
    let finding = |name: &str, level| FINDINGS.iter().any(|&(n, l)| (n, l) == (name, level));
    let mut failures = matrix(&procs, &runs(&[], finding), Form::Begin).await;
    if union != "c|1,p|1,p|2" {
        failures.push(format!("{P_C_UNION}: PostgreSQL c|1,p|1,p|2, got {union}"));
    }
    assert!(
        failures.is_empty(),
        "mismatches with PostgreSQL 18.6:\n{}",
        failures.join("\n\n")
    );
}
