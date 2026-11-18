//! GD-0061 R3-902 (design-7 D9): the first milestone. The spec's worked
//! example, steps 1-22, against an in-process v2 server with a real driver
//! (tokio-postgres): the executor `repair_agent` on the agent listener, the
//! approver `oncall_lead` and the application `delivery_app` on the password
//! PostgreSQL listener, while (in the first test) four `delivery_app`
//! connections run INSERT, UPDATE and SELECT on merchants 100-199 for the
//! whole script. The second test runs the same script with the workload off,
//! for failure isolation.
//!
//! The fixture is `r3_support` (R3-901): table `shipments` (`deliveries` is
//! reserved), task `inc-436` with the spec section 3 values (25 distinct rows
//! and 25 transitions, approval above 5 rows, 100,000 rows scanned).
//!
//! Counting. The spec's step 14 races for the last unit "when 24 are used",
//! while steps 20-22 and D9 count 16 distinct rows before the undo. Both hold
//! when the 24 are transitions: after step 13 the delivery worker fails nine
//! of the requeued deliveries again and the agent requeues them (step 13b),
//! which takes transitions to 24 and leaves distinct rows at 15. Step 14's
//! winner makes it 16 distinct rows and 25 transitions; the nine others are
//! `budget_exceeded` on transitions.
//!
//! Clocks. `advance_clock` pins the database wall clock, which the protected
//! commit's expiry check, receipts, previews and undo read. The server's
//! per-call standing check reads the host clock, so after step 19's expiry
//! the write is refused by the protected commit (28T02), not at admission.
#![cfg(feature = "engine-rocksdb")]
mod r3_support;

use giggles_core::{
    relational::Value,
    task::{
        Budget, DenyRule, OperationId, Outcome, ProtectedPoint, Receipt, StatementClass, TaskError,
        context::TaskDefinitions,
        definition::{TaskName, TaskObjectId, WallClockMs},
        inspect_outcome,
        statements::{TaskStatement, parse},
    },
};
use r3_support::agent_support::{DAY_MS, now_ms, timestamptz};
use r3_support::{
    APP_ROWS_PER_MERCHANT, CONTROLLER, DEFAULT_CLIENTS, EXECUTOR, EXECUTOR_PW, MAX_ATTEMPTS,
    R3Fixture, SCAN_TASK, TABLE, TASK, WorkloadReport, inc_436_sql,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Barrier, oneshot};
use tokio_postgres::{Client, SimpleQueryMessage, error::SqlState};

/// What a simple-protocol statement returned: column names, rows as text
/// (NULL as `None`) and the CommandComplete count.
#[derive(Clone, Debug, PartialEq)]
struct Reply {
    columns: Vec<String>,
    rows: Vec<Vec<Option<String>>>,
    count: u64,
}

async fn simple(client: &Client, sql: &str) -> Result<Reply, tokio_postgres::Error> {
    let mut reply = Reply {
        columns: vec![],
        rows: vec![],
        count: 0,
    };
    for message in client.simple_query(sql).await? {
        match message {
            SimpleQueryMessage::Row(row) => {
                if reply.columns.is_empty() {
                    reply.columns = row.columns().iter().map(|c| c.name().to_owned()).collect();
                }
                reply.rows.push(
                    (0..row.len())
                        .map(|i| row.get(i).map(str::to_owned))
                        .collect(),
                );
            }
            SimpleQueryMessage::CommandComplete(count) => reply.count = count,
            _ => {}
        }
    }
    Ok(reply)
}

/// The SQLSTATE and message of a failed statement.
#[track_caller]
fn failure(result: Result<Reply, tokio_postgres::Error>, step: &str) -> (String, String) {
    let error = result.expect_err(step);
    let db = error
        .as_db_error()
        .unwrap_or_else(|| panic!("{step}: {error:?}"));
    (db.code().code().to_owned(), db.message().to_owned())
}

fn expected(error: TaskError) -> (String, String) {
    (error.sqlstate().to_owned(), error.message().to_owned())
}

/// The 54T01 message of a refusal that carries only its code: the per-task
/// and all-agent statement slots at admission (step 14 checks afterwards
/// that such a refusal recorded nothing).
const BUDGET_CODE_ONLY: &str = "task budget exceeded";

/// A step 14 refusal that may have decided nothing and is retried with the
/// same operation ID: 40001, or a statement-slot refusal. A retry of a
/// recorded decision replays it, so retrying is always safe.
fn transient(error: &tokio_postgres::Error) -> bool {
    match error.as_db_error() {
        Some(db) if *db.code() == SqlState::T_R_SERIALIZATION_FAILURE => true,
        Some(db) => {
            db.code().code() == "54T01"
                && (db.message() == BUDGET_CODE_ONLY
                    || [Budget::ConcurrentStatements, Budget::AllAgents]
                        .iter()
                        .any(|b| db.message() == b.message()))
        }
        None => false,
    }
}

const REQUEUE: &str = "UPDATE shipments SET status = 'queued' WHERE status = 'failed'";

fn requeue(op: &str, rows: &str) -> String {
    format!("OPERATION '{op}' {REQUEUE} AND {rows}")
}

fn show_operation(op: &str) -> String {
    format!("SHOW OPERATION '{op}'")
}

/// Merchant 42's rows as `id -> status`, read by the administrator.
fn statuses(f: &R3Fixture) -> BTreeMap<i64, String> {
    f.sql("SELECT id, status FROM shipments WHERE merchant_id = 42 ORDER BY id")
        .into_iter()
        .map(|row| match (&row[0], &row[1]) {
            (Value::Int(id), Value::Text(status)) => (*id, status.clone()),
            other => panic!("{other:?}"),
        })
        .collect()
}

fn ids_with(map: &BTreeMap<i64, String>, status: &str) -> Vec<i64> {
    map.iter()
        .filter(|(_, s)| s.as_str() == status)
        .map(|(id, _)| *id)
        .collect()
}

/// The full receipt of a committed operation, as an inspector reads it.
#[track_caller]
fn receipt(f: &R3Fixture, task: TaskObjectId, op: &str) -> Receipt {
    let session = f.admin().sql_session();
    match inspect_outcome(&session, task, &OperationId::new(op).unwrap()).unwrap() {
        Some(Outcome::Committed(receipt)) => receipt,
        other => panic!("{op}: {other:?}"),
    }
}

/// A preview row: (change_id, digest, row_count).
#[track_caller]
fn preview_row(reply: &Reply, step: &str) -> (i64, String, i64) {
    assert_eq!(
        reply.columns,
        ["change_id", "digest", "row_count", "expires_at"],
        "{step}"
    );
    assert_eq!(reply.rows.len(), 1, "{step}: {reply:?}");
    let row = &reply.rows[0];
    let digest = row[1].clone().unwrap();
    assert!(digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_hexdigit()));
    (
        row[0].as_deref().unwrap().parse().unwrap(),
        digest,
        row[2].as_deref().unwrap().parse().unwrap(),
    )
}

fn apply(op: &str, change: i64, digest: &str) -> String {
    format!("OPERATION '{op}' APPLY CHANGE {change} DIGEST '{digest}'")
}

fn approve(change: i64, digest: &str) -> String {
    format!("APPROVE CHANGE {change} OF TASK \"{TASK}\" DIGEST '{digest}'")
}

/// One of step 14's ten sessions: requeue row `id` under its own operation
/// ID, retrying what decided nothing. Returns (attempts, result).
async fn racer(
    client: Client,
    k: usize,
    id: i64,
    start: Arc<Barrier>,
) -> (u32, Result<Reply, tokio_postgres::Error>) {
    let sql = requeue(&format!("r{k}"), &format!("id = {id}"));
    start.wait().await;
    let mut attempt = 0;
    loop {
        attempt += 1;
        let result = simple(&client, &sql).await;
        match &result {
            Err(error) if transient(error) && attempt < MAX_ATTEMPTS => {
                tokio::time::sleep(Duration::from_millis(5 * u64::from(attempt.min(20)))).await;
            }
            _ => return (attempt, result),
        }
    }
}

/// The trusted pause seam for step 13: the first apply that reaches
/// `AfterStatement` (its scan done, its commit not yet) signals `paused` and
/// waits for `go`.
fn pause_after_statement(
    paused: oneshot::Sender<()>,
    go: std::sync::mpsc::Receiver<()>,
) -> giggles_core::task::ProtectedHook {
    let fired = AtomicBool::new(false);
    let paused = Mutex::new(Some(paused));
    let go = Mutex::new(go);
    Arc::new(move |point| {
        if point != ProtectedPoint::AfterStatement || fired.swap(true, Ordering::SeqCst) {
            return;
        }
        if let Some(paused) = paused.lock().unwrap().take() {
            let _ = paused.send(());
        }
        let _ = go.lock().unwrap().recv_timeout(Duration::from_secs(10));
    })
}

/// The rows step 16 adds: delivered rows outside both tasks' visibility,
/// enough that the non-key-scoped task passes `rows_scanned` (100,000).
const BULK_MERCHANT: i64 = 7;
const BULK_ROWS: i64 = 101_000;

fn bulk_inserts() -> Vec<String> {
    (0..BULK_ROWS / 1_000)
        .map(|chunk| {
            let rows: Vec<String> = (1..=1_000)
                .map(|i| {
                    let id = chunk * 1_000 + i;
                    format!(
                        "({BULK_MERCHANT}, {id}, 'delivered', NULL, 0, 'bulk{id}@example.com', \
                         'tok_7_{id}')"
                    )
                })
                .collect();
            format!("INSERT INTO {TABLE} VALUES {}", rows.join(", "))
        })
        .collect()
}

async fn worked_example(load: bool) {
    let mut f = R3Fixture::start().await;
    f.create_inc_436();
    let name = TaskName::new(TASK).unwrap();
    let task = f
        .admin()
        .sql_session()
        .task_definition(&name)
        .unwrap()
        .expect("inc-436 exists")
        .id;
    let workload = load.then(|| f.workload(DEFAULT_CLIENTS));
    let agent = f.agent().await;
    let app = f.app().await;
    let approver = f.approver().await;

    // step 01: SELECT * returns merchant 42's 31 failed rows; customer_email
    // (masked) is NULL, merchant_id and card_token (hidden) are left out; the
    // rows are debited before release and the statement is in the read log.
    let step01 = "SELECT * FROM shipments WHERE status = 'failed'";
    let reply = f
        .retry_40001("step 01", || simple(&agent, step01))
        .await
        .expect("step 01");
    assert_eq!(reply.rows.len(), 31, "step 01");
    assert_eq!(
        reply.columns,
        ["id", "status", "failed_at", "attempt_count", "customer_email"],
        "step 01"
    );
    for row in &reply.rows {
        assert_eq!(row[1].as_deref(), Some("failed"), "step 01");
        assert_eq!(row[4], None, "step 01: masked customer_email");
        assert!(
            row.iter().flatten().all(|v| !v.contains("tok_") && !v.contains('@')),
            "step 01: {row:?}"
        );
    }
    let log = f.admin().task_read_log(task).unwrap();
    let record = log
        .iter()
        .rev()
        .find(|r| r.text == step01)
        .expect("step 01: statement in the read log");
    assert_eq!(record.sqlstate_text(), "00000", "step 01: {record:?}");
    assert_eq!(record.released_rows, 31, "step 01: {record:?}");
    assert!(record.charged_rows_returned >= 31, "step 01: {record:?}");
    let disclosure = f.admin().task_disclosure(task).unwrap();
    assert!(disclosure.rows_returned >= 31, "step 01: {disclosure:?}");

    // step 02: a hidden column fails at binding; no row is read.
    let step02 = "SELECT card_token FROM shipments";
    let (code, message) = failure(simple(&agent, step02).await, "step 02");
    // A hidden column is reported without naming it (no hint that it exists).
    assert_eq!(code, "42501", "step 02: {message}");
    assert!(!message.contains("card_token"), "step 02: {message}");
    if let Some(record) = f
        .admin()
        .task_read_log(task)
        .unwrap()
        .iter()
        .rev()
        .find(|r| r.text == step02)
    {
        assert_eq!(
            (record.rows_scanned, record.charged_rows_read, record.released_rows),
            (0, 0, 0),
            "step 02: {record:?}"
        );
    }

    // step 03: a masked column in a filter fails at binding.
    let step03 = "SELECT id FROM shipments WHERE customer_email LIKE 'a%'";
    // The agent listener reports every denial with one generic message.
    let (code, message) = failure(simple(&agent, step03).await, "step 03");
    assert_eq!(code, "42501", "step 03: {message}");
    assert!(!message.contains("customer_email"), "step 03: {message}");

    // step 04: explicit transactions are refused.
    assert_eq!(
        failure(simple(&agent, "BEGIN").await, "step 04"),
        expected(TaskError::NotAllowedInTaskSession(
            StatementClass::TransactionControl
        )),
        "step 04"
    );

    // step 05: requeue 4 failed rows under `a1`: a receipt and a change
    // record, 4 of 25 distinct rows used.
    let a1 = requeue("a1", "id <= 4");
    let reply = f
        .retry_40001("step 05", || simple(&agent, &a1))
        .await
        .expect("step 05");
    assert_eq!(reply.count, 4, "step 05");
    let r_a1 = receipt(&f, task, "a1");
    assert_eq!(
        (r_a1.rows_affected, r_a1.counters_after()),
        (4, Some((4, 4))),
        "step 05"
    );
    assert_eq!(r_a1.change_summary().map(|c| c.1).unwrap_or(4), 4, "step 05");
    assert_eq!(ids_with(&statuses(&f), "queued"), [1, 2, 3, 4], "step 05");

    // step 06: the reply was lost; the agent resends `a1` on a new connection
    // and gets the same receipt; nothing changes.
    {
        let again = f.agent().await;
        let reply = f
            .retry_40001("step 06", || simple(&again, &a1))
            .await
            .expect("step 06");
        assert_eq!(reply.count, 4, "step 06");
        let lookup = simple(&again, &show_operation("a1")).await.expect("step 06");
        assert_eq!(lookup.rows.len(), 1, "step 06");
        assert_eq!(lookup.rows[0][1].as_deref(), Some("committed"), "step 06");
        assert_eq!(lookup.rows[0][3], Some(r_a1.seq.to_string()), "step 06");
    }
    assert_eq!(receipt(&f, task, "a1"), r_a1, "step 06: same receipt");
    assert_eq!(ids_with(&statuses(&f), "queued"), [1, 2, 3, 4], "step 06");

    // step 07: 12 rows under `a2` needs approval; nothing changes and `a2`
    // stays free.
    let a2_direct = requeue("a2", "id BETWEEN 5 AND 16");
    let result = f.retry_40001("step 07", || simple(&agent, &a2_direct)).await;
    assert_eq!(
        failure(result, "step 07"),
        expected(TaskError::ApprovalRequired),
        "step 07"
    );
    assert_eq!(ids_with(&statuses(&f), "queued").len(), 4, "step 07");
    let lookup = simple(&agent, &show_operation("a2")).await.expect("step 07");
    assert!(lookup.rows.is_empty(), "step 07: a2 is free: {lookup:?}");

    // step 08: preview the 12-row change: change P, digest D.
    let preview12 = format!("PREVIEW {REQUEUE} AND id BETWEEN 5 AND 16");
    let reply = f
        .retry_40001("step 08", || simple(&agent, &preview12))
        .await
        .expect("step 08");
    let (p, d, rows) = preview_row(&reply, "step 08");
    assert_eq!(rows, 12, "step 08");

    // step 09: one of the 12 deliveries succeeds on its own.
    let delivered = "UPDATE shipments SET status = 'delivered', attempt_count = attempt_count + 1 \
                     WHERE merchant_id = 42 AND id = 7";
    let reply = f
        .retry_40001("app 09", || simple(&app, delivered))
        .await
        .expect("step 09");
    assert_eq!(reply.count, 1, "step 09");
    assert_eq!(statuses(&f)[&7], "delivered", "step 09");

    // step 10: oncall_lead approves P; the apply under `a2` is stale; nothing
    // changes and `a2` stays free.
    simple(&approver, &approve(p, &d)).await.expect("step 10: approve");
    let apply_p = apply("a2", p, &d);
    let result = f.retry_40001("step 10", || simple(&agent, &apply_p)).await;
    assert_eq!(
        failure(result, "step 10"),
        expected(TaskError::ApprovalStale),
        "step 10"
    );
    assert_eq!(ids_with(&statuses(&f), "queued").len(), 4, "step 10");
    let lookup = simple(&agent, &show_operation("a2")).await.expect("step 10");
    assert!(lookup.rows.is_empty(), "step 10: a2 is free: {lookup:?}");

    // step 11: preview again (P2, 11 rows), approve, apply P2 under `a2`:
    // 15 of 25 used.
    let reply = f
        .retry_40001("step 11 preview", || simple(&agent, &preview12))
        .await
        .expect("step 11");
    let (p2, d2, rows) = preview_row(&reply, "step 11");
    assert_eq!(rows, 11, "step 11");
    assert_ne!((p2, &d2), (p, &d), "step 11");
    simple(&approver, &approve(p2, &d2))
        .await
        .expect("step 11: approve");
    let apply_p2 = apply("a2", p2, &d2);
    let reply = f
        .retry_40001("step 11", || simple(&agent, &apply_p2))
        .await
        .expect("step 11");
    assert_eq!(reply.count, 11, "step 11");
    let r_a2 = receipt(&f, task, "a2");
    assert_eq!(
        (r_a2.rows_affected, r_a2.counters_after(), r_a2.approver()),
        (11, Some((15, 15)), Some("oncall_lead")),
        "step 11"
    );
    assert_eq!(r_a2.preview(), Some(p2 as u64), "step 11");
    let queued: Vec<i64> = [1, 2, 3, 4, 5, 6]
        .into_iter()
        .chain(8..=16)
        .collect();
    assert_eq!(ids_with(&statuses(&f), "queued"), queued, "step 11");

    // step 12: the direct 12-row update from step 7 under `a2` conflicts.
    let result = f.retry_40001("step 12", || simple(&agent, &a2_direct)).await;
    assert_eq!(
        failure(result, "step 12"),
        expected(TaskError::OperationConflict),
        "step 12"
    );
    assert_eq!(receipt(&f, task, "a2"), r_a2, "step 12");
    assert_eq!(ids_with(&statuses(&f), "queued"), queued, "step 12");

    // step 13: a 5-row change P3 is approved. While the apply is between its
    // scan and its commit (the pause seam), the delivery application inserts
    // a new failed row for merchant 42 in the incident window: validation
    // fails, nothing is applied, the agent must preview again.
    let preview5 = format!("PREVIEW {REQUEUE} AND id >= 27");
    let reply = f
        .retry_40001("step 13 preview", || simple(&agent, &preview5))
        .await
        .expect("step 13");
    let (p3, d3, rows) = preview_row(&reply, "step 13");
    assert_eq!(rows, 5, "step 13");
    simple(&approver, &approve(p3, &d3))
        .await
        .expect("step 13: approve");
    let (paused_tx, paused_rx) = oneshot::channel();
    let (go_tx, go_rx) = std::sync::mpsc::channel();
    f.admin()
        .set_protected_hook(Some(pause_after_statement(paused_tx, go_rx)))
        .unwrap();
    let apply_p3 = apply("a3", p3, &d3);
    let insert = "INSERT INTO shipments VALUES (42, 32, 'failed', '2026-11-05 17:06:00+00', 1, \
                  'customer32@example.com', 'tok_42_32')";
    let racing_insert = async {
        let paused = tokio::time::timeout(Duration::from_secs(20), paused_rx).await;
        let inserted = match paused {
            Ok(Ok(())) => tokio::time::timeout(Duration::from_secs(3), simple(&app, insert))
                .await
                .map_err(|_| "the insert waited on the paused apply".to_owned())
                .and_then(|r| r.map_err(|e| format!("{e:?}"))),
            _ => Err("the apply never reached the pause".to_owned()),
        };
        let _ = go_tx.send(());
        inserted
    };
    let (applied, inserted) = tokio::join!(
        f.retry_40001("step 13", || simple(&agent, &apply_p3)),
        racing_insert
    );
    f.admin().set_protected_hook(None).unwrap();
    assert_eq!(inserted.expect("step 13: insert").count, 1, "step 13");
    assert_eq!(
        failure(applied, "step 13"),
        expected(TaskError::ApprovalStale),
        "step 13"
    );
    let now = statuses(&f);
    assert_eq!(ids_with(&now, "queued"), queued, "step 13: nothing applied");
    assert_eq!(
        ids_with(&now, "failed"),
        (17..=32).collect::<Vec<_>>(),
        "step 13"
    );
    let lookup = simple(&agent, &show_operation("a3")).await.expect("step 13");
    assert!(lookup.rows.is_empty(), "step 13: a3 is free: {lookup:?}");

    // step 13b (deviation, see the module notes): the delivery worker fails
    // nine requeued deliveries again and the agent requeues them in two
    // direct writes below the approval threshold: 15 distinct rows, 24
    // transitions.
    let refail = "UPDATE shipments SET status = 'failed' WHERE merchant_id = 42 \
                  AND id BETWEEN 8 AND 16";
    let reply = f
        .retry_40001("app 13b", || simple(&app, refail))
        .await
        .expect("step 13b");
    assert_eq!(reply.count, 9, "step 13b");
    for (op, rows, n) in [("a4", "id BETWEEN 8 AND 12", 5), ("a5", "id BETWEEN 13 AND 16", 4)] {
        let sql = requeue(op, rows);
        let reply = f
            .retry_40001(&format!("step 13b {op}"), || simple(&agent, &sql))
            .await
            .expect("step 13b");
        assert_eq!(reply.count, n, "step 13b {op}");
    }
    assert_eq!(
        receipt(&f, task, "a5").counters_after(),
        Some((15, 24)),
        "step 13b"
    );
    assert_eq!(ids_with(&statuses(&f), "queued"), queued, "step 13b");

    // step 14: ten agent connections each try to requeue one row with 24
    // transitions used: exactly one commits, nine get budget_exceeded.
    let mut racers = Vec::new();
    for _ in 0..10 {
        racers.push(f.agent().await);
    }
    let start = Arc::new(Barrier::new(10));
    let handles: Vec<_> = racers
        .into_iter()
        .enumerate()
        .map(|(k, client)| tokio::spawn(racer(client, k, 17 + k as i64, start.clone())))
        .collect();
    let mut winners = Vec::new();
    let mut refused = BTreeMap::<String, u32>::new();
    let mut race_attempts = Vec::new();
    for (k, handle) in handles.into_iter().enumerate() {
        let (attempts, result) = handle.await.expect("step 14: racer");
        race_attempts.push(attempts);
        match result {
            Ok(reply) => {
                assert_eq!(reply.count, 1, "step 14: r{k}");
                winners.push(k);
            }
            Err(error) => {
                let db = error.as_db_error().unwrap_or_else(|| panic!("step 14: {error:?}"));
                *refused
                    .entry(format!("{} {}", db.code().code(), db.message()))
                    .or_default() += 1;
            }
        }
    }
    let transitions = expected(TaskError::BudgetExceeded(Budget::Transitions));
    assert_eq!(winners.len(), 1, "step 14: {winners:?} {refused:?}");
    assert_eq!(
        refused,
        BTreeMap::from([(format!("{} {}", transitions.0, transitions.1), 9)]),
        "step 14"
    );
    assert!(
        race_attempts.iter().all(|&n| n <= MAX_ATTEMPTS),
        "step 14: {race_attempts:?}"
    );
    // Each of the nine has its budget_exceeded recorded under its own ID.
    for k in (0..10).filter(|k| !winners.contains(k)) {
        let session = f.admin().sql_session();
        let op = OperationId::new(format!("r{k}")).unwrap();
        assert_eq!(
            inspect_outcome(&session, task, &op).unwrap(),
            Some(Outcome::BudgetExceeded(Budget::Transitions)),
            "step 14: r{k}"
        );
    }
    let winner = format!("r{}", winners[0]);
    let winner_id = 17 + winners[0] as i64;
    let r_win = receipt(&f, task, &winner);
    assert_eq!(r_win.counters_after(), Some((16, 25)), "step 14");
    let now = statuses(&f);
    assert_eq!(now[&winner_id], "queued", "step 14");
    assert_eq!(ids_with(&now, "queued").len(), 16, "step 14");

    // step 15: a row of merchant 43 (id 40 exists only there) is invisible:
    // zero rows, nothing changes, the same answer as for a row that does not
    // exist at all.
    let other_merchant = requeue("a6", "id = 40");
    let reply = f
        .retry_40001("step 15", || simple(&agent, &other_merchant))
        .await
        .expect("step 15");
    let missing = requeue("a7", "id = 999");
    let nothing = f
        .retry_40001("step 15 missing", || simple(&agent, &missing))
        .await
        .expect("step 15");
    assert_eq!((reply.count, &reply), (0, &nothing), "step 15");
    assert_eq!(
        f.count("merchant_id = 43 AND status = 'failed'"),
        40,
        "step 15"
    );

    // step 16: the incident task scans only merchant 42's key range. A
    // separate task scoped by non-key status must scan the whole table,
    // including 101,000 invisible delivered rows, and hit the default scan
    // limit (100,000). Its read log and counters are independent of inc-436.
    let scan_sql = inc_436_sql(&timestamptz(f.expires_ms))
        .replace(TASK, SCAN_TASK)
        .replace(
            "VISIBLE ROWS WHERE (merchant_id = 42)",
            "VISIBLE ROWS WHERE (status = 'failed')",
        )
        .replace(", rows_scanned = 100000", "");
    let Some(TaskStatement::Create(new)) = parse(&scan_sql) else {
        panic!("step 16: CREATE TASK did not parse: {scan_sql}");
    };
    let issuer = f.admin().roles().get(CONTROLLER).unwrap().unwrap();
    f.admin()
        .tasks()
        .create(&issuer, *new, WallClockMs(f.wall_ms()))
        .unwrap();
    let scan_task = f
        .admin()
        .sql_session()
        .task_definition(&TaskName::new(SCAN_TASK).unwrap())
        .unwrap()
        .expect("step 16: scan-limit exists")
        .id;
    let scan_agent = f.agent_for(SCAN_TASK).await;
    {
        let mut session = f.admin().sql_session();
        for insert in bulk_inserts() {
            session.execute(&insert, &[], None).unwrap();
        }
    }
    let step16 = "SELECT id, status FROM shipments WHERE attempt_count >= 0";
    let result = f
        .retry_40001("step 16", || simple(&scan_agent, step16))
        .await;
    assert_eq!(
        failure(result, "step 16"),
        expected(TaskError::BudgetExceeded(Budget::RowsScanned)),
        "step 16"
    );
    let log = f.admin().task_read_log(scan_task).unwrap();
    let record = log
        .iter()
        .rev()
        .find(|r| r.text == step16)
        .expect("step 16: statement in the read log");
    assert!(
        record.rows_scanned > 100_000 && record.rows_scanned <= 100_200,
        "step 16: {record:?}"
    );
    assert_eq!(record.sqlstate_text(), "54T01", "step 16: {record:?}");
    assert_eq!(record.released_rows, 0, "step 16");
    drop(scan_agent);

    // step 17: repair_agent's password on the PostgreSQL listener is refused:
    // the role is task-only.
    assert!(
        f.try_password(EXECUTOR, EXECUTOR_PW).await.is_err(),
        "step 17"
    );

    // step 18: the server stops and restarts; the agent asks for `a2`'s
    // outcome: the same receipt, counters at their committed values.
    let before = simple(&agent, &show_operation("a2")).await.expect("step 18");
    assert_eq!(before.rows.len(), 1, "step 18");
    drop((agent, app, approver));
    f.restart().await;
    let agent = f.agent().await;
    let app = f.app().await;
    let approver = f.approver().await;
    let show_a2 = show_operation("a2");
    let after = f
        .retry_40001("step 18", || simple(&agent, &show_a2))
        .await
        .expect("step 18");
    assert_eq!(after, before, "step 18");
    assert_eq!(after.rows[0][2].as_deref(), Some("11"), "step 18");
    assert_eq!(receipt(&f, task, "a2"), r_a2, "step 18");
    assert_eq!(receipt(&f, task, &winner), r_win, "step 18");
    assert_eq!(ids_with(&statuses(&f), "queued").len(), 16, "step 18");

    // step 19: the task expires at 17:30Z by the database clock. `a2`'s
    // outcome is still the same receipt; a new write is refused as expired.
    let past_expiry = f.expires_ms - now_ms() + 60_000;
    f.advance_clock(u64::try_from(past_expiry).unwrap());
    assert!(f.wall_ms() > f.expires_ms, "step 19");
    let lookup = f
        .retry_40001("step 19", || simple(&agent, &show_a2))
        .await
        .expect("step 19");
    assert_eq!(lookup, before, "step 19");
    let late = requeue("a8", "id = 27");
    let result = f.retry_40001("step 19 write", || simple(&agent, &late)).await;
    assert_eq!(
        failure(result, "step 19"),
        expected(TaskError::Expired),
        "step 19"
    );
    assert_eq!(statuses(&f)[&27], "failed", "step 19");

    // step 20: the next day the requeue turns out to be wrong. The delivery
    // worker has changed 3 of the task's rows since; oncall_lead previews an
    // undo of the task: 16 rows, 13 restorable and 3 conflicts.
    f.advance_clock(DAY_MS as u64);
    let worker = "UPDATE shipments SET status = 'delivered', attempt_count = attempt_count + 1 \
                  WHERE merchant_id = 42 AND id BETWEEN 1 AND 3";
    let reply = f
        .retry_40001("app 20", || simple(&app, worker))
        .await
        .expect("step 20");
    assert_eq!(reply.count, 3, "step 20");
    let undo = simple(&approver, &format!("PREVIEW UNDO OF TASK \"{TASK}\""))
        .await
        .expect("step 20");
    assert_eq!(
        undo.columns,
        [
            "change_id",
            "digest",
            "restorable",
            "conflicts",
            "superseded",
            "settled",
            "expires_at"
        ],
        "step 20"
    );
    let row = &undo.rows[0];
    let n = |i: usize| -> i64 { row[i].as_deref().unwrap().parse().unwrap() };
    let (undo_id, undo_digest) = (n(0), row[1].clone().unwrap());
    // Restorable 13 and conflicts 3 over 16 rows; the nine first requeues of
    // the rows requeued twice (step 13b) are superseded writes.
    assert_eq!([n(2), n(3), n(4), n(5)], [13, 3, 9, 0], "step 20: {undo:?}");
    let lines = simple(&approver, &format!("SHOW UNDO {undo_id} OF TASK \"{TASK}\""))
        .await
        .expect("step 20");
    let keys = |class: &str| -> BTreeSet<String> {
        lines
            .rows
            .iter()
            .filter(|l| l[0].as_deref() == Some(class))
            .map(|l| l[1].clone().unwrap())
            .collect()
    };
    let (restores, conflicts) = (keys("restore"), keys("conflict"));
    assert_eq!((restores.len(), conflicts.len()), (13, 3), "step 20: {lines:?}");
    for line in lines.rows.iter().filter(|l| l[0].as_deref() == Some("conflict")) {
        assert_eq!(line[5].as_deref(), Some("changed_since"), "step 20");
    }
    for line in lines.rows.iter().filter(|l| l[0].as_deref() == Some("restore")) {
        assert_eq!(
            (line[2].as_deref(), line[3].as_deref(), line[4].as_deref()),
            (Some("status"), Some("queued"), Some("failed")),
            "step 20"
        );
    }

    // step 21: a complete undo is refused while there are conflicts.
    let before_undo = statuses(&f);
    let complete = format!(
        "APPROVE UNDO {undo_id} OF TASK \"{TASK}\" DIGEST '{undo_digest}' MODE COMPLETE"
    );
    assert_eq!(
        failure(simple(&approver, &complete).await, "step 21"),
        expected(TaskError::Denied(DenyRule::UndoHasConflicts)),
        "step 21"
    );
    assert_eq!(statuses(&f), before_undo, "step 21: nothing changes");

    // step 22: a partial undo: 13 rows return to failed, 3 are untouched,
    // the receipt says partial.
    let partial = format!(
        "APPROVE UNDO {undo_id} OF TASK \"{TASK}\" DIGEST '{undo_digest}' MODE PARTIAL"
    );
    let reply = f
        .retry_40001("step 22", || simple(&approver, &partial))
        .await
        .expect("step 22");
    assert_eq!(reply.rows.len(), 1, "step 22");
    let fields: BTreeMap<&str, Option<&str>> = reply
        .columns
        .iter()
        .map(String::as_str)
        .zip(reply.rows[0].iter().map(Option::as_deref))
        .collect();
    for (name, value) in [
        ("decision", "undo_partial"),
        ("mode", "partial"),
        ("restored", "13"),
        ("conflicts", "3"),
        ("superseded", "9"),
        ("row_count", "13"),
        ("distinct_charged", "0"),
        ("transitions_charged", "0"),
        // Undo neither refunds nor charges (D8): the counters before it.
        ("distinct_after", "16"),
        ("transitions_after", "25"),
        ("executor", EXECUTOR),
        ("undone_by", "oncall_lead"),
    ] {
        assert_eq!(fields.get(name).copied().flatten(), Some(value), "step 22: {name}");
    }
    let after_undo = statuses(&f);
    for (id, status) in &before_undo {
        let expected = if [1, 2, 3, 7].contains(id) {
            &before_undo[id]
        } else {
            "failed"
        };
        assert_eq!(after_undo[id], expected, "step 22: id {id} (was {status})");
    }
    assert_eq!(ids_with(&after_undo, "delivered"), [1, 2, 3, 7], "step 22");
    assert_eq!(ids_with(&after_undo, "failed").len(), 28, "step 22");

    // D9 pass criteria. Receipts and change records agree with the counters:
    // the committed operations changed 25 rows (transitions) over 16
    // distinct rows, which the undo listed (13 + 3) with 9 superseded writes.
    let committed = ["a1", "a2", "a4", "a5", winner.as_str(), "a6", "a7"];
    let mut transitions_total = 0u64;
    for op in committed {
        let r = receipt(&f, task, op);
        if let Some((kind, rows)) = r.change_summary() {
            assert_eq!((kind, rows), (1, r.rows_affected), "{op}: change record");
        }
        transitions_total += u64::from(r.transitions_charged);
        assert_eq!(r.transitions_charged, r.rows_affected, "{op}");
    }
    assert_eq!(transitions_total, 25, "receipts");
    assert_eq!(receipt(&f, task, &winner).counters_after(), Some((16, 25)));
    assert_eq!(restores.len() + conflicts.len(), 16, "change records");
    // The read log's charged totals equal the disclosure budget consumed.
    let reconciliation = f.admin().task_read_verify(task).unwrap();
    assert!(reconciliation.ok(), "read log: {reconciliation:?}");
    let retries = f.retries();
    assert!(
        retries.values().all(|&n| n <= MAX_ATTEMPTS),
        "attempts: {retries:?}"
    );
    eprintln!("r3 milestone (load: {load}): attempts per step {retries:?}; step 14 {race_attempts:?}");
    if let Some(workload) = workload {
        let report: WorkloadReport = workload.stop().await;
        eprintln!(
            "r3 milestone workload: {} ops in {:?} ({:?}), 40001 {}, 40P01 {}, disconnects {}, \
             reconnect failures {}, p99 {:?}/{:?}/{:?}",
            report.total_ops(),
            report.elapsed,
            report.ops,
            report.serialization_failures,
            report.deadlocks,
            report.disconnects,
            report.connect_failures,
            report.quantile(r3_support::Op::Insert, 0.99),
            report.quantile(r3_support::Op::Update, 0.99),
            report.quantile(r3_support::Op::Select, 0.99),
        );
        assert_eq!(
            report.unexpected_errors(),
            0,
            "workload: {:?} {:?}",
            report.errors,
            report.error_samples
        );
        assert!(report.total_ops() > 0, "workload ran");
        // Every acknowledged insert is there (one in flight at the restart
        // may have committed without its reply).
        let inserts = report.ops.get(&r3_support::Op::Insert).copied().unwrap_or(0) as i64;
        let app_rows = f.count("merchant_id BETWEEN 100 AND 199");
        assert!(
            app_rows >= 100 * APP_ROWS_PER_MERCHANT + inserts,
            "workload inserts: {app_rows} rows, {inserts} inserts"
        );
    }
}

/// Task scans cover merchant 42's primary-key range, so application writes
/// to merchants 100-199 no longer invalidate the incident task's commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worked_example_steps_1_to_22_under_application_load() {
    worked_example(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worked_example_steps_1_to_22_without_load() {
    worked_example(false).await;
}
