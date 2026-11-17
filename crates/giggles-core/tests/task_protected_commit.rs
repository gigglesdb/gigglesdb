//! GD-0061 R3-626: the protected commit at the SQL level, through the public
//! API only, with the task created from `CREATE TASK` text (design-1 section 5,
//! design-6 D-F and D-G). Worked-example steps (spec section 14) 5, 6, 7, 12,
//! 14, 15, 18 and 19.
//!
//! `deliveries` is reserved, so the table is `shipments`. Merchant 42's failed
//! rows 1-40 fall inside the OLD ROW window; row 41 is outside it, row 50 is
//! delivered and row 100 belongs to merchant 43.
//!
//! Step 12 deviates: Phase 7 (preview, approve, apply) has not landed, so `a2`
//! is recorded by a direct write within the approval threshold, and the 12-row
//! resend under `a2` is then the conflicting request.
#![cfg(feature = "engine-rocksdb")]
use giggles_core::relational::Value;
use giggles_core::roles::{NewRole, TablePrivilege};
use giggles_core::task::context::TaskContext;
use giggles_core::task::definition::WallClockMs;
use giggles_core::task::statements::{parse, TaskStatement};
use giggles_core::task::{
    outcome, protected_commit, Actor, Budget, OperationId, Outcome, ProtectedRequest, Receipt,
    TaskError,
};
use giggles_core::{Admin, Engine, Limits};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const TASK: &str = "inc-436";
const DAY_MS: i64 = 86_400_000;

struct Dir(PathBuf);
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

/// Unix milliseconds as a timestamptz literal in UTC.
fn timestamptz(ms: i64) -> String {
    let days = ms.div_euclid(DAY_MS);
    let rem = ms.rem_euclid(DAY_MS);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}.{:03}+00",
        rem / 3_600_000,
        rem / 60_000 % 60,
        rem / 1000 % 60,
        rem % 1000
    )
}

fn sql(admin: &Admin, statement: &str) -> Vec<Vec<Value>> {
    admin
        .sql_session()
        .execute(statement, &[], None)
        .unwrap_or_else(|e| panic!("{statement}: {e:?}"))
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect()
}

fn count(admin: &Admin, filter: &str) -> i64 {
    match &sql(admin, &format!("SELECT count(*) FROM shipments WHERE {filter}"))[0][0] {
        Value::Int(n) => *n,
        other => panic!("{other:?}"),
    }
}

fn queued(admin: &Admin) -> i64 {
    count(admin, "status = 'queued'")
}

/// The worked example's `CREATE TASK`, as `incident_controller` would send it.
fn create_task_sql(expires: &str) -> String {
    format!(
        "CREATE TASK \"{TASK}\" \
           EXECUTOR repair_agent APPROVER oncall_lead \
           ON shipments \
           VISIBLE ROWS WHERE (merchant_id = 42) \
           READABLE (id, status, failed_at, attempt_count) \
           MASKED (customer_email) \
           ALLOW UPDATE (status) \
             OLD ROW WHERE (status = 'failed' AND failed_at >= '2026-11-05 17:00:00+00' \
                            AND failed_at < '2026-11-05 17:07:00+00') \
             NEW ROW WHERE (status = 'queued') \
             AT MOST 25 DISTINCT ROWS AND 25 TRANSITIONS \
             APPROVE ABOVE 5 \
           DISCLOSE AT MOST 1000 ROWS READ, 2097152 BYTES RETURNED \
           WITH (statement_ms = 2000, rows_scanned = 100000, concurrent_statements = 2) \
           EXPIRES '{expires}'"
    )
}

/// A fresh v2 database: the table, the roles of the worked example and the
/// task, created from its SQL text by `incident_controller`.
fn setup(expires_ms: i64) -> (Dir, Admin) {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let dir = Dir(std::env::temp_dir().join(format!(
        "task-protected-commit-{}-{}-{}",
        std::process::id(),
        now_ms(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )));
    let admin = Admin::create_engine(&dir.0, Limits::default(), Engine::V2).unwrap();
    sql(
        &admin,
        "CREATE TABLE shipments (id bigint primary key, merchant_id bigint, status text, \
         failed_at timestamptz, attempt_count int, customer_email text)",
    );
    let mut rows: Vec<String> = (1..=40)
        .map(|id| format!("({id}, 42, 'failed', '2026-11-05 17:03:00+00', 1, 'c{id}@example.com')"))
        .collect();
    rows.push("(41, 42, 'failed', '2026-11-05 17:10:00+00', 1, 'late@example.com')".into());
    rows.push("(50, 42, 'delivered', NULL, 0, 'done@example.com')".into());
    rows.push("(100, 43, 'failed', '2026-11-05 17:03:00+00', 1, 'other@example.com')".into());
    sql(&admin, &format!("INSERT INTO shipments VALUES {}", rows.join(", ")));
    let roles = admin.roles();
    for (name, task_only) in [
        ("incident_controller", false),
        ("oncall_lead", false),
        ("repair_agent", true),
    ] {
        roles
            .create(NewRole { name: name.into(), login: true, ..NewRole::default() })
            .unwrap();
        if task_only {
            roles.set_task_only(name, true).unwrap();
        }
    }
    for name in ["incident_controller", "repair_agent"] {
        roles
            .grant_table("shipments", name, &[TablePrivilege::Select, TablePrivilege::Update])
            .unwrap();
    }
    let issuer = roles.get("incident_controller").unwrap().unwrap();
    let Some(TaskStatement::Create(new)) = parse(&create_task_sql(&timestamptz(expires_ms)))
    else {
        panic!("CREATE TASK did not parse");
    };
    admin.tasks().create(&issuer, *new, WallClockMs(now_ms())).unwrap();
    (dir, admin)
}

/// The agent session's binding, as the server builds it at connect.
fn context(admin: &Admin) -> TaskContext {
    let def = admin.tasks().get(TASK).unwrap().expect("task");
    TaskContext {
        task: def.id,
        name: def.name.clone(),
        executor: def.executor.clone(),
        bound_def_version: def.def_version,
        credential_fingerprint: [7; 32],
    }
}

fn op(id: &str) -> OperationId {
    OperationId::new(id.as_bytes()).unwrap()
}

/// Requeue merchant 42's failed rows `from..=to` (merchant_id is hidden from
/// the task, so the agent filters by id; visible rows scope it to 42).
fn requeue(from: i64, to: i64) -> String {
    format!(
        "UPDATE shipments SET status = 'queued' \
         WHERE status = 'failed' AND id >= {from} AND id <= {to}"
    )
}

/// `OPERATION '<id>' <text>` on its own session.
fn commit(admin: &Admin, ctx: &TaskContext, id: &str, text: &str) -> Result<Receipt, TaskError> {
    let mut session = admin.sql_session();
    let request = ProtectedRequest::Direct {
        statement: session.prepare(text).unwrap_or_else(|e| panic!("{text}: {e:?}")),
        params: Vec::new(),
        text: Arc::from(text),
    };
    protected_commit(&mut session, Actor::Executor(ctx), &op(id), request)
}

/// `SHOW OPERATION '<id>'`.
fn lookup(admin: &Admin, ctx: &TaskContext, id: &str) -> Result<Option<Outcome>, TaskError> {
    outcome(&admin.sql_session(), ctx, &op(id))
}

fn committed(found: Result<Option<Outcome>, TaskError>) -> Receipt {
    match found {
        Ok(Some(Outcome::Committed(receipt))) => receipt,
        other => panic!("expected a committed outcome: {other:?}"),
    }
}

fn same(a: &Receipt, b: &Receipt) {
    assert_eq!(
        (a.operation_id.as_slice(), a.seq, a.rows_affected, a.distinct_charged, a.effects_digest),
        (b.operation_id.as_slice(), b.seq, b.rows_affected, b.distinct_charged, b.effects_digest)
    );
}

#[test]
fn worked_example_direct_writes() {
    let (_dir, admin) = setup(now_ms() + DAY_MS);
    let ctx = context(&admin);

    // Step 5: four failed rows to queued under `a1`; 4 of 25 distinct rows.
    let a1 = commit(&admin, &ctx, "a1", &requeue(1, 4)).unwrap();
    assert_eq!(
        (a1.seq, a1.rows_affected, a1.distinct_charged, a1.transitions_charged),
        (1, 4, 4, 4)
    );
    assert_eq!(a1.operation_id, b"a1");
    assert_eq!(queued(&admin), 4);
    same(&committed(lookup(&admin, &ctx, "a1")), &a1);

    // Step 6: the reply is lost and `a1` is resent: the same receipt, and no
    // counter moves (the next commit takes seq 2 and charges only its rows).
    let resent = commit(&admin, &ctx, "a1", &requeue(1, 4)).unwrap();
    same(&resent, &a1);
    assert_eq!(queued(&admin), 4);

    // Step 7: 12 rows under `a2` is above APPROVE ABOVE 5: approval required,
    // nothing changes and `a2` is not recorded.
    let twelve = requeue(5, 16);
    assert_eq!(commit(&admin, &ctx, "a2", &twelve), Err(TaskError::ApprovalRequired));
    assert_eq!(TaskError::ApprovalRequired.sqlstate(), "42T01");
    assert_eq!(queued(&admin), 4);
    assert_eq!(lookup(&admin, &ctx, "a2"), Ok(None));

    // `a2` is still free: a 5-row change takes it (in place of step 11's apply).
    let a2 = commit(&admin, &ctx, "a2", &requeue(5, 9)).unwrap();
    assert_eq!((a2.seq, a2.rows_affected, a2.distinct_charged), (2, 5, 5));
    assert_eq!(queued(&admin), 9);

    // Step 12: the 12-row update resent under `a2` is another request: 42T03,
    // nothing changes, and the stored outcome is untouched.
    let conflict = commit(&admin, &ctx, "a2", &twelve).unwrap_err();
    assert_eq!(conflict, TaskError::OperationConflict);
    assert_eq!(conflict.sqlstate(), "42T03");
    assert_eq!(queued(&admin), 9);
    same(&committed(lookup(&admin, &ctx, "a2")), &a2);

    // Step 15: a row of merchant 43 is outside the visible rows: zero rows
    // match, nothing changes, and the reply gives no hint that it exists.
    let other = commit(
        &admin,
        &ctx,
        "a3",
        "UPDATE shipments SET status = 'queued' WHERE status = 'failed' AND id = 100",
    )
    .unwrap();
    assert_eq!((other.rows_affected, other.distinct_charged), (0, 0));
    assert_eq!(count(&admin, "id = 100 AND status = 'failed'"), 1);
    let missing = commit(
        &admin,
        &ctx,
        "a4",
        "UPDATE shipments SET status = 'queued' WHERE status = 'failed' AND id = 99999",
    )
    .unwrap();
    assert_eq!(
        (missing.rows_affected, missing.distinct_charged, missing.noop_rows),
        (other.rows_affected, other.distinct_charged, other.noop_rows)
    );
    assert_eq!(queued(&admin), 9);
}

#[test]
fn ten_sessions_last_slot() {
    const SESSIONS: i64 = 10;
    let (_dir, admin) = setup(now_ms() + DAY_MS);
    let ctx = context(&admin);
    // Step 14 setup: 24 of 25 distinct rows used, each change within the
    // approval threshold of 5.
    for (n, (from, to)) in [(1, 5), (6, 10), (11, 15), (16, 20), (21, 24)].iter().enumerate() {
        commit(&admin, &ctx, &format!("fill-{n}"), &requeue(*from, *to)).unwrap();
    }
    assert_eq!(queued(&admin), 24);
    // Ten sessions each requeue one new row at once.
    let gate = Barrier::new(SESSIONS as usize);
    let results: Vec<(i64, Result<Receipt, TaskError>)> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..SESSIONS)
            .map(|n| {
                let (admin, ctx, gate) = (&admin, &ctx, &gate);
                s.spawn(move || {
                    let id = 25 + n;
                    let text = requeue(id, id);
                    let mut session = admin.sql_session();
                    let request = ProtectedRequest::Direct {
                        statement: session.prepare(&text).unwrap(),
                        params: Vec::new(),
                        text: Arc::from(text.as_str()),
                    };
                    gate.wait();
                    let id = op(&format!("last-{n}"));
                    (n, protected_commit(&mut session, Actor::Executor(ctx), &id, request))
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let winners: Vec<_> = results.iter().filter(|(_, r)| r.is_ok()).collect();
    assert_eq!(winners.len(), 1, "{results:?}");
    let winner = winners[0].0;
    let receipt = winners[0].1.as_ref().unwrap();
    assert_eq!((receipt.seq, receipt.rows_affected, receipt.distinct_charged), (6, 1, 1));
    for (n, result) in &results {
        let found = lookup(&admin, &ctx, &format!("last-{n}")).unwrap();
        if *n == winner {
            same(&committed(Ok(found)), receipt);
            continue;
        }
        let error = result.as_ref().unwrap_err();
        assert_eq!(*error, TaskError::BudgetExceeded(Budget::DistinctRows), "session {n}");
        assert_eq!(error.sqlstate(), "54T01");
        assert_eq!(found, Some(Outcome::BudgetExceeded(Budget::DistinctRows)), "session {n}");
    }
    assert_eq!(queued(&admin), 25);
    assert_eq!(count(&admin, &format!("id = {} AND status = 'queued'", 25 + winner)), 1);
}

#[test]
fn restart_keeps_counters_and_receipt() {
    let (dir, admin) = setup(now_ms() + DAY_MS);
    let ctx = context(&admin);
    let a1 = commit(&admin, &ctx, "a1", &requeue(1, 4)).unwrap();
    let a2 = commit(&admin, &ctx, "a2", &requeue(5, 9)).unwrap();
    assert_eq!((a2.seq, a2.distinct_charged), (2, 5));
    // Step 18: the server is killed and restarted.
    drop(admin);
    let admin = Admin::open(&dir.0, Limits::default()).unwrap();
    let ctx = context(&admin);
    same(&committed(lookup(&admin, &ctx, "a2")), &a2);
    same(&commit(&admin, &ctx, "a2", &requeue(5, 9)).unwrap(), &a2);
    same(&commit(&admin, &ctx, "a1", &requeue(1, 4)).unwrap(), &a1);
    assert_eq!(queued(&admin), 9);
    // Counters are at their committed values: 9 distinct rows used, so 16 more
    // fit (seq continues at 3) and the 26th does not.
    for (n, (from, to)) in [(10, 14), (15, 19), (20, 24)].iter().enumerate() {
        let receipt = commit(&admin, &ctx, &format!("b{n}"), &requeue(*from, *to)).unwrap();
        assert_eq!((receipt.seq, receipt.distinct_charged), (3 + n as u64, 5));
    }
    assert_eq!(
        commit(&admin, &ctx, "over", &requeue(25, 26)),
        Err(TaskError::BudgetExceeded(Budget::DistinctRows))
    );
    let last = commit(&admin, &ctx, "last", &requeue(25, 25)).unwrap();
    assert_eq!((last.seq, last.distinct_charged), (6, 1));
    assert_eq!(queued(&admin), 25);
}

#[test]
fn expiry_blocks_write_allows_lookup() {
    // The task expires a few seconds from now by the server's wall clock.
    let expires = now_ms() + 3_000;
    let (_dir, admin) = setup(expires);
    let ctx = context(&admin);
    let a2 = commit(&admin, &ctx, "a2", &requeue(1, 5)).unwrap();
    assert_eq!(queued(&admin), 5);
    while now_ms() <= expires + 50 {
        std::thread::sleep(Duration::from_millis(50));
    }
    // Step 19: the outcome of `a2` is still the same receipt.
    same(&committed(lookup(&admin, &ctx, "a2")), &a2);
    // A new write is refused as expired; nothing changes and the ID is free.
    let refused = commit(&admin, &ctx, "a5", &requeue(6, 6)).unwrap_err();
    assert_eq!(refused, TaskError::Expired);
    assert_eq!(refused.sqlstate(), "28T02");
    assert_eq!(lookup(&admin, &ctx, "a5"), Ok(None));
    assert_eq!(queued(&admin), 5);
    // A resend of a recorded operation still answers its stored receipt.
    same(&commit(&admin, &ctx, "a2", &requeue(1, 5)).unwrap(), &a2);
    same(&committed(lookup(&admin, &ctx, "a2")), &a2);
}
