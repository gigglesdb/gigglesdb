//! GD-0061 R3-1201: legacy safety scenarios ported to the R3 task API.
#![cfg(feature = "engine-rocksdb")]
use giggles_core::relational::{StatementClock, Value};
use giggles_core::roles::{NewRole, TablePrivilege};
use giggles_core::task::context::TaskContext;
use giggles_core::task::definition::WallClockMs;
use giggles_core::task::statements::{TaskStatement, parse};
use giggles_core::task::{
    Actor, Budget, DenyRule, OperationId, Outcome, PreviewId, PreviewSummary, ProtectedRequest,
    Receipt, Surface, TaskError, approve, outcome, preview, protected_commit, revoke_approval,
};
use giggles_core::{Admin, Engine, Limits};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{SystemTime, UNIX_EPOCH};

const TASK: &str = "inc-436";
/// 2026-11-05 18:00:00+00 in microseconds since 2000-01-01 UTC.
const PIN_US: i64 = 847_216_800_000_000;
const APPROVER: &str = "oncall_lead";
const ISSUER: &str = "incident_controller";

struct Dir(PathBuf);
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
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
    match &sql(
        admin,
        &format!("SELECT count(*) FROM shipments WHERE {filter}"),
    )[0][0]
    {
        Value::Int(n) => *n,
        other => panic!("{other:?}"),
    }
}

fn queued(admin: &Admin) -> i64 {
    count(admin, "status = 'queued'")
}

/// A fresh v2 database with the worked example's table, roles and task,
/// approving above `threshold` rows and capped at `cap` distinct rows and
/// transitions.
fn setup(threshold: u64, cap: u64) -> (Dir, Admin) {
    setup_caps(threshold, cap, cap)
}

fn setup_caps(threshold: u64, cap: u64, transitions: u64) -> (Dir, Admin) {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let dir = Dir(std::env::temp_dir().join(format!(
        "task-legacy-{}-{}-{}",
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
    rows.push("(50, 42, 'delivered', NULL, 0, 'done@example.com')".into());
    rows.push("(100, 43, 'failed', '2026-11-05 17:03:00+00', 1, 'other@example.com')".into());
    sql(
        &admin,
        &format!("INSERT INTO shipments VALUES {}", rows.join(", ")),
    );
    let roles = admin.roles();
    for (name, task_only) in [(ISSUER, false), (APPROVER, false), ("repair_agent", true)] {
        roles
            .create(NewRole {
                name: name.into(),
                login: true,
                ..NewRole::default()
            })
            .unwrap();
        if task_only {
            roles.set_task_only(name, true).unwrap();
        }
    }
    for name in [ISSUER, "repair_agent"] {
        roles
            .grant_table(
                "shipments",
                name,
                &[TablePrivilege::Select, TablePrivilege::Update],
            )
            .unwrap();
    }
    let text = format!(
        "CREATE TASK \"{TASK}\" \
           EXECUTOR repair_agent APPROVER {APPROVER} \
           ON shipments \
           VISIBLE ROWS WHERE (merchant_id = 42) \
           READABLE (id, status, failed_at, attempt_count) \
           MASKED (customer_email) \
           ALLOW UPDATE (status, failed_at) \
             OLD ROW WHERE (status = 'failed' AND failed_at >= '2026-11-05 17:00:00+00' \
                            AND failed_at < '2026-11-05 17:07:00+00') \
             NEW ROW WHERE (status = 'queued') \
             AT MOST {cap} DISTINCT ROWS AND {transitions} TRANSITIONS \
             APPROVE ABOVE {threshold} \
           DISCLOSE AT MOST 1000 ROWS READ, 2097152 BYTES RETURNED \
           WITH (statement_ms = 2000, rows_scanned = 100000, concurrent_statements = 2) \
           EXPIRES '{}'",
        "2099-12-31 00:00:00+00"
    );
    let issuer = roles.get(ISSUER).unwrap().unwrap();
    let Some(TaskStatement::Create(new)) = parse(&text) else {
        panic!("CREATE TASK did not parse");
    };
    admin
        .tasks()
        .create(&issuer, *new, WallClockMs(now_ms()))
        .unwrap();
    (dir, admin)
}

/// The worked example: approval above 5 rows, 25 distinct rows and transitions.
fn example() -> (Dir, Admin) {
    setup(5, 25)
}

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

fn clock() -> StatementClock {
    StatementClock {
        unavailable: None,
        transaction_us: PIN_US,
        statement_us: PIN_US + 1_000_000,
        zone: None,
        wall: Some(Arc::new(|| Ok(PIN_US + 5_000_000))),
    }
}

fn op(id: &str) -> OperationId {
    OperationId::new(id.as_bytes()).unwrap()
}

/// Requeue merchant 42's failed rows `from..=to`.
fn requeue(from: i64, to: i64) -> String {
    format!(
        "UPDATE shipments SET status = 'queued' \
         WHERE status = 'failed' AND id >= {from} AND id <= {to}"
    )
}

/// The agent previews `text` in its task session.
fn preview_text(admin: &Admin, ctx: &TaskContext, text: &str) -> PreviewSummary {
    let mut agent = admin.sql_session();
    agent.bind_task(ctx.clone()).unwrap();
    preview(&mut agent, ctx, Surface::Sql, text, &[], &clock())
        .unwrap_or_else(|e| panic!("{text}: {e:?}"))
}

fn approve_by(admin: &Admin, ctx: &TaskContext, p: &PreviewSummary) {
    approve(
        &mut admin.sql_session(),
        &APPROVER.to_owned(),
        &ctx.name,
        p.id,
        p.digest,
    )
    .unwrap();
}

/// `APPLY CHANGE <id> DIGEST '<digest>'` under operation `id`.
fn apply_change(
    admin: &Admin,
    ctx: &TaskContext,
    id: &str,
    change: PreviewId,
    digest: [u8; 32],
) -> Result<Receipt, TaskError> {
    let mut session = admin.sql_session();
    let request = ProtectedRequest::Apply {
        preview: change,
        digest,
    };
    protected_commit(&mut session, Actor::Executor(ctx), &op(id), request)
}

fn apply(
    admin: &Admin,
    ctx: &TaskContext,
    id: &str,
    p: &PreviewSummary,
) -> Result<Receipt, TaskError> {
    apply_change(admin, ctx, id, p.id, p.digest)
}

/// `OPERATION '<id>' <text>`, a direct protected write.
fn direct(admin: &Admin, ctx: &TaskContext, id: &str, text: &str) -> Result<Receipt, TaskError> {
    let mut session = admin.sql_session();
    let request = ProtectedRequest::Direct {
        statement: session
            .prepare(text)
            .unwrap_or_else(|e| panic!("{text}: {e:?}")),
        params: Vec::new(),
        text: Arc::from(text),
    };
    protected_commit(&mut session, Actor::Executor(ctx), &op(id), request)
}

fn lookup(admin: &Admin, ctx: &TaskContext, id: &str) -> Option<Outcome> {
    outcome(&admin.sql_session(), ctx, &op(id)).unwrap()
}

/// The preview `id` is still open and its approval, if any, unconsumed.
fn still_open(admin: &Admin, ctx: &TaskContext, p: &PreviewSummary) {
    let store = admin.task_previews(ctx.task).unwrap();
    let header = store
        .previews
        .iter()
        .find(|h| h.id == p.id)
        .expect("preview kept");
    assert_eq!(header.state, 0, "preview still open");
    assert_eq!(header.rows.len(), p.rows as usize, "rows kept");
    if let Some(approval) = admin.task_approval(ctx.task, p.id).unwrap() {
        assert_ne!(approval.state, 3, "approval not consumed");
    }
}

/// A refusal that is not recorded: the ID stays free, no rows changed, the
/// preview stays open.
fn unrecorded(admin: &Admin, ctx: &TaskContext, id: &str, p: &PreviewSummary, queued_before: i64) {
    assert_eq!(lookup(admin, ctx, id), None, "operation ID stays free");
    assert_eq!(queued(admin), queued_before, "no rows written");
    still_open(admin, ctx, p);
}

fn head_counts(admin: &Admin, ctx: &TaskContext, distinct: i64, transitions: i64) {
    let issuer = admin.roles().get(ISSUER).unwrap().unwrap();
    let report = giggles_core::task::exposure::show_exposure(
        &admin.sql_session(),
        &issuer,
        ctx.name.as_str(),
        &[],
    )
    .unwrap();
    for (item, expected) in [("distinct_rows", distinct), ("transitions", transitions)] {
        let row = report
            .rows
            .iter()
            .find(|r| {
                r.values[0] == Value::Text("changed".into())
                    && r.values[1] == Value::Text(item.into())
            })
            .unwrap();
        assert_eq!(row.values[3], Value::Int(expected), "{item}");
    }
}
// Prepare every request before the barrier, so all threads contend at commit.
fn race(
    admin: &Admin,
    ctx: &TaskContext,
    same_id: bool,
    same_text: bool,
) -> Vec<Result<Receipt, TaskError>> {
    let barrier = Barrier::new(10);
    std::thread::scope(|scope| {
        let jobs: Vec<_> = (1..=10)
            .map(|n| {
                let barrier = &barrier;
                scope.spawn(move || {
                    let text =
                        requeue(if same_text { 1 } else { n }, if same_text { 1 } else { n });
                    let id = if same_id {
                        "shared".into()
                    } else {
                        format!("race-{n}")
                    };
                    let mut session = admin.sql_session();
                    let request = ProtectedRequest::Direct {
                        statement: session.prepare(&text).unwrap(),
                        params: vec![],
                        text: Arc::from(text),
                    };
                    barrier.wait();
                    protected_commit(&mut session, Actor::Executor(ctx), &op(&id), request)
                })
            })
            .collect();
        jobs.into_iter().map(|job| job.join().unwrap()).collect()
    })
}

#[test]
fn ten_thread_last_slot_race() {
    let (_dir, admin) = setup(10, 1);
    let ctx = context(&admin);
    let results = race(&admin, &ctx, false, false);
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Err(TaskError::BudgetExceeded(Budget::DistinctRows))))
            .count(),
        9
    );
    let receipt = results.iter().find_map(|r| r.as_ref().ok()).unwrap();
    assert_eq!(
        (receipt.distinct_charged, receipt.transitions_charged),
        (1, 1)
    );
    assert_eq!(queued(&admin), 1);
    head_counts(&admin, &ctx, 1, 1);
    for n in 1..=10 {
        assert!(lookup(&admin, &ctx, &format!("race-{n}")).is_some());
    }
    // A fresh operation still cannot spend a second slot: counters are durable.
    assert!(matches!(
        direct(&admin, &ctx, "extra", &requeue(20, 20)),
        Err(TaskError::BudgetExceeded(_))
    ));
}

#[test]
fn concurrent_identical_retry_charges_once() {
    let (_dir, admin) = setup(10, 1);
    let ctx = context(&admin);
    let results = race(&admin, &ctx, true, true);
    let first = results[0].as_ref().unwrap();
    for result in &results {
        let r = result.as_ref().unwrap();
        assert_eq!((r.seq, r.effects_digest), (first.seq, first.effects_digest));
        assert_eq!((r.distinct_charged, r.transitions_charged), (1, 1));
    }
    assert_eq!(first.seq, 1);
    head_counts(&admin, &ctx, 1, 1);
    assert_eq!(queued(&admin), 1);
    assert!(matches!(
        lookup(&admin, &ctx, "shared"),
        Some(Outcome::Committed(_))
    ));
}

#[test]
fn failed_multirow_is_atomic_and_recorded() {
    let (_dir, admin) = example();
    let ctx = context(&admin);
    sql(
        &admin,
        "UPDATE shipments SET failed_at = TIMESTAMPTZ '2026-11-05 16:00:00+00' WHERE id = 2",
    );
    let text = requeue(1, 2);
    let denied = TaskError::Denied(DenyRule::OutsideAllowedWrite);
    assert_eq!(direct(&admin, &ctx, "bad", &text).unwrap_err(), denied);
    assert_eq!(queued(&admin), 0);
    assert_eq!(
        lookup(&admin, &ctx, "bad"),
        Some(Outcome::Denied(DenyRule::OutsideAllowedWrite))
    );
    sql(
        &admin,
        "UPDATE shipments SET failed_at = TIMESTAMPTZ '2026-11-05 17:03:00+00' WHERE id = 2",
    );
    assert_eq!(direct(&admin, &ctx, "bad", &text).unwrap_err(), denied);
    assert_eq!(queued(&admin), 0);
    assert_eq!(
        direct(&admin, &ctx, "fixed", &text).unwrap().rows_affected,
        2
    );
}

#[test]
fn changed_row_invalidates_unrelated_write_outside_scan_does_not() {
    for selected in [true, false] {
        let (_dir, admin) = setup(0, 25);
        let ctx = context(&admin);
        let p = preview_text(&admin, &ctx, &requeue(1, 1));
        approve_by(&admin, &ctx, &p);
        let id = if selected { 1 } else { 100 };
        sql(
            &admin,
            &format!("UPDATE shipments SET attempt_count = 2 WHERE id = {id}"),
        );
        sql(
            &admin,
            &format!("UPDATE shipments SET attempt_count = 1 WHERE id = {id}"),
        );
        if selected {
            assert_eq!(
                apply(&admin, &ctx, "aba", &p).unwrap_err(),
                TaskError::ApprovalStale
            );
            unrecorded(&admin, &ctx, "aba", &p, 0);
        } else {
            assert_eq!(apply(&admin, &ctx, "outside", &p).unwrap().rows_affected, 1);
            assert_eq!(queued(&admin), 1);
        }
    }
}

#[test]
fn repeated_transitions_and_distinct_cumulative() {
    let (_dir, admin) = setup_caps(10, 1, 2);
    let ctx = context(&admin);
    for n in 1..=2 {
        let r = direct(&admin, &ctx, &format!("repeat-{n}"), &requeue(1, 1)).unwrap();
        assert_eq!(
            (r.distinct_charged, r.transitions_charged),
            (u32::from(n == 1), 1)
        );
        sql(
            &admin,
            "UPDATE shipments SET status = 'failed' WHERE id = 1",
        );
        if n == 1 {
            assert_eq!(
                direct(&admin, &ctx, "new-row", &requeue(2, 2)).unwrap_err(),
                TaskError::BudgetExceeded(Budget::DistinctRows)
            );
        }
    }
    // Both lifetime caps are exhausted, with only one distinct row charged.
    assert_eq!(
        direct(&admin, &ctx, "over", &requeue(1, 1)).unwrap_err(),
        TaskError::BudgetExceeded(Budget::Transitions)
    );
    assert_eq!(queued(&admin), 0);
    head_counts(&admin, &ctx, 1, 2);
}

#[test]
fn altered_request_race_binds_one_request() {
    let (_dir, admin) = setup(10, 25);
    let ctx = context(&admin);
    let results = race(&admin, &ctx, true, false);
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Err(TaskError::OperationConflict)))
            .count(),
        9
    );
    let winner = results.iter().find_map(|r| r.as_ref().ok()).unwrap();
    match lookup(&admin, &ctx, "shared").unwrap() {
        Outcome::Committed(r) => assert_eq!(
            (r.seq, r.effects_digest),
            (winner.seq, winner.effects_digest)
        ),
        other => panic!("{other:?}"),
    }
    assert_eq!(queued(&admin), 1);
}

#[test]
fn noop_update_charges_no_transition_but_is_recorded() {
    let (_dir, admin) = example();
    let ctx = context(&admin);
    // Zero matched rows is the portable R3 no-op; OLD ROW rules still apply
    // to matching rows, even when their assigned values would not change.
    let text = requeue(1000, 1001);
    let r = direct(&admin, &ctx, "empty", &text).unwrap();
    assert_eq!(
        (r.rows_affected, r.distinct_charged, r.transitions_charged),
        (0, 0, 0)
    );
    assert!(matches!(
        lookup(&admin, &ctx, "empty"),
        Some(Outcome::Committed(_))
    ));
    assert_eq!(direct(&admin, &ctx, "empty", &text).unwrap().seq, r.seq);
    assert_eq!(
        direct(
            &admin,
            &ctx,
            "scope",
            "UPDATE shipments SET status = 'delivered' WHERE id = 50"
        )
        .unwrap_err(),
        TaskError::Denied(DenyRule::OutsideAllowedWrite)
    );
    assert_eq!(queued(&admin), 0);
}

// Create a second immutable task on the same table, preserving all policy
// clauses while choosing its own identity and disclosure budget.
fn second_task(admin: &Admin, name: &str) -> TaskContext {
    let def = admin.tasks().get(TASK).unwrap().unwrap();
    let mut new = format!(
        "CREATE TASK \"{name}\" EXECUTOR repair_agent APPROVER {APPROVER} ON shipments \
         VISIBLE ROWS WHERE (merchant_id = 42) READABLE (id, status, failed_at, attempt_count) \
         MASKED (customer_email) ALLOW UPDATE (status) OLD ROW WHERE (status = 'failed') \
         NEW ROW WHERE (status = 'queued') AT MOST 25 DISTINCT ROWS AND 25 TRANSITIONS \
         APPROVE ABOVE 0 DISCLOSE AT MOST 1000 ROWS READ, 2097152 BYTES RETURNED WITH (concurrent_statements = 32) EXPIRES '{}'",
        "2099-12-31 00:00:00+00"
    );
    // Keep this helper useful for disclosure tests without modifying a task.
    if name == "disclosure-b" {
        new = new.replace("2097152 BYTES", "17 BYTES");
    }
    let Some(TaskStatement::Create(new)) = parse(&new) else {
        panic!("parse second task");
    };
    let issuer = admin.roles().get(ISSUER).unwrap().unwrap();
    let task = admin
        .tasks()
        .create(&issuer, *new, WallClockMs(now_ms()))
        .unwrap();
    TaskContext {
        task: task.id,
        name: task.name,
        executor: def.executor.clone(),
        bound_def_version: task.def_version,
        credential_fingerprint: [8; 32],
    }
}

#[test]
fn unknown_or_forged_preview_never_authorizes() {
    let (_dir, admin) = setup(0, 25);
    let ctx = context(&admin);
    let p = preview_text(&admin, &ctx, &requeue(1, 1));
    approve_by(&admin, &ctx, &p);
    let other = second_task(&admin, "other");
    let q = preview_text(&admin, &other, &requeue(2, 2));
    approve_by(&admin, &other, &q);
    for (id, digest) in [
        (PreviewId(u64::MAX), p.digest),
        (p.id, [0; 32]),
        (q.id, q.digest),
    ] {
        assert_eq!(
            apply_change(&admin, &ctx, "forged", id, digest).unwrap_err(),
            TaskError::ApprovalStale
        );
        unrecorded(&admin, &ctx, "forged", &p, 0);
        still_open(&admin, &other, &q);
    }
    assert_eq!(apply(&admin, &ctx, "valid", &p).unwrap().rows_affected, 1);
}

#[test]
fn stale_preview_then_repreview_and_approval() {
    let (_dir, admin) = setup(0, 25);
    let ctx = context(&admin);
    let text = "UPDATE shipments SET status = 'queued' WHERE status = 'failed' AND id <= 2";
    let p = preview_text(&admin, &ctx, text);
    approve_by(&admin, &ctx, &p);
    sql(
        &admin,
        "INSERT INTO shipments VALUES (0, 42, 'failed', '2026-11-05 17:03:00+00', 1, 'new@example.com')",
    );
    assert_eq!(
        apply(&admin, &ctx, "stale", &p).unwrap_err(),
        TaskError::ApprovalStale
    );
    unrecorded(&admin, &ctx, "stale", &p, 0);
    let q = preview_text(&admin, &ctx, text);
    assert_eq!(q.rows, 3);
    approve_by(&admin, &ctx, &q);
    assert_eq!(apply(&admin, &ctx, "fresh", &q).unwrap().rows_affected, 3);
    assert_eq!(queued(&admin), 3);
}

#[test]
fn revoked_task_and_approval_deny_and_persist() {
    let (dir, admin) = setup(0, 25);
    let ctx = context(&admin);
    let p = preview_text(&admin, &ctx, &requeue(1, 1));
    approve_by(&admin, &ctx, &p);
    revoke_approval(
        &mut admin.sql_session(),
        &APPROVER.to_owned(),
        &ctx.name,
        p.id,
    )
    .unwrap();
    assert_eq!(
        apply(&admin, &ctx, "revoked-approval", &p).unwrap_err(),
        TaskError::ApprovalRequired
    );
    assert_eq!(lookup(&admin, &ctx, "revoked-approval"), None);
    drop(admin);
    let admin = Admin::open(&dir.0, Limits::default()).unwrap();
    assert_eq!(
        admin.task_approval(ctx.task, p.id).unwrap().unwrap().state,
        2
    );
    assert_eq!(
        apply(&admin, &ctx, "revoked-approval", &p).unwrap_err(),
        TaskError::ApprovalRequired
    );
    let issuer = admin.roles().get(ISSUER).unwrap().unwrap();
    admin.tasks().revoke(&issuer, TASK).unwrap();
    assert_eq!(
        direct(&admin, &ctx, "revoked-task", &requeue(1, 1)).unwrap_err(),
        TaskError::Revoked
    );
    drop(admin);
    let admin = Admin::open(&dir.0, Limits::default()).unwrap();
    assert_eq!(
        admin.task_approval(ctx.task, p.id).unwrap().unwrap().state,
        2
    );
    assert_eq!(
        direct(&admin, &ctx, "new-write", &requeue(1, 1)).unwrap_err(),
        TaskError::Revoked
    );
    assert_eq!(queued(&admin), 0);
}

fn read_one(admin: &Admin, ctx: &TaskContext) -> Result<(), giggles_core::CoreError> {
    let mut session = admin.sql_session();
    session.bind_task(ctx.clone()).unwrap();
    session.set_disclosure_chunk(0, 0);
    let page = session.execute_unreleased("SELECT id FROM shipments WHERE id = 1", &[], None)?;
    session.release_page(page).map(|_| ())
}

// A bigint row costs 8 bytes framing + 9 bytes value. Make exactly one
// release affordable while leaving ample read budget for all contenders.
fn disclosure_setup() -> (Dir, Admin) {
    let (dir, admin) = setup(10, 25);
    admin.set_agent_statement_ceiling(16).unwrap();
    let ctx = second_task(&admin, "disclosure-b");
    assert_eq!(ctx.name.as_str(), "disclosure-b");
    (dir, admin)
}

fn named_context(admin: &Admin, name: &str) -> TaskContext {
    let def = admin.tasks().get(name).unwrap().unwrap();
    TaskContext {
        task: def.id,
        name: def.name.clone(),
        executor: def.executor.clone(),
        bound_def_version: def.def_version,
        credential_fingerprint: [9; 32],
    }
}

#[test]
fn last_disclosure_budget_one_winner_task_scoped() {
    let (_dir, admin) = disclosure_setup();
    let a = named_context(&admin, "disclosure-b");
    let b = context(&admin);
    let barrier = Barrier::new(16);
    let results: Vec<_> = std::thread::scope(|scope| {
        let jobs: Vec<_> = (0..16)
            .map(|_| {
                let (admin, a, barrier) = (&admin, &a, &barrier);
                scope.spawn(move || {
                    barrier.wait();
                    read_one(admin, a)
                })
            })
            .collect();
        jobs.into_iter().map(|j| j.join().unwrap()).collect()
    });
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    for e in results.iter().filter_map(|r| r.as_ref().err()) {
        assert_eq!(
            *e,
            giggles_core::CoreError::Task(TaskError::BudgetExceeded(Budget::BytesReturned))
        );
    }
    let used = admin.task_disclosure(a.task).unwrap();
    assert_eq!((used.rows_returned, used.bytes_returned), (1, 17));
    assert_eq!(admin.task_disclosure(b.task).unwrap().bytes_returned, 0);
    read_one(&admin, &b).unwrap();
    assert_eq!(admin.task_disclosure(b.task).unwrap().rows_returned, 1);
}

#[test]
fn commit_survives_withheld_disclosure_no_refund() {
    let (_dir, admin) = disclosure_setup();
    let ctx = named_context(&admin, "disclosure-b");
    let text = requeue(1, 1);
    let p = preview_text(&admin, &ctx, &text);
    approve_by(&admin, &ctx, &p);
    let r = apply(&admin, &ctx, "committed", &p).unwrap();
    read_one(&admin, &ctx).unwrap();
    assert_eq!(
        read_one(&admin, &ctx).unwrap_err(),
        giggles_core::CoreError::Task(TaskError::BudgetExceeded(Budget::BytesReturned))
    );
    let before = admin.task_disclosure(ctx.task).unwrap();
    assert_eq!(queued(&admin), 1);
    assert!(matches!(
        lookup(&admin, &ctx, "committed"),
        Some(Outcome::Committed(_))
    ));
    let retry = apply(&admin, &ctx, "committed", &p).unwrap();
    assert_eq!((retry.seq, retry.effects_digest), (r.seq, r.effects_digest));
    assert_eq!(admin.task_disclosure(ctx.task).unwrap(), before);
    assert_eq!((before.rows_returned, before.bytes_returned), (1, 17));
    assert_eq!(queued(&admin), 1);
    // The retry neither refunds nor recharges the mutation counters.
    head_counts(&admin, &ctx, 1, 1);
}
