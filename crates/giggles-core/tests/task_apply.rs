//! GD-0061 R3-706: APPLY of a stored preview through the protected commit
//! (design-7 D7.6, D7.3), through the public API.
//!
//! `deliveries` is reserved, so the table is `shipments`, as in
//! `task_protected_commit.rs`: merchant 42's failed rows 1-40 fall inside the
//! OLD ROW window, row 50 is delivered and row 100 belongs to merchant 43.
//! `incident_controller` issues the task, `oncall_lead` approves and
//! `repair_agent` executes.
#![cfg(feature = "engine-rocksdb")]
use giggles_core::relational::{StatementClock, Value};
use giggles_core::roles::{NewRole, TablePrivilege};
use giggles_core::task::context::TaskContext;
use giggles_core::task::definition::WallClockMs;
use giggles_core::task::statements::{parse, TaskStatement};
use giggles_core::task::{
    approve, outcome, preview, protected_commit, revoke_approval, Actor, Budget, DenyRule,
    OperationId, Outcome, PreviewId, PreviewSummary, ProtectedRequest, Receipt, Surface,
    TaskError,
};
use giggles_core::{Admin, Engine, Limits};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const TASK: &str = "inc-436";
const DAY_MS: i64 = 86_400_000;
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

/// A fresh v2 database with the worked example's table, roles and task,
/// approving above `threshold` rows and capped at `cap` distinct rows and
/// transitions.
fn setup(threshold: u64, cap: u64) -> (Dir, Admin) {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let dir = Dir(std::env::temp_dir().join(format!(
        "task-apply-{}-{}-{}",
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
    sql(&admin, &format!("INSERT INTO shipments VALUES {}", rows.join(", ")));
    let roles = admin.roles();
    for (name, task_only) in [(ISSUER, false), (APPROVER, false), ("repair_agent", true)] {
        roles
            .create(NewRole { name: name.into(), login: true, ..NewRole::default() })
            .unwrap();
        if task_only {
            roles.set_task_only(name, true).unwrap();
        }
    }
    for name in [ISSUER, "repair_agent"] {
        roles
            .grant_table("shipments", name, &[TablePrivilege::Select, TablePrivilege::Update])
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
             AT MOST {cap} DISTINCT ROWS AND {cap} TRANSITIONS \
             APPROVE ABOVE {threshold} \
           DISCLOSE AT MOST 1000 ROWS READ, 2097152 BYTES RETURNED \
           WITH (statement_ms = 2000, rows_scanned = 100000, concurrent_statements = 2) \
           EXPIRES '{}'",
        timestamptz(now_ms() + DAY_MS)
    );
    let issuer = roles.get(ISSUER).unwrap().unwrap();
    let Some(TaskStatement::Create(new)) = parse(&text) else {
        panic!("CREATE TASK did not parse");
    };
    admin.tasks().create(&issuer, *new, WallClockMs(now_ms())).unwrap();
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
    preview(&mut agent, ctx, Surface::Sql, text, &[], &clock()).unwrap_or_else(|e| panic!("{text}: {e:?}"))
}

fn approve_by(admin: &Admin, ctx: &TaskContext, p: &PreviewSummary) {
    approve(&mut admin.sql_session(), &APPROVER.to_owned(), &ctx.name, p.id, p.digest).unwrap();
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
    let request = ProtectedRequest::Apply { preview: change, digest };
    protected_commit(&mut session, Actor::Executor(ctx), &op(id), request)
}

fn apply(admin: &Admin, ctx: &TaskContext, id: &str, p: &PreviewSummary) -> Result<Receipt, TaskError> {
    apply_change(admin, ctx, id, p.id, p.digest)
}

/// `OPERATION '<id>' <text>`, a direct protected write.
fn direct(admin: &Admin, ctx: &TaskContext, id: &str, text: &str) -> Result<Receipt, TaskError> {
    let mut session = admin.sql_session();
    let request = ProtectedRequest::Direct {
        statement: session.prepare(text).unwrap_or_else(|e| panic!("{text}: {e:?}")),
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
    let header = store.previews.iter().find(|h| h.id == p.id).expect("preview kept");
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

#[test]
fn apply_commits_exact_set_and_consumes_approval() {
    let (_dir, admin) = example();
    let ctx = context(&admin);
    let p = preview_text(&admin, &ctx, &requeue(1, 10));
    assert_eq!(p.rows, 10);
    approve_by(&admin, &ctx, &p);
    let before = admin.task_previews(ctx.task).unwrap();
    assert_eq!(before.stored_previews, 1);
    let receipt = apply(&admin, &ctx, "apply-1", &p).unwrap();
    assert_eq!(receipt.rows_affected, 10);
    assert_eq!(receipt.distinct_charged, 10);
    assert_eq!(queued(&admin), 10);
    assert_eq!(count(&admin, "status = 'queued' AND id >= 1 AND id <= 10"), 10);
    // The approval is consumed at the receipt's seq, the preview applied and
    // its rows gone; the head no longer counts it as open.
    let approval = admin.task_approval(ctx.task, p.id).unwrap().unwrap();
    assert_eq!((approval.state, approval.consumed_seq), (3, receipt.seq));
    let store = admin.task_previews(ctx.task).unwrap();
    assert_eq!(store.stored_previews, 0);
    assert_eq!(store.stored_rows, 0);
    let header = store.previews.iter().find(|h| h.id == p.id).unwrap();
    assert_eq!(header.state, 1, "applied");
    // The stored outcome is the committed receipt; a resend replays it.
    match lookup(&admin, &ctx, "apply-1") {
        Some(Outcome::Committed(stored)) => assert_eq!(
            (stored.seq, stored.rows_affected, stored.effects_digest),
            (receipt.seq, 10, receipt.effects_digest)
        ),
        other => panic!("{other:?}"),
    }
    let again = apply(&admin, &ctx, "apply-1", &p).unwrap();
    assert_eq!((again.seq, again.effects_digest), (receipt.seq, receipt.effects_digest));
    assert_eq!(queued(&admin), 10);
}

#[test]
fn changed_row_makes_apply_stale_and_op_id_stays_free() {
    let (_dir, admin) = example();
    let ctx = context(&admin);
    let p = preview_text(&admin, &ctx, &requeue(1, 10));
    approve_by(&admin, &ctx, &p);
    // Row 3 still matches, but its version changed.
    sql(&admin, "UPDATE shipments SET attempt_count = 2 WHERE id = 3");
    assert_eq!(apply(&admin, &ctx, "op-1", &p).unwrap_err(), TaskError::ApprovalStale);
    unrecorded(&admin, &ctx, "op-1", &p, 0);
    // The ID is free: a direct write may still use it.
    let receipt = direct(&admin, &ctx, "op-1", &requeue(1, 2)).unwrap();
    assert_eq!(receipt.rows_affected, 2);
}

#[test]
fn extra_matching_row_makes_apply_stale() {
    let (_dir, admin) = example();
    let ctx = context(&admin);
    let text = "UPDATE shipments SET status = 'queued' WHERE status = 'failed' AND id <= 10";
    let p = preview_text(&admin, &ctx, text);
    assert_eq!(p.rows, 10);
    approve_by(&admin, &ctx, &p);
    // Row 0 appears and matches the stored statement.
    sql(
        &admin,
        "INSERT INTO shipments VALUES (0, 42, 'failed', '2026-11-05 17:03:00+00', 1, 'z@example.com')",
    );
    assert_eq!(apply(&admin, &ctx, "op-1", &p).unwrap_err(), TaskError::ApprovalStale);
    unrecorded(&admin, &ctx, "op-1", &p, 0);
}

#[test]
fn row_that_stopped_matching_makes_apply_stale() {
    let (_dir, admin) = example();
    let ctx = context(&admin);
    let p = preview_text(&admin, &ctx, &requeue(1, 10));
    approve_by(&admin, &ctx, &p);
    sql(&admin, "UPDATE shipments SET status = 'delivered' WHERE id = 4");
    assert_eq!(apply(&admin, &ctx, "op-1", &p).unwrap_err(), TaskError::ApprovalStale);
    unrecorded(&admin, &ctx, "op-1", &p, 0);
}

#[test]
fn apply_without_approval_above_threshold_is_approval_required() {
    let (_dir, admin) = example();
    let ctx = context(&admin);
    let p = preview_text(&admin, &ctx, &requeue(1, 6));
    assert_eq!(p.rows, 6);
    assert_eq!(apply(&admin, &ctx, "op-1", &p).unwrap_err(), TaskError::ApprovalRequired);
    unrecorded(&admin, &ctx, "op-1", &p, 0);
    assert_eq!(admin.task_approval(ctx.task, p.id).unwrap(), None);
}

#[test]
fn apply_at_or_below_threshold_needs_no_approval() {
    let (_dir, admin) = example();
    let ctx = context(&admin);
    let p = preview_text(&admin, &ctx, &requeue(1, 5));
    assert_eq!(p.rows, 5);
    let receipt = apply(&admin, &ctx, "op-1", &p).unwrap();
    assert_eq!(receipt.rows_affected, 5);
    assert_eq!(queued(&admin), 5);
    assert_eq!(admin.task_approval(ctx.task, p.id).unwrap(), None);
    let header = admin.task_previews(ctx.task).unwrap().previews[0].clone();
    assert_eq!(header.state, 1, "applied");
    // Still the exact set: a second preview at the threshold goes stale when
    // one of its rows changes.
    let q = preview_text(&admin, &ctx, &requeue(11, 15));
    sql(&admin, "UPDATE shipments SET attempt_count = 3 WHERE id = 12");
    assert_eq!(apply(&admin, &ctx, "op-2", &q).unwrap_err(), TaskError::ApprovalStale);
    unrecorded(&admin, &ctx, "op-2", &q, 5);
}

#[test]
fn threshold_zero_direct_write_is_approval_required_and_not_recorded() {
    let (_dir, admin) = setup(0, 25);
    let ctx = context(&admin);
    assert_eq!(direct(&admin, &ctx, "op-1", &requeue(1, 1)).unwrap_err(), TaskError::ApprovalRequired);
    assert_eq!(lookup(&admin, &ctx, "op-1"), None);
    assert_eq!(queued(&admin), 0);
    // A statement that matches no row commits with count 0.
    let receipt = direct(&admin, &ctx, "op-2", &requeue(1000, 1001)).unwrap();
    assert_eq!(receipt.rows_affected, 0);
    // The one-row write goes through preview, approve and apply.
    let p = preview_text(&admin, &ctx, &requeue(1, 1));
    assert_eq!(apply(&admin, &ctx, "op-1", &p).unwrap_err(), TaskError::ApprovalRequired);
    approve_by(&admin, &ctx, &p);
    assert_eq!(apply(&admin, &ctx, "op-1", &p).unwrap().rows_affected, 1);
    assert_eq!(queued(&admin), 1);
}

#[test]
fn approval_is_single_use() {
    let (_dir, admin) = example();
    let ctx = context(&admin);
    let p = preview_text(&admin, &ctx, &requeue(1, 10));
    approve_by(&admin, &ctx, &p);
    let receipt = apply(&admin, &ctx, "op-1", &p).unwrap();
    // Another operation ID cannot apply the same preview again.
    assert_eq!(apply(&admin, &ctx, "op-2", &p).unwrap_err(), TaskError::ApprovalStale);
    assert_eq!(lookup(&admin, &ctx, "op-2"), None);
    // Nor can the consumed approval be renewed.
    let again = approve(&mut admin.sql_session(), &APPROVER.to_owned(), &ctx.name, p.id, p.digest);
    assert_eq!(again.unwrap_err(), TaskError::ApprovalStale);
    let approval = admin.task_approval(ctx.task, p.id).unwrap().unwrap();
    assert_eq!((approval.state, approval.consumed_seq), (3, receipt.seq));
    // A fresh preview of the same rows finds nothing left to change, and its
    // store sweeps the applied preview with its approval.
    let q = preview_text(&admin, &ctx, &requeue(1, 10));
    assert_eq!(q.rows, 0);
    assert_eq!(queued(&admin), 10);
    assert_eq!(admin.task_approval(ctx.task, p.id).unwrap(), None);
    let store = admin.task_previews(ctx.task).unwrap();
    assert_eq!(store.previews.iter().map(|h| h.id).collect::<Vec<_>>(), [q.id]);
    assert_eq!(store.stored_previews, 1);
}

#[test]
fn revoked_approval_is_approval_required() {
    let (_dir, admin) = example();
    let ctx = context(&admin);
    let p = preview_text(&admin, &ctx, &requeue(1, 10));
    approve_by(&admin, &ctx, &p);
    revoke_approval(&mut admin.sql_session(), &APPROVER.to_owned(), &ctx.name, p.id).unwrap();
    assert_eq!(apply(&admin, &ctx, "op-1", &p).unwrap_err(), TaskError::ApprovalRequired);
    unrecorded(&admin, &ctx, "op-1", &p, 0);
    assert_eq!(admin.task_approval(ctx.task, p.id).unwrap().unwrap().state, 2);
    // Approved again, it applies.
    approve_by(&admin, &ctx, &p);
    assert_eq!(apply(&admin, &ctx, "op-1", &p).unwrap().rows_affected, 10);
}

#[test]
fn apply_of_another_previews_digest_is_stale() {
    let (_dir, admin) = example();
    let ctx = context(&admin);
    let p = preview_text(&admin, &ctx, &requeue(1, 10));
    let q = preview_text(&admin, &ctx, &requeue(11, 20));
    approve_by(&admin, &ctx, &p);
    approve_by(&admin, &ctx, &q);
    assert_eq!(apply_change(&admin, &ctx, "op-1", p.id, q.digest).unwrap_err(), TaskError::ApprovalStale);
    assert_eq!(apply_change(&admin, &ctx, "op-1", q.id, p.digest).unwrap_err(), TaskError::ApprovalStale);
    let missing = PreviewId(q.id.0 + 1);
    assert_eq!(apply_change(&admin, &ctx, "op-1", missing, p.digest).unwrap_err(), TaskError::ApprovalStale);
    unrecorded(&admin, &ctx, "op-1", &p, 0);
    still_open(&admin, &ctx, &q);
}

#[test]
fn approved_apply_still_denied_outside_allowed_write() {
    let (_dir, admin) = example();
    let ctx = context(&admin);
    // NEW ROW WHERE (status = 'queued'): 'shipped' is outside the allowed
    // write. PREVIEW refuses it, so the hidden seam stores it unchecked.
    let text = "UPDATE shipments SET status = 'shipped' WHERE status = 'failed' AND id >= 1 AND id <= 10";
    let mut agent = admin.sql_session();
    agent.bind_task(ctx.clone()).unwrap();
    let refused = preview(&mut agent, &ctx, Surface::Sql, text, &[], &clock()).unwrap_err();
    assert_eq!(refused, TaskError::Denied(DenyRule::OutsideAllowedWrite));
    let p = admin.preview_task_without_policy(&ctx, text, &clock()).unwrap();
    assert_eq!(p.rows, 10);
    approve_by(&admin, &ctx, &p);
    // The approval does not widen the allowed write: the denial is recorded.
    let denied = apply(&admin, &ctx, "op-1", &p).unwrap_err();
    assert_eq!(denied, TaskError::Denied(DenyRule::OutsideAllowedWrite));
    assert_eq!(lookup(&admin, &ctx, "op-1"), Some(Outcome::Denied(DenyRule::OutsideAllowedWrite)));
    assert_eq!(count(&admin, "status = 'shipped'"), 0);
    still_open(&admin, &ctx, &p);
    assert_eq!(admin.task_approval(ctx.task, p.id).unwrap().unwrap().state, 1);
    // A resend replays the stored denial.
    assert_eq!(apply(&admin, &ctx, "op-1", &p).unwrap_err(), denied);
}

#[test]
fn apply_over_cap_is_budget_exceeded() {
    let (_dir, admin) = setup(5, 12);
    let ctx = context(&admin);
    let p = preview_text(&admin, &ctx, &requeue(1, 10));
    approve_by(&admin, &ctx, &p);
    // Three more distinct rows were written since the preview: 3 + 10 > 12.
    assert_eq!(direct(&admin, &ctx, "op-0", &requeue(11, 13)).unwrap().rows_affected, 3);
    let error = apply(&admin, &ctx, "op-1", &p).unwrap_err();
    assert_eq!(error, TaskError::BudgetExceeded(Budget::DistinctRows));
    assert_eq!(lookup(&admin, &ctx, "op-1"), Some(Outcome::BudgetExceeded(Budget::DistinctRows)));
    assert_eq!(queued(&admin), 3);
    still_open(&admin, &ctx, &p);
}

#[test]
fn def_version_change_makes_apply_stale() {
    let (_dir, admin) = example();
    let ctx = context(&admin);
    let p = preview_text(&admin, &ctx, &requeue(1, 10));
    approve_by(&admin, &ctx, &p);
    admin.set_task_preview_def_version(ctx.task, p.id, ctx.bound_def_version + 1).unwrap();
    assert_eq!(apply(&admin, &ctx, "op-1", &p).unwrap_err(), TaskError::ApprovalStale);
    assert_eq!(lookup(&admin, &ctx, "op-1"), None);
    assert_eq!(queued(&admin), 0);
}

#[test]
fn add_column_makes_apply_stale() {
    let (_dir, admin) = example();
    let ctx = context(&admin);
    let p = preview_text(&admin, &ctx, &requeue(1, 10));
    approve_by(&admin, &ctx, &p);
    sql(&admin, "ALTER TABLE shipments ADD COLUMN note text");
    assert_eq!(apply(&admin, &ctx, "op-1", &p).unwrap_err(), TaskError::ApprovalStale);
    unrecorded(&admin, &ctx, "op-1", &p, 0);
}

#[test]
fn direct_then_apply_same_op_id_conflicts() {
    let (_dir, admin) = example();
    let ctx = context(&admin);
    let p = preview_text(&admin, &ctx, &requeue(1, 10));
    approve_by(&admin, &ctx, &p);
    assert_eq!(direct(&admin, &ctx, "op-1", &requeue(11, 12)).unwrap().rows_affected, 2);
    let error = apply(&admin, &ctx, "op-1", &p).unwrap_err();
    assert_eq!(error, TaskError::OperationConflict);
    assert_eq!(error.sqlstate(), "42T03");
    assert_eq!(queued(&admin), 2);
    still_open(&admin, &ctx, &p);
    // Under its own ID the apply still commits.
    assert_eq!(apply(&admin, &ctx, "op-2", &p).unwrap().rows_affected, 10);
}

#[test]
fn apply_reruns_with_the_pinned_instant() {
    let (_dir, admin) = example();
    let ctx = context(&admin);
    let text = "UPDATE shipments SET status = 'queued', failed_at = now() \
                WHERE status = 'failed' AND id >= 1 AND id <= 3";
    let p = preview_text(&admin, &ctx, text);
    assert_eq!(p.rows, 3);
    apply(&admin, &ctx, "op-1", &p).unwrap();
    assert_eq!(count(&admin, "status = 'queued' AND failed_at = '2026-11-05 18:00:00+00'"), 3);
}
