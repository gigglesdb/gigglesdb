//! GD-0061 R3-704 / R3-705: APPROVE, REVOKE APPROVAL, SHOW CHANGES and SHOW
//! CHANGE (design-7 D7.5), through the public API, on previews stored by
//! `task::preview`.
//!
//! `deliveries` is reserved, so the table is `shipments`, as in
//! `task_preview.rs`. The task names `oncall_lead` as its approver,
//! `incident_controller` issues it and `repair_agent` executes it.
#![cfg(feature = "engine-rocksdb")]
use giggles_core::relational::{SqlResult, StatementClock, Value};
use giggles_core::roles::{NewRole, TablePrivilege};
use giggles_core::task::approval::{
    StoredApproval, CHANGED_SINCE_PREVIEW, CHANGES_COLUMNS, CHANGE_COLUMNS,
};
use giggles_core::task::context::TaskContext;
use giggles_core::task::definition::{TaskPrivileges, WallClockMs};
use giggles_core::task::statements::{parse, TaskStatement};
use giggles_core::task::{
    approve, list_changes, preview, revoke_approval, show_change, PreviewId, PreviewSummary,
    Surface, TaskError,
};
use giggles_core::{Admin, Engine, Limits};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const TASK: &str = "inc-436";
const DAY_MS: i64 = 86_400_000;
/// 2026-11-05 18:00:00+00 in microseconds since 2000-01-01 UTC.
const PIN_US: i64 = 847_216_800_000_000;
const APPROVER: &str = "oncall_lead";
const ISSUER: &str = "incident_controller";
const EXECUTOR: &str = "repair_agent";

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

fn sql(admin: &Admin, statement: &str) {
    admin
        .sql_session()
        .execute(statement, &[], None)
        .unwrap_or_else(|e| panic!("{statement}: {e:?}"));
}

/// A fresh v2 database with the D9 table, the roles and the worked
/// example's task, with `with` appended to its load limits.
fn setup(with: &str) -> (Dir, Admin) {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let dir = Dir(std::env::temp_dir().join(format!(
        "task-approval-{}-{}-{}",
        std::process::id(),
        now_ms(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )));
    let admin = Admin::create_engine(&dir.0, Limits::default(), Engine::V2).unwrap();
    sql(
        &admin,
        "CREATE TABLE shipments (merchant_id bigint, id bigint, status text, \
         failed_at timestamptz, attempt_count int, customer_email text, card_token text, \
         PRIMARY KEY (merchant_id, id))",
    );
    let rows: Vec<String> = (1..=20)
        .map(|id| {
            format!("(42, {id}, 'failed', '2026-11-05 17:03:00+00', 1, 'c{id}@example.com', 'tok_{id}')")
        })
        .collect();
    sql(&admin, &format!("INSERT INTO shipments VALUES {}", rows.join(", ")));
    let roles = admin.roles();
    for (name, task_only) in [(ISSUER, false), (APPROVER, false), (EXECUTOR, true), ("auditor", false)] {
        roles
            .create(NewRole { name: name.into(), login: true, ..NewRole::default() })
            .unwrap();
        if task_only {
            roles.set_task_only(name, true).unwrap();
        }
    }
    for name in [ISSUER, EXECUTOR, APPROVER] {
        roles
            .grant_table("shipments", name, &[TablePrivilege::Select, TablePrivilege::Update])
            .unwrap();
    }
    let text = format!(
        "CREATE TASK \"{TASK}\" \
           EXECUTOR {EXECUTOR} APPROVER {APPROVER} \
           ON shipments \
           VISIBLE ROWS WHERE (merchant_id = 42) \
           READABLE (id, status, failed_at, attempt_count) \
           MASKED (customer_email, card_token) \
           ALLOW UPDATE (status, failed_at) \
             OLD ROW WHERE (status = 'failed' AND failed_at >= '2026-11-05 17:00:00+00' \
                            AND failed_at < '2026-11-05 17:07:00+00') \
             NEW ROW WHERE (status = 'queued') \
             AT MOST 25 DISTINCT ROWS AND 25 TRANSITIONS \
             APPROVE ABOVE 5 \
           DISCLOSE AT MOST 1000 ROWS READ, 2097152 BYTES RETURNED \
           WITH (statement_ms = 2000, rows_scanned = 100000, concurrent_statements = 2{with}) \
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

/// The agent previews requeueing rows 1..=10 (above APPROVE ABOVE 5).
fn stored_preview(admin: &Admin, ctx: &TaskContext) -> PreviewSummary {
    preview_text(
        admin,
        ctx,
        "UPDATE shipments SET status = 'queued' WHERE status = 'failed' AND id >= 1 AND id <= 10",
    )
}

/// The agent previews `text`.
fn preview_text(admin: &Admin, ctx: &TaskContext, text: &str) -> PreviewSummary {
    let mut agent = admin.sql_session();
    agent.bind_task(ctx.clone()).unwrap();
    preview(&mut agent, ctx, Surface::Sql, text, &[], &clock()).unwrap()
}

fn approve_as(
    admin: &Admin,
    ctx: &TaskContext,
    role: &str,
    id: PreviewId,
    digest: [u8; 32],
) -> Result<(), TaskError> {
    approve(&mut admin.sql_session(), &role.to_owned(), &ctx.name, id, digest)
}

fn revoke_as(admin: &Admin, ctx: &TaskContext, role: &str, id: PreviewId) -> Result<(), TaskError> {
    revoke_approval(&mut admin.sql_session(), &role.to_owned(), &ctx.name, id)
}

fn approval(admin: &Admin, ctx: &TaskContext, id: PreviewId) -> Option<StoredApproval> {
    admin.task_approval(ctx.task, id).unwrap()
}

fn assert_no_privilege(result: Result<(), TaskError>) {
    let error = result.expect_err("expected 42501");
    assert_eq!(error.sqlstate(), "42501", "{error:?}");
    assert!(matches!(error, TaskError::Core(_)), "a CoreError, not a task denial: {error:?}");
}

#[test]
fn approve_requires_privilege() {
    let (_dir, admin) = setup("");
    let ctx = context(&admin);
    let p = stored_preview(&admin, &ctx);
    // The issuer holds UNDO and INSPECT, not APPROVE; an unrelated role
    // and a role that does not exist hold nothing.
    for role in [ISSUER, "auditor", "nobody"] {
        assert_no_privilege(approve_as(&admin, &ctx, role, p.id, p.digest));
    }
    // A privilege failure wins over a stale preview (check 1 before 4).
    assert_no_privilege(approve_as(&admin, &ctx, "auditor", PreviewId(99), [0; 32]));
    assert_eq!(approval(&admin, &ctx, p.id), None);
    // An unknown task is 42704.
    let other = giggles_core::task::definition::TaskName::new("no-such-task").unwrap();
    let error = approve(&mut admin.sql_session(), &APPROVER.to_owned(), &other, p.id, p.digest)
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42704");
    // Neither statement runs inside a task session.
    let mut agent = admin.sql_session();
    agent.bind_task(ctx.clone()).unwrap();
    assert_eq!(
        approve(&mut agent, &APPROVER.to_owned(), &ctx.name, p.id, p.digest)
            .unwrap_err()
            .sqlstate(),
        "0A000"
    );
}

#[test]
fn executor_cannot_approve() {
    let (_dir, admin) = setup("");
    let ctx = context(&admin);
    let p = stored_preview(&admin, &ctx);
    assert_no_privilege(approve_as(&admin, &ctx, EXECUTOR, p.id, p.digest));
    assert_eq!(approval(&admin, &ctx, p.id), None);
    // The executor cannot revoke either.
    approve_as(&admin, &ctx, APPROVER, p.id, p.digest).unwrap();
    assert_no_privilege(revoke_as(&admin, &ctx, EXECUTOR, p.id));
    assert_eq!(approval(&admin, &ctx, p.id).unwrap().state, 1);
}

#[test]
fn approve_with_wrong_digest_is_stale() {
    let (_dir, admin) = setup("");
    let ctx = context(&admin);
    let p = stored_preview(&admin, &ctx);
    let mut wrong = p.digest;
    wrong[0] ^= 1;
    assert_eq!(approve_as(&admin, &ctx, APPROVER, p.id, wrong), Err(TaskError::ApprovalStale));
    // A missing preview is stale too.
    assert_eq!(
        approve_as(&admin, &ctx, APPROVER, PreviewId(p.id.0 + 1), p.digest),
        Err(TaskError::ApprovalStale)
    );
    assert_eq!(approval(&admin, &ctx, p.id), None);
    assert_eq!(approve_as(&admin, &ctx, APPROVER, p.id, p.digest), Ok(()));
}

#[test]
fn approve_expired_preview_is_stale() {
    let (_dir, admin) = setup(", preview_ttl_s = 1");
    let ctx = context(&admin);
    let p = stored_preview(&admin, &ctx);
    while now_ms() <= p.expires_at.0 {
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(approve_as(&admin, &ctx, APPROVER, p.id, p.digest), Err(TaskError::ApprovalStale));
    assert_eq!(approval(&admin, &ctx, p.id), None);
}

#[test]
fn approve_is_idempotent_for_same_role_and_digest() {
    let (_dir, admin) = setup("");
    let ctx = context(&admin);
    let p = stored_preview(&admin, &ctx);
    let started = now_ms() as u64;
    approve_as(&admin, &ctx, APPROVER, p.id, p.digest).unwrap();
    let first = approval(&admin, &ctx, p.id).expect("approval");
    let oid = admin.roles().get(APPROVER).unwrap().unwrap().oid;
    assert_eq!(
        (first.state, first.approver.as_str(), first.approver_oid, first.digest, first.consumed_seq),
        (1, APPROVER, oid, p.digest, 0)
    );
    assert!(first.approved_ms >= started);
    std::thread::sleep(Duration::from_millis(5));
    approve_as(&admin, &ctx, APPROVER, p.id, p.digest).unwrap();
    assert_eq!(approval(&admin, &ctx, p.id), Some(first), "the second APPROVE writes nothing");
}

#[test]
fn revoke_then_reapprove() {
    let (_dir, admin) = setup("");
    let ctx = context(&admin);
    let p = stored_preview(&admin, &ctx);
    // Nothing to revoke yet.
    assert_eq!(revoke_as(&admin, &ctx, APPROVER, p.id), Err(TaskError::ApprovalStale));
    approve_as(&admin, &ctx, APPROVER, p.id, p.digest).unwrap();
    // An unrelated role may not revoke.
    assert_no_privilege(revoke_as(&admin, &ctx, "auditor", p.id));
    // The approving role revokes; revoking again is a no-op.
    revoke_as(&admin, &ctx, APPROVER, p.id).unwrap();
    assert_eq!(approval(&admin, &ctx, p.id).unwrap().state, 2);
    revoke_as(&admin, &ctx, APPROVER, p.id).unwrap();
    assert_eq!(approval(&admin, &ctx, p.id).unwrap().state, 2);
    // APPROVE sets it active again.
    approve_as(&admin, &ctx, APPROVER, p.id, p.digest).unwrap();
    let active = approval(&admin, &ctx, p.id).unwrap();
    assert_eq!((active.state, active.digest), (1, p.digest));
    // The issuer may revoke too.
    revoke_as(&admin, &ctx, ISSUER, p.id).unwrap();
    assert_eq!(approval(&admin, &ctx, p.id).unwrap().state, 2);
}

#[test]
fn consumed_approval_cannot_be_revoked() {
    let (_dir, admin) = setup("");
    let ctx = context(&admin);
    let p = stored_preview(&admin, &ctx);
    approve_as(&admin, &ctx, APPROVER, p.id, p.digest).unwrap();
    // What apply's stage step does (D7.6 step 6), until R3-706 lands.
    admin.consume_task_approval(ctx.task, p.id, 7).unwrap();
    assert_eq!(revoke_as(&admin, &ctx, APPROVER, p.id), Err(TaskError::ApprovalStale));
    assert_eq!(revoke_as(&admin, &ctx, ISSUER, p.id), Err(TaskError::ApprovalStale));
    assert_eq!(approve_as(&admin, &ctx, APPROVER, p.id, p.digest), Err(TaskError::ApprovalStale));
    let consumed = approval(&admin, &ctx, p.id).unwrap();
    assert_eq!((consumed.state, consumed.consumed_seq), (3, 7));
}

#[test]
fn approve_undo_preview_through_approve_change_is_stale() {
    let (_dir, admin) = setup("");
    let ctx = context(&admin);
    let p = stored_preview(&admin, &ctx);
    // A kind 02 header with the same digest, until the undo preview
    // (R3-802) can store one.
    admin.mark_task_preview_undo(ctx.task, p.id).unwrap();
    let stored = admin.task_previews(ctx.task).unwrap();
    assert_eq!((stored.previews[0].kind, stored.previews[0].digest), (2, p.digest));
    assert_eq!(approve_as(&admin, &ctx, APPROVER, p.id, p.digest), Err(TaskError::ApprovalStale));
    assert_eq!(approval(&admin, &ctx, p.id), None);
}


// ---- R3-705: SHOW CHANGES and SHOW CHANGE ----

/// Milliseconds from the Unix epoch to the PostgreSQL epoch.
const PG_EPOCH_UNIX_MS: i64 = 946_684_800_000;

type Line = (i64, String, Option<String>, Option<String>, Option<String>);
type Summary = (i64, String, String, String, i64, Option<String>);

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn opt_text(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Text(s) => Some(s.clone()),
        other => panic!("not text: {other:?}"),
    }
}

/// SHOW CHANGE as `role`, released through the session (a plain move).
fn show_as(
    admin: &Admin,
    ctx: &TaskContext,
    role: &str,
    id: PreviewId,
) -> Result<SqlResult, TaskError> {
    let mut session = admin.sql_session();
    let page = show_change(&mut session, &role.to_owned(), &ctx.name, id)?;
    assert!(!page.is_task());
    Ok(session.release_page(page).unwrap().into_inner())
}

/// SHOW CHANGES as `role`.
fn list_as(admin: &Admin, ctx: &TaskContext, role: &str) -> Result<SqlResult, TaskError> {
    let mut session = admin.sql_session();
    let page = list_changes(&mut session, &role.to_owned(), &ctx.name)?;
    assert!(!page.is_task());
    Ok(session.release_page(page).unwrap().into_inner())
}

fn lines(result: &SqlResult) -> Vec<Line> {
    assert_eq!(result.columns, CHANGE_COLUMNS);
    result
        .rows
        .iter()
        .map(|row| {
            let Value::Int(row_no) = row.values[0] else {
                panic!("row_no: {:?}", row.values[0]);
            };
            (
                row_no,
                opt_text(&row.values[1]).expect("key"),
                opt_text(&row.values[2]),
                opt_text(&row.values[3]),
                opt_text(&row.values[4]),
            )
        })
        .collect()
}

fn summary(result: &SqlResult) -> Vec<Summary> {
    assert_eq!(result.columns, CHANGES_COLUMNS);
    result
        .rows
        .iter()
        .map(|row| {
            let (Value::Int(id), Value::Int(count)) = (&row.values[0], &row.values[4]) else {
                panic!("{row:?}");
            };
            (
                *id,
                opt_text(&row.values[1]).unwrap(),
                opt_text(&row.values[2]).unwrap(),
                opt_text(&row.values[3]).unwrap(),
                *count,
                opt_text(&row.values[7]),
            )
        })
        .collect()
}

fn line(row_no: i64, key: &str, column: &str, old: Option<&str>, new: &str) -> Line {
    (row_no, key.into(), Some(column.into()), old.map(Into::into), Some(new.into()))
}

fn key(id: i64) -> String {
    format!("(merchant_id, id)=(42, {id})")
}

fn assert_42501<T: std::fmt::Debug>(result: Result<T, TaskError>) {
    let error = result.expect_err("expected 42501");
    assert_eq!(error.sqlstate(), "42501", "{error:?}");
}

#[test]
fn show_change_lists_one_line_per_changed_column() {
    let (_dir, admin) = setup("");
    let ctx = context(&admin);
    // One changed column per row.
    let p = stored_preview(&admin, &ctx);
    let shown = lines(&show_as(&admin, &ctx, APPROVER, p.id).unwrap());
    let expected: Vec<Line> = (1..=10)
        .map(|id| line(id, &key(id), "status", Some("failed"), "queued"))
        .collect();
    assert_eq!(shown, expected);
    // Two changed columns per row, in column order; a timestamptz in
    // PostgreSQL's text output.
    let two = preview_text(
        &admin,
        &ctx,
        "UPDATE shipments SET status = 'queued', failed_at = '2026-11-05 17:05:00+00' \
         WHERE id >= 11 AND id <= 12",
    );
    let shown = lines(&show_as(&admin, &ctx, APPROVER, two.id).unwrap());
    let mut expected = Vec::new();
    for (row_no, id) in [(1, 11), (2, 12)] {
        expected.push(line(row_no, &key(id), "status", Some("failed"), "queued"));
        expected.push(line(
            row_no,
            &key(id),
            "failed_at",
            Some("2026-11-05 17:03:00+00"),
            "2026-11-05 17:05:00+00",
        ));
    }
    assert_eq!(shown, expected);
    // An unknown change is 42704, and SHOW CHANGE never runs in a task session.
    let unknown = show_as(&admin, &ctx, APPROVER, PreviewId(99)).unwrap_err();
    assert_eq!(unknown.sqlstate(), "42704");
    let mut agent = admin.sql_session();
    agent.bind_task(ctx.clone()).unwrap();
    let refused = show_change(&mut agent, &APPROVER.to_owned(), &ctx.name, p.id).unwrap_err();
    assert_eq!(refused.sqlstate(), "0A000");
}

#[test]
fn show_change_without_select_is_42501() {
    let (_dir, admin) = setup("");
    let ctx = context(&admin);
    let p = stored_preview(&admin, &ctx);
    let roles = admin.roles();
    // Without a task privilege: the executor and an unrelated role.
    roles.grant_table("shipments", "auditor", &[TablePrivilege::Select]).unwrap();
    assert_42501(show_as(&admin, &ctx, EXECUTOR, p.id));
    assert_42501(show_as(&admin, &ctx, "auditor", p.id));
    assert_42501(list_as(&admin, &ctx, "auditor"));
    // INSPECT and SELECT together are enough.
    let issuer = roles.get(ISSUER).unwrap().unwrap();
    admin.tasks().grant(&issuer, TASK, "auditor", TaskPrivileges::INSPECT, true).unwrap();
    assert_eq!(lines(&show_as(&admin, &ctx, "auditor", p.id).unwrap()).len(), 10);
    // The approver without SELECT on the table: SHOW CHANGE is 42501, while
    // SHOW CHANGES (no row values) still works.
    roles.revoke_table("shipments", APPROVER, &[TablePrivilege::Select]).unwrap();
    assert_42501(show_as(&admin, &ctx, APPROVER, p.id));
    assert_eq!(list_as(&admin, &ctx, APPROVER).unwrap().rows.len(), 1);
    // The table owner needs no grant.
    roles.revoke_table("shipments", "auditor", &[TablePrivilege::Select]).unwrap();
    assert_42501(show_as(&admin, &ctx, "auditor", p.id));
    roles.set_table_owner("shipments", "auditor").unwrap();
    assert_eq!(lines(&show_as(&admin, &ctx, "auditor", p.id).unwrap()).len(), 10);
}

#[test]
fn show_change_ignores_task_column_modes() {
    let (_dir, admin) = setup("");
    let ctx = context(&admin);
    let p = stored_preview(&admin, &ctx);
    // The executor cannot see merchant_id (not READABLE) and sees the
    // MASKED columns masked; the approver sees both in clear.
    sql(
        &admin,
        "UPDATE shipments SET customer_email = 'new1@example.com' WHERE merchant_id = 42 AND id = 1",
    );
    let shown = lines(&show_as(&admin, &ctx, APPROVER, p.id).unwrap());
    assert!(shown.iter().all(|l| l.1.starts_with("(merchant_id, id)=(42, ")), "{shown:?}");
    let changed = format!("{}{CHANGED_SINCE_PREVIEW}", key(1));
    let row1: Vec<&Line> = shown.iter().filter(|l| l.0 == 1).collect();
    let columns: Vec<&str> = row1.iter().map(|l| l.2.as_deref().unwrap()).collect();
    assert_eq!(
        columns,
        ["merchant_id", "id", "status", "failed_at", "attempt_count", "customer_email", "card_token"]
    );
    assert!(row1.iter().all(|l| l.1 == changed && l.3.is_none()), "{row1:?}");
    // The after-image as previewed, with no mask applied.
    assert_eq!(row1[0].4.as_deref(), Some("42"));
    assert_eq!(row1[5].4.as_deref(), Some("c1@example.com"));
    assert_eq!(row1[6].4.as_deref(), Some("tok_1"));
}

#[test]
fn show_change_marks_rows_changed_since_preview() {
    let (_dir, admin) = setup("");
    let ctx = context(&admin);
    let p = stored_preview(&admin, &ctx);
    sql(&admin, "UPDATE shipments SET attempt_count = 2 WHERE merchant_id = 42 AND id = 3");
    sql(&admin, "DELETE FROM shipments WHERE merchant_id = 42 AND id = 4");
    let shown = lines(&show_as(&admin, &ctx, APPROVER, p.id).unwrap());
    for id in [1, 2, 5, 6, 7, 8, 9, 10] {
        let row: Vec<&Line> = shown.iter().filter(|l| l.0 == id).collect();
        assert_eq!(row, [&line(id, &key(id), "status", Some("failed"), "queued")]);
    }
    for id in [3, 4] {
        let row: Vec<&Line> = shown.iter().filter(|l| l.0 == id).collect();
        assert_eq!(row.len(), 7, "every column of the previewed row: {row:?}");
        let marked = format!("{}{CHANGED_SINCE_PREVIEW}", key(id));
        assert!(row.iter().all(|l| l.1 == marked && l.3.is_none()), "{row:?}");
        // The new values are the previewed ones, not the current row's.
        assert_eq!(row[2].2.as_deref(), Some("status"));
        assert_eq!(row[2].4.as_deref(), Some("queued"));
        assert_eq!(row[4].2.as_deref(), Some("attempt_count"));
        assert_eq!(row[4].4.as_deref(), Some("1"));
    }
    assert_eq!(shown.len(), 8 + 2 * 7);
}

#[test]
fn show_changes_lists_digest_state_and_approver() {
    let (_dir, admin) = setup("");
    let ctx = context(&admin);
    let started = now_ms();
    let p = stored_preview(&admin, &ctx);
    let q = preview_text(
        &admin,
        &ctx,
        "UPDATE shipments SET status = 'queued' WHERE id >= 11 AND id <= 12",
    );
    approve_as(&admin, &ctx, APPROVER, p.id, p.digest).unwrap();
    let us = |ms: i64| (ms - PG_EPOCH_UNIX_MS) * 1000;
    let shown = list_as(&admin, &ctx, ISSUER).unwrap();
    let approved: Summary =
        (p.id.0 as i64, "change".into(), "approved".into(), hex(&p.digest), 10, Some(APPROVER.into()));
    let open: Summary = (q.id.0 as i64, "change".into(), "open".into(), hex(&q.digest), 2, None);
    assert_eq!(summary(&shown), [approved, open]);
    for (row, preview) in shown.rows.iter().zip([&p, &q]) {
        let (Value::TimestampTz(created), Value::TimestampTz(expires)) =
            (&row.values[5], &row.values[6])
        else {
            panic!("{row:?}");
        };
        assert_eq!(expires.microseconds(), us(preview.expires_at.0));
        assert!(created.microseconds() >= us(started) && created.microseconds() <= us(now_ms()));
    }
    // A revoked approval is no approval; a consumed one still names its approver.
    revoke_as(&admin, &ctx, APPROVER, p.id).unwrap();
    let shown = summary(&list_as(&admin, &ctx, APPROVER).unwrap());
    assert_eq!((shown[0].2.as_str(), shown[0].5.as_deref()), ("open", None));
    approve_as(&admin, &ctx, APPROVER, p.id, p.digest).unwrap();
    admin.consume_task_approval(ctx.task, p.id, 9).unwrap();
    admin.mark_task_preview_undo(ctx.task, q.id).unwrap();
    let shown = summary(&list_as(&admin, &ctx, APPROVER).unwrap());
    assert_eq!(shown[0].5.as_deref(), Some(APPROVER));
    assert_eq!(shown[1].1, "undo");
    // SHOW CHANGE shows change previews only.
    let undo = show_as(&admin, &ctx, APPROVER, q.id).unwrap_err();
    assert_eq!(undo.sqlstate(), "42704");
    // The executor holds no task privilege.
    assert_42501(list_as(&admin, &ctx, EXECUTOR));
}
