//! GD-0061 R3-704: APPROVE and REVOKE APPROVAL (design-7 D7.5), through the
//! public API, on previews stored by `task::preview`.
//!
//! `deliveries` is reserved, so the table is `shipments`, as in
//! `task_preview.rs`. The task names `oncall_lead` as its approver,
//! `incident_controller` issues it and `repair_agent` executes it.
#![cfg(feature = "engine-rocksdb")]
use giggles_core::relational::StatementClock;
use giggles_core::roles::{NewRole, TablePrivilege};
use giggles_core::task::approval::StoredApproval;
use giggles_core::task::context::TaskContext;
use giggles_core::task::definition::WallClockMs;
use giggles_core::task::statements::{parse, TaskStatement};
use giggles_core::task::{
    approve, preview, revoke_approval, PreviewId, PreviewSummary, Surface, TaskError,
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
    let mut agent = admin.sql_session();
    agent.bind_task(ctx.clone()).unwrap();
    preview(
        &mut agent,
        ctx,
        Surface::Sql,
        "UPDATE shipments SET status = 'queued' WHERE status = 'failed' AND id >= 1 AND id <= 10",
        &[],
        &clock(),
    )
    .unwrap()
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

