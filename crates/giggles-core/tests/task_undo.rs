//! GD-0061 R3-802: PREVIEW UNDO, the dry run and the kind-02 store, and
//! R3-803: APPROVE UNDO, the undo commit (design-7 D8), through the public
//! API.
//!
//! `deliveries` is reserved, so the table is `shipments`. Merchant 42's
//! failed rows 1-10 are the task's; row 100 belongs to merchant 43. Each row
//! has a carrier (`carriers`, foreign key) and a unique tracking code.
//! `incident_controller` issues the task (and so holds the undo privilege),
//! `oncall_lead` approves and `repair_agent` executes.
#![cfg(feature = "engine-rocksdb")]
use giggles_core::relational::Value;
use giggles_core::roles::{NewRole, TablePrivilege};
use giggles_core::task::context::TaskContext;
use giggles_core::task::definition::WallClockMs;
use giggles_core::task::statements::{parse, TaskStatement};
use giggles_core::task::{
    outcome, protected_commit, undo_approve, undo_operation_id, undo_preview, Actor,
    ConflictReason, DenyRule, OperationId, Outcome, ProtectedRequest, Receipt, RetentionConfig,
    TaskError, UndoMode, UndoScope, UndoSummary,
};
use giggles_core::{Admin, Engine, Limits};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const TASK: &str = "inc-802";
const DAY_MS: i64 = 86_400_000;
const ISSUER: &str = "incident_controller";
const APPROVER: &str = "oncall_lead";
const EXECUTOR: &str = "repair_agent";

/// Stored row classes and the undo header kind.
const RESTORE: u8 = 0;
const CONFLICT: u8 = 1;
const SUPERSEDED: u8 = 2;
const UNDO_KIND: u8 = 2;

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

/// Every row of both tables, in key order.
fn table_contents(admin: &Admin) -> Vec<Vec<Value>> {
    let mut rows = sql(admin, "SELECT * FROM shipments ORDER BY id");
    rows.extend(sql(admin, "SELECT * FROM carriers ORDER BY id"));
    rows
}

fn setup() -> (Dir, Admin) {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let dir = Dir(std::env::temp_dir().join(format!(
        "task-undo-{}-{}-{}",
        std::process::id(),
        now_ms(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )));
    let admin = Admin::create_engine(&dir.0, Limits::default(), Engine::V2).unwrap();
    sql(&admin, "CREATE TABLE carriers (id bigint primary key)");
    sql(&admin, "INSERT INTO carriers VALUES (1), (2), (3)");
    sql(
        &admin,
        "CREATE TABLE shipments (id bigint primary key, merchant_id bigint, status text, \
         failed_at timestamptz, attempt_count int, customer_email text, \
         carrier_id bigint REFERENCES carriers(id), tracking text UNIQUE)",
    );
    let mut rows: Vec<String> = (1..=10)
        .map(|id| {
            let carrier = if id == 1 { 1 } else { 2 };
            format!(
                "({id}, 42, 'failed', '2026-11-05 17:03:00+00', 1, 'c{id}@example.com', \
                 {carrier}, 'T{id}')"
            )
        })
        .collect();
    rows.push("(100, 43, 'failed', '2026-11-05 17:03:00+00', 1, 'o@example.com', 2, 'T100')".into());
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
    for name in [ISSUER, EXECUTOR, "auditor"] {
        roles
            .grant_table("shipments", name, &[TablePrivilege::Select, TablePrivilege::Update])
            .unwrap();
    }
    let text = format!(
        "CREATE TASK \"{TASK}\" \
           EXECUTOR {EXECUTOR} APPROVER {APPROVER} \
           ON shipments \
           VISIBLE ROWS WHERE (merchant_id = 42) \
           READABLE (id, status, failed_at, attempt_count, carrier_id, tracking) \
           MASKED (customer_email) \
           ALLOW UPDATE (status, carrier_id, tracking) \
             OLD ROW WHERE (status = 'failed') \
             NEW ROW WHERE (status = 'queued') \
             AT MOST 50 DISTINCT ROWS AND 50 TRANSITIONS \
             APPROVE ABOVE 50 \
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

/// `OPERATION '<id>' <text>`, a direct protected write by the executor.
fn direct(admin: &Admin, ctx: &TaskContext, id: &str, text: &str) -> Result<Receipt, TaskError> {
    let mut session = admin.sql_session();
    let request = ProtectedRequest::Direct {
        statement: session.prepare(text).unwrap_or_else(|e| panic!("{text}: {e:?}")),
        params: Vec::new(),
        text: Arc::from(text),
    };
    protected_commit(&mut session, Actor::Executor(ctx), &op(id), request)
}

fn commit(admin: &Admin, ctx: &TaskContext, id: &str, text: &str) -> Receipt {
    direct(admin, ctx, id, text).unwrap_or_else(|e| panic!("{text}: {e:?}"))
}

/// Requeue rows `from..=to`.
fn requeue(from: i64, to: i64) -> String {
    format!("UPDATE shipments SET status = 'queued' WHERE status = 'failed' AND id >= {from} AND id <= {to}")
}

fn undo_as(admin: &Admin, ctx: &TaskContext, role: &str, scope: UndoScope) -> Result<UndoSummary, TaskError> {
    undo_preview(&mut admin.sql_session(), &role.to_owned(), &ctx.name, scope)
}

fn undo(admin: &Admin, ctx: &TaskContext, scope: UndoScope) -> UndoSummary {
    undo_as(admin, ctx, ISSUER, scope).unwrap_or_else(|e| panic!("undo preview: {e:?}"))
}

/// The stored undo preview's rows as `(class, aux, id from the after-values)`,
/// in stored order (ascending class, then table and key).
fn stored(admin: &Admin, ctx: &TaskContext, summary: &UndoSummary) -> Vec<(u8, u64, Option<Value>)> {
    let store = admin.task_previews(ctx.task).unwrap();
    let header = store.previews.iter().find(|p| p.id == summary.id).expect("stored undo preview");
    assert_eq!(header.kind, UNDO_KIND);
    assert_eq!(header.state, 0, "open");
    assert_eq!(header.digest, summary.digest);
    assert_eq!(
        header.counts,
        [summary.restorable, summary.conflicts, summary.superseded, summary.settled]
    );
    assert_eq!(header.creator, ISSUER);
    assert!(header.text.is_empty() && header.params.is_empty() && header.clock_pin.is_none());
    header
        .rows
        .iter()
        .map(|row| (row.class, row.aux, row.after.first().cloned()))
        .collect()
}

fn sqlstate(result: Result<UndoSummary, TaskError>) -> &'static str {
    result.expect_err("undo preview refused").sqlstate()
}

#[test]
fn undo_preview_lists_restorable_conflicts_superseded() {
    let (_dir, admin) = setup();
    let ctx = context(&admin);
    // Rows 1-3 and 5-6 requeued by the task.
    commit(&admin, &ctx, "op-1", &requeue(1, 3));
    commit(&admin, &ctx, "op-2", &requeue(5, 6));
    // Row 4: the task requeues it, the application fails it again, the task
    // requeues it once more. The first write is superseded; the second is
    // restorable to the application's values.
    commit(&admin, &ctx, "op-3", &requeue(4, 4));
    sql(&admin, "UPDATE shipments SET status = 'failed', attempt_count = 7 WHERE id = 4");
    commit(&admin, &ctx, "op-4", &requeue(4, 4));
    // Row 2 changed by the application since; row 6 deleted.
    sql(&admin, "UPDATE shipments SET attempt_count = 9 WHERE id = 2");
    sql(&admin, "DELETE FROM shipments WHERE id = 6");
    let summary = undo(&admin, &ctx, UndoScope::Task);
    assert_eq!(
        (summary.restorable, summary.conflicts, summary.superseded, summary.settled),
        (4, 2, 1, 0)
    );
    let rows = stored(&admin, &ctx, &summary);
    let restores: Vec<_> = rows.iter().filter(|r| r.0 == RESTORE).collect();
    assert_eq!(
        restores.iter().map(|r| r.2.clone().unwrap()).collect::<Vec<_>>(),
        vec![Value::Int(1), Value::Int(3), Value::Int(4), Value::Int(5)]
    );
    let conflicts: Vec<u64> = rows.iter().filter(|r| r.0 == CONFLICT).map(|r| r.1).collect();
    assert_eq!(
        conflicts,
        vec![ConflictReason::ChangedSince as u64, ConflictReason::RowMissing as u64]
    );
    let superseded: Vec<_> = rows.iter().filter(|r| r.0 == SUPERSEDED).collect();
    assert_eq!(superseded.len(), 1);
    assert!(superseded[0].2.is_none());
    // The restore of row 4 brings back the application's values, not the
    // original ones; the restore of row 1 brings back 'failed'.
    let store = admin.task_previews(ctx.task).unwrap();
    let header = store.previews.iter().find(|p| p.id == summary.id).unwrap();
    let after = |id: i64| {
        header
            .rows
            .iter()
            .find(|r| r.class == RESTORE && r.after.first() == Some(&Value::Int(id)))
            .map(|r| r.after.clone())
            .unwrap()
    };
    assert_eq!(after(1)[2], Value::Text("failed".into()));
    assert_eq!(after(4)[2], Value::Text("failed".into()));
    assert_eq!(after(4)[4], Value::Int(7));
    // The superseded row records the op-3 write's seq.
    let op3 = header.rows.iter().find(|r| r.class == SUPERSEDED).unwrap();
    assert!(op3.aux > 0);
    // Operation scope sees only that operation's rows.
    let one = undo(&admin, &ctx, UndoScope::Operation(op("op-2")));
    assert_eq!((one.restorable, one.conflicts, one.superseded, one.settled), (1, 1, 0, 0));
    assert_ne!(one.id, summary.id);
    let scoped = admin.task_previews(ctx.task).unwrap();
    let header = scoped.previews.iter().find(|p| p.id == one.id).unwrap();
    assert_eq!(header.rows.len(), 2);
}

#[test]
fn restore_breaking_foreign_key_is_constraint_conflict() {
    let (_dir, admin) = setup();
    let ctx = context(&admin);
    // Rows 1 and 2 move to carrier 3; carrier 1 is then no longer used and
    // the application deletes it. Restoring row 1 would point at it again.
    commit(
        &admin,
        &ctx,
        "op-1",
        "UPDATE shipments SET status = 'queued', carrier_id = 3 WHERE status = 'failed' AND id >= 1 AND id <= 2",
    );
    sql(&admin, "DELETE FROM carriers WHERE id = 1");
    let before = table_contents(&admin);
    let summary = undo(&admin, &ctx, UndoScope::Task);
    assert_eq!(
        (summary.restorable, summary.conflicts, summary.superseded, summary.settled),
        (1, 1, 0, 0)
    );
    let rows = stored(&admin, &ctx, &summary);
    assert_eq!(rows[0], (RESTORE, 0, Some(Value::Int(2))));
    assert_eq!((rows[1].0, rows[1].1, rows[1].2.clone()), (CONFLICT, ConflictReason::Constraint as u64, None));
    assert_eq!(table_contents(&admin), before, "the dry run wrote nothing");
}

#[test]
fn restore_breaking_unique_is_constraint_conflict() {
    let (_dir, admin) = setup();
    let ctx = context(&admin);
    // Row 1's tracking code changes; the application then reuses the old
    // code for a new row, so restoring row 1 would duplicate it.
    commit(
        &admin,
        &ctx,
        "op-1",
        "UPDATE shipments SET status = 'queued', tracking = 'N1' WHERE status = 'failed' AND id = 1",
    );
    commit(&admin, &ctx, "op-2", &requeue(2, 2));
    sql(
        &admin,
        "INSERT INTO shipments VALUES (200, 43, 'failed', NULL, 0, 'n@example.com', 2, 'T1')",
    );
    let before = table_contents(&admin);
    let summary = undo(&admin, &ctx, UndoScope::Task);
    assert_eq!(
        (summary.restorable, summary.conflicts, summary.superseded, summary.settled),
        (1, 1, 0, 0)
    );
    let rows = stored(&admin, &ctx, &summary);
    assert_eq!(rows[0], (RESTORE, 0, Some(Value::Int(2))));
    assert_eq!((rows[1].0, rows[1].1), (CONFLICT, ConflictReason::Constraint as u64));
    assert_eq!(table_contents(&admin), before, "the dry run wrote nothing");
}

#[test]
fn undo_preview_writes_no_user_rows() {
    let (_dir, admin) = setup();
    let ctx = context(&admin);
    commit(&admin, &ctx, "op-1", &requeue(1, 5));
    let before = table_contents(&admin);
    let previews_before = admin.task_previews(ctx.task).unwrap();
    let summary = undo(&admin, &ctx, UndoScope::Task);
    assert_eq!(summary.restorable, 5);
    assert_eq!(table_contents(&admin), before, "no user row changed");
    let after = admin.task_previews(ctx.task).unwrap();
    assert_eq!(after.next_preview, previews_before.next_preview + 1);
    assert_eq!(after.stored_previews, previews_before.stored_previews + 1);
    assert_eq!(after.stored_rows, 5);
    // A second preview sees the same rows at the same versions: the first
    // left nothing behind (no new versions, no locks).
    let again = undo(&admin, &ctx, UndoScope::Task);
    assert_eq!(again.restorable, 5);
    assert_ne!(again.digest, summary.digest, "a new id and expiry");
    // The application can still write a row the dry run restored: no lock
    // or version was left behind.
    sql(&admin, "UPDATE shipments SET attempt_count = 5 WHERE id = 3");
    // The stored-preview limit counts undo previews too (default 8 open).
    for _ in 2..8 {
        undo(&admin, &ctx, UndoScope::Task);
    }
    assert_eq!(sqlstate(undo_as(&admin, &ctx, ISSUER, UndoScope::Task)), "54T01");
}

#[test]
fn undo_preview_requires_undo_privilege_and_update() {
    let (_dir, admin) = setup();
    let ctx = context(&admin);
    commit(&admin, &ctx, "op-1", &requeue(1, 2));
    // No undo privilege: a role with table privileges only, and the executor.
    assert_eq!(sqlstate(undo_as(&admin, &ctx, "auditor", UndoScope::Task)), "42501");
    assert_eq!(sqlstate(undo_as(&admin, &ctx, EXECUTOR, UndoScope::Task)), "42501");
    assert_eq!(sqlstate(undo_as(&admin, &ctx, "nobody", UndoScope::Task)), "42501");
    // The approver holds the undo privilege but needs SELECT and UPDATE.
    assert_eq!(sqlstate(undo_as(&admin, &ctx, APPROVER, UndoScope::Task)), "42501");
    admin.roles().grant_table("shipments", APPROVER, &[TablePrivilege::Select]).unwrap();
    assert_eq!(sqlstate(undo_as(&admin, &ctx, APPROVER, UndoScope::Task)), "42501");
    admin.roles().grant_table("shipments", APPROVER, &[TablePrivilege::Update]).unwrap();
    let summary = undo_as(&admin, &ctx, APPROVER, UndoScope::Task).unwrap();
    assert_eq!(summary.restorable, 2);
    // The issuer holds the undo privilege by default.
    assert_eq!(undo(&admin, &ctx, UndoScope::Task).restorable, 2);
    // Nothing refused was stored.
    assert_eq!(admin.task_previews(ctx.task).unwrap().previews.len(), 2);
    // Not inside a task session.
    let mut agent = admin.sql_session();
    agent.bind_task(ctx.clone()).unwrap();
    let refused = undo_preview(&mut agent, &ISSUER.to_owned(), &ctx.name, UndoScope::Task);
    assert_eq!(sqlstate(refused), "0A000");
}

#[test]
fn undo_preview_of_unknown_operation_is_error() {
    let (_dir, admin) = setup();
    let ctx = context(&admin);
    commit(&admin, &ctx, "op-1", &requeue(1, 2));
    // Never used.
    assert_eq!(sqlstate(undo_as(&admin, &ctx, ISSUER, UndoScope::Operation(op("nope")))), "42704");
    // A recorded denial has no committed outcome to undo.
    let denied = direct(
        &admin,
        &ctx,
        "op-denied",
        "UPDATE shipments SET status = 'delivered' WHERE status = 'failed' AND id = 3",
    );
    assert_eq!(denied.unwrap_err().sqlstate(), "42501");
    assert_eq!(
        sqlstate(undo_as(&admin, &ctx, ISSUER, UndoScope::Operation(op("op-denied")))),
        "42704"
    );
    // An unknown task.
    let mut other = ctx.clone();
    other.name = giggles_core::task::definition::TaskName::new("no-such-task").unwrap();
    assert_eq!(sqlstate(undo_as(&admin, &other, ISSUER, UndoScope::Task)), "42704");
    assert!(admin.task_previews(ctx.task).unwrap().previews.is_empty());
    // The committed operation resolves.
    let summary = undo(&admin, &ctx, UndoScope::Operation(op("op-1")));
    assert_eq!(summary.restorable, 2);
}

#[test]
fn undo_preview_after_retention_reports_expired() {
    let (_dir, admin) = setup();
    let ctx = context(&admin);
    commit(&admin, &ctx, "op-1", &requeue(1, 2));
    assert_eq!(undo(&admin, &ctx, UndoScope::Task).restorable, 2);
    // Before-values older than the window are blanked by the sweeper.
    admin
        .set_task_retention(RetentionConfig { before_values_ms: 0, ..RetentionConfig::default() })
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));
    let stats = admin.task_retention_step().unwrap();
    assert_eq!(stats.rows_blanked, 2);
    let summary = undo(&admin, &ctx, UndoScope::Task);
    assert_eq!(
        (summary.restorable, summary.conflicts, summary.superseded, summary.settled),
        (0, 2, 0, 0)
    );
    let rows = stored(&admin, &ctx, &summary);
    assert!(rows
        .iter()
        .all(|r| (r.0, r.1) == (CONFLICT, ConflictReason::BeforeValuesExpired as u64)));
}

// ---------------------------------------------------------------- R3-803

/// Receipt decisions of an undo.
const UNDO_COMPLETE: u8 = 2;
const UNDO_PARTIAL: u8 = 3;
/// Preview header states.
const OPEN: u8 = 0;
const APPLIED: u8 = 1;

/// APPROVE UNDO of `summary` as `role`.
fn approve_as(
    admin: &Admin,
    ctx: &TaskContext,
    role: &str,
    summary: &UndoSummary,
    mode: UndoMode,
) -> Result<Receipt, TaskError> {
    undo_approve(
        &mut admin.sql_session(),
        &role.to_owned(),
        &ctx.name,
        summary.id,
        summary.digest,
        mode,
    )
}

fn approve(admin: &Admin, ctx: &TaskContext, summary: &UndoSummary, mode: UndoMode) -> Result<Receipt, TaskError> {
    approve_as(admin, ctx, ISSUER, summary, mode)
}

fn statuses(admin: &Admin) -> Vec<Vec<Value>> {
    sql(admin, "SELECT id, status, attempt_count FROM shipments ORDER BY id")
}

fn status(admin: &Admin, id: i64) -> Value {
    sql(admin, &format!("SELECT status FROM shipments WHERE id = {id}"))[0][0].clone()
}

fn failed() -> Value {
    Value::Text("failed".into())
}

fn queued_value() -> Value {
    Value::Text("queued".into())
}

/// The stored state of preview `summary` (0 open, 1 applied) and its row count.
fn preview_state(admin: &Admin, ctx: &TaskContext, summary: &UndoSummary) -> (u8, usize) {
    let store = admin.task_previews(ctx.task).unwrap();
    let header = store.previews.iter().find(|p| p.id == summary.id).expect("preview");
    (header.state, header.rows.len())
}

fn counts(summary: &UndoSummary) -> (u32, u32, u32, u32) {
    (summary.restorable, summary.conflicts, summary.superseded, summary.settled)
}

/// Rows 1-3 requeued by the task, then row 2 changed by the application:
/// two restorable rows and one `ChangedSince` conflict.
fn with_conflict(admin: &Admin, ctx: &TaskContext) -> UndoSummary {
    commit(admin, ctx, "op-1", &requeue(1, 3));
    sql(admin, "UPDATE shipments SET attempt_count = 9 WHERE id = 2");
    let summary = undo(admin, ctx, UndoScope::Task);
    assert_eq!(counts(&summary), (2, 1, 0, 0));
    summary
}

#[test]
fn complete_undo_refuses_with_conflicts_and_changes_nothing() {
    let (_dir, admin) = setup();
    let ctx = context(&admin);
    let summary = with_conflict(&admin, &ctx);
    let before = table_contents(&admin);
    let refused = approve(&admin, &ctx, &summary, UndoMode::Complete).unwrap_err();
    assert_eq!(refused, TaskError::Denied(DenyRule::UndoHasConflicts));
    assert_eq!(refused.sqlstate(), "42501");
    assert_eq!(table_contents(&admin), before, "no row changed");
    assert_eq!(preview_state(&admin, &ctx, &summary), (OPEN, 3), "the preview stays open");
    // Not recorded: the same request is checked again, not replayed.
    assert_eq!(
        approve(&admin, &ctx, &summary, UndoMode::Complete),
        Err(TaskError::Denied(DenyRule::UndoHasConflicts))
    );
    // No receipt seq was taken: the next write is seq 2.
    assert_eq!(commit(&admin, &ctx, "op-2", &requeue(5, 5)).seq, 2);
}

#[test]
fn partial_undo_restores_exactly_restorable_rows() {
    let (_dir, admin) = setup();
    let ctx = context(&admin);
    let summary = with_conflict(&admin, &ctx);
    let before = statuses(&admin);
    let receipt = approve(&admin, &ctx, &summary, UndoMode::Partial).unwrap();
    // Rows 1 and 3 are back to their values before the task; row 2 keeps
    // the task's and the application's changes; nothing else moved.
    let after = statuses(&admin);
    for (old, new) in before.iter().zip(&after) {
        match &old[0] {
            Value::Int(1) | Value::Int(3) => {
                assert_eq!(new[1], failed());
                assert_eq!(new[2], old[2]);
            }
            _ => assert_eq!(new, old),
        }
    }
    assert_eq!(status(&admin, 2), queued_value());
    assert_eq!(receipt.decision, UNDO_PARTIAL);
    assert_eq!((receipt.seq, receipt.rows_affected), (2, 2));
    assert_eq!((receipt.distinct_charged, receipt.transitions_charged, receipt.noop_rows), (0, 0, 0));
    assert_eq!(receipt.undo_counts(), Some((UndoMode::Partial as u8, 2, 1, 0)));
    assert_eq!(receipt.change_summary(), Some((2, 2)), "a kind-2 change record of two rows");
    assert_eq!(receipt.preview(), Some(summary.id.0));
    assert_eq!(receipt.operation_id, format!("undo:{}", summary.id.0).into_bytes());
    assert_eq!(undo_operation_id(summary.id).as_bytes(), receipt.operation_id.as_slice());
    assert_eq!(receipt.approver(), Some(ISSUER));
    assert_eq!(receipt.executor(), Some(EXECUTOR));
    assert!(!receipt.is_minimal(), "the undo role gets the full receipt");
    assert_eq!(preview_state(&admin, &ctx, &summary), (APPLIED, 0));
}

#[test]
fn partial_after_refused_complete_on_same_preview_commits() {
    let (_dir, admin) = setup();
    let ctx = context(&admin);
    let summary = with_conflict(&admin, &ctx);
    assert_eq!(
        approve(&admin, &ctx, &summary, UndoMode::Complete),
        Err(TaskError::Denied(DenyRule::UndoHasConflicts))
    );
    let receipt = approve(&admin, &ctx, &summary, UndoMode::Partial).unwrap();
    assert_eq!(receipt.undo_counts(), Some((UndoMode::Partial as u8, 2, 1, 0)));
    assert_eq!((status(&admin, 1), status(&admin, 2), status(&admin, 3)), (failed(), queued_value(), failed()));
}

#[test]
fn row_changed_between_undo_preview_and_approve_is_stale() {
    let (_dir, admin) = setup();
    let ctx = context(&admin);
    commit(&admin, &ctx, "op-1", &requeue(1, 3));
    let summary = undo(&admin, &ctx, UndoScope::Task);
    assert_eq!(counts(&summary), (3, 0, 0, 0));
    sql(&admin, "UPDATE shipments SET attempt_count = 4 WHERE id = 2");
    let before = table_contents(&admin);
    for mode in [UndoMode::Complete, UndoMode::Partial] {
        let stale = approve(&admin, &ctx, &summary, mode).unwrap_err();
        assert_eq!(stale, TaskError::ApprovalStale);
        assert_eq!(stale.sqlstate(), "55T01");
    }
    assert_eq!(table_contents(&admin), before, "nothing restored");
    assert_eq!(preview_state(&admin, &ctx, &summary), (OPEN, 3));
    // A wrong digest is stale too; a fresh preview sees the conflict.
    let mut wrong = summary.clone();
    wrong.digest[0] ^= 1;
    assert_eq!(approve(&admin, &ctx, &wrong, UndoMode::Complete), Err(TaskError::ApprovalStale));
    let fresh = undo(&admin, &ctx, UndoScope::Task);
    assert_eq!(counts(&fresh), (2, 1, 0, 0));
    assert_eq!(approve(&admin, &ctx, &fresh, UndoMode::Partial).unwrap().rows_affected, 2);
}

#[test]
fn undo_replay_returns_same_receipt() {
    let (_dir, admin) = setup();
    let ctx = context(&admin);
    commit(&admin, &ctx, "op-1", &requeue(1, 3));
    let summary = undo(&admin, &ctx, UndoScope::Task);
    let first = approve(&admin, &ctx, &summary, UndoMode::Complete).unwrap();
    assert_eq!(first.decision, UNDO_COMPLETE);
    assert_eq!(first.undo_counts(), Some((UndoMode::Complete as u8, 3, 0, 0)));
    let after = table_contents(&admin);
    // The application requeues row 1 again; a replay changes nothing.
    sql(&admin, "UPDATE shipments SET status = 'queued' WHERE id = 1");
    let changed = table_contents(&admin);
    assert_ne!(changed, after);
    let again = approve(&admin, &ctx, &summary, UndoMode::Complete).unwrap();
    assert_eq!(again, first, "the stored receipt with its change record");
    assert_eq!(table_contents(&admin), changed);
    // One seq was used.
    assert_eq!(commit(&admin, &ctx, "op-2", &requeue(5, 5)).seq, 3);
}

#[test]
fn other_mode_after_commit_is_operation_conflict() {
    let (_dir, admin) = setup();
    let ctx = context(&admin);
    commit(&admin, &ctx, "op-1", &requeue(1, 3));
    let summary = undo(&admin, &ctx, UndoScope::Task);
    approve(&admin, &ctx, &summary, UndoMode::Complete).unwrap();
    let after = table_contents(&admin);
    let conflict = approve(&admin, &ctx, &summary, UndoMode::Partial).unwrap_err();
    assert_eq!(conflict, TaskError::OperationConflict);
    assert_eq!(conflict.sqlstate(), "42T03");
    let mut wrong = summary.clone();
    wrong.digest[31] ^= 0x80;
    assert_eq!(approve(&admin, &ctx, &wrong, UndoMode::Complete), Err(TaskError::OperationConflict));
    assert_eq!(table_contents(&admin), after);
}

#[test]
fn undo_works_after_task_expiry_and_revocation() {
    let (_dir, admin) = setup();
    let ctx = context(&admin);
    let start = now_ms() as u64;
    admin.set_task_wall_clock(Some(start)).unwrap();
    commit(&admin, &ctx, "op-1", &requeue(1, 3));
    // Two days on: the task (one day) has expired.
    admin.set_task_wall_clock(Some(start + 2 * DAY_MS as u64)).unwrap();
    assert_eq!(direct(&admin, &ctx, "op-2", &requeue(4, 4)), Err(TaskError::Expired));
    let summary = undo(&admin, &ctx, UndoScope::Task);
    assert_eq!(counts(&summary), (3, 0, 0, 0));
    assert!(summary.expires_at > start + 2 * DAY_MS as u64, "not capped at the task's expiry");
    // Revoked between preview and approve (the revoke bumps def_version).
    let issuer = admin.roles().get(ISSUER).unwrap().unwrap();
    admin.tasks().revoke(&issuer, TASK).unwrap();
    assert_eq!(direct(&admin, &ctx, "op-3", &requeue(4, 4)), Err(TaskError::Revoked));
    let receipt = approve(&admin, &ctx, &summary, UndoMode::Complete).unwrap();
    assert_eq!(receipt.rows_affected, 3);
    assert_eq!(receipt.decided_ms, start + 2 * DAY_MS as u64, "the database clock");
    assert_eq!((status(&admin, 1), status(&admin, 2), status(&admin, 3)), (failed(), failed(), failed()));
    // A new preview after the revoke also works, and finds nothing left.
    assert_eq!(counts(&undo(&admin, &ctx, UndoScope::Task)), (0, 0, 0, 6));
}

#[test]
fn undo_does_not_refund_or_charge_counters() {
    let (_dir, admin) = setup();
    let ctx = context(&admin);
    let first = commit(&admin, &ctx, "op-1", &requeue(1, 5));
    assert_eq!((first.distinct_charged, first.transitions_charged), (5, 5));
    let summary = undo(&admin, &ctx, UndoScope::Task);
    let receipt = approve(&admin, &ctx, &summary, UndoMode::Complete).unwrap();
    assert_eq!((receipt.distinct_charged, receipt.transitions_charged), (0, 0));
    assert_eq!(receipt.counters_after(), Some((5, 5)), "nothing refunded");
    // The rows are failed again; requeueing them is no new distinct row
    // (membership kept) but five more transitions.
    let again = commit(&admin, &ctx, "op-2", &requeue(1, 5));
    assert_eq!((again.seq, again.distinct_charged, again.transitions_charged), (3, 0, 5));
    // The next undo's receipt shows the counters after both writes.
    let summary = undo(&admin, &ctx, UndoScope::Task);
    let receipt = approve(&admin, &ctx, &summary, UndoMode::Complete).unwrap();
    assert_eq!(receipt.counters_after(), Some((5, 10)));
}

#[test]
fn undo_of_undo_restores_task_values() {
    let (_dir, admin) = setup();
    let ctx = context(&admin);
    commit(&admin, &ctx, "op-1", &requeue(1, 3));
    let task_values = statuses(&admin);
    let first = undo(&admin, &ctx, UndoScope::Task);
    let undone = approve(&admin, &ctx, &first, UndoMode::Complete).unwrap();
    assert_eq!(status(&admin, 2), failed());
    // The undo is itself an operation: `undo:<id>`.
    let name = String::from_utf8(undone.operation_id.clone()).unwrap();
    let back = undo(&admin, &ctx, UndoScope::Operation(op(&name)));
    assert_eq!(counts(&back), (3, 0, 0, 0));
    let receipt = approve(&admin, &ctx, &back, UndoMode::Complete).unwrap();
    assert_eq!(receipt.change_summary(), Some((2, 3)));
    assert_eq!(statuses(&admin), task_values, "the task's values are back");
    // Task scope: the last undo record settles every write before it.
    assert_eq!(counts(&undo(&admin, &ctx, UndoScope::Task)), (0, 0, 0, 9));
    // An undo that never committed does not resolve.
    assert_eq!(sqlstate(undo_as(&admin, &ctx, ISSUER, UndoScope::Operation(op("undo:99")))), "42704");
}

#[test]
fn second_task_undo_finds_nothing_left() {
    let (_dir, admin) = setup();
    let ctx = context(&admin);
    commit(&admin, &ctx, "op-1", &requeue(1, 3));
    commit(&admin, &ctx, "op-2", &requeue(4, 5));
    let first = undo(&admin, &ctx, UndoScope::Task);
    assert_eq!(counts(&first), (5, 0, 0, 0));
    approve(&admin, &ctx, &first, UndoMode::Complete).unwrap();
    let after = table_contents(&admin);
    let second = undo(&admin, &ctx, UndoScope::Task);
    assert_eq!(counts(&second), (0, 0, 0, 10), "each row: its write and its undo, settled");
    let receipt = approve(&admin, &ctx, &second, UndoMode::Complete).unwrap();
    assert_eq!((receipt.rows_affected, receipt.change_summary()), (0, Some((2, 0))));
    assert_eq!(table_contents(&admin), after);
}

#[test]
fn agent_op_id_cannot_collide_with_undo_identity() {
    let (_dir, admin) = setup();
    let ctx = context(&admin);
    commit(&admin, &ctx, "op-1", &requeue(1, 2));
    // The agent takes the text of the first undo's operation ID, and the raw
    // bytes of its ledger identity (`03 | task | preview 1`), as its own IDs.
    let squat = commit(&admin, &ctx, "undo:1", &requeue(3, 3));
    let mut raw = vec![3u8];
    raw.extend_from_slice(&ctx.task.0.to_be_bytes());
    raw.extend_from_slice(&1u64.to_be_bytes());
    let mut session = admin.sql_session();
    let text = requeue(4, 4);
    let request = ProtectedRequest::Direct {
        statement: session.prepare(&text).unwrap(),
        params: Vec::new(),
        text: Arc::from(text.as_str()),
    };
    let raw_op = OperationId::new(raw).unwrap();
    let raw_receipt = protected_commit(&mut session, Actor::Executor(&ctx), &raw_op, request).unwrap();
    let summary = undo(&admin, &ctx, UndoScope::Operation(op("op-1")));
    assert_eq!(summary.id.0, 1);
    assert_eq!(counts(&summary), (2, 0, 0, 0));
    let receipt = approve(&admin, &ctx, &summary, UndoMode::Complete).unwrap();
    assert_eq!((receipt.seq, receipt.rows_affected), (4, 2));
    assert_eq!(receipt.operation_id, b"undo:1".to_vec());
    // Rows 1-2 restored; the agent's rows 3 and 4 kept.
    assert_eq!(
        (status(&admin, 1), status(&admin, 2), status(&admin, 3), status(&admin, 4)),
        (failed(), failed(), queued_value(), queued_value())
    );
    // Each ID still answers its own outcome.
    let lookup = |id: &OperationId| outcome(&admin.sql_session(), &ctx, id).unwrap();
    match lookup(&op("undo:1")) {
        Some(Outcome::Committed(found)) => assert_eq!(found, squat),
        other => panic!("{other:?}"),
    }
    match lookup(&raw_op) {
        Some(Outcome::Committed(found)) => assert_eq!(found, raw_receipt),
        other => panic!("{other:?}"),
    }
    assert_eq!(approve(&admin, &ctx, &summary, UndoMode::Complete).unwrap(), receipt);
}

#[test]
fn undo_approve_requires_undo_privilege_and_no_task_session() {
    let (_dir, admin) = setup();
    let ctx = context(&admin);
    commit(&admin, &ctx, "op-1", &requeue(1, 2));
    let summary = undo(&admin, &ctx, UndoScope::Task);
    let before = table_contents(&admin);
    for role in ["auditor", EXECUTOR, "nobody", APPROVER] {
        let refused = approve_as(&admin, &ctx, role, &summary, UndoMode::Complete).unwrap_err();
        assert_eq!(refused.sqlstate(), "42501", "{role}");
    }
    let mut agent = admin.sql_session();
    agent.bind_task(ctx.clone()).unwrap();
    let refused = undo_approve(
        &mut agent,
        &ISSUER.to_owned(),
        &ctx.name,
        summary.id,
        summary.digest,
        UndoMode::Complete,
    );
    assert_eq!(refused.unwrap_err().sqlstate(), "0A000");
    // The executor cannot reach the undo request through protected_commit.
    let request = ProtectedRequest::Undo { preview: summary.id, digest: summary.digest, mode: UndoMode::Complete };
    let mut session = admin.sql_session();
    let refused = protected_commit(&mut session, Actor::Executor(&ctx), &op("x"), request);
    assert_eq!(refused.unwrap_err().sqlstate(), "0A000");
    assert_eq!(table_contents(&admin), before);
    assert_eq!(approve(&admin, &ctx, &summary, UndoMode::Complete).unwrap().rows_affected, 2);
}
