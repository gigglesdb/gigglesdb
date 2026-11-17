//! GD-0061 R3-802: PREVIEW UNDO, the dry run and the kind-02 store
//! (design-7 D8), through the public API.
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
    protected_commit, undo_preview, Actor, ConflictReason, OperationId, ProtectedRequest,
    Receipt, RetentionConfig, TaskError, UndoScope, UndoSummary,
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
