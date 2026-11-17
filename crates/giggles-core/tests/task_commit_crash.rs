//! GD-0061 R3-619: the protected commit's crash points (design-6 D-I).
//!
//! Each crash runs in a child process that re-runs this binary by
//! `current_exe` (hence its own `[[test]]`), following
//! `durable_boundaries.rs`. The parent then reopens the database and checks:
//! a point before the sequencer sync leaves nothing (rows, head, membership,
//! change record, receipt and outcome all absent, the operation ID free); a
//! point after it leaves all of them together. Head and membership are
//! observed through a follow-up commit: its receipt's `seq` is the head's
//! `next_seq`, and re-touching a member row charges no new distinct row.
//!
//! Task "requeue failed for merchant 42" on `shipments` (`deliveries` is
//! reserved): rows 1-4 are merchant 42's failed rows.
#![cfg(feature = "engine-rocksdb")]
use giggles_core::relational::Value;
use giggles_core::roles::{NewRole, TablePrivilege};
use giggles_core::task::context::TaskContext;
use giggles_core::task::definition::{
    AllowedWrite, DisclosureCaps, LoadLimits, MutationCaps, NewTask, PredicateText, WallClockMs,
};
use giggles_core::task::{
    inspect_outcome, outcome, protected_commit, Actor, OperationId, Outcome, ProtectedPoint,
    ProtectedRequest, Receipt, TaskError,
};
use giggles_core::{Admin, CrashPoint, Engine, Limits};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

const TASK: &str = "inc-619";
const REQUEUE: &str =
    "UPDATE shipments SET status = 'queued' WHERE merchant_id = 42 AND status = 'failed'";
/// Also touches merchant 42's delivered row 5, which `allowed_write.old_row`
/// refuses: a recorded denial.
const TOO_WIDE: &str = "UPDATE shipments SET status = 'queued' WHERE merchant_id = 42";
const OP: &str = "op-crash";
const PROBE: &str = "op-probe";
const UNTOUCHED: [&str; 6] = ["failed", "failed", "failed", "failed", "delivered", "failed"];
const REQUEUED: [&str; 6] = ["queued", "queued", "queued", "queued", "delivered", "failed"];

/// Every protected point; the exhaustive match forces this registry to be
/// reviewed when the enum grows.
const PROTECTED: [ProtectedPoint; 6] = [
    ProtectedPoint::AfterLookup,
    ProtectedPoint::AfterHold,
    ProtectedPoint::AfterStatement,
    ProtectedPoint::AfterDecision,
    ProtectedPoint::AfterDenialRollback,
    ProtectedPoint::AfterDenialStage,
];
fn protected_name(point: ProtectedPoint) -> &'static str {
    match point {
        ProtectedPoint::AfterLookup => "AfterLookup",
        ProtectedPoint::AfterHold => "AfterHold",
        ProtectedPoint::AfterStatement => "AfterStatement",
        ProtectedPoint::AfterDecision => "AfterDecision",
        ProtectedPoint::AfterDenialRollback => "AfterDenialRollback",
        ProtectedPoint::AfterDenialStage => "AfterDenialStage",
    }
}
/// Denial points are reached only by a recorded denial.
fn on_denial(point: ProtectedPoint) -> bool {
    matches!(
        point,
        ProtectedPoint::AfterDenialRollback | ProtectedPoint::AfterDenialStage
    )
}
const CRASHES: [(CrashPoint, &str); 3] = [
    (CrashPoint::BeforeCommit, "BeforeCommit"),
    (CrashPoint::AfterCommit, "AfterCommit"),
    (CrashPoint::BeforeAck, "BeforeAck"),
];

/// Where a child crashes.
#[derive(Clone, Copy, Debug)]
enum Seam {
    Protected(ProtectedPoint),
    Crash(CrashPoint),
}
impl Seam {
    fn name(self) -> &'static str {
        match self {
            Seam::Protected(point) => protected_name(point),
            Seam::Crash(point) => {
                CRASHES
                    .iter()
                    .find(|(p, _)| *p == point)
                    .expect("registered crash point")
                    .1
            }
        }
    }
    fn parse(name: &str) -> Self {
        if let Some(point) = PROTECTED.iter().find(|p| protected_name(**p) == name) {
            return Seam::Protected(*point);
        }
        let (point, _) = CRASHES
            .iter()
            .find(|(_, n)| *n == name)
            .unwrap_or_else(|| panic!("unknown seam {name}"));
        Seam::Crash(*point)
    }
}

struct Dir(PathBuf);
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
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

fn statuses(admin: &Admin) -> Vec<String> {
    sql(admin, "SELECT status FROM shipments ORDER BY id")
        .into_iter()
        .map(|row| match &row[0] {
            Value::Text(text) => text.clone(),
            other => panic!("{other:?}"),
        })
        .collect()
}

/// A fresh v2 database with the table, the two roles and the task.
fn setup() -> Dir {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let dir = Dir(std::env::temp_dir().join(format!(
        "task-commit-crash-{}-{}-{}",
        std::process::id(),
        now_ms(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )));
    let admin = Admin::create_engine(&dir.0, Limits::default(), Engine::V2).unwrap();
    sql(
        &admin,
        "CREATE TABLE shipments (id bigint primary key, merchant_id bigint, status text, \
         attempt_count int)",
    );
    sql(
        &admin,
        "INSERT INTO shipments VALUES (1, 42, 'failed', 1), (2, 42, 'failed', 2), \
         (3, 42, 'failed', 1), (4, 42, 'failed', 3), (5, 42, 'delivered', 0), (6, 7, 'failed', 1)",
    );
    let roles = admin.roles();
    for (name, task_only) in [("incident_controller", false), ("repair_agent", true)] {
        roles
            .create(NewRole { name: name.into(), login: true, ..NewRole::default() })
            .unwrap();
        if task_only {
            roles.set_task_only(name, true).unwrap();
        }
        roles
            .grant_table("shipments", name, &[TablePrivilege::Select, TablePrivilege::Update])
            .unwrap();
    }
    let issuer = roles.get("incident_controller").unwrap().unwrap();
    let new = NewTask {
        name: TASK.into(),
        executor: "repair_agent".into(),
        approver: None,
        schema: None,
        table: "shipments".into(),
        visible_rows: PredicateText("merchant_id = 42".into()),
        readable: ["id", "merchant_id", "status", "attempt_count"]
            .iter()
            .map(|c| c.to_string())
            .collect(),
        masked: Vec::new(),
        untrusted_text: Vec::new(),
        allowed_write: Some(AllowedWrite {
            columns: ["status".to_string()].into_iter().collect(),
            old_row: PredicateText("status = 'failed'".into()),
            new_row: PredicateText("status = 'queued'".into()),
        }),
        mutation_caps: MutationCaps { distinct_rows: 25, transitions: 25 },
        disclosure_caps: DisclosureCaps {
            rows_read: 1000,
            rows_returned: 1000,
            bytes_returned: 2_097_152,
        },
        limits: LoadLimits::default(),
        approval_threshold: 25,
        expires: "2099-12-31 00:00:00+00".into(),
    };
    admin.tasks().create(&issuer, new, WallClockMs(now_ms())).unwrap();
    dir
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

fn commit(admin: &Admin, id: &str, text: &str) -> Result<Receipt, TaskError> {
    let ctx = context(admin);
    let mut session = admin.sql_session();
    let request = ProtectedRequest::Direct {
        statement: session.prepare(text).unwrap(),
        params: Vec::new(),
        text: Arc::from(text),
    };
    protected_commit(&mut session, Actor::Executor(&ctx), &op(id), request)
}

/// The inspector's view of `id`: full receipts carry their change record.
fn inspect(admin: &Admin, id: &str) -> Option<Outcome> {
    let task = context(admin).task;
    inspect_outcome(&admin.sql_session(), task, &op(id)).unwrap()
}

/// The executor's lookup (`SHOW OPERATION`).
fn lookup(admin: &Admin, id: &str) -> Option<Outcome> {
    let ctx = context(admin);
    outcome(&admin.sql_session(), &ctx, &op(id)).unwrap()
}

fn open(dir: &Dir) -> Admin {
    Admin::open(&dir.0, Limits::default()).unwrap()
}

#[test]
fn crash_worker() {
    let Ok(seam) = std::env::var("GD_TASK_CRASH_SEAM") else {
        return;
    };
    let path = PathBuf::from(std::env::var_os("GD_TASK_CRASH_PATH").unwrap());
    let text = std::env::var("GD_TASK_CRASH_TEXT").unwrap();
    let admin = Admin::open(&path, Limits::default()).unwrap();
    match Seam::parse(&seam) {
        Seam::Protected(point) => admin.set_protected_point(Some(point)).unwrap(),
        Seam::Crash(point) => admin.set_crash_point(point).unwrap(),
    }
    let result = commit(&admin, OP, &text);
    // Reaching here means the seam never fired.
    eprintln!("seam {seam} returned: {result:?}");
    std::process::exit(3);
}

/// Runs `text` under operation `OP` in a child armed at `seam`, and asserts
/// that the child was terminated there.
fn crash(dir: &Path, seam: Seam, text: &str) {
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "crash_worker", "--nocapture"])
        .env("GD_TASK_CRASH_PATH", dir)
        .env("GD_TASK_CRASH_SEAM", seam.name())
        .env("GD_TASK_CRASH_TEXT", text)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(86), "{seam:?} on {text}: {output:?}");
}

/// Before the sync: nothing durable, and the ID is free. The follow-up
/// commit under the same ID takes `seq` 1 (head untouched) and charges all
/// four rows as new (no membership).
fn assert_nothing(dir: &Dir, seam: Seam) {
    let admin = open(dir);
    assert_eq!(statuses(&admin), UNTOUCHED, "{seam:?}: rows");
    assert_eq!(inspect(&admin, OP), None, "{seam:?}: outcome");
    assert_eq!(lookup(&admin, OP), None, "{seam:?}: lookup");
    let receipt = commit(&admin, OP, REQUEUE).unwrap_or_else(|e| panic!("{seam:?}: {e:?}"));
    assert_eq!(
        (receipt.seq, receipt.rows_affected, receipt.distinct_charged),
        (1, 4, 4),
        "{seam:?}: head or membership survived"
    );
    assert_eq!(statuses(&admin), REQUEUED);
}

/// After the sync: rows, outcome, receipt and change record (the full view
/// decodes all three), head (`seq` 2 next) and membership (re-touching row 1
/// charges no distinct row) are all there.
fn assert_kept(dir: &Dir, seam: Seam) -> Receipt {
    let admin = open(dir);
    assert_eq!(statuses(&admin), REQUEUED, "{seam:?}: rows");
    let Some(Outcome::Committed(full)) = inspect(&admin, OP) else {
        panic!("{seam:?}: no committed outcome");
    };
    assert!(!full.is_minimal(), "{seam:?}: inspector view");
    assert_eq!(
        (full.seq, full.rows_affected, full.distinct_charged, full.transitions_charged),
        (1, 4, 4, 4),
        "{seam:?}: receipt"
    );
    // A resend replays the stored receipt.
    let again = commit(&admin, OP, REQUEUE).unwrap();
    assert_eq!((again.seq, again.effects_digest), (full.seq, full.effects_digest));
    assert!(again.is_minimal());
    // Head and membership.
    sql(&admin, "UPDATE shipments SET status = 'failed' WHERE id = 1");
    let probe = commit(&admin, PROBE, REQUEUE).unwrap();
    assert_eq!(
        (probe.seq, probe.rows_affected, probe.distinct_charged, probe.transitions_charged),
        (2, 1, 0, 1),
        "{seam:?}: head or membership lost"
    );
    full
}

#[test]
fn crash_at_each_point_before_sync_leaves_nothing() {
    let mut seams: Vec<(Seam, &str)> = PROTECTED
        .iter()
        .map(|&point| {
            let text = if on_denial(point) { TOO_WIDE } else { REQUEUE };
            (Seam::Protected(point), text)
        })
        .collect();
    seams.push((Seam::Crash(CrashPoint::BeforeCommit), REQUEUE));
    assert_eq!(seams.len(), 7);
    for (seam, text) in seams {
        let dir = setup();
        crash(&dir.0, seam, text);
        assert_nothing(&dir, seam);
    }
}

#[test]
fn crash_after_commit_keeps_rows_head_membership_change_receipt_outcome() {
    let dir = setup();
    let seam = Seam::Crash(CrashPoint::AfterCommit);
    crash(&dir.0, seam, REQUEUE);
    assert_kept(&dir, seam);
}

#[test]
fn crash_before_ack_then_lookup_returns_receipt() {
    let dir = setup();
    let seam = Seam::Crash(CrashPoint::BeforeAck);
    crash(&dir.0, seam, REQUEUE);
    {
        let admin = open(&dir);
        let Some(Outcome::Committed(receipt)) = lookup(&admin, OP) else {
            panic!("no committed outcome after BeforeAck");
        };
        assert!(receipt.is_minimal());
        assert_eq!((receipt.seq, receipt.rows_affected), (1, 4));
        assert_eq!(receipt.operation_id, OP.as_bytes());
    }
    assert_kept(&dir, seam);
}

#[test]
fn crash_during_denial_before_sync_leaves_id_free() {
    let seams = [
        Seam::Protected(ProtectedPoint::AfterDenialRollback),
        Seam::Protected(ProtectedPoint::AfterDenialStage),
        Seam::Crash(CrashPoint::BeforeCommit),
    ];
    for seam in seams {
        let dir = setup();
        crash(&dir.0, seam, TOO_WIDE);
        // The denial was never recorded: another request takes the ID.
        assert_nothing(&dir, seam);
    }
}

#[test]
fn crash_after_denial_commit_replays_denial() {
    // The same request, uncrashed elsewhere, is a recorded denial.
    let rule = {
        let other = setup();
        let admin = open(&other);
        match commit(&admin, OP, TOO_WIDE) {
            Err(TaskError::Denied(rule)) => rule,
            other => panic!("expected a denial: {other:?}"),
        }
    };
    for seam in [CrashPoint::AfterCommit, CrashPoint::BeforeAck] {
        let dir = setup();
        crash(&dir.0, Seam::Crash(seam), TOO_WIDE);
        let admin = open(&dir);
        assert_eq!(statuses(&admin), UNTOUCHED, "{seam:?}: rows");
        assert_eq!(inspect(&admin, OP), Some(Outcome::Denied(rule)), "{seam:?}");
        assert_eq!(lookup(&admin, OP), Some(Outcome::Denied(rule)), "{seam:?}");
        assert_eq!(commit(&admin, OP, TOO_WIDE), Err(TaskError::Denied(rule)));
        assert_eq!(commit(&admin, OP, REQUEUE), Err(TaskError::OperationConflict));
        // A denial takes no receipt sequence and charges no membership.
        let probe = commit(&admin, PROBE, REQUEUE).unwrap();
        assert_eq!((probe.seq, probe.distinct_charged), (1, 4), "{seam:?}");
    }
}

#[test]
fn error_after_commit_resolves_by_lookup() {
    let dir = setup();
    let admin = open(&dir);
    // An injected error before the sync leaves the ID free.
    admin.set_crash_point(CrashPoint::ErrorBeforeCommit).unwrap();
    assert!(matches!(commit(&admin, OP, REQUEUE), Err(TaskError::Core(_))));
    admin.set_crash_point(CrashPoint::None).unwrap();
    assert_eq!(lookup(&admin, OP), None);
    assert_eq!(statuses(&admin), UNTOUCHED);
    // A lost response after the sync: the caller resolves it by lookup.
    admin.set_crash_point(CrashPoint::ErrorAfterCommit).unwrap();
    let lost = commit(&admin, OP, REQUEUE);
    assert!(matches!(lost, Err(TaskError::Core(_))), "{lost:?}");
    admin.set_crash_point(CrashPoint::None).unwrap();
    let Some(Outcome::Committed(receipt)) = lookup(&admin, OP) else {
        panic!("lost commit not found by lookup");
    };
    assert_eq!((receipt.seq, receipt.rows_affected), (1, 4));
    assert_eq!(commit(&admin, OP, REQUEUE), Ok(receipt.clone()));
    assert_eq!(statuses(&admin), REQUEUED);
    drop(admin);
    assert_kept(&dir, Seam::Crash(CrashPoint::ErrorAfterCommit));
}
