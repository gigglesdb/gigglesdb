//! `protected_commit`: the protected write path (GD-0061 design section 6,
//! D-F; ruling R3).
//!
//! R3-615 lands the Direct success path, one attempt, hold first (D1):
//!
//! ```text
//!  0  v2 only and not REPLICATED, else 0A000 (D9, D13)
//!  1  request fingerprint; 2 ledger lookup (same fp -> stored outcome, other -> 42T03)
//!  3  latest committed definition: Revoked / Expired
//!  4  Serializable transaction; 5 authority hold (before any snapshot)
//!  6  steps 2 and 3 again under the hold
//!  7  the statement through the C1 seam (snapshot restarted after its locks)
//!  9  rechecks at the statement snapshot: definition, executor UPDATE privilege
//! 10  effects cross-check; 11 head; 12 decide::evaluate
//! 13  stage membership, change record, receipt, outcome, head
//! 14  commit; 15 dropping the transaction releases the hold
//! ```
//!
//! R3-616 adds the durable denials and the error table:
//! - `Denied` and `BudgetExceeded` from step 12 are recorded: the statement
//!   is rolled back to a mark taken after step 6, the snapshot restarted, and
//!   only the outcome (kind 3 or 4) and the head (`denials + 1`) commit. A
//!   resend replays the stored denial; another request under the ID is 42T03.
//! - Not recorded (the operation ID stays free): ApprovalRequired,
//!   ApprovalStale, Retry, lock timeout, cancellation, limits, Expired,
//!   Revoked, OperationConflict, privilege refusals and internal errors.
//! - A serialization failure or a deadlock retries steps 4-15 in a new
//!   transaction, at most three attempts in all, with a jittered 2 ms then
//!   8 ms backoff and a cancellation check; the third failure is
//!   `TaskError::Retry` (40001). A hold wait past `lock_wait_ms` (55P03),
//!   cancellation (57014), limits and an ambiguous commit (08007) surface
//!   unchanged.
//!
//! R3-618 adds the attachment points. `Apply` runs through an [`ApplySource`]
//! that Phase 7 implements over the stored preview and approval: the source
//! supplies the statement to re-run, the approval state of the re-run
//! effects (step 12) and stages the consumption in the commit's write set
//! (step 13). R3-706 fills in [`stored_apply_source`] with
//! [`super::preview::apply_stage`] (D7.6 steps 1-3, 5 and 6; step 4 is
//! `decide::evaluate` here). An Apply that consumes its preview takes it out
//! of the head's `stored_previews` count and names the approval's role in the
//! receipt.
//!
//! R3-803 adds the undo path (D8): `Actor::Undo` with `ProtectedRequest::Undo`
//! (any other pairing is `Unsupported`). It skips task expiry and
//! revocation, the column rules, the allowed write, the caps and the
//! threshold, and keeps the hold, the `seq` allocation, read validation and
//! the effects cross-check. Its ledger identity is `03 | task | preview`;
//! the role's undo and table privileges are checked before the outcome
//! lookup, so a replay never shows a receipt to a role that may not undo.
//! Nothing on this path is a recorded denial.
//!
//! R3-619 adds the crash seams (D-I). The commit reuses `CrashPoint`'s
//! `BeforeCommit` (after step 13), `AfterCommit`, `BeforeAck`,
//! `ErrorBeforeCommit` and `ErrorAfterCommit`, for a recorded denial's commit
//! too, and adds [`ProtectedPoint`]. Every point before the sync leaves
//! nothing; every point after it leaves rows, head, membership, change record,
//! receipt and outcome together (or, for a denial, the outcome and the head).
use super::authority;
use super::columns;
use super::context::TaskContext;
use super::decide::{self, ApprovalState, Decision, RequestKind};
use super::definition::{LoadLimits, RoleName, TaskDef};
use super::effects::Effects;
use super::error::{Budget, DenyRule, TaskError};
use super::ledger::{self, OperationId, Outcome, RequestBody, RowIntent};
use super::preview::PreviewId;
use super::receipt::{
    self, ChangeRecord, Receipt, DECISION_COMMITTED, DECISION_UNDO_COMPLETE, DECISION_UNDO_PARTIAL,
};
use super::records::{
    change_header_key, change_row_key, receipt_key, ChangeHeader, ChangeRow, MutationHead,
    OutcomePayload, OutcomeRecord, ReceiptRecord, TaskObjectId, MAX_STATEMENT_TEXT,
};
use super::undo::{self, UndoMode, UndoStaged};
use crate::relational::clock::ExecutionClock;
use crate::relational::{Prepared, SqlError, Value};
use crate::storage::engine::txn::{Savepoint, TxnError};
use crate::storage::roles::{table_privileges_txn, RoleError, TablePrivilege};
use crate::storage::{Isolation, Keyspace, ProtectedRun, SqlSession, Transaction, V2Db};
use crate::{CoreError, CrashPoint};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// How long an operation's outcome is kept after the later of its decision
/// and the task's expiry (D5 default; a server setting later).
pub const OUTCOME_RETENTION_MS: u64 = 30 * 24 * 60 * 60 * 1000;

/// The protected commit's own crash seams (D-I), armed only through the
/// trusted `Admin::set_protected_point`, never by environment. Each one
/// terminates the process where it is reached.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProtectedPoint {
    /// After the outcome lookup (step 2) found nothing.
    AfterLookup,
    /// After the authority hold is taken (step 5).
    AfterHold,
    /// After the statement ran through the C1 seam (step 7).
    AfterStatement,
    /// After `decide::evaluate` (step 12), whatever it decided.
    AfterDecision,
    /// In a recorded denial, after the rollback to the mark.
    AfterDenialRollback,
    /// In a recorded denial, after the outcome and head are staged.
    AfterDenialStage,
}

/// A trusted pause seam at the [`ProtectedPoint`]s (GD-0061 R3-902, design-7
/// D9 step 13): called on the committing thread each time a point is
/// reached, before any armed crash. Set only through the trusted
/// `Admin::set_protected_hook`, never by environment or network input.
#[doc(hidden)]
pub type ProtectedHook = Arc<dyn Fn(ProtectedPoint) + Send + Sync>;

/// The seams armed for one call, read once at its start.
#[derive(Clone)]
struct Faults {
    crash: CrashPoint,
    point: Option<ProtectedPoint>,
    hook: Option<ProtectedHook>,
}

impl Faults {
    fn at(&self, point: ProtectedPoint) {
        if let Some(hook) = &self.hook {
            hook(point);
        }
        if self.point == Some(point) {
            crate::storage::crash_now();
        }
    }

    /// Just before the sync: `BeforeCommit` and `ErrorBeforeCommit`.
    fn before_commit(&self) -> Result<(), TaskError> {
        match self.crash {
            CrashPoint::BeforeCommit => crate::storage::crash_now(),
            CrashPoint::ErrorBeforeCommit => {
                Err(CoreError::Storage("injected before commit".into()).into())
            }
            _ => Ok(()),
        }
    }

    /// After the sync: `AfterCommit`, then `ErrorAfterCommit` (a lost
    /// response) and `BeforeAck`. `ErrorAfterDecisionCommit` does not apply.
    fn after_commit(&self) -> Result<(), TaskError> {
        match self.crash {
            CrashPoint::AfterCommit | CrashPoint::BeforeAck => crate::storage::crash_now(),
            CrashPoint::ErrorAfterCommit => {
                Err(CoreError::Storage("injected lost commit response".into()).into())
            }
            _ => Ok(()),
        }
    }
}

/// Who performs a protected commit (ruling R3).
pub enum Actor<'a> {
    /// The task's executor, in its bound task session.
    Executor(&'a TaskContext),
    /// An undo by an issuer, approver or grantee (R3-803).
    Undo { role: &'a RoleName, task: TaskObjectId },
}

/// What a protected commit runs (ruling R3). `Direct` keeps the binding
/// `Prepared` by value; a request is built once per call.
#[allow(clippy::large_enum_variant)]
pub enum ProtectedRequest {
    /// One UPDATE, its parameters and its text exactly as received.
    Direct {
        statement: Prepared,
        params: Vec<Value>,
        text: Arc<str>,
    },
    Apply { preview: PreviewId, digest: [u8; 32] },
    Undo {
        preview: PreviewId,
        digest: [u8; 32],
        mode: UndoMode,
    },
}

/// What Phase 7 (apply) supplies to a protected commit. Every method runs
/// under the authority hold, inside the attempt; a retried attempt calls
/// them again.
pub(crate) trait ApplySource {
    /// The stored statement to re-run and its parameters, prepared under the
    /// current catalog. A preview that no longer holds is `ApprovalStale`.
    fn statement(&self, session: &mut SqlSession) -> Result<(Prepared, Vec<Value>), TaskError>;
    /// The pinned clock the statement re-runs with (D7.4); `None` is the
    /// session clock.
    fn clock(&self) -> Option<ExecutionClock> {
        None
    }
    /// Compares the re-run `effects` with the stored preview and reads the
    /// approval, through `txn` (tracked reads at the statement snapshot).
    fn approval_state(
        &self,
        txn: &mut Transaction,
        effects: &Effects,
    ) -> Result<ApprovalState, CoreError>;
    /// Stages the preview's applied state and, when one was used, the
    /// approval's consumption under receipt `seq`, in the commit's write set.
    fn stage_consumed(&self, txn: &mut Transaction, seq: u64) -> Result<(), CoreError>;
    /// The role whose approval this attempt uses, if one was needed; the
    /// receipt records it.
    fn approver(&self) -> Option<RoleName> {
        None
    }
}

/// Resolves the [`ApplySource`] of an Apply request, once per call, after
/// the outcome lookup and the task checks (D-F steps 2 and 3).
pub(crate) type Sources<'a> = dyn FnMut(
        &Arc<V2Db>,
        &TaskContext,
        PreviewId,
        &[u8; 32],
    ) -> Result<Box<dyn ApplySource + 'a>, TaskError>
    + 'a;

/// The attachment point for Phase 7 (R3-706): the stored preview and
/// approval of `preview`.
pub(crate) fn stored_apply_source(
    db: &Arc<V2Db>,
    ctx: &TaskContext,
    preview: PreviewId,
    digest: &[u8; 32],
) -> Result<Box<dyn ApplySource>, TaskError> {
    super::preview::apply_stage(db, ctx, preview, *digest)
}

/// Where a definition is read.
pub(crate) enum DefAt<'t> {
    /// The latest committed definition (D-F steps 3 and 6).
    Latest,
    /// At the statement snapshot of the protected transaction (step 9).
    Snapshot(&'t mut Transaction),
}

/// The definition loader; tests inject a `TaskDef` through it.
pub(crate) type LoadDef<'a> =
    dyn FnMut(&V2Db, TaskObjectId, DefAt<'_>) -> Result<Option<Arc<TaskDef>>, CoreError> + 'a;

/// Runs one protected request for `actor` under operation ID `op`.
///
/// Deviation from ruling R3: takes `&mut SqlSession`, because the C1 seam
/// (`execute_protected_v2`) parks the run on the session.
pub fn protected_commit(
    session: &mut SqlSession,
    actor: Actor<'_>,
    op: &OperationId,
    request: ProtectedRequest,
) -> Result<Receipt, TaskError> {
    commit_with(session, actor, op, request, &mut catalog_def)
}

/// A caller's check of the statement's affected row count (the native
/// intent's row bound: exactly one row, or at most `n`). It sees the real
/// count of the attempt that would commit.
pub type RowCheck<'a> = dyn Fn(usize) -> Result<(), TaskError> + 'a;

/// [`protected_commit`] with `check` applied to the affected row count of
/// each attempt, after the statement ran and before anything is staged. A
/// refusal from `check` is returned as is and is not recorded: the
/// statement's writes are rolled back with the transaction, and no outcome,
/// head, membership, change record or receipt is written, so the operation
/// ID stays free. A replayed outcome (the ID already decided) is returned
/// without the check when the fingerprint (including `intent`) matches.
/// A different intent under the same operation ID returns operation conflict.
pub fn protected_commit_checked(
    session: &mut SqlSession,
    actor: Actor<'_>,
    op: &OperationId,
    request: ProtectedRequest,
    intent: RowIntent,
    check: &RowCheck<'_>,
) -> Result<Receipt, TaskError> {
    commit_checked(
        session,
        actor,
        op,
        request,
        &mut catalog_def,
        &mut stored_apply_source,
        Some((intent, check)),
    )
}

/// The task's definition from the task catalog (kind 19).
pub(crate) fn catalog_def(
    db: &V2Db,
    task: TaskObjectId,
    at: DefAt<'_>,
) -> Result<Option<Arc<TaskDef>>, CoreError> {
    let view = match at {
        DefAt::Latest => {
            let mut txn = db.begin(Isolation::ReadCommitted);
            db.catalog().committed_view(&mut txn)?
        }
        DefAt::Snapshot(txn) => db.catalog().committed_view(txn)?,
    };
    Ok(view.tasks.get(&task.0).cloned())
}

pub(crate) fn unsupported() -> TaskError {
    TaskError::Core(Box::new(CoreError::Unsupported))
}

fn corrupt(message: &str) -> TaskError {
    TaskError::Core(Box::new(CoreError::Corrupt(message.into())))
}

/// Engine errors: a serialization failure or a deadlock is `Retry`, which
/// the attempt loop retries; everything else (55P03, 57014, limits, 08007)
/// passes through unchanged.
pub(crate) fn engine(error: CoreError) -> TaskError {
    match error {
        CoreError::Sql(SqlError::Conflict) | CoreError::Deadlock => TaskError::Retry,
        other => other.into(),
    }
}

/// The hold's lock wait ran past the task's `lock_wait_ms`: 55P03
/// lock_not_available, as `LOCK ... NOWAIT` reports it (not 53300 `Busy`).
pub(crate) fn hold_error(error: TxnError) -> TaskError {
    match error {
        TxnError::LockTimeout => CoreError::LockNotAvailable.into(),
        other => engine(other.into()),
    }
}

pub(crate) fn role_error(error: RoleError) -> TaskError {
    match error {
        RoleError::Core(core) => engine(core),
        _ => TaskError::Denied(DenyRule::RolePrivilege),
    }
}

/// Revoked: missing, revoked, another task, another executor or a newer
/// definition (R3 has no ALTER TASK).
fn bound<'d>(def: Option<&'d TaskDef>, ctx: &TaskContext) -> Result<&'d TaskDef, TaskError> {
    def.filter(|def| {
        !def.revoked
            && def.id == ctx.task
            && def.def_version == ctx.bound_def_version
            && def.executor == ctx.executor
    })
    .ok_or(TaskError::Revoked)
}

/// [`bound`], then Expired: `now >= expires_at`.
pub(crate) fn standing(def: Option<&TaskDef>, ctx: &TaskContext, now_ms: u64) -> Result<(), TaskError> {
    let def = bound(def, ctx)?;
    if i64::try_from(now_ms).unwrap_or(i64::MAX) >= def.expires_at.0 {
        return Err(TaskError::Expired);
    }
    Ok(())
}

/// D-G conflict rule on the latest committed outcome: same fingerprint
/// returns the stored outcome, another fingerprint is `OperationConflict`.
fn replay(
    db: &V2Db,
    task: TaskObjectId,
    key: &[u8],
    fp: &[u8; 32],
    view: View,
) -> Result<Option<Receipt>, TaskError> {
    let Some(record) = ledger::lookup(db, key)? else {
        return Ok(None);
    };
    if &record.request_fp != fp {
        return Err(TaskError::OperationConflict);
    }
    match stored_outcome(db, task, &record, view)? {
        Outcome::Committed(receipt) => Ok(Some(receipt)),
        Outcome::Denied(rule) => Err(TaskError::Denied(rule)),
        Outcome::BudgetExceeded(budget) => Err(TaskError::BudgetExceeded(budget)),
    }
}

/// Which receipt view an outcome lookup returns (D-H).
#[derive(Clone, Copy, PartialEq, Eq)]
enum View {
    /// The executor's: no roles, versions, after-counters or change record.
    Minimal,
    /// The inspector's: the stored record and its change record.
    Full,
}

/// Decodes a stored outcome of `task`. A committed outcome reads its
/// receipt (and, for the full view, its change record) at the latest
/// committed state; nothing is written and no hold is taken.
fn stored_outcome(
    db: &V2Db,
    task: TaskObjectId,
    record: &OutcomeRecord,
    view: View,
) -> Result<Outcome, TaskError> {
    match record.payload {
        OutcomePayload::Committed { task: stored, seq } if stored == task => {
            let txn = db.begin(Isolation::ReadCommitted);
            let read = |key: &[u8]| -> Result<Option<Vec<u8>>, TaskError> {
                Ok(txn.kernel_latest_committed(key).map_err(CoreError::from)?)
            };
            let value = read(&receipt_key(task, seq))?
                .ok_or_else(|| corrupt("task outcome without receipt"))?;
            let stored = ReceiptRecord::decode(&value)?;
            if view == View::Minimal {
                return Ok(Outcome::Committed(Receipt::from_record(&stored, None).minimal()));
            }
            let header = read(&change_header_key(task, seq))?
                .ok_or_else(|| corrupt("task receipt without change record"))?;
            let header = ChangeHeader::decode(&header)?;
            let rows = (0..header.row_count)
                .map(|row_no| {
                    let value = read(&change_row_key(task, seq, row_no))?
                        .ok_or_else(|| corrupt("task change record row missing"))?;
                    Ok(ChangeRow::decode(&value)?)
                })
                .collect::<Result<Vec<_>, TaskError>>()?;
            let change = ChangeRecord { header, rows };
            Ok(Outcome::Committed(Receipt::from_record(&stored, Some(change))))
        }
        OutcomePayload::Denied { task: stored, rule, .. } if stored == task => Ok(Outcome::Denied(
            DenyRule::from_code(rule).ok_or_else(|| corrupt("task outcome deny rule"))?,
        )),
        OutcomePayload::BudgetExceeded { task: stored, budget, .. } if stored == task => {
            Ok(Outcome::BudgetExceeded(
                Budget::from_code(budget).ok_or_else(|| corrupt("task outcome budget"))?,
            ))
        }
        _ => Err(corrupt("task outcome of another kind or task")),
    }
}

/// The database an outcome lookup reads: v2 only (D9), not replicated (D13).
fn lookup_db(session: &SqlSession) -> Result<Arc<V2Db>, TaskError> {
    let db = session.v2_db().ok_or_else(unsupported)?;
    if db.task_kernel_unsupported() {
        return Err(unsupported());
    }
    Ok(db)
}

/// `SHOW OPERATION '<id>'` for the session's executor (D-G): the stored
/// outcome of `op` under the session's own task, `None` when the ID was never
/// decided (or its refusal was not recorded). Never writes and takes no hold.
///
/// A revoked task (or a definition the session is no longer bound to) is
/// `Revoked`; an expired task may still look up its outcomes. A committed
/// outcome returns the minimal receipt.
pub fn outcome(
    session: &SqlSession,
    ctx: &TaskContext,
    op: &OperationId,
) -> Result<Option<Outcome>, TaskError> {
    outcome_with(session, ctx, op, &mut catalog_def)
}

pub(crate) fn outcome_with(
    session: &SqlSession,
    ctx: &TaskContext,
    op: &OperationId,
    load_def: &mut LoadDef<'_>,
) -> Result<Option<Outcome>, TaskError> {
    let db = lookup_db(session)?;
    let def = load_def(&db, ctx.task, DefAt::Latest)?;
    // Revocation only: an expired task may still read its outcomes.
    bound(def.as_deref(), ctx)?;
    let key = ledger::task_outcome_key(&db, ctx.task, op);
    ledger::lookup(&db, &key)?
        .map(|record| stored_outcome(&db, ctx.task, &record, View::Minimal))
        .transpose()
}

/// The inspector's outcome lookup (D-G): no revocation or expiry check, and a
/// committed outcome returns the full receipt with its change record. The
/// caller checks the inspection privilege. Never writes and takes no hold.
pub fn inspect_outcome(
    session: &SqlSession,
    task: TaskObjectId,
    op: &OperationId,
) -> Result<Option<Outcome>, TaskError> {
    let db = lookup_db(session)?;
    let key = ledger::task_outcome_key(&db, task, op);
    ledger::lookup(&db, &key)?
        .map(|record| stored_outcome(&db, task, &record, View::Full))
        .transpose()
}

/// The fingerprinted body of a Direct request.
fn direct_body<'r>(
    text: &'r str,
    params: &[Value],
    intent: Option<RowIntent>,
) -> Result<RequestBody<'r>, TaskError> {
    if text.len() > MAX_STATEMENT_TEXT || u16::try_from(params.len()).is_err() {
        return Err(TaskError::Core(Box::new(CoreError::Limit)));
    }
    let params = params
        .iter()
        .map(|value| crate::storage::task_row_payload(std::slice::from_ref(value)))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RequestBody::Direct { text: text.as_bytes(), params, intent })
}

/// Attempts per call: the first run and two internal retries (D-F).
pub(crate) const ATTEMPTS: usize = 3;
/// Mean backoff before the second and the third attempt, jittered by
/// plus or minus half (D-F: 2 ms, then 8 ms).
const BACKOFF_MS: [u64; ATTEMPTS - 1] = [2, 8];

/// What a call runs.
enum Work<'c> {
    Direct {
        statement: &'c Prepared,
        params: &'c [Value],
    },
    Apply {
        preview: PreviewId,
        source: &'c dyn ApplySource,
    },
}

/// What every attempt of one call shares.
struct Call<'c> {
    db: Arc<V2Db>,
    ctx: &'c TaskContext,
    op: &'c OperationId,
    work: Work<'c>,
    key: Vec<u8>,
    fp: [u8; 32],
    faults: Faults,
    check: Option<&'c RowCheck<'c>>,
}

/// A serialization failure or a deadlock: the attempt is retried in a new
/// transaction. Lock timeouts, cancellation, limits and ambiguous commits
/// are not.
fn retryable(error: &TaskError) -> bool {
    match error {
        TaskError::Retry | TaskError::Sql(SqlError::Conflict) => true,
        TaskError::Core(core) => matches!(
            **core,
            CoreError::Deadlock | CoreError::Sql(SqlError::Conflict)
        ),
        _ => false,
    }
}

/// Waits before retry number `retry` (1 or 2), checking for cancellation
/// before and after the sleep.
fn backoff(retry: usize) -> Result<(), TaskError> {
    crate::cancellation::check()?;
    let mean_us = BACKOFF_MS[retry.clamp(1, BACKOFF_MS.len()) - 1] * 1000;
    let mut noise = [0u8; 2];
    getrandom::fill(&mut noise).map_err(|e| CoreError::Storage(e.to_string()))?;
    let jitter = mean_us * u64::from(u16::from_be_bytes(noise)) / u64::from(u16::MAX);
    std::thread::sleep(Duration::from_micros(mean_us / 2 + jitter));
    crate::cancellation::check()?;
    Ok(())
}

pub(crate) fn commit_with(
    session: &mut SqlSession,
    actor: Actor<'_>,
    op: &OperationId,
    request: ProtectedRequest,
    load_def: &mut LoadDef<'_>,
) -> Result<Receipt, TaskError> {
    commit_sourced(session, actor, op, request, load_def, &mut stored_apply_source)
}

/// [`commit_with`] with the Apply source resolved by `sources`.
pub(crate) fn commit_sourced(
    session: &mut SqlSession,
    actor: Actor<'_>,
    op: &OperationId,
    request: ProtectedRequest,
    load_def: &mut LoadDef<'_>,
    sources: &mut Sources<'_>,
) -> Result<Receipt, TaskError> {
    commit_checked(session, actor, op, request, load_def, sources, None)
}

/// [`commit_sourced`] with an optional row-count check (an undo has none).
fn commit_checked(
    session: &mut SqlSession,
    actor: Actor<'_>,
    op: &OperationId,
    request: ProtectedRequest,
    load_def: &mut LoadDef<'_>,
    sources: &mut Sources<'_>,
    bound: Option<(RowIntent, &RowCheck<'_>)>,
) -> Result<Receipt, TaskError> {
    let check = bound.map(|(_, check)| check);
    // 0. v2 only (D9), not replicated (D13).
    let db = session.v2_db().ok_or_else(unsupported)?;
    if db.task_kernel_unsupported() {
        return Err(unsupported());
    }
    let (crash, point, hook) = session.fault_points()?;
    let faults = Faults { crash, point, hook };
    let ctx = match (actor, &request) {
        (Actor::Executor(ctx), _) => ctx,
        (Actor::Undo { role, task }, ProtectedRequest::Undo { preview, digest, mode }) => {
            let undo = UndoCall {
                db,
                role,
                task,
                preview: *preview,
                digest: *digest,
                mode: *mode,
                faults,
            };
            return commit_undo(session, undo, load_def);
        }
        (Actor::Undo { .. }, _) => return Err(unsupported()),
    };
    let task = ctx.task;
    // 1. Fingerprint.
    let body = match &request {
        ProtectedRequest::Direct { text, params, .. } => {
            direct_body(text, params, bound.map(|(intent, _)| intent))?
        }
        ProtectedRequest::Apply { preview, digest } => RequestBody::Apply {
            preview: preview.0,
            digest: *digest,
        },
        // The executor never undoes (D8).
        ProtectedRequest::Undo { .. } => return Err(unsupported()),
    };
    let fp = ledger::request_fingerprint(task, &body);
    let key = ledger::task_outcome_key(&db, task, op);
    // 2. Outcome lookup (never writes).
    if let Some(receipt) = replay(&db, task, &key, &fp, View::Minimal)? {
        return Ok(receipt);
    }
    faults.at(ProtectedPoint::AfterLookup);
    // 3. Latest committed definition.
    let def = load_def(&db, task, DefAt::Latest)?;
    standing(def.as_deref(), ctx, db.wall_now_ms())?;
    // The task's statement limits (R3-402): the hold's and the statement's
    // lock waits, its rows scanned and its pending writes.
    let limits = def.as_ref().map_or_else(
        || LoadLimits {
            lock_wait_ms: 0,
            ..LoadLimits::default()
        },
        |def| def.limits,
    );
    let source;
    let work = match &request {
        ProtectedRequest::Direct { statement, params, .. } => Work::Direct { statement, params },
        ProtectedRequest::Apply { preview, digest } => {
            source = sources(&db, ctx, *preview, digest)?;
            Work::Apply { preview: *preview, source: source.as_ref() }
        }
        ProtectedRequest::Undo { .. } => return Err(unsupported()),
    };
    let call = Call { db, ctx, op, work, key, fp, faults, check };
    // Steps 4-15, at most three times. Each attempt is a new transaction;
    // the hold is released between attempts.
    let mut retry = 0;
    loop {
        match attempt(session, &call, &limits, load_def) {
            Err(error) if retryable(&error) => {
                retry += 1;
                if retry >= ATTEMPTS {
                    return Err(TaskError::Retry);
                }
                backoff(retry)?;
            }
            other => return other,
        }
    }
}

/// One attempt, D-F steps 4-15. A recorded denial is committed here and
/// returned as its error.
fn attempt(
    session: &mut SqlSession,
    call: &Call<'_>,
    limits: &LoadLimits,
    load_def: &mut LoadDef<'_>,
) -> Result<Receipt, TaskError> {
    let Call { db, ctx, op, work, key, fp, faults, check } = call;
    let (ctx, fp) = (*ctx, *fp);
    let task = ctx.task;
    // 4. The transaction; 5. the hold, before any snapshot (D1).
    let mut txn = db.begin(Isolation::Serializable);
    let deadline = Instant::now().checked_add(Duration::from_millis(limits.lock_wait_ms));
    authority::hold(&txn, task, deadline, &|| crate::cancellation::check().is_err())
        .map_err(hold_error)?;
    faults.at(ProtectedPoint::AfterHold);
    // 6. A same-ID racer or a revoke may have committed while we waited.
    if let Some(receipt) = replay(db, task, key, &fp, View::Minimal)? {
        return Ok(receipt);
    }
    let latest = load_def(db, task, DefAt::Latest)?;
    standing(latest.as_deref(), ctx, db.wall_now_ms())?;
    let latest = latest.ok_or(TaskError::Revoked)?;
    // The denial mark: everything after it is discarded on a recorded
    // denial; the hold (a lock) survives the rollback.
    let mark = txn.savepoint();
    // 7. The statement, through the C1 seam; it never commits. An Apply
    // re-runs the stored statement with its pinned clock.
    let stored;
    let (statement, params, clock) = match work {
        Work::Direct { statement, params } => (*statement, *params, None),
        Work::Apply { source, .. } => {
            let ((statement, params), clock) = (source.statement(session)?, source.clock());
            stored = (statement, params, clock);
            (&stored.0, stored.1.as_slice(), stored.2.as_ref())
        }
    };
    // R3-303's column rules (design-3 section 2), before the statement runs:
    // no row is read or written, so no rows_affected, receipt or execution
    // error (22012, casts, overflow) can tell anything about a column the
    // task may not read. A denial is recorded (D-F "Denials").
    let latest_catalog = {
        let mut read = db.begin(Isolation::ReadCommitted);
        db.catalog().snapshot(&mut read)?
    };
    let image = latest_catalog.image().map_err(|e| TaskError::Core(Box::new(CoreError::Sql(e))))?;
    let ruled = columns::apply_in_catalog(
        &latest,
        ctx.bound_def_version,
        &latest_catalog.parts.column_labels,
        &image,
        statement,
    );
    let expanded = match ruled {
        Ok((expanded, _)) => expanded,
        Err(error @ TaskError::Denied(_)) => {
            let head = authority::load_head(&mut txn, task)?;
            return deny(call, txn, mark, &latest, head, error);
        }
        Err(error) => return Err(error),
    };
    drop((image, latest_catalog));
    // R3-402: the statement's writes are held to `pending_write_bytes`
    // (refused before staging), its scoped reads to `rows_scanned` and its
    // lock waits to `lock_wait_ms`. The records staged after it are not.
    super::limits::apply_to_txn(&mut txn, limits);
    let run = ProtectedRun { txn, effects: Effects::default() };
    // The statement runs bound to the executor's task, so it reads and
    // matches only the task's visible rows (R3-306); the session's own
    // binding, if any, is restored after.
    let bound = session.task.replace(ctx.clone());
    let metered = super::limits::metered(limits);
    let (run, result) = session.execute_protected_v2(db, run, &expanded, params, clock);
    drop(metered);
    session.task = bound;
    let ProtectedRun { mut txn, effects } = run;
    txn.set_write_limit(None);
    result.map_err(engine)?;
    faults.at(ProtectedPoint::AfterStatement);
    // 9. Rechecks at the statement snapshot (tracked reads).
    let def = load_def(db, task, DefAt::Snapshot(&mut txn))?;
    standing(def.as_deref(), ctx, db.wall_now_ms())?;
    let def = def.ok_or(TaskError::Revoked)?;
    let view = db.catalog().committed_view(&mut txn)?;
    let relation = view
        .by_id
        .get(&def.table)
        .map(|entry| entry.def.name.clone())
        .ok_or(TaskError::Denied(DenyRule::DefinitionChanged))?;
    let held = table_privileges_txn(&mut txn, &relation, &ctx.executor).map_err(role_error)?;
    if !held.contains(&TablePrivilege::Update) {
        return Err(TaskError::Denied(DenyRule::RolePrivilege));
    }
    let state = db
        .catalog()
        .snapshot(&mut txn)?
        .image()
        .map_err(|e| TaskError::Core(Box::new(CoreError::Sql(e))))?;
    let table = state
        .tables
        .values()
        .find(|table| table.id == def.table)
        .ok_or(TaskError::Denied(DenyRule::DefinitionChanged))?;
    // The column rules again at the statement snapshot, in case a label or
    // the table changed after the first check; a denial is recorded.
    if let Err(error) =
        columns::apply_in_catalog(&def, ctx.bound_def_version, &view.column_labels, &state, statement)
    {
        if !matches!(error, TaskError::Denied(_)) {
            return Err(error);
        }
        let head = authority::load_head(&mut txn, task)?;
        return deny(call, txn, mark, &def, head, error);
    }
    // 10. Every buffered row write is an effect and vice versa.
    crate::storage::cross_check_effects(&effects, &txn)?;
    // The caller's row bound on the real affected count, before anything is
    // charged or staged: a refusal drops the transaction (rows and hold) and
    // records nothing.
    if let Some(check) = check {
        check(effects.len())?;
    }
    // 11. Head (exact under the hold) and the membership charge.
    let head = authority::load_head(&mut txn, task)?;
    let seq = head.next_seq;
    let charge = authority::charge(&mut txn, task, seq, &effects)?;
    // The approval state of an Apply, read in the tracked transaction.
    let (kind, approval) = match work {
        Work::Direct { .. } => (RequestKind::Direct, ApprovalState::NotApplicable),
        Work::Apply { source, .. } => (
            RequestKind::Apply,
            source.approval_state(&mut txn, &effects).map_err(engine)?,
        ),
    };
    // 12. Decide. Denied and BudgetExceeded are recorded; the other refusals
    // leave the operation ID free.
    let decision = decide::evaluate(
        &state,
        &def,
        table,
        &head,
        &effects,
        &charge,
        kind,
        approval,
    );
    faults.at(ProtectedPoint::AfterDecision);
    let charge = match decision {
        Decision::Commit(charge) => charge,
        Decision::Denied(rule) => {
            return deny(call, txn, mark, &def, head, TaskError::Denied(rule));
        }
        Decision::BudgetExceeded(budget) => {
            return deny(call, txn, mark, &def, head, TaskError::BudgetExceeded(budget));
        }
        Decision::ApprovalRequired => return Err(TaskError::ApprovalRequired),
        Decision::ApprovalStale => return Err(TaskError::ApprovalStale),
    };
    // Keep the statement's writes; drop the undo log the mark kept.
    txn.release(mark).map_err(CoreError::from)?;
    // 13. Stage everything in the one write set.
    let decided_ms = db.wall_now_ms();
    let mut salt = [0u8; 16];
    getrandom::fill(&mut salt).map_err(|e| CoreError::Storage(e.to_string()))?;
    let (mut header, rows, digest) =
        receipt::build_change(task, seq, def.table, &effects, salt, decided_ms)?;
    header.outcome_key.copy_from_slice(&key[1..]);
    let put = |txn: &mut Transaction, key: Vec<u8>, value: Vec<u8>| -> Result<(), TaskError> {
        txn.put(Keyspace::Kernel, key, value).map_err(CoreError::from)?;
        Ok(())
    };
    put(&mut txn, change_header_key(task, seq), header.encode())?;
    for (row_no, row) in rows.iter().enumerate() {
        let row_no = u32::try_from(row_no).map_err(|_| CoreError::Limit)?;
        put(&mut txn, change_row_key(task, seq, row_no), row.encode())?;
    }
    let next = MutationHead {
        def_version: def.def_version,
        distinct_rows: head.distinct_rows.saturating_add(u64::from(charge.distinct_new)),
        transitions: head.transitions.saturating_add(u64::from(charge.transitions)),
        next_seq: seq.checked_add(1).ok_or(CoreError::Limit)?,
        last_decided_ms: decided_ms,
        // The applied preview no longer counts as open (the next preview's
        // sweep recomputes the count anyway).
        stored_previews: match work {
            Work::Direct { .. } => head.stored_previews,
            Work::Apply { .. } => head.stored_previews.saturating_sub(1),
        },
        ..head
    };
    let record = ReceiptRecord {
        decision: DECISION_COMMITTED,
        request_kind: match work {
            Work::Direct { .. } => ledger::KIND_DIRECT,
            Work::Apply { .. } => ledger::KIND_APPLY,
        },
        flags: 0,
        task,
        seq,
        def_version: def.def_version,
        catalog_version: txn.current_snapshot_ts().map_or(0, |ts| ts.0),
        decided_ms,
        request_fp: fp,
        effects_digest: digest,
        rows_affected: header.row_count,
        distinct_charged: charge.distinct_new,
        transitions_charged: charge.transitions,
        noop_rows: charge.noops,
        distinct_after: next.distinct_rows,
        transitions_after: next.transitions,
        preview: match work {
            Work::Direct { .. } => 0,
            Work::Apply { preview, .. } => preview.0,
        },
        undo_mode: 0,
        restored: 0,
        conflicts: 0,
        superseded: 0,
        op_id: op.as_bytes().to_vec(),
        issuer: def.issuer.clone(),
        executor: def.executor.clone(),
        approver: match work {
            Work::Direct { .. } => None,
            Work::Apply { source, .. } => source.approver(),
        },
    };
    put(&mut txn, receipt_key(task, seq), record.encode())?;
    // Phase 7: the preview applied and the approval consumed, in this batch.
    if let Work::Apply { source, .. } = work {
        source.stage_consumed(&mut txn, seq).map_err(engine)?;
    }
    let outcome = outcome_record(call, &def, decided_ms, OutcomePayload::Committed { task, seq });
    ledger::stage(&mut txn, key.clone(), &outcome)?;
    authority::stage_head(&mut txn, task, &next)?;
    // 14. One batch, synced; 15. the hold goes with the transaction.
    faults.before_commit()?;
    db.catalog()
        .commit_ddl(txn)
        .map_err(|e| engine(e.into()))?;
    faults.after_commit()?;
    Ok(Receipt::from_record(&record, None).minimal())
}

/// The ledger record for this call's operation ID (D-G retention).
fn outcome_record(
    call: &Call<'_>,
    def: &TaskDef,
    decided_ms: u64,
    payload: OutcomePayload,
) -> OutcomeRecord {
    identity_outcome(ledger::task_identity(call.ctx.task, call.op), call.fp, def, decided_ms, payload)
}

/// The ledger record of `identity`, kept until the outcome retention after
/// the later of the decision and the task's expiry.
fn identity_outcome(
    identity: Vec<u8>,
    fp: [u8; 32],
    def: &TaskDef,
    decided_ms: u64,
    payload: OutcomePayload,
) -> OutcomeRecord {
    let expires = u64::try_from(def.expires_at.0).unwrap_or(0);
    OutcomeRecord {
        flags: 0,
        identity,
        request_fp: fp,
        accepted_fp: fp,
        decided_lower_ms: decided_ms,
        decided_upper_ms: decided_ms,
        retain_until_ms: decided_ms.max(expires).saturating_add(OUTCOME_RETENTION_MS),
        payload,
    }
}

/// The durable denial (D-F "Denials"): discard the statement and the
/// membership charge back to `mark`, keep the hold, restart the snapshot so
/// the statement's read set cannot fail the commit, then commit only the
/// outcome (kind 3 or 4) and the head with `denials + 1`. No rows,
/// membership, change record or receipt are written. Returns `error`.
fn deny(
    call: &Call<'_>,
    mut txn: Transaction,
    mark: Savepoint,
    def: &TaskDef,
    head: MutationHead,
    error: TaskError,
) -> Result<Receipt, TaskError> {
    let task = call.ctx.task;
    let txn_error = |e: TxnError| engine(e.into());
    txn.rollback_to(&mark).map_err(txn_error)?;
    txn.release(mark).map_err(txn_error)?;
    txn.restart_snapshot().map_err(txn_error)?;
    call.faults.at(ProtectedPoint::AfterDenialRollback);
    let decided_ms = call.db.wall_now_ms();
    let denial_no = head.denials.checked_add(1).ok_or(CoreError::Limit)?;
    let payload = match &error {
        TaskError::Denied(rule) => OutcomePayload::Denied { task, rule: rule.code(), denial_no },
        TaskError::BudgetExceeded(budget) => {
            OutcomePayload::BudgetExceeded { task, budget: budget.code(), denial_no }
        }
        _ => return Err(corrupt("task denial of another kind")),
    };
    let outcome = outcome_record(call, def, decided_ms, payload);
    ledger::stage(&mut txn, call.key.clone(), &outcome)?;
    let next = MutationHead {
        def_version: def.def_version,
        denials: denial_no,
        last_decided_ms: decided_ms,
        ..head
    };
    authority::stage_head(&mut txn, task, &next)?;
    call.faults.at(ProtectedPoint::AfterDenialStage);
    call.faults.before_commit()?;
    call.db.catalog().commit_ddl(txn).map_err(txn_error)?;
    call.faults.after_commit()?;
    Err(error)
}

/// What every attempt of one undo call shares.
struct UndoCall<'c> {
    db: Arc<V2Db>,
    role: &'c RoleName,
    task: TaskObjectId,
    preview: PreviewId,
    digest: [u8; 32],
    mode: UndoMode,
    faults: Faults,
}

/// 42704 undefined_object: the task does not exist.
fn no_task() -> TaskError {
    CoreError::Sql(SqlError::UndefinedObject).into()
}

/// The undo path of [`protected_commit`] (D8). Steps: not in a task
/// session; the task (42704) and the role's privileges (42501) at the
/// latest committed state; the outcome lookup under `03 | task | preview`
/// (same request: the stored full receipt, another: `OperationConflict`);
/// the pre-check (`ApprovalStale`, `Denied(UndoHasConflicts)`, neither
/// recorded); then [`attempt_undo`] at most three times.
fn commit_undo(
    session: &mut SqlSession,
    call: UndoCall<'_>,
    load_def: &mut LoadDef<'_>,
) -> Result<Receipt, TaskError> {
    if session.task().is_some() {
        return Err(TaskError::NotAllowedInTaskSession(columns::StatementClass::Other));
    }
    let UndoCall { db, role, task, preview, digest, mode, faults } = &call;
    let def = load_def(db, *task, DefAt::Latest)?.ok_or_else(no_task)?;
    undo::may_undo(db, &def, &mut db.begin(Isolation::ReadCommitted), role)?;
    let fp = undo_fingerprint(&call);
    let key = ledger::undo_outcome_key(db, *task, preview.0);
    if let Some(receipt) = replay(db, *task, &key, &fp, View::Full)? {
        return Ok(receipt);
    }
    faults.at(ProtectedPoint::AfterLookup);
    undo::undo_precheck(db, *task, *preview, digest, *mode)?;
    let lock_wait = def.limits.lock_wait_ms;
    let mut retry = 0;
    loop {
        match attempt_undo(session, &call, &key, fp, lock_wait, load_def) {
            Err(error) if retryable(&error) => {
                retry += 1;
                if retry >= ATTEMPTS {
                    return Err(TaskError::Retry);
                }
                backoff(retry)?;
            }
            other => return other,
        }
    }
}

/// `sha256("GDTASKREQ\x01" ‖ task ‖ 03 ‖ preview ‖ digest ‖ mode)`.
fn undo_fingerprint(call: &UndoCall<'_>) -> [u8; 32] {
    let body = RequestBody::Undo {
        preview: call.preview.0,
        digest: call.digest,
        mode: call.mode as u8,
    };
    ledger::request_fingerprint(call.task, &body)
}

/// One undo attempt: the hold, the outcome lookup again, then
/// [`undo::undo_stage`] (recompute and compare with the restores applied),
/// and in the one write set the kind-2 change record (its rows the restored
/// rows with their pre-restore values), the receipt with the undo counts,
/// the preview applied, the outcome and the head (`next_seq + 1`, the
/// decision time and one stored preview fewer; membership, the distinct-row
/// and transition counts and the denials stay as they are).
fn attempt_undo(
    session: &mut SqlSession,
    call: &UndoCall<'_>,
    key: &[u8],
    fp: [u8; 32],
    lock_wait: u64,
    load_def: &mut LoadDef<'_>,
) -> Result<Receipt, TaskError> {
    let UndoCall { db, role, task, preview, digest, mode, faults } = call;
    let task = *task;
    let txn = db.begin(Isolation::Serializable);
    let deadline = Instant::now().checked_add(Duration::from_millis(lock_wait));
    authority::hold(&txn, task, deadline, &|| crate::cancellation::check().is_err())
        .map_err(hold_error)?;
    faults.at(ProtectedPoint::AfterHold);
    if let Some(receipt) = replay(db, task, key, &fp, View::Full)? {
        return Ok(receipt);
    }
    let def = load_def(db, task, DefAt::Latest)?.ok_or_else(no_task)?;
    let UndoStaged { run, header: stored } =
        undo::undo_stage(session, db, &def, role, *preview, digest, *mode, txn)?;
    faults.at(ProtectedPoint::AfterStatement);
    let ProtectedRun { mut txn, effects } = run;
    crate::storage::cross_check_effects(&effects, &txn)?;
    let head = authority::load_head(&mut txn, task)?;
    let seq = head.next_seq;
    let decided_ms = db.wall_now_ms();
    let mut salt = [0u8; 16];
    getrandom::fill(&mut salt).map_err(|e| CoreError::Storage(e.to_string()))?;
    let (mut header, rows, effects_digest) =
        receipt::build_change(task, seq, def.table, &effects, salt, decided_ms)?;
    header.kind = undo::KIND_UNDO;
    header.outcome_key.copy_from_slice(&key[1..]);
    let put = |txn: &mut Transaction, key: Vec<u8>, value: Vec<u8>| -> Result<(), TaskError> {
        txn.put(Keyspace::Kernel, key, value).map_err(CoreError::from)?;
        Ok(())
    };
    put(&mut txn, change_header_key(task, seq), header.encode())?;
    for (row_no, row) in rows.iter().enumerate() {
        let row_no = u32::try_from(row_no).map_err(|_| CoreError::Limit)?;
        put(&mut txn, change_row_key(task, seq, row_no), row.encode())?;
    }
    let [_, conflicts, superseded, _] = stored.counts;
    undo::stage_applied(&mut txn, task, *preview, stored, seq)?;
    let next = MutationHead {
        next_seq: seq.checked_add(1).ok_or(CoreError::Limit)?,
        last_decided_ms: decided_ms,
        stored_previews: head.stored_previews.saturating_sub(1),
        ..head
    };
    let record = ReceiptRecord {
        decision: match mode {
            UndoMode::Complete => DECISION_UNDO_COMPLETE,
            UndoMode::Partial => DECISION_UNDO_PARTIAL,
        },
        request_kind: ledger::KIND_UNDO,
        flags: 0,
        task,
        seq,
        def_version: def.def_version,
        catalog_version: txn.current_snapshot_ts().map_or(0, |ts| ts.0),
        decided_ms,
        request_fp: fp,
        effects_digest,
        rows_affected: header.row_count,
        distinct_charged: 0,
        transitions_charged: 0,
        noop_rows: 0,
        distinct_after: head.distinct_rows,
        transitions_after: head.transitions,
        preview: preview.0,
        undo_mode: *mode as u8,
        restored: header.row_count,
        conflicts,
        superseded,
        op_id: undo::undo_operation_id(*preview).as_bytes().to_vec(),
        issuer: def.issuer.clone(),
        executor: def.executor.clone(),
        approver: Some(role.to_string()),
    };
    put(&mut txn, receipt_key(task, seq), record.encode())?;
    let identity = ledger::undo_identity(task, preview.0);
    let payload = OutcomePayload::Committed { task, seq };
    let outcome = identity_outcome(identity, fp, &def, decided_ms, payload);
    ledger::stage(&mut txn, key.to_vec(), &outcome)?;
    authority::stage_head(&mut txn, task, &next)?;
    faults.before_commit()?;
    db.catalog()
        .commit_ddl(txn)
        .map_err(|e| engine(e.into()))?;
    faults.after_commit()?;
    Ok(Receipt::from_record(&record, Some(ChangeRecord { header, rows })))
}

impl crate::storage::Admin {
    /// For tests: pins the database's wall clock (task expiry, receipt and
    /// preview times, retention) at `ms` Unix milliseconds; `None` returns
    /// to the system clock (v2 only).
    #[doc(hidden)]
    pub fn set_task_wall_clock(&self, ms: Option<u64>) -> Result<(), CoreError> {
        let db = self.sql_session().v2_db().ok_or(CoreError::Unsupported)?;
        match ms {
            Some(ms) => db.set_wall_clock(Arc::new(crate::storage::engine::commit::ManualClock::new(ms))),
            None => db.set_wall_clock(Arc::new(crate::storage::engine::commit::SystemClock)),
        }
        Ok(())
    }
}

#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "commit_tests.rs"]
mod commit_tests;

#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "outcome_tests.rs"]
mod outcome_tests;

#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "commit_apply_seam_tests.rs"]
mod commit_apply_seam_tests;

#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "commit_race_tests.rs"]
mod commit_race_tests;
