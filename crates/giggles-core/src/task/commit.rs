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
//! `Apply` and `Undo` (and `Actor::Undo`) are `Unsupported` until R3-618.
use super::authority;
use super::context::TaskContext;
use super::decide::{self, ApprovalState, Decision, RequestKind};
use super::definition::{RoleName, TaskDef};
use super::effects::Effects;
use super::error::{Budget, DenyRule, TaskError};
use super::ledger::{self, OperationId, RequestBody};
use super::preview::PreviewId;
use super::receipt::{self, Receipt, DECISION_COMMITTED};
use super::records::{
    change_header_key, change_row_key, receipt_key, MutationHead, OutcomePayload, OutcomeRecord,
    ReceiptRecord, TaskObjectId, MAX_STATEMENT_TEXT,
};
use super::undo::UndoMode;
use crate::relational::{Prepared, SqlError, Value};
use crate::storage::engine::meta::REPLICATED;
use crate::storage::engine::txn::{Savepoint, TxnError};
use crate::storage::roles::{table_privileges_txn, RoleError, TablePrivilege};
use crate::storage::{Isolation, Keyspace, ProtectedRun, SqlSession, Transaction, V2Db};
use crate::CoreError;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// How long an operation's outcome is kept after the later of its decision
/// and the task's expiry (D5 default; a server setting later).
pub(crate) const OUTCOME_RETENTION_MS: u64 = 30 * 24 * 60 * 60 * 1000;

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

fn unsupported() -> TaskError {
    TaskError::Core(Box::new(CoreError::Unsupported))
}

fn corrupt(message: &str) -> TaskError {
    TaskError::Core(Box::new(CoreError::Corrupt(message.into())))
}

/// Engine errors: a serialization failure or a deadlock is `Retry`, which
/// the attempt loop retries; everything else (55P03, 57014, limits, 08007)
/// passes through unchanged.
fn engine(error: CoreError) -> TaskError {
    match error {
        CoreError::Sql(SqlError::Conflict) | CoreError::Deadlock => TaskError::Retry,
        other => other.into(),
    }
}

/// The hold's lock wait ran past the task's `lock_wait_ms`: 55P03
/// lock_not_available, as `LOCK ... NOWAIT` reports it (not 53300 `Busy`).
fn hold_error(error: TxnError) -> TaskError {
    match error {
        TxnError::LockTimeout => CoreError::LockNotAvailable.into(),
        other => engine(other.into()),
    }
}

fn role_error(error: RoleError) -> TaskError {
    match error {
        RoleError::Core(core) => engine(core),
        _ => TaskError::Denied(DenyRule::RolePrivilege),
    }
}

/// Revoked: missing, revoked, another task, another executor or a newer
/// definition (R3 has no ALTER TASK). Expired: `now >= expires_at`.
fn standing(def: Option<&TaskDef>, ctx: &TaskContext, now_ms: u64) -> Result<(), TaskError> {
    let def = def
        .filter(|def| {
            !def.revoked
                && def.id == ctx.task
                && def.def_version == ctx.bound_def_version
                && def.executor == ctx.executor
        })
        .ok_or(TaskError::Revoked)?;
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
) -> Result<Option<Receipt>, TaskError> {
    let Some(record) = ledger::lookup(db, key)? else {
        return Ok(None);
    };
    if &record.request_fp != fp {
        return Err(TaskError::OperationConflict);
    }
    match record.payload {
        OutcomePayload::Committed { task: stored, seq } if stored == task => {
            let txn = db.begin(Isolation::ReadCommitted);
            let value = txn
                .kernel_latest_committed(&receipt_key(task, seq))
                .map_err(CoreError::from)?
                .ok_or_else(|| corrupt("task outcome without receipt"))?;
            drop(txn);
            let stored = ReceiptRecord::decode(&value)?;
            Ok(Some(Receipt::from_record(&stored, None).minimal()))
        }
        OutcomePayload::Denied { task: stored, rule, .. } if stored == task => Err(TaskError::Denied(
            DenyRule::from_code(rule).ok_or_else(|| corrupt("task outcome deny rule"))?,
        )),
        OutcomePayload::BudgetExceeded { task: stored, budget, .. } if stored == task => {
            Err(TaskError::BudgetExceeded(
                Budget::from_code(budget).ok_or_else(|| corrupt("task outcome budget"))?,
            ))
        }
        _ => Err(corrupt("task outcome of another kind or task")),
    }
}

/// The fingerprinted body of a Direct request.
fn direct_body<'r>(text: &'r str, params: &[Value]) -> Result<RequestBody<'r>, TaskError> {
    if text.len() > MAX_STATEMENT_TEXT || u16::try_from(params.len()).is_err() {
        return Err(TaskError::Core(Box::new(CoreError::Limit)));
    }
    let params = params
        .iter()
        .map(|value| crate::storage::task_row_payload(std::slice::from_ref(value)))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RequestBody::Direct { text: text.as_bytes(), params })
}

/// Attempts per call: the first run and two internal retries (D-F).
pub(crate) const ATTEMPTS: usize = 3;
/// Mean backoff before the second and the third attempt, jittered by
/// plus or minus half (D-F: 2 ms, then 8 ms).
const BACKOFF_MS: [u64; ATTEMPTS - 1] = [2, 8];

/// What every attempt of one call shares.
struct Call<'c> {
    db: Arc<V2Db>,
    ctx: &'c TaskContext,
    op: &'c OperationId,
    statement: &'c Prepared,
    params: &'c [Value],
    key: Vec<u8>,
    fp: [u8; 32],
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
    // 0. v2 only (D9), not replicated (D13).
    let db = session.v2_db().ok_or_else(unsupported)?;
    if db.meta.features & REPLICATED != 0 {
        return Err(unsupported());
    }
    let ctx = match actor {
        Actor::Executor(ctx) => ctx,
        Actor::Undo { .. } => return Err(unsupported()),
    };
    let ProtectedRequest::Direct { statement, params, text } = &request else {
        return Err(unsupported());
    };
    let task = ctx.task;
    // 1. Fingerprint.
    let fp = ledger::request_fingerprint(task, &direct_body(text, params)?);
    let key = ledger::task_outcome_key(&db, task, op);
    // 2. Outcome lookup (never writes).
    if let Some(receipt) = replay(&db, task, &key, &fp)? {
        return Ok(receipt);
    }
    // 3. Latest committed definition.
    let def = load_def(&db, task, DefAt::Latest)?;
    standing(def.as_deref(), ctx, db.wall_now_ms())?;
    let lock_wait = def.as_ref().map_or(0, |def| def.limits.lock_wait_ms);
    let call = Call { db, ctx, op, statement, params, key, fp };
    // Steps 4-15, at most three times. Each attempt is a new transaction;
    // the hold is released between attempts.
    let mut retry = 0;
    loop {
        match attempt(session, &call, lock_wait, load_def) {
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
    lock_wait: u64,
    load_def: &mut LoadDef<'_>,
) -> Result<Receipt, TaskError> {
    let Call { db, ctx, op, statement, params, key, fp } = call;
    let (ctx, fp) = (*ctx, *fp);
    let task = ctx.task;
    // 4. The transaction; 5. the hold, before any snapshot (D1).
    let mut txn = db.begin(Isolation::Serializable);
    let deadline = Instant::now().checked_add(Duration::from_millis(lock_wait));
    authority::hold(&txn, task, deadline, &|| crate::cancellation::check().is_err())
        .map_err(hold_error)?;
    // 6. A same-ID racer or a revoke may have committed while we waited.
    if let Some(receipt) = replay(db, task, key, &fp)? {
        return Ok(receipt);
    }
    standing(load_def(db, task, DefAt::Latest)?.as_deref(), ctx, db.wall_now_ms())?;
    // The denial mark: everything after it is discarded on a recorded
    // denial; the hold (a lock) survives the rollback.
    let mark = txn.savepoint();
    // 7. The statement, through the C1 seam; it never commits.
    let run = ProtectedRun { txn, effects: Effects::default() };
    let (run, result) = session.execute_protected_v2(db, run, statement, params, None);
    let ProtectedRun { mut txn, effects } = run;
    result.map_err(engine)?;
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
    // 10. Every buffered row write is an effect and vice versa.
    crate::storage::cross_check_effects(&effects, &txn)?;
    // 11. Head (exact under the hold) and the membership charge.
    let head = authority::load_head(&mut txn, task)?;
    let seq = head.next_seq;
    let charge = authority::charge(&mut txn, task, seq, &effects)?;
    // 12. Decide. Denied and BudgetExceeded are recorded; the other refusals
    // leave the operation ID free.
    let charge = match decide::evaluate(
        &state,
        &def,
        table,
        &head,
        &effects,
        &charge,
        RequestKind::Direct,
        ApprovalState::NotApplicable,
    ) {
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
        ..head
    };
    let record = ReceiptRecord {
        decision: DECISION_COMMITTED,
        request_kind: ledger::KIND_DIRECT,
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
        preview: 0,
        undo_mode: 0,
        restored: 0,
        conflicts: 0,
        superseded: 0,
        op_id: op.as_bytes().to_vec(),
        issuer: def.issuer.clone(),
        executor: def.executor.clone(),
        approver: None,
    };
    put(&mut txn, receipt_key(task, seq), record.encode())?;
    let outcome = outcome_record(call, &def, decided_ms, OutcomePayload::Committed { task, seq });
    ledger::stage(&mut txn, key.clone(), &outcome)?;
    authority::stage_head(&mut txn, task, &next)?;
    // 14. One batch, synced; 15. the hold goes with the transaction.
    db.catalog()
        .commit_ddl(txn)
        .map_err(|e| engine(e.into()))?;
    Ok(Receipt::from_record(&record, None).minimal())
}

/// The ledger record for this call's operation ID (D-G retention).
fn outcome_record(
    call: &Call<'_>,
    def: &TaskDef,
    decided_ms: u64,
    payload: OutcomePayload,
) -> OutcomeRecord {
    let expires = u64::try_from(def.expires_at.0).unwrap_or(0);
    OutcomeRecord {
        flags: 0,
        identity: ledger::task_identity(call.ctx.task, call.op),
        request_fp: call.fp,
        accepted_fp: call.fp,
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
    call.db.catalog().commit_ddl(txn).map_err(txn_error)?;
    Err(error)
}

#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "commit_tests.rs"]
mod commit_tests;
