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
//! A decision other than `Commit` is returned as its error and nothing is
//! recorded yet (durable denials come later). A serialization failure or a
//! deadlock is `TaskError::Retry` (40001); the bounded internal retry is a
//! later item. `Apply` and `Undo` (and `Actor::Undo`) are `Unsupported`
//! until R3-618.
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

/// Engine errors: a serialization failure or a deadlock is 40001 (`Retry`);
/// everything else (55P03, 57014, limits, 08007) passes through unchanged.
fn engine(error: CoreError) -> TaskError {
    match error {
        CoreError::Sql(SqlError::Conflict) | CoreError::Deadlock => TaskError::Retry,
        other => other.into(),
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
    // 4. The transaction; 5. the hold, before any snapshot (D1).
    let txn = db.begin(Isolation::Serializable);
    let deadline = Instant::now().checked_add(Duration::from_millis(lock_wait));
    authority::hold(&txn, task, deadline, &|| false).map_err(|e| engine(e.into()))?;
    // 6. A same-ID racer or a revoke may have committed while we waited.
    if let Some(receipt) = replay(&db, task, &key, &fp)? {
        return Ok(receipt);
    }
    standing(load_def(&db, task, DefAt::Latest)?.as_deref(), ctx, db.wall_now_ms())?;
    // 7. The statement, through the C1 seam; it never commits.
    let run = ProtectedRun { txn, effects: Effects::default() };
    let (run, result) = session.execute_protected_v2(&db, run, statement, params, None);
    let ProtectedRun { mut txn, effects } = run;
    result.map_err(engine)?;
    // 9. Rechecks at the statement snapshot (tracked reads).
    let def = load_def(&db, task, DefAt::Snapshot(&mut txn))?;
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
    // 12. Decide.
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
        Decision::Denied(rule) => return Err(TaskError::Denied(rule)),
        Decision::BudgetExceeded(budget) => return Err(TaskError::BudgetExceeded(budget)),
        Decision::ApprovalRequired => return Err(TaskError::ApprovalRequired),
        Decision::ApprovalStale => return Err(TaskError::ApprovalStale),
    };
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
    let expires = u64::try_from(def.expires_at.0).unwrap_or(0);
    let outcome = OutcomeRecord {
        flags: 0,
        identity: ledger::task_identity(task, op),
        request_fp: fp,
        accepted_fp: fp,
        decided_lower_ms: decided_ms,
        decided_upper_ms: decided_ms,
        retain_until_ms: decided_ms.max(expires).saturating_add(OUTCOME_RETENTION_MS),
        payload: OutcomePayload::Committed { task, seq },
    };
    ledger::stage(&mut txn, key, &outcome)?;
    authority::stage_head(&mut txn, task, &next)?;
    // 14. One batch, synced; 15. the hold goes with the transaction.
    db.catalog()
        .commit_ddl(txn)
        .map_err(|e| engine(e.into()))?;
    Ok(Receipt::from_record(&record, None).minimal())
}

#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "commit_tests.rs"]
mod commit_tests;
