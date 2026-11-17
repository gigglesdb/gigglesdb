//! Undo (GD-0061 design D8): the per-row history planner (R3-801) and the
//! undo preview (R3-802).
//!
//! [`classify`] is the pure part of `plan`: it walks a task's change-record
//! rows and sorts every touched row into settled, superseded, conflict or a
//! restore candidate. [`undo_preview`] reads the history and the current
//! rows, dry-runs each candidate through the Phase 6 protected seam as the
//! native point update with its `expected_version` (one savepoint each, in
//! ascending `(table, pk)`; earlier restores stay in place, so a restore that
//! would succeed only after a later one is a conflict), rolls everything
//! back and stores a kind-02 preview through the change preview's T2.
//!
//! R3-803 adds the commit: [`undo_approve`] runs `ProtectedRequest::Undo`
//! through `protected_commit` as `Actor::Undo`, whose undo path calls
//! [`undo_precheck`] (no write), then, under the hold, [`undo_stage`]
//! (recompute and compare) and stages a kind-2 change record, a receipt
//! with the undo counts and the outcome under identity `03 | task |
//! preview`. Membership and the caps are never touched.
use super::admit::StatementClass;
use super::commit::{engine, protected_commit, role_error, unsupported, Actor, ProtectedRequest};
use super::definition::{RoleName, TableId, TaskDef, TaskName, TaskPrivileges};
use super::effects::Effects;
use super::error::{DenyRule, TaskError};
use super::ledger::{self, OperationId};
use super::preview::{
    encode_after, schema_fp, store_preview, stored_rows, PreviewClass, PreviewId, PreviewKind,
    Recheck, MAX_PREVIEW_ROWS, NO_CLOCK_PIN, STATE_APPLIED, STATE_OPEN,
};
use super::receipt::Receipt;
use super::records::{
    preview_key, preview_row_key, task_prefix, ChangeHeader, ChangeRow, OutcomePayload,
    PreviewHeader, PreviewRow, PreviewScope, TaskObjectId, NS_CHANGE_HEADER, NS_CHANGE_ROW,
    NS_OUTCOME, ROW_BEFORE_BLANKED,
};
use crate::relational::{SqlError, TableDef, Value};
use crate::storage::engine::{codec, kv::Direction};
use crate::storage::roles::{role_txn, table_owner_txn, table_privileges_txn, TablePrivilege};
use crate::storage::{Isolation, Keyspace, ProtectedRun, SqlSession, Transaction, V2Db};
use crate::CoreError;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

/// Undo scope: the whole task, or one committed operation of it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UndoScope {
    Task,
    Operation(OperationId),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum UndoMode {
    Complete = 1,
    Partial = 2,
}

/// Stored as the class-01 row's `aux`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ConflictReason {
    ChangedSince = 1,
    RowMissing = 2,
    Constraint = 3,
    BeforeValuesExpired = 4,
    SchemaChanged = 5,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UndoSummary {
    pub id: PreviewId,
    pub digest: [u8; 32],
    pub restorable: u32,
    pub conflicts: u32,
    pub superseded: u32,
    pub settled: u32,
    /// Wall clock, milliseconds.
    pub expires_at: u64,
}

/// Change-record kinds (`24` header `kind`, design-0): 01 change (a direct
/// write or an applied preview), 02 undo.
pub(crate) const KIND_CHANGE: u8 = 1;
pub(crate) const KIND_UNDO: u8 = 2;

/// One `25` row with the `seq` from its key and the `kind` from its `24` header.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HistoryWrite {
    pub(crate) seq: u64,
    pub(crate) kind: u8,
    pub(crate) row: ChangeRow,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Candidate {
    pub(crate) table: TableId,
    pub(crate) pk: Vec<u8>,
    /// The current version; the restore runs with this `expected_version`.
    pub(crate) expect: u64,
    /// The before-values to restore.
    pub(crate) restore: Vec<Value>,
    /// Column offsets where `restore` differs from the current values.
    pub(crate) sets: Vec<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Conflict {
    pub(crate) table: TableId,
    pub(crate) pk: Vec<u8>,
    /// The current version, or 0 when the row is missing.
    pub(crate) version_read: u64,
    pub(crate) reason: ConflictReason,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Superseded {
    pub(crate) table: TableId,
    pub(crate) pk: Vec<u8>,
    /// `version_written` of the superseded write.
    pub(crate) version_read: u64,
    pub(crate) seq: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Plan {
    /// Ascending `(table, pk)`.
    pub(crate) candidates: Vec<Candidate>,
    pub(crate) conflicts: Vec<Conflict>,
    pub(crate) superseded: Vec<Superseded>,
    pub(crate) settled: u32,
}

/// Reads a row's current `(version, values)` by `(table, pk)`.
pub(crate) type CurrentRow<'a> = dyn Fn(TableId, &[u8]) -> Option<(u64, Vec<Value>)> + 'a;
/// Decodes a write's before-values against the current table definition.
pub(crate) type DecodeBefore<'a> = dyn Fn(&ChangeRow) -> Option<Vec<Value>> + 'a;

/// The D8 `plan` up to and including the candidates. `writes` are the task's
/// `25` rows in any order (they are ordered by `seq` here, stably, so rows of
/// one record keep their `row_no` order). `scope_seq` is `Some(seq of the
/// operation's receipt)` for operation scope. `current` returns the row's
/// current `(version, values)`; `decode` returns the before-values resized to
/// the current table definition, or `None` when they no longer fit it.
/// Versions are compared for equality only.
pub(crate) fn classify(
    writes: &[HistoryWrite],
    scope_seq: Option<u64>,
    current: &CurrentRow<'_>,
    decode: &DecodeBefore<'_>,
) -> Plan {
    let mut ordered: Vec<&HistoryWrite> = writes
        .iter()
        .filter(|w| scope_seq.is_none_or(|s| w.seq == s))
        .collect();
    ordered.sort_by_key(|w| w.seq);
    let mut by_row: BTreeMap<(TableId, Vec<u8>), Vec<&HistoryWrite>> = BTreeMap::new();
    for w in ordered {
        by_row.entry((w.row.table, w.row.pk.clone())).or_default().push(w);
    }
    let mut plan = Plan::default();
    for ((table, pk), mut history) in by_row {
        if scope_seq.is_none()
            && let Some(j) = history.iter().rposition(|h| h.kind == KIND_UNDO)
        {
            plan.settled += (j + 1) as u32;
            history.drain(..=j);
            if history.is_empty() {
                continue;
            }
        }
        let n = history.len();
        let mut i = n - 1;
        while i > 0 && history[i].row.version_before == history[i - 1].row.version_written {
            i -= 1;
        }
        for h in &history[..i] {
            plan.superseded.push(Superseded {
                table,
                pk: pk.clone(),
                version_read: h.row.version_written,
                seq: h.seq,
            });
        }
        let first = &history[i].row;
        let last = &history[n - 1].row;
        let conflict = |reason, version_read| Conflict { table, pk: pk.clone(), version_read, reason };
        let Some((version, values)) = current(table, &pk) else {
            plan.conflicts.push(conflict(ConflictReason::RowMissing, 0));
            continue;
        };
        if version != last.version_written {
            plan.conflicts.push(conflict(ConflictReason::ChangedSince, version));
            continue;
        }
        if first.flags & ROW_BEFORE_BLANKED != 0 || first.before.is_empty() {
            plan.conflicts.push(conflict(ConflictReason::BeforeValuesExpired, version));
            continue;
        }
        let Some(restore) = decode(first) else {
            plan.conflicts.push(conflict(ConflictReason::SchemaChanged, version));
            continue;
        };
        if restore.len() != values.len() {
            plan.conflicts.push(conflict(ConflictReason::SchemaChanged, version));
            continue;
        }
        if restore == values {
            plan.settled += 1;
            continue;
        }
        let sets = (0..restore.len()).filter(|&c| restore[c] != values[c]).collect();
        plan.candidates.push(Candidate { table, pk, expect: version, restore, sets });
    }
    plan
}

/// PREVIEW UNDO OF TASK `task` [OPERATION `op`] as `role` (D8).
///
/// Checks, in order: not in a task session (0A000), v2 and not replicated
/// (0A000), the task exists (42704), `role` holds the undo privilege on it
/// and SELECT and UPDATE on its table (superuser, owner or grant; 42501).
/// Operation scope resolves the ID through the `22` ledger and needs a
/// committed outcome of this task (42704 otherwise): an executor's ID
/// first, then, for an ID of the form `undo:<id>`, the undo commit of
/// stored undo preview `<id>` (identity kind 03), which is how an undo is
/// itself undone. Neither revocation nor
/// expiry stops an undo.
///
/// The plan runs in a Serializable transaction that is always rolled back.
/// A restore that fails with a class-23 error, or whose run touched any row
/// other than its own, is a `Constraint` conflict; any other error is
/// returned. Before-values blanked by retention are `BeforeValuesExpired`
/// conflicts. The preview is then stored as kind 02 (`counts` = restorable,
/// conflicts, superseded, settled; `creator` = `role`; no text, parameters
/// or clock pin) under the same `stored_previews` limit as change previews.
/// Its expiry is `now + preview_ttl`, not capped at the task's expiry.
pub fn undo_preview(
    session: &mut SqlSession,
    role: &RoleName,
    task: &TaskName,
    scope: UndoScope,
) -> Result<UndoSummary, TaskError> {
    if session.task().is_some() {
        return Err(TaskError::NotAllowedInTaskSession(StatementClass::Other));
    }
    let db = session.v2_db().ok_or_else(unsupported)?;
    if db.task_kernel_unsupported() {
        return Err(unsupported());
    }
    let def = session
        .tasks()
        .lookup(task.as_str())?
        .ok_or(CoreError::Sql(SqlError::UndefinedObject))?;
    may_undo(&db, &def, &mut db.begin(Isolation::ReadCommitted), role)?;
    // The operation's committed seq, through the 22 ledger.
    let (scope_seq, scope) = match &scope {
        UndoScope::Task => (None, PreviewScope { kind: 0, outcome: [0; 32] }),
        UndoScope::Operation(op) => {
            // An executor's operation first; then an undo, named `undo:<id>`.
            let mut key = ledger::task_outcome_key(&db, def.id, op);
            let mut record = ledger::lookup(&db, &key)?;
            if record.is_none()
                && let Some(id) = parse_undo_operation(op)
            {
                key = ledger::undo_outcome_key(&db, def.id, id.0);
                record = ledger::lookup(&db, &key)?;
            }
            let seq = match record.map(|record| record.payload) {
                Some(OutcomePayload::Committed { task, seq }) if task == def.id => seq,
                _ => return Err(CoreError::Sql(SqlError::UndefinedObject).into()),
            };
            let mut outcome = [0u8; 32];
            outcome.copy_from_slice(&key[1..33]);
            (Some(seq), PreviewScope { kind: 1, outcome })
        }
    };
    let dry = dry_run(session, &db, &def, scope_seq)?;
    let header = PreviewHeader {
        kind: PreviewKind::Undo as u8,
        state: STATE_OPEN,
        created_ms: 0,
        expires_ms: 0,
        def_version: def.def_version,
        table: def.table,
        schema_fp: dry.schema_fp,
        clock_pin: NO_CLOCK_PIN,
        digest: [0; 32],
        counts: dry.counts,
        applied_seq: 0,
        creator: role.clone(),
        scope: Some(scope),
        surface: 0,
        text: String::new(),
        params: encode_after(&[])?,
    };
    // Under the hold: the task and the role's privileges again, and the
    // definition the plan was made on.
    let mut recheck = |txn: &mut Transaction, current: Option<Arc<TaskDef>>, _now: u64| {
        let current = current.ok_or(CoreError::Sql(SqlError::UndefinedObject))?;
        may_undo(&db, &current, txn, role)?;
        if current.def_version != def.def_version || current.table != def.table {
            return Err(TaskError::Retry);
        }
        Ok(current)
    };
    let recheck: &mut Recheck<'_> = &mut recheck;
    let stored = store_preview(&db, def.id, &def, recheck, header, &dry.rows, false)?;
    let [restorable, conflicts, superseded, settled] = stored.counts;
    Ok(UndoSummary {
        id: stored.id,
        digest: stored.digest,
        restorable,
        conflicts,
        superseded,
        settled,
        expires_at: stored.expires_ms,
    })
}

/// APPROVE UNDO `id` OF TASK `task` DIGEST `digest` MODE `mode` as `role`
/// (D8): the undo commit, through [`protected_commit`] with
/// [`Actor::Undo`] (ruling R3; no `TaskContext` is synthesized).
///
/// Not in a task session (0A000); the task must exist (42704). The rest is
/// [`protected_commit`]'s undo path: the role's privileges (42501), the
/// replay of a committed undo of this preview (same request: the same
/// receipt; another mode or digest: `OperationConflict`), the pre-check
/// (a missing, applied, expired or changed preview is `ApprovalStale`;
/// `Complete` with conflicts is `Denied(UndoHasConflicts)`, not recorded),
/// then, under the hold, [`undo_stage`] and the kind-2 change record and
/// receipt. Task expiry and revocation never stop an undo. Returns the full
/// receipt with its change record.
pub fn undo_approve(
    session: &mut SqlSession,
    role: &RoleName,
    task: &TaskName,
    id: PreviewId,
    digest: [u8; 32],
    mode: UndoMode,
) -> Result<Receipt, TaskError> {
    if session.task().is_some() {
        return Err(TaskError::NotAllowedInTaskSession(StatementClass::Other));
    }
    let db = session.v2_db().ok_or_else(unsupported)?;
    if db.task_kernel_unsupported() {
        return Err(unsupported());
    }
    let def = session
        .tasks()
        .lookup(task.as_str())?
        .ok_or(CoreError::Sql(SqlError::UndefinedObject))?;
    let request = ProtectedRequest::Undo { preview: id, digest, mode };
    protected_commit(
        session,
        Actor::Undo { role, task: def.id },
        &undo_operation_id(id),
        request,
    )
}

/// The operation ID an undo commit's receipt carries: `undo:<preview id>`.
/// Its ledger identity is `03 | task | preview`, never this text.
pub fn undo_operation_id(id: PreviewId) -> OperationId {
    OperationId::new(format!("undo:{}", id.0)).expect("undo operation id length")
}

/// The preview id of an `undo:<id>` operation ID.
fn parse_undo_operation(op: &OperationId) -> Option<PreviewId> {
    let digits = std::str::from_utf8(op.as_bytes()).ok()?.strip_prefix("undo:")?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) || digits.starts_with('0') {
        return None;
    }
    digits.parse().ok().map(PreviewId)
}

/// The stored undo preview `id` of `task` at `txn`'s snapshot, if it can
/// still be approved: an undo, open, unexpired, with `digest`.
fn open_undo_header(
    db: &V2Db,
    txn: &mut Transaction,
    task: TaskObjectId,
    id: PreviewId,
    digest: &[u8; 32],
) -> Result<Option<PreviewHeader>, CoreError> {
    let Some(value) = txn.get(Keyspace::Kernel, &preview_key(task, id.0))? else {
        return Ok(None);
    };
    let header = PreviewHeader::decode(&value)?;
    let open = header.kind == PreviewKind::Undo as u8
        && header.state == STATE_OPEN
        && header.expires_ms > db.wall_now_ms()
        && &header.digest == digest;
    Ok(open.then_some(header))
}

/// D8 commit step 1, the pre-check, at the latest committed state with no
/// write and no hold: the preview must still be approvable
/// (`ApprovalStale`), and `Complete` refuses a preview with conflicts
/// (`Denied(UndoHasConflicts)`, not recorded, so `Partial` stays possible
/// on the same preview).
pub(crate) fn undo_precheck(
    db: &V2Db,
    task: TaskObjectId,
    id: PreviewId,
    digest: &[u8; 32],
    mode: UndoMode,
) -> Result<(), TaskError> {
    let mut txn = db.begin(Isolation::ReadCommitted);
    let header = open_undo_header(db, &mut txn, task, id, digest)?.ok_or(TaskError::ApprovalStale)?;
    if mode == UndoMode::Complete && header.counts[1] > 0 {
        return Err(TaskError::Denied(DenyRule::UndoHasConflicts));
    }
    Ok(())
}

/// What [`undo_stage`] hands the commit.
pub(crate) struct UndoStaged {
    /// The held transaction with the restores applied, and their effects.
    pub(crate) run: ProtectedRun,
    /// The stored header the recompute matched.
    pub(crate) header: PreviewHeader,
}

/// D8 commit step 2, under the hold (`txn` holds it and has no snapshot
/// yet): the role's privileges and the preview again at the snapshot, then
/// `plan` recomputed with its restores left applied, compared with the
/// stored preview: the schema fingerprint, the four counts and every row by
/// `(class, table, pk, version_read, aux, after)`. Any difference is
/// `ApprovalStale`. The definition version is not compared: a revoke bumps
/// it, and revocation does not stop an undo.
#[allow(clippy::too_many_arguments)]
pub(crate) fn undo_stage(
    session: &mut SqlSession,
    db: &Arc<V2Db>,
    def: &TaskDef,
    role: &RoleName,
    id: PreviewId,
    digest: &[u8; 32],
    mode: UndoMode,
    mut txn: Transaction,
) -> Result<UndoStaged, TaskError> {
    may_undo(db, def, &mut txn, role)?;
    let task = def.id;
    let header = open_undo_header(db, &mut txn, task, id, digest)?.ok_or(TaskError::ApprovalStale)?;
    if header.table != def.table {
        return Err(TaskError::ApprovalStale);
    }
    if mode == UndoMode::Complete && header.counts[1] > 0 {
        return Err(TaskError::Denied(DenyRule::UndoHasConflicts));
    }
    let mut stored = Vec::with_capacity(stored_rows(&header) as usize);
    for row_no in 0..stored_rows(&header) {
        let key = preview_row_key(task, id.0, row_no);
        let Some(value) = txn.get(Keyspace::Kernel, &key).map_err(CoreError::from)? else {
            return Err(TaskError::ApprovalStale);
        };
        stored.push(PreviewRow::decode(&value)?);
    }
    // The operation an operation-scope preview was made for, by its outcome.
    let scope_seq = match &header.scope {
        Some(scope) if scope.kind == 1 => {
            let mut key = Vec::with_capacity(33);
            key.push(NS_OUTCOME);
            key.extend_from_slice(&scope.outcome);
            match ledger::lookup(db, &key)?.map(|record| record.payload) {
                Some(OutcomePayload::Committed { task: stored, seq }) if stored == task => Some(seq),
                _ => return Err(TaskError::ApprovalStale),
            }
        }
        _ => None,
    };
    let (dry, run) = match plan_in(session, db, def, scope_seq, txn) {
        Ok(planned) => planned,
        Err(TaskError::Denied(DenyRule::DefinitionChanged | DenyRule::PreviewTooLarge)) => {
            return Err(TaskError::ApprovalStale);
        }
        Err(error) => return Err(error),
    };
    if dry.schema_fp != header.schema_fp
        || dry.counts != header.counts
        || dry.rows != stored
        || run.effects.len() != dry.counts[0] as usize
    {
        return Err(TaskError::ApprovalStale);
    }
    Ok(UndoStaged { run, header })
}

/// D8 commit step 3's preview part: the header applied at `seq` and its
/// `28` rows deleted, in the commit's write set.
pub(crate) fn stage_applied(
    txn: &mut Transaction,
    task: TaskObjectId,
    id: PreviewId,
    mut header: PreviewHeader,
    seq: u64,
) -> Result<(), CoreError> {
    for row_no in 0..stored_rows(&header) {
        txn.delete(Keyspace::Kernel, preview_row_key(task, id.0, row_no))?;
    }
    header.state = STATE_APPLIED;
    header.applied_seq = seq;
    txn.put(Keyspace::Kernel, preview_key(task, id.0), header.encode())?;
    Ok(())
}

/// 42501 insufficient_privilege.
fn no_privilege() -> TaskError {
    CoreError::Policy(crate::Error::Scope).into()
}

/// The undo privilege on the task (never the executor), then SELECT and
/// UPDATE on its table as SQL allows them: a superuser, the owner or grants.
pub(crate) fn may_undo(
    db: &V2Db,
    def: &TaskDef,
    txn: &mut Transaction,
    role: &RoleName,
) -> Result<(), TaskError> {
    let Some(record) = role_txn(txn, role).map_err(role_error)? else {
        return Err(no_privilege());
    };
    if !def.privileges_of(&record).contains(TaskPrivileges::UNDO) {
        return Err(no_privilege());
    }
    let view = db.catalog().committed_view(txn)?;
    let relation = view
        .by_id
        .get(&def.table)
        .map(|entry| entry.def.name.clone())
        .ok_or(CoreError::Sql(SqlError::UnknownTable))?;
    if record.superuser
        || table_owner_txn(txn, &relation).map_err(role_error)?.as_deref() == Some(role.as_str())
    {
        return Ok(());
    }
    let held = table_privileges_txn(txn, &relation, role).map_err(role_error)?;
    if held.contains(&TablePrivilege::Select) && held.contains(&TablePrivilege::Update) {
        Ok(())
    } else {
        Err(no_privilege())
    }
}

/// What the dry run hands the store.
struct UndoDryRun {
    schema_fp: [u8; 32],
    /// Restorable, conflicts, superseded, settled.
    counts: [u32; 4],
    /// Ascending `(class, table, pk, aux)`.
    rows: Vec<PreviewRow>,
}

/// One committed Kernel `(key, value)`.
type KernelEntry = (Vec<u8>, Vec<u8>);

/// The task's `25` rows with their `seq` (from the key) and `kind` (from
/// the `24` header), in `(seq, row_no)` order.
fn history(txn: &mut Transaction, task: TaskObjectId) -> Result<Vec<HistoryWrite>, CoreError> {
    let seq_of = |key: &[u8]| {
        key.get(9..17)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u64::from_be_bytes)
            .ok_or_else(|| CoreError::Corrupt("task change record key".into()))
    };
    let scan = |txn: &mut Transaction, ns| -> Result<Vec<KernelEntry>, CoreError> {
        let (start, end) = task_prefix(ns, task);
        let mut out = Vec::new();
        for entry in txn.scan(
            Keyspace::Kernel,
            Bound::Included(start.as_slice()),
            Bound::Excluded(end.as_slice()),
            Direction::Forward,
            None,
        )? {
            out.push(entry?);
        }
        Ok(out)
    };
    let mut kinds = BTreeMap::new();
    for (key, value) in scan(txn, NS_CHANGE_HEADER)? {
        kinds.insert(seq_of(&key)?, ChangeHeader::decode(&value)?.kind);
    }
    let mut out = Vec::new();
    for (key, value) in scan(txn, NS_CHANGE_ROW)? {
        let seq = seq_of(&key)?;
        let kind = *kinds
            .get(&seq)
            .ok_or_else(|| CoreError::Corrupt("task change row without header".into()))?;
        out.push(HistoryWrite { seq, kind, row: ChangeRow::decode(&value)? });
    }
    Ok(out)
}

/// The row's current `(version, values)` at `txn`'s snapshot, or `None`
/// when it is gone.
fn read_row(
    txn: &mut Transaction,
    table: TableId,
    pk: &[u8],
) -> Result<Option<(u64, Vec<Value>)>, CoreError> {
    let mut key = table.to_be_bytes().to_vec();
    key.extend_from_slice(pk);
    let Some(value) = txn.get(Keyspace::Rows, &key)? else {
        return Ok(None);
    };
    let record = codec::decode_row(&key, &value)?;
    Ok((!record.tombstone).then_some((record.row_version, record.values)))
}

/// `plan` in a Serializable transaction that is always rolled back.
fn dry_run(
    session: &mut SqlSession,
    db: &Arc<V2Db>,
    def: &TaskDef,
    scope_seq: Option<u64>,
) -> Result<UndoDryRun, TaskError> {
    let mut txn = db.begin(Isolation::Serializable);
    let mark = txn.savepoint();
    let (dry, run) = plan_in(session, db, def, scope_seq, txn)?;
    // Discard the restores; dropping the transaction releases the locks.
    let ProtectedRun { mut txn, .. } = run;
    txn.rollback_to(&mark).map_err(CoreError::from)?;
    drop(txn);
    Ok(dry)
}

/// `plan` in `txn` (no snapshot taken yet): reads the history and the
/// current rows, runs every candidate restore through the protected seam
/// (a constraint failure is rolled back to its savepoint and becomes a
/// conflict) and returns the stored-row form of the plan with the run, its
/// successful restores still applied.
fn plan_in(
    session: &mut SqlSession,
    db: &Arc<V2Db>,
    def: &TaskDef,
    scope_seq: Option<u64>,
    mut txn: Transaction,
) -> Result<(UndoDryRun, ProtectedRun), TaskError> {
    let image = db.catalog().snapshot(&mut txn)?.image().map_err(CoreError::Sql)?;
    let tables: BTreeMap<TableId, TableDef> =
        image.tables.values().map(|table| (table.id, table.clone())).collect();
    let schema_fp = match tables.get(&def.table) {
        Some(table) => schema_fp(table, image.table_constraints(table.id))?,
        None => return Err(TaskError::Denied(DenyRule::DefinitionChanged)),
    };
    let writes = history(&mut txn, def.id)?;
    // Current rows, each read once, widened to the table's columns.
    let mut current = BTreeMap::new();
    for write in &writes {
        let key = (write.row.table, write.row.pk.clone());
        if current.contains_key(&key) {
            continue;
        }
        let row = match tables.get(&write.row.table) {
            Some(table) => read_row(&mut txn, write.row.table, &write.row.pk)?.map(
                |(version, mut values)| {
                    values.resize(table.columns.len(), Value::Null);
                    (version, values)
                },
            ),
            None => None,
        };
        current.insert(key, row);
    }
    let read = |table: TableId, pk: &[u8]| current.get(&(table, pk.to_vec())).cloned().flatten();
    let decode = |row: &ChangeRow| {
        let width = tables.get(&row.table)?.columns.len();
        let mut values = crate::storage::task_row_values(&row.before).ok()?;
        if values.len() > width {
            return None;
        }
        values.resize(width, Value::Null);
        Some(values)
    };
    let plan = classify(&writes, scope_seq, &read, &decode);
    let mut rows = Vec::new();
    for conflict in &plan.conflicts {
        rows.push(conflict_row(conflict.table, &conflict.pk, conflict.version_read, conflict.reason));
    }
    for s in &plan.superseded {
        rows.push(PreviewRow {
            class: PreviewClass::Superseded as u8,
            table: s.table,
            pk: s.pk.clone(),
            version_read: s.version_read,
            aux: s.seq,
            after: Vec::new(),
        });
    }
    // Each candidate through the protected seam, in ascending (table, pk).
    let mut run = ProtectedRun { txn, effects: Effects::default() };
    let mut restorable = 0u32;
    for candidate in &plan.candidates {
        let table = tables
            .get(&candidate.table)
            .ok_or_else(|| CoreError::Corrupt("undo candidate without table".into()))?;
        let key = table.primary_key.iter().map(|&c| candidate.restore[c].clone()).collect();
        let sets = candidate.sets.iter().map(|&c| (c, candidate.restore[c].clone())).collect();
        let prepared = crate::storage::task_restore_statement(db, &run.txn, table, key, sets)?;
        let point = run.txn.savepoint();
        let before = run.effects.clone();
        let (back, result) = session.execute_protected_v2_expecting(
            db,
            run,
            &prepared,
            &[],
            Some(candidate.expect),
            None,
        );
        run = back;
        let constraint = match &result {
            Err(error) => crate::sqlstate(error).starts_with("23"),
            Ok(_) => {
                run.effects.len() != before.len() + 1
                    || !run
                        .effects
                        .iter()
                        .any(|e| e.table == candidate.table && e.pk == candidate.pk)
            }
        };
        if constraint {
            run.txn.rollback_to(&point).map_err(CoreError::from)?;
            run.effects = before;
            run.txn.release(point).map_err(CoreError::from)?;
            rows.push(conflict_row(
                candidate.table,
                &candidate.pk,
                candidate.expect,
                ConflictReason::Constraint,
            ));
            continue;
        }
        result.map_err(engine)?;
        run.txn.release(point).map_err(CoreError::from)?;
        restorable += 1;
        rows.push(PreviewRow {
            class: PreviewClass::Change as u8,
            table: candidate.table,
            pk: candidate.pk.clone(),
            version_read: candidate.expect,
            aux: 0,
            after: encode_after(&candidate.restore)?,
        });
    }
    crate::storage::cross_check_effects(&run.effects, &run.txn)?;
    if rows.len() > MAX_PREVIEW_ROWS {
        return Err(TaskError::Denied(DenyRule::PreviewTooLarge));
    }
    rows.sort_by(|a, b| (a.class, a.table, &a.pk, a.aux).cmp(&(b.class, b.table, &b.pk, b.aux)));
    let conflicts = rows.iter().filter(|r| r.class == PreviewClass::Conflict as u8).count();
    let overflow = || TaskError::from(CoreError::Limit);
    let dry = UndoDryRun {
        schema_fp,
        counts: [
            restorable,
            u32::try_from(conflicts).map_err(|_| overflow())?,
            u32::try_from(plan.superseded.len()).map_err(|_| overflow())?,
            plan.settled,
        ],
        rows,
    };
    Ok((dry, run))
}

fn conflict_row(table: TableId, pk: &[u8], version_read: u64, reason: ConflictReason) -> PreviewRow {
    PreviewRow {
        class: PreviewClass::Conflict as u8,
        table,
        pk: pk.to_vec(),
        version_read,
        aux: reason as u64,
        after: Vec::new(),
    }
}

impl crate::storage::Admin {
    /// For tests: one task retention pass now (GD-0061 D5), with the
    /// settings from `set_task_retention` (v2 only).
    #[doc(hidden)]
    pub fn task_retention_step(&self) -> Result<super::retention::RetentionStats, CoreError> {
        let db = self.sql_session().v2_db().ok_or(CoreError::Unsupported)?;
        db.retention_step()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const T: u64 = 9;

    /// A write of row `pk` by record `seq` from version `vb` to `vw`, whose
    /// before-value is the single integer `before` (`None` = blanked).
    fn w(seq: u64, kind: u8, pk: u8, vb: u64, vw: u64, before: Option<i64>) -> HistoryWrite {
        let (flags, bytes) = match before {
            Some(v) => (0, v.to_be_bytes().to_vec()),
            None => (ROW_BEFORE_BLANKED, Vec::new()),
        };
        HistoryWrite {
            seq,
            kind,
            row: ChangeRow {
                flags,
                table: T,
                pk: vec![pk],
                version_before: vb,
                version_written: vw,
                before: bytes,
                ..ChangeRow::default()
            },
        }
    }
    fn decode_int(row: &ChangeRow) -> Option<Vec<Value>> {
        let bytes: [u8; 8] = row.before.as_slice().try_into().ok()?;
        Some(vec![Value::Int(i64::from_be_bytes(bytes))])
    }
    fn decode_never(_: &ChangeRow) -> Option<Vec<Value>> {
        None
    }
    fn run(writes: &[HistoryWrite], scope: Option<u64>, rows: &[(u8, u64, i64)]) -> Plan {
        let rows: BTreeMap<Vec<u8>, (u64, Vec<Value>)> =
            rows.iter().map(|&(pk, v, x)| (vec![pk], (v, vec![Value::Int(x)]))).collect();
        classify(writes, scope, &|t, pk| if t == T { rows.get(pk).cloned() } else { None }, &decode_int)
    }

    #[test]
    fn single_write_restorable() {
        let plan = run(&[w(1, KIND_CHANGE, 1, 5, 6, Some(100))], None, &[(1, 6, 200)]);
        assert_eq!(
            plan.candidates,
            vec![Candidate { table: T, pk: vec![1], expect: 6, restore: vec![Value::Int(100)], sets: vec![0] }]
        );
        assert!(plan.conflicts.is_empty() && plan.superseded.is_empty());
        assert_eq!(plan.settled, 0);
    }

    #[test]
    fn linked_writes_restore_to_before_first() {
        let writes = [
            w(1, KIND_CHANGE, 1, 5, 6, Some(100)),
            w(2, KIND_CHANGE, 1, 6, 7, Some(200)),
            w(3, KIND_CHANGE, 1, 7, 8, Some(300)),
        ];
        let plan = run(&writes, None, &[(1, 8, 400)]);
        assert_eq!(plan.candidates.len(), 1);
        assert_eq!(plan.candidates[0].restore, vec![Value::Int(100)]);
        assert_eq!(plan.candidates[0].expect, 8);
        assert!(plan.superseded.is_empty());
    }

    #[test]
    fn application_write_between_task_writes_keeps_application_change() {
        // A -> B by the task (v5 -> v6), B -> C by the application (v6 -> v7),
        // C -> D by the task (v7 -> v8). Restore C; the A -> B write is superseded.
        let (a, c, d) = (1, 3, 4);
        let writes = [w(1, KIND_CHANGE, 1, 5, 6, Some(a)), w(2, KIND_CHANGE, 1, 7, 8, Some(c))];
        let plan = run(&writes, None, &[(1, 8, d)]);
        assert_eq!(plan.candidates.len(), 1);
        assert_eq!(plan.candidates[0].restore, vec![Value::Int(c)]);
        assert_eq!(
            plan.superseded,
            vec![Superseded { table: T, pk: vec![1], version_read: 6, seq: 1 }]
        );
        assert!(plan.conflicts.is_empty());
    }

    #[test]
    fn changed_since_is_conflict() {
        let plan = run(&[w(1, KIND_CHANGE, 1, 5, 6, Some(100))], None, &[(1, 7, 200)]);
        assert_eq!(
            plan.conflicts,
            vec![Conflict { table: T, pk: vec![1], version_read: 7, reason: ConflictReason::ChangedSince }]
        );
        assert!(plan.candidates.is_empty());
    }

    #[test]
    fn missing_row_is_conflict() {
        let plan = run(&[w(1, KIND_CHANGE, 1, 5, 6, Some(100))], None, &[]);
        assert_eq!(
            plan.conflicts,
            vec![Conflict { table: T, pk: vec![1], version_read: 0, reason: ConflictReason::RowMissing }]
        );
    }

    #[test]
    fn expired_before_values_is_conflict() {
        let writes = [w(1, KIND_CHANGE, 1, 5, 6, None), w(2, KIND_CHANGE, 1, 6, 7, Some(150))];
        let plan = run(&writes, None, &[(1, 7, 200)]);
        assert_eq!(plan.conflicts.len(), 1);
        assert_eq!(plan.conflicts[0].reason, ConflictReason::BeforeValuesExpired);
        assert!(plan.candidates.is_empty());
        // A schema that no longer fits is SchemaChanged.
        let writes = [w(1, KIND_CHANGE, 1, 5, 6, Some(100))];
        let plan = classify(&writes, None, &|_, _| Some((6, vec![Value::Int(1)])), &decode_never);
        assert_eq!(plan.conflicts[0].reason, ConflictReason::SchemaChanged);
    }

    #[test]
    fn undo_record_settles_earlier_writes_in_task_scope() {
        let writes = [
            w(1, KIND_CHANGE, 1, 5, 6, Some(100)),
            w(2, KIND_UNDO, 1, 6, 7, Some(200)),
            w(1, KIND_CHANGE, 2, 1, 2, Some(10)),
            w(2, KIND_UNDO, 2, 2, 3, Some(20)),
            w(3, KIND_CHANGE, 2, 3, 4, Some(10)),
        ];
        let plan = run(&writes, None, &[(1, 7, 100), (2, 4, 30)]);
        // Row 1: both writes settled by the undo; nothing left.
        // Row 2: two settled, then the seq-3 write is restorable.
        assert_eq!(plan.settled, 4);
        assert_eq!(plan.candidates.len(), 1);
        assert_eq!(plan.candidates[0].pk, vec![2]);
        assert_eq!(plan.candidates[0].restore, vec![Value::Int(10)]);
        assert!(plan.conflicts.is_empty() && plan.superseded.is_empty());
    }

    #[test]
    fn operation_scope_ignores_other_records() {
        let writes = [
            w(1, KIND_CHANGE, 1, 5, 6, Some(100)),
            w(2, KIND_UNDO, 1, 6, 7, Some(200)),
            w(3, KIND_CHANGE, 2, 1, 2, Some(10)),
        ];
        // Undo of the undo: only seq 2 is seen, and the undo reset does not apply.
        let plan = run(&writes, Some(2), &[(1, 7, 100), (2, 2, 11)]);
        assert_eq!(plan.settled, 0);
        assert_eq!(plan.candidates.len(), 1);
        assert_eq!(plan.candidates[0].pk, vec![1]);
        assert_eq!(plan.candidates[0].restore, vec![Value::Int(200)]);
        assert!(plan.conflicts.is_empty() && plan.superseded.is_empty());
    }

    #[test]
    fn write_back_to_original_values_is_settled() {
        let plan = run(&[w(1, KIND_CHANGE, 1, 5, 6, Some(100))], None, &[(1, 6, 100)]);
        assert_eq!(plan.settled, 1);
        assert!(plan.candidates.is_empty() && plan.conflicts.is_empty());
    }

    // ------------------------------------------------ R3-803 byte formats

    use super::super::ledger::{undo_identity, KIND_UNDO as REQUEST_UNDO, UNDO_IDENTITY_TAG};
    use super::super::receipt::{DECISION_UNDO_COMPLETE, DECISION_UNDO_PARTIAL};
    use super::super::records::{outcome_key, OutcomeRecord, ReceiptRecord};

    #[test]
    fn undo_identity_golden() {
        let identity = undo_identity(TaskObjectId(0x0102_0304_0506_0708), 0x1122_3344_5566_7788);
        assert_eq!(UNDO_IDENTITY_TAG, 3);
        assert_eq!(
            identity,
            [
                0x03, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x11, 0x22, 0x33, 0x44, 0x55,
                0x66, 0x77, 0x88
            ]
        );
        // The `22` key is sha256(domain16 || identity) behind the namespace byte.
        let key = outcome_key([0xAB; 16], &identity);
        let mut pre = vec![0xAB; 16];
        pre.extend_from_slice(&identity);
        let mut expect = vec![0x22];
        expect.extend_from_slice(&<sha2::Sha256 as sha2::Digest>::digest(&pre));
        assert_eq!(key, expect);
        // No `02` task identity has the same bytes: its tag differs.
        let op = OperationId::new(identity[1..].to_vec()).unwrap();
        let task = super::super::ledger::task_identity(TaskObjectId(0x0102_0304_0506_0708), &op);
        assert_eq!(task[0], 2);
        assert_ne!(outcome_key([0xAB; 16], &task), key);
    }

    #[test]
    fn undo_operation_id_round_trip() {
        assert_eq!(undo_operation_id(PreviewId(7)).as_bytes(), b"undo:7");
        assert_eq!(parse_undo_operation(&undo_operation_id(PreviewId(u64::MAX))), Some(PreviewId(u64::MAX)));
        for text in ["undo:", "undo:07", "undo:1x", "Undo:1", "undo:18446744073709551616", "op-1"] {
            assert_eq!(parse_undo_operation(&OperationId::new(text).unwrap()), None, "{text}");
        }
    }

    fn undo_receipt() -> ReceiptRecord {
        ReceiptRecord {
            decision: DECISION_UNDO_PARTIAL,
            request_kind: REQUEST_UNDO,
            flags: 0,
            task: TaskObjectId(5),
            seq: 9,
            def_version: 2,
            catalog_version: 0x77,
            decided_ms: 1_000,
            request_fp: [0x11; 32],
            effects_digest: [0x22; 32],
            rows_affected: 2,
            distinct_charged: 0,
            transitions_charged: 0,
            noop_rows: 0,
            distinct_after: 5,
            transitions_after: 6,
            preview: 4,
            undo_mode: UndoMode::Partial as u8,
            restored: 2,
            conflicts: 1,
            superseded: 3,
            op_id: b"undo:4".to_vec(),
            issuer: "ic".into(),
            executor: "ra".into(),
            approver: Some("ic".into()),
        }
    }

    /// The `23` receipt of an undo, byte for byte (design-0 R4 additions:
    /// `undo_mode u8, restored u32, conflicts u32, superseded u32` after
    /// `preview u64`).
    #[test]
    fn undo_receipt_golden() {
        let mut expect = vec![0x01, 0x03, 0x03, 0x00];
        for v in [5u64, 9, 2, 0x77, 1_000] {
            expect.extend_from_slice(&v.to_be_bytes());
        }
        expect.extend_from_slice(&[0x11; 32]);
        expect.extend_from_slice(&[0x22; 32]);
        for v in [2u32, 0, 0, 0] {
            expect.extend_from_slice(&v.to_be_bytes());
        }
        for v in [5u64, 6, 4] {
            expect.extend_from_slice(&v.to_be_bytes());
        }
        expect.push(0x02);
        for v in [2u32, 1, 3] {
            expect.extend_from_slice(&v.to_be_bytes());
        }
        expect.extend_from_slice(b"\x06undo:4\x02ic\x02ra\x02ic");
        let record = undo_receipt();
        assert_eq!(record.encode(), expect);
        assert_eq!(ReceiptRecord::decode(&expect).unwrap(), record);
        let complete = ReceiptRecord {
            decision: DECISION_UNDO_COMPLETE,
            undo_mode: UndoMode::Complete as u8,
            ..undo_receipt()
        };
        assert_eq!(ReceiptRecord::decode(&complete.encode()).unwrap(), complete);
        assert_eq!((DECISION_UNDO_COMPLETE, DECISION_UNDO_PARTIAL), (2, 3));
    }

    /// The `24` header of an undo: `kind` 02.
    #[test]
    fn undo_change_header_golden() {
        let header = ChangeHeader {
            flags: 0,
            kind: KIND_UNDO,
            outcome_key: [0x33; 32],
            table: 7,
            row_count: 2,
            salt: [0x44; 16],
            effects_digest: [0x55; 32],
            decided_ms: 1_000,
        };
        let mut expect = vec![0x01, 0x00, 0x02];
        expect.extend_from_slice(&[0x33; 32]);
        expect.extend_from_slice(&7u64.to_be_bytes());
        expect.extend_from_slice(&2u32.to_be_bytes());
        expect.extend_from_slice(&[0x44; 16]);
        expect.extend_from_slice(&[0x55; 32]);
        expect.extend_from_slice(&1_000u64.to_be_bytes());
        assert_eq!(header.encode(), expect);
        assert_eq!(expect.len(), 103);
        assert_eq!(ChangeHeader::decode(&expect).unwrap(), header);
    }

    /// The `22` outcome of an undo: kind 2 (committed) under the 17-byte
    /// identity.
    #[test]
    fn undo_outcome_round_trip() {
        let record = OutcomeRecord {
            flags: 0,
            identity: undo_identity(TaskObjectId(5), 4),
            request_fp: [0x11; 32],
            accepted_fp: [0x11; 32],
            decided_lower_ms: 1_000,
            decided_upper_ms: 1_000,
            retain_until_ms: 2_000,
            payload: OutcomePayload::Committed { task: TaskObjectId(5), seq: 9 },
        };
        let bytes = record.encode();
        assert_eq!(&bytes[..5], &[0x01, 0x02, 0x00, 0x00, 0x11]);
        assert_eq!(&bytes[5..22], undo_identity(TaskObjectId(5), 4).as_slice());
        assert_eq!(OutcomeRecord::decode(&bytes).unwrap(), record);
    }
}
