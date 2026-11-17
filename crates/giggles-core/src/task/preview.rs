//! PREVIEW (GD-0061 design D7.2-D7.4): identity, schema fingerprint and
//! digest (R3-701), and the preview itself, a dry run then a store (R3-703).
//!
//! The stored header (`27`) and row (`28`) structs and their codecs live in
//! [`super::records`]; this module re-exports them and defines the values
//! that go into them.
//!
//! [`preview`] is two transactions and writes no user rows:
//! - T1, the dry run (no hold; a preview is advisory and apply rechecks it
//!   in full): the task's standing, the column rules, the statement through
//!   the protected seam bound to the task (visible rows) with a clock pinned
//!   at the transaction instant, the clock observations (D7.4), the policy
//!   and the caps on the effects, the 10,000-row ceiling; then roll back.
//! - T2, the store (ReadCommitted, under the authority hold): the standing
//!   again, the sweep of expired and applied previews (headers, rows and
//!   approvals), the `stored_previews` limit, the id from the head, then the
//!   header, its rows and the head in one commit.
//!
//! Nothing a preview refuses is recorded: it has no operation ID. In a task
//! session the preview writes one read-log record (no rows returned).
//!
//! [`apply_stage`] (R3-706, D7.6) is the stored preview as the protected
//! commit's [`ApplySource`]: step 1 (the header) and step 2 (the stored
//! statement, its parameters and its pinned clock) run under the hold before
//! the statement; step 3 ([`matches`]) and step 5 (the approval) are read in
//! the tracked transaction; step 6 stages the applied header, the consumed
//! approval and the deletion of the `28` rows in the commit's write set.
//! Step 4 (policy and caps) is `decide::evaluate` in the commit.
use super::admit::StatementClass;
use super::authority;
use super::columns;
use super::approval::{APPROVAL_ACTIVE, APPROVAL_CONSUMED};
use super::commit::{
    catalog_def, engine, hold_error, role_error, standing, unsupported, ApplySource, DefAt,
};
use super::context::TaskContext;
use super::decide::{self, ApprovalState, Decision, RequestKind};
use super::definition::{RoleName, TaskDef, TaskPrivileges, WallClockMs};
use super::effects::Effects;
use super::error::{Budget, DenyRule, TaskError};
use super::records::{
    approval_key, mutation_head_key, preview_key, preview_row_key, task_prefix, ApprovalRecord,
    MutationHead, TaskObjectId, MAX_STATEMENT_TEXT, NS_PREVIEW, NS_PREVIEW_ROW,
};
pub(crate) use super::records::{PreviewHeader, PreviewRow};
use crate::CoreError;
use crate::relational::clock::{
    ExecutionClock, StatementClock, OBS_INSTANT, OBS_SEQUENCE, OBS_WALL, OBS_XACT, OBS_ZONE,
};
use crate::relational::{Prepared, SqlRow, TableDef, Value, catalog_ext::Constraint};
use crate::storage::engine::kv::Direction;
use crate::storage::roles::{role_txn, table_privileges_txn, RoleError, TablePrivilege};
use crate::storage::{Isolation, Keyspace, ProtectedRun, SqlSession, Transaction, V2Db};
use sha2::{Digest, Sha256};
use std::cell::RefCell;
use std::ops::Bound;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Per-task preview number (`head.next_preview`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PreviewId(pub u64);

/// Header `kind`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum PreviewKind {
    Change = 1,
    Undo = 2,
}

/// Row `class`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum PreviewClass {
    /// A change, or an undo restore.
    Change = 0,
    Conflict = 1,
    Superseded = 2,
}

/// Header `state`.
pub(crate) const STATE_OPEN: u8 = 0;
pub(crate) const STATE_APPLIED: u8 = 1;
/// `clock_pin` value meaning "no pin".
pub(crate) const NO_CLOCK_PIN: i64 = i64::MIN;
/// One preview holds at most this many rows.
pub(crate) const MAX_PREVIEW_ROWS: usize = 10_000;

/// `SHA-256(encode_table(def) || each encode_constraint(c) in constraint-id order)`.
pub(crate) fn schema_fp(table: &TableDef, constraints: &[Constraint]) -> Result<[u8; 32], CoreError> {
    let mut ordered: Vec<Constraint> = constraints.to_vec();
    ordered.sort_by_key(|c| c.id);
    let parts = crate::storage::task_schema_bytes(table, &ordered)?;
    let mut h = Sha256::new();
    for part in parts {
        h.update(&part);
    }
    Ok(h.finalize().into())
}

/// The D7.3 preimage. Rows are hashed in ascending `(class, table, pk)`
/// order whatever order the caller passes them in.
pub(crate) fn preimage(
    domain: [u8; 16],
    task: TaskObjectId,
    id: PreviewId,
    header: &PreviewHeader,
    rows: &[PreviewRow],
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"GDPV");
    out.push(1);
    out.push(header.kind);
    out.extend_from_slice(&domain);
    out.extend_from_slice(&task.0.to_be_bytes());
    out.extend_from_slice(&id.0.to_be_bytes());
    out.extend_from_slice(&header.def_version.to_be_bytes());
    out.extend_from_slice(&header.table.to_be_bytes());
    out.extend_from_slice(&header.schema_fp);
    out.extend_from_slice(&header.clock_pin.to_be_bytes());
    out.extend_from_slice(&header.expires_ms.to_be_bytes());
    out.push(header.surface);
    let b = |out: &mut Vec<u8>, bytes: &[u8]| {
        out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
        out.extend_from_slice(bytes);
    };
    b(&mut out, header.text.as_bytes());
    b(&mut out, &header.params);
    if header.kind == PreviewKind::Undo as u8 {
        let scope = header.scope.clone().unwrap_or_default();
        out.push(scope.kind);
        out.extend_from_slice(&scope.outcome);
    }
    out.extend_from_slice(&(rows.len() as u32).to_be_bytes());
    let mut sorted: Vec<&PreviewRow> = rows.iter().collect();
    sorted.sort_by(|a, b| (a.class, a.table, &a.pk).cmp(&(b.class, b.table, &b.pk)));
    for row in sorted {
        out.push(row.class);
        out.extend_from_slice(&row.table.to_be_bytes());
        b(&mut out, &row.pk);
        out.extend_from_slice(&row.version_read.to_be_bytes());
        out.extend_from_slice(&row.aux.to_be_bytes());
        b(&mut out, &row.after);
    }
    out
}

/// SHA-256 of [`preimage`].
pub(crate) fn digest(
    domain: [u8; 16],
    task: TaskObjectId,
    id: PreviewId,
    header: &PreviewHeader,
    rows: &[PreviewRow],
) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(preimage(domain, task, id, header, rows));
    h.finalize().into()
}

/// Request digest of an Apply: `SHA-256(02 || task || preview || digest)`.
pub(crate) fn apply_request_digest(task: TaskObjectId, id: PreviewId, digest: &[u8; 32]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update([2u8]);
    h.update(task.0.to_be_bytes());
    h.update(id.0.to_be_bytes());
    h.update(digest);
    h.finalize().into()
}

/// Request digest of an Undo: leading `03`, trailing mode byte.
pub(crate) fn undo_request_digest(task: TaskObjectId, id: PreviewId, digest: &[u8; 32], mode: u8) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update([3u8]);
    h.update(task.0.to_be_bytes());
    h.update(id.0.to_be_bytes());
    h.update(digest);
    h.update([mode]);
    h.finalize().into()
}

/// Lowercase hex on the wire.
pub(crate) fn digest_hex(digest: &[u8; 32]) -> String {
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

/// One collected effect, as the Phase 6 `Effects::iter()` will yield it
/// (`task/effects.rs` is not in the tree yet; R3-703 maps `RowEffect` onto this view).
#[derive(Clone, Copy, Debug)]
pub(crate) struct EffectView<'a> {
    pub(crate) table: u64,
    pub(crate) pk: &'a [u8],
    pub(crate) old: Option<&'a SqlRow>,
    pub(crate) new: Option<&'a SqlRow>,
}

/// Class 00 rows in ascending `(table, pk)`, `version_read = old.version`,
/// `after = encode(new.values)`. An effect without an old or a new row (a
/// primary-key move or a delete) is `Denied(PrimaryKeyChange)`.
pub(crate) fn rows_from_effects<'a>(
    effects: impl IntoIterator<Item = EffectView<'a>>,
) -> Result<Vec<PreviewRow>, TaskError> {
    let mut rows = Vec::new();
    for e in effects {
        let (Some(old), Some(new)) = (e.old, e.new) else {
            return Err(TaskError::Denied(DenyRule::PrimaryKeyChange));
        };
        rows.push(PreviewRow {
            class: PreviewClass::Change as u8,
            table: e.table,
            pk: e.pk.to_vec(),
            version_read: old.version,
            aux: 0,
            after: encode_after(&new.values)?,
        });
    }
    rows.sort_by(|a, b| (a.table, &a.pk).cmp(&(b.table, &b.pk)));
    Ok(rows)
}

/// The row payload encoding of a full row (`params` and `after`).
pub(crate) fn encode_after(values: &[Value]) -> Result<Vec<u8>, CoreError> {
    crate::storage::task_row_payload(values)
}

/// Where the preview's text came from (header `surface`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Surface {
    Sql = 0,
    Native = 1,
}

/// What PREVIEW returns: `(change_id, digest, row_count, expires_at)` (D7.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PreviewSummary {
    pub id: PreviewId,
    pub digest: [u8; 32],
    pub rows: u32,
    pub expires_at: WallClockMs,
}

/// Clock observations a preview cannot fix in advance (D7.4).
const NOT_MATERIALIZABLE: u8 = OBS_WALL | OBS_ZONE | OBS_SEQUENCE | OBS_XACT;

/// What the dry run hands the store.
struct DryRun {
    def: Arc<TaskDef>,
    schema_fp: [u8; 32],
    clock_pin: i64,
    rows: Vec<PreviewRow>,
}

/// PREVIEW of one UPDATE for the task `ctx` (D7.2): T1, the dry run, then
/// T2, the store. `text` is the UPDATE exactly as received and `params` its
/// parameters; `clock` is the statement's clock, whose transaction instant
/// is the pin. In a task session the preview is logged as one read-log
/// record with its outcome.
///
/// Core has no native parser: for `Surface::Native`, `text` is the SQL
/// UPDATE that the native front end (giggles-language profile 0.22) lowered
/// the native `update` to, and it is prepared, stored and re-run at apply as
/// SQL. The header's `surface` byte records where it came from, so the same
/// UPDATE previewed from SQL and from native text has different digests.
pub fn preview(
    session: &mut SqlSession,
    ctx: &TaskContext,
    surface: Surface,
    text: &str,
    params: &[Value],
    clock: &StatementClock,
) -> Result<PreviewSummary, TaskError> {
    let entry = if session.task().is_some() {
        session.set_task_statement_text(text);
        session.start_task_statement(StatementClass::Preview, params.len())
    } else {
        None
    };
    let result = run(session, ctx, surface, text, params, clock);
    if let Some(entry) = entry {
        if let Err(error) = &result {
            entry.fail_with(error.sqlstate());
        }
        entry.finish();
    }
    result
}

fn run(
    session: &mut SqlSession,
    ctx: &TaskContext,
    surface: Surface,
    text: &str,
    params: &[Value],
    clock: &StatementClock,
) -> Result<PreviewSummary, TaskError> {
    // v2 only, never on a cluster gateway or a replicated store.
    let db = session.v2_db().ok_or_else(unsupported)?;
    if db.task_kernel_unsupported() {
        return Err(unsupported());
    }
    if session.task().is_some_and(|bound| bound.task != ctx.task) {
        return Err(TaskError::Revoked);
    }
    if text.len() > MAX_STATEMENT_TEXT || u16::try_from(params.len()).is_err() {
        return Err(CoreError::Limit.into());
    }
    let dry = dry_run(session, &db, ctx, text, params, clock, Policy::Check)?;
    store(&db, ctx, surface, text, params, dry)
}

/// Whether the dry run decides the policy and the caps. Only the hidden test
/// seam [`crate::storage::Admin::preview_task_without_policy`] skips them, to
/// show that apply decides them again (approvals never widen).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Policy {
    Check,
    Skip,
}

/// T1: everything a preview checks, in a Serializable transaction that is
/// always rolled back.
fn dry_run(
    session: &mut SqlSession,
    db: &Arc<V2Db>,
    ctx: &TaskContext,
    text: &str,
    params: &[Value],
    clock: &StatementClock,
    policy: Policy,
) -> Result<DryRun, TaskError> {
    let task = ctx.task;
    let sql = |e| TaskError::Core(Box::new(CoreError::Sql(e)));
    // 1. Not revoked, not expired, bound to the current definition.
    let latest = catalog_def(db, task, DefAt::Latest)?;
    standing(latest.as_deref(), ctx, db.wall_now_ms())?;
    let latest = latest.ok_or(TaskError::Revoked)?;
    // 2. One UPDATE, under the column rules (design-3 section 2).
    let prepared = session.prepare(text)?;
    let class = prepared.task_class();
    if class != StatementClass::Update {
        return Err(TaskError::NotAllowedInTaskSession(class));
    }
    let expanded = {
        let mut read = db.begin(Isolation::ReadCommitted);
        let catalog = db.catalog().snapshot(&mut read)?;
        let image = catalog.image().map_err(sql)?;
        columns::apply_in_catalog(
            &latest,
            ctx.bound_def_version,
            &catalog.parts.column_labels,
            &image,
            &prepared,
        )?
        .0
    };
    // 3. The statement through the protected seam, bound to the task so it
    // matches only the visible rows, with every instant pinned at the
    // transaction instant (what apply re-runs with) and no wall clock.
    let mut txn = db.begin(Isolation::Serializable);
    let mark = txn.savepoint();
    let mut pinned = ExecutionClock::pinned(clock.transaction_us, clock.zone);
    pinned.unavailable = clock.unavailable.clone();
    let execution = ExecutionClock::new(&pinned);
    let bound = session.task.replace(ctx.clone());
    let (run, result) = session.execute_protected_v2(
        db,
        ProtectedRun { txn, effects: Effects::default() },
        &expanded,
        params,
        Some(&execution),
    );
    session.task = bound;
    let ProtectedRun { mut txn, effects } = run;
    // D7.4 first: a refused source may also have failed the statement.
    let observed = execution.observed();
    if observed & NOT_MATERIALIZABLE != 0 {
        return Err(TaskError::Denied(DenyRule::NotMaterializable));
    }
    result.map_err(engine)?;
    crate::storage::cross_check_effects(&effects, &txn)?;
    // Rechecks at the statement snapshot, as the protected commit does.
    let def = catalog_def(db, task, DefAt::Snapshot(&mut txn))?;
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
    let state = db.catalog().snapshot(&mut txn)?.image().map_err(sql)?;
    let table = state
        .tables
        .values()
        .find(|table| table.id == def.table)
        .ok_or(TaskError::Denied(DenyRule::DefinitionChanged))?
        .clone();
    // 5 and 6. The policy and the caps, decided as an approved apply would
    // be: structure, visible rows, allowed write, caps; never the threshold.
    // The membership charge is advisory here (no hold) and rolled back.
    let head = authority::load_head(&mut txn, task)?;
    let charge = authority::charge(&mut txn, task, head.next_seq, &effects)?;
    let decision = if policy == Policy::Skip {
        Decision::Commit(charge)
    } else {
        decide::evaluate(
            &state,
            &def,
            &table,
            &head,
            &effects,
            &charge,
            RequestKind::Apply,
            ApprovalState::Matching,
        )
    };
    match decision {
        Decision::Commit(_) => {}
        Decision::Denied(rule) => return Err(TaskError::Denied(rule)),
        Decision::BudgetExceeded(budget) => return Err(TaskError::BudgetExceeded(budget)),
        Decision::ApprovalRequired => return Err(TaskError::ApprovalRequired),
        Decision::ApprovalStale => return Err(TaskError::ApprovalStale),
    }
    if effects.len() > MAX_PREVIEW_ROWS {
        return Err(TaskError::Denied(DenyRule::PreviewTooLarge));
    }
    // After-values are full rows, resized to the table's width.
    let width = table.columns.len();
    let resized: Vec<Option<SqlRow>> = effects
        .iter()
        .map(|effect| {
            effect.new.as_ref().map(|new| {
                let mut new = new.clone();
                new.values.resize(width, Value::Null);
                new
            })
        })
        .collect();
    let rows = rows_from_effects(effects.iter().zip(&resized).map(|(effect, new)| EffectView {
        table: effect.table,
        pk: &effect.pk,
        old: effect.old.as_ref(),
        new: new.as_ref(),
    }))?;
    let schema_fp = schema_fp(&table, state.table_constraints(table.id))?;
    let clock_pin = if observed & OBS_INSTANT != 0 {
        clock.transaction_us
    } else {
        NO_CLOCK_PIN
    };
    // 4. Discard the writes; dropping the transaction releases the locks.
    txn.rollback_to(&mark).map_err(|e| engine(e.into()))?;
    drop(txn);
    Ok(DryRun { def, schema_fp, clock_pin, rows })
}

/// T2: the store, under the authority hold.
fn store(
    db: &Arc<V2Db>,
    ctx: &TaskContext,
    surface: Surface,
    text: &str,
    params: &[Value],
    dry: DryRun,
) -> Result<PreviewSummary, TaskError> {
    let header = PreviewHeader {
        kind: PreviewKind::Change as u8,
        state: STATE_OPEN,
        created_ms: 0,
        expires_ms: 0,
        def_version: dry.def.def_version,
        table: dry.def.table,
        schema_fp: dry.schema_fp,
        clock_pin: dry.clock_pin,
        digest: [0; 32],
        counts: [0; 4],
        applied_seq: 0,
        creator: ctx.executor.clone(),
        scope: None,
        surface: surface as u8,
        text: text.to_owned(),
        params: encode_after(params)?,
    };
    // 2. Revoked, expired and the definition version again.
    let mut recheck = |_: &mut Transaction, def: Option<Arc<TaskDef>>, now: u64| {
        standing(def.as_deref(), ctx, now)?;
        let def = def.ok_or(TaskError::Revoked)?;
        if def.def_version != dry.def.def_version || def.table != dry.def.table {
            return Err(TaskError::Revoked);
        }
        Ok(def)
    };
    let stored = store_preview(db, ctx.task, &dry.def, &mut recheck, header, &dry.rows, true)?;
    Ok(PreviewSummary {
        id: stored.id,
        digest: stored.digest,
        rows: stored.counts[0],
        expires_at: WallClockMs(i64::try_from(stored.expires_ms).unwrap_or(i64::MAX)),
    })
}

/// Rechecks the task under the store's hold, at its snapshot: gets the
/// transaction, the definition at that snapshot and the server clock, and
/// returns the definition to store under.
pub(super) type Recheck<'a> =
    dyn FnMut(&mut Transaction, Option<Arc<TaskDef>>, u64) -> Result<Arc<TaskDef>, TaskError> + 'a;

/// What [`store_preview`] stored.
pub(super) struct StoredHeader {
    pub(super) id: PreviewId,
    pub(super) digest: [u8; 32],
    pub(super) counts: [u32; 4],
    pub(super) expires_ms: u64,
}

/// T2 of a change preview (D7.2) and of an undo preview (D8): one
/// ReadCommitted transaction under the hold on `20|task|00` (waiting at most
/// `def.limits.lock_wait_ms`), then a statement snapshot, the head and
/// `recheck`; the sweep of the task's expired and applied previews of both
/// kinds; the `stored_previews` limit, which counts both kinds; the id from
/// the head; the header (its `created_ms`, `expires_ms`, `digest` and, for a
/// change preview, `counts[0]` filled in here) with its rows in the order
/// given and the head, in one commit. `expires_ms` is `now + preview_ttl`,
/// capped at the task's expiry when `cap_at_task_expiry`.
pub(super) fn store_preview(
    db: &Arc<V2Db>,
    task: TaskObjectId,
    def: &TaskDef,
    recheck: &mut Recheck<'_>,
    mut header: PreviewHeader,
    rows: &[PreviewRow],
    cap_at_task_expiry: bool,
) -> Result<StoredHeader, TaskError> {
    // 1. The hold, then a statement snapshot taken after it, then the head.
    let mut txn = db.begin(Isolation::ReadCommitted);
    let deadline = Instant::now().checked_add(Duration::from_millis(def.limits.lock_wait_ms));
    txn.lock_kernel_key(&mutation_head_key(task), deadline, &|| {
        crate::cancellation::check().is_err()
    })
    .map_err(hold_error)?;
    txn.begin_statement();
    let head = authority::load_head(&mut txn, task)?;
    // 2. The caller's rechecks.
    let now = db.wall_now_ms();
    let def = catalog_def(db, task, DefAt::Snapshot(&mut txn))?;
    let def = recheck(&mut txn, def, now)?;
    // 3. Sweep this task's expired and applied previews.
    let open = sweep(&mut txn, task, now)?;
    // 4. The limit on open previews.
    if open >= def.limits.stored_previews {
        return Err(TaskError::BudgetExceeded(Budget::StoredPreviews));
    }
    // 5. The id.
    let id = PreviewId(head.next_preview);
    let overflow = || TaskError::from(CoreError::Limit);
    let count = u32::try_from(rows.len()).map_err(|_| overflow())?;
    if header.kind == PreviewKind::Change as u8 {
        header.counts[0] = count;
    }
    let ttl_ms = def.limits.preview_ttl_s.saturating_mul(1000);
    let mut expires_ms = now.saturating_add(ttl_ms);
    if cap_at_task_expiry {
        expires_ms = expires_ms.min(u64::try_from(def.expires_at.0).unwrap_or(0));
    }
    header.created_ms = now;
    header.expires_ms = expires_ms;
    header.def_version = def.def_version;
    header.table = def.table;
    header.digest = digest(db.meta.domain, task, id, &header, rows);
    // 6. Header, rows and head, in one commit.
    let put = |txn: &mut Transaction, key: Vec<u8>, value: Vec<u8>| -> Result<(), TaskError> {
        txn.put(Keyspace::Kernel, key, value).map_err(CoreError::from)?;
        Ok(())
    };
    put(&mut txn, preview_key(task, id.0), header.encode())?;
    for (row_no, row) in rows.iter().enumerate() {
        let row_no = u32::try_from(row_no).map_err(|_| overflow())?;
        put(&mut txn, preview_row_key(task, id.0, row_no), row.encode())?;
    }
    let next = MutationHead {
        def_version: def.def_version,
        next_preview: id.0.checked_add(1).ok_or_else(overflow)?,
        stored_previews: open.checked_add(1).ok_or_else(overflow)?,
        ..head
    };
    authority::stage_head(&mut txn, task, &next)?;
    db.catalog().commit_ddl(txn).map_err(|e| engine(e.into()))?;
    Ok(StoredHeader { id, digest: header.digest, counts: header.counts, expires_ms })
}

/// The task's stored preview headers, in id order.
pub(super) fn headers(
    txn: &mut Transaction,
    task: TaskObjectId,
) -> Result<Vec<(PreviewId, PreviewHeader)>, CoreError> {
    let (start, end) = task_prefix(NS_PREVIEW, task);
    let scan = txn.scan(
        Keyspace::Kernel,
        Bound::Included(start.as_slice()),
        Bound::Excluded(end.as_slice()),
        Direction::Forward,
        None,
    )?;
    let mut out = Vec::new();
    for entry in scan {
        let (key, value) = entry?;
        let id = key
            .get(9..17)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u64::from_be_bytes)
            .ok_or_else(|| CoreError::Corrupt("task preview key".into()))?;
        out.push((PreviewId(id), PreviewHeader::decode(&value)?));
    }
    Ok(out)
}

/// Rows stored under a header: classes 0, 1 and 2.
pub(super) fn stored_rows(header: &PreviewHeader) -> u32 {
    header.counts[..3].iter().fold(0u32, |n, c| n.saturating_add(*c))
}

/// Deletes the task's expired and applied previews with their rows and
/// approvals; returns how many stay open.
fn sweep(txn: &mut Transaction, task: TaskObjectId, now: u64) -> Result<u32, CoreError> {
    let mut open = 0u32;
    for (id, header) in headers(txn, task)? {
        if header.state == STATE_OPEN && header.expires_ms > now {
            open = open.saturating_add(1);
            continue;
        }
        for row_no in 0..stored_rows(&header) {
            txn.delete(Keyspace::Kernel, preview_row_key(task, id.0, row_no))?;
        }
        txn.delete(Keyspace::Kernel, approval_key(task, id.0))?;
        txn.delete(Keyspace::Kernel, preview_key(task, id.0))?;
    }
    Ok(open)
}

/// APPLY of the stored change preview `id` for the task `ctx`, requested
/// with `digest` (D7.6), as the protected commit's [`ApplySource`].
///
/// Deviation from plan v5's `apply_stage(tracked, ctx, def, id, digest) ->
/// ApplyStaged`: there is no `Tracked` type. The stages are the methods of
/// R3-618's seam, which `protected_commit` calls inside each attempt, under
/// the hold: [`ApplySource::statement`] is steps 1 and 2,
/// [`ApplySource::approval_state`] steps 3 and 5 (with step 1 again, in the
/// tracked transaction) and [`ApplySource::stage_consumed`] step 6. Step 4
/// is `decide::evaluate` in the commit. The source is resolved once per call;
/// nothing is read until the first attempt holds the hold.
pub(crate) fn apply_stage(
    db: &Arc<V2Db>,
    ctx: &TaskContext,
    id: PreviewId,
    digest: [u8; 32],
) -> Result<Box<dyn ApplySource>, TaskError> {
    Ok(Box::new(StoredApply {
        db: Arc::clone(db),
        ctx: ctx.clone(),
        id,
        digest,
        header: RefCell::new(None),
        used: RefCell::new(None),
    }))
}

/// D7.6 step 3, exactly: with `effects` (`E`, ascending `(table, pk)`) and
/// the stored `rows` (`R`, numbered in the same order), the preview is current
/// only if `len(E) == len(R)` and, for every `i`, the table and primary key
/// are equal, `E[i].old` is present with `version == R[i].version_read`, and
/// `E[i].new` is present with `encode(new.values) == R[i].after`. Only
/// equality is used. The new values are resized to `width` (the table's
/// column count) first, as the preview stored them. A stored row of another
/// class never matches.
pub(crate) fn matches(effects: &Effects, rows: &[PreviewRow], width: usize) -> bool {
    effects.len() == rows.len()
        && effects.iter().zip(rows).all(|(effect, row)| {
            row.class == PreviewClass::Change as u8
                && effect.table == row.table
                && effect.pk == row.pk
                && effect
                    .old
                    .as_ref()
                    .is_some_and(|old| old.version == row.version_read)
                && effect.new.as_ref().is_some_and(|new| {
                    let mut values = new.values.clone();
                    values.resize(width, Value::Null);
                    encode_after(&values).is_ok_and(|after| after == row.after)
                })
        })
}

/// The stored preview and approval behind one Apply call.
struct StoredApply {
    db: Arc<V2Db>,
    ctx: TaskContext,
    id: PreviewId,
    /// The digest the executor sent.
    digest: [u8; 32],
    /// The header step 1 accepted in the current attempt.
    header: RefCell<Option<PreviewHeader>>,
    /// The approval step 5 used in the current attempt, if one was needed.
    used: RefCell<Option<ApprovalRecord>>,
}

/// What step 1 accepted: the header, the table it was taken on and the
/// definition, all at one snapshot.
struct Checked {
    header: PreviewHeader,
    table: TableDef,
    def: Arc<TaskDef>,
}

impl StoredApply {
    /// D7.6 step 1 at `txn`'s snapshot. `None` (stale) when the header is
    /// missing, not a change, not open, expired, created by another executor,
    /// has another digest, or its `def_version`, table or `schema_fp` is not
    /// the current one.
    fn checked(&self, txn: &mut Transaction) -> Result<Option<Checked>, CoreError> {
        let task = self.ctx.task;
        let Some(value) = txn.get(Keyspace::Kernel, &preview_key(task, self.id.0))? else {
            return Ok(None);
        };
        let header = PreviewHeader::decode(&value)?;
        if header.kind != PreviewKind::Change as u8
            || header.state != STATE_OPEN
            || header.expires_ms <= self.db.wall_now_ms()
            || header.creator != self.ctx.executor
            || header.digest != self.digest
        {
            return Ok(None);
        }
        let Some(def) = catalog_def(&self.db, task, DefAt::Snapshot(txn))? else {
            return Ok(None);
        };
        if header.def_version != def.def_version || header.table != def.table {
            return Ok(None);
        }
        let state = self.db.catalog().snapshot(txn)?.image().map_err(CoreError::Sql)?;
        let Some(table) = state.tables.values().find(|table| table.id == header.table).cloned()
        else {
            return Ok(None);
        };
        if schema_fp(&table, state.table_constraints(table.id))? != header.schema_fp {
            return Ok(None);
        }
        Ok(Some(Checked { header, table, def }))
    }

    /// D7.6 step 5's role test: the approving role still exists (the same
    /// role, by oid), is not the executor, is the task's named approver when
    /// there is one, and still holds the approve privilege.
    fn approver_holds(
        &self,
        txn: &mut Transaction,
        def: &TaskDef,
        approval: &ApprovalRecord,
    ) -> Result<bool, CoreError> {
        let role = match role_txn(txn, &approval.approver) {
            Ok(Some(role)) => role,
            Ok(None) => return Ok(false),
            Err(RoleError::Core(error)) => return Err(error),
            Err(_) => return Ok(false),
        };
        Ok((approval.approver_oid == 0 || role.oid == approval.approver_oid)
            && role.name != def.executor
            && def.approver.as_ref().is_none_or(|approver| *approver == role.name)
            && def.privileges_of(&role).contains(TaskPrivileges::APPROVE))
    }
}

impl ApplySource for StoredApply {
    /// Steps 1 and 2: the header at the latest committed state (the hold
    /// keeps previews, approvals and applies of this task out), then the
    /// stored text prepared under the current catalog and the stored
    /// parameters. The commit applies the task's column rules to it.
    fn statement(&self, session: &mut SqlSession) -> Result<(Prepared, Vec<Value>), TaskError> {
        self.header.replace(None);
        self.used.replace(None);
        let mut read = self.db.begin(Isolation::ReadCommitted);
        let checked = self.checked(&mut read)?.ok_or(TaskError::ApprovalStale)?;
        drop(read);
        let header = checked.header;
        // Native previews store their SQL lowering (see `preview`).
        if header.surface != Surface::Sql as u8 && header.surface != Surface::Native as u8 {
            return Err(unsupported());
        }
        let prepared = session.prepare(&header.text)?;
        if prepared.task_class() != StatementClass::Update {
            return Err(TaskError::ApprovalStale);
        }
        let params = crate::storage::task_row_values(&header.params)?;
        self.header.replace(Some(header));
        Ok((prepared, params))
    }

    /// The preview's pinned instant (D7.4). A preview that observed no
    /// instant re-runs with time functions unavailable, so a re-run that
    /// would read one fails instead of reading a new value.
    fn clock(&self) -> Option<ExecutionClock> {
        let pin = self
            .header
            .borrow()
            .as_ref()
            .map_or(NO_CLOCK_PIN, |header| header.clock_pin);
        let mut clock = ExecutionClock::pinned(if pin == NO_CLOCK_PIN { 0 } else { pin }, None);
        if pin == NO_CLOCK_PIN {
            clock.unavailable = Some(CoreError::Unsupported);
        }
        Some(ExecutionClock::new(&clock))
    }

    /// Step 1 again and step 3 in the tracked transaction (a concurrent DDL
    /// or row change fails the commit's validation), then step 5 when the
    /// effects are above the threshold: the `29` record active, with the
    /// header's digest, from a role that still may approve. At or under the
    /// threshold no approval is read or consumed (`Missing` commits there).
    fn approval_state(
        &self,
        txn: &mut Transaction,
        effects: &Effects,
    ) -> Result<ApprovalState, CoreError> {
        self.used.replace(None);
        let Some(Checked { header, table, def }) = self.checked(txn)? else {
            return Ok(ApprovalState::Stale);
        };
        if self.header.borrow().as_ref() != Some(&header) {
            return Ok(ApprovalState::Stale);
        }
        let task = self.ctx.task;
        let count = stored_rows(&header);
        let mut rows = Vec::with_capacity(usize::try_from(count).unwrap_or(0));
        for row_no in 0..count {
            let key = preview_row_key(task, self.id.0, row_no);
            let Some(value) = txn.get(Keyspace::Kernel, &key)? else {
                return Ok(ApprovalState::Stale);
            };
            rows.push(PreviewRow::decode(&value)?);
        }
        if !matches(effects, &rows, table.columns.len()) {
            return Ok(ApprovalState::Stale);
        }
        if u64::try_from(effects.len()).unwrap_or(u64::MAX) <= def.approval_threshold {
            return Ok(ApprovalState::Missing);
        }
        let Some(value) = txn.get(Keyspace::Kernel, &approval_key(task, self.id.0))? else {
            return Ok(ApprovalState::Missing);
        };
        let approval = ApprovalRecord::decode(&value)?;
        if approval.state != APPROVAL_ACTIVE
            || approval.digest != header.digest
            || !self.approver_holds(txn, &def, &approval)?
        {
            return Ok(ApprovalState::Missing);
        }
        self.used.replace(Some(approval));
        Ok(ApprovalState::Matching)
    }

    /// Step 6: the header applied at `seq`, its `28` rows deleted and the
    /// approval, if one was used, consumed at `seq`.
    fn stage_consumed(&self, txn: &mut Transaction, seq: u64) -> Result<(), CoreError> {
        let task = self.ctx.task;
        let mut header = self
            .header
            .borrow()
            .clone()
            .ok_or_else(|| CoreError::Corrupt("apply without a checked preview".into()))?;
        for row_no in 0..stored_rows(&header) {
            txn.delete(Keyspace::Kernel, preview_row_key(task, self.id.0, row_no))?;
        }
        header.state = STATE_APPLIED;
        header.applied_seq = seq;
        txn.put(Keyspace::Kernel, preview_key(task, self.id.0), header.encode())?;
        if let Some(mut approval) = self.used.borrow().clone() {
            approval.state = APPROVAL_CONSUMED;
            approval.consumed_seq = seq;
            txn.put(Keyspace::Kernel, approval_key(task, self.id.0), approval.encode())?;
        }
        Ok(())
    }

    fn approver(&self) -> Option<RoleName> {
        self.used.borrow().as_ref().map(|approval| approval.approver.clone())
    }
}

/// One stored preview row, as [`StoredPreview`] shows it.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct StoredPreviewRow {
    pub class: u8,
    pub table: u64,
    pub pk: Vec<u8>,
    pub version_read: u64,
    pub aux: u64,
    /// The decoded after-values (class 00), empty otherwise.
    pub after: Vec<Value>,
}

/// One stored preview header and its rows. For tests.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct StoredPreview {
    pub id: PreviewId,
    pub kind: u8,
    pub state: u8,
    pub created_ms: u64,
    pub expires_ms: u64,
    pub def_version: u64,
    pub table: u64,
    pub schema_fp: [u8; 32],
    /// `None` when the statement observed no instant.
    pub clock_pin: Option<i64>,
    pub digest: [u8; 32],
    pub counts: [u32; 4],
    pub creator: String,
    pub surface: u8,
    pub text: String,
    pub params: Vec<Value>,
    pub rows: Vec<StoredPreviewRow>,
}

/// The preview store of one task: the head's counters and the stored previews.
#[doc(hidden)]
#[derive(Clone, Debug, Default)]
pub struct PreviewStore {
    pub next_preview: u64,
    pub stored_previews: u32,
    pub previews: Vec<StoredPreview>,
    /// Every `28` row record of the task, under any header or none.
    pub stored_rows: u64,
}

impl crate::storage::Admin {
    /// The stored previews of `task` and the head's preview counters, at the
    /// latest committed state (v2 only). For tests.
    #[doc(hidden)]
    pub fn task_previews(&self, task: TaskObjectId) -> Result<PreviewStore, CoreError> {
        let db = self.sql_session().v2_db().ok_or(CoreError::Unsupported)?;
        let mut txn = db.begin(Isolation::ReadCommitted);
        let head = authority::load_head(&mut txn, task)?;
        let mut previews = Vec::new();
        for (id, header) in headers(&mut txn, task)? {
            let mut rows = Vec::new();
            for row_no in 0..stored_rows(&header) {
                let key = preview_row_key(task, id.0, row_no);
                let Some(value) = txn.get(Keyspace::Kernel, &key)? else {
                    continue;
                };
                let row = PreviewRow::decode(&value)?;
                let after = if row.after.is_empty() {
                    Vec::new()
                } else {
                    crate::storage::task_row_values(&row.after)?
                };
                rows.push(StoredPreviewRow {
                    class: row.class,
                    table: row.table,
                    pk: row.pk,
                    version_read: row.version_read,
                    aux: row.aux,
                    after,
                });
            }
            previews.push(StoredPreview {
                id,
                kind: header.kind,
                state: header.state,
                created_ms: header.created_ms,
                expires_ms: header.expires_ms,
                def_version: header.def_version,
                table: header.table,
                schema_fp: header.schema_fp,
                clock_pin: (header.clock_pin != NO_CLOCK_PIN).then_some(header.clock_pin),
                digest: header.digest,
                counts: header.counts,
                creator: header.creator,
                surface: header.surface,
                text: header.text,
                params: crate::storage::task_row_values(&header.params)?,
                rows,
            });
        }
        let (start, end) = task_prefix(NS_PREVIEW_ROW, task);
        let mut stored_rows = 0u64;
        for entry in txn.scan(
            Keyspace::Kernel,
            Bound::Included(start.as_slice()),
            Bound::Excluded(end.as_slice()),
            Direction::Forward,
            None,
        )? {
            entry?;
            stored_rows += 1;
        }
        Ok(PreviewStore {
            next_preview: head.next_preview,
            stored_previews: head.stored_previews,
            previews,
            stored_rows,
        })
    }

    /// For tests: PREVIEW of `text` for `ctx` whose dry run skips the policy
    /// and the caps (everything else, the store included, runs as usual), so
    /// a test can show that apply decides them itself: an approval never
    /// widens what the task may write.
    #[doc(hidden)]
    pub fn preview_task_without_policy(
        &self,
        ctx: &TaskContext,
        text: &str,
        clock: &StatementClock,
    ) -> Result<PreviewSummary, TaskError> {
        let mut session = self.sql_session();
        let db = session.v2_db().ok_or_else(unsupported)?;
        let dry = dry_run(&mut session, &db, ctx, text, &[], clock, Policy::Skip)?;
        store(&db, ctx, Surface::Sql, text, &[], dry)
    }

    /// For tests: rewrites the stored `def_version` of preview `id` of
    /// `task`, keeping its digest (R3 has no ALTER TASK, and a revoke
    /// already stops the executor's session).
    #[doc(hidden)]
    pub fn set_task_preview_def_version(
        &self,
        task: TaskObjectId,
        id: PreviewId,
        def_version: u64,
    ) -> Result<(), CoreError> {
        let db = self.sql_session().v2_db().ok_or(CoreError::Unsupported)?;
        let mut txn = db.begin(Isolation::ReadCommitted);
        let value = txn
            .get(Keyspace::Kernel, &preview_key(task, id.0))?
            .ok_or_else(|| CoreError::Corrupt("no preview to rewrite".into()))?;
        let mut header = PreviewHeader::decode(&value)?;
        header.def_version = def_version;
        txn.put(Keyspace::Kernel, preview_key(task, id.0), header.encode())?;
        db.catalog().commit_ddl(txn)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::records::PreviewScope;
    use crate::relational::{ColumnDef, DataType};

    fn header() -> PreviewHeader {
        PreviewHeader {
            kind: 1,
            state: STATE_OPEN,
            created_ms: 1,
            expires_ms: 1000,
            def_version: 5,
            table: 9,
            schema_fp: [0xAA; 32],
            clock_pin: NO_CLOCK_PIN,
            digest: [0; 32],
            counts: [1, 0, 0, 0],
            applied_seq: 0,
            creator: "alice".into(),
            scope: None,
            surface: 0,
            text: "sql".into(),
            params: Vec::new(),
        }
    }
    fn row() -> PreviewRow {
        PreviewRow { class: 0, table: 9, pk: vec![0, 1], version_read: 4, aux: 0, after: vec![0xBE, 0xEF] }
    }
    fn domain() -> [u8; 16] {
        std::array::from_fn(|i| i as u8)
    }

    #[test]
    fn digest_matches_fixed_vector() {
        let mut pre = b"GDPV\x01".to_vec();
        pre.push(1);
        pre.extend_from_slice(&domain());
        for v in [7u64, 3, 5, 9] {
            pre.extend_from_slice(&v.to_be_bytes());
        }
        pre.extend_from_slice(&[0xAA; 32]);
        pre.extend_from_slice(&i64::MIN.to_be_bytes());
        pre.extend_from_slice(&1000u64.to_be_bytes());
        pre.push(0);
        pre.extend_from_slice(&3u32.to_be_bytes());
        pre.extend_from_slice(b"sql");
        pre.extend_from_slice(&0u32.to_be_bytes());
        pre.extend_from_slice(&1u32.to_be_bytes());
        pre.push(0);
        pre.extend_from_slice(&9u64.to_be_bytes());
        pre.extend_from_slice(&2u32.to_be_bytes());
        pre.extend_from_slice(&[0, 1]);
        pre.extend_from_slice(&4u64.to_be_bytes());
        pre.extend_from_slice(&0u64.to_be_bytes());
        pre.extend_from_slice(&2u32.to_be_bytes());
        pre.extend_from_slice(&[0xBE, 0xEF]);
        let built = preimage(domain(), TaskObjectId(7), PreviewId(3), &header(), &[row()]);
        assert_eq!(built, pre);
        let d = digest(domain(), TaskObjectId(7), PreviewId(3), &header(), &[row()]);
        assert_eq!(digest_hex(&d), "859fbce3344b35ed8401fafb10d92e2e56a80e4b33b682a1cc68e06530933de1");
        // Fields outside the preimage (state, created, counts, creator, digest) do not matter.
        let mut h = header();
        h.state = STATE_APPLIED;
        h.created_ms = 99;
        h.counts = [9, 9, 9, 9];
        h.creator = "bob".into();
        h.digest = [1; 32];
        assert_eq!(digest(domain(), TaskObjectId(7), PreviewId(3), &h, &[row()]), d);
    }

    #[test]
    fn digest_changes_with_each_field() {
        let base = digest(domain(), TaskObjectId(7), PreviewId(3), &header(), &[row()]);
        let mut seen = std::collections::BTreeSet::new();
        seen.insert(base);
        let mut check = |d: [u8; 32]| assert!(seen.insert(d), "digest did not change");
        let mut dom = domain();
        dom[0] ^= 1;
        check(digest(dom, TaskObjectId(7), PreviewId(3), &header(), &[row()]));
        check(digest(domain(), TaskObjectId(8), PreviewId(3), &header(), &[row()]));
        check(digest(domain(), TaskObjectId(7), PreviewId(4), &header(), &[row()]));
        let headers: Vec<fn(&mut PreviewHeader)> = vec![
            |h| {
                h.kind = 2;
                h.scope = Some(PreviewScope::default());
            },
            |h| h.def_version += 1,
            |h| h.table += 1,
            |h| h.schema_fp[31] ^= 1,
            |h| h.clock_pin = 0,
            |h| h.expires_ms += 1,
            |h| h.surface = 1,
            |h| h.text.push('x'),
            |h| h.params.push(0),
        ];
        for f in headers {
            let mut h = header();
            f(&mut h);
            check(digest(domain(), TaskObjectId(7), PreviewId(3), &h, &[row()]));
        }
        // Undo scope bytes are hashed.
        let mut h = header();
        h.kind = 2;
        h.scope = Some(PreviewScope { kind: 1, outcome: [3; 32] });
        check(digest(domain(), TaskObjectId(7), PreviewId(3), &h, &[row()]));
        let rows: Vec<fn(&mut PreviewRow)> = vec![
            |r| {
                r.class = 1;
                r.after.clear();
            },
            |r| r.table += 1,
            |r| r.pk.push(2),
            |r| r.version_read += 1,
            |r| r.aux += 1,
            |r| r.after.push(0),
        ];
        for f in rows {
            let mut r = row();
            f(&mut r);
            check(digest(domain(), TaskObjectId(7), PreviewId(3), &header(), &[r]));
        }
        check(digest(domain(), TaskObjectId(7), PreviewId(3), &header(), &[]));
        check(digest(domain(), TaskObjectId(7), PreviewId(3), &header(), &[row(), PreviewRow { pk: vec![5], ..row() }]));
        // Row order passed in does not matter.
        let a = PreviewRow { pk: vec![1], ..row() };
        let b = PreviewRow { pk: vec![2], ..row() };
        assert_eq!(
            digest(domain(), TaskObjectId(7), PreviewId(3), &header(), &[a.clone(), b.clone()]),
            digest(domain(), TaskObjectId(7), PreviewId(3), &header(), &[b, a]),
        );
    }

    #[test]
    fn rows_sorted_by_table_then_pk() {
        let r = |v: i64, version: u64| SqlRow { values: vec![Value::Int(v)], version };
        let (o1, n1, o2, n2, o3, n3) = (r(1, 10), r(2, 11), r(3, 20), r(4, 21), r(5, 30), r(6, 31));
        let effects = vec![
            EffectView { table: 2, pk: &[1], old: Some(&o1), new: Some(&n1) },
            EffectView { table: 1, pk: &[9], old: Some(&o2), new: Some(&n2) },
            EffectView { table: 1, pk: &[3], old: Some(&o3), new: Some(&n3) },
        ];
        let rows = rows_from_effects(effects).unwrap();
        let keys: Vec<(u64, Vec<u8>, u64)> = rows.iter().map(|r| (r.table, r.pk.clone(), r.version_read)).collect();
        assert_eq!(keys, vec![(1, vec![3], 30), (1, vec![9], 20), (2, vec![1], 10)]);
        assert!(rows.iter().all(|r| r.class == 0 && r.aux == 0));
        assert_eq!(rows[0].after, encode_after(&[Value::Int(6)]).unwrap());
        // Round trip through the R3-600 codec.
        for row in &rows {
            assert_eq!(&PreviewRow::decode(&row.encode()).unwrap(), row);
        }
        let h = header();
        assert_eq!(PreviewHeader::decode(&h.encode()).unwrap(), h);
        // Missing old or new is denied.
        let bad = vec![EffectView { table: 1, pk: &[1], old: None, new: Some(&n1) }];
        assert_eq!(rows_from_effects(bad).unwrap_err(), TaskError::Denied(DenyRule::PrimaryKeyChange));
        let bad = vec![EffectView { table: 1, pk: &[1], old: Some(&o1), new: None }];
        assert_eq!(rows_from_effects(bad).unwrap_err(), TaskError::Denied(DenyRule::PrimaryKeyChange));
    }

    #[test]
    fn matches_is_the_exact_d7_6_comparison() {
        let r = |v: i64, version: u64| SqlRow { values: vec![Value::Int(v)], version };
        let key = |pk: u8| {
            let mut k = 9u64.to_be_bytes().to_vec();
            k.push(pk);
            k
        };
        let mut effects = Effects::default();
        effects.record(9, &key(1), Some(&r(1, 10)), Some(&r(2, 11))).unwrap();
        effects.record(9, &key(2), Some(&r(3, 20)), Some(&r(4, 21))).unwrap();
        let stored = |pk: u8, version_read: u64, after: i64| PreviewRow {
            class: 0,
            table: 9,
            pk: vec![pk],
            version_read,
            aux: 0,
            after: encode_after(&[Value::Int(after), Value::Null]).unwrap(),
        };
        let rows = vec![stored(1, 10, 2), stored(2, 20, 4)];
        // New values are resized to the table width before encoding.
        assert!(matches(&effects, &rows, 2));
        assert!(!matches(&effects, &rows, 1));
        // A missing or extra row, a changed version, a different new value,
        // another key or another class are all stale.
        assert!(!matches(&effects, &rows[..1], 2));
        assert!(!matches(&effects, &[rows.clone(), vec![stored(3, 30, 6)]].concat(), 2));
        assert!(!matches(&effects, &[stored(1, 12, 2), stored(2, 20, 4)], 2));
        assert!(!matches(&effects, &[stored(1, 10, 5), stored(2, 20, 4)], 2));
        assert!(!matches(&effects, &[stored(1, 10, 2), stored(3, 20, 4)], 2));
        assert!(!matches(&effects, &[stored(1, 10, 2), PreviewRow { class: 1, ..stored(2, 20, 4) }], 2));
        assert!(!matches(&effects, &[PreviewRow { table: 8, ..stored(1, 10, 2) }, stored(2, 20, 4)], 2));
        // An effect without an old or a new row never matches.
        let mut deleted = Effects::default();
        deleted.record(9, &key(1), Some(&r(1, 10)), None).unwrap();
        assert!(!matches(&deleted, &rows[..1], 2));
        assert!(matches(&Effects::default(), &[], 2));
    }

    fn table(id: u64, name: &str, cols: &[&str]) -> TableDef {
        TableDef {
            id,
            name: name.into(),
            columns: cols
                .iter()
                .enumerate()
                .map(|(i, c)| ColumnDef { id: i as u64 + 1, name: (*c).into(), data_type: DataType::BigInt, not_null: false })
                .collect(),
            primary_key: vec![0],
        }
    }

    #[test]
    fn schema_fp_changes_on_add_column_and_not_on_unrelated_table() {
        let t = table(1, "public.t", &["id", "a"]);
        let fp = schema_fp(&t, &[]).unwrap();
        assert_eq!(fp, schema_fp(&t.clone(), &[]).unwrap());
        let added = table(1, "public.t", &["id", "a", "b"]);
        assert_ne!(fp, schema_fp(&added, &[]).unwrap());
        // Another table's DDL is not an input: the fingerprint of `t` is unchanged.
        let _other = table(2, "public.u", &["id", "z"]);
        assert_eq!(fp, schema_fp(&t, &[]).unwrap());
        // Constraints are hashed in id order, whatever order they come in.
        let c1 = Constraint { id: 10, name: "t_pkey".into(), kind: crate::relational::catalog_ext::ConstraintKind::PrimaryKey };
        let c2 = Constraint {
            id: 11,
            name: "t_a_check".into(),
            kind: crate::relational::catalog_ext::ConstraintKind::Check { expr: "a > 0".into(), not_valid: false },
        };
        let with = schema_fp(&t, &[c1.clone(), c2.clone()]).unwrap();
        assert_ne!(fp, with);
        assert_eq!(with, schema_fp(&t, &[c2, c1]).unwrap());
    }
}
