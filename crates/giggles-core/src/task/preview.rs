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
use super::admit::StatementClass;
use super::authority;
use super::columns;
use super::commit::{catalog_def, engine, hold_error, role_error, standing, unsupported, DefAt};
use super::context::TaskContext;
use super::decide::{self, ApprovalState, Decision, RequestKind};
use super::definition::{TaskDef, WallClockMs};
use super::effects::Effects;
use super::error::{Budget, DenyRule, TaskError};
use super::records::{
    approval_key, mutation_head_key, preview_key, preview_row_key, task_prefix, MutationHead,
    TaskObjectId,
    MAX_STATEMENT_TEXT, NS_PREVIEW, NS_PREVIEW_ROW,
};
pub(crate) use super::records::{PreviewHeader, PreviewRow};
use crate::CoreError;
use crate::relational::clock::{
    ExecutionClock, StatementClock, OBS_INSTANT, OBS_SEQUENCE, OBS_WALL, OBS_XACT, OBS_ZONE,
};
use crate::relational::{SqlRow, TableDef, Value, catalog_ext::Constraint};
use crate::storage::engine::kv::Direction;
use crate::storage::roles::{table_privileges_txn, TablePrivilege};
use crate::storage::{Isolation, Keyspace, ProtectedRun, SqlSession, Transaction, V2Db};
use sha2::{Digest, Sha256};
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
/// Only `Surface::Sql` is prepared here; native text is 0A000 until a
/// native preparer reaches core.
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
    if surface != Surface::Sql {
        return Err(unsupported());
    }
    if text.len() > MAX_STATEMENT_TEXT || u16::try_from(params.len()).is_err() {
        return Err(CoreError::Limit.into());
    }
    let dry = dry_run(session, &db, ctx, text, params, clock)?;
    store(&db, ctx, surface, text, params, dry)
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
    match decide::evaluate(
        &state,
        &def,
        &table,
        &head,
        &effects,
        &charge,
        RequestKind::Apply,
        ApprovalState::Matching,
    ) {
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
    let task = ctx.task;
    // 1. The hold, then a statement snapshot taken after it, then the head.
    let mut txn = db.begin(Isolation::ReadCommitted);
    let deadline = Instant::now().checked_add(Duration::from_millis(dry.def.limits.lock_wait_ms));
    txn.lock_kernel_key(&mutation_head_key(task), deadline, &|| {
        crate::cancellation::check().is_err()
    })
    .map_err(hold_error)?;
    txn.begin_statement();
    let head = authority::load_head(&mut txn, task)?;
    // 2. Revoked, expired and the definition version again.
    let now = db.wall_now_ms();
    let def = catalog_def(db, task, DefAt::Snapshot(&mut txn))?;
    standing(def.as_deref(), ctx, now)?;
    let def = def.ok_or(TaskError::Revoked)?;
    if def.def_version != dry.def.def_version || def.table != dry.def.table {
        return Err(TaskError::Revoked);
    }
    // 3. Sweep this task's expired and applied previews.
    let open = sweep(&mut txn, task, now)?;
    // 4. The limit on open previews.
    if open >= def.limits.stored_previews {
        return Err(TaskError::BudgetExceeded(Budget::StoredPreviews));
    }
    // 5. The id.
    let id = PreviewId(head.next_preview);
    let overflow = || TaskError::from(CoreError::Limit);
    let rows = u32::try_from(dry.rows.len()).map_err(|_| overflow())?;
    let ttl_ms = def.limits.preview_ttl_s.saturating_mul(1000);
    let task_expiry = u64::try_from(def.expires_at.0).unwrap_or(0);
    let expires_ms = now.saturating_add(ttl_ms).min(task_expiry);
    let mut header = PreviewHeader {
        kind: PreviewKind::Change as u8,
        state: STATE_OPEN,
        created_ms: now,
        expires_ms,
        def_version: def.def_version,
        table: def.table,
        schema_fp: dry.schema_fp,
        clock_pin: dry.clock_pin,
        digest: [0; 32],
        counts: [rows, 0, 0, 0],
        applied_seq: 0,
        creator: ctx.executor.clone(),
        scope: None,
        surface: surface as u8,
        text: text.to_owned(),
        params: encode_after(params)?,
    };
    header.digest = digest(db.meta.domain, task, id, &header, &dry.rows);
    // 6. Header, rows (ascending (table, pk)) and head, in one commit.
    let put = |txn: &mut Transaction, key: Vec<u8>, value: Vec<u8>| -> Result<(), TaskError> {
        txn.put(Keyspace::Kernel, key, value).map_err(CoreError::from)?;
        Ok(())
    };
    put(&mut txn, preview_key(task, id.0), header.encode())?;
    for (row_no, row) in dry.rows.iter().enumerate() {
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
    Ok(PreviewSummary {
        id,
        digest: header.digest,
        rows,
        expires_at: WallClockMs(i64::try_from(expires_ms).unwrap_or(i64::MAX)),
    })
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
