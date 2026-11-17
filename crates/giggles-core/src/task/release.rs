//! Release of task-session results (GD-0061 R3-301 and R3-308, design-3 section 4).
//!
//! Every core exit that can hand rows to a caller has an `*_unreleased` twin
//! returning [`Unreleased<T>`]. An `Unreleased` value exposes nothing: the only
//! ways out are [`SqlSession::release_page`], which turns it into a [`Released<T>`]
//! (the only thing encoders accept), and [`Unreleased::untasked`], which works
//! only when the value was produced outside a task session.
//!
//! The plain exits (`execute`, `execute_prepared`, `Cursor::fetch`,
//! `execute_native`, ...) are kept for trusted local callers. They refuse a task
//! session before running anything, so a caller that was never converted to the
//! twins cannot leak a task's rows. A write in a task session is refused by the
//! twins too, before it runs: the only write path of a task is the protected
//! commit (`task::commit`).
//!
//! Without a task, `release_page` is a move: no lock, catalog read,
//! transaction or allocation. A task page goes through the release protocol
//! (R3-308), in this order, and nothing reaches an encoder before the last
//! step:
//!
//! 1. the statement deadline (`cancellation::check`);
//! 2. masking in memory with the statement's [`OutputPlan`] ([`mask_page`]);
//!    the cost is taken after masking;
//! 3. validity from the cached catalog view and the database wall clock
//!    (revoked, then expired, then definition changed), on every page;
//! 4. unless the session's reservation covers the page and no `rows_read` is
//!    pending: a transaction of its own takes the engine lock on the
//!    disclosure head `20|task|01`, re-checks validity at its snapshot, checks
//!    the head plus the need against the disclosure caps (nothing is written
//!    or released when one is exceeded), and writes the new head, the debit
//!    `2B|task|read_seq|page_no` and, on the statement's first debit, the
//!    read-log record `2A|task|read_seq` (outcome open; `readlog.rs`
//!    finalizes it when the statement ends), then commits synced;
//! 5. hand-over.
//!
//! A crash before the commit leaves nothing charged and nothing sent; a crash
//! after it leaves the page charged and possibly not received. No ordering
//! leaves a sent page uncharged.
//!
//! # Byte cost
//!
//! Fixed and independent of `relational::value_bytes` (an internal memory
//! cap that may change):
//!
//! `row_cost(row) = 8 + Σ (1 + size(v))`, where `size` is 0 for NULL, 1 for
//! Bool, 8 for Int, Float8, Time, Timestamp and TimestampTz, 4 for Float4 and
//! Date, 12 for TimeTz, 16 for Interval, the length of the decimal string for
//! Numeric, the UTF-8 length for Text and the length for Bytes. A masked
//! value costs whatever replaced it. The cost is the same on every wire format.
//!
//! # Reservation
//!
//! With chunking on ([`SqlSession::set_disclosure_chunk`], defaults
//! [`DEFAULT_CHUNK_ROWS`] and [`DEFAULT_CHUNK_BYTES`]; zero means exact
//! debits), a durable debit takes `max(need, min(chunk, remaining / 8))`
//! returned rows and bytes, never more than `remaining`. The surplus is held
//! in session memory and used by later pages of the same session only.
//! `rows_read` is always debited exactly (the debit record has no reserved
//! read rows). Session end, a release error or a crash forfeit the
//! reservation: there is no refund path anywhere, including for a cancelled
//! or failed statement. The debit record carries `reserved_rows` and
//! `reserved_bytes`, so the head always equals the sum of the debits.
//!
//! # `rows_read`
//!
//! The scoped row sources count the rows the visible-rows predicate accepted.
//! The count is pending on the session until a release debits it; a
//! statement that ends without a release debits it in
//! [`SqlSession::finish_task_statement`], saturating at the cap.
//!
//! ```compile_fail
//! // `Unreleased` has no public field and no public constructor.
//! let page = giggles_core::Unreleased { value: Vec::<u8>::new(), tag: None };
//! ```
//!
//! ```compile_fail
//! // No accessor reads the value of an `Unreleased` page.
//! fn peek(page: &giggles_core::Unreleased<giggles_core::relational::SqlResult>) -> usize {
//!     page.rows.len()
//! }
//! ```
//!
//! ```compile_fail
//! // `Unreleased` is not `Clone`, so it cannot be duplicated around a release.
//! fn copy(page: &giggles_core::Unreleased<Vec<giggles_core::relational::SqlRow>>)
//!     -> giggles_core::Unreleased<Vec<giggles_core::relational::SqlRow>> {
//!     page.clone()
//! }
//! ```
//!
//! ```compile_fail
//! // `Unreleased` is not `Serialize`.
//! fn encode(page: &giggles_core::Unreleased<giggles_core::relational::SqlResult>) -> String {
//!     serde_json::to_string(page).unwrap()
//! }
//! ```
use std::cell::RefCell;
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use super::admit::StatementClass;
use super::columns::{OutputMode, OutputPlan};
use super::commit::{catalog_def, DefAt};
use super::context::TaskContext;
use super::definition::{DisclosureCaps, TaskDef};
use super::error::{Budget, DenyRule, TaskError};
use super::readlog::{fill_left, StatementEntry, StatementLog};
use super::records::{
    debit_key, disclosure_head_key, read_log_key, task_prefix, DebitRecord, DisclosureHead,
    ReadLogRecord, NS_DEBIT, NS_READ_LOG,
};
use super::records::TaskObjectId;
use crate::operations::{EmittedValue, Outcome, TransactionOutcome};
use crate::relational::{SqlResult, SqlRow, Value};
use crate::storage::engine::kv::Direction;
use crate::storage::engine::txn::TxnError;
use crate::storage::{
    CoreError, Isolation, Keyspace, SqlSession, Transaction, V2Db, PROTECTED_RECORDS,
};
use crate::CrashPoint;

/// Default reservation chunk, returned rows (`agent.disclosure_chunk_rows`).
pub const DEFAULT_CHUNK_ROWS: u64 = 256;
/// Default reservation chunk, returned bytes (`agent.disclosure_chunk_bytes`).
pub const DEFAULT_CHUNK_BYTES: u64 = 262_144;

/// The cost of one value: `1 + size(v)` (see the module docs).
pub fn value_cost(value: &Value) -> u64 {
    let size = match value {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Int(_)
        | Value::Float8(_)
        | Value::Time(_)
        | Value::Timestamp(_)
        | Value::TimestampTz(_) => 8,
        Value::Float4(_) | Value::Date(_) => 4,
        Value::TimeTz(_) => 12,
        Value::Interval(_) => 16,
        Value::Numeric(n) => n.to_string().len() as u64,
        Value::Text(text) => text.len() as u64,
        Value::Bytes(bytes) => bytes.len() as u64,
    };
    1 + size
}

/// The cost of one released row: 8 plus the cost of each of its values.
pub fn row_cost(row: &[Value]) -> u64 {
    row.iter()
        .fold(8u64, |sum, value| sum.saturating_add(value_cost(value)))
}

/// What masking one page did, and what the page costs after masking.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counts {
    pub rows: u64,
    pub bytes: u64,
    /// Values replaced by a masked column's NULL or constant.
    pub masked: u64,
    /// Untrusted values withheld as NULL.
    pub withheld: u64,
    /// Untrusted values released because the task allows them.
    pub untrusted_released: u64,
}

/// Applies `plan` to every row of `page` in place and returns the counts.
///
/// A row whose width differs from the plan cannot be masked by it: every
/// value of such a row is replaced by NULL and the page is refused (fail
/// closed), so the caller releases nothing.
pub fn mask_page<T: Page + ?Sized>(page: &mut T, plan: &OutputPlan) -> Result<Counts, CoreError> {
    let mut counts = Counts::default();
    let mut misaligned = false;
    page.for_each_row_mut(&mut |row| {
        if row.len() != plan.outputs.len() {
            misaligned = true;
            row.iter_mut().for_each(|value| *value = Value::Null);
            return;
        }
        for (value, mode) in row.iter_mut().zip(&plan.outputs) {
            match mode {
                OutputMode::Plain => {}
                OutputMode::Mask(constant) => {
                    *value = constant.clone();
                    counts.masked += 1;
                }
                OutputMode::Withhold => {
                    *value = Value::Null;
                    counts.withheld += 1;
                }
                OutputMode::CountUntrusted => counts.untrusted_released += 1,
            }
        }
        counts.rows += 1;
        counts.bytes = counts.bytes.saturating_add(row_cost(row));
    });
    if misaligned {
        return Err(CoreError::Task(TaskError::Denied(DenyRule::ColumnNotAvailable)));
    }
    Ok(counts)
}

/// Returned rows and bytes debited ahead of use, held in session memory.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Reservation {
    pub rows: u64,
    pub bytes: u64,
}

/// The release state of one task session, created by `bind_task`.
#[derive(Debug)]
pub(crate) struct TaskSessionState {
    ctx: TaskContext,
    reservation: Mutex<Reservation>,
    /// Rows the scope accepted that no debit has charged yet.
    pending_rows_read: AtomicU64,
    /// Rows the scoped sources pulled that no read-log record holds yet.
    pending_rows_scanned: AtomicU64,
    chunk_rows: AtomicU64,
    chunk_bytes: AtomicU64,
    /// The entry point statements are logged with (`set_task_entry`).
    entry: AtomicU8,
    /// The text the next statement is logged with.
    pub(crate) pending_text: Mutex<Option<Arc<str>>>,
    /// The statement started last; `finish_task_statement` ends it.
    pub(crate) current: Mutex<Option<Weak<StatementEntry>>>,
}

impl TaskSessionState {
    pub(crate) fn new(ctx: TaskContext) -> Self {
        Self {
            ctx,
            reservation: Mutex::new(Reservation::default()),
            pending_rows_read: AtomicU64::new(0),
            pending_rows_scanned: AtomicU64::new(0),
            chunk_rows: AtomicU64::new(DEFAULT_CHUNK_ROWS),
            chunk_bytes: AtomicU64::new(DEFAULT_CHUNK_BYTES),
            entry: AtomicU8::new(super::readlog::ENTRY_PGWIRE_SIMPLE),
            pending_text: Mutex::new(None),
            current: Mutex::new(None),
        }
    }

    pub(crate) fn ctx(&self) -> &TaskContext {
        &self.ctx
    }

    pub(crate) fn entry(&self) -> u8 {
        self.entry.load(Ordering::SeqCst)
    }

    pub(crate) fn set_entry(&self, entry: u8) {
        self.entry.store(entry, Ordering::SeqCst);
    }

    /// Adds rows the visible-rows scope accepted to the pending `rows_read`.
    pub(crate) fn add_rows_read(&self, rows: u64) {
        if rows > 0 {
            self.pending_rows_read.fetch_add(rows, Ordering::SeqCst);
        }
    }

    /// Takes a scoped read's new counts: accepted rows into the pending
    /// `rows_read`, pulled rows into the pending `rows_scanned`.
    pub(crate) fn add_meter(&self, meter: &super::rows::StatementMeter) {
        self.add_rows_read(meter.take_read_delta());
        let scanned = meter.take_scanned_delta();
        if scanned > 0 {
            self.pending_rows_scanned.fetch_add(scanned, Ordering::SeqCst);
        }
    }

    pub(crate) fn take_rows_read(&self) -> u64 {
        self.pending_rows_read.swap(0, Ordering::SeqCst)
    }

    pub(crate) fn take_rows_scanned(&self) -> u64 {
        self.pending_rows_scanned.swap(0, Ordering::SeqCst)
    }

    fn reservation(&self) -> MutexGuard<'_, Reservation> {
        lock(&self.reservation)
    }
}

impl Drop for TaskSessionState {
    /// Session end forfeits the reservation: it was charged when it was
    /// debited, and nothing ever refunds it.
    fn drop(&mut self) {
        *self.reservation() = Reservation::default();
    }
}

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// What the release of one task statement needs. Created when the statement
/// starts in a task session and shared by every page it produces.
#[derive(Debug)]
pub struct ReleaseTag {
    state: Arc<TaskSessionState>,
    /// The statement's output plan (R3-303 for SQL, R3-304 for native), or
    /// the denial its column rules gave; a denied plan refuses every page.
    plan: Result<OutputPlan, TaskError>,
    /// The statement's read-log entry; its last handle finalizes the record.
    entry: Arc<StatementEntry>,
}

impl ReleaseTag {
    pub(crate) fn new(
        state: &Arc<TaskSessionState>,
        entry: Arc<StatementEntry>,
        plan: Result<OutputPlan, TaskError>,
    ) -> Self {
        if let Ok(plan) = &plan {
            entry.set_columns(&plan.refs);
        }
        Self {
            state: state.clone(),
            plan,
            entry,
        }
    }

    /// Takes a scoped read's new counts while this statement produces pages.
    pub(crate) fn add_meter(&self, meter: &super::rows::StatementMeter) {
        self.state.add_meter(meter);
    }

    /// The statement failed with `error` (its read-log outcome).
    pub(crate) fn fail(&self, error: &CoreError) {
        self.entry.fail(error);
    }

    /// The statement failed with `sqlstate` (native errors).
    pub(crate) fn fail_with(&self, sqlstate: &str) {
        self.entry.fail_with(sqlstate);
    }

    /// The statement is rerun under a new tag (a cursor that buffers): this
    /// one writes no record.
    pub(crate) fn discard(&self) {
        self.entry.discard();
    }

    /// The denial the statement's column rules gave, as the error that
    /// refuses the statement before it runs (GD-0061 R3-308 follow-up).
    pub(crate) fn refuse_denied(&self) -> Result<(), CoreError> {
        match &self.plan {
            Ok(_) => Ok(()),
            Err(e) => Err(CoreError::Task(e.clone())),
        }
    }
}

/// A result produced by a core exit that has not been released.
///
/// It has no public field, no `Deref`, no `Clone`, no `Serialize` and a
/// `Debug` that never shows the value.
#[must_use = "an unreleased page must be released or dropped"]
pub struct Unreleased<T> {
    value: T,
    tag: Option<Arc<ReleaseTag>>,
}

impl<T> Unreleased<T> {
    pub(crate) fn new(value: T, tag: Option<Arc<ReleaseTag>>) -> Self {
        Self { value, tag }
    }

    /// The value, for a result produced outside a task session. A task
    /// session's result is refused.
    pub fn untasked(self) -> Result<T, CoreError> {
        match self.tag {
            None => Ok(self.value),
            Some(_) => Err(refusal()),
        }
    }

    /// Whether this result was produced in a task session.
    pub fn is_task(&self) -> bool {
        self.tag.is_some()
    }

    /// Crate-internal split, for exits that carry the tag into another
    /// container (a buffered cursor). Never exposed.
    pub(crate) fn into_parts(self) -> (T, Option<Arc<ReleaseTag>>) {
        (self.value, self.tag)
    }
}

impl<T> std::fmt::Debug for Unreleased<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Unreleased")
            .field("task", &self.is_task())
            .finish_non_exhaustive()
    }
}

/// A released result: the only form an encoder accepts.
#[derive(Debug, Clone, PartialEq)]
pub struct Released<T>(T);

impl<T> Released<T> {
    /// A result the server builds itself (SHOW, settings functions, empty
    /// command completions). It cannot wrap core rows, because `Unreleased`
    /// exposes none.
    pub fn synthetic(value: T) -> Self {
        Self(value)
    }
    pub fn get(&self) -> &T {
        &self.0
    }
    pub fn into_inner(self) -> T {
        self.0
    }
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Released<U> {
        Released(f(self.0))
    }
}

/// A result whose rows the release protocol masks and charges.
///
/// A row is visited as a value slice, so a scalar emitted by a native
/// transaction block is a one-value row.
pub trait Page {
    /// Every released row, in order.
    fn for_each_row(&self, f: &mut dyn FnMut(&[Value]));
    /// Every released row, in order, for masking in place.
    fn for_each_row_mut(&mut self, f: &mut dyn FnMut(&mut [Value]));
    /// The number of rows `for_each_row` visits.
    fn row_count(&self) -> usize {
        let mut n = 0;
        self.for_each_row(&mut |_| n += 1);
        n
    }
}

impl Page for Vec<SqlRow> {
    fn for_each_row(&self, f: &mut dyn FnMut(&[Value])) {
        self.iter().for_each(|row| f(&row.values));
    }
    fn for_each_row_mut(&mut self, f: &mut dyn FnMut(&mut [Value])) {
        self.iter_mut().for_each(|row| f(&mut row.values));
    }
}

impl Page for SqlResult {
    fn for_each_row(&self, f: &mut dyn FnMut(&[Value])) {
        self.rows.for_each_row(f);
    }
    fn for_each_row_mut(&mut self, f: &mut dyn FnMut(&mut [Value])) {
        self.rows.for_each_row_mut(f);
    }
}

impl Page for Outcome {
    fn for_each_row(&self, f: &mut dyn FnMut(&[Value])) {
        self.rows.for_each_row(f);
    }
    fn for_each_row_mut(&mut self, f: &mut dyn FnMut(&mut [Value])) {
        self.rows.for_each_row_mut(f);
    }
}

impl Page for TransactionOutcome {
    fn for_each_row(&self, f: &mut dyn FnMut(&[Value])) {
        for emitted in &self.emitted {
            match emitted {
                EmittedValue::Scalar(value) => f(std::slice::from_ref(value)),
                EmittedValue::Row { row, .. } => f(&row.values),
                EmittedValue::Rows { rows, .. } => rows.for_each_row(f),
            }
        }
    }
    fn for_each_row_mut(&mut self, f: &mut dyn FnMut(&mut [Value])) {
        for emitted in &mut self.emitted {
            match emitted {
                EmittedValue::Scalar(value) => f(std::slice::from_mut(value)),
                EmittedValue::Row { row, .. } => f(&mut row.values),
                EmittedValue::Rows { rows, .. } => rows.for_each_row_mut(f),
            }
        }
    }
}

/// The refusal of a plain exit in a task session.
pub(crate) fn refusal() -> CoreError {
    CoreError::Task(TaskError::NotAllowedInTaskSession(StatementClass::Other))
}

/// The refusal of a write outside the protected commit in a task session.
pub(crate) fn write_refusal(class: StatementClass) -> CoreError {
    CoreError::Task(TaskError::NotAllowedInTaskSession(class))
}

#[cfg(test)]
thread_local! {
    static TEST_TASK_WRITES: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Lib tests that observe the scope of a task write outside the protected
/// commit (R3-306) lift the write refusal on their own thread.
#[cfg(all(test, feature = "engine-rocksdb"))]
pub(crate) fn allow_task_writes_for_test(on: bool) {
    TEST_TASK_WRITES.set(on);
}

/// Whether a write outside the protected commit is refused in a task
/// session: always, except in a lib test that lifted it.
pub(crate) fn task_writes_refused() -> bool {
    #[cfg(test)]
    {
        !TEST_TASK_WRITES.get()
    }
    #[cfg(not(test))]
    {
        true
    }
}

type Probe = Box<dyn FnMut()>;

thread_local! {
    static PROBE: RefCell<Option<Probe>> = const { RefCell::new(None) };
}

/// Runs `f` with `probe` called after each durable debit of this thread's
/// releases commits and before the page is handed over. For tests that show
/// the debit precedes the hand-over.
#[doc(hidden)]
pub fn with_release_probe<R>(probe: impl FnMut() + 'static, f: impl FnOnce() -> R) -> R {
    let previous = PROBE.replace(Some(Box::new(probe)));
    let out = f();
    PROBE.replace(previous);
    out
}

fn run_probe() {
    // Taken out while it runs, so a probe that releases does not recurse.
    if let Some(mut probe) = PROBE.take() {
        probe();
        PROBE.with_borrow_mut(|slot| {
            if slot.is_none() {
                *slot = Some(probe);
            }
        });
    }
}

/// One debit of the task's disclosure head, as stored.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DebitView {
    pub read_seq: u64,
    pub page_no: u32,
    pub rows_read: u64,
    pub rows_returned: u64,
    pub bytes: u64,
    pub reserved_rows: u64,
    pub reserved_bytes: u64,
}

/// The committed disclosure records of one task: the head `20|task|01`, the
/// debits `2B` in key order and the number of read-log records `2A`.
#[doc(hidden)]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Disclosure {
    pub rows_read: u64,
    pub rows_returned: u64,
    pub bytes_returned: u64,
    pub next_read_seq: u64,
    pub debits: Vec<DebitView>,
    pub read_logs: u64,
}

impl crate::storage::Admin {
    /// The committed disclosure records of `task` (v2 only). For tests and
    /// the exposure report.
    #[doc(hidden)]
    pub fn task_disclosure(&self, task: TaskObjectId) -> Result<Disclosure, CoreError> {
        let db = self.sql_session().v2_db().ok_or(CoreError::Unsupported)?;
        let mut txn = db.begin(Isolation::ReadCommitted);
        let mut out = match txn.get(Keyspace::Kernel, &disclosure_head_key(task))? {
            Some(value) => {
                let head = DisclosureHead::decode(&value)?;
                Disclosure {
                    rows_read: head.rows_read,
                    rows_returned: head.rows_returned,
                    bytes_returned: head.bytes_returned,
                    next_read_seq: head.next_read_seq,
                    ..Disclosure::default()
                }
            }
            None => Disclosure::default(),
        };
        for (key, value) in scan(&mut txn, NS_DEBIT, task)? {
            let record = DebitRecord::decode(&value)?;
            let read_seq = u64::from_be_bytes(key[9..17].try_into().expect("debit key"));
            let page_no = u32::from_be_bytes(key[17..21].try_into().expect("debit key"));
            out.debits.push(DebitView {
                read_seq,
                page_no,
                rows_read: record.rows_read,
                rows_returned: record.rows_returned,
                bytes: record.bytes,
                reserved_rows: record.reserved_rows,
                reserved_bytes: record.reserved_bytes,
            });
        }
        out.read_logs = scan(&mut txn, NS_READ_LOG, task)?.len() as u64;
        Ok(out)
    }
}

/// A Kernel key and its value.
type Entry = (Vec<u8>, Vec<u8>);

fn scan(txn: &mut Transaction, ns: u8, task: TaskObjectId) -> Result<Vec<Entry>, CoreError> {
    let (start, end) = task_prefix(ns, task);
    let rows = txn.scan(
        Keyspace::Kernel,
        Bound::Included(&start),
        Bound::Excluded(&end),
        Direction::Forward,
        None,
    )?;
    let mut out = Vec::new();
    for entry in rows {
        out.push(entry?);
    }
    Ok(out)
}

/// Validity of the session's binding: revoked (missing, revoked, another
/// task or executor), then expired (`expires_at <= now`), then a changed
/// definition.
fn valid(def: Option<&TaskDef>, ctx: &TaskContext, now_ms: u64) -> Result<(), TaskError> {
    let def = def
        .filter(|def| !def.revoked && def.id == ctx.task && def.executor == ctx.executor)
        .ok_or(TaskError::Revoked)?;
    if i64::try_from(now_ms).unwrap_or(i64::MAX) >= def.expires_at.0 {
        return Err(TaskError::Expired);
    }
    if def.def_version != ctx.bound_def_version {
        return Err(TaskError::Denied(DenyRule::DefinitionChanged));
    }
    Ok(())
}

/// What one release (or a statement's end) asks to debit.
#[derive(Clone, Copy, Debug, Default)]
struct Need {
    rows_read: u64,
    rows: u64,
    bytes: u64,
}

/// The session-side inputs of one debit.
#[derive(Clone, Copy)]
struct Budgeting {
    reservation: Reservation,
    chunk: Reservation,
    need: Need,
}

/// What a staged debit charges, applied to the session after the commit.
struct Staged {
    read_seq: u64,
    page_no: u32,
    reserved: Reservation,
    /// The part of the session's reservation this page used.
    used: Reservation,
    /// What the debit charged: rows read, returned rows and bytes (with
    /// the reserved amounts).
    charged: Need,
}

/// Reservation arithmetic: with `need > 0`, `max(need, min(chunk, remaining
/// / 8))`, never more than `remaining`; `need` itself when chunking is off.
/// The caller has checked `need <= remaining`.
pub(crate) fn grant(need: u64, chunk: u64, remaining: u64) -> u64 {
    if need == 0 || chunk == 0 {
        return need;
    }
    need.max(chunk.min(remaining / 8)).min(remaining)
}

pub(crate) fn lock_error(error: TxnError) -> CoreError {
    match error {
        TxnError::LockTimeout => CoreError::LockNotAvailable,
        other => other.into(),
    }
}

/// Steps 3 and 4 of the protocol for one page. Pending `rows_read` that no
/// commit charged is put back.
fn debit(
    db: &V2Db,
    state: &TaskSessionState,
    log: &Mutex<StatementLog>,
    need: Need,
    crash: CrashPoint,
) -> Result<(), CoreError> {
    let result = debit_inner(db, state, log, need, crash);
    if let Err((_, false)) = &result {
        state.add_rows_read(need.rows_read);
    }
    result.map_err(|(error, _)| error)
}

/// The error, and whether the debit had committed.
type DebitResult = Result<(), (CoreError, bool)>;

fn debit_inner(
    db: &V2Db,
    state: &TaskSessionState,
    log: &Mutex<StatementLog>,
    need: Need,
    crash: CrashPoint,
) -> DebitResult {
    let ctx = &state.ctx;
    let before = |error: CoreError| (error, false);
    if db.task_kernel_unsupported() {
        return Err(before(CoreError::Unsupported));
    }
    let now = db.wall_now_ms();
    // 3. Validity on every page, from the cached view and the clock.
    let latest = catalog_def(db, ctx.task, DefAt::Latest).map_err(before)?;
    valid(latest.as_deref(), ctx, now).map_err(|e| before(e.into()))?;
    let mut reservation = state.reservation();
    if need.rows_read == 0 && reservation.rows >= need.rows && reservation.bytes >= need.bytes {
        reservation.rows -= need.rows;
        reservation.bytes -= need.bytes;
        return Ok(());
    }
    // 4. A transaction of its own; the head lock serializes this task's sessions.
    let mut txn = db.begin(Isolation::ReadCommitted);
    let wait = latest.as_ref().map_or(0, |def| def.limits.lock_wait_ms);
    let deadline = Instant::now().checked_add(Duration::from_millis(wait));
    let (pages, read_seq, template) = {
        let log = lock(log);
        (log.debit_pages, log.read_seq, log.read_seq.is_none().then(|| log.record()))
    };
    let chunk = Reservation {
        rows: state.chunk_rows.load(Ordering::SeqCst),
        bytes: state.chunk_bytes.load(Ordering::SeqCst),
    };
    let staged = stage(
        &mut txn,
        db,
        ctx,
        Budgeting { reservation: *reservation, chunk, need },
        now,
        deadline,
        (pages, read_seq, template),
    );
    let staged = match staged {
        Ok(Some(staged)) => staged,
        Ok(None) => return Ok(()),
        Err(error) => {
            *reservation = Reservation::default();
            return Err(before(error));
        }
    };
    match crash {
        CrashPoint::BeforeCommit => crate::storage::crash_now(),
        CrashPoint::ErrorBeforeCommit => {
            *reservation = Reservation::default();
            // An injected crash: the statement's end is never logged.
            lock(log).done = true;
            return Err(before(CoreError::Storage("injected before commit".into())));
        }
        _ => {}
    }
    if let Err(error) = db.catalog().commit_ddl(txn) {
        *reservation = Reservation::default();
        return Err(before(error.into()));
    }
    // Committed: the page is charged whatever happens next.
    {
        let mut log = lock(log);
        log.read_seq = Some(staged.read_seq);
        log.debit_pages = staged.page_no.saturating_add(1);
        let totals = &mut log.totals;
        totals.charged_rows_read = totals.charged_rows_read.saturating_add(staged.charged.rows_read);
        totals.charged_rows_returned =
            totals.charged_rows_returned.saturating_add(staged.charged.rows);
        totals.charged_bytes = totals.charged_bytes.saturating_add(staged.charged.bytes);
    }
    reservation.rows = reservation.rows - staged.used.rows + staged.reserved.rows;
    reservation.bytes = reservation.bytes - staged.used.bytes + staged.reserved.bytes;
    match crash {
        CrashPoint::AfterCommit | CrashPoint::BeforeAck => crate::storage::crash_now(),
        CrashPoint::ErrorAfterCommit => {
            *reservation = Reservation::default();
            // An injected crash: the record stays open.
            lock(log).done = true;
            return Err((CoreError::Storage("injected lost commit response".into()), true));
        }
        _ => {}
    }
    drop(reservation);
    run_probe();
    Ok(())
}

/// Locks the head, re-checks validity at the snapshot, checks the caps and
/// stages the head, the debit and (first debit of a statement, from
/// `template`) the open read-log record. `None`: nothing to write.
fn stage(
    txn: &mut Transaction,
    db: &V2Db,
    ctx: &TaskContext,
    budgeting: Budgeting,
    now: u64,
    deadline: Option<Instant>,
    (pages, read_seq, template): (u32, Option<u64>, Option<ReadLogRecord>),
) -> Result<Option<Staged>, CoreError> {
    let Budgeting { reservation, chunk, need } = budgeting;
    let task = ctx.task;
    let head_key = disclosure_head_key(task);
    txn.lock_kernel_key(&head_key, deadline, &|| crate::cancellation::check().is_err())
        .map_err(lock_error)?;
    // A snapshot taken after the lock: the head read below is exact, since
    // only holders of this lock write it.
    txn.begin_statement();
    let def = catalog_def(db, task, DefAt::Snapshot(txn))?;
    valid(def.as_deref(), ctx, now)?;
    let caps = def.as_ref().map_or(
        DisclosureCaps {
            rows_read: u64::MAX,
            rows_returned: u64::MAX,
            bytes_returned: u64::MAX,
        },
        |def| def.disclosure_caps,
    );
    let mut head = match txn.get(Keyspace::Kernel, &head_key)? {
        Some(value) => DisclosureHead::decode(&value)?,
        None => DisclosureHead {
            next_read_seq: 1,
            ..DisclosureHead::default()
        },
    };
    let left_read = caps.rows_read.saturating_sub(head.rows_read);
    let left_rows = caps.rows_returned.saturating_sub(head.rows_returned);
    let left_bytes = caps.bytes_returned.saturating_sub(head.bytes_returned);
    let used = Reservation {
        rows: need.rows.min(reservation.rows),
        bytes: need.bytes.min(reservation.bytes),
    };
    let rows = need.rows - used.rows;
    let bytes = need.bytes - used.bytes;
    let rows_read = need.rows_read;
    let exceeded = |budget| Err(TaskError::BudgetExceeded(budget).into());
    if rows_read > left_read {
        return exceeded(Budget::RowsRead);
    }
    if rows > left_rows {
        return exceeded(Budget::RowsReturned);
    }
    if bytes > left_bytes {
        return exceeded(Budget::BytesReturned);
    }
    if rows_read == 0 && rows == 0 && bytes == 0 {
        return Ok(None);
    }
    let granted_rows = grant(rows, chunk.rows, left_rows);
    let granted_bytes = grant(bytes, chunk.bytes, left_bytes);
    let reserved = Reservation {
        rows: granted_rows - rows,
        bytes: granted_bytes - bytes,
    };
    let overflow = || CoreError::Corrupt("task disclosure head overflow".into());
    let read_seq = match read_seq {
        Some(read_seq) => read_seq,
        None => {
            let read_seq = head.next_read_seq;
            head.next_read_seq = read_seq.checked_add(1).ok_or_else(overflow)?;
            read_seq
        }
    };
    head.rows_read = head.rows_read.checked_add(rows_read).ok_or_else(overflow)?;
    head.rows_returned = head.rows_returned.checked_add(granted_rows).ok_or_else(overflow)?;
    head.bytes_returned = head.bytes_returned.checked_add(granted_bytes).ok_or_else(overflow)?;
    if let Some(mut record) = template {
        // Created open by the statement's first debit, with what the
        // statement knows so far; its end finalizes it (`readlog.rs`).
        record.wall_ms = now;
        record.snapshot_ts = txn.current_snapshot_ts().map_or(0, |ts| ts.0);
        record.credential_fingerprint = ctx.credential_fingerprint;
        record.def_version = ctx.bound_def_version;
        record.catalog_version = db.catalog().snapshot_version(txn).map_or(0, |ts| ts.0);
        record.table_id = def.as_ref().map_or(0, |def| def.table);
        record.charged_rows_read = record.charged_rows_read.saturating_add(rows_read);
        record.charged_rows_returned = record.charged_rows_returned.saturating_add(granted_rows);
        record.charged_bytes = record.charged_bytes.saturating_add(granted_bytes);
        fill_left(&mut record, &caps, &head);
        txn.put(Keyspace::Kernel, read_log_key(task, read_seq), record.encode())?;
    }
    let debit = DebitRecord {
        wall_ms: now,
        rows_read,
        rows_returned: rows,
        bytes,
        reserved_rows: reserved.rows,
        reserved_bytes: reserved.bytes,
    };
    txn.put(Keyspace::Kernel, debit_key(task, read_seq, pages), debit.encode())?;
    txn.put(Keyspace::Kernel, head_key, head.encode())?;
    txn.require_features(PROTECTED_RECORDS);
    Ok(Some(Staged {
        read_seq,
        page_no: pages,
        reserved,
        used,
        charged: Need {
            rows_read,
            rows: granted_rows,
            bytes: granted_bytes,
        },
    }))
}

impl SqlSession {
    /// The tag for a statement starting now, with an empty output plan (a
    /// statement that returns no rows); `None` outside a task session (no
    /// allocation). For tests.
    #[cfg(test)]
    pub(crate) fn release_tag(&self) -> Option<Arc<ReleaseTag>> {
        self.release_tag_with(StatementClass::Other, 0, || Ok(OutputPlan::default()))
    }

    /// The tag for a statement of `class` with `param_count` parameters
    /// starting now, with the plan its column rules give; `plan` runs only
    /// in a task session. The statement's read-log entry starts here.
    pub(crate) fn release_tag_with(
        &self,
        class: StatementClass,
        param_count: usize,
        plan: impl FnOnce() -> Result<OutputPlan, TaskError>,
    ) -> Option<Arc<ReleaseTag>> {
        let entry = self.start_task_statement(class, param_count)?;
        self.release_tag_for(entry, plan())
    }

    /// The tag of a statement whose read-log entry already started.
    pub(crate) fn release_tag_for(
        &self,
        entry: Arc<StatementEntry>,
        plan: Result<OutputPlan, TaskError>,
    ) -> Option<Arc<ReleaseTag>> {
        self.task_state
            .as_ref()
            .map(|state| Arc::new(ReleaseTag::new(state, entry, plan)))
    }

    /// Guard for the plain exits: a task session must use the twins.
    pub(crate) fn refuse_task_session(&self) -> Result<(), CoreError> {
        match self.task {
            None => Ok(()),
            Some(_) => Err(refusal()),
        }
    }

    /// Releases one page (named `release_page` because `release` is the
    /// savepoint RELEASE). Without a task this is a move: no lock, catalog
    /// read, transaction or allocation. A task page goes through the release
    /// protocol (module docs); on any error nothing is handed over.
    pub fn release_page<T: Page>(&self, page: Unreleased<T>) -> Result<Released<T>, CoreError> {
        let Unreleased { mut value, tag } = page;
        let Some(tag) = tag else {
            return Ok(Released(value));
        };
        let counts = self
            .release_steps(&tag, &mut value)
            .inspect_err(|e| tag.fail(e))?;
        let mut log = lock(&tag.entry.log);
        let totals = &mut log.totals;
        totals.rows_scanned = totals.rows_scanned.saturating_add(tag.state.take_rows_scanned());
        totals.released_rows = totals.released_rows.saturating_add(counts.rows);
        totals.released_bytes = totals.released_bytes.saturating_add(counts.bytes);
        totals.masked = totals.masked.saturating_add(counts.masked);
        totals.withheld = totals.withheld.saturating_add(counts.withheld);
        totals.untrusted_released =
            totals.untrusted_released.saturating_add(counts.untrusted_released);
        totals.pages = totals.pages.saturating_add(1);
        drop(log);
        // 5. Hand over.
        Ok(Released(value))
    }

    /// Steps 1 to 4 of the protocol for one task page.
    fn release_steps<T: Page>(&self, tag: &ReleaseTag, value: &mut T) -> Result<Counts, CoreError> {
        // 1. The statement deadline.
        crate::cancellation::check()?;
        // 2. Mask in memory; the cost is taken after masking.
        let plan = tag.plan.as_ref().map_err(|e| CoreError::Task(e.clone()))?;
        let counts = mask_page(value, plan)?;
        // 3 and 4. Validity, then the durable debit (or the reservation).
        let db = self.v2_db().ok_or(CoreError::Unsupported)?;
        let crash = self.fault_points()?.0;
        let need = Need {
            rows_read: tag.state.take_rows_read(),
            rows: counts.rows,
            bytes: counts.bytes,
        };
        debit(&db, &tag.state, &tag.entry.log, need, crash)?;
        Ok(counts)
    }

    /// Validity only, for re-serving an already charged page.
    pub fn still_valid(&self) -> Result<(), CoreError> {
        let Some(state) = &self.task_state else {
            return Ok(());
        };
        let db = self.v2_db().ok_or(CoreError::Unsupported)?;
        let def = catalog_def(&db, state.ctx.task, DefAt::Latest)?;
        valid(def.as_deref(), &state.ctx, db.wall_now_ms())?;
        Ok(())
    }

    /// Ends the session's current task statement with `outcome`: a
    /// `rows_read` delta no release charged is debited, saturating at the
    /// cap, and the statement's read-log record is finalized (`readlog.rs`).
    /// A statement whose last handle already went was finalized then. No-op
    /// without a task; a failure leaves the delta pending and the record as
    /// it was.
    pub fn finish_task_statement(&self, outcome: Result<(), &CoreError>) {
        if let Some(entry) = self.current_task_statement() {
            if let Err(error) = outcome {
                entry.fail(error);
            }
            entry.finish();
        }
    }

    /// Sets the session's reservation chunk (`agent.disclosure_chunk_rows`
    /// and `agent.disclosure_chunk_bytes`); zero means exact debits. No-op
    /// without a task.
    pub fn set_disclosure_chunk(&self, rows: u64, bytes: u64) {
        if let Some(state) = &self.task_state {
            state.chunk_rows.store(rows, Ordering::SeqCst);
            state.chunk_bytes.store(bytes, Ordering::SeqCst);
        }
    }

    /// The session's unused reservation; `None` without a task.
    #[doc(hidden)]
    pub fn task_reservation(&self) -> Option<Reservation> {
        self.task_state.as_ref().map(|state| *state.reservation())
    }
}

#[cfg(test)]
#[path = "release_tests.rs"]
mod tests;
