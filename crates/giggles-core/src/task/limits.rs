//! Load limits of task statements (GD-0061 R3-401, R3-402, design-3 section
//! 7): statement time, concurrent statements per task and the all-agent
//! ceiling (R3-401); rows scanned, the statement's rows-read allowance, lock
//! wait and pending writes (R3-402).
//!
//! A task statement enters a `StatementGuard` at `execute_bound_clocked`,
//! `open_cursor_inner` and `native_scope`. The guard holds one per-task slot
//! and one all-agent slot and releases both on drop, so an error, a panic or
//! a dropped cursor gives them back. A session without a task takes one
//! branch and no lock. A cursor owns its guard from open until it is
//! exhausted, closed, expired or dropped.
//!
//! The guard also carries the statement's [`Meter`]: `activate` installs it
//! as this thread's meter, so every scoped read of the statement (the
//! `Scoped` source, the `SqlRows` sink scope and the native filter) counts
//! into it and is stopped per row at `rows_scanned` or past the rows-read
//! allowance, and every lock wait of the statement gets `lock_deadline()`.
//! Outside a task statement no meter is installed: `lock_deadline()` is
//! `None` and nothing is counted or checked.
use super::definition::{LoadLimits, TaskObjectId};
use super::error::{Budget, TaskError};
use crate::cancellation::{self, Cancellation};
use crate::storage::SqlSession;
use crate::CoreError;
use crate::storage::engine::txn::{Transaction, TxnError};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

/// The default all-agent ceiling (`agent.max_concurrent_statements`).
pub const DEFAULT_AGENT_STATEMENT_CEILING: usize = 8;

/// Statement slots held by task sessions on one engine handle.
#[derive(Debug)]
pub(crate) struct TaskLoad {
    per_task: Mutex<HashMap<TaskObjectId, u32>>,
    all_agents: AtomicUsize,
    ceiling: AtomicUsize,
    admitted: usize,
}

impl TaskLoad {
    /// The default ceiling, kept within `admitted / 2` (at least 1).
    pub(crate) fn new(admitted: usize) -> Self {
        Self {
            per_task: Mutex::new(HashMap::new()),
            all_agents: AtomicUsize::new(0),
            ceiling: AtomicUsize::new(DEFAULT_AGENT_STATEMENT_CEILING.min(admitted / 2).max(1)),
            admitted,
        }
    }

    /// Sets the all-agent ceiling: `1 <= n <= admitted / 2`, so applications
    /// always keep at least half of the engine's admitted operations.
    pub(crate) fn set_ceiling(&self, n: usize) -> Result<(), CoreError> {
        if n == 0 || n > self.admitted / 2 {
            return Err(CoreError::Limit);
        }
        self.ceiling.store(n, Ordering::SeqCst);
        Ok(())
    }

    /// All-agent slots held, and the per-task slots held summed over tasks.
    pub(crate) fn counts(&self) -> (usize, u64) {
        let per_task = self.per_task().values().map(|&n| u64::from(n)).sum();
        (self.all_agents.load(Ordering::SeqCst), per_task)
    }

    fn per_task(&self) -> MutexGuard<'_, HashMap<TaskObjectId, u32>> {
        self.per_task.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Takes one per-task and one all-agent slot, or neither. Refuses at
    /// once; nothing queues.
    fn acquire(&self, task: TaskObjectId, limit: u32) -> Result<(), TaskError> {
        let mut per_task = self.per_task();
        let held = per_task.get(&task).copied().unwrap_or(0);
        if held >= limit {
            return Err(TaskError::BudgetExceeded(Budget::ConcurrentStatements));
        }
        let ceiling = self.ceiling.load(Ordering::SeqCst);
        self.all_agents
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| (n < ceiling).then_some(n + 1))
            .map_err(|_| TaskError::BudgetExceeded(Budget::AllAgents))?;
        per_task.insert(task, held + 1);
        Ok(())
    }

    fn release(&self, task: TaskObjectId) {
        let mut per_task = self.per_task();
        if let Some(held) = per_task.get_mut(&task) {
            *held -= 1;
            if *held == 0 {
                per_task.remove(&task);
            }
        }
        self.all_agents.fetch_sub(1, Ordering::SeqCst);
    }
}

thread_local! {
    /// Set while a guarded statement runs on this thread: a statement it runs
    /// internally (a buffered cursor's execution, a native step) is the same
    /// statement and takes no second slot.
    static ACTIVE: Cell<bool> = const { Cell::new(false) };
}

/// One task statement's slots and time limit (RAII).
#[derive(Debug)]
pub(crate) struct StatementGuard {
    load: Arc<TaskLoad>,
    task: TaskObjectId,
    opened_at: Instant,
    statement: Duration,
    meter: Arc<Meter>,
}

impl StatementGuard {
    /// Enters a statement of `session`. `None` without a task (one branch,
    /// no lock), inside the protected commit, and for a statement nested in
    /// a guarded one on this thread.
    pub(crate) fn enter(session: &SqlSession) -> Result<Option<Self>, TaskError> {
        let Some(state) = session.task_state.as_ref() else {
            return Ok(None);
        };
        if session.in_protected_run() || ACTIVE.with(Cell::get) {
            return Ok(None);
        }
        let limits = state.load_limits(|| task_limits(session))?;
        let task = state.ctx().task;
        let load = session.task_load().clone();
        load.acquire(task, limits.concurrent_statements)?;
        Ok(Some(Self {
            load,
            task,
            opened_at: Instant::now(),
            statement: Duration::from_millis(limits.statement_ms),
            meter: Arc::new(Meter::new(&limits, true)),
        }))
    }

    /// When this statement's time is up.
    pub(crate) fn deadline(&self) -> Instant {
        self.opened_at
            .checked_add(self.statement)
            .unwrap_or_else(|| self.opened_at + Duration::from_secs(86_400 * 365))
    }

    /// Whether the statement's time is up (a suspended cursor's fetch).
    pub(crate) fn expired(&self) -> bool {
        Instant::now() >= self.deadline()
    }

    /// Runs the statement's work from here until the returned value drops:
    /// the current invocation gets this statement's deadline (an earlier
    /// deadline it already has still wins), or, with no invocation, one is
    /// installed for the statement; nested statements take no slot. The
    /// statement's meter is this thread's meter until then.
    pub(crate) fn activate(&self) -> Active {
        let deadline = self.deadline();
        let installed = match Cancellation::current() {
            Some(current) => {
                current.deadline(deadline);
                None
            }
            None => Some(cancellation::install(Cancellation::new(Some(deadline)))),
        };
        Active {
            nested: ACTIVE.with(|a| a.replace(true)),
            _metered: Metered::install(self.meter.clone()),
            _installed: installed,
        }
    }
}

impl Drop for StatementGuard {
    fn drop(&mut self) {
        self.load.release(self.task);
    }
}

/// The running part of a guarded statement; see `StatementGuard::activate`.
pub(crate) struct Active {
    nested: bool,
    _metered: Metered,
    _installed: Option<cancellation::Installed>,
}

impl Drop for Active {
    fn drop(&mut self) {
        ACTIVE.with(|a| a.set(self.nested));
    }
}

/// One task statement's row and lock limits and its running counts, shared
/// by every scoped read of the statement (a cursor's fetches included).
#[derive(Debug)]
pub(crate) struct Meter {
    scan_limit: u64,
    lock_wait: Duration,
    /// Whether the statement is held to a rows-read allowance (not the
    /// protected commit's statement, which releases nothing).
    reads: bool,
    /// Rows the task may still read, set when the statement's first scope
    /// is taken; `u64::MAX` until then.
    read_allowance: AtomicU64,
    scanned: AtomicU64,
    read: AtomicU64,
}

impl Meter {
    fn new(limits: &LoadLimits, reads: bool) -> Self {
        Self {
            scan_limit: limits.rows_scanned,
            lock_wait: Duration::from_millis(limits.lock_wait_ms),
            reads,
            read_allowance: AtomicU64::new(u64::MAX),
            scanned: AtomicU64::new(0),
            read: AtomicU64::new(0),
        }
    }
}

thread_local! {
    /// The meter of the task statement running on this thread.
    static METER: RefCell<Option<Arc<Meter>>> = const { RefCell::new(None) };
}

fn with_meter<T>(f: impl FnOnce(&Meter) -> T) -> Option<T> {
    METER.with(|m| m.borrow().as_deref().map(f))
}

/// This thread's meter from here until the value drops (RAII; the meter it
/// replaced is restored).
pub(crate) struct Metered {
    previous: Option<Arc<Meter>>,
}

impl Metered {
    fn install(meter: Arc<Meter>) -> Self {
        Self {
            previous: METER.with(|m| m.replace(Some(meter))),
        }
    }
}

impl Drop for Metered {
    fn drop(&mut self) {
        METER.with(|m| *m.borrow_mut() = self.previous.take());
    }
}

/// Meters the protected commit's statement with the task's `limits`: rows
/// scanned and lock wait. It releases nothing, so no rows-read allowance
/// applies.
pub(crate) fn metered(limits: &LoadLimits) -> Metered {
    Metered::install(Arc::new(Meter::new(limits, false)))
}

/// Counts one row a scoped read pulled from the task table; past
/// `rows_scanned` the statement stops. The engine reads a page of up to 200
/// rows at a time, so the rows pulled past the limit are at most one page.
pub(crate) fn count_scanned() -> Result<(), TaskError> {
    with_meter(|m| {
        let scanned = m.scanned.fetch_add(1, Ordering::SeqCst).saturating_add(1);
        if scanned > m.scan_limit {
            return Err(TaskError::BudgetExceeded(Budget::RowsScanned));
        }
        Ok(())
    })
    .unwrap_or(Ok(()))
}

/// Counts one row the visible-rows predicate accepted; past the allowance
/// read at statement start the statement stops (design-3 section 4).
pub(crate) fn count_read() -> Result<(), TaskError> {
    with_meter(|m| {
        let read = m.read.fetch_add(1, Ordering::SeqCst).saturating_add(1);
        if read > m.read_allowance.load(Ordering::SeqCst) {
            return Err(TaskError::BudgetExceeded(Budget::RowsRead));
        }
        Ok(())
    })
    .unwrap_or(Ok(()))
}

/// Whether the running task statement has no rows-read allowance yet.
pub(crate) fn wants_read_allowance() -> bool {
    with_meter(|m| m.reads && m.read_allowance.load(Ordering::SeqCst) == u64::MAX).unwrap_or(false)
}

/// Sets the running statement's rows-read allowance once, at its first
/// scope; later calls keep the first value.
pub(crate) fn set_read_allowance(left: u64) {
    with_meter(|m| {
        if m.reads {
            let _ = m
                .read_allowance
                .compare_exchange(u64::MAX, left, Ordering::SeqCst, Ordering::SeqCst);
        }
    });
}

/// The deadline of a lock wait starting now: `now + lock_wait_ms` inside a
/// task statement, `None` (wait as before) everywhere else.
pub(crate) fn lock_deadline() -> Option<Instant> {
    with_meter(|m| {
        let now = Instant::now();
        now.checked_add(m.lock_wait).unwrap_or(now)
    })
}

/// A lock wait's engine error. Inside a task statement a wait that ran past
/// `lock_deadline()` is 55P03 lock_not_available, as the protected commit's
/// hold reports it; everywhere else the conversion is unchanged.
pub(crate) fn lock_error(error: TxnError) -> CoreError {
    match error {
        TxnError::LockTimeout if with_meter(|_| ()).is_some() => CoreError::LockNotAvailable,
        other => other.into(),
    }
}

/// Holds `txn` to the task's `pending_write_bytes` (Phase 6 calls it for the
/// protected commit's statement): a write that would buffer more is refused
/// before it is staged, with `BudgetExceeded(PendingWrite)`.
pub(crate) fn apply_to_txn(txn: &mut Transaction, limits: &LoadLimits) {
    txn.set_write_limit(Some(usize::try_from(limits.pending_write_bytes).unwrap_or(usize::MAX)));
}

/// The bound task's load limits. A definition that is gone or replaced gets
/// the defaults here; the statement's own checks refuse it.
fn task_limits(session: &SqlSession) -> Result<Option<LoadLimits>, TaskError> {
    let Some(ctx) = session.task.as_ref() else {
        return Ok(None);
    };
    let def = session.tasks().lookup(ctx.name.as_str())?;
    Ok(def.filter(|def| def.id == ctx.task).map(|def| def.limits))
}

impl crate::storage::Admin {
    /// Sets the all-agent statement ceiling (`agent.max_concurrent_statements`,
    /// GD-0061 R3-401). `n` must be at least 1 and at most half the engine's
    /// admitted operations (`Limits::admitted`); otherwise `CoreError::Limit`.
    pub fn set_agent_statement_ceiling(&self, n: usize) -> Result<(), CoreError> {
        self.task_load().set_ceiling(n)
    }

    /// For tests: the statement slots task sessions hold now, as
    /// (all-agent slots, per-task slots summed over tasks).
    #[doc(hidden)]
    pub fn task_statement_slots(&self) -> (usize, u64) {
        self.task_load().counts()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ceiling_bounds_and_slots_balance() {
        let load = TaskLoad::new(32);
        assert_eq!(load.ceiling.load(Ordering::SeqCst), 8);
        assert_eq!(load.set_ceiling(0), Err(CoreError::Limit));
        assert_eq!(load.set_ceiling(17), Err(CoreError::Limit));
        load.set_ceiling(2).unwrap();
        let (a, b) = (TaskObjectId(1), TaskObjectId(2));
        load.acquire(a, 1).unwrap();
        assert_eq!(
            load.acquire(a, 1),
            Err(TaskError::BudgetExceeded(Budget::ConcurrentStatements))
        );
        load.acquire(b, 5).unwrap();
        assert_eq!(load.acquire(b, 5), Err(TaskError::BudgetExceeded(Budget::AllAgents)));
        assert_eq!(load.counts(), (2, 2));
        load.release(a);
        load.release(b);
        assert_eq!(load.counts(), (0, 0));
        assert_eq!(TaskLoad::new(1).ceiling.load(Ordering::SeqCst), 1);
    }
}
