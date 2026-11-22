//! MVCC GC wiring for `V2Db` (GD-0061 D3, design D-I).
//!
//! One background thread per non-replicated database. Every `TICK` it runs a
//! Kernel step, then one of Catalog, Rows and Index in rotation. Authority
//! heads live in Kernel and are rewritten on every commit, so Kernel is
//! collected on every tick. The thread waits on a condvar so dropping the
//! database stops it promptly, and the drop joins it before the KV closes.
//!
//! The same thread runs the task retention sweeper (GD-0061 D5, R3-622) every
//! `RetentionConfig::interval_ms` once the owning `Arc<V2Db>` is attached.
#![cfg_attr(not(feature = "engine-rocksdb"), allow(dead_code))]
use super::{CoreError, V2Db, engine};
use crate::task::retention::{self, RetentionConfig, RetentionStats};
use engine::{
    gc::{Gc, StepStats},
    kv::Keyspace,
};
use std::{
    sync::{
        Arc, Condvar, Mutex, Weak,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

/// Default `GcConfig.retention`: protects readers using an unpinned timestamp.
pub(crate) const RETENTION: Duration = Duration::from_secs(60);
pub(crate) const TICK: Duration = Duration::from_secs(5);
pub(crate) const BUDGET: usize = 20_000;
const ROTATION: [Keyspace; 3] = [Keyspace::Catalog, Keyspace::Rows, Keyspace::Index];

/// Totals of one maintenance round.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceStats {
    /// Raw versions examined across the keyspaces stepped.
    pub examined: usize,
    /// Raw versions deleted across the keyspaces stepped.
    pub deleted: usize,
    /// The GC watermark after the round (an HLC timestamp).
    pub watermark: u64,
    /// Failed background ticks since the database opened.
    pub background_errors: u64,
}

impl MaintenanceStats {
    fn add(&mut self, step: StepStats) {
        self.examined += step.examined;
        self.deleted += step.deleted;
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum WorkerState {
    Paused,
    Running,
    Stopped,
}

#[cfg(all(test, feature = "engine-rocksdb"))]
type TickHook = Box<dyn FnOnce() + Send>;

struct Control {
    state: Mutex<WorkerState>,
    wake: Condvar,
    #[cfg(all(test, feature = "engine-rocksdb"))]
    before_tick: Mutex<Option<TickHook>>,
}

/// The retention sweeper's database handle and settings. The handle is weak
/// so the thread never keeps the database open.
#[derive(Default)]
struct Retention {
    db: Mutex<Option<Weak<V2Db>>>,
    config: Mutex<RetentionConfig>,
}

impl Retention {
    fn config(&self) -> RetentionConfig {
        *self.config.lock().unwrap_or_else(|p| p.into_inner())
    }
}

pub(crate) struct Maintenance {
    control: Arc<Control>,
    thread: Mutex<Option<JoinHandle<()>>>,
    replicated: bool,
    budget: Arc<AtomicUsize>,
    errors: Arc<AtomicU64>,
    retention: Arc<Retention>,
}

impl Maintenance {
    pub(crate) fn new(replicated: bool) -> Self {
        Self {
            control: Arc::new(Control {
                state: Mutex::new(WorkerState::Paused),
                wake: Condvar::new(),
                #[cfg(all(test, feature = "engine-rocksdb"))]
                before_tick: Mutex::new(None),
            }),
            thread: Mutex::new(None),
            replicated,
            budget: Arc::new(AtomicUsize::new(BUDGET)),
            errors: Arc::new(AtomicU64::new(0)),
            retention: Arc::new(Retention::default()),
        }
    }

    /// Reserve the worker after the inline startup work passes its health check.
    /// A recovery candidate parks it until the slot publishes the database.
    pub(crate) fn start(&mut self, gc: &Arc<Gc>, paused: bool) -> Result<(), CoreError> {
        if !self.replicated {
            let (gc, control, budget, errors, retention) = (
                gc.clone(),
                self.control.clone(),
                self.budget.clone(),
                self.errors.clone(),
                self.retention.clone(),
            );
            *self.thread.get_mut().unwrap() = Some(
                std::thread::Builder::new()
                    .name("giggles-v2-gc".into())
                    .spawn(move || run(&gc, &control, &budget, &errors, &retention))
                    .map_err(|error| CoreError::Storage(error.to_string()))?,
            );
            if !paused {
                self.resume();
            }
        }
        Ok(())
    }

    pub(crate) fn resume(&self) {
        let mut state = self.control.state.lock().unwrap_or_else(|p| p.into_inner());
        if *state == WorkerState::Paused {
            *state = WorkerState::Running;
            self.control.wake.notify_all();
        }
    }

    #[cfg(test)]
    pub(crate) fn running(&self) -> bool {
        *self.control.state.lock().unwrap_or_else(|p| p.into_inner()) == WorkerState::Running
    }

    #[cfg(test)]
    pub(crate) fn set_budget(&self, budget: usize) {
        self.budget.store(budget, Ordering::Relaxed);
    }

    pub(crate) fn shutdown(&self) {
        *self.control.state.lock().unwrap_or_else(|p| p.into_inner()) = WorkerState::Stopped;
        self.control.wake.notify_all();
        let mut thread = self.thread.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(thread) = thread.take() {
            // The retention sweep upgrades its weak handle; if it held the
            // last reference, the database drops on this very thread, which
            // then exits on the stop flag without joining itself.
            if thread.thread().id() != std::thread::current().id() {
                // A panicking GC thread has nothing left to clean up.
                let _ = thread.join();
            }
        }
    }
}

impl Drop for Maintenance {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn run(gc: &Gc, control: &Control, budget: &AtomicUsize, errors: &AtomicU64, retention: &Retention) {
    let state = control.state.lock().unwrap_or_else(|p| p.into_inner());
    let mut state = control
        .wake
        .wait_while(state, |state| *state == WorkerState::Paused)
        .unwrap_or_else(|p| p.into_inner());
    let mut rotation = 0;
    let mut last_sweep = Instant::now();
    loop {
        let (next, _) = control
            .wake
            .wait_timeout_while(state, TICK, |state| *state == WorkerState::Running)
            .unwrap_or_else(|p| p.into_inner());
        if *next == WorkerState::Stopped {
            return;
        }
        drop(next);
        #[cfg(all(test, feature = "engine-rocksdb"))]
        {
            let hook = control.before_tick.lock().unwrap().take();
            if let Some(hook) = hook {
                hook();
            }
        }
        let budget = budget.load(Ordering::Relaxed);
        let mut stats = MaintenanceStats::default();
        let result = step(gc, Keyspace::Kernel, budget, &mut stats)
            .and_then(|()| step(gc, ROTATION[rotation], budget, &mut stats));
        rotation = (rotation + 1) % ROTATION.len();
        if result.is_err() {
            // A fenced or failing KV keeps failing; the next tick retries.
            errors.fetch_add(1, Ordering::Relaxed);
        }
        let config = retention.config();
        if last_sweep.elapsed() >= Duration::from_millis(config.interval_ms) {
            last_sweep = Instant::now();
            let db = retention
                .db
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .as_ref()
                .and_then(Weak::upgrade);
            if let Some(db) = db
                && retention::sweep(&db, &config).is_err()
            {
                errors.fetch_add(1, Ordering::Relaxed);
            }
        }
        state = control.state.lock().unwrap_or_else(|p| p.into_inner());
    }
}

fn step(
    gc: &Gc,
    ks: Keyspace,
    budget: usize,
    stats: &mut MaintenanceStats,
) -> Result<(), CoreError> {
    stats.add(gc.step(ks, budget)?);
    Ok(())
}

impl V2Db {
    #[cfg(all(test, feature = "engine-rocksdb"))]
    pub(in crate::storage) fn test_before_maintenance_tick(&self, hook: impl FnOnce() + Send + 'static) {
        *self.maintenance.control.before_tick.lock().unwrap() = Some(Box::new(hook));
    }

    /// One deterministic round: Kernel, then Catalog, Rows and Index, each
    /// stepped once from its cursor, independent of the thread's rotation.
    /// A replicated database runs no GC.
    pub(crate) fn maintenance_step(&self) -> Result<MaintenanceStats, CoreError> {
        // A cluster gateway holds no data to collect; range leaders own GC.
        self.require_local()?;
        let mut stats = MaintenanceStats::default();
        if !self.maintenance.replicated {
            let budget = self.maintenance.budget.load(Ordering::Relaxed);
            step(&self.gc, Keyspace::Kernel, budget, &mut stats)?;
            for ks in ROTATION {
                step(&self.gc, ks, budget, &mut stats)?;
            }
        }
        stats.watermark = self.gc.watermark().0;
        stats.background_errors = self.maintenance.errors.load(Ordering::Relaxed);
        Ok(stats)
    }

    pub(crate) fn set_gc_retention(&self, retention: Duration) {
        self.gc.set_retention(retention);
    }

    /// Lets the maintenance thread run the retention sweeper on this database.
    pub(crate) fn attach_retention(self: &Arc<Self>) {
        *self
            .maintenance
            .retention
            .db
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = Some(Arc::downgrade(self));
    }

    #[cfg(all(test, feature = "engine-rocksdb"))]
    pub(crate) fn test_task_retention(&self) -> RetentionConfig {
        *self.maintenance.retention.config.lock().unwrap()
    }

    /// Replaces the retention settings; the next background pass uses them.
    pub(crate) fn set_task_retention(&self, config: RetentionConfig) {
        *self
            .maintenance
            .retention
            .config
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = config;
    }

    /// One retention pass now, with the current settings.
    #[cfg_attr(not(all(test, feature = "engine-rocksdb")), allow(dead_code))]
    pub(crate) fn retention_step(&self) -> Result<RetentionStats, CoreError> {
        retention::sweep(self, &self.maintenance.retention.config())
    }
}
