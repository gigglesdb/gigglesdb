//! MVCC GC wiring for `V2Db` (GD-0061 D3, design D-I).
//!
//! One background thread per non-replicated database. Every `TICK` it runs a
//! Kernel step, then one of Catalog, Rows and Index in rotation. Authority
//! heads live in Kernel and are rewritten on every commit, so Kernel is
//! collected on every tick. The thread waits on a condvar so dropping the
//! database stops it promptly, and the drop joins it before the KV closes.
#![cfg_attr(not(feature = "engine-rocksdb"), allow(dead_code))]
use super::{CoreError, V2Db, engine};
use engine::{
    gc::{Gc, StepStats},
    kv::Keyspace,
};
use std::{
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
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

struct Stop {
    stopped: Mutex<bool>,
    wake: Condvar,
}

pub(crate) struct Maintenance {
    stop: Arc<Stop>,
    thread: Option<JoinHandle<()>>,
    replicated: bool,
    budget: Arc<AtomicUsize>,
    errors: Arc<AtomicU64>,
}

impl Maintenance {
    /// Starts the GC thread unless the database is replicated.
    pub(crate) fn start(gc: &Arc<Gc>, replicated: bool) -> Result<Self, CoreError> {
        let stop = Arc::new(Stop {
            stopped: Mutex::new(false),
            wake: Condvar::new(),
        });
        let budget = Arc::new(AtomicUsize::new(BUDGET));
        let errors = Arc::new(AtomicU64::new(0));
        let thread = if replicated {
            None
        } else {
            let (gc, stop, budget, errors) =
                (gc.clone(), stop.clone(), budget.clone(), errors.clone());
            Some(
                std::thread::Builder::new()
                    .name("giggles-v2-gc".into())
                    .spawn(move || run(&gc, &stop, &budget, &errors))
                    .map_err(|error| CoreError::Storage(error.to_string()))?,
            )
        };
        Ok(Self {
            stop,
            thread,
            replicated,
            budget,
            errors,
        })
    }

    #[cfg(test)]
    pub(crate) fn running(&self) -> bool {
        self.thread.is_some()
    }

    #[cfg(test)]
    pub(crate) fn set_budget(&self, budget: usize) {
        self.budget.store(budget, Ordering::Relaxed);
    }
}

impl Drop for Maintenance {
    fn drop(&mut self) {
        *self.stop.stopped.lock().unwrap_or_else(|p| p.into_inner()) = true;
        self.stop.wake.notify_all();
        if let Some(thread) = self.thread.take() {
            // A panicking GC thread has nothing left to clean up.
            let _ = thread.join();
        }
    }
}

fn run(gc: &Gc, stop: &Stop, budget: &AtomicUsize, errors: &AtomicU64) {
    let mut rotation = 0;
    loop {
        let stopped = stop.stopped.lock().unwrap_or_else(|p| p.into_inner());
        let (stopped, _) = stop
            .wake
            .wait_timeout_while(stopped, TICK, |stopped| !*stopped)
            .unwrap_or_else(|p| p.into_inner());
        if *stopped {
            return;
        }
        drop(stopped);
        let budget = budget.load(Ordering::Relaxed);
        let mut stats = MaintenanceStats::default();
        let result = step(gc, Keyspace::Kernel, budget, &mut stats)
            .and_then(|()| step(gc, ROTATION[rotation], budget, &mut stats));
        rotation = (rotation + 1) % ROTATION.len();
        if result.is_err() {
            // A fenced or failing KV keeps failing; the next tick retries.
            errors.fetch_add(1, Ordering::Relaxed);
        }
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
}
