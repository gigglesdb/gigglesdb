//! Trusted test-only seams at commit append and sync boundaries.
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};
#[derive(Default)]
pub struct CommitSyncHook {
    state: Mutex<State>,
    changed: Condvar,
    point: Point,
}
#[derive(Default, PartialEq, Eq)]
enum Point {
    #[default]
    BeforeSync,
    BeforeAppend,
    AfterAppend,
    #[cfg(test)]
    BeforePublish,
}
#[derive(Default)]
struct State {
    entered: bool,
    outcome: Option<SyncOutcome>,
}

#[derive(Clone, Copy)]
pub(crate) enum SyncOutcome {
    Proceed,
    Fail,
    #[cfg(all(test, feature = "engine-rocksdb"))]
    Panic,
    #[cfg(all(test, feature = "engine-rocksdb"))]
    WalError {
        after_apply: bool,
    },
}
impl CommitSyncHook {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
    pub fn before_append() -> Arc<Self> {
        Arc::new(Self { point: Point::BeforeAppend, ..Self::default() })
    }

    pub fn after_append_before_enqueue() -> Arc<Self> {
        Arc::new(Self { point: Point::AfterAppend, ..Self::default() })
    }

    /// Pause a successful nonempty Sync batch before publication.
    #[cfg(test)]
    pub(crate) fn before_publish() -> Arc<Self> {
        Arc::new(Self {
            point: Point::BeforePublish,
            ..Self::default()
        })
    }
    pub fn wait_entered(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut state = self.state.lock().unwrap();
        while !state.entered {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            state = self.changed.wait_timeout(state, remaining).unwrap().0;
        }
        true
    }
    pub fn release(&self, fail: bool) {
        self.release_with(if fail {
            SyncOutcome::Fail
        } else {
            SyncOutcome::Proceed
        });
    }

    #[cfg(all(test, feature = "engine-rocksdb"))]
    pub(crate) fn release_with_wal_error(&self, after_apply: bool) {
        self.release_with(SyncOutcome::WalError { after_apply });
    }

    #[cfg(all(test, feature = "engine-rocksdb"))]
    pub(crate) fn release_with_panic(&self) {
        self.release_with(SyncOutcome::Panic);
    }

    fn release_with(&self, outcome: SyncOutcome) {
        self.state.lock().unwrap().outcome = Some(outcome);
        self.changed.notify_all();
    }

    pub(crate) fn before_sync(&self) -> SyncOutcome {
        self.at(Point::BeforeSync)
    }

    pub(crate) fn on_before_append(&self) -> SyncOutcome {
        self.at(Point::BeforeAppend)
    }

    pub(crate) fn on_after_append(&self) {
        self.at(Point::AfterAppend);
    }

    fn at(&self, point: Point) -> SyncOutcome {
        if self.point == point { self.pause() } else { SyncOutcome::Proceed }
    }

    #[cfg(test)]
    pub(crate) fn after_sync(&self) {
        self.at(Point::BeforePublish);
    }

    fn pause(&self) -> SyncOutcome {
        let mut state = self.state.lock().unwrap();
        state.entered = true;
        self.changed.notify_all();
        let outcome = self.changed
            .wait_while(state, |state| state.outcome.is_none())
            .unwrap()
            .outcome
            .unwrap();
        #[cfg(all(test, feature = "engine-rocksdb"))]
        if let SyncOutcome::Panic = outcome {
            panic!("injected commit worker panic");
        }
        outcome
    }
}
