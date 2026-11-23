//! Trusted test-only seam at the commit worker's Sync boundary.
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};
#[derive(Default)]
pub struct CommitSyncHook {
    state: Mutex<State>,
    changed: Condvar,
    #[cfg(test)]
    pause_before_publish: bool,
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
    WalError {
        after_apply: bool,
    },
}
impl CommitSyncHook {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
    /// Pause a successful nonempty Sync batch before publication.
    #[cfg(test)]
    pub(crate) fn before_publish() -> Arc<Self> {
        Arc::new(Self {
            pause_before_publish: true,
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

    fn release_with(&self, outcome: SyncOutcome) {
        self.state.lock().unwrap().outcome = Some(outcome);
        self.changed.notify_all();
    }

    pub(crate) fn before_sync(&self) -> SyncOutcome {
        #[cfg(test)]
        if self.pause_before_publish {
            return SyncOutcome::Proceed;
        }
        self.pause()
    }

    #[cfg(test)]
    pub(crate) fn after_sync(&self) {
        if self.pause_before_publish {
            self.pause();
        }
    }

    fn pause(&self) -> SyncOutcome {
        let mut state = self.state.lock().unwrap();
        state.entered = true;
        self.changed.notify_all();
        self.changed
            .wait_while(state, |state| state.outcome.is_none())
            .unwrap()
            .outcome
            .unwrap()
    }
}
