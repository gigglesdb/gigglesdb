//! Trusted test-only seam at the commit worker's Sync boundary.
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};
#[derive(Default)]
pub struct CommitSyncHook { state: Mutex<State>, changed: Condvar }
#[derive(Default)]
struct State { entered: bool, released: bool, fail: bool }
impl CommitSyncHook {
    pub fn new() -> Arc<Self> { Arc::new(Self::default()) }
    pub fn wait_entered(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut state = self.state.lock().unwrap();
        while !state.entered {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() { return false; }
            state = self.changed.wait_timeout(state, remaining).unwrap().0;
        }
        true
    }
    pub fn release(&self, fail: bool) {
        let mut state = self.state.lock().unwrap();
        state.fail = fail;
        state.released = true;
        self.changed.notify_all();
    }
    pub(crate) fn before_sync(&self) -> bool {
        let mut state = self.state.lock().unwrap();
        state.entered = true;
        self.changed.notify_all();
        while !state.released { state = self.changed.wait(state).unwrap(); }
        state.fail
    }
}
