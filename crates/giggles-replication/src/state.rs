//! Local mirror of the ReplicaStore apply contract.
use std::{fmt, sync::Mutex};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AppliedPosition {
    pub index: u64,
    pub term: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOutcome {
    Applied,
    Skipped,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateError(pub String);
impl fmt::Display for StateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for StateError {}

/// Apply atomically: skip indexes already applied, accept only the next index,
/// and reject gaps. An empty machine has applied index zero.
pub trait StateMachine: Send + Sync {
    fn applied(&self) -> Result<Option<AppliedPosition>, StateError>;
    fn apply(&self, encoded_batch: &[u8], at: AppliedPosition) -> Result<ApplyOutcome, StateError>;
    fn apply_noop(&self, at: AppliedPosition) -> Result<ApplyOutcome, StateError>;
}

#[derive(Default)]
struct Memory {
    applied: Option<AppliedPosition>,
    batches: Vec<(AppliedPosition, Vec<u8>)>,
    fail_next: bool,
}

/// In-memory fake for tests and the harness; batch bytes are kept opaque.
#[derive(Default)]
pub struct MemStateMachine(Mutex<Memory>);
impl MemStateMachine {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn applied_batches(&self) -> Vec<(AppliedPosition, Vec<u8>)> {
        self.0.lock().expect("state mutex poisoned").batches.clone()
    }
    pub fn fail_next_apply(&self) {
        self.0.lock().expect("state mutex poisoned").fail_next = true;
    }
    fn advance(
        &self,
        batch: Option<&[u8]>,
        at: AppliedPosition,
    ) -> Result<ApplyOutcome, StateError> {
        let mut state = self
            .0
            .lock()
            .map_err(|_| StateError("state mutex poisoned".into()))?;
        if std::mem::take(&mut state.fail_next) {
            return Err(StateError("injected apply failure".into()));
        }
        let applied = state.applied.map_or(0, |p| p.index);
        if at.index <= applied {
            return Ok(ApplyOutcome::Skipped);
        }
        if applied.checked_add(1) != Some(at.index) {
            return Err(StateError(format!(
                "apply gap: index {} after {applied}",
                at.index
            )));
        }
        if let Some(bytes) = batch {
            state.batches.push((at, bytes.to_vec()));
        }
        state.applied = Some(at);
        Ok(ApplyOutcome::Applied)
    }
}
impl StateMachine for MemStateMachine {
    fn applied(&self) -> Result<Option<AppliedPosition>, StateError> {
        self.0
            .lock()
            .map(|s| s.applied)
            .map_err(|_| StateError("state mutex poisoned".into()))
    }
    fn apply(&self, encoded_batch: &[u8], at: AppliedPosition) -> Result<ApplyOutcome, StateError> {
        self.advance(Some(encoded_batch), at)
    }
    fn apply_noop(&self, at: AppliedPosition) -> Result<ApplyOutcome, StateError> {
        self.advance(None, at)
    }
}
