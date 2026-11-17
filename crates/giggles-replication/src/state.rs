//! Local mirror of the ReplicaStore apply contract.
use std::{
    fmt, fs,
    io::{Read, Write},
    path::Path,
    sync::Mutex,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AppliedPosition {
    pub index: u64,
    pub term: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOutcome {
    Applied,
    Skipped,
    /// The index was consumed as a no-op because its batch was refused (stale
    /// epoch or a key outside the range); see `StateMachine::take_refusal`.
    Refused,
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
    /// Apply a v2 configuration entry; None denotes a refused change.
    fn apply_conf_change(
        &self,
        _change: Option<&raft::eraftpb::ConfChangeV2>,
        at: AppliedPosition,
    ) -> Result<ApplyOutcome, StateError> {
        self.apply_noop(at)
    }
    fn checkpoint(&self, target: &Path) -> Result<AppliedPosition, StateError>;
    fn can_install(&self) -> bool {
        true
    }
    fn install(&self, source: &Path) -> Result<AppliedPosition, StateError>;
    fn flush(&self) -> Result<(), StateError>;
    /// Removes and returns the reason the batch at `index` was applied as a
    /// refused no-op (`ApplyOutcome::Refused`), if one was recorded.
    fn take_refusal(&self, _index: u64) -> Option<String> {
        None
    }
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
fn state_error(e: impl fmt::Display) -> StateError {
    StateError(e.to_string())
}
fn read_u64(r: &mut impl Read) -> Result<u64, StateError> {
    let mut bytes = [0; 8];
    r.read_exact(&mut bytes).map_err(state_error)?;
    Ok(u64::from_be_bytes(bytes))
}
impl StateMachine for MemStateMachine {
    fn checkpoint(&self, target: &Path) -> Result<AppliedPosition, StateError> {
        let state = self.0.lock().map_err(state_error)?;
        let p = state
            .applied
            .unwrap_or(AppliedPosition { index: 0, term: 0 });
        fs::create_dir_all(target).map_err(state_error)?;
        let mut file = fs::File::create(target.join("state.bin")).map_err(state_error)?;
        file.write_all(&p.index.to_be_bytes())
            .map_err(state_error)?;
        file.write_all(&p.term.to_be_bytes()).map_err(state_error)?;
        for (at, batch) in &state.batches {
            for n in [at.index, at.term, batch.len() as u64] {
                file.write_all(&n.to_be_bytes()).map_err(state_error)?;
            }
            file.write_all(batch).map_err(state_error)?;
        }
        file.sync_all().map_err(state_error)?;
        Ok(p)
    }
    fn install(&self, source: &Path) -> Result<AppliedPosition, StateError> {
        let bytes = fs::read(source.join("state.bin")).map_err(state_error)?;
        let mut r = bytes.as_slice();
        let p = AppliedPosition {
            index: read_u64(&mut r)?,
            term: read_u64(&mut r)?,
        };
        let mut batches = Vec::new();
        while !r.is_empty() {
            let at = AppliedPosition {
                index: read_u64(&mut r)?,
                term: read_u64(&mut r)?,
            };
            let len = usize::try_from(read_u64(&mut r)?).map_err(state_error)?;
            if len > r.len()
                || at.index > p.index
                || batches
                    .last()
                    .is_some_and(|(last, _): &(AppliedPosition, Vec<u8>)| last.index >= at.index)
            {
                return Err(StateError("invalid memory checkpoint".into()));
            }
            let (batch, rest) = r.split_at(len);
            batches.push((at, batch.to_vec()));
            r = rest;
        }
        let mut state = self.0.lock().map_err(state_error)?;
        *state = Memory {
            applied: (p.index != 0).then_some(p),
            batches,
            fail_next: false,
        };
        fs::remove_dir_all(source).map_err(state_error)?;
        Ok(p)
    }
    fn flush(&self) -> Result<(), StateError> {
        Ok(())
    }
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
