//! Per-range Raft state machine (GD-0075 R5-12) with the apply-time epoch
//! check of R5-22: a proposal made under an old epoch never applies after a
//! descriptor change; it becomes a refused no-op that still consumes its index.
use std::{
    collections::{BTreeMap, VecDeque},
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use giggles_core::{
    range_api::{Epoch, RangeDescriptor, RangeId},
    replicated::{RangeApplyOutcome, RangeStore},
};

use crate::state::{AppliedPosition, ApplyOutcome, StateError, StateMachine};

/// Per-range surface of the multi-range store (Track A's `RangeStore`, or
/// `MemRangeStore` in tests). Positions follow the `StateMachine` skip/gap rules
/// per range.
pub trait RangeStoreOps: Send + Sync {
    fn applied(&self, range: RangeId) -> Result<Option<AppliedPosition>, StateError>;
    /// The range's applied descriptor.
    fn descriptor(&self, range: RangeId) -> Result<Option<RangeDescriptor>, StateError>;
    /// A batch with a key outside the descriptor is applied as a no-op that
    /// advances the position and returns `Refused`.
    fn apply(
        &self,
        range: RangeId,
        batch: &[u8],
        at: AppliedPosition,
    ) -> Result<ApplyOutcome, StateError>;
    fn apply_noop(&self, range: RangeId, at: AppliedPosition) -> Result<ApplyOutcome, StateError>;
    fn export(&self, range: RangeId, target: &Path) -> Result<AppliedPosition, StateError>;
    fn install(&self, range: RangeId, source: &Path) -> Result<AppliedPosition, StateError>;
    fn flush(&self) -> Result<(), StateError>;
}

fn core_position(at: AppliedPosition) -> giggles_core::replicated::AppliedPosition {
    giggles_core::replicated::AppliedPosition {
        index: at.index,
        term: at.term,
    }
}
fn local_position(at: giggles_core::replicated::AppliedPosition) -> AppliedPosition {
    AppliedPosition {
        index: at.index,
        term: at.term,
    }
}
fn range_outcome(outcome: RangeApplyOutcome) -> ApplyOutcome {
    match outcome {
        RangeApplyOutcome::Applied => ApplyOutcome::Applied,
        RangeApplyOutcome::Skipped => ApplyOutcome::Skipped,
        RangeApplyOutcome::Refused => ApplyOutcome::Refused,
    }
}

/// Track A's multi-range RocksDB store (giggles-core R5-03..R5-05).
impl RangeStoreOps for RangeStore {
    fn applied(&self, range: RangeId) -> Result<Option<AppliedPosition>, StateError> {
        Ok(RangeStore::applied(self, range)
            .map_err(state_error)?
            .map(local_position))
    }
    fn descriptor(&self, range: RangeId) -> Result<Option<RangeDescriptor>, StateError> {
        RangeStore::descriptor(self, range).map_err(state_error)
    }
    fn apply(
        &self,
        range: RangeId,
        batch: &[u8],
        at: AppliedPosition,
    ) -> Result<ApplyOutcome, StateError> {
        RangeStore::apply(self, range, batch, core_position(at))
            .map(range_outcome)
            .map_err(state_error)
    }
    fn apply_noop(&self, range: RangeId, at: AppliedPosition) -> Result<ApplyOutcome, StateError> {
        RangeStore::apply_noop(self, range, core_position(at))
            .map(range_outcome)
            .map_err(state_error)
    }
    fn export(&self, range: RangeId, target: &Path) -> Result<AppliedPosition, StateError> {
        RangeStore::export(self, range, target)
            .map(local_position)
            .map_err(state_error)
    }
    fn install(&self, range: RangeId, source: &Path) -> Result<AppliedPosition, StateError> {
        RangeStore::install(self, range, source)
            .map(local_position)
            .map_err(state_error)
    }
    fn flush(&self) -> Result<(), StateError> {
        RangeStore::flush(self).map_err(state_error)
    }
}

const PROPOSAL_VERSION: u8 = 1;
const PROPOSAL_HEADER: usize = 9;
const MAX_REFUSALS: usize = 1024;

/// Range-group proposal envelope, persisted in Raft logs:
/// `[version u8 = 1][epoch u64 BE][batch bytes to the end]`. Version 1 is
/// strict; a new field needs a new version.
pub fn encode_proposal(epoch: Epoch, batch: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(PROPOSAL_HEADER + batch.len());
    out.push(PROPOSAL_VERSION);
    out.extend_from_slice(&epoch.0.to_be_bytes());
    out.extend_from_slice(batch);
    out
}

/// Rejects an unknown version or a truncated header.
pub fn decode_proposal(bytes: &[u8]) -> Result<(Epoch, &[u8]), StateError> {
    if bytes.len() < PROPOSAL_HEADER {
        return Err(StateError("truncated range proposal".into()));
    }
    if bytes[0] != PROPOSAL_VERSION {
        return Err(StateError(format!(
            "unknown range proposal version {}",
            bytes[0]
        )));
    }
    let epoch = u64::from_be_bytes(bytes[1..PROPOSAL_HEADER].try_into().expect("8 bytes"));
    Ok((Epoch(epoch), &bytes[PROPOSAL_HEADER..]))
}

/// RAII marker for a live leader engine of one range; installs are refused
/// while any guard lives.
pub struct EngineGuard(Arc<AtomicUsize>);
impl Drop for EngineGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// `StateMachine` for one range over a shared multi-range store.
pub struct RangeStateMachine {
    range: RangeId,
    store: Arc<dyn RangeStoreOps>,
    engines: Arc<AtomicUsize>,
    refusals: Mutex<VecDeque<(u64, String)>>,
}
impl RangeStateMachine {
    pub fn new(range: RangeId, store: Arc<dyn RangeStoreOps>) -> Self {
        Self {
            range,
            store,
            engines: Arc::default(),
            refusals: Mutex::default(),
        }
    }
    pub fn range(&self) -> RangeId {
        self.range
    }
    pub fn engine_guard(&self) -> EngineGuard {
        self.engines.fetch_add(1, Ordering::SeqCst);
        EngineGuard(self.engines.clone())
    }
    fn refuse(&self, index: u64, reason: String) -> Result<ApplyOutcome, StateError> {
        let mut refusals = self.refusals.lock().map_err(state_error)?;
        if refusals.len() == MAX_REFUSALS {
            refusals.pop_front();
        }
        refusals.push_back((index, reason));
        Ok(ApplyOutcome::Refused)
    }
}
impl StateMachine for RangeStateMachine {
    fn applied(&self) -> Result<Option<AppliedPosition>, StateError> {
        self.store.applied(self.range)
    }
    fn apply(&self, encoded_batch: &[u8], at: AppliedPosition) -> Result<ApplyOutcome, StateError> {
        let (epoch, batch) = decode_proposal(encoded_batch)?;
        let current = self.store.descriptor(self.range)?.map(|d| d.epoch);
        if current != Some(epoch) {
            let outcome = self.store.apply_noop(self.range, at)?;
            if outcome != ApplyOutcome::Applied {
                return Ok(outcome);
            }
            let current = current.map_or_else(|| "none".to_owned(), |e| e.0.to_string());
            return self.refuse(
                at.index,
                format!(
                    "stale epoch: proposal epoch {} but range {} is at epoch {current}",
                    epoch.0, self.range.0
                ),
            );
        }
        match self.store.apply(self.range, batch, at)? {
            ApplyOutcome::Refused => self.refuse(
                at.index,
                format!(
                    "range {} refused the batch: a key is outside its bounds or the descriptor change is invalid",
                    self.range.0
                ),
            ),
            outcome => Ok(outcome),
        }
    }
    fn apply_noop(&self, at: AppliedPosition) -> Result<ApplyOutcome, StateError> {
        self.store.apply_noop(self.range, at)
    }
    fn checkpoint(&self, target: &Path) -> Result<AppliedPosition, StateError> {
        self.store.export(self.range, target)
    }
    fn can_install(&self) -> bool {
        self.engines.load(Ordering::SeqCst) == 0
    }
    fn install(&self, source: &Path) -> Result<AppliedPosition, StateError> {
        self.store.install(self.range, source)
    }
    fn flush(&self) -> Result<(), StateError> {
        self.store.flush()
    }
    fn take_refusal(&self, index: u64) -> Option<String> {
        let mut refusals = self.refusals.lock().ok()?;
        let at = refusals.iter().position(|(i, _)| *i == index)?;
        refusals.remove(at).map(|(_, reason)| reason)
    }
}

/// One operation of a `MemRangeStore` batch. Keys are range keys
/// (`range_api::range_key`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    SetDescriptor(RangeDescriptor),
}

struct MemRange {
    applied: Option<AppliedPosition>,
    descriptor: RangeDescriptor,
    data: BTreeMap<Vec<u8>, Vec<u8>>,
}
#[derive(Default)]
struct MemRanges {
    ranges: BTreeMap<RangeId, MemRange>,
    fail_next: bool,
}

/// In-memory multi-range `RangeStoreOps` fake for tests and the harness.
///
/// Batch encoding (fake, test-only): a sequence of ops, each
/// `[1][key][value]` (Put), `[2][key]` (Delete) or `[3][descriptor]`
/// (SetDescriptor, section-1 encoding), every field `len u32 BE + bytes`.
/// A batch is refused as a whole when a key is outside the descriptor in
/// force at that op, or a SetDescriptor changes the id or does not raise the
/// epoch. Export writes one file per range, `range-<id>.bin`, under the target.
#[derive(Default)]
pub struct MemRangeStore(Mutex<MemRanges>);
impl MemRangeStore {
    pub fn new() -> Self {
        Self::default()
    }
    /// Encodes a batch; panics on an invalid descriptor (test helper).
    pub fn batch(ops: &[MemOp]) -> Vec<u8> {
        let mut out = Vec::new();
        for op in ops {
            match op {
                MemOp::Put { key, value } => {
                    out.push(1);
                    put_bytes(&mut out, key);
                    put_bytes(&mut out, value);
                }
                MemOp::Delete { key } => {
                    out.push(2);
                    put_bytes(&mut out, key);
                }
                MemOp::SetDescriptor(desc) => {
                    out.push(3);
                    put_bytes(&mut out, &desc.encode().expect("valid descriptor"));
                }
            }
        }
        out
    }
    pub fn create_range(&self, descriptor: RangeDescriptor) -> Result<(), StateError> {
        let mut state = self.0.lock().map_err(state_error)?;
        if state.ranges.contains_key(&descriptor.id) {
            return Err(StateError(format!("range {} exists", descriptor.id.0)));
        }
        let range = MemRange {
            applied: None,
            descriptor,
            data: BTreeMap::new(),
        };
        state.ranges.insert(range.descriptor.id, range);
        Ok(())
    }
    /// The next `apply` or `apply_noop` on any range fails once.
    pub fn fail_next_apply(&self) {
        self.0.lock().expect("store mutex poisoned").fail_next = true;
    }
    pub fn get(&self, range: RangeId, key: &[u8]) -> Option<Vec<u8>> {
        let state = self.0.lock().expect("store mutex poisoned");
        state.ranges.get(&range)?.data.get(key).cloned()
    }
    pub fn keys(&self, range: RangeId) -> Vec<Vec<u8>> {
        let state = self.0.lock().expect("store mutex poisoned");
        state
            .ranges
            .get(&range)
            .map(|r| r.data.keys().cloned().collect())
            .unwrap_or_default()
    }
    /// Checks injected failure and the skip/gap rules; `Some` ends the apply.
    fn admit(
        state: &mut MemRanges,
        range: RangeId,
        at: AppliedPosition,
    ) -> Result<(&mut MemRange, Option<ApplyOutcome>), StateError> {
        if std::mem::take(&mut state.fail_next) {
            return Err(StateError("injected apply failure".into()));
        }
        let r = state
            .ranges
            .get_mut(&range)
            .ok_or_else(|| StateError(format!("unknown range {}", range.0)))?;
        let applied = r.applied.map_or(0, |p| p.index);
        if at.index <= applied {
            return Ok((r, Some(ApplyOutcome::Skipped)));
        }
        if applied.checked_add(1) != Some(at.index) {
            return Err(StateError(format!(
                "apply gap: index {} after {applied}",
                at.index
            )));
        }
        Ok((r, None))
    }
}
fn file_for(dir: &Path, range: RangeId) -> PathBuf {
    dir.join(format!("range-{}.bin", range.0))
}
impl RangeStoreOps for MemRangeStore {
    fn applied(&self, range: RangeId) -> Result<Option<AppliedPosition>, StateError> {
        let state = self.0.lock().map_err(state_error)?;
        Ok(state.ranges.get(&range).and_then(|r| r.applied))
    }
    fn descriptor(&self, range: RangeId) -> Result<Option<RangeDescriptor>, StateError> {
        let state = self.0.lock().map_err(state_error)?;
        Ok(state.ranges.get(&range).map(|r| r.descriptor.clone()))
    }
    fn apply(
        &self,
        range: RangeId,
        batch: &[u8],
        at: AppliedPosition,
    ) -> Result<ApplyOutcome, StateError> {
        let mut state = self.0.lock().map_err(state_error)?;
        let (r, done) = Self::admit(&mut state, range, at)?;
        if let Some(outcome) = done {
            return Ok(outcome);
        }
        let ops = decode_batch(batch)?;
        let mut desc = r.descriptor.clone();
        let mut allowed = true;
        for op in &ops {
            allowed = match op {
                MemOp::Put { key, .. } | MemOp::Delete { key } => desc.contains(key),
                MemOp::SetDescriptor(next) if next.id == desc.id && next.epoch > desc.epoch => {
                    desc = next.clone();
                    true
                }
                MemOp::SetDescriptor(_) => false,
            };
            if !allowed {
                break;
            }
        }
        r.applied = Some(at);
        if !allowed {
            return Ok(ApplyOutcome::Refused);
        }
        for op in ops {
            match op {
                MemOp::Put { key, value } => {
                    r.data.insert(key, value);
                }
                MemOp::Delete { key } => {
                    r.data.remove(&key);
                }
                MemOp::SetDescriptor(_) => {}
            }
        }
        r.descriptor = desc;
        Ok(ApplyOutcome::Applied)
    }
    fn apply_noop(&self, range: RangeId, at: AppliedPosition) -> Result<ApplyOutcome, StateError> {
        let mut state = self.0.lock().map_err(state_error)?;
        let (r, done) = Self::admit(&mut state, range, at)?;
        if let Some(outcome) = done {
            return Ok(outcome);
        }
        r.applied = Some(at);
        Ok(ApplyOutcome::Applied)
    }
    /// File: `[index BE8][term BE8][descriptor][count BE8][(key, value)]*`,
    /// variable fields as `len u32 BE + bytes`.
    fn export(&self, range: RangeId, target: &Path) -> Result<AppliedPosition, StateError> {
        let state = self.0.lock().map_err(state_error)?;
        let r = state
            .ranges
            .get(&range)
            .ok_or_else(|| StateError(format!("unknown range {}", range.0)))?;
        let p = r.applied.unwrap_or(AppliedPosition { index: 0, term: 0 });
        let mut out = Vec::new();
        out.extend_from_slice(&p.index.to_be_bytes());
        out.extend_from_slice(&p.term.to_be_bytes());
        put_bytes(&mut out, &r.descriptor.encode().map_err(state_error)?);
        out.extend_from_slice(&(r.data.len() as u64).to_be_bytes());
        for (key, value) in &r.data {
            put_bytes(&mut out, key);
            put_bytes(&mut out, value);
        }
        fs::create_dir_all(target).map_err(state_error)?;
        let mut file = fs::File::create(file_for(target, range)).map_err(state_error)?;
        file.write_all(&out).map_err(state_error)?;
        file.sync_all().map_err(state_error)?;
        Ok(p)
    }
    fn install(&self, range: RangeId, source: &Path) -> Result<AppliedPosition, StateError> {
        let path = file_for(source, range);
        let bytes = fs::read(&path).map_err(state_error)?;
        let mut r = bytes.as_slice();
        let p = AppliedPosition {
            index: take_u64(&mut r)?,
            term: take_u64(&mut r)?,
        };
        let descriptor = RangeDescriptor::decode(take_bytes(&mut r)?).map_err(state_error)?;
        if descriptor.id != range {
            return Err(StateError("export is for another range".into()));
        }
        let mut data = BTreeMap::new();
        for _ in 0..take_u64(&mut r)? {
            let key = take_bytes(&mut r)?.to_vec();
            data.insert(key, take_bytes(&mut r)?.to_vec());
        }
        if !r.is_empty() {
            return Err(StateError("trailing range export bytes".into()));
        }
        let installed = MemRange {
            applied: (p.index != 0).then_some(p),
            descriptor,
            data,
        };
        self.0
            .lock()
            .map_err(state_error)?
            .ranges
            .insert(range, installed);
        fs::remove_file(&path).map_err(state_error)?;
        Ok(p)
    }
    fn flush(&self) -> Result<(), StateError> {
        Ok(())
    }
}

fn decode_batch(mut r: &[u8]) -> Result<Vec<MemOp>, StateError> {
    let mut ops = Vec::new();
    while let Some((&tag, rest)) = r.split_first() {
        r = rest;
        ops.push(match tag {
            1 => MemOp::Put {
                key: take_bytes(&mut r)?.to_vec(),
                value: take_bytes(&mut r)?.to_vec(),
            },
            2 => MemOp::Delete {
                key: take_bytes(&mut r)?.to_vec(),
            },
            3 => MemOp::SetDescriptor(
                RangeDescriptor::decode(take_bytes(&mut r)?).map_err(state_error)?,
            ),
            _ => return Err(StateError(format!("unknown memory range op {tag}"))),
        });
    }
    Ok(ops)
}
fn state_error(e: impl std::fmt::Display) -> StateError {
    StateError(e.to_string())
}
fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
}
fn take<'a>(r: &mut &'a [u8], len: usize) -> Result<&'a [u8], StateError> {
    if r.len() < len {
        return Err(StateError("truncated memory range bytes".into()));
    }
    let (value, rest) = r.split_at(len);
    *r = rest;
    Ok(value)
}
fn take_u64(r: &mut &[u8]) -> Result<u64, StateError> {
    Ok(u64::from_be_bytes(take(r, 8)?.try_into().expect("8 bytes")))
}
fn take_bytes<'a>(r: &mut &'a [u8]) -> Result<&'a [u8], StateError> {
    let len = u32::from_be_bytes(take(r, 4)?.try_into().expect("4 bytes"));
    take(r, len as usize)
}
