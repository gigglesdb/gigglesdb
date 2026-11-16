//! Adapter between the core storage seam and the Raft driver.
use crate::{
    error::Error,
    log_store::LogStore,
    sink::{ReplicaHandle, ReplicateError},
    state::{AppliedPosition, ApplyOutcome, StateError, StateMachine},
};
use giggles_core::replicated as gr;
use raft::StateRole;
use std::{
    path::Path,
    sync::{Arc, PoisonError, RwLock},
};

impl From<AppliedPosition> for gr::AppliedPosition {
    fn from(p: AppliedPosition) -> Self {
        Self {
            index: p.index,
            term: p.term,
        }
    }
}
impl From<gr::AppliedPosition> for AppliedPosition {
    fn from(p: gr::AppliedPosition) -> Self {
        Self {
            index: p.index,
            term: p.term,
        }
    }
}
fn outcome(
    result: Result<gr::ApplyOutcome, giggles_core::CoreError>,
) -> Result<ApplyOutcome, StateError> {
    result
        .map(|o| match o {
            gr::ApplyOutcome::Applied => ApplyOutcome::Applied,
            gr::ApplyOutcome::Skipped => ApplyOutcome::Skipped,
        })
        .map_err(|e| StateError(e.to_string()))
}
impl StateMachine for gr::ReplicaStore {
    fn checkpoint(&self, target: &Path) -> Result<AppliedPosition, StateError> {
        let before = self.applied().map_err(|e| StateError(e.to_string()))?;
        gr::ReplicaStore::checkpoint(self, target).map_err(|e| StateError(e.to_string()))?;
        if self.applied().map_err(|e| StateError(e.to_string()))? != before {
            return Err(StateError(
                "applied position changed during checkpoint".into(),
            ));
        }
        Ok(before
            .map(Into::into)
            .unwrap_or(AppliedPosition { index: 0, term: 0 }))
    }
    fn install(&self, _: &Path) -> Result<AppliedPosition, StateError> {
        Err(StateError("install needs a StoreSlot".into()))
    }
    fn flush(&self) -> Result<(), StateError> {
        gr::ReplicaStore::flush(self).map_err(|e| StateError(e.to_string()))
    }
    fn applied(&self) -> Result<Option<AppliedPosition>, StateError> {
        self.applied()
            .map(|p| p.map(Into::into))
            .map_err(|e| StateError(e.to_string()))
    }
    fn apply(&self, batch: &[u8], at: AppliedPosition) -> Result<ApplyOutcome, StateError> {
        outcome(self.apply(batch, at.into()))
    }
    fn apply_noop(&self, at: AppliedPosition) -> Result<ApplyOutcome, StateError> {
        outcome(self.apply_noop(at.into()))
    }
}

/// A sink that can never be reused after losing its leadership term or failing.
pub struct TermSink {
    handle: ReplicaHandle,
    term: u64,
    // Writers check the flag and submit under the read lock, so once `fence`
    // has returned no proposal from this sink can reach the Raft log.
    fenced: RwLock<bool>,
}
impl TermSink {
    pub fn new(handle: ReplicaHandle, term: u64) -> Self {
        Self {
            handle,
            term,
            fenced: RwLock::new(false),
        }
    }
    /// Refuse all later writes. Waits for any write that already passed the
    /// fence check to finish submitting its proposal, never for it to commit.
    pub fn fence(&self) {
        *self.fenced.write().unwrap_or_else(PoisonError::into_inner) = true;
    }
    pub fn is_fenced(&self) -> bool {
        *self.fenced.read().unwrap_or_else(PoisonError::into_inner)
    }
}
impl TermSink {
    /// `replicate` with this crate's error, so a caller can tell a refused batch
    /// (definitely not applied) from an unknown outcome. Any error fences the sink.
    pub(crate) fn replicate_local(&self, batch: Vec<u8>) -> Result<(), ReplicateError> {
        let submitted = {
            let fenced = self.fenced.read().unwrap_or_else(PoisonError::into_inner);
            let role = self.handle.role();
            if *fenced || role.state != StateRole::Leader || role.term != self.term {
                Err(ReplicateError::NotLeader)
            } else {
                self.handle.submit(batch)
            }
        };
        let result =
            submitted.and_then(|reply| reply.recv().unwrap_or(Err(ReplicateError::Stopped)));
        if result.is_err() {
            self.fence();
        }
        result
    }
}
impl gr::BatchSink for TermSink {
    fn replicate(&self, batch: Vec<u8>) -> Result<(), gr::ReplicateError> {
        self.replicate_local(batch).map_err(|e| match e {
            ReplicateError::NotLeader => gr::ReplicateError::NotLeader,
            e => gr::ReplicateError::Unavailable(e.to_string()),
        })
    }
}

/// Open the data store and its separate sibling Raft log with matching identities.
pub fn bootstrap(
    data_dir: &Path,
    log_dir: &Path,
    domain: [u8; 16],
    node_id: u64,
) -> Result<(Arc<gr::ReplicaStore>, LogStore), Error> {
    let empty = match std::fs::read_dir(data_dir) {
        Ok(mut entries) => entries.next().transpose()?.is_none(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => true,
        Err(e) => return Err(e.into()),
    };
    let store = if empty {
        gr::ReplicaStore::create(data_dir, domain)
    } else {
        gr::ReplicaStore::open(data_dir)
    }
    .map_err(|e| Error::Core(e.to_string()))?;
    if store.domain() != domain {
        return Err(Error::Identity("data domain mismatch".into()));
    }
    let log = LogStore::open(log_dir, domain, node_id)?;
    Ok((Arc::new(store), log))
}

/// A SQL engine permanently bound to one leadership term.
pub struct ReplicatedEngine {
    admin: giggles_core::Admin,
    sink: Arc<TermSink>,
    term: u64,
}
impl ReplicatedEngine {
    pub fn admin(&self) -> &giggles_core::Admin {
        &self.admin
    }
    pub fn term(&self) -> u64 {
        self.term
    }
}
impl crate::role::Engine for ReplicatedEngine {
    fn fence(&self) {
        self.sink.fence();
    }
    fn is_fenced(&self) -> bool {
        self.sink.is_fenced()
    }
}

/// Open a fresh engine for each confirmed leadership term.
pub fn engine_opener(
    store: Arc<gr::ReplicaStore>,
    handle: ReplicaHandle,
    limits: giggles_core::Limits,
    write_set_bytes: usize,
) -> Box<dyn FnMut(u64) -> Result<ReplicatedEngine, String> + Send> {
    Box::new(move |term| {
        let sink = Arc::new(TermSink::new(handle.clone(), term));
        let admin = store
            .open_admin(sink.clone(), limits, write_set_bytes)
            .map_err(|e| e.to_string())?;
        Ok(ReplicatedEngine { admin, sink, term })
    })
}

/// Open each leadership engine on the store currently installed in the slot.
pub fn slot_engine_opener(
    slot: Arc<crate::store_slot::StoreSlot>,
    handle: ReplicaHandle,
    limits: giggles_core::Limits,
    write_set_bytes: usize,
) -> Box<dyn FnMut(u64) -> Result<ReplicatedEngine, String> + Send> {
    Box::new(move |term| {
        let sink = Arc::new(TermSink::new(handle.clone(), term));
        let admin = slot
            .current()
            .map_err(|e| e.to_string())?
            .open_admin(sink.clone(), limits, write_set_bytes)
            .map_err(|e| e.to_string())?;
        Ok(ReplicatedEngine { admin, sink, term })
    })
}
