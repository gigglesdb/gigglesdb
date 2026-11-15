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
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
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
    fenced: AtomicBool,
}
impl TermSink {
    pub fn new(handle: ReplicaHandle, term: u64) -> Self {
        Self {
            handle,
            term,
            fenced: AtomicBool::new(false),
        }
    }
    pub fn fence(&self) {
        self.fenced.store(true, Ordering::SeqCst);
    }
    pub fn is_fenced(&self) -> bool {
        self.fenced.load(Ordering::SeqCst)
    }
}
impl gr::BatchSink for TermSink {
    fn replicate(&self, batch: Vec<u8>) -> Result<(), gr::ReplicateError> {
        let role = self.handle.role();
        let result =
            if self.is_fenced() || role.state != StateRole::Leader || role.term != self.term {
                Err(gr::ReplicateError::NotLeader)
            } else {
                self.handle.replicate(batch).map_err(|e| match e {
                    ReplicateError::NotLeader => gr::ReplicateError::NotLeader,
                    e => gr::ReplicateError::Unavailable(e.to_string()),
                })
            };
        if result.is_err() {
            self.fence();
        }
        result
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
