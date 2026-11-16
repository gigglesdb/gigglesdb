//! Replaceable replicated storage with recovery of interrupted directory swaps.
use crate::{
    error::Error,
    state::{AppliedPosition, ApplyOutcome, StateError, StateMachine},
};
use giggles_core::replicated as gr;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

pub struct StoreSlot {
    data_dir: PathBuf,
    domain: [u8; 16],
    current: RwLock<Option<Arc<gr::ReplicaStore>>>,
}
fn sibling(path: &Path, suffix: &str) -> Result<PathBuf, Error> {
    let mut name = path
        .file_name()
        .ok_or_else(|| Error::Identity("data directory needs a file name".into()))?
        .to_os_string();
    name.push(suffix);
    Ok(path.with_file_name(name))
}
fn state_error(e: impl std::fmt::Display) -> StateError {
    StateError(e.to_string())
}
fn remove(path: &Path) -> std::io::Result<()> {
    if path.exists() {
        fs::remove_dir_all(path)?;
    }
    Ok(())
}
impl StoreSlot {
    pub fn open_or_create(data_dir: &Path, domain: [u8; 16]) -> Result<Self, Error> {
        let old = sibling(data_dir, ".old")?;
        if old.exists() {
            if data_dir.exists() && gr::ReplicaStore::open(data_dir).is_ok() {
                remove(&old)?;
            } else {
                remove(data_dir)?;
                fs::rename(&old, data_dir)?;
            }
        }
        remove(&sibling(data_dir, ".failed")?)?;
        let empty = match fs::read_dir(data_dir) {
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
        Ok(Self {
            data_dir: data_dir.into(),
            domain,
            current: RwLock::new(Some(Arc::new(store))),
        })
    }
    pub fn current(&self) -> Result<Arc<gr::ReplicaStore>, Error> {
        self.current
            .read()
            .map_err(|_| Error::Core("store lock poisoned".into()))?
            .clone()
            .ok_or_else(|| Error::Core("store unavailable".into()))
    }
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }
    pub fn domain(&self) -> [u8; 16] {
        self.domain
    }
    /// Only the applying thread may call this, without concurrent apply calls.
    pub fn checkpoint(&self, target: &Path) -> Result<AppliedPosition, StateError> {
        let store = self.current().map_err(state_error)?;
        let before = store.applied().map_err(state_error)?;
        store.checkpoint(target).map_err(state_error)?;
        if store.applied().map_err(state_error)? != before {
            return Err(StateError(
                "applied position changed during checkpoint".into(),
            ));
        }
        Ok(before
            .map(Into::into)
            .unwrap_or(AppliedPosition { index: 0, term: 0 }))
    }
    pub fn install(&self, source: &Path) -> Result<AppliedPosition, StateError> {
        let old = sibling(&self.data_dir, ".old").map_err(state_error)?;
        let failed = sibling(&self.data_dir, ".failed").map_err(state_error)?;
        let mut current = self.current.write().map_err(state_error)?;
        if current.as_ref().is_some_and(|store| !store.is_exclusive()) {
            return Err(StateError("store busy".into()));
        }
        let store = current
            .take()
            .ok_or_else(|| StateError("store unavailable".into()))?;
        match Arc::try_unwrap(store) {
            Ok(store) => drop(store),
            Err(store) => {
                *current = Some(store);
                return Err(StateError("store busy".into()));
            }
        }
        if let Err(e) = fs::rename(&self.data_dir, &old) {
            *current = gr::ReplicaStore::open(&self.data_dir).ok().map(Arc::new);
            return Err(state_error(e));
        }
        crate::failpoint::hit(&self.domain(), None, "slot.install.old");
        let result = (|| {
            fs::rename(source, &self.data_dir).map_err(state_error)?;
            crate::failpoint::hit(&self.domain(), None, "slot.install.new");
            let store = gr::ReplicaStore::open(&self.data_dir).map_err(state_error)?;
            if store.domain() != self.domain {
                return Err(StateError("data domain mismatch".into()));
            }
            let at = store
                .applied()
                .map_err(state_error)?
                .ok_or_else(|| StateError("snapshot has no applied position".into()))?;
            remove(&old).map_err(state_error)?;
            Ok((store, at.into()))
        })();
        match result {
            Ok((store, at)) => {
                *current = Some(Arc::new(store));
                Ok(at)
            }
            Err(e) => {
                let rollback = (|| -> std::io::Result<()> {
                    if self.data_dir.exists() {
                        fs::rename(&self.data_dir, &failed)?;
                    }
                    fs::rename(&old, &self.data_dir)
                })();
                *current = gr::ReplicaStore::open(&self.data_dir)
                    .or_else(|_| gr::ReplicaStore::open(&old))
                    .ok()
                    .map(Arc::new);
                if let Err(restore) = rollback {
                    return Err(StateError(format!("{e}; rollback: {restore}")));
                }
                remove(&failed).map_err(state_error)?;
                Err(e)
            }
        }
    }
    pub fn flush(&self) -> Result<(), StateError> {
        self.current()
            .map_err(state_error)?
            .flush()
            .map_err(state_error)
    }
}
impl StateMachine for StoreSlot {
    fn can_install(&self) -> bool {
        self.current.read().is_ok_and(|current| {
            current
                .as_ref()
                .is_some_and(|store| Arc::strong_count(store) == 1 && store.is_exclusive())
        })
    }
    fn checkpoint(&self, target: &Path) -> Result<AppliedPosition, StateError> {
        StoreSlot::checkpoint(self, target)
    }
    fn install(&self, source: &Path) -> Result<AppliedPosition, StateError> {
        StoreSlot::install(self, source)
    }
    fn flush(&self) -> Result<(), StateError> {
        StoreSlot::flush(self)
    }
    fn applied(&self) -> Result<Option<AppliedPosition>, StateError> {
        StateMachine::applied(self.current().map_err(state_error)?.as_ref())
    }
    fn apply(&self, batch: &[u8], at: AppliedPosition) -> Result<ApplyOutcome, StateError> {
        StateMachine::apply(self.current().map_err(state_error)?.as_ref(), batch, at)
    }
    fn apply_noop(&self, at: AppliedPosition) -> Result<ApplyOutcome, StateError> {
        StateMachine::apply_noop(self.current().map_err(state_error)?.as_ref(), at)
    }
}
