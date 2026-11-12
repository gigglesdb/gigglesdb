//! Generation-scoped local control, separate from the authoritative journal.
use super::{Result, recovery};
use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

/// A control directory below the migration's state root. Use the same directory
/// for the running engine and a separate CLI process. No coordinator schema or
/// observer journal writes are needed; the active owner journals Paused itself.
#[derive(Clone)]
pub struct PauseControl {
    root: PathBuf,
}
impl PauseControl {
    pub fn new(root: impl AsRef<Path>) -> Result<Self> {
        std::fs::create_dir_all(root.as_ref())?;
        Ok(Self {
            root: root.as_ref().to_owned(),
        })
    }
    fn open(&self, name: &str) -> Result<File> {
        Ok(OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.root.join(name))?)
    }
    /// Durably request a safe pause of the current running generation. Returns
    /// false when there is no active owner, including after a process crash.
    /// Requests never change lifecycle directly or affect a later generation.
    pub fn request_pause(&self) -> Result<bool> {
        let gate = self.open("control.lock")?;
        gate.lock()?;
        let owner = self.open("owner.lock")?;
        match owner.try_lock() {
            Ok(()) => Ok(false),
            Err(std::fs::TryLockError::WouldBlock) => {
                let token = std::fs::read_to_string(self.root.join("owner-generation"))?;
                let token = uuid::Uuid::parse_str(&token).map_err(recovery)?;
                File::create(self.root.join(format!("pause-{token}")))?.sync_all()?;
                Ok(true)
            }
            Err(std::fs::TryLockError::Error(error)) => Err(error.into()),
        }
    }
    pub(super) fn activate(&self) -> Result<Owner> {
        let gate = self.open("control.lock")?;
        gate.lock()?;
        let lock = self.open("owner.lock")?;
        lock.try_lock().map_err(recovery)?;
        let token = uuid::Uuid::new_v4();
        let mut generation = File::create(self.root.join("owner-generation"))?;
        write!(generation, "{token}")?;
        generation.sync_all()?;
        Ok(Owner {
            token,
            control: self.clone(),
            request: self.root.join(format!("pause-{token}")),
            lock: Some(lock),
        })
    }
}
pub(super) struct Owner {
    token: uuid::Uuid,
    control: PauseControl,
    request: PathBuf,
    lock: Option<File>,
}
impl Owner {
    pub(super) fn generation(&self) -> uuid::Uuid {
        self.token
    }
    pub(super) fn requested(&self) -> Result<bool> {
        Ok(self.request.try_exists()?)
    }
    /// Freeze the last accepted request while still holding the owner lock.
    /// A later request observes no owner and cannot alter this stop decision.
    pub(super) fn retire(&mut self) -> Result<bool> {
        if self.lock.is_none() {
            return Ok(false);
        }
        let gate = self.control.open("control.lock")?;
        gate.lock()?;
        let requested = self.requested()?;
        for path in [&self.request, &self.control.root.join("owner-generation")] {
            match std::fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        drop(self.lock.take());
        Ok(requested)
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        let _ = self.retire();
    }
}
