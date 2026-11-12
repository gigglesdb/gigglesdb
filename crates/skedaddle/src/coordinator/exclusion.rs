//! Non-expiring migration exclusion. All contenders must use the same backend
//! and namespace. File locks require a shared, trusted local filesystem directory;
//! PostgreSQL locks require the same source database. Never remove lock files.

use std::{
    fs::{File, OpenOptions, TryLockError},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

#[derive(Debug)]
pub enum Error {
    NotConfigured,
    NotOwner,
    WrongGuard,
    Busy,
    Lost,
    Io(std::io::Error),
    Connection,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::NotConfigured => "migration exclusion is not configured",
            Self::NotOwner => "migration exclusion requires current ownership",
            Self::WrongGuard => "exclusion guard belongs to another migration or backend",
            Self::Busy => "migration exclusion is held by another operation",
            Self::Lost => "migration exclusion may have been lost; stop effects",
            Self::Io(_) => "migration exclusion filesystem operation failed",
            Self::Connection => "migration exclusion connection failed (details redacted)",
        })
    }
}
impl std::error::Error for Error {}
impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}
pub type Result<T> = std::result::Result<T, Error>;

pub struct MigrationExclusion {
    backend: Backend,
}
enum Backend {
    File(PathBuf),
    Postgres(Box<(crate::pg::tls::Config, crate::security::TlsPolicy)>),
}
impl MigrationExclusion {
    pub fn file(directory: impl AsRef<Path>) -> Result<Arc<Self>> {
        std::fs::create_dir_all(directory.as_ref())?;
        Ok(Arc::new(Self {
            backend: Backend::File(std::fs::canonicalize(directory)?),
        }))
    }
    pub fn postgres(
        config: crate::pg::tls::Config,
        policy: crate::security::TlsPolicy,
    ) -> Arc<Self> {
        Arc::new(Self {
            backend: Backend::Postgres(Box::new((config, policy))),
        })
    }
    /// Wait without a lease or expiry. Cancellation drops the pending session
    /// or file handle; no detached blocking waiter can subsequently take a lock.
    pub async fn acquire(self: &Arc<Self>, migration: &str) -> Result<ExclusionGuard> {
        if let Backend::Postgres(config) = &self.backend {
            return self.pg_guard(migration, config, true).await;
        }
        loop {
            match self.try_acquire(migration).await {
                Err(Error::Busy) => tokio::time::sleep(Duration::from_millis(10)).await,
                result => return result,
            }
        }
    }
    /// Never waits for another lock holder (connection establishment may wait).
    pub async fn try_acquire(self: &Arc<Self>, migration: &str) -> Result<ExclusionGuard> {
        let backend = match &self.backend {
            Backend::File(directory) => {
                // Lowercase hex is injective even on case-insensitive Windows
                // volumes (case-sensitive base64 filenames are not).
                let encoded: String = migration
                    .as_bytes()
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect();
                let path = directory.join(format!("migration-{encoded}.lock"));
                let mut options = OpenOptions::new();
                options.read(true).write(true).create(true).truncate(false);
                // Keep the locked file's name bound to this handle on Windows.
                #[cfg(windows)]
                {
                    use std::os::windows::fs::OpenOptionsExt;
                    options.share_mode(3); // FILE_SHARE_READ | FILE_SHARE_WRITE, no DELETE
                }
                let file = options.open(&path)?;
                match file.try_lock() {
                    Ok(()) => GuardBackend::File { file, path },
                    Err(TryLockError::WouldBlock) => return Err(Error::Busy),
                    Err(TryLockError::Error(error)) => return Err(error.into()),
                }
            }
            Backend::Postgres(config) => return self.pg_guard(migration, config, false).await,
        };
        let guard = ExclusionGuard {
            exclusion: self.clone(),
            migration: migration.into(),
            owner: None,
            backend,
        };
        guard.check_alive().await?;
        Ok(guard)
    }
    async fn pg_guard(
        self: &Arc<Self>,
        migration: &str,
        config: &(crate::pg::tls::Config, crate::security::TlsPolicy),
        wait: bool,
    ) -> Result<ExclusionGuard> {
        let session =
            crate::pg::exclusion::Session::acquire(&config.0, &config.1, migration, wait).await?;
        Ok(ExclusionGuard {
            exclusion: self.clone(),
            migration: migration.into(),
            owner: None,
            backend: GuardBackend::Postgres(session),
        })
    }
}

/// Uncloneable, owned capability. Drop closes the sole locked file/session,
/// including during error propagation and unwinding. No raw handles are exposed.
pub struct ExclusionGuard {
    exclusion: Arc<MigrationExclusion>,
    migration: String,
    owner: Option<(String, u64)>,
    backend: GuardBackend,
}
enum GuardBackend {
    File { file: File, path: PathBuf },
    Postgres(crate::pg::exclusion::Session),
}
impl ExclusionGuard {
    pub fn migration(&self) -> &str {
        &self.migration
    }
    /// Captured only by Coordinator::exclusive after refreshing under the lock.
    pub fn owner(&self) -> Option<(&str, u64)> {
        self.owner
            .as_ref()
            .map(|(owner, epoch)| (owner.as_str(), *epoch))
    }
    pub(super) fn bind_owner(&mut self, owner: &str, epoch: u64) {
        self.owner = Some((owner.into(), epoch));
    }
    pub(super) fn validate(
        &self,
        exclusion: &Arc<MigrationExclusion>,
        migration: &str,
    ) -> Result<()> {
        if !Arc::ptr_eq(&self.exclusion, exclusion) || self.migration != migration {
            return Err(Error::WrongGuard);
        }
        Ok(())
    }
    /// A round trip is evidence only at this instant. PG session loss can occur
    /// immediately afterward: effects still require the transactional epoch row.
    pub async fn check_alive(&self) -> Result<()> {
        match &self.backend {
            GuardBackend::File { file, path } => {
                let held = file.metadata().map_err(|_| Error::Lost)?;
                let named = std::fs::metadata(path).map_err(|_| Error::Lost)?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::MetadataExt;
                    if held.nlink() == 0 || held.dev() != named.dev() || held.ino() != named.ino() {
                        return Err(Error::Lost);
                    }
                }
                if !held.is_file() || !named.is_file() {
                    return Err(Error::Lost);
                }
                Ok(())
            }
            GuardBackend::Postgres(session) => session.check_alive(&self.migration).await,
        }
    }
    pub fn backend_pid(&self) -> Option<i32> {
        match &self.backend {
            GuardBackend::Postgres(session) => Some(session.pid()),
            _ => None,
        }
    }
}
