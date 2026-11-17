use super::{Admin, Backend, CoreError, Limits};
use std::path::Path;

/// Public fence metadata; holder credentials and their digest are never exposed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteFenceStatus {
    pub fenced: bool,
    pub epoch: u64,
    pub label: String,
    pub changed_at: u64,
}
impl Admin {
    /// Open an existing database with the write-fence holder credential.
    pub fn open_as_fence_holder(
        path: impl AsRef<Path>,
        limits: Limits,
        token: &[u8],
    ) -> Result<Self, CoreError> {
        Self::open_with_holder_token(path.as_ref(), limits, Some(token))
    }
    /// Install or advance the durable write fence. Labels are at most 256 UTF-8 bytes.
    pub fn install_write_fence(&self, token: &[u8], label: &str) -> Result<u64, CoreError> {
        let Backend::V2(db) = &self.inner.backend else {
            return Err(CoreError::Unsupported);
        };
        if db.task_kernel_unsupported() {
            return Err(CoreError::Unsupported);
        }
        db.sequencer
            .install_write_fence(token, label)
            .map_err(Into::into)
    }
    /// Release only the matching holder's current fence epoch.
    pub fn release_write_fence(&self, token: &[u8], epoch: u64) -> Result<(), CoreError> {
        let Backend::V2(db) = &self.inner.backend else {
            return Err(CoreError::Unsupported);
        };
        db.sequencer
            .release_write_fence(token, epoch)
            .map_err(Into::into)
    }
    pub fn write_fence_status(&self) -> Result<Option<WriteFenceStatus>, CoreError> {
        let Backend::V2(db) = &self.inner.backend else {
            return Ok(None);
        };
        Ok(db
            .sequencer
            .write_fence_status()?
            .map(|r| WriteFenceStatus {
                fenced: r.state == super::engine::write_fence::FenceState::Fenced,
                epoch: r.epoch,
                label: r.label,
                changed_at: r.changed_at,
            }))
    }
}

#[cfg(all(test, feature = "engine-rocksdb"))]
mod tests {
    use super::*;
    use crate::{Admin, Engine};
    #[test]
    #[cfg(feature = "engine-rocksdb")]
    fn write_fence_public_reopen() {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path = std::env::temp_dir().join(format!("fence-{:032x}", u128::from_be_bytes(nonce)));
        let db = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        db.sql_session()
            .execute("CREATE TABLE fenced_rows(id BIGINT PRIMARY KEY)", &[], None)
            .unwrap();
        assert_eq!(db.install_write_fence(b"holder", "public").unwrap(), 1);
        db.sql_session()
            .execute("INSERT INTO fenced_rows VALUES (1)", &[], None)
            .unwrap();
        drop(db);
        let db = Admin::open(&path, Limits::default()).unwrap();
        let err = db
            .sql_session()
            .execute("INSERT INTO fenced_rows VALUES (2)", &[], None)
            .unwrap_err();
        assert_eq!(err, CoreError::WriteFenced { epoch: 1 });
        assert_eq!(crate::sqlstate(&err), "25006");
        assert_eq!(err.to_string(), "database is write-fenced (epoch 1)");
        db.sql_session()
            .execute("SELECT * FROM fenced_rows", &[], None)
            .unwrap();
        assert!(db.write_fence_status().unwrap().unwrap().fenced);
        drop(db);
        let db = Admin::open_as_fence_holder(&path, Limits::default(), b"holder").unwrap();
        db.sql_session()
            .execute("INSERT INTO fenced_rows VALUES (2)", &[], None)
            .unwrap();
        db.release_write_fence(b"holder", 1).unwrap();
        assert_eq!(db.install_write_fence(b"next", "next").unwrap(), 2);
        drop(db);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[cfg(test)]
mod legacy_tests {
    use super::*;
    #[test]
    fn write_fence_legacy_unsupported() {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path =
            std::env::temp_dir().join(format!("fence-legacy-{:032x}", u128::from_be_bytes(nonce)));
        let db = Admin::create(&path, Limits::default()).unwrap();
        assert_eq!(
            db.install_write_fence(b"holder", "legacy"),
            Err(CoreError::Unsupported)
        );
        assert_eq!(
            db.release_write_fence(b"holder", 1),
            Err(CoreError::Unsupported)
        );
        assert_eq!(db.write_fence_status().unwrap(), None);
        drop(db);
        std::fs::remove_dir_all(path).unwrap();
    }
}
