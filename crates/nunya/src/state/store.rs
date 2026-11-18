//! Exclusive durable storage with atomic opaque-byte transactions.
use super::schema::{FORMAT_VERSION, STORE_FILE, Table, root_check_value};
use crate::token::keys::RootKey;
use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};
use std::{
    fmt,
    path::Path,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

const META: TableDefinition<&[u8], &[u8]> = TableDefinition::new("meta");
const FORMAT: &[u8] = b"format_version";
const DEPLOYMENT: &[u8] = b"deployment_id";
const ROOT_CHECK: &[u8] = b"root_check_value";
const CREATED: &[u8] = b"created_unix_ms";

/// Injectable Unix millisecond clock.
pub type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

/// Return a clock using the system's Unix millisecond time.
pub fn system_clock() -> Clock {
    Arc::new(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
            .unwrap_or(0)
    })
}

/// Public deployment metadata without the root-key check value.
pub struct StoreMeta {
    /// On-disk state format version.
    pub format_version: u32,
    /// Random identifier retained for this deployment's lifetime.
    pub deployment_id: [u8; 16],
    /// Deployment creation time in Unix milliseconds.
    pub created_unix_ms: u64,
}
impl fmt::Debug for StoreMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoreMeta")
            .field("format_version", &self.format_version)
            .field(
                "deployment_id",
                &data_encoding::HEXLOWER.encode(&self.deployment_id),
            )
            .field("created_unix_ms", &self.created_unix_ms)
            .finish()
    }
}

/// Data-free errors safe for service diagnostics.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// Another service holds the exclusive state lock.
    #[error("state is owned by a running service")]
    Owned,
    /// The configured root key differs from the deployment's original key.
    #[error(
        "root key does not match this state store: replacing the root key is a continuity break; restore the original root key, or move the state directory aside to start a new deployment (all sessions and tokens of the old deployment are lost)"
    )]
    ContinuityBreak,
    /// The state format requires another build.
    #[error("state store format {found} is not supported by this build (expected 1)")]
    UnsupportedFormat {
        /// Version read from the state metadata.
        found: u32,
    },
    /// Required metadata is absent or invalid.
    #[error("state store metadata is missing or malformed")]
    Corrupt,
    /// An underlying storage operation failed.
    #[error("state store unavailable")]
    Io,
}

/// Ordered opaque key/value rows returned by a prefix scan.
pub type ScanRows = Vec<(Vec<u8>, Vec<u8>)>;

/// Snapshot reads of opaque state records.
pub trait ReadTxn {
    /// Read a single record by exact key.
    fn get(&self, t: Table, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError>;
    /// Read matching records in ascending byte-key order.
    fn scan_prefix(&self, t: Table, prefix: &[u8]) -> Result<ScanRows, StoreError>;
}
/// Atomic mutations, aborted when dropped without committing.
pub trait WriteTxn: ReadTxn {
    /// Insert or replace an opaque record.
    fn put(&mut self, t: Table, key: &[u8], value: &[u8]) -> Result<(), StoreError>;
    /// Delete a record, returning whether it existed.
    fn delete(&mut self, t: Table, key: &[u8]) -> Result<bool, StoreError>;
    /// Atomically persist all mutations in this transaction.
    fn commit(self: Box<Self>) -> Result<(), StoreError>;
}
/// Object-safe durable gateway state interface.
pub trait Store: Send + Sync {
    /// Return the deployment's public metadata.
    fn meta(&self) -> &StoreMeta;
    /// Read the injected Unix millisecond clock.
    fn now_ms(&self) -> u64;
    /// Begin a consistent read snapshot.
    fn read(&self) -> Result<Box<dyn ReadTxn + '_>, StoreError>;
    /// Begin an atomic write transaction.
    fn write(&self) -> Result<Box<dyn WriteTxn + '_>, StoreError>;
}

/// Service-lifetime owner of a redb state database.
pub struct RedbStore {
    db: redb::Database,
    meta: StoreMeta,
    clock: Clock,
    #[cfg(test)]
    fail_commits: AtomicBool,
}
impl fmt::Debug for RedbStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedbStore")
            .field("meta", &self.meta)
            .finish_non_exhaustive()
    }
}

fn fixed<const N: usize>(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    key: &[u8],
) -> Result<[u8; N], StoreError> {
    table
        .get(key)
        .map_err(|_| StoreError::Io)?
        .ok_or(StoreError::Corrupt)?
        .value()
        .try_into()
        .map_err(|_| StoreError::Corrupt)
}

impl RedbStore {
    /// Open exclusive state ownership using the system clock.
    pub fn open(state_dir: &Path, root: &RootKey) -> Result<Self, StoreError> {
        Self::with_clock(state_dir, root, system_clock())
    }

    /// Open exclusive state ownership using an injectable millisecond clock.
    pub fn with_clock(state_dir: &Path, root: &RootKey, clock: Clock) -> Result<Self, StoreError> {
        std::fs::create_dir_all(state_dir).map_err(|_| StoreError::Io)?;
        let db =
            redb::Database::create(state_dir.join(STORE_FILE)).map_err(|error| match error {
                redb::DatabaseError::DatabaseAlreadyOpen => StoreError::Owned,
                _ => StoreError::Io,
            })?;
        let txn = db.begin_write().map_err(|_| StoreError::Io)?;
        let meta;
        let initialize;
        {
            let mut table = txn.open_table(META).map_err(|_| StoreError::Io)?;
            initialize = table.is_empty().map_err(|_| StoreError::Io)?;
            if initialize {
                let mut deployment_id = [0; 16];
                getrandom::fill(&mut deployment_id).map_err(|_| StoreError::Io)?;
                meta = StoreMeta {
                    format_version: FORMAT_VERSION,
                    deployment_id,
                    created_unix_ms: clock(),
                };
                table
                    .insert(FORMAT, FORMAT_VERSION.to_be_bytes().as_slice())
                    .map_err(|_| StoreError::Io)?;
                table
                    .insert(DEPLOYMENT, deployment_id.as_slice())
                    .map_err(|_| StoreError::Io)?;
                table
                    .insert(ROOT_CHECK, root_check_value(root).as_slice())
                    .map_err(|_| StoreError::Io)?;
                table
                    .insert(CREATED, meta.created_unix_ms.to_be_bytes().as_slice())
                    .map_err(|_| StoreError::Io)?;
            } else {
                let format_version = u32::from_be_bytes(fixed(&table, FORMAT)?);
                if format_version != FORMAT_VERSION {
                    return Err(StoreError::UnsupportedFormat {
                        found: format_version,
                    });
                }
                meta = StoreMeta {
                    format_version,
                    deployment_id: fixed(&table, DEPLOYMENT)?,
                    created_unix_ms: u64::from_be_bytes(fixed(&table, CREATED)?),
                };
                let check = fixed::<32>(&table, ROOT_CHECK)?;
                let expected = root_check_value(root);
                // Visit every byte without a data-dependent early exit.
                if check
                    .iter()
                    .zip(expected)
                    .fold(0u8, |diff, (a, b)| diff | (a ^ b))
                    != 0
                {
                    return Err(StoreError::ContinuityBreak);
                }
            }
        }
        for t in Table::ALL {
            txn.open_table(t.definition()).map_err(|_| StoreError::Io)?;
        }
        txn.commit().map_err(|_| StoreError::Io)?;
        Ok(Self {
            db,
            meta,
            clock,
            #[cfg(test)]
            fail_commits: AtomicBool::new(false),
        })
    }

    #[cfg(test)]
    pub(crate) fn fail_commits(&self, on: bool) {
        self.fail_commits.store(on, Ordering::SeqCst);
    }
}

impl Store for RedbStore {
    fn meta(&self) -> &StoreMeta {
        &self.meta
    }
    fn now_ms(&self) -> u64 {
        (self.clock)()
    }
    fn read(&self) -> Result<Box<dyn ReadTxn + '_>, StoreError> {
        Ok(Box::new(RedbRead(
            self.db.begin_read().map_err(|_| StoreError::Io)?,
        )))
    }
    fn write(&self) -> Result<Box<dyn WriteTxn + '_>, StoreError> {
        Ok(Box::new(RedbWrite {
            txn: self.db.begin_write().map_err(|_| StoreError::Io)?,
            #[cfg(test)]
            fail_commits: &self.fail_commits,
            _owner: std::marker::PhantomData,
        }))
    }
}

struct RedbRead(redb::ReadTransaction);
struct RedbWrite<'a> {
    txn: redb::WriteTransaction,
    #[cfg(test)]
    fail_commits: &'a AtomicBool,
    _owner: std::marker::PhantomData<&'a RedbStore>,
}

fn scan(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    prefix: &[u8],
) -> Result<ScanRows, StoreError> {
    let mut rows = Vec::new();
    for row in table.range(prefix..).map_err(|_| StoreError::Io)? {
        let (key, value) = row.map_err(|_| StoreError::Io)?;
        if !key.value().starts_with(prefix) {
            break;
        }
        rows.push((key.value().to_vec(), value.value().to_vec()));
    }
    Ok(rows)
}
impl ReadTxn for RedbRead {
    fn get(&self, t: Table, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        let table = self
            .0
            .open_table(t.definition())
            .map_err(|_| StoreError::Io)?;
        Ok(table
            .get(key)
            .map_err(|_| StoreError::Io)?
            .map(|value| value.value().to_vec()))
    }
    fn scan_prefix(&self, t: Table, prefix: &[u8]) -> Result<ScanRows, StoreError> {
        scan(
            &self
                .0
                .open_table(t.definition())
                .map_err(|_| StoreError::Io)?,
            prefix,
        )
    }
}
impl ReadTxn for RedbWrite<'_> {
    fn get(&self, t: Table, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        let table = self
            .txn
            .open_table(t.definition())
            .map_err(|_| StoreError::Io)?;
        Ok(table
            .get(key)
            .map_err(|_| StoreError::Io)?
            .map(|value| value.value().to_vec()))
    }
    fn scan_prefix(&self, t: Table, prefix: &[u8]) -> Result<ScanRows, StoreError> {
        scan(
            &self
                .txn
                .open_table(t.definition())
                .map_err(|_| StoreError::Io)?,
            prefix,
        )
    }
}
impl WriteTxn for RedbWrite<'_> {
    fn put(&mut self, t: Table, key: &[u8], value: &[u8]) -> Result<(), StoreError> {
        self.txn
            .open_table(t.definition())
            .map_err(|_| StoreError::Io)?
            .insert(key, value)
            .map_err(|_| StoreError::Io)?;
        Ok(())
    }
    fn delete(&mut self, t: Table, key: &[u8]) -> Result<bool, StoreError> {
        Ok(self
            .txn
            .open_table(t.definition())
            .map_err(|_| StoreError::Io)?
            .remove(key)
            .map_err(|_| StoreError::Io)?
            .is_some())
    }
    fn commit(self: Box<Self>) -> Result<(), StoreError> {
        #[cfg(test)]
        if self.fail_commits.load(Ordering::SeqCst) {
            return Err(StoreError::Io);
        }
        self.txn.commit().map_err(|_| StoreError::Io)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    struct TestDir(PathBuf);
    impl TestDir {
        fn new() -> Self {
            let mut id = [0; 16];
            getrandom::fill(&mut id).unwrap();
            let path = PathBuf::from(std::env::var_os("TEMP").expect("TEMP must be set")).join(
                format!("nunya-state-{}", data_encoding::HEXLOWER.encode(&id)),
            );
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for TestDir {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }
    fn root() -> RootKey {
        RootKey::from_hex(&"01".repeat(32)).unwrap()
    }

    #[test]
    fn create_reopen_and_record_round_trip() {
        let dir = TestDir::new();
        let deployment;
        {
            let store = RedbStore::with_clock(&dir.0, &root(), Arc::new(|| 1234)).unwrap();
            assert_eq!(store.meta().format_version, 1);
            assert_eq!(store.meta().created_unix_ms, 1234);
            assert_eq!(store.now_ms(), 1234);
            deployment = store.meta().deployment_id;
            assert!(
                format!("{:?}", store.meta())
                    .contains(&data_encoding::HEXLOWER.encode(&deployment))
            );
            let mut write = store.write().unwrap();
            for t in Table::ALL {
                write.put(t, b"ab2", b"second").unwrap();
                write.put(t, b"ab1", b"first").unwrap();
                write.put(t, b"ac", b"other").unwrap();
                assert_eq!(write.get(t, b"ab1").unwrap(), Some(b"first".to_vec()));
            }
            write.commit().unwrap();
        }
        {
            let store = RedbStore::with_clock(&dir.0, &root(), Arc::new(|| 5678)).unwrap();
            assert_eq!(store.meta().deployment_id, deployment);
            assert_eq!(store.meta().created_unix_ms, 1234);
            assert_eq!(store.now_ms(), 5678);
            let read = store.read().unwrap();
            for t in Table::ALL {
                assert_eq!(read.get(t, b"ab1").unwrap(), Some(b"first".to_vec()));
                assert_eq!(
                    read.scan_prefix(t, b"ab").unwrap(),
                    vec![
                        (b"ab1".to_vec(), b"first".to_vec()),
                        (b"ab2".to_vec(), b"second".to_vec()),
                    ]
                );
                assert_eq!(read.scan_prefix(t, b"").unwrap().len(), 3);
                assert!(read.scan_prefix(t, b"z").unwrap().is_empty());
            }
            let mut write = store.write().unwrap();
            assert!(write.delete(Table::Sessions, b"ab1").unwrap());
            assert!(!write.delete(Table::Sessions, b"missing").unwrap());
            write.commit().unwrap();
            assert!(read.get(Table::Sessions, b"ab1").unwrap().is_some());
        }
        let store = RedbStore::open(&dir.0, &root()).unwrap();
        assert_eq!(
            store.read().unwrap().get(Table::Sessions, b"ab1").unwrap(),
            None
        );
    }

    #[test]
    fn exclusive_ownership_is_released_on_drop() {
        let dir = TestDir::new();
        let store = RedbStore::open(&dir.0, &root()).unwrap();
        let error = RedbStore::open(&dir.0, &root()).unwrap_err();
        assert!(matches!(error, StoreError::Owned));
        assert_eq!(error.to_string(), "state is owned by a running service");
        drop(store);
        RedbStore::open(&dir.0, &root()).unwrap();
    }

    #[test]
    fn wrong_root_is_a_continuity_break() {
        let dir = TestDir::new();
        drop(RedbStore::open(&dir.0, &root()).unwrap());
        let wrong = RootKey::from_hex(&"02".repeat(32)).unwrap();
        let error = RedbStore::open(&dir.0, &wrong).unwrap_err();
        assert!(matches!(error, StoreError::ContinuityBreak));
        assert!(error.to_string().contains("continuity break"));
        RedbStore::open(&dir.0, &root()).unwrap();
    }

    fn edit_meta(dir: &TestDir, edit: impl FnOnce(&mut redb::Table<&[u8], &[u8]>)) {
        let db = redb::Database::create(dir.0.join(STORE_FILE)).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(META).unwrap();
            edit(&mut table);
        }
        txn.commit().unwrap();
    }

    #[test]
    fn unknown_format_is_checked_before_root_or_other_metadata() {
        let dir = TestDir::new();
        drop(RedbStore::open(&dir.0, &root()).unwrap());
        edit_meta(&dir, |table| {
            table.insert(FORMAT, 2u32.to_be_bytes().as_slice()).unwrap();
            table.remove(ROOT_CHECK).unwrap();
        });
        let wrong = RootKey::from_hex(&"02".repeat(32)).unwrap();
        assert!(matches!(
            RedbStore::open(&dir.0, &wrong),
            Err(StoreError::UnsupportedFormat { found: 2 })
        ));
    }

    #[test]
    fn missing_and_malformed_metadata_fail_closed() {
        for key in [FORMAT, DEPLOYMENT, ROOT_CHECK, CREATED] {
            for malformed in [false, true] {
                let dir = TestDir::new();
                drop(RedbStore::open(&dir.0, &root()).unwrap());
                edit_meta(&dir, |table| {
                    if malformed {
                        table.insert(key, b"x".as_slice()).unwrap();
                    } else {
                        table.remove(key).unwrap();
                    }
                });
                assert!(matches!(
                    RedbStore::open(&dir.0, &root()),
                    Err(StoreError::Corrupt)
                ));
            }
        }
    }

    #[test]
    fn dropped_and_failed_transactions_abort_every_mutation() {
        let dir = TestDir::new();
        let store = RedbStore::open(&dir.0, &root()).unwrap();
        {
            let mut write = store.write().unwrap();
            write.put(Table::Sessions, b"aborted", b"value").unwrap();
        }
        assert_eq!(
            store
                .read()
                .unwrap()
                .get(Table::Sessions, b"aborted")
                .unwrap(),
            None
        );
        store.fail_commits(true);
        let mut write = store.write().unwrap();
        write.put(Table::Sessions, b"failed", b"value").unwrap();
        assert!(matches!(write.commit(), Err(StoreError::Io)));
        assert_eq!(
            store
                .read()
                .unwrap()
                .get(Table::Sessions, b"failed")
                .unwrap(),
            None
        );
        store.fail_commits(false);
        let mut write = store.write().unwrap();
        write.put(Table::Sessions, b"committed", b"value").unwrap();
        write.commit().unwrap();
        drop(store);
        let store = RedbStore::open(&dir.0, &root()).unwrap();
        let read = store.read().unwrap();
        assert_eq!(read.get(Table::Sessions, b"aborted").unwrap(), None);
        assert_eq!(read.get(Table::Sessions, b"failed").unwrap(), None);
        assert_eq!(
            read.get(Table::Sessions, b"committed").unwrap(),
            Some(b"value".to_vec())
        );
    }
}
