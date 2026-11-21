//! Optional RocksDB implementation of the storage-engine KV seam.
use super::kv::*;
use rocksdb::{
    AsColumnFamilyRef, AsRawPtr, BlockBasedOptions, Cache, DB, DBCompressionType, DBRecoveryMode,
    IngestExternalFileOptions, IteratorMode, Options, SstFileWriter, WriteOptions,
};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions, TryLockError},
    io::{Read, Seek, SeekFrom, Write},
    ops::{Bound, Deref},
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex, mpsc},
    thread::{self, JoinHandle},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

pub(crate) const WRITER_LOCK_FILE: &str = "giggles.lock";

/// Never unlink this file: stale contents are harmless, but replacing its inode
/// would let a new opener lock a different file while the old writer is alive.
pub(crate) struct WriterLock {
    _file: File,
}

impl WriterLock {
    pub(crate) fn acquire(path: &Path) -> Result<Self, KvError> {
        let io_error = |error: std::io::Error| KvError::Io(error.to_string());
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        // Permit other openers to reach try_lock, but prevent Windows deletion
        // or replacement of the lock file for the entire writer lifetime.
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            options.share_mode(3); // FILE_SHARE_READ | FILE_SHARE_WRITE
        }
        let mut file = options
            .open(path.join(WRITER_LOCK_FILE))
            .map_err(io_error)?;
        match file.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                return Err(KvError::Io(
                    "database already open by another writer".into(),
                ));
            }
            Err(TryLockError::Error(error)) => return Err(io_error(error)),
        }
        // Only the OS lock decides ownership. Never trust a stale pid (which can
        // be reused), timestamp or nonce to decide whether a writer is alive.
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).map_err(|error| KvError::Io(error.to_string()))?;
        let started = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| KvError::Io(error.to_string()))?
            .as_nanos();
        let identity = format!(
            "pid={}\nstarted_unix_nanos={started}\nnonce={:032x}\n",
            std::process::id(),
            u128::from_be_bytes(nonce)
        );
        file.set_len(0).map_err(io_error)?;
        file.write_all(identity.as_bytes()).map_err(io_error)?;
        file.sync_all().map_err(io_error)?;
        file.seek(SeekFrom::Start(0)).map_err(io_error)?;
        let mut saved = String::new();
        file.read_to_string(&mut saved).map_err(io_error)?;
        if saved != identity {
            return Err(KvError::Io("writer lock identity changed".into()));
        }
        Ok(Self { _file: file })
    }
}

// At most two idle iterators per CF. Checked-out iterators are exclusively owned.
const POINT_POOL_CAP: usize = 2;
const POINT_POOL_IDLE: Duration = Duration::from_secs(1);
#[derive(Default)]
struct PointPool {
    generation: u64,
    #[cfg(test)]
    idle_timeout: Option<Duration>,
    #[cfg(test)]
    created: usize,
    #[cfg(test)]
    refreshed_reuses: usize,
    idle: BTreeMap<Keyspace, Vec<(Instant, rocksdb::DBRawIterator<'static>)>>,
}

#[derive(Default)]
struct CloseState {
    closed: Mutex<bool>,
    wake: Condvar,
}

#[derive(Clone, Default)]
pub(crate) struct CloseLatch(Arc<CloseState>);

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct StillHeld;

#[allow(dead_code, reason = "Used by the later recovery supervisor")]
impl CloseLatch {
    pub(crate) fn wait(&self, timeout: Duration) -> Result<(), StillHeld> {
        let closed = self.0.closed.lock().unwrap_or_else(|p| p.into_inner());
        let (closed, _) = self.0.wake.wait_timeout_while(closed, timeout, |closed| !*closed)
            .unwrap_or_else(|p| p.into_inner());
        if *closed { Ok(()) } else { Err(StillHeld) }
    }
}

/// Declared after the native DB and writer lock so neither can outlive the signal.
struct SignalClose(CloseLatch);
impl Drop for SignalClose {
    fn drop(&mut self) {
        let CloseLatch(state) = &self.0;
        *state.closed.lock().unwrap_or_else(|p| p.into_inner()) = true;
        state.wake.notify_all();
    }
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecoveryOpenCrash {
    BeforeNativeOpen,
    DuringRecoveryFlush,
    AfterNativeOpen,
}

#[cfg(test)]
pub(crate) const RECOVERY_FLUSH_MARKER: &str = "recovery-flush-fired";
#[cfg(test)]
pub(crate) const RECOVERY_FLUSH_MISSED: i32 = 88;

#[cfg(test)]
thread_local! {
    pub(crate) static RECOVERY_OPEN_CRASH: std::cell::Cell<Option<RecoveryOpenCrash>> = const { std::cell::Cell::new(None) };
    pub(crate) static WAL_ONLY_SHUTDOWN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

struct LockedDb {
    // Must precede db: all lifetime-extended iterators die before their DB.
    points: Mutex<PointPool>,
    // Field order matters: close RocksDB before releasing the explicit lock.
    db: DB,
    _writer_lock: WriterLock,
    close: SignalClose,
}

impl LockedDb {
    fn clear_points(&self) {
        let mut pool = self.points.lock().unwrap_or_else(|e| e.into_inner());
        pool.generation = pool.generation.wrapping_add(1);
        pool.idle.clear();
    }
}

impl Deref for LockedDb {
    type Target = DB;
    fn deref(&self) -> &DB {
        &self.db
    }
}

pub(crate) struct RocksKv {
    db: Arc<LockedDb>,
}

impl Drop for RocksKv {
    fn drop(&mut self) {
        // Owned snapshots may keep LockedDb alive after this live handle dies.
        self.db.clear_points();
    }
}

impl RocksKv {
    pub(crate) fn open(path: &Path, cache_bytes: usize) -> Result<Self, KvError> {
        Self::open_in_mode(path, cache_bytes, DBRecoveryMode::PointInTime)
    }

    pub(crate) fn open_recovery(path: &Path, cache_bytes: usize) -> Result<Self, KvError> {
        Self::open_in_mode(path, cache_bytes, DBRecoveryMode::TolerateCorruptedTailRecords)
    }

    fn open_in_mode(path: &Path, cache_bytes: usize, mode: DBRecoveryMode) -> Result<Self, KvError> {
        std::fs::create_dir_all(path).map_err(|error| KvError::Io(error.to_string()))?;
        Self::open_locked_in_mode(path, cache_bytes, WriterLock::acquire(path)?, mode)
    }

    pub(crate) fn close_latch(&self) -> CloseLatch {
        self.db.close.0.clone()
    }

    /// Capture an atomic, reopenable view of every column family.
    pub(crate) fn checkpoint(&self, target: &Path) -> Result<Vec<(String, u64)>, KvError> {
        self.db.clear_points();
        let io_error = |error: std::io::Error| KvError::Io(error.to_string());
        match std::fs::symlink_metadata(target) {
            Ok(_) => return Err(KvError::Io("checkpoint target already exists".into())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_error(error)),
        }
        rocksdb::checkpoint::Checkpoint::new(&self.db.db)
            .map_err(map_error)?
            .create_checkpoint(native_path(target))
            .map_err(map_error)?;
        let mut files = Vec::new();
        for entry in std::fs::read_dir(target).map_err(io_error)? {
            let entry = entry.map_err(io_error)?;
            if entry.file_type().map_err(io_error)?.is_file() {
                let name = entry
                    .file_name()
                    .into_string()
                    .map_err(|_| KvError::Io("checkpoint file name is not valid UTF-8".into()))?;
                files.push((name, entry.metadata().map_err(io_error)?.len()));
            }
        }
        files.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(files)
    }

    pub(crate) fn open_locked(
        path: &Path,
        cache_bytes: usize,
        writer_lock: WriterLock,
    ) -> Result<Self, KvError> {
        Self::open_locked_in_mode(path, cache_bytes, writer_lock, DBRecoveryMode::PointInTime)
    }

    fn open_locked_in_mode(
        path: &Path,
        cache_bytes: usize,
        writer_lock: WriterLock,
        mode: DBRecoveryMode,
    ) -> Result<Self, KvError> {
        let cache = Cache::new_lru_cache(cache_bytes);
        let mut table = BlockBasedOptions::default();
        table.set_block_cache(&cache);
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        options.set_compression_type(DBCompressionType::Lz4);
        options.set_block_based_table_factory(&table);
        // Fence recovery design note section 6.3. Normal startup keeps point-in-time replay.
        options.set_recycle_log_file_num(0);
        options.set_wal_recovery_mode(mode);
        options.set_manual_wal_flush(false);
        // SAFETY: the options handle is live and uniquely borrowed here.
        unsafe { librocksdb_sys::rocksdb_options_set_avoid_flush_during_recovery(options.as_raw_ptr(), 0) };
        #[cfg(test)]
        if WAL_ONLY_SHUTDOWN.get() {
            // SAFETY: the options handle is live and uniquely borrowed here.
            unsafe { librocksdb_sys::rocksdb_options_set_avoid_flush_during_shutdown(options.as_raw_ptr(), 1) };
        }
        #[cfg(test)]
        if RECOVERY_OPEN_CRASH.get() == Some(RecoveryOpenCrash::BeforeNativeOpen) {
            crate::storage::crash_now();
        }
        let open_native = || {
            DB::open_cf_with_opts(
                &options,
                native_path(path),
                Keyspace::ALL.map(|ks| (ks.name(), options.clone())),
            )
        };
        #[cfg(test)]
        let result = if RECOVERY_OPEN_CRASH.get() == Some(RecoveryOpenCrash::DuringRecoveryFlush) {
            crash_during_recovery_flush(path, open_native)
        } else {
            open_native()
        };
        #[cfg(not(test))]
        let result = open_native();
        let db = result.map_err(|error| map_error_at(error, FenceSite::Open))?;
        #[cfg(test)]
        if RECOVERY_OPEN_CRASH.get() == Some(RecoveryOpenCrash::AfterNativeOpen) {
            crate::storage::crash_now();
        }
        Ok(Self {
            db: Arc::new(LockedDb {
                points: Mutex::new(PointPool::default()),
                db,
                _writer_lock: writer_lock,
                close: SignalClose(CloseLatch::default()),
            }),
        })
    }
}

#[cfg(test)]
fn crash_during_recovery_flush<T>(path: &Path, open_native: impl FnOnce() -> T) -> T {
    use std::sync::{
        Barrier,
        atomic::{AtomicBool, Ordering},
    };

    fn ssts(path: &Path) -> impl Iterator<Item = PathBuf> {
        std::fs::read_dir(path)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|file| file.extension().is_some_and(|ext| ext == "sst"))
    }

    let existing: Vec<_> = ssts(path).collect();
    let mut marker = File::create(path.join(RECOVERY_FLUSH_MARKER)).unwrap();
    let finished = AtomicBool::new(false);
    let ready = Barrier::new(2);
    thread::scope(|scope| {
        scope.spawn(|| {
            ready.wait();
            while !finished.load(Ordering::SeqCst) {
                if ssts(path).any(|path| !existing.contains(&path)) {
                    marker.write_all(b"new SST during native open").unwrap();
                    if !finished.load(Ordering::SeqCst) {
                        std::process::abort();
                    }
                    break;
                }
                thread::yield_now();
            }
        });
        ready.wait();
        let result = open_native();
        finished.store(true, Ordering::SeqCst);
        result
    })
}

/// One exported SST part. `path` is `None` when the interval was empty at the
/// snapshot (RocksDB cannot write an SST with no entries).
#[cfg_attr(not(test), allow(dead_code, reason = "R5-04 export.rs consumes this"))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SstFile {
    pub(crate) ks: Keyspace,
    pub(crate) path: Option<PathBuf>,
    pub(crate) bytes: u64,
    pub(crate) keys: u64,
}

/// Files of one export, in `parts` order, plus what the caller read at the
/// same RocksDB snapshot (for example the range's applied position).
#[cfg_attr(not(test), allow(dead_code, reason = "R5-04 export.rs consumes this"))]
#[derive(Debug)]
pub(crate) struct SstExport<T> {
    pub(crate) files: Vec<SstFile>,
    pub(crate) at_snapshot: T,
}

/// `(keyspace, start, end)` interval to export.
pub(crate) type SstPart = (Keyspace, OwnedBound, OwnedBound);
/// `(keyspace, start, end, file)` interval to replace; `None` empties it.
pub(crate) type SstInstall = (Keyspace, OwnedBound, OwnedBound, Option<PathBuf>);

/// A `KvRead` over a borrowed native snapshot, valid only inside `export_sst` or `with_snapshot`.
struct BorrowedSnapshot<'a> {
    db: &'a DB,
    snapshot: &'a rocksdb::Snapshot<'a>,
}

impl KvRead for BorrowedSnapshot<'_> {
    fn get(&self, ks: Keyspace, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        self.snapshot
            .get_cf(self.db.cf_handle(ks.name()).unwrap(), key)
            .map_err(map_error)
    }

    fn range(
        &self,
        ks: Keyspace,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        dir: Direction,
    ) -> Result<KvIter<'_>, KvError> {
        let (start, end) = (own(start), own(end));
        let iter = self.snapshot.iterator_cf(
            self.db.cf_handle(ks.name()).unwrap(),
            mode(&start, &end, dir),
        );
        Ok(bounded(iter, start, end, dir))
    }
}

/// Smallest key strictly greater than `key` in bytewise order.
fn successor(key: &[u8]) -> Vec<u8> {
    let mut next = Vec::with_capacity(key.len() + 1);
    next.extend_from_slice(key);
    next.push(0);
    next
}

#[cfg_attr(
    not(test),
    allow(dead_code, reason = "R5-04/R5-05 range transfer uses these")
)]
impl RocksKv {
    /// Export each interval, as of ONE RocksDB snapshot, to one SST file per
    /// non-empty part under `dir` (`part-NNNN-<ks>.sst`, overwritten if present).
    /// `at_snapshot` runs against that same snapshot, so values it reads are
    /// consistent with the exported files.
    pub(crate) fn export_sst<T>(
        &self,
        dir: &Path,
        parts: &[SstPart],
        at_snapshot: impl FnOnce(&dyn KvRead) -> Result<T, KvError>,
    ) -> Result<SstExport<T>, KvError> {
        self.db.clear_points();
        let io_error = |error: std::io::Error| KvError::Io(error.to_string());
        std::fs::create_dir_all(dir).map_err(io_error)?;
        let db: &DB = &self.db.db;
        let snapshot = db.snapshot();
        let mut options = Options::default();
        options.set_compression_type(DBCompressionType::Lz4);
        let mut files = Vec::with_capacity(parts.len());
        for (index, (ks, start, end)) in parts.iter().enumerate() {
            let path = native_path(&dir.join(format!("part-{index:04}-{}.sst", ks.name())));
            let iter = snapshot.iterator_cf(
                db.cf_handle(ks.name()).unwrap(),
                mode(start, end, Direction::Forward),
            );
            let mut writer = SstFileWriter::create(&options);
            let mut keys = 0u64;
            for pair in bounded(iter, start.clone(), end.clone(), Direction::Forward) {
                let (key, value) = pair?;
                if keys == 0 {
                    writer.open(&path).map_err(map_error)?;
                }
                writer.put(key, value).map_err(map_error)?;
                keys += 1;
            }
            let file = if keys == 0 {
                SstFile {
                    ks: *ks,
                    path: None,
                    bytes: 0,
                    keys,
                }
            } else {
                writer.finish().map_err(map_error)?;
                let bytes = std::fs::metadata(&path).map_err(io_error)?.len();
                SstFile {
                    ks: *ks,
                    path: Some(path),
                    bytes,
                    keys,
                }
            };
            files.push(file);
        }
        let at_snapshot = at_snapshot(&BorrowedSnapshot {
            db,
            snapshot: &snapshot,
        })?;
        Ok(SstExport { files, at_snapshot })
    }

    /// Replace each interval's contents with its file (or nothing): a synced
    /// `delete_range_cf` of the interval, then ingestion with move_files=false,
    /// allow_global_seqno=true, ingest_behind=false. Not atomic across parts;
    /// repeating the same call converges to the same contents.
    ///
    /// Bounds map to the half-open `[from, to)` of `delete_range_cf` through the
    /// bytewise successor `key ++ 0x00`. An unbounded start is the empty key. An
    /// unbounded end has no finite bound, so it is the successor of the last key
    /// currently in the CF (nothing to delete if there is none); the caller must
    /// keep writers out of the interval while installing.
    pub(crate) fn ingest_sst(&self, parts: &[SstInstall]) -> Result<(), KvError> {
        self.db.clear_points();
        let db: &DB = &self.db.db;
        let mut ingest = IngestExternalFileOptions::default();
        ingest.set_move_files(false);
        ingest.set_allow_global_seqno(true);
        ingest.set_ingest_behind(false);
        let mut sync = WriteOptions::default();
        sync.set_sync(true);
        for (ks, start, end, file) in parts {
            let cf = db.cf_handle(ks.name()).unwrap();
            let from = match start {
                Bound::Included(key) => key.clone(),
                Bound::Excluded(key) => successor(key),
                Bound::Unbounded => Vec::new(),
            };
            let to = match end {
                Bound::Included(key) => Some(successor(key)),
                Bound::Excluded(key) => Some(key.clone()),
                Bound::Unbounded => match db.iterator_cf(cf, IteratorMode::End).next() {
                    Some(pair) => Some(successor(&pair.map_err(map_error)?.0)),
                    None => None,
                },
            };
            if let Some(to) = to.filter(|to| from < *to) {
                let mut batch = rocksdb::WriteBatch::default();
                batch.delete_range_cf(cf, from, to);
                db.write_opt(batch, &sync).map_err(map_error)?;
            }
            if let Some(path) = file {
                db.ingest_external_file_cf_opts(cf, &ingest, vec![native_path(path)])
                    .map_err(map_error)?;
            }
        }
        Ok(())
    }
}

/// The key standing for an unbounded end in `approximate_size`: raw keys start with a small
/// kind byte or a BE8 table/index id, so no stored key sorts at or above 64 bytes of 0xFF (and
/// a key that did would only make the approximation smaller).
const APPROX_END: [u8; 64] = [0xFF; 64];

impl RocksKv {
    /// Run `read` against ONE RocksDB snapshot held for the duration of the call.
    pub(crate) fn with_snapshot<T>(
        &self,
        read: impl FnOnce(&dyn KvRead) -> Result<T, KvError>,
    ) -> Result<T, KvError> {
        let db: &DB = &self.db.db;
        let snapshot = db.snapshot();
        read(&BorrowedSnapshot {
            db,
            snapshot: &snapshot,
        })
    }

    /// RocksDB's approximate bytes in memtables and SST files for one column family's raw
    /// interval. An unbounded start is the empty key, an unbounded end `APPROX_END`.
    pub(crate) fn approximate_size(
        &self,
        ks: Keyspace,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
    ) -> u64 {
        let db: &DB = &self.db.db;
        let from = match start {
            Bound::Included(key) => key.to_vec(),
            Bound::Excluded(key) => successor(key),
            Bound::Unbounded => Vec::new(),
        };
        let to = match end {
            Bound::Included(key) => successor(key),
            Bound::Excluded(key) => key.to_vec(),
            Bound::Unbounded => APPROX_END.to_vec(),
        };
        if from >= to {
            return 0;
        }
        let cf = db.cf_handle(ks.name()).unwrap();
        let start = from.as_ptr().cast();
        let limit = to.as_ptr().cast();
        let mut size = 0;
        let mut err = std::ptr::null_mut();
        let include_flags = (1 << 0) | (1 << 1); // include_memtable | include_files
        // SAFETY: db and its CF handle remain live through the call. The key buffers,
        // pointers and lengths describe one valid interval; size and err are writable.
        // RocksDB owns any returned error, which must be released with rocksdb_free.
        unsafe {
            librocksdb_sys::rocksdb_approximate_sizes_cf_with_flags(
                db.as_raw_ptr(),
                cf.inner(),
                1,
                &start,
                &from.len(),
                &limit,
                &to.len(),
                include_flags,
                &mut size,
                &mut err,
            );
            if !err.is_null() {
                librocksdb_sys::rocksdb_free(err.cast());
                return 0;
            }
        }
        size
    }

    /// Flush every column family's memtables to SST files.
    pub(crate) fn flush_memtables(&self) -> Result<(), KvError> {
        self.db.clear_points();
        for ks in Keyspace::ALL {
            let cf = self.db.cf_handle(ks.name()).unwrap();
            self.db.flush_cf(cf).map_err(map_error)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn flush_one_memtable(&self, ks: Keyspace) -> Result<(), KvError> {
        let mut options = rocksdb::FlushOptions::default();
        options.set_wait(true);
        self.db
            .flush_cf_opt(self.db.cf_handle(ks.name()).unwrap(), &options)
            .map_err(|error| self.map_write_error(error, FenceSite::BackgroundError, false))
    }

    #[cfg(test)]
    pub(crate) fn has_sst(&self, ks: Keyspace) -> bool {
        self.db
            .live_files()
            .unwrap()
            .iter()
            .any(|file| file.column_family_name == ks.name())
    }
}

fn map_error(error: rocksdb::Error) -> KvError {
    map_error_at(error, FenceSite::Read)
}

fn map_error_at(error: rocksdb::Error, site: FenceSite) -> KvError {
    use rocksdb::ErrorKind::*;
    let code = match error.kind() {
        NotFound => 1,
        Corruption => return KvError::Corrupt(error.into_string()),
        NotSupported => 3,
        InvalidArgument => 4,
        IOError => RocksStatus::IO_ERROR,
        MergeInProgress => 6,
        Incomplete => 7,
        ShutdownInProgress => 8,
        TimedOut => 9,
        Aborted => 10,
        Busy => 11,
        Expired => 12,
        TryAgain => 13,
        CompactionTooLarge => 14,
        ColumnFamilyDropped => 15,
        Unknown => u8::MAX,
    };
    let message = error.into_string();
    let (subcode, _) = status_detail(&message);
    KvError::RocksIo { message, status: RocksStatus { code, subcode, severity: None }, site }
}

fn status_detail(message: &str) -> (u8, &str) {
    // Status::ToString emits code and subcode prefixes. It does not serialize severity.
    // The labels and their order are from the pinned RocksDB util/status.cc.
    const SUBCODES: [&str; 19] = [
        "Timeout Acquiring Mutex",
        "Timeout waiting to lock key",
        "Failed to acquire lock due to max_num_locks limit",
        "No space left on device",
        "Deadlock",
        "Stale file handle",
        "Memory limit reached",
        "Space limit reached",
        "No such file or directory",
        "Insufficient capacity for merge operands",
        "Manual compaction paused",
        " (overwritten)",
        "Txn not prepared",
        "IO fenced off",
        "Merge operator failed",
        "Number of operands merged exceeded threshold",
        "MultiScan reached file prefetch limit",
        "Not expected code path",
        "All compactions aborted",
    ];
    let detail = message.split_once(": ").map_or("", |(_, detail)| detail);
    for (index, prefix) in SUBCODES.iter().enumerate() {
        if detail == *prefix {
            return ((index + 1) as u8, "");
        }
        if let Some(rest) = detail.strip_prefix(prefix).and_then(|rest| rest.strip_prefix(": ")) {
            return ((index + 1) as u8, rest);
        }
    }
    (0, detail)
}

fn wal_number(file: &str) -> Option<u64> {
    let number = file.strip_suffix(".log")?;
    if !number.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    number.parse().ok()
}

fn wal_io(detail: &str) -> Option<(&str, u64)> {
    // A write can return SST, MANIFEST or background errors as well. Require a
    // known append/sync diagnostic and a numeric RocksDB WAL filename.
    const OPERATIONS: [&str; 6] = [
        "While appending to file: ",
        "While fdatasync: ",
        "While fsync: ",
        "Failed to WriteFile: ",
        "Failed to write all bytes: ",
        "FlushFileBuffers failed at Sync() for: ",
    ];
    let diagnostic = OPERATIONS.iter().find_map(|op| detail.strip_prefix(op))?;
    let path = diagnostic
        .split_once(": ")
        .map_or(diagnostic, |(path, _)| path);
    Some((path, wal_number(path.rsplit(['/', '\\']).next()?)?))
}

fn newest_wal(path: &Path) -> std::io::Result<Option<u64>> {
    let mut newest = None;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        if let Some(number) = entry.file_name().to_str().and_then(wal_number)
            && entry.file_type()?.is_file()
        {
            newest = newest.max(Some(number));
        }
    }
    Ok(newest)
}

fn write_error_site(
    site: FenceSite,
    sync: bool,
    message: &str,
    background_errors: Option<u64>,
    wal_dir: &Path,
) -> FenceSite {
    match background_errors {
        Some(1..) => FenceSite::BackgroundError,
        Some(0)
            if sync
                && let Some((file, number)) = wal_io(status_detail(message).1)
                && Path::new(file).parent() == Some(wal_dir)
                && newest_wal(wal_dir).is_ok_and(|newest| newest == Some(number)) =>
        {
            match site {
                FenceSite::Write => FenceSite::WalWrite,
                FenceSite::FlushBarrier => FenceSite::WalFlush,
                _ => site,
            }
        }
        _ => site,
    }
}

impl RocksKv {
    // In RocksDB 11.8.1, background SyncClosedWals only syncs older WALs.
    // Its error can be published before the cumulative background-error count
    // increments, so require the diagnostic to name the newest live WAL too.
    // WALs are kept in the DB directory. Re-verify these and the native error
    // text and property semantics when upgrading RocksDB.
    fn map_write_error(
        &self,
        error: rocksdb::Error,
        site: FenceSite,
        sync: bool,
    ) -> KvError {
        let mut error = map_error_at(error, site);
        if let KvError::RocksIo { status, site: error_site, message } = &mut error
            && status.code == RocksStatus::IO_ERROR
        {
            let background_errors = self
                .db
                .property_int_value("rocksdb.background-errors")
                .ok()
                .flatten();
            *error_site = write_error_site(site, sync, message, background_errors, self.db.path());
        }
        error
    }
}

// RocksDB appends slash-separated filenames. Windows verbatim paths prohibit
// those forward slashes, so reconstruct their equivalent ordinary absolute path.
#[cfg(windows)]
fn native_path(path: &Path) -> PathBuf {
    use std::path::{Component, Prefix};
    let mut components = path.components();
    let Some(Component::Prefix(prefix)) = components.next() else {
        return path.into();
    };
    let mut normal = match prefix.kind() {
        Prefix::VerbatimDisk(drive) => PathBuf::from(format!("{}:\\", char::from(drive))),
        Prefix::VerbatimUNC(server, share) => {
            let mut unc = std::ffi::OsString::from("\\\\");
            unc.push(server);
            unc.push("\\");
            unc.push(share);
            PathBuf::from(unc)
        }
        _ => return path.into(),
    };
    for component in components {
        if !matches!(component, Component::RootDir) {
            normal.push(component.as_os_str());
        }
    }
    normal
}

#[cfg(not(windows))]
fn native_path(path: &Path) -> PathBuf {
    path.into()
}

type OwnedBound = Bound<Vec<u8>>;

fn own(bound: Bound<&[u8]>) -> OwnedBound {
    bound.map(<[u8]>::to_vec)
}

fn mode<'a>(start: &'a OwnedBound, end: &'a OwnedBound, dir: Direction) -> IteratorMode<'a> {
    match dir {
        Direction::Forward => match start {
            Bound::Included(key) | Bound::Excluded(key) => {
                IteratorMode::From(key, rocksdb::Direction::Forward)
            }
            Bound::Unbounded => IteratorMode::Start,
        },
        Direction::Reverse => match end {
            Bound::Included(key) | Bound::Excluded(key) => {
                IteratorMode::From(key, rocksdb::Direction::Reverse)
            }
            Bound::Unbounded => IteratorMode::End,
        },
    }
}

fn bounded<'a>(
    mut iter: rocksdb::DBIterator<'a>,
    start: OwnedBound,
    end: OwnedBound,
    dir: Direction,
) -> KvIter<'a> {
    let mut done = false;
    KvIter::new(std::iter::from_fn(move || {
        while !done {
            let pair = match iter.next()? {
                Ok(pair) => pair,
                Err(error) => {
                    done = true;
                    return Some(Err(map_error(error)));
                }
            };
            let key = pair.0.as_ref();
            let lower = match &start {
                Bound::Included(value) => key >= value.as_slice(),
                Bound::Excluded(value) => key > value.as_slice(),
                Bound::Unbounded => true,
            };
            let upper = match &end {
                Bound::Included(value) => key <= value.as_slice(),
                Bound::Excluded(value) => key < value.as_slice(),
                Bound::Unbounded => true,
            };
            if lower && upper {
                return Some(Ok((pair.0.into_vec(), pair.1.into_vec())));
            }
            done = match dir {
                Direction::Forward => !upper,
                Direction::Reverse => !lower,
            };
        }
        None
    }))
}

impl KvRead for RocksKv {
    fn get(&self, ks: Keyspace, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        self.db
            .get_cf(self.db.cf_handle(ks.name()).unwrap(), key)
            .map_err(map_error)
    }

    fn first_in(&self, ks: Keyspace, start: &[u8], end: &[u8]) -> Result<Option<KvPair>, KvError> {
        if start >= end {
            return Ok(None);
        }
        let (generation, cached) = {
            let mut pool = self.db.points.lock().unwrap_or_else(|e| e.into_inner());
            let idle_timeout = POINT_POOL_IDLE;
            #[cfg(test)]
            let idle_timeout = pool.idle_timeout.unwrap_or(idle_timeout);
            // Sweep all CFs so activity on one also releases other stale pins.
            for idle in pool.idle.values_mut() {
                idle.retain(|(used, _)| used.elapsed() < idle_timeout);
            }
            let generation = pool.generation;
            (generation, pool.idle.get_mut(&ks).and_then(Vec::pop))
        };
        #[cfg(test)]
        let reused = cached.is_some();
        let mut iter = match cached {
            Some((_, iter)) => iter,
            None => {
                let iter = self
                    .db
                    .raw_iterator_cf(self.db.cf_handle(ks.name()).unwrap());
                // SAFETY: constructed only after LockedDb is in its stable Arc allocation.
                // The iterator never escapes this method or LockedDb::points. This call's
                // &self keeps the Arc alive while checked out; idle iterators are dropped
                // before db by field order. Neither DB nor CF handles can be replaced.
                // rust-rocksdb supplies Send + Sync; we add no unsafe trait impls.
                unsafe {
                    std::mem::transmute::<rocksdb::DBRawIterator<'_>, rocksdb::DBRawIterator<'static>>(
                        iter,
                    )
                }
            }
        };
        // Refresh returns a status independent of iterator.status(). On either error,
        // drop the iterator rather than putting a potentially failed one back.
        iter.refresh().map_err(map_error)?;
        iter.seek(start);
        iter.status().map_err(map_error)?;
        let pair = iter
            .item()
            .filter(|(key, _)| *key < end)
            .map(|(key, value)| (key.to_vec(), value.to_vec()));
        let mut pool = self.db.points.lock().unwrap_or_else(|e| e.into_inner());
        #[cfg(test)]
        if reused {
            pool.refreshed_reuses += 1;
        } else {
            pool.created += 1;
        }
        if pool.generation == generation {
            let idle = pool.idle.entry(ks).or_default();
            if idle.len() < POINT_POOL_CAP {
                idle.push((Instant::now(), iter));
            }
        }
        Ok(pair)
    }

    fn range(
        &self,
        ks: Keyspace,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        dir: Direction,
    ) -> Result<KvIter<'_>, KvError> {
        let (start, end) = (own(start), own(end));
        let iter = self.db.iterator_cf(
            self.db.cf_handle(ks.name()).unwrap(),
            mode(&start, &end, dir),
        );
        Ok(bounded(iter, start, end, dir))
    }
}

impl KvBackend for RocksKv {
    fn close_latch(&self) -> Option<CloseLatch> {
        Some(self.close_latch())
    }


    fn write(&self, batch: WriteBatch, durability: Durability) -> Result<(), KvError> {
        let mut native = rocksdb::WriteBatch::default();
        for op in batch.into_ops() {
            match op {
                Op::Put { ks, key, value } => {
                    native.put_cf(self.db.cf_handle(ks.name()).unwrap(), key, value)
                }
                Op::Delete { ks, key } => {
                    native.delete_cf(self.db.cf_handle(ks.name()).unwrap(), key)
                }
                Op::DeleteRange { ks, start, end } if start < end => {
                    native.delete_range_cf(self.db.cf_handle(ks.name()).unwrap(), start, end);
                }
                Op::DeleteRange { .. } => {}
            }
        }
        let mut options = WriteOptions::default();
        options.set_sync(durability == Durability::Sync);
        options.disable_wal(false);
        self.db.write_opt(native, &options)
            .map_err(|error| self.map_write_error(error, FenceSite::Write, durability == Durability::Sync))
    }

    fn flush_barrier(&self) -> Result<(), KvError> {
        self.db.flush_wal(true)
            .map_err(|error| self.map_write_error(error, FenceSite::FlushBarrier, true))
    }

    fn snapshot(&self) -> Result<Arc<dyn KvRead>, KvError> {
        OwnedSnapshot::new(Arc::clone(&self.db))
            .map(|snapshot| Arc::new(snapshot) as Arc<dyn KvRead>)
    }

    fn engine_name(&self) -> &'static str {
        "rocksdb"
    }
}

type Reply<T> = mpsc::SyncSender<T>;
enum Request {
    Get(Keyspace, Vec<u8>, Reply<Result<Option<Vec<u8>>, KvError>>),
    Range(Keyspace, OwnedBound, OwnedBound, Direction, Reply<u64>),
    Next(u64, Reply<Option<Result<KvPair, KvError>>>),
    DropRange(u64),
}

// The worker owns the Arc<DB>; its native snapshot and native iterators borrow
// that DB on the same stack. RPC transfers only owned bytes, never lifetimes.
// Joining on drop ensures the snapshot and iterators die before the last DB Arc.
struct OwnedSnapshot {
    requests: Option<mpsc::Sender<Request>>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

fn disconnected() -> KvError {
    KvError::Io("RocksDB snapshot worker disconnected".into())
}

impl OwnedSnapshot {
    fn new(db: Arc<LockedDb>) -> Result<Self, KvError> {
        let (requests, receive) = mpsc::channel();
        let (ready, started) = mpsc::sync_channel(0);
        let worker = thread::Builder::new()
            .name("rocksdb-kv-snapshot".into())
            .spawn(move || {
                let snapshot = db.snapshot();
                let mut iterators = BTreeMap::new();
                let mut next_id = 0u64;
                if ready.send(()).is_err() {
                    return;
                }
                while let Ok(request) = receive.recv() {
                    match request {
                        Request::Get(ks, key, reply) => {
                            let _ = reply.send(
                                snapshot
                                    .get_cf(db.cf_handle(ks.name()).unwrap(), key)
                                    .map_err(map_error),
                            );
                        }
                        Request::Range(ks, start, end, dir, reply) => {
                            let iter = snapshot.iterator_cf(
                                db.cf_handle(ks.name()).unwrap(),
                                mode(&start, &end, dir),
                            );
                            let id = next_id;
                            next_id += 1;
                            iterators.insert(id, bounded(iter, start, end, dir));
                            if reply.send(id).is_err() {
                                iterators.remove(&id);
                            }
                        }
                        Request::Next(id, reply) => {
                            let item = iterators.get_mut(&id).and_then(Iterator::next);
                            let finished = item.is_none();
                            if reply.send(item).is_err() || finished {
                                iterators.remove(&id);
                            }
                        }
                        Request::DropRange(id) => {
                            iterators.remove(&id);
                        }
                    }
                }
            })
            .map_err(|error| KvError::Io(error.to_string()))?;
        let snapshot = Self {
            requests: Some(requests),
            worker: Mutex::new(Some(worker)),
        };
        started.recv().map_err(|_| disconnected())?;
        Ok(snapshot)
    }

    fn send(&self, request: Request) -> Result<(), KvError> {
        self.requests
            .as_ref()
            .unwrap()
            .send(request)
            .map_err(|_| disconnected())
    }
}

impl Drop for OwnedSnapshot {
    fn drop(&mut self) {
        self.requests.take();
        if let Some(worker) = self
            .worker
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            let _ = worker.join();
        }
    }
}

impl KvRead for OwnedSnapshot {
    fn get(&self, ks: Keyspace, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        let (reply, receive) = mpsc::sync_channel(0);
        self.send(Request::Get(ks, key.to_vec(), reply))?;
        receive.recv().map_err(|_| disconnected())?
    }

    fn range(
        &self,
        ks: Keyspace,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        dir: Direction,
    ) -> Result<KvIter<'_>, KvError> {
        let (reply, receive) = mpsc::sync_channel(0);
        self.send(Request::Range(ks, own(start), own(end), dir, reply))?;
        let id = receive.recv().map_err(|_| disconnected())?;
        Ok(KvIter::new(SnapshotIter {
            snapshot: self,
            id,
            done: false,
        }))
    }
}

struct SnapshotIter<'a> {
    snapshot: &'a OwnedSnapshot,
    id: u64,
    done: bool,
}

impl Iterator for SnapshotIter<'_> {
    type Item = Result<KvPair, KvError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let (reply, receive) = mpsc::sync_channel(0);
        let result = self
            .snapshot
            .send(Request::Next(self.id, reply))
            .and_then(|()| receive.recv().map_err(|_| disconnected()));
        match result {
            Ok(item) => {
                self.done = item.is_none();
                item
            }
            Err(error) => {
                self.done = true;
                Some(Err(error))
            }
        }
    }
}

impl Drop for SnapshotIter<'_> {
    fn drop(&mut self) {
        let _ = self.snapshot.send(Request::DropRange(self.id));
    }
}

#[cfg(test)]
#[path = "kv_rocksdb_sst_tests.rs"]
mod sst_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::v2::recovery_tests::retry_writer_lock;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct TestDir(PathBuf);
    impl TestDir {
        fn new() -> Self {
            static NEXT: AtomicUsize = AtomicUsize::new(0);
            let path = std::env::temp_dir().join(format!(
                "kv-rocksdb-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for TestDir {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn writer_lock_is_inherited_until_child_exec() {
        use std::{
            io,
            os::{fd::AsRawFd, unix::process::CommandExt},
            process::Command,
        };

        const ISOLATED: &str = "GIGGLES_TEST_WRITER_LOCK_INHERITANCE";
        if std::env::var_os(ISOLATED).is_none() {
            // Other tests can fork concurrently. Keep their children out of the control.
            let (_, module) = module_path!().split_once("::").unwrap();
            let output = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    &format!("{module}::writer_lock_is_inherited_until_child_exec"),
                ])
                .env(ISOLATED, "1")
                .output()
                .unwrap();
            let stdout = String::from_utf8_lossy(&output.stdout);
            assert!(
                output.status.success()
                    && stdout.lines().any(|line| line.starts_with("test result: ok. 1 passed;")),
                "{}\n{stdout}\n{}",
                output.status,
                String::from_utf8_lossy(&output.stderr),
            );
            return;
        }

        let dir = TestDir::new();
        let path = &dir.0;
        drop(WriterLock::acquire(path).unwrap());
        let lock = WriterLock::acquire(path).unwrap();
        let (mut entered, ready) = io::pipe().unwrap();
        let (resume, allow_exec) = io::pipe().unwrap();
        let child_release_writer = allow_exec.as_raw_fd();
        let mut command = Command::new("/bin/true");
        // SAFETY: the closure owns the pipe ends it reads and writes. The
        // parent keeps allow_exec live through fork, and close only affects
        // the child's inherited descriptor. All calls are async-signal-safe.
        unsafe {
            command.pre_exec(move || {
                let mut signal = [1u8];
                if libc::close(child_release_writer) == -1
                    || libc::write(ready.as_raw_fd(), signal.as_ptr().cast(), signal.len()) != 1
                {
                    return Err(io::Error::last_os_error());
                }
                libc::sleep(1);
                // The parent closes the remaining writer after probing the lock.
                if libc::read(resume.as_raw_fd(), signal.as_mut_ptr().cast(), signal.len()) != 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        thread::scope(move |scope| {
            // status() cannot return while the forked child waits in pre_exec.
            let status = scope.spawn(move || command.status().unwrap());
            let ready = entered.read_exact(&mut [0]);
            drop(lock);
            let blocked = WriterLock::acquire(path);
            drop(allow_exec);
            assert!(status.join().unwrap().success());
            ready.unwrap();
            assert!(matches!(
                blocked,
                Err(KvError::Io(message)) if message == "database already open by another writer"
            ));
        });
        drop(WriterLock::acquire(path).unwrap());
    }

    fn single() -> WriteBatch {
        let mut batch = WriteBatch::default();
        for ks in Keyspace::ALL {
            batch.put(ks, b"key".to_vec(), b"value".to_vec());
        }
        batch
    }

    #[test]
    fn status_detail_and_wal_io_fail_closed() {
        for (message, subcode, number) in [
            (
                "IO error: While appending to file: /db/000042.log: File too large",
                0,
                Some(42),
            ),
            (
                "IO error: No space left on device: While fdatasync: /db/000042.log: No space left on device",
                4,
                Some(42),
            ),
            ("IO error: No space left on device", 4, None),
            ("IO error: While fsync: /db/000042.log", 0, Some(42)),
            (
                r"IO error: Failed to WriteFile: C:\db\000042.log: Error 112",
                0,
                Some(42),
            ),
            (
                r"IO error: Failed to write all bytes: C:\db\000042.log",
                0,
                Some(42),
            ),
            (
                r"IO error: FlushFileBuffers failed at Sync() for: C:\db\000042.log: Error 112",
                0,
                Some(42),
            ),
            (
                "IO error: While appending to file: /db/000042.sst: out of space",
                0,
                None,
            ),
            (
                "IO error: While fsync: /db/MANIFEST-000042: out of space",
                0,
                None,
            ),
            ("IO error: While fsync: /db/: out of space", 0, None),
            (
                "IO error: While open a file for appending: /db/000042.log: out of space",
                0,
                None,
            ),
            (
                "IO error: While appending to file: /db/000042.log.tmp: out of space",
                0,
                None,
            ),
            (
                "IO error: While fsync: /db/123.log/data: path to 42.log",
                0,
                None,
            ),
            (
                "IO error: While appending to file: /db/current.log: out of space",
                0,
                None,
            ),
            (
                "IO error: While appending to file: /db/.log: out of space",
                0,
                None,
            ),
            (
                "IO error: While appending to file: /db/18446744073709551616.log",
                0,
                None,
            ),
            ("Incomplete", 0, None),
        ] {
            let (actual_subcode, detail) = status_detail(message);
            assert_eq!(actual_subcode, subcode, "{message}");
            assert_eq!(wal_io(detail).map(|(_, number)| number), number, "{message}");
        }
    }

    #[test]
    fn write_error_requires_newest_live_wal() {
        let dir = TestDir::new();
        for name in ["000099.log", "000100.log", "000101.log.tmp"] {
            File::create(dir.0.join(name)).unwrap();
        }
        std::fs::create_dir(dir.0.join("000102.log")).unwrap();
        let append_error = |path: &Path| format!("IO error: While appending to file: {}: File too large", path.display());
        let current = append_error(&dir.0.join("000100.log"));
        let old = append_error(&dir.0.join("000099.log"));
        let unreadable = dir.0.join("000100.log");
        let unreadable_error = append_error(&unreadable.join("000100.log"));
        let classify = |message: &str, sync, count, path| {
            let site = write_error_site(FenceSite::Write, sync, message, count, path);
            let error = KvError::RocksIo {
                message: message.into(),
                site,
                status: RocksStatus {
                    code: RocksStatus::IO_ERROR,
                    subcode: 0,
                    severity: None,
                },
            };
            (site, FenceCause::for_kv(FenceSite::Write, &error).class)
        };
        use {FenceClass as Class, FenceSite as Site};
        assert_eq!(newest_wal(&dir.0).unwrap(), Some(100));
        assert_eq!(
            classify(&current, true, Some(0), &dir.0),
            (Site::WalWrite, Class::Recoverable)
        );
        assert_eq!(
            write_error_site(Site::FlushBarrier, true, &current, Some(0), &dir.0),
            Site::WalFlush
        );
        assert_eq!(
            classify(&current, true, Some(1), &dir.0),
            (Site::BackgroundError, Class::NotRecoverable)
        );
        for (message, sync, count, path) in [
            (old.as_str(), true, Some(0), dir.0.as_path()),
            (
                "IO error: While appending to file: /db/000100.sst",
                true,
                Some(0),
                dir.0.as_path(),
            ),
            (
                "IO error: While appending to file: /db/.log",
                true,
                Some(0),
                dir.0.as_path(),
            ),
            ("IO error: While appending to file: /other/000100.log", true, Some(0), dir.0.as_path()),
            (&unreadable_error, true, Some(0), unreadable.as_path()),
            (&current, true, None, dir.0.as_path()),
            (&current, false, Some(0), dir.0.as_path()),
        ] {
            assert_eq!(
                classify(message, sync, count, path),
                (Site::Write, Class::NotRecoverable),
                "{message}"
            );
        }
    }

    #[test]
    fn conformance() {
        let dirs = Mutex::new(Vec::new());
        super::super::kv_conformance::run_all(&|| {
            let dir = TestDir::new();
            let db = RocksKv::open(&dir.0, 8 * 1024 * 1024).unwrap();
            dirs.lock().unwrap().push(dir);
            Arc::new(db)
        });
    }

    #[test]
    fn first_in_borrowed_and_reopened() {
        let dir = TestDir::new();
        {
            let db = RocksKv::open(&dir.0, 8 * 1024 * 1024).unwrap();
            let mut batch = WriteBatch::default();
            for key in [b"a", b"b", b"c", b"d", b"z"] {
                batch.put(Keyspace::Rows, key.to_vec(), key.to_vec());
            }
            db.write(batch, Durability::Sync).unwrap();
            db.flush_memtables().unwrap();
            let mut batch = WriteBatch::default();
            batch.delete(Keyspace::Rows, b"a".to_vec());
            batch.delete_range(Keyspace::Rows, b"b".to_vec(), b"d".to_vec());
            db.write(batch, Durability::Sync).unwrap();
            db.with_snapshot(|read| {
                super::super::kv_conformance::assert_first_in_deleted(read);
                Ok(())
            })
            .unwrap();
            super::super::mvcc::tests::point_versions(&db);
        }
        let db = retry_writer_lock(|| RocksKv::open(&dir.0, 8 * 1024 * 1024)).unwrap();
        super::super::kv_conformance::assert_first_in_deleted(&db);
        super::super::mvcc::tests::check_point_versions(&db);
        super::super::mvcc::tests::check_point_versions(db.snapshot().unwrap().as_ref());
        db.with_snapshot(|read| {
            super::super::mvcc::tests::check_point_versions(read);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn pooled_drop_clears_pins_while_snapshot_owns_db() {
        let dir = TestDir::new();
        let db = RocksKv::open(&dir.0, 8 * 1024 * 1024).unwrap();
        let latch = db.close_latch();
        point_put(&db, b"a", b"old");
        let snapshot = db.snapshot().unwrap();
        db.first_in(Keyspace::Rows, b"a", b"b").unwrap();
        let owner = db.db.clone();
        drop(db);
        assert!(owner.points.lock().unwrap().idle.is_empty());
        assert_eq!(
            snapshot.first_in(Keyspace::Rows, b"a", b"b").unwrap(),
            Some((b"a".to_vec(), b"old".to_vec()))
        );
        drop(snapshot);
        drop(owner);
        latch.wait(Duration::ZERO).unwrap();
        let reopened = retry_writer_lock(|| RocksKv::open(&dir.0, 8 * 1024 * 1024)).unwrap();
        assert!(
            reopened
                .first_in(Keyspace::Rows, b"a", b"b")
                .unwrap()
                .is_some()
        );
    }

    fn point_put(db: &RocksKv, key: &[u8], value: &[u8]) {
        let mut batch = WriteBatch::default();
        batch.put(Keyspace::Rows, key.to_vec(), value.to_vec());
        db.write(batch, Durability::Sync).unwrap();
    }

    // Keep refresh proofs independent of flush latency and scheduler pauses.
    fn disable_point_eviction(db: &RocksKv) {
        db.db.points.lock().unwrap().idle_timeout = Some(Duration::MAX);
    }

    // Call only while this test has exclusive access to point reads. Counters
    // advance after refresh and seek both succeed, so creation cannot pass.
    fn reused_point(db: &RocksKv, start: &[u8], end: &[u8]) -> Option<KvPair> {
        let before = {
            let pool = db.db.points.lock().unwrap();
            (pool.created, pool.refreshed_reuses)
        };
        let pair = db.first_in(Keyspace::Rows, start, end).unwrap();
        let pool = db.db.points.lock().unwrap();
        assert_eq!(
            (pool.created, pool.refreshed_reuses),
            (before.0, before.1 + 1)
        );
        pair
    }

    #[test]
    fn pooled_refresh_put_delete_and_range_tombstones() {
        let dir = TestDir::new();
        let db = RocksKv::open(&dir.0, 8 * 1024 * 1024).unwrap();
        disable_point_eviction(&db);
        point_put(&db, b"a", b"old");
        db.first_in(Keyspace::Rows, b"a", b"b").unwrap();
        let snapshot = db.snapshot().unwrap();
        for value in [b"old".as_slice(), b"new"] {
            point_put(&db, b"a", value);
            assert_eq!(
                reused_point(&db, b"a", b"b"),
                Some((b"a".to_vec(), value.to_vec()))
            );
        }
        assert_eq!(db.db.points.lock().unwrap().idle[&Keyspace::Rows].len(), 1);
        let mut batch = WriteBatch::default();
        batch.delete(Keyspace::Rows, b"a".to_vec());
        db.write(batch, Durability::Sync).unwrap();
        assert_eq!(reused_point(&db, b"a", b"b"), None);
        // First ever mutable-memtable range tombstone requires the refresh rebuild path.
        for _ in 0..2 {
            point_put(&db, b"a", b"back");
            assert!(reused_point(&db, b"a", b"b").is_some());
            let mut batch = WriteBatch::default();
            batch.delete_range(Keyspace::Rows, b"a".to_vec(), b"b".to_vec());
            db.write(batch, Durability::Sync).unwrap();
            assert_eq!(reused_point(&db, b"a", b"b"), None);
        }
        assert_eq!(
            snapshot.first_in(Keyspace::Rows, b"a", b"b").unwrap(),
            Some((b"a".to_vec(), b"old".to_vec()))
        );
    }

    #[test]
    fn pooled_refresh_concurrent_own_commits() {
        let dir = TestDir::new();
        let db = RocksKv::open(&dir.0, 8 * 1024 * 1024).unwrap();
        disable_point_eviction(&db);
        db.first_in(Keyspace::Rows, &[0], &[6]).unwrap();
        let reads = Mutex::new(());
        thread::scope(|scope| {
            for writer in 0..6u8 {
                let db = &db;
                let reads = &reads;
                scope.spawn(move || {
                    let key = [writer];
                    let end = [writer + 1];
                    for commit in 0..100u64 {
                        let value = commit.to_be_bytes();
                        point_put(db, &key, &value);
                        thread::yield_now();
                        // Writes still overlap other writers and readers; serialize
                        // only checkout so every own-commit read must reuse.
                        let _read = reads.lock().unwrap();
                        assert_eq!(
                            reused_point(db, &key, &end),
                            Some((key.to_vec(), value.to_vec()))
                        );
                    }
                });
            }
        });
        assert!(db.db.points.lock().unwrap().idle[&Keyspace::Rows].len() <= POINT_POOL_CAP);
    }

    #[test]
    fn pooled_refresh_superversion_and_eviction() {
        let dir = TestDir::new();
        {
            let db = RocksKv::open(&dir.0, 8 * 1024 * 1024).unwrap();
            disable_point_eviction(&db);
            point_put(&db, b"a", b"old");
            db.first_in(Keyspace::Rows, b"a", b"b").unwrap();
            // Bypass the public clearing helper to exercise Refresh with a changed SV.
            let cf = db.db.cf_handle(Keyspace::Rows.name()).unwrap();
            db.db.flush_cf(cf).unwrap();
            assert_eq!(
                reused_point(&db, b"a", b"b"),
                Some((b"a".to_vec(), b"old".to_vec()))
            );
            db.db.compact_range_cf(cf, None::<&[u8]>, None::<&[u8]>);
            assert_eq!(
                reused_point(&db, b"a", b"b"),
                Some((b"a".to_vec(), b"old".to_vec()))
            );
            point_put(&db, b"a", b"new");
            assert_eq!(
                reused_point(&db, b"a", b"b"),
                Some((b"a".to_vec(), b"new".to_vec()))
            );
            // Restore the production timeout and age the entry without sleeping.
            db.db.points.lock().unwrap().idle_timeout = None;
            db.db
                .points
                .lock()
                .unwrap()
                .idle
                .get_mut(&Keyspace::Rows)
                .unwrap()[0]
                .0 = Instant::now() - Duration::from_secs(2);
            db.first_in(Keyspace::Index, b"a", b"b").unwrap();
            assert!(db.db.points.lock().unwrap().idle[&Keyspace::Rows].is_empty());
            db.flush_memtables().unwrap();
            assert!(db.db.points.lock().unwrap().idle.is_empty());
            db.first_in(Keyspace::Rows, b"a", b"b").unwrap();
        }
        let db = RocksKv::open(&dir.0, 8 * 1024 * 1024).unwrap();
        assert_eq!(
            db.first_in(Keyspace::Rows, b"a", b"b").unwrap(),
            Some((b"a".to_vec(), b"new".to_vec()))
        );
    }

    fn reopen(durability: Durability) {
        let dir = TestDir::new();
        {
            let db = RocksKv::open(&dir.0, 8 * 1024 * 1024).unwrap();
            db.write(single(), durability).unwrap();
            if durability == Durability::NoSync {
                db.flush_barrier().unwrap();
            }
        }
        let db = retry_writer_lock(|| RocksKv::open(&dir.0, 8 * 1024 * 1024)).unwrap();
        assert_eq!(db.engine_name(), "rocksdb");
        for ks in Keyspace::ALL {
            assert_eq!(db.get(ks, b"key").unwrap(), Some(b"value".to_vec()));
        }
    }

    #[test]
    fn sync_reopen() {
        reopen(Durability::Sync);
    }
    #[test]
    fn nosync_barrier_reopen() {
        reopen(Durability::NoSync);
    }

    #[test]
    fn snapshot_owns_database_and_supports_interleaved_reads() {
        let dir = TestDir::new();
        let db = RocksKv::open(&dir.0, 8 * 1024 * 1024).unwrap();
        db.write(single(), Durability::Sync).unwrap();
        let snapshot = db.snapshot().unwrap();
        drop(db);
        assert!(matches!(
            RocksKv::open(&dir.0, 8 * 1024 * 1024),
            Err(KvError::Io(message)) if message == "database already open by another writer"
        ));
        let mut forward = snapshot
            .range(
                Keyspace::Rows,
                Bound::Unbounded,
                Bound::Unbounded,
                Direction::Forward,
            )
            .unwrap();
        let mut reverse = snapshot
            .range(
                Keyspace::Rows,
                Bound::Unbounded,
                Bound::Unbounded,
                Direction::Reverse,
            )
            .unwrap();
        assert_eq!(
            snapshot.get(Keyspace::Rows, b"key").unwrap(),
            Some(b"value".to_vec())
        );
        assert_eq!(
            forward.next().unwrap().unwrap(),
            (b"key".to_vec(), b"value".to_vec())
        );
        assert_eq!(
            reverse.next().unwrap().unwrap(),
            (b"key".to_vec(), b"value".to_vec())
        );
        assert!(forward.next().is_none());
        assert!(reverse.next().is_none());
        drop((forward, reverse));
        drop(snapshot);
        RocksKv::open(&dir.0, 8 * 1024 * 1024).unwrap();
    }

    #[test]
    fn checkpoint_concurrent_batches_are_consistent() {
        let dir = TestDir::new();
        let db = Arc::new(RocksKv::open(&dir.0.join("source"), 8 * 1024 * 1024).unwrap());
        let (milestone, reached) = mpsc::sync_channel(0);
        let (resume, proceed) = mpsc::sync_channel(0);
        let writer_db = Arc::clone(&db);
        let writer = thread::spawn(move || {
            for i in 1..=3000u64 {
                let value = i.to_be_bytes().to_vec();
                let mut batch = WriteBatch::default();
                batch.put(Keyspace::Meta, b"counter".to_vec(), value.clone());
                batch.put(Keyspace::Rows, value.clone(), value.clone());
                batch.put(Keyspace::Index, value.clone(), value);
                let durability = if i % 50 == 0 {
                    Durability::Sync
                } else {
                    Durability::NoSync
                };
                writer_db.write(batch, durability).unwrap();
                if i % 500 == 0 {
                    milestone.send(()).unwrap();
                    proceed.recv().unwrap();
                }
            }
        });
        let mut copies = Vec::new();
        for i in 0..6 {
            reached.recv().unwrap();
            // Release the next group of writes before copying the live store.
            resume.send(()).unwrap();
            let target = dir.0.join(format!("checkpoint-{i}"));
            db.checkpoint(&target).unwrap();
            copies.push(target);
        }
        writer.join().unwrap();
        let mut counters = Vec::new();
        for target in copies {
            let copy = RocksKv::open(&target, 8 * 1024 * 1024).unwrap();
            let value = copy.get(Keyspace::Meta, b"counter").unwrap().unwrap();
            let counter = u64::from_be_bytes(value.try_into().unwrap());
            counters.push(counter);
            for ks in [Keyspace::Rows, Keyspace::Index] {
                let mut entries = copy
                    .range(ks, Bound::Unbounded, Bound::Unbounded, Direction::Forward)
                    .unwrap();
                for i in 1..=counter {
                    let value = i.to_be_bytes().to_vec();
                    assert_eq!(
                        entries.next().transpose().unwrap(),
                        Some((value.clone(), value)),
                        "inconsistent checkpoint {target:?}: counter={counter}, {ks:?} key={i}"
                    );
                }
                assert!(
                    entries.next().is_none(),
                    "checkpoint {target:?}: {ks:?} has keys beyond counter={counter}"
                );
            }
        }
        println!("checkpoint counters: {counters:?}");
        assert!(counters.windows(2).any(|pair| pair[0] != pair[1]));
    }

    #[test]
    fn checkpoint_rejects_existing_target_and_lists_files() {
        let dir = TestDir::new();
        let db = RocksKv::open(&dir.0.join("source"), 8 * 1024 * 1024).unwrap();
        db.write(single(), Durability::Sync).unwrap();
        assert!(matches!(db.checkpoint(&dir.0), Err(KvError::Io(_))));
        let target = dir.0.join("copy");
        let files = db.checkpoint(&target).unwrap();
        let mut expected: Vec<_> = std::fs::read_dir(&target)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                assert!(entry.file_type().unwrap().is_file());
                (
                    entry.file_name().into_string().unwrap(),
                    entry.metadata().unwrap().len(),
                )
            })
            .collect();
        expected.sort_by(|a, b| a.0.cmp(&b.0));
        assert!(!files.is_empty());
        assert_eq!(files, expected);
        assert!(matches!(db.checkpoint(&target), Err(KvError::Io(_))));
    }

    #[test]
    #[ignore = "writes 1 GiB to measure live checkpoint cost"]
    fn checkpoint_cost_1gb() {
        let dir = TestDir::new();
        let db = RocksKv::open(&dir.0.join("source"), 8 * 1024 * 1024).unwrap();
        const VALUE_BYTES: usize = 64 * 1024;
        const TOTAL_BYTES: usize = 1024 * 1024 * 1024;
        for group in 0..TOTAL_BYTES / VALUE_BYTES / 16 {
            let mut batch = WriteBatch::default();
            for offset in 0..16 {
                let mut value = vec![0; VALUE_BYTES];
                getrandom::fill(&mut value).unwrap();
                batch.put(
                    Keyspace::Rows,
                    ((group * 16 + offset) as u64).to_be_bytes().to_vec(),
                    value,
                );
            }
            db.write(batch, Durability::NoSync).unwrap();
        }
        db.flush_barrier().unwrap();
        let started = std::time::Instant::now();
        let files = db.checkpoint(&dir.0.join("copy")).unwrap();
        let elapsed = started.elapsed();
        let listed_bytes: u64 = files.iter().map(|(_, bytes)| bytes).sum();
        println!(
            "checkpoint_cost_1gb: payload_bytes={TOTAL_BYTES}, elapsed={elapsed:?}, total_listed_bytes={listed_bytes} (logical bytes; hard-link identity unavailable via portable stable std)"
        );
    }

    #[cfg(windows)]
    fn crash_batch(number: u32) -> WriteBatch {
        let mut batch = WriteBatch::default();
        batch.put(
            Keyspace::Meta,
            number.to_be_bytes().to_vec(),
            b"committed".to_vec(),
        );
        for index in 0..10u32 {
            let key = [number.to_be_bytes(), index.to_be_bytes()].concat();
            batch.put(Keyspace::Rows, key, number.to_be_bytes().to_vec());
        }
        batch
    }

    #[cfg(windows)]
    #[test]
    fn process_crash_child() {
        use std::io::Write;
        let Some(path) = std::env::var_os("GIGGLES_ROCKSDB_CRASH_CHILD") else {
            return;
        };
        let db = RocksKv::open(Path::new(&path), 8 * 1024 * 1024).unwrap();
        for number in 0..200u32 {
            db.write(crash_batch(number), Durability::Sync).unwrap();
            println!("batch:{number}");
            std::io::stdout().flush().unwrap();
            thread::sleep(std::time::Duration::from_millis(2));
        }
    }

    #[cfg(windows)]
    #[test]
    fn process_crash_sync_batches() {
        use std::{
            io::{BufRead, BufReader},
            process::{Command, Stdio},
            time::Duration,
        };
        struct KillOnDrop(std::process::Child);
        impl Drop for KillOnDrop {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        for iteration in 1..=3 {
            let dir = TestDir::new();
            let mut random = [0; 8];
            getrandom::fill(&mut random).unwrap();
            let seed = u64::from_le_bytes(random);
            // SplitMix64 makes the threshold reproducible from the reported seed.
            let mut mixed = seed.wrapping_add(0x9e3779b97f4a7c15);
            mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
            mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d049bb133111eb);
            mixed ^= mixed >> 31;
            let stop_after = 50 + (mixed % 131) as usize;
            println!("crash iteration {iteration}: seed={seed}, kill_threshold={stop_after}");
            let mut child = KillOnDrop(
                Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "storage::engine::kv_rocksdb::tests::process_crash_child",
                        "--nocapture",
                        "--test-threads=1",
                    ])
                    .env("GIGGLES_ROCKSDB_CRASH_CHILD", &dir.0)
                    .stdout(Stdio::piped())
                    .spawn()
                    .unwrap(),
            );
            let stdout = child.0.stdout.take().unwrap();
            let (lines, receive) = mpsc::channel();
            let reader = thread::spawn(move || {
                for line in BufReader::new(stdout).lines() {
                    if lines.send(line.unwrap()).is_err() {
                        break;
                    }
                }
            });
            let mut printed = Vec::new();
            while printed.len() < stop_after {
                let line = receive.recv_timeout(Duration::from_secs(60)).unwrap();
                if let Some((_, number)) = line.split_once("batch:") {
                    printed.push(number.parse::<u32>().unwrap());
                }
            }
            child.0.kill().unwrap();
            assert!(!child.0.wait().unwrap().success());
            reader.join().unwrap();
            for line in receive {
                if let Some((_, number)) = line.split_once("batch:") {
                    printed.push(number.parse::<u32>().unwrap());
                }
            }
            assert!(
                printed.contains(&0),
                "child did not acknowledge its first batch"
            );
            let db = RocksKv::open(&dir.0, 8 * 1024 * 1024).unwrap();
            let mut recovered_complete = 0;
            for number in 0..200u32 {
                let marker = db.get(Keyspace::Meta, &number.to_be_bytes()).unwrap();
                let present = marker.is_some();
                if present {
                    assert_eq!(marker.unwrap(), b"committed");
                    recovered_complete += 1;
                }
                if printed.contains(&number) {
                    assert!(present, "acknowledged batch {number} missing");
                }
                for index in 0..10u32 {
                    let key = [number.to_be_bytes(), index.to_be_bytes()].concat();
                    assert_eq!(
                        db.get(Keyspace::Rows, &key).unwrap(),
                        present.then(|| number.to_be_bytes().to_vec()),
                        "partial batch {number}, item {index}"
                    );
                }
            }
            println!(
                "crash iteration {iteration}: seed={seed}, kill_threshold={stop_after}, acknowledged_count={}, recovered_complete_batch_count={recovered_complete}",
                printed.len()
            );
        }
    }
}
