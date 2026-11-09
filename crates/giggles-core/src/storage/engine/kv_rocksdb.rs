//! Optional RocksDB implementation of the storage-engine KV seam.
use super::kv::*;
use rocksdb::{
    BlockBasedOptions, Cache, DB, DBCompressionType, ErrorKind, IteratorMode, Options, WriteOptions,
};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions, TryLockError},
    io::{Read, Seek, SeekFrom, Write},
    ops::{Bound, Deref},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, mpsc},
    thread::{self, JoinHandle},
    time::{SystemTime, UNIX_EPOCH},
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

struct LockedDb {
    // Field order matters: close RocksDB before releasing the explicit lock.
    db: DB,
    _writer_lock: WriterLock,
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

impl RocksKv {
    pub(crate) fn open(path: &Path, cache_bytes: usize) -> Result<Self, KvError> {
        std::fs::create_dir_all(path).map_err(|error| KvError::Io(error.to_string()))?;
        Self::open_locked(path, cache_bytes, WriterLock::acquire(path)?)
    }

    pub(crate) fn open_locked(
        path: &Path,
        cache_bytes: usize,
        writer_lock: WriterLock,
    ) -> Result<Self, KvError> {
        let cache = Cache::new_lru_cache(cache_bytes);
        let mut table = BlockBasedOptions::default();
        table.set_block_cache(&cache);
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        options.set_compression_type(DBCompressionType::Lz4);
        options.set_block_based_table_factory(&table);
        let db = DB::open_cf_with_opts(
            &options,
            native_path(path),
            Keyspace::ALL.map(|ks| (ks.name(), options.clone())),
        )
        .map_err(map_error)?;
        Ok(Self {
            db: Arc::new(LockedDb {
                db,
                _writer_lock: writer_lock,
            }),
        })
    }
}

fn map_error(error: rocksdb::Error) -> KvError {
    if error.kind() == ErrorKind::Corruption {
        KvError::Corrupt(error.to_string())
    } else {
        KvError::Io(error.to_string())
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
    Box::new(std::iter::from_fn(move || {
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
        self.db.write_opt(native, &options).map_err(map_error)
    }

    fn flush_barrier(&self) -> Result<(), KvError> {
        self.db.flush_wal(true).map_err(map_error)
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
        Ok(Box::new(SnapshotIter {
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
mod tests {
    use super::*;
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

    fn single() -> WriteBatch {
        let mut batch = WriteBatch::default();
        for ks in Keyspace::ALL {
            batch.put(ks, b"key".to_vec(), b"value".to_vec());
        }
        batch
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

    fn reopen(durability: Durability) {
        let dir = TestDir::new();
        {
            let db = RocksKv::open(&dir.0, 8 * 1024 * 1024).unwrap();
            db.write(single(), durability).unwrap();
            if durability == Durability::NoSync {
                db.flush_barrier().unwrap();
            }
        }
        let db = RocksKv::open(&dir.0, 8 * 1024 * 1024).unwrap();
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
