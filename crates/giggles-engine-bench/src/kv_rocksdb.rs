use crate::kv::{Backend, KvIter, Result, WriteOp};
use rocksdb::{
    BlockBasedOptions, Cache, DB, DBCompressionType, Direction, IteratorMode, Options, WriteBatch,
    WriteOptions,
};
use std::{
    any::Any,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, mpsc},
    thread::{self, JoinHandle},
};

pub struct RocksDb {
    db: Arc<DB>,
    dir: PathBuf,
}

impl RocksDb {
    pub fn open(dir: &Path, cache_bytes: usize) -> Result<Self> {
        let cache = Cache::new_lru_cache(cache_bytes);
        let mut table = BlockBasedOptions::default();
        table.set_block_cache(&cache);
        table.set_bloom_filter(10.0, false);
        table.set_cache_index_and_filter_blocks(true);
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_block_based_table_factory(&table);
        options.set_compression_type(DBCompressionType::Lz4);
        options.set_max_background_jobs(
            thread::available_parallelism().map_or(1, |n| n.get().min(8)) as i32,
        );
        Ok(Self {
            db: Arc::new(DB::open(&options, native_path(dir))?),
            dir: dir.into(),
        })
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

// The thread owns the DB and borrows it for the actual native snapshot. Its stack
// releases the snapshot before the Arc<DB>; no self-reference or extended lifetime
// is needed. Joining on guard drop also makes database closure/reopen deterministic.
struct SnapshotPin {
    release: Option<mpsc::Sender<()>>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for SnapshotPin {
    fn drop(&mut self) {
        self.release.take();
        if let Some(handle) = self
            .thread
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            let _ = handle.join();
        }
    }
}

impl Backend for RocksDb {
    fn write_batch(&self, ops: &[WriteOp], durable: bool) -> Result<()> {
        let mut batch = WriteBatch::default();
        for (key, value) in ops {
            match value {
                Some(value) => batch.put(key, value),
                None => batch.delete(key),
            }
        }
        let mut options = WriteOptions::default();
        options.set_sync(durable);
        options.disable_wal(false);
        self.db.write_opt(batch, &options)?;
        Ok(())
    }

    fn iter_from(&self, key: &[u8]) -> Result<KvIter<'_>> {
        Ok(Box::new(
            self.db
                .iterator(IteratorMode::From(key, Direction::Forward))
                .map(|item| {
                    let (key, value) = item?;
                    Ok((key.into_vec(), value.into_vec()))
                }),
        ))
    }

    fn disk_bytes(&self) -> Result<u64> {
        crate::kv::directory_bytes(&self.dir)
    }

    fn flush(&self) -> Result<()> {
        self.db.flush_wal(true)?;
        self.db.flush()?;
        Ok(())
    }

    fn pin_snapshot(&self) -> Result<Option<Box<dyn Any + Send + Sync>>> {
        let db = Arc::clone(&self.db);
        let (release, wait) = mpsc::channel();
        let (ready, started) = mpsc::sync_channel(0);
        let handle = thread::Builder::new()
            .name("rocksdb-snapshot-pin".into())
            .spawn(move || {
                let snapshot = db.snapshot();
                if ready.send(()).is_ok() {
                    let _ = wait.recv();
                }
                drop(snapshot);
            })?;
        let pin = SnapshotPin {
            release: Some(release),
            thread: Mutex::new(Some(handle)),
        };
        started.recv()?;
        Ok(Some(Box::new(pin)))
    }

    fn compact(&self) -> Result<()> {
        self.db.compact_range(None::<&[u8]>, None::<&[u8]>);
        Ok(())
    }

    fn supports_compaction(&self) -> bool {
        true
    }
}
