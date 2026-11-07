use std::{any::Any, error::Error, path::Path, sync::Arc};
pub type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
pub type KvPair = (Vec<u8>, Vec<u8>);
pub type WriteOp = (Vec<u8>, Option<Vec<u8>>);
pub type KvIter<'a> = Box<dyn Iterator<Item = Result<KvPair>> + Send + 'a>;
pub trait Backend: Send + Sync {
    fn write_batch(&self, ops: &[WriteOp], durable: bool) -> Result<()>;
    fn seek_first_at_or_after(&self, key: &[u8]) -> Result<Option<KvPair>> {
        self.iter_from(key)?.next().transpose()
    }
    fn iter_from(&self, start_key: &[u8]) -> Result<KvIter<'_>>;
    fn disk_bytes(&self) -> Result<u64>;
    fn flush(&self) -> Result<()>;
    fn pin_snapshot(&self) -> Result<Option<Box<dyn Any + Send + Sync>>> {
        Ok(None)
    }
    fn compact(&self) -> Result<()> {
        Ok(())
    }
    fn supports_compaction(&self) -> bool {
        false
    }
}
pub fn open(engine: &str, dir: &Path, cache_bytes: usize) -> Result<Arc<dyn Backend>> {
    std::fs::create_dir_all(dir)?;
    match engine {
        "redb" => Ok(Arc::new(crate::kv_redb::Redb::open(dir, cache_bytes)?)),
        "fjall" => Ok(Arc::new(crate::kv_fjall::Fjall::open(dir, cache_bytes)?)),
        #[cfg(feature = "rocksdb")]
        "rocksdb" => Ok(Arc::new(crate::kv_rocksdb::RocksDb::open(
            dir,
            cache_bytes,
        )?)),
        #[cfg(not(feature = "rocksdb"))]
        "rocksdb" => Err("rocksdb requires building with --features rocksdb".into()),
        _ => Err(format!("unknown engine: {engine}").into()),
    }
}
pub fn engine_version(engine: &str) -> &str {
    match engine {
        "redb" => "3.1.3",
        "fjall" => "3.1.13",
        "rocksdb" => "0.25.0",
        _ => "unknown",
    }
}
pub(crate) fn directory_bytes(path: &Path) -> Result<u64> {
    let mut size = 0;
    // Background compaction can unlink files or directories during this sample.
    let entries = match std::fs::read_dir(path) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e.into()),
    };
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        };
        let metadata = match entry.metadata() {
            Ok(metadata) => metadata,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        };
        size += if metadata.is_dir() {
            directory_bytes(&entry.path())?
        } else {
            metadata.len()
        };
    }
    Ok(size)
}
