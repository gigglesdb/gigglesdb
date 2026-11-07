use crate::kv::{Backend, KvIter, Result, WriteOp};
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};
use std::{
    any::Any,
    path::{Path, PathBuf},
};
pub struct Fjall {
    db: Database,
    tree: Keyspace,
    dir: PathBuf,
}
impl Fjall {
    pub fn open(dir: &Path, cache: usize) -> Result<Self> {
        let db = Database::builder(dir).cache_size(cache as u64).open()?;
        let tree = db.keyspace("kv", KeyspaceCreateOptions::default)?;
        Ok(Self {
            db,
            tree,
            dir: dir.into(),
        })
    }
}
impl Backend for Fjall {
    fn write_batch(&self, ops: &[WriteOp], durable: bool) -> Result<()> {
        let mut batch = self.db.batch().durability(if durable {
            Some(PersistMode::SyncAll)
        } else {
            None
        });
        for (k, v) in ops {
            match v {
                Some(v) => batch.insert(&self.tree, k.as_slice(), v.as_slice()),
                None => batch.remove(&self.tree, k.as_slice()),
            }
        }
        batch.commit()?;
        Ok(())
    }
    fn iter_from(&self, key: &[u8]) -> Result<KvIter<'_>> {
        Ok(Box::new(self.tree.range(key..).map(|item| {
            let (k, v) = item.into_inner()?;
            Ok((k.to_vec(), v.to_vec()))
        })))
    }
    fn disk_bytes(&self) -> Result<u64> {
        crate::kv::directory_bytes(&self.dir)
    }
    fn flush(&self) -> Result<()> {
        self.db.persist(PersistMode::SyncAll)?;
        Ok(())
    }
    fn pin_snapshot(&self) -> Result<Option<Box<dyn Any + Send + Sync>>> {
        Ok(Some(Box::new(self.db.snapshot())))
    }
    fn compact(&self) -> Result<()> {
        self.tree.major_compact()?;
        Ok(())
    }
    fn supports_compaction(&self) -> bool {
        true
    }
}
