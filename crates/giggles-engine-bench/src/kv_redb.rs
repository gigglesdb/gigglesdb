use crate::kv::{Backend, KvIter, Result, WriteOp};
use redb::{Database, Durability, ReadableDatabase, TableDefinition};
use std::{
    any::Any,
    path::{Path, PathBuf},
    sync::RwLock,
};
const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("kv");
pub struct Redb {
    db: RwLock<Database>,
    dir: PathBuf,
}
impl Redb {
    pub fn open(dir: &Path, cache: usize) -> Result<Self> {
        let db = Database::builder()
            .set_cache_size(cache)
            .create(dir.join("data.redb"))?;
        let tx = db.begin_write()?;
        {
            tx.open_table(TABLE)?;
        }
        tx.commit()?;
        Ok(Self {
            db: RwLock::new(db),
            dir: dir.into(),
        })
    }
}
impl Backend for Redb {
    fn write_batch(&self, ops: &[WriteOp], durable: bool) -> Result<()> {
        let db = self.db.read().map_err(|_| "redb database lock poisoned")?;
        let mut tx = db.begin_write()?;
        tx.set_durability(if durable {
            Durability::Immediate
        } else {
            Durability::None
        })?;
        {
            let mut table = tx.open_table(TABLE)?;
            for (k, v) in ops {
                match v {
                    Some(v) => {
                        table.insert(k.as_slice(), v.as_slice())?;
                    }
                    None => {
                        table.remove(k.as_slice())?;
                    }
                }
            }
        }
        tx.commit()?;
        Ok(())
    }
    fn iter_from(&self, key: &[u8]) -> Result<KvIter<'_>> {
        let tx = self
            .db
            .read()
            .map_err(|_| "redb database lock poisoned")?
            .begin_read()?;
        let table = tx.open_table(TABLE)?;
        let range = table.range(key..)?;
        Ok(Box::new(range.map(|item| {
            let (k, v) = item?;
            Ok((k.value().to_vec(), v.value().to_vec()))
        })))
    }
    fn disk_bytes(&self) -> Result<u64> {
        crate::kv::directory_bytes(&self.dir)
    }
    fn flush(&self) -> Result<()> {
        let db = self.db.read().map_err(|_| "redb database lock poisoned")?;
        let mut tx = db.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        tx.commit()?;
        Ok(())
    }
    fn pin_snapshot(&self) -> Result<Option<Box<dyn Any + Send + Sync>>> {
        Ok(Some(Box::new(
            self.db
                .read()
                .map_err(|_| "redb database lock poisoned")?
                .begin_read()?,
        )))
    }
    fn compact(&self) -> Result<()> {
        self.db
            .write()
            .map_err(|_| "redb database lock poisoned")?
            .compact()?;
        Ok(())
    }
    fn supports_compaction(&self) -> bool {
        true
    }
}
