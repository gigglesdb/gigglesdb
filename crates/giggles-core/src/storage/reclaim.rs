//! Physical prefix GC; catalog tombstones already provide durable retirement times.
use super::{V2Db, engine};
use crate::storage::{Admin, Backend, CoreError};
use engine::{
    catalog_codec, indexes,
    kv::{Durability, Keyspace, WriteBatch},
    mvcc,
    txn::Transaction,
};
use std::{ops::Range, sync::Weak};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReclaimStats {
    pub tables: usize,
    pub indexes: usize,
}
#[derive(Default)]
pub(super) struct Registry {
    builds: Vec<(Range<u64>, Weak<engine::txn::TxnLifetime>)>,
    next: [u64; 2],
}
impl Admin {
    /// Reclaim retired v2 table/index prefixes. Legacy storage needs no such GC.
    /// Active snapshots and unpublished builders retain their physical data.
    pub fn reclaim_storage(&self) -> Result<ReclaimStats, CoreError> {
        let _ticket = self.inner.enter_sql()?;
        match &self.inner.backend {
            Backend::V2(slot) => slot.fresh(crate::storage::Holder::Admin)?.reclaim_storage(usize::MAX),
            _ => Ok(ReclaimStats::default()),
        }
    }
}
impl V2Db {
    pub(crate) fn reserve_sql_ids(
        &self,
        txn: &Transaction,
        n: u64,
    ) -> Result<Range<u64>, CoreError> {
        let mut registry = self.reclaim.lock().map_err(|_| CoreError::Fenced)?;
        let ids = self.catalog().reserve_ids(n)?;
        registry.builds.push((ids.clone(), txn.lifetime()));
        Ok(ids)
    }
    // Failed/rolled-back chunk builds are also retried by the open-time pass.
    pub(crate) fn reclaim_background(&self) -> Result<(), CoreError> {
        if let Err(error) = self.reclaim_storage(64) {
            self.reclaim_errors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if matches!(error, CoreError::Corrupt(_)) {
                return Err(error);
            }
        }
        Ok(())
    }

    pub(crate) fn reclaim_storage(&self, limit: usize) -> Result<ReclaimStats, CoreError> {
        // A cluster gateway holds no data: range leaders reclaim their own ranges.
        if self.cluster_services().is_some() {
            return Ok(ReclaimStats::default());
        }
        #[cfg(test)]
        {
            RECLAIM_PASSES.set(RECLAIM_PASSES.get() + 1);
            if let Some(error) = RECLAIM_ERROR.take() {
                return Err(error);
            }
        }
        let mut registry = self.reclaim.lock().map_err(|_| CoreError::Fenced)?;
        registry
            .builds
            .retain(|(_, owner)| owner.strong_count() != 0);
        // Read visible FIRST: a newly registered snapshot cannot be older than it.
        let visible = self.sequencer.visible_ts();
        let horizon = self
            .sequencer
            .oldest_active()
            .unwrap_or(visible)
            .min(visible);
        let mut stats = ReclaimStats::default();
        for (slot, ks) in [Keyspace::Rows, Keyspace::Index].into_iter().enumerate() {
            let mut lower = if limit == usize::MAX {
                0
            } else {
                registry.next[slot]
            };
            for _ in 0..limit {
                let Some((key, _)) = self.kv.seek_ge(ks, &lower.to_be_bytes())? else {
                    registry.next[slot] = 0;
                    break;
                };
                let id = u64::from_be_bytes(
                    key.get(..8)
                        .ok_or_else(|| CoreError::Corrupt("short GC prefix".into()))?
                        .try_into()
                        .unwrap(),
                );
                let Some(next) = id.checked_add(1) else { break };
                lower = next;
                registry.next[slot] = next;
                if registry.builds.iter().any(|(ids, _)| ids.contains(&id)) {
                    continue;
                }
                let catalog_key = if ks == Keyspace::Rows {
                    catalog_codec::table_key(id)
                } else {
                    indexes::descriptor_key(id)
                };
                // IDs never recur. A latest tombstone at/before the horizon proves
                // absence for ALL active snapshots; no record denotes an orphan.
                if mvcc::latest_version(self.kv.as_ref(), Keyspace::Catalog, &catalog_key)?
                    .is_some_and(|(ts, value)| value.is_some() || ts > horizon)
                {
                    continue;
                }
                let mut batch = WriteBatch::default();
                batch.delete_range(ks, id.to_be_bytes().to_vec(), next.to_be_bytes().to_vec());
                self.kv.write(batch, Durability::Sync)?;
                if ks == Keyspace::Rows {
                    stats.tables += 1;
                } else {
                    stats.indexes += 1;
                }
                #[cfg(test)]
                if CRASH_AFTER_RECLAIM.replace(false) {
                    crate::storage::crash_now();
                }
            }
        }
        Ok(stats)
    }
}
#[cfg(test)]
thread_local! {
    pub(crate) static RECLAIM_PASSES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    pub(crate) static RECLAIM_ERROR: std::cell::Cell<Option<CoreError>> = const { std::cell::Cell::new(None) };
    pub(crate) static CRASH_AFTER_RECLAIM: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
