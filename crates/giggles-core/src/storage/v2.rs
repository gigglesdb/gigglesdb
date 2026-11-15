//! Minimal v2 lifecycle. Public legacy operations fail before admission or mutation.
#[cfg(feature = "engine-rocksdb")]
use super::io;
use super::{CoreError, engine};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{fs, path::Path};

#[path = "reclaim.rs"]
pub(super) mod reclaim;

#[allow(dead_code)]
pub(crate) struct V2Db {
    pub(crate) kv: Arc<engine::kv::Kv>,
    pub(crate) meta: engine::meta::MetaInfo,
    pub(crate) sequencer: Arc<engine::commit::Sequencer>,
    locks: Arc<engine::locks::LockManager>,
    catalog: engine::catalog::Catalog,
    pub(crate) rows: engine::rows::RowStore,
    pub(crate) sequences: Arc<engine::seq::SequenceAllocator>,
    pub(crate) sql_counters:
        std::sync::Mutex<std::collections::BTreeMap<u64, super::sequences::SeqCounter>>,
    reclaim: std::sync::Mutex<reclaim::Registry>,
    pub(super) reclaim_errors: AtomicU64,
    binding_fences: std::sync::Mutex<std::collections::BTreeMap<(u64, u64, u64), u64>>,
    next_txn: AtomicU64,
}
pub(super) fn format_error(error: engine::meta::FormatError) -> CoreError {
    match error {
        engine::meta::FormatError::Corrupt(message)
        | engine::meta::FormatError::Incomplete(message) => CoreError::Corrupt(message),
        engine::meta::FormatError::Unsupported(_) => CoreError::Unsupported,
    }
}
pub(super) fn has_traces(path: &Path) -> bool {
    let Ok(entries) = fs::read_dir(path) else {
        return false;
    };
    for entry in entries.flatten() {
        if !entry.file_type().is_ok_and(|kind| kind.is_file()) {
            continue;
        }
        if let Some(name) = entry.file_name().to_str()
            && (matches!(name, "GIGGLES_FORMAT" | "GIGGLES_FORMAT.tmp")
                || engine::meta::rocks_file(name))
        {
            return true;
        }
    }
    false
}

#[cfg(feature = "engine-rocksdb")]
pub(crate) const DEFAULT_WRITE_SET_BYTES: usize = 1024 * 1024 * 1024;

impl V2Db {
    #[cfg(feature = "engine-rocksdb")]
    pub(crate) fn create(path: &Path) -> Result<Self, CoreError> {
        let mut domain = [0u8; 16];
        getrandom::fill(&mut domain).map_err(io)?;
        fs::create_dir_all(path).map_err(io)?;
        let writer_lock = engine::kv_rocksdb::WriterLock::acquire(path)?;
        let backend = engine::meta::create(path, domain, |path| {
            Ok(Arc::new(engine::kv_rocksdb::RocksKv::open_locked(
                path,
                64 * 1024 * 1024,
                writer_lock,
            )?))
        })
        .map_err(format_error)?;
        Self::from_kv(Arc::new(engine::kv::Kv::new(backend)))
    }
    #[cfg(feature = "engine-rocksdb")]
    fn from_kv(kv: Arc<engine::kv::Kv>) -> Result<Self, CoreError> {
        Self::from_kv_with_limit(kv, DEFAULT_WRITE_SET_BYTES, false)
    }
    #[cfg(feature = "engine-rocksdb")]
    pub(crate) fn from_backend(
        backend: Arc<dyn engine::kv::KvBackend>,
        write_set_bytes: usize,
    ) -> Result<Self, CoreError> {
        Self::from_kv_with_limit(
            Arc::new(engine::kv::Kv::new(backend)),
            write_set_bytes,
            true,
        )
    }
    #[cfg(feature = "engine-rocksdb")]
    fn from_kv_with_limit(
        kv: Arc<engine::kv::Kv>,
        write_set_bytes: usize,
        replicated_allowed: bool,
    ) -> Result<Self, CoreError> {
        let meta = engine::meta::open_checks(kv.as_ref()).map_err(format_error)?;
        if meta.features & engine::meta::REPLICATED != 0 && !replicated_allowed {
            return Err(CoreError::Unsupported);
        }
        if !meta.stale_counters.is_empty() {
            let mut batch = engine::kv::WriteBatch::default();
            for key in &meta.stale_counters {
                batch.delete(engine::kv::Keyspace::Seq, key.clone());
            }
            kv.write(batch, engine::kv::Durability::Sync)?;
        }
        let sequencer = engine::commit::Sequencer::open(
            kv.clone(),
            Arc::new(engine::commit::SystemClock),
            engine::commit::SequencerConfig {
                // Atomic paged DML retains transaction writes and savepoint undo.
                write_set_bytes,
                ..Default::default()
            },
        )?;
        let locks = Arc::new(engine::locks::LockManager::new(engine::locks::LockConfig {
            // One million rows plus old/new keys for secondary indexes.
            max_row_locks_per_txn: 8_000_000,
            ..Default::default()
        }));
        let sequences = Arc::new(engine::seq::SequenceAllocator::new(kv.clone(), 0));
        let catalog =
            engine::catalog::Catalog::open(sequencer.clone(), locks.clone(), sequences.clone())?;
        let db = Self {
            kv,
            meta,
            sequencer,
            locks,
            catalog,
            rows: engine::rows::RowStore::new(sequences.clone()),
            sequences,
            sql_counters: Default::default(),
            reclaim: Default::default(),
            reclaim_errors: AtomicU64::new(0),
            binding_fences: Default::default(),
            next_txn: AtomicU64::new(1),
        };
        db.reclaim_background()?;
        Ok(db)
    }

    // F8a-3b routes v2 SQL sessions through this.
    #[allow(dead_code)]
    pub(crate) fn begin(&self, isolation: engine::txn::Isolation) -> engine::txn::Transaction {
        let id = self
            .next_txn
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .expect("transaction ID space exhausted");
        engine::txn::Transaction::begin(
            &self.sequencer,
            &self.locks,
            engine::locks::TxnId(id),
            isolation,
        )
    }

    // F8a-3b routes v2 SQL sessions through this.
    #[allow(dead_code)]
    pub(crate) fn catalog(&self) -> &engine::catalog::Catalog {
        &self.catalog
    }

    /// Opaque in-process identities. Global allocation prevents reuse after reopen.
    pub(super) fn inspect_binding<T>(
        &self,
        txn: Option<&engine::txn::Transaction>,
        inspect: impl FnOnce(
            u128,
            u64,
            &std::collections::BTreeMap<String, crate::relational::TableDef>,
        ) -> T,
    ) -> Result<T, CoreError> {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let committed;
        let txn = match txn {
            Some(txn) => txn,
            None => {
                committed = self.begin(engine::txn::Isolation::ReadCommitted);
                &committed
            }
        };
        let (catalog, version) = self.catalog.binding_snapshot(txn)?;
        let version = version.ok_or(CoreError::Sql(crate::relational::SqlError::StaleSchema))?;
        let generation = txn.catalog_generation();
        let key = (
            version.0,
            if generation == 0 { 0 } else { txn.id().0 },
            generation,
        );
        let fence = *self
            .binding_fences
            .lock()
            .map_err(|_| CoreError::Corrupt("binding fence mutex poisoned".into()))?
            .entry(key)
            .or_insert_with(|| {
                NEXT.try_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
                    .expect("binding fence exhausted")
            });
        let image = catalog.image().map_err(CoreError::Sql)?;
        crate::cancellation::check()?;
        let result = inspect(u128::from_be_bytes(self.meta.domain), fence, &image.tables);
        crate::cancellation::check()?;
        Ok(result)
    }

    /// Rows-free image used only by catalog/authority metadata helpers.
    pub(super) fn metadata_state(
        &self,
        txn: Option<&engine::txn::Transaction>,
    ) -> Result<super::State, CoreError> {
        self.with_metadata_catalog(txn, |catalog, epoch| {
            let mut state = super::empty(u128::from_be_bytes(self.meta.domain));
            state.origin = self.origin_at(engine::hlc::Ts(epoch))?;
            state.sql = (*catalog.image().map_err(CoreError::Sql)?).clone();
            state.sql.epoch = epoch;
            Ok(state)
        })
    }

    /// Metadata helpers borrow the validated image rather than cloning all its
    /// tables for every name, type, default or authorization dependency lookup.
    pub(super) fn inspect_metadata<T>(
        &self,
        txn: Option<&engine::txn::Transaction>,
        inspect: impl FnOnce(&super::MetadataState<'_>) -> Result<T, CoreError>,
    ) -> Result<T, CoreError> {
        self.with_metadata_catalog(txn, |catalog, epoch| {
            // Preserve the owned metadata API's provenance read and its errors.
            self.origin_at(engine::hlc::Ts(epoch))?;
            let image = catalog.image().map_err(CoreError::Sql)?;
            let mut state = super::MetadataState::new(
                &image,
                epoch,
                u128::from_be_bytes(self.meta.domain),
            );
            state.sql.comments = Some(&catalog.parts.comments);
            state.sql.views = Some(&catalog.parts.sql_views);
            inspect(&state)
        })
    }

    fn with_metadata_catalog<T>(
        &self,
        txn: Option<&engine::txn::Transaction>,
        inspect: impl FnOnce(Arc<engine::catalog::CatalogSnapshot>, u64) -> Result<T, CoreError>,
    ) -> Result<T, CoreError> {
        let mut committed;
        let (catalog, epoch) = match txn {
            Some(txn) => (
                self.catalog.metadata_snapshot(txn)?,
                txn.current_snapshot_ts()
                    .unwrap_or_else(|| self.sequencer.visible_ts())
                    .0,
            ),
            None => {
                committed = self.begin(engine::txn::Isolation::ReadCommitted);
                let catalog = self.catalog.snapshot(&mut committed)?;
                (catalog, committed.snapshot_ts().0)
            }
        };
        inspect(catalog, epoch)
    }
    /// Older SQL-only v2 databases have no kernel image. When present, its
    /// existing metadata record carries restore provenance.
    pub(super) fn origin_at(&self, ts: engine::hlc::Ts) -> Result<Option<u128>, CoreError> {
        let Some(bytes) =
            engine::mvcc::read_at(self.kv.as_ref(), engine::kv::Keyspace::Kernel, &[0], ts)?
        else {
            return Ok(None);
        };
        let metadata: super::Metadata = super::decode(&bytes)?;
        if metadata.domain.to_be_bytes() != self.meta.domain {
            return Err(CoreError::Corrupt("kernel metadata domain mismatch".into()));
        }
        Ok(metadata.origin)
    }

    pub(crate) fn open(path: &Path) -> Result<Self, CoreError> {
        if engine::meta::classify_dir(path).map_err(format_error)? != engine::meta::DirClass::V2 {
            return Err(CoreError::Corrupt("not a v2 directory".into()));
        }
        #[cfg(not(feature = "engine-rocksdb"))]
        {
            Err(CoreError::EngineUnsupported {
                engine: "v2",
                feature: super::ROCKSDB_NOT_COMPILED,
            })
        }
        #[cfg(feature = "engine-rocksdb")]
        {
            let kv = Arc::new(engine::kv::Kv::new(Arc::new(
                engine::kv_rocksdb::RocksKv::open(path, 64 * 1024 * 1024)?,
            )));
            Self::from_kv(kv)
        }
    }
}
#[cfg(test)]
#[path = "v2_tests.rs"]
mod tests;

#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "v2_metadata_tests.rs"]
mod metadata_tests;

#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "v2_catalog_snapshot_tests.rs"]
mod catalog_snapshot_tests;

#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "v2_backend_tests.rs"]
mod backend_tests;
