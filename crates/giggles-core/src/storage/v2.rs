//! Minimal v2 lifecycle. Public legacy operations fail before admission or mutation.
#[cfg(feature = "engine-rocksdb")]
use super::io;
use super::{CoreError, engine};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{fs, path::Path};

#[path = "reclaim.rs"]
pub(super) mod reclaim;

#[path = "v2_maintenance.rs"]
pub(super) mod maintenance;

/// A stable allocation identity: entries are never replaced while the DB is open.
#[derive(Default)]
pub(crate) struct SeqSlot {
    pub(crate) counter: std::sync::Mutex<Option<super::sequences::SeqCounter>>,
}

#[allow(dead_code)]
pub(crate) struct V2Db {
    // First field: dropping it stops and joins the GC thread before the rest
    // of the database (and its KV handle) is released.
    maintenance: maintenance::Maintenance,
    pub(crate) gc: Arc<engine::gc::Gc>,
    pub(crate) kv: Arc<engine::kv::Kv>,
    pub(crate) meta: engine::meta::MetaInfo,
    pub(crate) sequencer: Arc<engine::commit::Sequencer>,
    locks: Arc<engine::locks::LockManager>,
    catalog: engine::catalog::Catalog,
    pub(crate) rows: engine::rows::RowStore,
    pub(crate) sequences: Arc<engine::seq::SequenceAllocator>,
    pub(crate) xacts: engine::xact::XactIds,
    pub(crate) sql_counters:
        std::sync::Mutex<std::collections::BTreeMap<u64, Arc<SeqSlot>>>,
    reclaim: std::sync::Mutex<reclaim::Registry>,
    pub(super) reclaim_errors: AtomicU64,
    binding_fences: std::sync::Mutex<std::collections::BTreeMap<(u64, u64, u64), u64>>,
    next_txn: AtomicU64,
    /// Set on a cluster gateway (GD-0075 R5-43): transactions, the catalog, allocators and
    /// sequences go to range leaders, and `kv`/`sequencer` are fenced placeholders.
    cluster: Option<Arc<dyn engine::txn::ClusterServices>>,
    /// The one wall clock for task expiry, receipts and retention (GD-0061
    /// ruling R7); tests replace it with a manual clock.
    wall_clock: std::sync::RwLock<Arc<dyn engine::commit::Clock>>,
    /// Bound task visible-rows predicates (GD-0061 R3-305).
    pub(crate) row_predicates: crate::task::rows::PredicateCache,
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
        Self::from_kv_with_limit(kv, DEFAULT_WRITE_SET_BYTES, false, None)
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
            None,
        )
    }
    #[cfg(feature = "engine-rocksdb")]
    fn from_kv_with_limit(
        kv: Arc<engine::kv::Kv>,
        write_set_bytes: usize,
        replicated_allowed: bool,
        token: Option<&[u8]>,
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
        #[cfg_attr(not(test), allow(unused_mut))]
        let mut config = engine::commit::SequencerConfig {
            // Atomic paged DML retains transaction writes and savepoint undo.
            write_set_bytes,
            // Replicated groups must fit the same budget as individual writes.
            group_max_bytes: replicated_allowed.then_some(write_set_bytes),
            ..Default::default()
        };
        #[cfg(test)]
        if let Some(bytes) = TEST_RECENT_LOG_BYTES.get() {
            config.recent_log_bytes = bytes;
        }
        let sequencer = engine::commit::Sequencer::open_with_holder(
            kv.clone(),
            Arc::new(engine::commit::SystemClock),
            config,
            token,
        )?;
        let locks = Arc::new(engine::locks::LockManager::new(engine::locks::LockConfig {
            // One million rows plus old/new keys for secondary indexes.
            max_row_locks_per_txn: 8_000_000,
            ..Default::default()
        }));
        let mut sequences = engine::seq::SequenceAllocator::new(kv.clone(), 0);
        sequences.set_sequencer(sequencer.clone());
        let sequences = Arc::new(sequences);
        let xacts = engine::xact::XactIds::new(kv.clone(), sequences.clone());
        let catalog =
            engine::catalog::Catalog::open(sequencer.clone(), locks.clone(), sequences.clone())?;
        let gc = Arc::new(engine::gc::Gc::open(
            kv.clone(),
            sequencer.clone(),
            engine::gc::GcConfig {
                retention: maintenance::RETENTION,
            },
        )?);
        let replicated = meta.features & engine::meta::REPLICATED != 0;
        let db = Self {
            maintenance: maintenance::Maintenance::start(&gc, replicated)?,
            gc,
            kv,
            meta,
            sequencer,
            locks,
            catalog,
            rows: engine::rows::RowStore::new(sequences.clone()),
            sequences,
            xacts,
            sql_counters: Default::default(),
            reclaim: Default::default(),
            reclaim_errors: AtomicU64::new(0),
            binding_fences: Default::default(),
            next_txn: AtomicU64::new(1),
            cluster: None,
            wall_clock: std::sync::RwLock::new(Arc::new(engine::commit::SystemClock)),
            row_predicates: Default::default(),
        };
        db.reclaim_background()?;
        Ok(db)
    }

    /// A gateway's database in a cluster (GD-0075 R5-43): it holds no data. Transactions use
    /// `services` (oracle timestamps, range locks and commits), the catalog loads through the
    /// routed reader from the system range, and object/row/xid ids and SQL sequences come from
    /// the system range leader. The local store and sequencer exist only so code that has no
    /// cluster branch fails loudly: the store is fenced, so every read or write of it fails.
    /// Reclaim does not run here; range leaders own their data.
    #[cfg(feature = "engine-rocksdb")]
    pub(crate) fn cluster(
        services: Arc<dyn engine::txn::ClusterServices>,
        domain: [u8; 16],
    ) -> Result<Self, CoreError> {
        let kv = Arc::new(engine::kv::Kv::new(Arc::new(
            engine::kv_mem::MemKv::new(),
        )));
        let sequencer = engine::commit::Sequencer::open(
            kv.clone(),
            Arc::new(engine::commit::SystemClock),
            engine::commit::SequencerConfig::default(),
        )?;
        // No GC on a gateway: range leaders own their data. Opened before the fence
        // (the empty store has no watermark); `replicated` starts no thread.
        let gc = Arc::new(engine::gc::Gc::open(
            kv.clone(),
            sequencer.clone(),
            engine::gc::GcConfig {
                retention: maintenance::RETENTION,
            },
        )?);
        let maintenance = maintenance::Maintenance::start(&gc, true)?;
        kv.fence();
        let client = services.client();
        let sequences = Arc::new(engine::seq::SequenceAllocator::leased(client.clone(), 0));
        let xacts = engine::xact::XactIds::leased(sequences.clone());
        // Catalog object ids are leased per reservation, never from a cached block: every
        // id a DDL allocates must exceed every id already in the catalog (image validation),
        // and DDL across gateways is serialized (Phase 11), so lease order is commit order.
        let catalog = engine::catalog::Catalog::open_with(
            engine::txn::TxnServices::Cluster(services.clone()),
            Arc::new(engine::seq::SequenceAllocator::leased(client, 1)),
        )?;
        Ok(Self {
            maintenance,
            gc,
            kv,
            meta: engine::meta::MetaInfo {
                stale_counters: Vec::new(),
                family: 2,
                subversion: engine::meta::CURRENT_SUBVERSION,
                features: 0,
                domain,
                engine: "cluster".into(),
                last_commit_ts: None,
                gc_watermark: None,
                write_fence: None,
            },
            sequencer,
            locks: Arc::new(engine::locks::LockManager::new(Default::default())),
            catalog,
            rows: engine::rows::RowStore::new(sequences.clone()),
            sequences,
            xacts,
            sql_counters: Default::default(),
            reclaim: Default::default(),
            reclaim_errors: AtomicU64::new(0),
            binding_fences: Default::default(),
            next_txn: AtomicU64::new(1),
            cluster: Some(services),
            wall_clock: std::sync::RwLock::new(Arc::new(engine::commit::SystemClock)),
            row_predicates: Default::default(),
        })
    }

    /// The task kernel (protected commit, release debits, read log; GD-0061)
    /// runs on a local, unreplicated v2 database only: a REPLICATED store or a
    /// cluster gateway is 0A000 (D13).
    pub(crate) fn task_kernel_unsupported(&self) -> bool {
        self.meta.features & engine::meta::REPLICATED != 0 || self.cluster.is_some()
    }

    /// The gateway services when this database is a cluster gateway.
    pub(crate) fn cluster_services(&self) -> Option<&Arc<dyn engine::txn::ClusterServices>> {
        self.cluster.as_ref()
    }

    /// Fails with Unsupported on a cluster gateway: for paths that read or write the local
    /// store directly and have no cluster form yet.
    #[cfg_attr(not(feature = "engine-rocksdb"), allow(dead_code))]
    pub(crate) fn require_local(&self) -> Result<(), CoreError> {
        match self.cluster {
            Some(_) => Err(CoreError::Unsupported),
            None => Ok(()),
        }
    }

    /// The newest visible timestamp: the local sequencer's, or a fresh oracle timestamp.
    pub(crate) fn visible_ts(&self) -> Result<engine::hlc::Ts, CoreError> {
        match &self.cluster {
            Some(services) => Ok(services.now()?),
            None => Ok(self.sequencer.visible_ts()),
        }
    }

    // F8a-3b routes v2 SQL sessions through this.
    #[allow(dead_code)]
    pub(crate) fn begin(&self, isolation: engine::txn::Isolation) -> engine::txn::Transaction {
        if let Some(services) = &self.cluster {
            return engine::txn::Transaction::begin_with(
                engine::txn::TxnServices::Cluster(services.clone()),
                services.next_txn_id(),
                isolation,
            );
        }
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

    /// Unix milliseconds from the database's wall clock (GD-0061 ruling R7):
    /// task expiry, receipt `decided_ms` and retention all read this.
    pub(crate) fn wall_now_ms(&self) -> u64 {
        let clock = match self.wall_clock.read() {
            Ok(clock) => Arc::clone(&clock),
            Err(poisoned) => Arc::clone(&poisoned.into_inner()),
        };
        clock.now_ms()
    }

    /// Test hook: replace the wall clock (also behind the hidden
    /// `Admin::set_task_wall_clock`).
    pub(crate) fn set_wall_clock(&self, clock: Arc<dyn engine::commit::Clock>) {
        match self.wall_clock.write() {
            Ok(mut slot) => *slot = clock,
            Err(poisoned) => *poisoned.into_inner() = clock,
        }
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
        self.with_metadata_catalog(txn, |catalog, epoch, ts| {
            let mut state = super::empty(u128::from_be_bytes(self.meta.domain));
            state.origin = self.origin_at(ts)?;
            state.sql = (*catalog.image().map_err(CoreError::Sql)?).clone();
            state.sql.epoch = epoch;
            Ok(state)
        })
    }

    /// The column labels of the catalog snapshot `inspect_metadata` reads
    /// (GD-0061 R3-304: native task requests take their marks from it).
    pub(super) fn column_labels(
        &self,
        txn: Option<&engine::txn::Transaction>,
    ) -> Result<std::collections::BTreeMap<(u64, u64), u8>, CoreError> {
        self.with_metadata_catalog(txn, |catalog, _, _| Ok(catalog.parts.column_labels.clone()))
    }

    /// Metadata helpers borrow the validated image rather than cloning all its
    /// tables for every name, type, default or authorization dependency lookup.
    pub(super) fn inspect_metadata<T>(
        &self,
        txn: Option<&engine::txn::Transaction>,
        inspect: impl FnOnce(&super::MetadataState<'_>) -> Result<T, CoreError>,
    ) -> Result<T, CoreError> {
        self.with_metadata_catalog(txn, |catalog, epoch, ts| {
            // Preserve the owned metadata API's provenance read and its errors.
            self.origin_at(ts)?;
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

    /// Passes the catalog, the metadata epoch and the read timestamp. The epoch is the read
    /// timestamp, except on a cluster gateway (GD-0075 P7-2): there every read draws a fresh
    /// oracle timestamp, so the epoch is the catalog version the snapshot used (the read
    /// timestamp only when the snapshot predates every cached version). Two reads then share
    /// an epoch exactly when no catalog change was published between them.
    fn with_metadata_catalog<T>(
        &self,
        txn: Option<&engine::txn::Transaction>,
        inspect: impl FnOnce(
            Arc<engine::catalog::CatalogSnapshot>,
            u64,
            engine::hlc::Ts,
        ) -> Result<T, CoreError>,
    ) -> Result<T, CoreError> {
        #[cfg(feature = "test-metadata-counts")]
        crate::metadata_read_counts::increment(if txn.is_some() { 0 } else if self.cluster.is_some() { 1 } else { 2 });
        let mut committed;
        let cluster = self.cluster.is_some();
        let (catalog, ts, version) = match txn {
            Some(txn) => {
                let (catalog, version) = if cluster {
                    self.catalog.binding_snapshot(txn)?
                } else {
                    (self.catalog.metadata_snapshot(txn)?, None)
                };
                let ts = match txn.current_snapshot_ts() {
                    Some(ts) => ts,
                    None => self.visible_ts()?,
                };
                (catalog, ts, version)
            }
            None if cluster => {
                committed = self.begin(engine::txn::Isolation::ReadCommitted);
                let ts = committed.snapshot_ts();
                if let Some(error) = committed.snapshot_error() {
                    return Err(error.clone().into());
                }
                let (catalog, version) = self.catalog.binding_snapshot(&committed)?;
                (catalog, ts, version)
            }
            None => {
                if let Some((catalog, pin)) = self.catalog.current_local_metadata()? {
                    let ts = pin.ts();
                    // Both this pin and a fresh READ COMMITTED transaction use
                    // Sequencer::snapshot(): identical epoch and read timestamp
                    // for the same committed state. Keep the pin through inspect
                    // (including origin_at), so GC cannot pass that timestamp.
                    let result = inspect(catalog, ts.0, ts);
                    drop(pin);
                    return result;
                }
                #[cfg(feature = "test-metadata-counts")]
                crate::metadata_read_counts::increment(4);
                committed = self.begin(engine::txn::Isolation::ReadCommitted);
                let catalog = self.catalog.snapshot(&mut committed)?;
                (catalog, committed.snapshot_ts(), None)
            }
        };
        inspect(catalog, version.unwrap_or(ts).0, ts)
    }
    /// Older SQL-only v2 databases have no kernel image. When present, its
    /// existing metadata record carries restore provenance.
    pub(super) fn origin_at(&self, ts: engine::hlc::Ts) -> Result<Option<u128>, CoreError> {
        #[cfg(feature = "test-metadata-counts")]
        crate::metadata_read_counts::increment(3);
        let bytes = match &self.cluster {
            Some(services) => engine::mvcc::read_at(
                &*services.reader(ts),
                engine::kv::Keyspace::Kernel,
                &[0],
                ts,
            )?,
            None => engine::mvcc::read_at(self.kv.as_ref(), engine::kv::Keyspace::Kernel, &[0], ts)?,
        };
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        let metadata: super::Metadata = super::decode(&bytes)?;
        if metadata.domain.to_be_bytes() != self.meta.domain {
            return Err(CoreError::Corrupt("kernel metadata domain mismatch".into()));
        }
        Ok(metadata.origin)
    }

    pub(crate) fn open(path: &Path) -> Result<Self, CoreError> {
        Self::open_with_holder(path, None)
    }
    pub(crate) fn open_with_holder(path: &Path, token: Option<&[u8]>) -> Result<Self, CoreError> {
        #[cfg(not(feature = "engine-rocksdb"))]
        let _ = token;
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
            Self::from_kv_with_limit(kv, DEFAULT_WRITE_SET_BYTES, false, token)
        }
    }
}
#[cfg(all(test, feature = "engine-rocksdb"))]
thread_local! {
    /// Test hook: the recent-log budget for databases opened on this thread,
    /// so SERIALIZABLE validation takes its durable fallback (R3-610).
    pub(crate) static TEST_RECENT_LOG_BYTES: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };
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

#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "v2_maintenance_tests.rs"]
mod maintenance_tests;
