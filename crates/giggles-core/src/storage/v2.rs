//! Minimal v2 lifecycle. Public legacy operations fail before admission or mutation.
#[cfg(feature = "engine-rocksdb")]
use super::io;
use super::{CoreError, engine};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{fs, path::Path};

#[allow(dead_code)]
pub(crate) struct V2Db {
    pub(crate) kv: Arc<engine::kv::Kv>,
    pub(crate) meta: engine::meta::MetaInfo,
    sequencer: Arc<engine::commit::Sequencer>,
    locks: Arc<engine::locks::LockManager>,
    catalog: engine::catalog::Catalog,
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

impl V2Db {
    #[cfg(feature = "engine-rocksdb")]
    pub(crate) fn create(path: &Path) -> Result<Self, CoreError> {
        let mut domain = [0u8; 16];
        getrandom::fill(&mut domain).map_err(io)?;
        let backend = engine::meta::create(path, domain, |path| {
            Ok(Arc::new(engine::kv_rocksdb::RocksKv::open(
                path,
                64 * 1024 * 1024,
            )?))
        })
        .map_err(format_error)?;
        Self::from_kv(Arc::new(engine::kv::Kv::new(backend)))
    }
    #[cfg(feature = "engine-rocksdb")]
    fn from_kv(kv: Arc<engine::kv::Kv>) -> Result<Self, CoreError> {
        let meta = engine::meta::open_checks(kv.as_ref()).map_err(format_error)?;
        let sequencer = engine::commit::Sequencer::open(
            kv.clone(),
            Arc::new(engine::commit::SystemClock),
            engine::commit::SequencerConfig::default(),
        )?;
        let locks = Arc::new(engine::locks::LockManager::new(
            engine::locks::LockConfig::default(),
        ));
        let sequences = Arc::new(engine::seq::SequenceAllocator::new(kv.clone(), 0));
        let catalog = engine::catalog::Catalog::open(sequencer.clone(), locks.clone(), sequences)?;
        Ok(Self {
            kv,
            meta,
            sequencer,
            locks,
            catalog,
            next_txn: AtomicU64::new(1),
        })
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
