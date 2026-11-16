//! Private setup seam; publication is connected by the following once tasks.
use super::*;
#[cfg(feature = "engine-rocksdb")]
use engine::{hlc::Ts, kernel::{Kernel, KernelImage}, txn::{Isolation, Savepoint}};

#[cfg(feature = "engine-rocksdb")]
pub(crate) struct OnceSetup {
    pub(crate) before: Option<KernelImage>,
    pub(crate) image: KernelImage,
    pub(crate) initial: Savepoint,
    pub(crate) fence: Ts,
}
#[cfg(not(feature = "engine-rocksdb"))]
pub(crate) struct OnceSetup;

impl SqlSession {
    #[cfg_attr(not(feature = "engine-rocksdb"), allow(dead_code))]
    pub(crate) fn begin_once_engine(&mut self) -> Result<OnceSetup, CoreError> {
        if self.aborted || self.staged.is_some() || self.v2_transaction.is_some() {
            return Err(CoreError::Sql(relational::SqlError::Transaction));
        }
        #[cfg(feature = "engine-rocksdb")]
        if let Backend::V2(db) = &self.admin.inner.backend {
            let inner = &self.admin.inner;
            let _ticket = inner.enter_sql()?;
            crate::cancellation::check()?;
            inner.sql_transactions.try_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < relational::MAX_TRANSACTIONS).then_some(n + 1)
            }).map_err(|_| CoreError::Busy)?;
            // Native once sessions lock the local kernel store (no cluster form yet).
            db.require_local()?;
            // Install immediately so every fallible setup path uses normal rollback.
            self.v2_transaction = Some(db.begin(Isolation::RepeatableRead));
            let result = (|| {
                let mut token = [0; 16];
                getrandom::fill(&mut token).map_err(io)?;
                let txn = self.v2_transaction.as_mut().expect("installed transaction");
                let mut guard = Kernel::lock(txn, &db.kv, None,
                    &|| crate::cancellation::check().is_err())?;
                self.sequence_baseline = db.catalog().snapshot(guard.transaction())?
                    .parts.ext.sequences.iter().map(|(&id, def)| (id, def.incarnation)).collect();
                let presence = Kernel::probe(guard.transaction())?;
                let domain = u128::from_be_bytes(db.meta.domain);
                let (before, image) = if !presence.metadata {
                    if presence.ledger {
                        return Err(CoreError::Corrupt("kernel ledger without metadata".into()));
                    }
                    (None, KernelImage(empty(domain)))
                } else {
                    let image = guard.load()?;
                    if image.0.view.domain != domain {
                        return Err(CoreError::Corrupt("kernel domain mismatch".into()));
                    }
                    (Some(image.clone()), image)
                };
                let fence = guard.transaction().snapshot_ts();
                let initial = guard.transaction().savepoint();
                self.transaction_id = Some(u128::from_be_bytes(token));
                Ok(OnceSetup { before, image, initial, fence })
            })();
            drop(_ticket);
            if result.is_err() { self.rollback(); }
            return result;
        }
        Err(CoreError::Unsupported)
    }
}

#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "once_session_tests.rs"]
mod tests;
