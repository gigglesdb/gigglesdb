//! Private setup seam; publication is connected by the following once tasks.
use super::*;
#[cfg(feature = "engine-rocksdb")]
use engine::{hlc::Ts, kernel::{Kernel, KernelImage}, txn::{Isolation, Savepoint}};

#[cfg(all(test, feature = "engine-rocksdb"))]
thread_local! {
    pub(super) static AFTER_ONCE_LOCK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(feature = "engine-rocksdb")]
pub(crate) struct OnceSetup {
    pub(crate) before: Option<KernelImage>,
    pub(crate) image: KernelImage,
    pub(crate) initial: Savepoint,
    pub(crate) fence: Ts,
    pub(crate) lease: EngineLease,
}
#[cfg(not(feature = "engine-rocksdb"))]
pub(crate) struct OnceSetup;

impl SqlSession {
    #[cfg_attr(not(feature = "engine-rocksdb"), allow(dead_code))]
    pub(crate) fn begin_once_engine(&mut self, owner: Option<&native_once::NativeOnceOwner>) -> Result<OnceSetup, CoreError> {
        #[cfg(not(feature = "engine-rocksdb"))]
        let _ = owner;
        if self.aborted || self.staged.is_some() || self.v2_transaction.is_some() {
            return Err(CoreError::Sql(relational::SqlError::Transaction));
        }
        #[cfg(feature = "engine-rocksdb")]
        if self.is_v2_engine() {
            let db = match owner {
                Some(owner) if owner.belongs_to(&self.admin) =>
                    owner.lease()?.ok_or(CoreError::Fenced)?.child(Holder::NativeOnce)?,
                Some(_) => return Err(CoreError::Fenced),
                None => self.admin.lease(Holder::NativeOnce)?.ok_or(CoreError::Unsupported)?,
            };
            self.bind_sequence_generation(&db);
            let inner = &self.admin.inner;
            let _ticket = inner.enter_sql()?;
            crate::cancellation::check()?;
            let txn = db.begin_leased(Isolation::RepeatableRead, Holder::NativeOnce)?;
            db.require_local()?;
            inner.sql_transactions.try_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < relational::MAX_TRANSACTIONS).then_some(n + 1)
            }).map_err(|_| CoreError::Busy)?;
            // Native once sessions lock the local kernel store (no cluster form yet).
            // Install immediately so every fallible setup path uses normal rollback.
            self.v2_transaction = Some(txn);
            let result = (|| {
                let mut token = [0; 16];
                getrandom::fill(&mut token).map_err(io)?;
                let txn = self.v2_transaction.as_mut().expect("installed transaction");
                let guard = Kernel::lock(txn, &db.kv, None,
                    &|| crate::cancellation::check().is_err() || db.check().is_err());
                #[cfg(test)]
                AFTER_ONCE_LOCK.with(|hook| {
                    if let Some(run) = hook.borrow_mut().take() {
                        run();
                    }
                });
                let mut guard = guard?;
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
                Ok(OnceSetup { before, image, initial, fence, lease: db.child(Holder::NativeOnce)? })
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
