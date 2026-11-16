//! Fresh committed ledger reads without acquiring the kernel lock under the gate.
use super::*;
use crate::storage::engine::{kv::Keyspace, txn::Isolation};

impl Inner {
    pub(super) fn native_once_view<T>(&self, work: impl FnOnce(&LedgerView<'_>) -> NativeResult<T>) -> NativeResult<T> {
        let Backend::V2(db) = &self.backend else {
            return self.native_once_locked(|state| work(&LedgerView::borrow(state)));
        };
        let _ticket = self.enter()?;
        let _guard = crate::cancellation::lock_notified(&self.gate, || {
            self.decision_point(DecisionPoint::WaitingLock)
        })?;
        if self.fenced.load(Ordering::Acquire) {
            return Err(CoreError::Fenced.into());
        }
        let ledger = read_v2_ledger(db).inspect_err(|_| { self.fenced.store(true, Ordering::Release); })?;
        work(&LedgerView { domain: u128::from_be_bytes(db.meta.domain), ledger: ledger.as_ref() })
    }
}

pub(super) fn read_v2_ledger(db: &crate::storage::v2::V2Db) -> Result<Option<NativeOutcomes>, CoreError> {
    let mut txn = db.begin(Isolation::RepeatableRead);
    let bytes = txn.get(Keyspace::Kernel, &[7]).map_err(CoreError::from)?;
    let metadata = txn.get(Keyspace::Kernel, &[0]).map_err(CoreError::from)?;
    drop(txn);
    if bytes.is_some() && metadata.is_none() {
        return Err(CoreError::Corrupt("native ledger without kernel metadata".into()));
    }
    bytes.as_deref().map(NativeOutcomes::from_bytes).transpose()
}

#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "v2_tests.rs"]
mod tests;
