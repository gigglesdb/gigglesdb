//! Fresh committed ledger reads without acquiring the kernel lock under the gate.
use super::*;
use crate::storage::engine::{kv::Keyspace, txn::Isolation};
use crate::task::records::LedgerHeader;

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
        let (ledger, header) = read_v2_view(db).inspect_err(|_| { self.fenced.store(true, Ordering::Release); })?;
        work(&v2_view(db, ledger.as_ref(), header))
    }
}

/// The ledger a v2 reader uses: the `[07]` blob while the ledger header
/// `26 00` is absent, the header (and through it the `22` records) after.
pub(super) fn read_v2_view(
    db: &crate::storage::v2::V2Db,
) -> Result<(Option<NativeOutcomes>, Option<LedgerHeader>), CoreError> {
    let mut txn = db.begin(Isolation::RepeatableRead);
    let header = v2_migrate::ledger_header(&mut txn)?;
    drop(txn);
    match header {
        Some(header) => Ok((None, Some(header))),
        None => Ok((read_v2_ledger(db)?, None)),
    }
}

pub(super) fn v2_view<'a>(
    db: &'a crate::storage::v2::V2Db,
    ledger: Option<&'a NativeOutcomes>,
    header: Option<LedgerHeader>,
) -> LedgerView<'a> {
    LedgerView {
        domain: u128::from_be_bytes(db.meta.domain),
        ledger,
        shared: header.map(|header| SharedLedger { db, header }),
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
