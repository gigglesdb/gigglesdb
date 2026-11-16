//! Ledger-only compaction keeps expired identities inside a live MVCC value.
use super::*;
use crate::storage::engine::{kernel::Kernel, once_guard::{CommitGuard, GuardRejection}, txn::{Isolation, TxnError}};
impl Admin {
    pub(super) fn compact_native_once_v2(
        &self, clock: &mut dyn FnMut() -> NativeResult<NativeClockLease>,
    ) -> NativeResult<usize> {
        let inner = &self.inner;
        let Backend::V2(db) = &inner.backend else { unreachable!() };
        let _ticket = inner.enter()?;
        db.require_local()?;
        let mut txn = db.begin(Isolation::RepeatableRead);
        let mut kernel = Kernel::lock(&mut txn, &db.kv, None, &|| crate::cancellation::check().is_err())?;
        let presence = Kernel::probe(kernel.transaction())?;
        if presence.ledger && !presence.metadata {
            return Err(CoreError::Corrupt("native ledger without kernel metadata".into()).into());
        }
        if !presence.ledger { return Ok(0); }
        let before = kernel.load()?;
        let _gate = crate::cancellation::lock_notified(&inner.gate, || inner.decision_point(DecisionPoint::WaitingLock))?;
        if inner.fenced.load(Ordering::Acquire) { return Err(CoreError::Fenced.into()); }
        let lease = certify_lease(before.0.native_outcomes.as_ref(), clock()?)?;
        let mut after = before.clone();
        let count = after.0.native_outcomes.as_mut().expect("present ledger").compact(lease);
        if count == 0 { return Ok(0); }
        Kernel::stage_from(&mut kernel, Some(&before), &after)?;
        let hook_inner = inner.clone();
        let guard = CommitGuard::capture_unfenced(lease.valid_until)
            .with_hook(Arc::new(move |point| hook_inner.decision_point(point)));
        db.catalog().commit_ddl_guarded(txn, guard).map_err(|error| match error {
            TxnError::Guard(GuardRejection::LeaseExpired) => NativeOnceError::ClockUncertain,
            TxnError::CommitAmbiguous => {
                inner.fenced.store(true, Ordering::Release);
                CoreError::Storage("engine commit outcome unknown".into()).into()
            }
            other => CoreError::from(other).into(),
        })?;
        Ok(count)
    }
}
#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "v2_compact_tests.rs"]
mod tests;
