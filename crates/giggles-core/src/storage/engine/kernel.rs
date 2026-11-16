//! Legacy non-SQL records in the engine transaction's atomic write set.
//!
//! Read-only loads use the caller's ordinary MVCC snapshot. Mutations must acquire
//! `lock` before their first kernel load and retain the transaction through commit.
//! After waiting, lock acquisition checks the newest committed mutation stamp.
//! ReadCommitted refreshes stale snapshots; RepeatableRead and Serializable reject
//! them with a serialization failure. Load mutation images through the guard.
//! The guard remains valid only for its validated statement snapshot.
//!
//! Prefix-free legacy user keys have this fixed namespace/length table:
//! namespaces 0 (metadata), 7 (native), 9 (application): 1 byte;
//! namespaces 1 (rows), 3 (operations): 17 bytes;
//! namespaces 2 (tasks), 4 (approvals), 6 (accounting): 9 bytes.
//! Internal namespace 255 (mutation stamp): 1 byte, with an empty value.
//! SQL namespaces 5 and 8 are never persisted in Keyspace::Kernel.
//! Namespaces 0x20..=0x2B hold protected task records (`task::records`); they
//! are not part of the legacy image and `load` does not read them.
use super::{
    hlc::Ts,
    kv::{Direction, Keyspace, Kv},
    locks::{KERNEL_LOCK_TABLE, LockKey, LockMode},
    mvcc,
    txn::{Isolation, SerializationReason, Transaction, TxnError},
};
use crate::{CoreError, storage, task::records};
use std::{
    collections::BTreeMap,
    ops::Bound::{Excluded, Included, Unbounded},
    time::Instant,
};

const STAMP_KEY: &[u8] = &[0xFF];

#[derive(Clone)]
pub(crate) struct KernelImage(pub(crate) storage::State);

pub(crate) struct Kernel;

pub(crate) struct KernelPresence {
    pub(crate) metadata: bool,
    pub(crate) ledger: bool,
}

/// Unforgeable borrowing evidence of successful kernel lock acquisition.
/// Dropping this borrow does not release the lock: the transaction owns it until
/// commit, rollback, or drop. Business row writes use `transaction()` and share
/// the same write set. End the borrow before consuming the transaction to commit.
pub(crate) struct KernelGuard<'a> {
    txn: &'a mut Transaction,
    validated_snapshot: Ts,
}
impl KernelGuard<'_> {
    pub(crate) fn transaction(&mut self) -> &mut Transaction {
        self.txn
    }
    pub(crate) fn load(&mut self) -> Result<KernelImage, CoreError> {
        let txn = self.guarded_transaction()?;
        Kernel::load(txn)
    }
}

mod sealed {
    pub trait Sealed {}
    impl Sealed for super::Transaction {}
    impl Sealed for super::KernelGuard<'_> {}
}
/// Bare transactions are accepted only to report misuse at the staging boundary;
/// only a guard can authorize staging. No global or unsafe lock tracking is used.
pub(crate) trait KernelAccess: sealed::Sealed {
    fn guarded_transaction(&mut self) -> Result<&mut Transaction, TxnError>;
}
impl KernelAccess for Transaction {
    fn guarded_transaction(&mut self) -> Result<&mut Transaction, TxnError> {
        Err(TxnError::Misuse)
    }
}
impl KernelAccess for KernelGuard<'_> {
    fn guarded_transaction(&mut self) -> Result<&mut Transaction, TxnError> {
        if self.txn.snapshot_ts() != self.validated_snapshot {
            return Err(TxnError::Misuse);
        }
        Ok(self.txn)
    }
}

pub(super) fn valid_key(key: &[u8]) -> bool {
    match key.first() {
        Some(0 | 7 | 9) => key.len() == 1,
        Some(1 | 3) => key.len() == 17,
        Some(2 | 4 | 6) => key.len() == 9,
        _ => false,
    }
}

// Legacy encode maps bincode errors to Corrupt. Translate its precise size-limit
// diagnostic at this boundary so a large individual record is a resource limit,
// just like the aggregate envelope_bound check inside records().
fn record_error(error: CoreError) -> CoreError {
    match error {
        CoreError::Corrupt(message) if message == bincode::ErrorKind::SizeLimit.to_string() => {
            CoreError::Limit
        }
        other => other,
    }
}

fn records(image: &KernelImage) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, CoreError> {
    storage::validate(&image.0)?;
    if image.0.sql != Default::default() {
        return Err(TxnError::Misuse.into());
    }
    // Validate and bound the complete legacy envelope, including ephemeral SQL,
    // before staging anything. Preserve all non-SQL key and value bytes exactly.
    let mut out = storage::records(&image.0).map_err(record_error)?;
    out.remove(&vec![5]);
    out.remove(&vec![8]);
    if !out.keys().all(|key| valid_key(key)) {
        return Err(TxnError::Misuse.into());
    }
    Ok(out)
}

impl Kernel {
    /// `kv` must be the live committed KV handle belonging to this transaction
    /// engine. The explicit handle allows latest-version reads without exposing
    /// transaction internals or using its possibly stale MVCC snapshot.
    pub(crate) fn lock<'a>(
        txn: &'a mut Transaction,
        kv: &Kv,
        deadline: Option<Instant>,
        cancel: &dyn Fn() -> bool,
    ) -> Result<KernelGuard<'a>, CoreError> {
        txn.acquire_lock(
            LockKey::Table(KERNEL_LOCK_TABLE),
            LockMode::AccessExclusive,
            deadline,
            cancel,
        )?;
        let snapshot = txn.snapshot_ts();
        if let Some((stamp, value)) = mvcc::latest_version(kv, Keyspace::Kernel, STAMP_KEY)? {
            if value.as_deref() != Some(&[]) {
                return Err(CoreError::Corrupt("kernel mutation stamp value".into()));
            }
            if stamp > snapshot {
                if txn.isolation() == Isolation::ReadCommitted {
                    txn.begin_statement();
                } else {
                    return Err(TxnError::Serialization(SerializationReason::WriteConflict).into());
                }
            }
        }
        let validated_snapshot = txn.snapshot_ts();
        Ok(KernelGuard {
            txn,
            validated_snapshot,
        })
    }

    pub(crate) fn probe(txn: &mut Transaction) -> Result<KernelPresence, CoreError> {
        Ok(KernelPresence {
            metadata: txn.get(Keyspace::Kernel, &[0])?.is_some(),
            ledger: txn.get(Keyspace::Kernel, &[7])?.is_some(),
        })
    }

    pub(crate) fn load(txn: &mut Transaction) -> Result<KernelImage, CoreError> {
        let mut out = BTreeMap::new();
        // The protected task namespaces 0x20..=0x2B are not legacy records and
        // can be large; skip them with two scans instead of reading them.
        let protected_end = [records::NS_MAX + 1];
        for (start, end) in [
            (Unbounded, Excluded(&[records::NS_MIN][..])),
            (Included(&protected_end[..]), Unbounded),
        ] {
            for item in txn.scan(Keyspace::Kernel, start, end, Direction::Forward, None)? {
                let (key, value) = item?;
                if key == STAMP_KEY {
                    if !value.is_empty() {
                        return Err(CoreError::Corrupt("kernel mutation stamp value".into()));
                    }
                    continue;
                }
                if !valid_key(&key) {
                    return Err(CoreError::Corrupt("kernel namespace or key length".into()));
                }
                out.insert(key, value);
            }
        }
        // Bootstrap must explicitly stage metadata (for example from an empty
        // domain-zero image to the chosen nonzero domain). An absent image is
        // corrupt, exactly as in the legacy loader; no synthetic metadata hides it.
        out.insert(
            vec![5],
            storage::encode(&crate::relational::Relational::default())?,
        );
        Ok(KernelImage(storage::from_records(out)?))
    }

    /// Stage only byte differences. Validation/encoding/size checks finish before
    /// the first put/delete. On any transaction write-set error, roll back the txn.
    /// The return value counts changed legacy keys, including deleted records,
    /// and excludes the auxiliary mutation stamp. No-op stages write no stamp.
    pub(crate) fn stage(
        access: &mut impl KernelAccess,
        before: &KernelImage,
        after: &KernelImage,
    ) -> Result<usize, CoreError> {
        Self::stage_from(access, Some(before), after)
    }

    pub(crate) fn stage_from(
        access: &mut impl KernelAccess,
        before: Option<&KernelImage>,
        after: &KernelImage,
    ) -> Result<usize, CoreError> {
        let txn = access.guarded_transaction()?;
        let before = before.map(records).transpose()?.unwrap_or_default();
        let wide = after.0.native_outcomes.as_ref().is_some_and(|n| n.contains_wide());
        let after = records(after)?;
        if after.contains_key(&vec![7]) {
            let metadata: storage::Metadata = storage::decode(&after[&vec![0]])?;
            let (family, _, _, temporal, _) = storage::envelope_family(metadata.format)?;
            txn.require_features(super::meta::NATIVE_OUTCOMES
                | u64::from(family.typed_scalars) | (u64::from(temporal) << 4)
                | if wide { super::meta::WIDE_ONCE_RECEIPTS } else { 0 });
            txn.require_subversion(super::meta::NATIVE_SUBVERSION);
        }
        let mut count = 0;
        for key in before.keys().filter(|key| !after.contains_key(*key)) {
            txn.delete(Keyspace::Kernel, key.clone())?;
            count += 1;
        }
        for (key, value) in &after {
            if before.get(key) != Some(value) {
                txn.put(Keyspace::Kernel, key.clone(), value.clone())?;
                count += 1;
            }
        }
        if count > 0 {
            txn.put(Keyspace::Kernel, STAMP_KEY.to_vec(), Vec::new())?;
        }
        Ok(count)
    }
}

#[cfg(test)]
#[path = "kernel_tests.rs"]
mod tests;
