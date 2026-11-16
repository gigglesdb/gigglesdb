//! Authority head, hold and membership (GD-0061 design section 6, D-E).
//!
//! - The hold is the engine's exclusive lock on the task's mutation head key
//!   `20 | task | 00`. It is the first lock a protected transaction takes and is
//!   taken before the transaction chooses its snapshot, so every read below
//!   is exact: only holders write these keys.
//! - The head is read through `txn.get` (a tracked point read) and staged
//!   through `txn.put`. An absent head is the zero head with `next_seq` 1.
//! - Membership (`21 | task | table | pk`): each non-no-op effect counts one
//!   transition; a row the task never changed before also counts one distinct
//!   row. No-op effects are counted but charge nothing and write no membership.
use super::effects::Effects;
use super::records::{membership_key, mutation_head_key, Membership, MutationHead, TaskObjectId};
use crate::storage::engine::txn::TxnError;
use crate::storage::{Keyspace, Transaction, PROTECTED_RECORDS};
use crate::CoreError;
use std::time::Instant;

/// What one protected operation charges against the task's mutation caps.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Charge {
    /// Rows this task had never changed before.
    pub(crate) distinct_new: u32,
    /// Non-no-op effects.
    pub(crate) transitions: u32,
    /// Effects whose values did not change.
    pub(crate) noops: u32,
}

/// Take the task's authority hold: an exclusive engine lock on `20|task|00`,
/// held until the transaction ends (savepoint rollback keeps it).
///
/// Must come before the transaction reads anything, so its snapshot is chosen
/// after the hold: a transaction that already has a snapshot is `Misuse`.
pub(crate) fn hold(
    txn: &Transaction,
    task: TaskObjectId,
    deadline: Option<Instant>,
    cancel: &dyn Fn() -> bool,
) -> Result<(), TxnError> {
    if txn.current_snapshot_ts().is_some() {
        return Err(TxnError::Misuse);
    }
    txn.lock_kernel_key(&mutation_head_key(task), deadline, cancel)
}

/// The task's mutation head at the transaction snapshot; absent is the zero
/// head with `next_seq` (and `next_preview`) 1.
pub(crate) fn load_head(txn: &mut Transaction, task: TaskObjectId) -> Result<MutationHead, CoreError> {
    match txn.get(Keyspace::Kernel, &mutation_head_key(task))? {
        Some(value) => MutationHead::decode(&value),
        None => Ok(MutationHead {
            next_seq: 1,
            next_preview: 1,
            ..MutationHead::default()
        }),
    }
}

/// Reads each changed row's membership through `txn.get` and stages the
/// updated record through `txn.put`. Charges nothing for no-op effects.
pub(crate) fn charge(
    txn: &mut Transaction,
    task: TaskObjectId,
    seq: u64,
    effects: &Effects,
) -> Result<Charge, CoreError> {
    if seq == 0 {
        return Err(CoreError::Corrupt("task authority: seq 0".into()));
    }
    let mut out = Charge::default();
    for effect in effects.iter() {
        if Effects::is_noop(effect) {
            out.noops = bump(out.noops)?;
            continue;
        }
        out.transitions = bump(out.transitions)?;
        let key = membership_key(task, effect.table, &effect.pk);
        let next = match txn.get(Keyspace::Kernel, &key)? {
            None => {
                out.distinct_new = bump(out.distinct_new)?;
                Membership { first_seq: seq, last_seq: seq, transitions: 1 }
            }
            Some(value) => {
                let old = Membership::decode(&value)?;
                if seq <= old.last_seq {
                    return Err(CoreError::Corrupt("task membership: seq not after last_seq".into()));
                }
                Membership {
                    first_seq: old.first_seq,
                    last_seq: seq,
                    transitions: old.transitions.saturating_add(1),
                }
            }
        };
        txn.put(Keyspace::Kernel, key, next.encode())?;
    }
    if out.transitions > 0 {
        txn.require_features(PROTECTED_RECORDS);
    }
    Ok(out)
}

/// Stage `head` at `20|task|00` in the transaction's write set.
pub(crate) fn stage_head(txn: &mut Transaction, task: TaskObjectId, head: &MutationHead) -> Result<(), CoreError> {
    txn.put(Keyspace::Kernel, mutation_head_key(task), head.encode())?;
    txn.require_features(PROTECTED_RECORDS);
    Ok(())
}

fn bump(n: u32) -> Result<u32, CoreError> {
    n.checked_add(1).ok_or(CoreError::Limit)
}

#[cfg(test)]
#[path = "authority_tests.rs"]
mod authority_tests;
