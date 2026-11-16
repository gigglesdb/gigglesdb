//! Call-side epoch and bounds checks a range leader runs before serving
//! (GD-0075 R5-22). Every refusal is `RangeError::StaleEpoch` carrying the
//! leader's current descriptor so the caller can refresh its route cache.
use std::ops::Bound;

use giggles_core::range_api::{
    Epoch, LockRequest, LockTarget, RangeDescriptor, RangeError, RangeSpace, RangeWrite,
    ReadRequest, range_key,
};

fn stale(desc: &RangeDescriptor) -> RangeError {
    RangeError::StaleEpoch {
        descriptor: desc.clone(),
    }
}
fn require(desc: &RangeDescriptor, ok: bool) -> Result<(), RangeError> {
    if ok { Ok(()) } else { Err(stale(desc)) }
}
/// The first range key after every key of `space` (the next tag).
fn after_space(space: RangeSpace) -> Vec<u8> {
    vec![space.tag() + 1]
}

pub fn check_epoch(desc: &RangeDescriptor, epoch: Epoch) -> Result<(), RangeError> {
    require(desc, desc.epoch == epoch)
}

/// The read's `[start, end)` raw-key bounds must lie inside the range.
pub fn check_read(desc: &RangeDescriptor, req: &ReadRequest) -> Result<(), RangeError> {
    check_epoch(desc, req.epoch)?;
    let start_ok = match &req.start {
        Bound::Unbounded => range_key(req.space, &[]) >= desc.start,
        Bound::Included(k) => range_key(req.space, k) >= desc.start,
        Bound::Excluded(k) => {
            let mut next = range_key(req.space, k);
            next.push(0);
            next >= desc.start
        }
    };
    let end_ok = match (&req.end, &desc.end) {
        (_, None) => true,
        (Bound::Unbounded, Some(end)) => after_space(req.space) <= *end,
        (Bound::Included(k), Some(end)) => range_key(req.space, k) < *end,
        (Bound::Excluded(k), Some(end)) => range_key(req.space, k) <= *end,
    };
    require(desc, start_ok && end_ok)
}

/// A key lock needs its key inside the range; a table sentinel needs the
/// table's row span to intersect the range (design.md 3.1).
pub fn check_lock(desc: &RangeDescriptor, req: &LockRequest) -> Result<(), RangeError> {
    check_epoch(desc, req.epoch)?;
    match &req.target {
        LockTarget::Key { space, key } => require(desc, desc.contains(&range_key(*space, key))),
        LockTarget::TableSentinel { table } => {
            let lo = range_key(RangeSpace::Rows, &table.to_be_bytes());
            let hi = table.checked_add(1).map_or_else(
                || after_space(RangeSpace::Rows),
                |next| range_key(RangeSpace::Rows, &next.to_be_bytes()),
            );
            let below_end = desc.end.as_ref().is_none_or(|end| lo < *end);
            require(desc, below_end && hi > desc.start)
        }
    }
}

pub fn check_writes(
    desc: &RangeDescriptor,
    epoch: Epoch,
    writes: &[RangeWrite],
) -> Result<(), RangeError> {
    check_epoch(desc, epoch)?;
    require(
        desc,
        writes
            .iter()
            .all(|w| desc.contains(&range_key(w.space, &w.key))),
    )
}
