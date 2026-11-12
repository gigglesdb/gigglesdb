use super::{Key, KeyOrder, RangePlan, Result, VerifyError};
use sha2::{Digest, Sha256};

/// Bytes are UTF-8 PostgreSQL text payloads plus eight bytes per field,
/// including key fields and NULLs. The SQL aggregate runs before row transfer.
#[derive(Clone, Copy, Debug)]
pub struct RangeBudget {
    pub max_rows: u64,
    pub max_bytes: u64,
}
impl RangeBudget {
    pub(super) fn validate(self) -> Result<()> {
        if self.max_rows == 0 || self.max_bytes == 0 {
            return Err(VerifyError::Invalid("zero verification range cap"));
        }
        Ok(())
    }
    pub fn accepts(self, rows: u64, bytes: u64) -> bool {
        rows <= self.max_rows && bytes <= self.max_bytes
    }
}

/// A split changes the plan fingerprint, invalidating the parent's coverage.
/// A complete table plan must be renumbered by the caller before aggregation.
pub fn split_range(
    order: &impl KeyOrder,
    parent: &RangePlan,
    boundary: Key,
) -> Result<Vec<RangePlan>> {
    use std::cmp::Ordering;
    super::super::compare::validate_range(order, parent)?;
    if boundary.is_empty()
        || boundary
            .iter()
            .any(|v| matches!(v, crate::mapping::Value::Null))
        || parent
            .lo
            .as_ref()
            .map(|lo| order.compare_keys(lo, &boundary))
            .transpose()?
            .is_some_and(|v| v != Ordering::Less)
        || parent
            .hi
            .as_ref()
            .map(|hi| order.compare_keys(&boundary, hi))
            .transpose()?
            .is_some_and(|v| v != Ordering::Less)
    {
        return Err(VerifyError::Invalid("sample does not split range"));
    }
    // Validate boundary type/arity even when both parent ends are unbounded.
    if order.compare_keys(&boundary, &boundary)? != Ordering::Equal {
        return Err(VerifyError::Invalid("invalid sample order"));
    }
    let mut hash = Sha256::new();
    hash.update(b"skedaddle-verifier-split-v1\0");
    hash.update(
        serde_json::to_vec(&(parent, &boundary))
            .map_err(|_| VerifyError::Invalid("range encoding"))?,
    );
    let plan = format!("{:x}", hash.finalize());
    let mut left = parent.clone();
    left.id.plan = plan.clone();
    left.id.ordinal = 0;
    left.hi = Some(boundary.clone());
    let mut right = parent.clone();
    right.id.plan = plan;
    right.id.ordinal = 1;
    right.lo = Some(boundary);
    Ok(vec![left, right])
}
