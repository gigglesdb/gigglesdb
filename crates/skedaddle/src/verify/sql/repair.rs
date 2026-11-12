use super::super::{Key, Lsn, RangePlan, TableCoverage};

pub const REPAIR_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepairScope {
    pub range: RangePlan,
    pub key: Key,
    pub columns: Vec<String>,
}

/// Positions here MUST be comparable timestamps/revisions from the same change
/// authority. A destination WAL LSN is never comparable with a source WAL LSN.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepairRequest {
    pub version: u32,
    pub scope: RepairScope,
    pub source_read_revision: u64,
    pub source_read_lsn: Lsn,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepairContext {
    pub expected_scope: RepairScope,
    pub authority_transferred: bool,
    /// Latest destination-originated change in the shared revision domain.
    /// None means missing provenance; Some(0) explicitly proves no such change.
    pub destination_change_revision: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RepairRefusal {
    Version,
    Scope,
    UnknownDestinationChange,
    NewerDestinationChange,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepairDecision {
    pub request: RepairRequest,
    /// Entire table proof becomes uncollected until affected ranges are reread.
    pub invalidated_coverage: TableCoverage,
}

/// Pure authorization decision; produces no SQL and never executes a repair.
/// The returned coverage replaces the old table coverage before execution.
pub fn decide_repair(
    order: &impl super::super::KeyOrder,
    request: &RepairRequest,
    context: &RepairContext,
    coverage: &TableCoverage,
) -> std::result::Result<RepairDecision, RepairRefusal> {
    if request.version != REPAIR_VERSION {
        return Err(RepairRefusal::Version);
    }
    if request.scope != context.expected_scope
        || request.scope.range.id.table != coverage.table
        || request.scope.key.is_empty()
        || request.scope.columns.is_empty()
        || request
            .scope
            .key
            .iter()
            .any(|v| matches!(v, crate::mapping::Value::Null))
        || request.scope.columns.iter().any(|c| c.is_empty())
        || request
            .scope
            .columns
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != request.scope.columns.len()
    {
        return Err(RepairRefusal::Scope);
    }
    let range = &request.scope.range;
    if super::super::compare::validate_range(order, range).is_err()
        || order
            .compare_keys(&request.scope.key, &request.scope.key)
            .ok()
            != Some(std::cmp::Ordering::Equal)
        || range.lo.as_ref().is_some_and(|lo| {
            !matches!(
                order.compare_keys(lo, &request.scope.key),
                Ok(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
            )
        })
        || range.hi.as_ref().is_some_and(|hi| {
            order.compare_keys(&request.scope.key, hi).ok() != Some(std::cmp::Ordering::Less)
        })
    {
        return Err(RepairRefusal::Scope);
    }
    if context.authority_transferred {
        let change = context
            .destination_change_revision
            .ok_or(RepairRefusal::UnknownDestinationChange)?;
        if change > request.source_read_revision {
            return Err(RepairRefusal::NewerDestinationChange);
        }
    }
    Ok(RepairDecision {
        request: request.clone(),
        invalidated_coverage: TableCoverage::excluded(
            coverage.table.clone(),
            coverage.rows.total,
            "repair invalidated coverage; reread required",
        ),
    })
}
