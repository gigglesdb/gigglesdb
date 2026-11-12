use crate::capability::Evidence;

use super::{
    KeyOrder, Result, Verification, VerifyError,
    compare::{compare_key, validate_range},
};

/// Categories are disjoint. Busy and excluded rows remain in `total`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Coverage {
    pub total: u64,
    pub verified: u64,
    pub busy: u64,
    pub mismatched: u64,
    pub excluded: u64,
}

impl Coverage {
    pub fn is_complete(&self) -> bool {
        self.verified == self.total && self.busy == 0 && self.mismatched == 0 && self.excluded == 0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableCoverage {
    pub table: String,
    pub rows: Coverage,
    pub exclusion_reason: Option<String>,
}

impl TableCoverage {
    /// Aggregate a complete table's contiguous ranges from one chunk plan.
    /// Repeated retry results, missing ranges and overlapping bounds are errors.
    pub fn from_ranges(order: &(impl KeyOrder + ?Sized), ranges: &[Verification]) -> Result<Self> {
        let first = ranges
            .first()
            .ok_or(VerifyError::Invalid("no table ranges"))?;
        if first.range.lo.is_some() || ranges.last().expect("nonempty").range.hi.is_some() {
            return Err(VerifyError::Invalid(
                "table coverage does not span full keyspace",
            ));
        }
        let mut rows = Coverage::default();
        for (index, verification) in ranges.iter().enumerate() {
            let range = &verification.range;
            validate_range(order, range)?;
            if range.id.table != first.range.id.table || range.id.plan != first.range.id.plan {
                return Err(VerifyError::Invalid("table coverage mixes tables or plans"));
            }
            if index > 0 {
                let previous = &ranges[index - 1].range;
                let (Some(hi), Some(lo)) = (&previous.hi, &range.lo) else {
                    return Err(VerifyError::Invalid("overlapping table coverage ranges"));
                };
                if previous.id.ordinal.checked_add(1) != Some(range.id.ordinal)
                    || compare_key(order, hi, lo)? != std::cmp::Ordering::Equal
                {
                    return Err(VerifyError::Invalid("noncontiguous table coverage ranges"));
                }
            }
            let coverage = &verification.comparison.coverage;
            for (total, value) in [
                (&mut rows.total, coverage.total),
                (&mut rows.verified, coverage.verified),
                (&mut rows.busy, coverage.busy),
                (&mut rows.mismatched, coverage.mismatched),
                (&mut rows.excluded, coverage.excluded),
            ] {
                *total = total
                    .checked_add(value)
                    .ok_or(VerifyError::Invalid("coverage count overflow"))?;
            }
        }
        Ok(Self {
            table: first.range.id.table.clone(),
            rows,
            exclusion_reason: None,
        })
    }

    pub fn excluded(table: impl Into<String>, total: u64, reason: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            rows: Coverage {
                total,
                excluded: total,
                ..Coverage::default()
            },
            exclusion_reason: Some(reason.into()),
        }
    }
}

/// Absence of a collector is explicit and can never imply success.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum ResultSlot<T> {
    #[default]
    NotCollected,
    Collected {
        checked: u64,
        findings: Vec<T>,
        evidence: Vec<Evidence>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaFinding {
    pub object: String,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SequenceFinding {
    pub sequence: String,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConstraintFinding {
    pub constraint: String,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexFinding {
    pub index: String,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForeignKeyFinding {
    pub constraint: String,
    pub violating_rows: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VerificationReport {
    pub tables: Vec<TableCoverage>,
    pub schema: ResultSlot<SchemaFinding>,
    pub sequences: ResultSlot<SequenceFinding>,
    pub constraints: ResultSlot<ConstraintFinding>,
    pub indexes: ResultSlot<IndexFinding>,
    /// The later SQL collector must inspect destination FK integrity.
    pub foreign_keys: ResultSlot<ForeignKeyFinding>,
}
