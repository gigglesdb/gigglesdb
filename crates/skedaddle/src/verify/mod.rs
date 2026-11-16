//! Independent typed comparison and comparable-boundary verification (P8a).
//!
//! SQL adapters must supply the same catalog key ordering on both endpoints,
//! select only the requested columns, and provide complete change evidence.
//! Counts describe coverage; only canonical content comparisons verify rows.
//! Reads are cached for each attempt: Merkle bisection needs no extra fetches.

mod compare;
mod coverage;
mod hash;
mod protocol;
pub mod sql;

pub use compare::{
    ColumnDifference, Comparison, Difference, KeyOrder, RowSource, compare_rows, compare_sources,
};
pub use coverage::*;
pub use hash::{ContentDigest, Projection, Row, range_digest, row_hash};
pub use protocol::{
    Applier, BoundarySource, BoundaryState, ChangeEvidence, SourceRead, Verification,
    VerificationSession,
};
// The chunk planner owns the bounds, identity, and catalog ordering contract.
pub use crate::pg::chunk::{ChunkRange as RangePlan, Key};

use std::{error::Error, fmt, time::Duration};

use crate::{
    event::{EventError, Position, PositionOrder},
    mapping::MappingError,
};

pub type SourcePositionOrder = std::sync::Arc<dyn PositionOrder + Send + Sync>;

/// Fail closed for wrong domains and connector-incomparable positions.
pub fn compare_positions(
    order: &(impl PositionOrder + ?Sized),
    left: &Position,
    right: &Position,
) -> Result<std::cmp::Ordering> {
    left.compare(right, order).map_err(VerifyError::Position)
}

pub type Result<T> = std::result::Result<T, VerifyError>;

/// Evidence continuity failures cannot be repaired by comparing current rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EvidenceInvalidation {
    CaptureGap,
    Reconnected,
    LineageChanged,
    SlotChanged,
}

/// Terminal failures are infrastructure errors, never a data mismatch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplierTerminalCause {
    CaptureStopped,
    ApplyStopped,
    CheckpointRejected,
}

/// A bounded evidence resource, including explicit units for diagnostics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EvidenceLimit {
    Keys(usize),
    EncodedBytes(usize),
    Observers(usize),
}

/// The range has no comparable boundary; its row denominator remains unknown.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NotVerifiableReason {
    EvidenceOverflow {
        limit: EvidenceLimit,
    },
    EvidenceInvalidated {
        reason: EvidenceInvalidation,
    },
    ApplierTimeout {
        waited_position: Position,
        elapsed: Duration,
    },
}

impl NotVerifiableReason {
    fn into_error(self, range: crate::pg::chunk::ChunkId) -> VerifyError {
        match self {
            Self::EvidenceOverflow { limit } => VerifyError::EvidenceOverflow { range, limit },
            Self::EvidenceInvalidated { reason } => VerifyError::EvidenceInvalidated { reason },
            Self::ApplierTimeout {
                waited_position,
                elapsed,
            } => VerifyError::ApplierTimeout {
                waited_position,
                elapsed,
            },
        }
    }
}

#[derive(Debug)]
pub enum VerifyError {
    Invalid(&'static str),
    Position(EventError),
    NonPgVerificationReportUnsupported,
    Mapping(MappingError),
    /// Adapters must redact credentials and row values from diagnostics.
    Read(String),
    /// Replan both endpoints with these bounds; no partial rows were returned.
    SplitRequired(Vec<RangePlan>),
    EvidenceOverflow {
        range: crate::pg::chunk::ChunkId,
        limit: EvidenceLimit,
    },
    EvidenceInvalidated {
        reason: EvidenceInvalidation,
    },
    ApplierTimeout {
        waited_position: Position,
        elapsed: Duration,
    },
    ApplierTerminal {
        cause: ApplierTerminalCause,
    },
}

impl VerifyError {
    /// Classify only the affected range. An overflow from another range is an
    /// adapter error and must not demote or certify this session's range.
    pub fn not_verifiable_reason(
        &self,
        range: &crate::pg::chunk::ChunkId,
    ) -> Option<NotVerifiableReason> {
        match self {
            Self::EvidenceOverflow {
                range: affected,
                limit,
            } if affected == range => Some(NotVerifiableReason::EvidenceOverflow { limit: *limit }),
            Self::EvidenceInvalidated { reason } => {
                Some(NotVerifiableReason::EvidenceInvalidated { reason: *reason })
            }
            Self::ApplierTimeout {
                waited_position,
                elapsed,
            } => Some(NotVerifiableReason::ApplierTimeout {
                waited_position: waited_position.clone(),
                elapsed: *elapsed,
            }),
            _ => None,
        }
    }
}

impl fmt::Display for VerifyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Position(error) => write!(f, "verification source position: {error}"),
            Self::NonPgVerificationReportUnsupported => {
                f.write_str("non-PG verification report not supported yet")
            }
            Self::Invalid(reason) => write!(f, "invalid verification input: {reason}"),
            Self::Mapping(error) => write!(f, "verification encoding: {error}"),
            Self::Read(reason) => write!(f, "verification read: {reason}"),
            Self::SplitRequired(_) => {
                write!(f, "verification range exceeds budget; split required")
            }
            Self::EvidenceOverflow { limit, .. } => {
                write!(f, "verification change evidence exceeds limit {limit:?}")
            }
            Self::EvidenceInvalidated { reason } => {
                write!(f, "verification change evidence invalidated: {reason:?}")
            }
            Self::ApplierTimeout {
                waited_position,
                elapsed,
            } => {
                write!(
                    f,
                    "verification applier timed out at source position {waited_position:?} after {elapsed:?}"
                )
            }
            Self::ApplierTerminal { cause } => {
                write!(f, "verification applier terminated: {cause:?}")
            }
        }
    }
}

impl Error for VerifyError {}

impl From<MappingError> for VerifyError {
    fn from(error: MappingError) -> Self {
        Self::Mapping(error)
    }
}
