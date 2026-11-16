//! Readers resolve a blocked prepared write through its home record (GD-0075 R5-53, Track D).
//!
//! A read at T (or a lock request) on a participant range that has waited `resolve_after` on a
//! transaction's prepared record with p <= T asks the transaction's home range for its status
//! (design.md 4a rule O6). The range engine (Track A) calls [`resolve_prepared`] from its read
//! and lock paths and supplies its own local finalize:
//! - Decided(o): finalize the prepared record locally with o, on the transaction's behalf.
//! - Unknown: presumed abort, finalize locally with Abort. The home record exists from before the
//!   Seal, which precedes every prepare, and is forgotten only after every participant
//!   acknowledged finalize; so a prepared record whose home record is gone is a late duplicate
//!   prepare, and the transaction's locks were already released at finalize.
//! - Open: keep waiting; recovery on the home aborts an idle record.
//!
//! Status does not restart the home's recovery clock (R5-50m B3), so polling never delays it.
// Wired by the range engine (Track A); until then only tests use it.
#![cfg_attr(not(test), allow(dead_code))]

use crate::range_api::{
    ClusterTxnId, Epoch, HomeOp, HomeReply, HomeStatus, Outcome, RangeClient, RangeError, RangeId,
};

/// What a blocked reader should do next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Resolution {
    /// The prepared record was finalized here with this outcome: retry the read.
    Finalized(Outcome),
    /// The transaction is still undecided at its home: keep waiting, ask again later.
    Pending,
}

/// Resolves `txn`'s prepared record on this range through its home record. `finalize_local`
/// finalizes the prepared record on this range (idempotent, as `RangeService::finalize`).
/// An error means the home could not answer: the reader keeps waiting (or fails its read with
/// `Unavailable` under rule O6), never guesses.
pub(crate) fn resolve_prepared(
    client: &dyn RangeClient,
    home: RangeId,
    txn: ClusterTxnId,
    finalize_local: &dyn Fn(Outcome) -> Result<(), RangeError>,
) -> Result<Resolution, RangeError> {
    let outcome = match home_status(client, home, txn)? {
        HomeStatus::Open { .. } => return Ok(Resolution::Pending),
        HomeStatus::Decided(outcome) => outcome,
        HomeStatus::Unknown => Outcome::Abort,
    };
    finalize_local(outcome.clone())?;
    Ok(Resolution::Finalized(outcome))
}

/// Status of `txn` at `home`. The reader does not know the home's epoch: it asks with epoch 0
/// and, if refused as stale, once more with the epoch of the descriptor the refusal carries.
fn home_status(
    client: &dyn RangeClient,
    home: RangeId,
    txn: ClusterTxnId,
) -> Result<HomeStatus, RangeError> {
    let ask = |epoch| client.home(home, epoch, HomeOp::Status { txn });
    let reply = match ask(Epoch(0)) {
        Err(RangeError::StaleEpoch { descriptor }) if descriptor.id == home => {
            ask(descriptor.epoch)?
        }
        other => other?,
    };
    match reply {
        HomeReply::Status(status) => Ok(status),
        other => Err(RangeError::Corrupt(format!(
            "home Status replied {other:?}"
        ))),
    }
}
