//! Transaction status across gateways (GD-0075 R5-55, J:/gigglesdb-work/r5/p7/r5-55/design.md).
//!
//! `XACT/<xid>` records live in the system range as Catalog user keys `[XACT_TAG] ++ xid BE8`.
//! Value version 1: `[1][0][owner node BE8][txn BE8]` (assigned, written before the xid is
//! shown) or `[1][1]` (committed, written by the owning transaction's own commit). They are
//! read here at a fresh oracle timestamp through the range read call, which waits on (and
//! resolves through its home) a prepared write of the key.
use std::{ops::Bound, thread, time::Duration};

use crate::storage::engine::{codec, hlc::Ts, mvcc, txn::TxnError};
use crate::storage::range_api::{
    ClusterTxnId, RangeClient, RangeError, RangeSpace, RangeWrite, ReadDirection, ReadRequest,
    XACT_TAG, range_key,
};

use super::txn::txn_error;

const VERSION: u8 = 1;
const OWNER: u8 = 0;
const COMMITTED: u8 = 1;
/// Route changes and leader moves tolerated by one record read.
const ATTEMPTS: u32 = 16;

/// Catalog user key of `XACT/<xid>`.
pub(super) fn xact_key(xid: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(9);
    key.push(XACT_TAG);
    key.extend_from_slice(&xid.to_be_bytes());
    key
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum XactRecord {
    /// Assigned by `txn` on `node`'s gateway, not (yet) committed.
    Owner { node: u64, txn: ClusterTxnId },
    Committed,
}

impl XactRecord {
    pub(super) fn encode(self) -> Vec<u8> {
        match self {
            Self::Owner { node, txn } => {
                let mut value = Vec::with_capacity(18);
                value.extend_from_slice(&[VERSION, OWNER]);
                value.extend_from_slice(&node.to_be_bytes());
                value.extend_from_slice(&txn.0.to_be_bytes());
                value
            }
            Self::Committed => vec![VERSION, COMMITTED],
        }
    }

    pub(super) fn decode(value: &[u8]) -> Result<Self, RangeError> {
        match value {
            [VERSION, OWNER, rest @ ..] if rest.len() == 16 => Ok(Self::Owner {
                node: u64::from_be_bytes(rest[..8].try_into().expect("checked length")),
                txn: ClusterTxnId(u64::from_be_bytes(
                    rest[8..].try_into().expect("checked length"),
                )),
            }),
            [VERSION, COMMITTED] => Ok(Self::Committed),
            _ => Err(RangeError::Corrupt(format!("invalid XACT record {value:02x?}"))),
        }
    }

    /// The write of this record for `xid` (a Catalog write in the system range).
    pub(super) fn write(self, xid: u64) -> RangeWrite {
        RangeWrite {
            space: RangeSpace::Catalog,
            key: xact_key(xid),
            value: Some(self.encode()),
        }
    }
}

/// One read of `XACT/<xid>`.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Lookup {
    Found(Option<XactRecord>),
    /// A prepared write of the record stayed unresolved for the read's whole wait (its
    /// transaction is undecided until recovery runs).
    Pending,
}

/// Reads `XACT/<xid>` at a fresh oracle timestamp.
pub(super) fn read_record(client: &dyn RangeClient, xid: u64) -> Result<Lookup, TxnError> {
    let key = xact_key(xid);
    let route = range_key(RangeSpace::Catalog, &key);
    let mut delay = Duration::from_millis(1);
    let mut last = RangeError::Unavailable("no attempt".into());
    for _ in 0..ATTEMPTS {
        let ts = match client.oracle().next(1) {
            Ok(ts) => ts,
            Err(e) => return Err(txn_error(e)),
        };
        let desc = client.descriptor_for(&route).map_err(txn_error)?;
        let request = ReadRequest {
            range: desc.id,
            epoch: desc.epoch,
            space: RangeSpace::Catalog,
            start: Bound::Included(codec::user_key_seek_at(&key, Ts(ts.0))),
            end: Bound::Excluded(codec::user_key_upper_bound(&key)),
            dir: ReadDirection::Forward,
            ts,
            page_bytes: 4096,
        };
        match client.read(request) {
            Ok(page) => {
                for (raw, value) in page.pairs {
                    let (user, _) = codec::split_versioned(&raw)
                        .map_err(|e| txn_error(RangeError::Corrupt(format!("{e:?}"))))?;
                    if user != key.as_slice() {
                        continue;
                    }
                    let value = mvcc::decode(&value)
                        .map_err(|e| txn_error(RangeError::Corrupt(format!("{e:?}"))))?;
                    return match value {
                        Some(value) => XactRecord::decode(value)
                            .map(|record| Lookup::Found(Some(record)))
                            .map_err(txn_error),
                        None => Ok(Lookup::Found(None)),
                    };
                }
                return Ok(Lookup::Found(None));
            }
            Err(RangeError::StaleEpoch { .. }) => client.invalidate(&desc),
            // The leader waited `read_wait` on a prepared write (or a commit in flight) at or
            // below the timestamp: the outcome is not known yet.
            Err(RangeError::Unavailable(_)) => return Ok(Lookup::Pending),
            Err(e @ RangeError::NotLeader { .. }) => {
                client.invalidate(&desc);
                last = e;
                thread::sleep(delay);
                delay = (delay * 2).min(Duration::from_millis(50));
            }
            Err(e) => return Err(txn_error(e)),
        }
    }
    Err(txn_error(last))
}

#[cfg(test)]
mod tests;
