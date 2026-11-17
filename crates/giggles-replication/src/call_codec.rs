//! Wire codec of range, gateway and node-status calls between nodes (GD-0075 R5-21, R5-64).
//!
//! A call body is `[version u8 = 1][call tag u8][fields]`; a reply body is
//! `[version u8 = 1][0][reply tag u8][fields]` or `[version u8 = 1][1][error tag u8][fields]`.
//! Integers are big-endian u64 unless noted; byte strings and lists carry a u32 BE length or
//! count; options and bounds carry a u8 flag. Version 1 is strict: unknown tags or versions,
//! truncation and trailing bytes are rejected. Node reports are emitted with version 2
//! (adding move_step); version 1 reports still decode. Not persisted (peer frames only).
use std::ops::{Bound, Range};

use giggles_core::range_api::{
    ClusterTxnId, DeadlockProbe, Epoch, HomeOp, HomeReply, HomeStatus, LeaseKind, LockGrant,
    LockRequest, LockTarget, Outcome, RangeDescriptor, RangeError, RangeId, RangeLockMode,
    RangeSpace, RangeWrite, ReadDirection, ReadPage, ReadRequest, SequenceOp, SequenceReply,
    Timestamp,
};

use crate::client::{RangeCall, RangeReply};

const VERSION: u8 = 1;

/// A deadlock-detection call to a gateway (design.md 3.2, `GatewayPeer`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GatewayCall {
    Probe(DeadlockProbe),
    Abort(ClusterTxnId),
    Blockers(ClusterTxnId),
    CancelWait(ClusterTxnId),
    /// Whether the gateway still runs the transaction (R5-55 status of another gateway's xid).
    TxnLive(ClusterTxnId),
}

/// Result of a `GatewayCall`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GatewayReply {
    Done,
    Blockers(Vec<ClusterTxnId>),
    Live(bool),
}

/// An operator call to a node (R5-64 `giggles-server cluster status`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeCall {
    Status,
    RetireNode {
        id: u64,
    },
    EnrolNode {
        id: u64,
        address: String,
        fingerprint: String,
    },
    HostRange(RangeDescriptor),
    MoveRange {
        range: RangeId,
        from: u64,
        to: u64,
    },
}

/// Progress or redirection for an operator's resumable move.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MoveReply {
    pub leader: Option<u64>,
    pub step: Option<String>,
    pub done: bool,
    pub error: Option<String>,
}

/// A replica's Raft role (a pre-candidate reports as candidate).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeRole {
    Leader,
    Follower,
    Candidate,
}

/// Calls a node's leader engines served for one range since the node started.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CallCounts {
    pub reads: u64,
    pub locks: u64,
    /// Commits, prepares and finalizes.
    pub writes: u64,
    pub other: u64,
}

impl CallCounts {
    pub fn total(&self) -> u64 {
        [self.locks, self.writes, self.other]
            .into_iter()
            .fold(self.reads, u64::saturating_add)
    }
}

/// One replica of a range as its node sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeReport {
    pub descriptor: RangeDescriptor,
    pub role: RangeRole,
    pub term: u64,
    /// The leader this replica knows of; 0 if none.
    pub leader: u64,
    pub applied: u64,
    /// Approximate bytes stored for the range; `None` when unknown.
    pub size: Option<u64>,
    pub calls: CallCounts,
    /// Calls served within the last `window_ms` (about a minute).
    pub recent: u64,
    pub window_ms: u64,
    pub move_step: Option<String>,
}

impl RangeReport {
    pub fn calls_total(&self) -> u64 {
        self.calls.total()
    }
    pub fn calls_per_sec(&self) -> f64 {
        self.recent as f64 * 1000.0 / self.window_ms.max(1) as f64
    }
}

/// A node's answer to `NodeCall::Status`: every range it replicates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeReport {
    pub node: u64,
    pub ranges: Vec<RangeReport>,
}

/// Any call one node makes to another over the peer connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerCall {
    Range(RangeCall),
    Gateway(GatewayCall),
    Node(NodeCall),
}

/// Result of a `PeerCall`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerReply {
    Range(RangeReply),
    Gateway(GatewayReply),
    Node(NodeReport),
    Hosted(bool),
    Moving(MoveReply),
    Retired(bool),
    Enrolled(bool),
}

fn corrupt(what: &str) -> RangeError {
    RangeError::Corrupt(format!("range call frame: {what}"))
}

struct Writer(Vec<u8>);
impl Writer {
    fn u8(&mut self, v: u8) {
        self.0.push(v);
    }
    fn u64(&mut self, v: u64) {
        self.0.extend_from_slice(&v.to_be_bytes());
    }
    fn len(&mut self, n: usize) -> Result<(), RangeError> {
        let n = u32::try_from(n).map_err(|_| corrupt("field longer than u32"))?;
        self.0.extend_from_slice(&n.to_be_bytes());
        Ok(())
    }
    fn bytes(&mut self, v: &[u8]) -> Result<(), RangeError> {
        self.len(v.len())?;
        self.0.extend_from_slice(v);
        Ok(())
    }
    fn str(&mut self, v: &str) -> Result<(), RangeError> {
        self.bytes(v.as_bytes())
    }
    fn opt_u64(&mut self, v: Option<u64>) {
        match v {
            None => self.u8(0),
            Some(v) => {
                self.u8(1);
                self.u64(v);
            }
        }
    }
    fn opt_bytes(&mut self, v: Option<&[u8]>) -> Result<(), RangeError> {
        match v {
            None => self.u8(0),
            Some(v) => {
                self.u8(1);
                self.bytes(v)?;
            }
        }
        Ok(())
    }
    fn bound(&mut self, v: &Bound<Vec<u8>>) -> Result<(), RangeError> {
        match v {
            Bound::Unbounded => self.u8(0),
            Bound::Included(k) => {
                self.u8(1);
                self.bytes(k)?;
            }
            Bound::Excluded(k) => {
                self.u8(2);
                self.bytes(k)?;
            }
        }
        Ok(())
    }
    fn descriptor(&mut self, d: &RangeDescriptor) -> Result<(), RangeError> {
        self.bytes(&d.encode()?)
    }
    fn txns(&mut self, txns: &[ClusterTxnId]) -> Result<(), RangeError> {
        self.len(txns.len())?;
        for t in txns {
            self.u64(t.0);
        }
        Ok(())
    }
    fn writes(&mut self, writes: &[RangeWrite]) -> Result<(), RangeError> {
        self.len(writes.len())?;
        for w in writes {
            self.u8(w.space.tag());
            self.bytes(&w.key)?;
            self.opt_bytes(w.value.as_deref())?;
        }
        Ok(())
    }
    fn outcome(&mut self, o: &Outcome) {
        match o {
            Outcome::Commit(ts) => {
                self.u8(0);
                self.u64(ts.0);
            }
            Outcome::Abort => self.u8(1),
        }
    }
    fn ranges(&mut self, ranges: &[RangeId]) -> Result<(), RangeError> {
        self.len(ranges.len())?;
        for r in ranges {
            self.u64(r.0);
        }
        Ok(())
    }
}

struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], RangeError> {
        if self.0.len() < n {
            return Err(corrupt("truncated"));
        }
        let (head, rest) = self.0.split_at(n);
        self.0 = rest;
        Ok(head)
    }
    fn u8(&mut self) -> Result<u8, RangeError> {
        Ok(self.take(1)?[0])
    }
    fn u64(&mut self) -> Result<u64, RangeError> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().expect("8 bytes"),
        ))
    }
    /// A count of items that each take at least `min` bytes (bounds the allocation).
    fn count(&mut self, min: usize) -> Result<usize, RangeError> {
        let n = u32::from_be_bytes(self.take(4)?.try_into().expect("4 bytes")) as usize;
        if n.saturating_mul(min) > self.0.len() {
            return Err(corrupt("truncated list"));
        }
        Ok(n)
    }
    fn bytes(&mut self) -> Result<Vec<u8>, RangeError> {
        let n = self.count(1)?;
        Ok(self.take(n)?.to_vec())
    }
    fn string(&mut self) -> Result<String, RangeError> {
        String::from_utf8(self.bytes()?).map_err(|_| corrupt("invalid UTF-8"))
    }
    fn flag(&mut self) -> Result<bool, RangeError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(corrupt("invalid flag")),
        }
    }
    fn opt_u64(&mut self) -> Result<Option<u64>, RangeError> {
        Ok(if self.flag()? {
            Some(self.u64()?)
        } else {
            None
        })
    }
    fn opt_bytes(&mut self) -> Result<Option<Vec<u8>>, RangeError> {
        Ok(if self.flag()? {
            Some(self.bytes()?)
        } else {
            None
        })
    }
    fn bound(&mut self) -> Result<Bound<Vec<u8>>, RangeError> {
        Ok(match self.u8()? {
            0 => Bound::Unbounded,
            1 => Bound::Included(self.bytes()?),
            2 => Bound::Excluded(self.bytes()?),
            _ => return Err(corrupt("invalid bound")),
        })
    }
    fn space(&mut self) -> Result<RangeSpace, RangeError> {
        RangeSpace::from_tag(self.u8()?).ok_or_else(|| corrupt("unknown keyspace"))
    }
    fn range(&mut self) -> Result<RangeId, RangeError> {
        Ok(RangeId(self.u64()?))
    }
    fn txn(&mut self) -> Result<ClusterTxnId, RangeError> {
        Ok(ClusterTxnId(self.u64()?))
    }
    fn ts(&mut self) -> Result<Timestamp, RangeError> {
        Ok(Timestamp(self.u64()?))
    }
    fn descriptor(&mut self) -> Result<RangeDescriptor, RangeError> {
        RangeDescriptor::decode(&self.bytes()?)
    }
    fn txns(&mut self) -> Result<Vec<ClusterTxnId>, RangeError> {
        let n = self.count(8)?;
        (0..n).map(|_| self.txn()).collect()
    }
    fn writes(&mut self) -> Result<Vec<RangeWrite>, RangeError> {
        let n = self.count(6)?;
        (0..n)
            .map(|_| {
                Ok(RangeWrite {
                    space: self.space()?,
                    key: self.bytes()?,
                    value: self.opt_bytes()?,
                })
            })
            .collect()
    }
    fn outcome(&mut self) -> Result<Outcome, RangeError> {
        Ok(match self.u8()? {
            0 => Outcome::Commit(self.ts()?),
            1 => Outcome::Abort,
            _ => return Err(corrupt("invalid outcome")),
        })
    }
    fn ranges(&mut self) -> Result<Vec<RangeId>, RangeError> {
        let n = self.count(8)?;
        (0..n).map(|_| self.range()).collect()
    }
    fn finish<T>(self, value: T) -> Result<T, RangeError> {
        if self.0.is_empty() {
            Ok(value)
        } else {
            Err(corrupt("trailing bytes"))
        }
    }
}

fn lease_kind(tag: u8) -> Result<LeaseKind, RangeError> {
    Ok(match tag {
        0 => LeaseKind::ObjectId,
        1 => LeaseKind::HeapRowId,
        3 => LeaseKind::RowVersion,
        4 => LeaseKind::Xid,
        _ => return Err(corrupt("unknown lease kind")),
    })
}

fn put_home_op(w: &mut Writer, op: &HomeOp) -> Result<(), RangeError> {
    match op {
        HomeOp::Open { txn } => {
            w.u8(0);
            w.u64(txn.0);
        }
        HomeOp::Seal {
            txn,
            incarnation,
            participants,
        } => {
            w.u8(1);
            w.u64(txn.0);
            w.u64(*incarnation);
            w.ranges(participants)?;
        }
        HomeOp::Vote {
            txn,
            incarnation,
            participant,
            prepared_at,
        } => {
            w.u8(2);
            w.u64(txn.0);
            w.u64(*incarnation);
            w.u64(participant.0);
            w.u64(prepared_at.0);
        }
        HomeOp::Decide {
            txn,
            incarnation,
            outcome,
        } => {
            w.u8(3);
            w.u64(txn.0);
            w.u64(*incarnation);
            w.outcome(outcome);
        }
        HomeOp::Status { txn } => {
            w.u8(4);
            w.u64(txn.0);
        }
        HomeOp::Forget { txn, incarnation } => {
            w.u8(5);
            w.u64(txn.0);
            w.u64(*incarnation);
        }
    }
    Ok(())
}

fn home_op(r: &mut Reader<'_>) -> Result<HomeOp, RangeError> {
    Ok(match r.u8()? {
        0 => HomeOp::Open { txn: r.txn()? },
        1 => HomeOp::Seal {
            txn: r.txn()?,
            incarnation: r.u64()?,
            participants: r.ranges()?,
        },
        2 => HomeOp::Vote {
            txn: r.txn()?,
            incarnation: r.u64()?,
            participant: r.range()?,
            prepared_at: r.ts()?,
        },
        3 => HomeOp::Decide {
            txn: r.txn()?,
            incarnation: r.u64()?,
            outcome: r.outcome()?,
        },
        4 => HomeOp::Status { txn: r.txn()? },
        5 => HomeOp::Forget {
            txn: r.txn()?,
            incarnation: r.u64()?,
        },
        _ => return Err(corrupt("unknown home op")),
    })
}

fn put_range_call(w: &mut Writer, call: &RangeCall) -> Result<(), RangeError> {
    match call {
        RangeCall::Descriptor { range } => {
            w.u8(1);
            w.u64(range.0);
        }
        RangeCall::Read(req) => {
            w.u8(2);
            w.u64(req.range.0);
            w.u64(req.epoch.0);
            w.u8(req.space.tag());
            w.bound(&req.start)?;
            w.bound(&req.end)?;
            w.u8(match req.dir {
                ReadDirection::Forward => 0,
                ReadDirection::Reverse => 1,
            });
            w.u64(req.ts.0);
            w.u64(req.page_bytes as u64);
        }
        RangeCall::Lock(req) => {
            w.u8(3);
            w.u64(req.range.0);
            w.u64(req.epoch.0);
            w.u64(req.txn.0);
            w.u64(req.priority.0);
            match &req.target {
                LockTarget::Key { space, key } => {
                    w.u8(0);
                    w.u8(space.tag());
                    w.bytes(key)?;
                }
                LockTarget::TableSentinel { table } => {
                    w.u8(1);
                    w.u64(*table);
                }
                LockTarget::Interval { space, start, end } => {
                    w.u8(2);
                    w.u8(space.tag());
                    w.bound(start)?;
                    w.bound(end)?;
                }
            }
            w.u8(match req.mode {
                RangeLockMode::Shared => 0,
                RangeLockMode::Exclusive => 1,
            });
            w.opt_u64(req.timeout_ms);
        }
        RangeCall::Blockers { range, txn }
        | RangeCall::CancelWait { range, txn }
        | RangeCall::Release { range, txn }
        | RangeCall::Heartbeat { range, txn } => {
            w.u8(match call {
                RangeCall::Blockers { .. } => 4,
                RangeCall::CancelWait { .. } => 5,
                RangeCall::Release { .. } => 6,
                _ => 7,
            });
            w.u64(range.0);
            w.u64(txn.0);
        }
        RangeCall::Commit {
            range,
            epoch,
            txn,
            writes,
            marker_xid,
            lock_generation,
        } => {
            w.u8(8);
            w.u64(range.0);
            w.u64(epoch.0);
            w.u64(txn.0);
            w.writes(writes)?;
            w.opt_u64(*marker_xid);
            w.opt_u64(*lock_generation);
        }
        RangeCall::Prepare {
            range,
            epoch,
            txn,
            home,
            writes,
            lock_generation,
        } => {
            w.u8(9);
            w.u64(range.0);
            w.u64(epoch.0);
            w.u64(txn.0);
            w.u64(home.0);
            w.writes(writes)?;
            w.opt_u64(*lock_generation);
        }
        RangeCall::Finalize {
            range,
            txn,
            outcome,
        } => {
            w.u8(10);
            w.u64(range.0);
            w.u64(txn.0);
            w.outcome(outcome);
        }
        RangeCall::Home { range, epoch, op } => {
            w.u8(11);
            w.u64(range.0);
            w.u64(epoch.0);
            put_home_op(w, op)?;
        }
        RangeCall::OracleNext { range, n } => {
            w.u8(12);
            w.u64(range.0);
            w.0.extend_from_slice(&n.to_be_bytes());
        }
        RangeCall::Lease {
            range,
            kind,
            id,
            count,
        } => {
            w.u8(13);
            w.u64(range.0);
            w.u8(*kind as u8);
            w.u64(*id);
            w.u64(*count);
        }
        RangeCall::Sequence {
            range,
            id,
            incarnation,
            op,
        } => {
            w.u8(14);
            w.u64(range.0);
            w.u64(*id);
            w.0.extend_from_slice(&incarnation.to_be_bytes());
            match op {
                SequenceOp::Next => w.u8(0),
                SequenceOp::Set { value, called } => {
                    w.u8(1);
                    w.0.extend_from_slice(&value.to_be_bytes());
                    w.u8(u8::from(*called));
                }
                SequenceOp::Forget => w.u8(2),
            }
        }
    }
    Ok(())
}

fn range_call(tag: u8, r: &mut Reader<'_>) -> Result<RangeCall, RangeError> {
    Ok(match tag {
        1 => RangeCall::Descriptor { range: r.range()? },
        2 => RangeCall::Read(ReadRequest {
            range: r.range()?,
            epoch: Epoch(r.u64()?),
            space: r.space()?,
            start: r.bound()?,
            end: r.bound()?,
            dir: match r.u8()? {
                0 => ReadDirection::Forward,
                1 => ReadDirection::Reverse,
                _ => return Err(corrupt("invalid read direction")),
            },
            ts: r.ts()?,
            page_bytes: usize::try_from(r.u64()?).map_err(|_| corrupt("page size"))?,
        }),
        3 => RangeCall::Lock(LockRequest {
            range: r.range()?,
            epoch: Epoch(r.u64()?),
            txn: r.txn()?,
            priority: r.ts()?,
            target: match r.u8()? {
                0 => LockTarget::Key {
                    space: r.space()?,
                    key: r.bytes()?,
                },
                1 => LockTarget::TableSentinel { table: r.u64()? },
                2 => LockTarget::Interval {
                    space: r.space()?,
                    start: r.bound()?,
                    end: r.bound()?,
                },
                _ => return Err(corrupt("invalid lock target")),
            },
            mode: match r.u8()? {
                0 => RangeLockMode::Shared,
                1 => RangeLockMode::Exclusive,
                _ => return Err(corrupt("invalid lock mode")),
            },
            timeout_ms: r.opt_u64()?,
        }),
        4..=7 => {
            let (range, txn) = (r.range()?, r.txn()?);
            match tag {
                4 => RangeCall::Blockers { range, txn },
                5 => RangeCall::CancelWait { range, txn },
                6 => RangeCall::Release { range, txn },
                _ => RangeCall::Heartbeat { range, txn },
            }
        }
        8 => RangeCall::Commit {
            range: r.range()?,
            epoch: Epoch(r.u64()?),
            txn: r.txn()?,
            writes: r.writes()?,
            marker_xid: r.opt_u64()?,
            lock_generation: r.opt_u64()?,
        },
        9 => RangeCall::Prepare {
            range: r.range()?,
            epoch: Epoch(r.u64()?),
            txn: r.txn()?,
            home: r.range()?,
            writes: r.writes()?,
            lock_generation: r.opt_u64()?,
        },
        10 => RangeCall::Finalize {
            range: r.range()?,
            txn: r.txn()?,
            outcome: r.outcome()?,
        },
        11 => RangeCall::Home {
            range: r.range()?,
            epoch: Epoch(r.u64()?),
            op: home_op(r)?,
        },
        12 => RangeCall::OracleNext {
            range: r.range()?,
            n: match u32::from_be_bytes(r.take(4)?.try_into().expect("4 bytes")) {
                0 => return Err(corrupt("oracle request for zero timestamps")),
                n => n,
            },
        },
        13 => RangeCall::Lease {
            range: r.range()?,
            kind: lease_kind(r.u8()?)?,
            id: r.u64()?,
            count: r.u64()?,
        },
        14 => RangeCall::Sequence {
            range: r.range()?,
            id: r.u64()?,
            incarnation: u128::from_be_bytes(r.take(16)?.try_into().expect("16 bytes")),
            op: match r.u8()? {
                0 => SequenceOp::Next,
                1 => SequenceOp::Set {
                    value: r.u64()? as i64,
                    called: r.flag()?,
                },
                2 => SequenceOp::Forget,
                _ => return Err(corrupt("unknown sequence op")),
            },
        },
        _ => return Err(corrupt("unknown call")),
    })
}

/// Encodes a call body.
pub fn encode_call(call: &PeerCall) -> Result<Vec<u8>, RangeError> {
    let mut w = Writer(vec![VERSION]);
    match call {
        PeerCall::Range(call) => put_range_call(&mut w, call)?,
        PeerCall::Gateway(call) => match call {
            GatewayCall::Probe(p) => {
                w.u8(32);
                w.u64(p.initiator.0);
                w.len(p.path.len())?;
                for (txn, ts) in &p.path {
                    w.u64(txn.0);
                    w.u64(ts.0);
                }
                w.u64(p.target.0);
            }
            GatewayCall::Abort(txn)
            | GatewayCall::Blockers(txn)
            | GatewayCall::CancelWait(txn)
            | GatewayCall::TxnLive(txn) => {
                w.u8(match call {
                    GatewayCall::Abort(_) => 33,
                    GatewayCall::Blockers(_) => 34,
                    GatewayCall::CancelWait(_) => 35,
                    _ => 36,
                });
                w.u64(txn.0);
            }
        },
        PeerCall::Node(NodeCall::MoveRange { range, from, to }) => {
            w.u8(50);
            w.u64(range.0);
            w.u64(*from);
            w.u64(*to);
        }
        PeerCall::Node(NodeCall::RetireNode { id }) => {
            w.u8(51);
            w.u64(*id);
        }
        PeerCall::Node(NodeCall::EnrolNode {
            id,
            address,
            fingerprint,
        }) => {
            w.u8(52);
            w.u64(*id);
            w.str(address)?;
            w.str(fingerprint)?;
        }
        PeerCall::Node(NodeCall::Status) => w.u8(48),
        PeerCall::Node(NodeCall::HostRange(desc)) => {
            w.u8(49);
            w.descriptor(desc)?;
        }
    }
    Ok(w.0)
}

/// Decodes a call body.
pub fn decode_call(bytes: &[u8]) -> Result<PeerCall, RangeError> {
    let mut r = Reader(bytes);
    if r.u8()? != VERSION {
        return Err(corrupt("unknown version"));
    }
    let call = match r.u8()? {
        32 => {
            let initiator = r.txn()?;
            let n = r.count(16)?;
            let path = (0..n)
                .map(|_| Ok((r.txn()?, r.ts()?)))
                .collect::<Result<_, RangeError>>()?;
            PeerCall::Gateway(GatewayCall::Probe(DeadlockProbe {
                initiator,
                path,
                target: r.txn()?,
            }))
        }
        33 => PeerCall::Gateway(GatewayCall::Abort(r.txn()?)),
        34 => PeerCall::Gateway(GatewayCall::Blockers(r.txn()?)),
        35 => PeerCall::Gateway(GatewayCall::CancelWait(r.txn()?)),
        36 => PeerCall::Gateway(GatewayCall::TxnLive(r.txn()?)),
        50 => PeerCall::Node(NodeCall::MoveRange {
            range: RangeId(r.u64()?),
            from: r.u64()?,
            to: r.u64()?,
        }),
        51 => PeerCall::Node(NodeCall::RetireNode { id: r.u64()? }),
        52 => PeerCall::Node(NodeCall::EnrolNode {
            id: r.u64()?,
            address: r.string()?,
            fingerprint: r.string()?,
        }),
        48 => PeerCall::Node(NodeCall::Status),
        49 => PeerCall::Node(NodeCall::HostRange(r.descriptor()?)),
        tag => PeerCall::Range(range_call(tag, &mut r)?),
    };
    r.finish(call)
}

fn put_error(w: &mut Writer, e: &RangeError) -> Result<(), RangeError> {
    match e {
        RangeError::NotLeader { hint } => {
            w.u8(0);
            w.opt_u64(*hint);
        }
        RangeError::StaleEpoch { descriptor } => {
            w.u8(1);
            w.descriptor(descriptor)?;
        }
        RangeError::Aborted(s) => {
            w.u8(2);
            w.str(s)?;
        }
        RangeError::Deadlock => w.u8(3),
        RangeError::LockTimeout => w.u8(4),
        RangeError::Cancelled => w.u8(5),
        RangeError::Conflict(s) => {
            w.u8(6);
            w.str(s)?;
        }
        RangeError::Unavailable(s) => {
            w.u8(7);
            w.str(s)?;
        }
        RangeError::Unknown(s) => {
            w.u8(8);
            w.str(s)?;
        }
        RangeError::Corrupt(s) => {
            w.u8(9);
            w.str(s)?;
        }
    }
    Ok(())
}

fn error(r: &mut Reader<'_>) -> Result<RangeError, RangeError> {
    Ok(match r.u8()? {
        0 => RangeError::NotLeader { hint: r.opt_u64()? },
        1 => RangeError::StaleEpoch {
            descriptor: r.descriptor()?,
        },
        2 => RangeError::Aborted(r.string()?),
        3 => RangeError::Deadlock,
        4 => RangeError::LockTimeout,
        5 => RangeError::Cancelled,
        6 => RangeError::Conflict(r.string()?),
        7 => RangeError::Unavailable(r.string()?),
        8 => RangeError::Unknown(r.string()?),
        9 => RangeError::Corrupt(r.string()?),
        _ => return Err(corrupt("unknown error")),
    })
}

fn put_range_reply(w: &mut Writer, reply: &RangeReply) -> Result<(), RangeError> {
    match reply {
        RangeReply::Descriptor(d) => {
            w.u8(1);
            w.descriptor(d)?;
        }
        RangeReply::Read(page) => {
            w.u8(2);
            w.len(page.pairs.len())?;
            for (k, v) in &page.pairs {
                w.bytes(k)?;
                w.bytes(v)?;
            }
            match &page.resume {
                None => w.u8(0),
                Some(b) => {
                    w.u8(1);
                    w.bound(b)?;
                }
            }
            w.opt_bytes(page.range_end.as_deref())?;
        }
        RangeReply::Lock(grant) => {
            w.u8(3);
            match &grant.latest {
                None => w.u8(0),
                Some((ts, value)) => {
                    w.u8(1);
                    w.u64(ts.0);
                    w.opt_bytes(value.as_deref())?;
                }
            }
            w.u64(grant.generation);
        }
        RangeReply::Blockers(txns) => {
            w.u8(4);
            w.txns(txns)?;
        }
        RangeReply::CancelWait => w.u8(5),
        RangeReply::Release => w.u8(6),
        RangeReply::Heartbeat => w.u8(7),
        RangeReply::Commit(ts) => {
            w.u8(8);
            w.u64(ts.0);
        }
        RangeReply::Prepare(ts) => {
            w.u8(9);
            w.u64(ts.0);
        }
        RangeReply::Finalize => w.u8(10),
        RangeReply::Home(reply) => {
            w.u8(11);
            match reply {
                HomeReply::Ok => w.u8(0),
                HomeReply::Opened { incarnation } => {
                    w.u8(1);
                    w.u64(*incarnation);
                }
                HomeReply::Status(status) => {
                    w.u8(2);
                    match status {
                        HomeStatus::Unknown => w.u8(0),
                        HomeStatus::Open { sealed, votes } => {
                            w.u8(1);
                            match sealed {
                                None => w.u8(0),
                                Some(ranges) => {
                                    w.u8(1);
                                    w.ranges(ranges)?;
                                }
                            }
                            w.len(votes.len())?;
                            for (range, ts) in votes {
                                w.u64(range.0);
                                w.u64(ts.0);
                            }
                        }
                        HomeStatus::Decided(o) => {
                            w.u8(2);
                            w.outcome(o);
                        }
                    }
                }
            }
        }
        RangeReply::OracleNext(ts) => {
            w.u8(12);
            w.u64(ts.0);
        }
        RangeReply::Lease(range) => {
            w.u8(13);
            w.u64(range.start);
            w.u64(range.end);
        }
        RangeReply::Sequence(reply) => {
            w.u8(14);
            match reply {
                SequenceReply::Value(v) => {
                    w.u8(0);
                    w.0.extend_from_slice(&v.to_be_bytes());
                }
                SequenceReply::Gone => w.u8(1),
                SequenceReply::Limit => w.u8(2),
                SequenceReply::OutOfRange => w.u8(3),
            }
        }
    }
    Ok(())
}

fn range_reply(tag: u8, r: &mut Reader<'_>) -> Result<RangeReply, RangeError> {
    Ok(match tag {
        1 => RangeReply::Descriptor(r.descriptor()?),
        2 => {
            let n = r.count(8)?;
            let pairs = (0..n)
                .map(|_| Ok((r.bytes()?, r.bytes()?)))
                .collect::<Result<_, RangeError>>()?;
            let resume = if r.flag()? { Some(r.bound()?) } else { None };
            RangeReply::Read(ReadPage {
                pairs,
                resume,
                range_end: r.opt_bytes()?,
            })
        }
        3 => RangeReply::Lock(LockGrant {
            latest: if r.flag()? {
                Some((r.ts()?, r.opt_bytes()?))
            } else {
                None
            },
            generation: r.u64()?,
        }),
        4 => RangeReply::Blockers(r.txns()?),
        5 => RangeReply::CancelWait,
        6 => RangeReply::Release,
        7 => RangeReply::Heartbeat,
        8 => RangeReply::Commit(r.ts()?),
        9 => RangeReply::Prepare(r.ts()?),
        10 => RangeReply::Finalize,
        11 => RangeReply::Home(match r.u8()? {
            0 => HomeReply::Ok,
            1 => HomeReply::Opened {
                incarnation: r.u64()?,
            },
            2 => HomeReply::Status(match r.u8()? {
                0 => HomeStatus::Unknown,
                1 => {
                    let sealed = if r.flag()? { Some(r.ranges()?) } else { None };
                    let n = r.count(16)?;
                    let votes = (0..n)
                        .map(|_| Ok((r.range()?, r.ts()?)))
                        .collect::<Result<_, RangeError>>()?;
                    HomeStatus::Open { sealed, votes }
                }
                2 => HomeStatus::Decided(r.outcome()?),
                _ => return Err(corrupt("unknown home status")),
            }),
            _ => return Err(corrupt("unknown home reply")),
        }),
        12 => RangeReply::OracleNext(r.ts()?),
        13 => {
            let (start, end) = (r.u64()?, r.u64()?);
            if start > end {
                return Err(corrupt("lease range start after end"));
            }
            RangeReply::Lease(Range { start, end })
        }
        14 => RangeReply::Sequence(match r.u8()? {
            0 => SequenceReply::Value(r.u64()? as i64),
            1 => SequenceReply::Gone,
            2 => SequenceReply::Limit,
            3 => SequenceReply::OutOfRange,
            _ => return Err(corrupt("unknown sequence reply")),
        }),
        _ => return Err(corrupt("unknown reply")),
    })
}

fn put_node_report(w: &mut Writer, report: &NodeReport) -> Result<(), RangeError> {
    w.u64(report.node);
    w.len(report.ranges.len())?;
    for range in &report.ranges {
        w.descriptor(&range.descriptor)?;
        w.u8(match range.role {
            RangeRole::Leader => 0,
            RangeRole::Follower => 1,
            RangeRole::Candidate => 2,
        });
        w.u64(range.term);
        w.u64(range.leader);
        w.u64(range.applied);
        w.opt_u64(range.size);
        let c = &range.calls;
        for v in [
            c.reads,
            c.locks,
            c.writes,
            c.other,
            range.recent,
            range.window_ms,
        ] {
            w.u64(v);
        }
        w.opt_bytes(range.move_step.as_deref().map(str::as_bytes))?;
    }
    Ok(())
}

fn node_report(r: &mut Reader<'_>, version: u8) -> Result<NodeReport, RangeError> {
    let node = r.u64()?;
    // Each range takes at least 4 + 1 + 3 * 8 + 1 + 6 * 8 bytes.
    let n = r.count(78)?;
    let ranges = (0..n)
        .map(|_| {
            Ok(RangeReport {
                descriptor: r.descriptor()?,
                role: match r.u8()? {
                    0 => RangeRole::Leader,
                    1 => RangeRole::Follower,
                    2 => RangeRole::Candidate,
                    _ => return Err(corrupt("unknown range role")),
                },
                term: r.u64()?,
                leader: r.u64()?,
                applied: r.u64()?,
                size: r.opt_u64()?,
                calls: CallCounts {
                    reads: r.u64()?,
                    locks: r.u64()?,
                    writes: r.u64()?,
                    other: r.u64()?,
                },
                recent: r.u64()?,
                window_ms: r.u64()?,
                move_step: if version == 1 {
                    None
                } else {
                    optional_string(r)?
                },
            })
        })
        .collect::<Result<_, RangeError>>()?;
    Ok(NodeReport { node, ranges })
}

fn optional_string(r: &mut Reader<'_>) -> Result<Option<String>, RangeError> {
    r.opt_bytes()?
        .map(|b| String::from_utf8(b).map_err(|_| corrupt("invalid UTF-8")))
        .transpose()
}

/// Encodes a reply body.
pub fn encode_reply(reply: &Result<PeerReply, RangeError>) -> Result<Vec<u8>, RangeError> {
    let mut w = Writer(vec![VERSION]);
    match reply {
        Ok(PeerReply::Range(reply)) => {
            w.u8(0);
            put_range_reply(&mut w, reply)?;
        }
        Ok(PeerReply::Gateway(GatewayReply::Done)) => {
            w.u8(0);
            w.u8(32);
        }
        Ok(PeerReply::Gateway(GatewayReply::Blockers(txns))) => {
            w.u8(0);
            w.u8(33);
            w.txns(txns)?;
        }
        Ok(PeerReply::Gateway(GatewayReply::Live(live))) => {
            w.u8(0);
            w.u8(34);
            w.u8(u8::from(*live));
        }
        Ok(PeerReply::Moving(reply)) => {
            w.u8(0);
            w.u8(50);
            w.opt_u64(reply.leader);
            w.opt_bytes(reply.step.as_deref().map(str::as_bytes))?;
            w.u8(u8::from(reply.done));
            w.opt_bytes(reply.error.as_deref().map(str::as_bytes))?;
        }
        Ok(PeerReply::Retired(changed) | PeerReply::Enrolled(changed)) => {
            w.u8(0);
            w.u8(if matches!(reply, Ok(PeerReply::Retired(_))) {
                51
            } else {
                52
            });
            w.u8(u8::from(*changed));
        }
        Ok(PeerReply::Hosted(created)) => {
            w.u8(0);
            w.u8(49);
            w.u8(u8::from(*created));
        }
        Ok(PeerReply::Node(report)) => {
            w.0[0] = 2; // Extended reports: old strict decoders reject the version.
            w.u8(0);
            w.u8(48);
            put_node_report(&mut w, report)?;
        }
        Err(e) => {
            w.u8(1);
            put_error(&mut w, e)?;
        }
    }
    Ok(w.0)
}

/// Decodes a reply body. The outer `Err` means the body itself is invalid.
pub fn decode_reply(bytes: &[u8]) -> Result<Result<PeerReply, RangeError>, RangeError> {
    let mut r = Reader(bytes);
    let version = r.u8()?;
    if version != VERSION && version != 2 {
        return Err(corrupt("unknown version"));
    }
    if version == 2 && !bytes.starts_with(&[2, 0, 48]) {
        return Err(corrupt("version 2 is only for node reports"));
    }
    let reply = if r.flag()? {
        Err(error(&mut r)?)
    } else {
        Ok(match r.u8()? {
            32 => PeerReply::Gateway(GatewayReply::Done),
            33 => PeerReply::Gateway(GatewayReply::Blockers(r.txns()?)),
            34 => PeerReply::Gateway(GatewayReply::Live(r.flag()?)),
            48 => PeerReply::Node(node_report(&mut r, version)?),
            50 => PeerReply::Moving(MoveReply {
                leader: r.opt_u64()?,
                step: optional_string(&mut r)?,
                done: r.flag()?,
                error: optional_string(&mut r)?,
            }),
            49 => PeerReply::Hosted(r.flag()?),
            51 => PeerReply::Retired(r.flag()?),
            52 => PeerReply::Enrolled(r.flag()?),
            tag => PeerReply::Range(range_reply(tag, &mut r)?),
        })
    };
    r.finish(reply)
}
