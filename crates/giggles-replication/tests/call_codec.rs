//! Range call wire codec (GD-0075 R5-21): every call, reply and error round-trips; the
//! decoder is strict (version, tags, truncation, trailing bytes).
use std::ops::Bound;

use giggles_core::range_api::{
    ClusterTxnId, DeadlockProbe, Epoch, HomeOp, HomeReply, HomeStatus, LeaseKind, LockGrant,
    LockRequest, LockTarget, Outcome, RangeDescriptor, RangeError, RangeId, RangeLockMode,
    RangeSpace, RangeWrite, ReadDirection, ReadPage, ReadRequest, ReplicaDesc, ReplicaRole,
    Timestamp,
};
use giggles_replication::{
    call_codec::{
        GatewayCall, GatewayReply, PeerCall, PeerReply, decode_call, decode_reply, encode_call,
        encode_reply,
    },
    client::{RangeCall, RangeReply},
};

fn desc() -> RangeDescriptor {
    RangeDescriptor {
        id: RangeId(7),
        epoch: Epoch(3),
        start: vec![0x80, 1],
        end: Some(vec![0x80, 9]),
        replicas: vec![
            ReplicaDesc {
                node: 1,
                role: ReplicaRole::Voter,
            },
            ReplicaDesc {
                node: 4,
                role: ReplicaRole::Learner,
            },
        ],
    }
}

fn calls() -> Vec<PeerCall> {
    let (range, epoch, txn) = (RangeId(7), Epoch(3), ClusterTxnId(0x0002_0000_0000_0011));
    let writes = vec![
        RangeWrite {
            space: RangeSpace::Rows,
            key: vec![0, 0, 0, 0, 0, 0, 0, 9, 1],
            value: Some(b"row".to_vec()),
        },
        RangeWrite {
            space: RangeSpace::Index,
            key: vec![2; 9],
            value: None,
        },
    ];
    let mut out: Vec<PeerCall> = [
        RangeCall::Descriptor { range },
        RangeCall::Read(ReadRequest {
            range,
            epoch,
            space: RangeSpace::Rows,
            start: Bound::Included(vec![1, 2]),
            end: Bound::Unbounded,
            dir: ReadDirection::Reverse,
            ts: Timestamp(99),
            page_bytes: 65536,
        }),
        RangeCall::Read(ReadRequest {
            range,
            epoch,
            space: RangeSpace::Catalog,
            start: Bound::Unbounded,
            end: Bound::Excluded(vec![]),
            dir: ReadDirection::Forward,
            ts: Timestamp(1),
            page_bytes: 0,
        }),
        RangeCall::Lock(LockRequest {
            range,
            epoch,
            txn,
            priority: Timestamp(5),
            target: LockTarget::Key {
                space: RangeSpace::Index,
                key: vec![3; 12],
            },
            mode: RangeLockMode::Shared,
            timeout_ms: Some(250),
        }),
        RangeCall::Lock(LockRequest {
            range,
            epoch,
            txn,
            priority: Timestamp(5),
            target: LockTarget::TableSentinel { table: u64::MAX },
            mode: RangeLockMode::Exclusive,
            timeout_ms: None,
        }),
        RangeCall::Blockers { range, txn },
        RangeCall::CancelWait { range, txn },
        RangeCall::Release { range, txn },
        RangeCall::Heartbeat { range, txn },
        RangeCall::Commit {
            range,
            epoch,
            txn,
            writes: writes.clone(),
            marker_xid: Some(77),
        },
        RangeCall::Commit {
            range,
            epoch,
            txn,
            writes: Vec::new(),
            marker_xid: None,
        },
        RangeCall::Prepare {
            range,
            epoch,
            txn,
            home: RangeId(2),
            writes,
        },
        RangeCall::Finalize {
            range,
            txn,
            outcome: Outcome::Commit(Timestamp(123)),
        },
        RangeCall::Finalize {
            range,
            txn,
            outcome: Outcome::Abort,
        },
        RangeCall::OracleNext { range, n: 64 },
        RangeCall::Lease {
            range,
            kind: LeaseKind::RowVersion,
            id: 5,
            count: 1000,
        },
    ]
    .into_iter()
    .map(PeerCall::Range)
    .collect();
    for op in [
        HomeOp::Open { txn },
        HomeOp::Seal {
            txn,
            incarnation: 4,
            participants: vec![RangeId(2), RangeId(7)],
        },
        HomeOp::Vote {
            txn,
            incarnation: 4,
            participant: RangeId(7),
            prepared_at: Timestamp(50),
        },
        HomeOp::Decide {
            txn,
            incarnation: 4,
            outcome: Outcome::Commit(Timestamp(60)),
        },
        HomeOp::Status { txn },
        HomeOp::Forget {
            txn,
            incarnation: 4,
        },
    ] {
        out.push(PeerCall::Range(RangeCall::Home { range, epoch, op }));
    }
    out.extend([
        PeerCall::Gateway(GatewayCall::Probe(DeadlockProbe {
            initiator: txn,
            path: vec![(txn, Timestamp(1)), (ClusterTxnId(9), Timestamp(2))],
            target: ClusterTxnId(9),
        })),
        PeerCall::Gateway(GatewayCall::Abort(txn)),
        PeerCall::Gateway(GatewayCall::Blockers(txn)),
        PeerCall::Gateway(GatewayCall::CancelWait(txn)),
    ]);
    out
}

fn replies() -> Vec<Result<PeerReply, RangeError>> {
    let mut out: Vec<Result<PeerReply, RangeError>> = [
        RangeReply::Descriptor(desc()),
        RangeReply::Read(ReadPage {
            pairs: vec![(vec![1, 2], vec![0, 9]), (vec![3], vec![1])],
            resume: Some(Bound::Excluded(vec![3])),
            range_end: Some(vec![0x80, 9]),
        }),
        RangeReply::Read(ReadPage {
            pairs: Vec::new(),
            resume: None,
            range_end: None,
        }),
        RangeReply::Lock(LockGrant {
            latest: Some((Timestamp(8), Some(b"v".to_vec()))),
        }),
        RangeReply::Lock(LockGrant {
            latest: Some((Timestamp(8), None)),
        }),
        RangeReply::Lock(LockGrant { latest: None }),
        RangeReply::Blockers(vec![ClusterTxnId(1), ClusterTxnId(2)]),
        RangeReply::CancelWait,
        RangeReply::Release,
        RangeReply::Heartbeat,
        RangeReply::Commit(Timestamp(10)),
        RangeReply::Prepare(Timestamp(11)),
        RangeReply::Finalize,
        RangeReply::Home(HomeReply::Ok),
        RangeReply::Home(HomeReply::Opened { incarnation: 12 }),
        RangeReply::Home(HomeReply::Status(HomeStatus::Unknown)),
        RangeReply::Home(HomeReply::Status(HomeStatus::Open {
            sealed: Some(vec![RangeId(1), RangeId(2)]),
            votes: vec![(RangeId(1), Timestamp(3))],
        })),
        RangeReply::Home(HomeReply::Status(HomeStatus::Open {
            sealed: None,
            votes: Vec::new(),
        })),
        RangeReply::Home(HomeReply::Status(HomeStatus::Decided(Outcome::Abort))),
        RangeReply::OracleNext(Timestamp(1 << 40)),
        RangeReply::Lease(10..20),
    ]
    .into_iter()
    .map(|r| Ok(PeerReply::Range(r)))
    .collect();
    out.extend([
        Ok(PeerReply::Gateway(GatewayReply::Done)),
        Ok(PeerReply::Gateway(GatewayReply::Blockers(vec![ClusterTxnId(3)]))),
        Err(RangeError::NotLeader { hint: Some(2) }),
        Err(RangeError::NotLeader { hint: None }),
        Err(RangeError::StaleEpoch { descriptor: desc() }),
        Err(RangeError::Aborted("lease lost".into())),
        Err(RangeError::Deadlock),
        Err(RangeError::LockTimeout),
        Err(RangeError::Cancelled),
        Err(RangeError::Conflict("40001".into())),
        Err(RangeError::Unavailable("overloaded".into())),
        Err(RangeError::Unknown("leader lost".into())),
        Err(RangeError::Corrupt("é".into())),
    ]);
    out
}

/// Every strict prefix and one appended byte must be rejected; a bad version too.
fn assert_strict<T: std::fmt::Debug>(bytes: &[u8], decode: impl Fn(&[u8]) -> Result<T, RangeError>) {
    for n in 0..bytes.len() {
        assert!(decode(&bytes[..n]).is_err(), "prefix {n} of {bytes:?}");
    }
    let mut longer = bytes.to_vec();
    longer.push(0);
    assert!(decode(&longer).is_err(), "trailing byte after {bytes:?}");
    let mut version = bytes.to_vec();
    version[0] = 2;
    assert!(decode(&version).is_err());
}

#[test]
fn every_call_round_trips_and_decoding_is_strict() {
    for call in calls() {
        let bytes = encode_call(&call).unwrap();
        assert_eq!(decode_call(&bytes).unwrap(), call);
        assert_strict(&bytes, decode_call);
    }
}

#[test]
fn every_reply_and_error_round_trips_and_decoding_is_strict() {
    for reply in replies() {
        let bytes = encode_reply(&reply).unwrap();
        assert_eq!(decode_reply(&bytes).unwrap(), reply);
        assert_strict(&bytes, decode_reply);
    }
}

#[test]
fn unknown_tags_and_huge_counts_are_rejected() {
    assert!(decode_call(&[1, 0]).is_err());
    assert!(decode_call(&[1, 14]).is_err());
    assert!(decode_call(&[1, 36]).is_err());
    assert!(decode_reply(&[1, 0, 0]).is_err());
    assert!(decode_reply(&[1, 1, 10]).is_err());
    assert!(decode_reply(&[1, 2]).is_err());
    // A blockers reply claiming 2^32-1 entries fails without allocating them.
    let mut huge = vec![1, 0, 4];
    huge.extend_from_slice(&u32::MAX.to_be_bytes());
    assert!(decode_reply(&huge).is_err());
    // An unknown keyspace tag in a read.
    let mut read = encode_call(&calls()[1]).unwrap();
    read[1 + 1 + 16] = 0x11;
    assert!(decode_call(&read).is_err());
}

#[test]
fn zero_timestamp_requests_and_inverted_leases_are_rejected() {
    let call = PeerCall::Range(RangeCall::OracleNext {
        range: RangeId(1),
        n: 0,
    });
    assert!(decode_call(&encode_call(&call).unwrap()).is_err());
    let reply: Result<PeerReply, RangeError> = Ok(PeerReply::Range(RangeReply::Lease(std::ops::Range {
        start: 9,
        end: 3,
    })));
    assert!(decode_reply(&encode_reply(&reply).unwrap()).is_err());
}
