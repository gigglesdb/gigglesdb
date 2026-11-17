//! Range call wire codec (GD-0075 R5-21): every call, reply and error round-trips; the
//! decoder is strict (version, tags, truncation, trailing bytes).
use std::ops::Bound;

use giggles_core::range_api::{
    ClusterTxnId, DeadlockProbe, Epoch, HomeOp, HomeReply, HomeStatus, LeaseKind, LockGrant,
    LockRequest, LockTarget, Outcome, RangeDescriptor, RangeError, RangeId, RangeLockMode,
    RangeSpace, RangeWrite, ReadDirection, ReadPage, ReadRequest, ReplicaDesc, ReplicaRole,
    SequenceOp, SequenceReply, Timestamp,
};
use giggles_replication::{
    call_codec::{
        CallCounts, GatewayCall, GatewayReply, NodeCall, NodeReport, PeerCall, PeerReply,
        RangeReport, RangeRole, decode_call, decode_reply, encode_call, encode_reply,
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
        // GD-0075 R5-72: gap locks.
        RangeCall::Lock(LockRequest {
            range,
            epoch,
            txn,
            priority: Timestamp(5),
            target: LockTarget::Interval {
                space: RangeSpace::Rows,
                start: Bound::Excluded(vec![1; 9]),
                end: Bound::Unbounded,
            },
            mode: RangeLockMode::Shared,
            timeout_ms: Some(0),
        }),
        RangeCall::Lock(LockRequest {
            range,
            epoch,
            txn,
            priority: Timestamp(5),
            target: LockTarget::Interval {
                space: RangeSpace::Index,
                start: Bound::Unbounded,
                end: Bound::Included(vec![2; 10]),
            },
            mode: RangeLockMode::Shared,
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
            lock_generation: Some((9 << 20) | 2),
        },
        RangeCall::Commit {
            range,
            epoch,
            txn,
            writes: Vec::new(),
            marker_xid: None,
            lock_generation: None,
        },
        RangeCall::Prepare {
            range,
            epoch,
            txn,
            home: RangeId(2),
            writes,
            lock_generation: Some(u64::MAX),
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
        RangeCall::Sequence {
            range,
            id: 9,
            incarnation: u128::MAX - 3,
            op: SequenceOp::Next,
        },
        RangeCall::Sequence {
            range,
            id: 9,
            incarnation: 1,
            op: SequenceOp::Set {
                value: i64::MIN,
                called: true,
            },
        },
        RangeCall::Sequence {
            range,
            id: u64::MAX,
            incarnation: 0,
            op: SequenceOp::Forget,
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
        PeerCall::Gateway(GatewayCall::TxnLive(txn)),
        PeerCall::Node(NodeCall::Status),
    ]);
    out
}

fn node_report() -> NodeReport {
    let range = |role, size, recent| RangeReport {
        descriptor: desc(),
        role,
        term: 5,
        leader: 1,
        applied: 77,
        size,
        calls: CallCounts {
            reads: 1,
            locks: 2,
            writes: 3,
            other: u64::MAX,
        },
        recent,
        window_ms: 60_000,
    };
    NodeReport {
        node: 4,
        ranges: vec![
            range(RangeRole::Leader, Some(1 << 30), 120),
            range(RangeRole::Follower, None, 0),
            range(RangeRole::Candidate, Some(0), 1),
        ],
    }
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
            generation: (4 << 20) | 1,
        }),
        RangeReply::Lock(LockGrant {
            latest: Some((Timestamp(8), None)),
            generation: u64::MAX,
        }),
        RangeReply::Lock(LockGrant {
            latest: None,
            generation: 0,
        }),
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
        RangeReply::Sequence(SequenceReply::Value(-7)),
        RangeReply::Sequence(SequenceReply::Value(i64::MAX)),
        RangeReply::Sequence(SequenceReply::Gone),
        RangeReply::Sequence(SequenceReply::Limit),
        RangeReply::Sequence(SequenceReply::OutOfRange),
    ]
    .into_iter()
    .map(|r| Ok(PeerReply::Range(r)))
    .collect();
    out.extend([
        Ok(PeerReply::Gateway(GatewayReply::Done)),
        Ok(PeerReply::Gateway(GatewayReply::Live(true))),
        Ok(PeerReply::Gateway(GatewayReply::Live(false))),
        Ok(PeerReply::Node(node_report())),
        Ok(PeerReply::Node(NodeReport {
            node: 1,
            ranges: Vec::new(),
        })),
        Ok(PeerReply::Gateway(GatewayReply::Blockers(vec![
            ClusterTxnId(3),
        ]))),
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
fn assert_strict<T: std::fmt::Debug>(
    bytes: &[u8],
    decode: impl Fn(&[u8]) -> Result<T, RangeError>,
) {
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
    assert!(decode_call(&[1, 15]).is_err());
    assert!(decode_call(&[1, 37]).is_err());
    assert!(decode_call(&[1, 49]).is_err());
    assert!(decode_reply(&[1, 0, 49]).is_err());
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
    // An unknown sequence op and an unknown sequence reply.
    let seq = PeerCall::Range(RangeCall::Sequence {
        range: RangeId(1),
        id: 2,
        incarnation: 3,
        op: SequenceOp::Forget,
    });
    let mut bytes = encode_call(&seq).unwrap();
    *bytes.last_mut().unwrap() = 3;
    assert!(decode_call(&bytes).is_err());
    let mut bytes = encode_reply(&Ok(PeerReply::Range(RangeReply::Sequence(
        SequenceReply::Gone,
    ))))
    .unwrap();
    *bytes.last_mut().unwrap() = 4;
    assert!(decode_reply(&bytes).is_err());
}

#[test]
fn node_reports_reject_unknown_roles_and_report_rates() {
    let report = node_report();
    assert_eq!(report.ranges[0].calls_per_sec(), 2.0);
    // Totals saturate rather than overflow on a peer's extreme counters.
    assert_eq!(report.ranges[1].calls_total(), u64::MAX);
    let mut bytes = encode_reply(&Ok(PeerReply::Node(NodeReport {
        node: 4,
        ranges: report.ranges[..1].to_vec(),
    })))
    .unwrap();
    // The role byte follows the version, ok flag, tag, node, count and descriptor.
    let role = 1 + 1 + 1 + 8 + 4 + 4 + desc().encode().unwrap().len();
    assert_eq!(bytes[role], 0);
    bytes[role] = 3;
    assert!(decode_reply(&bytes).is_err());
    // A report claiming 2^32-1 ranges fails without allocating them.
    let mut huge = vec![1, 0, 48, 0, 0, 0, 0, 0, 0, 0, 4];
    huge.extend_from_slice(&u32::MAX.to_be_bytes());
    assert!(decode_reply(&huge).is_err());
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
