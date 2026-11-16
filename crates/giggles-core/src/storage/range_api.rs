//! Shared range identities, calls, and descriptor codec.
use std::ops::{Bound, Range};
#[cfg(feature = "engine-rocksdb")]
use std::sync::Arc;

/// Range identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RangeId(pub u64);
/// System range identity.
pub const SYSTEM_RANGE: RangeId = RangeId(1);
/// Bumps on every descriptor change (split, move).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Epoch(pub u64);
/// Same layout as engine::hlc::Ts (ms << 16 | logical).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(pub u64);
/// Node id (high 16 bits) and per-node counter (low 48).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClusterTxnId(pub u64);
impl ClusterTxnId {
    /// Packs the node id and per-node counter.
    pub fn new(node: u16, counter: u64) -> Self {
        assert!(
            counter < (1_u64 << 48),
            "transaction counter exceeds 48 bits"
        );
        Self((u64::from(node) << 48) | counter)
    }
    /// Packs the node id and counter if both fit their assigned bits.
    pub fn try_new(node: u64, counter: u64) -> Option<Self> {
        if node >= (1_u64 << 16) || counter >= (1_u64 << 48) {
            return None;
        }
        Some(Self((node << 48) | counter))
    }
    /// Returns the node id (high 16 bits).
    pub fn node(self) -> u16 {
        (self.0 >> 48) as u16
    }
    /// Returns the per-node counter (low 48 bits).
    pub fn counter(self) -> u64 {
        self.0 & ((1_u64 << 48) - 1)
    }
}

/// A range or a lock in a gateway's own LockManager (table locks and DDL proxies).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum WaitSite {
    Range(RangeId),
    Gateway(u64),
}
/// Edge-chasing probe delivered to target.node().
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeadlockProbe {
    pub initiator: ClusterTxnId,
    /// Transaction and start timestamp priority, initiator first.
    pub path: Vec<(ClusterTxnId, Timestamp)>,
    pub target: ClusterTxnId,
}

/// Keyspaces minus Meta; tag is range order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum RangeSpace {
    Catalog = 0x10,
    Kernel = 0x20,
    Seq = 0x30,
    Gc = 0x40,
    Rows = 0x80,
    Index = 0x90,
}
impl RangeSpace {
    /// Returns the range-order tag.
    pub fn tag(self) -> u8 {
        self as u8
    }
    /// Decodes a range-order tag.
    pub fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0x10 => Some(Self::Catalog),
            0x20 => Some(Self::Kernel),
            0x30 => Some(Self::Seq),
            0x40 => Some(Self::Gc),
            0x80 => Some(Self::Rows),
            0x90 => Some(Self::Index),
            _ => None,
        }
    }
}
/// Tag followed by user key, with no version.
pub fn range_key(space: RangeSpace, user_key: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + user_key.len());
    key.push(space.tag());
    key.extend_from_slice(user_key);
    key
}
/// Splits a range key into its keyspace and user key.
pub fn split_range_key(key: &[u8]) -> Option<(RangeSpace, &[u8])> {
    let (&tag, key) = key.split_first()?;
    Some((RangeSpace::from_tag(tag)?, key))
}
/// System range is [MIN, this).
pub fn first_user_key() -> Vec<u8> {
    range_key(RangeSpace::Rows, &[])
}

/// Role of a replica in its range group.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum ReplicaRole {
    Voter = 0,
    Learner = 1,
}
/// Replica node identity and role.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReplicaDesc {
    pub node: u64,
    pub role: ReplicaRole,
}

/// Range bounds and replica descriptions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeDescriptor {
    pub id: RangeId,
    pub epoch: Epoch,
    pub start: Vec<u8>,
    pub end: Option<Vec<u8>>,
    pub replicas: Vec<ReplicaDesc>,
}
impl RangeDescriptor {
    /// Start is inclusive; end is exclusive; empty start and None end are unbounded.
    pub fn contains(&self, range_key: &[u8]) -> bool {
        range_key >= self.start.as_slice()
            && self
                .end
                .as_ref()
                .is_none_or(|end| range_key < end.as_slice())
    }
    /// Validates range bounds and sorted, unique replica node identities.
    pub fn validate(&self) -> Result<(), RangeError> {
        if self.start.len() > 65_536 || self.end.as_ref().is_some_and(|end| end.len() > 65_536) {
            return Err(RangeError::Corrupt(
                "descriptor bound exceeds 64 KiB".into(),
            ));
        }
        if self.end.as_ref().is_some_and(|end| end <= &self.start) {
            return Err(RangeError::Corrupt(
                "descriptor end must exceed start".into(),
            ));
        }
        if self.replicas.len() > 64 {
            return Err(RangeError::Corrupt("descriptor exceeds 64 replicas".into()));
        }
        if self
            .replicas
            .windows(2)
            .any(|pair| pair[0].node >= pair[1].node)
        {
            return Err(RangeError::Corrupt(
                "replica nodes must be strictly increasing".into(),
            ));
        }
        Ok(())
    }
    /// Encodes version 1, big-endian identity, bounds, and replica nodes and roles.
    pub fn encode(&self) -> Result<Vec<u8>, RangeError> {
        self.validate()?;
        fn put_bytes(out: &mut Vec<u8>, value: &[u8]) {
            out.extend_from_slice(&(value.len() as u32).to_be_bytes());
            out.extend_from_slice(value);
        }
        let mut out = vec![1];
        out.extend_from_slice(&self.id.0.to_be_bytes());
        out.extend_from_slice(&self.epoch.0.to_be_bytes());
        put_bytes(&mut out, &self.start);
        out.push(u8::from(self.end.is_some()));
        if let Some(end) = &self.end {
            put_bytes(&mut out, end);
        }
        out.extend_from_slice(&(self.replicas.len() as u16).to_be_bytes());
        for replica in &self.replicas {
            out.extend_from_slice(&replica.node.to_be_bytes());
            out.push(replica.role as u8);
        }
        Ok(out)
    }
    /// Rejects truncated, trailing, and unknown-version encodings.
    pub fn decode(mut bytes: &[u8]) -> Result<Self, RangeError> {
        fn take<'a>(bytes: &mut &'a [u8], len: usize) -> Result<&'a [u8], RangeError> {
            if bytes.len() < len {
                return Err(RangeError::Corrupt("truncated descriptor".into()));
            }
            let (value, rest) = bytes.split_at(len);
            *bytes = rest;
            Ok(value)
        }
        fn number<const N: usize>(bytes: &mut &[u8]) -> Result<[u8; N], RangeError> {
            Ok(take(bytes, N)?.try_into().expect("fixed length"))
        }
        fn bound(bytes: &mut &[u8]) -> Result<Vec<u8>, RangeError> {
            let len = u32::from_be_bytes(number(bytes)?) as usize;
            if len > 65_536 {
                return Err(RangeError::Corrupt(
                    "descriptor bound exceeds 64 KiB".into(),
                ));
            }
            Ok(take(bytes, len)?.to_vec())
        }
        if take(&mut bytes, 1)? != [1] {
            return Err(RangeError::Corrupt("unknown descriptor version".into()));
        }
        let id = RangeId(u64::from_be_bytes(number(&mut bytes)?));
        let epoch = Epoch(u64::from_be_bytes(number(&mut bytes)?));
        let start = bound(&mut bytes)?;
        let end = match take(&mut bytes, 1)?[0] {
            0 => None,
            1 => Some(bound(&mut bytes)?),
            _ => return Err(RangeError::Corrupt("invalid descriptor end flag".into())),
        };
        let count = usize::from(u16::from_be_bytes(number(&mut bytes)?));
        if count > 64 || count * 9 > bytes.len() {
            return Err(RangeError::Corrupt(
                "invalid descriptor replica count".into(),
            ));
        }
        let mut replicas = Vec::with_capacity(count);
        for _ in 0..count {
            let node = u64::from_be_bytes(number(&mut bytes)?);
            let role = match take(&mut bytes, 1)?[0] {
                0 => ReplicaRole::Voter,
                1 => ReplicaRole::Learner,
                _ => return Err(RangeError::Corrupt("invalid replica role".into())),
            };
            replicas.push(ReplicaDesc { node, role });
        }
        if !bytes.is_empty() {
            return Err(RangeError::Corrupt("trailing descriptor bytes".into()));
        }
        let descriptor = Self {
            id,
            epoch,
            start,
            end,
            replicas,
        };
        descriptor.validate()?;
        Ok(descriptor)
    }
}

/// Range routing, transaction, availability, and corruption errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RangeError {
    NotLeader { hint: Option<u64> },
    StaleEpoch { descriptor: RangeDescriptor },
    Aborted(String),
    Deadlock,
    LockTimeout,
    Cancelled,
    Conflict(String),
    Unavailable(String),
    Unknown(String),
    Corrupt(String),
}
impl std::fmt::Display for RangeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotLeader { hint } => write!(f, "not leader (hint: {hint:?})"),
            Self::StaleEpoch { descriptor } => write!(
                f,
                "stale epoch (range {:?}, epoch {:?})",
                descriptor.id, descriptor.epoch
            ),
            Self::Aborted(s) => write!(f, "aborted: {s}"),
            Self::Deadlock => f.write_str("deadlock"),
            Self::LockTimeout => f.write_str("lock timeout"),
            Self::Cancelled => f.write_str("cancelled"),
            Self::Conflict(s) => write!(f, "conflict: {s}"),
            Self::Unavailable(s) => write!(f, "unavailable: {s}"),
            Self::Unknown(s) => write!(f, "outcome unknown: {s}"),
            Self::Corrupt(s) => write!(f, "corrupt: {s}"),
        }
    }
}
impl std::error::Error for RangeError {}

/// Reads in forward or reverse key order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadDirection {
    Forward,
    Reverse,
}
/// Raw key bounds of that keyspace, as KvRead::range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadRequest {
    pub range: RangeId,
    pub epoch: Epoch,
    pub space: RangeSpace,
    pub start: Bound<Vec<u8>>,
    pub end: Bound<Vec<u8>>,
    pub dir: ReadDirection,
    pub ts: Timestamp,
    pub page_bytes: usize,
}
/// Raw pairs in direction order, newest versions at or below ts, tombstones included.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadPage {
    pub pairs: Vec<(Vec<u8>, Vec<u8>)>,
    pub resume: Option<Bound<Vec<u8>>>,
    pub range_end: Option<Vec<u8>>,
}
/// Key lock or per-table writer sentinel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LockTarget {
    Key { space: RangeSpace, key: Vec<u8> },
    TableSentinel { table: u64 },
}
/// Shared or exclusive range lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeLockMode {
    Shared,
    Exclusive,
}
/// Transaction lock request with start timestamp priority and optional timeout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockRequest {
    pub range: RangeId,
    pub epoch: Epoch,
    pub txn: ClusterTxnId,
    pub priority: Timestamp,
    pub target: LockTarget,
    pub mode: RangeLockMode,
    pub timeout_ms: Option<u64>,
}
/// Newest committed key version; table sentinel always returns None.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockGrant {
    pub latest: Option<(Timestamp, Option<Vec<u8>>)>,
}
/// User key write; None means delete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeWrite {
    pub space: RangeSpace,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}
/// Commit timestamp or abort decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Commit(Timestamp),
    Abort,
}
/// Seq allocator kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LeaseKind {
    ObjectId = 0,
    HeapRowId = 1,
    RowVersion = 3,
    Xid = 4,
}

/// Blocking client over the route cache and peer transport.
pub trait RangeClient: Send + Sync {
    fn descriptor_for(&self, range_key: &[u8]) -> Result<RangeDescriptor, RangeError>;
    fn invalidate(&self, stale: &RangeDescriptor);
    fn read(&self, req: ReadRequest) -> Result<ReadPage, RangeError>;
    fn lock(&self, req: LockRequest) -> Result<LockGrant, RangeError>;
    /// Holders and conflicting waiters queued ahead; empty if txn is not waiting there.
    fn blockers(&self, range: RangeId, txn: ClusterTxnId) -> Result<Vec<ClusterTxnId>, RangeError>;
    /// Makes txn's pending request return RangeError::Deadlock; no-op if not waiting.
    fn cancel_wait(&self, range: RangeId, txn: ClusterTxnId) -> Result<(), RangeError>;
    fn release(&self, range: RangeId, txn: ClusterTxnId) -> Result<(), RangeError>;
    fn heartbeat(&self, range: RangeId, txn: ClusterTxnId) -> Result<(), RangeError>;
    fn commit(
        &self,
        range: RangeId,
        epoch: Epoch,
        txn: ClusterTxnId,
        writes: Vec<RangeWrite>,
        marker_xid: Option<u64>,
    ) -> Result<Timestamp, RangeError>;
    fn prepare(
        &self,
        range: RangeId,
        epoch: Epoch,
        txn: ClusterTxnId,
        home: RangeId,
        writes: Vec<RangeWrite>,
    ) -> Result<Timestamp, RangeError>;
    fn finalize(
        &self,
        range: RangeId,
        txn: ClusterTxnId,
        outcome: Outcome,
    ) -> Result<(), RangeError>;
    fn home(&self, range: RangeId, epoch: Epoch, op: HomeOp) -> Result<HomeReply, RangeError>;
    fn oracle(&self) -> &dyn TsOracle;
    fn lease(&self, kind: LeaseKind, id: u64, count: u64) -> Result<Range<u64>, RangeError>;
}
/// One service per range, on its current leader.
pub trait RangeService: Send + Sync {
    fn descriptor(&self) -> RangeDescriptor;
    fn read(&self, req: ReadRequest) -> Result<ReadPage, RangeError>;
    fn lock(&self, req: LockRequest) -> Result<LockGrant, RangeError>;
    /// Holders and conflicting waiters queued ahead; empty if txn is not waiting there.
    fn blockers(&self, txn: ClusterTxnId) -> Result<Vec<ClusterTxnId>, RangeError>;
    /// Makes txn's pending request return RangeError::Deadlock; no-op if not waiting.
    fn cancel_wait(&self, txn: ClusterTxnId) -> Result<(), RangeError>;
    fn release(&self, txn: ClusterTxnId) -> Result<(), RangeError>;
    fn heartbeat(&self, txn: ClusterTxnId) -> Result<(), RangeError>;
    fn commit(
        &self,
        epoch: Epoch,
        txn: ClusterTxnId,
        writes: Vec<RangeWrite>,
        marker_xid: Option<u64>,
    ) -> Result<Timestamp, RangeError>;
    fn prepare(
        &self,
        epoch: Epoch,
        txn: ClusterTxnId,
        home: RangeId,
        writes: Vec<RangeWrite>,
    ) -> Result<Timestamp, RangeError>;
    fn finalize(&self, txn: ClusterTxnId, outcome: Outcome) -> Result<(), RangeError>;
    fn home(&self, epoch: Epoch, op: HomeOp) -> Result<HomeReply, RangeError>;
}
/// Gateway calls over the peer transport; blockers and cancel_wait address WaitSite::Gateway.
pub trait GatewayPeer: Send + Sync {
    /// Sends a fire-and-forget probe.
    fn probe(&self, node: u64, probe: DeadlockProbe) -> Result<(), RangeError>;
    /// Cancels the victim's waits and fails it with 40P01.
    fn abort(&self, node: u64, victim: ClusterTxnId) -> Result<(), RangeError>;
    /// Returns blockers in the node's LockManager.
    fn blockers(&self, node: u64, txn: ClusterTxnId) -> Result<Vec<ClusterTxnId>, RangeError>;
    /// Makes txn's pending request return RangeError::Deadlock; no-op if not waiting.
    fn cancel_wait(&self, node: u64, txn: ClusterTxnId) -> Result<(), RangeError>;
}
/// Deadlock service on every gateway, called by the peer dispatcher.
pub trait GatewayService: Send + Sync {
    /// Receives an edge-chasing probe.
    fn probe(&self, probe: DeadlockProbe);
    /// Cancels the victim's waits and fails it with 40P01.
    fn abort(&self, victim: ClusterTxnId);
    /// Returns blockers in this node's LockManager.
    fn blockers(&self, txn: ClusterTxnId) -> Vec<ClusterTxnId>;
    /// Makes txn's pending request return RangeError::Deadlock; no-op if not waiting.
    fn cancel_wait(&self, txn: ClusterTxnId);
}
/// Only on the system range's leader.
pub trait SystemService: Send + Sync {
    fn oracle(&self) -> &dyn TsOracle;
    fn lease(&self, kind: LeaseKind, id: u64, count: u64) -> Result<Range<u64>, RangeError>;
}
/// Linearizable oracle, strictly increasing across system-range leader changes.
pub trait TsOracle: Send + Sync {
    /// Reserves n >= 1 consecutive timestamps and returns the first, strictly after all prior reservations in real time.
    fn next(&self, n: u32) -> Result<Timestamp, RangeError>;
}
/// Called by the per-group role manager after the leader's read barrier.
#[cfg(feature = "engine-rocksdb")]
pub trait RangeEngineOpener: Send + Sync {
    fn open(
        &self,
        descriptor: RangeDescriptor,
        sink: Arc<dyn crate::replicated::BatchSink>,
    ) -> Result<Arc<dyn RangeService>, RangeError>;
    fn close(&self, range: RangeId);
}

/// Cross-range commit home operations; home is the first range written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HomeOp {
    Open {
        txn: ClusterTxnId,
    },
    Seal {
        txn: ClusterTxnId,
        participants: Vec<RangeId>,
    },
    Vote {
        txn: ClusterTxnId,
        participant: RangeId,
        prepared_at: Timestamp,
    },
    Decide {
        txn: ClusterTxnId,
        outcome: Outcome,
    },
    Status {
        txn: ClusterTxnId,
    },
    Forget {
        txn: ClusterTxnId,
    },
}
/// Home operation acknowledgement or status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HomeReply {
    Ok,
    Status(HomeStatus),
}
/// Unknown, open with seal and votes, or durably decided home record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HomeStatus {
    Unknown,
    Open {
        sealed: Option<Vec<RangeId>>,
        votes: Vec<(RangeId, Timestamp)>,
    },
    Decided(Outcome),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn descriptor(end: Option<Vec<u8>>, replicas: Vec<u64>) -> RangeDescriptor {
        RangeDescriptor {
            id: RangeId(0x0102030405060708),
            epoch: Epoch(17),
            start: vec![5, 10],
            end,
            replicas: replicas
                .into_iter()
                .map(|node| ReplicaDesc {
                    node,
                    role: ReplicaRole::Learner,
                })
                .collect(),
        }
    }

    #[test]
    fn descriptor_round_trip() {
        for end in [None, Some(vec![5, 20])] {
            for replicas in [vec![], vec![1, 17, u64::MAX]] {
                for start in [vec![], vec![5, 10]] {
                    let mut desc = descriptor(end.clone(), replicas.clone());
                    desc.start = start;
                    assert_eq!(RangeDescriptor::decode(&desc.encode().unwrap()), Ok(desc));
                }
            }
        }
    }

    #[test]
    fn descriptor_rejects_invalid_encodings() {
        for end in [None, Some(vec![5, 20])] {
            for replicas in [vec![], vec![1, 17, u64::MAX]] {
                let encoded = descriptor(end.clone(), replicas).encode().unwrap();
                for cut in 0..encoded.len() {
                    assert!(
                        matches!(
                            RangeDescriptor::decode(&encoded[..cut]),
                            Err(RangeError::Corrupt(_))
                        ),
                        "cut {cut}"
                    );
                }
                for version in [0, 2, 255] {
                    let mut unknown = encoded.clone();
                    unknown[0] = version;
                    assert!(matches!(
                        RangeDescriptor::decode(&unknown),
                        Err(RangeError::Corrupt(_))
                    ));
                }
                let mut trailing = encoded.clone();
                trailing.push(0);
                assert!(matches!(
                    RangeDescriptor::decode(&trailing),
                    Err(RangeError::Corrupt(_))
                ));
                let mut invalid_flag = encoded;
                let start_len =
                    u32::from_be_bytes(invalid_flag[17..21].try_into().unwrap()) as usize;
                invalid_flag[21 + start_len] = 2;
                assert!(matches!(
                    RangeDescriptor::decode(&invalid_flag),
                    Err(RangeError::Corrupt(_))
                ));
            }
        }
    }

    #[test]
    fn descriptor_wire_layout() {
        let desc = RangeDescriptor {
            id: RangeId(1),
            epoch: Epoch(2),
            start: vec![5],
            end: Some(vec![6]),
            replicas: vec![ReplicaDesc {
                node: 3,
                role: ReplicaRole::Voter,
            }],
        };
        let mut expected = vec![1];
        expected.extend(1_u64.to_be_bytes());
        expected.extend(2_u64.to_be_bytes());
        expected.extend([0, 0, 0, 1, 5, 1, 0, 0, 0, 1, 6, 0, 1]);
        expected.extend(3_u64.to_be_bytes());
        expected.push(0);
        assert_eq!(desc.encode().unwrap(), expected);
    }

    #[test]
    fn descriptor_contains_boundaries() {
        let mut desc = descriptor(Some(vec![5, 20]), vec![]);
        assert!(!desc.contains(&[5, 9]));
        assert!(desc.contains(&[5, 10]));
        assert!(desc.contains(&[5, 19]));
        assert!(!desc.contains(&[5, 20]));
        assert!(!desc.contains(&[5, 21]));
        desc.start.clear();
        assert!(desc.contains(&[]));
        desc.end = None;
        assert!(desc.contains(&[255, 255]));
        desc.end = Some(vec![]);
        assert!(!desc.contains(&[]));
    }

    #[test]
    fn range_keys_preserve_order_and_split() {
        let spaces = [
            RangeSpace::Catalog,
            RangeSpace::Kernel,
            RangeSpace::Seq,
            RangeSpace::Gc,
            RangeSpace::Rows,
            RangeSpace::Index,
        ];
        let keys: &[&[u8]] = &[&[], &[0], &[0, 0], &[0, 255], &[1], &[255], &[255, 255]];
        for space in spaces {
            for a in keys {
                let encoded = range_key(space, a);
                assert_eq!(split_range_key(&encoded), Some((space, *a)));
                assert_eq!(
                    encoded < first_user_key(),
                    space.tag() < RangeSpace::Rows.tag()
                );
                for b in keys {
                    assert_eq!(a.cmp(b), range_key(space, a).cmp(&range_key(space, b)));
                }
            }
            assert_eq!(RangeSpace::from_tag(space.tag()), Some(space));
        }
        assert_eq!(split_range_key(&[]), None);
        for tag in [0, 1, 7, 255] {
            assert_eq!(RangeSpace::from_tag(tag), None);
            assert_eq!(split_range_key(&[tag]), None);
        }
    }

    #[test]
    fn cluster_txn_id_packing() {
        for node in [0, 1, u16::MAX] {
            for counter in [0, 1, (1_u64 << 48) - 1] {
                let id = ClusterTxnId::new(node, counter);
                assert_eq!(ClusterTxnId::try_new(u64::from(node), counter), Some(id));
                assert_eq!(id.node(), node);
                assert_eq!(id.counter(), counter);
                assert_eq!(id.0, (u64::from(node) << 48) | counter);
            }
        }
    }

    #[test]
    fn cluster_txn_id_checked_bounds() {
        assert_eq!(ClusterTxnId::try_new(1 << 16, 0), None);
        assert_eq!(ClusterTxnId::try_new(0, 1 << 48), None);
        assert_eq!(ClusterTxnId::try_new(u64::MAX, u64::MAX), None);
    }

    #[test]
    fn descriptor_rejects_invalid_values() {
        let base = descriptor(Some(vec![5, 20]), vec![1, 2, 3]);
        let mut invalid = Vec::new();
        for end in [vec![], vec![5, 9], base.start.clone()] {
            let mut desc = base.clone();
            desc.end = Some(end);
            invalid.push(desc);
        }
        for nodes in [vec![2, 1], vec![1, 1], (0..65).collect()] {
            invalid.push(descriptor(None, nodes));
        }
        let mut long_start = base.clone();
        long_start.start = vec![0; 65_537];
        invalid.push(long_start);
        let mut long_end = base.clone();
        long_end.end = Some(vec![255; 65_537]);
        invalid.push(long_end);
        for desc in invalid {
            assert!(matches!(desc.validate(), Err(RangeError::Corrupt(_))));
            assert!(matches!(desc.encode(), Err(RangeError::Corrupt(_))));
            // Build malformed wire data independently of the validating encoder.
            let mut wire = vec![1];
            wire.extend(desc.id.0.to_be_bytes());
            wire.extend(desc.epoch.0.to_be_bytes());
            wire.extend((desc.start.len() as u32).to_be_bytes());
            wire.extend(&desc.start);
            wire.push(u8::from(desc.end.is_some()));
            if let Some(end) = desc.end {
                wire.extend((end.len() as u32).to_be_bytes());
                wire.extend(end);
            }
            wire.extend((desc.replicas.len() as u16).to_be_bytes());
            for replica in desc.replicas {
                wire.extend(replica.node.to_be_bytes());
                wire.push(replica.role as u8);
            }
            assert!(matches!(
                RangeDescriptor::decode(&wire),
                Err(RangeError::Corrupt(_))
            ));
        }
        let mut wire = base.encode().unwrap();
        *wire.last_mut().unwrap() = 2;
        assert!(matches!(
            RangeDescriptor::decode(&wire),
            Err(RangeError::Corrupt(_))
        ));
        let mut wire = base.encode().unwrap();
        wire[17..21].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(matches!(
            RangeDescriptor::decode(&wire),
            Err(RangeError::Corrupt(_))
        ));
    }

    #[test]
    fn system_descriptor_contains_only_system_spaces() {
        let mut desc = descriptor(Some(first_user_key()), vec![]);
        desc.start.clear();
        assert_eq!(desc.validate(), Ok(()));
        for (space, tag, contains) in [
            (RangeSpace::Catalog, 0x10, true),
            (RangeSpace::Kernel, 0x20, true),
            (RangeSpace::Seq, 0x30, true),
            (RangeSpace::Gc, 0x40, true),
            (RangeSpace::Rows, 0x80, false),
            (RangeSpace::Index, 0x90, false),
        ] {
            assert_eq!(space.tag(), tag);
            for suffix in [vec![], vec![0, 1, 255], vec![255; 100]] {
                assert_eq!(desc.contains(&range_key(space, &suffix)), contains);
            }
        }
    }

    #[test]
    #[should_panic(expected = "transaction counter exceeds 48 bits")]
    fn cluster_txn_id_rejects_overflow() {
        ClusterTxnId::new(0, 1_u64 << 48);
    }

    #[test]
    fn traits_are_object_safe() {
        fn check<T: ?Sized + Send + Sync>() {}
        check::<dyn RangeClient>();
        check::<dyn RangeService>();
        check::<dyn GatewayPeer>();
        check::<dyn GatewayService>();
        check::<dyn SystemService>();
        check::<dyn TsOracle>();
        #[cfg(feature = "engine-rocksdb")]
        check::<dyn RangeEngineOpener>();
    }
}
