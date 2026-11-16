use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use giggles_core::range_api::*;
use giggles_replication::client::{
    LocalRanges, RangeCall, RangeReply, RangeTransport, RoutedRangeClient, dispatch,
};
use giggles_replication::directory::*;

fn key(n: u8) -> Vec<u8> {
    range_key(RangeSpace::Rows, &[n])
}
fn replicas(nodes: &[(u64, ReplicaRole)]) -> Vec<ReplicaDesc> {
    nodes
        .iter()
        .map(|&(node, role)| ReplicaDesc { node, role })
        .collect()
}
fn desc(
    id: u64,
    epoch: u64,
    start: Vec<u8>,
    end: Option<Vec<u8>>,
    nodes: &[u64],
) -> RangeDescriptor {
    let voters: Vec<_> = nodes.iter().map(|&n| (n, ReplicaRole::Voter)).collect();
    RangeDescriptor {
        id: RangeId(id),
        epoch: Epoch(epoch),
        start,
        end,
        replicas: replicas(&voters),
    }
}
fn raw(user_key: &[u8], ts: u64) -> Vec<u8> {
    [user_key, &(!ts).to_be_bytes()].concat()
}
fn live(desc: &RangeDescriptor) -> Vec<u8> {
    [&[0][..], &desc.encode().unwrap()].concat()
}
/// System range [MIN, first user key) on node 1, epoch 2 (after the first split).
fn system() -> RangeDescriptor {
    desc(1, 2, vec![], Some(first_user_key()), &[1])
}
fn txn() -> ClusterTxnId {
    ClusterTxnId::new(9, 1)
}

/// Fake range leader: serves `desc` (directory pairs for the system range), or answers every
/// call with `StaleEpoch(stale)` once the range moved away.
struct FakeRange {
    desc: RangeDescriptor,
    stale: Option<RangeDescriptor>,
    pairs: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
}
impl FakeRange {
    fn new(desc: RangeDescriptor, stale: Option<RangeDescriptor>) -> Arc<Self> {
        let pairs = Mutex::default();
        Arc::new(Self { desc, stale, pairs })
    }
    fn record(&self, desc: &RangeDescriptor, ts: u64) {
        let write = record_write(desc).unwrap();
        self.pairs
            .lock()
            .unwrap()
            .insert(raw(&write.key, ts), live(desc));
    }
    fn check(&self, epoch: Option<Epoch>) -> Result<(), RangeError> {
        match (&self.stale, epoch) {
            (Some(to), _) => Err(RangeError::StaleEpoch {
                descriptor: to.clone(),
            }),
            (None, Some(epoch)) if epoch != self.desc.epoch => Err(RangeError::StaleEpoch {
                descriptor: self.desc.clone(),
            }),
            _ => Ok(()),
        }
    }
}
impl RangeService for FakeRange {
    fn descriptor(&self) -> RangeDescriptor {
        self.desc.clone()
    }
    fn read(&self, req: ReadRequest) -> Result<ReadPage, RangeError> {
        self.check(Some(req.epoch))?;
        let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for (k, v) in self.pairs.lock().unwrap().range((req.start, req.end)) {
            let (user, ts) = k.split_at(k.len() - 8);
            let newest_seen = pairs.last().is_some_and(|(p, _)| &p[..p.len() - 8] == user);
            if !newest_seen && !u64::from_be_bytes(ts.try_into().unwrap()) <= req.ts.0 {
                pairs.push((k.clone(), v.clone()));
            }
        }
        Ok(ReadPage {
            pairs,
            resume: None,
            range_end: self.desc.end.clone(),
        })
    }
    fn lock(&self, req: LockRequest) -> Result<LockGrant, RangeError> {
        self.check(Some(req.epoch))
            .map(|()| LockGrant { latest: None })
    }
    fn blockers(&self, _: ClusterTxnId) -> Result<Vec<ClusterTxnId>, RangeError> {
        self.check(None).map(|()| vec![])
    }
    fn cancel_wait(&self, _: ClusterTxnId) -> Result<(), RangeError> {
        self.check(None)
    }
    fn release(&self, _: ClusterTxnId) -> Result<(), RangeError> {
        self.check(None)
    }
    fn heartbeat(&self, _: ClusterTxnId) -> Result<(), RangeError> {
        self.check(None)
    }
    fn commit(
        &self,
        epoch: Epoch,
        _: ClusterTxnId,
        _: Vec<RangeWrite>,
        _: Option<u64>,
    ) -> Result<Timestamp, RangeError> {
        self.check(Some(epoch)).map(|()| Timestamp(7))
    }
    fn prepare(
        &self,
        epoch: Epoch,
        _: ClusterTxnId,
        _: RangeId,
        _: Vec<RangeWrite>,
    ) -> Result<Timestamp, RangeError> {
        self.check(Some(epoch)).map(|()| Timestamp(7))
    }
    fn finalize(&self, _: ClusterTxnId, _: Outcome) -> Result<(), RangeError> {
        self.check(None)
    }
    fn home(&self, epoch: Epoch, _: HomeOp) -> Result<HomeReply, RangeError> {
        self.check(Some(epoch)).map(|()| HomeReply::Ok)
    }
}

struct FakeSystem(AtomicU64);
impl TsOracle for FakeSystem {
    fn next(&self, n: u32) -> Result<Timestamp, RangeError> {
        Ok(Timestamp(self.0.fetch_add(u64::from(n), Ordering::SeqCst)))
    }
}
impl SystemService for FakeSystem {
    fn oracle(&self) -> &dyn TsOracle {
        self
    }
    fn lease(&self, _: LeaseKind, _: u64, count: u64) -> Result<std::ops::Range<u64>, RangeError> {
        Ok(0..count)
    }
}

#[derive(Default)]
struct FakeNode {
    ranges: Mutex<HashMap<RangeId, Arc<FakeRange>>>,
    system: Option<Arc<FakeSystem>>,
}
impl LocalRanges for FakeNode {
    fn service(&self, range: RangeId) -> Option<Arc<dyn RangeService>> {
        let service = self.ranges.lock().unwrap().get(&range).cloned()?;
        Some(service)
    }
    fn system(&self) -> Option<Arc<dyn SystemService>> {
        let system = self.system.clone()?;
        Some(system)
    }
}

/// In-memory transport: nodes 1..=4 (1 = system leader holding the directory) and client 9.
struct Net {
    nodes: HashMap<u64, Arc<FakeNode>>,
    hints: Mutex<HashMap<RangeId, u64>>,
    log: Mutex<Vec<(u64, String)>>,
}
impl Net {
    fn new(ranges: &[RangeDescriptor]) -> (Arc<Self>, Arc<FakeRange>) {
        let mut nodes: HashMap<u64, Arc<FakeNode>> = [2, 3, 4, 9]
            .into_iter()
            .map(|n| (n, Arc::default()))
            .collect();
        let sys = FakeNode {
            system: Some(Arc::new(FakeSystem(AtomicU64::new(100)))),
            ..Default::default()
        };
        let directory = FakeRange::new(system(), None);
        directory.record(&system(), 1);
        for range in ranges {
            directory.record(range, 1);
        }
        sys.ranges
            .lock()
            .unwrap()
            .insert(SYSTEM_RANGE, directory.clone());
        nodes.insert(1, Arc::new(sys));
        (
            Arc::new(Self {
                nodes,
                hints: Mutex::default(),
                log: Mutex::default(),
            }),
            directory,
        )
    }
    fn host(&self, node: u64, range: Arc<FakeRange>) {
        self.nodes[&node]
            .ranges
            .lock()
            .unwrap()
            .insert(range.desc.id, range);
    }
    fn client(self: &Arc<Self>, node: u64) -> RoutedRangeClient {
        RoutedRangeClient::new(node, system(), self.clone(), self.nodes[&node].clone())
    }
    fn calls(&self, kind: &str) -> Vec<u64> {
        let log = self.log.lock().unwrap();
        log.iter()
            .filter(|(_, k)| *k == kind)
            .map(|(n, _)| *n)
            .collect()
    }
}
impl RangeTransport for Net {
    fn call(&self, node: u64, call: RangeCall) -> Result<RangeReply, RangeError> {
        let kind = format!("{call:?}")
            .split([' ', '('])
            .next()
            .unwrap()
            .to_owned();
        self.log.lock().unwrap().push((node, kind));
        let range = call.range();
        // Fakes never answer NotLeader themselves: it means "not hosted here".
        dispatch(&*self.nodes[&node], call).map_err(|err| match err {
            RangeError::NotLeader { .. } => RangeError::NotLeader {
                hint: self.hints.lock().unwrap().get(&range).copied(),
            },
            err => err,
        })
    }
}

fn read_req(range: &RangeDescriptor, epoch: u64) -> ReadRequest {
    ReadRequest {
        range: range.id,
        epoch: Epoch(epoch),
        space: RangeSpace::Rows,
        start: std::ops::Bound::Unbounded,
        end: std::ops::Bound::Unbounded,
        dir: ReadDirection::Forward,
        ts: Timestamp(5),
        page_bytes: 1024,
    }
}

#[test]
fn directory_keys_order_prefix_free_round_trip() {
    let alphabet = [0x00, 0x01, 0x10, 0xFE, 0xFF];
    let mut keys = vec![vec![]];
    for len in 1..=3 {
        let mut next = Vec::new();
        for k in keys.iter().filter(|k: &&Vec<u8>| k.len() == len - 1) {
            next.extend(alphabet.iter().map(|&b| [k.as_slice(), &[b]].concat()));
        }
        keys.extend(next);
    }
    let max = record_key(None);
    for a in &keys {
        let ea = record_key(Some(a));
        assert_eq!(ea[0], DIRECTORY_TAG);
        assert!(
            ea < max && !max.starts_with(&ea) && !ea.starts_with(&max),
            "{a:?}"
        );
        for b in &keys {
            let eb = record_key(Some(b));
            assert_eq!(ea.cmp(&eb), a.cmp(b), "{a:?} {b:?}");
            assert!(a == b || !eb.starts_with(&ea), "prefix {a:?} {b:?}");
            // Versions never reorder distinct user keys.
            assert_eq!(
                raw(&ea, 0).cmp(&raw(&eb, u64::MAX)),
                a.cmp(b).then(std::cmp::Ordering::Greater)
            );
        }
    }
    let records = initial_records(replicas(&[
        (1, ReplicaRole::Voter),
        (2, ReplicaRole::Learner),
    ]))
    .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].key, max);
    assert_eq!(records[0].space, RangeSpace::Catalog);
    let initial = decode_record(
        &raw(&records[0].key, 3),
        &[&[0][..], records[0].value.as_ref().unwrap()].concat(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        (
            initial.id,
            initial.epoch,
            initial.start.len(),
            initial.end.clone()
        ),
        (SYSTEM_RANGE, Epoch(1), 0, None)
    );
    assert!(
        initial_records(replicas(&[
            (2, ReplicaRole::Voter),
            (1, ReplicaRole::Voter)
        ]))
        .is_err()
    );
    let d = desc(10, 3, key(1), Some(key(9)), &[2, 3]);
    let write = record_write(&d).unwrap();
    assert_eq!(write.key, record_key(Some(&key(9))));
    assert_eq!(
        decode_record(&raw(&write.key, 5), &live(&d)),
        Ok(Some(d.clone()))
    );
    assert_eq!(decode_record(&raw(&write.key, 5), &[1]), Ok(None));
    assert_eq!(
        delete_write(None),
        RangeWrite {
            space: RangeSpace::Catalog,
            key: max.clone(),
            value: None
        }
    );
    for (k, v) in [
        (raw(&max, 5), live(&d)),            // key does not match the descriptor end
        (raw(&[0x01, 9], 5), live(&d)),      // wrong tag
        (write.key.clone(), live(&d)),       // no version suffix
        (raw(&write.key, 5), vec![2, 1]),    // bad envelope
        (raw(&write.key, 5), vec![0, 1, 2]), // garbage descriptor
    ] {
        assert!(
            matches!(decode_record(&k, &v), Err(RangeError::Corrupt(_))),
            "{k:?} {v:?}"
        );
    }
}

#[test]
fn parse_lookup_skips_tombstones_over_raw_versions() {
    let d = desc(10, 2, key(20), Some(key(60)), &[2]);
    let page = ReadPage {
        pairs: vec![
            (raw(&record_key(Some(&key(20))), 9), vec![1]),
            (raw(&record_key(Some(&key(60))), 7), live(&d)),
        ],
        resume: None,
        range_end: None,
    };
    assert_eq!(parse_lookup(&page, &key(30)), Ok(Some(d.clone())));
    assert_eq!(parse_lookup(&page, &key(10)), Ok(None));
    assert!(!all_tombstones(&page));
    let garbage = ReadPage {
        pairs: vec![(raw(&record_key(None), 1), vec![0, 9])],
        ..page
    };
    assert!(matches!(
        parse_lookup(&garbage, &key(30)),
        Err(RangeError::Corrupt(_))
    ));
    // The seek bound excludes every version of the record ending at the key itself.
    let req = lookup_request(&key(60), &system(), Timestamp(11));
    assert_eq!(
        (req.range, req.epoch, req.space, req.ts),
        (SYSTEM_RANGE, Epoch(2), RangeSpace::Catalog, Timestamp(11))
    );
    let std::ops::Bound::Excluded(seek) = req.start else {
        panic!()
    };
    for ts in [0, 1, u64::MAX] {
        assert!(raw(&record_key(Some(&key(60))), ts) <= seek);
        assert!(raw(&record_key(Some(&[key(60), vec![0]].concat())), ts) > seek);
        assert!(raw(&record_key(None), ts) < vec![DIRECTORY_TAG + 1]);
    }
}

#[test]
fn stale_positive_negative_cache_and_redirect_limit() {
    let r10 = desc(10, 1, first_user_key(), None, &[2]);
    let (net, directory) = Net::new(std::slice::from_ref(&r10));
    net.host(2, FakeRange::new(r10.clone(), None));
    let client = net.client(9);
    // Negative: a miss refreshes once from the directory, then hits the cache.
    assert_eq!(client.descriptor_for(&key(99)), Ok(r10.clone()));
    assert_eq!(client.descriptor_for(&key(1)), Ok(r10.clone()));
    assert_eq!(
        (net.calls("Read"), net.calls("OracleNext")),
        (vec![1], vec![1])
    );
    // Split: 10 = [user, 50) epoch 2, 12 = [50, MAX) on node 3.
    let left = desc(10, 2, first_user_key(), Some(key(50)), &[2]);
    let right = desc(12, 1, key(50), None, &[3]);
    directory.record(&left, 50);
    directory.record(&right, 50);
    net.host(2, FakeRange::new(left.clone(), None));
    net.host(3, FakeRange::new(right.clone(), None));
    // Stale positive: the old epoch is rejected, returned, and replaces the cached entry.
    let err = client.read(read_req(&r10, 1)).unwrap_err();
    assert_eq!(
        err,
        RangeError::StaleEpoch {
            descriptor: left.clone()
        }
    );
    assert_eq!(client.descriptor_for(&key(10)), Ok(left.clone()));
    assert_eq!(net.calls("Read"), vec![1, 2]);
    assert_eq!(client.descriptor_for(&key(99)), Ok(right.clone()));
    assert_eq!(net.calls("Read"), vec![1, 2, 1]);
    // Redirect limit: hints that never reach a leader stop after three sends.
    net.nodes[&3].ranges.lock().unwrap().clear();
    net.hints.lock().unwrap().insert(right.id, 4);
    assert!(matches!(
        client.heartbeat(right.id, txn()),
        Err(RangeError::Unavailable(_))
    ));
    assert_eq!(net.calls("Heartbeat"), vec![3, 4, 4]);
}

#[test]
fn not_leader_hint_followed_and_voters_preferred() {
    let mut r10 = desc(10, 1, first_user_key(), None, &[]);
    r10.replicas = replicas(&[
        (2, ReplicaRole::Learner),
        (3, ReplicaRole::Voter),
        (4, ReplicaRole::Voter),
    ]);
    let (net, _) = Net::new(std::slice::from_ref(&r10));
    net.host(4, FakeRange::new(r10.clone(), None));
    net.hints.lock().unwrap().insert(r10.id, 4);
    let client = net.client(9);
    let range = client.descriptor_for(&key(5)).unwrap();
    let lock = LockRequest {
        range: range.id,
        epoch: range.epoch,
        txn: txn(),
        priority: Timestamp(1),
        target: LockTarget::TableSentinel { table: 7 },
        mode: RangeLockMode::Shared,
        timeout_ms: None,
    };
    assert_eq!(client.lock(lock.clone()), Ok(LockGrant { latest: None }));
    assert_eq!(client.lock(lock), Ok(LockGrant { latest: None }));
    assert_eq!(net.calls("Lock"), vec![3, 4, 4]);
}

#[test]
fn local_leaders_are_called_in_process() {
    let r10 = desc(10, 1, first_user_key(), None, &[1]);
    let (net, _) = Net::new(std::slice::from_ref(&r10));
    net.host(1, FakeRange::new(r10.clone(), None));
    let client = net.client(1);
    assert_eq!(client.descriptor_for(&key(5)), Ok(r10.clone()));
    assert_eq!(
        client.commit(r10.id, r10.epoch, txn(), vec![], None),
        Ok(Timestamp(7))
    );
    assert_eq!(client.finalize(r10.id, txn(), Outcome::Abort), Ok(()));
    assert!(client.oracle().next(2).unwrap() > Timestamp(100));
    assert_eq!(client.lease(LeaseKind::Xid, 0, 4), Ok(0..4));
    assert!(net.log.lock().unwrap().is_empty());
}

#[test]
fn stale_epoch_returned_for_read_but_retried_for_release() {
    let old = desc(10, 1, first_user_key(), None, &[2]);
    let new = desc(10, 2, first_user_key(), None, &[3]);
    let (net, _) = Net::new(std::slice::from_ref(&old));
    net.host(2, FakeRange::new(old.clone(), Some(new.clone())));
    net.host(3, FakeRange::new(new.clone(), None));
    let client = net.client(9);
    assert_eq!(client.descriptor_for(&key(5)), Ok(old.clone()));
    assert_eq!(client.release(old.id, txn()), Ok(()));
    assert_eq!(net.calls("Release"), vec![2, 3]);
    let err = client.read(read_req(&old, 1)).unwrap_err();
    assert_eq!(
        err,
        RangeError::StaleEpoch {
            descriptor: new.clone()
        }
    );
    assert_eq!(net.calls("Read"), vec![1, 3]);
    client.invalidate(&old);
    assert_eq!(client.descriptor_for(&key(5)), Ok(new.clone()));
    client.invalidate(&new);
    assert_eq!(client.descriptor_for(&key(5)), Ok(old));
}
