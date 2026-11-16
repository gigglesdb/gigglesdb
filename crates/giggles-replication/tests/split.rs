//! Range split (GD-0075 R5-80): the v2 split envelope, the split apply over
//! MemRangeStore, and `RangeHost::split` over three RangeStore nodes in process.
use super::common::range_net::{Cluster, NODES, row, start, start_with, table, temp_dir, wait};
use giggles_core::{
    range_api::{
        ClusterTxnId, Epoch, HomeOp, HomeReply, LockGrant, LockRequest, Outcome, RangeDescriptor,
        RangeEngineOpener, RangeError, RangeId, RangeService, RangeSpace, RangeWrite, ReadPage,
        ReadRequest, ReplicaDesc, ReplicaRole, Timestamp, first_user_key, range_key,
    },
    replicated::BatchSink,
};
use giggles_replication::{
    client::LocalRanges,
    range_state::{
        MemOp, MemRangeStore, RangeProposal, RangeStateMachine, RangeStoreOps, SplitEvent,
        decode_range_proposal, encode_proposal, encode_split_proposal,
    },
    sink::ReplicateError,
    state::{AppliedPosition, ApplyOutcome::*, StateMachine},
};
use raft::StateRole;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    thread,
};

const CLUSTER: [u8; 16] = [79; 16];
const PARENT: RangeId = RangeId(2);
const RIGHT: RangeId = RangeId(1000);

#[test]
fn split_envelope_is_pinned_and_strict() {
    let at = table(100);
    let bytes = encode_split_proposal(Epoch(5), &at, RIGHT);
    // Persisted Raft-log format: [2][epoch BE8][kind 1][at_key len BE4 + bytes][right BE8].
    let pinned = [
        2, 0, 0, 0, 0, 0, 0, 0, 5, 1, 0, 0, 0, 9, 0x80, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0,
        0x03, 0xe8,
    ];
    assert_eq!(bytes, pinned);
    let split = RangeProposal::Split {
        at_key: &at,
        right: RIGHT,
    };
    assert_eq!(decode_range_proposal(&bytes).unwrap(), (Epoch(5), split));
    let v1 = encode_proposal(Epoch(5), b"batch");
    let batch = RangeProposal::Batch(&b"batch"[..]);
    assert_eq!(decode_range_proposal(&v1).unwrap(), (Epoch(5), batch));
    for len in 0..bytes.len() {
        assert!(
            decode_range_proposal(&bytes[..len]).is_err(),
            "prefix {len}"
        );
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(decode_range_proposal(&trailing).is_err());
    let mut kind = bytes.clone();
    kind[9] = 2;
    assert!(decode_range_proposal(&kind).is_err());
    let mut version = bytes;
    version[0] = 3;
    assert!(decode_range_proposal(&version).is_err());
}

#[test]
fn mem_store_split_moves_keys_and_notifies_the_listener() {
    let store = Arc::new(MemRangeStore::new());
    let replicas = vec![ReplicaDesc {
        node: 1,
        role: ReplicaRole::Voter,
    }];
    let parent = RangeDescriptor {
        id: RangeId(7),
        epoch: Epoch(1),
        start: table(1),
        end: None,
        replicas,
    };
    store.create_range(parent).unwrap();
    let s = RangeStateMachine::new(RangeId(7), store.clone());
    let heard = Arc::new(Mutex::new(Vec::new()));
    let sink = heard.clone();
    s.set_split_listener(Arc::new(move |d: &RangeDescriptor, _: SplitEvent| {
        sink.lock().unwrap().push(d.clone());
    }));
    let pos = |index| AppliedPosition { index, term: 1 };
    let put = |key: Vec<u8>| {
        let value = b"v".to_vec();
        encode_proposal(
            Epoch(1),
            &MemRangeStore::batch(&[MemOp::Put { key, value }]),
        )
    };
    assert_eq!(s.apply(&put(table(50)), pos(1)).unwrap(), Applied);
    assert_eq!(s.apply(&put(table(200)), pos(2)).unwrap(), Applied);
    // Stale epoch, key not strictly inside, right id = parent or reserved: refused.
    let refused = [
        (Epoch(2), table(100), RangeId(8)),
        (Epoch(1), table(1), RangeId(8)),
        (Epoch(1), table(100), RangeId(7)),
        (Epoch(1), table(100), RangeId(1)),
    ];
    for (i, (epoch, at, right)) in refused.into_iter().enumerate() {
        let index = 3 + i as u64;
        let split = encode_split_proposal(epoch, &at, right);
        assert_eq!(s.apply(&split, pos(index)).unwrap(), Refused);
        assert!(s.take_refusal(index).unwrap().contains("refused the split"));
    }
    assert!(heard.lock().unwrap().is_empty());
    let split = encode_split_proposal(Epoch(1), &table(100), RangeId(8));
    assert_eq!(s.apply(&split, pos(7)).unwrap(), Applied);
    assert_eq!(s.apply(&split, pos(7)).unwrap(), Skipped);
    let right = store.descriptor(RangeId(8)).unwrap().unwrap();
    assert_eq!(*heard.lock().unwrap(), vec![right.clone()]);
    assert_eq!(
        (&right.start, &right.end, right.epoch),
        (&table(100), &None, Epoch(2))
    );
    let left = store.descriptor(RangeId(7)).unwrap().unwrap();
    assert_eq!((left.end, left.epoch), (Some(table(100)), Epoch(2)));
    assert_eq!(store.keys(RangeId(7)), vec![table(50)]);
    assert_eq!(store.keys(RangeId(8)), vec![table(200)]);
    assert_eq!(store.applied(RangeId(8)).unwrap(), Some(pos(7)));
    // The parent's batches at the old epoch are refused now.
    assert_eq!(s.apply(&put(table(60)), pos(8)).unwrap(), Refused);
}

#[test]
fn split_opens_the_right_group_everywhere_and_survives_restart() {
    let dir = temp_dir();
    let c = start(&dir, CLUSTER, true);
    let at = table(100);
    let left_rows = [row(50, 1), row(50, 2), row(50, 3)];
    let right_rows = [row(200, 1), row(200, 2)];
    c.write(
        PARENT,
        Epoch(1),
        &[&left_rows[..], &right_rows[..]].concat(),
    )
    .unwrap();

    let leader = c.leader(PARENT);
    let (left, right) = c.host(leader).split(PARENT, &at, RIGHT).unwrap();
    assert_eq!(
        (left.id, left.epoch, &left.end),
        (PARENT, Epoch(2), &Some(at.clone()))
    );
    assert_eq!(
        (right.id, right.epoch, &right.start),
        (RIGHT, Epoch(2), &at)
    );
    assert_eq!((right.end.clone(), &right.replicas), (None, &left.replicas));
    let split_at = c.applied(leader, PARENT);
    c.wait_applied(PARENT, split_at);
    // Every node hosts the right group; its log starts at the split index.
    wait("right group on every node", || {
        NODES.iter().all(|&n| c.host(n).group(RIGHT).is_some())
    });
    for node in NODES {
        assert_eq!(c.host(node).groups(), [RangeId(1), PARENT, RIGHT]);
        assert_eq!(
            c.store(node).descriptor(PARENT).unwrap(),
            Some(left.clone())
        );
        assert_eq!(
            c.store(node).descriptor(RIGHT).unwrap(),
            Some(right.clone())
        );
        assert!(c.applied(node, RIGHT) >= split_at);
        assert!(c.host(node).group(RIGHT).unwrap().last_index().unwrap() >= split_at);
    }

    // Both halves elect and accept in-bounds writes at the new epoch.
    c.leader(RIGHT);
    c.write(RIGHT, Epoch(2), &[row(300, 1)]).unwrap();
    assert!(NODES.iter().all(|&n| c.applied(n, RIGHT) > split_at));
    c.write(PARENT, Epoch(2), &[row(60, 1)]).unwrap();
    // The old epoch and the other half's keys are refused.
    let stale = c.write(PARENT, Epoch(1), &[row(60, 2)]);
    assert!(
        matches!(stale, Err(ReplicateError::Refused(_))),
        "{stale:?}"
    );
    let outside = c.write(PARENT, Epoch(2), &[row(200, 9)]);
    assert!(
        matches!(outside, Err(ReplicateError::Refused(_))),
        "{outside:?}"
    );
    let outside = c.write(RIGHT, Epoch(2), &[row(50, 9)]);
    assert!(
        matches!(outside, Err(ReplicateError::Refused(_))),
        "{outside:?}"
    );
    for node in NODES {
        assert_eq!(
            (c.rows(node, PARENT), c.rows(node, RIGHT)),
            (4, 3),
            "node {node}"
        );
    }

    // Restart: the same groups and data; both groups elect and accept writes.
    for host in &c.hosts {
        host.stop();
        assert!(host.groups().is_empty());
    }
    drop(c);
    let c = start(&dir, CLUSTER, false);
    for node in NODES {
        assert_eq!(c.host(node).groups(), [RangeId(1), PARENT, RIGHT]);
        assert_eq!(
            c.store(node).descriptor(PARENT).unwrap(),
            Some(left.clone())
        );
        assert_eq!(
            c.store(node).descriptor(RIGHT).unwrap(),
            Some(right.clone())
        );
        assert_eq!(
            (c.rows(node, PARENT), c.rows(node, RIGHT)),
            (4, 3),
            "node {node}"
        );
    }
    c.write(PARENT, Epoch(2), &[row(70, 1)]).unwrap();
    c.write(RIGHT, Epoch(2), &[row(400, 1)]).unwrap();
    for node in NODES {
        assert_eq!(
            (c.rows(node, PARENT), c.rows(node, RIGHT)),
            (5, 4),
            "node {node}"
        );
    }
    drop(c);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn split_needs_the_leader_and_a_key_inside_the_range() {
    let dir = temp_dir();
    let c = start(&dir, CLUSTER, true);
    let leader = c.leader(PARENT);
    let follower = NODES.into_iter().find(|&n| n != leader).unwrap();
    let err = c
        .host(follower)
        .split(PARENT, &table(100), RIGHT)
        .unwrap_err();
    assert!(err.to_string().contains("leader hint"), "{err}");

    // Range 2 starts at first_user_key(): neither it nor a key below it splits.
    for at in [first_user_key(), range_key(RangeSpace::Catalog, b"x")] {
        let leader = c.leader(PARENT);
        let err = c.host(leader).split(PARENT, &at, RIGHT).unwrap_err();
        // The proposer's check_split_key (review S3) refuses it before the apply would.
        assert!(err.to_string().contains("split key refused"), "{err}");
        c.wait_applied(PARENT, c.applied(leader, PARENT));
    }
    for node in NODES {
        let descriptor = c.store(node).descriptor(PARENT).unwrap().unwrap();
        assert_eq!((descriptor.epoch, descriptor.end), (Epoch(1), None));
        assert_eq!(c.store(node).descriptor(RIGHT).unwrap(), None);
        assert_eq!(c.host(node).groups(), [RangeId(1), PARENT]);
    }
    drop(c);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Every engine open as (node, descriptor, lock generation), and the engines open now.
#[derive(Default)]
struct Opens {
    log: Mutex<Vec<(u64, RangeDescriptor, u64)>>,
    live: Mutex<BTreeMap<(u64, RangeId), RangeDescriptor>>,
}
impl Opens {
    fn live(&self, range: RangeId) -> Vec<(u64, RangeDescriptor)> {
        let live = self.live.lock().unwrap();
        live.iter()
            .filter(|((_, r), _)| *r == range)
            .map(|((n, _), d)| (*n, d.clone()))
            .collect()
    }
    /// Lock generations of the opens of `descriptor` on `node`.
    fn generations(&self, node: u64, descriptor: &RangeDescriptor) -> Vec<u64> {
        let log = self.log.lock().unwrap();
        log.iter()
            .filter(|(n, d, _)| *n == node && d == descriptor)
            .map(|(_, _, g)| *g)
            .collect()
    }
}

/// Records opens and closes; its engines serve only their descriptor.
struct Recorder {
    node: u64,
    opens: Arc<Opens>,
}
impl RangeEngineOpener for Recorder {
    fn open(
        &self,
        descriptor: RangeDescriptor,
        _: Arc<dyn BatchSink>,
        generation: u64,
    ) -> Result<Arc<dyn RangeService>, RangeError> {
        let mut live = self.opens.live.lock().unwrap();
        let old = live.insert((self.node, descriptor.id), descriptor.clone());
        assert!(old.is_none(), "open twice without close");
        let open = (self.node, descriptor.clone(), generation);
        self.opens.log.lock().unwrap().push(open);
        Ok(Arc::new(Stub(descriptor)))
    }
    fn close(&self, range: RangeId) {
        let closed = self.opens.live.lock().unwrap().remove(&(self.node, range));
        assert!(closed.is_some(), "close without open");
    }
}
struct Stub(RangeDescriptor);
fn stub<T>() -> Result<T, RangeError> {
    Err(RangeError::Unavailable("stub".into()))
}
impl RangeService for Stub {
    fn descriptor(&self) -> RangeDescriptor {
        self.0.clone()
    }
    fn read(&self, _: ReadRequest) -> Result<ReadPage, RangeError> {
        stub()
    }
    fn lock(&self, _: LockRequest) -> Result<LockGrant, RangeError> {
        stub()
    }
    fn blockers(&self, _: ClusterTxnId) -> Result<Vec<ClusterTxnId>, RangeError> {
        stub()
    }
    fn cancel_wait(&self, _: ClusterTxnId) -> Result<(), RangeError> {
        stub()
    }
    fn release(&self, _: ClusterTxnId) -> Result<(), RangeError> {
        stub()
    }
    fn heartbeat(&self, _: ClusterTxnId) -> Result<(), RangeError> {
        stub()
    }
    fn commit(
        &self,
        _: Epoch,
        _: ClusterTxnId,
        _: Vec<RangeWrite>,
        _: Option<u64>,
        _: Option<u64>,
    ) -> Result<Timestamp, RangeError> {
        stub()
    }
    fn prepare(
        &self,
        _: Epoch,
        _: ClusterTxnId,
        _: RangeId,
        _: Vec<RangeWrite>,
        _: Option<u64>,
    ) -> Result<Timestamp, RangeError> {
        stub()
    }
    fn finalize(&self, _: ClusterTxnId, _: Outcome) -> Result<(), RangeError> {
        stub()
    }
    fn home(&self, _: Epoch, _: HomeOp) -> Result<HomeReply, RangeError> {
        stub()
    }
}

fn leads(c: &Cluster, node: u64, range: RangeId) -> bool {
    let group = c.host(node).group(range);
    group.is_some_and(|g| g.role().state == StateRole::Leader)
}

/// Waits until `range` has exactly one open engine anywhere: on its leader (not `except`),
/// over that leader's stored descriptor, and served there. Returns (leader, descriptor).
fn settled_on(
    c: &Cluster,
    opens: &Opens,
    range: RangeId,
    except: Option<u64>,
) -> (u64, RangeDescriptor) {
    let mut found = None;
    wait(
        "one engine of the range, on its leader, at its stored epoch",
        || {
            let live = opens.live(range);
            let [(node, ref descriptor)] = live[..] else {
                return false;
            };
            if Some(node) == except {
                return false;
            }
            let stored = c.store(node).descriptor(range).unwrap();
            let served = c.host(node).service(range).map(|s| s.descriptor());
            let ok = leads(c, node, range)
                && stored.as_ref() == Some(descriptor)
                && served.as_ref() == Some(descriptor);
            if ok {
                found = Some((node, descriptor.clone()));
            }
            ok
        },
    );
    found.unwrap()
}
fn settled(c: &Cluster, opens: &Opens, range: RangeId) -> (u64, RangeDescriptor) {
    settled_on(c, opens, range, None)
}

#[test]
fn split_reopens_the_parent_engine_on_whichever_node_leads_it() {
    let dir = temp_dir();
    let opens = Arc::new(Opens::default());
    let opener = |node| {
        let recorder = Recorder {
            node,
            opens: opens.clone(),
        };
        Some(Arc::new(recorder) as Arc<dyn RangeEngineOpener>)
    };
    let c = start_with(&dir, [81; 16], true, &opener);

    // R5-81: the split apply marks the leader's engine moved; its role manager reopens it
    // over the narrowed descriptor (in the same term, with a fresh lock generation).
    let (a, before) = settled(&c, &opens, PARENT);
    assert_eq!(before.epoch, Epoch(1));
    let term = c.host(a).group(PARENT).unwrap().role().term;
    let (left, right) = c.host(a).split(PARENT, &table(100), RIGHT).unwrap();
    let (leader, reopened) = settled(&c, &opens, PARENT);
    assert_eq!(reopened, left);
    assert_eq!(settled(&c, &opens, RIGHT).1, right);
    let old = *opens.generations(a, &before).last().unwrap();
    let new = opens.generations(leader, &left)[0];
    assert!(new > old, "generation went back: {old} -> {new}");
    if leader == a && c.host(a).group(PARENT).unwrap().role().term == term {
        assert_eq!(new >> 20, old >> 20, "not a same-term reopen");
    }

    // The leader splits again and is cut off at once: another node takes over and opens
    // the narrowed descriptor; no engine at an older epoch stays open anywhere.
    let a = leader;
    let right2 = RangeId(1001);
    let (left2, right2) = c.host(a).split(PARENT, &table(50), right2).unwrap();
    c.isolate(a);
    let mut b = 0;
    wait("another node leads over the narrowed descriptor", || {
        let open = opens.live(PARENT);
        b = NODES
            .into_iter()
            .find(|&n| n != a && leads(&c, n, PARENT) && open.contains(&(n, left2.clone())))
            .unwrap_or(0);
        b != 0 && open.iter().all(|(_, d)| d.epoch >= left2.epoch)
    });
    assert_eq!(opens.generations(b, &left2).len(), 1);
    c.heal();
    assert_eq!(settled(&c, &opens, PARENT).1, left2);
    assert_eq!(settled(&c, &opens, right2.id).1, right2);

    // The leader is cut off right after proposing a split: whether or not the split
    // survives, the node leading afterwards serves its stored descriptor, and only it does.
    let a = settled(&c, &opens, PARENT).0;
    let split = thread::scope(|s| {
        let split = s.spawn(|| c.host(a).split(PARENT, &table(20), RangeId(1002)));
        c.isolate(a);
        settled_on(&c, &opens, PARENT, Some(a));
        c.heal();
        split.join().unwrap()
    });
    let (leader, last) = settled(&c, &opens, PARENT);
    assert!(last.epoch >= left2.epoch && leads(&c, leader, PARENT));
    match split {
        Ok((split_left, _)) => assert_eq!(split_left, last),
        Err(e) => eprintln!("split on a cut-off leader: {e}"),
    }
    for node in NODES {
        wait("the leader's descriptor on every node", || {
            c.store(node).descriptor(PARENT).unwrap().as_ref() == Some(&last)
        });
    }
    drop(c);
    let _ = std::fs::remove_dir_all(&dir);
}
