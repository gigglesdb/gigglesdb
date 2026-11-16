//! A replica that learns of a split only from a parent snapshot hosts the child (GD-0075
//! P9-2c): node 3 is cut off across a split and comes back after the parent's log was
//! compacted past the split entry. It installs the parent's snapshot, creates the child from
//! the parent's lineage, receives the child's snapshot (built on demand by its leader) and
//! catches up; the child then commits with node 3 and only one other node.
use super::common::range_net::{
    Cluster, NODES, row, rows_ops, start_with_policy, table, temp_dir, wait,
};
use giggles_core::range_api::{Epoch, RangeId};
use giggles_replication::{
    range_state::encode_proposal,
    sink::{MembershipStatus, SnapshotPolicy},
};
use raft::StateRole;
use rocksdb::{DB, IteratorMode, Options};
use std::{collections::BTreeMap, path::Path, thread, time::Duration};

const PARENT: RangeId = RangeId(2);
const CHILD: RangeId = RangeId(1000);

/// Raw Rows keys to values.
type Rows = BTreeMap<Vec<u8>, Vec<u8>>;
/// Puts (`Some`) and deletes (`None`) of raw Rows keys.
type Ops = [(Vec<u8>, Option<Vec<u8>>)];

/// The node among `nodes` leading `range` (waits for one).
fn leader(c: &Cluster, range: RangeId, nodes: &[u64]) -> u64 {
    let mut found = None;
    wait("a leader among the live nodes", || {
        found = nodes.iter().copied().find(|&n| {
            let group = c.host(n).group(range);
            group.is_some_and(|g| g.role().state == StateRole::Leader)
        });
        found.is_some()
    });
    found.unwrap()
}

/// Commits `ops` to `range` at `epoch` through a leader among `nodes`, waits until those
/// nodes applied it and records it in `rows`. Puts and deletes are idempotent: an unknown
/// outcome is retried.
fn commit(c: &Cluster, rows: &mut Rows, range: RangeId, epoch: Epoch, nodes: &[u64], ops: &Ops) {
    let proposal = encode_proposal(epoch, &rows_ops(ops));
    for _ in 0..50 {
        let node = leader(c, range, nodes);
        match c
            .host(node)
            .group(range)
            .unwrap()
            .replicate(proposal.clone())
        {
            Ok(()) => {
                let index = c.applied(node, range);
                wait("applied on the live nodes", || {
                    nodes.iter().all(|&n| c.applied(n, range) >= index)
                });
                for (key, value) in ops {
                    match value {
                        Some(value) => rows.insert(key.clone(), value.clone()),
                        None => rows.remove(key),
                    };
                }
                return;
            }
            Err(e) => {
                eprintln!("retrying a write to range {}: {e}", range.0);
                thread::sleep(Duration::from_millis(20));
            }
        }
    }
    panic!("write to range {} never acknowledged", range.0);
}

fn status(c: &Cluster, node: u64, range: RangeId) -> MembershipStatus {
    let group = c.host(node).group(range).unwrap();
    group.membership_status().unwrap()
}

/// Waits until every node applied as far as the furthest one, for each of `ranges`.
fn converge(c: &Cluster, ranges: &[RangeId]) {
    for &range in ranges {
        let top = NODES.iter().map(|&n| c.applied(n, range)).max().unwrap();
        wait("every node caught up", || {
            NODES.iter().all(|&n| c.applied(n, range) >= top)
        });
    }
}

/// The Rows column family of `node`'s closed store.
fn disk_rows(dir: &Path, node: u64) -> Rows {
    let path = dir.join(format!("n{node}/data"));
    let options = Options::default();
    let families = DB::list_cf(&options, &path).unwrap();
    let db = DB::open_cf_for_read_only(&options, &path, &families, false).unwrap();
    let cf = db.cf_handle("rows").unwrap();
    let rows = db.iterator_cf(cf, IteratorMode::Start).map(|e| {
        let (key, value) = e.unwrap();
        (key.to_vec(), value.to_vec())
    });
    rows.collect()
}

#[test]
fn a_replica_behind_a_compacted_split_hosts_the_child() {
    let dir = temp_dir();
    // `every_entries` stays above every applied index here: the parent's snapshots are the
    // ones the test asks for, and the child's can only come from the on-demand path.
    let policy = SnapshotPolicy {
        every_entries: 1_000,
        margin: 1,
    };
    let c = start_with_policy(&dir, [0x2c; 16], policy);
    let epoch = c.store(1).descriptor(PARENT).unwrap().unwrap().epoch;
    let value = |s: &str| Some(s.as_bytes().to_vec());
    let mut rows = Rows::new();
    let live = [1, 2];
    // Two right-half rows reach node 3; one is deleted while it is away.
    let first = [
        (row(100, 1), value("a")),
        (row(300, 1), value("b")),
        (row(300, 2), value("c")),
    ];
    commit(&c, &mut rows, PARENT, epoch, &NODES, &first);
    c.isolate(3);
    commit(
        &c,
        &mut rows,
        PARENT,
        epoch,
        &live,
        &[(row(100, 2), value("d"))],
    );
    let at = table(200);
    let (left, right) = loop {
        match c.host(leader(&c, PARENT, &live)).split(PARENT, &at, CHILD) {
            Ok(halves) => break halves,
            Err(e) => eprintln!("retrying the split: {e}"),
        }
    };
    wait("the child hosted on the live nodes", || {
        live.iter().all(|&n| c.host(n).group(CHILD).is_some())
    });
    let child_ops = [(row(300, 1), None), (row(300, 3), value("e"))];
    commit(&c, &mut rows, CHILD, right.epoch, &live, &child_ops);
    for n in 3..=6 {
        let put = [(row(100, n), value("f"))];
        commit(&c, &mut rows, PARENT, left.epoch, &live, &put);
    }
    // Snapshot the parent on both live replicas; the policy compacts their logs past the
    // split (margin 1), which the next write's driver round has done.
    for n in live {
        let applied = c.applied(n, PARENT);
        c.host(n).group(PARENT).unwrap().build_snapshot().unwrap();
        wait("the parent's snapshot", || {
            status(&c, n, PARENT).snapshot_index >= applied
        });
    }
    commit(
        &c,
        &mut rows,
        PARENT,
        left.epoch,
        &live,
        &[(row(100, 7), value("g"))],
    );
    for n in live {
        assert_eq!(status(&c, n, CHILD).snapshot_index, 0, "node {n}");
    }
    assert_eq!(c.store(3).descriptor(CHILD).unwrap(), None);
    assert!(c.host(3).group(CHILD).is_none());

    c.heal();
    wait("node 3 hosts the child", || {
        c.host(3).group(CHILD).is_some()
    });
    converge(&c, &[PARENT, CHILD]);
    // Node 3 learned the split from the parent's snapshot, never from its log, and the
    // child's leader built the child's snapshot on demand.
    let lineage: Vec<u64> = NODES
        .iter()
        .map(|&n| c.host(n).lineage_children())
        .collect();
    assert_eq!(lineage, [0, 0, 1]);
    assert!(
        live.iter()
            .any(|&n| status(&c, n, CHILD).snapshot_index > 0)
    );
    for n in NODES {
        assert_eq!(c.store(n).descriptor(PARENT).unwrap(), Some(left.clone()));
        assert_eq!(c.store(n).descriptor(CHILD).unwrap(), Some(right.clone()));
    }
    // Node 3 is a working voter of the child (and the parent): with node 1 cut off, each
    // commits through nodes 2 and 3 only.
    c.isolate(1);
    let pair = [2, 3];
    commit(
        &c,
        &mut rows,
        CHILD,
        right.epoch,
        &pair,
        &[(row(300, 4), value("h"))],
    );
    commit(
        &c,
        &mut rows,
        PARENT,
        left.epoch,
        &pair,
        &[(row(100, 8), value("i"))],
    );
    c.heal();
    converge(&c, &[PARENT, CHILD]);
    for host in &c.hosts {
        host.stop();
    }
    drop(c);
    // Every node holds exactly the acknowledged rows: node 3's stale right-half row went
    // with the child's install.
    for n in NODES {
        assert!(disk_rows(&dir, n) == rows, "node {n}");
    }
    assert!(!rows.contains_key(&row(300, 1)));
    let _ = std::fs::remove_dir_all(&dir);
}
