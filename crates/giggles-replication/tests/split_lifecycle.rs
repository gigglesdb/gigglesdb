//! Range split crash recovery (GD-0075 R5-80 acceptance, P9-2b): three RangeStore
//! nodes in process restart as a whole at every durable step of a split (crashing
//! replica threads at the split failpoints), through a nested split of the right
//! child; the final rows equal an oracle of acknowledged writes. Writes racing a
//! split at the old epoch apply exactly once or not at all. Stores restored from a
//! pre-split copy (lost RocksDB writes, kept Raft logs) replay the split and the child.
use super::common::range_net::{Cluster, NODES, row, rows_batch, start, table, temp_dir, wait};
use giggles_core::{
    range_api::{Epoch, RangeId, RangeSpace, first_user_key, range_key},
    replicated::RangeStore,
};
use giggles_replication::{failpoint, range_state::encode_proposal, sink::ReplicateError};
use rocksdb::{DB, IteratorMode, Options};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

const PARENT: RangeId = RangeId(2);
const RIGHT: RangeId = RangeId(1000);
const NESTED: RangeId = RangeId(1001);

/// Raw Rows keys to values.
type Rows = BTreeMap<Vec<u8>, Vec<u8>>;

/// xorshift64*: the seed's choices of rows and split keys.
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d) % n
    }
}

/// Every acknowledged row; each write uses a fresh row number, so keys never repeat.
struct Oracle {
    rows: Rows,
    seed: u64,
    next: u8,
}
impl Oracle {
    /// Writes one row per table to `range` at `epoch`; the write must be acknowledged.
    fn put(&mut self, c: &Cluster, range: RangeId, epoch: Epoch, tables: &[u64]) {
        let mut puts = Vec::new();
        for &t in tables {
            self.next += 1;
            let value = format!("{}-{}", self.seed, self.next).into_bytes();
            puts.push((row(t, self.next), value));
        }
        // Rows are unique and idempotent: an unknown outcome is retried.
        for _ in 0..10 {
            match c.write_rows(range, epoch, &puts) {
                Ok(()) => return self.rows.extend(puts),
                Err(ReplicateError::Unknown(e)) => eprintln!("retrying a write: {e}"),
                Err(e) => panic!("write to range {} at epoch {}: {e}", range.0, epoch.0),
            }
        }
        panic!("write to range {} never acknowledged", range.0);
    }
}

/// The range key a raw Rows key is routed by (its 8-byte version suffix dropped).
fn routed(raw: &[u8]) -> Vec<u8> {
    range_key(RangeSpace::Rows, &raw[..raw.len() - 8])
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

/// Splits `range` at `at` into `right` unless an earlier attempt already applied (the
/// leader's descriptor, read after a read barrier, moved past `from`), then waits until
/// every node applied it and hosts `right`. Both halves end at exactly `from` + 1.
fn ensure_split(c: &Cluster, range: RangeId, at: &[u8], right: RangeId, from: Epoch) {
    let next = Epoch(from.0 + 1);
    for attempt in 0.. {
        assert!(attempt < 50, "the split of range {} never applied", range.0);
        let leader = c.leader(range);
        let host = c.host(leader);
        if host.group(range).unwrap().read_barrier().is_err() {
            thread::sleep(Duration::from_millis(20));
            continue;
        }
        if c.store(leader).descriptor(range).unwrap().unwrap().epoch != from {
            break;
        }
        if let Err(e) = host.split(range, at, right) {
            eprintln!("split of range {} at epoch {}: {e}", range.0, from.0);
        }
    }
    wait(
        "the split applied and its right range hosted everywhere",
        || {
            NODES.iter().all(|&n| {
                let child = c.store(n).descriptor(right).unwrap();
                child.is_some_and(|d| d.epoch == next) && c.host(n).group(right).is_some()
            })
        },
    );
    for n in NODES {
        let left = c.store(n).descriptor(range).unwrap().unwrap();
        let child = c.store(n).descriptor(right).unwrap().unwrap();
        assert_eq!(
            (left.epoch, left.end.as_deref()),
            (next, Some(at)),
            "node {n}"
        );
        assert_eq!(
            (child.epoch, child.start.as_slice()),
            (next, at),
            "node {n}"
        );
    }
}

/// Every node: the same descriptors for `ranges`, which tile the user key space, a
/// group for each, and exactly the expected rows inside each range's bounds.
fn check_ranges(c: &Cluster, expected: &Rows, ranges: &[RangeId]) {
    let mut tiles: Vec<_> = ranges
        .iter()
        .map(|&r| c.store(1).descriptor(r).unwrap().unwrap())
        .collect();
    tiles.sort_by(|a, b| a.start.cmp(&b.start));
    assert_eq!(tiles[0].start, first_user_key());
    for pair in tiles.windows(2) {
        assert_eq!(pair[0].end.as_ref(), Some(&pair[1].start));
    }
    assert_eq!(tiles.last().unwrap().end, None);
    for n in NODES {
        for d in &tiles {
            assert_eq!(c.store(n).descriptor(d.id).unwrap().as_ref(), Some(d));
            assert!(c.host(n).group(d.id).is_some(), "node {n} range {}", d.id.0);
            let owned = expected.keys().filter(|k| d.contains(&routed(k))).count();
            assert_eq!(c.rows(n, d.id), owned as u64, "node {n} range {}", d.id.0);
        }
    }
}

/// Stops every host and closes the stores.
fn stop(c: Cluster) {
    for host in &c.hosts {
        host.stop();
    }
}

/// One seed and one restart boundary b of the split sequence: (0) before the split is
/// proposed; (1) a follower crashed at `range.split.applied`; (2) the leader crashed
/// there; (3) right after `split` returned, a follower crashed at
/// `range.split.child_opened`; (4) after writes to both halves; (5) after a nested
/// split of the right child. Then one more full restart and the final checks.
fn run(seed: u64, boundary: u8, cluster: [u8; 16]) {
    let mut g = Rng::new(seed);
    let t1 = 100 + g.below(100);
    let t2 = t1 + 1 + g.below(100);
    let (k1, k2) = (table(t1), table(t2));
    // Rows before the split: three left of k1, three right of it (around k2).
    let mut pre: Vec<u64> = (0..3).map(|_| 10 + g.below(t1 - 10)).collect();
    pre.extend([t1 + g.below(t2 - t1), t2, t2 + 1 + g.below(100)]);
    let dir = temp_dir();
    let mut c = start(&dir, cluster, true);
    let mut oracle = Oracle {
        rows: Rows::new(),
        seed,
        next: 0,
    };
    oracle.put(&c, PARENT, Epoch(1), &pre);
    if boundary == 0 {
        c = c.restart();
    }

    let leader = c.leader(PARENT);
    let follower = NODES.into_iter().find(|&n| n != leader).unwrap();
    let crash = match boundary {
        1 => Some(("range.split.applied", follower)),
        2 => Some(("range.split.applied", leader)),
        3 => Some(("range.split.child_opened", follower)),
        _ => None,
    };
    let fired = failpoint::fired(cluster).len();
    if let Some((point, node)) = crash {
        failpoint::arm_node(cluster, point, 1, node);
    }
    let result = c.host(leader).split(PARENT, &k1, RIGHT);
    if let Some((point, node)) = crash {
        wait("the split failpoint", || {
            failpoint::fired(cluster).len() > fired
        });
        failpoint::disarm(cluster);
        assert_eq!(failpoint::fired(cluster)[fired..], [(point, Some(node))]);
        assert!(node == leader || result.is_ok(), "{result:?}");
        c = c.restart();
    } else {
        result.unwrap();
    }
    ensure_split(&c, PARENT, &k1, RIGHT, Epoch(1));
    oracle.put(&c, PARENT, Epoch(2), &[10 + g.below(t1 - 10)]);
    oracle.put(
        &c,
        RIGHT,
        Epoch(2),
        &[t1 + g.below(t2 - t1), t2 + g.below(9)],
    );
    if boundary == 4 {
        c = c.restart();
    }

    ensure_split(&c, RIGHT, &k2, NESTED, Epoch(2));
    if boundary == 5 {
        c = c.restart();
    }

    // One more full restart; the closed stores hold exactly the oracle's rows.
    stop(c);
    for n in NODES {
        assert!(
            disk_rows(&dir, n) == oracle.rows,
            "seed {seed} b{boundary} node {n}"
        );
    }
    let c = start(&dir, cluster, false);
    let ranges = [(PARENT, 2, t1 - 1), (RIGHT, 3, t1), (NESTED, 3, t2)];
    check_ranges(&c, &oracle.rows, &[PARENT, RIGHT, NESTED]);
    // Every group elects and accepts a write; every stale epoch is refused.
    for (range, epoch, t) in ranges {
        oracle.put(&c, range, Epoch(epoch), &[t]);
        for stale in 1..epoch {
            let put = [(row(t, 255), b"stale".to_vec())];
            let refused = c.write_refused(range, Epoch(stale), &put);
            assert!(
                matches!(refused, Err(ReplicateError::Refused(_))),
                "{refused:?}"
            );
        }
    }
    check_ranges(&c, &oracle.rows, &[PARENT, RIGHT, NESTED]);
    stop(c);
    let _ = std::fs::remove_dir_all(&dir);
}

fn restart_at_every_boundary(seed: u64, cluster: [u8; 16]) {
    for boundary in 0..=5 {
        run(seed, boundary, cluster);
    }
}

#[test]
fn restart_at_every_split_boundary_seed_1() {
    restart_at_every_boundary(1, [0x5b; 16]);
}
#[test]
fn restart_at_every_split_boundary_seed_7() {
    restart_at_every_boundary(7, [0x5c; 16]);
}
#[test]
fn restart_at_every_split_boundary_seed_91() {
    restart_at_every_boundary(91, [0x5d; 16]);
}

/// A writer proposes rows (both sides of the split key) to range 2 at the pre-split
/// epoch on the leader's handle while the split runs: an acknowledged row is present
/// on every node in the range that owns its key, a refused one nowhere.
#[test]
fn writes_racing_a_split_apply_once_or_never() {
    const CLUSTER: [u8; 16] = [0x5e; 16];
    for iteration in 0..5 {
        let mut g = Rng::new(100 + iteration);
        let t1 = 100 + g.below(100);
        let dir = temp_dir();
        let c = start(&dir, CLUSTER, true);
        let leader = c.leader(PARENT);
        let handle = c.host(leader).group(PARENT).unwrap();
        let (split, writes) = (
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicUsize::new(0)),
        );
        let (done, count) = (split.clone(), writes.clone());
        let writer = thread::spawn(move || {
            let (mut outcomes, mut refused) = (Vec::new(), 0);
            for i in 0..=255u8 {
                if done.load(Ordering::SeqCst) && refused >= 3 {
                    break;
                }
                let t = if i % 2 == 0 {
                    10 + u64::from(i) % 80
                } else {
                    t1 + u64::from(i)
                };
                let put = [(row(t, i), vec![i])];
                let result = handle.replicate(encode_proposal(Epoch(1), &rows_batch(&put)));
                refused += usize::from(matches!(result, Err(ReplicateError::Refused(_))));
                outcomes.push((put[0].clone(), result));
                count.fetch_add(1, Ordering::SeqCst);
            }
            outcomes
        });
        let ahead = 1 + g.below(8) as usize;
        wait("writes before the split", || {
            writes.load(Ordering::SeqCst) >= ahead
        });
        thread::sleep(Duration::from_millis(g.below(10)));
        c.host(leader).split(PARENT, &table(t1), RIGHT).unwrap();
        split.store(true, Ordering::SeqCst);
        let outcomes = writer.join().unwrap();
        c.wait_applied(PARENT, c.applied(leader, PARENT));
        wait("the right range on every node", || {
            NODES.iter().all(|&n| c.host(n).group(RIGHT).is_some())
        });

        let (mut acked, mut absent, mut unknown) = (Rows::new(), Vec::new(), Vec::new());
        for ((key, value), result) in outcomes {
            match result {
                Ok(()) => {
                    acked.insert(key, value);
                }
                Err(ReplicateError::Refused(_) | ReplicateError::NotLeader) => absent.push(key),
                Err(_) => unknown.push(key),
            }
        }
        assert!(
            !acked.is_empty() && absent.len() >= 3,
            "iteration {iteration}"
        );
        check_ranges(&c, &acked, &[PARENT, RIGHT]);
        stop(c);
        for n in NODES {
            let mut rows = disk_rows(&dir, n);
            rows.retain(|k, _| !unknown.contains(k));
            assert!(rows == acked, "iteration {iteration} node {n}");
            assert!(absent.iter().all(|k| !rows.contains_key(k)));
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// A child group that fails to open when its split applies (a file sits where its
/// snapshot directory goes) is opened by the host's retry once the cause is gone.
#[test]
fn a_failed_child_open_is_retried() {
    let dir = temp_dir();
    let c = start(&dir, [0x5f; 16], true);
    let leader = c.leader(PARENT);
    let follower = NODES.into_iter().find(|&n| n != leader).unwrap();
    let blocker = dir.join(format!("n{follower}/snap/{}", RIGHT.0));
    std::fs::write(&blocker, b"in the way").unwrap();
    c.host(leader).split(PARENT, &table(100), RIGHT).unwrap();
    wait("the follower applied the split", || {
        c.store(follower).descriptor(RIGHT).unwrap().is_some()
    });
    thread::sleep(Duration::from_millis(150));
    assert!(c.host(follower).group(RIGHT).is_none());
    std::fs::remove_file(&blocker).unwrap();
    wait("the retry opened the child", || {
        c.host(follower).group(RIGHT).is_some()
    });
    c.write(RIGHT, Epoch(2), &[row(200, 1)]).unwrap();
    assert_eq!(c.rows(follower, RIGHT), 1);
    stop(c);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Copies a closed store directory.
fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

/// The recovery path the split design relies on (review S6): the RocksDB writes of the split
/// and of everything after it are lost on `all` nodes (else on one follower of the parent),
/// while every Raft log and snapshot directory is kept. Reopened, the parent replays the split
/// from its log, the listener re-creates the child over its existing log, and the child's
/// entries past the split index re-apply. The parent's log is never compacted here
/// (`RangeHostConfig::policy` is None), so the split entry is still in it.
fn recover_lost_split_writes(all: bool, cluster: [u8; 16]) {
    let dir = temp_dir();
    let c = start(&dir, cluster, true);
    let mut oracle = Oracle {
        rows: Rows::new(),
        seed: u64::from(all),
        next: 0,
    };
    let k1 = table(150);
    oracle.put(&c, PARENT, Epoch(1), &[20, 60, 149, 150, 151, 300]);
    let pre = oracle.rows.clone();
    stop(c);
    let presplit = |n: u64| dir.join(format!("n{n}/presplit"));
    for n in NODES {
        copy_dir(&dir.join(format!("n{n}/data")), &presplit(n));
    }

    let c = start(&dir, cluster, false);
    ensure_split(&c, PARENT, &k1, RIGHT, Epoch(1));
    let split_at = NODES
        .map(|n| c.applied(n, RIGHT))
        .into_iter()
        .max()
        .unwrap();
    oracle.put(&c, PARENT, Epoch(2), &[30, 100]);
    oracle.put(&c, RIGHT, Epoch(2), &[160, 400]);
    oracle.put(&c, RIGHT, Epoch(2), &[200]);
    let applied = [PARENT, RIGHT].map(|r| (r, c.applied(c.leader(r), r)));
    assert!(
        applied[1].1 > split_at,
        "the child's log has entries past the split"
    );
    let follower = NODES.into_iter().find(|&n| n != c.leader(PARENT)).unwrap();
    let restored = if all { NODES.to_vec() } else { vec![follower] };
    stop(c);
    for &n in &restored {
        let data = dir.join(format!("n{n}/data"));
        std::fs::remove_dir_all(&data).unwrap();
        copy_dir(&presplit(n), &data);
        assert!(disk_rows(&dir, n) == pre, "node {n}");
        let store = RangeStore::open(&data).unwrap();
        assert_eq!(store.descriptor(PARENT).unwrap().unwrap().epoch, Epoch(1));
        assert_eq!(store.descriptor(RIGHT).unwrap(), None);
    }

    let c = start(&dir, cluster, false);
    wait(
        "the split replayed and the child re-created everywhere",
        || {
            NODES.iter().all(|&n| {
                let child = c.store(n).descriptor(RIGHT).unwrap();
                child.is_some_and(|d| d.epoch == Epoch(2)) && c.host(n).group(RIGHT).is_some()
            })
        },
    );
    for (range, index) in applied {
        c.wait_applied(range, index);
    }
    check_ranges(&c, &oracle.rows, &[PARENT, RIGHT]);
    // Both groups elect and accept writes; the old epoch stays refused.
    oracle.put(&c, PARENT, Epoch(2), &[40]);
    oracle.put(&c, RIGHT, Epoch(2), &[500]);
    let stale = c.write_refused(PARENT, Epoch(1), &[(row(50, 255), b"stale".to_vec())]);
    assert!(
        matches!(stale, Err(ReplicateError::Refused(_))),
        "{stale:?}"
    );
    check_ranges(&c, &oracle.rows, &[PARENT, RIGHT]);
    stop(c);
    for n in NODES {
        assert!(disk_rows(&dir, n) == oracle.rows, "all {all} node {n}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn lost_split_writes_on_every_node_replay_from_the_logs() {
    recover_lost_split_writes(true, [0x60; 16]);
}
#[test]
fn lost_split_writes_on_one_follower_replay_from_its_logs() {
    recover_lost_split_writes(false, [0x61; 16]);
}
