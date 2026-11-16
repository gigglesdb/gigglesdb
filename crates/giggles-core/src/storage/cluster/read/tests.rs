//! R5-41 acceptance: MVCC reads over `RoutedKvRead` equal MVCC reads of the full store.
use std::{
    ops::Bound,
    sync::Arc,
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use super::{
    RoutedKvRead,
    fake::{FakeClient, FakeCluster, ORACLE_START, keyspace, versioned},
};
use crate::storage::{
    engine::{
        hlc::Ts,
        kv::{Direction, KvPair, KvRead},
        mvcc,
    },
    range_api::{
        ClusterTxnId, Epoch, LeaseKind, LockRequest, LockTarget, RangeClient, RangeError, RangeId,
        RangeLockMode, RangeSpace, RangeWrite, ReadDirection, ReadRequest, Timestamp,
        first_user_key, range_key,
    },
};

/// xorshift64*: fixed seeds, reproducible runs.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

const TABLES: u64 = 3;
const KEYS: u64 = 40;

/// Rows: table BE8 || pk BE4; Index: index BE8 || tuple BE4. Fixed widths keep keys prefix-free.
fn key(id: u64, n: u64) -> Vec<u8> {
    [id.to_be_bytes().as_slice(), &(n as u32).to_be_bytes()].concat()
}

fn random_key(rng: &mut Rng, space: RangeSpace) -> Vec<u8> {
    let n = rng.below(KEYS);
    match space {
        RangeSpace::Rows => key(1 + rng.below(TABLES), n),
        RangeSpace::Index => key(101 + rng.below(TABLES), n),
        _ => (n as u32).to_be_bytes().to_vec(),
    }
}

fn random_space(rng: &mut Rng) -> RangeSpace {
    match rng.below(10) {
        0..=3 => RangeSpace::Rows,
        4..=6 => RangeSpace::Index,
        7..=8 => RangeSpace::Catalog,
        _ => RangeSpace::Seq,
    }
}

/// Table boundaries, inside a table's rows, the Rows/Index boundary, inside an index.
fn random_split(rng: &mut Rng) -> Vec<u8> {
    match rng.below(4) {
        0 => range_key(RangeSpace::Rows, &(1 + rng.below(TABLES)).to_be_bytes()),
        1 => range_key(RangeSpace::Rows, &random_key(rng, RangeSpace::Rows)),
        2 => range_key(RangeSpace::Index, &[]),
        _ => range_key(RangeSpace::Index, &random_key(rng, RangeSpace::Index)),
    }
}

fn random_bound(rng: &mut Rng, space: RangeSpace) -> Bound<Vec<u8>> {
    match rng.below(5) {
        0 => Bound::Unbounded,
        1 | 2 => Bound::Included(random_key(rng, space)),
        _ => Bound::Excluded(random_key(rng, space)),
    }
}

fn as_ref(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    bound.as_ref().map(Vec::as_slice)
}

fn routed(client: &Arc<FakeClient>, ts: Timestamp, page_bytes: usize) -> RoutedKvRead {
    RoutedKvRead::new(client.clone(), Ts(ts.0), page_bytes)
}

fn versions(
    kv: &dyn KvRead,
    space: RangeSpace,
    start: &Bound<Vec<u8>>,
    end: &Bound<Vec<u8>>,
    dir: Direction,
    ts: Timestamp,
) -> Result<Vec<KvPair>, String> {
    mvcc::VersionedRange::new(
        kv,
        keyspace(space),
        as_ref(start),
        as_ref(end),
        dir,
        Ts(ts.0),
    )
    .map_err(|e| format!("{e:?}"))?
    .collect::<Result<_, _>>()
    .map_err(|e| format!("{e:?}"))
}

/// One commit of 1-3 writes to the range owning a random key; returns its timestamp.
fn random_commit(rng: &mut Rng, client: &FakeClient, txn: u64) -> Option<Timestamp> {
    let space = random_space(rng);
    let first = random_key(rng, space);
    let desc = client.descriptor_for(&range_key(space, &first)).unwrap();
    let mut writes = Vec::new();
    for index in 0..1 + rng.below(3) {
        let key = if index == 0 {
            first.clone()
        } else {
            random_key(rng, space)
        };
        if desc.contains(&range_key(space, &key)) {
            let value =
                (!rng.chance(20)).then(|| vec![rng.below(256) as u8; rng.below(24) as usize]);
            writes.push(RangeWrite { space, key, value });
        }
    }
    match client.commit(desc.id, desc.epoch, ClusterTxnId(txn), writes, None, None) {
        Ok(ts) => Some(ts),
        Err(RangeError::StaleEpoch { .. }) => {
            client.invalidate(&desc);
            None
        }
        Err(e) => panic!("commit failed: {e}"),
    }
}

fn model(seed: u64, prefetch: usize) {
    let mut rng = Rng(seed);
    let mut splits: Vec<_> = (0..rng.below(7)).map(|_| random_split(&mut rng)).collect();
    splits.sort();
    splits.dedup();
    let cluster = FakeCluster::new(&splits);
    let writer = cluster.client();
    let gateway = cluster.client();
    let kv = cluster.kv();
    let mut stamps = vec![Timestamp(ORACLE_START - 1)];
    let mut txn = 0;
    for round in 0..3 {
        for _ in 0..150 {
            txn += 1;
            stamps.extend(random_commit(&mut rng, &writer, txn));
        }
        for _ in 0..round {
            // Later splits leave both gateways' route caches stale.
            let at = random_split(&mut rng);
            if cluster.descriptors().iter().all(|desc| desc.start != at) {
                cluster.split(&at);
            }
        }
        for check in 0..80 {
            let context = format!("seed {seed} prefetch {prefetch} round {round} check {check}");
            let ts = stamps[rng.below(stamps.len() as u64) as usize];
            let ts = Timestamp(ts.0 + rng.below(2));
            let page_bytes = 1 + rng.below(200) as usize;
            let reader = routed(&gateway, ts, page_bytes).with_prefetch(prefetch);
            let space = random_space(&mut rng);
            let ks = keyspace(space);
            let start = random_bound(&mut rng, space);
            let end = if rng.chance(10) {
                start.clone()
            } else {
                random_bound(&mut rng, space)
            };
            let dir = if rng.chance(50) {
                Direction::Forward
            } else {
                Direction::Reverse
            };
            if versioned(space) {
                assert_eq!(
                    versions(&reader, space, &start, &end, dir, ts),
                    versions(&*kv, space, &start, &end, dir, ts),
                    "{context}: {space:?} {start:?}..{end:?} {dir:?} at {ts:?}"
                );
                for _ in 0..3 {
                    let key = random_key(&mut rng, space);
                    assert_eq!(
                        mvcc::read_at(&reader, ks, &key, Ts(ts.0)).unwrap(),
                        mvcc::read_at(&*kv, ks, &key, Ts(ts.0)).unwrap(),
                        "{context}: point {key:?}"
                    );
                }
            } else {
                let raw = |read: &dyn KvRead| -> Vec<KvPair> {
                    read.range(ks, as_ref(&start), as_ref(&end), dir)
                        .unwrap()
                        .collect::<Result<_, _>>()
                        .unwrap()
                };
                assert_eq!(raw(&reader), raw(&*kv), "{context}: raw {start:?}..{end:?}");
                let key = random_key(&mut rng, space);
                assert_eq!(reader.get(ks, &key).unwrap(), kv.get(ks, &key).unwrap());
            }
        }
    }
}

#[test]
fn routed_reads_match_full_store_reads() {
    for seed in [0x5eed_0001, 0x5eed_0002, 0x5eed_0003, 0xdead_beef] {
        // Same seed, same cluster and checks: sequential, one ahead, the default four ahead.
        for prefetch in [0, 1, 4] {
            model(seed, prefetch);
        }
    }
}

/// Splits of table 1 every `width` keys, `count` user ranges; the first starts at
/// first_user_key(), the last is unbounded.
fn table_ranges(width: u64, count: u64) -> Arc<FakeCluster> {
    let splits: Vec<_> = (1..count)
        .map(|i| range_key(RangeSpace::Rows, &key(1, width * i)))
        .collect();
    FakeCluster::new(&splits)
}

/// One row of table 1 per `n`, committed to the range owning it; returns a read timestamp.
fn commit_rows(cluster: &Arc<FakeCluster>, rows: impl IntoIterator<Item = u64>) -> Timestamp {
    let writer = cluster.client();
    for (txn, n) in rows.into_iter().enumerate() {
        let key = key(1, n);
        let desc = writer
            .descriptor_for(&range_key(RangeSpace::Rows, &key))
            .unwrap();
        let write = RangeWrite {
            space: RangeSpace::Rows,
            key,
            value: Some(vec![n as u8; 10]),
        };
        writer
            .commit(
                desc.id,
                desc.epoch,
                ClusterTxnId(txn as u64 + 1),
                vec![write],
                None,
                None,
            )
            .unwrap();
    }
    writer.oracle().next(1).unwrap()
}

fn raw_rows(reader: &RoutedKvRead, dir: Direction) -> Vec<KvPair> {
    let rows = keyspace(RangeSpace::Rows);
    reader
        .range(rows, Bound::Unbounded, Bound::Unbounded, dir)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

/// 16 ranges with one row each (table 1, keys 5, 15, ..., 155).
fn sixteen_ranges() -> (Arc<FakeCluster>, Timestamp) {
    let cluster = table_ranges(10, 16);
    let ts = commit_rows(&cluster, (0..16).map(|i| 10 * i + 5));
    (cluster, ts)
}

const DIRS: [Direction; 2] = [Direction::Forward, Direction::Reverse];

#[test]
fn prefetch_matches_sequential_across_splits_and_faults() {
    let all = (Bound::Unbounded, Bound::Unbounded);
    let rows = RangeSpace::Rows;
    for dir in DIRS {
        for prefetch in [0, 4] {
            let context = format!("{dir:?} prefetch {prefetch}");
            // Six ranges of ten rows (ids 2..=7); pages of about two rows.
            let fresh = || {
                let cluster = table_ranges(10, 6);
                let ts = commit_rows(&cluster, 0..60);
                let client = cluster.client();
                let want = versions(&*cluster.kv(), rows, &all.0, &all.1, dir, ts).unwrap();
                assert_eq!(want.len(), 60);
                (cluster, ts, client, want)
            };

            // Splits of the current range and of ranges whose first page is already fetched.
            let (cluster, ts, client, want) = fresh();
            let reader = routed(&client, ts, 40).with_prefetch(prefetch);
            let mut scan = mvcc::VersionedRange::new(
                &reader,
                keyspace(rows),
                Bound::Unbounded,
                Bound::Unbounded,
                dir,
                Ts(ts.0),
            )
            .unwrap();
            let mut got: Vec<_> = scan.by_ref().take(15).map(Result::unwrap).collect();
            let ats = match dir {
                Direction::Forward => [17, 35, 55],
                Direction::Reverse => [42, 25, 5],
            };
            for at in ats {
                cluster.split(&range_key(rows, &key(1, at)));
            }
            got.extend(scan.map(Result::unwrap));
            assert_eq!(got, want, "{context}: splits");

            // StaleEpoch on range 4, which both directions prefetch: read it sequentially.
            let (cluster, ts, client, want) = fresh();
            let current = cluster.descriptors()[3].clone();
            assert_eq!(current.id, RangeId(4));
            cluster.fail_next(
                current.id,
                RangeError::StaleEpoch {
                    descriptor: current,
                },
            );
            let reader = routed(&client, ts, 40).with_prefetch(prefetch);
            let got = versions(&reader, rows, &all.0, &all.1, dir, ts);
            assert_eq!(got, Ok(want), "{context}: stale epoch");
            assert_eq!(client.stale_epochs(), 1, "{context}");

            // An error on range 4 (rows 20..30) surfaces only when the raw scan reaches it.
            let (cluster, ts, client, _) = fresh();
            let want: Vec<KvPair> = cluster
                .kv()
                .range(keyspace(rows), Bound::Unbounded, Bound::Unbounded, dir)
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            cluster.fail_next(RangeId(4), RangeError::Corrupt("bad page".into()));
            let reader = routed(&client, ts, 40).with_prefetch(prefetch);
            let got: Vec<_> = reader
                .range(keyspace(rows), Bound::Unbounded, Bound::Unbounded, dir)
                .unwrap()
                .collect();
            let before = match dir {
                Direction::Forward => 20,
                Direction::Reverse => 30,
            };
            assert_eq!(got.len(), before + 1, "{context}: error position");
            for (index, item) in got.iter().take(before).enumerate() {
                assert_eq!(item.as_ref().ok(), Some(&want[index]), "{context}");
            }
            assert!(got[before].is_err(), "{context}");
        }
    }
}

/// R5-44: with prefetch the 16 ranges' pages are fetched concurrently (sequential reads
/// never overlap), each exactly once. Asserted on overlap and read counts, not wall-clock
/// time, which a loaded machine makes unreliable; the elapsed times are printed.
#[test]
fn prefetch_scans_sixteen_ranges_faster_than_sequential() {
    let (cluster, ts) = sixteen_ranges();
    cluster.set_read_delay(Duration::from_millis(25));
    for dir in DIRS {
        let timed = |prefetch: usize| {
            let client = cluster.client();
            let reader = routed(&client, ts, 1 << 16).with_prefetch(prefetch);
            let begun = Instant::now();
            let rows = raw_rows(&reader, dir);
            (begun.elapsed(), rows, client.reads(), client.max_in_flight())
        };
        let (sequential, want, seq_reads, seq_overlap) = timed(0);
        let (parallel, got, par_reads, par_overlap) = timed(4);
        eprintln!("{dir:?}: prefetch {parallel:?} vs sequential {sequential:?}");
        assert_eq!(want.len(), 16, "{dir:?}");
        assert_eq!(got, want, "{dir:?}");
        assert_eq!((seq_reads, seq_overlap), (16, 1), "{dir:?}: sequential");
        assert_eq!(par_reads, 16, "{dir:?}: every range read exactly once");
        assert!(par_overlap >= 2, "{dir:?}: prefetched reads never overlapped");
    }
}

#[test]
fn prefetch_stays_within_its_memory_bound() {
    const K: u64 = 4;
    let (cluster, ts) = sixteen_ranges();
    cluster.set_read_delay(Duration::from_millis(2));
    for dir in DIRS {
        let client = cluster.client();
        let reader = routed(&client, ts, 1 << 16).with_prefetch(K as usize);
        let scan = reader
            .range(
                keyspace(RangeSpace::Rows),
                Bound::Unbounded,
                Bound::Unbounded,
                dir,
            )
            .unwrap();
        let mut consumed = 0;
        for item in scan {
            item.unwrap();
            consumed += 1;
            // One row per range: pages fetched and not fully consumed are the current one plus
            // at most K prefetched.
            thread::sleep(Duration::from_millis(3));
            let fetched = client.reads();
            assert!(
                fetched <= consumed + K,
                "{dir:?}: {fetched} pages fetched after {consumed} consumed"
            );
        }
        assert_eq!(consumed, 16, "{dir:?}");
        assert_eq!(client.reads(), 16, "{dir:?}: no wasted reads");
        let most = client.max_in_flight();
        assert!((2..=K + 1).contains(&most), "{dir:?}: {most} reads at once");
    }
}

#[test]
fn dropping_a_scan_stops_its_prefetch() {
    const K: u64 = 4;
    let (cluster, ts) = sixteen_ranges();
    cluster.set_read_delay(Duration::from_millis(20));
    for dir in DIRS {
        let client = cluster.client();
        let reader = routed(&client, ts, 1 << 16).with_prefetch(K as usize);
        let mut scan = reader
            .range(
                keyspace(RangeSpace::Rows),
                Bound::Unbounded,
                Bound::Unbounded,
                dir,
            )
            .unwrap();
        assert!(scan.next().unwrap().is_ok());
        drop(scan);
        // In-flight prefetches finish their one call; nothing new starts.
        thread::sleep(Duration::from_millis(150));
        let settled = client.reads();
        assert!(settled <= 1 + K, "{dir:?}: {settled} reads after drop");
        assert_eq!(client.in_flight(), 0, "{dir:?}");
        thread::sleep(Duration::from_millis(100));
        assert_eq!(client.reads(), settled, "{dir:?}: reads kept growing");
    }
}

/// 60 rows of table 1 in the single user range; returns the read timestamp.
fn sixty_rows(cluster: &Arc<FakeCluster>) -> Timestamp {
    let writer = cluster.client();
    let desc = writer.descriptor_for(&first_user_key()).unwrap();
    let writes = (0..60)
        .map(|n| RangeWrite {
            space: RangeSpace::Rows,
            key: key(1, n),
            value: Some(vec![n as u8; 10]),
        })
        .collect();
    writer
        .commit(desc.id, desc.epoch, ClusterTxnId(1), writes, None, None)
        .unwrap();
    writer.oracle().next(1).unwrap()
}

#[test]
fn split_during_scan_neither_repeats_nor_skips() {
    for (dir, at) in [(Direction::Forward, 40), (Direction::Reverse, 20)] {
        let cluster = FakeCluster::new(&[]);
        let ts = sixty_rows(&cluster);
        let client = cluster.client();
        let reader = routed(&client, ts, 40);
        let all = (Bound::Unbounded, Bound::Unbounded);
        let want = versions(&*cluster.kv(), RangeSpace::Rows, &all.0, &all.1, dir, ts).unwrap();
        assert_eq!(want.len(), 60);
        let mut scan = mvcc::VersionedRange::new(
            &reader,
            keyspace(RangeSpace::Rows),
            Bound::Unbounded,
            Bound::Unbounded,
            dir,
            Ts(ts.0),
        )
        .unwrap();
        let mut got: Vec<_> = scan.by_ref().take(20).map(Result::unwrap).collect();
        // Split the range being read, in its unread part.
        cluster.split(&range_key(RangeSpace::Rows, &key(1, at)));
        got.extend(scan.map(Result::unwrap));
        assert_eq!(got, want, "{dir:?}");
        assert!(
            client.stale_epochs() >= 1,
            "{dir:?}: route cache was not stale"
        );
    }
}

#[test]
fn transient_errors_retry_then_fail() {
    let cluster = FakeCluster::new(&[range_key(RangeSpace::Rows, &key(1, 30))]);
    // Rows 0..30 in range 2, 30..60 in range 3 (one commit per range owner).
    let ts = commit_rows(&cluster, 0..60);
    let client = cluster.client();
    let all = (Bound::Unbounded, Bound::Unbounded);
    for dir in [Direction::Forward, Direction::Reverse] {
        cluster.fail_next(RangeId(2), RangeError::NotLeader { hint: None });
        cluster.fail_next(RangeId(3), RangeError::Unavailable("busy".into()));
        let got = versions(
            &routed(&client, ts, 64),
            RangeSpace::Rows,
            &all.0,
            &all.1,
            dir,
            ts,
        );
        let want = versions(&*cluster.kv(), RangeSpace::Rows, &all.0, &all.1, dir, ts);
        assert_eq!(got, want, "{dir:?}");
    }
    cluster.fail_next(RangeId(3), RangeError::NotLeader { hint: Some(2) });
    let row = key(1, 45);
    let rows = keyspace(RangeSpace::Rows);
    let reader = routed(&client, ts, 64);
    assert_eq!(
        mvcc::read_at(&reader, rows, &row, Ts(ts.0)).unwrap(),
        Some(vec![45; 10])
    );
    for _ in 0..20 {
        cluster.fail_next(RangeId(2), RangeError::Unavailable("down".into()));
    }
    assert!(
        versions(
            &reader,
            RangeSpace::Rows,
            &all.0,
            &all.1,
            Direction::Forward,
            ts
        )
        .is_err()
    );
    cluster.fail_next(RangeId(3), RangeError::Corrupt("bad page".into()));
    assert!(mvcc::read_at(&reader, rows, &row, Ts(ts.0)).is_err());
}

fn read_all(desc: &crate::storage::range_api::RangeDescriptor, ts: Timestamp) -> ReadRequest {
    ReadRequest {
        range: desc.id,
        epoch: desc.epoch,
        space: RangeSpace::Rows,
        start: Bound::Unbounded,
        end: Bound::Unbounded,
        dir: ReadDirection::Forward,
        ts,
        page_bytes: 1 << 20,
    }
}

#[test]
fn fake_cluster_timestamps_epochs_and_locks() {
    let cluster = FakeCluster::new(&[]);
    let client = cluster.client();
    let desc = client.descriptor_for(&first_user_key()).unwrap();
    let put = |n: u64| RangeWrite {
        space: RangeSpace::Rows,
        key: key(1, n),
        value: Some(vec![n as u8]),
    };
    let mut last = Timestamp(0);
    for n in 0..5 {
        let commit = client
            .commit(
                desc.id,
                desc.epoch,
                ClusterTxnId(1),
                vec![put(n)],
                Some(10 + n),
                None,
            )
            .unwrap();
        assert!(commit > last);
        assert_eq!(cluster.marker(10 + n), Some(commit));
        last = commit;
    }
    assert_eq!(cluster.commits(desc.id), 5);
    cluster.fail_oracle(2);
    assert!(matches!(
        client.oracle().next(1),
        Err(RangeError::Unavailable(_))
    ));
    let failed = client.commit(
        desc.id,
        desc.epoch,
        ClusterTxnId(1),
        vec![put(0)],
        None,
        None,
    );
    assert!(matches!(failed, Err(RangeError::Unavailable(_))));
    assert!(client.oracle().next(1).unwrap() > last);
    // O4: a commit after a read at T commits above T.
    let future = Timestamp(client.oracle().next(1).unwrap().0 + 50);
    let page = client.read(read_all(&desc, future)).unwrap();
    assert_eq!(
        (page.pairs.len(), page.resume, page.range_end),
        (5, None, None)
    );
    let commit = client
        .commit(
            desc.id,
            desc.epoch,
            ClusterTxnId(1),
            vec![put(5)],
            None,
            None,
        )
        .unwrap();
    assert!(commit > future);

    // An old epoch, or a key outside the descriptor, is StaleEpoch with the current descriptor.
    cluster.split(&range_key(RangeSpace::Rows, &key(1, 3)));
    let left = cluster.descriptors()[1].clone();
    assert_eq!((left.id, left.epoch), (desc.id, Epoch(desc.epoch.0 + 1)));
    let stale = RangeError::StaleEpoch {
        descriptor: left.clone(),
    };
    assert_eq!(client.read(read_all(&desc, commit)), Err(stale.clone()));
    let old = client.commit(
        desc.id,
        desc.epoch,
        ClusterTxnId(1),
        vec![put(1)],
        None,
        None,
    );
    assert_eq!(old, Err(stale.clone()));
    let outside = client.commit(
        left.id,
        left.epoch,
        ClusterTxnId(1),
        vec![put(4)],
        None,
        None,
    );
    assert_eq!(outside, Err(stale));
    assert_eq!(client.lease(LeaseKind::Xid, 0, 3), Ok(1..4));
    assert_eq!(client.lease(LeaseKind::Xid, 0, 2), Ok(4..6));
    assert_eq!(client.lease(LeaseKind::ObjectId, 0, 1), Ok(1..2));

    // A second txn locking the same key waits until the first releases.
    let lock = |txn: u64, timeout_ms: Option<u64>| LockRequest {
        range: left.id,
        epoch: left.epoch,
        txn: ClusterTxnId(txn),
        priority: Timestamp(0),
        target: LockTarget::Key {
            space: RangeSpace::Rows,
            key: key(1, 1),
        },
        mode: RangeLockMode::Exclusive,
        timeout_ms,
    };
    let grant = client.lock(lock(1, None)).unwrap();
    assert!(matches!(grant.latest, Some((_, Some(ref value))) if value == &vec![1]));
    assert_eq!(client.lock(lock(2, Some(20))), Err(RangeError::LockTimeout));
    let (sent, received) = mpsc::channel();
    let waiter = {
        let client = client.clone();
        let request = lock(2, Some(10_000));
        thread::spawn(move || sent.send(client.lock(request)).unwrap())
    };
    assert!(received.recv_timeout(Duration::from_millis(100)).is_err());
    // The cluster keeps serving while a lock waits.
    assert!(client.read(read_all(&left, commit)).is_ok());
    client.heartbeat(left.id, ClusterTxnId(1)).unwrap();
    client.release(left.id, ClusterTxnId(1)).unwrap();
    let granted = received.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(granted.is_ok());
    waiter.join().unwrap();
    assert_eq!(
        (cluster.heartbeats(left.id), cluster.releases(left.id)),
        (1, 1)
    );
    let sentinel = |txn: u64| LockRequest {
        target: LockTarget::TableSentinel { table: 1 },
        mode: RangeLockMode::Shared,
        ..lock(txn, Some(20))
    };
    assert_eq!(client.lock(sentinel(3)).map(|g| g.latest), Ok(None));
    assert_eq!(client.lock(sentinel(4)).map(|g| g.latest), Ok(None));
    assert!(matches!(
        client.finalize(
            left.id,
            ClusterTxnId(1),
            crate::storage::range_api::Outcome::Abort
        ),
        Err(RangeError::Unavailable(_))
    ));
}
