use super::*;
use crate::storage::engine::meta::REPLICATED;
use crate::storage::range::engine::{EngineConfig, RangeEngine};
use crate::storage::range::keys::contains_raw;
use crate::storage::range::store::{
    RangeApplyOutcome,
    tests::{Dir, SPLIT_TABLE, descriptors, pos, row, rows, two_ranges},
};
use crate::storage::range_api::{
    ClusterTxnId, LockRequest, LockTarget, RangeError, RangeLockMode, RangeService, RangeWrite,
    ReadDirection, ReadRequest, ReplicaDesc, Timestamp, TsOracle, range_key,
};
use crate::storage::replicated::{BatchSink, ReplicateError};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

const P: RangeId = RangeId(2);
const R: RangeId = RangeId(5);
const LEFT_T: u64 = SPLIT_TABLE;
const RIGHT_T: u64 = SPLIT_TABLE + 2;

fn at_key() -> Vec<u8> {
    range_key(RangeSpace::Rows, &(SPLIT_TABLE + 1).to_be_bytes())
}
fn meta(id: RangeId, sub: RangeLocalKey) -> Vec<u8> {
    range_local_key(id, &sub)
}
fn get(store: &RangeStore, key: &[u8]) -> Option<Vec<u8>> {
    store.kv().get(Keyspace::Meta, key).unwrap()
}
fn one(ks: Keyspace, key: Vec<u8>) -> Vec<u8> {
    let mut batch = WriteBatch::default();
    batch.put(ks, key, b"v".to_vec());
    batch.encode()
}

/// Range 2 = [Rows table 10, MAX) at index 1 holding rows of tables 10 and 12, index keys,
/// two markers, features, last_commit_ts, gc_watermark and a home record.
fn parent(dir: &Dir) -> RangeStore {
    let store = two_ranges(dir);
    let mut batch = WriteBatch::default();
    for n in 1..=3 {
        batch.put(Keyspace::Rows, row(LEFT_T, n, 1), b"l".to_vec());
        batch.put(Keyspace::Rows, row(RIGHT_T, n, 1), b"r".to_vec());
        batch.put(Keyspace::Index, row(SPLIT_TABLE + 5, n, 1), b"i".to_vec());
    }
    for (sub, value) in [
        (RangeLocalKey::Marker(4), 9u64.to_be_bytes().to_vec()),
        (RangeLocalKey::Marker(6), 11u64.to_be_bytes().to_vec()),
        (RangeLocalKey::Features, REPLICATED.to_be_bytes().to_vec()),
        (RangeLocalKey::LastCommitTs, 12u64.to_be_bytes().to_vec()),
        (RangeLocalKey::GcWatermark, 3u64.to_be_bytes().to_vec()),
        (RangeLocalKey::Home(7), b"home".to_vec()),
    ] {
        batch.put(Keyspace::Meta, meta(P, sub), value);
    }
    assert_eq!(
        store.apply(P, &batch.encode(), pos(1, 1)).unwrap(),
        RangeApplyOutcome::Applied
    );
    store
}

/// Non-Meta rows of an export, installed into a fresh store.
fn exported(store: &RangeStore, dir: &Dir, range: RangeId, name: &str) -> Vec<(Keyspace, Vec<u8>)> {
    store.export(range, &dir.sub(name)).unwrap();
    let dest = RangeStore::create(dir.sub(&format!("{name}-db")), [7; 16]).unwrap();
    dest.install(range, &dir.sub(name)).unwrap();
    data(&dest)
}
fn data(store: &RangeStore) -> Vec<(Keyspace, Vec<u8>)> {
    Keyspace::ALL
        .into_iter()
        .filter(|ks| *ks != Keyspace::Meta)
        .flat_map(|ks| rows(store, ks).into_iter().map(move |(k, _)| (ks, k)))
        .collect()
}

#[test]
fn split_applies_both_halves_independently_and_survives_reopen() {
    let dir = Dir::new("range-split");
    let store = parent(&dir);
    let before = data(&store);
    let whole = exported(&store, &dir, P, "whole");
    let old = store.descriptor(P).unwrap().unwrap();
    let outcome = store.split(P, old.epoch, &at_key(), R, pos(2, 1)).unwrap();
    let SplitOutcome::Applied { left, right } = outcome else {
        panic!("{outcome:?}");
    };
    assert_eq!(
        (left.id, left.epoch, &left.start),
        (P, Epoch(2), &old.start)
    );
    assert_eq!(left.end, Some(at_key()));
    assert_eq!(
        (right.id, right.epoch, right.start.clone()),
        (R, Epoch(2), at_key())
    );
    assert_eq!((&right.end, &right.replicas), (&old.end, &old.replicas));
    assert_eq!(
        store.split(P, old.epoch, &at_key(), R, pos(2, 1)).unwrap(),
        SplitOutcome::Skipped
    );
    // Data keys did not move; each half's export holds exactly its keys.
    assert_eq!(data(&store), before);
    for (range, half) in [(P, &left), (R, &right)] {
        let expect: Vec<_> = whole
            .iter()
            .filter(|(ks, key)| contains_raw(half, *ks, key).unwrap())
            .cloned()
            .collect();
        assert!(!expect.is_empty());
        assert_eq!(
            exported(&store, &dir, range, &format!("half-{}", range.0)),
            expect
        );
    }
    // Copied local state; home records stay on the parent.
    for sub in [
        RangeLocalKey::Marker(4),
        RangeLocalKey::Marker(6),
        RangeLocalKey::Features,
        RangeLocalKey::LastCommitTs,
        RangeLocalKey::GcWatermark,
    ] {
        assert!(get(&store, &meta(P, sub)).is_some());
        assert_eq!(
            get(&store, &meta(R, sub)),
            get(&store, &meta(P, sub)),
            "{sub:?}"
        );
    }
    assert!(get(&store, &meta(R, RangeLocalKey::Home(7))).is_none());
    assert!(get(&store, &meta(P, RangeLocalKey::Home(7))).is_some());
    // Each half applies on its own: the right's first index is the split index + 1.
    let left_row = || one(Keyspace::Rows, row(LEFT_T, 9, 2));
    let right_row = || one(Keyspace::Rows, row(RIGHT_T, 9, 2));
    let applied = RangeApplyOutcome::Applied;
    let refused = RangeApplyOutcome::Refused;
    assert_eq!(store.apply(P, &left_row(), pos(3, 1)).unwrap(), applied);
    assert_eq!(store.apply(R, &right_row(), pos(3, 1)).unwrap(), applied);
    assert_eq!(store.apply(P, &right_row(), pos(4, 1)).unwrap(), refused);
    assert_eq!(store.apply(R, &left_row(), pos(4, 1)).unwrap(), refused);
    let index = || one(Keyspace::Index, row(SPLIT_TABLE + 5, 9, 2));
    assert_eq!(store.apply(P, &index(), pos(5, 1)).unwrap(), refused);
    assert_eq!(store.apply(R, &index(), pos(5, 1)).unwrap(), applied);
    // A normal batch never writes or deletes the lineage.
    let child = meta(P, RangeLocalKey::Child(R.0));
    let mut put = WriteBatch::default();
    put.put(Keyspace::Meta, child.clone(), right.encode().unwrap());
    let mut delete = WriteBatch::default();
    delete.delete(Keyspace::Meta, child.clone());
    let mut wipe = WriteBatch::default();
    let (lo, hi) = local_interval(P);
    wipe.delete_range(Keyspace::Meta, lo, hi);
    let (lo, hi) = child_interval(P);
    let mut wipe_children = WriteBatch::default();
    wipe_children.delete_range(Keyspace::Meta, lo, hi);
    for (n, batch) in [put, delete, wipe, wipe_children].into_iter().enumerate() {
        let at = pos(6 + n as u64, 1);
        assert_eq!(store.apply(P, &batch.encode(), at).unwrap(), refused);
    }
    assert!(get(&store, &child).is_some());
    assert_eq!(
        store.split(P, old.epoch, &at_key(), R, pos(2, 1)).unwrap(),
        SplitOutcome::Skipped
    );
    let gap = store.split(P, left.epoch, &at_key(), RangeId(8), pos(20, 1));
    assert!(matches!(gap, Err(CoreError::Corrupt(_))));
    drop(store);
    let store = RangeStore::open(dir.sub("db")).unwrap();
    assert_eq!(store.descriptor(P).unwrap(), Some(left.clone()));
    assert_eq!(store.descriptor(R).unwrap(), Some(right.clone()));
    assert_eq!(store.children(P).unwrap(), vec![right]);
    assert_eq!(store.children(R).unwrap(), vec![]);
    assert_eq!(store.applied(P).unwrap(), Some(pos(9, 1)));
    assert_eq!(store.applied(R).unwrap(), Some(pos(5, 1)));
    assert_eq!(store.ranges().unwrap(), vec![RangeId(1), P, R]);
}

#[test]
fn refused_splits_only_advance_the_parent() {
    let partial = range_key(RangeSpace::Rows, &[0, 0, 0, 0, 0, 0, 1]);
    let mut huge = RIGHT_T.to_be_bytes().to_vec();
    huge.resize(70_000, 0);
    let cases: Vec<(Epoch, Vec<u8>, RangeId, &str)> = vec![
        (Epoch(2), at_key(), R, "stale epoch"),
        (
            Epoch(1),
            range_key(RangeSpace::Rows, &LEFT_T.to_be_bytes()),
            R,
            "strictly inside",
        ),
        (
            Epoch(1),
            range_key(RangeSpace::Rows, &5u64.to_be_bytes()),
            R,
            "strictly inside",
        ),
        (Epoch(1), vec![0x85, 1], R, "not a range key"),
        (Epoch(1), partial, R, "no table or index id"),
        // Review S1: a bound over 64 KiB cannot be encoded.
        (
            Epoch(1),
            range_key(RangeSpace::Rows, &huge),
            R,
            "split descriptor invalid",
        ),
        (Epoch(1), at_key(), RangeId(0), "invalid right"),
        (Epoch(1), at_key(), SYSTEM_RANGE, "invalid right"),
        (Epoch(1), at_key(), RangeId(u64::MAX), "invalid right"),
        (Epoch(1), at_key(), P, "invalid right"),
    ];
    for (epoch, key, right, reason) in cases {
        let dir = Dir::new("range-split-refused");
        let store = parent(&dir);
        store
            .apply(P, &WriteBatch::default().encode(), pos(2, 1))
            .unwrap();
        let old = store.descriptor(P).unwrap();
        let ranges = store.ranges().unwrap();
        match store.split(P, epoch, &key, right, pos(3, 1)).unwrap() {
            SplitOutcome::Refused(why) => assert!(why.contains(reason), "{why} vs {reason}"),
            other => panic!("{reason}: {other:?}"),
        }
        assert_eq!(store.descriptor(P).unwrap(), old, "{reason}");
        assert_eq!(store.applied(P).unwrap(), Some(pos(3, 1)));
        assert_eq!(store.children(P).unwrap(), vec![]);
        assert_eq!(store.ranges().unwrap(), ranges);
        assert_eq!(
            store.split(P, epoch, &key, right, pos(3, 1)).unwrap(),
            SplitOutcome::Skipped
        );
    }
}

#[test]
fn system_range_splits_only_at_or_above_the_first_user_key() {
    let dir = Dir::new("range-split-system");
    let store = two_ranges(&dir);
    let kernel = range_key(RangeSpace::Kernel, b"k");
    let below = store
        .split(SYSTEM_RANGE, Epoch(1), &kernel, R, pos(1, 1))
        .unwrap();
    assert!(matches!(below, SplitOutcome::Refused(why) if why.contains("system key space")));
    let at = store
        .split(SYSTEM_RANGE, Epoch(1), &first_user_key(), R, pos(2, 1))
        .unwrap();
    let SplitOutcome::Applied { left, right } = at else {
        panic!("{at:?}");
    };
    assert_eq!(left.end, Some(first_user_key()));
    let end = range_key(RangeSpace::Rows, &SPLIT_TABLE.to_be_bytes());
    assert_eq!((right.start, right.end), (first_user_key(), Some(end)));
    assert_eq!(store.children(SYSTEM_RANGE).unwrap().len(), 1);
}

/// Review S2: a right id already stored is local state the parent's log does not replicate;
/// the apply fails (the replica halts) instead of refusing on some replicas only.
#[test]
fn a_stored_right_id_fails_the_apply() {
    let dir = Dir::new("range-split-stored-right");
    let store = parent(&dir);
    let mut batch = WriteBatch::default();
    batch.put(Keyspace::Meta, applied_key(RangeId(9)), vec![0; 16]);
    store.kv().write(batch, Durability::Sync).unwrap();
    let before = (store.descriptor(P).unwrap(), store.applied(P).unwrap());
    let error = store
        .split(P, Epoch(1), &at_key(), RangeId(9), pos(2, 1))
        .unwrap_err();
    assert!(
        matches!(&error, CoreError::Corrupt(why) if why.contains("right range 9 is already stored")),
        "{error:?}"
    );
    let after = (store.descriptor(P).unwrap(), store.applied(P).unwrap());
    assert_eq!(after, before);
    assert_eq!(store.children(P).unwrap(), vec![]);
    // A deterministic refusal comes first: a stale entry naming it only advances the parent.
    let stale = store.split(P, Epoch(7), &at_key(), RangeId(9), pos(2, 1));
    assert!(matches!(stale, Ok(SplitOutcome::Refused(_))), "{stale:?}");
}

/// Review S5: the right group opens with the voters only, so a learner would host it outside
/// its configuration.
#[test]
fn a_parent_with_a_learner_refuses_to_split() {
    let dir = Dir::new("range-split-learner");
    let store = RangeStore::create(dir.sub("db"), [7; 16]).unwrap();
    let [system, mut user] = descriptors();
    user.replicas.push(ReplicaDesc {
        node: 2,
        role: ReplicaRole::Learner,
    });
    for d in [&system, &user] {
        store.create_range(d).unwrap();
    }
    match store.split(P, Epoch(1), &at_key(), R, pos(1, 1)).unwrap() {
        SplitOutcome::Refused(why) => assert!(why.contains("learner"), "{why}"),
        other => panic!("{other:?}"),
    }
    assert_eq!(store.descriptor(P).unwrap(), Some(user));
    assert_eq!(store.descriptor(R).unwrap(), None);
}

/// Review S3: a proposer splits only at a whole user key, a table or index id, or a space tag.
#[test]
fn proposers_split_only_at_whole_user_keys() {
    let dir = Dir::new("range-split-key");
    let store = parent(&dir);
    let in_rows = |user: &[u8]| range_key(RangeSpace::Rows, user);
    let stored = user(RIGHT_T, 2);
    let mut inside = stored.clone();
    inside.push(0);
    let mut huge = RIGHT_T.to_be_bytes().to_vec();
    huge.resize(70_000, 0);
    for ok in [
        at_key(),
        range_key(RangeSpace::Index, &[]),
        in_rows(&stored),
        range_key(RangeSpace::Index, &user(SPLIT_TABLE + 5, 2)),
    ] {
        store.check_split_key(P, &ok).unwrap();
    }
    for (bad, why) in [
        // The review's case: u || 0x00 would put u and its versions on different sides.
        (in_rows(&inside), "not a stored user key"),
        (in_rows(&user(RIGHT_T, 9)), "not a stored user key"),
        (in_rows(&[0, 0, 0, 0, 0, 0, 1]), "no table or index id"),
        (in_rows(&LEFT_T.to_be_bytes()), "strictly inside"),
        (in_rows(&huge), "split descriptor invalid"),
    ] {
        match store.check_split_key(P, &bad) {
            Err(CoreError::Storage(e)) => assert!(e.contains(why), "{e} vs {why}"),
            other => panic!("{why}: {other:?}"),
        }
    }
    let kernel = range_key(RangeSpace::Kernel, b"k");
    assert!(store.check_split_key(SYSTEM_RANGE, &kernel).is_err());
    store
        .check_split_key(SYSTEM_RANGE, &first_user_key())
        .unwrap();
    let unknown = store.check_split_key(RangeId(77), &at_key());
    assert!(matches!(unknown, Err(CoreError::Corrupt(_))), "{unknown:?}");
}

/// Review B1: a parent routes a forwarded call to its child by the child's stored descriptor,
/// else by its lineage record of it.
#[test]
fn child_routes_prefer_the_stored_descriptor_then_the_lineage() {
    let dir = Dir::new("range-split-route");
    let store = parent(&dir);
    let SplitOutcome::Applied { right, .. } =
        store.split(P, Epoch(1), &at_key(), R, pos(2, 1)).unwrap()
    else {
        panic!("split refused");
    };
    let kv: &dyn KvRead = store.kv().as_ref();
    assert_eq!(child_route(kv, P, R).unwrap(), Some(right.clone()));
    let at = range_key(RangeSpace::Rows, &(SPLIT_TABLE + 3).to_be_bytes());
    let SplitOutcome::Applied { left: r, .. } = store
        .split(R, Epoch(2), &at, RangeId(6), pos(3, 1))
        .unwrap()
    else {
        panic!("second split refused");
    };
    assert_eq!(child_route(kv, P, R).unwrap(), Some(r));
    let mut gone = WriteBatch::default();
    gone.delete(Keyspace::Meta, meta(R, RangeLocalKey::Descriptor));
    store.kv().write(gone, Durability::Sync).unwrap();
    assert_eq!(child_route(kv, P, R).unwrap(), Some(right));
    assert_eq!(child_route(kv, P, RangeId(77)).unwrap(), None);
}

/// GD-0075 P9-2c: a replica that installs the parent's post-split snapshot learns the child
/// only from the lineage; stored as needing a snapshot, the child applies nothing (also after
/// a reopen) until its own snapshot replaces its interval, stale right-half rows included.
#[test]
fn a_child_learned_from_lineage_waits_for_its_own_snapshot() {
    let (source_dir, lagging_dir) = (Dir::new("range-lineage-a"), Dir::new("range-lineage-b"));
    let (source, lagging) = (parent(&source_dir), parent(&lagging_dir));
    let SplitOutcome::Applied { left, right } =
        source.split(P, Epoch(1), &at_key(), R, pos(2, 1)).unwrap()
    else {
        panic!("split refused");
    };
    let mut delete = WriteBatch::default();
    delete.delete(Keyspace::Rows, row(RIGHT_T, 1, 1));
    let applied = RangeApplyOutcome::Applied;
    assert_eq!(
        source.apply(R, &delete.encode(), pos(3, 1)).unwrap(),
        applied
    );
    let left_row = one(Keyspace::Rows, row(LEFT_T, 9, 1));
    assert_eq!(source.apply(P, &left_row, pos(3, 1)).unwrap(), applied);
    source.export(P, &source_dir.sub("parent")).unwrap();
    assert_eq!(
        lagging.install(P, &source_dir.sub("parent")).unwrap(),
        pos(3, 1)
    );
    assert_eq!(lagging.descriptor(P).unwrap(), Some(left));
    assert_eq!(lagging.children(P).unwrap(), vec![right.clone()]);
    assert_eq!(lagging.descriptor(R).unwrap(), None);
    let stale = row(RIGHT_T, 1, 1);
    assert!(
        rows(&lagging, Keyspace::Rows)
            .iter()
            .any(|(k, _)| *k == stale)
    );
    assert!(lagging.create_needs_snapshot(&right).unwrap());
    assert!(!lagging.create_needs_snapshot(&right).unwrap());
    assert!(
        !source.create_needs_snapshot(&right).unwrap(),
        "stored by the split"
    );
    let refuses = |store: &RangeStore| {
        assert_eq!(store.descriptor(R).unwrap(), Some(right.clone()));
        assert_eq!(store.applied(R).unwrap(), None);
        assert!(get(store, &meta(R, RangeLocalKey::Install)).is_some());
        let put = one(Keyspace::Rows, row(RIGHT_T, 7, 1));
        assert!(matches!(
            store.apply(R, &put, pos(1, 1)),
            Err(CoreError::Corrupt(_))
        ));
    };
    refuses(&lagging);
    drop(lagging);
    let lagging = RangeStore::open(lagging_dir.sub("db")).unwrap();
    refuses(&lagging);
    source.export(R, &source_dir.sub("child")).unwrap();
    assert_eq!(
        lagging.install(R, &source_dir.sub("child")).unwrap(),
        pos(3, 1)
    );
    assert_eq!(get(&lagging, &meta(R, RangeLocalKey::Install)), None);
    assert_eq!(data(&lagging), data(&source));
    let put = one(Keyspace::Rows, row(RIGHT_T, 8, 1));
    assert_eq!(lagging.apply(R, &put, pos(4, 1)).unwrap(), applied);
}

struct NoSink;
impl BatchSink for NoSink {
    fn replicate(&self, _: Vec<u8>) -> Result<(), ReplicateError> {
        Err(ReplicateError::NotLeader)
    }
}
struct Oracle(AtomicU64);
impl TsOracle for Oracle {
    fn next(&self, n: u32) -> Result<Timestamp, RangeError> {
        Ok(Timestamp(self.0.fetch_add(u64::from(n), Ordering::SeqCst)))
    }
}

fn engine(store: &RangeStore, descriptor: RangeDescriptor) -> Arc<RangeEngine> {
    let oracle = Arc::new(Oracle(AtomicU64::new(1 << 20)));
    RangeEngine::open(
        store,
        descriptor,
        Arc::new(NoSink),
        oracle,
        EngineConfig::default(),
        1,
    )
    .unwrap()
}
fn read(engine: &RangeEngine, epoch: Epoch, table: u64) -> Result<usize, RangeError> {
    let start = Bound::Included(table.to_be_bytes().to_vec());
    let end = Bound::Excluded((table + 1).to_be_bytes().to_vec());
    scan(engine, epoch, (start, end), ReadDirection::Forward)
}
type Span = (Bound<Vec<u8>>, Bound<Vec<u8>>);
fn scan(
    engine: &RangeEngine,
    epoch: Epoch,
    (start, end): Span,
    dir: ReadDirection,
) -> Result<usize, RangeError> {
    engine
        .read(ReadRequest {
            range: P,
            epoch,
            space: RangeSpace::Rows,
            start,
            end,
            dir,
            ts: Timestamp(1 << 20),
            page_bytes: 1 << 20,
        })
        .map(|page| page.pairs.len())
}
fn lock(engine: &RangeEngine, target: LockTarget) -> Result<(), RangeError> {
    engine
        .lock(LockRequest {
            range: P,
            epoch: Epoch(1),
            txn: crate::storage::range_api::ClusterTxnId::new(1, 1),
            priority: Timestamp(0),
            target,
            mode: RangeLockMode::Exclusive,
            timeout_ms: Some(0),
        })
        .map(drop)
}

#[test]
fn engine_reads_and_locks_after_a_split_are_stale() {
    let dir = Dir::new("range-split-engine");
    let store = parent(&dir);
    let old = engine(&store, store.descriptor(P).unwrap().unwrap());
    assert_eq!(read(&old, Epoch(1), RIGHT_T), Ok(3));
    let SplitOutcome::Applied { left, right } =
        store.split(P, Epoch(1), &at_key(), R, pos(2, 1)).unwrap()
    else {
        panic!("split refused");
    };
    let stale = |descriptor: &RangeDescriptor| -> Result<(), RangeError> {
        Err(RangeError::StaleEpoch {
            descriptor: descriptor.clone(),
        })
    };
    assert_eq!(read(&old, Epoch(1), RIGHT_T), stale(&right).map(|()| 0));
    assert_eq!(read(&old, Epoch(1), LEFT_T), stale(&left).map(|()| 0));
    // Review S4: a page is named by the key the gateway routes it by, its start forward and
    // its end in reverse (an unbounded end: the space's end).
    let table = |t: u64| t.to_be_bytes().to_vec();
    let across = || {
        (
            Bound::Included(table(LEFT_T)),
            Bound::Excluded(table(RIGHT_T + 1)),
        )
    };
    let to_end = || (Bound::Included(table(LEFT_T)), Bound::Unbounded);
    let below = || (Bound::Unbounded, Bound::Included(user(LEFT_T, 3)));
    for (span, dir, holder) in [
        (across(), ReadDirection::Forward, &left),
        (across(), ReadDirection::Reverse, &right),
        (to_end(), ReadDirection::Forward, &left),
        (to_end(), ReadDirection::Reverse, &right),
        (below(), ReadDirection::Forward, &left),
        (below(), ReadDirection::Reverse, &left),
    ] {
        let got = scan(&old, Epoch(1), span.clone(), dir);
        assert_eq!(got, stale(holder).map(|()| 0), "{span:?} {dir:?}");
    }
    let mut key = RIGHT_T.to_be_bytes().to_vec();
    key.extend_from_slice(&1u64.to_be_bytes());
    let row_lock = LockTarget::Key {
        space: RangeSpace::Rows,
        key,
    };
    assert_eq!(lock(&old, row_lock), stale(&right));
    assert_eq!(
        lock(&old, LockTarget::TableSentinel { table: LEFT_T }),
        stale(&left)
    );
    old.close();
    let new = engine(&store, left.clone());
    assert_eq!(read(&new, left.epoch, LEFT_T), Ok(3));
}

#[test]
fn newer_epochs_wait_for_the_reopen_and_older_ones_name_the_holder() {
    let dir = Dir::new("range-split-epochs");
    let store = parent(&dir);
    let old = engine(&store, store.descriptor(P).unwrap().unwrap());
    let SplitOutcome::Applied { left, .. } =
        store.split(P, Epoch(1), &at_key(), R, pos(2, 1)).unwrap()
    else {
        panic!("split refused");
    };
    // R5-81: a client already at the new epoch reached the engine before its reopen: retry.
    let reopening = Err(RangeError::Unavailable(
        "range engine reopening after a descriptor change".into(),
    ));
    assert_eq!(read(&old, left.epoch, LEFT_T), reopening);
    // Review N4: an epoch this node has not applied yet is retried, never answered with an
    // older descriptor.
    let ahead = Err(RangeError::Unavailable(
        "range 2 has not applied epoch 9 here yet".into(),
    ));
    assert_eq!(read(&old, Epoch(9), LEFT_T), ahead);
    // R = [11, MAX) splits again at table 13 into R = [11, 13) and S = [13, MAX).
    const S: RangeId = RangeId(6);
    let at = range_key(RangeSpace::Rows, &(SPLIT_TABLE + 3).to_be_bytes());
    let SplitOutcome::Applied { left: r, right: s } =
        store.split(R, Epoch(2), &at, S, pos(3, 1)).unwrap()
    else {
        panic!("second split refused");
    };
    let stale = |descriptor: &RangeDescriptor| -> Result<usize, RangeError> {
        Err(RangeError::StaleEpoch {
            descriptor: descriptor.clone(),
        })
    };
    // An older epoch names the range holding the key, two lineage levels down for S.
    let new = engine(&store, left.clone());
    let far = SPLIT_TABLE + 4;
    assert_eq!(read(&new, Epoch(1), far), stale(&s));
    assert_eq!(read(&new, Epoch(1), RIGHT_T), stale(&r));
    assert_eq!(read(&new, Epoch(1), LEFT_T), stale(&left));
    assert_eq!(read(&new, left.epoch, LEFT_T), Ok(3));
    assert_eq!(read(&new, Epoch(9), LEFT_T), ahead);
    // So does the old engine at its own epoch, and a stale commit by its first write.
    assert_eq!(read(&old, Epoch(1), far), stale(&s));
    let write = RangeWrite {
        space: RangeSpace::Rows,
        key: user(far, 1),
        value: Some(b"x".to_vec()),
    };
    let commit = new.commit(Epoch(1), ClusterTxnId::new(1, 9), vec![write], None, None);
    assert_eq!(commit.map(drop), stale(&s).map(drop));
    old.close();
    new.close();
}

use crate::storage::engine::{
    hlc::Ts,
    locks::{LockKey, LockMode},
};
use crate::storage::range::engine::prepared::Prepared;

fn user(table: u64, n: u64) -> Vec<u8> {
    [table.to_be_bytes(), n.to_be_bytes()].concat()
}
fn row_lock(table: u64, pk: &[u8]) -> (LockKey, LockMode) {
    let key = LockKey::Row {
        table,
        pk: pk.to_vec(),
    };
    (key, LockMode::Exclusive)
}
fn unique(index: u64, n: u64) -> (LockKey, LockMode) {
    let key = LockKey::Unique {
        index,
        tuple: n.to_be_bytes().to_vec(),
    };
    (key, LockMode::Exclusive)
}
fn record(writes: Vec<(Keyspace, Vec<u8>, bool)>, locks: Vec<(LockKey, LockMode)>) -> Prepared {
    Prepared {
        home: P,
        at: Ts(77),
        writes: writes
            .into_iter()
            .map(|(ks, key, put)| ((ks, key), put.then(|| b"w".to_vec())))
            .collect(),
        locks,
        children: Vec::new(),
    }
}

#[test]
fn split_divides_prepared_records_by_key() {
    const INDEX: u64 = SPLIT_TABLE + 5;
    let sentinel = |table| {
        (
            LockKey::Row {
                table,
                pk: Vec::new(),
            },
            LockMode::Shared,
        )
    };
    let left_only = record(
        vec![(Keyspace::Rows, user(LEFT_T, 1), true)],
        vec![row_lock(LEFT_T, &1u64.to_be_bytes()), sentinel(LEFT_T)],
    );
    let right_only = record(
        vec![
            (Keyspace::Rows, user(RIGHT_T, 1), true),
            (Keyspace::Index, user(INDEX, 1), true),
        ],
        vec![row_lock(RIGHT_T, &1u64.to_be_bytes()), unique(INDEX, 1)],
    );
    let spanning = |left: bool, right: bool, children: Vec<RangeId>| {
        let mut writes = Vec::new();
        let mut locks = Vec::new();
        if left {
            writes.push((Keyspace::Rows, user(LEFT_T, 2), true));
            // A sentinel has no key position: it stays with the parent (here a right table's).
            locks.extend([row_lock(LEFT_T, &2u64.to_be_bytes()), sentinel(RIGHT_T)]);
        }
        if right {
            writes.push((Keyspace::Rows, user(RIGHT_T, 2), false));
            writes.push((Keyspace::Index, user(INDEX, 2), true));
            locks.extend([row_lock(RIGHT_T, &2u64.to_be_bytes()), unique(INDEX, 2)]);
        }
        Prepared {
            children,
            ..record(writes, locks)
        }
    };
    let dir = Dir::new("range-split-prepared");
    let store = parent(&dir);
    let mut batch = WriteBatch::default();
    for (txn, r) in [
        (1, &left_only),
        (2, &right_only),
        (3, &spanning(true, true, vec![])),
    ] {
        let key = meta(P, RangeLocalKey::Prepared(txn));
        batch.put(Keyspace::Meta, key, r.encode().unwrap());
    }
    store.apply(P, &batch.encode(), pos(2, 1)).unwrap();
    let old = store.descriptor(P).unwrap().unwrap();
    let outcome = store.split(P, old.epoch, &at_key(), R, pos(3, 1)).unwrap();
    assert!(
        matches!(outcome, SplitOutcome::Applied { .. }),
        "{outcome:?}"
    );
    // Pinned v2: [2][home BE8][p BE8][0 writes BE4][0 locks BE4][1 child BE4][R BE8].
    let mut forward_only = vec![2];
    for part in [
        &2u64.to_be_bytes()[..],
        &77u64.to_be_bytes(),
        &[0; 8],
        &[0, 0, 0, 1],
    ] {
        forward_only.extend_from_slice(part);
    }
    forward_only.extend_from_slice(&R.0.to_be_bytes());
    let expected = [
        (1, Some(left_only.encode().unwrap()), None),
        (2, Some(forward_only), Some(right_only.encode().unwrap())),
        (
            3,
            Some(spanning(true, false, vec![R]).encode().unwrap()),
            Some(spanning(false, true, vec![]).encode().unwrap()),
        ),
    ];
    let check = |store: &RangeStore| {
        for (txn, parent_bytes, right_bytes) in &expected {
            let key = |id| meta(id, RangeLocalKey::Prepared(*txn));
            assert_eq!(&get(store, &key(P)), parent_bytes, "parent {txn}");
            assert_eq!(&get(store, &key(R)), right_bytes, "right {txn}");
        }
    };
    check(&store);
    let parent_share = Prepared::decode(expected[2].1.as_ref().unwrap()).unwrap();
    assert_eq!(parent_share, spanning(true, false, vec![R]));
    assert_eq!(expected[2].1.as_ref().unwrap()[0], 2);
    assert_eq!(expected[2].2.as_ref().unwrap()[0], 1);
    drop(store);
    check(&RangeStore::open(dir.sub("db")).unwrap());
}

#[test]
fn prepared_record_layouts_are_pinned() {
    let mut v1 = record(
        vec![(Keyspace::Rows, vec![1, 2], true)],
        vec![row_lock(3, &[4])],
    );
    v1.writes
        .insert((Keyspace::Rows, vec![1, 2]), Some(vec![7]));
    #[rustfmt::skip]
    let v1_bytes: Vec<u8> = [
        &[1][..], &2u64.to_be_bytes(), &77u64.to_be_bytes(),
        &[0, 0, 0, 1], &[0x80], &[0, 0, 0, 2], &[1, 2], &[1], &[0, 0, 0, 1], &[7],
        &[0, 0, 0, 1], &[1], &3u64.to_be_bytes(), &[0, 0, 0, 1], &[4], &[1],
    ]
    .concat();
    assert_eq!(v1.encode().unwrap(), v1_bytes);
    assert_eq!(Prepared::decode(&v1_bytes).unwrap(), v1);
    let v2 = Prepared {
        children: vec![RangeId(9), R],
        ..v1.clone()
    };
    let children = [&[0, 0, 0, 2][..], &9u64.to_be_bytes(), &5u64.to_be_bytes()].concat();
    let v2_bytes = [&[2][..], &v1_bytes[1..], &children].concat();
    assert_eq!(v2.encode().unwrap(), v2_bytes);
    assert_eq!(Prepared::decode(&v2_bytes).unwrap(), v2);
    // Strict: v1 with a children list, v2 without or with a bad child, unknown versions.
    let body = &v1_bytes[1..];
    let bad = |version: u8, tail: Vec<u8>| [&[version][..], body, &tail].concat();
    let child = |id: u64| id.to_be_bytes().to_vec();
    for damaged in [
        bad(1, children.clone()),
        bad(2, vec![]),
        bad(2, vec![0, 0, 0, 0]),
        bad(2, [vec![0, 0, 0, 2], child(5), child(5)].concat()),
        bad(2, [vec![0, 0, 0, 1], child(0)].concat()),
        bad(2, [vec![0, 0, 0, 1], child(1)].concat()),
        bad(2, [vec![0, 0, 0, 1], child(u64::MAX)].concat()),
        bad(2, [vec![0, 0, 0, 2], child(5)].concat()),
        bad(3, vec![]),
    ] {
        assert!(Prepared::decode(&damaged).is_err(), "{damaged:?}");
    }
}
