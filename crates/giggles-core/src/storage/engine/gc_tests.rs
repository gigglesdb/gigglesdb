use super::{Gc, GcConfig};
use crate::storage::engine::{
    codec,
    commit::{ManualClock, Sequencer, SequencerConfig},
    hlc::Ts,
    kv::{
        Direction, Durability, Keyspace, Kv, KvBackend, KvError, KvIter, KvPair, KvRead, Op,
        WriteBatch,
    },
    kv_mem::MemKv,
    locks::{LockConfig, LockManager, TxnId},
    mvcc,
    txn::{Isolation, Transaction, TxnError},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound,
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

fn ts(ms: u64) -> Ts {
    Ts(ms << 16)
}

fn row(table: u64, key: u8) -> Vec<u8> {
    let mut user = table.to_be_bytes().to_vec();
    user.push(key);
    user
}

fn version(batch: &mut WriteBatch, ks: Keyspace, user: &[u8], at: Ts, value: Option<&[u8]>) {
    batch.put(
        ks,
        codec::user_key_seek_at(user, at),
        value.map_or_else(mvcc::encode_tombstone, mvcc::encode_live),
    );
}

fn records(kv: &dyn KvRead, ks: Keyspace) -> Vec<KvPair> {
    kv.range(ks, Bound::Unbounded, Bound::Unbounded, Direction::Forward)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

#[derive(Default)]
struct RecordingKv {
    inner: Arc<MemKv>,
    batches: Mutex<Vec<Vec<Op>>>,
    writes: Mutex<Vec<(Durability, Vec<Op>)>>,
}

impl KvRead for RecordingKv {
    fn get(&self, ks: Keyspace, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        self.inner.get(ks, key)
    }

    fn range(
        &self,
        ks: Keyspace,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        dir: Direction,
    ) -> Result<KvIter<'_>, KvError> {
        self.inner.range(ks, start, end, dir)
    }
}

impl KvBackend for RecordingKv {
    fn write(&self, batch: WriteBatch, durability: Durability) -> Result<(), KvError> {
        let ops = batch.into_ops();
        self.batches.lock().unwrap().push(ops.clone());
        self.writes.lock().unwrap().push((durability, ops.clone()));
        let mut copy = WriteBatch::default();
        for op in ops {
            match op {
                Op::Put { ks, key, value } => copy.put(ks, key, value),
                Op::Delete { ks, key } => copy.delete(ks, key),
                Op::DeleteRange { ks, start, end } => copy.delete_range(ks, start, end),
            }
        }
        self.inner.write(copy, durability)
    }

    fn flush_barrier(&self) -> Result<(), KvError> {
        self.inner.flush_barrier()
    }

    fn snapshot(&self) -> Result<Arc<dyn KvRead>, KvError> {
        self.inner.snapshot()
    }

    fn engine_name(&self) -> &'static str {
        "gc-recording-mem"
    }
}

fn sequencer(kv: Arc<Kv>, clock: Arc<ManualClock>) -> Arc<Sequencer> {
    Sequencer::open(
        kv,
        clock,
        SequencerConfig {
            group_wait: Duration::ZERO,
            ..SequencerConfig::default()
        },
    )
    .unwrap()
}

fn seed(backend: &dyn KvBackend, visible: Ts, mut batch: WriteBatch) {
    batch.put(
        Keyspace::Meta,
        b"last_commit_ts".to_vec(),
        visible.0.to_be_bytes().to_vec(),
    );
    backend.write(batch, Durability::Sync).unwrap();
}

fn open(backend: Arc<dyn KvBackend>, retention: Duration) -> (Arc<Kv>, Arc<Sequencer>, Gc) {
    let kv = Arc::new(Kv::new(backend));
    let seq = sequencer(kv.clone(), Arc::new(ManualClock::new(100_000)));
    let gc = Gc::open(kv.clone(), seq.clone(), GcConfig { retention }).unwrap();
    (kv, seq, gc)
}

fn sweep(gc: &Gc, ks: Keyspace, budget: usize) {
    for _ in 0..100_010 {
        if gc.step(ks, budget).unwrap().wrapped {
            return;
        }
    }
    panic!("GC cursor failed to wrap");
}

#[test]
fn keep_rule_and_tombstone_deletion_are_atomic_in_every_versioned_keyspace() {
    for ks in [
        Keyspace::Rows,
        Keyspace::Index,
        Keyspace::Catalog,
        Keyspace::Kernel,
    ] {
        let backend = Arc::new(RecordingKv::default());
        let mut batch = WriteBatch::default();
        let a = row(7, 0);
        let b = row(7, 1);
        let c = row(7, 2);
        let d = row(7, 3);
        // All versions above W survive, including an above-W tombstone.
        version(&mut batch, ks, &a, ts(90), Some(b"new"));
        version(&mut batch, ks, &a, ts(70), None);
        version(&mut batch, ks, &a, ts(50), Some(b"anchor"));
        version(&mut batch, ks, &a, ts(20), Some(b"old"));
        // A tombstone at W and all its older versions disappear together.
        version(&mut batch, ks, &b, ts(80), Some(b"resurrected"));
        version(&mut batch, ks, &b, ts(50), None);
        version(&mut batch, ks, &b, ts(30), Some(b"old"));
        version(&mut batch, ks, &b, ts(0), Some(b"oldest"));
        version(&mut batch, ks, &c, ts(10), None);
        version(&mut batch, ks, &d, ts(75), None);
        version(&mut batch, ks, &d, ts(60), Some(b"above"));
        seed(backend.as_ref(), ts(100), batch);
        let (kv, _, gc) = open(backend.clone(), Duration::ZERO);
        let _pin = gc.pin(ts(50), "keep-rule".into()).unwrap();
        backend.writes.lock().unwrap().clear();
        assert_eq!(gc.advance().unwrap(), ts(50));
        backend.batches.lock().unwrap().clear();

        let first = gc.step(ks, 1).unwrap();
        assert_eq!((first.examined, first.deleted, first.keys), (4, 1, 1));
        assert!(!first.wrapped);
        let second = gc.step(ks, 2).unwrap();
        assert_eq!((second.examined, second.deleted, second.keys), (4, 3, 1));
        let b_deleted: Vec<_> = backend
            .batches
            .lock()
            .unwrap()
            .iter()
            .map(|ops| {
                ops.iter()
                    .filter_map(|op| match op {
                        Op::Delete { ks: space, key }
                            if *space == ks && codec::split_versioned(key).unwrap().0 == b =>
                        {
                            Some(key.clone())
                        }
                        _ => None,
                    })
                    .collect::<Vec<_>>()
            })
            .filter(|keys| !keys.is_empty())
            .collect();
        assert_eq!(
            b_deleted.len(),
            1,
            "one atomic batch must remove the entire hidden history"
        );
        assert_eq!(b_deleted[0].len(), 3);
        sweep(&gc, ks, 1);
        let expected = [
            (&a, ts(90), Some(&b"new"[..])),
            (&a, ts(70), None),
            (&a, ts(50), Some(&b"anchor"[..])),
            (&b, ts(80), Some(&b"resurrected"[..])),
            (&d, ts(75), None),
            (&d, ts(60), Some(&b"above"[..])),
        ]
        .into_iter()
        .map(|(user, at, value)| {
            (
                codec::user_key_seek_at(user, at),
                value.map_or_else(mvcc::encode_tombstone, mvcc::encode_live),
            )
        })
        .collect::<Vec<_>>();
        assert_eq!(records(kv.as_ref(), ks), expected);
        let stats = gc.stats();
        assert_eq!(stats.watermark, ts(50));
        assert_eq!(stats.visible, ts(100));
        assert_eq!(stats.lag_ms, 50);
        assert_eq!(stats.pins, 1);
        assert_eq!(stats.versions_deleted_total, 5);
        assert!(stats.versions_examined_total >= 11);
        let writes = backend.writes.lock().unwrap();
        let durable_watermark = writes.iter().position(|(durability, ops)| {
            *durability == Durability::Sync && ops.iter().any(|op| matches!(op,
                Op::Put { ks: Keyspace::Meta, key, value } if key == b"gc_watermark" && value == &ts(50).0.to_be_bytes()))
        }).unwrap();
        let first_delete = writes
            .iter()
            .position(|(_, ops)| ops.iter().any(|op| matches!(op, Op::Delete { .. })))
            .unwrap();
        assert!(
            durable_watermark < first_delete,
            "W must become durable before any delete is sent"
        );
    }
}

#[test]
fn tiny_budgets_finish_whole_keys_and_cursor_resumes_after_durable_crash() {
    let backend = Arc::new(MemKv::new());
    let mut batch = WriteBatch::default();
    for key in 0..12 {
        for at in [10, 20, 30] {
            version(
                &mut batch,
                Keyspace::Rows,
                &row(7, key),
                ts(at),
                Some(&[key]),
            );
        }
    }
    seed(backend.as_ref(), ts(100), batch);
    let (original_kv, _, gc) = open(backend.clone(), Duration::ZERO);
    assert_eq!(gc.advance().unwrap(), ts(100));
    let step = gc.step(Keyspace::Rows, 1).unwrap();
    assert_eq!((step.examined, step.deleted, step.keys), (3, 2, 1));
    // Losing a NoSync cursor also loses its deletes, so retry is harmless.
    let (lost_kv, _, lost_gc) = open(Arc::new(backend.crash()), Duration::ZERO);
    assert_eq!(lost_gc.watermark(), ts(100));
    assert_eq!(records(lost_kv.as_ref(), Keyspace::Rows).len(), 36);
    assert!(records(lost_kv.as_ref(), Keyspace::Gc).is_empty());
    let retried = lost_gc.step(Keyspace::Rows, 1).unwrap();
    assert_eq!((retried.examined, retried.deleted, retried.keys), (3, 2, 1));
    original_kv.flush_barrier().unwrap();
    drop(gc);
    let crashed = Arc::new(backend.crash());
    let (kv, _, reopened) = open(crashed, Duration::from_secs(1_000_000));
    assert_eq!(reopened.stats().watermark, ts(100));
    assert_eq!(
        reopened.advance().unwrap(),
        ts(100),
        "a new retention policy cannot rewind durable W"
    );
    let next = reopened.step(Keyspace::Rows, 1).unwrap();
    assert_eq!(
        (next.examined, next.deleted, next.keys),
        (3, 2, 1),
        "resume at the next key, not the first key's retained anchor"
    );
    for (key, count) in [(0, 1), (1, 1), (2, 3)] {
        assert_eq!(
            records(kv.as_ref(), Keyspace::Rows)
                .iter()
                .filter(|(raw, _)| { codec::split_versioned(raw).unwrap().0 == row(7, key) })
                .count(),
            count,
            "resumed cursor must process key 1, leaving key 2 untouched"
        );
    }
    sweep(&reopened, Keyspace::Rows, 2);
    assert_eq!(records(kv.as_ref(), Keyspace::Rows).len(), 12);
    for (key, _) in records(kv.as_ref(), Keyspace::Rows) {
        assert_eq!(codec::split_versioned(&key).unwrap().1, ts(30));
    }
}

#[test]
fn pin_admission_drop_active_snapshot_and_ten_second_retention() {
    let backend = Arc::new(MemKv::new());
    let mut batch = WriteBatch::default();
    for at in [8_000, 9_000, 10_000, 11_000, 15_000, 19_000] {
        version(
            &mut batch,
            Keyspace::Rows,
            &row(7, 1),
            ts(at),
            Some(b"retained"),
        );
    }
    seed(backend.as_ref(), Ts(ts(20_000).0 + 9), batch);
    let (kv, seq, gc) = open(backend, Duration::from_secs(10));
    assert_eq!(gc.compute(), ts(10_000));
    let pin = gc.pin(ts(9_000), "backup".into()).unwrap();
    let duplicate = gc.pin(ts(9_000), "backup".into()).unwrap();
    assert_eq!(
        gc.pins(),
        vec![(ts(9_000), "backup".into()), (ts(9_000), "backup".into())]
    );
    assert_eq!(gc.advance().unwrap(), ts(9_000));
    assert_eq!(
        gc.pin(ts(8_999), "too-old".into()).err(),
        Some(TxnError::SnapshotTooOld)
    );
    assert_eq!(gc.check_snapshot(ts(8_999)), Err(TxnError::SnapshotTooOld));
    assert!(gc.check_snapshot(ts(9_000)).is_ok());
    drop(pin);
    assert_eq!(gc.pins().len(), 1);
    drop(duplicate);
    assert!(gc.pins().is_empty());
    assert_eq!(gc.advance().unwrap(), ts(10_000));
    sweep(&gc, Keyspace::Rows, 1);
    assert_eq!(
        records(kv.as_ref(), Keyspace::Rows)
            .iter()
            .map(|(key, _)| codec::split_versioned(key).unwrap().1)
            .collect::<Vec<_>>(),
        vec![ts(19_000), ts(15_000), ts(11_000), ts(10_000)],
        "retention must preserve all above-cutoff versions and exactly the live anchor below it"
    );

    // Move the ManualClock through a real durable commit while an old snapshot lives.
    let held = seq.snapshot();
    let old = held.ts();
    let mut writes = BTreeMap::new();
    writes.insert((Keyspace::Kernel, b"clock".to_vec()), Some(vec![1]));
    let committed = seq
        .submit(crate::storage::engine::commit::CommitRequest {
            snapshot: old,
            writes,
            validation: None,
        })
        .unwrap();
    assert_eq!(committed, ts(100_000));
    assert_eq!(gc.compute(), old);
    assert_eq!(gc.advance().unwrap(), old);
    drop(held);
    assert_eq!(gc.advance().unwrap(), ts(90_000));
}

#[test]
fn table_reclamation_is_watermark_gated_and_isolates_neighbor_ids_including_maximum() {
    let backend = Arc::new(RecordingKv::default());
    let mut batch = WriteBatch::default();
    for table in [6, 7, 8, u64::MAX - 1, u64::MAX] {
        for key in 0..3 {
            version(
                &mut batch,
                Keyspace::Rows,
                &row(table, key),
                ts(10),
                Some(b"row"),
            );
            version(&mut batch, Keyspace::Rows, &row(table, key), ts(20), None);
            version(
                &mut batch,
                Keyspace::Index,
                &row(table, key),
                ts(10),
                Some(b"index"),
            );
        }
    }
    seed(backend.as_ref(), ts(100), batch);
    let (kv, _, gc) = open(backend.clone(), Duration::ZERO);
    let pin = gc.pin(ts(40), "old-reader".into()).unwrap();
    gc.advance().unwrap();
    let before = records(kv.as_ref(), Keyspace::Rows);
    assert!(!gc.reclaim_table(7, ts(50)).unwrap());
    assert_eq!(records(kv.as_ref(), Keyspace::Rows), before);
    drop(pin);
    gc.advance().unwrap();
    backend.batches.lock().unwrap().clear();
    assert!(gc.reclaim_table(7, ts(50)).unwrap());
    assert!(gc.reclaim_table(u64::MAX, ts(100)).unwrap());
    let after = records(kv.as_ref(), Keyspace::Rows);
    assert_eq!(
        after,
        before
            .into_iter()
            .filter(|(key, _)| {
                !key.starts_with(&7u64.to_be_bytes()) && !key.starts_with(&u64::MAX.to_be_bytes())
            })
            .collect::<Vec<_>>()
    );
    assert_eq!(
        records(kv.as_ref(), Keyspace::Index).len(),
        15,
        "table row reclamation must not reclaim unrelated index IDs"
    );
    let batches = backend.batches.lock().unwrap();
    assert!(batches.iter().flatten().any(|op| matches!(op, Op::DeleteRange { ks: Keyspace::Rows, start, end } if start == &7u64.to_be_bytes() && end == &8u64.to_be_bytes())));
}

#[test]
fn oversized_key_group_errors_without_deletion_or_cursor_movement() {
    let backend = Arc::new(RecordingKv::default());
    let mut batch = WriteBatch::default();
    let user = row(7, 1);
    for at in 0..100_001 {
        version(&mut batch, Keyspace::Rows, &user, Ts(at), Some(b"v"));
    }
    seed(backend.as_ref(), ts(100), batch);
    let (kv, _, gc) = open(backend.clone(), Duration::ZERO);
    gc.advance().unwrap();
    let cursors = records(kv.as_ref(), Keyspace::Gc);
    backend.batches.lock().unwrap().clear();
    assert!(gc.step(Keyspace::Rows, 1).is_err());
    assert_eq!(records(kv.as_ref(), Keyspace::Rows).len(), 100_001);
    assert_eq!(records(kv.as_ref(), Keyspace::Gc), cursors);
    assert!(backend.batches.lock().unwrap().is_empty());
    assert_eq!(gc.stats().versions_deleted_total, 0);
}

#[test]
fn nonversioned_spaces_and_malformed_envelopes_are_refused_without_data_loss() {
    let backend = Arc::new(MemKv::new());
    let mut batch = WriteBatch::default();
    let bad_key = codec::user_key_seek_at(&row(7, 1), ts(10));
    batch.put(Keyspace::Rows, bad_key.clone(), vec![1, 99]);
    seed(backend.as_ref(), ts(100), batch);
    let (kv, _, gc) = open(backend, Duration::ZERO);
    gc.advance().unwrap();
    for ks in [Keyspace::Meta, Keyspace::Seq, Keyspace::Gc] {
        assert_eq!(gc.step(ks, 10), Err(TxnError::Misuse));
    }
    assert!(gc.step(Keyspace::Rows, 10).is_err());
    assert_eq!(kv.get(Keyspace::Rows, &bad_key).unwrap(), Some(vec![1, 99]));
    assert_eq!(gc.stats().versions_deleted_total, 0);
}

#[test]
fn failed_watermark_sync_never_publishes_or_authorizes_deletes() {
    for after_apply in [false, true] {
        let backend = Arc::new(MemKv::new());
        let mut batch = WriteBatch::default();
        for at in [10, 20, 30] {
            version(
                &mut batch,
                Keyspace::Rows,
                &row(7, 1),
                ts(at),
                Some(b"value"),
            );
        }
        seed(backend.as_ref(), ts(100), batch);
        let (kv, _, gc) = open(backend.clone(), Duration::ZERO);
        if after_apply {
            backend.fail_next_write_after_apply();
        } else {
            backend.fail_next_write(KvError::Io("watermark sync failure".into()));
        }
        assert!(gc.step(Keyspace::Rows, 1).is_err());
        assert_eq!(gc.watermark(), Ts(0));
        assert!(kv.is_fenced());
        assert_eq!(records(backend.as_ref(), Keyspace::Rows).len(), 3);
        assert_eq!(gc.stats().versions_deleted_total, 0);
        let (_, _, recovered) = open(Arc::new(backend.crash()), Duration::ZERO);
        assert_eq!(
            recovered.watermark(),
            if after_apply { ts(100) } else { Ts(0) }
        );
        assert_eq!(recovered.advance().unwrap(), ts(100));
    }
}

#[derive(Clone, Debug)]
struct WriteEvent {
    at: Ts,
    key: u8,
    value: Option<Vec<u8>>,
}

#[derive(Debug)]
struct ReadEvent {
    at: Ts,
    key: u8,
    value: Option<Vec<u8>>,
}

struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn pause(&mut self) {
        if self.next() & 7 == 0 {
            thread::sleep(Duration::from_micros(30 + self.next() % 70));
        } else {
            thread::yield_now();
        }
    }
}

#[test]
fn seeded_concurrent_readers_writers_and_gc_match_the_complete_history() {
    let started = Instant::now();
    for seed_value in [
        0x1f12_a34b_9876_5d01,
        0xfedc_ba98_7654_3211,
        0x583a_ee20_1122_3345,
    ] {
        concurrent_history(seed_value);
    }
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "three bounded GC histories exceeded 20 seconds: {:?}",
        started.elapsed()
    );
}

fn concurrent_history(seed_value: u64) {
    const WRITES: usize = 160;
    const READS: usize = 100;
    let backend = Arc::new(MemKv::new());
    let mut batch = WriteBatch::default();
    let mut initial = Vec::new();
    for key in 0..64u8 {
        let value = vec![0, key];
        version(
            &mut batch,
            Keyspace::Rows,
            &row(7, key),
            ts(1000),
            Some(&value),
        );
        initial.push(WriteEvent {
            at: ts(1000),
            key,
            value: Some(value),
        });
    }
    seed(backend.as_ref(), ts(1000), batch);
    let kv = Arc::new(Kv::new(backend));
    let clock = Arc::new(ManualClock::new(1000));
    let seq = sequencer(kv.clone(), clock.clone());
    let gc = Arc::new(Gc::open(kv.clone(), seq.clone(), GcConfig::default()).unwrap());
    let locks = Arc::new(LockManager::new(LockConfig::default()));
    let start = Arc::new(Barrier::new(9));
    let primed = Arc::new(Barrier::new(9));
    let done = Arc::new(AtomicUsize::new(0));
    let ticks = Arc::new(AtomicU64::new(1001));
    let overlapping_readers = Arc::new(AtomicUsize::new(0));
    let write_events = Arc::new(Mutex::new(initial));
    let read_events = Arc::new(Mutex::new(Vec::new()));
    let mut workers = Vec::new();
    for writer in 0..4 {
        let (seq, locks, clock, start, primed, done, ticks, events) = (
            seq.clone(),
            locks.clone(),
            clock.clone(),
            start.clone(),
            primed.clone(),
            done.clone(),
            ticks.clone(),
            write_events.clone(),
        );
        workers.push(thread::spawn(move || {
            let mut random =
                Random(seed_value ^ (writer as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15));
            start.wait();
            primed.wait();
            for operation in 0..WRITES {
                let key = (random.next() % 64) as u8;
                let user = row(7, key);
                let value = (!random.next().is_multiple_of(4)).then(|| {
                    vec![
                        writer as u8 + 1,
                        (operation >> 8) as u8,
                        operation as u8,
                        key,
                    ]
                });
                let mut txn = Transaction::begin(
                    &seq,
                    &locks,
                    TxnId(1 + writer as u64 * 10_000 + operation as u64),
                    Isolation::ReadCommitted,
                );
                txn.lock_for_write(Keyspace::Rows, &user, None, &|| false)
                    .unwrap();
                match &value {
                    Some(value) => txn.put(Keyspace::Rows, user, value.clone()).unwrap(),
                    None => txn.delete(Keyspace::Rows, user).unwrap(),
                }
                clock.set(ticks.fetch_add(1, Ordering::SeqCst));
                let at = txn.commit().unwrap();
                events.lock().unwrap().push(WriteEvent { at, key, value });
                random.pause();
            }
            done.fetch_add(1, Ordering::SeqCst);
        }));
    }
    for reader in 0..4 {
        let (seq, locks, start, primed, done, events, overlap) = (
            seq.clone(),
            locks.clone(),
            start.clone(),
            primed.clone(),
            done.clone(),
            read_events.clone(),
            overlapping_readers.clone(),
        );
        workers.push(thread::spawn(move || {
            let mut random =
                Random(seed_value ^ (reader as u64 + 11).wrapping_mul(0xd1b5_4a32_d192_ed03));
            let mut local = Vec::new();
            start.wait();
            for operation in 0..READS {
                let mut txn = Transaction::begin(
                    &seq,
                    &locks,
                    TxnId(100_000 + reader as u64 * 10_000 + operation as u64),
                    Isolation::RepeatableRead,
                );
                assert_eq!(txn.current_snapshot_ts(), None);
                let keys: Vec<_> = (0..8).map(|_| (random.next() % 64) as u8).collect();
                let first = txn.get(Keyspace::Rows, &row(7, keys[0])).unwrap();
                let at = txn.current_snapshot_ts().unwrap();
                local.push(ReadEvent {
                    at,
                    key: keys[0],
                    value: first,
                });
                if operation == 0 {
                    primed.wait();
                }
                for &key in &keys[1..] {
                    random.pause();
                    local.push(ReadEvent {
                        at,
                        key,
                        value: txn.get(Keyspace::Rows, &row(7, key)).unwrap(),
                    });
                }
                random.pause();
                for &key in keys.iter().rev() {
                    random.pause();
                    local.push(ReadEvent {
                        at,
                        key,
                        value: txn.get(Keyspace::Rows, &row(7, key)).unwrap(),
                    });
                }
                assert_eq!(txn.current_snapshot_ts(), Some(at));
                if seq.visible_ts() > at {
                    overlap.fetch_add(1, Ordering::SeqCst);
                }
                txn.rollback();
            }
            events.lock().unwrap().extend(local);
            done.fetch_add(1, Ordering::SeqCst);
        }));
    }
    let gc_worker = {
        let (gc, start, primed, done) = (gc.clone(), start.clone(), primed.clone(), done.clone());
        thread::spawn(move || {
            let mut random = Random(seed_value ^ 0x55aa_9933_1122_7789);
            let mut steps = 0;
            let mut deleted = 0;
            start.wait();
            primed.wait();
            while done.load(Ordering::SeqCst) < 8 {
                gc.advance().unwrap();
                let step = gc
                    .step(Keyspace::Rows, 1 + (random.next() % 7) as usize)
                    .unwrap();
                steps += 1;
                deleted += step.deleted;
                random.pause();
            }
            (steps, deleted)
        })
    };
    for worker in workers {
        worker
            .join()
            .unwrap_or_else(|failure| panic!("seed {seed_value:#x}: worker failed: {failure:?}"));
    }
    let (steps, deleted) = gc_worker.join().unwrap();
    assert!(
        steps > 0 && deleted > 0,
        "seed {seed_value:#x}: GC must reclaim during the concurrent workload"
    );
    assert!(
        overlapping_readers.load(Ordering::SeqCst) > 0,
        "seed {seed_value:#x}: held reader snapshots did not overlap commits"
    );
    let mut writes = write_events.lock().unwrap().clone();
    writes.sort_by_key(|event| event.at);
    assert_eq!(writes.len(), 64 + 4 * WRITES);
    let reads = read_events.lock().unwrap();
    assert_eq!(reads.len(), 4 * READS * 16);
    for read in reads.iter() {
        let expected = writes
            .iter()
            .rev()
            .find(|write| write.key == read.key && write.at <= read.at)
            .and_then(|write| write.value.clone());
        assert_eq!(
            read.value, expected,
            "LOST VERSION: seed {seed_value:#x}, snapshot {:?}, key {}",
            read.at, read.key
        );
    }
    drop(reads);
    let watermark = gc.advance().unwrap();
    // Background GC may stop in the middle of the keyspace. Finish that tail
    // to reset its cursor, then visit every key at the now-fixed final W.
    sweep(&gc, Keyspace::Rows, 3);
    sweep(&gc, Keyspace::Rows, 3);
    assert_eq!(gc.watermark(), watermark);
    // The complete history independently determines the exact physical keep set.
    let mut anchors = BTreeSet::new();
    let mut expected = BTreeMap::new();
    for write in writes.iter().rev() {
        let keep = write.at > watermark || (anchors.insert(write.key) && write.value.is_some());
        if keep {
            expected.insert(
                codec::user_key_seek_at(&row(7, write.key), write.at),
                write
                    .value
                    .as_deref()
                    .map_or_else(mvcc::encode_tombstone, mvcc::encode_live),
            );
        }
    }
    assert_eq!(
        records(kv.as_ref(), Keyspace::Rows),
        expected.into_iter().collect::<Vec<_>>(),
        "seed {seed_value:#x}: final storage differs from exact keep rule at {watermark:?}"
    );
}
