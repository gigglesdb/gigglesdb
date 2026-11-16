//! GD-0061 C5: `track_range` records a read range without reading it.
use super::super::{
    commit::{ManualClock, SequencerConfig},
    kv::Kv,
    kv_mem::MemKv,
    locks::LockConfig,
};
use super::*;

fn setup(config: SequencerConfig) -> (Arc<Sequencer>, Arc<LockManager>) {
    let seq = Sequencer::open(
        Arc::new(Kv::new(Arc::new(MemKv::new()))),
        Arc::new(ManualClock::new(10)),
        config,
    )
    .unwrap();
    (seq, Arc::new(LockManager::new(LockConfig::default())))
}
fn key(n: u64) -> Vec<u8> {
    let mut k = 1u64.to_be_bytes().to_vec();
    k.extend(n.to_be_bytes());
    k
}
fn txn(seq: &Arc<Sequencer>, locks: &Arc<LockManager>, id: u64, i: Isolation) -> Transaction {
    Transaction::begin(seq, locks, TxnId(id), i)
}
fn store(seq: &Arc<Sequencer>, locks: &Arc<LockManager>, id: u64, k: &[u8], v: &[u8]) -> Ts {
    let mut t = txn(seq, locks, id, Isolation::ReadCommitted);
    t.lock_for_write(Keyspace::Rows, k, None, &|| false)
        .unwrap();
    t.put(Keyspace::Rows, k.to_vec(), v.to_vec()).unwrap();
    t.commit().unwrap()
}
/// A role write committed by READ COMMITTED, as `DROP ROLE x` stages it: a
/// delete of the kind-8 role record `08 'x' 00` under the roles feature bit.
fn role_write(seq: &Arc<Sequencer>, locks: &Arc<LockManager>, id: u64) {
    let mut t = txn(seq, locks, id, Isolation::ReadCommitted);
    t.delete(Keyspace::Catalog, vec![8, b'x', 0]).unwrap();
    t.require_features(super::super::super::roles::engine::FEATURE);
    t.commit_then(|_| Ok(())).unwrap();
}
/// A well-formed protected Kernel write (R3-601 gates the 0x20..=0x2B namespaces).
fn protected_write(t: &mut Transaction, n: u64) {
    use crate::task::records::{self as rec, TaskObjectId};
    t.put(
        Keyspace::Kernel,
        rec::debit_key(TaskObjectId(1), n, 0),
        rec::DebitRecord::default().encode(),
    )
    .unwrap();
    t.require_features(super::super::meta::PROTECTED_RECORDS);
}
fn rw_conflict() -> Result<Ts, TxnError> {
    Err(TxnError::Serialization(
        SerializationReason::ReadWriteConflict,
    ))
}

#[test]
fn tracked_catalog_range_conflicts_with_rc_role_write() {
    for track in [false, true] {
        let (seq, locks) = setup(SequencerConfig::default());
        let mut t = txn(&seq, &locks, 1, Isolation::Serializable);
        if track {
            t.track_range(Keyspace::Catalog, Bound::Unbounded, Bound::Unbounded)
                .unwrap();
            assert_eq!(t.read_set().ranges.len(), 1);
        } else {
            t.snapshot_ts();
        }
        protected_write(&mut t, 1);
        role_write(&seq, &locks, 2);
        let result = t.commit();
        if track {
            assert_eq!(result, rw_conflict());
        } else {
            assert!(result.is_ok(), "{result:?}");
        }
    }
}

#[test]
fn tracked_range_without_read_sees_insert() {
    for (inserted, conflicts) in [(15, true), (10, true), (20, false), (5, false)] {
        let (seq, locks) = setup(SequencerConfig::default());
        let mut t = txn(&seq, &locks, 1, Isolation::Serializable);
        assert_eq!(t.current_snapshot_ts(), None);
        t.track_range(
            Keyspace::Rows,
            Bound::Included(key(10)),
            Bound::Excluded(key(20)),
        )
        .unwrap();
        // The range is relative to a snapshot, so tracking fixes one.
        assert!(t.current_snapshot_ts().is_some());
        assert!(t.read_set().points.is_empty());
        store(&seq, &locks, 2, &key(inserted), b"new");
        protected_write(&mut t, 1);
        let result = t.commit();
        if conflicts {
            assert_eq!(result, rw_conflict(), "insert at {inserted}");
        } else {
            assert!(result.is_ok(), "insert at {inserted}: {result:?}");
        }
    }
}

#[test]
fn no_effect_under_read_committed() {
    let (seq, locks) = setup(SequencerConfig::default());
    for (id, i) in [
        (1, Isolation::ReadCommitted),
        (3, Isolation::RepeatableRead),
    ] {
        let mut t = txn(&seq, &locks, id, i);
        let before = t.current_snapshot_ts();
        t.track_range(Keyspace::Rows, Bound::Unbounded, Bound::Unbounded)
            .unwrap();
        assert_eq!(t.current_snapshot_ts(), before);
        assert!(t.read_set().ranges.is_empty());
        store(&seq, &locks, id + 1, &key(id), b"new");
        protected_write(&mut t, id);
        t.commit().unwrap();
    }
    for i in [
        Isolation::ReadCommitted,
        Isolation::RepeatableRead,
        Isolation::Serializable,
    ] {
        let mut t = txn(&seq, &locks, 9, i);
        for ks in [Keyspace::Meta, Keyspace::Seq, Keyspace::Gc] {
            assert_eq!(
                t.track_range(ks, Bound::Unbounded, Bound::Unbounded),
                Err(TxnError::Misuse)
            );
        }
        assert!(t.read_set().ranges.is_empty());
        t.rollback();
    }
}

#[test]
fn log_overflow_fallback_scans_tracked_range() {
    for (inserted, conflicts) in [(15, true), (25, false)] {
        let (seq, locks) = setup(SequencerConfig {
            recent_log_bytes: 0,
            ..SequencerConfig::default()
        });
        store(&seq, &locks, 1, &key(99), b"seed");
        let mut t = txn(&seq, &locks, 2, Isolation::Serializable);
        t.track_range(
            Keyspace::Rows,
            Bound::Included(key(10)),
            Bound::Excluded(key(20)),
        )
        .unwrap();
        store(&seq, &locks, 3, &key(inserted), b"new");
        store(&seq, &locks, 4, &key(99), b"churn");
        protected_write(&mut t, 1);
        let result = t.commit();
        if conflicts {
            assert_eq!(result, rw_conflict(), "insert at {inserted}");
        } else {
            assert!(result.is_ok(), "insert at {inserted}: {result:?}");
        }
    }
}
