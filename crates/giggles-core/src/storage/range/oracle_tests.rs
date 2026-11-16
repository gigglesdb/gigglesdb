use super::*;
use crate::storage::range::store::{
    RangeApplyOutcome,
    tests::{Dir, descriptors, two_ranges},
};
use crate::storage::replicated::{AppliedPosition, ReplicateError};
use std::sync::atomic::AtomicU64;

/// Applies proposals to the system range in log order.
struct Sink(Arc<RangeStore>);
impl BatchSink for Sink {
    fn replicate(&self, encoded_batch: Vec<u8>) -> Result<(), ReplicateError> {
        let index = self
            .0
            .applied(SYSTEM_RANGE)
            .unwrap()
            .map_or(1, |p| p.index + 1);
        let outcome = self
            .0
            .apply(
                SYSTEM_RANGE,
                &encoded_batch,
                AppliedPosition { index, term: 1 },
            )
            .map_err(|e| ReplicateError::Unavailable(format!("{e:?}")))?;
        assert_eq!(outcome, RangeApplyOutcome::Applied);
        Ok(())
    }
}

fn open(store: &Arc<RangeStore>, clock: Arc<AtomicU64>, width: u64) -> SystemRange {
    SystemRange::open_with(
        store,
        descriptors()[0].clone(),
        Arc::new(Sink(store.clone())),
        width,
        Box::new(move || clock.load(Ordering::SeqCst)),
    )
    .unwrap()
}

#[test]
fn timestamps_increase_across_terms_even_when_the_clock_goes_back() {
    let dir = Dir::new("range-oracle");
    let store = Arc::new(two_ranges(&dir));
    let clock = Arc::new(AtomicU64::new(5_000));
    let oracle = open(&store, clock.clone(), 8);
    let mut last = 0;
    for n in [1, 1, 3, 1, 9, 1] {
        let first = oracle.next(n).unwrap().0;
        assert!(first > last || last == 0, "{first} after {last}");
        assert!(first >= 5_000 << 16);
        last = first + u64::from(n) - 1;
    }
    let hw = read_u64(&oracle.kv, &oracle_hw_key()).unwrap().unwrap();
    assert!(
        hw > last,
        "every timestamp is below the persisted high-water"
    );
    // A new term with the clock far behind still starts above everything handed out.
    oracle.close();
    assert!(matches!(oracle.next(1), Err(RangeError::NotLeader { .. })));
    clock.store(1, Ordering::SeqCst);
    let oracle = open(&store, clock, 8);
    assert!(oracle.next(1).unwrap().0 >= hw);
    // Only the system range serves these.
    assert!(matches!(
        SystemRange::open(
            &store,
            descriptors()[1].clone(),
            Arc::new(Sink(store.clone()))
        ),
        Err(RangeError::Corrupt(_))
    ));
}

#[test]
fn leases_are_disjoint_and_survive_a_leader_change() {
    let dir = Dir::new("range-oracle");
    let store = Arc::new(two_ranges(&dir));
    let clock = Arc::new(AtomicU64::new(1));
    let system = open(&store, clock.clone(), WINDOW);
    let a = system.lease(LeaseKind::HeapRowId, 7, 100).unwrap();
    let b = system.lease(LeaseKind::HeapRowId, 7, 10).unwrap();
    assert_eq!((a.clone(), b.clone()), (1..101, 101..111));
    assert_eq!(system.lease(LeaseKind::Xid, 0, 5).unwrap(), 1..6);
    assert!(system.lease(LeaseKind::ObjectId, 0, 0).is_err());
    system.close();
    let system = open(&store, clock, WINDOW);
    assert_eq!(system.lease(LeaseKind::HeapRowId, 7, 1).unwrap(), 111..112);
    // The high-water uses the single-node allocator's layout (seq.rs).
    let mut key = vec![LeaseKind::HeapRowId as u8];
    key.extend_from_slice(&7u64.to_be_bytes());
    assert_eq!(
        store.kv().get(Keyspace::Seq, &key).unwrap(),
        Some(112u64.to_be_bytes().to_vec())
    );
}
