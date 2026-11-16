use super::*;
use crate::relational::DataType;
use crate::relational::catalog_ext::SequenceDef;
use crate::storage::engine::{codec, hlc::Ts};
use crate::storage::range::oracle::WINDOW;
use crate::storage::range::store::{
    RangeApplyOutcome, RangeStore,
    tests::{Dir, descriptors, two_ranges},
};
use crate::storage::range_api::{SYSTEM_RANGE, SystemService};
use crate::storage::replicated::{AppliedPosition, BatchSink, ReplicateError};
use std::sync::Arc;

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

/// The system range's services with a sequencer over the range, as the engine attaches it.
fn open(store: &Arc<RangeStore>) -> SystemRange {
    let system = SystemRange::open_with(
        store,
        descriptors()[0].clone(),
        Arc::new(Sink(store.clone())),
        WINDOW,
        Box::new(|| 1),
    )
    .unwrap();
    let backend: Arc<dyn crate::storage::engine::kv::KvBackend> = store.kv().clone();
    let kv = Arc::new(crate::storage::engine::kv::Kv::new(Arc::new(
        crate::storage::range::kv::RangeKv::new(
            backend,
            descriptors()[0].clone(),
            Arc::new(Sink(store.clone())),
        ),
    )));
    let sequencer = crate::storage::engine::commit::Sequencer::open(
        kv,
        Arc::new(crate::storage::engine::commit::SystemClock),
        crate::storage::engine::commit::SequencerConfig::default(),
    )
    .unwrap();
    system.attach_sequencer(sequencer);
    system
}

fn def(incarnation: u128, max: i64, cycle: bool) -> SequenceDef {
    SequenceDef {
        name: "s".into(),
        data_type: DataType::BigInt,
        start: 1,
        increment: 1,
        min: 1,
        max,
        cache: 1,
        cycle,
        owned_by: None,
        identity: false,
        incarnation,
    }
}

/// Commits a definition of sequence 9 at `at` (None = drop it).
fn define(store: &Arc<RangeStore>, at: u64, def: Option<&SequenceDef>) {
    let mut batch = WriteBatch::default();
    let value = match def {
        Some(def) => mvcc::encode_live(&catalog_codec::encode_sequence(def).unwrap()),
        None => mvcc::encode_tombstone(),
    };
    batch.put(
        Keyspace::Catalog,
        codec::user_key_seek_at(&catalog_codec::sequence_key(9), Ts(at)),
        value,
    );
    Sink(store.clone()).replicate(batch.encode()).unwrap();
}

fn next(system: &SystemRange, incarnation: u128) -> SequenceReply {
    system.sequence(9, incarnation, SequenceOp::Next).unwrap()
}

#[test]
fn values_continue_above_every_handed_out_value_after_a_leader_change() {
    let dir = Dir::new("range-sequence");
    let store = Arc::new(two_ranges(&dir));
    define(&store, 10, Some(&def(5, i64::MAX, false)));
    let system = open(&store);
    for want in 1..=40 {
        assert_eq!(next(&system, 5), SequenceReply::Value(want));
    }
    // The stored reservation is ahead of every value handed out, with the counter bits set.
    let stored = store
        .kv()
        .get(Keyspace::Seq, &catalog_codec::counter_key(9, 5))
        .unwrap()
        .unwrap();
    let counter = catalog_codec::decode_counter(
        &catalog_codec::counter_key(9, 5),
        &stored,
        &def(5, i64::MAX, false),
    )
    .unwrap();
    assert!(counter.log_hw >= 40);
    let features = u64::from_be_bytes(
        KvRead::get(&system.kv, Keyspace::Meta, b"features")
            .unwrap()
            .unwrap()
            .try_into()
            .unwrap(),
    );
    assert_eq!(features & (64 | 256), 64 | 256);
    // A new term resumes above the reservation, never below a value handed out.
    system.close();
    assert!(matches!(
        system.sequence(9, 5, SequenceOp::Next),
        Err(RangeError::NotLeader { .. })
    ));
    let system = open(&store);
    let SequenceReply::Value(after) = next(&system, 5) else {
        panic!("no value");
    };
    assert!(after > 40 && after <= counter.log_hw + 1, "{after}");
}

#[test]
fn setval_limits_wrong_incarnations_and_forget() {
    let dir = Dir::new("range-sequence");
    let store = Arc::new(two_ranges(&dir));
    define(&store, 10, Some(&def(5, 3, false)));
    let system = open(&store);
    assert_eq!(next(&system, 4), SequenceReply::Gone);
    assert_eq!(next(&system, 5), SequenceReply::Value(1));
    assert_eq!(
        system
            .sequence(
                9,
                5,
                SequenceOp::Set {
                    value: 4,
                    called: true
                }
            )
            .unwrap(),
        SequenceReply::OutOfRange
    );
    assert_eq!(
        system
            .sequence(
                9,
                5,
                SequenceOp::Set {
                    value: 2,
                    called: true
                }
            )
            .unwrap(),
        SequenceReply::Value(2)
    );
    assert_eq!(next(&system, 5), SequenceReply::Value(3));
    assert_eq!(next(&system, 5), SequenceReply::Limit);
    // setval is durable at once: a new term continues from it.
    system.close();
    let system = open(&store);
    assert_eq!(next(&system, 5), SequenceReply::Limit);
    // After DROP, Forget deletes the stored counter.
    define(&store, 20, None);
    assert_eq!(
        system.sequence(9, 5, SequenceOp::Forget).unwrap(),
        SequenceReply::Gone
    );
    assert_eq!(
        store
            .kv()
            .get(Keyspace::Seq, &catalog_codec::counter_key(9, 5))
            .unwrap(),
        None
    );
}

#[test]
fn a_cycle_wraps_and_concurrent_callers_never_share_a_value() {
    let dir = Dir::new("range-sequence");
    let store = Arc::new(two_ranges(&dir));
    define(&store, 10, Some(&def(5, 1000, true)));
    let system = Arc::new(open(&store));
    let threads: Vec<_> = (0..4)
        .map(|_| {
            let system = system.clone();
            std::thread::spawn(move || {
                (0..100)
                    .map(|_| match next(&system, 5) {
                        SequenceReply::Value(v) => v,
                        other => panic!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    let mut all: Vec<i64> = threads
        .into_iter()
        .flat_map(|t| t.join().unwrap())
        .collect();
    all.sort_unstable();
    assert_eq!(all, (1..=400).collect::<Vec<_>>());
    let mut cyc = def(6, 3, true);
    cyc.start = 2;
    define(&store, 30, Some(&cyc));
    let values: Vec<_> = (0..4).map(|_| next(&system, 6)).collect();
    assert_eq!(
        values,
        [2, 3, 1, 2].map(SequenceReply::Value).to_vec(),
        "CYCLE wraps to min"
    );
}
