use super::*;
use crate::storage::engine::kv_mem::MemKv;
use std::sync::Barrier;

fn sequencer() -> Arc<Sequencer> {
    Sequencer::open(
        Arc::new(Kv::new(Arc::new(MemKv::new()))),
        Arc::new(ManualClock::new(10)),
        SequencerConfig::default(),
    )
    .unwrap()
}

#[test]
fn snapshot_handles_unregister_on_another_thread() {
    let seq = sequencer();
    let first = seq.snapshot();
    let duplicate = seq.snapshot();
    assert_eq!(publish_visible(&seq.shared, Ts(10)), Ts(0));
    let newer = thread::scope(|scope| scope.spawn(|| seq.snapshot()).join().unwrap());
    assert_eq!(newer.ts(), Ts(10));
    thread::spawn(move || drop(first)).join().unwrap();
    assert_eq!(seq.oldest_active(), Some(Ts(0)));
    thread::spawn(move || drop(duplicate)).join().unwrap();
    assert_eq!(seq.oldest_active(), Some(Ts(10)));
    thread::spawn(move || drop(newer)).join().unwrap();
    assert_eq!(seq.oldest_active(), None);
    assert_eq!(publish_visible(&seq.shared, Ts(20)), Ts(20));
}

#[test]
fn stress_sharded_snapshots_trim_never_passes_live_snapshot() {
    let seq = sequencer();
    let trimmed = AtomicU64::new(0);
    let start = Barrier::new(32);
    thread::scope(|scope| {
        // One publisher matches the single production sequencer; timestamps
        // increase strictly, so its Release stores cannot regress visibility.
        scope.spawn(|| {
            start.wait();
            for ts in 1..=512 {
                let bound = publish_visible(&seq.shared, Ts(ts));
                trimmed.fetch_max(bound.0, Ordering::AcqRel);
                thread::yield_now();
            }
        });
        for _ in 0..31 {
            let seq = &seq;
            let trimmed = &trimmed;
            let start = &start;
            scope.spawn(move || {
                start.wait();
                for _ in 0..128 {
                    let handle = seq.snapshot();
                    for _ in 0..3 {
                        assert!(trimmed.load(Ordering::Acquire) <= handle.ts().0);
                        thread::yield_now();
                    }
                    drop(handle);
                }
            });
        }
    });
    assert_eq!(seq.oldest_active(), None);
    assert_eq!(seq.visible_ts(), Ts(512));
    let bound = publish_visible(&seq.shared, Ts(513));
    trimmed.fetch_max(bound.0, Ordering::AcqRel);
    assert_eq!(trimmed.load(Ordering::Acquire), 513);
}
