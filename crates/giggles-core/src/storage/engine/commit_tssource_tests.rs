use super::*;
use crate::storage::{
    engine::{kv_mem::MemKv, mvcc},
    range_api::{RangeError, Timestamp},
};
use std::sync::atomic::{AtomicU64, Ordering};

/// Hands out `script` first, then a counter; `fail` makes every call error.
struct FakeOracle {
    script: Mutex<VecDeque<u64>>,
    counter: AtomicU64,
    calls: AtomicU64,
    fail: bool,
}
impl FakeOracle {
    fn with(start: u64, script: &[u64], fail: bool) -> Arc<Self> {
        Arc::new(Self {
            script: Mutex::new(script.iter().copied().collect()),
            counter: AtomicU64::new(start),
            calls: AtomicU64::new(0),
            fail,
        })
    }
    fn new(start: u64, script: &[u64]) -> Arc<Self> {
        Self::with(start, script, false)
    }
    fn failing() -> Arc<Self> {
        Self::with(0, &[], true)
    }
    fn calls(&self) -> u64 {
        self.calls.load(Ordering::SeqCst)
    }
}
impl TsOracle for FakeOracle {
    fn next(&self, n: u32) -> Result<Timestamp, RangeError> {
        assert!(n >= 1);
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail {
            return Err(RangeError::Unavailable("oracle down".into()));
        }
        if let Some(ts) = self.script.lock().unwrap().pop_front() {
            return Ok(Timestamp(ts));
        }
        Ok(Timestamp(
            self.counter.fetch_add(u64::from(n), Ordering::SeqCst),
        ))
    }
}

fn request(n: u64) -> CommitRequest {
    CommitRequest {
        snapshot: Ts(0),
        writes: BTreeMap::from([((Keyspace::Rows, n.to_be_bytes().to_vec()), Some(vec![1]))]),
        features_or: 0,
        minimum_subversion: 0,
        raw: Vec::new(),
        validation: None,
    }
}

fn open(oracle: Arc<FakeOracle>) -> Arc<Sequencer> {
    Sequencer::open(
        Arc::new(Kv::new(Arc::new(MemKv::new()))),
        Arc::new(ManualClock::new(10)),
        SequencerConfig {
            ts_source: TsSource::Oracle(oracle),
            ..SequencerConfig::default()
        },
    )
    .unwrap()
}

fn version(seq: &Sequencer, n: u64) -> Option<(Ts, Option<Vec<u8>>)> {
    mvcc::latest_version(seq.kv(), Keyspace::Rows, &n.to_be_bytes()).unwrap()
}

#[test]
fn default_source_is_hlc() {
    assert!(matches!(
        SequencerConfig::default().ts_source,
        TsSource::Hlc
    ));
}

#[test]
fn oracle_commits_are_strictly_increasing_oracle_values_above_the_served_floor() {
    let start = 1000 << 16;
    let oracle = FakeOracle::new(start, &[]);
    let seq = open(oracle.clone());
    let floor = seq.served_gate();
    let mut last = Ts(0);
    for n in 0..20u64 {
        if n == 10 {
            // A read served well above the counter forces refetches (O4).
            seq.note_served(Ts(start + 40));
            assert_eq!(floor.served().0, start + 40);
        }
        let calls = oracle.calls();
        let ts = seq.submit(request(n)).unwrap();
        assert!(ts > last, "commit {n}: {ts:?} <= {last:?}");
        assert!(ts.0 > floor.served().0);
        // The value is the oracle's latest answer, not an HLC tick.
        assert_eq!(ts.0, oracle.counter.load(Ordering::SeqCst) - 1);
        assert_eq!(version(&seq, n).map(|(at, _)| at), Some(ts));
        if n == 10 {
            assert!(
                oracle.calls() - calls > 1,
                "stale oracle value was not refetched"
            );
            assert_eq!(ts.0, start + 41);
        } else {
            assert_eq!(oracle.calls() - calls, 1);
        }
        last = ts;
    }
    assert_eq!(seq.visible_ts(), last);
    // A lower floor never lowers the shared value.
    seq.note_served(Ts(1));
    assert_eq!(floor.served().0, start + 40);
}

#[test]
fn stale_oracle_values_at_or_below_the_last_commit_are_refetched() {
    let oracle = FakeOracle::new(50, &[10, 5, 10, 3]);
    let seq = open(oracle.clone());
    assert_eq!(seq.submit(request(1)).unwrap(), Ts(10));
    // 5 and 10 are <= the last commit (10), 3 likewise; then the counter's 50.
    assert_eq!(seq.submit(request(2)).unwrap(), Ts(50));
    assert_eq!(oracle.calls(), 5);
    assert_eq!(version(&seq, 2).map(|(at, _)| at), Some(Ts(50)));
}

#[test]
fn one_oracle_reservation_per_commit_group() {
    let oracle = FakeOracle::new(7 << 16, &[]);
    let seq = open(oracle.clone());
    let mut log = RecentLog::new(1 << 20);
    let results = seq.process_group_for_test(
        vec![request(1), request(2), request(3)],
        &ManualClock::new(10),
        &mut log,
    );
    assert_eq!(oracle.calls(), 1);
    assert_eq!(
        results,
        vec![
            Ok(Ts(7 << 16)),
            Ok(Ts((7 << 16) + 1)),
            Ok(Ts((7 << 16) + 2))
        ]
    );
    assert_eq!(seq.visible_ts(), Ts((7 << 16) + 2));
    // A group without writes takes no oracle timestamp.
    let mut empty = request(4);
    empty.writes.clear();
    let results = seq.process_group_for_test(vec![empty], &ManualClock::new(10), &mut log);
    assert_eq!(results, vec![Ok(Ts((7 << 16) + 2))]);
    assert_eq!(oracle.calls(), 1);
}

#[test]
fn oracle_error_fails_the_commit_and_writes_nothing() {
    let seq = open(FakeOracle::failing());
    assert_eq!(
        seq.submit(request(1)),
        Err(TxnError::Codec(CodecError::Clock))
    );
    assert_eq!(version(&seq, 1), None);
    assert_eq!(
        seq.kv().get(Keyspace::Meta, b"last_commit_ts").unwrap(),
        None
    );
    assert_eq!(seq.visible_ts(), Ts(0));
    // The sequencer is not fenced by an oracle failure.
    assert!(!seq.kv().is_fenced());
}

#[test]
fn an_oracle_stuck_below_the_floor_gives_up_without_writing() {
    let oracle = FakeOracle::new(0, &[]);
    let seq = open(oracle.clone());
    seq.note_served(Ts(u64::MAX - 1));
    assert_eq!(
        seq.submit(request(1)),
        Err(TxnError::Codec(CodecError::Clock))
    );
    assert_eq!(oracle.calls(), ORACLE_REFETCH_LIMIT as u64);
    assert_eq!(version(&seq, 1), None);
    assert_eq!(seq.visible_ts(), Ts(0));
}

#[test]
fn a_read_waits_for_reservations_at_or_below_its_timestamp() {
    let oracle = FakeOracle::new(100, &[]);
    let gate = Arc::new(ServedGate::new(Ts(0)));
    let first = gate.reserve(oracle.as_ref(), 2, Ts(0)).unwrap();
    assert_eq!(first, Ts(100));
    // Below the reservation: no wait. At or above it: waits until it finishes.
    assert!(gate.serve(Ts(99), Some(Instant::now())));
    assert!(!gate.serve(Ts(100), Some(Instant::now() + Duration::from_millis(20))));
    let reader = {
        let gate = gate.clone();
        thread::spawn(move || gate.serve(Ts(500), None))
    };
    thread::sleep(Duration::from_millis(20));
    gate.finish(first);
    assert!(reader.join().unwrap());
    // The raised floor makes the next reservation refetch above it.
    assert_eq!(gate.served(), Ts(500));
    oracle.counter.store(400, Ordering::SeqCst);
    let next = gate.reserve(oracle.as_ref(), 1, Ts(0)).unwrap();
    assert!(next > Ts(500));
}
