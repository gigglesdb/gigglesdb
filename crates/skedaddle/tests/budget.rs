use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use skedaddle::{
    budget::{
        CaptureAdmission, Clock, ConnectionLimit, Error, Limits, RateLimit, ReadLimits,
        ThroughputBudget, TokenBucket,
    },
    buffer::reader::{Backpressure, BufferBounds},
};

#[derive(Default)]
struct ManualClock(AtomicU64);

impl ManualClock {
    fn advance_ns(&self, nanos: u64) {
        self.0.fetch_add(nanos, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Duration {
        Duration::from_nanos(self.0.load(Ordering::SeqCst))
    }
}

fn rate(per_second: u64, burst: u64) -> RateLimit {
    RateLimit { per_second, burst }
}

fn limits() -> Limits {
    Limits {
        chunk_reads: ReadLimits {
            rows: rate(10, 10),
            bytes: rate(100, 100),
        },
        verification_reads: ReadLimits {
            rows: rate(5, 5),
            bytes: rate(50, 50),
        },
        applier_transactions: rate(20, 20),
        connections: BTreeMap::from([
            (
                "source".into(),
                ConnectionLimit {
                    total: 2,
                    capture_reserved: 1,
                },
            ),
            (
                "destination".into(),
                ConnectionLimit {
                    total: 2,
                    capture_reserved: 0,
                },
            ),
        ]),
        in_memory_segments: 1,
    }
}

#[test]
fn bucket_rate_accuracy_preserves_fractional_credit() {
    let clock = Arc::new(ManualClock::default());
    let mut bucket = TokenBucket::new(rate(3, 3), clock.clone()).unwrap();
    bucket.try_take(3).unwrap();
    // Three tokens/second is not an integer number of nanoseconds/token.
    let mut admitted = 0;
    for _ in 0..10_000 {
        clock.advance_ns(1_000_000);
        if bucket.try_take(1).is_ok() {
            admitted += 1;
        }
    }
    assert_eq!(admitted, 30);
    assert_eq!(
        bucket.try_take(1),
        Err(Error::Throttled {
            retry_after: Duration::from_nanos(333_333_334)
        })
    );
}

#[test]
fn bucket_burst_is_bounded_and_oversized_requests_are_explicit() {
    let clock = Arc::new(ManualClock::default());
    let mut bucket = TokenBucket::new(rate(10, 4), clock.clone()).unwrap();
    bucket.try_take(4).unwrap();
    clock.advance_ns(100_000_000_000);
    assert_eq!(
        bucket.try_take(5),
        Err(Error::RequestExceedsBurst {
            requested: 5,
            burst: 4
        })
    );
    bucket.try_take(4).unwrap();
    assert_eq!(
        bucket.try_take(1),
        Err(Error::Throttled {
            retry_after: Duration::from_millis(100)
        })
    );
}

#[test]
fn backwards_clock_cannot_mint_credit_twice() {
    let clock = Arc::new(ManualClock::default());
    let mut bucket = TokenBucket::new(rate(1, 1), clock.clone()).unwrap();
    bucket.try_take(1).unwrap();
    clock.advance_ns(1_000_000_000);
    bucket.try_take(1).unwrap();
    clock.0.store(0, Ordering::SeqCst);
    assert!(bucket.try_take(1).is_err());
    clock.0.store(1_000_000_000, Ordering::SeqCst);
    assert!(bucket.try_take(1).is_err());
    clock.advance_ns(1_000_000_000);
    bucket.try_take(1).unwrap();
}

#[test]
fn read_admission_is_atomic_across_rows_and_bytes() {
    let clock = Arc::new(ManualClock::default());
    let mut budget = ThroughputBudget::new(limits(), clock.clone()).unwrap();
    budget.try_chunk_read(0, 100).unwrap();
    assert_eq!(
        budget.try_chunk_read(10, 1),
        Err(Error::Throttled {
            retry_after: Duration::from_millis(10)
        })
    );
    // The failed byte check did not consume the row tokens.
    budget.try_chunk_read(10, 0).unwrap();
    assert_eq!(
        budget.try_chunk_read(10, 100),
        Err(Error::Throttled {
            retry_after: Duration::from_secs(1)
        })
    );
    clock.advance_ns(1_000_000_000);
    budget.try_chunk_read(10, 100).unwrap();
}

#[test]
fn read_oversize_does_not_report_a_retryable_delay() {
    let clock = Arc::new(ManualClock::default());
    let mut budget = ThroughputBudget::new(limits(), clock).unwrap();
    budget.try_chunk_read(10, 100).unwrap();
    assert!(matches!(
        budget.try_chunk_read(1, 101),
        Err(Error::RequestExceedsBurst { .. })
    ));
}

#[test]
fn copy_throttle_never_stalls_capture() {
    let clock = Arc::new(ManualClock::default());
    let mut budget = ThroughputBudget::new(limits(), clock.clone()).unwrap();
    let pressure = Backpressure::new(BufferBounds {
        max_buffered_bytes: 1000,
        max_unapplied_txns: 100,
        max_oldest_unapplied_age: Duration::from_secs(60),
    })
    .unwrap();
    let now = Instant::now();
    budget.try_chunk_read(10, 100).unwrap();
    budget.try_verification_read(5, 50).unwrap();
    budget.try_apply(20).unwrap();
    let _copy_connection = budget.try_connection("source").unwrap();
    let _segment = budget.try_segment().unwrap();
    assert!(budget.try_connection("source").is_err());
    assert!(budget.try_segment().is_err());
    let mut admissions = Vec::new();
    for _ in 0..100 {
        assert!(budget.try_chunk_read(1, 1).is_err());
        assert!(budget.try_verification_read(1, 1).is_err());
        assert!(budget.try_apply(1).is_err());
        assert_eq!(
            budget.capture_admission(&pressure.refresh(now)),
            CaptureAdmission::Ready
        );
        admissions.push(pressure.try_admit(1, 1, now).unwrap());
    }
    assert_eq!(
        budget.capture_admission(&pressure.refresh(now)),
        CaptureAdmission::BufferBackpressure
    );
    // Refilling all worker buckets cannot override the buffer's pause.
    clock.advance_ns(1_000_000_000);
    budget.try_chunk_read(10, 100).unwrap();
    assert_eq!(
        budget.capture_admission(&pressure.refresh(now)),
        CaptureAdmission::BufferBackpressure
    );
    pressure.mark_applied(admissions[0], now).unwrap();
    pressure.mark_deleted(admissions[0], now).unwrap();
    assert_eq!(
        budget.capture_admission(&pressure.refresh(now)),
        CaptureAdmission::Ready
    );
}

#[test]
fn copy_verify_and_apply_have_independent_rates() {
    let clock = Arc::new(ManualClock::default());
    let mut budget = ThroughputBudget::new(limits(), clock.clone()).unwrap();
    budget.try_chunk_read(10, 100).unwrap();
    budget.try_verification_read(5, 50).unwrap();
    budget.try_apply(20).unwrap();
    clock.advance_ns(500_000_000);
    budget.try_chunk_read(5, 50).unwrap();
    budget.try_verification_read(2, 25).unwrap();
    budget.try_apply(10).unwrap();
    assert!(budget.try_chunk_read(1, 0).is_err());
    assert!(budget.try_verification_read(1, 1).is_err());
    assert!(budget.try_apply(1).is_err());
}

#[test]
fn per_database_connection_caps_preserve_capture_reservations() {
    let budget = ThroughputBudget::new(limits(), Arc::new(ManualClock::default())).unwrap();
    let source = budget.try_connection("source").unwrap();
    assert!(matches!(
        budget.try_connection("source"),
        Err(Error::ConnectionsExhausted)
    ));
    let _dest_a = budget.try_connection("destination").unwrap();
    let _dest_b = budget.try_connection("destination").unwrap();
    assert!(matches!(
        budget.try_connection("destination"),
        Err(Error::ConnectionsExhausted)
    ));
    assert!(matches!(
        budget.try_connection("unconfigured"),
        Err(Error::UnknownDatabase)
    ));
    drop(source);
    let _replacement = budget.try_connection("source").unwrap();
}

#[test]
fn segment_permits_bound_residency_and_release_on_drop() {
    let budget = ThroughputBudget::new(limits(), Arc::new(ManualClock::default())).unwrap();
    let segment = budget.try_segment().unwrap();
    assert!(matches!(
        budget.try_segment(),
        Err(Error::SegmentsExhausted)
    ));
    drop(segment);
    let _replacement = budget.try_segment().unwrap();
}

#[test]
fn invalid_limits_fail_closed() {
    let clock = Arc::new(ManualClock::default());
    assert!(TokenBucket::new(rate(0, 1), clock.clone()).is_err());
    assert!(TokenBucket::new(rate(1, 0), clock.clone()).is_err());
    let mut config = limits();
    config.in_memory_segments = 0;
    assert!(ThroughputBudget::new(config, clock.clone()).is_err());
    let mut config = limits();
    config
        .connections
        .get_mut("source")
        .unwrap()
        .capture_reserved = 3;
    assert!(ThroughputBudget::new(config, clock).is_err());
}
