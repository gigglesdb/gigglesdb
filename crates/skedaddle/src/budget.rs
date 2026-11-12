//! Throughput admission, separate from coordinator local-state sizing.
//!
//! Callers retry denied work after `retry_after`; admission never sleeps or
//! holds a worker waiting for tokens. Capture has no token bucket. Its sole
//! pause input is the buffer's declared back-pressure signal.

use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::buffer::reader::Headroom;

const NANOS_PER_SECOND: u128 = 1_000_000_000;

/// Monotonic elapsed time in a clock-specific domain; wall time is unsuitable.
pub trait Clock: Send + Sync {
    fn now(&self) -> Duration;
}

pub struct SystemClock(Instant);

impl Default for SystemClock {
    fn default() -> Self {
        Self(Instant::now())
    }
}

impl Clock for SystemClock {
    fn now(&self) -> Duration {
        self.0.elapsed()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RateLimit {
    pub per_second: u64,
    /// Maximum request size and accumulated burst; starts full.
    pub burst: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    InvalidConfig(&'static str),
    RequestExceedsBurst { requested: u64, burst: u64 },
    Throttled { retry_after: Duration },
    UnknownDatabase,
    ConnectionsExhausted,
    SegmentsExhausted,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(reason) => write!(
                f,
                "Invalid throughput budget: {reason}; correct the configuration before admitting work."
            ),
            Self::RequestExceedsBurst { requested, burst } => write!(
                f,
                "Request of {requested} exceeds burst {burst}; reduce the read batch or increase its configured burst. Never split a transaction."
            ),
            Self::Throttled { retry_after } => write!(
                f,
                "Throughput tokens exhausted; retry this worker request after {retry_after:?}."
            ),
            Self::UnknownDatabase => f.write_str(
                "Database has no connection budget; configure its cap and capture reservation before opening connections."
            ),
            Self::ConnectionsExhausted => f.write_str(
                "Worker connection cap reached; retry after an existing worker connection closes; preserve capture reservations."
            ),
            Self::SegmentsExhausted => f.write_str(
                "Resident segment cap reached; release a processed segment before loading another."
            ),
        }
    }
}

impl std::error::Error for Error {}

struct Bucket {
    limit: RateLimit,
    // Integer token-nanoseconds preserve fractional credit across small ticks.
    credit: u128,
    last: Duration,
}

impl Bucket {
    fn new(limit: RateLimit, now: Duration) -> Result<Self, Error> {
        if limit.per_second == 0 || limit.burst == 0 {
            return Err(Error::InvalidConfig("rate and burst must be positive"));
        }
        Ok(Self {
            limit,
            credit: u128::from(limit.burst) * NANOS_PER_SECOND,
            last: now,
        })
    }

    fn check(&mut self, amount: u64, now: Duration) -> Result<(), Error> {
        // A faulty/backwards clock cannot mint credit a second time.
        let elapsed = now.saturating_sub(self.last);
        self.last = self.last.max(now);
        self.credit = self
            .credit
            .saturating_add(
                elapsed
                    .as_nanos()
                    .saturating_mul(self.limit.per_second.into()),
            )
            .min(u128::from(self.limit.burst) * NANOS_PER_SECOND);
        if amount > self.limit.burst {
            return Err(Error::RequestExceedsBurst {
                requested: amount,
                burst: self.limit.burst,
            });
        }
        let required = u128::from(amount) * NANOS_PER_SECOND;
        if required > self.credit {
            let nanos = (required - self.credit).div_ceil(self.limit.per_second.into());
            return Err(Error::Throttled {
                retry_after: Duration::new(
                    (nanos / NANOS_PER_SECOND) as u64,
                    (nanos % NANOS_PER_SECOND) as u32,
                ),
            });
        }
        Ok(())
    }

    fn debit(&mut self, amount: u64) {
        self.credit -= u128::from(amount) * NANOS_PER_SECOND;
    }
}

pub struct TokenBucket {
    clock: Arc<dyn Clock>,
    bucket: Bucket,
}

impl TokenBucket {
    pub fn new(limit: RateLimit, clock: Arc<dyn Clock>) -> Result<Self, Error> {
        Ok(Self {
            bucket: Bucket::new(limit, clock.now())?,
            clock,
        })
    }

    pub fn try_take(&mut self, amount: u64) -> Result<(), Error> {
        self.bucket.check(amount, self.clock.now())?;
        self.bucket.debit(amount);
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ReadLimits {
    pub rows: RateLimit,
    pub bytes: RateLimit,
}

struct ReadBuckets {
    rows: Bucket,
    bytes: Bucket,
}

impl ReadBuckets {
    fn new(limits: ReadLimits, now: Duration) -> Result<Self, Error> {
        Ok(Self {
            rows: Bucket::new(limits.rows, now)?,
            bytes: Bucket::new(limits.bytes, now)?,
        })
    }

    fn try_take(&mut self, rows: u64, bytes: u64, now: Duration) -> Result<(), Error> {
        let r = self.rows.check(rows, now);
        let b = self.bytes.check(bytes, now);
        // A denied two-dimensional request consumes neither bucket. Return the
        // longer delay so a caller need not spin between the two limits.
        match (r, b) {
            (Ok(()), Ok(())) => {
                self.rows.debit(rows);
                self.bytes.debit(bytes);
                Ok(())
            }
            (
                Err(Error::Throttled { retry_after: r }),
                Err(Error::Throttled { retry_after: b }),
            ) => Err(Error::Throttled {
                retry_after: r.max(b),
            }),
            (Err(e @ Error::RequestExceedsBurst { .. }), _)
            | (_, Err(e @ Error::RequestExceedsBurst { .. })) => Err(e),
            (Err(e), _) | (_, Err(e)) => Err(e),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ConnectionLimit {
    pub total: u32,
    /// Dedicated, preallocated capture connections. Copy/verify/apply cannot
    /// borrow these, even while idle. Include them in the total DB cap.
    pub capture_reserved: u32,
}

pub struct Limits {
    pub chunk_reads: ReadLimits,
    pub verification_reads: ReadLimits,
    pub applier_transactions: RateLimit,
    pub connections: BTreeMap<String, ConnectionLimit>,
    pub in_memory_segments: u32,
}

struct Pool {
    limit: u32,
    used: Mutex<u32>,
}

impl Pool {
    fn try_acquire(self: &Arc<Self>, exhausted: Error) -> Result<Permit, Error> {
        let mut used = self.used.lock().unwrap_or_else(|e| e.into_inner());
        if *used >= self.limit {
            return Err(exhausted);
        }
        *used += 1;
        Ok(Permit(self.clone()))
    }
}

/// Releasing/dropping a connection or segment releases its concurrency slot.
/// Keep the permit alive for the entire lifetime of the admitted resource.
pub struct Permit(Arc<Pool>);

impl Drop for Permit {
    fn drop(&mut self) {
        *self.0.used.lock().unwrap_or_else(|e| e.into_inner()) -= 1;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CaptureAdmission {
    Ready,
    BufferBackpressure,
}

pub struct ThroughputBudget {
    clock: Arc<dyn Clock>,
    chunks: ReadBuckets,
    verification: ReadBuckets,
    applier: Bucket,
    connections: BTreeMap<String, Arc<Pool>>,
    segments: Arc<Pool>,
}

impl ThroughputBudget {
    pub fn new(limits: Limits, clock: Arc<dyn Clock>) -> Result<Self, Error> {
        if limits.in_memory_segments == 0 {
            return Err(Error::InvalidConfig("segment cap must be positive"));
        }
        let mut connections = BTreeMap::new();
        for (database, limit) in limits.connections {
            if database.is_empty() || limit.total == 0 || limit.capture_reserved > limit.total {
                return Err(Error::InvalidConfig(
                    "invalid database connection reservation",
                ));
            }
            connections.insert(
                database,
                Arc::new(Pool {
                    limit: limit.total - limit.capture_reserved,
                    used: Mutex::new(0),
                }),
            );
        }
        let now = clock.now();
        Ok(Self {
            chunks: ReadBuckets::new(limits.chunk_reads, now)?,
            verification: ReadBuckets::new(limits.verification_reads, now)?,
            applier: Bucket::new(limits.applier_transactions, now)?,
            clock,
            connections,
            segments: Arc::new(Pool {
                limit: limits.in_memory_segments,
                used: Mutex::new(0),
            }),
        })
    }

    /// Refresh/subscribe to buffer Headroom before calling. This is a decision,
    /// not a replacement for `Backpressure::try_admit` capacity reservation.
    /// No token, connection lease, or copy/verify segment cap is consulted.
    pub fn capture_admission(&self, headroom: &Headroom) -> CaptureAdmission {
        if headroom.paused {
            CaptureAdmission::BufferBackpressure
        } else {
            CaptureAdmission::Ready
        }
    }

    pub fn try_chunk_read(&mut self, rows: u64, bytes: u64) -> Result<(), Error> {
        self.chunks.try_take(rows, bytes, self.clock.now())
    }

    pub fn try_verification_read(&mut self, rows: u64, bytes: u64) -> Result<(), Error> {
        self.verification.try_take(rows, bytes, self.clock.now())
    }

    pub fn try_apply(&mut self, transactions: u64) -> Result<(), Error> {
        self.applier.check(transactions, self.clock.now())?;
        self.applier.debit(transactions);
        Ok(())
    }

    /// Non-capture connections only; dedicated capture connections are reserved
    /// at configuration time and never enter the throttled worker pool.
    pub fn try_connection(&self, database: &str) -> Result<Permit, Error> {
        self.connections
            .get(database)
            .ok_or(Error::UnknownDatabase)?
            .try_acquire(Error::ConnectionsExhausted)
    }

    /// Bound resident copy/verify/replay segments. Capture spools through the
    /// file-backed buffer writer, governed by its own declared capacity bounds.
    pub fn try_segment(&self) -> Result<Permit, Error> {
        self.segments.try_acquire(Error::SegmentsExhausted)
    }
}
