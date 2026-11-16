//! Read service (GD-0075 R5-33; design.md 3 `ReadPage`, 4a O3/O6).
use std::ops::Bound;
use std::time::Instant;

use super::{RangeEngine, ts, txn_error};
use crate::storage::engine::{codec, hlc::Ts, kv::KvRead};
use crate::storage::range::keys::{cf_interval, is_versioned, keyspace_of};
use crate::storage::range_api::{RangeError, ReadDirection, ReadPage, ReadRequest};

type Raw = Bound<Vec<u8>>;

/// How far a read timestamp may be ahead of this node's wall clock (oracle timestamps are
/// `ms << 16 | logical` refilled from the system leader's clock).
const MAX_AHEAD_MS: u64 = 10 * 60 * 1000;

/// The tighter of two lower bounds.
fn max_start(a: Raw, b: Raw) -> Raw {
    match (&a, &b) {
        (Bound::Unbounded, _) => b,
        (_, Bound::Unbounded) => a,
        (Bound::Included(x) | Bound::Excluded(x), Bound::Included(y) | Bound::Excluded(y)) => {
            if x > y || (x == y && matches!(a, Bound::Excluded(_))) {
                a
            } else {
                b
            }
        }
    }
}

/// The tighter of two upper bounds.
fn min_end(a: Raw, b: Raw) -> Raw {
    match (&a, &b) {
        (Bound::Unbounded, _) => b,
        (_, Bound::Unbounded) => a,
        (Bound::Included(x) | Bound::Excluded(x), Bound::Included(y) | Bound::Excluded(y)) => {
            if x < y || (x == y && matches!(a, Bound::Excluded(_))) {
                a
            } else {
                b
            }
        }
    }
}

fn inside(key: &[u8], start: &Raw, end: &Raw) -> bool {
    use std::ops::RangeBounds;
    (
        start.as_ref().map(Vec::as_slice),
        end.as_ref().map(Vec::as_slice),
    )
        .contains(&key)
}

/// One page of a read: stops at the first pair that reaches the byte budget.
struct Page {
    pairs: Vec<(Vec<u8>, Vec<u8>)>,
    bytes: usize,
    limit: usize,
    dir: ReadDirection,
    resume: Option<Raw>,
}
impl Page {
    /// Adds a pair; true when the page is full (then `resume` continues after it). `user` is
    /// the pair's user key in a versioned space.
    fn push(&mut self, key: Vec<u8>, value: Vec<u8>, user: Option<&[u8]>) -> bool {
        self.bytes += key.len() + value.len();
        let resume = match (user, self.dir) {
            (None, _) => Bound::Excluded(key.clone()),
            (Some(user), ReadDirection::Forward) => {
                Bound::Included(codec::user_key_upper_bound(user))
            }
            (Some(user), ReadDirection::Reverse) => {
                Bound::Excluded(codec::user_key_seek_at(user, Ts(u64::MAX)))
            }
        };
        self.pairs.push((key, value));
        if self.bytes >= self.limit {
            self.resume = Some(resume);
            return true;
        }
        false
    }
}

impl RangeEngine {
    /// O6: raise the served floor to T and wait for reservations at or below T (gate), wait
    /// for prepared records with p <= T that write inside the span, then read MVCC at T.
    pub(super) fn read_page(&self, req: ReadRequest) -> Result<ReadPage, RangeError> {
        self.check_open()?;
        self.check_epoch(req.epoch)?;
        let range_end = match req.dir {
            ReadDirection::Forward => self.descriptor.end.clone(),
            ReadDirection::Reverse => Some(self.descriptor.start.clone()),
        };
        let empty = ReadPage {
            pairs: Vec::new(),
            resume: None,
            range_end: range_end.clone(),
        };
        let Some((lo, hi)) = cf_interval(&self.descriptor, req.space) else {
            return Ok(empty);
        };
        // A timestamp far beyond the clock (for example u64::MAX as "latest") would raise the
        // served floor for good and stop every later commit here (E67 S4).
        let horizon = (crate::storage::engine::commit::Clock::now_ms(
            &crate::storage::engine::commit::SystemClock,
        ) + MAX_AHEAD_MS)
            << 16;
        if req.ts.0 > horizon {
            return Err(RangeError::Corrupt(
                "read timestamp is ahead of the timestamp oracle".into(),
            ));
        }
        let start = max_start(req.start, lo);
        let end = min_end(req.end, hi);
        let at = ts(req.ts);
        let ks = keyspace_of(req.space);
        let versioned = is_versioned(req.space);
        let deadline = Instant::now() + self.config.read_wait;
        if !self.gate.serve(at, Some(deadline)) {
            return Err(RangeError::Unavailable(
                "a commit at or below the read ts is in flight".into(),
            ));
        }
        {
            let mut state = self.state();
            loop {
                self.check_open()?;
                let blocked = state.prepared.values().any(|record| {
                    record.at <= at
                        && record.writes.keys().any(|(wks, key)| {
                            *wks == ks
                                && inside(
                                    &if versioned {
                                        codec::user_key_seek_at(key, at)
                                    } else {
                                        key.clone()
                                    },
                                    &start,
                                    &end,
                                )
                        })
                });
                if !blocked {
                    break;
                }
                let now = Instant::now();
                if now >= deadline {
                    // R5-53 (Track D) resolves a stale prepared record through its home.
                    return Err(RangeError::Unavailable(
                        "prepared transaction unresolved".into(),
                    ));
                }
                state = self
                    .changed
                    .wait_timeout(state, deadline - now)
                    .unwrap_or_else(|p| p.into_inner())
                    .0;
            }
        }
        let dir = match req.dir {
            ReadDirection::Forward => crate::storage::engine::kv::Direction::Forward,
            ReadDirection::Reverse => crate::storage::engine::kv::Direction::Reverse,
        };
        let iter = KvRead::range(
            self.kv.as_ref(),
            ks,
            start.as_ref().map(Vec::as_slice),
            end.as_ref().map(Vec::as_slice),
            dir,
        )
        .map_err(|e| txn_error(e.into()))?;
        let mut page = Page {
            pairs: Vec::new(),
            bytes: 0,
            limit: req.page_bytes.max(1),
            dir: req.dir,
            resume: None,
        };
        if !versioned {
            for item in iter {
                let (key, value) = item.map_err(|e| txn_error(e.into()))?;
                if page.push(key, value, None) {
                    break;
                }
            }
        } else {
            // Raw versions of one user key are adjacent: newest first going forward, oldest
            // first going backward. Emit, per user key, its newest version at or below T.
            let mut current: Option<Vec<u8>> = None;
            let mut taken = false;
            let mut candidate: Option<(Vec<u8>, Vec<u8>)> = None;
            let mut full = false;
            for item in iter {
                let (key, value) = item.map_err(|e| txn_error(e.into()))?;
                let (user, version) = codec::split_versioned(&key)
                    .map_err(|e| RangeError::Corrupt(format!("{e:?}")))?;
                let user = user.to_vec();
                if current.as_deref() != Some(user.as_slice()) {
                    if let (Some(prev), Some((k, v))) = (current.take(), candidate.take())
                        && page.push(k, v, Some(&prev))
                    {
                        full = true;
                        break;
                    }
                    current = Some(user.clone());
                    taken = false;
                }
                if version > at {
                    continue;
                }
                match req.dir {
                    ReadDirection::Forward if !taken => {
                        taken = true;
                        if page.push(key, value, Some(&user)) {
                            full = true;
                            break;
                        }
                    }
                    ReadDirection::Forward => {}
                    ReadDirection::Reverse => candidate = Some((key, value)),
                }
            }
            if !full && let (Some(prev), Some((k, v))) = (current, candidate) {
                page.push(k, v, Some(&prev));
            }
        }
        let Page { pairs, resume, .. } = page;
        self.check_open()?;
        Ok(ReadPage {
            pairs,
            resume,
            range_end,
        })
    }
}
