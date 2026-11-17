//! Read service (GD-0075 R5-33; design.md 3 `ReadPage`, 4a O3/O6).
use std::ops::Bound;
use std::time::Instant;

use super::reclaim::MAX_CLOCK_OFFSET;
use super::{RangeEngine, ts, txn_error};
use crate::storage::engine::{codec, hlc::Ts, kv::KvRead};
use crate::storage::range::keys::{cf_interval, is_versioned, keyspace_of};
use crate::storage::range_api::{
    ClusterTxnId, RangeError, RangeId, ReadDirection, ReadPage, ReadRequest, range_key,
};

type Raw = Bound<Vec<u8>>;

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
    /// O6: raise the served floor to T and wait for reservations at or below T (gate), read
    /// MVCC at T, and wait for prepared records with p <= T that write inside the part of the
    /// span the page covers, reading again after they resolve (P8-0).
    pub(super) fn read_page(&self, req: ReadRequest) -> Result<ReadPage, RangeError> {
        self.check_open()?;
        // A stale reply names the range holding the key the gateway routes this page by:
        // the start of a forward page, the end of a reverse one (review S4; cluster/read.rs
        // `lower_route`, `upper_route`).
        let route_key = match (req.dir, &req.start, &req.end) {
            (ReadDirection::Forward, Bound::Included(key) | Bound::Excluded(key), _)
            | (ReadDirection::Reverse, _, Bound::Included(key) | Bound::Excluded(key)) => {
                range_key(req.space, key)
            }
            (ReadDirection::Forward, Bound::Unbounded, _) => range_key(req.space, &[]),
            // The space's end: the first key past every key of the space.
            (ReadDirection::Reverse, _, Bound::Unbounded) => vec![req.space.tag() + 1],
        };
        self.check_epoch(req.epoch, Some(&route_key))?;
        self.check_stored(Some(&route_key))?;
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
        ) + MAX_CLOCK_OFFSET)
            << 16;
        if req.ts.0 > horizon {
            return Err(RangeError::Corrupt(
                "read timestamp is ahead of the timestamp oracle".into(),
            ));
        }
        // R5-101: data retired before now - gc_ttl may be reclaimed; never serve below it.
        if req.ts.0 < super::reclaim::floor_now(self.config.gc_ttl) {
            return Err(RangeError::Conflict(format!(
                "snapshot older than the GC TTL ({:?})",
                self.config.gc_ttl
            )));
        }
        let start = max_start(req.start.clone(), lo);
        let end = min_end(req.end.clone(), hi);
        let at = ts(req.ts);
        let ks = keyspace_of(req.space);
        let versioned = is_versioned(req.space);
        let deadline = Instant::now() + self.config.read_wait;
        if !self.gate.serve(at, Some(deadline)) {
            return Err(RangeError::Unavailable(
                "a commit at or below the read ts is in flight".into(),
            ));
        }
        let mut next_resolve = Instant::now() + self.config.resolve_after;
        loop {
            // P8-0: wait only on prepared writes inside what this page actually read. Records
            // with p <= T can only leave (the gate above admits no new ones), so the set taken
            // before reading still covers every write a finalize could add under the page.
            let pending = self.prepared_at_or_below(at, ks, versioned, &start, &end);
            let page = self.scan_page(&req, ks, versioned, &start, &end)?;
            let (from, to) = covered(&start, &end, page.resume.as_ref(), req.dir);
            let blocking: Vec<ClusterTxnId> = pending
                .into_iter()
                .filter(|(_, positions)| positions.iter().any(|pos| inside(pos, &from, &to)))
                .map(|(txn, _)| txn)
                .collect();
            if blocking.is_empty() {
                self.check_open()?;
                return Ok(ReadPage {
                    pairs: page.pairs,
                    resume: page.resume,
                    range_end,
                });
            }
            self.await_prepared(&blocking, deadline, &mut next_resolve)?;
        }
    }

    /// Prepared records with p <= `at` that write inside [start, end], each with the raw
    /// positions (seek keys at `at` in a versioned space) of those writes.
    fn prepared_at_or_below(
        &self,
        at: Ts,
        ks: crate::storage::engine::kv::Keyspace,
        versioned: bool,
        start: &Raw,
        end: &Raw,
    ) -> Vec<(ClusterTxnId, Vec<Vec<u8>>)> {
        let state = self.state();
        state
            .prepared
            .iter()
            .filter(|(_, record)| record.at <= at)
            .filter_map(|(txn, record)| {
                let positions: Vec<Vec<u8>> = record
                    .writes
                    .keys()
                    .filter(|(wks, _)| *wks == ks)
                    .map(|(_, key)| {
                        if versioned {
                            codec::user_key_seek_at(key, at)
                        } else {
                            key.clone()
                        }
                    })
                    .filter(|pos| inside(pos, start, end))
                    .collect();
                (!positions.is_empty()).then_some((*txn, positions))
            })
            .collect()
    }

    /// O6: waits until none of `txns` holds a prepared record here, resolving them through
    /// their homes (R5-53) at most every `resolve_after`; Unavailable at `deadline`.
    fn await_prepared(
        &self,
        txns: &[ClusterTxnId],
        deadline: Instant,
        next_resolve: &mut Instant,
    ) -> Result<(), RangeError> {
        loop {
            // Taken outside `self.state`: dropping the last reference runs the client's Drop.
            let client = self.client();
            let blockers = {
                let mut state = self.state();
                loop {
                    self.check_open()?;
                    let blockers: Vec<(ClusterTxnId, RangeId)> = txns
                        .iter()
                        .filter_map(|txn| state.prepared.get(txn).map(|r| (*txn, r.home)))
                        .collect();
                    if blockers.is_empty() {
                        return Ok(());
                    }
                    let now = Instant::now();
                    if now >= deadline {
                        return Err(RangeError::Unavailable(
                            "prepared transaction unresolved".into(),
                        ));
                    }
                    if client.is_some() && now >= *next_resolve {
                        break blockers;
                    }
                    let until = match client {
                        Some(_) => deadline.min(*next_resolve),
                        None => deadline,
                    };
                    state = self
                        .changed
                        .wait_timeout(state, until - now)
                        .unwrap_or_else(|p| p.into_inner())
                        .0;
                }
            };
            let Some(client) = client else {
                continue;
            };
            // R5-53: Finalized removes the record (check again); Pending or an error waits,
            // asking again at most every `resolve_after`.
            self.resolve_blockers(&*client, &blockers);
            *next_resolve = Instant::now() + self.config.resolve_after;
        }
    }

    /// One page of MVCC at `req.ts` over [start, end] (already clipped to this range).
    fn scan_page(
        &self,
        req: &ReadRequest,
        ks: crate::storage::engine::kv::Keyspace,
        versioned: bool,
        start: &Raw,
        end: &Raw,
    ) -> Result<Page, RangeError> {
        let at = ts(req.ts);
        let dir = match req.dir {
            ReadDirection::Forward => crate::storage::engine::kv::Direction::Forward,
            ReadDirection::Reverse => crate::storage::engine::kv::Direction::Reverse,
        };
        // Waits may have crossed the GC floor; check again before taking the KV snapshot.
        if req.ts.0 < super::reclaim::floor_now(self.config.gc_ttl) {
            return Err(RangeError::Conflict(format!(
                "snapshot older than the GC TTL ({:?})",
                self.config.gc_ttl
            )));
        }
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
        Ok(page)
    }
}

/// The part of [start, end] a page read: up to (Forward) or down to (Reverse) its resume
/// point, which the next page starts from; all of it when the page ends the span.
fn covered(start: &Raw, end: &Raw, resume: Option<&Raw>, dir: ReadDirection) -> (Raw, Raw) {
    let flip = |bound: &Raw| match bound {
        Bound::Included(key) => Bound::Excluded(key.clone()),
        Bound::Excluded(key) => Bound::Included(key.clone()),
        Bound::Unbounded => Bound::Unbounded,
    };
    match (resume, dir) {
        (None, _) => (start.clone(), end.clone()),
        (Some(next), ReadDirection::Forward) => (start.clone(), flip(next)),
        (Some(next), ReadDirection::Reverse) => (flip(next), end.clone()),
    }
}
