//! Gateway reads routed to range leaders (GD-0075 R5-41; design.md sections 3 and 4a).
//!
//! `RoutedKvRead` is a `KvRead` over the raw stored pairs of one keyspace at a fixed read
//! timestamp, as `RangeClient::read` returns them: for versioned keyspaces only the newest
//! version at or below that timestamp of each user key, tombstones included. MVCC readers
//! (`mvcc::VersionedRange`, `mvcc::read_at`) over it at the same timestamp therefore see exactly
//! what they see over the full local store.
//!
//! Scans prefetch the first pages of the next K ranges in parallel (R5-44, `prefetch.rs`): at
//! most K pages per scan are queued, a memory bound of about K * page_bytes beyond the page being
//! consumed. K = 4 by default; `with_prefetch(0)` reads strictly sequentially.
use std::{
    ops::Bound,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use super::txn::FAILOVER_WINDOW;

use crate::storage::{
    engine::{
        catalog::GENERATION_KEY,
        hlc::Ts,
        kv::{Direction, Keyspace, KvError, KvIter, KvPair, KvRead},
    },
    range_api::{
        RangeClient, RangeDescriptor, RangeError, RangeSpace, ReadDirection, ReadPage, ReadRequest,
        Timestamp, cluster_record_key, range_key, split_range_key,
    },
};

#[cfg(test)]
pub(crate) mod fake;
mod prefetch;
#[cfg(test)]
mod tests;

/// NotLeader / Unavailable: at least this many attempts per request, with doubling backoff
/// (about 0.9 s, a new split child's first election, R5-84), and at least `FAILOVER_WINDOW`
/// (a leader's node dying: `Transient`).
const TRANSIENT_ATTEMPTS: u32 = 16;
const FIRST_BACKOFF: Duration = Duration::from_millis(1);
const MAX_BACKOFF: Duration = Duration::from_millis(100);

/// Paces retries of NotLeader / Unavailable: at least `TRANSIENT_ATTEMPTS` tries and for at
/// least `FAILOVER_WINDOW`, so a read outlasts a leader's node dying and a new election.
struct Transient {
    attempt: u32,
    backoff: Duration,
    started: Instant,
}

impl Transient {
    fn new() -> Self {
        Self {
            attempt: 1,
            backoff: FIRST_BACKOFF,
            started: Instant::now(),
        }
    }

    /// Pauses before another try; false once both the attempts and the window are spent.
    fn again(&mut self) -> bool {
        if self.attempt >= TRANSIENT_ATTEMPTS && self.started.elapsed() >= FAILOVER_WINDOW {
            return false;
        }
        self.attempt += 1;
        thread::sleep(self.backoff);
        self.backoff = (self.backoff * 2).min(MAX_BACKOFF);
        true
    }
}

/// The descriptor of the range containing `route`. A cache miss reads the directory on the
/// system range, whose leader may be failing over too: paced like a range request.
fn descriptor(client: &dyn RangeClient, route: &[u8]) -> Result<RangeDescriptor, KvError> {
    let mut transient = Transient::new();
    loop {
        match client.descriptor_for(route) {
            Err(e @ (RangeError::NotLeader { .. } | RangeError::Unavailable(_))) => {
                if !transient.again() {
                    return Err(kv_error(e));
                }
            }
            result => return result.map_err(kv_error),
        }
    }
}
/// Route changes (StaleEpoch) tolerated while fetching one page.
const STALE_ATTEMPTS: u32 = 64;
/// Ranges whose first page a scan fetches ahead (R5-44).
const DEFAULT_PREFETCH: usize = 4;

/// The range keyspace of a local keyspace; Meta is never in a range.
#[allow(dead_code)]
pub(crate) fn range_space(ks: Keyspace) -> Option<RangeSpace> {
    match ks {
        Keyspace::Meta => None,
        Keyspace::Catalog => Some(RangeSpace::Catalog),
        Keyspace::Kernel => Some(RangeSpace::Kernel),
        Keyspace::Seq => Some(RangeSpace::Seq),
        Keyspace::Gc => Some(RangeSpace::Gc),
        Keyspace::Rows => Some(RangeSpace::Rows),
        Keyspace::Index => Some(RangeSpace::Index),
    }
}

#[allow(dead_code)]
pub(crate) fn kv_error(e: RangeError) -> KvError {
    match e {
        RangeError::Corrupt(message) => KvError::Corrupt(message),
        other => KvError::Io(other.to_string()),
    }
}

/// Raw reads of every non-Meta keyspace at one fixed timestamp, routed through a `RangeClient`.
#[allow(dead_code)]
pub(crate) struct RoutedKvRead {
    client: Arc<dyn RangeClient>,
    ts: Timestamp,
    page_bytes: usize,
    /// K: ranges ahead whose first page a scan fetches in parallel; 0 = sequential.
    prefetch: usize,
}

#[allow(dead_code)]
impl RoutedKvRead {
    pub(crate) fn new(client: Arc<dyn RangeClient>, ts: Ts, page_bytes: usize) -> Self {
        Self {
            client,
            ts: Timestamp(ts.0),
            page_bytes,
            prefetch: DEFAULT_PREFETCH,
        }
    }

    /// Scans fetch the first pages of up to `k` ranges ahead in parallel, holding at most
    /// `k * page_bytes` (about) of prefetched pages; 0 reads one page at a time.
    pub(crate) fn with_prefetch(mut self, k: usize) -> Self {
        self.prefetch = k;
        self
    }

    fn space(ks: Keyspace) -> Result<RangeSpace, KvError> {
        range_space(ks)
            .ok_or_else(|| KvError::Corrupt("the meta keyspace is not range-routed".into()))
    }

    /// One page from the range whose descriptor contains `route`, following route changes.
    fn page(
        &self,
        space: RangeSpace,
        route: &[u8],
        start: &Bound<Vec<u8>>,
        end: &Bound<Vec<u8>>,
        dir: ReadDirection,
    ) -> Result<ReadPage, KvError> {
        for _ in 0..STALE_ATTEMPTS {
            let desc = descriptor(&*self.client, route)?;
            if let Some(page) = self.send(&desc, space, start, end, dir)? {
                return Ok(page);
            }
        }
        Err(route_unsettled())
    }

    /// One request to `desc`. `None` after a StaleEpoch (the route has been invalidated).
    fn send(
        &self,
        desc: &RangeDescriptor,
        space: RangeSpace,
        start: &Bound<Vec<u8>>,
        end: &Bound<Vec<u8>>,
        dir: ReadDirection,
    ) -> Result<Option<ReadPage>, KvError> {
        let mut transient = Transient::new();
        loop {
            match self.client.read(self.request(desc, space, start, end, dir)) {
                Ok(page) => return Ok(Some(page)),
                Err(RangeError::StaleEpoch { .. }) => {
                    self.client.invalidate(desc);
                    return Ok(None);
                }
                Err(e @ (RangeError::NotLeader { .. } | RangeError::Unavailable(_))) => {
                    self.client.invalidate(desc);
                    if !transient.again() {
                        return Err(kv_error(e));
                    }
                    // R5-84: a routed client picks replicas from its cached descriptor, so look
                    // the range up again (a new split child may simply have no leader yet).
                    // Only a newer epoch at the key is a route change (txn.rs `reroute`'s rule;
                    // review S2): an older or unknown record there (a split child not yet
                    // published to the directory) keeps `desc`, learned back so the by-id
                    // request still routes, and the backoff goes on.
                    match self.client.descriptor_for(&desc.start) {
                        Ok(current) if current.epoch > desc.epoch => return Ok(None),
                        Ok(current) if current == *desc => {}
                        _ => self.client.learn(desc),
                    }
                }
                Err(e) => return Err(kv_error(e)),
            }
        }
    }

    fn request(
        &self,
        desc: &RangeDescriptor,
        space: RangeSpace,
        start: &Bound<Vec<u8>>,
        end: &Bound<Vec<u8>>,
        dir: ReadDirection,
    ) -> ReadRequest {
        ReadRequest {
            range: desc.id,
            epoch: desc.epoch,
            space,
            start: start.clone(),
            end: end.clone(),
            dir,
            ts: self.ts,
            page_bytes: self.page_bytes,
        }
    }
}

/// Catalog keys with these leading tags are cluster records in the system range (0xF0
/// directory, 0xF1 `gw/`, 0xF2 `ddl/`; design.md Changes 2026-11-16T10:40Z (iii); 0xF3 `XACT/`,
/// R5-55), not catalog objects: catalog loads through a gateway never see them. The
/// catalog-generation record (0xF4, P7-1) stays readable: gateways read it to notice DDL
/// committed elsewhere, and catalog loads skip it themselves.
fn directory_key(ks: Keyspace, key: &[u8]) -> bool {
    ks == Keyspace::Catalog && cluster_record_key(key) && !key.starts_with(GENERATION_KEY)
}

fn route_unsettled() -> KvError {
    KvError::Io("range route did not settle after repeated stale epochs".into())
}

/// Range key where a scan from `start` begins.
fn lower_route(space: RangeSpace, start: &Bound<Vec<u8>>) -> Vec<u8> {
    match start {
        Bound::Included(key) | Bound::Excluded(key) => range_key(space, key),
        Bound::Unbounded => range_key(space, &[]),
    }
}

/// Range key where a reverse scan at `end` continues.
fn upper_route(space: RangeSpace, end: &Bound<Vec<u8>>) -> Vec<u8> {
    match end {
        Bound::Included(key) | Bound::Excluded(key) => range_key(space, key),
        Bound::Unbounded => vec![space.tag() + 1],
    }
}

/// Whether a range starting at `range_start` holds no key of `space` within `end`.
pub(super) fn past_end(space: RangeSpace, range_start: &[u8], end: &Bound<Vec<u8>>) -> bool {
    match end {
        Bound::Included(key) => range_start > range_key(space, key).as_slice(),
        Bound::Excluded(key) => range_start >= range_key(space, key).as_slice(),
        Bound::Unbounded => range_start >= [space.tag() + 1].as_slice(),
    }
}

/// The user key of `space` a range bound stands for, if the bound lies in that keyspace.
fn space_key(space: RangeSpace, range_bound: &[u8]) -> Option<Vec<u8>> {
    match split_range_key(range_bound) {
        Some((found, key)) if found == space => Some(key.to_vec()),
        _ => None,
    }
}

/// Lazy paged scan; one page buffered plus at most K prefetched first pages.
struct Scan<'a> {
    reader: &'a RoutedKvRead,
    space: RangeSpace,
    dir: ReadDirection,
    /// Forward: the current position. Reverse: the user's start bound.
    start: Bound<Vec<u8>>,
    /// Forward: the user's end bound. Reverse: the current position.
    end: Bound<Vec<u8>>,
    /// Reverse only: descriptors covering [start, end] in key order; the last one is next.
    plan: Vec<RangeDescriptor>,
    page: std::vec::IntoIter<KvPair>,
    done: bool,
    /// None when prefetch is off.
    ahead: Option<prefetch::Prefetcher>,
    /// The next page is the first one read from a range.
    fresh: bool,
}

impl Scan<'_> {
    fn fetch(&mut self) -> Result<Vec<KvPair>, KvError> {
        match self.dir {
            ReadDirection::Forward => self.fetch_forward(),
            ReadDirection::Reverse => self.fetch_reverse(),
        }
    }

    fn fetch_forward(&mut self) -> Result<Vec<KvPair>, KvError> {
        let route = lower_route(self.space, &self.start);
        let mut prefetched = None;
        if self.fresh
            && let Some(ahead) = self.ahead.as_mut()
        {
            let taken = ahead.take(&self.start, &route);
            ahead.top_up_forward(self.reader, self.space, &route, &self.end);
            if let Some(taken) = taken {
                prefetched = taken.wait(&*self.reader.client).map_err(kv_error)?;
            }
        }
        let page = match prefetched {
            Some(page) => page,
            None => self.reader.page(
                self.space,
                &route,
                &self.start,
                &self.end,
                ReadDirection::Forward,
            )?,
        };
        self.fresh = page.resume.is_none();
        match page.resume {
            Some(resume) => self.start = resume,
            // The next range starts at the serving descriptor's end.
            None => match page.range_end {
                Some(next) if !past_end(self.space, &next, &self.end) => {
                    match space_key(self.space, &next) {
                        Some(key) => self.start = Bound::Included(key),
                        None => self.done = true,
                    }
                }
                _ => self.done = true,
            },
        }
        Ok(page.pairs)
    }

    fn fetch_reverse(&mut self) -> Result<Vec<KvPair>, KvError> {
        if self.fresh && self.ahead.is_some() {
            let route = upper_route(self.space, &self.end);
            let taken = self
                .ahead
                .as_mut()
                .and_then(|ahead| ahead.take(&self.end, &route));
            if self.plan.is_empty() {
                self.plan = self.resolve()?;
            }
            if let Some(ahead) = self.ahead.as_mut() {
                ahead.top_up_reverse(self.reader, self.space, &route, &self.start, &self.plan);
            }
            if let Some(taken) = taken
                && let Some(page) = taken.wait(&*self.reader.client).map_err(kv_error)?
            {
                return Ok(self.advance_reverse(page));
            }
        }
        for _ in 0..STALE_ATTEMPTS {
            if self.plan.is_empty() {
                self.plan = self.resolve()?;
            }
            let desc = self.plan.last().cloned().expect("resolve returns a range");
            let Some(page) = self.reader.send(
                &desc,
                self.space,
                &self.start,
                &self.end,
                ReadDirection::Reverse,
            )?
            else {
                // Re-resolve from the current position.
                self.plan.clear();
                continue;
            };
            return Ok(self.advance_reverse(page));
        }
        Err(route_unsettled())
    }

    fn advance_reverse(&mut self, page: ReadPage) -> Vec<KvPair> {
        self.fresh = page.resume.is_none();
        match page.resume {
            Some(resume) => self.end = resume,
            // The previous range ends at the serving descriptor's start.
            None => match page.range_end {
                Some(prev) => {
                    // The serving range (and, after a prefetched page, any split of it).
                    while self.plan.last().is_some_and(|desc| desc.start >= prev) {
                        self.plan.pop();
                    }
                    match space_key(self.space, &prev) {
                        Some(key) if prev > lower_route(self.space, &self.start) => {
                            self.end = Bound::Excluded(key);
                        }
                        _ => self.done = true,
                    }
                }
                None => {
                    self.plan.pop();
                    self.done = true;
                }
            },
        }
        page.pairs
    }

    /// Descriptors covering [start, current end] by a forward walk of the route cache.
    fn resolve(&self) -> Result<Vec<RangeDescriptor>, KvError> {
        let mut key = lower_route(self.space, &self.start);
        let mut plan = Vec::new();
        loop {
            let desc = descriptor(&*self.reader.client, &key)?;
            let next = desc.end.clone();
            plan.push(desc);
            match next {
                Some(next) if !past_end(self.space, &next, &self.end) => {
                    if next <= key {
                        return Err(KvError::Corrupt("range route does not advance".into()));
                    }
                    key = next;
                }
                _ => return Ok(plan),
            }
        }
    }
}

impl Iterator for Scan<'_> {
    type Item = Result<KvPair, KvError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(pair) = self.page.next() {
                return Some(Ok(pair));
            }
            if self.done {
                return None;
            }
            match self.fetch() {
                Ok(pairs) => self.page = pairs.into_iter(),
                Err(e) => {
                    self.done = true;
                    return Some(Err(e));
                }
            }
        }
    }
}

impl KvRead for RoutedKvRead {
    fn get(&self, ks: Keyspace, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        let space = Self::space(ks)?;
        if directory_key(ks, key) {
            return Ok(None);
        }
        let bound = Bound::Included(key.to_vec());
        let page = self.page(
            space,
            &range_key(space, key),
            &bound,
            &bound,
            ReadDirection::Forward,
        )?;
        Ok(page
            .pairs
            .into_iter()
            .find(|(found, _)| found == key)
            .map(|(_, value)| value))
    }

    fn range(
        &self,
        ks: Keyspace,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        dir: Direction,
    ) -> Result<KvIter<'_>, KvError> {
        let space = Self::space(ks)?;
        let empty = match (start, end) {
            (Bound::Included(a), Bound::Included(b)) => a > b,
            (Bound::Included(a) | Bound::Excluded(a), Bound::Included(b) | Bound::Excluded(b)) => {
                a >= b
            }
            _ => false,
        };
        let dir = match dir {
            Direction::Forward => ReadDirection::Forward,
            Direction::Reverse => ReadDirection::Reverse,
        };
        let scan = Scan {
            reader: self,
            space,
            dir,
            start: start.map(<[u8]>::to_vec),
            end: end.map(<[u8]>::to_vec),
            plan: Vec::new(),
            page: Vec::new().into_iter(),
            done: empty,
            ahead: (self.prefetch > 0 && !empty)
                .then(|| prefetch::Prefetcher::new(Arc::clone(&self.client), dir, self.prefetch)),
            fresh: true,
        };
        if ks == Keyspace::Catalog {
            return Ok(Box::new(scan.filter(
                move |item| !matches!(item, Ok((key, _)) if directory_key(ks, key)),
            )));
        }
        Ok(Box::new(scan))
    }
}
