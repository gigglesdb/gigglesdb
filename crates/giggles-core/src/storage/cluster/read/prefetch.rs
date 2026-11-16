//! Parallel first-page prefetch for routed scans (GD-0075 R5-44).
//!
//! When a scan starts reading a range it also requests the FIRST page of each of the next K
//! ranges in scan order that intersect its bounds, one background thread and one
//! `RangeClient::read` call per page. The scan uses a prefetched page only when it reaches exactly
//! the position that page was requested from. A StaleEpoch, NotLeader or Unavailable reply, or a
//! page whose position the scan skipped (a split moved the boundaries), is discarded and the scan
//! reads that position sequentially; any other error is returned only when the scan reaches it.
//!
//! Memory bound: at most K pages are queued (in flight, or fetched and unconsumed) per scan, so
//! about K * page_bytes beyond the page being consumed. Dropping the scan sets a cancel flag:
//! threads that have not started their call skip it, in-flight calls finish and the result is
//! dropped. Nothing joins on drop.
use std::{
    collections::VecDeque,
    ops::Bound,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver},
    },
    thread,
};

use super::{RoutedKvRead, past_end, space_key};
use crate::storage::range_api::{
    RangeClient, RangeDescriptor, RangeError, RangeSpace, ReadDirection, ReadPage, ReadRequest,
};

/// One requested first page.
struct Pending {
    /// Range key of `at`.
    route: Vec<u8>,
    /// The bound the request starts from: Forward its start, Reverse its end.
    at: Bound<Vec<u8>>,
    desc: RangeDescriptor,
    reply: Receiver<Result<ReadPage, RangeError>>,
}

/// A prefetched page matched to the scan's position.
pub(super) struct Taken(Pending);

impl Taken {
    /// Blocks for the page. `None`: read this position sequentially.
    pub(super) fn wait(self, client: &dyn RangeClient) -> Result<Option<ReadPage>, RangeError> {
        match self.0.reply.recv() {
            Ok(Ok(page)) => Ok(Some(page)),
            Ok(Err(
                RangeError::StaleEpoch { .. }
                | RangeError::NotLeader { .. }
                | RangeError::Unavailable(_),
            )) => {
                client.invalidate(&self.0.desc);
                Ok(None)
            }
            Ok(Err(e)) => Err(e),
            // The thread did not deliver (it panicked).
            Err(_) => Ok(None),
        }
    }
}

/// Where the next prefetch walk starts.
enum Frontier {
    Unknown,
    /// Forward: range key of the first range not yet queued. Reverse: the lowest queued route.
    At(Vec<u8>),
    /// Forward: no further range intersects the scan.
    End,
}

pub(super) struct Prefetcher {
    client: Arc<dyn RangeClient>,
    dir: ReadDirection,
    limit: usize,
    queue: VecDeque<Pending>,
    frontier: Frontier,
    cancel: Arc<AtomicBool>,
}

impl Prefetcher {
    pub(super) fn new(client: Arc<dyn RangeClient>, dir: ReadDirection, limit: usize) -> Self {
        Self {
            client,
            dir,
            limit,
            queue: VecDeque::new(),
            frontier: Frontier::Unknown,
            cancel: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Discards queued pages at or behind the scan position `at` (range key `route`) and takes
    /// the one requested from exactly `at`, if any.
    pub(super) fn take(&mut self, at: &Bound<Vec<u8>>, route: &[u8]) -> Option<Taken> {
        while let Some(front) = self.queue.front() {
            if front.route == route && front.at == *at {
                return self.queue.pop_front().map(Taken);
            }
            let ahead = match self.dir {
                ReadDirection::Forward => front.route.as_slice() > route,
                ReadDirection::Reverse => front.route.as_slice() < route,
            };
            if ahead {
                return None;
            }
            self.queue.pop_front();
        }
        None
    }

    fn spawn(
        &mut self,
        desc: RangeDescriptor,
        req: ReadRequest,
        at: Bound<Vec<u8>>,
        route: Vec<u8>,
    ) -> bool {
        let (sent, reply) = mpsc::sync_channel(1);
        let client = Arc::clone(&self.client);
        let cancel = Arc::clone(&self.cancel);
        let spawned = thread::Builder::new()
            .name("routed-read-prefetch".into())
            .spawn(move || {
                if !cancel.load(Ordering::Acquire) {
                    // The receiver is gone when the scan was dropped or skipped this page.
                    let _ = sent.send(client.read(req));
                }
            });
        if spawned.is_err() {
            return false;
        }
        self.queue.push_back(Pending {
            route,
            at,
            desc,
            reply,
        });
        true
    }

    /// Forward: queues first pages of the ranges after the one containing `route`, walking the
    /// route cache, until K are queued or the walk leaves `end`. Walk failures only stop it.
    pub(super) fn top_up_forward(
        &mut self,
        reader: &RoutedKvRead,
        space: RangeSpace,
        route: &[u8],
        end: &Bound<Vec<u8>>,
    ) {
        let mut key = match &self.frontier {
            Frontier::End => return,
            Frontier::At(key) if key.as_slice() > route => key.clone(),
            _ => match self.client.descriptor_for(route) {
                Ok(RangeDescriptor {
                    end: Some(next), ..
                }) if next.as_slice() > route => next,
                Ok(_) => {
                    self.frontier = Frontier::End;
                    return;
                }
                Err(_) => return,
            },
        };
        while self.queue.len() < self.limit {
            if past_end(space, &key, end) {
                self.frontier = Frontier::End;
                return;
            }
            let Some(user) = space_key(space, &key) else {
                self.frontier = Frontier::End;
                return;
            };
            let Ok(desc) = self.client.descriptor_for(&key) else {
                break;
            };
            let at = Bound::Included(user);
            let req = reader.request(&desc, space, &at, end, ReadDirection::Forward);
            let next = desc.end.clone();
            if !self.spawn(desc, req, at, key.clone()) {
                break;
            }
            match next {
                Some(next) if next > key => key = next,
                _ => {
                    self.frontier = Frontier::End;
                    return;
                }
            }
        }
        self.frontier = Frontier::At(key);
    }

    /// Reverse: queues first pages of the ranges below the current one (`plan`'s last, the scan
    /// at range key `route`), highest first, until K are queued.
    pub(super) fn top_up_reverse(
        &mut self,
        reader: &RoutedKvRead,
        space: RangeSpace,
        route: &[u8],
        start: &Bound<Vec<u8>>,
        plan: &[RangeDescriptor],
    ) {
        let low = match &self.frontier {
            Frontier::At(low) if low.as_slice() < route => Some(low.clone()),
            _ => None,
        };
        let Some((_, below)) = plan.split_last() else {
            return;
        };
        for desc in below.iter().rev() {
            if self.queue.len() >= self.limit {
                break;
            }
            let Some(top) = desc.end.clone() else {
                break;
            };
            if low.as_ref().is_some_and(|low| top >= *low) {
                continue;
            }
            let Some(user) = space_key(space, &top) else {
                break;
            };
            let at = Bound::Excluded(user);
            let req = reader.request(desc, space, start, &at, ReadDirection::Reverse);
            if !self.spawn(desc.clone(), req, at, top.clone()) {
                break;
            }
            self.frontier = Frontier::At(top);
        }
    }
}

impl Drop for Prefetcher {
    fn drop(&mut self) {
        self.cancel.store(true, Ordering::Release);
    }
}
