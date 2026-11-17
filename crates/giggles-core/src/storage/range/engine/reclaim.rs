//! Reclaim after DROP on a range leader (GD-0075 R5-101; single-node counterpart: reclaim.rs).
//!
//! Horizon = a time-to-live, as CockroachDB's gc.ttl: a read at a timestamp older than
//! `now - gc_ttl` is refused (read.rs), so a table or index whose latest catalog version is a
//! tombstone at least the maximum clock offset older than that floor is invisible to every read the range still serves, and its
//! key run inside the range can go. Each pass walks the distinct table / index ids of the
//! range's Rows and Index keyspaces, reads the catalog record from the system range at a fresh
//! oracle timestamp, and deletes the run of each retired id inside the range's bounds with one
//! delete-range through the range's replicated write path (so every replica applies it).
//! Ids without any catalog record (rolled-back index builds) are left alone here.
use super::OpenEngines;
use std::sync::{
    Arc, Weak,
    atomic::{AtomicU64, Ordering},
    mpsc,
};
use std::{ops::Bound, time::Duration};

use super::{RangeEngine, txn_error};
use crate::ReclaimStats;
use crate::storage::engine::{
    catalog_codec, codec,
    commit::{Clock, SystemClock},
    indexes,
    kv::{Direction, Durability, Keyspace, WriteBatch},
    mvcc,
};
use crate::storage::range::keys::{cf_interval, keyspace_of};
use crate::storage::range_api::{
    RangeClient, RangeError, RangeSpace, ReadDirection, ReadRequest, Timestamp, range_key,
};

/// Maximum clock offset in milliseconds, also used by the read timestamp ahead check.
pub(super) const MAX_CLOCK_OFFSET: u64 = 10 * 60 * 1000;

/// Ids from here up are reserved lock tables (locks.rs, roles_engine.rs): they own no rows.
const RESERVED_IDS: u64 = u64::MAX - 15;

/// Lowest timestamp a read may use: `(now_ms - ttl) << 16` (0 when the TTL reaches past 0).
pub(super) fn gc_floor(now_ms: u64, ttl: Duration) -> u64 {
    let ttl = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX);
    now_ms.saturating_sub(ttl).saturating_mul(1 << 16)
}

/// The floor for this node's clock now.
pub(super) fn floor_now(ttl: Duration) -> u64 {
    gc_floor(Clock::now_ms(&SystemClock), ttl)
}

/// Commit ts and liveness of the newest version of catalog `key` at `at`; None = no record.
fn catalog_latest(
    client: &dyn RangeClient,
    key: &[u8],
    at: Timestamp,
) -> Result<Option<(u64, bool)>, RangeError> {
    let desc = client.descriptor_for(&range_key(RangeSpace::Catalog, key))?;
    let page = client.read(ReadRequest {
        range: desc.id,
        epoch: desc.epoch,
        space: RangeSpace::Catalog,
        start: Bound::Included(codec::user_key_seek_at(
            key,
            crate::storage::engine::hlc::Ts(u64::MAX),
        )),
        end: Bound::Excluded(codec::user_key_upper_bound(key)),
        dir: ReadDirection::Forward,
        ts: at,
        page_bytes: 1,
    })?;
    let Some((raw, value)) = page.pairs.first() else {
        return Ok(None);
    };
    let corrupt = |e: String| RangeError::Corrupt(format!("catalog record: {e}"));
    let (user, ts) = codec::split_versioned(raw).map_err(|e| corrupt(format!("{e:?}")))?;
    if user != key {
        return Ok(None);
    }
    let live = mvcc::decode(value).map_err(|e| corrupt(e.to_string()))?;
    Ok(Some((ts.0, live.is_some())))
}

impl RangeEngine {
    /// One reclaim pass over this range (see the module doc). Counts one per id and range.
    pub(crate) fn reclaim_once(
        &self,
        client: &dyn RangeClient,
        errors: &AtomicU64,
        now_ms: u64,
    ) -> Result<ReclaimStats, RangeError> {
        self.check_open()?;
        let floor = gc_floor(now_ms, self.config.gc_ttl);
        let cutoff = floor.checked_sub(MAX_CLOCK_OFFSET << 16);
        let at = client.oracle().next(1)?;
        let mut stats = ReclaimStats::default();
        for space in [RangeSpace::Rows, RangeSpace::Index] {
            let Some((lo, hi)) = cf_interval(&self.descriptor, space) else {
                continue;
            };
            let ks = keyspace_of(space);
            let mut from = lo.clone();
            while let Some(id) = self.first_id(ks, &from, &hi)? {
                if id >= RESERVED_IDS {
                    break;
                }
                let (first, next) = (id.to_be_bytes().to_vec(), (id + 1).to_be_bytes().to_vec());
                from = Bound::Included(next.clone());
                let catalog = match space {
                    RangeSpace::Rows => catalog_codec::table_key(id),
                    _ => indexes::descriptor_key(id),
                };
                let result = (|| {
                    // A slower new leader must still refuse every read that could see this run.
                    // IDs never recur; keep a full maximum clock offset below the read floor.
                    if !matches!(catalog_latest(client, &catalog, at)?, Some((d, false)) if cutoff.is_some_and(|c| d <= c))
                    {
                        return Ok(false);
                    }
                    let start = match &lo {
                        Bound::Included(lo) if *lo > first => lo.clone(),
                        _ => first,
                    };
                    let end = match &hi {
                        Bound::Excluded(hi) if *hi < next => hi.clone(),
                        _ => next,
                    };
                    let mut batch = WriteBatch::default();
                    batch.delete_range(ks, start, end);
                    self.check_open()?;
                    self.kv
                        .write(batch, Durability::Sync)
                        .map_err(|e| txn_error(e.into()))?;
                    Ok::<_, RangeError>(true)
                })();
                match result {
                    Ok(true) if space == RangeSpace::Rows => stats.tables += 1,
                    Ok(true) => stats.indexes += 1,
                    Ok(false) => {}
                    Err(_) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
        Ok(stats)
    }

    /// The id (first 8 key bytes) of the first stored key in `[from, hi)` of `ks`.
    fn first_id(
        &self,
        ks: Keyspace,
        from: &Bound<Vec<u8>>,
        hi: &Bound<Vec<u8>>,
    ) -> Result<Option<u64>, RangeError> {
        let mut iter = self
            .kv
            .range(
                ks,
                from.as_ref().map(Vec::as_slice),
                hi.as_ref().map(Vec::as_slice),
                Direction::Forward,
            )
            .map_err(|e| txn_error(e.into()))?;
        let Some(item) = iter.next() else {
            return Ok(None);
        };
        let (key, _) = item.map_err(|e| txn_error(e.into()))?;
        let id = key
            .get(..8)
            .and_then(|id| <[u8; 8]>::try_from(id).ok())
            .ok_or_else(|| RangeError::Corrupt(format!("short {ks:?} key")))?;
        Ok(Some(u64::from_be_bytes(id)))
    }
}

/// Reclaim cannot hold up home-record recovery. Dropping the sender stops this worker.
pub(super) fn spawn(
    open: Weak<OpenEngines>,
    client: Weak<dyn RangeClient>,
    stop: mpsc::Receiver<()>,
    every: Duration,
    errors: Arc<AtomicU64>,
) {
    let _ = std::thread::Builder::new()
        .name("range-reclaim".into())
        .spawn(move || {
            while let Err(mpsc::RecvTimeoutError::Timeout) = stop.recv_timeout(every) {
                let (Some(open), Some(client)) = (open.upgrade(), client.upgrade()) else {
                    return;
                };
                let engines: Vec<_> = open
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .values()
                    .cloned()
                    .collect();
                drop(open);
                for engine in engines {
                    if engine
                        .reclaim_once(&*client, &errors, Clock::now_ms(&SystemClock))
                        .is_err()
                    {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
}

/// Test hooks: the in-process cluster keeps its engines and store private, so `set_client`
/// registers them here under the client's address.
#[cfg(test)]
pub(crate) mod hooks {
    use super::super::OpenEngines;
    use super::*;
    use crate::storage::range::RangeStore;
    use std::sync::{Arc, Mutex, Weak};

    type Hook = (usize, Weak<OpenEngines>, Weak<RangeStore>, Arc<AtomicU64>);
    static HOOKS: Mutex<Vec<Hook>> = Mutex::new(Vec::new());

    fn address<T: ?Sized>(ptr: *const T) -> usize {
        ptr.cast::<()>() as usize
    }

    pub(in crate::storage::range::engine) fn register(
        client: &Weak<dyn RangeClient>,
        open: &Arc<OpenEngines>,
        store: &Arc<RangeStore>,
        errors: &Arc<AtomicU64>,
    ) {
        let mut hooks = HOOKS.lock().unwrap_or_else(|p| p.into_inner());
        hooks.retain(|(_, open, _, _)| open.strong_count() > 0);
        hooks.push((
            address(client.as_ptr()),
            Arc::downgrade(open),
            Arc::downgrade(store),
            errors.clone(),
        ));
    }

    fn find(client: &dyn RangeClient) -> (Arc<OpenEngines>, Arc<RangeStore>, Arc<AtomicU64>) {
        let at = address(client as *const dyn RangeClient);
        let hooks = HOOKS.lock().unwrap_or_else(|p| p.into_inner());
        let (_, open, store, errors) = hooks
            .iter()
            .find(|h| h.0 == at)
            .expect("registered engines");
        (
            open.upgrade().unwrap(),
            store.upgrade().unwrap(),
            errors.clone(),
        )
    }

    /// One reclaim pass on every engine open under `client`.
    pub(crate) fn reclaim_at(
        client: &dyn RangeClient,
        now_ms: u64,
    ) -> Result<ReclaimStats, RangeError> {
        let (open, _, errors) = find(client);
        let engines: Vec<_> = open.lock().unwrap().values().cloned().collect();
        let mut total = ReclaimStats::default();
        for engine in engines {
            let stats = engine.reclaim_once(client, &errors, now_ms)?;
            total.tables += stats.tables;
            total.indexes += stats.indexes;
        }
        Ok(total)
    }

    pub(crate) fn error_count(client: &dyn RangeClient) -> u64 {
        find(client).2.load(Ordering::Relaxed)
    }

    /// Every raw key of object `id` in `ks` in the node's store, across all ranges.
    pub(crate) fn raw_keys(client: &dyn RangeClient, ks: Keyspace, id: u64) -> Vec<Vec<u8>> {
        use crate::storage::engine::kv::KvRead;
        let store = find(client).1;
        let (lo, hi) = (id.to_be_bytes(), (id + 1).to_be_bytes());
        KvRead::range(
            store.kv().as_ref(),
            ks,
            Bound::Included(&lo[..]),
            Bound::Excluded(&hi[..]),
            Direction::Forward,
        )
        .unwrap()
        .map(|item| item.unwrap().0)
        .collect()
    }
}
