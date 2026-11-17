//! Range split driver (GD-0075 R5-81; r5/p9/split-design.md "Directory").
//!
//! `Splitter::split` runs on the range's leader: it first makes the range's own directory
//! record current (`reconcile`), leases the right range's id, splits (`RangeHost::split`, one
//! entry in the parent's log, applied here before it returns) and publishes both halves'
//! directory records in ONE system-range commit. Until they are published, routing works
//! through `StaleEpoch` replies carrying the child's descriptor (giggles-core range split.rs
//! `holder`); `reconcile` republishes after a crash between split and publish.
//!
//! `Splitter::split_by_size` is the size policy (R5-83): on the ranges this node leads, the one
//! system split at `first_user_key()`, then a split at `RangeStore::split_key` of every user range
//! whose approximate size reached a threshold and that is out of its cooldown. `SizeSplitter`
//! runs it (and `reconcile`) in the background.
use std::{
    sync::{
        Arc,
        mpsc::{self, RecvTimeoutError},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use giggles_core::{
    range_api::{
        ClusterTxnId, LeaseKind, RangeClient, RangeDescriptor, RangeError, RangeId, RangeSpace,
        SYSTEM_RANGE, Timestamp, first_user_key, range_key,
    },
    replicated::RangeStore,
};
use raft::StateRole;

use crate::{
    directory,
    error::{Error, Result},
    host::RangeHost,
};

/// Range ids made by splits: RANGE_ID_BASE + n, n from lease(LeaseKind::Xid, RANGE_ID_ALLOCATOR, 1);
/// disjoint from presplit ids (1..) and from gateway txn-id allocators ((1<<32)|node).
pub const RANGE_ID_BASE: u64 = 1 << 40;
pub const RANGE_ID_ALLOCATOR: u64 = 2 << 32;
/// Lease id namespace of per-node transaction ids, shared with the gateways' blocks
/// (design.md Changes, Track C (b)): `lease(Xid, TXN_ID_SPACE | node, n)`.
const TXN_ID_SPACE: u64 = 1 << 32;
/// Attempts of one directory read, lease or publish while the system range is unavailable,
/// changes leader or epoch, or a publish's outcome is unknown.
const ATTEMPTS: usize = 40;
const BACKOFF: Duration = Duration::from_millis(50);

/// The size split policy (R5-83).
#[derive(Clone, Copy, Debug)]
pub struct SizePolicy {
    /// A user range whose `RangeStore::approximate_size` is at least this many bytes splits.
    pub threshold: u64,
    /// A range is not split by size again until this long after this node applied its last
    /// split (or the split that created it).
    pub cooldown: Duration,
    /// `SizeSplitter`'s period.
    pub interval: Duration,
}

/// Splits ranges this node leads and keeps their directory records current.
pub struct Splitter {
    host: Arc<RangeHost>,
    client: Arc<dyn RangeClient>,
    node: u64,
}

/// A directory record to publish: a stored descriptor (`parent` None), or a lineage child of
/// `parent` as recorded at split time.
struct Candidate {
    desc: RangeDescriptor,
    parent: Option<RangeId>,
}

fn range_error(e: RangeError) -> Error {
    match e {
        RangeError::NotLeader { .. } => Error::NotLeader,
        e => Error::Core(format!("range call: {e}")),
    }
}

fn core(e: giggles_core::CoreError) -> Error {
    Error::Core(e.to_string())
}

/// Errors after which the same call may succeed (leader or epoch moved, no quorum yet).
fn transient(e: &RangeError) -> bool {
    matches!(
        e,
        RangeError::NotLeader { .. } | RangeError::Unavailable(_) | RangeError::StaleEpoch { .. }
    )
}

impl Splitter {
    pub fn new(host: Arc<RangeHost>, client: Arc<dyn RangeClient>) -> Self {
        let node = host.node_id();
        Self { host, client, node }
    }

    /// On the range's leader: reconcile(range) first (the range's own directory record must be
    /// current before it splits again), lease the right id, RangeHost::split, then publish
    /// record_write(left) + record_write(right) in ONE system-range commit; returns (left, right).
    pub fn split(
        &self,
        range: RangeId,
        at_key: &[u8],
    ) -> Result<(RangeDescriptor, RangeDescriptor)> {
        let handle = self
            .host
            .group(range)
            .ok_or_else(|| Error::Apply(format!("range {} is not hosted", range.0)))?;
        let status = crate::replace::status(&handle).map_err(|e| Error::Apply(e.to_string()))?;
        if membership_changing(&status.membership, &status.conf) {
            return Err(Error::Apply(format!(
                "range {}: cannot split during a membership change",
                range.0
            )));
        }
        self.reconcile(Some(range))?;
        let stored = self.stored(range)?;
        let found = self.retry(|| {
            let ts = self.client.oracle().next(1)?;
            self.lookup(&stored.start, ts)
        })?;
        if found.as_ref() != Some(&stored) {
            // Its parent has not published it yet (only the parent's leader can).
            return Err(Error::Apply(format!(
                "range {}: its directory record is not current (reconcile its parent first)",
                range.0
            )));
        }
        let n = self
            .retry(|| self.client.lease(LeaseKind::Xid, RANGE_ID_ALLOCATOR, 1))?
            .start;
        let right = RANGE_ID_BASE
            .checked_add(n)
            .filter(|&id| id != u64::MAX)
            .ok_or_else(|| Error::Core("split range ids exhausted".into()))?;
        let (left, right) = self.host.split(range, at_key, RangeId(right))?;
        self.publish(&[
            Candidate {
                desc: left.clone(),
                parent: None,
            },
            Candidate {
                desc: right.clone(),
                parent: Some(range),
            },
        ])?;
        Ok((left, right))
    }

    /// For every range this node leads (or just `range`): if the directory record at the stored
    /// descriptor's end is not the stored descriptor, republish it; for each lineage child
    /// (store.children) whose start key the directory still maps to this range id, publish the
    /// child's descriptor as recorded. Never writes a record older than the one it replaces.
    /// Returns how many records it wrote.
    pub fn reconcile(&self, range: Option<RangeId>) -> Result<usize> {
        let ranges = match range {
            Some(range) if self.leads(range) => vec![range],
            Some(_) => return Err(Error::NotLeader),
            None => self
                .host
                .groups()
                .into_iter()
                .filter(|&range| self.leads(range))
                .collect(),
        };
        let store = self.store()?;
        let mut candidates = Vec::new();
        for id in ranges {
            let Some(desc) = store.descriptor(id).map_err(core)? else {
                continue;
            };
            candidates.push(Candidate { desc, parent: None });
            for child in store.children(id).map_err(core)? {
                candidates.push(Candidate {
                    desc: child,
                    parent: Some(id),
                });
            }
        }
        self.publish(&candidates)
    }

    /// One size-policy pass over the ranges this node leads (listed once, at the start, in id
    /// order, so a range created by this pass is not split by it, and each range at most once):
    /// if the system range still covers user keys, split it at `first_user_key()` (its one
    /// split; it never splits by size); then split each other led range whose approximate size
    /// is at least `threshold` and that is out of its cooldown at `store.split_key()`, a whole
    /// user key near its midpoint. Returns the splits made, as `(left, right)`.
    ///
    /// Sizes count SST files only (`RangeStore::approximate_size`); the pass does not flush
    /// memtables. A split refused or no longer led here (`NotLeader`, `Identity`, `Apply`, e.g.
    /// prepared records draining or a directory record not yet current) is skipped until a later
    /// pass; other errors end the pass.
    ///
    /// Cooldown state is this host's memory of the splits it applied
    /// (`RangeHost::created_by_split` of the range and of its lineage children), so every
    /// replica knows it, but not across a restart: a node that just restarted may split a range
    /// that was split just before. Manual splits (`split`) start a cooldown too.
    pub fn split_by_size(
        &self,
        policy: &SizePolicy,
    ) -> Result<Vec<(RangeDescriptor, RangeDescriptor)>> {
        let store = self.store()?;
        let user = first_user_key();
        let mut led = self.host.groups();
        led.retain(|&range| self.leads(range));
        let mut splits = Vec::new();
        for range in led {
            let Some(desc) = store.descriptor(range).map_err(core)? else {
                continue;
            };
            let at = if range == SYSTEM_RANGE {
                if desc.end.as_ref().is_some_and(|end| *end <= user) {
                    continue;
                }
                user.clone()
            } else {
                if self.cooling(&store, range, policy.cooldown)?
                    || store.approximate_size(range).map_err(core)? < policy.threshold
                {
                    continue;
                }
                match store.split_key(range).map_err(core)? {
                    Some(at) => at,
                    None => continue,
                }
            };
            match self.split(range, &at) {
                Ok(pair) => splits.push(pair),
                Err(e @ (Error::NotLeader | Error::Identity(_) | Error::Apply(_))) => {
                    eprintln!("node {}: size split of range {}: {e}", self.node, range.0);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(splits)
    }

    /// Whether this host applied a split of `range`, or the split creating it, within `cooldown`.
    fn cooling(&self, store: &RangeStore, range: RangeId, cooldown: Duration) -> Result<bool> {
        if cooldown.is_zero() {
            return Ok(false);
        }
        let children = store.children(range).map_err(core)?;
        Ok(std::iter::once(range)
            .chain(children.iter().map(|child| child.id))
            .filter_map(|id| self.host.created_by_split(id))
            .any(|at| at.elapsed() < cooldown))
    }

    fn store(&self) -> Result<Arc<RangeStore>> {
        self.host
            .store()
            .ok_or_else(|| Error::Identity("range host has no range store".into()))
    }

    fn leads(&self, range: RangeId) -> bool {
        self.host
            .group(range)
            .is_some_and(|handle| handle.role().state == StateRole::Leader)
    }

    fn stored(&self, range: RangeId) -> Result<RangeDescriptor> {
        self.store()?
            .descriptor(range)
            .map_err(core)?
            .ok_or_else(|| Error::Core(format!("range {} has no descriptor", range.0)))
    }

    /// Runs `call` until it succeeds, fails for good, or `ATTEMPTS` transient failures.
    fn retry<T>(&self, mut call: impl FnMut() -> std::result::Result<T, RangeError>) -> Result<T> {
        let mut last = None;
        for _ in 0..ATTEMPTS {
            match call() {
                Ok(value) => return Ok(value),
                Err(e) if transient(&e) => last = Some(e),
                Err(e) => return Err(range_error(e)),
            }
            thread::sleep(BACKOFF);
        }
        Err(range_error(last.expect("ATTEMPTS > 0")))
    }

    /// The system range's descriptor, from the client's route cache or its directory.
    fn system(&self) -> std::result::Result<RangeDescriptor, RangeError> {
        let key = range_key(RangeSpace::Catalog, &[directory::DIRECTORY_TAG]);
        self.client.descriptor_for(&key)
    }

    /// The directory record containing `key` at `ts` (tombstoned records skipped).
    fn lookup(
        &self,
        key: &[u8],
        ts: Timestamp,
    ) -> std::result::Result<Option<RangeDescriptor>, RangeError> {
        let mut req = directory::lookup_request(key, &self.system()?, ts);
        loop {
            let page = self.client.read(req.clone())?;
            if let Some(desc) = directory::parse_lookup(&page, key)? {
                return Ok(Some(desc));
            }
            match page.resume {
                Some(resume) if directory::all_tombstones(&page) => req.start = resume,
                _ => return Ok(None),
            }
        }
    }

    /// The candidates the directory does not yet hold, read at one fresh timestamp. A stored
    /// descriptor is written over an older record of its own id (or a hole); a child over its
    /// parent's record. Anything else is newer information and is left alone.
    fn pending(
        &self,
        candidates: &[Candidate],
    ) -> std::result::Result<Vec<RangeDescriptor>, RangeError> {
        let ts = self.client.oracle().next(1)?;
        let mut out: Vec<RangeDescriptor> = Vec::new();
        for c in candidates {
            let write = match self.lookup(&c.desc.start, ts)? {
                Some(found) if found.id == c.desc.id => found.epoch < c.desc.epoch,
                Some(found) => c.parent == Some(found.id),
                None => c.parent.is_none(),
            };
            if write && !out.contains(&c.desc) {
                out.push(c.desc.clone());
            }
        }
        Ok(out)
    }

    /// Writes the pending candidates in one system-range commit, retried (re-reading the
    /// directory first) until one commits or the directory holds them all. Returns how many
    /// records the committed (or last unknown) attempt wrote; 0 if none was pending.
    fn publish(&self, candidates: &[Candidate]) -> Result<usize> {
        let mut written = 0;
        let mut last = None;
        for _ in 0..ATTEMPTS {
            let attempt = self.pending(candidates).and_then(|pending| {
                if pending.is_empty() {
                    return Ok(());
                }
                let writes = pending
                    .iter()
                    .map(directory::record_write)
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                let counter = self
                    .client
                    .lease(LeaseKind::Xid, TXN_ID_SPACE | self.node, 1)?
                    .start;
                let txn = ClusterTxnId::try_new(self.node, counter)
                    .ok_or_else(|| RangeError::Corrupt("transaction ids exhausted".into()))?;
                let system = self.system()?;
                written = pending.len();
                // Catalog-only writes take no lock (design.md Changes, Track C (c)).
                self.client
                    .commit(system.id, system.epoch, txn, writes, None, None)
                    .map(|_| ())
            });
            match attempt {
                Ok(()) => return Ok(written),
                // Unknown, refused or not reached: re-read the directory and retry.
                Err(
                    e @ (RangeError::Unknown(_) | RangeError::Aborted(_) | RangeError::Conflict(_)),
                ) => last = Some(e),
                Err(e) if transient(&e) => last = Some(e),
                Err(e) => return Err(range_error(e)),
            }
            thread::sleep(BACKOFF);
        }
        Err(Error::Core(format!(
            "directory publish not current after {ATTEMPTS} attempts ({})",
            last.map_or_else(String::new, |e| e.to_string())
        )))
    }
}

/// Runs `Splitter::split_by_size` and then `Splitter::reconcile(None)` every `policy.interval`
/// on a background thread until stopped or dropped; errors are logged and retried next period.
pub struct SizeSplitter {
    stop: Option<mpsc::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl SizeSplitter {
    pub fn start(splitter: Arc<Splitter>, policy: SizePolicy) -> Self {
        let (stop, stopped) = mpsc::channel::<()>();
        let thread = thread::spawn(move || {
            while let Err(RecvTimeoutError::Timeout) = stopped.recv_timeout(policy.interval) {
                let node = splitter.node;
                if let Err(e) = splitter.split_by_size(&policy) {
                    eprintln!("node {node}: size split pass: {e}");
                }
                if let Err(e) = splitter.reconcile(None) {
                    eprintln!("node {node}: directory reconcile: {e}");
                }
            }
        });
        Self {
            stop: Some(stop),
            thread: Some(thread),
        }
    }

    /// Stops the loop and waits for a pass in progress to end. Idempotent.
    pub fn stop(&mut self) {
        drop(self.stop.take());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for SizeSplitter {
    fn drop(&mut self) {
        self.stop();
    }
}

fn membership_changing(
    membership: &crate::membership::Membership,
    conf: &raft::eraftpb::ConfState,
) -> bool {
    membership.active.is_some()
        || !conf.voters_outgoing.is_empty()
        || !conf.learners.is_empty()
        || !conf.learners_next.is_empty()
}

#[cfg(test)]
mod membership_guard_tests {
    use super::*;
    use crate::membership::{Membership, PeerRecord, Replacement, Step};
    use raft::eraftpb::ConfState;

    #[test]
    fn split_guard_covers_replacement_joint_and_learners_and_clears_when_idle() {
        let mut membership = Membership::default();
        let conf = ConfState::default();
        assert!(!membership_changing(&membership, &conf));
        membership.active = Some(Replacement {
            lost: 3,
            new: PeerRecord {
                id: 4,
                address: "four".into(),
                fingerprint: "04".repeat(32),
            },
            step: Step::Enrolled,
            started_at: 1,
        });
        assert!(membership_changing(&membership, &conf));
        membership.active = None;
        assert!(!membership_changing(&membership, &conf));
        for kind in 0..3 {
            let mut conf = ConfState::default();
            match kind {
                0 => conf.voters_outgoing.push(3),
                1 => conf.learners.push(4),
                _ => conf.learners_next.push(4),
            }
            assert!(membership_changing(&membership, &conf));
        }
    }
}
