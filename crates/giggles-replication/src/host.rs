//! Many Raft groups in one process (GD-0075 R5-13 part 1).
//!
//! A `RangeHost` owns one replica thread per range, each with its own log
//! directory `<log_dir>/<range id>/` and snapshot directory
//! `<snap_dir>/<range id>/`, bound to the group's cluster id (`group.rs`).
//! The network is abstracted behind `GroupNetwork`.
use crate::{
    client::LocalRanges,
    error::{Error, Result},
    group::group_cluster,
    liveness::{LIVENESS_GROUP, Liveness, LivenessConfig, LivenessSink},
    log_store::LogStore,
    node::Replica,
    range_role::{RangeEngine, RangeRoles},
    range_state::{SplitEvent, SplitListener, encode_split_proposal},
    role::Serving,
    sink::{Outbox, ReplicaHandle, ReplicateError, SnapshotPolicy, spawn_with_policy},
    snapshot::SnapshotStore,
    state::StateMachine,
    transport::PeerSink,
};
use giggles_core::{
    range_api::{
        RangeDescriptor, RangeEngineOpener, RangeId, RangeService, ReplicaRole, SystemService,
    },
    replicated::RangeStore,
};
use raft::StateRole;
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicU64, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

/// First and longest wait between retries of a split child's group open.
const OPEN_RETRY: (Duration, Duration) = (Duration::from_millis(50), Duration::from_secs(2));

/// Routes Raft messages for many groups over one process-wide network.
pub trait GroupNetwork: Send + Sync {
    /// Outbox the group's replica thread sends through.
    fn outbox(&self, group: RangeId) -> Box<dyn Outbox>;
    /// Inbound messages for `group` on this node go to `sink`.
    fn register(&self, group: RangeId, sink: Arc<dyn PeerSink>);
    /// As `register`, with the group's snapshot store for snapshot transfers
    /// (GD-0075 R5-14). Networks without snapshot transfer ignore the store.
    fn register_group(
        &self,
        group: RangeId,
        sink: Arc<dyn PeerSink>,
        snapshots: Option<Arc<SnapshotStore>>,
    ) {
        let _ = snapshots;
        self.register(group, sink);
    }
    fn unregister(&self, group: RangeId);
}

/// Builds the state machine a range's replica applies to.
pub type StateFactory = Arc<dyn Fn(RangeId) -> Arc<dyn StateMachine> + Send + Sync>;

#[derive(Clone, Debug)]
pub struct RangeHostConfig {
    pub node_id: u64,
    pub cluster: [u8; 16],
    pub log_dir: PathBuf,
    pub snap_dir: PathBuf,
    pub tick: Duration,
    pub policy: Option<SnapshotPolicy>,
    /// Coalesced node liveness (GD-0075 R5-17b). When set, every group may
    /// quiesce when idle and learns peer node deaths from this host's tracker.
    pub liveness: Option<LivenessConfig>,
}

struct Group {
    handle: ReplicaHandle,
    join: JoinHandle<()>,
}

pub struct RangeHost {
    inner: Arc<Inner>,
}

/// The host's shared parts; split listeners hold a `Weak` to it (GD-0075 R5-80).
struct Inner {
    config: RangeHostConfig,
    network: Arc<dyn GroupNetwork>,
    states: StateFactory,
    groups: Mutex<BTreeMap<RangeId, Group>>,
    /// Store-backed hosts only (`open`): role managers and the system service.
    roles: RangeRoles,
    liveness: Option<Arc<Liveness>>,
    /// Handles told about peer liveness changes (quiescing groups).
    listeners: Arc<Mutex<BTreeMap<RangeId, ReplicaHandle>>>,
    /// Set once `stop()` began; held while a range opens so none opens after it.
    stopped: Mutex<bool>,
    /// When this host applied the split that created each range (since it opened; R5-83).
    split_children: Mutex<BTreeMap<RangeId, Instant>>,
    /// Split children created here from a parent's lineage (GD-0075 P9-2c).
    lineage: AtomicU64,
}

impl RangeHost {
    /// Opens every listed group. The caller passes the groups whose
    /// range-local descriptor lists this node (RangeStore enumeration comes later).
    pub fn start(
        config: RangeHostConfig,
        network: Arc<dyn GroupNetwork>,
        states: StateFactory,
        existing: Vec<(RangeId, Vec<u64>)>,
    ) -> Result<Self> {
        Self::build(config, network, states, RangeRoles::default(), existing)
    }

    fn build(
        config: RangeHostConfig,
        network: Arc<dyn GroupNetwork>,
        states: StateFactory,
        roles: RangeRoles,
        existing: Vec<(RangeId, Vec<u64>)>,
    ) -> Result<Self> {
        let mut host = Inner {
            config,
            network,
            states,
            groups: Mutex::new(BTreeMap::new()),
            roles,
            liveness: None,
            listeners: Arc::default(),
            stopped: Mutex::new(false),
            split_children: Mutex::default(),
            lineage: AtomicU64::new(0),
        };
        if let Some(liveness) = host.config.liveness {
            let group = RangeId(LIVENESS_GROUP);
            let tracker =
                Liveness::start(host.config.node_id, liveness, host.network.outbox(group));
            host.network
                .register(group, Arc::new(LivenessSink(tracker.clone())));
            let listeners = host.listeners.clone();
            tracker.listen(Arc::new(move |node, alive| {
                let handles = listeners.lock().expect("range host mutex poisoned");
                for handle in handles.values() {
                    handle.node_liveness(node, alive);
                }
            }));
            host.liveness = Some(tracker);
        }
        for (range, voters) in existing {
            host.create_group(range, &voters)?;
        }
        Ok(Self {
            inner: Arc::new(host),
        })
    }

    /// Opens a host over a multi-range store (GD-0075 R5-13/R5-16): one group,
    /// with a `RangeStateMachine` over `store`, for every stored range whose
    /// descriptor lists this node; its voters are the descriptor's Voter replicas.
    /// With an opener, each group's role manager opens a leader engine through it.
    /// A split applied by any of its groups opens the new right range's group here
    /// when its descriptor lists this node (a range already hosted is left alone).
    /// A snapshot install whose lineage names a child not stored here, listing this node,
    /// stores that child as needing a snapshot and opens its group (GD-0075 P9-2c); so does
    /// `open` for a child a crash left uncreated after such an install.
    pub fn open(
        config: RangeHostConfig,
        network: Arc<dyn GroupNetwork>,
        store: Arc<RangeStore>,
        opener: Option<Arc<dyn RangeEngineOpener>>,
    ) -> Result<Self> {
        if store.domain() != config.cluster {
            return Err(Error::Identity("range store domain mismatch".into()));
        }
        let slot: Arc<OnceLock<Weak<Inner>>> = Arc::default();
        let hook = slot.clone();
        let listener: SplitListener = Arc::new(move |right: &RangeDescriptor, event| {
            if let Some(inner) = hook.get().and_then(Weak::upgrade) {
                inner.open_split_child(right, event);
            }
        });
        let (roles, states) = RangeRoles::backed(store.clone(), opener, listener);
        let host = Self::build(config, network, states, roles, Vec::new())?;
        let _ = slot.set(Arc::downgrade(&host.inner));
        for range in store.ranges().map_err(core)? {
            let Some(descriptor) = store.descriptor(range).map_err(core)? else {
                continue;
            };
            if descriptor.replicas.iter().any(|r| r.node == host.node_id()) {
                host.inner.open_range(&descriptor, true)?;
            }
        }
        // A split stores both halves in one batch, so an unstored lineage child was learned
        // from a snapshot whose install a crash cut off before the child was created.
        for range in store.ranges().map_err(core)? {
            for child in store.children(range).map_err(core)? {
                if store.descriptor(child.id).map_err(core)?.is_none() {
                    host.inner.open_split_child(&child, SplitEvent::Lineage);
                }
            }
        }
        Ok(host)
    }

    /// Stores a new range's first descriptor and opens its group (store-backed hosts).
    pub fn create_range(&self, descriptor: &RangeDescriptor) -> Result<ReplicaHandle> {
        let inner = &self.inner;
        let store = inner
            .roles
            .store()
            .ok_or_else(|| Error::Identity("range host has no range store".into()))?;
        if descriptor.id.0 == 0 || descriptor.id.0 == LIVENESS_GROUP {
            return Err(Error::Identity(format!(
                "range id {} is reserved",
                descriptor.id.0
            )));
        }
        if !descriptor.replicas.iter().any(|r| r.node == self.node_id()) {
            return Err(Error::Identity(format!(
                "range {} does not list node {}",
                descriptor.id.0,
                self.node_id()
            )));
        }
        store.create_range(descriptor).map_err(core)?;
        inner
            .open_range(descriptor, false)?
            .ok_or_else(|| Error::Identity("range host is stopped".into()))
    }

    /// Splits `range` at `at_key` (GD-0075 R5-80): the range keeps `[start, at_key)`
    /// and the new range `right` gets `[at_key, end)`, both at the next epoch. Only on
    /// the range's current leader; the split entry is proposed at the stored
    /// descriptor's epoch and applied here before this returns `(left, right)`. Every
    /// replica's store-backed host opens `right`'s group when it applies the entry, and marks
    /// the range's leader engine there as moved: whichever node leads the range reopens it
    /// over `left` (GD-0075 R5-81).
    pub fn split(
        &self,
        range: RangeId,
        at_key: &[u8],
        right: RangeId,
    ) -> Result<(RangeDescriptor, RangeDescriptor)> {
        let store = self
            .inner
            .roles
            .store()
            .ok_or_else(|| Error::Identity("range host has no range store".into()))?;
        let handle = self
            .group(range)
            .ok_or_else(|| Error::Identity(format!("range {} is not hosted here", range.0)))?;
        let not_leader = || {
            let leader = handle.role().leader;
            let hint = if leader == 0 || leader == self.node_id() {
                "none".to_owned()
            } else {
                format!("node {leader}")
            };
            Error::Identity(format!(
                "node {} does not lead range {}; leader hint: {hint}",
                self.node_id(),
                range.0
            ))
        };
        if handle.role().state != StateRole::Leader {
            return Err(not_leader());
        }
        let read = |id: RangeId| {
            store
                .descriptor(id)
                .map_err(core)?
                .ok_or_else(|| Error::Core(format!("range {} has no descriptor", id.0)))
        };
        let epoch = read(range)?.epoch;
        // Proposer-side only (stored data is not a deterministic apply input): the bound must
        // be a whole user key or a table/index boundary (design.md Changes, Track A (g)).
        store
            .check_split_key(range, at_key)
            .map_err(|e| Error::Apply(format!("range {} split key refused: {e}", range.0)))?;
        match handle.replicate(encode_split_proposal(epoch, at_key, right)) {
            Ok(()) => Ok((read(range)?, read(right)?)),
            Err(ReplicateError::NotLeader) => Err(not_leader()),
            Err(ReplicateError::Refused(reason)) => Err(Error::Apply(reason)),
            Err(e) => Err(Error::Apply(format!("range {} split: {e}", range.0))),
        }
    }

    /// The multi-range store of a store-backed host (`open`).
    pub fn store(&self) -> Option<Arc<RangeStore>> {
        self.inner.roles.store().cloned()
    }

    /// The range's role (leader engine or follower); None without a role manager.
    pub fn serving(&self, range: RangeId) -> Option<Serving<RangeEngine>> {
        self.inner.roles.serving(range)
    }

    /// The system service `LocalRanges::system` returns (the system range's owner sets it).
    pub fn set_system(&self, system: Option<Arc<dyn SystemService>>) {
        self.inner.roles.set_system(system);
    }

    pub fn node_id(&self) -> u64 {
        self.inner.config.node_id
    }

    /// When this host applied the split entry that created `range` (GD-0075 R5-83 cooldown).
    /// In memory: None for ranges created before this host opened, or by a layout.
    pub fn created_by_split(&self, range: RangeId) -> Option<Instant> {
        let children = self
            .inner
            .split_children
            .lock()
            .expect("range host mutex poisoned");
        children.get(&range).copied()
    }

    /// Split children this host created from a parent snapshot's lineage (GD-0075 P9-2c).
    pub fn lineage_children(&self) -> u64 {
        self.inner.lineage.load(Ordering::Relaxed)
    }

    /// Opens (or creates) the group's log and snapshot stores and starts its replica thread.
    pub fn create_group(&self, range: RangeId, voters: &[u64]) -> Result<ReplicaHandle> {
        self.inner.create_group(range, voters)
    }

    pub fn group(&self, range: RangeId) -> Option<ReplicaHandle> {
        self.inner.group(range)
    }

    pub fn groups(&self) -> Vec<RangeId> {
        let groups = self.inner.groups.lock().expect("range host mutex poisoned");
        groups.keys().copied().collect()
    }

    /// Adds newly discovered node identities to the optional node-wide liveness tracker.
    pub fn add_liveness_peers(&self, peers: &[u64]) {
        if let Some(liveness) = &self.inner.liveness {
            liveness.add_peers(peers.iter().copied());
        }
    }

    /// Shuts every group down and joins its replica thread. Idempotent.
    pub fn stop(&self) {
        self.inner.stop();
    }
}

impl Inner {
    /// Opens the range's group and role manager. `Ok(None)` once `stop()` began, or
    /// when `if_absent` and the group is already hosted. Serialized with `stop()`.
    fn open_range(
        &self,
        descriptor: &RangeDescriptor,
        if_absent: bool,
    ) -> Result<Option<ReplicaHandle>> {
        let stopped = self.stopped.lock().expect("range host mutex poisoned");
        if *stopped || (if_absent && self.group(descriptor.id).is_some()) {
            return Ok(None);
        }
        let voters: Vec<u64> = descriptor
            .replicas
            .iter()
            .filter(|r| r.role == ReplicaRole::Voter)
            .map(|r| r.node)
            .collect();
        let handle = self.create_group(descriptor.id, &voters)?;
        self.roles
            .start(descriptor.id, handle.clone(), self.config.tick);
        Ok(Some(handle))
    }

    /// Split listener, on the parent's applying thread: after the store's split write
    /// (`Applied`) or a snapshot install (`Lineage`, the child first stored as needing a
    /// snapshot). Opens the child's group when it lists this node. A failed open is retried
    /// on a background thread until it succeeds or the host stops (the split entry is
    /// applied once, so nothing else would open the child before a restart).
    ///
    /// Running on the applying thread cannot deadlock: like a split's open, it takes only
    /// host and store locks no other thread holds while waiting on a replica (`stop()`
    /// releases them before joining), spawns the child's replica thread, and never waits on
    /// the parent's own driver (the caller).
    fn open_split_child(self: &Arc<Self>, right: &RangeDescriptor, event: SplitEvent) {
        if event == SplitEvent::Applied {
            self.split_children
                .lock()
                .expect("range host mutex poisoned")
                .insert(right.id, Instant::now());
        }
        let (cluster, node) = (&self.config.cluster, Some(self.config.node_id));
        if event == SplitEvent::Applied {
            crate::failpoint::hit(cluster, node, "range.split.applied");
        }
        if !right.replicas.iter().any(|r| r.node == self.config.node_id) {
            return;
        }
        if event == SplitEvent::Lineage && !self.create_from_lineage(right) {
            return;
        }
        match self.open_range(right, true) {
            Ok(Some(_)) => crate::failpoint::hit(cluster, node, "range.split.child_opened"),
            Ok(None) => {}
            Err(e) => {
                eprintln!(
                    "node {}: opening split range {} failed, retrying: {e}",
                    self.config.node_id, right.id.0
                );
                let weak = Arc::downgrade(self);
                let range = right.id;
                let spawned = thread::Builder::new()
                    .name(format!("split-open-{}", range.0))
                    .spawn(move || retry_open(&weak, range));
                if let Err(e) = spawned {
                    eprintln!("split range {}: no retry thread: {e}", range.0);
                }
            }
        }
    }

    /// Stores a lineage child as needing a snapshot (its leader sends one on demand);
    /// false when that failed (the next `open` retries it).
    fn create_from_lineage(&self, child: &RangeDescriptor) -> bool {
        let Some(store) = self.roles.store() else {
            return false;
        };
        match store.create_needs_snapshot(child) {
            Ok(created) => {
                if created {
                    self.lineage.fetch_add(1, Ordering::Relaxed);
                    eprintln!(
                        "node {}: range {} created from its parent's snapshot lineage; it waits for its own snapshot",
                        self.config.node_id, child.id.0
                    );
                }
                true
            }
            Err(e) => {
                eprintln!(
                    "node {}: creating range {} from lineage failed (retried on restart): {e}",
                    self.config.node_id, child.id.0
                );
                false
            }
        }
    }

    /// One retry of a split child's open, from its stored descriptor. `Ok` when done:
    /// opened, already hosted, no longer listing this node, or the host stopped.
    fn reopen_child(&self, range: RangeId) -> Result<()> {
        let Some(store) = self.roles.store() else {
            return Ok(());
        };
        match store.descriptor(range).map_err(core)? {
            Some(d) if d.replicas.iter().any(|r| r.node == self.config.node_id) => {
                self.open_range(&d, true).map(drop)
            }
            _ => Ok(()),
        }
    }

    fn create_group(&self, range: RangeId, voters: &[u64]) -> Result<ReplicaHandle> {
        if range.0 == 0 || range.0 == LIVENESS_GROUP {
            // 0 is the R4 single-group identity; u64::MAX carries node liveness.
            return Err(Error::Identity(format!("range id {} is reserved", range.0)));
        }
        let mut groups = self.groups.lock().expect("range host mutex poisoned");
        if groups.contains_key(&range) {
            return Err(Error::Identity(format!(
                "range {} is already hosted",
                range.0
            )));
        }
        let cluster = group_cluster(self.config.cluster, range.0);
        let dir = range.0.to_string();
        let snapshots = Arc::new(SnapshotStore::open(
            &self.config.snap_dir.join(&dir),
            cluster,
        )?);
        let mut log = LogStore::open(
            &self.config.log_dir.join(&dir),
            cluster,
            self.config.node_id,
        )?;
        log.attach_snapshots(snapshots.clone());
        let mut replica = Replica::new(log, (self.states)(range), voters)?;
        if let Some(liveness) = &self.liveness {
            replica.enable_quiescence();
            liveness.add_peers(voters.iter().copied());
        }
        let (handle, join) = spawn_with_policy(
            replica,
            self.network.outbox(range),
            self.config.tick,
            self.config.policy,
        );
        self.network
            .register_group(range, Arc::new(handle.clone()), Some(snapshots));
        if self.liveness.is_some() {
            self.listeners
                .lock()
                .expect("range host mutex poisoned")
                .insert(range, handle.clone());
        }
        groups.insert(
            range,
            Group {
                handle: handle.clone(),
                join,
            },
        );
        Ok(handle)
    }

    fn group(&self, range: RangeId) -> Option<ReplicaHandle> {
        let groups = self.groups.lock().expect("range host mutex poisoned");
        groups.get(&range).map(|g| g.handle.clone())
    }

    fn stop(&self) {
        *self.stopped.lock().expect("range host mutex poisoned") = true;
        if let Some(liveness) = &self.liveness {
            liveness.stop();
            self.network.unregister(RangeId(LIVENESS_GROUP));
        }
        self.listeners
            .lock()
            .expect("range host mutex poisoned")
            .clear();
        let drained = std::mem::take(&mut *self.groups.lock().expect("range host mutex poisoned"));
        for (range, group) in &drained {
            self.network.unregister(*range);
            group.handle.shutdown();
        }
        // After shutdown so a role manager blocked on a read barrier is released.
        self.roles.stop_all();
        for (range, group) in drained {
            if group.join.join().is_err() {
                eprintln!("range {} replica thread panicked", range.0);
            }
        }
    }
}

impl Drop for RangeHost {
    fn drop(&mut self) {
        self.stop();
    }
}

/// This node's open, unfenced leader engines (store-backed hosts with an opener).
impl LocalRanges for RangeHost {
    fn service(&self, range: RangeId) -> Option<Arc<dyn RangeService>> {
        self.inner.roles.service(range)
    }
    fn system(&self) -> Option<Arc<dyn SystemService>> {
        self.inner.roles.system()
    }
}

/// Retries a split child's open with backoff until done or the host is gone.
fn retry_open(host: &Weak<Inner>, range: RangeId) {
    let mut wait = OPEN_RETRY.0;
    loop {
        thread::sleep(wait);
        let Some(inner) = host.upgrade() else {
            return;
        };
        match inner.reopen_child(range) {
            Ok(()) => return,
            Err(e) => eprintln!(
                "node {}: opening split range {} failed, retrying: {e}",
                inner.config.node_id, range.0
            ),
        }
        wait = (wait * 2).min(OPEN_RETRY.1);
    }
}

fn core(e: giggles_core::CoreError) -> Error {
    Error::Core(e.to_string())
}
