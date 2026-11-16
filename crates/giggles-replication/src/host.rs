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
    role::Serving,
    sink::{Outbox, ReplicaHandle, SnapshotPolicy, spawn_with_policy},
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
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    thread::JoinHandle,
    time::Duration,
};

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
    config: RangeHostConfig,
    network: Arc<dyn GroupNetwork>,
    states: StateFactory,
    groups: Mutex<BTreeMap<RangeId, Group>>,
    /// Store-backed hosts only (`open`): role managers and the system service.
    roles: RangeRoles,
    liveness: Option<Arc<Liveness>>,
    /// Handles told about peer liveness changes (quiescing groups).
    listeners: Arc<Mutex<BTreeMap<RangeId, ReplicaHandle>>>,
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
        let mut host = Self {
            config,
            network,
            states,
            groups: Mutex::new(BTreeMap::new()),
            roles: RangeRoles::default(),
            liveness: None,
            listeners: Arc::default(),
        };
        if let Some(liveness) = host.config.liveness {
            let group = RangeId(LIVENESS_GROUP);
            let tracker = Liveness::start(
                host.config.node_id,
                liveness,
                host.network.outbox(group),
            );
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
        Ok(host)
    }

    /// Opens a host over a multi-range store (GD-0075 R5-13/R5-16): one group,
    /// with a `RangeStateMachine` over `store`, for every stored range whose
    /// descriptor lists this node; its voters are the descriptor's Voter replicas.
    /// With an opener, each group's role manager opens a leader engine through it.
    pub fn open(
        config: RangeHostConfig,
        network: Arc<dyn GroupNetwork>,
        store: Arc<RangeStore>,
        opener: Option<Arc<dyn RangeEngineOpener>>,
    ) -> Result<Self> {
        if store.domain() != config.cluster {
            return Err(Error::Identity("range store domain mismatch".into()));
        }
        let (roles, states) = RangeRoles::backed(store.clone(), opener);
        let mut host = Self::start(config, network, states, Vec::new())?;
        host.roles = roles;
        for range in store.ranges().map_err(core)? {
            let Some(descriptor) = store.descriptor(range).map_err(core)? else {
                continue;
            };
            if descriptor.replicas.iter().any(|r| r.node == host.node_id()) {
                host.open_range(&descriptor)?;
            }
        }
        Ok(host)
    }

    /// Stores a new range's first descriptor and opens its group (store-backed hosts).
    pub fn create_range(&self, descriptor: &RangeDescriptor) -> Result<ReplicaHandle> {
        let store = self
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
        self.open_range(descriptor)
    }

    fn open_range(&self, descriptor: &RangeDescriptor) -> Result<ReplicaHandle> {
        let voters: Vec<u64> = descriptor
            .replicas
            .iter()
            .filter(|r| r.role == ReplicaRole::Voter)
            .map(|r| r.node)
            .collect();
        let handle = self.create_group(descriptor.id, &voters)?;
        self.roles
            .start(descriptor.id, handle.clone(), self.config.tick);
        Ok(handle)
    }

    /// The range's role (leader engine or follower); None without a role manager.
    pub fn serving(&self, range: RangeId) -> Option<Serving<RangeEngine>> {
        self.roles.serving(range)
    }

    /// The system service `LocalRanges::system` returns (the system range's owner sets it).
    pub fn set_system(&self, system: Option<Arc<dyn SystemService>>) {
        self.roles.set_system(system);
    }

    pub fn node_id(&self) -> u64 {
        self.config.node_id
    }

    /// Opens (or creates) the group's log and snapshot stores and starts its replica thread.
    pub fn create_group(&self, range: RangeId, voters: &[u64]) -> Result<ReplicaHandle> {
        if range.0 == 0 || range.0 == LIVENESS_GROUP {
            // 0 is the R4 single-group identity; u64::MAX carries node liveness.
            return Err(Error::Identity(format!(
                "range id {} is reserved",
                range.0
            )));
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
        let mut log = LogStore::open(&self.config.log_dir.join(&dir), cluster, self.config.node_id)?;
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

    pub fn group(&self, range: RangeId) -> Option<ReplicaHandle> {
        let groups = self.groups.lock().expect("range host mutex poisoned");
        groups.get(&range).map(|g| g.handle.clone())
    }

    pub fn groups(&self) -> Vec<RangeId> {
        let groups = self.groups.lock().expect("range host mutex poisoned");
        groups.keys().copied().collect()
    }

    /// Shuts every group down and joins its replica thread. Idempotent.
    pub fn stop(&self) {
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
        self.roles.service(range)
    }
    fn system(&self) -> Option<Arc<dyn SystemService>> {
        self.roles.system()
    }
}

fn core(e: giggles_core::CoreError) -> Error {
    Error::Core(e.to_string())
}
