//! Phase 4 entry point for a durable, mutually authenticated replication node.
use crate::{
    core_store::{ReplicatedEngine, slot_engine_opener},
    error::{Error, Result},
    log_store::LogStore,
    node::Replica,
    role::RoleManager,
    sink::{ReplicaHandle, SnapshotPolicy, spawn_with_policy},
    snapshot::SnapshotStore,
    store_slot::StoreSlot,
    transport::{LateSink, Transport, TransportConfig},
};
use std::{path::PathBuf, sync::Arc, thread::JoinHandle, time::Duration};

/// Persistent directories, voter identities and runtime policy for one node.
#[derive(Clone)]
pub struct NodeConfig {
    pub transport: TransportConfig,
    pub data_dir: PathBuf,
    pub log_dir: PathBuf,
    pub snap_dir: PathBuf,
    pub voters: Vec<u64>,
    pub tick: Duration,
    pub policy: SnapshotPolicy,
    pub limits: giggles_core::Limits,
    pub write_set_bytes: usize,
}

/// Owns the transport, Raft driver and term-bound SQL engine publisher.
/// Call `stop` to fence the engine and join the background threads.
pub struct Node {
    pub handle: ReplicaHandle,
    pub slot: Arc<StoreSlot>,
    pub roles: RoleManager<ReplicatedEngine>,
    transport: Transport,
    join: JoinHandle<()>,
}

impl Node {
    /// Open durable state and start serving peers. SQL becomes available via
    /// `roles` only after this node confirms leadership with a quorum.
    pub fn start(config: NodeConfig) -> Result<Self> {
        config.transport.validate()?;
        if config.tick.is_zero() {
            return Err(Error::Identity("tick interval must be positive".into()));
        }
        let cluster = config.transport.cluster;
        let slot = Arc::new(StoreSlot::open_or_create(&config.data_dir, cluster)?);
        let snapshots = Arc::new(SnapshotStore::open(&config.snap_dir, cluster)?);
        let mut log = LogStore::open(&config.log_dir, cluster, config.transport.node_id)?;
        log.attach_snapshots(snapshots.clone());
        let replica = Replica::new(log, slot.clone(), &config.voters)?;
        let late = Arc::new(LateSink::new());
        let transport = Transport::start(config.transport, late.clone(), Some(snapshots))?;
        let (handle, join) = spawn_with_policy(
            replica,
            transport.outbox(),
            config.tick,
            Some(config.policy),
        );
        late.set(handle.clone());
        let roles = RoleManager::start(
            handle.clone(),
            slot_engine_opener(
                slot.clone(),
                handle.clone(),
                config.limits,
                config.write_set_bytes,
            ),
            config.tick,
        );
        Ok(Self {
            handle,
            slot,
            roles,
            transport,
            join,
        })
    }

    /// Fence the published engine, stop and join Raft, then close peer connections.
    pub fn stop(self) {
        self.roles.stop();
        self.handle.shutdown();
        let joined = self.join.join();
        self.transport.shutdown();
        // Preserve a caller's original panic while still closing all sockets.
        if let Err(panic) = joined
            && !std::thread::panicking()
        {
            std::panic::resume_unwind(panic);
        }
    }
}
