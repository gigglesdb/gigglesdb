//! Public facade over one cluster node's range engines and SQL gateway (GD-0075 Phase 7).
//!
//! giggles-replication and giggles-server run real range engines and gateways over real Raft
//! groups through these thin wrappers; the engines (`range::engine`) and the gateway
//! (`cluster::db`) stay crate-private and behave exactly as in the in-process test cluster.
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use super::cluster::coordinator::deadlock::DeadlockConfig;
use super::cluster::db;
use super::cluster::txn::{ClusterConfig, ClusterTxnServices};
use super::range::engine::{EngineConfig, RangeEngines};
use super::range::home::RecoveryConfig;
use crate::range_api::{
    GatewayPeer, GatewayService, LeaseKind, RangeClient, RangeEngineOpener, RangeError, SequenceOp,
    SequenceReply, SystemService, Timestamp, TsOracle,
};
use crate::replicated::RangeStore;
use crate::{Admin, CoreError, Limits};

/// Engine tunables a node may set. `Default` is the engines' own default.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineSettings {
    /// Lock lease of a transaction that has not prepared (renewed by heartbeat).
    pub lease: Duration,
    /// How long a read waits for in-flight commits and prepared records at or below its ts.
    pub read_wait: Duration,
    /// Lease sweep period.
    pub sweep: Duration,
    /// How long a read or lock waits on a prepared transaction before asking its home.
    pub resolve_after: Duration,
    /// An undecided home record idle this long is decided Abort.
    pub recover_after: Duration,
    /// A decided home record idle this long is finalized everywhere and forgotten.
    pub finish_after: Duration,
    /// Period of the home-record recovery worker.
    pub recovery_tick: Duration,
    /// Reads older than this TTL are refused.
    pub gc_ttl: Duration,
    /// Period of the independent dropped-object reclaim worker.
    pub reclaim_every: Duration,
}

impl Default for EngineSettings {
    fn default() -> Self {
        let config = EngineConfig::default();
        Self {
            lease: config.lease,
            read_wait: config.read_wait,
            sweep: config.sweep,
            resolve_after: config.resolve_after,
            recover_after: config.recovery.recover_after,
            finish_after: config.recovery.finish_after,
            recovery_tick: config.recovery_tick,
            gc_ttl: config.gc_ttl,
            reclaim_every: config.reclaim_every,
        }
    }
}

impl EngineSettings {
    fn config(&self) -> EngineConfig {
        EngineConfig {
            lease: self.lease,
            read_wait: self.read_wait,
            sweep: self.sweep,
            resolve_after: self.resolve_after,
            recovery: RecoveryConfig {
                recover_after: self.recover_after,
                finish_after: self.finish_after,
            },
            recovery_tick: self.recovery_tick,
            gc_ttl: self.gc_ttl,
            reclaim_every: self.reclaim_every,
        }
    }
}

/// The engines' oracle: the local system engine while it is open here, else the client's
/// oracle (the system range's leader elsewhere) once `NodeEngines::set_client` ran.
struct NodeOracle {
    engines: OnceLock<Weak<RangeEngines>>,
    client: OnceLock<Weak<dyn RangeClient>>,
}

impl TsOracle for NodeOracle {
    fn next(&self, n: u32) -> Result<Timestamp, RangeError> {
        if let Some(system) = self
            .engines
            .get()
            .and_then(Weak::upgrade)
            .and_then(|engines| engines.system())
        {
            return system.next(n);
        }
        match self.client.get().and_then(Weak::upgrade) {
            Some(client) => client.oracle().next(n),
            None => Err(RangeError::NotLeader { hint: None }),
        }
    }
}

/// Whichever system engine is open on this node; NotLeader otherwise.
struct NodeSystem(Weak<RangeEngines>);

impl NodeSystem {
    fn open(&self) -> Result<Arc<dyn SystemService>, RangeError> {
        self.0
            .upgrade()
            .and_then(|engines| engines.system())
            .map(|system| system as Arc<dyn SystemService>)
            .ok_or(RangeError::NotLeader { hint: None })
    }
}

impl TsOracle for NodeSystem {
    fn next(&self, n: u32) -> Result<Timestamp, RangeError> {
        self.open()?.oracle().next(n)
    }
}

impl SystemService for NodeSystem {
    fn oracle(&self) -> &dyn TsOracle {
        self
    }
    fn lease(
        &self,
        kind: LeaseKind,
        id: u64,
        count: u64,
    ) -> Result<std::ops::Range<u64>, RangeError> {
        self.open()?.lease(kind, id, count)
    }
    fn sequence(
        &self,
        id: u64,
        incarnation: u128,
        op: SequenceOp,
    ) -> Result<SequenceReply, RangeError> {
        self.open()?.sequence(id, incarnation, op)
    }
}

/// This node's range engines, over its one `RangeStore`.
pub struct NodeEngines {
    engines: Arc<RangeEngines>,
    oracle: Arc<NodeOracle>,
    system: Arc<NodeSystem>,
}

impl NodeEngines {
    /// The engines' oracle is late-bound: the local system engine if open, else the client's
    /// oracle once `set_client` ran, else NotLeader { hint: None }.
    pub fn new(store: Arc<RangeStore>, settings: EngineSettings) -> Arc<Self> {
        let oracle = Arc::new(NodeOracle {
            engines: OnceLock::new(),
            client: OnceLock::new(),
        });
        let engines = Arc::new(RangeEngines::new(store, oracle.clone(), settings.config()));
        let _ = oracle.engines.set(Arc::downgrade(&engines));
        Arc::new(Self {
            system: Arc::new(NodeSystem(Arc::downgrade(&engines))),
            engines,
            oracle,
        })
    }

    /// The opener every range group's role manager calls after its leader's read barrier.
    pub fn opener(&self) -> Arc<dyn RangeEngineOpener> {
        self.engines.clone()
    }

    /// A SystemService that forwards to whichever system engine is open on this node
    /// (NotLeader { hint: None } otherwise). Stable handle: give it once to RangeHost::set_system.
    pub fn system_service(&self) -> Arc<dyn SystemService> {
        self.system.clone()
    }

    /// Gives the engines a client for other ranges (prepared-record resolution through homes,
    /// R5-53, and the home-record recovery worker, R5-52) and binds the oracle's fallback.
    /// Weak, because the client normally owns the node. Set once; later calls are ignored.
    pub fn set_client(&self, client: Weak<dyn RangeClient>) {
        self.engines.set_client(client.clone());
        let _ = self.oracle.client.set(client);
    }
}

/// Gateway tunables. `Default` is the gateway's own default.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GatewaySettings {
    /// This gateway's node id (< 2^16): the high bits of every cluster transaction id.
    pub node: u16,
    /// Lock-lease heartbeat period for transactions holding range locks.
    pub heartbeat: Duration,
    /// A blocking range lock is requested in slices this long so cancellation is seen.
    pub lock_slice: Duration,
    /// Longest any lock wait lasts (55P03), with or without deadlock detection.
    pub lock_backstop: Duration,
    /// Attempts for NotLeader / Unavailable / StaleEpoch before giving up.
    pub retries: u32,
    /// Byte budget of one range read page.
    pub page_bytes: usize,
    /// Byte budget of one transaction's write set.
    pub write_set_bytes: usize,
    /// A lock wait this old starts deadlock probes (PostgreSQL's deadlock_timeout).
    pub deadlock_timeout: Duration,
    /// Scan period of the deadlock detector.
    pub deadlock_tick: Duration,
    /// Every gateway of the cluster, this one included (DDL across gateways, Phase 11).
    /// Empty: single-gateway mode. Non-empty: attaching joins the cluster first.
    pub gateways: Vec<u16>,
    /// How long a renewed liveness lock lets the gateway serve; must stay below the range
    /// lock lease (`EngineSettings::lease`) so a cut-off gateway fences before its locks expire.
    pub gateway_lease: Duration,
    /// Transaction lock lease at the ranges; must match EngineSettings::lease.
    pub lock_lease: Duration,
}

impl Default for GatewaySettings {
    fn default() -> Self {
        let cluster = ClusterConfig::default();
        let deadlock = DeadlockConfig::default();
        Self {
            node: cluster.node,
            heartbeat: cluster.heartbeat,
            lock_slice: cluster.lock_slice,
            lock_backstop: cluster.lock_backstop,
            retries: cluster.retries,
            page_bytes: cluster.page_bytes,
            write_set_bytes: cluster.write_set_bytes,
            deadlock_timeout: deadlock.deadlock_timeout,
            deadlock_tick: deadlock.tick,
            gateways: cluster.gateways,
            gateway_lease: cluster.gateway_lease,
            lock_lease: cluster.lock_lease,
        }
    }
}

/// A gateway's SQL database and, with a peer, its deadlock service.
pub struct ClusterGateway {
    services: Arc<ClusterTxnServices>,
    pub admin: Admin,
    /// Serve it to the other gateways' peers (probe, abort, blockers, cancel_wait).
    pub gateway_service: Option<Arc<dyn GatewayService>>,
}

impl ClusterGateway {
    /// Number of membership fences since this gateway was attached.
    #[doc(hidden)]
    pub fn membership_fences(&self) -> u64 {
        self.services.membership_fences()
    }

    /// Supplies additional gateway members to close before DDL. Lookup failures refuse
    /// the DDL with a retryable serialization error.
    pub fn set_member_source(
        &self,
        source: Box<dyn Fn() -> Result<Vec<u16>, RangeError> + Send + Sync>,
    ) {
        self.services.set_member_source(source);
    }
}

/// Attaches a gateway database for the cluster `client` reaches. `domain` is the cluster
/// database's identity (the range store's domain); `peer` enables cross-gateway deadlock
/// detection. Attaching reads the catalog and writes nothing; with `settings.gateways` it first
/// joins the cluster (takes its liveness lock).
pub fn attach_gateway(
    client: Arc<dyn RangeClient>,
    settings: GatewaySettings,
    peer: Option<Arc<dyn GatewayPeer>>,
    domain: [u8; 16],
    limits: Limits,
) -> Result<ClusterGateway, CoreError> {
    let config = ClusterConfig {
        node: settings.node,
        write_set_bytes: settings.write_set_bytes,
        page_bytes: settings.page_bytes,
        heartbeat: settings.heartbeat,
        lock_slice: settings.lock_slice,
        lock_backstop: settings.lock_backstop,
        retries: settings.retries,
        gateways: settings.gateways,
        gateway_lease: settings.gateway_lease,
        lock_lease: settings.lock_lease,
    };
    let deadlock = DeadlockConfig {
        deadlock_timeout: settings.deadlock_timeout,
        lock_wait_backstop: settings.lock_backstop,
        tick: settings.deadlock_tick,
    };
    let gateway = db::attach(
        client,
        config,
        peer.map(|peer| (peer, deadlock)),
        domain,
        limits,
    )?;
    Ok(ClusterGateway {
        admin: gateway.admin,
        gateway_service: Some(gateway.services.gateway_service()),
        services: gateway.services,
    })
}

#[cfg(test)]
#[path = "cluster_node_tests.rs"]
mod tests;
