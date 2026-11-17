//! Track C owns this module.
//! A gateway's SQL database in a cluster (GD-0075 R5-43): an `Admin` whose v2 backend runs
//! every transaction through `ClusterTxnServices` over a `RangeClient`. The SQL engine is the
//! single-node one; only reads, locks, commits, ids and sequences go to range leaders.
use std::sync::Arc;

use super::coordinator::deadlock::DeadlockConfig;
use super::txn::{ClusterConfig, ClusterTxnServices};
use crate::CoreError;
use crate::range_api::{GatewayPeer, RangeClient};
use crate::storage::{Admin, Backend, Limits, v2::V2Db};

#[cfg(test)]
pub(crate) mod local;
#[cfg(test)]
mod serializable_tests;
#[cfg(test)]
mod tests;

/// A gateway: its SQL admin and its transaction services (for the peer dispatcher).
pub(crate) struct Gateway {
    pub(crate) admin: Admin,
    pub(crate) services: Arc<ClusterTxnServices>,
}

/// Attaches a gateway database for the cluster `client` reaches. `domain` is the cluster
/// database's identity (the range store's domain). With `peer`, the gateway detects deadlocks
/// across ranges and gateways (design.md 3.2); its `services.gateway_service()` must then be
/// reachable by the other gateways' peers.
pub(crate) fn attach(
    client: Arc<dyn RangeClient>,
    config: ClusterConfig,
    peer: Option<(Arc<dyn GatewayPeer>, DeadlockConfig)>,
    domain: [u8; 16],
    limits: Limits,
) -> Result<Gateway, CoreError> {
    let services = match peer {
        Some((peer, deadlock)) => ClusterTxnServices::with_peer(client, config, peer, deadlock),
        None => ClusterTxnServices::new(client, config),
    };
    let db = V2Db::cluster(services.clone(), domain)?;
    let admin = Admin::attach_backend(Backend::V2(Arc::new(db)), limits)?;
    Ok(Gateway { admin, services })
}
