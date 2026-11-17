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
mod matrix_tests;
#[cfg(test)]
mod serializable_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod stream_tests;

/// A gateway: its SQL admin and its transaction services (for the peer dispatcher).
pub(crate) struct Gateway {
    pub(crate) admin: Admin,
    pub(crate) services: Arc<ClusterTxnServices>,
}

/// Attaches a gateway database for the cluster `client` reaches. `domain` is the cluster
/// database's identity (the range store's domain). With `peer`, the gateway detects deadlocks
/// across ranges and gateways (design.md 3.2); its `services.gateway_service()` must then be
/// reachable by the other gateways' peers. With a member list (`config.gateways`), the gateway
/// joins the cluster before its catalog is loaded (Phase 11, `txn::ddl`).
pub(crate) fn attach(
    client: Arc<dyn RangeClient>,
    config: ClusterConfig,
    peer: Option<(Arc<dyn GatewayPeer>, DeadlockConfig)>,
    domain: [u8; 16],
    limits: Limits,
) -> Result<Gateway, CoreError> {
    check_members(&config, peer.is_some())?;
    let services = match peer {
        Some((peer, deadlock)) => ClusterTxnServices::with_peer(client, config, peer, deadlock),
        None => ClusterTxnServices::new(client, config),
    };
    services.join_cluster()?;
    let db = Arc::new(V2Db::cluster(services.clone(), domain)?);
    let weak = Arc::downgrade(&db);
    services.set_catalog_refresh(Box::new(move || {
        if let Some(db) = weak.upgrade() {
            let _ = db.catalog().refresh_latest();
        }
    }));
    let admin = Admin::attach_backend(Backend::V2(db), limits)?;
    Ok(Gateway { admin, services })
}

/// A member list (Phase 11) must name this gateway, come with a peer when it names others,
/// and have a positive liveness lease. (The node checks the lease against its lock lease.)
fn check_members(config: &ClusterConfig, peer: bool) -> Result<(), CoreError> {
    let gateways = &config.gateways;
    let problem = if gateways.is_empty() {
        return Ok(());
    } else if !gateways.contains(&config.node) {
        format!(
            "gateway {} is not among the gateways {gateways:?}",
            config.node
        )
    } else if !peer && gateways.iter().any(|node| *node != config.node) {
        "a gateway with other members needs a gateway peer".to_string()
    } else if config.gateway_lease.is_zero() {
        "the gateway lease must be positive".to_string()
    } else {
        return Ok(());
    };
    Err(CoreError::Storage(format!(
        "invalid gateway settings: {problem}"
    )))
}
