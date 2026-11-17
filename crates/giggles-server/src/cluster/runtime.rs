//! The cluster runtime (GD-0075 R5-61): this node's range groups, engines and routed client
//! (`giggles_replication::cluster_node`), and its SQL gateway. Every node is a gateway: the
//! server binds its PostgreSQL listeners over the gateway's `Admin` on every node.
//!
//! DDL across gateways (GD-0075 Phase 11): every member joins as a gateway; a DDL statement on
//! one gateway closes the others (drains their writers and the readers of its tables) until
//! it ends, and a gateway that loses its membership lease stops serving until it rejoins.
use super::config::ClusterValidated;
use giggles_core::{
    Admin, Limits,
    cluster_node::{ClusterGateway, EngineSettings, GatewaySettings},
    range_api::RangeClient,
};
use giggles_replication::{
    bootstrap::presplit,
    cluster_node::{ClusterNode, ClusterNodeConfig, RegistryWatcher},
    liveness::LivenessConfig,
    sink::SnapshotPolicy,
    split::{SizePolicy, SizeSplitter, Splitter},
    transport::{CallLimits, PeerConfig, TransportConfig, fingerprint},
};
use std::{path::Path, sync::Arc, time::Duration};

/// How long a starting node waits for the directory and every range's leader.
const READY_TIMEOUT: Duration = Duration::from_secs(600);
/// Reply deadline of a range call (a lock call adds its own wait). A leader cut off by a
/// silent partition answers nothing; the router tries another replica only after this, so it
/// bounds the stall of every statement behind such a call (GD-0075 P7-2). Kept well above the
/// engines' read wait (5 s): a read may legitimately wait that long on a prepared record, and a
/// call timeout drops the peer connection with every call on it (delta review D1).
const CALL_TIMEOUT: Duration = Duration::from_secs(15);

/// A running cluster node and its gateway. Dropping it stops neither; call `stop`.
pub struct ClusterRuntime {
    gateway: ClusterGateway,
    node: ClusterNode,
    registry_watcher: RegistryWatcher,
    size_splitter: Option<SizeSplitter>,
}

/// The peer transport of this node: every member, with this node's own certificate.
pub fn transport_config(v: &ClusterValidated) -> Result<TransportConfig, String> {
    let cert =
        crate::tls::certs(&v.certificate).map_err(|e| format!("cluster.certificate: {e}"))?;
    let cert = cert.first().ok_or("cluster.certificate: no certificates")?;
    let mut peers: std::collections::BTreeMap<_, _> = v
        .peers
        .iter()
        .map(|(id, address, fingerprint)| {
            (
                *id,
                PeerConfig {
                    address: address.to_string(),
                    fingerprint: fingerprint.clone(),
                },
            )
        })
        .collect();
    peers.insert(
        v.node_id,
        PeerConfig {
            address: v.listen.to_string(),
            fingerprint: fingerprint(cert),
        },
    );
    Ok(TransportConfig {
        node_id: v.node_id,
        cluster: v.cluster,
        listen: v.listen.to_string(),
        certificate: v.certificate.clone(),
        key: v.private_key.clone(),
        ca: v.ca.clone(),
        peers,
    })
}

impl ClusterRuntime {
    /// Starts the node, writes the initial directory (bootstrap node only), waits until every
    /// range is served, attaches the gateway and creates the bootstrap owner (bootstrap node,
    /// empty role catalog). Blocking: call from a blocking thread.
    pub fn start(v: &ClusterValidated, database: &Path) -> Result<Self, String> {
        let layout = presplit(&v.split_keys, &v.initial_members, v.replication_factor)
            .map_err(|e| format!("cluster.split_keys: {e}"))?;
        let node = ClusterNode::start(ClusterNodeConfig {
            transport: transport_config(v)?,
            data_dir: database.into(),
            log_dir: v.log_dir.clone(),
            snap_dir: v.snapshot_dir.clone(),
            tick: v.tick,
            layout,
            engine: EngineSettings::default(),
            liveness: Some(LivenessConfig::default()),
            policy: Some(SnapshotPolicy::default()),
            call_limits: Some(CallLimits {
                timeout: CALL_TIMEOUT,
                ..CallLimits::default()
            }),
        })
        .map_err(|e| format!("cluster node: {e}"))?;
        let bootstrap = v.node_id == v.bootstrap_node();
        if bootstrap {
            node.bootstrap_directory(READY_TIMEOUT)
                .map_err(|e| format!("cluster bootstrap: {e}"))?;
            if let Err(error) = node.enrol_members(READY_TIMEOUT) {
                eprintln!("cluster registry enrolment: {error}");
            }
        }
        let registry_watcher = node.start_registry_watcher(Duration::from_secs(1));
        node.wait_ready(READY_TIMEOUT)
            .map_err(|e| format!("cluster: {e}"))?;
        let gateway = node
            .attach_gateway(
                GatewaySettings {
                    node: u16::try_from(v.node_id).map_err(|_| "cluster.node_id: above 65535")?,
                    gateways: v
                        .members
                        .iter()
                        .map(|&id| u16::try_from(id))
                        .collect::<Result<_, _>>()
                        .map_err(|_| "cluster member id above 65535")?,
                    gateway_lease: v.gateway_lease,
                    ..GatewaySettings::default()
                },
                Limits::default(),
            )
            .map_err(|e| format!("cluster gateway: {e}"))?;
        if bootstrap && let Some(owner) = &v.owner {
            bootstrap_owner(&gateway.admin, owner)?;
        }
        let size_splitter = v.split_policy.map(|policy| {
            let client: Arc<dyn RangeClient> = node.client().clone();
            SizeSplitter::start(
                Arc::new(Splitter::new(node.host().clone(), client)),
                SizePolicy {
                    threshold: policy.threshold,
                    interval: policy.interval,
                    cooldown: policy.cooldown,
                },
            )
        });
        Ok(Self {
            gateway,
            node,
            registry_watcher,
            size_splitter,
        })
    }

    pub fn admin(&self) -> &Admin {
        &self.gateway.admin
    }

    pub fn node_id(&self) -> u64 {
        self.node.node_id()
    }

    pub fn stop(mut self) {
        if let Some(splitter) = &mut self.size_splitter {
            splitter.stop();
        }
        self.registry_watcher.stop();
        drop(self.gateway);
        self.node.stop();
    }
}

/// Creates the configured superuser when the role catalog is empty (as replication mode does).
fn bootstrap_owner(admin: &Admin, owner: &str) -> Result<(), String> {
    let roles = admin.roles();
    if !roles.is_empty().map_err(|e| e.to_string())? {
        return Ok(());
    }
    let password = std::env::var("GIGGLES_OWNER_PASSWORD")
        .ok()
        .filter(|p| !p.is_empty())
        .ok_or("GIGGLES_OWNER_PASSWORD is required to bootstrap cluster.owner")?;
    roles
        .create(giggles_core::roles::NewRole {
            name: owner.into(),
            superuser: true,
            login: true,
            createrole: true,
            password: Some(crate::dev::password_verifier(&password).map_err(|e| e.to_string())?),
        })
        .map_err(|e| format!("cluster.owner: {e}"))?;
    Ok(())
}
