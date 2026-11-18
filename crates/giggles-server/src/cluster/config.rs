//! The optional `cluster` section of the server configuration (validation only).
use crate::replication::config::{PeerConfig, decode_hex, resolve_dirs};
use giggles_core::range_api;
use serde::{Deserialize, Serialize};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

/// Largest number of other members a node may list.
const MAX_PEERS: usize = 63;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    /// 1..=65535; it becomes the gateway's u16 transaction-id prefix.
    pub node_id: u64,
    /// Database cluster id (32 lowercase hex), the RangeStore domain.
    pub cluster: String,
    /// Peer transport address (Raft and range calls).
    pub listen: SocketAddr,
    /// Every other member of the cluster.
    #[serde(default)]
    pub peers: Vec<PeerConfig>,
    /// Raft logs, one subdirectory per range; separate from the database.
    pub log_dir: PathBuf,
    pub snapshot_dir: Option<PathBuf>,
    pub certificate: Option<PathBuf>,
    pub private_key: Option<PathBuf>,
    pub ca: Option<PathBuf>,
    #[serde(default = "default_tick_ms")]
    pub tick_ms: u64,
    /// Peer range-call reply deadline in milliseconds (10000..=600000).
    #[serde(default = "default_peer_call_timeout_ms")]
    pub peer_call_timeout_ms: u64,
    pub replication_factor: Option<usize>,
    /// Initial layout members; excludes a joining spare from bootstrap Raft groups.
    pub initial_members: Option<Vec<u64>>,
    /// Development only: initial split points as hex range keys.
    #[serde(default)]
    pub split_keys: Vec<String>,
    /// Bootstrap superuser, as `replication.owner`.
    pub owner: Option<String>,
    /// How long a gateway serves after its last renewed cluster membership (DDL across
    /// gateways); below the range lock lease. Default 6000 (range failover under load must fit in it).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gateway_lease_ms: Option<u64>,
    /// Enable live range splits at this approximate size.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub split_size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub split_interval_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub split_cooldown_ms: Option<u64>,
}

/// Validated independently of the optional RocksDB replication runtime.
#[derive(Clone, Copy, Debug)]
pub struct SplitPolicy {
    pub threshold: u64,
    pub interval: Duration,
    pub cooldown: Duration,
}

/// Default gateway membership lease.
const DEFAULT_GATEWAY_LEASE: Duration = Duration::from_secs(6);
/// The range lock lease the runtime's engines use (`EngineSettings::default().lease`, also
/// rechecked when the gateway attaches); a gateway lease must stay below it.
const LOCK_LEASE: Duration = Duration::from_secs(10);

fn default_tick_ms() -> u64 {
    100
}

fn default_peer_call_timeout_ms() -> u64 {
    15000
}

#[derive(Debug)]
pub struct ClusterValidated {
    pub node_id: u64,
    pub cluster: [u8; 16],
    pub listen: SocketAddr,
    /// Every member id including this node, ascending.
    pub members: Vec<u64>,
    pub initial_members: Vec<u64>,
    pub peers: Vec<(u64, SocketAddr, String)>,
    pub log_dir: PathBuf,
    pub snapshot_dir: PathBuf,
    pub certificate: PathBuf,
    pub private_key: PathBuf,
    pub ca: PathBuf,
    pub tick: Duration,
    pub peer_call_timeout: Duration,
    pub replication_factor: usize,
    /// Strictly increasing range keys, each above `range_api::first_user_key()`.
    pub split_keys: Vec<Vec<u8>>,
    pub owner: Option<String>,
    /// Below the range lock lease the runtime's engines use (`LOCK_LEASE`).
    pub gateway_lease: Duration,
    pub split_policy: Option<SplitPolicy>,
}

impl ClusterValidated {
    /// The smallest initial member id. That node alone writes the initial range directory and the
    /// bootstrap owner, so every member agrees on who bootstraps without coordination.
    pub fn bootstrap_node(&self) -> u64 {
        self.initial_members[0]
    }
}

fn decode_range_key(value: &str, field: &str) -> Result<Vec<u8>, String> {
    if value.is_empty()
        || !value.len().is_multiple_of(2)
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(format!(
            "{field}: expected a non-empty even number of lowercase hex characters"
        ));
    }
    let key = hex::decode(value).map_err(|e| format!("{field}: {e}"))?;
    if range_api::split_range_key(&key).is_none() {
        return Err(format!("{field}: not a range key"));
    }
    if key <= range_api::first_user_key() {
        return Err(format!("{field}: must be above the first user key"));
    }
    Ok(key)
}

impl ClusterConfig {
    pub fn validate(
        &self,
        database: &Path,
        certificate: &Path,
        private_key: &Path,
        ca: &Path,
    ) -> Result<ClusterValidated, String> {
        if !(1..=u64::from(u16::MAX)).contains(&self.node_id) {
            return Err("cluster.node_id: must be between 1 and 65535".into());
        }
        if self.owner.as_ref().is_some_and(|owner| owner.is_empty()) {
            return Err("cluster.owner: must be non-empty".into());
        }
        let cluster = decode_hex(&self.cluster, "cluster.cluster")?;
        if self.peers.len() > MAX_PEERS {
            return Err(format!("cluster.peers: at most {MAX_PEERS} peers"));
        }
        if !(10..=1000).contains(&self.tick_ms) {
            return Err("cluster.tick_ms: must be between 10 and 1000".into());
        }
        let gateway_lease = self
            .gateway_lease_ms
            .map_or(DEFAULT_GATEWAY_LEASE, Duration::from_millis);
        if gateway_lease < Duration::from_millis(100) || gateway_lease >= LOCK_LEASE {
            return Err(format!(
                "cluster.gateway_lease_ms: must be at least 100 and below the range lock lease ({} ms)",
                LOCK_LEASE.as_millis()
            ));
        }
        let split_policy = if let Some(threshold) = self.split_size_bytes {
            if threshold < 65536 {
                return Err("cluster.split_size_bytes: must be at least 65536".into());
            }
            let interval = self.split_interval_ms.unwrap_or(1000);
            if !(100..=3600000).contains(&interval) {
                return Err("cluster.split_interval_ms: must be between 100 and 3600000".into());
            }
            let cooldown = self.split_cooldown_ms.unwrap_or(10000);
            if cooldown > 86400000 {
                return Err("cluster.split_cooldown_ms: must be at most 86400000".into());
            }
            Some(SplitPolicy {
                threshold,
                interval: Duration::from_millis(interval),
                cooldown: Duration::from_millis(cooldown),
            })
        } else {
            if self.split_interval_ms.is_some() || self.split_cooldown_ms.is_some() {
                return Err(
                    "cluster.split_size_bytes: required with split interval or cooldown".into(),
                );
            }
            None
        };
        if !(10000..=600000).contains(&self.peer_call_timeout_ms) {
            return Err("cluster.peer_call_timeout_ms: must be between 10000 and 600000".into());
        }
        let (log_dir, snapshot_dir) = resolve_dirs(
            "cluster",
            &self.log_dir,
            self.snapshot_dir.as_deref(),
            database,
        )?;
        let mut members = vec![self.node_id];
        let mut peers: Vec<(u64, SocketAddr, String)> = Vec::with_capacity(self.peers.len());
        for (i, peer) in self.peers.iter().enumerate() {
            let field = format!("cluster.peers[{i}]");
            if !(1..=u64::from(u16::MAX)).contains(&peer.id) {
                return Err(format!("{field}.id: must be between 1 and 65535"));
            }
            if peer.id == self.node_id {
                return Err(format!("{field}.id: equals cluster.node_id"));
            }
            if members.contains(&peer.id) {
                return Err(format!("{field}.id: duplicate member id {}", peer.id));
            }
            members.push(peer.id);
            if peer.address == self.listen {
                return Err(format!("{field}.address: equals cluster.listen"));
            }
            decode_hex::<32>(&peer.fingerprint, &format!("{field}.fingerprint"))?;
            if peers.iter().any(|(_, _, fp)| fp == &peer.fingerprint) {
                return Err(format!("{field}.fingerprint: duplicate fingerprint"));
            }
            peers.push((peer.id, peer.address, peer.fingerprint.clone()));
        }
        members.sort_unstable();
        let initial_members = self
            .initial_members
            .clone()
            .unwrap_or_else(|| members.clone());
        if initial_members.is_empty()
            || initial_members.windows(2).any(|w| w[0] >= w[1])
            || initial_members.iter().any(|id| !members.contains(id))
        {
            return Err(
                "cluster.initial_members: must be sorted, unique configured member ids".into(),
            );
        }
        let replication_factor = match self.replication_factor {
            Some(n) if !(1..=initial_members.len()).contains(&n) => {
                return Err(format!(
                    "cluster.replication_factor: must be between 1 and {} (the initial member count)",
                    initial_members.len()
                ));
            }
            Some(n) => n,
            None => initial_members.len().min(3),
        };
        let mut split_keys: Vec<Vec<u8>> = Vec::with_capacity(self.split_keys.len());
        for (i, key) in self.split_keys.iter().enumerate() {
            let field = format!("cluster.split_keys[{i}]");
            let key = decode_range_key(key, &field)?;
            if split_keys.last().is_some_and(|last| *last >= key) {
                return Err(format!("{field}: must be above the previous split key"));
            }
            split_keys.push(key);
        }
        Ok(ClusterValidated {
            node_id: self.node_id,
            cluster,
            listen: self.listen,
            members,
            initial_members,
            peers,
            log_dir,
            snapshot_dir,
            certificate: self
                .certificate
                .clone()
                .unwrap_or_else(|| certificate.into()),
            private_key: self
                .private_key
                .clone()
                .unwrap_or_else(|| private_key.into()),
            ca: self.ca.clone().unwrap_or_else(|| ca.into()),
            tick: Duration::from_millis(self.tick_ms),
            peer_call_timeout: Duration::from_millis(self.peer_call_timeout_ms),
            replication_factor,
            split_keys,
            owner: self.owner.clone(),
            gateway_lease,
            split_policy,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use giggles_core::range_api::{RangeSpace, range_key};

    fn peer(id: u64, port: u16, byte: &str) -> PeerConfig {
        PeerConfig {
            id,
            address: SocketAddr::from(([127, 0, 0, 1], port)),
            fingerprint: byte.repeat(32),
        }
    }

    #[test]
    fn operators_are_not_a_cluster_config_field() {
        let mut value = serde_json::to_value(config()).unwrap();
        value["operators"] = serde_json::json!([]);
        assert!(serde_json::from_value::<ClusterConfig>(value).is_err());
    }

    fn config() -> ClusterConfig {
        ClusterConfig {
            node_id: 3,
            cluster: "0123456789abcdef0123456789abcdef".into(),
            listen: "127.0.0.1:7103".parse().unwrap(),
            peers: vec![peer(2, 7102, "02"), peer(1, 7101, "01")],
            log_dir: "cluster-log".into(),
            snapshot_dir: None,
            certificate: None,
            private_key: None,
            ca: None,
            tick_ms: 100,
            peer_call_timeout_ms: default_peer_call_timeout_ms(),
            replication_factor: None,
            initial_members: None,
            split_keys: Vec::new(),
            owner: None,
            gateway_lease_ms: None,
            split_size_bytes: None,
            split_interval_ms: None,
            split_cooldown_ms: None,
        }
    }

    fn validate(c: &ClusterConfig) -> Result<ClusterValidated, String> {
        c.validate(
            Path::new("database"),
            Path::new("server.pem"),
            Path::new("server.key"),
            Path::new("ca.pem"),
        )
    }

    fn reject(c: &ClusterConfig, field: &str) {
        let error = validate(c).unwrap_err();
        assert!(error.starts_with(field), "{field} vs {error}");
    }

    fn key(space: RangeSpace, user: &[u8]) -> String {
        hex::encode(range_key(space, user))
    }

    #[test]
    fn validates_spare_initial_members() {
        let mut c = config();
        c.initial_members = Some(vec![1, 2]);
        assert_eq!(validate(&c).unwrap().initial_members, [1, 2]);
        assert_eq!(validate(&c).unwrap().replication_factor, 2);
        c.replication_factor = Some(3);
        assert!(validate(&c).is_err());
        c.replication_factor = None;
        c.initial_members = Some(vec![2, 3]);
        assert_eq!(validate(&c).unwrap().bootstrap_node(), 2);
        for ids in [vec![], vec![2, 1], vec![1, 1], vec![1, 4]] {
            c.initial_members = Some(ids);
            assert!(validate(&c).is_err());
        }
    }

    #[test]
    fn three_nodes_resolve_defaults() {
        let mut c = config();
        c.owner = Some("owner".into());
        c.split_keys = vec![key(RangeSpace::Rows, b"m"), key(RangeSpace::Index, b"")];
        let v = validate(&c).unwrap();
        assert_eq!(v.node_id, 3);
        assert_eq!(v.members, [1, 2, 3]);
        assert_eq!(v.bootstrap_node(), 1);
        assert_eq!(v.peers.len(), 2);
        assert_eq!(v.cluster, hex::decode(&c.cluster).unwrap().as_slice());
        assert_eq!(v.replication_factor, 3);
        assert_eq!(v.tick, Duration::from_millis(100));
        assert!(v.log_dir.is_absolute());
        assert_eq!(
            v.snapshot_dir,
            v.log_dir.with_file_name("cluster-log.snapshots")
        );
        assert_eq!(v.certificate, Path::new("server.pem"));
        assert_eq!(v.private_key, Path::new("server.key"));
        assert_eq!(v.ca, Path::new("ca.pem"));
        assert_eq!(v.split_keys[0], range_key(RangeSpace::Rows, b"m"));
        assert_eq!(v.owner.as_deref(), Some("owner"));
        assert_eq!(v.gateway_lease, Duration::from_secs(6));
        c.replication_factor = Some(2);
        assert_eq!(validate(&c).unwrap().replication_factor, 2);
        c.peers.extend([peer(4, 7104, "04"), peer(5, 7105, "05")]);
        c.replication_factor = None;
        assert_eq!(validate(&c).unwrap().replication_factor, 3);
    }

    #[test]
    fn single_node_and_explicit_paths() {
        let mut c = config();
        c.node_id = 65535;
        c.peers.clear();
        c.snapshot_dir = Some("snapshots".into());
        c.certificate = Some("peer.pem".into());
        c.private_key = Some("peer.key".into());
        c.ca = Some("peer-ca.pem".into());
        let v = validate(&c).unwrap();
        assert_eq!(v.members, [65535]);
        assert_eq!(v.bootstrap_node(), 65535);
        assert_eq!(v.replication_factor, 1);
        assert!(v.snapshot_dir.ends_with("snapshots"));
        assert_eq!(v.certificate, Path::new("peer.pem"));
        assert_eq!(v.private_key, Path::new("peer.key"));
        assert_eq!(v.ca, Path::new("peer-ca.pem"));
    }

    #[test]
    fn rejects_node_ids() {
        for node_id in [0, 65536] {
            reject(
                &ClusterConfig {
                    node_id,
                    ..config()
                },
                "cluster.node_id:",
            );
        }
        for id in [0, 65536, 3, 2] {
            let mut c = config();
            c.peers[1].id = id;
            reject(&c, "cluster.peers[1].id:");
        }
        let mut c = config();
        c.peers = (1..=64).map(|id| peer(id + 10, 8000, "00")).collect();
        reject(&c, "cluster.peers:");
    }

    #[test]
    fn rejects_peer_address_and_fingerprints() {
        let mut c = config();
        c.peers[1].address = c.listen;
        reject(&c, "cluster.peers[1].address:");
        for fingerprint in ["".into(), "AA".repeat(32), "gg".repeat(32), "02".repeat(32)] {
            let mut c = config();
            c.peers[1].fingerprint = fingerprint;
            reject(&c, "cluster.peers[1].fingerprint:");
        }
        for cluster in ["ab", "0123456789ABCDEF0123456789abcdef"] {
            reject(
                &ClusterConfig {
                    cluster: cluster.into(),
                    ..config()
                },
                "cluster.cluster:",
            );
        }
    }

    #[test]
    fn rejects_tick_factor_and_owner() {
        for tick_ms in [9, 1001] {
            reject(
                &ClusterConfig {
                    tick_ms,
                    ..config()
                },
                "cluster.tick_ms:",
            );
        }
        for factor in [0, 4] {
            let c = ClusterConfig {
                replication_factor: Some(factor),
                ..config()
            };
            reject(&c, "cluster.replication_factor:");
        }
        reject(
            &ClusterConfig {
                owner: Some(String::new()),
                ..config()
            },
            "cluster.owner:",
        );
        for lease in [0, 99, 10_000, u64::MAX] {
            let c = ClusterConfig {
                gateway_lease_ms: Some(lease),
                ..config()
            };
            reject(&c, "cluster.gateway_lease_ms:");
        }
        let c = ClusterConfig {
            gateway_lease_ms: Some(1500),
            ..config()
        };
        assert_eq!(
            validate(&c).unwrap().gateway_lease,
            Duration::from_millis(1500)
        );
    }

    #[test]
    fn validates_peer_call_timeout() {
        for ms in [10000, 15000, 600000] {
            let c = ClusterConfig {
                peer_call_timeout_ms: ms,
                ..config()
            };
            assert_eq!(
                validate(&c).unwrap().peer_call_timeout,
                Duration::from_millis(ms)
            );
        }
        for ms in [0, 1000, 2000, 9999, 600001, u64::MAX] {
            let c = ClusterConfig {
                peer_call_timeout_ms: ms,
                ..config()
            };
            reject(&c, "cluster.peer_call_timeout_ms:");
        }
    }

    #[test]
    fn rejects_split_keys() {
        let first = hex::encode(range_api::first_user_key());
        let catalog = key(RangeSpace::Catalog, b"x");
        for bad in [
            "",
            "8",
            "8G",
            "AB",
            "ff00",
            catalog.as_str(),
            first.as_str(),
        ] {
            let c = ClusterConfig {
                split_keys: vec![bad.into()],
                ..config()
            };
            reject(&c, "cluster.split_keys[0]:");
        }
        for order in [["8062", "8061"], ["8061", "8061"]] {
            let c = ClusterConfig {
                split_keys: order.map(String::from).to_vec(),
                ..config()
            };
            reject(&c, "cluster.split_keys[1]:");
        }
    }

    #[test]
    fn rejects_overlapping_directories() {
        for path in ["database", "database/log", "."] {
            reject(
                &ClusterConfig {
                    log_dir: path.into(),
                    ..config()
                },
                "cluster.log_dir:",
            );
        }
        for path in [
            "database",
            "database/snap",
            "cluster-log",
            "cluster-log/snap",
            ".",
        ] {
            let c = ClusterConfig {
                snapshot_dir: Some(path.into()),
                ..config()
            };
            reject(&c, "cluster.snapshot_dir:");
        }
    }

    #[test]
    fn split_policy_defaults_and_bounds() {
        let mut c = config();
        assert!(validate(&c).unwrap().split_policy.is_none());
        c.split_size_bytes = Some(65536);
        let policy = validate(&c).unwrap().split_policy.unwrap();
        assert_eq!(policy.threshold, 65536);
        assert_eq!(policy.interval, Duration::from_millis(1000));
        assert_eq!(policy.cooldown, Duration::from_millis(10000));
        for (interval, cooldown) in [(100, 0), (3600000, 86400000)] {
            c.split_interval_ms = Some(interval);
            c.split_cooldown_ms = Some(cooldown);
            let policy = validate(&c).unwrap().split_policy.unwrap();
            assert_eq!(policy.interval, Duration::from_millis(interval));
            assert_eq!(policy.cooldown, Duration::from_millis(cooldown));
        }
    }

    #[test]
    fn rejects_invalid_split_policy() {
        let mut c = config();
        for size in [0, 65535] {
            c.split_size_bytes = Some(size);
            reject(&c, "cluster.split_size_bytes:");
        }
        c.split_size_bytes = Some(65536);
        for interval in [0, 99, 3600001, u64::MAX] {
            c.split_interval_ms = Some(interval);
            reject(&c, "cluster.split_interval_ms:");
        }
        c.split_interval_ms = None;
        for cooldown in [86400001, u64::MAX] {
            c.split_cooldown_ms = Some(cooldown);
            reject(&c, "cluster.split_cooldown_ms:");
        }
        c.split_size_bytes = None;
        c.split_cooldown_ms = Some(1000);
        reject(&c, "cluster.split_size_bytes:");
        c.split_cooldown_ms = None;
        c.split_interval_ms = Some(200);
        reject(&c, "cluster.split_size_bytes:");
    }

    #[test]
    fn serde_round_trip_defaults_and_unknown_fields() {
        let value = serde_json::to_value(config()).unwrap();
        for field in ["split_size_bytes", "split_interval_ms", "split_cooldown_ms"] {
            assert!(value.get(field).is_none(), "{field} should be omitted");
        }
        let mut enabled = config();
        enabled.split_size_bytes = Some(65536);
        enabled.split_interval_ms = Some(200);
        enabled.split_cooldown_ms = Some(1000);
        let enabled: ClusterConfig =
            serde_json::from_value(serde_json::to_value(enabled).unwrap()).unwrap();
        assert_eq!(enabled.split_size_bytes, Some(65536));
        assert_eq!(enabled.split_interval_ms, Some(200));
        assert_eq!(enabled.split_cooldown_ms, Some(1000));
        let back: ClusterConfig = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(&back).unwrap(), value);
        let minimal = serde_json::json!({
            "node_id": 1, "cluster": "0123456789abcdef0123456789abcdef",
            "listen": "127.0.0.1:7101", "log_dir": "cluster-log"
        });
        let c: ClusterConfig = serde_json::from_value(minimal.clone()).unwrap();
        assert_eq!(c.tick_ms, 100);
        assert_eq!(c.peer_call_timeout_ms, 15000);
        assert_eq!(
            validate(&c).unwrap().peer_call_timeout,
            Duration::from_secs(15)
        );
        assert!(c.peers.is_empty() && c.split_keys.is_empty() && c.replication_factor.is_none());
        let mut unknown = minimal.clone();
        unknown["surprise"] = true.into();
        assert!(serde_json::from_value::<ClusterConfig>(unknown).is_err());
        let mut unknown = value;
        unknown["peers"][0]["surprise"] = true.into();
        assert!(serde_json::from_value::<ClusterConfig>(unknown).is_err());
    }
}
