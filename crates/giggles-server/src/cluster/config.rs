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
    pub replication_factor: Option<usize>,
    /// Initial layout members; excludes a joining spare from bootstrap Raft groups.
    pub initial_members: Option<Vec<u64>>,
    /// Development only: initial split points as hex range keys.
    #[serde(default)]
    pub split_keys: Vec<String>,
    /// Bootstrap superuser, as `replication.owner`.
    pub owner: Option<String>,
}

fn default_tick_ms() -> u64 {
    100
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
    pub replication_factor: usize,
    /// Strictly increasing range keys, each above `range_api::first_user_key()`.
    pub split_keys: Vec<Vec<u8>>,
    pub owner: Option<String>,
}

impl ClusterValidated {
    /// The smallest member id. That node alone writes the initial range directory and the
    /// bootstrap owner, so every member agrees on who bootstraps without coordination.
    pub fn bootstrap_node(&self) -> u64 {
        self.members[0]
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
            Some(n) if !(1..=members.len()).contains(&n) => {
                return Err(format!(
                    "cluster.replication_factor: must be between 1 and {} (the member count)",
                    members.len()
                ));
            }
            Some(n) => n,
            None => members.len().min(3),
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
            replication_factor,
            split_keys,
            owner: self.owner.clone(),
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
            replication_factor: None,
            initial_members: None,
            split_keys: Vec::new(),
            owner: None,
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
    fn serde_round_trip_defaults_and_unknown_fields() {
        let value = serde_json::to_value(config()).unwrap();
        let back: ClusterConfig = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(&back).unwrap(), value);
        let minimal = serde_json::json!({
            "node_id": 1, "cluster": "0123456789abcdef0123456789abcdef",
            "listen": "127.0.0.1:7101", "log_dir": "cluster-log"
        });
        let c: ClusterConfig = serde_json::from_value(minimal.clone()).unwrap();
        assert_eq!(c.tick_ms, 100);
        assert!(c.peers.is_empty() && c.split_keys.is_empty() && c.replication_factor.is_none());
        let mut unknown = minimal.clone();
        unknown["surprise"] = true.into();
        assert!(serde_json::from_value::<ClusterConfig>(unknown).is_err());
        let mut unknown = value;
        unknown["peers"][0]["surprise"] = true.into();
        assert!(serde_json::from_value::<ClusterConfig>(unknown).is_err());
    }
}
