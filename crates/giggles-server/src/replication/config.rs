use serde::{Deserialize, Serialize};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PeerConfig {
    pub id: u64,
    pub address: SocketAddr,
    pub fingerprint: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicationConfig {
    pub node_id: u64,
    pub cluster: String,
    pub listen: SocketAddr,
    pub log_dir: PathBuf,
    pub peers: Vec<PeerConfig>,
    pub snapshot_dir: Option<PathBuf>,
    pub certificate: Option<PathBuf>,
    pub private_key: Option<PathBuf>,
    pub ca: Option<PathBuf>,
    #[serde(default = "default_tick_ms")]
    pub tick_ms: u64,
    pub write_set_bytes: Option<usize>,
    pub owner: Option<String>,
}

fn default_tick_ms() -> u64 {
    100
}

#[derive(Debug)]
pub struct Validated {
    pub node_id: u64,
    pub cluster: [u8; 16],
    pub voters: Vec<u64>,
    pub listen: SocketAddr,
    pub log_dir: PathBuf,
    pub peers: Vec<(u64, SocketAddr, String)>,
    pub snapshot_dir: PathBuf,
    pub certificate: PathBuf,
    pub private_key: PathBuf,
    pub ca: PathBuf,
    pub tick: Duration,
    pub write_set_bytes: Option<usize>,
    pub owner: Option<String>,
}

fn decode_hex<const N: usize>(value: &str, field: &str) -> Result<[u8; N], String> {
    if value.len() != N * 2
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(format!(
            "{field}: expected exactly {} lowercase hex characters",
            N * 2
        ));
    }
    let mut result = [0; N];
    hex::decode_to_slice(value, &mut result).map_err(|e| format!("{field}: {e}"))?;
    Ok(result)
}

impl ReplicationConfig {
    pub fn validate(
        &self,
        database: &Path,
        certificate: &Path,
        private_key: &Path,
        ca: &Path,
    ) -> Result<Validated, String> {
        if self.node_id == 0 {
            return Err("replication.node_id: must be non-zero".into());
        }
        if self.owner.as_ref().is_some_and(|owner| owner.is_empty()) {
            return Err("replication.owner: must be non-empty".into());
        }
        let cluster = decode_hex(&self.cluster, "replication.cluster")?;
        if self.peers.len() != 2 {
            return Err("replication.peers: expected exactly two peers".into());
        }
        if !(10..=1000).contains(&self.tick_ms) {
            return Err("replication.tick_ms: must be between 10 and 1000".into());
        }
        if self
            .write_set_bytes
            .is_some_and(|n| !(1 << 20..=32 << 20).contains(&n))
        {
            return Err("replication.write_set_bytes: must be between 1 MiB and 32 MiB".into());
        }
        let log_dir =
            std::path::absolute(&self.log_dir).map_err(|e| format!("replication.log_dir: {e}"))?;
        let database = std::path::absolute(database)
            .map_err(|e| format!("replication.log_dir: invalid database path: {e}"))?;
        if log_dir.starts_with(&database) || database.starts_with(&log_dir) {
            return Err("replication.log_dir: must be separate from database".into());
        }
        let snapshot_dir = match &self.snapshot_dir {
            Some(path) => path.clone(),
            None => {
                let name = log_dir
                    .file_name()
                    .ok_or("replication.log_dir: expected a directory name")?;
                let mut name = name.to_os_string();
                name.push(".snapshots");
                log_dir.with_file_name(name)
            }
        };
        let snapshot_dir = std::path::absolute(snapshot_dir)
            .map_err(|e| format!("replication.snapshot_dir: {e}"))?;
        for other in [&log_dir, &database] {
            if snapshot_dir.starts_with(other) || other.starts_with(&snapshot_dir) {
                return Err(
                    "replication.snapshot_dir: must be separate from log_dir and database".into(),
                );
            }
        }
        let mut voters = vec![self.node_id];
        let mut peers = Vec::with_capacity(2);
        for (i, peer) in self.peers.iter().enumerate() {
            let field = format!("replication.peers[{i}]");
            if peer.id == 0 {
                return Err(format!("{field}.id: must be non-zero"));
            }
            if voters.contains(&peer.id) {
                return Err(format!("{field}.id: duplicate voter id {}", peer.id));
            }
            voters.push(peer.id);
            decode_hex::<32>(&peer.fingerprint, &format!("{field}.fingerprint"))?;
            if peers.iter().any(|(_, _, fp)| fp == &peer.fingerprint) {
                return Err(format!("{field}.fingerprint: duplicate fingerprint"));
            }
            peers.push((peer.id, peer.address, peer.fingerprint.clone()));
        }
        voters.sort_unstable();
        Ok(Validated {
            node_id: self.node_id,
            cluster,
            voters,
            listen: self.listen,
            log_dir,
            peers,
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
            write_set_bytes: self.write_set_bytes,
            owner: self.owner.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> ReplicationConfig {
        ReplicationConfig {
            node_id: 3,
            cluster: "0123456789abcdef0123456789abcdef".into(),
            listen: "[::1]:9003".parse().unwrap(),
            log_dir: "replication-log".into(),
            peers: vec![
                PeerConfig {
                    id: 2,
                    address: "127.0.0.1:9002".parse().unwrap(),
                    fingerprint: "01".repeat(32),
                },
                PeerConfig {
                    id: 1,
                    address: "[::1]:9001".parse().unwrap(),
                    fingerprint: "02".repeat(32),
                },
            ],
            snapshot_dir: None,
            certificate: None,
            private_key: None,
            ca: None,
            tick_ms: 100,
            write_set_bytes: None,
            owner: None,
        }
    }

    fn reject(c: &ReplicationConfig, field: &str) {
        let error = c
            .validate(
                Path::new("database"),
                Path::new("server.pem"),
                Path::new("server.key"),
                Path::new("ca.pem"),
            )
            .unwrap_err();
        assert!(error.starts_with(field), "{error}");
    }

    #[test]
    fn accepted_and_boundaries() {
        for tick_ms in [10, 1000] {
            for write_set_bytes in [None, Some(1 << 20), Some(32 << 20)] {
                let c = ReplicationConfig {
                    tick_ms,
                    write_set_bytes,
                    ..config()
                };
                let v = c
                    .validate(
                        Path::new("database"),
                        Path::new("server.pem"),
                        Path::new("server.key"),
                        Path::new("ca.pem"),
                    )
                    .unwrap();
                assert_eq!(v.voters, [1, 2, 3]);
                assert_eq!(v.cluster, hex::decode(&c.cluster).unwrap().as_slice());
                assert_eq!(v.tick, Duration::from_millis(tick_ms));
                assert_eq!(v.write_set_bytes, write_set_bytes);
                assert!(v.log_dir.is_absolute());
                assert_eq!(
                    v.snapshot_dir,
                    v.log_dir.with_file_name("replication-log.snapshots")
                );
                assert_eq!(v.certificate, Path::new("server.pem"));
                assert_eq!(v.private_key, Path::new("server.key"));
                assert_eq!(v.ca, Path::new("ca.pem"));
            }
        }
    }

    #[test]
    fn accepts_owner_and_rejects_empty_owner() {
        let mut c = config();
        c.owner = Some("owner".into());
        assert_eq!(
            c.validate(
                Path::new("database"),
                Path::new("server.pem"),
                Path::new("server.key"),
                Path::new("ca.pem")
            )
            .unwrap()
            .owner
            .as_deref(),
            Some("owner")
        );
        c.owner = Some(String::new());
        reject(&c, "replication.owner:");
    }

    #[test]
    fn rejects_zero_node() {
        reject(
            &ReplicationConfig {
                node_id: 0,
                ..config()
            },
            "replication.node_id:",
        );
    }

    #[test]
    fn rejects_cluster_encoding() {
        for cluster in [
            "ab",
            "0123456789ABCDEF0123456789abcdef",
            "g123456789abcdef0123456789abcdef",
        ] {
            reject(
                &ReplicationConfig {
                    cluster: cluster.into(),
                    ..config()
                },
                "replication.cluster:",
            );
        }
    }

    #[test]
    fn rejects_peer_count() {
        for count in [0, 1, 3] {
            let mut c = config();
            c.peers.resize(count, c.peers[0].clone());
            reject(&c, "replication.peers:");
        }
    }

    #[test]
    fn rejects_voter_ids() {
        for id in [0, 2, 3] {
            let mut c = config();
            c.peers[1].id = id;
            reject(&c, "replication.peers[1].id:");
        }
    }

    #[test]
    fn rejects_tick() {
        for tick_ms in [9, 1001] {
            reject(
                &ReplicationConfig {
                    tick_ms,
                    ..config()
                },
                "replication.tick_ms:",
            );
        }
    }

    #[test]
    fn rejects_write_set_size() {
        for size in [0, (1 << 20) - 1, (32 << 20) + 1] {
            reject(
                &ReplicationConfig {
                    write_set_bytes: Some(size),
                    ..config()
                },
                "replication.write_set_bytes:",
            );
        }
    }

    #[test]
    fn rejects_overlapping_directories() {
        for path in ["database", "database/log", "."] {
            reject(
                &ReplicationConfig {
                    log_dir: path.into(),
                    ..config()
                },
                "replication.log_dir:",
            );
        }
    }

    #[test]
    fn rejects_fingerprints_and_snapshot_overlap() {
        for fingerprint in [
            "".into(),
            "ab".into(),
            "AA".repeat(32),
            "gg".repeat(32),
            "01".repeat(32),
        ] {
            let mut c = config();
            c.peers[1].fingerprint = fingerprint;
            reject(&c, "replication.peers[1].fingerprint:");
        }
        for path in [
            "database",
            "database/snap",
            "replication-log",
            "replication-log/snap",
            ".",
        ] {
            let mut c = config();
            c.snapshot_dir = Some(path.into());
            reject(&c, "replication.snapshot_dir:");
        }
        let mut value = serde_json::to_value(config()).unwrap();
        value["peers"][0]
            .as_object_mut()
            .unwrap()
            .remove("fingerprint");
        assert!(
            serde_json::from_value::<ReplicationConfig>(value)
                .unwrap_err()
                .to_string()
                .contains("fingerprint")
        );
    }

    #[test]
    fn serde_defaults_and_unknown_fields() {
        let mut value = serde_json::to_value(config()).unwrap();
        value.as_object_mut().unwrap().remove("tick_ms");
        let c: ReplicationConfig = serde_json::from_value(value.clone()).unwrap();

        assert_eq!(c.tick_ms, 100);
        value["surprise"] = true.into();
        assert!(serde_json::from_value::<ReplicationConfig>(value).is_err());
        let mut value = serde_json::to_value(config()).unwrap();
        value["peers"][0]["surprise"] = true.into();
        assert!(serde_json::from_value::<ReplicationConfig>(value).is_err());
    }
}
