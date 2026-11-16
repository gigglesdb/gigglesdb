use super::config::Validated;
use giggles_replication::{
    cluster::{Node, NodeConfig},
    core_store::ReplicatedEngine,
    role::Serving,
    sink::SnapshotPolicy,
    transport::{PeerConfig, TransportConfig, fingerprint},
};
use raft::StateRole;
use std::{path::Path, sync::Arc, time::Duration};

/// Whether a new serving role requires rebuilding the server's listeners.
pub fn serving_changed<E>(seen: &Serving<E>, now: &Serving<E>) -> bool {
    match (seen, now) {
        (Serving::Follower { .. }, Serving::Follower { .. }) => false,
        (
            Serving::Leader {
                term: a,
                engine: engine_a,
            },
            Serving::Leader {
                term: b,
                engine: engine_b,
            },
        ) => a != b || !Arc::ptr_eq(engine_a, engine_b),
        _ => true,
    }
}

pub struct Replicated {
    node_id: u64,
    node: Node,
}

/// Build the authenticated peer transport shared by the server and operator CLI.
pub fn transport_config(v: &Validated) -> Result<TransportConfig, String> {
    let cert =
        std::fs::File::open(&v.certificate).map_err(|e| format!("replication.certificate: {e}"))?;
    let cert = rustls_pemfile::certs(&mut std::io::BufReader::new(cert))
        .next()
        .transpose()
        .map_err(|e| format!("replication.certificate: {e}"))?
        .ok_or("replication.certificate: no certificates")?;
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
            fingerprint: fingerprint(&cert),
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

impl Replicated {
    pub fn start(v: &Validated, database: &Path) -> Result<Self, String> {
        let node = Node::start(NodeConfig {
            transport: transport_config(v)?,
            data_dir: database.into(),
            log_dir: v.log_dir.clone(),
            snap_dir: v.snapshot_dir.clone(),
            voters: if v.join { vec![] } else { v.voters.clone() },
            tick: v.tick,
            policy: SnapshotPolicy::default(),
            limits: giggles_core::Limits::default(),
            write_set_bytes: v.write_set_bytes.unwrap_or(16 << 20),
        })
        .map_err(|e| format!("replication node: {e}"))?;
        Ok(Self {
            node_id: v.node_id,
            node,
        })
    }

    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    pub fn current(&self) -> Serving<ReplicatedEngine> {
        self.node.roles.current()
    }

    /// Wait until the serving role, leader term or engine changes, or the timeout expires.
    pub fn wait_change(
        &self,
        seen: &Serving<ReplicatedEngine>,
        timeout: Duration,
    ) -> Serving<ReplicatedEngine> {
        self.node
            .roles
            .wait_for(|now| serving_changed(seen, now), timeout)
            .unwrap_or_else(|| self.current())
    }

    pub fn statement_gate(&self, engine: &Arc<ReplicatedEngine>) -> crate::service::StatementGate {
        let handle = self.node.handle.clone();
        let term = engine.term();
        Arc::new(move || {
            let role = handle.role();
            if handle.is_stopped() || role.state != StateRole::Leader {
                return Err("not leader".into());
            }
            if role.term != term {
                return Err("leadership changed".into());
            }
            handle
                .read_barrier()
                .map_err(|e| format!("read barrier: {e}"))?;
            let role = handle.role();
            if handle.is_stopped() || role.state != StateRole::Leader || role.term != term {
                return Err("leadership changed".into());
            }
            Ok(())
        })
    }

    pub fn refusal_hint(&self) -> Arc<dyn Fn() -> String + Send + Sync> {
        let handle = self.node.handle.clone();
        let node_id = self.node_id;
        Arc::new(move || {
            let leader = handle.role().leader;
            let suffix = if leader == 0 {
                "no leader is known yet".into()
            } else {
                format!("the leader is node {leader}")
            };
            format!("this node (id {node_id}) is a replication follower; {suffix}")
        })
    }

    pub fn stop(self) {
        self.node.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serving_change_tracks_engine_identity_and_term() {
        let engine = Arc::new(0u8);
        let seen = Serving::Leader {
            term: 7,
            engine: engine.clone(),
        };
        assert!(!serving_changed(&seen, &seen.clone()));
        assert!(serving_changed(
            &seen,
            &Serving::Leader {
                term: 7,
                engine: Arc::new(0u8),
            }
        ));
        assert!(serving_changed(&seen, &Serving::Leader { term: 8, engine }));
    }

    #[test]
    fn serving_change_tracks_role_transitions() {
        let follower = Serving::Follower { leader: 1, term: 7 };
        let leader = Serving::Leader {
            term: 7,
            engine: Arc::new(0u8),
        };
        assert!(serving_changed(&follower, &leader));
        assert!(serving_changed(&leader, &follower));
        assert!(!serving_changed(
            &follower,
            &Serving::<u8>::Follower { leader: 2, term: 8 }
        ));
    }
}
