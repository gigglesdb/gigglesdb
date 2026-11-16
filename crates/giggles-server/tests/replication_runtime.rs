#![cfg(feature = "engine-rocksdb")]
use giggles_core::relational::Value;
use giggles_replication::{core_store::ReplicatedEngine, role::Serving};
use giggles_server::replication::{
    config::{PeerConfig, ReplicationConfig},
    runtime::Replicated,
};
use std::{
    net::TcpListener,
    path::PathBuf,
    sync::{Arc, mpsc},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

struct Cluster {
    nodes: Vec<Option<Replicated>>,
    dir: PathBuf,
}
impl Drop for Cluster {
    fn drop(&mut self) {
        for node in &mut self.nodes {
            if let Some(node) = node.take() {
                node.stop();
            }
        }
        let cleanup = std::fs::remove_dir_all(&self.dir);
        if !thread::panicking() {
            cleanup.unwrap();
        }
    }
}
impl Cluster {
    fn leader(&self) -> (usize, Arc<ReplicatedEngine>) {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let leaders: Vec<_> = self
                .nodes
                .iter()
                .enumerate()
                .filter_map(|(i, node)| match node.as_ref()?.current() {
                    Serving::Leader { engine, .. } => Some((i, engine)),
                    _ => None,
                })
                .collect();
            if leaders.len() == 1 {
                return leaders.into_iter().next().unwrap();
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(!remaining.is_zero(), "leader election timed out");
            for node in self.nodes.iter().flatten() {
                node.wait_change(&node.current(), remaining.min(Duration::from_millis(20)));
            }
        }
    }
}
fn sql(engine: &Arc<ReplicatedEngine>, statement: &str) -> Result<Vec<Vec<Value>>, String> {
    let (tx, rx) = mpsc::channel();
    let engine = engine.clone();
    let statement = statement.to_owned();
    let join = thread::spawn(move || {
        let result = engine
            .admin()
            .sql_session()
            .execute(&statement, &[], None)
            .map(|r| r.rows.into_iter().map(|r| r.values).collect())
            .map_err(|e| e.to_string());
        let _ = tx.send(result);
    });
    let result = rx
        .recv_timeout(Duration::from_secs(20))
        .expect("SQL timed out");
    join.join().unwrap();
    result
}

#[test]
fn sql_survives_leader_stop() {
    let mut cluster = Cluster {
        nodes: Vec::new(),
        dir: std::env::temp_dir().join(format!(
            "replication-runtime-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )),
    };
    let reserved: Vec<_> = (0..3)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let addresses: Vec<_> = reserved.iter().map(|l| l.local_addr().unwrap()).collect();
    use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
    std::fs::create_dir_all(&cluster.dir).unwrap();
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_key = KeyPair::generate().unwrap();
    let ca_cert = params.self_signed(&ca_key).unwrap();
    let ca = cluster.dir.join("ca.pem");
    std::fs::write(&ca, ca_cert.pem()).unwrap();
    let certificates: Vec<_> = (0..3)
        .map(|i| {
            let key = KeyPair::generate().unwrap();
            let cert = CertificateParams::new(vec!["localhost".into()])
                .unwrap()
                .signed_by(&key, &ca_cert, &ca_key)
                .unwrap();
            let certificate = cluster.dir.join(format!("{i}.pem"));
            let private_key = cluster.dir.join(format!("{i}.key"));
            std::fs::write(&certificate, cert.pem()).unwrap();
            std::fs::write(&private_key, key.serialize_pem()).unwrap();
            (
                certificate,
                private_key,
                giggles_replication::transport::fingerprint(cert.der()),
            )
        })
        .collect();
    let configs: Vec<_> = (0..3)
        .map(|i| ReplicationConfig {
            node_id: i as u64 + 1,
            join: false,
            cluster: "12".repeat(16),
            listen: addresses[i],
            log_dir: cluster.dir.join(format!("log-{i}")),
            peers: (0..3)
                .filter(|j| *j != i)
                .map(|j| PeerConfig {
                    id: j as u64 + 1,
                    address: addresses[j],
                    fingerprint: certificates[j].2.clone(),
                })
                .collect(),
            snapshot_dir: None,
            certificate: Some(certificates[i].0.clone()),
            private_key: Some(certificates[i].1.clone()),
            ca: Some(ca.clone()),
            tick_ms: 10,
            write_set_bytes: None,
            owner: None,
        })
        .collect();
    for (i, listener) in reserved.into_iter().enumerate() {
        let database = cluster.dir.join(format!("data-{i}"));
        let v = configs[i]
            .validate(&database, &certificates[i].0, &certificates[i].1, &ca)
            .unwrap();
        drop(listener);
        cluster
            .nodes
            .push(Some(Replicated::start(&v, &database).unwrap()));
    }
    let (leader, engine) = cluster.leader();
    let node = cluster.nodes[leader].as_ref().unwrap();
    let gate = node.statement_gate(&engine);
    gate().unwrap();
    sql(&engine, "CREATE TABLE t(id int PRIMARY KEY)").unwrap();
    sql(&engine, "INSERT INTO t VALUES (42)").unwrap();
    let rows = sql(&engine, "SELECT id FROM t").unwrap();
    assert_eq!(rows, vec![vec![Value::Int(42)]]);
    let follower = cluster
        .nodes
        .iter()
        .flatten()
        .find(|n| n.node_id() != node.node_id())
        .unwrap();
    let hint = follower.refusal_hint();
    let expected_hint = format!(
        "this node (id {}) is a replication follower; the leader is node {}",
        follower.node_id(),
        node.node_id()
    );
    let deadline = Instant::now() + Duration::from_secs(15);
    while hint() != expected_hint {
        assert!(
            Instant::now() < deadline,
            "follower did not learn leader: {}",
            hint()
        );
        follower.wait_change(&follower.current(), Duration::from_millis(20));
    }
    assert_eq!(hint(), expected_hint);
    cluster.nodes[leader].take().unwrap().stop();
    assert!(gate().is_err());
    drop(engine);
    let (_, replacement) = cluster.leader();
    assert_eq!(sql(&replacement, "SELECT id FROM t").unwrap(), rows);
    drop(replacement);
}
