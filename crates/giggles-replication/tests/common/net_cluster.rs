use super::certs::{config_for, make_pki};
use giggles_core::Limits;
use giggles_replication::{
    cluster::{Node, NodeConfig},
    core_store::ReplicatedEngine,
    role::Serving,
    sink::SnapshotPolicy,
};
use std::{
    collections::BTreeMap,
    fs,
    net::TcpListener,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

static NEXT: AtomicU64 = AtomicU64::new(0);

pub struct NetCluster {
    pub nodes: Vec<Option<Node>>,
    pub configs: Vec<NodeConfig>,
    pub root: PathBuf,
}
impl NetCluster {
    pub fn new(name: &str, cluster: [u8; 16], policy: SnapshotPolicy) -> Self {
        let root = std::env::temp_dir().join(format!(
            "{name}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let pki = make_pki(&root.join("pki"), &[1, 2, 3]);
        let listeners: Vec<_> = (0..3)
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let addresses: BTreeMap<_, _> = listeners
            .iter()
            .enumerate()
            .map(|(i, l)| (i as u64 + 1, l.local_addr().unwrap().to_string()))
            .collect();
        let configs: Vec<_> = (1..=3)
            .map(|id| NodeConfig {
                transport: config_for(&pki, id, cluster, &addresses),
                data_dir: root.join(format!("data-{id}")),
                log_dir: root.join(format!("log-{id}")),
                snap_dir: root.join(format!("snap-{id}")),
                voters: vec![1, 2, 3],
                tick: Duration::from_millis(20),
                policy,
                limits: Limits::default(),
                write_set_bytes: 64 << 20,
            })
            .collect();
        drop(listeners);
        let mut c = Self {
            nodes: vec![],
            configs,
            root,
        };
        for config in &c.configs {
            c.nodes.push(Some(Node::start(config.clone()).unwrap()));
        }
        c
    }

    pub fn leader(&self) -> (usize, Arc<ReplicatedEngine>) {
        let mut found = None;
        until(|| {
            found = self.nodes.iter().enumerate().find_map(|(i, node)| {
                let node = node.as_ref()?;
                if let Serving::Leader { engine, .. } = node.roles.current()
                    && node.handle.read_barrier().is_ok()
                {
                    Some((i, engine))
                } else {
                    None
                }
            });
            found.is_some()
        });
        found.unwrap()
    }

    // The statements are idempotent so a lost acknowledgement at an election
    // cannot duplicate a row. Rediscover the published engine on every retry.
    pub fn execute(&self, sql: &str) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let (_, engine) = self.leader();
            match engine.admin().sql_session().execute(sql, &[], None) {
                Ok(_) => return,
                Err(error) => {
                    assert!(Instant::now() < deadline, "SQL deadline: {sql}: {error}");
                }
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    pub fn converged(&self) {
        until(|| {
            let positions: Vec<_> = self
                .nodes
                .iter()
                .flatten()
                .map(|n| n.slot.current().unwrap().applied().unwrap())
                .collect();
            !positions.is_empty()
                && positions[0].is_some()
                && positions.iter().all(|p| *p == positions[0])
        });
    }

    pub fn stop(&mut self, i: usize) {
        self.nodes[i].take().unwrap().stop();
    }

    pub fn start(&mut self, i: usize) {
        self.nodes[i] = Some(Node::start(self.configs[i].clone()).unwrap());
    }

    pub fn kill(&mut self, i: usize) {
        if let Some(node) = self.nodes[i].take() {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| node.stop()));
        }
    }
    #[allow(dead_code)] // Shared with subsequent network work orders.
    pub fn wipe(&mut self, i: usize) {
        self.stop(i);
        let config = &self.configs[i];
        for dir in [&config.data_dir, &config.log_dir, &config.snap_dir] {
            fs::remove_dir_all(dir).unwrap();
        }
        self.start(i);
    }
}
impl Drop for NetCluster {
    fn drop(&mut self) {
        for i in 0..self.nodes.len() {
            self.kill(i);
        }
        let result = fs::remove_dir_all(&self.root);
        if !thread::panicking() {
            result.unwrap();
        }
    }
}
pub fn until(mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !ready() {
        assert!(
            Instant::now() < deadline,
            "cluster network deadline exceeded"
        );
        thread::sleep(Duration::from_millis(10));
    }
}
