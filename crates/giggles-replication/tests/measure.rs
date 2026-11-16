//! In-process R4-44 measurements; all latencies are integer microseconds.
use super::common::certs::{config_for, make_pki};
use giggles_core::{Admin, Engine, Limits};
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
        Arc, Barrier,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

fn until(mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !ready() {
        assert!(Instant::now() < deadline, "measurement wait timed out");
        thread::sleep(Duration::from_millis(1));
    }
}
fn micros(duration: Duration) -> u64 {
    duration.as_micros().try_into().unwrap()
}
fn percentile(values: &[u64], percent: usize) -> u64 {
    values[(values.len() * percent).div_ceil(100).saturating_sub(1)]
}
fn stats(mut values: Vec<u64>) -> String {
    values.sort_unstable();
    format!(
        r#"{{"p50":{},"p95":{},"p99":{},"max":{}}}"#,
        percentile(&values, 50),
        percentile(&values, 95),
        percentile(&values, 99),
        values.last().unwrap()
    )
}
fn sample(count: usize, mut action: impl FnMut()) -> String {
    stats(
        (0..count)
            .map(|_| {
                let start = Instant::now();
                action();
                micros(start.elapsed())
            })
            .collect(),
    )
}
fn insert(id: u64) -> String {
    format!("INSERT INTO m VALUES ({id}, {id})")
}
fn workload(admin: &Admin, commits: usize, clients: usize, ids: &AtomicU64) -> String {
    let mut session = admin.sql_session();
    session
        .execute(
            "CREATE TABLE m(id bigint PRIMARY KEY, v bigint NOT NULL)",
            &[],
            None,
        )
        .unwrap();
    let latency = sample(commits, || {
        session
            .execute(&insert(ids.fetch_add(1, Ordering::Relaxed)), &[], None)
            .unwrap();
    });
    let barrier = Barrier::new(clients + 1);
    let throughput = thread::scope(|scope| {
        for client in 0..clients {
            let barrier = &barrier;
            scope.spawn(move || {
                let mut session = admin.sql_session();
                barrier.wait();
                for _ in (client..commits).step_by(clients) {
                    session
                        .execute(&insert(ids.fetch_add(1, Ordering::Relaxed)), &[], None)
                        .unwrap();
                }
            });
        }
        let start = Instant::now();
        barrier.wait();
        start
    })
    .elapsed();
    format!(
        r#"{{"commit_us":{latency},"commits_per_second":{},"commits":{commits},"clients":{clients}}}"#,
        (commits as f64 / throughput.as_secs_f64()) as u64
    )
}
struct Cluster {
    nodes: Vec<Option<Node>>,
    configs: Vec<NodeConfig>,
    root: PathBuf,
}
impl Cluster {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "r4-measure-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
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
                transport: config_for(&pki, id, [0x44; 16], &addresses),
                data_dir: root.join(format!("data-{id}")),
                log_dir: root.join(format!("log-{id}")),
                snap_dir: root.join(format!("snap-{id}")),
                voters: vec![1, 2, 3],
                tick: Duration::from_millis(20),
                policy: SnapshotPolicy::default(),
                limits: Limits::default(),
                write_set_bytes: 64 << 20,
            })
            .collect();
        drop(listeners);
        let mut cluster = Self {
            nodes: vec![],
            configs,
            root,
        };
        for config in &cluster.configs {
            cluster
                .nodes
                .push(Some(Node::start(config.clone()).unwrap()));
        }
        cluster
    }
    fn leader(&self) -> (usize, Arc<ReplicatedEngine>) {
        let mut found = None;
        until(|| {
            found = self.nodes.iter().enumerate().find_map(|(i, n)| {
                let n = n.as_ref()?;
                if let Serving::Leader { engine, .. } = n.roles.current()
                    && n.handle.read_barrier().is_ok()
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
    fn converged(&self) {
        until(|| {
            let positions: Vec<_> = self
                .nodes
                .iter()
                .flatten()
                .map(|n| n.slot.current().unwrap().applied().unwrap())
                .collect();
            positions.len() == 3
                && positions[0].is_some()
                && positions.iter().all(|p| *p == positions[0])
        });
    }
    fn failover(&mut self, ids: &AtomicU64) -> [u64; 4] {
        let (old, engine) = self.leader();
        drop(engine);
        let stopped = self.nodes[old].take().unwrap();
        let start = Instant::now();
        // Observe the surviving nodes while stop joins the old leader's threads.
        let times = thread::scope(|scope| {
            scope.spawn(move || stopped.stop());
            let mut elected = None;
            until(|| {
                elected = self
                    .nodes
                    .iter()
                    .flatten()
                    .find(|n| n.handle.role().state == raft::StateRole::Leader);
                elected.is_some()
            });
            let elect = Instant::now();
            let node = elected.unwrap();
            let mut opened = None;
            until(|| {
                if let Serving::Leader { engine, .. } = node.roles.current() {
                    opened = Some(engine);
                }
                opened.is_some()
            });
            let engine_time = Instant::now();
            let engine = opened.unwrap();
            until(|| {
                engine
                    .admin()
                    .sql_session()
                    .execute(&insert(ids.fetch_add(1, Ordering::Relaxed)), &[], None)
                    .is_ok()
            });
            let first = Instant::now();
            [
                micros(elect - start),
                micros(engine_time - elect),
                micros(first - engine_time),
                micros(first - start),
            ]
        });
        self.nodes[old] = Some(Node::start(self.configs[old].clone()).unwrap());
        self.converged();
        times
    }
}
impl Drop for Cluster {
    fn drop(&mut self) {
        for node in self.nodes.iter_mut().filter_map(Option::take) {
            node.stop();
        }
        let result = fs::remove_dir_all(&self.root);
        if !thread::panicking() {
            result.unwrap();
        }
    }
}
fn failover_json(values: [u64; 4]) -> String {
    format!(
        r#"{{"elect_us":{},"engine_after_elect_us":{},"first_after_engine_us":{},"total_us":{}}}"#,
        values[0], values[1], values[2], values[3]
    )
}
fn measure(commits: usize, clients: usize, failovers: usize, reads: usize) -> String {
    let mut cluster = Cluster::new();
    let ids = AtomicU64::new(1);
    let single =
        Admin::create_engine(cluster.root.join("single"), Limits::default(), Engine::V2).unwrap();
    let single_node = workload(&single, commits, clients, &ids);
    let (leader, engine) = cluster.leader();
    let three_node = workload(engine.admin(), commits, clients, &ids);
    let barrier = sample(reads, || {
        cluster.nodes[leader]
            .as_ref()
            .unwrap()
            .handle
            .read_barrier()
            .unwrap();
    });
    let mut single_session = single.sql_session();
    let single_select = sample(reads, || {
        single_session
            .execute("SELECT count(*) FROM m", &[], None)
            .unwrap();
    });
    let mut leader_session = engine.admin().sql_session();
    let leader_select = sample(reads, || {
        leader_session
            .execute("SELECT count(*) FROM m", &[], None)
            .unwrap();
    });
    drop(leader_session);
    drop(engine);
    cluster.converged();
    let runs: Vec<_> = (0..failovers).map(|_| cluster.failover(&ids)).collect();
    let medians = std::array::from_fn(|i| {
        let mut values: Vec<_> = runs.iter().map(|run| run[i]).collect();
        values.sort_unstable();
        percentile(&values, 50)
    });
    format!(
        r#"{{"single_node":{single_node},"three_node":{three_node},"read_barrier":{{"calls":{reads},"leader_us":{barrier},"single_select_us":{single_select},"leader_select_us":{leader_select}}},"failover":{{"runs":[{}],"median":{}}},"hardware":{{"logical_cpus":{},"os":"{}","notes":"in-process, loopback mTLS, tick 20ms, RocksDB NoSync apply + redb Immediate log"}}}}"#,
        runs.iter()
            .copied()
            .map(failover_json)
            .collect::<Vec<_>>()
            .join(","),
        failover_json(medians),
        thread::available_parallelism().unwrap().get(),
        std::env::consts::OS
    )
}
fn setting(name: &str, default: usize) -> usize {
    let value = std::env::var(name)
        .map(|v| v.parse().expect("positive integer setting"))
        .unwrap_or(default);
    assert!(value > 0, "{name} must be positive");
    value
}
#[test]
#[ignore = "R4-44 measurement; run with --ignored"]
fn measure_r4() {
    let json = measure(
        setting("R4_MEASURE_COMMITS", 2000),
        setting("R4_MEASURE_CLIENTS", 8),
        setting("R4_MEASURE_FAILOVERS", 5),
        1000,
    );
    let out = std::env::var_os("R4_MEASURE_OUT")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("r4-measure.json"));
    fs::write(out, &json).unwrap();
    println!("{json}");
}
#[test]
fn measure_smoke() {
    let json = measure(20, 2, 1, 5);
    for field in [
        "single_node",
        "three_node",
        "commit_us",
        "p50",
        "p95",
        "p99",
        "max",
        "commits_per_second",
        "commits",
        "clients",
        "read_barrier",
        "calls",
        "leader_us",
        "single_select_us",
        "leader_select_us",
        "failover",
        "runs",
        "median",
        "elect_us",
        "engine_after_elect_us",
        "first_after_engine_us",
        "total_us",
        "hardware",
        "logical_cpus",
        "os",
        "notes",
    ] {
        assert!(
            json.contains(&format!("\"{field}\":")),
            "missing {field}: {json}"
        );
    }
}
