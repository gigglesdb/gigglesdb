//! Idle groups in RangeHosts over TLS (GD-0075 R5-17b): with node liveness,
//! idle groups quiesce; a leader node's death is noticed through liveness and
//! its groups elect new leaders; and an ignored measurement of the idle CPU cost
//! of many groups per node (`R5_IDLE_GROUPS`, default 500).
use super::common::certs::{config_for, make_pki};
use giggles_core::range_api::RangeId;
use giggles_replication::{
    host::{RangeHost, RangeHostConfig, StateFactory},
    liveness::LivenessConfig,
    sink::{ReplicaHandle, ReplicateError},
    state::{MemStateMachine, StateMachine},
    transport::{GroupRouter, Transport, TransportNetwork},
};
use raft::StateRole;
use std::{
    collections::BTreeMap,
    net::TcpListener,
    path::PathBuf,
    sync::Arc,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const CLUSTER: [u8; 16] = [77; 16];
const VOTERS: [u64; 3] = [1, 2, 3];

fn until(what: &str, limit: Duration, mut ready: impl FnMut() -> bool) -> Duration {
    let start = Instant::now();
    while !ready() {
        assert!(start.elapsed() < limit, "deadline: {what}");
        thread::sleep(Duration::from_millis(10));
    }
    start.elapsed()
}

struct Node {
    host: RangeHost,
    _transport: Arc<Transport>,
}

fn start(
    name: &str,
    groups: u64,
    tick: Duration,
    liveness: LivenessConfig,
) -> (Vec<Node>, PathBuf) {
    let root = std::env::temp_dir().join(format!(
        "{name}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let pki = make_pki(&root.join("pki"), &VOTERS);
    let listeners: Vec<_> = VOTERS
        .iter()
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let addresses: BTreeMap<_, _> = VOTERS
        .iter()
        .zip(&listeners)
        .map(|(id, l)| (*id, l.local_addr().unwrap().to_string()))
        .collect();
    drop(listeners);
    let nodes = VOTERS
        .iter()
        .map(|&node| {
            let transport = Arc::new(
                Transport::start_groups(
                    config_for(&pki, node, CLUSTER, &addresses),
                    Arc::new(GroupRouter::new()),
                )
                .unwrap(),
            );
            let factory: StateFactory =
                Arc::new(|_| Arc::new(MemStateMachine::new()) as Arc<dyn StateMachine>);
            let config = RangeHostConfig {
                node_id: node,
                cluster: CLUSTER,
                log_dir: root.join(format!("n{node}/log")),
                snap_dir: root.join(format!("n{node}/snap")),
                tick,
                policy: None,
                liveness: Some(liveness),
            };
            let listed = (0..groups)
                .map(|g| (RangeId(100 + g), VOTERS.to_vec()))
                .collect();
            let network = Arc::new(TransportNetwork::new(transport.clone()));
            Node {
                host: RangeHost::start(config, network, factory, listed).unwrap(),
                _transport: transport,
            }
        })
        .collect();
    (nodes, root)
}

fn leader(nodes: &[Node], range: RangeId) -> Option<(u64, ReplicaHandle)> {
    nodes.iter().find_map(|n| {
        let g = n.host.group(range)?;
        (g.role().state == StateRole::Leader).then(|| (n.host.node_id(), g))
    })
}

fn write_each(nodes: &[Node], groups: u64) {
    for g in 0..groups {
        let range = RangeId(100 + g);
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            let Some((_, handle)) = leader(nodes, range) else {
                assert!(Instant::now() < deadline, "no leader for {range:?}");
                thread::sleep(Duration::from_millis(10));
                continue;
            };
            match handle.replicate(vec![g as u8; 8]) {
                Ok(()) => break,
                Err(ReplicateError::NotLeader) if Instant::now() < deadline => {}
                Err(e) => panic!("replicate {range:?}: {e}"),
            }
        }
    }
}

fn all_quiesced(nodes: &[Node], groups: u64) -> bool {
    nodes.iter().all(|n| {
        (0..groups).all(|g| {
            n.host
                .group(RangeId(100 + g))
                .is_some_and(|h| h.is_quiesced())
        })
    })
}

#[test]
fn idle_groups_quiesce_and_a_dead_leader_node_is_replaced() {
    let groups = 4;
    let tick = Duration::from_millis(20);
    let liveness = LivenessConfig {
        interval: Duration::from_millis(50),
        timeout: Duration::from_millis(400),
    };
    let (mut nodes, root) = start("idle-hosts", groups, tick, liveness);
    write_each(&nodes, groups);
    until("all groups quiesced", Duration::from_secs(30), || {
        all_quiesced(&nodes, groups)
    });
    // Stop the node that leads the most groups.
    let mut led: BTreeMap<u64, usize> = BTreeMap::new();
    for g in 0..groups {
        *led.entry(leader(&nodes, RangeId(100 + g)).unwrap().0)
            .or_default() += 1;
    }
    let victim = *led.iter().max_by_key(|(_, n)| **n).unwrap().0;
    let at = nodes
        .iter()
        .position(|n| n.host.node_id() == victim)
        .unwrap();
    let dead = nodes.remove(at);
    dead.host.stop();
    drop(dead);
    // Election timeout (10-20 ticks = 200-400 ms) plus the liveness timeout,
    // with slack for a loaded machine.
    let took = until("new leaders", Duration::from_secs(10), || {
        (0..groups).all(|g| leader(&nodes, RangeId(100 + g)).is_some())
    });
    eprintln!("leaders replaced {took:?} after the leader node stopped");
    write_each(&nodes, groups);
    drop(nodes);
    let _ = std::fs::remove_dir_all(root);
}

fn cpu_ms() -> f64 {
    let out = std::process::Command::new("powershell")
        .args([
            "-NoProfile",
            "-Command",
            &format!(
                "(Get-Process -Id {}).TotalProcessorTime.TotalMilliseconds",
                std::process::id()
            ),
        ])
        .output()
        .expect("powershell");
    String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .expect("cpu ms")
}

/// Idle CPU per node with many quiesced groups. Run alone:
/// `cargo test --release --test all -- --ignored idle_hosts:: --nocapture`.
#[test]
#[ignore]
fn measure_idle_cpu_of_many_groups() {
    let groups: u64 = std::env::var("R5_IDLE_GROUPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(500);
    let tick = Duration::from_millis(100);
    let (nodes, root) = start("idle-measure", groups, tick, LivenessConfig::default());
    let setup = Instant::now();
    write_each(&nodes, groups);
    until("all groups quiesced", Duration::from_secs(600), || {
        all_quiesced(&nodes, groups)
    });
    eprintln!(
        "{groups} groups x 3 nodes written and quiesced in {:?}",
        setup.elapsed()
    );
    thread::sleep(Duration::from_secs(2));
    let (cpu0, t0) = (cpu_ms(), Instant::now());
    thread::sleep(Duration::from_secs(30));
    let (cpu1, wall) = (cpu_ms(), t0.elapsed());
    let quiesced = all_quiesced(&nodes, groups);
    let per_node = (cpu1 - cpu0) / wall.as_secs_f64() / 1000.0 / 3.0 * 100.0;
    eprintln!(
        "IDLE RESULT groups={groups} wall={wall:?} cpu_ms={:.0} per_node_core_pct={per_node:.2} still_quiesced={quiesced}",
        cpu1 - cpu0
    );
    drop(nodes);
    let _ = std::fs::remove_dir_all(root);
    assert!(quiesced);
}
