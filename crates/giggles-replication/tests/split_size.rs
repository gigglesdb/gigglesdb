//! Split by size (GD-0075 R5-83): three ClusterNodes with real engines and transport and one
//! SQL gateway. `Splitter::split_by_size` splits the system range once at first_user_key(),
//! then every user range whose approximate size reached the threshold, once per crossing, at a
//! user key near its midpoint; a cooldown holds a range that just split; `SizeSplitter` runs
//! the policy in the background.
use super::common::certs::{config_for, make_pki};
use giggles_core::{
    Limits, SqlSession,
    cluster_node::{ClusterGateway, EngineSettings, GatewaySettings},
    range_api::{RangeClient, RangeDescriptor, RangeId, SYSTEM_RANGE, first_user_key},
    relational::{StatementClock, Value},
};
use giggles_replication::{
    bootstrap::presplit,
    cluster_node::{ClusterNode, ClusterNodeConfig},
    split::{SizePolicy, SizeSplitter, Splitter},
};
use raft::StateRole;
use std::{
    collections::{BTreeMap, BTreeSet},
    net::TcpListener,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const CLUSTER: [u8; 16] = [83; 16];
const NODES: [u64; 3] = [1, 2, 3];
const DEADLINE: Duration = Duration::from_secs(60);
const PAYLOAD: usize = 200;

/// Tests of this file run in parallel in one process: one root each.
static ROOTS: AtomicU64 = AtomicU64::new(0);

struct Root(PathBuf);
impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Three nodes over TLS. `whole`: range 1 = the system range over every key; otherwise the
/// presplit layout (range 1 = system, range 2 = every user key).
fn start(whole: bool) -> (Root, Vec<ClusterNode>) {
    let root = Root(std::env::temp_dir().join(format!(
        "split-size-{}-{}-{}",
        std::process::id(),
        ROOTS.fetch_add(1, Ordering::Relaxed),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )));
    let mut layout = presplit(&[], &NODES, 3).unwrap();
    if whole {
        layout.truncate(1);
        layout[0].end = None;
    }
    let pki = make_pki(&root.0.join("pki"), &NODES);
    let listeners: Vec<_> = NODES
        .iter()
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let addresses: BTreeMap<_, _> = NODES
        .iter()
        .zip(&listeners)
        .map(|(id, l)| (*id, l.local_addr().unwrap().to_string()))
        .collect();
    drop(listeners);
    let nodes: Vec<ClusterNode> = NODES
        .iter()
        .map(|&node| {
            ClusterNode::start(ClusterNodeConfig {
                transport: config_for(&pki, node, CLUSTER, &addresses),
                data_dir: root.0.join(format!("n{node}/data")),
                log_dir: root.0.join(format!("n{node}/log")),
                snap_dir: root.0.join(format!("n{node}/snap")),
                tick: Duration::from_millis(20),
                layout: layout.clone(),
                engine: EngineSettings {
                    lease: Duration::from_secs(3),
                    ..EngineSettings::default()
                },
                liveness: None,
                policy: None,
                call_limits: None,
            })
            .unwrap()
        })
        .collect();
    assert!(nodes[0].bootstrap_directory(DEADLINE).unwrap());
    for node in &nodes {
        node.wait_ready(DEADLINE).unwrap();
    }
    (root, nodes)
}

fn splitter(node: &ClusterNode) -> Arc<Splitter> {
    let client = node.client().clone() as Arc<dyn RangeClient>;
    Arc::new(Splitter::new(node.host().clone(), client))
}

fn policy(threshold: u64, cooldown: Duration) -> SizePolicy {
    SizePolicy {
        threshold,
        cooldown,
        interval: Duration::from_millis(100),
    }
}

/// One `split_by_size` pass on every node; the splits made, in node order.
fn pass(
    splitters: &[Arc<Splitter>],
    policy: &SizePolicy,
) -> Vec<(RangeDescriptor, RangeDescriptor)> {
    splitters
        .iter()
        .flat_map(|s| s.split_by_size(policy).unwrap())
        .collect()
}

fn flush(nodes: &[ClusterNode]) {
    for node in nodes {
        node.host().store().unwrap().flush_memtables().unwrap();
    }
}

/// Every user range with its approximate size on its Raft leader, once every node stores the
/// same user ranges and each has a leader.
fn user_ranges(nodes: &[ClusterNode]) -> BTreeMap<RangeId, u64> {
    let deadline = Instant::now() + DEADLINE;
    loop {
        let stored: Vec<Vec<RangeDescriptor>> = nodes
            .iter()
            .map(|node| {
                let store = node.host().store().unwrap();
                store
                    .ranges()
                    .unwrap()
                    .into_iter()
                    .filter(|&id| id != SYSTEM_RANGE)
                    .map(|id| store.descriptor(id).unwrap().unwrap())
                    .collect()
            })
            .collect();
        let leaders: Option<BTreeMap<RangeId, u64>> = stored[0]
            .iter()
            .map(|desc| {
                let node = nodes.iter().find(|n| {
                    n.host()
                        .group(desc.id)
                        .is_some_and(|h| h.role().state == StateRole::Leader)
                })?;
                let size = node.host().store().unwrap().approximate_size(desc.id);
                Some((desc.id, size.unwrap()))
            })
            .collect();
        if let Some(sizes) = leaders.filter(|_| stored.iter().all(|s| *s == stored[0])) {
            return sizes;
        }
        assert!(Instant::now() < deadline, "user ranges did not settle");
        thread::sleep(Duration::from_millis(20));
    }
}

fn gateway(node: &ClusterNode) -> ClusterGateway {
    let settings = GatewaySettings {
        node: node.node_id() as u16,
        heartbeat: Duration::from_millis(500),
        gateway_lease: Duration::from_secs(1),
        ..GatewaySettings::default()
    };
    node.attach_gateway(settings, Limits::default()).unwrap()
}

fn sql(s: &mut SqlSession, statement: &str) -> Result<Vec<Vec<Value>>, String> {
    let clock = StatementClock {
        unavailable: None,
        transaction_us: 100,
        statement_us: 100,
        zone: None,
        wall: None,
    };
    let result = s.execute_at(statement, &[], None, &clock);
    Ok(result
        .map_err(|e| e.to_string())?
        .rows
        .into_iter()
        .map(|r| r.values)
        .collect())
}

/// Inserts ids `ids` with ~200-byte random payloads, 100 per statement; a failed statement is
/// settled by counting its ids (all there: committed; none: retried).
fn insert(s: &mut SqlSession, ids: std::ops::Range<i64>, seed: &mut u64) {
    let ids: Vec<i64> = ids.collect();
    for chunk in ids.chunks(100) {
        let rows: Vec<String> = chunk
            .iter()
            .map(|id| {
                let p: String = (0..PAYLOAD)
                    .map(|_| {
                        *seed ^= *seed << 13;
                        *seed ^= *seed >> 7;
                        *seed ^= *seed << 17;
                        char::from(b"abcdefghijklmnopqrstuvwxyz0123456789"[(*seed % 36) as usize])
                    })
                    .collect();
                format!("({id}, '{p}')")
            })
            .collect();
        let statement = format!("INSERT INTO t VALUES {}", rows.join(", "));
        let (first, last) = (chunk[0], chunk[chunk.len() - 1]);
        let count = format!("SELECT count(*) FROM t WHERE id >= {first} AND id <= {last}");
        let deadline = Instant::now() + DEADLINE;
        while let Err(e) = sql(s, &statement) {
            eprintln!("insert {first}..={last}: {e}");
            match &retry_sql(s, &count)[0][0] {
                Value::Int(0) => assert!(Instant::now() < deadline, "insert timed out"),
                Value::Int(n) if *n == chunk.len() as i64 => break,
                other => panic!("insert {first}..={last}: {other:?} rows committed"),
            }
        }
    }
}

/// Runs a read statement until it succeeds (a range may be electing or reopening its engine).
fn retry_sql(s: &mut SqlSession, statement: &str) -> Vec<Vec<Value>> {
    let deadline = Instant::now() + DEADLINE;
    loop {
        match sql(s, statement) {
            Ok(rows) => return rows,
            Err(e) => {
                assert!(Instant::now() < deadline, "{statement}: {e}");
                eprintln!("{statement}: {e}");
                thread::sleep(Duration::from_millis(20));
            }
        }
    }
}

/// The table holds exactly ids 1..=n.
fn check_rows(s: &mut SqlSession, n: i64) {
    let rows = retry_sql(s, "SELECT count(*), sum(id), min(id), max(id) FROM t");
    let want = [n, n * (n + 1) / 2, 1, n].map(Value::Int).to_vec();
    assert_eq!(rows, vec![want]);
}

#[test]
fn system_range_over_every_key_splits_once_at_the_first_user_key() {
    let (_root, nodes) = start(true);
    let splitters: Vec<_> = nodes.iter().map(splitter).collect();
    let policy = policy(64 << 10, Duration::ZERO);
    let splits = pass(&splitters, &policy);
    assert_eq!(splits.len(), 1, "{splits:?}");
    let (left, right) = &splits[0];
    assert_eq!(
        (left.id, left.end.clone()),
        (SYSTEM_RANGE, Some(first_user_key()))
    );
    assert_eq!(
        (right.start.clone(), right.end.clone()),
        (first_user_key(), None)
    );
    // Nothing else to split: the system range is done and the user range is empty.
    assert_eq!(pass(&splitters, &policy), vec![]);
    assert_eq!(user_ranges(&nodes).len(), 1);

    // The background loop on every node splits the user range once it holds enough data.
    let mut loops: Vec<_> = splitters
        .iter()
        .map(|s| SizeSplitter::start(s.clone(), policy))
        .collect();
    let g = gateway(&nodes[0]);
    let mut s = g.admin.sql_session();
    sql(&mut s, "CREATE TABLE t(id INT PRIMARY KEY, p TEXT)").unwrap();
    insert(&mut s, 1..601, &mut 7);
    let deadline = Instant::now() + DEADLINE;
    while user_ranges(&nodes).len() < 2 {
        assert!(
            Instant::now() < deadline,
            "the background loop did not split"
        );
        flush(&nodes);
        thread::sleep(Duration::from_millis(100));
    }
    loops.iter_mut().for_each(SizeSplitter::stop);
    check_rows(&mut s, 600);
    let system = nodes[0].host().store().unwrap().descriptor(SYSTEM_RANGE);
    assert_eq!(system.unwrap().unwrap().end, Some(first_user_key()));
    drop((s, g));
    nodes.into_iter().for_each(|n| n.stop());
}

#[test]
fn a_growing_table_splits_once_per_threshold_crossing() {
    const THRESHOLD: u64 = 256 << 10;
    const ROUNDS: i64 = 10;
    const ROUND_ROWS: i64 = 500;
    let (_root, nodes) = start(false);
    let splitters: Vec<_> = nodes.iter().map(splitter).collect();
    let policy = policy(THRESHOLD, Duration::ZERO);
    let g = gateway(&nodes[0]);
    let mut s = g.admin.sql_session();
    sql(&mut s, "CREATE TABLE t(id INT PRIMARY KEY, p TEXT)").unwrap();
    let (mut crossings, mut splits, mut seed) = (0, 0, 1);
    for round in 0..ROUNDS {
        insert(
            &mut s,
            round * ROUND_ROWS + 1..(round + 1) * ROUND_ROWS + 1,
            &mut seed,
        );
        flush(&nodes);
        let before = user_ranges(&nodes);
        let over: BTreeSet<RangeId> = before
            .iter()
            .filter(|(_, size)| **size >= THRESHOLD)
            .map(|(id, _)| *id)
            .collect();
        let made = pass(&splitters, &policy);
        // Each range that crossed the threshold split exactly once, and no other range.
        let parents: Vec<RangeId> = made.iter().map(|(left, _)| left.id).collect();
        assert_eq!(parents.iter().copied().collect::<BTreeSet<_>>(), over);
        assert_eq!(parents.len(), over.len(), "round {round}: {made:?}");
        assert!(!parents.contains(&SYSTEM_RANGE));
        let after = user_ranges(&nodes);
        eprintln!("round {round}: {before:?} -> {after:?}");
        assert!(
            after.values().all(|&size| size < THRESHOLD),
            "round {round}: {after:?}"
        );
        assert_eq!(after.len(), before.len() + made.len());
        (crossings, splits) = (crossings + over.len(), splits + made.len());
        check_rows(&mut s, (round + 1) * ROUND_ROWS);
    }
    assert_eq!(splits, crossings);
    assert!(splits >= 3, "{splits} splits");
    let system = nodes[0].host().store().unwrap().descriptor(SYSTEM_RANGE);
    assert_eq!(system.unwrap().unwrap().end, Some(first_user_key()));
    drop((s, g));
    nodes.into_iter().for_each(|n| n.stop());
}

#[test]
fn a_range_that_just_split_waits_out_its_cooldown() {
    const THRESHOLD: u64 = 96 << 10;
    let (_root, nodes) = start(false);
    let splitters: Vec<_> = nodes.iter().map(splitter).collect();
    let g = gateway(&nodes[0]);
    let mut s = g.admin.sql_session();
    sql(&mut s, "CREATE TABLE t(id INT PRIMARY KEY, p TEXT)").unwrap();
    insert(&mut s, 1..1501, &mut 3);
    flush(&nodes);
    let cooling = policy(THRESHOLD, Duration::from_secs(3600));
    let first = pass(&splitters, &cooling);
    assert_eq!(first.len(), 1, "{first:?}");
    // Both halves are still above the threshold, wherever their leaders are.
    let sizes = user_ranges(&nodes);
    assert_eq!(sizes.len(), 2);
    assert!(sizes.values().all(|&size| size >= THRESHOLD), "{sizes:?}");
    assert_eq!(pass(&splitters, &cooling), vec![]);
    // Without the cooldown, both split.
    assert_eq!(
        pass(&splitters, &policy(THRESHOLD, Duration::ZERO)).len(),
        2
    );
    check_rows(&mut s, 1500);
    drop((s, g));
    nodes.into_iter().for_each(|n| n.stop());
}
