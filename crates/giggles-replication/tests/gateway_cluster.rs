//! Every node is a gateway, end to end in process (GD-0075 Phase 7, P7-0d): three nodes over
//! TLS, each with real range engines (giggles-core) under real Raft groups, routed range calls
//! and peer transport; two SQL gateways on different nodes write, read and call nextval, and a
//! transaction spanning ranges led by different nodes commits or rolls back as one. Every
//! node answers node-status calls with its ranges and served-call counters (R5-64). A first
//! start cut off while it created its ranges is completed on restart (P7-3, review S5).
use super::common::certs::{config_for, make_pki};
use giggles_core::{
    CoreError, Limits, SqlSession,
    cluster_node::{ClusterGateway, EngineSettings, GatewaySettings},
    range_api::{RangeSpace, SYSTEM_RANGE, range_key},
    relational::{StatementClock, Value},
    replicated::RangeStore,
};
use giggles_replication::{
    bootstrap::presplit,
    call_codec::{NodeCall, NodeReport, PeerCall, PeerReply, RangeRole},
    client::LocalRanges,
    cluster_node::{ClusterNode, ClusterNodeConfig, query_status},
    transport::TransportConfig,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::TcpListener,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const CLUSTER: [u8; 16] = [75; 16];
const NODES: [u64; 3] = [1, 2, 3];

/// Rows and index entries of tables 1..=8 each in their own range (as giggles-core's gateway
/// tests split), so one table's writes and another's sit in different groups.
fn splits() -> Vec<Vec<u8>> {
    let mut splits = Vec::new();
    for space in [RangeSpace::Rows, RangeSpace::Index] {
        for id in 1..=8_u64 {
            splits.push(range_key(space, &id.to_be_bytes()));
        }
    }
    splits.sort();
    splits
}

fn clock() -> StatementClock {
    StatementClock {
        unavailable: None,
        transaction_us: 100,
        statement_us: 100,
        zone: None,
        wall: None,
    }
}

fn sql(s: &mut SqlSession, statement: &str) -> Result<Vec<Vec<Value>>, CoreError> {
    Ok(s.execute_at(statement, &[], None, &clock())?
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect())
}

fn ints(rows: Vec<Vec<Value>>) -> Vec<Vec<i64>> {
    rows.into_iter()
        .map(|row| {
            row.into_iter()
                .map(|v| match v {
                    Value::Int(i) => i,
                    other => panic!("not an int: {other:?}"),
                })
                .collect()
        })
        .collect()
}

struct Root(PathBuf);
impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn start() -> (Root, Vec<ClusterNode>) {
    let (root, nodes, _) = start_with_configs();
    (root, nodes)
}

fn start_with_configs() -> (Root, Vec<ClusterNode>, Vec<TransportConfig>) {
    let root = Root(std::env::temp_dir().join(format!(
        "gateway-cluster-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )));
    let layout = presplit(&splits(), &NODES, 3).unwrap();
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
    let configs: Vec<_> = NODES
        .iter()
        .map(|&node| config_for(&pki, node, CLUSTER, &addresses))
        .collect();
    let nodes: Vec<ClusterNode> = NODES
        .iter()
        .zip(&configs)
        .map(|(&node, transport)| {
            ClusterNode::start(ClusterNodeConfig {
                transport: transport.clone(),
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
    assert!(nodes[0].bootstrap_directory(Duration::from_secs(60)).unwrap());
    // A second bootstrap finds the directory and writes nothing.
    assert!(!nodes[0].bootstrap_directory(Duration::from_secs(60)).unwrap());
    for node in &nodes {
        node.wait_ready(Duration::from_secs(60)).unwrap();
    }
    (root, nodes, configs)
}

fn gateway(node: &ClusterNode) -> ClusterGateway {
    node.attach_gateway(
        GatewaySettings {
            node: node.node_id() as u16,
            heartbeat: Duration::from_millis(500),
            ..GatewaySettings::default()
        },
        Limits::default(),
    )
    .unwrap()
}

#[test]
fn two_gateways_write_read_and_take_sequence_values_over_raft_range_groups() {
    let (_root, nodes) = start();
    // Every range has a leader.
    let leaders: BTreeSet<u64> = nodes[0]
        .layout()
        .iter()
        .filter_map(|d| {
            nodes
                .iter()
                .find(|n| n.host().service(d.id).is_some())
                .map(|n| n.node_id())
        })
        .collect();
    assert_eq!(
        nodes[0]
            .layout()
            .iter()
            .filter(|d| nodes.iter().any(|n| n.host().service(d.id).is_some()))
            .count(),
        nodes[0].layout().len()
    );
    // Elections decide where leaders land (often all on one node); gateways on the other nodes
    // reach them over the peer wire either way.
    eprintln!("range leaders on nodes {leaders:?}");

    // DDL from one gateway (DDL across gateways is Phase 11; a gateway sees DDL committed
    // before it attached).
    let a = gateway(&nodes[0]);
    let mut sa = a.admin.sql_session();
    sql(&mut sa, "CREATE TABLE t(id SERIAL PRIMARY KEY, g INT)").unwrap();
    sql(&mut sa, "CREATE TABLE u(id INT PRIMARY KEY, v INT)").unwrap();
    let b = gateway(&nodes[1]);
    let mut sb = b.admin.sql_session();

    // Writes on one node, reads on another; SERIAL values from both gateways are unique.
    for i in 0..10 {
        sql(&mut sa, &format!("INSERT INTO t(g) VALUES ({})", 2 * i + 1)).unwrap();
        sql(&mut sb, &format!("INSERT INTO t(g) VALUES ({})", 2 * i + 2)).unwrap();
    }
    for session in [&mut sa, &mut sb] {
        let rows = ints(sql(session, "SELECT id, g FROM t ORDER BY g").unwrap());
        assert_eq!(rows.len(), 20);
        let ids: BTreeSet<i64> = rows.iter().map(|r| r[0]).collect();
        assert_eq!(ids.len(), 20, "{rows:?}");
        // One counter on the system range leader: values are handed out in call order.
        let in_order: Vec<i64> = rows.iter().map(|r| r[0]).collect();
        assert!(in_order.windows(2).all(|w| w[0] < w[1]), "{in_order:?}");
    }
    // nextval through each gateway's routed client.
    let x = ints(sql(&mut sa, "SELECT nextval('t_id_seq')").unwrap())[0][0];
    let y = ints(sql(&mut sb, "SELECT nextval('t_id_seq')").unwrap())[0][0];
    assert!(y > x && x > 20, "{x} {y}");

    // One transaction over two tables in different ranges commits as one...
    for statement in [
        "BEGIN",
        "INSERT INTO t(g) VALUES (100)",
        "INSERT INTO u VALUES (1, 100)",
        "COMMIT",
    ] {
        sql(&mut sb, statement).unwrap();
    }
    // ...and a rolled-back one leaves nothing in either.
    for statement in [
        "BEGIN",
        "INSERT INTO t(g) VALUES (200)",
        "INSERT INTO u VALUES (2, 200)",
        "ROLLBACK",
    ] {
        sql(&mut sb, statement).unwrap();
    }
    for session in [&mut sa, &mut sb] {
        assert_eq!(
            ints(sql(session, "SELECT g FROM t WHERE g >= 100").unwrap()),
            vec![vec![100]]
        );
        assert_eq!(
            ints(sql(session, "SELECT id, v FROM u ORDER BY id").unwrap()),
            vec![vec![1, 100]]
        );
    }
    // Transaction status across gateways (R5-55): B asks about A's xid over the peer wire.
    let xid = |rows: Vec<Vec<Value>>| match &rows[0][0] {
        Value::Int(x) => *x,
        other => panic!("xid: {other:?}"),
    };
    let status = |s: &mut SqlSession, x: i64| match &sql(s, &format!("SELECT pg_xact_status({x})"))
        .unwrap()[0][0]
    {
        Value::Text(t) => t.clone(),
        other => panic!("status: {other:?}"),
    };
    sql(&mut sa, "BEGIN").unwrap();
    let open = xid(sql(&mut sa, "SELECT pg_current_xact_id()").unwrap());
    sql(&mut sa, "INSERT INTO u VALUES (3, 300)").unwrap();
    assert_eq!(status(&mut sb, open), "in progress");
    sql(&mut sa, "COMMIT").unwrap();
    assert_eq!(status(&mut sb, open), "committed");
    sql(&mut sa, "BEGIN").unwrap();
    let rolled = xid(sql(&mut sa, "SELECT pg_current_xact_id()").unwrap());
    sql(&mut sa, "INSERT INTO u VALUES (4, 400)").unwrap();
    sql(&mut sa, "ROLLBACK").unwrap();
    assert_eq!(status(&mut sb, rolled), "aborted");

    // A third gateway, on the node that took no SQL yet, sees all of it.
    let c = gateway(&nodes[2]);
    let mut sc = c.admin.sql_session();
    assert_eq!(
        ints(sql(&mut sc, "SELECT count(*) FROM t").unwrap()),
        vec![vec![21]]
    );
    drop((sa, sb, sc, a, b, c));
    for node in nodes {
        node.stop();
    }
}

/// Every node's report, as the CLI asks: node 1's certificate, every member including itself.
fn reports(config: &TransportConfig) -> Vec<NodeReport> {
    query_status(config, &NODES, Duration::from_secs(10))
        .unwrap()
        .into_iter()
        .map(|(node, answer)| {
            let report = answer.unwrap_or_else(|e| panic!("node {node}: {e}"));
            assert_eq!(report.node, node);
            report
        })
        .collect()
}

fn total_calls(reports: &[NodeReport]) -> u64 {
    reports
        .iter()
        .flat_map(|r| &r.ranges)
        .map(|r| r.calls_total())
        .sum()
}

#[test]
fn every_node_reports_its_ranges_with_one_leader_each_and_counts_served_calls() {
    let (_root, nodes, configs) = start_with_configs();
    let layout = nodes[0].layout().to_vec();
    // A peer call from node 1 to node 2 answers what node 2 reports locally (modulo timing).
    let over_wire = match nodes[0]
        .transport()
        .calls()
        .call(2, PeerCall::Node(NodeCall::Status), Duration::from_secs(10))
        .unwrap()
    {
        PeerReply::Node(report) => report,
        other => panic!("{other:?}"),
    };
    let ids = |r: &NodeReport| r.ranges.iter().map(|r| r.descriptor.id).collect::<Vec<_>>();
    assert_eq!(ids(&over_wire), ids(&nodes[1].status()));
    // Leaders settle after wait_ready; allow a re-election to finish.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let before = loop {
        let reports = reports(&configs[0]);
        let leaders: Vec<usize> = layout
            .iter()
            .map(|d| {
                reports
                    .iter()
                    .flat_map(|r| &r.ranges)
                    .filter(|r| r.descriptor.id == d.id && r.role == RangeRole::Leader)
                    .count()
            })
            .collect();
        if leaders.iter().all(|&n| n == 1) {
            break reports;
        }
        assert!(std::time::Instant::now() < deadline, "leaders {leaders:?}");
        std::thread::sleep(Duration::from_millis(100));
    };
    for report in &before {
        // Each node lists exactly the layout's ranges that name it as a replica.
        let mut expected: Vec<_> = layout
            .iter()
            .filter(|d| d.replicas.iter().any(|r| r.node == report.node))
            .map(|d| d.id)
            .collect();
        expected.sort();
        let mut listed = ids(report);
        listed.sort();
        assert_eq!(listed, expected, "node {}", report.node);
        for range in &report.ranges {
            assert!(range.term > 0 && range.leader != 0, "{range:?}");
            if range.role == RangeRole::Leader {
                assert_eq!(range.leader, report.node);
                assert!(range.applied > 0, "{range:?}");
            }
            assert!(range.window_ms > 0 && range.size.is_none());
        }
    }
    // wait_ready read every range through its leader: counters already moved.
    assert!(total_calls(&before) > 0);
    let a = gateway(&nodes[0]);
    let mut sa = a.admin.sql_session();
    sql(&mut sa, "CREATE TABLE t(id INT PRIMARY KEY, g INT)").unwrap();
    for i in 0..20 {
        sql(&mut sa, &format!("INSERT INTO t VALUES ({i}, {i})")).unwrap();
    }
    assert_eq!(
        ints(sql(&mut sa, "SELECT count(*) FROM t").unwrap()),
        vec![vec![20]]
    );
    let after = reports(&configs[0]);
    assert!(
        total_calls(&after) >= total_calls(&before) + 20,
        "{} -> {}",
        total_calls(&before),
        total_calls(&after)
    );
    // Only leaders serve: the writes landed on leader replicas, with a recent rate.
    let writes: u64 = after
        .iter()
        .flat_map(|r| &r.ranges)
        .map(|r| r.calls.writes)
        .sum();
    assert!(writes >= 20, "{writes}");
    assert!(
        after
            .iter()
            .flat_map(|r| &r.ranges)
            .any(|r| r.role == RangeRole::Leader && r.calls_per_sec() > 0.0)
    );
    drop((sa, a));
    for node in nodes {
        node.stop();
    }
}

/// Review S5: a first start cut off after the store was created but before its last range was
/// (here: only the system range is stored) is completed by the next start, which creates the
/// layout's hosted ranges the store lacks and then writes the layout marker.
#[test]
fn a_first_start_cut_off_while_creating_ranges_is_completed_on_restart() {
    let root = Root(std::env::temp_dir().join(format!(
        "gateway-cluster-partial-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )));
    let nodes = [1_u64];
    let layout = presplit(&splits(), &nodes, 1).unwrap();
    let pki = make_pki(&root.0.join("pki"), &nodes);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addresses = BTreeMap::from([(1, listener.local_addr().unwrap().to_string())]);
    drop(listener);
    let data_dir = root.0.join("n1/data");
    std::fs::create_dir_all(&data_dir).unwrap();
    {
        let store = RangeStore::create(&data_dir, CLUSTER).unwrap();
        let system = layout.iter().find(|d| d.id == SYSTEM_RANGE).unwrap();
        store.create_range(system).unwrap();
    }
    let marker = root.0.join("n1/data.layout-created");
    assert!(!marker.exists());
    let node = ClusterNode::start(ClusterNodeConfig {
        transport: config_for(&pki, 1, CLUSTER, &addresses),
        data_dir,
        log_dir: root.0.join("n1/log"),
        snap_dir: root.0.join("n1/snap"),
        tick: Duration::from_millis(20),
        layout: layout.clone(),
        engine: EngineSettings::default(),
        liveness: None,
        policy: None,
        call_limits: None,
    })
    .unwrap();
    assert!(marker.exists());
    assert!(node.bootstrap_directory(Duration::from_secs(60)).unwrap());
    node.wait_ready(Duration::from_secs(60)).unwrap();
    let mut listed: Vec<_> = node
        .status()
        .ranges
        .iter()
        .map(|r| r.descriptor.id)
        .collect();
    listed.sort();
    let mut expected: Vec<_> = layout.iter().map(|d| d.id).collect();
    expected.sort();
    assert_eq!(listed, expected);
    node.stop();
}
