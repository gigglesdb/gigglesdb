mod common;
use common::*;
use giggles_distributed::{
    runtime::{Config, GroupConfig, Initial},
    transport::{Configuration, Peer, fingerprint},
    *,
};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdin, Command, Stdio},
    sync::mpsc,
    time::{Duration, Instant},
};

struct Running {
    child: Child,
    input: ChildStdin,
    output: mpsc::Receiver<Value>,
    history: std::fs::File,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_embedded_runtime_releases_peer_listeners_and_stores() {
    let mut fixture = Processes::new();
    fixture.nodes.clear(); // Stops only this fixture's owned child processes.
    let config: Config =
        serde_json::from_slice(&std::fs::read(&fixture.configs[&1]).unwrap()).unwrap();
    for _ in 0..2 {
        let runtime = runtime::Runtime::start(config.clone()).await.unwrap();
        let handle = runtime.group(1).unwrap().clone();
        handle.inspect().await.unwrap();
        drop(runtime);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let listeners: std::io::Result<Vec<_>> = config
                .groups
                .iter()
                .map(|g| std::net::TcpListener::bind(&g.peer.listen))
                .collect();
            if let Ok(listeners) = listeners {
                let stores = config
                    .groups
                    .iter()
                    .map(|g| {
                        Store::open(&g.peer.database, config.node, config.domain, g.peer.group)
                    })
                    .collect::<Result<Vec<_>>>();
                if stores.is_ok() {
                    drop(stores);
                    drop(listeners);
                    break;
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "runtime retained listeners or stores after drop"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(handle.inspect().await.is_err());
    }
}
impl Running {
    fn start(config: &Path, root: &Path, id: u64, round: u64) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_giggles-ranges"))
            .arg(config)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(std::fs::File::create(root.join(format!("stderr-{id}-{round}.txt"))).unwrap())
            .spawn()
            .unwrap();
        let input = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (tx, output) = mpsc::channel();
        let mut log =
            std::fs::File::create(root.join(format!("stdout-{id}-{round}.jsonl"))).unwrap();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                writeln!(log, "{line}").unwrap();
                if let Ok(v) = serde_json::from_str(&line) {
                    let _ = tx.send(v);
                }
            }
        });
        let s = Self {
            child,
            input,
            output,
            history: std::fs::File::create(root.join(format!("input-{id}-{round}.jsonl"))).unwrap(),
        };
        assert_eq!(
            s.output.recv_timeout(Duration::from_secs(10)).unwrap()["ready"],
            true
        );
        s
    }
    fn send(&mut self, v: Value) {
        writeln!(self.history, "{v}").unwrap();
        writeln!(self.input, "{v}").unwrap();
        self.input.flush().unwrap();
    }
    fn receive(&self) -> Value {
        self.output
            .recv_timeout(Duration::from_secs(12))
            .expect("process response timeout")
    }
    fn request(&mut self, v: Value) -> Value {
        self.send(v);
        self.receive()
    }
}
impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
struct Processes {
    root: PathBuf,
    configs: BTreeMap<u64, PathBuf>,
    nodes: BTreeMap<u64, Running>,
    round: u64,
}
impl Processes {
    fn new() -> Self {
        let root = fresh("multi-process");
        let mut cp = CertificateParams::new(Vec::<String>::new()).unwrap();
        cp.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        cp.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let cak = KeyPair::generate().unwrap();
        let ca = cp.self_signed(&cak).unwrap();
        std::fs::write(root.join("ca.pem"), ca.pem()).unwrap();
        let mut leaves = BTreeMap::new();
        for id in VOTERS {
            let mut p = CertificateParams::new(vec!["localhost".into()]).unwrap();
            p.key_usages = vec![KeyUsagePurpose::DigitalSignature];
            p.extended_key_usages = vec![
                ExtendedKeyUsagePurpose::ServerAuth,
                ExtendedKeyUsagePurpose::ClientAuth,
            ];
            let k = KeyPair::generate().unwrap();
            let c = p.signed_by(&k, &ca, &cak).unwrap();
            std::fs::write(root.join(format!("{id}.pem")), c.pem()).unwrap();
            std::fs::write(root.join(format!("{id}.key.pem")), k.serialize_pem()).unwrap();
            leaves.insert(id, fingerprint(c.der()));
        }
        let mut configs = BTreeMap::new();
        let mut per_node = BTreeMap::<u64, Vec<GroupConfig>>::new();
        let mut listeners = Vec::new();
        for group in 1..=4 {
            let mut peers = BTreeMap::new();
            for id in VOTERS {
                let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                peers.insert(
                    id,
                    Peer {
                        address: listener.local_addr().unwrap().to_string(),
                        fingerprint: leaves[&id].clone(),
                    },
                );
                listeners.push(listener);
            }
            for id in VOTERS {
                per_node.entry(id).or_default().push(GroupConfig {
                    initial: match group {
                        1 => Initial::Directory(descriptor()),
                        2 => Initial::Data(descriptor()),
                        _ => Initial::Vacant,
                    },
                    peer: Configuration {
                        id,
                        domain: 42,
                        group,
                        database: root.join(format!("node-{id}-group-{group}.redb")),
                        listen: peers[&id].address.clone(),
                        certificate: root.join(format!("{id}.pem")),
                        key: root.join(format!("{id}.key.pem")),
                        ca: root.join("ca.pem"),
                        peers: peers.clone(),
                    },
                });
            }
        }
        for (id, groups) in per_node {
            let path = root.join(format!("config-{id}.json"));
            std::fs::write(
                &path,
                serde_json::to_vec_pretty(&Config {
                    domain: 42,
                    node: id,
                    groups,
                })
                .unwrap(),
            )
            .unwrap();
            configs.insert(id, path);
        }
        drop(listeners);
        let mut s = Self {
            root,
            configs,
            nodes: BTreeMap::new(),
            round: 0,
        };
        s.start();
        s
    }
    fn start(&mut self) {
        self.round += 1;
        for id in VOTERS {
            self.nodes.insert(
                id,
                Running::start(&self.configs[&id], &self.root, id, self.round),
            );
        }
        self.colocate();
    }
    fn restart(&mut self) {
        self.nodes = BTreeMap::new();
        self.start();
    }
    fn leader(&mut self, group: u64) -> u64 {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            for id in VOTERS {
                let v = self
                    .nodes
                    .get_mut(&id)
                    .unwrap()
                    .request(json!({"command":"inspect","group":group}));
                if v["ok"] == true && v["data"]["is_leader"] == true {
                    return id;
                }
            }
            assert!(Instant::now() < deadline, "no group leader {group}");
        }
    }
    fn colocate(&mut self) {
        for group in 1..=4 {
            self.nodes
                .get_mut(&1)
                .unwrap()
                .request(json!({"command":"campaign","group":group}));
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                let leader = self.leader(group);
                if leader == 1 {
                    break;
                }
                let r = self
                    .nodes
                    .get_mut(&leader)
                    .unwrap()
                    .request(json!({"command":"transfer","group":group,"to":1}));
                assert_eq!(r["ok"], true, "{r}");
                assert!(Instant::now() < deadline, "transfer timeout");
            }
        }
    }
    fn request(&mut self, v: Value) -> Value {
        self.nodes.get_mut(&1).unwrap().request(v)
    }
    fn current(&mut self, g: u64) -> Machine {
        let v = self.request(json!({"command":"current","group":g}));
        assert_eq!(v["ok"], true, "{v}");
        serde_json::from_value(v["data"]["machine"].clone()).unwrap()
    }
    fn apply(&mut self, g: u64, action: Action) -> Value {
        let v = self.request(json!({"command":"apply","group":g,"action":action}));
        assert_eq!(v["ok"], true, "{v}");
        v
    }
    fn data(&mut self, g: u64) -> Data {
        let Machine::Range { data, .. } = self.current(g) else {
            panic!()
        };
        *data
    }
    fn resume(&mut self) {
        let r = self.request(json!({"command":"resume","id":77}));
        assert_eq!(r["ok"], true, "{r}");
    }
}

#[test]
fn real_three_process_split_restart_at_every_durable_lifecycle_boundary() {
    for boundary in 0..8 {
        let mut p = Processes::new();
        // Oracle comes from the requested independent fixture, not a sum of child
        // metadata. Version 999 is deliberately ignored by core put semantics.
        let fixture = [1, 49, 50, 99];
        for k in fixture {
            p.apply(2, put(descriptor(), k, key(k), None));
        }
        p.apply(2, put(descriptor(), 1000, key(1), Some(0)));
        let expected = fixture
            .into_iter()
            .map(|n| {
                let mut r = row();
                r.version = if n == 1 { 1 } else { 0 };
                (key(n), r)
            })
            .collect::<BTreeMap<_, _>>();
        std::fs::write(
            p.root.join("oracle.json"),
            serde_json::to_vec(&expected.iter().collect::<Vec<_>>()).unwrap(),
        )
        .unwrap();
        let s = split();
        let mut step = 0;
        let crash = |p: &mut Processes, step: &mut usize| {
            if *step == boundary {
                p.restart();
            }
            *step += 1;
        };
        crash(&mut p, &mut step);
        p.apply(1, Action::Begin(s.clone()));
        crash(&mut p, &mut step);
        p.apply(2, Action::Fence(s.clone()));
        crash(&mut p, &mut step);
        let children = p.data(2).children().unwrap();
        let proofs = [
            children[0].provenance.clone().unwrap(),
            children[1].provenance.clone().unwrap(),
        ];
        for child in children {
            p.apply(child.descriptor.group, Action::Bootstrap(Box::new(child)));
            crash(&mut p, &mut step);
        }
        p.apply(
            1,
            Action::Publish {
                id: 77,
                ready: Box::new(proofs.clone()),
            },
        );
        crash(&mut p, &mut step);
        for proof in proofs {
            p.apply(proof.child.group, Action::Activate { proof, revision: 2 });
            crash(&mut p, &mut step);
        }
        p.restart();
        p.resume();
        p.resume();
        let actual = p
            .data(3)
            .image
            .snapshot()
            .unwrap()
            .rows
            .into_iter()
            .chain(p.data(4).image.snapshot().unwrap().rows)
            .collect::<BTreeMap<_, _>>();
        assert_eq!(actual, expected);
        for g in [3, 4] {
            let snapshot = p.data(g).image.snapshot().unwrap();
            assert!(
                snapshot.tasks.is_empty()
                    && snapshot.operations.is_empty()
                    && snapshot.approvals.is_empty()
            );
        }
        let old = p.request(
            json!({"command":"apply","group":2,"action":put(descriptor(),5000,key(2),None)}),
        );
        assert_eq!(old["ok"], false);
        let task = p.request(json!({"command":"apply","group":3,"action":Action::TaskWrite}));
        assert_eq!(task["ok"], false);
        let scan =
            p.request(json!({"command":"observe","bounds":Bounds{start:minimum(),end:None}}));
        assert_eq!(scan["ok"], true, "{scan}");
        let observed: Vec<(giggles_core::Key, giggles_core::Row)> =
            serde_json::from_value(scan["data"].clone()).unwrap();
        assert_eq!(observed.into_iter().collect::<BTreeMap<_, _>>(), expected);
        p.apply(3, put(s.left, 6000, key(2), None));
        p.apply(4, put(s.right, 6001, key(60), None));
        let ck = p.request(json!({"command":"checkpoint","group":3}));
        assert_eq!(ck["ok"], true);
        p.restart();
        assert!(
            p.data(3)
                .image
                .snapshot()
                .unwrap()
                .rows
                .contains_key(&key(2))
        );
    }
}

#[test]
fn real_process_independent_group_partition_and_one_child_ready_recovery() {
    let mut p = Processes::new();
    let s = split();
    p.apply(1, Action::Begin(s.clone()));
    p.apply(2, put(descriptor(), 1, key(1), None));
    p.apply(2, Action::Fence(s));
    let children = p.data(2).children().unwrap();
    p.apply(3, Action::Bootstrap(Box::new(children[0].clone())));
    // Isolate only right-child group, leaving directory/left independently live.
    for id in VOTERS {
        let peers = VOTERS
            .into_iter()
            .filter(|other| *other != id)
            .collect::<Vec<_>>();
        let r = p
            .nodes
            .get_mut(&id)
            .unwrap()
            .request(json!({"command":"block","group":4,"peers":peers}));
        assert_eq!(r["ok"], true);
    }
    let noquorum = p.request(json!({"command":"resume","id":77}));
    assert_eq!(noquorum["ok"], false, "{noquorum}");
    let Machine::Root { directory, .. } = p.current(1) else {
        panic!()
    };
    assert_eq!(directory.revision, 1);
    assert!(!directory.intents[&77].published);
    assert_eq!(p.data(3).descriptor.range, 11);
    let scan = p.request(json!({"command":"observe","bounds":Bounds{start:minimum(),end:None}}));
    assert_eq!(scan["ok"], false);
    assert!(scan.get("data").is_none());
    p.restart();
    p.resume();
    let Machine::Root { directory, .. } = p.current(1) else {
        panic!()
    };
    assert_eq!(directory.revision, 2);
    assert_eq!(p.data(3).image.snapshot().unwrap().rows[&key(1)].version, 0);
}

#[test]
fn concurrent_routed_write_and_split_fence_has_one_complete_outcome() {
    let mut p = Processes::new();
    p.apply(1, Action::Begin(split()));
    let route = descriptor();
    let race=p.request(json!({"command":"concurrent","group":2,"actions":[put(route.clone(),1,key(1),None),Action::Fence(split())]}));
    assert_eq!(race["ok"], true, "{race}");
    assert_eq!(race["data"].as_array().unwrap().len(), 2);
    p.resume();
    let left = p.data(3).image.snapshot().unwrap().rows;
    let successful = race["data"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["reply"]["code"] == 0)
        .count();
    assert_eq!(successful, if left.contains_key(&key(1)) { 2 } else { 1 });
    let attempt = p.request(json!({"command":"apply","group":2,"action":put(route,2,key(2),None)}));
    assert_eq!(attempt["ok"], false);
}

#[test]
fn real_process_crash_before_and_after_each_applied_lifecycle_save() {
    for after in [false, true] {
        for stage in 0..7 {
            let mut p = Processes::new();
            let s = split();
            p.apply(2, put(descriptor(), 1, key(1), None));
            let mut actions = vec![(1, Action::Begin(s.clone())), (2, Action::Fence(s.clone()))];
            if stage >= 2 {
                for (g, a) in actions.drain(..) {
                    p.apply(g, a);
                }
            }
            let children = if stage >= 2 {
                Some(p.data(2).children().unwrap())
            } else {
                None
            };
            if let Some(children) = children {
                let proofs = [
                    children[0].provenance.clone().unwrap(),
                    children[1].provenance.clone().unwrap(),
                ];
                actions.extend(
                    children
                        .into_iter()
                        .map(|child| (child.descriptor.group, Action::Bootstrap(Box::new(child)))),
                );
                actions.push((
                    1,
                    Action::Publish {
                        id: 77,
                        ready: Box::new(proofs.clone()),
                    },
                ));
                actions.extend(
                    proofs
                        .into_iter()
                        .map(|proof| (proof.child.group, Action::Activate { proof, revision: 2 })),
                );
            }
            let offset = if stage >= 2 { 2 } else { 0 };
            let (g, target) = actions.remove(stage - offset);
            for (group, action) in actions.into_iter().take(stage - offset) {
                p.apply(group, action);
            }
            let arm = p.request(json!({"command":"arm_crash","group":g,"after":after}));
            assert_eq!(arm["ok"], true, "{arm}");
            let leader = p.nodes.get_mut(&1).unwrap();
            leader.send(json!({"command":"apply","group":g,"action":target}));
            let status = leader.child.wait().unwrap();
            assert_eq!(status.code(), Some(86));
            // Kill the remaining processes before recovery: their logs may contain a
            // committed or uncommitted tail. No timeout is interpreted as rollback.
            p.restart();
            p.apply(1, Action::Begin(s));
            p.resume();
            assert_eq!(p.data(3).image.snapshot().unwrap().rows[&key(1)].version, 0);
            assert!(p.data(4).image.snapshot().unwrap().rows.is_empty());
        }
    }
}

#[test]
fn ownership_change_during_observation_returns_retry_without_partial_rows() {
    let mut p = Processes::new();
    p.apply(2, put(descriptor(), 1, key(1), None));
    p.apply(2, put(descriptor(), 2, key(99), None));
    p.apply(1, Action::Begin(split()));
    let answer = p.request(
        json!({"command":"observe_resume","bounds":Bounds{start:minimum(),end:None},"id":77}),
    );
    assert_eq!(answer["ok"], false, "{answer}");
    assert!(answer.get("data").is_none());
    assert!(
        answer["error"]
            .as_str()
            .unwrap()
            .contains("ownership changed")
    );
    let retry = p.request(json!({"command":"observe","bounds":Bounds{start:minimum(),end:None}}));
    assert_eq!(retry["ok"], true);
    assert_eq!(retry["data"].as_array().unwrap().len(), 2);
}

#[test]
fn real_process_snapshot_plus_tail_catches_up_independent_group() {
    let mut p = Processes::new();
    for id in VOTERS {
        let peers = if id == 3 { vec![1, 2] } else { vec![3] };
        let answer = p
            .nodes
            .get_mut(&id)
            .unwrap()
            .request(json!({"command":"block","group":2,"peers":peers}));
        assert_eq!(answer["ok"], true);
    }
    for n in 1..=8 {
        p.apply(2, put(descriptor(), n, key(n), None));
    }
    let checkpoint = p.request(json!({"command":"checkpoint","group":2}));
    assert_eq!(checkpoint["ok"], true);
    // The fence is a log tail after the compacted image.
    p.apply(1, Action::Begin(split()));
    p.apply(2, Action::Fence(split()));
    for id in VOTERS {
        let answer = p
            .nodes
            .get_mut(&id)
            .unwrap()
            .request(json!({"command":"block","group":2,"peers":[]}));
        assert_eq!(answer["ok"], true);
    }
    let expected = p.current(2);
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let observed = p
            .nodes
            .get_mut(&3)
            .unwrap()
            .request(json!({"command":"inspect","group":2}));
        assert_eq!(observed["ok"], true);
        let machine: Machine = serde_json::from_value(observed["data"]["machine"].clone()).unwrap();
        if machine == expected {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "snapshot/tail follower not caught up"
        );
    }
    p.restart();
    p.resume();
    assert_eq!(p.data(3).image.snapshot().unwrap().rows.len(), 8);
}

#[test]
fn different_process_group_leaders_survive_leader_process_loss() {
    let mut p = Processes::new();
    let transfer = p.request(json!({"command":"transfer","group":2,"to":2}));
    assert_eq!(transfer["ok"], true);
    let deadline = Instant::now() + Duration::from_secs(15);
    while p.leader(2) != 2 {
        assert!(Instant::now() < deadline);
    }
    let write = p
        .nodes
        .get_mut(&2)
        .unwrap()
        .request(json!({"command":"apply","group":2,"action":put(descriptor(),1,key(1),None)}));
    assert_eq!(write["ok"], true, "{write}");
    // Independent groups may legitimately elect or transfer independently;
    // establish distinct leaders without assuming an immutable node location.
    while p.leader(1) == 2 {
        let transfer = p
            .nodes
            .get_mut(&2)
            .unwrap()
            .request(json!({"command":"transfer","group":1,"to":1}));
        assert_eq!(transfer["ok"], true);
        assert!(Instant::now() < deadline, "directory transfer timeout");
    }
    drop(p.nodes.remove(&2));
    let deadline = Instant::now() + Duration::from_secs(15);
    let leader = loop {
        let mut elected = None;
        for id in [1, 3] {
            let o = p
                .nodes
                .get_mut(&id)
                .unwrap()
                .request(json!({"command":"inspect","group":2}));
            if o["data"]["is_leader"] == true {
                elected = Some(id);
                break;
            }
        }
        if let Some(id) = elected {
            break id;
        }
        assert!(Instant::now() < deadline, "survivors did not elect");
    };
    let write = p
        .nodes
        .get_mut(&leader)
        .unwrap()
        .request(json!({"command":"apply","group":2,"action":put(descriptor(),2,key(2),None)}));
    assert_eq!(write["ok"], true, "{write}");
    let deadline = Instant::now() + Duration::from_secs(15);
    'directory: loop {
        for id in [1, 3] {
            let directory = p
                .nodes
                .get_mut(&id)
                .unwrap()
                .request(json!({"command":"current","group":1}));
            if directory["ok"] == true {
                assert_eq!(directory["data"]["quorum_checked"], true);
                break 'directory;
            }
        }
        assert!(
            Instant::now() < deadline,
            "surviving directory quorum unavailable"
        );
    }
    p.restart();
    let rows = p.data(2).image.snapshot().unwrap().rows;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[&key(1)].version, 0);
    assert_eq!(rows[&key(2)].version, 0);
}

#[tokio::test]
async fn real_mtls_peer_cannot_rewrap_foreign_group_log_or_envelope() {
    let mut p = Processes::new();
    let config: Config = serde_json::from_slice(&std::fs::read(&p.configs[&2]).unwrap()).unwrap();
    let peer = &config
        .groups
        .iter()
        .find(|g| g.peer.group == 2)
        .unwrap()
        .peer;
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut BufReader::new(
        std::fs::File::open(p.root.join("ca.pem")).unwrap(),
    )) {
        roots.add(cert.unwrap()).unwrap();
    }
    let chain = rustls_pemfile::certs(&mut BufReader::new(
        std::fs::File::open(p.root.join("1.pem")).unwrap(),
    ))
    .collect::<std::result::Result<Vec<_>, _>>()
    .unwrap();
    let client_key = rustls_pemfile::private_key(&mut BufReader::new(
        std::fs::File::open(p.root.join("1.key.pem")).unwrap(),
    ))
    .unwrap()
    .unwrap();
    let client = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(chain, client_key)
        .unwrap();
    let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(client));
    for mode in 0..3 {
        let tcp = tokio::net::TcpStream::connect(&peer.listen).await.unwrap();
        let mut tls = connector
            .connect(
                rustls::pki_types::ServerName::try_from("localhost").unwrap(),
                tcp,
            )
            .await
            .unwrap();
        let mut message = raft::eraftpb::Message {
            from: 1,
            to: 2,
            term: 999,
            ..Default::default()
        };
        message.set_msg_type(raft::eraftpb::MessageType::MsgAppend);
        message.mut_entries().push(raft::eraftpb::Entry {
            index: 1,
            term: 999,
            data: serde_json::to_vec(&common::command(3, Action::TaskWrite))
                .unwrap()
                .into(),
            ..Default::default()
        });
        let bytes = transport::encode_message(
            if mode == 0 { 43 } else { 42 },
            if mode == 1 { 3 } else { 2 },
            &message,
        )
        .unwrap();
        transport::write_frame(&mut tls, &bytes).await.unwrap();
        let reply = transport::read_frame(&mut tls).await;
        if mode < 2 {
            assert!(reply.is_err());
        } else {
            assert_eq!(reply.unwrap(), b"ok");
        }
    }
    // Enqueue a diagnostic behind the foreign log and prove the group remains
    // alive without applying it or accepting its term.
    let inspected = p
        .nodes
        .get_mut(&2)
        .unwrap()
        .request(json!({"command":"inspect","group":2}));
    assert_eq!(inspected["ok"], true);
    p.apply(2, put(descriptor(), 1, key(1), None));
    let rows = p.data(2).image.snapshot().unwrap().rows;
    assert_eq!(rows.len(), 1);
    let vacant = p.current(3);
    assert!(matches!(vacant, Machine::Vacant { .. }));
}

#[test]
fn restart_never_reuses_log_context_for_a_different_pending_attempt() {
    use raft::{GetEntriesContext, Storage};
    let mut p = Processes::new();
    p.apply(2, put(descriptor(), 1, key(1), None));
    p.restart();
    let replay = p.apply(2, put(descriptor(), 1, key(1), None));
    assert_eq!(replay["data"]["reply"]["data"]["version"], 0);
    let update = p.apply(2, put(descriptor(), 2, key(1), Some(0)));
    assert_eq!(update["data"]["reply"]["data"]["version"], 1);
    let config: Config = serde_json::from_slice(&std::fs::read(&p.configs[&1]).unwrap()).unwrap();
    let path = &config
        .groups
        .iter()
        .find(|g| g.peer.group == 2)
        .unwrap()
        .peer
        .database;
    p.nodes = BTreeMap::new();
    let store = Store::open(path, 1, 42, 2).unwrap();
    let mut contexts = std::collections::BTreeSet::new();
    let mut attempts = 0;
    for entry in store
        .entries(
            store.first_index().unwrap(),
            store.last_index().unwrap() + 1,
            None,
            GetEntriesContext::empty(false),
        )
        .unwrap()
    {
        if entry.data.is_empty() {
            continue;
        }
        let c: giggles_distributed::Command = serde_json::from_slice(&entry.data).unwrap();
        if matches!(c.action, Action::Put { .. }) {
            attempts += 1;
            assert!(
                contexts.insert(entry.context.to_vec()),
                "a retained old log entry can collide with a new pending request after restart"
            );
        }
    }
    assert_eq!(attempts, 3);
    std::fs::write(
        p.root.join("request-contexts.json"),
        serde_json::to_vec(&contexts).unwrap(),
    )
    .unwrap();
}
