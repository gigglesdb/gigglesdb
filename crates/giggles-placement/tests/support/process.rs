use super::*;
use giggles_distributed::{
    runtime::{Config, GroupConfig, Initial},
    transaction,
    transport::{Configuration, Peer, fingerprint},
};
use giggles_placement::{
    Owner,
    owner::{ProcessConfig, StdioOwner},
};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
pub struct Fixture {
    pub root: PathBuf,
    pub configs: BTreeMap<u64, ProcessConfig>,
    pub owner: StdioOwner,
    pub round: u64,
}
impl Fixture {
    pub fn new() -> Self {
        let root = fresh("four-mtls");
        eprintln!("retained fixture {}", root.display());
        let binary = std::env::var_os("GIGGLES_RANGES_BIN")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                Path::new(env!("CARGO_MANIFEST_DIR")).join("target/debug/giggles-ranges.exe")
            });
        assert!(
            binary.is_file(),
            "build actual runtime first into owned target: {}",
            binary.display()
        );
        let mut cp = CertificateParams::new(Vec::<String>::new()).unwrap();
        cp.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        cp.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let cak = KeyPair::generate().unwrap();
        let ca = cp.self_signed(&cak).unwrap();
        std::fs::write(root.join("ca.pem"), ca.pem()).unwrap();
        let mut leaves = BTreeMap::new();
        for id in 1..=4 {
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
        let mut per_node = BTreeMap::<u64, Vec<GroupConfig>>::new();
        let mut listeners = Vec::new();
        for group in 1..=4 {
            let mut peers = BTreeMap::new();
            for id in 1..=4 {
                let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                peers.insert(
                    id,
                    Peer {
                        address: l.local_addr().unwrap().to_string(),
                        fingerprint: leaves[&id].clone(),
                    },
                );
                listeners.push(l);
            }
            for id in 1..=4 {
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
                        database: root.join(format!("n{id}-g{group}.redb")),
                        listen: peers[&id].address.clone(),
                        certificate: root.join(format!("{id}.pem")),
                        key: root.join(format!("{id}.key.pem")),
                        ca: root.join("ca.pem"),
                        peers: peers.clone(),
                    },
                });
            }
        }
        let mut configs = BTreeMap::new();
        for (id, groups) in per_node {
            let config = root.join(format!("config-{id}.json"));
            std::fs::write(
                &config,
                serde_json::to_vec_pretty(&Config {
                    domain: 42,
                    node: id,
                    groups,
                })
                .unwrap(),
            )
            .unwrap();
            configs.insert(
                id,
                ProcessConfig {
                    binary: binary.clone(),
                    config,
                    artifacts: root.join(format!("process-{id}-1")),
                    timeout_ms: 12000,
                },
            );
        }
        drop(listeners);
        let owner = StdioOwner::start(configs.clone()).unwrap();
        let mut s = Self {
            root,
            configs,
            owner,
            round: 1,
        };
        for group in 1..=2 {
            let _ = s
                .owner
                .processes
                .get_mut(&1)
                .unwrap()
                .request(json!({"command":"campaign","group":group}));
            s.current(group);
        }
        s
    }
    pub fn current(&mut self, group: u64) -> giggles_distributed::runtime::Observation {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match self.owner.current(group) {
                Ok(o) => return o,
                Err(e) => {
                    assert!(
                        Instant::now() < deadline,
                        "current: {e}; {}",
                        self.root.display()
                    );
                    std::thread::sleep(Duration::from_millis(25));
                }
            }
        }
    }
    pub fn request(&mut self, group: u64, v: Value) -> Value {
        let o = self.current(group);
        self.owner
            .processes
            .get_mut(&o.leader)
            .unwrap()
            .request(v)
            .unwrap()
    }
    pub fn transaction() -> transaction::Plan {
        transaction::Plan {
            database: 42,
            nonce: 40001,
            home: descriptor(),
            participants: vec![transaction::Participant {
                route: descriptor(),
                writes: vec![transaction::Write {
                    key: giggles_core::Key {
                        tenant: giggles_core::TenantId(42),
                        row: giggles_core::RowId(1),
                    },
                    before: None,
                    row: giggles_core::Row {
                        status: giggles_core::Status::Failed,
                        failed_at: 103,
                        attempt_count: 7,
                        version: 999,
                    },
                }],
                reads: vec![],
            }],
        }
    }
    pub fn step_until(&mut self, c: &mut Controller, phase: Phase) {
        let deadline = Instant::now() + Duration::from_secs(40);
        loop {
            let o = self.current(2);
            if o.membership
                .receipts
                .get(&4001)
                .is_some_and(|p| p.phase == phase)
            {
                return;
            }
            let result = c.step(&mut self.owner);
            assert!(
                Instant::now() < deadline,
                "step timeout {result:?}; {}",
                self.root.display()
            );
            if let Err(e) = result {
                assert!(
                    matches!(e.code, Code::Runtime | Code::Transport | Code::NoQuorum),
                    "{e}"
                );
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    }
    pub fn restart(&mut self) {
        self.owner.processes.clear();
        self.round += 1;
        for (id, c) in &mut self.configs {
            c.artifacts = self.root.join(format!("process-{id}-{}", self.round));
        }
        self.owner = StdioOwner::start(self.configs.clone()).unwrap();
        self.current(2);
    }
}
