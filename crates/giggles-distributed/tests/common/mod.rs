#![allow(dead_code)]
use giggles_core::{Key, Row, RowId, Status, TenantId};
use giggles_distributed::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write,
    path::PathBuf,
};
pub fn fresh(name: &str) -> PathBuf {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("artifacts")
        .join(format!(
            "{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
    std::fs::create_dir_all(&p).unwrap();
    p
}
pub fn key(row: u64) -> Key {
    Key {
        tenant: TenantId(42),
        row: RowId(row),
    }
}
pub fn row() -> Row {
    Row {
        status: Status::Failed,
        failed_at: 103,
        attempt_count: 0,
        version: 999,
    }
}
pub fn descriptor() -> Descriptor {
    Descriptor {
        range: 10,
        epoch: 1,
        group: 2,
        bounds: Bounds {
            start: minimum(),
            end: None,
        },
        voters: VOTERS.to_vec(),
    }
}
pub fn split() -> Split {
    let parent = descriptor();
    let mut left = parent.clone();
    left.range = 11;
    left.group = 3;
    left.epoch = 2;
    left.bounds.end = Some(key(50));
    let mut right = parent.clone();
    right.range = 12;
    right.group = 4;
    right.epoch = 2;
    right.bounds.start = key(50);
    Split {
        id: 77,
        parent,
        key: key(50),
        left,
        right,
    }
}
pub fn initial(group: u64) -> Machine {
    match group {
        1 => Machine::root(42, descriptor()).unwrap(),
        2 => Machine::range(42, descriptor()).unwrap(),
        _ => Machine::Vacant { domain: 42, group },
    }
}
pub fn command(group: u64, action: Action) -> Command {
    Command {
        domain: 42,
        group,
        action,
    }
}
pub fn put(route: Descriptor, id: u64, key: Key, expected: Option<u64>) -> Action {
    Action::Put {
        route,
        operation: id,
        key,
        expected,
        row: row(),
    }
}
pub struct Cluster {
    pub nodes: BTreeMap<(u64, u64), Node>,
    pub blocked: BTreeSet<(u64, u64, u64)>,
    pub root: PathBuf,
    pub seed: u64,
    pub responses: BTreeMap<(u64, Vec<u8>), u32>,
    pub reads: BTreeSet<(u64, Vec<u8>)>,
    pub serial: u64,
    pub restarts: u64,
    trace: std::fs::File,
}
impl Cluster {
    pub fn new(seed: u64) -> Self {
        let root = fresh(&format!("seed-{seed}"));
        let trace = std::fs::File::create(root.join("trace.jsonl")).unwrap();
        let mut nodes = BTreeMap::new();
        for group in 1..=4 {
            for id in VOTERS {
                nodes.insert(
                    (group, id),
                    Node::new(Store::simulated(id, 42, group, initial(group)).unwrap()).unwrap(),
                );
            }
        }
        let mut c = Self {
            nodes,
            blocked: BTreeSet::new(),
            root,
            seed,
            responses: BTreeMap::new(),
            reads: BTreeSet::new(),
            serial: 0,
            restarts: 0,
            trace,
        };
        writeln!(c.trace, "{}", serde_json::json!({"seed":seed})).unwrap();
        for group in 1..=4 {
            c.nodes.get_mut(&(group, 1)).unwrap().campaign().unwrap();
        }
        c.pump();
        c
    }
    pub fn pump(&mut self) {
        for _ in 0..1000 {
            let mut messages = Vec::new();
            for ((g, id), node) in &mut self.nodes {
                let out = node.drain().unwrap();
                for a in out.applied {
                    self.responses.insert((*g, a.context), a.reply.code);
                }
                for (ctx, _) in out.reads {
                    self.reads.insert((*g, ctx));
                }
                for m in out.messages {
                    messages.push((*g, *id, m));
                }
            }
            if messages.is_empty() {
                return;
            }
            self.seed = self.seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let len = messages.len();
            messages.rotate_left(self.seed as usize % len);
            for (g, from, m) in messages {
                writeln!(self.trace,"{}",serde_json::json!({"group":g,"from":from,"to":m.to,"type":format!("{:?}",m.get_msg_type())})).unwrap();
                if !self.blocked.contains(&(g, from, m.to)) {
                    self.nodes
                        .get_mut(&(g, m.to))
                        .unwrap()
                        .step(from, m)
                        .unwrap();
                }
            }
        }
        panic!("simulation did not quiesce")
    }
    pub fn tick(&mut self, n: usize) {
        for _ in 0..n {
            for node in self.nodes.values_mut() {
                node.deterministic_tick();
            }
            self.pump();
        }
    }
    pub fn submit(&mut self, g: u64, id: u64, action: Action) -> (Vec<u8>, Option<u32>) {
        self.serial += 1;
        let ctx = self.serial.to_be_bytes().to_vec();
        let c = command(g, action);
        writeln!(
            self.trace,
            "{}",
            serde_json::json!({"command":c,"node":id,"context":self.serial})
        )
        .unwrap();
        if self
            .nodes
            .get_mut(&(g, id))
            .unwrap()
            .propose(ctx.clone(), &c)
            .is_err()
        {
            return (ctx, None);
        }
        self.pump();
        let r = self.responses.get(&(g, ctx.clone())).copied();
        (ctx, r)
    }
    pub fn apply(&mut self, g: u64, action: Action) {
        assert_eq!(self.submit(g, 1, action).1, Some(0));
    }
    pub fn machine(&self, g: u64, id: u64) -> Machine {
        self.nodes[&(g, id)].store.image().unwrap()
    }
    pub fn restart(&mut self) {
        self.restarts += 1;
        let states = self
            .nodes
            .iter()
            .map(|((g, id), n)| ((*g, *id), n.store.durable_bytes().unwrap()))
            .collect::<Vec<_>>();
        self.nodes = BTreeMap::new();
        for ((g, id), bytes) in states {
            std::fs::write(
                self.root.join(format!(
                    "durable-{g}-{id}-{}-{}.bin",
                    self.serial, self.restarts
                )),
                &bytes,
            )
            .unwrap();
            self.nodes.insert(
                (g, id),
                Node::new(Store::recover_simulated(&bytes, id, 42, g).unwrap()).unwrap(),
            );
        }
        for g in 1..=4 {
            self.nodes.get_mut(&(g, 1)).unwrap().campaign().unwrap();
        }
        self.pump();
    }
    pub fn data(&self, g: u64) -> Data {
        match self.machine(g, 1) {
            Machine::Range { data, .. } => *data,
            _ => panic!("not data"),
        }
    }
}
