#![allow(dead_code)]
use giggles_consensus::*;
use giggles_core::*;
use raft::eraftpb::Message;
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
pub fn fresh(name: &str) -> PathBuf {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("artifacts")
        .join(format!(
            "{name}-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
    std::fs::create_dir_all(&p).unwrap();
    p
}
pub fn key(n: u64) -> Key {
    Key {
        tenant: TenantId(42),
        row: RowId(n),
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
pub fn grant(limit: u64, threshold: u64) -> Grant {
    Grant {
        task: TaskId(1),
        issuer: Principal(1),
        executor: Principal(2),
        approver: Principal(3),
        policy: Policy {
            tenant: TenantId(42),
            columns: BTreeSet::from([Column::Status]),
            transitions: BTreeSet::from([(Status::Failed, Status::Queued)]),
            incident: (100, 107),
            distinct_limit: limit,
            transition_limit: limit,
            approval_above: threshold,
            valid_from: 1,
            expires: 1000,
        },
    }
}
pub fn request(op: u64, ids: &[u64]) -> Request {
    Request {
        operation: OperationId(op),
        selection: Selection::Keys(ids.iter().map(|i| key(*i)).collect()),
        column: Column::Status,
        after: Status::Queued,
    }
}
pub fn admin(v: serde_json::Value) -> Command {
    Command::Admin(serde_json::to_vec(&v).unwrap())
}
pub fn task(action: TaskAction) -> Command {
    Command::Task {
        task: TaskId(1),
        principal: Principal(2),
        lower: 10,
        upper: 10,
        action,
    }
}
pub struct Cluster {
    pub nodes: BTreeMap<u64, Node>,
    pub queue: Vec<Message>,
    pub root: PathBuf,
    pub applied_events: BTreeMap<(u64, Vec<u8>), Applied>,
    pub responses: BTreeMap<Vec<u8>, serde_json::Value>,
    pub reads: BTreeSet<Vec<u8>>,
    pub blocked: BTreeSet<(u64, u64)>,
    pub rng: u64,
    pub trace: std::fs::File,
    pub serial: u64,
    pub drop_messages: bool,
}
impl Cluster {
    pub fn new(seed: u64) -> Self {
        let root = fresh(&format!("seed-{seed}"));
        let trace = std::fs::File::create(root.join("history.jsonl")).unwrap();
        let mut nodes = BTreeMap::new();
        for id in VOTERS {
            nodes.insert(
                id,
                Node::new(Store::create(root.join(format!("node-{id}.redb")), id, 42).unwrap())
                    .unwrap(),
            );
        }
        let mut s = Self {
            nodes,
            queue: Vec::new(),
            root,
            applied_events: BTreeMap::new(),
            responses: BTreeMap::new(),
            reads: BTreeSet::new(),
            blocked: BTreeSet::new(),
            rng: seed,
            trace,
            serial: 0,
            drop_messages: false,
        };
        writeln!(s.trace, "{}", json!({"seed":seed})).unwrap();
        s.nodes.get_mut(&1).unwrap().campaign().unwrap();
        s.pump();
        assert!(s.nodes[&1].is_leader());
        s
    }
    pub fn rand(&mut self) -> u64 {
        self.rng = self.rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        self.rng
    }
    pub fn drain(&mut self) {
        for (id, n) in &mut self.nodes {
            let out = n.drain().unwrap();
            for a in out.applied {
                let value = serde_json::to_value(&a.reply).unwrap();
                writeln!(
                    self.trace,
                    "{}",
                    json!({"node":id,"apply":a.index,"context":a.context,"reply":value})
                )
                .unwrap();
                self.responses.insert(a.context.clone(), value);
                self.applied_events.insert((*id, a.context.clone()), a);
            }
            for (ctx, idx) in out.reads {
                assert!(n.store.applied().unwrap() >= idx);
                self.reads.insert(ctx);
            }
            self.queue.extend(out.messages);
        }
    }
    pub fn pump(&mut self) {
        for _ in 0..10000 {
            self.drain();
            if self.queue.is_empty() {
                return;
            }
            let i = (self.rand() as usize) % self.queue.len();
            let m = self.queue.swap_remove(i);
            if self.drop_messages && self.rand().is_multiple_of(17) {
                writeln!(
                    self.trace,
                    "{}",
                    json!({"drop":true,"from":m.from,"to":m.to,"term":m.term})
                )
                .unwrap();
                continue;
            }
            if self.blocked.contains(&(m.from, m.to)) || !self.nodes.contains_key(&m.to) {
                continue;
            }
            let snap = m.has_snapshot();
            let from = m.from;
            let to = m.to;
            writeln!(self.trace,"{}",json!({"message":format!("{:?}",m.get_msg_type()),"from":from,"to":to,"term":m.term})).unwrap();
            self.nodes
                .get_mut(&to)
                .unwrap()
                .step(from, m.clone())
                .unwrap();
            if self.rand().is_multiple_of(5) {
                self.nodes.get_mut(&to).unwrap().step(from, m).unwrap();
            }
            if snap && let Some(n) = self.nodes.get_mut(&from) {
                n.report_snapshot(to, true);
            }
        }
        panic!("schedule did not quiesce");
    }
    pub fn isolate(&mut self, id: u64) {
        for other in VOTERS {
            if id != other {
                self.blocked.insert((id, other));
                self.blocked.insert((other, id));
            }
        }
    }
    pub fn heal(&mut self) {
        self.blocked.clear();
        for n in self.nodes.values_mut() {
            for _ in 0..2 {
                n.tick();
            }
        }
        self.pump();
    }
    pub fn submit(&mut self, leader: u64, c: Command) -> serde_json::Value {
        self.serial += 1;
        let ctx = self.serial.to_be_bytes().to_vec();
        writeln!(
            self.trace,
            "{}",
            json!({"invoke":self.serial,"leader":leader,"command":c})
        )
        .unwrap();
        self.nodes
            .get_mut(&leader)
            .unwrap()
            .propose(ctx.clone(), &c)
            .unwrap();
        self.pump();
        for _ in 0..20 {
            if self.responses.contains_key(&ctx) {
                break;
            }
            for n in self.nodes.values_mut() {
                n.deterministic_tick();
            }
            self.pump();
        }
        let reply = self.responses.get(&ctx).expect("committed result").clone();
        writeln!(
            self.trace,
            "{}",
            json!({"complete":self.serial,"reply":reply})
        )
        .unwrap();
        reply
    }
    pub fn provision(&mut self, rows: u64, limit: u64, threshold: u64) {
        for i in 0..rows {
            assert_eq!(
                self.submit(1, admin(json!({"command":"seed","key":key(i),"row":row()})))["code"],
                0
            );
        }
        assert_eq!(
            self.submit(
                1,
                admin(json!({"command":"grant","grant":grant(limit,threshold)}))
            )["code"],
            0
        );
        assert_eq!(self.submit(1,admin(json!({"command":"provision_task_resources","task":1,"limits":{"read_rows":100000,"disclosed_rows":10000,"encoded_bytes":10000000}})))["code"],0);
    }
    pub fn restart(&mut self, id: u64) {
        drop(self.nodes.remove(&id));
        let store = Store::open(self.root.join(format!("node-{id}.redb")), id, 42).unwrap();
        self.nodes.insert(id, Node::new(store).unwrap());
    }
    pub fn elect(&mut self, id: u64) {
        for _ in 0..20 {
            for n in self.nodes.values_mut() {
                n.deterministic_tick();
            }
            self.pump();
        }
        if !self.nodes[&id].is_leader() {
            self.nodes.get_mut(&id).unwrap().campaign().unwrap();
            self.pump();
        }
        assert!(self.nodes[&id].is_leader());
    }
    pub fn equal(&mut self) {
        self.drop_messages = false;
        for n in self.nodes.values_mut() {
            for _ in 0..2 {
                n.deterministic_tick();
            }
        }
        self.pump();
        let a = self.nodes.values().next().unwrap().store.image().unwrap();
        let idx = self.nodes.values().next().unwrap().store.applied().unwrap();
        for n in self.nodes.values() {
            assert_eq!(n.store.applied().unwrap(), idx, "replica applied lag");
            assert!(
                n.store.image().unwrap() == a,
                "image differs at equal index"
            );
        }
    }
    pub fn checkpoint_all(&self) {
        for n in self.nodes.values() {
            n.store.checkpoint().unwrap();
        }
    }
    pub fn hash_evidence(&self) {
        independent_history_check(&self.root);
        let mut f = std::fs::File::create(self.root.join("checker.txt")).unwrap();
        writeln!(
            f,
            "independent assertions completed; seed state {}; applied {:?}",
            self.rng,
            self.nodes
                .iter()
                .map(|(id, n)| (id, n.store.applied().unwrap()))
                .collect::<Vec<_>>()
        )
        .unwrap();
    }
}

/// Checks serialized histories without calling the engine/model under test.
/// Sequential invocations must have increasing committed indexes; identical
/// indexes across replicas must contain identical context and response.
pub fn independent_history_check(root: &std::path::Path) {
    let text = std::fs::read_to_string(root.join("history.jsonl")).unwrap();
    let mut slots = BTreeMap::new();
    let mut by_context = BTreeMap::new();
    let mut invoked = BTreeSet::new();
    let mut last_complete_index = 0;
    let mut count = 0;
    for line in text.lines() {
        let v: serde_json::Value = serde_json::from_str(line).unwrap();
        if let Some(id) = v["invoke"].as_u64() {
            assert!(invoked.insert(id));
        }
        if let Some(index) = v["apply"].as_u64() {
            let record = json!({"context":v["context"],"reply":v["reply"]});
            if let Some(old) = slots.insert(index, record.clone()) {
                assert_eq!(old, record, "conflicting committed slot");
            }
            by_context.insert(v["context"].to_string(), (index, v["reply"].clone()));
        }
        if let Some(id) = v["complete"].as_u64() {
            assert!(invoked.contains(&id));
            let ctx = serde_json::to_value(id.to_be_bytes().to_vec())
                .unwrap()
                .to_string();
            let (index, reply) = by_context
                .get(&ctx)
                .expect("completion without applied decision");
            assert!(*index > last_complete_index, "real time order violation");
            assert_eq!(reply, &v["reply"]);
            last_complete_index = *index;
            count += 1;
        }
    }
    std::fs::write(root.join("history-checker.json"),serde_json::to_vec_pretty(&json!({"pass":true,"completed":count,"observed_committed_slots":slots.len(),"checks":["no conflicting observed committed slots","completion has applied decision","sequential real-time order","replica response equality"],"limits":"sequential observed histories only; not proof or concurrent general serializability"})).unwrap()).unwrap();
}
