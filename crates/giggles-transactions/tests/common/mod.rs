#![allow(dead_code)]
use giggles_transactions::*;
use std::collections::BTreeMap;

pub fn domain(range: u64) -> Domain {
    Domain {
        database: DatabaseId([42; 16]),
        range,
        epoch: 1,
    }
}
pub fn transaction(nonce: u128) -> TransactionId {
    TransactionId {
        home: domain(1),
        nonce,
    }
}
pub fn digest(n: u128) -> Digest {
    operation_digest(&n.to_le_bytes())
}
pub fn point(key: &[u8], mode: Mode) -> Lock {
    Lock {
        span: Span::Point(key.to_vec()),
        mode,
    }
}
pub fn interval(start: &[u8], end: &[u8], mode: Mode) -> Lock {
    Lock {
        span: Span::Interval {
            start: start.to_vec(),
            end: end.to_vec(),
        },
        mode,
    }
}

#[derive(Clone)]
pub struct DurableReplica {
    pub machine: Machine,
    pub image: BTreeMap<Vec<u8>, Vec<u8>>,
}

/// Only durably persisted machines are exposed through this test adapter.
#[derive(Clone)]
pub struct View(pub BTreeMap<Domain, DurableReplica>);
impl CommittedView for View {
    fn seal(&self, r: &SealRef) -> Option<SealEvidence> {
        self.0.get(&r.transaction.home)?.machine.seal_evidence(r)
    }
    fn prepare(&self, r: &PrepareRef) -> Option<PrepareEvidence> {
        self.0.get(&r.participant)?.machine.prepare_evidence(r)
    }
    fn decision(&self, r: &DecisionRef) -> Option<DecisionEvidence> {
        self.0
            .get(&r.transaction.home)?
            .machine
            .decision_evidence(r)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Crash {
    None,
    BeforeApply,
    BeforePersist,
    AfterPersist,
}

#[derive(Clone)]
pub struct Harness {
    pub durable: View,
}
impl Harness {
    pub fn new(ranges: &[u64]) -> Self {
        Self::with_limits(ranges, Limits::default())
    }
    pub fn with_limits(ranges: &[u64], limits: Limits) -> Self {
        Self {
            durable: View(
                ranges
                    .iter()
                    .map(|&r| {
                        (
                            domain(r),
                            DurableReplica {
                                machine: Machine::new(domain(r), limits.clone()).unwrap(),
                                image: BTreeMap::new(),
                            },
                        )
                    })
                    .collect(),
            ),
        }
    }
    pub fn machine(&self, range: u64) -> &Machine {
        &self.durable.0[&domain(range)].machine
    }
    pub fn image(&self, range: u64) -> &BTreeMap<Vec<u8>, Vec<u8>> {
        &self.durable.0[&domain(range)].image
    }

    /// Simulates the atomic adapter transaction: snapshot + applied index + redo.
    /// A crash cannot expose the tentative machine or any subset of its effects.
    pub fn submit(
        &mut self,
        range: u64,
        command: Command,
        crash: Crash,
    ) -> Option<Result<Response, Error>> {
        if crash == Crash::BeforeApply {
            self.restart();
            return None;
        }
        let d = domain(range);
        let mut candidate = self.durable.0[&d].clone();
        let index = candidate.machine.applied_index() + 1;
        let result = candidate
            .machine
            .apply_committed(index, command, &self.durable);
        if crash == Crash::BeforePersist {
            self.restart();
            return None;
        }
        if let Ok(Response::Finalized { intents, .. }) = &result {
            for intent in intents {
                if let Some(v) = &intent.value {
                    candidate.image.insert(intent.key.clone(), v.clone());
                } else {
                    candidate.image.remove(&intent.key);
                }
            }
        }
        self.durable.0.insert(d, candidate);
        self.restart();
        if crash == Crash::AfterPersist {
            None
        } else {
            Some(result)
        }
    }
    pub fn apply(&mut self, range: u64, command: Command) -> Result<Response, Error> {
        self.submit(range, command, Crash::None).unwrap()
    }
    pub fn ok(&mut self, range: u64, command: Command) -> Response {
        self.apply(range, command).unwrap()
    }
    pub fn restart(&mut self) {
        for (d, replica) in &mut self.durable.0 {
            let bytes = replica.machine.snapshot().unwrap();
            replica.machine =
                Machine::restore(*d, replica.machine.limits().clone(), &bytes).unwrap();
            assert_eq!(bytes, replica.machine.snapshot().unwrap());
        }
    }
    pub fn seal(&mut self, t: TransactionId, ranges: &[u64]) -> SealRef {
        self.ok(
            1,
            Command::OpenHome {
                transaction: t,
                digest: digest(t.nonce),
            },
        );
        for &r in ranges {
            self.ok(
                1,
                Command::Register {
                    transaction: t,
                    digest: digest(t.nonce),
                    participant: domain(r),
                },
            );
        }
        match self.ok(
            1,
            Command::Seal {
                transaction: t,
                digest: digest(t.nonce),
            },
        ) {
            Response::Seal(s) => s,
            _ => panic!("seal response"),
        }
    }
    pub fn begin(&mut self, t: TransactionId, ranges: &[u64]) -> SealRef {
        let s = self.seal(t, ranges);
        for &r in ranges {
            self.ok(r, Command::Begin(s.clone()));
        }
        s
    }
    pub fn prepare(&mut self, t: TransactionId, range: u64) -> PrepareRef {
        match self.ok(range, Command::Prepare { transaction: t }) {
            Response::Prepared(p) => p,
            _ => panic!("prepare response"),
        }
    }
    pub fn vote(&mut self, t: TransactionId, range: u64) -> PrepareRef {
        let p = self.prepare(t, range);
        self.ok(1, Command::RecordPrepare(p.clone()));
        p
    }
    pub fn decide(&mut self, t: TransactionId, outcome: Outcome) -> DecisionRef {
        match self.ok(
            1,
            Command::Decide {
                transaction: t,
                digest: digest(t.nonce),
                outcome,
            },
        ) {
            Response::Decision(d) => d,
            _ => panic!("decision response"),
        }
    }
    pub fn acquire(&mut self, range: u64, t: TransactionId, lock: Lock) -> Result<Response, Error> {
        self.apply(
            range,
            Command::Acquire {
                transaction: t,
                lock,
            },
        )
    }
    pub fn stage(&mut self, range: u64, t: TransactionId, key: &[u8], value: Option<&[u8]>) {
        self.ok(
            range,
            Command::Stage {
                transaction: t,
                intent: Intent {
                    key: key.to_vec(),
                    value: value.map(<[u8]>::to_vec),
                },
            },
        );
    }
}

pub struct SeedGuard {
    seed: u64,
}
impl SeedGuard {
    pub fn new(seed: u64) -> Self {
        Self { seed }
    }
}
impl Drop for SeedGuard {
    fn drop(&mut self) {
        if std::thread::panicking() {
            let unique = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("artifacts")
                .join(format!("failed-seed-{}-{unique}", self.seed));
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("seed.txt"), self.seed.to_string()).unwrap();
        }
    }
}
