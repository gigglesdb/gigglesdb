//! Bounded typed transactions over the actual range state machines.
//!
//! Owner controls are trusted. A `Pinned` value MUST originate in an authenticated
//! owner session's quorum-checked `tx_pin` response. It is NOT a signature or a
//! quorum certificate. Peer listeners cannot submit owner commands. Replicated
//! apply only restores these already pinned views; it never contacts a network.
//!
//! Engine positions are `(raft_index * 256 + subcommand)`. This permits one
//! atomic range transition to register/seal or acquire/stage its bounded local
//! work while retaining an unambiguous binding to the durable Raft entry.
use crate::{Bounds, Data, Descriptor, LIMIT, Result, runtime::Observation};
use giggles_core::{Key, Row, local::AdminCommand, replica};
use giggles_transactions as tx;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const MAX_TRANSACTIONS: usize = 16;
pub const MAX_PARTICIPANTS: usize = 8;
pub const MAX_ITEMS: usize = 8;

fn limits() -> tx::Limits {
    tx::Limits {
        transactions: MAX_TRANSACTIONS,
        participants: MAX_PARTICIPANTS,
        locks_per_transaction: 32,
        intents_per_transaction: MAX_ITEMS,
        value_bytes: 512,
        encoded_bytes: 64 * 1024,
        ..Default::default()
    }
}

pub fn domain(database: u128, route: &Descriptor) -> tx::Domain {
    tx::Domain {
        database: tx::DatabaseId(database.to_be_bytes()),
        range: route.range,
        epoch: route.epoch,
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Write {
    pub key: Key,
    /// Exact before-image, including version. None means an absent-key CAS.
    pub before: Option<Row>,
    pub row: Row,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Predicate {
    pub bounds: Bounds,
    /// Optional exact complete observation precondition, in ascending key order.
    pub expected: Option<Vec<(Key, Row)>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Participant {
    pub route: Descriptor,
    pub writes: Vec<Write>,
    pub reads: Vec<Predicate>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    pub database: u128,
    pub nonce: u128,
    pub home: Descriptor,
    /// Canonical ascending group order; the immutable, complete participant set.
    pub participants: Vec<Participant>,
}

impl Plan {
    pub fn id(&self) -> tx::TransactionId {
        tx::TransactionId {
            home: domain(self.database, &self.home),
            nonce: self.nonce,
        }
    }
    pub fn digest(&self) -> Result<tx::Digest> {
        Ok(tx::operation_digest(&bincode::serialize(self)?))
    }
    pub fn validate(&self) -> Result<()> {
        self.home.validate()?;
        if self.database == 0
            || self.nonce == 0
            || self.participants.is_empty()
            || self.participants.len() > MAX_PARTICIPANTS
            || bincode::serialized_size(self)? > 4 * 1024
        {
            return Err("transaction plan limit/identity".into());
        }
        let mut last = 0;
        let mut ranges = BTreeSet::new();
        for p in &self.participants {
            p.route.validate()?;
            if p.route.group <= last
                || !ranges.insert(p.route.range)
                || p.writes.len() + p.reads.len() == 0
                || p.writes.len() + p.reads.len() > MAX_ITEMS
            {
                return Err("transaction participants/order/limit".into());
            }
            last = p.route.group;
            let mut keys = BTreeSet::new();
            for w in &p.writes {
                if !p.route.bounds.contains(w.key) || !keys.insert(w.key) {
                    return Err("write bounds/duplicate".into());
                }
            }
            for r in &p.reads {
                if !p.route.bounds.contains_bounds(&r.bounds)
                    || r.expected.as_ref().is_some_and(|rows| {
                        rows.len() > LIMIT
                            || rows.windows(2).any(|w| w[0].0 >= w[1].0)
                            || rows.iter().any(|(k, _)| !r.bounds.contains(*k))
                    })
                {
                    return Err("predicate bounds/canonical observation".into());
                }
            }
        }
        if !self.participants.iter().any(|p| p.route == self.home) {
            return Err("home must participate".into());
        }
        for a in &self.participants {
            for b in &self.participants {
                if a.route.group != b.route.group
                    && a.route.bounds.end.is_none_or(|e| b.route.bounds.start < e)
                    && b.route.bounds.end.is_none_or(|e| a.route.bounds.start < e)
                {
                    return Err("participant overlap".into());
                }
            }
        }
        Ok(())
    }
    fn participant(&self, route: &Descriptor) -> Result<&Participant> {
        self.participants
            .iter()
            .find(|p| p.route == *route)
            .ok_or_else(|| "participant mismatch".into())
    }
}

/// Complete observations are retained at each participant and copied into the
/// home on votes. They are released only after a durable commit decision.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rows {
    pub group: u64,
    pub predicates: Vec<Vec<(Key, Row)>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    pub plan: Plan,
    pub seal: Option<tx::SealRef>,
    pub locked: bool,
    pub rows: Rows,
    pub votes: Vec<Rows>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct State {
    /// Hex encoding keeps owner JSON bounded without arrays of byte integers.
    snapshot: String,
    pub records: Vec<Record>,
}

impl State {
    pub fn new(database: u128, route: &Descriptor) -> Result<Self> {
        Ok(Self {
            snapshot: hex::encode(tx::Machine::new(domain(database, route), limits())?.snapshot()?),
            records: Vec::new(),
        })
    }
    pub fn engine(&self, database: u128, route: &Descriptor) -> Result<tx::Machine> {
        Ok(tx::Machine::restore(
            domain(database, route),
            limits(),
            &hex::decode(&self.snapshot)?,
        )?)
    }
    pub fn unresolved(&self, database: u128, route: &Descriptor) -> Result<bool> {
        Ok(self.engine(database, route)?.has_unresolved_work())
    }
    pub fn record(&self, id: tx::TransactionId) -> Result<&Record> {
        self.records
            .iter()
            .find(|r| r.plan.id() == id)
            .ok_or_else(|| "unknown transaction".into())
    }
    fn record_mut(&mut self, id: tx::TransactionId) -> Result<&mut Record> {
        self.records
            .iter_mut()
            .find(|r| r.plan.id() == id)
            .ok_or_else(|| "unknown transaction".into())
    }
    pub fn validate(&self, database: u128, route: &Descriptor) -> Result<()> {
        let engine = self.engine(database, route)?;
        if self.records.len() > MAX_TRANSACTIONS {
            return Err("transaction records capacity".into());
        }
        let mut ids = BTreeSet::new();
        for r in &self.records {
            r.plan.validate()?;
            r.plan.participant(route)?;
            if r.plan.database != database
                || !ids.insert(r.plan.id())
                || r.rows.group != route.group
                || r.rows.predicates.len() > MAX_ITEMS
                || r.votes.len() > MAX_PARTICIPANTS
                || r.rows.predicates.iter().any(|rows| rows.len() > LIMIT)
            {
                return Err("invalid transaction record".into());
            }
            if let Some(s) = &r.seal
                && (s.transaction != r.plan.id() || s.digest != r.plan.digest()?)
            {
                return Err("seal binding".into());
            }
            if r.locked
                && matches!(
                    engine.phase(r.plan.id()),
                    Some(tx::Phase::Active | tx::Phase::Prepared)
                )
            {
                for l in locks(r.plan.participant(route)?) {
                    if !engine.holds(r.plan.id(), &l.span, l.mode) {
                        return Err("missing retained lock".into());
                    }
                }
            }
        }
        Ok(())
    }
}

/// A locator plus committed state pinned at an owner-controlled read barrier.
/// Deserialization does not authenticate this value. Only trusted owner control
/// may supply it; untrusted API users must never get access to this surface.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pinned {
    pub database: u128,
    pub route: Descriptor,
    pub applied: u64,
    pub state: State,
}
impl Pinned {
    /// Retain the one operation's adapter record while keeping the complete
    /// authenticated engine snapshot. Bounds evidence size during resolution.
    pub fn for_transaction(mut self, id: tx::TransactionId) -> Self {
        self.state.records.retain(|r| r.plan.id() == id);
        self
    }
    pub fn from_current(o: Observation) -> Result<Self> {
        if !o.quorum_checked || o.applied == 0 {
            return Err("pin requires committed current barrier".into());
        }
        let crate::Machine::Range { domain, data } = o.machine else {
            return Err("pin needs data group".into());
        };
        let p = Self {
            database: domain,
            route: data.descriptor,
            applied: o.applied,
            state: data.transactions,
        };
        p.validate()?;
        Ok(p)
    }
    fn validate(&self) -> Result<()> {
        self.state.validate(self.database, &self.route)?;
        if self.applied == 0
            || self
                .state
                .engine(self.database, &self.route)?
                .applied_index()
                / 256
                > self.applied
        {
            return Err("pinned applied index binding".into());
        }
        Ok(())
    }
}

struct View(Vec<tx::Machine>);
impl tx::CommittedView for View {
    fn seal(&self, r: &tx::SealRef) -> Option<tx::SealEvidence> {
        self.0.iter().find_map(|m| m.seal_evidence(r))
    }
    fn prepare(&self, r: &tx::PrepareRef) -> Option<tx::PrepareEvidence> {
        self.0.iter().find_map(|m| m.prepare_evidence(r))
    }
    fn decision(&self, r: &tx::DecisionRef) -> Option<tx::DecisionEvidence> {
        self.0.iter().find_map(|m| m.decision_evidence(r))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Step {
    Open(Plan),
    Begin {
        id: tx::TransactionId,
    },
    Lock {
        id: tx::TransactionId,
    },
    Prepare {
        id: tx::TransactionId,
    },
    Vote {
        id: tx::TransactionId,
        participant: Descriptor,
    },
    Decide {
        id: tx::TransactionId,
        outcome: tx::Outcome,
    },
    Finalize {
        id: tx::TransactionId,
    },
}
impl Step {
    pub fn id(&self) -> tx::TransactionId {
        match self {
            Self::Open(p) => p.id(),
            Self::Begin { id }
            | Self::Lock { id }
            | Self::Prepare { id }
            | Self::Vote { id, .. }
            | Self::Decide { id, .. }
            | Self::Finalize { id } => *id,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Request {
    pub route: Descriptor,
    pub step: Step,
    /// Supplied only by trusted owner after obtaining `tx_pin` from real leaders.
    pub evidence: Vec<Pinned>,
}

fn bytes(k: Key) -> Vec<u8> {
    let mut b = vec![0];
    b.extend(k.tenant.0.to_be_bytes());
    b.extend(k.row.0.to_be_bytes());
    b
}
fn locks(p: &Participant) -> Vec<tx::Lock> {
    let mut locks = Vec::new();
    for w in &p.writes {
        locks.push(tx::Lock {
            span: tx::Span::Point(bytes(w.key)),
            mode: tx::Mode::Exclusive,
        });
    }
    for r in &p.reads {
        locks.push(tx::Lock {
            span: tx::Span::Interval {
                start: bytes(r.bounds.start),
                end: r.bounds.end.map(bytes).unwrap_or_else(|| vec![1]),
            },
            mode: tx::Mode::Shared,
        });
    }
    locks.sort();
    locks.dedup();
    locks
}

struct Apply<'a> {
    engine: tx::Machine,
    view: &'a View,
    position: u64,
    end: u64,
}
impl Apply<'_> {
    fn run(&mut self, c: tx::Command) -> Result<tx::Response> {
        self.position = self
            .position
            .checked_add(1)
            .ok_or("transaction index exhausted")?;
        if self.position >= self.end {
            return Err("transaction subcommand limit".into());
        }
        Ok(self.engine.apply_committed(self.position, c, self.view)?)
    }
}

fn pinned<'a>(evidence: &'a [Pinned], database: u128, route: &Descriptor) -> Result<&'a Pinned> {
    evidence
        .iter()
        .find(|p| p.database == database && p.route == *route)
        .ok_or_else(|| "missing pinned committed evidence".into())
}

/// Called on a tentative range clone inside the existing atomic redb apply.
pub(crate) fn apply(
    data: &mut Data,
    database: u128,
    index: u64,
    request: &Request,
) -> Result<serde_json::Value> {
    if request.route != data.descriptor || request.evidence.len() > MAX_PARTICIPANTS {
        return Err("transaction route/evidence limit".into());
    }
    let id = request.step.id();
    if id.home.database != tx::DatabaseId(database.to_be_bytes()) {
        return Err("transaction database mismatch".into());
    }
    let mut view = View(Vec::new());
    let mut seen = BTreeSet::new();
    for p in &request.evidence {
        p.validate()?;
        if p.database != database || !seen.insert(p.route.group) {
            return Err("pinned domain/duplicate".into());
        }
        view.0.push(p.state.engine(database, &p.route)?);
    }
    let base = index
        .checked_mul(256)
        .ok_or("transaction index exhausted")?;
    let mut a = Apply {
        engine: data.transactions.engine(database, &data.descriptor)?,
        view: &view,
        position: base,
        end: base.checked_add(256).ok_or("transaction index exhausted")?,
    };
    let response = match &request.step {
        Step::Open(plan) => {
            plan.validate()?;
            if plan.database != database || plan.home != data.descriptor {
                return Err("wrong transaction home".into());
            }
            if let Ok(old) = data.transactions.record(id) {
                if old.plan != *plan {
                    return Err("conflicting transaction body".into());
                }
                return status(data, database, id);
            }
            // A terminal operation's immutable identity remains replayable on
            // its retained home after a split. Fences still reject every new ID.
            data.check(&request.route, &request.route.bounds)?;
            if data.transactions.records.len() >= MAX_TRANSACTIONS {
                return Err("transaction retention capacity".into());
            }
            a.run(tx::Command::OpenHome {
                transaction: id,
                digest: plan.digest()?,
            })?;
            for p in &plan.participants {
                a.run(tx::Command::Register {
                    transaction: id,
                    digest: plan.digest()?,
                    participant: domain(database, &p.route),
                })?;
            }
            let tx::Response::Seal(seal) = a.run(tx::Command::Seal {
                transaction: id,
                digest: plan.digest()?,
            })?
            else {
                return Err("expected seal".into());
            };
            data.transactions.records.push(Record {
                plan: plan.clone(),
                seal: Some(seal),
                locked: false,
                rows: Rows {
                    group: data.descriptor.group,
                    predicates: Vec::new(),
                },
                votes: Vec::new(),
            });
            tx::Response::Applied
        }
        Step::Begin { .. } => {
            data.check(&request.route, &request.route.bounds)?;
            let home = request
                .evidence
                .iter()
                .find(|p| domain(database, &p.route) == id.home)
                .ok_or("missing home seal")?;
            let record = home.state.record(id)?;
            record.plan.participant(&data.descriptor)?;
            if let Ok(old) = data.transactions.record(id) {
                if old.plan != record.plan {
                    return Err("plan mismatch".into());
                }
            } else {
                if data.transactions.records.len() >= MAX_TRANSACTIONS {
                    return Err("transaction retention capacity".into());
                }
                data.transactions.records.push(Record {
                    plan: record.plan.clone(),
                    seal: record.seal.clone(),
                    locked: false,
                    rows: Rows {
                        group: data.descriptor.group,
                        predicates: Vec::new(),
                    },
                    votes: Vec::new(),
                });
            }
            a.run(tx::Command::Begin(
                record.seal.clone().ok_or("unsealed home")?,
            ))?
        }
        Step::Lock { .. } => {
            data.check(&request.route, &request.route.bounds)?;
            let record = data.transactions.record(id)?.clone();
            if record.locked {
                return status(data, database, id);
            }
            let p = record.plan.participant(&data.descriptor)?;
            for l in locks(p) {
                if a.run(tx::Command::Acquire {
                    transaction: id,
                    lock: l,
                })? != tx::Response::LockGranted
                {
                    return Err("transaction blocked; resolve durable home or abort/retry".into());
                }
            }
            let rows = data.image.snapshot()?.rows;
            // Validate all CAS and complete predicate observations under locks.
            for w in &p.writes {
                if rows.get(&w.key) != w.before.as_ref() {
                    return Err("transaction CAS before-image conflict".into());
                }
            }
            // Reserve row slots for all admitted, unfinalized inserts, so
            // a later finalize cannot fail after unrelated inserts fill a range.
            let mut reserved = rows.keys().copied().collect::<BTreeSet<_>>();
            for other in &data.transactions.records {
                if matches!(
                    a.engine.phase(other.plan.id()),
                    Some(tx::Phase::Active | tx::Phase::Prepared)
                ) {
                    reserved.extend(
                        other
                            .plan
                            .participant(&data.descriptor)?
                            .writes
                            .iter()
                            .map(|w| w.key),
                    );
                }
            }
            if reserved.len() > LIMIT {
                return Err("reserved row capacity".into());
            }
            let mut observed = Vec::new();
            for r in &p.reads {
                let found = rows
                    .iter()
                    .filter(|(k, _)| r.bounds.contains(**k))
                    .map(|(k, v)| (*k, v.clone()))
                    .collect::<Vec<_>>();
                if found.len() > LIMIT || r.expected.as_ref().is_some_and(|want| *want != found) {
                    return Err("complete predicate conflict/limit".into());
                }
                observed.push(found);
            }
            if observed.iter().map(Vec::len).sum::<usize>() > 32 {
                return Err("transaction observation capacity".into());
            }
            // Evaluate the exact prospective core image now. Capacity/version
            // failures occur before prepare, leaving terminal resolution possible.
            let mut image = data.image.clone();
            for w in &p.writes {
                let (next, reply) = image.evaluate(&replica::Command::Admin(
                    serde_json::to_vec(&AdminCommand::Seed {
                        key: w.key,
                        row: w.row.clone(),
                    })?,
                ))?;
                if reply.code != 0 {
                    return Err("transaction core mutation refused".into());
                }
                image = next;
                a.run(tx::Command::Stage {
                    transaction: id,
                    intent: tx::Intent {
                        key: bytes(w.key),
                        value: Some(bincode::serialize(&w.row)?),
                    },
                })?;
            }
            if image.snapshot()?.rows.len() > LIMIT {
                return Err("range row capacity".into());
            }
            // Other locked transactions may finalize before this one. Reserve
            // their remaining core mutations too: per-key locks alone do not
            // protect the shared storage generation or encoded image capacity.
            // This image is only a capacity probe; no foreign redo is published.
            for other in &data.transactions.records {
                if other.plan.id() == id
                    || !other.locked
                    || !matches!(
                        a.engine.phase(other.plan.id()),
                        Some(tx::Phase::Active | tx::Phase::Prepared)
                    )
                {
                    continue;
                }
                for w in &other.plan.participant(&data.descriptor)?.writes {
                    let (next, reply) = image.evaluate(&replica::Command::Admin(
                        serde_json::to_vec(&AdminCommand::Seed {
                            key: w.key,
                            row: w.row.clone(),
                        })?,
                    ))?;
                    if reply.code != 0 {
                        return Err("reserved core mutation capacity".into());
                    }
                    image = next;
                }
            }
            let r = data.transactions.record_mut(id)?;
            r.locked = true;
            r.rows.predicates = observed;
            tx::Response::Applied
        }
        Step::Prepare { .. } => {
            let r = data.transactions.record(id)?;
            if !r.locked {
                return Err("local locks missing".into());
            }
            // Every participant must have acquired all locks before any prepare.
            for p in &r.plan.participants {
                let source = pinned(&request.evidence, database, &p.route)?;
                let other = source.state.record(id)?;
                let m = source.state.engine(database, &p.route)?;
                if other.plan != r.plan
                    || !other.locked
                    || !matches!(m.phase(id), Some(tx::Phase::Active | tx::Phase::Prepared))
                    || locks(p).iter().any(|l| !m.holds(id, &l.span, l.mode))
                {
                    return Err("all participant locks required before prepare".into());
                }
            }
            a.run(tx::Command::Prepare { transaction: id })?
        }
        Step::Vote { participant, .. } => {
            let r = data.transactions.record(id)?.clone();
            if r.plan.home != data.descriptor {
                return Err("vote wrong home".into());
            }
            r.plan.participant(participant)?;
            let source = pinned(&request.evidence, database, participant)?;
            let other = source.state.record(id)?;
            if other.plan != r.plan {
                return Err("vote plan mismatch".into());
            }
            let reference = source
                .state
                .engine(database, participant)?
                .prepared_reference(id)
                .ok_or("no committed prepare")?;
            let response = a.run(tx::Command::RecordPrepare(reference))?;
            let r = data.transactions.record_mut(id)?;
            if let Some(old) = r.votes.iter().find(|v| v.group == participant.group) {
                if *old != other.rows {
                    return Err("vote observation conflict".into());
                }
            } else {
                r.votes.push(other.rows.clone());
                r.votes.sort_by_key(|r| r.group);
            }
            response
        }
        Step::Decide { outcome, .. } => {
            let r = data.transactions.record(id)?;
            if r.plan.home != data.descriptor {
                return Err("decision wrong home".into());
            }
            if *outcome == tx::Outcome::Commit && r.votes.len() != r.plan.participants.len() {
                return Err("missing complete votes".into());
            }
            a.run(tx::Command::Decide {
                transaction: id,
                digest: r.plan.digest()?,
                outcome: *outcome,
            })?
        }
        Step::Finalize { .. } => {
            let r = data.transactions.record(id)?.clone();
            let home = pinned(&request.evidence, database, &r.plan.home)?;
            if home.state.record(id)?.plan != r.plan {
                return Err("decision plan mismatch".into());
            }
            let decision = home
                .state
                .engine(database, &r.plan.home)?
                .outcome(id)
                .cloned()
                .ok_or("home has no durable decision; remain blocked")?;
            // A participant may not have begun when a durable abort was chosen.
            if a.engine.phase(id).is_none() {
                a.run(tx::Command::Begin(r.seal.clone().ok_or("missing seal")?))?;
            }
            let response = a.run(tx::Command::Finalize(decision))?;
            if let tx::Response::Finalized {
                outcome: tx::Outcome::Commit,
                intents,
            } = &response
                && !intents.is_empty()
            {
                let p = r.plan.participant(&data.descriptor)?;
                if intents.len() != p.writes.len() {
                    return Err("redo intent count mismatch".into());
                }
                for w in &p.writes {
                    if !intents.iter().any(|i| {
                        i.key == bytes(w.key)
                            && i.value.as_ref() == bincode::serialize(&w.row).ok().as_ref()
                    }) {
                        return Err("redo intent mismatch".into());
                    }
                    if data.image.snapshot()?.rows.get(&w.key) != w.before.as_ref() {
                        return Err("locked before-image changed".into());
                    }
                    let (next, reply) =
                        data.image
                            .evaluate(&replica::Command::Admin(serde_json::to_vec(
                                &AdminCommand::Seed {
                                    key: w.key,
                                    row: w.row.clone(),
                                },
                            )?))?;
                    if reply.code != 0 {
                        return Err("core redo refused".into());
                    }
                    data.image = next;
                }
            }
            response
        }
    };
    data.transactions.snapshot = hex::encode(a.engine.snapshot()?);
    Ok(serde_json::json!({"response":response}))
}

pub fn status(data: &Data, database: u128, id: tx::TransactionId) -> Result<serde_json::Value> {
    let r = data.transactions.record(id)?;
    let engine = data.transactions.engine(database, &data.descriptor)?;
    let decision = engine.outcome(id);
    let rows = decision
        .filter(|d| d.outcome == tx::Outcome::Commit)
        .map(|_| &r.votes);
    Ok(
        serde_json::json!({"id":id,"digest":r.plan.digest()?,"phase":engine.phase(id),"decision":decision,"rows":rows,"locked":r.locked}),
    )
}
