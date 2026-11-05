//! Reference membership-only adapter. No user database, transport or authentication.
//! All errors from `drive` fence the actor: drop it and reopen durable storage.
#[cfg(test)]
#[path = "durable_tests.rs"]
mod tests;
use crate::*;
use protobuf::Message as Pb;
use raft::{
    Config, GetEntriesContext, RaftState, RawNode, StateRole, Storage, StorageError, eraftpb::*,
    storage::MemStorage,
};
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex},
};

const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("membership-v1");
pub const MAX_ENTRY: usize = 4096;
pub const MAX_LOG: usize = 4096;
pub const MAX_SNAPSHOT: usize = 2 * 1024 * 1024;
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Image {
    membership: Membership,
    applied: u64,
    term: u64,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Record {
    node: u64,
    image: Image,
    conf: Vec<u8>,
    hard: Vec<u8>,
    snapshot: Vec<u8>,
    log: BTreeMap<u64, Vec<u8>>,
}
pub(crate) fn expected_conf(m: &Membership) -> Result<ConfState> {
    let mut c = ConfState {
        voters: m.voters.clone(),
        ..Default::default()
    };
    if m.active.is_some() {
        let p = m.progress()?;
        match p.phase {
            Phase::Planned => {}
            Phase::CatchingUp => c.learners.push(p.intent.learner),
            Phase::Joint => {
                c.voters_outgoing = c.voters.clone();
                c.voters.retain(|id| *id != p.intent.remove);
                c.voters.push(p.intent.learner);
                c.voters.sort_unstable();
            }
            Phase::Complete => return Err("completed active intent".into()),
        }
    }
    Ok(c)
}
pub(crate) fn equal_conf(a: &ConfState, b: &ConfState) -> bool {
    let sorted = |v: &[u64]| {
        let mut v = v.to_vec();
        v.sort_unstable();
        v
    };
    sorted(&a.voters) == sorted(&b.voters)
        && sorted(&a.learners) == sorted(&b.learners)
        && sorted(&a.voters_outgoing) == sorted(&b.voters_outgoing)
        && sorted(&a.learners_next) == sorted(&b.learners_next)
        && a.auto_leave == b.auto_leave
}
impl Record {
    fn validate(&self) -> Result<()> {
        let c = ConfState::parse_from_bytes(&self.conf)?;
        let m = &self.image.membership;
        m.validate(self.image.applied)?;
        if m.receipts.len() > m.capacity || !equal_conf(&c, &expected_conf(m)?) {
            return Err("membership/ConfState invariant".into());
        }
        let h = HardState::parse_from_bytes(&self.hard)?;
        let snap = Snapshot::parse_from_bytes(&self.snapshot)?;
        let base = snap.get_metadata().index;
        let mut last = base;
        for (index, bytes) in &self.log {
            let e = Entry::parse_from_bytes(bytes)?;
            if *index != last.checked_add(1).ok_or("log overflow")?
                || e.index != *index
                || e.term == 0
                || e.data.len() > MAX_ENTRY
            {
                return Err("log continuity/bounds".into());
            }
            last = *index;
        }
        if last >= u64::MAX - 1
            || self.log.len() > MAX_LOG
            || self.image.applied < base
            || self.image.applied > h.commit
            || h.commit > last
        {
            return Err("applied/commit/log invariant".into());
        }
        if base > 0 {
            let image = validate_snapshot(&snap, m.identity.group)?;
            if base == self.image.applied && image != self.image {
                return Err("snapshot/application disagreement".into());
            }
        }
        let applied_term = if self.image.applied == base {
            snap.get_metadata().term
        } else {
            Entry::parse_from_bytes(
                self.log
                    .get(&self.image.applied)
                    .ok_or("missing applied entry")?,
            )?
            .term
        };
        if self.image.term != applied_term
            || (self.image.applied > 0 && (self.image.term == 0 || self.image.term > h.term))
        {
            return Err("applied term invariant".into());
        }
        Ok(())
    }
    fn memory(&self) -> Result<MemStorage> {
        let c = ConfState::parse_from_bytes(&self.conf)?;
        let mem = MemStorage::new_with_conf_state(c.clone());
        let mut w = mem.wl();
        let snap = Snapshot::parse_from_bytes(&self.snapshot)?;
        if !snap.is_empty() {
            w.apply_snapshot(snap)?;
        }
        let entries = self
            .log
            .values()
            .map(|e| Entry::parse_from_bytes(e))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        w.append(&entries)?;
        w.set_hardstate(HardState::parse_from_bytes(&self.hard)?);
        w.set_conf_state(c);
        drop(w);
        Ok(mem)
    }
    fn snapshot(&self) -> Result<Snapshot> {
        let mut s = Snapshot::default();
        s.mut_metadata().index = self.image.applied;
        s.mut_metadata().term = self.image.term;
        s.mut_metadata()
            .set_conf_state(ConfState::parse_from_bytes(&self.conf)?);
        s.data = serde_json::to_vec(&self.image)?.into();
        if s.compute_size() as usize > MAX_SNAPSHOT {
            return Err("snapshot bound".into());
        }
        Ok(s)
    }
}
fn validate_snapshot(s: &Snapshot, group: u64) -> Result<Image> {
    if s.compute_size() as usize > MAX_SNAPSHOT {
        return Err("snapshot bound".into());
    }
    let image: Image = serde_json::from_slice(&s.data)?;
    image.membership.validate(image.applied)?;
    if image.membership.identity.group != group
        || image.applied != s.get_metadata().index
        || image.term != s.get_metadata().term
        || image.applied == 0
        || image.term == 0
        || !equal_conf(
            s.get_metadata().get_conf_state(),
            &expected_conf(&image.membership)?,
        )
    {
        return Err("snapshot binding".into());
    }
    Ok(image)
}
/// Trusted fault injection. Process exits are real subprocess exits, not power loss.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    BeforeCommit,
    AfterCommit,
    ExitBeforeCommit,
    ExitAfterCommit,
}
struct Inner {
    db: Database,
    record: Record,
    fenced: bool,
    fault: Option<Fault>,
}
#[derive(Clone)]
pub struct Store(Arc<Mutex<Inner>>);
impl Store {
    /// Existing files must match node/group; membership is recovered, never reset.
    pub fn open(path: impl AsRef<Path>, node: u64, initial: Membership) -> Result<Self> {
        if node == 0 {
            return Err("zero node".into());
        }
        let db = Database::create(path)?;
        let mut tx = db.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        let record;
        {
            let mut t = tx.open_table(TABLE)?;
            let bytes = t.get(0)?.map(|v| v.value().to_vec());
            record = if let Some(bytes) = bytes {
                serde_json::from_slice::<Record>(&bytes)?
            } else {
                Record {
                    node,
                    conf: expected_conf(&initial)?.write_to_bytes()?,
                    hard: HardState::default().write_to_bytes()?,
                    snapshot: Snapshot::default().write_to_bytes()?,
                    log: BTreeMap::new(),
                    image: Image {
                        membership: initial.clone(),
                        applied: 0,
                        term: 0,
                    },
                }
            };
            if record.node != node
                || record.image.membership.identity.group != initial.identity.group
            {
                return Err("store node/group binding".into());
            }
            record.validate()?;
            let bytes = serde_json::to_vec(&record)?;
            t.insert(0, bytes.as_slice())?;
        }
        tx.commit()?;
        Ok(Self(Arc::new(Mutex::new(Inner {
            db,
            record,
            fenced: false,
            fault: None,
        }))))
    }
    fn record(&self) -> Result<Record> {
        Ok(self.0.lock().map_err(|_| "poison")?.record.clone())
    }
    fn update<T>(&self, f: impl FnOnce(&mut Record) -> Result<T>) -> Result<T> {
        let mut g = self.0.lock().map_err(|_| "poison")?;
        if g.fenced {
            return Err("store fenced; reopen".into());
        }
        let mut next = g.record.clone();
        let result = f(&mut next)?;
        next.validate()?;
        let fault = if next.image.applied > g.record.image.applied {
            g.fault.take()
        } else {
            None
        };
        let save = (|| -> Result<()> {
            let mut tx = g.db.begin_write()?;
            tx.set_durability(Durability::Immediate)?;
            {
                let mut t = tx.open_table(TABLE)?;
                let bytes = serde_json::to_vec(&next)?;
                t.insert(0, bytes.as_slice())?;
            }
            if fault == Some(Fault::ExitBeforeCommit) {
                std::process::exit(86);
            }
            if fault == Some(Fault::BeforeCommit) {
                return Err("injected before commit".into());
            }
            tx.commit()?;
            if fault == Some(Fault::ExitAfterCommit) {
                std::process::exit(87);
            }
            if fault == Some(Fault::AfterCommit) {
                return Err("injected ambiguous commit".into());
            }
            Ok(())
        })();
        if let Err(e) = save {
            g.fenced = true;
            return Err(e);
        }
        g.record = next;
        Ok(result)
    }
    pub fn membership(&self) -> Result<Membership> {
        Ok(self.record()?.image.membership)
    }
    pub fn applied(&self) -> Result<u64> {
        Ok(self.record()?.image.applied)
    }
    pub fn conf_state(&self) -> Result<ConfState> {
        Ok(ConfState::parse_from_bytes(&self.record()?.conf)?)
    }
    pub fn inject(&self, fault: Fault) -> Result<()> {
        self.0.lock().map_err(|_| "poison")?.fault = Some(fault);
        Ok(())
    }
    /// Compact only durably applied entries; snapshot carries controller + ConfState.
    pub fn compact(&self) -> Result<()> {
        self.update(|r| {
            if r.image.applied == 0 {
                return Err("nothing applied".into());
            }
            r.snapshot = r.snapshot()?.write_to_bytes()?;
            r.log.retain(|i, _| *i > r.image.applied);
            Ok(())
        })
    }
    pub fn durable_bytes(&self) -> Result<Vec<u8>> {
        let g = self.0.lock().map_err(|_| "poison")?;
        let tx = g.db.begin_read()?;
        let t = tx.open_table(TABLE)?;
        Ok(t.get(0)?.ok_or("record")?.value().to_vec())
    }
    fn memory(&self) -> raft::Result<MemStorage> {
        self.record()
            .and_then(|r| r.memory())
            .map_err(|e| StorageError::Other(e).into())
    }
    /// Hash the last durable record, including after an ambiguous commit.
    pub fn durable_hash(&self) -> Result<[u8; 32]> {
        Ok(Sha256::digest(self.durable_bytes()?).into())
    }
}
impl Storage for Store {
    fn initial_state(&self) -> raft::Result<RaftState> {
        self.memory()?.initial_state()
    }
    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        ctx: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        self.memory()?.entries(low, high, max_size, ctx)
    }
    fn term(&self, idx: u64) -> raft::Result<u64> {
        self.memory()?.term(idx)
    }
    fn first_index(&self) -> raft::Result<u64> {
        self.memory()?.first_index()
    }
    fn last_index(&self) -> raft::Result<u64> {
        self.memory()?.last_index()
    }
    fn snapshot(&self, request: u64, _to: u64) -> raft::Result<Snapshot> {
        let r = self.record().map_err(StorageError::Other)?;
        if r.image.applied < request || r.image.applied == 0 {
            return Err(StorageError::SnapshotTemporarilyUnavailable.into());
        }
        r.snapshot().map_err(|e| StorageError::Other(e).into())
    }
}
#[derive(Default)]
pub struct Output {
    pub messages: Vec<Message>,
    pub rejected: Vec<(u64, String)>,
}
pub struct Node {
    raw: RawNode<Store>,
    pub store: Store,
    fenced: bool,
}
impl Node {
    pub fn new(store: Store) -> Result<Self> {
        let r = store.record()?;
        let config = Config {
            id: r.node,
            applied: r.image.applied,
            election_tick: 10,
            heartbeat_tick: 2,
            check_quorum: true,
            pre_vote: true,
            max_size_per_msg: 65536,
            max_inflight_msgs: 8,
            max_uncommitted_size: 65536,
            ..Default::default()
        };
        let logger = slog::Logger::root(slog::Discard, slog::o!());
        Ok(Self {
            raw: RawNode::new(&config, store.clone(), &logger)?,
            store,
            fenced: false,
        })
    }
    fn live(&self) -> Result<()> {
        if self.fenced {
            Err("actor fenced; reopen".into())
        } else {
            Ok(())
        }
    }
    fn admission(&self) -> Result<()> {
        self.live()?;
        if !self.is_leader() || self.raw.raft.raft_log.last_index() != self.store.applied()? {
            return Err("leader/applied administrative barrier pending".into());
        }
        if self.store.record()?.log.len() >= MAX_LOG {
            return Err("compact applied log before admission".into());
        }
        Ok(())
    }
    pub fn is_leader(&self) -> bool {
        self.raw.raft.state == StateRole::Leader && !self.fenced
    }
    pub fn term(&self) -> u64 {
        self.raw.raft.term
    }
    pub fn committed(&self) -> u64 {
        self.raw.raft.raft_log.committed
    }
    pub fn campaign(&mut self) -> Result<()> {
        self.live()?;
        self.raw.campaign()?;
        Ok(())
    }
    pub fn tick(&mut self) -> Result<()> {
        self.live()?;
        self.raw
            .raft
            .set_randomized_election_timeout(10 + (self.raw.raft.id % 10) as usize);
        self.raw.tick();
        Ok(())
    }
    /// Caller must authenticate peer + group. Peer roster management belongs to
    /// the integrating owner; this checks binding and excludes local/client msgs.
    pub fn step(&mut self, authenticated_peer: u64, message: Message) -> Result<()> {
        self.live()?;
        if message.from != authenticated_peer
            || authenticated_peer == 0
            || message.to != self.raw.raft.id
            || message.compute_size() as usize > MAX_SNAPSHOT + 65536
            || raft::raw_node::is_local_msg(message.get_msg_type())
            || matches!(
                message.get_msg_type(),
                MessageType::MsgPropose | MessageType::MsgReadIndex
            )
            || message
                .entries
                .iter()
                .any(|e| e.data.len() > MAX_ENTRY || e.context.len() > MAX_ENTRY)
        {
            return Err("message binding/bounds".into());
        }
        if message.has_snapshot() && !message.get_snapshot().is_empty() {
            validate_snapshot(
                message.get_snapshot(),
                self.store.membership()?.identity.group,
            )?;
        }
        match self.raw.step(message) {
            Ok(()) | Err(raft::Error::StepPeerNotFound) => {}
            Err(e) => return Err(e.into()),
        }
        Ok(())
    }
    pub fn propose(&mut self, command: Command) -> Result<()> {
        self.admission()?;
        let mut m = self.store.membership()?;
        if let Operation::Ready(r) = &command.operation {
            let p = m.progress()?;
            let matched = self
                .raw
                .raft
                .prs()
                .get(p.intent.learner)
                .ok_or("no learner progress")?
                .matched;
            if r.term != self.term()
                || r.barrier > self.store.applied()?
                || matched < r.durable_applied
                || r.durable_applied > self.store.applied()?
            {
                return Err("unverified readiness barrier".into());
            }
        }
        m.apply(
            &command,
            self.raw
                .raft
                .raft_log
                .last_index()
                .checked_add(1)
                .ok_or("log index overflow")?,
            self.term(),
        )?;
        let bytes = serde_json::to_vec(&command)?;
        if bytes.len() > MAX_ENTRY {
            return Err("command bound".into());
        }
        self.raw.propose(Vec::new(), bytes)?;
        Ok(())
    }
    pub fn change(&mut self, change: Change) -> Result<()> {
        self.live()?;
        if !self.is_leader() {
            return Err("not leader".into());
        }
        if self.store.membership()?.change_applied(&change)? {
            return Ok(());
        }
        self.admission()?;
        if !self.is_leader() || self.raw.raft.has_pending_conf() {
            return Err("leader/pending configuration".into());
        }
        let m = self.store.membership()?;
        if change.step == Step::EnterJoint
            && m.progress()?.readiness.as_ref().ok_or("not ready")?.term != self.term()
        {
            return Err("readiness from old term".into());
        }
        self.raw
            .propose_conf_change(Vec::new(), m.configuration(&change)?)?;
        Ok(())
    }
    fn apply(&mut self, entries: &[Entry], out: &mut Output) -> Result<()> {
        let store = self.store.clone();
        let original = store.record()?;
        let mut next = original.clone();
        {
            let r = &mut next;
            for e in entries {
                if e.index <= r.image.applied {
                    continue;
                }
                if e.index != r.image.applied + 1 {
                    return Err("apply gap".into());
                }
                match e.get_entry_type() {
                    EntryType::EntryNormal if !e.data.is_empty() => {
                        let command: Command = serde_json::from_slice(&e.data)?;
                        if let Err(error) = r.image.membership.apply(&command, e.index, e.term) {
                            out.rejected.push((e.index, error.to_string()));
                        }
                    }
                    EntryType::EntryNormal => {}
                    EntryType::EntryConfChangeV2 => {
                        let cc = ConfChangeV2::parse_from_bytes(&e.data)?;
                        let change: Change = serde_json::from_slice(&cc.context)?;
                        if r.image.membership.configuration(&change)? != cc {
                            return Err("configuration payload mismatch".into());
                        }
                        let conf = self.raw.apply_conf_change(&cc)?;
                        r.image
                            .membership
                            .applied_configuration(&change, e.index, &conf)?;
                        r.conf = conf.write_to_bytes()?;
                    }
                    _ => return Err("v1 configuration unsupported".into()),
                }
                r.image.applied = e.index;
                r.image.term = e.term;
            }
        }
        if next == original {
            return Ok(());
        }
        store.update(|r| {
            if *r != original {
                return Err("concurrent store modification".into());
            }
            *r = next;
            Ok(())
        })
    }
    pub fn drive(&mut self) -> Result<Output> {
        self.live()?;
        let result = self.drive_inner();
        if result.is_err() {
            self.fenced = true;
        }
        result
    }
    fn drive_inner(&mut self) -> Result<Output> {
        let mut out = Output::default();
        while self.raw.has_ready() {
            let mut ready = self.raw.ready();
            self.store.update(|r| {
                if !ready.snapshot().is_empty() {
                    let s = ready.snapshot();
                    let image = validate_snapshot(s, r.image.membership.identity.group)?;
                    if image.applied < r.image.applied {
                        return Err("stale snapshot".into());
                    }
                    if image.membership.identity.owner_epoch
                        < r.image.membership.identity.owner_epoch
                        || image.membership.epoch < r.image.membership.epoch
                    {
                        return Err("snapshot epoch regression".into());
                    }
                    r.image = image;
                    r.conf = s.get_metadata().get_conf_state().write_to_bytes()?;
                    r.snapshot = s.write_to_bytes()?;
                    r.log.clear();
                }
                for e in ready.entries() {
                    r.log.retain(|i, _| *i < e.index);
                    r.log.insert(e.index, e.write_to_bytes()?);
                }
                if let Some(h) = ready.hs() {
                    r.hard = h.write_to_bytes()?;
                }
                Ok(())
            })?;
            self.apply(ready.committed_entries(), &mut out)?;
            out.messages.extend(ready.take_messages());
            out.messages.extend(ready.take_persisted_messages());
            let mut light = self.raw.advance(ready);
            if let Some(commit) = light.commit_index() {
                self.store.update(|r| {
                    let mut h = HardState::parse_from_bytes(&r.hard)?;
                    h.commit = commit;
                    r.hard = h.write_to_bytes()?;
                    Ok(())
                })?;
            }
            self.apply(light.committed_entries(), &mut out)?;
            out.messages.extend(light.take_messages());
            self.raw.advance_apply();
            // raft 0.7 post_conf_change explicitly leaves removed leaders in
            // Leader role (TODO upstream). Stop their heartbeats after the
            // durable leave barrier; survivor election proceeds normally.
            if self.raw.raft.state == StateRole::Leader && !self.raw.raft.promotable() {
                self.raw.raft.become_follower(self.raw.raft.term, 0);
            }
        }
        Ok(out)
    }
}
