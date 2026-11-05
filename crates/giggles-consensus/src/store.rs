//! One redb Immediate transaction publishes hard state, log and indexed kernel.
//! Logical log truncation replaces map records; filesystem artifacts are retained.
use crate::{Image, Result, VOTERS};
use bincode::Options;
use protobuf::Message as Pb;
use raft::{GetEntriesContext, RaftState, Storage, StorageError, eraftpb::*};
use redb::{Database, Durability, ReadableDatabase, TableDefinition};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    path::Path,
    sync::{Arc, Mutex},
};

const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("p3_consensus_v1");
const MAX_STORE: u64 = 64 * 1024 * 1024;
fn codec() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(MAX_STORE)
        .reject_trailing_bytes()
}
fn corrupt() -> raft::Error {
    raft::Error::Store(StorageError::Other("invalid durable state".into()))
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct State {
    pub domain: u128,
    pub node: u64,
    pub hard: Vec<u8>,
    pub log: BTreeMap<u64, Vec<u8>>,
    pub snapshot: Vec<u8>,
    pub image: Image,
    pub authority: crate::authority::Authority,
    pub applied: u64,
    pub applied_term: u64,
}
#[derive(Serialize, Deserialize)]
struct SnapshotBody {
    version: u32,
    domain: u128,
    voters: Vec<u64>,
    index: u64,
    term: u64,
    image: Image,
    authority: crate::authority::Authority,
}

fn pack<T: Serialize>(v: &T) -> Result<Vec<u8>> {
    let payload = codec().serialize(v)?;
    if payload.len() as u64 > MAX_STORE - 32 {
        return Err("store limit".into());
    }
    let mut bytes = Sha256::digest(&payload).to_vec();
    bytes.extend(payload);
    Ok(bytes)
}
fn unpack<T: for<'a> Deserialize<'a>>(bytes: &[u8]) -> Result<T> {
    if bytes.len() < 32
        || bytes.len() as u64 > MAX_STORE
        || Sha256::digest(&bytes[32..]).as_slice() != &bytes[..32]
    {
        return Err("checksum/size".into());
    }
    Ok(codec().deserialize(&bytes[32..])?)
}
fn conf() -> ConfState {
    let mut c = ConfState::default();
    c.set_voters(VOTERS.to_vec());
    c
}

impl State {
    pub fn hard(&self) -> Result<HardState> {
        Ok(HardState::parse_from_bytes(&self.hard)?)
    }
    pub fn snap(&self) -> Result<Snapshot> {
        Ok(Snapshot::parse_from_bytes(&self.snapshot)?)
    }
    pub fn snapshot_now(&self) -> Result<Snapshot> {
        let mut s = Snapshot::default();
        s.mut_metadata().index = self.applied;
        s.mut_metadata().term = self.applied_term;
        s.mut_metadata().set_conf_state(conf());
        s.set_data(
            pack(&SnapshotBody {
                version: 2,
                domain: self.domain,
                voters: VOTERS.to_vec(),
                index: self.applied,
                term: self.applied_term,
                image: self.image.clone(),
                authority: self.authority.clone(),
            })?
            .into(),
        );
        Ok(s)
    }
    pub fn validate_snapshot(&self, s: &Snapshot) -> Result<Image> {
        let m = s.get_metadata();
        let b: SnapshotBody = unpack(s.get_data())?;
        if b.version != 2
            || b.domain != self.domain
            || b.voters != VOTERS
            || m.get_conf_state() != &conf()
            || b.index != m.index
            || b.term != m.term
            || b.index == 0
            || b.term == 0
            || b.index < self.applied
            || (b.index == self.applied
                && (b.term != self.applied_term
                    || b.image != self.image
                    || b.authority != self.authority))
        {
            return Err("snapshot identity/membership/index/term".into());
        }
        b.authority.validate(self.domain)?;
        let image = Image::from_bytes(b.image.bytes().to_vec())?;
        if image.domain()? != self.domain {
            return Err("snapshot kernel domain".into());
        }
        // A committed local entry at this index cannot disagree with the snapshot.
        if b.index <= self.hard()?.commit
            && let Some(e) = self.log.get(&b.index)
            && Entry::parse_from_bytes(e)?.term != b.term
        {
            return Err("snapshot conflicts with committed log".into());
        }
        Ok(image)
    }
    pub fn install(&mut self, s: &Snapshot) -> Result<()> {
        let image = self.validate_snapshot(s)?;
        self.image = image;
        let body: SnapshotBody = unpack(s.get_data())?;
        self.authority = body.authority;
        self.applied = s.get_metadata().index;
        self.applied_term = s.get_metadata().term;
        self.snapshot = s.write_to_bytes()?;
        self.log = BTreeMap::new();
        let mut h = self.hard()?;
        h.commit = h.commit.max(self.applied);
        self.hard = h.write_to_bytes()?;
        Ok(())
    }
    fn validate(&self) -> Result<()> {
        if !VOTERS.contains(&self.node) || self.image.domain()? != self.domain {
            return Err("store identity".into());
        }
        Image::from_bytes(self.image.bytes().to_vec())?;
        self.authority.validate(self.domain)?;
        let h = self.hard()?;
        let snap = self.snap()?;
        let base = snap.get_metadata().index;
        if base > 0 {
            self.validate_snapshot_historical(&snap)?;
        }
        let mut last = base;
        for (i, b) in &self.log {
            let e = Entry::parse_from_bytes(b)?;
            if *i != last + 1 || e.index != *i || e.term == 0 {
                return Err("log continuity".into());
            }
            last = *i;
        }
        if self.applied < base
            || self.applied > h.commit
            || h.commit > last
            || (self.applied > 0 && self.applied_term == 0)
        {
            return Err("applied/commit/log invariant".into());
        }
        if self.applied > base
            && Entry::parse_from_bytes(self.log.get(&self.applied).ok_or("missing applied entry")?)?
                .term
                != self.applied_term
        {
            return Err("applied term".into());
        }
        Ok(())
    }
    fn validate_snapshot_historical(&self, s: &Snapshot) -> Result<()> {
        let mut old = self.clone();
        old.applied = 0;
        old.applied_term = 0;
        old.validate_snapshot(s)?;
        Ok(())
    }
}

/// Trusted subprocess fault seam. Never accepted over the network.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplyCrash {
    BeforeCommit,
    AfterCommit,
}
/// Deterministic atomic-storage fault model; no device flush claim.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SaveFault {
    BeforeCommit,
    AfterCommit,
}
struct Inner {
    db: Database,
    state: State,
    fenced: bool,
    crash: Option<ApplyCrash>,
    save_fault: Option<SaveFault>,
}
#[derive(Clone)]
pub struct Store(Arc<Mutex<Inner>>);
impl Store {
    pub fn create(path: impl AsRef<Path>, node: u64, domain: u128) -> Result<Self> {
        if !VOTERS.contains(&node) {
            return Err("unsupported member".into());
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        let db = Database::builder()
            .set_cache_size(2 * 1024 * 1024)
            .create_file(file)?;
        let state = State {
            domain,
            node,
            hard: HardState::default().write_to_bytes()?,
            log: BTreeMap::new(),
            snapshot: Snapshot::default().write_to_bytes()?,
            image: Image::new(domain)?,
            authority: Default::default(),
            applied: 0,
            applied_term: 0,
        };
        let store = Self(Arc::new(Mutex::new(Inner {
            db,
            state: state.clone(),
            fenced: false,
            crash: None,
            save_fault: None,
        })));
        store.update(|s| {
            *s = state;
            Ok(())
        })?;
        Ok(store)
    }
    pub fn open(path: impl AsRef<Path>, node: u64, domain: u128) -> Result<Self> {
        let db = Database::builder()
            .set_cache_size(2 * 1024 * 1024)
            .open(path)?;
        let state: State = {
            let tx = db.begin_read()?;
            let t = tx.open_table(TABLE)?;
            let v = t.get(0)?.ok_or("missing store")?;
            unpack(v.value())?
        };
        state.validate()?;
        if state.node != node || state.domain != domain {
            return Err("wrong node/domain".into());
        }
        Ok(Self(Arc::new(Mutex::new(Inner {
            db,
            state,
            fenced: false,
            crash: None,
            save_fault: None,
        }))))
    }
    pub(crate) fn state(&self) -> Result<State> {
        let g = self.0.lock().map_err(|_| "poison")?;
        if g.fenced {
            return Err("fenced".into());
        }
        Ok(g.state.clone())
    }
    /// Same transaction adapter on deterministic memory storage. Durable model
    /// bytes explicitly survive a simulated crash; process tests use real files.
    pub fn simulated(node: u64, domain: u128) -> Result<Self> {
        let state = State {
            domain,
            node,
            hard: HardState::default().write_to_bytes()?,
            log: BTreeMap::new(),
            snapshot: Snapshot::default().write_to_bytes()?,
            image: Image::new(domain)?,
            authority: Default::default(),
            applied: 0,
            applied_term: 0,
        };
        Self::recover_simulated(&pack(&state)?, node, domain)
    }
    pub fn recover_simulated(bytes: &[u8], node: u64, domain: u128) -> Result<Self> {
        let state: State = unpack(bytes)?;
        state.validate()?;
        if state.node != node || state.domain != domain {
            return Err("simulated identity".into());
        }
        let db = Database::builder()
            .set_cache_size(1024 * 1024)
            .create_with_backend(redb::backends::InMemoryBackend::new())?;
        let store = Self(Arc::new(Mutex::new(Inner {
            db,
            state: state.clone(),
            fenced: false,
            crash: None,
            save_fault: None,
        })));
        store.update(|s| {
            *s = state;
            Ok(())
        })?;
        Ok(store)
    }
    /// Read the last durable atomic record, including after an ambiguous save.
    pub fn durable_bytes(&self) -> Result<Vec<u8>> {
        let g = self.0.lock().map_err(|_| "poison")?;
        let tx = g.db.begin_read()?;
        let t = tx.open_table(TABLE)?;
        Ok(t.get(0)?.ok_or("missing store")?.value().to_vec())
    }
    pub fn fail_next_save(&self, fault: SaveFault) -> Result<()> {
        self.0.lock().map_err(|_| "poison")?.save_fault = Some(fault);
        Ok(())
    }
    pub(crate) fn update<T>(&self, f: impl FnOnce(&mut State) -> Result<T>) -> Result<T> {
        let mut g = self.0.lock().map_err(|_| "poison")?;
        if g.fenced {
            return Err("fenced".into());
        }
        let mut next = g.state.clone();
        let out = f(&mut next)?;
        next.validate()?;
        let bytes = pack(&next)?;
        let crash = if next.applied > g.state.applied {
            g.crash.take()
        } else {
            None
        };
        let fault = g.save_fault.take();
        let saved = (|| -> Result<()> {
            let mut tx = g.db.begin_write()?;
            tx.set_durability(Durability::Immediate)?;
            {
                let mut t = tx.open_table(TABLE)?;
                t.insert(0, bytes.as_slice())?;
            }
            if crash == Some(ApplyCrash::BeforeCommit) {
                std::process::exit(86);
            }
            if fault == Some(SaveFault::BeforeCommit) {
                return Err("injected before atomic commit".into());
            }
            tx.commit()?;
            if crash == Some(ApplyCrash::AfterCommit) {
                std::process::exit(86);
            }
            if fault == Some(SaveFault::AfterCommit) {
                return Err("injected lost atomic commit response".into());
            }
            Ok(())
        })();
        if let Err(e) = saved {
            g.fenced = true;
            return Err(e);
        }
        g.state = next;
        Ok(out)
    }
    /// Trusted local diagnostics only; current remote reads go through Node ReadIndex.
    pub fn image(&self) -> Result<Image> {
        Ok(self.state()?.image)
    }
    pub fn authority(&self) -> Result<crate::authority::Authority> {
        Ok(self.state()?.authority)
    }
    pub fn applied(&self) -> Result<u64> {
        Ok(self.state()?.applied)
    }
    pub fn arm_apply_crash(&self, point: ApplyCrash) -> Result<()> {
        self.0.lock().map_err(|_| "poison")?.crash = Some(point);
        Ok(())
    }
    pub fn checkpoint(&self) -> Result<Snapshot> {
        self.update(|s| {
            if s.applied == 0 {
                return Err("no applied state".into());
            }
            let snap = s.snapshot_now()?;
            s.snapshot = snap.write_to_bytes()?;
            s.log = s.log.split_off(&(s.applied + 1));
            Ok(snap)
        })
    }
    pub fn verify_snapshot(&self, s: &Snapshot) -> Result<()> {
        self.state()?.validate_snapshot(s)?;
        Ok(())
    }
}
impl Storage for Store {
    fn initial_state(&self) -> raft::Result<RaftState> {
        let s = self.state().map_err(|_| corrupt())?;
        Ok(RaftState::new(s.hard().map_err(|_| corrupt())?, conf()))
    }
    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _ctx: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        if low < self.first_index()? {
            return Err(StorageError::Compacted.into());
        }
        if high > self.last_index()? + 1 || low > high {
            return Err(StorageError::Unavailable.into());
        }
        let s = self.state().map_err(|_| corrupt())?;
        let mut out = Vec::new();
        let mut size = 0;
        let max = max_size.into().unwrap_or(u64::MAX);
        for i in low..high {
            let e = Entry::parse_from_bytes(s.log.get(&i).ok_or_else(corrupt)?)
                .map_err(|_| corrupt())?;
            let n = e.compute_size() as u64;
            if !out.is_empty() && size + n > max {
                break;
            }
            size += n;
            out.push(e);
        }
        Ok(out)
    }
    fn term(&self, i: u64) -> raft::Result<u64> {
        let s = self.state().map_err(|_| corrupt())?;
        let snap = s.snap().map_err(|_| corrupt())?;
        let m = snap.get_metadata();
        if i == m.index {
            return Ok(m.term);
        }
        if i < m.index {
            return Err(StorageError::Compacted.into());
        }
        Ok(
            Entry::parse_from_bytes(s.log.get(&i).ok_or(StorageError::Unavailable)?)
                .map_err(|_| corrupt())?
                .term,
        )
    }
    fn first_index(&self) -> raft::Result<u64> {
        Ok(self
            .state()
            .map_err(|_| corrupt())?
            .snap()
            .map_err(|_| corrupt())?
            .get_metadata()
            .index
            + 1)
    }
    fn last_index(&self) -> raft::Result<u64> {
        let s = self.state().map_err(|_| corrupt())?;
        Ok(s.log.last_key_value().map_or(
            s.snap().map_err(|_| corrupt())?.get_metadata().index,
            |(i, _)| *i,
        ))
    }
    fn snapshot(&self, request: u64, _to: u64) -> raft::Result<Snapshot> {
        let s = self.state().map_err(|_| corrupt())?;
        if s.applied < request || s.applied == 0 {
            return Err(StorageError::SnapshotTemporarilyUnavailable.into());
        }
        s.snapshot_now().map_err(|_| corrupt())
    }
}
