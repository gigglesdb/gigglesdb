//! Durable protected admission/staging over the real range machines.
//!
//! Default physical COMMIT is deliberately fenced. The separately installed
//! logical profile uses serialized authority ticks and an atomic decision context;
//! it makes no physical-time expiry claim. Staging has no visible effects.
//! Owner pins are privileged authenticated-channel snapshots, NOT certificates.
use crate::{DIRECTORY, Data, Descriptor, Directory, Machine, Result, minimum};
use giggles_core::{
    CoreError, TaskId,
    replica::{Image, protected as core},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub mod logical;
use logical::{Decision, LogicalProfile, Original, Prepared};

pub const MAX_RECORDS: usize = 8;
pub const MAX_ROUTES: usize = 8;
pub const MAX_STAGE: usize = 512 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    pub database: u128,
    pub nonce: u128,
    pub revision: u64,
    pub task: TaskId,
    pub intent: core::Intent,
    /// Exact complete directory, in keyspace order, including empty ranges.
    pub routes: Vec<Descriptor>,
}
impl Plan {
    pub fn validate(&self) -> Result<()> {
        if self.database == 0
            || self.nonce == 0
            || self.routes.len() > MAX_ROUTES
            || self.intent.fence == 0
            || self.intent.lower > self.intent.upper
            || bincode::serialized_size(self)? > 16 * 1024
        {
            return Err("protected plan identity/limit/time".into());
        }
        Directory {
            revision: self.revision,
            routes: self.routes.clone(),
            intents: Default::default(),
        }
        .validate()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    pub plan: Plan,
    pub opened: u64,
    pub aborted: Option<u64>,
    pub released: bool,
    /// Privileged canonical material, never a receipt or external task response.
    pub staged: Option<String>,
    pub staged_digest: Option<[u8; 32]>,
    pub original: Option<Original>,
    pub decision: Option<Decision>,
    pub all_finalized: bool,
    pub logical: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Home {
    pub image: Image,
    pub installed: Option<String>,
    pub logical: Option<LogicalProfile>,
    pub records: Vec<Record>,
}
impl Home {
    pub fn new(domain: u128) -> Result<Self> {
        Ok(Self {
            image: Image::new(domain)?,
            installed: None,
            logical: None,
            records: Vec::new(),
        })
    }
    pub fn busy(&self) -> bool {
        self.records.iter().any(|r| !r.released)
    }
    pub fn record(&self, nonce: u128) -> Result<&Record> {
        self.records
            .iter()
            .find(|r| r.plan.nonce == nonce)
            .ok_or_else(|| "unknown protected home identity".into())
    }
    pub fn validate(&self, domain: u128) -> Result<()> {
        if let Some(profile) = self.logical
            && (profile.epoch == 0 || profile.tick == 0 || self.installed.is_none())
        {
            return Err("invalid logical authority profile".into());
        }
        if self.installed.is_none() {
            if !self.records.is_empty() || self.image != Image::new(domain)? {
                return Err("uninstalled protected authority must be pristine".into());
            }
            return Ok(());
        }
        authority(&self.image, domain)?;
        if self.image.bytes().len() > MAX_STAGE
            || self
                .installed
                .as_ref()
                .is_some_and(|d| hex::decode(d).map_or(true, |b| b.len() != 32))
        {
            return Err("protected authority installation/capacity".into());
        }
        if self.records.len() > MAX_RECORDS
            || self.records.iter().filter(|r| !r.released).count() > 1
        {
            return Err("protected home capacity/ownership".into());
        }
        let mut seen = std::collections::BTreeSet::new();
        for r in &self.records {
            r.plan.validate()?;
            if r.plan.database != domain
                || r.opened == 0
                || self.installed.is_none()
                || r.aborted.is_some_and(|i| i < r.opened)
                || !seen.insert(r.plan.nonce)
                || r.released && r.aborted.is_none() && r.decision.is_none()
                || r.staged.is_some() != r.staged_digest.is_some()
            {
                return Err("protected home record binding".into());
            }
            logical::validate_record(r, domain)?;
            if let Some(bytes) = &r.staged {
                let bytes = hex::decode(bytes)?;
                if bytes.len() > MAX_STAGE || Some(hash(&bytes)) != r.staged_digest {
                    return Err("protected stage digest/limit".into());
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lock {
    pub plan: Plan,
    pub acquired: u64,
    pub prepared: Option<Prepared>,
    pub finalized: bool,
    pub publication: Option<logical::Finalization>,
    /// Retained terminal identities make late duplicate Lock fail after abort.
    pub released: bool,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Participant {
    pub records: Vec<Lock>,
}
impl Participant {
    pub fn busy(&self) -> bool {
        self.records.iter().any(|r| !r.released)
    }
    pub fn validate(&self, database: u128, route: &Descriptor) -> Result<()> {
        if self.records.len() > MAX_RECORDS
            || self.records.iter().filter(|r| !r.released).count() > 1
        {
            return Err("protected participant capacity/ownership".into());
        }
        let mut seen = std::collections::BTreeSet::new();
        for r in &self.records {
            r.plan.validate()?;
            logical::validate_lock(r, route)?;
            if r.plan.database != database
                || r.acquired == 0
                || !seen.insert(r.plan.nonce)
                || !r.plan.routes.contains(route)
            {
                return Err("protected participant binding".into());
            }
        }
        Ok(())
    }
}

/// Construct only from a real quorum current barrier through trusted owner input.
/// Deserialization validates structure, never network provenance.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pin {
    pub database: u128,
    pub group: u64,
    pub applied: u64,
    pub machine: Machine,
}
impl Pin {
    pub fn from_current(o: crate::runtime::Observation) -> Result<Self> {
        if !o.quorum_checked || !o.is_leader {
            return Err("protected pin requires current quorum leader".into());
        }
        let p = Self {
            database: o.machine.domain()?,
            group: o.group,
            applied: o.applied,
            machine: o.machine,
        };
        p.validate()?;
        Ok(p)
    }
    pub fn validate(&self) -> Result<()> {
        if self.applied == 0 {
            return Err("protected pin has no applied barrier".into());
        }
        self.machine.validate(self.database, self.group)?;
        if let Machine::Range { data, .. } = &self.machine
            && data
                .transactions
                .engine(self.database, &data.descriptor)?
                .applied_index()
                / 256
                > self.applied
        {
            return Err("protected pin transaction frontier ahead of barrier".into());
        }
        Ok(())
    }
    /// Strip all grants, approvals, receipts and staging bytes before carrying
    /// evidence to a row group. Only immutable home identity/outcome is needed.
    pub fn for_home(self, nonce: u128) -> Result<HomePin> {
        self.validate()?;
        let Machine::Root { protected, .. } = self.machine else {
            return Err("protected home pin required".into());
        };
        let r = protected.record(nonce)?;
        Ok(HomePin {
            database: self.database,
            applied: self.applied,
            plan: r.plan.clone(),
            opened: r.opened,
            aborted: r.aborted,
            released: r.released,
            committed: r.decision.is_some(),
            all_finalized: r.all_finalized,
        })
    }
    /// Bound abort-completion proof independently of rows and ordinary history.
    pub fn for_release(self, nonce: u128) -> Result<Released> {
        self.validate()?;
        let Machine::Range { data, .. } = self.machine else {
            return Err("release pin requires row range".into());
        };
        let r = data
            .protected
            .records
            .iter()
            .find(|r| r.plan.nonce == nonce)
            .ok_or("missing protected release identity")?;
        Ok(Released {
            database: self.database,
            applied: self.applied,
            route: data.descriptor,
            nonce,
            plan_digest: hash(&bincode::serialize(&r.plan)?),
            acquired: r.acquired,
            released: r.released,
        })
    }
}

/// Minimal terminal participant evidence. It is privileged owner-pinned state,
/// not a remote certificate; it contains no row or authority image.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Released {
    pub database: u128,
    pub applied: u64,
    pub route: Descriptor,
    pub nonce: u128,
    pub plan_digest: [u8; 32],
    pub acquired: u64,
    pub released: bool,
}

/// Minimal owner-pinned committed home view. Contains NO authority image or
/// staged data. Trusted provenance is mandatory; this is not a certificate.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HomePin {
    pub database: u128,
    pub applied: u64,
    pub plan: Plan,
    pub opened: u64,
    pub aborted: Option<u64>,
    pub released: bool,
    pub committed: bool,
    pub all_finalized: bool,
}
impl HomePin {
    fn home(&self, plan: &Plan) -> Result<&Self> {
        self.plan.validate()?;
        if self.database != plan.database
            || self.plan != *plan
            || self.opened == 0
            || self.opened > self.applied
            || self
                .aborted
                .is_some_and(|i| i < self.opened || i > self.applied)
            || self.released && self.aborted.is_none() && !self.committed
            || self.committed && self.aborted.is_some()
            || self.all_finalized && !self.committed
        {
            return Err("protected pinned home identity/index".into());
        }
        Ok(self)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Step {
    Logical(Box<logical::Step>),
    ResumeStage {
        nonce: u128,
    },
    Install {
        image: Image,
    },
    /// Restricted real core administration, serialized behind all home work.
    Admin {
        command: Vec<u8>,
    },
    Open(Plan),
    Lock {
        plan: Plan,
        home: Box<HomePin>,
    },
    Stage {
        nonce: u128,
        rows: Vec<Pin>,
    },
    Commit {
        nonce: u128,
    },
    Abort {
        nonce: u128,
    },
    Unlock {
        plan: Plan,
        home: Box<HomePin>,
    },
    Complete {
        nonce: u128,
        rows: Vec<Released>,
    },
}
fn hash(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}
fn authority(image: &Image, database: u128) -> Result<()> {
    Image::from_bytes(image.bytes().to_vec())?;
    if image.domain()? != database || !image.snapshot()?.rows.is_empty() {
        return Err("authority must be row-free in exact database".into());
    }
    // Check relational namespaces too: the protected evaluator excludes them.
    // Catalog is read-only and carries no changes to the installation image.
    let (_, reply) = image.evaluate(&giggles_core::replica::Command::Admin(serde_json::to_vec(
        &giggles_core::local::AdminCommand::Catalog,
    )?))?;
    if reply.code != 0
        || reply
            .data
            .get(1)
            .and_then(serde_json::Value::as_array)
            .is_some_and(|t| !t.is_empty())
    {
        return Err("protected authority relational catalog unsupported".into());
    }
    Ok(())
}

struct Locked<'a> {
    plan: &'a Plan,
    home: &'a Home,
    opened: u64,
    partitions: &'a [core::Partition],
}
impl core::TrustedRuntime for Locked<'_> {
    fn verify_locked(&self, b: &core::Binding) -> std::result::Result<(), CoreError> {
        let a = &b.authority;
        if a.location != DIRECTORY
            || a.generation != 1
            || a.applied != self.opened
            || a.domain != self.plan.database
            || a.task != self.plan.task
            || b.authority_image != hash(self.home.image.bytes())
            || b.intent != self.plan.intent
            || self.home.record(self.plan.nonce).is_err()
            || b.ranges.len() != self.partitions.len()
        {
            return Err(CoreError::Fenced);
        }
        for (b, p) in b.ranges.iter().zip(self.partitions) {
            if b.owner != p.owner
                || b.epoch != p.epoch
                || b.applied != p.applied
                || b.start != p.start
                || b.end != p.end
                || b.image != hash(p.image.bytes())
            {
                return Err(CoreError::Fenced);
            }
        }
        Ok(())
    }
    fn verify_decision_fence(&self, _: &core::Binding) -> std::result::Result<(), CoreError> {
        Err(CoreError::Fenced)
    }
    fn decision(
        &self,
        _: &core::DecisionId,
        _: [u8; 32],
    ) -> std::result::Result<core::Outcome, CoreError> {
        Err(CoreError::Fenced)
    }
}

fn partitions(plan: &Plan, pins: &[Pin], released: bool) -> Result<Vec<core::Partition>> {
    if pins.len() != plan.routes.len() {
        return Err("complete protected range pins required".into());
    }
    plan.routes
        .iter()
        .zip(pins)
        .map(|(route, p)| {
            p.validate()?;
            let Machine::Range { data, .. } = &p.machine else {
                return Err("protected row pin required".into());
            };
            if p.database != plan.database
                || p.group != route.group
                || data.descriptor != *route
                || data
                    .protected
                    .records
                    .iter()
                    .find(|r| r.plan.nonce == plan.nonce)
                    .is_none_or(|r| {
                        r.plan != *plan || r.released != released || r.acquired > p.applied
                    })
                || data.fence.is_some()
                || !data.active
            {
                return Err("protected range identity/lock/index".into());
            }
            Ok(core::Partition {
                owner: route.group,
                epoch: route.epoch,
                applied: p.applied,
                start: route.bounds.start,
                end: route.bounds.end,
                image: data.image.clone(),
            })
        })
        .collect()
}

pub(crate) fn apply(machine: &mut Machine, index: u64, step: &Step) -> Result<serde_json::Value> {
    let database = machine.domain()?;
    if let Step::Logical(step) = step {
        return logical::apply(machine, index, step);
    }
    if let Step::ResumeStage { nonce } = step {
        return logical::resume(machine, *nonce);
    }
    match (machine, step) {
        (Machine::Root { protected: h, .. }, Step::Install { image }) => {
            authority(image, database)?;
            if image.bytes().len() > MAX_STAGE {
                return Err("protected authority capacity".into());
            }
            let digest = hex::encode(hash(image.bytes()));
            if let Some(old) = &h.installed {
                if *old != digest {
                    return Err("protected authority already installed".into());
                }
            } else {
                if h.busy() || !h.records.is_empty() {
                    return Err("protected authority cannot be replaced".into());
                }
                h.image = image.clone();
                h.installed = Some(digest);
            }
        }
        (Machine::Root { protected: h, .. }, Step::Admin { command }) => {
            if h.installed.is_none() || (h.busy() && h.logical.is_none()) {
                return Err("protected authority locked; resolve home first".into());
            }
            let command: giggles_core::local::AdminCommand = serde_json::from_slice(command)?;
            use giggles_core::local::AdminCommand as A;
            if h.busy() && !matches!(command, A::Revoke { .. } | A::RevokeApproval { .. }) {
                return Err("logical authority permits only ordered revoke while locked".into());
            }
            if !matches!(
                command,
                A::Grant { .. }
                    | A::Revoke { .. }
                    | A::RevokeApproval { .. }
                    | A::ProvisionTaskResources { .. }
            ) {
                return Err("protected owner administration unsupported".into());
            }
            let (next, reply) =
                h.image
                    .evaluate(&giggles_core::replica::Command::Admin(serde_json::to_vec(
                        &command,
                    )?))?;
            if reply.code != 0 {
                return Err(format!("protected core admin refused: {}", reply.data).into());
            }
            authority(&next, database)?;
            if next.bytes().len() > MAX_STAGE {
                return Err("protected authority capacity".into());
            }
            h.image = next;
        }
        (
            Machine::Root {
                protected: h,
                directory,
                ..
            },
            Step::Open(plan),
        ) => {
            plan.validate()?;
            if let Ok(old) = h.record(plan.nonce) {
                if old.plan != *plan {
                    return Err("protected same-ID body conflict".into());
                }
                return Ok(status(old));
            }
            if plan.database != database
                || plan.revision != directory.revision
                || plan.routes != directory.routes
                || directory.intents.values().any(|i| !i.published)
            {
                return Err("protected current complete coverage required".into());
            }
            if h.installed.is_none() || h.busy() || h.records.len() >= MAX_RECORDS {
                return Err("protected authority unavailable/locked/capacity".into());
            }
            h.records.push(Record {
                plan: plan.clone(),
                opened: index,
                aborted: None,
                released: false,
                staged: None,
                staged_digest: None,
                original: None,
                decision: None,
                all_finalized: false,
                logical: false,
            });
        }
        (Machine::Range { data, .. }, Step::Lock { plan, home }) => {
            plan.validate()?;
            let r = home.home(plan)?;
            if r.aborted.is_some() || r.released {
                return Err("protected home already aborted".into());
            }
            if let Some(old) = data
                .protected
                .records
                .iter()
                .find(|r| r.plan.nonce == plan.nonce)
            {
                if old.plan != *plan || old.released {
                    return Err("protected lock replay conflict/terminal".into());
                }
                return Ok(serde_json::json!({"locked":true}));
            }
            data.check(&data.descriptor, &data.descriptor.bounds)?;
            if !plan.routes.contains(&data.descriptor)
                || plan.database != database
                || data.protected.busy()
                || data.protected.records.len() >= MAX_RECORDS
                || data.transactions.unresolved(database, &data.descriptor)?
            {
                return Err("protected range unavailable/conflict/capacity".into());
            }
            data.protected.records.push(Lock {
                plan: plan.clone(),
                acquired: index,
                prepared: None,
                finalized: false,
                publication: None,
                released: false,
            });
        }
        (Machine::Root { protected: h, .. }, Step::Stage { nonce, rows }) => {
            let r = h.record(*nonce)?.clone();
            if r.aborted.is_some() || r.released || r.decision.is_some() {
                return Err("protected terminal home".into());
            }
            let fresh = partitions(&r.plan, rows, false)?;
            let original = match &r.original {
                Some(o) => {
                    o.check_fresh(&fresh)?;
                    o.clone()
                }
                None => Original::capture(h, &r, &fresh)?,
            };
            let parts = original.partitions();
            let runtime = Locked {
                plan: &r.plan,
                home: h,
                opened: r.opened,
                partitions: &parts,
            };
            let staged = core::stage(
                &h.image,
                core::Authority {
                    location: DIRECTORY,
                    generation: 1,
                    applied: r.opened,
                    domain: database,
                    task: r.plan.task,
                },
                &parts,
                r.plan.intent.clone(),
                &runtime,
            )?;
            let bytes = staged.to_bytes()?;
            if bytes.len() > MAX_STAGE {
                return Err("protected staging capacity".into());
            }
            core::Staged::verify_bytes(&bytes, &h.image, &parts, &runtime)?;
            let digest = staged.digest()?;
            // A later barrier changes applied indices. Exact repeated staging is
            // supported only with the original immutable pins, never rebound.
            if r.staged_digest.is_some_and(|old| old != digest) {
                return Err("protected staged pin/body conflict".into());
            }
            let r = h
                .records
                .iter_mut()
                .find(|r| r.plan.nonce == *nonce)
                .ok_or("missing home")?;
            r.original = Some(original);
            r.staged = Some(hex::encode(bytes));
            r.staged_digest = Some(digest);
            return Ok(
                serde_json::json!({"staged":true,"digest":hex::encode(digest),"committable":false}),
            );
        }
        (Machine::Root { .. }, Step::Commit { .. }) => return Err(
            "protected commit fenced: trusted apply-time clock/pause fence unavailable; no effects"
                .into(),
        ),
        (Machine::Root { protected: h, .. }, Step::Abort { nonce }) => {
            let r = h
                .records
                .iter_mut()
                .find(|r| r.plan.nonce == *nonce)
                .ok_or("unknown protected identity")?;
            if r.decision.is_some() {
                return Err("committed protected decision cannot abort".into());
            }
            if r.aborted.is_none() {
                r.aborted = Some(index);
            }
            return Ok(status(r));
        }
        (Machine::Range { data, .. }, Step::Unlock { plan, home }) => {
            if home.home(plan)?.aborted.is_none() && !home.all_finalized {
                return Err("protected unlock requires durable home abort".into());
            }
            if let Some(r) = data
                .protected
                .records
                .iter_mut()
                .find(|r| r.plan.nonce == plan.nonce)
            {
                if r.plan != *plan || home.committed && !r.finalized {
                    return Err("protected unlock body/phase conflict".into());
                }
                r.released = true;
            } else {
                // Retain a tombstone even when Lock had not reached this range.
                // Otherwise an old pre-abort home pin could acquire it later.
                if plan.database != database
                    || !plan.routes.contains(&data.descriptor)
                    || data.protected.records.len() >= MAX_RECORDS
                {
                    return Err("protected abort tombstone capacity/route".into());
                }
                data.protected.records.push(Lock {
                    plan: plan.clone(),
                    acquired: index,
                    prepared: None,
                    finalized: false,
                    publication: None,
                    released: true,
                });
            }
        }
        (Machine::Root { protected: h, .. }, Step::Complete { nonce, rows }) => {
            let r = h.record(*nonce)?.clone();
            if r.released {
                return Ok(status(&r));
            }
            if r.aborted.is_none() && !r.all_finalized {
                return Err("protected completion requires abort or all-finalized".into());
            }
            if rows.len() != r.plan.routes.len() {
                return Err("complete protected release set required".into());
            }
            let digest = hash(&bincode::serialize(&r.plan)?);
            for (route, p) in r.plan.routes.iter().zip(rows) {
                if p.database != database
                    || p.route != *route
                    || p.nonce != *nonce
                    || p.plan_digest != digest
                    || !p.released
                    || p.acquired == 0
                    || p.acquired > p.applied
                {
                    return Err("protected terminal release binding".into());
                }
            }
            h.records
                .iter_mut()
                .find(|r| r.plan.nonce == *nonce)
                .ok_or("missing home")?
                .released = true;
        }
        _ => return Err("protected step incompatible with group".into()),
    }
    Ok(serde_json::json!({"applied":true,"committable":false}))
}

pub fn status(r: &Record) -> serde_json::Value {
    serde_json::json!({"nonce":r.plan.nonce,"aborted":r.aborted.is_some(),"released":r.released,
        "staged":r.staged.is_some(),"committed":r.decision.is_some(),"all_finalized":r.all_finalized,"profile":if r.logical {"trusted-owner-logical"} else {"physical-fenced"},"committable":r.logical})
}

/// Common guard used by every ordinary data path, including observation locks.
pub(crate) fn guard(data: &Data) -> Result<()> {
    if data.protected.busy() {
        Err("protected whole-range intent blocks ordinary access".into())
    } else {
        Ok(())
    }
}

/// Data images never inherit authority. Used by regression tests and inspectors.
pub fn data_only(data: &Data) -> Result<bool> {
    Ok(data.image.row_partition(minimum(), None)? == data.image)
}
