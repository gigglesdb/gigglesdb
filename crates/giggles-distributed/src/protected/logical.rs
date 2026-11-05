//! Explicit trusted-owner logical profile. Logical ticks are replicated authority
//! state, never wall time, and this API does not accept physical credentials.
use super::*;
use giggles_core::{Effect, local::AdminCommand, replica};

/// Installed once alongside an authority whose validity bounds mean logical ticks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalProfile {
    pub epoch: u64,
    pub tick: u64,
}
impl LogicalProfile {
    fn validate(self) -> Result<()> {
        if self.epoch == 0 || self.tick == 0 {
            return Err("logical epoch/tick must be positive".into());
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OriginalPart {
    pub route: Descriptor,
    pub applied: u64,
    pub image: Image,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Original {
    pub authority: Image,
    pub parts: Vec<OriginalPart>,
    pub binding: [u8; 32],
}
// Fixed-width typed rows do not grow when rewritten. Keeping the profile small
// reserves room for original+after bytes, canonical redo and all terminal proofs.
pub const MAX_IMAGE: usize = 16 * 1024;
pub const MAX_RECORD: usize = 384 * 1024;
impl Original {
    pub(super) fn capture(h: &Home, r: &Record, parts: &[core::Partition]) -> Result<Self> {
        let mut value = Self {
            authority: h.image.clone(),
            parts: parts
                .iter()
                .zip(&r.plan.routes)
                .map(|(p, route)| OriginalPart {
                    route: route.clone(),
                    applied: p.applied,
                    image: p.image.clone(),
                })
                .collect(),
            binding: [0; 32],
        };
        value.binding = value.binding_digest(&r.plan, r.opened)?;
        if bincode::serialized_size(&value)? > MAX_STAGE as u64 {
            return Err("original footprint capacity".into());
        }
        Ok(value)
    }
    fn binding_digest(&self, plan: &Plan, opened: u64) -> Result<[u8; 32]> {
        Ok(hash(&bincode::serialize(&(
            &self.authority,
            &self.parts,
            plan,
            opened,
        ))?))
    }
    pub fn partitions(&self) -> Vec<core::Partition> {
        self.parts
            .iter()
            .map(|p| core::Partition {
                owner: p.route.group,
                epoch: p.route.epoch,
                applied: p.applied,
                start: p.route.bounds.start,
                end: p.route.bounds.end,
                image: p.image.clone(),
            })
            .collect()
    }
    pub(super) fn check_fresh(&self, parts: &[core::Partition]) -> Result<()> {
        if self.parts.len() != parts.len()
            || self.parts.iter().zip(parts).any(|(o, p)| {
                o.route.group != p.owner
                    || o.route.epoch != p.epoch
                    || o.route.bounds.start != p.start
                    || o.route.bounds.end != p.end
                    || o.image != p.image
                    || p.applied < o.applied
            })
        {
            return Err("fresh pin changed original intent/image".into());
        }
        Ok(())
    }
}
/// Data-only exact prepare envelope, projected from a current home owner pin.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Preparation {
    pub plan: Plan,
    pub opened: u64,
    pub staged: [u8; 32],
    pub part: OriginalPart,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Prepared {
    pub preparation: Preparation,
    pub acquired: u64,
    pub prepared_at: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationPart {
    pub prepared: Prepared,
    pub after: Image,
}
/// Immutable home outcome. All authority and data before/after bytes are retained
/// only at the trusted home; participant projection carries no authority image.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Decision {
    pub at: u64,
    pub profile: LogicalProfile,
    pub id: core::DecisionId,
    pub validated: Vec<u8>,
    pub digest: [u8; 32],
    pub authority_after: Image,
    pub parts: Vec<PublicationPart>,
    pub seal: [u8; 32],
}
impl Decision {
    fn seal_digest(&self) -> Result<[u8; 32]> {
        Ok(hash(&bincode::serialize(&(
            self.at,
            self.profile,
            &self.id,
            &self.validated,
            self.digest,
            &self.authority_after,
            &self.parts,
        ))?))
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Finalization {
    pub database: u128,
    pub nonce: u128,
    pub digest: [u8; 32],
    pub decision_at: u64,
    pub part: PublicationPart,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Finalized {
    pub seal: Finalization,
    pub applied: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Step {
    Install {
        image: Image,
        profile: LogicalProfile,
    },
    Advance {
        profile: LogicalProfile,
    },
    Open(Plan),
    Prepare(Preparation),
    Commit {
        nonce: u128,
        prepared: Vec<Prepared>,
    },
    Finalize(Finalization),
    VerifyDecision {
        nonce: u128,
    },
    ConfirmFinalized {
        nonce: u128,
        rows: Vec<Finalized>,
    },
    /// Trusted owner only: real NativeTask Status debits each delivery attempt.
    /// No listener dispatch exists. Principal is resolved by the owner channel.
    Output {
        nonce: u128,
        principal: giggles_core::Principal,
    },
}
impl Pin {
    pub fn logical_preparation(&self, nonce: u128, group: u64) -> Result<Preparation> {
        self.validate()?;
        let Machine::Root { protected: h, .. } = &self.machine else {
            return Err("home required".into());
        };
        let r = h.record(nonce)?;
        if !r.logical || r.aborted.is_some() || r.released || r.decision.is_some() {
            return Err("logical staging home required".into());
        }
        let o = r.original.as_ref().ok_or("stage original missing")?;
        Ok(Preparation {
            plan: r.plan.clone(),
            opened: r.opened,
            staged: r.staged_digest.ok_or("stage missing")?,
            part: o
                .parts
                .iter()
                .find(|p| p.route.group == group)
                .ok_or("prepare route missing")?
                .clone(),
        })
    }
    pub fn logical_prepared(&self, nonce: u128) -> Result<Prepared> {
        self.validate()?;
        let Machine::Range { data, .. } = &self.machine else {
            return Err("participant required".into());
        };
        data.protected
            .records
            .iter()
            .find(|r| r.plan.nonce == nonce)
            .and_then(|r| r.prepared.clone())
            .ok_or_else(|| "prepared identity missing".into())
    }
    pub fn logical_finalization(&self, nonce: u128, group: u64) -> Result<Finalization> {
        self.validate()?;
        let Machine::Root { protected: h, .. } = &self.machine else {
            return Err("home required".into());
        };
        let d = h
            .record(nonce)?
            .decision
            .as_ref()
            .ok_or("committed home required")?;
        Ok(Finalization {
            database: self.database,
            nonce,
            digest: d.digest,
            decision_at: d.at,
            part: d
                .parts
                .iter()
                .find(|p| p.prepared.preparation.part.route.group == group)
                .ok_or("publication route missing")?
                .clone(),
        })
    }
    pub fn logical_finalized(&self, seal: Finalization) -> Result<Finalized> {
        self.validate()?;
        let Machine::Range { data, .. } = &self.machine else {
            return Err("participant required".into());
        };
        let r = data
            .protected
            .records
            .iter()
            .find(|r| r.plan.nonce == seal.nonce)
            .ok_or("participant missing")?;
        if !r.finalized
            || r.publication.as_ref() != Some(&seal)
            || r.prepared.as_ref() != Some(&seal.part.prepared)
            || !r.released && data.image != seal.part.after
            || seal.database != self.database
        {
            return Err("finalized proof mismatch".into());
        }
        Ok(Finalized {
            seal,
            applied: self.applied,
        })
    }
}
pub(super) fn validate_record(r: &Record, domain: u128) -> Result<()> {
    if r.original.is_some() != r.staged.is_some()
        || r.decision.is_some() && (!r.logical || r.aborted.is_some())
        || r.all_finalized && r.decision.is_none()
    {
        return Err("protected original/decision phase".into());
    }
    if let Some(o) = &r.original {
        authority(&o.authority, domain)?;
        if o.parts.len() != r.plan.routes.len() {
            return Err("original complete coverage".into());
        }
        for (p, route) in o.parts.iter().zip(&r.plan.routes) {
            if &p.route != route
                || p.applied == 0
                || p.image.domain()? != domain
                || p.image
                    .row_partition(route.bounds.start, route.bounds.end)?
                    != p.image
            {
                return Err("original partition binding".into());
            }
        }
        if o.binding != o.binding_digest(&r.plan, r.opened)? {
            return Err("original binding digest mismatch".into());
        }
        if let Some(d) = &r.decision {
            d.profile.validate()?;
            if d.at < r.opened
                || d.id.home != DIRECTORY
                || d.id.transaction != r.plan.nonce
                || d.id.epoch != d.profile.epoch
                || hash(&d.validated) != d.digest
                || d.parts.len() != o.parts.len()
                || d.seal != d.seal_digest()?
            {
                return Err("decision sealed identity/context".into());
            }
            check_prepared(
                r,
                o,
                &d.parts
                    .iter()
                    .map(|p| p.prepared.clone())
                    .collect::<Vec<_>>(),
            )?;
            for (p, original) in d.parts.iter().zip(&o.parts) {
                if p.prepared.preparation.part != *original
                    || p.after.bytes().len() > MAX_IMAGE
                    || p.after
                        .row_partition(original.route.bounds.start, original.route.bounds.end)?
                        != p.after
                {
                    return Err("decision publication binding".into());
                }
            }
        }
    }
    if r.logical && bincode::serialized_size(r)? > MAX_RECORD as u64 {
        return Err("logical retained record capacity".into());
    }
    Ok(())
}
pub(super) fn validate_lock(r: &Lock, route: &Descriptor) -> Result<()> {
    if r.finalized != r.publication.is_some() {
        return Err("publication phase mismatch".into());
    }
    if let Some(p) = &r.prepared {
        if p.preparation.plan != r.plan
            || p.acquired != r.acquired
            || p.prepared_at < r.acquired
            || &p.preparation.part.route != route
            || p.preparation.part.image.bytes().len() > MAX_IMAGE
        {
            return Err("prepared lock identity".into());
        }
    } else if r.finalized {
        return Err("finalized without prepare".into());
    }
    if let Some(seal) = &r.publication
        && (seal.nonce != r.plan.nonce
            || seal.database != r.plan.database
            || r.prepared.as_ref() != Some(&seal.part.prepared)
            || seal.decision_at <= seal.part.prepared.preparation.opened
            || seal.part.after.bytes().len() > MAX_IMAGE
            || seal
                .part
                .after
                .row_partition(route.bounds.start, route.bounds.end)?
                != seal.part.after)
    {
        return Err("retained publication identity".into());
    }
    Ok(())
}
pub(super) fn resume(machine: &mut Machine, nonce: u128) -> Result<serde_json::Value> {
    let Machine::Root { protected: h, .. } = machine else {
        return Err("home required".into());
    };
    let r = h.record(nonce)?;
    let o = r.original.as_ref().ok_or("original stage absent")?;
    let parts = o.partitions();
    let runtime = Locked {
        plan: &r.plan,
        home: h,
        opened: r.opened,
        partitions: &parts,
    };
    let bytes = hex::decode(r.staged.as_ref().ok_or("stage absent")?)?;
    // Undecided work must still have its exact original authority. Decided work
    // verifies retained redo separately and is never reauthorized here.
    if h.image != o.authority {
        return Err("original authority changed; resolve outcome".into());
    }
    let stage = core::Staged::verify_bytes(&bytes, &o.authority, &parts, &runtime)?;
    if Some(stage.digest()?) != r.staged_digest {
        return Err("original digest mismatch".into());
    }
    Ok(
        serde_json::json!({"staged":true,"digest":hex::encode(stage.digest()?),"profile":if r.logical {"trusted-owner-logical"} else {"physical-fenced"}}),
    )
}
// This context exists only inside the serialized deterministic home apply. It
// represents the exact decision being atomically installed, not a proposal-time
// timestamp or an externally reusable capability. Machine::apply discards every
// mutation if any validation/publication/capacity check below fails.
struct Deciding<'a> {
    locked: Locked<'a>,
    profile: LogicalProfile,
    id: core::DecisionId,
    digest: Option<[u8; 32]>,
}
impl core::TrustedRuntime for Deciding<'_> {
    fn verify_locked(&self, b: &core::Binding) -> std::result::Result<(), CoreError> {
        let mut original = b.clone();
        original.intent = self.locked.plan.intent.clone();
        self.locked.verify_locked(&original)
    }
    fn verify_decision_fence(&self, b: &core::Binding) -> std::result::Result<(), CoreError> {
        if self.locked.home.logical != Some(self.profile)
            || b.intent.lower != self.profile.tick
            || b.intent.upper != self.profile.tick
            || b.intent.fence != self.profile.epoch
        {
            return Err(CoreError::Fenced);
        }
        self.verify_locked(b)
    }
    fn decision(
        &self,
        id: &core::DecisionId,
        digest: [u8; 32],
    ) -> std::result::Result<core::Outcome, CoreError> {
        if id != &self.id || self.digest != Some(digest) {
            return Err(CoreError::Fenced);
        }
        Ok(core::Outcome::Committed)
    }
}
fn rewrite(image: &Image, effects: &[Effect]) -> Result<Image> {
    let mut image = image.clone();
    for e in effects {
        if image.snapshot()?.rows.get(&e.key) != Some(&e.before) {
            return Err("publication before CAS".into());
        }
        let (next, reply) = image.evaluate(&replica::Command::Admin(serde_json::to_vec(
            &AdminCommand::Seed {
                key: e.key,
                row: e.after.clone(),
            },
        )?))?;
        if reply.code != 0 || next.snapshot()?.rows.get(&e.key) != Some(&e.after) {
            return Err("publication exact after CAS".into());
        }
        image = next;
    }
    Ok(image)
}
fn record_mut(h: &mut Home, nonce: u128) -> Result<&mut Record> {
    h.records
        .iter_mut()
        .find(|r| r.plan.nonce == nonce)
        .ok_or_else(|| "unknown logical identity".into())
}
fn check_prepared(r: &Record, o: &Original, proofs: &[Prepared]) -> Result<()> {
    if proofs.len() != o.parts.len() {
        return Err("complete prepared set required".into());
    }
    for (p, part) in proofs.iter().zip(&o.parts) {
        if p.preparation.plan != r.plan
            || p.preparation.opened != r.opened
            || Some(p.preparation.staged) != r.staged_digest
            || p.preparation.part != *part
            || p.acquired == 0
            || p.acquired > p.prepared_at
        {
            return Err("prepared seal mismatch".into());
        }
    }
    Ok(())
}
pub(super) fn apply(machine: &mut Machine, index: u64, step: &Step) -> Result<serde_json::Value> {
    let database = machine.domain()?;
    match step {
        Step::Install { image, profile } => {
            profile.validate()?;
            let Machine::Root { protected: h, .. } = machine else {
                return Err("logical home required".into());
            };
            if h.installed.is_some() {
                if h.logical == Some(*profile)
                    && h.installed.as_deref() == Some(&hex::encode(hash(image.bytes())))
                {
                    return Ok(serde_json::json!({"installed":true}));
                }
                return Err(
                    "physical authority cannot enter logical profile; installation immutable"
                        .into(),
                );
            }
            if image.bytes().len() > MAX_IMAGE {
                return Err("logical authority capacity".into());
            }
            super::apply(
                machine,
                index,
                &super::Step::Install {
                    image: image.clone(),
                },
            )?;
            let Machine::Root { protected: h, .. } = machine else {
                unreachable!()
            };
            h.logical = Some(*profile);
        }
        Step::Advance { profile } => {
            profile.validate()?;
            let Machine::Root { protected: h, .. } = machine else {
                return Err("logical home required".into());
            };
            let old = h.logical.ok_or("physical authority fenced")?;
            if profile.epoch < old.epoch || profile.tick < old.tick {
                return Err("logical authority regression".into());
            }
            h.logical = Some(*profile);
        }
        Step::Open(plan) => {
            let Machine::Root { protected: h, .. } = machine else {
                return Err("logical home required".into());
            };
            let profile = h.logical.ok_or("physical authority fenced")?;
            if let Ok(r) = h.record(plan.nonce) {
                if r.plan != *plan || !r.logical {
                    return Err("logical replay conflict".into());
                }
                return Ok(status(r));
            }
            if plan.intent.lower != profile.tick
                || plan.intent.upper != profile.tick
                || plan.intent.fence != profile.epoch
            {
                return Err(
                    "intent must use committed logical authority; client time is not authority"
                        .into(),
                );
            }
            super::apply(machine, index, &super::Step::Open(plan.clone()))?;
            let Machine::Root { protected: h, .. } = machine else {
                unreachable!()
            };
            record_mut(h, plan.nonce)?.logical = true;
        }
        Step::Prepare(preparation) => {
            let Machine::Range { data, .. } = machine else {
                return Err("logical participant required".into());
            };
            let r = data
                .protected
                .records
                .iter_mut()
                .find(|r| r.plan.nonce == preparation.plan.nonce)
                .ok_or("lock required")?;
            if r.plan != preparation.plan
                || r.released
                || preparation.part.route != data.descriptor
                || preparation.part.image != data.image
                || preparation.part.applied < r.acquired
                || preparation.opened == 0
            {
                return Err("logical prepare original/lock mismatch".into());
            }
            if let Some(p) = &r.prepared {
                if p.preparation != *preparation {
                    return Err("prepare replay conflict".into());
                }
                return Ok(serde_json::json!({"prepared":true}));
            }
            if data.image.bytes().len() > MAX_IMAGE {
                return Err("prepared footprint capacity".into());
            }
            // Reserve the worst allowed rewrite: every existing fixed-width row
            // once. The whole-range lock excludes all other generation spenders.
            let mut reserved = data.image.clone();
            for (key, row) in data.image.snapshot()?.rows {
                let (next, reply) = reserved.evaluate(&replica::Command::Admin(
                    serde_json::to_vec(&AdminCommand::Seed { key, row })?,
                ))?;
                if reply.code != 0 || next.bytes().len() > MAX_IMAGE {
                    return Err("prepared terminal image reservation".into());
                }
                reserved = next;
            }
            r.prepared = Some(Prepared {
                preparation: preparation.clone(),
                acquired: r.acquired,
                prepared_at: index,
            });
        }
        Step::Commit { nonce, prepared } => {
            let Machine::Root { protected: h, .. } = machine else {
                return Err("logical home required".into());
            };
            let r = h.record(*nonce)?.clone();
            if let Some(d) = &r.decision {
                if prepared
                    != &d
                        .parts
                        .iter()
                        .map(|p| p.prepared.clone())
                        .collect::<Vec<_>>()
                {
                    return Err("committed prepare replay conflict".into());
                }
                return Ok(status(&r));
            }
            let profile = h.logical.ok_or("physical authority fenced")?;
            if !r.logical
                || r.aborted.is_some()
                || r.released
                || profile.epoch != r.plan.intent.fence
            {
                return Err("logical decision fenced/terminal".into());
            }
            let original = r.original.as_ref().ok_or("stage required")?;
            if h.image != original.authority || h.image.bytes().len() > MAX_IMAGE {
                return Err("logical authority changed/revoked/capacity".into());
            }
            check_prepared(&r, original, prepared)?;
            let parts = original.partitions();
            let locked = Locked {
                plan: &r.plan,
                home: h,
                opened: r.opened,
                partitions: &parts,
            };
            let staged = core::Staged::verify_bytes(
                &hex::decode(r.staged.as_ref().ok_or("stage absent")?)?,
                &original.authority,
                &parts,
                &locked,
            )?;
            let id = core::DecisionId {
                home: DIRECTORY,
                epoch: profile.epoch,
                transaction: *nonce,
            };
            let mut context = Deciding {
                locked,
                profile,
                id: id.clone(),
                digest: None,
            };
            let validated = core::validate(
                &staged,
                &original.authority,
                &parts,
                core::DecisionTime {
                    lower: profile.tick,
                    upper: profile.tick,
                    fence: profile.epoch,
                },
                id.clone(),
                &context,
            )?;
            let digest = validated.digest()?;
            context.digest = Some(digest);
            let publication =
                core::publish(&validated, &h.image, &context)?.ok_or("unexpected logical abort")?;
            let mut publications = Vec::new();
            for proof in prepared {
                let part = &proof.preparation.part;
                let effects = publication
                    .row_effects()
                    .iter()
                    .filter(|e| part.route.bounds.contains(e.key))
                    .cloned()
                    .collect::<Vec<_>>();
                let after = rewrite(&part.image, &effects)?;
                if after.bytes().len() > MAX_IMAGE {
                    return Err("reserved publication image exceeded".into());
                }
                publications.push(PublicationPart {
                    prepared: proof.clone(),
                    after,
                });
            }
            if publication.authority().bytes().len() > MAX_IMAGE {
                return Err("authority resolution capacity".into());
            }
            let mut decision = Decision {
                at: index,
                profile,
                id,
                validated: validated.to_bytes()?,
                digest,
                authority_after: publication.authority().clone(),
                parts: publications,
                seal: [0; 32],
            };
            decision.seal = decision.seal_digest()?;
            let mut next = r.clone();
            next.decision = Some(decision.clone());
            if bincode::serialized_size(&next)? > MAX_RECORD as u64 {
                return Err("decision reservation capacity".into());
            }
            // Atomic with outcome and applied index by Machine::apply / Store.
            h.image = decision.authority_after.clone();
            *record_mut(h, *nonce)? = next;
            return Ok(status(h.record(*nonce)?));
        }
        Step::Finalize(seal) => {
            let Machine::Range { data, .. } = machine else {
                return Err("logical participant required".into());
            };
            let r = data
                .protected
                .records
                .iter_mut()
                .find(|r| r.plan.nonce == seal.nonce)
                .ok_or("prepared lock missing")?;
            if seal.database != database
                || seal.nonce != r.plan.nonce
                || seal.decision_at <= seal.part.prepared.preparation.opened
                || r.prepared.as_ref() != Some(&seal.part.prepared)
                || seal.part.after.bytes().len() > MAX_IMAGE
                || seal
                    .part
                    .after
                    .row_partition(data.descriptor.bounds.start, data.descriptor.bounds.end)?
                    != seal.part.after
            {
                return Err("publication seal mismatch".into());
            }
            if r.finalized {
                if r.publication.as_ref() != Some(seal)
                    || (!r.released && data.image != seal.part.after)
                {
                    return Err("finalized after mismatch".into());
                }
                return Ok(serde_json::json!({"finalized":true}));
            }
            if r.released || data.image != seal.part.prepared.preparation.part.image {
                return Err("publication before image changed".into());
            }
            data.image = seal.part.after.clone();
            r.finalized = true;
            r.publication = Some(seal.clone());
        }
        Step::VerifyDecision { nonce } => {
            let Machine::Root { protected: h, .. } = machine else {
                return Err("home required".into());
            };
            verify_decision(h, *nonce)?;
        }
        Step::ConfirmFinalized { nonce, rows } => {
            let Machine::Root { protected: h, .. } = machine else {
                return Err("logical home required".into());
            };
            let r = h.record(*nonce)?;
            let d = r.decision.as_ref().ok_or("committed decision required")?;
            if rows.len() != d.parts.len() {
                return Err("complete finalized set required".into());
            }
            for (proof, part) in rows.iter().zip(&d.parts) {
                if proof.seal.database != database
                    || proof.seal.nonce != *nonce
                    || proof.seal.digest != d.digest
                    || proof.seal.decision_at != d.at
                    || &proof.seal.part != part
                    || proof.applied < part.prepared.prepared_at
                {
                    return Err("finalized proof mismatch".into());
                }
            }
            record_mut(h, *nonce)?.all_finalized = true;
        }
        Step::Output { nonce, principal } => {
            let Machine::Root { protected: h, .. } = machine else {
                return Err("logical home required".into());
            };
            let r = h.record(*nonce)?;
            if !r.logical || !r.released || r.decision.is_none() || h.busy() {
                return Err("complete logical owner outcome required".into());
            }
            let profile = h.logical.ok_or("logical profile required")?;
            let task = r.plan.task;
            let operation = r.plan.intent.request.operation;
            let (next, reply) = h.image.evaluate(&replica::Command::NativeTask {
                task,
                principal: *principal,
                lower: profile.tick,
                upper: profile.tick,
                action: replica::TaskAction::Status(operation),
            })?;
            if reply.code != 0 {
                return Err("task output accounting refused".into());
            }
            let (next, release) = next.evaluate(&replica::Command::ValidateRelease {
                task,
                principal: *principal,
                lower: profile.tick,
                upper: profile.tick,
            })?;
            if release.code != 0 || next.bytes().len() > MAX_IMAGE {
                return Err("task output release refused".into());
            }
            h.image = next;
            return Ok(reply.data);
        }
    }
    Ok(serde_json::json!({"applied":true,"profile":"trusted-owner-logical"}))
}

/// Re-run actual core redo against immutable decision-time originals. This is
/// explicit recovery work; routine ReadIndex and local shape validation use
/// canonical retained integrity bindings instead of reexecuting the evaluator.
fn verify_decision(h: &Home, nonce: u128) -> Result<()> {
    let r = h.record(nonce)?;
    let d = r.decision.as_ref().ok_or("decision absent")?;
    let o = r.original.as_ref().ok_or("original absent")?;
    let parts = o.partitions();
    let retained = Home {
        image: o.authority.clone(),
        installed: h.installed.clone(),
        logical: Some(d.profile),
        records: vec![r.clone()],
    };
    let context = Deciding {
        locked: Locked {
            plan: &r.plan,
            home: &retained,
            opened: r.opened,
            partitions: &parts,
        },
        profile: d.profile,
        id: d.id.clone(),
        digest: Some(d.digest),
    };
    let staged = core::Staged::verify_bytes(
        &hex::decode(r.staged.as_ref().ok_or("stage absent")?)?,
        &o.authority,
        &parts,
        &context.locked,
    )?;
    let v = core::Validated::verify_bytes(&d.validated, &o.authority, &parts)?;
    let recomputed = core::validate(
        &staged,
        &o.authority,
        &parts,
        core::DecisionTime {
            lower: d.profile.tick,
            upper: d.profile.tick,
            fence: d.profile.epoch,
        },
        d.id.clone(),
        &context,
    )?;
    if recomputed.to_bytes()? != d.validated {
        return Err("decision redo context mismatch".into());
    }
    let publication = core::publish(&v, &o.authority, &context)?.ok_or("publication absent")?;
    if publication.authority() != &d.authority_after {
        return Err("authority redo mismatch".into());
    }
    for p in &d.parts {
        let original = &p.prepared.preparation.part;
        let effects = publication
            .row_effects()
            .iter()
            .filter(|e| original.route.bounds.contains(e.key))
            .cloned()
            .collect::<Vec<_>>();
        if rewrite(&original.image, &effects)? != p.after {
            return Err("row redo mismatch".into());
        }
    }
    Ok(())
}
