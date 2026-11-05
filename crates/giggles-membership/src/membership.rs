use crate::Result;
use raft::eraftpb::{
    ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, ConfState,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Trusted owner binding, carried by every administrative command.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Identity {
    pub group: u64,
    pub owner_epoch: u64,
}
/// Stable semantic identity. The same id may never identify a different payload.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Intent {
    pub id: u64,
    pub epoch: u64,
    pub learner: u64,
    pub remove: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Phase {
    Planned,
    CatchingUp,
    Joint,
    Complete,
}
/// An authenticated adapter assertion of *durable application* on the learner.
/// The driver additionally checks current leader term and replication progress.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Readiness {
    pub intent: u64,
    pub epoch: u64,
    pub term: u64,
    pub barrier: u64,
    pub durable_applied: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Progress {
    pub intent: Intent,
    pub phase: Phase,
    pub added_at: u64,
    pub readiness: Option<Readiness>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Operation {
    Begin(Intent),
    Ready(Readiness),
    Fence { next_owner_epoch: u64 },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Command {
    pub identity: Identity,
    pub operation: Operation,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Step {
    AddLearner,
    EnterJoint,
    LeaveJoint,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Change {
    pub identity: Identity,
    pub intent: u64,
    pub epoch: u64,
    pub step: Step,
}
/// Replicated controller state, included in the same atomic batch as ConfState
/// and the application index. Receipts are never implicitly evicted.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Membership {
    pub identity: Identity,
    pub epoch: u64,
    pub voters: Vec<u64>,
    pub active: Option<u64>,
    pub receipts: BTreeMap<u64, Progress>,
    pub capacity: usize,
}
impl Membership {
    /// Validate a recovered controller against its atomic applied frontier.
    pub fn validate(&self, applied: u64) -> Result<()> {
        Self::new(self.identity, self.voters.clone(), self.capacity)?;
        if self.epoch == 0 || self.receipts.len() > self.capacity {
            return Err("controller bounds/epoch".into());
        }
        let mut active = None;
        for (id, p) in &self.receipts {
            if *id == 0
                || *id != p.intent.id
                || p.intent.learner == 0
                || p.intent.remove == 0
                || p.intent.learner == p.intent.remove
                || p.intent.epoch == 0
            {
                return Err("receipt identity".into());
            }
            if p.phase == Phase::Complete {
                if p.intent.epoch >= self.epoch {
                    return Err("completed epoch".into());
                }
            } else {
                if active.replace(*id).is_some()
                    || p.intent.epoch != self.epoch
                    || !self.voters.contains(&p.intent.remove)
                    || self.voters.contains(&p.intent.learner)
                {
                    return Err("active membership invariant".into());
                }
            }
            if p.phase == Phase::Planned {
                if p.added_at != 0 || p.readiness.is_some() {
                    return Err("planned progress invariant".into());
                }
            } else if p.added_at == 0 || p.added_at > applied {
                return Err("learner apply barrier".into());
            }
            if let Some(r) = &p.readiness
                && (r.intent != *id
                    || r.epoch != p.intent.epoch
                    || r.term == 0
                    || r.barrier < p.added_at
                    || r.durable_applied < r.barrier
                    || r.durable_applied > applied)
            {
                return Err("readiness invariant".into());
            }
        }
        if active != self.active {
            return Err("active receipt invariant".into());
        }
        Ok(())
    }
    pub fn new(identity: Identity, mut voters: Vec<u64>, capacity: usize) -> Result<Self> {
        voters.sort_unstable();
        if identity.group == 0
            || identity.owner_epoch == 0
            || capacity == 0
            || capacity > 1024
            || voters.is_empty()
            || voters.len() > 31
            || voters.contains(&0)
            || voters.windows(2).any(|w| w[0] == w[1])
        {
            return Err("initial membership bounds".into());
        }
        Ok(Self {
            identity,
            epoch: 1,
            voters,
            active: None,
            receipts: BTreeMap::new(),
            capacity,
        })
    }
    pub fn progress(&self) -> Result<&Progress> {
        self.receipts
            .get(&self.active.ok_or("no active intent")?)
            .ok_or_else(|| "missing intent".into())
    }
    fn bound(&self, identity: Identity) -> Result<()> {
        if self.identity != identity {
            return Err("group/owner epoch fence".into());
        }
        Ok(())
    }
    /// Apply a committed normal command. Rejection is deterministic and leaves
    /// state untouched; the adapter still advances the entry's applied index.
    pub fn apply(&mut self, command: &Command, index: u64, term: u64) -> Result<()> {
        if index == 0 || term == 0 {
            return Err("zero committed position".into());
        }
        self.bound(command.identity)?;
        match &command.operation {
            Operation::Begin(i) => {
                if let Some(old) = self.receipts.get(&i.id) {
                    return if old.intent == *i {
                        Ok(())
                    } else {
                        Err("intent identity conflict".into())
                    };
                }
                if i.id == 0
                    || i.epoch != self.epoch
                    || i.learner == 0
                    || self.voters.contains(&i.learner)
                    || self
                        .receipts
                        .values()
                        .any(|p| p.intent.learner == i.learner || p.intent.remove == i.learner)
                    || !self.voters.contains(&i.remove)
                    || self.active.is_some()
                    || self.epoch == u64::MAX
                    || self.receipts.len() >= self.capacity
                {
                    return Err("intent admission/epoch".into());
                }
                self.active = Some(i.id);
                self.receipts.insert(
                    i.id,
                    Progress {
                        intent: i.clone(),
                        phase: Phase::Planned,
                        added_at: 0,
                        readiness: None,
                    },
                );
            }
            Operation::Ready(r) => {
                let p = self.progress()?;
                if p.phase != Phase::CatchingUp
                    || r.intent != p.intent.id
                    || r.epoch != self.epoch
                    || r.term != term
                    || r.barrier < p.added_at
                    || r.barrier >= index
                    || r.durable_applied < r.barrier
                    || r.durable_applied >= index
                {
                    return Err("stale/incomplete readiness".into());
                }
                self.receipts.get_mut(&r.intent).ok_or("intent")?.readiness = Some(r.clone());
            }
            Operation::Fence { next_owner_epoch } => {
                if *next_owner_epoch <= self.identity.owner_epoch {
                    return Err("owner fence must advance".into());
                }
                self.identity.owner_epoch = *next_owner_epoch;
                if let Some(id) = self.active {
                    self.receipts.get_mut(&id).ok_or("intent")?.readiness = None;
                }
            }
        }
        Ok(())
    }
    /// Derive the only allowed configuration entry for this progress state.
    /// Compare the entire protobuf (including context) before applying it.
    pub fn configuration(&self, change: &Change) -> Result<ConfChangeV2> {
        self.bound(change.identity)?;
        let p = self.progress()?;
        if change.intent != p.intent.id || change.epoch != self.epoch {
            return Err("intent/epoch fence".into());
        }
        let mut cc = ConfChangeV2::default();
        let mut add = |node_id, change_type| {
            let mut c = ConfChangeSingle {
                node_id,
                ..Default::default()
            };
            c.set_change_type(change_type);
            cc.mut_changes().push(c);
        };
        match change.step {
            Step::AddLearner if p.phase == Phase::Planned => {
                add(p.intent.learner, ConfChangeType::AddLearnerNode)
            }
            Step::EnterJoint if p.phase == Phase::CatchingUp && p.readiness.is_some() => {
                add(p.intent.learner, ConfChangeType::AddNode);
                add(p.intent.remove, ConfChangeType::RemoveNode);
                cc.set_transition(ConfChangeTransition::Explicit);
            }
            Step::LeaveJoint if p.phase == Phase::Joint => {}
            _ => return Err("out-of-order configuration".into()),
        }
        cc.context = serde_json::to_vec(change)?.into();
        Ok(cc)
    }
    /// Durable status for a retried configuration command. A true result means
    /// this exact intent/epoch/step already passed its atomic applied barrier.
    pub fn change_applied(&self, change: &Change) -> Result<bool> {
        self.bound(change.identity)?;
        let p = self.receipts.get(&change.intent).ok_or("unknown intent")?;
        if p.intent.epoch != change.epoch {
            return Err("intent epoch conflict".into());
        }
        Ok(match change.step {
            Step::AddLearner => p.phase != Phase::Planned,
            Step::EnterJoint => matches!(p.phase, Phase::Joint | Phase::Complete),
            Step::LeaveJoint => p.phase == Phase::Complete,
        })
    }
    /// Called only after raft successfully applies the matching committed change.
    pub fn applied_configuration(
        &mut self,
        change: &Change,
        index: u64,
        conf: &ConfState,
    ) -> Result<()> {
        self.configuration(change)?;
        let mut next = self.clone();
        let p = next.receipts.get_mut(&change.intent).ok_or("intent")?;
        match change.step {
            Step::AddLearner => {
                p.phase = Phase::CatchingUp;
                p.added_at = index;
            }
            Step::EnterJoint => p.phase = Phase::Joint,
            Step::LeaveJoint => {
                p.phase = Phase::Complete;
                next.active = None;
                next.voters.retain(|id| *id != p.intent.remove);
                next.voters.push(p.intent.learner);
                next.voters.sort_unstable();
                next.epoch = next.epoch.checked_add(1).ok_or("epoch overflow")?;
            }
        }
        if !crate::durable::equal_conf(conf, &crate::durable::expected_conf(&next)?) {
            return Err("applied configuration differs from intended membership".into());
        }
        next.validate(index)?;
        *self = next;
        Ok(())
    }
}
