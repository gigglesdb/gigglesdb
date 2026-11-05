use crate::{Action, Code, Error, MAX_BYTES, Owner, Result};
use giggles_distributed::{Descriptor, Machine, runtime::Observation};
use giggles_membership::{Identity, Intent, Phase};
use giggles_topology::{Measurement, Member, Profile, Role, Topology, Validated, Verdict};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub version: u32,
    pub topology: Topology,
    pub topology_group: String,
    /// Exact numeric runtime identity -> declared topology node identity.
    pub nodes: BTreeMap<u64, String>,
    pub domain: u128,
    #[serde(deserialize_with = "strict_identity")]
    pub identity: Identity,
    #[serde(deserialize_with = "strict_intent")]
    pub intent: Intent,
    /// Explicit supplied allocation for the provisioned candidate.
    pub learner_capacity: Measurement,
}
fn strict_identity<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<Identity, D::Error> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Binding {
        group: u64,
        owner_epoch: u64,
    }
    let b = Binding::deserialize(d)?;
    Ok(Identity {
        group: b.group,
        owner_epoch: b.owner_epoch,
    })
}
fn strict_intent<'de, D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Intent, D::Error> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Replacement {
        id: u64,
        epoch: u64,
        learner: u64,
        remove: u64,
    }
    let r = Replacement::deserialize(d)?;
    Ok(Intent {
        id: r.id,
        epoch: r.epoch,
        learner: r.learner,
        remove: r.remove,
    })
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    pub version: u32,
    pub request: Request,
    pub initial_voters: Vec<u64>,
    /// Stable ownership descriptor, deliberately never rewritten on replacement.
    pub retained_home: Option<Descriptor>,
    pub term: u64,
    pub applied: u64,
    pub input_digest: String,
    pub digest: String,
}
fn hash<T: Serialize>(v: &T) -> Result<String> {
    let bytes = serde_json::to_vec(v).map_err(|e| Error::new(Code::StalePlan, e))?;
    if bytes.len() > MAX_BYTES {
        return Err(Error::new(Code::Limit, "serialized plan"));
    }
    Ok(hex::encode(Sha256::digest(bytes)))
}
fn topology_error(e: giggles_topology::Error) -> Error {
    Error::new(
        match e.code.as_str() {
            "residency" => Code::IneligibleResidency,
            "profile" => Code::IneligibleProfile,
            _ => Code::InvalidTopology,
        },
        e,
    )
}
fn eligible(t: Topology, group: &str) -> Result<Validated> {
    let v = Validated::new(t).map_err(topology_error)?;
    for scenario in v.analyze().scenarios {
        let g = scenario
            .groups
            .iter()
            .find(|g| g.id == group)
            .ok_or_else(|| Error::new(Code::InvalidTopology, "group missing"))?;
        if g.voter_arithmetic.verdict != Verdict::Pass {
            return Err(Error::new(
                Code::IneligibleProfile,
                format!("{}: voter survival", scenario.id),
            ));
        }
        match g.dependency_reachability.verdict {
            Verdict::Pass => {}
            Verdict::Fail => return Err(Error::new(Code::DependencyFailed, &scenario.id)),
            _ => return Err(Error::new(Code::DependencyUnknown, &scenario.id)),
        }
        match g.capacity.assessment.verdict {
            Verdict::Pass => {}
            Verdict::Fail => return Err(Error::new(Code::CapacityFailed, &scenario.id)),
            _ => return Err(Error::new(Code::CapacityUnknown, &scenario.id)),
        }
    }
    Ok(v)
}
impl Request {
    fn canonicalize(&mut self) -> Result<()> {
        self.topology = serde_json::from_str(
            &Validated::new(self.topology.clone())
                .map_err(topology_error)?
                .export_json(),
        )
        .map_err(|e| Error::new(Code::InvalidTopology, e))?;
        Ok(())
    }
    pub(crate) fn voters(&self) -> Result<Vec<u64>> {
        if self.version != 1
            || self.domain == 0
            || self.identity.group == 0
            || self.identity.owner_epoch == 0
            || self.intent.id == 0
            || self.intent.epoch == 0
            || self.intent.epoch == u64::MAX
            || self.intent.learner == self.intent.remove
            || self.nodes.len() > 256
            || self.nodes.contains_key(&0)
        {
            return Err(Error::new(
                Code::StalePlan,
                "version/identity/intent bounds",
            ));
        }
        let declared: BTreeSet<_> = self.topology.nodes.iter().map(|n| &n.id).collect();
        let mapped: BTreeSet<_> = self.nodes.values().collect();
        if mapped.len() != self.nodes.len() || mapped != declared {
            return Err(Error::new(
                Code::IdentityConflict,
                "node mapping must be an exact bijection",
            ));
        }
        let g = self
            .topology
            .groups
            .iter()
            .find(|g| g.id == self.topology_group)
            .ok_or_else(|| Error::new(Code::InvalidTopology, "unknown topology group"))?;
        let mut voters = Vec::new();
        for m in &g.members {
            if m.role != Role::FullDataVoter {
                return Err(Error::new(
                    Code::StalePlan,
                    "base profile must be stable without learners",
                ));
            }
            voters.push(
                *self
                    .nodes
                    .iter()
                    .find(|(_, name)| **name == m.node)
                    .ok_or_else(|| Error::new(Code::IdentityConflict, "unmapped member"))?
                    .0,
            );
        }
        voters.sort_unstable();
        if !voters.contains(&self.intent.remove)
            || voters.contains(&self.intent.learner)
            || !self.nodes.contains_key(&self.intent.learner)
        {
            return Err(Error::new(
                Code::StalePlan,
                "replacement is not a provisioned nonvoter for a current voter",
            ));
        }
        eligible(self.topology.clone(), &self.topology_group)?;
        let candidate = self.nodes[&self.intent.learner].clone();
        if self.learner_capacity.node != candidate {
            return Err(Error::new(
                Code::CapacityUnknown,
                "candidate allocation missing/binding",
            ));
        }
        let mut transient = self.topology.clone();
        let g = transient
            .groups
            .iter_mut()
            .find(|g| g.id == self.topology_group)
            .expect("validated group");
        g.profile = Profile::Majority;
        g.members.push(Member {
            node: candidate.clone(),
            role: Role::Learner,
        });
        // Static transient validation is residency/shape only, never joint quorum proof.
        // The learner is not serving capacity. Base and final serving allocations
        // are checked separately; summing both copies can spuriously overflow.
        g.capacity = None;
        Validated::new(transient).map_err(topology_error)?;
        let mut final_topology = self.topology.clone();
        let g = final_topology
            .groups
            .iter_mut()
            .find(|g| g.id == self.topology_group)
            .expect("validated group");
        let old = &self.nodes[&self.intent.remove];
        g.members.retain(|m| &m.node != old);
        g.members.push(Member {
            node: candidate,
            role: Role::FullDataVoter,
        });
        if let Some(c) = &mut g.capacity {
            c.measurements.retain(|m| &m.node != old);
            c.measurements.push(self.learner_capacity.clone());
        }
        eligible(final_topology, &self.topology_group)?;
        Ok(voters)
    }
}
fn home(o: &Observation) -> Option<Descriptor> {
    match &o.machine {
        Machine::Range { data, .. } => Some(data.descriptor.clone()),
        _ => None,
    }
}
pub(crate) fn current_binding(r: &Request, o: &Observation) -> Result<()> {
    if !o.is_leader || !o.quorum_checked || o.term == 0 || o.applied == 0 {
        return Err(Error::new(
            Code::NoQuorum,
            "fresh leader quorum current required",
        ));
    }
    if o.group != r.identity.group
        || o.machine.group() != o.group
        || o.machine
            .domain()
            .map_err(|e| Error::new(Code::IdentityConflict, e))?
            != r.domain
        || o.membership.identity != r.identity
    {
        return Err(Error::new(
            Code::IdentityConflict,
            "domain/group/owner identity changed",
        ));
    }
    o.membership
        .validate(o.applied)
        .map_err(|e| Error::new(Code::StalePlan, e))?;
    Ok(())
}
impl Plan {
    /// Obtain authority directly from an owner adapter, never a diagnostic file.
    pub fn create(mut request: Request, owner: &mut impl Owner) -> Result<Self> {
        request.canonicalize()?;
        let voters = request.voters()?;
        let o = owner.current(request.identity.group)?;
        current_binding(&request, &o)?;
        let mut live = o.membership.voters.clone();
        live.sort_unstable();
        if live != voters
            || o.membership.epoch != request.intent.epoch
            || o.membership.active.is_some()
            || o.membership.receipts.contains_key(&request.intent.id)
        {
            return Err(Error::new(
                Code::StalePlan,
                "base membership/epoch/intent changed",
            ));
        }
        let mut p = Self {
            version: 1,
            input_digest: hash(&request)?,
            request,
            initial_voters: voters,
            retained_home: home(&o),
            term: o.term,
            applied: o.applied,
            digest: String::new(),
        };
        p.digest = p.calculate_digest()?;
        Ok(p)
    }
    fn calculate_digest(&self) -> Result<String> {
        let mut p = self.clone();
        p.digest.clear();
        hash(&p)
    }
    pub fn validate(&self) -> Result<()> {
        if self.version != 1
            || self.digest != self.calculate_digest()?
            || self.input_digest != hash(&self.request)?
            || self.term == 0
            || self.applied == 0
            || self.initial_voters != self.request.voters()?
        {
            return Err(Error::new(
                Code::StalePlan,
                "plan digest/input/frontier mismatch",
            ));
        }
        Ok(())
    }
    pub(crate) fn next(&self, o: &Observation) -> Result<Option<Action>> {
        current_binding(&self.request, o)?;
        if o.term < self.term || o.applied < self.applied || home(o) != self.retained_home {
            return Err(Error::new(
                Code::StalePlan,
                "current frontier regressed or retained home changed",
            ));
        }
        let m = &o.membership;
        let i = &self.request.intent;
        let mut voters = m.voters.clone();
        voters.sort_unstable();
        if let Some(p) = m.receipts.get(&i.id) {
            if p.intent != *i {
                return Err(Error::new(
                    Code::IdentityConflict,
                    "intent receipt payload conflict",
                ));
            }
            if p.phase == Phase::Complete {
                let expected: BTreeSet<_> = self
                    .initial_voters
                    .iter()
                    .copied()
                    .filter(|n| *n != i.remove)
                    .chain([i.learner])
                    .collect();
                if voters.into_iter().collect::<BTreeSet<_>>() != expected
                    || m.epoch
                        != i.epoch
                            .checked_add(1)
                            .ok_or_else(|| Error::new(Code::StalePlan, "epoch overflow"))?
                {
                    return Err(Error::new(
                        Code::StalePlan,
                        "completed placement superseded",
                    ));
                }
                return Ok(None);
            }
            if m.active != Some(i.id) || m.epoch != i.epoch || voters != self.initial_voters {
                return Err(Error::new(Code::StalePlan, "active membership changed"));
            }
            return Ok(Some(Action::Resume {
                group: o.group,
                id: i.id,
            }));
        }
        if m.active.is_some() || m.epoch != i.epoch || voters != self.initial_voters {
            return Err(Error::new(
                Code::StalePlan,
                "membership changed since planning",
            ));
        }
        Ok(Some(Action::Begin {
            group: o.group,
            intent: i.clone(),
        }))
    }
}
