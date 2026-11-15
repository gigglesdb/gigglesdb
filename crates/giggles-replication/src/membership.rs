//! Deterministic committed-entry model for replacing a lost voter.
use raft::eraftpb::{
    ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, ConfState,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

pub const MEMBERSHIP_VERSION: u8 = 1;
pub const CATCH_UP_DISTANCE: u64 = 64;
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerRecord {
    pub id: u64,
    pub address: String,
    pub fingerprint: String,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Step {
    Enrolled,
    Learner { added_at: u64 },
    CaughtUp { added_at: u64, matched: u64 },
    Joint,
    Left,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Replacement {
    pub lost: u64,
    pub new: PeerRecord,
    pub step: Step,
    pub started_at: u64,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Membership {
    pub peers: BTreeMap<u64, PeerRecord>,
    pub retired: BTreeSet<u64>,
    pub retired_fingerprints: BTreeSet<String>,
    pub active: Option<Replacement>,
    pub completed: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdminCommand {
    Enrol { lost: u64, peer: PeerRecord },
    CaughtUp { node: u64, matched: u64 },
    Retire { node: u64 },
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Next {
    Idle,
    AddLearner,
    CatchUp { node: u64, added_at: u64 },
    EnterJoint,
    LeaveJoint,
    Retire { node: u64 },
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Refusal {
    Invalid(String),
    Encoding(&'static str),
}
impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(s) => f.write_str(s),
            Self::Encoding(s) => write!(f, "invalid membership encoding: {s}"),
        }
    }
}
impl std::error::Error for Refusal {}
fn refuse(s: String) -> Refusal {
    Refusal::Invalid(s)
}
impl Membership {
    pub fn is_retired(&self, id: u64) -> bool {
        self.retired.contains(&id)
    }
    pub fn apply_admin(
        &mut self,
        cmd: &AdminCommand,
        conf: &ConfState,
        index: u64,
    ) -> Result<(), Refusal> {
        match cmd {
            AdminCommand::Enrol { lost, peer } => {
                if let Some(a) = &self.active {
                    if a.lost == *lost && a.new == *peer {
                        return Ok(());
                    }
                    return Err(refuse(format!(
                        "replacement of {} by {} is active; cannot enrol {}",
                        a.lost, a.new.id, peer.id
                    )));
                }
                if !conf.voters_outgoing.is_empty() || !conf.learners.is_empty() {
                    return Err(refuse(format!(
                        "cannot replace voter {lost}: joint configuration or existing learners"
                    )));
                }
                if !conf.voters.contains(lost) {
                    return Err(refuse(format!("lost node {lost} is not a voter")));
                }
                if peer.id == 0
                    || conf.voters.contains(&peer.id)
                    || conf.learners.contains(&peer.id)
                    || self.peers.contains_key(&peer.id)
                    || self.retired.contains(&peer.id)
                {
                    return Err(refuse(format!(
                        "new node {} has a zero, existing or retired id",
                        peer.id
                    )));
                }
                if peer.fingerprint.len() != 64
                    || !peer
                        .fingerprint
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                {
                    return Err(refuse(format!(
                        "node {} fingerprint must be 64 lowercase hex characters",
                        peer.id
                    )));
                }
                if self.retired_fingerprints.contains(&peer.fingerprint)
                    || self
                        .peers
                        .values()
                        .any(|p| p.fingerprint == peer.fingerprint)
                {
                    return Err(refuse(format!(
                        "node {} fingerprint is existing or retired",
                        peer.id
                    )));
                }
                if peer.address.is_empty() || peer.address.len() > 255 {
                    return Err(refuse(format!(
                        "node {} address must contain 1..=255 bytes",
                        peer.id
                    )));
                }
                self.peers.insert(peer.id, peer.clone());
                self.active = Some(Replacement {
                    lost: *lost,
                    new: peer.clone(),
                    step: Step::Enrolled,
                    started_at: index,
                });
            }
            AdminCommand::CaughtUp { node, matched } => {
                let a = self
                    .active
                    .as_mut()
                    .ok_or_else(|| refuse(format!("node {node} has no active replacement")))?;
                if a.new.id == *node && matches!(a.step, Step::CaughtUp { .. }) {
                    return Ok(());
                }
                let Step::Learner { added_at } = a.step else {
                    return Err(refuse(format!(
                        "node {node} is not awaiting learner catch-up"
                    )));
                };
                if a.new.id != *node || *matched < added_at || *matched >= index {
                    return Err(refuse(format!(
                        "node {node} catch-up {matched} must match learner {} and satisfy {added_at} <= matched < {index}",
                        a.new.id
                    )));
                }
                a.step = Step::CaughtUp {
                    added_at,
                    matched: *matched,
                };
            }
            AdminCommand::Retire { node } => {
                let a = self
                    .active
                    .as_ref()
                    .ok_or_else(|| refuse(format!("node {node} has no active replacement")))?;
                if a.lost != *node || a.step != Step::Left {
                    return Err(refuse(format!(
                        "node {node} cannot retire before replacement of {} has left joint consensus",
                        a.lost
                    )));
                }
                let completed = self.completed.checked_add(1).ok_or_else(|| {
                    refuse(format!(
                        "cannot retire node {node}: completed counter overflow"
                    ))
                })?;
                self.retired.insert(*node);
                if let Some(p) = self.peers.remove(node) {
                    self.retired_fingerprints.insert(p.fingerprint);
                }
                self.active = None;
                self.completed = completed;
            }
        }
        Ok(())
    }
    pub fn expected_conf_change(&self) -> Option<ConfChangeV2> {
        let a = self.active.as_ref()?;
        let mut cc = ConfChangeV2::default();
        let mut add = |node_id, change_type| {
            let mut c = ConfChangeSingle {
                node_id,
                ..Default::default()
            };
            c.set_change_type(change_type);
            cc.mut_changes().push(c);
        };
        match a.step {
            Step::Enrolled => add(a.new.id, ConfChangeType::AddLearnerNode),
            Step::CaughtUp { .. } => {
                add(a.new.id, ConfChangeType::AddNode);
                add(a.lost, ConfChangeType::RemoveNode);
                cc.set_transition(ConfChangeTransition::Explicit);
            }
            Step::Joint => {}
            _ => return None,
        }
        Some(cc)
    }
    pub fn accept_conf_change(&mut self, cc: &ConfChangeV2, index: u64) -> bool {
        let Some(expected) = self.expected_conf_change() else {
            return false;
        };
        if cc.get_transition() != expected.get_transition()
            || cc.changes.len() != expected.changes.len()
            || cc
                .changes
                .iter()
                .zip(&expected.changes)
                .any(|(a, b)| a.node_id != b.node_id || a.get_change_type() != b.get_change_type())
        {
            return false;
        }
        if let Some(a) = &mut self.active {
            a.step = match a.step {
                Step::Enrolled => Step::Learner { added_at: index },
                Step::CaughtUp { .. } => Step::Joint,
                Step::Joint => Step::Left,
                _ => unreachable!(),
            };
        }
        true
    }
    pub fn next(&self) -> Next {
        let Some(a) = &self.active else {
            return Next::Idle;
        };
        match a.step {
            Step::Enrolled => Next::AddLearner,
            Step::Learner { added_at } => Next::CatchUp {
                node: a.new.id,
                added_at,
            },
            Step::CaughtUp { .. } => Next::EnterJoint,
            Step::Joint => Next::LeaveJoint,
            Step::Left => Next::Retire { node: a.lost },
        }
    }
    /// Version, peer map, retired ids, retired fingerprints, active, completed.
    /// Counts and integers are u64 BE; UTF-8 string lengths are u16 BE.
    pub fn encode(&self) -> Vec<u8> {
        let mut o = vec![MEMBERSHIP_VERSION];
        put_u64(&mut o, self.peers.len() as u64);
        for (id, p) in &self.peers {
            put_u64(&mut o, *id);
            put_peer(&mut o, p);
        }
        put_u64(&mut o, self.retired.len() as u64);
        for id in &self.retired {
            put_u64(&mut o, *id);
        }
        put_u64(&mut o, self.retired_fingerprints.len() as u64);
        for fp in &self.retired_fingerprints {
            put_str(&mut o, fp);
        }
        match &self.active {
            None => o.push(0),
            Some(a) => {
                o.push(1);
                put_u64(&mut o, a.lost);
                put_peer(&mut o, &a.new);
                match a.step {
                    Step::Enrolled => o.push(0),
                    Step::Learner { added_at } => {
                        o.push(1);
                        put_u64(&mut o, added_at);
                    }
                    Step::CaughtUp { added_at, matched } => {
                        o.push(2);
                        put_u64(&mut o, added_at);
                        put_u64(&mut o, matched);
                    }
                    Step::Joint => o.push(3),
                    Step::Left => o.push(4),
                }
                put_u64(&mut o, a.started_at);
            }
        }
        put_u64(&mut o, self.completed);
        o
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, Refusal> {
        let mut r = Reader(bytes);
        r.version()?;
        let mut v = Self::default();
        for _ in 0..r.u64()? {
            let id = r.u64()?;
            let p = r.peer()?;
            if v.peers.insert(id, p).is_some() {
                return Err(Refusal::Encoding("duplicate peer"));
            }
        }
        for _ in 0..r.u64()? {
            let id = r.u64()?;
            if !v.retired.insert(id) {
                return Err(Refusal::Encoding("duplicate retired id"));
            }
        }
        for _ in 0..r.u64()? {
            let fp = r.string()?;
            if !v.retired_fingerprints.insert(fp) {
                return Err(Refusal::Encoding("duplicate retired fingerprint"));
            }
        }
        v.active = match r.byte()? {
            0 => None,
            1 => {
                let lost = r.u64()?;
                let new = r.peer()?;
                let step = match r.byte()? {
                    0 => Step::Enrolled,
                    1 => Step::Learner { added_at: r.u64()? },
                    2 => Step::CaughtUp {
                        added_at: r.u64()?,
                        matched: r.u64()?,
                    },
                    3 => Step::Joint,
                    4 => Step::Left,
                    _ => return Err(Refusal::Encoding("unknown step tag")),
                };
                Some(Replacement {
                    lost,
                    new,
                    step,
                    started_at: r.u64()?,
                })
            }
            _ => return Err(Refusal::Encoding("unknown active tag")),
        };
        v.completed = r.u64()?;
        r.end()?;
        Ok(v)
    }
}
pub fn check_safe(
    conf: &ConfState,
    lost: u64,
    leader: u64,
    reachable: &BTreeSet<u64>,
) -> Result<(), Refusal> {
    if lost == leader {
        return Err(refuse(format!("lost voter {lost} is the current leader")));
    }
    let voters: BTreeSet<_> = conf
        .voters
        .iter()
        .chain(&conf.voters_outgoing)
        .copied()
        .collect();
    let count = voters
        .iter()
        .filter(|&&id| id != lost && reachable.contains(&id))
        .count();
    if count < 2 {
        return Err(refuse(format!(
            "would leave {count} reachable voter{}; at least 2 are required",
            if count == 1 { "" } else { "s" }
        )));
    }
    Ok(())
}
impl AdminCommand {
    pub fn encode(&self) -> Vec<u8> {
        let mut o = vec![MEMBERSHIP_VERSION];
        match self {
            Self::Enrol { lost, peer } => {
                o.push(1);
                put_u64(&mut o, *lost);
                put_peer(&mut o, peer);
            }
            Self::CaughtUp { node, matched } => {
                o.push(2);
                put_u64(&mut o, *node);
                put_u64(&mut o, *matched);
            }
            Self::Retire { node } => {
                o.push(3);
                put_u64(&mut o, *node);
            }
        }
        o
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, Refusal> {
        let mut r = Reader(bytes);
        r.version()?;
        let cmd = match r.byte()? {
            1 => Self::Enrol {
                lost: r.u64()?,
                peer: r.peer()?,
            },
            2 => Self::CaughtUp {
                node: r.u64()?,
                matched: r.u64()?,
            },
            3 => Self::Retire { node: r.u64()? },
            _ => return Err(Refusal::Encoding("unknown command tag")),
        };
        r.end()?;
        Ok(cmd)
    }
}
fn put_u64(o: &mut Vec<u8>, n: u64) {
    o.extend_from_slice(&n.to_be_bytes());
}
fn put_str(o: &mut Vec<u8>, s: &str) {
    let n = u16::try_from(s.len()).expect("membership string exceeds u16 length");
    o.extend_from_slice(&n.to_be_bytes());
    o.extend_from_slice(s.as_bytes());
}
fn put_peer(o: &mut Vec<u8>, p: &PeerRecord) {
    put_u64(o, p.id);
    put_str(o, &p.address);
    put_str(o, &p.fingerprint);
}
struct Reader<'a>(&'a [u8]);
impl Reader<'_> {
    fn take(&mut self, n: usize) -> Result<&[u8], Refusal> {
        if self.0.len() < n {
            return Err(Refusal::Encoding("truncated input"));
        }
        let (head, tail) = self.0.split_at(n);
        self.0 = tail;
        Ok(head)
    }
    fn byte(&mut self) -> Result<u8, Refusal> {
        Ok(self.take(1)?[0])
    }
    fn u64(&mut self) -> Result<u64, Refusal> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().expect("eight bytes"),
        ))
    }
    fn string(&mut self) -> Result<String, Refusal> {
        let n = u16::from_be_bytes(self.take(2)?.try_into().expect("two bytes")) as usize;
        String::from_utf8(self.take(n)?.to_vec()).map_err(|_| Refusal::Encoding("invalid UTF-8"))
    }
    fn peer(&mut self) -> Result<PeerRecord, Refusal> {
        Ok(PeerRecord {
            id: self.u64()?,
            address: self.string()?,
            fingerprint: self.string()?,
        })
    }
    fn version(&mut self) -> Result<(), Refusal> {
        if self.byte()? == MEMBERSHIP_VERSION {
            Ok(())
        } else {
            Err(Refusal::Encoding("unknown version"))
        }
    }
    fn end(&self) -> Result<(), Refusal> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(Refusal::Encoding("trailing bytes"))
        }
    }
}
