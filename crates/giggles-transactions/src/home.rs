//! Decision rules of one transaction's home record, without consensus or evidence.
//!
//! [`HomeRecord`] holds the participant set, the seal, one vote per participant and
//! the decision. [`crate::Machine`] uses it for its home records with evidence-carrying
//! seal, vote and decision types; an embedding with its own replication (for example
//! a range-local record written through a range's log) can use it with plain values,
//! such as a vote that is just a prepared timestamp.
//!
//! The rules:
//! - participants are registered until the record is sealed or decided;
//! - the seal is taken once; a retry returns the existing seal, and sealing an exact
//!   set refuses a different set;
//! - a vote needs a matching seal and a sealed participant; an identical retry is
//!   idempotent, a different vote from the same participant is refused, and a new
//!   vote after the decision is refused;
//! - the decision is taken once; a retry with the same outcome returns the existing
//!   decision, the opposite outcome is refused; commit needs a seal and every vote.
//!
//! The record is pure state: the embedding persists it atomically with whatever it
//! applies, and absence of a record is never an abort certificate.

use crate::types::{DecisionRef, Error, Outcome};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// The outcome a stored decision carries.
pub trait HomeDecision {
    fn outcome(&self) -> Outcome;
}

impl HomeDecision for Outcome {
    fn outcome(&self) -> Outcome {
        *self
    }
}

impl HomeDecision for DecisionRef {
    fn outcome(&self) -> Outcome {
        self.outcome
    }
}

/// Where a home record stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HomePhase {
    /// Accepting participants.
    Open,
    /// Participant set fixed; collecting votes.
    Sealed,
    /// Decided; the outcome never changes.
    Decided(Outcome),
}

/// One transaction's home record: participants `P`, votes `V`, seal `S`, decision `D`.
///
/// Fields are private so the rules above hold for every value built through the
/// methods. A deserialized record must be checked with [`HomeRecord::validate`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HomeRecord<P, V, S, D>
where
    P: Ord,
{
    pub(crate) participants: BTreeSet<P>,
    pub(crate) seal: Option<S>,
    pub(crate) votes: BTreeMap<P, V>,
    pub(crate) decision: Option<D>,
}

impl<P: Ord, V, S, D> Default for HomeRecord<P, V, S, D> {
    fn default() -> Self {
        Self {
            participants: BTreeSet::new(),
            seal: None,
            votes: BTreeMap::new(),
            decision: None,
        }
    }
}

impl<P, V, S, D> HomeRecord<P, V, S, D>
where
    P: Ord,
    V: Eq,
    D: HomeDecision,
{
    /// An open record with no participants.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn participants(&self) -> &BTreeSet<P> {
        &self.participants
    }
    pub fn seal(&self) -> Option<&S> {
        self.seal.as_ref()
    }
    pub fn votes(&self) -> &BTreeMap<P, V> {
        &self.votes
    }
    pub fn decision(&self) -> Option<&D> {
        self.decision.as_ref()
    }

    pub fn phase(&self) -> HomePhase {
        match (&self.decision, &self.seal) {
            (Some(d), _) => HomePhase::Decided(d.outcome()),
            (None, Some(_)) => HomePhase::Sealed,
            (None, None) => HomePhase::Open,
        }
    }

    /// True when the record is sealed and every sealed participant has voted.
    pub fn all_voted(&self) -> bool {
        self.seal.is_some() && self.votes.len() == self.participants.len()
    }

    /// Sealed participants that have not voted yet (empty before the seal).
    pub fn missing_votes(&self) -> impl Iterator<Item = &P> {
        let sealed = self.seal.is_some();
        self.participants
            .iter()
            .filter(move |p| sealed && !self.votes.contains_key(p))
    }

    /// Registers one participant. `Ok(true)` when added, `Ok(false)` when it was
    /// already registered. `admit` sees the current set first and may refuse.
    pub fn register(
        &mut self,
        participant: P,
        cap: usize,
        admit: impl FnOnce(&BTreeSet<P>, &P) -> Result<(), Error>,
    ) -> Result<bool, Error> {
        if self.participants.contains(&participant) {
            return Ok(false);
        }
        if self.seal.is_some() || self.decision.is_some() {
            return Err(Error::WrongPhase);
        }
        admit(&self.participants, &participant)?;
        if self.participants.len() >= cap {
            return Err(Error::Capacity);
        }
        self.participants.insert(participant);
        Ok(true)
    }

    /// Seals the registered set once. A retry returns the existing seal, even after
    /// the decision.
    pub fn seal_with(
        &mut self,
        make: impl FnOnce(&BTreeSet<P>) -> Result<S, Error>,
    ) -> Result<&S, Error> {
        if self.seal.is_none() {
            if self.decision.is_some() {
                return Err(Error::WrongPhase);
            }
            if self.participants.is_empty() {
                return Err(Error::MissingParticipants);
            }
            self.seal = Some(make(&self.participants)?);
        }
        Ok(self.seal.as_ref().expect("sealed above"))
    }

    /// Registers and seals exactly `set` at once. A retry with the same set returns
    /// the existing seal; a different set is a conflict. Participants registered
    /// earlier must all be in `set`.
    pub fn seal_set(
        &mut self,
        set: BTreeSet<P>,
        cap: usize,
        make: impl FnOnce(&BTreeSet<P>) -> Result<S, Error>,
    ) -> Result<&S, Error> {
        if self.seal.is_none() {
            if self.decision.is_some() {
                return Err(Error::WrongPhase);
            }
            if set.is_empty() {
                return Err(Error::MissingParticipants);
            }
            if set.len() > cap {
                return Err(Error::Capacity);
            }
            if !self.participants.is_subset(&set) {
                return Err(Error::Conflict);
            }
            let seal = make(&set)?;
            self.participants = set;
            self.seal = Some(seal);
        } else if self.participants != set {
            return Err(Error::Conflict);
        }
        Ok(self.seal.as_ref().expect("sealed above"))
    }

    /// Records `participant`'s vote. `Ok(true)` when recorded, `Ok(false)` for an
    /// identical retry. `seal_matches` checks the vote's view of the seal; `accept`
    /// checks anything else (evidence) just before a new vote is stored.
    pub fn vote(
        &mut self,
        participant: P,
        vote: V,
        seal_matches: impl FnOnce(&S) -> bool,
        accept: impl FnOnce(&V) -> Result<(), Error>,
    ) -> Result<bool, Error> {
        if !self.seal.as_ref().is_some_and(seal_matches)
            || !self.participants.contains(&participant)
        {
            return Err(Error::Conflict);
        }
        if let Some(old) = self.votes.get(&participant) {
            return if *old == vote {
                Ok(false)
            } else {
                Err(Error::Conflict)
            };
        }
        if self.decision.is_some() {
            return Err(Error::WrongPhase);
        }
        accept(&vote)?;
        self.votes.insert(participant, vote);
        Ok(true)
    }

    /// Decides once. A retry with the same outcome returns the stored decision (and
    /// does not call `make`); the opposite outcome is a conflict. Commit needs the
    /// seal and every vote.
    pub fn decide(&mut self, outcome: Outcome, make: impl FnOnce() -> D) -> Result<&D, Error> {
        if let Some(d) = &self.decision {
            return if d.outcome() == outcome {
                Ok(self.decision.as_ref().expect("checked"))
            } else {
                Err(Error::Conflict)
            };
        }
        if outcome == Outcome::Commit && !self.all_voted() {
            return Err(Error::MissingParticipants);
        }
        self.decision = Some(make());
        Ok(self.decision.as_ref().expect("decided above"))
    }

    /// Checks the invariants the methods keep, for a record read back from storage:
    /// votes only from sealed participants, and commit only with every vote.
    pub fn validate(&self, cap: usize) -> Result<(), Error> {
        if self.participants.len() > cap
            || (self.seal.is_none() && !self.votes.is_empty())
            || self.votes.keys().any(|p| !self.participants.contains(p))
            || self
                .decision
                .as_ref()
                .is_some_and(|d| d.outcome() == Outcome::Commit && !self.all_voted())
        {
            return Err(Error::Invalid);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The shape an embedding without evidence uses: range ids, prepared timestamps.
    type Plain = HomeRecord<u64, u64, (), Outcome>;

    fn sealed(set: &[u64]) -> Plain {
        let mut h = Plain::new();
        h.seal_set(set.iter().copied().collect(), 8, |_| Ok(()))
            .unwrap();
        h
    }
    fn vote(h: &mut Plain, p: u64, ts: u64) -> Result<bool, Error> {
        h.vote(p, ts, |_| true, |_| Ok(()))
    }

    #[test]
    fn decide_once_and_identical_retry_is_idempotent() {
        let mut h = sealed(&[1, 2]);
        assert_eq!(vote(&mut h, 1, 10), Ok(true));
        assert_eq!(vote(&mut h, 2, 11), Ok(true));
        assert_eq!(
            h.decide(Outcome::Commit, || Outcome::Commit),
            Ok(&Outcome::Commit)
        );
        let mut called = false;
        assert_eq!(
            h.decide(Outcome::Commit, || {
                called = true;
                Outcome::Commit
            }),
            Ok(&Outcome::Commit)
        );
        assert!(!called, "a retry never rebuilds the decision");
        assert_eq!(h.phase(), HomePhase::Decided(Outcome::Commit));
    }

    #[test]
    fn opposite_decision_is_refused_both_ways() {
        let mut h = sealed(&[1]);
        vote(&mut h, 1, 5).unwrap();
        h.decide(Outcome::Commit, || Outcome::Commit).unwrap();
        assert_eq!(
            h.decide(Outcome::Abort, || Outcome::Abort),
            Err(Error::Conflict)
        );
        let mut h = Plain::new();
        h.decide(Outcome::Abort, || Outcome::Abort).unwrap();
        assert_eq!(
            h.decide(Outcome::Commit, || Outcome::Commit),
            Err(Error::Conflict)
        );
        assert_eq!(h.phase(), HomePhase::Decided(Outcome::Abort));
    }

    #[test]
    fn commit_needs_seal_and_every_vote() {
        let mut h = Plain::new();
        assert_eq!(
            h.decide(Outcome::Commit, || Outcome::Commit),
            Err(Error::MissingParticipants)
        );
        let mut h = sealed(&[1, 2]);
        vote(&mut h, 1, 3).unwrap();
        assert_eq!(h.missing_votes().copied().collect::<Vec<_>>(), vec![2]);
        assert_eq!(
            h.decide(Outcome::Commit, || Outcome::Commit),
            Err(Error::MissingParticipants)
        );
        assert_eq!(h.decision(), None);
    }

    #[test]
    fn seal_once_with_exact_set() {
        let mut h = sealed(&[1, 2]);
        assert!(h.seal_set([1, 2].into(), 8, |_| Ok(())).is_ok());
        assert_eq!(
            h.seal_set([1, 3].into(), 8, |_| Ok(())),
            Err(Error::Conflict)
        );
        assert_eq!(h.seal_set([1].into(), 8, |_| Ok(())), Err(Error::Conflict));
        assert_eq!(h.register(3, 8, |_, _| Ok(())), Err(Error::WrongPhase));
        assert_eq!(h.register(1, 8, |_, _| Ok(())), Ok(false));
        let mut h = Plain::new();
        assert_eq!(
            h.seal_set(BTreeSet::new(), 8, |_| Ok(())),
            Err(Error::MissingParticipants)
        );
        assert_eq!(
            h.seal_set([1, 2, 3].into(), 2, |_| Ok(())),
            Err(Error::Capacity)
        );
        h.register(4, 8, |_, _| Ok(())).unwrap();
        assert_eq!(
            h.seal_set([1, 2].into(), 8, |_| Ok(())),
            Err(Error::Conflict)
        );
        assert!(h.seal_set([1, 4].into(), 8, |_| Ok(())).is_ok());
        let mut h = Plain::new();
        h.decide(Outcome::Abort, || Outcome::Abort).unwrap();
        assert_eq!(
            h.seal_set([1].into(), 8, |_| Ok(())),
            Err(Error::WrongPhase)
        );
        assert_eq!(h.seal_with(|_| Ok(())), Err(Error::WrongPhase));
    }

    #[test]
    fn seal_with_returns_existing_seal_and_needs_participants() {
        let mut h: HomeRecord<u64, u64, u32, Outcome> = HomeRecord::new();
        assert_eq!(h.seal_with(|_| Ok(7)), Err(Error::MissingParticipants));
        h.register(1, 8, |_, _| Ok(())).unwrap();
        assert_eq!(h.seal_with(|_| Ok(7)), Ok(&7));
        assert_eq!(h.seal_with(|_| Ok(8)), Ok(&7));
        h.decide(Outcome::Abort, || Outcome::Abort).unwrap();
        assert_eq!(h.seal_with(|_| Ok(9)), Ok(&7));
    }

    #[test]
    fn votes_need_seal_membership_and_are_fixed() {
        let mut h = Plain::new();
        h.register(1, 8, |_, _| Ok(())).unwrap();
        assert_eq!(vote(&mut h, 1, 4), Err(Error::Conflict), "vote before seal");
        let mut h = sealed(&[1, 2]);
        assert_eq!(
            vote(&mut h, 9, 4),
            Err(Error::Conflict),
            "not a participant"
        );
        assert_eq!(
            h.vote(1, 4, |_| false, |_| Ok(())),
            Err(Error::Conflict),
            "seal mismatch"
        );
        assert_eq!(
            h.vote(1, 4, |_| true, |_| Err(Error::MissingEvidence)),
            Err(Error::MissingEvidence)
        );
        assert_eq!(vote(&mut h, 1, 4), Ok(true));
        assert_eq!(vote(&mut h, 1, 4), Ok(false));
        assert_eq!(vote(&mut h, 1, 5), Err(Error::Conflict));
        h.decide(Outcome::Abort, || Outcome::Abort).unwrap();
        assert_eq!(
            vote(&mut h, 2, 6),
            Err(Error::WrongPhase),
            "new vote after decision"
        );
        assert_eq!(
            vote(&mut h, 1, 4),
            Ok(false),
            "old vote retry after decision"
        );
    }

    #[test]
    fn register_admission_and_capacity() {
        let mut h = Plain::new();
        assert_eq!(h.register(1, 1, |_, _| Ok(())), Ok(true));
        assert_eq!(h.register(2, 1, |_, _| Ok(())), Err(Error::Capacity));
        assert_eq!(
            h.register(2, 8, |_, _| Err(Error::Conflict)),
            Err(Error::Conflict)
        );
        assert_eq!(h.phase(), HomePhase::Open);
    }

    #[test]
    fn validate_rejects_impossible_records() {
        let mut h = sealed(&[1, 2]);
        vote(&mut h, 1, 1).unwrap();
        assert_eq!(h.validate(8), Ok(()));
        assert_eq!(h.validate(1), Err(Error::Invalid));
        let mut bad = h.clone();
        bad.decision = Some(Outcome::Commit);
        assert_eq!(bad.validate(8), Err(Error::Invalid));
        let mut bad = h.clone();
        bad.votes.insert(9, 1);
        assert_eq!(bad.validate(8), Err(Error::Invalid));
        let mut bad = h.clone();
        bad.seal = None;
        assert_eq!(bad.validate(8), Err(Error::Invalid));
        let bytes = bincode::serialize(&h).unwrap();
        let back: Plain = bincode::deserialize(&bytes).unwrap();
        assert_eq!(back, h);
    }
}
