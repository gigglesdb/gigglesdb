use crate::types::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// Trusted, committed seal evidence. No public constructor or deserializer exists.
#[derive(Clone, Debug)]
pub struct SealEvidence {
    seal: SealRef,
    participants: Vec<Domain>,
}
/// Trusted, committed participant prepare evidence.
#[derive(Clone, Debug)]
pub struct PrepareEvidence {
    reference: PrepareRef,
}
/// Trusted, committed home outcome evidence.
#[derive(Clone, Debug)]
pub struct DecisionEvidence {
    reference: DecisionRef,
    seal: Option<SealRef>,
    participants: Vec<Domain>,
}

/// Trusted consensus adapter, never a client-supplied quorum-claim validator.
///
/// Resolve only from authenticated, committed and durably applied state, after
/// authoritative read barriers as needed. Each return value must come from the
/// corresponding Machine evidence method. Unavailable evidence returns None;
/// absence never implies abort. The caller must pin these facts for deterministic
/// replay (for example, include authenticated certificates in the consensus layer
/// and reconstruct the same committed view). Replicas must not nondeterministically
/// query a changing network inside state-machine application.
pub trait CommittedView {
    fn seal(&self, reference: &SealRef) -> Option<SealEvidence>;
    fn prepare(&self, reference: &PrepareRef) -> Option<PrepareEvidence>;
    fn decision(&self, reference: &DecisionRef) -> Option<DecisionEvidence>;
}

/// Fails closed when no authenticated committed evidence is available.
pub struct NoEvidence;
impl CommittedView for NoEvidence {
    fn seal(&self, _: &SealRef) -> Option<SealEvidence> {
        None
    }
    fn prepare(&self, _: &PrepareRef) -> Option<PrepareEvidence> {
        None
    }
    fn decision(&self, _: &DecisionRef) -> Option<DecisionEvidence> {
        None
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Home {
    digest: Digest,
    participants: BTreeSet<Domain>,
    seal: Option<SealRef>,
    votes: BTreeMap<Domain, PrepareRef>,
    decision: Option<DecisionRef>,
}

/// Terminal records are retained and prevent conflicting retries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Phase {
    Active,
    Prepared,
    RolledBack,
    Committed,
    Aborted,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Participant {
    seal: SealRef,
    participants: Vec<Domain>,
    phase: Phase,
    locks: Vec<Lock>,
    intents: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    prepare_index: Option<u64>,
    decision: Option<DecisionRef>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Waiter {
    transaction: TransactionId,
    lock: Lock,
}

/// One exact range/epoch's deterministic state. Fields are private to enforce invariants.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Machine {
    version: u32,
    domain: Domain,
    limits: Limits,
    applied: u64,
    last: Option<(Digest, Result<Response, Error>)>,
    homes: BTreeMap<TransactionId, Home>,
    participants: BTreeMap<TransactionId, Participant>,
    waiters: VecDeque<Waiter>,
}

// Deliberately private: public machines cannot be deserialized into evidence issuers.
#[derive(Deserialize)]
struct WireMachine {
    version: u32,
    domain: Domain,
    limits: Limits,
    applied: u64,
    last: Option<(Digest, Result<Response, Error>)>,
    homes: BTreeMap<TransactionId, Home>,
    participants: BTreeMap<TransactionId, Participant>,
    waiters: VecDeque<Waiter>,
}

impl From<WireMachine> for Machine {
    fn from(w: WireMachine) -> Self {
        Self {
            version: w.version,
            domain: w.domain,
            limits: w.limits,
            applied: w.applied,
            last: w.last,
            homes: w.homes,
            participants: w.participants,
            waiters: w.waiters,
        }
    }
}

impl Machine {
    pub fn new(domain: Domain, limits: Limits) -> Result<Self, Error> {
        limits.validate()?;
        if !domain.valid() {
            return Err(Error::WrongDomain);
        }
        Ok(Self {
            version: 1,
            domain,
            limits,
            applied: 0,
            last: None,
            homes: BTreeMap::new(),
            participants: BTreeMap::new(),
            waiters: VecDeque::new(),
        })
    }

    pub fn domain(&self) -> Domain {
        self.domain
    }
    pub fn applied_index(&self) -> u64 {
        self.applied
    }
    pub fn limits(&self) -> &Limits {
        &self.limits
    }
    pub fn phase(&self, transaction: TransactionId) -> Option<Phase> {
        self.participants.get(&transaction).map(|p| p.phase)
    }
    pub fn outcome(&self, transaction: TransactionId) -> Option<&DecisionRef> {
        self.homes
            .get(&transaction)
            .and_then(|h| h.decision.as_ref())
    }

    /// Split/drain guard. Includes unprepared work and queued waiters.
    pub fn has_unresolved_work(&self) -> bool {
        self.participants
            .values()
            .any(|p| matches!(p.phase, Phase::Active | Phase::Prepared))
            || self.homes.values().any(|h| h.decision.is_none())
    }

    /// True only when this transaction currently holds coverage for the entire span.
    /// The adapter must still validate its authoritative data read barrier.
    pub fn holds(&self, transaction: TransactionId, span: &Span, mode: Mode) -> bool {
        if span.validate(&self.limits).is_err() {
            return false;
        }
        self.participants.get(&transaction).is_some_and(|p| {
            matches!(p.phase, Phase::Active | Phase::Prepared)
                && p.locks.iter().any(|l| {
                    (mode == Mode::Shared || l.mode == Mode::Exclusive) && covers(&l.span, span)
                })
        })
    }

    /// Apply one committed entry. Gaps are allowed for non-transaction log entries.
    ///
    /// Errors at a new index are themselves deterministic applied results; only
    /// index/order/encoding errors leave the applied index unchanged. A byte-identical
    /// latest-index replay returns its cached result. The embedding must use its
    /// durable applied index to avoid applying cached external redo twice.
    pub fn apply_committed(
        &mut self,
        index: u64,
        command: Command,
        view: &impl CommittedView,
    ) -> Result<Response, Error> {
        let bytes = encode_command(&command, &self.limits)?;
        let fingerprint = hash(b"giggles-transactions/command/v1", &bytes);
        if index == self.applied && index != 0 {
            return match &self.last {
                Some((h, result)) if *h == fingerprint => result.clone(),
                _ => Err(Error::ConflictingIndex),
            };
        }
        if index <= self.applied {
            return Err(Error::StaleIndex);
        }
        let mut next = self.clone();
        let mut result = next.transition(index, command, view);
        if result.is_ok() {
            next.applied = index;
            next.last = Some((fingerprint, result.clone()));
            if next
                .snapshot()
                .and_then(|bytes| {
                    let reserve = next.resolution_reserve()?;
                    if bytes
                        .len()
                        .checked_add(reserve)
                        .is_none_or(|n| n > next.limits.encoded_bytes)
                    {
                        Err(Error::Capacity)
                    } else {
                        Ok(())
                    }
                })
                .is_err()
            {
                result = Err(Error::Capacity);
            }
        }
        if result.is_ok() {
            *self = next;
        } else {
            self.applied = index;
            self.last = Some((fingerprint, result.clone()));
        }
        result
    }

    // Admission reserves space for all already registered votes, terminal records
    // and the largest retained redo reply. Resource exhaustion must not strand a
    // prepared participant by denying its subsequent durable resolution.
    fn resolution_reserve(&self) -> Result<usize, Error> {
        let mut reserve = 0;
        let mut reply = 256;
        for h in self.homes.values().filter(|h| h.decision.is_none()) {
            reserve += 256
                + usize::from(h.seal.is_none()) * 256
                + (h.participants.len() - h.votes.len()) * 256;
        }
        for p in self
            .participants
            .values()
            .filter(|p| matches!(p.phase, Phase::Active | Phase::Prepared))
        {
            reserve += 256 + usize::from(p.prepare_index.is_none()) * 16;
            let intents: Vec<_> = p
                .intents
                .iter()
                .map(|(k, v)| Intent {
                    key: k.clone(),
                    value: v.clone(),
                })
                .collect();
            let response: Option<(Digest, Result<Response, Error>)> = Some((
                [0; 32],
                Ok(Response::Finalized {
                    outcome: Outcome::Commit,
                    intents,
                }),
            ));
            reply = reply.max(encode(&response, self.limits.encoded_bytes)?.len());
        }
        let current = encode(&self.last, self.limits.encoded_bytes)?.len();
        Ok(reserve + reply.saturating_sub(current))
    }

    fn valid_transaction(&self, t: TransactionId) -> Result<(), Error> {
        if !t.home.valid() || t.nonce == 0 || t.home.database != self.domain.database {
            Err(Error::WrongDomain)
        } else {
            Ok(())
        }
    }

    fn capacity(&self) -> Result<(), Error> {
        if self.homes.len() + self.participants.len() >= self.limits.transactions {
            Err(Error::Capacity)
        } else {
            Ok(())
        }
    }

    fn home_mut(&mut self, t: TransactionId, digest: Digest) -> Result<&mut Home, Error> {
        self.valid_transaction(t)?;
        if t.home != self.domain {
            return Err(Error::WrongDomain);
        }
        let h = self.homes.get_mut(&t).ok_or(Error::Missing)?;
        if h.digest != digest {
            return Err(Error::Conflict);
        }
        Ok(h)
    }

    fn transition(
        &mut self,
        index: u64,
        command: Command,
        view: &impl CommittedView,
    ) -> Result<Response, Error> {
        match command {
            Command::OpenHome {
                transaction,
                digest,
            } => {
                self.valid_transaction(transaction)?;
                if transaction.home != self.domain {
                    return Err(Error::WrongDomain);
                }
                if let Some(h) = self.homes.get(&transaction) {
                    return if h.digest == digest {
                        Ok(Response::Applied)
                    } else {
                        Err(Error::Conflict)
                    };
                }
                self.capacity()?;
                self.homes.insert(
                    transaction,
                    Home {
                        digest,
                        participants: BTreeSet::new(),
                        seal: None,
                        votes: BTreeMap::new(),
                        decision: None,
                    },
                );
                Ok(Response::Applied)
            }
            Command::Register {
                transaction,
                digest,
                participant,
            } => {
                if !participant.valid() || participant.database != self.domain.database {
                    return Err(Error::WrongDomain);
                }
                let cap = self.limits.participants;
                let h = self.home_mut(transaction, digest)?;
                if h.participants.contains(&participant) {
                    return Ok(Response::Applied);
                }
                if h.seal.is_some() || h.decision.is_some() {
                    return Err(Error::WrongPhase);
                }
                // One range cannot appear with two ownership epochs.
                if h.participants.iter().any(|p| p.range == participant.range) {
                    return Err(Error::Conflict);
                }
                if h.participants.len() >= cap {
                    return Err(Error::Capacity);
                }
                h.participants.insert(participant);
                Ok(Response::Applied)
            }
            Command::Seal {
                transaction,
                digest,
            } => {
                let cap = self.limits.encoded_bytes;
                let h = self.home_mut(transaction, digest)?;
                if let Some(seal) = &h.seal {
                    return Ok(Response::Seal(seal.clone()));
                }
                if h.decision.is_some() {
                    return Err(Error::WrongPhase);
                }
                if h.participants.is_empty() {
                    return Err(Error::MissingParticipants);
                }
                let seal = SealRef {
                    transaction,
                    digest,
                    index,
                    participants_digest: participant_digest(&h.participants, cap)?,
                };
                h.seal = Some(seal.clone());
                Ok(Response::Seal(seal))
            }
            Command::RecordPrepare(reference) => {
                let h = self.home_mut(reference.seal.transaction, reference.seal.digest)?;
                if h.seal.as_ref() != Some(&reference.seal)
                    || !h.participants.contains(&reference.participant)
                {
                    return Err(Error::Conflict);
                }
                if let Some(old) = h.votes.get(&reference.participant) {
                    return if old == &reference {
                        Ok(Response::Applied)
                    } else {
                        Err(Error::Conflict)
                    };
                }
                if h.decision.is_some() {
                    return Err(Error::WrongPhase);
                }
                let proof = view.prepare(&reference).ok_or(Error::MissingEvidence)?;
                if proof.reference != reference || reference.index == 0 {
                    return Err(Error::Conflict);
                }
                h.votes.insert(reference.participant, reference);
                Ok(Response::Applied)
            }
            Command::Decide {
                transaction,
                digest,
                outcome,
            } => {
                let h = self.home_mut(transaction, digest)?;
                if let Some(d) = &h.decision {
                    return if d.outcome == outcome {
                        Ok(Response::Decision(d.clone()))
                    } else {
                        Err(Error::Conflict)
                    };
                }
                if outcome == Outcome::Commit
                    && (h.seal.is_none() || h.votes.len() != h.participants.len())
                {
                    return Err(Error::MissingParticipants);
                }
                let reference = DecisionRef {
                    transaction,
                    digest,
                    index,
                    outcome,
                };
                h.decision = Some(reference.clone());
                Ok(Response::Decision(reference))
            }
            Command::Begin(seal) => {
                self.valid_transaction(seal.transaction)?;
                if let Some(p) = self.participants.get(&seal.transaction) {
                    return if p.seal == seal && matches!(p.phase, Phase::Active | Phase::Prepared) {
                        Ok(Response::Applied)
                    } else {
                        Err(Error::Conflict)
                    };
                }
                self.capacity()?;
                let proof = view.seal(&seal).ok_or(Error::MissingEvidence)?;
                if proof.participants.len() > self.limits.participants {
                    return Err(Error::Capacity);
                }
                if proof.seal != seal || !proof.participants.contains(&self.domain) {
                    return Err(Error::WrongDomain);
                }
                self.participants.insert(
                    seal.transaction,
                    Participant {
                        seal,
                        participants: proof.participants,
                        phase: Phase::Active,
                        locks: Vec::new(),
                        intents: BTreeMap::new(),
                        prepare_index: None,
                        decision: None,
                    },
                );
                Ok(Response::Applied)
            }
            Command::Acquire { transaction, lock } => self.acquire(transaction, lock),
            Command::Stage {
                transaction,
                intent,
            } => {
                self.valid_transaction(transaction)?;
                Span::Point(intent.key.clone()).validate(&self.limits)?;
                if intent
                    .value
                    .as_ref()
                    .is_some_and(|v| v.len() > self.limits.value_bytes)
                {
                    return Err(Error::Capacity);
                }
                if self.queued(transaction) {
                    return Err(Error::Waiting);
                }
                if !self.holds(
                    transaction,
                    &Span::Point(intent.key.clone()),
                    Mode::Exclusive,
                ) {
                    return Err(Error::WrongPhase);
                }
                let p = self
                    .participants
                    .get_mut(&transaction)
                    .ok_or(Error::Missing)?;
                if p.phase != Phase::Active {
                    return Err(Error::WrongPhase);
                }
                if let Some(old) = p.intents.get(&intent.key) {
                    return if old == &intent.value {
                        Ok(Response::Applied)
                    } else {
                        Err(Error::Conflict)
                    };
                }
                if p.intents.len() >= self.limits.intents_per_transaction {
                    return Err(Error::Capacity);
                }
                let used: usize = p
                    .intents
                    .iter()
                    .map(|(k, v)| k.len() + v.as_ref().map_or(0, Vec::len))
                    .sum();
                if used + intent.key.len() + intent.value.as_ref().map_or(0, Vec::len)
                    > self.limits.intent_bytes_per_transaction
                {
                    return Err(Error::Capacity);
                }
                p.intents.insert(intent.key, intent.value);
                Ok(Response::Applied)
            }
            Command::Prepare { transaction } => {
                self.valid_transaction(transaction)?;
                if self.queued(transaction) {
                    return Err(Error::Waiting);
                }
                let p = self
                    .participants
                    .get_mut(&transaction)
                    .ok_or(Error::Missing)?;
                if !matches!(p.phase, Phase::Active | Phase::Prepared) {
                    return Err(Error::WrongPhase);
                }
                p.phase = Phase::Prepared;
                let prepare_index = *p.prepare_index.get_or_insert(index);
                Ok(Response::Prepared(PrepareRef {
                    seal: p.seal.clone(),
                    participant: self.domain,
                    index: prepare_index,
                }))
            }
            Command::Rollback { transaction } => {
                self.valid_transaction(transaction)?;
                let p = self
                    .participants
                    .get_mut(&transaction)
                    .ok_or(Error::Missing)?;
                if p.phase == Phase::RolledBack {
                    return Ok(Response::Applied);
                }
                if p.phase != Phase::Active {
                    return Err(Error::WrongPhase);
                }
                p.phase = Phase::RolledBack;
                p.locks.clear();
                p.intents.clear();
                self.waiters.retain(|w| w.transaction != transaction);
                self.admit();
                Ok(Response::Applied)
            }
            Command::Finalize(reference) => {
                self.valid_transaction(reference.transaction)?;
                let p = self
                    .participants
                    .get_mut(&reference.transaction)
                    .ok_or(Error::Missing)?;
                if let Some(old) = &p.decision {
                    return if old == &reference {
                        Ok(Response::Finalized {
                            outcome: old.outcome,
                            intents: Vec::new(),
                        })
                    } else {
                        Err(Error::Conflict)
                    };
                }
                if p.phase == Phase::RolledBack {
                    return Err(Error::WrongPhase);
                }
                let proof = view.decision(&reference).ok_or(Error::MissingEvidence)?;
                if proof.reference != reference
                    || proof.seal.as_ref() != Some(&p.seal)
                    || proof.participants != p.participants
                    || reference.digest != p.seal.digest
                {
                    return Err(Error::Conflict);
                }
                if reference.outcome == Outcome::Commit && p.phase != Phase::Prepared {
                    return Err(Error::WrongPhase);
                }
                let intents = if reference.outcome == Outcome::Commit {
                    p.intents
                        .iter()
                        .map(|(k, v)| Intent {
                            key: k.clone(),
                            value: v.clone(),
                        })
                        .collect()
                } else {
                    Vec::new()
                };
                p.phase = if reference.outcome == Outcome::Commit {
                    Phase::Committed
                } else {
                    Phase::Aborted
                };
                p.decision = Some(reference.clone());
                p.locks.clear();
                p.intents.clear();
                self.waiters
                    .retain(|w| w.transaction != reference.transaction);
                self.admit();
                Ok(Response::Finalized {
                    outcome: reference.outcome,
                    intents,
                })
            }
        }
    }

    fn queued(&self, t: TransactionId) -> bool {
        self.waiters.iter().any(|w| w.transaction == t)
    }

    fn conflicts(&self, t: TransactionId, lock: &Lock) -> bool {
        self.participants
            .iter()
            .any(|(id, p)| *id != t && p.locks.iter().any(|l| lock.conflicts(l)))
    }

    fn acquire(&mut self, transaction: TransactionId, lock: Lock) -> Result<Response, Error> {
        self.valid_transaction(transaction)?;
        lock.span.validate(&self.limits)?;
        let p = self.participants.get(&transaction).ok_or(Error::Missing)?;
        if !matches!(p.phase, Phase::Active | Phase::Prepared) {
            return Err(Error::WrongPhase);
        }
        if p.locks.contains(&lock) {
            return Ok(Response::LockGranted);
        }
        if p.phase != Phase::Active {
            return Err(Error::WrongPhase);
        }
        if let Some(w) = self.waiters.iter().find(|w| w.transaction == transaction) {
            return if w.lock == lock {
                Ok(Response::Queued)
            } else {
                Err(Error::Conflict)
            };
        }
        let blocked = !self.waiters.is_empty() || self.conflicts(transaction, &lock);
        if blocked && !p.locks.is_empty() {
            return Err(Error::MustRollback);
        }
        // Every wait edge strictly decreases the same global TransactionId order,
        // including FIFO predecessor edges. This prevents cycles across ranges,
        // where a requester may hold locks invisible to this local machine.
        if blocked
            && (self.participants.iter().any(|(id, holder)| {
                *id > transaction && holder.locks.iter().any(|held| lock.conflicts(held))
            }) || self.waiters.iter().any(|w| w.transaction > transaction))
        {
            return Err(Error::MustRollback);
        }
        if p.locks.len() >= self.limits.locks_per_transaction {
            return Err(Error::Capacity);
        }
        let count: usize = self
            .participants
            .values()
            .map(|p| p.locks.len())
            .sum::<usize>()
            + self.waiters.len();
        if count >= self.limits.lock_requests {
            return Err(Error::Capacity);
        }
        if blocked {
            if self.waiters.len() >= self.limits.waiters {
                return Err(Error::Capacity);
            }
            self.waiters.push_back(Waiter { transaction, lock });
            Ok(Response::Queued)
        } else {
            self.participants
                .get_mut(&transaction)
                .ok_or(Error::Missing)?
                .locks
                .push(lock);
            Ok(Response::LockGranted)
        }
    }

    fn admit(&mut self) {
        while let Some(w) = self.waiters.front() {
            if self.conflicts(w.transaction, &w.lock) {
                break;
            }
            let w = self.waiters.pop_front().expect("front exists");
            self.participants
                .get_mut(&w.transaction)
                .expect("validated waiter")
                .locks
                .push(w.lock);
        }
    }

    /// Mint only after this machine's state and combined effects are durable.
    pub fn seal_evidence(&self, reference: &SealRef) -> Option<SealEvidence> {
        let h = self.homes.get(&reference.transaction)?;
        (h.seal.as_ref() == Some(reference) && h.decision.is_none()).then(|| SealEvidence {
            seal: reference.clone(),
            participants: h.participants.iter().copied().collect(),
        })
    }
    /// Prepared evidence remains available after finalization for delayed retries.
    pub fn prepare_evidence(&self, reference: &PrepareRef) -> Option<PrepareEvidence> {
        let p = self.participants.get(&reference.seal.transaction)?;
        (reference.participant == self.domain
            && p.seal == reference.seal
            && p.prepare_index == Some(reference.index))
        .then(|| PrepareEvidence {
            reference: reference.clone(),
        })
    }

    /// Recovery/status locator; evidence still requires a trusted committed view.
    pub fn prepared_reference(&self, transaction: TransactionId) -> Option<PrepareRef> {
        let p = self.participants.get(&transaction)?;
        Some(PrepareRef {
            seal: p.seal.clone(),
            participant: self.domain,
            index: p.prepare_index?,
        })
    }

    /// Canonical own-transaction overlay for a trusted storage adapter. This is
    /// inspection, not a public current read or policy authorization decision.
    pub fn staged_intents(&self, transaction: TransactionId) -> Option<Vec<Intent>> {
        let p = self.participants.get(&transaction)?;
        Some(
            p.intents
                .iter()
                .map(|(key, value)| Intent {
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect(),
        )
    }
    pub fn decision_evidence(&self, reference: &DecisionRef) -> Option<DecisionEvidence> {
        let h = self.homes.get(&reference.transaction)?;
        (h.decision.as_ref() == Some(reference)).then(|| DecisionEvidence {
            reference: reference.clone(),
            seal: h.seal.clone(),
            participants: h.participants.iter().copied().collect(),
        })
    }

    /// Versioned canonical snapshot, bounded including its SHA256 checksum.
    pub fn snapshot(&self) -> Result<Vec<u8>, Error> {
        let cap = self
            .limits
            .encoded_bytes
            .checked_sub(32)
            .ok_or(Error::Capacity)?;
        let mut bytes = encode(self, cap)?;
        let checksum = hash(b"giggles-transactions/snapshot/v1", &bytes);
        bytes.extend(checksum);
        Ok(bytes)
    }

    /// Validate exact domain, pinned limits, checksum, canonical encoding and invariants.
    /// Authentication, freshness and consensus snapshot-index checks are caller duties.
    pub fn restore(domain: Domain, limits: Limits, bytes: &[u8]) -> Result<Self, Error> {
        limits.validate()?;
        if bytes.len() < 32 || bytes.len() > limits.encoded_bytes {
            return Err(Error::InvalidSnapshot);
        }
        let (body, checksum) = bytes.split_at(bytes.len() - 32);
        if hash(b"giggles-transactions/snapshot/v1", body).as_slice() != checksum {
            return Err(Error::InvalidSnapshot);
        }
        let machine: Self = decode::<WireMachine>(body, limits.encoded_bytes)
            .map_err(|_| Error::InvalidSnapshot)?
            .into();
        if machine.domain != domain
            || machine.limits != limits
            || machine.version != 1
            || encode(&machine, limits.encoded_bytes)?.as_slice() != body
        {
            return Err(Error::InvalidSnapshot);
        }
        machine.validate().map_err(|_| Error::InvalidSnapshot)?;
        Ok(machine)
    }

    fn validate(&self) -> Result<(), Error> {
        if !self.domain.valid()
            || (self.applied == 0) != self.last.is_none()
            || self.homes.len() + self.participants.len() > self.limits.transactions
            || self.waiters.len() > self.limits.waiters
        {
            return Err(Error::InvalidSnapshot);
        }
        for (t, h) in &self.homes {
            self.valid_transaction(*t)?;
            if t.home != self.domain || h.participants.len() > self.limits.participants {
                return Err(Error::Invalid);
            }
            validate_participants(
                self.domain.database,
                &h.participants,
                self.limits.participants,
            )?;
            if let Some(s) = &h.seal {
                validate_seal(s, *t, h.digest, &h.participants, self.limits.encoded_bytes)?;
                if s.index > self.applied {
                    return Err(Error::Invalid);
                }
            } else if !h.votes.is_empty() {
                return Err(Error::Invalid);
            }
            for (domain, vote) in &h.votes {
                if !h.participants.contains(domain)
                    || vote.participant != *domain
                    || Some(&vote.seal) != h.seal.as_ref()
                    || vote.index == 0
                {
                    return Err(Error::Invalid);
                }
            }
            if let Some(d) = &h.decision
                && (d.transaction != *t
                    || d.digest != h.digest
                    || d.index == 0
                    || d.index > self.applied
                    || h.seal.as_ref().is_some_and(|s| s.index >= d.index)
                    || (d.outcome == Outcome::Commit
                        && (h.seal.is_none() || h.votes.len() != h.participants.len())))
            {
                return Err(Error::Invalid);
            }
        }
        let mut count = self.waiters.len();
        for (t, p) in &self.participants {
            self.valid_transaction(*t)?;
            let domains: BTreeSet<_> = p.participants.iter().copied().collect();
            validate_participants(self.domain.database, &domains, self.limits.participants)?;
            if domains.iter().copied().collect::<Vec<_>>() != p.participants
                || !domains.contains(&self.domain)
            {
                return Err(Error::Invalid);
            }
            validate_seal(
                &p.seal,
                *t,
                p.seal.digest,
                &domains,
                self.limits.encoded_bytes,
            )?;
            if p.locks.len() > self.limits.locks_per_transaction
                || p.intents.len() > self.limits.intents_per_transaction
            {
                return Err(Error::Capacity);
            }
            let locks: BTreeSet<_> = p.locks.iter().collect();
            if locks.len() != p.locks.len() {
                return Err(Error::Invalid);
            }
            count += p.locks.len();
            for lock in &p.locks {
                lock.span.validate(&self.limits)?;
                if self.conflicts(*t, lock) {
                    return Err(Error::Invalid);
                }
            }
            let mut used = 0;
            for (key, value) in &p.intents {
                Span::Point(key.clone()).validate(&self.limits)?;
                if value
                    .as_ref()
                    .is_some_and(|v| v.len() > self.limits.value_bytes)
                    || !self.holds(*t, &Span::Point(key.clone()), Mode::Exclusive)
                {
                    return Err(Error::Invalid);
                }
                used += key.len() + value.as_ref().map_or(0, Vec::len);
            }
            if used > self.limits.intent_bytes_per_transaction {
                return Err(Error::Capacity);
            }
            if p.prepare_index.is_some_and(|i| i == 0 || i > self.applied) {
                return Err(Error::Invalid);
            }
            match p.phase {
                Phase::Active if p.prepare_index.is_some() || p.decision.is_some() => {
                    return Err(Error::Invalid);
                }
                Phase::Prepared if p.prepare_index.is_none() || p.decision.is_some() => {
                    return Err(Error::Invalid);
                }
                Phase::RolledBack if p.prepare_index.is_some() || p.decision.is_some() => {
                    return Err(Error::Invalid);
                }
                Phase::Committed | Phase::Aborted => {
                    let d = p.decision.as_ref().ok_or(Error::Invalid)?;
                    if d.transaction != *t
                        || d.digest != p.seal.digest
                        || d.index <= p.seal.index
                        || (p.phase == Phase::Committed) != (d.outcome == Outcome::Commit)
                        || (p.phase == Phase::Committed && p.prepare_index.is_none())
                    {
                        return Err(Error::Invalid);
                    }
                }
                _ => {}
            }
            if matches!(
                p.phase,
                Phase::RolledBack | Phase::Committed | Phase::Aborted
            ) && (!p.locks.is_empty() || !p.intents.is_empty())
            {
                return Err(Error::Invalid);
            }
        }
        if count > self.limits.lock_requests {
            return Err(Error::Capacity);
        }
        let mut queued = BTreeSet::new();
        for w in &self.waiters {
            let p = self
                .participants
                .get(&w.transaction)
                .ok_or(Error::Invalid)?;
            if p.phase != Phase::Active || !p.locks.is_empty() || !queued.insert(w.transaction) {
                return Err(Error::Invalid);
            }
            w.lock.span.validate(&self.limits)?;
            if queued.iter().any(|id| *id > w.transaction)
                || self.participants.iter().any(|(id, holder)| {
                    *id > w.transaction && holder.locks.iter().any(|held| w.lock.conflicts(held))
                })
            {
                return Err(Error::Invalid);
            }
        }
        if let Some(w) = self.waiters.front()
            && !self.conflicts(w.transaction, &w.lock)
        {
            return Err(Error::Invalid);
        }
        if self
            .snapshot()?
            .len()
            .checked_add(self.resolution_reserve()?)
            .is_none_or(|n| n > self.limits.encoded_bytes)
        {
            return Err(Error::Capacity);
        }
        Ok(())
    }
}

fn covers(outer: &Span, inner: &Span) -> bool {
    match (outer, inner) {
        (_, Span::Point(k)) => outer.contains(k),
        (Span::Interval { start: a, end: b }, Span::Interval { start: c, end: d }) => {
            a <= c && d <= b
        }
        _ => false,
    }
}

fn participant_digest(domains: &BTreeSet<Domain>, cap: usize) -> Result<Digest, Error> {
    Ok(hash(
        b"giggles-transactions/participants/v1",
        &encode(domains, cap)?,
    ))
}

fn validate_participants(
    database: DatabaseId,
    domains: &BTreeSet<Domain>,
    cap: usize,
) -> Result<(), Error> {
    let mut ranges = BTreeSet::new();
    if domains.len() > cap {
        return Err(Error::Capacity);
    }
    for d in domains {
        if !d.valid() || d.database != database || !ranges.insert(d.range) {
            return Err(Error::WrongDomain);
        }
    }
    Ok(())
}

fn validate_seal(
    s: &SealRef,
    t: TransactionId,
    digest: Digest,
    domains: &BTreeSet<Domain>,
    cap: usize,
) -> Result<(), Error> {
    if s.transaction != t
        || s.digest != digest
        || s.index == 0
        || domains.is_empty()
        || s.participants_digest != participant_digest(domains, cap)?
    {
        return Err(Error::Invalid);
    }
    Ok(())
}

#[cfg(test)]
mod snapshot_invariants {
    use super::*;
    fn domain() -> Domain {
        Domain {
            database: DatabaseId([1; 16]),
            range: 1,
            epoch: 1,
        }
    }
    fn t(nonce: u128) -> TransactionId {
        TransactionId {
            home: domain(),
            nonce,
        }
    }
    fn machine() -> Machine {
        Machine::new(domain(), Limits::default()).unwrap()
    }
    fn rejected(m: &Machine) {
        // Recompute a correct checksum to exercise semantic checks, not hashing.
        let bytes = m.snapshot().unwrap();
        assert_eq!(
            Machine::restore(m.domain, m.limits.clone(), &bytes),
            Err(Error::InvalidSnapshot)
        );
    }
    fn participant(nonce: u128) -> Participant {
        let participants = BTreeSet::from([domain()]);
        Participant {
            seal: SealRef {
                transaction: t(nonce),
                digest: [3; 32],
                index: 1,
                participants_digest: participant_digest(&participants, 4096).unwrap(),
            },
            participants: vec![domain()],
            phase: Phase::Active,
            locks: vec![],
            intents: BTreeMap::new(),
            prepare_index: None,
            decision: None,
        }
    }
    fn populated() -> Machine {
        let mut m = machine();
        m.applied = 10;
        m.last = Some(([1; 32], Ok(Response::Applied)));
        m.participants.insert(t(1), participant(1));
        m
    }
    #[test]
    fn rechecksummed_snapshots_reject_conflicting_locks_and_unlocked_intents() {
        let lock = Lock {
            span: Span::Point(vec![1]),
            mode: Mode::Exclusive,
        };
        let mut m = populated();
        m.participants
            .get_mut(&t(1))
            .unwrap()
            .locks
            .push(lock.clone());
        let mut second = participant(2);
        second.locks.push(lock);
        m.participants.insert(t(2), second);
        rejected(&m);
        let mut m = populated();
        m.participants
            .get_mut(&t(1))
            .unwrap()
            .intents
            .insert(vec![1], Some(vec![2]));
        rejected(&m);
    }
    #[test]
    fn rechecksummed_snapshots_reject_missing_prepare_and_terminal_live_locks() {
        let mut m = populated();
        m.participants.get_mut(&t(1)).unwrap().phase = Phase::Prepared;
        rejected(&m);
        let mut m = populated();
        let p = m.participants.get_mut(&t(1)).unwrap();
        p.phase = Phase::RolledBack;
        p.locks.push(Lock {
            span: Span::Point(vec![1]),
            mode: Mode::Shared,
        });
        rejected(&m);
    }
    #[test]
    fn rechecksummed_snapshots_reject_fabricated_commit_without_all_votes() {
        let mut m = populated();
        m.participants.clear();
        let p = participant(1);
        m.homes.insert(
            t(1),
            Home {
                digest: p.seal.digest,
                participants: BTreeSet::from([domain()]),
                seal: Some(p.seal),
                votes: BTreeMap::new(),
                decision: Some(DecisionRef {
                    transaction: t(1),
                    digest: [3; 32],
                    index: 9,
                    outcome: Outcome::Commit,
                }),
            },
        );
        rejected(&m);
    }
    #[test]
    fn rechecksummed_snapshots_reject_unblocked_or_duplicate_waiters() {
        let mut m = populated();
        let waiter = Waiter {
            transaction: t(1),
            lock: Lock {
                span: Span::Point(vec![1]),
                mode: Mode::Exclusive,
            },
        };
        m.waiters.push_back(waiter.clone());
        rejected(&m);
        m.waiters.push_back(waiter);
        rejected(&m);
    }
    #[test]
    fn rechecksummed_snapshots_reject_future_format_wrong_digest_and_future_index() {
        let mut m = populated();
        m.version = 2;
        rejected(&m);
        let mut m = populated();
        m.participants
            .get_mut(&t(1))
            .unwrap()
            .seal
            .participants_digest = [9; 32];
        rejected(&m);
        let mut m = populated();
        let p = m.participants.get_mut(&t(1)).unwrap();
        p.phase = Phase::Prepared;
        p.prepare_index = Some(11);
        rejected(&m);
    }
}
