use bincode::Options;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest as _, Sha256};

/// Database identity includes the generation; a restored timeline needs a new ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DatabaseId(pub [u8; 16]);

/// Exact consensus ownership domain. Zero range/epoch and zero database are invalid.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Domain {
    pub database: DatabaseId,
    pub range: u64,
    pub epoch: u64,
}

impl Domain {
    pub(crate) fn valid(self) -> bool {
        self.database.0 != [0; 16] && self.range != 0 && self.epoch != 0
    }
}

/// Stable retry identity; never reuse the nonce for a different operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TransactionId {
    pub home: Domain,
    pub nonce: u128,
}

pub type Digest = [u8; 32];

/// Domain-separated digest of the adapter's canonical full operation bytes.
pub fn operation_digest(bytes: &[u8]) -> Digest {
    hash(b"giggles-transactions/operation/v1", bytes)
}

pub(crate) fn hash(tag: &[u8], bytes: &[u8]) -> Digest {
    let mut h = Sha256::new();
    h.update(tag);
    h.update(bytes);
    h.finalize().into()
}

/// Explicit bounded memory and retention policy. Limits are pinned in snapshots.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Limits {
    pub transactions: usize,
    pub participants: usize,
    pub lock_requests: usize,
    pub locks_per_transaction: usize,
    pub waiters: usize,
    pub intents_per_transaction: usize,
    pub key_bytes: usize,
    pub value_bytes: usize,
    pub intent_bytes_per_transaction: usize,
    pub encoded_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            transactions: 256,
            participants: 32,
            lock_requests: 2048,
            locks_per_transaction: 64,
            waiters: 128,
            intents_per_transaction: 64,
            key_bytes: 1024,
            value_bytes: 64 * 1024,
            intent_bytes_per_transaction: 256 * 1024,
            encoded_bytes: 16 * 1024 * 1024,
        }
    }
}

impl Limits {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        let values = [
            self.transactions,
            self.participants,
            self.lock_requests,
            self.locks_per_transaction,
            self.waiters,
            self.intents_per_transaction,
            self.key_bytes,
            self.value_bytes,
            self.intent_bytes_per_transaction,
            self.encoded_bytes,
        ];
        if values.contains(&0)
            || values.iter().any(|&v| v > 64 * 1024 * 1024)
            || self.encoded_bytes < 1024
            || self.encoded_bytes > 64 * 1024 * 1024
        {
            return Err(Error::Invalid);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Span {
    Point(Vec<u8>),
    /// Predicate or gap in an explicit index namespace, [start, end).
    Interval {
        start: Vec<u8>,
        end: Vec<u8>,
    },
}

impl Span {
    pub(crate) fn validate(&self, limits: &Limits) -> Result<(), Error> {
        let valid_key = |k: &[u8]| !k.is_empty() && k.len() <= limits.key_bytes;
        let ok = match self {
            Self::Point(k) => valid_key(k),
            Self::Interval { start, end } => valid_key(start) && valid_key(end) && start < end,
        };
        if ok { Ok(()) } else { Err(Error::Invalid) }
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        match self {
            Self::Point(k) => k == key,
            Self::Interval { start, end } => start.as_slice() <= key && key < end.as_slice(),
        }
    }

    pub fn overlaps(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Point(k), s) | (s, Self::Point(k)) => s.contains(k),
            (Self::Interval { start: a, end: b }, Self::Interval { start: c, end: d }) => {
                a < d && c < b
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Mode {
    Shared,
    Exclusive,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Lock {
    pub span: Span,
    pub mode: Mode,
}

impl Lock {
    pub(crate) fn conflicts(&self, other: &Self) -> bool {
        (self.mode == Mode::Exclusive || other.mode == Mode::Exclusive)
            && self.span.overlaps(&other.span)
    }
}

/// Canonical opaque redo. None deletes; Some(empty) is a stored empty value.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Intent {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Outcome {
    Commit,
    Abort,
}

/// Serializable locator, not proof. Resolved against trusted committed state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SealRef {
    pub transaction: TransactionId,
    pub digest: Digest,
    pub index: u64,
    pub participants_digest: Digest,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrepareRef {
    pub seal: SealRef,
    pub participant: Domain,
    pub index: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionRef {
    pub transaction: TransactionId,
    pub digest: Digest,
    pub index: u64,
    pub outcome: Outcome,
}

/// Deterministic replicated command. Decode with [`decode_command`] at ingress.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    OpenHome {
        transaction: TransactionId,
        digest: Digest,
    },
    Register {
        transaction: TransactionId,
        digest: Digest,
        participant: Domain,
    },
    Seal {
        transaction: TransactionId,
        digest: Digest,
    },
    RecordPrepare(PrepareRef),
    Decide {
        transaction: TransactionId,
        digest: Digest,
        outcome: Outcome,
    },
    Begin(SealRef),
    Acquire {
        transaction: TransactionId,
        lock: Lock,
    },
    Stage {
        transaction: TransactionId,
        intent: Intent,
    },
    Prepare {
        transaction: TransactionId,
    },
    Rollback {
        transaction: TransactionId,
    },
    Finalize(DecisionRef),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Response {
    Applied,
    Seal(SealRef),
    LockGranted,
    Queued,
    Prepared(PrepareRef),
    Decision(DecisionRef),
    /// Apply these effects with the state snapshot and applied index atomically.
    Finalized {
        outcome: Outcome,
        intents: Vec<Intent>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Error {
    Invalid,
    WrongDomain,
    Missing,
    Conflict,
    WrongPhase,
    MissingEvidence,
    MissingParticipants,
    MustRollback,
    Waiting,
    Capacity,
    StaleIndex,
    ConflictingIndex,
    InvalidSnapshot,
    Codec,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

pub(crate) fn encode<T: Serialize>(v: &T, cap: usize) -> Result<Vec<u8>, Error> {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(cap as u64)
        .serialize(v)
        .map_err(|_| Error::Capacity)
}

pub(crate) fn decode<T: DeserializeOwned>(bytes: &[u8], cap: usize) -> Result<T, Error> {
    if bytes.len() > cap {
        return Err(Error::Capacity);
    }
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(cap as u64)
        .reject_trailing_bytes()
        .deserialize(bytes)
        .map_err(|_| Error::Codec)
}

pub fn encode_command(command: &Command, limits: &Limits) -> Result<Vec<u8>, Error> {
    limits.validate()?;
    encode(&(1_u32, command), limits.encoded_bytes)
}

pub fn decode_command(bytes: &[u8], limits: &Limits) -> Result<Command, Error> {
    limits.validate()?;
    let (version, command): (u32, Command) = decode(bytes, limits.encoded_bytes)?;
    if version != 1 {
        return Err(Error::Codec);
    }
    Ok(command)
}
