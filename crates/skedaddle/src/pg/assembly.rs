//! Bounded transaction envelopes around caller-supplied semantic event mapping.
//!
//! The protocol layer cannot infer mapped values, table identities, schema
//! versions, or the meaning of arbitrary logical messages. Every message other
//! than Begin/Commit/Origin is therefore offered to an explicit mapper. A mapper must
//! handle metadata and control messages deliberately or return an error; this
//! layer never silently discards them. Nontransactional logical messages must
//! be routed separately by the capture loop before calling `push`.
//!
//! This small in-memory adapter is capped, not a spill implementation. Capture
//! of larger transactions belongs on the buffer writer's streaming assembler.

use std::{
    collections::BTreeSet,
    fmt,
    io::{self, Write},
};

use super::pgoutput::{Begin, Message};
use crate::event::{
    AssembledTxn, Event, EventError, EventKind, Position, Row, SourceIdentity, SourceOrigin, TxnId,
};

pub const MAX_TRANSACTION_BYTES: u64 = 64 * 1024 * 1024;
pub const MAX_TRANSACTION_EVENTS: usize = 100_000;
const PG_EPOCH_UNIX_MICROS: i64 = 946_684_800_000_000;

/// Semantic fields supplied by P4/source mapping. The assembler owns envelope
/// identity, commit position, timestamp conversion, ordinals and digest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MappedEvent {
    pub schema_version: u64,
    pub kind: EventKind,
    pub table: Option<String>,
    pub key: Row,
    pub before: Option<Row>,
    pub after: Option<Row>,
    pub unchanged_cols: BTreeSet<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AssembleError {
    Invalid(&'static str),
    /// Use for an unhandled logical-message prefix or relation mapping.
    Mapping(&'static str),
    LimitExceeded,
    Event(EventError),
}

impl fmt::Display for AssembleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "pgoutput transaction assembly: {self:?}")
    }
}
impl std::error::Error for AssembleError {}
impl From<EventError> for AssembleError {
    fn from(value: EventError) -> Self {
        Self::Event(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompletedTransaction {
    pub transaction: AssembledTxn,
    /// WAL resume/feedback boundary, distinct from the commit record's LSN.
    /// Merely assembling a transaction does not make this safe to acknowledge.
    pub end_lsn: u64,
}

pub struct TransactionAssembler {
    source: SourceIdentity,
    order_domain: String,
    maximum_bytes: u64,
    maximum_events: usize,
    begin: Option<Begin>,
    source_origin: Option<SourceOrigin>,
    events: Vec<Event>,
    bytes: u64,
    failed: bool,
}

impl TransactionAssembler {
    pub fn new(
        source: SourceIdentity,
        order_domain: impl Into<String>,
        maximum_bytes: u64,
        maximum_events: usize,
    ) -> Result<Self, AssembleError> {
        if maximum_bytes == 0
            || maximum_bytes > MAX_TRANSACTION_BYTES
            || maximum_events == 0
            || maximum_events > MAX_TRANSACTION_EVENTS
        {
            return Err(AssembleError::Invalid("transaction limits"));
        }
        Ok(Self {
            source,
            order_domain: order_domain.into(),
            maximum_bytes,
            maximum_events,
            begin: None,
            source_origin: None,
            events: Vec::new(),
            bytes: 0,
            failed: false,
        })
    }

    /// Any error poisons the assembler and drops its provisional events. The
    /// caller must reconnect/replay with a fresh assembler after fixing the
    /// error; a later Commit can never emit a partial transaction.
    pub fn push(
        &mut self,
        message: &Message<'_>,
        map: impl FnOnce(&Message<'_>) -> Result<Vec<MappedEvent>, AssembleError>,
    ) -> Result<Option<CompletedTransaction>, AssembleError> {
        if self.failed {
            return Err(AssembleError::Invalid(
                "assembler poisoned; replay required",
            ));
        }
        let result = self.push_inner(message, map);
        if result.is_err() {
            self.failed = true;
            self.begin = None;
            self.source_origin = None;
            self.events = Vec::new();
            self.bytes = 0;
        }
        result
    }

    fn push_inner(
        &mut self,
        message: &Message<'_>,
        map: impl FnOnce(&Message<'_>) -> Result<Vec<MappedEvent>, AssembleError>,
    ) -> Result<Option<CompletedTransaction>, AssembleError> {
        match message {
            Message::Begin(begin) => {
                if self.begin.is_some() {
                    return Err(AssembleError::Invalid("nested Begin"));
                }
                self.begin = Some(*begin);
                self.bytes = 0;
                Ok(None)
            }
            Message::Commit(commit) => {
                let begin = self
                    .begin
                    .ok_or(AssembleError::Invalid("Commit without Begin"))?;
                if begin.final_lsn != commit.commit_lsn
                    || begin.commit_time != commit.commit_time
                    || commit.end_lsn < commit.commit_lsn
                {
                    return Err(AssembleError::Invalid("Begin/Commit metadata mismatch"));
                }
                let commit_time = commit
                    .commit_time
                    .checked_add(PG_EPOCH_UNIX_MICROS)
                    .ok_or(AssembleError::Invalid("Unix timestamp overflow"))?;
                let transaction = AssembledTxn::new_with_origin(
                    self.source.clone(),
                    txn_id(begin),
                    Position::new(&self.order_domain, commit.commit_lsn.to_be_bytes()),
                    Some(commit_time),
                    self.source_origin.take(),
                    std::mem::take(&mut self.events),
                )?;
                self.begin = None;
                self.bytes = 0;
                Ok(Some(CompletedTransaction {
                    transaction,
                    end_lsn: commit.end_lsn,
                }))
            }
            Message::Origin { commit_lsn, name } => {
                if self.begin.is_none() {
                    return Err(AssembleError::Invalid("Origin without Begin"));
                }
                if self.source_origin.is_some() || !self.events.is_empty() {
                    return Err(AssembleError::Invalid("duplicate or late Origin"));
                }
                self.bytes = self
                    .bytes
                    .checked_add(name.len() as u64 + 13)
                    .ok_or(AssembleError::LimitExceeded)?;
                if self.bytes > self.maximum_bytes {
                    return Err(AssembleError::LimitExceeded);
                }
                self.source_origin = Some(SourceOrigin {
                    name: (*name).into(),
                    lsn: *commit_lsn,
                });
                Ok(None)
            }
            Message::LogicalMessage {
                transactional: false,
                ..
            } => Err(AssembleError::Invalid(
                "route nontransactional logical messages separately",
            )),
            _ => {
                // Relation/Type can occur outside a transaction (for example
                // before a nontransactional logical message). All DML, Origin
                // and transactional logical messages require an envelope.
                if self.begin.is_none()
                    && !matches!(message, Message::Relation(_) | Message::Type { .. })
                {
                    return Err(AssembleError::Invalid(
                        "transactional message without Begin",
                    ));
                }
                let mapped = map(message)?;
                // The semantic mapper resolves relation IDs to durable table
                // identities in wire order. Never allow splitting one source
                // statement or dropping its flags/relations.
                if let Message::Truncate {
                    relation_ids,
                    cascade,
                    restart_identity,
                } = message
                {
                    if !matches!(mapped.as_slice(), [MappedEvent {
                        kind: EventKind::Truncate { relations, cascade: c, restart_identity: r }, ..
                    }] if relations.len() == relation_ids.len() && c == cascade && r == restart_identity)
                    {
                        return Err(AssembleError::Mapping(
                            "TRUNCATE must map to one complete ordered group",
                        ));
                    }
                } else if mapped
                    .iter()
                    .any(|event| matches!(event.kind, EventKind::Truncate { .. }))
                {
                    return Err(AssembleError::Mapping(
                        "TRUNCATE requires a source TRUNCATE message",
                    ));
                }
                if mapped.len() > self.maximum_events - self.events.len() {
                    return Err(AssembleError::LimitExceeded);
                }
                for data in mapped {
                    let begin = self
                        .begin
                        .ok_or(AssembleError::Invalid("mapped event without Begin"))?;
                    let event = Event {
                        source: self.source.clone(),
                        txn: txn_id(begin),
                        ordinal_in_txn: self.events.len() as u64,
                        position: Position::new(&self.order_domain, begin.final_lsn.to_be_bytes()),
                        schema_version: data.schema_version,
                        kind: data.kind,
                        table: data.table,
                        key: data.key,
                        before: data.before,
                        after: data.after,
                        unchanged_cols: data.unchanged_cols,
                    };
                    // Count canonical output without allocating another event.
                    // A separate count cap also bounds tiny/empty events.
                    let mut counter = CountBytes(0);
                    event.write_canonical(&mut counter)?;
                    self.bytes = self
                        .bytes
                        .checked_add(counter.0)
                        .ok_or(AssembleError::LimitExceeded)?;
                    if self.bytes > self.maximum_bytes {
                        return Err(AssembleError::LimitExceeded);
                    }
                    self.events.push(event);
                }
                Ok(None)
            }
        }
    }

    /// A connection ending inside Begin/Commit is incomplete, even if no rows
    /// were seen. No transaction is emitted until a validated Commit arrives.
    pub fn finish(&self) -> Result<(), AssembleError> {
        if self.failed || self.begin.is_some() {
            Err(AssembleError::Invalid(
                "incomplete or failed transaction stream",
            ))
        } else {
            Ok(())
        }
    }
}

fn txn_id(begin: Begin) -> TxnId {
    let mut bytes = Vec::with_capacity(12);
    bytes.extend_from_slice(&begin.xid.to_be_bytes());
    bytes.extend_from_slice(&begin.final_lsn.to_be_bytes());
    TxnId(bytes)
}

struct CountBytes(u64);
impl Write for CountBytes {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| io::Error::other("event size overflow"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
