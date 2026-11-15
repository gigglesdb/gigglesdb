//! Engine-neutral events and canonical version 3 binary records.
//!
//! Integers and lengths are big-endian (lengths/counts are u32). A record is
//! `SKEV | version:u16 | kind:u8 | fields | SHA-256(prefix)`. Kinds 1 and 2
//! identify events and transactions. Fields follow declaration order below;
//! strings/bytes have length prefixes, options have 0/1 tags, maps/sets are
//! strictly increasing by UTF-8 column name. Transactions embed event fields
//! without nested envelopes. Their stored digest is the envelope's trailer,
//! excluded from its own preimage. Decoders reject noncanonical representations.
//! Version 2 adds grouped TRUNCATE payloads and optional transaction source
//! origins. Like the buffer's canonical-record writer, readers accept only the
//! current version: v1 and v2 records fail explicitly with UnsupportedVersion.
//! Version 3 names the fifth source identity component stream, preserving field
//! order while changing the header and therefore canonical digests.
//! TRUNCATE tag 3 is followed by a u32 relation count, length-prefixed table
//! identities in source order, then cascade and restart_identity boolean bytes.
//! Transaction origin is an option after commit_time: name string then u64 LSN.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashMap},
    error::Error,
    fmt,
    io::{self, Read, Seek, SeekFrom, Write},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const ENCODING_VERSION: u16 = 3;
pub type ContentDigest = [u8; 32];

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SourceIdentity {
    pub engine: String,
    pub system_id: String,
    pub lineage: String,
    pub db_name: String,
    pub stream: String,
}

/// Connector bytes and their ordering domain. Deliberately has no Ord or
/// PartialOrd implementation and no timestamp conversion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Position {
    order_domain: String,
    bytes: Vec<u8>,
}

/// Version 1 JSON representation of an opaque position: domain and lowercase
/// hex bytes. The enclosing format supplies its version; no version field is
/// inserted here, preserving existing manifest bytes and digest preimages.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PositionWireV1 {
    order_domain: String,
    bytes: String,
}

impl From<&Position> for PositionWireV1 {
    fn from(position: &Position) -> Self {
        const DIGITS: &[u8] = b"0123456789abcdef";
        let mut bytes = String::new();
        for byte in position.bytes() {
            bytes.push(DIGITS[(byte >> 4) as usize] as char);
            bytes.push(DIGITS[(byte & 15) as usize] as char);
        }
        Self {
            order_domain: position.order_domain().into(),
            bytes,
        }
    }
}

impl TryFrom<PositionWireV1> for Position {
    type Error = &'static str;

    fn try_from(wire: PositionWireV1) -> Result<Self, Self::Error> {
        if !wire.bytes.len().is_multiple_of(2) {
            return Err("hex length");
        }
        fn nibble(byte: u8) -> Result<u8, &'static str> {
            match byte {
                b'0'..=b'9' => Ok(byte - b'0'),
                b'a'..=b'f' => Ok(byte - b'a' + 10),
                _ => Err("lowercase hex"),
            }
        }
        let bytes: Result<Vec<_>, Self::Error> = wire
            .bytes
            .as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| Ok(nibble(pair[0])? * 16 + nibble(pair[1])?))
            .collect();
        Ok(Self::new(wire.order_domain, bytes?))
    }
}

/// A connector defines both the total order and the domain in which it applies.
/// The domain must distinguish independent streams and source lineages.
pub trait PositionOrder {
    fn domain(&self) -> &str;
    fn compare(&self, left: &[u8], right: &[u8]) -> Result<Ordering, EventError>;
}

impl Position {
    pub fn new(order_domain: impl Into<String>, bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            order_domain: order_domain.into(),
            bytes: bytes.into(),
        }
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn order_domain(&self) -> &str {
        &self.order_domain
    }

    pub fn compare(
        &self,
        other: &Self,
        order: &(impl PositionOrder + ?Sized),
    ) -> Result<Ordering, EventError> {
        if self.order_domain != other.order_domain || self.order_domain != order.domain() {
            return Err(EventError::PositionDomainMismatch);
        }
        order.compare(&self.bytes, &other.bytes)
    }
}

/// Opaque connector-scoped identity (for PG, xid plus commit LSN).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TxnId(pub Vec<u8>);

/// Explicit P4 seam: the connector supplies a type/representation identity and
/// opaque canonical bytes. None is SQL NULL; Some(empty) is a non-NULL value.
/// P4 will supply typed Value conversion without interpreting bytes here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpaqueValue {
    pub type_id: String,
    pub bytes: Option<Vec<u8>>,
}

pub type Row = BTreeMap<String, OpaqueValue>;

#[derive(Clone, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum EventKind {
    Insert = 0,
    Update = 1,
    Delete = 2,
    /// One atomic statement. Relation identities are unique and retain source
    /// order; Event.table is None and row images/keys/TOAST markers are empty.
    Truncate {
        relations: Vec<String>,
        cascade: bool,
        restart_identity: bool,
    } = 3,
    DdlBoundary = 4,
    Watermark = 5,
    FinalMarker = 6,
    ChunkRows = 7,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    pub source: SourceIdentity,
    pub txn: TxnId,
    pub ordinal_in_txn: u64,
    pub position: Position,
    pub schema_version: u64,
    pub kind: EventKind,
    /// None for source-wide controls and grouped TRUNCATE (whose kind carries
    /// every relation identity).
    pub table: Option<String>,
    pub key: Row,
    pub before: Option<Row>,
    pub after: Option<Row>,
    /// pgoutput's unchanged TOAST markers: the applier must not write these
    /// columns. These are distinct from NULL and from absent row images.
    pub unchanged_cols: BTreeSet<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AssembledTxn {
    pub source: SourceIdentity,
    pub txn: TxnId,
    pub commit_position: Position,
    /// Connector-reported Unix microseconds, if available; never derived from
    /// commit_position and never used to order transactions.
    pub commit_time: Option<i64>,
    pub source_origin: Option<SourceOrigin>,
    pub events: Vec<Event>,
    pub digest: ContentDigest,
}

/// Original publisher metadata, separate from the connector's commit position
/// and the destination's stream-specific replication origin.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceOrigin {
    pub name: String,
    pub lsn: u64,
}

/// Common transaction metadata. StreamingTxn stores optional source origin
/// separately so existing origin-free producers retain their constructor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransactionMetadata {
    pub source: SourceIdentity,
    pub txn: TxnId,
    pub commit_position: Position,
    pub commit_time: Option<i64>,
}

/// A complete canonical transaction backed by caller-owned seekable storage.
/// Use a file (or a product scratch guard implementing Read/Write/Seek) for a
/// transaction-independent memory bound. No event Vec or ordinal index is kept.
/// The storage owner controls its lifetime and crash-reclamation policy.
pub struct StreamingTxn<S> {
    storage: S,
    metadata: TransactionMetadata,
    source_origin: Option<SourceOrigin>,
    event_count: u32,
    canonical_len: u64,
    canonical_start: u64,
    digest: ContentDigest,
}

impl<S: Read + Write + Seek> StreamingTxn<S> {
    /// Consumes at most one event at a time, validating membership and ordering
    /// while writing and hashing exactly the same bytes as AssembledTxn v2.
    /// `storage` must be empty. Failed output is provisional and must be discarded.
    /// `event_count` is required before encoding; a count mismatch is an error.
    pub fn from_events(
        metadata: TransactionMetadata,
        event_count: u32,
        events: impl IntoIterator<Item = Result<Event, EventError>>,
        storage: S,
    ) -> Result<Self, EventError> {
        Self::from_events_with_origin(metadata, None, event_count, events, storage)
    }

    pub fn from_events_with_origin(
        metadata: TransactionMetadata,
        source_origin: Option<SourceOrigin>,
        event_count: u32,
        events: impl IntoIterator<Item = Result<Event, EventError>>,
        mut storage: S,
    ) -> Result<Self, EventError> {
        if storage.seek(SeekFrom::End(0))? != 0 {
            return Err(EventError::InvalidTransaction(
                "stream storage must be empty",
            ));
        }
        let mut hashed = HashWriter::new(&mut storage);
        put_transaction_header(
            &mut hashed,
            &metadata.source,
            &metadata.txn,
            &metadata.commit_position,
            metadata.commit_time,
            source_origin.as_ref(),
            event_count as usize,
        )?;
        let mut count = 0_u32;
        let mut last = None;
        for event in events {
            let event = event?;
            if count == event_count {
                return Err(EventError::InvalidTransaction(
                    "stream event count mismatch",
                ));
            }
            if event.source != metadata.source || event.txn != metadata.txn {
                return Err(EventError::InvalidTransaction(
                    "event source/transaction mismatch",
                ));
            }
            if event.position.order_domain != metadata.commit_position.order_domain {
                return Err(EventError::PositionDomainMismatch);
            }
            if last.is_some_and(|ordinal| ordinal >= event.ordinal_in_txn) {
                return Err(EventError::InvalidTransaction(
                    "event ordinals must strictly increase",
                ));
            }
            last = Some(event.ordinal_in_txn);
            put_event(&mut hashed, &event)?;
            count += 1;
        }
        if count != event_count {
            return Err(EventError::InvalidTransaction(
                "stream event count mismatch",
            ));
        }
        let digest = hashed.digest();
        let canonical_len = hashed
            .bytes
            .checked_add(32)
            .ok_or(EventError::InvalidEncoding("record size overflow"))?;
        storage.write_all(&digest)?;
        storage.flush()?;
        Ok(Self {
            storage,
            metadata,
            source_origin,
            event_count,
            canonical_len,
            canonical_start: 0,
            digest,
        })
    }
}

impl<S: Read + Seek> StreamingTxn<S> {
    /// Validates a file-backed transaction without retaining its events.
    pub fn from_storage(mut storage: S) -> Result<Self, EventError> {
        let length = storage.seek(SeekFrom::End(0))?;
        Self::from_storage_range(storage, 0, length, |_| Ok(()))
    }

    pub(crate) fn from_storage_range(
        mut storage: S,
        start: u64,
        length: u64,
        mut visit: impl FnMut(&Event) -> Result<(), EventError>,
    ) -> Result<Self, EventError> {
        storage.seek(SeekFrom::Start(start))?;
        let mut input = stream_prefix(&mut storage, length, 2)?;
        let metadata = TransactionMetadata {
            source: input.source()?,
            txn: TxnId(input.bytes()?),
            commit_position: input.position()?,
            commit_time: input.option(|r| Ok(i64::from_be_bytes(r.array()?)))?,
        };
        let source_origin = input.option(Reader::origin)?;
        let event_count = u32::try_from(input.count()?)
            .map_err(|_| EventError::InvalidEncoding("event count"))?;
        let mut last = None;
        for _ in 0..event_count {
            let event = input.event()?;
            if event.source != metadata.source || event.txn != metadata.txn {
                return Err(EventError::InvalidTransaction(
                    "event source/transaction mismatch",
                ));
            }
            if event.position.order_domain != metadata.commit_position.order_domain {
                return Err(EventError::PositionDomainMismatch);
            }
            if last.is_some_and(|n| n >= event.ordinal_in_txn) {
                return Err(EventError::InvalidTransaction(
                    "event ordinals must strictly increase",
                ));
            }
            last = Some(event.ordinal_in_txn);
            visit(&event)?;
        }
        let digest = stream_finish(input)?;
        Ok(Self {
            storage,
            metadata,
            source_origin,
            event_count,
            canonical_len: length,
            canonical_start: start,
            digest,
        })
    }

    pub fn metadata(&self) -> &TransactionMetadata {
        &self.metadata
    }

    pub fn source_origin(&self) -> Option<&SourceOrigin> {
        self.source_origin.as_ref()
    }

    pub fn event_count(&self) -> u32 {
        self.event_count
    }

    pub fn canonical_len(&self) -> u64 {
        self.canonical_len
    }

    pub fn digest(&self) -> ContentDigest {
        self.digest
    }

    /// Rewinds a bounded, read-only view. Keeping the transaction alive keeps
    /// its storage alive through segment encoding and publication retries.
    pub fn canonical_reader(&mut self) -> Result<io::Take<&mut S>, EventError> {
        self.storage.seek(SeekFrom::Start(self.canonical_start))?;
        Ok((&mut self.storage).take(self.canonical_len))
    }

    /// Transfers storage ownership (and any associated cleanup responsibility).
    pub fn into_storage(self) -> S {
        self.storage
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventError {
    Io(io::ErrorKind),
    UnsupportedVersion(u16),
    InvalidEncoding(&'static str),
    DigestMismatch,
    InvalidTransaction(&'static str),
    PositionDomainMismatch,
    RedeliveryConflict { txn: TxnId, ordinal: Option<u64> },
}

impl fmt::Display for EventError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(kind) => write!(f, "canonical record I/O: {kind}"),
            Self::UnsupportedVersion(v) => write!(f, "unsupported event encoding version {v}"),
            Self::InvalidEncoding(reason) => write!(f, "invalid event encoding: {reason}"),
            Self::DigestMismatch => f.write_str("event/transaction digest mismatch"),
            Self::InvalidTransaction(reason) => {
                write!(f, "invalid assembled transaction: {reason}")
            }
            Self::PositionDomainMismatch => {
                f.write_str("positions belong to different connector order domains")
            }
            Self::RedeliveryConflict { txn, ordinal } => write!(
                f,
                "conflicting redelivery for transaction {txn:?}, ordinal {ordinal:?}"
            ),
        }
    }
}

impl Error for EventError {}

impl From<io::Error> for EventError {
    fn from(error: io::Error) -> Self {
        Self::Io(error.kind())
    }
}

impl Event {
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, EventError> {
        let mut out = Vec::new();
        self.write_canonical(&mut out)?;
        Ok(out)
    }

    /// Writes the v2 envelope without allocating an encoded event.
    /// On error the destination contains a provisional, incomplete record.
    pub fn write_canonical(&self, out: &mut impl Write) -> Result<(), EventError> {
        let mut hashed = HashWriter::new(out);
        hashed.write_all(&header(1))?;
        put_event(&mut hashed, self)?;
        let digest = hashed.digest();
        hashed.inner.write_all(&digest)?;
        Ok(())
    }

    /// Reads exactly one sized event, allocating only the decoded event fields.
    pub fn read_canonical(reader: impl Read, length: u64) -> Result<Self, EventError> {
        let mut input = stream_prefix(reader, length, 1)?;
        let event = input.event()?;
        stream_finish(input)?;
        Ok(event)
    }

    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self, EventError> {
        let mut input = open(bytes, 1)?;
        let event = input.event()?;
        input.finish()?;
        Ok(event)
    }

    /// SHA-256 of the versioned canonical prefix, excluding the digest trailer.
    pub fn digest(&self) -> Result<ContentDigest, EventError> {
        let mut out = HashWriter::new(io::sink());
        out.write_all(&header(1))?;
        put_event(&mut out, self)?;
        Ok(out.digest())
    }
}

impl AssembledTxn {
    pub fn new(
        source: SourceIdentity,
        txn: TxnId,
        commit_position: Position,
        commit_time: Option<i64>,
        events: Vec<Event>,
    ) -> Result<Self, EventError> {
        Self::new_with_origin(source, txn, commit_position, commit_time, None, events)
    }

    pub fn new_with_origin(
        source: SourceIdentity,
        txn: TxnId,
        commit_position: Position,
        commit_time: Option<i64>,
        source_origin: Option<SourceOrigin>,
        events: Vec<Event>,
    ) -> Result<Self, EventError> {
        let mut txn = Self {
            source,
            txn,
            commit_position,
            commit_time,
            source_origin,
            events,
            digest: [0; 32],
        };
        txn.digest = txn.computed_digest()?;
        Ok(txn)
    }

    fn validate(&self) -> Result<(), EventError> {
        let mut last = None;
        for event in &self.events {
            if event.source != self.source || event.txn != self.txn {
                return Err(EventError::InvalidTransaction(
                    "event source/transaction mismatch",
                ));
            }
            if event.position.order_domain != self.commit_position.order_domain {
                return Err(EventError::PositionDomainMismatch);
            }
            if last.is_some_and(|ordinal| ordinal >= event.ordinal_in_txn) {
                return Err(EventError::InvalidTransaction(
                    "event ordinals must strictly increase",
                ));
            }
            last = Some(event.ordinal_in_txn);
        }
        Ok(())
    }

    fn write_prefix(&self, out: &mut impl Write) -> Result<(), EventError> {
        self.validate()?;
        put_transaction_header(
            out,
            &self.source,
            &self.txn,
            &self.commit_position,
            self.commit_time,
            self.source_origin.as_ref(),
            self.events.len(),
        )?;
        for event in &self.events {
            put_event(out, event)?;
        }
        Ok(())
    }

    fn computed_digest(&self) -> Result<ContentDigest, EventError> {
        let mut hashed = HashWriter::new(io::sink());
        self.write_prefix(&mut hashed)?;
        Ok(hashed.digest())
    }

    /// Streams an owned transaction without allocating its encoded record.
    /// A failed validation or I/O leaves provisional output; discard it.
    pub fn write_canonical(&self, out: &mut impl Write) -> Result<(), EventError> {
        let mut hashed = HashWriter::new(out);
        self.write_prefix(&mut hashed)?;
        if hashed.digest() != self.digest {
            return Err(EventError::DigestMismatch);
        }
        hashed.inner.write_all(&self.digest)?;
        Ok(())
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>, EventError> {
        let mut out = Vec::new();
        self.write_canonical(&mut out)?;
        Ok(out)
    }

    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self, EventError> {
        let mut input = open(bytes, 2)?;
        let source = input.source()?;
        let txn = TxnId(input.bytes()?);
        let commit_position = input.position()?;
        let commit_time = input.option(|input| Ok(i64::from_be_bytes(input.array()?)))?;
        let source_origin = input.option(Reader::origin)?;
        let count = input.count()?;
        let mut events = Vec::new();
        for _ in 0..count {
            events.push(input.event()?);
        }
        input.finish()?;
        // open has already verified that the trailer is the prefix digest.
        let value = Self {
            source,
            txn,
            commit_position,
            commit_time,
            source_origin,
            events,
            digest: sha256(&bytes[..bytes.len() - 32]),
        };
        value.validate()?;
        Ok(value)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Redelivery {
    New,
    Noop,
}

/// In-memory redelivery check, scoped by complete source identity. Buffer/apply
/// layers must persist or rebuild this history for their retention window;
/// this type alone is neither durable nor a bounded transaction assembler.
#[derive(Default)]
pub struct RedeliveryCheck {
    events: HashMap<(SourceIdentity, TxnId, u64), ContentDigest>,
    txns: HashMap<(SourceIdentity, TxnId), ContentDigest>,
}

impl RedeliveryCheck {
    pub fn check_event(&mut self, event: &Event) -> Result<Redelivery, EventError> {
        let key = (
            event.source.clone(),
            event.txn.clone(),
            event.ordinal_in_txn,
        );
        let digest = event.digest()?;
        if let Some(existing) = self.events.get(&key) {
            return if *existing == digest {
                Ok(Redelivery::Noop)
            } else {
                Err(conflict(&event.txn, Some(event.ordinal_in_txn)))
            };
        }
        if self
            .txns
            .contains_key(&(event.source.clone(), event.txn.clone()))
        {
            return Err(conflict(&event.txn, Some(event.ordinal_in_txn)));
        }
        self.events.insert(key, digest);
        Ok(Redelivery::New)
    }

    pub fn check_transaction(&mut self, txn: &AssembledTxn) -> Result<Redelivery, EventError> {
        // Validate even on a matching stored digest: public fields may have changed.
        if txn.computed_digest()? != txn.digest {
            return Err(EventError::DigestMismatch);
        }
        let key = (txn.source.clone(), txn.txn.clone());
        if let Some(existing) = self.txns.get(&key) {
            return if *existing == txn.digest {
                Ok(Redelivery::Noop)
            } else {
                Err(conflict(&txn.txn, None))
            };
        }
        let mut pending = Vec::new();
        for event in &txn.events {
            let key = (
                event.source.clone(),
                event.txn.clone(),
                event.ordinal_in_txn,
            );
            let digest = event.digest()?;
            if self
                .events
                .get(&key)
                .is_some_and(|existing| *existing != digest)
            {
                return Err(conflict(&txn.txn, Some(event.ordinal_in_txn)));
            }
            pending.push((key, digest));
        }
        // An assembled transaction must also account for every earlier event.
        if self.events.keys().any(|(source, id, ordinal)| {
            source == &txn.source
                && id == &txn.txn
                && txn
                    .events
                    .binary_search_by_key(ordinal, |e| e.ordinal_in_txn)
                    .is_err()
        }) {
            return Err(conflict(&txn.txn, None));
        }
        self.events.extend(pending);
        self.txns.insert(key, txn.digest);
        Ok(Redelivery::New)
    }
}

fn conflict(txn: &TxnId, ordinal: Option<u64>) -> EventError {
    EventError::RedeliveryConflict {
        txn: txn.clone(),
        ordinal,
    }
}

fn sha256(bytes: &[u8]) -> ContentDigest {
    Sha256::digest(bytes).into()
}

fn header(kind: u8) -> Vec<u8> {
    let mut out = b"SKEV".to_vec();
    out.extend(ENCODING_VERSION.to_be_bytes());
    out.push(kind);
    out
}

struct HashWriter<W> {
    inner: W,
    hash: Sha256,
    bytes: u64,
}

impl<W> HashWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            hash: Sha256::new(),
            bytes: 0,
        }
    }

    fn digest(&self) -> ContentDigest {
        self.hash.clone().finalize().into()
    }
}

impl<W: Write> Write for HashWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(bytes)?;
        self.bytes = self.bytes.checked_add(written as u64).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "canonical record size overflow")
        })?;
        self.hash.update(&bytes[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

fn put_transaction_header(
    out: &mut impl Write,
    source: &SourceIdentity,
    txn: &TxnId,
    position: &Position,
    time: Option<i64>,
    origin: Option<&SourceOrigin>,
    count: usize,
) -> Result<(), EventError> {
    out.write_all(&header(2))?;
    put_source(out, source)?;
    put_bytes(out, &txn.0)?;
    put_position(out, position)?;
    put_option(out, time.as_ref(), |out, time| {
        out.write_all(&time.to_be_bytes())?;
        Ok(())
    })?;
    put_option(out, origin, |out, origin| {
        put_bytes(out, origin.name.as_bytes())?;
        out.write_all(&origin.lsn.to_be_bytes())?;
        Ok(())
    })?;
    put_len(out, count)
}

fn open(bytes: &[u8], kind: u8) -> Result<Reader<io::Cursor<&[u8]>>, EventError> {
    if bytes.len() < 39 {
        return Err(EventError::InvalidEncoding("missing envelope/digest"));
    }
    check_envelope(&bytes[..7], kind)?;
    let end = bytes.len() - 32;
    if sha256(&bytes[..end]).as_slice() != &bytes[end..] {
        return Err(EventError::DigestMismatch);
    }
    Ok(Reader {
        input: io::Cursor::new(&bytes[7..end]),
        remaining: (end - 7) as u64,
    })
}

fn check_envelope(bytes: &[u8], kind: u8) -> Result<(), EventError> {
    if &bytes[..4] != b"SKEV" {
        return Err(EventError::InvalidEncoding("magic"));
    }
    let version = u16::from_be_bytes([bytes[4], bytes[5]]);
    if version != ENCODING_VERSION {
        return Err(EventError::UnsupportedVersion(version));
    }
    if bytes[6] != kind {
        return Err(EventError::InvalidEncoding("record kind"));
    }
    Ok(())
}

struct HashRead<R> {
    inner: R,
    hash: Sha256,
}
impl<R: Read> Read for HashRead<R> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(bytes)?;
        self.hash.update(&bytes[..n]);
        Ok(n)
    }
}
fn stream_prefix<R: Read>(
    reader: R,
    length: u64,
    kind: u8,
) -> Result<Reader<HashRead<R>>, EventError> {
    if length < 39 {
        return Err(EventError::InvalidEncoding("missing envelope/digest"));
    }
    let mut input = Reader {
        input: HashRead {
            inner: reader,
            hash: Sha256::new(),
        },
        remaining: length - 32,
    };
    let envelope: [u8; 7] = input.array()?;
    check_envelope(&envelope, kind)?;
    Ok(input)
}
fn stream_finish<R: Read>(mut input: Reader<HashRead<R>>) -> Result<ContentDigest, EventError> {
    if input.remaining != 0 {
        return Err(EventError::InvalidEncoding("trailing bytes"));
    }
    let digest: ContentDigest = input.input.hash.finalize().into();
    let mut trailer = [0; 32];
    input.input.inner.read_exact(&mut trailer)?;
    if digest != trailer {
        return Err(EventError::DigestMismatch);
    }
    Ok(digest)
}

fn put_len(out: &mut impl Write, len: usize) -> Result<(), EventError> {
    out.write_all(
        &u32::try_from(len)
            .map_err(|_| EventError::InvalidEncoding("length exceeds u32"))?
            .to_be_bytes(),
    )?;
    Ok(())
}

fn put_bytes(out: &mut impl Write, bytes: &[u8]) -> Result<(), EventError> {
    put_len(out, bytes.len())?;
    out.write_all(bytes)?;
    Ok(())
}

fn put_source(out: &mut impl Write, source: &SourceIdentity) -> Result<(), EventError> {
    for field in [
        &source.engine,
        &source.system_id,
        &source.lineage,
        &source.db_name,
        &source.stream,
    ] {
        put_bytes(out, field.as_bytes())?;
    }
    Ok(())
}

fn put_position(out: &mut impl Write, position: &Position) -> Result<(), EventError> {
    put_bytes(out, position.order_domain.as_bytes())?;
    put_bytes(out, &position.bytes)
}

fn put_option<T, W: Write>(
    out: &mut W,
    value: Option<&T>,
    write: impl FnOnce(&mut W, &T) -> Result<(), EventError>,
) -> Result<(), EventError> {
    out.write_all(&[u8::from(value.is_some())])?;
    if let Some(value) = value {
        write(out, value)?;
    }
    Ok(())
}

fn put_row(out: &mut impl Write, row: &Row) -> Result<(), EventError> {
    put_len(out, row.len())?;
    for (column, value) in row {
        put_bytes(out, column.as_bytes())?;
        put_bytes(out, value.type_id.as_bytes())?;
        put_option(out, value.bytes.as_ref(), |out, bytes| {
            put_bytes(out, bytes)
        })?;
    }
    Ok(())
}

fn put_event(out: &mut impl Write, event: &Event) -> Result<(), EventError> {
    validate_truncate(event)?;
    put_source(out, &event.source)?;
    put_bytes(out, &event.txn.0)?;
    out.write_all(&event.ordinal_in_txn.to_be_bytes())?;
    put_position(out, &event.position)?;
    out.write_all(&event.schema_version.to_be_bytes())?;
    let tag = match &event.kind {
        EventKind::Insert => 0,
        EventKind::Update => 1,
        EventKind::Delete => 2,
        EventKind::Truncate { .. } => 3,
        EventKind::DdlBoundary => 4,
        EventKind::Watermark => 5,
        EventKind::FinalMarker => 6,
        EventKind::ChunkRows => 7,
    };
    out.write_all(&[tag])?;
    if let EventKind::Truncate {
        relations,
        cascade,
        restart_identity,
    } = &event.kind
    {
        put_len(out, relations.len())?;
        for relation in relations {
            put_bytes(out, relation.as_bytes())?;
        }
        out.write_all(&[u8::from(*cascade), u8::from(*restart_identity)])?;
    }
    put_option(out, event.table.as_ref(), |out, table| {
        put_bytes(out, table.as_bytes())
    })?;
    put_row(out, &event.key)?;
    put_option(out, event.before.as_ref(), put_row)?;
    put_option(out, event.after.as_ref(), put_row)?;
    put_len(out, event.unchanged_cols.len())?;
    for column in &event.unchanged_cols {
        put_bytes(out, column.as_bytes())?;
    }
    Ok(())
}

fn validate_truncate(event: &Event) -> Result<(), EventError> {
    if let EventKind::Truncate { relations, .. } = &event.kind {
        let mut seen = BTreeSet::new();
        if relations.is_empty()
            || relations.iter().any(|r| r.is_empty() || !seen.insert(r))
            || event.table.is_some()
            || !event.key.is_empty()
            || event.before.is_some()
            || event.after.is_some()
            || !event.unchanged_cols.is_empty()
        {
            return Err(EventError::InvalidEncoding("invalid grouped truncate"));
        }
    }
    Ok(())
}

struct Reader<R> {
    input: R,
    remaining: u64,
}

impl<R: Read> Reader<R> {
    fn array<const N: usize>(&mut self) -> Result<[u8; N], EventError> {
        if N as u64 > self.remaining {
            return Err(EventError::InvalidEncoding("truncated field"));
        }
        let mut value = [0; N];
        self.input.read_exact(&mut value)?;
        self.remaining -= N as u64;
        Ok(value)
    }

    fn byte(&mut self) -> Result<u8, EventError> {
        Ok(self.array::<1>()?[0])
    }

    fn count(&mut self) -> Result<usize, EventError> {
        let count = usize::try_from(u32::from_be_bytes(self.array()?))
            .map_err(|_| EventError::InvalidEncoding("count overflow"))?;
        if count as u64 > self.remaining {
            return Err(EventError::InvalidEncoding("count exceeds remaining bytes"));
        }
        Ok(count)
    }

    fn bytes(&mut self) -> Result<Vec<u8>, EventError> {
        let count = self.count()?;
        let mut value = vec![0; count];
        self.input.read_exact(&mut value)?;
        self.remaining -= count as u64;
        Ok(value)
    }

    fn string(&mut self) -> Result<String, EventError> {
        String::from_utf8(self.bytes()?).map_err(|_| EventError::InvalidEncoding("invalid UTF-8"))
    }

    fn option<T>(
        &mut self,
        read: impl FnOnce(&mut Self) -> Result<T, EventError>,
    ) -> Result<Option<T>, EventError> {
        match self.byte()? {
            0 => Ok(None),
            1 => read(self).map(Some),
            _ => Err(EventError::InvalidEncoding("option tag")),
        }
    }

    fn source(&mut self) -> Result<SourceIdentity, EventError> {
        Ok(SourceIdentity {
            engine: self.string()?,
            system_id: self.string()?,
            lineage: self.string()?,
            db_name: self.string()?,
            stream: self.string()?,
        })
    }

    fn position(&mut self) -> Result<Position, EventError> {
        Ok(Position::new(self.string()?, self.bytes()?))
    }

    fn origin(&mut self) -> Result<SourceOrigin, EventError> {
        Ok(SourceOrigin {
            name: self.string()?,
            lsn: u64::from_be_bytes(self.array()?),
        })
    }

    fn boolean(&mut self) -> Result<bool, EventError> {
        match self.byte()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(EventError::InvalidEncoding("boolean tag")),
        }
    }

    fn row(&mut self) -> Result<Row, EventError> {
        let count = self.count()?;
        let mut row = Row::new();
        for _ in 0..count {
            let column = self.string()?;
            if row
                .last_key_value()
                .is_some_and(|(last, _)| last >= &column)
            {
                return Err(EventError::InvalidEncoding(
                    "row columns not strictly increasing",
                ));
            }
            let value = OpaqueValue {
                type_id: self.string()?,
                bytes: self.option(Self::bytes)?,
            };
            row.insert(column, value);
        }
        Ok(row)
    }

    fn event(&mut self) -> Result<Event, EventError> {
        let source = self.source()?;
        let txn = TxnId(self.bytes()?);
        let ordinal_in_txn = u64::from_be_bytes(self.array()?);
        let position = self.position()?;
        let schema_version = u64::from_be_bytes(self.array()?);
        let kind = match self.byte()? {
            0 => EventKind::Insert,
            1 => EventKind::Update,
            2 => EventKind::Delete,
            3 => {
                let count = self.count()?;
                let mut relations = Vec::new();
                for _ in 0..count {
                    relations.push(self.string()?);
                }
                EventKind::Truncate {
                    relations,
                    cascade: self.boolean()?,
                    restart_identity: self.boolean()?,
                }
            }
            4 => EventKind::DdlBoundary,
            5 => EventKind::Watermark,
            6 => EventKind::FinalMarker,
            7 => EventKind::ChunkRows,
            _ => return Err(EventError::InvalidEncoding("event kind")),
        };
        let table = self.option(Self::string)?;
        let key = self.row()?;
        let before = self.option(Self::row)?;
        let after = self.option(Self::row)?;
        let count = self.count()?;
        let mut unchanged_cols = BTreeSet::new();
        for _ in 0..count {
            let column = self.string()?;
            if unchanged_cols.last().is_some_and(|last| last >= &column) {
                return Err(EventError::InvalidEncoding(
                    "unchanged columns not strictly increasing",
                ));
            }
            unchanged_cols.insert(column);
        }
        let event = Event {
            source,
            txn,
            ordinal_in_txn,
            position,
            schema_version,
            kind,
            table,
            key,
            before,
            after,
            unchanged_cols,
        };
        validate_truncate(&event)?;
        Ok(event)
    }

    fn finish(self) -> Result<(), EventError> {
        if self.remaining == 0 {
            Ok(())
        } else {
            Err(EventError::InvalidEncoding("trailing bytes"))
        }
    }
}
