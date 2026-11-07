//! Engine-neutral events and canonical version 1 binary records.
//!
//! Integers and lengths are big-endian (lengths/counts are u32). A record is
//! `SKEV | version:u16 | kind:u8 | fields | SHA-256(prefix)`. Kinds 1 and 2
//! identify events and transactions. Fields follow declaration order below;
//! strings/bytes have length prefixes, options have 0/1 tags, maps/sets are
//! strictly increasing by UTF-8 column name. Transactions embed event fields
//! without nested envelopes. Their stored digest is the envelope's trailer,
//! excluded from its own preimage. Decoders reject noncanonical representations.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashMap},
    error::Error,
    fmt,
};

use sha2::{Digest, Sha256};

pub const ENCODING_VERSION: u16 = 1;
pub type ContentDigest = [u8; 32];

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SourceIdentity {
    pub engine: String,
    pub system_id: String,
    pub lineage: String,
    pub db_name: String,
    pub slot: String,
}

/// Connector bytes and their ordering domain. Deliberately has no Ord or
/// PartialOrd implementation and no timestamp conversion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Position {
    order_domain: String,
    bytes: Vec<u8>,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum EventKind {
    Insert = 0,
    Update = 1,
    Delete = 2,
    Truncate = 3,
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
    /// None for source-wide control records.
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
    pub events: Vec<Event>,
    pub digest: ContentDigest,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventError {
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

impl Event {
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, EventError> {
        let mut out = header(1);
        put_event(&mut out, self)?;
        Ok(seal(out))
    }

    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self, EventError> {
        let mut input = open(bytes, 1)?;
        let event = input.event()?;
        input.finish()?;
        Ok(event)
    }

    /// SHA-256 of the versioned canonical prefix, excluding the digest trailer.
    pub fn digest(&self) -> Result<ContentDigest, EventError> {
        let mut out = header(1);
        put_event(&mut out, self)?;
        Ok(sha256(&out))
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
        let mut txn = Self {
            source,
            txn,
            commit_position,
            commit_time,
            events,
            digest: [0; 32],
        };
        txn.digest = sha256(&txn.prefix()?);
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

    fn prefix(&self) -> Result<Vec<u8>, EventError> {
        self.validate()?;
        let mut out = header(2);
        put_source(&mut out, &self.source)?;
        put_bytes(&mut out, &self.txn.0)?;
        put_position(&mut out, &self.commit_position)?;
        put_option(&mut out, self.commit_time.as_ref(), |out, time| {
            out.extend(time.to_be_bytes());
            Ok(())
        })?;
        put_len(&mut out, self.events.len())?;
        for event in &self.events {
            put_event(&mut out, event)?;
        }
        Ok(out)
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>, EventError> {
        let out = self.prefix()?;
        if sha256(&out) != self.digest {
            return Err(EventError::DigestMismatch);
        }
        Ok(seal(out))
    }

    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self, EventError> {
        let mut input = open(bytes, 2)?;
        let source = input.source()?;
        let txn = TxnId(input.bytes()?);
        let commit_position = input.position()?;
        let commit_time = input.option(|input| Ok(i64::from_be_bytes(input.array()?)))?;
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
        txn.canonical_bytes()?;
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

fn seal(mut out: Vec<u8>) -> Vec<u8> {
    let digest = sha256(&out);
    out.extend(digest);
    out
}

fn open(bytes: &[u8], kind: u8) -> Result<Reader<'_>, EventError> {
    let mut input = Reader { bytes };
    if input.take(4)? != b"SKEV" {
        return Err(EventError::InvalidEncoding("magic"));
    }
    let version = u16::from_be_bytes(input.array()?);
    if version != ENCODING_VERSION {
        return Err(EventError::UnsupportedVersion(version));
    }
    if input.byte()? != kind {
        return Err(EventError::InvalidEncoding("record kind"));
    }
    if input.bytes.len() < 32 {
        return Err(EventError::InvalidEncoding("missing digest"));
    }
    let end = bytes.len() - 32;
    if sha256(&bytes[..end]).as_slice() != &bytes[end..] {
        return Err(EventError::DigestMismatch);
    }
    Ok(Reader {
        bytes: &bytes[7..end],
    })
}

fn put_len(out: &mut Vec<u8>, len: usize) -> Result<(), EventError> {
    out.extend(
        u32::try_from(len)
            .map_err(|_| EventError::InvalidEncoding("length exceeds u32"))?
            .to_be_bytes(),
    );
    Ok(())
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), EventError> {
    put_len(out, bytes.len())?;
    out.extend(bytes);
    Ok(())
}

fn put_source(out: &mut Vec<u8>, source: &SourceIdentity) -> Result<(), EventError> {
    for field in [
        &source.engine,
        &source.system_id,
        &source.lineage,
        &source.db_name,
        &source.slot,
    ] {
        put_bytes(out, field.as_bytes())?;
    }
    Ok(())
}

fn put_position(out: &mut Vec<u8>, position: &Position) -> Result<(), EventError> {
    put_bytes(out, position.order_domain.as_bytes())?;
    put_bytes(out, &position.bytes)
}

fn put_option<T>(
    out: &mut Vec<u8>,
    value: Option<&T>,
    write: impl FnOnce(&mut Vec<u8>, &T) -> Result<(), EventError>,
) -> Result<(), EventError> {
    out.push(u8::from(value.is_some()));
    if let Some(value) = value {
        write(out, value)?;
    }
    Ok(())
}

fn put_row(out: &mut Vec<u8>, row: &Row) -> Result<(), EventError> {
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

fn put_event(out: &mut Vec<u8>, event: &Event) -> Result<(), EventError> {
    put_source(out, &event.source)?;
    put_bytes(out, &event.txn.0)?;
    out.extend(event.ordinal_in_txn.to_be_bytes());
    put_position(out, &event.position)?;
    out.extend(event.schema_version.to_be_bytes());
    out.push(event.kind as u8);
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

struct Reader<'a> {
    bytes: &'a [u8],
}

impl<'a> Reader<'a> {
    fn take(&mut self, count: usize) -> Result<&'a [u8], EventError> {
        let value = self
            .bytes
            .get(..count)
            .ok_or(EventError::InvalidEncoding("truncated field"))?;
        self.bytes = &self.bytes[count..];
        Ok(value)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], EventError> {
        self.take(N)?
            .try_into()
            .map_err(|_| EventError::InvalidEncoding("fixed width"))
    }

    fn byte(&mut self) -> Result<u8, EventError> {
        Ok(self.array::<1>()?[0])
    }

    fn count(&mut self) -> Result<usize, EventError> {
        let count = usize::try_from(u32::from_be_bytes(self.array()?))
            .map_err(|_| EventError::InvalidEncoding("count overflow"))?;
        if count > self.bytes.len() {
            return Err(EventError::InvalidEncoding("count exceeds remaining bytes"));
        }
        Ok(count)
    }

    fn bytes(&mut self) -> Result<Vec<u8>, EventError> {
        let count = self.count()?;
        Ok(self.take(count)?.to_vec())
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
            slot: self.string()?,
        })
    }

    fn position(&mut self) -> Result<Position, EventError> {
        Ok(Position::new(self.string()?, self.bytes()?))
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
            3 => EventKind::Truncate,
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
        Ok(Event {
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
        })
    }

    fn finish(self) -> Result<(), EventError> {
        if self.bytes.is_empty() {
            Ok(())
        } else {
            Err(EventError::InvalidEncoding("trailing bytes"))
        }
    }
}
