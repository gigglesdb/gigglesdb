//! Bounded, allocation-conscious pgoutput protocol v1 decoding.
//!
//! Input is one complete CopyData payload or one complete pgoutput message;
//! neither includes the PostgreSQL message tag/length envelope. Identifiers
//! require UTF-8 (configure the replication connection accordingly); text tuple
//! values and logical-message content remain uninterpreted bytes. No relation
//! cache, I/O, acknowledgement advancement, or type conversion happens here.
//!
//! Wire reference: <https://www.postgresql.org/docs/18/protocol-logicalrep-message-formats.html>
//! and <https://www.postgresql.org/docs/18/protocol-replication.html>.

use std::{error::Error, fmt};

pub const MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_COLUMNS: usize = 1600;
pub const MAX_TRUNCATE_RELATIONS: usize = 4096;
/// XLogData's tag, two LSNs and signed PostgreSQL timestamp.
pub const XLOG_DATA_HEADER_BYTES: usize = 25;

pub type Result<T> = std::result::Result<T, DecodeError>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DecodeError {
    InvalidLimits,
    LimitExceeded {
        field: &'static str,
        actual: usize,
        maximum: usize,
    },
    Truncated {
        offset: usize,
        needed: usize,
        remaining: usize,
    },
    Invalid {
        offset: usize,
        field: &'static str,
    },
    UnsupportedTag(u8),
    /// Stream Start/Stop/Commit/Abort/Prepare must never occur in v1.
    StreamingDisabled(u8),
    UnsupportedTwoPhase(u8),
    TrailingBytes(usize),
    UnsafeAcknowledgement,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StreamingDisabled(tag) => write!(
                f,
                "pgoutput streaming message 0x{tag:02x} rejected: protocol v1 requires streaming off"
            ),
            Self::UnsafeAcknowledgement => f.write_str(
                "standby status violates ack rule: write/apply must not exceed flush, and flush must not exceed the durable manifest LSN",
            ),
            other => write!(f, "pgoutput protocol error: {other:?}"),
        }
    }
}

impl Error for DecodeError {}

/// Callers may lower the limits, but cannot disable the hard ceilings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    pub message_bytes: usize,
    pub columns: usize,
    pub truncate_relations: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            message_bytes: MAX_MESSAGE_BYTES,
            columns: MAX_COLUMNS,
            truncate_relations: MAX_TRUNCATE_RELATIONS,
        }
    }
}

impl Limits {
    pub(crate) fn validate(self) -> Result<Self> {
        if self.message_bytes == 0
            || self.message_bytes > MAX_MESSAGE_BYTES
            || self.columns == 0
            || self.columns > MAX_COLUMNS
            || self.truncate_relations == 0
            || self.truncate_relations > MAX_TRUNCATE_RELATIONS
        {
            return Err(DecodeError::InvalidLimits);
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Begin {
    pub final_lsn: u64,
    /// Signed microseconds since 2000-01-01, not Unix time.
    pub commit_time: i64,
    pub xid: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Commit {
    pub commit_lsn: u64,
    pub end_lsn: u64,
    pub commit_time: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplicaIdentity {
    Default,
    Nothing,
    Full,
    Index,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RelationColumn<'a> {
    pub key: bool,
    pub name: &'a str,
    pub type_oid: u32,
    pub type_modifier: i32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Relation<'a> {
    pub id: u32,
    /// An empty namespace means pg_catalog on the wire.
    pub namespace: &'a str,
    pub name: &'a str,
    pub replica_identity: ReplicaIdentity,
    pub columns: Vec<RelationColumn<'a>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Column<'a> {
    Null,
    UnchangedToast,
    Text(&'a [u8]),
    Binary(&'a [u8]),
}

pub type Tuple<'a> = Vec<Column<'a>>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OldTuple<'a> {
    Key(Tuple<'a>),
    Full(Tuple<'a>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message<'a> {
    Begin(Begin),
    Commit(Commit),
    Origin {
        commit_lsn: u64,
        name: &'a str,
    },
    Relation(Relation<'a>),
    Type {
        id: u32,
        namespace: &'a str,
        name: &'a str,
    },
    Insert {
        relation_id: u32,
        new: Tuple<'a>,
    },
    Update {
        relation_id: u32,
        old: Option<OldTuple<'a>>,
        new: Tuple<'a>,
    },
    Delete {
        relation_id: u32,
        old: OldTuple<'a>,
    },
    Truncate {
        cascade: bool,
        restart_identity: bool,
        relation_ids: Vec<u32>,
    },
    LogicalMessage {
        transactional: bool,
        lsn: u64,
        prefix: &'a str,
        content: &'a [u8],
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CopyData<'a> {
    XLogData {
        wal_start: u64,
        /// Server progress is informational; never use it as a durable ack.
        wal_end: u64,
        server_time: i64,
        data: &'a [u8],
    },
    PrimaryKeepalive {
        wal_end: u64,
        server_time: i64,
        reply_requested: bool,
    },
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Decoder {
    limits: Limits,
}

impl Decoder {
    pub fn new(limits: Limits) -> Result<Self> {
        Ok(Self {
            limits: limits.validate()?,
        })
    }

    pub fn limits(&self) -> Limits {
        self.limits
    }

    pub fn decode_copy_data<'a>(&self, bytes: &'a [u8]) -> Result<CopyData<'a>> {
        // Allow the XLogData header in addition to the logical message limit.
        limit(
            "CopyData bytes",
            bytes.len(),
            self.limits.message_bytes + XLOG_DATA_HEADER_BYTES,
        )?;
        let mut r = Cursor::new(bytes);
        let value = match r.byte()? {
            b'w' => {
                let wal_start = r.u64()?;
                let wal_end = r.u64()?;
                let server_time = r.i64()?;
                // A logical XLogData frame must contain a pgoutput message.
                r.require(1)?;
                let data = r.take(r.remaining())?;
                limit("message bytes", data.len(), self.limits.message_bytes)?;
                CopyData::XLogData {
                    wal_start,
                    wal_end,
                    server_time,
                    data,
                }
            }
            b'k' => CopyData::PrimaryKeepalive {
                wal_end: r.u64()?,
                server_time: r.i64()?,
                reply_requested: r.flag("keepalive reply flag")?,
            },
            tag => return Err(DecodeError::UnsupportedTag(tag)),
        };
        r.finish()?;
        Ok(value)
    }

    /// Strictly decodes exactly one message. The returned byte/string slices
    /// borrow input; allocations are limited to capped column/relation vectors.
    pub fn decode<'a>(&self, bytes: &'a [u8]) -> Result<Message<'a>> {
        limit("message bytes", bytes.len(), self.limits.message_bytes)?;
        let mut r = Cursor::new(bytes);
        let value = match r.byte()? {
            b'B' => Message::Begin(Begin {
                final_lsn: r.u64()?,
                commit_time: r.i64()?,
                xid: r.u32()?,
            }),
            b'C' => {
                r.expect(0, "commit flags")?;
                let commit = Commit {
                    commit_lsn: r.u64()?,
                    end_lsn: r.u64()?,
                    commit_time: r.i64()?,
                };
                if commit.end_lsn < commit.commit_lsn {
                    return Err(r.invalid("commit end LSN precedes commit LSN"));
                }
                Message::Commit(commit)
            }
            b'O' => Message::Origin {
                commit_lsn: r.u64()?,
                name: r.string()?,
            },
            b'R' => {
                let id = r.u32()?;
                let namespace = r.string()?;
                let name = r.string()?;
                let replica_identity = match r.byte()? {
                    b'd' => ReplicaIdentity::Default,
                    b'n' => ReplicaIdentity::Nothing,
                    b'f' => ReplicaIdentity::Full,
                    b'i' => ReplicaIdentity::Index,
                    _ => return Err(r.invalid("replica identity")),
                };
                let count = usize::from(r.u16()?);
                limit("relation columns", count, self.limits.columns)?;
                // The shortest column has flag + NUL + OID + typmod.
                r.require(count * 10)?;
                let mut columns = Vec::with_capacity(count);
                for _ in 0..count {
                    columns.push(RelationColumn {
                        key: r.flag("relation column flags")?,
                        name: r.string()?,
                        type_oid: r.u32()?,
                        type_modifier: r.i32()?,
                    });
                }
                Message::Relation(Relation {
                    id,
                    namespace,
                    name,
                    replica_identity,
                    columns,
                })
            }
            b'Y' => Message::Type {
                id: r.u32()?,
                namespace: r.string()?,
                name: r.string()?,
            },
            b'I' => {
                let relation_id = r.u32()?;
                r.expect(b'N', "insert new tuple tag")?;
                Message::Insert {
                    relation_id,
                    new: r.tuple(self.limits)?,
                }
            }
            b'U' => {
                let relation_id = r.u32()?;
                let tag = r.byte()?;
                let old = match tag {
                    b'K' | b'O' => {
                        let old = r.old_tuple(tag, self.limits)?;
                        r.expect(b'N', "update new tuple tag")?;
                        Some(old)
                    }
                    b'N' => None,
                    _ => return Err(r.invalid("update tuple tag")),
                };
                Message::Update {
                    relation_id,
                    old,
                    new: r.tuple(self.limits)?,
                }
            }
            b'D' => {
                let relation_id = r.u32()?;
                let tag = r.byte()?;
                Message::Delete {
                    relation_id,
                    old: r.old_tuple(tag, self.limits)?,
                }
            }
            b'T' => {
                let count = r.u32()? as usize;
                limit("truncate relations", count, self.limits.truncate_relations)?;
                let flags = r.byte()?;
                if flags & !3 != 0 {
                    return Err(r.invalid("truncate flags"));
                }
                r.require(count * 4)?;
                let mut relation_ids = Vec::with_capacity(count);
                for _ in 0..count {
                    relation_ids.push(r.u32()?);
                }
                Message::Truncate {
                    cascade: flags & 1 != 0,
                    restart_identity: flags & 2 != 0,
                    relation_ids,
                }
            }
            b'M' => {
                let transactional = r.flag("logical message flags")?;
                let lsn = r.u64()?;
                let prefix = r.string()?;
                let length = r.u32()? as usize;
                limit(
                    "logical message content bytes",
                    length,
                    self.limits.message_bytes,
                )?;
                Message::LogicalMessage {
                    transactional,
                    lsn,
                    prefix,
                    content: r.take(length)?,
                }
            }
            tag @ (b'S' | b'E' | b'c' | b'A' | b'p') => {
                return Err(DecodeError::StreamingDisabled(tag));
            }
            tag @ (b'b' | b'P' | b'K' | b'r') => return Err(DecodeError::UnsupportedTwoPhase(tag)),
            tag => return Err(DecodeError::UnsupportedTag(tag)),
        };
        r.finish()?;
        Ok(value)
    }
}

/// The caller supplies the last manifest-durable transaction boundary. This
/// encoder cannot establish durability itself and never advances a position.
/// Unlike the S1 spike, the plan requires write AND apply <= flush.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StandbyStatusUpdate {
    pub write_lsn: u64,
    pub flush_lsn: u64,
    pub apply_lsn: u64,
    /// Signed microseconds since PostgreSQL epoch (2000-01-01).
    pub client_time: i64,
    /// Request a server reply; responding to a keepalive normally sets false.
    pub reply_requested: bool,
}

impl StandbyStatusUpdate {
    /// Encodes the 34-byte payload, without a CopyData envelope.
    pub fn encode(self, durable_manifest_lsn: u64) -> Result<[u8; 34]> {
        if self.flush_lsn > durable_manifest_lsn
            || self.write_lsn > self.flush_lsn
            || self.apply_lsn > self.flush_lsn
        {
            return Err(DecodeError::UnsafeAcknowledgement);
        }
        let mut out = [0; 34];
        out[0] = b'r';
        out[1..9].copy_from_slice(&self.write_lsn.to_be_bytes());
        out[9..17].copy_from_slice(&self.flush_lsn.to_be_bytes());
        out[17..25].copy_from_slice(&self.apply_lsn.to_be_bytes());
        out[25..33].copy_from_slice(&self.client_time.to_be_bytes());
        out[33] = u8::from(self.reply_requested);
        Ok(out)
    }

    /// Encodes the complete frontend CopyData frame (tag + length + payload).
    pub fn encode_copy_data(self, durable_manifest_lsn: u64) -> Result<[u8; 39]> {
        let payload = self.encode(durable_manifest_lsn)?;
        let mut frame = [0; 39];
        frame[0] = b'd';
        frame[1..5].copy_from_slice(&38_u32.to_be_bytes());
        frame[5..].copy_from_slice(&payload);
        Ok(frame)
    }
}

pub(crate) fn limit(field: &'static str, actual: usize, maximum: usize) -> Result<()> {
    if actual > maximum {
        Err(DecodeError::LimitExceeded {
            field,
            actual,
            maximum,
        })
    } else {
        Ok(())
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }
    fn remaining(&self) -> usize {
        self.bytes.len() - self.offset
    }
    fn require(&self, needed: usize) -> Result<()> {
        if needed > self.remaining() {
            Err(DecodeError::Truncated {
                offset: self.offset,
                needed,
                remaining: self.remaining(),
            })
        } else {
            Ok(())
        }
    }
    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        self.require(length)?;
        let start = self.offset;
        self.offset += length;
        Ok(&self.bytes[start..self.offset])
    }
    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut bytes = [0; N];
        bytes.copy_from_slice(self.take(N)?);
        Ok(bytes)
    }
    fn byte(&mut self) -> Result<u8> {
        Ok(self.array::<1>()?[0])
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.array()?))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.array()?))
    }
    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(self.array()?))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.array()?))
    }
    fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_be_bytes(self.array()?))
    }
    fn invalid(&self, field: &'static str) -> DecodeError {
        DecodeError::Invalid {
            offset: self.offset,
            field,
        }
    }
    fn expect(&mut self, expected: u8, field: &'static str) -> Result<()> {
        if self.byte()? == expected {
            Ok(())
        } else {
            Err(self.invalid(field))
        }
    }
    fn flag(&mut self, field: &'static str) -> Result<bool> {
        match self.byte()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(self.invalid(field)),
        }
    }
    fn string(&mut self) -> Result<&'a str> {
        let length = self.bytes[self.offset..]
            .iter()
            .position(|b| *b == 0)
            .ok_or_else(|| self.invalid("unterminated string"))?;
        let bytes = self.take(length)?;
        self.byte()?;
        std::str::from_utf8(bytes).map_err(|_| self.invalid("non-UTF-8 identifier"))
    }
    fn tuple(&mut self, limits: Limits) -> Result<Tuple<'a>> {
        let count = usize::from(self.u16()?);
        limit("tuple columns", count, limits.columns)?;
        self.require(count)?;
        let mut columns = Vec::with_capacity(count);
        for _ in 0..count {
            columns.push(match self.byte()? {
                b'n' => Column::Null,
                b'u' => Column::UnchangedToast,
                tag @ (b't' | b'b') => {
                    let length = self.u32()? as usize;
                    limit("column bytes", length, limits.message_bytes)?;
                    let bytes = self.take(length)?;
                    if tag == b't' {
                        Column::Text(bytes)
                    } else {
                        Column::Binary(bytes)
                    }
                }
                _ => return Err(self.invalid("tuple column tag")),
            });
        }
        Ok(columns)
    }
    fn old_tuple(&mut self, tag: u8, limits: Limits) -> Result<OldTuple<'a>> {
        match tag {
            b'K' => self.tuple(limits).map(OldTuple::Key),
            b'O' => self.tuple(limits).map(OldTuple::Full),
            _ => Err(self.invalid("old tuple tag")),
        }
    }
    fn finish(&self) -> Result<()> {
        if self.remaining() == 0 {
            Ok(())
        } else {
            Err(DecodeError::TrailingBytes(self.remaining()))
        }
    }
}
