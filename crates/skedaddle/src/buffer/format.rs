//! Immutable version 1 segments and manifest entries.
//!
//! All binary integers are big endian. The layout is `MAGIC | version:u16 |
//! header_len:u32 | header | (record_len:u64 | canonical_record)* | 0:u64 |
//! END_MAGIC | actual_count:u64 | SHA-256(all preceding segment bytes)`.
//! Header fields follow [`SegmentHeader`] declaration order; strings/bytes have
//! u32 lengths and positions contain domain then bytes. Transaction records are
//! exact P3a canonical envelopes, including SHA-256. A ChunkRows record is
//! `SKCR | version:u16 | P3a AssembledTxn | SHA-256(prefix)`. Its events are all
//! snapshot rows at H, grouped into one atomic apply unit. Empty chunks are
//! supported. No transaction or event encoding is duplicated here.
//!
//! Memory is O(max header + one bounded record), independent of segment size.
//! The P3a codec materializes one record; it never materializes the segment.
//! Decoder records are provisional until `next_record()` returns None or
//! `finish()` succeeds: only then are the count, trailer and EOF verified.
//! Any error poisons the stream; retrying after a partial I/O cannot seal it.

use std::{
    error::Error,
    fmt,
    io::{self, Read, Write},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::event::{AssembledTxn, ContentDigest, EventError, EventKind, Position};

pub const MAGIC: &[u8; 8] = b"SKEDSEG\0";
pub const END_MAGIC: &[u8; 8] = b"SKEDEND\0";
pub const FORMAT_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_header_bytes: u64,
    pub max_record_bytes: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_header_bytes: 64 * 1024,
            max_record_bytes: 16 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentHeader {
    pub migration_id: String,
    pub stream: String,
    pub epoch: u64,
    pub first_pos: Position,
    pub last_pos: Position,
    pub record_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SegmentRecord {
    Transaction(AssembledTxn),
    /// One atomic snapshot batch using the existing transaction/row encoding.
    /// All events must be ChunkRows for the same table/schema and position H.
    ChunkRows(AssembledTxn),
}

impl SegmentRecord {
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, FormatError> {
        match self {
            Self::Transaction(txn) => Ok(txn.canonical_bytes()?),
            Self::ChunkRows(txn) => {
                validate_chunk(txn)?;
                let inner = txn.canonical_bytes()?;
                let mut bytes = b"SKCR".to_vec();
                bytes.extend(FORMAT_VERSION.to_be_bytes());
                bytes.extend(inner);
                let digest = Sha256::digest(&bytes);
                bytes.extend(digest);
                Ok(bytes)
            }
        }
    }

    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self, FormatError> {
        // P3a validates magic, version, checksum, fields and membership.
        let chunk = bytes.starts_with(b"SKCR");
        let inner = if chunk {
            if bytes.len() < 38 {
                return Err(FormatError::Truncated("ChunkRows envelope"));
            }
            let version = u16::from_be_bytes([bytes[4], bytes[5]]);
            if version != FORMAT_VERSION {
                return Err(FormatError::UnsupportedVersion(version));
            }
            let end = bytes.len() - 32;
            if Sha256::digest(&bytes[..end]).as_slice() != &bytes[end..] {
                return Err(FormatError::RecordDigestMismatch);
            }
            &bytes[6..end]
        } else {
            bytes
        };
        let txn = AssembledTxn::from_canonical_bytes(inner).map_err(|error| match error {
            EventError::DigestMismatch => FormatError::RecordDigestMismatch,
            other => FormatError::Event(other),
        })?;
        if chunk {
            validate_chunk(&txn)?;
            Ok(Self::ChunkRows(txn))
        } else {
            Ok(Self::Transaction(txn))
        }
    }

    pub fn position(&self) -> &Position {
        match self {
            Self::Transaction(txn) => &txn.commit_position,
            Self::ChunkRows(txn) => &txn.commit_position,
        }
    }
}

fn validate_chunk(txn: &AssembledTxn) -> Result<(), FormatError> {
    for event in &txn.events {
        if event.kind != EventKind::ChunkRows {
            return Err(FormatError::Invalid("expected ChunkRows event"));
        }
        if event.position != txn.commit_position {
            return Err(FormatError::Invalid("ChunkRows watermark"));
        }
        if txn.events.first().is_some_and(|first| {
            event.table != first.table || event.schema_version != first.schema_version
        }) {
            return Err(FormatError::Invalid("ChunkRows table/schema"));
        }
    }
    Ok(())
}

#[derive(Debug)]
pub enum FormatError {
    Io(io::Error),
    WrongMagic,
    UnsupportedVersion(u16),
    Truncated(&'static str),
    LengthLimit {
        field: &'static str,
        length: u64,
        maximum: u64,
    },
    Allocation,
    RecordDigestMismatch,
    TrailerDigestMismatch,
    CountMismatch {
        expected: u64,
        actual: u64,
    },
    TrailingGarbage,
    Invalid(&'static str),
    Event(EventError),
    Json(serde_json::Error),
    ManifestDigestMismatch,
    Poisoned,
}

impl fmt::Display for FormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "segment I/O: {e}"),
            Self::WrongMagic => f.write_str("wrong segment magic"),
            Self::UnsupportedVersion(v) => write!(f, "unsupported segment version {v}"),
            Self::Truncated(part) => write!(f, "truncated {part}"),
            Self::LengthLimit {
                field,
                length,
                maximum,
            } => write!(f, "{field} length {length} exceeds maximum {maximum}"),
            Self::Allocation => f.write_str("buffer allocation failed"),
            Self::RecordDigestMismatch => f.write_str("record digest mismatch"),
            Self::TrailerDigestMismatch => f.write_str("segment trailer digest mismatch"),
            Self::CountMismatch { expected, actual } => write!(
                f,
                "record count mismatch: expected {expected}, got {actual}"
            ),
            Self::TrailingGarbage => f.write_str("trailing segment garbage"),
            Self::Invalid(reason) => write!(f, "invalid buffer encoding: {reason}"),
            Self::Event(e) => write!(f, "canonical record: {e}"),
            Self::Json(e) => write!(f, "manifest JSON: {e}"),
            Self::ManifestDigestMismatch => f.write_str("manifest digest mismatch"),
            Self::Poisoned => f.write_str("segment stream failed previously"),
        }
    }
}

impl Error for FormatError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::Event(e) => Some(e),
            Self::Json(e) => Some(e),
            _ => None,
        }
    }
}
impl From<io::Error> for FormatError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<EventError> for FormatError {
    fn from(e: EventError) -> Self {
        Self::Event(e)
    }
}
impl From<serde_json::Error> for FormatError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentSummary {
    pub digest: ContentDigest,
    pub record_count: u64,
    pub bytes: u64,
    /// Largest owned encoded header/record Vec capacity (not caller-owned I/O
    /// or decoded objects). The encoder's P3a codec temporarily uses two such
    /// buffers; total working memory remains O(one record).
    pub peak_buffer_bytes: usize,
}

pub struct SegmentEncoder<W> {
    writer: W,
    header: SegmentHeader,
    limits: Limits,
    hash: Sha256,
    count: u64,
    bytes: u64,
    peak: usize,
    poisoned: bool,
}

impl<W: Write> SegmentEncoder<W> {
    pub fn new(writer: W, header: SegmentHeader, limits: Limits) -> Result<Self, FormatError> {
        let encoded = encode_header(&header, limits)?;
        let mut encoder = Self {
            writer,
            header,
            limits,
            hash: Sha256::new(),
            count: 0,
            bytes: 0,
            peak: encoded.capacity(),
            poisoned: false,
        };
        encoder.write(MAGIC)?;
        encoder.write(&FORMAT_VERSION.to_be_bytes())?;
        encoder.write(&(encoded.len() as u32).to_be_bytes())?;
        encoder.write(&encoded)?;
        Ok(encoder)
    }

    fn write(&mut self, bytes: &[u8]) -> Result<(), FormatError> {
        self.writer.write_all(bytes)?;
        self.hash.update(bytes);
        self.bytes = self
            .bytes
            .checked_add(bytes.len() as u64)
            .ok_or(FormatError::Invalid("segment size overflow"))?;
        Ok(())
    }

    pub fn write_record(&mut self, record: &SegmentRecord) -> Result<(), FormatError> {
        if self.poisoned {
            return Err(FormatError::Poisoned);
        }
        self.poisoned = true;
        if self.count >= self.header.record_count {
            return Err(FormatError::CountMismatch {
                expected: self.header.record_count,
                actual: self.count.saturating_add(1),
            });
        }
        check_position(&self.header, record.position(), self.count)?;
        let encoded = record.canonical_bytes()?;
        check_length("record", encoded.len() as u64, self.limits.max_record_bytes)?;
        self.peak = self.peak.max(encoded.capacity());
        self.write(&(encoded.len() as u64).to_be_bytes())?;
        self.write(&encoded)?;
        self.count += 1;
        self.poisoned = false;
        Ok(())
    }

    /// Writes and flushes the trailer. Flush alone does not imply backend
    /// durability; segment put and manifest put are a later layer's contract.
    pub fn finish(mut self) -> Result<(W, SegmentSummary), FormatError> {
        if self.poisoned {
            return Err(FormatError::Poisoned);
        }
        if self.count != self.header.record_count {
            return Err(FormatError::CountMismatch {
                expected: self.header.record_count,
                actual: self.count,
            });
        }
        self.write(&0u64.to_be_bytes())?;
        self.write(END_MAGIC)?;
        self.write(&self.count.to_be_bytes())?;
        let digest: ContentDigest = self.hash.clone().finalize().into();
        self.writer.write_all(&digest)?;
        self.writer.flush()?;
        let bytes = self
            .bytes
            .checked_add(32)
            .ok_or(FormatError::Invalid("segment size overflow"))?;
        Ok((
            self.writer,
            SegmentSummary {
                digest,
                record_count: self.count,
                bytes,
                peak_buffer_bytes: self.peak,
            },
        ))
    }
}

pub struct SegmentDecoder<R> {
    reader: R,
    header: SegmentHeader,
    limits: Limits,
    hash: Sha256,
    count: u64,
    bytes: u64,
    peak: usize,
    poisoned: bool,
    summary: Option<SegmentSummary>,
}

impl<R: Read> SegmentDecoder<R> {
    pub fn new(mut reader: R, limits: Limits) -> Result<Self, FormatError> {
        let mut hash = Sha256::new();
        let mut preamble = [0u8; 14];
        read_exact(&mut reader, &mut preamble, "header")?;
        if &preamble[..8] != MAGIC {
            return Err(FormatError::WrongMagic);
        }
        let version = u16::from_be_bytes([preamble[8], preamble[9]]);
        if version != FORMAT_VERSION {
            return Err(FormatError::UnsupportedVersion(version));
        }
        let length =
            u32::from_be_bytes([preamble[10], preamble[11], preamble[12], preamble[13]]) as u64;
        let mut encoded = allocate("header", length, limits.max_header_bytes)?;
        read_exact(&mut reader, &mut encoded, "header")?;
        hash.update(preamble);
        hash.update(&encoded);
        let header = decode_header(&encoded)?;
        Ok(Self {
            reader,
            header,
            limits,
            hash,
            count: 0,
            bytes: 14 + length,
            peak: encoded.capacity(),
            poisoned: false,
            summary: None,
        })
    }

    pub fn header(&self) -> &SegmentHeader {
        &self.header
    }

    /// Peak capacity of the single encoded buffer. Decoded objects returned to
    /// the caller are not retained by this decoder.
    pub fn peak_buffer_bytes(&self) -> usize {
        self.peak
    }

    fn read(&mut self, bytes: &mut [u8], part: &'static str) -> Result<(), FormatError> {
        read_exact(&mut self.reader, bytes, part)?;
        self.hash.update(&*bytes);
        self.bytes = self
            .bytes
            .checked_add(bytes.len() as u64)
            .ok_or(FormatError::Invalid("segment size overflow"))?;
        Ok(())
    }

    pub fn next_record(&mut self) -> Result<Option<SegmentRecord>, FormatError> {
        if self.poisoned {
            return Err(FormatError::Poisoned);
        }
        if self.summary.is_some() {
            return Ok(None);
        }
        self.poisoned = true;
        let value = self.read_next()?;
        self.poisoned = false;
        Ok(value)
    }

    fn read_next(&mut self) -> Result<Option<SegmentRecord>, FormatError> {
        let mut prefix = [0u8; 8];
        self.read(&mut prefix, "record length")?;
        let length = u64::from_be_bytes(prefix);
        if length == 0 {
            let mut magic = [0u8; 8];
            self.read(&mut magic, "trailer")?;
            if &magic != END_MAGIC {
                return Err(FormatError::WrongMagic);
            }
            let mut actual = [0u8; 8];
            self.read(&mut actual, "trailer count")?;
            let actual = u64::from_be_bytes(actual);
            if self.count != self.header.record_count {
                return Err(FormatError::CountMismatch {
                    expected: self.header.record_count,
                    actual: self.count,
                });
            }
            if actual != self.count {
                return Err(FormatError::CountMismatch {
                    expected: self.count,
                    actual,
                });
            }
            let mut digest = [0u8; 32];
            read_exact(&mut self.reader, &mut digest, "trailer digest")?;
            if digest != ContentDigest::from(self.hash.clone().finalize()) {
                return Err(FormatError::TrailerDigestMismatch);
            }
            let mut extra = [0u8; 1];
            loop {
                match self.reader.read(&mut extra) {
                    Ok(0) => break,
                    Ok(_) => return Err(FormatError::TrailingGarbage),
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                    Err(e) => return Err(e.into()),
                }
            }
            self.bytes = self
                .bytes
                .checked_add(32)
                .ok_or(FormatError::Invalid("segment size overflow"))?;
            self.summary = Some(SegmentSummary {
                digest,
                record_count: self.count,
                bytes: self.bytes,
                peak_buffer_bytes: self.peak,
            });
            return Ok(None);
        }
        // Guard precedes allocation, reading the payload, and count checks.
        check_length("record", length, self.limits.max_record_bytes)?;
        if self.count >= self.header.record_count {
            return Err(FormatError::CountMismatch {
                expected: self.header.record_count,
                actual: self.count.saturating_add(1),
            });
        }
        let mut encoded = allocate("record", length, self.limits.max_record_bytes)?;
        self.peak = self.peak.max(encoded.capacity());
        self.read(&mut encoded, "record")?;
        let record = SegmentRecord::from_canonical_bytes(&encoded)?;
        check_position(&self.header, record.position(), self.count)?;
        self.count += 1;
        Ok(Some(record))
    }

    /// Drains unconsumed records and verifies the complete segment and EOF.
    pub fn finish(mut self) -> Result<(R, SegmentSummary), FormatError> {
        while self.next_record()?.is_some() {}
        let summary = self
            .summary
            .ok_or(FormatError::Invalid("missing trailer"))?;
        Ok((self.reader, summary))
    }
}

fn read_exact(
    reader: &mut impl Read,
    bytes: &mut [u8],
    part: &'static str,
) -> Result<(), FormatError> {
    reader.read_exact(bytes).map_err(|e| {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            FormatError::Truncated(part)
        } else {
            FormatError::Io(e)
        }
    })
}

fn check_length(field: &'static str, length: u64, maximum: u64) -> Result<(), FormatError> {
    if length > maximum {
        Err(FormatError::LengthLimit {
            field,
            length,
            maximum,
        })
    } else {
        Ok(())
    }
}

fn allocate(field: &'static str, length: u64, maximum: u64) -> Result<Vec<u8>, FormatError> {
    check_length(field, length, maximum)?;
    let length = usize::try_from(length).map_err(|_| FormatError::Allocation)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|_| FormatError::Allocation)?;
    bytes.resize(length, 0);
    Ok(bytes)
}

fn check_position(
    header: &SegmentHeader,
    position: &Position,
    index: u64,
) -> Result<(), FormatError> {
    if position.order_domain() != header.first_pos.order_domain() {
        return Err(FormatError::Invalid("position domain"));
    }
    if (index == 0 && position != &header.first_pos)
        || (index.checked_add(1) == Some(header.record_count) && position != &header.last_pos)
    {
        return Err(FormatError::Invalid("segment endpoint position"));
    }
    Ok(())
}

fn validate_header(header: &SegmentHeader) -> Result<(), FormatError> {
    if header.first_pos.order_domain() != header.last_pos.order_domain() {
        return Err(FormatError::Invalid("header position domains"));
    }
    if header.record_count == 1 && header.first_pos != header.last_pos {
        return Err(FormatError::Invalid("single-record endpoints"));
    }
    Ok(())
}

fn encode_header(header: &SegmentHeader, limits: Limits) -> Result<Vec<u8>, FormatError> {
    validate_header(header)?;
    let fields = [
        header.migration_id.as_bytes(),
        header.stream.as_bytes(),
        header.first_pos.order_domain().as_bytes(),
        header.first_pos.bytes(),
        header.last_pos.order_domain().as_bytes(),
        header.last_pos.bytes(),
    ];
    let mut length = 16u64;
    for field in fields {
        check_length("header field", field.len() as u64, u32::MAX as u64)?;
        length = length
            .checked_add(4 + field.len() as u64)
            .ok_or(FormatError::Invalid("header overflow"))?;
    }
    let mut out = allocate(
        "header",
        length,
        limits.max_header_bytes.min(u32::MAX as u64),
    )?;
    // Exact size is known before allocation; write into the allocated slice.
    let mut writer = out.as_mut_slice();
    for field in &fields[..2] {
        put_bytes(&mut writer, field)?;
    }
    writer.write_all(&header.epoch.to_be_bytes())?;
    for field in &fields[2..] {
        put_bytes(&mut writer, field)?;
    }
    writer.write_all(&header.record_count.to_be_bytes())?;
    Ok(out)
}

fn put_bytes(out: &mut impl Write, bytes: &[u8]) -> Result<(), FormatError> {
    let length = u32::try_from(bytes.len()).map_err(|_| FormatError::Invalid("field length"))?;
    out.write_all(&length.to_be_bytes())?;
    out.write_all(bytes)?;
    Ok(())
}

struct HeaderReader<'a>(&'a [u8]);
impl HeaderReader<'_> {
    fn take(&mut self, length: usize) -> Result<&[u8], FormatError> {
        let bytes = self
            .0
            .get(..length)
            .ok_or(FormatError::Truncated("header field"))?;
        self.0 = &self.0[length..];
        Ok(bytes)
    }
    fn array<const N: usize>(&mut self) -> Result<[u8; N], FormatError> {
        self.take(N)?
            .try_into()
            .map_err(|_| FormatError::Invalid("header integer"))
    }
    fn bytes(&mut self) -> Result<Vec<u8>, FormatError> {
        let length = usize::try_from(u32::from_be_bytes(self.array()?))
            .map_err(|_| FormatError::Allocation)?;
        Ok(self.take(length)?.to_vec())
    }
    fn string(&mut self) -> Result<String, FormatError> {
        String::from_utf8(self.bytes()?).map_err(|_| FormatError::Invalid("header UTF-8"))
    }
    fn position(&mut self) -> Result<Position, FormatError> {
        Ok(Position::new(self.string()?, self.bytes()?))
    }
}

fn decode_header(encoded: &[u8]) -> Result<SegmentHeader, FormatError> {
    let mut input = HeaderReader(encoded);
    let header = SegmentHeader {
        migration_id: input.string()?,
        stream: input.string()?,
        epoch: u64::from_be_bytes(input.array()?),
        first_pos: input.position()?,
        last_pos: input.position()?,
        record_count: u64::from_be_bytes(input.array()?),
    };
    if !input.0.is_empty() {
        return Err(FormatError::Invalid("trailing header fields"));
    }
    validate_header(&header)?;
    Ok(header)
}

/// Manifest digest is SHA-256 of compact, recursively key-sorted UTF-8 JSON
/// containing these nine fields (excluding the self-digest). Serialized entries
/// add `digest`. Digests and opaque position bytes are lowercase hex; positions
/// also retain their connector domain. No timestamps or global ordering appear.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestEntry {
    pub mseq: u64,
    pub epoch: u64,
    pub prev_digest: ContentDigest,
    pub segment_key: String,
    pub segment_digest: ContentDigest,
    pub first_pos: Position,
    pub last_pos: Position,
    pub txn_count: u64,
    pub bytes: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct JsonPosition {
    order_domain: String,
    bytes: String,
}
impl From<&Position> for JsonPosition {
    fn from(p: &Position) -> Self {
        Self {
            order_domain: p.order_domain().into(),
            bytes: hex(p.bytes()),
        }
    }
}
impl JsonPosition {
    fn position(self) -> Result<Position, FormatError> {
        Ok(Position::new(self.order_domain, unhex(&self.bytes)?))
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct JsonManifest {
    mseq: u64,
    epoch: u64,
    prev_digest: String,
    segment_key: String,
    segment_digest: String,
    first_pos: JsonPosition,
    last_pos: JsonPosition,
    txn_count: u64,
    bytes: u64,
    digest: String,
}

impl ManifestEntry {
    fn body(&self) -> serde_json::Value {
        serde_json::json!({ "mseq": self.mseq, "epoch": self.epoch,
            "prev_digest": hex(&self.prev_digest), "segment_key": self.segment_key,
            "segment_digest": hex(&self.segment_digest), "first_pos": JsonPosition::from(&self.first_pos),
            "last_pos": JsonPosition::from(&self.last_pos), "txn_count": self.txn_count, "bytes": self.bytes })
    }

    pub fn digest(&self) -> Result<ContentDigest, FormatError> {
        Ok(Sha256::digest(serde_json::to_vec(&self.body())?).into())
    }

    pub fn canonical_json(&self) -> Result<Vec<u8>, FormatError> {
        let mut body = self.body();
        body.as_object_mut()
            .ok_or(FormatError::Invalid("manifest object"))?
            .insert("digest".into(), hex(&self.digest()?).into());
        Ok(serde_json::to_vec(&body)?)
    }

    pub fn from_canonical_json(bytes: &[u8]) -> Result<Self, FormatError> {
        let wire: JsonManifest = serde_json::from_slice(bytes)?;
        let digest = parse_digest(&wire.digest)?;
        let entry = Self {
            mseq: wire.mseq,
            epoch: wire.epoch,
            prev_digest: parse_digest(&wire.prev_digest)?,
            segment_key: wire.segment_key,
            segment_digest: parse_digest(&wire.segment_digest)?,
            first_pos: wire.first_pos.position()?,
            last_pos: wire.last_pos.position()?,
            txn_count: wire.txn_count,
            bytes: wire.bytes,
        };
        if entry.digest()? != digest {
            return Err(FormatError::ManifestDigestMismatch);
        }
        if entry.canonical_json()? != bytes {
            return Err(FormatError::Invalid("noncanonical manifest JSON"));
        }
        Ok(entry)
    }
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8] = b"0123456789abcdef";
    let mut out = String::new();
    for byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 15) as usize] as char);
    }
    out
}

fn unhex(text: &str) -> Result<Vec<u8>, FormatError> {
    if !text.len().is_multiple_of(2) {
        return Err(FormatError::Invalid("hex length"));
    }
    fn nibble(byte: u8) -> Result<u8, FormatError> {
        match byte {
            b'0'..=b'9' => Ok(byte - b'0'),
            b'a'..=b'f' => Ok(byte - b'a' + 10),
            _ => Err(FormatError::Invalid("lowercase hex")),
        }
    }
    text.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| Ok(nibble(pair[0])? * 16 + nibble(pair[1])?))
        .collect()
}
fn parse_digest(text: &str) -> Result<ContentDigest, FormatError> {
    if text.len() != 64 {
        return Err(FormatError::Invalid("digest length"));
    }
    unhex(text)?
        .try_into()
        .map_err(|_| FormatError::Invalid("digest length"))
}

// Encode each dynamic path component injectively, including dots and slashes.
// This also prevents connector-supplied bytes becoming filesystem traversal.
fn component(text: &str) -> String {
    if text.is_empty() {
        return "~".into();
    }
    let mut out = String::new();
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-' {
            out.push(byte as char);
        } else {
            use fmt::Write as _;
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}
fn position_component(position: &Position) -> String {
    format!(
        "x{}.{}",
        hex(position.order_domain().as_bytes()),
        hex(position.bytes())
    )
}
fn root(prefix: &str, migration_id: &str, stream: &str) -> String {
    let prefix = prefix.trim_end_matches('/');
    let base = format!("buffer/{}/{}", component(migration_id), component(stream));
    if prefix.is_empty() {
        base
    } else {
        format!("{prefix}/{base}")
    }
}

pub fn segment_key(
    prefix: &str,
    migration_id: &str,
    stream: &str,
    first_pos: &Position,
    last_pos: &Position,
    epoch: u64,
) -> String {
    format!(
        "{}/seg/{}-{}-{epoch}.seg",
        root(prefix, migration_id, stream),
        position_component(first_pos),
        position_component(last_pos)
    )
}

pub fn manifest_key(prefix: &str, migration_id: &str, stream: &str, mseq: u64) -> String {
    format!(
        "{}/manifest/{mseq:020}.json",
        root(prefix, migration_id, stream)
    )
}
