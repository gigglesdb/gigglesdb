//! Versioned, cluster-bound Raft entry envelopes and proposal identities.

use sha2::{Digest, Sha256};
use std::fmt;

pub const ENTRY_VERSION: u8 = 1;
pub const MAX_ENTRY_BYTES: usize = 64 << 20;
const HEADER_BYTES: usize = 22;
const CHECKSUM_BYTES: usize = 4;
const OVERHEAD: usize = HEADER_BYTES + CHECKSUM_BYTES;

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum EntryKind {
    Batch = 1,
    Admin = 2,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EntryError {
    Truncated,
    UnknownVersion,
    WrongCluster,
    UnknownKind,
    Oversize,
    BadChecksum,
    TrailingBytes,
}

impl fmt::Display for EntryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Truncated => "truncated entry envelope",
            Self::UnknownVersion => "unknown entry envelope version",
            Self::WrongCluster => "entry belongs to another cluster",
            Self::UnknownKind => "unknown entry kind",
            Self::Oversize => "entry exceeds maximum encoded size",
            Self::BadChecksum => "entry checksum mismatch",
            Self::TrailingBytes => "trailing bytes after entry envelope",
        })
    }
}

impl std::error::Error for EntryError {}

pub fn encode(cluster: [u8; 16], kind: EntryKind, payload: &[u8]) -> Result<Vec<u8>, EntryError> {
    let total = payload
        .len()
        .checked_add(OVERHEAD)
        .ok_or(EntryError::Oversize)?;
    if total > MAX_ENTRY_BYTES {
        return Err(EntryError::Oversize);
    }
    let mut bytes = Vec::with_capacity(total);
    bytes.push(ENTRY_VERSION);
    bytes.extend_from_slice(&cluster);
    bytes.push(kind as u8);
    bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    bytes.extend_from_slice(payload);
    let checksum = Sha256::digest(&bytes);
    bytes.extend_from_slice(&checksum[..CHECKSUM_BYTES]);
    Ok(bytes)
}

pub fn decode(cluster: [u8; 16], bytes: &[u8]) -> Result<(EntryKind, &[u8]), EntryError> {
    if bytes.len() < HEADER_BYTES {
        return Err(EntryError::Truncated);
    }
    if bytes[0] != ENTRY_VERSION {
        return Err(EntryError::UnknownVersion);
    }
    if bytes[1..17] != cluster {
        return Err(EntryError::WrongCluster);
    }
    let kind = match bytes[17] {
        1 => EntryKind::Batch,
        2 => EntryKind::Admin,
        _ => return Err(EntryError::UnknownKind),
    };
    let payload_len = u32::from_be_bytes(bytes[18..HEADER_BYTES].try_into().unwrap()) as usize;
    let total = payload_len
        .checked_add(OVERHEAD)
        .ok_or(EntryError::Oversize)?;
    if total > MAX_ENTRY_BYTES {
        return Err(EntryError::Oversize);
    }
    if bytes.len() < total {
        return Err(EntryError::Truncated);
    }
    if bytes.len() > total {
        return Err(EntryError::TrailingBytes);
    }
    let payload_end = total - CHECKSUM_BYTES;
    let checksum = Sha256::digest(&bytes[..payload_end]);
    if bytes[payload_end..] != checksum[..CHECKSUM_BYTES] {
        return Err(EntryError::BadChecksum);
    }
    Ok((kind, &bytes[HEADER_BYTES..payload_end]))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ProposalContext {
    pub proposer: u64,
    pub seq: u64,
}

impl ProposalContext {
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(16);
        bytes.extend_from_slice(&self.proposer.to_be_bytes());
        bytes.extend_from_slice(&self.seq.to_be_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let bytes: &[u8; 16] = bytes.try_into().ok()?;
        Some(Self {
            proposer: u64::from_be_bytes(bytes[..8].try_into().ok()?),
            seq: u64::from_be_bytes(bytes[8..].try_into().ok()?),
        })
    }
}
