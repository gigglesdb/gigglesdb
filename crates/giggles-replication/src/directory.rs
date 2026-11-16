//! Range directory records in the system range (GD-0075 R5-20).
//!
//! Each range descriptor is stored as a versioned Catalog record keyed by its END range key, so
//! the range containing key K is the first record whose user key is greater than
//! `[DIRECTORY_TAG] ++ enc(K)`, read at any timestamp. A new cluster database starts with one
//! record (range 1 = everything); the first split separates system keys from user keys.
//!
//! Catalog user-key tags 1..=18 are in use (giggles-core catalog_codec.rs). The tags below are
//! reserved for range mode; `classify_catalog_key` must learn them there (Track A, DD-0068
//! heads-up), it does not accept them today.
use std::ops::Bound;

use giggles_core::range_api::{
    Epoch, RangeDescriptor, RangeError, RangeSpace, RangeWrite, ReadDirection, ReadPage,
    ReadRequest, ReplicaDesc, SYSTEM_RANGE, Timestamp,
};

/// Catalog user-key tag of range directory records.
pub const DIRECTORY_TAG: u8 = 0xF0;
/// Reserved Catalog tag for gateway records (`gw/`, R5-00g).
pub const GATEWAY_TAG: u8 = 0xF1;
/// Reserved Catalog tag for DDL records (`ddl/`).
pub const DDL_TAG: u8 = 0xF2;
/// Encoded end of the last range (end = None). Every `enc` output that starts with 0xFF
/// continues with 0x00, so this sorts above all of them and is no prefix of any.
const MAX_END: [u8; 2] = [0xFF, 0xFF];
const TS_LEN: usize = 8;
const LOOKUP_PAGE_BYTES: usize = 4096;

/// Directory user key for a descriptor end (`None` = maximum). Order-preserving and
/// prefix-free: 0x00 -> 00 FF, 0xFF -> FF 00, other bytes as is, terminator 00 01.
pub fn record_key(end: Option<&[u8]>) -> Vec<u8> {
    let mut key = vec![DIRECTORY_TAG];
    let Some(end) = end else {
        key.extend_from_slice(&MAX_END);
        return key;
    };
    for &byte in end {
        match byte {
            0x00 => key.extend_from_slice(&[0x00, 0xFF]),
            0xFF => key.extend_from_slice(&[0xFF, 0x00]),
            byte => key.push(byte),
        }
    }
    key.extend_from_slice(&[0x00, 0x01]);
    key
}

/// Write that stores `desc` under its end key.
pub fn record_write(desc: &RangeDescriptor) -> Result<RangeWrite, RangeError> {
    Ok(RangeWrite {
        space: RangeSpace::Catalog,
        key: record_key(desc.end.as_deref()),
        value: Some(desc.encode()?),
    })
}

/// Write that deletes the record for a range end (after a merge).
pub fn delete_write(end: Option<&[u8]>) -> RangeWrite {
    RangeWrite {
        space: RangeSpace::Catalog,
        key: record_key(end),
        value: None,
    }
}

/// Range 1 at epoch 1 covering every key: the system range of a new cluster database.
pub fn initial_descriptor(replicas: Vec<ReplicaDesc>) -> RangeDescriptor {
    RangeDescriptor {
        id: SYSTEM_RANGE,
        epoch: Epoch(1),
        start: Vec::new(),
        end: None,
        replicas,
    }
}

/// Directory records of a new cluster database. Errors if `replicas` is not sorted and unique.
pub fn initial_records(replicas: Vec<ReplicaDesc>) -> Result<Vec<RangeWrite>, RangeError> {
    Ok(vec![record_write(&initial_descriptor(replicas))?])
}

/// Forward Catalog read on the system range from just past every version of the record whose
/// end equals `range_key` to the end of the directory tag.
pub fn lookup_request(range_key: &[u8], system: &RangeDescriptor, ts: Timestamp) -> ReadRequest {
    let mut seek = record_key(Some(range_key));
    // The largest raw key of that user key (version ts 0); user keys are prefix-free.
    seek.extend_from_slice(&[0xFF; TS_LEN]);
    ReadRequest {
        range: system.id,
        epoch: system.epoch,
        space: RangeSpace::Catalog,
        start: Bound::Excluded(seek),
        end: Bound::Excluded(vec![DIRECTORY_TAG + 1]),
        dir: ReadDirection::Forward,
        ts,
        page_bytes: LOOKUP_PAGE_BYTES,
    }
}

/// First live record of a lookup page, if it contains `range_key`. Tombstones are skipped.
pub fn parse_lookup(
    page: &ReadPage,
    range_key: &[u8],
) -> Result<Option<RangeDescriptor>, RangeError> {
    for (key, value) in &page.pairs {
        if let Some(desc) = decode_record(key, value)? {
            return Ok(desc.contains(range_key).then_some(desc));
        }
    }
    Ok(None)
}

/// True when every pair of the page is a tombstone (the lookup must read the next page).
pub fn all_tombstones(page: &ReadPage) -> bool {
    page.pairs.iter().all(|(_, value)| value.as_slice() == [1])
}

/// Decodes one raw stored pair (user key ++ inverted ts, MVCC envelope). `None` = tombstone.
pub fn decode_record(
    raw_key: &[u8],
    raw_value: &[u8],
) -> Result<Option<RangeDescriptor>, RangeError> {
    let corrupt = |what: &str| RangeError::Corrupt(format!("directory record: {what}"));
    if raw_key.len() <= 1 + TS_LEN || raw_key[0] != DIRECTORY_TAG {
        return Err(corrupt("invalid key"));
    }
    let payload = match raw_value.split_first() {
        Some((0, payload)) => payload,
        Some((1, [])) => return Ok(None),
        _ => return Err(corrupt("invalid MVCC envelope")),
    };
    let desc = RangeDescriptor::decode(payload)?;
    if record_key(desc.end.as_deref()) != raw_key[..raw_key.len() - TS_LEN] {
        return Err(corrupt("key does not match descriptor end"));
    }
    Ok(Some(desc))
}
