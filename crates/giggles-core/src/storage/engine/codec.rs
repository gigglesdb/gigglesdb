use super::{crc32c::crc32c_parts, hlc::Ts};
use crate::relational::{MAX_COLUMNS, MAX_STORED_COLUMNS, MAX_VALUE_BYTES, SqlError, Value, encode_key_iter};
use bincode::Options;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CodecError {
    NullInKey,
    Arity,
    Limit,
    Corrupt(&'static str),
    Checksum,
    Clock,
    Sql(SqlError),
}

impl From<SqlError> for CodecError {
    fn from(error: SqlError) -> Self {
        Self::Sql(error)
    }
}

impl From<CodecError> for crate::CoreError {
    fn from(error: CodecError) -> Self {
        match error {
            CodecError::Limit => Self::Limit,
            CodecError::Sql(error) => Self::Sql(error),
            error => Self::Corrupt(format!("engine codec: {error:?}")),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Direction {
    Asc,
    Desc,
}

pub(crate) fn pk_bytes(values: &[Value]) -> Result<Vec<u8>, CodecError> {
    pk_bytes_iter(values.iter())
}

/// Each component is self-delimiting (type tag plus a fixed width, an
/// escaped `00 00`-terminated body, or a terminated numeric body), so keys of
/// one arity are prefix-free. VersionedRange and TxnScan rely on this
/// (GD-0084 Q40); a different arity under the same table id is prefix-related.
pub(crate) fn pk_bytes_iter<'a>(
    values: impl ExactSizeIterator<Item = &'a Value> + Clone,
) -> Result<Vec<u8>, CodecError> {
    if values.clone().any(|value| matches!(value, Value::Null)) {
        return Err(CodecError::NullInKey);
    }
    Ok(encode_key_iter(values)?)
}

/// Components are prefix-free for a fixed schema (arity and directions).
/// NULL is the single byte `FF`, never a component tag, so a UNIQUE index's
/// NULL tuple with a PK suffix is not prefix-related to a non-NULL tuple of the
/// same arity (GD-0084 Q40).
pub(crate) fn index_tuple_bytes(
    values: &[Value],
    dirs: &[Direction],
) -> Result<Vec<u8>, CodecError> {
    if values.len() != dirs.len() {
        return Err(CodecError::Arity);
    }
    let mut out = Vec::new();
    append_index_tuple(&mut out, values.iter().zip(dirs.iter().copied()))?;
    Ok(out)
}

pub(crate) fn append_index_tuple<'a>(
    out: &mut Vec<u8>,
    values: impl ExactSizeIterator<Item = (&'a Value, Direction)>,
) -> Result<(), CodecError> {
    if values.len() > MAX_COLUMNS {
        return Err(CodecError::Limit);
    }
    for (value, direction) in values {
        let start = out.len();
        if matches!(value, Value::Null) {
            out.push(0xff);
        } else {
            crate::relational::secondary_index_keys::append_component(out, value)?;
        }
        if direction == Direction::Desc {
            for byte in &mut out[start..] {
                *byte = !*byte;
            }
        }
    }
    Ok(())
}

pub(crate) fn row_key(table_id: u64, pk: &[u8], ts: Ts) -> Vec<u8> {
    let mut key = table_id.to_be_bytes().to_vec();
    key.extend_from_slice(pk);
    key.extend_from_slice(&ts.inverted_be());
    key
}

pub(crate) fn index_key(index_id: u64, tuple: &[u8], pk: &[u8], ts: Ts) -> Vec<u8> {
    let mut key = index_id.to_be_bytes().to_vec();
    key.extend_from_slice(tuple);
    key.extend_from_slice(pk);
    key.extend_from_slice(&ts.inverted_be());
    key
}

pub(crate) fn split_versioned(key: &[u8]) -> Result<(&[u8], Ts), CodecError> {
    if key.len() < 9 {
        return Err(CodecError::Corrupt("short versioned key"));
    }
    let split = key.len() - 8;
    Ok((
        &key[..split],
        Ts::from_inverted_be(key[split..].try_into().unwrap()),
    ))
}

pub(crate) fn user_key_seek_at(user_key: &[u8], snapshot: Ts) -> Vec<u8> {
    let mut key = Vec::with_capacity(user_key.len() + 8);
    key.extend_from_slice(user_key);
    key.extend_from_slice(&snapshot.inverted_be());
    key
}

/// Exclusive upper bound `user_key || [0xff; 8] || [0x00]`, including timestamp zero.
///
/// GD-0084 Q40: the interval `[user_key || !MAX, user_key_upper_bound(user_key))`
/// holds exactly one user key's versions only if no stored key extends
/// `user_key` with bytes that sort below `[0xff; 8] || [0x00]`. That holds over
/// prefix-free regions (Rows and Index at one snapshot, fixed-width kinds), not
/// for Catalog name and role membership keys. `VersionedRange` drops the
/// extensions this bound admits for an `Included` end (`above_end`); its
/// emission order, the `TxnScan` merge and the routed read pager's reverse
/// resume (`range/engine/read.rs`) still rely on it; GC, the local SSI fallback
/// and point lookups do not. See `mvcc.rs`.
pub(crate) fn user_key_upper_bound(user_key: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(user_key.len() + 9);
    key.extend_from_slice(user_key);
    key.extend_from_slice(&[0xff; 8]);
    key.push(0);
    key
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RowRecord {
    pub(crate) row_version: u64,
    pub(crate) tombstone: bool,
    pub(crate) values: Vec<Value>,
}

const HEADER: usize = 16;

/// Range protocols cannot publish WIDE_ROWS with a transaction. This checks the
/// encoded row's payload bound without decoding or allocating its SQL values.
pub(crate) fn range_row_fits(record: &[u8]) -> bool {
    record.len() <= crate::relational::MAX_ROW_BYTES + HEADER + 4
        && (record.len() < HEADER + 4
            || u32::from_be_bytes(record[12..16].try_into().unwrap()) as usize
                <= crate::relational::MAX_ROW_BYTES)
}

/// Stored row versions carry one MVCC envelope byte. Short opaque values used
/// by KV callers still receive the same size bound.
pub(crate) fn range_stored_row_fits(value: &[u8]) -> bool {
    range_row_fits(value.strip_prefix(&[0]).unwrap_or(value))
}

const MAX_ROW_PAYLOAD: usize = MAX_VALUE_BYTES + MAX_COLUMNS * 32;

pub(crate) fn encode_row(
    key: &[u8],
    row_version: u64,
    values: &[Value],
) -> Result<Vec<u8>, CodecError> {
    if values.len() > MAX_STORED_COLUMNS {
        return Err(CodecError::Limit);
    }
    let options = || super::super::codec().with_limit(MAX_ROW_PAYLOAD as u64);
    let size = options()
        .serialized_size(&values)
        .map_err(|_| CodecError::Limit)?;
    if size > crate::relational::max_value_bytes() as u64 {
        return Err(CodecError::Limit);
    }
    let payload = options()
        .serialize(&values)
        .map_err(|_| CodecError::Corrupt("row serialization"))?;
    if payload.len() > MAX_ROW_PAYLOAD {
        return Err(CodecError::Limit);
    }
    encode_record(key, row_version, false, values.len(), &payload)
}

pub(crate) fn encode_tombstone(key: &[u8], row_version: u64) -> Result<Vec<u8>, CodecError> {
    encode_record(key, row_version, true, 0, &[])
}

fn encode_record(
    key: &[u8],
    row_version: u64,
    tombstone: bool,
    count: usize,
    payload: &[u8],
) -> Result<Vec<u8>, CodecError> {
    let mut out = Vec::with_capacity(HEADER + payload.len() + 4);
    out.extend_from_slice(&[1, u8::from(tombstone)]);
    out.extend_from_slice(&row_version.to_be_bytes());
    out.extend_from_slice(&(count as u16).to_be_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    let checksum = crc32c_parts(&[key, &out]);
    out.extend_from_slice(&checksum.to_be_bytes());
    Ok(out)
}

pub(crate) fn decode_row(key: &[u8], record: &[u8]) -> Result<RowRecord, CodecError> {
    if record.len() < HEADER + 4 {
        return Err(CodecError::Corrupt("short row"));
    }
    if record[0] != 1 {
        return Err(CodecError::Corrupt("row format"));
    }
    if record[1] & !1 != 0 {
        return Err(CodecError::Corrupt("row flags"));
    }
    let tombstone = record[1] == 1;
    let row_version = u64::from_be_bytes(record[2..10].try_into().unwrap());
    let count = u16::from_be_bytes(record[10..12].try_into().unwrap()) as usize;
    let len = u32::from_be_bytes(record[12..16].try_into().unwrap()) as usize;
    if count > MAX_STORED_COLUMNS || len > MAX_ROW_PAYLOAD {
        return Err(CodecError::Corrupt("row bounds"));
    }
    if record.len() != HEADER + len + 4 {
        return Err(CodecError::Corrupt("row length"));
    }
    if tombstone && (count != 0 || len != 0) {
        return Err(CodecError::Corrupt("tombstone payload"));
    }
    let end = HEADER + len;
    let expected = u32::from_be_bytes(record[end..].try_into().unwrap());
    if crc32c_parts(&[key, &record[..end]]) != expected {
        return Err(CodecError::Checksum);
    }
    let values: Vec<Value> = if tombstone {
        Vec::new()
    } else {
        super::super::codec()
            .with_limit(MAX_ROW_PAYLOAD as u64)
            .deserialize(&record[HEADER..end])
            .map_err(|_| CodecError::Corrupt("row payload"))?
    };
    if values.len() != count {
        return Err(CodecError::Corrupt("column count"));
    }
    Ok(RowRecord {
        row_version,
        tombstone,
        values,
    })
}

#[cfg(test)]
mod tests {
    use super::{CodecError, Ts, split_versioned, user_key_seek_at};

    #[test]
    fn appended_index_tuple_keeps_existing_component_bytes_and_prefix() {
        use super::{Direction, Value, append_index_tuple};

        let values = [
            Value::Null,
            Value::Text("a\0b".into()),
            Value::Bytes(vec![0, 255, 0, 6]),
            Value::Text("\u{c4}bc".into()),
            Value::Text("m".repeat(8192)),
        ];
        let dirs = [
            Direction::Asc,
            Direction::Desc,
            Direction::Desc,
            Direction::Asc,
            Direction::Asc,
        ];

        // Captured from c761b044f before direct-to-buffer component encoding.
        let mut expected = b"index id".to_vec();
        expected.extend([0xff]);
        expected.extend([0xfc, 0x9e, 0xff, 0x00, 0x9d, 0xff, 0xff]);
        expected.extend([0xfb, 0xff, 0x00, 0x00, 0xff, 0x00, 0xf9, 0xff, 0xff]);
        expected.extend([0x03, 0xc3, 0x84, 0x62, 0x63, 0x00, 0x00]);
        expected.push(0x03);
        expected.resize(expected.len() + 8192, 0x6d);
        expected.extend([0x00, 0x00]);

        let mut actual = b"index id".to_vec();
        append_index_tuple(&mut actual, values.iter().zip(dirs)).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn versioned_key_concatenation_equivalence() {
        for user in [vec![], vec![0x42], b"table/primary/key".to_vec(), vec![0xab; 4096]] {
            for timestamp in [0, 1, u64::MAX, 0x0123_4567_89ab_cdef] {
                assert_eq!(user_key_seek_at(&user, Ts(timestamp)),
                    [user.as_slice(), &(!timestamp).to_be_bytes()].concat());
                assert_eq!(super::user_key_upper_bound(&user),
                    [user.as_slice(), &[0xff; 8], &[0]].concat());
            }
        }
        assert_eq!(user_key_seek_at(b"k", Ts(0x0123_4567_89ab_cdef)),
            vec![0x6b, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10]);
        assert_eq!(super::user_key_upper_bound(b"k"),
            vec![0x6b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0]);
    }

    #[test]
    fn versioned_key_requires_a_user_byte_and_timestamp() {
        let key = user_key_seek_at(b"x", Ts(42));
        assert_eq!(key.len(), 9);
        assert_eq!(split_versioned(&key), Ok((&b"x"[..], Ts(42))));
        assert!(matches!(
            split_versioned(&[0; 8]),
            Err(CodecError::Corrupt(_))
        ));
    }
}
