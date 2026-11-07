use super::{crc32c::crc32c_parts, hlc::Ts};
use crate::relational::{MAX_COLUMNS, MAX_ROW_BYTES, SqlError, Value, encode_key};

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
    if values.iter().any(|value| matches!(value, Value::Null)) {
        return Err(CodecError::NullInKey);
    }
    Ok(encode_key(values)?)
}

/// Components are prefix-free for a fixed schema (arity and directions).
pub(crate) fn index_tuple_bytes(
    values: &[Value],
    dirs: &[Direction],
) -> Result<Vec<u8>, CodecError> {
    if values.len() != dirs.len() {
        return Err(CodecError::Arity);
    }
    if values.len() > MAX_COLUMNS {
        return Err(CodecError::Limit);
    }
    let mut out = Vec::new();
    for (value, direction) in values.iter().zip(dirs) {
        let mut component = if matches!(value, Value::Null) {
            vec![0xff]
        } else {
            encode_key(std::slice::from_ref(value))?
        };
        if *direction == Direction::Desc {
            for byte in &mut component {
                *byte = !*byte;
            }
        }
        out.extend(component);
    }
    Ok(out)
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
    if key.len() < 16 {
        return Err(CodecError::Corrupt("short versioned key"));
    }
    let split = key.len() - 8;
    Ok((
        &key[..split],
        Ts::from_inverted_be(key[split..].try_into().unwrap()),
    ))
}

pub(crate) fn user_key_seek_at(user_key: &[u8], snapshot: Ts) -> Vec<u8> {
    let mut key = user_key.to_vec();
    key.extend_from_slice(&snapshot.inverted_be());
    key
}

/// Exclusive upper bound `user_key || [0xff; 8] || [0x00]`, including timestamp zero.
/// Range isolation assumes prefix-free user keys under the fixed schema.
pub(crate) fn user_key_upper_bound(user_key: &[u8]) -> Vec<u8> {
    let mut key = user_key.to_vec();
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

pub(crate) fn encode_row(
    key: &[u8],
    row_version: u64,
    values: &[Value],
) -> Result<Vec<u8>, CodecError> {
    if values.len() > MAX_COLUMNS {
        return Err(CodecError::Limit);
    }
    let payload =
        super::super::encode(&values).map_err(|_| CodecError::Corrupt("row serialization"))?;
    if payload.len() > MAX_ROW_BYTES {
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
    if count > MAX_COLUMNS || len > MAX_ROW_BYTES {
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
        super::super::decode(&record[HEADER..end])
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
