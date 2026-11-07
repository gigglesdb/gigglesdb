use crate::kv::{Backend, KvPair, Result};
pub fn encode_user_key(table: u64, pk: i64) -> [u8; 17] {
    let mut key = [0; 17];
    key[..8].copy_from_slice(&table.to_be_bytes());
    key[8] = 2;
    key[9..].copy_from_slice(&((pk as u64) ^ (1u64 << 63)).to_be_bytes());
    key
}
pub fn encode_key(user: &[u8], ts: u64) -> Vec<u8> {
    assert_eq!(user.len(), 17);
    let mut key = user.to_vec();
    key.extend_from_slice(&(u64::MAX - ts).to_be_bytes());
    key
}
pub fn decode_key(key: &[u8]) -> Result<([u8; 17], u64)> {
    if key.len() != 25 || key[8] != 2 {
        return Err("invalid MVCC key".into());
    }
    Ok((
        key[..17].try_into()?,
        u64::MAX - u64::from_be_bytes(key[17..].try_into()?),
    ))
}
pub fn encode_value(payload: Option<&[u8]>) -> Vec<u8> {
    match payload {
        Some(p) => {
            let mut v = Vec::with_capacity(p.len() + 1);
            v.push(0);
            v.extend_from_slice(p);
            v
        }
        None => vec![1],
    }
}
fn decode_value(value: &[u8]) -> Result<Option<Vec<u8>>> {
    match value.first() {
        Some(0) => Ok(Some(value[1..].to_vec())),
        Some(1) if value.len() == 1 => Ok(None),
        _ => Err("invalid MVCC value".into()),
    }
}
pub fn get_at(backend: &dyn Backend, user: &[u8], snapshot: u64) -> Result<Option<Vec<u8>>> {
    if user.len() != 17 {
        return Err("invalid user key length".into());
    }
    match backend.seek_first_at_or_after(&encode_key(user, snapshot))? {
        Some((key, value)) if decode_key(&key)?.0.as_slice() == user => decode_value(&value),
        _ => Ok(None),
    }
}
/// Streams physical records, retaining only the current user key and requested output.
pub fn scan_at(
    backend: &dyn Backend,
    start: &[u8],
    limit: usize,
    snapshot: u64,
) -> Result<Vec<KvPair>> {
    if start.len() != 17 {
        return Err("invalid user key length".into());
    }
    let mut out = Vec::new();
    if limit == 0 {
        return Ok(out);
    }
    let mut resolved: Option<[u8; 17]> = None;
    for record in backend.iter_from(&encode_key(start, snapshot))? {
        let (key, value) = record?;
        let (user, ts) = decode_key(&key)?;
        if resolved == Some(user) || ts > snapshot {
            continue;
        }
        resolved = Some(user);
        if let Some(payload) = decode_value(&value)? {
            out.push((user.to_vec(), payload));
            if out.len() == limit {
                break;
            }
        }
    }
    Ok(out)
}
