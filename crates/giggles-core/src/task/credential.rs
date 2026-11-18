//! Persisted task credentials and their bearer-secret format (R3-1301).
use super::definition::WallClockMs;
use crate::CoreError;
use sha2::{Digest, Sha256};
use std::fmt;

/// Catalog kind 24 (0x18). Kind 22 is GD-0079 user collations (9-byte key)
/// and kind 23 column collations; a separate kind keeps the record types
/// apart by their first byte, not by key length.
pub(crate) const CREDENTIAL_KIND: u8 = 24;

fn corrupt(message: &str) -> CoreError {
    CoreError::Corrupt(format!("credential record: {message}"))
}

pub fn credential_key(task: u64, id: u64) -> Vec<u8> {
    let mut key = vec![CREDENTIAL_KIND];
    key.extend(task.to_be_bytes());
    key.extend(id.to_be_bytes());
    key
}

pub fn decode_credential_key(key: &[u8]) -> Result<(u64, u64), CoreError> {
    if key.len() != 17 || key[0] != CREDENTIAL_KIND {
        return Err(corrupt("invalid key"));
    }
    let task = u64::from_be_bytes(key[1..9].try_into().unwrap());
    let id = u64::from_be_bytes(key[9..17].try_into().unwrap());
    if task == 0 || id == 0 {
        return Err(corrupt("zero id"));
    }
    Ok((task, id))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CredentialRecord {
    pub issued_at: WallClockMs,
    pub expires_at: Option<WallClockMs>,
    pub issued_by: String,
    pub verifier: [u8; 32],
}

pub fn encode_credential(record: &CredentialRecord) -> Result<Vec<u8>, CoreError> {
    let mut value = vec![1];
    value.extend(record.issued_at.0.to_be_bytes());
    // MAX is reserved for None; it cannot represent an explicit expiry.
    if record.expires_at == Some(WallClockMs(i64::MAX)) {
        return Err(corrupt("reserved expiry"));
    }
    value.extend(
        record
            .expires_at
            .map_or(i64::MAX, |time| time.0)
            .to_be_bytes(),
    );
    if record.issued_by.is_empty() || record.issued_by.len() > 63 {
        return Err(corrupt("issuer length"));
    }
    value.extend((record.issued_by.len() as u16).to_be_bytes());
    value.extend(record.issued_by.as_bytes());
    value.extend(record.verifier);
    decode_credential(&value)?;
    Ok(value)
}

pub fn decode_credential(value: &[u8]) -> Result<CredentialRecord, CoreError> {
    if !(52..=114).contains(&value.len()) || value[0] != 1 {
        return Err(corrupt("version or length"));
    }
    let issued = i64::from_be_bytes(value[1..9].try_into().unwrap());
    let expires = i64::from_be_bytes(value[9..17].try_into().unwrap());
    if expires <= issued {
        return Err(corrupt("expiry must follow issue"));
    }
    let len = u16::from_be_bytes(value[17..19].try_into().unwrap()) as usize;
    if !(1..=63).contains(&len) || value.len() != 19 + len + 32 {
        return Err(corrupt("issuer length"));
    }
    let issued_by = std::str::from_utf8(&value[19..19 + len])
        .map_err(|_| corrupt("issuer UTF-8"))?
        .to_owned();
    if issued_by.contains('\0') {
        return Err(corrupt("NUL in issuer"));
    }
    Ok(CredentialRecord {
        issued_at: WallClockMs(issued),
        expires_at: (expires != i64::MAX).then_some(WallClockMs(expires)),
        issued_by,
        verifier: value[19 + len..].try_into().unwrap(),
    })
}

pub struct SecretText(String);

impl fmt::Debug for SecretText {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretText([redacted])")
    }
}

impl SecretText {
    pub fn expose(&self) -> &str {
        &self.0
    }
}

pub fn new_secret(id: u64) -> Result<(SecretText, [u8; 32]), CoreError> {
    if id == 0 {
        return Err(corrupt("zero credential id"));
    }
    let mut random = [0; 32];
    getrandom::fill(&mut random).map_err(|_| CoreError::Storage("no randomness".into()))?;
    let mut secret = format!("ggtc1_{id:016x}_");
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in random {
        secret.push(HEX[(byte >> 4) as usize] as char);
        secret.push(HEX[(byte & 15) as usize] as char);
    }
    Ok((SecretText(secret), verifier(id, &random)))
}

pub fn parse_secret(secret: &str) -> Option<(u64, [u8; 32])> {
    let bytes = secret.as_bytes();
    if bytes.len() != 87 || &bytes[..6] != b"ggtc1_" || bytes[22] != b'_' {
        return None;
    }
    fn nibble(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            _ => None,
        }
    }
    let mut id = 0u64;
    for &byte in &bytes[6..22] {
        id = (id << 4) | u64::from(nibble(byte)?);
    }
    if id == 0 {
        return None;
    }
    let mut random = [0; 32];
    for (out, pair) in random.iter_mut().zip(bytes[23..].chunks_exact(2)) {
        *out = (nibble(pair[0])? << 4) | nibble(pair[1])?;
    }
    Some((id, random))
}

pub fn verifier(id: u64, random: &[u8; 32]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"giggles task credential v1\0");
    hash.update(id.to_be_bytes());
    hash.update(random);
    hash.finalize().into()
}

pub fn ct_eq(a: &[u8; 32], b: &[u8; 32]) -> bool {
    a.iter().zip(b).fold(0u8, |diff, (a, b)| diff | (a ^ b)) == 0
}
