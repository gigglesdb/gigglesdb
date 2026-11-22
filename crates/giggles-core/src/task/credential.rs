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
    for (out, pair) in random.iter_mut().zip(bytes[23..].as_chunks::<2>().0) {
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

use super::{
    catalog::{TaskCatalog, TaskDdlError, ddl},
    definition::{TaskName, TaskObjectId},
};
use crate::storage::{
    engine::{kv::Keyspace, meta::TASK_CATALOG},
    roles::CatalogRole,
};

pub const MAX_LIVE_CREDENTIALS: usize = 8;

#[derive(Debug)]
pub struct IssuedCredential {
    pub id: u64,
    pub task: TaskName,
    pub secret: SecretText,
    pub expires_at: Option<WallClockMs>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialMatch {
    pub task: TaskObjectId,
    pub credential: u64,
    pub executor: String,
    pub expires_at: Option<WallClockMs>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CredentialRefused;

fn own_live(record: &CredentialRecord, now: WallClockMs) -> bool {
    record.expires_at.is_none_or(|expires| now < expires)
}

impl TaskCatalog {
    /// Mint a bearer secret once; persist only its verifier.
    pub fn issue_credential(
        &self,
        actor: &CatalogRole,
        task: &str,
        expires: Option<WallClockMs>,
        now: WallClockMs,
    ) -> Result<IssuedCredential, TaskDdlError> {
        let (db, mut txn, _) = self.begin_write(None)?;
        let view = db.catalog().committed_view(&mut txn)?;
        let def = Self::current(&view, task)?;
        Self::may_administer(actor, &def)?;
        if def.revoked || now >= def.expires_at {
            return Err(ddl("55000", "task is not live"));
        }
        if expires.is_some_and(|expires| expires <= now) {
            return Err(ddl("22023", "credential expiry is not in the future"));
        }
        let mut live = 0;
        for (&id, (task, record)) in &view.task_credentials {
            if *task != def.id.0 {
                continue;
            }
            if own_live(record, now) {
                live += 1;
            } else {
                txn.delete(Keyspace::Catalog, credential_key(*task, id))?;
            }
        }
        if live >= MAX_LIVE_CREDENTIALS {
            return Err(ddl("54000", "too many live credentials for task"));
        }
        let id = db.sequences.next(0, 0)?;
        let (secret, verifier) = new_secret(id)?;
        let record = CredentialRecord {
            issued_at: now,
            expires_at: expires,
            issued_by: actor.name.clone(),
            verifier,
        };
        txn.put(
            Keyspace::Catalog,
            credential_key(def.id.0, id),
            encode_credential(&record)?,
        )?;
        txn.require_features(TASK_CATALOG);
        self.commit_catalog_write(&db, txn)?;
        Ok(IssuedCredential {
            id,
            task: def.name,
            secret,
            expires_at: expires,
        })
    }

    pub fn revoke_credential(&self, actor: &CatalogRole, id: u64) -> Result<(), TaskDdlError> {
        let (db, mut txn, _) = self.begin_write(None)?;
        let view = db.catalog().committed_view(&mut txn)?;
        let (task, _) = view
            .task_credentials
            .get(&id)
            .ok_or_else(|| ddl("42704", format!("credential {id} does not exist")))?;
        let def = view
            .tasks
            .get(task)
            .ok_or_else(|| ddl("42704", "task does not exist"))?;
        Self::may_administer(actor, def)?;
        txn.delete(Keyspace::Catalog, credential_key(*task, id))?;
        txn.require_features(TASK_CATALOG);
        self.commit_catalog_write(&db, txn)?;
        Ok(())
    }

    /// Own-expiry filtering, in ascending credential id order.
    pub fn credentials(
        &self,
        task: TaskObjectId,
        now: WallClockMs,
    ) -> Result<Vec<(u64, Option<WallClockMs>)>, TaskDdlError> {
        Ok(self
            .view()?
            .task_credentials
            .iter()
            .filter(|(_, (owner, record))| *owner == task.0 && own_live(record, now))
            .map(|(&id, (_, record))| (id, record.expires_at))
            .collect())
    }

    /// Task expiry is handled by the session's outcome-retention policy.
    pub fn authenticate(
        &self,
        secret: &str,
        now: WallClockMs,
    ) -> Result<CredentialMatch, CredentialRefused> {
        let Some((id, random)) = parse_secret(secret) else {
            // Keep the dummy hash and comparison in optimized builds too.
            let dummy = verifier(std::hint::black_box(0), &[0; 32]);
            std::hint::black_box(ct_eq(&dummy, &[0; 32]));
            return Err(CredentialRefused);
        };
        let hash = verifier(id, &random);
        let view = self.view().map_err(|_| CredentialRefused)?;
        let Some((task, record)) = view.task_credentials.get(&id) else {
            std::hint::black_box(ct_eq(&hash, &[0; 32]));
            return Err(CredentialRefused);
        };
        if !ct_eq(&hash, &record.verifier) || !own_live(record, now) {
            return Err(CredentialRefused);
        }
        let def = view
            .tasks
            .get(task)
            .filter(|def| !def.revoked)
            .ok_or(CredentialRefused)?;
        Ok(CredentialMatch {
            task: def.id,
            credential: id,
            executor: def.executor.clone(),
            expires_at: record.expires_at,
        })
    }

    pub fn credential_live(&self, task: TaskObjectId, credential: u64, now: WallClockMs) -> bool {
        let Ok(view) = self.view() else {
            return false;
        };
        view.task_credentials
            .get(&credential)
            .is_some_and(|(owner, record)| {
                *owner == task.0
                    && own_live(record, now)
                    && view.tasks.get(owner).is_some_and(|def| !def.revoked)
            })
    }
}
