use std::collections::BTreeMap;

use sha2::{Digest, Sha256};

use crate::mapping::Value;

use super::{Key, Result, VerifyError};

pub type ContentDigest = [u8; 32];

/// Included column identities in a fixed order, identical on both endpoints.
/// Exclusions are implemented by omission, including in every fetch request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Projection {
    columns: Vec<String>,
}

impl Projection {
    pub fn new(columns: Vec<String>) -> Result<Self> {
        let mut seen = std::collections::BTreeSet::new();
        if columns
            .iter()
            .any(|name| name.is_empty() || name.contains('\0') || !seen.insert(name))
        {
            return Err(VerifyError::Invalid("duplicate or invalid included column"));
        }
        Ok(Self { columns })
    }

    pub fn columns(&self) -> &[String] {
        &self.columns
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Row {
    pub key: Key,
    pub columns: BTreeMap<String, Value>,
}

fn frame(hash: &mut Sha256, bytes: &[u8]) {
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
}

pub(super) fn key_bytes(key: &Key) -> Result<Vec<u8>> {
    if key.is_empty() || key.contains(&Value::Null) {
        return Err(VerifyError::Invalid("empty or NULL verification key"));
    }
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&(key.len() as u64).to_be_bytes());
    for value in key {
        let value = value.canonical_bytes()?;
        bytes.extend_from_slice(&(value.len() as u64).to_be_bytes());
        bytes.extend_from_slice(&value);
    }
    Ok(bytes)
}

/// SHA-256 v1 over framed typed key values and included column names/values.
/// Extra columns never enter the preimage; readers reject them at the boundary.
pub fn row_hash(row: &Row, projection: &Projection) -> Result<ContentDigest> {
    let mut hash = Sha256::new();
    hash.update(b"skedaddle-verify-row\0\x01");
    frame(&mut hash, &key_bytes(&row.key)?);
    hash.update((projection.columns.len() as u64).to_be_bytes());
    for name in &projection.columns {
        let value = row
            .columns
            .get(name)
            .ok_or(VerifyError::Invalid("included column missing from row"))?;
        frame(&mut hash, name.as_bytes());
        frame(&mut hash, &value.canonical_bytes()?);
    }
    Ok(hash.finalize().into())
}

pub(super) fn empty_digest() -> ContentDigest {
    Sha256::digest(b"skedaddle-verify-empty\0\x01").into()
}

pub(super) fn parent(left: ContentDigest, right: ContentDigest) -> ContentDigest {
    let mut hash = Sha256::new();
    hash.update(b"skedaddle-verify-node\0\x01");
    hash.update(left);
    hash.update(right);
    hash.finalize().into()
}

/// Ordered, balanced, bottom-up Merkle digest. Counts alone are never proof.
/// Callers must validate catalog ordering before using this standalone helper.
pub fn range_digest(rows: &[Row], projection: &Projection) -> Result<ContentDigest> {
    let leaves = rows
        .iter()
        .map(|row| row_hash(row, projection))
        .collect::<Result<Vec<_>>>()?;
    fn reduce(leaves: &[ContentDigest]) -> ContentDigest {
        match leaves {
            [] => empty_digest(),
            [leaf] => *leaf,
            _ => {
                let middle = leaves.len() / 2;
                parent(reduce(&leaves[..middle]), reduce(&leaves[middle..]))
            }
        }
    }
    Ok(reduce(&leaves))
}
