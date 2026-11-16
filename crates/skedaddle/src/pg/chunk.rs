//! D15 snapshot primitives. Journal Started before emitting low; read in a
//! short transaction, emit high, then feed committed stream records to Window.
//! Publish its ONE ChunkRows record durably before journaling Done. Observing H
//! alone never authorizes WAL feedback. Scheduling/budgets belong to the caller.
//! Preflight supplies a non-null unique key and its actual catalog ordering.
//! Canonical bytes are identities, never sort keys; PostgreSQL orders samples.

mod sql;
mod window;
pub use sql::{ChunkQuery, Direction, KeyColumn, QualifiedName, TableKey};
pub use window::{
    ChunkTransition, KeyIdentity, Watermark, WatermarkKind, Window, unfinished_chunks,
};

/// Exact logical-message route, distinct from the DDL route.
pub const MESSAGE_PREFIX: &str = "skedaddle.wm";

use crate::{
    event::EventError,
    mapping::{MappingError, Value},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, fmt};

pub type Key = Vec<Value>;
pub type Result<T> = std::result::Result<T, ChunkError>;

#[derive(Debug)]
pub enum ChunkError {
    Invalid(&'static str),
    UnsupportedSource(&'static str),
    MysqlKey(crate::mysql::chunk_key::MysqlKeyError),
    Mapping(MappingError),
    Event(EventError),
    Postgres(tokio_postgres::Error),
    Json(serde_json::Error),
}
impl fmt::Display for ChunkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "watermark chunk: {self:?}")
    }
}
impl std::error::Error for ChunkError {}
impl From<MappingError> for ChunkError {
    fn from(v: MappingError) -> Self {
        Self::Mapping(v)
    }
}
impl From<EventError> for ChunkError {
    fn from(v: EventError) -> Self {
        Self::Event(v)
    }
}
impl From<tokio_postgres::Error> for ChunkError {
    fn from(v: tokio_postgres::Error) -> Self {
        Self::Postgres(v)
    }
}
impl From<serde_json::Error> for ChunkError {
    fn from(v: serde_json::Error) -> Self {
        Self::Json(v)
    }
}

/// Includes the whole plan: changed samples cannot inherit an older completion.
/// Persist samples/plan for recovery rather than resampling after a crash.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChunkId {
    pub table: String,
    pub plan: String,
    pub ordinal: u64,
}
impl ChunkId {
    fn validate(&self) -> Result<()> {
        if self.table.is_empty()
            || self.table.contains('\0')
            || self.plan.len() != 64
            || !self
                .plan
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(ChunkError::Invalid("chunk identity"));
        }
        Ok(())
    }
}

/// Bounds follow INDEX order. None is an unbounded end, not SQL NULL.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChunkRange {
    pub id: ChunkId,
    pub lo: Option<Key>,
    pub hi: Option<Key>,
}

/// Samples are first rows of subsequent chunks, in sampling_query's SOURCE
/// order. Never sort them in Rust. Concurrent writes can change chunk sizes.
pub fn plan_chunks(table: &TableKey, size: usize, samples: &[Key]) -> Result<Vec<ChunkRange>> {
    table.validate()?;
    if size == 0 || i64::try_from(size).is_err() {
        return Err(ChunkError::Invalid("chunk size"));
    }
    let mut seen = BTreeSet::new();
    for key in samples {
        table.validate_key(key)?;
        if !seen.insert(key_bytes(key)?) {
            return Err(ChunkError::Invalid("duplicate sample boundary"));
        }
    }
    let mut hash = Sha256::new();
    hash.update(b"skedaddle-chunk-plan-v1\0");
    hash.update(serde_json::to_vec(&(table, size, samples))?);
    let plan = format!("{:x}", hash.finalize());
    Ok((0..=samples.len())
        .map(|i| ChunkRange {
            id: ChunkId {
                table: table.table.clone(),
                plan: plan.clone(),
                ordinal: i as u64,
            },
            lo: i.checked_sub(1).map(|j| samples[j].clone()),
            hi: samples.get(i).cloned(),
        })
        .collect())
}

pub(crate) fn key_bytes(key: &[Value]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    for value in key {
        if matches!(value, Value::Null) {
            return Err(ChunkError::Invalid("NULL key"));
        }
        let bytes = value.canonical_bytes()?;
        out.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
        out.extend_from_slice(&bytes);
    }
    Ok(out)
}

#[cfg(test)]
mod tests;
