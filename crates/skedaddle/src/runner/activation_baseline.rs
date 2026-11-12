//! Immutable activation catalog pair, independent of forward buffer retention.
use super::*;
use crate::coordinator::{self, journal::Journal};
use crate::engine;
use object_store::path::Path as ObjectPath;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub(super) const STREAM: &str = "activation-catalog-v1";
const LIMIT: usize = 4 * 1024 * 1024;
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Baseline {
    version: u32,
    namespace: String,
    pub source: preflight::Snapshot,
    pub destination: preflight::Snapshot,
}
fn invalid() -> coordinator::Error {
    coordinator::Error::Rejected("activation catalog authentication/qualification failed".into())
}
fn key(journal: &Journal, digest: &str) -> String {
    format!(
        "activation-catalog/{:x}/{digest}.json",
        Sha256::digest(journal.namespace().as_bytes())
    )
}
impl Baseline {
    pub fn new(
        journal: &Journal,
        source: preflight::Snapshot,
        destination: preflight::Snapshot,
    ) -> Self {
        Self {
            version: 1,
            namespace: journal.namespace().into(),
            source,
            destination,
        }
    }
    pub async fn persist(
        &self,
        journal: &Journal,
        runtime: &engine::Runtime,
    ) -> coordinator::Result<()> {
        // Never replace a baseline: a retry must compare with the original.
        if let Some(old) = load(journal).await? {
            if descriptors(&old.destination)? != descriptors(&self.destination)?
                || descriptors(&old.source)? != descriptors(&self.source)?
            {
                return Err(invalid());
            }
            return Ok(());
        }
        if runtime.state().await.lifecycle.state != crate::coordinator::State::FinalBoundaryApplied
        {
            return Err(invalid());
        }
        descriptors(&self.source)?;
        descriptors(&self.destination)?;
        let bytes = serde_json::to_vec(self)?;
        if bytes.len() > LIMIT {
            return Err(invalid());
        }
        let digest = format!("{:x}", Sha256::digest(&bytes));
        let key = key(journal, &digest);
        journal
            .create(&ObjectPath::from(key.as_str()), bytes)
            .await?;
        runtime.marker(STREAM, digest, key).await
    }
    pub fn matches(&self, live: &preflight::Snapshot) -> coordinator::Result<bool> {
        Ok(descriptors(&self.destination)? == descriptors(live)?)
    }
}
/// Only evidence read successfully and rejected by validation is permanent.
#[derive(Debug)]
pub(super) enum LoadError {
    Invalid,
    Unavailable(coordinator::Error),
}
impl From<coordinator::Error> for LoadError {
    fn from(error: coordinator::Error) -> Self {
        Self::Unavailable(error)
    }
}
impl From<LoadError> for coordinator::Error {
    fn from(error: LoadError) -> Self {
        match error {
            LoadError::Invalid => invalid(),
            LoadError::Unavailable(error) => error,
        }
    }
}
pub(super) async fn load(journal: &Journal) -> Result<Option<Baseline>, LoadError> {
    let state = journal.load(true).await?;
    let Some((digest, path)) = state.progress.get(STREAM) else {
        return Ok(None);
    };
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        || *path != key(journal, digest)
    {
        return Err(LoadError::Invalid);
    }
    let bytes = journal
        .read_bounded(&ObjectPath::from(path.as_str()), LIMIT)
        .await
        .map_err(|error| match error {
            // read_bounded rejects only objects exceeding the format limit.
            coordinator::Error::Rejected(_) => LoadError::Invalid,
            error => LoadError::Unavailable(error),
        })?
        .ok_or_else(|| {
            LoadError::Unavailable(coordinator::Error::RecoveryRequired(
                "referenced activation catalog object unavailable; retry".into(),
            ))
        })?;
    if format!("{:x}", Sha256::digest(&bytes)) != *digest {
        return Err(LoadError::Invalid);
    }
    let value: Baseline = serde_json::from_slice(&bytes).map_err(|_| LoadError::Invalid)?;
    if value.version != 1 || value.namespace != journal.namespace() {
        return Err(LoadError::Invalid);
    }
    descriptors(&value.source).map_err(|_| LoadError::Invalid)?;
    descriptors(&value.destination).map_err(|_| LoadError::Invalid)?;
    Ok(Some(value))
}
fn descriptors(snapshot: &preflight::Snapshot) -> coordinator::Result<serde_json::Value> {
    use preflight::Check::*;
    let mut values = Vec::new();
    for observation in &snapshot.observations {
        if !matches!(
            observation.check,
            Table
                | Key
                | ReplicaIdentity
                | ColumnTypes
                | GeneratedColumns
                | IdentityColumns
                | Sequences
                | Triggers
                | Partitions
                | Extensions
                | SchemaDescriptor
        ) {
            continue;
        }
        if observation.error.is_some() {
            return Err(invalid());
        }
        values.push(serde_json::json!([
            observation.check,
            observation.object,
            observation.rows
        ]));
    }
    if values.is_empty() {
        return Err(invalid());
    }
    Ok(serde_json::Value::Array(values))
}
