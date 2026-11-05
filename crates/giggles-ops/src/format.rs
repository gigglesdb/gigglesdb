use crate::{OpsError, Result};
use serde::{Deserialize, Serialize};

/// Maximum admitted JSON bytes, independent of caller configuration.
pub const MAX_MANIFEST: u64 = 16 * 1024;
/// Core's bounded export envelope maximum.
pub const MAX_SNAPSHOT: u64 = 16 * 1024 * 1024 + 52;
/// A portable catalog identifier, never a filesystem path.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct BackupId(String);
impl BackupId {
    /// Validate 1..64 ASCII alphanumeric/hyphen bytes; first byte alphanumeric.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let v = value.into();
        if v.is_empty()
            || v.len() > 64
            || !v.as_bytes()[0].is_ascii_alphanumeric()
            || !v.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
            || matches!(
                v.to_ascii_uppercase().as_str(),
                "CON"
                    | "PRN"
                    | "AUX"
                    | "NUL"
                    | "COM1"
                    | "COM2"
                    | "COM3"
                    | "COM4"
                    | "COM5"
                    | "COM6"
                    | "COM7"
                    | "COM8"
                    | "COM9"
                    | "LPT1"
                    | "LPT2"
                    | "LPT3"
                    | "LPT4"
                    | "LPT5"
                    | "LPT6"
                    | "LPT7"
                    | "LPT8"
                    | "LPT9"
            )
        {
            return Err(OpsError::Invalid);
        }
        Ok(Self(v))
    }
    /// Borrow the validated identifier.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for BackupId {
    type Error = OpsError;
    fn try_from(v: String) -> Result<Self> {
        Self::new(v)
    }
}
impl From<BackupId> for String {
    fn from(v: BackupId) -> Self {
        v.0
    }
}
/// Only a published manifest can have this state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Completion {
    Complete,
}
/// Version one catalog manifest. Paths are validated before any snapshot IO.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub format_version: u32,
    pub backup_id: BackupId,
    pub state: Completion,
    pub source_domain: String,
    pub source_generation: Option<u64>,
    pub snapshot: String,
    pub content_length: u64,
    pub sha256: String,
    pub envelope_version: u32,
    pub tool_version: String,
}
pub(crate) fn hex_valid(s: &str, n: usize) -> bool {
    s.len() == n
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
pub(crate) fn stage_valid(s: &str) -> bool {
    s.strip_prefix("staging-").is_some_and(|s| hex_valid(s, 32))
}
impl Manifest {
    /// Check version, exact relative path grammar and all bounded scalar fields.
    pub fn validate(&self) -> Result<()> {
        let parts: Vec<_> = self.snapshot.split('/').collect();
        if self.format_version != 1
            || self.source_generation.is_some()
            || !hex_valid(&self.source_domain, 32)
            || !hex_valid(&self.sha256, 64)
            || !(52..=MAX_SNAPSHOT).contains(&self.content_length)
            || ![1, 2, 3].contains(&self.envelope_version)
            || self.tool_version != env!("CARGO_PKG_VERSION")
            || parts.len() != 2
            || !stage_valid(parts[0])
            || parts[1] != "snapshot.gdb"
        {
            return Err(OpsError::Invalid);
        }
        Ok(())
    }
}
/// Per-operation admission limits; no hidden background queue is created.
#[derive(Clone, Copy, Debug)]
pub struct OpsLimits {
    pub manifest_bytes: u64,
    pub snapshot_bytes: u64,
    pub catalog_entries: usize,
}
impl Default for OpsLimits {
    fn default() -> Self {
        Self {
            manifest_bytes: MAX_MANIFEST,
            snapshot_bytes: MAX_SNAPSHOT,
            catalog_entries: 1024,
        }
    }
}
impl OpsLimits {
    pub(crate) fn validate(self) -> Result<Self> {
        if self.manifest_bytes == 0
            || self.manifest_bytes > MAX_MANIFEST
            || self.snapshot_bytes < 52
            || self.snapshot_bytes > MAX_SNAPSHOT
            || self.catalog_entries == 0
            || self.catalog_entries > 1024
        {
            Err(OpsError::Limit)
        } else {
            Ok(self)
        }
    }
}
