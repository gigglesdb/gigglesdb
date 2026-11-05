use crate::{BackupId, Completion, Manifest, OpsError, OpsLimits, Result, fs};
use giggles_core::{Admin, Snapshot};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Explicit trusted test seam; production calls use None.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultPoint {
    None,
    BeforeExport,
    AfterSnapshot,
    BeforePublish,
    AfterPublish,
    BeforeRestore,
    InstallCreated,
    InstallStaged,
    InstallCommitted,
    AfterInstall,
}
/// Fault injection action. Never selected by environment variables.
#[derive(Clone, Copy, Debug)]
pub enum FaultAction {
    Error,
    Abort,
}
/// Explicit recovery test configuration.
#[derive(Clone, Copy, Debug)]
pub struct Fault {
    pub point: FaultPoint,
    pub action: FaultAction,
}
impl Default for Fault {
    fn default() -> Self {
        Self {
            point: FaultPoint::None,
            action: FaultAction::Error,
        }
    }
}
impl Fault {
    pub(crate) fn hit(self, p: FaultPoint) -> Result<()> {
        if self.point != FaultPoint::None && self.point == p {
            match self.action {
                FaultAction::Error => return Err(OpsError::Injected),
                FaultAction::Abort => std::process::abort(),
            }
        }
        Ok(())
    }
}
/// Catalog metadata enumeration; staging is never reported as a completed backup.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum CatalogEntry {
    Complete { manifest: Manifest },
    Incomplete { stage: String },
}
/// A verified cut summary without customer values.
#[derive(Debug, Serialize, Deserialize)]
pub struct Verification {
    pub manifest: Manifest,
    pub rows: usize,
    pub tasks: usize,
    pub approvals: usize,
    pub operations: usize,
}
/// Synchronous catalog over an existing operator-owned directory.
#[derive(Clone, Debug)]
pub struct Catalog {
    pub(crate) root: PathBuf,
    pub(crate) limits: OpsLimits,
}
impl Catalog {
    /// Root must already exist and contain no symbolic links/reparse ancestors.
    pub fn open(root: impl AsRef<Path>, limits: OpsLimits) -> Result<Self> {
        let limits = limits.validate()?;
        let root = fs::checked(root.as_ref())?;
        if !root.is_dir() {
            return Err(OpsError::UnsafePath);
        }
        Ok(Self { root, limits })
    }
    /// Fixed validated root used for all relative snapshot names.
    pub fn root(&self) -> &Path {
        &self.root
    }
    fn published(&self, id: &BackupId) -> PathBuf {
        self.root.join(format!("{}.json", id.as_str()))
    }
    /// Create a verified snapshot and publish its manifest without replacing anything.
    pub fn backup(&self, admin: &Admin, id: &BackupId) -> Result<Manifest> {
        self.backup_with_fault(admin, id, Fault::default())
    }
    /// Trusted fault seam for recovery tests; retained stages are intentional.
    pub fn backup_with_fault(
        &self,
        admin: &Admin,
        id: &BackupId,
        fault: Fault,
    ) -> Result<Manifest> {
        fs::checked(&self.root)?;
        let _admission = fs::admission(&self.root)?;
        fs::absent(&self.published(id))?;
        // Include the retained control file and reserve both stage and publication.
        if self.list()?.len().saturating_add(3) > self.limits.catalog_entries {
            return Err(OpsError::Limit);
        }
        let stage_name = format!("staging-{}", fs::nonce()?);
        let stage = self.root.join(&stage_name);
        std::fs::create_dir(&stage)?;
        fs::write_json(
            &stage.join("incomplete.json"),
            &serde_json::json!({"backup_id":id,"state":"incomplete"}),
        )?;
        fault.hit(FaultPoint::BeforeExport)?;
        let snapshot = stage.join("snapshot.gdb");
        admin.export(&snapshot)?;
        fault.hit(FaultPoint::AfterSnapshot)?;
        let (len, hash, version) = fs::hash(&snapshot, self.limits.snapshot_bytes)?;
        let cut = Admin::verify_export(&snapshot)?;
        let manifest = Manifest {
            format_version: 1,
            backup_id: id.clone(),
            state: Completion::Complete,
            source_domain: format!("{:032x}", cut.domain),
            source_generation: None,
            snapshot: format!("{stage_name}/snapshot.gdb"),
            content_length: len,
            sha256: hash,
            envelope_version: version,
            tool_version: env!("CARGO_PKG_VERSION").into(),
        };
        manifest.validate()?;
        if serde_json::to_vec_pretty(&manifest)
            .map_err(|_| OpsError::Invalid)?
            .len() as u64
            > self.limits.manifest_bytes
        {
            return Err(OpsError::Limit);
        }
        fs::write_json(&stage.join("manifest.json"), &manifest)?;
        fault.hit(FaultPoint::BeforePublish)?;
        std::fs::hard_link(stage.join("manifest.json"), self.published(id))?;
        fault.hit(FaultPoint::AfterPublish)?;
        Ok(manifest)
    }
    /// Parse and validate metadata only. Use verify before relying on contents.
    pub fn inspect(&self, id: &BackupId) -> Result<Manifest> {
        let b = fs::read(&self.published(id), self.limits.manifest_bytes)?;
        let m: Manifest = serde_json::from_slice(&b).map_err(|_| OpsError::Invalid)?;
        m.validate()?;
        if &m.backup_id != id {
            return Err(OpsError::Invalid);
        }
        Ok(m)
    }
    pub(crate) fn verified(&self, id: &BackupId) -> Result<(Manifest, Snapshot)> {
        let m = self.inspect(id)?;
        self.verify_content(&m)?;
        let s = Admin::verify_export(self.root.join(&m.snapshot))?;
        self.verify_content(&m)?;
        if format!("{:032x}", s.domain) != m.source_domain {
            return Err(OpsError::Invalid);
        }
        Ok((m, s))
    }
    pub(crate) fn verify_content(&self, manifest: &Manifest) -> Result<()> {
        let (len, hash, version) = fs::hash(
            &self.root.join(&manifest.snapshot),
            self.limits.snapshot_bytes,
        )?;
        if len != manifest.content_length
            || hash != manifest.sha256
            || version != manifest.envelope_version
        {
            return Err(OpsError::Invalid);
        }
        Ok(())
    }
    /// Require exact whole-file checksum/length plus core envelope/logical validation.
    pub fn verify(&self, id: &BackupId) -> Result<Verification> {
        let (manifest, s) = self.verified(id)?;
        Ok(Verification {
            manifest,
            rows: s.rows.len(),
            tasks: s.tasks.len(),
            approvals: s.approvals.len(),
            operations: s.operations.len(),
        })
    }
    /// Bounded metadata catalog; does not silently discard malformed entries.
    /// Every retained stage is listed as incomplete staging even when a published
    /// manifest references it; the published record independently marks completion.
    pub fn list(&self) -> Result<Vec<CatalogEntry>> {
        fs::checked(&self.root)?;
        let mut names = Vec::new();
        for e in std::fs::read_dir(&self.root)? {
            let e = e?;
            if names.len() >= self.limits.catalog_entries {
                return Err(OpsError::Limit);
            }
            fs::checked(&e.path())?;
            names.push(e.file_name().into_string().map_err(|_| OpsError::Invalid)?);
        }
        names.sort();
        let mut out = Vec::new();
        for n in names {
            if n == ".ops-admission" {
                if !std::fs::symlink_metadata(self.root.join(&n))?.is_file() {
                    return Err(OpsError::UnsafePath);
                }
                continue;
            }
            if crate::format::stage_valid(&n) && self.root.join(&n).is_dir() {
                out.push(CatalogEntry::Incomplete { stage: n });
            } else if let Some(id) = n.strip_suffix(".json") {
                out.push(CatalogEntry::Complete {
                    manifest: self.inspect(&BackupId::new(id)?)?,
                });
            } else {
                return Err(OpsError::Invalid);
            }
        }
        Ok(out)
    }
}
