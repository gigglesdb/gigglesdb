use crate::{BackupId, Catalog, Fault, FaultAction, FaultPoint, OpsError, Result, fs};
use giggles_core::{Admin, Limits, RecoveryPoint};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Durable acknowledgement stored in the restored database directory.
/// Contains no row values, SQL, task credentials or approval contents.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestoreReport {
    pub format_version: u32,
    pub backup_id: BackupId,
    pub source_domain: String,
    pub restored_domain: String,
    pub rows: usize,
    pub tasks: usize,
    pub approvals: usize,
    pub operations: usize,
    pub authorities_revoked: bool,
    pub reopened: bool,
}

impl Catalog {
    /// Verify, restore to a nonexistent target, reopen and persist a safe report.
    pub fn restore(&self, id: &BackupId, target: impl AsRef<Path>) -> Result<RestoreReport> {
        self.restore_with_fault(id, target, Fault::default())
    }
    /// Explicit trusted recovery test seam. Existing targets always reject.
    pub fn restore_with_fault(
        &self,
        id: &BackupId,
        target: impl AsRef<Path>,
        fault: Fault,
    ) -> Result<RestoreReport> {
        let target = target.as_ref();
        fs::absent(target)?;
        let (manifest, before) = self.verified(id)?;
        self.restore_verified(manifest, before, target, fault)
    }
    fn restore_verified(
        &self,
        manifest: crate::Manifest,
        before: giggles_core::Snapshot,
        target: &Path,
        fault: Fault,
    ) -> Result<RestoreReport> {
        fault.hit(FaultPoint::BeforeRestore)?;
        let point = match (fault.point, fault.action) {
            (FaultPoint::InstallCreated, FaultAction::Error) => RecoveryPoint::InstallCreated,
            (FaultPoint::InstallStaged, FaultAction::Error) => RecoveryPoint::InstallStaged,
            (FaultPoint::InstallCommitted, FaultAction::Error) => RecoveryPoint::InstallCommitted,
            (FaultPoint::InstallCreated, FaultAction::Abort) => RecoveryPoint::CrashInstallCreated,
            (FaultPoint::InstallStaged, FaultAction::Abort) => RecoveryPoint::CrashInstallStaged,
            (FaultPoint::InstallCommitted, FaultAction::Abort) => {
                RecoveryPoint::CrashInstallCommitted
            }
            _ => RecoveryPoint::None,
        };
        let a = Admin::restore_with_fault(
            self.root.join(&manifest.snapshot),
            target,
            Limits::default(),
            point,
        )?;
        fault.hit(FaultPoint::AfterInstall)?;
        let after = a.snapshot()?;
        let meta = a.authority_metadata()?;
        if after.domain == before.domain
            || meta.3 != Some(before.domain)
            || before.rows != after.rows
            || before.operations != after.operations
            || after.tasks.len() != before.tasks.len()
            || after.approvals.len() != before.approvals.len()
            || after.tasks.values().any(|t| !t.revoked)
            || after.approvals.values().any(|a| !a.revoked)
        {
            return Err(OpsError::Invalid);
        }
        drop(a);
        let reopened = Admin::open(target, Limits::default())?;
        if reopened.snapshot()? != after {
            return Err(OpsError::Invalid);
        }
        // Snapshot contains protected state, not every relational table. Verify
        // the complete source bytes again before acknowledging the restored cut.
        self.verify_content(&manifest)?;
        let report = RestoreReport {
            format_version: 1,
            backup_id: manifest.backup_id.clone(),
            source_domain: manifest.source_domain,
            restored_domain: format!("{:032x}", after.domain),
            rows: after.rows.len(),
            tasks: after.tasks.len(),
            approvals: after.approvals.len(),
            operations: after.operations.len(),
            authorities_revoked: true,
            reopened: true,
        };
        fs::write_json(&target.join("ops-restore-report.json"), &report)?;
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn later_sql_only_export_cannot_replace_a_verified_restore_cut() {
        let root = std::env::temp_dir().join(format!(
            "ops-cut-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(root.join("catalog")).unwrap();
        let admin = Admin::create(root.join("source"), Limits::default()).unwrap();
        let mut session = admin.sql_session();
        session
            .execute(
                "CREATE TABLE app (id BIGINT PRIMARY KEY, value BIGINT)",
                &[],
                None,
            )
            .unwrap();
        session
            .execute("INSERT INTO app VALUES(1,10)", &[], None)
            .unwrap();
        let catalog = Catalog::open(root.join("catalog"), crate::OpsLimits::default()).unwrap();
        let id = BackupId::new("original").unwrap();
        catalog.backup(&admin, &id).unwrap();
        let (manifest, cut) = catalog.verified(&id).unwrap();
        session
            .execute("UPDATE app SET value=20 WHERE id=1", &[], None)
            .unwrap();
        let later = root.join("later.gdb");
        admin.export(&later).unwrap();
        // The old checks cannot distinguish these two SQL cuts.
        assert_eq!(cut, Admin::verify_export(&later).unwrap());
        std::fs::copy(&later, catalog.root.join(&manifest.snapshot)).unwrap();
        let target = root.join("restored");
        let result = catalog.restore_verified(manifest, cut, &target, Fault::default());
        assert!(
            matches!(result, Err(OpsError::Invalid)),
            "changed cut was acknowledged: {result:?}"
        );
        assert!(!target.join("ops-restore-report.json").exists());
    }
}
