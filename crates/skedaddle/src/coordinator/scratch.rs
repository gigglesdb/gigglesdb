//! Exclusive scratch owned by one local export/restore operation.
use std::path::{Path, PathBuf};

use super::{Error, Result};

fn root(database: &Path) -> Result<PathBuf> {
    let mut name = database
        .file_name()
        .ok_or_else(|| Error::Rejected("local database requires a final path component".into()))?
        .to_os_string();
    name.push(".skedaddle-scratch");
    Ok(database.with_file_name(name))
}

pub(super) fn check(database: &Path) -> Result<()> {
    let root = root(database)?;
    match root.symlink_metadata() {
        Ok(_) => Err(orphan(&root)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

fn orphan(root: &Path) -> Error {
    Error::RecoveryRequired(format!(
        "local scratch exists at {}; an operation is active or left crash orphans; inspect it before retrying",
        root.display()
    ))
}

/// Never adopts an existing directory. All core-generated export staging files
/// are confined to this newly created scratch root, including on failure.
pub(super) fn with_archive<T>(
    database: &Path,
    operation: impl FnOnce(&Path) -> Result<T>,
) -> Result<T> {
    let root = root(database)?;
    std::fs::create_dir(&root).map_err(|e| {
        if e.kind() == std::io::ErrorKind::AlreadyExists {
            orphan(&root)
        } else {
            e.into()
        }
    })?;
    let result = operation(&root.join("archive.export"));
    // Run on success AND error. Cleanup errors are visible rather than hidden
    // behind an export/restore error; retain its context for diagnosis.
    if let Err(error) = cleanup(&root) {
        return Err(Error::RecoveryRequired(format!(
            "scratch cleanup failed at {}: {error}; operation error: {:?}",
            root.display(),
            result.as_ref().err()
        )));
    }
    result
}

fn cleanup(root: &Path) -> Result<()> {
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let stage = name.strip_prefix("archive.export.staging-");
        let owned = name == "archive.export"
            || stage.is_some_and(|s| s.len() == 32 && s.bytes().all(|b| b.is_ascii_hexdigit()));
        if !owned || !entry.file_type()?.is_file() {
            return Err(orphan(&entry.path()));
        }
        std::fs::remove_file(entry.path())?;
    }
    std::fs::remove_dir(root)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use giggles_core::{Admin, Limits, RecoveryPoint};

    #[test]
    fn core_export_failures_remove_their_partial_staging() {
        let parent =
            PathBuf::from(std::env::var_os("SKEDADDLE_TEST_ROOT").expect("set J: test root"))
                .join(format!("scratch-export-failures-{}", uuid::Uuid::new_v4()));
        assert!(parent.starts_with("J:/gigglesdb-work"));
        std::fs::create_dir_all(&parent).unwrap();
        let database = parent.join("db");
        let admin = Admin::create(&database, Limits::default()).unwrap();
        for point in [
            RecoveryPoint::ExportWrite,
            RecoveryPoint::ExportSync,
            RecoveryPoint::ExportVerify,
            RecoveryPoint::ExportPublish,
        ] {
            let result = with_archive(&database, |archive| {
                Ok(admin.export_with_fault(archive, point)?)
            });
            assert!(matches!(result, Err(Error::Core(_))));
            check(&database).unwrap();
            assert_eq!(std::fs::read_dir(&parent).unwrap().count(), 1);
        }
    }
}
