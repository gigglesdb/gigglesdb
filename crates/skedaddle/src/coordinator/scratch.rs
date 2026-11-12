//! Exclusive scratch owned by one local export/restore operation.
use std::path::{Path, PathBuf};

use super::{Error, Result};
use crate::fsconfine::Dir;

fn root(database: &Path) -> Result<PathBuf> {
    let mut name = database
        .file_name()
        .ok_or_else(|| Error::Rejected("local database requires a final path component".into()))?
        .to_os_string();
    name.push(".skedaddle-scratch");
    Ok(database.with_file_name(name))
}

pub(super) fn check(database: &Path) -> Result<()> {
    let root = std::path::absolute(root(database)?)?;
    let parent = Dir::open_root(root.parent().ok_or_else(|| orphan(&root))?)?;
    if parent
        .names()?
        .iter()
        .any(|name| Some(name.as_os_str()) == root.file_name())
    {
        return Err(orphan(&root));
    }
    Ok(())
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
    operation: impl FnOnce(&Dir, &Path) -> Result<T>,
) -> Result<T> {
    let root = std::path::absolute(root(database)?)?;
    let parent = Dir::open_root(root.parent().ok_or_else(|| orphan(&root))?)?;
    let name = root.file_name().ok_or_else(|| orphan(&root))?;
    let scratch = parent.create_dir(Path::new(name)).map_err(|e| {
        if e.kind() == std::io::ErrorKind::AlreadyExists {
            orphan(&root)
        } else {
            e.into()
        }
    })?;
    let archive = scratch.core_path(&root, "archive.export")?;
    let result = operation(&scratch, &archive);
    // Both outcomes clean through the same retained capability. The displayed
    // pathname is never re-opened for enumeration or file deletion.
    let cleanup = cleanup(&scratch, &root);
    drop(scratch);
    let cleanup = cleanup.and_then(|()| {
        parent.remove_dir(Path::new(name))?;
        parent.sync()?;
        Ok(())
    });
    if let Err(error) = cleanup {
        return Err(Error::RecoveryRequired(format!(
            "scratch cleanup failed at {}: {error}; operation error: {:?}",
            root.display(),
            result.as_ref().err()
        )));
    }
    result
}

fn cleanup(root: &Dir, display: &Path) -> Result<()> {
    for name in root.names()? {
        let text = name.to_string_lossy();
        let stage = text.strip_prefix("archive.export.staging-");
        let owned = text == "archive.export"
            || stage.is_some_and(|s| s.len() == 32 && s.bytes().all(|b| b.is_ascii_hexdigit()));
        if !owned {
            return Err(orphan(&display.join(&name)));
        }
        root.remove_file(Path::new(&name))?;
    }
    root.sync()?;
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
            let result = with_archive(&database, |_, archive| {
                Ok(admin.export_with_fault(archive, point)?)
            });
            assert!(matches!(result, Err(Error::Core(_))));
            check(&database).unwrap();
            assert_eq!(std::fs::read_dir(&parent).unwrap().count(), 1);
        }
    }
}
